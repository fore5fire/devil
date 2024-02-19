use std::iter;
use std::time::Instant;
use std::{mem, sync::Arc};

use bytes::Bytes;
use chrono::Duration;
use h2::client::{handshake, SendRequest};
use tokio::task::JoinHandle;

use crate::{
    Http2FramesOutput, Http2FramesPauseOutput, Http2FramesPlanOutput, WithPlannedCapacity,
};

use super::extract;
use super::{runner::Runner, Context};

#[derive(Debug)]
pub struct Http2FramesRunner {
    ctx: Arc<Context>,
    out: Http2FramesOutput,
    state: State,
    start_time: Option<Instant>,
}

#[derive(Debug)]
enum State {
    Pending,
    StartFailed,
    Open {
        connection: JoinHandle<Result<Runner, Box<dyn std::error::Error + Send + Sync>>>,
        streams: Vec<SendRequest<Bytes>>,
    },
    Completed {
        transport: Option<Runner>,
    },
    Invalid,
}

impl Http2FramesRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: Http2FramesPlanOutput) -> Self {
        Self {
            ctx,
            out: Http2FramesOutput {
                errors: Vec::new(),
                duration: Duration::zero(),
                pause: Http2FramesPauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
            state: State::Pending,
            start_time: None,
        }
    }

    pub fn new_stream(&mut self) -> Option<SendRequest<Bytes>> {
        let State::Open {
            ref mut streams, ..
        } = &mut self.state
        else {
            panic!("attempt to create new http2 stream from invalid state");
        };
        streams.pop()
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        None
    }

    pub(super) async fn start(
        &mut self,
        transport: Runner,
        streams: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.start_time = Some(Instant::now());
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Pending = state else {
            return Err(Box::new(crate::Error(format!(
                "state {state:?} not valid for open"
            ))));
        };

        let (extractor, transport) = extract::new(transport);

        match handshake(transport).await {
            Ok((stream, connection)) => {
                self.state = State::Open {
                    connection: tokio::spawn(async {
                        connection.await?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(extractor.await?)
                    }),
                    streams: iter::repeat(stream).take(streams).collect(),
                };

                Ok(())
            }
            Err(e) => {
                self.out.errors.push(crate::Http2FramesError {
                    kind: "handshake".to_owned(),
                    message: e.to_string(),
                });
                self.state = State::StartFailed;
                Err(Box::new(e))
            }
        }
    }

    pub(super) async fn finish(mut self) -> (Http2FramesOutput, Option<Runner>) {
        self.complete().await;
        let State::Completed { transport } = self.state else {
            panic!("incorrect state to finish Http2FramesRunner")
        };
        (self.out, transport)
    }

    async fn complete(&mut self) {
        let end_time = Instant::now();
        if let Some(start) = self.start_time {
            self.out.duration = chrono::Duration::from_std(end_time.duration_since(start))
                .expect("durations should fit in chrono");
        }
        let state = std::mem::replace(&mut self.state, State::Invalid);
        match state {
            State::Open {
                connection,
                streams,
            } => {
                // Explicitly drop any unused streams so the connection manager knows we can start
                // closing down the connection.
                drop(streams);
                match connection.await {
                    Ok(Ok(transport)) => {
                        self.state = State::Completed {
                            transport: Some(transport),
                        }
                    }
                    Ok(Err(e)) => {
                        self.out.errors.push(crate::Http2FramesError {
                            kind: "network".to_owned(),
                            message: e.to_string(),
                        });
                        self.state = State::Completed { transport: None }
                    }
                    Err(e) => {
                        self.out.errors.push(crate::Http2FramesError {
                            kind: "processing".to_owned(),
                            message: e.to_string(),
                        });
                        self.state = State::Completed { transport: None }
                    }
                }
            }
            State::Completed { transport } => {
                self.state = State::Completed { transport };
            }
            State::Pending { .. } | State::StartFailed => {
                self.state = State::Completed { transport: None };
            }
            State::Invalid => panic!(),
        };
    }
}
