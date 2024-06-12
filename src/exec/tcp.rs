use std::mem;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use chrono::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::{
    Error, TcpError, TcpOutput, TcpPauseOutput, TcpPlanOutput, TcpRequestOutput, TcpResponse,
    WithPlannedCapacity,
};

use super::pause::{self, PauseSpec, PauseStream};
use super::tcpsegments::TcpSegmentsRunner;
use super::tee::Tee;
use super::Context;

#[derive(Debug)]
pub(super) struct TcpRunner {
    ctx: Arc<Context>,
    out: TcpOutput,
    state: State,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    size_hint: Option<usize>,
}

#[derive(Debug)]
pub enum State {
    Pending {
        pause: TcpPauseOutput,
    },
    Open {
        start: Instant,
        stream: PauseStream<BufWriter<Tee<TcpSegmentsRunner>>>,
        size_hint: Option<usize>,
    },
    Completed {
        transport: TcpSegmentsRunner,
    },
    Invalid,
}

impl TcpRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TcpPlanOutput) -> TcpRunner {
        TcpRunner {
            state: State::Pending {
                pause: plan.pause.clone(),
            },
            out: TcpOutput {
                request: Some(TcpRequestOutput {
                    host: plan.host.clone(),
                    port: plan.port,
                    body: Vec::new(),
                    time_to_first_byte: None,
                    time_to_last_byte: None,
                }),
                pause: TcpPauseOutput::with_planned_capacity(&plan.pause),
                plan,
                response: None,
                errors: Vec::new(),
                duration: Duration::zero(),
                handshake_duration: None,
            },
            ctx,
            first_read: None,
            first_write: None,
            last_read: None,
            last_write: None,
            size_hint: None,
        }
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        self.size_hint = hint;
        None
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn start(
        &mut self,
        transport: TcpSegmentsRunner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let State::Pending { pause } = mem::replace(&mut self.state, State::Invalid) else {
            panic!("invalid state to start tcp {:?}", self.state)
        };

        self.state = State::Open {
            start,
            stream: pause::new_stream(
                self.ctx.clone(),
                BufWriter::new(Tee::new(transport)),
                // TODO: implement read size hints.
                vec![PauseSpec {
                    group_offset: 0,
                    plan: pause.receive_body.start,
                }],
                if let Some(size) = self.size_hint {
                    vec![
                        PauseSpec {
                            group_offset: 0,
                            plan: pause.send_body.start,
                        },
                        PauseSpec {
                            group_offset: size.try_into().unwrap(),
                            plan: pause.send_body.end,
                        },
                    ]
                } else {
                    if !pause.send_body.end.is_empty() {
                        return Err(Box::new(Error(
                            "tcp.pause.send_body.end is unsupported in this request".to_owned(),
                        )));
                    }
                    vec![PauseSpec {
                        group_offset: 0,
                        plan: pause.send_body.start,
                    }]
                },
            ),
            size_hint: self.size_hint,
        };
        Ok(())
    }

    pub async fn execute(&mut self) {
        let body = std::mem::take(&mut self.out.plan.body);
        if let Err(e) = self.write_all(&body).await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.out.plan.body = body;
            self.complete();
            return;
        };
        self.out.plan.body = body;
        if let Err(e) = self.flush().await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.complete();
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.read_to_end(&mut response).await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.complete();
            return;
        }
    }

    pub fn finish(mut self) -> (TcpOutput, TcpSegmentsRunner) {
        self.complete();
        let State::Completed { transport } = self.state else {
            panic!("invalid completion state {:?}", self.state)
        };
        (self.out, transport)
    }

    fn complete(&mut self) {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Open { start, stream, .. } = state else {
            return;
        };

        let end_time = Instant::now();

        // TODO: how to sort out which pause outputs came from first or last?
        let (stream, send_pause, receive_pause) = stream.finish_stream();
        let stream = stream.into_inner();
        let (transport, writes, reads) = stream.into_parts();

        let mut receive_pause = receive_pause.into_iter();
        self.out.pause.receive_body.start = receive_pause.next().unwrap_or_default();
        self.out.pause.receive_body.end = receive_pause.next().unwrap_or_default();
        let mut send_pause = send_pause.into_iter();
        self.out.pause.send_body.start = send_pause.next().unwrap_or_default();
        self.out.pause.send_body.end = send_pause.next().unwrap_or_default();

        if let Some(req) = &mut self.out.request {
            if let Some(first_write) = self.first_write {
                req.time_to_first_byte =
                    Some(chrono::Duration::from_std(first_write - start).unwrap());
            }
            if let Some(last_write) = self.first_write {
                req.time_to_last_byte =
                    Some(chrono::Duration::from_std(last_write - start).unwrap());
            }
            req.body = writes;
        }
        if !reads.is_empty() {
            self.out.response = Some(TcpResponse {
                body: reads,
                time_to_first_byte: self
                    .first_read
                    .map(|first_read| first_read - start)
                    .map(Duration::from_std)
                    .transpose()
                    .unwrap(),
                time_to_last_byte: self
                    .last_read
                    .map(|last_read| last_read - start)
                    .map(Duration::from_std)
                    .transpose()
                    .unwrap(),
            });
        }
        self.out.duration = chrono::Duration::from_std(end_time - start).unwrap();
        self.state = State::Completed { transport };
    }
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(Error(format!(
                "cannot read from stream in {:?} state",
                self.state
            )))));
        };
        // Read some data.
        let result = pin!(stream).poll_read(cx, buf);

        // Record the time of this read.
        self.last_read = Some(Instant::now());
        if self.first_read.is_none() {
            self.first_read = self.last_read;
        }

        result
    }
}

impl AsyncWrite for TcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let mut state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Open { stream, .. } = &mut state else {
            return Poll::Ready(Err(std::io::Error::other(Error(format!(
                "cannot write to stream in {:?} state",
                self.state
            )))));
        };
        if self.first_write.is_none() {
            self.first_write = Some(Instant::now());
        }
        let result = std::pin::pin!(stream).poll_write(cx, buf);
        self.last_write = Some(Instant::now());
        self.state = state;
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(Error(format!(
                "cannot flush stream in {:?} state",
                self.state
            )))));
        };
        std::pin::pin!(stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(Error(format!(
                "cannot shutdown stream in {:?} state",
                self.state
            )))));
        };
        let poll = pin!(stream).poll_shutdown(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.complete();
        }
        poll
    }
}

impl Unpin for TcpRunner {}
