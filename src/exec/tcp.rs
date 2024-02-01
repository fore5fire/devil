use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use chrono::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::exec::pause::Pause;
use crate::{
    Error, Output, TcpError, TcpOutput, TcpPauseOutput, TcpPlanOutput, TcpRequestOutput,
    TcpResponse, WithPlannedCapacity,
};

use super::pause::{PauseSpec, PauseStream};
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
}

#[derive(Debug)]
pub enum State {
    Pending {
        addr: String,
        pause: TcpPauseOutput,
    },
    Open {
        start: Instant,
        stream: PauseStream<Tee<TcpStream>>,
        size_hint: Option<usize>,
    },
    Completed,
    Invalid,
}

impl TcpRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TcpPlanOutput) -> TcpRunner {
        TcpRunner {
            state: State::Pending {
                addr: format!("{}:{}", plan.host, plan.port),
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
        }
    }

    pub async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Pending { addr, pause } = state else {
            return Err(Box::new(Error(
                "attempt to start TcpRunner from unexpected state".to_owned(),
            )));
        };
        let start = Instant::now();
        self.out
            .pause
            .handshake
            .start
            .reserve_exact(self.out.plan.pause.handshake.start.len());
        for p in &self.out.plan.pause.handshake.start {
            if p.offset_bytes != 0 {
                return Err(Box::new(Error(
                    "pause offset not yet supported for tcp handshake".to_string(),
                )));
            }
            println!("pausing before tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .start
                .push(Pause::new(&self.ctx, p).await?);
        }
        let stream = TcpStream::connect(addr).await.map_err(|e| {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::Completed;
            Error(e.to_string())
        })?;
        let handshake_duration = start.elapsed();
        self.out
            .pause
            .handshake
            .end
            .reserve_exact(self.out.plan.pause.handshake.end.len());
        for p in self.out.plan.pause.handshake.end.iter() {
            if p.offset_bytes != 0 {
                return Err(Box::new(Error(
                    "pause offset not yet supported for tcp handshake".to_string(),
                )));
            }
            println!("pausing after tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .end
                .push(Pause::new(&self.ctx, p).await?);
        }

        self.out.handshake_duration = Some(chrono::Duration::from_std(handshake_duration).unwrap());
        self.state = State::Open {
            start,
            stream: PauseStream::new(
                self.ctx.clone(),
                Tee::new(stream),
                if let Some(size) = size_hint {
                    vec![
                        PauseSpec {
                            group_offset: 0,
                            plan: pause.receive_body.start,
                        },
                        PauseSpec {
                            group_offset: size.try_into().unwrap(),
                            plan: pause.receive_body.end,
                        },
                    ]
                } else {
                    if !pause.receive_body.end.is_empty() {
                        return Err(Box::new(Error(
                            "tcp.pause.receive_body.end is unsupported in this request".to_owned(),
                        )));
                    }
                    vec![PauseSpec {
                        group_offset: 0,
                        plan: pause.receive_body.start,
                    }]
                },
                if let Some(size) = size_hint {
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
            size_hint,
        };
        Ok(())
    }
}

impl TcpRunner {
    pub async fn execute(&mut self) {
        if let Err(e) = self.start(Some(self.out.plan.body.len())).await {
            self.out.errors.push(TcpError {
                kind: "tcp_start".to_owned(),
                message: e.to_string(),
            })
        }
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

    pub fn finish(mut self) -> Output {
        self.complete();
        Output::Tcp(self.out)
    }

    fn complete(&mut self) {
        let state = std::mem::replace(&mut self.state, State::Completed);
        let State::Open { start, stream, .. } = state else {
            return;
        };

        let end_time = Instant::now();

        // TODO: how to sort out which pause outputs came from first or last?
        let (stream, send_pause, receive_pause) = stream.finish();
        let (_, writes, reads) = stream.into_parts();

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
