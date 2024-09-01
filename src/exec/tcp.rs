use std::mem;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use chrono::TimeDelta;
use futures::FutureExt;
use log::{debug, info, warn};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::transport::{self, TransportChannelType};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{self, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::{
    TcpError, TcpOutput, TcpPauseOutput, TcpPlanOutput, TcpReceivedOutput, TcpSegmentOutput,
    TcpSentOutput, WithPlannedCapacity,
};

use super::pause::{self, PauseSpec, PauseStream};
use super::tee::Tee;
use super::timing::Timing;
use super::{tcp_common, Context};

#[derive(Debug)]
pub(super) struct TcpRunner {
    ctx: Arc<Context>,
    out: TcpOutput,
    state: State,
    size_hint: Option<usize>,
}

#[derive(Debug)]
pub enum State {
    Pending {
        pause: TcpPauseOutput,
    },
    Open {
        start: Instant,
        stream: PauseStream<BufWriter<Tee<Timing<TcpStream>>>>,
        raw_reads: JoinHandle<Vec<TcpSegmentOutput>>,
        raw_writes: JoinHandle<Vec<TcpSegmentOutput>>,
        size_hint: Option<usize>,
        send_done: oneshot::Sender<usize>,
        recv_done: oneshot::Sender<usize>,
    },
    Completed {
        raw_reads: JoinHandle<Vec<TcpSegmentOutput>>,
        raw_writes: JoinHandle<Vec<TcpSegmentOutput>>,
    },
    CompletedEmpty,
    Invalid,
}

impl TcpRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TcpPlanOutput) -> TcpRunner {
        TcpRunner {
            state: State::Pending {
                pause: plan.pause.clone(),
            },
            out: TcpOutput {
                sent: None,
                pause: TcpPauseOutput::with_planned_capacity(&plan.pause),
                plan,
                received: None,
                errors: Vec::new(),
                duration: TimeDelta::zero(),
                handshake_duration: None,
            },
            ctx,
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

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let State::Pending { pause } = mem::replace(&mut self.state, State::Invalid) else {
            panic!("invalid state to start tcp {:?}", self.state)
        };

        let Some(remote_addr) = net::lookup_host(format!(
            "{}:{}",
            self.out.plan.dest_host, self.out.plan.dest_port
        ))
        .await
        .map_err(|e| {
            anyhow!(
                "lookup host '{}:{}': {e}",
                self.out.plan.dest_host,
                self.out.plan.dest_port
            )
        })?
        .next() else {
            self.out.errors.push(TcpError {
                kind: "dns lookup".to_owned(),
                message: format!(
                    "no A records found for tcp_segments.dest_host '{}'",
                    self.out.plan.dest_host
                ),
            });
            self.state = State::CompletedEmpty;
            bail!(
                "no A records found for tcp_segments.dest_host '{}'",
                self.out.plan.dest_host
            );
        };
        let remote_addr_string = remote_addr.ip().to_string();

        let (_, read) = transport::transport_channel(
            65535,
            TransportChannelType::Layer4(transport::TransportProtocol::Ipv4(
                IpNextHeaderProtocols::Tcp,
            )),
        )
        .inspect_err(|e| {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::CompletedEmpty;
        })?;

        self.out.sent = Some(TcpSentOutput {
            dest_ip: remote_addr_string,
            dest_port: remote_addr.port(),
            body: Vec::new(),
            segments: Vec::new(),
            time_to_first_byte: None,
            time_to_last_byte: None,
        });

        let start = Instant::now();
        let raw_reads = tcp_common::reader(read, remote_addr, start);
        let (recv_done, is_done) = oneshot::channel();
        let raw_reads = tokio::spawn(segment_handler(raw_reads, is_done));

        // TODO: support recording of raw writes.
        let (_, raw_writes) = mpsc::unbounded_channel::<TcpSegmentOutput>();
        let (send_done, is_done) = oneshot::channel();
        let raw_writes = tokio::spawn(segment_handler(raw_writes, is_done));

        let transport = match TcpStream::connect(remote_addr).await {
            Ok(t) => t,
            Err(e) => {
                self.out.errors.push(TcpError {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                self.state = State::Completed {
                    raw_reads,
                    raw_writes,
                };
                bail!("connect to {remote_addr}: {e}");
            }
        };

        let mut tee = Tee::new(Timing::new(transport, self.out.plan.close.timeout));
        if let Some(limit) = self.out.plan.close.bytes {
            tee.set_read_limit(limit.try_into()?);
        }
        if let Some(pattern) = &self.out.plan.close.pattern {
            tee.set_pattern(
                Some(pattern.parsed.clone()),
                self.out
                    .plan
                    .close
                    .pattern_window
                    .map(|window| window.try_into())
                    .transpose()?,
            );
        }

        self.state = State::Open {
            start,
            send_done,
            recv_done,
            stream: pause::new_stream(
                self.ctx.clone(),
                BufWriter::new(tee),
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
                        bail!("tcp.pause.send_body.end is unsupported in this request");
                    }
                    vec![PauseSpec {
                        group_offset: 0,
                        plan: pause.send_body.start,
                    }]
                },
            ),
            raw_reads,
            raw_writes,
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

    pub async fn finish(mut self) -> TcpOutput {
        self.complete();
        match self.state {
            State::CompletedEmpty => {}
            State::Completed {
                raw_reads,
                raw_writes,
            } => {
                if let Ok(raw_writes) = raw_writes.await {
                    if let Some(ref mut sent) = &mut self.out.sent {
                        sent.segments = raw_writes;
                    }
                }
                if let Ok(raw_reads) = raw_reads.await {
                    if let Some(ref mut received) = &mut self.out.received {
                        received.segments = raw_reads;
                    } else {
                        self.out.received = Some(TcpReceivedOutput {
                            body: Vec::new(),
                            segments: raw_reads,
                            time_to_first_byte: None,
                            time_to_last_byte: None,
                        });
                    }
                }
            }
            _ => {
                panic!("invalid completion state {:?}", self.state)
            }
        }
        self.out
    }

    fn complete(&mut self) {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Open {
            start,
            stream,
            raw_reads,
            raw_writes,
            send_done,
            recv_done,
            ..
        } = state
        else {
            self.state = state;
            return;
        };

        let end_time = Instant::now();

        // TODO: how to sort out which pause outputs came from first or last?
        let (stream, send_pause, receive_pause) = stream.finish_stream();
        let stream = stream.into_inner();
        let (stream, writes, reads, truncated_reads) = stream.into_parts();

        if let Err(e) = send_done.send(writes.len()) {
            warn!("send expected length of writes to raw socket collector: {e}");
        }
        if let Err(e) = recv_done.send(reads.len()) {
            warn!("send expected length of reads to raw socket collector: {e}");
        }

        let mut receive_pause = receive_pause.into_iter();
        self.out.pause.receive_body.start = receive_pause.next().unwrap_or_default();
        self.out.pause.receive_body.end = receive_pause.next().unwrap_or_default();
        let mut send_pause = send_pause.into_iter();
        self.out.pause.send_body.start = send_pause.next().unwrap_or_default();
        self.out.pause.send_body.end = send_pause.next().unwrap_or_default();

        if let Some(sent) = &mut self.out.sent {
            if let Some(first_write) = stream.first_write() {
                sent.time_to_first_byte = Some(TimeDelta::from_std(first_write - start).unwrap());
            }
            if let Some(last_write) = stream.first_write() {
                sent.time_to_last_byte = Some(TimeDelta::from_std(last_write - start).unwrap());
            }
            sent.body = writes;
        }
        if !reads.is_empty() {
            self.out.received = Some(TcpReceivedOutput {
                body: reads,
                segments: Vec::new(),
                time_to_first_byte: stream
                    .first_read()
                    .map(|first_read| first_read - start)
                    .map(TimeDelta::from_std)
                    .transpose()
                    .unwrap(),
                time_to_last_byte: stream
                    .last_read()
                    .map(|last_read| last_read - start)
                    .map(TimeDelta::from_std)
                    .transpose()
                    .unwrap(),
            });
        }
        self.out.duration = TimeDelta::from_std(end_time - start).unwrap();
        self.state = State::Completed {
            raw_reads,
            raw_writes,
        };
    }
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot read from stream in {:?} state",
                self.state
            ))));
        };

        // Read some data.
        let result = pin!(stream).poll_read(cx, buf);

        result
    }
}

impl AsyncWrite for TcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot write to stream in {:?} state",
                self.state
            ))));
        };
        pin!(stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot flush stream in {:?} state",
                self.state
            ))));
        };
        std::pin::pin!(stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { stream, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot shutdown stream in {:?} state",
                self.state
            ))));
        };

        let poll = pin!(stream).poll_shutdown(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.complete();
        }
        poll
    }
}

impl Unpin for TcpRunner {}

async fn segment_handler(
    mut chan: mpsc::UnboundedReceiver<TcpSegmentOutput>,
    is_done: oneshot::Receiver<usize>,
) -> Vec<TcpSegmentOutput> {
    let mut is_done = is_done.fuse();
    let mut segments = Vec::new();
    let mut total_size = 0;
    let expect_size = loop {
        select! {
            Some(segment) = chan.recv() => {
                debug!("raw socket got segment {segment:?}");
                total_size += segment.payload.len();
                segments.push(segment);
            }
            done = &mut is_done => {
                match done {
                    Ok(size) => break size,
                    Err(e) => {
                        info!("raw socket done signal: {e}");
                        return segments;
                    }
                }
            },
            else => return segments,
        }
    };
    debug!("raw socket got signal to expect {expect_size} bytes");
    loop {
        select! {
            Some(segment) = chan.recv() => {
                debug!("raw socket got segment {segment:?}");
                total_size += segment.payload.len();
                segments.push(segment);
                if total_size >= expect_size {
                    info!("raw socket post-completion reached threshold {total_size} of {expect_size} bytes");
                    return segments;
                }
            }
            _ = sleep(Duration::new(0, 50000000)) => {
                info!("raw socket post-completion timed out with {total_size} of {expect_size} bytes");
                return segments;
            }
        }
    }
}
