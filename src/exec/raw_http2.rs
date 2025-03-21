use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{ready, Poll};
use std::time::Instant;
use std::{iter, pin::pin};
use std::{mem, sync::Arc};

use anyhow::{anyhow, bail};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Bytes, BytesMut};
use chrono::TimeDelta;
use h2::client::{handshake, SendRequest};
use tokio::io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::join;
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, Instrument};

use crate::{
    Direction, Http2FrameOutput, Http2FrameType, MaybeUtf8, PduName, ProtocolDiscriminants,
    ProtocolName, RawHttp2Error, RawHttp2Output, RawHttp2PlanOutput,
};

use super::extract;
use super::{runner::Runner, Context};

#[derive(Debug)]
pub struct RawHttp2Runner {
    ctx: Arc<Context>,
    out: RawHttp2Output,
    state: State,
    start_time: Option<Instant>,
    send_frames: Vec<Http2FrameOutput>,
    send_preface: MaybeUtf8,
}

#[derive(Debug)]
enum State {
    Pending {
        executor: bool,
    },
    StartFailed,
    Open {
        connection: JoinHandle<Result<FrameParserStream, Box<dyn std::error::Error + Send + Sync>>>,
        streams: Vec<SendRequest<Bytes>>,
    },
    Executing {
        transport: Runner,
    },
    Completed {
        transport: Option<Runner>,
    },
    Invalid,
}

impl RawHttp2Runner {
    pub(super) fn new(
        ctx: Arc<Context>,
        mut plan: RawHttp2PlanOutput,
        protocol: ProtocolDiscriminants,
        executor: bool,
    ) -> Self {
        Self {
            // Pull the frames out of the plan, since we'll need to update their pdu numbers when
            // we actually send them (or, if we're not executor, we'll just put them back
            // unmodified in [complete]
            send_frames: mem::take(&mut plan.frames)
                .into_iter()
                .map(Arc::unwrap_or_clone)
                .collect(),
            send_preface: plan.preamble.clone().unwrap_or_default(),
            out: RawHttp2Output {
                name: ProtocolName::with_job(ctx.job_name.clone(), protocol),
                errors: Vec::new(),
                duration: TimeDelta::zero().into(),
                received: Vec::new(),
                sent: plan.frames.clone(),
                plan,
            },
            ctx,
            state: State::Pending { executor },
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

    pub fn size_hint(&mut self, _hint: Option<usize>) -> Option<usize> {
        None
    }

    pub async fn execute(&mut self) {
        let State::Executing { transport } = mem::replace(&mut self.state, State::Invalid) else {
            panic!("wrong state to execute raw_http2: {:?}", self.state);
        };
        let (mut recv, mut send) = split(transport);
        let mut frames = mem::take(&mut self.send_frames);
        let preface = mem::take(&mut self.send_preface);
        let counter = Arc::new(AtomicU64::new(0));
        let proto = self.out.name.clone();
        let (send_result, (received, recv_err)) = join!(
            async {
                send.write_all(&preface).await?;
                for frame in &mut frames {
                    frame.write(&mut send).await?;
                    frame.name.pdu = counter.fetch_add(1, Ordering::Relaxed);
                }
                send.shutdown().await
            },
            async {
                let mut parser = FrameParser::new(
                    FrameParserState::FrameHeader,
                    Direction::Recv,
                    proto,
                    counter.clone(),
                );
                let mut buf = [0; 2048];
                loop {
                    match recv.read(&mut buf).await {
                        Ok(0) => return (parser.out, None),
                        Err(e) => return (parser.out, Some(e)),
                        Ok(len) => {
                            if let Err(e) = parser.push(&buf[..len]) {
                                return (parser.out, Some(e));
                            }
                        }
                    }
                }
            }
            .instrument(debug_span!("raw_http2_execute_read"))
        );
        self.send_frames = frames;
        self.out.received = received;
        if let Some(e) = recv_err {
            self.out.errors.push(RawHttp2Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        }
        if let Err(e) = send_result {
            self.out.errors.push(RawHttp2Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        }
        self.state = State::Executing {
            transport: recv.unsplit(send),
        };
    }

    pub(super) async fn start(&mut self, transport: Runner, streams: usize) -> anyhow::Result<()> {
        self.start_time = Some(Instant::now());
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Pending { executor } = state else {
            bail!("state {state:?} not valid for open");
        };

        self.state = if executor {
            State::Executing { transport }
        } else {
            let (extractor, transport) =
                extract::new(FrameParserStream::new(transport, self.out.name.clone()));

            let (stream, connection) = handshake(transport).await.inspect_err(|e| {
                self.out.errors.push(crate::RawHttp2Error {
                    kind: "handshake".to_owned(),
                    message: e.to_string(),
                });
                self.state = State::StartFailed;
            })?;
            State::Open {
                connection: tokio::spawn(async {
                    connection.await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(extractor.await?)
                }),
                streams: iter::repeat(stream).take(streams).collect(),
            }
        };

        Ok(())
    }

    pub(super) async fn finish(mut self) -> (RawHttp2Output, Option<Runner>) {
        self.complete().await;
        let State::Completed { transport } = self.state else {
            panic!("incorrect state to finish RawHttp2Runner")
        };
        (self.out, transport)
    }

    async fn complete(&mut self) {
        let end_time = Instant::now();
        if let Some(start) = self.start_time {
            self.out.duration = TimeDelta::from_std(end_time.duration_since(start))
                .expect("durations should fit in chrono")
                .into();
        }
        // Restore the planned frames to send, even if we're not executor.
        self.out.plan.frames = mem::take(&mut self.send_frames)
            .into_iter()
            .map(Arc::new)
            .collect();
        let state = std::mem::replace(&mut self.state, State::Invalid);
        match state {
            State::Open { connection, .. } => match connection.await {
                Ok(Ok(transport)) => {
                    let (reads, writes, inner) = transport.finish();
                    self.out.received = reads;
                    self.out.sent = writes;
                    self.state = State::Completed {
                        transport: Some(inner),
                    }
                }
                Ok(Err(e)) => {
                    self.out.errors.push(crate::RawHttp2Error {
                        kind: "network".to_owned(),
                        message: e.to_string(),
                    });
                    self.state = State::Completed { transport: None }
                }
                Err(e) => {
                    self.out.errors.push(crate::RawHttp2Error {
                        kind: "processing".to_owned(),
                        message: e.to_string(),
                    });
                    self.state = State::Completed { transport: None }
                }
            },
            State::Executing { transport } => {
                self.state = State::Completed {
                    transport: Some(transport),
                };
                self.out.sent = self.out.plan.frames.clone();
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

const WRITE_PREFACE: &str = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Debug)]
struct FrameParser {
    buf: BytesMut,
    state: FrameParserState,
    out: Vec<Arc<Http2FrameOutput>>,
    direction: Direction,
    proto: ProtocolName,
    counter: Arc<AtomicU64>,
}

impl FrameParser {
    fn new(
        state: FrameParserState,
        direction: Direction,
        proto: ProtocolName,
        counter: Arc<AtomicU64>,
    ) -> Self {
        Self {
            buf: BytesMut::new(),
            state,
            out: Vec::new(),
            direction,
            proto,
            counter,
        }
    }

    fn push(&mut self, mut buf: &[u8]) -> io::Result<()> {
        debug!("push called with data: {buf:?}");
        while !buf.is_empty() {
            match &self.state {
                FrameParserState::Preface(remaining) => {
                    debug!("push processing remaining preface: {remaining:?}");
                    let shorter = remaining.len().min(buf.len());
                    if remaining[..shorter] != buf[..shorter] {
                        return Err(io::Error::other(anyhow!("unexpected http2 preface")));
                    }
                    if shorter < remaining.len() {
                        self.state = FrameParserState::Preface(&remaining[shorter..]);
                    } else {
                        buf = &buf[shorter..];
                        self.state = FrameParserState::FrameHeader;
                    }
                }
                FrameParserState::FrameHeader => {
                    debug!(
                        buffered = self.buf.len(),
                        got = buf.len(),
                        "push processing frame header",
                    );
                    let to_copy = (9 - self.buf.len()).min(buf.len());
                    self.buf.extend_from_slice(&buf[..to_copy]);
                    if self.buf.len() < 9 {
                        break;
                    }
                    buf = &buf[to_copy..];

                    self.state = FrameParserState::FramePayload {
                        len: NetworkEndian::read_u24(&self.buf).try_into().unwrap(),
                        kind: self.buf[3],
                        flags: self.buf[4],
                        r: self.buf[5] & 1 << 7 != 0,
                        stream_id: NetworkEndian::read_u32(&self.buf[5..]) & !(1 << 31),
                    };
                    self.buf.clear();
                }
                FrameParserState::FramePayload {
                    len,
                    kind,
                    flags,
                    r,
                    stream_id,
                } => {
                    debug!(
                        buffered = self.buf.len(),
                        got = buf.len(),
                        expect_len = len,
                        "push processing frame payload",
                    );
                    let to_copy = (len - self.buf.len()).min(buf.len());
                    self.buf.extend_from_slice(&buf[..to_copy]);
                    if self.buf.len() < *len {
                        break;
                    }
                    buf = &buf[to_copy..];
                    let out = Arc::new(Http2FrameOutput::new(
                        PduName::with_protocol(
                            self.proto.clone(),
                            self.counter.fetch_add(1, Ordering::Relaxed),
                        ),
                        Http2FrameType::new(*kind),
                        (*flags).into(),
                        *r,
                        *stream_id,
                        self.buf.split().freeze(),
                        self.direction,
                    ));
                    debug!(out = ?out, "push finished frame");
                    self.out.push(out);
                    self.buf.clear();
                    self.state = FrameParserState::FrameHeader;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct FrameParserStream {
    transport: Runner,
    write: FrameParser,
    read: FrameParser,
}

impl FrameParserStream {
    fn new(transport: Runner, proto: ProtocolName) -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        Self {
            transport,
            write: FrameParser::new(
                FrameParserState::Preface(WRITE_PREFACE.as_bytes()),
                Direction::Send,
                proto.clone(),
                counter.clone(),
            ),
            read: FrameParser::new(
                FrameParserState::FrameHeader,
                Direction::Recv,
                proto,
                counter,
            ),
        }
    }

    fn finish(
        self,
    ) -> (
        Vec<Arc<Http2FrameOutput>>,
        Vec<Arc<Http2FrameOutput>>,
        Runner,
    ) {
        (self.read.out, self.write.out, self.transport)
    }
}

#[derive(Debug)]
enum FrameParserState {
    Preface(&'static [u8]),
    FrameHeader,
    FramePayload {
        len: usize,
        kind: u8,
        flags: u8,
        r: bool,
        stream_id: u32,
    },
}

impl AsyncRead for FrameParserStream {
    #[inline]
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let prev = buf.filled().len();
        ready!(pin!(&mut self.transport).poll_read(cx, buf))?;
        let _guard = debug_span!("raw_http2_read").entered();
        self.read.push(&buf.filled()[prev..])?;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FrameParserStream {
    #[inline]
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let len = ready!(pin!(&mut self.transport).poll_write(cx, buf))?;
        let _guard = debug_span!("raw_http2_write").entered();
        self.write.push(&buf[..len])?;
        Poll::Ready(Ok(len))
    }
    #[inline]
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.transport).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.transport).poll_shutdown(cx)
    }
}
