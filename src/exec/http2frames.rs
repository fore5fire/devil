use std::io;
use std::task::{ready, Poll};
use std::time::Instant;
use std::{iter, pin::pin};
use std::{mem, sync::Arc};

use anyhow::{anyhow, bail};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use chrono::Duration;
use h2::client::{handshake, SendRequest};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

use crate::{
    Http2FrameOutput, Http2FramesOutput, Http2FramesPauseOutput, Http2FramesPlanOutput,
    WithPlannedCapacity,
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
        connection: JoinHandle<Result<FrameParserStream, Box<dyn std::error::Error + Send + Sync>>>,
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

    //pub fn sent_frames(&self, id: u32) -> impl Iterator<Item = Http2FrameOutput> {}
    //pub fn received_frames(&self, id: u32) -> impl Iterator<Item = Http2FrameOutput> {}

    pub fn size_hint(&mut self, _hint: Option<usize>) -> Option<usize> {
        None
    }

    pub(super) async fn start(&mut self, transport: Runner, streams: usize) -> anyhow::Result<()> {
        self.start_time = Some(Instant::now());
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Pending = state else {
            bail!("state {state:?} not valid for open");
        };

        let (extractor, transport) = extract::new(FrameParserStream::new(transport));

        let (stream, connection) = handshake(transport).await.inspect_err(|e| {
            self.out.errors.push(crate::Http2FramesError {
                kind: "handshake".to_owned(),
                message: e.to_string(),
            });
            self.state = State::StartFailed;
        })?;
        self.state = State::Open {
            connection: tokio::spawn(async {
                connection.await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(extractor.await?)
            }),
            streams: iter::repeat(stream).take(streams).collect(),
        };

        Ok(())
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
                            transport: Some(transport.into_inner()),
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

const WRITE_PREFACE: &str = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Debug)]
struct FrameParser {
    buf: Vec<u8>,
    state: FrameParserState,
    out: Vec<Http2FrameOutput>,
}

impl FrameParser {
    fn new(state: FrameParserState) -> Self {
        Self {
            buf: Vec::new(),
            state,
            out: Vec::new(),
        }
    }

    fn push(&mut self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match &self.state {
                FrameParserState::Preface(remaining) => {
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
                    let to_copy = (len - self.buf.len()).min(buf.len());
                    self.buf.extend_from_slice(&buf[..to_copy]);
                    if self.buf.len() < *len {
                        break;
                    }
                    buf = &buf[to_copy..];
                    self.out.push(Http2FrameOutput::new(
                        *kind, *flags, *r, *stream_id, &self.buf,
                    ));
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
    fn new(transport: Runner) -> Self {
        Self {
            transport,
            write: FrameParser::new(FrameParserState::Preface(WRITE_PREFACE.as_bytes())),
            read: FrameParser::new(FrameParserState::FrameHeader),
        }
    }

    fn into_inner(self) -> Runner {
        self.transport
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
