use std::sync::Arc;
use std::task::{ready, Poll};
use std::time::Instant;
use std::{mem, pin::pin};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use cel_interpreter::Duration;
use chrono::TimeDelta;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpSocket, TcpStream};
use tokio::spawn;

use crate::{MaybeUtf8, TcpError, TcpOutput, TcpPlanOutput, TcpReceivedOutput, TcpSentOutput};

use super::pause::{PauseReader, PauseSpec, PauseWriter};
use super::raw_tcp::RawTcpRunner;
use super::tee::{self, TeeReader, TeeWriter};
use super::timing::{TimingReader, TimingWriter};
use super::{Context, Error};

#[derive(Debug)]
pub(super) struct TcpRunner {
    ctx: Arc<Context>,
    out: TcpOutput,
    state: State,
    size_hint: Option<usize>,
    reader: Option<TcpRunnerReader>,
}

#[derive(Debug)]
pub enum State {
    Pending,
    Open {
        start: Instant,
        writer: PauseWriter<BufWriter<TeeWriter<TimingWriter<WriteHalf<TcpStream>>>>>,
        size_hint: Option<usize>,
        raw: RawTcpRunner,
    },
    Completed,
    Invalid,
}

impl TcpRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TcpPlanOutput) -> TcpRunner {
        TcpRunner {
            state: State::Pending,
            reader: None,
            out: TcpOutput {
                sent: None,
                plan,
                received: None,
                //close: TcpCloseOutput::default(),
                errors: Vec::new(),
                duration: TimeDelta::zero().into(),
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

    pub async fn start(&mut self, raw: RawTcpRunner) -> anyhow::Result<()> {
        let State::Pending = mem::replace(&mut self.state, State::Invalid) else {
            panic!("invalid state to start tcp {:?}", self.state)
        };

        let (local_addr, remote_addr) = raw.resolved_addrs();
        let remote_addr_string = remote_addr.ip().to_string();

        self.out.sent = Some(TcpSentOutput {
            dest_ip: remote_addr_string,
            dest_port: remote_addr.port(),
            body: MaybeUtf8::default(),
            time_to_first_byte: None,
            time_to_last_byte: None,
        });

        let start = Instant::now();
        let socket = TcpSocket::new_v4().inspect_err(|e| {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::Completed;
        })?;
        socket.bind(local_addr);
        let transport = match socket.connect(remote_addr).await {
            Ok(t) => t,
            Err(e) => {
                self.out.errors.push(TcpError {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                self.state = State::Completed;
                bail!("connect to {remote_addr}: {e}");
            }
        };
        let (reader, writer) = tokio::io::split(transport);

        let tee_reader = TeeReader::new(TimingReader::new(reader));
        //if let Some(limit) = self.out.plan.close.bytes {
        //    tee_reader.set_read_limit(limit.try_into()?);
        //}
        //if let Some(pattern) = &self.out.plan.close.pattern {
        //    tee_reader.set_pattern(
        //        Some(pattern.parsed.clone()),
        //        self.out
        //            .plan
        //            .close
        //            .pattern_window
        //            .map(|window| window.try_into())
        //            .transpose()?,
        //    );
        //}

        self.state = State::Open {
            raw,
            start,
            size_hint: self.size_hint,
            writer: PauseWriter::new(
                self.ctx.clone(),
                BufWriter::new(TeeWriter::new(TimingWriter::new(writer))),
                vec![], //if let Some(size) = self.size_hint {
                        //    vec![
                        //        PauseSpec {
                        //            group_offset: 0,
                        //            plan: pause.send_body.start,
                        //        },
                        //        PauseSpec {
                        //            group_offset: size.try_into().unwrap(),
                        //            plan: pause.send_body.end,
                        //        },
                        //    ]
                        //} else {
                        //    if !pause.send_body.end.is_empty() {
                        //        bail!("tcp.pause.send_body.end is unsupported in this request");
                        //    }
                        //    vec![PauseSpec {
                        //        group_offset: 0,
                        //        plan: pause.send_body.start,
                        //    }]
                        //},
            ),
        };
        self.reader = Some(TcpRunnerReader::new(PauseReader::new(
            self.ctx.clone(),
            tee_reader,
            // TODO: implement read size hints.
            vec![/*PauseSpec {
                group_offset: 0,
                plan: pause.receive_body.start,
            }*/],
        )));

        Ok(())
    }

    pub async fn execute(&mut self) {
        let mut reader =
            mem::take(&mut self.reader).expect("reader should be set for call to take_reader");

        // Setup signal to close the write half of the connection.
        //let done = CancellationToken::new();
        //let is_done = done.clone();
        //if self.out.plan.close.pattern.is_none()
        //    && self.out.plan.close.timeout.is_none()
        //    && self.out.plan.close.bytes.is_none()
        //{
        //    done.cancel();
        //}
        let handle = spawn(async move {
            //defer! {
            //    done.cancel();
            //}
            let mut buf = [0; 512];
            loop {
                // Read and ignore the data since its already recorded by TeeReader.
                match reader.read(&mut buf).await {
                    Ok(size) if size == 0 => {
                        return (reader, Ok(()));
                    }
                    //Err(e) => match e.downcast::<Error>() {
                    //    Ok(Error::Done) => done.cancel(),
                    Err(e) => {
                        return (reader, Err(e));
                    }
                    //},
                    _ => {}
                }
            }
        });

        let body = std::mem::take(&mut self.out.plan.body);
        if let Err(e) = self.write_all(&body).await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        };
        self.out.plan.body = body;
        if let Err(e) = self.flush().await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        }
        //is_done.cancelled().await;
        if let Err(e) = &self.shutdown().await {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        }
        let (reader, read_result) = handle.await.expect("tcp reader should not panic");
        if let Err(e) = read_result {
            self.out.errors.push(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
        }
        self.reader = Some(reader);
    }

    pub async fn finish(mut self) -> (TcpOutput, RawTcpRunner) {
        let end_time = Instant::now();
        self.complete();

        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Open {
            start, writer, raw, ..
        } = state
        else {
            panic!("invalid tcp runner state after complete");
        };
        let Some(reader) = mem::take(&mut self.reader) else {
            panic!("reader unset in Open state");
        };

        // TODO: how to sort out which pause outputs came from first or last?
        let (writer, send_pause) = writer.finish();
        let writer = writer.into_inner();
        let (writer, writes) = writer.into_parts();

        //let recv_max_reached = reader.recv_max_reached;
        //let read_timed_out = reader.timed_out;

        let (reader, receive_pause) = reader.inner.finish();
        let (reader, reads, truncated_reads, pattern_match) = reader.into_parts();

        let end_time = writer.shutdown_end().unwrap_or(end_time);

        let mut receive_pause = receive_pause.into_iter();
        //self.out.pause.receive_body.start = receive_pause.next().unwrap_or_default();
        //self.out.pause.receive_body.end = receive_pause.next().unwrap_or_default();
        let mut send_pause = send_pause.into_iter();
        //self.out.pause.send_body.start = send_pause.next().unwrap_or_default();
        //self.out.pause.send_body.end = send_pause.next().unwrap_or_default();

        //self.out.close = TcpCloseOutput {
        //    timed_out: read_timed_out,
        //    recv_max_reached,
        //    pattern_match: pattern_match.map(|range| reads[range].to_owned()),
        //};

        if let Some(sent) = &mut self.out.sent {
            if let Some(first_write) = writer.first_write() {
                sent.time_to_first_byte =
                    Some(TimeDelta::from_std(first_write - start).unwrap().into());
            }
            if let Some(last_write) = writer.last_write() {
                sent.time_to_last_byte =
                    Some(TimeDelta::from_std(last_write - start).unwrap().into());
            }
            sent.body = MaybeUtf8(Bytes::from(writes).into());
        }
        if !reads.is_empty() {
            self.out.received = Some(TcpReceivedOutput {
                body: MaybeUtf8(Bytes::from(reads).into()),
                time_to_first_byte: reader
                    .first_read()
                    .map(|first_read| first_read - start)
                    .map(TimeDelta::from_std)
                    .transpose()
                    .unwrap()
                    .map(Duration),
                time_to_last_byte: reader
                    .last_read()
                    .map(|last_read| last_read - start)
                    .map(TimeDelta::from_std)
                    .transpose()
                    .unwrap()
                    .map(Duration),
            });
        }
        self.out.duration = TimeDelta::from_std(end_time - start).unwrap().into();
        self.state = State::Completed;
        (self.out, raw)
    }

    fn complete(&mut self) {
        let State::Open { writer, raw, .. } = &mut self.state else {
            return;
        };
        raw.shutdown(
            self.reader
                .as_ref()
                .map(|r| r.inner.inner_ref().reads.len())
                .unwrap_or_default(),
            writer.inner_ref().get_ref().writes.len(),
        );
    }
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let Some(reader) = &mut self.reader else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot read from stream in {:?} state",
                self.state
            ))));
        };
        pin!(reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let State::Open { writer, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot write to stream in {:?} state",
                self.state
            ))));
        };
        pin!(writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { writer, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot flush stream in {:?} state",
                self.state
            ))));
        };
        std::pin::pin!(writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { writer, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot shutdown stream in {:?} state",
                self.state
            ))));
        };
        ready!(pin!(writer).poll_shutdown(cx))?;
        self.complete();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct TcpRunnerReader {
    inner: PauseReader<TeeReader<TimingReader<ReadHalf<TcpStream>>>>,
    recv_max_reached: bool,
    timed_out: bool,
}

impl TcpRunnerReader {
    fn new(inner: PauseReader<TeeReader<TimingReader<ReadHalf<TcpStream>>>>) -> Self {
        Self {
            inner,
            recv_max_reached: false,
            timed_out: false,
        }
    }
}

impl AsyncRead for TcpRunnerReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Read some data.
        let reader = &mut self.inner;
        let Err(e) = ready!(pin!(reader).poll_read(cx, buf)) else {
            return Poll::Ready(Ok(()));
        };
        // Handle errors which signal clean shutdown.
        match e.downcast::<tee::Error>() {
            Ok(e) => {
                if matches!(e, tee::Error::LimitReached) {
                    self.recv_max_reached = true;
                }
                return Poll::Ready(Err(std::io::Error::other(Error::Done)));
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Unpin for TcpRunner {}
