use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use chrono::Duration;
use futures::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{
    Error, Output, PauseOutput, TcpError, TcpOutput, TcpPlanOutput, TcpRequestOutput, TcpResponse,
    WithPlannedCapacity,
};

use super::pause::Pause;
use super::runner::Runner;
use super::tee::Tee;
use super::Context;

#[derive(Debug)]
pub(super) struct TcpRunner {
    ctx: Arc<Context>,
    out: TcpOutput,
    stream: Tee<TcpStream>,
    start: Instant,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    end_time: Option<Instant>,
    error: Option<TcpError>,
    size_hint: Option<usize>,
    bytes_read: i64,
    read_pauses: Vec<Pause>,
    read_pauses_calculated: bool,
    bytes_written: usize,
    write_pauses: Vec<Pause>,
    write_pauses_calculated: bool,
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Look for required pauses before reading any more bytes.
        let mut pauses = std::mem::take(&mut self.read_pauses);
        if !self.read_pauses_calculated {
            pauses.extend(
                self.out
                    .plan
                    .pause
                    .before
                    .first_read
                    .iter()
                    .filter(|p| p.offset_bytes == self.bytes_read)
                    .map(|p| Pause::new(&self.ctx, p)),
            );
            println!(
                "calculated required pauses: {pauses:?}, time {:?}",
                Instant::now(),
            );
            self.read_pauses_calculated = true
        }

        // Execute any pending pauses.
        self.out.pause.before.first_read.reserve(pauses.len());
        for i in 0..pauses.len() {
            match pauses[i].poll_unpin(cx) {
                Poll::Ready(actual) => self.out.pause.before.first_read.push(actual?),
                Poll::Pending => {
                    // Remove the completed pauses so we don't double record them.
                    pauses.drain(0..i);
                    self.read_pauses = pauses;
                    return Poll::Pending;
                }
            }
        }

        // Pending pauses finished - save the empty pauses vec so we can reuse its allocated
        // memory.
        pauses.clear();
        self.read_pauses = pauses;

        // Don't read more bytes than we need to get to the next pause.
        let read_len = self
            .out
            .plan
            .pause
            .before
            .first_read
            .iter()
            .find(|p| p.offset_bytes > self.bytes_read)
            .map(|p| p.offset_bytes - self.bytes_read)
            .map(usize::try_from)
            .transpose()
            .expect("bytes to read should fit in a usize")
            .unwrap_or(usize::MAX);
        let mut sub_buf = buf.take(read_len);

        // Read some data.
        let result = Pin::new(&mut self.stream).poll_read(cx, &mut sub_buf);

        // Record the time of this read.
        self.last_read = Some(Instant::now());
        if self.first_read.is_none() {
            self.first_read = self.last_read;
        }

        let bytes_read = sub_buf.filled().len();

        // sub_buf started assuming it was fully uninitialized, and any writes to it would have
        // initialized the shared memory in buf.
        let sub_buf_initialized = sub_buf.initialized().len();
        unsafe { buf.assume_init(buf.filled().len() + sub_buf_initialized) }
        buf.advance(bytes_read);

        // If bytes were read then signal to look for more matching pauses on the next read.
        if bytes_read > 0 {
            self.read_pauses_calculated = false;
        }

        // Record the newly read bytes.
        self.bytes_read += i64::try_from(bytes_read).expect("too many bytes written");

        result
    }
}

impl AsyncWrite for TcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        if self.first_write.is_none() {
            self.first_write = Some(Instant::now());
        }
        let result = Pin::new(&mut self.stream).poll_write(cx, buf);
        self.last_write = Some(Instant::now());
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let poll = Pin::new(&mut self.stream).poll_shutdown(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.end_time = Some(Instant::now());
        }
        poll
    }
}

impl Unpin for TcpRunner {}

impl TcpRunner {
    pub(super) async fn new(ctx: Arc<Context>, plan: TcpPlanOutput) -> crate::Result<TcpRunner> {
        //let addr = ip_for_host(&host).await?;
        let addr = format!("{}:{}", plan.host, plan.port);
        let mut pause = PauseOutput::with_planned_capacity(&plan.pause);
        let start = Instant::now();
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error(e.to_string()))?;
        let handshake_duration = start.elapsed();
        for p in &plan.pause.after.handshake {
            println!("pausing after tcp handshake for {:?}", p.duration);
        }
        Ok(TcpRunner {
            out: TcpOutput {
                request: Some(TcpRequestOutput {
                    host: plan.host.clone(),
                    port: plan.port,
                    body: Vec::new(),
                    time_to_first_byte: None,
                    time_to_last_byte: None,
                }),
                plan,
                response: None,
                error: None,
                duration: Duration::zero(),
                handshake_duration: Some(Duration::from_std(handshake_duration).unwrap()),
                pause,
            },
            ctx,
            stream: Tee::new(stream),
            start,
            first_write: None,
            last_write: None,
            first_read: None,
            last_read: None,
            end_time: None,
            error: None,
            bytes_read: 0,
            bytes_written: 0,
            read_pauses: Vec::new(),
            write_pauses: Vec::new(),
            read_pauses_calculated: false,
            write_pauses_calculated: false,
            size_hint: None,
        })
    }

    pub async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.size_hint = size_hint;
        Ok(())
    }

    pub async fn execute(&mut self) {
        if let Err(e) = self.stream.write_all(&self.out.plan.body).await {
            self.error = Some(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        };
        if let Err(e) = self.stream.flush().await {
            self.error = Some(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.stream.read_to_end(&mut response).await {
            self.error = Some(TcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
    }

    pub async fn finish(mut self) -> (Output, Option<Runner>) {
        let end_time = Instant::now();
        let (_, writes, reads) = self.stream.into_parts();

        if let Some(req) = &mut self.out.request {
            if let Some(first_write) = self.first_write {
                req.time_to_first_byte =
                    Some(chrono::Duration::from_std(first_write - self.start).unwrap());
            }
            if let Some(last_write) = self.first_write {
                req.time_to_last_byte =
                    Some(chrono::Duration::from_std(last_write - self.start).unwrap());
            }
            req.body = writes;
        }
        if !reads.is_empty() {
            self.out.response = Some(TcpResponse {
                body: reads,
                time_to_first_byte: self
                    .first_read
                    .map(|first_read| first_read - self.start)
                    .map(Duration::from_std)
                    .transpose()
                    .unwrap(),
                time_to_last_byte: self
                    .last_read
                    .map(|last_read| last_read - self.start)
                    .map(Duration::from_std)
                    .transpose()
                    .unwrap(),
            });
        }
        self.out.duration =
            chrono::Duration::from_std(self.end_time.unwrap_or(end_time) - self.start).unwrap();
        (Output::Tcp(self.out), None)
    }
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
