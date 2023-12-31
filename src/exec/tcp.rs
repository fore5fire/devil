use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{
    Error, Output, PauseOutput, TcpError, TcpOutput, TcpPlanOutput, TcpRequestOutput, TcpResponse,
    WithPlannedCapacity,
};

use super::runner::Runner;
use super::tee::Tee;

#[derive(Debug)]
pub(super) struct TcpRunner {
    out: TcpOutput,
    stream: Tee<TcpStream>,
    start: Instant,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    end_time: Option<Instant>,
    error: Option<TcpError>,
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.first_read.is_none() {
            self.first_read = Some(Instant::now());
        }
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        self.last_read = Some(Instant::now());
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

impl<'a> TcpRunner {
    pub(super) async fn new(plan: TcpPlanOutput) -> crate::Result<TcpRunner> {
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
            let before = Instant::now();
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
            pause.after.handshake.push(crate::PauseValueOutput {
                duration: Duration::from_std(before.elapsed()).unwrap(),
                offset_bytes: p.offset_bytes,
            });
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
            stream: Tee::new(stream),
            start,
            first_write: None,
            last_write: None,
            first_read: None,
            last_read: None,
            end_time: None,
            error: None,
        })
    }
}

#[async_trait]
impl Runner for TcpRunner {
    async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn execute(&mut self) {
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

    async fn finish(mut self: Box<Self>) -> (Output, Option<Box<dyn Runner>>) {
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
