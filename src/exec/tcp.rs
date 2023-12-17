use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{Error, Output, TcpOutput, TcpRequestOutput, TcpResponse};

use super::runner::Runner;
use super::tee::Tee;

#[derive(Debug)]
pub(super) struct TcpRunner {
    req: TcpRequestOutput,
    stream: Tee<TcpStream>,
    start: Instant,
}

impl AsyncRead for TcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
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
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Unpin for TcpRunner {}

impl<'a> TcpRunner {
    pub(super) async fn new(req: TcpRequestOutput) -> crate::Result<TcpRunner> {
        //let addr = ip_for_host(&host).await?;
        let start = Instant::now();
        let addr = format!("{}:{}", req.host, req.port);
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error(e.to_string()))?;
        if let Some(p) = req.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
        }
        Ok(TcpRunner {
            stream: Tee::new(stream),
            start,
            req,
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

    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stream.write_all(&self.req.body).await?;
        self.stream.flush().await?;
        if let Some(p) = self.req.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        let (_, writes, reads) = self.stream.into_parts();

        self.req.body = writes;
        Ok((
            Output::Tcp(TcpOutput {
                request: self.req,
                response: TcpResponse {
                    body: reads,
                    duration: chrono::Duration::from_std(self.start.elapsed()).unwrap(),
                },
            }),
            None,
        ))
    }
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
