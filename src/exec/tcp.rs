use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{Error, TCPOutput, TCPResponse};

use super::tee::Tee;

pub(super) struct TCPRunner {
    out: TCPOutput,
    stream: Tee<TcpStream>,
    start: Instant,
}

impl AsyncRead for TCPRunner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_ref().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TCPRunner {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_shutdown(cx)
    }
}

impl<'a> TCPRunner {
    pub(super) async fn new(data: TCPOutput) -> crate::Result<TCPRunner> {
        //let addr = ip_for_host(&host).await?;
        let start = Instant::now();
        let addr = format!("{}:{}", data.host, data.port);
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error(e.to_string()))?;
        if let Some(p) = data.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        Ok(TCPRunner {
            stream: Tee::new(stream),
            start,
            out: data,
        })
    }

    pub(super) async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stream.write_all(&self.out.body).await?;
        self.stream.flush().await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        Ok(())
    }

    pub(super) async fn finish(mut self) -> TCPOutput {
        let (_, writes, reads) = self.stream.into_parts();

        self.out.body = writes;
        self.out.response = Some(TCPResponse {
            body: reads,
            duration: self.start.elapsed(),
        });
        self.out
    }
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}