use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{Error, PauseOutput, TCPOutput, TCPRequest, TCPResponse};

use super::{State, StepOutput};

pub(super) struct TCPRunner {
    out: TCPOutput,
    stream: TcpStream,
    start: Instant,
    host: String,
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

impl Unpin for TCPRunner {}

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
    pub(super) async fn new(data: TCPOutput, state: &State<'_>) -> crate::Result<TCPRunner> {
        //let addr = ip_for_host(&host).await?;
        let addr = format!("{}:{}", data.host, data.port);
        let start = Instant::now();
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error(e.to_string()))?;
        Ok(TCPRunner {
            stream,
            start,
            host: format!("{}:{}", data.host, data.port),
            out: data,
        })
    }

    pub(super) async fn execute(
        &mut self,
        tcp: &TCPRequest,
        state: &State<'_>,
    ) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        self.stream.write_all(&self.out.body).await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        //let stream = Tee::new(stream);

        Ok(StepOutput {
            tcp: Some(TCPOutput {
                host,
                port: port.parse()?,
                body,
                pause,
                response: Some(TCPResponse {
                    body: response,
                    duration: start.elapsed(),
                }),
            }),
            ..Default::default()
        })
    }
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
