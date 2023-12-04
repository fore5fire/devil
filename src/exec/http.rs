use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

use super::http1::HTTP1Runner;
use super::tcp::TCPRunner;
use super::tls::TLSRunner;
use crate::Error;
use crate::TLSOutput;
use crate::{HTTPOutput, TCPOutput};

#[derive(Debug)]
enum HTTPStream {
    TCP(TCPRunner),
    TLS(TLSRunner<TCPRunner>),
}

impl AsyncRead for HTTPStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::TCP(mut s) => Pin::new(&mut s).poll_read(cx, buf),
            Self::TLS(mut s) => Pin::new(&mut s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for HTTPStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::TCP(mut s) => Pin::new(&mut s).poll_write(cx, buf),
            Self::TLS(mut s) => Pin::new(&mut s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::TCP(mut s) => Pin::new(&mut s).poll_flush(cx),
            Self::TLS(mut s) => Pin::new(&mut s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::TCP(mut s) => Pin::new(&mut s).poll_shutdown(cx),
            Self::TLS(mut s) => Pin::new(&mut s).poll_shutdown(cx),
        }
    }
}

#[derive(Debug)]
pub(super) enum HTTPRunner {
    HTTP1(HTTP1Runner<HTTPStream>),
}

impl AsyncRead for HTTPRunner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Read the first bytes of the response as the header.
        if self.out.response.is_none() {
            let buf = self.as_ref().receive_header();
        }
        Pin::new(&mut self.as_ref().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for HTTPRunner {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_shutdown(cx),
        }
    }
}

impl HTTPRunner {
    pub(super) async fn new(data: HTTPOutput) -> crate::Result<Self> {
        // For now we always use TCP and possibly TLS. To support HTTP/3 we'll need to decide
        // whether to use UPD and QUIC instead.
        let tcp = TCPRunner::new(TCPOutput {
            host: data
                .url
                .host()
                .ok_or_else(|| Error::from("url is missing host"))?
                .to_string(),
            port: data
                .url
                .port_or_known_default()
                .ok_or_else(|| Error::from("url is missing port"))?,
            body: Vec::new(),
            pause: Vec::new(),
            response: None,
        })
        .await?;

        let stream = if data.url.scheme() == "http" {
            HTTPStream::TCP(tcp)
        } else {
            HTTPStream::TLS(
                TLSRunner::new(
                    tcp,
                    TLSOutput {
                        host: data
                            .url
                            .host()
                            .ok_or_else(|| Error::from("url is missing host"))?
                            .to_string(),
                        port: data
                            .url
                            .port_or_known_default()
                            .ok_or_else(|| Error::from("url is missing port"))?,
                        version: None,
                        body: Vec::new(),
                        pause: Vec::new(),
                        response: None,
                    },
                )
                .await?,
            )
        };

        HTTPRunner::HTTP1(HTTP1Runner::new(stream, data))
    }

    pub(super) async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::HTTP1(r) => r.execute().await,
        }
    }

    pub(super) async fn finish(mut self) -> crate::Result<HTTPOutput> {
        match self {
            Self::HTTP1(r) => r.finish().await,
        }
    }
}
