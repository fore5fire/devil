use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use super::http1::HTTP1Runner;
use super::runner::Runner;
use super::tcp::TCPRunner;
use super::tls::TLSRunner;
use crate::Error;
use crate::Output;
use crate::TLSOutput;
use crate::{HTTPOutput, TCPOutput};

#[derive(Debug)]
pub(super) enum HTTPRunner {
    HTTP1(HTTP1Runner<Box<dyn Runner>>),
}

impl AsyncRead for HTTPRunner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::HTTP1(mut r) => Pin::new(&mut r).poll_read(cx, buf),
        }
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
        let tcp: Box<dyn Runner> = Box::new(
            TCPRunner::new(TCPOutput {
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
            .await?,
        );

        let inner = if data.url.scheme() == "http" {
            tcp
        } else {
            Box::new(
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
            ) as Box<dyn Runner>
        };

        Ok(HTTPRunner::HTTP1(
            HTTP1Runner::new(inner as Box<dyn Runner>, data).await?,
        ))
    }
}

#[async_trait]
impl Runner for HTTPRunner {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::HTTP1(r) => r.execute().await,
        }
    }

    async fn finish(mut self) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        match self {
            Self::HTTP1(r) => r.finish().await,
        }
    }

    fn size_hint(&mut self, size: usize) {
        match self {
            Self::HTTP1(r) => r.size_hint(size),
        }
    }
}
