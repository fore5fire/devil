use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use super::http1::HTTP1Runner;
use super::runner::Runner;
use super::tcp::TCPRunner;
use super::tls::TLSRunner;
use crate::Error;
use crate::HTTPOutput;
use crate::HTTPResponse;
use crate::Output;
use crate::{HTTPRequestOutput, TCPRequestOutput, TLSRequestOutput};

#[derive(Debug)]
pub(super) enum HTTPRunner {
    HTTP1(HTTP1Runner<Box<dyn Runner>>),
}

impl AsyncRead for HTTPRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::HTTP1(ref mut r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for HTTPRunner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::HTTP1(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::HTTP1(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::HTTP1(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl HTTPRunner {
    pub(super) async fn new(req: HTTPRequestOutput) -> crate::Result<Self> {
        // For now we always use TCP and possibly TLS. To support HTTP/3 we'll need to decide
        // whether to use UPD and QUIC instead.
        let tcp: Box<dyn Runner> = Box::new(
            TCPRunner::new(TCPRequestOutput {
                host: req
                    .url
                    .host()
                    .ok_or_else(|| Error::from("url is missing host"))?
                    .to_string(),
                port: req
                    .url
                    .port_or_known_default()
                    .ok_or_else(|| Error::from("url is missing port"))?,
                body: Vec::new(),
                pause: Vec::new(),
            })
            .await?,
        );

        let inner = if req.url.scheme() == "http" {
            tcp
        } else {
            Box::new(
                TLSRunner::new(
                    tcp,
                    TLSRequestOutput {
                        host: req
                            .url
                            .host()
                            .ok_or_else(|| Error::from("url is missing host"))?
                            .to_string(),
                        port: req
                            .url
                            .port_or_known_default()
                            .ok_or_else(|| Error::from("url is missing port"))?,
                        body: Vec::new(),
                        pause: Vec::new(),
                    },
                )
                .await?,
            ) as Box<dyn Runner>
        };

        Ok(HTTPRunner::HTTP1(
            HTTP1Runner::new(
                inner as Box<dyn Runner>,
                crate::HTTP1RequestOutput {
                    url: req.url,
                    method: req.method,
                    version_string: Some("HTTP/1.1".into()),
                    headers: req.headers,
                    body: req.body,
                    pause: req.pause,
                },
            )
            .await?,
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
        let (out, inner) = match self {
            Self::HTTP1(r) => r.finish().await?,
        };
        Ok((
            match out {
                Output::HTTP1(out) => Output::HTTP(HTTPOutput {
                    request: HTTPRequestOutput {
                        url: out.request.url,
                        method: out.request.method,
                        headers: out.request.headers,
                        body: out.request.body,
                        pause: out.request.pause,
                    },
                    response: HTTPResponse {
                        protocol: out.response.protocol,
                        status_code: out.response.status_code,
                        headers: out.response.headers,
                        body: out.response.body,
                        duration: out.response.duration,
                    },
                    protocol: "HTTP/1.1".to_string(),
                }),
                _ => return Err(Error::from("unexpected output")),
            },
            inner,
        ))
    }

    fn size_hint(&mut self, size: usize) {
        match self {
            Self::HTTP1(r) => r.size_hint(size),
        }
    }
}
