use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use super::http1::Http1Runner;
use super::runner::Runner;
use super::tcp::TcpRunner;
use super::tls::TlsRunner;
use crate::{
    Error, HttpOutput, HttpPlanOutput, HttpRequestOutput, HttpResponse, Output, TcpPlanOutput,
    TlsPlanOutput,
};

#[derive(Debug)]
pub(super) enum HttpRunner {
    Http1(Box<Http1Runner>),
}

impl AsyncRead for HttpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::Http1(ref mut r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for HttpRunner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::Http1(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::Http1(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::Http1(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl HttpRunner {
    pub(super) async fn new(plan: HttpPlanOutput) -> crate::Result<Self> {
        // For now we always use TCP and possibly TLS. To support HTTP/3 we'll need to decide
        // whether to use UPD and QUIC instead.
        let tcp: Box<dyn Runner> = Box::new(
            TcpRunner::new(TcpPlanOutput {
                host: plan
                    .url
                    .host()
                    .ok_or_else(|| Error("url is missing host".to_owned()))?
                    .to_string(),
                port: plan
                    .url
                    .port_or_known_default()
                    .ok_or_else(|| Error("url is missing port".to_owned()))?,
                body: Vec::new(),
                pause: Vec::new(),
            })
            .await?,
        );

        let inner = if plan.url.scheme() == "http" {
            tcp
        } else {
            Box::new(
                TlsRunner::new(
                    tcp,
                    TlsPlanOutput {
                        host: plan
                            .url
                            .host()
                            .ok_or_else(|| Error("url is missing host".to_owned()))?
                            .to_string(),
                        port: plan
                            .url
                            .port_or_known_default()
                            .ok_or_else(|| Error("url is missing port".to_owned()))?,
                        body: Vec::new(),
                        pause: Vec::new(),
                    },
                )
                .await?,
            ) as Box<dyn Runner>
        };

        Ok(HttpRunner::Http1(Box::new(
            Http1Runner::new(
                inner as Box<dyn Runner>,
                crate::Http1PlanOutput {
                    url: plan.url,
                    method: plan.method,
                    version_string: Some("HTTP/1.1".into()),
                    headers: plan.headers,
                    body: plan.body,
                    pause: plan.pause,
                },
            )
            .await?,
        )))
    }
}

#[async_trait]
impl Runner for HttpRunner {
    async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Http1(r) => r.start(size_hint).await,
        }
    }

    async fn execute(&mut self) {
        match self {
            Self::Http1(r) => r.execute().await,
        }
    }

    async fn finish(self: Box<Self>) -> (Output, Option<Box<dyn Runner>>) {
        let (out, inner) = match *self {
            Self::Http1(r) => r.finish().await,
        };
        (
            match out {
                Output::Http1(out) => Output::Http(HttpOutput {
                    plan: HttpPlanOutput {
                        url: out.plan.url,
                        method: out.plan.method,
                        headers: out.plan.headers,
                        body: out.plan.body,
                        pause: out.plan.pause,
                    },
                    request: out.request.map(|req| HttpRequestOutput {
                        url: req.url,
                        method: req.method,
                        headers: req.headers,
                        body: req.body,
                        pause: req.pause,
                        duration: req.duration,
                        header_duration: req.header_duration,
                        body_duration: req.body_duration,
                    }),
                    response: out.response.map(|resp| HttpResponse {
                        protocol: resp.protocol,
                        status_code: resp.status_code,
                        headers: resp.headers,
                        body: resp.body,
                        duration: resp.duration,
                        header_duration: resp.header_duration,
                        body_duration: resp.body_duration,
                    }),
                    error: out.error.map(|e| crate::HttpError {
                        kind: e.kind,
                        message: e.message,
                    }),
                    protocol: Some("HTTP/1.1".to_string()),
                    duration: out.duration,
                }),
                _ => unreachable!(),
            },
            inner,
        )
    }
}
