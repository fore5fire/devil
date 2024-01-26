use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use super::runner::Runner;
use super::tcp::TcpRunner;
use super::tls::TlsRunner;
use super::{http1::Http1Runner, Context};
use crate::{
    Error, HttpOutput, HttpPauseOutput, HttpPlanOutput, HttpRequestOutput, HttpResponse, Output,
    PauseOutput, TcpPlanOutput, TlsPlanOutput,
};

#[derive(Debug)]
pub(super) enum HttpRunner {
    Http1(Http1Runner),
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
    pub(super) fn new(ctx: Arc<Context>, plan: HttpPlanOutput) -> crate::Result<Self> {
        // For now we always use TCP and possibly TLS. To support HTTP/3 we'll need to decide
        // whether to use UPD and QUIC instead.
        let tcp = Runner::Tcp(Box::new(TcpRunner::new(
            ctx.clone(),
            TcpPlanOutput {
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
                pause: PauseOutput::default(),
            },
        )));

        let inner = if plan.url.scheme() == "http" {
            tcp
        } else {
            Runner::Tls(Box::new(TlsRunner::new(
                ctx.clone(),
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
                    pause: PauseOutput::default(),
                },
            )))
        };

        Ok(HttpRunner::Http1(Http1Runner::new(
            ctx,
            inner,
            crate::Http1PlanOutput {
                url: plan.url,
                method: plan.method,
                version_string: Some("HTTP/1.1".into()),
                headers: plan.headers,
                body: plan.body,
                pause: PauseOutput {
                    before: crate::Http1PauseOutput {
                        open: plan.pause.before.open,
                        request_headers: plan.pause.before.request_headers,
                        request_body: plan.pause.before.request_body,
                        response_headers: plan.pause.before.response_headers,
                        response_body: plan.pause.before.response_body,
                    },
                    after: crate::Http1PauseOutput {
                        open: plan.pause.after.open,
                        request_headers: plan.pause.after.request_headers,
                        request_body: plan.pause.after.request_body,
                        response_headers: plan.pause.after.response_headers,
                        response_body: plan.pause.after.response_body,
                    },
                },
            },
        )))
    }

    pub async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Http1(r) => r.start(size_hint).await,
        }
    }

    pub async fn execute(&mut self) {
        match self {
            Self::Http1(r) => r.execute().await,
        }
    }

    pub fn finish(self) -> (Output, Runner) {
        let (out, inner) = match self {
            Self::Http1(r) => r.finish(),
        };
        (
            match out {
                Output::Http1(out) => Output::Http(HttpOutput {
                    plan: HttpPlanOutput {
                        url: out.plan.url,
                        method: out.plan.method,
                        headers: out.plan.headers,
                        body: out.plan.body,
                        pause: PauseOutput {
                            before: HttpPauseOutput {
                                open: out.plan.pause.before.open,
                                request_headers: out.plan.pause.before.request_headers,
                                request_body: out.plan.pause.before.request_body,
                                response_headers: out.plan.pause.before.response_headers,
                                response_body: out.plan.pause.before.response_body,
                            },
                            after: HttpPauseOutput {
                                open: out.plan.pause.after.open,
                                request_headers: out.plan.pause.after.request_headers,
                                request_body: out.plan.pause.after.request_body,
                                response_headers: out.plan.pause.after.response_headers,
                                response_body: out.plan.pause.after.response_body,
                            },
                        },
                    },
                    request: out.request.map(|req| HttpRequestOutput {
                        url: req.url,
                        method: req.method,
                        headers: req.headers,
                        body: req.body,
                        duration: req.duration,
                        body_duration: req.body_duration,
                        time_to_first_byte: req.time_to_first_byte,
                    }),
                    response: out.response.map(|resp| HttpResponse {
                        protocol: resp.protocol,
                        status_code: resp.status_code,
                        headers: resp.headers,
                        body: resp.body,
                        duration: resp.duration,
                        header_duration: resp.header_duration,
                        time_to_first_byte: resp.time_to_first_byte,
                    }),
                    error: out.error.map(|e| crate::HttpError {
                        kind: e.kind,
                        message: e.message,
                    }),
                    protocol: Some("HTTP/1.1".to_string()),
                    duration: out.duration,
                    pause: PauseOutput {
                        before: HttpPauseOutput {
                            open: out.pause.before.open,
                            request_headers: out.pause.before.request_headers,
                            request_body: out.pause.before.request_body,
                            response_headers: out.pause.before.response_headers,
                            response_body: out.pause.before.response_body,
                        },
                        after: HttpPauseOutput {
                            open: out.pause.after.open,
                            request_headers: out.pause.after.request_headers,
                            request_body: out.pause.after.request_body,
                            response_headers: out.pause.after.response_headers,
                            response_body: out.pause.after.response_body,
                        },
                    },
                }),
                _ => unreachable!(),
            },
            inner,
        )
    }
}
