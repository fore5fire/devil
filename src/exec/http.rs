use std::mem;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use tokio::io::{AsyncRead, AsyncWrite};

use super::raw_tcp::RawTcpRunner;
use super::runner::Runner;
use super::tcp::TcpRunner;
use super::tls::TlsRunner;
use super::{http1::Http1Runner, Context};
use crate::{
    HttpOutput, HttpPauseOutput, HttpPlanOutput, HttpRequestOutput, HttpResponse, RawTcpPlanOutput,
    TcpPlanOutput, TlsPlanOutput,
};

#[derive(Debug)]
pub(super) struct HttpRunner {
    inner: HttpProtocol,
    state: State,
}

#[derive(Debug)]
enum State {
    Pending { transports: Vec<Runner> },
    Running,
    Invalid,
}

#[derive(Debug)]
enum HttpProtocol {
    Http1(Http1Runner),
}

impl AsyncRead for HttpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.inner {
            HttpProtocol::Http1(ref mut r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for HttpRunner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.inner {
            HttpProtocol::Http1(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.inner {
            HttpProtocol::Http1(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.inner {
            HttpProtocol::Http1(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl HttpRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: HttpPlanOutput) -> crate::Result<Self> {
        let mut transports = if plan.url.scheme() == "https" {
            Vec::with_capacity(2)
        } else {
            Vec::with_capacity(1)
        };

        // For now we always use TCP and possibly TLS. To support HTTP/3 we'll need to decide
        // whether to use UPD and QUIC instead.
        transports.push(Runner::RawTcp(Box::new(RawTcpRunner::new(
            ctx.clone(),
            RawTcpPlanOutput {
                dest_host: plan
                    .url
                    .host()
                    .ok_or_else(|| anyhow!("url is missing host"))?
                    .to_string(),
                dest_port: plan
                    .url
                    .port_or_known_default()
                    .ok_or_else(|| anyhow!("url is missing port"))?,
                src_host: None,
                src_port: None,
                // Unused, probably will remove.
                isn: 0,
                // Unused, probably will remove.
                window: 1000,
                // Only used when RawTcp is executor.
                segments: Vec::new(),
                //close: TcpPlanCloseOutput::default(),
                pause: crate::RawTcpPauseOutput::default(),
            },
        ))));
        transports.push(Runner::Tcp(Box::new(TcpRunner::new(
            ctx.clone(),
            TcpPlanOutput {
                host: plan
                    .url
                    .host()
                    .ok_or_else(|| anyhow!("url is missing host"))?
                    .to_string(),
                port: plan
                    .url
                    .port_or_known_default()
                    .ok_or_else(|| anyhow!("url is missing port"))?,
                body: Vec::new(),
                //close: TcpPlanCloseOutput::default(),
                pause: crate::TcpPauseOutput::default(),
            },
        ))));

        if plan.url.scheme() == "https" {
            transports.push(Runner::Tls(Box::new(TlsRunner::new(
                ctx.clone(),
                TlsPlanOutput {
                    host: plan
                        .url
                        .host()
                        .ok_or_else(|| anyhow!("url is missing host"))?
                        .to_string(),
                    port: plan
                        .url
                        .port_or_known_default()
                        .ok_or_else(|| anyhow!("url is missing port"))?,
                    alpn: vec![b"http/1.1".to_vec() /*, b"h2".to_vec()*/],
                    body: Vec::new(),
                    pause: crate::TlsPauseOutput::default(),
                },
            ))))
        }

        Ok(HttpRunner {
            state: State::Pending { transports },
            inner: HttpProtocol::Http1(Http1Runner::new(
                ctx,
                crate::Http1PlanOutput {
                    url: plan.url,
                    method: plan.method,
                    version_string: Some("HTTP/1.1".into()),
                    add_content_length: plan.add_content_length,
                    headers: plan.headers,
                    body: plan.body,
                    pause: crate::Http1PauseOutput {
                        open: plan.pause.open,
                        request_headers: plan.pause.request_headers,
                        request_body: plan.pause.request_body,
                        response_headers: plan.pause.response_headers,
                        response_body: plan.pause.response_body,
                    },
                },
            )),
        })
    }

    pub fn size_hint(&mut self, size_hint: Option<usize>) -> Option<usize> {
        let State::Pending { transports } = &mut self.state else {
            panic!("invalid state to call size_hint")
        };
        let mut size_hint = match &mut self.inner {
            HttpProtocol::Http1(p) => p.size_hint(size_hint),
        };
        for t in transports.iter_mut().rev() {
            size_hint = t.size_hint(size_hint);
        }
        size_hint
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        match &self.inner {
            HttpProtocol::Http1(r) => r.executor_size_hint(),
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let state = mem::replace(&mut self.state, State::Running);
        let State::Pending { transports } = state else {
            bail!("invalid state to call start")
        };

        let mut transport = None;
        for mut t in transports {
            t.start(transport, 1).await?;
            transport = Some(t);
        }
        let transport = transport.expect("http should always provide a transport");

        match &mut self.inner {
            HttpProtocol::Http1(r) => r.start(transport).await,
        }
    }

    pub async fn execute(&mut self) {
        match &mut self.inner {
            HttpProtocol::Http1(r) => r.execute().await,
        }
    }

    pub fn finish(self) -> (HttpOutput, Option<Runner>) {
        match self.inner {
            HttpProtocol::Http1(r) => {
                let (out, inner) = r.finish();
                (
                    HttpOutput {
                        plan: HttpPlanOutput {
                            url: out.plan.url,
                            method: out.plan.method,
                            add_content_length: out.plan.add_content_length,
                            headers: out.plan.headers,
                            body: out.plan.body,
                            pause: HttpPauseOutput {
                                open: out.plan.pause.open,
                                request_headers: out.plan.pause.request_headers,
                                request_body: out.plan.pause.request_body,
                                response_headers: out.plan.pause.response_headers,
                                response_body: out.plan.pause.response_body,
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
                        errors: out
                            .errors
                            .into_iter()
                            .map(|e| crate::HttpError {
                                kind: e.kind,
                                message: e.message,
                            })
                            .collect(),
                        protocol: Some("HTTP/1.1".to_string()),
                        duration: out.duration,
                        pause: HttpPauseOutput {
                            open: out.pause.open,
                            request_headers: out.pause.request_headers,
                            request_body: out.pause.request_body,
                            response_headers: out.pause.response_headers,
                            response_body: out.pause.response_body,
                        },
                    },
                    inner,
                )
            }
        }
    }
}
