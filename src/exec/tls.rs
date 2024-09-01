use std::task::Poll;
use std::time::Instant;
use std::{pin::Pin, sync::Arc};

use anyhow::{anyhow, bail};
use chrono::Duration;
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use super::pause::{self, PauseStream};
use super::runner::Runner;
use super::tee::Tee;
use super::timing::Timing;
use super::Context;
use crate::exec::pause::{Pause, PauseSpec};
use crate::{
    TlsError, TlsOutput, TlsPauseOutput, TlsPlanOutput, TlsRequestOutput, TlsResponse, TlsVersion,
    WithPlannedCapacity,
};

#[derive(Debug)]
pub(super) struct TlsRunner {
    ctx: Arc<Context>,
    out: TlsOutput,
    state: State,
    size_hint: Option<usize>,
}

enum State {
    Pending {
        connector: TlsConnector,
        domain: Box<String>,
        pause: TlsPauseOutput,
    },
    Open {
        start: Instant,
        transport: PauseStream<Tee<Timing<TlsStream<Runner>>>>,
    },
    Completed {
        transport: Option<Runner>,
    },
    StartFailed {
        transport: Runner,
    },
    Invalid,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending {
                connector: _,
                domain,
                pause,
            } => {
                write!(
                    f,
                    "Pending {{ connector: <Opaque>, domain: {domain}, pause: {pause:?} }}"
                )
            }
            Self::Open { start, transport } => {
                write!(f, "Open {{ start: {start:?}, transport: {transport:?} }}")
            }
            Self::Completed { transport } => write!(f, "Completed {{ transport: {transport:?} }}"),
            Self::StartFailed { transport } => {
                write!(f, "StartFailed {{ transport: {transport:?} }}")
            }
            Self::Invalid => write!(f, "Invalid"),
        }
    }
}

impl TlsRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TlsPlanOutput) -> Self {
        let root_cert_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.into(),
        };
        let mut tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        tls_config.alpn_protocols = plan.alpn.clone();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

        let pause = crate::TlsPauseOutput::with_planned_capacity(&plan.pause);

        TlsRunner {
            ctx,
            state: State::Pending {
                connector,
                domain: Box::new(plan.host.clone()),
                pause: plan.pause.clone(),
            },
            out: TlsOutput {
                request: Some(TlsRequestOutput {
                    host: plan.host.clone(),
                    port: plan.port,
                    body: Vec::new(),
                    time_to_first_byte: None,
                    time_to_last_byte: None,
                }),
                plan,
                response: None,
                errors: Vec::new(),
                version: None,
                duration: Duration::zero(),
                handshake_duration: None,
                pause,
            },
            size_hint: None,
        }
    }

    pub(super) fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        self.size_hint = hint;
        // It's really complicated to pre-calculate the number of bytes TLS will increase the
        // stream by, so don't forward a size hint even if we have one.
        None
    }

    pub async fn start(&mut self, transport: Runner) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Pending {
            connector,
            domain,
            pause,
        } = state
        else {
            bail!("attempt to start TlsRunner from unexpected state");
        };

        // FIXME: Why does rustls ClientConnector require a static lifetime for DNS names?
        let leaked_name = Box::leak(domain);
        let domain = match ServerName::try_from(leaked_name.as_str()) {
            Ok(domain) => domain,
            Err(e) => {
                self.out.errors.push(TlsError {
                    kind: "parse domain".to_owned(),
                    message: e.to_string(),
                });
                self.state = State::StartFailed { transport };
                self.complete();
                return Err(e.into());
            }
        };

        let start = Instant::now();
        self.out
            .pause
            .handshake
            .start
            .reserve_exact(self.out.plan.pause.handshake.start.len());
        for p in &self.out.plan.pause.handshake.start {
            if p.offset_bytes != 0 {
                bail!("pause offset not yet supported for tls handshake");
            }
            println!("pausing before tls handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .start
                .push(Pause::new(&self.ctx, p).await?);
        }
        // Perform the TLS handshake.
        let connection = match connector.connect(domain, transport).await {
            Ok(conn) => conn,
            Err(e) => {
                panic!("TLS handshake failure: {e}");
            }
        };
        let handshake_duration = start.elapsed();
        for p in &self.out.plan.pause.handshake.end {
            if p.offset_bytes != 0 {
                bail!("pause offset not yet supported for tls handshake");
            }
            println!("pausing after tls handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .end
                .push(Pause::new(&self.ctx, p).await?);
        }
        self.out.handshake_duration = Some(Duration::from_std(handshake_duration).unwrap());
        if !pause.receive_body.end.is_empty() {
            bail!("tls.pause.receive_body.end is unsupported in this request");
        }
        self.state = State::Open {
            start,
            transport: pause::new_stream(
                self.ctx.clone(),
                Tee::new(Timing::new(connection, None)),
                // TODO: Implement read size hints.
                vec![PauseSpec {
                    group_offset: 0,
                    plan: pause.receive_body.start,
                }],
                if let Some(size) = self.size_hint {
                    vec![
                        PauseSpec {
                            group_offset: 0,
                            plan: pause.send_body.start,
                        },
                        PauseSpec {
                            group_offset: size.try_into().unwrap(),
                            plan: pause.send_body.end,
                        },
                    ]
                } else {
                    if !pause.send_body.end.is_empty() {
                        bail!("tls.pause.send_body.end is unsupported in this request");
                    }
                    vec![PauseSpec {
                        group_offset: 0,
                        plan: pause.send_body.start,
                    }]
                },
            ),
        };
        Ok(())
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn execute(&mut self) {
        let body = std::mem::take(&mut self.out.plan.body);
        if let Err(e) = self.write_all(&body).await {
            self.out.errors.push(TlsError {
                kind: "write failure".to_owned(),
                message: e.to_string(),
            });
            self.out.plan.body = body;
            self.complete();
            return;
        }
        self.out.plan.body = body;
        if let Err(e) = self.flush().await {
            self.out.errors.push(TlsError {
                kind: "write failure".to_owned(),
                message: e.to_string(),
            });
            self.complete();
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.read_to_end(&mut response).await {
            self.out.errors.push(TlsError {
                kind: "read failure".to_owned(),
                message: e.to_string(),
            });
            self.complete();
            return;
        }
    }

    pub fn finish(mut self) -> (TlsOutput, Option<Runner>) {
        self.complete();
        let State::Completed { transport } = self.state else {
            unreachable!();
        };
        (self.out, transport)
    }

    fn complete(&mut self) {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let (start, transport) = match state {
            State::Open {
                start, transport, ..
            } => (start, transport),
            State::Completed { transport } => {
                self.state = State::Completed { transport };
                return;
            }
            State::StartFailed { transport } => {
                self.state = State::Completed {
                    transport: Some(transport),
                };
                return;
            }
            State::Pending { .. } => {
                self.state = State::Completed { transport: None };
                return;
            }
            State::Invalid => panic!("tls has invalid end state"),
        };
        let end_time = Instant::now();
        let (tee, send_pause, receive_pause) = transport.finish_stream();
        let (stream, writes, reads, truncated_reads) = tee.into_parts();

        let mut receive_pause = receive_pause.into_iter();
        self.out.pause.receive_body.start = receive_pause.next().unwrap_or_default();
        self.out.pause.receive_body.end = receive_pause.next().unwrap_or_default();
        let mut send_pause = send_pause.into_iter();
        self.out.pause.send_body.start = send_pause.next().unwrap_or_default();
        self.out.pause.send_body.end = send_pause.next().unwrap_or_default();

        if let Some(req) = &mut self.out.request {
            req.time_to_first_byte = stream
                .first_write()
                .map(|first_write| Duration::from_std(first_write - start).unwrap());
            req.time_to_last_byte = stream
                .last_write()
                .map(|last_write| Duration::from_std(last_write - start).unwrap());
            req.body = writes;
        }
        if !reads.is_empty() {
            self.out.response = Some(TlsResponse {
                body: reads,
                time_to_first_byte: stream
                    .first_read()
                    .map(|first_read| Duration::from_std(first_read - start).unwrap()),
                time_to_last_byte: stream
                    .last_read()
                    .map(|last_read| Duration::from_std(last_read - start).unwrap()),
            });
        }
        self.out.duration = Duration::from_std(end_time - start).unwrap();

        let (inner, conn) = stream.into_inner().into_inner();

        self.state = State::Completed {
            transport: Some(inner),
        };

        self.out.version = match conn.protocol_version() {
            Some(rustls::ProtocolVersion::SSLv2) => Some(TlsVersion::SSL2),
            Some(rustls::ProtocolVersion::SSLv3) => Some(TlsVersion::SSL3),
            Some(rustls::ProtocolVersion::TLSv1_0) => Some(TlsVersion::TLS1_0),
            Some(rustls::ProtocolVersion::TLSv1_1) => Some(TlsVersion::TLS1_1),
            Some(rustls::ProtocolVersion::TLSv1_2) => Some(TlsVersion::TLS1_2),
            Some(rustls::ProtocolVersion::TLSv1_3) => Some(TlsVersion::TLS1_3),
            Some(rustls::ProtocolVersion::DTLSv1_0) => Some(TlsVersion::DTLS1_0),
            Some(rustls::ProtocolVersion::DTLSv1_2) => Some(TlsVersion::DTLS1_2),
            Some(rustls::ProtocolVersion::DTLSv1_3) => Some(TlsVersion::DTLS1_3),
            Some(rustls::ProtocolVersion::Unknown(val)) => Some(TlsVersion::Other(val)),
            Some(_) => Some(TlsVersion::Other(0)),
            None => None,
        };
    }
}

impl AsyncRead for TlsRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let State::Open { transport, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot read stream in {:?} state",
                self.state
            ))));
        };
        Pin::new(transport).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let State::Open { transport, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot write stream in {:?} state",
                self.state
            ))));
        };
        Pin::new(transport).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { transport, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot flush stream in {:?} state",
                self.state
            ))));
        };
        Pin::new(transport).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let State::Open { transport, .. } = &mut self.state else {
            return Poll::Ready(Err(std::io::Error::other(anyhow!(
                "cannot shutdown stream in {:?} state",
                self.state
            ))));
        };
        let poll = Pin::new(transport).poll_shutdown(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.complete();
        }
        poll
    }
}

impl Unpin for TlsRunner {}
