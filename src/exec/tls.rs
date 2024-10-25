use std::mem;
use std::task::Poll;
use std::time::Instant;
use std::{pin::pin, sync::Arc};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use chrono::Duration;
use derivative::Derivative;
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
    MaybeUtf8, TlsError, TlsOutput, TlsPlanOutput, TlsRequestOutput, TlsResponse, TlsVersion,
};

#[derive(Debug)]
pub(super) struct TlsRunner {
    ctx: Arc<Context>,
    out: TlsOutput,
    state: State,
    size_hint: Option<usize>,
}

#[derive(Derivative)]
#[derivative(Debug)]
enum State {
    Pending {
        #[derivative(Debug = "ignore")]
        connector: TlsConnector,
        domain: Box<String>,
    },
    Open {
        start: Instant,
        transport: PauseStream<Tee<Timing<TlsStream<Runner>>>>,
    },
    Completed {
        transport: Runner,
    },
    StartFailed {
        transport: Runner,
    },
    Invalid,
}

impl TlsRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: TlsPlanOutput) -> Self {
        let root_cert_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.into(),
        };
        let mut tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        tls_config.alpn_protocols = plan.alpn.iter().map(|alpn| alpn.to_vec()).collect();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

        TlsRunner {
            ctx,
            state: State::Pending {
                connector,
                domain: Box::new(plan.host.clone()),
            },
            out: TlsOutput {
                request: Some(TlsRequestOutput {
                    host: plan.host.clone(),
                    port: plan.port,
                    body: MaybeUtf8::default(),
                    time_to_first_byte: None,
                    time_to_last_byte: None,
                }),
                plan,
                response: None,
                errors: Vec::new(),
                version: None,
                duration: Duration::zero().into(),
                handshake_duration: None,
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
        let State::Pending { connector, domain } = state else {
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
        //self.out
        //    .pause
        //    .handshake
        //    .start
        //    .reserve_exact(self.out.plan.pause.handshake.start.len());
        //for p in &self.out.plan.pause.handshake.start {
        //    if p.offset_bytes != 0 {
        //        bail!("pause offset not yet supported for tls handshake");
        //    }
        //    println!("pausing before tls handshake for {:?}", p.duration);
        //    self.out
        //        .pause
        //        .handshake
        //        .start
        //        .push(Pause::new(&self.ctx, p).await?);
        //}
        // Perform the TLS handshake.
        let connection = match connector.connect(domain, transport).await {
            Ok(conn) => conn,
            Err(e) => {
                panic!("TLS handshake failure: {e}");
            }
        };
        let handshake_duration = start.elapsed();
        //for p in &self.out.plan.pause.handshake.end {
        //    if p.offset_bytes != 0 {
        //        bail!("pause offset not yet supported for tls handshake");
        //    }
        //    println!("pausing after tls handshake for {:?}", p.duration);
        //    self.out
        //        .pause
        //        .handshake
        //        .end
        //        .push(Pause::new(&self.ctx, p).await?);
        //}
        self.out.handshake_duration = Some(Duration::from_std(handshake_duration).unwrap().into());
        //if !pause.receive_body.end.is_empty() {
        //    bail!("tls.pause.receive_body.end is unsupported in this request");
        //}
        self.state = State::Open {
            start,
            transport: pause::new_stream(
                self.ctx.clone(),
                Tee::new(Timing::new(connection)),
                // TODO: Implement read size hints.
                vec![/*PauseSpec {
                    group_offset: 0,
                    plan: pause.receive_body.start,
                }*/],
                vec![],
                //if let Some(size) = self.size_hint {
                //    vec![
                //        PauseSpec {
                //            group_offset: 0,
                //            plan: pause.send_body.start,
                //        },
                //        PauseSpec {
                //            group_offset: size.try_into().unwrap(),
                //            plan: pause.send_body.end,
                //        },
                //    ]
                //} else {
                //    if !pause.send_body.end.is_empty() {
                //        bail!("tls.pause.send_body.end is unsupported in this request");
                //    }
                //    vec![PauseSpec {
                //        group_offset: 0,
                //        plan: pause.send_body.start,
                //    }]
                //},
            ),
        };
        Ok(())
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn execute(&mut self) {
        let State::Open { start, transport } = mem::replace(&mut self.state, State::Invalid) else {
            panic!("invalid state for execute: {:?}", self.state)
        };
        let (mut reader, mut writer) = tokio::io::split(transport);

        let handle = tokio::spawn(async move {
            let mut buf = [0; 512];
            loop {
                // Read and ignore the data since its already recorded by Tee.
                match reader.read(&mut buf).await {
                    Ok(size) if size == 0 => {
                        return (reader, Ok(()));
                    }
                    Err(e) => {
                        return (reader, Err(e));
                    }
                    _ => {}
                }
            }
        });
        if let Err(e) = writer.write_all(&self.out.plan.body).await {
            self.out.errors.push(TlsError {
                kind: "write failure".to_owned(),
                message: e.to_string(),
            });
        }
        if let Err(e) = writer.shutdown().await {
            self.out.errors.push(TlsError {
                kind: "read failure".to_owned(),
                message: e.to_string(),
            });
        }
        let (reader, read_result) = handle.await.expect("tls reader should not panic");
        if let Err(e) = read_result {
            self.out.errors.push(TlsError {
                kind: "read failure".to_owned(),
                message: e.to_string(),
            });
        }
        self.state = State::Open {
            start,
            transport: reader.unsplit(writer),
        }
    }

    pub fn finish(mut self) -> (TlsOutput, Runner) {
        self.complete();
        let State::Completed { transport } = self.state else {
            unreachable!();
        };
        (self.out, transport)
    }

    fn complete(&mut self) {
        let end_time = Instant::now();
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
                self.state = State::Completed { transport };
                return;
            }
            state => panic!("tls has invalid end state {state:?}"),
        };
        let (tee, send_pause, receive_pause) = transport.finish_stream();
        let (stream, writes, reads, truncated_reads, pattern_match) = tee.into_parts();

        let end_time = stream.shutdown_end().unwrap_or(end_time);

        let mut receive_pause = receive_pause.into_iter();
        //self.out.pause.receive_body.start = receive_pause.next().unwrap_or_default();
        //self.out.pause.receive_body.end = receive_pause.next().unwrap_or_default();
        let mut send_pause = send_pause.into_iter();
        //self.out.pause.send_body.start = send_pause.next().unwrap_or_default();
        //self.out.pause.send_body.end = send_pause.next().unwrap_or_default();

        if let Some(req) = &mut self.out.request {
            req.time_to_first_byte = stream
                .first_write()
                .map(|first_write| Duration::from_std(first_write - start).unwrap().into());
            req.time_to_last_byte = stream
                .last_write()
                .map(|last_write| Duration::from_std(last_write - start).unwrap().into());
            req.body = MaybeUtf8(Bytes::from(writes).into());
        }
        if !reads.is_empty() {
            self.out.response = Some(TlsResponse {
                body: MaybeUtf8(Bytes::from(reads).into()),
                time_to_first_byte: stream
                    .first_read()
                    .map(|first_read| Duration::from_std(first_read - start).unwrap().into()),
                time_to_last_byte: stream
                    .last_read()
                    .map(|last_read| Duration::from_std(last_read - start).unwrap().into()),
            });
        }
        self.out.duration = Duration::from_std(end_time - start).unwrap().into();

        let (inner, conn) = stream.into_inner().into_inner();

        self.state = State::Completed { transport: inner };

        self.out.version = match conn.protocol_version() {
            Some(rustls::ProtocolVersion::SSLv2) => Some(TlsVersion::Ssl2),
            Some(rustls::ProtocolVersion::SSLv3) => Some(TlsVersion::Ssl3),
            Some(rustls::ProtocolVersion::TLSv1_0) => Some(TlsVersion::Tls1_0),
            Some(rustls::ProtocolVersion::TLSv1_1) => Some(TlsVersion::Tls1_1),
            Some(rustls::ProtocolVersion::TLSv1_2) => Some(TlsVersion::Tls1_2),
            Some(rustls::ProtocolVersion::TLSv1_3) => Some(TlsVersion::Tls1_3),
            Some(rustls::ProtocolVersion::DTLSv1_0) => Some(TlsVersion::Dtls1_0),
            Some(rustls::ProtocolVersion::DTLSv1_2) => Some(TlsVersion::Dtls1_2),
            Some(rustls::ProtocolVersion::DTLSv1_3) => Some(TlsVersion::Dtls1_3),
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
        pin!(transport).poll_read(cx, buf)
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
        pin!(transport).poll_write(cx, buf)
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
        pin!(transport).poll_flush(cx)
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
        pin!(transport).poll_shutdown(cx)
    }
}

impl Unpin for TlsRunner {}
