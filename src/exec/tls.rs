use std::task::Poll;
use std::time::Instant;
use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use chrono::Duration;
use futures::future::join_all;
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::client::TlsStream;

use super::runner::Runner;
use super::tee::Tee;
use super::Context;
use crate::{
    Output, PauseOutput, PauseValueOutput, TlsError, TlsOutput, TlsPlanOutput, TlsRequestOutput,
    TlsResponse, TlsVersion, WithPlannedCapacity,
};

#[derive(Debug)]
pub(super) struct TlsRunner {
    ctx: Arc<Context>,
    out: TlsOutput,
    stream: Tee<TlsStream<Box<dyn Runner>>>,
    start: Instant,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    end_time: Option<Instant>,
    error: Option<TlsError>,
}

impl AsyncRead for TlsRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = Pin::new(&mut self.stream).poll_write(cx, buf);
        if let Poll::Ready(Ok(_)) = &poll {
            if self.first_write.is_none() {
                let mut pause_results = Vec::new();
                for p in &self.out.plan.pause.after.first_write {
                    println!("pausing after first write for {:?}", p.duration);
                    let start = Instant::now();
                    std::thread::sleep(p.duration.to_std().unwrap());
                    join_all(
                        p.join
                            .iter()
                            .map(|join| self.ctx.pause_barrier(join).wait()),
                    );
                    pause_results.push(crate::PauseValueOutput {
                        duration: Duration::from_std(start.elapsed()).unwrap(),
                        offset_bytes: p.offset_bytes,
                        join: p.join.clone(),
                    });
                }
                self.out.pause.after.first_write.extend(pause_results);
            }
        }
        poll
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let poll = Pin::new(&mut self.stream).poll_shutdown(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.end_time = Some(Instant::now());
        }
        poll
    }
}

impl Unpin for TlsRunner {}

impl TlsRunner {
    pub(super) async fn new(
        ctx: Arc<Context>,
        stream: Box<dyn Runner>,
        plan: TlsPlanOutput,
    ) -> crate::Result<TlsRunner> {
        let root_cert_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.into(),
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
        // FIXME: Why does rustls ClientConnector require a static lifetime for DNS names?
        let leaked_name = Box::leak(Box::new(plan.host.clone()));
        let domain =
            ServerName::try_from(leaked_name.as_str()).map_err(|e| crate::Error(e.to_string()))?;

        let mut pause = PauseOutput::with_planned_capacity(&plan.pause);

        // Perform the TLS handshake.
        for p in &plan.pause.before.handshake {
            println!("pausing before tls handshake for {:?}", p.duration);
            let before = Instant::now();
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
            pause.before.handshake.push(PauseValueOutput {
                duration: Duration::from_std(before.elapsed()).unwrap(),
                offset_bytes: p.offset_bytes,
                join: p.join.clone(),
            })
        }
        let start = Instant::now();
        let connection = connector
            .connect(domain, stream)
            .await
            .map_err(|e| crate::Error(e.to_string()))?;
        let handshake_duration = start.elapsed();
        for p in &plan.pause.after.handshake {
            println!("pausing after tls handshake for {:?}", p.duration);
            let start = Instant::now();
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
            join_all(p.join.iter().map(|join| ctx.pause_barrier(join).wait())).await;
            pause.after.handshake.push(PauseValueOutput {
                duration: Duration::from_std(start.elapsed()).unwrap(),
                offset_bytes: p.offset_bytes,
                join: p.join.clone(),
            })
        }
        Ok(TlsRunner {
            ctx,
            stream: Tee::new(connection),
            start,
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
                error: None,
                version: None,
                duration: Duration::zero(),
                handshake_duration: Some(Duration::from_std(handshake_duration).unwrap()),
                pause,
            },
            error: None,
            end_time: None,
            first_read: None,
            last_read: None,
            first_write: None,
            last_write: None,
        })
    }
}

#[async_trait]
impl Runner for TlsRunner {
    async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: figure out how restructure things so we can call start before wrapping the
        // transport. For now, only no supported transport for TLS actually needs start.
        Ok(())
    }

    async fn execute(&mut self) {
        if let Err(e) = self.start(None).await {
            self.error = Some(TlsError {
                kind: "start failure".to_owned(),
                message: e.to_string(),
            });
            return;
        }

        if let Err(e) = self.stream.write_all(&self.out.plan.body).await {
            self.error = Some(TlsError {
                kind: "write failure".to_owned(),
                message: e.to_string(),
            });
            return;
        }
        if let Err(e) = self.stream.flush().await {
            self.error = Some(TlsError {
                kind: "write failure".to_owned(),
                message: e.to_string(),
            });
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.stream.read_to_end(&mut response).await {
            self.error = Some(TlsError {
                kind: "read failure".to_owned(),
                message: e.to_string(),
            });
            return;
        }
    }

    async fn finish(mut self: Box<Self>) -> (Output, Option<Box<dyn Runner>>) {
        let end_time = Instant::now();
        let (stream, writes, reads) = self.stream.into_parts();
        let (inner, conn) = stream.into_inner();

        if let Some(req) = &mut self.out.request {
            req.time_to_first_byte = self
                .first_write
                .map(|first_write| Duration::from_std(first_write - self.start).unwrap());
            req.time_to_last_byte = self
                .last_write
                .map(|last_write| Duration::from_std(last_write - self.start).unwrap());
            req.body = writes;
        }
        if !reads.is_empty() {
            self.out.response = Some(TlsResponse {
                body: reads,
                time_to_first_byte: self
                    .first_read
                    .map(|first_read| Duration::from_std(first_read - self.start).unwrap()),
                time_to_last_byte: self
                    .last_read
                    .map(|last_read| Duration::from_std(last_read - self.start).unwrap()),
            });
        }
        self.out.duration =
            Duration::from_std(self.end_time.unwrap_or(end_time) - self.start).unwrap();
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
        (Output::Tls(self.out), Some(inner))
    }
}
