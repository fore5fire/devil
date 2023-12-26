use std::task::Poll;
use std::time::Instant;
use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use chrono::Duration;
use rustls::OwnedTrustAnchor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::client::TlsStream;

use super::runner::Runner;
use super::tee::Tee;
use crate::{
    Output, TlsError, TlsOutput, TlsPlanOutput, TlsRequestOutput, TlsResponse, TlsVersion,
};

#[derive(Debug)]
pub(super) struct TlsRunner {
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
        Pin::new(&mut self.stream).poll_write(cx, buf)
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
        stream: Box<dyn Runner>,
        plan: TlsPlanOutput,
    ) -> crate::Result<TlsRunner> {
        let mut root_cert_store = rustls::RootCertStore::empty();
        root_cert_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject.to_vec(),
                ta.subject_public_key_info.to_vec(),
                ta.name_constraints.clone().map(|nc| nc.to_vec()),
            )
        }));
        let tls_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
        let domain = rustls::ServerName::try_from(plan.host.as_str())
            .map_err(|e| crate::Error(e.to_string()))?;

        // Perform the TLS handshake.
        let start = Instant::now();
        let connection = connector
            .connect(domain, stream)
            .await
            .map_err(|e| crate::Error(e.to_string()))?;
        let handshake_duration = start.elapsed();
        if let Some(p) = plan.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
        }
        Ok(TlsRunner {
            stream: Tee::new(connection),
            start,
            out: TlsOutput {
                request: Some(TlsRequestOutput {
                    host: plan.host.clone(),
                    port: plan.port,
                    body: Vec::new(),
                    pause: Vec::new(),
                    time_to_first_byte: None,
                    time_to_last_byte: None,
                }),
                plan,
                response: None,
                error: None,
                version: None,
                duration: Duration::zero(),
                handshake_duration: Some(Duration::from_std(handshake_duration).unwrap()),
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
        if let Some(p) = self
            .out
            .plan
            .pause
            .iter()
            .find(|p| p.after == "request_body")
        {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
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
        }
        if !reads.is_empty() {
            self.out.response = Some(TlsResponse {
                body: writes,
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
