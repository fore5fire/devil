use std::time::Instant;
use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use rustls::OwnedTrustAnchor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::client::TlsStream;

use super::runner::Runner;
use super::tee::Tee;
use crate::{Output, TlsOutput, TlsRequestOutput, TlsResponse, TlsVersion};

#[derive(Debug)]
pub(super) struct TlsRunner {
    req: TlsRequestOutput,
    stream: Tee<TlsStream<Box<dyn Runner>>>,
    start: Instant,
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
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Unpin for TlsRunner {}

impl TlsRunner {
    pub(super) async fn new(
        stream: Box<dyn Runner>,
        req: TlsRequestOutput,
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
        let domain = rustls::ServerName::try_from(req.host.as_str())
            .map_err(|e| crate::Error(e.to_string()))?;

        // Perform the TLS handshake.
        let start = Instant::now();
        let connection = connector
            .connect(domain, stream)
            .await
            .map_err(|e| crate::Error(e.to_string()))?;
        if let Some(p) = req.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
        }
        Ok(TlsRunner {
            stream: Tee::new(connection),
            start,
            req,
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

    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.start(None).await?;

        self.stream.write_all(&self.req.body).await?;
        self.stream.flush().await?;
        if let Some(p) = self.req.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration.to_std().unwrap());
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        let (stream, writes, reads) = self.stream.into_parts();
        let (inner, conn) = stream.into_inner();

        self.req.body = writes;
        Ok((
            Output::Tls(TlsOutput {
                version: match conn
                    .protocol_version()
                    .ok_or_else(|| crate::Error("finished before version established".to_owned()))?
                {
                    rustls::ProtocolVersion::SSLv2 => TlsVersion::SSL2,
                    rustls::ProtocolVersion::SSLv3 => TlsVersion::SSL3,
                    rustls::ProtocolVersion::TLSv1_0 => TlsVersion::TLS1_0,
                    rustls::ProtocolVersion::TLSv1_1 => TlsVersion::TLS1_1,
                    rustls::ProtocolVersion::TLSv1_2 => TlsVersion::TLS1_2,
                    rustls::ProtocolVersion::TLSv1_3 => TlsVersion::TLS1_3,
                    rustls::ProtocolVersion::DTLSv1_0 => TlsVersion::DTLS1_0,
                    rustls::ProtocolVersion::DTLSv1_2 => TlsVersion::DTLS1_2,
                    rustls::ProtocolVersion::DTLSv1_3 => TlsVersion::DTLS1_3,
                    rustls::ProtocolVersion::Unknown(val) => TlsVersion::Other(val),
                    _ => TlsVersion::Other(0),
                },
                request: self.req,
                response: TlsResponse {
                    body: reads,
                    duration: chrono::Duration::from_std(self.start.elapsed()).unwrap(),
                },
            }),
            Some(inner),
        ))
    }
}
