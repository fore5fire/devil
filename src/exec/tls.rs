use std::time::Instant;
use std::{pin::Pin, sync::Arc};

use rustls::OwnedTrustAnchor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::client::TlsStream;

use super::tee::{Stream, Tee};
use crate::{TLSOutput, TLSResponse};

#[derive(Debug)]
pub(super) struct TLSRunner<S: Stream> {
    out: TLSOutput,
    stream: Tee<TlsStream<S>>,
    start: Instant,
}

impl<S: Stream> AsyncRead for TLSRunner<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_ref().stream).poll_read(cx, buf)
    }
}

impl<S: Stream> AsyncWrite for TLSRunner<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.as_ref().stream).poll_shutdown(cx)
    }
}

impl<S: Stream> Unpin for TLSRunner<S> {}

impl<S: Stream + Send + 'static> TLSRunner<S> {
    pub(super) async fn new(stream: S, data: TLSOutput) -> crate::Result<TLSRunner<S>> {
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
        let domain = rustls::ServerName::try_from(data.host.as_str())
            .map_err(|e| crate::Error(e.to_string()))?;

        // Perform the TLS handshake.
        let start = Instant::now();
        let connection = connector
            .connect(domain, stream)
            .await
            .map_err(|e| crate::Error(e.to_string()))?;
        if let Some(p) = data.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        Ok(TLSRunner {
            stream: Tee::new(connection),
            start,
            out: data,
        })
    }

    pub(super) async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stream.write_all(&self.out.body).await?;
        self.stream.flush().await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            std::thread::sleep(p.duration);
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        Ok(())
    }

    pub(super) async fn finish(mut self) -> (TLSOutput, S) {
        let (stream, writes, reads) = self.stream.into_parts();

        self.out.body = writes;
        self.out.response = Some(TLSResponse {
            body: reads,
            duration: self.start.elapsed(),
        });
        (self.out, stream.into_inner().0)
    }
}
