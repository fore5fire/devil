use std::sync::Arc;
use std::time::Instant;

use rustls::OwnedTrustAnchor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::{tee::Tee, State};
use crate::{StepOutput, TCPOutput, TCPResponse, TLSOutput, TLSRequest, TLSResponse};

pub(super) async fn execute(
    tls: &TLSRequest,
    state: &State<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    // Start the TCP timer.
    let tcp_start = Instant::now();
    // Open a TCP connection to the remote host
    // TODO: Allow reusing the connection for future requests.
    let host = tls.host.evaluate(state)?;
    let port = tls.port.evaluate(state)?;
    let address = format!("{}:{}", host, port);
    let stream = TcpStream::connect(address).await?;
    let tee = Tee::new(stream);
    //println!("using TLS with name {}", host);
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
    let domain =
        rustls::ServerName::try_from(host.as_str()).map_err(|e| crate::Error(e.to_string()))?;
    let tls_start = Instant::now();
    let mut connection = connector.connect(domain, tee).await?;
    let req_body = tls.body.evaluate(state)?;
    connection.write_all(&req_body).await?;
    // Get the response body into a Vec.
    let mut resp_body = Vec::new();
    connection.read_to_end(&mut resp_body).await?;
    // Finalize times.
    connection.shutdown().await?;
    let tls_duration = tls_start.elapsed();
    let tee = connection.into_inner().0;
    let tcp_duration = tcp_start.elapsed();

    Ok(StepOutput {
        tls: Some(TLSOutput {
            host: host.clone(),
            port: port.parse()?,
            body: req_body,
            response: TLSResponse {
                body: resp_body,
                duration: tls_duration,
            },
        }),
        tcp: Some(TCPOutput {
            host,
            port: port.parse()?,
            body: tee.writes,
            response: TCPResponse {
                body: tee.reads,
                duration: tcp_duration,
            },
        }),
        ..Default::default()
    })
}
