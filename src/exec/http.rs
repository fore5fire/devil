use std::future;
use std::io::Read;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use http_body_util::BodyExt;
use hyper::body::Buf;
use hyper::body::Incoming;
use hyper::header::ToStrError;
use hyper::http::response::Parts;
use hyper::http::uri::Scheme;
use hyper::{Request, Version};
use hyper_util::rt::TokioIo;
use rustls::OwnedTrustAnchor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use super::tee::Tee;
use super::State;
use crate::PauseOutput;
use crate::{
    HTTPOutput, HTTPRequest, HTTPResponse, StepOutput, TCPOutput, TCPResponse, TLSOutput,
    TLSResponse,
};

pub(super) async fn execute(
    http: &HTTPRequest,
    state: &State<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    // Get the host and the port
    let raw_url = http.url.evaluate(state)?;
    let url: hyper::Uri = raw_url.parse()?;
    let host = url
        .host()
        .ok_or_else(|| crate::Error::from("url has no host"))?;
    let port = url.port_u16().unwrap_or_else(|| {
        if url.scheme() == Some(&Scheme::HTTP) {
            80
        } else {
            443
        }
    });

    let pause = http
        .pause
        .clone()
        .into_iter()
        .map(|p| {
            Ok(PauseOutput {
                after: p.after.evaluate(state)?,
                duration: p.duration.evaluate(state)?,
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    // TODO: explicitly lookup dns names so we can do pausing if needed. Also dns should get its
    // own protocol options.
    let address = format!("{}:{}", host, port);

    // Prepare the request.
    let authority = url.authority().ok_or("request missing host")?.clone();
    let default_headers = [(hyper::header::HOST, authority.as_str())];
    let headers = http.headers.evaluate(state)?;
    let method = http.method.evaluate(state)?;
    let mut req_builder = Request::builder().method(method.as_str()).uri(url.clone());
    for (k, v) in default_headers {
        if !contains_header(&headers, k.as_str()) {
            req_builder = req_builder.header(k, v);
        }
    }
    for (key, val) in headers.iter() {
        req_builder = req_builder.header(key, val)
    }
    //let req_builder = if let Some(v) = http.http_version {
    //    req_builder.version(v)
    //} else {
    //    req_builder
    //};

    let req_body = http
        .body
        .clone()
        .map(|body| body.evaluate(state))
        .transpose()?;
    let req = req_builder.body(req_body.clone().unwrap_or_default())?;
    // Start the TCP timer.
    let tcp_start = std::time::Instant::now();
    // Open a TCP connection to the remote host
    // TODO: Allow reusing the connection for future requests.
    let stream = TcpStream::connect(address).await?;
    let tee = Tee::new(stream);
    let tls_writes: Vec<u8>;
    let tls_reads: Vec<u8>;
    let tls_start: Option<Instant>;
    let http_start: std::time::Instant;
    let (tcp_tee, head, body) = if url.scheme() == Some(&Scheme::HTTP) {
        tls_writes = Vec::new();
        tls_reads = Vec::new();
        tls_start = None;
        if let Some(p) = pause.iter().find(|p| p.after == "open") {
            println!("pausing after open for {:?}", p.duration);
            std::thread::sleep(p.duration);
        }
        http_start = std::time::Instant::now();
        run(tee, req).await?
    } else {
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
        let domain = rustls::ServerName::try_from(host).map_err(|e| crate::Error(e.to_string()))?;
        tls_start = Some(std::time::Instant::now());
        let connection = connector.connect(domain, tee).await?;
        let tee = Tee::new(connection);

        //println!("connected: {:?}", connection);
        if let Some(p) = pause.iter().find(|p| p.after == "open") {
            println!("pausing after open for {:?}", p.duration);
            std::thread::sleep(p.duration);
        }
        http_start = std::time::Instant::now();
        let (mut tee, head, body) = run(tee, req).await?;
        // Record tls request and response body before unwrapping back to the tcp tee.
        let inner;
        (inner, tls_writes, tls_reads) = tee.into_parts();
        (inner.into_inner().0, head, body)
    };
    if let Some(p) = pause.iter().find(|p| p.after == "request_body") {
        println!("pausing after {} for {:?}", p.after, p.duration);
        std::thread::sleep(p.duration);
    }

    // Collect the remaining bytes in the response body.
    let body_bytes = body.collect().await?;
    // Finalize times.
    let http_duration = http_start.elapsed();
    let tls_duration = if let Some(tls_start) = tls_start {
        Some(tls_start.elapsed())
    } else {
        None
    };
    let tcp_duration = tcp_start.elapsed();

    // Get the response body into a Vec.
    let body_bytes = body_bytes.aggregate();
    let mut body = Vec::with_capacity(body_bytes.remaining());
    let mut reader = body_bytes.reader();
    reader.read_to_end(&mut body)?;

    Ok(StepOutput {
        http: Some(HTTPOutput {
            url: url.to_string(),
            method,
            headers,
            body: req_body,
            pause,
            response: HTTPResponse {
                status_code: head.status.as_u16(),
                status_reason: "unimplemented".to_owned(),
                headers: head
                    .headers
                    .into_iter()
                    .map(|(name, value)| {
                        Ok((
                            name.map(|n| n.to_string()).unwrap_or_default(),
                            value.to_str()?.to_string(),
                        ))
                    })
                    .collect::<Result<_, ToStrError>>()?,
                protocol: match head.version {
                    Version::HTTP_09 => "HTTP/0.9".to_owned(),
                    Version::HTTP_10 => "HTTP/1.0".to_owned(),
                    Version::HTTP_11 => "HTTP/1.1".to_owned(),
                    Version::HTTP_2 => "HTTP/2".to_owned(),
                    Version::HTTP_3 => "HTTP/3".to_owned(),
                    _ => "unrecognized".to_owned(),
                },
                body,
                duration: http_duration,
            },
        }),
        tls: if let Some(tls_duration) = tls_duration {
            Some(TLSOutput {
                host: host.to_owned(),
                port,
                body: tls_writes,
                response: TLSResponse {
                    body: tls_reads,
                    duration: tls_duration,
                },
            })
        } else {
            None
        },
        tcp: Some(TCPOutput {
            host: host.to_owned(),
            port,
            body: tcp_tee.writes,
            pause: Vec::new(),
            response: Some(TCPResponse {
                body: tcp_tee.reads,
                duration: tcp_duration,
            }),
        }),
        ..Default::default()
    })
}

fn contains_header(headers: &[(String, String)], key: &str) -> bool {
    headers
        .iter()
        .find(|(k, _)| key.eq_ignore_ascii_case(k))
        .is_some()
}

async fn run<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: Tee<T>,
    req: hyper::Request<String>,
) -> Result<(Tee<T>, Parts, Incoming), Box<dyn std::error::Error + Send + Sync>> {
    // Wrap the AsyncRead/AsyncWrite stream for tokio's Read/Write traits.
    let io = TokioIo::new(stream);
    // Perform a TCP handshake
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    // Wrap conn in an Option to convince the borrow checker that it's ok to
    // move conn out of the closure even if it may be called again (it won't).
    let mut conn = Some(conn);
    let (parts, (head, body)) = futures::try_join!(
        future::poll_fn(move |cx| {
            futures::ready!(conn.as_mut().unwrap().poll_without_shutdown(cx))?;
            Poll::Ready(Ok::<_, hyper::Error>(conn.take().unwrap().into_parts()))
        }),
        async move {
            let res = sender.send_request(req).await?;
            Ok(res.into_parts())
        }
    )?;
    let parts = parts.io.into_inner();
    Ok((parts, head, body))
}