use std::fmt::Display;
use std::future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::Poll;

use bytes::Buf;
use http_body_util::BodyExt;
use hyper::{HeaderMap, Request, StatusCode, Version};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use super::{StepInputs, StepOutput, StepParsedOutput};
use crate::HTTPRequest;

#[derive(Debug, Clone, PartialEq)]
pub struct HTTPOutput {
    pub version: HTTPVersion,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HTTPVersion {
    HTTP0_9,
    HTTP1_0,
    HTTP1_1,
    HTTP2,
    HTTP3,
    Unrecognized,
}

impl Display for HTTPVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HTTP0_9 => f.write_str("HTTP/0.9"),
            Self::HTTP1_0 => f.write_str("HTTP/1.0"),
            Self::HTTP1_1 => f.write_str("HTTP/1.1"),
            Self::HTTP2 => f.write_str("HTTP/2"),
            Self::HTTP3 => f.write_str("HTTP/3"),
            Self::Unrecognized => f.write_str("unrecognized protocol"),
        }
    }
}

impl From<Version> for HTTPVersion {
    fn from(value: Version) -> Self {
        match value {
            Version::HTTP_09 => Self::HTTP0_9,
            Version::HTTP_10 => Self::HTTP1_0,
            Version::HTTP_11 => Self::HTTP1_1,
            Version::HTTP_2 => Self::HTTP2,
            Version::HTTP_3 => Self::HTTP3,
            _ => Self::Unrecognized,
        }
    }
}

pub(super) async fn execute(
    step: &HTTPRequest<'_>,
    inputs: &StepInputs<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    // Get the host and the port
    let host = step.endpoint.host().expect("uri has no host");
    let port = step.endpoint.port_u16().unwrap_or(80);

    let address = format!("{}:{}", host, port);

    // Open a TCP connection to the remote host
    let stream = TcpStream::connect(address).await?;
    let stream = Tee::new(stream);

    // Prepare the request.
    let authority = step
        .endpoint
        .authority()
        .ok_or("request missing host")?
        .clone();
    let default_headers = [
        (hyper::header::HOST, authority.as_str()),
        (hyper::header::USER_AGENT, "courier/0.1.0"),
    ];
    let mut req_builder = Request::builder()
        .method(step.method)
        .uri(step.endpoint.clone());
    for (k, v) in default_headers {
        if !contains_header(step, k.as_str()) {
            req_builder = req_builder.header(k, v);
        }
    }
    for (key, val) in step.headers.iter() {
        req_builder = req_builder.header(*key, *val)
    }
    let req = req_builder.body(step.body.to_owned())?;

    // Perform a TCP handshake
    let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await?;

    // Wrap conn in an Option to convince the compiler that it's ok to move
    // conn out of the closure even if it may be called again (it won't).
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

    let mut body_bytes = body.collect().await?.aggregate();
    let mut body = Vec::with_capacity(body_bytes.remaining());
    body_bytes.copy_to_slice(&mut body);

    Ok(StepOutput {
        raw_request: parts.io.writes,
        raw_response: parts.io.reads,
        parsed: StepParsedOutput::HTTP(HTTPOutput {
            status: head.status,
            headers: head.headers,
            version: head.version.into(),
            body,
        }),
    })
}

fn contains_header(step: &HTTPRequest, key: &str) -> bool {
    step.headers
        .iter()
        .find(|(k, _)| key.eq_ignore_ascii_case(k))
        .is_some()
}

struct Tee<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    inner: T,
    pub reads: Vec<u8>,
    pub writes: Vec<u8>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Tee<T> {
    pub fn new(wrap: T) -> Self {
        Tee {
            inner: wrap,
            reads: Vec::new(),
            writes: Vec::new(),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncRead for Tee<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let old_len = buf.filled().len();
        let poll = Pin::new(&mut self.deref_mut().inner).poll_read(cx, buf);
        self.reads.extend_from_slice(&buf.filled()[old_len..]);
        poll
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncWrite for Tee<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = Pin::new(&mut self.deref_mut().inner).poll_write(cx, buf);
        if poll.is_ready() {
            self.get_mut().writes.extend_from_slice(&buf);
        }
        poll
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.deref_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.deref_mut().inner).poll_shutdown(cx)
    }
}
