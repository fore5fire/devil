use std::future;
use std::io::Read;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::Poll;

use bytes::Buf;
use http_body_util::BodyExt;
use hyper::header::ToStrError;
use hyper::{Request, Version};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use super::{State, StepOutput};
use crate::{HTTPOutput, HTTPRequest, HTTPResponse};

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
    let port = url.port_u16().unwrap_or(80);

    let address = format!("{}:{}", host, port);

    // Open a TCP connection to the remote host
    let stream = TcpStream::connect(address).await?;
    let stream = Tee::new(stream);

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
    let req_body = http.body.evaluate(state)?;
    let req = req_builder.body(req_body.clone())?;

    // Perform a TCP handshake
    let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await?;

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

    let body_bytes = body.collect().await?.aggregate();
    let mut body = Vec::with_capacity(body_bytes.remaining());
    let mut reader = body_bytes.reader();
    reader.read_to_end(&mut body)?;

    Ok(StepOutput::HTTP(HTTPOutput {
        url: url.to_string(),
        method,
        headers,
        body: req_body,
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
        },
        raw_request: parts.io.writes,
        raw_response: parts.io.reads,
    }))
}

fn contains_header(headers: &[(String, String)], key: &str) -> bool {
    headers
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
