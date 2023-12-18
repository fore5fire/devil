use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::runner::Runner;
use super::tee::Tee;
use crate::{Error, Http1Output, Http1RequestOutput, Http1Response, Output};

#[derive(Debug)]
pub(super) struct Http1Runner {
    req: Http1RequestOutput,
    resp: Option<Http1Response>,
    stream: Tee<Box<dyn Runner>>,
    start_time: Instant,
    resp_header_buf: BytesMut,
    header_sent: bool,
}

impl AsyncRead for Http1Runner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Read the first bytes of the response as the header.
        if self.resp.is_some() {
            return Pin::new(&mut self.stream).poll_read(cx, buf);
        }

        // TODO: optimize this to avoid the intermediate allocation and write.
        let mut header_vec = Box::new([0; 1 << 16]);
        let mut header_buf = ReadBuf::new(header_vec.as_mut());
        let poll = Pin::new(&mut self.stream).poll_read(cx, &mut header_buf);
        self.resp_header_buf.put(header_buf.filled());
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            // Data was read - try to process it.
            Poll::Ready(Ok(())) => match self.receive_header() {
                // Not enough data, we'll try again later.
                Poll::Pending => Poll::Pending,
                // The full header was read, read the leftover bytes as part of the body.
                Poll::Ready(Ok(remaining)) if buf.remaining() > remaining.len() => {
                    buf.put(remaining);
                    Poll::Ready(Ok(()))
                }
                // Not enough room for all the leftover bytes - write what we can to the caller's
                // buffer and save the rest.
                Poll::Ready(Ok(remaining)) => {
                    self.resp_header_buf = remaining;
                    let bytes_that_fit = self.resp_header_buf.split_to(buf.remaining());
                    buf.put(bytes_that_fit);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            },
        }
    }
}

impl AsyncWrite for Http1Runner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
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

impl Http1Runner {
    pub(super) async fn new(
        stream: Box<dyn Runner>,
        req: Http1RequestOutput,
    ) -> crate::Result<Self> {
        let start_time = Instant::now();

        if let Some(p) = req.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        Ok(Self {
            stream: Tee::new(stream),
            start_time,
            req,
            resp: None,
            resp_header_buf: BytesMut::new(),
            header_sent: false,
        })
    }

    #[inline]
    fn compute_header(&self) -> BytesMut {
        // Build a buffer with the header contents to avoid the overhead of separate writes.
        // TODO: We may actually want to split packets based on info at the HTTP layer, that logic
        // will go here once I figure out the right configuration to express it.
        let mut buf = BytesMut::with_capacity(
            self.req.method.as_ref().map(Vec::len).unwrap_or(0)
                + 1
                + self.req.url.path().len()
                + self.req.url.query().map(|x| x.len() + 1).unwrap_or(0)
                + 1
                + self.req.version_string.as_ref().map(Vec::len).unwrap_or(0)
                + 2
                + self
                    .req
                    .headers
                    .iter()
                    .fold(0, |sum, (k, v)| sum + k.len() + 2 + v.len() + 2)
                + 2
                + self.req.body.len(),
        );
        if let Some(m) = &self.req.method {
            buf.put_slice(m);
        }
        buf.put_u8(b' ');
        buf.put_slice(self.req.url.path().as_bytes());
        if let Some(q) = self.req.url.query() {
            buf.put_u8(b'?');
            buf.put_slice(q.as_bytes());
        }
        buf.put_u8(b' ');
        if let Some(p) = &self.req.version_string {
            buf.put_slice(p);
        }
        buf.put(b"\r\n".as_slice());
        for (k, v) in &self.req.headers {
            buf.put_slice(k.as_slice());
            buf.put_slice(b": ");
            buf.put_slice(v.as_slice());
            buf.put_slice(b"\r\n");
        }
        buf.put(b"\r\n".as_slice());
        buf
    }

    #[inline]
    fn receive_header(&mut self) -> Poll<std::io::Result<BytesMut>> {
        // TODO: Write our own extra-permissive parser.
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut resp = httparse::Response::new(&mut headers);
        match resp.parse(&self.resp_header_buf) {
            Ok(httparse::Status::Complete(body_start)) => {
                // Set the header fields in our response.
                self.resp = Some(Http1Response {
                    protocol: format!("HTTP/1.{}", resp.version.unwrap()).into(),
                    status_code: resp.code.unwrap(),
                    status_reason: resp.reason.unwrap().into(),
                    headers: resp
                        .headers
                        .into_iter()
                        .map(|h| (Vec::from(h.name), Vec::from(h.value)))
                        .collect(),
                    body: Vec::new(),
                    duration: Duration::ZERO,
                });
                // Return the bytes we didn't read.
                self.resp_header_buf.advance(body_start);
                Poll::Ready(Ok(std::mem::take(&mut self.resp_header_buf)))
            }
            Ok(httparse::Status::Partial) => Poll::Pending,
            Err(e) => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    Error(e.to_string()),
                )))
            }
        }
    }
}

#[async_trait]
impl Runner for Http1Runner {
    async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(size) = size_hint {
            self.req
                .headers
                .push(("Content-Length".into(), format!("{size}").into()));
        }
        let mut header = self.compute_header();
        self.header_sent = true;
        self.stream.write_all_buf(&mut header).await?;
        if let Some(p) = self.req.pause.iter().find(|p| p.after == "headers") {
            self.stream.flush().await?;
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        self.stream
            .inner_mut()
            .start(Some(header.len() + size_hint.unwrap_or(0)))
            .await?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Send headers.
        self.start(Some(self.req.body.len())).await?;

        if !self.req.body.is_empty() {
            let body = std::mem::take(&mut self.req.body);
            self.write_all(body.as_slice()).await?;
            self.req.body = body;
        }
        self.stream.flush().await?;
        if let Some(p) = self.req.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        let mut response = Vec::new();
        self.read_to_end(&mut response).await?;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        let (stream, writes, reads) = self.stream.into_parts();

        // Update the response body to the actual data that was sent since it will differ for
        // layered protocols.
        self.req.body = writes;

        // The response should always be set once the header has been read.
        let Some(mut resp) = self.resp else {
            return Err(Error("closing before response headers received".to_owned()));
        };
        resp.body = reads;
        resp.duration = self.start_time.elapsed();
        Ok((
            Output::Http1(Http1Output {
                request: self.req,
                response: resp,
            }),
            Some(stream),
        ))
    }
}

fn contains_header(headers: &[(String, String)], key: &str) -> bool {
    headers
        .iter()
        .find(|(k, _)| key.eq_ignore_ascii_case(k))
        .is_some()
}
