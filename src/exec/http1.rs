use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::runner::Runner;
use super::tee::Stream;
use super::tee::Tee;
use crate::Error;
use crate::HTTP1Output;
use crate::Output;
use crate::{HTTPOutput, HTTPResponse};

#[derive(Debug)]
pub(super) struct HTTP1Runner<S: Stream> {
    out: HTTP1Output,
    stream: Tee<S>,
    start_time: Instant,
    header_sent: bool,
}

impl<S: Stream> AsyncRead for HTTP1Runner<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Read the first bytes of the response as the header.
        if self.out.response.is_none() {
            let buf = self.as_ref().receive_header();
        }
        Pin::new(&mut self.as_ref().stream).poll_read(cx, buf)
    }
}

impl<S: Stream> AsyncWrite for HTTP1Runner<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Write the first bytes of the request as the header.
        if !self.header_sent {
            self.as_ref().send_header();
        }
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

impl<S: Stream> HTTP1Runner<S> {
    pub(super) async fn new(stream: S, data: HTTP1Output) -> crate::Result<Self> {
        let start_time = Instant::now();

        if let Some(p) = data.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration).await;
        }
        Ok(Self {
            stream: Tee::new(stream),
            start_time,
            out: data,
            header_sent: false,
        })
    }

    #[inline]
    async fn send_header(&mut self) -> std::io::Result<()> {
        // Build a buffer with the header contents to avoid the overhead of separate writes.
        // TODO: We may actually want to split packets based on info at the HTTP layer, that logic
        // will go here once I figure out the right configuration to express it.
        let mut buf = Vec::with_capacity(
            self.out.method.len()
                + 1
                + self.out.url.path().len()
                + self.out.url.query().map(|x| x.len() + 1).unwrap_or(0)
                + 1
                + self.out.protocol.len()
                + 2
                + self
                    .out
                    .headers
                    .iter()
                    .fold(0, |sum, (k, v)| sum + k.len() + 2 + v.len() + 2)
                + 2
                + self.out.body.as_ref().map(Vec::len).unwrap_or(0),
        );
        buf.extend_from_slice(self.out.method.as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(self.out.url.path().as_bytes());
        if let Some(q) = self.out.url.query() {
            buf.push(b'?');
            buf.extend_from_slice(q.as_bytes());
        }
        buf.push(b' ');
        buf.extend_from_slice(self.out.protocol.as_bytes());
        buf.extend(b"\r\n");
        for (k, v) in &self.out.headers {
            buf.extend_from_slice(k.as_bytes());
            buf.extend(b": ");
            buf.extend_from_slice(v.as_bytes());
            buf.extend(b"\r\n");
        }
        buf.extend(b"\r\n");

        // Send the header data.
        self.stream.write_all(buf.as_slice()).await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "headers") {
            self.stream.flush().await?;
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration).await;
        }
        Ok(())
    }

    #[inline]
    async fn receive_header(&mut self) -> crate::Result<bytes::BytesMut> {
        let mut buf = bytes::BytesMut::new();

        // TODO: Write our own extra-permissive parser.
        loop {
            let mut headers = [httparse::EMPTY_HEADER; 16];
            let mut resp = httparse::Response::new(&mut headers);
            self.stream
                .read_buf(&mut buf)
                .await
                .map_err(|e| Error(e.to_string()))?;
            match resp.parse(&buf) {
                Ok(httparse::Status::Complete(body_start)) => {
                    // Update the header fields in our output.
                    self.out.response = Some(HTTPResponse {
                        protocol: format!("HTTP/1.{}", resp.version.unwrap()),
                        status_code: resp.code.unwrap(),
                        status_reason: resp.reason.unwrap().to_owned(),
                        headers: resp
                            .headers
                            .into_iter()
                            .map(|h| {
                                (
                                    h.name.to_owned(),
                                    String::from_utf8_lossy(h.value).to_string(),
                                )
                            })
                            .collect(),
                        body: Vec::new(),
                        duration: Duration::ZERO,
                    });
                    return Ok(buf.split_off(body_start));
                }
                Ok(httparse::Status::Partial) => drop(resp),
                Err(e) => return Err(Error(e.to_string())),
            }
        }
    }
}

#[async_trait]
impl Runner for HTTP1Runner<Box<dyn Runner>> {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Headers will be sent automatically before the first body byte.
        if let Some(body) = &self.out.body {
            self.size_hint(body.len());
            self.stream.write_all(body.as_slice()).await?;
        }
        self.stream.flush().await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration).await;
        }
        let mut response = Vec::new();
        self.stream.read_to_end(&mut response).await?;
        Ok(())
    }

    async fn finish(mut self) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        let (stream, writes, reads) = self.stream.into_parts();

        // Update the response body to the actual data that was sent since it will differ for
        // layered protocols.
        self.out.body = Some(writes);

        // The response should always be set once the header has been read.
        if let Some(response) = &mut self.out.response {
            response.body = reads;
            response.duration = self.start_time.elapsed();
        }
        Ok((Output::HTTP(self.out), Some(stream)))
    }

    fn size_hint(&mut self, size: usize) {
        if self.header_sent {
            panic!("size_hint called after header was already sent")
        }
        if self.out.automatic_content_length {
            self.out
                .headers
                .push(("Content-Length".to_owned(), format!("{size}")));
        }
    }
}

fn contains_header(headers: &[(String, String)], key: &str) -> bool {
    headers
        .iter()
        .find(|(k, _)| key.eq_ignore_ascii_case(k))
        .is_some()
}
