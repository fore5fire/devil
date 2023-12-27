use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use chrono::Duration;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::runner::Runner;
use crate::Http1Error;
use crate::Http1PlanOutput;
use crate::Http1RequestOutput;
use crate::{Error, Http1Output, Http1Response, Output};

#[derive(Debug)]
pub(super) struct Http1Runner {
    out: Http1Output,
    stream: Box<dyn Runner>,
    start_time: Instant,
    req_end_time: Option<Instant>,
    resp_start_time: Option<Instant>,
    first_read: Option<Instant>,
    end_time: Option<Instant>,
    resp_header_buf: BytesMut,
    req_body_buf: Vec<u8>,
    resp_body_buf: Vec<u8>,
}

impl AsyncRead for Http1Runner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // If we've already read the header then read and record body bytes.
        if self
            .out
            .response
            .as_ref()
            .and_then(|resp| resp.header_duration)
            .is_some()
        {
            let old_len = buf.filled().len();
            let poll = Pin::new(&mut self.stream).poll_read(cx, buf);
            self.resp_body_buf
                .extend_from_slice(&buf.filled()[old_len..]);
            return poll;
        }

        // Record the response start time if this is our first read poll and we didn't explicitly
        // start it in execute (running as a transport).
        if self.resp_start_time.is_none() {
            self.resp_start_time = Some(Instant::now());
        }

        // Don't read in more bytes at a time than we could fit in buf if there's extra after
        // reading the header.
        // TODO: optimize this to avoid the intermediate allocation and write.
        let mut header_vec = vec![0; buf.remaining() + 1];
        loop {
            let mut header_buf = ReadBuf::new(header_vec.as_mut());
            let poll = Pin::new(&mut self.stream).poll_read(cx, &mut header_buf);
            self.resp_header_buf.put_slice(header_buf.filled());
            match poll {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                // If no data was read then the stream has ended.
                Poll::Ready(Ok(())) => {
                    if header_buf.filled().len() == 0 {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "header incomplete".to_owned(),
                        )));
                    }
                }
            }
            // Data was read - try to process it.
            if self.first_read.is_none() {
                self.first_read = Some(Instant::now());
            }
            match self.receive_header() {
                // Not enough data, let's read some more.
                Poll::Pending => {}
                // The full header was read, read the leftover bytes as part of the body.
                Poll::Ready(Ok(remaining)) => {
                    self.resp_body_buf.extend_from_slice(&remaining);
                    buf.put(remaining);
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncWrite for Http1Runner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let poll = Pin::new(&mut self.stream).poll_write(cx, buf);
        if poll.is_ready() {
            self.get_mut().req_body_buf.extend_from_slice(&buf);
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

impl Http1Runner {
    pub(super) async fn new(stream: Box<dyn Runner>, plan: Http1PlanOutput) -> crate::Result<Self> {
        let start_time = Instant::now();

        if let Some(p) = plan.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        Ok(Self {
            out: Http1Output {
                plan,
                request: None,
                response: None,
                error: None,
                duration: Duration::zero(),
            },
            stream,
            start_time,
            req_end_time: None,
            resp_start_time: None,
            first_read: None,
            end_time: None,
            resp_header_buf: BytesMut::new(),
            req_body_buf: Vec::new(),
            resp_body_buf: Vec::new(),
        })
    }

    #[inline]
    fn compute_header(&self) -> BytesMut {
        // Build a buffer with the header contents to avoid the overhead of separate writes.
        // TODO: We may actually want to split packets based on info at the HTTP layer, that logic
        // will go here once I figure out the right configuration to express it.
        let mut buf = BytesMut::with_capacity(
            self.out.plan.method.as_ref().map(Vec::len).unwrap_or(0)
                + 1
                + self.out.plan.url.path().len()
                + self.out.plan.url.query().map(|x| x.len() + 1).unwrap_or(0)
                + 1
                + self
                    .out
                    .plan
                    .version_string
                    .as_ref()
                    .map(Vec::len)
                    .unwrap_or(0)
                + 2
                + self
                    .out
                    .plan
                    .headers
                    .iter()
                    .fold(0, |sum, (k, v)| sum + k.len() + 2 + v.len() + 2)
                + 2
                + self.out.plan.body.len(),
        );
        if let Some(m) = &self.out.plan.method {
            buf.put_slice(m);
        }
        buf.put_u8(b' ');
        buf.put_slice(self.out.plan.url.path().as_bytes());
        if let Some(q) = self.out.plan.url.query() {
            buf.put_u8(b'?');
            buf.put_slice(q.as_bytes());
        }
        buf.put_u8(b' ');
        if let Some(p) = &self.out.plan.version_string {
            buf.put_slice(p);
        }
        buf.put(b"\r\n".as_slice());
        for (k, v) in &self.out.plan.headers {
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
            Ok(result) => {
                let header_complete_time = Instant::now();
                // Set the header fields in our response.
                self.out.response = Some(Http1Response {
                    protocol: resp.version.map(|v| format!("HTTP/1.{}", v).into()),
                    status_code: resp.code,
                    // If the reason hasn't been read yet then also no headers were parsed.
                    headers: resp.reason.as_ref().map(|_| {
                        resp.headers
                            .into_iter()
                            .map(|h| (Vec::from(h.name), Vec::from(h.value)))
                            .collect()
                    }),
                    status_reason: resp.reason.map(Vec::from),
                    body: None,
                    duration: chrono::Duration::zero(),
                    body_duration: None,
                    header_duration: None,
                    time_to_first_byte: self.first_read.map(|first_read| {
                        Duration::from_std(
                            first_read
                                - self.resp_start_time.expect(
                                    "response start time should be set before header is processed",
                                ),
                        )
                        .unwrap()
                    }),
                });
                match result {
                    httparse::Status::Partial => Poll::Pending,
                    httparse::Status::Complete(body_start) => {
                        self.out.response.as_mut().unwrap().header_duration = Some(
                            Duration::from_std(header_complete_time - self.start_time).unwrap(),
                        );
                        // Return the bytes we didn't read.
                        self.resp_header_buf.advance(body_start);
                        Poll::Ready(Ok(std::mem::take(&mut self.resp_header_buf)))
                    }
                }
            }
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
            self.out
                .plan
                .headers
                .push(("Content-Length".into(), format!("{size}").into()));
        }
        let mut header = self.compute_header();
        self.out.request = Some(Http1RequestOutput {
            url: self.out.plan.url.clone(),
            headers: self.out.plan.headers.clone(),
            method: self.out.plan.method.clone(),
            version_string: self.out.plan.version_string.clone(),
            pause: Vec::new(),
            body: Vec::new(),
            duration: Duration::zero(),
            body_duration: None,
            header_duration: None,
            time_to_first_byte: None,
        });
        self.stream.write_all_buf(&mut header).await?;
        if let Some(p) = self.out.plan.pause.iter().find(|p| p.after == "headers") {
            self.stream.flush().await?;
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        // Write directly to the transport isntead of self so we don't record the header as the
        // body.
        self.stream
            .start(Some(header.len() + size_hint.unwrap_or(0)))
            .await?;
        Ok(())
    }

    async fn execute(&mut self) {
        // Send headers.
        if let Err(e) = self.start(Some(self.out.plan.body.len())).await {
            self.out.error = Some(Http1Error {
                kind: "send headers".to_owned(),
                message: e.to_string(),
            });
            return;
        }

        if !self.out.plan.body.is_empty() {
            let body = std::mem::take(&mut self.out.plan.body);
            if let Err(e) = self.write_all(body.as_slice()).await {
                self.out.error = Some(Http1Error {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                return;
            }
            self.out.plan.body = body;
        }
        if let Err(e) = self.stream.flush().await {
            self.out.error = Some(Http1Error {
                kind: e.kind().to_string(),
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
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        self.resp_start_time = Some(Instant::now());
        let mut response = Vec::new();
        if let Err(e) = self.read_to_end(&mut response).await {
            self.out.error = Some(Http1Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
    }

    async fn finish(mut self: Box<Self>) -> (Output, Option<Box<dyn Runner>>) {
        let end_time = self.end_time.unwrap_or_else(Instant::now);

        if let Some(req) = &mut self.out.request {
            req.duration =
                Duration::from_std(self.req_end_time.unwrap_or(end_time) - self.start_time)
                    .unwrap();
            req.body = self.req_body_buf.to_vec();
        }

        // The response should be set if the header has been read.
        if let Some(resp) = &mut self.out.response {
            resp.body = Some(self.resp_body_buf.to_vec());
            resp.duration = Duration::from_std(
                end_time
                    - self
                        .resp_start_time
                        .expect("response start time should be recorded when response is set"),
            )
            .unwrap();
        }

        self.out.duration = Duration::from_std(end_time - self.start_time).unwrap();

        (Output::Http1(self.out), Some(self.stream))
    }
}
