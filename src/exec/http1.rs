use std::mem;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use cel_interpreter::Duration;
use chrono::TimeDelta;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::pause;
use super::pause::PauseSpec;
use super::pause::PauseStream;
use super::runner::Runner;
use super::Context;
use crate::AddContentLength;
use crate::Http1Error;
use crate::Http1PlanOutput;
use crate::Http1RequestOutput;
use crate::{Http1Output, Http1Response};

#[derive(Debug)]
pub(super) struct Http1Runner {
    out: Http1Output,
    state: State,
    start_time: Option<Instant>,
    req_header_start_time: Option<Instant>,
    req_body_start_time: Option<Instant>,
    req_end_time: Option<Instant>,
    resp_start_time: Option<Instant>,
    resp_header_end_time: Option<Instant>,
    first_read: Option<Instant>,
    shutdown_time: Option<Instant>,
    resp_header_buf: BytesMut,
    req_body_buf: Vec<u8>,
    resp_body_buf: Vec<u8>,
    size_hint: Option<usize>,
    send_headers: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug)]
enum State {
    Pending { ctx: Arc<Context> },
    Ready { ctx: Arc<Context>, header: BytesMut },
    StartFailed { transport: Runner },
    SendingHeader { transport: PauseStream<Runner> },
    SendingBody { transport: PauseStream<Runner> },
    ReceivingHeader { transport: PauseStream<Runner> },
    ReceivingBody { transport: PauseStream<Runner> },
    Complete { transport: Option<Runner> },
    Invalid,
}

impl AsyncRead for Http1Runner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut state = std::mem::replace(&mut self.state, State::Invalid);

        // Update the state to ReceivingHeader.
        if let State::SendingBody { transport } = state {
            state = State::ReceivingHeader { transport };
        }

        match state {
            State::ReceivingHeader { mut transport } => {
                // Record the time we start listening for a response.
                if self.resp_start_time.is_none() {
                    self.resp_start_time = Some(Instant::now());
                }
                let poll = self.poll_header(cx, buf, &mut transport);
                self.state = match &poll {
                    Poll::Ready(Ok(())) => {
                        // Schedule planned pauses for the response body.
                        //transport.add_reads([
                        //    PauseSpec {
                        //        group_offset: 0,
                        //        plan: self.out.plan.pause.response_headers.end.clone(),
                        //    },
                        //    PauseSpec {
                        //        plan: self.out.plan.pause.response_body.start.clone(),
                        //        group_offset: 0,
                        //    },
                        //]);
                        //if let Some(content_length) =
                        //    self.out.response.as_ref().and_then(|r| r.content_length)
                        //{
                        //    transport.add_reads([PauseSpec {
                        //        plan: self.out.plan.pause.response_body.end.clone(),
                        //        group_offset: content_length.try_into().unwrap(),
                        //    }]);
                        //} else if self
                        //    .out
                        //    .plan
                        //    .pause
                        //    .response_body
                        //    .end
                        //    .iter()
                        //    .any(|p| p.offset_bytes < 0)
                        //{
                        //    self.out.errors.push(Http1Error {
                        //            kind: "unsupported pause".to_owned(),
                        //            message:
                        //                "response must include Content-Length header to use http1.plan.pause.response_body.end with a negative offset".to_owned(),
                        //        });
                        //}

                        State::ReceivingBody { transport }
                    }
                    _ => State::ReceivingHeader { transport },
                };
                poll
            }

            State::ReceivingBody { mut transport } => {
                let old_len = buf.filled().len();
                let poll = pin!(&mut transport).poll_read(cx, buf);
                self.resp_body_buf
                    .extend_from_slice(&buf.filled()[old_len..]);
                self.state = State::ReceivingBody { transport };
                poll
            }
            state => panic!("unexpected state {:?} for http1 poll_read", state),
        }
    }
}

impl AsyncWrite for Http1Runner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut self.state {
            State::SendingHeader { transport, .. } => pin!(transport).poll_write(cx, buf),
            State::SendingBody { transport, .. } => {
                let poll = pin!(transport).poll_write(cx, buf);
                if poll.is_ready() {
                    if self.req_body_start_time.is_none() {
                        self.req_body_start_time = Some(Instant::now());
                    }
                    if let Poll::Ready(Ok(len)) = &poll {
                        self.get_mut().req_body_buf.extend_from_slice(&buf[0..*len]);
                    }
                }
                poll
            }
            _ => panic!("unexpected state for http1 poll_write"),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let (State::SendingHeader { transport, .. }
        | State::SendingBody { transport, .. }
        | State::ReceivingHeader { transport, .. }
        | State::ReceivingBody { transport, .. }) = &mut self.state
        else {
            panic!("unexpected state for http1 poll_flush");
        };
        pin!(transport).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let poll = match &mut self.state {
            State::SendingHeader { transport, .. }
            | State::SendingBody { transport, .. }
            | State::ReceivingHeader { transport, .. }
            | State::ReceivingBody { transport, .. } => pin!(transport).poll_shutdown(cx),
            State::Complete {
                transport: Some(transport),
            } => pin!(transport).poll_shutdown(cx),
            state => panic!("unexpected state {state:?} for http1 poll_shutdown"),
        };
        if poll.is_ready() && self.shutdown_time.is_none() {
            self.shutdown_time = Some(Instant::now());
        }
        poll
    }
}

impl Http1Runner {
    pub(super) fn new(ctx: Arc<Context>, plan: Http1PlanOutput) -> Self {
        Self {
            state: State::Pending { ctx },
            send_headers: plan.headers.clone(),
            out: Http1Output {
                request: None,
                response: None,
                errors: Vec::new(),
                duration: TimeDelta::zero().into(),
                //pause: crate::Http1PauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
            start_time: None,
            req_header_start_time: None,
            req_body_start_time: None,
            req_end_time: None,
            resp_start_time: None,
            resp_header_end_time: None,
            first_read: None,
            shutdown_time: None,
            resp_header_buf: BytesMut::new(),
            req_body_buf: Vec::new(),
            resp_body_buf: Vec::new(),
            size_hint: None,
        }
    }

    #[inline]
    fn compute_header(plan: &Http1PlanOutput, headers: &[(Vec<u8>, Vec<u8>)]) -> BytesMut {
        // Build a buffer with the header contents to avoid the overhead of separate writes.
        // TODO: We may actually want to split packets based on info at the HTTP layer, that logic
        // will go here once I figure out the right configuration to express it.
        let mut buf = BytesMut::with_capacity(
            plan.method.as_ref().map(Vec::len).unwrap_or(0)
                + 1
                + plan.url.path().len()
                + plan.url.query().map(|x| x.len() + 1).unwrap_or(0)
                + 1
                + plan.version_string.as_ref().map(Vec::len).unwrap_or(0)
                + 2
                + headers
                    .iter()
                    .fold(0, |sum, (k, v)| sum + k.len() + 2 + v.len() + 2)
                + 2
                + plan.body.len(),
        );
        if let Some(m) = &plan.method {
            buf.put_slice(m);
        }
        buf.put_u8(b' ');
        buf.put_slice(plan.url.path().as_bytes());
        if let Some(q) = plan.url.query() {
            buf.put_u8(b'?');
            buf.put_slice(q.as_bytes());
        }
        buf.put_u8(b' ');
        if let Some(p) = &plan.version_string {
            buf.put_slice(p);
        }
        buf.put(b"\r\n".as_slice());
        for (k, v) in headers {
            buf.put_slice(k.as_slice());
            buf.put_slice(b": ");
            buf.put_slice(v.as_slice());
            buf.put_slice(b"\r\n");
        }
        buf.put(b"\r\n".as_slice());
        buf
    }

    fn poll_header(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
        transport: &mut PauseStream<Runner>,
    ) -> Poll<std::io::Result<()>> {
        // Don't read in more bytes at a time than we could fit in buf if there's extra after
        // reading the header.
        // TODO: optimize this to avoid the intermediate allocation and write.
        let mut header_vec = vec![0; buf.remaining() + 1];
        loop {
            let mut header_buf = ReadBuf::new(header_vec.as_mut());
            let poll = pin!(&mut *transport).poll_read(cx, &mut header_buf);
            // Record when we first get any response data.
            if poll.is_ready() && self.first_read.is_none() {
                self.first_read = Some(Instant::now());
            }
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
            match self.receive_header() {
                // Not enough data, let's read some more.
                Poll::Pending => {}
                // The full header was read, read the leftover bytes as part of the body.
                Poll::Ready(Ok(remaining)) => {
                    self.resp_header_end_time = Some(Instant::now());
                    self.resp_body_buf.extend_from_slice(&remaining);
                    buf.put(remaining);
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
    }

    #[inline]
    fn receive_header(&mut self) -> Poll<std::io::Result<BytesMut>> {
        // TODO: Write our own extra-permissive parser.
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut resp = httparse::Response::new(&mut headers);
        match resp.parse(&self.resp_header_buf) {
            Ok(result) => {
                let header_complete_time = Instant::now();
                // Set the header fields in our response.
                self.out.response = Some(Http1Response {
                    protocol: resp.version.map(|v| format!("HTTP/1.{}", v).into()),
                    status_code: resp.code,
                    // Use the first valid Content-Length header as the content length, if any.
                    content_length: resp
                        .headers
                        .iter()
                        .filter(|h| h.name.eq_ignore_ascii_case("content-length"))
                        .find_map(|h| atoi::atoi(h.value)),
                    // If the reason hasn't been read yet then also no headers were parsed.
                    headers: resp.reason.as_ref().map(|_| {
                        resp.headers
                            .into_iter()
                            .map(|h| (Vec::from(h.name), Vec::from(h.value)))
                            .collect()
                    }),
                    status_reason: resp.reason.map(Vec::from),
                    body: None,
                    duration: TimeDelta::zero().into(),
                    header_duration: None,
                    time_to_first_byte: self
                        .first_read
                        .map(|first_read| {
                            self.resp_start_time
                                .map(|start| first_read - start)
                                .unwrap_or_default()
                        })
                        .map(TimeDelta::from_std)
                        .transpose()
                        .expect("durations should fit in std")
                        .map(Duration),
                });
                match result {
                    httparse::Status::Partial => Poll::Pending,
                    httparse::Status::Complete(body_start) => {
                        self.out.response.as_mut().unwrap().header_duration = Some(
                            TimeDelta::from_std(header_complete_time - self.start_time.unwrap())
                                .unwrap()
                                .into(),
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
                    anyhow!(e),
                )))
            }
        }
    }

    pub fn size_hint(&mut self, size_hint: Option<usize>) -> Option<usize> {
        let State::Pending { ctx } = mem::replace(&mut self.state, State::Invalid) else {
            panic!("invalid state {:?}", self.state);
        };

        self.size_hint = size_hint;

        // Add a Content-Length header if the size_hint has a value and either:
        //   automatic_content_length is auto (the default),
        //   we don't have a content length header specified,
        //   and TODO: we aren't using chunked transport encoding
        // or
        //   automatic_content_length is force
        if let Some(size_hint) = size_hint {
            if self.out.plan.add_content_length == AddContentLength::Force
                || self.out.plan.add_content_length == AddContentLength::Auto
                    && self
                        .send_headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(b"content-length"))
                        .is_none()
            //&& self.out.plan.chunked_transfer_encoding != ChunkedTransferEncoding::Force
            {
                self.send_headers.push((
                    b"Content-Length".to_vec(),
                    size_hint.to_string().into_bytes(),
                ))
            }
        }

        let header = Self::compute_header(&self.out.plan, &self.send_headers);
        let header_len = header.len();
        self.state = State::Ready { ctx, header };

        size_hint.map(|hint| header_len + hint)
    }

    pub async fn start(&mut self, transport: Runner) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Ready { mut header, ctx } = state else {
            bail!("attempt to start Http1Runner from invalid state");
        };

        let header_len = i64::try_from(header.len()).unwrap();
        let mut transport = pause::new_stream(
            ctx,
            transport,
            [/*PauseSpec {
                group_offset: 0,
                plan: self.out.plan.pause.response_headers.start.clone(),
            }*/],
            [/*
                PauseSpec {
                    plan: self.out.plan.pause.request_headers.start.clone(),
                    group_offset: 0,
                },
                PauseSpec {
                    plan: self.out.plan.pause.request_headers.end.clone(),
                    group_offset: header_len,
                },
                PauseSpec {
                    plan: self.out.plan.pause.request_body.start.clone(),
                    group_offset: header_len,
                },
            */],
        );
        //if let Some(size_hint) = self.size_hint {
        //    transport.add_writes([PauseSpec {
        //        plan: self.out.plan.pause.request_body.end.clone(),
        //        group_offset: i64::try_from(size_hint).unwrap() + header_len,
        //    }]);
        //} else {
        //    if self
        //        .out
        //        .plan
        //        .pause
        //        .request_body
        //        .end
        //        .iter()
        //        .any(|p| p.offset_bytes < 0)
        //    {
        //        bail!(
        //            "http1.pause.request_body.end with negative offset is unsupported in this request"
        //        );
        //    }
        //}

        self.start_time = Some(Instant::now());
        self.state = State::SendingHeader { transport };

        self.req_header_start_time = Some(Instant::now());
        self.write_all_buf(&mut header).await?;

        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::SendingHeader { transport } = state else {
            panic!("invalid state after HTTP/1 header write");
        };

        self.state = State::SendingBody { transport };

        self.out.request = Some(Http1RequestOutput {
            url: self.out.plan.url.clone(),
            headers: self.send_headers.clone(),
            method: self.out.plan.method.clone(),
            version_string: self.out.plan.version_string.clone(),
            body: Vec::new(),
            duration: TimeDelta::zero().into(),
            body_duration: None,
            time_to_first_byte: None,
        });
        Ok(())
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn execute(&mut self) {
        if !self.out.plan.body.is_empty() {
            let body = std::mem::take(&mut self.out.plan.body);
            if let Err(e) = self.write_all(body.as_slice()).await {
                self.out.errors.push(Http1Error {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                return;
            }
            self.out.plan.body = body;
        }
        if let Err(e) = self.flush().await {
            self.out.errors.push(Http1Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.read_to_end(&mut response).await {
            self.out.errors.push(Http1Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
    }

    pub fn finish(mut self) -> (Http1Output, Option<Runner>) {
        self.complete();
        let State::Complete { transport } = self.state else {
            unreachable!();
        };
        (self.out, transport)
    }

    fn complete(&mut self) {
        let end_time = self.shutdown_time.unwrap_or_else(Instant::now);

        let state = std::mem::replace(&mut self.state, State::Invalid);
        let transport = match state {
            State::SendingHeader { transport }
            | State::SendingBody { transport }
            | State::ReceivingHeader { transport }
            | State::ReceivingBody { transport } => transport,
            // If we've already cleaned up or we never did anything then the output doesn't need
            // post processing.
            State::Complete { transport } => {
                self.state = State::Complete { transport };
                return;
            }
            State::StartFailed { transport } => {
                self.state = State::Complete {
                    transport: Some(transport),
                };
                return;
            }
            State::Pending { .. } | State::Ready { .. } => {
                self.state = State::Complete { transport: None };
                return;
            }
            State::Invalid => panic!(),
        };

        let (inner, write_pause, read_pause) = transport.finish_stream();
        //let mut write_pause = write_pause.into_iter();
        //self.out.pause.request_headers.start = write_pause.next().unwrap_or_default();
        //self.out.pause.request_headers.end = write_pause.next().unwrap_or_default();
        //self.out.pause.request_body.start = write_pause.next().unwrap_or_default();
        //self.out.pause.request_body.end = write_pause.next().unwrap_or_default();
        //assert!(write_pause.next().is_none(), "leftover write pause output");
        //let mut read_pause = read_pause.into_iter();
        //self.out.pause.response_headers.start = read_pause.next().unwrap_or_default();
        //self.out.pause.response_headers.end = read_pause.next().unwrap_or_default();
        //self.out.pause.response_body.start = read_pause.next().unwrap_or_default();
        //self.out.pause.response_body.end = read_pause.next().unwrap_or_default();
        //assert!(read_pause.next().is_none(), "leftover read pause output");

        let start_time = self.start_time.unwrap();

        if let Some(req) = &mut self.out.request {
            req.duration = TimeDelta::from_std(self.req_end_time.unwrap_or(end_time) - start_time)
                .unwrap()
                .into();
            req.body_duration = self
                .req_body_start_time
                .map(|start| self.resp_start_time.unwrap_or(end_time) - start)
                .map(TimeDelta::from_std)
                .transpose()
                .unwrap()
                .map(Duration);
            req.time_to_first_byte = self
                .req_header_start_time
                .map(|header_start| header_start - start_time)
                .map(TimeDelta::from_std)
                .transpose()
                .unwrap()
                .map(Duration);
            req.body = self.req_body_buf.to_vec();
        }

        // The response should be set if the header has been read.
        if let Some(resp) = &mut self.out.response {
            resp.body = Some(self.resp_body_buf.to_vec());
            resp.duration = TimeDelta::from_std(
                self.resp_start_time
                    .map(|start| end_time - start)
                    .unwrap_or_default(),
            )
            .unwrap()
            .into();
            resp.header_duration = self
                .resp_header_end_time
                .map(|end| {
                    self.resp_start_time
                        .map(|start| end - start)
                        .unwrap_or_default()
                })
                .map(TimeDelta::from_std)
                .transpose()
                .unwrap()
                .map(Duration);
        }

        self.state = State::Complete {
            transport: Some(inner),
        };
        self.out.duration = TimeDelta::from_std(end_time - start_time).unwrap().into();
    }
}
