use std::{
    future, mem,
    pin::pin,
    sync::Arc,
    task::{ready, Poll},
    time::Instant,
};

use anyhow::bail;
use bytes::{Bytes, BytesMut};
use cel_interpreter::Duration;
use chrono::TimeDelta;
use futures::FutureExt;
use h2::{client::SendRequest, Reason, RecvStream, SendStream};
use http::{response::Parts, HeaderMap, HeaderName, HeaderValue, Request, Uri};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    join,
    task::JoinHandle,
};

use crate::{
    AddContentLength, Http2Error, Http2Output, Http2PlanOutput, Http2RequestOutput, MaybeUtf8,
};

use super::{
    pause::{PauseReader, PauseSpec, PauseWriter},
    raw_http2::RawHttp2Runner,
    Context,
};

#[derive(Debug)]
pub struct Http2Runner {
    ctx: Arc<Context>,
    out: Http2Output,
    read_state: ReadState,
    write_state: WriteState,
    transport: Option<RawHttp2Runner>,
    start_time: Option<Instant>,
    header_send_start: Option<Instant>,
    header_send_end: Option<Instant>,
    trailer_send_start: Option<Instant>,
    trailer_send_end: Option<Instant>,
    first_read: Option<Instant>,
    header_receive_start: Option<Instant>,
    header_receive_end: Option<Instant>,
    trailer_receive_start: Option<Instant>,
    trailer_receive_end: Option<Instant>,
    shutdown_start: Option<Instant>,
    shutdown_end: Option<Instant>,
    size_hint: Option<usize>,
    close_reason: Option<h2::Reason>,
    send_headers: http::HeaderMap,
    receive_trailers: Option<http::HeaderMap>,
    stream_id: Option<u32>,
}

#[derive(Debug)]
enum WriteState {
    Pending {
        request_out: Http2RequestOutput,
    },
    Ready {
        request: http::Request<()>,
        request_out: Http2RequestOutput,
    },
    StartFailed,
    Body {
        stream: PauseWriter<SendTransport>,
    },
    Completed {
        stream: Option<SendTransport>,
    },
    Invalid,
}

#[derive(Debug)]
enum ReadState {
    Pending,
    StartFailed,
    Headers {
        response: futures::future::Fuse<JoinHandle<Result<http::Response<RecvStream>, h2::Error>>>,
    },
    Body {
        head: Parts,
        body: PauseReader<RecvTransport>,
    },
    Completed,
    Invalid,
}

impl Http2Runner {
    pub(super) fn new(ctx: Arc<Context>, plan: Http2PlanOutput) -> crate::Result<Self> {
        Ok(Self {
            ctx,
            write_state: WriteState::Pending {
                request_out: Http2RequestOutput {
                    url: plan.url.clone(),
                    method: plan.method.clone(),
                    headers: plan.headers.clone(),
                    body: plan.body.clone(),
                    duration: TimeDelta::zero().into(),
                    headers_duration: None,
                    body_duration: None,
                    time_to_first_byte: None,
                },
            },
            read_state: ReadState::Pending,
            out: Http2Output {
                plan,
                request: None,
                response: None,
                errors: Vec::new(),
                duration: TimeDelta::zero().into(),
            },
            transport: None,
            start_time: None,
            header_send_start: None,
            header_send_end: None,
            trailer_send_start: None,
            trailer_send_end: None,
            first_read: None,
            header_receive_start: None,
            header_receive_end: None,
            trailer_receive_start: None,
            trailer_receive_end: None,
            shutdown_start: None,
            shutdown_end: None,
            size_hint: None,
            close_reason: None,
            receive_trailers: None,
            send_headers: HeaderMap::new(),
            stream_id: None,
        })
    }

    fn build_request(plan: &Http2PlanOutput) -> Result<http::Request<()>, http::Error> {
        let uri: Uri = plan.url.as_str().parse().unwrap();
        let mut req = Request::builder().uri(uri).method(
            plan.method
                .as_ref()
                .expect("missing method is not yet supported for http2")
                .as_str()
                .expect("non-utf8 method is not yet supported with http2"),
        );
        for (k, v) in &plan.headers {
            req = req.header(k.as_bytes(), v.as_bytes());
        }
        req.body(())
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        self.size_hint = hint;
        // Add a Content-Length header if the size_hint has a value and either:
        //   automatic_content_length is auto (the default) and
        //   we don't have a content length header specified,
        // or
        //   automatic_content_length is force
        if let Some(size_hint) = hint {
            if self.out.plan.add_content_length == AddContentLength::Force
                || self.out.plan.add_content_length == AddContentLength::Auto
                    && self
                        .send_headers
                        .iter()
                        .find(|(k, _)| k.as_str().eq_ignore_ascii_case("content-length"))
                        .is_none()
            {
                self.send_headers.append(
                    HeaderName::from_static("content-length"),
                    HeaderValue::from_str(&size_hint.to_string())
                        .expect("u64::to_string should always produce a valid header value"),
                );
            }
        }

        let WriteState::Pending { request_out } =
            mem::replace(&mut self.write_state, WriteState::Invalid)
        else {
            panic!("invalid write_state for size_hint {:?}", self.write_state)
        };
        self.write_state = WriteState::Ready {
            request: Self::build_request(&self.out.plan).expect("build h2 request: {e}"),
            request_out,
        };

        None
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn start(&mut self, mut transport: RawHttp2Runner) -> anyhow::Result<()> {
        let stream = transport.new_stream();
        self.transport = Some(transport);
        self.start_shared(stream.expect("a single stream should be available for non-shared http2"))
            .await
    }

    pub async fn start_shared(&mut self, transport: SendRequest<Bytes>) -> anyhow::Result<()> {
        let state = mem::replace(&mut self.write_state, WriteState::Invalid);
        let WriteState::Ready {
            request,
            mut request_out,
        } = state
        else {
            self.write_state = state;
            bail!(
                "attempt to start Http2Runner from invalid state {:?}",
                self.write_state
            );
        };

        let need_send_stream =
            self.size_hint.is_some_and(|size| size > 0) || !self.out.plan.trailers.is_empty();

        let start = Instant::now();
        self.start_time = Some(start);

        let mut conn = transport.ready().await?;
        let (response, send_stream) = conn.send_request(request, !need_send_stream)?;
        self.stream_id = Some(response.stream_id().into());

        // Setup the response to write to the stream once its ready.

        self.write_state = if need_send_stream {
            WriteState::Body {
                stream: PauseWriter::new(
                    self.ctx.clone(),
                    SendTransport::new(send_stream, self.size_hint),
                    [/*
                        PauseSpec {
                            group_offset: 0,
                            plan: self.out.plan.pause.request_body.start.clone(),
                        },
                        self.size_hint
                            .map(|size| PauseSpec {
                                group_offset: size.try_into().expect(
                                    "request body size exceeding addressable platform size not yet supported",
                                ),
                                plan: self.out.plan.pause.request_body.end.clone(),
                            })
                            .unwrap_or_else(|| {
                                if !self.out.plan.pause.request_body.end.is_empty() {
                                    panic!("pause.request_body.end not valid for this request")
                                }
                                PauseSpec {
                                    group_offset: 0,
                                    plan: Vec::new(),
                                }
                            }),
                    */],
                ),
            }
        } else {
            WriteState::Completed { stream: None }
        };
        request_out.headers_duration = Some(
            TimeDelta::from_std(Instant::now().duration_since(start))
                .expect("durations should be positive")
                .into(),
        );
        self.read_state = ReadState::Headers {
            response: tokio::task::spawn(response).fuse(),
        };

        self.out.request = Some(request_out);

        Ok(())
    }

    pub async fn execute(&mut self) {
        if !self.out.plan.body.is_empty() {
            let body = std::mem::take(&mut self.out.plan.body);
            if let Err(e) = self.write_all(body.as_slice()).await {
                self.out.errors.push(Http2Error {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                return;
            }
            self.out.plan.body = body;
        }
        if let Err(e) = self.flush().await {
            self.out.errors.push(Http2Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        let mut response = Vec::new();
        if let Err(e) = self.read_to_end(&mut response).await {
            self.out.errors.push(Http2Error {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
    }

    pub async fn finish(mut self) -> (Http2Output, Option<RawHttp2Runner>) {
        let end_time = self.shutdown_end.unwrap_or_else(Instant::now);

        let mut read_state = mem::replace(&mut self.read_state, ReadState::Invalid);
        let ReadState::Body { ref mut body, .. } = read_state else {
            panic!("invalid read state for finish: {:?}", self.read_state);
        };
        // Read and write the trailers in parallel.
        let (read_trailers, _) = join!(body.inner_mut().inner_mut().trailers(), async {
            if !self.out.plan.trailers.is_empty() {
                // Send the trailers.
                if let WriteState::Body { stream, .. } = &mut self.write_state {
                    let trailers = self
                        .out
                        .plan
                        .trailers
                        .clone()
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                http::HeaderName::from_bytes(&k)
                                    .expect("out-of-spec http2 headers names are supported yet"),
                                http::HeaderValue::from_bytes(v.as_bytes()).expect(
                                    "out-of-spec http2 header values are not yet supported",
                                ),
                            )
                        })
                        .collect();

                    self.trailer_send_start = Some(Instant::now());
                    match stream.inner_mut().inner_mut().send_trailers(trailers) {
                        Ok(()) => {
                            let result =
                                future::poll_fn(|cx| stream.inner_mut().inner_mut().poll_reset(cx))
                                    .await;
                            self.trailer_send_end = Some(Instant::now());
                            match result {
                                Ok(reason) => {}
                                Err(e) => self.set_error("sending trailers", e),
                            }
                        }
                        Err(e) => self.set_error("start sending trailers", e),
                    }
                }
            } else {
                // No trailers, so manually close the write stream.
                if let Err(e) = self.shutdown().await {
                    self.set_error("shutdown write stream", e);
                }
            }
        });

        match read_trailers {
            Ok(trailers) => self.receive_trailers = trailers,
            Err(e) => self.set_error("read trailers", e),
        }

        self.read_state = read_state;

        let (resp_head, resp_body) = match mem::replace(&mut self.read_state, ReadState::Invalid) {
            ReadState::Body { body, head, .. } => {
                let (body, pauses) = body.finish();
                let mut pauses = pauses.into_iter();
                //self.out.plan.pause.response_body.start = pauses.next().unwrap_or_default();
                //self.out.plan.pause.response_body.end = pauses.next().unwrap_or_default();
                let (_, body) = body.into_parts();
                (head, Some(body))
            }
            ReadState::Headers { response, .. } => match response.now_or_never() {
                Some(Ok(Ok(resp))) => (resp.into_parts().0, None),
                Some(Err(e)) => {
                    self.set_error("response processing error", e);
                    return (self.out, self.transport);
                }
                Some(Ok(Err(e))) => {
                    self.set_error("response error", e);
                    return (self.out, self.transport);
                }
                None => return (self.out, self.transport),
            },
            ReadState::Invalid => panic!("invalid state for http2 finish"),
            _ => return (self.out, self.transport),
        };
        self.out.response = Some(crate::Http2Response {
            status_code: Some(resp_head.status.into()),
            content_length: None,
            headers: Some(
                resp_head
                    .headers
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.map(|k| MaybeUtf8(k.to_string().into())),
                            MaybeUtf8(Bytes::copy_from_slice(v.as_bytes()).into()),
                        )
                    })
                    .collect(),
            ),
            body: resp_body.map(|b| MaybeUtf8(b.freeze().into())),
            trailers: mem::take(&mut self.receive_trailers).map(|trailers| {
                trailers
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.map(|k| MaybeUtf8(k.to_string().into())),
                            MaybeUtf8(Bytes::copy_from_slice(v.as_bytes()).into()),
                        )
                    })
                    .collect()
            }),
            duration: TimeDelta::from_std(
                end_time
                    - self
                        .start_time
                        .expect("start time should be set in current state"),
            )
            .expect("duration should fit in std::time::Duration")
            .into(),
            header_duration: self
                .header_receive_end
                .zip(self.header_receive_start)
                .map(|(header_end, header_start)| TimeDelta::from_std(header_end - header_start))
                .transpose()
                .expect("duration should fit in std duration")
                .map(Duration),
            time_to_first_byte: self
                .start_time
                .zip(self.first_read)
                .map(|(start, first_read)| first_read - start)
                .map(TimeDelta::from_std)
                .transpose()
                .expect("duration should fit into std duration")
                .map(Duration),
        });

        self.read_state = ReadState::Completed;
        self.write_state = match mem::replace(&mut self.write_state, WriteState::Invalid) {
            WriteState::Body { stream, .. } => {
                let (stream, pauses) = stream.finish();
                let mut pauses = pauses.into_iter();
                //self.out.pause.request_body.start = pauses.next().unwrap_or_default();
                //self.out.pause.request_body.end = pauses.next().unwrap_or_default();
                WriteState::Completed {
                    stream: Some(stream),
                }
            }
            WriteState::Completed { stream } => WriteState::Completed { stream },
            WriteState::Pending { .. } | WriteState::Ready { .. } => {
                WriteState::Completed { stream: None }
            }
            write_state => panic!("invalid http2 write state for completion {:?}", write_state),
        };
        (self.out, self.transport)
    }

    fn set_error<S, E>(&mut self, kind: S, e: E)
    where
        S: Into<String>,
        E: std::error::Error,
    {
        self.out.errors.push(crate::Http2Error {
            kind: kind.into(),
            message: e.to_string(),
        })
    }
}

impl AsyncRead for Http2Runner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut state = mem::replace(&mut self.read_state, ReadState::Invalid);
        let resp = (|| {
            // Read headers before reporting any body bytes back to the caller.
            if let ReadState::Headers { ref mut response } = state {
                let (head, body) = match ready!(response.poll_unpin(cx)) {
                    Ok(Ok(response)) => response.into_parts(),
                    Ok(Err(e)) => return Poll::Ready(Err(std::io::Error::other(e))),
                    Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
                };

                state = ReadState::Body {
                    head,
                    body: PauseReader::new(
                        self.ctx.clone(),
                        RecvTransport::new(body),
                        [/*
                            PauseSpec {
                                group_offset: 0,
                                plan: self.out.plan.pause.response_body.start.clone(),
                            },
                            self.size_hint
                                .map(|size| PauseSpec {
                                    group_offset: size.try_into().expect(
                                        "response body size exceeding addressable platform size not yet supported",
                                    ),
                                    plan: self.out.plan.pause.response_body.end.clone(),
                                })
                                .unwrap_or_else(|| {
                                    if !self.out.plan.pause.response_body.end.is_empty() {
                                        panic!("pause.response_body.end not valid for this request")
                                    }
                                    PauseSpec {
                                        group_offset: 0,
                                        plan: Vec::new(),
                                    }
                                }),
                        */],
                    ),
                };
            }

            // Read body bytes
            let ReadState::Body { ref mut body, .. } = state else {
                panic!("invalid for state http2 poll_read {state:?}");
            };
            pin!(body).poll_read(cx, buf)
        })();
        if matches!(self.read_state, ReadState::Invalid) {
            self.read_state = state;
        }
        resp
    }
}

impl AsyncWrite for Http2Runner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let WriteState::Body { stream } = &mut self.write_state else {
            panic!("http2 write in invalid state {:?}", self.write_state)
        };

        pin!(stream).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        // TODO: implement flush with custom http2 implementation
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        if self.shutdown_start.is_none() {
            self.shutdown_start = Some(Instant::now());
        }

        let stream = match &mut self.write_state {
            WriteState::Body { stream, .. } => stream.inner_mut(),
            WriteState::Completed {
                stream: Some(stream),
            } => stream,
            WriteState::Completed { stream: None } | WriteState::StartFailed => {
                return Poll::Ready(Ok(()))
            }
            _ => panic!(
                "invalid state for http2 write shutdown {:?}",
                self.write_state
            ),
        };

        let result = ready!(pin!(stream).poll_shutdown(cx));

        if self.shutdown_end.is_none() {
            self.shutdown_end = Some(Instant::now());
        }

        Poll::Ready(result)
    }
}

#[derive(Debug)]
struct SendTransport {
    inner: SendStream<Bytes>,
    state: SendTransportState,
    remaining: Option<usize>,
    reason: Option<Reason>,
    data: Vec<u8>,
}

#[derive(Debug)]
enum SendTransportState {
    Open,
    Closing,
    Closed,
}

impl SendTransport {
    fn new(inner: SendStream<Bytes>, size_hint: Option<usize>) -> Self {
        Self {
            inner,
            state: SendTransportState::Open,
            reason: None,
            remaining: size_hint,
            data: size_hint.map(Vec::with_capacity).unwrap_or_default(),
        }
    }

    fn inner_mut(&mut self) -> &mut SendStream<Bytes> {
        &mut self.inner
    }
}

impl AsyncWrite for SendTransport {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // Reserve capacity if needed.
        let mut capacity = self.inner.capacity();
        if capacity == 0 {
            self.inner.reserve_capacity(buf.len());
            capacity = ready!(self.inner.poll_capacity(cx))
                .expect("stream should not close before we finish writing the request body")
                .map_err(|e| std::io::Error::other(e))?;
        }

        // Send as much data as we have capacity for.
        let eos = self
            .remaining
            .map(|remaining_to_send| capacity >= remaining_to_send)
            .unwrap_or_default();
        self.inner
            .send_data(Bytes::copy_from_slice(&buf[..capacity]), eos)
            .map_err(|e| std::io::Error::other(e))?;

        // Track sent bytes.
        if let Some(remaining) = &mut self.remaining {
            *remaining -= capacity;
        }

        Poll::Ready(Ok(capacity))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.state {
            SendTransportState::Open => {
                self.inner.send_reset(h2::Reason::NO_ERROR);
                self.state = SendTransportState::Closing;
            }
            SendTransportState::Closing => {}
            SendTransportState::Closed => return Poll::Ready(Ok(())),
        }
        match ready!(self.inner.poll_reset(cx)) {
            Ok(reason) => {
                self.reason = Some(reason);
                self.state = SendTransportState::Closed;
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }
}

#[derive(Debug)]
struct RecvTransport {
    inner: RecvStream,
    pending_index: usize,
    data: BytesMut,
}

impl RecvTransport {
    fn new(inner: RecvStream) -> Self {
        Self {
            inner,
            pending_index: 0,
            data: BytesMut::new(),
        }
    }

    fn inner_mut(&mut self) -> &mut RecvStream {
        &mut self.inner
    }

    fn into_parts(self) -> (RecvStream, BytesMut) {
        (self.inner, self.data)
    }
}

impl AsyncRead for RecvTransport {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Copy any buffered bytes from a previous read.
        let start = self.pending_index;
        let pending_len = self.data.len() - self.pending_index;
        self.pending_index += buf.remaining().min(pending_len);
        buf.put_slice(&self.data[start..self.pending_index]);
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // We've got room still, so read another frame.
        match ready!(self.inner.poll_data(cx)) {
            Some(Ok(data)) => {
                self.pending_index = self.data.len();
                self.data.extend_from_slice(&data);
                buf.put_slice(&data[..buf.remaining().min(data.len())]);
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(std::io::Error::other(e))),
            None => Poll::Ready(Ok(())),
        }
    }
}
