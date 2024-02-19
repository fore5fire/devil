use std::{
    future, mem,
    pin::pin,
    sync::Arc,
    task::{ready, Poll},
    time::Instant,
};

use bytes::Bytes;
use chrono::Duration;
use futures::FutureExt;
use h2::{client::SendRequest, Reason, RecvStream, SendStream};
use http::{response::Parts, HeaderName, Request, Uri};
use nom::AsBytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    join,
    task::JoinHandle,
};

use crate::{Http2Error, Http2Output, Http2PauseOutput, Http2PlanOutput, Http2RequestOutput};

use super::{
    http2frames::Http2FramesRunner,
    pause::{PauseReader, PauseSpec, PauseWriter},
    Context,
};

#[derive(Debug)]
pub struct Http2Runner {
    ctx: Arc<Context>,
    out: Http2Output,
    read_state: ReadState,
    write_state: WriteState,
    transport: Option<Http2FramesRunner>,
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
    size_hint: Option<usize>,
    close_reason: Option<h2::Reason>,
    trailers: Option<http::HeaderMap>,
}

#[derive(Debug)]
enum WriteState {
    Pending {
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
                request: Self::build_request(&plan).map_err(|e| crate::Error(e.to_string()))?,
                request_out: Http2RequestOutput {
                    url: plan.url.clone(),
                    method: plan.method.clone(),
                    content_length: plan.content_length.clone(),
                    headers: plan.headers.clone(),
                    body: plan.body.clone(),
                    duration: chrono::Duration::zero(),
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
                duration: Duration::zero(),
                pause: Http2PauseOutput::default(),
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
            size_hint: None,
            close_reason: None,
            trailers: None,
        })
    }

    fn build_request(plan: &Http2PlanOutput) -> Result<http::Request<()>, http::Error> {
        let uri: Uri = plan.url.as_str().parse().unwrap();
        let mut req = Request::builder().uri(uri).method(
            String::from_utf8(
                plan.clone()
                    .method
                    .expect("missing method is not yet supported for http2"),
            )
            .expect("non-utf8 method is not yet supported with http2")
            .as_str(),
        );
        for (k, v) in plan.headers.clone() {
            req = req.header(k, v);
        }
        req.body(())
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        self.size_hint = hint;
        None
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.out.plan.body.len())
    }

    pub async fn start(
        &mut self,
        mut transport: Http2FramesRunner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stream = transport.new_stream();
        self.transport = Some(transport);
        self.start_shared(stream.expect("a single stream should be available for non-shared http2"))
            .await
    }

    pub async fn start_shared(
        &mut self,
        transport: SendRequest<Bytes>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = mem::replace(&mut self.write_state, WriteState::Invalid);
        let WriteState::Pending {
            request,
            mut request_out,
        } = state
        else {
            self.write_state = state;
            return Err(Box::new(crate::Error(format!(
                "attempt to start Http2Runner from invalid state {:?}",
                self.write_state
            ))));
        };

        let need_send_stream =
            self.size_hint.is_some_and(|size| size > 0) || !self.out.plan.trailers.is_empty();

        let start = Instant::now();
        self.start_time = Some(start);

        let mut conn = transport.ready().await?;
        let (response, send_stream) = conn.send_request(request, !need_send_stream)?;

        // Setup the response to write to the stream once its ready.

        self.write_state = if need_send_stream {
            WriteState::Body {
                stream: PauseWriter::new(
                    self.ctx.clone(),
                    SendTransport::new(send_stream, self.size_hint),
                    [
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
                    ],
                ),
            }
        } else {
            WriteState::Completed { stream: None }
        };
        request_out.headers_duration = Some(
            chrono::Duration::from_std(Instant::now().duration_since(start))
                .expect("durations should be positive"),
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

    pub async fn finish(mut self) -> (Http2Output, Option<Http2FramesRunner>) {
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
            Ok(trailers) => self.trailers = trailers,
            Err(e) => self.set_error("read trailers", e),
        }

        self.read_state = read_state;
        self.complete();
        (self.out, self.transport)
    }

    fn complete(&mut self) {
        let end = Instant::now();
        let (resp_head, resp_body) = match mem::replace(&mut self.read_state, ReadState::Invalid) {
            ReadState::Body { body, head, .. } => {
                let (body, pauses) = body.finish();
                let mut pauses = pauses.into_iter();
                self.out.plan.pause.response_body.start = pauses.next().unwrap_or_default();
                self.out.plan.pause.response_body.end = pauses.next().unwrap_or_default();
                let (_, body) = body.into_parts();
                (head, Some(body))
            }
            ReadState::Headers { response, .. } => match response.now_or_never() {
                Some(Ok(Ok(resp))) => (resp.into_parts().0, None),
                Some(Err(e)) => return self.set_error("response processing error", e),
                Some(Ok(Err(e))) => return self.set_error("response error", e),
                None => return,
            },
            ReadState::Invalid => panic!("invalid state for http2 finish"),
            _ => return,
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
                            k.as_ref()
                                .map(HeaderName::as_str)
                                .unwrap_or_default()
                                .into(),
                            v.as_bytes().to_owned(),
                        )
                    })
                    .collect(),
            ),
            body: resp_body,
            trailers: mem::take(&mut self.trailers).map(|trailers| {
                trailers
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.map(|k| k.as_str().as_bytes().to_vec())
                                .unwrap_or_default(),
                            v.as_bytes().to_vec(),
                        )
                    })
                    .collect()
            }),
            duration: chrono::Duration::from_std(
                end - self
                    .start_time
                    .expect("start time should be set in current state"),
            )
            .expect("duration should fit in std::time::Duration"),
            header_duration: self
                .header_receive_end
                .zip(self.header_receive_start)
                .map(|(header_end, header_start)| {
                    chrono::Duration::from_std(header_end - header_start)
                })
                .transpose()
                .expect("duration should fit in std duration"),
            time_to_first_byte: self
                .start_time
                .zip(self.first_read)
                .map(|(start, first_read)| first_read - start)
                .map(chrono::Duration::from_std)
                .transpose()
                .expect("duration should fit into std duration"),
        });

        self.read_state = ReadState::Completed;
        self.write_state = match mem::replace(&mut self.write_state, WriteState::Invalid) {
            WriteState::Body { stream, .. } => {
                let (stream, pauses) = stream.finish();
                let mut pauses = pauses.into_iter();
                self.out.pause.request_body.start = pauses.next().unwrap_or_default();
                self.out.pause.request_body.end = pauses.next().unwrap_or_default();
                WriteState::Completed {
                    stream: Some(stream),
                }
            }
            WriteState::Completed { stream } => WriteState::Completed { stream },
            WriteState::Pending { .. } => WriteState::Completed { stream: None },
            write_state => panic!("invalid http2 write state for {:?}", write_state),
        }
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
                        [
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
                        ],
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
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        // TODO: implement flush with custom http2 implementation
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
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

        pin!(stream).poll_shutdown(cx)
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
    buffer: Bytes,
    data: Vec<u8>,
}

impl RecvTransport {
    fn new(inner: RecvStream) -> Self {
        Self {
            inner,
            buffer: Bytes::new(),
            data: Vec::new(),
        }
    }

    fn inner_mut(&mut self) -> &mut RecvStream {
        &mut self.inner
    }

    fn into_parts(self) -> (RecvStream, Vec<u8>) {
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
        let len = self.buffer.len();
        buf.put_slice(&self.buffer.split_to(buf.remaining().min(len)));
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // We've got room still, so read another frame.
        match ready!(self.inner.poll_data(cx)) {
            Some(Ok(mut data)) => {
                self.data.extend_from_slice(&data);
                self.buffer = data.split_off(buf.remaining().min(data.len()));
                buf.put_slice(data.as_bytes());
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(std::io::Error::other(e))),
            None => Poll::Ready(Ok(())),
        }
    }
}
