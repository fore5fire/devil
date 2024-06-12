use std::{pin::pin, sync::Arc};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};

use super::http2frames::Http2FramesRunner;
use super::{http2::Http2Runner, tcpsegments::TcpSegmentsRunner};
use crate::{Output, ProtocolField, StepPlanOutput};

use super::{
    graphql::GraphQlRunner, http::HttpRunner, http1::Http1Runner, tcp::TcpRunner, tls::TlsRunner,
};

#[derive(Debug)]
pub(super) enum Runner {
    GraphQl(Box<GraphQlRunner>),
    Http(Box<HttpRunner>),
    H1c(Box<Http1Runner>),
    H1(Box<Http1Runner>),
    H2c(Box<Http2Runner>),
    H2(Box<Http2Runner>),
    Http2Frames(Box<Http2FramesRunner>),
    Tls(Box<TlsRunner>),
    Tcp(Box<TcpRunner>),
    TcpSegments(Box<TcpSegmentsRunner>),
    MuxHttp2Frames(h2::client::SendRequest<bytes::Bytes>),
    //PipelinedHttp(PipelineRunner<HttpRunner>),
    //PipelinedH1c(PipelineRunner<Http1Runner>),
    //PipelinedH1(PipelineRunner<Http1Runner>),
    //PipelinedH2c(PipelineRunner<Http2Runner>),
    //PipelinedH2(PipelineRunner<Http2Runner>),
}

impl Runner {
    pub(super) fn new(ctx: Arc<super::Context>, step: StepPlanOutput) -> crate::Result<Self> {
        Ok(match step {
            StepPlanOutput::TcpSegments(output) => {
                Self::TcpSegments(Box::new(TcpSegmentsRunner::new(ctx, output)))
            }
            StepPlanOutput::Tcp(output) => Self::Tcp(Box::new(TcpRunner::new(ctx, output))),
            StepPlanOutput::Tls(output) => Self::Tls(Box::new(TlsRunner::new(ctx, output))),
            StepPlanOutput::Http(output) => Self::Http(Box::new(HttpRunner::new(ctx, output)?)),
            StepPlanOutput::H1c(output) => Runner::H1c(Box::new(Http1Runner::new(ctx, output))),
            StepPlanOutput::H1(output) => Runner::H1(Box::new(Http1Runner::new(ctx, output))),
            StepPlanOutput::H2c(output) => Self::H2c(Box::new(Http2Runner::new(ctx, output)?)),
            StepPlanOutput::H2(output) => Self::H2(Box::new(Http2Runner::new(ctx, output)?)),
            StepPlanOutput::Http2Frames(output) => {
                Self::Http2Frames(Box::new(Http2FramesRunner::new(ctx, output)))
            }
            StepPlanOutput::GraphQl(output) => {
                Self::GraphQl(Box::new(GraphQlRunner::new(ctx, output)?))
            }
        })
    }

    pub(super) fn field(&self) -> ProtocolField {
        match self {
            Self::TcpSegments(_) => ProtocolField::TcpSegments,
            Self::Tcp(_) => ProtocolField::Tcp,
            Self::Tls(_) => ProtocolField::Tls,
            Self::H1c(_) => ProtocolField::H1c,
            Self::H1(_) => ProtocolField::H1,
            Self::H2c(_) => ProtocolField::H2c,
            Self::H2(_) => ProtocolField::Http2Frames,
            Self::Http2Frames(_) => ProtocolField::Http2Frames,
            Self::MuxHttp2Frames(_) => ProtocolField::Http2Frames,
            Self::Http(_) => ProtocolField::Http,
            Self::GraphQl(_) => ProtocolField::GraphQl,
        }
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        match self {
            Self::TcpSegments(r) => None,
            Self::Tcp(r) => r.size_hint(hint),
            Self::Tls(r) => r.size_hint(hint),
            Self::H1c(r) | Self::H1(r) => r.size_hint(hint),
            Self::H2c(r) | Self::H2(r) => r.size_hint(hint),
            Self::Http2Frames(r) => r.size_hint(hint),
            Self::MuxHttp2Frames(_) => None,
            Self::Http(r) => r.size_hint(hint),
            Self::GraphQl(r) => r.size_hint(hint),
        }
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        match self {
            Self::TcpSegments(r) => None,
            Self::Tcp(r) => r.executor_size_hint(),
            Self::Tls(r) => r.executor_size_hint(),
            Self::H1c(r) | Self::H1(r) => r.executor_size_hint(),
            Self::H2c(r) | Self::H2(r) => r.executor_size_hint(),
            Self::Http(r) => r.executor_size_hint(),
            Self::GraphQl(r) => r.executor_size_hint(),
            Self::Http2Frames(_) => unimplemented!(),
            Self::MuxHttp2Frames(_) => unimplemented!(),
        }
    }

    pub fn start(
        &mut self,
        transport: Option<Runner>,
        concurrent_shares: usize,
    ) -> BoxFuture<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        match self {
            Self::TcpSegments(r) => {
                assert!(transport.is_none());
                Box::pin(r.start())
            }
            Self::Tcp(r) => Box::pin(match transport {
                Some(Runner::TcpSegments(transport)) => Box::pin(r.start(*transport)),
                Some(_) => panic!("tcp requires tcp_segments transport"),
                None => panic!("no plan should have tcp as a base protocol"),
            }),
            Self::Tls(r) => {
                Box::pin(r.start(transport.expect("no plan should have tls as a base protocol")))
            }
            Self::H1c(r) | Self::H1(r) => {
                Box::pin(r.start(transport.expect("no plan should have http1 as a base protocol")))
            }
            Self::H2c(r) | Self::H2(r) => match transport {
                Some(Runner::Http2Frames(transport)) => Box::pin(r.start(*transport)),
                Some(Runner::MuxHttp2Frames(transport)) => Box::pin(r.start_shared(transport)),
                Some(_) => panic!("http2 requires http2_frames transport"),
                None => panic!("no stack should use http2 as a base protocol"),
            },
            Self::Http2Frames(r) => Box::pin(r.start(
                transport.expect("no plan should have http2_frames as a base protocol"),
                concurrent_shares,
            )),
            Self::MuxHttp2Frames(_) => Box::pin(async { Ok(()) }),
            Self::Http(r) => {
                assert!(transport.is_none());
                Box::pin(r.start())
            }
            Self::GraphQl(r) => Box::pin(
                r.start(transport.expect("no plan should have graphql as a base protocol")),
            ),
        }
    }

    pub async fn execute(&mut self) {
        match self {
            Self::TcpSegments(r) => r.execute().await,
            Self::Tcp(r) => r.execute().await,
            Self::Tls(r) => r.execute().await,
            Self::H1c(r) | Self::H1(r) => r.execute().await,
            Self::H2c(r) | Self::H2(r) => r.execute().await,
            Self::Http2Frames(_) => unimplemented!(),
            Self::MuxHttp2Frames(_) => unimplemented!(),
            Self::Http(r) => r.execute().await,
            Self::GraphQl(r) => r.execute().await,
        }
    }

    pub async fn finish(self: Self) -> (Output, Option<Runner>) {
        match self {
            Self::TcpSegments(r) => {
                let out = r.finish();
                (Output::TcpSegments(out), None)
            }
            Self::Tcp(r) => {
                let (out, inner) = r.finish();
                (Output::Tcp(out), Some(Runner::TcpSegments(Box::new(inner))))
            }
            Self::Tls(r) => {
                let (out, inner) = r.finish();
                (Output::Tls(out), inner)
            }
            Self::Http(r) => {
                let (out, inner) = r.finish();
                (Output::Http(out), inner)
            }
            Self::H1c(r) => {
                let (out, inner) = r.finish();
                (Output::H1c(out), inner)
            }
            Self::H1(r) => {
                let (out, inner) = r.finish();
                (Output::H1(out), inner)
            }
            Self::H2c(r) => {
                let (out, inner) = r.finish().await;
                (
                    Output::H2c(out),
                    inner.map(|inner| Runner::Http2Frames(Box::new(inner))),
                )
            }
            Self::H2(r) => {
                let (out, inner) = r.finish().await;
                (
                    Output::H2(out),
                    inner.map(|inner| Runner::Http2Frames(Box::new(inner))),
                )
            }
            Self::Http2Frames(r) => {
                let (out, inner) = r.finish().await;
                (Output::Http2Frames(out), inner)
            }
            Self::GraphQl(r) => {
                let (out, inner) = r.finish();
                (Output::GraphQl(out), inner)
            }
            Self::MuxHttp2Frames(_) => panic!(),
        }
    }
}

impl AsyncRead for Runner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::TcpSegments(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Tcp(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Http2Frames(_) | Self::MuxHttp2Frames(_) => {
                panic!("http2_frames doesn't support stream reading")
            }
            Self::Http(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::GraphQl(_) => panic!("graphql cannot be used as a transport"),
        }
    }
}

impl AsyncWrite for Runner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::TcpSegments(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Tcp(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Http2Frames(_) | Self::MuxHttp2Frames(_) => {
                panic!("http2_frames doesn't support stream writing")
            }
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Http(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::GraphQl(_) => panic!("graphql cannot be used as a transport"),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::TcpSegments(ref mut r) => pin!(r).poll_flush(cx),
            Self::Tcp(ref mut r) => pin!(r).poll_flush(cx),
            Self::Tls(ref mut r) => pin!(r).poll_flush(cx),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_flush(cx),
            Self::Http2Frames(_) | Self::MuxHttp2Frames(_) => {
                panic!("http2_frames doesn't support stream writing")
            }
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_flush(cx),
            Self::Http(ref mut r) => pin!(r).poll_flush(cx),
            Self::GraphQl(_) => panic!("graphql cannot be used as a transport"),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::TcpSegments(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Tcp(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Tls(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Http2Frames(_) | Self::MuxHttp2Frames(_) => {
                panic!("http2_frames doesn't support stream writing")
            }
            Self::Http(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::GraphQl(_) => panic!("graphql cannot be used as a transport"),
        }
    }
}

#[derive(Debug)]
pub struct SharedRunner<T> {
    inner: T,
    reads: tokio::sync::broadcast::Receiver<u8>,
    writes: tokio::sync::broadcast::Sender<u8>,
}
