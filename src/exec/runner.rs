use std::{pin::pin, sync::Arc};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use super::raw_http2::RawHttp2Runner;
use super::{http2::Http2Runner, raw_tcp::RawTcpRunner};
use crate::{JobOutput, ProtocolField, StepPlanOutput};

use super::{
    graphql::GraphqlRunner, http::HttpRunner, http1::Http1Runner, tcp::TcpRunner, tls::TlsRunner,
};

#[derive(Debug)]
pub(super) enum Runner {
    Graphql(Box<GraphqlRunner>),
    Http(Box<HttpRunner>),
    H1c(Box<Http1Runner>),
    H1(Box<Http1Runner>),
    H2c(Box<Http2Runner>),
    RawH2c(Box<RawHttp2Runner>),
    H2(Box<Http2Runner>),
    RawH2(Box<RawHttp2Runner>),
    Tls(Box<TlsRunner>),
    Tcp(Box<TcpRunner>),
    RawTcp(Box<RawTcpRunner>),
    MuxRawH2(h2::client::SendRequest<bytes::Bytes>),
    MuxRawH2c(h2::client::SendRequest<bytes::Bytes>),
    //PipelinedHttp(PipelineRunner<HttpRunner>),
    //PipelinedH1c(PipelineRunner<Http1Runner>),
    //PipelinedH1(PipelineRunner<Http1Runner>),
    //PipelinedH2c(PipelineRunner<Http2Runner>),
    //PipelinedH2(PipelineRunner<Http2Runner>),
}

impl Runner {
    pub(super) fn new(
        ctx: Arc<super::Context>,
        step: StepPlanOutput,
        executor: bool,
    ) -> crate::Result<Self> {
        Ok(match step {
            StepPlanOutput::RawTcp(output) => {
                Self::RawTcp(Box::new(RawTcpRunner::new(ctx, output)))
            }
            StepPlanOutput::Tcp(output) => Self::Tcp(Box::new(TcpRunner::new(ctx, output))),
            StepPlanOutput::Tls(output) => Self::Tls(Box::new(TlsRunner::new(ctx, output))),
            StepPlanOutput::Http(output) => Self::Http(Box::new(HttpRunner::new(ctx, output)?)),
            StepPlanOutput::H1c(output) => Runner::H1c(Box::new(Http1Runner::new(ctx, output))),
            StepPlanOutput::H1(output) => Runner::H1(Box::new(Http1Runner::new(ctx, output))),
            StepPlanOutput::H2c(output) => Self::H2c(Box::new(Http2Runner::new(ctx, output)?)),
            StepPlanOutput::RawH2c(output) => {
                Self::RawH2c(Box::new(RawHttp2Runner::new(ctx, output, executor)))
            }
            StepPlanOutput::H2(output) => Self::H2(Box::new(Http2Runner::new(ctx, output)?)),
            StepPlanOutput::RawH2(output) => {
                Self::RawH2(Box::new(RawHttp2Runner::new(ctx, output, executor)))
            }
            StepPlanOutput::Graphql(output) => {
                Self::Graphql(Box::new(GraphqlRunner::new(ctx, output)?))
            }
        })
    }

    pub(super) fn field(&self) -> ProtocolField {
        match self {
            Self::RawTcp(_) => ProtocolField::RawTcp,
            Self::Tcp(_) => ProtocolField::Tcp,
            Self::Tls(_) => ProtocolField::Tls,
            Self::H1c(_) => ProtocolField::H1c,
            Self::H1(_) => ProtocolField::H1,
            Self::H2c(_) => ProtocolField::H2c,
            Self::RawH2c(_) => ProtocolField::RawH2c,
            Self::MuxRawH2c(_) => ProtocolField::RawH2c,
            Self::H2(_) => ProtocolField::H2,
            Self::RawH2(_) => ProtocolField::RawH2,
            Self::MuxRawH2(_) => ProtocolField::RawH2,
            Self::Http(_) => ProtocolField::Http,
            Self::Graphql(_) => ProtocolField::Graphql,
        }
    }

    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        match self {
            Self::RawTcp(_) => None,
            Self::Tcp(r) => r.size_hint(hint),
            Self::Tls(r) => r.size_hint(hint),
            Self::H1c(r) | Self::H1(r) => r.size_hint(hint),
            Self::H2c(r) | Self::H2(r) => r.size_hint(hint),
            Self::RawH2c(r) | Self::RawH2(r) => r.size_hint(hint),
            Self::MuxRawH2(_) | Self::MuxRawH2c(_) => None,
            Self::Http(r) => r.size_hint(hint),
            Self::Graphql(r) => r.size_hint(hint),
        }
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        match self {
            Self::RawTcp(_) => None,
            Self::Tcp(r) => r.executor_size_hint(),
            Self::Tls(r) => r.executor_size_hint(),
            Self::H1c(r) | Self::H1(r) => r.executor_size_hint(),
            Self::H2c(r) | Self::H2(r) => r.executor_size_hint(),
            Self::Http(r) => r.executor_size_hint(),
            Self::Graphql(r) => r.executor_size_hint(),
            Self::RawH2c(_) => None,
            Self::RawH2(_) => None,
            Self::MuxRawH2c(_) => unimplemented!(),
            Self::MuxRawH2(_) => unimplemented!(),
        }
    }

    pub fn start(
        &mut self,
        transport: Option<Runner>,
        concurrent_shares: usize,
    ) -> BoxFuture<anyhow::Result<()>> {
        match self {
            Self::RawTcp(r) => {
                assert!(transport.is_none());
                Box::pin(r.start())
            }
            Self::Tcp(r) => Box::pin(match transport {
                Some(Runner::RawTcp(transport)) => Box::pin(r.start(*transport)),
                Some(_) => panic!("tcp requires raw_tcp transport"),
                None => panic!("no plan should have tcp as a base protocol"),
            }),
            Self::Tls(r) => {
                Box::pin(r.start(transport.expect("no plan should have tls as a base protocol")))
            }
            Self::H1c(r) | Self::H1(r) => {
                Box::pin(r.start(transport.expect("no plan should have http1 as a base protocol")))
            }
            Self::H2c(r) | Self::H2(r) => match transport {
                Some(Runner::RawH2(transport)) | Some(Runner::RawH2c(transport)) => {
                    info!("h2 has raw_h2 transport");
                    Box::pin(r.start(*transport))
                }
                Some(Runner::MuxRawH2c(transport)) | Some(Runner::MuxRawH2(transport)) => {
                    Box::pin(r.start_shared(transport))
                }
                Some(_) => panic!("http2 requires http2_frames transport"),
                None => panic!("no stack should use http2 as a base protocol"),
            },
            Self::RawH2(r) | Self::RawH2c(r) => Box::pin(r.start(
                transport.expect("no plan should have http2_frames as a base protocol"),
                concurrent_shares,
            )),
            Self::MuxRawH2(_) | Self::MuxRawH2c(_) => Box::pin(async { Ok(()) }),
            Self::Http(r) => {
                assert!(transport.is_none());
                Box::pin(r.start())
            }
            Self::Graphql(r) => Box::pin(
                r.start(transport.expect("no plan should have graphql as a base protocol")),
            ),
        }
    }

    pub async fn execute(&mut self) {
        match self {
            Self::RawTcp(r) => r.execute().await,
            Self::Tcp(r) => r.execute().await,
            Self::Tls(r) => r.execute().await,
            Self::H1c(r) | Self::H1(r) => r.execute().await,
            Self::H2c(r) | Self::H2(r) => r.execute().await,
            Self::RawH2c(r) | Self::RawH2(r) => r.execute().await,
            Self::MuxRawH2c(_) | Self::MuxRawH2(_) => {
                panic!("cannot multiplex and execute at the same layer")
            }
            Self::Http(r) => r.execute().await,
            Self::Graphql(r) => r.execute().await,
        }
    }

    pub async fn finish(self: Self, output: &mut JobOutput) -> Option<Runner> {
        match self {
            Self::RawTcp(r) => {
                output.raw_tcp = Some(Arc::new(r.finish().await));
                None
            }
            Self::Tcp(r) => {
                let (out, inner) = r.finish().await;
                output.tcp = Some(Arc::new(out));
                Some(Runner::RawTcp(Box::new(inner)))
            }
            Self::Tls(r) => {
                let (out, inner) = r.finish();
                output.tls = Some(Arc::new(out));
                Some(inner)
            }
            Self::Http(r) => {
                let (out, inner) = r.finish();
                output.http = Some(Arc::new(out));
                inner
            }
            Self::H1c(r) => {
                let (out, inner) = r.finish();
                output.h1c = Some(Arc::new(out));
                inner
            }
            Self::H1(r) => {
                let (out, inner) = r.finish();
                output.h1 = Some(Arc::new(out));
                inner
            }
            Self::H2c(r) => {
                let (out, inner) = r.finish().await;
                output.h2c = Some(Arc::new(out));
                inner.map(|inner| Runner::RawH2c(Box::new(inner)))
            }
            Self::RawH2c(r) => {
                let (out, inner) = r.finish().await;
                output.raw_h2c = Some(Arc::new(out));
                inner
            }
            Self::H2(r) => {
                let (out, inner) = r.finish().await;
                output.h2 = Some(Arc::new(out));
                inner.map(|inner| Runner::RawH2(Box::new(inner)))
            }
            Self::RawH2(r) => {
                let (out, inner) = r.finish().await;
                output.raw_h2 = Some(Arc::new(out));
                inner
            }
            Self::Graphql(r) => {
                let (out, inner) = r.finish();
                output.graphql = Some(Arc::new(out));
                inner
            }
            Self::MuxRawH2(_) | Self::MuxRawH2c(_) => panic!(),
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
            Self::RawTcp(_) => {
                panic!("raw_tcp doesn't support stream reading")
            }
            Self::Tcp(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::RawH2c(_) | Self::MuxRawH2c(_) => {
                panic!("raw_h2c doesn't support stream reading")
            }
            Self::RawH2(_) | Self::MuxRawH2(_) => {
                panic!("raw_h2 doesn't support stream reading")
            }
            Self::Http(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Graphql(_) => panic!("graphql cannot be used as a transport"),
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
            Self::RawTcp(_) => {
                panic!("raw_tcp doesn't support stream writing")
            }
            Self::Tcp(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::RawH2c(_) | Self::MuxRawH2c(_) => {
                panic!("raw_h2c doesn't support stream writing")
            }
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::RawH2(_) | Self::MuxRawH2(_) => {
                panic!("raw_h2 doesn't support stream writing")
            }
            Self::Http(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Graphql(_) => panic!("graphql cannot be used as a transport"),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::RawTcp(_) => {
                panic!("raw_tcp doesn't support stream writing")
            }
            Self::Tcp(ref mut r) => pin!(r).poll_flush(cx),
            Self::Tls(ref mut r) => pin!(r).poll_flush(cx),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_flush(cx),
            Self::RawH2c(_) | Self::MuxRawH2c(_) => {
                panic!("h2c doesn't support stream writing")
            }
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_flush(cx),
            Self::RawH2(_) | Self::MuxRawH2(_) => {
                panic!("h2 doesn't support stream writing")
            }
            Self::Http(ref mut r) => pin!(r).poll_flush(cx),
            Self::Graphql(_) => panic!("graphql cannot be used as a transport"),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::RawTcp(_) => {
                panic!("raw_tcp doesn't support stream writing")
            }
            Self::Tcp(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Tls(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::H1c(ref mut r) | Self::H1(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::H2c(ref mut r) | Self::H2(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::RawH2c(_) | Self::MuxRawH2c(_) => {
                panic!("raw_h2c doesn't support stream writing")
            }
            Self::RawH2(_) | Self::MuxRawH2(_) => {
                panic!("raw_h2 doesn't support stream writing")
            }
            Self::Http(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Graphql(_) => panic!("graphql cannot be used as a transport"),
        }
    }
}

#[derive(Debug)]
pub struct SharedRunner<T> {
    inner: T,
    reads: tokio::sync::broadcast::Receiver<u8>,
    writes: tokio::sync::broadcast::Sender<u8>,
}
