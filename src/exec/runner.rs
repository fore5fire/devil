use std::{pin::pin, sync::Arc};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Output, StepPlanOutput};

use super::{
    graphql::GraphQlRunner, http::HttpRunner, http1::Http1Runner, tcp::TcpRunner, tls::TlsRunner,
};

#[derive(Debug)]
pub(super) enum Runner {
    GraphQl(Box<GraphQlRunner>),
    Http(Box<HttpRunner>),
    Http1(Box<Http1Runner>),
    Tls(Box<TlsRunner>),
    Tcp(Box<TcpRunner>),
}

impl Runner {
    pub(super) fn new(
        ctx: Arc<super::Context>,
        transport: Option<Runner>,
        step: StepPlanOutput,
    ) -> crate::Result<Self> {
        Ok(match step {
            StepPlanOutput::Tcp(output) => {
                assert!(transport.is_none());
                Runner::Tcp(Box::new(TcpRunner::new(ctx, output)))
            }
            StepPlanOutput::Http(output) => {
                assert!(transport.is_none());
                Runner::Http(Box::new(HttpRunner::new(ctx, output)?))
            }
            StepPlanOutput::Tls(output) => Runner::Tls(Box::new(TlsRunner::new(
                ctx,
                transport.expect("no plan should have tls as a base protocol"),
                output,
            ))),

            StepPlanOutput::Http1(output) => Runner::Http1(Box::new(Http1Runner::new(
                ctx,
                transport.expect("no plan should have http1 as a base protocol"),
                output,
            ))),
            StepPlanOutput::GraphQl(output) => Runner::GraphQl(Box::new(GraphQlRunner::new(
                ctx,
                transport.expect("no plan should have graphql as a base protocol"),
                output,
            )?)),
        })
    }
    pub fn start(
        &mut self,

        size_hint: Option<usize>,
    ) -> BoxFuture<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        match self {
            Self::Tcp(r) => Box::pin(r.start(size_hint)),
            Self::Tls(r) => Box::pin(r.start(size_hint)),
            Self::Http1(r) => Box::pin(r.start(size_hint)),
            Self::Http(r) => Box::pin(r.start(size_hint)),
            Self::GraphQl(r) => Box::pin(r.start(size_hint)),
        }
    }

    pub async fn execute(&mut self) {
        match self {
            Self::Tcp(r) => r.execute().await,
            Self::Tls(r) => r.execute().await,
            Self::Http1(r) => r.execute().await,
            Self::Http(r) => r.execute().await,
            Self::GraphQl(r) => r.execute().await,
        }
    }

    pub async fn finish(self: Self) -> (Output, Option<Runner>) {
        match self {
            Self::Tcp(r) => (r.finish(), None),
            Self::Tls(r) => {
                let (out, inner) = r.finish();
                (out, Some(inner))
            }
            Self::Http1(r) => {
                let (out, inner) = r.finish();
                (out, Some(inner))
            }
            Self::Http(r) => {
                let (out, inner) = r.finish();
                (out, Some(inner))
            }
            Self::GraphQl(r) => {
                let (out, inner) = r.finish();
                (out, Some(inner))
            }
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
            Self::Tcp(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Http1(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::Http(ref mut r) => pin!(r).poll_read(cx, buf),
            Self::GraphQl(ref mut r) => panic!(),
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
            Self::Tcp(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Tls(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Http1(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::Http(ref mut r) => pin!(r).poll_write(cx, buf),
            Self::GraphQl(ref mut r) => panic!(),
        }
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::Tcp(ref mut r) => pin!(r).poll_flush(cx),
            Self::Tls(ref mut r) => pin!(r).poll_flush(cx),
            Self::Http1(ref mut r) => pin!(r).poll_flush(cx),
            Self::Http(ref mut r) => pin!(r).poll_flush(cx),
            Self::GraphQl(ref mut r) => panic!(),
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::Tcp(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Tls(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Http1(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::Http(ref mut r) => pin!(r).poll_shutdown(cx),
            Self::GraphQl(ref mut r) => panic!(),
        }
    }
}
