use crate::{Output, Protocol};

use super::http1::HTTP1Runner;
use super::tcp::TCPRunner;
use super::tls::TLSRunner;
use super::{http::HTTPRunner, tee::Stream};

use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug)]
pub enum Runner<S: Stream> {
    GraphQL(GraphQLRunner),
    HTTP(HTTPRunner),
    HTTP1(HTTP1Runner<S>),
    //HTTP2(HTTP2Runner),
    //HTTP3(HTTP3Runner),
    TLS(TLSRunner<S>),
    TCP(TCPRunner),
    //UDP(UDPRunner),
    //QUIC(QUICRunner),
}

impl<S: Stream> Runner<S> {
    pub(super) async fn new(stream: Option<S>, output: Output) -> crate::Result<Runner<S>> {
        match proto {
            Protocol::TCP(proto) => Runner::TCP(Box::new(TCPRunner::new(output).await?)),
            Protocol::HTTP(proto) => Runner::HTTP(HTTPRunner::new(proto.evaluate(inputs)?).await?),
            Protocol::TLS(proto) => Runner::TLS(
                TLSRunner::new(
                    runner.expect("no plan should have tls as a base protocol"),
                    output,
                )
                .await?,
            ),
            Protocol::HTTP1(proto) => Runner::HTTP1(
                HTTP1Runner::new(
                    runner.expect("no plan should have http1 as a base protocol"),
                    output,
                )
                .await?,
            ),
            Protocol::GraphQL(proto) => Runner::GraphQL(
                GraphQLRunner::new(
                    runner.expect("no plan should have graphql as a base protocol"),
                    output,
                )
                .await?,
            ),
            _ => Error::from("protocol unimplemented"),
        }
    }

    pub(super) async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::GraphQL(r) => r.execute().await,
            Self::HTTP(r) => r.execute().await,
            Self::HTTP1(r) => r.execute().await,
            Self::TLS(r) => r.execute().await,
            Self::TCP(r) => r.execute().await,
        }
    }

    pub(super) async fn finish(
        self,
    ) -> Result<(Output, Option<S>), Box<dyn std::error::Error + Send + Sync>> {
        Ok(match self {
            Self::GraphQL(r) => {
                let (out, inner) = r.finish().await?;
                (Output::GraphQL(out), Some(inner))
            }
            Self::HTTP(r) => (Output::HTTP(r.finish().await?), None),
            Self::HTTP1(r) => {
                let (out, inner) = r.finish().await?;
                (Output::HTTP1(out), Some(inner))
            }
            Self::TLS(r) => {
                let (out, inner) = r.finish().await;
                (Output::TLS(out), Some(inner))
            }
            Self::TCP(r) => (Output::TCP(r.finish().await), None),
        })
    }
}

impl<S: Stream> AsyncRead for Runner<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            Self::GraphQL(mut s) => Pin::new(&mut s).poll_read(cx, buf),
            Self::HTTP(mut s) => Pin::new(&mut s).poll_read(cx, buf),
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_read(cx, buf),
            Self::TCP(mut s) => Pin::new(&mut s).poll_read(cx, buf),
            Self::TLS(mut s) => Pin::new(&mut s).poll_read(cx, buf),
        }
    }
}

impl<S: Stream> AsyncWrite for Runner<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match *self {
            Self::GraphQL(mut s) => Pin::new(&mut s).poll_write(cx, buf),
            Self::HTTP(mut s) => Pin::new(&mut s).poll_write(cx, buf),
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_write(cx, buf),
            Self::TCP(mut s) => Pin::new(&mut s).poll_write(cx, buf),
            Self::TLS(mut s) => Pin::new(&mut s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::GraphQL(mut s) => Pin::new(&mut s).poll_flush(cx),
            Self::HTTP(mut s) => Pin::new(&mut s).poll_flush(cx),
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_flush(cx),
            Self::TCP(mut s) => Pin::new(&mut s).poll_flush(cx),
            Self::TLS(mut s) => Pin::new(&mut s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match *self {
            Self::GraphQL(mut s) => Pin::new(&mut s).poll_shutdown(cx),
            Self::HTTP(mut s) => Pin::new(&mut s).poll_shutdown(cx),
            Self::HTTP1(mut s) => Pin::new(&mut s).poll_shutdown(cx),
            Self::TCP(mut s) => Pin::new(&mut s).poll_shutdown(cx),
            Self::TLS(mut s) => Pin::new(&mut s).poll_shutdown(cx),
        }
    }
}
