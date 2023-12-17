use crate::{Output, RequestOutput};

use super::{
    graphql::GraphQlRunner, http::HttpRunner, http1::Http1Runner, tcp::TcpRunner, tee::Stream,
    tls::TlsRunner,
};
use async_trait::async_trait;

#[async_trait]
pub(crate) trait Runner: Stream + std::fmt::Debug {
    async fn start(
        &mut self,
        size_hint: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn finish(self: Box<Self>) -> crate::Result<(Output, Option<Box<dyn Runner>>)>;
}

pub(super) async fn new_runner(
    transport: Option<Box<dyn Runner>>,
    req: RequestOutput,
) -> crate::Result<Box<dyn Runner>> {
    Ok(match req {
        RequestOutput::Tcp(output) => {
            assert!(transport.is_none());
            Box::new(TcpRunner::new(output).await?) as Box<dyn Runner>
        }
        RequestOutput::Http(output) => {
            assert!(transport.is_none());
            Box::new(HttpRunner::new(output).await?) as Box<dyn Runner>
        }
        RequestOutput::Tls(output) => Box::new(
            TlsRunner::new(
                transport.expect("no plan should have tls as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        RequestOutput::Http1(output) => Box::new(
            Http1Runner::new(
                transport.expect("no plan should have http1 as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        RequestOutput::GraphQl(output) => Box::new(
            GraphQlRunner::new(
                transport.expect("no plan should have graphql as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
    })
}
