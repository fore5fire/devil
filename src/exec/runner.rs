use crate::{Output, RequestOutput};

use super::{
    graphql::GraphQLRunner, http::HTTPRunner, http1::HTTP1Runner, tcp::TCPRunner, tee::Stream,
    tls::TLSRunner,
};
use async_trait::async_trait;

#[async_trait]
pub(crate) trait Runner: Stream {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn finish(self) -> crate::Result<(Output, Option<Box<dyn Runner>>)>;
    fn size_hint(&mut self, _size: usize) {}
}

#[async_trait]
impl Runner for Box<dyn Runner> {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        (*self).execute().await
    }
    async fn finish(self) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        self.finish().await
    }
    fn size_hint(&mut self, size: usize) {
        self.as_mut().size_hint(size)
    }
}

pub(super) async fn new_runner(
    transport: Option<Box<dyn Runner>>,
    req: RequestOutput,
) -> crate::Result<Box<dyn Runner>> {
    Ok(match req {
        RequestOutput::TCP(output) => {
            assert!(transport.is_none());
            Box::new(TCPRunner::new(output).await?) as Box<dyn Runner>
        }
        RequestOutput::HTTP(output) => {
            assert!(transport.is_none());
            Box::new(HTTPRunner::new(output).await?) as Box<dyn Runner>
        }
        RequestOutput::TLS(output) => Box::new(
            TLSRunner::new(
                transport.expect("no plan should have tls as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        RequestOutput::HTTP1(output) => Box::new(
            HTTP1Runner::new(
                transport.expect("no plan should have http1 as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        RequestOutput::GraphQL(output) => Box::new(
            GraphQLRunner::new(
                transport.expect("no plan should have graphql as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
    })
}
