use crate::Output;

use super::{
    graphql::GraphQLRunner, http::HTTPRunner, http1::HTTP1Runner, tcp::TCPRunner, tee::Stream,
    tls::TLSRunner,
};
use async_trait::async_trait;

#[async_trait]
pub trait Runner: Stream {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn finish(self) -> crate::Result<(Output, Option<Box<dyn Runner>>)>;
    fn size_hint(&mut self, size: usize) {}
}

#[async_trait]
impl Runner for Box<dyn Runner> {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        (*self).execute().await
    }
    async fn finish(self) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        (*self).finish().await
    }
    fn size_hint(&mut self, size: usize) {
        (*self).size_hint(size)
    }
}

pub(super) async fn new_runner(
    runner: Option<Box<dyn Runner>>,
    output: Output,
) -> crate::Result<Box<dyn Runner>> {
    Ok(match output {
        Output::TCP(output) => Box::new(TCPRunner::new(output).await?) as Box<dyn Runner>,
        Output::HTTP(output) => Box::new(HTTPRunner::new(output).await?) as Box<dyn Runner>,
        Output::TLS(output) => Box::new(
            TLSRunner::new(
                runner.expect("no plan should have tls as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        Output::HTTP1(output) => Box::new(
            HTTP1Runner::new(
                runner.expect("no plan should have http1 as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        Output::GraphQL(output) => Box::new(
            GraphQLRunner::new(
                runner.expect("no plan should have graphql as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        _ => return Err(crate::Error::from("protocol unimplemented")),
    })
}
