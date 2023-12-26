use crate::{Output, StepPlanOutput};

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
    async fn execute(&mut self);
    async fn finish(self: Box<Self>) -> (Output, Option<Box<dyn Runner>>);
}

pub(super) async fn new_runner(
    transport: Option<Box<dyn Runner>>,
    step: StepPlanOutput,
) -> crate::Result<Box<dyn Runner>> {
    Ok(match step {
        StepPlanOutput::Tcp(output) => {
            assert!(transport.is_none());
            Box::new(TcpRunner::new(output).await?) as Box<dyn Runner>
        }
        StepPlanOutput::Http(output) => {
            assert!(transport.is_none());
            Box::new(HttpRunner::new(output).await?) as Box<dyn Runner>
        }
        StepPlanOutput::Tls(output) => Box::new(
            TlsRunner::new(
                transport.expect("no plan should have tls as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        StepPlanOutput::Http1(output) => Box::new(
            Http1Runner::new(
                transport.expect("no plan should have http1 as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
        StepPlanOutput::GraphQl(output) => Box::new(
            GraphQlRunner::new(
                transport.expect("no plan should have graphql as a base protocol"),
                output,
            )
            .await?,
        ) as Box<dyn Runner>,
    })
}
