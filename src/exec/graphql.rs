use std::time::Instant;

use async_trait::async_trait;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::runner::Runner;
use crate::{GraphQlOutput, GraphQlRequestOutput, GraphQlResponse, Output};

#[derive(Debug)]
pub(super) struct GraphQlRunner {
    req: GraphQlRequestOutput,
    http_body: Vec<u8>,
    resp: Vec<u8>,
    transport: Box<dyn Runner>,
    start_time: Instant,
}

impl GraphQlRunner {
    pub(super) async fn new(
        transport: Box<dyn Runner>,
        req: GraphQlRequestOutput,
    ) -> crate::Result<Self> {
        let start_time = Instant::now();

        if let Some(p) = req.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        Ok(Self {
            transport,
            start_time,
            req,
            resp: Vec::new(),
            http_body: Vec::new(),
        })
    }
}

impl AsyncRead for GraphQlRunner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        panic!()
    }
}

impl AsyncWrite for GraphQlRunner {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        panic!()
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        panic!()
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        panic!()
    }
}

#[async_trait]
impl Runner for GraphQlRunner {
    async fn start(
        &mut self,
        _: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = GraphQlRequestPayload {
            query: serde_json::Value::String(self.req.query.clone()),
            operation_name: self.req.operation.clone(),
            variables: self.req.params.clone().map(|params| {
                serde_json::Value::Object(
                    params
                        .into_iter()
                        .map(|(k, v)| (String::from_utf8_lossy(k.as_slice()).to_string(), v))
                        .collect(),
                )
            }),
        };
        self.http_body = serde_json::to_vec(&body)?;
        self.transport.start(Some(self.http_body.len())).await?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.start(None).await?;
        self.transport.write_all(&self.http_body).await?;
        self.transport.flush().await?;
        if let Some(p) = self.req.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration.to_std().unwrap()).await;
        }
        self.transport.read_to_end(&mut self.resp).await?;
        Ok(())
    }

    async fn finish(self: Box<Self>) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        let resp: serde_json::Value =
            serde_json::from_slice(&self.resp).map_err(|e| crate::Error(e.to_string()))?;

        Ok((
            Output::GraphQl(GraphQlOutput {
                request: self.req,
                response: GraphQlResponse {
                    data: resp
                        .get("data")
                        .unwrap_or(&serde_json::Value::Null)
                        .clone()
                        .into(),
                    errors: resp
                        .get("errors")
                        .unwrap_or(&serde_json::Value::Null)
                        .clone()
                        .into(),
                    full: resp.clone().into(),
                    json: resp,
                    // TODO: give just the time starting after http headers are sent.
                    duration: chrono::Duration::from_std(self.start_time.elapsed()).unwrap(),
                },
            }),
            Some(self.transport),
        ))
    }
}

#[derive(Debug, Serialize)]
struct GraphQlRequestPayload {
    query: serde_json::Value,
    #[serde(rename = "operationName", skip_serializing_if = "Option::is_none")]
    operation_name: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<serde_json::Value>,
}
