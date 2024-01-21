use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::Duration;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{runner::Runner, Context};
use crate::{
    GraphQlError, GraphQlOutput, GraphQlPlanOutput, Output, PauseOutput, WithPlannedCapacity,
};

#[derive(Debug)]
pub(super) struct GraphQlRunner {
    ctx: Arc<Context>,
    out: GraphQlOutput,
    http_body: Vec<u8>,
    resp: Vec<u8>,
    transport: Box<dyn Runner>,
    start_time: Instant,
    resp_start_time: Option<Instant>,
    end_time: Option<Instant>,
}

impl GraphQlRunner {
    pub(super) async fn new(
        ctx: Arc<Context>,
        transport: Box<dyn Runner>,
        plan: GraphQlPlanOutput,
    ) -> crate::Result<Self> {
        let start_time = Instant::now();

        Ok(Self {
            out: GraphQlOutput {
                request: None,
                response: None,
                error: None,
                duration: Duration::zero(),
                pause: PauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
            ctx,
            transport,
            start_time,
            resp_start_time: None,
            end_time: None,
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
            query: serde_json::Value::String(self.out.plan.query.clone()),
            operation_name: self.out.plan.operation.clone(),
            variables: self.out.plan.params.clone().map(|params| {
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

    async fn execute(&mut self) {
        if let Err(e) = self.start(None).await {
            self.out.error = Some(GraphQlError {
                kind: "start failed".to_owned(),
                message: e.to_string(),
            });
            return;
        }
        if let Err(e) = self.transport.write_all(&self.http_body).await {
            self.out.error = Some(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        if let Err(e) = self.transport.flush().await {
            self.out.error = Some(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.resp_start_time = Some(Instant::now());
        if let Err(e) = self.transport.read_to_end(&mut self.resp).await {
            self.out.error = Some(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.end_time = Some(Instant::now());
    }

    async fn finish(mut self: Box<Self>) -> (Output, Option<Box<dyn Runner>>) {
        let end_time = Instant::now();
        let resp_body: Option<serde_json::Value> = match serde_json::from_slice(&self.resp) {
            Ok(resp) => Some(resp),
            Err(e) => {
                self.out.error = Some(GraphQlError {
                    kind: "json response body deserialize".to_owned(),
                    message: e.to_string(),
                });
                None
            }
        };

        if let Some(resp) = &mut self.out.response {
            if let Some(resp_body) = resp_body {
                resp.data = resp_body
                    .get("data")
                    .unwrap_or(&serde_json::Value::Null)
                    .clone()
                    .into();
                resp.data = resp_body
                    .get("errors")
                    .unwrap_or(&serde_json::Value::Null)
                    .clone()
                    .into();
                resp.full = resp_body.into();
            }
            resp.duration = chrono::Duration::from_std(
                self.end_time.unwrap_or(end_time)
                    - self
                        .resp_start_time
                        .expect("response start time should be set before header is processed"),
            )
            .unwrap();
        }
        self.out.duration =
            chrono::Duration::from_std(self.end_time.unwrap_or(end_time) - self.start_time)
                .unwrap();
        (Output::GraphQl(self.out), Some(self.transport))
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
