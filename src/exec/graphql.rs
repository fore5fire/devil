use std::{collections::HashMap, time::Instant};

use async_trait::async_trait;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::runner::Runner;
use crate::{GraphQlOutput, GraphQlRequestOutput, GraphQlResponse, Output};

#[derive(Debug)]
pub(super) struct GraphQlRunner {
    req: GraphQlRequestOutput,
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
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = GraphQlRequestPayload {
            query: self.req.query.clone(),
            operation_name: self.req.operation.clone(),
            variables: self.req.params.clone(),
        };
        let (http_body, url) = if self.req.use_query_string {
            let mut query_pairs = self.req.url.query_pairs_mut();
            query_pairs.append_pair("query", &body.query);
            if let Some(name) = &body.operation_name {
                query_pairs.append_pair("operationName", &name);
            }
            if !body.variables.is_some() {
                query_pairs.append_pair("variables", &serde_json::to_string(&body.variables)?);
            }
            //query_pairs.append_pair("extensions", &serde_json::to_string(&body.extensions)?);
            query_pairs.finish();
            drop(query_pairs);
            (None, self.req.url.clone())
        } else {
            (Some(serde_json::to_string(&body)?), self.req.url.clone())
        };
        if let Some(http_body) = &http_body {
            self.transport.size_hint(http_body.len());
            self.transport.write_all(http_body.as_bytes()).await?;
            self.transport.flush().await?;
        }
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
    query: String,
    // TODO: Figure out how we can represent both unset and null for this field.
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
    // TODO: Figure out how we can represent unset, null, or empty object for this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<HashMap<String, Option<String>>>,
}
