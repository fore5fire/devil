use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};

use super::{runner::Runner, State};
use crate::{
    GraphQLOutput, GraphQLRequest, GraphQLResponse, HTTPRequest, Output, PlanValue, StepOutput,
};

#[derive(Debug)]
pub(super) struct GraphQLRunner<T: Runner> {
    out: Arc<Mutex<GraphQLOutput>>,
    transport: T,
    start_time: Instant,
}

impl<T: Runner> GraphQLRunner<T> {
    pub(super) async fn new(transport: T, data: GraphQLOutput) -> crate::Result<Self> {
        let start_time = Instant::now();

        if let Some(p) = data.pause.iter().find(|p| p.after == "open") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration).await;
        }
        Ok(Self {
            transport,
            start_time,
            out: data,
        })
    }
}

impl<T: Runner> AsyncRead for GraphQLRunner<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        panic!()
    }
}

impl<T: Runner> AsyncWrite for GraphQLRunner<T> {
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
impl Runner for GraphQLRunner<Box<dyn Runner>> {
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = GraphQLRequestPayload {
            query: self.out.query,
            operation_name: self.out.operation,
            variables: self.out.params,
        };
        let (http_body, url) = if self.out.use_query_string {
            let mut query_pairs = self.out.url.query_pairs_mut();
            query_pairs.append_pair("query", &body.query);
            if let Some(name) = &body.operation_name {
                query_pairs.append_pair("operationName", &name);
            }
            if !body.variables.is_empty() {
                query_pairs.append_pair("variables", &serde_json::to_string(&body.variables)?);
            }
            //query_pairs.append_pair("extensions", &serde_json::to_string(&body.extensions)?);
            query_pairs.finish();
            drop(query_pairs);
            (None, self.out.url.clone())
        } else {
            (Some(serde_json::to_string(&body)?), self.out.url.clone())
        };
        let body = serde_json::to_string(&body)?;
        self.transport.size_hint(body.len());
        self.transport.write_all(body.as_bytes()).await?;
        self.transport.flush().await?;
        if let Some(p) = self.out.pause.iter().find(|p| p.after == "request_body") {
            println!("pausing after {} for {:?}", p.after, p.duration);
            tokio::time::sleep(p.duration).await;
        }
        let mut response = Vec::new();
        self.transport.read_to_end(&mut response).await?;
        Ok(())
    }

    async fn finish(mut self) -> crate::Result<(Output, Option<Box<dyn Runner>>)> {
        Ok((Output::GraphQL(self.out), Some(self.transport)))
    }
}

pub(super) async fn execute(
    req: &GraphQLRequest,
    state: &State<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    let body = GraphQLRequestPayload {
        query: req.query.evaluate(state)?,
        operation_name: req
            .operation
            .clone()
            .map(|o| o.evaluate(state))
            .transpose()?,
        variables: req.params.evaluate(state)?.into_iter().collect(),
    };
    let raw_url = req.url.evaluate(state)?;
    let mut url: url::Url = raw_url.parse()?;
    let use_query_string = req.use_query_string.evaluate(state)?;
    let (http_body, url) = if use_query_string {
        let mut query_pairs = url.query_pairs_mut();
        query_pairs.append_pair("query", &body.query);
        if let Some(name) = &body.operation_name {
            query_pairs.append_pair("operationName", &name);
        }
        if !body.variables.is_empty() {
            query_pairs.append_pair("variables", &serde_json::to_string(&body.variables)?);
        }
        //query_pairs.append_pair("extensions", &serde_json::to_string(&body.extensions)?);
        query_pairs.finish();
        drop(query_pairs);
        (None, url)
    } else {
        (Some(serde_json::to_string(&body)?), url)
    };
    let mut out = super::http::execute(
        &HTTPRequest {
            body: req
                .http
                .body
                .clone()
                .or_else(|| http_body.map(PlanValue::Literal)),
            url: PlanValue::Literal(url.to_string()),
            ..req.http.clone()
        },
        state,
    )
    .await?;

    let http = out.http.as_ref().unwrap();
    let resp: serde_json::Value = serde_json::from_slice(&http.response.body)?;

    out.graphql = Some(GraphQLOutput {
        url: http.url.clone(),
        query: body.query,
        operation: body.operation_name,
        params: body.variables,
        use_query_string,
        response: GraphQLResponse {
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
            // TODO: give just the time starting after headers are sent.
            duration: http.response.duration,
        },
    });

    Ok(out)
}

#[derive(Debug, Serialize)]
struct GraphQLRequestPayload {
    query: String,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
    variables: HashMap<String, String>,
}
