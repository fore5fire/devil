use std::{sync::Arc, time::Instant};

use chrono::Duration;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{runner::Runner, Context};
use crate::{
    GraphqlError, GraphqlOutput, GraphqlPlanOutput, GraphqlRequestOutput, PduName, ProtocolName,
    ProtocolOutputDiscriminants,
};

#[derive(Debug)]
pub(super) struct GraphqlRunner {
    ctx: Arc<Context>,
    out: GraphqlOutput,
    http_body: Vec<u8>,
    resp: Vec<u8>,
    state: State,
    resp_start_time: Option<Instant>,
    end_time: Option<Instant>,
}

#[derive(Debug)]
enum State {
    Pending,
    Running {
        start_time: Instant,
        transport: Runner,
    },
    Completed {
        transport: Option<Runner>,
    },
}

impl GraphqlRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: GraphqlPlanOutput) -> crate::Result<Self> {
        let body = GraphqlRequestPayload {
            query: serde_json::Value::String(plan.query.clone()),
            operation_name: plan.operation.clone(),
            variables: plan.params.clone().map(|params| {
                serde_json::Value::Object(
                    params
                        .into_iter()
                        .map(|(k, v)| (String::from_utf8_lossy(k.as_slice()).to_string(), v))
                        .collect(),
                )
            }),
        };

        Ok(Self {
            out: GraphqlOutput {
                name: ProtocolName::with_job(
                    ctx.job_name.clone(),
                    ProtocolOutputDiscriminants::Graphql,
                ),
                request: None,
                response: None,
                errors: Vec::new(),
                duration: Duration::zero().into(),
                plan,
            },
            ctx,
            state: State::Pending,
            resp_start_time: None,
            end_time: None,
            resp: Vec::new(),
            http_body: serde_json::to_vec(&body)?,
        })
    }
}

impl<'a> GraphqlRunner {
    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        hint
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.http_body.len())
    }

    pub async fn start(&mut self, transport: Runner) -> anyhow::Result<()> {
        self.state = State::Running {
            start_time: Instant::now(),
            transport,
        };
        Ok(())
    }

    pub async fn execute(&mut self) {
        let State::Running { transport, .. } = &mut self.state else {
            panic!("execute called in unsupported state: {:?}", self.state)
        };
        if let Err(e) = transport.write_all(&self.http_body).await {
            self.out.errors.push(GraphqlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        if let Err(e) = transport.flush().await {
            self.out.errors.push(GraphqlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.resp_start_time = Some(Instant::now());
        if let Err(e) = transport.read_to_end(&mut self.resp).await {
            self.out.errors.push(GraphqlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.end_time = Some(Instant::now());
    }

    pub fn finish(mut self) -> (GraphqlOutput, Option<Runner>) {
        let end_time = self.end_time.unwrap_or(Instant::now());

        let State::Running {
            start_time,
            transport,
        } = self.state
        else {
            return (self.out, None);
        };

        // TODO: Reflect how far a failed request got
        if let Some(req_end) = self.resp_start_time {
            self.out.request = Some(Arc::new(GraphqlRequestOutput {
                name: PduName::with_job(
                    self.ctx.job_name.clone(),
                    ProtocolOutputDiscriminants::Graphql,
                    0,
                ),
                url: self.out.plan.url.clone(),
                query: self.out.plan.query.clone(),
                operation: self.out.plan.operation.clone(),
                params: self.out.plan.params.clone(),
                duration: Duration::from_std(req_end - start_time).unwrap().into(),
            }));
        }

        let resp_body: Option<serde_json::Value> = match serde_json::from_slice(&self.resp) {
            Ok(resp) => Some(resp),
            Err(e) => {
                self.out.errors.push(GraphqlError {
                    kind: "json response body deserialize".to_owned(),
                    message: e.to_string(),
                });
                None
            }
        };

        if let Some(resp_body) = resp_body {
            self.out.response = Some(Arc::new(crate::GraphqlResponse {
                name: PduName::with_job(
                    self.ctx.job_name.clone(),
                    ProtocolOutputDiscriminants::Graphql,
                    1,
                ),
                data: resp_body
                    .get("data")
                    .unwrap_or(&serde_json::Value::Null)
                    .clone()
                    .into(),
                errors: resp_body
                    .get("errors")
                    .unwrap_or(&serde_json::Value::Null)
                    .clone()
                    .into(),
                full: resp_body.into(),
                duration: chrono::Duration::from_std(
                    end_time
                        - self
                            .resp_start_time
                            .expect("response start time should be set before header is processed"),
                )
                .unwrap()
                .into(),
            }));
        }

        self.out.duration = chrono::Duration::from_std(end_time - start_time)
            .unwrap()
            .into();

        (self.out, Some(transport))
    }
}

#[derive(Debug, Serialize)]
struct GraphqlRequestPayload {
    query: serde_json::Value,
    #[serde(rename = "operationName", skip_serializing_if = "Option::is_none")]
    operation_name: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<serde_json::Value>,
}
