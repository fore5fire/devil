use std::{sync::Arc, time::Instant};

use chrono::Duration;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{runner::Runner, Context};
use crate::{GraphQlError, GraphQlOutput, GraphQlPlanOutput, WithPlannedCapacity};

#[derive(Debug)]
pub(super) struct GraphQlRunner {
    ctx: Arc<Context>,
    out: GraphQlOutput,
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

impl GraphQlRunner {
    pub(super) fn new(ctx: Arc<Context>, plan: GraphQlPlanOutput) -> crate::Result<Self> {
        let body = GraphQlRequestPayload {
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
            out: GraphQlOutput {
                request: None,
                response: None,
                errors: Vec::new(),
                duration: Duration::zero(),
                pause: crate::GraphQlPauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
            ctx,
            state: State::Pending,
            resp_start_time: None,
            end_time: None,
            resp: Vec::new(),
            http_body: serde_json::to_vec(&body).map_err(|e| crate::Error(e.to_string()))?,
        })
    }
}

impl<'a> GraphQlRunner {
    pub fn size_hint(&mut self, hint: Option<usize>) -> Option<usize> {
        hint
    }

    pub fn executor_size_hint(&self) -> Option<usize> {
        Some(self.http_body.len())
    }

    pub async fn start(
        &mut self,
        transport: Runner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            self.out.errors.push(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        if let Err(e) = transport.flush().await {
            self.out.errors.push(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.resp_start_time = Some(Instant::now());
        if let Err(e) = transport.read_to_end(&mut self.resp).await {
            self.out.errors.push(GraphQlError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            return;
        }
        self.end_time = Some(Instant::now());
    }

    pub fn finish(mut self) -> (GraphQlOutput, Option<Runner>) {
        let State::Running {
            start_time,
            transport,
        } = self.state
        else {
            return (self.out, None);
        };
        let end_time = Instant::now();
        let resp_body: Option<serde_json::Value> = match serde_json::from_slice(&self.resp) {
            Ok(resp) => Some(resp),
            Err(e) => {
                self.out.errors.push(GraphQlError {
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
            chrono::Duration::from_std(self.end_time.unwrap_or(end_time) - start_time).unwrap();
        (self.out, Some(transport))
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
