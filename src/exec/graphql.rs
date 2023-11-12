use std::collections::HashMap;

use serde::Serialize;

use super::State;
use crate::{GraphQLOutput, GraphQLRequest, GraphQLResponse, HTTPRequest, StepOutput};

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
    let http_body = serde_json::to_string(&body)?;
    let mut out = super::http::execute(
        &HTTPRequest {
            url: req.url.clone(),
            body: crate::PlanValue::Literal(http_body),
            options: req.http.clone(),
            tls: req.tls.clone(),
            ip: req.ip.clone(),
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
