use std::collections::HashMap;

use serde::Serialize;

use super::State;
use crate::{GraphQLOutput, GraphQLRequest, GraphQLResponse, HTTPRequest};

pub(super) async fn execute(
    req: &GraphQLRequest,
    state: &State<'_>,
) -> Result<GraphQLOutput, Box<dyn std::error::Error + Send + Sync>> {
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
    let http_out = super::http::execute(
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

    let resp: serde_json::Value = serde_json::from_slice(&http_out.response.body)?;

    Ok(GraphQLOutput {
        url: http_out.url.clone(),
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
            formatted: resp,
            raw: http_out.response.body.clone(),
        },
        http: http_out,
    })
}

#[derive(Debug, Serialize)]
struct GraphQLRequestPayload {
    query: String,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
    variables: HashMap<String, String>,
}
