use std::collections::HashMap;

use serde::Serialize;

use super::State;
use crate::{GraphQLOutput, GraphQLRequest, GraphQLResponse, HTTPRequest, PlanValue, StepOutput};

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
