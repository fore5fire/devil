use std::{collections::HashMap, sync::Arc};

use cel_interpreter::Duration;
use doberman_derive::{BigQuerySchema, Record};
use serde::Serialize;
use url::Url;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "graphql")]
#[bigquery(tag = "kind")]
#[record(rename = "graphql")]
pub struct GraphqlOutput {
    pub name: ProtocolName,
    pub plan: GraphqlPlanOutput,
    pub request: Option<Arc<GraphqlRequestOutput>>,
    pub response: Option<Arc<GraphqlResponse>>,
    pub errors: Vec<GraphqlError>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct GraphqlPlanOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "graphql_request")]
#[bigquery(tag = "kind")]
#[record(rename = "graphql_request")]
pub struct GraphqlRequestOutput {
    pub name: PduName,
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "graphql_response")]
#[bigquery(tag = "kind")]
#[record(rename = "graphql_response")]
pub struct GraphqlResponse {
    pub name: PduName,
    pub data: serde_json::Value,
    pub errors: serde_json::Value,
    pub full: serde_json::Value,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct GraphqlError {
    pub kind: String,
    pub message: String,
}
