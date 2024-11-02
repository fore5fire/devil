use std::{collections::HashMap, sync::Arc};

use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct GraphqlOutput {
    pub name: ProtocolName,
    pub plan: GraphqlPlanOutput,
    pub request: Option<Arc<GraphqlRequestOutput>>,
    pub response: Option<Arc<GraphqlResponse>>,
    pub errors: Vec<GraphqlError>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphqlPlanOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct GraphqlRequestOutput {
    pub name: PduName,
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct GraphqlResponse {
    pub name: PduName,
    pub data: serde_json::Value,
    pub errors: serde_json::Value,
    pub full: serde_json::Value,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphqlError {
    pub kind: String,
    pub message: String,
}
