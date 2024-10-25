use std::collections::HashMap;

use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use super::MaybeUtf8;

#[derive(Debug, Clone, Serialize)]
pub struct GraphQlOutput {
    pub plan: GraphQlPlanOutput,
    pub request: Option<GraphQlRequestOutput>,
    pub response: Option<GraphQlResponse>,
    pub errors: Vec<GraphQlError>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphQlPlanOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphQlRequestOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<MaybeUtf8, serde_json::Value>>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphQlResponse {
    pub data: serde_json::Value,
    pub errors: serde_json::Value,
    pub full: serde_json::Value,
    pub json: serde_json::Value,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphQlError {
    pub kind: String,
    pub message: String,
}
