use std::sync::Arc;

use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct Http1Output {
    pub name: ProtocolName,
    pub plan: Http1PlanOutput,
    pub request: Option<Arc<Http1RequestOutput>>,
    pub response: Option<Arc<Http1Response>>,
    pub errors: Vec<Http1Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1PlanOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub version_string: Option<MaybeUtf8>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(MaybeUtf8, MaybeUtf8)>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct Http1RequestOutput {
    pub name: PduName,
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub version_string: Option<MaybeUtf8>,
    pub headers: Vec<(MaybeUtf8, MaybeUtf8)>,
    pub body: MaybeUtf8,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "snake_case")]
pub struct Http1Response {
    pub name: PduName,
    pub protocol: Option<MaybeUtf8>,
    pub status_code: Option<u16>,
    pub status_reason: Option<MaybeUtf8>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<(MaybeUtf8, MaybeUtf8)>>,
    pub body: Option<MaybeUtf8>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1Error {
    pub kind: String,
    pub message: String,
}
