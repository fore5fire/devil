use std::sync::Arc;

use cel_interpreter::Duration;
use doberman_derive::{BigQuerySchema, Record};
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

use super::{HttpHeader, MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http1")]
#[bigquery(tag = "kind")]
#[record(rename = "http1")]
pub struct Http1Output {
    pub name: ProtocolName,
    pub plan: Http1PlanOutput,
    pub request: Option<Arc<Http1RequestOutput>>,
    pub response: Option<Arc<Http1Response>>,
    pub errors: Vec<Http1Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct Http1PlanOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub version_string: Option<MaybeUtf8>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http1_request")]
#[bigquery(tag = "kind")]
#[record(rename = "http1_request")]
pub struct Http1RequestOutput {
    pub name: PduName,
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub version_string: Option<MaybeUtf8>,
    pub headers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http1_response")]
#[bigquery(tag = "kind")]
#[record(rename = "http1_response")]
pub struct Http1Response {
    pub name: PduName,
    pub protocol: Option<MaybeUtf8>,
    pub status_code: Option<u16>,
    pub status_reason: Option<MaybeUtf8>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<HttpHeader>>,
    pub body: Option<MaybeUtf8>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct Http1Error {
    pub kind: String,
    pub message: String,
}
