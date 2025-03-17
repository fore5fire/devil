use std::sync::Arc;

use cel_interpreter::Duration;
use doberman_derive::{BigQuerySchema, Record};
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

use super::{HttpHeader, MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http2")]
#[bigquery(tag = "kind")]
#[record(rename = "http2")]
pub struct Http2Output {
    pub name: ProtocolName,
    pub plan: Http2PlanOutput,
    pub request: Option<Arc<Http2RequestOutput>>,
    pub response: Option<Arc<Http2Response>>,
    pub errors: Vec<Http2Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct Http2PlanOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<HttpHeader>,
    pub trailers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http2_request")]
#[bigquery(tag = "kind")]
#[record(rename = "http2_request")]
pub struct Http2RequestOutput {
    pub name: PduName,
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub headers: Vec<HttpHeader>,
    pub trailers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
    pub duration: Duration,
    pub headers_duration: Option<Duration>,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http2_response")]
#[bigquery(tag = "kind")]
#[record(rename = "http2_response")]
pub struct Http2Response {
    pub name: PduName,
    pub status_code: Option<u16>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<HttpHeader>>,
    pub trailers: Option<Vec<HttpHeader>>,
    pub body: Option<MaybeUtf8>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct Http2Error {
    pub kind: String,
    pub message: String,
}
