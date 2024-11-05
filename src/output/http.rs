use std::sync::Arc;

use cel_interpreter::Duration;
use devil_derive::{BigQuerySchema, Record};
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http")]
#[bigquery(tag = "kind")]
#[record(rename = "http")]
pub struct HttpOutput {
    pub name: ProtocolName,
    pub plan: HttpPlanOutput,
    pub request: Option<Arc<HttpRequestOutput>>,
    pub response: Option<Arc<HttpResponse>>,
    pub errors: Vec<HttpError>,
    pub protocol: Option<String>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct HttpPlanOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
}

impl From<(MaybeUtf8, MaybeUtf8)> for HttpHeader {
    fn from(value: (MaybeUtf8, MaybeUtf8)) -> Self {
        Self {
            key: Some(value.0),
            value: value.1,
        }
    }
}

impl From<(Option<MaybeUtf8>, MaybeUtf8)> for HttpHeader {
    fn from(value: (Option<MaybeUtf8>, MaybeUtf8)) -> Self {
        Self {
            key: value.0,
            value: value.1,
        }
    }
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct HttpHeader {
    pub key: Option<MaybeUtf8>,
    pub value: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http_request")]
#[bigquery(tag = "kind")]
#[record(rename = "http_request")]
pub struct HttpRequestOutput {
    pub name: PduName,
    pub url: Url,
    pub protocol: MaybeUtf8,
    pub method: Option<MaybeUtf8>,
    pub headers: Vec<HttpHeader>,
    pub body: MaybeUtf8,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "http_response")]
#[bigquery(tag = "kind")]
#[record(rename = "http_response")]
pub struct HttpResponse {
    pub name: PduName,
    pub protocol: Option<MaybeUtf8>,
    pub status_code: Option<u16>,
    pub headers: Option<Vec<HttpHeader>>,
    pub body: Option<MaybeUtf8>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct HttpError {
    pub kind: String,
    pub message: String,
}
