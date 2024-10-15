use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

#[derive(Debug, Clone, Serialize)]
pub struct Http1Output {
    pub plan: Http1PlanOutput,
    pub request: Option<Http1RequestOutput>,
    pub response: Option<Http1Response>,
    pub errors: Vec<Http1Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1PlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub version_string: Option<Vec<u8>>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1RequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub version_string: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1Response {
    pub protocol: Option<Vec<u8>>,
    pub status_code: Option<u16>,
    pub status_reason: Option<Vec<u8>>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub body: Option<Vec<u8>>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http1Error {
    pub kind: String,
    pub message: String,
}
