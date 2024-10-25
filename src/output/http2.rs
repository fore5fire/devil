use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

use super::MaybeUtf8;

#[derive(Debug, Clone, Serialize)]
pub struct Http2Output {
    pub plan: Http2PlanOutput,
    pub request: Option<Http2RequestOutput>,
    pub response: Option<Http2Response>,
    pub errors: Vec<Http2Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2PlanOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(MaybeUtf8, MaybeUtf8)>,
    pub trailers: Vec<(MaybeUtf8, MaybeUtf8)>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2RequestOutput {
    pub url: Url,
    pub method: Option<MaybeUtf8>,
    pub headers: Vec<(MaybeUtf8, MaybeUtf8)>,
    pub body: MaybeUtf8,
    pub duration: Duration,
    pub headers_duration: Option<Duration>,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2Response {
    pub status_code: Option<u16>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<(Option<MaybeUtf8>, MaybeUtf8)>>,
    pub body: Option<MaybeUtf8>,
    pub trailers: Option<Vec<(Option<MaybeUtf8>, MaybeUtf8)>>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2Error {
    pub kind: String,
    pub message: String,
}
