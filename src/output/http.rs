use cel_interpreter::Duration;
use serde::Serialize;
use url::Url;

use crate::AddContentLength;

#[derive(Debug, Clone, Serialize)]
pub struct HttpOutput {
    pub plan: HttpPlanOutput,
    pub request: Option<HttpRequestOutput>,
    pub response: Option<HttpResponse>,
    pub errors: Vec<HttpError>,
    pub protocol: Option<String>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpPlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpRequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpResponse {
    pub protocol: Option<Vec<u8>>,
    pub status_code: Option<u16>,
    pub headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub body: Option<Vec<u8>>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpError {
    pub kind: String,
    pub message: String,
}
