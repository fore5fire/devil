use cel_interpreter::Duration;
use serde::Serialize;

use crate::TlsVersion;

#[derive(Debug, Clone, Serialize)]
pub struct TlsOutput {
    pub plan: TlsPlanOutput,
    pub request: Option<TlsRequestOutput>,
    pub response: Option<TlsResponse>,
    pub errors: Vec<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsResponse {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsError {
    pub kind: String,
    pub message: String,
}
