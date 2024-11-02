use std::sync::Arc;

use cel_interpreter::Duration;
use serde::Serialize;

use crate::TlsVersion;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tls")]
pub struct TlsOutput {
    pub name: ProtocolName,
    pub plan: TlsPlanOutput,
    pub sent: Option<Arc<TlsSentOutput>>,
    pub received: Option<Arc<TlsReceivedOutput>>,
    pub errors: Vec<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<MaybeUtf8>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tls_sent")]
pub struct TlsSentOutput {
    pub name: PduName,
    pub host: String,
    pub port: u16,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tls_recv")]
pub struct TlsReceivedOutput {
    pub name: PduName,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TlsError {
    pub kind: String,
    pub message: String,
}
