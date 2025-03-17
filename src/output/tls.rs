use std::{str::FromStr, sync::Arc};

use anyhow::bail;
use cel_interpreter::Duration;
use doberman_derive::{BigQuerySchema, Record};
use serde::Serialize;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "tls")]
#[bigquery(tag = "kind")]
#[record(rename = "tls")]
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

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<MaybeUtf8>,
    pub body: MaybeUtf8,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "tls_sent")]
#[bigquery(tag = "kind")]
#[record(rename = "tls_sent")]
pub struct TlsSentOutput {
    pub name: PduName,
    pub host: String,
    pub port: u16,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "tls_received")]
#[bigquery(tag = "kind")]
#[record(rename = "tls_received")]
pub struct TlsReceivedOutput {
    pub name: PduName,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct TlsError {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct TlsVersion {
    pub parsed: Option<ParsedTlsVersion>,
    pub raw: u16,
}

impl From<u16> for TlsVersion {
    fn from(value: u16) -> Self {
        Self {
            parsed: match value {
                0x0002 => Some(ParsedTlsVersion::Ssl2),
                0x0300 => Some(ParsedTlsVersion::Ssl3),
                0x0301 => Some(ParsedTlsVersion::Tls1_0),
                0x0302 => Some(ParsedTlsVersion::Tls1_1),
                0x0303 => Some(ParsedTlsVersion::Tls1_2),
                0x0304 => Some(ParsedTlsVersion::Tls1_3),
                0xFEFF => Some(ParsedTlsVersion::Dtls1_0),
                0xFEFE => Some(ParsedTlsVersion::Dtls1_1),
                0xFEFD => Some(ParsedTlsVersion::Dtls1_2),
                0xFEFC => Some(ParsedTlsVersion::Dtls1_3),
                _ => None,
            },
            raw: value,
        }
    }
}

impl From<rustls::ProtocolVersion> for TlsVersion {
    fn from(value: rustls::ProtocolVersion) -> Self {
        Self {
            // FIXME: SSL2 used a different format than later versions, but rustls normalizes it to
            // match the modern format, so what we get here may not match what's on the wire.
            raw: value.get_u16(),
            parsed: match &value {
                rustls::ProtocolVersion::SSLv2 => Some(ParsedTlsVersion::Ssl2),
                rustls::ProtocolVersion::SSLv3 => Some(ParsedTlsVersion::Ssl3),
                rustls::ProtocolVersion::TLSv1_0 => Some(ParsedTlsVersion::Tls1_0),
                rustls::ProtocolVersion::TLSv1_1 => Some(ParsedTlsVersion::Tls1_1),
                rustls::ProtocolVersion::TLSv1_2 => Some(ParsedTlsVersion::Tls1_2),
                rustls::ProtocolVersion::TLSv1_3 => Some(ParsedTlsVersion::Tls1_3),
                rustls::ProtocolVersion::DTLSv1_0 => Some(ParsedTlsVersion::Dtls1_0),
                rustls::ProtocolVersion::DTLSv1_2 => Some(ParsedTlsVersion::Dtls1_2),
                rustls::ProtocolVersion::DTLSv1_3 => Some(ParsedTlsVersion::Dtls1_3),
                _ => None,
            },
        }
    }
}

impl From<ParsedTlsVersion> for TlsVersion {
    fn from(value: ParsedTlsVersion) -> Self {
        Self {
            parsed: Some(value),
            raw: value.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, BigQuerySchema)]
#[serde(rename_all = "snake_case", untagged)]
pub enum ParsedTlsVersion {
    Ssl2,
    Ssl3,
    #[serde(rename = "tls1.0")]
    Tls1_0,
    #[serde(rename = "tls1.1")]
    Tls1_1,
    #[serde(rename = "tls1.2")]
    Tls1_2,
    #[serde(rename = "tls1.3")]
    Tls1_3,
    #[serde(rename = "dtls1.0")]
    Dtls1_0,
    #[serde(rename = "dtls1.1")]
    Dtls1_1,
    #[serde(rename = "dtls1.2")]
    Dtls1_2,
    #[serde(rename = "dtls1.3")]
    Dtls1_3,
}

impl From<ParsedTlsVersion> for u16 {
    fn from(value: ParsedTlsVersion) -> Self {
        match value {
            ParsedTlsVersion::Ssl2 => 0x0002,
            ParsedTlsVersion::Ssl3 => 0x0300,
            ParsedTlsVersion::Tls1_0 => 0x0301,
            ParsedTlsVersion::Tls1_1 => 0x0302,
            ParsedTlsVersion::Tls1_2 => 0x0303,
            ParsedTlsVersion::Tls1_3 => 0x0304,
            ParsedTlsVersion::Dtls1_0 => 0xFEFF,
            ParsedTlsVersion::Dtls1_1 => 0xFEFE,
            ParsedTlsVersion::Dtls1_2 => 0xFEFD,
            ParsedTlsVersion::Dtls1_3 => 0xFEFC,
        }
    }
}

impl FromStr for ParsedTlsVersion {
    type Err = crate::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.into() {
            "ssl2" => Self::Ssl2,
            "ssl3" => Self::Ssl3,
            "tls1.0" => Self::Tls1_0,
            "tls1.1" => Self::Tls1_1,
            "tls1.2" => Self::Tls1_2,
            "tls1.3" => Self::Tls1_3,
            "dtls1.0" => Self::Dtls1_0,
            "dtls1.1" => Self::Dtls1_1,
            "dtls1.2" => Self::Dtls1_2,
            "dtls1.3" => Self::Dtls1_3,
            _ => bail!("invalid tls version string {}", s),
        })
    }
}

impl From<&ParsedTlsVersion> for cel_interpreter::Value {
    fn from(value: &ParsedTlsVersion) -> Self {
        match value {
            ParsedTlsVersion::Ssl2 => cel_interpreter::Value::String(Arc::new("ssl2".to_owned())),
            ParsedTlsVersion::Ssl3 => cel_interpreter::Value::String(Arc::new("ssl3".to_owned())),
            ParsedTlsVersion::Tls1_0 => {
                cel_interpreter::Value::String(Arc::new("tls1.0".to_owned()))
            }
            ParsedTlsVersion::Tls1_1 => {
                cel_interpreter::Value::String(Arc::new("tls1.1".to_owned()))
            }
            ParsedTlsVersion::Tls1_2 => {
                cel_interpreter::Value::String(Arc::new("tls1.2".to_owned()))
            }
            ParsedTlsVersion::Tls1_3 => {
                cel_interpreter::Value::String(Arc::new("tls1.3".to_owned()))
            }
            ParsedTlsVersion::Dtls1_0 => {
                cel_interpreter::Value::String(Arc::new("dtls1.0".to_owned()))
            }
            ParsedTlsVersion::Dtls1_1 => {
                cel_interpreter::Value::String(Arc::new("dtls1.1".to_owned()))
            }
            ParsedTlsVersion::Dtls1_2 => {
                cel_interpreter::Value::String(Arc::new("dtls1.2".to_owned()))
            }
            ParsedTlsVersion::Dtls1_3 => {
                cel_interpreter::Value::String(Arc::new("dtls1.3".to_owned()))
            }
        }
    }
}
