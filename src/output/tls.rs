use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::Duration;

use crate::TlsVersion;

#[derive(Debug, Clone)]
pub struct TlsOutput {
    pub plan: TlsPlanOutput,
    pub request: Option<TlsRequestOutput>,
    pub response: Option<TlsResponse>,
    pub errors: Vec<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

impl From<TlsOutput> for Value {
    fn from(value: TlsOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("errors".into(), value.errors.into()),
                ("version".into(), value.version.as_ref().into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub body: Vec<u8>,
}

impl From<TlsPlanOutput> for Value {
    fn from(value: TlsPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.into()),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), value.body.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TlsRequestOutput> for Value {
    fn from(value: TlsRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.into()),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), value.body.into()),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsResponse {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TlsResponse> for Value {
    fn from(value: TlsResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), value.body.into()),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsError {
    pub kind: String,
    pub message: String,
}

impl From<TlsError> for Value {
    fn from(value: TlsError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
