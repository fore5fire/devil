use std::fmt::Display;
use std::io;
use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use bitmask_enum::bitmask;
use byteorder::{ByteOrder, NetworkEndian};
use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::{Duration, TimeDelta};
use indexmap::IndexMap;
use itertools::Itertools;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use url::Url;

use crate::{AddContentLength, Parallelism, ProtocolField, TlsVersion};

#[derive(Debug, Clone)]
pub struct HttpOutput {
    pub plan: HttpPlanOutput,
    pub request: Option<HttpRequestOutput>,
    pub response: Option<HttpResponse>,
    pub errors: Vec<HttpError>,
    pub protocol: Option<String>,
    pub duration: Duration,
}

impl From<HttpOutput> for Value {
    fn from(value: HttpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("protocol".into(), value.protocol.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpPlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl From<HttpPlanOutput> for Value {
    fn from(value: HttpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.into()),
                (
                    "headers".into(),
                    Value::List(Arc::new(
                        value.headers.into_iter().map(kv_pair_to_map).collect(),
                    )),
                ),
                ("body".into(), value.body.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: Duration,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

impl From<HttpRequestOutput> for Value {
    fn from(value: HttpRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.clone().into()),
                (
                    "headers".into(),
                    Value::List(Arc::new(
                        value.headers.into_iter().map(kv_pair_to_map).collect(),
                    )),
                ),
                ("body".into(), value.body.clone().into()),
                ("duration".into(), value.duration.into()),
                ("body_duration".into(), value.body_duration.into()),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub protocol: Option<Vec<u8>>,
    pub status_code: Option<u16>,
    pub headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub body: Option<Vec<u8>>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

impl From<HttpResponse> for Value {
    fn from(value: HttpResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("protocol".into(), value.protocol.clone().into()),
                (
                    "status_code".into(),
                    value
                        .status_code
                        .clone()
                        .map(|status_code| Value::UInt(status_code.into()))
                        .into(),
                ),
                (
                    "headers".into(),
                    value
                        .headers
                        .clone()
                        .map(|headers| {
                            Value::List(Arc::new(headers.into_iter().map(kv_pair_to_map).collect()))
                        })
                        .into(),
                ),
                ("body".into(), value.body.clone().into()),
                ("duration".into(), value.duration.into()),
                ("header_duration".into(), value.header_duration.into()),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpError {
    pub kind: String,
    pub message: String,
}

impl From<HttpError> for Value {
    fn from(value: HttpError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
