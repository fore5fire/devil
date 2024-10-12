use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::Duration;
use url::Url;

use crate::AddContentLength;

#[derive(Debug, Clone)]
pub struct Http1Output {
    pub plan: Http1PlanOutput,
    pub request: Option<Http1RequestOutput>,
    pub response: Option<Http1Response>,
    pub errors: Vec<Http1Error>,
    pub duration: Duration,
}

impl From<Http1Output> for Value {
    fn from(value: Http1Output) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http1PlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub version_string: Option<Vec<u8>>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl From<Http1PlanOutput> for Value {
    fn from(value: Http1PlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.into()),
                ("version_string".into(), value.version_string.into()),
                (
                    "add_content_length".into(),
                    value.add_content_length.to_string().into(),
                ),
                (
                    "headers".into(),
                    Value::List(Arc::new(
                        value
                            .headers
                            .into_iter()
                            .map(super::kv_pair_to_map)
                            .collect(),
                    )),
                ),
                ("body".into(), value.body.clone().into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
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

impl From<Http1RequestOutput> for Value {
    fn from(value: Http1RequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.clone().into()),
                ("version_string".into(), value.version_string.clone().into()),
                (
                    "headers".into(),
                    Value::List(Arc::new(
                        value
                            .headers
                            .into_iter()
                            .map(super::kv_pair_to_map)
                            .collect(),
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

impl From<Http1Response> for Value {
    fn from(value: Http1Response) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                (
                    "protocol".into(),
                    value
                        .protocol
                        .clone()
                        .map(|protocol| Value::Bytes(Arc::new(protocol)))
                        .into(),
                ),
                (
                    "status_code".into(),
                    value
                        .status_code
                        .clone()
                        .map(|status_code| Value::UInt(status_code.into()))
                        .into(),
                ),
                ("status_reason".into(), value.status_reason.clone().into()),
                ("content_length".into(), value.content_length.into()),
                (
                    "headers".into(),
                    value
                        .headers
                        .clone()
                        .map(|headers| {
                            Value::List(Arc::new(
                                headers.into_iter().map(super::kv_pair_to_map).collect(),
                            ))
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
pub struct Http1Error {
    pub kind: String,
    pub message: String,
}

impl From<Http1Error> for Value {
    fn from(value: Http1Error) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
