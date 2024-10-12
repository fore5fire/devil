use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::Duration;
use url::Url;

use crate::AddContentLength;

#[derive(Debug, Clone)]
pub struct Http2Output {
    pub plan: Http2PlanOutput,
    pub request: Option<Http2RequestOutput>,
    pub response: Option<Http2Response>,
    pub errors: Vec<Http2Error>,
    pub duration: Duration,
}

impl From<Http2Output> for Value {
    fn from(value: Http2Output) -> Self {
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
pub struct Http2PlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub trailers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl From<Http2PlanOutput> for Value {
    fn from(value: Http2PlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.into()),
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
pub struct Http2RequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: Duration,
    pub headers_duration: Option<Duration>,
    pub body_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

impl From<Http2RequestOutput> for Value {
    fn from(value: Http2RequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.clone().into()),
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
pub struct Http2Response {
    pub status_code: Option<u16>,
    pub content_length: Option<u64>,
    pub headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub body: Option<Vec<u8>>,
    pub trailers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub duration: Duration,
    pub header_duration: Option<Duration>,
    pub time_to_first_byte: Option<Duration>,
}

impl From<Http2Response> for Value {
    fn from(value: Http2Response) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                (
                    "status_code".into(),
                    value
                        .status_code
                        .clone()
                        .map(|status_code| Value::UInt(status_code.into()))
                        .into(),
                ),
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
pub struct Http2Error {
    pub kind: String,
    pub message: String,
}

impl From<Http2Error> for Value {
    fn from(value: Http2Error) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
