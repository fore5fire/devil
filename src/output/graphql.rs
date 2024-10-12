use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::Duration;
use url::Url;

use crate::OutValue;

#[derive(Debug, Clone)]
pub struct GraphQlOutput {
    pub plan: GraphQlPlanOutput,
    pub request: Option<GraphQlRequestOutput>,
    pub response: Option<GraphQlResponse>,
    pub errors: Vec<GraphQlError>,
    pub duration: Duration,
}

impl From<GraphQlOutput> for Value {
    fn from(value: GraphQlOutput) -> Self {
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
pub struct GraphQlPlanOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<Vec<u8>, serde_json::Value>>,
}

impl From<GraphQlPlanOutput> for Value {
    fn from(value: GraphQlPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("query".into(), value.query.into()),
                (
                    "operation".into(),
                    value.operation.map(OutValue::from).into(),
                ),
                (
                    "params".into(),
                    value
                        .params
                        .map(|params| {
                            Value::Map(Map {
                                map: Rc::new(
                                    params
                                        .clone()
                                        .into_iter()
                                        .map(|(k, v)| {
                                            (
                                                // FIXME: We allow non-utf8 keys, but cel will only
                                                // represent utf8 or numeric keys... We probably
                                                // need to detect and base64 encode these or
                                                // something for cel eventually.
                                                String::from_utf8_lossy(k.as_slice())
                                                    .as_ref()
                                                    .into(),
                                                OutValue::from(v).into(),
                                            )
                                        })
                                        .collect(),
                                ),
                            })
                        })
                        .unwrap_or(Value::Null),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlRequestOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<Vec<u8>, serde_json::Value>>,
    pub duration: Duration,
}

impl From<GraphQlRequestOutput> for Value {
    fn from(value: GraphQlRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("query".into(), value.query.into()),
                (
                    "operation".into(),
                    value.operation.map(OutValue::from).into(),
                ),
                (
                    "params".into(),
                    value
                        .params
                        .map(|params| {
                            Value::Map(Map {
                                map: Rc::new(
                                    params
                                        .clone()
                                        .into_iter()
                                        .map(|(k, v)| {
                                            (
                                                // FIXME: We allow non-utf8 keys, but cel will only
                                                // represent utf8 or numeric keys... We probably
                                                // need to detect and base64 encode these or
                                                // something for cel eventually.
                                                String::from_utf8_lossy(k.as_slice())
                                                    .as_ref()
                                                    .into(),
                                                OutValue::from(v).into(),
                                            )
                                        })
                                        .collect(),
                                ),
                            })
                        })
                        .unwrap_or(Value::Null),
                ),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlResponse {
    pub data: OutValue,
    pub errors: OutValue,
    pub full: OutValue,
    // This is a hack - find a better way to respresent the raw output for GraphQL in a
    // transport-independant way that can be directly used in cel. Probably just make OutValue
    // implement Display and drop this field completely.
    pub json: serde_json::Value,
    pub duration: Duration,
}

impl From<GraphQlResponse> for Value {
    fn from(value: GraphQlResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("data".into(), value.data.clone().into()),
                ("errors".into(), value.errors.clone().into()),
                ("full".into(), value.full.clone().into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlError {
    pub kind: String,
    pub message: String,
}

impl From<GraphQlError> for Value {
    fn from(value: GraphQlError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
