use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::Duration;
use url::Url;

use crate::TLSVersion;

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&StepOutput>;
    fn iter(&self) -> I;
}

pub enum Output {
    GraphQL(GraphQLOutput),
    HTTP(HTTPOutput),
    HTTP1(HTTP1Output),
    HTTP2(HTTPOutput),
    HTTP3(HTTPOutput),
    TLS(TLSOutput),
    TCP(TCPOutput),
}

#[derive(Debug, Clone, Default)]
pub struct StepOutput {
    pub graphql: Option<GraphQLOutput>,
    pub http: Option<HTTPOutput>,
    pub http1: Option<HTTPOutput>,
    pub http2: Option<HTTPOutput>,
    pub http3: Option<HTTPOutput>,
    pub tls: Option<TLSOutput>,
    pub tcp: Option<TCPOutput>,
}

impl From<StepOutput> for Value {
    fn from(value: StepOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("graphql".into(), value.graphql.into());
        map.insert("http".into(), value.http.into());
        map.insert("http1".into(), value.http1.into());
        map.insert("http2".into(), value.http2.into());
        map.insert("http3".into(), value.http3.into());
        map.insert("tls".into(), value.tls.into());
        map.insert("tcp".into(), value.tcp.into());
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub enum ProtocolOutput {
    HTTP(HTTPOutput),
    HTTP11(HTTPOutput),
    HTTP2(HTTPOutput),
    HTTP3(HTTPOutput),
    TCP(TCPOutput),
    GraphQL(GraphQLOutput),
}

pub type OutputStack = Vec<ProtocolOutput>;

#[derive(Debug, Clone)]
pub struct HTTPOutput {
    pub request: HTTPRequestOutput,
    pub response: HTTPResponse,
    pub protocol: String,
}

impl From<HTTPOutput> for Value {
    fn from(value: HTTPOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("protocol".into(), value.protocol.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HTTPRequestOutput {
    pub url: Url,
    pub method: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

impl From<HTTPRequestOutput> for Value {
    fn from(value: HTTPRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.clone().into()),
                (
                    "headers".into(),
                    Value::List(Arc::new(value.headers.iter().map(kv_pair_to_map).collect())),
                ),
                ("body".into(), value.body.clone().into()),
                (
                    "pause".into(),
                    Value::List(Arc::new(value.pause.into_iter().map(Value::from).collect())),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HTTPResponse {
    pub protocol: String,
    pub status_code: u16,
    pub status_reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<HTTPResponse> for Value {
    fn from(value: HTTPResponse) -> Self {
        let mut map = HashMap::with_capacity(6);
        map.insert(
            "protocol".into(),
            Value::String(Arc::new(value.protocol.clone())),
        );
        map.insert("status_code".into(), Value::UInt(value.status_code.into()));
        map.insert(
            "status_reason".into(),
            Value::String(Arc::new(value.status_reason.clone())),
        );
        map.insert(
            "headers".into(),
            Value::List(Arc::new(value.headers.iter().map(kv_pair_to_map).collect())),
        );
        map.insert("body".into(), Value::Bytes(Arc::new(value.body.clone())));
        map.insert(
            "duration".into(),
            // Unwrap conversion since duration is bounded by program runtime.
            Value::Duration(chrono::Duration::from_std(value.duration).unwrap()),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct HTTP1Output {
    pub request: HTTP1RequestOutput,
    pub response: HTTP1Response,
}

impl From<HTTP1Output> for Value {
    fn from(value: HTTP1Output) -> Self {
        let mut map: HashMap<Key, Value> = HashMap::with_capacity(5);
        map.insert("url".into(), value.request.url.to_string().into());
        map.insert("method".into(), value.request.method.clone().into());
        map.insert(
            "headers".into(),
            Value::List(Arc::new(
                value.request.headers.iter().map(kv_pair_to_map).collect(),
            )),
        );
        map.insert("body".into(), value.request.body.clone().into());
        map.insert("response".into(), value.response.into());
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct HTTP1RequestOutput {
    pub url: Url,
    pub method: Option<String>,
    pub version_string: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

#[derive(Debug, Clone)]
pub struct HTTP1Response {
    pub protocol: String,
    pub status_code: u16,
    pub status_reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<HTTP1Response> for Value {
    fn from(value: HTTP1Response) -> Self {
        let mut map = HashMap::with_capacity(6);
        map.insert(
            "protocol".into(),
            Value::String(Arc::new(value.protocol.clone())),
        );
        map.insert("status_code".into(), Value::UInt(value.status_code.into()));
        map.insert(
            "status_reason".into(),
            Value::String(Arc::new(value.status_reason.clone())),
        );
        map.insert(
            "headers".into(),
            Value::List(Arc::new(value.headers.iter().map(kv_pair_to_map).collect())),
        );
        map.insert("body".into(), Value::Bytes(Arc::new(value.body.clone())));
        map.insert(
            "duration".into(),
            // Unwrap conversion since duration is bounded by program runtime.
            Value::Duration(chrono::Duration::from_std(value.duration).unwrap()),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQLOutput {
    pub request: GraphQLRequestOutput,
    pub response: GraphQLResponse,
}

impl From<GraphQLOutput> for Value {
    fn from(value: GraphQLOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQLRequestOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<String>,
    pub use_query_string: bool,
    pub params: HashMap<String, Option<String>>,
    pub pause: Vec<PauseOutput>,
}

impl From<GraphQLRequestOutput> for Value {
    fn from(value: GraphQLRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("query".into(), value.query.clone().into()),
                ("operation".into(), value.operation.clone().into()),
                (
                    "params".into(),
                    Value::Map(Map {
                        map: Rc::new(
                            value
                                .params
                                .clone()
                                .into_iter()
                                .map(|(k, v)| (k.into(), v.into()))
                                .collect(),
                        ),
                    }),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQLResponse {
    pub data: OutValue,
    pub errors: OutValue,
    pub full: OutValue,
    pub duration: Duration,
    pub json: serde_json::Value,
}

impl From<GraphQLResponse> for Value {
    fn from(value: GraphQLResponse) -> Self {
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
pub struct TLSOutput {
    pub request: TLSRequestOutput,
    pub response: TLSResponse,
    pub version: TLSVersion,
}

impl From<TLSOutput> for Value {
    fn from(value: TLSOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TLSRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

impl From<TLSRequestOutput> for Value {
    fn from(value: TLSRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.into()),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), value.body.into()),
                (
                    "pause".into(),
                    Value::List(Arc::new(value.pause.into_iter().map(Value::from).collect())),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TLSResponse {
    pub body: Vec<u8>,
    pub duration: Duration,
}

impl From<TLSResponse> for Value {
    fn from(value: TLSResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), value.body.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TCPOutput {
    pub request: TCPRequestOutput,
    pub response: TCPResponse,
}

impl From<TCPOutput> for Value {
    fn from(value: TCPOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TCPRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}
impl From<TCPRequestOutput> for Value {
    fn from(value: TCPRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::Bytes(Arc::new(value.body.clone()))),
                ("port".into(), (value.port as u64).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body.clone()))),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TCPResponse {
    pub body: Vec<u8>,
    pub duration: Duration,
}

impl From<TCPResponse> for Value {
    fn from(value: TCPResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), Value::Bytes(Arc::new(value.body.clone()))),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutValue {
    List(Vec<OutValue>),
    Map(OutMap),

    Function(Arc<String>, Option<Box<OutValue>>),

    // Atoms
    Int(i64),
    UInt(u64),
    Float(f64),
    String(Arc<String>),
    Bytes(Arc<Vec<u8>>),
    Bool(bool),
    Duration(chrono::Duration),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    Null,
}

impl From<Value> for OutValue {
    fn from(item: Value) -> Self {
        match item {
            Value::List(rc_list) => OutValue::List(
                Arc::try_unwrap(rc_list)
                    .unwrap_or_else(|rc| (*rc).clone())
                    .into_iter()
                    .map(OutValue::from)
                    .collect(),
            ),
            Value::Map(map) => OutValue::Map(map.into()),
            Value::Function(rc_string, opt_box_value) => OutValue::Function(
                rc_string,
                opt_box_value.map(|box_value| Box::new((*box_value).into())),
            ),
            Value::Int(value) => OutValue::Int(value),
            Value::UInt(value) => OutValue::UInt(value),
            Value::Float(value) => OutValue::Float(value),
            Value::String(arc_string) => OutValue::String(arc_string),
            Value::Bytes(arc_bytes) => OutValue::Bytes(arc_bytes),
            Value::Bool(value) => OutValue::Bool(value),
            Value::Duration(value) => OutValue::Duration(value),
            Value::Timestamp(value) => OutValue::Timestamp(value),
            Value::Null => OutValue::Null,
        }
    }
}

impl From<OutValue> for Value {
    fn from(item: OutValue) -> Self {
        match item {
            OutValue::List(arc_list) => {
                Value::List(Arc::new(arc_list.into_iter().map(Value::from).collect()))
            }
            OutValue::Map(out_map) => Value::Map(out_map.into()),
            OutValue::Function(arc_string, opt_box_value) => Value::Function(
                arc_string,
                opt_box_value.map(|box_value| Box::new((*box_value).into())),
            ),
            OutValue::Int(value) => Value::Int(value),
            OutValue::UInt(value) => Value::UInt(value),
            OutValue::Float(value) => Value::Float(value),
            OutValue::String(arc_string) => Value::String(arc_string),
            OutValue::Bytes(arc_bytes) => Value::Bytes(arc_bytes),
            OutValue::Bool(value) => Value::Bool(value),
            OutValue::Duration(value) => Value::Duration(value),
            OutValue::Timestamp(value) => Value::Timestamp(value),
            OutValue::Null => Value::Null,
        }
    }
}

impl From<serde_json::Value> for OutValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => OutValue::Null,
            serde_json::Value::Bool(x) => OutValue::Bool(x),
            serde_json::Value::Number(x) => OutValue::Float(x.as_f64().unwrap()),
            serde_json::Value::String(x) => OutValue::String(Arc::new(x)),
            serde_json::Value::Array(x) => {
                OutValue::List(x.into_iter().map(|x| OutValue::from(x)).collect())
            }
            serde_json::Value::Object(x) => OutValue::Map(OutMap {
                map: x
                    .into_iter()
                    .map(|(k, v)| (Key::String(Arc::new(k)), OutValue::from(v)))
                    .collect(),
            }),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OutMap {
    pub map: HashMap<Key, OutValue>,
}

impl From<Map> for OutMap {
    fn from(item: Map) -> Self {
        OutMap {
            map: Rc::try_unwrap(item.map)
                .unwrap_or_else(|rc| (*rc).clone())
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<OutMap> for Map {
    fn from(item: OutMap) -> Self {
        Map {
            map: Rc::new(item.map.into_iter().map(|(k, v)| (k, v.into())).collect()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PauseOutput {
    pub after: String,
    pub duration: Duration,
}

impl From<PauseOutput> for Value {
    fn from(value: PauseOutput) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                ("after".into(), value.after.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

fn kv_pair_to_map(pair: &(String, String)) -> Value {
    let mut map = HashMap::with_capacity(2);
    let pair = pair.clone();
    map.insert("key".into(), pair.0.into());
    map.insert("value".into(), pair.1.into());
    Value::Map(Map { map: Rc::new(map) })
}
