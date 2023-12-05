use std::sync::Arc;
use std::{collections::HashMap, rc::Rc, time::Duration};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use url::Url;

use crate::TLSVersion;

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&StepOutput>;
    fn iter(&self) -> I;
}

pub enum Output {
    GraphQL(GraphQLOutput),
    HTTP(HTTPOutput),
    HTTP1(HTTPOutput),
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

impl From<&StepOutput> for Value {
    fn from(value: &StepOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        if let Some(out) = &value.graphql {
            map.insert("graphql".into(), out.into());
        }
        if let Some(out) = &value.http {
            map.insert("http".into(), out.into());
        }
        if let Some(out) = &value.http1 {
            map.insert("http1".into(), out.into());
        }
        if let Some(out) = &value.http2 {
            map.insert("http2".into(), out.into());
        }
        if let Some(out) = &value.http3 {
            map.insert("http3".into(), out.into());
        }
        if let Some(out) = &value.tls {
            map.insert("tls".into(), out.into());
        }
        if let Some(out) = &value.tcp {
            map.insert("tcp".into(), out.into());
        }
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
    pub url: Url,
    pub method: String,
    pub protocol: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub response: Option<HTTPResponse>,
    pub pause: Vec<PauseOutput>,
}

impl From<&HTTPOutput> for Value {
    fn from(value: &HTTPOutput) -> Self {
        let mut map: HashMap<Key, Value> = HashMap::with_capacity(5);
        map.insert("url".into(), value.url.to_string().into());
        map.insert("method".into(), value.method.clone().into());
        map.insert(
            "headers".into(),
            Value::List(Rc::new(value.headers.iter().map(kv_pair_to_map).collect())),
        );
        map.insert("body".into(), value.body.clone().into());
        map.insert(
            "response".into(),
            value
                .response
                .as_ref()
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        Value::Map(Map { map: Rc::new(map) })
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

impl From<&HTTPResponse> for Value {
    fn from(value: &HTTPResponse) -> Self {
        let mut map = HashMap::with_capacity(6);
        map.insert(
            "protocol".into(),
            Value::String(Rc::new(value.protocol.clone())),
        );
        map.insert("status_code".into(), Value::UInt(value.status_code.into()));
        map.insert(
            "status_reason".into(),
            Value::String(Rc::new(value.status_reason.clone())),
        );
        map.insert(
            "headers".into(),
            Value::List(Rc::new(value.headers.iter().map(kv_pair_to_map).collect())),
        );
        map.insert("body".into(), Value::Bytes(Rc::new(value.body.clone())));
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
    pub url: Url,
    pub query: String,
    pub operation: Option<String>,
    pub use_query_string: bool,
    pub params: HashMap<String, String>,
    pub response: GraphQLResponse,
}

impl From<&GraphQLOutput> for Value {
    fn from(value: &GraphQLOutput) -> Self {
        let mut map: HashMap<Key, Value> = HashMap::with_capacity(6);
        map.insert("url".into(), value.url.to_string().into());
        map.insert("query".into(), value.query.clone().into());
        map.insert(
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
        );
        map.insert("operation".into(), value.operation.clone().into());
        map.insert("response".into(), (&value.response).into());
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQLResponse {
    pub data: OutValue,
    pub errors: OutValue,
    pub full: OutValue,
    pub duration: std::time::Duration,
    pub json: serde_json::Value,
}

impl From<&GraphQLResponse> for Value {
    fn from(value: &GraphQLResponse) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("data".into(), value.data.clone().into());
        map.insert("errors".into(), value.errors.clone().into());
        map.insert("full".into(), value.full.clone().into());
        map.insert(
            "duration".into(),
            // Unwrap conversion since duration is bounded by program runtime.
            Value::Duration(chrono::Duration::from_std(value.duration).unwrap()),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct TLSOutput {
    pub host: String,
    pub port: u16,
    pub version: Option<TLSVersion>,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
    pub response: Option<TLSResponse>,
}

impl From<&TLSOutput> for Value {
    fn from(value: &TLSOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("host".into(), value.host.clone().into());
        map.insert("port".into(), (value.port as u32).into());
        map.insert(
            "version".into(),
            value
                .version
                .as_ref()
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        map.insert("body".into(), value.body.clone().into());
        map.insert(
            "response".into(),
            value
                .response
                .as_ref()
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct TLSResponse {
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<&TLSResponse> for Value {
    fn from(value: &TLSResponse) -> Self {
        let mut map = HashMap::with_capacity(2);
        map.insert("body".into(), Value::Bytes(Rc::new(value.body.clone())));
        map.insert(
            "duration".into(),
            // Unwrap conversion since duration is bounded by program runtime.
            Value::Duration(chrono::Duration::from_std(value.duration).unwrap()),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct TCPOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
    pub response: Option<TCPResponse>,
}

impl From<&TCPOutput> for Value {
    fn from(value: &TCPOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("host".into(), Value::Bytes(Rc::new(value.body.clone())));
        map.insert("port".into(), (value.port as u32).into());
        map.insert("body".into(), Value::Bytes(Rc::new(value.body.clone())));
        value
            .response
            .as_ref()
            .map(|resp| map.insert("response".into(), resp.into()));
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct TCPResponse {
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<&TCPResponse> for Value {
    fn from(value: &TCPResponse) -> Self {
        let mut map = HashMap::with_capacity(2);
        map.insert("body".into(), Value::Bytes(Rc::new(value.body.clone())));
        map.insert(
            "duration".into(),
            // Unwrap conversion since duration is bounded by program runtime.
            Value::Duration(chrono::Duration::from_std(value.duration).unwrap()),
        );
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutValue {
    List(Arc<Vec<OutValue>>),
    Map(OutMap),

    Function(Arc<String>, Option<Box<OutValue>>),

    // Atoms
    Int(i32),
    UInt(u32),
    Float(f64),
    String(Arc<String>),
    Bytes(Arc<Vec<u8>>),
    Bool(bool),
    Duration(chrono::Duration),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OutMap {
    pub map: Arc<HashMap<OutKey, OutValue>>,
}

#[derive(Debug, Eq, PartialEq, Hash, Ord, Clone, PartialOrd)]
pub enum OutKey {
    Int(i32),
    Uint(u32),
    Bool(bool),
    String(Arc<String>),
}

impl From<Value> for OutValue {
    fn from(item: Value) -> Self {
        match item {
            Value::List(rc_list) => OutValue::List(Arc::new(
                Rc::try_unwrap(rc_list)
                    .unwrap_or_else(|rc| (*rc).clone())
                    .into_iter()
                    .map(OutValue::from)
                    .collect(),
            )),
            Value::Map(map) => OutValue::Map(map.into()),
            Value::Function(rc_string, opt_box_value) => OutValue::Function(
                Arc::new(Rc::try_unwrap(rc_string).unwrap_or_else(|rc| (*rc).clone())),
                opt_box_value.map(|box_value| Box::new((*box_value).into())),
            ),
            Value::Int(value) => OutValue::Int(value),
            Value::UInt(value) => OutValue::UInt(value),
            Value::Float(value) => OutValue::Float(value),
            Value::String(rc_string) => OutValue::String(Arc::new(
                Rc::try_unwrap(rc_string).unwrap_or_else(|rc| (*rc).clone()),
            )),
            Value::Bytes(rc_bytes) => OutValue::Bytes(Arc::new(
                Rc::try_unwrap(rc_bytes).unwrap_or_else(|rc| (*rc).clone()),
            )),
            Value::Bool(value) => OutValue::Bool(value),
            Value::Duration(value) => OutValue::Duration(value),
            Value::Timestamp(value) => OutValue::Timestamp(value),
            Value::Null => OutValue::Null,
        }
    }
}

impl From<Map> for OutMap {
    fn from(item: Map) -> Self {
        OutMap {
            map: Arc::new(
                Rc::try_unwrap(item.map)
                    .unwrap_or_else(|rc| (*rc).clone())
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
        }
    }
}

impl From<Key> for OutKey {
    fn from(item: Key) -> Self {
        match item {
            Key::Int(value) => OutKey::Int(value),
            Key::Uint(value) => OutKey::Uint(value),
            Key::Bool(value) => OutKey::Bool(value),
            Key::String(rc_string) => OutKey::String(Arc::new(
                Rc::try_unwrap(rc_string).unwrap_or_else(|rc| (*rc).clone()),
            )),
        }
    }
}

impl From<OutValue> for Value {
    fn from(item: OutValue) -> Self {
        match item {
            OutValue::List(arc_list) => Value::List(Rc::new(
                Arc::try_unwrap(arc_list)
                    .unwrap_or_else(|arc| (*arc).clone())
                    .into_iter()
                    .map(Value::from)
                    .collect(),
            )),
            OutValue::Map(out_map) => Value::Map(out_map.into()),
            OutValue::Function(arc_string, opt_box_value) => Value::Function(
                Rc::new(Arc::try_unwrap(arc_string).unwrap_or_else(|arc| (*arc).clone())),
                opt_box_value.map(|box_value| Box::new((*box_value).into())),
            ),
            OutValue::Int(value) => Value::Int(value),
            OutValue::UInt(value) => Value::UInt(value),
            OutValue::Float(value) => Value::Float(value),
            OutValue::String(arc_string) => Value::String(Rc::new(
                Arc::try_unwrap(arc_string).unwrap_or_else(|arc| (*arc).clone()),
            )),
            OutValue::Bytes(arc_bytes) => Value::Bytes(Rc::new(
                Arc::try_unwrap(arc_bytes).unwrap_or_else(|arc| (*arc).clone()),
            )),
            OutValue::Bool(value) => Value::Bool(value),
            OutValue::Duration(value) => Value::Duration(value),
            OutValue::Timestamp(value) => Value::Timestamp(value),
            OutValue::Null => Value::Null,
        }
    }
}

impl From<OutMap> for Map {
    fn from(item: OutMap) -> Self {
        Map {
            map: Rc::new(
                Arc::try_unwrap(item.map)
                    .unwrap_or_else(|arc| (*arc).clone())
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
        }
    }
}

impl From<OutKey> for Key {
    fn from(item: OutKey) -> Self {
        match item {
            OutKey::Int(value) => Key::Int(value),
            OutKey::Uint(value) => Key::Uint(value),
            OutKey::Bool(value) => Key::Bool(value),
            OutKey::String(arc_string) => Key::String(Rc::new(
                Arc::try_unwrap(arc_string).unwrap_or_else(|arc| (*arc).clone()),
            )),
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
                OutValue::List(Arc::new(x.into_iter().map(|x| OutValue::from(x)).collect()))
            }
            serde_json::Value::Object(x) => OutValue::Map(OutMap {
                map: Arc::new(
                    x.into_iter()
                        .map(|(k, v)| (OutKey::String(Arc::new(k)), OutValue::from(v)))
                        .collect(),
                ),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PauseOutput {
    pub after: String,
    pub duration: Duration,
}

fn kv_pair_to_map(pair: &(String, String)) -> Value {
    let mut map = HashMap::with_capacity(2);
    let pair = pair.clone();
    map.insert("key".into(), pair.0.into());
    map.insert("value".into(), pair.1.into());
    Value::Map(Map { map: Rc::new(map) })
}
