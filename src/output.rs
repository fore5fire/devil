use std::{collections::HashMap, rc::Rc, time::Duration};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};

use crate::TCPRequest;

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&StepOutput>;
    fn iter(&self) -> I;
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
            map.insert("http".into(), out.into());
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
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub response: HTTPResponse,
    pub pause: Vec<PauseOutput>,
}

impl From<&HTTPOutput> for Value {
    fn from(value: &HTTPOutput) -> Self {
        let mut map: HashMap<Key, Value> = HashMap::with_capacity(5);
        map.insert("url".into(), value.url.clone().into());
        map.insert("method".into(), value.method.clone().into());
        map.insert(
            "headers".into(),
            Value::List(Rc::new(value.headers.iter().map(kv_pair_to_map).collect())),
        );
        map.insert("body".into(), value.body.clone().into());
        map.insert("response".into(), (&value.response).into());
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
    pub url: String,
    pub query: String,
    pub operation: Option<String>,
    pub use_query_string: bool,
    pub params: HashMap<String, String>,
    pub response: GraphQLResponse,
}

impl From<&GraphQLOutput> for Value {
    fn from(value: &GraphQLOutput) -> Self {
        let mut map: HashMap<Key, Value> = HashMap::with_capacity(6);
        map.insert("url".into(), value.url.clone().into());
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
    pub body: Vec<u8>,
    pub response: TLSResponse,
}

impl From<&TLSOutput> for Value {
    fn from(value: &TLSOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("host".into(), value.host.clone().into());
        map.insert("port".into(), (value.port as u32).into());
        map.insert("body".into(), value.body.clone().into());
        map.insert("response".into(), (&value.response).into());
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

impl TCPRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<TCPOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TCPOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            pause: self
                .pause
                .into_iter()
                .map(|p| {
                    Ok(PauseOutput {
                        after: p.after.evaluate(state)?,
                        duration: p.duration.evaluate(state)?,
                    })
                })
                .collect::<crate::Result<_>>()?,
            response: None,
        })
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

#[derive(Debug, Clone)]
pub struct OutValue(pub cel_interpreter::Value);

impl From<OutValue> for cel_interpreter::Value {
    fn from(value: OutValue) -> Self {
        value.0
    }
}

impl From<serde_json::Value> for OutValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => OutValue(cel_interpreter::Value::Null),
            serde_json::Value::Bool(x) => OutValue(cel_interpreter::Value::Bool(x)),
            serde_json::Value::Number(x) => {
                OutValue(cel_interpreter::Value::Float(x.as_f64().unwrap()))
            }
            serde_json::Value::String(x) => OutValue(cel_interpreter::Value::String(Rc::new(x))),
            serde_json::Value::Array(x) => OutValue(cel_interpreter::Value::List(Rc::new(
                x.into_iter().map(|x| OutValue::from(x).0).collect(),
            ))),
            serde_json::Value::Object(x) => {
                OutValue(cel_interpreter::Value::Map(cel_interpreter::objects::Map {
                    map: Rc::new(
                        x.into_iter()
                            .map(|(k, v)| (k.into(), OutValue::from(v).0))
                            .collect(),
                    ),
                }))
            }
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
