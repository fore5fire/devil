use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&StepOutput>;
    fn iter(&self) -> I;
}

#[derive(Debug, Clone)]
pub enum StepOutput {
    HTTP(HTTPOutput),
    HTTP11(HTTPOutput),
    HTTP2(HTTPOutput),
    HTTP3(HTTPOutput),
    TCP(TCPOutput),
    GraphQL(GraphQLOutput),
}

impl From<&StepOutput> for Value {
    fn from(value: &StepOutput) -> Self {
        let mut map = HashMap::with_capacity(1);
        match value {
            StepOutput::HTTP(http) => map.insert("http".into(), http.into()),
            StepOutput::HTTP11(http) => map.insert("http".into(), http.into()),
            StepOutput::HTTP2(http) => map.insert("http".into(), http.into()),
            StepOutput::HTTP3(http) => map.insert("http".into(), http.into()),
            StepOutput::TCP(tcp) => map.insert("tcp".into(), tcp.into()),
            StepOutput::GraphQL(out) => map.insert("graphql".into(), out.into()),
        };
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct HTTPOutput {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub response: HTTPResponse,
    pub raw_request: Vec<u8>,
    pub raw_response: Vec<u8>,
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
}

impl From<&HTTPResponse> for Value {
    fn from(value: &HTTPResponse) -> Self {
        let mut map = HashMap::with_capacity(5);
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
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQLOutput {
    pub url: String,
    pub query: String,
    pub operation: Option<String>,
    pub params: HashMap<String, String>,
    pub http: HTTPOutput,
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
        map.insert("http".into(), Value::from(&value.http));
        map.insert("response".into(), (&value.response).into());
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
pub struct GraphQLResponse {
    pub data: OutValue,
    pub errors: OutValue,
    pub full: OutValue,
    pub formatted: serde_json::Value,
    pub raw: Vec<u8>,
}

impl From<&GraphQLResponse> for Value {
    fn from(value: &GraphQLResponse) -> Self {
        let mut map = HashMap::with_capacity(3);
        map.insert("data", value.data.clone());
        map.insert("errors", value.errors.clone());
        map.insert("full", value.full.clone());
        Value::from(map)
    }
}

#[derive(Debug, Clone)]
pub struct TCPOutput {
    pub response: TCPResponse,
}

impl From<&TCPOutput> for Value {
    fn from(value: &TCPOutput) -> Self {
        let mut map = HashMap::with_capacity(1);
        map.insert("response".into(), (&value.response).into());
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub struct TCPResponse {
    pub body: Vec<u8>,
}

impl From<&TCPResponse> for Value {
    fn from(value: &TCPResponse) -> Self {
        let mut map = HashMap::with_capacity(1);
        map.insert("body".into(), Value::Bytes(Rc::new(value.body.clone())));
        Value::Map(Map { map: Rc::new(map) })
    }
}

fn kv_pair_to_map(pair: &(String, String)) -> Value {
    let mut map = HashMap::with_capacity(2);
    let pair = pair.clone();
    map.insert("key".into(), pair.0.into());
    map.insert("value".into(), pair.1.into());
    Value::Map(Map { map: Rc::new(map) })
}
