use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::Duration;
use url::Url;

use crate::TlsVersion;

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&StepOutput>;
    fn iter(&self) -> I;
}

#[derive(Debug)]
pub enum Output {
    GraphQl(GraphQlOutput),
    Http(HttpOutput),
    Http1(Http1Output),
    //Http2(Http2Output),
    //Http3(Http3Output),
    Tls(TlsOutput),
    Tcp(TcpOutput),
}

#[derive(Debug)]
pub enum RequestOutput {
    GraphQl(GraphQlRequestOutput),
    Http(HttpRequestOutput),
    Http1(Http1RequestOutput),
    //Http2(Http2RequestOutput),
    //Http3(Http3RequestOutput),
    Tls(TlsRequestOutput),
    Tcp(TcpRequestOutput),
}

#[derive(Debug, Clone, Default)]
pub struct StepOutput {
    pub graphql: Option<GraphQlOutput>,
    pub http: Option<HttpOutput>,
    pub http1: Option<Http1Output>,
    //pub http2: Option<Http2Output>,
    //pub http3: Option<Http3Output>,
    pub tls: Option<TlsOutput>,
    pub tcp: Option<TcpOutput>,
}

impl From<StepOutput> for Value {
    fn from(value: StepOutput) -> Self {
        let mut map = HashMap::with_capacity(4);
        map.insert("graphql".into(), value.graphql.into());
        map.insert("http".into(), value.http.into());
        map.insert("http1".into(), value.http1.into());
        //map.insert("http2".into(), value.http2.into());
        //map.insert("http3".into(), value.http3.into());
        map.insert("tls".into(), value.tls.into());
        map.insert("tcp".into(), value.tcp.into());
        Value::Map(Map { map: Rc::new(map) })
    }
}

#[derive(Debug, Clone)]
pub enum ProtocolOutput {
    Http(HttpOutput),
    Http11(HttpOutput),
    Http2(HttpOutput),
    Http3(HttpOutput),
    Tcp(TcpOutput),
    GraphQl(GraphQlOutput),
}

pub type OutputStack = Vec<ProtocolOutput>;

#[derive(Debug, Clone)]
pub struct HttpOutput {
    pub request: HttpRequestOutput,
    pub response: HttpResponse,
    pub protocol: String,
}

impl From<HttpOutput> for Value {
    fn from(value: HttpOutput) -> Self {
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
pub struct HttpRequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

impl From<HttpRequestOutput> for Value {
    fn from(value: HttpRequestOutput) -> Self {
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
pub struct HttpResponse {
    pub protocol: Vec<u8>,
    pub status_code: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<HttpResponse> for Value {
    fn from(value: HttpResponse) -> Self {
        let mut map = HashMap::with_capacity(6);
        map.insert(
            "protocol".into(),
            Value::Bytes(Arc::new(value.protocol.clone())),
        );
        map.insert("status_code".into(), Value::UInt(value.status_code.into()));
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
pub struct Http1Output {
    pub request: Http1RequestOutput,
    pub response: Http1Response,
}

impl From<Http1Output> for Value {
    fn from(value: Http1Output) -> Self {
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
pub struct Http1RequestOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub version_string: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

#[derive(Debug, Clone)]
pub struct Http1Response {
    pub protocol: Vec<u8>,
    pub status_code: u16,
    pub status_reason: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub duration: std::time::Duration,
}

impl From<Http1Response> for Value {
    fn from(value: Http1Response) -> Self {
        let mut map = HashMap::with_capacity(6);
        map.insert(
            "protocol".into(),
            Value::Bytes(Arc::new(value.protocol.clone())),
        );
        map.insert("status_code".into(), Value::UInt(value.status_code.into()));
        map.insert(
            "status_reason".into(),
            Value::Bytes(Arc::new(value.status_reason.clone())),
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
pub struct GraphQlOutput {
    pub request: GraphQlRequestOutput,
    pub response: GraphQlResponse,
}

impl From<GraphQlOutput> for Value {
    fn from(value: GraphQlOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlRequestOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<String>,
    pub params: Option<HashMap<Vec<u8>, Option<serde_json::Value>>>,
    pub pause: Vec<PauseOutput>,
}

impl From<GraphQlRequestOutput> for Value {
    fn from(value: GraphQlRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("query".into(), value.query.clone().into()),
                ("operation".into(), value.operation.clone().into()),
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
                                                v.map(OutValue::from).into(),
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
pub struct GraphQlResponse {
    pub data: OutValue,
    pub errors: OutValue,
    pub full: OutValue,
    pub duration: Duration,
    pub json: serde_json::Value,
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
pub struct TlsOutput {
    pub request: TlsRequestOutput,
    pub response: TlsResponse,
    pub version: TlsVersion,
}

impl From<TlsOutput> for Value {
    fn from(value: TlsOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}

impl From<TlsRequestOutput> for Value {
    fn from(value: TlsRequestOutput) -> Self {
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
pub struct TlsResponse {
    pub body: Vec<u8>,
    pub duration: Duration,
}

impl From<TlsResponse> for Value {
    fn from(value: TlsResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), value.body.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpOutput {
    pub request: TcpRequestOutput,
    pub response: TcpResponse,
}

impl From<TcpOutput> for Value {
    fn from(value: TcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: Vec<PauseOutput>,
}
impl From<TcpRequestOutput> for Value {
    fn from(value: TcpRequestOutput) -> Self {
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
pub struct TcpResponse {
    pub body: Vec<u8>,
    pub duration: Duration,
}

impl From<TcpResponse> for Value {
    fn from(value: TcpResponse) -> Self {
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

fn kv_pair_to_map(pair: &(Vec<u8>, Vec<u8>)) -> Value {
    let mut map = HashMap::with_capacity(2);
    let pair = pair.clone();
    map.insert("key".into(), pair.0.into());
    map.insert("value".into(), pair.1.into());
    Value::Map(Map { map: Rc::new(map) })
}
