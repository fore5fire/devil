use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::Duration;
use indexmap::IndexMap;
use url::Url;

use crate::TlsVersion;

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&IndexMap<crate::IterableKey, StepOutput>>;
    fn current(&self) -> &StepPlanOutputs;
    fn run_for(&self) -> &Option<RunForOutput>;
    fn run_while(&self) -> &Option<RunWhileOutput>;
    fn run_count(&self) -> &Option<RunCountOutput>;
    fn iter(&self) -> I;
    // Check for matching singed int indexes too.
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

#[derive(Debug, Clone)]
pub enum StepPlanOutput {
    GraphQl(GraphQlPlanOutput),
    Http(HttpPlanOutput),
    Http1(Http1PlanOutput),
    //Http2(Http2PlanOutput),
    //Http3(Http3PlanOutput),
    Tls(TlsPlanOutput),
    Tcp(TcpPlanOutput),
}

#[derive(Debug, Clone, Default)]
pub struct StepPlanOutputs {
    pub graphql: Option<GraphQlPlanOutput>,
    pub http: Option<HttpPlanOutput>,
    pub http1: Option<Http1PlanOutput>,
    //pub http2: Option<Http2PlanOutput>,
    //pub http3: Option<Http3PlanOutput>,
    pub tls: Option<TlsPlanOutput>,
    pub tcp: Option<TcpPlanOutput>,
}

impl From<StepPlanOutputs> for Value {
    fn from(value: StepPlanOutputs) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                (
                    "graphql".into(),
                    HashMap::from([("plan", Value::from(value.graphql))]).into(),
                ),
                (
                    "http".into(),
                    HashMap::from([("plan", Value::from(value.http))]).into(),
                ),
                (
                    "http1".into(),
                    HashMap::from([("plan", Value::from(value.http1))]).into(),
                ),
                (
                    "tls".into(),
                    HashMap::from([("plan", Value::from(value.tls))]).into(),
                ),
                (
                    "tcp".into(),
                    HashMap::from([("plan", Value::from(value.tcp))]).into(),
                ),
            ])),
        })
    }
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
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("graphql".into(), value.graphql.into()),
                ("http".into(), value.http.into()),
                ("http1".into(), value.http1.into()),
                //("http2".into(), value.http2.into()),
                //("http3".into(), value.http3.into()),
                ("tls".into(), value.tls.into()),
                ("tcp".into(), value.tcp.into()),
            ])),
        })
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
    pub plan: HttpPlanOutput,
    pub request: Option<HttpRequestOutput>,
    pub response: Option<HttpResponse>,
    pub error: Option<HttpError>,
    pub protocol: Option<String>,
    pub duration: Duration,
    pub pause: PauseOutput<HttpPauseOutput>,
}

impl From<HttpOutput> for Value {
    fn from(value: HttpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("protocol".into(), value.protocol.into()),
                ("error".into(), value.error.into()),
                ("duration".into(), value.duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpPlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub pause: PauseOutput<HttpPauseOutput>,
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
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpPauseOutput {
    pub open: Vec<PauseValueOutput>,
    pub request_headers: Vec<PauseValueOutput>,
    pub request_body: Vec<PauseValueOutput>,
    pub response_headers: Vec<PauseValueOutput>,
    pub response_body: Vec<PauseValueOutput>,
}

impl From<HttpPauseOutput> for Value {
    fn from(value: HttpPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("open".into(), value.open.into()),
                ("request_headers".into(), value.request_headers.into()),
                ("request_body".into(), value.request_body.into()),
                ("response_headers".into(), value.response_headers.into()),
                ("response_body".into(), value.response_body.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for HttpPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            open: Vec::with_capacity(planned.open.len()),
            request_headers: Vec::with_capacity(planned.request_headers.len()),
            request_body: Vec::with_capacity(planned.request_body.len()),
            response_headers: Vec::with_capacity(planned.response_headers.len()),
            response_body: Vec::with_capacity(planned.response_body.len()),
        }
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

#[derive(Debug, Clone)]
pub struct Http1Output {
    pub plan: Http1PlanOutput,
    pub request: Option<Http1RequestOutput>,
    pub response: Option<Http1Response>,
    pub error: Option<Http1Error>,
    pub duration: Duration,
    pub pause: PauseOutput<Http1PauseOutput>,
}

impl From<Http1Output> for Value {
    fn from(value: Http1Output) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("error".into(), value.error.into()),
                ("duration".into(), value.duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http1PlanOutput {
    pub url: Url,
    pub method: Option<Vec<u8>>,
    pub version_string: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub pause: PauseOutput<Http1PauseOutput>,
}

impl From<Http1PlanOutput> for Value {
    fn from(value: Http1PlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("url".into(), value.url.to_string().into()),
                ("method".into(), value.method.into()),
                ("version_string".into(), value.version_string.into()),
                (
                    "headers".into(),
                    Value::List(Arc::new(
                        value.headers.into_iter().map(kv_pair_to_map).collect(),
                    )),
                ),
                ("body".into(), value.body.clone().into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http1PauseOutput {
    pub open: Vec<PauseValueOutput>,
    pub request_headers: Vec<PauseValueOutput>,
    pub request_body: Vec<PauseValueOutput>,
    pub response_headers: Vec<PauseValueOutput>,
    pub response_body: Vec<PauseValueOutput>,
}

impl From<Http1PauseOutput> for Value {
    fn from(value: Http1PauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("open".into(), value.open.into()),
                ("request_headers".into(), value.request_headers.into()),
                ("request_body".into(), value.request_body.into()),
                ("response_headers".into(), value.response_headers.into()),
                ("response_body".into(), value.response_body.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for Http1PauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            open: Vec::with_capacity(planned.open.len()),
            request_headers: Vec::with_capacity(planned.request_headers.len()),
            request_body: Vec::with_capacity(planned.request_body.len()),
            response_headers: Vec::with_capacity(planned.response_headers.len()),
            response_body: Vec::with_capacity(planned.response_body.len()),
        }
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
pub struct Http1Response {
    pub protocol: Option<Vec<u8>>,
    pub status_code: Option<u16>,
    pub status_reason: Option<Vec<u8>>,
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

#[derive(Debug, Clone)]
pub struct GraphQlOutput {
    pub plan: GraphQlPlanOutput,
    pub request: Option<GraphQlRequestOutput>,
    pub response: Option<GraphQlResponse>,
    pub error: Option<GraphQlError>,
    pub duration: Duration,
    pub pause: PauseOutput<GraphQlPauseOutput>,
}

impl From<GraphQlOutput> for Value {
    fn from(value: GraphQlOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("error".into(), value.error.into()),
                ("duration".into(), value.duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GraphQlPauseOutput {}

impl From<GraphQlPauseOutput> for Value {
    fn from(value: GraphQlPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([])),
        })
    }
}

impl WithPlannedCapacity for GraphQlPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {}
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlPlanOutput {
    pub url: Url,
    pub query: String,
    pub operation: Option<serde_json::Value>,
    pub params: Option<HashMap<Vec<u8>, serde_json::Value>>,
    pub pause: PauseOutput<GraphQlPauseOutput>,
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
                ("pause".into(), value.pause.into()),
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

#[derive(Debug, Clone)]
pub struct TlsOutput {
    pub plan: TlsPlanOutput,
    pub request: Option<TlsRequestOutput>,
    pub response: Option<TlsResponse>,
    pub error: Option<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
    pub pause: PauseOutput<TlsPauseOutput>,
}

impl From<TlsOutput> for Value {
    fn from(value: TlsOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("error".into(), value.error.into()),
                ("version".into(), value.version.as_ref().into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TlsPauseOutput {
    pub handshake: Vec<PauseValueOutput>,
    pub first_read: Vec<PauseValueOutput>,
    pub first_write: Vec<PauseValueOutput>,
}

impl From<TlsPauseOutput> for Value {
    fn from(value: TlsPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("handshake".into(), value.handshake.into()),
                ("first_read".into(), value.first_read.into()),
                ("first_write".into(), value.first_write.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for TlsPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: Vec::with_capacity(planned.handshake.len()),
            first_read: Vec::with_capacity(planned.first_read.len()),
            first_write: Vec::with_capacity(planned.first_write.len()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: PauseOutput<TlsPauseOutput>,
}

impl From<TlsPlanOutput> for Value {
    fn from(value: TlsPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.into()),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), value.body.into()),
                ("pause".into(), value.pause.into()),
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

#[derive(Debug, Clone)]
pub struct TcpOutput {
    pub plan: TcpPlanOutput,
    pub request: Option<TcpRequestOutput>,
    pub response: Option<TcpResponse>,
    pub error: Option<TcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
    pub pause: PauseOutput<TcpPauseOutput>,
}

impl From<TcpOutput> for Value {
    fn from(value: TcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("error".into(), value.error.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpPauseOutput {
    pub handshake: Vec<PauseValueOutput>,
    pub first_read: Vec<PauseValueOutput>,
    pub first_write: Vec<PauseValueOutput>,
}

impl From<TcpPauseOutput> for Value {
    fn from(value: TcpPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("handshake".into(), value.handshake.into()),
                ("first_read".into(), value.first_read.into()),
                ("first_write".into(), value.first_write.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for TcpPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: Vec::with_capacity(planned.handshake.len()),
            first_read: Vec::with_capacity(planned.first_read.len()),
            first_write: Vec::with_capacity(planned.first_write.len()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: PauseOutput<TcpPauseOutput>,
}

impl From<TcpPlanOutput> for Value {
    fn from(value: TcpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::String(Arc::new(value.host))),
                ("port".into(), (value.port as u64).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpRequestOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}
impl From<TcpRequestOutput> for Value {
    fn from(value: TcpRequestOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::String(Arc::new(value.host))),
                ("port".into(), (value.port as u64).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpResponse {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TcpResponse> for Value {
    fn from(value: TcpResponse) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), Value::Bytes(Arc::new(value.body.clone()))),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpError {
    pub kind: String,
    pub message: String,
}

impl From<TcpError> for Value {
    fn from(value: TcpError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([("kind".into(), value.kind.into())])),
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
pub struct PauseOutput<T> {
    pub before: T,
    pub after: T,
}

impl<T: WithPlannedCapacity> WithPlannedCapacity for PauseOutput<T> {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            before: T::with_planned_capacity(&planned.before),
            after: T::with_planned_capacity(&planned.after),
        }
    }
}

impl<T: Default> Default for PauseOutput<T> {
    fn default() -> Self {
        PauseOutput {
            before: T::default(),
            after: T::default(),
        }
    }
}

impl<T: Into<Value>> From<PauseOutput<T>> for Value {
    fn from(value: PauseOutput<T>) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                ("before".into(), value.before.into()),
                ("after".into(), value.after.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PauseValueOutput {
    pub duration: Duration,
    pub offset_bytes: i64,
    pub join: Vec<String>,
}

impl From<PauseValueOutput> for Value {
    fn from(value: PauseValueOutput) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                ("duration".into(), value.duration.into()),
                ("offset_bytes".into(), value.offset_bytes.into()),
            ])),
        })
    }
}

fn kv_pair_to_map(pair: (Vec<u8>, Vec<u8>)) -> Value {
    //let pair = pair.clone();
    Value::Map(Map {
        map: Rc::new(HashMap::from([
            ("key".into(), pair.0.into()),
            ("value".into(), pair.1.into()),
        ])),
    })
}

pub trait WithPlannedCapacity {
    fn with_planned_capacity(planned: &Self) -> Self;
}

#[derive(Debug, Clone)]
pub struct RunOutput {
    pub run_if: bool,
    pub run_while: Option<bool>,
    pub run_for: Option<Vec<(crate::IterableKey, crate::PlanData)>>,
    pub count: u64,
    pub parallel: bool,
}

#[derive(Debug, Clone)]
pub struct RunForOutput {
    pub key: crate::IterableKey,
    pub value: cel_interpreter::Value,
}

impl From<RunForOutput> for Value {
    fn from(value: RunForOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("key".into(), value.key.into()),
                ("value".into(), value.value),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RunWhileOutput {
    pub index: u64,
}

impl From<RunWhileOutput> for Value {
    fn from(value: RunWhileOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([("index".into(), value.index.into())])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RunCountOutput {
    pub index: u64,
}

impl From<RunCountOutput> for Value {
    fn from(value: RunCountOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([("index".into(), value.index.into())])),
        })
    }
}
