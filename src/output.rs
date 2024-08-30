use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::Duration;
use indexmap::IndexMap;
use itertools::Itertools;
use url::Url;

use crate::{AddContentLength, Parallelism, ProtocolField, TlsVersion};

pub trait State<'a, O: Into<&'a str>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a str) -> Option<&IndexMap<crate::IterableKey, StepOutput>>;
    fn current(&self) -> &StepPlanOutputs;
    fn run_for(&self) -> &Option<RunForOutput>;
    fn run_while(&self) -> &Option<RunWhileOutput>;
    fn run_count(&self) -> &Option<RunCountOutput>;
    fn locals(&self) -> cel_interpreter::objects::Map;
    fn iter(&self) -> I;
    // Check for matching singed int indexes too.
}

#[derive(Debug)]
pub enum Output {
    GraphQl(GraphQlOutput),
    Http(HttpOutput),
    H1c(Http1Output),
    H1(Http1Output),
    H2c(Http2Output),
    H2(Http2Output),
    Http2Frames(Http2FramesOutput),
    //Http3(Http3Output),
    Tls(TlsOutput),
    Tcp(TcpOutput),
    TcpSegments(TcpSegmentsOutput),
}

#[derive(Debug, Clone)]
pub enum StepPlanOutput {
    GraphQl(GraphQlPlanOutput),
    Http(HttpPlanOutput),
    H1c(Http1PlanOutput),
    H1(Http1PlanOutput),
    H2c(Http2PlanOutput),
    H2(Http2PlanOutput),
    Http2Frames(Http2FramesPlanOutput),
    //Http3(Http3PlanOutput),
    Tls(TlsPlanOutput),
    Tcp(TcpPlanOutput),
    TcpSegments(TcpSegmentsPlanOutput),
}

#[derive(Debug, Clone, Default)]
pub struct StepPlanOutputs {
    pub graphql: Option<GraphQlPlanOutput>,
    pub http: Option<HttpPlanOutput>,
    pub h1c: Option<Http1PlanOutput>,
    pub h1: Option<Http1PlanOutput>,
    pub h2c: Option<Http2PlanOutput>,
    pub h2: Option<Http2PlanOutput>,
    pub http2_frames: Option<Http2FramesPlanOutput>,
    //pub http3: Option<Http3PlanOutput>,
    pub tls: Option<TlsPlanOutput>,
    pub tcp: Option<TcpPlanOutput>,
    pub tcp_segments: Option<TcpSegmentsPlanOutput>,
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
                    "h1c".into(),
                    HashMap::from([("plan", Value::from(value.h1c))]).into(),
                ),
                (
                    "h1".into(),
                    HashMap::from([("plan", Value::from(value.h1))]).into(),
                ),
                (
                    "h2c".into(),
                    HashMap::from([("plan", Value::from(value.h2c))]).into(),
                ),
                (
                    "h2".into(),
                    HashMap::from([("plan", Value::from(value.h2))]).into(),
                ),
                (
                    "http2_frames".into(),
                    HashMap::from([("plan", Value::from(value.http2_frames))]).into(),
                ),
                (
                    "tls".into(),
                    HashMap::from([("plan", Value::from(value.tls))]).into(),
                ),
                (
                    "tcp".into(),
                    HashMap::from([("plan", Value::from(value.tcp))]).into(),
                ),
                (
                    "tcp_segments".into(),
                    HashMap::from([("plan", Value::from(value.tcp_segments))]).into(),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct StepOutput {
    pub graphql: Option<GraphQlOutput>,
    pub http: Option<HttpOutput>,
    pub h1c: Option<Http1Output>,
    pub h1: Option<Http1Output>,
    pub h2c: Option<Http2Output>,
    pub h2: Option<Http2Output>,
    pub http2_frames: Option<Http2FramesOutput>,
    //pub http3: Option<Http3Output>,
    pub tls: Option<TlsOutput>,
    pub tcp: Option<TcpOutput>,
    pub tcp_segments: Option<TcpSegmentsOutput>,
}

impl StepOutput {
    pub fn http1(&self) -> Option<&Http1Output> {
        self.h1.as_ref().or_else(|| self.h1c.as_ref())
    }
    pub fn http2(&self) -> Option<&Http2Output> {
        self.h2.as_ref().or_else(|| self.h2c.as_ref())
    }
}

impl From<StepOutput> for Value {
    fn from(value: StepOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("graphql".into(), value.graphql.into()),
                ("http".into(), value.http.into()),
                ("h1c".into(), value.h1c.into()),
                ("h1".into(), value.h1.into()),
                ("h2c".into(), value.h2c.into()),
                ("h2".into(), value.h2.into()),
                ("http2_frames".into(), value.http2_frames.into()),
                //("http3".into(), value.http3.into()),
                ("tls".into(), value.tls.into()),
                ("tcp".into(), value.tcp.into()),
                ("tcp_segments".into(), value.tcp_segments.into()),
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
    pub errors: Vec<HttpError>,
    pub protocol: Option<String>,
    pub duration: Duration,
    pub pause: HttpPauseOutput,
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
                ("pause".into(), value.pause.into()),
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
    pub pause: HttpPauseOutput,
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
    pub open: PausePointsOutput,
    pub request_headers: PausePointsOutput,
    pub request_body: PausePointsOutput,
    pub response_headers: PausePointsOutput,
    pub response_body: PausePointsOutput,
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
            open: PausePointsOutput::with_planned_capacity(&planned.open),
            request_headers: PausePointsOutput::with_planned_capacity(&planned.request_headers),
            request_body: PausePointsOutput::with_planned_capacity(&planned.request_body),
            response_headers: PausePointsOutput::with_planned_capacity(&planned.response_headers),
            response_body: PausePointsOutput::with_planned_capacity(&planned.response_body),
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
    pub errors: Vec<Http1Error>,
    pub duration: Duration,
    pub pause: Http1PauseOutput,
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
    pub add_content_length: AddContentLength,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub pause: Http1PauseOutput,
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
    pub open: PausePointsOutput,
    pub request_headers: PausePointsOutput,
    pub request_body: PausePointsOutput,
    pub response_headers: PausePointsOutput,
    pub response_body: PausePointsOutput,
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
            open: PausePointsOutput::with_planned_capacity(&planned.open),
            request_headers: PausePointsOutput::with_planned_capacity(&planned.request_headers),
            request_body: PausePointsOutput::with_planned_capacity(&planned.request_body),
            response_headers: PausePointsOutput::with_planned_capacity(&planned.response_headers),
            response_body: PausePointsOutput::with_planned_capacity(&planned.response_body),
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
pub struct Http2Output {
    pub plan: Http2PlanOutput,
    pub request: Option<Http2RequestOutput>,
    pub response: Option<Http2Response>,
    pub errors: Vec<Http2Error>,
    pub duration: Duration,
    pub pause: Http2PauseOutput,
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
                ("pause".into(), value.pause.into()),
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
    pub pause: Http2PauseOutput,
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
pub struct Http2PauseOutput {
    pub open: PausePointsOutput,
    pub request_headers: PausePointsOutput,
    pub request_body: PausePointsOutput,
    pub response_headers: PausePointsOutput,
    pub response_body: PausePointsOutput,
}

impl From<Http2PauseOutput> for Value {
    fn from(value: Http2PauseOutput) -> Self {
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

impl WithPlannedCapacity for Http2PauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            open: PausePointsOutput::with_planned_capacity(&planned.open),
            request_headers: PausePointsOutput::with_planned_capacity(&planned.request_headers),
            request_body: PausePointsOutput::with_planned_capacity(&planned.request_body),
            response_headers: PausePointsOutput::with_planned_capacity(&planned.response_headers),
            response_body: PausePointsOutput::with_planned_capacity(&planned.response_body),
        }
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
#[derive(Debug, Clone)]
pub struct Http2FramesOutput {
    pub plan: Http2FramesPlanOutput,
    pub errors: Vec<Http2FramesError>,
    pub duration: Duration,
    pub pause: Http2FramesPauseOutput,
}

impl From<Http2FramesOutput> for Value {
    fn from(value: Http2FramesOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2FramesPlanOutput {
    pub host: String,
    pub port: u16,
    //pub preamble: Vec<u8>,
    pub pause: Http2FramesPauseOutput,
}

impl From<Http2FramesPlanOutput> for Value {
    fn from(value: Http2FramesPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.to_string().into()),
                ("port".into(), u64::from(value.port).into()),
                //("preamble".into(), value.preamble.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http2FramesPauseOutput {
    pub handshake: PausePointsOutput,
}

impl From<Http2FramesPauseOutput> for Value {
    fn from(value: Http2FramesPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([(
                "handshake".into(),
                value.handshake.into(),
            )])),
        })
    }
}

impl WithPlannedCapacity for Http2FramesPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: PausePointsOutput::with_planned_capacity(&planned.handshake),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Http2FramesError {
    pub kind: String,
    pub message: String,
}

impl From<Http2FramesError> for Value {
    fn from(value: Http2FramesError) -> Self {
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
    pub errors: Vec<GraphQlError>,
    pub duration: Duration,
    pub pause: GraphQlPauseOutput,
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
    pub pause: GraphQlPauseOutput,
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
    pub errors: Vec<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
    pub pause: TlsPauseOutput,
}

impl From<TlsOutput> for Value {
    fn from(value: TlsOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("errors".into(), value.errors.into()),
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
    pub handshake: PausePointsOutput,
    pub send_body: PausePointsOutput,
    pub receive_body: PausePointsOutput,
}

impl From<TlsPauseOutput> for Value {
    fn from(value: TlsPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("handshake".into(), value.handshake.into()),
                ("send_body".into(), value.send_body.into()),
                ("receive_body".into(), value.receive_body.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for TlsPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: PausePointsOutput::with_planned_capacity(&planned.handshake),
            send_body: PausePointsOutput::with_planned_capacity(&planned.send_body),
            receive_body: PausePointsOutput::with_planned_capacity(&planned.receive_body),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub body: Vec<u8>,
    pub pause: TlsPauseOutput,
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
    pub errors: Vec<TcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
    pub pause: TcpPauseOutput,
}

impl From<TcpOutput> for Value {
    fn from(value: TcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("request".into(), value.request.into()),
                ("response".into(), value.response.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpPauseOutput {
    pub handshake: PausePointsOutput,
    pub send_body: PausePointsOutput,
    pub receive_body: PausePointsOutput,
}

impl From<TcpPauseOutput> for Value {
    fn from(value: TcpPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("handshake".into(), value.handshake.into()),
                ("send_body".into(), value.send_body.into()),
                ("receive_body".into(), value.receive_body.into()),
            ])),
        })
    }
}

impl WithPlannedCapacity for TcpPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: PausePointsOutput::with_planned_capacity(&planned.handshake),
            send_body: PausePointsOutput::with_planned_capacity(&planned.send_body),
            receive_body: PausePointsOutput::with_planned_capacity(&planned.receive_body),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    pub pause: TcpPauseOutput,
}

impl From<TcpPlanOutput> for Value {
    fn from(value: TcpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::String(Arc::new(value.host))),
                ("port".into(), u64::from(value.port).into()),
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
                ("port".into(), u64::from(value.port).into()),
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

#[derive(Debug, Clone)]
pub struct TcpSegmentsOutput {
    pub plan: TcpSegmentsPlanOutput,
    pub dest_host: String,
    pub dest_port: u16,
    pub sent: Vec<TcpSegmentOutput>,
    pub src_host: String,
    pub src_port: u16,
    pub received: Vec<TcpSegmentOutput>,
    pub errors: Vec<TcpSegmentsError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
    pub pause: TcpSegmentsPauseOutput,
}

impl From<TcpSegmentsOutput> for Value {
    fn from(value: TcpSegmentsOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("dest_host".into(), value.dest_host.into()),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                ("src_host".into(), value.src_host.into()),
                ("src_port".into(), u64::from(value.src_port).into()),
                ("sent".into(), value.sent.into()),
                ("received".into(), value.received.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpSegmentsPauseOutput {
    pub handshake: PausePointsOutput,
}

impl From<TcpSegmentsPauseOutput> for Value {
    fn from(value: TcpSegmentsPauseOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([(
                "handshake".into(),
                value.handshake.into(),
            )])),
        })
    }
}

impl WithPlannedCapacity for TcpSegmentsPauseOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            handshake: PausePointsOutput::with_planned_capacity(&planned.handshake),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpSegmentPauseOutput {}

#[derive(Debug, Clone)]
pub struct TcpSegmentsPlanOutput {
    pub dest_host: String,
    pub dest_port: u16,
    pub src_host: Option<String>,
    pub src_port: Option<u16>,
    pub isn: u32,
    pub window: u16,
    pub segments: Vec<TcpSegmentOutput>,
    pub pause: TcpSegmentsPauseOutput,
}

impl From<TcpSegmentsPlanOutput> for Value {
    fn from(value: TcpSegmentsPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("dest_host".into(), Value::String(Arc::new(value.dest_host))),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                (
                    "src_host".into(),
                    value
                        .src_host
                        .map(|src_host| Value::String(Arc::new(src_host)))
                        .into(),
                ),
                (
                    "src_port".into(),
                    value.src_port.map(|src_port| u64::from(src_port)).into(),
                ),
                ("isn".into(), u64::from(value.isn).into()),
                ("window".into(), u64::from(value.window).into()),
                (
                    "segments".into(),
                    Value::List(Arc::new(
                        value.segments.into_iter().map(Value::from).collect(),
                    )),
                ),
                ("pause".into(), value.pause.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegmentOutput {
    pub source: u16,
    pub destination: u16,
    pub sequence_number: u32,
    pub acknowledgment: u32,
    pub data_offset: u8,
    pub reserved: u8,
    pub flags: u8,
    pub window: u16,
    pub checksum: Option<u16>,
    pub urgent_ptr: u16,
    pub options: Vec<TcpSegmentOptionOutput>,
    pub payload: Vec<u8>,
}

impl From<TcpSegmentOutput> for Value {
    fn from(value: TcpSegmentOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                (
                    "sequence_number".into(),
                    u64::from(value.sequence_number).into(),
                ),
                (
                    "payload".into(),
                    Value::Bytes(Arc::new(value.payload.clone())),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub enum TcpSegmentOptionOutput {
    Nop,
    Mss(u16),
    Wscale(u8),
    SackPermitted,
    Sack(Vec<u32>),
    Timestamps { tsval: u32, tsecr: u32 },
    Raw { kind: u8, value: Vec<u8> },
}

impl TcpSegmentOptionOutput {
    pub const KIND_KEY: &'static str = "kind";
    pub const VALUE_KEY: &'static str = "value";
    pub const TSVAL_KEY: &'static str = "tsval";
    pub const TSECR_KEY: &'static str = "tsecr";

    pub const NOP_KIND: &'static str = "nop";
    pub const TIMESTAMPS_KIND: &'static str = "timestamps";
    pub const MSS_KIND: &'static str = "mss";
    pub const WSCALE_KIND: &'static str = "wscale";
    pub const SACK_PERMITTED_KIND: &'static str = "sack_permitted";
    pub const SACK_KIND: &'static str = "sack";

    // the number of bytes required for the option on the wire. See
    // https://www.iana.org/assignments/tcp-parameters/tcp-parameters.xhtml
    pub fn size(&self) -> usize {
        match self {
            Self::Nop => 1,
            Self::Mss(_) => 4,
            Self::Wscale(_) => 3,
            Self::SackPermitted => 2,
            Self::Sack(vals) => vals
                .len()
                .checked_mul(4)
                .expect("tcp sack option size calculation should not overflow")
                .checked_add(2)
                .expect("tcp sack option size calculation should not overflow"),
            Self::Timestamps { .. } => 10,
            // Except for nop and end-of-options-list, options are a kind byte, a length byte, and
            // the value bytes.
            Self::Raw { value, .. } => value
                .len()
                .checked_add(2)
                .expect("tcp raw option size calculation should not overflow"),
        }
    }
}

impl From<TcpSegmentOptionOutput> for Value {
    fn from(value: TcpSegmentOptionOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(match value {
                TcpSegmentOptionOutput::Nop => {
                    HashMap::from([(TcpSegmentOptionOutput::KIND_KEY.into(), "nop".into())])
                }
                TcpSegmentOptionOutput::Timestamps { tsval, tsecr } => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "timestamps".into()),
                    (
                        TcpSegmentOptionOutput::TSVAL_KEY.into(),
                        u64::from(tsval).into(),
                    ),
                    (
                        TcpSegmentOptionOutput::TSECR_KEY.into(),
                        u64::from(tsecr).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Mss(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "mss".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        u64::from(val).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Wscale(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "wscale".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        u64::from(val).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::SackPermitted => HashMap::from([(
                    TcpSegmentOptionOutput::KIND_KEY.into(),
                    "sack_permitted".into(),
                )]),
                TcpSegmentOptionOutput::Sack(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "sack".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        val.into_iter()
                            .map(|x| Value::UInt(x.into()))
                            .collect_vec()
                            .into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Raw { kind, value } => HashMap::from([
                    (
                        TcpSegmentOptionOutput::KIND_KEY.into(),
                        Value::UInt(kind.into()),
                    ),
                    (TcpSegmentOptionOutput::VALUE_KEY.into(), value.into()),
                ]),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegmentsError {
    pub kind: String,
    pub message: String,
}

impl From<TcpSegmentsError> for Value {
    fn from(value: TcpSegmentsError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
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

#[derive(Debug, Clone, Default)]
pub struct PausePointsOutput {
    pub start: Vec<PauseValueOutput>,
    pub end: Vec<PauseValueOutput>,
}

impl WithPlannedCapacity for PausePointsOutput {
    fn with_planned_capacity(planned: &Self) -> Self {
        Self {
            start: Vec::with_capacity(planned.start.len()),
            end: Vec::with_capacity(planned.end.len()),
        }
    }
}

impl From<PausePointsOutput> for Value {
    fn from(value: PausePointsOutput) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                ("start".into(), value.start.into()),
                ("end".into(), value.end.into()),
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
                ("join".into(), value.join.into()),
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
    pub parallel: Parallelism,
    pub share: Option<ProtocolField>,
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
