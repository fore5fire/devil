use std::fmt::Display;
use std::io;
use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use bitmask_enum::bitmask;
use byteorder::{ByteOrder, NetworkEndian};
use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::{Duration, TimeDelta};
use indexmap::IndexMap;
use itertools::Itertools;
use tokio::io::{AsyncWrite, AsyncWriteExt};
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

#[derive(Debug, Clone)]
pub enum StepPlanOutput {
    GraphQl(GraphQlPlanOutput),
    Http(HttpPlanOutput),
    H1c(Http1PlanOutput),
    H1(Http1PlanOutput),
    H2c(Http2PlanOutput),
    RawH2c(RawHttp2PlanOutput),
    H2(Http2PlanOutput),
    RawH2(RawHttp2PlanOutput),
    //Http3(Http3PlanOutput),
    Tls(TlsPlanOutput),
    Tcp(TcpPlanOutput),
    RawTcp(RawTcpPlanOutput),
}

#[derive(Debug, Clone, Default)]
pub struct StepPlanOutputs {
    pub graphql: Option<GraphQlPlanOutput>,
    pub http: Option<HttpPlanOutput>,
    pub h1c: Option<Http1PlanOutput>,
    pub h1: Option<Http1PlanOutput>,
    pub h2c: Option<Http2PlanOutput>,
    pub raw_h2c: Option<RawHttp2PlanOutput>,
    pub h2: Option<Http2PlanOutput>,
    pub raw_h2: Option<RawHttp2PlanOutput>,
    //pub http3: Option<Http3PlanOutput>,
    pub tls: Option<TlsPlanOutput>,
    pub tcp: Option<TcpPlanOutput>,
    pub raw_tcp: Option<RawTcpPlanOutput>,
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
                    "raw_h2c".into(),
                    HashMap::from([("plan", Value::from(value.raw_h2c))]).into(),
                ),
                (
                    "h2".into(),
                    HashMap::from([("plan", Value::from(value.h2))]).into(),
                ),
                (
                    "raw_h2".into(),
                    HashMap::from([("plan", Value::from(value.raw_h2))]).into(),
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
                    "raw_tcp".into(),
                    HashMap::from([("plan", Value::from(value.raw_tcp))]).into(),
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
    pub raw_h2c: Option<RawHttp2Output>,
    pub h2: Option<Http2Output>,
    pub raw_h2: Option<RawHttp2Output>,
    //pub http3: Option<Http3Output>,
    pub tls: Option<TlsOutput>,
    pub tcp: Option<TcpOutput>,
    pub raw_tcp: Option<RawTcpOutput>,
}

impl StepOutput {
    pub fn http1(&self) -> Option<&Http1Output> {
        self.h1.as_ref().or_else(|| self.h1c.as_ref())
    }
    pub fn http2(&self) -> Option<&Http2Output> {
        self.h2.as_ref().or_else(|| self.h2c.as_ref())
    }
    pub fn raw_http2(&self) -> Option<&RawHttp2Output> {
        self.raw_h2.as_ref().or_else(|| self.raw_h2c.as_ref())
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
                ("raw_h2c".into(), value.raw_h2c.into()),
                ("h2".into(), value.h2.into()),
                ("raw_h2".into(), value.raw_h2.into()),
                //("http3".into(), value.http3.into()),
                ("tls".into(), value.tls.into()),
                ("tcp".into(), value.tcp.into()),
                ("raw_tcp".into(), value.raw_tcp.into()),
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
                        value.headers.into_iter().map(kv_pair_to_map).collect(),
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
                        value.headers.into_iter().map(kv_pair_to_map).collect(),
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
pub enum Http2FrameOutput {
    Data(Http2DataFrameOutput),
    Headers(Http2HeadersFrameOutput),
    Priority(Http2PriorityFrameOutput),
    RstStream(Http2RstStreamFrameOutput),
    Settings(Http2SettingsFrameOutput),
    PushPromise(Http2PushPromiseFrameOutput),
    Ping(Http2PingFrameOutput),
    Goaway(Http2GoawayFrameOutput),
    WindowUpdate(Http2WindowUpdateFrameOutput),
    Continuation(Http2ContinuationFrameOutput),
    Generic(Http2GenericFrameOutput),
}

impl Http2FrameOutput {
    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: W) -> io::Result<()> {
        match self {
            Self::Data(frame) => frame.write(writer).await,
            Self::Headers(frame) => frame.write(writer).await,
            Self::Priority(frame) => frame.write(writer).await,
            Self::RstStream(frame) => frame.write(writer).await,
            Self::Settings(frame) => frame.write(writer).await,
            Self::PushPromise(frame) => frame.write(writer).await,
            Self::Ping(frame) => frame.write(writer).await,
            Self::Goaway(frame) => frame.write(writer).await,
            Self::WindowUpdate(frame) => frame.write(writer).await,
            Self::Continuation(frame) => frame.write(writer).await,
            Self::Generic(frame) => frame.write(writer).await,
        }
    }

    pub fn flags(&self) -> Http2FrameFlag {
        match self {
            Self::Data(frame) => frame.flags,
            Self::Headers(frame) => frame.flags,
            Self::Priority(frame) => frame.flags,
            Self::RstStream(frame) => frame.flags,
            Self::Settings(frame) => frame.flags,
            Self::PushPromise(frame) => frame.flags,
            Self::Ping(frame) => frame.flags,
            Self::Goaway(frame) => frame.flags,
            Self::WindowUpdate(frame) => frame.flags,
            Self::Continuation(frame) => frame.flags,
            Self::Generic(frame) => frame.flags,
        }
    }

    pub fn r(&self) -> bool {
        match self {
            Self::Data(frame) => frame.r,
            Self::Headers(frame) => frame.r,
            Self::Priority(frame) => frame.r,
            Self::RstStream(frame) => frame.r,
            Self::Settings(frame) => frame.r,
            Self::PushPromise(frame) => frame.r,
            Self::Ping(frame) => frame.r,
            Self::Goaway(frame) => frame.r,
            Self::WindowUpdate(frame) => frame.r,
            Self::Continuation(frame) => frame.r,
            Self::Generic(frame) => frame.r,
        }
    }
    pub fn stream_id(&self) -> u32 {
        match self {
            Self::Data(frame) => frame.stream_id,
            Self::Headers(frame) => frame.stream_id,
            Self::Priority(frame) => frame.stream_id,
            Self::RstStream(frame) => frame.stream_id,
            Self::Settings(frame) => frame.stream_id,
            Self::PushPromise(frame) => frame.stream_id,
            Self::Ping(frame) => frame.stream_id,
            Self::Goaway(frame) => frame.stream_id,
            Self::WindowUpdate(frame) => frame.stream_id,
            Self::Continuation(frame) => frame.stream_id,
            Self::Generic(frame) => frame.stream_id,
        }
    }

    pub fn r#type(&self) -> Http2FrameType {
        match self {
            Self::Data(_) => Http2FrameType::Data,
            Self::Headers(_) => Http2FrameType::Headers,
            Self::Priority(_) => Http2FrameType::Priority,
            Self::RstStream(_) => Http2FrameType::RstStream,
            Self::Settings(_) => Http2FrameType::Settings,
            Self::PushPromise(_) => Http2FrameType::PushPromise,
            Self::Ping(_) => Http2FrameType::Ping,
            Self::Goaway(_) => Http2FrameType::Goaway,
            Self::WindowUpdate(_) => Http2FrameType::WindowUpdate,
            Self::Continuation(_) => Http2FrameType::Continuation,
            Self::Generic(frame) => frame.r#type,
        }
    }

    pub fn new(
        kind: Http2FrameType,
        flags: Http2FrameFlag,
        r: bool,
        stream_id: u32,
        mut payload: &[u8],
    ) -> Self {
        match kind {
            Http2FrameType::Data if payload.len() >= Http2FrameFlag::Padded.min_bytes(flags) => {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload = &payload[1..];
                }
                Self::Data(Http2DataFrameOutput {
                    flags: flags.into(),
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    r,
                    stream_id,
                    data: payload[..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
                })
            }
            Http2FrameType::Headers
                if payload.len()
                    >= Http2FrameFlag::Padded.min_bytes(flags)
                        + Http2FrameFlag::Priority.min_bytes(flags) =>
            {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload = &payload[1..];
                }
                let priority = flags.contains(Http2FrameFlag::Priority).then(|| {
                    Http2HeadersFramePriorityOutput {
                        e: payload[0] & 1 << 7 != 0,
                        stream_dependency: NetworkEndian::read_u32(payload) & !(1 << 31),
                        weight: payload[4],
                    }
                });
                if priority.is_some() {
                    payload = &payload[5..];
                }
                Self::Headers(Http2HeadersFrameOutput {
                    flags: flags.into(),
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                    r,
                    stream_id,
                    priority,
                    header_block_fragment: payload[..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
                })
            }
            Http2FrameType::Priority if payload.len() == 5 => {
                Self::Priority(Http2PriorityFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    e: payload[0] & 1 << 7 != 0,
                    stream_dependency: NetworkEndian::read_u32(payload) & !(1 << 31),
                    weight: payload[4],
                })
            }
            Http2FrameType::RstStream if payload.len() == 4 => {
                Self::RstStream(Http2RstStreamFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    error_code: NetworkEndian::read_u32(payload),
                })
            }
            Http2FrameType::Settings if payload.len() % 6 == 0 => {
                Self::Settings(Http2SettingsFrameOutput {
                    flags: flags.into(),
                    ack: flags.contains(Http2FrameFlag::Ack),
                    r,
                    stream_id,
                    parameters: payload
                        .chunks_exact(6)
                        .map(|chunk| Http2SettingsParameterOutput {
                            id: Http2SettingsParameterId::new(NetworkEndian::read_u16(chunk)),
                            value: NetworkEndian::read_u32(&chunk[2..]),
                        })
                        .collect(),
                })
            }
            Http2FrameType::PushPromise
                if payload.len() >= Http2FrameFlag::Padded.min_bytes(flags) + 4 =>
            {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload = &payload[1..];
                }
                Self::PushPromise(Http2PushPromiseFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    promised_r: payload[0] & 1 << 7 != 0,
                    promised_stream_id: NetworkEndian::read_u32(payload) & !(1 << 31),
                    header_block_fragment: payload[4..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
                })
            }
            Http2FrameType::Ping => Self::Ping(Http2PingFrameOutput {
                flags: flags.into(),
                ack: flags.contains(Http2FrameFlag::Ack),
                r,
                stream_id,
                data: payload.to_vec(),
            }),
            Http2FrameType::Goaway if payload.len() >= 8 => Self::Goaway(Http2GoawayFrameOutput {
                flags: flags.into(),
                r,
                stream_id,
                last_r: payload[0] & 1 << 7 != 0,
                last_stream_id: NetworkEndian::read_u32(payload) & !(1 << 31),
                error_code: NetworkEndian::read_u32(&payload[4..]),
                debug_data: payload[8..].to_vec(),
            }),
            Http2FrameType::WindowUpdate if payload.len() == 4 => {
                Self::WindowUpdate(Http2WindowUpdateFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    window_r: payload[0] & 1 << 7 != 0,
                    window_size_increment: NetworkEndian::read_u32(payload) & !(1 << 31),
                })
            }
            Http2FrameType::Continuation => Self::Continuation(Http2ContinuationFrameOutput {
                flags: flags.into(),
                end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                r,
                stream_id,
                header_block_fragment: payload.to_vec(),
            }),
            _ => Self::Generic(Http2GenericFrameOutput {
                r#type: kind,
                flags: flags.into(),
                r,
                stream_id,
                payload: payload.to_vec(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Http2FrameType {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    Goaway,
    WindowUpdate,
    Continuation,
    Generic(u8),
}

impl Http2FrameType {
    #[inline]
    pub fn new(value: u8) -> Self {
        match value {
            0 => Self::Data,
            1 => Self::Headers,
            2 => Self::Priority,
            3 => Self::RstStream,
            4 => Self::Settings,
            5 => Self::PushPromise,
            6 => Self::Ping,
            7 => Self::Goaway,
            8 => Self::WindowUpdate,
            9 => Self::Continuation,
            val => Self::Generic(val),
        }
    }

    #[inline]
    pub fn value(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Headers => 1,
            Self::Priority => 2,
            Self::RstStream => 3,
            Self::Settings => 4,
            Self::PushPromise => 5,
            Self::Ping => 6,
            Self::Goaway => 7,
            Self::WindowUpdate => 8,
            Self::Continuation => 9,
            Self::Generic(val) => val,
        }
    }
}

impl Display for Http2FrameType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reparse the Http2FrameType to correctly print generic with a recognized type.
        match Http2FrameType::new(self.value()) {
            Self::Data => write!(f, "DATA"),
            Self::Headers => write!(f, "HEADERS"),
            Self::Priority => write!(f, "PRIORITY"),
            Self::RstStream => write!(f, "RST_STREAM"),
            Self::Settings => write!(f, "SETTINGS"),
            Self::PushPromise => write!(f, "PUSH_PROMISE"),
            Self::Ping => write!(f, "PING"),
            Self::Goaway => write!(f, "GOAWAY"),
            Self::WindowUpdate => write!(f, "WINDOW_UPDATE"),
            Self::Continuation => write!(f, "CONTINUATION"),
            Self::Generic(t) => write!(f, "{t:#04x}"),
        }
    }
}

#[bitmask(u8)]
pub enum Http2FrameFlag {
    Ack = 0x01,
    EndStream = 0x01,
    EndHeaders = 0x4,
    Padded = 0x8,
    Priority = 0x20,
}

impl Http2FrameFlag {
    #[inline]
    pub fn min_bytes(self, flags: Self) -> usize {
        if !flags.contains(self) {
            return 0;
        }
        match self {
            Self::Ack => 0,
            #[expect(unreachable_patterns)]
            Self::EndStream => 0,
            Self::EndHeaders => 0,
            Self::Padded => 1,
            Self::Priority => 5,
            _ => 0,
        }
    }
    #[inline]
    pub fn cond(self, cond: bool) -> Self {
        if cond {
            self
        } else {
            Self::none()
        }
    }
}

impl From<Http2FrameFlag> for Value {
    #[inline]
    fn from(value: Http2FrameFlag) -> Self {
        u64::from(u8::from(value)).into()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Http2SettingsParameterId {
    HeaderTableSize,
    EnablePush,
    MaxConcurrentStreams,
    InitialWindowSize,
    MaxFrameSize,
    MaxHeaderListSize,
    Generic(u16),
}

impl Http2SettingsParameterId {
    #[inline]
    pub fn new(value: u16) -> Self {
        match value {
            1 => Self::HeaderTableSize,
            2 => Self::EnablePush,
            3 => Self::MaxConcurrentStreams,
            4 => Self::InitialWindowSize,
            5 => Self::MaxFrameSize,
            6 => Self::MaxHeaderListSize,
            val => Self::Generic(val),
        }
    }

    #[inline]
    pub fn value(self) -> u16 {
        match self {
            Self::HeaderTableSize => 1,
            Self::EnablePush => 2,
            Self::MaxConcurrentStreams => 3,
            Self::InitialWindowSize => 4,
            Self::MaxFrameSize => 5,
            Self::MaxHeaderListSize => 6,
            Self::Generic(val) => val,
        }
    }
}

impl Display for Http2SettingsParameterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reparse the Http2FrameType to correctly print generic with a recognized type.
        match Self::new(self.value()) {
            Self::HeaderTableSize => write!(f, "HEADER_TABLE_SIZE"),
            Self::EnablePush => write!(f, "ENABLE_PUSH"),
            Self::MaxConcurrentStreams => write!(f, "MAX_CONCURRENT_STREAMS"),
            Self::InitialWindowSize => write!(f, "INITIAL_WINDOW_SIZE"),
            Self::MaxFrameSize => write!(f, "MAX_FRAME_SIZE"),
            Self::MaxHeaderListSize => write!(f, "MAX_HEADER_LIST_SIZE"),
            Self::Generic(t) => write!(f, "{t:#06x}"),
        }
    }
}

impl From<Http2SettingsParameterId> for Value {
    #[inline]
    fn from(value: Http2SettingsParameterId) -> Self {
        u64::from(value.value()).into()
    }
}

impl From<Http2FrameOutput> for Value {
    fn from(value: Http2FrameOutput) -> Self {
        match value {
            Http2FrameOutput::Data(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("data".into(), x.into())])),
            }),
            Http2FrameOutput::Headers(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("headers".into(), x.into())])),
            }),
            Http2FrameOutput::Priority(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("priority".into(), x.into())])),
            }),
            Http2FrameOutput::RstStream(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("rst_stream".into(), x.into())])),
            }),
            Http2FrameOutput::Settings(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("settings".into(), x.into())])),
            }),
            Http2FrameOutput::PushPromise(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("push_promise".into(), x.into())])),
            }),
            Http2FrameOutput::Ping(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("ping".into(), x.into())])),
            }),
            Http2FrameOutput::Goaway(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("goaway".into(), x.into())])),
            }),
            Http2FrameOutput::WindowUpdate(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("window_update".into(), x.into())])),
            }),
            Http2FrameOutput::Continuation(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("continuation".into(), x.into())])),
            }),
            Http2FrameOutput::Generic(x) => Value::Map(Map {
                map: Rc::new(HashMap::from([("generic".into(), x.into())])),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Http2DataFrameOutput {
    pub flags: Http2FrameFlag,
    pub end_stream: bool,
    pub r: bool,
    pub stream_id: u32,
    pub data: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2DataFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Data
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        let mut flags = Http2FrameFlag::none();
        if self.end_stream {
            flags |= Http2FrameFlag::EndStream;
        }
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndStream.cond(self.end_stream)
                | Http2FrameFlag::Padded.cond(self.padding.is_some()),
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + self.data.len(),
        )
        .await?;
        // Padding Length
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        writer.write_all(&self.data).await?;
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

impl From<Http2DataFrameOutput> for Value {
    fn from(value: Http2DataFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("end_stream".into(), value.end_stream.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("data".into(), value.data.into()),
                ("padding".into(), value.padding.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2HeadersFrameOutput {
    pub flags: Http2FrameFlag,
    pub end_stream: bool,
    pub end_headers: bool,
    pub r: bool,
    pub stream_id: u32,
    pub priority: Option<Http2HeadersFramePriorityOutput>,
    pub header_block_fragment: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2HeadersFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Headers
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndStream.cond(self.end_stream)
                | Http2FrameFlag::EndHeaders.cond(self.end_headers)
                | Http2FrameFlag::Padded.cond(self.padding.is_some())
                | Http2FrameFlag::Priority.cond(self.priority.is_some()),
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + self.priority.as_ref().map(|_| 5).unwrap_or_default()
                + self.header_block_fragment.len(),
        )
        .await?;
        // Write the padding length.
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        if let Some(priority) = &self.priority {
            writer
                .write_u32(to_u31(priority.e, priority.stream_dependency))
                .await?;
            writer.write_u8(priority.weight).await?;
        }
        writer.write_all(&self.header_block_fragment).await?;
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

impl From<Http2HeadersFrameOutput> for Value {
    fn from(value: Http2HeadersFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("end_stream".into(), value.end_stream.into()),
                ("end_headers".into(), value.end_headers.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("priority".into(), value.priority.into()),
                (
                    "header_block_fragment".into(),
                    value.header_block_fragment.into(),
                ),
                ("padding".into(), value.padding.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2HeadersFramePriorityOutput {
    pub e: bool,
    pub stream_dependency: u32,
    pub weight: u8,
}

impl From<Http2HeadersFramePriorityOutput> for Value {
    fn from(value: Http2HeadersFramePriorityOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("e".into(), value.e.into()),
                (
                    "stream_dependency".into(),
                    u64::from(value.stream_dependency).into(),
                ),
                ("weight".into(), u64::from(value.weight).into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PriorityFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub e: bool,
    pub stream_dependency: u32,
    pub weight: u8,
}

impl Http2PriorityFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Priority
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            5,
        )
        .await?;
        let mut buf = [0; 5];
        NetworkEndian::write_u32(&mut buf, to_u31(self.e, self.stream_dependency));
        buf[4] = self.weight;
        writer.write_all(&buf).await
    }
}

impl From<Http2PriorityFrameOutput> for Value {
    fn from(value: Http2PriorityFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("e".into(), value.e.into()),
                (
                    "stream_dependency".into(),
                    u64::from(value.stream_dependency).into(),
                ),
                ("weight".into(), u64::from(value.weight).into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2RstStreamFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub error_code: u32,
}

impl Http2RstStreamFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::RstStream
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            4,
        )
        .await?;
        writer.write_u32(self.error_code).await
    }
}

impl From<Http2RstStreamFrameOutput> for Value {
    fn from(value: Http2RstStreamFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("error_code".into(), u64::from(value.error_code).into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2SettingsFrameOutput {
    pub flags: Http2FrameFlag,
    pub ack: bool,
    pub r: bool,
    pub stream_id: u32,
    pub parameters: Vec<Http2SettingsParameterOutput>,
}

impl Http2SettingsFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Settings
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            6 * self.parameters.len(),
        )
        .await?;
        let mut buf = [0; 6];
        for param in &self.parameters {
            NetworkEndian::write_u16(&mut buf, param.id.value());
            NetworkEndian::write_u32(&mut buf, param.value);
            writer.write_all(&buf).await?;
        }
        Ok(())
    }
}

impl From<Http2SettingsFrameOutput> for Value {
    fn from(value: Http2SettingsFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("ack".into(), value.ack.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                (
                    "parameters".into(),
                    value
                        .parameters
                        .into_iter()
                        .map(|p| p.into())
                        .collect::<Vec<Http2SettingsParameterOutput>>()
                        .into(),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2SettingsParameterOutput {
    pub id: Http2SettingsParameterId,
    pub value: u32,
}

impl From<Http2SettingsParameterOutput> for Value {
    fn from(value: Http2SettingsParameterOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("id".into(), value.id.into()),
                ("value".into(), u64::from(value.value).into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PushPromiseFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub promised_r: bool,
    pub promised_stream_id: u32,
    pub header_block_fragment: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2PushPromiseFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::PushPromise
    }
    #[inline]
    pub fn end_headers(&self) -> bool {
        self.flags.contains(Http2FrameFlag::EndHeaders)
    }
    #[inline]
    fn padded(&self) -> bool {
        self.flags.contains(Http2FrameFlag::Padded)
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            self.flags,
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + 4
                + self.header_block_fragment.len(),
        )
        .await?;
        // Write the padding length.
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        // Write the promised stream ID.
        writer
            .write_u32(to_u31(self.promised_r, self.promised_stream_id))
            .await?;
        // Write the header block fragment.
        writer.write_all(&self.header_block_fragment).await?;
        // Write the padding.
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

impl From<Http2PushPromiseFrameOutput> for Value {
    fn from(value: Http2PushPromiseFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("end_headers".into(), value.end_headers().into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("promised_r".into(), value.promised_r.into()),
                (
                    "promised_stream_id".into(),
                    u64::from(value.promised_stream_id).into(),
                ),
                (
                    "header_block_fragment".into(),
                    value.header_block_fragment.into(),
                ),
                ("padding".into(), value.padding.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PingFrameOutput {
    pub flags: Http2FrameFlag,
    pub ack: bool,
    pub r: bool,
    pub stream_id: u32,
    pub data: Vec<u8>,
}

impl Http2PingFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Ping
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::Ack.cond(self.ack),
            to_u31(self.r, self.stream_id),
            self.data.len(),
        )
        .await?;
        writer.write_all(&self.data).await
    }
}

impl From<Http2PingFrameOutput> for Value {
    fn from(value: Http2PingFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("ack".into(), value.ack.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("data".into(), value.data.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2GoawayFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub last_r: bool,
    pub last_stream_id: u32,
    pub error_code: u32,
    pub debug_data: Vec<u8>,
}

impl Http2GoawayFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Goaway
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            8,
        )
        .await?;
        let mut buf = [0; 8];
        NetworkEndian::write_u32(&mut buf, to_u31(self.last_r, self.last_stream_id));
        NetworkEndian::write_u32(&mut buf, self.error_code);
        writer.write_all(&buf).await?;
        writer.write_all(&self.debug_data).await
    }
}

impl From<Http2GoawayFrameOutput> for Value {
    fn from(value: Http2GoawayFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("last_r".into(), value.last_r.into()),
                (
                    "last_stream_id".into(),
                    u64::from(value.last_stream_id).into(),
                ),
                ("error_code".into(), u64::from(value.error_code).into()),
                ("debug_data".into(), value.debug_data.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2WindowUpdateFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub window_r: bool,
    pub window_size_increment: u32,
}

impl Http2WindowUpdateFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::WindowUpdate
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            4,
        )
        .await?;
        writer
            .write_u32(to_u31(self.window_r, self.window_size_increment))
            .await
    }
}

impl From<Http2WindowUpdateFrameOutput> for Value {
    fn from(value: Http2WindowUpdateFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("window_r".into(), value.window_r.into()),
                (
                    "window_size_increment".into(),
                    u64::from(value.window_size_increment).into(),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2ContinuationFrameOutput {
    pub flags: Http2FrameFlag,
    pub end_headers: bool,
    pub r: bool,
    pub stream_id: u32,
    pub header_block_fragment: Vec<u8>,
}

impl Http2ContinuationFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Continuation
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndHeaders.cond(self.end_headers),
            to_u31(self.r, self.stream_id),
            self.header_block_fragment.len(),
        )
        .await?;
        writer.write_all(&self.header_block_fragment).await
    }
}

impl From<Http2ContinuationFrameOutput> for Value {
    fn from(value: Http2ContinuationFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("flags".into(), value.flags.into()),
                ("end_headers".into(), value.end_headers.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                (
                    "header_block_fragment".into(),
                    value.header_block_fragment.into(),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2GenericFrameOutput {
    pub r#type: Http2FrameType,
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

impl Http2GenericFrameOutput {
    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        write_frame_header(
            &mut writer,
            self.r#type,
            self.flags,
            to_u31(self.r, self.stream_id),
            self.payload.len(),
        )
        .await?;
        writer.write_all(&self.payload).await?;
        Ok(())
    }
}

impl From<Http2GenericFrameOutput> for Value {
    fn from(value: Http2GenericFrameOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("type".into(), u64::from(value.r#type.value()).into()),
                ("flags".into(), value.flags.into()),
                ("r".into(), value.r.into()),
                ("stream_id".into(), u64::from(value.stream_id).into()),
                ("payload".into(), value.payload.into()),
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
pub struct RawHttp2Output {
    pub plan: RawHttp2PlanOutput,
    pub sent: Vec<Http2FrameOutput>,
    pub received: Vec<Http2FrameOutput>,
    pub errors: Vec<RawHttp2Error>,
    pub duration: Duration,
}

impl From<RawHttp2Output> for Value {
    fn from(value: RawHttp2Output) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RawHttp2PlanOutput {
    pub host: String,
    pub port: u16,
    pub preamble: Option<Vec<u8>>,
    pub frames: Vec<Http2FrameOutput>,
}

impl From<RawHttp2PlanOutput> for Value {
    fn from(value: RawHttp2PlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.to_string().into()),
                ("port".into(), u64::from(value.port).into()),
                //("preamble".into(), value.preamble.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RawHttp2Error {
    pub kind: String,
    pub message: String,
}

impl From<RawHttp2Error> for Value {
    fn from(value: RawHttp2Error) -> Self {
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

#[derive(Debug, Clone)]
pub struct TlsOutput {
    pub plan: TlsPlanOutput,
    pub request: Option<TlsRequestOutput>,
    pub response: Option<TlsResponse>,
    pub errors: Vec<TlsError>,
    pub version: Option<TlsVersion>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
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
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsPlanOutput {
    pub host: String,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub body: Vec<u8>,
}

impl From<TlsPlanOutput> for Value {
    fn from(value: TlsPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), value.host.into()),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), value.body.into()),
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
    pub sent: Option<TcpSentOutput>,
    pub received: Option<TcpReceivedOutput>,
    //pub close: TcpCloseOutput,
    pub errors: Vec<TcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

impl From<TcpOutput> for Value {
    fn from(value: TcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("sent".into(), value.sent.into()),
                //("close".into(), value.close.into()),
                ("received".into(), value.received.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
            ])),
        })
    }
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpCloseOutput {
//    pub timed_out: bool,
//    pub recv_max_reached: bool,
//    pub pattern_match: Option<Vec<u8>>,
//}

#[derive(Debug, Clone)]
pub struct TcpPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    //pub close: TcpPlanCloseOutput,
}

impl From<TcpPlanOutput> for Value {
    fn from(value: TcpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::String(Arc::new(value.host))),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                //("close".into(), value.close.into()),
            ])),
        })
    }
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpPlanCloseOutput {
//    pub min_duration: Option<Duration>,
//    pub read_pattern: Option<Regex>,
//    pub read_pattern_window: Option<u64>,
//    pub read_length: Option<u64>,
//}
//
//impl From<TcpPlanCloseOutput> for Value {
//    fn from(value: TcpPlanCloseOutput) -> Self {
//        Value::Map(Map {
//            map: Rc::new(HashMap::from([
//                ("min_duration".into(), value.min_duration.into()),
//                ("read_pattern".into(), value.read_pattern.into()),
//                (
//                    "read_pattern_window".into(),
//                    value.read_pattern_window.into(),
//                ),
//                ("read_length".into(), value.read_length.into()),
//            ])),
//        })
//    }
//}

#[derive(Debug, Clone)]
pub struct TcpSentOutput {
    pub dest_ip: String,
    pub dest_port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}
impl From<TcpSentOutput> for Value {
    fn from(value: TcpSentOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("dest_ip".into(), Value::String(Arc::new(value.dest_ip))),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpReceivedOutput {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TcpReceivedOutput> for Value {
    fn from(value: TcpReceivedOutput) -> Self {
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
pub struct RawTcpOutput {
    pub plan: RawTcpPlanOutput,
    pub dest_ip: String,
    pub dest_port: u16,
    pub sent: Vec<TcpSegmentOutput>,
    pub src_host: String,
    pub src_port: u16,
    pub received: Vec<TcpSegmentOutput>,
    pub errors: Vec<RawTcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

impl From<RawTcpOutput> for Value {
    fn from(value: RawTcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("dest_ip".into(), value.dest_ip.into()),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                ("src_host".into(), value.src_host.into()),
                ("src_port".into(), u64::from(value.src_port).into()),
                ("sent".into(), value.sent.into()),
                ("received".into(), value.received.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RawTcpPlanOutput {
    pub dest_host: String,
    pub dest_port: u16,
    pub src_host: Option<String>,
    pub src_port: Option<u16>,
    pub isn: u32,
    pub window: u16,
    pub segments: Vec<TcpSegmentOutput>,
}

impl From<RawTcpPlanOutput> for Value {
    fn from(value: RawTcpPlanOutput) -> Self {
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
    pub received: Option<TimeDelta>,
    pub sent: Option<TimeDelta>,
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
    Generic { kind: u8, value: Vec<u8> },
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
            Self::Generic { value, .. } => value
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
                TcpSegmentOptionOutput::Generic { kind, value } => HashMap::from([
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
pub struct RawTcpError {
    pub kind: String,
    pub message: String,
}

impl From<RawTcpError> for Value {
    fn from(value: RawTcpError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Regex {
    pub parsed: regex::bytes::Regex,
    raw: Arc<String>,
}

impl Regex {
    pub fn new<S: Into<Arc<String>>>(s: S) -> crate::Result<Self> {
        let s = s.into();
        Ok(Regex {
            parsed: regex::bytes::Regex::new(&s)?,
            raw: s.into(),
        })
    }
}

impl From<Regex> for Value {
    fn from(value: Regex) -> Self {
        Self::String(value.raw)
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
pub struct PauseValueOutput {
    pub location: LocationOutput,
    pub duration: Duration,
    pub offset_bytes: i64,
    pub r#await: Option<String>,
}

impl From<PauseValueOutput> for Value {
    fn from(value: PauseValueOutput) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                value.location.into(),
                ("duration".into(), value.duration.into()),
                ("offset_bytes".into(), value.offset_bytes.into()),
                ("await".into(), value.r#await.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub enum LocationOutput {
    Before(String),
    After(String),
}

impl LocationOutput {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Before(x) | Self::After(x) => x,
        }
    }
}

impl From<LocationOutput> for (cel_interpreter::objects::Key, cel_interpreter::Value) {
    fn from(value: LocationOutput) -> Self {
        match value {
            LocationOutput::Before(b) => ("before".into(), b.into()),
            LocationOutput::After(a) => ("after".into(), a.into()),
        }
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
pub struct RunPlanOutput {
    pub run_if: bool,
    pub run_while: Option<bool>,
    pub run_for: Option<Vec<RunForOutput>>,
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

fn to_u31(r: bool, val: u32) -> u32 {
    (r as u32) << 31 | !(1 << 31) | val
}

#[inline]
async fn write_frame_header<W, E, S>(
    mut writer: W,
    kind: Http2FrameType,
    flags: Http2FrameFlag,
    stream_id: u32,
    payload_len: S,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    E: std::fmt::Debug,
    S: TryInto<u32, Error = E>,
{
    let len = payload_len
        .try_into()
        .expect("frame payload should fit in u24");
    let mut buf = [0; 9];
    // Length
    NetworkEndian::write_u24(&mut buf, len);
    buf[3] = kind.value();
    buf[4] = flags.bits();
    // Stream ID
    NetworkEndian::write_u32(&mut buf[5..], stream_id);
    writer.write_all(&buf).await
}
