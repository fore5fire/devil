use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{
    objects::{Key, Map},
    Value,
};
use chrono::Duration;
use indexmap::IndexMap;
use serde::Serialize;

use crate::{Parallelism, ProtocolField};

mod graphql;
mod http;
mod http1;
mod http2;
mod raw_http2;
mod raw_tcp;
mod tcp;
mod tls;

pub use graphql::*;
pub use http::*;
pub use http1::*;
pub use http2::*;
pub use raw_http2::*;
pub use raw_tcp::*;
pub use tcp::*;
pub use tls::*;

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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct SignalValueOutput {
    pub location: LocationOutput,
    pub target: String,
    pub kind: SignalKind,
}

impl From<SignalValueOutput> for Value {
    fn from(value: SignalValueOutput) -> Self {
        Self::Map(Map {
            map: Rc::new(HashMap::from([
                value.location.into(),
                ("target".into(), value.target.into()),
                ("kind".into(), value.kind.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum SignalKind {
    Register { priority: usize },
    Release,
    Unlock,
}

impl From<SignalKind> for (cel_interpreter::objects::Key, Value) {
    fn from(value: SignalKind) -> Self {
        match value {
            SignalKind::Register { priority } => (
                "register".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::from([(
                        "priority".into(),
                        u64::try_from(priority)
                            .expect("barrier count should fit into platform word size")
                            .into(),
                    )])),
                }),
            ),
            SignalKind::Release => (
                "release".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::new()),
                }),
            ),
            SignalKind::Unlock => (
                "unlock".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::new()),
                }),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SyncOutput {
    Barrier { count: usize },
    Mutex,
    PriorityMutex,
    Semaphore { permits: usize },
    PrioritySemaphore { permits: usize },
}

impl From<SyncOutput> for (cel_interpreter::objects::Key, Value) {
    fn from(value: SyncOutput) -> Self {
        match value {
            SyncOutput::Barrier { count } => (
                "barrier".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::from([(
                        "count".into(),
                        u64::try_from(count)
                            .expect("barrier count should fit into platform word size")
                            .into(),
                    )])),
                }),
            ),
            SyncOutput::Mutex => (
                "mutex".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::new()),
                }),
            ),
            SyncOutput::PriorityMutex => (
                "priority_mutex".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::new()),
                }),
            ),
            SyncOutput::Semaphore { permits } => (
                "semaphore".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::from([(
                        "permits".into(),
                        u64::try_from(permits)
                            .expect("barrier count should fit into platform word size")
                            .into(),
                    )])),
                }),
            ),
            SyncOutput::PrioritySemaphore { permits } => (
                "priority_semaphore".into(),
                Value::Map(Map {
                    map: Rc::new(HashMap::from([(
                        "permits".into(),
                        u64::try_from(permits)
                            .expect("barrier count should fit into platform word size")
                            .into(),
                    )])),
                }),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum LocationOutput {
    Before { id: String, offset_bytes: usize },
    After { id: String, offset_bytes: usize },
}

impl LocationOutput {
    #[inline]
    pub fn id(&self) -> &str {
        match self {
            Self::Before { id, .. } | Self::After { id, .. } => id,
        }
    }

    #[inline]
    pub fn into_id(self) -> String {
        match self {
            Self::Before { id, .. } | Self::After { id, .. } => id,
        }
    }

    #[inline]
    pub fn offset_bytes(&self) -> usize {
        match self {
            Self::Before { offset_bytes, .. } | Self::After { offset_bytes, .. } => *offset_bytes,
        }
    }
}

impl From<LocationOutput> for (cel_interpreter::objects::Key, Value) {
    fn from(value: LocationOutput) -> Self {
        let key = match &value {
            LocationOutput::Before { .. } => "before".into(),
            LocationOutput::After { .. } => "after".into(),
        };
        let offset_bytes = value.offset_bytes();
        let id = value.into_id();
        (
            key,
            Value::from(HashMap::from([
                ("id", Value::from(id)),
                (
                    "offset_bytes",
                    u64::try_from(offset_bytes)
                        .expect("offset_bytes must fit in 64 bit unsigned int")
                        .into(),
                ),
            ])),
        )
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
