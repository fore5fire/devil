use std::fmt::Debug;
use std::sync::Arc;

use cel_interpreter::{Duration, Value};
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
mod value;

pub use graphql::*;
pub use http::*;
pub use http1::*;
pub use http2::*;
pub use raw_http2::*;
pub use raw_tcp::*;
pub use tcp::*;
pub use tls::*;
pub use value::*;

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

#[derive(Debug, Clone, Default, Serialize)]
pub struct StepPlanOutputs {
    pub graphql: Option<PlanWrapper<GraphQlPlanOutput>>,
    pub http: Option<PlanWrapper<HttpPlanOutput>>,
    pub h1c: Option<PlanWrapper<Http1PlanOutput>>,
    pub h1: Option<PlanWrapper<Http1PlanOutput>>,
    pub h2c: Option<PlanWrapper<Http2PlanOutput>>,
    pub raw_h2c: Option<PlanWrapper<RawHttp2PlanOutput>>,
    pub h2: Option<PlanWrapper<Http2PlanOutput>>,
    pub raw_h2: Option<PlanWrapper<RawHttp2PlanOutput>>,
    //pub http3: Option<Http3PlanOutput>>,
    pub tls: Option<PlanWrapper<TlsPlanOutput>>,
    pub tcp: Option<PlanWrapper<TcpPlanOutput>>,
    pub raw_tcp: Option<PlanWrapper<RawTcpPlanOutput>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanWrapper<T: Debug + Clone> {
    plan: T,
}

impl<T: Debug + Clone> PlanWrapper<T> {
    pub fn new(plan: T) -> Self {
        Self { plan }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct PauseValueOutput {
    pub location: LocationOutput,
    pub duration: Duration,
    pub offset_bytes: i64,
    pub r#await: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalValueOutput {
    pub location: LocationOutput,
    pub target: String,
    pub kind: SignalKind,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum SignalKind {
    Register { priority: usize },
    Release,
    Unlock,
}

#[derive(Debug, Clone, Serialize)]
pub enum SyncOutput {
    Barrier { count: usize },
    Mutex,
    PriorityMutex,
    Semaphore { permits: usize },
    PrioritySemaphore { permits: usize },
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

#[derive(Debug, Clone, Serialize)]
pub struct RunForOutput {
    pub key: crate::IterableKey,
    pub value: OutValue,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunWhileOutput {
    pub index: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunCountOutput {
    pub index: u64,
}
