use core::str;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::bail;
use cel_interpreter::{Duration, Value};
use doberman_derive::{BigQuerySchema, Record};
use indexmap::IndexMap;
use serde::Serialize;
use strum::EnumIs;

use crate::{location, IterableKey, Parallelism, ProtocolField};

mod bytes;
mod graphql;
mod http;
mod http1;
mod http2;
mod name;
mod normalize;
mod raw_http2;
mod raw_tcp;
mod tcp;
mod tls;
mod value;

pub use bytes::*;
pub use graphql::*;
pub use http::*;
pub use http1::*;
pub use http2::*;
pub use name::*;
pub use normalize::*;
pub use raw_http2::*;
pub use raw_tcp::*;
pub use tcp::*;
pub use tls::*;
pub use value::*;

pub trait State<'a, O: Into<&'a Arc<String>>, I: IntoIterator<Item = O>> {
    fn get(&self, name: &'a Arc<String>) -> Option<&StepOutput>;
    fn current(&self) -> &StepPlanOutputs;
    fn run_for(&self) -> &Option<RunForOutput>;
    fn run_while(&self) -> &Option<RunWhileOutput>;
    fn run_count(&self) -> &Option<RunCountOutput>;
    fn locals(&self) -> cel_interpreter::objects::Map;
    fn iter(&self) -> I;
    fn run_name(&self) -> &RunName;
    fn job_name(&self) -> Option<&JobName>;
    // Check for matching singed int indexes too.
}

#[derive(Debug, Clone)]
pub enum StepPlanOutput {
    Graphql(GraphqlPlanOutput),
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
    pub graphql: Option<PlanWrapper<GraphqlPlanOutput>>,
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

#[derive(Debug, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "run")]
#[bigquery(tag = "kind")]
#[record(rename = "run")]
pub struct RunOutput {
    pub name: RunName,
    pub steps: IndexMap<Arc<String>, Arc<StepOutput>>,
}

impl RunOutput {
    pub fn new(name: RunName) -> Self {
        Self {
            name,
            steps: IndexMap::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "step")]
#[bigquery(tag = "kind")]
#[record(rename = "step")]
pub struct StepOutput {
    pub name: StepName,
    pub jobs: IndexMap<IterableKey, Arc<JobOutput>>,
}

impl StepOutput {
    pub fn new(name: StepName) -> Self {
        Self {
            name,
            jobs: IndexMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "job")]
#[bigquery(tag = "kind")]
#[record(rename = "job")]
pub struct JobOutput {
    pub name: JobName,
    pub graphql: Option<Arc<GraphqlOutput>>,
    pub http: Option<Arc<HttpOutput>>,
    pub h1: Option<Arc<Http1Output>>,
    pub h1c: Option<Arc<Http1Output>>,
    pub h2: Option<Arc<Http2Output>>,
    pub h2c: Option<Arc<Http2Output>>,
    pub raw_h2: Option<Arc<RawHttp2Output>>,
    pub raw_h2c: Option<Arc<RawHttp2Output>>,
    //pub http3: Option<Http3Output>>,
    pub tls: Option<Arc<TlsOutput>>,
    pub tcp: Option<Arc<TcpOutput>>,
    pub raw_tcp: Option<Arc<RawTcpOutput>>,
}

impl JobOutput {
    pub fn empty(name: JobName) -> Self {
        Self {
            name,
            graphql: None,
            http: None,
            h1: None,
            h1c: None,
            h2: None,
            h2c: None,
            raw_h2: None,
            raw_h2c: None,
            // http3: None,
            tls: None,
            tcp: None,
            raw_tcp: None,
        }
    }
    pub fn http1(&self) -> Option<&Arc<Http1Output>> {
        self.h1.as_ref().or_else(|| self.h1c.as_ref())
    }
    pub fn http2(&self) -> Option<&Arc<Http2Output>> {
        self.h2.as_ref().or_else(|| self.h2c.as_ref())
    }
    pub fn raw_http2(&self) -> Option<&Arc<RawHttp2Output>> {
        self.raw_h2.as_ref().or_else(|| self.raw_h2c.as_ref())
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

#[derive(Debug, Clone, Serialize)]
pub struct PauseValueOutput {
    pub location: LocationOutput,
    pub duration: Duration,
    pub r#await: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalValueOutput {
    pub location: LocationOutput,
    pub target: String,
    pub op: SignalOp,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum SignalOp {
    Register { priority: usize },
    Release,
    Unlock,
}

impl SignalOp {
    pub fn try_from_str(raw: &str) -> anyhow::Result<Self> {
        Ok(match raw {
            "register" => Self::Register { priority: 0 },
            "release" => Self::Release,
            "unlock" => Self::Unlock,
            raw => bail!("invalid value {raw} for signal op"),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum SyncOutput {
    Barrier { count: usize },
    Mutex,
    PriorityMutex,
    Semaphore { permits: usize },
    PrioritySemaphore { permits: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct LocationValueOutput {
    pub id: location::Location,
    pub offset_bytes: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum LocationOutput {
    Before(LocationValueOutput),
    After(LocationValueOutput),
}

impl LocationOutput {
    #[inline]
    pub fn value(&self) -> &LocationValueOutput {
        match self {
            Self::Before(loc) | Self::After(loc) => &loc,
        }
    }

    #[inline]
    pub fn into_value(self) -> LocationValueOutput {
        match self {
            Self::Before(loc) | Self::After(loc) => loc,
        }
    }
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

#[derive(Debug, Clone, Copy, Serialize, EnumIs, BigQuerySchema)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Send,
    Recv,
}
