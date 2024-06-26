use crate::bindings::{EnumKind, Literal, ValueOrArray};
use crate::{
    bindings, cel_functions, Error, GraphQlPauseOutput, Http1PauseOutput, Http2FramesPauseOutput,
    Http2PauseOutput, HttpPauseOutput, Result, State, StepPlanOutput, TcpPauseOutput,
    TcpSegmentOptionOutput, TcpSegmentOutput, TcpSegmentsPauseOutput, TlsPauseOutput,
};
use base64::Engine;
use cel_interpreter::{Context, Program};
use chrono::{Duration, NaiveDateTime, TimeZone};
use go_parse_duration::parse_duration;
use indexmap::IndexMap;
use itertools::Itertools;
use rand::RngCore;
use std::convert::Infallible;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::OnceLock;
use std::{collections::HashMap, ops::Deref, rc::Rc, sync::Arc};
use tokio::sync::Semaphore;
use url::Url;

#[derive(Debug)]
pub struct Plan {
    pub steps: IndexMap<String, Step>,
    pub locals: IndexMap<String, PlanValue<PlanData, Infallible>>,
}

impl<'a> Plan {
    pub fn parse(input: &'a str) -> Result<Self> {
        let parsed = bindings::Plan::parse(input)?;
        Self::from_binding(parsed)
    }

    pub fn from_binding(mut plan: bindings::Plan) -> Result<Self> {
        static IMPLICIT_DEFUALTS: OnceLock<bindings::Plan> = OnceLock::new();

        // Apply the implicit defaults to the user defaults.
        let implicit_defaults = IMPLICIT_DEFUALTS.get_or_init(|| {
            let raw = include_str!("implicit_defaults.cp.toml");
            toml::de::from_str::<bindings::Plan>(raw).unwrap()
        });
        plan.devil
            .defaults
            .extend(implicit_defaults.devil.defaults.clone());
        // Generate final steps.
        let steps: IndexMap<String, Step> = plan
            .steps
            .into_iter()
            .map(|(name, value)| {
                // Apply the user and implicit defaults.
                let value = value.apply_defaults(plan.devil.defaults.clone());
                // Apply planner requirements and convert to planner structure.
                Ok((name, Step::from_bindings(value)?))
            })
            .collect::<Result<_>>()?;
        let locals = plan
            .devil
            .locals
            .into_iter()
            .map(|(k, v)| Ok((k, PlanValue::try_from(v)?)))
            .collect::<Result<_>>()?;

        Ok(Plan { steps, locals })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsVersion {
    SSL1,
    SSL2,
    SSL3,
    TLS1_0,
    TLS1_1,
    TLS1_2,
    TLS1_3,
    DTLS1_0,
    DTLS1_1,
    DTLS1_2,
    DTLS1_3,
    Other(u16),
}

impl FromStr for TlsVersion {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.into() {
            "ssl1" => Self::SSL1,
            "ssl2" => Self::SSL2,
            "ssl3" => Self::SSL3,
            "tls1.0" => Self::TLS1_0,
            "tls1.1" => Self::TLS1_1,
            "tls1.2" => Self::TLS1_2,
            "tls1.3" => Self::TLS1_3,
            _ => return Err(Error(format!("invalid tls version string {}", s))),
        })
    }
}

impl From<&TlsVersion> for cel_interpreter::Value {
    fn from(value: &TlsVersion) -> Self {
        match value {
            TlsVersion::SSL1 => cel_interpreter::Value::String(Arc::new("ssl1".to_owned())),
            TlsVersion::SSL2 => cel_interpreter::Value::String(Arc::new("ssl2".to_owned())),
            TlsVersion::SSL3 => cel_interpreter::Value::String(Arc::new("ssl3".to_owned())),
            TlsVersion::TLS1_0 => cel_interpreter::Value::String(Arc::new("tls1.0".to_owned())),
            TlsVersion::TLS1_1 => cel_interpreter::Value::String(Arc::new("tls1.1".to_owned())),
            TlsVersion::TLS1_2 => cel_interpreter::Value::String(Arc::new("tls1.2".to_owned())),
            TlsVersion::TLS1_3 => cel_interpreter::Value::String(Arc::new("tls1.3".to_owned())),
            TlsVersion::DTLS1_0 => cel_interpreter::Value::String(Arc::new("dtls1.0".to_owned())),
            TlsVersion::DTLS1_1 => cel_interpreter::Value::String(Arc::new("dtls1.1".to_owned())),
            TlsVersion::DTLS1_2 => cel_interpreter::Value::String(Arc::new("dtls1.2".to_owned())),
            TlsVersion::DTLS1_3 => cel_interpreter::Value::String(Arc::new("dtls1.3".to_owned())),
            TlsVersion::Other(a) => cel_interpreter::Value::UInt(*a as u64),
        }
    }
}

#[derive(Debug)]
pub enum HttpVersion {
    HTTP0_9,
    HTTP1_0,
    HTTP1_1,
    HTTP2,
    HTTP3,
}

#[derive(Debug, Clone, Default)]
pub struct PausePoints {
    pub start: Vec<PauseValue>,
    pub end: Vec<PauseValue>,
}

impl PauseJoins for PausePoints {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.start
            .iter()
            .flat_map(|p| p.join.iter())
            .chain(self.end.iter().flat_map(|p| p.join.iter()))
            .map(ToOwned::to_owned)
    }
}

impl TryFrom<bindings::PausePoints> for PausePoints {
    type Error = Error;
    fn try_from(binding: bindings::PausePoints) -> Result<PausePoints> {
        Ok(Self {
            start: binding
                .start
                .unwrap_or_default()
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            end: binding
                .end
                .unwrap_or_default()
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl Evaluate<crate::PausePointsOutput> for PausePoints {
    fn evaluate<'a, S, SO, I>(&self, state: &S) -> Result<crate::PausePointsOutput>
    where
        S: State<'a, SO, I>,
        SO: Into<&'a str>,
        I: IntoIterator<Item = SO>,
    {
        Ok(crate::PausePointsOutput {
            start: self.start.evaluate(state)?,
            end: self.end.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PauseValue {
    duration: PlanValue<Duration>,
    offset_bytes: PlanValue<i64>,
    join: Vec<String>,
}

impl TryFrom<bindings::PauseValue> for PauseValue {
    type Error = Error;
    fn try_from(binding: bindings::PauseValue) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            duration: binding
                .duration
                .ok_or_else(|| Error("pause duration is required".to_owned()))?
                .try_into()?,
            offset_bytes: binding
                .offset_bytes
                .map(PlanValue::<i64>::try_from)
                .transpose()?
                .unwrap_or_default(),
            join: binding.join.map(|j| Vec::from(j)).unwrap_or_default(),
        })
    }
}

impl Evaluate<crate::PauseValueOutput> for PauseValue {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::PauseValueOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::PauseValueOutput {
            duration: self.duration.evaluate(state)?,
            offset_bytes: self.offset_bytes.evaluate(state)?,
            join: self.join.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub pause: HttpPause,
}

impl TryFrom<bindings::Http> for HttpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Http) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::<Url>::try_from)
                .ok_or_else(|| Error("http.url is required".to_owned()))??,
            method: binding
                .method
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            add_content_length: binding
                .add_content_length
                .map(PlanValue::<AddContentLength>::try_from)
                .ok_or_else(|| Error("http.add_content_length is required".to_owned()))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            headers: PlanValueTable::try_from(binding.headers.unwrap_or_default())?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

impl Evaluate<crate::HttpPlanOutput> for HttpRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::HttpPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::HttpPlanOutput {
            url: self.url.evaluate(state)?,
            method: self
                .method
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?,
            add_content_length: self.add_content_length.evaluate(state)?,
            headers: self.headers.evaluate(state)?,
            body: self
                .body
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?
                .unwrap_or_default(),
            pause: self.pause.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpPause {
    pub open: PausePoints,
    pub request_headers: PausePoints,
    pub request_body: PausePoints,
    pub response_headers: PausePoints,
    pub response_body: PausePoints,
}

impl PauseJoins for HttpPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.open
            .joins()
            .chain(self.request_headers.joins())
            .chain(self.request_body.joins())
            .chain(self.response_headers.joins())
            .chain(self.response_body.joins())
    }
}

impl TryFrom<bindings::HttpPause> for HttpPause {
    type Error = Error;
    fn try_from(value: bindings::HttpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            open: PausePoints::try_from(value.open.unwrap_or_default())?,
            request_headers: PausePoints::try_from(value.request_headers.unwrap_or_default())?,
            request_body: PausePoints::try_from(value.request_body.unwrap_or_default())?,
            response_headers: PausePoints::try_from(value.response_headers.unwrap_or_default())?,
            response_body: PausePoints::try_from(value.response_body.unwrap_or_default())?,
        })
    }
}

impl Evaluate<HttpPauseOutput> for HttpPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<HttpPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let resp = HttpPauseOutput {
            open: self.open.evaluate(state)?,
            request_headers: self.request_headers.evaluate(state)?,
            request_body: self.request_body.evaluate(state)?,
            response_headers: self.response_headers.evaluate(state)?,
            response_body: self.response_body.evaluate(state)?,
        };
        if resp.response_headers.end.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.end with negative offset is not supported".to_owned(),
            ));
        }
        if resp.response_body.start.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.start with negative offset is not supported"
                    .to_owned(),
            ));
        }
        Ok(resp)
    }
}

#[derive(Debug, Clone)]
pub struct Http1Request {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub version_string: Option<PlanValue<Vec<u8>>>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,
    pub body: Option<PlanValue<Vec<u8>>>,

    pub pause: Http1Pause,
}

impl Evaluate<crate::Http1PlanOutput> for Http1Request {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http1PlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http1PlanOutput {
            url: self.url.evaluate(state)?,
            method: self
                .method
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?,
            version_string: self
                .version_string
                .as_ref()
                .map(|v| v.evaluate(state))
                .transpose()?,
            add_content_length: self.add_content_length.evaluate(state)?,
            headers: self.headers.evaluate(state)?,
            body: self
                .body
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?
                .unwrap_or_default(),
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::Http1> for Http1Request {
    type Error = Error;
    fn try_from(binding: bindings::Http1) -> Result<Self> {
        Ok(Self {
            url: binding
                .common
                .url
                .map(PlanValue::<Url>::try_from)
                .ok_or_else(|| Error("http1.url is required".to_owned()))??,
            version_string: binding
                .version_string
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            method: binding
                .common
                .method
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            add_content_length: binding
                .common
                .add_content_length
                .map(PlanValue::<AddContentLength>::try_from)
                .ok_or_else(|| Error("http.add_content_length is required".to_owned()))??,
            headers: PlanValueTable::try_from(binding.common.headers.unwrap_or_default())?,
            body: binding
                .common
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http1Pause {
    pub open: PausePoints,
    pub request_headers: PausePoints,
    pub request_body: PausePoints,
    pub response_headers: PausePoints,
    pub response_body: PausePoints,
}

impl PauseJoins for Http1Pause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.open
            .joins()
            .chain(self.request_headers.joins())
            .chain(self.request_body.joins())
            .chain(self.response_headers.joins())
            .chain(self.response_body.joins())
    }
}

impl TryFrom<bindings::Http1Pause> for Http1Pause {
    type Error = Error;
    fn try_from(value: bindings::Http1Pause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            open: PausePoints::try_from(value.open.unwrap_or_default())?,
            request_headers: PausePoints::try_from(value.request_headers.unwrap_or_default())?,
            request_body: PausePoints::try_from(value.request_body.unwrap_or_default())?,
            response_headers: PausePoints::try_from(value.response_headers.unwrap_or_default())?,
            response_body: PausePoints::try_from(value.response_body.unwrap_or_default())?,
        })
    }
}

impl Evaluate<Http1PauseOutput> for Http1Pause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http1PauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let resp = Http1PauseOutput {
            open: self.open.evaluate(state)?,
            request_headers: self.request_headers.evaluate(state)?,
            request_body: self.request_body.evaluate(state)?,
            response_headers: self.response_headers.evaluate(state)?,
            response_body: self.response_body.evaluate(state)?,
        };
        if resp.response_headers.end.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.end with negative offset is not supported".to_owned(),
            ));
        }
        if resp.response_body.start.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.start with negative offset is not supported"
                    .to_owned(),
            ));
        }
        Ok(resp)
    }
}

#[derive(Debug, Clone)]
pub struct Http2Request {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub trailers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,

    pub pause: Http2Pause,
}

impl Evaluate<crate::Http2PlanOutput> for Http2Request {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http2PlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http2PlanOutput {
            url: self.url.evaluate(state)?,
            method: self
                .method
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?,
            add_content_length: self.add_content_length.evaluate(state)?,
            headers: self.headers.evaluate(state)?,
            trailers: self.trailers.evaluate(state)?,
            body: self
                .body
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?
                .unwrap_or_default(),
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::Http2> for Http2Request {
    type Error = Error;
    fn try_from(binding: bindings::Http2) -> Result<Self> {
        Ok(Self {
            url: binding
                .common
                .url
                .map(PlanValue::<Url>::try_from)
                .ok_or_else(|| Error("http2.url is required".to_owned()))??,
            method: binding
                .common
                .method
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            body: binding
                .common
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            add_content_length: binding
                .common
                .add_content_length
                .map(PlanValue::<AddContentLength>::try_from)
                .ok_or_else(|| Error("http2.add_content_length is required".to_owned()))??,
            headers: PlanValueTable::try_from(binding.common.headers.unwrap_or_default())?,
            trailers: PlanValueTable::try_from(binding.trailers.unwrap_or_default())?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http2Pause {
    pub open: PausePoints,
    pub request_headers: PausePoints,
    pub request_body: PausePoints,
    pub response_headers: PausePoints,
    pub response_body: PausePoints,
}

impl PauseJoins for Http2Pause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.open
            .joins()
            .chain(self.request_headers.joins())
            .chain(self.request_body.joins())
            .chain(self.response_headers.joins())
            .chain(self.response_body.joins())
    }
}

impl TryFrom<bindings::Http2Pause> for Http2Pause {
    type Error = Error;
    fn try_from(value: bindings::Http2Pause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            open: PausePoints::try_from(value.open.unwrap_or_default())?,
            request_headers: PausePoints::try_from(value.request_headers.unwrap_or_default())?,
            request_body: PausePoints::try_from(value.request_body.unwrap_or_default())?,
            response_headers: PausePoints::try_from(value.response_headers.unwrap_or_default())?,
            response_body: PausePoints::try_from(value.response_body.unwrap_or_default())?,
        })
    }
}

impl Evaluate<Http2PauseOutput> for Http2Pause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2PauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let resp = Http2PauseOutput {
            open: self.open.evaluate(state)?,
            request_headers: self.request_headers.evaluate(state)?,
            request_body: self.request_body.evaluate(state)?,
            response_headers: self.response_headers.evaluate(state)?,
            response_body: self.response_body.evaluate(state)?,
        };
        if resp.response_headers.end.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.end with negative offset is not supported".to_owned(),
            ));
        }
        if resp.response_body.start.iter().any(|p| p.offset_bytes < 0) {
            return Err(Error(
                "http.pause.response_headers.start with negative offset is not supported"
                    .to_owned(),
            ));
        }
        Ok(resp)
    }
}

#[derive(Debug, Clone)]
pub struct Http2FramesRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: Http2FramesPause,
}

impl Evaluate<crate::Http2FramesPlanOutput> for Http2FramesRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::Http2FramesPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http2FramesPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::Http2Frames> for Http2FramesRequest {
    type Error = Error;
    fn try_from(binding: bindings::Http2Frames) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("tcp.host is required".to_owned()))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("tcp.port is required".to_owned()))??,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http2FramesPause {
    pub handshake: PausePoints,
}

impl PauseJoins for Http2FramesPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.handshake.joins()
    }
}

impl TryFrom<bindings::Http2FramesPause> for Http2FramesPause {
    type Error = Error;
    fn try_from(value: bindings::Http2FramesPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: PausePoints::try_from(value.handshake.unwrap_or_default())?,
        })
    }
}

impl Evaluate<Http2FramesPauseOutput> for Http2FramesPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2FramesPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2FramesPauseOutput {
            handshake: self.handshake.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http3Request {}

impl TryFrom<bindings::Http3> for Http3Request {
    type Error = Error;
    fn try_from(binding: bindings::Http3) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlRequest {
    pub url: PlanValue<Url>,
    pub query: PlanValue<String>,
    pub params: Option<PlanValueTable<Vec<u8>, Error, serde_json::Value, Error>>,
    pub operation: Option<PlanValue<serde_json::Value>>,
    pub pause: GraphQlPause,
}

impl PauseJoins for GraphQlRequest {
    fn joins(&self) -> impl Iterator<Item = String> {
        std::iter::empty()
    }
}

impl TryFrom<bindings::GraphQl> for GraphQlRequest {
    type Error = Error;
    fn try_from(binding: bindings::GraphQl) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::<Url>::try_from)
                .ok_or_else(|| Error("graphql.url is required".to_owned()))??,
            query: binding
                .query
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("graphql.query is required".to_owned()))??,
            params: binding.params.map(PlanValueTable::try_from).transpose()?,
            operation: binding
                .operation
                .map(PlanValue::<serde_json::Value>::try_from)
                .transpose()?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

impl Evaluate<crate::GraphQlPlanOutput> for GraphQlRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::GraphQlPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::GraphQlPlanOutput {
            url: self.url.evaluate(state)?,
            query: self.query.evaluate(state)?,
            operation: self
                .operation
                .as_ref()
                .map(|x| x.evaluate(state))
                .transpose()?,
            params: self
                .params
                .as_ref()
                .map(|p| p.evaluate(state))
                .transpose()?
                .map(|p| p.into_iter().collect()),
            pause: self.pause.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GraphQlPause {}

impl TryFrom<bindings::GraphQlPause> for GraphQlPause {
    type Error = Error;
    fn try_from(value: bindings::GraphQlPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {})
    }
}

impl Evaluate<GraphQlPauseOutput> for GraphQlPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<GraphQlPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(GraphQlPauseOutput {})
    }
}

#[derive(Debug, Clone)]
pub struct TcpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: TcpPause,
}

impl Evaluate<crate::TcpPlanOutput> for TcpRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::TcpPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TcpPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::Tcp> for TcpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Tcp) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("tcp.host is required".to_owned()))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("tcp.port is required".to_owned()))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpPause {
    pub handshake: PausePoints,
    pub send_body: PausePoints,
    pub receive_body: PausePoints,
}

impl PauseJoins for TcpPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.handshake
            .joins()
            .chain(self.send_body.joins())
            .chain(self.receive_body.joins())
    }
}

impl TryFrom<bindings::TcpPause> for TcpPause {
    type Error = Error;
    fn try_from(value: bindings::TcpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: PausePoints::try_from(value.handshake.unwrap_or_default())?,
            send_body: PausePoints::try_from(value.send_body.unwrap_or_default())?,
            receive_body: PausePoints::try_from(value.receive_body.unwrap_or_default())?,
        })
    }
}

impl Evaluate<TcpPauseOutput> for TcpPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TcpPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TcpPauseOutput {
            handshake: self.handshake.evaluate(state)?,
            send_body: self.send_body.evaluate(state)?,
            receive_body: self.receive_body.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegmentsRequest {
    pub remote_host: PlanValue<String>,
    pub remote_port: PlanValue<u16>,
    pub local_host: PlanValue<String>,
    // 0 asks the implementation to select an unused port.
    pub local_port: PlanValue<u16>,
    pub isn: PlanValue<u32>,
    pub segments: Vec<TcpSegment>,
    pub pause: TcpSegmentsPause,
}

impl Evaluate<crate::TcpSegmentsPlanOutput> for TcpSegmentsRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::TcpSegmentsPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TcpSegmentsPlanOutput {
            remote_host: self.remote_host.evaluate(state)?,
            remote_port: self.remote_port.evaluate(state)?,
            local_host: self.local_host.evaluate(state)?,
            local_port: self.local_port.evaluate(state)?,
            isn: self.isn.evaluate(state)?,
            window: self.window.evaluate(state)?,
            segments: self
                .segments
                .iter()
                .map(|segments| segments.evaluate(state))
                .try_collect()?,
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::TcpSegments> for TcpSegmentsRequest {
    type Error = Error;
    fn try_from(binding: bindings::TcpSegments) -> Result<Self> {
        Ok(Self {
            remote_host: binding
                .remote_host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("tcp_segments.remote_host is required".to_owned()))??,
            remote_port: binding
                .remote_port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("tcp_segments.remote_port is required".to_owned()))??,
            local_host: binding
                .local_host
                .map(PlanValue::<String>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal("localhost".to_owned())),
            local_port: binding
                .local_port
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(0)),
            isn: binding
                .isn
                .map(PlanValue::<u32>::try_from)
                .transpose()?
                // Random sequence number if not specified.
                .unwrap_or_else(|| PlanValue::Literal(rand::thread_rng().next_u32())),
            segments: binding
                .segments
                .into_iter()
                .flatten()
                .map(TcpSegment::try_from)
                .try_collect()?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegment {
    pub source: PlanValue<u16>,
    pub destination: PlanValue<u16>,
    pub sequence_number: PlanValue<u32>,
    pub acknowledgment: PlanValue<u32>,
    pub data_offset: PlanValue<u8>,
    pub reserved: PlanValue<u8>,
    pub flags: PlanValue<u8>,
    pub window: PlanValue<u16>,
    pub checksum: PlanValue<u16>,
    pub urgent_ptr: PlanValue<u16>,
    pub options: Vec<PlanValue<TcpSegmentOptionOutput>>,
    pub payload: PlanValue<Vec<u8>>,
}

impl Evaluate<TcpSegmentOutput> for TcpSegment {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TcpSegmentOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TcpSegmentOutput {
            source: self.source.evaluate(state)?,
            destination: self.destination.evaluate(state)?,
            sequence_number: self.sequence_number.evaluate(state)?,
            acknowledgment: self.acknowledgment.evaluate(state)?,
            data_offset: self.data_offset.evaluate(state)?,
            reserved: self.reserved.evaluate(state)?,
            flags: self.flags.evaluate(state)?,
            window: self.window.evaluate(state)?,
            checksum: self.checksum.evaluate(state)?,
            urgent_ptr: self.urgent_ptr.evaluate(state)?,
            options: self.options.evaluate(state)?,
            payload: self.payload.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::TcpSegment> for TcpSegment {
    type Error = crate::Error;
    fn try_from(value: bindings::TcpSegment) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            source: value
                .source
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or_default(),
            destination: value
                .destination
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or_default(),
            sequence_number: value
                .sequence_number
                .map(PlanValue::<u32>::try_from)
                .transpose()?
                .unwrap_or_default(),
            acknowledgment: value
                .acknowledgment
                .map(PlanValue::<u32>::try_from)
                .transpose()?
                .unwrap_or_default(),
            data_offset: value
                .data_offset
                .map(PlanValue::<u8>::try_from)
                .transpose()?
                .unwrap_or_default(),
            reserved: value
                .reserved
                .map(PlanValue::<u8>::try_from)
                .transpose()?
                .unwrap_or_default(),
            flags: value
                .flags
                .map(PlanValue::<u8>::try_from)
                .transpose()?
                .unwrap_or_default(),
            window: value
                .window
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or_default(),
            checksum: value
                .checksum
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or_default(),
            urgent_ptr: value
                .urgent_ptr
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or_default(),
            options: value
                .options
                .unwrap_or_default()
                .into_iter()
                .map(PlanValue::<TcpSegmentOptionOutput>::try_from)
                .collect::<Result<_>>()?,
            payload: value
                .payload
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

//#[derive(Debug, Clone)]
//pub enum TcpSegmentOption {
//    Nop,
//    Timestamps { tsval: u32, tsecr: u32 },
//    Mss(u16),
//    Wscale(u8),
//    SackPermitted,
//    Sack(Vec<u32>),
//    Raw { kind: u8, value: Vec<u8> },
//}
//
//impl TryFrom<bindings::EnumValue> for TcpSegmentOption {
//    type Error = crate::Error;
//    fn try_from(value: bindings::EnumValue) -> std::prelude::v1::Result<Self, Self::Error> {
//        match value {
//            bindings::Value::LiteralEnum { kind, fields } => Ok(PlanValue::Literal(
//                PlanData(cel_interpreter::Value::Map(cel_interpreter::objects::Map {
//                    map: Rc::new(
//                        fields
//                            .into_iter()
//                            .map(|(k, v)| Ok((k.into(), Self::try_from(v)?)))
//                            .collect::<Result<Vec<_>>>()?,
//                    ),
//                }))
//                .try_into()?,
//            )),
//        }
//    }
//}

#[derive(Debug, Clone, Default)]
pub struct TcpSegmentsPause {
    pub handshake: PausePoints,
}

impl PauseJoins for TcpSegmentsPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.handshake.joins()
    }
}

impl TryFrom<bindings::TcpSegmentsPause> for TcpSegmentsPause {
    type Error = Error;
    fn try_from(value: bindings::TcpSegmentsPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: PausePoints::try_from(value.handshake.unwrap_or_default())?,
        })
    }
}

impl Evaluate<TcpSegmentsPauseOutput> for TcpSegmentsPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TcpSegmentsPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TcpSegmentsPauseOutput {
            handshake: self.handshake.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PlanData(pub cel_interpreter::Value);

impl TryFrom<PlanData> for String {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::Bytes(x) => Ok(String::from_utf8_lossy(&x).to_string()),
            val => Err(Error(format!("{val:?} has invalid value for string value"))),
        }
    }
}

impl TryFrom<PlanData> for u8 {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u8::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u8::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            val => Err(Error(format!(
                "{val:?} has invalid value for 8 bit unsigned int value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for u16 {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u16::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u16::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            val => Err(Error(format!(
                "{val:?} has invalid value for 16 bit unsigned int value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for u32 {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u32::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u32::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            val => Err(Error(format!(
                "{val:?} has invalid value for 32 bit unsigned int value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for u64 {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u64::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u64::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            val => Err(Error(format!(
                "{val:?} has invalid type for 64 bit unsigned int value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for i64 {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(i64::try_from(x).map_err(|e| Error(e.to_string()))?)
            }
            cel_interpreter::Value::Int(x) => Ok(x),
            val => Err(Error(format!(
                "{val:?} has invalid type for 64 bit signed int value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for bool {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bool(x) => Ok(x),
            val => Err(Error(format!("{val:?} has invalid type for bool value",))),
        }
    }
}

impl TryFrom<PlanData> for Vec<u8> {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bytes(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::String(x) => Ok(x.deref().clone().into_bytes()),
            val => Err(Error(format!("{val:?} has invalid type for bytes value"))),
        }
    }
}

impl TryFrom<PlanData> for Duration {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) => parse_duration(&x)
                .map(Duration::nanoseconds)
                .map_err(|e| match e {
                    go_parse_duration::Error::ParseError(s) => Error(s),
                }),
            cel_interpreter::Value::Duration(x) => Ok(x),
            val => Err(Error(format!(
                "{val:?} has invalid type for duration value",
            ))),
        }
    }
}

impl TryFrom<PlanData> for TlsVersion {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        let cel_interpreter::Value::String(x) = value.0 else {
            return Err(Error("TLS version must be a string".to_owned()));
        };
        match x.as_str() {
            "ssl1" => Ok(Self::SSL1),
            "ssl2" => Ok(Self::SSL2),
            "ssl3" => Ok(Self::SSL3),
            "tls1_0" => Ok(Self::TLS1_0),
            "tls1_1" => Ok(Self::TLS1_1),
            "tls1_2" => Ok(Self::TLS1_2),
            "tls1_3" => Ok(Self::TLS1_3),
            val => Err(Error(format!("invalid TLS version {val:?}"))),
        }
    }
}

impl TryFrom<PlanData> for TcpSegmentOptionOutput {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::Map(x) => match x.get(&Self::KIND_KEY.into()) {
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::NOP_KIND => {
                    Ok(Self::Nop)
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::TIMESTAMPS_KIND => {
                    Ok(Self::Timestamps {
                        tsval: x
                            .get(&Self::TSVAL_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                cel_interpreter::Value::Int(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                _ => Err(Error("tcp segment option timestamps `tsval` must be convertible to 32 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option timestamps missing `tsval`".to_owned(),
                            ))??
                            .into(),
                        tsecr: x
                            .get(&Self::TSECR_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                cel_interpreter::Value::Int(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                _ => Err(Error("tcp segment option timestamps `tsecr` must be convertible to 32 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option timestamps missing `tsecr`".to_owned(),
                            ))??
                            .into(),
                    })
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::MSS_KIND => {
                    Ok(Self::Mss(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => u16::try_from(*val).map_err(|e| Error(e.to_string())),
                                cel_interpreter::Value::Int(val) => u16::try_from(*val).map_err(|e| Error(e.to_string())),
                                _ => Err(Error("tcp segment option mss value must be convertible to 16 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option mss missing value".to_owned(),
                            ))??))
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::WSCALE_KIND => {
                    Ok(Self::Wscale(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => u8::try_from(*val).map_err(|e| Error(e.to_string())),
                                cel_interpreter::Value::Int(val) => u8::try_from(*val).map_err(|e| Error(e.to_string())),
                                _ => Err(Error("tcp segment option wscale value must be convertible to 8 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option wscale missing value".to_owned(),
                            ))??))
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::SACK_PERMITTED_KIND => {
                    Ok(Self::SackPermitted)
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::SACK_KIND => {
                    Ok(Self::Sack(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::List(vals) => vals.iter().map(|x| match x {
                                    cel_interpreter::Value::Int(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                    cel_interpreter::Value::UInt(val) => u32::try_from(*val).map_err(|e| Error(e.to_string())),
                                    _ => Err(Error("tcp segment option sack must be convertible to list of 32 bit unsigned int".to_owned()))
                                }).try_collect(),
                                _ => Err(Error("tcp segment option sack value must be convertible to list of 32 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option wscale missing value".to_owned(),
                            ))??))
                }
                Some(cel_interpreter::Value::UInt(kind)) => {
                    Ok(Self::Raw{ kind: u8::try_from(*kind).map_err(|e| Error(e.to_string()))?,
                        value: x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::Bytes(data) => Ok(data.as_ref().to_owned()),
                                cel_interpreter::Value::String(data) => Ok(data.as_bytes().to_vec()),
                                _ => Err(Error("tcp segment option sack value must be convertible to list of 32 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option raw missing value".to_owned(),
                            ))??})
                }
                Some(cel_interpreter::Value::Int(kind)) => {
                    Ok(Self::Raw{ kind: u8::try_from(*kind).map_err(|e| Error(e.to_string()))?,
                        value: x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::Bytes(data) => Ok(data.as_ref().to_owned()),
                                cel_interpreter::Value::String(data) => Ok(data.as_bytes().to_vec()),
                                _ => Err(Error("tcp segment option sack value must be convertible to list of 32 bit unsigned int".to_owned())),
                            })
                            .ok_or_else(|| Error(
                                "tcp segment option raw missing value".to_owned(),
                            ))??})
                }
                _ => Err(Error(
                    "tcp segment option expression result requires string value for key `kind`".to_owned(),
                )),
            },
            cel_interpreter::Value::String(x) => match x.as_str() {
                "nop" => Ok(Self::Nop),
                "sack_permitted" => Ok(Self::SackPermitted),
                val => Err(Error(format!("invalid TLS version {val:?}"))),
            },
            _ => Err(Error(
                "TCP segment option must be a string or map".to_owned(),
            )),
        }
    }
}

impl TryFrom<PlanData> for Url {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        let cel_interpreter::Value::String(x) = value.0 else {
            return Err(Error("URL must be a string".to_owned()));
        };
        Url::parse(&x).map_err(|e| Error(e.to_string()))
    }
}

impl TryFrom<PlanData> for serde_json::Value {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        Ok(match value.0 {
            cel_interpreter::Value::List(l) => Self::Array(
                Arc::try_unwrap(l)
                    .unwrap_or_else(|l| (*l).clone())
                    .into_iter()
                    .map(PlanData)
                    .map(Self::try_from)
                    .try_collect()?,
            ),
            cel_interpreter::Value::Map(m) => Self::Object(
                Rc::try_unwrap(m.map)
                    .unwrap_or_else(|m| (*m).clone())
                    .into_iter()
                    .map(|(k, v)| {
                        let cel_interpreter::objects::Key::String(k) = k else {
                            return Err(Error(
                                "only string keys may be used in json output".to_owned(),
                            ));
                        };
                        Ok((
                            Arc::try_unwrap(k).unwrap_or_else(|k| (*k).clone()),
                            Self::try_from(PlanData(v))?,
                        ))
                    })
                    .try_collect()?,
            ),
            cel_interpreter::Value::Int(n) => Self::Number(serde_json::Number::from(n)),
            cel_interpreter::Value::UInt(n) => Self::Number(serde_json::Number::from(n)),
            cel_interpreter::Value::Float(n) => {
                Self::Number(serde_json::Number::from_f64(n).ok_or_else(|| {
                    Error("json input number fields cannot contain infinity".to_owned())
                })?)
            }
            cel_interpreter::Value::String(s) => {
                Self::String(Arc::try_unwrap(s).unwrap_or_else(|s| (*s).clone()))
            }
            cel_interpreter::Value::Bytes(b) => {
                Self::String(String::from_utf8_lossy(b.as_slice()).to_string())
            }
            cel_interpreter::Value::Bool(b) => Self::Bool(b),
            cel_interpreter::Value::Timestamp(ts) => Self::String(ts.to_rfc3339()),
            cel_interpreter::Value::Null => Self::Null,
            _ => return Err(Error("no mapping to json".to_owned())),
        })
    }
}

impl From<cel_interpreter::Value> for PlanData {
    fn from(value: cel_interpreter::Value) -> Self {
        PlanData(value)
    }
}

impl From<PlanData> for cel_interpreter::Value {
    fn from(value: PlanData) -> Self {
        value.0
    }
}

impl From<String> for PlanData {
    fn from(value: String) -> Self {
        PlanData(value.into())
    }
}

impl TryFrom<toml::value::Datetime> for PlanData {
    type Error = Error;
    fn try_from(value: toml::value::Datetime) -> std::result::Result<Self, Self::Error> {
        use chrono::FixedOffset;
        use chrono::Offset;

        let date = value
            .date
            .map(|date| {
                chrono::NaiveDate::from_ymd_opt(
                    date.year as i32,
                    date.month as u32,
                    date.day as u32,
                )
                .ok_or_else(|| Error("out of bounds date".to_owned()))
            })
            .transpose()?
            .unwrap_or_default();

        let time = value
            .time
            .map(|time| {
                chrono::NaiveTime::from_hms_nano_opt(
                    time.hour as u32,
                    time.minute as u32,
                    time.second as u32,
                    time.nanosecond,
                )
                .ok_or_else(|| Error("out of bounds time".to_owned()))
            })
            .transpose()?
            .unwrap_or_default();

        let datetime = NaiveDateTime::new(date, time);

        let offset = match value.offset {
            Some(toml::value::Offset::Custom { minutes }) => {
                FixedOffset::east_opt(minutes as i32 * 60)
                    .ok_or_else(|| Error("invalid offset".to_owned()))?
            }
            Some(toml::value::Offset::Z) => chrono::Utc.fix(),
            None => chrono::Local
                .offset_from_local_datetime(&datetime)
                .single()
                .ok_or_else(|| Error("ambiguous datetime".to_owned()))?,
        };

        Ok(PlanData(
            offset
                .from_local_datetime(&datetime)
                .single()
                .ok_or_else(|| Error("ambiguous datetime".to_owned()))?
                .into(),
        ))
    }
}

impl TryFrom<toml::Value> for PlanData {
    type Error = Error;
    fn try_from(value: toml::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            toml::Value::String(s) => Ok(Self(cel_interpreter::Value::String(s.into()))),
            toml::Value::Integer(s) => Ok(Self(cel_interpreter::Value::Int(s.into()))),
            toml::Value::Float(s) => Ok(Self(cel_interpreter::Value::Float(s.into()))),
            toml::Value::Boolean(s) => Ok(Self(cel_interpreter::Value::Bool(s.into()))),
            toml::Value::Datetime(s) => Ok(Self(PlanData::try_from(s)?.0)),
            toml::Value::Array(s) => Ok(Self(cel_interpreter::Value::List(Arc::new(
                s.into_iter()
                    .map(|x| Ok(PlanData::try_from(x)?.0))
                    .collect::<Result<_>>()?,
            )))),
            toml::Value::Table(s) => Ok(Self(cel_interpreter::Value::Map(
                cel_interpreter::objects::Map {
                    map: Rc::new(
                        s.into_iter()
                            .map(|(k, v)| {
                                Ok((
                                    cel_interpreter::objects::Key::from(k),
                                    PlanData::try_from(v)?.0,
                                ))
                            })
                            .collect::<Result<_>>()?,
                    ),
                },
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub alpn: Vec<PlanValue<Vec<u8>>>,
    pub body: PlanValue<Vec<u8>>,
    pub pause: TlsPause,
}

impl Evaluate<crate::TlsPlanOutput> for TlsRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::TlsPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TlsPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            alpn: self.alpn.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            pause: self.pause.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::Tls> for TlsRequest {
    type Error = Error;
    fn try_from(binding: bindings::Tls) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("tls.host is required".to_owned()))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("tls.port is required".to_owned()))??,
            alpn: binding
                .alpn
                .into_iter()
                .flatten()
                .map(PlanValue::<Vec<u8>>::try_from)
                .try_collect()?,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TlsPause {
    pub handshake: PausePoints,
    pub send_body: PausePoints,
    pub receive_body: PausePoints,
}

impl PauseJoins for TlsPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.handshake
            .joins()
            .chain(self.send_body.joins())
            .chain(self.receive_body.joins())
    }
}

impl TryFrom<bindings::TlsPause> for TlsPause {
    type Error = Error;
    fn try_from(value: bindings::TlsPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: PausePoints::try_from(value.handshake.unwrap_or_default())?,
            send_body: PausePoints::try_from(value.send_body.unwrap_or_default())?,
            receive_body: PausePoints::try_from(value.receive_body.unwrap_or_default())?,
        })
    }
}

impl Evaluate<TlsPauseOutput> for TlsPause {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TlsPauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TlsPauseOutput {
            handshake: self.handshake.evaluate(state)?,
            send_body: self.send_body.evaluate(state)?,
            receive_body: self.receive_body.evaluate(state)?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct WebsocketRequest {}

#[derive(Debug, Default, Clone)]
pub struct QuicRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub body: PlanValue<Vec<u8>>,
    pub version: Option<PlanValue<TlsVersion>>,
    pub pause: QuicPause,
}

impl TryFrom<bindings::Quic> for QuicRequest {
    type Error = Error;
    fn try_from(binding: bindings::Quic) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("quic.host is required".to_owned()))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("quic.port is required".to_owned()))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            version: binding
                .tls_version
                .map(PlanValue::<TlsVersion>::try_from)
                .transpose()?,
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct QuicPause {
    pub handshake: PausePoints,
}

impl PauseJoins for QuicPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.handshake.joins()
    }
}

impl TryFrom<bindings::QuicPause> for QuicPause {
    type Error = Error;
    fn try_from(value: bindings::QuicPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: PausePoints::try_from(value.handshake.unwrap_or_default())?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct UdpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: UdpPause,
}

impl TryFrom<bindings::Udp> for UdpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Udp) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("udp.host is required".to_owned()))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| Error("udp.port is required".to_owned()))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            pause: binding.pause.unwrap_or_default().try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UdpPause {
    pub send_body: PausePoints,
    pub receive_body: PausePoints,
}

impl PauseJoins for UdpPause {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.send_body.joins().chain(self.receive_body.joins())
    }
}

impl TryFrom<bindings::UdpPause> for UdpPause {
    type Error = Error;
    fn try_from(value: bindings::UdpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            send_body: PausePoints::try_from(value.send_body.unwrap_or_default())?,
            receive_body: PausePoints::try_from(value.receive_body.unwrap_or_default())?,
        })
    }
}

//#[derive(Debug, Default, Clone)]
//pub struct IPRequest {}
#[derive(Debug, Clone)]
pub struct Step {
    pub protocols: StepProtocols,
    pub run: Run,
}

impl Step {
    pub fn from_bindings(binding: bindings::Step) -> Result<Step> {
        let protocols = match binding.protocols {
            bindings::StepProtocols::GraphQl { graphql, http } => StepProtocols::GraphQlHttp {
                graphql: graphql.try_into()?,
                http: http.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                tcp_segments,
            } => StepProtocols::GraphQlH1c {
                graphql: graphql.try_into()?,
                h1c: h1c.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                tcp_segments,
            } => StepProtocols::GraphQlH1 {
                graphql: graphql.try_into()?,
                h1: h1.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH2c {
                graphql,
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => StepProtocols::GraphQlH2c {
                graphql: graphql.try_into()?,
                h2c: h2c.unwrap_or_default().try_into()?,
                http2_frames: http2_frames.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH2 {
                graphql,
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => StepProtocols::GraphQlH2 {
                graphql: graphql.try_into()?,
                h2: h2.unwrap_or_default().try_into()?,
                http2_frames: http2_frames.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => StepProtocols::GraphQlH3 {
                graphql: graphql.try_into()?,
                h3: h3.unwrap_or_default().try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Http { http } => StepProtocols::Http {
                http: http.try_into()?,
            },
            bindings::StepProtocols::H1c {
                h1c,
                tcp,
                tcp_segments,
            } => StepProtocols::H1c {
                h1c: h1c.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H1 {
                h1,
                tls,
                tcp,
                tcp_segments,
            } => StepProtocols::H1 {
                h1: h1.try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H2c {
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => StepProtocols::H2c {
                h2c: h2c.try_into()?,
                http2_frames: http2_frames.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H2 {
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => StepProtocols::H2 {
                h2: h2.try_into()?,
                http2_frames: http2_frames.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H3 { h3, quic, udp } => StepProtocols::H3 {
                h3: h3.try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tls {
                tls,
                tcp,
                tcp_segments,
            } => StepProtocols::Tls {
                tls: tls.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Dtls { dtls, udp } => StepProtocols::Dtls {
                dtls: dtls.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tcp { tcp, tcp_segments } => StepProtocols::Tcp {
                tcp: tcp.try_into()?,
                tcp_segments: tcp_segments.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::TcpSegments { tcp_segments } => StepProtocols::TcpSegments {
                tcp_segments: tcp_segments.try_into()?,
            },
            bindings::StepProtocols::Quic { quic, udp } => StepProtocols::Quic {
                quic: quic.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Udp { udp } => StepProtocols::Udp {
                udp: udp.try_into()?,
            },
        };

        Ok(Step {
            protocols,
            run: binding
                .run
                .map(|run| {
                    Ok::<_, Error>(Run {
                        count: run
                            .count
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or_else(|| {
                                // The default count shouldn't inhibit looping when while or for is
                                // set, but also shouldn't cause looping if neither are set.
                                PlanValue::Literal(
                                    if run.run_while.is_some() || run.run_for.is_some() {
                                        u64::MAX
                                    } else {
                                        1
                                    },
                                )
                            }),
                        run_if: run
                            .run_if
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or(PlanValue::Literal(true)),
                        run_while: run.run_while.map(PlanValue::try_from).transpose()?,
                        run_for: run
                            .run_for
                            .map(|x| IterablePlanValue::try_from(x))
                            .transpose()?,
                        parallel: run
                            .parallel
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or_default(),
                        share: run.share.map(PlanValue::try_from).transpose()?,
                    })
                })
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub enum Parallelism {
    #[default]
    Serial,
    Parallel(usize),
    Pipelined,
}

impl FromStr for Parallelism {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "serial" => Ok(Self::Serial),
            "parallel" => Ok(Self::Parallel(Semaphore::MAX_PERMITS)),
            "pipelined" => Ok(Self::Pipelined),
            val => Err(Error(format!("unrecognized parallelism string {val}"))),
        }
    }
}

impl TryFrom<PlanData> for Parallelism {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            cel_interpreter::Value::Bool(b) if b => {
                Ok(Parallelism::Parallel(Semaphore::MAX_PERMITS))
            }
            cel_interpreter::Value::Bool(_) => Ok(Parallelism::Serial),
            cel_interpreter::Value::Int(i) => {
                Ok(Parallelism::Parallel(i.try_into().map_err(|_| {
                    Error(format!(
                        "parallelism value {i} must fit in platform word size"
                    ))
                })?))
            }
            cel_interpreter::Value::UInt(i) => {
                Ok(Parallelism::Parallel(i.try_into().map_err(|_| {
                    Error(format!(
                        "parallelism value {i} must fit in platform word size"
                    ))
                })?))
            }
            val => Err(Error(format!(
                "unsupported value {val:?} for field run.parallel"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddContentLength {
    Never,
    Auto,
    Force,
}

impl FromStr for AddContentLength {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "never" => Ok(Self::Never),
            "auto" => Ok(Self::Auto),
            "force" => Ok(Self::Force),
            val => Err(Error(format!(
                "unrecognized add_content_length string {val}"
            ))),
        }
    }
}

impl ToString for AddContentLength {
    fn to_string(&self) -> String {
        match self {
            Self::Never => "never",
            Self::Auto => "auto",
            Self::Force => "force",
        }
        .to_owned()
    }
}

impl TryFrom<PlanData> for AddContentLength {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            val => Err(Error(format!(
                "unsupported value {val:?} for field add_content_length"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub run_if: PlanValue<bool>,
    pub run_while: Option<PlanValue<bool>>,
    pub run_for: Option<IterablePlanValue>,
    pub count: PlanValue<u64>,
    pub parallel: PlanValue<Parallelism>,
    pub share: Option<PlanValue<ProtocolField>>,
}

impl Default for Run {
    fn default() -> Self {
        Run {
            run_if: PlanValue::Literal(true),
            run_while: None,
            run_for: None,
            count: PlanValue::Literal(1),
            parallel: PlanValue::Literal(Parallelism::Serial),
            share: None,
        }
    }
}

impl Evaluate<crate::RunOutput> for Run {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::RunOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let out = crate::RunOutput {
            run_if: self.run_if.evaluate(state)?,
            run_while: self
                .run_while
                .clone()
                .map(|x| x.evaluate(state))
                .transpose()?,
            run_for: self
                .run_for
                .clone()
                .map(|x| x.evaluate(state))
                .transpose()?,
            count: self.count.evaluate(state)?,
            parallel: self.parallel.evaluate(state)?,
            share: self
                .share
                .clone()
                .map(|share| share.evaluate(state))
                .transpose()?,
        };
        // Only one of while or for may be used.
        if out.run_while.is_some() && out.run_for.is_some() {
            return Err(Error("run.while and run.for cannot both be set".to_owned()));
        }
        // While cannot be parallel.
        if !matches!(out.parallel, Parallelism::Serial) && out.run_while.is_some() {
            return Err(Error("run.while cannot be parallel".to_owned()));
        }

        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub enum StepProtocols {
    GraphQlHttp {
        graphql: GraphQlRequest,
        http: HttpRequest,
    },
    GraphQlH1c {
        graphql: GraphQlRequest,
        h1c: Http1Request,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    GraphQlH1 {
        graphql: GraphQlRequest,
        h1: Http1Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    GraphQlH2c {
        graphql: GraphQlRequest,
        h2c: Http2Request,
        http2_frames: Http2FramesRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    GraphQlH2 {
        graphql: GraphQlRequest,
        h2: Http2Request,
        http2_frames: Http2FramesRequest,
        tls: TlsRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    GraphQlH3 {
        graphql: GraphQlRequest,
        h3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Http {
        http: HttpRequest,
    },
    H1c {
        h1c: Http1Request,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    H1 {
        h1: Http1Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    H2c {
        h2c: Http2Request,
        http2_frames: Http2FramesRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    H2 {
        h2: Http2Request,
        http2_frames: Http2FramesRequest,
        tls: TlsRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    H3 {
        h3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Tls {
        tls: TlsRequest,
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    Dtls {
        dtls: TlsRequest,
        udp: UdpRequest,
    },
    Tcp {
        tcp: TcpRequest,
        tcp_segments: TcpSegmentsRequest,
    },
    TcpSegments {
        tcp_segments: TcpSegmentsRequest,
    },
    Quic {
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Udp {
        udp: UdpRequest,
    },
}

impl StepProtocols {
    pub fn into_stack(self) -> Vec<Protocol> {
        match self {
            Self::GraphQlHttp { graphql, http } => {
                vec![Protocol::GraphQl(graphql), Protocol::Http(http)]
            }
            Self::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H1c(h1c),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H1(h1),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::GraphQlH2c {
                graphql,
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H2c(h2c),
                    Protocol::Http2Frames(http2_frames),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::GraphQlH2 {
                graphql,
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H2(h2),
                    Protocol::Http2Frames(http2_frames),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H3(h3),
                    Protocol::Quic(quic),
                    Protocol::Udp(udp),
                ]
            }
            Self::Http { http } => {
                vec![Protocol::Http(http)]
            }
            Self::H1c {
                h1c,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::H1c(h1c),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::H1 {
                h1,
                tls,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::H1(h1),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::H2c {
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::H2c(h2c),
                    Protocol::Http2Frames(http2_frames),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::H2 {
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::H2(h2),
                    Protocol::Http2Frames(http2_frames),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::H3 { h3, quic, udp } => {
                vec![Protocol::H3(h3), Protocol::Quic(quic), Protocol::Udp(udp)]
            }
            Self::Tls {
                tls,
                tcp,
                tcp_segments,
            } => {
                vec![
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::TcpSegments(tcp_segments),
                ]
            }
            Self::Dtls { dtls, udp } => {
                vec![Protocol::Tls(dtls), Protocol::Udp(udp)]
            }
            Self::Tcp { tcp, tcp_segments } => {
                vec![Protocol::Tcp(tcp), Protocol::TcpSegments(tcp_segments)]
            }
            Self::TcpSegments { tcp_segments } => {
                vec![Protocol::TcpSegments(tcp_segments)]
            }
            Self::Quic { quic, udp } => {
                vec![Protocol::Udp(udp), Protocol::Quic(quic)]
            }
            Self::Udp { udp } => {
                vec![Protocol::Udp(udp)]
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Protocol {
    GraphQl(GraphQlRequest),
    Http(HttpRequest),
    H1c(Http1Request),
    H1(Http1Request),
    H2c(Http2Request),
    H2(Http2Request),
    Http2Frames(Http2FramesRequest),
    H3(Http3Request),
    Tls(TlsRequest),
    Tcp(TcpRequest),
    TcpSegments(TcpSegmentsRequest),
    Quic(QuicRequest),
    Udp(UdpRequest),
}

impl Protocol {
    pub fn joins(&self) -> Vec<String> {
        match self {
            Self::GraphQl(proto) => proto.joins().collect(),
            Self::Http(proto) => proto.pause.joins().collect(),
            Self::H1c(proto) => proto.pause.joins().collect(),
            Self::H1(proto) => proto.pause.joins().collect(),
            Self::H2c(proto) => proto.pause.joins().collect(),
            Self::H2(proto) => proto.pause.joins().collect(),
            Self::Http2Frames(proto) => proto.pause.joins().collect(),
            Self::H3(proto) => Vec::new(),
            Self::Tls(proto) => proto.pause.joins().collect(),
            Self::Tcp(proto) => proto.pause.joins().collect(),
            Self::TcpSegments(proto) => proto.pause.joins().collect(),
            Self::Quic(proto) => proto.pause.joins().collect(),
            Self::Udp(proto) => proto.pause.joins().collect(),
        }
    }

    pub fn field(&self) -> ProtocolField {
        match self {
            Self::GraphQl(_) => ProtocolField::GraphQl,
            Self::Http(_) => ProtocolField::Http,
            Self::H1c(_) => ProtocolField::H1c,
            Self::H1(_) => ProtocolField::H1,
            Self::H2c(_) => ProtocolField::H2c,
            Self::H2(_) => ProtocolField::H2,
            Self::Http2Frames(_) => ProtocolField::Http2Frames,
            Self::H3(_) => ProtocolField::H3,
            Self::Tls(_) => ProtocolField::Tls,
            Self::Tcp(_) => ProtocolField::Tcp,
            Self::TcpSegments(_) => ProtocolField::TcpSegments,
            Self::Quic(_) => ProtocolField::Quic,
            Self::Udp(_) => ProtocolField::Udp,
        }
    }
}

impl Evaluate<StepPlanOutput> for Protocol {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<StepPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(match self {
            Self::GraphQl(proto) => StepPlanOutput::GraphQl(proto.evaluate(state)?),
            Self::Http(proto) => StepPlanOutput::Http(proto.evaluate(state)?),
            Self::H1c(proto) => StepPlanOutput::H1c(proto.evaluate(state)?),
            Self::H1(proto) => StepPlanOutput::H1(proto.evaluate(state)?),
            Self::H2c(proto) => StepPlanOutput::H2c(proto.evaluate(state)?),
            Self::H2(proto) => StepPlanOutput::H2(proto.evaluate(state)?),
            Self::Http2Frames(proto) => StepPlanOutput::Http2Frames(proto.evaluate(state)?),
            //Self::Http3(proto) => ProtocolOutput::Http3(proto.evaluate(state)?),
            Self::Tls(proto) => StepPlanOutput::Tls(proto.evaluate(state)?),
            Self::Tcp(proto) => StepPlanOutput::Tcp(proto.evaluate(state)?),
            Self::TcpSegments(proto) => StepPlanOutput::TcpSegments(proto.evaluate(state)?),
            //Self::Quic(proto) => ProtocolOutput::Quic(proto.evaluate(state)?),
            //Self::Udp(proto) => ProtocolOutput::Udp(proto.evaluate(state)?),
            proto => {
                return Err(Error(format!(
                    "support for protocol {proto:?} is incomplete",
                )))
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ProtocolField {
    GraphQl,
    Http,
    H1c,
    H1,
    H2c,
    H2,
    Http2Frames,
    H3,
    Tls,
    Tcp,
    TcpSegments,
    Dtls,
    Quic,
    Udp,
}

impl FromStr for ProtocolField {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.into() {
            "udp" => Ok(Self::Udp),
            "quic" => Ok(Self::Quic),
            "dtls" => Ok(Self::Dtls),
            "tcp_segments" => Ok(Self::TcpSegments),
            "tcp" => Ok(Self::Tcp),
            "tls" => Ok(Self::Tls),
            "http" => Ok(Self::Http),
            "h1c" => Ok(Self::H1c),
            "h1" => Ok(Self::H1),
            "h2c" => Ok(Self::H2c),
            "h2" => Ok(Self::H2),
            "http2_frames" => Ok(Self::Http2Frames),
            "h3" => Ok(Self::H3),
            "graphql" => Ok(Self::GraphQl),
            _ => return Err(Error(format!("invalid tls version string {}", s))),
        }
    }
}

impl TryFrom<PlanData> for ProtocolField {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            val => Err(Error(format!(
                "invalid value {val:?} for protocol reference"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PlanValue<T, E = Error>
where
    T: TryFrom<PlanData, Error = E> + Clone,
{
    Literal(T),
    Dynamic {
        cel: String,
        vars: Vec<(String, String)>,
    },
}

impl<T, E> Default for PlanValue<T, E>
where
    T: TryFrom<PlanData, Error = E> + Clone + Default,
    E: std::error::Error,
{
    fn default() -> Self {
        PlanValue::Literal(T::default())
    }
}

// Conversions from toml keys to PlanValue literals.
impl From<String> for PlanValue<String> {
    fn from(value: String) -> Self {
        Self::Literal(value)
    }
}
impl From<String> for PlanValue<Vec<u8>> {
    fn from(value: String) -> Self {
        Self::Literal(value.into_bytes())
    }
}
impl From<String> for PlanValue<PlanData, Infallible> {
    fn from(value: String) -> Self {
        Self::Literal(value.into())
    }
}

impl<T, E> TryFrom<bindings::Value> for Option<PlanValue<T, E>>
where
    T: TryFrom<PlanData, Error = E> + Clone,
    E: std::error::Error,
    PlanValue<T, E>: TryFrom<bindings::Value, Error = Error>,
{
    type Error = Error;
    fn try_from(value: bindings::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            bindings::Value::Unset { .. } => Ok(None),
            value => Ok(Some(value.try_into()?)),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<String> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!("invalid value {binding:?} for string field"))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<u8> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Int(x)) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error("out-of-bounds unsigned 8 bit integer literal".to_owned())
                })?))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 8 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<u16> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Int(x)) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error("out-of-bounds unsigned 16 bit integer literal".to_owned())
                })?))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 16 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<u32> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Int(x)) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error("out-of-bounds unsigned 32 bit integer literal".to_owned())
                })?))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 32 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<u64> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Int(x)) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error("out-of-bounds unsigned 64 bit integer literal".to_owned())
                })?))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 64 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<i64> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Int(x)) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error("out-of-bounds signed 64 bit integer literal".to_owned())
                })?))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for signed 64 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<bool> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Bool(x)) => Ok(Self::Literal(x)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for boolean field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<Vec<u8>> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(PlanValue::Literal(x.into_bytes())),
            bindings::Value::Literal(Literal::Base64 { base64: data }) => Ok(Self::Literal(
                base64::prelude::BASE64_STANDARD_NO_PAD
                    .decode(data)
                    .map_err(|e| Error(format!("base64 decode: {}", e)))?,
            )),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!("invalid value {binding:?} for bytes field"))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<Duration> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(
                parse_duration(x.as_str())
                    .map(Duration::nanoseconds)
                    .map_err(|_| Error(format!("invalid duration string")))?,
            )),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for duration field"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<TlsVersion> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for tls version field"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<TcpSegmentOptionOutput> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::Enum { kind, mut fields }) => match kind {
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::NOP_KIND => {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::Nop))
                }
                EnumKind::Named(kind)
                    if kind.as_str() == TcpSegmentOptionOutput::TIMESTAMPS_KIND =>
                {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::Timestamps {
                        tsval: fields
                            .remove(TcpSegmentOptionOutput::TSVAL_KEY)
                            .map(|val| match val {
                                ValueOrArray::Value(Literal::Int(i)) => {
                                    u32::try_from(i).map_err(|e| Error(e.to_string()))
                                }
                                _ => Err(Error("invalid type for tsval".to_owned())),
                            })
                            .ok_or_else(|| {
                                Error(
                                    "tsval is required for tcp segment option 'timestamps'"
                                        .to_owned(),
                                )
                            })??,
                        tsecr: fields
                            .remove(TcpSegmentOptionOutput::TSECR_KEY)
                            .map(|val| match val {
                                ValueOrArray::Value(Literal::Int(i)) => {
                                    u32::try_from(i).map_err(|e| Error(e.to_string()))
                                }
                                _ => Err(Error("invalid type for tsecr".to_owned())),
                            })
                            .ok_or_else(|| {
                                Error(
                                    "tsecr is required for tcp segment option 'timestamps'"
                                        .to_owned(),
                                )
                            })??,
                    }))
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::MSS_KIND => {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::Mss(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| match val {
                            ValueOrArray::Value(Literal::Int(i)) => Ok(u16::try_from(i).map_err(|e| Error(e.to_string()))?),
                            _ => Err(Error("invalid type for mss value (expect 16 bit unsigned integer)".to_owned())),
                        })
                        .ok_or_else(|| {
                            Error(
                                "value is required for tcp segment option 'mss'".to_owned(),
                            )
                        })??)))
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::WSCALE_KIND => {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::Wscale(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| match val {
                            ValueOrArray::Value(Literal::Int(i)) => Ok(u8::try_from(i).map_err(|e| Error(e.to_string()))?),
                            _ => Err(Error("invalid type for wscale value (expect 8 bit unsigned integer)".to_owned())),
                        })
                        .ok_or_else(|| {
                            Error(
                                "value is required for tcp segment option 'wscale'".to_owned(),
                            )
                        })??)))
                }
                EnumKind::Named(kind)
                    if kind.as_str() == TcpSegmentOptionOutput::SACK_PERMITTED_KIND =>
                {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::SackPermitted))
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::SACK_KIND => {
                    Ok(PlanValue::Literal(TcpSegmentOptionOutput::Sack(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| match val {
                            ValueOrArray::Array(array) => Ok(array.into_iter().map(|literal| match literal {
                                Literal::Int(i) => u32::try_from(i).map_err(|e| Error(e.to_string())),
                                _ => Err(Error("invalid type for sack value (expect list of 32 bit unsigned integers)".to_owned())),
                            }).try_collect()),
                            _ => Err(Error("invalid type for sack value (expect list of 32 bit unsigned integers)".to_owned())),
                        })
                        .ok_or_else(|| {
                            Error(
                                "value is required for tcp segment option 'sack'".to_owned(),
                            )
                        })???)))
                }
                EnumKind::Numeric(kind) => Ok(PlanValue::Literal(TcpSegmentOptionOutput::Raw {
                    kind: u8::try_from(kind).map_err(|e| Error(e.to_string()))?,
                    value: fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| match val {
                            ValueOrArray::Value(Literal::String(s)) => Ok(s.into_bytes()),
                            ValueOrArray::Value(Literal::Base64 { base64 }) => {
                                Ok(base64::prelude::BASE64_STANDARD_NO_PAD
                                    .decode(base64)
                                    .map_err(|e| Error(format!("base64 decode: {}", e)))?)
                            }
                            _ => Err(Error("invalid type for raw value (expect either a string literal or '{ base64: \"...\" }')".to_owned())),
                        })
                        .ok_or_else(|| {
                            Error(
                                "value is required for raw tcp segment option".to_owned(),
                            )
                        })??,
                })),
                _ => Err(Error(format!(
                    "invalid kind '{:?}' for tcp segment option",
                    kind
                ))),
            },
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for tls version field"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<ProtocolField> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!(
                "invalid value {binding:?} for tls version field"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<Parallelism> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            bindings::Value::Literal(Literal::Bool(b)) if b => {
                Ok(Self::Literal(Parallelism::Parallel(Semaphore::MAX_PERMITS)))
            }
            bindings::Value::Literal(Literal::Bool(_)) => Ok(Self::Literal(Parallelism::Serial)),
            bindings::Value::Literal(Literal::Int(i)) => Ok(Self::Literal(Parallelism::Parallel(
                i.try_into().map_err(|_| {
                    Error(format!(
                        "parallelism value {i} must fit in platform word size"
                    ))
                })?,
            ))),
            val => Err(Error(format!(
                "invalid value {val:?} for field run.parallel"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<AddContentLength> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            val => Err(Error(format!(
                "invalid value {val:?} for field add_content_length"
            ))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<Url> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(
                Url::parse(&x).map_err(|e| Error(e.to_string()))?,
            )),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!("invalid value {binding:?} for url field"))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<serde_json::Value> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.into())),
            bindings::Value::Literal(Literal::Int(x)) => Ok(Self::Literal(x.into())),
            bindings::Value::Literal(Literal::Float(x)) => Ok(Self::Literal(x.into())),
            bindings::Value::Literal(Literal::Bool(x)) => Ok(Self::Literal(x.into())),
            bindings::Value::Literal(Literal::Toml { literal: x }) => Ok(Self::Literal(
                serde_json::to_value(x).map_err(|e| Error(e.to_string()))?,
            )),
            bindings::Value::Literal(Literal::Base64 { base64 }) => Ok(Self::Literal(
                base64::prelude::BASE64_STANDARD_NO_PAD
                    .decode(base64)
                    .map_err(|e| Error(format!("base64 decode: {}", e)))?
                    .into(),
            )),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!("invalid value {binding:?} for json field"))),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<PlanData, Infallible> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Int(x)) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::Literal(Literal::Float(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Bool(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Datetime(x)) => Ok(PlanValue::Literal(x.try_into()?)),
            bindings::Value::Literal(Literal::Toml { literal }) => {
                Ok(PlanValue::Literal(PlanData::try_from(literal)?))
            }
            bindings::Value::Literal(Literal::Base64 { base64 }) => {
                Ok(PlanValue::Literal(PlanData(base64.into())))
            }
            bindings::Value::Literal(Literal::Enum { .. }) => Err(Error(
                "enumerations are not supported for this field".to_owned(),
            )),
            bindings::Value::ExpressionCel { cel, vars } => Ok(PlanValue::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::ExpressionVars { .. } | bindings::Value::Unset { .. } => {
                Err(Error(format!("incomplete value")))
            }
        }
    }
}

impl<T, E> Evaluate<T> for PlanValue<T, E>
where
    T: TryFrom<PlanData, Error = E> + Clone + std::fmt::Debug,
    E: std::error::Error,
{
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        match self {
            PlanValue::Literal(val) => Ok(val.clone()),
            Self::Dynamic { cel, vars } => exec_cel(cel, vars, state)?
                .try_into()
                .map_err(|e: E| Error(e.to_string())),
        }
    }
}

impl<T, E> PlanValue<T, E>
where
    T: TryFrom<PlanData, Error = E> + Clone + std::fmt::Debug,
    E: std::error::Error,
{
    fn vars_from_toml(value: toml::Value) -> Result<Vec<(String, String)>> {
        if let toml::Value::Table(vars) = value {
            Ok(vars
                .into_iter()
                .map(|(name, value)| {
                    let plan_value = match value {
                        toml::Value::String(s) => s,
                        _ => return Err(Error(format!("invalid _vars.{}", name))),
                    };
                    Ok((name, plan_value))
                })
                .try_collect()?)
        } else {
            Err(Error("invalid _vars".to_owned()))
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PlanValueTable<K, KE, V, VE>(pub Vec<(PlanValue<K, KE>, PlanValue<V, VE>)>)
where
    K: TryFrom<PlanData, Error = KE> + Clone,
    KE: std::error::Error,
    V: TryFrom<PlanData, Error = VE> + Clone,
    VE: std::error::Error;

impl<K, KE, VE2, V, VE, KE2> TryFrom<bindings::Table> for PlanValueTable<K, KE, V, VE>
where
    K: TryFrom<PlanData, Error = KE> + Clone,
    KE: std::error::Error,
    PlanValue<K, KE>: TryFrom<bindings::Value, Error = KE2> + From<String>,
    VE2: std::error::Error,
    V: TryFrom<PlanData, Error = VE> + Clone,
    VE: std::error::Error,
    PlanValue<V, VE>: TryFrom<bindings::Value, Error = VE2>,
    KE2: std::error::Error,
{
    type Error = Error;
    fn try_from(binding: bindings::Table) -> Result<Self> {
        Ok(PlanValueTable(match binding {
            bindings::Table::Map(m) => m
                .into_iter()
                // Flatten literal array values into separate entries of the same key.
                // TODO: this should probably be handled in an intermediary layer between bindings
                // and PlanValues since it's currently duplicated in the bindings merge process for
                // map -> table conversion.
                .flat_map(|(k, v)| match v {
                    bindings::ValueOrArray::Array(a) => {
                        a.into_iter().map(|v| (k.clone(), v)).collect_vec()
                    }
                    bindings::ValueOrArray::Value(v) => vec![(k, v)],
                })
                // Convert bindings to PlanValues.
                .map(|(k, v)| {
                    if let bindings::Value::Unset { .. } = v {
                        return Ok(None);
                    }
                    Ok(Some((
                        k.into(),
                        PlanValue::try_from(v).map_err(|e: VE2| Error(e.to_string()))?,
                    )))
                })
                .filter_map(Result::transpose)
                .try_collect()?,
            bindings::Table::Array(a) => a
                .into_iter()
                .map(|entry| {
                    // Filter entries with no value.
                    if let bindings::Value::Unset { .. } = &entry.value {
                        return Ok(None);
                    }
                    Ok(Some((
                        PlanValue::try_from(entry.key).map_err(|e: KE2| Error(e.to_string()))?,
                        PlanValue::try_from(entry.value).map_err(|e: VE2| Error(e.to_string()))?,
                    )))
                })
                .filter_map(Result::transpose)
                .try_collect()?,
        }))
    }
}

#[derive(Debug, Clone)]
pub enum IterablePlanValue {
    Pairs(Vec<(IterableKey, PlanData)>),
    Expression {
        cel: String,
        vars: Vec<(String, String)>,
    },
}

impl Default for IterablePlanValue {
    fn default() -> Self {
        Self::Pairs(Vec::new())
    }
}

impl TryFrom<bindings::Iterable> for IterablePlanValue {
    type Error = Error;
    fn try_from(value: bindings::Iterable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            bindings::Iterable::Array(a) => IterablePlanValue::Pairs(
                a.into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        Ok((
                            IterableKey::Uint(u64::try_from(i).map_err(|e| Error(e.to_string()))?),
                            PlanData::try_from(v)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            bindings::Iterable::Map(m) => IterablePlanValue::Pairs(
                m.into_iter()
                    .map(|(k, v)| Ok((IterableKey::String(k.into()), PlanData::try_from(v)?)))
                    .collect::<Result<_>>()?,
            ),
            bindings::Iterable::ExpressionCel { cel, vars } => IterablePlanValue::Expression {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            },
            // If default vars were specified but never overriden or given a cel expression, treat
            // it as empty.
            bindings::Iterable::ExpressionVars { vars } => Self::default(),
        })
    }
}

impl Evaluate<Vec<(IterableKey, PlanData)>> for IterablePlanValue {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<(IterableKey, PlanData)>>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        match self {
            Self::Pairs(p) => Ok(p.clone()),
            Self::Expression { cel, vars } => match exec_cel(cel, vars, state)?.0 {
                cel_interpreter::Value::List(l) => Arc::try_unwrap(l)
                    .map_or_else(|arc| arc.as_ref().clone(), |val| val)
                    .into_iter()
                    .enumerate()
                    .map(|(i, x)| {
                        Ok((
                            IterableKey::Uint(u64::try_from(i).map_err(|e| Error(e.to_string()))?),
                            PlanData(x),
                        ))
                    })
                    .try_collect(),
                cel_interpreter::Value::Map(m) => Rc::try_unwrap(m.map)
                    .map_or_else(|arc| arc.as_ref().clone(), |val| val)
                    .into_iter()
                    .map(|(k, v)| Ok((k.into(), PlanData(v))))
                    .try_collect(),
                _ => Err(Error("type not iterable".to_owned())),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IterableKey {
    Int(i64),
    Uint(u64),
    Bool(bool),
    String(Arc<String>),
}

impl Display for IterableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(x) => write!(f, "{x}"),
            Self::Uint(x) => write!(f, "{x}"),
            Self::Bool(x) => write!(f, "{x}"),
            Self::String(x) => write!(f, "{x}"),
        }
    }
}

impl From<cel_interpreter::objects::Key> for IterableKey {
    fn from(value: cel_interpreter::objects::Key) -> Self {
        match value {
            cel_interpreter::objects::Key::Int(x) => Self::Int(x),
            cel_interpreter::objects::Key::Uint(x) => Self::Uint(x),
            cel_interpreter::objects::Key::Bool(x) => Self::Bool(x),
            cel_interpreter::objects::Key::String(x) => Self::String(x),
        }
    }
}

impl From<IterableKey> for cel_interpreter::objects::Key {
    fn from(value: IterableKey) -> Self {
        match value {
            IterableKey::Int(x) => Self::Int(x),
            IterableKey::Uint(x) => Self::Uint(x),
            IterableKey::Bool(x) => Self::Bool(x),
            IterableKey::String(x) => Self::String(x),
        }
    }
}

impl From<IterableKey> for cel_interpreter::Value {
    fn from(value: IterableKey) -> Self {
        match value {
            IterableKey::Int(x) => Self::Int(x),
            IterableKey::Uint(x) => Self::UInt(x),
            IterableKey::Bool(x) => Self::Bool(x),
            IterableKey::String(x) => Self::String(x),
        }
    }
}

impl<K, KE, V, VE> Evaluate<Vec<(K, V)>> for PlanValueTable<K, KE, V, VE>
where
    K: TryFrom<PlanData, Error = KE> + Clone + std::fmt::Debug,
    KE: std::error::Error,
    V: TryFrom<PlanData, Error = VE> + Clone + std::fmt::Debug,
    VE: std::error::Error,
{
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<(K, V)>>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        self.0
            .iter()
            .map(|(key, val)| Ok((key.evaluate(state)?, val.evaluate(state)?)))
            .collect()
    }
}

impl<K, KE, V, VE> PlanValueTable<K, KE, V, VE>
where
    K: TryFrom<PlanData, Error = KE> + Clone + std::fmt::Debug,
    KE: std::error::Error,
    V: TryFrom<PlanData, Error = VE> + Clone + std::fmt::Debug,
    VE: std::error::Error,
{
    fn leaf_to_key_value(key: String, value: &mut toml::Value) -> Result<PlanValue<String>> {
        match value {
            // Strings or array values mean the key is not templated.
            toml::Value::String(_) | toml::Value::Array(_) => Ok(PlanValue::Literal(key)),
            // If the value is a table, check for the appropriate option to decide if the key is
            // templated.
            toml::Value::Table(t) => match t.remove("key_is_template") {
                Some(toml::Value::Boolean(b)) if b => Ok(PlanValue::Dynamic {
                    cel: key,
                    vars: t
                        .get("vars")
                        .map(toml::Value::to_owned)
                        .map(PlanValue::<String, Error>::vars_from_toml)
                        .transpose()?
                        .unwrap_or_default(),
                }),
                Some(toml::Value::Boolean(_)) | None => Ok(PlanValue::Literal(key)),
                _ => return Err(Error(format!("{}.key_is_template invalid", key))),
            },
            _ => return Err(Error(format!("{} has invalid type", key))),
        }
    }
}

fn add_state_to_context<'a, S, O, I>(state: &S, ctx: &mut cel_interpreter::Context)
where
    O: Into<&'a str>,
    S: State<'a, O, I>,
    I: IntoIterator<Item = O>,
{
    ctx.add_variable("locals", cel_interpreter::Value::Map(state.locals()))
        .unwrap();
    ctx.add_variable_from_value(
        "steps",
        state
            .iter()
            .into_iter()
            .map(O::into)
            .map(|name| {
                (
                    name,
                    state
                        .get(name)
                        .unwrap()
                        .to_owned()
                        .into_iter()
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<HashMap<_, _>>(),
    );
    ctx.add_variable_from_value("current", state.current().to_owned());
    ctx.add_variable_from_value("for", state.run_for().to_owned());
    ctx.add_variable_from_value("while", state.run_while().to_owned());
    ctx.add_variable_from_value("count", state.run_count().to_owned());
    ctx.add_function("parse_url", cel_functions::url);
    ctx.add_function(
        "parse_form_urlencoded",
        cel_functions::form_urlencoded_parts,
    );
    ctx.add_function("bytes", cel_functions::bytes);
    ctx.add_function("randomDuration", cel_functions::random_duration);
    ctx.add_function("randomInt", cel_functions::random_int);
    ctx.add_function("printf", cel_functions::printf);
}

pub trait Evaluate<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>;
}

impl<T: Evaluate<T2>, T2> Evaluate<Vec<T2>> for Vec<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<T2>>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        self.iter().map(|x| x.evaluate(state)).collect()
    }
}

fn exec_cel<'a, S, O, I>(cel: &str, vars: &[(String, String)], state: &S) -> Result<PlanData>
where
    O: Into<&'a str>,
    S: State<'a, O, I>,
    I: IntoIterator<Item = O>,
{
    let program =
        Program::compile(cel).map_err(|e| Error(format!("compile cel {}: {}", cel, e)))?;
    let mut context = Context::default();
    context.add_variable_from_value(
        "vars",
        vars.into_iter()
            .map(|(name, value)| (name.clone().into(), value.clone().into()))
            .collect::<HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>>(),
    );
    add_state_to_context(state, &mut context);
    Ok(PlanData(program.execute(&context).map_err(|e| {
        Error(format!("execute cel {}: {}", cel, e))
    })?))
}

trait PauseJoins {
    fn joins(&self) -> impl Iterator<Item = String>;
}

impl<T: PauseJoins> PauseJoins for Vec<T> {
    fn joins(&self) -> impl Iterator<Item = String> {
        self.iter().flat_map(PauseJoins::joins)
    }
}
