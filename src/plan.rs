use crate::{
    bindings, cel_functions, Error, GraphQlPauseOutput, Http1PauseOutput, HttpPauseOutput, Result,
    State, StepPlanOutput, TcpPauseOutput, TlsPauseOutput,
};
use base64::Engine;
use cel_interpreter::{Context, Program};
use chrono::{Duration, NaiveDateTime, TimeZone};
use go_parse_duration::parse_duration;
use indexmap::IndexMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::OnceLock;
use std::{collections::HashMap, ops::Deref, rc::Rc, sync::Arc};
use url::Url;

#[derive(Debug)]
pub struct Plan {
    pub steps: IndexMap<String, Step>,
}

impl<'a> Plan {
    pub fn parse(input: &'a str) -> Result<Self> {
        let parsed = toml::from_str(input).map_err(|e| Error(e.to_string()))?;
        Self::from_binding(parsed)
    }

    pub fn from_binding(mut plan: bindings::Plan) -> Result<Self> {
        static IMPLICIT_DEFUALTS: OnceLock<bindings::Plan> = OnceLock::new();

        // Apply the implicit defaults to the user defaults.
        let implicit_defaults = IMPLICIT_DEFUALTS.get_or_init(|| {
            let raw = include_str!("implicit_defaults.cp.toml");
            toml::de::from_str::<bindings::Plan>(raw).unwrap()
        });
        plan.courier
            .defaults
            .extend(implicit_defaults.courier.defaults.clone());
        // Generate final steps.
        let steps: IndexMap<String, Step> = plan
            .steps
            .into_iter()
            .map(|(name, value)| {
                // Apply the user and implicit defaults.
                let value = value.apply_defaults(plan.courier.defaults.clone());
                // Apply planner requirements and convert to planner structure.
                Ok((name, Step::from_bindings(value)?))
            })
            .collect::<Result<_>>()?;

        Ok(Plan { steps })
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

impl TlsVersion {
    pub fn try_from_str(s: &str) -> Result<TlsVersion> {
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

#[derive(Debug)]
pub struct Pause<T> {
    pub before: T,
    pub after: T,
}

impl<T: Clone> Clone for Pause<T> {
    fn clone(&self) -> Self {
        Pause {
            before: self.before.clone(),
            after: self.after.clone(),
        }
    }
}

impl<T: Default> Default for Pause<T> {
    fn default() -> Self {
        Pause {
            before: T::default(),
            after: T::default(),
        }
    }
}

impl<T: TryInto<P, Error = Error>, P: Default> TryFrom<bindings::Pause<T>> for Pause<P> {
    type Error = Error;
    fn try_from(binding: bindings::Pause<T>) -> Result<Pause<P>> {
        Ok(Pause {
            before: binding
                .before
                .map(T::try_into)
                .transpose()?
                .unwrap_or_default(),
            after: binding
                .after
                .map(T::try_into)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl<P: TryInto<T, Error = Error>, T> TryFrom<Pause<P>> for crate::PauseOutput<T> {
    type Error = Error;
    fn try_from(binding: Pause<P>) -> Result<crate::PauseOutput<T>> {
        Ok(crate::PauseOutput {
            before: binding.before.try_into()?,
            after: binding.after.try_into()?,
        })
    }
}

impl<T: Evaluate<O>, O> Evaluate<crate::PauseOutput<O>> for Pause<T> {
    fn evaluate<'a, S, SO, I>(&self, state: &S) -> Result<crate::PauseOutput<O>>
    where
        S: State<'a, SO, I>,
        SO: Into<&'a str>,
        I: IntoIterator<Item = SO>,
    {
        Ok(crate::PauseOutput {
            before: self.before.evaluate(state)?,
            after: self.after.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PauseValue {
    duration: PlanValue<Duration>,
    offset_bytes: PlanValue<i64>,
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
        })
    }
}

impl Evaluate<Vec<crate::PauseValueOutput>> for Vec<PauseValue> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<crate::PauseValueOutput>>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        self.iter().map(|x| x.evaluate(state)).collect()
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,
    pub pause: Pause<HttpPause>,
}

impl TryFrom<bindings::Http> for HttpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Http) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::<Url>::try_from)
                .ok_or_else(|| Error("http.url is required".to_owned()))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            method: binding
                .method
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            headers: PlanValueTable::try_from(binding.headers.unwrap_or_default())?,
            pause: binding.pause.try_into()?,
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
    pub open: Vec<PauseValue>,
    pub request_header: Vec<PauseValue>,
    pub request_body: Vec<PauseValue>,
    pub response_header: Vec<PauseValue>,
    pub response_body: Vec<PauseValue>,
}

impl TryFrom<bindings::HttpPause> for HttpPause {
    type Error = Error;
    fn try_from(value: bindings::HttpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            open: Vec::from(value.open.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            request_header: Vec::from(value.request_header.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            request_body: Vec::from(value.request_body.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            response_header: Vec::from(value.response_header.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            response_body: Vec::from(value.response_body.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
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
        Ok(HttpPauseOutput {
            open: self.open.evaluate(state)?,
            request_header: self.request_header.evaluate(state)?,
            request_body: self.request_body.evaluate(state)?,
            response_header: self.response_header.evaluate(state)?,
            response_body: self.response_body.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http1Request {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub version_string: Option<PlanValue<Vec<u8>>>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,

    pub pause: Pause<Http1Pause>,
}

impl Http1Request {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http1PlanOutput>
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
            method: binding
                .common
                .method
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            version_string: binding
                .common
                .version_string
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            body: binding
                .common
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?,
            headers: PlanValueTable::try_from(binding.common.headers.unwrap_or_default())?,
            pause: binding.pause.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Http1Pause {
    pub open: Vec<PauseValue>,
    pub request_header: Vec<PauseValue>,
    pub request_body: Vec<PauseValue>,
    pub response_header: Vec<PauseValue>,
    pub response_body: Vec<PauseValue>,
}

impl TryFrom<bindings::Http1Pause> for Http1Pause {
    type Error = Error;
    fn try_from(value: bindings::Http1Pause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            open: Vec::from(value.open.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            request_header: Vec::from(value.request_header.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            request_body: Vec::from(value.request_body.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            response_header: Vec::from(value.response_header.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            response_body: Vec::from(value.response_body.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
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
        Ok(Http1PauseOutput {
            open: self.open.evaluate(state)?,
            request_header: self.request_header.evaluate(state)?,
            request_body: self.request_body.evaluate(state)?,
            response_header: self.response_header.evaluate(state)?,
            response_body: self.response_body.evaluate(state)?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct Http2Request {}

impl TryFrom<bindings::Http2> for Http2Request {
    type Error = Error;
    fn try_from(binding: bindings::Http2) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
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
    pub pause: Pause<GraphQlPause>,
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
            pause: binding.pause.try_into()?,
        })
    }
}

impl GraphQlRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::GraphQlPlanOutput>
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
    pub pause: Pause<TcpPause>,
}

impl TcpRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::TcpPlanOutput>
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
            pause: binding.pause.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpPause {
    pub handshake: Vec<PauseValue>,
    pub first_read: Vec<PauseValue>,
    pub first_write: Vec<PauseValue>,
}

impl TryFrom<bindings::TcpPause> for TcpPause {
    type Error = Error;
    fn try_from(value: bindings::TcpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: Vec::from(value.handshake.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            first_read: Vec::from(value.first_read.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            first_write: Vec::from(value.first_write.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
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
            first_read: self.first_read.evaluate(state)?,
            first_write: self.first_write.evaluate(state)?,
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
            _ => Err(Error("invalid type for string value".to_owned())),
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
            _ => Err(Error(
                "invalid type for 16 bit unsigned int value".to_owned(),
            )),
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
            _ => Err(Error(
                "invalid type for 64 bit unsigned int value".to_owned(),
            )),
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
            _ => Err(Error(
                "invalid type for 16 bit unsigned int value".to_owned(),
            )),
        }
    }
}

impl TryFrom<PlanData> for bool {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bool(x) => Ok(x),
            _ => Err(Error("invalid type for bool value".to_owned())),
        }
    }
}

impl TryFrom<PlanData> for Vec<u8> {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bytes(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::String(x) => Ok(x.deref().clone().into_bytes()),
            _ => Err(Error("invalid type for bytes value".to_owned())),
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
            _ => Err(Error("invalid type for duration value".to_owned())),
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
            "ssl1" => Ok(TlsVersion::SSL1),
            "ssl2" => Ok(TlsVersion::SSL2),
            "ssl3" => Ok(TlsVersion::SSL3),
            "tls1_0" => Ok(TlsVersion::TLS1_0),
            "tls1_1" => Ok(TlsVersion::TLS1_1),
            "tls1_2" => Ok(TlsVersion::TLS1_2),
            "tls1_3" => Ok(TlsVersion::TLS1_3),
            _ => Err(Error("invalid TLS version".to_owned())),
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
                    .collect::<Result<_>>()?,
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
                    .collect::<Result<_>>()?,
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
    pub body: PlanValue<Vec<u8>>,
    pub pause: Pause<TlsPause>,
}

impl TlsRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::TlsPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TlsPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
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
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            pause: binding.pause.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TlsPause {
    pub handshake: Vec<PauseValue>,
    pub first_read: Vec<PauseValue>,
    pub first_write: Vec<PauseValue>,
}

impl TryFrom<bindings::TlsPause> for TlsPause {
    type Error = Error;
    fn try_from(value: bindings::TlsPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: Vec::from(value.handshake.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            first_read: Vec::from(value.first_read.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            first_write: Vec::from(value.first_write.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
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
            first_read: self.first_read.evaluate(state)?,
            first_write: self.first_write.evaluate(state)?,
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
    pub pause: Pause<QuicPause>,
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
            pause: binding.pause.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct QuicPause {
    pub handshake: Vec<PauseValue>,
}

impl TryFrom<bindings::QuicPause> for QuicPause {
    type Error = Error;
    fn try_from(value: bindings::QuicPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            handshake: Vec::from(value.handshake.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct UdpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: Pause<UdpPause>,
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
            pause: binding.pause.try_into()?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UdpPause {
    pub first_read: Vec<PauseValue>,
    pub first_write: Vec<PauseValue>,
}

impl TryFrom<bindings::UdpPause> for UdpPause {
    type Error = Error;
    fn try_from(value: bindings::UdpPause) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            first_read: Vec::from(value.first_read.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
            first_write: Vec::from(value.first_write.unwrap_or_default())
                .into_iter()
                .map(PauseValue::try_from)
                .collect::<Result<_>>()?,
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
            // If HTTP1, TLS, or TCP is specified we use HTTP1.
            bindings::StepProtocols::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => StepProtocols::GraphQlHttp1 {
                graphql: graphql.try_into()?,
                http1: http1.unwrap_or_default().try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => StepProtocols::GraphQlHttp2 {
                graphql: graphql.try_into()?,
                http2: http2.unwrap_or_default().try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => StepProtocols::GraphQlHttp3 {
                graphql: graphql.try_into()?,
                http3: http3.unwrap_or_default().try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Http { http } => StepProtocols::Http {
                http: http.try_into()?,
            },
            bindings::StepProtocols::Http1 { http1, tls, tcp } => StepProtocols::Http1 {
                http1: http1.try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Http2 { http2, tls, tcp } => StepProtocols::Http2 {
                http2: http2.try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Http3 { http3, quic, udp } => StepProtocols::Http3 {
                http3: http3.try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tls { tls, tcp } => StepProtocols::Tls {
                tls: tls.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Dtls { tls, udp } => StepProtocols::Dtls {
                tls: tls.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tcp { tcp } => StepProtocols::Tcp {
                tcp: tcp.try_into()?,
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
                    })
                })
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub run_if: PlanValue<bool>,
    pub run_while: Option<PlanValue<bool>>,
    pub run_for: Option<IterablePlanValue>,
    pub count: PlanValue<u64>,
    pub parallel: PlanValue<bool>,
}

impl Default for Run {
    fn default() -> Self {
        Run {
            run_if: PlanValue::Literal(true),
            run_while: None,
            run_for: None,
            count: PlanValue::Literal(1),
            parallel: PlanValue::Literal(false),
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
        };
        // Only one of while or for may be used.
        if out.run_while.is_some() && out.run_for.is_some() {
            return Err(Error("run.while and run.for cannot both be set".to_owned()));
        }
        // While cannot be parallel.
        if out.parallel && out.run_while.is_some() {
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
    GraphQlHttp1 {
        graphql: GraphQlRequest,
        http1: Http1Request,
        tls: Option<TlsRequest>,
        tcp: TcpRequest,
    },
    GraphQlHttp2 {
        graphql: GraphQlRequest,
        http2: Http2Request,
        tls: Option<TlsRequest>,
        tcp: TcpRequest,
    },
    GraphQlHttp3 {
        graphql: GraphQlRequest,
        http3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Http {
        http: HttpRequest,
    },
    Http1 {
        http1: Http1Request,
        tls: Option<TlsRequest>,
        tcp: TcpRequest,
    },
    Http2 {
        http2: Http2Request,
        tls: Option<TlsRequest>,
        tcp: TcpRequest,
    },
    Http3 {
        http3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Tls {
        tls: TlsRequest,
        tcp: TcpRequest,
    },
    Dtls {
        tls: TlsRequest,
        udp: UdpRequest,
    },
    Tcp {
        tcp: TcpRequest,
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
                vec![Protocol::Http(http), Protocol::GraphQl(graphql)]
            }
            Self::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => {
                if let Some(tls) = tls {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Tls(tls),
                        Protocol::Http1(http1),
                        Protocol::GraphQl(graphql),
                    ]
                } else {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Http1(http1),
                        Protocol::GraphQl(graphql),
                    ]
                }
            }
            Self::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => {
                if let Some(tls) = tls {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Tls(tls),
                        Protocol::Http2(http2),
                        Protocol::GraphQl(graphql),
                    ]
                } else {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Http2(http2),
                        Protocol::GraphQl(graphql),
                    ]
                }
            }
            Self::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => {
                vec![
                    Protocol::Udp(udp),
                    Protocol::Quic(quic),
                    Protocol::Http3(http3),
                    Protocol::GraphQl(graphql),
                ]
            }
            Self::Http { http } => {
                vec![Protocol::Http(http)]
            }
            Self::Http1 { http1, tls, tcp } => {
                if let Some(tls) = tls {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Tls(tls),
                        Protocol::Http1(http1),
                    ]
                } else {
                    vec![Protocol::Tcp(tcp), Protocol::Http1(http1)]
                }
            }
            Self::Http2 { http2, tls, tcp } => {
                if let Some(tls) = tls {
                    vec![
                        Protocol::Tcp(tcp),
                        Protocol::Tls(tls),
                        Protocol::Http2(http2),
                    ]
                } else {
                    vec![Protocol::Tcp(tcp), Protocol::Http2(http2)]
                }
            }
            Self::Http3 { http3, quic, udp } => {
                vec![
                    Protocol::Udp(udp),
                    Protocol::Quic(quic),
                    Protocol::Http3(http3),
                ]
            }
            Self::Tls { tls, tcp } => {
                vec![Protocol::Tcp(tcp), Protocol::Tls(tls)]
            }
            Self::Dtls { tls, udp } => {
                vec![Protocol::Udp(udp), Protocol::Tls(tls)]
            }
            Self::Tcp { tcp } => {
                vec![Protocol::Tcp(tcp)]
            }
            Self::Quic { quic, udp } => {
                vec![Protocol::Quic(quic), Protocol::Udp(udp)]
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
    Http1(Http1Request),
    Http2(Http2Request),
    Http3(Http3Request),
    Tls(TlsRequest),
    Tcp(TcpRequest),
    Quic(QuicRequest),
    Udp(UdpRequest),
}

impl Protocol {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<StepPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(match self {
            Self::GraphQl(proto) => StepPlanOutput::GraphQl(proto.evaluate(state)?),
            Self::Http(proto) => StepPlanOutput::Http(proto.evaluate(state)?),
            Self::Http1(proto) => StepPlanOutput::Http1(proto.evaluate(state)?),
            //Self::Http2(proto) => ProtocolOutput::Http2(proto.evaluate(state)?),
            //Self::Http3(proto) => ProtocolOutput::Http3(proto.evaluate(state)?),
            Self::Tls(proto) => StepPlanOutput::Tls(proto.evaluate(state)?),
            Self::Tcp(proto) => StepPlanOutput::Tcp(proto.evaluate(state)?),
            //Self::Quic(proto) => ProtocolOutput::Quic(proto.evaluate(state)?),
            //Self::Udp(proto) => ProtocolOutput::Udp(proto.evaluate(state)?),
            _ => {
                return Err(Error(
                    "support for protocol {proto:?} is incomplete".to_owned(),
                ))
            }
        })
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
            bindings::Value::LiteralString(x) => Ok(Self::Literal(x)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => Err(Error(format!("invalid value {binding:?} for string field"))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<u16> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralInt(x) => {
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
impl TryFrom<bindings::Value> for PlanValue<u64> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralInt(x) => {
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
            bindings::Value::LiteralInt(x) => {
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
            bindings::Value::LiteralBool(x) => Ok(Self::Literal(x)),
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
            bindings::Value::LiteralString(x) => Ok(PlanValue::Literal(x.into_bytes())),
            bindings::Value::LiteralBase64 { base64: data } => Ok(Self::Literal(
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
            bindings::Value::LiteralString(x) => Ok(Self::Literal(
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
            bindings::Value::LiteralString(x) => Ok(Self::Literal(
                TlsVersion::try_from_str(x.as_str()).map_err(|_| {
                    Error("out-of-bounds unsigned 16 bit integer literal".to_owned())
                })?,
            )),
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

impl TryFrom<bindings::Value> for PlanValue<Url> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralString(x) => Ok(Self::Literal(
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
            bindings::Value::LiteralString(x) => Ok(Self::Literal(x.into())),
            bindings::Value::LiteralInt(x) => Ok(Self::Literal(x.into())),
            bindings::Value::LiteralFloat(x) => Ok(Self::Literal(x.into())),
            bindings::Value::LiteralBool(x) => Ok(Self::Literal(x.into())),
            bindings::Value::LiteralToml { literal: x } => Ok(Self::Literal(
                serde_json::to_value(x).map_err(|e| Error(e.to_string()))?,
            )),
            bindings::Value::LiteralBase64 { base64 } => Ok(Self::Literal(
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
            bindings::Value::LiteralString(x) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::LiteralInt(x) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::LiteralFloat(x) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::LiteralBool(x) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::LiteralDatetime(x) => Ok(PlanValue::Literal(x.try_into()?)),
            bindings::Value::LiteralToml { literal: x } => {
                Ok(PlanValue::Literal(PlanData::try_from(x)?))
            }
            bindings::Value::LiteralBase64 { base64: x } => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
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
            PlanValue::Literal(s) => Ok(s.clone()),
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
                .collect::<Result<_>>()?)
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
                        a.into_iter().map(|v| (k.clone(), v)).collect::<Vec<_>>()
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
                .collect::<Result<_>>()?,
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
                .collect::<Result<_>>()?,
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
                    .collect::<Result<_>>(),
                cel_interpreter::Value::Map(m) => Rc::try_unwrap(m.map)
                    .map_or_else(|arc| arc.as_ref().clone(), |val| val)
                    .into_iter()
                    .map(|(k, v)| Ok((k.into(), PlanData(v))))
                    .collect::<Result<_>>(),
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
    ctx.add_variable(
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
    ctx.add_variable("current", state.current().to_owned());
    ctx.add_variable("for", state.run_for().to_owned());
    ctx.add_variable("while", state.run_while().to_owned());
    ctx.add_variable("count", state.run_count().to_owned());
    ctx.add_function("parse_url", cel_functions::url);
    ctx.add_function(
        "parse_form_urlencoded",
        cel_functions::form_urlencoded_parts,
    );
    ctx.add_function("bytes", cel_functions::bytes);
    ctx.add_function("uint", cel_functions::uint);
}

pub trait Evaluate<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>;
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
    context.add_variable(
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
