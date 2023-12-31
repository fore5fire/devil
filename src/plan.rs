use crate::{
    bindings, Error, GraphQlPauseOutput, Http1PauseOutput, HttpPauseOutput, Result, State,
    StepPlanOutput, TcpPauseOutput, TlsPauseOutput,
};
use base64::Engine;
use cel_interpreter::extractors::This;
use cel_interpreter::{Context, FunctionContext, Program, ResolveResult};
use chrono::Duration;
use go_parse_duration::parse_duration;
use indexmap::IndexMap;
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
pub enum Step {
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

impl Step {
    pub fn from_bindings(binding: bindings::Step) -> Result<Step> {
        match binding {
            bindings::Step::GraphQl { graphql, http } => Ok(Step::GraphQlHttp {
                graphql: graphql.try_into()?,
                http: http.unwrap_or_default().try_into()?,
            }),
            // If HTTP1, TLS, or TCP is specified we use HTTP1.
            bindings::Step::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => Ok(Step::GraphQlHttp1 {
                graphql: graphql.try_into()?,
                http1: http1.unwrap_or_default().try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => Ok(Step::GraphQlHttp2 {
                graphql: graphql.try_into()?,
                http2: http2.unwrap_or_default().try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => Ok(Step::GraphQlHttp3 {
                graphql: graphql.try_into()?,
                http3: http3.unwrap_or_default().try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Http { http } => Ok(Step::Http {
                http: http.try_into()?,
            }),
            bindings::Step::Http1 { http1, tls, tcp } => Ok(Step::Http1 {
                http1: http1.try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Http2 { http2, tls, tcp } => Ok(Step::Http2 {
                http2: http2.try_into()?,
                tls: tls.map(TlsRequest::try_from).transpose()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Http3 { http3, quic, udp } => Ok(Step::Http3 {
                http3: http3.try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Tls { tls, tcp } => Ok(Step::Tls {
                tls: tls.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Dtls { tls, udp } => Ok(Step::Dtls {
                tls: tls.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Tcp { tcp } => Ok(Step::Tcp {
                tcp: tcp.try_into()?,
            }),
            bindings::Step::Quic { quic, udp } => Ok(Step::Quic {
                quic: quic.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            }),
            bindings::Step::Udp { udp } => Ok(Step::Udp {
                udp: udp.try_into()?,
            }),
        }
    }

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
            bindings::Value::LiteralStruct { r#struct: x } => Ok(Self::Literal(
                serde_json::to_value(x).map_err(|e| Error(e.to_string()))?,
            )),
            bindings::Value::LiteralBase64 { base64 } => Ok(Self::Literal(
                base64::prelude::BASE64_STANDARD_NO_PAD
                    .decode(base64)
                    .map_err(|e| Error(format!("base64 decode: {}", e)))?
                    .into(),
            )),
            bindings::Value::LiteralArray(x) => Ok(Self::Literal(
                // Dirty hack: recursively try_from and then unwrap to get the json array elements.
                x.into_iter()
                    .map(Self::try_from)
                    .map(|x| {
                        Ok(match x? {
                            Self::Literal(l) => l,
                            Self::Dynamic { .. } => unreachable!(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
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
        match self.to_owned() {
            PlanValue::Literal(s) => Ok(s.clone()),
            Self::Dynamic { cel, vars } => {
                let program = Program::compile(cel.as_str())
                    .map_err(|e| Error(format!("compile cel {}: {}", cel, e)))?;
                let mut context = Context::default();
                context.add_variable(
                    "vars",
                    vars.into_iter()
                        .map(|(name, value)| (name.clone().into(), value.clone().into()))
                        .collect::<HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>>(
                        ),
                );
                add_state_to_context(state, &mut context);
                PlanData(
                    program
                        .execute(&context)
                        .map_err(|e| Error(format!("execute cel {}: {}", cel, e)))?,
                )
                .try_into()
                .map_err(|e: E| Error(e.to_string()))
            }
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
                    bindings::Value::LiteralArray(a) => {
                        a.into_iter().map(|v| (k.clone(), v)).collect::<Vec<_>>()
                    }
                    v => vec![(k, v)],
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
                    let Some(value) = entry.value else {
                        return Ok(None);
                    };
                    if let bindings::Value::Unset { .. } = value {
                        return Ok(None);
                    }
                    Ok(Some((
                        PlanValue::try_from(entry.key).map_err(|e: KE2| Error(e.to_string()))?,
                        PlanValue::try_from(value).map_err(|e: VE2| Error(e.to_string()))?,
                    )))
                })
                .filter_map(Result::transpose)
                .collect::<Result<_>>()?,
        }))
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
            .map(|name| (name, state.get(name).unwrap().to_owned()))
            .collect::<HashMap<_, _>>(),
    );
    ctx.add_variable("current", state.current().to_owned());
    ctx.add_function("parse_url", url);
    ctx.add_function("parse_form_urlencoded", form_urlencoded_parts);
}

fn url(ftx: &FunctionContext, This(url): This<Arc<String>>) -> ResolveResult {
    let url = Url::parse(&url).map_err(|e| ftx.error(&e.to_string()))?;
    Ok(url_to_cel(url))
}

fn url_to_cel(url: Url) -> cel_interpreter::Value {
    cel_interpreter::Value::Map(cel_interpreter::objects::Map {
        map: Rc::new(HashMap::from([
            ("scheme".into(), url.scheme().into()),
            ("username".into(), url.username().into()),
            ("password".into(), url.password().into()),
            ("host".into(), url.host_str().into()),
            ("port".into(), url.port().map(|x| x as u64).into()),
            (
                "port_or_default".into(),
                url.port_or_known_default().map(|x| x as u64).into(),
            ),
            ("path".into(), url.path().into()),
            (
                "path_segments".into(),
                url.path_segments().map(|x| x.collect::<Vec<_>>()).into(),
            ),
            ("query".into(), url.query().into()),
            ("fragment".into(), url.fragment().into()),
        ])),
    })
}

fn form_urlencoded_parts(This(query): This<Arc<String>>) -> Arc<Vec<cel_interpreter::Value>> {
    Arc::new(
        form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .map(|(k, v)| {
                cel_interpreter::Value::Map(cel_interpreter::objects::Map {
                    map: Rc::new(HashMap::from([
                        ("key".into(), k.into()),
                        ("value".into(), v.into()),
                    ])),
                })
            })
            .collect(),
    )
}

pub trait Evaluate<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>;
}
