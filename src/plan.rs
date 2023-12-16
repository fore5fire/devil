use crate::{
    bindings::{self, Defaults, ProtocolKind},
    Error, RequestOutput, Result, State,
};
use base64::Engine;
use cel_interpreter::{Context, Program};
use chrono::Duration;
use go_parse_duration::parse_duration;
use indexmap::IndexMap;
use std::{collections::HashMap, ops::Deref, sync::Arc};
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
        // Apply the implicit defaults to the user defaults.
        plan.courier.defaults.extend([
            Defaults {
                selector: Some(bindings::Selector::List(vec![
                    ProtocolKind::GraphQl,
                    ProtocolKind::GraphQlHttp1,
                    ProtocolKind::GraphQlHttp2,
                    ProtocolKind::GraphQlHttp3,
                ])),

                http: Some(bindings::Http {
                    method: Some(bindings::Value::LiteralString("POST".to_owned())),
                    headers: Some(bindings::Table::Map(
                        [(
                            "Content-Type".to_owned(),
                            Some(bindings::Value::LiteralString(
                                "application/json".to_owned(),
                            )),
                        )]
                        .into(),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Defaults {
                selector: None,
                http: Some(bindings::Http {
                    method: Some(bindings::Value::LiteralString("GET".to_owned())),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]);

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

#[derive(Debug, Clone)]
pub struct Pause {
    pub after: PlanValue<String>,
    pub duration: PlanValue<Duration>,
}

impl TryFrom<bindings::Pause> for Pause {
    type Error = Error;
    fn try_from(binding: bindings::Pause) -> Result<Pause> {
        Ok(Pause {
            after: binding
                .after
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| Error("pause.after is required".to_owned()))??,
            duration: binding
                .duration
                .map(PlanValue::<Duration>::try_from)
                .ok_or_else(|| Error("pause.duration is required".to_owned()))??,
        })
    }
}

impl Pause {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::PauseOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let duration = self.duration.evaluate(state)?;
        // Ensure we can convert this to a standard duration for use in sleep later.
        if let Err(e) = duration.to_std() {
            return Err(Error(e.to_string()));
        }
        Ok(crate::PauseOutput {
            after: self.after.evaluate(state)?,
            duration,
        })
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub url: PlanValue<Url>,
    pub method: Option<PlanValue<Vec<u8>>>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub headers: PlanValueTable<Vec<u8>, Error, Vec<u8>, Error>,

    pub pause: Vec<Pause>,
}

impl TryFrom<bindings::Http> for HttpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Http) -> Result<Self> {
        println!("{binding:?}");
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
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl HttpRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::HttpRequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::HttpRequestOutput {
            url: self.url.evaluate(state)?,
            method: self
                .method
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?,
            headers: self
                .headers
                .evaluate(state)?
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
            body: self
                .body
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?
                .unwrap_or_default(),
            pause: self
                .pause
                .iter()
                .map(|p| p.evaluate(state))
                .collect::<Result<_>>()?,
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

    pub pause: Vec<Pause>,
}

impl Http1Request {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http1RequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http1RequestOutput {
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
            headers: self
                .headers
                .evaluate(state)?
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect(),
            body: self
                .body
                .as_ref()
                .map(|body| body.evaluate(state))
                .transpose()?
                .unwrap_or_default(),
            pause: self
                .pause
                .iter()
                .map(|p| p.evaluate(state))
                .collect::<crate::Result<_>>()?,
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
            pause: binding
                .common
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
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
    pub operation: Option<PlanValue<String>>,
    pub pause: Vec<Pause>,
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
                .map(PlanValue::<String>::try_from)
                .transpose()?,
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl GraphQlRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::GraphQlRequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::GraphQlRequestOutput {
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
                .map(|p| Ok::<_, crate::Error>(p.evaluate(state)?.into_iter().collect()))
                .transpose()?,
            pause: self
                .pause
                .iter()
                .map(|p| p.evaluate(state))
                .collect::<crate::Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct TcpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: Vec<Pause>,
}

impl TcpRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::TcpRequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TcpRequestOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            pause: self
                .pause
                .iter()
                .map(|p| p.evaluate(state))
                .collect::<crate::Result<_>>()?,
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
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
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
            return Err(Error("TLS version must be a string".to_owned()));
        };
        Url::parse(&x).map_err(|e| Error(e.to_string()))
    }
}

impl TryFrom<PlanData> for serde_json::Value {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        Ok(match value.0 {
            cel_interpreter::Value::List(l) => Self::Array(
                l.into_iter()
                    .map(PlanData)
                    .map(Self::try_from)
                    .collect::<Result<_>>()?,
            ),
            cel_interpreter::Value::Map(m) => Self::Object(
                m.map
                    .into_iter()
                    .map(|(k, v)| {
                        let cel_interpreter::objects::Key::String(k) = k else {
                            return Err(Error(
                                "only string keys may be used in json output".to_owned(),
                            ));
                        };
                        Ok((*k, Self::try_from(PlanData(v))?))
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
            cel_interpreter::Value::String(s) => Self::String(*s),
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
    pub pause: Vec<Pause>,
}

impl TlsRequest {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::TlsRequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TlsRequestOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            pause: self
                .pause
                .iter()
                .map(|p| p.evaluate(state))
                .collect::<crate::Result<_>>()?,
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
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
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
    pub pause: Vec<Pause>,
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
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct UdpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: Vec<Pause>,
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
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from)
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
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<RequestOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(match self {
            Self::GraphQl(proto) => RequestOutput::GraphQl(proto.evaluate(state)?),
            Self::Http(proto) => RequestOutput::Http(proto.evaluate(state)?),
            Self::Http1(proto) => RequestOutput::Http1(proto.evaluate(state)?),
            //Self::HTTp2(proto) => RequestOutput::Http2(proto.evaluate(state)?),
            //Self::Http3(proto) => RequestOutput::Http3(proto.evaluate(state)?),
            Self::Tls(proto) => RequestOutput::Tls(proto.evaluate(state)?),
            Self::Tcp(proto) => RequestOutput::Tcp(proto.evaluate(state)?),
            //Self::Quic(proto) => RequestOutput::Quic(proto.evaluate(state)?),
            //Self::Udp(proto) => RequestOutput::Udp(proto.evaluate(state)?),
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
    E: std::error::Error,
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

impl TryFrom<bindings::Value> for PlanValue<String> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralString(x) => Ok(Self::Literal(x)),
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
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 16 bit integer field"
            ))),
        }
    }
}
impl TryFrom<bindings::Value> for PlanValue<bool> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralBool(x) => Ok(Self::Literal(x)),
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
                    .map_err(|_| Error(format!("invalid duration string {binding:?}")))?,
            )),
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
            _ => Err(Error(format!("invalid value {binding:?} for json field"))),
        }
    }
}

impl<T, E> PlanValue<T, E>
where
    T: TryFrom<PlanData, Error = E> + Clone,
    E: std::error::Error,
{
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        match self.to_owned() {
            PlanValue::Literal(s) => Ok(s.clone()),
            Self::Dynamic { cel, vars } => {
                let program = Program::compile(cel.as_str()).map_err(|e| Error(e.to_string()))?;
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
                        .map_err(|e| Error(e.to_string()))?,
                )
                .try_into()
                .map_err(|e: E| Error(e.to_string()))
            }
        }
    }

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
pub struct PlanValueTable<K, KE, V, VE>(pub Vec<(PlanValue<K, KE>, Option<PlanValue<V, VE>>)>)
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
                .map(|(k, v)| {
                    Ok((
                        k.into(),
                        v.map(PlanValue::<V, VE>::try_from)
                            .transpose()
                            .map_err(|e| Error(e.to_string()))?,
                    ))
                })
                .collect::<Result<_>>()?,
            bindings::Table::Array(a) => a
                .into_iter()
                .map(|entry| {
                    Ok((
                        PlanValue::try_from(entry.key).map_err(|e: KE2| Error(e.to_string()))?,
                        entry
                            .value
                            .map(PlanValue::<V, VE>::try_from)
                            .transpose()
                            .map_err(|e| Error(e.to_string()))?,
                    ))
                })
                .collect::<Result<_>>()?,
        }))
    }
}

impl<K, KE, V, VE> PlanValueTable<K, KE, V, VE>
where
    K: TryFrom<PlanData, Error = KE> + Clone,
    KE: std::error::Error,
    V: TryFrom<PlanData, Error = VE> + Clone,
    VE: std::error::Error,
{
    pub fn evaluate<'a, O, S, I>(&self, state: &S) -> Result<Vec<(K, Option<V>)>>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        self.0
            .iter()
            .map(|(key, val)| {
                Ok((
                    key.evaluate(state)?,
                    val.as_ref().map(|v| v.evaluate(state)).transpose()?,
                ))
            })
            .collect()
    }

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
    for name in state.iter() {
        let name = name.into();
        let output = state.get(name).unwrap();
        ctx.add_variable(name, output.to_owned());
    }
}
