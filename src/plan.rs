use crate::{
    bindings::{self, Defaults},
    Error, Result, State,
};
use base64::Engine;
use cel_interpreter::{Context, Program};
use indexmap::IndexMap;
use std::{collections::HashMap, ops::Deref, rc::Rc, time::Duration};

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
                selector: Some(bindings::Selector::Single("graphql".to_owned())),
                step: bindings::Step {
                    http: Some(bindings::HTTP {
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
            },
            Defaults {
                selector: None,
                step: bindings::Step {
                    http: Some(bindings::HTTP {
                        method: Some(bindings::Value::LiteralString("GET".to_owned())),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            },
        ]);

        // Generate final steps by combining with defaults.
        let steps: IndexMap<String, Step> = plan
            .steps
            .into_iter()
            .map(|(name, value)| Ok(Some((name, Step::from_bindings(name, value)?))))
            .filter_map(Result::transpose)
            .collect::<Result<_>>()?;

        Ok(Plan { steps })
    }
}

fn merge_toml(target: &mut toml::Value, defaults: &toml::Value) {
    let (toml::Value::Table(target), toml::Value::Table(defaults)) = (target, defaults) else {
        return;
    };
    for (name, default) in defaults {
        match target.entry(name) {
            toml::map::Entry::Vacant(entry) => {
                entry.insert(default.clone());
            }
            toml::map::Entry::Occupied(mut entry) => {
                merge_toml(entry.get_mut(), default);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TLSVersion {
    SSL1,
    SSL2,
    SSL3,
    TLS1_0,
    TLS1_1,
    TLS1_2,
    TLS1_3,
}

impl TLSVersion {
    pub fn try_from_str(s: &str) -> Result<TLSVersion> {
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

#[derive(Debug)]
pub enum HTTPVersion {
    HTTP0_9,
    HTTP1_0,
    HTTP1_1,
    HTTP2,
    HTTP3,
}

#[derive(Debug, Clone, Default)]
pub struct Pause {
    pub after: PlanValue<String>,
    pub duration: PlanValue<Duration>,
}

impl Pause {
    fn try_from_binding(binding: bindings::Pause) -> Result<Pause> {
        Ok(Pause {
            after: binding
                .after
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("pause.after is required"))??,
            duration: binding
                .duration
                .map(PlanValue::<Duration>::try_from_binding)
                .ok_or_else(|| Error::from("pause.duration is required"))??,
        })
    }
    fn process_toml(value: toml::Value) -> Result<Vec<Pause>> {
        let toml::Value::Array(list) = value else {
            return Err(Error::from("wrong type for pause"));
        };
        list.into_iter()
            .enumerate()
            .map(|(i, pause)| {
                let toml::Value::Table(mut table) = pause else {
                    return Err(Error::from(format!("wrong type for pause[{i}]")));
                };
                Ok(Pause {
                    after: table
                        .remove("after")
                        .map(PlanValue::process_toml)
                        .transpose()?
                        .flatten()
                        .ok_or_else(|| Error::from("pause.after is required"))?,
                    duration: table
                        .remove("duration")
                        .map(PlanValue::process_toml)
                        .transpose()?
                        .flatten()
                        .ok_or_else(|| Error::from("pause.duration is required"))?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTPRequest {
    pub url: PlanValue<String>,
    pub body: Option<PlanValue<Vec<u8>>>,
    pub method: Option<PlanValue<String>>,
    pub headers: PlanValueTable,

    pub pause: Vec<Pause>,
}

impl HTTPRequest {
    pub fn try_from_binding(binding: bindings::HTTP) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("http.url is required"))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from_binding)
                .transpose()?,
            method: binding
                .method
                .map(PlanValue::<String>::try_from_binding)
                .transpose()?,
            headers: PlanValueTable::try_from_binding(binding.headers.unwrap_or_default())?,
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from_binding)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTP1Request {}

impl HTTP1Request {
    pub fn try_from_binding(binding: bindings::HTTP1) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTP2Request {}

impl HTTP2Request {
    pub fn try_from_binding(binding: bindings::HTTP2) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTP3Request {}

impl HTTP3Request {
    pub fn try_from_binding(binding: bindings::HTTP3) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct GraphQLRequest {
    pub url: PlanValue<String>,
    pub query: PlanValue<String>,
    pub params: PlanValueTable,
    pub operation: Option<PlanValue<String>>,
    pub use_query_string: PlanValue<bool>,
    pub pause: Vec<Pause>,
}

impl GraphQLRequest {
    pub fn try_from_binding(binding: bindings::GraphQL) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("graphql.url is required"))??,
            query: binding
                .query
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("graphql.query is required"))??,
            params: PlanValueTable::try_from_binding(binding.params.unwrap_or_default())?,
            operation: binding
                .operation
                .map(PlanValue::<String>::try_from_binding)
                .transpose()?,
            use_query_string: binding
                .use_query_string
                .map(PlanValue::<bool>::try_from_binding)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(false)),
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from_binding)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct TCPRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub pause: Vec<Pause>,
}

impl TCPRequest {
    pub fn try_from_binding(binding: bindings::TCP) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("tcp.host is required"))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from_binding)
                .ok_or_else(|| Error::from("tcp.port is required"))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from_binding)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from_binding)
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
            _ => Err(Error::from("invalid type for string value")),
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
            _ => Err(Error::from("invalid type for 16 bit unsigned int value")),
        }
    }
}

impl TryFrom<PlanData> for bool {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bool(x) => Ok(x),
            _ => Err(Error::from("invalid type for bool value")),
        }
    }
}

impl TryFrom<PlanData> for Vec<u8> {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bytes(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::String(x) => Ok(x.deref().clone().into_bytes()),
            _ => Err(Error::from("invalid type for bytes value")),
        }
    }
}

impl TryFrom<PlanData> for Duration {
    type Error = Error;
    fn try_from(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(x) => {
                parse_duration::parse(&x).map_err(|e| Error(e.to_string()))
            }
            _ => Err(Error::from("invalid type for duration value")),
        }
    }
}

impl TryFrom<PlanData> for TLSVersion {
    type Error = Error;
    fn try_from(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) if *x == "SSL1" => Ok(TLSVersion::SSL1),
            cel_interpreter::Value::String(x) if *x == "SSL2" => Ok(TLSVersion::SSL2),
            cel_interpreter::Value::String(x) if *x == "SSL3" => Ok(TLSVersion::SSL3),
            cel_interpreter::Value::String(x) if *x == "TLS1_0" => Ok(TLSVersion::TLS1_0),
            cel_interpreter::Value::String(x) if *x == "TLS1_1" => Ok(TLSVersion::TLS1_1),
            cel_interpreter::Value::String(x) if *x == "TLS1_2" => Ok(TLSVersion::TLS1_2),
            cel_interpreter::Value::String(x) if *x == "TLS1_3" => Ok(TLSVersion::TLS1_3),
            _ => Err(Error("invalid TLS version".to_owned())),
        }
    }
}

impl TryFrom<toml::Value> for PlanData {
    type Error = Error;
    fn try_from(value: toml::Value) -> std::result::Result<Self, Self::Error> {
        Ok(PlanData(match value {
            toml::Value::String(x) => cel_interpreter::Value::String(Rc::new(x)),
            toml::Value::Integer(x) => cel_interpreter::Value::Int(
                i32::try_from(x).map_err(|e| Error::from(e.to_string()))?,
            ),
            toml::Value::Float(x) => cel_interpreter::Value::Float(x),
            toml::Value::Boolean(x) => cel_interpreter::Value::Bool(x),
            toml::Value::Datetime(x) => cel_interpreter::Value::Timestamp(
                chrono::DateTime::parse_from_rfc3339(&x.to_string())
                    .map_err(|e| Error(e.to_string()))?,
            ),
            toml::Value::Array(x) => cel_interpreter::Value::List(Rc::new(
                x.into_iter()
                    .map(|x| Ok(PlanData::try_from(x)?.0))
                    .collect::<Result<_>>()?,
            )),
            toml::Value::Table(x) => cel_interpreter::Value::Map(cel_interpreter::objects::Map {
                map: Rc::new(
                    x.into_iter()
                        .map(|(k, v)| Ok((k.into(), PlanData::try_from(v)?.0)))
                        .collect::<Result<_>>()?,
                ),
            }),
        }))
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
pub struct TLSRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub body: PlanValue<Vec<u8>>,
    pub version: Option<PlanValue<TLSVersion>>,
    pub pause: Vec<Pause>,
}

impl TLSRequest {
    pub fn try_from_binding(binding: bindings::TLS) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from_binding)
                .ok_or_else(|| Error::from("tls.host is required"))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from_binding)
                .ok_or_else(|| Error::from("tls.port is required"))??,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from_binding)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
            version: binding
                .body
                .map(PlanValue::<TLSVersion>::try_from_binding)
                .transpose()?,
            pause: binding
                .pause
                .into_iter()
                .map(Pause::try_from_binding)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct WebsocketRequest {}

impl TryFrom<toml::Value> for WebsocketRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error::from("invalid type"));
        };
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct QUICRequest {}

impl TryFrom<toml::Value> for QUICRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error::from("invalid type"));
        };
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct UDPRequest {}

impl TryFrom<toml::Value> for UDPRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error::from("invalid type"));
        };
        Ok(Self {})
    }
}

#[derive(Debug, Default, Clone)]
pub struct IPRequest {}

impl TryFrom<toml::Value> for IPRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error::from("invalid type"));
        };
        Ok(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct Step {
    pub name: String,
    pub graphql: Option<GraphQLRequest>,
    pub http: Option<HTTPRequest>,
    pub http1: Option<HTTP1Request>,
    pub http2: Option<HTTP2Request>,
    pub http3: Option<HTTP3Request>,
    pub tls: Option<TLSRequest>,
    pub tcp: Option<TCPRequest>,
}

impl Step {
    pub fn from_bindings(name: String, binding: bindings::Step) -> Result<Step> {
        let mut s = Self {
            name,
            graphql: binding
                .graphql
                .map(GraphQLRequest::try_from_binding)
                .transpose()?,
            http: binding
                .http
                .map(HTTPRequest::try_from_binding)
                .transpose()?,
            http1: binding
                .http1
                .map(HTTP1Request::try_from_binding)
                .transpose()?,
            http2: binding
                .http2
                .map(HTTP2Request::try_from_binding)
                .transpose()?,
            http3: binding
                .http3
                .map(HTTP3Request::try_from_binding)
                .transpose()?,
            tls: binding.tls.map(TLSRequest::try_from_binding).transpose()?,
            tcp: binding.tcp.map(TCPRequest::try_from_binding).transpose()?,
        };
        Ok(s)
    }
}

#[derive(Debug, Clone)]
pub enum Protocol {
    HTTP(HTTPRequest),
    HTTP1(HTTP1Request),
    HTTP2(HTTP2Request),
    HTTP3(HTTP3Request),
    TLS(TLSRequest),
    TCP(TCPRequest),
    //UDP {
    //    udp: UDPRequest,
    //},
    GraphQL(GraphQLRequest),
    //GRPC {
    //    grpc: GRPCRequest,
    //    http2: HTTP2Request,
    //},
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PlanValue<T: TryFrom<PlanData, Error = Error> + Clone> {
    Literal(T),
    Dynamic {
        template: String,
        vars: Vec<(String, String)>,
    },
}

impl<T: TryFrom<PlanData, Error = Error> + Clone + Default> Default for PlanValue<T> {
    fn default() -> Self {
        PlanValue::Literal(T::default())
    }
}

impl PlanValue<String> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralString(x) => Ok(Self::Literal(x)),
            _ => Err(Error(format!("invalid value {binding:?} for string field"))),
        }
    }
}
impl PlanValue<u16> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralInt(x) => {
                Ok(Self::Literal(x.try_into().map_err(|_| {
                    Error::from("out-of-bounds unsigned 16 bit integer literal")
                })?))
            }
            _ => Err(Error(format!(
                "invalid value {binding:?} for unsigned 16 bit integer field"
            ))),
        }
    }
}
impl PlanValue<bool> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralBool(x) => Ok(Self::Literal(x)),
            _ => Err(Error(format!(
                "invalid value {binding:?} for boolean field"
            ))),
        }
    }
}
impl PlanValue<Vec<u8>> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
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
impl PlanValue<Duration> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralString(x) => Ok(Self::Literal(Duration::from_nanos(
                go_parse_duration::parse_duration(x.as_str())
                    .map(TryInto::try_into)
                    .map_err(|_| Error::from("invalid value {binding:?} for duration field"))?
                    .map_err(|_| Error::from("duration cannot be negative"))?,
            ))),
            _ => Err(Error(format!(
                "invalid value {binding:?} for duration field"
            ))),
        }
    }
}
impl PlanValue<TLSVersion> {
    pub fn try_from_binding(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::LiteralString(x) => Ok(Self::Literal(
                TLSVersion::try_from_str(x.as_str())
                    .map_err(|_| Error::from("out-of-bounds unsigned 16 bit integer literal"))?,
            )),
            _ => Err(Error(format!(
                "invalid value {binding:?} for tls version field"
            ))),
        }
    }
}

impl<T: TryFrom<PlanData, Error = Error> + Clone> PlanValue<T> {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        match self.to_owned() {
            PlanValue::Literal(s) => Ok(s.clone()),
            Self::Dynamic { template, vars } => {
                let program =
                    Program::compile(template.as_str()).map_err(|e| Error(e.to_string()))?;
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
            Err("invalid _vars".into())
        }
    }
}

impl<T: TryFrom<PlanData, Error = Error> + Clone> PlanValue<T> {
    fn process_toml(val: toml::Value) -> Result<Option<Self>> {
        match val {
            toml::Value::String(s) => Ok(Some(Self::Literal(
                PlanData::from(cel_interpreter::Value::from(s))
                    .try_into()
                    .map_err(|e: Error| Error(e.to_string()))?,
            ))),
            toml::Value::Boolean(b) => Ok(Some(Self::Literal(
                PlanData::from(cel_interpreter::Value::from(b))
                    .try_into()
                    .map_err(|e: Error| Error(e.to_string()))?,
            ))),
            toml::Value::Table(mut t) => {
                if let Some(toml::Value::Boolean(unset)) = t.remove("unset") {
                    if unset {
                        return Ok(None);
                    }
                }
                if let Some(toml::Value::String(s)) = t.remove("template") {
                    Ok(Some(Self::Dynamic {
                        template: s,
                        vars: t
                            .remove("vars")
                            .map(Self::vars_from_toml)
                            .transpose()?
                            .unwrap_or_default(),
                    }))
                } else if let Some(toml::Value::String(s)) = t.remove("value") {
                    Ok(Some(Self::Literal(
                        PlanData(cel_interpreter::Value::from(s)).try_into()?,
                    )))
                } else {
                    Err(format!(
                        "value long-form must have unset, template, or value set {:?}",
                        t,
                    )
                    .into())
                }
            }
            _ => return Err("invalid value".into()),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PlanValueTable(pub Vec<(PlanValue<String>, Option<PlanValue<String>>)>);

impl PlanValueTable {
    pub fn try_from_binding(binding: bindings::Table) -> Result<Self> {
        Ok(PlanValueTable(match binding {
            bindings::Table::Map(m) => m
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        PlanValue::Literal(k),
                        v.map(|v| PlanValue::<String>::try_from_binding(v))
                            .transpose()?,
                    ))
                })
                .collect::<Result<_>>()?,
            bindings::Table::Array(a) => a
                .into_iter()
                .map(|entry| {
                    Ok(Some((
                        PlanValue::<String>::try_from_binding(entry.key)?,
                        entry
                            .value
                            .map(|v| PlanValue::<String>::try_from_binding(v))
                            .transpose()?,
                    )))
                })
                .filter_map(Result::transpose)
                .collect::<Result<_>>()?,
        }))
    }

    pub fn evaluate<'a, O, S, I>(&self, state: &S) -> Result<Vec<(String, Option<String>)>>
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
                    val.map(|v| v.evaluate(state)).transpose()?,
                ))
            })
            .collect()
    }
}

impl PlanValueTable {
    fn leaf_to_key_value(key: String, value: &mut toml::Value) -> Result<PlanValue<String>> {
        match value {
            // Strings or array values mean the key is not templated.
            toml::Value::String(_) | toml::Value::Array(_) => Ok(PlanValue::Literal(key)),
            // If the value is a table, check for the appropriate option to decide if the key is
            // templated.
            toml::Value::Table(t) => match t.remove("key_is_template") {
                Some(toml::Value::Boolean(b)) if b => Ok(PlanValue::Dynamic {
                    template: key,
                    vars: t
                        .get("vars")
                        .map(toml::Value::to_owned)
                        .map(PlanValue::<String>::vars_from_toml)
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
        ctx.add_variable(name, output);
    }
}
