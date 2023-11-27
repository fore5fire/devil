use crate::{Error, Result, State};
use cel_interpreter::{Context, Program};
use indexmap::IndexMap;
use serde::Deserialize;
use std::{collections::HashMap, iter::repeat, ops::Deref, rc::Rc, time::Duration};
use toml::Table;

#[derive(Debug)]
pub struct Plan {
    pub steps: IndexMap<String, Step>,
}

impl<'a> Plan {
    pub fn parse(input: &'a str) -> Result<Self> {
        let parsed = toml::from_str(input).map_err(|e| Error(e.to_string()))?;
        Self::from_value(parsed)
    }

    pub fn from_value(mut table: Table) -> Result<Self> {
        // Remove the special courier table.
        let toml::Value::Table(courier) = table
            .remove("courier")
            .unwrap_or_else(|| toml::Value::Table(Table::default()))
        else {
            return Err(Error::from("invalid type for courier table"));
        };

        let defaults = match courier.remove("defaults") {
            Some(toml::Value::Array(a)) => a,
            Some(_) => Error::from("courier.defaults must be an array"),
            None => Defaults::default(),
        };
        defaults
            .iter_mut()
            .filter_map(|(k, v)| match (k.as_str(), v) {
                ("http", toml::Value::Table(mut t)) => match t.remove("defaults") {
                    Some(toml::Value::Table(t)) => Some(Ok(t)),
                    None => None,
                    _ => Some(Err(Error::from("invalid type for courier.{}"))),
                },
                (k, _) => Some(Err(Error(format!("invalid key courier.{}", k)))),
            })
            .collect()?;

        // Apply the implicit defaults to the user defaults.
        merge_toml(
            &mut defaults,
            &toml::Value::Table(
                Table::try_from(HashMap::from([
                    (
                        "http",
                        toml::Value::from(HashMap::from([("method", toml::Value::from("GET"))])),
                    ),
                    (
                        "graphql",
                        toml::Value::from(HashMap::from([(
                            "http",
                            HashMap::from([
                                ("method", toml::Value::from("POST")),
                                (
                                    "headers",
                                    toml::Value::from(HashMap::from([(
                                        "Content-Type",
                                        "application/json",
                                    )])),
                                ),
                            ]),
                        )])),
                    ),
                ]))
                .map_err(|e| Error(e.to_string()))?,
            ),
        );

        // Parse all remaining tables as steps.
        let steps: IndexMap<String, Step> = table
            .into_iter()
            .map(|(name, value)| Ok(Some((name, Step::from_toml(name, value)?))))
            .filter_map(Result::transpose)
            .collect::<Result<_>>()?;

        Ok(Plan { steps })
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Defaults {
    pub selector: Option<String>,
    pub http: HTTPRequest,
    pub http1: HTTP1Request,
    pub http2: HTTP2Request,
    pub http3: HTTP3Request,
    pub tls: TLSSettings,
    pub tcp: TCPRequest,
    pub graphql: GraphQLRequest,
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
    pub body: Option<PlanValue<String>>,
    pub method: PlanValue<String>,
    pub headers: PlanValueTable,
    pub tls: TLSRequest,
    pub ip: IPRequest,

    pub pause: Vec<Pause>,
}

impl TryFrom<toml::Value> for HTTPRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error("invalid type".to_owned()));
        };
        Ok(HTTPRequest {
            url: protocol
                .remove("url")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("http.url is required"))?,
            body: protocol
                .remove("body")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten(),
            pause: protocol
                .remove("pause")
                .map(Pause::process_toml)
                .transpose()?
                .unwrap_or_default(),
            tls: protocol
                .remove("tls")
                .map(TLSRequest::try_from)
                .transpose()?
                .unwrap_or_default(),
            ip: protocol
                .remove("ip")
                .map(IPRequest::try_from)
                .transpose()?
                .unwrap_or_default(),
            method: protocol
                .remove("method")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("http.method is required"))?,
            headers: protocol
                .remove("headers")
                .map(PlanValueTable::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTP1Request {}

#[derive(Debug, Default, Clone)]
pub struct HTTP2Request {}

#[derive(Debug, Default, Clone)]
pub struct HTTP3Request {}

#[derive(Debug, Default, Clone)]
pub struct GraphQLRequest {
    pub url: PlanValue<String>,
    pub query: PlanValue<String>,
    pub params: PlanValueTable,
    pub operation: Option<PlanValue<String>>,
    pub use_query_string: PlanValue<bool>,
    pub pause: Vec<Pause>,
}

impl TryFrom<toml::Value> for GraphQLRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error("invalid type".to_owned()));
        };
        Ok(GraphQLRequest {
            url: protocol
                .remove("url")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("graphql.url is required"))?,
            query: protocol
                .remove("query")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("graphql.query is required"))?,
            params: protocol
                .remove("params")
                .map(PlanValueTable::try_from)
                .transpose()?
                .unwrap_or_default(),
            operation: protocol
                .remove("operation")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten(),
            use_query_string: protocol
                .remove("use_query_string")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .unwrap_or_default(),
            pause: protocol
                .remove("pause")
                .map(Pause::process_toml)
                .transpose()?
                .unwrap_or_default(),
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

impl TryFrom<toml::Value> for TCPRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error("invalid type".to_owned()));
        };
        Ok(Self {
            host: protocol
                .remove("host")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("tcp.host is required"))?,
            port: protocol
                .remove("port")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("tcp.port is required"))?,
            body: protocol
                .remove("body")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .unwrap_or_default(),
            pause: protocol
                .remove("pause")
                .map(Pause::process_toml)
                .transpose()?
                .unwrap_or_default(),
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

#[derive(Debug, Default, Clone)]
pub struct TLSRequest {
    pub port: PlanValue<String>,
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub pause: Vec<Pause>,
    pub version: Option<PlanValue<TLSVersion>>,
}

impl TryFrom<toml::Value> for TLSRequest {
    type Error = Error;
    fn try_from(value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut protocol) = value else {
            return Err(Error("invalid type".to_owned()));
        };
        Ok(Self {
            host: protocol
                .remove("host")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("tls.host is required"))?,
            port: protocol
                .remove("port")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .ok_or_else(|| Error::from("tls.port is required"))?,
            body: protocol
                .remove("body")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten()
                .unwrap_or_default(),
            pause: protocol
                .remove("pause")
                .map(Pause::process_toml)
                .transpose()?
                .unwrap_or_default(),
            version: protocol
                .remove("version")
                .map(PlanValue::process_toml)
                .transpose()?
                .flatten(),
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
    pub main: Protocol,
    pub graphql: Option<GraphQLRequest>,
    pub http: Option<HTTPRequest>,
    pub http1: Option<HTTPRequest>,
    pub http2: Option<HTTPRequest>,
    pub http3: Option<HTTPRequest>,
    pub tls: Option<TLSRequest>,
    pub tcp: Option<TCPRequest>,
}

impl Step {
    pub fn from_toml(name: String, value: toml::Value) -> Result<Step> {
        let toml::Value::Table(t) = value else {
            return Err(Error(format!("step {name} must be a table")));
        };
        let iter = t.into_iter();
        let mut s = Step {
            name,
            main: iter
                .next()
                .map(|(proto, value)| Protocol::from_toml(proto, value))
                .ok_or_else(|| Error(format!("step {name} missing protocol")))??,
            graphql: None,
            http: None,
            http1: None,
            http2: None,
            http3: None,
            tls: None,
            tcp: None,
        };
        for (name, proto) in iter {}
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

impl Protocol {
    pub fn from_toml(name: String, value: toml::Value) -> Result<Self> {
        let toml::Value::Table(mut table) = value else {
            return Err(Error::from("protocol must be a table"));
        };
        // The first step specified is the top-most protocol we're executing. All others must be
        // protocols that can run under the speicified protocol.
        let mut iter = table.keys();
        let name = iter
            .next()
            .ok_or_else(|| Error::from("step must contain at least one protocol"))?
            .clone();
        let mut proto = table.remove(&name).unwrap();
        Ok(match name.as_str() {
            "http" => Protocol::HTTP(HTTPRequest::try_from(proto)?),
            "http11" => Protocol::HTTP1(HTTP1Request {}),
            "http2" => Protocol::HTTP2(HTTP2Request {}),
            "http3" => Protocol::HTTP3(HTTP3Request {}),
            "graphql" => Protocol::GraphQL(GraphQLRequest::try_from(proto)?),
            "tcp" => Protocol::TCP(TCPRequest::try_from(proto)?),
            "tls" => Protocol::TLS(TLSRequest::try_from(proto)?),
            _ => {
                return Err(Error::from("no matching protocols"));
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PlanValue<T: TryFrom<PlanData, Error = Error> + Clone> {
    Literal(T),
    Dynamic {
        template: String,
        vars: Vec<(String, String)>,
    },
}

impl<'a> Deserialize<'a> for PlanValue<String> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        deserializer.deserialize_string(PlanValueStringVisitor)
    }
}

impl<T: TryFrom<PlanData, Error = Error> + Clone + Default> Default for PlanValue<T> {
    fn default() -> Self {
        PlanValue::Literal(T::default())
    }
}

impl<T: TryFrom<PlanData, Error = Error> + Clone> PlanValue<T> {
    pub fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where    {
        if s.len() >= self.min {
            Ok(s.to_owned())
        } else {
            Err(de::Error::invalid_value(Unexpected::Str(s), &self))
        }
    }
}
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
pub struct PlanValueTable(pub Vec<(PlanValue<String>, PlanValue<String>)>);

impl PlanValueTable {
    pub fn evaluate<'a, O, S, I>(&self, state: &S) -> Result<Vec<(String, String)>>
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

impl TryFrom<toml::Value> for PlanValueTable {
    type Error = Error;
    fn try_from(val: toml::Value) -> Result<Self> {
        Ok(PlanValueTable(match val {
            // Array syntax [{ key = "foo", value = "bar" }]
            toml::Value::Array(a) => a
                .into_iter()
                .map(|val| {
                    let toml::Value::Table(mut t) = val else {
                        return Err(Error::from("invalid type"));
                    };
                    let key = t
                        .remove("key")
                        .map(PlanValue::process_toml)
                        .transpose()?
                        .flatten()
                        .ok_or_else(|| Error::from("key is required"))?;
                    // The value can't just be missing entirely, but if the { unset = true } syntax
                    // is used we want to filter out the whole entry.
                    let value = t
                        .remove("value")
                        .ok_or_else(|| Error::from("value is required"))?;
                    let Some(value) = PlanValue::process_toml(value)? else {
                        return Ok(None);
                    };

                    Ok(Some((key, value)))
                })
                .filter_map(Result::transpose)
                .collect::<Result<_>>()?,
            // Table syntax { foo = "bar", foobar = ["foo", "bar"] }
            toml::Value::Table(t) => t
                .into_iter()
                .filter_map(|(name, mut value)| {
                    Some(Ok((
                        match Self::leaf_to_key_value(name, &mut value) {
                            Ok(key) => key,
                            Err(e) => return Some(Err(e)),
                        },
                        match value {
                            toml::Value::Array(list) => {
                                let result: Result<Vec<_>> = list
                                    .into_iter()
                                    .map(PlanValue::process_toml)
                                    .filter_map(Result::transpose)
                                    .collect();
                                match result {
                                    Ok(list) => list,
                                    Err(e) => return Some(Err(e)),
                                }
                            }
                            value => vec![match PlanValue::process_toml(value) {
                                Ok(Some(pv)) => pv,
                                // If there's no value then it was omitted with { unset = true },
                                // so filter the whole entry out.
                                Ok(None) => return None,
                                Err(e) => return Some(Err(e)),
                            }],
                        },
                    )))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                // Flat map the array from [(key, [foo, bar])] to [(key, foo), (key, bar)]
                .flat_map(|(name, values)| repeat(name).zip(values.into_iter()))
                .collect(),
            _ => return Err(Error::from("invalid map")),
        }))
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
