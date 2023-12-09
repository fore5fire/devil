use std::{collections::HashMap, convert::identity};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Plan {
    #[serde(default)]
    pub courier: Settings,
    #[serde(flatten)]
    pub steps: IndexMap<String, Step>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Settings {
    pub defaults: Vec<Defaults>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Defaults {
    pub selector: Option<Selector>,
    #[serde(flatten)]
    pub step: Step,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProtocolKind {
    #[serde(rename = "graphql")]
    GraphQl,
    #[serde(rename = "graphqlhttp1")]
    GraphQlHttp1,
    #[serde(rename = "graphqlhttp2")]
    GraphQlHttp2,
    #[serde(rename = "graphqlhttp3")]
    GraphQlHttp3,
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "http1")]
    Http1,
    #[serde(rename = "http2")]
    Http2,
    #[serde(rename = "http3")]
    Http3,
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "dtls")]
    Dtls,
    #[serde(rename = "quic")]
    Quic,
    #[serde(rename = "udp")]
    Udp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Protocol {
    GraphQl(GraphQl),
    Http(Http),
    Http1(Http1),
    Http2(Http2),
    Http3(Http3),
    Tls(Tls),
    Tcp(Tcp),
    Quic(Quic),
    Udp(Udp),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Selector {
    Single(ProtocolKind),
    List(Vec<ProtocolKind>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Step {
    GraphQlHttp {
        graphql: Option<GraphQl>,
        http: Option<Http>,
    },
    GraphQlHttp1 {
        graphql: Option<GraphQl>,
        http1: Option<Http1>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    GraphQlHttp2 {
        graphql: Option<GraphQl>,
        http2: Option<Http2>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    GraphQlHttp3 {
        graphql: Option<GraphQl>,
        http3: Option<Http3>,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Http {
        http: Option<Http>,
    },
    Http1 {
        http1: Option<Http1>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    Http2 {
        http2: Option<Http2>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    Http3 {
        http3: Option<Http3>,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Tls {
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    Dtls {
        tls: Option<Tls>,
        udp: Option<Udp>,
    },
    Tcp {
        tcp: Option<Tcp>,
    },
    Quic {
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Udp {
        udp: Option<Udp>,
    },
}

impl Step {
    pub fn select() {}
    pub fn merge(steps: Vec<Step>) -> Step {
        assert!(!steps.is_empty());

        let (graphql, http, http1, http2, http3, tls, tcp, quic, udp): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = itertools::multiunzip(steps.into_iter().map(|x| {
            (
                x.graphql, x.http, x.http1, x.http2, x.http3, x.tls, x.tcp, x.quic, x.udp,
            )
        }));
        Step {
            graphql: GraphQl::merge(graphql),
            http: Http::merge(http),
            http1: Http1::merge(http1),
            http2: Http2::merge(http2),
            http3: Http3::merge(http3),
            tls: Tls::merge(tls),
            tcp: Tcp::merge(tcp),
            quic: Quic::merge(quic),
            udp: Udp::merge(udp),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GraphQl {
    pub url: Option<Value>,
    pub query: Option<Value>,
    pub params: Option<Table>,
    pub operation: Option<Value>,
    pub use_query_string: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl GraphQl {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (url, query, params, operation, use_query_string, pause): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = itertools::multiunzip(protos.into_iter().map(|x| {
            (
                x.url,
                x.query,
                x.params,
                x.operation,
                x.use_query_string,
                x.pause,
            )
        }));
        Some(Self {
            url: Value::merge(url),
            query: Value::merge(query),
            params: Table::merge(params),
            operation: Value::merge(operation),
            use_query_string: Value::merge(use_query_string),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Http {
    pub url: Option<Value>,
    pub body: Option<Value>,
    pub method: Option<Value>,
    pub version_string: Option<Value>,
    pub headers: Option<Table>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Http {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (url, headers, method, version_string, body, pause): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = itertools::multiunzip(protos.into_iter().map(|x| {
            (
                x.url,
                x.headers,
                x.method,
                x.version_string,
                x.body,
                x.pause,
            )
        }));
        Some(Self {
            url: Value::merge(url),
            version_string: Value::merge(version_string),
            headers: Table::merge(headers),
            method: Value::merge(method),
            body: Value::merge(body),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Http1 {
    #[serde(flatten, default)]
    pub common: Http,
}

impl Http1 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: Http::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Http2 {
    #[serde(flatten)]
    common: Http,
}

impl Http2 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: Http::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Http3 {
    #[serde(flatten)]
    common: Http,
}

impl Http3 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: Http::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Tls {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub version: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Tls {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (host, port, body, version, pause): (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(
                protos
                    .into_iter()
                    .map(|x| (x.host, x.port, x.body, x.version, x.pause)),
            );
        Some(Self {
            host: Value::merge(host),
            port: Value::merge(port),
            body: Value::merge(body),
            version: Value::merge(version),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Tcp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Tcp {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (host, port, body, pause): (Vec<_>, Vec<_>, Vec<_>, Vec<_>) = itertools::multiunzip(
            protos
                .into_iter()
                .map(|x| (x.host, x.port, x.body, x.pause)),
        );
        Some(Self {
            host: Value::merge(host),
            port: Value::merge(port),
            body: Value::merge(body),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Quic {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub tls_version: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Quic {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (host, port, body, version, pause): (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(
                protos
                    .into_iter()
                    .map(|x| (x.host, x.port, x.body, x.tls_version, x.pause)),
            );
        Some(Self {
            host: Value::merge(host),
            port: Value::merge(port),
            body: Value::merge(body),
            tls_version: Value::merge(version),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Udp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Udp {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (host, port, body, pause): (Vec<_>, Vec<_>, Vec<_>, Vec<_>) = itertools::multiunzip(
            protos
                .into_iter()
                .map(|x| (x.host, x.port, x.body, x.pause)),
        );
        Some(Self {
            host: Value::merge(host),
            port: Value::merge(port),
            body: Value::merge(body),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Pause {
    pub after: Option<Value>,
    pub duration: Option<Value>,
}
impl Pause {
    /// Merge groups of pauses, with earlier groups taking presedence over later ones. If the same
    /// after tag is found in multiple groups, all entries with that after tag from later groups
    /// are ignored. Otherwise, they are appended.
    fn merge<I: IntoIterator<Item = Vec<Self>>>(steps: I) -> Vec<Self> {
        let mut results = Vec::<Self>::new();
        for pauses in steps {
            for pause in pauses {
                // Only add pauses whose after doesn't match an existing entry.
                if results.iter().find(|p| p.after == pause.after).is_none() {
                    results.push(pause);
                }
            }
        }
        results
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    LiteralString(String),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralBool(bool),
    LiteralDatetime(toml::value::Datetime),
    LiteralArray(Vec<Value>),
    LiteralBase64 {
        base64: String,
    },
    Expression {
        template: Option<String>,
        vars: Option<Vec<(String, String)>>,
    },
    Unset {
        unset: bool,
    },
}

impl Value {
    /// Merge values, with earlier values taking presenence over later ones. For primitive types
    /// and arrays, the first non-empty value is used. For expressions, the first specified data is
    /// used for each field, except unset which indicates no more merging should be done.
    ///
    /// All non-primitive values stop merging after a primitive value or unset = true expression.
    fn merge(vals: Vec<Option<Self>>) -> Option<Self> {
        let mut tables = vals.into_iter().filter_map(identity);
        match tables.next()? {
            Self::Expression {
                mut template,
                mut vars,
            } => {
                // Merge defaults until we find one that's not an expression, ie. any expressions
                // overriden by a non-expression should be ignored.
                for t in tables {
                    match t {
                        Self::Expression {
                            template: t,
                            vars: v,
                        } => {
                            template = template.or(t);
                            vars = vars.or(v);
                        }
                        _ => return Some(Self::Expression { template, vars }),
                    }
                }
                return Some(Self::Expression { template, vars });
            }
            Self::Unset { unset } if unset => return None,
            Self::Unset { .. } => {
                // I guess we just ignore it if someone puts `unset = false`? I honestly would
                // return an error but I don't want to add extra error handling complexity to
                // the defaults system just for this one case.
                // Collect to a vec to break the recursive iterator cycle.
                Self::merge(tables.map(|x| Some(x)).collect())
            }
            x => return Some(x),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Table {
    Map(HashMap<String, Option<Value>>),
    Array(Vec<TableEntry>),
}

impl Default for Table {
    fn default() -> Self {
        Self::Array(Vec::new())
    }
}

impl Table {
    /// Merge tables, with earlier tables taking presedence over later ones. If the same key is
    /// found in multiple tables, all entries with that key from later groups are ignored.
    /// Otherwise, they are appended.
    fn merge<I: IntoIterator<Item = Option<Self>>>(tables: I) -> Option<Self> {
        let mut tables = tables.into_iter().filter_map(identity).peekable();
        tables.peek()?;

        let mut result = Vec::<TableEntry>::new();
        for t in tables {
            match t {
                Self::Map(m) => {
                    for (key, value) in m.into_iter() {
                        // Try to merge with an existing value.
                        if let Some(entry) = result.iter_mut().find(
                            |x| matches!(&x.key, Value::LiteralString(k) if k.as_str() == key),
                        ) {
                            entry.value = value;
                            continue;
                        }
                        // It can't be merged, so just append it.
                        result.push(TableEntry {
                            key: Value::LiteralString(key),
                            value,
                        })
                    }
                }
                Self::Array(a) => {
                    for row in a.into_iter() {
                        // Try to merge with an existing value.
                        if let Some(entry) = result.iter_mut().find(|x| x.key == row.key) {
                            entry.value = row.value;
                            continue;
                        }
                        // It can't be merged, so just append it.
                        result.push(row)
                    }
                }
            };
        }
        Some(Self::Array(result))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntry {
    pub key: Value,
    pub value: Option<Value>,
}
