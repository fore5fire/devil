use std::{collections::HashMap, convert::identity};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Plan {
    pub courier: Settings,
    #[serde(flatten)]
    pub steps: HashMap<String, Step>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Settings {
    pub defaults: Vec<Defaults>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Defaults {
    pub selector: Option<Selector>,
    #[serde(flatten)]
    pub step: Step,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Selector {
    Single(String),
    List(Vec<String>),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Step {
    pub graphql: Option<GraphQL>,
    pub http: Option<HTTP>,
    pub http1: Option<HTTP1>,
    pub http2: Option<HTTP2>,
    pub http3: Option<HTTP3>,
    pub tls: Option<TLS>,
    pub tcp: Option<TCP>,
}

impl Step {
    pub fn merge(steps: &[Step]) -> Step {
        assert!(!steps.is_empty());

        let (graphql, http, http1, http2, http3, tls, tcp): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = itertools::multiunzip(
            steps
                .into_iter()
                .map(|x| (x.graphql, x.http, x.http1, x.http2, x.http3, x.tls, x.tcp)),
        );
        Step {
            graphql: GraphQL::merge(graphql),
            http: HTTP::merge(http),
            http1: HTTP1::merge(http1),
            http2: HTTP2::merge(http2),
            http3: HTTP3::merge(http3),
            tls: TLS::merge(tls),
            tcp: TCP::merge(tcp),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GraphQL {
    pub url: Option<Value>,
    pub query: Option<Value>,
    pub params: Option<Table>,
    pub operation: Option<Value>,
    pub use_query_string: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl GraphQL {
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
pub struct HTTP {
    pub url: Option<Value>,
    pub body: Option<Value>,
    pub method: Option<Value>,
    pub headers: Option<Table>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl HTTP {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        let (url, body, headers, method, pause): (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>) =
            itertools::multiunzip(
                protos
                    .into_iter()
                    .map(|x| (x.url, x.body, x.headers, x.method, x.pause)),
            );
        Some(Self {
            url: Value::merge(url),
            body: Value::merge(body),
            headers: Table::merge(headers),
            method: Value::merge(method),
            pause: Pause::merge(pause),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HTTP1 {
    #[serde(flatten, default)]
    common: HTTP,
}

impl HTTP1 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: HTTP::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HTTP2 {
    #[serde(flatten)]
    common: HTTP,
}

impl HTTP2 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: HTTP::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HTTP3 {
    #[serde(flatten)]
    common: HTTP,
}

impl HTTP3 {
    fn merge<I: IntoIterator<Item = Option<Self>>>(protos: I) -> Option<Self> {
        let mut protos = protos.into_iter().filter_map(identity).peekable();
        protos.peek()?;
        Some(Self {
            common: HTTP::merge(protos.map(|x| Some(x.common))).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TLS {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub version: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl TLS {
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
pub struct TCP {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl TCP {
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
    fn merge<I: IntoIterator<Item = Option<Self>>>(vals: I) -> Option<Self> {
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
                Some(Self::Expression { template, vars })
            }
            Self::Unset { unset } => {
                if unset {
                    None
                } else {
                    // I guess we just ignore it if someone puts `unset = false`? I honestly would
                    // return an error but I don't want to add extra error handling complexity to
                    // the defaults system just for this one case.
                    Self::merge(tables.map(|x| Some(x)))
                }
            }
            x => Some(x),
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
                    for (key, value) in m {
                        // Try to merge with an existing value.
                        if let Some(entry) =
                            result.iter().find(|x| x.key == Value::LiteralString(key))
                        {
                            entry.value;
                            continue;
                        }
                        // It can't be merged, so just append it.
                        result.push(TableEntry {
                            key: Value::LiteralString(key),
                            value,
                        })
                    }
                }
                Self::Array(a) => {}
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
