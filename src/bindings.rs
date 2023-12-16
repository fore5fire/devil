use std::collections::HashMap;

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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Defaults {
    pub selector: Option<Selector>,
    pub graphql: Option<GraphQl>,
    pub http: Option<Http>,
    pub http1: Option<Http1>,
    pub http2: Option<Http2>,
    pub http3: Option<Http3>,
    pub tls: Option<Tls>,
    pub tcp: Option<Tcp>,
    pub quic: Option<Quic>,
    pub dtls: Option<Tls>,
    pub udp: Option<Udp>,
}

impl Defaults {
    fn matches(&self, kind: ProtocolKind) -> bool {
        match &self.selector {
            None => true,
            Some(Selector::Single(k)) => *k == kind,
            Some(Selector::List(l)) => l.contains(&kind),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Selector {
    Single(ProtocolKind),
    List(Vec<ProtocolKind>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Step {
    GraphQl {
        graphql: GraphQl,
        http: Option<Http>,
    },
    GraphQlHttp1 {
        graphql: GraphQl,
        http1: Option<Http1>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    GraphQlHttp2 {
        graphql: GraphQl,
        http2: Option<Http2>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    GraphQlHttp3 {
        graphql: GraphQl,
        http3: Option<Http3>,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Http {
        http: Http,
    },
    Http1 {
        http1: Http1,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    Http2 {
        http2: Http2,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
    },
    Http3 {
        http3: Http3,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Tls {
        tls: Tls,
        tcp: Option<Tcp>,
    },
    Dtls {
        tls: Tls,
        udp: Option<Udp>,
    },
    Tcp {
        tcp: Tcp,
    },
    Quic {
        quic: Quic,
        udp: Option<Udp>,
    },
    Udp {
        udp: Udp,
    },
}

impl Step {
    pub fn apply_defaults<'a, I: IntoIterator<Item = Defaults>>(mut self, defaults: I) -> Self {
        for d in defaults {
            self = self.merge(d);
        }
        self
    }

    #[inline]
    fn merge(self, default: Defaults) -> Self {
        match self {
            Self::GraphQl { graphql, http } => Self::GraphQl {
                graphql: GraphQl::merge(graphql, default.graphql),
                http: http.map(|http| http.merge(default.http)),
            },
            Self::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => Self::GraphQlHttp1 {
                graphql: graphql.merge(default.graphql),
                http1: http1.map(|x| x.merge(default.http1)),
                tls: tls.map(|x| x.merge(default.tls)),
                tcp: tcp.map(|x| x.merge(default.tcp)),
            },
            Self::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => Self::GraphQlHttp2 {
                graphql: graphql.merge(default.graphql),
                http2: http2.map(|x| x.merge(default.http2)),
                tls: tls.map(|x| x.merge(default.tls)),
                tcp: tcp.map(|x| x.merge(default.tcp)),
            },

            Self::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => Self::GraphQlHttp3 {
                graphql: graphql.merge(default.graphql),
                http3: http3.map(|x| x.merge(default.http3)),
                quic: quic.map(|x| x.merge(default.quic)),
                udp: udp.map(|x| x.merge(default.udp)),
            },
            Self::Http { http } => Self::Http {
                http: http.merge(default.http),
            },
            Self::Http1 { http1, tls, tcp } => Self::Http1 {
                http1: http1.merge(default.http1),
                tls: tls.map(|x| x.merge(default.tls)),
                tcp: tcp.map(|x| x.merge(default.tcp)),
            },
            Self::Http2 { http2, tls, tcp } => Self::Http2 {
                http2: http2.merge(default.http2),
                tls: tls.map(|x| x.merge(default.tls)),
                tcp: tcp.map(|x| x.merge(default.tcp)),
            },
            Self::Http3 { http3, quic, udp } => Self::Http3 {
                http3: http3.merge(default.http3),
                quic: quic.map(|x| x.merge(default.quic)),
                udp: udp.map(|x| x.merge(default.udp)),
            },
            Self::Tls { tls, tcp } => Self::Tls {
                tls: tls.merge(default.tls),
                tcp: tcp.map(|x| x.merge(default.tcp)),
            },
            Self::Tcp { tcp } => Self::Tcp {
                tcp: tcp.merge(default.tcp),
            },

            Self::Dtls { tls, udp } => Self::Dtls {
                tls: tls.merge(default.tls),
                udp: udp.map(|x| x.merge(default.udp)),
            },
            Self::Udp { udp } => Self::Udp {
                udp: udp.merge(default.udp),
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GraphQl {
    pub url: Option<Value>,
    pub query: Option<Value>,
    pub params: Option<Table>,
    pub operation: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl GraphQl {
    fn merge(self, second: Option<Self>) -> Self {
        let Some(second) = second else {
            return self;
        };
        Self {
            url: Value::merge(self.url, second.url),
            query: Value::merge(self.query, second.query),
            params: Table::merge(self.params, second.params),
            operation: Value::merge(self.operation, second.operation),
            pause: Pause::merge(self.pause, second.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
    fn merge(self, second: Option<Self>) -> Self {
        let Some(second) = second else {
            return self;
        };
        Self {
            url: Value::merge(self.url, second.url),
            version_string: Value::merge(self.version_string, second.version_string),
            headers: Table::merge(self.headers, second.headers),
            method: Value::merge(self.method, second.method),
            body: Value::merge(self.body, second.body),
            pause: Pause::merge(self.pause, second.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http1 {
    #[serde(flatten, default)]
    pub common: Http,
}

impl Http1 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            common: self.common.merge(Some(default.common)),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http2 {
    #[serde(flatten)]
    common: Http,
}

impl Http2 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            common: self.common.merge(Some(default.common)),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http3 {
    #[serde(flatten)]
    common: Http,
}

impl Http3 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            common: self.common.merge(Some(default.common)),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tls {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub version: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Tls {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            body: Value::merge(self.body, default.body),
            version: Value::merge(self.version, default.version),
            pause: Pause::merge(self.pause, default.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tcp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Tcp {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            body: Value::merge(self.body, default.body),
            pause: Pause::merge(self.pause, default.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Quic {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub tls_version: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Quic {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            body: Value::merge(self.body, default.body),
            tls_version: Value::merge(self.tls_version, default.tls_version),
            pause: Pause::merge(self.pause, default.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Udp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Vec<Pause>,
}

impl Udp {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            body: Value::merge(self.body, default.body),
            pause: Pause::merge(self.pause, default.pause),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Pause {
    pub after: Option<Value>,
    pub duration: Option<Value>,
}
impl Pause {
    /// Merge two groups of pauses, with first groups taking presedence over second. If the same
    /// after tag is found in both groups, all entries with that after tag second are ignored.
    /// Otherwise, they are appended.
    fn merge(mut first: Vec<Pause>, second: Vec<Pause>) -> Vec<Pause> {
        for pause in second {
            // Only add pauses whose after doesn't match an existing entry.
            if first.iter().find(|p| p.after == pause.after).is_none() {
                first.push(pause);
            }
        }
        first
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    LiteralString(String),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralBool(bool),
    LiteralDatetime(toml::value::Datetime),
    LiteralArray(Vec<Value>),
    LiteralStruct {
        r#struct: toml::Table,
    },
    LiteralBase64 {
        base64: String,
    },
    Unset {
        unset: bool,
    },
    ExpressionCel {
        cel: String,
        vars: Option<IndexMap<String, String>>,
    },
    ExpressionVars {
        vars: IndexMap<String, String>,
    },
}

impl Value {
    /// Merge values, with taking presenence over second. For primitive types and arrays, the
    /// earliest non-empty value is used. For expressions, the earlier specified data is used for
    /// each field, except unset which indicates no more merging should be done.
    ///
    /// All non-primitive values stop merging after a primitive value or unset = true expression.
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Value> {
        match (first, second) {
            (None, second) => second,
            // Merge individual fields if both are expressions.
            (
                Some(Self::ExpressionCel { cel, vars }),
                Some(Self::ExpressionCel {
                    vars: second_vars, ..
                }),
            ) => Some(Self::ExpressionCel {
                cel,
                vars: Some(Self::merge_vars(vars, second_vars)),
            }),
            (
                Some(Self::ExpressionCel { cel, vars }),
                Some(Self::ExpressionVars { vars: second_vars }),
            ) => Some(Self::ExpressionCel {
                cel,
                vars: Some(Self::merge_vars(vars, Some(second_vars))),
            }),
            (
                Some(Self::ExpressionVars { vars }),
                Some(Self::ExpressionCel {
                    cel,
                    vars: second_vars,
                }),
            ) => Some(Self::ExpressionCel {
                cel,
                vars: Some(Self::merge_vars(Some(vars), second_vars)),
            }),
            (
                Some(Self::ExpressionVars { vars }),
                Some(Self::ExpressionVars { vars: second_vars }),
            ) => Some(Self::ExpressionVars {
                vars: Self::merge_vars(Some(vars), Some(second_vars)),
            }),
            (Some(Self::Unset { unset }), _) if unset => Some(Value::Unset { unset }),
            // I guess we just ignore it if someone puts `unset = false`? I honestly would
            // return an error but I don't want to add extra error handling complexity to
            // the defaults system just for this one case.
            (Some(Self::Unset { .. }), second) => second,
            (Some(x), _) => Some(x),
        }
    }

    fn merge_vars(
        first: Option<IndexMap<String, String>>,
        second: Option<IndexMap<String, String>>,
    ) -> IndexMap<String, String> {
        let Some(mut first) = first else {
            return second.unwrap_or_default();
        };
        let Some(second) = second else {
            return first;
        };
        for (k, v) in second {
            if first.iter().find(|(key, _)| **key == k).is_none() {
                first.insert(k, v);
            }
        }
        first
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Merge tables, with entries in first taking prescedence over second. If the same key is
    /// found in both tables, all entries with that key from second are ignored. Otherwise, they
    /// are appended.
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(result) = first else {
            return second;
        };
        // Convert first to array format if needed.
        let mut table = match result {
            Self::Map(m) => m
                .into_iter()
                .map(|(key, value)| TableEntry {
                    key: Value::LiteralString(key),
                    value,
                })
                .collect(),
            Self::Array(a) => a,
        };
        // Merge second into the table-ized first.
        match second {
            Some(Self::Map(m)) => {
                for (key, value) in m.into_iter() {
                    // Try to merge with an existing value.
                    if let Some(entry) = table
                        .iter_mut()
                        .find(|x| matches!(&x.key, Value::LiteralString(k) if k.as_str() == key))
                    {
                        entry.value = value;
                        continue;
                    }
                    // It can't be merged, so just append it.
                    table.push(TableEntry {
                        key: Value::LiteralString(key),
                        value,
                    });
                }
            }
            Some(Self::Array(a)) => {
                for row in a.into_iter() {
                    // Try to merge with an existing value.
                    if let Some(entry) = table.iter_mut().find(|x| x.key == row.key) {
                        entry.value = row.value;
                        continue;
                    }
                    // It can't be merged, so just append it.
                    table.push(row)
                }
            }
            None => {}
        }
        // Re-wrap the array as a Table.
        Some(Self::Array(table))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEntry {
    pub key: Value,
    pub value: Option<Value>,
}
