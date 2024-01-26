use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

pub trait Merge: std::fmt::Debug + Clone + Serialize + Deserialize<'static> {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub courier: Settings,
    #[serde(flatten)]
    pub steps: IndexMap<String, Step>,
}

impl Plan {
    pub fn parse(input: &str) -> crate::Result<Plan> {
        let mut plan: Plan = toml::from_str(input).map_err(|e| crate::Error(e.to_string()))?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&mut self) -> crate::Result<()> {
        self.courier.validate()?;
        for (name, step) in &mut self.steps {
            step.validate()
                .map_err(|e| crate::Error(format!("validate step {}: {}", name, e)))?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub version: u16,
    #[serde(default)]
    pub defaults: Vec<Defaults>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Settings {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
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
    pub run: Option<Run>,
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

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
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
pub struct Step {
    #[serde(flatten)]
    pub unrecognized: toml::Table,
    #[serde(flatten)]
    pub protocols: StepProtocols,
    #[serde(default)]
    pub run: Option<Run>,
}

impl Step {
    pub fn apply_defaults<'a, I: IntoIterator<Item = Defaults>>(self, defaults: I) -> Self {
        let (run_defaults, proto_defaults): (Vec<_>, Vec<_>) = itertools::multiunzip(
            defaults
                .into_iter()
                .filter_map(|d| Some((d.run.clone(), d))),
        );
        Step {
            run: run_defaults.into_iter().fold(self.run, Run::merge),
            protocols: self.protocols.apply_defaults(proto_defaults),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&mut self) -> crate::Result<()> {
        match &self.protocols {
            StepProtocols::GraphQl { graphql, http } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("http");
                graphql.validate()?;
                if let Some(x) = &http {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("http1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                graphql.validate()?;
                if let Some(x) = &http1 {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("http2");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                graphql.validate()?;
                if let Some(x) = &http2 {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("http3");
                self.unrecognized.remove("quic");
                self.unrecognized.remove("udp");
                graphql.validate()?;
                if let Some(x) = &http3 {
                    x.validate()?;
                };
                if let Some(x) = &quic {
                    x.validate()?;
                };
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Http { http } => {
                self.unrecognized.remove("http");
                http.validate()?;
            }
            StepProtocols::Http1 { http1, tls, tcp } => {
                self.unrecognized.remove("http1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                http1.validate()?;
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
            }
            StepProtocols::Http2 { http2, tls, tcp } => {
                self.unrecognized.remove("http2");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                http2.validate()?;
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
            }
            StepProtocols::Http3 { http3, quic, udp } => {
                self.unrecognized.remove("http3");
                self.unrecognized.remove("quic");
                self.unrecognized.remove("udp");
                http3.validate()?;
                if let Some(x) = &quic {
                    x.validate()?;
                };
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Tls { tls, tcp } => {
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                tls.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
            }
            StepProtocols::Dtls { tls, udp } => {
                self.unrecognized.remove("tls");
                self.unrecognized.remove("udp");
                tls.validate()?;
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Tcp { tcp } => {
                self.unrecognized.remove("tcp");
                tcp.validate()?;
            }
            StepProtocols::Quic { quic, udp } => {
                self.unrecognized.remove("quic");
                self.unrecognized.remove("udp");
                quic.validate()?;
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Udp { udp } => {
                self.unrecognized.remove("udp");
                udp.validate()?;
            }
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepProtocols {
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

impl StepProtocols {
    pub fn apply_defaults<'a, I: IntoIterator<Item = Defaults>>(self, defaults: I) -> Self {
        // Apply defaults with a matching selector.
        let kind = self.kind();
        defaults
            .into_iter()
            .filter(|d| d.matches(kind))
            .fold(self, Self::merge)
    }

    #[inline]
    fn merge(self, default: Defaults) -> Self {
        match self {
            Self::GraphQl { graphql, http } => Self::GraphQl {
                graphql: GraphQl::merge(graphql, default.graphql),
                http: Some(http.unwrap_or_default().merge(default.http)),
            },
            Self::GraphQlHttp1 {
                graphql,
                http1,
                tls,
                tcp,
            } => Self::GraphQlHttp1 {
                graphql: graphql.merge(default.graphql),
                http1: Some(http1.unwrap_or_default().merge(default.http1)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
            },
            Self::GraphQlHttp2 {
                graphql,
                http2,
                tls,
                tcp,
            } => Self::GraphQlHttp2 {
                graphql: graphql.merge(default.graphql),
                http2: Some(http2.unwrap_or_default().merge(default.http2)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
            },

            Self::GraphQlHttp3 {
                graphql,
                http3,
                quic,
                udp,
            } => Self::GraphQlHttp3 {
                graphql: graphql.merge(default.graphql),
                http3: Some(http3.unwrap_or_default().merge(default.http3)),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Http { http } => Self::Http {
                http: http.merge(default.http),
            },
            Self::Http1 { http1, tls, tcp } => Self::Http1 {
                http1: http1.merge(default.http1),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
            },
            Self::Http2 { http2, tls, tcp } => Self::Http2 {
                http2: http2.merge(default.http2),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
            },
            Self::Http3 { http3, quic, udp } => Self::Http3 {
                http3: http3.merge(default.http3),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Tls { tls, tcp } => Self::Tls {
                tls: tls.merge(default.tls),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
            },
            Self::Tcp { tcp } => Self::Tcp {
                tcp: tcp.merge(default.tcp),
            },

            Self::Dtls { tls, udp } => Self::Dtls {
                tls: tls.merge(default.tls),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Udp { udp } => Self::Udp {
                udp: udp.merge(default.udp),
            },
            _ => unreachable!(),
        }
    }

    fn kind(&self) -> ProtocolKind {
        match self {
            Self::GraphQl { .. } => ProtocolKind::GraphQl,
            Self::GraphQlHttp1 { .. } => ProtocolKind::GraphQlHttp1,
            Self::GraphQlHttp2 { .. } => ProtocolKind::GraphQlHttp2,
            Self::GraphQlHttp3 { .. } => ProtocolKind::GraphQlHttp3,
            Self::Http { .. } => ProtocolKind::Http,
            Self::Http1 { .. } => ProtocolKind::Http1,
            Self::Http2 { .. } => ProtocolKind::Http2,
            Self::Http3 { .. } => ProtocolKind::Http3,
            Self::Tls { .. } => ProtocolKind::Tls,
            Self::Dtls { .. } => ProtocolKind::Dtls,
            Self::Tcp { .. } => ProtocolKind::Tcp,
            Self::Quic { .. } => ProtocolKind::Quic,
            Self::Udp { .. } => ProtocolKind::Udp,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Run {
    #[serde(rename = "if")]
    pub run_if: Option<Value>,
    #[serde(rename = "while")]
    pub run_while: Option<Value>,
    #[serde(rename = "for")]
    pub run_for: Option<Iterable>,
    pub count: Option<Value>,
    pub parallel: Option<Value>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Run {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else {
            return second;
        };
        let Some(second) = second else {
            return Some(first);
        };
        Some(Self {
            run_if: first.run_if.or(second.run_if),
            run_for: first.run_for.or(second.run_for),
            run_while: first.run_while.or(second.run_while),
            count: first.count.or(second.count),
            parallel: first.parallel.or(second.parallel),
            unrecognized: toml::Table::new(),
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GraphQl {
    pub url: Option<Value>,
    pub query: Option<Value>,
    pub params: Option<Table>,
    pub operation: Option<Value>,
    #[serde(default)]
    pub pause: Pause<GraphQlPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQlPause {
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for GraphQlPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };
        Some(GraphQlPause {
            unrecognized: toml::Table::new(),
        })
    }
}

impl GraphQlPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
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
    pub pause: Pause<HttpPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPause {
    pub open: Option<ValueOrArray<PauseValue>>,
    pub request_headers: Option<ValueOrArray<PauseValue>>,
    pub request_body: Option<ValueOrArray<PauseValue>>,
    pub response_headers: Option<ValueOrArray<PauseValue>>,
    pub response_body: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for HttpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(HttpPause {
            open: ValueOrArray::merge(first.open, second.open),
            request_headers: ValueOrArray::merge(first.request_headers, second.request_headers),
            request_body: ValueOrArray::merge(first.request_body, second.request_body),
            response_headers: ValueOrArray::merge(first.response_headers, second.response_headers),
            response_body: ValueOrArray::merge(first.response_body, second.response_body),
            unrecognized: toml::Table::new(),
        })
    }
}

impl HttpPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http1 {
    #[serde(flatten, default)]
    pub common: Http,
    #[serde(default)]
    pub pause: Pause<Http1Pause>,
}

impl Http1 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            common: self.common.merge(Some(default.common)),
            pause: Pause::merge(self.pause, default.pause),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http1Pause {
    pub open: Option<ValueOrArray<PauseValue>>,
    pub request_headers: Option<ValueOrArray<PauseValue>>,
    pub request_body: Option<ValueOrArray<PauseValue>>,
    pub response_headers: Option<ValueOrArray<PauseValue>>,
    pub response_body: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for Http1Pause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Http1Pause {
            open: ValueOrArray::merge(first.open, second.open),
            request_headers: ValueOrArray::merge(first.request_headers, second.request_headers),
            request_body: ValueOrArray::merge(first.request_body, second.request_body),
            response_headers: ValueOrArray::merge(first.response_headers, second.response_headers),
            response_body: ValueOrArray::merge(first.response_body, second.response_body),
            unrecognized: toml::Table::new(),
        })
    }
}

impl Http1Pause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http2 {
    #[serde(flatten)]
    pub common: Http,
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

    fn validate(&self) -> crate::Result<()> {
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http3 {
    #[serde(flatten)]
    pub common: Http,
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

    fn validate(&self) -> crate::Result<()> {
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tls {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub version: Option<Value>,
    #[serde(default)]
    pub pause: Pause<TlsPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsPause {
    pub handshake: Option<ValueOrArray<PauseValue>>,
    pub first_read: Option<ValueOrArray<PauseValue>>,
    pub first_write: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TlsPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(TlsPause {
            handshake: ValueOrArray::merge(first.handshake, second.handshake),
            first_read: ValueOrArray::merge(first.first_read, second.first_read),
            first_write: ValueOrArray::merge(first.first_write, second.first_write),
            unrecognized: toml::Table::new(),
        })
    }
}

impl TlsPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tcp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Pause<TcpPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpPause {
    pub handshake: Option<ValueOrArray<PauseValue>>,
    pub first_read: Option<ValueOrArray<PauseValue>>,
    pub first_write: Option<ValueOrArray<PauseValue>>,
    pub last_read: Option<ValueOrArray<PauseValue>>,
    pub last_write: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TcpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(TcpPause {
            handshake: ValueOrArray::merge(first.handshake, second.handshake),
            first_read: ValueOrArray::merge(first.first_read, second.first_read),
            first_write: ValueOrArray::merge(first.first_write, second.first_write),
            last_read: ValueOrArray::merge(first.last_read, second.last_read),
            last_write: ValueOrArray::merge(first.last_write, second.last_write),
            unrecognized: toml::Table::new(),
        })
    }
}

impl TcpPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Quic {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    pub tls_version: Option<Value>,
    #[serde(default)]
    pub pause: Pause<QuicPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicPause {
    pub handshake: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for QuicPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(QuicPause {
            handshake: ValueOrArray::merge(first.handshake, second.handshake),
            unrecognized: toml::Table::new(),
        })
    }
}

impl QuicPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Udp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub source_port: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Pause<UdpPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Udp {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            source_port: Value::merge(self.source_port, default.source_port),
            body: Value::merge(self.body, default.body),
            pause: Pause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause.before {
            p.validate()?;
        }
        if let Some(p) = &self.pause.after {
            p.validate()?;
        }
        if !self.pause.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.pause.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.pause.unrecognized.keys().join(", "),
            )));
        }
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpPause {
    pub first_read: Option<ValueOrArray<PauseValue>>,
    pub first_write: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for UdpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(UdpPause {
            first_read: ValueOrArray::merge(first.first_read, second.first_read),
            first_write: ValueOrArray::merge(first.first_write, second.first_write),
            unrecognized: toml::Table::new(),
        })
    }
}

impl UdpPause {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pause<T> {
    pub before: Option<T>,
    pub after: Option<T>,
    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl<T: Clone> Clone for Pause<T> {
    fn clone(&self) -> Self {
        Pause {
            before: self.before.clone(),
            after: self.after.clone(),
            unrecognized: self.unrecognized.clone(),
        }
    }
}
impl<T: Clone> Default for Pause<T> {
    fn default() -> Self {
        Pause {
            before: None,
            after: None,
            unrecognized: toml::Table::new(),
        }
    }
}

impl<T: Merge> Pause<T> {
    /// Merge two groups of pauses, with first groups taking presedence over second. If the same
    /// after tag is found in both groups, all entries with that after tag second are ignored.
    /// Otherwise, they are appended.
    fn merge(mut first: Pause<T>, second: Pause<T>) -> Pause<T> {
        first.before = T::merge(first.before, second.before);
        first.after = T::merge(first.after, second.after);
        first
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PauseValue {
    pub duration: Option<Value>,
    pub offset_bytes: Option<Value>,
    pub join: Option<ValueOrArray<String>>,
}

impl Merge for PauseValue {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(PauseValue {
            duration: Value::merge(first.duration, second.duration),
            offset_bytes: Value::merge(first.offset_bytes, second.offset_bytes),
            join: first.join.or(second.join),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueOrArray<T> {
    Value(T),
    Array(Vec<T>),
}

impl<T> Default for ValueOrArray<T> {
    fn default() -> Self {
        Self::Array(Vec::new())
    }
}

impl<T: Merge> Merge for ValueOrArray<T> {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        // Only merge single values.
        match (first, second) {
            (Some(Self::Value(first)), Some(Self::Value(second))) => Some(Self::Value(
                T::merge(Some(first), Some(second))
                    .expect("merging two set values should return a set value"),
            )),
            (Some(first), _) => Some(first),
            (_, second) => second,
        }
    }
}

impl<T> From<ValueOrArray<T>> for Vec<T> {
    fn from(value: ValueOrArray<T>) -> Self {
        match value {
            ValueOrArray::Value(val) => vec![val],
            ValueOrArray::Array(vec) => vec,
        }
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
    LiteralToml {
        literal: toml::Value,
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

impl Default for Value {
    fn default() -> Self {
        Self::Unset { unset: true }
    }
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
    Map(IndexMap<String, ValueOrArray<Value>>),
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
                // Flatten any array values into multiple records with the same key.
                // TODO: clean this up so its not duplicated converting bindings to plan format,
                // since this code won't be run if there are no defaults for a Table field.
                .flat_map(|(key, value)| match value {
                    ValueOrArray::Array(a) => a.into_iter().map(|v| (key.clone(), v)).collect(),
                    ValueOrArray::Value(value) => vec![(key, value)],
                })
                .map(|(key, value)| TableEntry {
                    key: Value::LiteralString(key),
                    value,
                })
                .collect(),
            Self::Array(a) => a,
        };
        // Merge second into the table-ized first, filtering keys already present.
        table.extend(match second {
            Some(Self::Map(m)) => m
                .into_iter()
                .filter(|(key, _)| {
                    !table
                        .iter()
                        .any(|x| matches!(&x.key, Value::LiteralString(k) if k == key))
                })
                .flat_map(|(key, value)| {
                    Vec::from(value).into_iter().map(move |v| TableEntry {
                        key: Value::LiteralString(key.clone()),
                        value: v,
                    })
                })
                .collect(),
            Some(Self::Array(a)) => a
                .into_iter()
                .filter(|x| table.iter().any(|y| x.key == y.key))
                .collect(),
            None => Vec::new(),
        });
        // Re-wrap the array as a Table.
        Some(Self::Array(table))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEntry {
    pub key: Value,
    #[serde(default)]
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Iterable {
    Array(Vec<toml::Value>),
    Map(IndexMap<String, toml::Value>),
    ExpressionCel {
        cel: String,
        vars: Option<IndexMap<String, String>>,
    },
    ExpressionVars {
        vars: IndexMap<String, String>,
    },
}
