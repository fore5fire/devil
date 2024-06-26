use std::collections::HashMap;

use indexmap::IndexMap;
use itertools::{Either, Itertools};
use serde::{Deserialize, Serialize};

pub trait Merge: std::fmt::Debug + Clone + Serialize + Deserialize<'static> {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub devil: Settings,
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
        self.devil.validate()?;
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
    #[serde(default)]
    pub locals: IndexMap<String, Value>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Settings {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            return Err(crate::Error(format!(
                "unrecognized field devil.{} {}",
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
    pub h1c: Option<Http1>,
    pub h1: Option<Http1>,
    pub h2c: Option<Http2>,
    pub h2: Option<Http2>,
    pub http2_frames: Option<Http2Frames>,
    pub h3: Option<Http3>,
    pub tls: Option<Tls>,
    pub tcp: Option<Tcp>,
    pub tcp_segments: Option<TcpSegments>,
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
    #[serde(rename = "graphqlh1c")]
    GraphQlH1c,
    #[serde(rename = "graphqlh1")]
    GraphQlH1,
    #[serde(rename = "graphqlh2c")]
    GraphQlH2c,
    #[serde(rename = "graphqlh2")]
    GraphQlH2,
    #[serde(rename = "graphqlh3")]
    GraphQlH3,
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "h1c")]
    H1c,
    #[serde(rename = "h1")]
    H1,
    #[serde(rename = "h2c")]
    H2c,
    #[serde(rename = "h2")]
    H2,
    #[serde(rename = "h3")]
    H3,
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tcp_segments")]
    TcpSegments,
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
            StepProtocols::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h1c");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                graphql.validate()?;
                if let Some(x) = &h1c {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                graphql.validate()?;
                if let Some(x) = &h1 {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlH2c {
                graphql,
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h2c");
                self.unrecognized.remove("http2_frames");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                graphql.validate()?;
                if let Some(x) = &h2c {
                    x.validate()?;
                };
                if let Some(x) = &http2_frames {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlH2 {
                graphql,
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h2");
                self.unrecognized.remove("http2_frames");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                graphql.validate()?;
                if let Some(x) = &h2 {
                    x.validate()?;
                };
                if let Some(x) = &http2_frames {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h3");
                self.unrecognized.remove("quic");
                self.unrecognized.remove("udp");
                graphql.validate()?;
                if let Some(x) = &h3 {
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
            StepProtocols::H1c {
                h1c,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("h1c");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                h1c.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::H1 {
                h1,
                tls,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("h1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                h1.validate()?;
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::H2c {
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("h2c");
                self.unrecognized.remove("http2_frames");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                h2c.validate()?;
                if let Some(x) = &http2_frames {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::H2 {
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("h2");
                self.unrecognized.remove("http2_frames");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                h2.validate()?;
                if let Some(x) = &http2_frames {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::H3 { h3, quic, udp } => {
                self.unrecognized.remove("h3");
                self.unrecognized.remove("quic");
                self.unrecognized.remove("udp");
                h3.validate()?;
                if let Some(x) = &quic {
                    x.validate()?;
                };
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Tls {
                tls,
                tcp,
                tcp_segments,
            } => {
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                tls.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::Dtls { dtls, udp } => {
                self.unrecognized.remove("tls");
                self.unrecognized.remove("udp");
                dtls.validate()?;
                if let Some(x) = &udp {
                    x.validate()?;
                };
            }
            StepProtocols::Tcp { tcp, tcp_segments } => {
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("tcp_segments");
                tcp.validate()?;
                if let Some(x) = &tcp_segments {
                    x.validate()?;
                };
            }
            StepProtocols::TcpSegments { tcp_segments } => {
                self.unrecognized.remove("tcp_segments");
                tcp_segments.validate()?;
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
#[serde(untagged, deny_unknown_fields)]
pub enum StepProtocols {
    GraphQl {
        graphql: GraphQl,
        http: Option<Http>,
    },
    // If only graphql and tcp are specified we assume GraphQLH1c.
    GraphQlH1c {
        graphql: GraphQl,
        h1c: Option<Http1>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    // If only graphql and tls are specified we assume GraphQLH1.
    GraphQlH1 {
        graphql: GraphQl,
        h1: Option<Http1>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    GraphQlH2c {
        graphql: GraphQl,
        h2c: Option<Http2>,
        http2_frames: Option<Http2Frames>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    GraphQlH2 {
        graphql: GraphQl,
        h2: Option<Http2>,
        http2_frames: Option<Http2Frames>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    GraphQlH3 {
        graphql: GraphQl,
        h3: Option<Http3>,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Http {
        http: Http,
    },
    H1c {
        h1c: Http1,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    H1 {
        h1: Http1,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    H2c {
        h2c: Http2,
        http2_frames: Option<Http2Frames>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    H2 {
        h2: Http2,
        http2_frames: Option<Http2Frames>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    H3 {
        h3: Http3,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    Tls {
        tls: Tls,
        tcp: Option<Tcp>,
        tcp_segments: Option<TcpSegments>,
    },
    Dtls {
        dtls: Tls,
        udp: Option<Udp>,
    },
    Tcp {
        tcp: Tcp,
        tcp_segments: Option<TcpSegments>,
    },
    TcpSegments {
        tcp_segments: TcpSegments,
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
            Self::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                tcp_segments,
            } => Self::GraphQlH1c {
                graphql: graphql.merge(default.graphql),
                h1c: Some(h1c.unwrap_or_default().merge(default.h1c)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                tcp_segments,
            } => Self::GraphQlH1 {
                graphql: graphql.merge(default.graphql),
                h1: Some(h1.unwrap_or_default().merge(default.h1)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::GraphQlH2c {
                graphql,
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => Self::GraphQlH2c {
                graphql: graphql.merge(default.graphql),
                h2c: Some(h2c.unwrap_or_default().merge(default.h2c)),
                http2_frames: Some(http2_frames.unwrap_or_default().merge(default.http2_frames)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::GraphQlH2 {
                graphql,
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => Self::GraphQlH2 {
                graphql: graphql.merge(default.graphql),
                h2: Some(h2.unwrap_or_default().merge(default.h2)),
                http2_frames: Some(http2_frames.unwrap_or_default().merge(default.http2_frames)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },

            Self::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => Self::GraphQlH3 {
                graphql: graphql.merge(default.graphql),
                h3: Some(h3.unwrap_or_default().merge(default.h3)),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Http { http } => Self::Http {
                http: http.merge(default.http),
            },
            Self::H1c {
                h1c,
                tcp,
                tcp_segments,
            } => Self::H1c {
                h1c: h1c.merge(default.h1c),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::H1 {
                h1,
                tls,
                tcp,
                tcp_segments,
            } => Self::H1 {
                h1: h1.merge(default.h1),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::H2c {
                h2c,
                http2_frames,
                tcp,
                tcp_segments,
            } => Self::H2c {
                h2c: h2c.merge(default.h2c),
                http2_frames: Some(http2_frames.unwrap_or_default().merge(default.http2_frames)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::H2 {
                h2,
                http2_frames,
                tls,
                tcp,
                tcp_segments,
            } => Self::H2 {
                h2: h2.merge(default.h2),
                http2_frames: Some(http2_frames.unwrap_or_default().merge(default.http2_frames)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::H3 { h3, quic, udp } => Self::H3 {
                h3: h3.merge(default.h3),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Tls {
                tls,
                tcp,
                tcp_segments,
            } => Self::Tls {
                tls: tls.merge(default.tls),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::Tcp { tcp, tcp_segments } => Self::Tcp {
                tcp: tcp.merge(default.tcp),
                tcp_segments: Some(tcp_segments.unwrap_or_default().merge(default.tcp_segments)),
            },
            Self::TcpSegments { tcp_segments } => Self::TcpSegments {
                tcp_segments: tcp_segments.merge(default.tcp_segments),
            },

            Self::Dtls { dtls, udp } => Self::Dtls {
                dtls: dtls.merge(default.tls),
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
            Self::GraphQlH1c { .. } => ProtocolKind::GraphQlH1c,
            Self::GraphQlH1 { .. } => ProtocolKind::GraphQlH1,
            Self::GraphQlH2c { .. } => ProtocolKind::GraphQlH2c,
            Self::GraphQlH2 { .. } => ProtocolKind::GraphQlH2,
            Self::GraphQlH3 { .. } => ProtocolKind::GraphQlH3,
            Self::Http { .. } => ProtocolKind::Http,
            Self::H1c { .. } => ProtocolKind::H1c,
            Self::H1 { .. } => ProtocolKind::H1,
            Self::H2c { .. } => ProtocolKind::H2c,
            Self::H2 { .. } => ProtocolKind::H2,
            Self::H3 { .. } => ProtocolKind::H3,
            Self::Tls { .. } => ProtocolKind::Tls,
            Self::Dtls { .. } => ProtocolKind::Dtls,
            Self::Tcp { .. } => ProtocolKind::Tcp,
            Self::TcpSegments { .. } => ProtocolKind::TcpSegments,
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
    pub share: Option<Value>,
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
            share: first.share.or(second.share),
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
    pub pause: Option<GraphQlPause>,
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
            pause: GraphQlPause::merge(self.pause, second.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
            if !p.unrecognized.is_empty() {
                return Err(crate::Error(format!(
                    "unrecognized field{} {}",
                    if p.unrecognized.len() == 1 { "" } else { "s" },
                    p.unrecognized.keys().join(", "),
                )));
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
        Some(Self {
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
    pub method: Option<Value>,
    pub headers: Option<Table>,
    pub add_content_length: Option<Value>,
    pub body: Option<Value>,
    #[serde(default)]
    pub pause: Option<HttpPause>,
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
            method: Value::merge(self.method, second.method),
            headers: Table::merge(self.headers, second.headers),
            add_content_length: Value::merge(self.add_content_length, second.add_content_length),
            body: Value::merge(self.body, second.body),
            pause: HttpPause::merge(self.pause, second.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpPause {
    pub open: Option<PausePoints>,
    pub request_headers: Option<PausePoints>,
    pub request_body: Option<PausePoints>,
    pub response_headers: Option<PausePoints>,
    pub response_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for HttpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            open: PausePoints::merge(first.open, second.open),
            request_headers: PausePoints::merge(first.request_headers, second.request_headers),
            request_body: PausePoints::merge(first.request_body, second.request_body),
            response_headers: PausePoints::merge(first.response_headers, second.response_headers),
            response_body: PausePoints::merge(first.response_body, second.response_body),
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
    pub version_string: Option<Value>,
    #[serde(flatten, default)]
    pub common: Http,
    #[serde(default)]
    pub pause: Option<Http1Pause>,
}

impl Http1 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            version_string: Value::merge(self.version_string, default.version_string),
            common: self.common.merge(Some(default.common)),
            pause: Http1Pause::merge(self.pause, default.pause),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
        }
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Http1Pause {
    pub open: Option<PausePoints>,
    pub request_headers: Option<PausePoints>,
    pub request_body: Option<PausePoints>,
    pub response_headers: Option<PausePoints>,
    pub response_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for Http1Pause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            open: PausePoints::merge(first.open, second.open),
            request_headers: PausePoints::merge(first.request_headers, second.request_headers),
            request_body: PausePoints::merge(first.request_body, second.request_body),
            response_headers: PausePoints::merge(first.response_headers, second.response_headers),
            response_body: PausePoints::merge(first.response_body, second.response_body),
            unrecognized: toml::Table::new(),
        })
    }
}

impl Http1Pause {
    fn validate(&self) -> crate::Result<()> {
        if let Some(open) = &self.open {
            open.validate()?;
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http2 {
    pub trailers: Option<Table>,
    #[serde(flatten, default)]
    pub common: Http,
    #[serde(default)]
    pub pause: Option<Http2Pause>,
}

impl Http2 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            trailers: Table::merge(self.trailers, default.trailers),
            common: self.common.merge(Some(default.common)),
            pause: self.pause.or(default.pause),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Http2Pause {
    pub open: Option<PausePoints>,
    pub request_headers: Option<PausePoints>,
    pub request_body: Option<PausePoints>,
    pub response_headers: Option<PausePoints>,
    pub response_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for Http2Pause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            open: PausePoints::merge(first.open, second.open),
            request_headers: PausePoints::merge(first.request_headers, second.request_headers),
            request_body: PausePoints::merge(first.request_body, second.request_body),
            response_headers: PausePoints::merge(first.response_headers, second.response_headers),
            response_body: PausePoints::merge(first.response_body, second.response_body),
            unrecognized: toml::Table::new(),
        })
    }
}

impl Http2Pause {
    fn validate(&self) -> crate::Result<()> {
        if let Some(open) = &self.open {
            open.validate()?;
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http2Frames {
    pub host: Option<Value>,
    pub port: Option<Value>,
    #[serde(default)]
    pub pause: Option<Http2FramesPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Http2Frames {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            pause: Http2FramesPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Http2FramesPause {
    pub handshake: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for Http2FramesPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            handshake: PausePoints::merge(first.handshake, second.handshake),
            unrecognized: toml::Table::new(),
        })
    }
}

impl Http2FramesPause {
    fn validate(&self) -> crate::Result<()> {
        if let Some(handshake) = &self.handshake {
            handshake.validate()?;
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
    pub alpn: Option<ValueOrArray<Value>>,
    pub body: Option<Value>,
    pub version: Option<Value>,
    #[serde(default)]
    pub pause: Option<TlsPause>,
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
            alpn: ValueOrArray::merge(self.alpn, default.alpn),
            body: Value::merge(self.body, default.body),
            version: Value::merge(self.version, default.version),
            pause: TlsPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsPause {
    pub handshake: Option<PausePoints>,
    pub send_body: Option<PausePoints>,
    pub receive_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TlsPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            handshake: PausePoints::merge(first.handshake, second.handshake),
            send_body: PausePoints::merge(first.send_body, second.send_body),
            receive_body: PausePoints::merge(first.receive_body, second.receive_body),
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
    pub pause: Option<TcpPause>,
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
            pause: TcpPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TcpPause {
    pub handshake: Option<PausePoints>,
    pub send_body: Option<PausePoints>,
    pub receive_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TcpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            handshake: PausePoints::merge(first.handshake, second.handshake),
            send_body: PausePoints::merge(first.send_body, second.send_body),
            receive_body: PausePoints::merge(first.receive_body, second.receive_body),
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
pub struct TcpSegments {
    pub remote_host: Option<Value>,
    pub remote_port: Option<Value>,
    pub local_host: Option<Value>,
    pub local_port: Option<Value>,
    pub isn: Option<Value>,
    pub segments: Option<ValueOrArray<TcpSegment>>,
    #[serde(default)]
    pub pause: Option<TcpSegmentsPause>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl TcpSegments {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            remote_host: Value::merge(self.remote_host, default.remote_host),
            remote_port: Value::merge(self.remote_port, default.remote_port),
            local_host: Value::merge(self.local_host, default.local_host),
            local_port: Value::merge(self.local_port, default.local_port),
            isn: Value::merge(self.isn, default.isn),
            segments: ValueOrArray::merge(self.segments, default.segments),
            pause: TcpSegmentsPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        for s in self.segments.iter().flatten() {
            s.validate()?;
        }
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TcpSegment {
    pub source: Option<Value>,
    pub destination: Option<Value>,
    pub sequence_number: Option<Value>,
    pub acknowledgment: Option<Value>,
    pub data_offset: Option<Value>,
    pub reserved: Option<Value>,
    pub flags: Option<Value>,
    pub window: Option<Value>,
    pub checksum: Option<Value>,
    pub urgent_ptr: Option<Value>,
    pub options: Option<ValueOrArray<Value>>,
    pub payload: Option<Value>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TcpSegment {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else {
            return second;
        };
        let Some(second) = second else {
            return Some(first);
        };
        Some(Self {
            source: Value::merge(first.source, second.source),
            destination: Value::merge(first.destination, second.destination),
            sequence_number: Value::merge(first.sequence_number, second.sequence_number),
            acknowledgment: Value::merge(first.acknowledgment, second.acknowledgment),
            data_offset: Value::merge(first.data_offset, second.data_offset),
            reserved: Value::merge(first.reserved, second.reserved),
            flags: Value::merge(first.flags, second.flags),
            window: Value::merge(first.window, second.window),
            checksum: Value::merge(first.checksum, second.checksum),
            urgent_ptr: Value::merge(first.urgent_ptr, second.urgent_ptr),
            options: ValueOrArray::merge(first.options, second.options),
            payload: Value::merge(first.payload, second.payload),
            unrecognized: toml::Table::new(),
        })
    }
}

impl TcpSegment {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TcpSegmentsPause {
    pub handshake: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for TcpSegmentsPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            handshake: PausePoints::merge(first.handshake, second.handshake),
            unrecognized: toml::Table::new(),
        })
    }
}

impl TcpSegmentsPause {
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
    pub pause: Option<QuicPause>,
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
            pause: QuicPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuicPause {
    pub handshake: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for QuicPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            handshake: PausePoints::merge(first.handshake, second.handshake),
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
    pub pause: Option<UdpPause>,
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
            pause: UdpPause::merge(self.pause, default.pause),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UdpPause {
    pub send_body: Option<PausePoints>,
    pub receive_body: Option<PausePoints>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Merge for UdpPause {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            send_body: PausePoints::merge(first.send_body, second.send_body),
            receive_body: PausePoints::merge(first.receive_body, second.receive_body),
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PausePoints {
    pub start: Option<ValueOrArray<PauseValue>>,
    pub end: Option<ValueOrArray<PauseValue>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl PausePoints {
    /// Merge two groups of pause values, with first groups taking presedence over second. If the same
    /// after tag is found in both groups, all entries with that after tag second are ignored.
    /// Otherwise, they are appended.
    fn merge(first: Option<PausePoints>, second: Option<PausePoints>) -> Option<PausePoints> {
        let Some(mut first) = first else {
            return second;
        };
        let Some(second) = second else {
            return Some(first);
        };
        first.start = ValueOrArray::merge(first.start, second.start);
        first.end = ValueOrArray::merge(first.end, second.end);
        Some(first)
    }

    fn validate(&self) -> crate::Result<()> {
        for pause in self.start.iter().flatten() {
            pause.validate()?;
        }
        for pause in self.end.iter().flatten() {
            pause.validate()?;
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PauseValue {
    pub duration: Option<Value>,
    pub offset_bytes: Option<Value>,
    pub join: Option<ValueOrArray<String>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl PauseValue {
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

impl Merge for PauseValue {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            duration: Value::merge(first.duration, second.duration),
            offset_bytes: Value::merge(first.offset_bytes, second.offset_bytes),
            join: first.join.or(second.join),
            unrecognized: toml::Table::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

impl<T> IntoIterator for ValueOrArray<T> {
    type Item = T;
    type IntoIter = Either<std::iter::Once<T>, std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ValueOrArray::Value(val) => Either::Left(std::iter::once(val)),
            ValueOrArray::Array(vec) => Either::Right(vec.into_iter()),
        }
    }
}

impl<'a, T> IntoIterator for &'a ValueOrArray<T> {
    type Item = &'a T;
    type IntoIter = Either<std::iter::Once<&'a T>, std::slice::Iter<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ValueOrArray::Value(val) => Either::Left(std::iter::once(val)),
            ValueOrArray::Array(vec) => Either::Right(vec.iter()),
        }
    }
}

impl<'a, T> IntoIterator for &'a mut ValueOrArray<T> {
    type Item = &'a mut T;
    type IntoIter = Either<std::iter::Once<&'a mut T>, std::slice::IterMut<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ValueOrArray::Value(val) => Either::Left(std::iter::once(val)),
            ValueOrArray::Array(vec) => Either::Right(vec.iter_mut()),
        }
    }
}

impl<T> ValueOrArray<T> {
    fn iter(&self) -> impl Iterator<Item = &T> {
        match self {
            ValueOrArray::Value(val) => Either::Left(std::iter::once(val)),
            ValueOrArray::Array(vec) => Either::Right(vec.iter()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Datetime(toml::value::Datetime),
    Toml {
        literal: toml::Value,
    },
    Base64 {
        base64: String,
    },
    Enum {
        kind: EnumKind,
        #[serde(flatten)]
        fields: HashMap<String, ValueOrArray<Literal>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum EnumKind {
    Named(String),
    Numeric(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Literal(Literal),
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

impl Merge for Value {
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
}

impl Value {
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
                    key: Value::Literal(Literal::String(key)),
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
                        .any(|x| matches!(&x.key, Value::Literal(Literal::String(k)) if k == key))
                })
                .flat_map(|(key, value)| {
                    Vec::from(value).into_iter().map(move |v| TableEntry {
                        key: Value::Literal(Literal::String(key.clone())),
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
