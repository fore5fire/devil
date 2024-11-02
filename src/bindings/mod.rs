use std::collections::HashMap;

use anyhow::{anyhow, bail};
use indexmap::IndexMap;
use itertools::{Either, Itertools};
use serde::{Deserialize, Serialize};

mod pause;
mod raw_http2;
mod signal;

pub use pause::*;
pub use raw_http2::*;
pub use signal::*;

pub trait Merge: std::fmt::Debug + Clone + Serialize + Deserialize<'static> {
    // TODO: Since all types handle option wrappers the same way, just have implementations handle
    // merging if both are some and provide a default implementation of the fn which unwraps
    // options.
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
        let mut plan: Plan = toml::from_str(input)?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&mut self) -> crate::Result<()> {
        self.devil.validate()?;
        for (name, step) in &mut self.steps {
            step.validate()
                .map_err(|e| anyhow!("validate step {}: {}", name, e))?;
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

impl Validate for Settings {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field devil.{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Defaults {
    pub selector: Option<Selector>,
    pub graphql: Option<Graphql>,
    pub http: Option<Http>,
    pub h1c: Option<Http1>,
    pub h1: Option<Http1>,
    pub h2c: Option<Http2>,
    pub raw_h2c: Option<RawHttp2>,
    pub h2: Option<Http2>,
    pub raw_h2: Option<RawHttp2>,
    pub h3: Option<Http3>,
    pub tls: Option<Tls>,
    pub tcp: Option<Tcp>,
    pub raw_tcp: Option<RawTcp>,
    pub quic: Option<Quic>,
    pub dtls: Option<Tls>,
    pub udp: Option<Udp>,
    pub run: Option<Run>,
    #[serde(default)]
    pub sync: IndexMap<String, Sync>,
    #[serde(default)]
    pub pause: IndexMap<String, PauseValue>,
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
#[serde(rename_all = "snake_case")]
pub enum ProtocolKind {
    Graphql,
    GraphqlH1c,
    GraphqlH1,
    GraphqlH2c,
    GraphqlH2,
    GraphqlH3,
    Http,
    H1c,
    H1,
    H2c,
    H2,
    H3,
    RawH2c,
    RawH2,
    Tls,
    Tcp,
    RawTcp,
    Dtls,
    Quic,
    Udp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Protocol {
    Graphql(Graphql),
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
    pub run: Option<Run>,
    #[serde(default)]
    pub sync: IndexMap<String, Sync>,
    #[serde(default)]
    pub pause: IndexMap<String, PauseValue>,
    #[serde(default)]
    pub signal: IndexMap<String, SignalValue>,
}

impl Step {
    pub fn apply_defaults<'a, I: IntoIterator<Item = Defaults>>(mut self, defaults: I) -> Self {
        let (run_defaults, pause_defaults, sync_defaults, proto_defaults): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = itertools::multiunzip(
            defaults
                .into_iter()
                .filter_map(|d| Some((d.run.clone(), d.pause.clone(), d.sync.clone(), d))),
        );
        self.sync.extend(sync_defaults.into_iter().flatten());
        self.pause.extend(pause_defaults.into_iter().flatten());
        Step {
            run: run_defaults.into_iter().fold(self.run, Run::merge),
            protocols: self.protocols.apply_defaults(proto_defaults),
            sync: self.sync,
            pause: self.pause,
            signal: self.signal,
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&mut self) -> crate::Result<()> {
        for (_, pause) in &self.pause {
            pause.validate()?;
        }
        match &self.protocols {
            StepProtocols::Graphql { graphql, http } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("http");
                graphql.validate()?;
                if let Some(x) = &http {
                    x.validate()?;
                };
            }
            StepProtocols::GraphqlH1c {
                graphql,
                h1c,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h1c");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                graphql.validate()?;
                if let Some(x) = &h1c {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphqlH1 {
                graphql,
                h1,
                tls,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
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
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphqlH2c {
                graphql,
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h2c");
                self.unrecognized.remove("raw_h2c");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                graphql.validate()?;
                if let Some(x) = &h2c {
                    x.validate()?;
                };
                if let Some(x) = &raw_h2c {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphqlH2 {
                graphql,
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("graphql");
                self.unrecognized.remove("h2");
                self.unrecognized.remove("raw_h2");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                graphql.validate()?;
                if let Some(x) = &h2 {
                    x.validate()?;
                };
                if let Some(x) = &raw_h2 {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::GraphqlH3 {
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
            StepProtocols::H1c { h1c, tcp, raw_tcp } => {
                self.unrecognized.remove("h1c");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                h1c.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::H1 {
                h1,
                tls,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("h1");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                h1.validate()?;
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::H2c {
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("h2c");
                self.unrecognized.remove("raw_h2c");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                h2c.validate()?;
                if let Some(x) = &raw_h2c {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::H2 {
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("h2");
                self.unrecognized.remove("raw_h2");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                h2.validate()?;
                if let Some(x) = &raw_h2 {
                    x.validate()?;
                };
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
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
            StepProtocols::RawH2c {
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("raw_h2c");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                raw_h2c.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::RawH2 {
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                self.unrecognized.remove("raw_h2");
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                raw_h2.validate()?;
                if let Some(x) = &tls {
                    x.validate()?;
                };
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::Tls { tls, tcp, raw_tcp } => {
                self.unrecognized.remove("tls");
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                tls.validate()?;
                if let Some(x) = &tcp {
                    x.validate()?;
                };
                if let Some(x) = &raw_tcp {
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
            StepProtocols::Tcp { tcp, raw_tcp } => {
                self.unrecognized.remove("tcp");
                self.unrecognized.remove("raw_tcp");
                tcp.validate()?;
                if let Some(x) = &raw_tcp {
                    x.validate()?;
                };
            }
            StepProtocols::RawTcp { raw_tcp } => {
                self.unrecognized.remove("raw_tcp");
                raw_tcp.validate()?;
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
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum StepProtocols {
    Graphql {
        graphql: Graphql,
        http: Option<Http>,
    },
    // If only graphql and tcp are specified we assume GraphQLH1c.
    GraphqlH1c {
        graphql: Graphql,
        h1c: Option<Http1>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    // If only graphql and tls are specified we assume GraphQLH1.
    GraphqlH1 {
        graphql: Graphql,
        h1: Option<Http1>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    GraphqlH2c {
        graphql: Graphql,
        h2c: Option<Http2>,
        raw_h2c: Option<RawHttp2>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    GraphqlH2 {
        graphql: Graphql,
        h2: Option<Http2>,
        raw_h2: Option<RawHttp2>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    GraphqlH3 {
        graphql: Graphql,
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
        raw_tcp: Option<RawTcp>,
    },
    H1 {
        h1: Http1,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    H2c {
        h2c: Http2,
        raw_h2c: Option<RawHttp2>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    H2 {
        h2: Http2,
        raw_h2: Option<RawHttp2>,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    H3 {
        h3: Http3,
        quic: Option<Quic>,
        udp: Option<Udp>,
    },
    RawH2c {
        raw_h2c: RawHttp2,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    RawH2 {
        raw_h2: RawHttp2,
        tls: Option<Tls>,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    Tls {
        tls: Tls,
        tcp: Option<Tcp>,
        raw_tcp: Option<RawTcp>,
    },
    Dtls {
        dtls: Tls,
        udp: Option<Udp>,
    },
    Tcp {
        tcp: Tcp,
        raw_tcp: Option<RawTcp>,
    },
    RawTcp {
        raw_tcp: RawTcp,
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
            Self::Graphql { graphql, http } => Self::Graphql {
                graphql: Graphql::merge(graphql, default.graphql),
                http: Some(http.unwrap_or_default().merge(default.http)),
            },
            Self::GraphqlH1c {
                graphql,
                h1c,
                tcp,
                raw_tcp,
            } => Self::GraphqlH1c {
                graphql: graphql.merge(default.graphql),
                h1c: Some(h1c.unwrap_or_default().merge(default.h1c)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::GraphqlH1 {
                graphql,
                h1,
                tls,
                tcp,
                raw_tcp,
            } => Self::GraphqlH1 {
                graphql: graphql.merge(default.graphql),
                h1: Some(h1.unwrap_or_default().merge(default.h1)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::GraphqlH2c {
                graphql,
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => Self::GraphqlH2c {
                graphql: graphql.merge(default.graphql),
                h2c: Some(h2c.unwrap_or_default().merge(default.h2c)),
                raw_h2c: Some(raw_h2c.unwrap_or_default().merge(default.raw_h2c)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::GraphqlH2 {
                graphql,
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => Self::GraphqlH2 {
                graphql: graphql.merge(default.graphql),
                h2: Some(h2.unwrap_or_default().merge(default.h2)),
                raw_h2: Some(raw_h2.unwrap_or_default().merge(default.raw_h2)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },

            Self::GraphqlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => Self::GraphqlH3 {
                graphql: graphql.merge(default.graphql),
                h3: Some(h3.unwrap_or_default().merge(default.h3)),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::Http { http } => Self::Http {
                http: http.merge(default.http),
            },
            Self::H1c { h1c, tcp, raw_tcp } => Self::H1c {
                h1c: h1c.merge(default.h1c),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::H1 {
                h1,
                tls,
                tcp,
                raw_tcp,
            } => Self::H1 {
                h1: h1.merge(default.h1),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::H2c {
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => Self::H2c {
                h2c: h2c.merge(default.h2c),
                raw_h2c: Some(raw_h2c.unwrap_or_default().merge(default.raw_h2c)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::H2 {
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => Self::H2 {
                h2: h2.merge(default.h2),
                raw_h2: Some(raw_h2.unwrap_or_default().merge(default.raw_h2)),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::H3 { h3, quic, udp } => Self::H3 {
                h3: h3.merge(default.h3),
                quic: Some(quic.unwrap_or_default().merge(default.quic)),
                udp: Some(udp.unwrap_or_default().merge(default.udp)),
            },
            Self::RawH2c {
                raw_h2c,
                tcp,
                raw_tcp,
            } => Self::RawH2c {
                raw_h2c: raw_h2c.merge(default.raw_h2c),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::RawH2 {
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => Self::RawH2 {
                raw_h2: raw_h2.merge(default.raw_h2),
                tls: Some(tls.unwrap_or_default().merge(default.tls)),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::Tls { tls, tcp, raw_tcp } => Self::Tls {
                tls: tls.merge(default.tls),
                tcp: Some(tcp.unwrap_or_default().merge(default.tcp)),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::Tcp { tcp, raw_tcp } => Self::Tcp {
                tcp: tcp.merge(default.tcp),
                raw_tcp: Some(raw_tcp.unwrap_or_default().merge(default.raw_tcp)),
            },
            Self::RawTcp { raw_tcp } => Self::RawTcp {
                raw_tcp: raw_tcp.merge(default.raw_tcp),
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
            Self::Graphql { .. } => ProtocolKind::Graphql,
            Self::GraphqlH1c { .. } => ProtocolKind::GraphqlH1c,
            Self::GraphqlH1 { .. } => ProtocolKind::GraphqlH1,
            Self::GraphqlH2c { .. } => ProtocolKind::GraphqlH2c,
            Self::GraphqlH2 { .. } => ProtocolKind::GraphqlH2,
            Self::GraphqlH3 { .. } => ProtocolKind::GraphqlH3,
            Self::Http { .. } => ProtocolKind::Http,
            Self::H1c { .. } => ProtocolKind::H1c,
            Self::H1 { .. } => ProtocolKind::H1,
            Self::H2c { .. } => ProtocolKind::H2c,
            Self::H2 { .. } => ProtocolKind::H2,
            Self::H3 { .. } => ProtocolKind::H3,
            Self::RawH2c { .. } => ProtocolKind::RawH2c,
            Self::RawH2 { .. } => ProtocolKind::RawH2,
            Self::Tls { .. } => ProtocolKind::Tls,
            Self::Dtls { .. } => ProtocolKind::Dtls,
            Self::Tcp { .. } => ProtocolKind::Tcp,
            Self::RawTcp { .. } => ProtocolKind::RawTcp,
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

impl Merge for Run {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sync {
    Barrier { count: Value },
    Mutex,
    PriorityMutex,
    Semaphore { permits: Value },
    PrioritySemaphore { permits: Value },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Graphql {
    pub url: Option<Value>,
    pub query: Option<Value>,
    pub params: Option<Table>,
    pub operation: Option<Value>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Graphql {
    fn merge(self, second: Option<Self>) -> Self {
        let Some(second) = second else {
            return self;
        };
        Self {
            url: Value::merge(self.url, second.url),
            query: Value::merge(self.query, second.query),
            params: Table::merge(self.params, second.params),
            operation: Value::merge(self.operation, second.operation),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http1 {
    pub version_string: Option<Value>,
    #[serde(flatten, default)]
    pub common: Http,
}

impl Http1 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            version_string: Value::merge(self.version_string, default.version_string),
            common: self.common.merge(Some(default.common)),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        self.common.validate()?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Http2 {
    pub trailers: Option<Table>,
    #[serde(flatten, default)]
    pub common: Http,
}

impl Http2 {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            trailers: Table::merge(self.trailers, default.trailers),
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
    pub alpn: Option<ValueOrArray<Value>>,
    pub body: Option<Value>,
    pub version: Option<Value>,
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tcp {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub body: Option<Value>,
    //pub close: Option<TcpClose>,
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
            //close: TcpClose::merge(self.close, default.close),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        //if let Some(c) = &self.close {
        //    c.validate()?;
        //}
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

//#[derive(Debug, Default, Clone, Serialize, Deserialize)]
//pub struct TcpClose {
//    pub timeout: Option<Value>,
//    pub pattern: Option<Value>,
//    pub pattern_window: Option<Value>,
//    pub bytes: Option<Value>,
//    #[serde(flatten)]
//    pub unrecognized: toml::Table,
//}
//
//impl TcpClose {
//    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
//        let Some(first) = first else { return second };
//        let Some(second) = second else {
//            return Some(first);
//        };
//        Some(Self {
//            timeout: Value::merge(first.timeout, second.timeout),
//            pattern: Value::merge(first.pattern, second.pattern),
//            pattern_window: Value::merge(first.pattern_window, second.pattern_window),
//            bytes: Value::merge(first.bytes, second.bytes),
//            unrecognized: toml::Table::new(),
//        })
//    }
//
//    fn validate(&self) -> crate::Result<()> {
//        if !self.unrecognized.is_empty() {
//            bail!(
//                "unrecognized field{} {}",
//                if self.unrecognized.len() == 1 {
//                    ""
//                } else {
//                    "s"
//                },
//                self.unrecognized.keys().join(", "),
//            );
//        }
//        Ok(())
//    }
//}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RawTcp {
    pub dest_host: Option<Value>,
    pub dest_port: Option<Value>,
    pub src_host: Option<Value>,
    pub src_port: Option<Value>,
    pub isn: Option<Value>,
    pub window: Option<Value>,
    pub segments: Option<ValueOrArray<TcpSegment>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl RawTcp {
    fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            dest_host: Value::merge(self.dest_host, default.dest_host),
            dest_port: Value::merge(self.dest_port, default.dest_port),
            src_host: Value::merge(self.src_host, default.src_host),
            src_port: Value::merge(self.src_port, default.src_port),
            isn: Value::merge(self.isn, default.isn),
            window: Value::merge(self.window, default.window),
            segments: ValueOrArray::merge(self.segments, default.segments),
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        for s in self.segments.iter().flatten() {
            s.validate()?;
        }
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
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

impl Validate for TcpSegment {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
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
            unrecognized: toml::Table::new(),
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocationValue {
    pub id: Option<Value>,
    pub offset_bytes: Option<Value>,
    unrecognized: toml::Table,
}

impl Merge for LocationValue {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else {
            return second;
        };
        let Some(second) = second else {
            return Some(first);
        };
        Some(Self {
            id: first.id.or(second.id),
            offset_bytes: first.offset_bytes.or(second.offset_bytes),
            unrecognized: toml::Table::new(),
        })
    }
}

impl Validate for LocationValue {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized location field {} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
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

pub trait Validate {
    fn validate(&self) -> crate::Result<()>;
}
