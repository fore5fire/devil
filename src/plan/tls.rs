use super::{Evaluate, PlanData, PlanValue, TryFromPlanData};
use crate::bindings::Literal;
use crate::{bindings, Error, Result, State};
use anyhow::{anyhow, bail};
use itertools::Itertools;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum TlsVersion {
    Ssl1,
    Ssl2,
    Ssl3,
    #[serde(rename = "tls1.0")]
    Tls1_0,
    #[serde(rename = "tls1.1")]
    Tls1_1,
    #[serde(rename = "tls1.2")]
    Tls1_2,
    #[serde(rename = "tls1.3")]
    Tls1_3,
    #[serde(rename = "dtls1.0")]
    Dtls1_0,
    #[serde(rename = "dtls1.1")]
    Dtls1_1,
    #[serde(rename = "dtls1.2")]
    Dtls1_2,
    #[serde(rename = "dtls1.3")]
    Dtls1_3,
    #[serde(untagged)]
    Other(u16),
}

impl FromStr for TlsVersion {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.into() {
            "ssl1" => Self::Ssl1,
            "ssl2" => Self::Ssl2,
            "ssl3" => Self::Ssl3,
            "tls1.0" => Self::Tls1_0,
            "tls1.1" => Self::Tls1_1,
            "tls1.2" => Self::Tls1_2,
            "tls1.3" => Self::Tls1_3,
            "dtls1.0" => Self::Dtls1_0,
            "dtls1.1" => Self::Dtls1_1,
            "dtls1.2" => Self::Dtls1_2,
            "dtls1.3" => Self::Dtls1_3,
            _ => bail!("invalid tls version string {}", s),
        })
    }
}

impl From<&TlsVersion> for cel_interpreter::Value {
    fn from(value: &TlsVersion) -> Self {
        match value {
            TlsVersion::Ssl1 => cel_interpreter::Value::String(Arc::new("ssl1".to_owned())),
            TlsVersion::Ssl2 => cel_interpreter::Value::String(Arc::new("ssl2".to_owned())),
            TlsVersion::Ssl3 => cel_interpreter::Value::String(Arc::new("ssl3".to_owned())),
            TlsVersion::Tls1_0 => cel_interpreter::Value::String(Arc::new("tls1.0".to_owned())),
            TlsVersion::Tls1_1 => cel_interpreter::Value::String(Arc::new("tls1.1".to_owned())),
            TlsVersion::Tls1_2 => cel_interpreter::Value::String(Arc::new("tls1.2".to_owned())),
            TlsVersion::Tls1_3 => cel_interpreter::Value::String(Arc::new("tls1.3".to_owned())),
            TlsVersion::Dtls1_0 => cel_interpreter::Value::String(Arc::new("dtls1.0".to_owned())),
            TlsVersion::Dtls1_1 => cel_interpreter::Value::String(Arc::new("dtls1.1".to_owned())),
            TlsVersion::Dtls1_2 => cel_interpreter::Value::String(Arc::new("dtls1.2".to_owned())),
            TlsVersion::Dtls1_3 => cel_interpreter::Value::String(Arc::new("dtls1.3".to_owned())),
            TlsVersion::Other(a) => cel_interpreter::Value::UInt(*a as u64),
        }
    }
}

impl TryFromPlanData for TlsVersion {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        let cel_interpreter::Value::String(x) = value.0 else {
            bail!("TLS version must be a string");
        };
        match x.as_str() {
            "ssl1" => Ok(Self::Ssl1),
            "ssl2" => Ok(Self::Ssl2),
            "ssl3" => Ok(Self::Ssl3),
            "tls1.0" => Ok(Self::Tls1_0),
            "tls1.1" => Ok(Self::Tls1_1),
            "tls1.2" => Ok(Self::Tls1_2),
            "tls1.3" => Ok(Self::Tls1_3),
            "dtls1.0" => Ok(Self::Dtls1_0),
            "dtls1.1" => Ok(Self::Dtls1_1),
            "dtls1.2" => Ok(Self::Dtls1_2),
            "dtls1.3" => Ok(Self::Dtls1_3),
            val => bail!("invalid TLS version {val:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub alpn: Vec<PlanValue<Vec<u8>>>,
    pub body: PlanValue<Vec<u8>>,
}

impl Evaluate<crate::TlsPlanOutput> for TlsRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::TlsPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TlsPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            alpn: self.alpn.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
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
                .ok_or_else(|| anyhow!("tls.host is required"))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| anyhow!("tls.port is required"))??,
            alpn: binding
                .alpn
                .into_iter()
                .flatten()
                .map(PlanValue::<Vec<u8>>::try_from)
                .try_collect()?,
            body: binding
                .body
                .map(PlanValue::<Vec<u8>>::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
        })
    }
}

impl TryFrom<bindings::Value> for PlanValue<TlsVersion> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => bail!("invalid value {binding:?} for tls version field"),
        }
    }
}
