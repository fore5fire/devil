use super::{Evaluate, PlanData, PlanValue, TryFromPlanData};
use crate::bindings::Literal;
use crate::{bindings, Error, MaybeUtf8, ParsedTlsVersion, Result, State, TlsVersion};
use anyhow::{anyhow, bail};
use itertools::Itertools;
use std::sync::Arc;

impl TryFromPlanData for TlsVersion {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        Ok(match value.0 {
            cel_interpreter::Value::String(x) => x.parse::<ParsedTlsVersion>()?.into(),
            cel_interpreter::Value::Int(raw) => u16::try_from(raw)?.into(),
            _ => bail!("TLS version must be a string or 16 bit unsigned integer"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TlsRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub alpn: Vec<PlanValue<MaybeUtf8>>,
    pub body: PlanValue<MaybeUtf8>,
}

impl Evaluate<crate::TlsPlanOutput> for TlsRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::TlsPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a Arc<String>>,
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
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("tls.host is required"))??,
            port: binding
                .port
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("tls.port is required"))??,
            alpn: binding
                .alpn
                .into_iter()
                .flatten()
                .map(PlanValue::try_from)
                .try_collect()?,
            body: binding
                .body
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl TryFrom<bindings::Value> for PlanValue<TlsVersion> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => {
                Ok(Self::Literal(x.parse::<ParsedTlsVersion>()?.into()))
            }
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            _ => bail!("invalid value {binding:?} for tls version field"),
        }
    }
}
