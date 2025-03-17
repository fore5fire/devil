use super::{Evaluate, PlanData, PlanValue, PlanValueTable, TryFromPlanData};
use crate::bindings::Literal;
use crate::{bindings, Error, HttpHeader, MaybeUtf8, Result, State};
use anyhow::{anyhow, bail};
use doberman_derive::BigQuerySchema;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, BigQuerySchema)]
pub enum AddContentLength {
    Never,
    Auto,
    Force,
}

impl FromStr for AddContentLength {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "never" => Ok(Self::Never),
            "auto" => Ok(Self::Auto),
            "force" => Ok(Self::Force),
            val => bail!("unrecognized add_content_length string {val}"),
        }
    }
}

impl ToString for AddContentLength {
    fn to_string(&self) -> String {
        match self {
            Self::Never => "never",
            Self::Auto => "auto",
            Self::Force => "force",
        }
        .to_owned()
    }
}

impl TryFromPlanData for AddContentLength {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            val => bail!("unsupported value {val:?} for field add_content_length"),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<AddContentLength> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::Literal(Literal::String(x)) => Ok(Self::Literal(x.parse()?)),
            val => bail!("invalid value {val:?} for field add_content_length"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub url: PlanValue<Url>,
    pub method: PlanValue<Option<MaybeUtf8>>,
    pub headers: PlanValueTable<MaybeUtf8, MaybeUtf8>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub body: PlanValue<Option<MaybeUtf8>>,
}

impl TryFrom<bindings::Http> for HttpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Http) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("http.url is required"))??,
            method: binding.method.try_into()?,
            add_content_length: binding
                .add_content_length
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("http.add_content_length is required"))??,
            body: binding.body.try_into()?,
            headers: PlanValueTable::try_from(binding.headers.unwrap_or_default())?,
        })
    }
}

impl Evaluate<crate::HttpPlanOutput> for HttpRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::HttpPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a Arc<String>>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::HttpPlanOutput {
            url: self.url.evaluate(state)?,
            method: self.method.evaluate(state)?,
            add_content_length: self.add_content_length.evaluate(state)?,
            headers: self
                .headers
                .evaluate(state)?
                .into_iter()
                .map(HttpHeader::from)
                .collect(),
            body: self.body.evaluate(state)?.unwrap_or_default(),
        })
    }
}
