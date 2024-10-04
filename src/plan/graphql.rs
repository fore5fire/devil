use super::{Evaluate, PlanValue, PlanValueTable};
use crate::{bindings, Error, Result, State};
use anyhow::anyhow;
use url::Url;

#[derive(Debug, Clone)]
pub struct GraphQlRequest {
    pub url: PlanValue<Url>,
    pub query: PlanValue<String>,
    pub params: Option<PlanValueTable<Vec<u8>, serde_json::Value>>,
    pub operation: PlanValue<Option<serde_json::Value>>,
}

impl TryFrom<bindings::GraphQl> for GraphQlRequest {
    type Error = Error;
    fn try_from(binding: bindings::GraphQl) -> Result<Self> {
        Ok(Self {
            url: binding
                .url
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("graphql.url is required"))??,
            query: binding
                .query
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("graphql.query is required"))??,
            params: binding.params.map(PlanValueTable::try_from).transpose()?,
            operation: binding.operation.try_into()?,
        })
    }
}

impl Evaluate<crate::GraphQlPlanOutput> for GraphQlRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::GraphQlPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::GraphQlPlanOutput {
            url: self.url.evaluate(state)?,
            query: self.query.evaluate(state)?,
            operation: self.operation.evaluate(state)?,
            params: self
                .params
                .evaluate(state)?
                .map(|p| p.into_iter().collect()),
        })
    }
}
