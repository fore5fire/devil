use super::{Evaluate, PlanValue, PlanValueTable};
use crate::{bindings, Error, MaybeUtf8, Result, State};
use anyhow::anyhow;
use url::Url;

#[derive(Debug, Clone)]
pub struct GraphqlRequest {
    pub url: PlanValue<Url>,
    pub query: PlanValue<String>,
    pub params: Option<PlanValueTable<MaybeUtf8, serde_json::Value>>,
    pub operation: PlanValue<Option<serde_json::Value>>,
}

impl TryFrom<bindings::Graphql> for GraphqlRequest {
    type Error = Error;
    fn try_from(binding: bindings::Graphql) -> Result<Self> {
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

impl Evaluate<crate::GraphqlPlanOutput> for GraphqlRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::GraphqlPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::GraphqlPlanOutput {
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
