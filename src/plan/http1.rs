use std::sync::Arc;

use super::{AddContentLength, Evaluate, PlanValue, PlanValueTable};
use crate::{bindings, Error, HttpHeader, MaybeUtf8, Result, State};
use anyhow::anyhow;
use url::Url;

#[derive(Debug, Clone)]
pub struct Http1Request {
    pub url: PlanValue<Url>,
    pub method: PlanValue<Option<MaybeUtf8>>,
    pub version_string: PlanValue<Option<MaybeUtf8>>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub headers: PlanValueTable<MaybeUtf8, MaybeUtf8>,
    pub body: PlanValue<Option<MaybeUtf8>>,
}

impl Evaluate<crate::Http1PlanOutput> for Http1Request {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http1PlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a Arc<String>>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http1PlanOutput {
            url: self.url.evaluate(state)?,
            method: self.method.evaluate(state)?,
            version_string: self.version_string.evaluate(state)?,
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

impl TryFrom<bindings::Http1> for Http1Request {
    type Error = Error;
    fn try_from(binding: bindings::Http1) -> Result<Self> {
        Ok(Self {
            url: binding
                .common
                .url
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("http1.url is required"))??,
            version_string: binding.version_string.try_into()?,
            method: binding.common.method.try_into()?,
            add_content_length: binding
                .common
                .add_content_length
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("http.add_content_length is required"))??,
            headers: PlanValueTable::try_from(binding.common.headers.unwrap_or_default())?,
            body: binding.common.body.try_into()?,
        })
    }
}
