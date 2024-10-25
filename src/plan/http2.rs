use super::{AddContentLength, Evaluate, PlanValue, PlanValueTable};
use crate::{bindings, Error, MaybeUtf8, Result, State};
use anyhow::anyhow;
use url::Url;

#[derive(Debug, Clone)]
pub struct Http2Request {
    pub url: PlanValue<Url>,
    pub method: PlanValue<Option<MaybeUtf8>>,
    pub add_content_length: PlanValue<AddContentLength>,
    pub headers: PlanValueTable<MaybeUtf8, MaybeUtf8>,
    pub body: PlanValue<Option<MaybeUtf8>>,
    pub trailers: PlanValueTable<MaybeUtf8, MaybeUtf8>,
}

impl Evaluate<crate::Http2PlanOutput> for Http2Request {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::Http2PlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::Http2PlanOutput {
            url: self.url.evaluate(state)?,
            method: self.method.evaluate(state)?,
            add_content_length: self.add_content_length.evaluate(state)?,
            headers: self.headers.evaluate(state)?,
            trailers: self.trailers.evaluate(state)?,
            body: self.body.evaluate(state)?.unwrap_or_default(),
        })
    }
}

impl TryFrom<bindings::Http2> for Http2Request {
    type Error = Error;
    fn try_from(binding: bindings::Http2) -> Result<Self> {
        Ok(Self {
            url: binding
                .common
                .url
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("http2.url is required"))??,
            method: binding.common.method.try_into()?,
            body: binding.common.body.try_into()?,
            add_content_length: binding
                .common
                .add_content_length
                .map(PlanValue::<AddContentLength>::try_from)
                .ok_or_else(|| anyhow!("http2.add_content_length is required"))??,
            headers: PlanValueTable::try_from(binding.common.headers.unwrap_or_default())?,
            trailers: PlanValueTable::try_from(binding.trailers.unwrap_or_default())?,
        })
    }
}
