use super::{PlanValue, TlsVersion};
use crate::{bindings, Error, MaybeUtf8, Result};
use anyhow::anyhow;

#[derive(Debug, Default, Clone)]
pub struct QuicRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub body: PlanValue<MaybeUtf8>,
    pub version: PlanValue<Option<TlsVersion>>,
}

impl TryFrom<bindings::Quic> for QuicRequest {
    type Error = Error;
    fn try_from(binding: bindings::Quic) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("quic.host is required"))??,
            port: binding
                .port
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("quic.port is required"))??,
            body: binding
                .body
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            version: binding.tls_version.try_into()?,
        })
    }
}
