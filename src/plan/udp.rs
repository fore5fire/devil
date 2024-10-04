use super::PlanValue;
use crate::{bindings, Error, Result};
use anyhow::anyhow;

#[derive(Debug, Default, Clone)]
pub struct UdpRequest {
    pub body: PlanValue<Vec<u8>>,
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
}

impl TryFrom<bindings::Udp> for UdpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Udp) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("udp.host is required"))??,
            port: binding
                .port
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("udp.port is required"))??,
            body: binding
                .body
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_else(|| PlanValue::Literal(Vec::new())),
        })
    }
}
