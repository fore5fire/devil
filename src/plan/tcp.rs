use super::{Evaluate, PlanValue};
use crate::{bindings, Error, MaybeUtf8, Result, State};
use anyhow::anyhow;

#[derive(Debug, Clone)]
pub struct TcpRequest {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub body: PlanValue<MaybeUtf8>,
    //pub close: TcpClose,
}

impl Evaluate<crate::TcpPlanOutput> for TcpRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::TcpPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::TcpPlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            body: self.body.evaluate(state)?.into(),
            //close: self.close.evaluate(state)?.into(),
        })
    }
}

impl TryFrom<bindings::Tcp> for TcpRequest {
    type Error = Error;
    fn try_from(binding: bindings::Tcp) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("tcp.host is required"))??,
            port: binding
                .port
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("tcp.port is required"))??,
            body: binding
                .body
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            //close: binding.close.unwrap_or_default().try_into()?,
        })
    }
}

//#[derive(Debug, Clone)]
//pub struct TcpClose {
//    timeout: Option<PlanValue<Duration>>,
//    pattern: Option<PlanValue<Regex>>,
//    pattern_window: Option<PlanValue<u64>>,
//    bytes: Option<PlanValue<u64>>,
//}
//
//impl TryFrom<bindings::TcpClose> for TcpClose {
//    type Error = Error;
//    fn try_from(value: bindings::TcpClose) -> std::result::Result<Self, Self::Error> {
//        Ok(Self {
//            timeout: value.timeout.map(PlanValue::try_from).transpose()?,
//            pattern: value.pattern.map(PlanValue::try_from).transpose()?,
//            pattern_window: value.pattern_window.map(PlanValue::try_from).transpose()?,
//            bytes: value.bytes.map(PlanValue::try_from).transpose()?,
//        })
//    }
//}

//impl Evaluate<TcpPlanCloseOutput> for TcpClose {
//    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TcpPlanCloseOutput>
//    where
//        S: State<'a, O, I>,
//        O: Into<&'a str>,
//        I: IntoIterator<Item = O>,
//    {
//        Ok(TcpPlanCloseOutput {
//            timeout: self.timeout.as_ref().map(|timeout| timeout.evaluate(state)).transpose()?,
//            pattern: self.pattern.as_ref().map(|pattern| pattern.evaluate(state)).transpose()?,
//            pattern_window: self.pattern_window.as_ref().map(|window| window.evaluate(state)).transpose()?,
//            bytes: self.bytes.as_ref().map(|bytes| bytes.evaluate(state)).transpose()?,
//        })
//    }
//}
