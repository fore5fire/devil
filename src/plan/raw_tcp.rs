use super::{Evaluate, PlanValue};
use crate::{
    bindings, BytesOutput, Error, Result, State, TcpSegmentOptionOutput, TcpSegmentOutput,
};
use anyhow::anyhow;
use itertools::Itertools;
use rand::RngCore;

#[derive(Debug, Clone)]
pub struct RawTcpRequest {
    pub dest_host: PlanValue<String>,
    pub dest_port: PlanValue<u16>,
    pub src_host: PlanValue<Option<String>>,
    // 0 asks the implementation to select an unused port.
    pub src_port: PlanValue<Option<u16>>,
    pub isn: PlanValue<u32>,
    pub window: PlanValue<u16>,
    pub segments: Vec<TcpSegment>,
}

impl Evaluate<crate::RawTcpPlanOutput> for RawTcpRequest {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::RawTcpPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::RawTcpPlanOutput {
            dest_host: self.dest_host.evaluate(state)?,
            dest_port: self.dest_port.evaluate(state)?,
            src_host: self.src_host.evaluate(state)?,
            src_port: self.src_port.evaluate(state)?,
            isn: self.isn.evaluate(state)?,
            window: self.window.evaluate(state)?,
            segments: self
                .segments
                .iter()
                .map(|segments| segments.evaluate(state))
                .try_collect()?,
        })
    }
}

impl TryFrom<bindings::RawTcp> for RawTcpRequest {
    type Error = Error;
    fn try_from(binding: bindings::RawTcp) -> Result<Self> {
        Ok(Self {
            dest_host: binding
                .dest_host
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("raw_tcp.dest_host is required"))??,
            dest_port: binding
                .dest_port
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("raw_tcp.dest_port is required"))??,
            src_host: binding.src_host.try_into()?,
            src_port: binding.src_port.try_into()?,
            isn: binding
                .isn
                .map(PlanValue::try_from)
                .transpose()?
                // Random sequence number if not specified.
                .unwrap_or_else(|| PlanValue::Literal(rand::thread_rng().next_u32())),
            window: binding
                .window
                .map(PlanValue::<u16>::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(1 << 15)),
            segments: binding
                .segments
                .into_iter()
                .flatten()
                .map(TcpSegment::try_from)
                .try_collect()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegment {
    pub source: PlanValue<u16>,
    pub destination: PlanValue<u16>,
    pub sequence_number: PlanValue<u32>,
    pub acknowledgment: PlanValue<u32>,
    pub data_offset: PlanValue<u8>,
    pub reserved: PlanValue<u8>,
    pub flags: PlanValue<u8>,
    pub window: PlanValue<u16>,
    pub checksum: Option<PlanValue<u16>>,
    pub urgent_ptr: PlanValue<u16>,
    pub options: Vec<PlanValue<TcpSegmentOptionOutput>>,
    pub payload: PlanValue<BytesOutput>,
}

impl Evaluate<TcpSegmentOutput> for TcpSegment {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<TcpSegmentOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(TcpSegmentOutput {
            source: self.source.evaluate(state)?,
            destination: self.destination.evaluate(state)?,
            sequence_number: self.sequence_number.evaluate(state)?,
            acknowledgment: self.acknowledgment.evaluate(state)?,
            data_offset: self.data_offset.evaluate(state)?,
            reserved: self.reserved.evaluate(state)?,
            flags: self.flags.evaluate(state)?,
            window: self.window.evaluate(state)?,
            checksum: self.checksum.evaluate(state)?,
            urgent_ptr: self.urgent_ptr.evaluate(state)?,
            options: self.options.evaluate(state)?,
            payload: self.payload.evaluate(state)?,
            received: None,
            sent: None,
        })
    }
}

impl TryFrom<bindings::TcpSegment> for TcpSegment {
    type Error = crate::Error;
    fn try_from(value: bindings::TcpSegment) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            source: value
                .source
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            destination: value
                .destination
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            sequence_number: value
                .sequence_number
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            acknowledgment: value
                .acknowledgment
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            data_offset: value
                .data_offset
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            reserved: value
                .reserved
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            flags: value
                .flags
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            window: value
                .window
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            checksum: value.checksum.map(PlanValue::try_from).transpose()?,
            urgent_ptr: value
                .urgent_ptr
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            options: value
                .options
                .unwrap_or_default()
                .into_iter()
                .map(PlanValue::try_from)
                .collect::<Result<_>>()?,
            payload: value
                .payload
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

//#[derive(Debug, Clone)]
//pub enum TcpSegmentOption {
//    Nop,
//    Timestamps { tsval: u32, tsecr: u32 },
//    Mss(u16),
//    Wscale(u8),
//    SackPermitted,
//    Sack(Vec<u32>),
//    Generic { kind: u8, value: Vec<u8> },
//}
//
//impl TryFrom<bindings::EnumValue> for TcpSegmentOption {
//    type Error = crate::Error;
//    fn try_from(value: bindings::EnumValue) -> std::prelude::v1::Result<Self, Self::Error> {
//        match value {
//            bindings::Value::LiteralEnum { kind, fields } => Ok(PlanValue::Literal(
//                PlanData(cel_interpreter::Value::Map(cel_interpreter::objects::Map {
//                    map: Rc::new(
//                        fields
//                            .into_iter()
//                            .map(|(k, v)| Ok((k.into(), Self::try_from(v)?)))
//                            .collect::<Result<Vec<_>>>()?,
//                    ),
//                }))
//                .try_into()?,
//            )),
//        }
//    }
//}
