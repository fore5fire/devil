use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::{Duration, TimeDelta};
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct RawTcpOutput {
    pub plan: RawTcpPlanOutput,
    pub dest_ip: String,
    pub dest_port: u16,
    pub sent: Vec<TcpSegmentOutput>,
    pub src_host: String,
    pub src_port: u16,
    pub received: Vec<TcpSegmentOutput>,
    pub errors: Vec<RawTcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

impl From<RawTcpOutput> for Value {
    fn from(value: RawTcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("dest_ip".into(), value.dest_ip.into()),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                ("src_host".into(), value.src_host.into()),
                ("src_port".into(), u64::from(value.src_port).into()),
                ("sent".into(), value.sent.into()),
                ("received".into(), value.received.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RawTcpPlanOutput {
    pub dest_host: String,
    pub dest_port: u16,
    pub src_host: Option<String>,
    pub src_port: Option<u16>,
    pub isn: u32,
    pub window: u16,
    pub segments: Vec<TcpSegmentOutput>,
}

impl From<RawTcpPlanOutput> for Value {
    fn from(value: RawTcpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("dest_host".into(), Value::String(Arc::new(value.dest_host))),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                (
                    "src_host".into(),
                    value
                        .src_host
                        .map(|src_host| Value::String(Arc::new(src_host)))
                        .into(),
                ),
                (
                    "src_port".into(),
                    value.src_port.map(|src_port| u64::from(src_port)).into(),
                ),
                ("isn".into(), u64::from(value.isn).into()),
                ("window".into(), u64::from(value.window).into()),
                (
                    "segments".into(),
                    Value::List(Arc::new(
                        value.segments.into_iter().map(Value::from).collect(),
                    )),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpSegmentOutput {
    pub source: u16,
    pub destination: u16,
    pub sequence_number: u32,
    pub acknowledgment: u32,
    pub data_offset: u8,
    pub reserved: u8,
    pub flags: u8,
    pub window: u16,
    pub checksum: Option<u16>,
    pub urgent_ptr: u16,
    pub options: Vec<TcpSegmentOptionOutput>,
    pub payload: Vec<u8>,
    pub received: Option<TimeDelta>,
    pub sent: Option<TimeDelta>,
}

impl From<TcpSegmentOutput> for Value {
    fn from(value: TcpSegmentOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                (
                    "sequence_number".into(),
                    u64::from(value.sequence_number).into(),
                ),
                (
                    "payload".into(),
                    Value::Bytes(Arc::new(value.payload.clone())),
                ),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub enum TcpSegmentOptionOutput {
    Nop,
    Mss(u16),
    Wscale(u8),
    SackPermitted,
    Sack(Vec<u32>),
    Timestamps { tsval: u32, tsecr: u32 },
    Generic { kind: u8, value: Vec<u8> },
}

impl TcpSegmentOptionOutput {
    pub const KIND_KEY: &'static str = "kind";
    pub const VALUE_KEY: &'static str = "value";
    pub const TSVAL_KEY: &'static str = "tsval";
    pub const TSECR_KEY: &'static str = "tsecr";

    pub const NOP_KIND: &'static str = "nop";
    pub const TIMESTAMPS_KIND: &'static str = "timestamps";
    pub const MSS_KIND: &'static str = "mss";
    pub const WSCALE_KIND: &'static str = "wscale";
    pub const SACK_PERMITTED_KIND: &'static str = "sack_permitted";
    pub const SACK_KIND: &'static str = "sack";

    // the number of bytes required for the option on the wire. See
    // https://www.iana.org/assignments/tcp-parameters/tcp-parameters.xhtml
    pub fn size(&self) -> usize {
        match self {
            Self::Nop => 1,
            Self::Mss(_) => 4,
            Self::Wscale(_) => 3,
            Self::SackPermitted => 2,
            Self::Sack(vals) => vals
                .len()
                .checked_mul(4)
                .expect("tcp sack option size calculation should not overflow")
                .checked_add(2)
                .expect("tcp sack option size calculation should not overflow"),
            Self::Timestamps { .. } => 10,
            // Except for nop and end-of-options-list, options are a kind byte, a length byte, and
            // the value bytes.
            Self::Generic { value, .. } => value
                .len()
                .checked_add(2)
                .expect("tcp raw option size calculation should not overflow"),
        }
    }
}

impl From<TcpSegmentOptionOutput> for Value {
    fn from(value: TcpSegmentOptionOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(match value {
                TcpSegmentOptionOutput::Nop => {
                    HashMap::from([(TcpSegmentOptionOutput::KIND_KEY.into(), "nop".into())])
                }
                TcpSegmentOptionOutput::Timestamps { tsval, tsecr } => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "timestamps".into()),
                    (
                        TcpSegmentOptionOutput::TSVAL_KEY.into(),
                        u64::from(tsval).into(),
                    ),
                    (
                        TcpSegmentOptionOutput::TSECR_KEY.into(),
                        u64::from(tsecr).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Mss(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "mss".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        u64::from(val).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Wscale(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "wscale".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        u64::from(val).into(),
                    ),
                ]),
                TcpSegmentOptionOutput::SackPermitted => HashMap::from([(
                    TcpSegmentOptionOutput::KIND_KEY.into(),
                    "sack_permitted".into(),
                )]),
                TcpSegmentOptionOutput::Sack(val) => HashMap::from([
                    (TcpSegmentOptionOutput::KIND_KEY.into(), "sack".into()),
                    (
                        TcpSegmentOptionOutput::VALUE_KEY.into(),
                        val.into_iter()
                            .map(|x| Value::UInt(x.into()))
                            .collect_vec()
                            .into(),
                    ),
                ]),
                TcpSegmentOptionOutput::Generic { kind, value } => HashMap::from([
                    (
                        TcpSegmentOptionOutput::KIND_KEY.into(),
                        Value::UInt(kind.into()),
                    ),
                    (TcpSegmentOptionOutput::VALUE_KEY.into(), value.into()),
                ]),
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RawTcpError {
    pub kind: String,
    pub message: String,
}

impl From<RawTcpError> for Value {
    fn from(value: RawTcpError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("kind".into(), value.kind.into()),
                ("message".into(), value.message.into()),
            ])),
        })
    }
}
