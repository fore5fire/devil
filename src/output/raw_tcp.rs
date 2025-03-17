use std::sync::Arc;

use cel_interpreter::Duration;
use doberman_derive::{BigQuerySchema, Record};
use serde::Serialize;

use super::{BytesOutput, Direction, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "raw_tcp")]
#[bigquery(tag = "kind")]
#[record(rename = "raw_tcp")]
pub struct RawTcpOutput {
    pub name: ProtocolName,
    pub plan: RawTcpPlanOutput,
    pub dest_ip: String,
    pub dest_port: u16,
    pub sent: Vec<Arc<TcpSegmentOutput>>,
    pub src_host: String,
    pub src_port: u16,
    pub received: Vec<Arc<TcpSegmentOutput>>,
    pub errors: Vec<RawTcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct RawTcpPlanOutput {
    pub dest_host: String,
    pub dest_port: u16,
    pub src_host: Option<String>,
    pub src_port: Option<u16>,
    pub isn: u32,
    pub window: u16,
    pub segments: Vec<Arc<TcpSegmentOutput>>,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema, Record)]
#[serde(tag = "kind", rename = "raw_tcp_segment")]
#[bigquery(tag = "kind")]
#[record(rename = "raw_tcp_segment")]
pub struct TcpSegmentOutput {
    pub name: PduName,
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
    pub payload: BytesOutput,
    pub received: Option<Duration>,
    pub sent: Option<Duration>,
    pub direction: Direction,
}

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
#[serde(rename_all = "snake_case")]
pub enum TcpSegmentOptionOutput {
    // bool value is unused, but required to support serialization to parquet.
    Nop(bool),
    Mss(u16),
    Wscale(u8),
    // bool value is unused, but required to support serialization to parquet.
    SackPermitted(bool),
    Sack(Vec<u32>),
    Timestamps { tsval: u32, tsecr: u32 },
    Generic { kind: u8, value: BytesOutput },
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
            Self::Nop(_) => 1,
            Self::Mss(_) => 4,
            Self::Wscale(_) => 3,
            Self::SackPermitted(_) => 2,
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

#[derive(Debug, Clone, Serialize, BigQuerySchema)]
pub struct RawTcpError {
    pub kind: String,
    pub message: String,
}
