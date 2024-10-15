use cel_interpreter::Duration;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct RawTcpPlanOutput {
    pub dest_host: String,
    pub dest_port: u16,
    pub src_host: Option<String>,
    pub src_port: Option<u16>,
    pub isn: u32,
    pub window: u16,
    pub segments: Vec<TcpSegmentOutput>,
}

#[derive(Debug, Clone, Serialize)]
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
    pub received: Option<Duration>,
    pub sent: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Serialize)]
pub struct RawTcpError {
    pub kind: String,
    pub message: String,
}
