use std::sync::Arc;

use cel_interpreter::Duration;
use serde::Serialize;

use super::{MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tcp")]
pub struct TcpOutput {
    pub name: ProtocolName,
    pub plan: TcpPlanOutput,
    pub sent: Option<Arc<TcpSentOutput>>,
    pub received: Option<Arc<TcpReceivedOutput>>,
    //pub close: TcpCloseOutput,
    pub errors: Vec<TcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpCloseOutput {
//    pub timed_out: bool,
//    pub recv_max_reached: bool,
//    pub pattern_match: Option<Vec<u8>>,
//}

#[derive(Debug, Clone, Serialize)]
pub struct TcpPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: MaybeUtf8,
    //pub close: TcpPlanCloseOutput,
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpPlanCloseOutput {
//    pub min_duration: Option<Duration>,
//    pub read_pattern: Option<Regex>,
//    pub read_pattern_window: Option<u64>,
//    pub read_length: Option<u64>,
//}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tcp_sent")]
pub struct TcpSentOutput {
    pub name: PduName,
    pub dest_ip: String,
    pub dest_port: u16,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "tcp_recv")]
pub struct TcpReceivedOutput {
    pub name: PduName,
    pub body: MaybeUtf8,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TcpError {
    pub kind: String,
    pub message: String,
}
