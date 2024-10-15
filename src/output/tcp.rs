use cel_interpreter::Duration;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TcpOutput {
    pub plan: TcpPlanOutput,
    pub sent: Option<TcpSentOutput>,
    pub received: Option<TcpReceivedOutput>,
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
    pub body: Vec<u8>,
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
pub struct TcpSentOutput {
    pub dest_ip: String,
    pub dest_port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TcpReceivedOutput {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TcpError {
    pub kind: String,
    pub message: String,
}
