use std::sync::Arc;
use std::{collections::HashMap, rc::Rc};

use cel_interpreter::{objects::Map, Value};
use chrono::Duration;

#[derive(Debug, Clone)]
pub struct TcpOutput {
    pub plan: TcpPlanOutput,
    pub sent: Option<TcpSentOutput>,
    pub received: Option<TcpReceivedOutput>,
    //pub close: TcpCloseOutput,
    pub errors: Vec<TcpError>,
    pub duration: Duration,
    pub handshake_duration: Option<Duration>,
}

impl From<TcpOutput> for Value {
    fn from(value: TcpOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("plan".into(), value.plan.into()),
                ("sent".into(), value.sent.into()),
                //("close".into(), value.close.into()),
                ("received".into(), value.received.into()),
                ("errors".into(), value.errors.into()),
                ("duration".into(), value.duration.into()),
                ("handshake_duration".into(), value.handshake_duration.into()),
            ])),
        })
    }
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpCloseOutput {
//    pub timed_out: bool,
//    pub recv_max_reached: bool,
//    pub pattern_match: Option<Vec<u8>>,
//}

#[derive(Debug, Clone)]
pub struct TcpPlanOutput {
    pub host: String,
    pub port: u16,
    pub body: Vec<u8>,
    //pub close: TcpPlanCloseOutput,
}

impl From<TcpPlanOutput> for Value {
    fn from(value: TcpPlanOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("host".into(), Value::String(Arc::new(value.host))),
                ("port".into(), u64::from(value.port).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                //("close".into(), value.close.into()),
            ])),
        })
    }
}

//#[derive(Debug, Clone, Default)]
//pub struct TcpPlanCloseOutput {
//    pub min_duration: Option<Duration>,
//    pub read_pattern: Option<Regex>,
//    pub read_pattern_window: Option<u64>,
//    pub read_length: Option<u64>,
//}
//
//impl From<TcpPlanCloseOutput> for Value {
//    fn from(value: TcpPlanCloseOutput) -> Self {
//        Value::Map(Map {
//            map: Rc::new(HashMap::from([
//                ("min_duration".into(), value.min_duration.into()),
//                ("read_pattern".into(), value.read_pattern.into()),
//                (
//                    "read_pattern_window".into(),
//                    value.read_pattern_window.into(),
//                ),
//                ("read_length".into(), value.read_length.into()),
//            ])),
//        })
//    }
//}

#[derive(Debug, Clone)]
pub struct TcpSentOutput {
    pub dest_ip: String,
    pub dest_port: u16,
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TcpSentOutput> for Value {
    fn from(value: TcpSentOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("dest_ip".into(), Value::String(Arc::new(value.dest_ip))),
                ("dest_port".into(), u64::from(value.dest_port).into()),
                ("body".into(), Value::Bytes(Arc::new(value.body))),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpReceivedOutput {
    pub body: Vec<u8>,
    pub time_to_first_byte: Option<Duration>,
    pub time_to_last_byte: Option<Duration>,
}

impl From<TcpReceivedOutput> for Value {
    fn from(value: TcpReceivedOutput) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([
                ("body".into(), Value::Bytes(Arc::new(value.body.clone()))),
                ("time_to_first_byte".into(), value.time_to_first_byte.into()),
                ("time_to_last_byte".into(), value.time_to_last_byte.into()),
            ])),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TcpError {
    pub kind: String,
    pub message: String,
}

impl From<TcpError> for Value {
    fn from(value: TcpError) -> Self {
        Value::Map(Map {
            map: Rc::new(HashMap::from([("kind".into(), value.kind.into())])),
        })
    }
}
