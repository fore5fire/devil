use std::str::FromStr;

use anyhow::bail;
use serde::Serialize;
use strum::EnumString;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Location {
    Udp(UdpLocation, Side),
    Quic(QuicLocation, Side),
    RawTcp(RawTcpLocation, Side),
    Tcp(TcpLocation, Side),
    Tls(TlsLocation, Side),
    Http(HttpLocation, Side),
    Http1(Http1Location, Side),
    RawHttp2(RawHttp2Location, Side),
    Http2(Http2Location, Side),
    Http3(Http3Location, Side),
    GraphQl(GraphQlLocation, Side),
}

impl FromStr for Location {
    type Err = crate::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(".");
        let (Some(proto), Some(loc), Some(side), None) =
            (split.next(), split.next(), split.next(), split.next())
        else {
            bail!("location \"{s}\" should be of format <protocol>.<location>.<side>");
        };
        let side = Side::from_str(side)?;
        match proto {
            "udp" => Ok(Self::Udp(UdpLocation::from_str(loc)?, side)),
            "quic" => Ok(Self::Quic(QuicLocation::from_str(loc)?, side)),
            "raw_tcp" => Ok(Self::RawTcp(RawTcpLocation::from_str(loc)?, side)),
            "tcp" => Ok(Self::Tcp(TcpLocation::from_str(loc)?, side)),
            "tls" => Ok(Self::Tls(TlsLocation::from_str(loc)?, side)),
            "http" => Ok(Self::Http(HttpLocation::from_str(loc)?, side)),
            "http1" => Ok(Self::Http1(Http1Location::from_str(loc)?, side)),
            "raw_http2" => Ok(Self::RawHttp2(RawHttp2Location::from_str(loc)?, side)),
            "http2" => Ok(Self::Http2(Http2Location::from_str(loc)?, side)),
            "http3" => Ok(Self::Http3(Http3Location::from_str(loc)?, side)),
            "graphql" => Ok(Self::GraphQl(GraphQlLocation::from_str(loc)?, side)),
            _ => bail!(r#"location "{s}" contains unsupported protocol "{proto}""#),
        }
    }
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum Side {
    Start,
    End,
    // TODO: add invalid variant for all locations types for better error messages.
    //#[strum(default)]
    //Invalid(String),
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum GraphQlLocation {
    Handshake,
    SendBody,
    ReceiveBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum HttpLocation {
    Open,
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum Http1Location {
    Open,
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum RawHttp2Location {
    Handshake,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum Http2Location {
    Open,
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum Http3Location {
    Open,
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum TlsLocation {
    Handshake,
    SendBody,
    ReceiveBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum TcpLocation {
    Handshake,
    SendBody,
    ReceiveBody,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum RawTcpLocation {
    Handshake,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum QuicLocation {
    Handshake,
}

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[strum(serialize_all = "snake_case")]
pub enum UdpLocation {
    SendBody,
    ReceiveBody,
}
