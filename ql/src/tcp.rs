use tokio::net::ToSocketAddrs;

use crate::ReadUntil;

#[derive(Debug, PartialEq, Eq)]
pub struct TCPRequest<'a, T: ToSocketAddrs> {
    pub host: T,
    pub read_until: ReadUntil<'a>,
    pub payload: &'a str,
}
