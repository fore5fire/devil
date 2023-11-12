use std::net::SocketAddr;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{TCPOutput, TCPRequest, TCPResponse};

use super::{State, StepOutput};

pub(super) async fn execute(
    tcp: &TCPRequest,
    state: &State<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    // Get the host and the port
    let host = tcp.host.evaluate(state)?;
    let port = tcp.port.evaluate(state)?;
    //let addr = ip_for_host(&host).await?;
    let addr = format!("{}:{}", host, port);

    // Open a TCP connection to the remote host
    let start = Instant::now();
    let mut stream = TcpStream::connect(addr).await?;
    let body = tcp.body.evaluate(state)?;
    stream.write_all(&body).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    //let stream = Tee::new(stream);

    Ok(StepOutput {
        tcp: Some(TCPOutput {
            host,
            port: port.parse()?,
            body,
            response: TCPResponse {
                body: response,
                duration: start.elapsed(),
            },
        }),
        ..Default::default()
    })
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
