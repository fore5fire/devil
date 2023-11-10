use std::net::SocketAddr;

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
    let addr = ip_for_host(&host).await?;

    // Open a TCP connection to the remote host
    let mut stream = TcpStream::connect(addr).await?;
    let body = tcp.body.evaluate(state)?;
    stream.write_all(&body).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    //let stream = Tee::new(stream);

    Ok(StepOutput::TCP(TCPOutput {
        response: TCPResponse { body: response },
    }))
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
