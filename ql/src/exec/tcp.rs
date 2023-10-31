use std::net::{IpAddr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};

use crate::{Step, StepBody};

use super::{StepInputs, StepOutput};

#[derive(Debug, PartialEq, Clone)]
pub struct TCPOutput {
    pub response: Vec<u8>,
}

pub(super) async fn execute(
    step: &Step<'_>,
    inputs: &StepInputs<'_>,
) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
    let StepBody::TCP(step_body) = &step.body else {
        return Err("non-tcp step".into());
    };
    // Get the host and the port
    let addr = ip_for_host(step_body.address).await?;

    // Open a TCP connection to the remote host
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(step_body.body.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    //let stream = Tee::new(stream);

    Ok(StepOutput::TCP(TCPOutput { response }))
}

async fn ip_for_host(host: &str) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let Some(a) = lookup_host(host).await.map_err(|e| e)?.next() else {
        return Err("host not found".into());
    };
    Ok(a)
}
