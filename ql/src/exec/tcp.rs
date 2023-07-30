use std::net::ToSocketAddrs;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
        return Err("non-http step".into())
    };
    // Get the host and the port
    let  Some(address) = step_body.host.to_socket_addrs()?.next() else {
        return Err("host resolved but returned no IP addresses".into());
    };

    // Open a TCP connection to the remote host
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(step_body.payload.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    //let stream = Tee::new(stream);

    Ok(StepOutput::TCP(TCPOutput { response }))
}
