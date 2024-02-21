use std::{io, mem, sync::Arc, time::Instant};

use chrono::Duration;
use pnet::{
    datalink,
    packet::ip::IpNextHeaderProtocols,
    transport::{self, TransportChannelType, TransportReceiver, TransportSender},
};
use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite};

use crate::{
    exec::pause::Pause, TcpSegmentsError, TcpSegmentsOutput, TcpSegmentsPauseOutput,
    TcpSegmentsPlanOutput, WithPlannedCapacity,
};

use super::Context;
use crate::Error;

#[derive(Debug)]
pub(super) struct TcpSegmentsRunner {
    ctx: Arc<Context>,
    out: TcpSegmentsOutput,
    state: State,
    start_time: Option<Instant>,
}

enum State {
    Pending {
        pause: TcpSegmentsPauseOutput,
    },
    Open {
        write: TransportSender,
        read: TransportReceiver,
    },
    Completed,
    Invalid,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending { pause } => f.debug_struct("Pending").field("pause", pause).finish(),
            Self::Open { .. } => f.debug_struct("Open").field("transport", &"...").finish(),
            Self::Completed => f.debug_struct("Completed").finish(),
            Self::Invalid => f.debug_struct("Invalid").finish(),
        }
    }
}

impl TcpSegmentsRunner {
    pub fn new(ctx: Arc<Context>, plan: TcpSegmentsPlanOutput) -> Self {
        Self {
            ctx,
            out: TcpSegmentsOutput {
                remote_host: plan.remote_host.clone(),
                remote_port: plan.remote_port,
                sent: Vec::new(),
                local_host: plan.local_host.clone(),
                local_port: plan.local_port,
                received: Vec::new(),
                errors: Vec::new(),
                duration: Duration::zero(),
                handshake_duration: None,
                pause: TcpSegmentsPauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
            state: State::Pending {
                pause: plan.pause.clone(),
            },
            start_time: None,
        }
    }
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Pending { pause } = state else {
            return Err(Box::new(Error(format!(
                "attempt to start TcpRunner from unexpected state: {state:?}",
            ))));
        };
        let start = Instant::now();
        self.out
            .pause
            .handshake
            .start
            .reserve_exact(self.out.plan.pause.handshake.start.len());
        for p in &self.out.plan.pause.handshake.start {
            if p.offset_bytes != 0 {
                return Err(Box::new(Error(
                    "pause offset not yet supported for tcp handshake".to_string(),
                )));
            }
            println!("pausing before tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .start
                .push(Pause::new(&self.ctx, p).await?);
        }
        let Some(interface) = datalink::interfaces()
            .into_iter()
            .find(|e| e.is_up() && !e.is_loopback() && !e.ips.is_empty())
        else {
            return Err(Box::new(Error("no valid interface found".to_owned())));
        };
        let channel = datalink::channel(&interface, datalink::Config::default()).map_err(|e| {
            self.out.errors.push(TcpSegmentsError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::Completed;
            Error(e.to_string())
        })?;
        let (write, read) = transport::transport_channel(
            2048,
            TransportChannelType::Layer4(transport::TransportProtocol::Ipv4(
                IpNextHeaderProtocols::Ipv4,
            )),
        )
        .map_err(|e| {
            self.out.errors.push(TcpSegmentsError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::Completed;
            Error(e.to_string())
        })?;
        //let read_fd = AsyncFd::new(read.socket.fd)?;
        //let readable = read_fd.readable().await?;

        let read = tokio::task::spawn_blocking(move || {
            let iter = transport::tcp_packet_iter(&mut read);
            loop {
                match iter.next() {
                    Ok((packet, addr)) => {}
                    Err(e) => return Err(Box::new(e)),
                }
            }
        });
        let handshake_duration = start.elapsed();
        self.out
            .pause
            .handshake
            .end
            .reserve_exact(self.out.plan.pause.handshake.end.len());
        for p in self.out.plan.pause.handshake.end.iter() {
            if p.offset_bytes != 0 {
                return Err(Box::new(Error(
                    "pause offset not yet supported for tcp handshake".to_string(),
                )));
            }
            println!("pausing after tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .end
                .push(Pause::new(&self.ctx, p).await?);
        }

        self.out.handshake_duration = Some(chrono::Duration::from_std(handshake_duration).unwrap());
        self.state = State::Open { write, read };
        Ok(())
    }
    pub async fn execute(&mut self) {}
    pub fn finish(mut self) -> TcpSegmentsOutput {
        self.complete();
        self.out
    }

    fn complete(&mut self) {
        let end = Instant::now();
        let state = mem::replace(&mut self.state, State::Completed);
        self.out.duration = self
            .start_time
            .map(|start| end - start)
            .map(Duration::from_std)
            .transpose()
            .expect("durations should fit in chrono")
            .unwrap_or_else(|| Duration::zero())
    }
}

impl AsyncRead for TcpSegmentsRunner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
    }
}

impl AsyncWrite for TcpSegmentsRunner {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
    }
}
