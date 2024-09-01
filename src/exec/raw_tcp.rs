use std::{io, mem, net::IpAddr, pin::Pin, str::FromStr, sync::Arc, task::Poll, time::Instant};

use anyhow::{anyhow, bail};
use chrono::Duration;
use futures::{Future, TryFutureExt};
use itertools::Itertools;
use pnet::{
    packet::{
        ip::IpNextHeaderProtocols,
        tcp::{self, MutableTcpPacket, TcpFlags, TcpOption, TcpPacket},
    },
    transport::{self, TransportChannelType},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net,
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    exec::pause::Pause, RawTcpError, RawTcpOutput, RawTcpPauseOutput, RawTcpPlanOutput,
    TcpSegmentOptionOutput, TcpSegmentOutput, WithPlannedCapacity,
};

use super::{tcp_common, Context};

#[derive(Debug)]
pub(super) struct RawTcpRunner {
    ctx: Arc<Context>,
    out: RawTcpOutput,
    state: State,
    start_time: Option<Instant>,
    remote_ip: Option<IpAddr>,
    local_ip: Option<IpAddr>,
}

type OwnedBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug)]
enum State {
    Pending { pause: RawTcpPauseOutput },
    Open(OpenState),
    Completed,
    Invalid,
}

#[derive(Debug)]
struct OpenState {
    write: WriteState,
    write_done: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    read: mpsc::UnboundedReceiver<TcpSegmentOutput>,
    pending: Vec<u8>,
    pending_at: usize,
    write_sequence_number: u32,
}

enum WriteState {
    Ready(mpsc::Sender<Option<TcpPacket<'static>>>),
    Pending(
        OwnedBoxFuture<
            io::Result<(
                Option<TcpSegmentOutput>,
                mpsc::Sender<Option<TcpPacket<'static>>>,
            )>,
        >,
    ),
    Invalid,
}

impl std::fmt::Debug for WriteState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready(sender) => f.debug_tuple("WriteState::Ready").field(sender).finish(),
            Self::Pending(_) => f.debug_tuple("WriteState::Pending").field(&"?").finish(),
            Self::Invalid => f.debug_tuple("WriteState::Invalid").finish(),
        }
    }
}

impl RawTcpRunner {
    pub fn new(ctx: Arc<Context>, plan: RawTcpPlanOutput) -> Self {
        Self {
            ctx,
            state: State::Pending {
                pause: plan.pause.clone(),
            },
            start_time: None,
            remote_ip: None,
            local_ip: None,
            out: RawTcpOutput {
                dest_ip: String::new(),
                dest_port: plan.dest_port,
                sent: Vec::new(),
                src_host: String::new(),
                src_port: 0,
                received: Vec::new(),
                errors: Vec::new(),
                duration: Duration::zero(),
                handshake_duration: None,
                pause: RawTcpPauseOutput::with_planned_capacity(&plan.pause),
                plan,
            },
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Pending { pause } = state else {
            bail!("attempt to start TcpRunner from unexpected state: {state:?}");
        };

        let Some(remote_addr) = net::lookup_host(format!(
            "{}:{}",
            self.out.plan.dest_host, self.out.plan.dest_port
        ))
        .await
        .map_err(|e| {
            anyhow!(
                "lookup host '{}:{}': {e}",
                self.out.plan.dest_host,
                self.out.plan.dest_port
            )
        })?
        .next() else {
            self.out.errors.push(RawTcpError {
                kind: "dns lookup".to_owned(),
                message: format!(
                    "no A records found for tcp_segments.dest_host '{}'",
                    self.out.plan.dest_host
                ),
            });
            bail!(
                "no A records found for tcp_segments.dest_host '{}'",
                self.out.plan.dest_host
            );
        };

        let (mut write, read) = transport::transport_channel(
            65535,
            TransportChannelType::Layer4(transport::TransportProtocol::Ipv4(
                IpNextHeaderProtocols::Tcp,
            )),
        )
        .inspect_err(|e| {
            self.out.errors.push(RawTcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::Completed;
        })?;

        let src_host = self
            .out
            .plan
            .src_host
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_owned());
        let src_port = self.out.plan.src_port.unwrap_or_else(|| 8888);
        self.remote_ip = Some(remote_addr.ip());
        self.local_ip = Some(
            IpAddr::from_str(&src_host)
                .map_err(|e| anyhow!("parse tcp_segments.src_host '{src_host}': {e}"))?,
        );
        // Record the actual resolved destination and source IPs for the output.
        self.out.dest_ip = remote_addr.to_string();
        self.out.src_host = src_host;
        self.out.src_port = src_port;

        let start = Instant::now();
        let receive_read = tcp_common::reader(read, remote_addr, start);

        // TODO: Use AsyncFd instead of one thread per request.
        //let write_fd = AsyncFd::new(write.socket.fd)?;
        //let readable = read_fd.readable().await?;
        let (send_write, mut receive_write) = mpsc::channel::<Option<TcpPacket<'static>>>(1);
        let write_done = tokio::task::spawn_blocking(
            move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                while let Some(packet) = receive_write.blocking_recv() {
                    // Do nothing if this is just a sync message.
                    let Some(packet) = packet else {
                        continue;
                    };

                    match write.send_to(packet, remote_addr.ip()) {
                        Ok(_) => {}
                        Err(e) => return Err(Box::new(e)),
                    }
                }
                Ok(())
            },
        );

        self.out
            .pause
            .handshake
            .start
            .reserve_exact(self.out.plan.pause.handshake.start.len());
        for p in &self.out.plan.pause.handshake.start {
            if p.offset_bytes != 0 {
                bail!("pause offset not yet supported for tcp handshake");
            }
            println!("pausing before tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .start
                .push(Pause::new(&self.ctx, p).await?);
        }

        let write_sequence_number = self.out.plan.isn;
        let (segment, send_write) = Self::send(
            TcpSegmentOutput {
                source: self.out.src_port,
                destination: self.out.dest_port,
                sequence_number: write_sequence_number,
                flags: TcpFlags::SYN,
                // TODO: congestion control if no window specified?
                window: self.out.plan.window,
                acknowledgment: 0,
                checksum: None,
                // Packet with no options or padding is 5 32-bit words.
                data_offset: Self::data_offset(&Vec::new()),
                urgent_ptr: 0,
                reserved: 0,
                options: Vec::new(),
                payload: Vec::new(),
                received: None,
                sent: None,
            },
            self.local_ip.unwrap(),
            self.remote_ip.unwrap(),
            send_write,
        )
        .await
        .inspect_err(|e| {
            self.out.errors.push(RawTcpError {
                kind: "handshake".to_owned(),
                message: format!("send SYN to '{}' failed: {e}", self.out.dest_ip),
            });
        })
        .map_err(|e| anyhow!("send SYN to '{}' failed: {e}", self.out.dest_ip))?;
        self.out.sent.push(segment.unwrap());

        let handshake_duration = start.elapsed();
        self.out
            .pause
            .handshake
            .end
            .reserve_exact(self.out.plan.pause.handshake.end.len());
        for p in self.out.plan.pause.handshake.end.iter() {
            if p.offset_bytes != 0 {
                bail!("pause offset not yet supported for tcp handshake");
            }
            println!("pausing after tcp handshake for {:?}", p.duration);
            self.out
                .pause
                .handshake
                .end
                .push(Pause::new(&self.ctx, p).await?);
        }

        self.out.handshake_duration = Some(chrono::Duration::from_std(handshake_duration).unwrap());
        self.state = State::Open(OpenState {
            write: WriteState::Ready(send_write),
            write_done,
            write_sequence_number,
            read: receive_read,
            pending: Vec::new(),
            pending_at: 0,
        });
        Ok(())
    }

    pub async fn execute(&mut self) {}

    pub fn finish(mut self) -> RawTcpOutput {
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

    fn data_offset(options: &Vec<TcpSegmentOptionOutput>) -> u8 {
        options
            .iter()
            .fold(0, |acc, option| acc + option.size())
            .div_ceil(4)
            .checked_add(5)
            .map(|x| x.try_into().ok())
            .flatten()
            .expect("tcp data offset calculation should not exceed 255")
    }

    async fn send<'a>(
        segment: TcpSegmentOutput,
        src_addr: IpAddr,
        dest_addr: IpAddr,
        send: mpsc::Sender<Option<TcpPacket<'static>>>,
    ) -> io::Result<(
        Option<TcpSegmentOutput>,
        mpsc::Sender<Option<TcpPacket<'static>>>,
    )> {
        let mut packet = MutableTcpPacket::owned(vec![
            0;
            TcpPacket::minimum_packet_size()
                + segment.payload.len()
        ])
        .expect("tcp segments should always be allocated with enough memory");

        packet.set_source(segment.source);
        packet.set_destination(segment.destination);
        packet.set_flags(segment.flags);
        packet.set_sequence(segment.sequence_number);
        packet.set_window(segment.window);
        packet.set_payload(&segment.payload);
        packet.set_reserved(segment.reserved);
        packet.set_urgent_ptr(segment.urgent_ptr);
        packet.set_data_offset(segment.data_offset);
        packet.set_acknowledgement(segment.acknowledgment);
        packet.set_options(
            &segment
                .options
                .iter()
                .map(|opt| match opt {
                    TcpSegmentOptionOutput::Nop => TcpOption::nop(),
                    TcpSegmentOptionOutput::Timestamps { tsval, tsecr } => {
                        TcpOption::timestamp(*tsval, *tsecr)
                    }
                    TcpSegmentOptionOutput::Mss(val) => TcpOption::mss(*val),
                    TcpSegmentOptionOutput::Wscale(val) => TcpOption::wscale(*val),
                    TcpSegmentOptionOutput::SackPermitted => TcpOption::sack_perm(),
                    TcpSegmentOptionOutput::Sack(acks) => TcpOption::selective_ack(acks),
                    TcpSegmentOptionOutput::Raw { .. } => {
                        panic!("sending raw tcp segment options is not yet supported")
                    }
                })
                .collect_vec(),
        );
        if let Some(checksum) = segment.checksum {
            packet.set_checksum(checksum);
        } else {
            match (src_addr, dest_addr) {
                (IpAddr::V4(source), IpAddr::V4(dest)) => {
                    packet.set_checksum(tcp::ipv4_checksum(&packet.to_immutable(), &source, &dest))
                }
                (IpAddr::V6(source), IpAddr::V6(dest)) => {
                    packet.set_checksum(tcp::ipv6_checksum(&packet.to_immutable(), &source, &dest))
                }
                _ => {
                    return Err(io::Error::other(
                        "source and address must use same IP version for checksum calculation",
                    ))
                }
            }
        }

        send.send(Some(packet.consume_to_immutable()))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::ConnectionReset))?;
        Ok((Some(segment), send))
    }

    fn pending_write(
        &mut self,
        cx: &mut std::task::Context<'_>,
        mut pending: OwnedBoxFuture<
            io::Result<(
                Option<TcpSegmentOutput>,
                mpsc::Sender<Option<TcpPacket<'static>>>,
            )>,
        >,
        mut state: OpenState,
    ) -> Poll<io::Result<()>> {
        match (&mut pending).try_poll_unpin(cx) {
            Poll::Ready(Ok((output, sender))) => {
                state.write = WriteState::Ready(sender);
                self.state = State::Open(state);
                if let Some(output) = output {
                    self.out.sent.push(output);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self.state = State::Completed;
                self.out.errors.push(RawTcpError {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset)))
            }
            Poll::Pending => {
                state.write = WriteState::Pending(pending);
                self.state = State::Open(state);
                Poll::Pending
            }
        }
    }
}

impl AsyncRead for RawTcpRunner {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Open(mut state) = state else {
            self.state = state;
            panic!("invalid state to poll_read: {:?}", self.state)
        };

        // Get some data if we don't have any waiting.
        while state.pending_at >= state.pending.len() {
            match state.read.poll_recv(cx) {
                Poll::Ready(Some(segment)) => {
                    state.pending_at = 0;
                    state.pending.truncate(0);
                    state.pending.extend(&segment.payload);
                    self.out.received.push(segment);
                }
                // Channel has closed so return without writing to buf to indicate we're done.
                Poll::Ready(None) => {
                    self.state = State::Open(state);
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    self.state = State::Open(state);
                    return Poll::Pending;
                }
            };
        }

        // Send data.
        let write_count = buf.remaining().min(state.pending.len() - state.pending_at);
        buf.put_slice(&state.pending[state.pending_at..write_count]);
        state.pending_at += write_count;
        self.state = State::Open(state);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for RawTcpRunner {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Open(mut state) = state else {
            self.state = state;
            panic!("invalid state to poll_read: {:?}", self.state)
        };
        let pending = match mem::replace(&mut state.write, WriteState::Invalid) {
            WriteState::Ready(sender) => Box::pin(Self::send(
                TcpSegmentOutput {
                    source: self.out.src_port,
                    destination: self.out.dest_port,
                    sequence_number: state.write_sequence_number,
                    flags: 0,
                    window: 0,
                    acknowledgment: 0,
                    checksum: None,
                    data_offset: 0,
                    urgent_ptr: 0,
                    reserved: 0,
                    options: Vec::new(),
                    payload: Vec::from(buf),
                    received: None,
                    sent: None,
                },
                self.local_ip.unwrap(),
                self.remote_ip.unwrap(),
                sender,
            )),
            WriteState::Pending(pending) => pending,
            WriteState::Invalid => panic!("invalid write state"),
        };
        self.pending_write(cx, pending, state)
            .map_ok(|()| {
                if let State::Open(OpenState {
                    write_sequence_number,
                    ..
                }) = &mut self.state
                {
                    *write_sequence_number = write_sequence_number.wrapping_add(buf.len() as u32);
                } else {
                    println!(
                        "not incrementing sequence number since state is {:?} not open",
                        self.state
                    )
                }
                buf.len()
            })
            .map_err(|_| {
                self.state = State::Completed;
                io::Error::from(io::ErrorKind::ConnectionReset)
            })
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Open(mut state) = state else {
            self.state = state;
            panic!("invalid state to poll_read: {:?}", self.state)
        };
        let pending = match mem::replace(&mut state.write, WriteState::Invalid) {
            WriteState::Ready(sender) => {
                let future = Box::pin(async move {
                    // Send None twice - once to fill the buffer and another to ensure the first was
                    // read from the buffer.
                    sender
                        .send(None)
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::ConnectionReset))?;
                    sender
                        .send(None)
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::ConnectionReset))?;
                    Result::Ok((Option::<TcpSegmentOutput>::None, sender))
                });
                future
            }
            WriteState::Pending(pending) => pending,
            WriteState::Invalid => panic!("invalid write state for flush"),
        };
        self.pending_write(cx, pending, state)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let state = mem::replace(&mut self.state, State::Invalid);
        let State::Open(mut state) = state else {
            self.state = state;
            panic!("invalid state to poll_read: {:?}", self.state)
        };
        let pending = match mem::replace(&mut state.write, WriteState::Invalid) {
            WriteState::Ready(sender) => Box::pin(Self::send(
                TcpSegmentOutput {
                    source: self.out.src_port,
                    destination: self.out.dest_port,
                    //state: State::Pending {
                    //    pause: plan.pause.clone(),
                    //},
                    sequence_number: state.write_sequence_number,
                    acknowledgment: 0,
                    data_offset: 0,
                    reserved: 0,
                    flags: TcpFlags::FIN,
                    window: 0,
                    checksum: None,
                    urgent_ptr: 0,
                    options: Vec::new(),
                    payload: Vec::new(),
                    received: None,
                    sent: None,
                },
                self.local_ip.unwrap(),
                self.remote_ip.unwrap(),
                sender,
            )),
            WriteState::Pending(pending) => pending,
            WriteState::Invalid => panic!("invalid write state for shutdown"),
        };
        self.pending_write(cx, pending, state)
    }
}
