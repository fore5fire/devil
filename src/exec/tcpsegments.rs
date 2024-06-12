use std::{
    io, mem,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::Poll,
    time::Instant,
};

use chrono::Duration;
use futures::{Future, TryFutureExt};
use itertools::Itertools;
use pnet::{
    packet::{
        ip::IpNextHeaderProtocols,
        tcp::{self, MutableTcpPacket, TcpFlags, TcpOption, TcpOptionNumbers, TcpPacket},
        Packet,
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
    exec::pause::Pause, TcpSegmentOptionOutput, TcpSegmentOutput, TcpSegmentsError,
    TcpSegmentsOutput, TcpSegmentsPauseOutput, TcpSegmentsPlanOutput, WithPlannedCapacity,
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

type OwnedBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug)]
enum State {
    Pending { pause: TcpSegmentsPauseOutput },
    Open(OpenState),
    Completed,
    Invalid,
}

#[derive(Debug)]
struct OpenState {
    write: WriteState,
    write_done: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    read: mpsc::UnboundedReceiver<TcpSegmentOutput>,
    read_done: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
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

impl TcpSegmentsRunner {
    pub fn new(ctx: Arc<Context>, plan: TcpSegmentsPlanOutput) -> Self {
        Self {
            ctx,
            state: State::Pending {
                pause: plan.pause.clone(),
            },
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

        let (mut write, mut read) = transport::transport_channel(
            2048,
            TransportChannelType::Layer4(transport::TransportProtocol::Ipv4(
                IpNextHeaderProtocols::Tcp,
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

        // TODO: Use AsyncFd instead of one thread per request.
        //let read_fd = AsyncFd::new(read.socket.fd)?;
        //let readable = read_fd.readable().await?;
        let (send_read, receive_read) = mpsc::unbounded_channel();
        let read_done = tokio::task::spawn_blocking(
            move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                let mut iter = transport::tcp_packet_iter(&mut read);
                while !send_read.is_closed() {
                    match iter.next_with_timeout(std::time::Duration::from_millis(100)) {
                        // Timed out, loop to check that we're still running.
                        Ok(None) => {}
                        // Value Received, forward it.
                        Ok(Some((packet, _))) => {
                            send_read.send(TcpSegmentOutput {
                                sequence_number: packet.get_sequence(),
                                flags: packet.get_flags(),
                                source: packet.get_source(),
                                window: packet.get_window(),
                                acknowledgment: packet.get_acknowledgement(),
                                checksum: Some(packet.get_checksum()),
                                data_offset: packet.get_data_offset(),
                                destination: packet.get_destination(),
                                reserved: packet.get_reserved(),
                                urgent_ptr: packet.get_urgent_ptr(),
                                options: packet
                                    .get_options_iter()
                                    .map(|opts| match opts.get_number() {
                                        TcpOptionNumbers::NOP if opts.payload().len() == 0 => {
                                            TcpSegmentOptionOutput::Nop
                                        }
                                        TcpOptionNumbers::TIMESTAMPS
                                            if opts.payload().len() == 8 =>
                                        {
                                            TcpSegmentOptionOutput::Timestamps {
                                                tsval: u32::from_be_bytes(
                                                    opts.payload()[..4].try_into().unwrap(),
                                                ),
                                                tsecr: u32::from_be_bytes(
                                                    opts.payload()[4..8].try_into().unwrap(),
                                                ),
                                            }
                                        }
                                        TcpOptionNumbers::MSS if opts.payload().len() == 2 => {
                                            TcpSegmentOptionOutput::Mss(u16::from_be_bytes(
                                                opts.payload().try_into().unwrap(),
                                            ))
                                        }
                                        TcpOptionNumbers::WSCALE if opts.payload().len() == 1 => {
                                            TcpSegmentOptionOutput::Wscale(u8::from_be_bytes(
                                                opts.payload().try_into().unwrap(),
                                            ))
                                        }
                                        TcpOptionNumbers::SACK_PERMITTED
                                            if opts.payload().len() == 0 =>
                                        {
                                            TcpSegmentOptionOutput::SackPermitted
                                        }
                                        _ => TcpSegmentOptionOutput::Raw {
                                            kind: opts.get_number().0,
                                            value: opts.payload().to_vec(),
                                        },
                                    })
                                    .collect(),
                                payload: packet.payload().to_vec(),
                            })?;
                        }
                        Err(e) => return Err(Box::new(e)),
                    }
                }
                Ok(())
            },
        );

        let Some(remote_addr) =
            net::lookup_host(format!("{}:{}", self.out.remote_host, self.out.remote_port))
                .await?
                .next()
        else {
            self.out.errors.push(TcpSegmentsError {
                kind: "dns lookup".to_owned(),
                message: format!("dns lookup for {} failed", self.out.remote_host),
            });
            return Err(Box::new(crate::Error(format!(
                "dns lookup for {} failed",
                self.out.remote_host
            ))));
        };

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

        let local_addr =
            IpAddr::from_str(&self.out.plan.local_address).map_err(|e| Error(e.to_string()))?;
        let write_sequence_number = self.out.plan.isn;
        let Ok((segment, send_write)) = Self::send(
            TcpSegmentOutput {
                source: self.out.local_port,
                destination: self.out.remote_port,
                sequence_number: write_sequence_number,
                flags: TcpFlags::SYN,
                // TODO: congestion control if no window specified?
                window: self.out.plan.window.unwrap_or(u16::MAX),
                acknowledgment: 0,
                checksum: None,
                data_offset: 0,
                urgent_ptr: 0,
                reserved: 0,
                options: Vec::new(),
                payload: Vec::new(),
            },
            local_addr,
            remote_addr.ip(),
            send_write,
        )
        .await
        else {
            self.out.errors.push(TcpSegmentsError {
                kind: "dns lookup".to_owned(),
                message: format!("dns lookup for {} failed", self.out.remote_host),
            });
            return Err(Box::new(crate::Error(format!(
                "dns lookup for {} failed",
                self.out.remote_host
            ))));
        };
        self.out.sent.push(segment.unwrap());

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
        self.state = State::Open(OpenState {
            write: WriteState::Ready(send_write),
            write_done,
            write_sequence_number,
            read: receive_read,
            read_done,
            pending: Vec::new(),
            pending_at: 0,
        });
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

    async fn send<'a>(
        segment: TcpSegmentOutput,
        checksum_source: IpAddr,
        checksum_destination: IpAddr,
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
            match (checksum_source, checksum_destination) {
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
                self.out.errors.push(TcpSegmentsError {
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

impl AsyncRead for TcpSegmentsRunner {
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

impl AsyncWrite for TcpSegmentsRunner {
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
                    source: self.out.local_port,
                    destination: self.out.remote_port,
                    sequence_number: state.write_sequence_number,
                    flags: 0,
                    window: 0,
                    acknowledgment: 0,
                    checksum: 0,
                    data_offset: 0,
                    urgent_ptr: 0,
                    reserved: 0,
                    options: Vec::new(),
                    payload: Vec::from(buf),
                },
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
                    source: self.out.plan.local_port,
                    destination: self.out.plan.remote_port,
                    //state: State::Pending {
                    //    pause: plan.pause.clone(),
                    //},
                    sequence_number: state.write_sequence_number,
                    acknowledgment: 0,
                    data_offset: 0,
                    reserved: 0,
                    flags: TcpFlags::FIN,
                    window: 0,
                    checksum: 0,
                    urgent_ptr: 0,
                    options: Vec::new(),
                    payload: Vec::new(),
                },
                sender,
            )),
            WriteState::Pending(pending) => pending,
            WriteState::Invalid => panic!("invalid write state for shutdown"),
        };
        self.pending_write(cx, pending, state)
    }
}
