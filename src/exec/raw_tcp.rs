use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io, net::IpAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use cel_interpreter::Duration;
use chrono::TimeDelta;
use futures::Future;
use itertools::Itertools;
use pnet::packet::tcp::{self, MutableTcpPacket, TcpOption};
use pnet::{
    packet::{ip::IpNextHeaderProtocols, tcp::TcpPacket},
    transport::{self, TransportChannelType},
};
use pnet::{
    packet::{tcp::TcpOptionNumbers, Packet},
    transport::TransportReceiver,
};
use tokio::join;
use tokio::net::TcpSocket;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::{
    net,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, info};

use crate::{
    Direction, PduName, ProtocolDiscriminants, ProtocolName, RawTcpError, RawTcpOutput,
    RawTcpPlanOutput, TcpSegmentOptionOutput, TcpSegmentOutput,
};

use super::Context;

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
    Pending,
    Open(OpenState),
    Passive {
        writes: JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>,
        writes_done: oneshot::Sender<usize>,
        reads: JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>,
        reads_done: oneshot::Sender<usize>,
        remote_addr: SocketAddr,
        local_addr: SocketAddr,
    },
    Completed {
        reads: JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>,
    },
    CompletedPassive {
        reads: JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>,
        writes: JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>,
    },
    CompletedEmpty,
    Invalid,
}

#[derive(Debug)]
struct OpenState {
    send_write: mpsc::UnboundedSender<Option<TcpPacket<'static>>>,
    write_done: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    reads: Option<JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)>>,
    reads_done: oneshot::Sender<usize>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    send_segments: Vec<TcpSegmentOutput>,
}

impl RawTcpRunner {
    pub fn new(ctx: Arc<Context>, plan: RawTcpPlanOutput) -> Self {
        Self {
            state: State::Pending,
            start_time: None,
            remote_ip: None,
            local_ip: None,
            out: RawTcpOutput {
                name: ProtocolName::with_job(ctx.job_name.clone(), ProtocolDiscriminants::RawTcp),
                dest_ip: String::new(),
                dest_port: plan.dest_port,
                sent: Vec::new(),
                src_host: String::new(),
                src_port: 0,
                received: Vec::new(),
                errors: Vec::new(),
                duration: TimeDelta::zero().into(),
                handshake_duration: None,
                plan,
            },
            ctx,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut self.state, State::Invalid);
        let State::Pending = state else {
            bail!("attempt to start TcpRunner from unexpected state: {state:?}");
        };

        // DNS lookup for remote address.
        let Some(remote_addr) = net::lookup_host(format!(
            "{}:{}",
            self.out.plan.dest_host, self.out.plan.dest_port,
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
                    "no A records found for raw_tcp.dest_host '{}'",
                    self.out.plan.dest_host
                ),
            });
            bail!(
                "no A records found for raw_tcp.dest_host '{}'",
                self.out.plan.dest_host
            );
        };

        // DNS lookup for local address.
        let src_host = self
            .out
            .plan
            .src_host
            .clone()
            .unwrap_or_else(|| "localhost".to_owned());
        let src_port = self.out.plan.src_port.unwrap_or(0);
        let Some(local_addr) = net::lookup_host(format!("{}:{}", src_host, src_port,))
            .await
            .map_err(|e| {
                anyhow!(
                    "lookup host '{}:{}': {e}",
                    self.out.plan.dest_host,
                    self.out.plan.dest_port
                )
            })?
            .next()
        else {
            self.out.errors.push(RawTcpError {
                kind: "dns lookup".to_owned(),
                message: format!(
                    "no A records found for raw_tcp.src_host '{}'",
                    self.out.plan.dest_host
                ),
            });
            bail!(
                "no A records found for raw_tcp.src_host '{}'",
                self.out.plan.dest_host
            );
        };

        // Bind a temporary tcp socket to let the OS resolve our final local device and port.
        let tmp_socket = if local_addr.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .inspect_err(|e| {
            self.out.errors.push(RawTcpError {
                kind: e.kind().to_string(),
                message: e.to_string(),
            });
            self.state = State::CompletedEmpty;
        })?;
        let local_addr = tmp_socket
            .bind(local_addr)
            .and_then(|()| tmp_socket.local_addr())
            .inspect_err(|e| {
                self.out.errors.push(RawTcpError {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                });
                self.state = State::CompletedEmpty;
            })?;
        // Record the actual resolved destination and source IPs for the output.
        self.out.dest_ip = remote_addr.to_string();
        self.out.src_host = local_addr.ip().to_string();
        self.out.src_port = local_addr.port();

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
            self.state = State::CompletedEmpty;
        })?;

        let counter = Arc::new(AtomicU64::new(0));

        let start = || {
            let start = Instant::now();
            self.start_time = Some(start);
            let (reads_done, recv_reads_done) = oneshot::channel();
            let reads = reader(
                read,
                remote_addr,
                start,
                recv_reads_done,
                Direction::Recv,
                self.out.name.clone(),
                counter.clone(),
            );
            (start, reads, reads_done)
        };

        let write_sequence_number = self.out.plan.isn;

        if self.out.plan.segments.is_empty() {
            let (start, reads, reads_done) = start();
            let (_, read) = transport::transport_channel(
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
                self.state = State::CompletedEmpty;
            })?;
            let (writes_done, recv_writes_done) = oneshot::channel();
            self.state = State::Passive {
                writes: reader(
                    read,
                    local_addr,
                    start,
                    recv_writes_done,
                    Direction::Send,
                    self.out.name.clone(),
                    counter,
                ),
                writes_done,
                reads,
                reads_done,
                remote_addr,
                local_addr,
            }
        } else {
            let send_segments = self
                .out
                .plan
                .segments
                .iter()
                .map(|seg| seg.deref().clone())
                .collect();
            let (_, reads, reads_done) = start();
            // TODO: Use AsyncFd instead of one thread per request.
            //let write_fd = AsyncFd::new(write.socket.fd)?;
            //let readable = read_fd.readable().await?;
            let (send_write, mut receive_write) =
                mpsc::unbounded_channel::<Option<TcpPacket<'static>>>();
            let write_done = tokio::task::spawn_blocking(move || {
                while let Some(packet) = receive_write.blocking_recv() {
                    // Do nothing if this is just a sync message.
                    let Some(packet) = packet else {
                        continue;
                    };

                    write.send_to(packet, remote_addr.ip())?;
                }
                Ok(())
            });
            self.state = State::Open(OpenState {
                send_write,
                write_done,
                reads: Some(reads),
                reads_done,
                local_addr,
                remote_addr,
                send_segments,
            });
        }
        Ok(())
    }

    pub fn resolved_addrs(&self) -> (SocketAddr, SocketAddr) {
        match &self.state {
            State::Open(OpenState {
                remote_addr,
                local_addr,
                ..
            })
            | State::Passive {
                remote_addr,
                local_addr,
                ..
            } => (*local_addr, *remote_addr),
            s => panic!("invalid state to get resolved ips: {s:?}"),
        }
    }

    pub async fn execute(&mut self) {
        let State::Open(state) = &mut self.state else {
            panic!("invalid state to execute raw_tcp: {:?}", self.state);
        };
        let send_segments = mem::take(&mut state.send_segments);
        let reads = mem::take(&mut state.reads).expect("reads handle should be set on execute");
        // TODO: implement pauses, probably moving send and recieve into the same task or with a
        // mutex.
        let (send, receive) = join!(
            async {
                for segment in send_segments {
                    self.send(segment)?
                }
                Ok::<_, anyhow::Error>(())
            },
            reads
        );
        if let Err(e) = send {
            self.out.errors.push(RawTcpError {
                kind: "send error".to_owned(),
                message: e.to_string(),
            });
        };
        if let Err(e) = receive {
            self.out.errors.push(RawTcpError {
                kind: "receive error".to_owned(),
                message: e.to_string(),
            });
        };
        self.shutdown(0, 0);
    }

    pub async fn finish(mut self) -> RawTcpOutput {
        self.shutdown(0, 0);
        match self.state {
            State::CompletedPassive { reads, writes } => {
                let (reads, writes) = join!(reads, writes);
                let (reads, read_err) = reads.expect("raw_tcp read handler should not panic");
                self.out.received = reads;
                if let Some(e) = read_err {
                    self.out.errors.push(RawTcpError {
                        kind: e.kind().to_string(),
                        message: e.to_string(),
                    });
                }

                let (writes, write_err) = writes.expect("raw_tcp write handler should not panic");
                self.out.sent = writes;
                if let Some(e) = write_err {
                    self.out.errors.push(RawTcpError {
                        kind: e.kind().to_string(),
                        message: e.to_string(),
                    });
                }
            }
            State::Completed { reads } => {
                let (reads, read_err) = reads.await.expect("raw_tcp read handler should not panic");
                self.out.received = reads;
                if let Some(e) = read_err {
                    self.out.errors.push(RawTcpError {
                        kind: e.kind().to_string(),
                        message: e.to_string(),
                    });
                }
            }
            State::CompletedEmpty => {}
            state => panic!("invalid state to finish raw_tcp: {state:?}"),
        };
        self.out
    }

    pub fn shutdown(&mut self, expect_read_len: usize, expect_write_len: usize) {
        let end = Instant::now();

        match mem::replace(&mut self.state, State::Invalid) {
            State::Open(OpenState {
                reads_done, reads, ..
            }) => {
                _ = reads_done.send(expect_read_len);
                self.state = State::Completed {
                    reads: reads.expect("reads handle should be set at shutdown"),
                }
            }
            State::Passive {
                writes_done,
                writes,
                reads_done,
                reads,
                ..
            } => {
                _ = reads_done.send(expect_read_len);
                _ = writes_done.send(expect_write_len);
                self.state = State::CompletedPassive { reads, writes }
            }
            state => self.state = state,
        }

        self.out.duration = self
            .start_time
            .map(|start| end - start)
            .map(TimeDelta::from_std)
            .transpose()
            .expect("durations should fit in chrono")
            .unwrap_or_else(|| TimeDelta::zero())
            .into();
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

    pub fn send(&mut self, mut segment: TcpSegmentOutput) -> io::Result<()> {
        let State::Open(OpenState {
            send_write,
            local_addr,
            remote_addr,
            ..
        }) = &mut self.state
        else {
            panic!("invalid state to send: {:?}", self.state);
        };
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
                    TcpSegmentOptionOutput::Nop(_) => TcpOption::nop(),
                    TcpSegmentOptionOutput::Timestamps { tsval, tsecr } => {
                        TcpOption::timestamp(*tsval, *tsecr)
                    }
                    TcpSegmentOptionOutput::Mss(val) => TcpOption::mss(*val),
                    TcpSegmentOptionOutput::Wscale(val) => TcpOption::wscale(*val),
                    TcpSegmentOptionOutput::SackPermitted(_) => TcpOption::sack_perm(),
                    TcpSegmentOptionOutput::Sack(acks) => TcpOption::selective_ack(acks),
                    TcpSegmentOptionOutput::Generic { .. } => {
                        panic!("sending generic tcp segment options is not yet supported")
                    }
                })
                .collect_vec(),
        );
        if let Some(checksum) = segment.checksum {
            packet.set_checksum(checksum);
        } else {
            match (local_addr.ip(), remote_addr.ip()) {
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
            segment.checksum = Some(packet.get_checksum());
        }

        send_write
            .send(Some(packet.consume_to_immutable()))
            .map_err(|e| {
                self.out.errors.push(RawTcpError {
                    kind: "".to_owned(),
                    message: format!("send SYN to '{}' failed: {e}", self.out.dest_ip),
                });
                io::Error::from(io::ErrorKind::ConnectionReset)
            })?;
        // Now that the segment is sent it will never be mutated again, and we're already incuring
        // the cost of moving it into the Vec's backing memory, so wrap it in an Arc here.
        self.out.sent.push(Arc::new(segment));
        Ok(())
    }
}

pub fn reader(
    mut read: TransportReceiver,
    target_addr: SocketAddr,
    start: Instant,
    mut done: oneshot::Receiver<usize>,
    direction: Direction,
    proto: ProtocolName,
    counter: Arc<AtomicU64>,
) -> JoinHandle<(Vec<Arc<TcpSegmentOutput>>, Option<io::Error>)> {
    // TODO: Use AsyncFd instead of one thread per request.
    //let read_fd = AsyncFd::new(read.socket.fd)?;
    //let readable = read_fd.readable().await?;
    tokio::task::spawn_blocking(move || {
        let mut total_size = 0;
        let mut seen_fin = false;
        let mut out = Vec::new();
        let mut iter = transport::tcp_packet_iter(&mut read);
        let expect_size = loop {
            match done.try_recv() {
                Ok(expect) => break expect,
                Err(TryRecvError::Closed) => return (out, None),
                Err(TryRecvError::Empty) => {}
            }
            match iter.next_with_timeout(std::time::Duration::from_millis(50)) {
                // Timed out, loop to check that we're still running.
                Ok(None) => {}
                // Value Received, process it.
                Ok(Some((packet, src_ip))) => {
                    if src_ip != target_addr.ip() || packet.get_source() != target_addr.port() {
                        debug!("filtered packet from {src_ip}:{}", packet.get_source());
                        continue;
                    }
                    debug!("recording packet from {src_ip}:{}", packet.get_source());
                    total_size += packet.payload().len();
                    if packet.get_flags() & pnet::packet::tcp::TcpFlags::FIN != 0 {
                        info!("got fin packet from {src_ip}:{}", packet.get_source());
                        seen_fin = true;
                    }
                    out.push(packet_to_output(
                        packet,
                        start,
                        PduName::with_protocol(
                            proto.clone(),
                            counter.fetch_add(1, Ordering::Relaxed),
                        ),
                        direction,
                    ));
                }
                // Network failure, bail.
                Err(e) => return (out, Some(e)),
            }
        };
        while !seen_fin || total_size < expect_size {
            match iter.next_with_timeout(std::time::Duration::from_millis(50)) {
                // Timed out, give up on finding any remaining packets.
                Ok(None) => {
                    info!("raw socket post-completion timed out with {total_size} of {expect_size} bytes");
                    break;
                }
                // Value Received, process it.
                Ok(Some((packet, src_ip))) => {
                    if src_ip != target_addr.ip() || packet.get_source() != target_addr.port() {
                        debug!("filtered packet from {src_ip}:{}", packet.get_source());
                        continue;
                    }
                    debug!("recording packet from {src_ip}:{}", packet.get_source());
                    total_size += packet.payload().len();
                    if packet.get_flags() & pnet::packet::tcp::TcpFlags::FIN != 0 {
                        info!("got fin packet from {src_ip}:{}", packet.get_source());
                        seen_fin = true;
                    }
                    out.push(packet_to_output(
                        packet,
                        start,
                        PduName::with_protocol(
                            proto.clone(),
                            counter.fetch_add(1, Ordering::Relaxed),
                        ),
                        direction,
                    ));
                }
                // Network failure, bail.
                Err(e) => return (out, Some(e)),
            }
        }
        (out, None)
    })
}

fn packet_to_output(
    packet: TcpPacket,
    start: Instant,
    name: PduName,
    direction: Direction,
) -> Arc<TcpSegmentOutput> {
    let dur = TimeDelta::from_std(start.elapsed()).ok().map(Duration);
    Arc::new(TcpSegmentOutput {
        name,
        received: if direction.is_recv() { dur } else { None },
        sent: if direction.is_send() { dur } else { None },
        direction,
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
                    TcpSegmentOptionOutput::Nop(true)
                }
                TcpOptionNumbers::TIMESTAMPS if opts.payload().len() == 8 => {
                    TcpSegmentOptionOutput::Timestamps {
                        tsval: u32::from_be_bytes(opts.payload()[..4].try_into().unwrap()),
                        tsecr: u32::from_be_bytes(opts.payload()[4..8].try_into().unwrap()),
                    }
                }
                TcpOptionNumbers::MSS if opts.payload().len() == 2 => TcpSegmentOptionOutput::Mss(
                    u16::from_be_bytes(opts.payload().try_into().unwrap()),
                ),
                TcpOptionNumbers::WSCALE if opts.payload().len() == 1 => {
                    TcpSegmentOptionOutput::Wscale(u8::from_be_bytes(
                        opts.payload().try_into().unwrap(),
                    ))
                }
                TcpOptionNumbers::SACK_PERMITTED if opts.payload().len() == 0 => {
                    TcpSegmentOptionOutput::SackPermitted(true)
                }
                _ => TcpSegmentOptionOutput::Generic {
                    kind: opts.get_number().0,
                    value: Bytes::copy_from_slice(opts.payload()).into(),
                },
            })
            .collect(),
        payload: Bytes::copy_from_slice(packet.payload()).into(),
    })
}
