use std::{net::SocketAddr, time::Instant};

use chrono::TimeDelta;
use log::debug;
use pnet::{
    packet::{tcp::TcpOptionNumbers, Packet},
    transport::{self, TransportReceiver},
};
use tokio::sync::mpsc;

use crate::{TcpSegmentOptionOutput, TcpSegmentOutput};

pub fn reader(
    mut read: TransportReceiver,
    target_addr: SocketAddr,
    start: Instant,
) -> mpsc::UnboundedReceiver<TcpSegmentOutput> {
    // TODO: Use AsyncFd instead of one thread per request.
    //let read_fd = AsyncFd::new(read.socket.fd)?;
    //let readable = read_fd.readable().await?;
    let (send_read, receive_read) = mpsc::unbounded_channel();
    tokio::task::spawn_blocking(
        move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
            let mut iter = transport::tcp_packet_iter(&mut read);
            while !send_read.is_closed() {
                match iter.next_with_timeout(std::time::Duration::from_millis(100)) {
                    // Timed out, loop to check that we're still running.
                    Ok(None) => {}
                    // Value Received, forward it.
                    Ok(Some((packet, src_ip))) => {
                        if src_ip != target_addr.ip() || packet.get_source() != target_addr.port() {
                            debug!("filtered packet from {src_ip}:{}", packet.get_source());
                            continue;
                        }
                        debug!("recording packet from {src_ip}:{}", packet.get_source());
                        send_read.send(TcpSegmentOutput {
                            received: TimeDelta::from_std(start.elapsed()).ok(),
                            sent: None,
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
                                    TcpOptionNumbers::TIMESTAMPS if opts.payload().len() == 8 => {
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
    receive_read
}
