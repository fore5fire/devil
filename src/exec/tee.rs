use std::fmt::Debug;
use std::pin::{pin, Pin};
use std::task::{ready, Poll};

use anyhow::anyhow;
use regex::bytes::Regex;
use tokio::io::{self, AsyncRead, AsyncWrite};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Debug + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Debug + Send> Stream for T {}

#[derive(Debug)]
pub struct Tee<T: Stream> {
    inner: T,
    pub reads: Vec<u8>,
    pub writes: Vec<u8>,
    pattern: Option<Regex>,
    pattern_window: usize,
    read_limit: usize,
    read_state: ReadState,
    end: usize,
}

#[derive(Debug)]
enum ReadState {
    Open,
    PatternMatched,
    LimitReached,
}

impl<T: Stream> Tee<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: wrap,
            reads: Vec::new(),
            writes: Vec::new(),
            pattern: None,
            pattern_window: usize::MAX,
            read_state: ReadState::Open,
            read_limit: usize::MAX,
            end: 0,
        }
    }
    pub fn set_pattern(&mut self, pattern: Option<Regex>, window: Option<usize>) {
        self.pattern = pattern;
        self.pattern_window = window.unwrap_or(usize::MAX);
    }
    pub fn set_read_limit(&mut self, limit: usize) {
        self.read_limit = limit;
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        &mut self.inner
    }
    pub fn into_parts(mut self) -> (T, Vec<u8>, Vec<u8>, Vec<u8>) {
        let remainder = self.reads.split_off(self.end);
        (self.inner, self.writes, self.reads, remainder)
    }
}

impl<T: Stream> AsyncRead for Tee<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.read_state {
            ReadState::Open => {
                let old_len = buf.filled().len();
                ready!(pin!(&mut self.inner).poll_read(cx, buf))?;
                self.reads.extend_from_slice(&buf.filled()[old_len..]);
                self.end = self.reads.len();
                if self.end >= self.read_limit {
                    self.end = self.read_limit;
                    self.read_state = ReadState::LimitReached;
                }
                if let Some(pattern) = &self.pattern {
                    if let Some(end) = pattern.find_at(
                        &self.reads[..self.end],
                        self.end.saturating_sub(self.pattern_window),
                    ) {
                        self.end = end.end();
                        self.read_state = ReadState::PatternMatched;
                    }
                }
                if self.end < self.reads.len() {
                    let truncate = self.reads.len() - self.end;
                    buf.set_filled(buf.filled().len() - truncate);
                }
                Poll::Ready(Ok(()))
            }
            ReadState::PatternMatched => {
                Poll::Ready(Err(io::Error::other(anyhow!("read pattern matched"))))
            }
            ReadState::LimitReached => {
                Poll::Ready(Err(io::Error::other(anyhow!("read limit reached"))))
            }
        }
    }
}

impl<T: Stream> AsyncWrite for Tee<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = pin!(&mut self.inner).poll_write(cx, buf);
        if poll.is_ready() {
            self.writes.extend_from_slice(&buf);
        }
        poll
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }
}
