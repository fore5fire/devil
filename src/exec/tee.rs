use std::fmt::Debug;
use std::ops::Range;
use std::pin::{pin, Pin};
use std::task::{ready, Poll};

use derivative::Derivative;
use log::info;
use regex::bytes::Regex;
use tokio::io::{self, AsyncRead, AsyncWrite};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Debug + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Debug + Send> Stream for T {}

#[derive(Debug)]
pub struct Tee<T: AsyncRead + AsyncWrite + Unpin + Send> {
    inner: TeeReader<TeeWriter<T>>,
}

impl<T: Stream> Tee<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: TeeReader::new(TeeWriter::new(wrap)),
        }
    }
    pub fn set_pattern(&mut self, pattern: Option<Regex>, window: Option<usize>) {
        self.inner.set_pattern(pattern, window)
    }
    pub fn set_read_limit(&mut self, limit: usize) {
        self.inner.set_read_limit(limit)
    }
    pub fn into_inner(self) -> T {
        self.inner.into_inner().into_inner()
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        self.inner.inner_mut().inner_mut()
    }
    pub fn into_parts(self) -> (T, Vec<u8>, Vec<u8>, Vec<u8>, Option<Range<usize>>) {
        let (inner, reads, remainder, matched) = self.inner.into_parts();
        let (inner, writes) = inner.into_parts();
        (inner, writes, reads, remainder, matched)
    }
}

impl<T: AsyncWrite + AsyncRead + Unpin + Send> AsyncRead for Tee<T> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + AsyncRead + Unpin + Send> AsyncWrite for Tee<T> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        pin!(&mut self.inner).poll_write(cx, buf)
    }
    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TeeReader<T: AsyncRead + Unpin + Send> {
    #[derivative(Debug = "ignore")]
    inner: T,
    pub reads: Vec<u8>,
    pattern: Option<Regex>,
    pattern_window: usize,
    pattern_matched: Option<Range<usize>>,
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

impl<T: AsyncRead + Unpin + Send> TeeReader<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: wrap,
            reads: Vec::new(),
            pattern: None,
            pattern_window: usize::MAX,
            read_state: ReadState::Open,
            read_limit: usize::MAX,
            end: 0,
            pattern_matched: None,
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
    pub fn into_parts(mut self) -> (T, Vec<u8>, Vec<u8>, Option<Range<usize>>) {
        let remainder = self.reads.split_off(self.end);
        (self.inner, self.reads, remainder, self.pattern_matched)
    }
}

impl<T: AsyncRead + Unpin + Send> AsyncRead for TeeReader<T> {
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
                    info!("tee limit reached");
                }
                if let (Some(pattern), None) = (&self.pattern, &self.pattern_matched) {
                    if let Some(matched) = pattern.find_at(
                        &self.reads[..self.end],
                        self.end.saturating_sub(self.pattern_window),
                    ) {
                        info!("tee pattern matched {:?}", matched.as_bytes());
                        (self.end, self.pattern_matched) = (matched.end(), Some(matched.range()));
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
                self.read_state = ReadState::Open;
                Poll::Ready(Err(io::Error::other(Error::PatternMatched)))
            }
            ReadState::LimitReached => {
                self.read_limit = usize::MAX;
                self.read_state = ReadState::Open;
                Poll::Ready(Err(io::Error::other(Error::LimitReached)))
            }
        }
    }
}

impl<T: AsyncRead + Unpin + AsyncWrite + Send> AsyncWrite for TeeReader<T> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        pin!(&mut self.inner).poll_write(cx, buf)
    }
    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TeeWriter<T: AsyncWrite + Unpin + Send> {
    #[derivative(Debug = "ignore")]
    inner: T,
    pub writes: Vec<u8>,
}

impl<T: AsyncWrite + Unpin + Send> TeeWriter<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: wrap,
            writes: Vec::new(),
        }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        &mut self.inner
    }
    pub fn into_parts(self) -> (T, Vec<u8>) {
        (self.inner, self.writes)
    }
}

impl<T: AsyncWrite + AsyncRead + Unpin + Send> AsyncRead for TeeWriter<T> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin + Send> AsyncWrite for TeeWriter<T> {
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
    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("read pattern matched")]
    PatternMatched,
    #[error("read limit reached")]
    LimitReached,
}
