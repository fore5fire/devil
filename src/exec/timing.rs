use std::fmt::Debug;
use std::io::ErrorKind;
use std::pin::{pin, Pin};
use std::task::{ready, Poll};
use std::time::Instant;

use super::tee::Stream;

use anyhow::anyhow;
use derivative::Derivative;
use tokio::io::{self, AsyncRead, AsyncWrite};

#[derive(Debug)]
pub struct Timing<T: AsyncRead + AsyncWrite + Unpin + Send> {
    inner: TimingReader<TimingWriter<T>>,
}

impl<T: Stream> Timing<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: TimingReader::new(TimingWriter::new(wrap)),
        }
    }
    pub fn into_inner(self) -> T {
        self.inner.into_inner().into_inner()
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        self.inner.inner_mut().inner_mut()
    }
    pub fn inner_ref(&self) -> &'_ T {
        self.inner.inner_ref().inner_ref()
    }
    pub fn first_write(&self) -> Option<Instant> {
        self.inner.inner_ref().first_write
    }
    pub fn last_write(&self) -> Option<Instant> {
        self.inner.inner_ref().last_write
    }
    pub fn shutdown_start(&self) -> Option<Instant> {
        self.inner.inner_ref().shutdown_start()
    }
    pub fn shutdown_end(&self) -> Option<Instant> {
        self.inner.inner_ref().shutdown_end()
    }
    pub fn first_read(&self) -> Option<Instant> {
        self.inner.first_read()
    }
    pub fn last_read(&self) -> Option<Instant> {
        self.inner.last_read()
    }
}

impl<T: AsyncWrite + AsyncRead + Unpin + Send> AsyncRead for Timing<T> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for Timing<T> {
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
pub struct TimingReader<T: AsyncRead + Unpin + Send> {
    #[derivative(Debug = "ignore")]
    inner: T,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    read_state: ReadState,
}

#[derive(Debug)]
enum ReadState {
    Open,
    TimedOut,
}

impl<T: AsyncRead + Unpin + Send> TimingReader<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: wrap,
            first_read: None,
            last_read: None,
            read_state: ReadState::Open,
        }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        &mut self.inner
    }
    pub fn inner_ref(&self) -> &'_ T {
        &self.inner
    }
    pub fn first_read(&self) -> Option<Instant> {
        self.first_read
    }
    pub fn last_read(&self) -> Option<Instant> {
        self.last_read
    }
}

impl<T: AsyncRead + Unpin + Send> AsyncRead for TimingReader<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.read_state {
            ReadState::Open => {
                let poll = pin!(&mut self.inner).poll_read(cx, buf);
                let now = Instant::now();

                ready!(poll)?;

                // Record the time of this read.
                self.last_read = Some(now);
                self.first_read = self.first_read.or(self.last_read);

                Poll::Ready(Ok(()))
            }
            ReadState::TimedOut => Poll::Ready(Err(io::Error::new(
                ErrorKind::TimedOut,
                anyhow!("read timeout reached"),
            ))),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TimingReader<T> {
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
pub struct TimingWriter<T: AsyncWrite + Unpin + Send> {
    #[derivative(Debug = "ignore")]
    inner: T,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    shutdown_start: Option<Instant>,
    shutdown_end: Option<Instant>,
}

impl<T: AsyncWrite + Unpin + Send> TimingWriter<T> {
    pub fn new(wrap: T) -> Self {
        Self {
            inner: wrap,
            first_write: None,
            last_write: None,
            shutdown_start: None,
            shutdown_end: None,
        }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        &mut self.inner
    }
    pub fn inner_ref(&self) -> &'_ T {
        &self.inner
    }
    pub fn first_write(&self) -> Option<Instant> {
        self.first_write
    }
    pub fn last_write(&self) -> Option<Instant> {
        self.last_write
    }
    pub fn shutdown_start(&self) -> Option<Instant> {
        self.shutdown_start
    }
    pub fn shutdown_end(&self) -> Option<Instant> {
        self.shutdown_end
    }
}

impl<T: AsyncWrite + AsyncRead + Unpin + Send> AsyncRead for TimingWriter<T> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin + Send> AsyncWrite for TimingWriter<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let read = ready!(pin!(&mut self.inner).poll_write(cx, buf))?;
        self.last_write = Some(Instant::now());
        self.first_write = self.first_write.or(self.last_write);
        Poll::Ready(Ok(read))
    }
    #[inline]
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
        if self.shutdown_start.is_none() {
            self.shutdown_start = Some(Instant::now());
        }
        let poll = pin!(&mut self.inner).poll_shutdown(cx);
        if poll.is_ready() && self.shutdown_end.is_none() {
            self.shutdown_end = Some(Instant::now());
        }
        poll
    }
}
