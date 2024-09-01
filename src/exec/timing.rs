use std::fmt::Debug;
use std::io::ErrorKind;
use std::pin::{pin, Pin};
use std::task::{ready, Poll};
use std::time::{Duration, Instant};

use super::tee::Stream;

use anyhow::anyhow;
use chrono::TimeDelta;
use futures::FutureExt;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::time::{sleep, Sleep};

#[derive(Debug)]
pub struct Timing<T: Stream> {
    inner: T,
    first_read: Option<Instant>,
    last_read: Option<Instant>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    read_state: ReadState,
    sleep: Option<Pin<Box<Sleep>>>,
    read_timeout: Option<Duration>,
}

#[derive(Debug)]
enum ReadState {
    Open,
    TimedOut,
}

impl<T: Stream> Timing<T> {
    pub fn new(wrap: T, read_timeout: Option<TimeDelta>) -> Self {
        let read_timeout = read_timeout.map(|timeout| timeout.to_std().unwrap());
        Self {
            inner: wrap,
            first_read: None,
            last_read: None,
            first_write: None,
            last_write: None,
            read_state: ReadState::Open,
            sleep: None,
            read_timeout,
        }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    pub fn inner_mut(&mut self) -> &'_ mut T {
        &mut self.inner
    }
    pub fn first_read(&self) -> Option<Instant> {
        self.first_read
    }
    pub fn last_read(&self) -> Option<Instant> {
        self.last_read
    }
    pub fn first_write(&self) -> Option<Instant> {
        self.first_write
    }
    pub fn last_write(&self) -> Option<Instant> {
        self.last_write
    }
}

impl<T: Stream> AsyncRead for Timing<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.read_state {
            ReadState::Open => {
                let poll = pin!(&mut self.inner).poll_read(cx, buf);
                let now = Instant::now();

                // We're waiting for data - apply the timeout if set.
                if poll.is_pending() {
                    if let Some(timeout) = self.read_timeout {
                        // Get the running timer or start one.
                        let sleep = self.sleep.get_or_insert_with(|| Box::pin(sleep(timeout)));
                        // Advance the timer.
                        ready!(sleep.poll_unpin(cx));
                        self.read_state = ReadState::TimedOut;
                        return Poll::Ready(Err(io::Error::new(
                            ErrorKind::TimedOut,
                            anyhow!("read timeout reached"),
                        )));
                    }
                }

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

impl<T: Stream> AsyncWrite for Timing<T> {
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
