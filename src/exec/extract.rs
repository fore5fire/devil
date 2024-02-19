use std::mem;
use std::pin::pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

use super::tee::Stream;

pub struct Transport<T> {
    inner: Option<T>,
    channel: Option<oneshot::Sender<T>>,
}

pub fn new<T>(inner: T) -> (oneshot::Receiver<T>, Transport<T>) {
    let (send, receive) = oneshot::channel();
    (
        receive,
        Transport {
            inner: Some(inner),
            channel: Some(send),
        },
    )
}

impl<T: Stream> AsyncRead for Transport<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        pin!(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}

impl<T: Stream> AsyncWrite for Transport<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        pin!(self.inner.as_mut().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        pin!(self.inner.as_mut().unwrap()).poll_shutdown(cx)
    }
}

impl<T> Drop for Transport<T> {
    fn drop(&mut self) {
        let inner = mem::take(&mut self.inner).unwrap();
        let channel = mem::take(&mut self.channel).unwrap();
        // Err indicates the receiver has disconnected and is safe to ignore.
        _ = channel.send(inner);
    }
}
