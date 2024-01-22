use std::{pin::Pin, sync::Arc, task::Poll, time::Instant};

use futures::{future::join_all, Future, FutureExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::{JoinError, JoinHandle},
};

use crate::PauseValueOutput;

use super::tee::Stream;

#[derive(Debug)]
pub struct Pause(JoinHandle<PauseValueOutput>);

impl Pause {
    pub(crate) fn new(ctx: &super::Context, planned: &PauseValueOutput) -> Self {
        let start = tokio::time::Instant::now();
        let duration = planned
            .duration
            .to_std()
            .expect("pause durations should fit in both std and chrono");
        let sleep = tokio::time::sleep_until(start + duration);
        let barriers: Vec<_> = planned.join.iter().map(|j| ctx.pause_barrier(j)).collect();
        let join_tags = planned.join.clone();
        let offset_bytes = planned.offset_bytes;
        Pause(tokio::spawn(async move {
            println!("pausing for {duration:?}");
            sleep.await;
            println!("finished sleep");
            join_all(barriers.iter().map(|b| b.wait())).await;
            println!("finished join at {:?}", std::time::Instant::now());
            PauseValueOutput {
                duration: chrono::Duration::from_std(start.elapsed())
                    .expect("pause durations should fit in both std and chrono"),
                offset_bytes,
                join: join_tags,
            }
        }))
    }
}

impl Future for Pause {
    type Output = Result<PauseValueOutput, JoinError>;
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

#[derive(Debug)]
pub struct PauseStream<T, FR, FW>
where
    T: Stream,
    FR: Fn(i64) -> Vec<PauseValueOutput>,
    FW: Fn(i64) -> Vec<PauseValueOutput>,
{
    inner: T,
    ctx: Arc<super::Context>,

    read_planned: FR,
    read_bytes: i64,
    read_pending: Vec<Pause>,
    read_pending_calculated: bool,
    read_out: Vec<PauseValueOutput>,

    write_planned: FW,
    write_bytes: i64,
    write_pending: Vec<Pause>,
    write_pending_calculated: bool,
    write_out: Vec<PauseValueOutput>,
}

impl<T, FR, FW> PauseStream<T, FR, FW>
where
    T: Stream,
    FR: Fn(i64) -> Vec<PauseValueOutput>,
    FW: Fn(i64) -> Vec<PauseValueOutput>,
{
    fn new(ctx: Arc<super::Context>, inner: T, read_pauses: FR, write_pauses: FW) -> Self {
        PauseStream {
            inner,
            ctx,
            read_out: Vec::new(),
            read_planned: read_pauses,
            read_bytes: 0,
            read_pending: Vec::new(),
            read_pending_calculated: false,
            write_out: Vec::new(),
            write_planned: write_pauses,
            write_bytes: 0,
            write_pending: Vec::new(),
            write_pending_calculated: false,
        }
    }

    fn finish(self) -> (Vec<PauseValueOutput>, Vec<PauseValueOutput>) {
        (self.read_out, self.write_out)
    }
}

impl<T, FR, FW> AsyncRead for PauseStream<T, FR, FW>
where
    T: Stream,
    FR: Fn(i64) -> Vec<PauseValueOutput>,
    FW: Fn(i64) -> Vec<PauseValueOutput>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Look for required pauses before reading any more bytes.
        let mut pauses = std::mem::take(&mut self.read_pending);
        if !self.read_pending_calculated {
            let read_plans = (self.read_planned)(self.read_bytes);
            pauses.extend(
                read_plans
                    .iter()
                    .filter(|p| p.offset_bytes == self.read_bytes)
                    .map(|p| Pause::new(&self.ctx, p)),
            );
            println!(
                "calculated required pauses: {pauses:?}, time {:?}",
                Instant::now(),
            );
            self.read_pending_calculated = true
        }

        // Execute any pending pauses.
        self.read_out.reserve(pauses.len());
        for i in 0..pauses.len() {
            match pauses[i].poll_unpin(cx) {
                Poll::Ready(actual) => self.read_out.push(actual?),
                Poll::Pending => {
                    // Remove the completed pauses so we don't double record them.
                    pauses.drain(0..i);
                    self.read_pending = pauses;
                    return Poll::Pending;
                }
            }
        }

        // Pending pauses finished - save the empty pauses vec so we can reuse its allocated
        // memory.
        pauses.clear();
        self.read_pending = pauses;

        // Don't read more bytes than we need to get to the next pause.
        let next_read_plans = (self.read_planned)(self.read_bytes + 1);
        let read_len = next_read_plans
            .first()
            .map(|p| p.offset_bytes - self.read_bytes)
            .map(usize::try_from)
            .transpose()
            .expect("bytes to read should fit in a usize")
            .unwrap_or(usize::MAX);
        let mut sub_buf = buf.take(read_len);

        // Read some data.
        let result = Pin::new(&mut self.inner).poll_read(cx, &mut sub_buf);

        let bytes_read = sub_buf.filled().len();

        // sub_buf started assuming it was fully uninitialized, and any writes to it would have
        // initialized the shared memory in buf.
        let sub_buf_initialized = sub_buf.initialized().len();
        unsafe { buf.assume_init(buf.filled().len() + sub_buf_initialized) }
        buf.advance(bytes_read);

        // If bytes were read then signal to look for more matching pauses on the next read.
        if bytes_read > 0 {
            self.read_pending_calculated = false;
        }

        // Record the newly read bytes.
        self.read_bytes += i64::try_from(bytes_read).expect("too many bytes written");

        result
    }
}

impl<T, FR, FW> AsyncWrite for PauseStream<T, FR, FW>
where
    T: Stream,
    FR: Fn(i64) -> Vec<PauseValueOutput>,
    FW: Fn(i64) -> Vec<PauseValueOutput>,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Look for required pauses before writing any more bytes.
        let mut pauses = std::mem::take(&mut self.write_pending);
        if !self.write_pending_calculated {
            let write_plans = (self.write_planned)(self.write_bytes);

            // Ensure we've flushed before starting any new pause timers.
            if !write_plans.is_empty() {
                if let Poll::Pending = self.as_mut().poll_flush(cx) {
                    return Poll::Pending;
                }
            }

            pauses.extend(write_plans.iter().map(|p| Pause::new(&self.ctx, p)));
            println!(
                "calculated required pauses: {pauses:?}, time {:?}",
                Instant::now(),
            );
            self.write_pending_calculated = true
        }

        // Execute any pending pauses.
        self.write_out.reserve(pauses.len());
        for i in 0..pauses.len() {
            match pauses[i].poll_unpin(cx) {
                Poll::Ready(actual) => self.write_out.push(actual?),
                Poll::Pending => {
                    // Remove the completed pauses so we don't double record them.
                    pauses.drain(0..i);
                    self.write_pending = pauses;
                    return Poll::Pending;
                }
            }
        }

        // Pending pauses finished - save the empty pauses vec so we can reuse its allocated
        // memory.
        pauses.clear();
        self.write_pending = pauses;

        // Don't write more bytes than we need to get to the next pause.
        let write_plans = (self.write_planned)(self.write_bytes + 1);
        let write_len = write_plans
            .first()
            .map(|p| usize::try_from(p.offset_bytes))
            .transpose()
            .expect("bytes to read should fit in a usize")
            .unwrap_or_else(|| buf.len());

        // Write some bytes.
        let result = Pin::new(&mut self.inner).poll_write(cx, &buf[0..write_len]);

        let Poll::Ready(Ok(bytes_written)) = result else {
            // Nothing else to do if no bytes were written.
            return result;
        };

        // If bytes were read then signal to look for more matching pauses on the next write.
        if bytes_written > 0 {
            self.write_pending_calculated = false;
        }

        // Record the newly read bytes.
        self.write_bytes += i64::try_from(bytes_written).expect("too many bytes written");
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let poll = Pin::new(&mut self.inner).poll_shutdown(cx);
        poll
    }
}

impl<T, FR, FW> Unpin for PauseStream<T, FR, FW>
where
    T: Stream,
    FR: Fn(i64) -> Vec<PauseValueOutput>,
    FW: Fn(i64) -> Vec<PauseValueOutput>,
{
}
