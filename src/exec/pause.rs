use std::{
    collections::VecDeque,
    pin::{pin, Pin},
    sync::Arc,
    task::Poll,
};

use futures::{future::join_all, ready, Future, FutureExt};
use itertools::Itertools;
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
            sleep.await;
            join_all(barriers.iter().map(|b| b.wait())).await;
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
struct AbsolutePlan {
    plan: PauseValueOutput,
    absolute_offset: i64,
    output_index: usize,
}

pub fn new_stream<T: Stream>(
    ctx: Arc<super::Context>,
    inner: T,
    read_plans: impl IntoIterator<Item = PauseSpec>,
    write_plans: impl IntoIterator<Item = PauseSpec>,
) -> PauseStream<T> {
    PauseReader::new(
        ctx.clone(),
        PauseWriter::new(ctx, inner, write_plans),
        read_plans,
    )
}

pub type PauseStream<T> = PauseReader<PauseWriter<T>>;

impl<T: Stream> PauseStream<T> {
    pub fn finish_stream(self) -> (T, Vec<Vec<PauseValueOutput>>, Vec<Vec<PauseValueOutput>>) {
        let (inner, reads) = self.finish();
        let (inner, writes) = inner.finish();
        (inner, reads, writes)
    }

    pub fn add_writes(&mut self, write_plans: impl IntoIterator<Item = PauseSpec>) {
        self.inner_mut().add_writes(write_plans)
    }
}

#[derive(Debug)]
pub struct PauseReader<T>
where
    T: AsyncRead + Send + Unpin + std::fmt::Debug,
{
    inner: T,
    ctx: Arc<super::Context>,

    read_bytes: i64,
    read_pending: Option<(Pause, usize)>,
    read_plans: VecDeque<AbsolutePlan>,
    read_out: Vec<Vec<PauseValueOutput>>,
}

impl<T> PauseReader<T>
where
    T: AsyncRead + Send + Unpin + std::fmt::Debug,
{
    pub(crate) fn new(
        ctx: Arc<super::Context>,
        inner: T,
        read_plans: impl IntoIterator<Item = PauseSpec>,
    ) -> Self {
        let mut result = Self {
            inner,
            ctx,
            read_bytes: 0,
            read_pending: None,
            read_plans: VecDeque::new(),
            read_out: Vec::new(),
        };
        result.add_reads(read_plans);
        result
    }

    pub(crate) fn add_reads(&mut self, read_plans: impl IntoIterator<Item = PauseSpec>) {
        let read_plans = read_plans.into_iter();
        if let Some(s) = read_plans.size_hint().1 {
            self.read_out.reserve(s);
        }
        self.read_plans.extend(
            read_plans
                .map(|spec| {
                    let output_index = self.read_out.len();
                    self.read_out.push(Vec::with_capacity(spec.plan.len()));
                    let current_offset = spec.group_offset + self.read_bytes;
                    spec.plan.into_iter().map(move |p| AbsolutePlan {
                        absolute_offset: current_offset + p.offset_bytes,
                        plan: p,
                        output_index,
                    })
                })
                .flatten()
                .sorted_by(|a, b| a.absolute_offset.cmp(&b.absolute_offset)),
        );
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn finish(self) -> (T, Vec<Vec<PauseValueOutput>>) {
        (self.inner, self.read_out)
    }
}

impl<T> AsyncRead for PauseReader<T>
where
    T: AsyncRead + Send + Unpin + std::fmt::Debug,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Look for required or running pauses before reading any more bytes.
        loop {
            if self.read_pending.is_none() {
                if let Some(plan) = self.read_plans.front() {
                    if plan.absolute_offset == self.read_bytes {
                        self.read_pending =
                            Some((Pause::new(&self.ctx, &plan.plan), plan.output_index));
                        self.read_plans.pop_front();
                    }
                }
            }

            // Execute any pending pauses.
            let Some(mut pause) = std::mem::take(&mut self.read_pending) else {
                break;
            };
            match pause.0.poll_unpin(cx) {
                Poll::Ready(actual) => self.read_out[pause.1].push(actual?),
                Poll::Pending => {
                    self.read_pending = Some(pause);
                    return Poll::Pending;
                }
            }
        }

        // Don't read more bytes than we need to get to the next pause.
        let read_len = self
            .read_plans
            .front()
            .map(|p| p.absolute_offset - self.read_bytes)
            .map(usize::try_from)
            .transpose()
            .expect("bytes to read should fit in a usize")
            .unwrap_or_else(|| buf.remaining());
        // When we let the sub buffer writes initalize bytes in the parent buffer things break, so
        // just pre-initialize the whole parent for now.
        buf.initialize_unfilled();
        let mut sub_buf = buf.take(read_len);
        // We just initalized the parent buffer so it's safe to assume the sub buffer is initalized
        // too.
        unsafe { sub_buf.assume_init(sub_buf.remaining()) }

        // Read some data.
        let result = ready!(Pin::new(&mut self.inner).poll_read(cx, &mut sub_buf));

        let bytes_read = sub_buf.filled().len();

        buf.advance(bytes_read);

        // Record the newly read bytes.
        self.read_bytes += i64::try_from(bytes_read).expect("too many bytes written");

        Poll::Ready(result)
    }
}

// Passthrough if T supports writes too.
impl<T> AsyncWrite for PauseReader<T>
where
    T: Stream,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        pin!(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }
}

impl<T> Unpin for PauseReader<T> where T: AsyncRead + Send + Unpin + std::fmt::Debug {}

#[derive(Debug)]
pub struct PauseWriter<T: AsyncWrite> {
    inner: T,
    ctx: Arc<super::Context>,

    write_bytes: i64,
    write_pending: Option<(Pause, usize)>,
    write_plans: VecDeque<AbsolutePlan>,
    write_out: Vec<Vec<PauseValueOutput>>,
}

impl<T: AsyncWrite + std::fmt::Debug> PauseWriter<T> {
    pub(crate) fn new(
        ctx: Arc<super::Context>,
        inner: T,
        write_plans: impl IntoIterator<Item = PauseSpec>,
    ) -> Self {
        let mut result = Self {
            inner,
            ctx,
            write_bytes: 0,
            write_pending: None,
            write_plans: VecDeque::new(),
            write_out: Vec::new(),
        };
        result.add_writes(write_plans);
        result
    }

    pub fn add_writes(&mut self, write_plans: impl IntoIterator<Item = PauseSpec>) {
        let write_plans = write_plans.into_iter();
        if let Some(s) = write_plans.size_hint().1 {
            self.write_out.reserve(s);
        }
        self.write_plans.extend(
            write_plans
                .map(|spec| {
                    let output_index = self.write_out.len();
                    self.write_out.push(Vec::with_capacity(spec.plan.len()));
                    let current_offset = spec.group_offset + self.write_bytes;
                    spec.plan.into_iter().map(move |p| AbsolutePlan {
                        absolute_offset: current_offset + p.offset_bytes,
                        plan: p,
                        output_index,
                    })
                })
                .flatten()
                .sorted_by(|a, b| a.absolute_offset.cmp(&b.absolute_offset)),
        );
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn finish(self) -> (T, Vec<Vec<PauseValueOutput>>) {
        (self.inner, self.write_out)
    }
}

impl<T> AsyncWrite for PauseWriter<T>
where
    T: AsyncWrite + std::fmt::Debug + Send + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Look for required or running pauses before writing any more bytes.
        loop {
            if self.write_pending.is_none() {
                if let Some(plan) = self.write_plans.front() {
                    if plan.absolute_offset == self.write_bytes {
                        self.write_pending =
                            Some((Pause::new(&self.ctx, &plan.plan), plan.output_index));
                        self.write_plans.pop_front();
                    }
                }
            }

            // Always flush before pausing.
            if self.write_pending.is_some() {
                if let Err(e) = ready!(self.as_mut().poll_flush(cx)) {
                    return Poll::Ready(Err(e));
                };
            }

            // Execute any pending pauses.
            let Some(mut pause) = std::mem::take(&mut self.write_pending) else {
                break;
            };

            match pause.0.poll_unpin(cx) {
                Poll::Ready(actual) => self.write_out[pause.1].push(actual?),
                Poll::Pending => {
                    self.write_pending = Some(pause);
                    return Poll::Pending;
                }
            }
        }

        // Don't write more bytes than we need to get to the next pause.
        let write_len = self
            .write_plans
            .front()
            .map(|p| p.absolute_offset - self.write_bytes)
            .map(usize::try_from)
            .transpose()
            .expect("bytes to write should fit in a usize")
            .unwrap_or_else(|| buf.len());

        // Write some bytes.
        let result =
            ready!(Pin::new(&mut self.inner).poll_write(cx, &buf[0..write_len.min(buf.len())]));

        if let Ok(bytes_written) = result {
            // Record the newly read bytes.
            self.write_bytes += i64::try_from(bytes_written).expect("too many bytes written");
        };

        return Poll::Ready(result);
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

impl<T> AsyncRead for PauseWriter<T>
where
    T: Stream,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T> Unpin for PauseWriter<T> where T: AsyncWrite + std::fmt::Debug {}

#[derive(Debug)]
pub struct PauseSpec {
    pub group_offset: i64,
    pub plan: Vec<PauseValueOutput>,
}
