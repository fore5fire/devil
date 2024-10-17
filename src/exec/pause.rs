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
        //let duration = planned
        //    .duration
        //    .to_std()
        //    .expect("pause durations should fit in both std and chrono");
        //let sleep = tokio::time::sleep_until(start + duration);
        //let barriers: Vec<_> = planned.join.iter().map(|j| ctx.pause_barrier(j)).collect();
        //let join_tags = planned.join.clone();
        //let offset_bytes = planned.offset_bytes;
        Pause(tokio::spawn(async move {
            //sleep.await;
            //join_all(barriers.iter().map(|b| b.wait())).await;
            PauseValueOutput {
                duration: chrono::Duration::from_std(start.elapsed())
                    .expect("pause durations should fit in both std and chrono")
                    .into(),
                r#await: None,
                location: crate::LocationOutput::Before(crate::LocationValueOutput {
                    id: crate::location::Location::Udp(
                        crate::location::UdpLocation::SendBody,
                        crate::location::Side::Start,
                    ),
                    offset_bytes: 0,
                }), //offset_bytes,
                    //join: join_tags,
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

    bytes_read: i64,
    pending: Option<(Pause, usize)>,
    plans: VecDeque<AbsolutePlan>,
    out: Vec<Vec<PauseValueOutput>>,
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
            bytes_read: 0,
            pending: None,
            plans: VecDeque::new(),
            out: Vec::new(),
        };
        result.add_reads(read_plans);
        result
    }

    pub(crate) fn add_reads(&mut self, read_plans: impl IntoIterator<Item = PauseSpec>) {
        let read_plans = read_plans.into_iter();
        if let Some(s) = read_plans.size_hint().1 {
            self.out.reserve(s);
        }
        self.plans.extend(
            read_plans
                .map(|spec| {
                    let output_index = self.out.len();
                    self.out.push(Vec::with_capacity(spec.plan.len()));
                    let current_offset = spec.group_offset + self.bytes_read;
                    spec.plan.into_iter().map(move |p| AbsolutePlan {
                        absolute_offset: current_offset + /*p.offset_bytes*/0,
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

    pub fn inner_ref(&self) -> &T {
        &self.inner
    }

    pub fn finish(self) -> (T, Vec<Vec<PauseValueOutput>>) {
        (self.inner, self.out)
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
            if self.pending.is_none() {
                if let Some(plan) = self.plans.front() {
                    if plan.absolute_offset == self.bytes_read {
                        self.pending = Some((Pause::new(&self.ctx, &plan.plan), plan.output_index));
                        self.plans.pop_front();
                    }
                }
            }

            // Execute any pending pauses.
            let Some(mut pause) = std::mem::take(&mut self.pending) else {
                break;
            };
            match pause.0.poll_unpin(cx) {
                Poll::Ready(actual) => self.out[pause.1].push(actual?),
                Poll::Pending => {
                    self.pending = Some(pause);
                    return Poll::Pending;
                }
            }
        }

        // Don't read more bytes than we need to get to the next pause.
        let read_len = self
            .plans
            .front()
            .map(|p| p.absolute_offset - self.bytes_read)
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
        self.bytes_read += i64::try_from(bytes_read).expect("too many bytes read");

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

    bytes_written: i64,
    pending: Option<(Pause, usize)>,
    plans: VecDeque<AbsolutePlan>,
    out: Vec<Vec<PauseValueOutput>>,
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
            bytes_written: 0,
            pending: None,
            plans: VecDeque::new(),
            out: Vec::new(),
        };
        result.add_writes(write_plans);
        result
    }

    pub fn add_writes(&mut self, write_plans: impl IntoIterator<Item = PauseSpec>) {
        let write_plans = write_plans.into_iter();
        if let Some(s) = write_plans.size_hint().1 {
            self.out.reserve(s);
        }
        self.plans.extend(
            write_plans
                .map(|spec| {
                    let output_index = self.out.len();
                    self.out.push(Vec::with_capacity(spec.plan.len()));
                    let current_offset = spec.group_offset + self.bytes_written;
                    spec.plan.into_iter().map(move |p| AbsolutePlan {
                        absolute_offset: current_offset + /*p.offset_bytes*/0,
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

    pub fn inner_ref(&self) -> &T {
        &self.inner
    }

    pub fn finish(self) -> (T, Vec<Vec<PauseValueOutput>>) {
        (self.inner, self.out)
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
            if self.pending.is_none() {
                if let Some(plan) = self.plans.front() {
                    if plan.absolute_offset == self.bytes_written {
                        self.pending = Some((Pause::new(&self.ctx, &plan.plan), plan.output_index));
                        self.plans.pop_front();
                    }
                }
            }

            // Always flush before pausing.
            if self.pending.is_some() {
                if let Err(e) = ready!(self.as_mut().poll_flush(cx)) {
                    return Poll::Ready(Err(e));
                };
            }

            // Execute any pending pauses.
            let Some(mut pause) = std::mem::take(&mut self.pending) else {
                break;
            };

            match pause.0.poll_unpin(cx) {
                Poll::Ready(actual) => self.out[pause.1].push(actual?),
                Poll::Pending => {
                    self.pending = Some(pause);
                    return Poll::Pending;
                }
            }
        }

        // Don't write more bytes than we need to get to the next pause.
        let write_len = self
            .plans
            .front()
            .map(|p| p.absolute_offset - self.bytes_written)
            .map(usize::try_from)
            .transpose()
            .expect("bytes to write should fit in a usize")
            .unwrap_or_else(|| buf.len());

        // Write some bytes.
        let result =
            ready!(Pin::new(&mut self.inner).poll_write(cx, &buf[0..write_len.min(buf.len())]));

        if let Ok(bytes_written) = result {
            // Record the newly read bytes.
            self.bytes_written += i64::try_from(bytes_written).expect("too many bytes written");
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
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
