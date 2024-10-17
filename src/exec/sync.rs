use std::{
    collections::{BinaryHeap, HashMap},
    mem,
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use derivative::Derivative;
use tokio::{
    select,
    sync::{self, mpsc, oneshot, Notify, OwnedSemaphorePermit},
    task::AbortHandle,
};

use crate::{location, output, LocationOutput, PauseValueOutput, SignalOp, SignalValueOutput};

#[derive(Debug, Default)]
pub(super) struct StepLocations {
    syncs: Vec<(Arc<String>, Option<Synchronizer>)>,
    send_locations: Vec<StepLocation>,
    recv_locations: Vec<StepLocation>,
}

impl StepLocations {
    pub(super) fn new(
        syncs: Vec<(Arc<String>, Option<Synchronizer>)>,
        signals: &[(Arc<String>, SignalValueOutput)],
        pauses: &[(Arc<String>, PauseValueOutput)],
    ) -> Self
where {
        let mut grouped = HashMap::<LocationOutput, Vec<Action>>::new();
        let signals = signals.into_iter().map(|(name, signal)| {
            (
                signal.location,
                Action::with_signal(name.clone(), signal, &syncs),
            )
        });
        let pauses = pauses.into_iter().map(|(name, pause)| {
            let a = Action::with_pause(name.clone(), &pause, &syncs);
            (pause.location, a)
        });
        for (loc, action) in signals.chain(pauses) {
            grouped.entry(loc).or_default().push(action);
        }
        let (send_locations, recv_locations) = grouped
            .into_iter()
            .map(|(location, actions)| StepLocation { location, actions })
            .partition(|step_loc| matches!(step_loc.location, LocationOutput::Before { .. }));
        // TODO: sort locations.
        Self {
            syncs,
            send_locations,
            recv_locations,
        }
    }

    pub(super) fn set_range(
        stream: location::Location,
        after: Option<location::Location>,
        size: usize,
    ) {
    }
}

#[derive(Debug)]
pub(super) struct StepLocation {
    pub location: LocationOutput,
    pub actions: Vec<Action>,
}

#[derive(Debug)]
enum ActionKind {
    Signal {
        op: SignalOp,
        target: usize,
    },
    Pause {
        duration: Duration,
        target: Option<usize>,
    },
}

#[derive(Debug)]
struct Action {
    name: Arc<String>,
    kind: ActionKind,
}

impl Action {
    fn with_signal(
        name: Arc<String>,
        out: &SignalValueOutput,
        syncs: &[(Arc<String>, Option<Synchronizer>)],
    ) -> Self {
        Self {
            name,
            kind: ActionKind::Signal {
                op: out.op,
                target: syncs
                    .iter()
                    .position(|(name, _)| name.as_str() == out.target.as_str())
                    .expect("signal target should refer to a sync"),
            },
        }
    }

    fn with_pause(
        name: Arc<String>,
        out: &PauseValueOutput,
        syncs: &[(Arc<String>, Option<Synchronizer>)],
    ) -> Self {
        Self {
            name,
            kind: ActionKind::Pause {
                duration: out
                    .duration
                    .0
                    .to_std()
                    .expect("pause duration should be non-negative"),
                target: out.r#await.as_ref().map(|target| {
                    syncs
                        .iter()
                        .position(|(name, _)| name.as_str() == target.as_str())
                        .expect("signal target should refer to a sync")
                }),
            },
        }
    }

    async fn execute(&mut self, syncs: &mut [(String, Option<Synchronizer>)]) -> crate::Result<()> {
        match &self.kind {
            ActionKind::Pause { duration, target } => {
                let pause = (!duration.is_zero()).then(|| tokio::time::sleep(*duration));
                if let Some(i) = *target {
                    let target = mem::take(&mut syncs[i].1)
                        .expect("sync should be replaced between actions");
                    syncs[i].1 = Some(match target {
                        Synchronizer::Barrier(b) => {
                            b.wait().await;
                            Synchronizer::Barrier(b)
                        }
                        Synchronizer::Mutex(m, None) => {
                            let guard = m.lock().await;
                            Synchronizer::Mutex(m, Some(guard))
                        }
                        // Mutex is re-entrant, so do nothing if the lock is already held by this
                        // job.
                        Synchronizer::Mutex(m, Some(guard)) => Synchronizer::Mutex(m, Some(guard)),
                        Synchronizer::Semaphore(s, None) => {
                            let permit = s.aquire().await;
                            Synchronizer::Semaphore(s, Some(permit))
                        }
                        // TODO: Should we allow semaphores to be aquired multiple times by the
                        // same job, or allow a variable number of permits to be aquired with one
                        // await, or both (which requires specifying which permit when releasing to
                        // ensure the right number of perimts are released)
                        Synchronizer::Semaphore(_, Some(_)) => bail!("cannot await semaphore that is already held by this job"),
                        Synchronizer::PriorityMutex(_, PriorityState::Unregistered) => {
                            bail!(
                                "must send register signal to priority mutex before it can be awaited"
                            )
                        }
                        Synchronizer::PriorityMutex(m, PriorityState::Registered(r)) => {
                            let guard = r.aquire().await;
                            Synchronizer::PriorityMutex(m, PriorityState::Held(guard))
                        }
                        // PriorityMutex is re-entrant, so do nothing if the lock is already held
                        // by this job.
                        Synchronizer::PriorityMutex(m, PriorityState::Held(guard)) => {
                            Synchronizer::PriorityMutex(m, PriorityState::Held(guard))
                        }
                        Synchronizer::PrioritySemaphore(
                            _,
                            PriorityState::Unregistered,
                        ) => bail!(
                            "must send register signal to priority semaphore before it can be awaited"
                        ),
                        Synchronizer::PrioritySemaphore(s, PriorityState::Registered(r)) => {
                            let guard = r.aquire().await;
                            Synchronizer::PrioritySemaphore(s, PriorityState::Held(guard))
                        }
                        Synchronizer::PrioritySemaphore(_, PriorityState::Held(_)) => {
                            bail!("cannot await semaphore that is already held by this job")
                        }
                    })
                }
                if let Some(pause) = pause {
                    pause.await;
                }
            }
            ActionKind::Signal { op, target: i } => {
                let target =
                    mem::take(&mut syncs[*i].1).expect("sync should be replaced between actions");
                syncs[*i].1 = Some(match (target, op) {
                    (Synchronizer::Mutex(s, Some(_)), SignalOp::Unlock) => {
                        Synchronizer::Mutex(s, None)
                    }
                    (Synchronizer::Semaphore(s, Some(_)), SignalOp::Release) => {
                        Synchronizer::Semaphore(s, None)
                    }
                    (
                        Synchronizer::PriorityMutex(s, PriorityState::Unregistered),
                        SignalOp::Register { priority },
                    ) => {
                        let r = s.register(*priority);
                        Synchronizer::PriorityMutex(s, PriorityState::Registered(r))
                    }
                    (Synchronizer::PriorityMutex(s, PriorityState::Held(_)), SignalOp::Unlock) => {
                        Synchronizer::PriorityMutex(s, PriorityState::Unregistered)
                    }
                    (
                        Synchronizer::PrioritySemaphore(s, PriorityState::Unregistered),
                        SignalOp::Register { priority },
                    ) => {
                        let r = s.register(*priority);
                        Synchronizer::PrioritySemaphore(s, PriorityState::Registered(r))
                    }
                    (
                        Synchronizer::PrioritySemaphore(s, PriorityState::Held(_)),
                        SignalOp::Release,
                    ) => Synchronizer::PrioritySemaphore(s, PriorityState::Unregistered),
                    (target, kind) => {
                        bail!(
                            "invalid signal {kind:?} for synchronizer {} in state {}",
                            target.name(),
                            target.state_name()
                        )
                    }
                })
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) enum Synchronizer {
    Barrier(Arc<sync::Barrier>),
    Mutex(Mutex, Option<Guard<NotifyPermit>>),
    Semaphore(Semaphore, Option<Guard<OwnedSemaphorePermit>>),
    PriorityMutex(PriorityMutex, PriorityState<NotifyPermit>),
    PrioritySemaphore(PrioritySemaphore, PriorityState<OwnedSemaphorePermit>),
}

impl Synchronizer {
    pub(super) fn new(sync: &output::SyncOutput) -> Self {
        match *sync {
            output::SyncOutput::Barrier { count } => {
                Self::Barrier(Arc::new(sync::Barrier::new(count)))
            }
            output::SyncOutput::Mutex => Self::Mutex(Mutex::new(), None),
            output::SyncOutput::Semaphore { permits } => {
                Self::Semaphore(Semaphore::new(permits), None)
            }
            output::SyncOutput::PriorityMutex => {
                Self::PriorityMutex(PriorityMutex::new(), PriorityState::Unregistered)
            }
            output::SyncOutput::PrioritySemaphore { permits } => Self::PrioritySemaphore(
                PrioritySemaphore::new(permits),
                PriorityState::Unregistered,
            ),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Barrier(..) => "Barrier",
            Self::Mutex(..) => "Mutex",
            Self::Semaphore(..) => "Semaphore",
            Self::PriorityMutex(..) => "PriorityMutex",
            Self::PrioritySemaphore(..) => "PrioritySemaphore",
        }
    }

    fn state_name(&self) -> &'static str {
        match self {
            Self::Barrier(_) => "N/A",
            Self::Mutex(_, None) => "NotHeld",
            Self::Mutex(_, Some(_)) => "Held",
            Self::Semaphore(_, None) => "NotAquired",
            Self::Semaphore(_, Some(_)) => "Aquired",
            Self::PriorityMutex(_, PriorityState::Unregistered) => "Unregistered",
            Self::PriorityMutex(_, PriorityState::Registered(..)) => "Registered",
            Self::PriorityMutex(_, PriorityState::Held(..)) => "Held",
            Self::PrioritySemaphore(_, PriorityState::Unregistered) => "Unregistered",
            Self::PrioritySemaphore(_, PriorityState::Registered(..)) => "Registered",
            Self::PrioritySemaphore(_, PriorityState::Held(..)) => "Held",
        }
    }
}

impl Clone for Synchronizer {
    fn clone(&self) -> Self {
        match self {
            Self::Barrier(b) => Self::Barrier(b.clone()),
            Self::Mutex(m, _) => Self::Mutex(m.clone(), None),
            Self::Semaphore(s, _) => Self::Semaphore(s.clone(), None),
            Self::PriorityMutex(m, _) => {
                Self::PriorityMutex(m.clone(), PriorityState::Unregistered)
            }
            Self::PrioritySemaphore(s, _) => {
                Self::PrioritySemaphore(s.clone(), PriorityState::Unregistered)
            }
        }
    }
}

#[derive(Debug)]
enum PriorityState<T> {
    Unregistered,
    Registered(PriorityRegistration<T>),
    Held(Guard<T>),
}

#[derive(Debug, Clone)]
pub(super) struct Mutex {
    notify: Arc<Notify>,
}

impl Mutex {
    fn new() -> Self {
        let notify = Arc::new(Notify::new());
        // To start as unlocked we prime the notify with a signal.
        notify.notify_one();
        Self { notify }
    }

    async fn lock(&self) -> Guard<NotifyPermit> {
        self.notify.notified().await;
        Guard {
            permit: NotifyPermit(self.notify.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Semaphore {
    semaphore: Arc<sync::Semaphore>,
}

impl Semaphore {
    fn new(n: usize) -> Self {
        Self {
            semaphore: Arc::new(sync::Semaphore::new(n)),
        }
    }

    async fn aquire(&self) -> Guard<OwnedSemaphorePermit> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should not be used after execution");
        Guard { permit }
    }
}

#[derive(Debug, Clone)]
struct AbortOnDrop(Option<Arc<AbortHandle>>);

impl AbortOnDrop {
    #[inline]
    fn new(handle: AbortHandle) -> Self {
        AbortOnDrop(Some(Arc::new(handle)))
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = Arc::into_inner(mem::take(&mut self.0).unwrap()) {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PriorityMutex {
    enqueue: mpsc::UnboundedSender<Entry<NotifyPermit>>,
    abort_task: AbortOnDrop,
}

impl PriorityMutex {
    fn new() -> Self {
        let (enqueue, mut enqueued) = mpsc::unbounded_channel::<Entry<NotifyPermit>>();
        let task = tokio::spawn(async move {
            let notify = Arc::new(Notify::new());
            let mut queue = BinaryHeap::new();
            let mut locked = false;
            loop {
                select! {
                    Some(entry) = enqueued.recv() => {
                        if !locked {
                            entry.obtain.send(NotifyPermit(notify.clone()));
                        } else {
                            queue.push(entry);
                        }
                    }
                    () = notify.notified() => {
                        if let Some(current) = queue.pop() {
                            current.obtain.send(NotifyPermit(notify.clone()));
                        } else {
                            locked = false;
                        }
                    }
                    else => return,
                }
            }
        });
        Self {
            enqueue,
            abort_task: AbortOnDrop::new(task.abort_handle()),
        }
    }

    pub(super) fn register(&self, priority: usize) -> PriorityMutexRegistration {
        let (obtain, obtained) = oneshot::channel();
        self.enqueue.send(Entry { priority, obtain }).unwrap();
        PriorityRegistration { obtained }
    }
}

pub(super) type PriorityMutexRegistration = PriorityRegistration<NotifyPermit>;

#[derive(Debug)]
pub(super) struct NotifyPermit(Arc<Notify>);

impl Drop for NotifyPermit {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Debug, Clone)]
pub(super) struct PrioritySemaphore {
    enqueue: mpsc::UnboundedSender<Entry<OwnedSemaphorePermit>>,
    abort_task: AbortOnDrop,
}

impl PrioritySemaphore {
    pub(super) fn new(permits: usize) -> Self {
        let (enqueue, mut enqueued) = mpsc::unbounded_channel::<Entry<OwnedSemaphorePermit>>();
        let task = tokio::spawn(async move {
            let semaphore = Arc::new(sync::Semaphore::new(permits));
            let mut queue = BinaryHeap::new();
            let mut permit = None;
            loop {
                select! {
                    Some(entry) = enqueued.recv() => {
                        if let Some(permit) = permit.take() {
                            entry.obtain.send(permit);
                        } else {
                            queue.push(entry);
                        }
                    }
                    Ok(perm) = semaphore.clone().acquire_owned() => {
                        if let Some(current) = queue.pop() {
                            current.obtain.send(perm);
                        } else {
                            permit = Some(perm);
                        }
                    }
                    else => return,
                }
            }
        });
        Self {
            enqueue,
            abort_task: AbortOnDrop::new(task.abort_handle()),
        }
    }

    pub(super) fn register(&self, priority: usize) -> PrioritySemaphoreRegistration {
        let (obtain, obtained) = oneshot::channel();
        self.enqueue.send(Entry { priority, obtain }).unwrap();
        PriorityRegistration { obtained }
    }
}

pub(super) type PrioritySemaphoreRegistration = PriorityRegistration<OwnedSemaphorePermit>;

#[derive(Debug, Derivative)]
#[derivative(PartialEq, Eq, PartialOrd, Ord)]
struct Entry<T> {
    priority: usize,
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    obtain: oneshot::Sender<T>,
}

#[derive(Debug)]
pub(super) struct PriorityRegistration<T> {
    obtained: oneshot::Receiver<T>,
}

impl<T> PriorityRegistration<T> {
    pub(super) async fn aquire(self) -> Guard<T> {
        Guard {
            permit: self
                .obtained
                .await
                .expect("priority mutex sender should be listening while executors are running"),
        }
    }
}

#[derive(Debug)]
pub(super) struct Guard<T> {
    permit: T,
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::{future::select_all, poll};
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_mutex_single() {
        let sem = Mutex::new();
        let guard = sem.lock().await;
        drop(guard);
        let guard = sem.lock().await;
        drop(guard);
    }

    #[tokio::test]
    async fn test_mutex_multiple() {
        let sem = Mutex::new();
        let mut futs = (0..25).map(|_| Box::pin(sem.lock())).collect_vec();
        while !futs.is_empty() {
            let (_guard, _, mut remaining) = select_all(futs).await;
            assert_eq!(
                Poll::Pending,
                // ignore the passing value since it doesn't impl Debug.
                poll!(select_all(&mut remaining)).map(|_| ()),
                "semaphore allowed progress when all leases were aquired",
            );
            futs = remaining;
        }
    }

    #[tokio::test]
    async fn test_semaphore_single() {
        let sem = Semaphore::new(1);
        let guard = sem.aquire().await;
        drop(guard);
        let guard = sem.aquire().await;
        drop(guard);
    }

    #[tokio::test]
    async fn test_semaphore_multiple() {
        let sem = Semaphore::new(1);
        let mut futs = (0..25).map(|_| Box::pin(sem.aquire())).collect_vec();
        while !futs.is_empty() {
            let (_guard, _, mut remaining) = select_all(futs).await;
            assert_eq!(
                Poll::Pending,
                // ignore the passing value since it doesn't impl Debug.
                poll!(select_all(&mut remaining)).map(|_| ()),
                "semaphore allowed progress when all leases were aquired",
            );
            futs = remaining;
        }
    }

    #[tokio::test]
    async fn test_priority_semaphore_single() {
        let sem = PrioritySemaphore::new(1);
        let reg = sem.register(1);
        let guard = reg.aquire().await;
        drop(guard);
        let reg = sem.register(2);
        let guard = reg.aquire().await;
        drop(guard);
    }

    #[tokio::test]
    async fn test_priority_semaphore_multiple() {
        let sem = PrioritySemaphore::new(1);
        let mut registrations = [sem.register(1), sem.register(2), sem.register(3)]
            .into_iter()
            .map(|reg| Box::pin(reg.aquire()))
            .collect_vec();
        let mut futs = registrations.iter_mut().collect_vec();
        while !futs.is_empty() {
            let (_, i, remaining) = select_all(futs).await;
            if i != remaining.len() {
                panic!("lower priority registration aquired the semaphore first");
            }
            futs = remaining;
        }
    }

    #[tokio::test]
    async fn test_priority_mutex_single() {
        let sem = PriorityMutex::new();
        let reg = sem.register(1);
        let guard = reg.aquire().await;
        drop(guard);
        let reg = sem.register(2);
        let guard = reg.aquire().await;
        drop(guard);
    }

    #[tokio::test]
    async fn test_priority_mutex_multiple() {
        let sem = PriorityMutex::new();
        let mut registrations = [sem.register(1), sem.register(2), sem.register(3)]
            .into_iter()
            .map(|reg| Box::pin(reg.aquire()))
            .collect_vec();
        let mut futs = registrations.iter_mut().collect_vec();
        while !futs.is_empty() {
            let (_, i, remaining) = select_all(futs).await;
            if i != remaining.len() {
                panic!("lower priority registration aquired the semaphore first");
            }
            futs = remaining;
        }
    }
}
