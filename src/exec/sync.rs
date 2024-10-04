use std::{collections::BinaryHeap, sync::Arc};

use derivative::Derivative;
use tokio::{
    select,
    sync::{self, mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    task::AbortHandle,
};

use crate::PauseValueOutput;

pub(super) struct Synchronizers {
    signal: Vec<Signal>,
    sync: Vec<Synchronizer>,
    out: Vec<PauseValueOutput>,
}

pub(super) struct Signal {}

pub(super) enum Synchronizer {
    Barrier(sync::Barrier),
    PriorityMutex(PrioritySemaphore),
}

pub(super) struct PrioritySemaphore {
    enqueue: mpsc::UnboundedSender<Entry>,
    abort_task: AbortHandle,
}

impl PrioritySemaphore {
    pub(super) fn new(permits: usize) -> Self {
        let (enqueue, mut enqueued) = mpsc::unbounded_channel::<Entry>();
        let task = tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::const_new(permits));
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
            abort_task: task.abort_handle(),
        }
    }

    pub(super) fn register(&self, priority: usize) -> PrioritySemaphoreRegistration {
        let (obtain, obtained) = oneshot::channel();
        self.enqueue.send(Entry { priority, obtain }).unwrap();
        PrioritySemaphoreRegistration { obtained }
    }
}

impl Drop for PrioritySemaphore {
    fn drop(&mut self) {
        self.abort_task.abort();
    }
}

#[derive(Debug, Derivative)]
#[derivative(PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    priority: usize,
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    obtain: oneshot::Sender<OwnedSemaphorePermit>,
}

pub(super) struct PrioritySemaphoreRegistration {
    obtained: oneshot::Receiver<OwnedSemaphorePermit>,
}

impl PrioritySemaphoreRegistration {
    pub(super) async fn aquire(self) -> PrioritySemaphoreGuard {
        PrioritySemaphoreGuard {
            permit: self
                .obtained
                .await
                .expect("priority mutex sender should be listening while executors are running"),
        }
    }
}

pub(super) struct PrioritySemaphoreGuard {
    permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use futures::future::select_all;
    use itertools::Itertools;
    use std::pin::{pin, Pin};
    use tokio::time::{self, Duration};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[tokio::test]
    async fn test_single() {
        let sem = PrioritySemaphore::new(1);
        let reg = sem.register(1);
        let guard = reg.aquire().await;
        drop(guard);
    }

    #[tokio::test]
    async fn test_multiple() {
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
}
