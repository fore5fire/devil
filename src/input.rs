use futures::StreamExt;
use std::time::Duration;

use crossterm::event::{Event, EventStream};
use tokio::sync::mpsc;
use tokio::time::interval;

pub enum InputEvent {
    Input(Event),
    Tick,
}

pub struct Events {
    rx: mpsc::Receiver<InputEvent>,
    task: tokio::task::JoinHandle<()>,
}

impl Events {
    pub fn new(tick_rate: Duration) -> Events {
        let (tx, rx) = mpsc::channel(10);
        let task = tokio::spawn(async move {
            let mut stream = EventStream::new();
            let mut ticker = interval(tick_rate);
            loop {
                tokio::select! {
                    event = stream.next() => {
                        _ = tx.send(InputEvent::Input(event.expect("event stream ended").expect("input error"))).await;
                    }
                    _ = ticker.tick() => {
                        _ = tx.send(InputEvent::Tick).await;
                    }
                }
            }
        });

        Events { rx, task }
    }

    pub fn next(&mut self) -> InputEvent {
        self.rx
            .blocking_recv()
            .expect("Events::next called after channel was closed.")
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        self.task.abort();
    }
}
