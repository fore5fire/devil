use std::collections::HashSet;

use crossbeam::channel::{unbounded, Receiver};
use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::style::Modifier;
use tui::widgets::{canvas::Label, Block, Borders};
use tui::Frame;
use url::Url;

use super::{ListPartial, Panel, Signal};

pub struct IndexPanel {
    entries: HashSet<IndexEntry>,
    rx: Receiver<IndexEntry>,
    list: ListPartial,
}

impl IndexPanel {
    pub fn new(has_focus: bool) -> Self {
        // We use crossbeam channels despite having async produceers since we have a synchronous
        // reciever and use an unboounded channel.
        let (tx, rx) = unbounded();

        Self {
            entries: HashSet::new(),
            rx,
            list: ListPartial::new(has_focus, 0, Vec::new()),
        }
    }
}

impl<B: Backend> Panel<B> for IndexPanel {
    fn draw<'a>(&mut self, f: &mut Frame<B>, r: Rect) {
        self.list.draw(f, r)
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        self.list.event(&event);

        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Delete => {}
            _ => {}
        }
        Vec::new()
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.list.has_focus = has_focus;
    }

    fn has_focus(&self) -> bool {
        self.list.has_focus
    }

    fn title(&self) -> &str {
        "Index"
    }

    fn tick(&mut self) -> Vec<Signal<B>> {
        // Add any completed updates to the output.
        while let Ok(entry) = self.rx.try_recv() {
            self.entries.insert(entry);
        }
        Vec::new()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IndexEntry {
    url: Url,
}
