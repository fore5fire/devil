use crossbeam::channel::{unbounded, Receiver};
use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::style::Modifier;
use tui::widgets::{canvas::Label, Block, Borders, ListState};
use tui::Frame;
use url::Url;

use super::{Panel, Signal};

pub struct ConsolePanel {
    has_focus: bool,
    rx: Receiver<Request>,
    list_state: ListState,
    history: Vec<Request>,
}

impl ConsolePanel {
    pub fn new(has_focus: bool) -> Self {
        // We use crossbeam channels despite having async produceers since we have a synchronous
        // reciever and use an unboounded channel.
        let (tx, rx) = unbounded();

        Self {
            has_focus,
            rx,
            list_state: ListState::default(),
            history: Vec::new(),
        }
    }
}

impl<B: Backend> Panel<B> for ConsolePanel {
    fn draw<'a>(&mut self, f: &mut Frame<B>, r: Rect) {}

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Char('j') => {
                let i = match self.list_state.selected() {
                    Some(i) => (i + 1).min(self.history.len() - 1),
                    None => 0,
                };
                self.list_state.select(Some(i));
            }
            KeyCode::Char('k') => {
                let mut i = self.list_state.selected().unwrap_or(0);
                if i > 0 {
                    i -= 1;
                }
                self.list_state.select(Some(i));
            }
            KeyCode::Delete => {}
            _ => {}
        }
        Vec::new()
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.has_focus = has_focus
    }

    fn has_focus(&self) -> bool {
        self.has_focus
    }

    fn title(&self) -> &str {
        "Console"
    }

    fn tick(&mut self) -> Vec<Signal<B>> {
        // Add any completed updates to the output.
        while let Ok(entry) = self.rx.try_recv() {
            self.history.push(entry);
        }
        Vec::new()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Request {
    url: Url,
}
