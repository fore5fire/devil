use crossbeam::channel::{unbounded, Receiver};
use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::widgets::{canvas::Label, Block, Borders};
use tui::Frame;
use url::Url;

use super::{ListPartial, Panel, Signal};

pub struct ConsolePanel {
    has_focus: bool,
    rx: Receiver<Request>,
    list: ListPartial,
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
            list: ListPartial::new(has_focus, 0, Vec::new()),
            history: Vec::new(),
        }
    }

    fn push(&mut self, req: Request) {
        self.history.push(req);
        self.list.items = self.history.iter().map(|req| req.to_string()).collect();
    }
}

impl<B: Backend> Panel<B> for ConsolePanel {
    fn draw<'a>(&mut self, f: &mut Frame<B>, r: Rect) {
        self.list.draw(f, r);
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
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
            self.push(entry);
        }
        Vec::new()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Request {
    url: Url,
    method: String,
}

impl ToString for Request {
    fn to_string(&self) -> String {
        format!("{} {}", self.url, self.method)
    }
}
