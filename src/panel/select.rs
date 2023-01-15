use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;

use super::{ListPartial, Panel, Signal};

pub struct SelectPanel<'a, B: Backend> {
    title: &'a str,
    list: ListPartial,
    callback: Box<dyn Fn(usize) -> Vec<Signal<B>>>,
}

impl<'a, B: Backend> SelectPanel<'a, B> {
    pub fn new(
        title: &'a str,
        has_focus: bool,
        choices: Vec<String>,
        callback: Box<dyn Fn(usize) -> Vec<Signal<B>>>,
    ) -> SelectPanel<'a, B> {
        Self {
            title,
            list: ListPartial::new(has_focus, 0, Vec::from(choices)),
            callback,
        }
    }

    fn callback(&self, i: Option<usize>) -> Vec<Signal<B>> {
        let Some(i) = i else {
            return Vec::new();
        };
        if i >= self.list.items.len() {
            return Vec::new();
        }
        (self.callback)(i)
    }
}

impl<B: Backend> Panel<B> for SelectPanel<'_, B> {
    fn tick(&mut self) -> Vec<Signal<B>> {
        Vec::new()
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        self.list.event(&event);

        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Enter => self.callback(self.list.selected()),
            KeyCode::Char(n) if n.is_digit(10) => {
                let n = n.to_digit(10).unwrap() as usize;
                self.callback(Some(n - 1))
            }
            _ => Vec::new(),
        }
    }

    fn draw<'a>(&mut self, f: &mut tui::Frame<B>, r: tui::layout::Rect) {
        self.list.draw(f, r)
    }

    fn title(&self) -> &str {
        self.title
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.list.has_focus = has_focus;
    }

    fn has_focus(&self) -> bool {
        self.list.has_focus
    }
}
