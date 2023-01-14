use crossterm::event::Event;
use tui::backend::Backend;

use super::{Panel, Signal};

pub struct NavStack<B: Backend> {
    panels: Vec<Box<dyn Panel<B>>>,
}

impl<B: Backend> NavStack<B> {
    pub fn new(start: Box<dyn Panel<B>>) -> NavStack<B> {
        Self {
            panels: vec![start],
        }
    }
    pub fn push(&mut self, panel: Box<dyn Panel<B>>) {
        self.panels.push(panel);
    }
    pub fn pop(&mut self) -> Option<Box<dyn Panel<B>>> {
        let Some(old) = self.panels.pop() else {
            return None;
        };
        if let Some(p) = self.panels.last_mut() {
            p.set_focus(old.has_focus())
        }
        Some(old)
    }
    fn handle_signal(&mut self, signal: Signal<B>) -> Option<Signal<B>> {
        match signal {
            Signal::NavStackPush(p) => {
                self.push(p);
                None
            }
            Signal::NavStackPop => {
                self.pop();
                None
            }
            _ => Some(signal),
        }
    }
}

impl<B: Backend> Panel<B> for NavStack<B> {
    fn tick(&mut self) -> Vec<Signal<B>> {
        (0..self.panels.len())
            .into_iter()
            .flat_map(|i| self.panels[i].tick())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|s| self.handle_signal(s))
            .collect()
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        if let Some(p) = self.panels.last_mut() {
            p.event(event)
                .into_iter()
                .filter_map(|s| self.handle_signal(s))
                .collect()
        } else {
            Vec::new()
        }
        //let Event::Key(key) = event else {
        //    return self.panels[self.current].event(event);
        //};
        //match key.code {
        //    KeyCode::Char('o') if key.modifiers == KeyModifiers::CONTROL => {
        //        if self.current > 0 {
        //            self.current -= 1
        //        }
        //        None
        //    }
        //    KeyCode::Char('i') if key.modifiers == KeyModifiers::CONTROL => {
        //        if self.current < self.panels.len() - 1 {
        //            self.current += 1
        //        }
        //        None
        //    }
        //    _ => self.panels[self.current].event(event),
        //}
    }
    fn draw<'a>(&mut self, f: &mut tui::Frame<B>, r: tui::layout::Rect) {
        self.panels.last_mut().map(|p| p.draw(f, r));
    }

    fn title(&self) -> &str {
        self.panels.last().map(|p| p.title()).unwrap_or("")
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.panels.last_mut().map(|p| p.set_focus(has_focus));
    }

    fn has_focus(&self) -> bool {
        self.panels.last().map(|p| p.has_focus()).unwrap_or(false)
    }
}
