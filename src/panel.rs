mod console;
mod editor;
mod index;
mod list;
mod nav;
mod plan;
mod select;

pub use console::*;
pub use editor::*;
pub use index::*;
pub use list::*;
pub use nav::*;
pub use plan::*;
pub use select::*;

use crossterm::event::Event;
use tui::{backend::Backend, layout::Rect, Frame};

pub enum Signal<B: Backend> {
    NavStackPush(Box<dyn Panel<B>>),
    NavStackPop,
    Exit,
}

pub trait Panel<B: Backend> {
    fn draw<'a>(&mut self, f: &mut Frame<B>, r: Rect);
    fn event(&mut self, key: Event) -> Vec<Signal<B>>;
    fn set_focus(&mut self, has_focus: bool);
    fn has_focus(&self) -> bool;
    fn title(&self) -> &str;
    fn tick(&mut self) -> Vec<Signal<B>>;
}
