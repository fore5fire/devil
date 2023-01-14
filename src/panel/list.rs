use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::style::{Color, Modifier};
use tui::widgets::{ListItem, ListState};

pub struct ListPartial {
    list_state: ListState,
    pub choices: Vec<String>,
    pub has_focus: bool,
}

impl<'a> ListPartial {
    pub fn new(has_focus: bool, default: usize, choices: Vec<String>) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(default));
        Self {
            choices,
            list_state,
            has_focus,
        }
    }

    /// The currently selected option. If Some, guaranteed to be less than the length of choices.
    pub fn selected(&self) -> Option<usize> {
        self.list_state.selected().and_then(|s| {
            if s < self.choices.len() {
                Some(s)
            } else {
                None
            }
        })
    }

    pub fn event(&mut self, event: &Event) {
        let Event::Key(key) = event else {
            return;
        };
        match key.code {
            KeyCode::Char('j') => {
                if self.choices.is_empty() {
                    return;
                }
                let i = match self.list_state.selected() {
                    Some(i) => (i + 1).min(self.choices.len() - 1),
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
            _ => {}
        }
    }

    pub fn draw<B: Backend>(&mut self, f: &mut tui::Frame<B>, r: tui::layout::Rect) {
        let items: Vec<_> = self
            .choices
            .iter()
            .map(|c| ListItem::new(c.to_owned()))
            .collect();

        let list = tui::widgets::List::new(items);
        let list = if self.has_focus {
            list.highlight_style(tui::style::Style::default().fg(Color::Blue).bg(Color::Gray))
        } else {
            list.highlight_style(
                tui::style::Style::default()
                    .fg(Color::LightBlue)
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::DIM),
            )
        };
        f.render_stateful_widget(list, r, &mut self.list_state);
    }
}
