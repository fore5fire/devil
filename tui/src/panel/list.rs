use crossterm::event::{Event, KeyCode, KeyModifiers};
use tui::backend::Backend;
use tui::style::{Color, Modifier};
use tui::widgets::{ListItem, ListState};

/// ListPartial provides utilities to help with rendering and control of a list of items.
pub struct ListPartial {
    pub items: Vec<String>,
    pub has_focus: bool,
    list_state: ListState,
    height: Option<usize>,
}

impl<'a> ListPartial {
    /// Creates a new ListPartial.
    pub fn new(has_focus: bool, default: usize, items: Vec<String>) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(default));
        Self {
            items,
            list_state,
            has_focus,
            height: None,
        }
    }

    /// The currently selected option. If Some, guaranteed to be less than the length of items.
    pub fn selected(&self) -> Option<usize> {
        self.list_state
            .selected()
            .and_then(|s| if s < self.items.len() { Some(s) } else { None })
    }

    pub fn select(&mut self, i: usize) {
        self.list_state.select(Some(i))
    }

    /// Moves the selected item by an offset relative to the current item, or 0 if no item is
    /// selected.
    pub fn offset(&mut self, rows: isize) {
        let i = self.list_state.selected().unwrap_or(0) as isize;
        let i = (i + rows).min(self.items.len() as isize - 1).max(0) as usize;
        self.list_state.select(Some(i));
    }

    /// Process an event. j/up-arrow moves up in the list, k/down-arrow moves down, ctrl-u/d moves
    /// up/down one half screen, ctrl-f/b or page-up/page-down moves up/down one full screen. g/G
    /// moves to the beginning or end. / initiates a forward search and ? a backwards search, which
    /// are followed by n and N to jump to the next/previous result. y copies the current selection
    /// to the clipboard.
    pub fn event(&mut self, event: &Event) {
        let Event::Key(key) = event else {
            return;
        };
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => self.offset(1),
            KeyCode::Char('k') | KeyCode::Up => self.offset(-1),
            KeyCode::Char('u') | KeyCode::Char('b') | KeyCode::Char('d') | KeyCode::Char('f')
                if key.modifiers == KeyModifiers::CONTROL =>
            {
                let Some(height) = self.height else {
                    return;
                };
                let height = height as isize;
                let rows = match key.code {
                    KeyCode::Char('u') => height / -2,
                    KeyCode::Char('d') => height / 2,
                    KeyCode::Char('b') => -height,
                    KeyCode::Char('f') => height,
                    _ => unreachable!(),
                };
                self.offset(rows);
            }
            KeyCode::Char('g') => self.select(0),
            KeyCode::Char('G') => self.select(self.items.len() - 1),
            _ => {}
        }
    }

    /// Draws the list to f in the area specified by r.
    pub fn draw<B: Backend>(&mut self, f: &mut tui::Frame<B>, r: tui::layout::Rect) {
        let items: Vec<_> = self
            .items
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

        // Update the last-seen panel height.
        // This feels like a hack, probably should go in its own function that gets called
        // explicitly.
        self.height = Some(r.height.into());
    }
}
