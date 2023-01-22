use crossterm::event::{Event, KeyCode, KeyModifiers};
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    symbols::DOT,
    text::{Span, Spans},
    widgets::{Block, Borders, Tabs},
    Frame,
};

use crate::panel::{HistoryPanel, IndexPanel, NavStack, Panel, PlanListPanel, SelectPanel, Signal};

pub struct App<B: Backend> {
    panels: Vec<NavStack<B>>,
    sections: [Section; 2],
    focus: usize,
    layout: AppLayout,
}

struct Section {
    panels: Vec<usize>,
    focus: usize,
}

impl<B: Backend + 'static> App<B> {
    pub fn new() -> Self {
        let mut panels = vec![Self::new_panel(true), Self::new_panel(false)];
        panels[0].push(Box::new(HistoryPanel::new(true)));
        panels[1].push(Box::new(PlanListPanel::new(false)));
        Self {
            panels,
            focus: 0,
            sections: [
                Section {
                    focus: 0,
                    panels: vec![0],
                },
                Section {
                    focus: 0,
                    panels: vec![1],
                },
            ],
            layout: AppLayout::HorizontalSplit(50),
        }
    }

    pub fn tick(&mut self) -> Vec<Signal<B>> {
        self.panels.iter_mut().flat_map(|p| p.tick()).collect()
    }

    pub fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        // Forward any non-key events to the focused panel.
        let Event::Key(key) = event else {
            return self.focused_mut().event(event);
        };
        match key.code {
            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => {
                Vec::from([Signal::Exit])
            }
            KeyCode::Left | KeyCode::Up | KeyCode::Char('h') | KeyCode::Char('j')
                if key.modifiers == KeyModifiers::CONTROL =>
            {
                // Ignore when in full screen layout.
                if self.layout == AppLayout::NoSplit {
                    return Vec::new();
                }
                self.notify_focus(false);
                self.focus = if self.focus > 0 {
                    self.focus - 1
                } else {
                    self.sections.len() - 1
                };
                self.notify_focus(true);
                Vec::new()
            }
            KeyCode::Right | KeyCode::Down | KeyCode::Char('l') | KeyCode::Char('k')
                if key.modifiers == KeyModifiers::CONTROL =>
            {
                // Ignore when in full screen layout.
                if self.layout == AppLayout::NoSplit {
                    return Vec::new();
                }
                self.notify_focus(false);
                self.focus = if self.focus < self.sections.len() - 1 {
                    self.focus + 1
                } else {
                    0
                };
                self.notify_focus(true);
                Vec::new()
            }
            KeyCode::BackTab => {
                self.notify_focus(false);
                let mut section = &mut self.sections[self.focus];
                if section.focus > 0 {
                    section.focus -= 1
                } else {
                    self.focus = if self.focus > 0 {
                        self.focus - 1
                    } else {
                        self.sections.len() - 1
                    };
                    section = &mut self.sections[self.focus];
                    section.focus = section.panels.len() - 1;
                };
                self.notify_focus(true);
                Vec::new()
            }
            KeyCode::Tab => {
                self.notify_focus(false);
                let mut section = &mut self.sections[self.focus];
                if section.focus < section.panels.len() - 1 {
                    section.focus += 1
                } else {
                    self.focus = if self.focus < self.sections.len() - 1 {
                        self.focus + 1
                    } else {
                        0
                    };
                    section = &mut self.sections[self.focus];
                    section.focus = 0;
                };
                self.notify_focus(true);
                Vec::new()
            }
            KeyCode::Char('j') if key.modifiers == KeyModifiers::CONTROL => {
                self.notify_focus(false);
                self.panels
                    .push(NavStack::new(vec![Box::new(IndexPanel::new(true))]));
                let section = &mut self.sections[self.focus];
                section.focus += 1;
                section.panels.insert(self.focus, self.panels.len() - 1);
                Vec::new()
            }
            KeyCode::Char('n') if key.modifiers == KeyModifiers::CONTROL => {
                self.notify_focus(false);
                let section = &mut self.sections[self.focus];
                section.focus += 1;
                let section_focus = section.focus;
                self.add_panel(self.focus, section_focus, true);
                Vec::new()
            }
            KeyCode::Char('s') if key.modifiers == KeyModifiers::CONTROL => {
                self.layout = match self.layout {
                    AppLayout::NoSplit => AppLayout::VerticalSplit(50),
                    AppLayout::VerticalSplit(_) => AppLayout::HorizontalSplit(50),
                    AppLayout::HorizontalSplit(_) => AppLayout::NoSplit,
                };
                Vec::new()
            }
            // ctrl-w closes the focused panel.
            KeyCode::Char('w') if key.modifiers == KeyModifiers::CONTROL => {
                // Notify and remove the focused panel.
                self.notify_focus(false);
                let section = self.focused_section_mut();
                section.panels.remove(section.focus);

                // If there are no more panels in this section then open a new one.
                if section.panels.is_empty() {
                    self.add_panel(self.focus, 0, true);
                }

                // If we removed the last panel the focus is now overflowing, so cap it at the
                // last of this section's panels.
                let section = self.focused_section_mut();
                section.focus = section.focus.min(section.panels.len() - 1);

                self.notify_focus(true);
                Vec::new()
            }
            // All other key events are forwarded to the focused panel.
            _ => self.focused_mut().event(event),
        }
    }

    pub fn draw(&mut self, f: &mut Frame<B>) {
        let mut single = Section {
            focus: 0,
            panels: Vec::new(),
        };
        let (sections, focus_section) = match self.layout {
            AppLayout::NoSplit => {
                single.panels = self
                    .sections
                    .iter()
                    .flat_map(|s| s.panels.clone())
                    .collect();
                for section in self.sections[..self.focus].iter() {
                    single.focus += section.panels.len()
                }
                single.focus += self.sections[self.focus].focus;
                (vec![(&single, f.size())], 0)
            }

            AppLayout::VerticalSplit(percent) => (
                self.sections
                    .iter()
                    .zip(
                        Layout::default()
                            .direction(Direction::Horizontal)
                            .margin(0)
                            .constraints(
                                [
                                    Constraint::Percentage(percent),
                                    Constraint::Percentage(100 - percent),
                                ]
                                .as_ref(),
                            )
                            .split(f.size())
                            .into_iter(),
                    )
                    .collect(),
                self.focus,
            ),

            AppLayout::HorizontalSplit(percent) => (
                self.sections
                    .iter()
                    .zip(
                        Layout::default()
                            .direction(Direction::Vertical)
                            .constraints(
                                [
                                    Constraint::Percentage(percent),
                                    Constraint::Percentage(100 - percent),
                                ]
                                .as_ref(),
                            )
                            .split(f.size())
                            .into_iter(),
                    )
                    .collect(),
                self.focus,
            ),
        };
        for (i, (section, rect)) in sections.iter().enumerate() {
            // Create the block widget.
            let block = Block::default().borders(Borders::ALL);

            // Render the panel's contents.
            let panel_rect = block.inner(*rect);
            self.panels[section.panels[section.focus]].draw(f, panel_rect);

            // Calculate the tab size and set a blank block title to make space for it.
            let titles: Vec<_> = section
                .panels
                .iter()
                .map(|p| self.panels[*p].title())
                .map(Spans::from)
                .collect();
            // The total width is the width of each title, plus the width of spacers between the
            // titles, plus one space around the outside of the tabs section.
            let tabs_width = titles.iter().fold(0, |total, title| total + title.width())
                + titles.len().saturating_sub(1) * (2 + Span::raw(DOT).width())
                + 2;
            let block = block.title(" ".repeat(tabs_width));

            // Create the tabs widget.
            let tabs_rect = Rect {
                x: rect.x + 1,
                y: rect.y,
                width: rect.width.saturating_sub(2),
                height: 1,
            };
            let highlight_style = Style::default();
            let highlight_style = if i == focus_section {
                highlight_style.fg(Color::Blue).bg(Color::Gray)
            } else {
                highlight_style.fg(Color::Blue).bg(Color::DarkGray)
            };
            let tabs = Tabs::new(titles)
                .select(section.focus)
                .highlight_style(highlight_style)
                .divider(DOT);

            // Render the panel's block and tabs.
            f.render_widget(block, *rect);
            f.render_widget(tabs, tabs_rect);
        }
    }

    fn focused_mut(&mut self) -> &mut NavStack<B> {
        let section = &mut self.sections[self.focus];
        &mut self.panels[section.panels[section.focus]]
    }

    fn focused_section_mut(&mut self) -> &mut Section {
        &mut self.sections[self.focus]
    }

    fn notify_focus(&mut self, has_focus: bool) {
        self.focused_mut().set_focus(has_focus)
    }

    fn new_panel(has_focus: bool) -> NavStack<B> {
        NavStack::new(vec![Box::new(SelectPanel::new(
            "New",
            has_focus,
            Vec::from([
                "1. Console".to_owned(),
                "2. Plans".to_owned(),
                "3. Index".to_owned(),
            ]),
            Box::new(|i| {
                let panel: Box<dyn Panel<B>> = match i {
                    0 => Box::new(HistoryPanel::new(true)),
                    1 => Box::new(PlanListPanel::new(true)),
                    2 => Box::new(IndexPanel::new(true)),
                    _ => unreachable!(),
                };
                Vec::from([Signal::NavStackPush(panel)])
            }),
        ))])
    }

    /// Add a new panel at self.sections[section].panels[panel].
    fn add_panel(&mut self, section: usize, panel: usize, has_focus: bool) {
        self.panels.push(Self::new_panel(has_focus));
        let section = &mut self.sections[section];
        section.panels.insert(panel, self.panels.len() - 1);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AppLayout {
    VerticalSplit(u16),
    HorizontalSplit(u16),
    NoSplit,
}
