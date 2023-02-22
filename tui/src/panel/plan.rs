use crossbeam::channel::{unbounded, Receiver};
use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::{self, Rect};
use tui::widgets::Paragraph;
use tui::widgets::{canvas::Label, Block, BorderType, Borders, Widget};
use tui::Frame;

use super::{Direction, EditorPartial, ListPartial, Signal};
use crate::ql::{HTTPRequest, Plan, Step};

pub struct PlanListPanel {
    plans: Vec<Plan>,
    list: ListPartial,
    rx: Receiver<Plan>,
}

impl PlanListPanel {
    pub fn new(has_focus: bool) -> Self {
        // We use crossbeam channels despite having async produceers since we have a synchronous
        // reciever and use an unboounded channel.
        let (tx, rx) = unbounded();
        let plans = Vec::from([]);
        Self {
            list: ListPartial::new(has_focus, 0, plans.iter().map(|p| p.name.clone()).collect()),
            plans,
            rx,
        }
    }
}

impl<B: Backend> super::Panel<B> for PlanListPanel {
    fn tick(&mut self) -> Vec<Signal<B>> {
        // Add any completed updates to the output.
        while let Ok(plan) = self.rx.try_recv() {
            self.plans.push(plan);
        }
        Vec::new()
    }

    fn draw(&mut self, f: &mut Frame<B>, r: Rect) {
        self.list.draw(f, r);
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        self.list.event(&event);

        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Enter => {
                let Some(i) = self.list.selected() else {
                    return Vec::new();
                };
                let child = Box::new(PlanEditPanel::new(
                    self.plans[i].clone(),
                    self.list.has_focus,
                ));
                Vec::from([Signal::NavStackPush(child)])
            }
            KeyCode::Delete => Vec::new(),
            _ => Vec::new(),
        }
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.list.has_focus = has_focus;
    }

    fn has_focus(&self) -> bool {
        self.list.has_focus
    }

    fn title(&self) -> &str {
        "Plans"
    }
}

struct PlanEditPanel<'a> {
    plan: Plan,
    editor: EditorPartial<'a>,
}

impl<'a> PlanEditPanel<'a> {
    fn new(plan: Plan, has_focus: bool) -> PlanEditPanel<'a> {
        Self {
            editor: EditorPartial::new(
                plan.steps
                    .iter()
                    .map(|s| match s {
                        Step::HTTP(req) => req.0.as_str(),
                    })
                    .collect::<Vec<_>>()
                    .join("\n\n"),
                has_focus,
            ),
            plan,
        }
    }
}

impl<B: Backend> super::Panel<B> for PlanEditPanel<'_> {
    fn tick(&mut self) -> Vec<Signal<B>> {
        Vec::new()
    }

    fn draw(&mut self, frame: &mut Frame<B>, area: Rect) {
        let mut layout = layout::Layout::default()
            .direction(layout::Direction::Horizontal)
            .margin(0)
            .constraints(
                [
                    layout::Constraint::Percentage(50),
                    layout::Constraint::Percentage(50),
                ]
                .as_ref(),
            )
            .split(area)
            .into_iter();

        self.editor.draw(frame, layout.next().unwrap());

        let responses = Paragraph::new("");
        frame.render_widget(responses, layout.next().unwrap());
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Up => {
                self.editor.move_cursor(Direction::Up, 1);
                Vec::new()
            }
            KeyCode::Down => {
                self.editor.move_cursor(Direction::Down, 1);
                Vec::new()
            }
            KeyCode::Left => {
                self.editor.move_cursor(Direction::Left, 1);
                Vec::new()
            }
            KeyCode::Right => {
                self.editor.move_cursor(Direction::Right, 1);
                Vec::new()
            }
            KeyCode::Enter => {
                self.editor.newline();
                self.editor.move_cursor(Direction::Next, 1);
                Vec::new()
            }
            KeyCode::Backspace => {
                if self.editor.move_cursor(Direction::Prev, 1) {
                    self.editor.delete();
                }
                Vec::new()
            }
            KeyCode::Delete => {
                self.editor.delete();
                Vec::new()
            }
            KeyCode::Char(c) => {
                if self.editor.insert(c) {
                    self.editor.move_cursor(Direction::Next, 1);
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn set_focus(&mut self, has_focus: bool) {
        self.editor.has_focus = has_focus
    }

    fn has_focus(&self) -> bool {
        self.editor.has_focus
    }

    fn title(&self) -> &str {
        &self.plan.name
    }
}
