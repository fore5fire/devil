use std::collections::HashSet;
use std::task::Poll;

use crossbeam::channel::{unbounded, Receiver};
use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::style::{Color, Modifier};
use tui::text::{Span, Spans, Text};
use tui::widgets::Paragraph;
use tui::widgets::{canvas::Label, Block, BorderType, Borders, Widget};
use tui::Frame;

use super::{EditorPartial, ListPartial, Signal};

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
        let plans = Vec::from([
            Plan {
                name: "hi".to_owned(),
                text: "hi test 123\nabc".to_owned(),
            },
            Plan {
                name: "bye".to_owned(),
                text: "bye test 123\nabc".to_owned(),
            },
        ]);
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

#[derive(Debug, Clone)]
struct Plan {
    name: String,
    text: String,
}

struct PlanEditPanel<'a> {
    plan: Plan,
    editor: EditorPartial<'a>,
}

impl PlanEditPanel<'_> {
    fn new(plan: Plan, has_focus: bool) -> Self {
        Self {
            editor: EditorPartial::new(plan.text.clone(), has_focus),
            plan,
        }
    }
}

impl<B: Backend> super::Panel<B> for PlanEditPanel<'_> {
    fn tick(&mut self) -> Vec<Signal<B>> {
        Vec::new()
    }

    fn draw(&mut self, f: &mut Frame<B>, r: Rect) {
        self.editor.draw(f, r)
    }

    fn event(&mut self, event: Event) -> Vec<Signal<B>> {
        self.editor.event(event.clone());

        let Event::Key(key) = event else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Enter => Vec::new(),
            KeyCode::Delete => Vec::new(),
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
