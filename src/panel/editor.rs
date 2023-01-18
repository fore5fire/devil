use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::Paragraph;
use tui::Frame;
use unicode_segmentation::UnicodeSegmentation;

pub struct EditorPartial<'a> {
    text: Text<'a>,
    pub has_focus: bool,
    cursor_index: (usize, usize),
    cursor: (usize, usize),
}

impl<'a> EditorPartial<'a> {
    pub fn new(text: String, has_focus: bool) -> Self {
        Self {
            text: Text::raw(text),
            has_focus,
            cursor_index: (0, 0),
            cursor: (0, 0),
        }
    }

    pub fn draw<B: Backend>(&mut self, f: &mut Frame<B>, r: Rect) {
        f.set_cursor(r.x + self.cursor.0 as u16, r.y + self.cursor.1 as u16);
        let paragraph = Paragraph::new(self.text.clone());
        f.render_widget(paragraph, r);
    }

    pub fn event(&mut self, event: Event) {
        let Event::Key(key) = event else {
            return;
        };
        match key.code {
            KeyCode::Up => self.cursor_up(),
            KeyCode::Down => self.cursor_down(),
            KeyCode::Left => self.cursor_left(),
            KeyCode::Right => self.cursor_right(),

            KeyCode::Enter => {
                let old = &mut self.text.lines[self.cursor.1].0;
                let mut new: Vec<Span> = Vec::new();
                // Split the line after the cursor. We also take the entire span where the cursor
                // is even if part of it is before the cursor so we can have ownership to truncate
                // it, and then we add it back again.
                let mut remainder = old.drain(self.cursor_index.0..);
                let (pre, post) = match remainder.next().unwrap_or(Span::raw("")).content {
                    std::borrow::Cow::Owned(mut s) => {
                        let post = std::borrow::Cow::Owned(
                            s.drain(self.cursor_index.1..).collect::<String>(),
                        );
                        let pre = Span::raw(s);
                        (pre, post)
                    }
                    std::borrow::Cow::Borrowed(s) => (
                        Span::raw(&s[..self.cursor_index.1]),
                        std::borrow::Cow::Borrowed(&s[self.cursor_index.1..]),
                    ),
                };
                // Add the portion of the span after the cursor to the new line.
                new.push(Span::raw(post));
                // Move all remaining spans to the new line.
                new.extend(&mut remainder);
                // Move the beginning of the split span back if non-empty.
                drop(remainder);
                old.push(pre);
                // Move the cursor to the start of the new line.
                self.cursor = (0, self.cursor.1 + 1);
                self.cursor_index = (0, 0);
                // Add the new line to the editor.
                self.text.lines.insert(self.cursor.1, new.into());
            }
            KeyCode::Delete => {}
            KeyCode::Char(c) => {
                self.text.lines[self.cursor.1].0[self.cursor_index.0]
                    .content
                    .to_mut()
                    .insert(self.cursor_index.1, c);
                self.cursor_right()
            }

            _ => {}
        }
    }

    pub fn cursor_right(&mut self) {
        let line = &self.text.lines[self.cursor.1];
        if self.cursor.0 < line.width() {
            self.cursor.0 += 1;
        }

        // Decide if we should increment within the span or move to the next one.
        if let Some(_) = line.0[self.cursor_index.0]
            .content
            .graphemes(true)
            .nth(self.cursor_index.1)
        {
            self.cursor_index.1 += 1;
        } else if self.cursor_index.0 + 1 < line.0.len() {
            // Find the next non-empty span, if any.
            self.cursor_index.0 += 1;
            while self.cursor_index.0 + 1 < line.0.len()
                && line.0[self.cursor_index.0].content.is_empty()
            {
                self.cursor_index.0 += 1;
            }
            self.cursor_index.1 = 0;
        }
    }

    pub fn cursor_left(&mut self) {
        if self.cursor.0 > 0 {
            self.cursor.0 -= 1;
        }

        let line = &mut self.text.lines[self.cursor.1];
        // Decide if we should decrement within the span or move to the previous one.
        if self.cursor_index.1 > 0 {
            self.cursor_index.1 -= 1;
        } else if self.cursor_index.0 > 0 {
            // Find the previous non-empty span, if any.
            self.cursor_index.0 -= 1;
            while self.cursor_index.0 > 0 && line.0[self.cursor_index.0].content.is_empty() {
                self.cursor_index.0 -= 1;
            }
            self.cursor_index.1 = line.0[self.cursor_index.0].width() - 1;
        }
    }

    pub fn cursor_down(&mut self) {
        if self.cursor.1 + 1 >= self.text.height() {
            return;
        }
        self.cursor.1 += 1;
        let line = &self.text.lines[self.cursor.1];
        let width = line.width();
        if self.cursor.0 > width {
            self.cursor.0 = width;
            let default = &Span::raw("");
            let span = line.0.last().unwrap_or(default);
            self.cursor_index.0 = line.0.len() - 1;
            self.cursor_index.1 = span.width();
        } else {
            self.reindex_cursor();
        }
    }

    pub fn cursor_up(&mut self) {
        if self.cursor.1 == 0 {
            return;
        }
        self.cursor.1 -= 1;
        let line = &self.text.lines[self.cursor.1];
        let width = line.width();
        if self.cursor.0 > width {
            self.cursor.0 = width;
            let default = &Span::raw("");
            let span = line.0.last().unwrap_or(default);
            self.cursor_index.0 = line.0.len() - 1;
            self.cursor_index.1 = span.width();
        } else {
            self.reindex_cursor();
        }
    }

    fn reindex_cursor(&mut self) {
        let line = &self.text.lines[self.cursor.1];
        // Find the span which contains the cursor.
        let mut i = 0;
        self.cursor_index.0 = 0;
        for span in line.0.iter() {
            let width = span.width();
            if i + width > self.cursor.0 {
                break;
            }
            i += width;
            self.cursor_index.0 += 1;
        }
        // If we went past the end of the line then use the last element.
        if self.cursor_index.0 == line.0.len() {
            self.cursor_index.0 = line.0.len() - 1;
            self.cursor_index.1 = line.0.last().unwrap_or(&Span::raw("")).width();
            return;
        }
        // Find the cursor index within the selected span.
        let remaining = self.cursor.0 - i;
        let count = line.0[self.cursor_index.0]
            .styled_graphemes(Style::default())
            .take(remaining)
            .count();
        self.cursor_index.1 = remaining.min(count);
    }
}
