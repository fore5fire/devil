use std::convert::TryInto;
use std::ops::Range;

use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::style::Style;
use tui::text::{Span, Text};
use tui::widgets::Paragraph;
use tui::Frame;
use unicode_segmentation::UnicodeSegmentation;

// EditorPartial provides utilities for rendering and controlling a text editor with tui-rs.
pub struct EditorPartial<'a> {
    text: Text<'a>,
    pub has_focus: bool,
    cursor_index: (usize, usize),
    cursor: Point,
    window: Rect,
}

impl<'a> EditorPartial<'a> {
    pub fn new(text: String, has_focus: bool) -> Self {
        Self {
            text: Text::raw(text),
            has_focus,
            cursor_index: (0, 0),
            cursor: Point::default(),
            window: Rect::default(),
        }
    }

    // Draw the editor.
    pub fn draw<B: Backend>(&mut self, f: &mut Frame<B>, r: tui::layout::Rect) {
        self.window.width = r.width.into();
        self.window.height = r.height.into();
        f.set_cursor(
            r.x + u16::try_from(self.cursor.x - self.window.x).unwrap(),
            r.y + u16::try_from(self.cursor.y - self.window.y).unwrap(),
        );
        let paragraph = Paragraph::new(self.text.clone());
        let paragraph = paragraph.scroll((
            self.window.y.try_into().unwrap(),
            self.window.x.try_into().unwrap(),
        ));
        f.render_widget(paragraph, r);
    }

    // Process an event.
    pub fn event(&mut self, event: Event) {
        let Event::Key(key) = event else {
            return;
        };
        match key.code {
            KeyCode::Up => {
                self.set_cursor(Point {
                    x: self.cursor.x,
                    y: self.cursor.y.saturating_sub(1),
                });
            }
            KeyCode::Down => {
                self.set_cursor(Point {
                    x: self.cursor.x,
                    y: self.cursor.y.saturating_add(1),
                });
            }
            KeyCode::Left => {
                self.set_cursor(Point {
                    x: self.cursor.x.saturating_sub(1),
                    y: self.cursor.y,
                });
            }
            KeyCode::Right => {
                self.set_cursor(Point {
                    x: self.cursor.x.saturating_add(1),
                    y: self.cursor.y,
                });
            }

            KeyCode::Enter => {
                self.newline();
                self.set_cursor(Point {
                    x: self.cursor.x,
                    y: self.cursor.y + 1,
                })
            }
            // TODO: what's the expected behavior for multi-codepoint graphemes?
            KeyCode::Delete => self.delete(),
            KeyCode::Backspace => {
                // Move to the previous character then delete it. It's possible that the previous
                // character is on the previous line, or doesn't exist at all.
                if self.cursor.x > 0 {
                    self.set_cursor(Point {
                        x: self.cursor.x - 1,
                        y: self.cursor.y,
                    });
                    self.delete();
                } else if self.cursor.y > 0 {
                    self.set_cursor(Point {
                        x: usize::MAX,
                        y: self.cursor.y - 1,
                    });
                    self.delete();
                }
            }
            KeyCode::Char(c) => {
                self.text.lines[self.cursor.y].0[self.cursor_index.0]
                    .content
                    .to_mut()
                    .insert(self.cursor_index.1, c);
                // TODO: we shouldn't move the cursor if inserting a codepoint which combines with
                // the previous grapheme.
                self.set_cursor(Point {
                    x: self.cursor.x + 1,
                    y: self.cursor.y,
                });
            }

            _ => {}
        }
    }

    // Insert a newline at the cursor location.
    pub fn newline(&mut self) {
        let old = &mut self.text.lines[self.cursor.y].0;
        let mut new: Vec<Span> = Vec::new();
        // Split the line after the cursor. We also take the entire span where the cursor
        // is even if part of it is before the cursor so we can have ownership to truncate
        // it, and then we add it back again.
        let mut remainder = old.drain(self.cursor_index.0..);
        let (pre, post) = match remainder.next().unwrap_or(Span::raw("")).content {
            std::borrow::Cow::Owned(mut s) => {
                let post =
                    std::borrow::Cow::Owned(s.drain(self.cursor_index.1..).collect::<String>());
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
        self.cursor = Point {
            x: 0,
            y: self.cursor.y + 1,
        };
        self.cursor_index = (0, 0);
        // Add the new line to the editor.
        self.text.lines.insert(self.cursor.y, new.into());
    }

    // Delete the grapheme after the cursor.
    pub fn delete(&mut self) {
        let line = &mut self.text.lines[self.cursor.y].0;
        // There are four cases we need to handle, from highest priority to lowest:
        // 1. The current span contains at least one grapheme after the cursor that we can delete.
        // 2. A span after the current one in the current line contains at least one grapheme that
        //    we can delete.
        // 3. There are no more graphemes in the current line, so merge the next line into the
        //    current line.
        // 4. There are no more lines after the current, so do nothing.
        //
        // We handle cases 1 and 2 with a loop starting at the current span and iterating spans
        // until a match is found.
        let mut i = self.cursor_index.0;
        let mut j = self.cursor_index.1;
        let mut found = false;
        while i < line.len() && found == false {
            if let Some((start, grapheme)) = line[i].content.grapheme_indices(true).nth(j) {
                let range = start..start + grapheme.len();
                Self::delete_bytes(&mut line[i], range);
                found = true
            }
            if line[i].content.is_empty() {
                line.remove(i);
            }
            i += 1;
            j = 0;
        }

        // If the cursor is at the end of a line then move the next line to the end of the
        // current.
        if !found && self.cursor.y + 1 < self.text.height() {
            let mut spans = self.text.lines.remove(self.cursor.y + 1);
            self.text.lines[self.cursor.y].0.append(&mut spans.0);
        }
    }

    /// Sets the cursor position to p.
    pub fn set_cursor(&mut self, p: Point) {
        self.cursor.y = p.y.min(self.text.height() - 1);
        self.cursor.x = p.x.min(self.text.lines[self.cursor.y].width());
        if self.cursor.x < self.window.x {
            self.window.x = self.cursor.x;
        } else if self.cursor.x >= self.window.x + self.window.width {
            self.window.x = self.cursor.x - self.window.width + 1;
        }
        if self.cursor.y < self.window.y {
            self.window.y = self.cursor.y;
        } else if self.cursor.y >= self.window.y + self.window.height {
            self.window.y = self.cursor.y - self.window.height + 1;
        }
        self.reindex_cursor();
    }

    // Set self.cursor_index based by reading self.cursor and searching the line's graphemes for
    // the cursor position.
    fn reindex_cursor(&mut self) {
        let line = &self.text.lines[self.cursor.y];
        // Find the span which contains the cursor.
        let mut i = 0;
        self.cursor_index.0 = 0;
        for span in line.0.iter() {
            let width = span.width();
            if i + width > self.cursor.x {
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
        let remaining = self.cursor.x - i;
        let count = line.0[self.cursor_index.0]
            .styled_graphemes(Style::default())
            .take(remaining)
            .count();
        self.cursor_index.1 = remaining.min(count);
    }

    fn delete_bytes(span: &mut Span, range: Range<usize>) {
        let mut content = std::mem::replace(&mut span.content, "".into());
        let mut raw_content = content.to_mut().to_owned().into_bytes();
        raw_content.drain(range);
        span.content = String::from_utf8(raw_content).unwrap().into();
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct Point {
    x: usize,
    y: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
struct Rect {
    x: usize,
    y: usize,
    width: usize,
    height: usize,
}
