use std::borrow::Cow;
use std::convert::TryInto;
use std::ops::Range;

use tui::backend::Backend;
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::Paragraph;
use tui::Frame;
use unicode_segmentation::UnicodeSegmentation;

/// EditorPartial provides utilities for rendering and controlling a text editor with tui-rs.
pub struct EditorPartial<'a> {
    text: Text<'a>,
    pub has_focus: bool,
    cursor_index: (usize, usize),
    cursor: Point,
    restore_x: usize,
    window: Rect,
}

impl<'a> EditorPartial<'a> {
    pub fn new<T>(text: T, has_focus: bool) -> Self
    where
        T: Into<Cow<'a, str>>,
    {
        Self {
            text: Text::raw(text),
            has_focus,
            cursor_index: (0, 0),
            cursor: Point::default(),
            restore_x: 0,
            window: Rect::default(),
        }
    }

    /// Draw the editor.
    pub fn draw<B: Backend>(&mut self, f: &mut Frame<B>, r: tui::layout::Rect) {
        self.window.width = r.width.into();
        self.window.height = r.height.into();
        f.set_cursor(
            r.x + u16::try_from(self.cursor.x.saturating_sub(self.window.x)).unwrap(),
            r.y + u16::try_from(self.cursor.y.saturating_sub(self.window.y)).unwrap(),
        );
        let paragraph = Paragraph::new(self.text.clone());
        let paragraph = paragraph.scroll((
            self.window.y.try_into().unwrap(),
            self.window.x.try_into().unwrap(),
        ));
        f.render_widget(paragraph, r);
    }

    pub fn set_scroll(&mut self, x: usize, y: usize) {
        self.window.x = x;
        self.window.y = y;
    }

    pub fn cursor(&self) -> Point {
        self.cursor
    }

    /// Moves the cursor relative to its current position. Returns whether the cursor was able to
    /// move at least one position.
    pub fn move_cursor(&mut self, direction: Direction, steps: usize) -> bool {
        match direction {
            Direction::Left => {
                let prev = self.cursor.x;
                self.set_cursor(Point {
                    x: self.cursor.x.saturating_sub(steps),
                    y: self.cursor.y,
                });
                if self.cursor.x != prev {
                    self.restore_x = self.cursor.x;
                }
                self.cursor.x != prev
            }
            Direction::Right => {
                let prev = self.cursor.x;
                self.set_cursor(Point {
                    x: self.cursor.x.saturating_add(steps),
                    y: self.cursor.y,
                });
                if self.cursor.x != prev {
                    self.restore_x = self.cursor.x;
                }
                self.cursor.x != prev
            }
            Direction::Up => {
                let prev = self.cursor;
                self.set_cursor(Point {
                    x: self.restore_x,
                    y: self.cursor.y.saturating_sub(steps),
                });
                self.cursor != prev
            }
            Direction::Down => {
                let prev = self.cursor;
                self.set_cursor(Point {
                    x: self.restore_x,
                    y: self.cursor.y.saturating_add(steps),
                });
                self.cursor != prev
            }
            // TODO: right-to-left text support. For now this just allows convinent line wrap.
            Direction::Prev => {
                let mut remaining = steps;
                let mut dest = self.cursor;
                while remaining > 0 {
                    if dest.x >= remaining {
                        dest.x -= remaining;
                        break;
                    } else if dest.y == 0 {
                        dest.x = 0;
                        break;
                    } else {
                        remaining -= dest.x + 1;
                        dest.y -= 1;
                        dest.x = self.text.lines[dest.y].width();
                    }
                }
                // Set the identified cursor location.
                let prev = self.cursor;
                self.set_cursor(dest);
                if self.cursor != prev {
                    self.restore_x = dest.x;
                }
                self.cursor != prev
            }
            Direction::Next => {
                if self.cursor.y >= self.text.lines.len() {
                    return false;
                }
                let mut remaining = steps;
                let mut dest = self.cursor;
                while remaining > 0 {
                    let line_steps = self.text.lines[dest.y].width() - dest.x;
                    if line_steps >= remaining {
                        dest.x += remaining;
                        break;
                    } else if dest.y == self.text.lines.len() - 1 {
                        dest.x += line_steps;
                        break;
                    } else {
                        remaining -= line_steps + 1;
                        dest.y += 1;
                        dest.x = 0;
                    }
                }
                // Set the identified cursor location.
                let prev = self.cursor;
                self.set_cursor(dest);
                if self.cursor != prev {
                    self.restore_x = dest.x;
                }
                self.cursor != prev
            }
        }
    }

    /// Insert a codepoint at the cursor location. Returns whether the insert created a new
    /// grapheme.
    pub fn insert(&mut self, c: char) -> bool {
        if self.cursor.y == self.text.lines.len() {
            self.text.lines.push(Spans::from(""));
        };
        let line = &mut self.text.lines[self.cursor.y];

        if self.cursor_index.0 == line.0.len() {
            line.0.push(Span::from(""))
        }

        line.0[self.cursor_index.0]
            .content
            .to_mut()
            .insert(self.cursor_index.1, c);

        // TODO: report whether inserting a codepoint combined with the previous grapheme.
        true
    }

    /// Insert a newline at the cursor location.
    pub fn newline(&mut self) {
        if self.cursor.y >= self.text.height() {
            self.text.lines.insert(self.cursor.y, Spans::from(""));
        };
        let old = &mut self.text.lines[self.cursor.y].0;
        // Split the line after the cursor. We also take the entire span where the cursor
        // is even if part of it is before the cursor so we can have ownership to truncate
        // it, and then we add it back again.
        let mut remainder = old.drain(self.cursor_index.0..);
        // If there's nothing past the cursor to copy, just add an empty line and return.
        let Some(mut span) = remainder.next() else {
            drop(remainder);
            self.text.lines.insert(self.cursor.y + 1, Spans::from(""));
            return;
        };
        // Split the the span under the cursor into the part to keep and part to move.
        let (pre, post) = match span.content {
            std::borrow::Cow::Owned(ref mut s) => {
                let post = Span::styled(
                    s.drain(self.cursor_index.1..).collect::<String>(),
                    span.style,
                );
                (span, post)
            }
            std::borrow::Cow::Borrowed(s) => (
                Span::styled(&s[..self.cursor_index.1], span.style),
                Span::styled(&s[self.cursor_index.1..], span.style),
            ),
        };
        // Build the new line from the split span and all following spans, ensuring it contains at
        // least one span.
        let mut new: Vec<Span> =
            Vec::with_capacity(remainder.len() + if post.content.is_empty() { 0 } else { 1 });
        if !post.content.is_empty() || remainder.len() == 0 {
            new.push(post);
        }
        new.extend(&mut remainder);
        // Move the beginning of the split span back to the original line if non-empty.
        drop(remainder);
        if pre.content.len() > 0 {
            old.push(pre);
        }
        // Add the new line to the editor.
        self.text.lines.insert(self.cursor.y + 1, Spans::from(new));
    }

    /// Delete the grapheme after the cursor.
    pub fn delete(&mut self) {
        // If the cursor is after the end of a document, like in a newly created empty document,
        // then there's nothing to do.
        if self.cursor.y >= self.text.lines.len() {
            return;
        };

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
        self.cursor.y = p.y.min(self.text.height().saturating_sub(1));
        self.cursor.x = p.x.min(
            self.text
                .lines
                .get_mut(self.cursor.y)
                .map(|line| line.width())
                .unwrap_or(0),
        );
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

    /// Returns the number of lines in the document.
    pub fn height(&self) -> usize {
        self.text.height().max(1)
    }

    /// Returns the width of the line the cursor is on.
    pub fn line_width(&self) -> usize {
        self.text
            .lines
            .get(self.cursor.y)
            .map(|line| line.width())
            .unwrap_or(0)
    }

    /// Set self.cursor_index based by reading self.cursor and searching the line's graphemes for
    /// the cursor position.
    fn reindex_cursor(&mut self) {
        let default = Spans::from("");
        let line = self.text.lines.get(self.cursor.y).unwrap_or(&default);
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
        if self.cursor_index.0 >= line.0.len() {
            self.cursor_index.0 = line.0.len().saturating_sub(1);
            self.cursor_index.1 = line.0.last().map(|span| span.width()).unwrap_or(0);
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

    /// Deletes a range of a span's content and verifies the span still contains valid UTF-8 text.
    fn delete_bytes(span: &mut Span, range: Range<usize>) {
        let mut content = std::mem::replace(&mut span.content, "".into());
        let mut raw_content = content.to_mut().to_owned().into_bytes();
        raw_content.drain(range);
        span.content = String::from_utf8(raw_content).unwrap().into();
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct Point {
    pub x: usize,
    pub y: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Rect {
    x: usize,
    y: usize,
    width: usize,
    height: usize,
}

impl From<tui::layout::Rect> for Rect {
    fn from(r: tui::layout::Rect) -> Self {
        Rect {
            x: r.x.into(),
            y: r.y.into(),
            width: r.width.into(),
            height: r.height.into(),
        }
    }
}

#[derive(Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
    Prev,
    Next,
}
