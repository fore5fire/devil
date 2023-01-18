use crossterm::event::{Event, KeyCode};
use tui::backend::Backend;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::{Span, Text};
use tui::widgets::Paragraph;
use tui::Frame;
use unicode_segmentation::UnicodeSegmentation;

// EditorPartial provides utilities for rendering and controlling a text editor with tui-rs.
//
// TODO: It should supports UTF-8 rendering, displaying whitespace symbols, displaying and editing
// arbitrary codepoints as hex values (including zero-width characters), and fully customizable
// keybinds with support for vim-style modes and chords.
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

    // Draw the editor.
    pub fn draw<B: Backend>(&self, f: &mut Frame<B>, r: Rect) {
        f.set_cursor(r.x + self.cursor.0 as u16, r.y + self.cursor.1 as u16);
        let paragraph = Paragraph::new(self.text.clone());
        f.render_widget(paragraph, r);
    }

    // Process an event.
    pub fn event(&mut self, event: Event) {
        let Event::Key(key) = event else {
            return;
        };
        match key.code {
            KeyCode::Up => {
                self.cursor_up();
            }
            KeyCode::Down => {
                self.cursor_down();
            }
            KeyCode::Left => {
                self.cursor_left();
            }
            KeyCode::Right => {
                self.cursor_right();
            }

            KeyCode::Enter => self.newline(),
            // TODO: what's the expected behavior for multi-codepoint graphemes?
            KeyCode::Delete => self.delete(),
            KeyCode::Backspace => {
                if self.cursor_left() {
                    self.delete();
                }
            }
            KeyCode::Char(c) => {
                self.text.lines[self.cursor.1].0[self.cursor_index.0]
                    .content
                    .to_mut()
                    .insert(self.cursor_index.1, c);
                // TODO: we shouldn't move the cursor if inserting a codepoint which combines with
                // the previous grapheme.
                self.cursor_right();
            }

            _ => {}
        }
    }

    // Insert a newline at the cursor location.
    pub fn newline(&mut self) {
        let old = &mut self.text.lines[self.cursor.1].0;
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
        self.cursor = (0, self.cursor.1 + 1);
        self.cursor_index = (0, 0);
        // Add the new line to the editor.
        self.text.lines.insert(self.cursor.1, new.into());
    }

    // Delete the grapheme after the cursor.
    pub fn delete(&mut self) {
        let line = &mut self.text.lines[self.cursor.1].0;
        if let Some((i, grapheme)) = line[self.cursor_index.0]
            .content
            .grapheme_indices(true)
            .nth(self.cursor_index.1)
        {
            let remove_range = i..i + grapheme.len();
            let mut span = std::mem::replace(&mut line[self.cursor_index.0].content, "".into());
            let mut raw_str = span.to_mut().to_owned().into_bytes();
            raw_str.drain(remove_range);
            line[self.cursor_index.0].content = String::from_utf8(raw_str).unwrap().into();
        } else {
            let i = self.cursor_index.0 + 1;
            while i < line.len() {
                if !line[i].content.is_empty() {
                    line[i].content.to_mut().remove(0);
                }
                if line[i].content.is_empty() {
                    line.remove(i);
                }
            }
        }
    }

    // Move the cursor right unless it's already at the end of the current line. Returns whether
    // the cursor was able to move.
    pub fn cursor_right(&mut self) -> bool {
        let line = &self.text.lines[self.cursor.1];
        if self.cursor.0 == line.width() {
            return false;
        }
        self.cursor.0 += 1;

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
        true
    }

    // Move the cursor left unless it's already at the beginning of the current line. Returns
    // whether the cursor was able to move.
    pub fn cursor_left(&mut self) -> bool {
        if self.cursor.0 == 0 {
            return false;
        }
        self.cursor.0 -= 1;

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
        true
    }

    // Move the cursor down unless it's already at the bottom of the document. Returns whether the
    // cursor was able to move.
    pub fn cursor_down(&mut self) -> bool {
        if self.cursor.1 + 1 >= self.text.height() {
            return false;
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
        true
    }

    // Move the cursor up unless it's already at the top of the document. Returns whether the
    // cursor was able to move.
    pub fn cursor_up(&mut self) -> bool {
        if self.cursor.1 == 0 {
            return false;
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
        true
    }

    // Set self.cursor_index based by reading self.cursor and searching the line's graphemes for
    // the cursor position.
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
