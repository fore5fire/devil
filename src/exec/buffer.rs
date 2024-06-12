use std::ops::{Deref, DerefMut, Index, IndexMut};

pub(crate) struct Buffer {
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            start: 0,
            end: 0,
        }
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf[self.start..self.end]
    }
}

impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.buf[self.start..self.end]
    }
}
