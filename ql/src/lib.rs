pub mod exec;
pub mod http;
mod plan;
mod step;
pub mod tcp;
mod util;

pub use plan::*;
pub use step::*;

#[derive(Debug, PartialEq, Eq)]
pub enum ReadUntil<'a> {
    Bytes(),
    Codepoints(Charset),
    Tag(&'a str),
    All,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Charset {
    UTF8,
    UTF16,
    UTF32,
}
