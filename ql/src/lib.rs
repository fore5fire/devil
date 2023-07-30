pub mod exec;
mod http;
mod plan;
mod step;
mod tcp;
mod util;

pub use http::*;
pub use plan::*;
pub use step::*;
pub use tcp::*;

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
