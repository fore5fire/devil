mod bindings;
mod cel_functions;
mod error;
pub mod exec;
mod output;
mod plan;
pub mod record;

pub use output::*;
pub use plan::*;

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

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;
