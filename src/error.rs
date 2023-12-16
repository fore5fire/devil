use std::{convert::Infallible, fmt::Display};

#[derive(Debug, PartialEq, Clone)]
pub struct Error(pub String);

impl From<cel_interpreter::ParseError> for Error {
    fn from(value: cel_interpreter::ParseError) -> Self {
        Error(value.to_string())
    }
}

impl From<Infallible> for Error {
    fn from(e: Infallible) -> Self {
        unreachable!()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
