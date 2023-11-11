use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub struct Error(pub String);

impl<I: Into<String>> From<I> for Error {
    fn from(value: I) -> Self {
        Error(value.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
