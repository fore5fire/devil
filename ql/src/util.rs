use nom::{bytes::complete::take_while1, IResult};

pub fn ident(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}
