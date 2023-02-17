use nom::{
    branch::alt,
    bytes::complete::{
        escaped_transform, tag, take_until, take_until1, take_while1, take_while_m_n,
    },
    character::{
        complete::{alpha1, line_ending, multispace0, none_of, not_line_ending},
        is_space,
    },
    combinator::{iterator, map, value},
    sequence::{terminated, Tuple},
    IResult,
};

use super::{util::ident, Step};

#[derive(Debug)]
pub struct Plan<'a> {
    steps: Vec<Step<'a>>,
}

impl Plan<'_> {
    pub fn parse(input: &str) -> IResult<&str, Plan> {
        // Step over whitespace before the first step.
        let (input, _) = multispace0(input)?;

        let it = iterator(input, terminated(Step::parse, multispace0));
        let steps = it.collect();
        let (input, _) = it.finish()?;
        Ok((input, Plan { steps }))
    }
}

fn from_hex(input: &str) -> Result<u8, std::num::ParseIntError> {
    u8::from_str_radix(input, 16)
}

fn is_hex_digit(c: char) -> bool {
    c.is_digit(16)
}

#[test]
fn hex_test() {
    assert_eq!(
        hex_color("#2F14DF"),
        Ok(("", HTTPRequest { version: "1.1" },))
    );
    assert_eq!(
        hex_color("$2F14DF"),
        Err(nom::Err::Error(nom::error::Error::new(
            "$2F14DF",
            nom::error::ErrorKind::Tag,
        )))
    );
}
