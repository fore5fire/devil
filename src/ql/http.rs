use nom::{
    branch::alt,
    bytes::complete::{
        escaped_transform, is_not, tag, take_until, take_until1, take_while, take_while1,
    },
    character::complete::{alpha1, line_ending, none_of, not_line_ending, space0, space1},
    combinator::{iterator, map, value},
    error::ParseError,
    multi::separated_list0,
    sequence::{terminated, Tuple},
    Compare, IResult, InputTake, Parser,
};

use super::util::ident;

#[derive(Debug, PartialEq)]
pub struct HTTPRequest<'a> {
    pub endpoint: &'a str,
    pub version: &'a str,
    pub headers: Vec<(&'a str, &'a str)>,
    pub body: Vec<u8>,
}

impl<'a> HTTPRequest<'a> {
    pub fn parse(input: &'a str, eof: &str) -> IResult<&'a str, HTTPRequest<'a>> {
        // Read the connection details.
        let (input, (method, endpoint)) = (ident, take_until1("\n")).parse(input)?;

        // Read the headers.
        let headers = separated_list0(line_ending, header)(input);

        // Read the body, applying any escape characters.
        let (input, encoded_body) = take_until(("\n".to_owned() + eof).as_str())(input)?;
        let (input, body) = escaped_transform(
            none_of("\\"),
            '\\',
            alt((value("\\", tag("\\")), map_res(hex, |bytes| bytes))),
        )(encoded_body)?;

        Ok((
            input,
            HTTPRequest {
                endpoint,
                version: "1.1",
                headers,
                body,
            },
        ))
    }
}

fn header<'a>(input: &str) -> IResult<&'a str, (&'a str, &'a str)> {
    (
        terminated(escaped(alt(is_not(":"), line_ending)), tag(":")),
        space0,
        terminated(escaped(not_line_ending), line_ending),
    )
        .parse(input)
}

pub fn escaped<'a, E: ParseError<&'a str>>(
    mut end: &str,
) -> impl FnMut(&str) -> IResult<&'a str, E> {
    let end = format!("\\{}", end);
    move |input: &str| {
        escaped_transform(
            is_not(end.as_str()),
            '\\',
            alt((
                value("\\", tag("\\")),
                value("\"", tag("\"")),
                value("\n", tag("n")),
                value("\r", tag("r")),
            )),
        )(input)
    }
}

fn from_hex(input: &str) -> Result<Vec<u8>, std::num::ParseIntError> {
    input
        .chars()
        .enumerate()
        .step_by(2)
        .map(|(c1, c2)| u8::from_str_radix(input, 16))
        .collect()
}

fn is_hex_digit(c: char) -> bool {
    c.is_digit(16)
}

fn step(input: &str) -> IResult<&str, HTTPRequest> {
    let (input, _) = tag("#")(input)?;
    let (input, (red, green, blue)) = (tag("match"), space1, ident).parse(input)?;

    Ok((input, Step { red, green, blue }))
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
    assert_eq!(
        hex_color("#2F14DG"),
        Err(nom::Err::Error(nom::error::Error::new(
            "DG",
            nom::error::ErrorKind::TakeWhileMN,
        )))
    );
}
