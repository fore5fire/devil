use std::fmt::Display;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_until},
    character::complete::{alpha1, line_ending, not_line_ending, space0, space1},
    multi::many_till,
    sequence::{pair, separated_pair, terminated},
    IResult,
};

#[derive(Debug, PartialEq)]
pub struct HTTPRequest<'a> {
    pub method: &'a str,
    pub endpoint: hyper::Uri,
    pub version: Protocol,
    pub headers: Vec<(&'a str, &'a str)>,
    pub body: &'a str,
}

impl<'a> HTTPRequest<'a> {
    pub fn parse(input: &'a str, eof: &str) -> IResult<&'a str, Self> {
        // Read the connection details.
        let (input, (method, endpoint)) =
            terminated(separated_pair(alpha1, space1, not_line_ending), line_ending)(input)?;

        // Read the headers.
        let (input, (headers, _)) = many_till(terminated(header, line_ending), line_ending)(input)?;

        // Read the body, allowing either line ending before the eof token.
        let eof = format!("\r\n{}", eof);
        let (input, body) = alt((
            terminated(take_until(eof.as_str()), tag(eof.as_str())),
            terminated(take_until(&eof[1..]), tag(&eof[1..])),
        ))(input)?;

        Ok((
            input,
            HTTPRequest {
                method,
                endpoint: endpoint.parse::<hyper::Uri>().map_err(|e| {
                    nom::Err::Error(nom::error::Error {
                        input: endpoint,
                        code: nom::error::ErrorKind::Tag,
                    })
                })?,
                version: Protocol::HTTP1_1,
                headers,
                body,
            },
        ))
    }
}

fn header(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(header_key, pair(tag(":"), space0), header_val)(input)
}

pub fn header_key(input: &str) -> IResult<&str, &str> {
    is_not(":")(input)
}

pub fn header_val(input: &str) -> IResult<&str, &str> {
    not_line_ending(input)
}
#[derive(Debug, PartialEq, Eq)]
pub enum Protocol {
    HTTP1_1,
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HTTP1_1 => f.write_str("HTTP/1.1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_test() {
        assert_eq!(
            HTTPRequest::parse(
                "POST example.com\nContent-Type: text/plain\n\ntest body\nEOF",
                "EOF"
            ),
            Ok((
                "",
                HTTPRequest {
                    method: "POST",
                    version: Protocol::HTTP1_1,
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: vec![("Content-Type", "text/plain")],
                    body: "test body",
                },
            ))
        );
        assert_eq!(
            HTTPRequest::parse(
                "POST example.com\r\nContent-Type: text/plain\r\n\r\ntest body\r\nEOF",
                "EOF"
            ),
            Ok((
                "",
                HTTPRequest {
                    method: "POST",
                    version: Protocol::HTTP1_1,
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: vec![("Content-Type", "text/plain")],
                    body: "test body",
                },
            ))
        );
        assert_eq!(
            HTTPRequest::parse(
                "example.com\nContent-Type:text/plain\n\ntest body\nEOF",
                "EOF"
            ),
            Err(nom::Err::Error(nom::error::Error::new(
                ".com\nContent-Type:text/plain\n\ntest body\nEOF",
                nom::error::ErrorKind::Space,
            )))
        );
    }
}
