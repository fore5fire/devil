use nom::character::complete::not_line_ending;
use nom::character::streaming::line_ending;
use nom::sequence::{separated_pair, terminated};
use nom::{branch::alt, character::complete::space1, error::ErrorKind, sequence::Tuple, IResult};

use super::util::ident;
use super::HTTPRequest;

#[derive(Debug, PartialEq)]
pub enum StepBody<'a> {
    HTTP(HTTPRequest<'a>),
    //GraphQL(GraphQLRequest, GraphQLResponse, HTTPRequest, HTTPResponse),
}

#[derive(Debug, PartialEq)]
pub struct Step<'a> {
    pub name: Option<&'a str>,
    pub body: StepBody<'a>,
}

impl<'a> Step<'a> {
    pub fn parse(input: &'a str) -> IResult<&str, Self> {
        alt((Self::named, Self::unnamed))(input)
    }

    fn named(input: &'a str) -> IResult<&str, Step> {
        let (input, (kind, _, name, _, eof)) =
            (ident, space1, ident, space1, not_line_ending).parse(input)?;
        let (input, body) = Self::body(input, kind, eof)?;
        Ok((
            input,
            Self {
                name: Some(name),
                body,
            },
        ))
    }

    fn unnamed(input: &'a str) -> IResult<&str, Step> {
        let (input, (kind, eof)) =
            terminated(separated_pair(ident, space1, not_line_ending), line_ending)(input)?;
        let (input, body) = Self::body(input, kind, eof)?;
        Ok((input, Self { name: None, body }))
    }

    fn body(input: &'a str, kind: &str, eof: &str) -> IResult<&'a str, StepBody<'a>> {
        match kind {
            "http" => {
                let (input, req) = HTTPRequest::parse(input, eof)?;
                Ok((input, StepBody::HTTP(req)))
            }
            _ => Err(nom::Err::Error(nom::error::Error {
                input,
                code: ErrorKind::Switch,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HTTPRequest;
    use crate::Protocol;

    #[test]
    fn step_test() {
        assert_eq!(
            Step::parse("http EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"),
            Ok((
                "",
                Step {
                    name: None,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
                        version: Protocol::HTTP1_1,
                        endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                        headers: vec![("Content-Type", "text/plain")],
                        body: "test body",
                    })
                }
            ))
        );
        assert_eq!(
            Step::parse("http EOF\nexample.com\nContent-Type:text/plain\n\ntest body\nEOF"),
            Err(nom::Err::Error(nom::error::Error::new(
                ".com\nContent-Type:text/plain\n\ntest body\nEOF",
                nom::error::ErrorKind::Space,
            )))
        );
        assert_eq!(
            Step::parse("http EOF\nPOST example.com\n\ntest body\nEOF"),
            Ok((
                "",
                Step {
                    name: None,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
                        version: Protocol::HTTP1_1,
                        endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                        headers: Vec::new(),
                        body: "test body",
                    })
                }
            ))
        );
        assert_eq!(
            Step::parse("http EOF\nPOST example.com\n\nbody\nEOF"),
            Ok((
                "",
                Step {
                    name: None,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
                        version: Protocol::HTTP1_1,
                        endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                        headers: Vec::new(),
                        body: "body",
                    })
                }
            ))
        );
    }
}
