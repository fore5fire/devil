use nom::character::complete::not_line_ending;
use nom::character::streaming::line_ending;
use nom::sequence::{separated_pair, terminated};
use nom::{branch::alt, character::complete::space1, sequence::Tuple, IResult};
use url::Url;

use super::http::HTTPRequest;
use super::tcp::TCPRequest;
use super::util::ident;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Protocol {
    HTTP,
    HTTP0_9,
    HTTP1_0,
    HTTP1_1,
    TCP,
}

impl Protocol {
    pub fn parse(input: &str) -> Option<Protocol> {
        match input {
            "http" => Some(Protocol::HTTP),
            "http/0.9" => Some(Protocol::HTTP0_9),
            "http/1.0" => Some(Protocol::HTTP1_0),
            "http/1.1" => Some(Protocol::HTTP1_1),
            "tcp" => Some(Protocol::TCP),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StepBody<'a> {
    HTTP(HTTPRequest<'a>),
    TCP(TCPRequest<'a>),
    //GraphQL(GraphQLRequest, GraphQLResponse, HTTPRequest, HTTPResponse),
}

#[derive(Debug, PartialEq)]
pub struct Step<'a> {
    pub name: Option<&'a str>,
    pub url: Url,
    pub body: StepBody<'a>,
}

impl<'a> Step<'a> {
    pub fn parse(input: &'a str) -> IResult<&str, Self> {
        println!("2");
        alt((Self::named, Self::unnamed))(input)
    }

    fn named(input: &'a str) -> IResult<&str, Step> {
        let (input, (kind, _, name, _, eof)) =
            (ident, space1, ident, space1, not_line_ending).parse(input)?;
        let Some(protocol) = Protocol::parse(kind) else {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        };
        let (input, body) = Self::body(input, protocol, eof)?;
        Ok((
            input,
            Self {
                name: Some(name),
                protocol,
                body,
            },
        ))
    }

    fn unnamed(input: &'a str) -> IResult<&str, Step> {
        let (input, (kind, eof)) =
            terminated(separated_pair(ident, space1, not_line_ending), line_ending)(input)?;
        let Some(protocol) = Protocol::parse(kind) else {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        };
        let (input, body) = Self::body(input, protocol, eof)?;
        Ok((
            input,
            Self {
                name: None,
                protocol,
                body,
            },
        ))
    }

    fn body(input: &'a str, kind: Protocol, eof: &str) -> IResult<&'a str, StepBody<'a>> {
        match kind {
            Protocol::HTTP | Protocol::HTTP0_9 | Protocol::HTTP1_0 | Protocol::HTTP1_1 => {
                let (input, req) = HTTPRequest::parse(input, eof)?;
                Ok((input, StepBody::HTTP(req)))
            }
            Protocol::TCP => {
                let (input, req) = TCPRequest::parse(input, eof)?;
                Ok((input, StepBody::TCP(req)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use HTTPRequest;
    use Protocol;

    #[test]
    fn step_test() {
        assert_eq!(
            Step::parse("http EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"),
            Ok((
                "",
                Step {
                    name: None,
                    protocol: Protocol::HTTP,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
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
                    protocol: Protocol::HTTP,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
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
                    protocol: Protocol::HTTP,
                    body: StepBody::HTTP(HTTPRequest {
                        method: "POST",
                        endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                        headers: Vec::new(),
                        body: "body",
                    })
                }
            ))
        );
    }
}
