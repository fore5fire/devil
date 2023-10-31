use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::newline,
    character::complete::{multispace1, not_line_ending, space0},
    combinator::all_consuming,
    multi::many0,
    sequence::{terminated, tuple},
    IResult,
};

use super::Step;

#[derive(Debug)]
pub struct Plan<'a> {
    pub steps: Vec<Step<'a>>,
}

impl<'a> Plan<'a> {
    pub fn parse(input: &'a str) -> Result<Self, String> {
        let (_, result) = all_consuming(Self::parse_partial)(input).map_err(|e| e.to_string())?;
        Ok(result)
    }

    pub fn parse_partial(input: &'a str) -> IResult<&str, Self> {
        // Step over whitespace before the first step.
        let (input, _) = many0(Self::comment_or_whitespace)(input)?;
        println!("1");
        let (input, steps) =
            many0(terminated(Step::parse, many0(Self::comment_or_whitespace)))(input)?;
        Ok((input, Plan { steps }))
    }

    fn comment_or_whitespace(input: &'a str) -> IResult<&str, &str> {
        alt((multispace1, Self::comment))(input)
    }

    fn comment(input: &'a str) -> IResult<&str, &str> {
        let (input, (_, _, content, _)) =
            tuple((tag("#"), space0, not_line_ending, newline))(input)?;
        Ok((input, content))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http::HTTPRequest, tcp::TCPRequest, Protocol, Step, StepBody};

    #[test]
    fn plan_test() {
        assert_eq!(
            Plan::parse_partial(
                "http EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"
            )
            .unwrap()
            .0,
            "",
        );
        // Test comment.
        assert_eq!(
            Plan::parse_partial(
                "\n# test\n\n\nhttp EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"
            )
            .unwrap().1.steps,
            vec![Step {
                name: None,
                protocol: Protocol::HTTP,
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: vec![("Content-Type", "text/plain")],
                    body: "test body",
                }),
            }],
        );
        assert_eq!(
            Plan::parse_partial(
                "http EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"
            )
            .unwrap()
            .1
            .steps,
            vec![Step {
                name: None,
                protocol: Protocol::HTTP,
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: vec![("Content-Type", "text/plain")],
                    body: "test body",
                }),
            }],
        );
        assert_eq!(
            Plan::parse("http EOF\nPOSt example.com\nContent-Type:text/plain\n\ntest body\nEOFa")
                .unwrap_err(),
            "Parsing Error: Error { input: \"a\", code: Eof }"
        );
        assert_eq!(
            Plan::parse("http EOF\nPOST example.com\n\ntest body\nEOF")
                .unwrap()
                .steps[0],
            Step {
                name: None,
                protocol: Protocol::HTTP,
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: Vec::new(),
                    body: "test body",
                }),
            },
        );
        assert_eq!(
            Plan::parse("http EOF\nPOST example.com\n\nbody\nEOF")
                .unwrap()
                .steps[0],
            Step {
                name: None,
                protocol: Protocol::HTTP,
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: Vec::new(),
                    body: "body",
                }),
            },
        );
        assert_eq!(
            Plan::parse("tcp EOF\nPOST /\n\nbody\nEOF").unwrap().steps[0],
            Step {
                name: None,
                protocol: Protocol::TCP,
                body: StepBody::TCP(TCPRequest {
                    address: "example.com",
                    body: "POST / \n\nbody",
                }),
            },
        );
    }
}
