use nom::combinator::all_consuming;
use nom::{character::complete::multispace0, multi::many0, sequence::terminated, IResult};

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
        let (input, _) = multispace0(input)?;

        let (input, steps) = many0(terminated(Step::parse, multispace0))(input)?;
        Ok((input, Plan { steps }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HTTPRequest, Protocol, Step, StepBody};

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
        assert_eq!(
            Plan::parse_partial(
                "http EOF\nPOST example.com\nContent-Type: text/plain\n\ntest body\nEOF"
            )
            .unwrap()
            .1
            .steps[0],
            Step {
                name: None,
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    version: Protocol::HTTP1_1,
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: vec![("Content-Type", "text/plain")],
                    body: "test body",
                }),
            },
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
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    version: Protocol::HTTP1_1,
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
                body: StepBody::HTTP(HTTPRequest {
                    method: "POST",
                    version: Protocol::HTTP1_1,
                    endpoint: "example.com".parse::<hyper::Uri>().unwrap(),
                    headers: Vec::new(),
                    body: "body",
                }),
            },
        );
    }
}
