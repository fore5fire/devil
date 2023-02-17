use nom::{
    bytes::complete::{tag, take_until},
    character::{complete::space1, is_newline},
    combinator::map_res,
    sequence::Tuple,
    IResult, InputLength,
};

use super::util::ident;
use super::HTTPRequest;

#[derive(Debug)]
pub enum Step<'a> {
    HTTP(HTTPRequest<'a>),
    //GraphQL(GraphQLRequest, GraphQLResponse, HTTPRequest, HTTPResponse),
}

impl Step<'_> {
    pub fn parse(input: &str) -> IResult<&str, Step> {
        if let Ok((input, step)) = named_step(input) {
            return Ok((input, step));
        }
        unnamed_step(input)
    }
}

fn named_step(input: &str) -> IResult<&str, Step> {
    let (input, (kind, _, name, _, eof)) =
        (ident, space1, ident, space1, take_until("\n")).parse(input)?;
    step_body(input, kind, eof)
}

fn unnamed_step(input: &str) -> IResult<&str, Step> {
    let (input, (kind, _, eof)) = (ident, space1, take_until("\n")).parse(input)?;
    step_body(input, kind, eof)
}

fn step_body<'a>(input: &'a str, kind: &str, eof: &str) -> IResult<&'a str, Step<'a>> {
    Ok((
        input,
        match kind {
            "http" => {
                let (_, req) = HTTPRequest::parse(input, eof)?;
                Step::HTTP(req)
            }
        },
    ))
}
