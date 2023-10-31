use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_until},
    character::complete::{alpha1, line_ending, not_line_ending, space0, space1},
    multi::many_till,
    sequence::{pair, separated_pair, terminated},
    IResult,
};

#[derive(Debug, PartialEq)]
pub struct TCPRequest<'a> {
    pub body: &'a str,
}

impl<'a> TCPRequest<'a> {
    pub fn parse(input: &'a str, eof: &str) -> IResult<&'a str, Self> {
        // Read the body, allowing either line ending before the eof token.
        let eof = format!("\r\n{}", eof);
        let (input, body) = alt((
            terminated(take_until(eof.as_str()), tag(eof.as_str())),
            terminated(take_until(&eof[1..]), tag(&eof[1..])),
        ))(input)?;

        Ok((input, TCPRequest { body }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_test() {
        assert_eq!(
            TCPRequest::parse(
                "POST example.com\nContent-Type: text/plain\n\ntest body\nEOF",
                "EOF",
            ),
            Ok((
                "",
                TCPRequest {
                    address: "example.com",
                    body: "test body",
                },
            ))
        );
        assert_eq!(
            TCPRequest::parse(
                "POST example.com\r\nContent-Type: text/plain\r\n\r\ntest body\r\nEOF",
                "EOF"
            ),
            Ok((
                "",
                TCPRequest {
                    address: "example.com",
                    body: "test body",
                },
            ))
        );
        assert_eq!(
            TCPRequest::parse(
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
