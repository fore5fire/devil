use std::pin::Pin;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::Request;
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_until},
    character::complete::{alpha1, line_ending, not_line_ending, space0, space1},
    multi::many_till,
    sequence::{pair, separated_pair, terminated},
    IResult,
};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;

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

    pub async fn exec(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get the host and the port
        let host = self.endpoint.host().expect("uri has no host");
        let port = self.endpoint.port_u16().unwrap_or(80);

        let address = format!("{}:{}", host, port);

        // Open a TCP connection to the remote host
        let stream = TcpStream::connect(address).await?;
        let stream = Tee::new(stream);

        // Perform a TCP handshake
        let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await?;

        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                println!("Connection failed: {:?}", err);
            }
        });

        let authority = self
            .endpoint
            .authority()
            .ok_or("request missing host")?
            .clone();
        let default_headers = [
            (hyper::header::HOST, authority.as_str()),
            (hyper::header::USER_AGENT, "courier/0.1.0"),
        ];

        let mut req_builder = Request::builder()
            .method(self.method)
            .uri(self.endpoint.clone());

        for (k, v) in default_headers {
            if !self.contains_header(k.as_str()) {
                req_builder = req_builder.header(k, v);
            }
        }
        for (key, val) in self.headers.iter() {
            req_builder = req_builder.header(*key, *val)
        }
        let req = req_builder.body(self.body.to_owned())?;

        let mut res = sender.send_request(req).await?;

        // Stream the response body, writing each chunk to stdout as we get it
        // (instead of buffering and printing at the end).
        while let Some(next) = res.frame().await {
            let frame = next?;
            if let Some(chunk) = frame.data_ref() {
                io::stdout().write_all(&chunk).await?;
            }
        }

        Ok(())
    }

    fn contains_header(&self, key: &str) -> bool {
        self.headers
            .iter()
            .find(|(k, _)| key.eq_ignore_ascii_case(k))
            .is_some()
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

struct Tee<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    inner: T,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Tee<T> {
    pub fn new(wrap: T) -> Self {
        Tee { inner: wrap }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncRead for Tee<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let old_len = buf.filled().len();
        let poll = Pin::new(&mut self.get_mut().inner).poll_read(cx, buf);
        let str_data = String::from_utf8(buf.filled()[old_len..].to_vec()).unwrap_or_default();
        for line in str_data.lines() {
            println!("< {}", line)
        }
        poll
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncWrite for Tee<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = Pin::new(&mut self.get_mut().inner).poll_write(cx, buf);
        if poll.is_ready() {
            let str_data = String::from_utf8(buf.to_vec()).unwrap_or_default();
            for line in str_data.lines() {
                println!("> {}", line)
            }
        }
        poll
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
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
