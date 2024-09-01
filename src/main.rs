use clap::{Parser, ValueEnum};
use devil::exec::Executor;
use devil::{Plan, StepOutput};

// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Print more details.
    #[arg(short, long)]
    debug: bool,

    /// Print responses at a lower level protocol.
    #[arg(short, long, value_enum, value_delimiter = ',')]
    level: Vec<Protocol>,

    /// Print requests at a lower level protocol.
    #[arg(short = 'L', long, value_enum)]
    request_level: Vec<Protocol>,

    /// The path to the query plan.
    #[arg(value_name = "FILE")]
    file: String,

    /// Compile a query plan but don't execute it.
    #[arg(long)]
    dry_run: bool,
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "lower")]
enum Protocol {
    GraphQL,
    HTTP,
    TLS,
    TCP,
    TCPSegments,
    UDP,
    QUIC,
    IP,
    NONE,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let buffer = std::fs::read(&args.file)?;
    let text = String::from_utf8(buffer)?;
    {
        let plan = Plan::parse(&text)?;
        if args.debug {
            println!("query plan: {:#?}", plan);
        }
        if args.dry_run {
            return Ok(());
        }

        let mut executor = Executor::new(&plan)?;
        for (name, _) in plan.steps.iter() {
            println!("------- executing {name} --------");
            let output = executor.next().await?;
            for (key, out) in output.iter() {
                println!("---- step {key} ----");
                print_proto(&args, &out);
            }
        }
    }
    Ok(())
}

fn print_proto(args: &Args, proto: &StepOutput) {
    // TODO: escape or refuse to print dangerous term characters in output.
    for level in &args.request_level {
        match level {
            Protocol::TCPSegments => {
                if let Some(segments) = &proto.raw_tcp {
                    for seg in &segments.sent {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&seg.payload).replace("\n", "\n> ")
                        );
                    }
                }
                if let Some(tcp) = &proto.tcp {
                    if let Some(sent) = &tcp.sent {
                        for seg in &sent.segments {
                            println!(
                                "> {}",
                                String::from_utf8_lossy(&seg.payload).replace("\n", "\n> ")
                            );
                        }
                        if let Some(ttfb) = &sent.time_to_first_byte {
                            println!("sent time to first byte: {}", ttfb);
                        }
                    }
                }
            }
            Protocol::TCP => {
                if let Some(tcp) = &proto.tcp {
                    if let Some(req) = &tcp.sent {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("sent time to first byte: {}", ttfb);
                        }
                    }
                }
            }
            Protocol::TLS => {
                if let Some(tls) = &proto.tls {
                    if let Some(req) = &tls.request {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("request time to first byte: {}", ttfb);
                        }
                    }
                }
            }
            Protocol::HTTP => {
                if let Some(http) = &proto.http {
                    println!(
                        "> {}{} {}",
                        http.request
                            .as_ref()
                            .map(|req| req.method.as_ref())
                            .flatten()
                            .as_deref()
                            .map(|x| String::from_utf8_lossy(x) + " ")
                            .unwrap_or_default(),
                        http.request
                            .as_ref()
                            .map(|req| req.url.to_string())
                            .unwrap_or_else(|| "".to_owned()),
                        http.protocol
                            .as_ref()
                            .map(String::as_str)
                            .unwrap_or_default(),
                    );
                    if let Some(req) = &http.request {
                        for (k, v) in &req.headers {
                            println!(
                                ">   {}: {}",
                                String::from_utf8_lossy(k),
                                String::from_utf8_lossy(v)
                            );
                        }
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("request time to first byte: {}", ttfb);
                        }
                        println!("request duration: {}", req.duration);
                    }
                }
                if let Some(http) = proto.http1() {
                    println!(
                        "> {}{}{}",
                        http.request
                            .as_ref()
                            .map(|req| req.method.as_ref())
                            .flatten()
                            .as_deref()
                            .map(|x| String::from_utf8_lossy(x) + " ")
                            .unwrap_or_default(),
                        http.request
                            .as_ref()
                            .map(|req| req.url.to_string())
                            .unwrap_or_else(|| "".to_owned()),
                        http.request
                            .as_ref()
                            .map(|req| req
                                .version_string
                                .as_ref()
                                .map(|v| " ".to_owned() + &String::from_utf8_lossy(v))
                                .unwrap_or_default())
                            .unwrap_or_default(),
                    );
                    if let Some(req) = &http.request {
                        for (k, v) in &req.headers {
                            println!(
                                ">   {}: {}",
                                String::from_utf8_lossy(k),
                                String::from_utf8_lossy(v)
                            );
                        }
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("request time to first byte: {}", ttfb);
                        }
                        println!("request duration: {}", req.duration);
                    }
                }
                if let Some(http) = proto.http2() {
                    println!(
                        "> {}{} HTTP/2",
                        http.request
                            .as_ref()
                            .map(|req| req.method.as_ref())
                            .flatten()
                            .as_deref()
                            .map(|x| String::from_utf8_lossy(x) + " ")
                            .unwrap_or_default(),
                        http.request
                            .as_ref()
                            .map(|req| req.url.to_string())
                            .unwrap_or_else(|| "".to_owned()),
                    );
                    if let Some(req) = &http.request {
                        for (k, v) in &req.headers {
                            println!(
                                ">   {}: {}",
                                String::from_utf8_lossy(k),
                                String::from_utf8_lossy(v)
                            );
                        }
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("request time to first byte: {}", ttfb);
                        }
                        println!("request duration: {}", req.duration);
                    }
                }
            }
            Protocol::GraphQL => {
                if let Some(req) = proto
                    .graphql
                    .as_ref()
                    .map(|tcp| tcp.request.as_ref())
                    .flatten()
                {
                    println!("> {}", &req.query.replace("\n", "\n> "));
                    println!("request duration: {}", req.duration);
                }
            }
            _ => {}
        }
    }

    let mut out_level = args.level.as_slice();
    if out_level.is_empty() {
        out_level = if proto.graphql.is_some() {
            &[Protocol::GraphQL]
        //} else if proto.http3.is_some() {
        //    vec![Protocol::HTTP]
        } else if proto.http2().is_some() {
            &[Protocol::HTTP]
        } else if proto.http1().is_some() {
            &[Protocol::HTTP]
        } else if proto.http.is_some() {
            &[Protocol::HTTP]
        } else if proto.tls.is_some() {
            &[Protocol::TLS]
        } else if proto.tcp.is_some() {
            &[Protocol::TCP]
        } else if proto.raw_tcp.is_some() {
            &[Protocol::TCPSegments]
        } else {
            &[]
        }
    };

    for level in out_level {
        // Default output is at the highest level protocol in the request.
        match level {
            Protocol::TCPSegments => {
                if let Some(raw) = &proto.raw_tcp {
                    for segment in &raw.received {
                        println!("< {segment:?}");
                    }
                    for e in &raw.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &raw.pause.handshake.start {
                        println!("handshake start pause duration: {}", p.duration);
                    }
                    for p in &raw.pause.handshake.end {
                        println!("handshake end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", raw.duration);
                }
                if let Some(tcp) = &proto.tcp {
                    if let Some(received) = &tcp.received {
                        for segment in &received.segments {
                            println!("< {segment:?}");
                        }
                    }
                    for e in &tcp.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &tcp.pause.handshake.start {
                        println!("handshake start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.handshake.end {
                        println!("handshake end pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.send_body.start {
                        println!("send body start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.send_body.end {
                        println!("send body end pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.receive_body.start {
                        println!("receive body start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.receive_body.end {
                        println!("receive body end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", tcp.duration);
                }
            }
            Protocol::TCP => {
                if let Some(tcp) = &proto.tcp {
                    if let Some(resp) = &tcp.received {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                        );
                        if let Some(ttfb) = &resp.time_to_first_byte {
                            println!("response time to first byte: {}", ttfb);
                        }
                    }
                    for e in &tcp.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &tcp.pause.handshake.start {
                        println!("handshake start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.handshake.end {
                        println!("handshake end pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.send_body.start {
                        println!("send body start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.send_body.end {
                        println!("send body end pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.receive_body.start {
                        println!("receive body start pause duration: {}", p.duration);
                    }
                    for p in &tcp.pause.receive_body.end {
                        println!("receive body end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", tcp.duration);
                }
            }
            Protocol::TLS => {
                if let Some(tls) = &proto.tls {
                    if let Some(resp) = &tls.response {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                        );
                        if let Some(ttfb) = &resp.time_to_first_byte {
                            println!("response time to first byte: {}", ttfb);
                        }
                    }
                    for e in &tls.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &tls.pause.handshake.start {
                        println!("handshake start pause duration: {}", p.duration);
                    }
                    for p in &tls.pause.handshake.end {
                        println!("handshake end pause duration: {}", p.duration);
                    }
                    for p in &tls.pause.send_body.start {
                        println!("send body start pause duration: {}", p.duration);
                    }
                    for p in &tls.pause.send_body.end {
                        println!("send body end pause duration: {}", p.duration);
                    }
                    for p in &tls.pause.receive_body.start {
                        println!("receive body start pause duration: {}", p.duration);
                    }
                    for p in &tls.pause.receive_body.end {
                        println!("receive body end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", tls.duration);
                }
            }
            Protocol::HTTP => {
                if let Some(http) = &proto.http {
                    if let Some(resp) = &http.response {
                        println!(
                            "< {} {}",
                            http.response
                                .as_ref()
                                .map(|resp| resp.status_code)
                                .flatten()
                                .unwrap_or(0),
                            http.protocol
                                .as_ref()
                                .map(String::as_str)
                                .unwrap_or_default()
                        );
                        if let Some(headers) = &resp.headers {
                            for (k, v) in headers {
                                println!(
                                    "< {}: {}",
                                    String::from_utf8_lossy(&k),
                                    String::from_utf8_lossy(&v)
                                );
                            }
                        }
                        println!("< ");
                        if let Some(body) = &resp.body {
                            println!("< {}", String::from_utf8_lossy(&body).replace("\n", "\n< "));
                        }
                        if let Some(ttfb) = &resp.time_to_first_byte {
                            println!("response time to first byte: {}", ttfb);
                        }
                        println!("response duration: {}", resp.duration);
                    }
                    for e in &http.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &http.pause.request_headers.start {
                        println!("request headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_headers.end {
                        println!("request headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.start {
                        println!("request body start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.end {
                        println!("request body end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", http.duration);
                }
                if let Some(http) = proto.http1() {
                    if let Some(resp) = &http.response {
                        println!(
                            "< {} {}",
                            resp.status_code.unwrap_or(0),
                            resp.protocol
                                .as_ref()
                                .map(|x| String::from_utf8_lossy(x))
                                .unwrap_or_default()
                        );
                        if let Some(headers) = &resp.headers {
                            for (k, v) in headers {
                                println!(
                                    "< {}: {}",
                                    String::from_utf8_lossy(&k),
                                    String::from_utf8_lossy(&v)
                                );
                            }
                        }
                        println!("< ");
                        if let Some(body) = &resp.body {
                            println!("< {}", String::from_utf8_lossy(&body).replace("\n", "\n< "));
                        }
                        if let Some(ttfb) = &resp.time_to_first_byte {
                            println!("response time to first byte: {}", ttfb);
                        }
                        println!("response duration: {}", resp.duration);
                    }
                    for e in &http.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &http.pause.request_headers.start {
                        println!("request headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_headers.end {
                        println!("request headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.start {
                        println!("request body start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.end {
                        println!("request body end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", http.duration);
                }
                if let Some(http) = proto.http2() {
                    if let Some(resp) = &http.response {
                        println!("< {} HTTP/2", resp.status_code.unwrap_or(0),);
                        if let Some(headers) = &resp.headers {
                            for (k, v) in headers {
                                println!(
                                    "< {}: {}",
                                    String::from_utf8_lossy(&k),
                                    String::from_utf8_lossy(&v)
                                );
                            }
                        }
                        println!("< ");
                        if let Some(body) = &resp.body {
                            println!("< {}", String::from_utf8_lossy(&body).replace("\n", "\n< "));
                        }
                        if let Some(ttfb) = &resp.time_to_first_byte {
                            println!("response time to first byte: {}", ttfb);
                        }
                        println!("response duration: {}", resp.duration);
                    }
                    for e in &http.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    for p in &http.pause.request_headers.start {
                        println!("request headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_headers.end {
                        println!("request headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.start {
                        println!("request body start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.request_body.end {
                        println!("request body end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_headers.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.start {
                        println!("response headers start pause duration: {}", p.duration);
                    }
                    for p in &http.pause.response_body.end {
                        println!("response headers end pause duration: {}", p.duration);
                    }
                    println!("total duration: {}", http.duration);
                }
            }
            Protocol::GraphQL => {
                if let Some(gql) = &proto.graphql {
                    if let Some(resp) = &gql.response {
                        println!(
                            "< {}",
                            serde_json::to_string_pretty(&resp.json)
                                .unwrap()
                                .replace("\n", "\n< ")
                        );
                        println!("response duration: {}", resp.duration);
                    }
                    for e in &gql.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("total duration: {}", gql.duration);
                }
            }
            _ => {}
        }
    }
}
