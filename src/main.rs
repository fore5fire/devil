use anyhow::anyhow;
use cel_interpreter::to_value;
use clap::{Parser, ValueEnum};
use devil::exec::Executor;
use devil::{Http2FrameOutput, Plan, PlanOutput, StepOutput};
use tracing_subscriber::EnvFilter;

// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Print more details.
    #[arg(short, long)]
    debug: bool,

    /// Print requests and responses in the specified format.
    #[arg(short, long, value_enum, default_value_t)]
    format: OutputFormat,

    /// Print responses at a lower level protocol.
    #[arg(short, long, value_enum, value_delimiter = ',')]
    level: Option<Vec<Protocol>>,

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
    Http,
    Http2Frames,
    TLS,
    TCP,
    TCPSegments,
    Udp,
    Quic,
    Ip,
    None,
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq, Default)]
#[clap(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Level,
    Toml,
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let buffer = std::fs::read(&args.file)?;
    let text = String::from_utf8(buffer)?;
    let plan = Plan::parse(&text)?;
    if args.debug {
        println!("query plan: {:#?}", plan);
    }
    if args.dry_run {
        return Ok(());
    }

    let mut executor = Executor::new(&plan)?;
    match args.format {
        OutputFormat::Level => {
            for (name, _) in plan.steps.iter() {
                println!("------- executing {name} --------");
                let output = executor.next().await?;
                for (key, out) in output.iter() {
                    println!("---- step {key} ----");
                    print_proto(&args, &out);
                }
            }
        }
        OutputFormat::Toml => {
            let mut out = PlanOutput::default();
            for (name, _) in plan.steps.iter() {
                out.steps.insert(name.clone(), executor.next().await?);
            }
            // Convert to cel to fix Durations and Timestamps.
            let value = to_value(out)?;
            let json = value
                .json()
                .map_err(|e| anyhow!("convert output to json: {}", e.to_string()))?;
            println!("{}", toml::ser::to_string_pretty(&json)?);
        }
        OutputFormat::Json => {
            let mut out = PlanOutput::default();
            for (name, _) in plan.steps.iter() {
                out.steps.insert(name.clone(), executor.next().await?);
            }
            // Convert to cel to fix Durations and Timestamps.
            print!(
                "{}",
                to_value(out)?
                    .json()
                    .map_err(|e| anyhow!("convert output to json: {}", e.to_string()))?
            );
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
            }
            Protocol::TCP => {
                if let Some(tcp) = &proto.tcp {
                    if let Some(req) = &tcp.sent {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                        );
                        if let Some(ttfb) = &req.time_to_first_byte {
                            println!("sent time to first byte: {}", ttfb.0);
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
                            println!("request time to first byte: {}", ttfb.0);
                        }
                    }
                }
            }
            Protocol::Http2Frames => {
                if let Some(http) = proto.raw_http2() {
                    for frame in &http.sent {
                        print_frame('>', frame);
                    }
                }
            }
            Protocol::Http => {
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
                            println!("request time to first byte: {}", ttfb.0);
                        }
                        println!("request duration: {}", req.duration.0);
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
                            println!("request time to first byte: {}", ttfb.0);
                        }
                        println!("request duration: {}", req.duration.0);
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
                            println!("request time to first byte: {}", ttfb.0);
                        }
                        println!("request duration: {}", req.duration.0);
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
                    println!("request duration: {}", req.duration.0);
                }
            }
            _ => {}
        }
    }

    let out_level = args
        .level
        .as_ref()
        .map(|level| level.as_slice())
        .unwrap_or_else(|| {
            if proto.graphql.is_some() {
                &[Protocol::GraphQL]
            //} else if proto.http3.is_some() {
            //    vec![Protocol::HTTP]
            } else if proto.http2().is_some() {
                &[Protocol::Http]
            } else if proto.http1().is_some() {
                &[Protocol::Http]
            } else if proto.http.is_some() {
                &[Protocol::Http]
            } else if proto.raw_http2().is_some() {
                &[Protocol::Http2Frames]
            } else if proto.tls.is_some() {
                &[Protocol::TLS]
            } else if proto.tcp.is_some() {
                &[Protocol::TCP]
            } else if proto.raw_tcp.is_some() {
                &[Protocol::TCPSegments]
            } else {
                &[]
            }
        });

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
                    //for p in &raw.pause.handshake.start {
                    //    println!("handshake start pause duration: {}", p.duration);
                    //}
                    //for p in &raw.pause.handshake.end {
                    //    println!("handshake end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", raw.duration.0);
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
                            println!("response time to first byte: {}", ttfb.0);
                        }
                    }
                    for e in &tcp.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    //for p in &tcp.pause.handshake.start {
                    //    println!("handshake start pause duration: {}", p.duration);
                    //}
                    //for p in &tcp.pause.handshake.end {
                    //    println!("handshake end pause duration: {}", p.duration);
                    //}
                    //for p in &tcp.pause.send_body.start {
                    //    println!("send body start pause duration: {}", p.duration);
                    //}
                    //for p in &tcp.pause.send_body.end {
                    //    println!("send body end pause duration: {}", p.duration);
                    //}
                    //for p in &tcp.pause.receive_body.start {
                    //    println!("receive body start pause duration: {}", p.duration);
                    //}
                    //for p in &tcp.pause.receive_body.end {
                    //    println!("receive body end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", tcp.duration.0);
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
                            println!("response time to first byte: {}", ttfb.0);
                        }
                    }
                    for e in &tls.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    //for p in &tls.pause.handshake.start {
                    //    println!("handshake start pause duration: {}", p.duration);
                    //}
                    //for p in &tls.pause.handshake.end {
                    //    println!("handshake end pause duration: {}", p.duration);
                    //}
                    //for p in &tls.pause.send_body.start {
                    //    println!("send body start pause duration: {}", p.duration);
                    //}
                    //for p in &tls.pause.send_body.end {
                    //    println!("send body end pause duration: {}", p.duration);
                    //}
                    //for p in &tls.pause.receive_body.start {
                    //    println!("receive body start pause duration: {}", p.duration);
                    //}
                    //for p in &tls.pause.receive_body.end {
                    //    println!("receive body end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", tls.duration.0);
                }
            }
            Protocol::Http2Frames => {
                if let Some(http) = proto.raw_http2() {
                    for frame in &http.received {
                        print_frame('<', frame);
                    }
                }
            }
            Protocol::Http => {
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
                            println!("response time to first byte: {}", ttfb.0);
                        }
                        println!("response duration: {}", resp.duration.0);
                    }
                    for e in &http.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    //for p in &http.pause.request_headers.start {
                    //    println!("request headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_headers.end {
                    //    println!("request headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.start {
                    //    println!("request body start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.end {
                    //    println!("request body end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", http.duration.0);
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
                            println!("response time to first byte: {}", ttfb.0);
                        }
                        println!("response duration: {}", resp.duration.0);
                    }
                    //for e in &http.errors {
                    //    println!("{} error: {}", e.kind, e.message);
                    //}
                    //for p in &http.pause.request_headers.start {
                    //    println!("request headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_headers.end {
                    //    println!("request headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.start {
                    //    println!("request body start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.end {
                    //    println!("request body end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", http.duration.0);
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
                            println!("response time to first byte: {}", ttfb.0);
                        }
                        println!("response duration: {}", resp.duration.0);
                    }
                    for e in &http.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    //for p in &http.pause.request_headers.start {
                    //    println!("request headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_headers.end {
                    //    println!("request headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.start {
                    //    println!("request body start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.request_body.end {
                    //    println!("request body end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_headers.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.start {
                    //    println!("response headers start pause duration: {}", p.duration);
                    //}
                    //for p in &http.pause.response_body.end {
                    //    println!("response headers end pause duration: {}", p.duration);
                    //}
                    println!("total duration: {}", http.duration.0);
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
                        println!("response duration: {}", resp.duration.0);
                    }
                    for e in &gql.errors {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("total duration: {}", gql.duration.0);
                }
            }
            _ => {}
        }
    }
}

fn print_frame(direction: char, frame: &Http2FrameOutput) {
    let d = direction;
    println!("{d} type: {:}", frame.r#type());
    println!("{d} flags: {:#010b}", frame.flags().bits());
    match frame {
        Http2FrameOutput::Data(frame) => {
            println!("{d}   END_STREAM: {}", frame.end_stream);
            println!("{d}   PADDING: {}", frame.padding.is_some());
        }
        Http2FrameOutput::Headers(frame) => {
            println!("{d}   END_STREAM: {}", frame.end_stream);
            println!("{d}   END_HEADERS: {}", frame.end_headers);
            println!("{d}   PADDING: {}", frame.padding.is_some());
            println!("{d}   PRIORITY: {}", frame.priority.is_some());
        }
        Http2FrameOutput::Settings(frame) => {
            println!("{d}   ACK: {}", frame.ack);
        }
        Http2FrameOutput::PushPromise(frame) => {
            println!("{d}   END_HEADERS: {}", frame.end_headers());
            println!("{d}   PADDING: {}", frame.padding.is_some());
        }
        Http2FrameOutput::Ping(frame) => {
            println!("{d}   ACK: {}", frame.ack);
        }
        Http2FrameOutput::Continuation(frame) => {
            println!("{d}   END_HEADERS: {}", frame.end_headers);
        }
        _ => {}
    }
    let r = frame.r();
    if r {
        println!("{d} r: 1");
    }
    println!("{d} stream_id: {}", frame.stream_id());
    match frame {
        Http2FrameOutput::Data(frame) => {
            println!("{d} data: {:?}", frame.data);
            if frame
                .padding
                .as_ref()
                .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
            {
                println!("{d} padding: {:?}", frame.padding);
            }
        }
        Http2FrameOutput::Headers(frame) => {
            if let Some(priority) = &frame.priority {
                println!("{d} exclusive: {}", priority.e);
                println!("{d} stream dependency: {}", priority.stream_dependency);
                println!("{d} weight: {}", priority.weight);
            }
            println!(
                "{d} header block fragment: {:?}",
                frame.header_block_fragment
            );
            if frame
                .padding
                .as_ref()
                .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
            {
                println!("{d} padding: {:?}", frame.padding);
            }
        }
        Http2FrameOutput::Settings(frame) => {
            println!("{d} parameters:");
            for param in &frame.parameters {
                println!("{d}   ID: {}", param.id);
                println!("{d}   value: {:#06x}", param.value);
            }
        }
        Http2FrameOutput::PushPromise(frame) => {
            if frame.promised_r {
                println!("{d} promised stream ID r bit: 1");
            }
            println!("{d} promised stream ID: {}", frame.promised_stream_id);
            println!(
                "{d} header block fragment: {:?}",
                frame.header_block_fragment
            );
            if frame
                .padding
                .as_ref()
                .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
            {
                println!("{d} padding: {:?}", frame.padding);
            }
        }
        Http2FrameOutput::Ping(frame) => {
            println!("{d} data: {:?}", frame.data);
        }
        Http2FrameOutput::Continuation(frame) => {
            println!(
                "{d} header block fragment: {:?}",
                frame.header_block_fragment
            );
        }
        _ => {}
    }
}
