use clap::{Parser, ValueEnum};
use courier_qe::exec::Executor;
use courier_qe::{Plan, StepOutput};

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
    UDP,
    QUIC,
    IP,
    NONE,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
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

        let mut executor = Executor::new(&plan);
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
            Protocol::TCP => {
                if let Some(req) = proto.tcp.as_ref().map(|tcp| tcp.request.as_ref()).flatten() {
                    println!(
                        "> {}",
                        String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                    );
                }
            }
            Protocol::TLS => {
                if let Some(req) = proto.tls.as_ref().map(|tls| tls.request.as_ref()).flatten() {
                    println!(
                        "> {}",
                        String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                    );
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
                    }
                }
                if let Some(http) = &proto.http1 {
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
        //} else if proto.http2.is_some() {
        //    vec![Protocol::HTTP]
        } else if proto.http1.is_some() {
            &[Protocol::HTTP]
        } else if proto.http.is_some() {
            &[Protocol::HTTP]
        } else if proto.tls.is_some() {
            &[Protocol::TLS]
        } else if proto.tcp.is_some() {
            &[Protocol::TCP]
        } else {
            &[]
        }
    };

    for level in out_level {
        // Default output is at the highest level protocol in the request.
        match level {
            Protocol::TCP => {
                if let Some(tcp) = &proto.tcp {
                    if let Some(resp) = &tcp.response {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                        );
                    }
                    if let Some(e) = &tcp.error {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("duration: {}ms", tcp.duration.num_milliseconds());
                }
            }
            Protocol::TLS => {
                if let Some(tls) = &proto.tls {
                    if let Some(resp) = &tls.response {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                        );
                    }
                    if let Some(e) = &tls.error {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("duration: {}ms", tls.duration.num_milliseconds());
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
                    }
                    if let Some(e) = &http.error {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("duration: {}ms", http.duration.num_milliseconds());
                }
                if let Some(http) = &proto.http1 {
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
                    }
                    if let Some(e) = &http.error {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("duration: {}ms", http.duration.num_milliseconds());
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
                    }
                    if let Some(e) = &gql.error {
                        println!("{} error: {}", e.kind, e.message);
                    }
                    println!("duration: {}ms", gql.duration.num_milliseconds());
                }
            }
            _ => {}
        }
    }
}
