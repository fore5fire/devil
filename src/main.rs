use clap::{Parser, ValueEnum};
use courier_ql::exec::Executor;
use courier_ql::{Plan, StepOutput};

// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Print more details.
    #[arg(short, long)]
    debug: bool,

    /// Print responses at a lower level protocol.
    #[arg(short, long, value_enum)]
    level: Option<Protocol>,

    /// Print requests at a lower level protocol.
    #[arg(short = 'L', long, value_enum, default_value_t = Protocol::GraphQL)]
    request_level: Protocol,

    /// Don't prettify output.
    #[arg(short, long)]
    raw: bool,

    /// The path to the query plan.
    #[arg(value_name = "FILE")]
    file: String,
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "lower")]
enum Protocol {
    None,
    GraphQL,
    HTTP,
    TLS,
    TCP,
    UDP,
    QUIC,
    IP,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    //let mut buffer = Vec::new();
    //let stdin = std::io::stdin();
    //let mut handle = stdin.lock();
    //let input = handle.read_to_end(&mut buffer)?;

    let args = Args::parse();
    let buffer = std::fs::read(args.file)?;
    let text = String::from_utf8(buffer)?;
    {
        let plan = Plan::parse(&text)?;
        let mut executor = Executor::new(&plan);
        for (name, _) in plan.steps.iter() {
            println!("------- executing {} --------", name);
            let output = executor.next().await?;
            match output {
                StepOutput::HTTP(http)
                | StepOutput::HTTP11(http)
                | StepOutput::HTTP2(http)
                | StepOutput::HTTP3(http) => {
                    if args.request_level == Protocol::TCP {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&http.raw_request).replace("\n", "\n> ")
                        );
                    } else if args.request_level == Protocol::HTTP {
                        println!("> {} {}", http.method, http.url);
                        for (k, v) in http.headers {
                            println!(">   {}: {}", k, v);
                        }
                        println!("> {}", &http.body);
                    }
                    if args.level == Some(Protocol::TCP) {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&http.raw_response).replace("\n", "\n< ")
                        );
                    }
                    if args.level == Some(Protocol::HTTP) || args.level == None {
                        println!("< {} {}", http.response.status_code, http.response.protocol);
                        for (k, v) in http.response.headers {
                            println!("<   {}: {}", k, v);
                        }
                        println!("< {}", String::from_utf8_lossy(&http.response.body));
                    }
                }
                StepOutput::TCP(tcp) => {}
                StepOutput::GraphQL(gql) => {
                    if args.request_level == Protocol::TCP {
                        println!(
                            "> {}",
                            String::from_utf8_lossy(&gql.http.raw_request).replace("\n", "\n> ")
                        );
                    } else if args.request_level == Protocol::HTTP {
                        println!("> {} {}", gql.http.method, gql.http.url);
                        for (k, v) in gql.http.headers {
                            println!(">   {}: {}", k, v);
                        }
                        println!("> {}", &gql.http.body);
                    }
                    if args.level == Some(Protocol::TCP) {
                        println!(
                            "< {}",
                            String::from_utf8_lossy(&gql.http.raw_response).replace("\n", "\n< ")
                        );
                    }
                    if args.level == Some(Protocol::HTTP) || args.level == None {
                        println!(
                            "< {} {}",
                            gql.http.response.status_code, gql.http.response.protocol
                        );
                        for (k, v) in gql.http.response.headers {
                            println!("<   {}: {}", k, v);
                        }
                        println!("< {}", String::from_utf8_lossy(&gql.http.response.body));
                    }
                    if args.level == Some(Protocol::GraphQL) || args.level == None {
                        if args.raw {
                            println!("{}", String::from_utf8_lossy(&gql.response.raw));
                        } else {
                            println!("{}", serde_json::to_string_pretty(&gql.response.formatted)?);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
