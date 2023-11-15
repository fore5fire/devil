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
    #[arg(short = 'L', long, value_enum, default_value_t = Protocol::None)]
    request_level: Protocol,

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
    let buffer = std::fs::read(&args.file)?;
    let text = String::from_utf8(buffer)?;
    {
        let plan = Plan::parse(&text)?;
        let mut executor = Executor::new(&plan);
        for (name, _) in plan.steps.iter() {
            println!("------- executing {} --------", name);
            let output = executor.next().await?;
            print_proto(&args, &output);
        }
    }
    Ok(())
}

fn print_proto(args: &Args, proto: &StepOutput) {
    // TODO: escape or refuse to print dangerous term characters in output.
    match args.request_level {
        Protocol::TCP => {
            if let Some(tcp) = &proto.tcp {
                println!(
                    "> {}",
                    String::from_utf8_lossy(&tcp.body).replace("\n", "\n> ")
                );
            }
        }
        Protocol::TLS => {
            if let Some(tls) = &proto.tls {
                println!(
                    "> {}",
                    String::from_utf8_lossy(&tls.body).replace("\n", "\n> ")
                );
            }
        }
        Protocol::HTTP => {
            if let Some(http) = &proto.http {
                println!("> {} {}", http.method, http.url);
                for (k, v) in &http.headers {
                    println!(">   {}: {}", k, v);
                }
                if let Some(body) = &http.body {
                    println!("> {}", body);
                }
            }
        }
        Protocol::GraphQL => {
            if let Some(gql) = &proto.tcp {
                println!(
                    "< {}",
                    String::from_utf8_lossy(&gql.response.body).replace("\n", "\n< ")
                );
            }
        }
        _ => {}
    }
    // Default output is at the highest level protocol in the request.
    let out_level = args.level.clone().unwrap_or_else(|| {
        if proto.graphql.is_some() {
            Protocol::GraphQL
        } else if proto.http3.is_some() {
            Protocol::HTTP
        } else if proto.http2.is_some() {
            Protocol::HTTP
        } else if proto.http1.is_some() {
            Protocol::HTTP
        } else if proto.http.is_some() {
            Protocol::HTTP
        } else if proto.tls.is_some() {
            Protocol::TLS
        } else if proto.tcp.is_some() {
            Protocol::TCP
        } else {
            Protocol::None
        }
    });
    match out_level {
        Protocol::TCP => {
            if let Some(tcp) = &proto.tcp {
                println!(
                    "< {}",
                    String::from_utf8_lossy(&tcp.response.body).replace("\n", "\n< ")
                );
                println!("duration: {}ms", tcp.response.duration.as_millis());
            }
        }
        Protocol::TLS => {
            if let Some(tls) = &proto.tls {
                println!(
                    "< {}",
                    String::from_utf8_lossy(&tls.response.body).replace("\n", "\n< ")
                );
                println!("duration: {}ms", tls.response.duration.as_millis());
            }
        }
        Protocol::HTTP => {
            if let Some(http) = &proto.http {
                println!("< {} {}", http.response.status_code, http.response.protocol);
                for (k, v) in &http.response.headers {
                    println!("<   {}: {}", k, v);
                }
                println!("< {}", String::from_utf8_lossy(&http.response.body));
                println!("duration: {}ms", http.response.duration.as_millis());
            }
        }
        Protocol::GraphQL => {
            if let Some(gql) = &proto.graphql {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&gql.response.json).unwrap()
                );
                println!("duration: {}ms", gql.response.duration.as_millis());
            }
        }
        _ => {}
    }
}
