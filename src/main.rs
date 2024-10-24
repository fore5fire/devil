use anyhow::anyhow;
use clap::{ArgGroup, Parser, ValueEnum};
use devil::exec::Executor;
use devil::record::{BigQueryWriter, FileWriter, RecordWriter};
use devil::{Plan, PlanOutput};
use futures::future::try_join_all;
use itertools::Itertools;
use serde::Deserialize;
use serde_hashkey::{from_key, Key, RejectFloatPolicy};
use std::mem;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(ArgGroup::new("bigquery").multiple(true)))]
struct Args {
    /// Print more details.
    #[arg(long)]
    debug: bool,

    /// Print requests and responses in the specified format.
    #[arg(short, long, value_enum)]
    format: Option<OutputFormat>,

    /// Print responses at a lower level protocol. Shorthand for -o recv_level=VALUE
    #[arg(short = 'l', long, value_enum, value_delimiter = ',')]
    recv_level: Option<Vec<Protocol>>,

    /// Print requests at a lower level protocol. Shorthand for -o send_level=VALUE
    #[arg(short = 'L', long, value_enum)]
    send_level: Option<Vec<Protocol>>,

    /// The path to the query plan.
    #[arg(value_name = "FILE")]
    file: Vec<String>,

    /// Compile a query plan but don't execute it.
    #[arg(long)]
    dry_run: bool,

    // Specify output sinks.
    #[arg(short, long, value_parser = Output::parse)]
    out: Vec<Output>,

    // The number of output records to buffer for writers that fall behind.
    #[arg(long)]
    out_buffer: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Output {
    File {
        path: String,
        format: OutputFormat,
        send_level: Vec<Protocol>,
        recv_level: Option<Vec<Protocol>>,
    },
    BigQuery {
        project: String,
        dataset: String,
        table: String,
    },
}

impl Default for Output {
    fn default() -> Self {
        Self::File {
            path: "-".to_owned(),
            format: OutputFormat::Describe,
            send_level: Vec::new(),
            recv_level: None,
        }
    }
}

impl Output {
    fn parse(arg: &str) -> anyhow::Result<Self> {
        let args = Key::Map::<RejectFloatPolicy>(
            arg.split(",")
                .map(|pair| {
                    pair.split_once("=")
                        .map(|(k, v)| {
                            (
                                Key::<RejectFloatPolicy>::String(k.trim().into()),
                                Key::<RejectFloatPolicy>::String(v.trim().into()),
                            )
                        })
                        .ok_or_else(|| anyhow!("invalid out flag format"))
                })
                .try_collect()?,
        );
        Ok(from_key(&args)?)
    }
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq, Deserialize)]
#[clap(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum Protocol {
    Graphql,
    Http,
    Http2Frames,
    Tls,
    Tcp,
    TcpSegments,
    Udp,
    Quic,
    Ip,
    None,
}

impl From<&Protocol> for devil::record::Protocol {
    fn from(value: &Protocol) -> Self {
        match value {
            Protocol::Graphql => Self::Graphql,
            Protocol::Http => Self::Http,
            Protocol::Http2Frames => Self::Http2Frames,
            Protocol::Tls => Self::Tls,
            Protocol::Tcp => Self::Tcp,
            Protocol::TcpSegments => Self::TcpSegments,
            Protocol::Udp => Self::Udp,
            Protocol::Quic => Self::Quic,
            Protocol::Ip => Self::Ip,
            Protocol::None => Self::None,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[clap(rename_all = "lower")]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    #[default]
    Describe,
    Toml,
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut args = Args::parse();
    if args.recv_level.is_some() || args.send_level.is_some() || args.format.is_some() {
        args.out.push(Output::File {
            path: "-".to_owned(),
            format: args.format.unwrap_or_default(),
            send_level: args.send_level.unwrap_or_default(),
            recv_level: args.recv_level,
        })
    }
    for file in &args.file {
        let buffer = std::fs::read(file)?;
        let text = String::from_utf8(buffer)?;
        let plan = Plan::parse(&text)?;
        if args.debug {
            println!("query plan: {:#?}", plan);
        }
        if args.dry_run {
            return Ok(());
        }

        let (send, recv) = broadcast::channel(args.out_buffer);

        let outputs = mem::take(&mut args.out)
            .into_iter()
            .map(|out| (out, send.subscribe()));
        let mut writers: Vec<_> = try_join_all(outputs.map(|(out, recv)| async move {
            match out {
                Output::File {
                    path,
                    format,
                    send_level,
                    recv_level,
                } => Ok::<_, anyhow::Error>(RecordWriter::File(
                    FileWriter::new(
                        &path,
                        match format {
                            OutputFormat::Describe => devil::record::Serializer::Describe {
                                sent: send_level.iter().map(From::from).collect(),
                                received: recv_level
                                    .map(|levels| levels.iter().map(From::from).collect()),
                            },
                            OutputFormat::Json => devil::record::Serializer::Json,
                            OutputFormat::Toml => devil::record::Serializer::Toml,
                        },
                    )
                    .await?,
                )),
                Output::BigQuery {
                    project,
                    dataset,
                    table,
                } => Ok(RecordWriter::BigQuery(
                    BigQueryWriter::with_project_id(
                        project.clone(),
                        dataset.clone(),
                        table.clone(),
                    )
                    .await?,
                )),
            }
        }))
        .await?;

        // I'm not sure whether it's allowed to subscribe after all receivers have been dropped, so
        // hang onto the first one until we're sure the writers are listening.
        drop(recv);

        let mut executor = Executor::new(&plan)?;
        let mut result = PlanOutput::default();
        for (name, _) in plan.steps.iter() {
            let step_output = executor.next().await?;
            send.send(step_output);
            for w in &mut writers {
                w.write(&step_output).await?;
            }
            result.steps.insert(name.clone(), step_output);
        }
    }
    Ok(())
}
