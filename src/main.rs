use anyhow::anyhow;
use async_broadcast::{broadcast, Receiver, RecvError, Sender};
use clap::{ArgGroup, Parser, ValueEnum};
use devil::exec::Executor;
use devil::record::{BigQueryWriter, Describe, FileWriter, Record, RecordWriter, StdoutWriter};
use devil::{
    JobOutput, Pdu, Plan, ProtocolOutput, ProtocolOutputDiscriminants, RunOutput, StepOutput,
};
use futures::future::try_join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use strum::Display;
use tokio::spawn;
use tracing::{error, warn};
use tracing_subscriber::EnvFilter;

// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The path to the query plan.
    #[arg(value_name = "FILE")]
    file: Vec<String>,

    /// Print requests and responses in the specified format.
    #[arg(short, long, conflicts_with = "out", value_enum)]
    format: Option<OutputFormat>,

    /// Which layers of the stack to output. Shorthand for -o layers=VALUE
    #[arg(short, long, value_enum, conflicts_with = "out", value_delimiter = ',')]
    layers: Vec<Protocol>,

    /// Compile a query plan but don't execute it.
    #[arg(long)]
    dry_run: bool,

    /// Specify output sinks.
    #[arg(short, long, value_parser = parse_outputs)]
    out: Vec<Output>,

    /// The number of output records to buffer for writers that fall behind.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..u64::try_from(usize::MAX).unwrap_or(u64::MAX)), default_value_t = 100)]
    out_buffer: u64,

    /// What to do when there is no room in the output buffer.
    #[arg(long, default_value_t)]
    overflow_behavior: OverflowBehavior,

    /// Print more details.
    #[arg(long)]
    debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum Output {
    Stdout {
        #[serde(default)]
        format: OutputFormat,
        #[serde(default)]
        layers: Vec<Protocol>,
        #[serde(default = "Normalize::stdout_default")]
        normalize: Normalize,
    },
    File {
        path: String,
        #[serde(default)]
        format: OutputFormat,
        #[serde(default)]
        layers: Vec<Protocol>,
        #[serde(default)]
        normalize: Normalize,
    },
    BigQuery {
        project: String,
        dataset: String,
        table: String,
        #[serde(default)]
        layers: Vec<Protocol>,
        #[serde(default)]
        normalize: Normalize,
    },
}

fn parse_outputs(s: &str) -> anyhow::Result<Output> {
    let args = s
        .split(",")
        .map(|pair| {
            pair.split_once("=")
                .map(|(k, v)| {
                    (
                        k.trim().to_string(),
                        serde_json::Value::String(v.trim().into()),
                    )
                })
                .ok_or_else(|| anyhow!("invalid out flag format"))
        })
        .try_collect()?;
    Ok(serde_json::from_value(serde_json::Value::Object(args)).unwrap())
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Normalize {
    Pdu,
    Protocol,
    Job,
    Step,
    #[default]
    None,
}

impl Normalize {
    fn stdout_default() -> Self {
        Self::Step
    }
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[clap(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum Protocol {
    Graphql,
    Http,
    H1,
    H1c,
    H2,
    H2c,
    RawH2,
    RawH2c,
    Tls,
    Tcp,
    RawTcp,
    //Udp,
    //Quic,
    //Ip,
}

impl From<&Protocol> for devil::ProtocolOutputDiscriminants {
    fn from(value: &Protocol) -> Self {
        match value {
            Protocol::Graphql => Self::GraphQl,
            Protocol::Http => Self::Http,
            Protocol::H1 => Self::H1,
            Protocol::H1c => Self::H1c,
            Protocol::H2 => Self::H2,
            Protocol::H2c => Self::H2c,
            Protocol::RawH2 => Self::RawH2,
            Protocol::RawH2c => Self::RawH2c,
            Protocol::Tls => Self::Tls,
            Protocol::Tcp => Self::Tcp,
            Protocol::RawTcp => Self::RawTcp,
            //Protocol::Udp => Self::Udp,
            //Protocol::Quic => Self::Quic,
            //Protocol::Ip => Self::Ip,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[clap(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    #[default]
    Describe,
    Toml,
    Json,
}

#[derive(ValueEnum, Debug, Clone, Default, Display)]
#[clap(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum OverflowBehavior {
    #[default]
    Wait,
    Skip,
    Discard,
    // TODO: implement disk/bucket buffering
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut args = Args::parse();
    if args.out.is_empty() {
        args.out.push(Output::Stdout {
            format: args.format.unwrap_or_default(),
            layers: args.layers,
            normalize: Normalize::Step,
        });
    }

    let writers = try_join_all(args.out.into_iter().map(Writer::new)).await?;

    let (mut sender, recv) = broadcast(args.out_buffer.try_into().unwrap());

    let handles = writers
        .into_iter()
        .map(|mut w| {
            let mut recv = recv.clone();
            spawn(async move {
                while !recv.is_closed() {
                    if let Err(e) = w.write(&mut recv).await {
                        error!("write output to {}: {e}", w.inner.name())
                    }
                }
            })
        })
        .collect_vec();

    // Allow the queue to drain properly.
    drop(recv);

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

        let mut executor = Executor::new(&plan)?;
        let mut plan_output = RunOutput::default();
        for (name, _) in plan.steps.iter() {
            let step_output = Arc::new(executor.next().await?);
            send(
                &mut sender,
                FlushMessages::Step(step_output.clone()),
                &args.overflow_behavior,
            )
            .await;
            plan_output.steps.insert(name.clone(), step_output);
        }
        send(
            &mut sender,
            FlushMessages::Plan(Arc::new(plan_output)),
            &args.overflow_behavior,
        )
        .await;
    }

    // Shutdown and wait for all writers.
    drop(sender);
    try_join_all(handles).await?;

    Ok(())
}

async fn send(sender: &mut Sender<FlushMessages>, out: FlushMessages, overflow: &OverflowBehavior) {
    let broadcast_result = sender.broadcast_direct(out).await;
    match (broadcast_result, overflow) {
        (Ok(None), _) => {}
        (Ok(Some(_)), _) => warn!("output discarded by at least one writer"),
        (Err(async_broadcast::SendError(_)), OverflowBehavior::Skip) => {
            warn!("output buffer is full, output skipped")
        }
        (Err(async_broadcast::SendError(_)), _) => {
            panic!("writers closed before shutdown")
        }
    }
}

#[derive(Debug, Clone)]
enum FlushMessages {
    Step(Arc<StepOutput>),
    Plan(Arc<RunOutput>),
}

impl FlushMessages {
    fn normalize(self, target: Normalize) -> Vec<Normalized> {
        match (self, target) {
            (Self::Step(s), Normalize::Step) => vec![Normalized::Step(s)],
            (Self::Step(s), Normalize::Job) => s
                .jobs
                .values()
                .cloned()
                .map(|j| Normalized::Job(j))
                .collect(),
            (Self::Step(s), Normalize::Protocol) => s
                .jobs
                .values()
                .map(|s| s.stack())
                .flatten()
                .map(Normalized::Protocol)
                .collect(),
            (Self::Step(s), Normalize::Pdu) => s
                .jobs
                .values()
                .map(|s| s.stack())
                .flatten()
                .map(|p| p.pdus())
                .flatten()
                .map(Normalized::Pdu)
                .collect(),

            (Self::Plan(p), Normalize::None) => vec![Normalized::None(p)],
            (Self::Plan(p), Normalize::Step) => {
                p.steps.values().cloned().map(Normalized::Step).collect()
            }
            (Self::Plan(p), Normalize::Job) => p
                .steps
                .values()
                .map(|s| s.jobs.values())
                .flatten()
                .cloned()
                .map(Normalized::Job)
                .collect(),
            (Self::Plan(p), Normalize::Protocol) => p
                .steps
                .values()
                .map(|s| s.jobs.values())
                .flatten()
                .map(|j| j.stack())
                .flatten()
                .map(Normalized::Protocol)
                .collect(),
            (Self::Plan(s), Normalize::Pdu) => s
                .steps
                .values()
                .map(|s| s.jobs.values())
                .flatten()
                .map(|s| s.stack())
                .flatten()
                .map(|p| p.pdus())
                .flatten()
                .map(Normalized::Pdu)
                .collect(),
            (_, Normalize::None) => {
                unreachable!("normalization is required when flush is not plan")
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Normalized {
    None(Arc<RunOutput>),
    Step(Arc<StepOutput>),
    Job(Arc<JobOutput>),
    Protocol(ProtocolOutput),
    Pdu(Pdu),
}

impl Record for Normalized {}

impl Describe for Normalized {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        match self {
            Self::None(o) => o.describe(w, layers),
            Self::Step(o) => o.describe(&mut w, layers),
            Self::Job(o) => o.describe(&mut w, layers),
            Self::Protocol(o) => o.describe(&mut w, layers),
            Self::Pdu(o) => o.describe(&mut w, layers),
        }
    }
}

struct Writer {
    inner: RecordWriter,
    normalize: Normalize,
    layers: Vec<ProtocolOutputDiscriminants>,
}

impl Writer {
    async fn new(out: Output) -> anyhow::Result<Self> {
        match out {
            Output::Stdout {
                format,
                layers,
                normalize,
            } => Ok(Writer {
                inner: RecordWriter::Stdout(StdoutWriter::new(match format {
                    OutputFormat::Describe => devil::record::Serializer::Describe,
                    OutputFormat::Json => devil::record::Serializer::Json,
                    OutputFormat::Toml => devil::record::Serializer::Toml,
                })),
                layers: layers
                    .iter()
                    .map(devil::ProtocolOutputDiscriminants::from)
                    .collect(),
                normalize,
            }),
            Output::File {
                path,
                format,
                layers,
                normalize,
            } => Ok(Writer {
                inner: RecordWriter::File(
                    FileWriter::new(
                        &path,
                        match format {
                            OutputFormat::Describe => devil::record::Serializer::Describe,
                            OutputFormat::Json => devil::record::Serializer::Json,
                            OutputFormat::Toml => devil::record::Serializer::Toml,
                        },
                    )
                    .await?,
                ),
                layers: layers
                    .iter()
                    .map(devil::ProtocolOutputDiscriminants::from)
                    .collect(),
                normalize,
            }),
            Output::BigQuery {
                project,
                dataset,
                table,
                normalize,
                layers,
            } => Ok(Writer {
                inner: RecordWriter::BigQuery(
                    BigQueryWriter::with_project_id(
                        project.clone(),
                        dataset.clone(),
                        table.clone(),
                    )
                    .await?,
                ),
                layers: layers
                    .iter()
                    .map(devil::ProtocolOutputDiscriminants::from)
                    .collect(),
                normalize,
            }),
        }
    }

    async fn write(&mut self, recv: &mut Receiver<FlushMessages>) -> anyhow::Result<()> {
        match recv.recv_direct().await {
            Ok(flush) => {
                let normalized = flush.normalize(self.normalize);
                self.inner.write(&normalized, &self.layers).await
            }
            Err(RecvError::Overflowed(n)) => {
                warn!("{} writer missed {n} messages", self.inner.name());
                Ok(())
            }
            Err(RecvError::Closed) => return Ok(()),
        }
    }
}
