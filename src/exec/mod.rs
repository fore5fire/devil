pub mod graphql;
pub mod http;
pub mod http1;
mod runner;
pub mod tcp;
mod tee;
pub mod tls;

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

use crate::{Output, Plan, Step, StepOutput};

use self::graphql::GraphQLRunner;
use self::http::HTTPRunner;
use self::http1::HTTP1Runner;
use self::runner::Runner;
use self::tcp::TCPRunner;
use self::tls::TLSRunner;

pub struct Executor<'a> {
    iter: indexmap::map::Iter<'a, String, Step>,
    outputs: HashMap<&'a str, StepOutput>,
}

impl<'a> Executor<'a> {
    pub fn new(plan: &'a Plan) -> Self {
        Executor {
            iter: plan.steps.iter(),
            outputs: HashMap::new(),
        }
    }

    pub async fn next(&mut self) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
        let Some((name, step)) = &self.iter.next() else {
            return Err(Box::new(Error::Done));
        };
        let inputs = &State {
            data: &self.outputs,
        };
        let mut runner: Option<Box<dyn Runner>> = None;
        for proto in step.stack() {
            runner = Some(match proto.evaluate(inputs)? {
                Output::GraphQL(proto) => Box::new(
                    GraphQLRunner::new(
                        runner.expect("no plan should have graphql as a base protocol"),
                        proto,
                    )
                    .await?,
                ),
                Output::HTTP(proto) => Box::new(HTTPRunner::new(proto).await?),
                Output::HTTP1(proto) => Box::new(
                    HTTP1Runner::new(
                        runner.expect("no plan should have http1 as a base protocol"),
                        proto,
                    )
                    .await?,
                ),
                Output::TLS(proto) => Box::new(
                    TLSRunner::new(
                        runner.expect("no plan should have tls as a base protocol"),
                        proto,
                    )
                    .await?,
                ),
                Output::TCP(proto) => Box::new(TCPRunner::new(proto).await?),
                _ => return Err(Box::new(crate::Error::from("protocol not implemented"))),
            })
        }
        let mut runner = runner.expect("no plan should have an empty protocol stack");
        runner.execute().await?;
        let out = StepOutput::default();
        loop {
            let (out, inner) = runner.finish().await?;
            let Some(inner) = inner else {
                break;
            };
            runner = inner;
        }

        self.outputs.insert(name, out.clone());
        Ok(out)
    }
}

struct State<'a> {
    data: &'a HashMap<&'a str, StepOutput>,
}

impl<'a> crate::State<'a, &'a str, StateIterator<'a>> for State<'a> {
    fn get(&self, name: &'a str) -> Option<&StepOutput> {
        self.data.get(name)
    }
    fn iter(&self) -> StateIterator<'a> {
        StateIterator {
            data: self.data.keys().map(|k| k.to_owned()).collect(),
        }
    }
}

struct StateIterator<'a> {
    data: VecDeque<&'a str>,
}

impl<'a> Iterator for StateIterator<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front()
    }
}

#[derive(Debug)]
pub enum Error {
    Done,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("execution done")
    }
}

impl std::error::Error for Error {}
