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

use self::runner::{new_runner, Runner};

pub struct Executor<'a> {
    steps: VecDeque<(&'a str, Step)>,
    outputs: HashMap<&'a str, StepOutput>,
}

impl<'a> Executor<'a> {
    pub fn new(plan: &'a Plan) -> Self {
        Executor {
            steps: plan
                .steps
                .iter()
                .map(|(name, step)| (name.as_str(), step.to_owned()))
                .collect(),
            outputs: HashMap::new(),
        }
    }

    pub async fn next(&mut self) -> Result<StepOutput, Box<dyn std::error::Error + Send + Sync>> {
        let Some((name, step)) = self.steps.pop_front() else {
            return Err(Box::new(Error::Done));
        };
        let inputs = &State {
            data: &self.outputs,
        };
        let mut runner: Option<Box<dyn Runner>> = None;
        for proto in step.into_stack() {
            runner = Some(new_runner(runner, proto.evaluate(inputs)?).await?)
        }
        let mut runner = runner.expect("no plan should have an empty protocol stack");
        runner.execute().await?;
        let mut output = StepOutput::default();
        loop {
            let (out, inner) = runner.finish().await?;
            match out {
                Output::GraphQl(out) => output.graphql = Some(out),
                Output::Http(out) => output.http = Some(out),
                Output::Http1(out) => output.http1 = Some(out),
                Output::Tls(out) => output.tls = Some(out),
                Output::Tcp(out) => output.tcp = Some(out),
            }
            let Some(inner) = inner else {
                break;
            };
            runner = inner;
        }

        self.outputs.insert(name, output.clone());
        Ok(output)
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
            data: self.data.keys().map(ToOwned::to_owned).collect(),
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
