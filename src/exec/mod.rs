pub mod graphql;
pub mod http;
pub mod http1;
mod runner;
pub mod tcp;
mod tee;
pub mod tls;

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

use crate::{Output, Plan, Step, StepOutput, StepPlanOutput, StepPlanOutputs};

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
        let mut inputs = State {
            data: &self.outputs,
            current: StepPlanOutputs::default(),
        };
        // Reverse iterate the protocol stack for evaluation so that protocols below can access
        // request fields from higher protocols.
        let mut runner: Option<Box<dyn Runner>> = None;
        let requests = step
            .into_stack()
            .iter()
            .rev()
            .map(|proto| {
                let req = proto.evaluate(&inputs)?;
                match &req {
                    StepPlanOutput::GraphQl(req) => inputs.current.graphql = Some(req.clone()),
                    StepPlanOutput::Http(req) => inputs.current.http = Some(req.clone()),
                    StepPlanOutput::Http1(req) => inputs.current.http1 = Some(req.clone()),
                    StepPlanOutput::Tls(req) => inputs.current.tls = Some(req.clone()),
                    StepPlanOutput::Tcp(req) => inputs.current.tcp = Some(req.clone()),
                }
                Ok(req)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // We built the protocol requests top to bottom, now reverse iterate so we build the
        // runners bottom to top.
        for req in requests.into_iter().rev() {
            runner = Some(new_runner(runner, req).await?)
        }
        let mut runner = runner.expect("no plan should have an empty protocol stack");
        runner.execute().await;
        let mut output = StepOutput::default();
        loop {
            let (out, inner) = runner.finish().await;
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
    current: StepPlanOutputs,
}

impl<'a> crate::State<'a, &'a str, StateIterator<'a>> for State<'a> {
    fn get(&self, name: &'a str) -> Option<&StepOutput> {
        self.data.get(name)
    }
    fn current(&self) -> &StepPlanOutputs {
        &self.current
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
