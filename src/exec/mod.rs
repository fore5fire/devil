mod http;
mod tcp;

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

pub use tcp::*;

use crate::{Plan, Step, StepOutput};

pub struct Executor<'a> {
    plan: &'a Plan,
    iter: indexmap::map::Iter<'a, String, Step>,
    current: Option<&'a Step>,
    outputs: HashMap<&'a str, StepOutput>,
}

impl<'a> Executor<'a> {
    pub fn new(plan: &'a Plan) -> Self {
        Executor {
            plan,
            iter: plan.steps.iter(),
            current: None,
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
        let out = match step {
            Step::HTTP(http) => http::execute(http, &inputs).await?,
            Step::HTTP11(http11) => http::execute(&http11.http, &inputs).await?,
            Step::HTTP2(http2) => http::execute(&http2.http, &inputs).await?,
            Step::HTTP3(http3) => http::execute(&http3.http, &inputs).await?,
        };

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
