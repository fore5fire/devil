pub mod graphql;
pub mod http;
pub mod tcp;
mod tee;
pub mod tls;

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;

use crate::{Plan, Protocol, Step, StepOutput};

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
        let out = match &step.main {
            Protocol::TCP(req) => tcp::execute(req, &inputs).await?,
            Protocol::TLS(req) => tls::execute(req, &inputs).await?,
            Protocol::HTTP(req) => http::execute(req, &inputs).await?,
            Protocol::HTTP1(req) => http::execute(&req.http, &inputs).await?,
            Protocol::HTTP2(req) => http::execute(&req.http, &inputs).await?,
            Protocol::HTTP3(req) => http::execute(&req.http, &inputs).await?,
            Protocol::GraphQL(req) => graphql::execute(&req, &inputs).await?,
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
