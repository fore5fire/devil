pub mod graphql;
pub mod http;
pub mod http1;
mod pause;
mod runner;
pub mod tcp;
mod tee;
pub mod tls;

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::num::TryFromIntError;
use std::sync::Arc;

use futures::future::try_join_all;
use indexmap::IndexMap;
use tokio::sync::Barrier;

use crate::{
    Evaluate, IterableKey, Output, Plan, Step, StepOutput, StepPlanOutput, StepPlanOutputs,
    StepProtocols,
};

use self::runner::Runner;

pub struct Executor<'a> {
    steps: VecDeque<(&'a str, Step)>,
    outputs: HashMap<&'a str, IndexMap<IterableKey, StepOutput>>,
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

    pub async fn next(
        &mut self,
    ) -> Result<IndexMap<IterableKey, StepOutput>, Box<dyn std::error::Error + Send + Sync>> {
        let Some((name, step)) = self.steps.pop_front() else {
            return Err(Box::new(Error::Done));
        };
        let mut inputs = State {
            data: &self.outputs,
            current: StepPlanOutputs::default(),
            run_while: None,
            run_for: None,
            run_count: None,
        };

        let mut output = IndexMap::new();

        // Check the if condition only before the first iteration.
        if !step.run.run_if.evaluate(&inputs)? {
            return Ok(output);
        }

        let parallel = step.run.parallel.evaluate(&inputs)?;
        // Don't allow parallel execution with while (for now at least).
        if step.run.run_while.is_some() && parallel {
            return Err(Box::new(crate::Error(
                "run.while cannot be used with run.parallel".to_owned(),
            )));
        }

        let for_pairs = step.run.run_for.map(|f| f.evaluate(&inputs)).transpose()?;

        let mut count = step.run.count.evaluate(&inputs)?;
        if let Some(pairs) = &for_pairs {
            count = count.min(pairs.len().try_into()?)
        }
        let count_usize: usize = count.try_into()?;

        // Preallocate space when able.
        if step.run.run_while.is_none() {
            output.try_reserve(count_usize)?;
        }

        let mut for_iterator = for_pairs.map(|pairs| pairs.into_iter());

        if parallel {
            let ctx = Arc::new(Context {
                pause_barriers: step
                    .protocols
                    .pause_joins()
                    .into_iter()
                    .map(|key| Ok((key.to_owned(), Arc::new(Barrier::new(count.try_into()?)))))
                    .collect::<Result<HashMap<_, _>, TryFromIntError>>()?,
            });

            let mut states: Vec<_> = (0..count)
                .map(|i| {
                    // Process current item if for is used.
                    let mut key = None;
                    if let Some(pairs) = for_iterator.as_mut() {
                        let (k, v) = pairs.next().expect(
                            "iteration count should be limited by the length of the for iterable",
                        );
                        inputs.run_for = Some(crate::RunForOutput {
                            key: k.clone(),
                            value: v.into(),
                        });
                        key = Some(k.clone());
                    }

                    inputs.run_count = Some(crate::RunCountOutput { index: i });
                    (key.unwrap_or(IterableKey::Uint(i)), inputs.clone())
                })
                .collect();

            let ops = try_join_all(states.iter_mut().map(|(key, state)| {
                Self::iteration(ctx.clone(), key, step.protocols.clone(), state)
            }))
            .await?;

            output.extend(ops.into_iter().map(|(key, out)| (key.to_owned(), out)));
        } else {
            if !step.protocols.pause_joins().is_empty() {
                return Err(Box::new(crate::Error(
                    "join only allowed with parallel steps".to_owned(),
                )));
            }
            let ctx = Arc::new(Context::default());
            for i in 0..count {
                // Process current item if for is used.
                let mut key = None;
                if let Some(pairs) = for_iterator.as_mut() {
                    let (k, v) = pairs.next().expect(
                        "iteration count should be limited by the length of the for iterable",
                    );
                    inputs.run_for = Some(crate::RunForOutput {
                        key: k.clone(),
                        value: v.into(),
                    });
                    key = Some(k.clone());
                }
                let key = key.unwrap_or(IterableKey::Uint(i));

                // Evaluate while condition on each loop if it is set.
                if let Some(w) = &step.run.run_while {
                    inputs.run_while = Some(crate::RunWhileOutput { index: i });
                    if !w.evaluate(&inputs)? {
                        break;
                    }
                    output.try_reserve(1)?;
                }

                inputs.run_count = Some(crate::RunCountOutput { index: i });
                let (_, out) =
                    Self::iteration(ctx.clone(), &key, step.protocols.clone(), &mut inputs).await?;
                output.insert(key, out);
            }
        }

        self.outputs.insert(name, output.clone());
        Ok(output)
    }

    async fn iteration(
        ctx: Arc<Context>,
        key: &IterableKey,
        protos: StepProtocols,
        inputs: &mut State<'_>,
    ) -> Result<(IterableKey, StepOutput), Box<dyn std::error::Error + Send + Sync>> {
        // Reverse iterate the protocol stack for evaluation so that protocols below can access
        // request fields from higher protocols.
        let mut runner: Option<Runner> = None;
        let requests = protos
            .into_stack()
            .iter()
            .rev()
            .map(|proto| {
                let req = proto.evaluate(inputs)?;
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
            runner = Some(Runner::new(ctx.clone(), runner, req).await?)
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
        Ok((key.to_owned(), output))
    }
}

#[derive(Debug, Clone)]
struct State<'a> {
    data: &'a HashMap<&'a str, IndexMap<crate::IterableKey, StepOutput>>,
    current: StepPlanOutputs,
    run_while: Option<crate::RunWhileOutput>,
    run_for: Option<crate::RunForOutput>,
    run_count: Option<crate::RunCountOutput>,
}

impl<'a> crate::State<'a, &'a str, StateIterator<'a>> for State<'a> {
    fn get(&self, name: &'a str) -> Option<&IndexMap<IterableKey, StepOutput>> {
        self.data.get(name)
    }
    fn current(&self) -> &StepPlanOutputs {
        &self.current
    }
    fn run_for(&self) -> &Option<crate::RunForOutput> {
        &self.run_for
    }
    fn run_while(&self) -> &Option<crate::RunWhileOutput> {
        &self.run_while
    }
    fn run_count(&self) -> &Option<crate::RunCountOutput> {
        &self.run_count
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

#[derive(Debug, Default)]
pub(super) struct Context {
    pause_barriers: HashMap<String, Arc<Barrier>>,
}

impl Context {
    pub(super) fn pause_barrier(&self, tag: &str) -> Arc<Barrier> {
        self.pause_barriers[tag].clone()
    }
}
