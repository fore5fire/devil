mod extract;
pub mod graphql;
pub mod http;
pub mod http1;
pub mod http2;
pub mod http2frames;
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
use itertools::{Either, Itertools};
use tokio::sync::Barrier;
use tokio_task_pool::Pool;

use crate::{
    Evaluate, IterableKey, Output, Parallelism, Plan, Protocol, ProtocolField, Step, StepOutput,
    StepPlanOutput, StepPlanOutputs,
};

use self::runner::Runner;

pub struct Executor<'a> {
    locals: HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>,
    steps: VecDeque<(&'a str, Step)>,
    outputs: HashMap<&'a str, IndexMap<IterableKey, StepOutput>>,
}

impl<'a> Executor<'a> {
    pub fn new(plan: &'a Plan) -> Result<Self, crate::Error> {
        // Evaluate the locals in order.
        let mut locals = HashMap::new();
        for (k, v) in plan.locals.iter() {
            let inputs = State {
                data: &HashMap::new(),
                locals: &locals,
                current: StepPlanOutputs::default(),
                run_while: None,
                run_for: None,
                run_count: None,
            };
            let out = v.evaluate(&inputs)?;
            locals.insert(k.clone().into(), out.0);
        }
        Ok(Executor {
            locals: locals.into(),
            steps: plan
                .steps
                .iter()
                .map(|(name, step)| (name.as_str(), step.to_owned()))
                .collect(),
            outputs: HashMap::with_capacity(plan.steps.len()),
        })
    }

    pub async fn next(
        &mut self,
    ) -> Result<IndexMap<IterableKey, StepOutput>, Box<dyn std::error::Error + Send + Sync>> {
        let Some((name, step)) = self.steps.pop_front() else {
            return Err(Box::new(Error::Done));
        };
        let mut inputs = State {
            data: &self.outputs,
            locals: &self.locals,
            current: StepPlanOutputs::default(),
            run_while: None,
            run_for: None,
            run_count: None,
        };

        // Check the if condition only before the first iteration.
        if !step.run.run_if.evaluate(&inputs)? {
            return Ok(IndexMap::new());
        }

        let parallel = step.run.parallel.evaluate(&inputs)?;
        // Don't allow parallel execution with while (for now at least).
        if step.run.run_while.is_some() && !matches!(parallel, crate::Parallelism::Serial) {
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
        let mut output = IndexMap::new();
        if step.run.run_while.is_none() {
            output.try_reserve(count_usize)?;
        }

        let mut for_iterator = for_pairs.map(|pairs| pairs.into_iter());

        // Compute the shared and duplicated protocol stacks.
        let mut stack = step.protocols.into_stack();
        let shared = step
            .run
            .share
            .map(|share| share.evaluate(&inputs))
            .transpose()?;
        let shared_stack = shared
            .map(|share| {
                stack
                    .iter()
                    .enumerate()
                    .find(|(_, proto)| proto.field() != share)
                    .map(|(i, _)| i)
            })
            .flatten()
            .map(|i| stack.split_off(i))
            .unwrap_or_default();

        if shared_stack
            .iter()
            .flat_map(Protocol::joins)
            .next()
            .is_some()
        {
            return Err(Box::new(crate::Error(
                "join not allowed with shared steps".to_owned(),
            )));
        }

        // Create the runners for the shared stack in advance.
        let shared_runners =
            Self::prepare_runners(&Arc::new(Context::default()), &shared_stack, &mut inputs)?;

        match parallel {
            Parallelism::Parallel(max_parallel) => {
                let ctx = Arc::new(Context {
                    pause_barriers: stack
                        .iter()
                        .flat_map(Protocol::joins)
                        .map(|key| Ok((key.to_owned(), Arc::new(Barrier::new(count.try_into()?)))))
                        .collect::<Result<HashMap<_, _>, TryFromIntError>>()?,
                });

                let states: Vec<_> = (0..count)
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
                        Ok((
                            key.unwrap_or(IterableKey::Uint(i)),
                            Self::prepare_runners(&ctx, &stack, &mut inputs.clone())?,
                            shared,
                        ))
                    })
                    .collect::<crate::Result<_>>()?;

                // Start the shared runners.
                let mut shared_transport =
                    Executor::start_runners(None, shared_runners, count_usize).await?;
                let shared_transports = match &mut shared_transport {
                    Some(Runner::Http2Frames(r)) => {
                        Either::Left(itertools::repeat_n(r.new_stream(), count_usize).map(|s| {
                            Some(Runner::MuxHttp2Frames(s.expect(
                                "a stream should be available for each concurrent run",
                            )))
                        }))
                    }
                    Some(r) => {
                        return Err(Box::new(crate::Error(format!(
                            "concurrent sharing of protocol {:?} is not supported",
                            r.field(),
                        ))))
                    }
                    None => Either::Right((0..count_usize).map(|_| None)),
                };

                // Start the parallel runners and execute.
                let task_pool = Pool::bounded(max_parallel);
                let mut ops = Vec::with_capacity(states.len());
                for ((key, runners, shared), shared_transport) in
                    states.into_iter().zip(shared_transports)
                {
                    let op = task_pool
                        .spawn(async move {
                            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                                key,
                                Executor::iteration(
                                    Executor::start_runners(shared_transport, runners, 1)
                                        .await?
                                        .expect("any stack should have at least one protocol"),
                                    shared,
                                )
                                .await?,
                            ))
                        })
                        // Wait for our turn in the pool.
                        .await?;
                    ops.push(op);
                }

                output.extend(
                    try_join_all(ops)
                        .await?
                        .into_iter()
                        .collect::<Result<Result<Vec<_>, _>, _>>()??
                        .into_iter()
                        .map(|(key, (out, _))| (key.to_owned(), out)),
                );
            }
            Parallelism::Serial => {
                if stack.iter().flat_map(Protocol::joins).next().is_some() {
                    return Err(Box::new(crate::Error(
                        "join only allowed with parallel steps".to_owned(),
                    )));
                }
                let ctx = Arc::new(Context::default());

                // Start the shared runners.
                let mut shared_transport = Executor::start_runners(None, shared_runners, 1).await?;

                // Iteratively start and execute the independant runners.
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
                    let runners = Self::prepare_runners(&ctx, &stack, &mut inputs.clone())?;
                    let out;
                    (out, shared_transport) = Self::iteration(
                        Self::start_runners(shared_transport, runners, 1)
                            .await?
                            .expect("any stack should have at least one protocol"),
                        shared,
                    )
                    .await?;
                    output.insert(key, out);
                }
            }
            Parallelism::Pipelined => {
                return Err(Box::new(crate::Error(
                    "pipelining support is not yet implemented".to_owned(),
                )))
            }
        }

        self.outputs.insert(name, output.clone());
        Ok(output)
    }

    fn prepare_runners<'p>(
        ctx: &Arc<Context>,
        stack: impl IntoIterator<Item = &'p Protocol>,
        inputs: &mut State<'_>,
    ) -> Result<Vec<Runner>, crate::Error> {
        // Reverse iterate the protocol stack for evaluation so that protocols below can access
        // request fields from higher protocols.
        let requests = stack
            .into_iter()
            .map(|proto| {
                let req = proto.evaluate(inputs)?;
                match &req {
                    StepPlanOutput::GraphQl(req) => inputs.current.graphql = Some(req.clone()),
                    StepPlanOutput::Http(req) => inputs.current.http = Some(req.clone()),
                    StepPlanOutput::H1c(req) => inputs.current.h1c = Some(req.clone()),
                    StepPlanOutput::H1(req) => inputs.current.h1 = Some(req.clone()),
                    StepPlanOutput::H2c(req) => inputs.current.h2c = Some(req.clone()),
                    StepPlanOutput::H2(req) => inputs.current.h2 = Some(req.clone()),
                    StepPlanOutput::Http2Frames(req) => {
                        inputs.current.http2frames = Some(req.clone())
                    }
                    StepPlanOutput::Tls(req) => inputs.current.tls = Some(req.clone()),
                    StepPlanOutput::Tcp(req) => inputs.current.tcp = Some(req.clone()),
                }
                Ok(req)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Build the runners.
        let mut runners: Vec<_> = requests
            .into_iter()
            .map(|req| Runner::new(ctx.clone(), req))
            .try_collect()?;

        // Compute size hints for each runner.
        if let Some(executor) = runners.first() {
            let mut size_hint = executor.executor_size_hint();
            for r in &mut runners {
                size_hint = r.size_hint(size_hint);
            }
        }

        Ok(runners)
    }

    async fn start_runners(
        shared_transport: Option<Runner>,
        runners: Vec<Runner>,
        concurrent_shares: usize,
    ) -> Result<Option<Runner>, Box<dyn std::error::Error + Send + Sync>> {
        // Start the runners.
        // The runner stack was built top to bottom, so iterate backwards.
        let mut transport = shared_transport;
        for (i, mut runner) in runners.into_iter().enumerate().rev() {
            runner
                .start(transport, if i > 0 { 1 } else { concurrent_shares })
                .await?;
            transport = Some(runner);
        }

        Ok(transport)
    }

    async fn iteration(
        mut runner: Runner,
        shared: Option<ProtocolField>,
    ) -> Result<(StepOutput, Option<Runner>), Box<dyn std::error::Error + Send + Sync>> {
        runner.execute().await;
        let mut output = StepOutput::default();
        loop {
            if let Some(shared) = shared {
                if runner.field() == shared {
                    return Ok((output, Some(runner)));
                }
            }
            let (out, inner) = runner.finish().await;
            match out {
                Output::GraphQl(out) => output.graphql = Some(out),
                Output::Http(out) => output.http = Some(out),
                Output::H1c(out) => output.h1c = Some(out),
                Output::H1(out) => output.h1 = Some(out),
                Output::H2c(out) => output.h2c = Some(out),
                Output::H2(out) => output.h2 = Some(out),
                Output::Http2Frames(out) => output.http2frames = Some(out),
                Output::Tls(out) => output.tls = Some(out),
                Output::Tcp(out) => output.tcp = Some(out),
            }
            let Some(inner) = inner else {
                break;
            };
            runner = inner;
        }
        Ok((output, None))
    }
}

#[derive(Debug, Clone)]
struct State<'a> {
    data: &'a HashMap<&'a str, IndexMap<crate::IterableKey, StepOutput>>,
    current: StepPlanOutputs,
    run_while: Option<crate::RunWhileOutput>,
    run_for: Option<crate::RunForOutput>,
    run_count: Option<crate::RunCountOutput>,
    locals: &'a HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>,
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
    fn locals(&self) -> cel_interpreter::objects::Map {
        self.locals.clone().into()
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
