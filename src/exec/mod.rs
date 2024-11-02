mod buffer;
mod extract;
pub mod graphql;
pub mod http;
pub mod http1;
pub mod http2;
mod pause;
pub mod raw_http2;
pub mod raw_tcp;
mod runner;
mod sync;
pub mod tcp;
mod tee;
mod timing;
pub mod tls;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::bail;
use futures::future::try_join_all;
use itertools::{Either, Itertools, Position};
use svix_ksuid::{KsuidLike, KsuidMs};
use tokio_task_pool::Pool;
use tracing::debug;

use crate::{
    location, Evaluate, IterableKey, JobName, JobOutput, Parallelism, Plan, PlanWrapper, Protocol,
    ProtocolField, RunName, Step, StepOutput, StepPlanOutput, StepPlanOutputs,
};

use self::runner::Runner;
use sync::*;

pub struct Executor {
    locals: HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>,
    steps: VecDeque<(Arc<String>, Step)>,
    outputs: HashMap<Arc<String>, StepOutput>,
    run: RunName,
}

impl<'a> Executor {
    pub fn new(plan: &'a Plan) -> Result<Self, crate::Error> {
        let mut locals = HashMap::new();
        let run_name = RunName {
            plan: plan.name.clone(),
            run: KsuidMs::new(None, None),
        };
        // Evaluate the locals in order.
        for (k, v) in plan.locals.iter() {
            let inputs = State {
                data: &HashMap::new(),
                locals: &mut locals,
                current: StepPlanOutputs::default(),
                run_while: None,
                run_for: None,
                run_count: None,
                run_name: &run_name,
                job_name: None,
            };
            let out = v.evaluate(&inputs)?;
            locals.insert(k.clone().into(), out.0);
        }
        Ok(Executor {
            steps: plan
                .steps
                .iter()
                .map(|(name, step)| (name.clone(), step.to_owned()))
                .collect(),
            outputs: HashMap::with_capacity(plan.steps.len()),
            run: run_name,
            locals: locals.into(),
        })
    }

    pub async fn next(&mut self) -> anyhow::Result<StepOutput> {
        let Some((name, step)) = self.steps.pop_front() else {
            bail!(Error::Done);
        };
        let job_name = JobName::with_run(self.run.clone(), name.clone(), IterableKey::Uint(0));
        let mut inputs = State {
            data: &self.outputs,
            locals: &mut self.locals,
            current: StepPlanOutputs::default(),
            run_while: None,
            run_for: None,
            run_count: None,
            run_name: &job_name.run_name(),
            job_name: Some(job_name),
        };

        // Check the if condition only before the first iteration.
        if !step.run.run_if.evaluate(&inputs)? {
            return Ok(StepOutput::default());
        }

        let parallel = step.run.parallel.evaluate(&inputs)?;
        // Don't allow parallel execution with while (for now at least).
        if step.run.run_while.is_some() && !matches!(parallel, crate::Parallelism::Serial) {
            bail!("run.while cannot be used with run.parallel");
        }

        let for_pairs = step.run.run_for.map(|f| f.evaluate(&inputs)).transpose()?;

        let mut count = step.run.count.evaluate(&inputs)?;
        if let Some(pairs) = &for_pairs {
            count = count.min(pairs.len().try_into()?)
        }
        let count_usize: usize = count.try_into()?;

        // Preallocate space when able.
        let mut output = StepOutput::default();
        if step.run.run_while.is_none() {
            output.jobs.try_reserve(count_usize)?;
        }

        let mut for_iterator = for_pairs.map(|pairs| pairs.into_iter());

        // Compute the shared and duplicated protocol stacks.
        let mut stack = step.protocols.into_stack();
        let shared = step.run.share.evaluate(&inputs)?;
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

        // Create the runners for the shared stack in advance.
        let shared_runners =
            Self::prepare_runners(&Arc::new(Context::default()), &shared_stack, &mut inputs)?;
        let syncs = step
            .sync
            .iter()
            .map(|(k, v)| Ok::<_, anyhow::Error>((Arc::new(k.to_owned()), v.evaluate(&inputs)?)))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(name, sync)| (name, Some(Synchronizer::new(&sync))))
            .collect_vec();
        let signals: Vec<_> = step
            .signal
            .iter()
            .map(|(k, v)| Ok::<_, anyhow::Error>((Arc::new(k.to_owned()), v.evaluate(&inputs)?)))
            .try_collect()?;
        let pauses: Vec<_> = step
            .pause
            .iter()
            .map(|(k, v)| Ok::<_, anyhow::Error>((Arc::new(k.to_owned()), v.evaluate(&inputs)?)))
            .try_collect()?;

        match parallel {
            Parallelism::Parallel(max_parallel) => {
                let ctx = Arc::new(Context {
                    sync_locations: StepLocations::new(syncs, &signals, &pauses),
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
                                value: v.0.try_into()?,
                            });
                            if let Some(job_name) = &mut inputs.job_name {
                                job_name.job = k.clone();
                            }
                            key = Some(k);
                        } else {
                            if let Some(job_name) = &mut inputs.job_name {
                                job_name.job = IterableKey::Uint(i);
                            }
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
                    Some(Runner::RawH2c(r)) => Either::Left(Either::Left(
                        itertools::repeat_n(r.new_stream(), count_usize).map(|s| {
                            Some(Runner::MuxRawH2c(s.expect(
                                "a stream should be available for each concurrent run",
                            )))
                        }),
                    )),
                    Some(Runner::RawH2(r)) => Either::Left(Either::Right(
                        itertools::repeat_n(r.new_stream(), count_usize).map(|s| {
                            Some(Runner::MuxRawH2(s.expect(
                                "a stream should be available for each concurrent run",
                            )))
                        }),
                    )),
                    Some(r) => {
                        bail!(
                            "concurrent sharing of protocol {:?} is not supported",
                            r.field(),
                        )
                    }
                    None => Either::Right((0..count_usize).map(|_| None)),
                };

                // Start the parallel runners and execute.
                let task_pool = Pool::bounded(max_parallel);
                let mut ops = Vec::with_capacity(states.len());
                for ((key, runners, shared), shared_transport) in
                    states.into_iter().zip(shared_transports)
                {
                    let job_name = inputs.job_name.clone().unwrap();
                    let op = task_pool
                        .spawn(async move {
                            anyhow::Ok((
                                key,
                                Executor::iteration(
                                    Executor::start_runners(shared_transport, runners, 1)
                                        .await?
                                        .expect("any stack should have at least one protocol"),
                                    shared,
                                    job_name,
                                )
                                .await?,
                            ))
                        })
                        // Wait for our turn in the pool.
                        .await?;
                    ops.push(op);
                }

                output.jobs.extend(
                    try_join_all(ops)
                        .await?
                        .into_iter()
                        .collect::<Result<anyhow::Result<Vec<_>>, _>>()??
                        .into_iter()
                        .map(|(key, (out, _))| (key.to_owned(), Arc::new(out))),
                );
            }
            Parallelism::Serial => {
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
                            value: v.0.try_into()?,
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
                        output.jobs.try_reserve(1)?;
                    }

                    inputs.run_count = Some(crate::RunCountOutput { index: i });
                    let runners = Self::prepare_runners(&ctx, &stack, &mut inputs.clone())?;
                    let out;
                    (out, shared_transport) = Self::iteration(
                        Self::start_runners(shared_transport, runners, 1)
                            .await?
                            .expect("any stack should have at least one protocol"),
                        shared,
                        inputs.job_name.as_ref().unwrap().clone(),
                    )
                    .await?;
                    output.jobs.insert(key, Arc::new(out));
                }
            }
            Parallelism::Pipelined => {
                bail!("pipelining support is not yet implemented")
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
                match req.clone() {
                    StepPlanOutput::Graphql(req) => {
                        inputs.current.graphql = Some(PlanWrapper::new(req))
                    }
                    StepPlanOutput::Http(req) => inputs.current.http = Some(PlanWrapper::new(req)),
                    StepPlanOutput::H1c(req) => inputs.current.h1c = Some(PlanWrapper::new(req)),
                    StepPlanOutput::H1(req) => inputs.current.h1 = Some(PlanWrapper::new(req)),
                    StepPlanOutput::H2c(req) => inputs.current.h2c = Some(PlanWrapper::new(req)),
                    StepPlanOutput::RawH2c(req) => {
                        inputs.current.raw_h2c = Some(PlanWrapper::new(req))
                    }
                    StepPlanOutput::H2(req) => inputs.current.h2 = Some(PlanWrapper::new(req)),
                    StepPlanOutput::RawH2(req) => {
                        inputs.current.raw_h2 = Some(PlanWrapper::new(req))
                    }
                    StepPlanOutput::Tls(req) => inputs.current.tls = Some(PlanWrapper::new(req)),
                    StepPlanOutput::Tcp(req) => inputs.current.tcp = Some(PlanWrapper::new(req)),
                    StepPlanOutput::RawTcp(req) => {
                        inputs.current.raw_tcp = Some(PlanWrapper::new(req))
                    }
                }
                Ok(req)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Build the runners.
        let mut runners: Vec<_> = requests
            .into_iter()
            .with_position()
            .map(|(pos, req)| {
                Runner::new(
                    ctx.clone(),
                    req,
                    matches!(pos, Position::First | Position::Only),
                )
            })
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
    ) -> anyhow::Result<Option<Runner>> {
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
        name: JobName,
    ) -> anyhow::Result<(JobOutput, Option<Runner>)> {
        runner.execute().await;
        let mut output = JobOutput::empty(name);
        let mut current = Some(runner);
        while let Some(r) = current {
            if let Some(shared) = shared {
                if r.field() == shared {
                    return Ok((output, Some(r)));
                }
            }
            let inner = r.finish(&mut output).await;
            debug!(?inner, "finished runner");
            current = inner;
        }
        Ok((output, None))
    }
}

#[derive(Debug, Clone)]
struct State<'a> {
    data: &'a HashMap<Arc<String>, StepOutput>,
    current: StepPlanOutputs,
    run_while: Option<crate::RunWhileOutput>,
    run_for: Option<crate::RunForOutput>,
    run_count: Option<crate::RunCountOutput>,
    locals: &'a HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>,
    run_name: &'a RunName,
    job_name: Option<JobName>,
}

impl<'a> crate::State<'a, &'a Arc<String>, StateIterator<'a>> for State<'a> {
    fn get(&self, name: &'a Arc<String>) -> Option<&StepOutput> {
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
            data: self.data.keys().collect(),
        }
    }
    fn run_name(&self) -> &crate::RunName {
        &self.run_name
    }
    fn job_name(&self) -> Option<&crate::JobName> {
        self.job_name.as_ref()
    }
}

struct StateIterator<'a> {
    data: VecDeque<&'a Arc<String>>,
}

impl<'a> Iterator for StateIterator<'a> {
    type Item = &'a Arc<String>;
    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("execution done")]
    Done,
}

#[derive(Debug, Default)]
pub(super) struct Context {
    sync_locations: sync::StepLocations,
}

impl Context {
    pub(super) fn next_sync_location(&self, loc: location::Location) -> Option<StepLocation> {
        // TODO: implement
        None
    }
}
