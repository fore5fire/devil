use std::{fmt::Display, sync::Arc};

use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use serde::Serialize;
use svix_ksuid::{KsuidLike, KsuidMs};

use crate::{record::BigQuerySchema, IterableKey, ProtocolDiscriminants};

#[derive(Debug, Clone)]
pub struct RunName {
    pub plan: Arc<String>,
    pub run: KsuidMs,
}

impl RunName {
    pub fn new(plan: Arc<String>) -> Self {
        Self {
            plan,
            run: KsuidMs::new(None, None),
        }
    }
}

impl BigQuerySchema for RunName {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl Display for RunName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.plan, self.run)
    }
}

impl Serialize for RunName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone)]
pub struct StepName {
    pub plan: Arc<String>,
    pub run: KsuidMs,
    pub step: Arc<String>,
}

impl StepName {
    pub fn with_run(run: RunName, step: Arc<String>) -> Self {
        Self {
            plan: run.plan,
            run: run.run,
            step,
        }
    }
}

impl BigQuerySchema for StepName {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl Display for StepName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.plan, self.run, self.step)
    }
}

impl Serialize for StepName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone)]
pub struct JobName {
    pub plan: Arc<String>,
    pub run: KsuidMs,
    pub step: Arc<String>,
    pub job: IterableKey,
}

impl JobName {
    pub fn with_run(run: RunName, step: Arc<String>, job: IterableKey) -> Self {
        Self {
            plan: run.plan,
            run: run.run,
            step,
            job,
        }
    }

    pub fn run_name(&self) -> RunName {
        RunName {
            plan: self.plan.clone(),
            run: self.run,
        }
    }

    pub fn into_step_name(self) -> StepName {
        StepName {
            plan: self.plan,
            run: self.run,
            step: self.step,
        }
    }

    pub fn step_name(&self) -> StepName {
        StepName {
            plan: self.plan.clone(),
            run: self.run,
            step: self.step.clone(),
        }
    }
}

impl BigQuerySchema for JobName {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl Display for JobName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}.{}", self.plan, self.run, self.step, self.job)
    }
}

impl Serialize for JobName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolName {
    pub plan: Arc<String>,
    pub run: KsuidMs,
    pub step: Arc<String>,
    pub job: IterableKey,
    pub protocol: ProtocolDiscriminants,
}

impl ProtocolName {
    pub fn with_job(job: JobName, protocol: ProtocolDiscriminants) -> Self {
        Self {
            plan: job.plan,
            run: job.run,
            step: job.step,
            job: job.job,
            protocol,
        }
    }
}

impl BigQuerySchema for ProtocolName {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl Display for ProtocolName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}.{}",
            self.plan, self.run, self.step, self.job, self.protocol
        )
    }
}

impl Serialize for ProtocolName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone)]
pub struct PduName {
    pub plan: Arc<String>,
    pub run: KsuidMs,
    pub step: Arc<String>,
    pub job: IterableKey,
    pub protocol: ProtocolDiscriminants,
    pub pdu: u64,
}

impl PduName {
    pub fn with_protocol(proto: ProtocolName, pdu: u64) -> Self {
        Self {
            plan: proto.plan,
            run: proto.run,
            step: proto.step,
            job: proto.job,
            protocol: proto.protocol,
            pdu,
        }
    }

    pub fn with_job(job: JobName, protocol: ProtocolDiscriminants, pdu: u64) -> Self {
        Self {
            plan: job.plan,
            run: job.run,
            step: job.step,
            job: job.job,
            protocol,
            pdu,
        }
    }
}

impl BigQuerySchema for PduName {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl Display for PduName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}.{}.{}",
            self.plan, self.run, self.step, self.job, self.protocol, self.pdu,
        )
    }
}

impl Serialize for PduName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}
