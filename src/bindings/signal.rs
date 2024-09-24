use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{Merge, Points, Validate, ValueOrArray};

pub type SignalPoints = ValueOrArray<Points<SignalValue>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SignalValue {
    pub target: Option<super::Value>,
    pub op: Option<super::Value>,
    pub offset_bytes: Option<super::Value>,
    pub before: Option<String>,
    pub after: Option<String>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Validate for SignalValue {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

impl Merge for SignalValue {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            target: super::Value::merge(first.target, second.target),
            op: super::Value::merge(first.op, second.op),
            offset_bytes: super::Value::merge(first.offset_bytes, second.offset_bytes),
            before: first.before.or(second.before),
            after: first.after.or(second.after),
            unrecognized: toml::Table::new(),
        })
    }
}