use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{Merge, Points, Validate, ValueOrArray};

pub type PausePoints = ValueOrArray<Points<PauseValue>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PauseValue {
    pub duration: Option<super::Value>,
    pub offset_bytes: Option<super::Value>,
    pub r#await: Option<ValueOrArray<String>>,
    pub before: Option<String>,
    pub after: Option<String>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl Validate for PauseValue {
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

impl Merge for PauseValue {
    fn merge(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        let Some(first) = first else { return second };
        let Some(second) = second else {
            return Some(first);
        };

        Some(Self {
            duration: super::Value::merge(first.duration, second.duration),
            offset_bytes: super::Value::merge(first.offset_bytes, second.offset_bytes),
            r#await: first.r#await.or(second.r#await),
            before: first.before.or(second.before),
            after: first.after.or(second.after),
            unrecognized: toml::Table::new(),
        })
    }
}
