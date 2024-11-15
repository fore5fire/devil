use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{LocationValue, Merge, Validate, Value};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PauseValue {
    pub before: Option<LocationValue>,
    pub after: Option<LocationValue>,
    pub duration: Option<Value>,
    pub r#await: Option<Value>,
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
            before: LocationValue::merge(first.before, second.before),
            after: LocationValue::merge(first.after, second.after),
            duration: super::Value::merge(first.duration, second.duration),
            r#await: first.r#await.or(second.r#await),
            unrecognized: toml::Table::new(),
        })
    }
}
