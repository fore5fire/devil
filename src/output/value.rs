use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::bail;
use cel_interpreter::objects::{Key, Map};
use cel_interpreter::{Duration, Timestamp, Value};
use itertools::Itertools;
use serde::Serialize;

// TODO: implement a macro to generate TryIntoValue rather than using Serialize to avoid
// unnecessary copies and translations to and from this serializable variant of
// cel_interpreter::Value.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum OutValue {
    List(Vec<OutValue>),
    Map(OutMap),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(Arc<String>),
    Bytes(Arc<Vec<u8>>),
    Bool(bool),
    Duration(Duration),
    Timestamp(Timestamp),
    Null,
}

impl TryFrom<Value> for OutValue {
    type Error = crate::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::List(l) => Self::List(
                Arc::unwrap_or_clone(l)
                    .into_iter()
                    .map(Self::try_from)
                    .try_collect()?,
            ),
            Value::Map(m) => Self::Map(m.try_into()?),
            Value::Int(i) => Self::Int(i),
            Value::UInt(u) => Self::UInt(u),
            Value::Float(f) => Self::Float(f),
            Value::String(s) => Self::String(s),
            Value::Bytes(b) => Self::Bytes(b),
            Value::Bool(b) => Self::Bool(b),
            Value::Duration(dur) => Self::Duration(dur.into()),
            Value::Timestamp(ts) => Self::Timestamp(ts.into()),
            Value::Null => Self::Null,
            _ => bail!("unsupported value: {value:?}"),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OutMap {
    pub map: HashMap<OutKey, OutValue>,
}

impl TryFrom<Map> for OutMap {
    type Error = crate::Error;
    fn try_from(value: Map) -> Result<Self, Self::Error> {
        Ok(Self {
            map: Arc::unwrap_or_clone(value.map)
                .into_iter()
                .map(|(k, v)| Ok::<_, crate::Error>((k.into(), v.try_into()?)))
                .try_collect()?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Ord, Clone, PartialOrd, Serialize)]
pub enum OutKey {
    Int(i64),
    Uint(u64),
    Bool(bool),
    String(Arc<String>),
}

impl From<Key> for OutKey {
    fn from(value: Key) -> Self {
        match value {
            Key::Int(i) => Self::Int(i),
            Key::Uint(u) => Self::Uint(u),
            Key::Bool(b) => Self::Bool(b),
            Key::String(s) => Self::String(s),
        }
    }
}

impl From<OutKey> for Key {
    fn from(value: OutKey) -> Self {
        match value {
            OutKey::Int(i) => Self::Int(i),
            OutKey::Uint(u) => Self::Uint(u),
            OutKey::Bool(b) => Self::Bool(b),
            OutKey::String(s) => Self::String(s),
        }
    }
}
