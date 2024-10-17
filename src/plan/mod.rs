mod graphql;
mod http;
mod http1;
mod raw_http2;
mod http2;
mod http3;
mod tls;
mod tcp;
mod raw_tcp;
mod udp;
mod quic;
pub mod location;

pub use graphql::*;
pub use http::*;
pub use http1::*;
use location::{HttpLocation, Side};
pub use raw_http2::*;
pub use http2::*;
pub use http3::*;
pub use tls::*;
pub use udp::*;
pub use quic::*;
pub use tcp::*;
pub use raw_tcp::*;

use crate::bindings::{EnumKind, Literal, ValueOrArray};
use crate::{
    bindings, cel_functions, Error, LocationOutput, LocationValueOutput, Regex, Result, SignalOp, State, StepPlanOutput, TcpSegmentOptionOutput 
};
use anyhow::{anyhow, bail};
use base64::Engine;
use cel_interpreter::{Duration, Context, Program};
use chrono::{NaiveDateTime, TimeDelta, TimeZone};
use go_parse_duration::parse_duration;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Serialize;
use std::convert::Infallible;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::OnceLock;
use std::{collections::HashMap, ops::Deref, sync::Arc};
use tokio::sync::Semaphore;
use url::Url;

#[derive(Debug)]
pub struct Plan {
    pub steps: IndexMap<String, Step>,
    pub locals: IndexMap<String, PlanValue<PlanData, Infallible>>,
}

impl<'a> Plan {
    pub fn parse(input: &'a str) -> Result<Self> {
        let parsed = bindings::Plan::parse(input)?;
        Self::from_binding(parsed)
    }

    pub fn from_binding(mut plan: bindings::Plan) -> Result<Self> {
        static IMPLICIT_DEFUALTS: OnceLock<bindings::Plan> = OnceLock::new();

        // Apply the implicit defaults to the user defaults.
        let implicit_defaults = IMPLICIT_DEFUALTS.get_or_init(|| {
            let raw = include_str!("../implicit_defaults.cp.toml");
            toml::de::from_str::<bindings::Plan>(raw).unwrap()
        });
        plan.devil
            .defaults
            .extend(implicit_defaults.devil.defaults.clone());
        // Generate final steps.
        let steps: IndexMap<String, Step> = plan
            .steps
            .into_iter()
            .map(|(name, value)| {
                // Apply the user and implicit defaults.
                let value = value.apply_defaults(plan.devil.defaults.clone());
                // Apply planner requirements and convert to planner structure.
                Ok((name, Step::from_bindings(value)?))
            })
            .collect::<Result<_>>()?;
        let locals = plan
            .devil
            .locals
            .into_iter()
            .map(|(k, v)| Ok((k, PlanValue::try_from(v)?)))
            .collect::<Result<_>>()?;

        Ok(Plan { steps, locals })
    }
}

#[derive(Debug)]
pub enum HttpVersion {
    HTTP0_9,
    HTTP1_0,
    HTTP1_1,
    HTTP2,
    HTTP3,
}

#[derive(Debug, Clone)]
pub struct LocationValue {
    id: PlanValue<location::Location>,
    offset_bytes: PlanValue<Option<usize>>,
}

impl TryFrom<bindings::LocationValue> for LocationValue {
    type Error = Error;
    fn try_from(binding: bindings::LocationValue) -> Result<Self> {
        Ok(Self {
            id: binding.id.map(PlanValue::try_from).ok_or_else(|| anyhow!("location id is required"))??,
            offset_bytes: binding.offset_bytes.try_into()?,
        })
    }
}


impl Evaluate<LocationValueOutput> for LocationValue {
fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<LocationValueOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O> {
    Ok(LocationValueOutput {
        id: self.id.evaluate(state)?,
        offset_bytes: self.offset_bytes.evaluate(state)?.unwrap_or_default(),
    })
}
}

#[derive(Debug, Clone)]
pub enum Location {
    Before(LocationValue),
    After(LocationValue),
}

impl Location {
    fn from_bindings(before: Option<bindings::LocationValue>, after: Option<bindings::LocationValue>) -> Result<Self> {
        match (before, after) {
            (Some(loc), None) => Ok(Self::Before(loc.try_into()?)),
            (None, Some(loc)) => Ok(Self::After(loc.try_into()?)),
            _ => bail!("exactly one of before or after is required")
        }
    }
}

impl Evaluate<LocationOutput> for Location {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<LocationOutput>
        where
            S: State<'a, O, I>,
            O: Into<&'a str>,
            I: IntoIterator<Item = O> {
                match self {
                    Self::Before(loc) => Ok(LocationOutput::Before(loc.evaluate(state)?)),
                    Self::After(loc) => Ok(LocationOutput::After(loc.evaluate(state)?)),
                }
    }
}

#[derive(Debug, Clone)]
pub struct PauseValue {
    pub location: Location,
    pub duration: PlanValue<Option<Duration>>,
    pub r#await: PlanValue<Option<String>>,
}

impl TryFrom<bindings::PauseValue> for PauseValue {
    type Error = Error;
    fn try_from(binding: bindings::PauseValue) -> Result<Self> {
        Ok(Self {
            location: Location::from_bindings(binding.before, binding.after)?,
            duration: binding.duration.try_into()?,
            r#await: binding.r#await.try_into()?,
        })
    }
}

impl Evaluate<crate::PauseValueOutput> for PauseValue {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::PauseValueOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let out = crate::PauseValueOutput {
            location: self.location.evaluate(state)?,
            duration: self.duration.evaluate(state)?.unwrap_or_default(),
            r#await: self.r#await.evaluate(state)?,
        };
        match out.location.value().id {
            location::Location::Http(HttpLocation::ResponseHeaders, Side::End) if out.location.value().offset_bytes < 0 => {
                bail!(
                    "http.pause.response_headers.end with negative offset is not supported"
                );
            }
            location::Location::Http(HttpLocation::ResponseHeaders, Side::Start) if out.location.value().offset_bytes < 0 => {
                bail!(
                    "http.pause.response_headers.start with negative offset is not supported"
                );
            }
            _ => Ok(out)
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignalValue {
    pub target: PlanValue<String>,
    pub op: PlanValue<SignalOp>,
    pub location: Location,
}

impl TryFrom<bindings::SignalValue> for SignalValue {
    type Error = Error;
    fn try_from(binding: bindings::SignalValue) -> Result<Self> {
        Ok(Self {
            target: binding.target.map(PlanValue::try_from).ok_or_else(|| anyhow!("signal target is required"))??,
            op: binding.op.map(PlanValue::try_from).ok_or_else(|| anyhow!("signal op is required"))??,
            location: Location::from_bindings(binding.before, binding.after)?,
        })
    }
}

impl Evaluate<crate::SignalValueOutput> for SignalValue {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::SignalValueOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let out = crate::SignalValueOutput {
            target: self.target.evaluate(state)?,
            op: self.op.evaluate(state)?,
            location: self.location.evaluate(state)?,
        };
        match out.location.value().id {
            location::Location::Http(HttpLocation::ResponseHeaders, Side::End) if out.location.value().offset_bytes < 0 => {
                bail!(
                    "http.pause.response_headers.end with negative offset is not supported"
                );
            }
            location::Location::Http(HttpLocation::ResponseHeaders, Side::Start) if out.location.value().offset_bytes < 0 => {
                bail!(
                    "http.pause.response_headers.start with negative offset is not supported"
                );
            }
            _ => Ok(out)
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanData(pub cel_interpreter::Value);

trait TryFromPlanData: Sized {
    type Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error>;
}

impl TryFromPlanData for PlanData {
    type Error = Infallible;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        Ok(value)
    }
}

impl<T: TryFromPlanData> TryFromPlanData for Option<T> {
    type Error = T::Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Null => Ok(None),
            val => Ok(Some(T::try_from_plan_data(PlanData(val))?)),
        }
    }
}

impl TryFromPlanData for String {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::Bytes(x) => Ok(String::from_utf8_lossy(&x).to_string()),
            val => bail!("{val:?} has invalid value for string value"),
        }
    }
}

impl TryFromPlanData for u8 {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u8::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u8::try_from(x)?)
            }
            val => bail!(
                "{val:?} has invalid value for 8 bit unsigned int value",
            ),
        }
    }
}

impl TryFromPlanData for u16 {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u16::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u16::try_from(x)?)
            }
            val => bail!(
                "{val:?} has invalid value for 16 bit unsigned int value",
            ),
        }
    }
}

impl TryFromPlanData for u32 {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u32::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u32::try_from(x)?)
            }
            val => bail!(
                "{val:?} has invalid value for 32 bit unsigned int value",
            ),
        }
    }
}

impl TryFromPlanData for u64 {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(u64::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(u64::try_from(x)?)
            }
            val => bail!(
                "{val:?} has invalid type for 64 bit unsigned int value",
            ),
        }
    }
}

impl TryFromPlanData for i64 {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(i64::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => Ok(x),
            val => bail!(
                "{val:?} has invalid type for 64 bit signed int value",
            ),
        }
    }
}

impl TryFromPlanData for usize {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::UInt(x) => {
                Ok(usize::try_from(x)?)
            }
            cel_interpreter::Value::Int(x) => {
                Ok(usize::try_from(x)?)
            }
            val => bail!(
                "{val:?} has invalid type for 64 bit unsigned int value",
            ),
        }
    }
}

impl TryFromPlanData for bool {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bool(x) => Ok(x),
            val => bail!("{val:?} has invalid type for bool value"),
        }
    }
}

impl TryFromPlanData for Vec<u8> {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::Bytes(x) => Ok(x.deref().clone()),
            cel_interpreter::Value::String(x) => Ok(x.deref().clone().into_bytes()),
            val => bail!("{val:?} has invalid type for bytes value"),
        }
    }
}

impl TryFromPlanData for Duration {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) => parse_duration(&x)
                .map(TimeDelta::nanoseconds)
                .map(Duration)
                .map_err(|e| match e {
                    go_parse_duration::Error::ParseError(s) => anyhow!(s),
                }),
            cel_interpreter::Value::Duration(x) => Ok(Duration(x)),
            val => bail!(
                "{val:?} has invalid type for duration value",
            ),
        }
    }
}

impl TryFromPlanData for Regex {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) => Ok(Regex::new(x)?),
            val => bail!(
                "{val:?} has invalid type for duration value",
            ),
        }
    }
}

impl TryFromPlanData for location::Location {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) => Ok(Self::from_str(&x)?),
            val => bail!(
                "{val:?} has invalid type for location value",
            ),
        }
    }
}

impl TryFromPlanData for SignalOp {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::String(x) => Ok(Self::try_from_str(&x)?),
            val => bail!(
                "{val:?} has invalid type for location value",
            ),
        }
    }
}

impl TryFromPlanData for TcpSegmentOptionOutput {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        match value.0 {
            cel_interpreter::Value::Map(x) => match x.get(&Self::KIND_KEY.into()) {
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::NOP_KIND => {
                    Ok(Self::Nop)
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::TIMESTAMPS_KIND => {
                    Ok(Self::Timestamps {
                        tsval: x
                            .get(&Self::TSVAL_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => Ok(u32::try_from(*val)?),
                                cel_interpreter::Value::Int(val) => Ok(u32::try_from(*val)?),
                                _ => bail!("tcp segment option timestamps `tsval` must be convertible to 32 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option timestamps missing `tsval`",
                            ))??
                            .into(),
                        tsecr: x
                            .get(&Self::TSECR_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => Ok(u32::try_from(*val)?),
                                cel_interpreter::Value::Int(val) => Ok(u32::try_from(*val)?),
                                _ => bail!("tcp segment option timestamps `tsecr` must be convertible to 32 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option timestamps missing `tsecr`",
                            ))??
                            .into(),
                    })
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::MSS_KIND => {
                    Ok(Self::Mss(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => Ok(u16::try_from(*val)?),
                                cel_interpreter::Value::Int(val) => Ok(u16::try_from(*val)?),
                                _ => bail!("tcp segment option mss value must be convertible to 16 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option mss missing value",
                            ))??))
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::WSCALE_KIND => {
                    Ok(Self::Wscale(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::UInt(val) => Ok(u8::try_from(*val)?),
                                cel_interpreter::Value::Int(val) => Ok(u8::try_from(*val)?),
                                _ => bail!("tcp segment option wscale value must be convertible to 8 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option wscale missing value",
                            ))??))
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::SACK_PERMITTED_KIND => {
                    Ok(Self::SackPermitted)
                }
                Some(cel_interpreter::Value::String(kind)) if kind.as_str() == Self::SACK_KIND => {
                    Ok(Self::Sack(x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::List(vals) => vals.iter().map(|x| match x {
                                    cel_interpreter::Value::Int(val) => Ok(u32::try_from(*val)?),
                                    cel_interpreter::Value::UInt(val) => Ok(u32::try_from(*val)?),
                                    _ => bail!("tcp segment option sack must be convertible to list of 32 bit unsigned int")
                                }).try_collect(),
                                _ => bail!("tcp segment option sack value must be convertible to list of 32 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option wscale missing value",
                            ))??))
                }
                Some(cel_interpreter::Value::UInt(kind)) => {
                    Ok(Self::Generic{ kind: u8::try_from(*kind)?,
                        value: x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::Bytes(data) => Ok(data.as_ref().to_owned()),
                                cel_interpreter::Value::String(data) => Ok(data.as_bytes().to_vec()),
                                _ => bail!("tcp segment option sack value must be convertible to list of 32 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option raw missing value",
                            ))??})
                }
                Some(cel_interpreter::Value::Int(kind)) => {
                    Ok(Self::Generic{ kind: u8::try_from(*kind)?,
                        value: x.get(&Self::VALUE_KEY.into())
                            .map(|x| match x {
                                cel_interpreter::Value::Bytes(data) => Ok(data.as_ref().to_owned()),
                                cel_interpreter::Value::String(data) => Ok(data.as_bytes().to_vec()),
                                _ => bail!("tcp segment option sack value must be convertible to list of 32 bit unsigned int"),
                            })
                            .ok_or_else(|| anyhow!(
                                "tcp segment option raw missing value",
                            ))??})
                }
                _ => bail!(
                    "tcp segment option expression result requires string value for key `kind`",
                ),
            },
            cel_interpreter::Value::String(x) => match x.as_str() {
                "nop" => Ok(Self::Nop),
                "sack_permitted" => Ok(Self::SackPermitted),
                val => bail!("invalid TLS version {val:?}"),
            },
            _ => bail!(
                "TCP segment option must be a string or map",
            ),
        }
    }
}

impl TryFromPlanData for Url {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        let cel_interpreter::Value::String(x) = value.0 else {
            bail!("URL must be a string");
        };
        Ok(Url::parse(&x)?)
    }
}

impl TryFromPlanData for serde_json::Value {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> Result<Self> {
        Ok(match value.0 {
            cel_interpreter::Value::List(l) => Self::Array(
                Arc::try_unwrap(l)
                    .unwrap_or_else(|l| (*l).clone())
                    .into_iter()
                    .map(PlanData)
                    .map(Self::try_from_plan_data)
                    .try_collect()?,
            ),
            cel_interpreter::Value::Map(m) => Self::Object(
                Arc::try_unwrap(m.map)
                    .unwrap_or_else(|m| (*m).clone())
                    .into_iter()
                    .map(|(k, v)| {
                        let cel_interpreter::objects::Key::String(k) = k else {
                            bail!(
                                "only string keys may be used in json output",
                            );
                        };
                        Ok((
                            Arc::try_unwrap(k).unwrap_or_else(|k| (*k).clone()),
                            Self::try_from_plan_data(PlanData(v))?,
                        ))
                    })
                    .try_collect()?,
            ),
            cel_interpreter::Value::Int(n) => Self::Number(serde_json::Number::from(n)),
            cel_interpreter::Value::UInt(n) => Self::Number(serde_json::Number::from(n)),
            cel_interpreter::Value::Float(n) => {
                Self::Number(serde_json::Number::from_f64(n).ok_or_else(|| {
                    anyhow!("json input number fields cannot contain infinity".to_owned())
                })?)
            }
            cel_interpreter::Value::String(s) => {
                Self::String(Arc::try_unwrap(s).unwrap_or_else(|s| (*s).clone()))
            }
            cel_interpreter::Value::Bytes(b) => {
                Self::String(String::from_utf8_lossy(b.as_slice()).to_string())
            }
            cel_interpreter::Value::Bool(b) => Self::Bool(b),
            cel_interpreter::Value::Timestamp(ts) => Self::String(ts.to_rfc3339()),
            cel_interpreter::Value::Null => Self::Null,
            _ => bail!("no mapping to json"),
        })
    }
}

impl From<cel_interpreter::Value> for PlanData {
    fn from(value: cel_interpreter::Value) -> Self {
        PlanData(value)
    }
}

impl From<PlanData> for cel_interpreter::Value {
    fn from(value: PlanData) -> Self {
        value.0
    }
}

impl From<String> for PlanData {
    fn from(value: String) -> Self {
        PlanData(value.into())
    }
}

impl<T: TryInto<PlanData>> TryFrom<Option<T>> for PlanData {
    type Error = T::Error;
    fn try_from(value: Option<T>) -> std::result::Result<Self, Self::Error> {
        if let Some(v) = value {
            v.try_into()
        } else {
            Ok(PlanData(cel_interpreter::Value::Null))
        }
    }
}

impl TryFrom<toml::value::Datetime> for PlanData {
    type Error = Error;
    fn try_from(value: toml::value::Datetime) -> std::result::Result<Self, Self::Error> {
        use chrono::FixedOffset;
        use chrono::Offset;

        let date = value
            .date
            .map(|date| {
                chrono::NaiveDate::from_ymd_opt(
                    date.year as i32,
                    date.month as u32,
                    date.day as u32,
                )
                .ok_or_else(|| anyhow!("out of bounds date"))
            })
            .transpose()?
            .unwrap_or_default();

        let time = value
            .time
            .map(|time| {
                chrono::NaiveTime::from_hms_nano_opt(
                    time.hour as u32,
                    time.minute as u32,
                    time.second as u32,
                    time.nanosecond,
                )
                .ok_or_else(|| anyhow!("out of bounds time"))
            })
            .transpose()?
            .unwrap_or_default();

        let datetime = NaiveDateTime::new(date, time);

        let offset = match value.offset {
            Some(toml::value::Offset::Custom { minutes }) => {
                FixedOffset::east_opt(minutes as i32 * 60)
                    .ok_or_else(|| anyhow!("invalid offset"))?
            }
            Some(toml::value::Offset::Z) => chrono::Utc.fix(),
            None => chrono::Local
                .offset_from_local_datetime(&datetime)
                .single()
                .ok_or_else(|| anyhow!("ambiguous datetime"))?,
        };

        Ok(PlanData(
            cel_interpreter::Value::Timestamp(offset
                .from_local_datetime(&datetime)
                .single()
                .ok_or_else(|| anyhow!("ambiguous datetime"))?
                ),
        ))
    }
}

impl TryFrom<toml::Value> for PlanData {
    type Error = Error;
    fn try_from(value: toml::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            toml::Value::String(s) => Ok(Self(cel_interpreter::Value::String(s.into()))),
            toml::Value::Integer(s) => Ok(Self(cel_interpreter::Value::Int(s.into()))),
            toml::Value::Float(s) => Ok(Self(cel_interpreter::Value::Float(s.into()))),
            toml::Value::Boolean(s) => Ok(Self(cel_interpreter::Value::Bool(s.into()))),
            toml::Value::Datetime(s) => Ok(Self(PlanData::try_from(s)?.0)),
            toml::Value::Array(s) => Ok(Self(cel_interpreter::Value::List(Arc::new(
                s.into_iter()
                    .map(|x| Ok(PlanData::try_from(x)?.0))
                    .collect::<Result<_>>()?,
            )))),
            toml::Value::Table(s) => Ok(Self(cel_interpreter::Value::Map(
                cel_interpreter::objects::Map {
                    map: Arc::new(
                        s.into_iter()
                            .map(|(k, v)| {
                                Ok((
                                    cel_interpreter::objects::Key::from(k),
                                    PlanData::try_from(v)?.0,
                                ))
                            })
                            .collect::<Result<_>>()?,
                    ),
                },
            ))),
        }
    }
}


#[derive(Debug, Default, Clone)]
pub struct WebsocketRequest {}

#[derive(Debug, Clone)]
pub struct Step {
    pub protocols: StepProtocols,
    pub run: Run,
    pub sync: IndexMap<String, Synchronizer>,
    pub pause: IndexMap<String, PauseValue>,
    pub signal: IndexMap<String, SignalValue>,
}

impl Step {
    pub fn from_bindings(binding: bindings::Step) -> Result<Step> {
        let protocols = match binding.protocols {
            bindings::StepProtocols::GraphQl { graphql, http } => StepProtocols::GraphQlHttp {
                graphql: graphql.try_into()?,
                http: http.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                raw_tcp,
            } => StepProtocols::GraphQlH1c {
                graphql: graphql.try_into()?,
                h1c: h1c.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::GraphQlH1 {
                graphql: graphql.try_into()?,
                h1: h1.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH2c {
                graphql,
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => StepProtocols::GraphQlH2c {
                graphql: graphql.try_into()?,
                h2c: h2c.unwrap_or_default().try_into()?,
                raw_h2c: raw_h2c.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH2 {
                graphql,
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::GraphQlH2 {
                graphql: graphql.try_into()?,
                h2: h2.unwrap_or_default().try_into()?,
                raw_h2: raw_h2.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => StepProtocols::GraphQlH3 {
                graphql: graphql.try_into()?,
                h3: h3.unwrap_or_default().try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Http { http } => StepProtocols::Http {
                http: http.try_into()?,
            },
            bindings::StepProtocols::H1c {
                h1c,
                tcp,
                raw_tcp,
            } => StepProtocols::H1c {
                h1c: h1c.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H1 {
                h1,
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::H1 {
                h1: h1.try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H2c {
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => StepProtocols::H2c {
                h2c: h2c.try_into()?,
                raw_h2c: raw_h2c.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H2 {
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::H2 {
                h2: h2.try_into()?,
                raw_h2: raw_h2.unwrap_or_default().try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::H3 { h3, quic, udp } => StepProtocols::H3 {
                h3: h3.try_into()?,
                quic: quic.unwrap_or_default().try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::RawH2c {
                raw_h2c,
                tcp,
                raw_tcp,
            } => StepProtocols::RawH2c {
                raw_h2c: raw_h2c.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::RawH2 {
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::RawH2 {
                raw_h2: raw_h2.try_into()?,
                tls: tls.unwrap_or_default().try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tls {
                tls,
                tcp,
                raw_tcp,
            } => StepProtocols::Tls {
                tls: tls.try_into()?,
                tcp: tcp.unwrap_or_default().try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Dtls { dtls, udp } => StepProtocols::Dtls {
                dtls: dtls.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Tcp { tcp, raw_tcp } => StepProtocols::Tcp {
                tcp: tcp.try_into()?,
                raw_tcp: raw_tcp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::RawTcp { raw_tcp } => StepProtocols::RawTcp {
                raw_tcp: raw_tcp.try_into()?,
            },
            bindings::StepProtocols::Quic { quic, udp } => StepProtocols::Quic {
                quic: quic.try_into()?,
                udp: udp.unwrap_or_default().try_into()?,
            },
            bindings::StepProtocols::Udp { udp } => StepProtocols::Udp {
                udp: udp.try_into()?,
            },
        };

        Ok(Step {
            protocols,
            sync: binding.sync.into_iter().map(|(k, v)| Ok::<_, crate::Error>((k, <Synchronizer>::try_from(v)?))).try_collect()?,
            pause: binding.pause.into_iter().map(|(k, v)| Ok::<_, crate::Error>((k, <PauseValue>::try_from(v)?))).try_collect()?,
            signal: binding.signal.into_iter().map(|(k, v)| Ok::<_, crate::Error>((k, <SignalValue>::try_from(v)?))).try_collect()?,
            run: binding
                .run
                .map(|run| {
                    Ok::<_, Error>(Run {
                        count: run
                            .count
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or_else(|| {
                                // The default count shouldn't inhibit looping when while or for is
                                // set, but also shouldn't cause looping if neither are set.
                                PlanValue::Literal(
                                    if run.run_while.is_some() || run.run_for.is_some() {
                                        u64::MAX
                                    } else {
                                        1
                                    },
                                )
                            }),
                        run_if: run
                            .run_if
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or(PlanValue::Literal(true)),
                        run_while: run.run_while.map(PlanValue::try_from).transpose()?,
                        run_for: run
                            .run_for
                            .map(|x| IterablePlanValue::try_from(x))
                            .transpose()?,
                        parallel: run
                            .parallel
                            .map(PlanValue::try_from)
                            .transpose()?
                            .unwrap_or_default(),
                        share: run.share.try_into()?,
                    })
                })
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone)]
pub enum Synchronizer {
    Barrier{ count: PlanValue<usize> },
    Mutex,
    PriorityMutex,
    Semaphore{ permits: PlanValue<usize> },
    PrioritySemaphore{ permits: PlanValue<usize> },
}

impl TryFrom<bindings::Sync> for Synchronizer {
    type Error = Error;
    fn try_from(value: bindings::Sync) -> std::result::Result<Self, Self::Error> {
        match value {
            bindings::Sync::Barrier{ count } => Ok(Self::Barrier { count: count.try_into()? }),
            bindings::Sync::Mutex => Ok(Self::Mutex),
            bindings::Sync::PriorityMutex => Ok(Self::PriorityMutex),
            bindings::Sync::Semaphore{ permits } => Ok(Self::Semaphore { permits: permits.try_into()? }),
            bindings::Sync::PrioritySemaphore{ permits } => Ok(Self::PrioritySemaphore { permits: permits.try_into()? }),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub enum Parallelism {
    #[default]
    Serial,
    Parallel(usize),
    Pipelined,
}

impl FromStr for Parallelism {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "serial" => Ok(Self::Serial),
            "parallel" => Ok(Self::Parallel(Semaphore::MAX_PERMITS)),
            "pipelined" => Ok(Self::Pipelined),
            val => bail!("unrecognized parallelism string {val}"),
        }
    }
}

impl TryFromPlanData for Parallelism {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            cel_interpreter::Value::Bool(b) if b => {
                Ok(Parallelism::Parallel(Semaphore::MAX_PERMITS))
            }
            cel_interpreter::Value::Bool(_) => Ok(Parallelism::Serial),
            cel_interpreter::Value::Int(i) => {
                Ok(Parallelism::Parallel(i.try_into().map_err(|_| {
                    anyhow!(
                        "parallelism value {i} must fit in platform word size"
                    )
                })?))
            }
            cel_interpreter::Value::UInt(i) => {
                Ok(Parallelism::Parallel(i.try_into().map_err(|_| {
                    anyhow!(
                        "parallelism value {i} must fit in platform word size"
                    )
                })?))
            }
            val => bail!(
                "unsupported value {val:?} for field run.parallel"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub run_if: PlanValue<bool>,
    pub run_while: Option<PlanValue<bool>>,
    pub run_for: Option<IterablePlanValue>,
    pub count: PlanValue<u64>,
    pub parallel: PlanValue<Parallelism>,
    pub share: PlanValue<Option<ProtocolField>>,
}

impl Default for Run {
    fn default() -> Self {
        Run {
            run_if: PlanValue::Literal(true),
            run_while: None,
            run_for: None,
            count: PlanValue::Literal(1),
            parallel: PlanValue::default(),
            share: PlanValue::default(),
        }
    }
}

impl Evaluate<crate::RunPlanOutput> for Run {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<crate::RunPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let out = crate::RunPlanOutput {
            run_if: self.run_if.evaluate(state)?,
            run_while: self
                .run_while.evaluate(state)?,
            run_for: self
                .run_for
                .evaluate(state)?
                .map(|pairs| pairs
                    .into_iter()
                    .map(|(key, v)| Ok::<_,crate::Error>(crate::RunForOutput {
                        key, 
                        value: v.0.try_into()?,
                    })).try_collect())
                .transpose()?,
            count: self.count.evaluate(state)?,
            parallel: self.parallel.evaluate(state)?,
            share: self.share.evaluate(state)?,
        };
        // Only one of while or for may be used.
        if out.run_while.is_some() && out.run_for.is_some() {
            bail!("run.while and run.for cannot both be set");
        }
        // While cannot be parallel.
        if !matches!(out.parallel, Parallelism::Serial) && out.run_while.is_some() {
            bail!("run.while cannot be parallel");
        }

        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub enum StepProtocols {
    GraphQlHttp {
        graphql: GraphQlRequest,
        http: HttpRequest,
    },
    GraphQlH1c {
        graphql: GraphQlRequest,
        h1c: Http1Request,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    GraphQlH1 {
        graphql: GraphQlRequest,
        h1: Http1Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    GraphQlH2c {
        graphql: GraphQlRequest,
        h2c: Http2Request,
        raw_h2c: RawHttp2Request,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    GraphQlH2 {
        graphql: GraphQlRequest,
        h2: Http2Request,
        raw_h2: RawHttp2Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    GraphQlH3 {
        graphql: GraphQlRequest,
        h3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Http {
        http: HttpRequest,
    },
    H1c {
        h1c: Http1Request,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    H1 {
        h1: Http1Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    H2c {
        h2c: Http2Request,
        raw_h2c: RawHttp2Request,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    H2 {
        h2: Http2Request,
        raw_h2: RawHttp2Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    H3 {
        h3: Http3Request,
        quic: QuicRequest,
        udp: UdpRequest,
    },
    RawH2c {
        raw_h2c: RawHttp2Request,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    RawH2 {
        raw_h2: RawHttp2Request,
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    Tls {
        tls: TlsRequest,
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    Dtls {
        dtls: TlsRequest,
        udp: UdpRequest,
    },
    Tcp {
        tcp: TcpRequest,
        raw_tcp: RawTcpRequest,
    },
    RawTcp {
        raw_tcp: RawTcpRequest,
    },
    Quic {
        quic: QuicRequest,
        udp: UdpRequest,
    },
    Udp {
        udp: UdpRequest,
    },
}

impl StepProtocols {
    pub fn into_stack(self) -> Vec<Protocol> {
        match self {
            Self::GraphQlHttp { graphql, http } => {
                vec![Protocol::GraphQl(graphql), Protocol::Http(http)]
            }
            Self::GraphQlH1c {
                graphql,
                h1c,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H1c(h1c),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::GraphQlH1 {
                graphql,
                h1,
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H1(h1),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::GraphQlH2c {
                graphql,
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H2c(h2c),
                    Protocol::RawH2c(raw_h2c),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::GraphQlH2 {
                graphql,
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H2(h2),
                    Protocol::RawH2(raw_h2),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::GraphQlH3 {
                graphql,
                h3,
                quic,
                udp,
            } => {
                vec![
                    Protocol::GraphQl(graphql),
                    Protocol::H3(h3),
                    Protocol::Quic(quic),
                    Protocol::Udp(udp),
                ]
            }
            Self::Http { http } => {
                vec![Protocol::Http(http)]
            }
            Self::H1c {
                h1c,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::H1c(h1c),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::H1 {
                h1,
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::H1(h1),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::H2c {
                h2c,
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::H2c(h2c),
                    Protocol::RawH2c(raw_h2c),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::H2 {
                h2,
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::H2(h2),
                    Protocol::RawH2(raw_h2),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::H3 { h3, quic, udp } => {
                vec![Protocol::H3(h3), Protocol::Quic(quic), Protocol::Udp(udp)]
            }
            Self::RawH2c {
                raw_h2c,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::RawH2c(raw_h2c),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::RawH2 {
                raw_h2,
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::RawH2(raw_h2),
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::Tls {
                tls,
                tcp,
                raw_tcp,
            } => {
                vec![
                    Protocol::Tls(tls),
                    Protocol::Tcp(tcp),
                    Protocol::RawTcp(raw_tcp),
                ]
            }
            Self::Dtls { dtls, udp } => {
                vec![Protocol::Tls(dtls), Protocol::Udp(udp)]
            }
            Self::Tcp { tcp, raw_tcp } => {
                vec![Protocol::Tcp(tcp), Protocol::RawTcp(raw_tcp)]
            }
            Self::RawTcp { raw_tcp } => {
                vec![Protocol::RawTcp(raw_tcp)]
            }
            Self::Quic { quic, udp } => {
                vec![Protocol::Udp(udp), Protocol::Quic(quic)]
            }
            Self::Udp { udp } => {
                vec![Protocol::Udp(udp)]
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Protocol {
    GraphQl(GraphQlRequest),
    Http(HttpRequest),
    H1c(Http1Request),
    H1(Http1Request),
    H2c(Http2Request),
    RawH2c(RawHttp2Request),
    H2(Http2Request),
    RawH2(RawHttp2Request),
    H3(Http3Request),
    Tls(TlsRequest),
    Tcp(TcpRequest),
    RawTcp(RawTcpRequest),
    Quic(QuicRequest),
    Udp(UdpRequest),
}

impl Protocol {
    pub fn field(&self) -> ProtocolField {
        match self {
            Self::GraphQl(_) => ProtocolField::GraphQl,
            Self::Http(_) => ProtocolField::Http,
            Self::H1c(_) => ProtocolField::H1c,
            Self::H1(_) => ProtocolField::H1,
            Self::H2c(_) => ProtocolField::H2c,
            Self::RawH2c(_) => ProtocolField::RawH2c,
            Self::H2(_) => ProtocolField::H2,
            Self::RawH2(_) => ProtocolField::RawH2,
            Self::H3(_) => ProtocolField::H3,
            Self::Tls(_) => ProtocolField::Tls,
            Self::Tcp(_) => ProtocolField::Tcp,
            Self::RawTcp(_) => ProtocolField::RawTcp,
            Self::Quic(_) => ProtocolField::Quic,
            Self::Udp(_) => ProtocolField::Udp,
        }
    }
}

impl Evaluate<StepPlanOutput> for Protocol {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<StepPlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(match self {
            Self::GraphQl(proto) => StepPlanOutput::GraphQl(proto.evaluate(state)?),
            Self::Http(proto) => StepPlanOutput::Http(proto.evaluate(state)?),
            Self::H1c(proto) => StepPlanOutput::H1c(proto.evaluate(state)?),
            Self::H1(proto) => StepPlanOutput::H1(proto.evaluate(state)?),
            Self::H2c(proto) => StepPlanOutput::H2c(proto.evaluate(state)?),
            Self::RawH2c(proto) => StepPlanOutput::RawH2c(proto.evaluate(state)?),
            Self::H2(proto) => StepPlanOutput::H2(proto.evaluate(state)?),
            Self::RawH2(proto) => StepPlanOutput::RawH2(proto.evaluate(state)?),
            //Self::Http3(proto) => ProtocolOutput::Http3(proto.evaluate(state)?),
            Self::Tls(proto) => StepPlanOutput::Tls(proto.evaluate(state)?),
            Self::Tcp(proto) => StepPlanOutput::Tcp(proto.evaluate(state)?),
            Self::RawTcp(proto) => StepPlanOutput::RawTcp(proto.evaluate(state)?),
            //Self::Quic(proto) => ProtocolOutput::Quic(proto.evaluate(state)?),
            //Self::Udp(proto) => ProtocolOutput::Udp(proto.evaluate(state)?),
            proto => {
                bail!("support for protocol {proto:?} is incomplete")
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ProtocolField {
    GraphQl,
    Http,
    H1c,
    H1,
    H2c,
    RawH2c,
    H2,
    RawH2,
    H3,
    Tls,
    Tcp,
    RawTcp,
    Dtls,
    Quic,
    Udp,
}

impl FromStr for ProtocolField {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.into() {
            "udp" => Ok(Self::Udp),
            "quic" => Ok(Self::Quic),
            "dtls" => Ok(Self::Dtls),
            "raw_tcp" => Ok(Self::RawTcp),
            "tcp" => Ok(Self::Tcp),
            "tls" => Ok(Self::Tls),
            "http" => Ok(Self::Http),
            "h1c" => Ok(Self::H1c),
            "h1" => Ok(Self::H1),
            "h2c" => Ok(Self::H2c),
            "raw_h2c" => Ok(Self::RawH2c),
            "h2" => Ok(Self::H2),
            "raw_h2" => Ok(Self::RawH2),
            "h3" => Ok(Self::H3),
            "graphql" => Ok(Self::GraphQl),
            _ => bail!("invalid tls version string {}", s),
        }
    }
}

impl TryFromPlanData for ProtocolField {
    type Error = Error;
    fn try_from_plan_data(value: PlanData) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            cel_interpreter::Value::String(s) => s.parse(),
            val => bail!("invalid value {val:?} for protocol reference"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PlanValue<T, E = Error>
where
    T: TryFromPlanData<Error = E> + Clone,
{
    Literal(T),
    Dynamic {
        cel: String,
        vars: Vec<(String, String)>,
    },
}

impl<T, E> Clone for PlanValue<T, E>
where
    T: TryFromPlanData<Error = E> + Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Literal(val) => Self::Literal(val.clone()),
            Self::Dynamic { cel, vars } => Self::Dynamic {
                cel: cel.clone(),
                vars: vars.clone(),
            },
        }
    }
}

impl<T, E> Default for PlanValue<T, E>
where
    T: TryFromPlanData<Error = E> + Clone + Default,
{
    fn default() -> Self {
        PlanValue::Literal(T::default())
    }
}

// Conversions from toml keys to PlanValue literals.
impl From<String> for PlanValue<String> {
    fn from(value: String) -> Self {
        Self::Literal(value)
    }
}
impl From<String> for PlanValue<Vec<u8>> {
    fn from(value: String) -> Self {
        Self::Literal(value.into_bytes())
    }
}
impl From<String> for PlanValue<PlanData, Infallible> {
    fn from(value: String) -> Self {
        Self::Literal(value.into())
    }
}

impl<T, E> TryFrom<bindings::Value> for Option<PlanValue<T, E>>
where
    T: TryFromPlanData<Error = E> + Clone,
    E: Into<anyhow::Error>,
    PlanValue<T, E>: TryFrom<bindings::Value, Error = Error>,
{
    type Error = Error;
    fn try_from(value: bindings::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            bindings::Value::Unset { .. } => Ok(None),
            value => Ok(Some(value.try_into()?)),
        }
    }
}

impl<T, E> TryFrom<Option<bindings::Value>> for PlanValue<Option<T>, E>
where
    T: TryFromPlanData<Error = E> + Clone,
    E: Into<anyhow::Error>,
    PlanValue<T, E>: TryFrom<bindings::Value, Error = Error>,
{
    type Error = Error;
    fn try_from(value: Option<bindings::Value>) -> std::result::Result<Self, Self::Error> {
        match value {
            Some(bindings::Value::Unset { .. }) | None => Ok(PlanValue::Literal(None)),
            value => Ok(value.try_into()?),
        }
    }
}

impl<T> TryFrom<bindings::Value> for PlanValue<T, <T as TryFromPlanData>::Error> where 
    T: TryFrom<Literal, Error = crate::Error> + TryFromPlanData + Clone {
    type Error = crate::Error;
    fn try_from(value: bindings::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            bindings::Value::Literal(l) => Ok(PlanValue::Literal(T::try_from(l)?)),
            bindings::Value::ExpressionCel { cel, vars } => Ok(Self::Dynamic { 
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::ExpressionVars { .. } => bail!("missing cel field"),
            bindings::Value::Unset { .. } => bail!("missing cel field"),
        }
    }
}

impl TryFrom<Literal> for String {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(x),
            _ => bail!(format!("invalid type {binding:?} for string field")),
        }
    }
}
impl TryFrom<Literal> for u8 {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds unsigned 8 bit integer literal")
                })?)
            }
            _ => bail!("invalid type {binding:?} for unsigned 8 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for u16 {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds unsigned 16 bit integer literal")
                })?)
            }
            _ => bail!("invalid type {binding:?} for unsigned 16 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for u32 {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds unsigned 32 bit integer literal")
                })?)
            }
            _ => bail!("invalid type {binding:?} for unsigned 32 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for u64 {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds unsigned 64 bit integer literal")
                })?)
            }
            _ => bail!("invalid type {binding:?} for unsigned 64 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for i64 {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds signed 64 bit integer literal".to_owned())
                })?)
            }
            _ => bail!("invalid type {binding:?} for signed 64 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for usize {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Int(x) => {
                Ok(x.try_into().map_err(|_| {
                    anyhow!("out-of-bounds unsigned 64 bit integer literal")
                })?)
            }
            _ => bail!("invalid type {binding:?} for unsigned 64 bit integer field"),
        }
    }
}
impl TryFrom<Literal> for bool {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Bool(x) => Ok(x),
            _ => bail!("invalid type {binding:?} for boolean field"),
        }
    }
}
impl TryFrom<Literal> for Vec<u8> {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(x.into_bytes()),
            Literal::Base64 { base64: data } => Ok(
                base64::prelude::BASE64_STANDARD_NO_PAD
                    .decode(data)
                    .map_err(|e| anyhow!("base64 decode: {e}"))?,
            ),
            _ => bail!("invalid type {binding:?} for bytes field"),
        }
    }
}
impl TryFrom<Literal> for Duration {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(
                parse_duration(x.as_str())
                    .map(TimeDelta::nanoseconds)
                    .map(Duration)
                    .map_err(|e| anyhow!("invalid duration string: {e:?}"))?,
            ),
            _ => bail!("invalid type {binding:?} for duration field"),
        }
    }
}

impl TryFrom<Literal> for Regex {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(
                Regex::new(x)?,
            ),
            _ => bail!("invalid type {binding:?} for regex"),
        }
    }
}

impl TryFrom<Literal> for location::Location {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(
                Self::from_str(&x)?,
            ),
            _ => bail!("invalid type {binding:?} for regex"),
        }
    }
}

impl TryFrom<Literal> for SignalOp {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(
                Self::try_from_str(&x)?,
            ),
            _ => bail!("invalid type {binding:?} for regex"),
        }
    }
}

impl TryFrom<Literal> for TcpSegmentOptionOutput {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::Enum { kind, mut fields } => match kind {
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::NOP_KIND => {
                    Ok(TcpSegmentOptionOutput::Nop)
                }
                EnumKind::Named(kind)
                    if kind.as_str() == TcpSegmentOptionOutput::TIMESTAMPS_KIND =>
                {
                    Ok(TcpSegmentOptionOutput::Timestamps {
                        tsval: fields
                            .remove(TcpSegmentOptionOutput::TSVAL_KEY)
                            .map(|val| {
                                let ValueOrArray::Value(Literal::Int(i)) = val else {
                                    bail!("invalid type for tsval");
                                };
                                Ok(u32::try_from(i)?)
                            })
                            .ok_or_else(|| 
                                anyhow!(
                                    "tsval is required for tcp segment option 'timestamps'"
                                        .to_owned(),
                                )
                            )??,
                        tsecr: fields
                            .remove(TcpSegmentOptionOutput::TSECR_KEY)
                            .map(|val| {
                                let ValueOrArray::Value(Literal::Int(i)) = val else {
                                    bail!("invalid type for tsecr");
                                };
                                Ok(u32::try_from(i)?)
                            })
                            .ok_or_else(|| 
                                anyhow!(
                                    "tsecr is required for tcp segment option 'timestamps'"
                                )
                            )??,
                    })
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::MSS_KIND => {
                    Ok(TcpSegmentOptionOutput::Mss(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| {
                            let ValueOrArray::Value(Literal::Int(i)) = val else {
                                bail!("invalid type for mss value (expect 16 bit unsigned integer)");
                            };
                            Ok(u16::try_from(i)?)
                        })
                        .ok_or_else(|| 
                            anyhow!(
                                "value is required for tcp segment option 'mss'"
                            )
                        )??))
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::WSCALE_KIND => {
                    Ok(TcpSegmentOptionOutput::Wscale(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| {
                            let ValueOrArray::Value(Literal::Int(i)) = val else {
                                bail!("invalid type for wscale value (expect 8 bit unsigned integer)");
                            };
                            Ok(u8::try_from(i)?)
                        })
                        .ok_or_else(|| 
                            anyhow!(
                                "value is required for tcp segment option 'wscale'"
                            )
                        )??))
                }
                EnumKind::Named(kind)
                    if kind.as_str() == TcpSegmentOptionOutput::SACK_PERMITTED_KIND =>
                {
                    Ok(TcpSegmentOptionOutput::SackPermitted)
                }
                EnumKind::Named(kind) if kind.as_str() == TcpSegmentOptionOutput::SACK_KIND => {
                    Ok(TcpSegmentOptionOutput::Sack(fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| {
                            let ValueOrArray::Array(array) = val else {
                                bail!("invalid type for sack value (expect list of 32 bit unsigned integers)");
                            };
                            Ok(array.into_iter().map(|literal| {
                                let Literal::Int(i) = literal else {
                                    bail!("invalid type for sack value (expect list of 32 bit unsigned integers)");
                                };
                                Ok(u32::try_from(i)?)
                            }).try_collect())
                        })
                        .ok_or_else(|| 
                            anyhow!(
                                "value is required for tcp segment option 'sack'"
                            )
                        )???))
                }
                EnumKind::Numeric(kind) => Ok(TcpSegmentOptionOutput::Generic {
                    kind: u8::try_from(kind)?,
                    value: fields
                        .remove(TcpSegmentOptionOutput::VALUE_KEY)
                        .map(|val| match val {
                            ValueOrArray::Value(Literal::String(s)) => Ok(s.into_bytes()),
                            ValueOrArray::Value(Literal::Base64 { base64 }) => {
                                Ok(base64::prelude::BASE64_STANDARD_NO_PAD
                                    .decode(base64)
                                    .map_err(|e| anyhow!("base64 decode: {}", e))?)
                            }
                            _ => bail!("invalid type for raw value (expect either a string literal or '{{ base64: \"...\" }}')"),
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "value is required for raw tcp segment option"
                            )
                        })??,
                }),
                _ => bail!(
                    "invalid kind '{:?}' for tcp segment option",
                    kind
                ),
            },
            _ => bail!(
                "invalid value {binding:?} for tcp segment option field"
            ),
        }
    }
}

impl TryFrom<Literal> for ProtocolField {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(x.parse()?),
            _ => bail!(
                "invalid value {binding:?} for tls version field"
            ),
        }
    }
}

impl TryFrom<Literal> for Parallelism {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(x.parse()?),
            Literal::Bool(b) if b => {
                Ok(Parallelism::Parallel(Semaphore::MAX_PERMITS))
            }
            Literal::Bool(_) => Ok(Parallelism::Serial),
            Literal::Int(i) => Ok(Parallelism::Parallel(
                i.try_into().map_err(|_| {
                    anyhow!(
                        "parallelism value {i} must fit in platform word size"
                    )
                })?,
            )),
            val => bail!(
                "invalid value {val:?} for field run.parallel"
            ),
        }
    }
}

impl TryFrom<Literal> for Url {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(
                Url::parse(&x)?,
            ),
            _ => bail!("invalid value {binding:?} for url field"),
        }
    }
}

impl TryFrom<Literal> for serde_json::Value {
    type Error = Error;
    fn try_from(binding: Literal) -> Result<Self> {
        match binding {
            Literal::String(x) => Ok(x.into()),
            Literal::Int(x) => Ok(x.into()),
            Literal::Float(x) => Ok(x.into()),
            Literal::Bool(x) => Ok(x.into()),
            Literal::Toml { literal: x } => Ok(
                serde_json::to_value(x)?,
            ),
            Literal::Base64 { base64 } => Ok(
                base64::prelude::BASE64_STANDARD_NO_PAD
                    .decode(base64)
                    .map_err(|e| anyhow!("base64 decode: {}", e))?
                    .into(),
            ),
            _ => bail!("invalid value {binding:?} for json field"),
        }
    }
}

impl TryFrom<bindings::Value> for PlanValue<PlanData, Infallible> {
    type Error = Error;
    fn try_from(binding: bindings::Value) -> Result<Self> {
        match binding {
            bindings::Value::Literal(Literal::String(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Int(x)) => Ok(PlanValue::Literal(PlanData(x.into()))),
            bindings::Value::Literal(Literal::Float(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Bool(x)) => {
                Ok(PlanValue::Literal(PlanData(x.into())))
            }
            bindings::Value::Literal(Literal::Datetime(x)) => Ok(PlanValue::Literal(x.try_into()?)),
            bindings::Value::Literal(Literal::Toml { literal }) => {
                Ok(PlanValue::Literal(PlanData::try_from(literal)?))
            }
            bindings::Value::Literal(Literal::Base64 { base64 }) => {
                Ok(PlanValue::Literal(PlanData(base64.into())))
            }
            bindings::Value::Literal(Literal::Enum { .. }) => bail!(
                "enumerations are not supported for this field".to_owned(),
            ),
            bindings::Value::ExpressionCel { cel, vars } => Ok(PlanValue::Dynamic {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            }),
            bindings::Value::ExpressionVars { .. } | bindings::Value::Unset { .. } => {
                bail!("incomplete value")
            }
        }
    }
}

impl<T, E> Evaluate<T> for PlanValue<T, E>
where
    T: TryFromPlanData<Error = E> + Clone + std::fmt::Debug,
    E: Into<anyhow::Error>,
{
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        match self {
            PlanValue::Literal(val) => Ok(val.clone()),
            Self::Dynamic { cel, vars } => T::try_from_plan_data(exec_cel(cel, vars, state)?)
                .map_err(|e: E| anyhow!(e)),
        }
    }
}

impl<T> PlanValue<T, Error>
where
    T: TryFromPlanData<Error = Error> + Clone + std::fmt::Debug,
{
    fn vars_from_toml(value: toml::Value) -> Result<Vec<(String, String)>> {
        if let toml::Value::Table(vars) = value {
            Ok(vars
                .into_iter()
                .map(|(name, value)| {
                    let toml::Value::String(plan_value) = value else {
                        bail!("invalid _vars.{name}");
                    };
                    Ok((name, plan_value))
                })
                .try_collect()?)
        } else {
            bail!("invalid _vars")
        }
    }
}

#[derive(Debug, Default)]
pub struct PlanValueTable<K, V, KE = crate::Error, VE = crate::Error>(pub Vec<(PlanValue<K, KE>, PlanValue<V, VE>)>)
where
    K: TryFromPlanData<Error = KE> + Clone,
    KE: Into<anyhow::Error>,
    V: TryFromPlanData<Error = VE> + Clone,
    VE: Into<anyhow::Error>;

impl<K, V, KE, VE> Clone for PlanValueTable<K, V, KE, VE>
where
    K: TryFromPlanData<Error = KE> + Clone,
    KE: Into<anyhow::Error>,
    V: TryFromPlanData<Error = VE> + Clone,
    VE: Into<anyhow::Error>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V, KE, KE2, VE, VE2> TryFrom<bindings::Table> for PlanValueTable<K, V, KE, VE>
where
    K: TryFromPlanData<Error = KE> + Clone,
    KE: Into<anyhow::Error>,
    PlanValue<K, KE>: TryFrom<bindings::Value, Error = KE2> + From<String>,
    VE2: Into<anyhow::Error>,
    V: TryFromPlanData<Error = VE> + Clone,
    VE: Into<anyhow::Error>,
    PlanValue<V, VE>: TryFrom<bindings::Value, Error = VE2>,
    KE2: Into<anyhow::Error>,
{
    type Error = Error;
    fn try_from(binding: bindings::Table) -> Result<Self> {
        Ok(PlanValueTable(match binding {
            bindings::Table::Map(m) => m
                .into_iter()
                // Flatten literal array values into separate entries of the same key.
                // TODO: this should probably be handled in an intermediary layer between bindings
                // and PlanValues since it's currently duplicated in the bindings merge process for
                // map -> table conversion.
                .flat_map(|(k, v)| match v {
                    bindings::ValueOrArray::Array(a) => {
                        a.into_iter().map(|v| (k.clone(), v)).collect_vec()
                    }
                    bindings::ValueOrArray::Value(v) => vec![(k, v)],
                })
                // Convert bindings to PlanValues.
                .map(|(k, v)| {
                    if let bindings::Value::Unset { .. } = v {
                        return Ok(None);
                    }
                    Ok(Some((
                        k.into(),
                        PlanValue::try_from(v).map_err(VE2::into)?,
                    )))
                })
                .filter_map(Result::transpose)
                .try_collect()?,
            bindings::Table::Array(a) => a
                .into_iter()
                .map(|entry| {
                    // Filter entries with no value.
                    if let bindings::Value::Unset { .. } = &entry.value {
                        return Ok(None);
                    }
                    Ok(Some((
                        PlanValue::try_from(entry.key).map_err(KE2::into)?,
                        PlanValue::try_from(entry.value).map_err(VE2::into)?,
                    )))
                })
                .filter_map(Result::transpose)
                .try_collect()?,
        }))
    }
}

#[derive(Debug, Clone)]
pub enum IterablePlanValue {
    Pairs(Vec<(IterableKey, PlanData)>),
    Expression {
        cel: String,
        vars: Vec<(String, String)>,
    },
}

impl Default for IterablePlanValue {
    fn default() -> Self {
        Self::Pairs(Vec::new())
    }
}

impl TryFrom<bindings::Iterable> for IterablePlanValue {
    type Error = Error;
    fn try_from(value: bindings::Iterable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            bindings::Iterable::Array(a) => IterablePlanValue::Pairs(
                a.into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        Ok((
                            IterableKey::Uint(u64::try_from(i)?),
                            PlanData::try_from(v)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            bindings::Iterable::Map(m) => IterablePlanValue::Pairs(
                m.into_iter()
                    .map(|(k, v)| Ok((IterableKey::String(k.into()), PlanData::try_from(v)?)))
                    .collect::<Result<_>>()?,
            ),
            bindings::Iterable::ExpressionCel { cel, vars } => IterablePlanValue::Expression {
                cel,
                vars: vars.unwrap_or_default().into_iter().collect(),
            },
            // If default vars were specified but never overriden or given a cel expression, treat
            // it as empty.
            bindings::Iterable::ExpressionVars { vars } => Self::default(),
        })
    }
}

impl Evaluate<Vec<(IterableKey, PlanData)>> for IterablePlanValue {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<(IterableKey, PlanData)>>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        match self {
            Self::Pairs(p) => Ok(p.clone()),
            Self::Expression { cel, vars } => match exec_cel(cel, vars, state)?.0 {
                cel_interpreter::Value::List(l) => Arc::try_unwrap(l)
                    .map_or_else(|arc| arc.as_ref().clone(), |val| val)
                    .into_iter()
                    .enumerate()
                    .map(|(i, x)| {
                        Ok((
                            IterableKey::Uint(u64::try_from(i)?),
                            PlanData(x),
                        ))
                    })
                    .try_collect(),
                cel_interpreter::Value::Map(m) => Arc::try_unwrap(m.map)
                    .map_or_else(|arc| arc.as_ref().clone(), |val| val)
                    .into_iter()
                    .map(|(k, v)| Ok((k.into(), PlanData(v))))
                    .try_collect(),
                _ => bail!("type not iterable"),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(untagged)]
pub enum IterableKey {
    Int(i64),
    Uint(u64),
    Bool(bool),
    String(Arc<String>),
}

impl Display for IterableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(x) => write!(f, "{x}"),
            Self::Uint(x) => write!(f, "{x}"),
            Self::Bool(x) => write!(f, "{x}"),
            Self::String(x) => write!(f, "{x}"),
        }
    }
}

impl From<cel_interpreter::objects::Key> for IterableKey {
    fn from(value: cel_interpreter::objects::Key) -> Self {
        match value {
            cel_interpreter::objects::Key::Int(x) => Self::Int(x),
            cel_interpreter::objects::Key::Uint(x) => Self::Uint(x),
            cel_interpreter::objects::Key::Bool(x) => Self::Bool(x),
            cel_interpreter::objects::Key::String(x) => Self::String(x),
        }
    }
}

impl From<IterableKey> for cel_interpreter::objects::Key {
    fn from(value: IterableKey) -> Self {
        match value {
            IterableKey::Int(x) => Self::Int(x),
            IterableKey::Uint(x) => Self::Uint(x),
            IterableKey::Bool(x) => Self::Bool(x),
            IterableKey::String(x) => Self::String(x),
        }
    }
}

impl From<IterableKey> for cel_interpreter::Value {
    fn from(value: IterableKey) -> Self {
        match value {
            IterableKey::Int(x) => Self::Int(x),
            IterableKey::Uint(x) => Self::UInt(x),
            IterableKey::Bool(x) => Self::Bool(x),
            IterableKey::String(x) => Self::String(x),
        }
    }
}

impl<K, KE, V, VE> Evaluate<Vec<(K, V)>> for PlanValueTable<K, V, KE, VE>
where
    K: TryFromPlanData< Error = KE> + Clone + std::fmt::Debug,
    KE: Into<anyhow::Error>,
    V: TryFromPlanData< Error = VE> + Clone + std::fmt::Debug,
    VE: Into<anyhow::Error>,
{
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<(K, V)>>
    where
        O: Into<&'a str>,
        S: State<'a, O, I>,
        I: IntoIterator<Item = O>,
    {
        self.0
            .iter()
            .map(|(key, val)| Ok((key.evaluate(state)?, val.evaluate(state)?)))
            .collect()
    }
}

impl<K, KE, V, VE> PlanValueTable<K, V, KE, VE>
where
    K: TryFromPlanData<Error = KE> + Clone + std::fmt::Debug,
    KE: Into<anyhow::Error>,
    V: TryFromPlanData<Error = VE> + Clone + std::fmt::Debug,
    VE: Into<anyhow::Error>,
{
    fn leaf_to_key_value(key: String, value: &mut toml::Value) -> Result<PlanValue<String>> {
        match value {
            // Strings or array values mean the key is not templated.
            toml::Value::String(_) | toml::Value::Array(_) => Ok(PlanValue::Literal(key)),
            // If the value is a table, check for the appropriate option to decide if the key is
            // templated.
            toml::Value::Table(t) => match t.remove("key_is_template") {
                Some(toml::Value::Boolean(b)) if b => Ok(PlanValue::Dynamic {
                    cel: key,
                    vars: t
                        .get("vars")
                        .map(toml::Value::to_owned)
                        .map(PlanValue::<String, Error>::vars_from_toml)
                        .transpose()?
                        .unwrap_or_default(),
                }),
                Some(toml::Value::Boolean(_)) | None => Ok(PlanValue::Literal(key)),
                _ => bail!("{key}.key_is_template invalid"),
            },
            _ => bail!("{key} has invalid type"),
        }
    }
}

fn add_state_to_context<'a, S, O, I>(state: &S, ctx: &mut cel_interpreter::Context)
where
    O: Into<&'a str>,
    S: State<'a, O, I>,
    I: IntoIterator<Item = O>,
{
    ctx.add_variable("locals", cel_interpreter::Value::Map(state.locals()))
        .unwrap();
    ctx.add_variable(
        "steps",
        state
            .iter()
            .into_iter()
            .map(O::into)
            .map(|name| {
                (
                    name,
                    state
                        .get(name)
                        .unwrap()
                        .to_owned()
                        .into_iter()
                        .collect::<HashMap<_, _>>(),
                )
            })
            .collect::<HashMap<_, _>>(),
    );
    ctx.add_variable("current", state.current());
    ctx.add_variable("for", state.run_for());
    ctx.add_variable("while", state.run_while());
    ctx.add_variable("count", state.run_count());
    ctx.add_function("parse_url", cel_functions::url);
    ctx.add_function(
        "parse_form_urlencoded",
        cel_functions::form_urlencoded_parts,
    );
    ctx.add_function("bytes", cel_functions::bytes);
    ctx.add_function("randomDuration", cel_functions::random_duration);
    ctx.add_function("randomInt", cel_functions::random_int);
    ctx.add_function("printf", cel_functions::printf);
}

pub trait Evaluate<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<T>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>;
}

impl<T: Evaluate<T2>, T2> Evaluate<Vec<T2>> for Vec<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Vec<T2>>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        self.iter().map(|x| x.evaluate(state)).collect()
    }
}

impl<T: Evaluate<T2>, T2> Evaluate<Option<T2>> for Option<T> {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Option<T2>>
        where
            S: State<'a, O, I>,
            O: Into<&'a str>,
            I: IntoIterator<Item = O> {
        self.as_ref().map(|x| x.evaluate(state)).transpose()
    }
}

fn exec_cel<'a, S, O, I>(cel: &str, vars: &[(String, String)], state: &S) -> Result<PlanData>
where
    O: Into<&'a str>,
    S: State<'a, O, I>,
    I: IntoIterator<Item = O>,
{
    let program =
        Program::compile(cel).map_err(|e| anyhow!("compile cel {cel}: {e}"))?;
    let mut context = Context::default();
    context.add_variable_from_value(
        "vars",
        vars.into_iter()
            .map(|(name, value)| (name.clone().into(), value.clone().into()))
            .collect::<HashMap<cel_interpreter::objects::Key, cel_interpreter::Value>>(),
    );
    add_state_to_context(state, &mut context);
    Ok(PlanData(program.execute(&context).map_err(|e| {
        anyhow!("execute cel {cel}: {e}")
    })?))
}

