use std::{collections::HashMap, fmt, rc::Rc, sync::Arc};

use cel_interpreter::extractors::This;
use cel_interpreter::{FunctionContext, ResolveResult, Value};

use rand::Rng;
use sprintf::{vsprintf, Printf, PrintfError};
use url::Url;

type Result<T> = std::result::Result<T, cel_interpreter::ExecutionError>;

pub fn url(ftx: &FunctionContext, This(url): This<Arc<String>>) -> ResolveResult {
    let url = Url::parse(&url).map_err(|e| ftx.error(&e.to_string()))?;
    Ok(url_to_cel(url))
}

fn url_to_cel(url: Url) -> cel_interpreter::Value {
    cel_interpreter::Value::Map(cel_interpreter::objects::Map {
        map: Rc::new(HashMap::from([
            ("scheme".into(), url.scheme().into()),
            ("username".into(), url.username().into()),
            ("password".into(), url.password().into()),
            ("host".into(), url.host_str().into()),
            ("port".into(), url.port().map(|x| x as u64).into()),
            (
                "port_or_default".into(),
                url.port_or_known_default().map(|x| x as u64).into(),
            ),
            ("path".into(), url.path().into()),
            (
                "path_segments".into(),
                url.path_segments().map(|x| x.collect::<Vec<_>>()).into(),
            ),
            ("query".into(), url.query().into()),
            ("fragment".into(), url.fragment().into()),
        ])),
    })
}

pub fn form_urlencoded_parts(This(query): This<Arc<String>>) -> Arc<Vec<Value>> {
    Arc::new(
        form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .map(|(k, v)| {
                cel_interpreter::Value::Map(cel_interpreter::objects::Map {
                    map: Rc::new(HashMap::from([
                        ("key".into(), k.into()),
                        ("value".into(), v.into()),
                    ])),
                })
            })
            .collect(),
    )
}

pub fn bytes(This(string): This<Arc<String>>) -> Arc<Vec<u8>> {
    Arc::new(string.as_ref().clone().into_bytes())
}

pub fn random_duration(
    ftx: &FunctionContext,
    min: i64,
    max: i64,
    unit: Arc<String>,
) -> Result<Value> {
    let val = rand::thread_rng().gen_range(min..max);
    Ok(Value::Duration(match unit.as_str() {
        "d" => chrono::Duration::days(val),
        "h" => chrono::Duration::hours(val),
        "m" => chrono::Duration::minutes(val),
        "s" => chrono::Duration::seconds(val),
        "ms" => chrono::Duration::milliseconds(val),
        "us" => chrono::Duration::microseconds(val),
        "ns" => chrono::Duration::nanoseconds(val),
        _ => return Err(ftx.error("invalid unit")),
    }))
}

pub fn random_int(ftx: &FunctionContext, min: i64, max: i64) -> Result<Value> {
    Ok(cel_interpreter::Value::Int(
        rand::thread_rng().gen_range(min..max),
    ))
}

pub fn printf(ftx: &FunctionContext, format: Arc<String>) -> Result<Arc<String>> {
    let args = ftx
        .args
        .iter()
        .skip(1)
        .map(|a| Ok(FormatValue(ftx.ptx.resolve(a)?)))
        .collect::<Result<Vec<_>>>()?;
    let args_ref: Vec<&dyn Printf> = args.iter().map(|x| x as &dyn Printf).collect();
    vsprintf(&format, &args_ref).map(Arc::new).map_err(|e| {
        ftx.error(match e {
            sprintf::PrintfError::WrongType => "wrong type",
            sprintf::PrintfError::NotEnoughArgs => "more placeholders than arguments",
            sprintf::PrintfError::TooManyArgs => "more arguments than placeholders",
            sprintf::PrintfError::ParseError => "format string parse error",
            sprintf::PrintfError::Unknown => "unknown error",
        })
    })
}

struct FormatValue(Value);

impl Printf for FormatValue {
    fn format(&self, spec: &sprintf::ConversionSpecifier) -> sprintf::Result<String> {
        match &self.0 {
            Value::Int(x) => x.format(spec),
            Value::UInt(x) => x.format(spec),
            Value::Float(x) => x.format(spec),
            Value::String(x) => x.format(spec),
            _ => Err(PrintfError::WrongType),
        }
    }
    fn as_int(&self) -> Option<i32> {
        match &self.0 {
            Value::Int(x) => x.as_int(),
            Value::UInt(x) => x.as_int(),
            Value::Float(x) => x.as_int(),
            Value::String(x) => x.as_int(),
            _ => None,
        }
    }
}
