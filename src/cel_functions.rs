use cel_interpreter::extractors::This;
use cel_interpreter::{ExecutionError, FunctionContext, ResolveResult, Value};
use std::{collections::HashMap, rc::Rc, sync::Arc};
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

pub fn form_urlencoded_parts(This(query): This<Arc<String>>) -> Arc<Vec<cel_interpreter::Value>> {
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

pub fn uint(This(val): This<Value>) -> Result<u64> {
    match val {
        Value::UInt(uint) => Ok(uint),
        Value::Int(int) => u64::try_from(int).map_err(|e| ExecutionError::FunctionError {
            function: "uint".to_owned(),
            message: e.to_string(),
        }),
        Value::Float(float) => {
            if float < u64::MIN as f64 || float > u64::MAX as f64 {
                return Err(ExecutionError::FunctionError {
                    function: "uint".to_owned(),
                    message: "double out of bounds for uint".to_owned(),
                });
            }
            Ok(float as u64)
        }
        Value::String(string) => string
            .parse::<u64>()
            .map_err(|e| ExecutionError::FunctionError {
                function: "uint".to_owned(),
                message: e.to_string(),
            }),
        target => Err(ExecutionError::UnsupportedTargetType { target }),
    }
}
