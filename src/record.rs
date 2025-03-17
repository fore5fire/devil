use std::{collections::HashMap, io::Write, mem, sync::Arc};

use anyhow::bail;
use derivative::Derivative;
use gcp_bigquery_client::{
    error::{BQError, NestedResponseError},
    model::{
        table_data_insert_all_request::TableDataInsertAllRequest,
        table_data_insert_all_request_rows::TableDataInsertAllRequestRows,
        table_field_schema::TableFieldSchema,
    },
};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Serialize;
use tokio::{
    fs::File,
    io::{stdout, AsyncWriteExt, Stdout},
};
use tracing::{debug, info, info_span, span, Instrument};

use crate::{
    Direction, GraphqlOutput, GraphqlRequestOutput, GraphqlResponse, Http1Output,
    Http1RequestOutput, Http1Response, Http2FrameOutput, Http2FramePayloadOutput, Http2Output,
    Http2RequestOutput, Http2Response, HttpHeader, HttpOutput, HttpRequestOutput, HttpResponse,
    JobOutput, ProtocolDiscriminants, RawHttp2Output, RawTcpOutput, Result, RunOutput, StepOutput,
    TcpOutput, TcpReceivedOutput, TcpSegmentOutput, TcpSentOutput, TlsOutput, TlsReceivedOutput,
    TlsSentOutput,
};

pub trait BigQuerySchema {
    fn big_query_schema(name: &str) -> TableFieldSchema;
}

impl<T: BigQuerySchema> BigQuerySchema for Arc<T> {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        T::big_query_schema(name)
    }
}

impl<T: BigQuerySchema> BigQuerySchema for Option<T> {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        let mut inner = T::big_query_schema(name);
        // BigQuery can't directly represent an optional array, so just leave it as an array if its
        // both.
        if inner.mode.as_ref().map(String::as_ref) != Some("REPEATED") {
            inner.mode = Some("NULLABLE".to_owned());
        }
        inner
    }
}

impl<T: BigQuerySchema> BigQuerySchema for Vec<T> {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        let mut inner = T::big_query_schema(name);
        inner.mode = Some("REPEATED".to_owned());
        inner
    }
}

impl<K: ToString, V: BigQuerySchema> BigQuerySchema for IndexMap<K, V> {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::json(name)
    }
}

impl<K: ToString, V: BigQuerySchema> BigQuerySchema for HashMap<K, V> {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::json(name)
    }
}

impl BigQuerySchema for String {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl BigQuerySchema for &str {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl BigQuerySchema for usize {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for u64 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for u32 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for u16 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for u8 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for i64 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::integer(name)
    }
}

impl BigQuerySchema for bool {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::bool(name)
    }
}

impl BigQuerySchema for cel_interpreter::Duration {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::record(
            name,
            vec![
                // TODO: use duration types constants once exposed.
                TableFieldSchema::integer("secs"),
                TableFieldSchema::integer("nanos"),
            ],
        )
    }
}

impl BigQuerySchema for url::Url {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::string(name)
    }
}

impl BigQuerySchema for serde_json::Value {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::json(name)
    }
}

pub trait Record: Serialize + Describe + BigQuerySchema {
    fn table_name() -> &'static str;
}

impl<T: Record> Record for Arc<T> {
    fn table_name() -> &'static str {
        T::table_name()
    }
}

#[derive(Debug)]
pub enum Serializer {
    Describe,
    Json,
    Toml,
}

impl Serializer {
    fn serialize<W: Write, R: Record>(
        &mut self,
        mut writer: W,
        records: &[R],
        layers: &[ProtocolDiscriminants],
    ) -> Result<()> {
        // TODO: refactor layers to work with all formats
        match self {
            Self::Describe => {
                for r in records {
                    r.describe(&mut writer, layers)?;
                }
            }
            Self::Json => {
                for r in records {
                    serde_json::to_writer(&mut writer, &r)?;
                    writeln!(writer)?;
                }
            }
            Self::Toml => {
                for r in records {
                    let out = toml::to_string_pretty(&r)?;
                    writer.write_all(out.as_bytes())?;
                    writeln!(writer, "+++")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum RecordWriter {
    Stdout(StdoutWriter),
    File(FileWriter),
    BigQuery(BigQueryWriter),
}

impl RecordWriter {
    pub async fn write<R: Record>(
        &mut self,
        records: &[R],
        layers: &[ProtocolDiscriminants],
    ) -> Result<()> {
        match self {
            Self::Stdout(w) => w.write(records, layers).await,
            Self::File(w) => w.write(records, layers).await,
            Self::BigQuery(w) => w.write(records, layers).await,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Stdout(_) => "stdout",
            Self::File(_) => "file",
            Self::BigQuery(_) => "BigQuery",
        }
    }
}

#[derive(Debug)]
pub struct StdoutWriter {
    inner: Stdout,
    ser: Serializer,
    buf: Vec<u8>,
}

impl StdoutWriter {
    pub fn new(serializer: Serializer) -> Self {
        Self {
            inner: stdout(),
            ser: serializer,
            buf: Vec::new(),
        }
    }

    pub async fn write<R: Record>(
        &mut self,
        records: &[R],
        layers: &[ProtocolDiscriminants],
    ) -> Result<()> {
        self.buf.clear();
        self.ser.serialize(&mut self.buf, records, layers)?;
        self.inner.write_all(&self.buf).await?;
        // TODO: Do we want to flush self.inner here?
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileWriter {
    inner: File,
    ser: Serializer,
    buf: Vec<u8>,
}

impl FileWriter {
    pub async fn new(path: &str, serializer: Serializer) -> Result<Self> {
        Ok(Self {
            inner: File::options().append(true).open(path).await?,
            ser: serializer,
            buf: Vec::new(),
        })
    }

    pub async fn write<R: Record>(
        &mut self,
        records: &[R],
        layers: &[ProtocolDiscriminants],
    ) -> Result<()> {
        self.buf.clear();
        self.ser.serialize(&mut self.buf, records, layers)?;
        self.inner.write_all(&self.buf).await?;
        // TODO: Do we want to flush self.inner here?
        Ok(())
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BigQueryWriter {
    #[derivative(Debug = "ignore")]
    client: gcp_bigquery_client::Client,
    project: String,
    dataset: String,
    table_prefix: String,
    //request: TableDataInsertAllRequest,
    //name: StreamName,
    //trace_id: String,
}

impl BigQueryWriter {
    //const BUFFER_RECORDS: usize = 100;

    pub async fn new(
        project: String,
        dataset: String,
        table_prefix: String,
        creds: Option<&str>,
    ) -> Result<Self> {
        let client = if let Some(creds) = creds {
            gcp_bigquery_client::Client::from_service_account_key_file(creds).await?
        } else {
            gcp_bigquery_client::Client::from_application_default_credentials().await?
        };
        Ok(Self {
            client,
            project,
            dataset,
            table_prefix,
            //request: TableDataInsertAllRequest::new(),
            //name: StreamName::new_default(project, dataset, table),
            //trace_id: "doberman".to_owned(),
        })
    }

    pub async fn write<R: Record>(
        &mut self,
        records: &[R],
        layers: &[ProtocolDiscriminants],
    ) -> Result<()> {
        let mut request = TableDataInsertAllRequest::new();
        let rows = records
            .iter()
            .map(|r| {
                Ok::<_, serde_json::Error>(TableDataInsertAllRequestRows {
                    insert_id: None,
                    json: serde_json::to_value(r)?,
                })
            })
            .try_collect()?;
        request.add_rows(rows)?;

        let table = self.table_prefix.clone() + R::table_name();

        // HACK: we should be smarter about only creating tables when needed.
        match self
            .client
            .table()
            .get(&self.project, &self.dataset, &table, None)
            .await
        {
            Err(BQError::ResponseError { error }) if error.error.code == 404 => {
                let span = info_span!("creat table", "{table}");
                let fields = R::big_query_schema("dummy").fields;
                span.in_scope(|| debug!("{:?}", fields));
                self.client
                    .table()
                    .create(gcp_bigquery_client::model::table::Table::new(
                        &self.project,
                        &self.dataset,
                        &table,
                        gcp_bigquery_client::model::table_schema::TableSchema { fields },
                    ))
                    .instrument(span)
                    .await?;
            }
            Ok(resp) => debug!("table '{table}' already exists: {resp:?}"),
            e => bail!("{e:?}"),
        }

        let span = info_span!("insert data", "{table}");
        span.in_scope(|| debug!("request: {request:?}"));
        let resp = self
            .client
            .tabledata()
            .insert_all(&self.project, &self.dataset, &table, request)
            .instrument(span)
            .await?;
        debug!("insert into '{table}' response: {resp:?}");

        //if self.request.len() > Self::BUFFER_RECORDS {
        //    self.flush(name).await?;
        //}
        Ok(())
    }

    //#[inline]
    //pub async fn flush(&mut self, table: &str) -> Result<()> {
    //    let request = mem::take(&mut self.request);
    //    self.client
    //        .tabledata()
    //        .insert_all(&self.project, &self.dataset, table, request)
    //        .await?;
    //    Ok(())
    //}
}

pub trait Describe {
    fn describe<W: Write>(&self, w: W, layers: &[ProtocolDiscriminants]) -> std::io::Result<()>;
}

impl<T: Describe> Describe for &T {
    fn describe<W: Write>(&self, w: W, layers: &[ProtocolDiscriminants]) -> std::io::Result<()> {
        (*self).describe(w, layers)
    }
}

impl<T: Describe> Describe for Arc<T> {
    fn describe<W: Write>(&self, w: W, layers: &[ProtocolDiscriminants]) -> std::io::Result<()> {
        self.as_ref().describe(w, layers)
    }
}

impl Describe for RunOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        for (step_name, step) in &self.steps {
            writeln!(w, "------- step {step_name} --------")?;
            step.describe(&mut w, layers)?;
        }
        Ok(())
    }
}

impl Describe for StepOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        for (_, job) in &self.jobs {
            writeln!(w, "---- job {} ----", job.name)?;
            job.describe(&mut w, layers)?;
        }
        Ok(())
    }
}

impl JobOutput {
    fn default_layers<'a>(
        &self,
        input: &'a [ProtocolDiscriminants],
    ) -> &'a [ProtocolDiscriminants] {
        if !input.is_empty() {
            input
        } else if self.graphql.is_some() {
            &[ProtocolDiscriminants::Graphql]
        //} else if proto.http3.is_some() {
        //    vec![ProtocolDiscriminants::HTTP]
        } else if self.h2.is_some() {
            &[ProtocolDiscriminants::H2]
        } else if self.h2c.is_some() {
            &[ProtocolDiscriminants::H2c]
        } else if self.h1.is_some() {
            &[ProtocolDiscriminants::H1]
        } else if self.h1c.is_some() {
            &[ProtocolDiscriminants::H1c]
        } else if self.http.is_some() {
            &[ProtocolDiscriminants::Http]
        } else if self.raw_h2.is_some() {
            &[ProtocolDiscriminants::RawH2]
        } else if self.raw_h2c.is_some() {
            &[ProtocolDiscriminants::RawH2c]
        } else if self.tls.is_some() {
            &[ProtocolDiscriminants::Tls]
        } else if self.tcp.is_some() {
            &[ProtocolDiscriminants::Tcp]
        } else if self.raw_tcp.is_some() {
            &[ProtocolDiscriminants::RawTcp]
        } else {
            &[]
        }
    }
}

impl Describe for JobOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        // TODO: escape or refuse to print dangerous term characters in output.

        let layers = self.default_layers(layers);
        for level in layers {
            match level {
                ProtocolDiscriminants::RawTcp => {
                    if let Some(segments) = &self.raw_tcp {
                        segments.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::Tcp => {
                    if let Some(tcp) = &self.tcp {
                        tcp.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::Tls => {
                    if let Some(tls) = &self.tls {
                        tls.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::RawH2 => {
                    if let Some(http) = &self.raw_h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::RawH2c => {
                    if let Some(http) = &self.raw_h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::Http => {
                    if let Some(http) = &self.http {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::H1 => {
                    if let Some(http) = &self.h1 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::H1c => {
                    if let Some(http) = &self.h1c {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::H2 => {
                    if let Some(http) = &self.h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::H2c => {
                    if let Some(http) = &self.h2c {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolDiscriminants::Graphql => {
                    if let Some(graphql) = &self.graphql {
                        graphql.describe(&mut w, layers)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Describe for GraphqlOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Graphql) {
            return Ok(());
        }
        if let Some(req) = &self.request {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.response {
            resp.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(w, "{} error: {}", e.kind, e.message)?;
        }
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for GraphqlRequestOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Graphql) {
            return Ok(());
        }
        writeln!(w, "> {}", &self.query.replace("\n", "\n> "))?;
        writeln!(w, "request duration: {}", self.duration.0)
    }
}

impl Describe for GraphqlResponse {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Graphql) {
            return Ok(());
        }
        writeln!(
            w,
            "< {}",
            serde_json::to_string_pretty(&self.full)
                .unwrap()
                .replace("\n", "\n< ")
        )?;
        writeln!(w, "response duration: {}", self.duration.0)
    }
}

impl Describe for Http2Output {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::H2)
            && !layers.contains(&ProtocolDiscriminants::H2c)
        {
            return Ok(());
        }
        if let Some(req) = &self.request {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.response {
            resp.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(w, "{} error: {}", e.kind, e.message)?;
        }
        //for p in &http.pause.request_headers.start {
        //    writeln!(w,"request headers start pause duration: {}", p.duration);
        //}
        //for p in &http.pause.request_headers.end {
        //    writeln!(w,"request headers end pause duration: {}", p.duration);
        //}
        //for p in &http.pause.request_body.start {
        //    writeln!(w,"request body start pause duration: {}", p.duration);
        //}
        //for p in &http.pause.request_body.end {
        //    writeln!(w,"request body end pause duration: {}", p.duration);
        //}
        //for p in &http.pause.response_headers.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration);
        //}
        //for p in &http.pause.response_headers.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration);
        //}
        //for p in &http.pause.response_body.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration);
        //}
        //for p in &http.pause.response_body.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration);
        //}
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for Http2RequestOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::H2)
            && !layers.contains(&ProtocolDiscriminants::H2c)
        {
            return Ok(());
        }
        writeln!(
            w,
            "> {}{}{} HTTP/2",
            self.method.as_ref().unwrap_or_default(),
            if self.method.is_some() { " " } else { "" },
            self.url,
        )?;
        for header in &self.headers {
            header.describe(&mut w, layers)?;
        }
        writeln!(w, "> {}", &self.body.to_string().replace("\n", "\n> "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "request time to first byte: {}", ttfb.0)?;
        }
        writeln!(w, "request duration: {}", self.duration.0)
    }
}

impl Describe for Http2Response {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::H2)
            && !layers.contains(&ProtocolDiscriminants::H2c)
        {
            return Ok(());
        }
        writeln!(w, "< {} HTTP/2", self.status_code.unwrap_or(0))?;
        if let Some(headers) = &self.headers {
            for header in headers {
                header.describe(&mut w, layers)?;
            }
        }
        writeln!(w, "< ")?;
        if let Some(body) = &self.body {
            writeln!(w, "< {}", &body.to_string().replace("\n", "\n< "))?;
        }
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "response time to first byte: {}", ttfb.0)?;
        }
        writeln!(w, "response duration: {}", self.duration.0)
    }
}

impl Describe for Http1Output {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::H1)
            && !layers.contains(&ProtocolDiscriminants::H1c)
        {
            return Ok(());
        }
        if let Some(req) = &self.request {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.response {
            resp.describe(&mut w, layers)?;
        }
        //for e in &http.errors {
        //    writeln!(w,"{} error: {}", e.kind, e.message)?;
        //}
        //for p in &http.pause.request_headers.start {
        //    writeln!(w,"request headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_headers.end {
        //    writeln!(w,"request headers end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_body.start {
        //    writeln!(w,"request body start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_body.end {
        //    writeln!(w,"request body end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_headers.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_headers.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_body.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_body.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration)?;
        //}
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for Http1RequestOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        // TODO: How to handle allowing h1c when h1 is skipped or vice-versa?
        if !layers.contains(&ProtocolDiscriminants::H1)
            && !layers.contains(&ProtocolDiscriminants::H1c)
        {
            return Ok(());
        }
        writeln!(
            w,
            "> {}{}{}{}{}",
            self.method.as_ref().unwrap_or_default(),
            if self.method.is_some() { " " } else { "" },
            self.url,
            if self.version_string.is_some() {
                " "
            } else {
                ""
            },
            self.version_string.as_ref().unwrap_or_default(),
        )?;
        for header in &self.headers {
            header.describe(&mut w, layers)?;
        }
        writeln!(w, "> {}", &self.body.to_string().replace("\n", "\n> "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "request time to first byte: {}", ttfb.0)?;
        }
        writeln!(w, "request duration: {}", self.duration.0)
    }
}

impl Describe for Http1Response {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::H1)
            && !layers.contains(&ProtocolDiscriminants::H1c)
        {
            return Ok(());
        }
        writeln!(
            w,
            "< {} {}",
            self.status_code.unwrap_or(0),
            self.protocol
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or_default()
        )?;
        if let Some(headers) = &self.headers {
            for header in headers {
                header.describe(&mut w, layers)?;
            }
        }
        writeln!(w, "< ")?;
        if let Some(body) = &self.body {
            writeln!(w, "< {}", body.to_string().replace("\n", "\n< "))?;
        }
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "response time to first byte: {}", ttfb.0)?;
        }
        writeln!(w, "response duration: {}", self.duration.0)
    }
}

impl Describe for HttpOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Http) {
            return Ok(());
        }
        if let Some(req) = &self.request {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.response {
            resp.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(w, "{} error: {}", e.kind, e.message)?;
        }
        //for p in &http.pause.request_headers.start {
        //    writeln!(w,"request headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_headers.end {
        //    writeln!(w,"request headers end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_body.start {
        //    writeln!(w,"request body start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.request_body.end {
        //    writeln!(w,"request body end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_headers.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_headers.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_body.start {
        //    writeln!(w,"response headers start pause duration: {}", p.duration)?;
        //}
        //for p in &http.pause.response_body.end {
        //    writeln!(w,"response headers end pause duration: {}", p.duration)?;
        //}
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for HttpRequestOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Http) {
            return Ok(());
        }
        writeln!(
            w,
            "> {}{}{} {}",
            self.method.as_ref().unwrap_or_default(),
            if self.method.is_some() { " " } else { "" },
            self.url,
            self.protocol
        )?;
        for header in &self.headers {
            header.describe(&mut w, layers)?;
        }

        writeln!(w, "> {}", &self.body.to_string().replace("\n", "\n> "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "request time to first byte: {}", ttfb.0)?;
        }

        writeln!(w, "request duration: {}", self.duration.0)
    }
}

impl Describe for HttpResponse {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Http) {
            return Ok(());
        }
        writeln!(
            w,
            "< {} {}",
            self.status_code.unwrap_or(0),
            self.protocol.as_ref().unwrap_or_default()
        )?;
        if let Some(headers) = &self.headers {
            for header in headers {
                header.describe(&mut w, layers)?;
            }
        }
        writeln!(w, "< ")?;
        if let Some(body) = &self.body {
            writeln!(w, "< {}", &body.to_string().replace("\n", "\n< "))?;
        }
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "response time to first byte: {}", ttfb.0)?;
        }
        writeln!(w, "response duration: {}", self.duration.0)
    }
}

impl Describe for HttpHeader {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if let Some(k) = &self.key {
            writeln!(w, "< {}: {}", k, self.value)
        } else {
            writeln!(w, "< {}", self.value)
        }
    }
}

impl Describe for RawHttp2Output {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::RawH2)
            && !layers.contains(&ProtocolDiscriminants::RawH2c)
        {
            return Ok(());
        }
        for frame in &self.sent {
            frame.describe(&mut w, layers)?;
        }
        for frame in &self.received {
            frame.describe(&mut w, layers)?;
        }
        Ok(())
    }
}

impl Describe for Http2FrameOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::RawH2)
            && !layers.contains(&ProtocolDiscriminants::RawH2c)
        {
            return Ok(());
        }
        let d = match self.direction {
            Direction::Send => '>',
            Direction::Recv => '<',
        };
        writeln!(w, "{d} type: {:}", self.payload.r#type())?;
        writeln!(w, "{d} flags: {:#010b}", self.flags.bits())?;
        match &self.payload {
            Http2FramePayloadOutput::Data(frame) => {
                writeln!(w, "{d}   END_STREAM: {}", frame.end_stream)?;
                writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
            }
            Http2FramePayloadOutput::Headers(frame) => {
                writeln!(w, "{d}   END_STREAM: {}", frame.end_stream)?;
                writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers)?;
                writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
                writeln!(w, "{d}   PRIORITY: {}", frame.priority.is_some())?;
            }
            Http2FramePayloadOutput::Settings(frame) => {
                writeln!(w, "{d}   ACK: {}", frame.ack)?;
            }
            Http2FramePayloadOutput::PushPromise(frame) => {
                writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers)?;
                writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
            }
            Http2FramePayloadOutput::Ping(frame) => {
                writeln!(w, "{d}   ACK: {}", frame.ack)?;
            }
            Http2FramePayloadOutput::Continuation(frame) => {
                writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers)?;
            }
            _ => {}
        }
        let r = self.r;
        if r {
            writeln!(w, "{d} r: 1")?;
        }
        writeln!(w, "{d} stream_id: {}", self.stream_id)?;
        match &self.payload {
            Http2FramePayloadOutput::Data(frame) => {
                writeln!(w, "{d} data: {:?}", frame.data)?;
                if frame
                    .padding
                    .as_ref()
                    .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
                {
                    writeln!(w, "{d} padding: {:?}", frame.padding)?;
                }
            }
            Http2FramePayloadOutput::Headers(frame) => {
                if let Some(priority) = &frame.priority {
                    writeln!(w, "{d} exclusive: {}", priority.e)?;
                    writeln!(w, "{d} stream dependency: {}", priority.stream_dependency)?;
                    writeln!(w, "{d} weight: {}", priority.weight)?;
                }
                writeln!(
                    w,
                    "{d} header block fragment: {:?}",
                    frame.header_block_fragment
                )?;
                if frame
                    .padding
                    .as_ref()
                    .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
                {
                    writeln!(w, "{d} padding: {:?}", frame.padding)?;
                }
            }
            Http2FramePayloadOutput::Settings(frame) => {
                writeln!(w, "{d} parameters:")?;
                for param in &frame.parameters {
                    writeln!(w, "{d}   ID: {}", param.id)?;
                    writeln!(w, "{d}   value: {:#06x}", param.value)?;
                }
            }
            Http2FramePayloadOutput::PushPromise(frame) => {
                if frame.promised_r {
                    writeln!(w, "{d} promised stream ID r bit: 1")?;
                }
                writeln!(w, "{d} promised stream ID: {}", frame.promised_stream_id)?;
                writeln!(
                    w,
                    "{d} header block fragment: {:?}",
                    frame.header_block_fragment
                )?;
                if frame
                    .padding
                    .as_ref()
                    .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
                {
                    writeln!(w, "{d} padding: {:?}", frame.padding)?;
                }
            }
            Http2FramePayloadOutput::Ping(frame) => {
                writeln!(w, "{d} data: {:?}", frame.data)?;
            }
            Http2FramePayloadOutput::Continuation(frame) => {
                writeln!(
                    w,
                    "{d} header block fragment: {:?}",
                    frame.header_block_fragment
                )?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl Describe for TlsOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tls) {
            return Ok(());
        }
        if let Some(req) = &self.sent {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.received {
            resp.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(w, "{} error: {}", e.kind, e.message)?;
        }
        //for p in &tls.pause.handshake.start {
        //    writeln!(w,"handshake start pause duration: {}", p.duration)?;
        //}
        //for p in &tls.pause.handshake.end {
        //    writeln!(w,"handshake end pause duration: {}", p.duration)?;
        //}
        //for p in &tls.pause.send_body.start {
        //    writeln!(w,"send body start pause duration: {}", p.duration)?;
        //}
        //for p in &tls.pause.send_body.end {
        //    writeln!(w,"send body end pause duration: {}", p.duration)?;
        //}
        //for p in &tls.pause.receive_body.start {
        //    writeln!(w,"receive body start pause duration: {}", p.duration)?;
        //}
        //for p in &tls.pause.receive_body.end {
        //    writeln!(w,"receive body end pause duration: {}", p.duration)?;
        //}
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for TlsSentOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tls) {
            return Ok(());
        }
        writeln!(w, "> {}", &self.body.to_string().replace("\n", "\n> "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "sent time to first byte: {}", ttfb.0)?;
        }
        Ok(())
    }
}

impl Describe for TlsReceivedOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tls) {
            return Ok(());
        }
        writeln!(w, "< {}", &self.body.to_string().replace("\n", "\n< "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "response time to first byte: {}", ttfb.0)?;
        }
        Ok(())
    }
}

impl Describe for TcpOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tcp) {
            return Ok(());
        }
        if let Some(req) = &self.sent {
            req.describe(&mut w, layers)?;
        }
        if let Some(resp) = &self.received {
            resp.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(&mut w, "{} error: {}", e.kind, e.message)?;
        }
        //for p in &tcp.pause.handshake.start {
        //    writeln!(w,"handshake start pause duration: {}", p.duration)?;
        //}
        //for p in &tcp.pause.handshake.end {
        //    writeln!(w,"handshake end pause duration: {}", p.duration)?;
        //}
        //for p in &tcp.pause.send_body.start {
        //    writeln!(w,"send body start pause duration: {}", p.duration)?;
        //}
        //for p in &tcp.pause.send_body.end {
        //    writeln!(w,"send body end pause duration: {}", p.duration)?;
        //}
        //for p in &tcp.pause.receive_body.start {
        //    writeln!(w,"receive body start pause duration: {}", p.duration)?;
        //}
        //for p in &tcp.pause.receive_body.end {
        //    writeln!(w,"receive body end pause duration: {}", p.duration)?;
        //}
        writeln!(w, "total duration: {}", self.duration.0)
    }
}

impl Describe for TcpSentOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tcp) {
            return Ok(());
        }
        writeln!(w, "> {}", &self.body.to_string().replace("\n", "\n> "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "sent time to first byte: {}", ttfb.0)?;
        }
        Ok(())
    }
}

impl Describe for TcpReceivedOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::Tcp) {
            return Ok(());
        }
        writeln!(w, "< {}", &self.body.to_string().replace("\n", "\n< "))?;
        if let Some(ttfb) = &self.time_to_first_byte {
            writeln!(w, "response time to first byte: {}", ttfb.0)?;
        }
        Ok(())
    }
}

impl Describe for RawTcpOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::RawTcp) {
            return Ok(());
        }
        for segment in &self.sent {
            segment.describe(&mut w, layers)?;
        }
        for segment in &self.received {
            write!(w, "< ")?;
            segment.describe(&mut w, layers)?;
        }
        for e in &self.errors {
            writeln!(w, "{} error: {}", e.kind, e.message)?;
        }
        //for p in &raw.pause.handshake.start {
        //    writeln!(w,"handshake start pause duration: {}", p.duration)?;
        //}
        //for p in &raw.pause.handshake.end {
        //    writeln!(w,"handshake end pause duration: {}", p.duration)?;
        //}
        writeln!(w, "total duration: {}", self.duration.0)?;
        Ok(())
    }
}

impl Describe for TcpSegmentOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolDiscriminants::RawTcp) {
            return Ok(());
        }
        let d = match self.direction {
            Direction::Send => '>',
            Direction::Recv => '<',
        };
        writeln!(w, "{d} {}", self.payload)
    }
}
