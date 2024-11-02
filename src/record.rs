use std::{io::Write, mem, sync::Arc};

use derivative::Derivative;
use gcp_bigquery_client::model::{
    table_data_insert_all_request::TableDataInsertAllRequest,
    table_data_insert_all_request_rows::TableDataInsertAllRequestRows,
};
use itertools::Itertools;
use serde::Serialize;
use tokio::{
    fs::File,
    io::{stdout, AsyncWriteExt, Stdout},
};

use crate::{
    Direction, GraphqlOutput, GraphqlRequestOutput, GraphqlResponse, Http1Output,
    Http1RequestOutput, Http1Response, Http2FrameOutput, Http2FramePayloadOutput, Http2Output,
    Http2PushPromiseFrameOutput, Http2RequestOutput, Http2Response, HttpOutput, HttpRequestOutput,
    HttpResponse, JobOutput, Pdu, ProtocolOutput, ProtocolOutputDiscriminants, RawHttp2Output,
    RawTcpOutput, Result, RunOutput, StepOutput, TcpOutput, TcpReceivedOutput, TcpSegmentOutput,
    TcpSentOutput, TlsOutput, TlsReceivedOutput, TlsSentOutput,
};

pub trait Record: Serialize + Describe {}

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
        layers: &[ProtocolOutputDiscriminants],
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
        layers: &[ProtocolOutputDiscriminants],
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
        layers: &[ProtocolOutputDiscriminants],
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
        layers: &[ProtocolOutputDiscriminants],
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
    table: String,
    request: TableDataInsertAllRequest,
    //name: StreamName,
    //trace_id: String,
}

impl BigQueryWriter {
    const BUFFER_RECORDS: usize = 100;

    pub async fn with_project_id(project: String, dataset: String, table: String) -> Result<Self> {
        Ok(Self {
            client: gcp_bigquery_client::Client::from_application_default_credentials().await?,
            project,
            dataset,
            table,
            request: TableDataInsertAllRequest::new(),
            //name: StreamName::new_default(project, dataset, table),
            //trace_id: "devil".to_owned(),
        })
    }

    pub async fn write<R: Record>(
        &mut self,
        records: &[R],
        layers: &[ProtocolOutputDiscriminants],
    ) -> Result<()> {
        let rows = records
            .iter()
            .map(|r| {
                Ok::<_, serde_json::Error>(TableDataInsertAllRequestRows {
                    insert_id: None,
                    json: serde_json::to_value(r)?,
                })
            })
            .try_collect()?;
        self.request.add_rows(rows)?;
        if self.request.len() > Self::BUFFER_RECORDS {
            self.flush().await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn flush(&mut self) -> Result<()> {
        let request = mem::take(&mut self.request);
        self.client
            .tabledata()
            .insert_all(&self.project, &self.dataset, &self.table, request)
            .await?;
        Ok(())
    }
}

pub trait Describe {
    fn describe<W: Write>(
        &self,
        w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()>;
}

impl<T: Describe> Describe for &T {
    fn describe<W: Write>(
        &self,
        w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        (*self).describe(w, layers)
    }
}

impl<T: Describe> Describe for Arc<T> {
    fn describe<W: Write>(
        &self,
        w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        self.as_ref().describe(w, layers)
    }
}

impl Describe for RunOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolOutputDiscriminants],
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        for (job_name, job) in &self.jobs {
            writeln!(w, "---- job {job_name} ----")?;
            job.describe(&mut w, layers)?;
        }
        Ok(())
    }
}

impl JobOutput {
    fn default_layers<'a>(
        &self,
        input: &'a [ProtocolOutputDiscriminants],
    ) -> &'a [ProtocolOutputDiscriminants] {
        if !input.is_empty() {
            return input;
        } else if self.graphql.is_some() {
            &[ProtocolOutputDiscriminants::Graphql]
        //} else if proto.http3.is_some() {
        //    vec![ProtocolOutputDiscriminants::HTTP]
        } else if self.h2.is_some() {
            &[ProtocolOutputDiscriminants::H2]
        } else if self.h2c.is_some() {
            &[ProtocolOutputDiscriminants::H2c]
        } else if self.h1.is_some() {
            &[ProtocolOutputDiscriminants::H1]
        } else if self.h1c.is_some() {
            &[ProtocolOutputDiscriminants::H1c]
        } else if self.http.is_some() {
            &[ProtocolOutputDiscriminants::Http]
        } else if self.raw_h2.is_some() {
            &[ProtocolOutputDiscriminants::RawH2]
        } else if self.raw_h2c.is_some() {
            &[ProtocolOutputDiscriminants::RawH2c]
        } else if self.tls.is_some() {
            &[ProtocolOutputDiscriminants::Tls]
        } else if self.tcp.is_some() {
            &[ProtocolOutputDiscriminants::Tcp]
        } else if self.raw_tcp.is_some() {
            &[ProtocolOutputDiscriminants::RawTcp]
        } else {
            &[]
        }
    }
}

impl Describe for JobOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        // TODO: escape or refuse to print dangerous term characters in output.

        for level in self.default_layers(layers) {
            match level {
                ProtocolOutputDiscriminants::RawTcp => {
                    if let Some(segments) = &self.raw_tcp {
                        segments.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::Tcp => {
                    if let Some(tcp) = &self.tcp {
                        tcp.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::Tls => {
                    if let Some(tls) = &self.tls {
                        tls.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::RawH2 => {
                    if let Some(http) = &self.raw_h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::RawH2c => {
                    if let Some(http) = &self.raw_h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::Http => {
                    if let Some(http) = &self.http {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::H1 => {
                    if let Some(http) = &self.h1 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::H1c => {
                    if let Some(http) = &self.h1c {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::H2 => {
                    if let Some(http) = &self.h2 {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::H2c => {
                    if let Some(http) = &self.h2c {
                        http.describe(&mut w, layers)?;
                    }
                }
                ProtocolOutputDiscriminants::Graphql => {
                    if let Some(graphql) = &self.graphql {
                        graphql.describe(&mut w, layers)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Describe for ProtocolOutput {
    fn describe<W: Write>(
        &self,
        w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        match self {
            Self::Graphql(o) => o.describe(w, layers),
            Self::Http(o) => o.describe(w, layers),
            Self::H1c(o) => o.describe(w, layers),
            Self::H1(o) => o.describe(w, layers),
            Self::H2c(o) => o.describe(w, layers),
            Self::RawH2c(o) => o.describe(w, layers),
            Self::H2(o) => o.describe(w, layers),
            Self::RawH2(o) => o.describe(w, layers),
            Self::Tls(o) => o.describe(w, layers),
            Self::Tcp(o) => o.describe(w, layers),
            Self::RawTcp(o) => o.describe(w, layers),
        }
    }
}

impl Describe for Pdu {
    fn describe<W: Write>(
        &self,
        w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        match self {
            Self::GraphqlRequest(o) => o.describe(w, layers),
            Self::GraphqlResponse(o) => o.describe(w, layers),
            Self::HttpRequest(o) => o.describe(w, layers),
            Self::HttpResponse(o) => o.describe(w, layers),
            Self::H1cRequest(o) => o.describe(w, layers),
            Self::H1cResponse(o) => o.describe(w, layers),
            Self::H1Request(o) => o.describe(w, layers),
            Self::H1Response(o) => o.describe(w, layers),
            Self::H2cRequest(o) => o.describe(w, layers),
            Self::H2cResponse(o) => o.describe(w, layers),
            Self::H2Request(o) => o.describe(w, layers),
            Self::H2Response(o) => o.describe(w, layers),
            Self::RawH2c(o) => o.describe(w, layers),
            Self::RawH2(o) => o.describe(w, layers),
            Self::TlsSent(o) => o.describe(w, layers),
            Self::TlsReceived(o) => o.describe(w, layers),
            Self::TcpSent(o) => o.describe(w, layers),
            Self::TcpReceived(o) => o.describe(w, layers),
            Self::RawTcp(o) => o.describe(w, layers),
        }
    }
}

impl Describe for GraphqlOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Graphql) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Graphql) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Graphql) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::H2)
            && !layers.contains(&ProtocolOutputDiscriminants::H2c)
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::H2)
            && !layers.contains(&ProtocolOutputDiscriminants::H2c)
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
        for (k, v) in &self.headers {
            writeln!(w, ">   {}: {}", k, v)?;
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::H2)
            && !layers.contains(&ProtocolOutputDiscriminants::H2c)
        {
            return Ok(());
        }
        writeln!(w, "< {} HTTP/2", self.status_code.unwrap_or(0))?;
        if let Some(headers) = &self.headers {
            for (k, v) in headers {
                if let Some(k) = k {
                    writeln!(w, "< {}: {}", k, v)?;
                } else {
                    writeln!(w, "< {}", v)?;
                }
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::H1)
            && !layers.contains(&ProtocolOutputDiscriminants::H1c)
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        // TODO: How to handle allowing h1c when h1 is skipped or vice-versa?
        if !layers.contains(&ProtocolOutputDiscriminants::H1)
            && !layers.contains(&ProtocolOutputDiscriminants::H1c)
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
        for (k, v) in &self.headers {
            writeln!(w, ">   {}: {}", k, v)?;
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::H1)
            && !layers.contains(&ProtocolOutputDiscriminants::H1c)
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
            for (k, v) in headers {
                writeln!(w, "< {}: {}", k, v)?;
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Http) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Http) {
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
        for (k, v) in &self.headers {
            writeln!(w, ">   {}: {}", k, v)?;
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Http) {
            return Ok(());
        }
        writeln!(
            w,
            "< {} {}",
            self.status_code.unwrap_or(0),
            self.protocol.as_ref().unwrap_or_default()
        )?;
        if let Some(headers) = &self.headers {
            for (k, v) in headers {
                writeln!(w, "< {}: {}", k, v)?;
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

impl Describe for RawHttp2Output {
    fn describe<W: Write>(
        &self,
        mut w: W,
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::RawH2)
            && !layers.contains(&ProtocolOutputDiscriminants::RawH2c)
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::RawH2)
            && !layers.contains(&ProtocolOutputDiscriminants::RawH2c)
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tls) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tls) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tls) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tcp) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tcp) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::Tcp) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::RawTcp) {
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
        layers: &[ProtocolOutputDiscriminants],
    ) -> std::io::Result<()> {
        if !layers.contains(&ProtocolOutputDiscriminants::RawTcp) {
            return Ok(());
        }
        let d = match self.direction {
            Direction::Send => '>',
            Direction::Recv => '<',
        };
        writeln!(w, "{d} {}", self.payload)
    }
}
