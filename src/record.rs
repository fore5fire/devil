use std::{io::Write, mem};

use clap::ValueEnum;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use serde::Serialize;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::{Http2FrameOutput, JobOutput, PlanOutput, Result, StepOutput};

pub enum Serializer {
    Describe {
        sent: Vec<Protocol>,
        received: Option<Vec<Protocol>>,
    },
    Json,
    Toml,
}

impl Serializer {
    fn serialize<W: Write, R: Serialize + Describe>(
        &mut self,
        mut writer: W,
        record: R,
    ) -> Result<()> {
        match self {
            Self::Describe { sent, received } => record.describe(
                writer,
                sent.as_slice(),
                received.as_ref().map(|r| r.as_slice()),
            )?,
            Self::Json => serde_json::to_writer(writer, &record)?,
            Self::Toml => {
                let out = toml::to_string_pretty(&record)?;
                writer.write_all(out.as_bytes())?;
            }
        }
        Ok(())
    }
}

pub enum RecordWriter {
    File(FileWriter),
    BigQuery(BigQueryWriter),
}

impl RecordWriter {
    pub async fn write<R: Serialize + Describe>(&mut self, record: R) -> Result<()> {
        match self {
            Self::File(f) => f.write(record).await,
            Self::BigQuery(bq) => bq.write(record).await,
        }
    }
}

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

    pub async fn write<R: Serialize + Describe>(&mut self, record: R) -> Result<()> {
        self.buf.clear();
        self.ser.serialize(&mut self.buf, record)?;
        self.inner.write_all(&self.buf).await?;
        Ok(())
    }
}

pub struct BigQueryWriter {
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

    pub async fn write<R: Serialize>(&mut self, record: R) -> Result<()> {
        self.request.add_row(None, record)?;
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

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[clap(rename_all = "snake_case")]
pub enum Protocol {
    Graphql,
    Http,
    Http2Frames,
    Tls,
    Tcp,
    TcpSegments,
    Udp,
    Quic,
    Ip,
    None,
}

pub trait Describe {
    fn describe<W: Write>(
        &self,
        w: W,
        sent_level: &[Protocol],
        recv_level: Option<&[Protocol]>,
    ) -> std::io::Result<()>;
}

impl<T: Describe> Describe for &T {
    fn describe<W: Write>(
        &self,
        w: W,
        sent_level: &[Protocol],
        recv_level: Option<&[Protocol]>,
    ) -> std::io::Result<()> {
        (*self).describe(w, sent_level, recv_level)
    }
}

impl Describe for PlanOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        sent_level: &[Protocol],
        recv_level: Option<&[Protocol]>,
    ) -> std::io::Result<()> {
        for (step_name, step) in &self.steps {
            writeln!(w, "------- step {step_name} --------")?;
            step.describe(&mut w, sent_level, recv_level)?;
        }
        Ok(())
    }
}

impl Describe for StepOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        sent_level: &[Protocol],
        recv_level: Option<&[Protocol]>,
    ) -> std::io::Result<()> {
        for (job_name, job) in &self.jobs {
            writeln!(w, "---- job {job_name} ----")?;
            job.describe(&mut w, sent_level, recv_level)?;
        }
        Ok(())
    }
}

impl Describe for JobOutput {
    fn describe<W: Write>(
        &self,
        mut w: W,
        sent_level: &[Protocol],
        recv_level: Option<&[Protocol]>,
    ) -> std::io::Result<()> {
        // TODO: escape or refuse to print dangerous term characters in output.
        for level in sent_level {
            match level {
                Protocol::TcpSegments => {
                    if let Some(segments) = &self.raw_tcp {
                        for seg in &segments.sent {
                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&seg.payload).replace("\n", "\n> ")
                            )?;
                        }
                    }
                }
                Protocol::Tcp => {
                    if let Some(tcp) = &self.tcp {
                        if let Some(req) = &tcp.sent {
                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                            )?;
                            if let Some(ttfb) = &req.time_to_first_byte {
                                writeln!(w, "sent time to first byte: {}", ttfb.0)?;
                            }
                        }
                    }
                }
                Protocol::Tls => {
                    if let Some(tls) = &self.tls {
                        if let Some(req) = &tls.request {
                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                            )?;
                            if let Some(ttfb) = &req.time_to_first_byte {
                                writeln!(w, "request time to first byte: {}", ttfb.0)?;
                            }
                        }
                    }
                }
                Protocol::Http2Frames => {
                    if let Some(http) = self.raw_http2() {
                        for frame in &http.sent {
                            print_frame(&mut w, '>', frame)?;
                        }
                    }
                }
                Protocol::Http => {
                    if let Some(http) = &self.http {
                        writeln!(
                            w,
                            "> {}{} {}",
                            http.request
                                .as_ref()
                                .map(|req| req.method.as_ref())
                                .flatten()
                                .as_deref()
                                .map(|x| String::from_utf8_lossy(x) + " ")
                                .unwrap_or_default(),
                            http.request
                                .as_ref()
                                .map(|req| req.url.to_string())
                                .unwrap_or_else(|| "".to_owned()),
                            http.protocol
                                .as_ref()
                                .map(String::as_str)
                                .unwrap_or_default(),
                        )?;
                        if let Some(req) = &http.request {
                            for (k, v) in &req.headers {
                                writeln!(
                                    w,
                                    ">   {}: {}",
                                    String::from_utf8_lossy(k),
                                    String::from_utf8_lossy(v)
                                )?;
                            }

                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                            )?;
                            if let Some(ttfb) = &req.time_to_first_byte {
                                writeln!(w, "request time to first byte: {}", ttfb.0)?;
                            }

                            writeln!(w, "request duration: {}", req.duration.0)?;
                        }
                    }
                    if let Some(http) = self.http1() {
                        writeln!(
                            w,
                            "> {}{}{}",
                            http.request
                                .as_ref()
                                .map(|req| req.method.as_ref())
                                .flatten()
                                .as_deref()
                                .map(|x| String::from_utf8_lossy(x) + " ")
                                .unwrap_or_default(),
                            http.request
                                .as_ref()
                                .map(|req| req.url.to_string())
                                .unwrap_or_else(|| "".to_owned()),
                            http.request
                                .as_ref()
                                .map(|req| req
                                    .version_string
                                    .as_ref()
                                    .map(|v| " ".to_owned() + &String::from_utf8_lossy(v))
                                    .unwrap_or_default())
                                .unwrap_or_default(),
                        )?;
                        if let Some(req) = &http.request {
                            for (k, v) in &req.headers {
                                writeln!(
                                    w,
                                    ">   {}: {}",
                                    String::from_utf8_lossy(k),
                                    String::from_utf8_lossy(v)
                                )?;
                            }
                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                            )?;
                            if let Some(ttfb) = &req.time_to_first_byte {
                                writeln!(w, "request time to first byte: {}", ttfb.0)?;
                            }
                            writeln!(w, "request duration: {}", req.duration.0)?;
                        }
                    }
                    if let Some(http) = self.http2() {
                        writeln!(
                            w,
                            "> {}{} HTTP/2",
                            http.request
                                .as_ref()
                                .map(|req| req.method.as_ref())
                                .flatten()
                                .as_deref()
                                .map(|x| String::from_utf8_lossy(x) + " ")
                                .unwrap_or_default(),
                            http.request
                                .as_ref()
                                .map(|req| req.url.to_string())
                                .unwrap_or_else(|| "".to_owned()),
                        )?;
                        if let Some(req) = &http.request {
                            for (k, v) in &req.headers {
                                writeln!(
                                    w,
                                    ">   {}: {}",
                                    String::from_utf8_lossy(k),
                                    String::from_utf8_lossy(v)
                                )?;
                            }
                            writeln!(
                                w,
                                "> {}",
                                String::from_utf8_lossy(&req.body).replace("\n", "\n> ")
                            )?;
                            if let Some(ttfb) = &req.time_to_first_byte {
                                writeln!(w, "request time to first byte: {}", ttfb.0)?;
                            }
                            writeln!(w, "request duration: {}", req.duration.0)?;
                        }
                    }
                }
                Protocol::Graphql => {
                    if let Some(req) = self
                        .graphql
                        .as_ref()
                        .map(|tcp| tcp.request.as_ref())
                        .flatten()
                    {
                        writeln!(w, "> {}", &req.query.replace("\n", "\n> "))?;
                        writeln!(w, "request duration: {}", req.duration.0)?;
                    }
                }
                _ => {}
            }
        }

        let recv_level = recv_level.map(|level| level).unwrap_or_else(|| {
            if self.graphql.is_some() {
                &[Protocol::Graphql]
            //} else if proto.http3.is_some() {
            //    vec![Protocol::HTTP]
            } else if self.http2().is_some() {
                &[Protocol::Http]
            } else if self.http1().is_some() {
                &[Protocol::Http]
            } else if self.http.is_some() {
                &[Protocol::Http]
            } else if self.raw_http2().is_some() {
                &[Protocol::Http2Frames]
            } else if self.tls.is_some() {
                &[Protocol::Tls]
            } else if self.tcp.is_some() {
                &[Protocol::Tcp]
            } else if self.raw_tcp.is_some() {
                &[Protocol::TcpSegments]
            } else {
                &[]
            }
        });

        for level in recv_level {
            // Default output is at the highest level protocol in the request.
            match level {
                Protocol::TcpSegments => {
                    if let Some(raw) = &self.raw_tcp {
                        for segment in &raw.received {
                            writeln!(w, "< {segment:?}")?;
                        }
                        for e in &raw.errors {
                            writeln!(w, "{} error: {}", e.kind, e.message)?;
                        }
                        //for p in &raw.pause.handshake.start {
                        //    writeln!(w,"handshake start pause duration: {}", p.duration)?;
                        //}
                        //for p in &raw.pause.handshake.end {
                        //    writeln!(w,"handshake end pause duration: {}", p.duration)?;
                        //}
                        writeln!(w, "total duration: {}", raw.duration.0)?;
                    }
                }
                Protocol::Tcp => {
                    if let Some(tcp) = &self.tcp {
                        if let Some(resp) = &tcp.received {
                            writeln!(
                                w,
                                "< {}",
                                String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                            )?;
                            if let Some(ttfb) = &resp.time_to_first_byte {
                                writeln!(w, "response time to first byte: {}", ttfb.0)?;
                            }
                        }
                        for e in &tcp.errors {
                            writeln!(w, "{} error: {}", e.kind, e.message)?;
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
                        writeln!(w, "total duration: {}", tcp.duration.0)?;
                    }
                }
                Protocol::Tls => {
                    if let Some(tls) = &self.tls {
                        if let Some(resp) = &tls.response {
                            writeln!(
                                w,
                                "< {}",
                                String::from_utf8_lossy(&resp.body).replace("\n", "\n< ")
                            )?;
                            if let Some(ttfb) = &resp.time_to_first_byte {
                                writeln!(w, "response time to first byte: {}", ttfb.0)?;
                            }
                        }
                        for e in &tls.errors {
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
                        writeln!(w, "total duration: {}", tls.duration.0)?;
                    }
                }
                Protocol::Http2Frames => {
                    if let Some(http) = self.raw_http2() {
                        for frame in &http.received {
                            print_frame(&mut w, '<', frame)?;
                        }
                    }
                }
                Protocol::Http => {
                    if let Some(http) = &self.http {
                        if let Some(resp) = &http.response {
                            writeln!(
                                w,
                                "< {} {}",
                                http.response
                                    .as_ref()
                                    .map(|resp| resp.status_code)
                                    .flatten()
                                    .unwrap_or(0),
                                http.protocol
                                    .as_ref()
                                    .map(String::as_str)
                                    .unwrap_or_default()
                            )?;
                            if let Some(headers) = &resp.headers {
                                for (k, v) in headers {
                                    writeln!(
                                        w,
                                        "< {}: {}",
                                        String::from_utf8_lossy(&k),
                                        String::from_utf8_lossy(&v)
                                    )?;
                                }
                            }
                            writeln!(w, "< ")?;
                            if let Some(body) = &resp.body {
                                writeln!(
                                    w,
                                    "< {}",
                                    String::from_utf8_lossy(&body).replace("\n", "\n< ")
                                )?;
                            }
                            if let Some(ttfb) = &resp.time_to_first_byte {
                                writeln!(w, "response time to first byte: {}", ttfb.0)?;
                            }
                            writeln!(w, "response duration: {}", resp.duration.0)?;
                        }
                        for e in &http.errors {
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
                        writeln!(w, "total duration: {}", http.duration.0)?;
                    }
                    if let Some(http) = self.http1() {
                        if let Some(resp) = &http.response {
                            writeln!(
                                w,
                                "< {} {}",
                                resp.status_code.unwrap_or(0),
                                resp.protocol
                                    .as_ref()
                                    .map(|x| String::from_utf8_lossy(x))
                                    .unwrap_or_default()
                            )?;
                            if let Some(headers) = &resp.headers {
                                for (k, v) in headers {
                                    writeln!(
                                        w,
                                        "< {}: {}",
                                        String::from_utf8_lossy(&k),
                                        String::from_utf8_lossy(&v)
                                    )?;
                                }
                            }
                            writeln!(w, "< ")?;
                            if let Some(body) = &resp.body {
                                writeln!(
                                    w,
                                    "< {}",
                                    String::from_utf8_lossy(&body).replace("\n", "\n< ")
                                )?;
                            }
                            if let Some(ttfb) = &resp.time_to_first_byte {
                                writeln!(w, "response time to first byte: {}", ttfb.0)?;
                            }
                            writeln!(w, "response duration: {}", resp.duration.0)?;
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
                        writeln!(w, "total duration: {}", http.duration.0)?;
                    }
                    if let Some(http) = self.http2() {
                        if let Some(resp) = &http.response {
                            writeln!(w, "< {} HTTP/2", resp.status_code.unwrap_or(0))?;
                            if let Some(headers) = &resp.headers {
                                for (k, v) in headers {
                                    writeln!(
                                        w,
                                        "< {}: {}",
                                        String::from_utf8_lossy(&k),
                                        String::from_utf8_lossy(&v)
                                    )?;
                                }
                            }
                            writeln!(w, "< ")?;
                            if let Some(body) = &resp.body {
                                writeln!(
                                    w,
                                    "< {}",
                                    String::from_utf8_lossy(&body).replace("\n", "\n< ")
                                )?;
                            }
                            if let Some(ttfb) = &resp.time_to_first_byte {
                                writeln!(w, "response time to first byte: {}", ttfb.0)?;
                            }
                            writeln!(w, "response duration: {}", resp.duration.0)?;
                        }
                        for e in &http.errors {
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
                        writeln!(w, "total duration: {}", http.duration.0)?;
                    }
                }
                Protocol::Graphql => {
                    if let Some(gql) = &self.graphql {
                        if let Some(resp) = &gql.response {
                            writeln!(
                                w,
                                "< {}",
                                serde_json::to_string_pretty(&resp.json)
                                    .unwrap()
                                    .replace("\n", "\n< ")
                            )?;
                            writeln!(w, "response duration: {}", resp.duration.0)?;
                        }
                        for e in &gql.errors {
                            writeln!(w, "{} error: {}", e.kind, e.message)?;
                        }
                        writeln!(w, "total duration: {}", gql.duration.0)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn print_frame<W: Write>(
    mut w: W,
    direction: char,
    frame: &Http2FrameOutput,
) -> std::io::Result<()> {
    let d = direction;
    writeln!(w, "{d} type: {:}", frame.r#type())?;
    writeln!(w, "{d} flags: {:#010b}", frame.flags().bits())?;
    match frame {
        Http2FrameOutput::Data(frame) => {
            writeln!(w, "{d}   END_STREAM: {}", frame.end_stream)?;
            writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
        }
        Http2FrameOutput::Headers(frame) => {
            writeln!(w, "{d}   END_STREAM: {}", frame.end_stream)?;
            writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers)?;
            writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
            writeln!(w, "{d}   PRIORITY: {}", frame.priority.is_some())?;
        }
        Http2FrameOutput::Settings(frame) => {
            writeln!(w, "{d}   ACK: {}", frame.ack)?;
        }
        Http2FrameOutput::PushPromise(frame) => {
            writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers())?;
            writeln!(w, "{d}   PADDING: {}", frame.padding.is_some())?;
        }
        Http2FrameOutput::Ping(frame) => {
            writeln!(w, "{d}   ACK: {}", frame.ack)?;
        }
        Http2FrameOutput::Continuation(frame) => {
            writeln!(w, "{d}   END_HEADERS: {}", frame.end_headers)?;
        }
        _ => {}
    }
    let r = frame.r();
    if r {
        writeln!(w, "{d} r: 1")?;
    }
    writeln!(w, "{d} stream_id: {}", frame.stream_id())?;
    match frame {
        Http2FrameOutput::Data(frame) => {
            writeln!(w, "{d} data: {:?}", frame.data)?;
            if frame
                .padding
                .as_ref()
                .is_some_and(|pad| pad.iter().any(|byte| *byte != 0))
            {
                writeln!(w, "{d} padding: {:?}", frame.padding)?;
            }
        }
        Http2FrameOutput::Headers(frame) => {
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
        Http2FrameOutput::Settings(frame) => {
            writeln!(w, "{d} parameters:")?;
            for param in &frame.parameters {
                writeln!(w, "{d}   ID: {}", param.id)?;
                writeln!(w, "{d}   value: {:#06x}", param.value)?;
            }
        }
        Http2FrameOutput::PushPromise(frame) => {
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
        Http2FrameOutput::Ping(frame) => {
            writeln!(w, "{d} data: {:?}", frame.data)?;
        }
        Http2FrameOutput::Continuation(frame) => {
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
