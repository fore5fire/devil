use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    record::{Record, RecordWriter},
    ProtocolDiscriminants,
};

use super::{
    GraphqlOutput, GraphqlRequestOutput, GraphqlResponse, Http1Output, Http1RequestOutput,
    Http1Response, Http2FrameOutput, Http2Output, Http2RequestOutput, Http2Response, HttpOutput,
    HttpRequestOutput, HttpResponse, JobOutput, RawHttp2Output, RawTcpOutput, RunOutput,
    StepOutput, TcpOutput, TcpReceivedOutput, TcpSegmentOutput, TcpSentOutput, TlsOutput,
    TlsReceivedOutput, TlsSentOutput,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Normalize {
    Pdu,
    Protocol,
    Job,
    Step,
    None,
}

#[derive(Debug)]
pub enum Normalized {
    None(Vec<Arc<RunOutput>>),
    Step(Vec<Arc<StepOutput>>),
    Job(Vec<Arc<JobOutput>>),

    Graphql(Vec<Arc<GraphqlOutput>>),
    Http(Vec<Arc<HttpOutput>>),
    H1c(Vec<Arc<Http1Output>>),
    H1(Vec<Arc<Http1Output>>),
    H2c(Vec<Arc<Http2Output>>),
    RawH2c(Vec<Arc<RawHttp2Output>>),
    H2(Vec<Arc<Http2Output>>),
    RawH2(Vec<Arc<RawHttp2Output>>),
    //Http3(Arc<Http3Output>),
    Tls(Vec<Arc<TlsOutput>>),
    Tcp(Vec<Arc<TcpOutput>>),
    RawTcp(Vec<Arc<RawTcpOutput>>),

    GraphqlRequest(Vec<Arc<GraphqlRequestOutput>>),
    GraphqlResponse(Vec<Arc<GraphqlResponse>>),
    HttpRequest(Vec<Arc<HttpRequestOutput>>),
    HttpResponse(Vec<Arc<HttpResponse>>),
    H1cRequest(Vec<Arc<Http1RequestOutput>>),
    H1cResponse(Vec<Arc<Http1Response>>),
    H1Request(Vec<Arc<Http1RequestOutput>>),
    H1Response(Vec<Arc<Http1Response>>),
    H2cRequest(Vec<Arc<Http2RequestOutput>>),
    H2cResponse(Vec<Arc<Http2Response>>),
    H2Request(Vec<Arc<Http2RequestOutput>>),
    H2Response(Vec<Arc<Http2Response>>),
    RawH2cFrame(Vec<Arc<Http2FrameOutput>>),
    RawH2Frame(Vec<Arc<Http2FrameOutput>>),
    TlsSent(Vec<Arc<TlsSentOutput>>),
    TlsReceived(Vec<Arc<TlsReceivedOutput>>),
    TcpSent(Vec<Arc<TcpSentOutput>>),
    TcpReceived(Vec<Arc<TcpReceivedOutput>>),
    RawTcpSegment(Vec<Arc<TcpSegmentOutput>>),
}

impl Normalized {
    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            Self::None(x) => x.is_empty(),
            Self::Step(x) => x.is_empty(),
            Self::Job(x) => x.is_empty(),

            Self::Graphql(x) => x.is_empty(),
            Self::Http(x) => x.is_empty(),
            Self::H1c(x) => x.is_empty(),
            Self::H1(x) => x.is_empty(),
            Self::H2c(x) => x.is_empty(),
            Self::RawH2c(x) => x.is_empty(),
            Self::H2(x) => x.is_empty(),
            Self::RawH2(x) => x.is_empty(),
            Self::Tls(x) => x.is_empty(),
            Self::Tcp(x) => x.is_empty(),
            Self::RawTcp(x) => x.is_empty(),

            Self::GraphqlRequest(x) => x.is_empty(),
            Self::GraphqlResponse(x) => x.is_empty(),
            Self::HttpRequest(x) => x.is_empty(),
            Self::HttpResponse(x) => x.is_empty(),
            Self::H1cRequest(x) => x.is_empty(),
            Self::H1cResponse(x) => x.is_empty(),
            Self::H1Request(x) => x.is_empty(),
            Self::H1Response(x) => x.is_empty(),
            Self::H2cRequest(x) => x.is_empty(),
            Self::H2cResponse(x) => x.is_empty(),
            Self::H2Request(x) => x.is_empty(),
            Self::H2Response(x) => x.is_empty(),
            Self::RawH2cFrame(x) => x.is_empty(),
            Self::RawH2Frame(x) => x.is_empty(),
            Self::TlsSent(x) => x.is_empty(),
            Self::TlsReceived(x) => x.is_empty(),
            Self::TcpSent(x) => x.is_empty(),
            Self::TcpReceived(x) => x.is_empty(),
            Self::RawTcpSegment(x) => x.is_empty(),
        }
    }

    pub async fn write(
        &self,
        w: &mut RecordWriter,
        layers: &[ProtocolDiscriminants],
    ) -> crate::Result<()> {
        Ok(match self {
            Normalized::None(x) => w.write(x, layers).await?,
            Self::Step(x) => w.write(x, layers).await?,
            Self::Job(x) => w.write(x, layers).await?,

            Self::Graphql(x) => w.write(x, layers).await?,
            Self::Http(x) => w.write(x, layers).await?,
            Self::H1c(x) => w.write(x, layers).await?,
            Self::H1(x) => w.write(x, layers).await?,
            Self::H2c(x) => w.write(x, layers).await?,
            Self::RawH2c(x) => w.write(x, layers).await?,
            Self::H2(x) => w.write(x, layers).await?,
            Self::RawH2(x) => w.write(x, layers).await?,
            Self::Tls(x) => w.write(x, layers).await?,
            Self::Tcp(x) => w.write(x, layers).await?,
            Self::RawTcp(x) => w.write(x, layers).await?,

            Self::GraphqlRequest(x) => w.write(x, layers).await?,
            Self::GraphqlResponse(x) => w.write(x, layers).await?,
            Self::HttpRequest(x) => w.write(x, layers).await?,
            Self::HttpResponse(x) => w.write(x, layers).await?,
            Self::H1cRequest(x) => w.write(x, layers).await?,
            Self::H1cResponse(x) => w.write(x, layers).await?,
            Self::H1Request(x) => w.write(x, layers).await?,
            Self::H1Response(x) => w.write(x, layers).await?,
            Self::H2cRequest(x) => w.write(x, layers).await?,
            Self::H2cResponse(x) => w.write(x, layers).await?,
            Self::H2Request(x) => w.write(x, layers).await?,
            Self::H2Response(x) => w.write(x, layers).await?,
            Self::RawH2cFrame(x) => w.write(x, layers).await?,
            Self::RawH2Frame(x) => w.write(x, layers).await?,
            Self::TlsSent(x) => w.write(x, layers).await?,
            Self::TlsReceived(x) => w.write(x, layers).await?,
            Self::TcpSent(x) => w.write(x, layers).await?,
            Self::TcpReceived(x) => w.write(x, layers).await?,
            Self::RawTcpSegment(x) => w.write(x, layers).await?,
        })
    }
}

impl JobOutput {
    pub fn normalize(self: &Arc<Self>, target: Normalize) -> Vec<Normalized> {
        match target {
            Normalize::None | Normalize::Step => unreachable!(),
            Normalize::Job => vec![Normalized::Job(vec![self.clone()])],
            Normalize::Protocol => [
                self.graphql
                    .as_ref()
                    .cloned()
                    .map(|x| Normalized::Graphql(vec![x])),
                self.http
                    .as_ref()
                    .cloned()
                    .map(|x| Normalized::Http(vec![x])),
                self.h1.as_ref().cloned().map(|x| Normalized::H1(vec![x])),
                self.h1c.as_ref().cloned().map(|x| Normalized::H1c(vec![x])),
                self.h2.as_ref().cloned().map(|x| Normalized::H2(vec![x])),
                self.h2c.as_ref().cloned().map(|x| Normalized::H2c(vec![x])),
                self.raw_h2
                    .as_ref()
                    .cloned()
                    .map(|x| Normalized::RawH2(vec![x])),
                self.raw_h2c
                    .as_ref()
                    .cloned()
                    .map(|x| Normalized::RawH2c(vec![x])),
                self.tls.as_ref().cloned().map(|x| Normalized::Tls(vec![x])),
                self.tcp.as_ref().cloned().map(|x| Normalized::Tcp(vec![x])),
                self.raw_tcp
                    .as_ref()
                    .cloned()
                    .map(|x| Normalized::RawTcp(vec![x])),
            ]
            .into_iter()
            .filter_map(|x| x)
            .collect(),
            Normalize::Pdu => [
                self.graphql
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::GraphqlRequest(vec![req])),
                self.graphql
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::GraphqlResponse(vec![resp])),
                self.http
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::HttpRequest(vec![req])),
                self.http
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::HttpResponse(vec![resp])),
                self.h1
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::H1Request(vec![req])),
                self.h1
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::H1Response(vec![resp])),
                self.h1c
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::H1cRequest(vec![req])),
                self.h1c
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::H1cResponse(vec![resp])),
                self.h2
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::H2Request(vec![req])),
                self.h2
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::H2Response(vec![resp])),
                self.h2c
                    .as_ref()
                    .map(|x| x.request.clone())
                    .flatten()
                    .map(|req| Normalized::H2cRequest(vec![req])),
                self.h2c
                    .as_ref()
                    .map(|x| x.response.clone())
                    .flatten()
                    .map(|resp| Normalized::H2cResponse(vec![resp])),
                self.raw_h2.as_ref().map(|x| {
                    Normalized::RawH2Frame(
                        x.sent.iter().chain(x.received.iter()).cloned().collect(),
                    )
                }),
                self.raw_h2c.as_ref().map(|x| {
                    Normalized::RawH2cFrame(
                        x.sent.iter().chain(x.received.iter()).cloned().collect(),
                    )
                }),
                self.tls
                    .as_ref()
                    .map(|x| x.sent.clone())
                    .flatten()
                    .map(|req| Normalized::TlsSent(vec![req])),
                self.tls
                    .as_ref()
                    .map(|x| x.received.clone())
                    .flatten()
                    .map(|resp| Normalized::TlsReceived(vec![resp])),
                self.tcp
                    .as_ref()
                    .map(|x| x.sent.clone())
                    .flatten()
                    .map(|req| Normalized::TcpSent(vec![req])),
                self.tcp
                    .as_ref()
                    .map(|x| x.received.clone())
                    .flatten()
                    .map(|resp| Normalized::TcpReceived(vec![resp])),
                self.raw_tcp.as_ref().map(|x| {
                    Normalized::RawTcpSegment(
                        x.sent.iter().chain(x.received.iter()).cloned().collect(),
                    )
                }),
            ]
            .into_iter()
            .filter_map(|x| x)
            .collect(),
        }
    }
}

impl StepOutput {
    pub fn normalize(self: &Arc<Self>, target: Normalize) -> Vec<Normalized> {
        match target {
            Normalize::None => unreachable!(),
            Normalize::Step => vec![Normalized::Step(vec![self.clone()])],
            Normalize::Job => vec![Normalized::Job(self.jobs.values().cloned().collect())],
            Normalize::Protocol => [
                Normalized::Graphql(
                    self.jobs
                        .values()
                        .filter_map(|job| job.graphql.clone())
                        .collect(),
                ),
                Normalized::Http(
                    self.jobs
                        .values()
                        .filter_map(|job| job.http.clone())
                        .collect(),
                ),
                Normalized::H1(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1.clone())
                        .collect(),
                ),
                Normalized::H1c(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1c.clone())
                        .collect(),
                ),
                Normalized::H2(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2.clone())
                        .collect(),
                ),
                Normalized::H2c(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2c.clone())
                        .collect(),
                ),
                Normalized::RawH2(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_h2.clone())
                        .collect(),
                ),
                Normalized::RawH2c(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_h2c.clone())
                        .collect(),
                ),
                Normalized::Tls(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tls.clone())
                        .collect(),
                ),
                Normalized::Tcp(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tcp.clone())
                        .collect(),
                ),
                Normalized::RawTcp(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_tcp.clone())
                        .collect(),
                ),
            ]
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(),
            Normalize::Pdu => [
                Normalized::GraphqlRequest(
                    self.jobs
                        .values()
                        .filter_map(|job| job.graphql.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::GraphqlResponse(
                    self.jobs
                        .values()
                        .filter_map(|job| job.graphql.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::HttpRequest(
                    self.jobs
                        .values()
                        .filter_map(|job| job.http.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::HttpResponse(
                    self.jobs
                        .values()
                        .filter_map(|job| job.http.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H1Request(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H1Response(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H1cRequest(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1c.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H1cResponse(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h1c.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H2Request(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H2Response(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H2cRequest(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2c.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H2cResponse(
                    self.jobs
                        .values()
                        .filter_map(|job| job.h2c.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::RawH2Frame(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_h2.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
                Normalized::RawH2cFrame(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_h2c.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
                Normalized::TlsSent(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tls.as_ref())
                        .filter_map(|proto| proto.sent.clone())
                        .collect(),
                ),
                Normalized::TlsReceived(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tls.as_ref())
                        .filter_map(|proto| proto.received.clone())
                        .collect(),
                ),
                Normalized::TcpSent(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tcp.as_ref())
                        .filter_map(|proto| proto.sent.clone())
                        .collect(),
                ),
                Normalized::TcpReceived(
                    self.jobs
                        .values()
                        .filter_map(|job| job.tcp.as_ref())
                        .filter_map(|proto| proto.received.clone())
                        .collect(),
                ),
                Normalized::RawTcpSegment(
                    self.jobs
                        .values()
                        .filter_map(|job| job.raw_tcp.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
            ]
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(),
        }
    }
}

impl RunOutput {
    pub fn normalize(self: &Arc<Self>, target: Normalize) -> Vec<Normalized> {
        match target {
            Normalize::None => vec![Normalized::None(vec![self.clone()])],
            Normalize::Step => vec![Normalized::Step(self.steps.values().cloned().collect())],
            Normalize::Job => vec![Normalized::Job(
                self.steps
                    .values()
                    .map(|step| step.jobs.values())
                    .flatten()
                    .cloned()
                    .collect(),
            )],
            Normalize::Protocol => [
                Normalized::Graphql(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.graphql.clone())
                        .collect(),
                ),
                Normalized::Http(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.http.clone())
                        .collect(),
                ),
                Normalized::H1(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1.clone())
                        .collect(),
                ),
                Normalized::H1c(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1c.clone())
                        .collect(),
                ),
                Normalized::H2(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2.clone())
                        .collect(),
                ),
                Normalized::H2c(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2c.clone())
                        .collect(),
                ),
                Normalized::RawH2(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_h2.clone())
                        .collect(),
                ),
                Normalized::RawH2c(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_h2c.clone())
                        .collect(),
                ),
                Normalized::Tls(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tls.clone())
                        .collect(),
                ),
                Normalized::Tcp(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tcp.clone())
                        .collect(),
                ),
                Normalized::RawTcp(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_tcp.clone())
                        .collect(),
                ),
            ]
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(),
            Normalize::Pdu => [
                Normalized::GraphqlRequest(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.graphql.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::GraphqlResponse(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.graphql.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::HttpRequest(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.http.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::HttpResponse(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.http.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H1Request(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H1Response(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H1cRequest(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1c.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H1cResponse(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h1c.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H2Request(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H2Response(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::H2cRequest(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2c.as_ref())
                        .filter_map(|proto| proto.request.clone())
                        .collect(),
                ),
                Normalized::H2cResponse(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.h2c.as_ref())
                        .filter_map(|proto| proto.response.clone())
                        .collect(),
                ),
                Normalized::RawH2Frame(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_h2.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
                Normalized::RawH2cFrame(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_h2c.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
                Normalized::TlsSent(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tls.as_ref())
                        .filter_map(|proto| proto.sent.clone())
                        .collect(),
                ),
                Normalized::TlsReceived(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tls.as_ref())
                        .filter_map(|proto| proto.received.clone())
                        .collect(),
                ),
                Normalized::TcpSent(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tcp.as_ref())
                        .filter_map(|proto| proto.sent.clone())
                        .collect(),
                ),
                Normalized::TcpReceived(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.tcp.as_ref())
                        .filter_map(|proto| proto.received.clone())
                        .collect(),
                ),
                Normalized::RawTcpSegment(
                    self.steps
                        .values()
                        .map(|step| step.jobs.values())
                        .flatten()
                        .filter_map(|job| job.raw_tcp.as_ref())
                        .map(|proto| proto.sent.iter().chain(proto.received.iter()).cloned())
                        .flatten()
                        .collect(),
                ),
            ]
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(),
        }
    }
}
