use anyhow::anyhow;
use bytes::Bytes;
use itertools::Itertools;

use crate::{
    bindings, BytesOutput, Error, Evaluate, Http2ContinuationFrameOutput, Http2DataFrameOutput,
    Http2FrameFlag, Http2FrameOutput, Http2FrameType, Http2GenericFrameOutput,
    Http2GoawayFrameOutput, Http2HeadersFrameOutput, Http2HeadersFramePriorityOutput,
    Http2PingFrameOutput, Http2PriorityFrameOutput, Http2PushPromiseFrameOutput,
    Http2RstStreamFrameOutput, Http2SettingsFrameOutput, Http2SettingsParameterId,
    Http2SettingsParameterOutput, Http2WindowUpdateFrameOutput, MaybeUtf8, PlanValue, Result,
    State,
};

#[derive(Debug, Clone)]
pub struct RawHttp2Request {
    pub host: PlanValue<String>,
    pub port: PlanValue<u16>,
    pub preamble: PlanValue<Option<MaybeUtf8>>,
    pub frames: Vec<Http2Frame>,
}

impl Evaluate<crate::RawHttp2PlanOutput> for RawHttp2Request {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> crate::Result<crate::RawHttp2PlanOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(crate::RawHttp2PlanOutput {
            host: self.host.evaluate(state)?,
            port: self.port.evaluate(state)?,
            preamble: self.preamble.evaluate(state)?,
            frames: self.frames.evaluate(state)?,
        })
    }
}

impl TryFrom<bindings::RawHttp2> for RawHttp2Request {
    type Error = Error;
    fn try_from(binding: bindings::RawHttp2) -> Result<Self> {
        Ok(Self {
            host: binding
                .host
                .map(PlanValue::<String>::try_from)
                .ok_or_else(|| anyhow!("tcp.host is required"))??,
            port: binding
                .port
                .map(PlanValue::<u16>::try_from)
                .ok_or_else(|| anyhow!("tcp.port is required"))??,
            preamble: binding.preamble.try_into()?,
            frames: binding
                .frames
                .into_iter()
                .flatten()
                .map(Http2Frame::try_from)
                .try_collect()?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum Http2Frame {
    Data(Http2DataFrame),
    Headers(Http2HeadersFrame),
    Priority(Http2PriorityFrame),
    RstStream(Http2RstStreamFrame),
    Settings(Http2SettingsFrame),
    PushPromise(Http2PushPromiseFrame),
    Ping(Http2PingFrame),
    Goaway(Http2GoawayFrame),
    WindowUpdate(Http2WindowUpdateFrame),
    Continuation(Http2ContinuationFrame),
    Generic(Http2GenericFrame),
}

impl Evaluate<Http2FrameOutput> for Http2Frame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2FrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        match self {
            Self::Data(frame) => Ok(Http2FrameOutput::Data(frame.evaluate(state)?)),
            Self::Headers(frame) => Ok(Http2FrameOutput::Headers(frame.evaluate(state)?)),
            Self::Priority(frame) => Ok(Http2FrameOutput::Priority(frame.evaluate(state)?)),
            Self::RstStream(frame) => Ok(Http2FrameOutput::RstStream(frame.evaluate(state)?)),
            Self::Settings(frame) => Ok(Http2FrameOutput::Settings(frame.evaluate(state)?)),
            Self::PushPromise(frame) => Ok(Http2FrameOutput::PushPromise(frame.evaluate(state)?)),
            Self::Ping(frame) => Ok(Http2FrameOutput::Ping(frame.evaluate(state)?)),
            Self::Goaway(frame) => Ok(Http2FrameOutput::Goaway(frame.evaluate(state)?)),
            Self::WindowUpdate(frame) => Ok(Http2FrameOutput::WindowUpdate(frame.evaluate(state)?)),
            Self::Continuation(frame) => Ok(Http2FrameOutput::Continuation(frame.evaluate(state)?)),
            Self::Generic(frame) => Ok(Http2FrameOutput::Generic(frame.evaluate(state)?)),
        }
    }
}

impl TryFrom<bindings::Http2Frame> for Http2Frame {
    type Error = Error;
    fn try_from(binding: bindings::Http2Frame) -> Result<Self> {
        match binding {
            bindings::Http2Frame::Data(frame) => Ok(Self::Data(frame.try_into()?)),
            bindings::Http2Frame::Headers(frame) => Ok(Self::Headers(frame.try_into()?)),
            bindings::Http2Frame::Priority(frame) => Ok(Self::Priority(frame.try_into()?)),
            bindings::Http2Frame::RstStream(frame) => Ok(Self::RstStream(frame.try_into()?)),
            bindings::Http2Frame::Settings(frame) => Ok(Self::Settings(frame.try_into()?)),
            bindings::Http2Frame::PushPromise(frame) => Ok(Self::PushPromise(frame.try_into()?)),
            bindings::Http2Frame::Ping(frame) => Ok(Self::Ping(frame.try_into()?)),
            bindings::Http2Frame::Goaway(frame) => Ok(Self::Goaway(frame.try_into()?)),
            bindings::Http2Frame::WindowUpdate(frame) => Ok(Self::WindowUpdate(frame.try_into()?)),
            bindings::Http2Frame::Continuation(frame) => Ok(Self::Continuation(frame.try_into()?)),
            bindings::Http2Frame::Generic(frame) => Ok(Self::Generic(frame.try_into()?)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Http2DataFrame {
    end_stream: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    data: PlanValue<BytesOutput>,
    padding: Option<PlanValue<BytesOutput>>,
}

impl TryFrom<bindings::Http2DataFrame> for Http2DataFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2DataFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            end_stream: binding
                .end_stream
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("stream_id is required for HTTP2 DATA frame"))??,
            data: binding
                .data
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
            padding: binding.padding.map(PlanValue::try_from).transpose()?,
        })
    }
}

impl Evaluate<Http2DataFrameOutput> for Http2DataFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2DataFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let end_stream = self.end_stream.evaluate(state)?;
        if end_stream {
            flags |= Http2FrameFlag::EndStream;
        }
        if self.padding.is_some() {
            flags |= Http2FrameFlag::Padded
        }
        Ok(Http2DataFrameOutput {
            flags,
            end_stream,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            data: self.data.evaluate(state)?,
            padding: self.padding.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2HeadersFrame {
    end_stream: PlanValue<bool>,
    end_headers: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    priority: Option<Http2HeadersFramePriority>,
    header_block_fragment: PlanValue<BytesOutput>,
    padding: Option<PlanValue<BytesOutput>>,
}

impl TryFrom<bindings::Http2HeadersFrame> for Http2HeadersFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2HeadersFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            end_stream: binding
                .end_stream
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            end_headers: binding
                .end_headers
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("stream_id is required for HTTP2 HEADERS frame"))??,
            priority: binding
                .priority
                .map(Http2HeadersFramePriority::try_from)
                .transpose()?,
            header_block_fragment: binding
                .header_block_fragment
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("header_block_fragment is required for HTTP2 HEADERS frame")
                })??,
            padding: binding.padding.map(PlanValue::try_from).transpose()?,
        })
    }
}

impl Evaluate<Http2HeadersFrameOutput> for Http2HeadersFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2HeadersFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let end_stream = self.end_stream.evaluate(state)?;
        if end_stream {
            flags |= Http2FrameFlag::EndStream;
        }
        let end_headers = self.end_headers.evaluate(state)?;
        if end_headers {
            flags |= Http2FrameFlag::EndHeaders;
        }
        if self.padding.is_some() {
            flags |= Http2FrameFlag::Padded
        }
        if self.priority.is_some() {
            flags |= Http2FrameFlag::Priority
        }
        Ok(Http2HeadersFrameOutput {
            flags,
            end_stream,
            end_headers,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            priority: self.priority.evaluate(state)?,
            header_block_fragment: self.header_block_fragment.evaluate(state)?,
            padding: self.padding.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2HeadersFramePriority {
    e: PlanValue<bool>,
    stream_dependency: PlanValue<u32>,
    weight: PlanValue<u8>,
}

impl TryFrom<bindings::Http2HeadersFramePriority> for Http2HeadersFramePriority {
    type Error = Error;
    fn try_from(
        binding: bindings::Http2HeadersFramePriority,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            e: binding
                .e
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_dependency: binding
                .stream_dependency
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("priority.stream_dependency is required for HTTP2 HEADERS frame")
                })??,
            weight: binding
                .weight
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("priority.weight is required for HTTP2 HEADERS frame"))??,
        })
    }
}

impl Evaluate<Http2HeadersFramePriorityOutput> for Http2HeadersFramePriority {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2HeadersFramePriorityOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2HeadersFramePriorityOutput {
            e: self.e.evaluate(state)?,
            stream_dependency: self.stream_dependency.evaluate(state)?,
            weight: self.weight.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PriorityFrame {
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    e: PlanValue<bool>,
    stream_dependency: PlanValue<u32>,
    weight: PlanValue<u8>,
}

impl TryFrom<bindings::Http2PriorityFrame> for Http2PriorityFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2PriorityFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("stream_id is required for HTTP2 PRIORITY frame"))??,
            e: binding
                .e
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_dependency: binding
                .stream_dependency
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("stream_dependency is required for HTTP2 PRIORITY frame")
                })??,
            weight: binding
                .weight
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("weight is required for HTTP2 PRIORITY frame"))??,
        })
    }
}

impl Evaluate<Http2PriorityFrameOutput> for Http2PriorityFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2PriorityFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2PriorityFrameOutput {
            flags: 0.into(),
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            e: self.e.evaluate(state)?,
            stream_dependency: self.stream_dependency.evaluate(state)?,
            weight: self.weight.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2RstStreamFrame {
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    error_code: PlanValue<u32>,
}

impl TryFrom<bindings::Http2RstStreamFrame> for Http2RstStreamFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2RstStreamFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("stream_id is required for HTTP2 RST_STREAM frame"))??,
            error_code: binding
                .error_code
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl Evaluate<Http2RstStreamFrameOutput> for Http2RstStreamFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2RstStreamFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2RstStreamFrameOutput {
            flags: 0.into(),
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            error_code: self.error_code.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2SettingsFrame {
    ack: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    parameters: Vec<Http2SettingsParameter>,
}

impl TryFrom<bindings::Http2SettingsFrame> for Http2SettingsFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2SettingsFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            ack: binding
                .ack
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(0)),
            parameters: binding
                .parameters
                .ok_or_else(|| anyhow!("parameters is required for HTTP2 SETTINGS frame"))?
                .into_iter()
                .map(Http2SettingsParameter::try_from)
                .try_collect()?,
        })
    }
}

impl Evaluate<Http2SettingsFrameOutput> for Http2SettingsFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2SettingsFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let ack = self.ack.evaluate(state)?;
        if ack {
            flags |= Http2FrameFlag::Ack;
        }
        Ok(Http2SettingsFrameOutput {
            flags,
            ack,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            parameters: self.parameters.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2SettingsParameter {
    id: PlanValue<u16>,
    value: PlanValue<u32>,
}

impl TryFrom<bindings::Http2SettingsParameter> for Http2SettingsParameter {
    type Error = Error;
    fn try_from(
        binding: bindings::Http2SettingsParameter,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            id: binding
                .id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("parameters.id is required for HTTP2 settings frame"))??,
            value: binding
                .value
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("parameters.value is required HTTP2 settings frame"))??,
        })
    }
}

impl Evaluate<Http2SettingsParameterOutput> for Http2SettingsParameter {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2SettingsParameterOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2SettingsParameterOutput {
            // TODO: Allow bindings (and cel?) to use named settings parameter IDs.
            id: Http2SettingsParameterId::new(self.id.evaluate(state)?),
            value: self.value.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PushPromiseFrame {
    end_headers: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    promised_r: PlanValue<bool>,
    promised_stream_id: PlanValue<u32>,
    header_block_fragment: PlanValue<BytesOutput>,
    padding: Option<PlanValue<BytesOutput>>,
}

impl TryFrom<bindings::Http2PushPromiseFrame> for Http2PushPromiseFrame {
    type Error = Error;
    fn try_from(
        binding: bindings::Http2PushPromiseFrame,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            end_headers: binding
                .end_headers
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding.stream_id.map(PlanValue::try_from).ok_or_else(|| {
                anyhow!("stream_id is required is required for HTTP2 PUSH_PROMISE frame")
            })??,
            promised_r: binding
                .promised_r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            promised_stream_id: binding
                .promised_stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("promised_stream_id is required for HTTP2 PUSH_PROMISE frame")
                })??,
            header_block_fragment: binding
                .header_block_fragment
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("header_block_fragment is required for HTTP2 PUSH_PROMISE frame")
                })??,
            padding: binding.padding.map(PlanValue::try_from).transpose()?,
        })
    }
}

impl Evaluate<Http2PushPromiseFrameOutput> for Http2PushPromiseFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2PushPromiseFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let end_headers = self.end_headers.evaluate(state)?;
        if end_headers {
            flags |= Http2FrameFlag::EndHeaders;
        }
        if self.padding.is_some() {
            flags |= Http2FrameFlag::Padded
        }
        Ok(Http2PushPromiseFrameOutput {
            flags,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            promised_r: self.promised_r.evaluate(state)?,
            promised_stream_id: self.promised_stream_id.evaluate(state)?,
            header_block_fragment: self.header_block_fragment.evaluate(state)?,
            padding: self.padding.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2PingFrame {
    ack: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    data: PlanValue<BytesOutput>,
}

impl TryFrom<bindings::Http2PingFrame> for Http2PingFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2PingFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            ack: binding
                .ack
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(0)),
            data: binding
                .data
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(BytesOutput::Bytes(Bytes::from(vec![0])))),
        })
    }
}

impl Evaluate<Http2PingFrameOutput> for Http2PingFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2PingFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let ack = self.ack.evaluate(state)?;
        if ack {
            flags |= Http2FrameFlag::Ack;
        }
        Ok(Http2PingFrameOutput {
            flags,
            ack,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            data: self.data.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2GoawayFrame {
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    last_r: PlanValue<bool>,
    last_stream_id: PlanValue<u32>,
    error_code: PlanValue<u32>,
    debug_data: PlanValue<MaybeUtf8>,
}

impl TryFrom<bindings::Http2GoawayFrame> for Http2GoawayFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2GoawayFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding.stream_id.map(PlanValue::try_from).ok_or_else(|| {
                anyhow!("stream_id is required is required for HTTP2 GOAWAY frame")
            })??,
            last_r: binding
                .last_r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            last_stream_id: binding
                .last_stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("last_stream_id is required is required for HTTP2 GOAWAY frame")
                })??,
            error_code: binding
                .error_code
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!("error_code is required is required for HTTP2 GOAWAY frame")
                })??,
            debug_data: binding
                .debug_data
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl Evaluate<Http2GoawayFrameOutput> for Http2GoawayFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2GoawayFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2GoawayFrameOutput {
            flags: 0.into(),
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            last_r: self.last_r.evaluate(state)?,
            last_stream_id: self.last_stream_id.evaluate(state)?,
            error_code: self.error_code.evaluate(state)?,
            debug_data: self.debug_data.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2WindowUpdateFrame {
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    window_r: PlanValue<bool>,
    window_size_increment: PlanValue<u32>,
}

impl TryFrom<bindings::Http2WindowUpdateFrame> for Http2WindowUpdateFrame {
    type Error = Error;
    fn try_from(
        binding: bindings::Http2WindowUpdateFrame,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding.stream_id.map(PlanValue::try_from).ok_or_else(|| {
                anyhow!("stream_id is required is required for HTTP2 WINDOW_UPDATE frame")
            })??,
            window_r: binding
                .window_r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            window_size_increment: binding
                .window_size_increment
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!(
                        "window_size_increment is required is required for HTTP2 WINDOW_UPDATE frame"
                    )
                })??,
        })
    }
}

impl Evaluate<Http2WindowUpdateFrameOutput> for Http2WindowUpdateFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2WindowUpdateFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2WindowUpdateFrameOutput {
            flags: 0.into(),
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            window_r: self.window_r.evaluate(state)?,
            window_size_increment: self.window_size_increment.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2ContinuationFrame {
    end_headers: PlanValue<bool>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    header_block_fragment: PlanValue<BytesOutput>,
}

impl TryFrom<bindings::Http2ContinuationFrame> for Http2ContinuationFrame {
    type Error = Error;
    fn try_from(
        binding: bindings::Http2ContinuationFrame,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            end_headers: binding
                .end_headers
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding.stream_id.map(PlanValue::try_from).ok_or_else(|| {
                anyhow!("stream_id is required is required for HTTP2 CONTINUATION frame")
            })??,
            header_block_fragment: binding
                .header_block_fragment
                .map(PlanValue::try_from)
                .ok_or_else(|| {
                    anyhow!(
                        "header_block_fragment is required is required for HTTP2 CONTINUATION frame"
                    )
                })??,
        })
    }
}

impl Evaluate<Http2ContinuationFrameOutput> for Http2ContinuationFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2ContinuationFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        let mut flags: Http2FrameFlag = 0.into();
        let end_headers = self.end_headers.evaluate(state)?;
        if end_headers {
            flags |= Http2FrameFlag::EndHeaders;
        }
        Ok(Http2ContinuationFrameOutput {
            flags: 0.into(),
            end_headers,
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            header_block_fragment: self.header_block_fragment.evaluate(state)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Http2GenericFrame {
    r#type: PlanValue<u8>,
    flags: PlanValue<u8>,
    r: PlanValue<bool>,
    stream_id: PlanValue<u32>,
    payload: PlanValue<BytesOutput>,
}

impl TryFrom<bindings::Http2GenericFrame> for Http2GenericFrame {
    type Error = Error;
    fn try_from(binding: bindings::Http2GenericFrame) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            r#type: binding
                .r#type
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("type is required is required for HTTP2 frame"))??,
            flags: binding
                .flags
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(0)),
            r: binding
                .r
                .map(PlanValue::try_from)
                .transpose()?
                .unwrap_or(PlanValue::Literal(false)),
            stream_id: binding
                .stream_id
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("stream_id is required is required for HTTP2 frame"))??,
            payload: binding
                .payload
                .map(PlanValue::try_from)
                .ok_or_else(|| anyhow!("payload is required is required for HTTP2 frame"))??,
        })
    }
}

impl Evaluate<Http2GenericFrameOutput> for Http2GenericFrame {
    fn evaluate<'a, S, O, I>(&self, state: &S) -> Result<Http2GenericFrameOutput>
    where
        S: State<'a, O, I>,
        O: Into<&'a str>,
        I: IntoIterator<Item = O>,
    {
        Ok(Http2GenericFrameOutput {
            // TODO: allow using type and flag names for in bindings for generic frames.
            r#type: Http2FrameType::new(self.r#type.evaluate(state)?),
            flags: self.flags.evaluate(state)?.into(),
            r: self.r.evaluate(state)?,
            stream_id: self.stream_id.evaluate(state)?,
            payload: self.payload.evaluate(state)?,
        })
    }
}
