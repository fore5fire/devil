use std::io;
use std::{fmt::Display, sync::Arc};

use bitmask_enum::bitmask;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, Bytes};
use cel_interpreter::Duration;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{BytesOutput, Direction, MaybeUtf8, PduName, ProtocolName};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", rename = "raw_http2_frame")]
pub struct Http2FrameOutput {
    pub name: PduName,
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    #[serde(flatten)]
    pub payload: Http2FramePayloadOutput,
    pub direction: Direction,
}

impl Http2FrameOutput {
    pub fn new(
        name: PduName,
        frame_type: Http2FrameType,
        flags: Http2FrameFlag,
        r: bool,
        stream_id: u32,
        payload: Bytes,
        direction: Direction,
    ) -> Self {
        Self {
            name,
            flags,
            r,
            stream_id,
            payload: Http2FramePayloadOutput::new(frame_type, flags, payload),
            direction,
        }
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        write_frame_header(
            &mut writer,
            self.payload.r#type(),
            self.flags,
            to_u31(self.r, self.stream_id),
            self.payload.len(),
        )
        .await?;

        match &self.payload {
            Http2FramePayloadOutput::Data(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Headers(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Priority(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::RstStream(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Settings(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::PushPromise(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Ping(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Goaway(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::WindowUpdate(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Continuation(frame) => frame.write_payload(writer).await,
            Http2FramePayloadOutput::Generic(frame) => frame.write_payload(writer).await,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Http2FramePayloadOutput {
    Data(Http2DataFrameOutput),
    Headers(Http2HeadersFrameOutput),
    Priority(Http2PriorityFrameOutput),
    RstStream(Http2RstStreamFrameOutput),
    Settings(Http2SettingsFrameOutput),
    PushPromise(Http2PushPromiseFrameOutput),
    Ping(Http2PingFrameOutput),
    Goaway(Http2GoawayFrameOutput),
    WindowUpdate(Http2WindowUpdateFrameOutput),
    Continuation(Http2ContinuationFrameOutput),
    Generic(Http2GenericFrameOutput),
}

impl Http2FramePayloadOutput {
    pub fn r#type(&self) -> Http2FrameType {
        match self {
            Self::Data(_) => Http2FrameType::Data,
            Self::Headers(_) => Http2FrameType::Headers,
            Self::Priority(_) => Http2FrameType::Priority,
            Self::RstStream(_) => Http2FrameType::RstStream,
            Self::Settings(_) => Http2FrameType::Settings,
            Self::PushPromise(_) => Http2FrameType::PushPromise,
            Self::Ping(_) => Http2FrameType::Ping,
            Self::Goaway(_) => Http2FrameType::Goaway,
            Self::WindowUpdate(_) => Http2FrameType::WindowUpdate,
            Self::Continuation(_) => Http2FrameType::Continuation,
            Self::Generic(frame) => frame.r#type,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Data(kind) => kind.len(),
            Self::Headers(kind) => kind.len(),
            Self::Priority(kind) => kind.len(),
            Self::RstStream(kind) => kind.len(),
            Self::Settings(kind) => kind.len(),
            Self::PushPromise(kind) => kind.len(),
            Self::Ping(kind) => kind.len(),
            Self::Goaway(kind) => kind.len(),
            Self::WindowUpdate(kind) => kind.len(),
            Self::Continuation(kind) => kind.len(),
            Self::Generic(kind) => kind.len(),
        }
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        match self {
            Self::Data(frame) => frame.compute_flags(),
            Self::Headers(frame) => frame.compute_flags(),
            Self::Priority(_) => Http2FrameFlag::none(),
            Self::RstStream(_) => Http2FrameFlag::none(),
            Self::Settings(_) => Http2FrameFlag::none(),
            Self::PushPromise(frame) => frame.compute_flags(),
            Self::Ping(frame) => frame.compute_flags(),
            Self::Goaway(_) => Http2FrameFlag::none(),
            Self::WindowUpdate(_) => Http2FrameFlag::none(),
            Self::Continuation(frame) => frame.compute_flags(),
            Self::Generic(_) => Http2FrameFlag::none(),
        }
    }

    pub fn new(kind: Http2FrameType, flags: Http2FrameFlag, mut payload: Bytes) -> Self {
        match kind {
            Http2FrameType::Data if payload.len() >= Http2FrameFlag::Padded.min_bytes(flags) => {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload.advance(1);
                }
                Self::Data(Http2DataFrameOutput {
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    data: payload.split_to(payload.len() - pad_len).into(),
                    padding: padded.then(|| payload.into()),
                })
            }
            Http2FrameType::Headers
                if payload.len()
                    >= Http2FrameFlag::Padded.min_bytes(flags)
                        + Http2FrameFlag::Priority.min_bytes(flags) =>
            {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload.advance(1);
                }
                let priority = flags.contains(Http2FrameFlag::Priority).then(|| {
                    Http2HeadersFramePriorityOutput {
                        e: payload[0] & 1 << 7 != 0,
                        stream_dependency: NetworkEndian::read_u32(&payload.split_to(4))
                            & !(1 << 31),
                        weight: payload.split_to(1)[0],
                    }
                });
                Self::Headers(Http2HeadersFrameOutput {
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                    priority,
                    header_block_fragment: payload.split_to(payload.len() - pad_len).into(),
                    padding: padded.then(|| payload.into()),
                })
            }
            Http2FrameType::Priority if payload.len() == 5 => {
                Self::Priority(Http2PriorityFrameOutput {
                    e: payload[0] & 1 << 7 != 0,
                    stream_dependency: NetworkEndian::read_u32(&payload.split_to(4)) & !(1 << 31),
                    weight: payload[0],
                })
            }
            Http2FrameType::RstStream if payload.len() == 4 => {
                Self::RstStream(Http2RstStreamFrameOutput {
                    error_code: NetworkEndian::read_u32(&payload),
                })
            }
            Http2FrameType::Settings if payload.len() % 6 == 0 => {
                Self::Settings(Http2SettingsFrameOutput {
                    ack: flags.contains(Http2FrameFlag::Ack),
                    parameters: payload
                        .chunks_exact(6)
                        .map(|chunk| Http2SettingsParameterOutput {
                            id: Http2SettingsParameterId::new(NetworkEndian::read_u16(chunk)),
                            value: NetworkEndian::read_u32(&chunk[2..]),
                        })
                        .collect(),
                })
            }
            Http2FrameType::PushPromise
                if payload.len() >= Http2FrameFlag::Padded.min_bytes(flags) + 4 =>
            {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload.split_to(1)[0]);
                }
                Self::PushPromise(Http2PushPromiseFrameOutput {
                    end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                    promised_r: payload[0] & 1 << 7 != 0,
                    promised_stream_id: NetworkEndian::read_u32(&payload.split_to(4)) & !(1 << 31),
                    header_block_fragment: payload.split_to(payload.len() - pad_len).into(),
                    padding: padded.then(|| payload.into()),
                })
            }
            Http2FrameType::Ping => Self::Ping(Http2PingFrameOutput {
                ack: flags.contains(Http2FrameFlag::Ack),
                data: payload.into(),
            }),
            Http2FrameType::Goaway if payload.len() >= 8 => Self::Goaway(Http2GoawayFrameOutput {
                last_r: payload[0] & 1 << 7 != 0,
                last_stream_id: NetworkEndian::read_u32(&payload.split_to(4)) & !(1 << 31),
                error_code: NetworkEndian::read_u32(&payload.split_to(4)),
                debug_data: MaybeUtf8(payload.into()),
            }),
            Http2FrameType::WindowUpdate if payload.len() == 4 => {
                Self::WindowUpdate(Http2WindowUpdateFrameOutput {
                    window_r: payload[0] & 1 << 7 != 0,
                    window_size_increment: NetworkEndian::read_u32(&payload) & !(1 << 31),
                })
            }
            Http2FrameType::Continuation => Self::Continuation(Http2ContinuationFrameOutput {
                end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                header_block_fragment: payload.into(),
            }),
            _ => Self::Generic(Http2GenericFrameOutput {
                r#type: kind,
                payload: payload.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Http2FrameType {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    Goaway,
    WindowUpdate,
    Continuation,
    Generic(u8),
}

impl Http2FrameType {
    #[inline]
    pub fn new(value: u8) -> Self {
        match value {
            0 => Self::Data,
            1 => Self::Headers,
            2 => Self::Priority,
            3 => Self::RstStream,
            4 => Self::Settings,
            5 => Self::PushPromise,
            6 => Self::Ping,
            7 => Self::Goaway,
            8 => Self::WindowUpdate,
            9 => Self::Continuation,
            val => Self::Generic(val),
        }
    }

    #[inline]
    pub fn value(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Headers => 1,
            Self::Priority => 2,
            Self::RstStream => 3,
            Self::Settings => 4,
            Self::PushPromise => 5,
            Self::Ping => 6,
            Self::Goaway => 7,
            Self::WindowUpdate => 8,
            Self::Continuation => 9,
            Self::Generic(val) => val,
        }
    }
}

impl Serialize for Http2FrameType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(self.value())
    }
}

impl Display for Http2FrameType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reparse the Http2FrameType to correctly print generic with a recognized type.
        match Http2FrameType::new(self.value()) {
            Self::Data => write!(f, "DATA"),
            Self::Headers => write!(f, "HEADERS"),
            Self::Priority => write!(f, "PRIORITY"),
            Self::RstStream => write!(f, "RST_STREAM"),
            Self::Settings => write!(f, "SETTINGS"),
            Self::PushPromise => write!(f, "PUSH_PROMISE"),
            Self::Ping => write!(f, "PING"),
            Self::Goaway => write!(f, "GOAWAY"),
            Self::WindowUpdate => write!(f, "WINDOW_UPDATE"),
            Self::Continuation => write!(f, "CONTINUATION"),
            Self::Generic(t) => write!(f, "{t:#04x}"),
        }
    }
}

#[bitmask(u8)]
#[derive(Serialize)]
pub enum Http2FrameFlag {
    Ack = 0x01,
    EndStream = 0x01,
    EndHeaders = 0x4,
    Padded = 0x8,
    Priority = 0x20,
}

impl Http2FrameFlag {
    #[inline]
    pub fn min_bytes(self, flags: Self) -> usize {
        if !flags.contains(self) {
            return 0;
        }
        match self {
            Self::Ack => 0,
            #[expect(unreachable_patterns)]
            Self::EndStream => 0,
            Self::EndHeaders => 0,
            Self::Padded => 1,
            Self::Priority => 5,
            _ => 0,
        }
    }
    #[inline]
    pub fn cond(self, cond: bool) -> Self {
        if cond {
            self
        } else {
            Self::none()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Http2SettingsParameterId {
    HeaderTableSize,
    EnablePush,
    MaxConcurrentStreams,
    InitialWindowSize,
    MaxFrameSize,
    MaxHeaderListSize,
    Generic(u16),
}

impl Http2SettingsParameterId {
    #[inline]
    pub fn new(value: u16) -> Self {
        match value {
            1 => Self::HeaderTableSize,
            2 => Self::EnablePush,
            3 => Self::MaxConcurrentStreams,
            4 => Self::InitialWindowSize,
            5 => Self::MaxFrameSize,
            6 => Self::MaxHeaderListSize,
            val => Self::Generic(val),
        }
    }

    #[inline]
    pub fn value(self) -> u16 {
        match self {
            Self::HeaderTableSize => 1,
            Self::EnablePush => 2,
            Self::MaxConcurrentStreams => 3,
            Self::InitialWindowSize => 4,
            Self::MaxFrameSize => 5,
            Self::MaxHeaderListSize => 6,
            Self::Generic(val) => val,
        }
    }
}

impl Serialize for Http2SettingsParameterId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u16(self.value())
    }
}

impl Display for Http2SettingsParameterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reparse the Http2FrameType to correctly print generic with a recognized type.
        match Self::new(self.value()) {
            Self::HeaderTableSize => write!(f, "HEADER_TABLE_SIZE"),
            Self::EnablePush => write!(f, "ENABLE_PUSH"),
            Self::MaxConcurrentStreams => write!(f, "MAX_CONCURRENT_STREAMS"),
            Self::InitialWindowSize => write!(f, "INITIAL_WINDOW_SIZE"),
            Self::MaxFrameSize => write!(f, "MAX_FRAME_SIZE"),
            Self::MaxHeaderListSize => write!(f, "MAX_HEADER_LIST_SIZE"),
            Self::Generic(t) => write!(f, "{t:#06x}"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2DataFrameOutput {
    pub end_stream: bool,
    pub data: BytesOutput,
    pub padding: Option<BytesOutput>,
}

impl Http2DataFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Data
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.padding
            .as_ref()
            .map(|p| p.len() + 1)
            .unwrap_or_default()
            + self.data.len()
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        Http2FrameFlag::EndStream.cond(self.end_stream)
            | Http2FrameFlag::Padded.cond(self.padding.is_some())
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Padding Length
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        writer.write_all(&self.data).await?;
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2HeadersFrameOutput {
    pub end_stream: bool,
    pub end_headers: bool,
    pub priority: Option<Http2HeadersFramePriorityOutput>,
    pub header_block_fragment: BytesOutput,
    pub padding: Option<BytesOutput>,
}

impl Http2HeadersFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Headers
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.padding
            .as_ref()
            .map(|p| p.len() + 1)
            .unwrap_or_default()
            + self.priority.as_ref().map(|_| 5).unwrap_or_default()
            + self.header_block_fragment.len()
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        Http2FrameFlag::EndStream.cond(self.end_stream)
            | Http2FrameFlag::EndHeaders.cond(self.end_headers)
            | Http2FrameFlag::Padded.cond(self.padding.is_some())
            | Http2FrameFlag::Priority.cond(self.priority.is_some())
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the padding length.
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        if let Some(priority) = &self.priority {
            writer
                .write_u32(to_u31(priority.e, priority.stream_dependency))
                .await?;
            writer.write_u8(priority.weight).await?;
        }
        writer.write_all(&self.header_block_fragment).await?;
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2HeadersFramePriorityOutput {
    pub e: bool,
    pub stream_dependency: u32,
    pub weight: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2PriorityFrameOutput {
    pub e: bool,
    pub stream_dependency: u32,
    pub weight: u8,
}

impl Http2PriorityFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Priority
    }

    #[inline]
    pub fn len(&self) -> usize {
        5
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        let mut buf = [0; 5];
        NetworkEndian::write_u32(&mut buf, to_u31(self.e, self.stream_dependency));
        buf[4] = self.weight;
        writer.write_all(&buf).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2RstStreamFrameOutput {
    pub error_code: u32,
}

impl Http2RstStreamFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::RstStream
    }

    #[inline]
    pub fn len(&self) -> usize {
        4
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        writer.write_u32(self.error_code).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2SettingsFrameOutput {
    pub ack: bool,
    pub parameters: Vec<Http2SettingsParameterOutput>,
}

impl Http2SettingsFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Settings
    }

    #[inline]
    pub fn len(&self) -> usize {
        6 * self.parameters.len()
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        let mut buf = [0; 6];
        for param in &self.parameters {
            NetworkEndian::write_u16(&mut buf, param.id.value());
            NetworkEndian::write_u32(&mut buf, param.value);
            writer.write_all(&buf).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2SettingsParameterOutput {
    pub id: Http2SettingsParameterId,
    pub value: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2PushPromiseFrameOutput {
    pub end_headers: bool,
    pub promised_r: bool,
    pub promised_stream_id: u32,
    pub header_block_fragment: BytesOutput,
    pub padding: Option<BytesOutput>,
}

impl Http2PushPromiseFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::PushPromise
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        Http2FrameFlag::EndHeaders.cond(self.end_headers)
            | Http2FrameFlag::Padded.cond(self.padding.is_some())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.padding
            .as_ref()
            .map(|p| p.len() + 1)
            .unwrap_or_default()
            + 4
            + self.header_block_fragment.len()
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the padding length.
        if let Some(padding) = &self.padding {
            writer
                .write_u8(
                    padding
                        .len()
                        .try_into()
                        .expect("padding length should fit in u8"),
                )
                .await?;
        }
        // Write the promised stream ID.
        writer
            .write_u32(to_u31(self.promised_r, self.promised_stream_id))
            .await?;
        // Write the header block fragment.
        writer.write_all(&self.header_block_fragment).await?;
        // Write the padding.
        if let Some(padding) = &self.padding {
            writer.write_all(&padding).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2PingFrameOutput {
    pub ack: bool,
    pub data: BytesOutput,
}

impl Http2PingFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Ping
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        Http2FrameFlag::Ack.cond(self.ack)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&self.data).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2GoawayFrameOutput {
    pub last_r: bool,
    pub last_stream_id: u32,
    pub error_code: u32,
    pub debug_data: MaybeUtf8,
}

impl Http2GoawayFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Goaway
    }

    #[inline]
    pub fn len(&self) -> usize {
        8
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        let mut buf = [0; 8];
        NetworkEndian::write_u32(&mut buf, to_u31(self.last_r, self.last_stream_id));
        NetworkEndian::write_u32(&mut buf, self.error_code);
        writer.write_all(&buf).await?;
        writer.write_all(&self.debug_data).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2WindowUpdateFrameOutput {
    pub window_r: bool,
    pub window_size_increment: u32,
}

impl Http2WindowUpdateFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::WindowUpdate
    }

    #[inline]
    pub fn len(&self) -> usize {
        4
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        writer
            .write_u32(to_u31(self.window_r, self.window_size_increment))
            .await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2ContinuationFrameOutput {
    pub end_headers: bool,
    pub header_block_fragment: BytesOutput,
}

impl Http2ContinuationFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Continuation
    }

    pub fn compute_flags(&self) -> Http2FrameFlag {
        Http2FrameFlag::EndHeaders.cond(self.end_headers)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.header_block_fragment.len()
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&self.header_block_fragment).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2GenericFrameOutput {
    pub r#type: Http2FrameType,
    pub payload: BytesOutput,
}

impl Http2GenericFrameOutput {
    #[inline]
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub async fn write_payload<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&self.payload).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename = "raw_http2")]
pub struct RawHttp2Output {
    pub name: ProtocolName,
    pub plan: RawHttp2PlanOutput,
    pub sent: Vec<Arc<Http2FrameOutput>>,
    pub received: Vec<Arc<Http2FrameOutput>>,
    pub errors: Vec<RawHttp2Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawHttp2PlanOutput {
    pub host: String,
    pub port: u16,
    pub preamble: Option<MaybeUtf8>,
    pub frames: Vec<Arc<Http2FrameOutput>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawHttp2Error {
    pub kind: String,
    pub message: String,
}

fn to_u31(r: bool, val: u32) -> u32 {
    (r as u32) << 31 | !(1 << 31) | val
}

#[inline]
async fn write_frame_header<W, E, S>(
    mut writer: W,
    kind: Http2FrameType,
    flags: Http2FrameFlag,
    stream_id: u32,
    payload_len: S,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    E: std::fmt::Debug,
    S: TryInto<u32, Error = E>,
{
    let len = payload_len
        .try_into()
        .expect("frame payload should fit in u24");
    let mut buf = [0; 9];
    // Length
    NetworkEndian::write_u24(&mut buf, len);
    buf[3] = kind.value();
    buf[4] = flags.bits();
    // Stream ID
    NetworkEndian::write_u32(&mut buf[5..], stream_id);
    writer.write_all(&buf).await
}
