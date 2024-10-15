use std::fmt::Display;
use std::io;

use bitmask_enum::bitmask;
use byteorder::{ByteOrder, NetworkEndian};
use cel_interpreter::Duration;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Http2FrameOutput {
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

impl Http2FrameOutput {
    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: W) -> io::Result<()> {
        match self {
            Self::Data(frame) => frame.write(writer).await,
            Self::Headers(frame) => frame.write(writer).await,
            Self::Priority(frame) => frame.write(writer).await,
            Self::RstStream(frame) => frame.write(writer).await,
            Self::Settings(frame) => frame.write(writer).await,
            Self::PushPromise(frame) => frame.write(writer).await,
            Self::Ping(frame) => frame.write(writer).await,
            Self::Goaway(frame) => frame.write(writer).await,
            Self::WindowUpdate(frame) => frame.write(writer).await,
            Self::Continuation(frame) => frame.write(writer).await,
            Self::Generic(frame) => frame.write(writer).await,
        }
    }

    pub fn flags(&self) -> Http2FrameFlag {
        match self {
            Self::Data(frame) => frame.flags,
            Self::Headers(frame) => frame.flags,
            Self::Priority(frame) => frame.flags,
            Self::RstStream(frame) => frame.flags,
            Self::Settings(frame) => frame.flags,
            Self::PushPromise(frame) => frame.flags,
            Self::Ping(frame) => frame.flags,
            Self::Goaway(frame) => frame.flags,
            Self::WindowUpdate(frame) => frame.flags,
            Self::Continuation(frame) => frame.flags,
            Self::Generic(frame) => frame.flags,
        }
    }

    pub fn r(&self) -> bool {
        match self {
            Self::Data(frame) => frame.r,
            Self::Headers(frame) => frame.r,
            Self::Priority(frame) => frame.r,
            Self::RstStream(frame) => frame.r,
            Self::Settings(frame) => frame.r,
            Self::PushPromise(frame) => frame.r,
            Self::Ping(frame) => frame.r,
            Self::Goaway(frame) => frame.r,
            Self::WindowUpdate(frame) => frame.r,
            Self::Continuation(frame) => frame.r,
            Self::Generic(frame) => frame.r,
        }
    }
    pub fn stream_id(&self) -> u32 {
        match self {
            Self::Data(frame) => frame.stream_id,
            Self::Headers(frame) => frame.stream_id,
            Self::Priority(frame) => frame.stream_id,
            Self::RstStream(frame) => frame.stream_id,
            Self::Settings(frame) => frame.stream_id,
            Self::PushPromise(frame) => frame.stream_id,
            Self::Ping(frame) => frame.stream_id,
            Self::Goaway(frame) => frame.stream_id,
            Self::WindowUpdate(frame) => frame.stream_id,
            Self::Continuation(frame) => frame.stream_id,
            Self::Generic(frame) => frame.stream_id,
        }
    }

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

    pub fn new(
        kind: Http2FrameType,
        flags: Http2FrameFlag,
        r: bool,
        stream_id: u32,
        mut payload: &[u8],
    ) -> Self {
        match kind {
            Http2FrameType::Data if payload.len() >= Http2FrameFlag::Padded.min_bytes(flags) => {
                let padded = flags.contains(Http2FrameFlag::Padded);
                let mut pad_len = 0;
                if padded {
                    pad_len = usize::from(payload[0]);
                    payload = &payload[1..];
                }
                Self::Data(Http2DataFrameOutput {
                    flags: flags.into(),
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    r,
                    stream_id,
                    data: payload[..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
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
                    payload = &payload[1..];
                }
                let priority = flags.contains(Http2FrameFlag::Priority).then(|| {
                    Http2HeadersFramePriorityOutput {
                        e: payload[0] & 1 << 7 != 0,
                        stream_dependency: NetworkEndian::read_u32(payload) & !(1 << 31),
                        weight: payload[4],
                    }
                });
                if priority.is_some() {
                    payload = &payload[5..];
                }
                Self::Headers(Http2HeadersFrameOutput {
                    flags: flags.into(),
                    end_stream: flags.contains(Http2FrameFlag::EndStream),
                    end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                    r,
                    stream_id,
                    priority,
                    header_block_fragment: payload[..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
                })
            }
            Http2FrameType::Priority if payload.len() == 5 => {
                Self::Priority(Http2PriorityFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    e: payload[0] & 1 << 7 != 0,
                    stream_dependency: NetworkEndian::read_u32(payload) & !(1 << 31),
                    weight: payload[4],
                })
            }
            Http2FrameType::RstStream if payload.len() == 4 => {
                Self::RstStream(Http2RstStreamFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    error_code: NetworkEndian::read_u32(payload),
                })
            }
            Http2FrameType::Settings if payload.len() % 6 == 0 => {
                Self::Settings(Http2SettingsFrameOutput {
                    flags: flags.into(),
                    ack: flags.contains(Http2FrameFlag::Ack),
                    r,
                    stream_id,
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
                    pad_len = usize::from(payload[0]);
                    payload = &payload[1..];
                }
                Self::PushPromise(Http2PushPromiseFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    promised_r: payload[0] & 1 << 7 != 0,
                    promised_stream_id: NetworkEndian::read_u32(payload) & !(1 << 31),
                    header_block_fragment: payload[4..payload.len() - pad_len].to_vec(),
                    padding: padded.then(|| payload[payload.len() - pad_len..].to_vec()),
                })
            }
            Http2FrameType::Ping => Self::Ping(Http2PingFrameOutput {
                flags: flags.into(),
                ack: flags.contains(Http2FrameFlag::Ack),
                r,
                stream_id,
                data: payload.to_vec(),
            }),
            Http2FrameType::Goaway if payload.len() >= 8 => Self::Goaway(Http2GoawayFrameOutput {
                flags: flags.into(),
                r,
                stream_id,
                last_r: payload[0] & 1 << 7 != 0,
                last_stream_id: NetworkEndian::read_u32(payload) & !(1 << 31),
                error_code: NetworkEndian::read_u32(&payload[4..]),
                debug_data: payload[8..].to_vec(),
            }),
            Http2FrameType::WindowUpdate if payload.len() == 4 => {
                Self::WindowUpdate(Http2WindowUpdateFrameOutput {
                    flags: flags.into(),
                    r,
                    stream_id,
                    window_r: payload[0] & 1 << 7 != 0,
                    window_size_increment: NetworkEndian::read_u32(payload) & !(1 << 31),
                })
            }
            Http2FrameType::Continuation => Self::Continuation(Http2ContinuationFrameOutput {
                flags: flags.into(),
                end_headers: flags.contains(Http2FrameFlag::EndHeaders),
                r,
                stream_id,
                header_block_fragment: payload.to_vec(),
            }),
            _ => Self::Generic(Http2GenericFrameOutput {
                r#type: kind,
                flags: flags.into(),
                r,
                stream_id,
                payload: payload.to_vec(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
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
    pub flags: Http2FrameFlag,
    pub end_stream: bool,
    pub r: bool,
    pub stream_id: u32,
    pub data: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2DataFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Data
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        let mut flags = Http2FrameFlag::none();
        if self.end_stream {
            flags |= Http2FrameFlag::EndStream;
        }
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndStream.cond(self.end_stream)
                | Http2FrameFlag::Padded.cond(self.padding.is_some()),
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + self.data.len(),
        )
        .await?;
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
    pub flags: Http2FrameFlag,
    pub end_stream: bool,
    pub end_headers: bool,
    pub r: bool,
    pub stream_id: u32,
    pub priority: Option<Http2HeadersFramePriorityOutput>,
    pub header_block_fragment: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2HeadersFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Headers
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndStream.cond(self.end_stream)
                | Http2FrameFlag::EndHeaders.cond(self.end_headers)
                | Http2FrameFlag::Padded.cond(self.padding.is_some())
                | Http2FrameFlag::Priority.cond(self.priority.is_some()),
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + self.priority.as_ref().map(|_| 5).unwrap_or_default()
                + self.header_block_fragment.len(),
        )
        .await?;
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
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub e: bool,
    pub stream_dependency: u32,
    pub weight: u8,
}

impl Http2PriorityFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Priority
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            5,
        )
        .await?;
        let mut buf = [0; 5];
        NetworkEndian::write_u32(&mut buf, to_u31(self.e, self.stream_dependency));
        buf[4] = self.weight;
        writer.write_all(&buf).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2RstStreamFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub error_code: u32,
}

impl Http2RstStreamFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::RstStream
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            4,
        )
        .await?;
        writer.write_u32(self.error_code).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2SettingsFrameOutput {
    pub flags: Http2FrameFlag,
    pub ack: bool,
    pub r: bool,
    pub stream_id: u32,
    pub parameters: Vec<Http2SettingsParameterOutput>,
}

impl Http2SettingsFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Settings
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            6 * self.parameters.len(),
        )
        .await?;
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
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub promised_r: bool,
    pub promised_stream_id: u32,
    pub header_block_fragment: Vec<u8>,
    pub padding: Option<Vec<u8>>,
}

impl Http2PushPromiseFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::PushPromise
    }
    #[inline]
    pub fn end_headers(&self) -> bool {
        self.flags.contains(Http2FrameFlag::EndHeaders)
    }
    #[inline]
    fn padded(&self) -> bool {
        self.flags.contains(Http2FrameFlag::Padded)
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            self.flags,
            to_u31(self.r, self.stream_id),
            self.padding
                .as_ref()
                .map(|p| p.len() + 1)
                .unwrap_or_default()
                + 4
                + self.header_block_fragment.len(),
        )
        .await?;
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
    pub flags: Http2FrameFlag,
    pub ack: bool,
    pub r: bool,
    pub stream_id: u32,
    pub data: Vec<u8>,
}

impl Http2PingFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Ping
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::Ack.cond(self.ack),
            to_u31(self.r, self.stream_id),
            self.data.len(),
        )
        .await?;
        writer.write_all(&self.data).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2GoawayFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub last_r: bool,
    pub last_stream_id: u32,
    pub error_code: u32,
    pub debug_data: Vec<u8>,
}

impl Http2GoawayFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Goaway
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            8,
        )
        .await?;
        let mut buf = [0; 8];
        NetworkEndian::write_u32(&mut buf, to_u31(self.last_r, self.last_stream_id));
        NetworkEndian::write_u32(&mut buf, self.error_code);
        writer.write_all(&buf).await?;
        writer.write_all(&self.debug_data).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2WindowUpdateFrameOutput {
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub window_r: bool,
    pub window_size_increment: u32,
}

impl Http2WindowUpdateFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::WindowUpdate
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::none(),
            to_u31(self.r, self.stream_id),
            4,
        )
        .await?;
        writer
            .write_u32(to_u31(self.window_r, self.window_size_increment))
            .await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2ContinuationFrameOutput {
    pub flags: Http2FrameFlag,
    pub end_headers: bool,
    pub r: bool,
    pub stream_id: u32,
    pub header_block_fragment: Vec<u8>,
}

impl Http2ContinuationFrameOutput {
    pub const fn r#type() -> Http2FrameType {
        Http2FrameType::Continuation
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        // Write the header.
        write_frame_header(
            &mut writer,
            Self::r#type(),
            Http2FrameFlag::EndHeaders.cond(self.end_headers),
            to_u31(self.r, self.stream_id),
            self.header_block_fragment.len(),
        )
        .await?;
        writer.write_all(&self.header_block_fragment).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Http2GenericFrameOutput {
    pub r#type: Http2FrameType,
    pub flags: Http2FrameFlag,
    pub r: bool,
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

impl Http2GenericFrameOutput {
    pub async fn write<W: AsyncWrite + Unpin>(&self, mut writer: W) -> io::Result<()> {
        write_frame_header(
            &mut writer,
            self.r#type,
            self.flags,
            to_u31(self.r, self.stream_id),
            self.payload.len(),
        )
        .await?;
        writer.write_all(&self.payload).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RawHttp2Output {
    pub plan: RawHttp2PlanOutput,
    pub sent: Vec<Http2FrameOutput>,
    pub received: Vec<Http2FrameOutput>,
    pub errors: Vec<RawHttp2Error>,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawHttp2PlanOutput {
    pub host: String,
    pub port: u16,
    pub preamble: Option<Vec<u8>>,
    pub frames: Vec<Http2FrameOutput>,
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
