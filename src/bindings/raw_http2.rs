use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{Merge, Value};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RawHttp2 {
    pub host: Option<Value>,
    pub port: Option<Value>,
    pub preamble: Option<Value>,
    pub frames: Option<Vec<Http2Frame>>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

impl RawHttp2 {
    pub(super) fn merge(self, default: Option<Self>) -> Self {
        let Some(default) = default else {
            return self;
        };
        Self {
            host: Value::merge(self.host, default.host),
            port: Value::merge(self.port, default.port),
            preamble: Value::merge(self.preamble, default.preamble),
            frames: self.frames.or(default.frames),
            unrecognized: toml::Table::new(),
        }
    }

    pub(super) fn validate(&self) -> crate::Result<()> {
        if let Some(frames) = &self.frames {
            for f in frames {
                f.validate()?;
            }
        }
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2Frame {
    pub flags: Option<Value>,
    pub r: Option<Value>,
    pub stream_id: Option<Value>,
    pub payload: Option<Http2FramePayload>,
    #[serde(flatten)]
    pub unrecognized: toml::Table,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Http2FramePayload {
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

impl Http2Frame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        match &self.payload {
            Some(Http2FramePayload::Data(frame)) => frame.validate(),
            Some(Http2FramePayload::Headers(frame)) => frame.validate(),
            Some(Http2FramePayload::Priority(frame)) => frame.validate(),
            Some(Http2FramePayload::RstStream(frame)) => frame.validate(),
            Some(Http2FramePayload::Settings(frame)) => frame.validate(),
            Some(Http2FramePayload::PushPromise(frame)) => frame.validate(),
            Some(Http2FramePayload::Ping(frame)) => frame.validate(),
            Some(Http2FramePayload::Goaway(frame)) => frame.validate(),
            Some(Http2FramePayload::WindowUpdate(frame)) => frame.validate(),
            Some(Http2FramePayload::Continuation(frame)) => frame.validate(),
            Some(Http2FramePayload::Generic(frame)) => frame.validate(),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2DataFrame {
    pub end_stream: Option<Value>,
    pub data: Option<Value>,
    pub padding: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2DataFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2HeadersFrame {
    pub end_stream: Option<Value>,
    pub end_headers: Option<Value>,
    pub priority: Option<Http2HeadersFramePriority>,
    pub header_block_fragment: Option<Value>,
    pub padding: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2HeadersFrame {
    fn validate(&self) -> crate::Result<()> {
        if let Some(x) = &self.priority {
            x.validate()?;
        }
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2HeadersFramePriority {
    pub e: Option<Value>,
    pub stream_dependency: Option<Value>,
    pub weight: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2HeadersFramePriority {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2PriorityFrame {
    pub e: Option<Value>,
    pub stream_dependency: Option<Value>,
    pub weight: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2PriorityFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2RstStreamFrame {
    pub error_code: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2RstStreamFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2SettingsFrame {
    pub ack: Option<Value>,
    pub parameters: Option<Vec<Http2SettingsParameter>>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2SettingsFrame {
    fn validate(&self) -> crate::Result<()> {
        for param in self.parameters.as_ref().into_iter().flatten() {
            param.validate()?;
        }
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2SettingsParameter {
    pub id: Option<Value>,
    pub value: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2SettingsParameter {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2PushPromiseFrame {
    pub end_headers: Option<Value>,
    pub promised_r: Option<Value>,
    pub promised_stream_id: Option<Value>,
    pub header_block_fragment: Option<Value>,
    pub padding: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2PushPromiseFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2PingFrame {
    pub ack: Option<Value>,
    pub data: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2PingFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2GoawayFrame {
    pub last_r: Option<Value>,
    pub last_stream_id: Option<Value>,
    pub error_code: Option<Value>,
    pub debug_data: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2GoawayFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2WindowUpdateFrame {
    pub window_r: Option<Value>,
    pub window_size_increment: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2WindowUpdateFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2ContinuationFrame {
    pub end_headers: Option<Value>,
    pub header_block_fragment: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2ContinuationFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Http2GenericFrame {
    pub r#type: Option<Value>,
    pub payload: Option<Value>,

    #[serde(flatten)]
    unrecognized: toml::Table,
}

impl Http2GenericFrame {
    fn validate(&self) -> crate::Result<()> {
        if !self.unrecognized.is_empty() {
            bail!(
                "unrecognized field{} {}",
                if self.unrecognized.len() == 1 {
                    ""
                } else {
                    "s"
                },
                self.unrecognized.keys().join(", "),
            );
        }
        Ok(())
    }
}
