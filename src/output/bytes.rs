use core::str;
use std::fmt::{Display, Write};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use serde::ser::SerializeStruct;
use serde::Serialize;

use crate::record::BigQuerySchema;

#[derive(Debug, Clone, Eq)]
pub enum BytesOutput {
    Vec(Arc<Vec<u8>>),
    String(Arc<String>),
    StaticStr(&'static str),
    Bytes(Bytes),
}

impl BigQuerySchema for BytesOutput {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        // TODO: use bytes format with newer protobuf API.
        TableFieldSchema::string(name)
    }
}

impl Display for BytesOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const HEX_CHAR_LOOKUP: [char; 16] = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
        ];

        let bytes = self.as_ref();
        if !bytes.is_empty() {
            let byte = bytes[0];
            f.write_char(HEX_CHAR_LOOKUP[(byte >> 4) as usize])?;
            f.write_char(HEX_CHAR_LOOKUP[(byte & 0xF) as usize])?;
            for byte in &bytes[1..] {
                f.write_char(' ')?;
                f.write_char(HEX_CHAR_LOOKUP[(byte >> 4) as usize])?;
                f.write_char(HEX_CHAR_LOOKUP[(byte & 0xF) as usize])?;
            }
        }
        Ok(())
    }
}

impl Hash for BytesOutput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self)
    }
}

impl PartialEq for BytesOutput {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Serialize for BytesOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            Base64Field(self).serialize(serializer)
        } else {
            serializer.serialize_bytes(self)
        }
    }
}

impl Default for BytesOutput {
    fn default() -> Self {
        Self::Bytes(Bytes::new())
    }
}

impl Default for &BytesOutput {
    fn default() -> Self {
        static DEFAULT: BytesOutput = BytesOutput::Bytes(Bytes::new());
        &DEFAULT
    }
}

impl From<Arc<Vec<u8>>> for BytesOutput {
    fn from(value: Arc<Vec<u8>>) -> Self {
        Self::Vec(value)
    }
}

impl From<Vec<u8>> for BytesOutput {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(value))
    }
}

impl From<Arc<String>> for BytesOutput {
    fn from(value: Arc<String>) -> Self {
        Self::String(value)
    }
}

impl From<&'static str> for BytesOutput {
    fn from(value: &'static str) -> Self {
        Self::StaticStr(value)
    }
}

impl From<String> for BytesOutput {
    fn from(value: String) -> Self {
        Self::String(Arc::new(value))
    }
}

impl From<Bytes> for BytesOutput {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl Deref for BytesOutput {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Self::Vec(x) => x,
            Self::String(x) => x.as_bytes(),
            Self::StaticStr(x) => x.as_bytes(),
            Self::Bytes(x) => x.as_ref(),
        }
    }
}

impl AsRef<[u8]> for BytesOutput {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl BytesOutput {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct MaybeUtf8(pub BytesOutput);

impl Default for &MaybeUtf8 {
    fn default() -> Self {
        static DEFAULT: MaybeUtf8 = MaybeUtf8(BytesOutput::Bytes(Bytes::new()));
        &DEFAULT
    }
}

impl BigQuerySchema for MaybeUtf8 {
    fn big_query_schema(name: &str) -> TableFieldSchema {
        TableFieldSchema::record(
            name,
            vec![
                TableFieldSchema::string("utf8"),
                TableFieldSchema::string("base64"),
                TableFieldSchema::bytes("raw"),
            ],
        )
    }
}

impl Serialize for MaybeUtf8 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let human_readable = serializer.is_human_readable();
        let mut ss = serializer.serialize_struct("MaybeUtf8", 1)?;
        if let Some(s) = self.as_str() {
            ss.serialize_field("utf8", &Utf8Field(s))?;
        } else if human_readable {
            ss.serialize_field("base64", &Base64Field(self))?;
        } else {
            ss.serialize_field("raw", &BytesField(self))?;
        }
        ss.end()
    }
}

impl Deref for MaybeUtf8 {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for MaybeUtf8 {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Display for MaybeUtf8 {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(s) = self.as_str() {
            return f.write_str(s);
        } else {
            Display::fmt(&self.0, f)
        }
    }
}

impl MaybeUtf8 {
    pub fn as_str(&self) -> Option<&str> {
        match &self.0 {
            BytesOutput::String(s) => Some(s.as_str()),
            BytesOutput::StaticStr(s) => Some(s),
            BytesOutput::Vec(v) => str::from_utf8(v.as_slice()).ok(),
            BytesOutput::Bytes(b) => str::from_utf8(b.as_ref()).ok(),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

struct Utf8Field<'a>(&'a str);

impl Serialize for Utf8Field<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0)
    }
}

struct Base64Field<'a>(&'a [u8]);

impl Serialize for Base64Field<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&base64::display::Base64Display::new(
            &self.0,
            &base64::prelude::BASE64_STANDARD,
        ))
    }
}

struct BytesField<'a>(&'a [u8]);

impl Serialize for BytesField<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.0)
    }
}
