//! CBOR Frame Types for Plugin Communication
//!
//! This module defines the binary CBOR frame format that replaces JSON messages.
//! Frames use integer keys for compact encoding and support native binary payloads.
//!
//! ## Frame Format
//!
//! Each frame is a CBOR map with integer keys:
//! ```text
//! {
//!   0: version (u8, always 1)
//!   1: frame_type (u8)
//!   2: id (bytes[16] or uint)
//!   3: seq (u64)
//!   4: content_type (tstr, optional)
//!   5: meta (map, optional)
//!   6: payload (bstr, optional)
//!   7: len (u64, optional - total payload length for chunked)
//!   8: offset (u64, optional - byte offset in chunked stream)
//!   9: eof (bool, optional - true on final chunk)
//!   10: cap (tstr, optional - cap URN for requests)
//! }
//! ```
//!
//! ## Frame Types
//!
//! - HELLO (0): Handshake to negotiate limits
//! - REQ (1): Request to invoke a cap
//! - RES (2): Single complete response
//! - CHUNK (3): Streaming data chunk
//! - END (4): Stream complete marker
//! - LOG (5): Log/progress message
//! - ERR (6): Error message

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Protocol version. Always 1 for this implementation.
pub const PROTOCOL_VERSION: u8 = 1;

/// Default maximum frame size (1 MB)
pub const DEFAULT_MAX_FRAME: usize = 1_048_576;

/// Default maximum chunk size (256 KB)
pub const DEFAULT_MAX_CHUNK: usize = 262_144;

/// Frame type discriminator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FrameType {
    /// Handshake frame for negotiating limits
    Hello = 0,
    /// Request to invoke a cap
    Req = 1,
    /// Single complete response
    Res = 2,
    /// Streaming data chunk
    Chunk = 3,
    /// Stream complete marker
    End = 4,
    /// Log/progress message
    Log = 5,
    /// Error message
    Err = 6,
    /// Health monitoring ping/pong - either side can send, receiver must respond with same ID
    Heartbeat = 7,
}

impl FrameType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(FrameType::Hello),
            1 => Some(FrameType::Req),
            2 => Some(FrameType::Res),
            3 => Some(FrameType::Chunk),
            4 => Some(FrameType::End),
            5 => Some(FrameType::Log),
            6 => Some(FrameType::Err),
            7 => Some(FrameType::Heartbeat),
            _ => None,
        }
    }
}

/// Message ID - either a 16-byte UUID or a simple integer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageId {
    /// 16-byte UUID
    Uuid([u8; 16]),
    /// Simple integer ID
    Uint(u64),
}

impl MessageId {
    /// Create a new random UUID message ID
    pub fn new_uuid() -> Self {
        let uuid = uuid::Uuid::new_v4();
        MessageId::Uuid(*uuid.as_bytes())
    }

    /// Create from a UUID string
    pub fn from_uuid_str(s: &str) -> Option<Self> {
        uuid::Uuid::parse_str(s)
            .ok()
            .map(|u| MessageId::Uuid(*u.as_bytes()))
    }

    /// Convert to UUID string if this is a UUID
    pub fn to_uuid_string(&self) -> Option<String> {
        match self {
            MessageId::Uuid(bytes) => {
                uuid::Uuid::from_bytes(*bytes)
                    .to_string()
                    .into()
            }
            MessageId::Uint(_) => None,
        }
    }

    /// Get as bytes for comparison
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            MessageId::Uuid(bytes) => bytes.to_vec(),
            MessageId::Uint(n) => n.to_be_bytes().to_vec(),
        }
    }
}

impl Default for MessageId {
    fn default() -> Self {
        MessageId::new_uuid()
    }
}

/// Negotiated protocol limits
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum frame size in bytes
    pub max_frame: usize,
    /// Maximum chunk payload size in bytes
    pub max_chunk: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: DEFAULT_MAX_FRAME,
            max_chunk: DEFAULT_MAX_CHUNK,
        }
    }
}

/// A CBOR protocol frame
#[derive(Debug, Clone)]
pub struct Frame {
    /// Protocol version (always 1)
    pub version: u8,
    /// Frame type
    pub frame_type: FrameType,
    /// Message ID for correlation
    pub id: MessageId,
    /// Sequence number within a stream
    pub seq: u64,
    /// Content type of payload (MIME-like)
    pub content_type: Option<String>,
    /// Metadata map
    pub meta: Option<BTreeMap<String, ciborium::Value>>,
    /// Binary payload
    pub payload: Option<Vec<u8>>,
    /// Total length for chunked transfers (first chunk only)
    pub len: Option<u64>,
    /// Byte offset in chunked stream
    pub offset: Option<u64>,
    /// End of stream marker
    pub eof: Option<bool>,
    /// Cap URN (for requests)
    pub cap: Option<String>,
}

impl Frame {
    /// Create a new frame with required fields
    pub fn new(frame_type: FrameType, id: MessageId) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            frame_type,
            id,
            seq: 0,
            content_type: None,
            meta: None,
            payload: None,
            len: None,
            offset: None,
            eof: None,
            cap: None,
        }
    }

    /// Create a HELLO frame for handshake
    pub fn hello(max_frame: usize, max_chunk: usize) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "max_frame".to_string(),
            ciborium::Value::Integer((max_frame as i64).into()),
        );
        meta.insert(
            "max_chunk".to_string(),
            ciborium::Value::Integer((max_chunk as i64).into()),
        );
        meta.insert(
            "version".to_string(),
            ciborium::Value::Integer((PROTOCOL_VERSION as i64).into()),
        );

        let mut frame = Self::new(FrameType::Hello, MessageId::Uint(0));
        frame.meta = Some(meta);
        frame
    }

    /// Create a REQ frame for invoking a cap
    pub fn req(id: MessageId, cap_urn: &str, payload: Vec<u8>, content_type: &str) -> Self {
        let mut frame = Self::new(FrameType::Req, id);
        frame.cap = Some(cap_urn.to_string());
        frame.payload = Some(payload);
        frame.content_type = Some(content_type.to_string());
        frame
    }

    /// Create a RES frame for a single response
    pub fn res(id: MessageId, payload: Vec<u8>, content_type: &str) -> Self {
        let mut frame = Self::new(FrameType::Res, id);
        frame.payload = Some(payload);
        frame.content_type = Some(content_type.to_string());
        frame
    }

    /// Create a CHUNK frame for streaming
    pub fn chunk(id: MessageId, seq: u64, payload: Vec<u8>) -> Self {
        let mut frame = Self::new(FrameType::Chunk, id);
        frame.seq = seq;
        frame.payload = Some(payload);
        frame
    }

    /// Create a CHUNK frame with offset info (for large binary transfers)
    pub fn chunk_with_offset(
        id: MessageId,
        seq: u64,
        payload: Vec<u8>,
        offset: u64,
        total_len: Option<u64>,
        is_last: bool,
    ) -> Self {
        let mut frame = Self::new(FrameType::Chunk, id);
        frame.seq = seq;
        frame.payload = Some(payload);
        frame.offset = Some(offset);
        if seq == 0 {
            frame.len = total_len;
        }
        if is_last {
            frame.eof = Some(true);
        }
        frame
    }

    /// Create an END frame to mark stream completion
    pub fn end(id: MessageId, final_payload: Option<Vec<u8>>) -> Self {
        let mut frame = Self::new(FrameType::End, id);
        frame.payload = final_payload;
        frame.eof = Some(true);
        frame
    }

    /// Create a LOG frame for progress/status
    pub fn log(id: MessageId, level: &str, message: &str) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert("level".to_string(), ciborium::Value::Text(level.to_string()));
        meta.insert("message".to_string(), ciborium::Value::Text(message.to_string()));

        let mut frame = Self::new(FrameType::Log, id);
        frame.meta = Some(meta);
        frame
    }

    /// Create an ERR frame
    pub fn err(id: MessageId, code: &str, message: &str) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert("code".to_string(), ciborium::Value::Text(code.to_string()));
        meta.insert("message".to_string(), ciborium::Value::Text(message.to_string()));

        let mut frame = Self::new(FrameType::Err, id);
        frame.meta = Some(meta);
        frame
    }

    /// Create a HEARTBEAT frame for health monitoring.
    /// Either side can send; receiver must respond with HEARTBEAT using the same ID.
    pub fn heartbeat(id: MessageId) -> Self {
        Self::new(FrameType::Heartbeat, id)
    }

    /// Check if this is the final frame in a stream
    pub fn is_eof(&self) -> bool {
        self.eof.unwrap_or(false)
    }

    /// Get error code if this is an ERR frame
    pub fn error_code(&self) -> Option<&str> {
        if self.frame_type != FrameType::Err {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("code").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get error message if this is an ERR frame
    pub fn error_message(&self) -> Option<&str> {
        if self.frame_type != FrameType::Err {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("message").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get log level if this is a LOG frame
    pub fn log_level(&self) -> Option<&str> {
        if self.frame_type != FrameType::Log {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("level").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Get log message if this is a LOG frame
    pub fn log_message(&self) -> Option<&str> {
        if self.frame_type != FrameType::Log {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("message").and_then(|v| {
                if let ciborium::Value::Text(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
        })
    }

    /// Extract max_frame from HELLO metadata
    pub fn hello_max_frame(&self) -> Option<usize> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("max_frame").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= usize::MAX as i128 {
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Extract max_chunk from HELLO metadata
    pub fn hello_max_chunk(&self) -> Option<usize> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("max_chunk").and_then(|v| {
                if let ciborium::Value::Integer(i) = v {
                    let n: i128 = (*i).into();
                    if n > 0 && n <= usize::MAX as i128 {
                        Some(n as usize)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        })
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new(FrameType::Req, MessageId::default())
    }
}

/// Integer keys for CBOR map fields
pub mod keys {
    pub const VERSION: u64 = 0;
    pub const FRAME_TYPE: u64 = 1;
    pub const ID: u64 = 2;
    pub const SEQ: u64 = 3;
    pub const CONTENT_TYPE: u64 = 4;
    pub const META: u64 = 5;
    pub const PAYLOAD: u64 = 6;
    pub const LEN: u64 = 7;
    pub const OFFSET: u64 = 8;
    pub const EOF: u64 = 9;
    pub const CAP: u64 = 10;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_type_roundtrip() {
        for t in [
            FrameType::Hello,
            FrameType::Req,
            FrameType::Res,
            FrameType::Chunk,
            FrameType::End,
            FrameType::Log,
            FrameType::Err,
            FrameType::Heartbeat,
        ] {
            let v = t as u8;
            let recovered = FrameType::from_u8(v).expect("should recover frame type");
            assert_eq!(t, recovered);
        }
    }

    #[test]
    fn test_invalid_frame_type() {
        assert!(FrameType::from_u8(100).is_none());
    }

    #[test]
    fn test_message_id_uuid() {
        let id = MessageId::new_uuid();
        let s = id.to_uuid_string().expect("should be uuid");
        let recovered = MessageId::from_uuid_str(&s).expect("should parse");
        assert_eq!(id, recovered);
    }

    #[test]
    fn test_hello_frame() {
        let frame = Frame::hello(1_000_000, 100_000);
        assert_eq!(frame.frame_type, FrameType::Hello);
        assert_eq!(frame.hello_max_frame(), Some(1_000_000));
        assert_eq!(frame.hello_max_chunk(), Some(100_000));
    }

    #[test]
    fn test_req_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::req(id.clone(), "cap:op=test", b"payload".to_vec(), "application/json");
        assert_eq!(frame.frame_type, FrameType::Req);
        assert_eq!(frame.id, id);
        assert_eq!(frame.cap, Some("cap:op=test".to_string()));
        assert_eq!(frame.payload, Some(b"payload".to_vec()));
    }

    #[test]
    fn test_err_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::err(id, "NOT_FOUND", "Cap not found");
        assert_eq!(frame.frame_type, FrameType::Err);
        assert_eq!(frame.error_code(), Some("NOT_FOUND"));
        assert_eq!(frame.error_message(), Some("Cap not found"));
    }

    #[test]
    fn test_chunk_with_offset() {
        let id = MessageId::new_uuid();
        let frame = Frame::chunk_with_offset(id, 0, b"data".to_vec(), 0, Some(1000), false);
        assert_eq!(frame.seq, 0);
        assert_eq!(frame.offset, Some(0));
        assert_eq!(frame.len, Some(1000));
        assert!(!frame.is_eof());

        let id2 = MessageId::new_uuid();
        let last = Frame::chunk_with_offset(id2, 5, b"last".to_vec(), 900, None, true);
        assert!(last.is_eof());
        assert!(last.len.is_none()); // len only on first chunk
    }

    #[test]
    fn test_heartbeat_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::heartbeat(id.clone());
        assert_eq!(frame.frame_type, FrameType::Heartbeat);
        assert_eq!(frame.id, id);
        assert!(frame.payload.is_none());
        assert!(frame.meta.is_none());
    }
}
