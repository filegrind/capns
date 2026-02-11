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
//!   0: version (u8, always 2)
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

/// Protocol version. Version 2: Result-based emitters, negotiated chunk limits, per-request errors.
pub const PROTOCOL_VERSION: u8 = 2;

/// Default maximum frame size (3.5 MB) - safe margin below 3.75MB limit
/// Larger payloads automatically use CHUNK frames
pub const DEFAULT_MAX_FRAME: usize = 3_670_016;

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
    // Res = 2 REMOVED - old single-response protocol no longer supported
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
    /// Announce new stream for a request (multiplexed streaming)
    StreamStart = 8,
    /// End a specific stream (multiplexed streaming)
    StreamEnd = 9,
    /// Relay capability advertisement (slave → master). Carries aggregate manifest + limits.
    RelayNotify = 10,
    /// Relay host system resources + cap demands (master → slave). Carries opaque resource payload.
    RelayState = 11,
}

impl FrameType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(FrameType::Hello),
            1 => Some(FrameType::Req),
            // 2 = Res REMOVED - old protocol no longer supported
            3 => Some(FrameType::Chunk),
            4 => Some(FrameType::End),
            5 => Some(FrameType::Log),
            6 => Some(FrameType::Err),
            7 => Some(FrameType::Heartbeat),
            8 => Some(FrameType::StreamStart),
            9 => Some(FrameType::StreamEnd),
            10 => Some(FrameType::RelayNotify),
            11 => Some(FrameType::RelayState),
            _ => None,
        }
    }
}

/// Message ID - either a 16-byte UUID or a simple integer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// Protocol version (always 2)
    pub version: u8,
    /// Frame type
    pub frame_type: FrameType,
    /// Message ID for correlation (request ID)
    pub id: MessageId,
    /// Stream ID for multiplexed streams (used in STREAM_START, CHUNK, STREAM_END)
    pub stream_id: Option<String>,
    /// Media URN for stream type identification (used in STREAM_START)
    pub media_urn: Option<String>,
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
            stream_id: None,
            media_urn: None,
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

    /// Create a HELLO frame for handshake (host side - no manifest)
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

    /// Create a HELLO frame for handshake with manifest (plugin side).
    /// The manifest is JSON-encoded plugin metadata including name, version, and caps.
    /// This is the ONLY way for plugins to communicate their capabilities.
    pub fn hello_with_manifest(max_frame: usize, max_chunk: usize, manifest: &[u8]) -> Self {
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
        meta.insert(
            "manifest".to_string(),
            ciborium::Value::Bytes(manifest.to_vec()),
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

    // Frame::res() REMOVED - old single-response protocol no longer supported
    // Use stream multiplexing: STREAM_START + CHUNK + STREAM_END + END

    /// Create a CHUNK frame for multiplexed streaming.
    /// Each chunk belongs to a specific stream within a request.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this chunk belongs to
    /// * `stream_id` - The stream ID this chunk belongs to
    /// * `seq` - Sequence number within the stream
    /// * `payload` - Chunk data
    pub fn chunk(req_id: MessageId, stream_id: String, seq: u64, payload: Vec<u8>) -> Self {
        let mut frame = Self::new(FrameType::Chunk, req_id);
        frame.stream_id = Some(stream_id);
        frame.seq = seq;
        frame.payload = Some(payload);
        frame
    }

    /// Create a CHUNK frame with offset info (for large binary transfers).
    /// Used for multiplexed streaming with offset tracking.
    pub fn chunk_with_offset(
        req_id: MessageId,
        stream_id: String,
        seq: u64,
        payload: Vec<u8>,
        offset: u64,
        total_len: Option<u64>,
        is_last: bool,
    ) -> Self {
        let mut frame = Self::new(FrameType::Chunk, req_id);
        frame.stream_id = Some(stream_id);
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

    /// Create a STREAM_START frame to announce a new stream within a request.
    /// Used for multiplexed streaming - multiple streams can exist per request.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this stream belongs to
    /// * `stream_id` - Unique ID for this stream (UUID generated by sender)
    /// * `media_urn` - Media URN identifying the stream's data type
    pub fn stream_start(req_id: MessageId, stream_id: String, media_urn: String) -> Self {
        let mut frame = Self::new(FrameType::StreamStart, req_id);
        frame.stream_id = Some(stream_id);
        frame.media_urn = Some(media_urn);
        frame
    }

    /// Create a STREAM_END frame to mark completion of a specific stream.
    /// After this, any CHUNK for this stream_id is a fatal protocol error.
    ///
    /// # Arguments
    /// * `req_id` - The request ID this stream belongs to
    /// * `stream_id` - The stream being ended
    pub fn stream_end(req_id: MessageId, stream_id: String) -> Self {
        let mut frame = Self::new(FrameType::StreamEnd, req_id);
        frame.stream_id = Some(stream_id);
        frame
    }

    /// Create a RelayNotify frame for capability advertisement (slave → master).
    /// Carries the aggregate manifest of all plugin capabilities and negotiated limits.
    ///
    /// # Arguments
    /// * `manifest` - Aggregate manifest bytes (JSON-encoded list of all plugin caps)
    /// * `limits` - Protocol limits for the relay connection
    pub fn relay_notify(manifest: &[u8], limits: &Limits) -> Self {
        let mut meta = BTreeMap::new();
        meta.insert(
            "manifest".to_string(),
            ciborium::Value::Bytes(manifest.to_vec()),
        );
        meta.insert(
            "max_frame".to_string(),
            ciborium::Value::Integer((limits.max_frame as i64).into()),
        );
        meta.insert(
            "max_chunk".to_string(),
            ciborium::Value::Integer((limits.max_chunk as i64).into()),
        );

        let mut frame = Self::new(FrameType::RelayNotify, MessageId::Uint(0));
        frame.meta = Some(meta);
        frame
    }

    /// Create a RelayState frame for host system resources + cap demands (master → slave).
    /// Carries an opaque resource payload whose format is defined by the host.
    ///
    /// # Arguments
    /// * `resources` - Opaque resource payload (CBOR or JSON encoded by the host)
    pub fn relay_state(resources: &[u8]) -> Self {
        let mut frame = Self::new(FrameType::RelayState, MessageId::Uint(0));
        frame.payload = Some(resources.to_vec());
        frame
    }

    /// Extract manifest from RelayNotify metadata.
    /// Returns None if not a RelayNotify frame or no manifest present.
    pub fn relay_notify_manifest(&self) -> Option<&[u8]> {
        if self.frame_type != FrameType::RelayNotify {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("manifest").and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
                } else {
                    None
                }
            })
        })
    }

    /// Extract limits from RelayNotify metadata.
    /// Returns None if not a RelayNotify frame or limits are missing.
    pub fn relay_notify_limits(&self) -> Option<Limits> {
        if self.frame_type != FrameType::RelayNotify {
            return None;
        }
        let meta = self.meta.as_ref()?;
        let max_frame = meta.get("max_frame").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= usize::MAX as i128 { Some(n as usize) } else { None }
            } else {
                None
            }
        })?;
        let max_chunk = meta.get("max_chunk").and_then(|v| {
            if let ciborium::Value::Integer(i) = v {
                let n: i128 = (*i).into();
                if n > 0 && n <= usize::MAX as i128 { Some(n as usize) } else { None }
            } else {
                None
            }
        })?;
        Some(Limits { max_frame, max_chunk })
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

    /// Extract manifest from HELLO metadata (plugin side sends this).
    /// Returns None if no manifest present (host HELLO) or not a HELLO frame.
    /// The manifest is JSON-encoded plugin metadata.
    pub fn hello_manifest(&self) -> Option<&[u8]> {
        if self.frame_type != FrameType::Hello {
            return None;
        }
        self.meta.as_ref().and_then(|m| {
            m.get("manifest").and_then(|v| {
                if let ciborium::Value::Bytes(bytes) = v {
                    Some(bytes.as_slice())
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
    pub const STREAM_ID: u64 = 11;     // Stream ID for multiplexed streams
    pub const MEDIA_URN: u64 = 12;      // Media URN for stream type identification
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity
    #[test]
    fn test_frame_type_roundtrip() {
        for t in [
            FrameType::Hello,
            FrameType::Req,
            // Res REMOVED - old protocol
            FrameType::Chunk,
            FrameType::End,
            FrameType::Log,
            FrameType::Err,
            FrameType::Heartbeat,
            FrameType::StreamStart,
            FrameType::StreamEnd,
            FrameType::RelayNotify,
            FrameType::RelayState,
        ] {
            let v = t as u8;
            let recovered = FrameType::from_u8(v).expect("should recover frame type");
            assert_eq!(t, recovered);
        }
    }

    // TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range
    #[test]
    fn test_invalid_frame_type() {
        assert!(FrameType::from_u8(12).is_none(), "value 12 is one past RelayState");
        assert!(FrameType::from_u8(100).is_none());
        assert!(FrameType::from_u8(255).is_none());
    }

    // TEST173: Test FrameType discriminant values match the wire protocol specification exactly
    #[test]
    fn test_frame_type_discriminant_values() {
        assert_eq!(FrameType::Hello as u8, 0);
        assert_eq!(FrameType::Req as u8, 1);
        // 2 = Res REMOVED - old protocol
        assert_eq!(FrameType::Chunk as u8, 3);
        assert_eq!(FrameType::End as u8, 4);
        assert_eq!(FrameType::Log as u8, 5);
        assert_eq!(FrameType::Err as u8, 6);
        assert_eq!(FrameType::Heartbeat as u8, 7);
        assert_eq!(FrameType::StreamStart as u8, 8);
        assert_eq!(FrameType::StreamEnd as u8, 9);
        assert_eq!(FrameType::RelayNotify as u8, 10);
        assert_eq!(FrameType::RelayState as u8, 11);
    }

    // TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion
    #[test]
    fn test_message_id_uuid() {
        let id = MessageId::new_uuid();
        let s = id.to_uuid_string().expect("should be uuid");
        let recovered = MessageId::from_uuid_str(&s).expect("should parse");
        assert_eq!(id, recovered);
    }

    // TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions)
    #[test]
    fn test_message_id_uuid_uniqueness() {
        let id1 = MessageId::new_uuid();
        let id2 = MessageId::new_uuid();
        assert_ne!(id1, id2, "two UUIDs must be distinct");
    }

    // TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None
    #[test]
    fn test_message_id_uint_has_no_uuid_string() {
        let id = MessageId::Uint(42);
        assert!(id.to_uuid_string().is_none(), "Uint IDs have no UUID representation");
    }

    // TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings
    #[test]
    fn test_message_id_from_invalid_uuid_str() {
        assert!(MessageId::from_uuid_str("not-a-uuid").is_none());
        assert!(MessageId::from_uuid_str("").is_none());
        assert!(MessageId::from_uuid_str("12345678").is_none());
    }

    // TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants
    #[test]
    fn test_message_id_as_bytes() {
        let uuid_id = MessageId::new_uuid();
        let uuid_bytes = uuid_id.as_bytes();
        assert_eq!(uuid_bytes.len(), 16, "UUID must be 16 bytes");

        let uint_id = MessageId::Uint(0x0102030405060708);
        let uint_bytes = uint_id.as_bytes();
        assert_eq!(uint_bytes.len(), 8, "Uint ID must be 8 bytes big-endian");
        assert_eq!(uint_bytes, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    // TEST179: Test MessageId::default creates a UUID variant (not Uint)
    #[test]
    fn test_message_id_default_is_uuid() {
        let id = MessageId::default();
        assert!(id.to_uuid_string().is_some(), "default MessageId must be UUID");
    }

    // TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side
    #[test]
    fn test_hello_frame() {
        let frame = Frame::hello(1_000_000, 100_000);
        assert_eq!(frame.frame_type, FrameType::Hello);
        assert_eq!(frame.version, PROTOCOL_VERSION);
        assert_eq!(frame.hello_max_frame(), Some(1_000_000));
        assert_eq!(frame.hello_max_chunk(), Some(100_000));
        assert!(frame.hello_manifest().is_none(), "Host HELLO must not include manifest");
        assert!(frame.payload.is_none(), "HELLO has no payload");
        // ID should be Uint(0) for HELLO
        assert_eq!(frame.id, MessageId::Uint(0));
    }

    // TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for plugin side
    #[test]
    fn test_hello_frame_with_manifest() {
        let manifest_json = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test","caps":[]}"#;
        let frame = Frame::hello_with_manifest(1_000_000, 100_000, manifest_json.as_bytes());
        assert_eq!(frame.frame_type, FrameType::Hello);
        assert_eq!(frame.hello_max_frame(), Some(1_000_000));
        assert_eq!(frame.hello_max_chunk(), Some(100_000));
        let manifest = frame.hello_manifest().expect("Plugin HELLO must include manifest");
        assert_eq!(manifest, manifest_json.as_bytes());
    }

    // TEST182: Test Frame::req stores cap URN, payload, and content_type correctly
    #[test]
    fn test_req_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::req(id.clone(), "cap:op=test", b"payload".to_vec(), "application/json");
        assert_eq!(frame.frame_type, FrameType::Req);
        assert_eq!(frame.id, id);
        assert_eq!(frame.cap, Some("cap:op=test".to_string()));
        assert_eq!(frame.payload, Some(b"payload".to_vec()));
        assert_eq!(frame.content_type, Some("application/json".to_string()));
        assert_eq!(frame.version, PROTOCOL_VERSION);
    }

    // TEST183 REMOVED: Frame::res() and FrameType::Res removed - old protocol no longer supported
    // NEW PROTOCOL: Use stream multiplexing (STREAM_START + CHUNK + STREAM_END + END)

    // TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id)
    #[test]
    fn test_chunk_frame() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-123".to_string();
        let frame = Frame::chunk(id.clone(), stream_id.clone(), 3, b"data".to_vec());
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.id, id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.seq, 3);
        assert_eq!(frame.payload, Some(b"data".to_vec()));
        assert!(!frame.is_eof(), "plain chunk should not be EOF");
    }

    // TEST185: Test Frame::err stores error code and message in metadata
    #[test]
    fn test_err_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::err(id, "NOT_FOUND", "Cap not found");
        assert_eq!(frame.frame_type, FrameType::Err);
        assert_eq!(frame.error_code(), Some("NOT_FOUND"));
        assert_eq!(frame.error_message(), Some("Cap not found"));
    }

    // TEST186: Test Frame::log stores level and message in metadata
    #[test]
    fn test_log_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::log(id.clone(), "info", "Processing started");
        assert_eq!(frame.frame_type, FrameType::Log);
        assert_eq!(frame.id, id);
        assert_eq!(frame.log_level(), Some("info"));
        assert_eq!(frame.log_message(), Some("Processing started"));
    }

    // TEST187: Test Frame::end with payload sets eof and optional final payload
    #[test]
    fn test_end_frame_with_payload() {
        let id = MessageId::new_uuid();
        let frame = Frame::end(id.clone(), Some(b"final".to_vec()));
        assert_eq!(frame.frame_type, FrameType::End);
        assert!(frame.is_eof());
        assert_eq!(frame.payload, Some(b"final".to_vec()));
    }

    // TEST188: Test Frame::end without payload still sets eof marker
    #[test]
    fn test_end_frame_without_payload() {
        let id = MessageId::new_uuid();
        let frame = Frame::end(id, None);
        assert_eq!(frame.frame_type, FrameType::End);
        assert!(frame.is_eof());
        assert!(frame.payload.is_none());
    }

    // TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id)
    #[test]
    fn test_chunk_with_offset() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-456".to_string();
        let first = Frame::chunk_with_offset(id.clone(), stream_id.clone(), 0, b"data".to_vec(), 0, Some(1000), false);
        assert_eq!(first.seq, 0);
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.len, Some(1000), "first chunk must carry total len");
        assert!(!first.is_eof());

        let mid = Frame::chunk_with_offset(id.clone(), stream_id.clone(), 3, b"mid".to_vec(), 500, Some(9999), false);
        assert!(mid.len.is_none(), "non-first chunk must not carry len, seq != 0");
        assert_eq!(mid.offset, Some(500));

        let last = Frame::chunk_with_offset(id, stream_id, 5, b"last".to_vec(), 900, None, true);
        assert!(last.is_eof());
        assert!(last.len.is_none());
    }

    // TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata
    #[test]
    fn test_heartbeat_frame() {
        let id = MessageId::new_uuid();
        let frame = Frame::heartbeat(id.clone());
        assert_eq!(frame.frame_type, FrameType::Heartbeat);
        assert_eq!(frame.id, id);
        assert!(frame.payload.is_none());
        assert!(frame.meta.is_none());
        assert_eq!(frame.seq, 0);
    }

    // TEST191: Test error_code and error_message return None for non-Err frame types
    #[test]
    fn test_error_accessors_on_non_err_frame() {
        let req = Frame::req(MessageId::new_uuid(), "cap:op=test", vec![], "text/plain");
        assert!(req.error_code().is_none(), "REQ must have no error_code");
        assert!(req.error_message().is_none(), "REQ must have no error_message");

        let hello = Frame::hello(1000, 500);
        assert!(hello.error_code().is_none());
    }

    // TEST192: Test log_level and log_message return None for non-Log frame types
    #[test]
    fn test_log_accessors_on_non_log_frame() {
        let req = Frame::req(MessageId::new_uuid(), "cap:op=test", vec![], "text/plain");
        assert!(req.log_level().is_none(), "REQ must have no log_level");
        assert!(req.log_message().is_none(), "REQ must have no log_message");
    }

    // TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types
    #[test]
    fn test_hello_accessors_on_non_hello_frame() {
        let err = Frame::err(MessageId::new_uuid(), "E", "m");
        assert!(err.hello_max_frame().is_none());
        assert!(err.hello_max_chunk().is_none());
        assert!(err.hello_manifest().is_none());
    }

    // TEST194: Test Frame::new sets version and defaults correctly, optional fields are None
    #[test]
    fn test_frame_new_defaults() {
        let id = MessageId::new_uuid();
        let frame = Frame::new(FrameType::Chunk, id.clone());
        assert_eq!(frame.version, PROTOCOL_VERSION);
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert_eq!(frame.id, id);
        assert_eq!(frame.seq, 0);
        assert!(frame.content_type.is_none());
        assert!(frame.meta.is_none());
        assert!(frame.payload.is_none());
        assert!(frame.len.is_none());
        assert!(frame.offset.is_none());
        assert!(frame.eof.is_none());
        assert!(frame.cap.is_none());
    }

    // TEST195: Test Frame::default creates a Req frame (the documented default)
    #[test]
    fn test_frame_default() {
        let frame = Frame::default();
        assert_eq!(frame.frame_type, FrameType::Req);
        assert_eq!(frame.version, PROTOCOL_VERSION);
    }

    // TEST196: Test is_eof returns false when eof field is None (unset)
    #[test]
    fn test_is_eof_when_none() {
        let frame = Frame::new(FrameType::Chunk, MessageId::Uint(0));
        assert!(!frame.is_eof(), "eof=None must mean not EOF");
    }

    // TEST197: Test is_eof returns false when eof field is explicitly Some(false)
    #[test]
    fn test_is_eof_when_false() {
        let mut frame = Frame::new(FrameType::Chunk, MessageId::Uint(0));
        frame.eof = Some(false);
        assert!(!frame.is_eof());
    }

    // TEST198: Test Limits::default provides the documented default values
    #[test]
    fn test_limits_default() {
        let limits = Limits::default();
        assert_eq!(limits.max_frame, DEFAULT_MAX_FRAME);
        assert_eq!(limits.max_chunk, DEFAULT_MAX_CHUNK);
        assert_eq!(limits.max_frame, 3_670_016, "default max_frame = 3.5 MB");
        assert_eq!(limits.max_chunk, 262_144, "default max_chunk = 256 KB");
    }

    // TEST199: Test PROTOCOL_VERSION is 2
    #[test]
    fn test_protocol_version_constant() {
        assert_eq!(PROTOCOL_VERSION, 2);
    }

    // TEST200: Test integer key constants match the protocol specification
    #[test]
    fn test_key_constants() {
        assert_eq!(keys::VERSION, 0);
        assert_eq!(keys::FRAME_TYPE, 1);
        assert_eq!(keys::ID, 2);
        assert_eq!(keys::SEQ, 3);
        assert_eq!(keys::CONTENT_TYPE, 4);
        assert_eq!(keys::META, 5);
        assert_eq!(keys::PAYLOAD, 6);
        assert_eq!(keys::LEN, 7);
        assert_eq!(keys::OFFSET, 8);
        assert_eq!(keys::EOF, 9);
        assert_eq!(keys::CAP, 10);
    }

    // TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text)
    #[test]
    fn test_hello_manifest_binary_data() {
        let binary_manifest = vec![0x00, 0x01, 0xFF, 0xFE, 0x80];
        let frame = Frame::hello_with_manifest(1000, 500, &binary_manifest);
        assert_eq!(frame.hello_manifest().unwrap(), &binary_manifest);
    }

    // TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not
    #[test]
    fn test_message_id_equality_and_hash() {
        use std::collections::HashSet;

        let id1 = MessageId::Uuid([1; 16]);
        let id2 = MessageId::Uuid([1; 16]);
        let id3 = MessageId::Uuid([2; 16]);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1.clone());
        assert!(set.contains(&id2), "equal IDs must hash the same");
        assert!(!set.contains(&id3));

        let uint1 = MessageId::Uint(42);
        let uint2 = MessageId::Uint(42);
        let uint3 = MessageId::Uint(43);
        assert_eq!(uint1, uint2);
        assert_ne!(uint1, uint3);
    }

    // TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values
    #[test]
    fn test_message_id_cross_variant_inequality() {
        let uuid_id = MessageId::Uuid([0; 16]);
        let uint_id = MessageId::Uint(0);
        assert_ne!(uuid_id, uint_id, "different variants must not be equal");
    }

    // TEST204: Test Frame::req with empty payload stores Some(empty vec) not None
    #[test]
    fn test_req_frame_empty_payload() {
        let frame = Frame::req(MessageId::new_uuid(), "cap:op=test", vec![], "text/plain");
        assert_eq!(frame.payload, Some(vec![]), "empty payload is still Some(vec![])");
    }

    // TEST365: Frame::stream_start stores request_id, stream_id, and media_urn
    #[test]
    fn test_stream_start_frame() {
        let req_id = MessageId::new_uuid();
        let stream_id = "stream-abc-123".to_string();
        let media_urn = "media:bytes".to_string();

        let frame = Frame::stream_start(req_id.clone(), stream_id.clone(), media_urn.clone());

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.id, req_id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert_eq!(frame.media_urn, Some(media_urn));
        assert_eq!(frame.seq, 0);
        assert!(frame.payload.is_none());
    }

    // TEST366: Frame::stream_end stores request_id and stream_id
    #[test]
    fn test_stream_end_frame() {
        let req_id = MessageId::new_uuid();
        let stream_id = "stream-xyz-789".to_string();

        let frame = Frame::stream_end(req_id.clone(), stream_id.clone());

        assert_eq!(frame.frame_type, FrameType::StreamEnd);
        assert_eq!(frame.id, req_id);
        assert_eq!(frame.stream_id, Some(stream_id));
        assert!(frame.media_urn.is_none(), "StreamEnd should not have media_urn");
        assert_eq!(frame.seq, 0);
        assert!(frame.payload.is_none());
    }

    // TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere)
    #[test]
    fn test_stream_start_with_empty_stream_id() {
        let req_id = MessageId::new_uuid();
        let frame = Frame::stream_start(req_id.clone(), String::new(), "media:bytes".to_string());

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.stream_id, Some(String::new()));
        // Protocol validation happens at a higher level, not in constructor
    }

    // TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere)
    #[test]
    fn test_stream_start_with_empty_media_urn() {
        let req_id = MessageId::new_uuid();
        let frame = Frame::stream_start(req_id.clone(), "stream-id".to_string(), String::new());

        assert_eq!(frame.frame_type, FrameType::StreamStart);
        assert_eq!(frame.media_urn, Some(String::new()));
        // Protocol validation happens at a higher level, not in constructor
    }

    // TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10)
    #[test]
    fn test_relay_notify_discriminant_roundtrip() {
        let v = FrameType::RelayNotify as u8;
        assert_eq!(v, 10);
        let recovered = FrameType::from_u8(v).expect("10 must map to RelayNotify");
        assert_eq!(recovered, FrameType::RelayNotify);
    }

    // TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11)
    #[test]
    fn test_relay_state_discriminant_roundtrip() {
        let v = FrameType::RelayState as u8;
        assert_eq!(v, 11);
        let recovered = FrameType::from_u8(v).expect("11 must map to RelayState");
        assert_eq!(recovered, FrameType::RelayState);
    }

    // TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them
    #[test]
    fn test_relay_notify_frame() {
        let manifest = b"{\"caps\":[\"cap:op=test\"]}";
        let limits = Limits { max_frame: 2_000_000, max_chunk: 128_000 };
        let frame = Frame::relay_notify(manifest, &limits);

        assert_eq!(frame.frame_type, FrameType::RelayNotify);
        assert_eq!(frame.id, MessageId::Uint(0));
        assert_eq!(frame.relay_notify_manifest(), Some(manifest.as_slice()));

        let extracted_limits = frame.relay_notify_limits().expect("must have limits");
        assert_eq!(extracted_limits.max_frame, 2_000_000);
        assert_eq!(extracted_limits.max_chunk, 128_000);
    }

    // TEST402: Verify relay_state factory stores resource payload in frame payload field
    #[test]
    fn test_relay_state_frame() {
        let resources = b"{\"memory_mb\":4096,\"cpu_percent\":50}";
        let frame = Frame::relay_state(resources);

        assert_eq!(frame.frame_type, FrameType::RelayState);
        assert_eq!(frame.id, MessageId::Uint(0));
        assert_eq!(frame.payload, Some(resources.to_vec()));
        assert!(frame.meta.is_none(), "RelayState carries data in payload, not meta");
    }

    // TEST403: Verify from_u8 returns None for value 12 (one past RelayState)
    #[test]
    fn test_invalid_frame_type_past_relay_state() {
        assert!(FrameType::from_u8(12).is_none(), "12 is past the last valid frame type");
        assert!(FrameType::from_u8(2).is_none(), "2 (old Res) is still invalid");
    }
}
