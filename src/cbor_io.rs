//! CBOR I/O - Reading and Writing CBOR Frames
//!
//! This module provides streaming CBOR frame encoding/decoding over stdio pipes.
//! Frames are written as length-prefixed CBOR (same framing as before, but CBOR payload).
//!
//! ## Wire Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  4 bytes: u32 big-endian length                         │
//! ├─────────────────────────────────────────────────────────┤
//! │  N bytes: CBOR-encoded Frame                            │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! The CBOR payload is a map with integer keys (see cbor_frame.rs).

use crate::cbor_frame::{keys, Frame, FrameType, Limits, MessageId, DEFAULT_MAX_CHUNK, DEFAULT_MAX_FRAME};
use ciborium::Value;
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum frame size (16 MB) - hard limit to prevent memory exhaustion
const MAX_FRAME_HARD_LIMIT: usize = 16 * 1024 * 1024;

/// Errors that can occur during CBOR I/O
#[derive(Debug, thiserror::Error)]
pub enum CborError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("CBOR encoding error: {0}")]
    Encode(String),

    #[error("CBOR decoding error: {0}")]
    Decode(String),

    #[error("Frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: usize, max: usize },

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Unexpected end of stream")]
    UnexpectedEof,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Handshake failed: {0}")]
    Handshake(String),
}

/// Encode a frame to CBOR bytes
pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, CborError> {
    let mut map: Vec<(Value, Value)> = Vec::with_capacity(11);

    // Required fields
    map.push((
        Value::Integer(keys::VERSION.into()),
        Value::Integer((frame.version as i64).into()),
    ));
    map.push((
        Value::Integer(keys::FRAME_TYPE.into()),
        Value::Integer((frame.frame_type as u8 as i64).into()),
    ));

    // Message ID
    let id_value = match &frame.id {
        MessageId::Uuid(bytes) => Value::Bytes(bytes.to_vec()),
        MessageId::Uint(n) => Value::Integer((*n as i64).into()),
    };
    map.push((Value::Integer(keys::ID.into()), id_value));

    // Sequence number
    map.push((
        Value::Integer(keys::SEQ.into()),
        Value::Integer((frame.seq as i64).into()),
    ));

    // Optional fields
    if let Some(ref ct) = frame.content_type {
        map.push((
            Value::Integer(keys::CONTENT_TYPE.into()),
            Value::Text(ct.clone()),
        ));
    }

    if let Some(ref meta) = frame.meta {
        let meta_map: Vec<(Value, Value)> = meta
            .iter()
            .map(|(k, v)| (Value::Text(k.clone()), v.clone()))
            .collect();
        map.push((Value::Integer(keys::META.into()), Value::Map(meta_map)));
    }

    if let Some(ref payload) = frame.payload {
        map.push((
            Value::Integer(keys::PAYLOAD.into()),
            Value::Bytes(payload.clone()),
        ));
    }

    if let Some(len) = frame.len {
        map.push((
            Value::Integer(keys::LEN.into()),
            Value::Integer((len as i64).into()),
        ));
    }

    if let Some(offset) = frame.offset {
        map.push((
            Value::Integer(keys::OFFSET.into()),
            Value::Integer((offset as i64).into()),
        ));
    }

    if let Some(eof) = frame.eof {
        map.push((Value::Integer(keys::EOF.into()), Value::Bool(eof)));
    }

    if let Some(ref cap) = frame.cap {
        map.push((Value::Integer(keys::CAP.into()), Value::Text(cap.clone())));
    }

    let value = Value::Map(map);
    let mut buf = Vec::new();
    ciborium::into_writer(&value, &mut buf)
        .map_err(|e| CborError::Encode(e.to_string()))?;

    Ok(buf)
}

/// Decode a frame from CBOR bytes
pub fn decode_frame(bytes: &[u8]) -> Result<Frame, CborError> {
    let value: Value = ciborium::from_reader(bytes)
        .map_err(|e| CborError::Decode(e.to_string()))?;

    let map = match value {
        Value::Map(m) => m,
        _ => return Err(CborError::InvalidFrame("expected map".to_string())),
    };

    // Convert to lookup map
    let mut lookup: BTreeMap<u64, Value> = BTreeMap::new();
    for (k, v) in map {
        if let Value::Integer(i) = k {
            let key: i128 = i.into();
            if key >= 0 {
                lookup.insert(key as u64, v);
            }
        }
    }

    // Extract required fields
    let version = lookup
        .get(&keys::VERSION)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u8)
            }
            _ => None,
        })
        .ok_or_else(|| CborError::InvalidFrame("missing version".to_string()))?;

    let frame_type_u8 = lookup
        .get(&keys::FRAME_TYPE)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u8)
            }
            _ => None,
        })
        .ok_or_else(|| CborError::InvalidFrame("missing frame_type".to_string()))?;

    let frame_type = FrameType::from_u8(frame_type_u8)
        .ok_or_else(|| CborError::InvalidFrame(format!("invalid frame_type: {}", frame_type_u8)))?;

    let id = lookup
        .get(&keys::ID)
        .map(|v| match v {
            Value::Bytes(bytes) => {
                if bytes.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    MessageId::Uuid(arr)
                } else {
                    // Treat as bytes, but not a valid UUID - fallback to uint interpretation
                    MessageId::Uint(0)
                }
            }
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                MessageId::Uint(n as u64)
            }
            _ => MessageId::Uint(0),
        })
        .ok_or_else(|| CborError::InvalidFrame("missing id".to_string()))?;

    let seq = lookup
        .get(&keys::SEQ)
        .and_then(|v| match v {
            Value::Integer(i) => {
                let n: i128 = (*i).into();
                Some(n as u64)
            }
            _ => None,
        })
        .unwrap_or(0);

    // Optional fields
    let content_type = lookup.get(&keys::CONTENT_TYPE).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let meta = lookup.get(&keys::META).and_then(|v| match v {
        Value::Map(m) => {
            let mut result = BTreeMap::new();
            for (k, v) in m {
                if let Value::Text(key) = k {
                    result.insert(key.clone(), v.clone());
                }
            }
            Some(result)
        }
        _ => None,
    });

    let payload = lookup.get(&keys::PAYLOAD).and_then(|v| match v {
        Value::Bytes(b) => Some(b.clone()),
        _ => None,
    });

    let len = lookup.get(&keys::LEN).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let offset = lookup.get(&keys::OFFSET).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let eof = lookup.get(&keys::EOF).and_then(|v| match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    });

    let cap = lookup.get(&keys::CAP).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    Ok(Frame {
        version,
        frame_type,
        id,
        seq,
        content_type,
        meta,
        payload,
        len,
        offset,
        eof,
        cap,
    })
}

/// Write a length-prefixed CBOR frame to a writer
pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame, limits: &Limits) -> Result<(), CborError> {
    let bytes = encode_frame(frame)?;

    if bytes.len() > limits.max_frame {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: limits.max_frame,
        });
    }

    if bytes.len() > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_HARD_LIMIT,
        });
    }

    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;

    Ok(())
}

/// Write a length-prefixed CBOR frame to an async writer
pub async fn write_frame_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    limits: &Limits,
) -> Result<(), CborError> {
    let bytes = encode_frame(frame)?;

    if bytes.len() > limits.max_frame {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: limits.max_frame,
        });
    }

    if bytes.len() > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_HARD_LIMIT,
        });
    }

    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;

    Ok(())
}

/// Read a length-prefixed CBOR frame from an async reader
///
/// Returns Ok(None) on clean EOF, Err(UnexpectedEof) on partial read.
pub async fn read_frame_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: &Limits,
) -> Result<Option<Frame>, CborError> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(CborError::Io(e)),
    }

    let length = u32::from_be_bytes(len_buf) as usize;

    // Validate length
    if length > limits.max_frame || length > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: length,
            max: limits.max_frame.min(MAX_FRAME_HARD_LIMIT),
        });
    }

    // Read payload
    let mut payload = vec![0u8; length];
    if let Err(e) = reader.read_exact(&mut payload).await {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Err(CborError::UnexpectedEof);
        } else {
            return Err(CborError::Io(e));
        }
    }

    let frame = decode_frame(&payload)?;
    Ok(Some(frame))
}

/// Read a length-prefixed CBOR frame from a reader
///
/// Returns Ok(None) on clean EOF, Err(UnexpectedEof) on partial read.
pub fn read_frame<R: Read>(reader: &mut R, limits: &Limits) -> Result<Option<Frame>, CborError> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(CborError::Io(e)),
    }

    let length = u32::from_be_bytes(len_buf) as usize;

    // Validate length
    if length > limits.max_frame || length > MAX_FRAME_HARD_LIMIT {
        return Err(CborError::FrameTooLarge {
            size: length,
            max: limits.max_frame.min(MAX_FRAME_HARD_LIMIT),
        });
    }

    // Read payload
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            CborError::UnexpectedEof
        } else {
            CborError::Io(e)
        }
    })?;

    let frame = decode_frame(&payload)?;
    Ok(Some(frame))
}

/// CBOR frame reader with buffering
pub struct FrameReader<R: Read> {
    reader: R,
    limits: Limits,
}

impl<R: Read> FrameReader<R> {
    /// Create a new frame reader with default limits
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            limits: Limits::default(),
        }
    }

    /// Create a new frame reader with specified limits
    pub fn with_limits(reader: R, limits: Limits) -> Self {
        Self { reader, limits }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Read the next frame
    pub fn read(&mut self) -> Result<Option<Frame>, CborError> {
        read_frame(&mut self.reader, &self.limits)
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Get mutable access to the underlying reader
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

/// CBOR frame writer with buffering
pub struct FrameWriter<W: Write> {
    writer: W,
    limits: Limits,
}

impl<W: Write> FrameWriter<W> {
    /// Create a new frame writer with default limits
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            limits: Limits::default(),
        }
    }

    /// Create a new frame writer with specified limits
    pub fn with_limits(writer: W, limits: Limits) -> Self {
        Self { writer, limits }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Write a frame
    pub fn write(&mut self, frame: &Frame) -> Result<(), CborError> {
        write_frame(&mut self.writer, frame, &self.limits)
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Get mutable access to the underlying writer
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Write a large payload as multiple chunks
    ///
    /// This splits the payload into chunks respecting max_chunk and writes
    /// them as CHUNK frames with proper offset/len/eof markers.
    pub fn write_chunked(
        &mut self,
        id: MessageId,
        content_type: &str,
        data: &[u8],
    ) -> Result<(), CborError> {
        let total_len = data.len();
        let max_chunk = self.limits.max_chunk;

        if total_len == 0 {
            // Empty payload - send single chunk with eof
            let mut frame = Frame::chunk(id, 0, Vec::new());
            frame.content_type = Some(content_type.to_string());
            frame.len = Some(0);
            frame.offset = Some(0);
            frame.eof = Some(true);
            return self.write(&frame);
        }

        let mut seq = 0u64;
        let mut offset = 0usize;

        while offset < total_len {
            let chunk_size = max_chunk.min(total_len - offset);
            let is_last = offset + chunk_size >= total_len;

            let chunk_data = data[offset..offset + chunk_size].to_vec();

            let mut frame = Frame::chunk(id.clone(), seq, chunk_data);
            frame.offset = Some(offset as u64);

            // Set content_type and total len on first chunk
            if seq == 0 {
                frame.content_type = Some(content_type.to_string());
                frame.len = Some(total_len as u64);
            }

            if is_last {
                frame.eof = Some(true);
            }

            self.write(&frame)?;

            seq += 1;
            offset += chunk_size;
        }

        Ok(())
    }
}

/// Handshake result including manifest (host side - receives plugin's HELLO with manifest)
#[derive(Debug, Clone)]
pub struct HandshakeResult {
    /// Negotiated protocol limits
    pub limits: Limits,
    /// Plugin manifest JSON data (from plugin's HELLO response).
    /// This is REQUIRED - plugins MUST include their manifest in HELLO.
    pub manifest: Vec<u8>,
}

/// Perform HELLO handshake and extract plugin manifest (host side - sends first).
/// Returns HandshakeResult containing negotiated limits and plugin manifest.
/// Fails if plugin HELLO is missing the required manifest.
pub fn handshake<R: Read, W: Write>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
) -> Result<HandshakeResult, CborError> {
    // Send our HELLO
    let our_hello = Frame::hello(DEFAULT_MAX_FRAME, DEFAULT_MAX_CHUNK);
    writer.write(&our_hello)?;

    // Read their HELLO (should include manifest)
    let their_frame = reader.read()?.ok_or_else(|| {
        CborError::Handshake("connection closed before receiving HELLO".to_string())
    })?;

    if their_frame.frame_type != FrameType::Hello {
        return Err(CborError::Handshake(format!(
            "expected HELLO, got {:?}",
            their_frame.frame_type
        )));
    }

    // Extract manifest - REQUIRED for plugins
    let manifest = their_frame
        .hello_manifest()
        .ok_or_else(|| CborError::Handshake("Plugin HELLO missing required manifest".to_string()))?
        .to_vec();

    // Negotiate minimum of both
    let their_max_frame = their_frame.hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME);
    let their_max_chunk = their_frame.hello_max_chunk().unwrap_or(DEFAULT_MAX_CHUNK);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
    };

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(HandshakeResult { limits, manifest })
}

/// Accept HELLO handshake with manifest (plugin side - receives first, sends manifest in response).
///
/// Reads host's HELLO, sends our HELLO with manifest, returns negotiated limits.
/// The manifest is REQUIRED - plugins MUST provide their manifest.
pub fn handshake_accept<R: Read, W: Write>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    manifest: &[u8],
) -> Result<Limits, CborError> {
    // Read their HELLO first (host initiates)
    let their_frame = reader.read()?.ok_or_else(|| {
        CborError::Handshake("connection closed before receiving HELLO".to_string())
    })?;

    if their_frame.frame_type != FrameType::Hello {
        return Err(CborError::Handshake(format!(
            "expected HELLO, got {:?}",
            their_frame.frame_type
        )));
    }

    // Negotiate minimum of both
    let their_max_frame = their_frame.hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME);
    let their_max_chunk = their_frame.hello_max_chunk().unwrap_or(DEFAULT_MAX_CHUNK);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
    };

    // Send our HELLO with manifest
    let our_hello = Frame::hello_with_manifest(limits.max_frame, limits.max_chunk, manifest);
    writer.write(&our_hello)?;

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(limits)
}

// =============================================================================
// ASYNC I/O TYPES
// =============================================================================

/// Async CBOR frame reader
pub struct AsyncFrameReader<R: AsyncRead + Unpin> {
    reader: R,
    limits: Limits,
}

impl<R: AsyncRead + Unpin> AsyncFrameReader<R> {
    /// Create a new async frame reader with default limits
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            limits: Limits::default(),
        }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Read the next frame
    pub async fn read(&mut self) -> Result<Option<Frame>, CborError> {
        read_frame_async(&mut self.reader, &self.limits).await
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// Async CBOR frame writer
pub struct AsyncFrameWriter<W: AsyncWrite + Unpin> {
    writer: W,
    limits: Limits,
}

impl<W: AsyncWrite + Unpin> AsyncFrameWriter<W> {
    /// Create a new async frame writer with default limits
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            limits: Limits::default(),
        }
    }

    /// Update limits (after handshake)
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Write a frame
    pub async fn write(&mut self, frame: &Frame) -> Result<(), CborError> {
        write_frame_async(&mut self.writer, frame, &self.limits).await
    }

    /// Get the current limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// Perform async HELLO handshake and extract plugin manifest (host side - sends first).
/// Returns HandshakeResult containing negotiated limits and plugin manifest.
/// Fails if plugin HELLO is missing the required manifest.
pub async fn handshake_async<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut AsyncFrameReader<R>,
    writer: &mut AsyncFrameWriter<W>,
) -> Result<HandshakeResult, CborError> {
    // Send our HELLO
    let our_hello = Frame::hello(DEFAULT_MAX_FRAME, DEFAULT_MAX_CHUNK);
    writer.write(&our_hello).await?;

    // Read their HELLO (should include manifest)
    let their_frame = reader.read().await?.ok_or_else(|| {
        CborError::Handshake("connection closed before receiving HELLO".to_string())
    })?;

    if their_frame.frame_type != FrameType::Hello {
        return Err(CborError::Handshake(format!(
            "expected HELLO, got {:?}",
            their_frame.frame_type
        )));
    }

    // Extract manifest - REQUIRED for plugins
    let manifest = their_frame
        .hello_manifest()
        .ok_or_else(|| CborError::Handshake("Plugin HELLO missing required manifest".to_string()))?
        .to_vec();

    // Negotiate minimum of both
    let their_max_frame = their_frame.hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME);
    let their_max_chunk = their_frame.hello_max_chunk().unwrap_or(DEFAULT_MAX_CHUNK);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
    };

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(HandshakeResult { limits, manifest })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_encode_decode_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::req(id.clone(), "cap:op=test", b"payload".to_vec(), "application/json");

        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.version, original.version);
        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.content_type, original.content_type);
    }

    #[test]
    fn test_hello_frame_roundtrip() {
        let original = Frame::hello(500_000, 50_000);
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Hello);
        assert_eq!(decoded.hello_max_frame(), Some(500_000));
        assert_eq!(decoded.hello_max_chunk(), Some(50_000));
    }

    #[test]
    fn test_err_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::err(id, "NOT_FOUND", "Cap not found");
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Err);
        assert_eq!(decoded.error_code(), Some("NOT_FOUND"));
        assert_eq!(decoded.error_message(), Some("Cap not found"));
    }

    #[test]
    fn test_frame_io_roundtrip() {
        let limits = Limits::default();
        let id = MessageId::new_uuid();
        let original = Frame::req(id, "cap:op=test", b"payload".to_vec(), "application/json");

        let mut buf = Vec::new();
        write_frame(&mut buf, &original, &limits).expect("write should succeed");

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor, &limits)
            .expect("read should succeed")
            .expect("should have frame");

        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn test_multiple_frames() {
        let limits = Limits::default();
        let mut buf = Vec::new();

        let id1 = MessageId::new_uuid();
        let id2 = MessageId::new_uuid();
        let id3 = MessageId::new_uuid();

        let f1 = Frame::req(id1.clone(), "cap:op=first", b"one".to_vec(), "text/plain");
        let f2 = Frame::chunk(id2.clone(), 0, b"two".to_vec());
        let f3 = Frame::end(id3.clone(), Some(b"three".to_vec()));

        write_frame(&mut buf, &f1, &limits).unwrap();
        write_frame(&mut buf, &f2, &limits).unwrap();
        write_frame(&mut buf, &f3, &limits).unwrap();

        let mut cursor = Cursor::new(buf);

        let r1 = read_frame(&mut cursor, &limits).unwrap().unwrap();
        assert_eq!(r1.frame_type, FrameType::Req);
        assert_eq!(r1.id, id1);

        let r2 = read_frame(&mut cursor, &limits).unwrap().unwrap();
        assert_eq!(r2.frame_type, FrameType::Chunk);
        assert_eq!(r2.id, id2);

        let r3 = read_frame(&mut cursor, &limits).unwrap().unwrap();
        assert_eq!(r3.frame_type, FrameType::End);
        assert_eq!(r3.id, id3);

        // EOF
        assert!(read_frame(&mut cursor, &limits).unwrap().is_none());
    }

    #[test]
    fn test_frame_too_large() {
        let limits = Limits {
            max_frame: 100,
            max_chunk: 50,
        };

        let id = MessageId::new_uuid();
        let large_payload = vec![0u8; 200];
        let frame = Frame::req(id, "cap:op=test", large_payload, "application/octet-stream");

        let mut buf = Vec::new();
        let result = write_frame(&mut buf, &frame, &limits);
        assert!(matches!(result, Err(CborError::FrameTooLarge { .. })));
    }

    #[test]
    fn test_write_chunked() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 10, // Very small for testing
        };

        let mut buf = Vec::new();
        let mut writer = FrameWriter::with_limits(&mut buf, limits);

        let id = MessageId::new_uuid();
        let data = b"Hello, this is a longer message that will be chunked!";

        writer
            .write_chunked(id.clone(), "text/plain", data)
            .expect("chunked write should succeed");

        // Read back all chunks
        let mut cursor = Cursor::new(buf);
        let mut reader = FrameReader::with_limits(&mut cursor, limits);

        let mut received = Vec::new();
        let mut chunk_count = 0;

        loop {
            let frame = reader.read().unwrap();
            match frame {
                Some(f) => {
                    assert_eq!(f.frame_type, FrameType::Chunk);
                    assert_eq!(f.id, id);
                    assert_eq!(f.seq, chunk_count);

                    let is_eof = f.is_eof();
                    if let Some(payload) = f.payload {
                        received.extend_from_slice(&payload);
                    }

                    if is_eof {
                        break;
                    }
                    chunk_count += 1;
                }
                None => break,
            }
        }

        assert_eq!(received, data);
        assert!(chunk_count > 0); // Should have multiple chunks
    }

    #[test]
    fn test_eof_handling() {
        let limits = Limits::default();
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor, &limits).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_message_id_uint() {
        let id = MessageId::Uint(12345);
        let frame = Frame::new(FrameType::Req, id.clone());

        let bytes = encode_frame(&frame).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.id, id);
    }
}
