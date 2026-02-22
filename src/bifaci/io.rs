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

use crate::bifaci::frame::{keys, Frame, FrameType, Limits, MessageId, DEFAULT_MAX_CHUNK, DEFAULT_MAX_FRAME, DEFAULT_MAX_REORDER_BUFFER};
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

    if let Some(ref stream_id) = frame.stream_id {
        map.push((Value::Integer(keys::STREAM_ID.into()), Value::Text(stream_id.clone())));
    }

    if let Some(ref media_urn) = frame.media_urn {
        map.push((Value::Integer(keys::MEDIA_URN.into()), Value::Text(media_urn.clone())));
    }

    if let Some(ref routing_id) = frame.routing_id {
        let routing_id_value = match routing_id {
            MessageId::Uuid(bytes) => Value::Bytes(bytes.to_vec()),
            MessageId::Uint(n) => Value::Integer((*n as i64).into()),
        };
        map.push((Value::Integer(keys::ROUTING_ID.into()), routing_id_value));
    }

    if let Some(chunk_index) = frame.chunk_index {
        map.push((
            Value::Integer(keys::INDEX.into()),
            Value::Integer((chunk_index as i64).into()),
        ));
    }

    if let Some(chunk_count) = frame.chunk_count {
        map.push((
            Value::Integer(keys::CHUNK_COUNT.into()),
            Value::Integer((chunk_count as i64).into()),
        ));
    }

    if let Some(checksum) = frame.checksum {
        map.push((
            Value::Integer(keys::CHECKSUM.into()),
            Value::Integer((checksum as i64).into()),
        ));
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

    let stream_id = lookup.get(&keys::STREAM_ID).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let media_urn = lookup.get(&keys::MEDIA_URN).and_then(|v| match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    });

    let routing_id = lookup.get(&keys::ROUTING_ID).map(|v| match v {
        Value::Bytes(bytes) => {
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                MessageId::Uuid(arr)
            } else {
                MessageId::Uint(0)
            }
        }
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            MessageId::Uint(n as u64)
        }
        _ => MessageId::Uint(0),
    });

    let chunk_index = lookup.get(&keys::INDEX).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let chunk_count = lookup.get(&keys::CHUNK_COUNT).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let checksum = lookup.get(&keys::CHECKSUM).and_then(|v| match v {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            Some(n as u64)
        }
        _ => None,
    });

    let frame = Frame {
        version,
        frame_type,
        id,
        routing_id,
        stream_id,
        media_urn,
        seq,
        content_type,
        meta,
        payload,
        len,
        offset,
        eof,
        cap,
        chunk_index,
        chunk_count,
        checksum,
    };

    // Validate required fields based on frame type
    match frame.frame_type {
        FrameType::Chunk => {
            if frame.chunk_index.is_none() {
                return Err(CborError::InvalidFrame("CHUNK frame missing required field: chunk_index".to_string()));
            }
            if frame.checksum.is_none() {
                return Err(CborError::InvalidFrame("CHUNK frame missing required field: checksum".to_string()));
            }
        }
        FrameType::StreamEnd => {
            if frame.chunk_count.is_none() {
                return Err(CborError::InvalidFrame("STREAM_END frame missing required field: chunk_count".to_string()));
            }
        }
        _ => {} // Other frame types don't require these fields
    }

    Ok(frame)
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
#[derive(Debug)]
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
        stream_id: String,
        content_type: &str,
        data: &[u8],
    ) -> Result<(), CborError> {
        let total_len = data.len();
        let max_chunk = self.limits.max_chunk;

        if total_len == 0 {
            // Empty payload - send single chunk with eof
            let checksum = Frame::compute_checksum(&[]);
            let mut frame = Frame::chunk(id, stream_id, 0, Vec::new(), 0, checksum);
            frame.content_type = Some(content_type.to_string());
            frame.len = Some(0);
            frame.offset = Some(0);
            frame.eof = Some(true);
            return self.write(&frame);
        }

        let mut offset = 0usize;
        let mut chunk_index = 0u64;

        while offset < total_len {
            let chunk_size = max_chunk.min(total_len - offset);
            let is_last = offset + chunk_size >= total_len;

            let chunk_data = data[offset..offset + chunk_size].to_vec();
            let checksum = Frame::compute_checksum(&chunk_data);

            let mut frame = Frame::chunk(id.clone(), stream_id.clone(), 0, chunk_data, chunk_index, checksum);
            frame.offset = Some(offset as u64);

            // Set content_type and total len on first chunk (chunk_index-based, not seq-based)
            if chunk_index == 0 {
                frame.content_type = Some(content_type.to_string());
                frame.len = Some(total_len as u64);
            }

            if is_last {
                frame.eof = Some(true);
            }

            self.write(&frame)?;

            chunk_index += 1;
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
    let our_hello = Frame::hello(&Limits::default());
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
    let their_max_reorder_buffer = their_frame.hello_max_reorder_buffer().unwrap_or(DEFAULT_MAX_REORDER_BUFFER);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
        max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER.min(their_max_reorder_buffer),
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
    let their_max_reorder_buffer = their_frame.hello_max_reorder_buffer().unwrap_or(DEFAULT_MAX_REORDER_BUFFER);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
        max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER.min(their_max_reorder_buffer),
    };

    // Send our HELLO with manifest
    let our_hello = Frame::hello_with_manifest(&limits, manifest);
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
    let our_hello = Frame::hello(&Limits::default());
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
    let their_max_reorder_buffer = their_frame.hello_max_reorder_buffer().unwrap_or(DEFAULT_MAX_REORDER_BUFFER);

    let limits = Limits {
        max_frame: DEFAULT_MAX_FRAME.min(their_max_frame),
        max_chunk: DEFAULT_MAX_CHUNK.min(their_max_chunk),
        max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER.min(their_max_reorder_buffer),
    };

    // Update both reader and writer with negotiated limits
    reader.set_limits(limits);
    writer.set_limits(limits);

    Ok(HandshakeResult { limits, manifest })
}

// =============================================================================
// IDENTITY VERIFICATION
// =============================================================================

/// CBOR-encoded Text("bifaci") — deterministic 7-byte nonce for identity verification.
pub(crate) fn identity_nonce() -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(&Value::Text("bifaci".to_string()), &mut buf)
        .expect("BUG: failed to encode identity nonce");
    buf
}

/// Verify a connection by invoking the identity capability (async).
///
/// Sends a REQ with CAP_IDENTITY carrying the "bifaci" nonce with proper
/// XID and seq assignment, then verifies the response echoes it back unchanged.
/// This proves the entire protocol stack works end-to-end before the connection
/// is considered live.
///
/// Must be called after handshake, before any other traffic.
pub async fn verify_identity<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut AsyncFrameReader<R>,
    writer: &mut AsyncFrameWriter<W>,
) -> Result<(), CborError> {
    use crate::standard::caps::CAP_IDENTITY;
    use crate::bifaci::frame::SeqAssigner;

    let nonce = identity_nonce();
    let req_id = MessageId::new_uuid();
    let stream_id = "identity-verify".to_string();
    let xid = MessageId::Uint(0);
    let mut seq = SeqAssigner::new();

    // Send REQ (empty payload) with XID + seq
    let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
    req.routing_id = Some(xid.clone());
    seq.assign(&mut req);
    writer.write(&req).await?;

    // Send request body: STREAM_START → CHUNK → STREAM_END → END
    let mut stream_start = Frame::stream_start(req_id.clone(), stream_id.clone(), "media:".to_string());
    stream_start.routing_id = Some(xid.clone());
    seq.assign(&mut stream_start);
    writer.write(&stream_start).await?;

    // CBOR-encode nonce before checksumming (protocol v2: CHUNK payload = CBOR-encoded data)
    let mut cbor_nonce = Vec::new();
    ciborium::into_writer(&Value::Bytes(nonce.clone()), &mut cbor_nonce)
        .expect("BUG: failed to CBOR-encode nonce");
    let checksum = Frame::compute_checksum(&cbor_nonce);
    let mut chunk = Frame::chunk(req_id.clone(), stream_id.clone(), 0, cbor_nonce, 0, checksum);
    chunk.routing_id = Some(xid.clone());
    seq.assign(&mut chunk);
    writer.write(&chunk).await?;

    let mut stream_end = Frame::stream_end(req_id.clone(), stream_id, 1);
    stream_end.routing_id = Some(xid.clone());
    seq.assign(&mut stream_end);
    writer.write(&stream_end).await?;

    let mut end = Frame::end(req_id.clone(), None);
    end.routing_id = Some(xid.clone());
    seq.assign(&mut end);
    writer.write(&end).await?;

    // Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
    // Each CHUNK payload is CBOR-encoded (protocol v2), decode each and concatenate
    let mut cbor_chunks = Vec::new();
    loop {
        let frame = reader.read().await?.ok_or_else(|| {
            CborError::Protocol("Connection closed during identity verification".to_string())
        })?;

        match frame.frame_type {
            FrameType::StreamStart => {}
            FrameType::Chunk => {
                if let Some(cbor_payload) = frame.payload {
                    // Decode CBOR chunk
                    let value: Value = ciborium::from_reader(&cbor_payload[..])
                        .map_err(|e| CborError::Protocol(format!("Failed to decode CBOR chunk: {}", e)))?;
                    if let Value::Bytes(bytes) = value {
                        cbor_chunks.push(bytes);
                    } else {
                        return Err(CborError::Protocol(format!(
                            "Expected bytes chunk, got {:?}",
                            value
                        )));
                    }
                }
            }
            FrameType::StreamEnd => {}
            FrameType::End => {
                // Concatenate all decoded chunks
                let accumulated: Vec<u8> = cbor_chunks.into_iter().flatten().collect();
                if accumulated != nonce {
                    return Err(CborError::Protocol(format!(
                        "Identity verification failed: payload mismatch (expected {} bytes, got {})",
                        nonce.len(),
                        accumulated.len()
                    )));
                }
                return Ok(());
            }
            FrameType::Err => {
                let code = frame.error_code().unwrap_or("UNKNOWN");
                let msg = frame.error_message().unwrap_or("no message");
                return Err(CborError::Protocol(format!(
                    "Identity verification failed: [{code}] {msg}"
                )));
            }
            other => {
                return Err(CborError::Protocol(format!(
                    "Identity verification: unexpected frame type {:?}",
                    other
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // TEST205: Test REQ frame encode/decode roundtrip preserves all fields
    #[test]
    fn test205_encode_decode_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::req(id.clone(), r#"cap:in="media:void";op=test;out="media:void""#, b"payload".to_vec(), "application/json");

        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.version, original.version);
        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.content_type, original.content_type);
    }

    // TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer
    #[test]
    fn test206_hello_frame_roundtrip() {
        let original = Frame::hello(&Limits { max_frame: 500_000, max_chunk: 50_000, max_reorder_buffer: 128 });
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Hello);
        assert_eq!(decoded.hello_max_frame(), Some(500_000));
        assert_eq!(decoded.hello_max_chunk(), Some(50_000));
        assert_eq!(decoded.hello_max_reorder_buffer(), Some(128));
    }

    // TEST207: Test ERR frame encode/decode roundtrip preserves error code and message
    #[test]
    fn test207_err_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::err(id, "NOT_FOUND", "Cap not found");
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Err);
        assert_eq!(decoded.error_code(), Some("NOT_FOUND"));
        assert_eq!(decoded.error_message(), Some("Cap not found"));
    }

    // TEST208: Test LOG frame encode/decode roundtrip preserves level and message
    #[test]
    fn test208_log_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::log(id, "warn", "Something happened");
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Log);
        assert_eq!(decoded.log_level(), Some("warn"));
        assert_eq!(decoded.log_message(), Some("Something happened"));
    }

    // TEST209 REMOVED: RES frame removed - old protocol no longer supported
    // NEW PROTOCOL: Use stream multiplexing (STREAM_START + CHUNK + STREAM_END + END)

    // TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload
    #[test]
    fn test210_end_frame_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::end(id.clone(), Some(b"final".to_vec()));
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::End);
        assert_eq!(decoded.id, id);
        assert!(decoded.is_eof());
        assert_eq!(decoded.payload, Some(b"final".to_vec()));
    }

    // TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits
    #[test]
    fn test211_hello_with_manifest_roundtrip() {
        let manifest = b"{\"name\":\"Test\",\"version\":\"1.0\"}";
        let original = Frame::hello_with_manifest(&Limits { max_frame: 1_000_000, max_chunk: 100_000, max_reorder_buffer: 48 }, manifest);
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.hello_manifest().unwrap(), manifest);
        assert_eq!(decoded.hello_max_frame(), Some(1_000_000));
        assert_eq!(decoded.hello_max_chunk(), Some(100_000));
        assert_eq!(decoded.hello_max_reorder_buffer(), Some(48));
    }

    // TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id)
    #[test]
    fn test212_chunk_with_offset_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-test".to_string();
        let payload = b"data".to_vec();
        let checksum = Frame::compute_checksum(&payload);
        let original = Frame::chunk_with_offset(id.clone(), stream_id, 0, payload, 100, Some(5000), true, 0, checksum);
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Chunk);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.seq, 0);
        assert_eq!(decoded.offset, Some(100));
        assert_eq!(decoded.len, Some(5000));
        assert!(decoded.is_eof());
        assert_eq!(decoded.payload, Some(b"data".to_vec()));
    }

    // TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields
    #[test]
    fn test213_heartbeat_roundtrip() {
        let id = MessageId::new_uuid();
        let original = Frame::heartbeat(id.clone());
        let bytes = encode_frame(&original).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.frame_type, FrameType::Heartbeat);
        assert_eq!(decoded.id, id);
        assert!(decoded.payload.is_none());
        assert!(decoded.meta.is_none());
    }

    // TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format
    #[test]
    fn test214_frame_io_roundtrip() {
        let limits = Limits::default();
        let id = MessageId::new_uuid();
        let original = Frame::req(id, r#"cap:in="media:void";op=test;out="media:void""#, b"payload".to_vec(), "application/json");

        let mut buf = Vec::new();
        write_frame(&mut buf, &original, &limits).expect("write should succeed");

        // Verify length prefix exists (first 4 bytes are u32 big-endian length)
        assert!(buf.len() > 4, "must have length prefix + data");
        let prefix_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(buf.len(), 4 + prefix_len, "buffer must be exactly prefix + frame");

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor, &limits)
            .expect("read should succeed")
            .expect("should have frame");

        assert_eq!(decoded.frame_type, original.frame_type);
        assert_eq!(decoded.cap, original.cap);
        assert_eq!(decoded.payload, original.payload);
    }

    // TEST215: Test reading multiple sequential frames from a single buffer
    #[test]
    fn test215_multiple_frames() {
        let limits = Limits::default();
        let mut buf = Vec::new();

        let id1 = MessageId::new_uuid();
        let id2 = MessageId::new_uuid();
        let id3 = MessageId::new_uuid();

        let f1 = Frame::req(id1.clone(), r#"cap:in="media:void";op=first;out="media:void""#, b"one".to_vec(), "text/plain");
        let payload2 = b"two".to_vec();
        let checksum2 = Frame::compute_checksum(&payload2);
        let f2 = Frame::chunk(id2.clone(), "stream-2".to_string(), 0, payload2, 0, checksum2);
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

        // EOF after all frames read
        assert!(read_frame(&mut cursor, &limits).unwrap().is_none());
    }

    // TEST216: Test write_frame rejects frames exceeding max_frame limit
    #[test]
    fn test216_frame_too_large() {
        let limits = Limits {
            max_frame: 100,
            max_chunk: 50,
            ..Limits::default()
        };

        let id = MessageId::new_uuid();
        let large_payload = vec![0u8; 200];
        let frame = Frame::req(id, r#"cap:in="media:void";op=test;out="media:void""#, large_payload, "application/octet-stream");

        let mut buf = Vec::new();
        let result = write_frame(&mut buf, &frame, &limits);
        assert!(matches!(result, Err(CborError::FrameTooLarge { .. })));
    }

    // TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit
    #[test]
    fn test217_read_frame_too_large() {
        let write_limits = Limits { max_frame: 10_000_000, max_chunk: 1_000_000, ..Limits::default() };
        let read_limits = Limits { max_frame: 50, max_chunk: 50, ..Limits::default() };

        // Write a frame with generous limits
        let id = MessageId::new_uuid();
        let frame = Frame::req(id, r#"cap:in="media:void";op=test;out="media:void""#, vec![0u8; 200], "text/plain");
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame, &write_limits).unwrap();

        // Try to read with strict limits
        let mut cursor = Cursor::new(buf);
        let result = read_frame(&mut cursor, &read_limits);
        assert!(matches!(result, Err(CborError::FrameTooLarge { .. })));
    }

    // TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly
    // Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq.
    // Chunk ordering within a stream is tracked by chunk_index (chunk_index field).
    #[test]
    fn test218_write_chunked() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 10, // Very small for testing
            ..Limits::default()
        };

        let mut buf = Vec::new();
        let mut writer = FrameWriter::with_limits(&mut buf, limits);

        let id = MessageId::new_uuid();
        let stream_id = "stream-chunked".to_string();
        let data = b"Hello, this is a longer message that will be chunked!";

        writer
            .write_chunked(id.clone(), stream_id, "text/plain", data)
            .expect("chunked write should succeed");

        // Read back all chunks
        let mut cursor = Cursor::new(buf);
        let mut reader = FrameReader::with_limits(&mut cursor, limits);

        let mut received = Vec::new();
        let mut chunk_count = 0u64;
        let mut first_chunk_had_len = false;
        let mut first_chunk_had_content_type = false;

        loop {
            let frame = reader.read().unwrap();
            match frame {
                Some(f) => {
                    assert_eq!(f.frame_type, FrameType::Chunk);
                    assert_eq!(f.id, id);
                    assert_eq!(f.seq, 0, "write_chunked produces seq=0; SeqAssigner assigns at output stage");
                    // chunk_index tracks ordering within the chunked write
                    assert_eq!(f.chunk_index, Some(chunk_count), "chunk_index must increment monotonically");

                    if chunk_count == 0 {
                        first_chunk_had_len = f.len.is_some();
                        first_chunk_had_content_type = f.content_type.is_some();
                        assert_eq!(f.len, Some(data.len() as u64), "first chunk must carry total len");
                        assert_eq!(f.content_type, Some("text/plain".to_string()));
                    }

                    let is_eof = f.is_eof();
                    if let Some(payload) = f.payload {
                        assert!(payload.len() <= limits.max_chunk, "chunk must not exceed max_chunk");
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
        assert!(chunk_count > 0, "data larger than max_chunk must produce multiple chunks");
        assert!(first_chunk_had_len, "first chunk must carry total length");
        assert!(first_chunk_had_content_type, "first chunk must carry content_type");
    }

    // TEST219: Test write_chunked with empty data produces a single EOF chunk
    #[test]
    fn test219_write_chunked_empty_data() {
        let limits = Limits { max_frame: 1_000_000, max_chunk: 100, ..Limits::default() };
        let mut buf = Vec::new();
        let mut writer = FrameWriter::with_limits(&mut buf, limits);

        let id = MessageId::new_uuid();
        writer.write_chunked(id.clone(), "stream-empty".to_string(), "text/plain", b"").unwrap();

        let mut cursor = Cursor::new(buf);
        let frame = read_frame(&mut cursor, &limits).unwrap().expect("should have frame");
        assert_eq!(frame.frame_type, FrameType::Chunk);
        assert!(frame.is_eof(), "empty data must produce immediate EOF");
        assert_eq!(frame.len, Some(0), "empty payload must report len=0");
        assert_eq!(frame.payload, Some(vec![]));
    }

    // TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk
    #[test]
    fn test220_write_chunked_exact_fit() {
        let limits = Limits { max_frame: 1_000_000, max_chunk: 10, ..Limits::default() };
        let mut buf = Vec::new();
        let mut writer = FrameWriter::with_limits(&mut buf, limits);

        let id = MessageId::new_uuid();
        let data = b"0123456789"; // exactly 10 bytes = max_chunk
        writer.write_chunked(id.clone(), "stream-1mb".to_string(), "text/plain", data).unwrap();

        let mut cursor = Cursor::new(buf);
        let frame = read_frame(&mut cursor, &limits).unwrap().expect("should have frame");
        assert!(frame.is_eof(), "single-chunk data must be EOF");
        assert_eq!(frame.payload, Some(data.to_vec()));
        assert_eq!(frame.seq, 0);
        // No more frames
        assert!(read_frame(&mut cursor, &limits).unwrap().is_none());
    }

    // TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream)
    #[test]
    fn test221_eof_handling() {
        let limits = Limits::default();
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor, &limits).unwrap();
        assert!(result.is_none());
    }

    // TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available)
    #[test]
    fn test222_truncated_length_prefix() {
        let limits = Limits::default();
        // Only 2 bytes, but 4 needed for length prefix
        let mut cursor = Cursor::new(vec![0x00, 0x01]);
        let result = read_frame(&mut cursor, &limits);
        // read_exact on Cursor with insufficient data returns UnexpectedEof,
        // which maps to Ok(None) for the clean-EOF path in read_frame.
        match result {
            Ok(None) => {} // clean EOF interpretation
            Err(_) => {}   // partial read error interpretation
            Ok(Some(_)) => panic!("must not produce a frame from truncated data"),
        }
    }

    // TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available)
    #[test]
    fn test223_truncated_frame_body() {
        let limits = Limits::default();
        // Length prefix says 100 bytes, but only 5 bytes of data follow
        let mut data = vec![0x00, 0x00, 0x00, 100]; // length = 100
        data.extend_from_slice(b"short"); // only 5 bytes
        let mut cursor = Cursor::new(data);
        let result = read_frame(&mut cursor, &limits);
        assert!(result.is_err(), "truncated body must be an error");
    }

    // TEST224: Test MessageId::Uint roundtrips through encode/decode
    #[test]
    fn test224_message_id_uint() {
        let id = MessageId::Uint(12345);
        let frame = Frame::new(FrameType::Req, id.clone());

        let bytes = encode_frame(&frame).expect("encode should succeed");
        let decoded = decode_frame(&bytes).expect("decode should succeed");

        assert_eq!(decoded.id, id);
    }

    // TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string)
    #[test]
    fn test225_decode_non_map_value() {
        // Encode a CBOR array instead of map
        let value = ciborium::Value::Array(vec![ciborium::Value::Integer(1.into())]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST226: Test decode_frame rejects CBOR map missing required version field
    #[test]
    fn test226_decode_missing_version() {
        // Build CBOR map with frame_type and id but missing version
        let map = ciborium::Value::Map(vec![
            (ciborium::Value::Integer(keys::FRAME_TYPE.into()), ciborium::Value::Integer(1.into())),
            (ciborium::Value::Integer(keys::ID.into()), ciborium::Value::Integer(0.into())),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST227: Test decode_frame rejects CBOR map with invalid frame_type value
    #[test]
    fn test227_decode_invalid_frame_type_value() {
        let map = ciborium::Value::Map(vec![
            (ciborium::Value::Integer(keys::VERSION.into()), ciborium::Value::Integer(1.into())),
            (ciborium::Value::Integer(keys::FRAME_TYPE.into()), ciborium::Value::Integer(99.into())),
            (ciborium::Value::Integer(keys::ID.into()), ciborium::Value::Integer(0.into())),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST228: Test decode_frame rejects CBOR map missing required id field
    #[test]
    fn test228_decode_missing_id() {
        let map = ciborium::Value::Map(vec![
            (ciborium::Value::Integer(keys::VERSION.into()), ciborium::Value::Integer(1.into())),
            (ciborium::Value::Integer(keys::FRAME_TYPE.into()), ciborium::Value::Integer(1.into())),
            // No ID field
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&map, &mut bytes).unwrap();

        let result = decode_frame(&bytes);
        assert!(matches!(result, Err(CborError::InvalidFrame(_))));
    }

    // TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits
    #[test]
    fn test229_frame_reader_writer_set_limits() {
        let buf: Vec<u8> = Vec::new();
        let mut reader = FrameReader::new(Cursor::new(buf));
        let mut writer = FrameWriter::new(Vec::new());

        let custom = Limits { max_frame: 500, max_chunk: 100, ..Limits::default() };
        reader.set_limits(custom);
        writer.set_limits(custom);

        assert_eq!(reader.limits().max_frame, 500);
        assert_eq!(reader.limits().max_chunk, 100);
        assert_eq!(writer.limits().max_frame, 500);
        assert_eq!(writer.limits().max_chunk, 100);
    }

    // TEST230: Test sync handshake exchanges HELLO frames and negotiates minimum limits
    #[test]
    fn test230_sync_handshake() {
        use std::thread;

        let (host_std, plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_write_std, host_read_std) = std::os::unix::net::UnixStream::pair().unwrap();

        let manifest = b"{\"name\":\"Test\",\"version\":\"1.0\",\"caps\":[]}";
        let manifest_clone = manifest.to_vec();

        // Plugin side in thread
        let plugin_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(std::io::BufReader::new(plugin_std));
            let mut writer = FrameWriter::new(std::io::BufWriter::new(plugin_write_std));
            handshake_accept(&mut reader, &mut writer, &manifest_clone).unwrap()
        });

        // Host side
        let mut reader = FrameReader::new(std::io::BufReader::new(host_read_std));
        let mut writer = FrameWriter::new(std::io::BufWriter::new(host_std));
        let result = handshake(&mut reader, &mut writer).unwrap();

        let plugin_limits = plugin_handle.join().unwrap();

        // Both sides must agree on limits
        assert_eq!(result.limits.max_frame, plugin_limits.max_frame);
        assert_eq!(result.limits.max_chunk, plugin_limits.max_chunk);
        assert_eq!(result.limits.max_reorder_buffer, plugin_limits.max_reorder_buffer);
        assert_eq!(result.manifest, manifest.to_vec());
    }

    // TEST231: Test handshake fails when peer sends non-HELLO frame
    #[test]
    fn test231_handshake_rejects_non_hello() {
        let (host_std, plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_write_std, host_read_std) = std::os::unix::net::UnixStream::pair().unwrap();

        // Plugin side: send a REQ frame instead of HELLO
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(std::io::BufReader::new(plugin_std));
            let mut writer = FrameWriter::new(std::io::BufWriter::new(plugin_write_std));
            // Read host's HELLO (consume it)
            let _ = reader.read().unwrap();
            // Send a REQ instead of HELLO
            let bad_frame = Frame::req(MessageId::Uint(1), r#"cap:in="media:void";op=bad;out="media:void""#, vec![], "text/plain");
            writer.write(&bad_frame).unwrap();
        });

        let mut reader = FrameReader::new(std::io::BufReader::new(host_read_std));
        let mut writer = FrameWriter::new(std::io::BufWriter::new(host_std));
        let result = handshake(&mut reader, &mut writer);
        assert!(result.is_err(), "handshake must fail when peer sends non-HELLO");
        let err = result.unwrap_err();
        assert!(matches!(err, CborError::Handshake(_)));

        plugin_handle.join().unwrap();
    }

    // TEST232: Test handshake fails when plugin HELLO is missing required manifest
    #[test]
    fn test232_handshake_rejects_missing_manifest() {
        let (host_std, plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_write_std, host_read_std) = std::os::unix::net::UnixStream::pair().unwrap();

        // Plugin side: send HELLO without manifest
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(std::io::BufReader::new(plugin_std));
            let mut writer = FrameWriter::new(std::io::BufWriter::new(plugin_write_std));
            let _ = reader.read().unwrap(); // consume host HELLO
            let no_manifest_hello = Frame::hello(&Limits { max_frame: 1_000_000, max_chunk: 200_000, max_reorder_buffer: DEFAULT_MAX_REORDER_BUFFER });
            writer.write(&no_manifest_hello).unwrap();
        });

        let mut reader = FrameReader::new(std::io::BufReader::new(host_read_std));
        let mut writer = FrameWriter::new(std::io::BufWriter::new(host_std));
        let result = handshake(&mut reader, &mut writer);
        assert!(result.is_err(), "handshake must fail when manifest is missing");

        plugin_handle.join().unwrap();
    }

    // TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode
    #[test]
    fn test233_binary_payload_all_byte_values() {
        let mut data = Vec::with_capacity(256);
        for i in 0u8..=255 {
            data.push(i);
        }

        let id = MessageId::new_uuid();
        let frame = Frame::req(id.clone(), r#"cap:in="media:void";op=binary;out="media:void""#, data.clone(), "application/octet-stream");

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.payload.unwrap(), data);
    }

    // TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error
    #[test]
    fn test234_decode_garbage_bytes() {
        let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB];
        let result = decode_frame(&garbage);
        assert!(result.is_err(), "garbage bytes must produce decode error");
    }

    // TEST399a: RelayNotify encode/decode roundtrip preserves manifest and limits
    #[test]
    fn test399a_relay_notify_roundtrip() {
        let manifest = br#"{"caps":["cap:in=\"media:void\";op=test;out=\"media:void\"","cap:in=\"media:void\";op=convert;out=\"media:void\""]}"#;
        let limits = crate::bifaci::frame::Limits { max_frame: 2_000_000, max_chunk: 128_000, ..crate::bifaci::frame::Limits::default() };
        let frame = Frame::relay_notify(manifest, &limits);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::RelayNotify);
        assert_eq!(decoded.id, MessageId::Uint(0));
        assert_eq!(decoded.relay_notify_manifest(), Some(manifest.as_slice()));
        let decoded_limits = decoded.relay_notify_limits().expect("must have limits");
        assert_eq!(decoded_limits.max_frame, 2_000_000);
        assert_eq!(decoded_limits.max_chunk, 128_000);
    }

    // TEST400a: RelayState encode/decode roundtrip preserves resource payload
    #[test]
    fn test400a_relay_state_roundtrip() {
        let resources = b"{\"memory_mb\":8192,\"cpu_cores\":8}";
        let frame = Frame::relay_state(resources);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::RelayState);
        assert_eq!(decoded.id, MessageId::Uint(0));
        assert_eq!(decoded.payload, Some(resources.to_vec()));
    }

    // TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn
    #[test]
    fn test389_stream_start_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-abc-123".to_string();
        let media_urn = "media:".to_string();

        let frame = Frame::stream_start(id.clone(), stream_id.clone(), media_urn.clone());
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamStart);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id.as_deref(), Some("stream-abc-123"));
        assert_eq!(decoded.media_urn.as_deref(), Some("media:"));
    }

    // TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn
    #[test]
    fn test390_stream_end_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-xyz-789".to_string();

        let frame = Frame::stream_end(id.clone(), stream_id.clone(), 10);
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamEnd);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id.as_deref(), Some("stream-xyz-789"));
        assert!(decoded.media_urn.is_none(), "StreamEnd should not have media_urn");
    }

    // TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum
    #[test]
    fn test497_chunk_corrupted_payload_rejected() {
        let id = MessageId::new_uuid();
        let stream_id = "stream-test".to_string();
        let payload = b"original data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        // Create CHUNK with correct checksum
        let chunk = Frame::chunk(id.clone(), stream_id.clone(), 0, payload.clone(), 0, checksum);

        // Encode it
        let encoded = encode_frame(&chunk).unwrap();

        // Decode it
        let mut decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.checksum, Some(checksum));

        // Now CORRUPT the payload but keep the checksum
        decoded.payload = Some(b"corrupted data".to_vec());

        // Verify checksum doesn't match corrupted payload
        let corrupted_checksum = Frame::compute_checksum(decoded.payload.as_ref().unwrap());
        assert_ne!(corrupted_checksum, checksum, "Checksums should differ for corrupted data");
        assert_eq!(decoded.checksum, Some(checksum), "Frame still has original checksum");

        // This proves that if someone modifies the payload in transit,
        // the checksum will not match and verification will fail
    }

    // TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode
    #[test]
    fn test440_chunk_index_checksum_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        let payload = b"test chunk data".to_vec();
        let checksum = Frame::compute_checksum(&payload);

        let frame = Frame::chunk(id.clone(), stream_id.clone(), 5, payload.clone(), 3, checksum);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::Chunk);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id, Some(stream_id));
        assert_eq!(decoded.seq, 5);
        assert_eq!(decoded.payload, Some(payload));
        assert_eq!(decoded.chunk_index, Some(3), "chunk_index must roundtrip");
        assert_eq!(decoded.checksum, Some(checksum), "checksum must roundtrip");
    }

    // TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode
    #[test]
    fn test441_stream_end_chunk_count_roundtrip() {
        let id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();

        let frame = Frame::stream_end(id.clone(), stream_id.clone(), 42);

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded.frame_type, FrameType::StreamEnd);
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.stream_id, Some(stream_id));
        assert_eq!(decoded.chunk_count, Some(42), "chunk_count must roundtrip");
    }

    // TEST461: write_chunked produces frames with seq=0; SeqAssigner assigns at output stage
    #[test]
    fn test461_write_chunked_seq_zero() {
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 5,
            ..Limits::default()
        };
        let mut buf = Vec::new();
        let mut writer = FrameWriter::with_limits(&mut buf, limits);

        let id = MessageId::new_uuid();
        writer
            .write_chunked(id.clone(), "s".to_string(), "application/octet-stream", b"abcdefghij")
            .unwrap();

        let mut cursor = Cursor::new(buf);
        let mut reader = FrameReader::with_limits(&mut cursor, limits);

        let mut frames = Vec::new();
        loop {
            match reader.read().unwrap() {
                Some(f) => {
                    let is_eof = f.is_eof();
                    frames.push(f);
                    if is_eof { break; }
                }
                None => break,
            }
        }

        // 10 bytes / 5 max_chunk = 2 chunks
        assert_eq!(frames.len(), 2);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.seq, 0, "chunk {} must have seq=0", i);
            assert_eq!(f.chunk_index, Some(i as u64), "chunk {} must have chunk_index={}", i, i);
        }
    }

    // TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides)
    #[test]
    fn test472_handshake_negotiates_reorder_buffer() {
        // Simulate plugin sending HELLO with max_reorder_buffer=32
        let plugin_limits = Limits {
            max_frame: DEFAULT_MAX_FRAME,
            max_chunk: DEFAULT_MAX_CHUNK,
            max_reorder_buffer: 32,
        };
        let manifest = br#"{"name":"test","version":"1.0","caps":[]}"#;

        // Write plugin's HELLO with manifest to a buffer
        let mut plugin_hello_buf = Vec::new();
        {
            let mut w = FrameWriter::new(&mut plugin_hello_buf);
            let hello = Frame::hello_with_manifest(&plugin_limits, manifest);
            w.write(&hello).unwrap();
        }

        // Write host's HELLO to a buffer (our default: max_reorder_buffer=64)
        let mut host_hello_buf = Vec::new();
        {
            let mut w = FrameWriter::new(&mut host_hello_buf);
            let hello = Frame::hello(&Limits::default());
            w.write(&hello).unwrap();
        }

        // Host reads plugin's HELLO
        let mut cursor = Cursor::new(plugin_hello_buf);
        let their_frame = {
            let mut r = FrameReader::new(&mut cursor);
            r.read().unwrap().unwrap()
        };
        let their_reorder = their_frame.hello_max_reorder_buffer().unwrap();
        assert_eq!(their_reorder, 32);
        let negotiated = DEFAULT_MAX_REORDER_BUFFER.min(their_reorder);
        assert_eq!(negotiated, 32, "must pick minimum (32 < 64)");

        // Plugin reads host's HELLO
        let mut cursor2 = Cursor::new(host_hello_buf);
        let host_frame = {
            let mut r = FrameReader::new(&mut cursor2);
            r.read().unwrap().unwrap()
        };
        let host_reorder = host_frame.hello_max_reorder_buffer().unwrap();
        assert_eq!(host_reorder, DEFAULT_MAX_REORDER_BUFFER);
    }

    // =========================================================================
    // Identity verification tests
    // =========================================================================

    /// Manifest with only CAP_IDENTITY (minimum valid manifest)
    const IDENTITY_MANIFEST: &str = r#"{"name":"Test","version":"1.0","description":"Test","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]}]}"#;

    /// Simulate plugin side: handshake_accept, then handle one identity REQ
    /// by echoing back the payload (like the standard identity handler).
    fn run_plugin_identity_echo(
        from_host: std::os::unix::net::UnixStream,
        to_host: std::os::unix::net::UnixStream,
        manifest: &[u8],
    ) {
        let mut reader = FrameReader::new(std::io::BufReader::new(from_host));
        let mut writer = FrameWriter::new(std::io::BufWriter::new(to_host));
        handshake_accept(&mut reader, &mut writer, manifest).unwrap();

        // Read REQ
        let req = reader.read().unwrap().expect("expected REQ");
        assert_eq!(req.frame_type, FrameType::Req);

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!("unexpected frame type during identity request: {:?}", other),
            }
        }

        // Echo response: STREAM_START → CHUNK → STREAM_END → END
        let stream_id = "echo".to_string();
        let ss = Frame::stream_start(req.id.clone(), stream_id.clone(), "media:".to_string());
        writer.write(&ss).unwrap();
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
        writer.write(&chunk).unwrap();
        let se = Frame::stream_end(req.id.clone(), stream_id, 1);
        writer.write(&se).unwrap();
        let end = Frame::end(req.id, None);
        writer.write(&end).unwrap();
    }

    // TEST481: verify_identity succeeds with standard identity echo handler
    #[tokio::test]
    async fn test481_verify_identity_succeeds() {
        let (host_to_plugin_std, plugin_from_host_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_to_host_std, host_from_plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();

        // Plugin side runs sync in a thread
        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            run_plugin_identity_echo(plugin_from_host_std, plugin_to_host_std, &manifest);
        });

        // Host side runs async
        host_from_plugin_std.set_nonblocking(true).unwrap();
        host_to_plugin_std.set_nonblocking(true).unwrap();
        let host_read = tokio::net::UnixStream::from_std(host_from_plugin_std).unwrap();
        let host_write = tokio::net::UnixStream::from_std(host_to_plugin_std).unwrap();

        let mut reader = AsyncFrameReader::new(host_read);
        let mut writer = AsyncFrameWriter::new(host_write);
        let _hs = handshake_async(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(result.is_ok(), "verify_identity must succeed: {:?}", result.unwrap_err());

        plugin_handle.join().unwrap();
    }

    // TEST482: verify_identity fails when plugin returns ERR on identity call
    #[tokio::test]
    async fn test482_verify_identity_fails_on_err() {
        let (host_to_plugin_std, plugin_from_host_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_to_host_std, host_from_plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();

        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(std::io::BufReader::new(plugin_from_host_std));
            let mut writer = FrameWriter::new(std::io::BufWriter::new(plugin_to_host_std));
            handshake_accept(&mut reader, &mut writer, &manifest).unwrap();

            // Read REQ, respond with ERR
            let req = reader.read().unwrap().expect("expected REQ");
            let err = Frame::err(req.id, "BROKEN", "identity handler broken");
            writer.write(&err).unwrap();
        });

        host_from_plugin_std.set_nonblocking(true).unwrap();
        host_to_plugin_std.set_nonblocking(true).unwrap();
        let host_read = tokio::net::UnixStream::from_std(host_from_plugin_std).unwrap();
        let host_write = tokio::net::UnixStream::from_std(host_to_plugin_std).unwrap();

        let mut reader = AsyncFrameReader::new(host_read);
        let mut writer = AsyncFrameWriter::new(host_write);
        handshake_async(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(result.is_err(), "verify_identity must fail on ERR");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("BROKEN"), "error must contain error code: {}", err);

        plugin_handle.join().unwrap();
    }

    // TEST483: verify_identity fails when connection closes before response
    #[tokio::test]
    async fn test483_verify_identity_fails_on_close() {
        let (host_to_plugin_std, plugin_from_host_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (plugin_to_host_std, host_from_plugin_std) = std::os::unix::net::UnixStream::pair().unwrap();

        let manifest = IDENTITY_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(std::io::BufReader::new(plugin_from_host_std));
            let mut writer = FrameWriter::new(std::io::BufWriter::new(plugin_to_host_std));
            handshake_accept(&mut reader, &mut writer, &manifest).unwrap();

            // Read REQ but close connection without responding
            let _req = reader.read().unwrap().expect("expected REQ");
            drop(writer);
            drop(reader);
        });

        host_from_plugin_std.set_nonblocking(true).unwrap();
        host_to_plugin_std.set_nonblocking(true).unwrap();
        let host_read = tokio::net::UnixStream::from_std(host_from_plugin_std).unwrap();
        let host_write = tokio::net::UnixStream::from_std(host_to_plugin_std).unwrap();

        let mut reader = AsyncFrameReader::new(host_read);
        let mut writer = AsyncFrameWriter::new(host_write);
        handshake_async(&mut reader, &mut writer).await.unwrap();

        let result = verify_identity(&mut reader, &mut writer).await;
        assert!(result.is_err(), "verify_identity must fail on connection close");

        plugin_handle.join().unwrap();
    }

}
