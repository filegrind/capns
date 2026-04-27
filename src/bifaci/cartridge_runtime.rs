//! Cartridge Runtime - Unified I/O handling for cartridge binaries
//!
//! The CartridgeRuntime provides a unified interface for cartridge binaries to handle
//! cap invocations. Cartridges register handlers for caps they provide, and the
//! runtime handles all I/O mechanics:
//!
//! - **Automatic mode detection**: CLI mode vs Cartridge CBOR mode
//! - CBOR frame encoding/decoding (Cartridge mode)
//! - CLI argument parsing from cap definitions (CLI mode)
//! - Handler routing by cap URN
//! - Real-time streaming response support
//! - HELLO handshake for limit negotiation
//! - **Multiplexed concurrent request handling**
//!
//! # Invocation Modes
//!
//! - **No CLI arguments**: Cartridge CBOR mode - HELLO handshake, REQ/RES frames via stdin/stdout
//! - **Any CLI arguments**: CLI mode - parse args based on cap definitions
//!
//! # Example
//!
//! ```ignore
//! use capdag::CartridgeRuntime;
//!
//! fn main() {
//!     let manifest = build_manifest(); // Your manifest with caps
//!     let mut runtime = CartridgeRuntime::new(manifest);
//!
//!     runtime.register::<MyRequest, _>("cap:op=my_op;...", |request, output, peer| {
//!         output.log("info", "Starting work...");
//!         output.emit_cbor(&ciborium::Value::Bytes(b"result".to_vec()))?;
//!         Ok(())
//!     });
//!
//!     // runtime.run() automatically detects CLI vs Cartridge CBOR mode
//!     runtime.run().unwrap();
//! }
//! ```

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{handshake_accept, CborError, FrameReader, FrameWriter};
use crate::bifaci::manifest::CapManifest;
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::{ArgSource, Cap, CapArg};
use crate::standard::caps::{CAP_ADAPTER_SELECTION, CAP_DISCARD, CAP_IDENTITY};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::{MediaUrn, MEDIA_FILE_PATH};
use async_trait::async_trait;
// crossbeam is used for demux_multi_stream (bridging sync stdin reads to async handlers)
use ops::{DryContext, Op, OpError, OpMetadata, OpResult, WetContext};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::os::unix::io::FromRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::task::JoinHandle;

/// Errors that can occur in the cartridge runtime
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("CBOR error: {0}")]
    Cbor(#[from] CborError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("No handler registered for cap: {0}")]
    NoHandler(String),

    #[error("Handler error: {0}")]
    Handler(String),

    #[error("Cap URN parse error: {0}")]
    CapUrn(String),

    #[error("Deserialization error: {0}")]
    Deserialize(String),

    #[error("Serialization error: {0}")]
    Serialize(String),

    #[error("Peer request error: {0}")]
    PeerRequest(String),

    #[error("Peer response error: {0}")]
    PeerResponse(String),

    #[error("CLI error: {0}")]
    Cli(String),

    #[error("Missing required argument: {0}")]
    MissingArgument(String),

    #[error("Unknown subcommand: {0}")]
    UnknownSubcommand(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Stream error: {0}")]
    Stream(#[from] StreamError),
}

// =============================================================================
// STREAM ABSTRACTIONS — hide the frame protocol from handlers
// =============================================================================

/// Per-stream or per-item metadata carried on frames.
///
/// In non-sequence mode, set once on STREAM_START — describes the whole stream.
/// In sequence mode, set per-item on CHUNK frames — describes each item.
pub type StreamMeta = BTreeMap<String, ciborium::Value>;

/// Errors that can occur during stream operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("Remote error [{code}]: {message}")]
    RemoteError { code: String, message: String },

    #[error("Stream closed")]
    Closed,

    #[error("CBOR decode error: {0}")]
    Decode(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Allows sending frames directly through the output channel.
/// Internal to the runtime — handlers never see this.
pub trait FrameSender: Send + Sync {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError>;
}

/// A single input stream — yields decoded CBOR values from CHUNK frames.
/// Handler never sees Frame, STREAM_START, STREAM_END, checksum, seq, or index.
///
/// This is an async stream. Use `recv()` to get the next value with metadata,
/// or `recv_data()` / `collect_*` methods if you only need the data.
///
/// Metadata semantics depend on mode:
/// - Non-sequence: `stream_meta()` returns the STREAM_START metadata (whole-stream).
/// - Sequence: `recv()` delivers per-item metadata from CHUNK frames.
pub struct InputStream {
    media_urn: String,
    stream_meta: Option<StreamMeta>,
    rx: tokio::sync::mpsc::UnboundedReceiver<
        Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
    >,
}

impl InputStream {
    /// Media URN of this stream (from STREAM_START).
    pub fn media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Stream-level metadata from STREAM_START (non-sequence mode).
    pub fn stream_meta(&self) -> Option<&StreamMeta> {
        self.stream_meta.as_ref()
    }

    /// Receive the next CBOR value with per-item metadata from this stream.
    /// Returns None when the stream ends.
    pub async fn recv(
        &mut self,
    ) -> Option<Result<(ciborium::Value, Option<StreamMeta>), StreamError>> {
        self.rx.recv().await
    }

    /// Receive the next CBOR value, discarding any per-item metadata.
    /// Convenience for handlers that don't use metadata.
    pub async fn recv_data(&mut self) -> Option<Result<ciborium::Value, StreamError>> {
        match self.rx.recv().await {
            Some(Ok((value, _meta))) => Some(Ok(value)),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    /// Collect each chunk as a separate item with its metadata.
    /// For sequence streams (is_sequence=true), each chunk is one item.
    /// Returns a Vec of (raw_bytes, optional_per_item_meta).
    pub async fn collect_items(
        mut self,
    ) -> Result<Vec<(Vec<u8>, Option<StreamMeta>)>, StreamError> {
        let mut items = Vec::new();
        while let Some(item) = self.recv().await {
            let (value, meta) = item?;
            let bytes = match value {
                ciborium::Value::Bytes(b) => b,
                ciborium::Value::Text(s) => s.into_bytes(),
                other => {
                    let mut buf = Vec::new();
                    ciborium::into_writer(&other, &mut buf).map_err(|e| {
                        StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                    })?;
                    buf
                }
            };
            items.push((bytes, meta));
        }
        Ok(items)
    }

    /// Collect all chunks into a single byte vector.
    /// Extracts inner bytes from Value::Bytes/Text and concatenates.
    /// Per-item metadata is discarded.
    ///
    /// WARNING: Only call this if you know the stream is finite.
    /// Infinite streams will block forever.
    pub async fn collect_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        let mut result = Vec::new();
        while let Some(item) = self.recv().await {
            let (value, _meta) = item?;
            match value {
                ciborium::Value::Bytes(b) => result.extend(b),
                ciborium::Value::Text(s) => result.extend(s.into_bytes()),
                other => {
                    // For non-byte types, CBOR-encode them
                    let mut buf = Vec::new();
                    ciborium::into_writer(&other, &mut buf).map_err(|e| {
                        StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                    })?;
                    result.extend(buf);
                }
            }
        }
        Ok(result)
    }

    /// Collect a single CBOR value (expects exactly one chunk).
    /// Per-item metadata is discarded.
    pub async fn collect_value(mut self) -> Result<ciborium::Value, StreamError> {
        match self.recv().await {
            Some(Ok((value, _meta))) => Ok(value),
            Some(Err(e)) => Err(e),
            None => Err(StreamError::Closed),
        }
    }
}

/// A single item from a peer response — either decoded data or a LOG frame.
///
/// `PeerResponse::recv()` yields these interleaved in arrival order. Handlers
/// match on each variant to decide how to react (e.g., forward progress, accumulate data).
pub enum PeerResponseItem {
    /// A decoded CBOR data chunk from the peer response, with optional per-chunk metadata.
    Data(Result<ciborium::Value, StreamError>, Option<StreamMeta>),
    /// A LOG frame from the peer (progress, status messages, etc.).
    Log(Frame),
}

/// Response from a peer call — yields both data items and LOG frames from a single receiver.
///
/// The handler drains this with `recv()` and reacts to each `PeerResponseItem` as it arrives.
/// LOG frames are delivered in real-time as they arrive (not buffered until data starts).
/// For callers that don't care about LOG frames, `collect_bytes()` and `collect_value()`
/// silently discard them and return only data.
pub struct PeerResponse {
    rx: tokio::sync::mpsc::UnboundedReceiver<PeerResponseItem>,
}

impl PeerResponse {
    /// Receive the next item (data or LOG) from the peer response.
    /// Returns None when the stream ends.
    pub async fn recv(&mut self) -> Option<PeerResponseItem> {
        self.rx.recv().await
    }

    /// Collect all data chunks into a single byte vector, discarding LOG frames and metadata.
    ///
    /// WARNING: Only call this if you know the stream is finite.
    pub async fn collect_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        let mut result = Vec::new();
        let mut chunk_count = 0u32;
        while let Some(item) = self.recv().await {
            match item {
                PeerResponseItem::Data(Ok(value), _meta) => {
                    chunk_count += 1;
                    match value {
                        ciborium::Value::Bytes(b) => {
                            tracing::info!(
                                "[collect_bytes] chunk {} Bytes({} bytes)",
                                chunk_count,
                                b.len()
                            );
                            result.extend(b);
                        }
                        ciborium::Value::Text(s) => {
                            tracing::info!(
                                "[collect_bytes] chunk {} Text({} bytes)",
                                chunk_count,
                                s.len()
                            );
                            result.extend(s.into_bytes());
                        }
                        other => {
                            let mut buf = Vec::new();
                            ciborium::into_writer(&other, &mut buf).map_err(|e| {
                                StreamError::Decode(format!("Failed to encode CBOR: {}", e))
                            })?;
                            tracing::info!(
                                "[collect_bytes] chunk {} Other({} bytes)",
                                chunk_count,
                                buf.len()
                            );
                            result.extend(buf);
                        }
                    }
                }
                PeerResponseItem::Data(Err(e), _) => return Err(e),
                PeerResponseItem::Log(_) => {} // Discard LOG frames
            }
        }
        tracing::info!(
            "[collect_bytes] DONE: {} chunks, {} total bytes, preview={:?}",
            chunk_count,
            result.len(),
            String::from_utf8_lossy(&result[..result.len().min(100)])
        );
        Ok(result)
    }

    /// Collect a single CBOR data value (expects exactly one data chunk), discarding LOG frames and metadata.
    pub async fn collect_value(mut self) -> Result<ciborium::Value, StreamError> {
        while let Some(item) = self.recv().await {
            match item {
                PeerResponseItem::Data(Ok(value), _meta) => return Ok(value),
                PeerResponseItem::Data(Err(e), _) => return Err(e),
                PeerResponseItem::Log(_) => {} // Discard LOG frames
            }
        }
        Err(StreamError::Closed)
    }
}

/// The bundle of all input arg streams for one request.
/// Yields InputStream objects as STREAM_START frames arrive from the wire.
/// Returns None after END frame (all args delivered).
///
/// This is an async stream. Use `recv()` to get the next stream.
pub struct InputPackage {
    rx: tokio::sync::mpsc::UnboundedReceiver<Result<InputStream, StreamError>>,
    _demux_handle: Option<tokio::task::JoinHandle<()>>,
}

impl InputPackage {
    /// Get the next input stream. Async - waits until STREAM_START or END.
    pub async fn recv(&mut self) -> Option<Result<InputStream, StreamError>> {
        self.rx.recv().await
    }

    /// Collect all streams' bytes into a single Vec<u8>.
    ///
    /// WARNING: Only call this if you know all streams are finite.
    /// Infinite streams will block forever.
    pub async fn collect_all_bytes(mut self) -> Result<Vec<u8>, StreamError> {
        let mut all = Vec::new();
        while let Some(stream_result) = self.recv().await {
            let stream = stream_result?;
            all.extend(stream.collect_bytes().await?);
        }
        Ok(all)
    }

    /// Collect each stream individually into a Vec of (media_urn, bytes) pairs.
    /// Each stream's bytes are accumulated separately — NOT concatenated.
    /// Use `find_stream()` helpers to retrieve args by URN pattern matching.
    ///
    /// WARNING: Only call this if you know all streams are finite.
    pub async fn collect_streams(
        mut self,
    ) -> Result<Vec<(String, Vec<u8>, Option<StreamMeta>)>, StreamError> {
        let mut result = Vec::new();
        while let Some(stream_result) = self.recv().await {
            let stream = stream_result?;
            let urn = stream.media_urn().to_string();
            let meta = stream.stream_meta().cloned();
            let bytes = stream.collect_bytes().await?;
            result.push((urn, bytes, meta));
        }
        Ok(result)
    }
}

/// Find a stream's bytes by exact URN equivalence.
///
/// Uses `MediaUrn::is_equivalent()` — matches only if both URNs have the
/// exact same tag set (order-independent). Both the caller and the cartridge
/// know the arg media URNs from the cap definition, so this is always an
/// exact match — never a subsumption/pattern match.
///
/// The `media_urn` parameter must be the FULL media URN from the cap arg
/// definition (e.g., `"media:model-spec;textable"`).
pub fn find_stream<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<&'a [u8]> {
    let target = match crate::MediaUrn::from_string(media_urn) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, bytes, _meta)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if target.is_equivalent(&urn).unwrap_or(false) {
            Some(bytes.as_slice())
        } else {
            None
        }
    })
}

/// Like `find_stream` but returns a UTF-8 string.
pub fn find_stream_str(
    streams: &[(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<String> {
    find_stream(streams, media_urn).and_then(|b| String::from_utf8(b.to_vec()).ok())
}

/// Find the stream-level metadata (from STREAM_START) for a stream by media URN.
pub fn find_stream_meta<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Option<&'a StreamMeta> {
    let target = match crate::MediaUrn::from_string(media_urn) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, _bytes, meta)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if target.is_equivalent(&urn).unwrap_or(false) {
            meta.as_ref()
        } else {
            None
        }
    })
}

/// Like `find_stream` but fails hard if not found.
pub fn require_stream<'a>(
    streams: &'a [(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Result<&'a [u8], StreamError> {
    find_stream(streams, media_urn)
        .ok_or_else(|| StreamError::Protocol(format!("Missing required arg: {}", media_urn)))
}

/// Like `require_stream` but returns a UTF-8 string.
pub fn require_stream_str(
    streams: &[(String, Vec<u8>, Option<StreamMeta>)],
    media_urn: &str,
) -> Result<String, StreamError> {
    let bytes = require_stream(streams, media_urn)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| StreamError::Decode(format!("Arg '{}' is not valid UTF-8: {}", media_urn, e)))
}

/// Detached progress/log emitter that can be moved into `spawn_blocking`.
///
/// Holds an `Arc<dyn FrameSender>` and the request routing info needed to
/// construct LOG frames. `Send + Sync + 'static` by construction.
#[derive(Clone)]
pub struct ProgressSender {
    sender: Arc<dyn FrameSender>,
    request_id: MessageId,
    routing_id: Option<MessageId>,
}

impl ProgressSender {
    /// Emit a progress update (0.0–1.0) with a human-readable status message.
    pub fn progress(&self, progress: f32, message: &str) {
        let mut frame = Frame::progress(self.request_id.clone(), progress, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a log message.
    pub fn log(&self, level: &str, message: &str) {
        let mut frame = Frame::log(self.request_id.clone(), level, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }
}

/// Detachable handle that can emit CBOR data chunks from any thread
/// (including `spawn_blocking`).  Obtained via [`OutputStream::stream_sender`].
///
/// Like [`ProgressSender`], this is `Send + Sync + 'static` and does not
/// borrow the parent `OutputStream`.
///
/// **Important:** call [`OutputStream::start()`] *before* moving the
/// `StreamSender` into `spawn_blocking` so that the STREAM_START frame is
/// sent while the async context is still available.
pub struct StreamSender {
    sender: Arc<dyn FrameSender>,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    stream_id: String,
    max_chunk: usize,
    /// Shared chunk_index counter (same instance as OutputStream).
    chunk_index: Arc<Mutex<u64>>,
    /// Shared chunk_count counter (same instance as OutputStream).
    chunk_count: Arc<Mutex<u64>>,
}

impl StreamSender {
    /// Emit a single CBOR value as one or more CHUNK frames.
    ///
    /// Bytes and Text values are automatically split at `max_chunk` boundaries.
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        match value {
            ciborium::Value::Bytes(bytes) => {
                let mut offset = 0;
                while offset < bytes.len() {
                    let chunk_size = (bytes.len() - offset).min(self.max_chunk);
                    let chunk_bytes = bytes[offset..offset + chunk_size].to_vec();
                    self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
                    offset += chunk_size;
                }
            }
            _ => {
                self.send_chunk(value)?;
            }
        }
        Ok(())
    }

    fn send_chunk(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(value, &mut cbor_payload)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;

        let chunk_index = {
            let mut guard = self.chunk_index.lock().unwrap();
            let current = *guard;
            *guard += 1;
            current
        };
        {
            let mut guard = self.chunk_count.lock().unwrap();
            *guard += 1;
        }

        let checksum = Frame::compute_checksum(&cbor_payload);
        let mut frame = Frame::chunk(
            self.request_id.clone(),
            self.stream_id.clone(),
            0,
            cbor_payload,
            chunk_index,
            checksum,
        );
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }
}

/// Writable stream handle for handler output or peer call arguments.
/// Manages STREAM_START/CHUNK/STREAM_END framing automatically.
pub struct OutputStream {
    sender: Arc<dyn FrameSender>,
    stream_id: String,
    media_urn: String,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    max_chunk: usize,
    /// None = not started, Some(false) = write mode, Some(true) = sequence mode
    stream_mode: Mutex<Option<bool>>,
    chunk_index: Arc<Mutex<u64>>,
    chunk_count: Arc<Mutex<u64>>,
    closed: AtomicBool,
}

impl OutputStream {
    fn new(
        sender: Arc<dyn FrameSender>,
        stream_id: String,
        media_urn: String,
        request_id: MessageId,
        routing_id: Option<MessageId>,
        max_chunk: usize,
    ) -> Self {
        Self {
            sender,
            stream_id,
            media_urn,
            request_id,
            routing_id,
            max_chunk,
            stream_mode: Mutex::new(None),
            chunk_index: Arc::new(Mutex::new(0)),
            chunk_count: Arc::new(Mutex::new(0)),
            closed: AtomicBool::new(false),
        }
    }

    fn check_mode(&self, is_sequence: bool) -> Result<(), RuntimeError> {
        let mode = self.stream_mode.lock().unwrap();
        match *mode {
            None => Err(RuntimeError::Handler(
                "stream not started: call start() before write/emit_list_item".to_string(),
            )),
            Some(existing) if existing == is_sequence => Ok(()),
            Some(existing) => Err(RuntimeError::Handler(format!(
                "stream mode conflict: started as {} but called with {}",
                if existing { "sequence" } else { "write" },
                if is_sequence { "sequence" } else { "write" },
            ))),
        }
    }

    fn send_chunk(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(value, &mut cbor_payload)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;

        let chunk_index = {
            let mut chunk_index_guard = self.chunk_index.lock().unwrap();
            let current = *chunk_index_guard;
            *chunk_index_guard += 1;
            current
        };
        {
            let mut count_guard = self.chunk_count.lock().unwrap();
            *count_guard += 1;
        }

        let checksum = Frame::compute_checksum(&cbor_payload);
        let mut frame = Frame::chunk(
            self.request_id.clone(),
            self.stream_id.clone(),
            0,
            cbor_payload,
            chunk_index,
            checksum,
        );
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }

    /// Write raw bytes. Splits into max_chunk pieces, each wrapped as CBOR Bytes.
    /// Requires `start(false)` to have been called first.
    pub fn write(&self, data: &[u8]) -> Result<(), RuntimeError> {
        self.check_mode(false)?;
        if data.is_empty() {
            return Ok(());
        }
        let mut offset = 0;
        while offset < data.len() {
            let chunk_size = (data.len() - offset).min(self.max_chunk);
            let chunk_bytes = data[offset..offset + chunk_size].to_vec();
            self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
            offset += chunk_size;
        }
        Ok(())
    }

    /// Emit a single CBOR value as one item in an RFC 8742 CBOR sequence.
    ///
    /// For list outputs: the receiver concatenates raw frame payloads and stores
    /// the result as a CBOR sequence. This method CBOR-encodes the value, then
    /// splits the encoded bytes across chunk frames at `max_chunk` boundaries.
    /// The receiver's concatenation reconstructs the original CBOR encoding,
    /// producing exactly one self-delimiting CBOR value in the sequence per call.
    ///
    /// Unlike `emit_cbor` (which re-wraps each piece as a separate CBOR value),
    /// this sends raw CBOR bytes as frame payloads directly.
    ///
    /// `meta` is per-item metadata, placed on the first chunk frame of this item only.
    pub fn emit_list_item(
        &self,
        value: &ciborium::Value,
        meta: Option<StreamMeta>,
    ) -> Result<(), RuntimeError> {
        self.check_mode(true)?;
        let mut cbor_bytes = Vec::new();
        ciborium::into_writer(value, &mut cbor_bytes)
            .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR: {}", e)))?;

        let mut offset = 0;
        let mut first_chunk = true;
        while offset < cbor_bytes.len() {
            let chunk_size = (cbor_bytes.len() - offset).min(self.max_chunk);
            let chunk_payload = cbor_bytes[offset..offset + chunk_size].to_vec();

            let chunk_index = {
                let mut guard = self.chunk_index.lock().unwrap();
                let current = *guard;
                *guard += 1;
                current
            };
            {
                let mut guard = self.chunk_count.lock().unwrap();
                *guard += 1;
            }

            let checksum = Frame::compute_checksum(&chunk_payload);
            let mut frame = Frame::chunk(
                self.request_id.clone(),
                self.stream_id.clone(),
                0,
                chunk_payload,
                chunk_index,
                checksum,
            );
            frame.routing_id = self.routing_id.clone();
            // Per-item meta goes on the first chunk frame only
            if first_chunk {
                frame.meta = meta.clone();
                first_chunk = false;
            }
            self.sender.send(&frame)?;
            offset += chunk_size;
        }
        Ok(())
    }

    /// Emit a CBOR value. Handles Bytes/Text/Array/Map chunking.
    /// Uses write mode (is_sequence=false) — each chunk is a complete CBOR value.
    /// Requires `start(false)` to have been called first.
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        self.check_mode(false)?;
        match value {
            ciborium::Value::Bytes(bytes) => {
                let mut offset = 0;
                while offset < bytes.len() {
                    let chunk_size = (bytes.len() - offset).min(self.max_chunk);
                    let chunk_bytes = bytes[offset..offset + chunk_size].to_vec();
                    self.send_chunk(&ciborium::Value::Bytes(chunk_bytes))?;
                    offset += chunk_size;
                }
            }
            ciborium::Value::Text(text) => {
                let text_bytes = text.as_bytes();
                let mut offset = 0;
                while offset < text_bytes.len() {
                    let mut chunk_size = (text_bytes.len() - offset).min(self.max_chunk);
                    while chunk_size > 0 && !text.is_char_boundary(offset + chunk_size) {
                        chunk_size -= 1;
                    }
                    if chunk_size == 0 {
                        return Err(RuntimeError::Handler(
                            "Cannot split text on character boundary".to_string(),
                        ));
                    }
                    let chunk_text = text[offset..offset + chunk_size].to_string();
                    self.send_chunk(&ciborium::Value::Text(chunk_text))?;
                    offset += chunk_size;
                }
            }
            ciborium::Value::Array(elements) => {
                for element in elements {
                    self.send_chunk(element)?;
                }
            }
            ciborium::Value::Map(entries) => {
                for (key, val) in entries {
                    let entry = ciborium::Value::Array(vec![key.clone(), val.clone()]);
                    self.send_chunk(&entry)?;
                }
            }
            _ => {
                self.send_chunk(value)?;
            }
        }
        Ok(())
    }

    /// Emit a log message.
    pub fn log(&self, level: &str, message: &str) {
        let mut frame = Frame::log(self.request_id.clone(), level, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Emit a progress update (0.0–1.0) with a human-readable status message.
    pub fn progress(&self, progress: f32, message: &str) {
        let mut frame = Frame::progress(self.request_id.clone(), progress, message);
        frame.routing_id = self.routing_id.clone();
        let _ = self.sender.send(&frame);
    }

    /// Create a detached progress sender that can be moved into `spawn_blocking`.
    ///
    /// The returned `ProgressSender` is `Send + Sync + 'static` and can emit
    /// progress and log frames from any thread without holding a reference to
    /// this `OutputStream`. Use this when blocking work (FFI model loads, inference)
    /// needs to emit per-token or keepalive progress from a dedicated thread.
    pub fn progress_sender(&self) -> ProgressSender {
        ProgressSender {
            sender: Arc::clone(&self.sender),
            request_id: self.request_id.clone(),
            routing_id: self.routing_id.clone(),
        }
    }

    /// Create a detached stream sender that can emit CBOR data chunks from any
    /// thread (including `spawn_blocking`).
    ///
    /// Shares chunk counters with this `OutputStream` so that `close()` reports
    /// the correct total chunk count.
    ///
    /// **Call `start()` before creating the `StreamSender`** so that
    /// STREAM_START is sent while the async context is still active.
    pub fn stream_sender(&self) -> StreamSender {
        StreamSender {
            sender: Arc::clone(&self.sender),
            request_id: self.request_id.clone(),
            routing_id: self.routing_id.clone(),
            stream_id: self.stream_id.clone(),
            max_chunk: self.max_chunk,
            chunk_index: Arc::clone(&self.chunk_index),
            chunk_count: Arc::clone(&self.chunk_count),
        }
    }

    /// Send STREAM_START with the given mode. Must be called exactly once
    /// before any write/emit_list_item/emit_cbor calls.
    ///
    /// * `is_sequence = false` — write mode: each chunk is a complete CBOR value.
    ///   `meta` is placed on the STREAM_START frame (whole-stream metadata).
    /// * `is_sequence = true`  — sequence mode: chunks are CBOR fragments (RFC 8742).
    ///   `meta` is placed on the STREAM_START frame. Per-item metadata goes via `emit_list_item`.
    pub fn start(&self, is_sequence: bool, meta: Option<StreamMeta>) -> Result<(), RuntimeError> {
        let mut mode = self.stream_mode.lock().unwrap();
        if mode.is_some() {
            return Err(RuntimeError::Handler("stream already started".to_string()));
        }
        *mode = Some(is_sequence);
        drop(mode);
        let mut start_frame = Frame::stream_start(
            self.request_id.clone(),
            self.stream_id.clone(),
            self.media_urn.clone(),
            Some(is_sequence),
        );
        start_frame.routing_id = self.routing_id.clone();
        start_frame.meta = meta;
        self.sender.send(&start_frame)
    }

    /// Run a blocking closure on a dedicated thread while emitting keepalive progress
    /// frames every 30 seconds.
    ///
    /// Model loading (GGUF, Candle, etc.) is synchronous FFI that can take minutes
    /// for large models. The engine's 120s activity timeout kills the task if no
    /// frames arrive.
    ///
    /// The closure runs on `tokio::task::spawn_blocking` (dedicated thread pool),
    /// freeing the tokio worker thread so the frame writer task can flush keepalive
    /// frames to stdout. Without this, blocking on the tokio worker prevents the
    /// writer task from running, and frames queue up but never reach the engine.
    ///
    /// Keepalive frames are emitted every 30s via `tokio::time::interval` on the
    /// async runtime.
    pub async fn run_with_keepalive<T: Send + 'static>(
        &self,
        progress: f32,
        message: &str,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let sender = Arc::clone(&self.sender);
        let request_id = self.request_id.clone();
        let routing_id = self.routing_id.clone();
        let msg = message.to_string();

        // Spawn the blocking work on the dedicated blocking thread pool
        let mut join_handle = tokio::task::spawn_blocking(f);

        // Emit keepalive frames every 30s while blocking work runs
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.tick().await; // first tick is immediate — skip it

        loop {
            tokio::select! {
                biased;
                result = &mut join_handle => {
                    // Blocking work completed — return its result
                    return result.expect("spawn_blocking task panicked");
                }
                _ = interval.tick() => {
                    // Emit keepalive progress frame
                    let mut frame = Frame::progress(request_id.clone(), progress, &msg);
                    frame.routing_id = routing_id.clone();
                    let _ = sender.send(&frame);
                }
            }
        }
    }

    /// Close the output stream (sends STREAM_END). Idempotent.
    /// If `start()` was never called, this is a no-op (no STREAM_START was sent,
    /// so no STREAM_END is needed — the handler produced no output).
    pub fn close(&self) -> Result<(), RuntimeError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already closed
        }
        {
            let mode = self.stream_mode.lock().unwrap();
            if mode.is_none() {
                return Ok(()); // Never started — no output produced, nothing to close
            }
        }
        let chunk_count = {
            let count_guard = self.chunk_count.lock().unwrap();
            *count_guard
        };
        let mut frame =
            Frame::stream_end(self.request_id.clone(), self.stream_id.clone(), chunk_count);
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }
}

/// Handle for an in-progress peer invocation.
/// Handler creates arg streams with `arg()`, writes data, then calls `finish()`
/// to get a `PeerResponse` that yields both data and LOG frames.
pub struct PeerCall {
    pub(crate) sender: Arc<dyn FrameSender>,
    pub(crate) request_id: MessageId,
    pub(crate) max_chunk: usize,
    pub(crate) response_rx: Option<tokio::sync::mpsc::UnboundedReceiver<Frame>>,
}

impl PeerCall {
    /// Create a new arg OutputStream for this peer call.
    /// Each arg is an independent stream (own stream_id, no routing_id).
    pub fn arg(&self, media_urn: &str) -> OutputStream {
        let stream_id = uuid::Uuid::new_v4().to_string();
        OutputStream::new(
            Arc::clone(&self.sender),
            stream_id,
            media_urn.to_string(),
            self.request_id.clone(),
            None, // No routing_id for peer requests
            self.max_chunk,
        )
    }

    /// Finish sending args and get the peer response.
    /// Sends END for the peer request, spawns Demux on response channel.
    ///
    /// Returns a `PeerResponse` that yields `PeerResponseItem::Data` and
    /// `PeerResponseItem::Log` interleaved in arrival order. The handler
    /// decides how to react to each (e.g., forward progress, accumulate data).
    pub async fn finish(mut self) -> Result<PeerResponse, RuntimeError> {
        // Send END frame for the peer request
        tracing::info!(
            "[PeerCall] finish: sending END for peer_rid={:?}",
            self.request_id
        );
        let end_frame = Frame::end(self.request_id.clone(), None);
        self.sender.send(&end_frame)?;

        // Take the response receiver
        let response_rx = self
            .response_rx
            .take()
            .ok_or_else(|| RuntimeError::PeerRequest("PeerCall already finished".to_string()))?;

        // Start demux — returns immediately so LOG frames can be consumed
        // before data arrives (critical for keeping activity timer alive)
        let peer_response = demux_single_stream(response_rx);
        tracing::info!(
            "[PeerCall] finish: demux started for peer_rid={:?}",
            self.request_id
        );

        Ok(peer_response)
    }
}

/// Allows handlers to invoke caps on the peer (host).
///
/// This trait enables bidirectional communication where a cartridge handler can
/// invoke caps on the host while processing a request.
///
/// The `call` method starts a peer invocation and returns a `PeerCall`.
/// The handler creates arg streams with `call.arg()`, writes data, then
/// calls `call.finish()` to get a `PeerResponse` with data + LOG frames.
#[async_trait]
pub trait PeerInvoker: Send + Sync {
    /// Start a peer call. Sends REQ, registers response channel.
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError>;

    /// Convenience: open call, write each arg's bytes, finish, return response.
    ///
    /// Returns a `PeerResponse` — use `collect_bytes()` / `collect_value()` to
    /// discard LOG frames, or `recv()` to process them alongside data.
    async fn call_with_bytes(
        &self,
        cap_urn: &str,
        args: &[(&str, &[u8])],
    ) -> Result<PeerResponse, RuntimeError> {
        self.call_with_bytes_and_meta(cap_urn, args, None).await
    }

    /// Like `call_with_bytes`, but sets stream metadata on each arg's STREAM_START.
    ///
    /// The meta carries provenance context (e.g. {"title": "page_3"}) through
    /// peer calls so the receiving cap can propagate it to its output.
    async fn call_with_bytes_and_meta(
        &self,
        cap_urn: &str,
        args: &[(&str, &[u8])],
        meta: Option<&crate::StreamMeta>,
    ) -> Result<PeerResponse, RuntimeError> {
        let call = self.call(cap_urn)?;
        for &(media_urn, data) in args {
            let arg = call.arg(media_urn);
            arg.start(false, meta.cloned())?;
            arg.write(data)?;
            arg.close()?;
        }
        call.finish().await
    }
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported (e.g., CLI mode).
pub struct NoPeerInvoker;

#[async_trait]
impl PeerInvoker for NoPeerInvoker {
    fn call(&self, _cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        Err(RuntimeError::PeerRequest(
            "Peer invocation not supported in this context".to_string(),
        ))
    }
}

/// Channel-based frame sender for cartridge output.
/// ALL frames (peer requests AND responses) go through a single output channel.
/// CartridgeRuntime has a writer task that drains this channel and writes to stdout.
pub(crate) struct ChannelFrameSender {
    pub(crate) tx: tokio::sync::mpsc::UnboundedSender<Frame>,
}

impl FrameSender for ChannelFrameSender {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        // UnboundedSender::send is sync-compatible (no .await needed)
        self.tx
            .send(frame.clone())
            .map_err(|_| RuntimeError::Handler("Output channel closed".to_string()))
    }
}

/// CLI-mode emitter that writes directly to stdout.
/// Used when the cartridge is invoked via CLI (with arguments).
pub struct CliStreamEmitter {
    /// Whether to add newlines after each emit (NDJSON style)
    ndjson: bool,
}

impl CliStreamEmitter {
    /// Create a new CLI emitter with NDJSON formatting (newline after each emit)
    pub fn new() -> Self {
        Self { ndjson: true }
    }

    /// Create a CLI emitter without NDJSON formatting
    pub fn without_ndjson() -> Self {
        Self { ndjson: false }
    }
}

impl Default for CliStreamEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliStreamEmitter {
    /// Emit a CBOR value to stdout (CLI mode)
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();

        // In CLI mode: extract raw bytes/text from CBOR and emit to stdout
        // Supported types: Bytes, Text, Array (of Bytes/Text), Map (extract "value" field)
        // NO FALLBACK - fail hard if unsupported type

        match value {
            ciborium::Value::Array(arr) => {
                // Array - emit each element's raw content
                for item in arr {
                    match item {
                        ciborium::Value::Bytes(bytes) => {
                            let _ = handle.write_all(bytes);
                        }
                        ciborium::Value::Text(text) => {
                            let _ = handle.write_all(text.as_bytes());
                        }
                        ciborium::Value::Map(map) => {
                            // Map - extract "value" field (for argument structures)
                            if let Some(val) = map
                                .iter()
                                .find(
                                    |(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"),
                                )
                                .map(|(_, v)| v)
                            {
                                match val {
                                    ciborium::Value::Bytes(bytes) => {
                                        let _ = handle.write_all(bytes);
                                    }
                                    ciborium::Value::Text(text) => {
                                        let _ = handle.write_all(text.as_bytes());
                                    }
                                    _ => {
                                        return Err(RuntimeError::Handler(
                                            "Map 'value' field is not bytes/text".to_string(),
                                        ))
                                    }
                                }
                            } else {
                                return Err(RuntimeError::Handler(
                                    "Map in array has no 'value' field".to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(RuntimeError::Handler(
                                "Array contains unsupported element type".to_string(),
                            ));
                        }
                    }
                }
            }
            ciborium::Value::Bytes(bytes) => {
                // Simple bytes - emit raw
                let _ = handle.write_all(bytes);
            }
            ciborium::Value::Text(text) => {
                // Simple text - emit as UTF-8
                let _ = handle.write_all(text.as_bytes());
            }
            ciborium::Value::Map(map) => {
                // Single map - extract "value" field
                if let Some(val) = map
                    .iter()
                    .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
                    .map(|(_, v)| v)
                {
                    match val {
                        ciborium::Value::Bytes(bytes) => {
                            let _ = handle.write_all(bytes);
                        }
                        ciborium::Value::Text(text) => {
                            let _ = handle.write_all(text.as_bytes());
                        }
                        _ => {
                            return Err(RuntimeError::Handler(
                                "Map 'value' field is not bytes/text".to_string(),
                            ))
                        }
                    }
                } else {
                    return Err(RuntimeError::Handler(
                        "Map has no 'value' field".to_string(),
                    ));
                }
            }
            _ => {
                return Err(RuntimeError::Handler(
                    "Handler emitted unsupported CBOR type".to_string(),
                ));
            }
        }

        if self.ndjson {
            let _ = handle.write_all(b"\n");
        }
        let _ = handle.flush();
        Ok(())
    }

    fn emit_log(&self, level: &str, message: &str) {
        // In CLI mode, logs go to stderr
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        let _ = writeln!(handle, "[{}] {}", level, message);
    }
}

/// CLI-mode frame sender that extracts payloads from frames and outputs to stdout.
/// Adapts FrameSender trait for CLI mode using CliStreamEmitter.
pub struct CliFrameSender {
    emitter: CliStreamEmitter,
}

impl CliFrameSender {
    pub fn new() -> Self {
        Self {
            emitter: CliStreamEmitter::new(),
        }
    }

    pub fn with_emitter(emitter: CliStreamEmitter) -> Self {
        Self { emitter }
    }
}

impl FrameSender for CliFrameSender {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        match frame.frame_type {
            FrameType::Chunk => {
                // Extract CBOR payload from CHUNK frame and emit to stdout
                if let Some(ref payload) = frame.payload {
                    // Verify checksum (protocol v2 integrity check)
                    let expected_checksum = Frame::compute_checksum(payload);
                    let actual_checksum = frame.checksum.ok_or_else(|| {
                        RuntimeError::Protocol("CHUNK frame missing checksum field".to_string())
                    })?;
                    if expected_checksum != actual_checksum {
                        return Err(RuntimeError::CorruptedData(format!(
                            "CHUNK checksum mismatch: expected {}, got {} (payload {} bytes)",
                            expected_checksum,
                            actual_checksum,
                            payload.len()
                        )));
                    }

                    // Decode CBOR payload
                    let value: ciborium::Value =
                        ciborium::from_reader(&payload[..]).map_err(|e| {
                            RuntimeError::Handler(format!("Failed to decode CBOR payload: {}", e))
                        })?;

                    // Emit to stdout via CliStreamEmitter
                    self.emitter.emit_cbor(&value)?;
                }
                Ok(())
            }
            FrameType::Log => {
                // Extract log message and emit to stderr
                let level = frame.log_level().unwrap_or("INFO");
                let message = frame.log_message().unwrap_or("");
                self.emitter.emit_log(level, message);
                Ok(())
            }
            FrameType::StreamStart | FrameType::StreamEnd | FrameType::End => {
                // Ignore framing messages in CLI mode
                Ok(())
            }
            FrameType::Err => {
                // Output error to stderr
                let code = frame.error_code().unwrap_or("ERROR");
                let msg = frame.error_message().unwrap_or("Unknown error");
                Err(RuntimeError::Handler(format!("[{}] {}", code, msg)))
            }
            _ => {
                // Fail hard on unexpected frame types
                Err(RuntimeError::Handler(format!(
                    "Unexpected frame type in CLI mode: {:?}",
                    frame.frame_type
                )))
            }
        }
    }
}

// =============================================================================
// OP-BASED HANDLER SYSTEM — handlers implement ops::Op<()>
// =============================================================================

/// Bundles capdag I/O for WetContext. Op handlers extract this from WetContext
/// to access streaming input, output, and peer invocation.
pub struct Request {
    input: Mutex<Option<InputPackage>>,
    output: Arc<OutputStream>,
    peer: Arc<dyn PeerInvoker>,
}

impl Request {
    /// Create a new Request bundling input, output, and peer invoker.
    pub fn new(input: InputPackage, output: OutputStream, peer: Arc<dyn PeerInvoker>) -> Self {
        Self {
            input: Mutex::new(Some(input)),
            output: Arc::new(output),
            peer,
        }
    }

    /// Take the input package. Can only be called once — second call returns error.
    pub fn take_input(&self) -> Result<InputPackage, RuntimeError> {
        self.input
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| RuntimeError::Handler("Input already consumed".to_string()))
    }

    /// Access the output stream.
    pub fn output(&self) -> &OutputStream {
        &self.output
    }

    /// Access the peer invoker.
    pub fn peer(&self) -> &dyn PeerInvoker {
        &*self.peer
    }
}

/// WetContext key for the Request object.
pub const WET_KEY_REQUEST: &str = "request";

/// Factory function that creates a fresh Op<()> instance per invocation.
pub type OpFactory = Arc<dyn Fn() -> Box<dyn Op<()>> + Send + Sync>;

/// Standard identity handler — pure passthrough. Forwards all input chunks to output.
#[derive(Default)]
pub struct IdentityOp;

#[async_trait]
impl Op<()> for IdentityOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut started = false;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Identity input error: {}", e)))?;
            // Start output with the first input stream's meta (propagates provenance context)
            if !started {
                req.output()
                    .start(false, stream.stream_meta().cloned())
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                started = true;
            }
            while let Some(chunk_result) = stream.recv_data().await {
                let chunk = chunk_result.map_err(|e| {
                    OpError::ExecutionFailed(format!("Identity chunk error: {}", e))
                })?;
                req.output()
                    .emit_cbor(&chunk)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            }
        }
        // If no input streams arrived, still need to start and close the output
        if !started {
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        }
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("IdentityOp")
            .description("Pure passthrough — forwards all input to output")
            .build()
    }
}

/// Standard discard handler — terminal morphism. Drains all input, produces nothing.
#[derive(Default)]
pub struct DiscardOp;

#[async_trait]
impl Op<()> for DiscardOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Discard input error: {}", e)))?;
            while let Some(chunk_result) = stream.recv_data().await {
                let _ = chunk_result
                    .map_err(|e| OpError::ExecutionFailed(format!("Discard chunk error: {}", e)))?;
            }
        }
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("DiscardOp")
            .description("Terminal morphism — drains all input, produces nothing")
            .build()
    }
}

/// Default adapter selection handler — returns empty END (no match).
///
/// This is the standard default for cartridges that do not inspect file content.
/// Cartridges that provide content inspection override this by registering their
/// own handler for `CAP_ADAPTER_SELECTION`.
///
/// The empty END frame (exit code 0, no stream output) is the ONLY valid "no match"
/// response. The orchestrator treats any stream output that isn't valid
/// `{"media_urns": [...]}` as a runtime error.
#[derive(Default)]
pub struct AdapterSelectionOp;

#[async_trait]
impl Op<()> for AdapterSelectionOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req: Arc<Request> = wet
            .get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let mut input = req
            .take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        // Drain all input — we don't inspect it in the default handler
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result.map_err(|e| {
                OpError::ExecutionFailed(format!("AdapterSelection input error: {}", e))
            })?;
            while let Some(chunk_result) = stream.recv_data().await {
                let _ = chunk_result.map_err(|e| {
                    OpError::ExecutionFailed(format!("AdapterSelection chunk error: {}", e))
                })?;
            }
        }
        // Return Ok(()) without starting output — produces empty END frame
        Ok(())
    }

    fn metadata(&self) -> OpMetadata {
        OpMetadata::builder("AdapterSelectionOp")
            .description("Default adapter selection — returns empty END (no match)")
            .build()
    }
}

/// Tracks a pending peer request (cartridge invoking host cap).
/// The reader loop forwards response frames to the channel.
/// LOG frames are re-stamped with the origin request ID and forwarded
/// back to the host automatically (no handler involvement).
struct PendingPeerRequest {
    sender: tokio::sync::mpsc::UnboundedSender<Frame>,
    origin_request_id: MessageId,
    origin_routing_id: Option<MessageId>,
}

/// Implementation of PeerInvoker that sends REQ frames to the host.
struct PeerInvokerImpl {
    output_tx: tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    max_chunk: usize,
    origin_request_id: MessageId,
    origin_routing_id: Option<MessageId>,
}

/// Extract the effective payload from a CBOR arguments payload.
///
/// Handles file-path auto-conversion for BOTH CLI and CBOR modes:
/// 1. Detects media:file-path arguments
/// 2. Reads file(s) from filesystem
/// 3. Replaces with file bytes and correct media_urn (from arg's stdin source)
/// 4. Validates at least one arg matches in_spec (unless void)
///
/// For non-CBOR content types, returns raw payload as-is.
///
/// `is_cli_mode`: true if CLI mode (args from command line), false if CBOR mode (cartridge protocol)
fn extract_effective_payload(
    payload: &[u8],
    content_type: Option<&str>,
    cap: &Cap,
    is_cli_mode: bool,
) -> Result<Vec<u8>, RuntimeError> {
    // Check if this is CBOR arguments
    if content_type != Some("application/cbor") {
        // Not CBOR arguments - return raw payload
        return Ok(payload.to_vec());
    }

    // Parse cap URN to get expected input media URN
    let cap_urn = CapUrn::from_string(&cap.urn_string())
        .map_err(|e| RuntimeError::CapUrn(format!("Invalid cap URN: {}", e)))?;
    let expected_input = cap_urn.in_spec().to_string();
    let expected_media_urn = MediaUrn::from_string(&expected_input).ok();

    // Build an arg-definition lookup: parsed MediaUrn → (stdin target URN,
    // is_sequence flag). File-path conversion consults this to decide whether
    // to emit a single file's bytes or a sequence of files, and what URN to
    // relabel the stream with so downstream handlers see the target media
    // type rather than the raw `media:file-path` input.
    struct ArgDefInfo {
        stdin_target: Option<String>,
        is_sequence: bool,
    }
    let arg_defs: Vec<(MediaUrn, ArgDefInfo)> = cap
        .get_args()
        .iter()
        .filter_map(|a| {
            let parsed = MediaUrn::from_string(&a.media_urn).ok()?;
            let stdin_target = a.sources.iter().find_map(|s| match s {
                ArgSource::Stdin { stdin } => Some(stdin.clone()),
                _ => None,
            });
            Some((
                parsed,
                ArgDefInfo {
                    stdin_target,
                    is_sequence: a.is_sequence,
                },
            ))
        })
        .collect();

    // Parse the CBOR payload as an array of argument maps
    let cbor_value: ciborium::Value = ciborium::from_reader(payload)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e)))?;

    let mut arguments = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR arguments must be an array".to_string(),
            ));
        }
    };

    // File-path auto-conversion.
    //
    // When an arg's media URN is a specialization of `media:file-path`, the
    // incoming value is treated as one or more filesystem paths (literal or
    // glob) that the runtime reads and turns into file-bytes.
    //
    // Cardinality is driven exclusively by the arg definition's `is_sequence`
    // flag — URN tags carry semantic shape only.
    //
    // - `is_sequence = true`  → emit a CBOR `Array` of file bytes, regardless
    //   of whether the incoming value was a single path or a list.
    // - `is_sequence = false` → expand to exactly one file and emit a single
    //   CBOR `Bytes`. More than one resolved file is a configuration error
    //   at this layer — CLI-mode dispatch is responsible for iterating the
    //   handler when it detects a glob-to-many against a scalar arg.
    let file_path_base = MediaUrn::from_string("media:file-path")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path base pattern: {}", e)))?;

    for arg in arguments.iter_mut() {
        let ciborium::Value::Map(ref mut arg_map) = arg else {
            continue;
        };

        let mut urn_str: Option<String> = None;
        let mut value_snapshot: Option<ciborium::Value> = None;
        for (k, v) in arg_map.iter() {
            if let ciborium::Value::Text(key) = k {
                match key.as_str() {
                    "media_urn" => {
                        if let ciborium::Value::Text(s) = v {
                            urn_str = Some(s.clone());
                        }
                    }
                    "value" => value_snapshot = Some(v.clone()),
                    _ => {}
                }
            }
        }

        let (Some(urn_str), Some(value)) = (urn_str, value_snapshot) else {
            continue;
        };

        let arg_urn = MediaUrn::from_string(&urn_str).map_err(|e| {
            RuntimeError::Handler(format!(
                "Invalid argument media URN '{}': {}",
                urn_str, e
            ))
        })?;

        if !file_path_base
            .accepts(&arg_urn)
            .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?
        {
            continue;
        }

        // Look up the cap's arg definition by URN equivalence (NOT string
        // compare) — the arg we received may carry the same tags in a
        // different textual order.
        let arg_def = arg_defs.iter().find_map(|(parsed, info)| {
            if parsed.is_equivalent(&arg_urn).unwrap_or(false) {
                Some(info)
            } else {
                None
            }
        });

        let Some(arg_def) = arg_def else {
            // File-path arg with no matching definition: leave it alone.
            continue;
        };

        // Args without a stdin source pass the path bytes through verbatim
        // — the handler reads them itself (rare but legal).
        let Some(ref stdin_target) = arg_def.stdin_target else {
            continue;
        };

        let paths = expand_file_path_value(&value, &urn_str, is_cli_mode)?;

        if !arg_def.is_sequence {
            if paths.len() != 1 {
                return Err(RuntimeError::Handler(format!(
                    "File-path arg '{}' declared is_sequence=false resolved to {} files; \
                     expected exactly 1. CLI-mode dispatch should have iterated the \
                     handler across the expanded files before calling the runtime.",
                    urn_str,
                    paths.len()
                )));
            }
            let bytes = std::fs::read(&paths[0]).map_err(|e| {
                RuntimeError::Handler(format!(
                    "Failed to read file '{}': {}",
                    paths[0].display(),
                    e
                ))
            })?;
            replace_arg_value(
                arg_map,
                ciborium::Value::Bytes(bytes),
                stdin_target.clone(),
            );
        } else {
            let mut items: Vec<ciborium::Value> = Vec::with_capacity(paths.len());
            for p in &paths {
                let bytes = std::fs::read(p).map_err(|e| {
                    RuntimeError::Handler(format!(
                        "Failed to read file '{}': {}",
                        p.display(),
                        e
                    ))
                })?;
                items.push(ciborium::Value::Bytes(bytes));
            }
            replace_arg_value(
                arg_map,
                ciborium::Value::Array(items),
                stdin_target.clone(),
            );
        }
    }

    // Validate: at least ONE argument must match the cap's declared in=spec,
    // unless the cap takes no input (in=media:void). After file-path
    // auto-conversion, an arg's media_urn may have been relabeled to the
    // arg-def's stdin-source target rather than the original
    // `media:file-path;...`, so we also accept any stdin-source target URN
    // as a valid match.
    let void_urn = MediaUrn::from_string("media:void")
        .map_err(|e| RuntimeError::Handler(format!("Invalid void URN literal: {}", e)))?;
    let is_void_input = expected_media_urn
        .as_ref()
        .and_then(|expected| expected.is_equivalent(&void_urn).ok())
        .unwrap_or(false);

    if !is_void_input {
        // Collect all valid target URNs: in_spec + every arg-def's stdin
        // source target.
        let mut valid_targets: Vec<MediaUrn> = Vec::new();
        if let Some(ref expected) = expected_media_urn {
            valid_targets.push(expected.clone());
        }
        for (_, info) in &arg_defs {
            if let Some(ref stdin_urn_str) = info.stdin_target {
                if let Ok(stdin_urn) = MediaUrn::from_string(stdin_urn_str) {
                    valid_targets.push(stdin_urn);
                }
            }
        }

        let mut found_matching_arg = false;
        for arg in &arguments {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let (ciborium::Value::Text(key), ciborium::Value::Text(urn_str)) = (k, v) {
                        if key == "media_urn" {
                            if let Ok(arg_urn) = MediaUrn::from_string(urn_str) {
                                for target in &valid_targets {
                                    // Use is_comparable for discovery: are they on the same chain?
                                    if arg_urn.is_comparable(target).unwrap_or(false) {
                                        found_matching_arg = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if found_matching_arg {
                        break;
                    }
                }
                if found_matching_arg {
                    break;
                }
            }
        }

        if !found_matching_arg {
            return Err(RuntimeError::Deserialize(format!(
                "No argument found matching expected input media type '{}' in CBOR arguments",
                expected_input
            )));
        }
    }

    // After file-path conversion and validation, return the full CBOR array
    // Handler will parse it and extract arguments by matching against in_spec
    let modified_cbor = ciborium::Value::Array(arguments);
    let mut serialized = Vec::new();
    ciborium::into_writer(&modified_cbor, &mut serialized).map_err(|e| {
        RuntimeError::Serialize(format!("Failed to serialize modified CBOR: {}", e))
    })?;

    Ok(serialized)
}

/// Compute the per-iteration CBOR argument payloads for a CLI invocation.
///
/// The input is the raw payload produced by `build_payload_from_cli` — a
/// CBOR array of `{media_urn, value}` maps where file-path values are still
/// raw path or glob strings.
///
/// Rules:
/// - An arg whose media URN specializes `media:file-path` is **iterable**
///   iff its arg-definition declares `is_sequence = false` **and** its raw
///   value expands to more than one concrete file.
/// - Zero iterable args → return the payload unchanged (single iteration).
/// - One iterable arg → return one payload per expanded file, each with the
///   iterable arg's value replaced by that single path as a `Text` value.
///   `extract_effective_payload` then reads the single file and emits bytes.
/// - Two or more iterable args → hard error: the ForEach axis is ambiguous
///   and there is no user-specified policy for a cartesian product.
fn build_cli_foreach_iterations(
    raw_payload: &[u8],
    cap: &Cap,
) -> Result<Vec<Vec<u8>>, RuntimeError> {
    let file_path_base = MediaUrn::from_string("media:file-path").map_err(|e| {
        RuntimeError::Handler(format!("Invalid file-path base pattern: {}", e))
    })?;

    let cbor_value: ciborium::Value = ciborium::from_reader(raw_payload)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e)))?;
    let arguments = match cbor_value {
        ciborium::Value::Array(ref arr) => arr.clone(),
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR arguments must be an array".to_string(),
            ))
        }
    };

    // Build arg-def map for is_sequence lookup via URN equivalence.
    let arg_defs: Vec<(MediaUrn, bool)> = cap
        .get_args()
        .iter()
        .filter_map(|a| {
            MediaUrn::from_string(&a.media_urn)
                .ok()
                .map(|u| (u, a.is_sequence))
        })
        .collect();

    let mut iterable: Option<(usize, Vec<std::path::PathBuf>)> = None;
    for (idx, arg) in arguments.iter().enumerate() {
        let ciborium::Value::Map(arg_map) = arg else {
            continue;
        };
        let mut urn_str: Option<String> = None;
        let mut value: Option<ciborium::Value> = None;
        for (k, v) in arg_map {
            if let ciborium::Value::Text(key) = k {
                match key.as_str() {
                    "media_urn" => {
                        if let ciborium::Value::Text(s) = v {
                            urn_str = Some(s.clone());
                        }
                    }
                    "value" => value = Some(v.clone()),
                    _ => {}
                }
            }
        }
        let (Some(urn_str), Some(value)) = (urn_str, value) else {
            continue;
        };
        let arg_urn = MediaUrn::from_string(&urn_str).map_err(|e| {
            RuntimeError::Handler(format!("Invalid argument media URN '{}': {}", urn_str, e))
        })?;
        if !file_path_base
            .accepts(&arg_urn)
            .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?
        {
            continue;
        }

        let is_sequence_arg = arg_defs
            .iter()
            .find(|(p, _)| p.is_equivalent(&arg_urn).unwrap_or(false))
            .map(|(_, s)| *s)
            .unwrap_or(false);

        if is_sequence_arg {
            // Sequence args take multiple files as-is; no ForEach iteration.
            continue;
        }

        let paths = expand_file_path_value(&value, &urn_str, true)?;
        if paths.len() <= 1 {
            continue;
        }

        if iterable.is_some() {
            return Err(RuntimeError::Handler(
                "Multiple file-path arguments with is_sequence=false each resolved \
                 to more than one file; the ForEach axis is ambiguous. Declare at \
                 most one such arg as scalar, or mark additional args as \
                 is_sequence=true."
                    .to_string(),
            ));
        }
        iterable = Some((idx, paths));
    }

    let Some((idx, paths)) = iterable else {
        return Ok(vec![raw_payload.to_vec()]);
    };

    // Build N per-iteration payloads: clone the CBOR array, replace the
    // iterable arg's value at index `idx` with a single-path Text value.
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        let mut args_for_iter = arguments.clone();
        if let ciborium::Value::Map(ref mut arg_map) = args_for_iter[idx] {
            for (k, v) in arg_map.iter_mut() {
                if let ciborium::Value::Text(key) = k {
                    if key == "value" {
                        *v = ciborium::Value::Text(path.to_string_lossy().into_owned());
                    }
                }
            }
        }
        let wrapped = ciborium::Value::Array(args_for_iter);
        let mut buf = Vec::new();
        ciborium::into_writer(&wrapped, &mut buf).map_err(|e| {
            RuntimeError::Serialize(format!("Failed to re-encode iter payload: {}", e))
        })?;
        out.push(buf);
    }

    Ok(out)
}

/// Expand a file-path arg value into a concrete list of filesystem paths.
///
/// The incoming value may be:
/// - `Bytes` or `Text` containing a single path or a single glob pattern
/// - `Array` of `Bytes`/`Text` items, each a path or a glob (CBOR mode only)
///
/// Globs (detected via `*`, `?`, or `[`) are expanded and the results filtered
/// to regular files. Literal paths must exist and point at a regular file.
/// Returns at least one path on success; empty matches fail hard so the
/// caller never has to guard against a silently-empty list.
fn expand_file_path_value(
    value: &ciborium::Value,
    urn_str: &str,
    is_cli_mode: bool,
) -> Result<Vec<std::path::PathBuf>, RuntimeError> {
    let raw_paths: Vec<String> = match value {
        ciborium::Value::Bytes(b) => vec![String::from_utf8_lossy(b).into_owned()],
        ciborium::Value::Text(t) => vec![t.clone()],
        ciborium::Value::Array(arr) => {
            if is_cli_mode {
                return Err(RuntimeError::Handler(format!(
                    "File-path arg '{}' received a CBOR Array value in CLI mode; CLI \
                     dispatch must expand globs before calling into the runtime",
                    urn_str
                )));
            }
            let mut paths = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    ciborium::Value::Text(s) => paths.push(s.clone()),
                    ciborium::Value::Bytes(b) => paths.push(String::from_utf8_lossy(b).into_owned()),
                    other => {
                        return Err(RuntimeError::Handler(format!(
                            "File-path arg '{}' array contained an unsupported CBOR item: {:?}",
                            urn_str, other
                        )));
                    }
                }
            }
            paths
        }
        other => {
            return Err(RuntimeError::Handler(format!(
                "File-path arg '{}' value must be Bytes, Text, or (CBOR mode) Array — got {:?}",
                urn_str, other
            )));
        }
    };

    let mut resolved: Vec<std::path::PathBuf> = Vec::new();
    for raw in &raw_paths {
        let is_glob = raw.contains('*') || raw.contains('?') || raw.contains('[');
        if is_glob {
            let paths = glob::glob(raw).map_err(|e| {
                RuntimeError::Handler(format!("Invalid glob pattern '{}': {}", raw, e))
            })?;
            let before = resolved.len();
            for p in paths {
                let p = p.map_err(|e| RuntimeError::Handler(format!("Glob error: {}", e)))?;
                if p.is_file() {
                    resolved.push(p);
                }
            }
            if resolved.len() == before {
                return Err(RuntimeError::Handler(format!(
                    "No files matched glob pattern '{}'",
                    raw
                )));
            }
        } else {
            let path = std::path::PathBuf::from(raw);
            if !path.exists() {
                return Err(RuntimeError::Handler(format!("File not found: '{}'", raw)));
            }
            if !path.is_file() {
                return Err(RuntimeError::Handler(format!(
                    "Path is not a regular file: '{}'",
                    raw
                )));
            }
            resolved.push(path);
        }
    }

    Ok(resolved)
}

/// Replace an argument map's `value` and `media_urn` entries in place. Used by
/// `extract_effective_payload` after reading file bytes so the downstream
/// handler sees the post-conversion URN, not the original `media:file-path`.
fn replace_arg_value(
    arg_map: &mut Vec<(ciborium::Value, ciborium::Value)>,
    new_value: ciborium::Value,
    new_media_urn: String,
) {
    for (k, v) in arg_map.iter_mut() {
        if let ciborium::Value::Text(key) = k {
            match key.as_str() {
                "value" => *v = new_value.clone(),
                "media_urn" => *v = ciborium::Value::Text(new_media_urn.clone()),
                _ => {}
            }
        }
    }
}

#[async_trait]
impl PeerInvoker for PeerInvokerImpl {
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        let request_id = MessageId::new_uuid();
        tracing::info!(
            "[CartridgeRuntime] PEER_CALL: cap='{}' peer_rid={:?} origin_rid={:?}",
            cap_urn,
            request_id,
            self.origin_request_id
        );

        // Create tokio channel for response frames (unbounded to avoid backpressure issues)
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        // Register pending request before sending REQ
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(
                request_id.clone(),
                PendingPeerRequest {
                    sender,
                    origin_request_id: self.origin_request_id.clone(),
                    origin_routing_id: self.origin_routing_id.clone(),
                },
            );
        }

        // Send REQ with empty payload, stamped with parent_rid for cancel cascade
        let mut req_frame = Frame::req(request_id.clone(), cap_urn, vec![], "application/cbor");
        let mut meta = req_frame.meta.take().unwrap_or_default();
        meta.insert(
            "parent_rid".to_string(),
            match &self.origin_request_id {
                MessageId::Uuid(bytes) => ciborium::Value::Bytes(bytes.to_vec()),
                MessageId::Uint(n) => ciborium::Value::Integer((*n as i64).into()),
            },
        );
        req_frame.meta = Some(meta);
        self.output_tx.send(req_frame).map_err(|_| {
            self.pending_requests.lock().unwrap().remove(&request_id);
            RuntimeError::PeerRequest("Output channel closed".to_string())
        })?;

        // Create FrameSender for the PeerCall's arg OutputStreams
        let sender_arc: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: self.output_tx.clone(),
        });

        Ok(PeerCall {
            sender: sender_arc,
            request_id,
            max_chunk: self.max_chunk,
            response_rx: Some(receiver),
        })
    }
}

// =============================================================================
// DEMUX — splits a raw Frame channel into per-stream InputStream channels
// =============================================================================

/// Context for file-path auto-conversion in the Demux.
struct FilePathContext {
    file_path_pattern: MediaUrn,
    cap_urn: String,
    manifest: Option<CapManifest>,
}

impl FilePathContext {
    fn new(cap_urn: &str, manifest: Option<CapManifest>) -> Result<Self, RuntimeError> {
        Ok(Self {
            file_path_pattern: MediaUrn::from_string("media:file-path").map_err(|e| {
                RuntimeError::Handler(format!("Failed to create file-path pattern: {}", e))
            })?,
            cap_urn: cap_urn.to_string(),
            manifest,
        })
    }

    fn is_file_path(&self, media_urn_str: &str) -> bool {
        let arg_urn = match MediaUrn::from_string(media_urn_str) {
            Ok(u) => u,
            Err(_) => return false,
        };
        self.file_path_pattern.accepts(&arg_urn).unwrap_or(false)
    }

    /// Find a cap arg whose media URN is equivalent to the incoming URN.
    /// Uses `MediaUrn::is_equivalent` (tag-set equality) rather than string
    /// comparison so order-normalization and whitespace don't matter.
    fn find_arg<'a>(&'a self, incoming: &MediaUrn) -> Option<&'a CapArg> {
        let manifest = self.manifest.as_ref()?;
        let cap_def = manifest
            .all_caps()
            .into_iter()
            .find(|c| c.urn.to_string() == self.cap_urn)?;
        cap_def.args.iter().find(|a| {
            MediaUrn::from_string(&a.media_urn)
                .map(|arg_urn| arg_urn.is_equivalent(incoming).unwrap_or(false))
                .unwrap_or(false)
        })
    }

    /// Given the media URN of an incoming file-path stream, return the
    /// matching arg's stdin-source target URN.
    fn resolve_stdin_urn(&self, file_path_media_urn: &str) -> Option<String> {
        let incoming = MediaUrn::from_string(file_path_media_urn).ok()?;
        let arg_def = self.find_arg(&incoming)?;
        arg_def.sources.iter().find_map(|s| {
            if let ArgSource::Stdin { stdin } = s {
                Some(stdin.clone())
            } else {
                None
            }
        })
    }

    /// Return the matching arg's `is_sequence` declaration. Defaults to
    /// `false` when no matching arg is found (the conservative scalar path).
    fn arg_is_sequence(&self, file_path_media_urn: &str) -> bool {
        let Ok(incoming) = MediaUrn::from_string(file_path_media_urn) else {
            return false;
        };
        self.find_arg(&incoming)
            .map(|a| a.is_sequence)
            .unwrap_or(false)
    }
}

/// Demux for multi-stream mode (handler input).
/// Spawns a background tokio task that reads raw Frame channel and splits into
/// per-stream InputStream channels. Handles file-path interception.
///
/// Input: crossbeam channel of raw frames (fed by main loop's active_requests)
/// Output: tokio channels for async stream consumption
fn demux_multi_stream(
    raw_rx: crossbeam_channel::Receiver<Frame>,
    file_path_ctx: Option<FilePathContext>,
) -> InputPackage {
    let (streams_tx, streams_rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = tokio::task::spawn_blocking(move || {
        // Per-stream channels: stream_id → chunk sender (tokio unbounded for async recv)
        let mut stream_channels: HashMap<
            String,
            tokio::sync::mpsc::UnboundedSender<
                Result<(ciborium::Value, Option<StreamMeta>), StreamError>,
            >,
        > = HashMap::new();
        // File-path accumulators: stream_id → (media_urn, accumulated_chunk_payloads)
        let mut fp_accumulators: HashMap<String, (String, Vec<Vec<u8>>)> = HashMap::new();

        for frame in raw_rx {
            match frame.frame_type {
                FrameType::StreamStart => {
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let _ = streams_tx.send(Err(StreamError::Protocol(
                                "STREAM_START missing stream_id".into(),
                            )));
                            break;
                        }
                    };
                    let media_urn = frame.media_urn.as_ref().cloned().unwrap_or_default();

                    // Check if file-path (only when FilePathContext provided)
                    let is_fp = file_path_ctx
                        .as_ref()
                        .map_or(false, |ctx| ctx.is_file_path(&media_urn));

                    if is_fp {
                        fp_accumulators.insert(stream_id, (media_urn, Vec::new()));
                    } else {
                        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                        stream_channels.insert(stream_id.clone(), chunk_tx);
                        let input_stream = InputStream {
                            media_urn,
                            stream_meta: frame.meta,
                            rx: chunk_rx,
                        };
                        if streams_tx.send(Ok(input_stream)).is_err() {
                            break; // Handler dropped InputPackage
                        }
                    }
                }

                FrameType::Chunk => {
                    let stream_id = frame.stream_id.as_ref().cloned().unwrap_or_default();

                    // File-path accumulation?
                    if let Some((_, ref mut chunks)) = fp_accumulators.get_mut(&stream_id) {
                        if let Some(payload) = frame.payload {
                            chunks.push(payload);
                        }
                        continue;
                    }

                    // Regular stream — decode CBOR and forward with per-chunk meta
                    if let Some(tx) = stream_channels.get(&stream_id) {
                        if let Some(payload) = frame.payload {
                            // Checksum validation (MANDATORY in protocol v2)
                            let expected_checksum = match frame.checksum {
                                Some(c) => c,
                                None => {
                                    let _ = tx.send(Err(StreamError::Protocol(
                                        "CHUNK frame missing required checksum field".to_string(),
                                    )));
                                    continue;
                                }
                            };
                            let actual = Frame::compute_checksum(&payload);
                            if actual != expected_checksum {
                                let _ = tx.send(Err(StreamError::Protocol(format!(
                                    "Checksum mismatch: expected={}, actual={}",
                                    expected_checksum, actual
                                ))));
                                continue;
                            }
                            let chunk_meta = frame.meta;
                            match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                                Ok(value) => {
                                    let _ = tx.send(Ok((value, chunk_meta)));
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(StreamError::Decode(e.to_string())));
                                }
                            }
                        }
                    }
                }

                FrameType::StreamEnd => {
                    let stream_id = frame.stream_id.as_ref().cloned().unwrap_or_default();

                    // File-path stream ended — read file and deliver
                    if let Some((media_urn, chunks)) = fp_accumulators.remove(&stream_id) {
                        let ctx = match file_path_ctx.as_ref() {
                            Some(ctx) => ctx,
                            None => continue,
                        };

                        // Concatenate accumulated CBOR payloads → decode each as Value::Bytes → get path bytes
                        let mut path_bytes = Vec::new();
                        for chunk_payload in &chunks {
                            match ciborium::from_reader::<ciborium::Value, _>(&chunk_payload[..]) {
                                Ok(ciborium::Value::Bytes(b)) => path_bytes.extend(b),
                                Ok(ciborium::Value::Text(s)) => path_bytes.extend(s.into_bytes()),
                                Ok(other) => {
                                    let mut buf = Vec::new();
                                    let _ = ciborium::into_writer(&other, &mut buf);
                                    path_bytes.extend(buf);
                                }
                                Err(_) => {
                                    // Raw bytes (not CBOR-encoded)
                                    path_bytes.extend(chunk_payload);
                                }
                            }
                        }

                        // If the arg has a stdin source, read the file(s)
                        // and relabel. If not, pass through the file path as
                        // a plain value.
                        //
                        // Cardinality is driven by the arg's `is_sequence`
                        // declaration. Scalar args read one file; sequence
                        // args read N files and emit each as its own CHUNK
                        // (sequence mode on the output stream).
                        if let Some(resolved_urn) = ctx.resolve_stdin_urn(&media_urn) {
                            let is_sequence_arg = ctx.arg_is_sequence(&media_urn);
                            let paths_raw = String::from_utf8_lossy(&path_bytes).into_owned();
                            let candidates: Vec<String> = if is_sequence_arg {
                                // Sequence arg: allow a newline-separated list
                                // of paths or globs (plain text, no CBOR wrapping).
                                paths_raw
                                    .lines()
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect()
                            } else {
                                vec![paths_raw]
                            };

                            let mut resolved: Vec<std::path::PathBuf> = Vec::new();
                            let mut expansion_error: Option<String> = None;
                            for raw in &candidates {
                                let is_glob = raw.contains('*') || raw.contains('?') || raw.contains('[');
                                if is_glob {
                                    match glob::glob(raw) {
                                        Ok(paths) => {
                                            let before = resolved.len();
                                            for p in paths {
                                                match p {
                                                    Ok(p) if p.is_file() => resolved.push(p),
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        expansion_error = Some(format!("Glob error: {}", e));
                                                        break;
                                                    }
                                                }
                                            }
                                            if expansion_error.is_none() && resolved.len() == before {
                                                expansion_error = Some(format!("No files matched glob pattern '{}'", raw));
                                            }
                                        }
                                        Err(e) => {
                                            expansion_error = Some(format!("Invalid glob pattern '{}': {}", raw, e));
                                        }
                                    }
                                } else {
                                    let p = std::path::PathBuf::from(raw);
                                    if !p.exists() {
                                        expansion_error = Some(format!("File not found: '{}'", raw));
                                    } else if !p.is_file() {
                                        expansion_error = Some(format!("Path is not a regular file: '{}'", raw));
                                    } else {
                                        resolved.push(p);
                                    }
                                }
                                if expansion_error.is_some() {
                                    break;
                                }
                            }

                            if let Some(err) = expansion_error {
                                let _ = streams_tx.send(Err(StreamError::Io(err)));
                                break;
                            }

                            if !is_sequence_arg && resolved.len() != 1 {
                                let _ = streams_tx.send(Err(StreamError::Protocol(format!(
                                    "File-path arg with is_sequence=false resolved to {} files; \
                                     expected exactly 1. Sender must declare is_sequence=true to send multiple files.",
                                    resolved.len()
                                ))));
                                break;
                            }

                            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                            let mut send_failed = false;
                            for path in &resolved {
                                match std::fs::read(path) {
                                    Ok(bytes) => {
                                        if chunk_tx.send(Ok((ciborium::Value::Bytes(bytes), None))).is_err() {
                                            send_failed = true;
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = chunk_tx.send(Err(StreamError::Io(format!(
                                            "Failed to read file '{}': {}",
                                            path.display(),
                                            e
                                        ))));
                                        send_failed = true;
                                        break;
                                    }
                                }
                            }
                            drop(chunk_tx);

                            if send_failed {
                                break;
                            }

                            let input_stream = InputStream {
                                media_urn: resolved_urn,
                                stream_meta: None,
                                rx: chunk_rx,
                            };
                            if streams_tx.send(Ok(input_stream)).is_err() {
                                break;
                            }
                        } else {
                            // No stdin source — pass through the path bytes as-is
                            let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
                            let _ = chunk_tx.send(Ok((ciborium::Value::Bytes(path_bytes), None)));
                            drop(chunk_tx);
                            let input_stream = InputStream {
                                media_urn: media_urn.clone(),
                                stream_meta: None,
                                rx: chunk_rx,
                            };
                            if streams_tx.send(Ok(input_stream)).is_err() {
                                break;
                            }
                        }
                    } else {
                        // Regular stream ended — close per-stream channel
                        stream_channels.remove(&stream_id);
                    }
                }

                FrameType::End => {
                    // All streams done
                    break;
                }

                FrameType::Err => {
                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                    // Error all open streams
                    for (_, tx) in &stream_channels {
                        let _ = tx.send(Err(StreamError::RemoteError {
                            code: code.clone(),
                            message: message.clone(),
                        }));
                    }
                    stream_channels.clear();
                    let _ = streams_tx.send(Err(StreamError::RemoteError { code, message }));
                    break;
                }

                _ => {
                    // Ignore LOG, HEARTBEAT, etc.
                }
            }
        }
        // Dropping stream_channels closes all per-stream channels
        drop(stream_channels);
    });

    InputPackage {
        rx: streams_rx,
        _demux_handle: Some(handle),
    }
}

/// Demux for single-stream mode (peer response).
/// Reads frames from tokio channel expecting a single stream. Returns PeerResponse
/// that yields both data items and LOG frames through a single receiver.
///
/// Returns immediately — LOG frames are delivered in real-time as they arrive,
/// not blocked until the first data frame. This is critical for keeping the
/// engine's activity timer alive during long peer calls (e.g., model downloads).
fn demux_single_stream(mut raw_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>) -> PeerResponse {
    let (item_tx, item_rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(frame) = raw_rx.recv().await {
            match frame.frame_type {
                FrameType::StreamStart => {
                    // Structural frame — no item to deliver
                }
                FrameType::Chunk => {
                    if let Some(payload) = frame.payload {
                        // Checksum validation (MANDATORY in protocol v2)
                        let expected_checksum = match frame.checksum {
                            Some(c) => c,
                            None => {
                                let _ = item_tx.send(PeerResponseItem::Data(
                                    Err(StreamError::Protocol(
                                        "CHUNK frame missing required checksum field".to_string(),
                                    )),
                                    None,
                                ));
                                continue;
                            }
                        };
                        let actual = Frame::compute_checksum(&payload);
                        if actual != expected_checksum {
                            let _ = item_tx.send(PeerResponseItem::Data(
                                Err(StreamError::Protocol(format!(
                                    "Checksum mismatch: expected={}, actual={}",
                                    expected_checksum, actual
                                ))),
                                None,
                            ));
                            continue;
                        }
                        let chunk_meta = frame.meta;
                        match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                            Ok(value) => {
                                let _ = item_tx.send(PeerResponseItem::Data(Ok(value), chunk_meta));
                            }
                            Err(e) => {
                                let _ = item_tx.send(PeerResponseItem::Data(
                                    Err(StreamError::Decode(e.to_string())),
                                    None,
                                ));
                            }
                        }
                    }
                }
                FrameType::Log => {
                    let _ = item_tx.send(PeerResponseItem::Log(frame));
                }
                FrameType::StreamEnd | FrameType::End => {
                    break;
                }
                FrameType::Err => {
                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                    let _ = item_tx.send(PeerResponseItem::Data(
                        Err(StreamError::RemoteError { code, message }),
                        None,
                    ));
                    break;
                }
                _ => {}
            }
        }
    });

    PeerResponse { rx: item_rx }
}

// =============================================================================
// ACTIVE REQUEST TRACKING
// =============================================================================

/// Tracks an active incoming request. Reader loop routes frames here.
struct ActiveRequest {
    raw_tx: crossbeam_channel::Sender<Frame>,
}

/// A queued incoming request waiting for a handler slot.
/// The crossbeam sender is in `active_requests` for frame routing.
/// The receiver is held here until the handler is spawned.
struct QueuedRequest {
    factory: OpFactory,
    cap_urn: String,
    routing_id: Option<MessageId>,
    request_id: MessageId,
    raw_rx: crossbeam_channel::Receiver<Frame>,
}

/// Shared handle for dynamic concurrency capacity adjustment.
///
/// Cartridges receive this via `Request::capacity_handle()` and can call
/// `set(n)` at any time to adjust how many concurrent requests the runtime
/// will dispatch to handlers. For example, ggufcartridge might set capacity
/// to 1 during model load, then increase it when VRAM allows.
#[derive(Clone)]
pub struct CapacityHandle {
    value: Arc<std::sync::atomic::AtomicUsize>,
}

impl CapacityHandle {
    fn new(initial: usize) -> Self {
        Self {
            value: Arc::new(std::sync::atomic::AtomicUsize::new(initial)),
        }
    }

    /// Set the concurrency capacity. 0 means unlimited.
    pub fn set(&self, n: usize) {
        self.value.store(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the current capacity. 0 means unlimited.
    pub fn get(&self) -> usize {
        self.value.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The cartridge runtime that handles all I/O for cartridge binaries.
///
/// Cartridges create a runtime with their manifest, register handlers for their caps,
/// then call `run()` to process requests.
///
/// The manifest is REQUIRED - cartridges MUST provide their manifest which is sent
/// in the HELLO response during handshake. This is the ONLY way for cartridges to
/// communicate their capabilities to the host.
///
/// **Invocation Modes**:
/// - No CLI args: Cartridge CBOR mode (stdin/stdout binary frames)
/// - Any CLI args: CLI mode (parse args from cap definitions)
///
/// **Multiplexed execution** (CBOR mode): Multiple requests can be processed concurrently.
/// Each request handler runs in its own thread, allowing the runtime to:
/// - Respond to heartbeats while handlers are running
/// - Accept new requests while previous ones are still processing
/// - Handle multiple concurrent cap invocations
///
/// **Concurrency capacity**: Set via `set_capacity(n)` before `run()`. When set,
/// incoming requests beyond the capacity are queued. The runtime sends LOG frames
/// with `level="queued"` so the pipeline knows the request is alive but waiting.
/// When a handler slot opens, the next queued request is dequeued and dispatched.
/// Default is 0 (unlimited).
pub struct CartridgeRuntime {
    /// Registered Op factories by cap URN pattern
    handlers: HashMap<String, OpFactory>,

    /// Cartridge manifest JSON data - sent in HELLO response.
    /// This is REQUIRED - cartridges must provide their manifest.
    manifest_data: Vec<u8>,

    /// Parsed manifest for CLI mode processing
    manifest: Option<CapManifest>,

    /// Negotiated protocol limits
    limits: Limits,

    /// Concurrency capacity: 0 = unlimited, N = max N concurrent handlers.
    /// Shared via CapacityHandle so handlers can adjust dynamically.
    capacity: CapacityHandle,
}

/// Dispatch an Op with a Request via WetContext.
/// Closes the output stream on success (sends STREAM_END if stream was started).
async fn dispatch_op(
    op: Box<dyn Op<()>>,
    input: InputPackage,
    output: OutputStream,
    peer: Arc<dyn PeerInvoker>,
) -> Result<(), RuntimeError> {
    let req = Arc::new(Request::new(input, output, peer));
    let mut dry = DryContext::new();
    let mut wet = WetContext::new();
    wet.insert_arc(WET_KEY_REQUEST, req.clone());

    let result = op
        .perform(&mut dry, &mut wet)
        .await
        .map_err(|e| RuntimeError::Handler(e.to_string()));

    if result.is_ok() {
        let _ = req.output().close();
    }
    result
}

/// Spawn a handler task for an incoming request.
///
/// The crossbeam receiver carries frames routed by the main loop's active_requests
/// map. The handler's demux drains them (even if they arrived before this spawn).
fn spawn_handler(
    raw_rx: crossbeam_channel::Receiver<Frame>,
    factory: OpFactory,
    cap_urn: String,
    request_id: MessageId,
    routing_id: Option<MessageId>,
    output_tx: &tokio::sync::mpsc::UnboundedSender<Frame>,
    pending_peer_requests: &Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    manifest: &Option<CapManifest>,
    max_chunk: usize,
    handler_done_tx: &tokio::sync::mpsc::UnboundedSender<MessageId>,
) -> JoinHandle<()> {
    let output_tx_clone = output_tx.clone();
    let pending_clone = Arc::clone(pending_peer_requests);
    let manifest_clone = manifest.clone();
    let done_tx = handler_done_tx.clone();

    tokio::spawn(async move {
        tracing::info!(
            "[CartridgeRuntime] handler started: cap='{}' rid={:?}",
            cap_urn,
            request_id
        );
        let fp_ctx = FilePathContext::new(&cap_urn, manifest_clone).ok();
        let input_package = demux_multi_stream(raw_rx, fp_ctx);

        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
            tx: output_tx_clone.clone(),
        });
        let stream_id = uuid::Uuid::new_v4().to_string();
        let out_media = crate::CapUrn::from_string(&cap_urn)
            .map(|u| u.out_spec().to_string())
            .unwrap_or_else(|_| "media:".to_string());
        let output = OutputStream::new(
            Arc::clone(&sender),
            stream_id,
            out_media,
            request_id.clone(),
            routing_id.clone(),
            max_chunk,
        );

        let peer_invoker = PeerInvokerImpl {
            output_tx: output_tx_clone.clone(),
            pending_requests: Arc::clone(&pending_clone),
            max_chunk,
            origin_request_id: request_id.clone(),
            origin_routing_id: routing_id.clone(),
        };

        let op = factory();
        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer_invoker);
        let result = dispatch_op(op, input_package, output, peer_arc).await;

        match result {
            Ok(()) => {
                tracing::info!(
                    "[CartridgeRuntime] handler completed OK: cap='{}' rid={:?}",
                    cap_urn,
                    request_id
                );
                let mut end_frame = Frame::end_ok(request_id.clone(), None);
                end_frame.routing_id = routing_id;
                let _ = sender.send(&end_frame);
            }
            Err(e) => {
                tracing::error!(
                    "[CartridgeRuntime] handler FAILED: cap='{}' rid={:?} error={}",
                    cap_urn,
                    request_id,
                    e
                );
                let mut err_frame = Frame::err(request_id.clone(), "HANDLER_ERROR", &e.to_string());
                err_frame.routing_id = routing_id;
                let _ = sender.send(&err_frame);
            }
        }
        // Notify the main loop which handler finished so it can
        // check cancelled state and send deferred ERR if needed.
        let _ = done_tx.send(request_id);
    })
}

impl CartridgeRuntime {
    /// Create a new cartridge runtime with the required manifest.
    ///
    /// The manifest is JSON-encoded cartridge metadata including:
    /// - name: Cartridge name
    /// - version: Cartridge version
    /// - caps: Array of capability definitions with args and sources
    ///
    /// This manifest is sent in the HELLO response to the host (CBOR mode)
    /// and used for CLI argument parsing (CLI mode).
    /// **Cartridges MUST provide a manifest - there is no fallback.**
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - cartridges must declare it explicitly.
    pub fn new(manifest: &[u8]) -> Self {
        // Try to parse the manifest for CLI mode support
        let parsed_manifest = serde_json::from_slice::<CapManifest>(manifest).ok();

        // Validate manifest if parseable
        let (manifest_data, parsed_manifest) = match parsed_manifest {
            Some(m) => {
                // FAIL HARD if manifest doesn't have CAP_IDENTITY
                m.validate()
                    .expect("Manifest validation failed - cartridge MUST declare CAP_IDENTITY");
                let data = serde_json::to_vec(&m).unwrap_or_else(|_| manifest.to_vec());
                (data, Some(m))
            }
            None => (manifest.to_vec(), None),
        };

        let mut rt = Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: parsed_manifest,
            limits: Limits::default(),
            capacity: CapacityHandle::new(0),
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new cartridge runtime with a pre-built CapManifest.
    /// This is the preferred method as it ensures the manifest is valid.
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - cartridges must declare it explicitly.
    pub fn with_manifest(manifest: CapManifest) -> Self {
        // FAIL HARD if manifest doesn't have CAP_IDENTITY
        manifest
            .validate()
            .expect("Manifest validation failed - cartridge MUST declare CAP_IDENTITY");

        let manifest_data = serde_json::to_vec(&manifest).unwrap_or_default();
        let mut rt = Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: Some(manifest),
            limits: Limits::default(),
            capacity: CapacityHandle::new(0),
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new cartridge runtime with manifest JSON string.
    ///
    /// Auto-registers standard handlers (identity, discard) and ensures
    /// CAP_IDENTITY is present in the manifest.
    pub fn with_manifest_json(manifest_json: &str) -> Self {
        Self::new(manifest_json.as_bytes())
    }

    /// Register the standard identity and discard handlers.
    /// Cartridge authors can override either by calling register_op() after construction.
    fn register_standard_caps(&mut self) {
        if self.find_handler(CAP_IDENTITY).is_none() {
            self.register_op_type::<IdentityOp>(CAP_IDENTITY);
        }
        if self.find_handler(CAP_DISCARD).is_none() {
            self.register_op_type::<DiscardOp>(CAP_DISCARD);
        }
        if self.find_handler(CAP_ADAPTER_SELECTION).is_none() {
            self.register_op_type::<AdapterSelectionOp>(CAP_ADAPTER_SELECTION);
        }
    }

    /// Set the maximum number of concurrent handler invocations.
    ///
    /// When set to N > 0, the runtime queues incoming requests beyond N active
    /// handlers. Queued requests receive a LOG frame with `level="queued"` so the
    /// pipeline's activity timeout pauses for that body.
    ///
    /// * `0` — unlimited (default)
    /// * `1` — serial execution (e.g., ggufcartridge with single model loaded)
    /// * `N` — up to N concurrent handlers
    pub fn set_capacity(&mut self, n: usize) {
        self.capacity.set(n);
    }

    /// Get a clonable handle to the concurrency capacity.
    ///
    /// Handlers can use this to adjust capacity dynamically at runtime —
    /// for example, increasing capacity after freeing VRAM or decreasing it
    /// under memory pressure.
    pub fn capacity_handle(&self) -> CapacityHandle {
        self.capacity.clone()
    }

    /// Register an Op factory for a cap URN.
    /// The factory creates a fresh Op<()> instance per invocation.
    pub fn register_op<F>(&mut self, cap_urn: &str, factory: F)
    where
        F: Fn() -> Box<dyn Op<()>> + Send + Sync + 'static,
    {
        self.handlers.insert(cap_urn.to_string(), Arc::new(factory));
    }

    /// Register an Op type for a cap URN. The type must implement Op<()> + Default.
    /// Creates instances via Default::default() on each invocation.
    pub fn register_op_type<T: Op<()> + Default + 'static>(&mut self, cap_urn: &str) {
        self.handlers.insert(
            cap_urn.to_string(),
            Arc::new(|| Box::new(T::default()) as Box<dyn Op<()>>),
        );
    }

    /// Find a handler for a cap URN.
    /// Returns the OpFactory if found, None otherwise.
    ///
    /// Uses `is_dispatchable(provider, request)` to find handlers that can
    /// legally handle the request, then ranks by specificity.
    ///
    /// Ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific providers (positive distance) - refinements
    /// 3. More generic providers (negative distance) - fallbacks
    pub fn find_handler(&self, cap_urn: &str) -> Option<OpFactory> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();
        // (handler, signed_distance, is_non_negative)
        let mut best: Option<(OpFactory, isize)> = None;

        for (registered_cap_str, handler) in &self.handlers {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap_str) {
                // Use is_dispatchable: can this provider handle this request?
                if registered_urn.is_dispatchable(&request_urn) {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;

                    let dominated = match &best {
                        None => false,
                        Some((_, best_dist)) => {
                            // Current best dominates if:
                            // - best is non-negative and candidate is negative
                            // - OR both same sign and best has smaller abs distance
                            match (best_dist >= &0, signed_distance >= 0) {
                                (true, false) => true,  // best is refinement, candidate is fallback
                                (false, true) => false, // candidate is refinement, best is fallback
                                _ => best_dist.unsigned_abs() <= signed_distance.unsigned_abs(),
                            }
                        }
                    };

                    if !dominated {
                        best = Some((Arc::clone(handler), signed_distance));
                    }
                }
            }
        }

        best.map(|(handler, _)| handler)
    }

    /// Run the cartridge runtime.
    ///
    /// **Mode Detection**:
    /// - No CLI arguments: Cartridge CBOR mode (stdin/stdout binary frames)
    /// - Any CLI arguments: CLI mode (parse args from cap definitions)
    ///
    /// **CLI Mode**:
    /// - `manifest` subcommand: output manifest JSON
    /// - `<op>` subcommand: find cap by op tag, parse args, invoke handler
    /// - `--help`: show available subcommands
    ///
    /// **Cartridge CBOR Mode** (no CLI args):
    /// 1. Receive HELLO from host
    /// 2. Send HELLO back with manifest (handshake)
    /// 3. Main loop reads frames:
    ///    - REQ frames: spawn handler thread, continue reading
    ///    - HEARTBEAT frames: respond immediately
    ///    - RES/CHUNK/END frames: route to pending peer requests
    ///    - Other frames: ignore
    /// 4. Exit when stdin closes, wait for active handlers to complete
    ///
    /// **Multiplexing** (CBOR mode): The main loop never blocks on handler execution.
    /// Handlers run in separate threads, allowing concurrent processing
    /// of multiple requests and immediate heartbeat responses.
    ///
    /// **Bidirectional communication** (CBOR mode): Handlers can invoke caps on the host
    /// using the `PeerInvoker` parameter. Response frames from the host are
    /// routed to the appropriate pending request by MessageId.
    pub async fn run(&self) -> Result<(), RuntimeError> {
        let args: Vec<String> = std::env::args().collect();

        // No CLI arguments at all → Cartridge CBOR mode
        if args.len() == 1 {
            return self.run_cbor_mode().await;
        }

        // Any CLI arguments → CLI mode
        self.run_cli_mode(&args).await
    }

    /// Run in CLI mode - parse arguments and invoke handler.
    ///
    /// If stdin is piped (binary data), this streams it in chunks and accumulates.
    /// All modes converge: CLI args and stdin data are sent as CBOR frame streams
    /// through InputPackage, so handlers see the same API regardless of mode.
    async fn run_cli_mode(&self, args: &[String]) -> Result<(), RuntimeError> {
        let manifest = self.manifest.as_ref().ok_or_else(|| {
            RuntimeError::Manifest("Failed to parse manifest for CLI mode".to_string())
        })?;

        // Handle --help at top level
        if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
            self.print_help(manifest);
            return Ok(());
        }

        let subcommand = &args[1];

        // Handle manifest subcommand (always provided by runtime)
        if subcommand == "manifest" {
            let json = serde_json::to_string_pretty(manifest)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))?;
            println!("{}", json);
            return Ok(());
        }

        // Handle subcommand --help
        if args.len() == 3 && (args[2] == "--help" || args[2] == "-h") {
            if let Some(cap) = self.find_cap_by_command(manifest, subcommand) {
                self.print_cap_help(&cap);
                return Ok(());
            }
        }

        // Find cap by command name
        let cap = self
            .find_cap_by_command(manifest, subcommand)
            .ok_or_else(|| {
                RuntimeError::UnknownSubcommand(format!(
                    "Unknown subcommand '{}'. Run with --help to see available commands.",
                    subcommand
                ))
            })?;

        // Find handler factory
        let factory = self.find_handler(&cap.urn_string()).ok_or_else(|| {
            RuntimeError::NoHandler(format!(
                "No handler registered for cap '{}'",
                cap.urn_string()
            ))
        })?;

        // Extract CLI arguments (everything after subcommand)
        let cli_args = &args[2..];

        // Check if stdin is piped (binary streaming mode)
        let stdin_is_piped = !atty::is(atty::Stream::Stdin);
        let cap_accepts_stdin = cap.accepts_stdin();

        // Priority: CLI args > stdin (args take precedence)
        if !cli_args.is_empty() {
            // ARGUMENT PATH: Build from CLI arguments (may include file paths
            // or globs). If any file-path arg is declared `is_sequence=false`
            // but its value expands to multiple files, the runtime iterates
            // the handler once per file — a single process, N invocations,
            // outputs concatenated to stdout in glob-expansion order.
            let raw_payload = self.build_payload_from_cli(&cap, cli_args)?;
            let iterations = build_cli_foreach_iterations(&raw_payload, &cap)?;
            for per_iter_payload in iterations {
                let payload = extract_effective_payload(
                    &per_iter_payload,
                    Some("application/cbor"),
                    &cap,
                    true, // CLI mode
                )?;
                self.dispatch_cli_payload(&cap, factory.clone(), payload)
                    .await?;
            }
            Ok(())
        } else if stdin_is_piped && cap_accepts_stdin {
            // STREAMING PATH: No args, read stdin in chunks and accumulate
            let payload = self.build_payload_from_streaming_stdin(&cap)?;
            self.dispatch_cli_payload(&cap, factory, payload).await
        } else {
            Err(RuntimeError::MissingArgument(
                "No input provided (expected CLI arguments or piped stdin)".to_string(),
            ))
        }
    }

    /// Dispatch one CLI-mode invocation: take the (already file-path-resolved)
    /// CBOR arguments payload, build input streams, set up a CLI-backed
    /// `OutputStream`, and run the handler to completion.
    async fn dispatch_cli_payload(
        &self,
        _cap: &Cap,
        factory: OpFactory,
        payload: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let cli_emitter = CliStreamEmitter::without_ndjson();
        let frame_sender = CliFrameSender::with_emitter(cli_emitter);
        let peer = NoPeerInvoker;

        let cbor_value: ciborium::Value = ciborium::from_reader(&payload[..]).map_err(|e| {
            RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e))
        })?;
        let arguments = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => {
                return Err(RuntimeError::Deserialize(
                    "CBOR arguments must be an array".to_string(),
                ))
            }
        };

        let (tx, rx) = crossbeam_channel::unbounded();
        let max_chunk = Limits::default().max_chunk;
        let request_id = MessageId::new_uuid();

        for arg in arguments {
            let ciborium::Value::Map(arg_map) = arg else {
                continue;
            };
            let mut media_urn: Option<String> = None;
            let mut value_bytes: Option<Vec<u8>> = None;
            for (k, v) in arg_map {
                if let ciborium::Value::Text(key) = k {
                    match key.as_str() {
                        "media_urn" => {
                            if let ciborium::Value::Text(s) = v {
                                media_urn = Some(s);
                            }
                        }
                        "value" => {
                            let mut cbor_bytes = Vec::new();
                            ciborium::into_writer(&v, &mut cbor_bytes).map_err(|e| {
                                RuntimeError::Serialize(format!(
                                    "Failed to encode value: {}",
                                    e
                                ))
                            })?;
                            value_bytes = Some(cbor_bytes);
                        }
                        _ => {}
                    }
                }
            }

            let (Some(urn), Some(bytes)) = (media_urn, value_bytes) else {
                continue;
            };
            let stream_id = uuid::Uuid::new_v4().to_string();
            let start_frame = Frame::stream_start(
                request_id.clone(),
                stream_id.clone(),
                urn.clone(),
                None,
            );
            tx.send(start_frame).map_err(|_| {
                RuntimeError::Handler("Failed to send STREAM_START".to_string())
            })?;

            let chunk_count = if bytes.is_empty() {
                let checksum = Frame::compute_checksum(&[]);
                let chunk_frame = Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    0,
                    vec![],
                    0,
                    checksum,
                );
                tx.send(chunk_frame).map_err(|_| {
                    RuntimeError::Handler("Failed to send CHUNK".to_string())
                })?;
                1
            } else {
                let mut offset = 0;
                let mut chunk_index = 0u64;
                while offset < bytes.len() {
                    let chunk_size = (bytes.len() - offset).min(max_chunk);
                    let chunk_data = bytes[offset..offset + chunk_size].to_vec();
                    let checksum = Frame::compute_checksum(&chunk_data);
                    let chunk_frame = Frame::chunk(
                        request_id.clone(),
                        stream_id.clone(),
                        0,
                        chunk_data,
                        chunk_index,
                        checksum,
                    );
                    tx.send(chunk_frame).map_err(|_| {
                        RuntimeError::Handler("Failed to send CHUNK".to_string())
                    })?;
                    offset += chunk_size;
                    chunk_index += 1;
                }
                chunk_index
            };

            let end_frame =
                Frame::stream_end(request_id.clone(), stream_id.clone(), chunk_count);
            tx.send(end_frame).map_err(|_| {
                RuntimeError::Handler("Failed to send STREAM_END".to_string())
            })?;
        }

        let end_frame = Frame::end(request_id.clone(), None);
        tx.send(end_frame)
            .map_err(|_| RuntimeError::Handler("Failed to send END".to_string()))?;
        drop(tx);

        let input_package = demux_multi_stream(rx, None);

        let cli_sender: Arc<dyn FrameSender> = Arc::new(frame_sender);
        let output = OutputStream::new(
            cli_sender.clone(),
            uuid::Uuid::new_v4().to_string(),
            "*".to_string(),
            request_id.clone(),
            None,
            Limits::default().max_chunk,
        );

        let op = factory();
        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer);
        dispatch_op(op, input_package, output, peer_arc).await
    }

    /// Find a cap by its command name (the CLI subcommand).
    fn find_cap_by_command<'a>(
        &self,
        manifest: &'a CapManifest,
        command_name: &str,
    ) -> Option<&'a Cap> {
        manifest.all_caps().into_iter().find(|cap| cap.command == command_name)
    }

    /// Build payload from streaming stdin (CLI mode with piped binary).
    ///
    /// Public wrapper that reads from actual stdin.
    fn build_payload_from_streaming_stdin(&self, cap: &Cap) -> Result<Vec<u8>, RuntimeError> {
        let stdin = io::stdin();
        let locked = stdin.lock();
        self.build_payload_from_streaming_reader(cap, locked, Limits::default().max_chunk)
    }

    /// Build payload from streaming reader (testable version).
    ///
    /// This simulates the CBOR chunked request flow for CLI piped stdin:
    /// - Pure binary chunks from reader
    /// - Converted to virtual CHUNK frames on-the-fly
    /// - Accumulated via accumulation (same as CBOR mode)
    /// - Handler invoked when reader EOF (simulates END frame)
    ///
    /// This makes all 4 modes use the SAME accumulation code path:
    /// - CLI file path → read file → payload
    /// - CLI piped binary → chunk reader → accumulation → payload
    /// - CBOR chunked → accumulation → payload
    /// - CBOR file path → auto-convert → payload
    fn build_payload_from_streaming_reader<R: io::Read>(
        &self,
        cap: &Cap,
        mut reader: R,
        max_chunk: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        // Simulate accumulation structure (same as CBOR mode)
        struct PendingRequest {
            cap_urn: String,
            chunks: Vec<Vec<u8>>,
        }

        let mut pending = PendingRequest {
            cap_urn: cap.urn_string(),
            chunks: Vec::new(),
        };
        loop {
            let mut buffer = vec![0u8; max_chunk];
            match reader.read(&mut buffer) {
                Ok(0) => {
                    // EOF - simulate END frame
                    break;
                }
                Ok(n) => {
                    buffer.truncate(n);

                    // Simulate receiving CHUNK frame - add to accumulator immediately
                    pending.chunks.push(buffer);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Err(RuntimeError::Io(e));
                }
            }
        }

        // Concatenate chunks (same as accumulation does on END frame)
        let complete_payload = pending.chunks.concat();

        // Build CBOR arguments array (same format as CBOR mode)
        let cap_urn = CapUrn::from_string(&pending.cap_urn)
            .map_err(|e| RuntimeError::Cli(format!("Invalid cap URN: {}", e)))?;
        let expected_media_urn = cap_urn.in_spec();

        let arg = CapArgumentValue::new(expected_media_urn, complete_payload);
        let mut cbor_payload = Vec::new();
        let cbor_args: Vec<ciborium::Value> = vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(arg.media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(arg.value.clone()),
            ),
        ])];
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut cbor_payload)
            .map_err(|e| RuntimeError::Serialize(format!("Failed to serialize CBOR: {}", e)))?;

        Ok(cbor_payload)
    }

    /// Build payload from CLI arguments based on cap's arg definitions.
    ///
    /// This method builds a CBOR arguments array (same format as CBOR mode) to ensure
    /// consistency between CLI mode and CBOR mode. The payload format is:
    /// ```text
    /// [ { media_urn: "...", value: bytes }, ... ]
    /// ```
    fn build_payload_from_cli(
        &self,
        cap: &Cap,
        cli_args: &[String],
    ) -> Result<Vec<u8>, RuntimeError> {
        let mut arguments: Vec<CapArgumentValue> = Vec::new();

        // Check for stdin data if cap accepts stdin
        // Non-blocking check - if no data ready immediately, returns None
        let stdin_data = if cap.accepts_stdin() {
            self.read_stdin_if_available()?
        } else {
            None
        };

        // Process each cap argument
        for arg_def in cap.get_args() {
            let (value, came_from_stdin) =
                self.extract_arg_value(&arg_def, cli_args, stdin_data.as_deref())?;

            if let Some(val) = value {
                // Determine media_urn: if value came from stdin source, use stdin's media_urn
                // Otherwise use arg's media_urn as-is (file-path conversion happens later)
                let media_urn = if came_from_stdin {
                    // Find stdin source's media_urn
                    arg_def
                        .sources
                        .iter()
                        .find_map(|s| match s {
                            ArgSource::Stdin { stdin } => Some(stdin.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| arg_def.media_urn.clone())
                } else {
                    arg_def.media_urn.clone()
                };

                arguments.push(CapArgumentValue {
                    media_urn,
                    value: val,
                });
            } else if arg_def.required {
                return Err(RuntimeError::MissingArgument(format!(
                    "Required argument '{}' not provided",
                    arg_def.media_urn
                )));
            }
        }

        // If no arguments are defined but stdin data exists, use it as raw payload
        if cap.get_args().is_empty() {
            if let Some(data) = stdin_data {
                return Ok(data);
            }
            // No args and no stdin - return empty payload
            return Ok(vec![]);
        }

        // Build CBOR arguments array (same format as CBOR mode)
        if !arguments.is_empty() {
            let cbor_args: Vec<ciborium::Value> = arguments
                .iter()
                .map(|arg| {
                    ciborium::Value::Map(vec![
                        (
                            ciborium::Value::Text("media_urn".to_string()),
                            ciborium::Value::Text(arg.media_urn.clone()),
                        ),
                        (
                            ciborium::Value::Text("value".to_string()),
                            ciborium::Value::Bytes(arg.value.clone()),
                        ),
                    ])
                })
                .collect();

            let cbor_array = ciborium::Value::Array(cbor_args);
            let mut payload = Vec::new();
            ciborium::into_writer(&cbor_array, &mut payload).map_err(|e| {
                RuntimeError::Serialize(format!("Failed to encode CBOR payload: {}", e))
            })?;

            return Ok(payload);
        }

        // No arguments and no stdin
        Ok(vec![])
    }

    /// Extract a single argument value from CLI args or stdin.
    /// Returns (value, came_from_stdin) to track the source.
    fn extract_arg_value(
        &self,
        arg_def: &CapArg,
        cli_args: &[String],
        stdin_data: Option<&[u8]>,
    ) -> Result<(Option<Vec<u8>>, bool), RuntimeError> {
        // Try each source in order, returning RAW values (file paths, flags, etc.)
        // File-path auto-conversion happens later in extract_effective_payload()
        for source in &arg_def.sources {
            match source {
                ArgSource::CliFlag { cli_flag } => {
                    if let Some(value) = self.get_cli_flag_value(cli_args, cli_flag) {
                        return Ok((Some(value.into_bytes()), false));
                    }
                }
                ArgSource::Position { position } => {
                    // Positional args: filter out flags and their values
                    let positional = self.get_positional_args(cli_args);
                    if let Some(value) = positional.get(*position) {
                        return Ok((Some(value.clone().into_bytes()), false));
                    }
                }
                ArgSource::Stdin { .. } => {
                    if let Some(data) = stdin_data {
                        return Ok((Some(data.to_vec()), true)); // true = came from stdin
                    }
                }
            }
        }

        // Try default value
        if let Some(default) = &arg_def.default_value {
            let bytes =
                serde_json::to_vec(default).map_err(|e| RuntimeError::Serialize(e.to_string()))?;
            return Ok((Some(bytes), false));
        }

        Ok((None, false))
    }

    /// Get value for a CLI flag (e.g., --model "value")
    fn get_cli_flag_value(&self, args: &[String], flag: &str) -> Option<String> {
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            if arg == flag {
                return iter.next().cloned();
            }
            // Handle --flag=value format
            if let Some(stripped) = arg.strip_prefix(&format!("{}=", flag)) {
                return Some(stripped.to_string());
            }
        }
        None
    }

    /// Get positional arguments (non-flag arguments)
    fn get_positional_args(&self, args: &[String]) -> Vec<String> {
        let mut positional = Vec::new();
        let mut skip_next = false;

        for arg in args {
            if skip_next {
                skip_next = false;
                continue;
            }
            if arg.starts_with('-') {
                // This is a flag - skip its value too
                if !arg.contains('=') {
                    skip_next = true;
                }
            } else {
                positional.push(arg.clone());
            }
        }
        positional
    }

    /// Read stdin if data is available (non-blocking check).
    /// Returns None immediately if stdin is a terminal or no data is ready.
    fn read_stdin_if_available(&self) -> Result<Option<Vec<u8>>, RuntimeError> {
        use std::io::IsTerminal;
        use std::os::fd::AsRawFd;

        let stdin = io::stdin();

        // Don't read from stdin if it's a terminal (interactive)
        if stdin.is_terminal() {
            return Ok(None);
        }

        // Non-blocking check: use poll() to see if data is ready (Unix only for now)
        #[cfg(unix)]
        {
            use std::time::Duration;
            let fd = stdin.as_raw_fd();

            // Create pollfd structure for stdin
            let mut pollfd = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };

            // Poll with 0 timeout = non-blocking check
            let poll_result = unsafe { libc::poll(&mut pollfd as *mut libc::pollfd, 1, 0) };

            if poll_result < 0 {
                return Err(RuntimeError::Io(io::Error::last_os_error()));
            }

            // No data ready - return None immediately without blocking
            if poll_result == 0 || (pollfd.revents & libc::POLLIN) == 0 {
                return Ok(None);
            }

            // Data is ready - read it
            let mut data = Vec::new();
            stdin.lock().read_to_end(&mut data)?;

            if data.is_empty() {
                Ok(None)
            } else {
                Ok(Some(data))
            }
        }

        // Windows fallback: just try is_terminal check
        #[cfg(not(unix))]
        {
            // On Windows, if not a terminal, assume no data for CLI mode
            // This is conservative but prevents hangs
            Ok(None)
        }
    }

    /// Print help message showing all available subcommands.
    fn print_help(&self, manifest: &CapManifest) {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        use std::io::Write;

        let _ = writeln!(handle, "Usage: {} <command> [options]", manifest.name);
        let _ = writeln!(handle);
        let _ = writeln!(handle, "Commands:");
        let _ = writeln!(
            handle,
            "    {:16} Output cartridge manifest as JSON",
            "manifest"
        );

        for cap in manifest.all_caps() {
            let desc = cap.cap_description.as_deref().unwrap_or(&cap.title);
            let padded_command = format!("{:16}", cap.command);
            let _ = writeln!(handle, "    {}{}", padded_command, desc);
        }
        let _ = writeln!(handle);
        let _ = writeln!(
            handle,
            "Run '<command> --help' for more information on a command."
        );
    }

    /// Print help for a specific cap.
    fn print_cap_help(&self, cap: &Cap) {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        use std::io::Write;

        let _ = writeln!(handle, "Usage: {} [options]", cap.command);
        let _ = writeln!(handle);
        let desc = cap.cap_description.as_deref().unwrap_or(&cap.title);
        let _ = writeln!(handle, "{}", desc);
        let _ = writeln!(handle);
        let _ = writeln!(handle, "Arguments:");

        for arg in &cap.args {
            let desc = arg.arg_description.as_deref().unwrap_or("");
            let required_str = if arg.required { " (required)" } else { "" };

            for source in &arg.sources {
                match source {
                    ArgSource::CliFlag { cli_flag } => {
                        let padded_flag = format!("{:16}", cli_flag);
                        let _ = writeln!(handle, "    {}{}{}", padded_flag, desc, required_str);
                    }
                    ArgSource::Position { position } => {
                        let arg_name = format!("<arg{}>", position);
                        let padded_arg = format!("{:16}", arg_name);
                        let _ = writeln!(handle, "    {}{}{}", padded_arg, desc, required_str);
                    }
                    ArgSource::Stdin { .. } => {
                        let _ = writeln!(handle, "    {:16}{}{}", "<stdin>", desc, required_str);
                    }
                }
            }
        }
    }

    /// Run in Cartridge CBOR mode - binary frame protocol via stdin/stdout.
    ///
    /// When `capacity` is set (> 0), incoming requests beyond the active limit
    /// are queued. A LOG frame with `level="queued"` is sent back immediately
    /// so the pipeline's per-body activity timeout pauses. When a handler
    /// finishes and a slot opens, the next queued request is dequeued and its
    /// handler spawned. Frames for queued requests are buffered in the crossbeam
    /// channel (created on REQ) until the handler's demux drains them.
    async fn run_cbor_mode(&self) -> Result<(), RuntimeError> {
        let stdin = tokio::io::stdin();

        // Duplicate stdout so CBOR frame I/O is immune to anything that
        // writes to or closes the original FD 1 (e.g. a native library
        // writing to stdout, Metal shader compilation).  The duplicated FD
        // points to the same pipe but lives at a different descriptor number.
        let safe_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
        if safe_fd < 0 {
            return Err(RuntimeError::Io(std::io::Error::last_os_error()));
        }
        // Redirect FD 1 → stderr so any stray stdout writes end up in the
        // log instead of injecting non-CBOR bytes into the frame pipe.
        unsafe {
            libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO);
        }
        let stdout = tokio::fs::File::from_std(unsafe { std::fs::File::from_raw_fd(safe_fd) });

        // Use async buffered readers/writers
        let reader = BufReader::new(stdin);
        let writer = BufWriter::new(stdout);

        let mut frame_reader = FrameReader::new(reader);
        let mut frame_writer = FrameWriter::new(writer);

        // Perform handshake - send our manifest in the HELLO response
        let negotiated_limits =
            handshake_accept(&mut frame_reader, &mut frame_writer, &self.manifest_data).await?;
        frame_reader.set_limits(negotiated_limits);
        frame_writer.set_limits(negotiated_limits);

        // Create output channel - ALL frames (peer requests + responses) go through here
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();

        // Spawn writer task to drain output channel and write frames to stdout
        let writer_handle = tokio::spawn(async move {
            let mut seq_assigner = SeqAssigner::new();
            while let Some(mut frame) = output_rx.recv().await {
                // Assign centralized seq per request ID before writing
                seq_assigner.assign(&mut frame);
                if frame_writer.write(&frame).await.is_err() {
                    break;
                }
                // Cleanup seq tracking on terminal frames
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
                // Flush when no more frames are queued so the host sees
                // progress/log frames immediately instead of waiting for
                // BufWriter's 8KB buffer to fill.
                if output_rx.is_empty() {
                    let _ = frame_writer.inner_mut().flush().await;
                }
            }
            // CRITICAL: Flush buffered output before exiting!
            let _ = frame_writer.inner_mut().flush().await;
        });

        // Track pending peer requests (cartridge invoking host caps)
        let pending_peer_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Track active requests (incoming, frames routed here regardless of queue state).
        // The crossbeam sender lives here so the frame reader loop can route
        // STREAM_START/CHUNK/STREAM_END/END frames to it. This happens even for
        // queued requests — frames accumulate in the crossbeam channel until the
        // handler is spawned.
        let mut active_requests: HashMap<MessageId, ActiveRequest> = HashMap::new();

        // Track active handler tasks by request ID for per-request abort
        let mut active_handlers: HashMap<MessageId, JoinHandle<()>> = HashMap::new();
        // Track routing IDs per handler for stamping ERR frames on cancel
        let mut handler_routing_ids: HashMap<MessageId, Option<MessageId>> = HashMap::new();
        // Track cancelled requests to prevent duplicate ERR frames
        let mut cancelled_requests: std::collections::HashSet<MessageId> =
            std::collections::HashSet::new();

        // Queue for requests waiting for a handler slot.
        // Each entry holds the crossbeam receiver (the sender side is in active_requests).
        // When dequeued, the receiver is passed to the spawned handler.
        let mut request_queue: std::collections::VecDeque<QueuedRequest> =
            std::collections::VecDeque::new();

        // Number of currently running handlers (decremented when JoinHandles finish).
        let mut running_handler_count: usize = 0;

        // Notification channel: handlers send their RID when they finish so the main
        // loop can check cancelled state and send deferred ERR CANCELLED if needed.
        let (handler_done_tx, mut handler_done_rx) =
            tokio::sync::mpsc::unbounded_channel::<MessageId>();

        // Spawn a reader task that feeds frames into a channel.
        // This decouples stdin reading from the main select loop so that
        // handler-done signals can wake the loop even when no frames arrive.
        let (frame_tx, mut frame_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<Frame, CborError>>();
        let reader_handle = tokio::spawn(async move {
            loop {
                match frame_reader.read().await {
                    Ok(Some(frame)) => {
                        if frame_tx.send(Ok(frame)).is_err() {
                            break; // Main loop dropped — shutting down
                        }
                    }
                    Ok(None) => {
                        break; // EOF — stdin closed
                    }
                    Err(e) => {
                        let _ = frame_tx.send(Err(e));
                        break;
                    }
                }
            }
        });

        // Main loop: select between incoming frames and handler completion signals.
        // When a handler finishes it sends its RID on handler_done_tx, waking the
        // loop so it can check cancelled state, send deferred ERR if needed, and
        // drain the queue immediately — without waiting for the next frame from stdin.
        loop {
            // Drain queue: spawn handlers for queued requests that now have capacity.
            let cap = self.capacity.get();
            while !request_queue.is_empty() && (cap == 0 || running_handler_count < cap) {
                let queued = request_queue.pop_front().unwrap();

                tracing::info!(
                    "[CartridgeRuntime] dequeuing request: cap='{}' rid={:?} remaining_queue={}",
                    queued.cap_urn,
                    queued.request_id,
                    request_queue.len()
                );

                // Notify the caller that this request has been dequeued and is
                // starting. The "dequeued" level is the counterpart to "queued":
                // on the pipeline side, ActivityTimer unpauses and resets the
                // timeout clock, and the stall tracker is touched.
                let mut dequeued_log = Frame::log(
                    queued.request_id.clone(),
                    "dequeued",
                    "Request dequeued, handler starting",
                );
                dequeued_log.routing_id = queued.routing_id.clone();
                let _ = output_tx.send(dequeued_log);

                let handler_rid = queued.request_id.clone();
                let handler_xid = queued.routing_id.clone();
                let handle = spawn_handler(
                    queued.raw_rx,
                    queued.factory,
                    queued.cap_urn,
                    queued.request_id,
                    queued.routing_id,
                    &output_tx,
                    &pending_peer_requests,
                    &self.manifest,
                    negotiated_limits.max_chunk,
                    &handler_done_tx,
                );
                active_handlers.insert(handler_rid.clone(), handle);
                handler_routing_ids.insert(handler_rid, handler_xid);
                running_handler_count += 1;
            }

            // Select: either a frame arrives from stdin or a handler finishes.
            let frame = tokio::select! {
                biased;
                // Handler done — reap by RID, send deferred ERR if cancelled.
                Some(rid) = handler_done_rx.recv() => {
                    active_handlers.remove(&rid);
                    running_handler_count = running_handler_count.saturating_sub(1);
                    if cancelled_requests.remove(&rid) {
                        let routing_id = handler_routing_ids.remove(&rid).flatten();
                        let mut err = Frame::err(rid, "CANCELLED", "Request cancelled");
                        err.routing_id = routing_id;
                        let _ = output_tx.send(err);
                        tracing::info!("[CartridgeRuntime] Cancelled handler finished, sent deferred ERR");
                    } else {
                        handler_routing_ids.remove(&rid);
                    }
                    continue
                },
                // Frame from reader task.
                result = frame_rx.recv() => {
                    match result {
                        Some(Ok(f)) => f,
                        Some(Err(e)) => return Err(e.into()),
                        None => break, // Reader task ended (EOF)
                    }
                }
            };

            match frame.frame_type {
                FrameType::Req => {
                    // Extract routing_id (XID) FIRST — all error paths must include it
                    let routing_id = frame.routing_id.clone();
                    tracing::debug!(target: "cartridge_runtime", "Received REQ: cap={:?} xid={:?} rid={:?}", frame.cap, routing_id, frame.id);

                    let cap_urn = match frame.cap.as_ref() {
                        Some(urn) => urn.clone(),
                        None => {
                            let mut err_frame =
                                Frame::err(frame.id, "INVALID_REQUEST", "Request missing cap URN");
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    let factory = match self.find_handler(&cap_urn) {
                        Some(f) => f,
                        None => {
                            let mut err_frame = Frame::err(
                                frame.id.clone(),
                                "NO_HANDLER",
                                &format!("No handler registered for cap: {}", cap_urn),
                            );
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    if frame.payload.as_ref().map_or(false, |p| !p.is_empty()) {
                        let mut err_frame = Frame::err(
                            frame.id,
                            "PROTOCOL_ERROR",
                            "REQ frame must have empty payload - use STREAM_START for arguments",
                        );
                        err_frame.routing_id = routing_id;
                        let _ = output_tx.send(err_frame);
                        continue;
                    }

                    let request_id = frame.id.clone();

                    // Create channel for streaming frames to handler.
                    // Always created immediately so subsequent frames (STREAM_START,
                    // CHUNK, END) are routed here even if the handler isn't spawned
                    // yet. Frames accumulate in the crossbeam channel until the handler
                    // is spawned and the demux drains them.
                    let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
                    active_requests.insert(request_id.clone(), ActiveRequest { raw_tx });

                    let cap = self.capacity.get();
                    if cap > 0 && running_handler_count >= cap {
                        // At capacity — queue the request, send "queued" LOG back to caller.
                        let queue_pos = request_queue.len() + 1;
                        let mut log_frame = Frame::log(
                            request_id.clone(),
                            "queued",
                            &format!(
                                "Request queued (position {}, {} active)",
                                queue_pos, running_handler_count
                            ),
                        );
                        log_frame.routing_id = routing_id.clone();
                        let _ = output_tx.send(log_frame);

                        tracing::info!(
                            "[CartridgeRuntime] request queued: cap='{}' rid={:?} queue_pos={}",
                            cap_urn,
                            request_id,
                            queue_pos
                        );

                        request_queue.push_back(QueuedRequest {
                            factory,
                            cap_urn,
                            routing_id,
                            request_id,
                            raw_rx,
                        });
                    } else {
                        // Under capacity — spawn handler immediately.
                        let handler_rid = request_id.clone();
                        let handler_xid = routing_id.clone();
                        let handle = spawn_handler(
                            raw_rx,
                            factory,
                            cap_urn,
                            request_id,
                            routing_id,
                            &output_tx,
                            &pending_peer_requests,
                            &self.manifest,
                            negotiated_limits.max_chunk,
                            &handler_done_tx,
                        );
                        active_handlers.insert(handler_rid.clone(), handle);
                        handler_routing_ids.insert(handler_rid, handler_xid);
                        running_handler_count += 1;
                    }
                }

                // Route STREAM_START / CHUNK / STREAM_END / LOG to active request or peer response
                FrameType::StreamStart
                | FrameType::Chunk
                | FrameType::StreamEnd
                | FrameType::Log => {
                    // Try active request first
                    if let Some(ar) = active_requests.get(&frame.id) {
                        tracing::debug!(target: "cartridge_runtime", "Routing {:?} to active_request rid={:?}", frame.frame_type, frame.id);
                        if ar.raw_tx.send(frame.clone()).is_err() {
                            active_requests.remove(&frame.id);
                        }
                        continue;
                    }

                    // Try peer response
                    let peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.get(&frame.id) {
                        tracing::debug!(target: "cartridge_runtime", "Routing {:?} to peer_response rid={:?}", frame.frame_type, frame.id);
                        let _ = pr.sender.send(frame.clone());
                    } else {
                        tracing::warn!("[CartridgeRuntime] {:?} rid={:?} not found in active_requests or pending_peer_requests", frame.frame_type, frame.id);
                    }
                    drop(peer);
                }

                FrameType::End => {
                    // Try active request first -- send END then remove
                    if let Some(ar) = active_requests.remove(&frame.id) {
                        tracing::info!(
                            "[CartridgeRuntime] END routed to active_request rid={:?}",
                            frame.id
                        );
                        let _ = ar.raw_tx.send(frame.clone());
                        // raw_tx dropped here → Demux sees channel close after END
                        continue;
                    }

                    // Try peer response — send END then remove
                    let mut peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.remove(&frame.id) {
                        tracing::info!(
                            "[CartridgeRuntime] PEER_END received: peer_rid={:?} origin_rid={:?}",
                            frame.id,
                            pr.origin_request_id
                        );
                        let _ = pr.sender.send(frame.clone());
                    } else {
                        tracing::warn!("[CartridgeRuntime] END for unknown rid={:?} (not in active_requests or pending_peer_requests)", frame.id);
                    }
                    drop(peer);
                }

                FrameType::Err => {
                    tracing::error!(
                        "[CartridgeRuntime] ERR received: rid={:?} code={:?} msg={:?}",
                        frame.id,
                        frame.error_code(),
                        frame.error_message()
                    );
                    // Try active request first
                    if let Some(ar) = active_requests.remove(&frame.id) {
                        let _ = ar.raw_tx.send(frame.clone());
                        continue;
                    }

                    // Try peer response
                    let mut peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.remove(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    }
                    drop(peer);
                }

                FrameType::Cancel => {
                    let target_rid = frame.id.clone();
                    tracing::info!(
                        "[CartridgeRuntime] Cancel received: rid={:?} force_kill={:?}",
                        target_rid,
                        frame.force_kill
                    );

                    // Skip if already cancelled (prevent duplicate ERR)
                    if cancelled_requests.contains(&target_rid) {
                        tracing::debug!(
                            "[CartridgeRuntime] Cancel for already-cancelled rid={:?} — ignoring",
                            target_rid
                        );
                        continue;
                    }

                    // Case 1: Request is in the queue — remove it, send ERR
                    if let Some(pos) = request_queue
                        .iter()
                        .position(|q| q.request_id == target_rid)
                    {
                        let queued = request_queue.remove(pos).unwrap();
                        active_requests.remove(&target_rid);
                        let mut err = Frame::err(
                            target_rid.clone(),
                            "CANCELLED",
                            "Request cancelled while queued",
                        );
                        err.routing_id = queued.routing_id;
                        let _ = output_tx.send(err);
                        tracing::info!(
                            "[CartridgeRuntime] Cancelled queued request rid={:?}",
                            target_rid
                        );
                        continue;
                    }

                    // Case 2: Request has an active handler — cooperative cancel.
                    // force_kill is handled at the host level (kills the process);
                    // the cartridge runtime only ever sees cooperative cancels.
                    // Close the input channel so the handler's demux sees disconnect
                    // and the handler exits naturally. ERR CANCELLED is deferred
                    // until handlerDone(RID) arrives — this guarantees the handler's
                    // stream lifecycle completes (no orphaned streams) and produces
                    // identical wire behavior regardless of implementation language.
                    if active_handlers.contains_key(&target_rid) {
                        cancelled_requests.insert(target_rid.clone());
                        active_requests.remove(&target_rid);

                        // Cancel peer calls originating from this request
                        let peer_rids_to_cancel: Vec<(MessageId, Option<MessageId>)> = {
                            let peer = pending_peer_requests.lock().unwrap();
                            peer.iter()
                                .filter(|(_, pr)| pr.origin_request_id == target_rid)
                                .map(|(rid, pr)| (rid.clone(), pr.origin_routing_id.clone()))
                                .collect()
                        };
                        for (peer_rid, _) in &peer_rids_to_cancel {
                            let cancel =
                                Frame::cancel(peer_rid.clone(), frame.force_kill.unwrap_or(false));
                            let _ = output_tx.send(cancel);
                        }
                        {
                            let mut peer = pending_peer_requests.lock().unwrap();
                            for (peer_rid, _) in &peer_rids_to_cancel {
                                peer.remove(peer_rid);
                            }
                        }

                        tracing::info!("[CartridgeRuntime] Cancelled in-flight request (cooperative): rid={:?}", target_rid);
                        continue;
                    }

                    // Case 3: Unknown RID — silently ignore
                    tracing::debug!(
                        "[CartridgeRuntime] Cancel for unknown rid={:?} — ignoring",
                        target_rid
                    );
                }

                FrameType::Heartbeat => {
                    let mut response = Frame::heartbeat(frame.id);
                    if let Some((footprint_mb, rss_mb)) = get_own_memory_mb() {
                        let mut meta = std::collections::BTreeMap::new();
                        meta.insert(
                            "footprint_mb".into(),
                            ciborium::Value::Integer(footprint_mb.into()),
                        );
                        meta.insert("rss_mb".into(), ciborium::Value::Integer(rss_mb.into()));
                        response.meta = Some(meta);
                    }
                    let _ = output_tx.send(response);
                }

                FrameType::Hello => {
                    let err_frame = Frame::err(
                        frame.id,
                        "PROTOCOL_ERROR",
                        "Unexpected HELLO after handshake",
                    );
                    let _ = output_tx.send(err_frame);
                }

                FrameType::RelayNotify | FrameType::RelayState => {
                    return Err(CborError::Protocol(format!(
                        "Relay frame {:?} must not reach cartridge runtime",
                        frame.frame_type
                    ))
                    .into());
                }
            }
        }

        // Graceful shutdown
        reader_handle.abort();
        let _ = reader_handle.await;
        drop(output_tx);

        let _ = writer_handle.await;

        for (_, handle) in active_handlers {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Get the current protocol limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// Get this process's own physical memory footprint and RSS in MB.
/// Uses `proc_pid_rusage(getpid(), RUSAGE_INFO_V4)` which is always permitted,
/// even inside a macOS sandbox (the sandbox only blocks querying OTHER processes).
/// Returns `(footprint_mb, rss_mb)` or `None` on failure.
#[cfg(target_os = "macos")]
fn get_own_memory_mb() -> Option<(u64, u64)> {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::pid_t,
            4, // RUSAGE_INFO_V4
            &mut info as *mut _ as *mut libc::rusage_info_t,
        )
    };
    if result == 0 {
        Some((
            info.ri_phys_footprint / (1024 * 1024),
            info.ri_resident_size / (1024 * 1024),
        ))
    } else {
        None
    }
}

#[cfg(not(target_os = "macos"))]
fn get_own_memory_mb() -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::DEFAULT_MAX_CHUNK;

    // =========================================================================
    // Reusable test Op structs
    // =========================================================================

    /// Test Op: emits a fixed byte value
    struct EmitBytesOp {
        data: Vec<u8>,
    }
    #[async_trait]
    impl Op<()> for EmitBytesOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EmitBytesOp").build()
        }
    }

    /// Test Op: echoes all input chunks to output, optionally records received bytes
    struct EchoOp {
        received: Option<Arc<Mutex<Vec<u8>>>>,
    }
    impl Default for EchoOp {
        fn default() -> Self {
            Self { received: None }
        }
    }
    #[async_trait]
    impl Op<()> for EchoOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut total = Vec::new();
            while let Some(stream) = input.recv().await {
                let mut stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(chunk) = stream.recv_data().await {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    if let ciborium::Value::Bytes(ref b) = chunk {
                        total.extend(b);
                    }
                    req.output()
                        .emit_cbor(&chunk)
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            if let Some(ref received) = self.received {
                *received.lock().unwrap() = total;
            }
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EchoOp").build()
        }
    }

    /// Test Op: echoes input then appends a tag byte
    struct EchoTagOp {
        tag: Vec<u8>,
    }
    #[async_trait]
    impl Op<()> for EchoTagOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            while let Some(stream) = input.recv().await {
                let mut stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(chunk) = stream.recv_data().await {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    req.output()
                        .emit_cbor(&chunk)
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            req.output()
                .emit_cbor(&ciborium::Value::Bytes(self.tag.clone()))
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("EchoTagOp").build()
        }
    }

    /// Test Op: extracts CBOR "value" key from args, stores in shared state
    struct ExtractValueOp {
        received: Arc<Mutex<Vec<u8>>>,
    }
    #[async_trait]
    impl Op<()> for ExtractValueOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output()
                .start(false, None)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let bytes = input
                .collect_all_bytes()
                .await
                .map_err(|e| OpError::ExecutionFailed(format!("Stream error: {}", e)))?;
            let cbor_val: ciborium::Value = ciborium::from_reader(&bytes[..])
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            if let ciborium::Value::Array(args) = cbor_val {
                for arg in args {
                    if let ciborium::Value::Map(map) = arg {
                        for (k, v) in map {
                            if let (ciborium::Value::Text(key), ciborium::Value::Bytes(b)) = (k, v)
                            {
                                if key == "value" {
                                    *self.received.lock().unwrap() = b.clone();
                                    req.output()
                                        .emit_cbor(&ciborium::Value::Bytes(b))
                                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("ExtractValueOp").build()
        }
    }

    /// Test Op: no-op (does nothing)
    #[derive(Default)]
    struct NoOpOp;
    #[async_trait]
    impl Op<()> for NoOpOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet
                .get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req
                .take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata {
            OpMetadata::builder("NoOpOp").build()
        }
    }

    /// Helper: invoke a factory-produced Op with test input/output
    async fn invoke_op(
        factory: &OpFactory,
        input: InputPackage,
        output: OutputStream,
    ) -> Result<(), RuntimeError> {
        let op = factory();
        let peer: Arc<dyn PeerInvoker> = Arc::new(NoPeerInvoker);
        dispatch_op(op, input, output, peer).await
    }

    /// Create an InputPackage from a list of streams for testing.
    /// Each stream is a (media_urn, data_bytes) pair.
    /// The data is CBOR-encoded as Value::Bytes in a CHUNK frame.
    fn test_input_package(streams: &[(&str, &[u8])]) -> InputPackage {
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();

        for (media_urn, data) in streams {
            let stream_id = uuid::Uuid::new_v4().to_string();
            raw_tx
                .send(Frame::stream_start(
                    request_id.clone(),
                    stream_id.clone(),
                    media_urn.to_string(),
                    None,
                ))
                .ok();

            // Encode data as CBOR Bytes and wrap in CHUNK
            let value = ciborium::Value::Bytes(data.to_vec());
            let mut cbor = Vec::new();
            ciborium::into_writer(&value, &mut cbor).unwrap();
            let checksum = Frame::compute_checksum(&cbor);
            raw_tx
                .send(Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    0,
                    cbor,
                    0,
                    checksum,
                ))
                .ok();
            raw_tx
                .send(Frame::stream_end(request_id.clone(), stream_id, 1))
                .ok();
        }
        raw_tx.send(Frame::end(request_id, None)).ok();
        drop(raw_tx);

        demux_multi_stream(raw_rx, None)
    }

    /// Create an OutputStream backed by a channel for testing.
    /// Returns (OutputStream, frame_receiver) so tests can inspect output.
    fn test_output_stream() -> (OutputStream, tokio::sync::mpsc::UnboundedReceiver<Frame>) {
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender { tx: out_tx });
        let output = OutputStream::new(
            sender,
            uuid::Uuid::new_v4().to_string(),
            "*".to_string(),
            MessageId::new_uuid(),
            None,
            Limits::default().max_chunk,
        );
        (output, out_rx)
    }

    /// Helper function to create a Cap for tests
    fn create_test_cap(urn_str: &str, title: &str, command: &str, args: Vec<CapArg>) -> Cap {
        let urn = CapUrn::from_string(urn_str).expect("Invalid cap URN");
        Cap::with_args(urn, title.to_string(), command.to_string(), args)
    }

    /// Mock registry for tests - stores caps and returns them by URN lookup
    struct MockRegistry {
        caps: HashMap<String, Cap>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                caps: HashMap::new(),
            }
        }

        fn add_cap(&mut self, cap: Cap) {
            self.caps.insert(cap.urn_string(), cap);
        }

        fn get(&self, urn_str: &str) -> Option<&Cap> {
            // Normalize the URN for lookup
            let normalized = CapUrn::from_string(urn_str).ok()?.to_string();
            self.caps
                .iter()
                .find(|(k, _)| {
                    if let Ok(k_norm) = CapUrn::from_string(k) {
                        k_norm.to_string() == normalized
                    } else {
                        false
                    }
                })
                .map(|(_, v)| v)
        }

        /// Create a registry with common test caps
        fn with_test_caps() -> Self {
            let mut registry = Self::new();

            // Add common test caps used across tests
            registry.add_cap(create_test_cap(
                r#"cap:in="media:void";op=test;out="media:void""#,
                "Test",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:";op=process;out="media:void""#,
                "Process",
                "process",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:string;textable";op=test;out="*""#,
                "Test String",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="*";op=test;out="*""#,
                "Test Wildcard",
                "test",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:model-spec;textable";op=infer;out="*""#,
                "Infer",
                "infer",
                vec![],
            ));

            registry.add_cap(create_test_cap(
                r#"cap:in="media:pdf";op=process;out="*""#,
                "Process PDF",
                "process",
                vec![],
            ));

            registry
        }
    }

    /// Helper to test file-path array conversion: returns array of file bytes
    fn test_filepath_array_conversion(
        cap: &Cap,
        cli_args: &[String],
        runtime: &CartridgeRuntime,
    ) -> Vec<Vec<u8>> {
        // Extract raw argument value
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], cli_args, None)
            .unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(cap.args[0].media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

        // Decode and extract array of bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let value_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr.clone(),
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Extract bytes from each element
        value_array
            .iter()
            .map(|v| match v {
                ciborium::Value::Bytes(b) => b.clone(),
                _ => panic!("Expected bytes in array"),
            })
            .collect()
    }

    /// Helper to test file-path conversion: takes Cap, CLI args, and returns converted bytes
    fn test_filepath_conversion(
        cap: &Cap,
        cli_args: &[String],
        runtime: &CartridgeRuntime,
    ) -> Vec<u8> {
        // Extract raw argument value
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], cli_args, None)
            .unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text(cap.args[0].media_urn.clone()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

        // Decode and extract bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Bytes(b) => b.clone(),
                _ => panic!("Expected bytes"),
            })
            .unwrap()
    }

    /// Helper function to create a CapManifest for tests
    fn create_test_manifest(
        name: &str,
        version: &str,
        description: &str,
        mut caps: Vec<Cap>,
    ) -> CapManifest {
        // Always append CAP_IDENTITY at the end - cartridges must declare it
        // (Appending instead of prepending to avoid breaking tests that reference caps[0])
        let identity_urn = crate::CapUrn::from_string("cap:").unwrap();
        let identity_cap = Cap::new(identity_urn, "Identity".to_string(), "identity".to_string());
        caps.push(identity_cap);

        CapManifest::new(
            name.to_string(),
            version.to_string(),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            description.to_string(),
            vec![crate::CapGroup {
                name: "default".to_string(),
                caps,
                adapter_urns: Vec::new(),
            }],
        )
    }

    /// Test manifest JSON with identity and a test cap.
    /// Uses cap_groups format. The test cap URN "cap:op=test" has no in/out tags;
    /// CapUrn defaults both to media: (wildcard), which is valid.
    const TEST_MANIFEST: &str = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:","title":"Identity","command":"identity"},{"urn":"cap:op=test","title":"Test","command":"test"}]}]}"#;

    /// Valid manifest with proper in/out specs for tests that need parsed CapManifest
    const VALID_MANIFEST: &str = r#"{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:","title":"Identity","command":"identity"},{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test"}],"adapter_urns":[]}]}"#;

    // TEST248: Test register_op and find_handler by exact cap URN
    #[test]
    fn test248_register_and_find_handler() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:in=*;op=test;out=*", || {
            Box::new(EmitBytesOp {
                data: b"result".to_vec(),
            })
        });
        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    // TEST249: Test register_op handler echoes bytes directly
    #[tokio::test]
    async fn test249_raw_handler() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=raw", move || {
            Box::new(EchoOp {
                received: Some(Arc::clone(&received_clone)),
            }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:op=raw").unwrap();
        let input = test_input_package(&[("media:", b"echo this")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(
            &*received.lock().unwrap(),
            b"echo this",
            "raw handler must echo payload"
        );
    }

    // TEST250: Test Op handler collects input and processes it
    #[tokio::test]
    async fn test250_typed_handler_deserialization() {
        /// Test Op: parses JSON, extracts "key" field, emits as bytes
        struct JsonKeyOp {
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for JsonKeyOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input
                    .collect_all_bytes()
                    .await
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let json: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                let value = json
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("missing");
                let bytes = value.as_bytes();
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(bytes.to_vec()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = bytes.to_vec();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("JsonKeyOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=test", move || {
            Box::new(JsonKeyOp {
                received: Arc::clone(&received_clone),
            }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"{\"key\":\"hello\"}")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(&*received.lock().unwrap(), b"hello");
    }

    // TEST251: Test Op handler propagates errors through RuntimeError::Handler
    #[tokio::test]
    async fn test251_typed_handler_rejects_invalid_json() {
        /// Op that parses JSON — fails on invalid input
        struct JsonParseOp;
        #[async_trait]
        impl Op<()> for JsonParseOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input
                    .collect_all_bytes()
                    .await
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let _: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("JsonParseOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:op=test", || Box::new(JsonParseOp));

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"not json {{{{")]);
        let (output, _out_rx) = test_output_stream();
        let result = invoke_op(&factory, input, output).await;
        assert!(result.is_err(), "Invalid JSON must produce error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("JSON"),
            "Error should mention JSON: {}",
            err_msg
        );
    }

    // TEST252: Test find_handler returns None for unregistered cap URNs
    #[test]
    fn test252_find_handler_unknown_cap() {
        let runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        assert!(runtime.find_handler("cap:op=nonexistent").is_none());
    }

    // TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync)
    #[tokio::test]
    async fn test253_handler_is_send_sync() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=threaded", move || {
            let r = Arc::clone(&received_clone);
            Box::new(EmitAndRecordOp {
                data: b"done".to_vec(),
                received: r,
            }) as Box<dyn Op<()>>
        });

        /// Test Op: emits fixed bytes and records in shared state
        struct EmitAndRecordOp {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let _input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("EmitAndRecordOp").build()
            }
        }

        let factory = runtime.find_handler("cap:op=threaded").unwrap();
        let factory_clone = Arc::clone(&factory);

        let handle = tokio::spawn(async move {
            let input = test_input_package(&[("media:", b"{}")]);
            let (output, _out_rx) = test_output_stream();
            invoke_op(&factory_clone, input, output).await.unwrap();
        });

        handle.await.unwrap();
        assert_eq!(&*received.lock().unwrap(), b"done");
    }

    // TEST254: Test NoPeerInvoker always returns PeerRequest error
    #[test]
    fn test254_no_peer_invoker() {
        let no_peer = NoPeerInvoker;
        let result = no_peer.call("cap:op=test");
        assert!(result.is_err());
        match result {
            Err(RuntimeError::PeerRequest(msg)) => {
                assert!(
                    msg.contains("not supported"),
                    "error must indicate peer not supported"
                );
            }
            _ => panic!("Expected PeerRequest error"),
        }
    }

    // TEST255: Test NoPeerInvoker call_with_bytes also returns error
    #[tokio::test]
    async fn test255_no_peer_invoker_with_arguments() {
        let no_peer = NoPeerInvoker;
        let result = no_peer
            .call_with_bytes("cap:op=test", &[("media:test", b"value".as_slice())])
            .await;
        assert!(result.is_err());
    }

    // TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid
    #[test]
    fn test256_with_manifest_json() {
        // TEST_MANIFEST uses cap_groups format with identity + test cap.
        // "cap:op=test" has no in/out tags; CapUrn defaults both to media: (wildcard).
        let runtime_basic = CartridgeRuntime::with_manifest_json(TEST_MANIFEST);
        assert!(!runtime_basic.manifest_data.is_empty());
        assert!(
            runtime_basic.manifest.is_some(),
            "TEST_MANIFEST must parse: cap:op=test is valid (in/out default to media:)"
        );
        let manifest = runtime_basic.manifest.unwrap();
        assert_eq!(manifest.all_caps().len(), 2, "Two caps declared: identity + test");

        // VALID_MANIFEST has proper in/out specs
        let runtime_valid = CartridgeRuntime::with_manifest_json(VALID_MANIFEST);
        assert!(!runtime_valid.manifest_data.is_empty());
        assert!(
            runtime_valid.manifest.is_some(),
            "VALID_MANIFEST must parse into CapManifest"
        );
    }

    // TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None)
    #[test]
    fn test257_new_with_invalid_json() {
        let runtime = CartridgeRuntime::new(b"not json");
        assert!(!runtime.manifest_data.is_empty());
        assert!(
            runtime.manifest.is_none(),
            "invalid JSON should leave manifest as None"
        );
    }

    // TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data
    #[test]
    fn test258_with_manifest_struct() {
        let manifest: crate::bifaci::manifest::CapManifest =
            serde_json::from_str(VALID_MANIFEST).unwrap();
        let runtime = CartridgeRuntime::with_manifest(manifest);
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_some());
    }

    // TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged
    #[test]
    fn test259_extract_effective_payload_non_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:void";op=test;out="media:void""#)
            .unwrap();
        let payload = b"raw data";
        let result =
            extract_effective_payload(payload, Some("application/json"), cap, true).unwrap();
        assert_eq!(result, payload, "non-CBOR must return raw payload");
    }

    // TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
    #[test]
    fn test260_extract_effective_payload_no_content_type() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:void";op=test;out="media:void""#)
            .unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, None, cap, true).unwrap();
        assert_eq!(result, payload);
    }

    // TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
    #[test]
    fn test261_extract_effective_payload_cbor_match() {
        // Build CBOR arguments: [{media_urn: "media:string;textable", value: bytes("hello")}]
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:string;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(b"hello".to_vec()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // The cap URN has in=media:string;textable
        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:string;textable";op=test;out="*""#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Result is full CBOR array, handler must parse and extract
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        // Extract value from matching argument
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                found_value = Some(b);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            found_value,
            Some(b"hello".to_vec()),
            "Handler extracts value from CBOR array"
        );
    }

    // TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input
    #[test]
    fn test262_extract_effective_payload_cbor_no_match() {
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:other-type".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(b"data".to_vec()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:string;textable";op=test;out="*""#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err(), "must fail when no argument matches");
        match result.unwrap_err() {
            RuntimeError::Deserialize(msg) => {
                assert!(msg.contains("No argument found matching"), "{}", msg);
            }
            other => panic!("expected Deserialize, got {:?}", other),
        }
    }

    // TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error
    #[test]
    fn test263_extract_effective_payload_invalid_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="*";op=test;out="*""#).unwrap();
        let result = extract_effective_payload(
            b"not cbor",
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err());
    }

    // TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error
    #[test]
    fn test264_extract_effective_payload_cbor_not_array() {
        let value = ciborium::Value::Map(vec![]);
        let mut payload = Vec::new();
        ciborium::into_writer(&value, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="*";op=test;out="*""#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::Deserialize(msg) => {
                assert!(msg.contains("must be an array"), "{}", msg);
            }
            other => panic!("expected Deserialize, got {:?}", other),
        }
    }

    // TEST266: Test CliFrameSender wraps CliStreamEmitter correctly (basic construction)
    #[test]
    fn test266_cli_frame_sender_construction() {
        let sender = CliFrameSender::new();
        assert!(sender.emitter.ndjson, "default CLI sender must use NDJSON");

        let emitter2 = CliStreamEmitter::without_ndjson();
        let sender2 = CliFrameSender::with_emitter(emitter2);
        assert!(!sender2.emitter.ndjson);
    }

    // TEST268: Test RuntimeError variants display correct messages
    #[test]
    fn test268_runtime_error_display() {
        let err = RuntimeError::NoHandler("cap:op=missing".to_string());
        assert!(format!("{}", err).contains("cap:op=missing"));

        let err2 = RuntimeError::MissingArgument("model".to_string());
        assert!(format!("{}", err2).contains("model"));

        let err3 = RuntimeError::UnknownSubcommand("badcmd".to_string());
        assert!(format!("{}", err3).contains("badcmd"));

        let err4 = RuntimeError::Manifest("parse failed".to_string());
        assert!(format!("{}", err4).contains("parse failed"));

        let err5 = RuntimeError::PeerRequest("denied".to_string());
        assert!(format!("{}", err5).contains("denied"));

        let err6 = RuntimeError::PeerResponse("timeout".to_string());
        assert!(format!("{}", err6).contains("timeout"));
    }

    // TEST270: Test registering multiple Op handlers for different caps and finding each independently
    #[tokio::test]
    async fn test270_multiple_handlers() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_op("cap:op=alpha", || {
            Box::new(EchoTagOp { tag: b"a".to_vec() })
        });
        runtime.register_op("cap:op=beta", || Box::new(EchoTagOp { tag: b"b".to_vec() }));
        runtime.register_op("cap:op=gamma", || {
            Box::new(EchoTagOp { tag: b"g".to_vec() })
        });

        let f_alpha = runtime.find_handler("cap:op=alpha").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_alpha, input, output).await.unwrap();

        let f_beta = runtime.find_handler("cap:op=beta").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_beta, input, output).await.unwrap();

        let f_gamma = runtime.find_handler("cap:op=gamma").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_gamma, input, output).await.unwrap();
    }

    // TEST271: Test Op handler replacing an existing registration for the same cap URN
    #[tokio::test]
    async fn test271_handler_replacement() {
        let mut runtime = CartridgeRuntime::new(TEST_MANIFEST.as_bytes());

        let result1: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2_clone = Arc::clone(&result2);

        runtime.register_op("cap:op=test", move || {
            Box::new(EchoTagOp {
                tag: b"first".to_vec(),
            }) as Box<dyn Op<()>>
        });
        runtime.register_op("cap:op=test", move || {
            let r = Arc::clone(&result2_clone);
            Box::new(EmitAndRecordOp2 {
                data: b"second".to_vec(),
                received: r,
            }) as Box<dyn Op<()>>
        });

        /// Op that emits fixed data and records it
        struct EmitAndRecordOp2 {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp2 {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet
                    .get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let mut input = req
                    .take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                while let Some(stream_result) = input.recv().await {
                    let mut stream =
                        stream_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    while let Some(chunk) = stream.recv_data().await {
                        let _ = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    }
                }
                req.output()
                    .start(false, None)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output()
                    .emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("EmitAndRecordOp2").build()
            }
        }

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();
        assert_eq!(
            &*result2.lock().unwrap(),
            b"second",
            "later registration must replace earlier"
        );
        // result1 should NOT have been called
        assert!(
            result1.lock().unwrap().is_empty(),
            "first handler must not be called after replacement"
        );
    }

    // TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one
    #[test]
    fn test272_extract_effective_payload_multiple_args() {
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("media_urn".to_string()),
                    ciborium::Value::Text("media:other-type;textable".to_string()),
                ),
                (
                    ciborium::Value::Text("value".to_string()),
                    ciborium::Value::Bytes(b"wrong".to_vec()),
                ),
            ]),
            ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("media_urn".to_string()),
                    ciborium::Value::Text("media:model-spec;textable".to_string()),
                ),
                (
                    ciborium::Value::Text("value".to_string()),
                    ciborium::Value::Bytes(b"correct".to_vec()),
                ),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:model-spec;textable";op=infer;out="*""#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Handler receives full CBOR array with BOTH arguments
        // Handler must match against in_spec to find main input
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        assert_eq!(
            result_array.len(),
            2,
            "Both arguments present in CBOR array"
        );

        // Find the argument matching in_spec (media:model-spec)
        let in_spec = MediaUrn::from_string("media:model-spec;textable").unwrap();
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                let mut arg_urn_str = None;
                let mut arg_value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "media_urn" {
                            if let ciborium::Value::Text(s) = v {
                                arg_urn_str = Some(s);
                            }
                        } else if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                arg_value = Some(b);
                            }
                        }
                    }
                }

                // Match against in_spec using is_comparable for discovery
                if let (Some(urn_str), Some(val)) = (arg_urn_str, arg_value) {
                    if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                        if in_spec.is_comparable(&arg_urn).unwrap_or(false) {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(
            found_value,
            Some(b"correct".to_vec()),
            "Handler finds correct argument by matching in_spec"
        );
    }

    // TEST273: Test extract_effective_payload with binary data in CBOR value (not just text)
    #[test]
    fn test273_extract_effective_payload_binary_value() {
        let binary_data: Vec<u8> = (0u8..=255).collect();
        let args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:pdf".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(binary_data.clone()),
            ),
        ])]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry
            .get(r#"cap:in="media:pdf";op=process;out="*""#)
            .unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false, // CBOR mode - tests pass CBOR payloads directly
        )
        .unwrap();

        // NEW REGIME: Parse CBOR array and extract value
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                found_value = Some(b);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            found_value,
            Some(binary_data),
            "binary values must roundtrip through CBOR array"
        );
    }

    // TEST336: Single file-path arg with stdin source reads file and passes bytes to handler
    #[tokio::test]
    async fn test336_file_path_reads_file_passes_bytes() {
        use std::sync::{Arc, Mutex};

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test336_input.pdf");
        std::fs::write(&test_file, b"PDF binary content 336").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);

        // Track what handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            move || {
                Box::new(ExtractValueOp {
                    received: Arc::clone(&received_clone),
                }) as Box<dyn Op<()>>
            },
        );

        // Simulate CLI invocation: cartridge process /path/to/file.pdf
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (simulates what run_cli_mode does)
        // This does file-path auto-conversion: path → bytes
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Simulate CLI mode: parse CBOR args → send as streams → InputPackage
        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        // Verify handler received file bytes (not file path string)
        let received = received_payload.lock().unwrap();
        assert_eq!(
            &*received, b"PDF binary content 336",
            "Handler receives file bytes after auto-conversion"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST337: file-path arg without stdin source passes path as string (no conversion)
    #[test]
    fn test337_file_path_without_stdin_passes_string() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test337_input.txt");
        std::fs::write(&test_file, b"content").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:void\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![ArgSource::Position { position: 0 }], // NO stdin source!
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let result = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();

        // Should get file PATH as string, not file CONTENTS
        let value_str = String::from_utf8(result.0.unwrap()).unwrap();
        assert!(
            value_str.contains("test337_input.txt"),
            "Should receive file path string when no stdin source"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST338: file-path arg reads file via --file CLI flag
    #[test]
    fn test338_file_path_via_cli_flag() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test338.pdf");
        std::fs::write(&test_file, b"PDF via flag 338").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::CliFlag {
                        cli_flag: "--file".to_string(),
                    },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![
            "--file".to_string(),
            test_file.to_string_lossy().to_string(),
        ];
        let file_contents = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            file_contents, b"PDF via flag 338",
            "Should read file from --file flag"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST339: file-path-array reads multiple files with glob pattern
    #[test]
    fn test339_file_path_array_glob_expansion() {
        // A sequence-declared file-path arg (`is_sequence = true`) expands a
        // glob to N files and the runtime delivers them as a CBOR Array of
        // Bytes — one array item per matched file. List-ness comes from the
        // arg declaration, not from any `;list` URN tag.
        let temp_dir = std::env::temp_dir().join("test339");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file1 = temp_dir.join("doc1.txt");
        let file2 = temp_dir.join("doc2.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();

        let mut batch_arg = CapArg::new(
            "media:file-path;textable",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Batch",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let pattern = format!("{}/*.txt", temp_dir.display());
        let cli_args = vec![pattern];
        let files_bytes = test_filepath_array_conversion(&cap, &cli_args, &runtime);

        assert_eq!(files_bytes.len(), 2, "Should find 2 files");

        let mut sorted = files_bytes.clone();
        sorted.sort();
        assert_eq!(sorted, vec![b"content1".to_vec(), b"content2".to_vec()]);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST340: File not found error provides clear message
    #[test]
    fn test340_file_not_found_clear_error() {
        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["/nonexistent/file.pdf".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // extract_effective_payload should fail when trying to read nonexistent file
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when file doesn't exist");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("/nonexistent/file.pdf"),
            "Error should mention file path; got: {}",
            err_msg,
        );
        assert!(
            err_msg.contains("File not found") || err_msg.contains("Failed to read file"),
            "Error should be clear; got: {}",
            err_msg,
        );
    }

    // TEST341: stdin takes precedence over file-path in source order
    #[test]
    fn test341_stdin_precedence_over_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test341_input.txt");
        std::fs::write(&test_file, b"file content").unwrap();

        // Stdin source comes BEFORE position source
        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // First
                    ArgSource::Position { position: 0 }, // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let stdin_data = b"stdin content 341";
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        let (result, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, Some(stdin_data))
            .unwrap();
        let result = result.unwrap();

        // Should get stdin data, not file content (stdin source tried first)
        assert_eq!(
            result, b"stdin content 341",
            "stdin source should take precedence"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST342: file-path with position 0 reads first positional arg as file
    #[test]
    fn test342_file_path_position_zero_reads_first_arg() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test342.dat");
        std::fs::write(&test_file, b"binary data 342").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // CLI: cartridge test /path/to/file (position 0 after subcommand)
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, b"binary data 342", "Should read file at position 0");

        std::fs::remove_file(test_file).ok();
    }

    // TEST343: Non-file-path args are not affected by file reading
    #[test]
    fn test343_non_file_path_args_unaffected() {
        // Arg with different media type should NOT trigger file reading
        let cap = create_test_cap(
            "cap:in=\"media:void\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:model-spec;textable", // NOT file-path
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:model-spec;textable".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["mlx-community/Llama-3.2-3B-Instruct-4bit".to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let (result, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let result = result.unwrap();

        // Should get the string value, not attempt file read
        let value_str = String::from_utf8(result).unwrap();
        assert_eq!(value_str, "mlx-community/Llama-3.2-3B-Instruct-4bit");
    }

    // TEST344: file-path-array with nonexistent path fails clearly
    #[test]
    fn test344_file_path_array_invalid_json_fails() {
        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Pass nonexistent path (without `;json` tag, this is NOT JSON - it's a path/pattern)
        let cli_args = vec!["/nonexistent/path/to/nothing".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when path doesn't exist");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("/nonexistent/path/to/nothing"),
            "Error should mention the path"
        );
        assert!(
            err.contains("File not found") || err.contains("Failed to read"),
            "Error should be clear about file access failure"
        );
    }

    // TEST345: file-path-array with literal nonexistent path fails hard
    #[test]
    fn test345_file_path_array_one_file_missing_fails_hard() {
        let temp_dir = std::env::temp_dir();
        let missing_path = temp_dir.join("test345_missing.txt");

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Pass literal path (non-glob) that doesn't exist - should fail
        let cli_args = vec![missing_path.to_string_lossy().to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(
            result.is_err(),
            "Should fail hard when literal path doesn't exist"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("test345_missing.txt"),
            "Error should mention the missing file"
        );
        assert!(
            err.contains("File not found") || err.contains("doesn't exist"),
            "Error should be clear about missing file"
        );
    }

    // TEST346: Large file (1MB) reads successfully
    #[test]
    fn test346_large_file_reads_successfully() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test346_large.bin");

        // Create 1MB file
        let large_data = vec![42u8; 1_000_000];
        std::fs::write(&test_file, &large_data).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result.len(), 1_000_000, "Should read entire 1MB file");
        assert_eq!(result, large_data, "Content should match exactly");

        std::fs::remove_file(test_file).ok();
    }

    // TEST347: Empty file reads as empty bytes
    #[test]
    fn test347_empty_file_reads_as_empty_bytes() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test347_empty.txt");
        std::fs::write(&test_file, b"").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, b"", "Empty file should produce empty bytes");

        std::fs::remove_file(test_file).ok();
    }

    // TEST348: file-path conversion respects source order
    #[test]
    fn test348_file_path_conversion_respects_source_order() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test348.txt");
        std::fs::write(&test_file, b"file content 348").unwrap();

        // Position source BEFORE stdin source
        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Position { position: 0 }, // First
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        // Position source tried first, so file is read
        assert_eq!(
            result, b"file content 348",
            "Position source tried first, file read"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST349: file-path arg with multiple sources tries all in order
    #[test]
    fn test349_file_path_multiple_sources_fallback() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test349.txt");
        std::fs::write(&test_file, b"content 349").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::CliFlag {
                        cli_flag: "--file".to_string(),
                    }, // First (not provided)
                    ArgSource::Position { position: 0 }, // Second (provided)
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    }, // Third (not used)
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Only provide position arg, no --file flag
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            result, b"content 349",
            "Should fall back to position source and read file"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST350: Integration test - full CLI mode invocation with file-path
    #[tokio::test]
    async fn test350_full_cli_mode_with_file_path_integration() {
        use std::sync::{Arc, Mutex};

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test350_input.pdf");
        let test_content = b"PDF file content for integration test";
        std::fs::write(&test_file, test_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:result;textable\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);

        // Track what the handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:pdf\";op=process;out=\"media:result;textable\"",
            move || {
                Box::new(ExtractValueOp {
                    received: Arc::clone(&received_clone),
                }) as Box<dyn Op<()>>
            },
        );

        // Simulate full CLI invocation
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        // Verify handler received file bytes
        let received = received_payload.lock().unwrap();
        assert_eq!(
            &*received, test_content,
            "Handler receives file bytes after auto-conversion"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST351: sequence-declared file-path arg with empty input array (CBOR
    // mode) passes through as an empty CBOR Array — no implicit expansion,
    // no spurious error. Declaring `is_sequence = true` is what makes the
    // runtime emit an Array shape; URN tags are semantic only.
    #[test]
    fn test351_file_path_array_empty_array() {
        let mut batch_arg = CapArg::new(
            "media:file-path;textable",
            false, // Not required
            vec![ArgSource::Stdin {
                stdin: "media:".to_string(),
            }],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let value_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(
            value_array.len(),
            0,
            "Empty array should produce empty result"
        );
    }

    // TEST352: file permission denied error is clear (Unix-specific)
    #[test]
    #[cfg(unix)]
    fn test352_file_permission_denied_clear_error() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test352_noperm.txt");

        // Clean up any existing file from previous test runs (might have restricted permissions)
        if test_file.exists() {
            if let Ok(metadata) = std::fs::metadata(&test_file) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o644);
                let _ = std::fs::set_permissions(&test_file, perms);
            }
            std::fs::remove_file(&test_file).ok();
        }

        std::fs::write(&test_file, b"content").unwrap();

        // Remove read permissions
        let mut perms = std::fs::metadata(&test_file).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&test_file, perms).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Build full CBOR payload and attempt file-path conversion
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail on permission denied");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("test352_noperm.txt"),
            "Error should mention the file"
        );

        // Cleanup: restore permissions then delete
        let mut perms = std::fs::metadata(&test_file).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&test_file, perms).unwrap();
        std::fs::remove_file(test_file).ok();
    }

    // TEST353: CBOR payload format matches between CLI and CBOR mode
    #[test]
    fn test353_cbor_payload_format_consistency() {
        let cap = create_test_cap(
            "cap:in=\"media:text;textable\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:text;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:text;textable".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec!["test value".to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Decode CBOR payload
        let cbor_value: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let args_array = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        assert_eq!(args_array.len(), 1, "Should have 1 argument");

        // Verify structure: { media_urn: "...", value: bytes }
        let arg_map = match &args_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected CBOR map"),
        };

        assert_eq!(arg_map.len(), 2, "Argument should have media_urn and value");

        // Check media_urn key
        let media_urn_val = arg_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| v)
            .expect("Should have media_urn key");

        match media_urn_val {
            ciborium::Value::Text(s) => assert_eq!(s, "media:text;textable"),
            _ => panic!("media_urn should be text"),
        }

        // Check value key
        let value_val = arg_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .expect("Should have value key");

        match value_val {
            ciborium::Value::Bytes(b) => assert_eq!(b, b"test value"),
            _ => panic!("value should be bytes"),
        }
    }

    // TEST354: Glob pattern with no matches fails hard (NO FALLBACK)
    #[test]
    fn test354_glob_pattern_no_matches_empty_array() {
        let temp_dir = std::env::temp_dir();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Glob pattern that matches nothing - should FAIL HARD (no fallback to empty array)
        let pattern = format!("{}/nonexistent_*.xyz", temp_dir.display());
        let cli_args = vec![pattern]; // NOT JSON - just the pattern

        // Build CBOR payload and try conversion - should fail when glob matches nothing
        let (raw_value, _) = runtime
            .extract_arg_value(&cap.args[0], &cli_args, None)
            .unwrap();
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(raw_value.unwrap()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(
            result.is_err(),
            "Should fail hard when glob matches nothing - NO FALLBACK"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No files matched") || err.contains("nonexistent"),
            "Error should explain glob matched nothing"
        );
    }

    // TEST355: Glob pattern skips directories
    #[test]
    fn test355_glob_pattern_skips_directories() {
        let temp_dir = std::env::temp_dir().join("test355");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let subdir = temp_dir.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();

        let file1 = temp_dir.join("file1.txt");
        std::fs::write(&file1, b"content1").unwrap();

        let mut batch_arg = CapArg::new(
            "media:file-path;textable",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Glob that matches both file and directory
        let pattern = format!("{}/*", temp_dir.display());
        let cli_args = vec![pattern]; // NOT JSON - just the glob pattern
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to test file-path array conversion
        let files_array = test_filepath_array_conversion(&cap, &cli_args, &runtime);

        // Should only include the file, not the directory
        assert_eq!(
            files_array.len(),
            1,
            "Should only include files, not directories"
        );
        assert_eq!(files_array[0], b"content1");

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST356: Multiple glob patterns combined
    #[test]
    fn test356_multiple_glob_patterns_combined() {
        let temp_dir = std::env::temp_dir().join("test356");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file1 = temp_dir.join("doc.txt");
        let file2 = temp_dir.join("data.json");
        std::fs::write(&file1, b"text").unwrap();
        std::fs::write(&file2, b"json").unwrap();

        let mut batch_arg = CapArg::new(
            "media:file-path;textable",
            true,
            vec![
                ArgSource::Stdin {
                    stdin: "media:".to_string(),
                },
                ArgSource::Position { position: 0 },
            ],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![batch_arg],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Multiple patterns as CBOR Array (CBOR mode)
        let pattern1 = format!("{}/*.txt", temp_dir.display());
        let pattern2 = format!("{}/*.json", temp_dir.display());

        // Build CBOR payload with Array of patterns
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![
                    ciborium::Value::Text(pattern1),
                    ciborium::Value::Text(pattern2),
                ]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        // Decode and verify both files found
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let files_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(
            files_array.len(),
            2,
            "Should find both files from different patterns"
        );

        // Collect contents (order may vary)
        let mut contents = Vec::new();
        for val in files_array {
            match val {
                ciborium::Value::Bytes(b) => contents.push(b.as_slice()),
                _ => panic!("Expected bytes"),
            }
        }
        contents.sort();
        assert_eq!(contents, vec![b"json" as &[u8], b"text" as &[u8]]);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST357: Symlinks are followed when reading files
    #[test]
    #[cfg(unix)]
    fn test357_symlinks_followed() {
        use std::os::unix::fs as unix_fs;

        let temp_dir = std::env::temp_dir().join("test357");
        // Clean up from previous test runs
        std::fs::remove_dir_all(&temp_dir).ok();
        std::fs::create_dir_all(&temp_dir).unwrap();

        let real_file = temp_dir.join("real.txt");
        let link_file = temp_dir.join("link.txt");
        std::fs::write(&real_file, b"real content").unwrap();
        unix_fs::symlink(&real_file, &link_file).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![link_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Use helper to test file-path conversion
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(
            result, b"real content",
            "Should follow symlink and read real file"
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST358: Binary file with non-UTF8 data reads correctly
    #[test]
    fn test358_binary_file_non_utf8() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test358.bin");

        // Binary data that's not valid UTF-8
        let binary_data = vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F, 0xAB, 0xCD];
        std::fs::write(&test_file, &binary_data).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let result = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(result, binary_data, "Binary data should read correctly");

        std::fs::remove_file(test_file).ok();
    }

    // TEST359: Invalid glob pattern fails with clear error
    #[test]
    fn test359_invalid_glob_pattern_fails() {
        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Invalid glob pattern (unclosed bracket)
        let pattern = "[invalid";

        // Build CBOR payload with invalid pattern
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Text(pattern.to_string()),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Try file-path conversion with invalid glob - should fail
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail on invalid glob pattern");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid glob pattern") || err.contains("Pattern"),
            "Error should mention invalid glob"
        );
    }

    // TEST360: Extract effective payload handles file-path data correctly
    #[test]
    fn test360_extract_effective_payload_with_file_data() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test360.pdf");
        let pdf_content = b"PDF content for extraction test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:file-path;textable",
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();

        // Build CBOR payload (what build_payload_from_cli does)
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        // This does file-path auto-conversion and returns full CBOR array
        let effective = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true, // CLI mode
        )
        .unwrap();

        // NEW REGIME: Parse CBOR array and extract file bytes
        let result_cbor: ciborium::Value = ciborium::from_reader(&effective[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        // Extract value from argument matching in_spec
        let in_spec = MediaUrn::from_string("media:pdf").unwrap();
        let mut found_value = None;
        for arg in result_array {
            if let ciborium::Value::Map(map) = arg {
                let mut arg_urn_str = None;
                let mut arg_value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        if key == "media_urn" {
                            if let ciborium::Value::Text(s) = v {
                                arg_urn_str = Some(s);
                            }
                        } else if key == "value" {
                            if let ciborium::Value::Bytes(b) = v {
                                arg_value = Some(b);
                            }
                        }
                    }
                }

                if let (Some(urn_str), Some(val)) = (arg_urn_str, arg_value) {
                    if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                        let matches = in_spec.accepts(&arg_urn).unwrap_or(false)
                            || arg_urn.conforms_to(&in_spec).unwrap_or(false);
                        if matches {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(
            found_value,
            Some(pdf_content.to_vec()),
            "File-path auto-converted to bytes"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST361: CLI mode with file path - pass file path as command-line argument
    #[test]
    fn test361_cli_mode_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test361.pdf");
        let pdf_content = b"PDF content for CLI file path test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![
                    ArgSource::Stdin {
                        stdin: "media:pdf".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // CLI mode: pass file path as positional argument
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = runtime.manifest.as_ref().unwrap().all_caps()[0].clone();
        let payload = runtime
            .build_payload_from_cli(&cap, &cli_args)
            .unwrap();

        // Verify payload is CBOR array with file-path argument
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        assert!(
            matches!(cbor_val, ciborium::Value::Array(_)),
            "CLI mode produces CBOR array"
        );

        std::fs::remove_file(test_file).ok();
    }

    // TEST362: CLI mode with binary piped in - pipe binary data via stdin
    //
    // This test simulates real-world conditions:
    // - Pure binary data piped to stdin (NOT CBOR)
    // - CLI mode detected (command arg present)
    // - Cap accepts stdin source
    // - Binary is chunked on-the-fly and accumulated
    // - Handler receives complete CBOR payload
    #[test]
    fn test362_cli_mode_piped_binary() {
        use std::io::Cursor;

        // Simulate large binary being piped (1MB PDF)
        let pdf_content = vec![0xAB; 1_000_000];

        // Create cap that accepts stdin
        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:pdf",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:pdf".to_string(),
                }],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = CartridgeRuntime::with_manifest(manifest);

        // Mock stdin with Cursor (simulates piped binary)
        let mock_stdin = Cursor::new(pdf_content.clone());

        // Build payload from streaming reader (what CLI piped mode does)
        let payload = runtime
            .build_payload_from_streaming_reader(&cap, mock_stdin, Limits::default().max_chunk)
            .unwrap();

        // Verify payload is CBOR array with correct structure
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        match cbor_val {
            ciborium::Value::Array(arr) => {
                assert_eq!(arr.len(), 1, "CBOR array has one argument");

                if let ciborium::Value::Map(map) = &arr[0] {
                    let mut media_urn = None;
                    let mut value = None;

                    for (k, v) in map {
                        if let ciborium::Value::Text(key) = k {
                            match key.as_str() {
                                "media_urn" => {
                                    if let ciborium::Value::Text(s) = v {
                                        media_urn = Some(s.clone());
                                    }
                                }
                                "value" => {
                                    if let ciborium::Value::Bytes(b) = v {
                                        value = Some(b.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    assert_eq!(
                        media_urn,
                        Some("media:pdf".to_string()),
                        "Media URN matches cap in_spec"
                    );
                    assert_eq!(value, Some(pdf_content), "Binary content preserved exactly");
                } else {
                    panic!("Expected Map in CBOR array");
                }
            }
            _ => panic!("Expected CBOR Array"),
        }
    }

    // TEST363: CBOR mode with chunked content - send file content streaming as chunks
    #[tokio::test]
    async fn test363_cbor_mode_chunked_content() {
        use std::sync::{Arc, Mutex};

        let pdf_content = vec![0xAA; 10000]; // 10KB of data
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:pdf",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:pdf".to_string(),
                }],
            )],
        );

        let manifest = create_test_manifest("TestCartridge", "1.0.0", "Test", vec![cap.clone()]);
        let mut runtime = CartridgeRuntime::with_manifest(manifest);
        runtime.register_op(&cap.urn_string(), move || {
            Box::new(ExtractValueOp {
                received: Arc::clone(&received_clone),
            }) as Box<dyn Op<()>>
        });

        // Build CBOR payload with pdf_content
        let mut payload_bytes = Vec::new();
        let cbor_args = ciborium::Value::Array(vec![ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:pdf".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Bytes(pdf_content.clone()),
            ),
        ])]);
        ciborium::into_writer(&cbor_args, &mut payload_bytes).unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Send payload as InputPackage
        let input = test_input_package(&[("media:", &payload_bytes)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            pdf_content,
            "Handler receives chunked content"
        );
    }

    // TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion)
    #[test]
    fn test364_cbor_mode_file_path() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test364.pdf");
        let pdf_content = b"PDF content for CBOR file path test";
        std::fs::write(&test_file, pdf_content).unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:pdf".to_string(),
                }],
            )],
        );

        // Build CBOR arguments with file-path URN
        let args = vec![CapArgumentValue::new(
            MEDIA_FILE_PATH.to_string(),
            test_file.to_string_lossy().as_bytes().to_vec(),
        )];
        let mut payload = Vec::new();
        let cbor_args: Vec<ciborium::Value> = args
            .iter()
            .map(|arg| {
                ciborium::Value::Map(vec![
                    (
                        ciborium::Value::Text("media_urn".to_string()),
                        ciborium::Value::Text(arg.media_urn.clone()),
                    ),
                    (
                        ciborium::Value::Text("value".to_string()),
                        ciborium::Value::Bytes(arg.value.clone()),
                    ),
                ])
            })
            .collect();
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut payload).unwrap();

        // Extract effective payload (triggers file-path auto-conversion)
        let effective = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            &cap,
            false, // CBOR mode
        )
        .unwrap();

        // Verify the result is modified CBOR with PDF bytes (not file path)
        let result: ciborium::Value = ciborium::from_reader(&effective[..]).unwrap();
        if let ciborium::Value::Array(arr) = result {
            if let ciborium::Value::Map(map) = &arr[0] {
                let mut media_urn = None;
                let mut value = None;
                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        match key.as_str() {
                            "media_urn" => {
                                if let ciborium::Value::Text(s) = v {
                                    media_urn = Some(s);
                                }
                            }
                            "value" => {
                                if let ciborium::Value::Bytes(b) = v {
                                    value = Some(b);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                assert_eq!(
                    media_urn,
                    Some(&"media:pdf".to_string()),
                    "URN converted to expected input"
                );
                assert_eq!(
                    value,
                    Some(&pdf_content.to_vec()),
                    "File auto-converted to bytes"
                );
            }
        }

        std::fs::remove_file(test_file).ok();
    }

    // TEST1121: CBOR Array of file-paths in CBOR mode (validates new Array support)
    #[test]
    fn test1121_cbor_array_file_paths_in_cbor_mode() {
        let temp_dir = std::env::temp_dir().join("test361");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create three test files
        let file1 = temp_dir.join("file1.txt");
        let file2 = temp_dir.join("file2.txt");
        let file3 = temp_dir.join("file3.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();
        std::fs::write(&file3, b"content3").unwrap();

        let mut batch_arg = CapArg::new(
            "media:file-path;textable",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:".to_string(),
            }],
        );
        batch_arg.is_sequence = true;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![batch_arg],
        );

        // Build CBOR payload with Array of file paths (CBOR mode only)
        let arg = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("media_urn".to_string()),
                ciborium::Value::Text("media:file-path;textable".to_string()),
            ),
            (
                ciborium::Value::Text("value".to_string()),
                ciborium::Value::Array(vec![
                    ciborium::Value::Text(file1.to_string_lossy().to_string()),
                    ciborium::Value::Text(file2.to_string_lossy().to_string()),
                    ciborium::Value::Text(file3.to_string_lossy().to_string()),
                ]),
            ),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result =
            extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        // Decode and verify all three files read
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let files_array = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Verify all three files were read
        assert_eq!(
            files_array.len(),
            3,
            "Should read all three files from CBOR Array"
        );

        // Verify contents
        let mut contents = Vec::new();
        for val in files_array {
            match val {
                ciborium::Value::Bytes(b) => contents.push(b.clone()),
                _ => panic!("Expected bytes"),
            }
        }
        contents.sort();
        assert_eq!(
            contents,
            vec![
                b"content1".to_vec(),
                b"content2".to_vec(),
                b"content3".to_vec()
            ]
        );

        // Verify media_urn was converted
        let media_urn = result_map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| match v {
                ciborium::Value::Text(s) => s,
                _ => panic!("Expected text"),
            })
            .unwrap();
        assert_eq!(
            media_urn, "media:",
            "media_urn should be converted to stdin source"
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST395: Small payload (< max_chunk) produces correct CBOR arguments
    #[test]
    fn test395_build_payload_small() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let data = b"small payload";
        let reader = Cursor::new(data.to_vec());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk)
            .unwrap();

        // Verify CBOR structure
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        match cbor_val {
            ciborium::Value::Array(arr) => {
                assert_eq!(arr.len(), 1, "Should have one argument");
                match &arr[0] {
                    ciborium::Value::Map(map) => {
                        let value = map
                            .iter()
                            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
                            .map(|(_, v)| v)
                            .unwrap();
                        match value {
                            ciborium::Value::Bytes(b) => {
                                assert_eq!(b, &data.to_vec(), "Payload bytes should match");
                            }
                            _ => panic!("Expected Bytes"),
                        }
                    }
                    _ => panic!("Expected Map"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    // TEST396: Large payload (> max_chunk) accumulates across chunks correctly
    #[test]
    fn test396_build_payload_large() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        // Use small max_chunk to force multi-chunk
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let reader = Cursor::new(data.clone());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, 100)
            .unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .unwrap();
        match value {
            ciborium::Value::Bytes(b) => {
                assert_eq!(b.len(), 1000, "All bytes should be accumulated");
                assert_eq!(b, &data, "Data should match exactly");
            }
            _ => panic!("Expected Bytes"),
        }
    }

    // TEST397: Empty reader produces valid empty CBOR arguments
    #[test]
    fn test397_build_payload_empty() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let reader = Cursor::new(Vec::<u8>::new());

        let payload = runtime
            .build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk)
            .unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map
            .iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| v)
            .unwrap();
        match value {
            ciborium::Value::Bytes(b) => {
                assert!(b.is_empty(), "Empty reader should produce empty bytes");
            }
            _ => panic!("Expected Bytes"),
        }
    }

    // TEST398: IO error from reader propagates as RuntimeError::Io
    #[test]
    fn test398_build_payload_io_error() {
        struct ErrorReader;
        impl std::io::Read for ErrorReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "simulated read error",
                ))
            }
        }

        let cap = create_test_cap(
            "cap:in=\"media:\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![],
        );

        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());
        let result = runtime.build_payload_from_streaming_reader(
            &cap,
            ErrorReader,
            Limits::default().max_chunk,
        );

        assert!(result.is_err(), "IO error should propagate");
        match result {
            Err(RuntimeError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            }
            Err(e) => panic!("Expected RuntimeError::Io, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction
    #[test]
    fn test478_auto_registers_identity_handler() {
        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Identity handler must be registered at exact CAP_IDENTITY URN
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "CartridgeRuntime must auto-register identity handler"
        );

        // Discard handler must be registered at exact CAP_DISCARD URN
        assert!(
            runtime.find_handler(CAP_DISCARD).is_some(),
            "CartridgeRuntime must auto-register discard handler"
        );

        // Standard handlers must NOT match arbitrary specific requests
        // (request is pattern, registered cap is instance — broad caps don't satisfy specific patterns)
        assert!(
            runtime
                .find_handler("cap:in=\"media:void\";op=nonexistent;out=\"media:void\"")
                .is_none(),
            "Standard handlers must not catch arbitrary specific requests"
        );
    }

    // TEST1282: AdapterSelectionOp is auto-registered by CartridgeRuntime
    #[test]
    fn test1282_adapter_selection_auto_registered() {
        let runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        assert!(
            runtime.find_handler(CAP_ADAPTER_SELECTION).is_some(),
            "CartridgeRuntime must auto-register adapter selection handler"
        );
    }

    // TEST1283: Custom adapter selection Op overrides the default
    #[test]
    fn test1283_adapter_selection_custom_override() {
        let mut runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Verify default is registered
        assert!(runtime.find_handler(CAP_ADAPTER_SELECTION).is_some());

        // Override with custom handler
        #[derive(Default)]
        struct CustomAdapterOp;
        #[async_trait]
        impl Op<()> for CustomAdapterOp {
            async fn perform(&self, _dry: &mut DryContext, _wet: &mut WetContext) -> OpResult<()> {
                Ok(())
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("CustomAdapterOp").build()
            }
        }

        runtime.register_op_type::<CustomAdapterOp>(CAP_ADAPTER_SELECTION);

        // Must still find a handler (the custom one)
        assert!(
            runtime.find_handler(CAP_ADAPTER_SELECTION).is_some(),
            "Custom adapter selection handler must be findable after override"
        );
    }

    // TEST479: Custom identity Op overrides auto-registered default
    #[test]
    fn test479_custom_identity_overrides_default() {
        /// Op that always fails (to verify it's the custom handler that gets called)
        #[derive(Default)]
        struct FailOp;
        #[async_trait]
        impl Op<()> for FailOp {
            async fn perform(&self, _dry: &mut DryContext, _wet: &mut WetContext) -> OpResult<()> {
                Err(OpError::ExecutionFailed("custom identity".to_string()))
            }
            fn metadata(&self) -> OpMetadata {
                OpMetadata::builder("FailOp").build()
            }
        }

        let mut runtime = CartridgeRuntime::new(VALID_MANIFEST.as_bytes());

        // Auto-registered identity handler must exist
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "Auto-registered identity must exist before override"
        );

        // Count handlers before override
        let handlers_before = runtime.handlers.len();

        // Override identity with a custom Op
        runtime.register_op_type::<FailOp>(CAP_IDENTITY);

        // Handler count must not change (HashMap insert replaces, doesn't add)
        assert_eq!(
            runtime.handlers.len(),
            handlers_before,
            "Overriding identity must replace, not add a new entry"
        );

        // The handler at CAP_IDENTITY must still be findable
        assert!(
            runtime.find_handler(CAP_IDENTITY).is_some(),
            "Identity handler must be findable after override"
        );

        // Also verify discard was NOT affected by the override
        assert!(
            runtime.find_handler(CAP_DISCARD).is_some(),
            "Discard handler must still be present after overriding identity"
        );
    }

    // =========================================================================
    // Stream Abstractions Tests (InputStream, InputPackage, OutputStream, PeerCall)
    // =========================================================================

    use ciborium::Value;
    use std::sync::Arc;
    use tokio::sync::mpsc::unbounded_channel;

    // Helper: Create test InputStream from chunks (using tokio channels)
    fn create_test_input_stream(
        media_urn: &str,
        chunks: Vec<Result<Value, StreamError>>,
    ) -> InputStream {
        let (tx, rx) = unbounded_channel();
        for chunk in chunks {
            match chunk {
                Ok(value) => tx.send(Ok((value, None))).unwrap(),
                Err(e) => tx.send(Err(e)).unwrap(),
            }
        }
        drop(tx); // Close channel
        InputStream {
            media_urn: media_urn.to_string(),
            stream_meta: None,
            rx,
        }
    }

    // TEST529: InputStream recv yields chunks in order
    #[tokio::test]
    async fn test529_input_stream_recv_order() {
        let chunks = vec![
            Ok(Value::Bytes(b"chunk1".to_vec())),
            Ok(Value::Bytes(b"chunk2".to_vec())),
            Ok(Value::Bytes(b"chunk3".to_vec())),
        ];
        let mut stream = create_test_input_stream("media:test", chunks);

        let mut collected = Vec::new();
        while let Some(item) = stream.recv_data().await {
            collected.push(item);
        }
        assert_eq!(collected.len(), 3);
        assert_eq!(
            collected[0].as_ref().unwrap(),
            &Value::Bytes(b"chunk1".to_vec())
        );
        assert_eq!(
            collected[1].as_ref().unwrap(),
            &Value::Bytes(b"chunk2".to_vec())
        );
        assert_eq!(
            collected[2].as_ref().unwrap(),
            &Value::Bytes(b"chunk3".to_vec())
        );
    }

    // TEST530: InputStream::collect_bytes concatenates byte chunks
    #[tokio::test]
    async fn test530_input_stream_collect_bytes() {
        let chunks = vec![
            Ok(Value::Bytes(b"hello".to_vec())),
            Ok(Value::Bytes(b" ".to_vec())),
            Ok(Value::Bytes(b"world".to_vec())),
        ];
        let stream = create_test_input_stream("media:", chunks);

        let result = stream.collect_bytes().await.expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST531: InputStream::collect_bytes handles text chunks
    #[tokio::test]
    async fn test531_input_stream_collect_bytes_text() {
        let chunks = vec![
            Ok(Value::Text("hello".to_string())),
            Ok(Value::Text(" world".to_string())),
        ];
        let stream = create_test_input_stream("media:text", chunks);

        let result = stream.collect_bytes().await.expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST532: InputStream empty stream produces empty bytes
    #[tokio::test]
    async fn test532_input_stream_empty() {
        let chunks = vec![];
        let stream = create_test_input_stream("media:void", chunks);

        let result = stream
            .collect_bytes()
            .await
            .expect("empty stream must succeed");
        assert_eq!(result, b"");
    }

    // TEST533: InputStream propagates errors
    #[tokio::test]
    async fn test533_input_stream_error_propagation() {
        let chunks = vec![
            Ok(Value::Bytes(b"data".to_vec())),
            Err(StreamError::Protocol("test error".to_string())),
        ];
        let stream = create_test_input_stream("media:test", chunks);

        let result = stream.collect_bytes().await;
        assert!(result.is_err(), "error must propagate");

        if let Err(StreamError::Protocol(msg)) = result {
            assert_eq!(msg, "test error");
        } else {
            panic!("expected Protocol error");
        }
    }

    // TEST534: InputStream::media_urn returns correct URN
    #[test]
    fn test534_input_stream_media_urn() {
        let chunks = vec![Ok(Value::Bytes(b"data".to_vec()))];
        let stream = create_test_input_stream("media:image;format=png", chunks);

        assert_eq!(stream.media_urn(), "media:image;format=png");
    }

    // TEST535: InputPackage recv yields streams
    #[tokio::test]
    async fn test535_input_package_iteration() {
        let (tx, rx) = unbounded_channel();

        // Send 3 streams
        for i in 0..3 {
            let (stream_tx, stream_rx) = unbounded_channel();
            stream_tx
                .send(Ok((
                    Value::Bytes(format!("stream{}", i).into_bytes()),
                    None,
                )))
                .unwrap();
            drop(stream_tx);

            tx.send(Ok(InputStream {
                media_urn: format!("media:stream{}", i),
                stream_meta: None,
                rx: stream_rx,
            }))
            .unwrap();
        }
        drop(tx);

        let mut package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let mut streams = Vec::new();
        while let Some(result) = package.recv().await {
            streams.push(result);
        }
        assert_eq!(streams.len(), 3, "must yield 3 streams");

        for (i, result) in streams.iter().enumerate() {
            assert!(result.is_ok(), "stream {} must be Ok", i);
            let stream = result.as_ref().unwrap();
            assert_eq!(stream.media_urn(), format!("media:stream{}", i));
        }
    }

    // TEST536: InputPackage::collect_all_bytes aggregates all streams
    #[tokio::test]
    async fn test536_input_package_collect_all_bytes() {
        let (tx, rx) = unbounded_channel();

        // Stream 1: "hello"
        let (s1_tx, s1_rx) = unbounded_channel();
        s1_tx
            .send(Ok((Value::Bytes(b"hello".to_vec()), None)))
            .unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s1".to_string(),
            stream_meta: None,
            rx: s1_rx,
        }))
        .unwrap();

        // Stream 2: " world"
        let (s2_tx, s2_rx) = unbounded_channel();
        s2_tx
            .send(Ok((Value::Bytes(b" world".to_vec()), None)))
            .unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s2".to_string(),
            stream_meta: None,
            rx: s2_rx,
        }))
        .unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package.collect_all_bytes().await.expect("must succeed");
        assert_eq!(all_bytes, b"hello world");
    }

    // TEST537: InputPackage empty package produces empty bytes
    #[tokio::test]
    async fn test537_input_package_empty() {
        let (tx, rx) = unbounded_channel();
        drop(tx); // No streams

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package
            .collect_all_bytes()
            .await
            .expect("empty package must succeed");
        assert_eq!(all_bytes, b"");
    }

    // TEST538: InputPackage propagates stream errors
    #[tokio::test]
    async fn test538_input_package_error_propagation() {
        let (tx, rx) = unbounded_channel();

        // Good stream
        let (s1_tx, s1_rx) = unbounded_channel();
        s1_tx
            .send(Ok((Value::Bytes(b"data".to_vec()), None)))
            .unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:good".to_string(),
            stream_meta: None,
            rx: s1_rx,
        }))
        .unwrap();

        // Error stream
        let (s2_tx, s2_rx) = unbounded_channel();
        s2_tx
            .send(Err(StreamError::Protocol("stream error".to_string())))
            .unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:bad".to_string(),
            stream_meta: None,
            rx: s2_rx,
        }))
        .unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let result = package.collect_all_bytes().await;
        assert!(result.is_err(), "error must propagate from bad stream");
    }

    // Mock FrameSender for testing OutputStream
    struct MockFrameSender {
        frames: Arc<Mutex<Vec<Frame>>>,
    }

    impl MockFrameSender {
        fn new() -> (Self, Arc<Mutex<Vec<Frame>>>) {
            let frames = Arc::new(Mutex::new(Vec::new()));
            let sender = Self {
                frames: Arc::clone(&frames),
            };
            (sender, frames)
        }
    }

    impl FrameSender for MockFrameSender {
        fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
            self.frames.lock().unwrap().push(frame.clone());
            Ok(())
        }
    }

    // TEST539: OutputStream sends STREAM_START on first write
    #[test]
    fn test539_output_stream_sends_stream_start() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        stream.start(false, None).expect("start must succeed");
        stream
            .emit_cbor(&Value::Bytes(b"test".to_vec()))
            .expect("write must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured.len() >= 1, "must send at least STREAM_START");
        assert_eq!(
            captured[0].frame_type,
            FrameType::StreamStart,
            "first frame must be STREAM_START"
        );
        assert_eq!(captured[0].stream_id, Some("stream-1".to_string()));
    }

    // TEST540: OutputStream::close sends STREAM_END with correct chunk_count
    #[test]
    fn test540_output_stream_close_sends_stream_end() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        // Write 3 chunks
        stream.start(false, None).unwrap();
        stream.emit_cbor(&Value::Bytes(b"chunk1".to_vec())).unwrap();
        stream.emit_cbor(&Value::Bytes(b"chunk2".to_vec())).unwrap();
        stream.emit_cbor(&Value::Bytes(b"chunk3".to_vec())).unwrap();

        stream.close().expect("close must succeed");

        let captured = frames.lock().unwrap();
        let stream_end = captured
            .iter()
            .find(|f| f.frame_type == FrameType::StreamEnd)
            .expect("must have STREAM_END");

        assert_eq!(stream_end.chunk_count, Some(3), "chunk_count must be 3");
    }

    // TEST541: OutputStream chunks large data correctly
    #[test]
    fn test541_output_stream_chunks_large_data() {
        let (sender, frames) = MockFrameSender::new();
        let max_chunk = 100; // Small chunk size for testing
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:".to_string(),
            MessageId::new_uuid(),
            None,
            max_chunk,
        );

        // Write 250 bytes (should create 3 chunks: 100, 100, 50)
        stream.start(false, None).unwrap();
        let large_data = vec![0xAA; 250];
        stream.emit_cbor(&Value::Bytes(large_data)).unwrap();
        stream.close().unwrap();

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();

        assert!(
            chunks.len() >= 3,
            "large data must be chunked (got {} chunks)",
            chunks.len()
        );
    }

    // TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only
    #[test]
    fn test542_output_stream_empty() {
        let (sender, frames) = MockFrameSender::new();
        let mut stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:void".to_string(),
            MessageId::new_uuid(),
            None,
            256_000,
        );

        stream.start(false, None).expect("start must succeed");
        stream.close().expect("close must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured
            .iter()
            .any(|f| f.frame_type == FrameType::StreamStart));
        assert!(captured
            .iter()
            .any(|f| f.frame_type == FrameType::StreamEnd));

        let chunk_count = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .count();
        assert_eq!(chunk_count, 0, "empty stream must have zero chunks");
    }

    // TEST543: PeerCall::arg creates OutputStream with correct stream_id
    #[test]
    fn test543_peer_call_arg_creates_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (_response_tx, response_rx) = unbounded_channel();

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: MessageId::new_uuid(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let arg_stream = peer.arg("media:argument");
        assert_eq!(arg_stream.media_urn, "media:argument");
        assert!(
            !arg_stream.stream_id.is_empty(),
            "stream_id must be generated"
        );
    }

    // TEST544: PeerCall::finish sends END frame
    #[tokio::test]
    async fn test544_peer_call_finish_sends_end() {
        let (sender, frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        // Close response channel immediately (simulates empty response)
        drop(response_tx);

        let request_id = MessageId::new_uuid();
        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: request_id.clone(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let _response = peer.finish().await.expect("finish must succeed");

        let captured = frames.lock().unwrap();
        let end_frame = captured
            .iter()
            .find(|f| f.frame_type == FrameType::End)
            .expect("must send END frame");

        assert_eq!(end_frame.id, request_id, "END must have correct request ID");
    }

    // TEST545: PeerCall::finish returns PeerResponse with data
    #[tokio::test]
    async fn test545_peer_call_finish_returns_response_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        // Send response frames (simulating STREAM_START + CHUNK + STREAM_END)
        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("response-stream".to_string());
        start.media_urn = Some("media:response".to_string());
        response_tx.send(start).unwrap();

        // CHUNK - payload must be CBOR-encoded
        let raw_data = b"response data".to_vec();
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(raw_data.clone()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "response-stream".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(
                req_id.clone(),
                "response-stream".to_string(),
                1,
            ))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let response = peer.finish().await.expect("finish must succeed");

        let bytes = response
            .collect_bytes()
            .await
            .expect("collect must succeed");
        assert_eq!(bytes, b"response data");
    }

    // TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately
    //
    // This tests the critical fix: during a peer call, the peer (e.g., modelcartridge)
    // sends LOG frames for minutes during model download BEFORE sending any data
    // (StreamStart + Chunk). The handler must receive these LOGs in real-time so it
    // can re-emit progress and keep the engine's activity timer alive.
    //
    // Previously, demux_single_stream blocked on awaiting StreamStart before returning
    // PeerResponse, which meant the handler couldn't call recv() until data arrived —
    // causing 120s activity timeouts during long downloads.
    #[tokio::test]
    async fn test839_peer_response_delivers_logs_before_stream_start() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // Send LOG frames BEFORE any StreamStart — simulates modelcartridge
        // sending download progress before the actual data response
        response_tx
            .send(Frame::progress(
                req_id.clone(),
                0.1,
                "downloading file 1/10",
            ))
            .unwrap();
        response_tx
            .send(Frame::progress(
                req_id.clone(),
                0.5,
                "downloading file 5/10",
            ))
            .unwrap();
        response_tx
            .send(Frame::log(
                req_id.clone(),
                "status",
                "large file in progress",
            ))
            .unwrap();

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id.clone(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        // finish() must return immediately — NOT block waiting for StreamStart
        let mut response = peer.finish().await.expect("finish must succeed");

        // Handler must be able to recv() LOG frames right away
        let item1 = response.recv().await.expect("first LOG must arrive");
        match item1 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_progress(), Some(0.1));
                assert_eq!(f.log_message(), Some("downloading file 1/10"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        let item2 = response.recv().await.expect("second LOG must arrive");
        match item2 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_progress(), Some(0.5));
                assert_eq!(f.log_message(), Some("downloading file 5/10"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        let item3 = response.recv().await.expect("third LOG must arrive");
        match item3 {
            PeerResponseItem::Log(f) => {
                assert_eq!(f.log_message(), Some("large file in progress"));
            }
            PeerResponseItem::Data(..) => panic!("expected LOG frame, got Data"),
        }

        // Now send the actual data (StreamStart, Chunk, StreamEnd, End)
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        let raw_data = b"model output".to_vec();
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(raw_data.clone()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        // Data must arrive after the LOGs
        let item4 = response.recv().await.expect("data item must arrive");
        match item4 {
            PeerResponseItem::Data(Ok(value), _meta) => {
                assert_eq!(value, Value::Bytes(b"model output".to_vec()));
            }
            PeerResponseItem::Data(Err(e), _) => panic!("expected data, got error: {}", e),
            PeerResponseItem::Log(_) => panic!("expected Data, got LOG"),
        }

        assert!(
            response.recv().await.is_none(),
            "stream must end after STREAM_END"
        );
    }

    // TEST840: PeerResponse::collect_bytes discards LOG frames
    #[tokio::test]
    async fn test840_peer_response_collect_bytes_discards_logs() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        // LOG frame (should be discarded by collect_bytes)
        response_tx
            .send(Frame::progress(req_id.clone(), 0.25, "working"))
            .unwrap();
        response_tx
            .send(Frame::progress(req_id.clone(), 0.75, "almost"))
            .unwrap();

        // CHUNK
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Bytes(b"hello".to_vec()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // Another LOG
        response_tx
            .send(Frame::log(req_id.clone(), "info", "done"))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let response = peer.finish().await.expect("finish must succeed");
        let bytes = response
            .collect_bytes()
            .await
            .expect("collect must succeed");
        assert_eq!(
            bytes, b"hello",
            "collect_bytes must return only data, discarding all LOG frames"
        );
    }

    // TEST841: PeerResponse::collect_value discards LOG frames
    #[tokio::test]
    async fn test841_peer_response_collect_value_discards_logs() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded_channel();

        let req_id = MessageId::new_uuid();

        // STREAM_START
        let mut start = Frame::new(FrameType::StreamStart, req_id.clone());
        start.stream_id = Some("s1".to_string());
        start.media_urn = Some("media:binary".to_string());
        response_tx.send(start).unwrap();

        // LOG frames before the data value
        response_tx
            .send(Frame::progress(req_id.clone(), 0.5, "half"))
            .unwrap();
        response_tx
            .send(Frame::log(req_id.clone(), "debug", "processing"))
            .unwrap();

        // Single CHUNK with a CBOR integer
        let mut cbor_payload = Vec::new();
        ciborium::into_writer(&Value::Integer(42.into()), &mut cbor_payload).unwrap();
        let checksum = Frame::compute_checksum(&cbor_payload);
        response_tx
            .send(Frame::chunk(
                req_id.clone(),
                "s1".to_string(),
                0,
                cbor_payload,
                0,
                checksum,
            ))
            .unwrap();

        // STREAM_END
        response_tx
            .send(Frame::stream_end(req_id.clone(), "s1".to_string(), 1))
            .unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let response = peer.finish().await.expect("finish must succeed");
        let value = response
            .collect_value()
            .await
            .expect("collect must succeed");
        assert_eq!(
            value,
            Value::Integer(42.into()),
            "collect_value must skip LOG frames and return first data value"
        );
    }

    // ==================== find_stream / require_stream Tests ====================

    // TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds
    #[test]
    fn test678_find_stream_equivalent_urn_different_tag_order() {
        let streams = vec![(
            "media:json;record;llm-generation-request".to_string(),
            b"data".to_vec(),
            None,
        )];
        // Tags in different order — is_equivalent is order-independent
        let found = super::find_stream(&streams, "media:llm-generation-request;json;record");
        assert!(
            found.is_some(),
            "Same tags in different order must match via is_equivalent"
        );
        assert_eq!(found.unwrap(), b"data");
    }

    // TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict
    // This is the root cause of the cartridge_client.rs bug. Sender sent
    // "media:llm-generation-request" but receiver looked for
    // "media:llm-generation-request;json;record".
    #[test]
    fn test679_find_stream_base_urn_does_not_match_full_urn() {
        let streams = vec![(
            "media:llm-generation-request".to_string(),
            b"data".to_vec(),
            None,
        )];
        let found = super::find_stream(&streams, "media:llm-generation-request;json;record");
        assert!(
            found.is_none(),
            "Base URN without tags must NOT match full URN with tags"
        );
    }

    // TEST680: require_stream with missing URN returns hard StreamError
    #[test]
    fn test680_require_stream_missing_urn_returns_error() {
        let streams = vec![(
            "media:model-spec;textable".to_string(),
            b"gpt-4".to_vec(),
            None,
        )];
        let result = super::require_stream(&streams, "media:llm-generation-request;json;record");
        assert!(result.is_err(), "Missing stream must fail hard");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("media:llm-generation-request;json;record"),
            "Error must name the missing media URN, got: {}",
            err
        );
    }

    // TEST681: find_stream with multiple streams returns the correct one
    #[test]
    fn test681_find_stream_multiple_streams_returns_correct() {
        let streams = vec![
            (
                "media:model-spec;textable".to_string(),
                b"gpt-4".to_vec(),
                None,
            ),
            (
                "media:llm-generation-request;json;record".to_string(),
                b"{\"prompt\":\"test\"}".to_vec(),
                None,
            ),
            (
                "media:temperature;textable;numeric".to_string(),
                b"0.7".to_vec(),
                None,
            ),
        ];
        let found = super::find_stream(&streams, "media:llm-generation-request;json;record");
        assert!(found.is_some());
        assert_eq!(found.unwrap(), b"{\"prompt\":\"test\"}");
    }

    // TEST682: require_stream_str returns UTF-8 string for text data
    #[test]
    fn test682_require_stream_str_returns_utf8() {
        let streams = vec![("media:textable".to_string(), b"hello world".to_vec(), None)];
        let result = super::require_stream_str(&streams, "media:textable");
        assert_eq!(result.unwrap(), "hello world");
    }

    // TEST683: find_stream returns None for invalid media URN string (not a parse error — just None)
    #[test]
    fn test683_find_stream_invalid_urn_returns_none() {
        let streams = vec![("media:valid".to_string(), b"data".to_vec(), None)];
        // Empty string is not a valid media URN
        let found = super::find_stream(&streams, "");
        assert!(found.is_none(), "Invalid URN must return None, not panic");
    }

    // TEST842: run_with_keepalive returns closure result (fast operation, no keepalive frames)
    #[tokio::test]
    async fn test842_run_with_keepalive_returns_result() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        // Run a fast operation — no keepalive frame expected (interval is 30s)
        let result: i32 = stream
            .run_with_keepalive(0.25, "Loading model", || 42)
            .await;
        assert_eq!(result, 42, "Closure result must be returned");

        // No keepalive frame should have been emitted (operation was instant)
        let captured = frames.lock().unwrap();
        let progress_frames: Vec<_> = captured
            .iter()
            .filter(|f| f.frame_type == FrameType::Log)
            .collect();
        assert_eq!(
            progress_frames.len(),
            0,
            "No keepalive frame for instant operation"
        );
    }

    // TEST843: run_with_keepalive returns Ok/Err from closure
    #[tokio::test]
    async fn test843_run_with_keepalive_returns_result_type() {
        let (sender, _frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let result: Result<String, String> = stream
            .run_with_keepalive(0.5, "Loading", || Ok("model_loaded".to_string()))
            .await;
        assert_eq!(result.unwrap(), "model_loaded");
    }

    // TEST844: run_with_keepalive propagates errors from closure
    #[tokio::test]
    async fn test844_run_with_keepalive_propagates_error() {
        let (sender, _frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let result: Result<(), RuntimeError> = stream
            .run_with_keepalive(0.25, "Loading", || {
                Err(RuntimeError::Handler("load failed".to_string()))
            })
            .await;
        assert!(result.is_err(), "Error from closure must propagate");
        let err = result.unwrap_err();
        match err {
            RuntimeError::Handler(msg) => assert_eq!(msg, "load failed"),
            other => panic!("Expected Handler error, got: {:?}", other),
        }
    }

    // TEST845: ProgressSender emits progress and log frames independently of OutputStream
    #[test]
    fn test845_progress_sender_emits_frames() {
        let (sender, frames) = MockFrameSender::new();
        let stream = OutputStream::new(
            Arc::new(sender),
            "stream-1".to_string(),
            "media:test".to_string(),
            MessageId::new_uuid(),
            None,
            DEFAULT_MAX_CHUNK,
        );

        let ps = stream.progress_sender();
        ps.progress(0.5, "halfway there");
        ps.log("info", "loading complete");

        let captured = frames.lock().unwrap();
        assert_eq!(captured.len(), 2, "ProgressSender should emit 2 frames");
        assert_eq!(captured[0].frame_type, FrameType::Log);
        assert_eq!(captured[1].frame_type, FrameType::Log);
        // Verify progress frame has correct progress value
        assert_eq!(captured[0].log_progress(), Some(0.5));
        assert_eq!(captured[0].log_message(), Some("halfway there"));
        // Verify log frame
        assert_eq!(captured[1].log_level(), Some("info"));
        assert_eq!(captured[1].log_message(), Some("loading complete"));
    }

    /// Verify get_own_memory_mb returns non-zero values on macOS.
    /// This function calls proc_pid_rusage(getpid()) which must always work —
    /// even in a sandbox. If it returns None on macOS, the self-reporting
    /// mechanism is broken and cartridges will report 0 footprint.
    #[test]
    #[cfg(target_os = "macos")]
    // TEST1270: Runtime memory inspection returns non-negative resident and virtual memory values.
    fn test1270_get_own_memory_mb_returns_values() {
        let result = get_own_memory_mb();
        assert!(
            result.is_some(),
            "proc_pid_rusage(getpid()) must succeed on macOS"
        );
        let (footprint_mb, rss_mb) = result.unwrap();
        // A running test process should use at least some memory
        assert!(
            rss_mb > 0,
            "RSS should be non-zero for a running process, got {}",
            rss_mb
        );
        // Footprint should also be non-zero (it's the physical memory charged to us)
        assert!(
            footprint_mb > 0,
            "Footprint should be non-zero for a running process, got {}",
            footprint_mb
        );
    }
}
