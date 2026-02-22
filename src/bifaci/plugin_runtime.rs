//! Plugin Runtime - Unified I/O handling for plugin binaries
//!
//! The PluginRuntime provides a unified interface for plugin binaries to handle
//! cap invocations. Plugins register handlers for caps they provide, and the
//! runtime handles all I/O mechanics:
//!
//! - **Automatic mode detection**: CLI mode vs Plugin CBOR mode
//! - CBOR frame encoding/decoding (Plugin mode)
//! - CLI argument parsing from cap definitions (CLI mode)
//! - Handler routing by cap URN
//! - Real-time streaming response support
//! - HELLO handshake for limit negotiation
//! - **Multiplexed concurrent request handling**
//!
//! # Invocation Modes
//!
//! - **No CLI arguments**: Plugin CBOR mode - HELLO handshake, REQ/RES frames via stdin/stdout
//! - **Any CLI arguments**: CLI mode - parse args based on cap definitions
//!
//! # Example
//!
//! ```ignore
//! use capns::PluginRuntime;
//!
//! fn main() {
//!     let manifest = build_manifest(); // Your manifest with caps
//!     let mut runtime = PluginRuntime::new(manifest);
//!
//!     runtime.register::<MyRequest, _>("cap:op=my_op;...", |request, output, peer| {
//!         output.log("info", "Starting work...");
//!         output.emit_cbor(&ciborium::Value::Bytes(b"result".to_vec()))?;
//!         Ok(())
//!     });
//!
//!     // runtime.run() automatically detects CLI vs Plugin CBOR mode
//!     runtime.run().unwrap();
//! }
//! ```

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{handshake_accept, CborError, FrameReader, FrameWriter};
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::{ArgSource, Cap, CapArg};
use crate::urn::cap_urn::CapUrn;
use crate::bifaci::manifest::CapManifest;
use crate::urn::media_urn::{MediaUrn, MEDIA_FILE_PATH, MEDIA_FILE_PATH_ARRAY};
use crate::standard::caps::{CAP_IDENTITY, CAP_DISCARD};
use async_trait::async_trait;
use crossbeam_channel::{bounded, Receiver, Sender};
use ops::{Op, OpMetadata, DryContext, WetContext, OpResult, OpError};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

/// Errors that can occur in the plugin runtime
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
pub struct InputStream {
    media_urn: String,
    rx: Receiver<Result<ciborium::Value, StreamError>>,
}

impl InputStream {
    /// Media URN of this stream (from STREAM_START).
    pub fn media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Collect all chunks into a single byte vector.
    /// Extracts inner bytes from Value::Bytes/Text and concatenates.
    pub fn collect_bytes(self) -> Result<Vec<u8>, StreamError> {
        let mut result = Vec::new();
        for item in self {
            match item? {
                ciborium::Value::Bytes(b) => result.extend(b),
                ciborium::Value::Text(s) => result.extend(s.into_bytes()),
                other => {
                    // For non-byte types, CBOR-encode them
                    let mut buf = Vec::new();
                    ciborium::into_writer(&other, &mut buf)
                        .map_err(|e| StreamError::Decode(format!("Failed to encode CBOR: {}", e)))?;
                    result.extend(buf);
                }
            }
        }
        Ok(result)
    }

    /// Collect a single CBOR value (expects exactly one chunk).
    pub fn collect_value(mut self) -> Result<ciborium::Value, StreamError> {
        match self.next() {
            Some(Ok(value)) => Ok(value),
            Some(Err(e)) => Err(e),
            None => Err(StreamError::Closed),
        }
    }
}

impl Iterator for InputStream {
    type Item = Result<ciborium::Value, StreamError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.recv() {
            Ok(item) => Some(item),
            Err(_) => None, // Channel closed — stream ended
        }
    }
}

/// The bundle of all input arg streams for one request.
/// Yields InputStream objects as STREAM_START frames arrive from the wire.
/// Returns None after END frame (all args delivered).
pub struct InputPackage {
    rx: Receiver<Result<InputStream, StreamError>>,
    _demux_handle: Option<JoinHandle<()>>,
}

impl InputPackage {
    /// Get the next input stream. Blocks until STREAM_START or END.
    pub fn next_stream(&mut self) -> Option<Result<InputStream, StreamError>> {
        match self.rx.recv() {
            Ok(item) => Some(item),
            Err(_) => None, // Channel closed — all streams delivered
        }
    }

    /// Collect all streams' bytes into a single Vec<u8>.
    pub fn collect_all_bytes(self) -> Result<Vec<u8>, StreamError> {
        let mut all = Vec::new();
        for stream_result in self {
            let stream = stream_result?;
            all.extend(stream.collect_bytes()?);
        }
        Ok(all)
    }

    /// Collect each stream individually into a Vec of (media_urn, bytes) pairs.
    /// Each stream's bytes are accumulated separately — NOT concatenated.
    /// Use `find_stream()` helpers to retrieve args by URN pattern matching.
    pub fn collect_streams(self) -> Result<Vec<(String, Vec<u8>)>, StreamError> {
        let mut result = Vec::new();
        for stream_result in self {
            let stream = stream_result?;
            let urn = stream.media_urn().to_string();
            let bytes = stream.collect_bytes()?;
            result.push((urn, bytes));
        }
        Ok(result)
    }
}

/// Find a stream's bytes by exact URN equivalence.
///
/// Uses `MediaUrn::is_equivalent()` — matches only if both URNs have the
/// exact same tag set (order-independent). Both the caller and the plugin
/// know the arg media URNs from the cap definition, so this is always an
/// exact match — never a subsumption/pattern match.
///
/// The `media_urn` parameter must be the FULL media URN from the cap arg
/// definition (e.g., `"media:model-spec;textable;form=scalar"`).
pub fn find_stream<'a>(streams: &'a [(String, Vec<u8>)], media_urn: &str) -> Option<&'a [u8]> {
    let target = match crate::MediaUrn::from_string(media_urn) {
        Ok(p) => p,
        Err(_) => return None,
    };
    streams.iter().find_map(|(urn_str, bytes)| {
        let urn = crate::MediaUrn::from_string(urn_str).ok()?;
        if target.is_equivalent(&urn).unwrap_or(false) {
            Some(bytes.as_slice())
        } else {
            None
        }
    })
}

/// Like `find_stream` but returns a UTF-8 string.
pub fn find_stream_str(streams: &[(String, Vec<u8>)], media_urn: &str) -> Option<String> {
    find_stream(streams, media_urn).and_then(|b| String::from_utf8(b.to_vec()).ok())
}

/// Like `find_stream` but fails hard if not found.
pub fn require_stream<'a>(streams: &'a [(String, Vec<u8>)], media_urn: &str) -> Result<&'a [u8], StreamError> {
    find_stream(streams, media_urn).ok_or_else(|| StreamError::Protocol(
        format!("Missing required arg: {}", media_urn)
    ))
}

/// Like `require_stream` but returns a UTF-8 string.
pub fn require_stream_str(streams: &[(String, Vec<u8>)], media_urn: &str) -> Result<String, StreamError> {
    let bytes = require_stream(streams, media_urn)?;
    String::from_utf8(bytes.to_vec()).map_err(|e| StreamError::Decode(
        format!("Arg '{}' is not valid UTF-8: {}", media_urn, e)
    ))
}

impl Iterator for InputPackage {
    type Item = Result<InputStream, StreamError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_stream()
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
    stream_started: AtomicBool,
    chunk_index: Mutex<u64>,
    chunk_count: Mutex<u64>,
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
            stream_started: AtomicBool::new(false),
            chunk_index: Mutex::new(0),
            chunk_count: Mutex::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn ensure_started(&self) -> Result<(), RuntimeError> {
        if !self.stream_started.swap(true, Ordering::SeqCst) {
            let mut start_frame = Frame::stream_start(
                self.request_id.clone(),
                self.stream_id.clone(),
                self.media_urn.clone(),
            );
            start_frame.routing_id = self.routing_id.clone();
            self.sender.send(&start_frame)?;
        }
        Ok(())
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
    /// Auto-sends STREAM_START before first chunk.
    pub fn write(&self, data: &[u8]) -> Result<(), RuntimeError> {
        self.ensure_started()?;
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

    /// Emit a CBOR value. Handles Bytes/Text/Array/Map chunking.
    pub fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        self.ensure_started()?;
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
                        return Err(RuntimeError::Handler("Cannot split text on character boundary".to_string()));
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

    /// Close the output stream (sends STREAM_END). Idempotent.
    /// If stream was never started, sends STREAM_START first.
    pub fn close(&self) -> Result<(), RuntimeError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already closed
        }
        self.ensure_started()?;
        let chunk_count = {
            let count_guard = self.chunk_count.lock().unwrap();
            *count_guard
        };
        let mut frame = Frame::stream_end(
            self.request_id.clone(),
            self.stream_id.clone(),
            chunk_count,
        );
        frame.routing_id = self.routing_id.clone();
        self.sender.send(&frame)
    }
}

/// Handle for an in-progress peer invocation.
/// Handler creates arg streams with `arg()`, writes data, then calls `finish()`
/// to get the single response InputStream.
pub struct PeerCall {
    sender: Arc<dyn FrameSender>,
    request_id: MessageId,
    max_chunk: usize,
    response_rx: Option<Receiver<Frame>>,
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

    /// Finish sending args and get the response stream.
    /// Sends END for the peer request, spawns Demux on response channel.
    pub fn finish(mut self) -> Result<InputStream, RuntimeError> {
        // Send END frame for the peer request
        let end_frame = Frame::end(self.request_id.clone(), None);
        self.sender.send(&end_frame)?;

        // Take the response receiver
        let response_rx = self.response_rx.take()
            .ok_or_else(|| RuntimeError::PeerRequest("PeerCall already finished".to_string()))?;

        // Spawn single-stream Demux for the response
        let input_stream = demux_single_stream(response_rx)?;
        Ok(input_stream)
    }
}

/// Allows handlers to invoke caps on the peer (host).
///
/// This trait enables bidirectional communication where a plugin handler can
/// invoke caps on the host while processing a request.
///
/// The `call` method starts a peer invocation and returns a `PeerCall`.
/// The handler creates arg streams with `call.arg()`, writes data, then
/// calls `call.finish()` to get the single response `InputStream`.
pub trait PeerInvoker: Send + Sync {
    /// Start a peer call. Sends REQ, registers response channel.
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError>;

    /// Convenience: open call, write each arg's bytes, finish, return response.
    fn call_with_bytes(&self, cap_urn: &str, args: &[(&str, &[u8])]) -> Result<InputStream, RuntimeError> {
        let call = self.call(cap_urn)?;
        for &(media_urn, data) in args {
            let arg = call.arg(media_urn);
            arg.write(data)?;
            arg.close()?;
        }
        call.finish()
    }
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported (e.g., CLI mode).
pub struct NoPeerInvoker;

impl PeerInvoker for NoPeerInvoker {
    fn call(&self, _cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        Err(RuntimeError::PeerRequest(
            "Peer invocation not supported in this context".to_string(),
        ))
    }
}

/// Channel-based frame sender for plugin output.
/// ALL frames (peer requests AND responses) go through a single output channel.
/// PluginRuntime has a writer thread that drains this channel and writes to stdout.
struct ChannelFrameSender {
    tx: crossbeam_channel::Sender<Frame>,
}

impl FrameSender for ChannelFrameSender {
    fn send(&self, frame: &Frame) -> Result<(), RuntimeError> {
        self.tx.send(frame.clone())
            .map_err(|_| RuntimeError::Handler("Output channel closed".to_string()))
    }
}


/// CLI-mode emitter that writes directly to stdout.
/// Used when the plugin is invoked via CLI (with arguments).
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
                            if let Some(val) = map.iter().find(|(k, _)| {
                                matches!(k, ciborium::Value::Text(s) if s == "value")
                            }).map(|(_, v)| v) {
                                match val {
                                    ciborium::Value::Bytes(bytes) => {
                                        let _ = handle.write_all(bytes);
                                    }
                                    ciborium::Value::Text(text) => {
                                        let _ = handle.write_all(text.as_bytes());
                                    }
                                    _ => return Err(RuntimeError::Handler("Map 'value' field is not bytes/text".to_string())),
                                }
                            } else {
                                return Err(RuntimeError::Handler("Map in array has no 'value' field".to_string()));
                            }
                        }
                        _ => {
                            return Err(RuntimeError::Handler("Array contains unsupported element type".to_string()));
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
                if let Some(val) = map.iter().find(|(k, _)| {
                    matches!(k, ciborium::Value::Text(s) if s == "value")
                }).map(|(_, v)| v) {
                    match val {
                        ciborium::Value::Bytes(bytes) => {
                            let _ = handle.write_all(bytes);
                        }
                        ciborium::Value::Text(text) => {
                            let _ = handle.write_all(text.as_bytes());
                        }
                        _ => return Err(RuntimeError::Handler("Map 'value' field is not bytes/text".to_string())),
                    }
                } else {
                    return Err(RuntimeError::Handler("Map has no 'value' field".to_string()));
                }
            }
            _ => {
                return Err(RuntimeError::Handler("Handler emitted unsupported CBOR type".to_string()));
            }
        }

        if self.ndjson {
            let _ = handle.write_all(b"\n");
        }
        let _ = handle.flush();
        Ok(())
    }

    fn emit_log(&self, _level: &str, _message: &str) {
        // In CLI mode, logs go to stderr (currently disabled)
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
                            expected_checksum, actual_checksum, payload.len()
                        )));
                    }

                    // Decode CBOR payload
                    let value: ciborium::Value = ciborium::from_reader(&payload[..])
                        .map_err(|e| RuntimeError::Handler(format!("Failed to decode CBOR payload: {}", e)))?;

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
                Err(RuntimeError::Handler(format!("Unexpected frame type in CLI mode: {:?}", frame.frame_type)))
            }
        }
    }
}

// =============================================================================
// OP-BASED HANDLER SYSTEM — handlers implement ops::Op<()>
// =============================================================================

/// Bundles capns I/O for WetContext. Op handlers extract this from WetContext
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
        self.input.lock().unwrap().take().ok_or_else(|| {
            RuntimeError::Handler("Input already consumed".to_string())
        })
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
        let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let input = req.take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        for stream_result in input {
            let stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Identity input error: {}", e)))?;
            for chunk_result in stream {
                let chunk = chunk_result
                    .map_err(|e| OpError::ExecutionFailed(format!("Identity chunk error: {}", e)))?;
                req.output().emit_cbor(&chunk)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            }
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
        let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let input = req.take_input()
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        for stream_result in input {
            let stream = stream_result
                .map_err(|e| OpError::ExecutionFailed(format!("Discard input error: {}", e)))?;
            for chunk_result in stream {
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

/// Tracks a pending peer request (plugin invoking host cap).
/// The reader loop forwards response frames to the channel.
/// LOG frames are re-stamped with the origin request ID and forwarded
/// back to the host automatically (no handler involvement).
struct PendingPeerRequest {
    sender: Sender<Frame>,
    origin_request_id: MessageId,
    origin_routing_id: Option<MessageId>,
}

/// Implementation of PeerInvoker that sends REQ frames to the host.
struct PeerInvokerImpl {
    output_tx: crossbeam_channel::Sender<Frame>,
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
/// `is_cli_mode`: true if CLI mode (args from command line), false if CBOR mode (plugin protocol)
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

    // Build map of arg media_urn → stdin source media_urn for file-path conversion
    let mut arg_to_stdin: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for arg_def in cap.get_args() {
        if let Some(stdin_urn) = arg_def.sources.iter().find_map(|s| match s {
            ArgSource::Stdin { stdin } => Some(stdin.clone()),
            _ => None,
        }) {
            arg_to_stdin.insert(arg_def.media_urn.clone(), stdin_urn);
        }
    }

    // Parse the CBOR payload as an array of argument maps
    let cbor_value: ciborium::Value = ciborium::from_reader(payload).map_err(|e| {
        RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e))
    })?;

    let mut arguments = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR arguments must be an array".to_string(),
            ));
        }
    };

    // File-path auto-conversion: If arg is media:file-path, read file(s)
    // Pattern hierarchy:
    // - media:file-path (base) accepts both scalar and list
    // - media:file-path;form=scalar (single file)
    // - media:file-path;form=list (array of files)
    let file_path_base = MediaUrn::from_string("media:file-path")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path base pattern: {}", e)))?;
    let file_path_scalar = MediaUrn::from_string("media:file-path;form=scalar")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path scalar pattern: {}", e)))?;
    let file_path_list = MediaUrn::from_string("media:file-path;form=list")
        .map_err(|e| RuntimeError::Handler(format!("Invalid file-path list pattern: {}", e)))?;

    for arg in arguments.iter_mut() {
        if let ciborium::Value::Map(ref mut arg_map) = arg {
            let mut media_urn: Option<String> = None;
            let mut value_ref: Option<&ciborium::Value> = None;

            // Extract media_urn and value (preserve CBOR Value type)
            for (k, v) in arg_map.iter() {
                if let ciborium::Value::Text(key) = k {
                    match key.as_str() {
                        "media_urn" => {
                            if let ciborium::Value::Text(s) = v {
                                media_urn = Some(s.clone());
                            }
                        }
                        "value" => {
                            value_ref = Some(v);
                        }
                        _ => {}
                    }
                }
            }

            // Check if this is a file-path argument using pattern matching
            if let (Some(ref urn_str), Some(value)) = (media_urn, value_ref) {
                let arg_urn = MediaUrn::from_string(urn_str)
                    .map_err(|e| RuntimeError::Handler(format!("Invalid argument media URN '{}': {}", urn_str, e)))?;

                // Check if it's a file-path using pattern matching (pattern accepts instance)
                let is_file_path = file_path_base.accepts(&arg_urn)
                    .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;

                if is_file_path {
                    // Determine if it's scalar or list using specific patterns
                    let is_scalar = file_path_scalar.accepts(&arg_urn)
                        .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;
                    let is_list = file_path_list.accepts(&arg_urn)
                        .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;

                    // Hard failure if neither scalar nor list
                    if !is_scalar && !is_list {
                        return Err(RuntimeError::Handler(format!(
                            "File-path argument '{}' must be either form=scalar or form=list",
                            urn_str
                        )));
                    }

                    // Hard failure if both (should never happen with proper URN matching)
                    if is_scalar && is_list {
                        return Err(RuntimeError::Handler(format!(
                            "File-path argument '{}' cannot be both scalar and list",
                            urn_str
                        )));
                    }

                    // Read file(s) and replace value
                    if is_scalar {
                        // Single file - value must be Bytes or Text (not Array)
                        let path_bytes = match value {
                            ciborium::Value::Bytes(b) => b.clone(),
                            ciborium::Value::Text(t) => t.as_bytes().to_vec(),
                            ciborium::Value::Array(_) => {
                                return Err(RuntimeError::Handler(format!(
                                    "File-path scalar cannot be an Array - got Array for '{}'",
                                    urn_str
                                )));
                            }
                            _ => {
                                return Err(RuntimeError::Handler(format!(
                                    "File-path scalar must be Bytes or Text - got unexpected type for '{}'",
                                    urn_str
                                )));
                            }
                        };

                        let path_str = String::from_utf8_lossy(&path_bytes);
                        let file_bytes = std::fs::read(path_str.as_ref())
                            .map_err(|e| RuntimeError::Handler(format!("Failed to read file '{}': {}", path_str, e)))?;

                        // Find target media_urn from arg_to_stdin map
                        let target_urn = arg_to_stdin.get(urn_str)
                            .cloned()
                            .unwrap_or_else(|| expected_input.clone());

                        // Replace value with file contents AND media_urn with target
                        for (k, v) in arg_map.iter_mut() {
                            if let ciborium::Value::Text(key) = k {
                                if key == "value" {
                                    *v = ciborium::Value::Bytes(file_bytes.clone());
                                }
                                if key == "media_urn" {
                                    *v = ciborium::Value::Text(target_urn.clone());
                                }
                            }
                        }
                    } else {
                        // Array of files - mode-dependent logic
                        let paths_to_process: Vec<String> = match value {
                            ciborium::Value::Array(arr) => {
                                // CBOR Array - ONLY allowed in CBOR mode (NOT CLI mode)
                                if is_cli_mode {
                                    return Err(RuntimeError::Handler(format!(
                                        "File-path array cannot be CBOR Array in CLI mode - got Array for '{}'",
                                        urn_str
                                    )));
                                }

                                // CBOR mode - extract each path from array
                                let mut paths = Vec::new();
                                for item in arr {
                                    match item {
                                        ciborium::Value::Text(s) => paths.push(s.clone()),
                                        ciborium::Value::Bytes(b) => paths.push(String::from_utf8_lossy(b).to_string()),
                                        _ => return Err(RuntimeError::Handler(
                                            "CBOR array must contain text or bytes paths".to_string()
                                        )),
                                    }
                                }
                                paths
                            }
                            ciborium::Value::Bytes(b) => {
                                // Bytes - treat as text glob/literal path
                                vec![String::from_utf8_lossy(b).to_string()]
                            }
                            ciborium::Value::Text(t) => {
                                // Text - treat as glob/literal path
                                vec![t.clone()]
                            }
                            _ => {
                                return Err(RuntimeError::Handler(format!(
                                    "File-path list must be Bytes, Text, or Array (CBOR mode only) - got unexpected type for '{}'",
                                    urn_str
                                )));
                            }
                        };

                        let mut all_files = Vec::new();

                        // Process each path (could be glob pattern or literal)
                        for path_str in paths_to_process {
                            // Detect glob pattern
                            let is_glob = path_str.contains('*') || path_str.contains('?') || path_str.contains('[');

                        if is_glob {
                            // Expand glob pattern
                            let paths = glob::glob(&path_str)
                                .map_err(|e| RuntimeError::Handler(format!(
                                    "Invalid glob pattern '{}': {}",
                                    path_str, e
                                )))?;

                            for path_result in paths {
                                let path = path_result
                                    .map_err(|e| RuntimeError::Handler(format!("Glob error: {}", e)))?;

                                // Only include files (skip directories)
                                if path.is_file() {
                                    all_files.push(path);
                                }
                            }

                            if all_files.is_empty() {
                                return Err(RuntimeError::Handler(format!(
                                    "No files matched glob pattern '{}'",
                                    path_str
                                )));
                            }
                        } else {
                            // Literal path - verify it exists
                            let path = std::path::Path::new(&path_str);
                            if !path.exists() {
                                return Err(RuntimeError::Handler(format!(
                                    "File not found: '{}'",
                                    path_str
                                )));
                            }
                            if path.is_file() {
                                all_files.push(path.to_path_buf());
                            } else {
                                return Err(RuntimeError::Handler(format!(
                                    "Path is not a file: '{}'",
                                    path_str
                                )));
                            }
                        }
                        }  // End for path_str in paths_to_process

                        // Read all files
                        let mut files_data = Vec::new();
                        for path in &all_files {
                            let bytes = std::fs::read(path)
                                .map_err(|e| RuntimeError::Handler(format!(
                                    "Failed to read file '{}': {}",
                                    path.display(), e
                                )))?;
                            files_data.push(ciborium::Value::Bytes(bytes));
                        }

                        // Find target media_urn from arg_to_stdin map
                        let target_urn = arg_to_stdin.get(urn_str)
                            .cloned()
                            .unwrap_or_else(|| expected_input.clone());

                        // Store as CBOR Array directly (NOT double-encoded as bytes)
                        let cbor_array = ciborium::Value::Array(files_data);

                        // Replace value with CBOR array AND media_urn with target
                        for (k, v) in arg_map.iter_mut() {
                            if let ciborium::Value::Text(key) = k {
                                if key == "value" {
                                    *v = cbor_array.clone();
                                }
                                if key == "media_urn" {
                                    *v = ciborium::Value::Text(target_urn.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Validate: At least ONE argument must match in_spec (fail hard if none)
    // UNLESS in_spec is "media:void" (no input required)
    // After file-path conversion, arg media_urn may be the stdin target (e.g., "media:")
    // rather than the original in_spec (e.g., "media:file-path;..."), so we also accept
    // any stdin source target as a valid match.
    let is_void_input = expected_input == "media:void";

    if !is_void_input {
        // Collect all valid target URNs: in_spec + all stdin source targets
        let mut valid_targets: Vec<MediaUrn> = Vec::new();
        if let Some(ref expected) = expected_media_urn {
            valid_targets.push(expected.clone());
        }
        for stdin_urn_str in arg_to_stdin.values() {
            if let Ok(stdin_urn) = MediaUrn::from_string(stdin_urn_str) {
                valid_targets.push(stdin_urn);
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
                                    let fwd = arg_urn.conforms_to(target).unwrap_or(false);
                                    let rev = target.accepts(&arg_urn).unwrap_or(false);
                                    if fwd || rev {
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
    ciborium::into_writer(&modified_cbor, &mut serialized)
        .map_err(|e| RuntimeError::Serialize(format!("Failed to serialize modified CBOR: {}", e)))?;

    Ok(serialized)
}

impl PeerInvoker for PeerInvokerImpl {
    fn call(&self, cap_urn: &str) -> Result<PeerCall, RuntimeError> {
        let request_id = MessageId::new_uuid();

        // Create channel for response frames
        let (sender, receiver) = bounded(64);

        // Register pending request before sending REQ
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(request_id.clone(), PendingPeerRequest {
                sender,
                origin_request_id: self.origin_request_id.clone(),
                origin_routing_id: self.origin_routing_id.clone(),
            });
        }

        // Send REQ with empty payload
        let req_frame = Frame::req(
            request_id.clone(),
            cap_urn,
            vec![],
            "application/cbor",
        );
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
    file_path_scalar: MediaUrn,
    file_path_list: MediaUrn,
    cap_urn: String,
    manifest: Option<CapManifest>,
}

impl FilePathContext {
    fn new(cap_urn: &str, manifest: Option<CapManifest>) -> Result<Self, RuntimeError> {
        Ok(Self {
            file_path_pattern: MediaUrn::from_string("media:file-path")
                .map_err(|e| RuntimeError::Handler(format!("Failed to create file-path pattern: {}", e)))?,
            file_path_scalar: MediaUrn::from_string("media:file-path;form=scalar")
                .map_err(|e| RuntimeError::Handler(format!("Failed to create file-path;form=scalar pattern: {}", e)))?,
            file_path_list: MediaUrn::from_string("media:file-path;form=list")
                .map_err(|e| RuntimeError::Handler(format!("Failed to create file-path;form=list pattern: {}", e)))?,
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

    fn is_scalar(&self, media_urn_str: &str) -> bool {
        let arg_urn = match MediaUrn::from_string(media_urn_str) {
            Ok(u) => u,
            Err(_) => return false,
        };
        self.file_path_scalar.accepts(&arg_urn).unwrap_or(false)
    }

    /// Given the media URN of an incoming file-path stream, find the matching
    /// arg in the cap definition and return its stdin source URN.
    /// Uses is_equivalent (not string comparison) to match the arg.
    fn resolve_stdin_urn(&self, file_path_media_urn: &str) -> Option<String> {
        let manifest = self.manifest.as_ref()?;
        let cap_def = manifest.caps.iter().find(|c| c.urn.to_string() == self.cap_urn)?;
        let incoming = crate::MediaUrn::from_string(file_path_media_urn).ok()?;
        let arg_def = cap_def.args.iter().find(|a| {
            crate::MediaUrn::from_string(&a.media_urn)
                .map(|arg_urn| arg_urn.is_equivalent(&incoming).unwrap_or(false))
                .unwrap_or(false)
        })?;
        arg_def.sources.iter().find_map(|s| {
            if let ArgSource::Stdin { stdin } = s {
                Some(stdin.clone())
            } else {
                None
            }
        })
    }
}

/// Demux for multi-stream mode (handler input).
/// Spawns a background thread that reads raw Frame channel and splits into
/// per-stream InputStream channels. Handles file-path interception.
fn demux_multi_stream(
    raw_rx: Receiver<Frame>,
    file_path_ctx: Option<FilePathContext>,
) -> InputPackage {
    let (streams_tx, streams_rx) = crossbeam_channel::bounded(16);

    let handle = thread::spawn(move || {
        // Per-stream channels: stream_id → chunk sender
        let mut stream_channels: HashMap<String, Sender<Result<ciborium::Value, StreamError>>> = HashMap::new();
        // File-path accumulators: stream_id → (media_urn, accumulated_chunk_payloads)
        let mut fp_accumulators: HashMap<String, (String, Vec<Vec<u8>>)> = HashMap::new();

        for frame in raw_rx {
            match frame.frame_type {
                FrameType::StreamStart => {
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let _ = streams_tx.send(Err(StreamError::Protocol("STREAM_START missing stream_id".into())));
                            break;
                        }
                    };
                    let media_urn = frame.media_urn.as_ref().cloned().unwrap_or_default();

                    // Check if file-path (only when FilePathContext provided)
                    let is_fp = file_path_ctx.as_ref()
                        .map_or(false, |ctx| ctx.is_file_path(&media_urn));

                    if is_fp {
                        fp_accumulators.insert(stream_id, (media_urn, Vec::new()));
                    } else {
                        let (chunk_tx, chunk_rx) = crossbeam_channel::bounded(64);
                        stream_channels.insert(stream_id.clone(), chunk_tx);
                        let input_stream = InputStream {
                            media_urn,
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

                    // Regular stream — decode CBOR and forward
                    if let Some(tx) = stream_channels.get(&stream_id) {
                        if let Some(payload) = frame.payload {
                            // Checksum validation (MANDATORY in protocol v2)
                            let expected_checksum = match frame.checksum {
                                Some(c) => c,
                                None => {
                                    let _ = tx.send(Err(StreamError::Protocol(
                                        "CHUNK frame missing required checksum field".to_string()
                                    )));
                                    continue;
                                }
                            };
                            let actual = Frame::compute_checksum(&payload);
                            if actual != expected_checksum {
                                let _ = tx.send(Err(StreamError::Protocol(
                                    format!("Checksum mismatch: expected={}, actual={}", expected_checksum, actual)
                                )));
                                continue;
                            }
                            match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                                Ok(value) => { let _ = tx.send(Ok(value)); }
                                Err(e) => { let _ = tx.send(Err(StreamError::Decode(e.to_string()))); }
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

                        // If the arg has a stdin source, read the file and relabel.
                        // If not, pass through the file path as a plain value (no file reading).
                        if let Some(resolved_urn) = ctx.resolve_stdin_urn(&media_urn) {
                            let is_scalar = ctx.is_scalar(&media_urn);
                            if is_scalar {
                                let path_str = String::from_utf8_lossy(&path_bytes);
                                match std::fs::read(path_str.as_ref()) {
                                    Ok(file_bytes) => {
                                        let (chunk_tx, chunk_rx) = crossbeam_channel::bounded(64);
                                        let _ = chunk_tx.send(Ok(ciborium::Value::Bytes(file_bytes)));
                                        drop(chunk_tx);
                                        let input_stream = InputStream {
                                            media_urn: resolved_urn,
                                            rx: chunk_rx,
                                        };
                                        if streams_tx.send(Ok(input_stream)).is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = streams_tx.send(Err(StreamError::Io(
                                            format!("Failed to read file '{}': {}", path_str, e)
                                        )));
                                        break;
                                    }
                                }
                            } else {
                                // form=list — not yet implemented in CBOR mode
                                let _ = streams_tx.send(Err(StreamError::Protocol(
                                    "File-path form=list conversion not yet implemented in CBOR mode".into()
                                )));
                                break;
                            }
                        } else {
                            // No stdin source — pass through the path bytes as-is
                            let (chunk_tx, chunk_rx) = crossbeam_channel::bounded(64);
                            let _ = chunk_tx.send(Ok(ciborium::Value::Bytes(path_bytes)));
                            drop(chunk_tx);
                            let input_stream = InputStream {
                                media_urn: media_urn.clone(),
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
/// Reads frames from channel expecting a single stream. Returns InputStream.
fn demux_single_stream(raw_rx: Receiver<Frame>) -> Result<InputStream, RuntimeError> {
    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded(64);
    let (meta_tx, meta_rx) = crossbeam_channel::bounded::<String>(1);

    thread::spawn(move || {
        let mut media_urn_sent = false;
        for frame in raw_rx {
            match frame.frame_type {
                FrameType::StreamStart => {
                    if !media_urn_sent {
                        let urn = frame.media_urn.as_ref().cloned().unwrap_or_else(|| "*".to_string());
                        let _ = meta_tx.send(urn);
                        media_urn_sent = true;
                    }
                }
                FrameType::Chunk => {
                    if let Some(payload) = frame.payload {
                        // Checksum validation (MANDATORY in protocol v2)
                        let expected_checksum = match frame.checksum {
                            Some(c) => c,
                            None => {
                                let _ = chunk_tx.send(Err(StreamError::Protocol(
                                    "CHUNK frame missing required checksum field".to_string()
                                )));
                                continue;
                            }
                        };
                        let actual = Frame::compute_checksum(&payload);
                        if actual != expected_checksum {
                            let _ = chunk_tx.send(Err(StreamError::Protocol(
                                format!("Checksum mismatch: expected={}, actual={}", expected_checksum, actual)
                            )));
                            continue;
                        }
                        match ciborium::from_reader::<ciborium::Value, _>(&payload[..]) {
                            Ok(value) => { let _ = chunk_tx.send(Ok(value)); }
                            Err(e) => { let _ = chunk_tx.send(Err(StreamError::Decode(e.to_string()))); }
                        }
                    }
                }
                FrameType::StreamEnd | FrameType::End => {
                    break;
                }
                FrameType::Err => {
                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                    let _ = chunk_tx.send(Err(StreamError::RemoteError { code, message }));
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for media_urn from the first STREAM_START
    let media_urn = meta_rx.recv().unwrap_or_else(|_| "*".to_string());

    Ok(InputStream {
        media_urn,
        rx: chunk_rx,
    })
}

// =============================================================================
// ACTIVE REQUEST TRACKING
// =============================================================================

/// Tracks an active incoming request. Reader loop routes frames here.
struct ActiveRequest {
    raw_tx: crossbeam_channel::Sender<Frame>,
}

/// The plugin runtime that handles all I/O for plugin binaries.
///
/// Plugins create a runtime with their manifest, register handlers for their caps,
/// then call `run()` to process requests.
///
/// The manifest is REQUIRED - plugins MUST provide their manifest which is sent
/// in the HELLO response during handshake. This is the ONLY way for plugins to
/// communicate their capabilities to the host.
///
/// **Invocation Modes**:
/// - No CLI args: Plugin CBOR mode (stdin/stdout binary frames)
/// - Any CLI args: CLI mode (parse args from cap definitions)
///
/// **Multiplexed execution** (CBOR mode): Multiple requests can be processed concurrently.
/// Each request handler runs in its own thread, allowing the runtime to:
/// - Respond to heartbeats while handlers are running
/// - Accept new requests while previous ones are still processing
/// - Handle multiple concurrent cap invocations
pub struct PluginRuntime {
    /// Registered Op factories by cap URN pattern
    handlers: HashMap<String, OpFactory>,

    /// Plugin manifest JSON data - sent in HELLO response.
    /// This is REQUIRED - plugins must provide their manifest.
    manifest_data: Vec<u8>,

    /// Parsed manifest for CLI mode processing
    manifest: Option<CapManifest>,

    /// Negotiated protocol limits
    limits: Limits,
}

/// Dispatch an Op with a Request via WetContext. Bridges sync handler threads to async Op::perform.
/// Closes the output stream on success (sends STREAM_END if stream was started).
fn dispatch_op(
    op: Box<dyn Op<()>>,
    input: InputPackage,
    output: OutputStream,
    peer: Arc<dyn PeerInvoker>,
) -> Result<(), RuntimeError> {
    let req = Arc::new(Request::new(input, output, peer));
    let mut dry = DryContext::new();
    let mut wet = WetContext::new();
    wet.insert_arc(WET_KEY_REQUEST, req.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| RuntimeError::Handler(format!("Runtime: {}", e)))?;
    let result = rt.block_on(op.perform(&mut dry, &mut wet))
        .map_err(|e| RuntimeError::Handler(e.to_string()));

    if result.is_ok() {
        let _ = req.output().close();
    }
    result
}

impl PluginRuntime {
    /// Create a new plugin runtime with the required manifest.
    ///
    /// The manifest is JSON-encoded plugin metadata including:
    /// - name: Plugin name
    /// - version: Plugin version
    /// - caps: Array of capability definitions with args and sources
    ///
    /// This manifest is sent in the HELLO response to the host (CBOR mode)
    /// and used for CLI argument parsing (CLI mode).
    /// **Plugins MUST provide a manifest - there is no fallback.**
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - plugins must declare it explicitly.
    pub fn new(manifest: &[u8]) -> Self {
        // Try to parse the manifest for CLI mode support
        let parsed_manifest = serde_json::from_slice::<CapManifest>(manifest).ok();

        // Validate manifest if parseable
        let (manifest_data, parsed_manifest) = match parsed_manifest {
            Some(m) => {
                // FAIL HARD if manifest doesn't have CAP_IDENTITY
                m.validate().expect("Manifest validation failed - plugin MUST declare CAP_IDENTITY");
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
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new plugin runtime with a pre-built CapManifest.
    /// This is the preferred method as it ensures the manifest is valid.
    ///
    /// Auto-registers standard handlers (identity, discard).
    /// **PANICS** if manifest is missing CAP_IDENTITY - plugins must declare it explicitly.
    pub fn with_manifest(manifest: CapManifest) -> Self {
        // FAIL HARD if manifest doesn't have CAP_IDENTITY
        manifest.validate().expect("Manifest validation failed - plugin MUST declare CAP_IDENTITY");

        let manifest_data = serde_json::to_vec(&manifest).unwrap_or_default();
        let mut rt = Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: Some(manifest),
            limits: Limits::default(),
        };
        rt.register_standard_caps();
        rt
    }

    /// Create a new plugin runtime with manifest JSON string.
    ///
    /// Auto-registers standard handlers (identity, discard) and ensures
    /// CAP_IDENTITY is present in the manifest.
    pub fn with_manifest_json(manifest_json: &str) -> Self {
        Self::new(manifest_json.as_bytes())
    }

    /// Register the standard identity and discard handlers.
    /// Plugin authors can override either by calling register_op() after construction.
    fn register_standard_caps(&mut self) {
        if self.find_handler(CAP_IDENTITY).is_none() {
            self.register_op_type::<IdentityOp>(CAP_IDENTITY);
        }
        if self.find_handler(CAP_DISCARD).is_none() {
            self.register_op_type::<DiscardOp>(CAP_DISCARD);
        }
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
        self.handlers.insert(cap_urn.to_string(), Arc::new(|| Box::new(T::default()) as Box<dyn Op<()>>));
    }

    /// Find a handler for a cap URN.
    /// Returns the OpFactory if found, None otherwise.
    ///
    /// Matching direction: request is pattern, registered cap is instance.
    /// `request.accepts(registered_cap)` — the request must accept the registered cap,
    /// meaning the registered cap must be able to satisfy what the request asks for.
    pub fn find_handler(&self, cap_urn: &str) -> Option<OpFactory> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();
        let mut best: Option<(OpFactory, usize)> = None;

        for (registered_cap_str, handler) in &self.handlers {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap_str) {
                if request_urn.accepts(&registered_urn) {
                    let specificity = registered_urn.specificity();
                    let distance = (specificity as isize - request_specificity as isize).unsigned_abs();
                    match &best {
                        None => best = Some((Arc::clone(handler), distance)),
                        Some((_, best_distance)) if distance < *best_distance => {
                            best = Some((Arc::clone(handler), distance));
                        }
                        _ => {}
                    }
                }
            }
        }

        best.map(|(handler, _)| handler)
    }

    /// Run the plugin runtime.
    ///
    /// **Mode Detection**:
    /// - No CLI arguments: Plugin CBOR mode (stdin/stdout binary frames)
    /// - Any CLI arguments: CLI mode (parse args from cap definitions)
    ///
    /// **CLI Mode**:
    /// - `manifest` subcommand: output manifest JSON
    /// - `<op>` subcommand: find cap by op tag, parse args, invoke handler
    /// - `--help`: show available subcommands
    ///
    /// **Plugin CBOR Mode** (no CLI args):
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
    pub fn run(&self) -> Result<(), RuntimeError> {
        let args: Vec<String> = std::env::args().collect();

        // No CLI arguments at all → Plugin CBOR mode
        if args.len() == 1 {
            return self.run_cbor_mode();
        }

        // Any CLI arguments → CLI mode
        self.run_cli_mode(&args)
    }


    /// Run in CLI mode - parse arguments and invoke handler.
    ///
    /// If stdin is piped (binary data), this streams it in chunks and accumulates.
    /// All modes converge: CLI args and stdin data are sent as CBOR frame streams
    /// through InputPackage, so handlers see the same API regardless of mode.
    fn run_cli_mode(&self, args: &[String]) -> Result<(), RuntimeError> {
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
        let cap = self.find_cap_by_command(manifest, subcommand).ok_or_else(|| {
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
        let payload = if !cli_args.is_empty() {
            // ARGUMENT PATH: Build from CLI arguments (may include file paths)
            // File-path auto-conversion happens in extract_effective_payload
            let raw_payload = self.build_payload_from_cli(&cap, cli_args)?;
            extract_effective_payload(
                &raw_payload,
                Some("application/cbor"),
                &cap,
                true,  // CLI mode
            )?
        } else if stdin_is_piped && cap_accepts_stdin {
            // STREAMING PATH: No args, read stdin in chunks and accumulate
            self.build_payload_from_streaming_stdin(&cap)?
        } else {
            // No input provided
            return Err(RuntimeError::MissingArgument(
                "No input provided (expected CLI arguments or piped stdin)".to_string()
            ));
        };

        // Create CLI-mode frame sender and no-op peer invoker
        let cli_emitter = CliStreamEmitter::without_ndjson();
        let frame_sender = CliFrameSender::with_emitter(cli_emitter);
        let peer = NoPeerInvoker;

        // STREAM MULTIPLEXING: Parse CBOR arguments and create separate streams
        // The payload from extract_effective_payload is a CBOR array of argument maps
        let cbor_value: ciborium::Value = ciborium::from_reader(&payload[..])
            .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR arguments: {}", e)))?;

        let arguments = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => return Err(RuntimeError::Deserialize("CBOR arguments must be an array".to_string())),
        };

        // Create channel and send each argument as separate Frame streams
        let (tx, rx) = crossbeam_channel::unbounded();
        let max_chunk = Limits::default().max_chunk;
        let request_id = MessageId::new_uuid(); // Dummy request ID for CLI mode

        for arg in arguments {
            if let ciborium::Value::Map(arg_map) = arg {
                let mut media_urn: Option<String> = None;
                let mut value_bytes: Option<Vec<u8>> = None;

                // Extract media_urn and value
                for (k, v) in arg_map {
                    if let ciborium::Value::Text(key) = k {
                        match key.as_str() {
                            "media_urn" => {
                                if let ciborium::Value::Text(s) = v {
                                    media_urn = Some(s);
                                }
                            }
                            "value" => {
                                // ALL values must be CBOR-encoded before sending as CHUNK payloads
                                // Protocol: CHUNK payloads contain CBOR-encoded data (encode once, no double-wrapping)
                                let mut cbor_bytes = Vec::new();
                                ciborium::into_writer(&v, &mut cbor_bytes)
                                    .map_err(|e| RuntimeError::Serialize(format!("Failed to encode value: {}", e)))?;
                                value_bytes = Some(cbor_bytes);
                            }
                            _ => {}
                        }
                    }
                }

                // Send this argument as a CBOR frame stream
                if let (Some(urn), Some(bytes)) = (media_urn, value_bytes) {
                    let stream_id = uuid::Uuid::new_v4().to_string();

                    // Send STREAM_START
                    let start_frame = Frame::stream_start(request_id.clone(), stream_id.clone(), urn.clone());
                    tx.send(start_frame).map_err(|_| RuntimeError::Handler("Failed to send STREAM_START".to_string()))?;

                    // Send CHUNK frame(s)
                    let chunk_count = if bytes.is_empty() {
                        // Empty value - send single empty chunk
                        let checksum = Frame::compute_checksum(&[]);
                        let chunk_frame = Frame::chunk(request_id.clone(), stream_id.clone(), 0, vec![], 0, checksum);
                        tx.send(chunk_frame).map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                        1
                    } else {
                        // Non-empty value - chunk into max_chunk pieces
                        let mut offset = 0;
                        let mut chunk_index = 0u64;
                        while offset < bytes.len() {
                            let chunk_size = (bytes.len() - offset).min(max_chunk);
                            let chunk_data = bytes[offset..offset + chunk_size].to_vec();
                            let checksum = Frame::compute_checksum(&chunk_data);
                            let chunk_frame = Frame::chunk(request_id.clone(), stream_id.clone(), 0, chunk_data, chunk_index, checksum);
                            tx.send(chunk_frame).map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                            offset += chunk_size;
                            chunk_index += 1;
                        }
                        chunk_index
                    };

                    // Send STREAM_END
                    let end_frame = Frame::stream_end(request_id.clone(), stream_id.clone(), chunk_count);
                    tx.send(end_frame).map_err(|_| RuntimeError::Handler("Failed to send STREAM_END".to_string()))?;
                }
            }
        }

        // Send END frame to signal request completion
        let end_frame = Frame::end(request_id.clone(), None);
        tx.send(end_frame).map_err(|_| RuntimeError::Handler("Failed to send END".to_string()))?;
        drop(tx); // Close channel

        // Create InputPackage from frame channel (no file-path interception — already done)
        let input_package = demux_multi_stream(rx, None);

        // Create OutputStream backed by CLI frame sender
        let cli_sender: Arc<dyn FrameSender> = Arc::new(frame_sender);
        let output = OutputStream::new(
            cli_sender.clone(),
            uuid::Uuid::new_v4().to_string(),
            "*".to_string(),
            request_id.clone(),
            None, // No routing_id in CLI mode
            Limits::default().max_chunk,
        );

        // Invoke Op handler
        let op = factory();
        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer);
        let result = dispatch_op(op, input_package, output, peer_arc);

        match result {
            Ok(()) => {
                Ok(())
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    /// Find a cap by its command name (the CLI subcommand).
    fn find_cap_by_command<'a>(&self, manifest: &'a CapManifest, command_name: &str) -> Option<&'a Cap> {
        manifest.caps.iter().find(|cap| cap.command == command_name)
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
        let cbor_args: Vec<ciborium::Value> = vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text(arg.media_urn.clone())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(arg.value.clone())),
            ])
        ];
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
    fn build_payload_from_cli(&self, cap: &Cap, cli_args: &[String]) -> Result<Vec<u8>, RuntimeError> {
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
            let (value, came_from_stdin) = self.extract_arg_value(&arg_def, cli_args, stdin_data.as_deref())?;

            if let Some(val) = value {
                // Determine media_urn: if value came from stdin source, use stdin's media_urn
                // Otherwise use arg's media_urn as-is (file-path conversion happens later)
                let media_urn = if came_from_stdin {
                    // Find stdin source's media_urn
                    arg_def.sources.iter()
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
            let cbor_args: Vec<ciborium::Value> = arguments.iter().map(|arg| {
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
            }).collect();

            let cbor_array = ciborium::Value::Array(cbor_args);
            let mut payload = Vec::new();
            ciborium::into_writer(&cbor_array, &mut payload)
                .map_err(|e| RuntimeError::Serialize(format!("Failed to encode CBOR payload: {}", e)))?;

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
            let bytes = serde_json::to_vec(default)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))?;
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
            let poll_result = unsafe {
                libc::poll(&mut pollfd as *mut libc::pollfd, 1, 0)
            };

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

    /// Read file(s) for file-path arguments and return bytes.
    ///
    /// This method implements automatic file-path to bytes conversion when:
    /// - arg.media_urn is "media:file-path" or "media:file-path-array"
    /// - arg has a stdin source (indicating bytes are the canonical type)
    ///
    /// # Arguments
    /// * `path_value` - File path string (single path or JSON array of path patterns)
    /// * `is_array` - True if media:file-path-array (read multiple files with glob expansion)
    ///
    /// # Returns
    /// - For single file: Vec<u8> containing raw file bytes
    /// - For array: CBOR-encoded array of file bytes (each element is one file's contents)
    ///
    /// # Errors
    /// Returns RuntimeError::Io if file cannot be read with clear error message.
    fn read_file_path_to_bytes(&self, path_value: &str, is_array: bool) -> Result<Option<Vec<u8>>, RuntimeError> {
        if is_array {
            // Parse JSON array of path patterns
            let path_patterns: Vec<String> = serde_json::from_str(path_value)
                .map_err(|e| RuntimeError::Cli(format!(
                    "Failed to parse file-path-array: expected JSON array of path patterns, got '{}': {}",
                    path_value, e
                )))?;

            // Expand globs and collect all file paths
            let mut all_files = Vec::new();
            for pattern in &path_patterns {
                // Check if this is a literal path (no glob metacharacters) or a glob pattern
                let is_glob = pattern.contains('*') || pattern.contains('?') || pattern.contains('[');

                if !is_glob {
                    // Literal path - verify it exists and is a file
                    let path = std::path::Path::new(pattern);
                    if !path.exists() {
                        return Err(RuntimeError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Failed to read file '{}' from file-path-array: No such file or directory", pattern)
                        )));
                    }
                    if path.is_file() {
                        all_files.push(path.to_path_buf());
                    }
                    // Skip directories silently for consistency with glob behavior
                } else {
                    // Glob pattern - expand it
                    let paths = glob::glob(pattern)
                        .map_err(|e| RuntimeError::Cli(format!(
                            "Invalid glob pattern '{}': {}",
                            pattern, e
                        )))?;

                    for path_result in paths {
                        let path = path_result
                            .map_err(|e| RuntimeError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Glob error: {}", e)
                            )))?;

                        // Only include files (skip directories)
                        if path.is_file() {
                            all_files.push(path);
                        }
                    }
                }
            }

            // Read each file sequentially (streaming construction - don't load all at once)
            let mut files_data = Vec::new();
            for path in &all_files {
                let bytes = std::fs::read(path)
                    .map_err(|e| RuntimeError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read file '{}' from file-path-array: {}", path.display(), e)
                    )))?;
                files_data.push(ciborium::Value::Bytes(bytes));
            }

            // Encode as CBOR array
            let cbor_array = ciborium::Value::Array(files_data);
            let mut cbor_bytes = Vec::new();
            ciborium::into_writer(&cbor_array, &mut cbor_bytes)
                .map_err(|e| RuntimeError::Serialize(format!("Failed to encode CBOR array: {}", e)))?;

            Ok(Some(cbor_bytes))
        } else {
            // Single file path - read and return raw bytes
            let bytes = std::fs::read(path_value)
                .map_err(|e| RuntimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read file '{}': {}", path_value, e)
                )))?;

            Ok(Some(bytes))
        }
    }

    /// Print help message showing all available subcommands.
    fn print_help(&self, _manifest: &CapManifest) {
    }

    /// Print help for a specific cap.
    fn print_cap_help(&self, _cap: &Cap) {
    }

    /// Run in Plugin CBOR mode - binary frame protocol via stdin/stdout.
    fn run_cbor_mode(&self) -> Result<(), RuntimeError> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        // Lock stdin for reading (single reader thread)
        let reader = BufReader::new(stdin.lock());

        // Use direct stdout for writer (will be moved to writer thread)
        let writer = BufWriter::new(stdout);

        let mut frame_reader = FrameReader::new(reader);
        let mut frame_writer = FrameWriter::new(writer);

        // Perform handshake - send our manifest in the HELLO response
        let negotiated_limits = handshake_accept(&mut frame_reader, &mut frame_writer, &self.manifest_data)?;
        frame_reader.set_limits(negotiated_limits);
        frame_writer.set_limits(negotiated_limits);

        // Create output channel - ALL frames (peer requests + responses) go through here
        let (output_tx, output_rx) = crossbeam_channel::unbounded::<Frame>();

        // Spawn writer thread to drain output channel and write frames to stdout
        let writer_handle = std::thread::spawn(move || {
            let mut seq_assigner = SeqAssigner::new();
            for mut frame in output_rx {
                // Assign centralized seq per request ID before writing
                seq_assigner.assign(&mut frame);
                if let Err(_) = frame_writer.write(&frame) {
                    break;
                }
                // Cleanup seq tracking on terminal frames
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
            }
            // CRITICAL: Flush buffered output before exiting!
            let _ = frame_writer.inner_mut().flush();
        });

        // Track pending peer requests (plugin invoking host caps)
        let pending_peer_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Track active requests (incoming, handler already spawned)
        let mut active_requests: HashMap<MessageId, ActiveRequest> = HashMap::new();

        // Track active handler threads for cleanup
        let mut active_handlers: Vec<JoinHandle<()>> = Vec::new();

        // Main loop: simple frame router. No accumulation.
        loop {
            active_handlers.retain(|h| !h.is_finished());

            let frame = match frame_reader.read()? {
                Some(f) => f,
                None => break,
            };

            match frame.frame_type {
                FrameType::Req => {
                    // Extract routing_id (XID) FIRST — all error paths must include it
                    let routing_id = frame.routing_id.clone();

                    let cap_urn = match frame.cap.as_ref() {
                        Some(urn) => urn.clone(),
                        None => {
                            let mut err_frame = Frame::err(frame.id, "INVALID_REQUEST", "Request missing cap URN");
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    let factory = match self.find_handler(&cap_urn) {
                        Some(f) => f,
                        None => {
                            let mut err_frame = Frame::err(frame.id.clone(), "NO_HANDLER",
                                &format!("No handler registered for cap: {}", cap_urn));
                            err_frame.routing_id = routing_id;
                            let _ = output_tx.send(err_frame);
                            continue;
                        }
                    };

                    if frame.payload.as_ref().map_or(false, |p| !p.is_empty()) {
                        let mut err_frame = Frame::err(frame.id, "PROTOCOL_ERROR",
                            "REQ frame must have empty payload - use STREAM_START for arguments");
                        err_frame.routing_id = routing_id;
                        let _ = output_tx.send(err_frame);
                        continue;
                    }

                    let request_id = frame.id.clone();

                    // Create channel for streaming frames to handler
                    let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
                    active_requests.insert(request_id.clone(), ActiveRequest { raw_tx });

                    // Spawn handler thread immediately (not on END)
                    let output_tx_clone = output_tx.clone();
                    let pending_clone = Arc::clone(&pending_peer_requests);
                    let manifest_clone = self.manifest.clone();
                    let cap_urn_clone = cap_urn.clone();
                    let max_chunk = negotiated_limits.max_chunk;

                    let handle = thread::spawn(move || {
                        // Build file-path context for Demux
                        let fp_ctx = FilePathContext::new(&cap_urn_clone, manifest_clone).ok();

                        // Create InputPackage via Demux (multi-stream, with file-path interception)
                        let input_package = demux_multi_stream(raw_rx, fp_ctx);

                        // Create OutputStream for handler output
                        let sender: Arc<dyn FrameSender> = Arc::new(ChannelFrameSender {
                            tx: output_tx_clone.clone(),
                        });
                        let stream_id = uuid::Uuid::new_v4().to_string();
                        let output = OutputStream::new(
                            Arc::clone(&sender),
                            stream_id,
                            "*".to_string(),
                            request_id.clone(),
                            routing_id.clone(),
                            max_chunk,
                        );

                        // Create PeerInvoker
                        let peer_invoker = PeerInvokerImpl {
                            output_tx: output_tx_clone.clone(),
                            pending_requests: Arc::clone(&pending_clone),
                            max_chunk,
                            origin_request_id: request_id.clone(),
                            origin_routing_id: routing_id.clone(),
                        };

                        // Call Op handler via dispatch
                        let op = factory();
                        let peer_arc: Arc<dyn PeerInvoker> = Arc::new(peer_invoker);
                        let result = dispatch_op(op, input_package, output, peer_arc);

                        match result {
                            Ok(()) => {
                                // Send END frame with routing_id
                                let mut end_frame = Frame::end(request_id, None);
                                end_frame.routing_id = routing_id;
                                let _ = sender.send(&end_frame);
                            }
                            Err(e) => {
                                let mut err_frame = Frame::err(request_id, "HANDLER_ERROR", &e.to_string());
                                err_frame.routing_id = routing_id;
                                let _ = sender.send(&err_frame);
                            }
                        }
                    });

                    active_handlers.push(handle);
                }

                // Route STREAM_START / CHUNK / STREAM_END to active request or peer response
                FrameType::StreamStart | FrameType::Chunk | FrameType::StreamEnd => {
                    // Try active request first
                    if let Some(ar) = active_requests.get(&frame.id) {
                        if ar.raw_tx.send(frame.clone()).is_err() {
                            active_requests.remove(&frame.id);
                        }
                        continue;
                    }

                    // Try peer response
                    let peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.get(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    }
                    drop(peer);
                }

                FrameType::End => {
                    // Try active request first -- send END then remove
                    if let Some(ar) = active_requests.remove(&frame.id) {
                        let _ = ar.raw_tx.send(frame.clone());
                        // raw_tx dropped here → Demux sees channel close after END
                        continue;
                    }

                    // Try peer response — send END then remove
                    let mut peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.remove(&frame.id) {
                        let _ = pr.sender.send(frame.clone());
                    }
                    drop(peer);
                }

                FrameType::Err => {
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

                FrameType::Heartbeat => {
                    let response = Frame::heartbeat(frame.id);
                    let _ = output_tx.send(response);
                }

                FrameType::Hello => {
                    let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "Unexpected HELLO after handshake");
                    let _ = output_tx.send(err_frame);
                }

                FrameType::Log => {
                    // Forward peer response LOG frames back on the original request.
                    // Re-stamp with origin RID/XID so the engine sees them as progress
                    // on the cap execution that triggered the peer call.
                    let peer = pending_peer_requests.lock().unwrap();
                    if let Some(pr) = peer.get(&frame.id) {
                        let level = frame.log_level().unwrap_or("INFO");
                        let message = frame.log_message().unwrap_or("");
                        let mut log_frame = Frame::log(pr.origin_request_id.clone(), level, message);
                        log_frame.routing_id = pr.origin_routing_id.clone();
                        let _ = output_tx.send(log_frame);
                    }
                    drop(peer);
                }

                FrameType::RelayNotify | FrameType::RelayState => {
                    return Err(CborError::Protocol(format!(
                        "Relay frame {:?} must not reach plugin runtime",
                        frame.frame_type
                    )).into());
                }
            }
        }

        // Graceful shutdown
        drop(output_tx);

        let _ = writer_handle.join();

        for handle in active_handlers {
            let _ = handle.join();
        }

        Ok(())
    }

    /// Get the current protocol limits
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}


#[cfg(test)]
mod tests {
    use super::*;

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
            let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req.take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            req.output().emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata { OpMetadata::builder("EmitBytesOp").build() }
    }

    /// Test Op: echoes all input chunks to output, optionally records received bytes
    struct EchoOp {
        received: Option<Arc<Mutex<Vec<u8>>>>,
    }
    impl Default for EchoOp {
        fn default() -> Self { Self { received: None } }
    }
    #[async_trait]
    impl Op<()> for EchoOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let input = req.take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let mut total = Vec::new();
            for stream in input {
                let stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                for chunk in stream {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    if let ciborium::Value::Bytes(ref b) = chunk {
                        total.extend(b);
                    }
                    req.output().emit_cbor(&chunk)
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            if let Some(ref received) = self.received {
                *received.lock().unwrap() = total;
            }
            Ok(())
        }
        fn metadata(&self) -> OpMetadata { OpMetadata::builder("EchoOp").build() }
    }

    /// Test Op: echoes input then appends a tag byte
    struct EchoTagOp {
        tag: Vec<u8>,
    }
    #[async_trait]
    impl Op<()> for EchoTagOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let input = req.take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            for stream in input {
                let stream = stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                for chunk in stream {
                    let chunk = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                    req.output().emit_cbor(&chunk)
                        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                }
            }
            req.output().emit_cbor(&ciborium::Value::Bytes(self.tag.clone()))
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata { OpMetadata::builder("EchoTagOp").build() }
    }

    /// Test Op: extracts CBOR "value" key from args, stores in shared state
    struct ExtractValueOp {
        received: Arc<Mutex<Vec<u8>>>,
    }
    #[async_trait]
    impl Op<()> for ExtractValueOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let input = req.take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let bytes = input.collect_all_bytes()
                .map_err(|e| OpError::ExecutionFailed(format!("Stream error: {}", e)))?;
            let cbor_val: ciborium::Value = ciborium::from_reader(&bytes[..])
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            if let ciborium::Value::Array(args) = cbor_val {
                for arg in args {
                    if let ciborium::Value::Map(map) = arg {
                        for (k, v) in map {
                            if let (ciborium::Value::Text(key), ciborium::Value::Bytes(b)) = (k, v) {
                                if key == "value" {
                                    *self.received.lock().unwrap() = b.clone();
                                    req.output().emit_cbor(&ciborium::Value::Bytes(b))
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
        fn metadata(&self) -> OpMetadata { OpMetadata::builder("ExtractValueOp").build() }
    }

    /// Test Op: no-op (does nothing)
    #[derive(Default)]
    struct NoOpOp;
    #[async_trait]
    impl Op<()> for NoOpOp {
        async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
            let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            let _input = req.take_input()
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            Ok(())
        }
        fn metadata(&self) -> OpMetadata { OpMetadata::builder("NoOpOp").build() }
    }

    /// Helper: invoke a factory-produced Op with test input/output
    fn invoke_op(factory: &OpFactory, input: InputPackage, output: OutputStream) -> Result<(), RuntimeError> {
        let op = factory();
        let peer: Arc<dyn PeerInvoker> = Arc::new(NoPeerInvoker);
        dispatch_op(op, input, output, peer)
    }

    /// Create an InputPackage from a list of streams for testing.
    /// Each stream is a (media_urn, data_bytes) pair.
    /// The data is CBOR-encoded as Value::Bytes in a CHUNK frame.
    fn test_input_package(streams: &[(&str, &[u8])]) -> InputPackage {
        let (raw_tx, raw_rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();

        for (media_urn, data) in streams {
            let stream_id = uuid::Uuid::new_v4().to_string();
            raw_tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), media_urn.to_string())).ok();

            // Encode data as CBOR Bytes and wrap in CHUNK
            let value = ciborium::Value::Bytes(data.to_vec());
            let mut cbor = Vec::new();
            ciborium::into_writer(&value, &mut cbor).unwrap();
            let checksum = Frame::compute_checksum(&cbor);
            raw_tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, cbor, 0, checksum)).ok();
            raw_tx.send(Frame::stream_end(request_id.clone(), stream_id, 1)).ok();
        }
        raw_tx.send(Frame::end(request_id, None)).ok();
        drop(raw_tx);

        demux_multi_stream(raw_rx, None)
    }

    /// Create an OutputStream backed by a channel for testing.
    /// Returns (OutputStream, frame_receiver) so tests can inspect output.
    fn test_output_stream() -> (OutputStream, crossbeam_channel::Receiver<Frame>) {
        let (out_tx, out_rx) = crossbeam_channel::unbounded();
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
            Self { caps: HashMap::new() }
        }

        fn add_cap(&mut self, cap: Cap) {
            self.caps.insert(cap.urn_string(), cap);
        }

        fn get(&self, urn_str: &str) -> Option<&Cap> {
            // Normalize the URN for lookup
            let normalized = CapUrn::from_string(urn_str).ok()?.to_string();
            self.caps.iter()
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
                r#"cap:in="media:string;textable;form=scalar";op=test;out="*""#,
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
                r#"cap:in="media:model-spec;textable;form=scalar";op=infer;out="*""#,
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
    fn test_filepath_array_conversion(cap: &Cap, cli_args: &[String], runtime: &PluginRuntime) -> Vec<Vec<u8>> {
        // Extract raw argument value
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], cli_args, None).unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text(cap.args[0].media_urn.clone())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result = extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

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
        let value_array = result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr.clone(),
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Extract bytes from each element
        value_array.iter().map(|v| match v {
            ciborium::Value::Bytes(b) => b.clone(),
            _ => panic!("Expected bytes in array"),
        }).collect()
    }

    /// Helper to test file-path conversion: takes Cap, CLI args, and returns converted bytes
    fn test_filepath_conversion(cap: &Cap, cli_args: &[String], runtime: &PluginRuntime) -> Vec<u8> {
        // Extract raw argument value
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], cli_args, None).unwrap();

        // Build CBOR payload
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text(cap.args[0].media_urn.clone())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion
        let result = extract_effective_payload(&payload, Some("application/cbor"), cap, true).unwrap();

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
        result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Bytes(b) => b.clone(),
                _ => panic!("Expected bytes"),
            })
            .unwrap()
    }

    /// Helper function to create a CapManifest for tests
    fn create_test_manifest(name: &str, version: &str, description: &str, mut caps: Vec<Cap>) -> CapManifest {
        // Always append CAP_IDENTITY at the end - plugins must declare it
        // (Appending instead of prepending to avoid breaking tests that reference caps[0])
        let identity_urn = crate::CapUrn::from_string("cap:").unwrap();
        let identity_cap = Cap::new(identity_urn, "Identity".to_string(), "identity".to_string());
        caps.push(identity_cap);

        CapManifest::new(
            name.to_string(),
            version.to_string(),
            description.to_string(),
            caps,
        )
    }

    /// Test manifest JSON with identity and a test cap for basic tests.
    /// Note: cap URN uses "cap:op=test" which lacks in/out tags, so CapManifest deserialization
    /// may fail because Cap requires in/out specs. For tests that only need raw manifest bytes
    /// (CBOR mode handshake), this is fine. For tests that need parsed CapManifest, use
    /// VALID_MANIFEST instead.
    const TEST_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:","title":"Identity","command":"identity"},{"urn":"cap:op=test","title":"Test","command":"test"}]}"#;

    /// Valid manifest with proper in/out specs for tests that need parsed CapManifest
    const VALID_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:","title":"Identity","command":"identity"},{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test"}]}"#;

    // TEST248: Test register_op and find_handler by exact cap URN
    #[test]
    fn test248_register_and_find_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:in=*;op=test;out=*", || Box::new(EmitBytesOp { data: b"result".to_vec() }));
        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    // TEST249: Test register_op handler echoes bytes directly
    #[test]
    fn test249_raw_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        let received: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=raw", move || {
            Box::new(EchoOp { received: Some(Arc::clone(&received_clone)) }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:op=raw").unwrap();
        let input = test_input_package(&[("media:", b"echo this")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();
        assert_eq!(&*received.lock().unwrap(), b"echo this", "raw handler must echo payload");
    }

    // TEST250: Test Op handler collects input and processes it
    #[test]
    fn test250_typed_handler_deserialization() {
        /// Test Op: parses JSON, extracts "key" field, emits as bytes
        struct JsonKeyOp {
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for JsonKeyOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req.take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input.collect_all_bytes()
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let json: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                let value = json.get("key").and_then(|v| v.as_str()).unwrap_or("missing");
                let bytes = value.as_bytes();
                req.output().emit_cbor(&ciborium::Value::Bytes(bytes.to_vec()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = bytes.to_vec();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata { OpMetadata::builder("JsonKeyOp").build() }
        }

        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=test", move || {
            Box::new(JsonKeyOp { received: Arc::clone(&received_clone) }) as Box<dyn Op<()>>
        });

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"{\"key\":\"hello\"}")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();
        assert_eq!(&*received.lock().unwrap(), b"hello");
    }

    // TEST251: Test Op handler propagates errors through RuntimeError::Handler
    #[test]
    fn test251_typed_handler_rejects_invalid_json() {
        /// Op that parses JSON — fails on invalid input
        struct JsonParseOp;
        #[async_trait]
        impl Op<()> for JsonParseOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req.take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let all_bytes = input.collect_all_bytes()
                    .map_err(|e| OpError::ExecutionFailed(format!("Failed to collect: {}", e)))?;
                let _: serde_json::Value = serde_json::from_slice(&all_bytes)
                    .map_err(|e| OpError::ExecutionFailed(format!("Bad JSON: {}", e)))?;
                Ok(())
            }
            fn metadata(&self) -> OpMetadata { OpMetadata::builder("JsonParseOp").build() }
        }

        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register_op("cap:op=test", || Box::new(JsonParseOp));

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"not json {{{{")]);
        let (output, _out_rx) = test_output_stream();
        let result = invoke_op(&factory, input, output);
        assert!(result.is_err(), "Invalid JSON must produce error");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("JSON"), "Error should mention JSON: {}", err_msg);
    }

    // TEST252: Test find_handler returns None for unregistered cap URNs
    #[test]
    fn test252_find_handler_unknown_cap() {
        let runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        assert!(runtime.find_handler("cap:op=nonexistent").is_none());
    }

    // TEST253: Test OpFactory can be cloned via Arc and sent across threads (Send + Sync)
    #[test]
    fn test253_handler_is_send_sync() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_op("cap:op=threaded", move || {
            let r = Arc::clone(&received_clone);
            Box::new(EmitAndRecordOp { data: b"done".to_vec(), received: r }) as Box<dyn Op<()>>
        });

        /// Test Op: emits fixed bytes and records in shared state
        struct EmitAndRecordOp {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let _input = req.take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                req.output().emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata { OpMetadata::builder("EmitAndRecordOp").build() }
        }

        let factory = runtime.find_handler("cap:op=threaded").unwrap();
        let factory_clone = Arc::clone(&factory);

        let handle = std::thread::spawn(move || {
            let input = test_input_package(&[("media:", b"{}")]);
            let (output, _out_rx) = test_output_stream();
            invoke_op(&factory_clone, input, output).unwrap();
        });

        handle.join().unwrap();
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
                assert!(msg.contains("not supported"), "error must indicate peer not supported");
            }
            _ => panic!("Expected PeerRequest error"),
        }
    }

    // TEST255: Test NoPeerInvoker call_with_bytes also returns error
    #[test]
    fn test255_no_peer_invoker_with_arguments() {
        let no_peer = NoPeerInvoker;
        let result = no_peer.call_with_bytes("cap:op=test", &[("media:test", b"value")]);
        assert!(result.is_err());
    }

    // TEST256: Test PluginRuntime::with_manifest_json stores manifest data and parses when valid
    #[test]
    fn test256_with_manifest_json() {
        // TEST_MANIFEST has "cap:op=test" — missing in/out defaults to media: (wildcard).
        // Manifest must declare CAP_IDENTITY explicitly or it will fail validation.
        let runtime_basic = PluginRuntime::with_manifest_json(TEST_MANIFEST);
        assert!(!runtime_basic.manifest_data.is_empty());
        assert!(runtime_basic.manifest.is_some(), "cap:op=test is valid (defaults to media: for in/out)");
        let manifest = runtime_basic.manifest.unwrap();
        assert_eq!(manifest.caps.len(), 2, "Original cap + auto-added identity");

        // VALID_MANIFEST has proper in/out specs
        let runtime_valid = PluginRuntime::with_manifest_json(VALID_MANIFEST);
        assert!(!runtime_valid.manifest_data.is_empty());
        assert!(runtime_valid.manifest.is_some(), "VALID_MANIFEST must parse into CapManifest");
    }

    // TEST257: Test PluginRuntime::new with invalid JSON still creates runtime (manifest is None)
    #[test]
    fn test257_new_with_invalid_json() {
        let runtime = PluginRuntime::new(b"not json");
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_none(), "invalid JSON should leave manifest as None");
    }

    // TEST258: Test PluginRuntime::with_manifest creates runtime with valid manifest data
    #[test]
    fn test258_with_manifest_struct() {
        let manifest: crate::bifaci::manifest::CapManifest = serde_json::from_str(VALID_MANIFEST).unwrap();
        let runtime = PluginRuntime::with_manifest(manifest);
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_some());
    }

    // TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged
    #[test]
    fn test259_extract_effective_payload_non_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:void";op=test;out="media:void""#).unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, Some("application/json"), cap, true).unwrap();
        assert_eq!(result, payload, "non-CBOR must return raw payload");
    }

    // TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
    #[test]
    fn test260_extract_effective_payload_no_content_type() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:void";op=test;out="media:void""#).unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, None, cap, true).unwrap();
        assert_eq!(result, payload);
    }

    // TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
    #[test]
    fn test261_extract_effective_payload_cbor_match() {
        // Build CBOR arguments: [{media_urn: "media:string;textable;form=scalar", value: bytes("hello")}]
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:string;textable;form=scalar".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(b"hello".to_vec())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // The cap URN has in=media:string;textable;form=scalar
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:string;textable;form=scalar";op=test;out="*""#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false,  // CBOR mode - tests pass CBOR payloads directly
        ).unwrap();

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
        assert_eq!(found_value, Some(b"hello".to_vec()), "Handler extracts value from CBOR array");
    }

    // TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input
    #[test]
    fn test262_extract_effective_payload_cbor_no_match() {
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:other-type".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(b"data".to_vec())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:string;textable;form=scalar";op=test;out="*""#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false,  // CBOR mode
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
            false,  // CBOR mode
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
            false,  // CBOR mode
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
    #[test]
    fn test270_multiple_handlers() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_op("cap:op=alpha", || Box::new(EchoTagOp { tag: b"a".to_vec() }));
        runtime.register_op("cap:op=beta", || Box::new(EchoTagOp { tag: b"b".to_vec() }));
        runtime.register_op("cap:op=gamma", || Box::new(EchoTagOp { tag: b"g".to_vec() }));

        let f_alpha = runtime.find_handler("cap:op=alpha").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_alpha, input, output).unwrap();

        let f_beta = runtime.find_handler("cap:op=beta").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_beta, input, output).unwrap();

        let f_gamma = runtime.find_handler("cap:op=gamma").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&f_gamma, input, output).unwrap();
    }

    // TEST271: Test Op handler replacing an existing registration for the same cap URN
    #[test]
    fn test271_handler_replacement() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        let result1: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let result2_clone = Arc::clone(&result2);

        runtime.register_op("cap:op=test", move || {
            Box::new(EchoTagOp { tag: b"first".to_vec() }) as Box<dyn Op<()>>
        });
        runtime.register_op("cap:op=test", move || {
            let r = Arc::clone(&result2_clone);
            Box::new(EmitAndRecordOp2 { data: b"second".to_vec(), received: r }) as Box<dyn Op<()>>
        });

        /// Op that emits fixed data and records it
        struct EmitAndRecordOp2 {
            data: Vec<u8>,
            received: Arc<Mutex<Vec<u8>>>,
        }
        #[async_trait]
        impl Op<()> for EmitAndRecordOp2 {
            async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
                let req: Arc<Request> = wet.get_required(WET_KEY_REQUEST)
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                let input = req.take_input()
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                for stream in input { for chunk in stream.map_err(|e| OpError::ExecutionFailed(e.to_string()))? { let _ = chunk.map_err(|e| OpError::ExecutionFailed(e.to_string()))?; } }
                req.output().emit_cbor(&ciborium::Value::Bytes(self.data.clone()))
                    .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                *self.received.lock().unwrap() = self.data.clone();
                Ok(())
            }
            fn metadata(&self) -> OpMetadata { OpMetadata::builder("EmitAndRecordOp2").build() }
        }

        let factory = runtime.find_handler("cap:op=test").unwrap();
        let input = test_input_package(&[("media:", b"")]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();
        assert_eq!(&*result2.lock().unwrap(), b"second", "later registration must replace earlier");
        // result1 should NOT have been called
        assert!(result1.lock().unwrap().is_empty(), "first handler must not be called after replacement");
    }

    // TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one
    #[test]
    fn test272_extract_effective_payload_multiple_args() {
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:other-type;textable".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(b"wrong".to_vec())),
            ]),
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:model-spec;textable;form=scalar".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(b"correct".to_vec())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:model-spec;textable;form=scalar";op=infer;out="*""#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false,  // CBOR mode - tests pass CBOR payloads directly
        ).unwrap();

        // NEW REGIME: Handler receives full CBOR array with BOTH arguments
        // Handler must match against in_spec to find main input
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };

        assert_eq!(result_array.len(), 2, "Both arguments present in CBOR array");

        // Find the argument matching in_spec (media:model-spec)
        let in_spec = MediaUrn::from_string("media:model-spec;textable;form=scalar").unwrap();
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

                // Match against in_spec
                if let (Some(urn_str), Some(val)) = (arg_urn_str, arg_value) {
                    if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                        let matches = in_spec.accepts(&arg_urn).unwrap_or(false) ||
                                     arg_urn.conforms_to(&in_spec).unwrap_or(false);
                        if matches {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(found_value, Some(b"correct".to_vec()), "Handler finds correct argument by matching in_spec");
    }

    // TEST273: Test extract_effective_payload with binary data in CBOR value (not just text)
    #[test]
    fn test273_extract_effective_payload_binary_value() {
        let binary_data: Vec<u8> = (0u8..=255).collect();
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:pdf".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(binary_data.clone())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:pdf";op=process;out="*""#).unwrap();
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            cap,
            false,  // CBOR mode - tests pass CBOR payloads directly
        ).unwrap();

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
        assert_eq!(found_value, Some(binary_data), "binary values must roundtrip through CBOR array");
    }

    // TEST336: Single file-path arg with stdin source reads file and passes bytes to handler
    #[test]
    fn test336_file_path_reads_file_passes_bytes() {
        use std::sync::{Arc, Mutex};

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test336_input.pdf");
        std::fs::write(&test_file, b"PDF binary content 336").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let mut runtime = PluginRuntime::with_manifest(manifest);

        // Track what handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:pdf\";op=process;out=\"media:void\"",
            move || {
                Box::new(ExtractValueOp { received: Arc::clone(&received_clone) }) as Box<dyn Op<()>>
            },
        );

        // Simulate CLI invocation: plugin process /path/to/file.pdf
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (simulates what run_cli_mode does)
        // This does file-path auto-conversion: path → bytes
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true,  // CLI mode
        ).unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Simulate CLI mode: parse CBOR args → send as streams → InputPackage
        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();

        // Verify handler received file bytes (not file path string)
        let received = received_payload.lock().unwrap();
        assert_eq!(&*received, b"PDF binary content 336", "Handler receives file bytes after auto-conversion");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![ArgSource::Position { position: 0 }],  // NO stdin source!
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];
        let result = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();

        // Should get file PATH as string, not file CONTENTS
        let value_str = String::from_utf8(result.0.unwrap()).unwrap();
        assert!(value_str.contains("test337_input.txt"), "Should receive file path string when no stdin source");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::CliFlag { cli_flag: "--file".to_string() },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec!["--file".to_string(), test_file.to_string_lossy().to_string()];
        let file_contents = test_filepath_conversion(&cap, &cli_args, &runtime);

        assert_eq!(file_contents, b"PDF via flag 338", "Should read file from --file flag");

        std::fs::remove_file(test_file).ok();
    }

    // TEST339: file-path-array reads multiple files with glob pattern
    #[test]
    fn test339_file_path_array_glob_expansion() {
        let temp_dir = std::env::temp_dir().join("test339");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let file1 = temp_dir.join("doc1.txt");
        let file2 = temp_dir.join("doc2.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Batch",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Pass glob pattern directly (NOT JSON - no ;json tag in media URN)
        let pattern = format!("{}/*.txt", temp_dir.display());
        let cli_args = vec![pattern];
        let files_bytes = test_filepath_array_conversion(&cap, &cli_args, &runtime);

        assert_eq!(files_bytes.len(), 2, "Should find 2 files");

        // Verify contents (order may vary, so sort)
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec!["/nonexistent/file.pdf".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=scalar".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // extract_effective_payload should fail when trying to read nonexistent file
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when file doesn't exist");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("/nonexistent/file.pdf"), "Error should mention file path");
        assert!(err_msg.contains("Failed to read file"), "Error should be clear");
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },  // First
                    ArgSource::Position { position: 0 },                     // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let stdin_data = b"stdin content 341";
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        let (result, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, Some(stdin_data)).unwrap();
        let result = result.unwrap();

        // Should get stdin data, not file content (stdin source tried first)
        assert_eq!(result, b"stdin content 341", "stdin source should take precedence");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // CLI: plugin test /path/to/file (position 0 after subcommand)
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
                "media:model-spec;textable;form=scalar",  // NOT file-path
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:model-spec;textable;form=scalar".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec!["mlx-community/Llama-3.2-3B-Instruct-4bit".to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];
        let (result, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
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
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Pass nonexistent path (without `;json` tag, this is NOT JSON - it's a path/pattern)
        let cli_args = vec!["/nonexistent/path/to/nothing".to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail when path doesn't exist");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("/nonexistent/path/to/nothing"), "Error should mention the path");
        assert!(err.contains("File not found") || err.contains("Failed to read"), "Error should be clear about file access failure");
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
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Pass literal path (non-glob) that doesn't exist - should fail
        let cli_args = vec![missing_path.to_string_lossy().to_string()];

        // Build CBOR payload and try conversion - should fail on file read
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail hard when literal path doesn't exist");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("test345_missing.txt"), "Error should mention the missing file");
        assert!(err.contains("File not found") || err.contains("doesn't exist"), "Error should be clear about missing file");
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Position { position: 0 },                     // First
                    ArgSource::Stdin { stdin: "media:".to_string() },  // Second
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(cap, &cli_args, &runtime);

        // Position source tried first, so file is read
        assert_eq!(result, b"file content 348", "Position source tried first, file read");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::CliFlag { cli_flag: "--file".to_string() },  // First (not provided)
                    ArgSource::Position { position: 0 },                     // Second (provided)
                    ArgSource::Stdin { stdin: "media:".to_string() },  // Third (not used)
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Only provide position arg, no --file flag
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Use helper to properly test file-path conversion
        let result = test_filepath_conversion(cap, &cli_args, &runtime);

        assert_eq!(result, b"content 349", "Should fall back to position source and read file");

        std::fs::remove_file(test_file).ok();
    }

    // TEST350: Integration test - full CLI mode invocation with file-path
    #[test]
    fn test350_full_cli_mode_with_file_path_integration() {
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let mut runtime = PluginRuntime::with_manifest(manifest);

        // Track what the handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_op(
            "cap:in=\"media:pdf\";op=process;out=\"media:result;textable\"",
            move || {
                Box::new(ExtractValueOp { received: Arc::clone(&received_clone) }) as Box<dyn Op<()>>
            },
        );

        // Simulate full CLI invocation
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        let payload = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true,  // CLI mode
        ).unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        let input = test_input_package(&[("media:", &payload)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();

        // Verify handler received file bytes
        let received = received_payload.lock().unwrap();
        assert_eq!(&*received, test_content, "Handler receives file bytes after auto-conversion");

        std::fs::remove_file(test_file).ok();
    }

    // TEST351: file-path array with empty CBOR array returns empty (CBOR mode)
    #[test]
    fn test351_file_path_array_empty_array() {
        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                false,  // Not required
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                ],
            )],
        );

        // Build CBOR payload with empty Array value (CBOR mode)
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Array(vec![])),  // Empty array
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

        // Decode and verify empty array is preserved
        let result_cbor: ciborium::Value = ciborium::from_reader(&result[..]).unwrap();
        let result_array = match result_cbor {
            ciborium::Value::Array(arr) => arr,
            _ => panic!("Expected CBOR array"),
        };
        let result_map = match &result_array[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected map"),
        };
        let value_array = result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(value_array.len(), 0, "Empty array should produce empty result");
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Build full CBOR payload and attempt file-path conversion
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=scalar".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), cap, true);

        assert!(result.is_err(), "Should fail on permission denied");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("test352_noperm.txt"), "Error should mention the file");

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
                "media:text;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:text;textable".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec!["test value".to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];
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
        let media_urn_val = arg_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| v)
            .expect("Should have media_urn key");

        match media_urn_val {
            ciborium::Value::Text(s) => assert_eq!(s, "media:text;textable;form=scalar"),
            _ => panic!("media_urn should be text"),
        }

        // Check value key
        let value_val = arg_map.iter()
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
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Glob pattern that matches nothing - should FAIL HARD (no fallback to empty array)
        let pattern = format!("{}/nonexistent_*.xyz", temp_dir.display());
        let cli_args = vec![pattern];  // NOT JSON - just the pattern

        // Build CBOR payload and try conversion - should fail when glob matches nothing
        let (raw_value, _) = runtime.extract_arg_value(&cap.args[0], &cli_args, None).unwrap();
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(raw_value.unwrap())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, true);

        assert!(result.is_err(), "Should fail hard when glob matches nothing - NO FALLBACK");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No files matched") || err.contains("nonexistent"), "Error should explain glob matched nothing");
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

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Glob that matches both file and directory
        let pattern = format!("{}/*", temp_dir.display());
        let cli_args = vec![pattern];  // NOT JSON - just the glob pattern
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Use helper to test file-path array conversion
        let files_array = test_filepath_array_conversion(cap, &cli_args, &runtime);

        // Should only include the file, not the directory
        assert_eq!(files_array.len(), 1, "Should only include files, not directories");
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

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Multiple patterns as CBOR Array (CBOR mode)
        let pattern1 = format!("{}/*.txt", temp_dir.display());
        let pattern2 = format!("{}/*.json", temp_dir.display());

        // Build CBOR payload with Array of patterns
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Array(vec![
                ciborium::Value::Text(pattern1),
                ciborium::Value::Text(pattern2),
            ])),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result = extract_effective_payload(&payload, Some("application/cbor"), cap, false).unwrap();

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
        let files_array = result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        assert_eq!(files_array.len(), 2, "Should find both files from different patterns");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![link_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Use helper to test file-path conversion
        let result = test_filepath_conversion(cap, &cli_args, &runtime);

        assert_eq!(result, b"real content", "Should follow symlink and read real file");

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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

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
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Invalid glob pattern (unclosed bracket)
        let pattern = "[invalid";

        // Build CBOR payload with invalid pattern
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Text(pattern.to_string())),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Try file-path conversion with invalid glob - should fail
        let result = extract_effective_payload(&payload, Some("application/cbor"), cap, true);

        assert!(result.is_err(), "Should fail on invalid glob pattern");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid glob pattern") || err.contains("Pattern"), "Error should mention invalid glob");
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
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let cap = &runtime.manifest.as_ref().unwrap().caps[0];

        // Build CBOR payload (what build_payload_from_cli does)
        let raw_payload = runtime.build_payload_from_cli(&cap, &cli_args).unwrap();

        // Extract effective payload (what run_cli_mode does)
        // This does file-path auto-conversion and returns full CBOR array
        let effective = extract_effective_payload(
            &raw_payload,
            Some("application/cbor"),
            &cap,
            true,  // CLI mode
        ).unwrap();

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
                        let matches = in_spec.accepts(&arg_urn).unwrap_or(false) ||
                                     arg_urn.conforms_to(&in_spec).unwrap_or(false);
                        if matches {
                            found_value = Some(val);
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(found_value, Some(pdf_content.to_vec()), "File-path auto-converted to bytes");

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
                    ArgSource::Stdin { stdin: "media:pdf".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // CLI mode: pass file path as positional argument
        let cli_args = vec![test_file.to_string_lossy().to_string()];
        let payload = runtime.build_payload_from_cli(
            &runtime.manifest.as_ref().unwrap().caps[0],
            &cli_args
        ).unwrap();

        // Verify payload is CBOR array with file-path argument
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        assert!(matches!(cbor_val, ciborium::Value::Array(_)), "CLI mode produces CBOR array");

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
                vec![ArgSource::Stdin { stdin: "media:pdf".to_string() }],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let runtime = PluginRuntime::with_manifest(manifest);

        // Mock stdin with Cursor (simulates piped binary)
        let mock_stdin = Cursor::new(pdf_content.clone());

        // Build payload from streaming reader (what CLI piped mode does)
        let payload = runtime.build_payload_from_streaming_reader(&cap, mock_stdin, Limits::default().max_chunk).unwrap();

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

                    assert_eq!(media_urn, Some("media:pdf".to_string()), "Media URN matches cap in_spec");
                    assert_eq!(value, Some(pdf_content), "Binary content preserved exactly");
                } else {
                    panic!("Expected Map in CBOR array");
                }
            }
            _ => panic!("Expected CBOR Array"),
        }
    }

    // TEST363: CBOR mode with chunked content - send file content streaming as chunks
    #[test]
    fn test363_cbor_mode_chunked_content() {
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
                vec![ArgSource::Stdin { stdin: "media:pdf".to_string() }],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let mut runtime = PluginRuntime::with_manifest(manifest);
        runtime.register_op(&cap.urn_string(), move || {
            Box::new(ExtractValueOp { received: Arc::clone(&received_clone) }) as Box<dyn Op<()>>
        });

        // Build CBOR payload with pdf_content
        let mut payload_bytes = Vec::new();
        let cbor_args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:pdf".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(pdf_content.clone())),
            ]),
        ]);
        ciborium::into_writer(&cbor_args, &mut payload_bytes).unwrap();

        let factory = runtime.find_handler(&cap.urn_string()).unwrap();

        // Send payload as InputPackage
        let input = test_input_package(&[("media:", &payload_bytes)]);
        let (output, _out_rx) = test_output_stream();
        invoke_op(&factory, input, output).unwrap();

        assert_eq!(*received.lock().unwrap(), pdf_content, "Handler receives chunked content");
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
                vec![ArgSource::Stdin { stdin: "media:pdf".to_string() }],
            )],
        );

        // Build CBOR arguments with file-path URN
        let args = vec![CapArgumentValue::new(
            MEDIA_FILE_PATH.to_string(),
            test_file.to_string_lossy().as_bytes().to_vec()
        )];
        let mut payload = Vec::new();
        let cbor_args: Vec<ciborium::Value> = args.iter().map(|arg| {
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text(arg.media_urn.clone())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(arg.value.clone())),
            ])
        }).collect();
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut payload).unwrap();

        // Extract effective payload (triggers file-path auto-conversion)
        let effective = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            &cap,
            false,  // CBOR mode
        ).unwrap();

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
                assert_eq!(media_urn, Some(&"media:pdf".to_string()), "URN converted to expected input");
                assert_eq!(value, Some(&pdf_content.to_vec()), "File auto-converted to bytes");
            }
        }

        std::fs::remove_file(test_file).ok();
    }

    // TEST361: CBOR Array of file-paths in CBOR mode (validates new Array support)
    #[test]
    fn test361_cbor_array_file_paths_in_cbor_mode() {
        let temp_dir = std::env::temp_dir().join("test361");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create three test files
        let file1 = temp_dir.join("file1.txt");
        let file2 = temp_dir.join("file2.txt");
        let file3 = temp_dir.join("file3.txt");
        std::fs::write(&file1, b"content1").unwrap();
        std::fs::write(&file2, b"content2").unwrap();
        std::fs::write(&file3, b"content3").unwrap();

        let cap = create_test_cap(
            "cap:in=\"media:\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:".to_string() },
                ],
            )],
        );

        // Build CBOR payload with Array of file paths (CBOR mode only)
        let arg = ciborium::Value::Map(vec![
            (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:file-path;textable;form=list".to_string())),
            (ciborium::Value::Text("value".to_string()), ciborium::Value::Array(vec![
                ciborium::Value::Text(file1.to_string_lossy().to_string()),
                ciborium::Value::Text(file2.to_string_lossy().to_string()),
                ciborium::Value::Text(file3.to_string_lossy().to_string()),
            ])),
        ]);
        let args = ciborium::Value::Array(vec![arg]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // Do file-path conversion with is_cli_mode=false (CBOR mode allows Arrays)
        let result = extract_effective_payload(&payload, Some("application/cbor"), &cap, false).unwrap();

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
        let files_array = result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "value"))
            .map(|(_, v)| match v {
                ciborium::Value::Array(arr) => arr,
                _ => panic!("Expected array"),
            })
            .unwrap();

        // Verify all three files were read
        assert_eq!(files_array.len(), 3, "Should read all three files from CBOR Array");

        // Verify contents
        let mut contents = Vec::new();
        for val in files_array {
            match val {
                ciborium::Value::Bytes(b) => contents.push(b.clone()),
                _ => panic!("Expected bytes"),
            }
        }
        contents.sort();
        assert_eq!(contents, vec![b"content1".to_vec(), b"content2".to_vec(), b"content3".to_vec()]);

        // Verify media_urn was converted
        let media_urn = result_map.iter()
            .find(|(k, _)| matches!(k, ciborium::Value::Text(s) if s == "media_urn"))
            .map(|(_, v)| match v {
                ciborium::Value::Text(s) => s,
                _ => panic!("Expected text"),
            })
            .unwrap();
        assert_eq!(media_urn, "media:", "media_urn should be converted to stdin source");

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

        let runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());
        let data = b"small payload";
        let reader = Cursor::new(data.to_vec());

        let payload = runtime.build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk).unwrap();

        // Verify CBOR structure
        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        match cbor_val {
            ciborium::Value::Array(arr) => {
                assert_eq!(arr.len(), 1, "Should have one argument");
                match &arr[0] {
                    ciborium::Value::Map(map) => {
                        let value = map.iter()
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

        let runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());
        // Use small max_chunk to force multi-chunk
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let reader = Cursor::new(data.clone());

        let payload = runtime.build_payload_from_streaming_reader(&cap, reader, 100).unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map.iter()
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

        let runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());
        let reader = Cursor::new(Vec::<u8>::new());

        let payload = runtime.build_payload_from_streaming_reader(&cap, reader, Limits::default().max_chunk).unwrap();

        let cbor_val: ciborium::Value = ciborium::from_reader(&payload[..]).unwrap();
        let arr = match cbor_val {
            ciborium::Value::Array(a) => a,
            _ => panic!("Expected Array"),
        };
        let map = match &arr[0] {
            ciborium::Value::Map(m) => m,
            _ => panic!("Expected Map"),
        };
        let value = map.iter()
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
                Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "simulated read error"))
            }
        }

        let cap = create_test_cap(
            "cap:in=\"media:\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![],
        );

        let runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());
        let result = runtime.build_payload_from_streaming_reader(&cap, ErrorReader, Limits::default().max_chunk);

        assert!(result.is_err(), "IO error should propagate");
        match result {
            Err(RuntimeError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            }
            Err(e) => panic!("Expected RuntimeError::Io, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // TEST478: PluginRuntime auto-registers identity and discard handlers on construction
    #[test]
    fn test478_auto_registers_identity_handler() {
        let runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());

        // Identity handler must be registered at exact CAP_IDENTITY URN
        assert!(runtime.find_handler(CAP_IDENTITY).is_some(),
            "PluginRuntime must auto-register identity handler");

        // Discard handler must be registered at exact CAP_DISCARD URN
        assert!(runtime.find_handler(CAP_DISCARD).is_some(),
            "PluginRuntime must auto-register discard handler");

        // Standard handlers must NOT match arbitrary specific requests
        // (request is pattern, registered cap is instance — broad caps don't satisfy specific patterns)
        assert!(runtime.find_handler("cap:in=\"media:void\";op=nonexistent;out=\"media:void\"").is_none(),
            "Standard handlers must not catch arbitrary specific requests");
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
            fn metadata(&self) -> OpMetadata { OpMetadata::builder("FailOp").build() }
        }

        let mut runtime = PluginRuntime::new(VALID_MANIFEST.as_bytes());

        // Auto-registered identity handler must exist
        assert!(runtime.find_handler(CAP_IDENTITY).is_some(),
            "Auto-registered identity must exist before override");

        // Count handlers before override
        let handlers_before = runtime.handlers.len();

        // Override identity with a custom Op
        runtime.register_op_type::<FailOp>(CAP_IDENTITY);

        // Handler count must not change (HashMap insert replaces, doesn't add)
        assert_eq!(runtime.handlers.len(), handlers_before,
            "Overriding identity must replace, not add a new entry");

        // The handler at CAP_IDENTITY must still be findable
        assert!(runtime.find_handler(CAP_IDENTITY).is_some(),
            "Identity handler must be findable after override");

        // Also verify discard was NOT affected by the override
        assert!(runtime.find_handler(CAP_DISCARD).is_some(),
            "Discard handler must still be present after overriding identity");
    }

    // =========================================================================
    // Stream Abstractions Tests (InputStream, InputPackage, OutputStream, PeerCall)
    // =========================================================================

    use crossbeam_channel::{unbounded, Sender};
    use ciborium::Value;
    use std::sync::Arc;

    // Helper: Create test InputStream from chunks
    fn create_test_input_stream(media_urn: &str, chunks: Vec<Result<Value, StreamError>>) -> InputStream {
        let (tx, rx) = unbounded();
        for chunk in chunks {
            tx.send(chunk).unwrap();
        }
        drop(tx); // Close channel
        InputStream {
            media_urn: media_urn.to_string(),
            rx,
        }
    }

    // TEST529: InputStream iterator yields chunks in order
    #[test]
    fn test529_input_stream_iterator_order() {
        let chunks = vec![
            Ok(Value::Bytes(b"chunk1".to_vec())),
            Ok(Value::Bytes(b"chunk2".to_vec())),
            Ok(Value::Bytes(b"chunk3".to_vec())),
        ];
        let stream = create_test_input_stream("media:test", chunks);

        let collected: Vec<_> = stream.collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].as_ref().unwrap(), &Value::Bytes(b"chunk1".to_vec()));
        assert_eq!(collected[1].as_ref().unwrap(), &Value::Bytes(b"chunk2".to_vec()));
        assert_eq!(collected[2].as_ref().unwrap(), &Value::Bytes(b"chunk3".to_vec()));
    }

    // TEST530: InputStream::collect_bytes concatenates byte chunks
    #[test]
    fn test530_input_stream_collect_bytes() {
        let chunks = vec![
            Ok(Value::Bytes(b"hello".to_vec())),
            Ok(Value::Bytes(b" ".to_vec())),
            Ok(Value::Bytes(b"world".to_vec())),
        ];
        let stream = create_test_input_stream("media:", chunks);

        let result = stream.collect_bytes().expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST531: InputStream::collect_bytes handles text chunks
    #[test]
    fn test531_input_stream_collect_bytes_text() {
        let chunks = vec![
            Ok(Value::Text("hello".to_string())),
            Ok(Value::Text(" world".to_string())),
        ];
        let stream = create_test_input_stream("media:text", chunks);

        let result = stream.collect_bytes().expect("collect must succeed");
        assert_eq!(result, b"hello world");
    }

    // TEST532: InputStream empty stream produces empty bytes
    #[test]
    fn test532_input_stream_empty() {
        let chunks = vec![];
        let stream = create_test_input_stream("media:void", chunks);

        let result = stream.collect_bytes().expect("empty stream must succeed");
        assert_eq!(result, b"");
    }

    // TEST533: InputStream propagates errors
    #[test]
    fn test533_input_stream_error_propagation() {
        let chunks = vec![
            Ok(Value::Bytes(b"data".to_vec())),
            Err(StreamError::Protocol("test error".to_string())),
        ];
        let stream = create_test_input_stream("media:test", chunks);

        let result = stream.collect_bytes();
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

    // TEST535: InputPackage iterator yields streams
    #[test]
    fn test535_input_package_iteration() {
        let (tx, rx) = unbounded();

        // Send 3 streams
        for i in 0..3 {
            let (stream_tx, stream_rx) = unbounded();
            stream_tx.send(Ok(Value::Bytes(format!("stream{}", i).into_bytes()))).unwrap();
            drop(stream_tx);

            tx.send(Ok(InputStream {
                media_urn: format!("media:stream{}", i),
                rx: stream_rx,
            })).unwrap();
        }
        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let streams: Vec<_> = package.collect();
        assert_eq!(streams.len(), 3, "must yield 3 streams");

        for (i, result) in streams.iter().enumerate() {
            assert!(result.is_ok(), "stream {} must be Ok", i);
            let stream = result.as_ref().unwrap();
            assert_eq!(stream.media_urn(), format!("media:stream{}", i));
        }
    }

    // TEST536: InputPackage::collect_all_bytes aggregates all streams
    #[test]
    fn test536_input_package_collect_all_bytes() {
        let (tx, rx) = unbounded();

        // Stream 1: "hello"
        let (s1_tx, s1_rx) = unbounded();
        s1_tx.send(Ok(Value::Bytes(b"hello".to_vec()))).unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s1".to_string(),
            rx: s1_rx,
        })).unwrap();

        // Stream 2: " world"
        let (s2_tx, s2_rx) = unbounded();
        s2_tx.send(Ok(Value::Bytes(b" world".to_vec()))).unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:s2".to_string(),
            rx: s2_rx,
        })).unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package.collect_all_bytes().expect("must succeed");
        assert_eq!(all_bytes, b"hello world");
    }

    // TEST537: InputPackage empty package produces empty bytes
    #[test]
    fn test537_input_package_empty() {
        let (tx, rx) = unbounded();
        drop(tx); // No streams

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let all_bytes = package.collect_all_bytes().expect("empty package must succeed");
        assert_eq!(all_bytes, b"");
    }

    // TEST538: InputPackage propagates stream errors
    #[test]
    fn test538_input_package_error_propagation() {
        let (tx, rx) = unbounded();

        // Good stream
        let (s1_tx, s1_rx) = unbounded();
        s1_tx.send(Ok(Value::Bytes(b"data".to_vec()))).unwrap();
        drop(s1_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:good".to_string(),
            rx: s1_rx,
        })).unwrap();

        // Error stream
        let (s2_tx, s2_rx) = unbounded();
        s2_tx.send(Err(StreamError::Protocol("stream error".to_string()))).unwrap();
        drop(s2_tx);
        tx.send(Ok(InputStream {
            media_urn: "media:bad".to_string(),
            rx: s2_rx,
        })).unwrap();

        drop(tx);

        let package = InputPackage {
            rx,
            _demux_handle: None,
        };

        let result = package.collect_all_bytes();
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

        stream.emit_cbor(&Value::Bytes(b"test".to_vec())).expect("write must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured.len() >= 1, "must send at least STREAM_START");
        assert_eq!(captured[0].frame_type, FrameType::StreamStart,
                   "first frame must be STREAM_START");
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
        stream.emit_cbor(&Value::Bytes(b"chunk1".to_vec())).unwrap();
        stream.emit_cbor(&Value::Bytes(b"chunk2".to_vec())).unwrap();
        stream.emit_cbor(&Value::Bytes(b"chunk3".to_vec())).unwrap();

        stream.close().expect("close must succeed");

        let captured = frames.lock().unwrap();
        let stream_end = captured.iter().find(|f| f.frame_type == FrameType::StreamEnd)
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
        let large_data = vec![0xAA; 250];
        stream.emit_cbor(&Value::Bytes(large_data)).unwrap();
        stream.close().unwrap();

        let captured = frames.lock().unwrap();
        let chunks: Vec<_> = captured.iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .collect();

        assert!(chunks.len() >= 3, "large data must be chunked (got {} chunks)", chunks.len());
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

        stream.close().expect("close must succeed");

        let captured = frames.lock().unwrap();
        assert!(captured.iter().any(|f| f.frame_type == FrameType::StreamStart));
        assert!(captured.iter().any(|f| f.frame_type == FrameType::StreamEnd));

        let chunk_count = captured.iter()
            .filter(|f| f.frame_type == FrameType::Chunk)
            .count();
        assert_eq!(chunk_count, 0, "empty stream must have zero chunks");
    }

    // TEST543: PeerCall::arg creates OutputStream with correct stream_id
    #[test]
    fn test543_peer_call_arg_creates_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded();
        drop(response_tx); // Close immediately for test

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: MessageId::new_uuid(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let arg_stream = peer.arg("media:argument");
        assert_eq!(arg_stream.media_urn, "media:argument");
        assert!(!arg_stream.stream_id.is_empty(), "stream_id must be generated");
    }

    // TEST544: PeerCall::finish sends END frame
    #[test]
    fn test544_peer_call_finish_sends_end() {
        let (sender, frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded();

        // Send empty response stream
        drop(response_tx);

        let request_id = MessageId::new_uuid();
        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: request_id.clone(),
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let _response = peer.finish().expect("finish must succeed");

        let captured = frames.lock().unwrap();
        let end_frame = captured.iter().find(|f| f.frame_type == FrameType::End)
            .expect("must send END frame");

        assert_eq!(end_frame.id, request_id, "END must have correct request ID");
    }

    // TEST545: PeerCall::finish returns InputStream for response
    #[test]
    fn test545_peer_call_finish_returns_response_stream() {
        let (sender, _frames) = MockFrameSender::new();
        let (response_tx, response_rx) = unbounded();

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
        response_tx.send(Frame::chunk(
            req_id.clone(),
            "response-stream".to_string(),
            0,
            cbor_payload,
            0,
            checksum,
        )).unwrap();

        // STREAM_END
        response_tx.send(Frame::stream_end(req_id.clone(), "response-stream".to_string(), 1)).unwrap();
        drop(response_tx);

        let peer = PeerCall {
            sender: Arc::new(sender),
            request_id: req_id,
            max_chunk: 256_000,
            response_rx: Some(response_rx),
        };

        let response_stream = peer.finish().expect("finish must succeed");
        assert_eq!(response_stream.media_urn(), "media:response");

        let bytes = response_stream.collect_bytes().expect("collect must succeed");
        assert_eq!(bytes, b"response data");
    }
}
