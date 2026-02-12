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
//! use capns::{PluginRuntime, StreamEmitter, CapManifest};
//!
//! fn main() {
//!     let manifest = build_manifest(); // Your manifest with caps
//!     let mut runtime = PluginRuntime::new(manifest);
//!
//!     runtime.register::<MyRequest, _>("cap:op=my_op;...", |request, emitter, peer| {
//!         emitter.emit_log("info", "Starting work...");
//!         // Do work, emit chunks in real-time
//!         emitter.emit_cbor(&ciborium::Value::Bytes(b"partial result".to_vec()));
//!         // Return final result
//!         Ok(b"final result".to_vec())
//!     });
//!
//!     // runtime.run() automatically detects CLI vs Plugin CBOR mode
//!     runtime.run().unwrap();
//! }
//! ```

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake_accept, CborError, FrameReader, FrameWriter};
use crate::caller::CapArgumentValue;
use crate::cap::{ArgSource, Cap, CapArg};
use crate::cap_urn::CapUrn;
use crate::manifest::CapManifest;
use crate::media_urn::{MediaUrn, MEDIA_FILE_PATH, MEDIA_FILE_PATH_ARRAY};
use crossbeam_channel::{bounded, Receiver, Sender};
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
}

/// A streaming emitter that writes chunks immediately to the output.
/// Thread-safe for use in concurrent handlers.
/// Handlers emit CBOR values via emit_cbor() or logs via emit_log().
pub trait StreamEmitter: Send + Sync {
    /// Emit a CBOR value as output.
    /// The value is CBOR-encoded once and sent as raw CBOR bytes in CHUNK frames.
    /// No double-encoding: one CBOR layer from handler to consumer.
    fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError>;

    /// Emit a log message at the given level.
    /// Sends a LOG frame (side-channel, does not affect response stream).
    fn emit_log(&self, level: &str, message: &str);
}

/// Allows handlers to invoke caps on the peer (host).
///
/// This trait enables bidirectional communication where a plugin handler can
/// invoke caps on the host while processing a request. This is essential for
/// sandboxed plugins that need to delegate certain operations (like model
/// downloading) to the host.
///
/// The `invoke` method sends a request and returns a receiver that yields
/// decoded `StreamChunk` values — the same pattern handlers receive their
/// own input in. Chunks arrive as they are received from the host; the
/// stream may be infinite so callers must NOT wait for END.
pub trait PeerInvoker: Send + Sync {
    /// Invoke a cap on the host with arguments.
    ///
    /// Sends REQ + streaming argument frames to the host.
    /// Spawns a thread that receives response frames and forwards them to a channel.
    /// Returns a receiver that yields bare CBOR Frame objects (STREAM_START, CHUNK,
    /// STREAM_END, END, ERR) as they arrive from the host. The consumer processes
    /// frames directly - no decoding, no wrapper types.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke on the host
    /// * `arguments` - Arguments identified by media_urn
    ///
    /// # Returns
    /// A receiver that yields bare Frame objects.
    ///
    /// # Example
    /// ```ignore
    /// let args = vec![CapArgumentValue::new("media:bytes", payload)];
    /// let rx = peer.invoke("cap:op=echo;...", &args)?;
    /// let mut result = Vec::new();
    /// for frame in rx {
    ///     match frame.frame_type {
    ///         FrameType::Chunk => {
    ///             if let Some(payload) = frame.payload {
    ///                 result.extend(payload);
    ///             }
    ///         }
    ///         FrameType::End => break,
    ///         _ => {}
    ///     }
    /// }
    /// ```
    fn invoke(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Frame>, RuntimeError>;
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported.
pub struct NoPeerInvoker;

impl PeerInvoker for NoPeerInvoker {
    fn invoke(
        &self,
        _cap_urn: &str,
        _arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Frame>, RuntimeError> {
        Err(RuntimeError::PeerRequest(
            "Peer invocation not supported in this context".to_string(),
        ))
    }
}

/// Thread-safe implementation of StreamEmitter that writes CBOR frames.
/// Uses Arc<Mutex<>> for safe concurrent access from multiple handler threads.
/// Implements stream multiplexing protocol: sends STREAM_START before first chunk.
struct ThreadSafeEmitter<W: Write + Send> {
    writer: Arc<Mutex<FrameWriter<W>>>,
    request_id: MessageId,
    stream_id: String,  // Response stream ID
    media_urn: String,  // Response media URN
    stream_started: AtomicBool,  // Track if STREAM_START was sent
    seq: Mutex<u64>,
    max_chunk: usize,  // Negotiated max chunk size
}

impl<W: Write + Send> StreamEmitter for ThreadSafeEmitter<W> {
    fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
        // CHUNK payloads = complete, independently decodable CBOR values
        //
        // Streams might never end (logs, video, real-time data), so each CHUNK must be
        // processable immediately without waiting for END frame.
        //
        // For Value::Bytes/Text: split raw data, encode each chunk as complete Value
        // For other types: encode once (typically small)
        //
        // Each CHUNK payload can be decoded independently: cbor2.loads(chunk.payload)

        // STREAM MULTIPLEXING PROTOCOL: Send STREAM_START before first chunk
        if !self.stream_started.swap(true, Ordering::SeqCst) {
            let start_frame = Frame::stream_start(
                self.request_id.clone(),
                self.stream_id.clone(),
                self.media_urn.clone()
            );

            let mut writer = self.writer.lock().unwrap();
            writer.write(&start_frame)
                .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write STREAM_START: {}", e))))?;
            drop(writer); // Release lock before continuing
        }

        // Split large byte/text data, encode each chunk as complete CBOR value
        match value {
            ciborium::Value::Bytes(bytes) => {
                // Split bytes BEFORE encoding, encode each chunk as Value::Bytes
                let mut offset = 0;
                while offset < bytes.len() {
                    let chunk_size = (bytes.len() - offset).min(self.max_chunk);
                    let chunk_bytes = bytes[offset..offset + chunk_size].to_vec();

                    // Encode as complete Value::Bytes - independently decodable
                    let chunk_value = ciborium::Value::Bytes(chunk_bytes);
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(&chunk_value, &mut cbor_payload)
                        .map_err(|e| RuntimeError::Handler(format!("Failed to encode chunk: {}", e)))?;

                    let seq = {
                        let mut seq_guard = self.seq.lock().unwrap();
                        let current = *seq_guard;
                        *seq_guard += 1;
                        current
                    };

                    let frame = Frame::chunk(self.request_id.clone(), self.stream_id.clone(), seq, cbor_payload);

                    let mut writer = self.writer.lock().unwrap();
                    writer.write(&frame)
                        .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write chunk: {}", e))))?;
                    drop(writer);

                    offset += chunk_size;
                }
            }
            ciborium::Value::Text(text) => {
                // Split text BEFORE encoding, encode each chunk as Value::Text
                let text_bytes = text.as_bytes();
                let mut offset = 0;
                while offset < text_bytes.len() {
                    // Ensure we split on UTF-8 character boundaries
                    let mut chunk_size = (text_bytes.len() - offset).min(self.max_chunk);
                    while chunk_size > 0 && !text.is_char_boundary(offset + chunk_size) {
                        chunk_size -= 1;
                    }
                    if chunk_size == 0 {
                        return Err(RuntimeError::Handler("Cannot split text on character boundary".to_string()));
                    }

                    let chunk_text = text[offset..offset + chunk_size].to_string();

                    // Encode as complete Value::Text - independently decodable
                    let chunk_value = ciborium::Value::Text(chunk_text);
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(&chunk_value, &mut cbor_payload)
                        .map_err(|e| RuntimeError::Handler(format!("Failed to encode chunk: {}", e)))?;

                    let seq = {
                        let mut seq_guard = self.seq.lock().unwrap();
                        let current = *seq_guard;
                        *seq_guard += 1;
                        current
                    };

                    let frame = Frame::chunk(self.request_id.clone(), self.stream_id.clone(), seq, cbor_payload);

                    let mut writer = self.writer.lock().unwrap();
                    writer.write(&frame)
                        .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write chunk: {}", e))))?;
                    drop(writer);

                    offset += chunk_size;
                }
            }
            ciborium::Value::Array(elements) => {
                // Array: send each element as independent CBOR chunk
                // Allows receiver to reconstruct elements without waiting for entire array
                for element in elements {
                    // Encode each element as complete CBOR value
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(element, &mut cbor_payload)
                        .map_err(|e| RuntimeError::Handler(format!("Failed to encode array element: {}", e)))?;

                    let seq = {
                        let mut seq_guard = self.seq.lock().unwrap();
                        let current = *seq_guard;
                        *seq_guard += 1;
                        current
                    };

                    let frame = Frame::chunk(self.request_id.clone(), self.stream_id.clone(), seq, cbor_payload);

                    let mut writer = self.writer.lock().unwrap();
                    writer.write(&frame)
                        .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write chunk: {}", e))))?;
                    drop(writer);
                }
            }
            ciborium::Value::Map(entries) => {
                // Map: send each entry as independent CBOR chunk
                // Receiver must wait for all entries before reconstructing map
                for (key, val) in entries {
                    // Encode each key-value pair as a 2-element array: [key, value]
                    let entry = ciborium::Value::Array(vec![key.clone(), val.clone()]);
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(&entry, &mut cbor_payload)
                        .map_err(|e| RuntimeError::Handler(format!("Failed to encode map entry: {}", e)))?;

                    let seq = {
                        let mut seq_guard = self.seq.lock().unwrap();
                        let current = *seq_guard;
                        *seq_guard += 1;
                        current
                    };

                    let frame = Frame::chunk(self.request_id.clone(), self.stream_id.clone(), seq, cbor_payload);

                    let mut writer = self.writer.lock().unwrap();
                    writer.write(&frame)
                        .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write chunk: {}", e))))?;
                    drop(writer);
                }
            }
            _ => {
                // For other types (Integer, Float, Bool, Null, Tag): encode as single chunk
                // These have single-value semantics and are typically small
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(value, &mut cbor_payload)
                    .map_err(|e| RuntimeError::Handler(format!("Failed to encode CBOR value: {}", e)))?;

                let seq = {
                    let mut seq_guard = self.seq.lock().unwrap();
                    let current = *seq_guard;
                    *seq_guard += 1;
                    current
                };

                let frame = Frame::chunk(self.request_id.clone(), self.stream_id.clone(), seq, cbor_payload);

                let mut writer = self.writer.lock().unwrap();
                writer.write(&frame)
                    .map_err(|e| RuntimeError::Io(io::Error::new(io::ErrorKind::Other, format!("Failed to write chunk: {}", e))))?;
            }
        }

        Ok(())
    }

    fn emit_log(&self, level: &str, message: &str) {
        let frame = Frame::log(self.request_id.clone(), level, message);

        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write log: {}", e);
        }
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

impl StreamEmitter for CliStreamEmitter {
    fn emit_cbor(&self, value: &ciborium::Value) -> Result<(), RuntimeError> {
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

    fn emit_log(&self, level: &str, message: &str) {
        // In CLI mode, logs go to stderr
        eprintln!("[{}] {}", level.to_uppercase(), message);
    }
}

// StreamChunk removed - handlers now receive bare CBOR Frame objects directly

/// Handler function type - must be Send + Sync for concurrent execution.
///
/// STREAM MULTIPLEXING ARCHITECTURE:
/// Handlers receive multiple argument streams via StreamChunk messages.
/// Each chunk identifies its stream (stream_id) and type (media_urn).
/// Handlers can process streams as they arrive, enabling:
/// - Multiple large arguments streamed simultaneously
/// - Configuration args sent first (no form ordering needed)
/// - Infinite streams (video, audio, real-time data)
/// - Low memory footprint (no buffering all args)
///
/// Handler receives:
/// - Stream chunks via mpsc::Receiver<StreamChunk>
/// - StreamEmitter for emitting output chunks
/// - PeerInvoker for calling host caps
/// Handler function that processes CBOR frames.
/// Receives bare Frame objects for both input arguments and peer responses.
/// Handler has full streaming control - decides when to consume frames and when to produce output.
pub type HandlerFn = Arc<
    dyn Fn(Receiver<Frame>, &dyn StreamEmitter, &dyn PeerInvoker) -> Result<(), RuntimeError> + Send + Sync,
>;

/// Stream data accumulator for multiplexed protocol
#[derive(Clone)]
struct PendingStream {
    media_urn: String,
    chunks: Vec<Vec<u8>>,
    complete: bool,
}

/// Internal struct to track pending peer requests (plugin invoking host caps).
/// Uses stream multiplexing protocol to track response streams.
/// Reader loop sends decoded StreamChunk values as they arrive; closes channel on END/ERR.
/// Tracks a pending peer request (plugin invoking host cap).
/// The reader loop forwards response frames to the channel.
struct PendingPeerRequest {
    sender: Sender<Frame>,  // Channel to send response frames to handler
    ended: bool,  // true after END frame (close channel)
}

/// Implementation of PeerInvoker that sends REQ frames to the host.
struct PeerInvokerImpl<W: Write + Send> {
    writer: Arc<Mutex<FrameWriter<W>>>,
    pending_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
    max_chunk: usize,  // Negotiated max chunk size
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

            eprintln!("[PluginRuntime] Checking arg: media_urn={:?}, has_value={}", media_urn, value_ref.is_some());

            // Check if this is a file-path argument using proper URN matching
            if let (Some(ref urn_str), Some(value)) = (media_urn, value_ref) {
                let arg_urn = MediaUrn::from_string(urn_str)
                    .map_err(|e| RuntimeError::Handler(format!("Invalid argument media URN '{}': {}", urn_str, e)))?;

                // Check if it's a file-path at all
                let is_file_path = file_path_base.accepts(&arg_urn)
                    .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;

                eprintln!("[PluginRuntime] is_file_path={}", is_file_path);

                if is_file_path {
                    // Determine if it's scalar or list - MUST be one or the other
                    let is_scalar = file_path_scalar.accepts(&arg_urn)
                        .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;
                    let is_list = file_path_list.accepts(&arg_urn)
                        .map_err(|e| RuntimeError::Handler(format!("URN matching failed: {}", e)))?;

                    eprintln!("[PluginRuntime] is_scalar={}, is_list={}", is_scalar, is_list);

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
                        eprintln!("[PluginRuntime] Converting single file-path '{}' to bytes", path_str);
                        let file_bytes = std::fs::read(path_str.as_ref())
                            .map_err(|e| RuntimeError::Handler(format!("Failed to read file '{}': {}", path_str, e)))?;

                        // Find target media_urn from arg_to_stdin map
                        let target_urn = arg_to_stdin.get(urn_str)
                            .cloned()
                            .unwrap_or_else(|| expected_input.clone());

                        eprintln!("[PluginRuntime] Read {} bytes, converting media_urn to '{}'", file_bytes.len(), target_urn);

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
                                eprintln!("[PluginRuntime] Converting CBOR array of {} file-paths to bytes", arr.len());
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

                        eprintln!("[PluginRuntime] Processing {} path(s)", paths_to_process.len());

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

                        eprintln!("[PluginRuntime] Read {} files, total CBOR array elements", files_data.len());

                        // Find target media_urn from arg_to_stdin map
                        let target_urn = arg_to_stdin.get(urn_str)
                            .cloned()
                            .unwrap_or_else(|| expected_input.clone());

                        eprintln!("[PluginRuntime] Converting media_urn to '{}'", target_urn);

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
    // After file-path conversion, arg media_urn may be the stdin target (e.g., "media:bytes")
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

    eprintln!("[PluginRuntime] Returning modified CBOR array ({} bytes) with validated matching argument", serialized.len());
    Ok(serialized)
}

impl<W: Write + Send> PeerInvoker for PeerInvokerImpl<W> {
    fn invoke(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Frame>, RuntimeError> {
        // Generate a new message ID for this request
        let request_id = MessageId::new_uuid();

        // Create a bounded channel for response frames
        let (sender, receiver) = bounded(64);

        // Register the pending request before sending
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(request_id.clone(), PendingPeerRequest {
                sender,
                ended: false,
            });
        }

        // STREAM MULTIPLEXING PROTOCOL: Send REQ + streams for each argument + END
        let max_chunk = self.max_chunk;

        {
            let mut writer = self.writer.lock().unwrap();

            // 1. Send REQ with empty payload
            let req_frame = Frame::req(
                request_id.clone(),
                cap_urn,
                vec![], // Empty payload - arguments come as streams
                "application/cbor",
            );
            writer.write(&req_frame).map_err(|e| {
                self.pending_requests.lock().unwrap().remove(&request_id);
                RuntimeError::PeerRequest(format!("Failed to send REQ frame: {}", e))
            })?;

            // 2. Send each argument as an independent stream
            for arg in arguments {
                let stream_id = uuid::Uuid::new_v4().to_string();

                // STREAM_START: Announce new stream
                let start_frame = Frame::stream_start(
                    request_id.clone(),
                    stream_id.clone(),
                    arg.media_urn.clone()
                );
                writer.write(&start_frame).map_err(|e| {
                    self.pending_requests.lock().unwrap().remove(&request_id);
                    RuntimeError::PeerRequest(format!("Failed to send STREAM_START: {}", e))
                })?;

                // CHUNK(s): Send argument data as CBOR-encoded chunks
                // Each CHUNK payload MUST be independently decodable CBOR
                let mut offset = 0;
                let mut seq = 0u64;
                while offset < arg.value.len() {
                    let chunk_size = (arg.value.len() - offset).min(max_chunk);
                    let chunk_bytes = arg.value[offset..offset + chunk_size].to_vec();

                    // CBOR-encode chunk as Value::Bytes - independently decodable
                    let chunk_value = ciborium::Value::Bytes(chunk_bytes);
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(&chunk_value, &mut cbor_payload)
                        .map_err(|e| {
                            self.pending_requests.lock().unwrap().remove(&request_id);
                            RuntimeError::PeerRequest(format!("Failed to encode chunk: {}", e))
                        })?;

                    let chunk_frame = Frame::chunk(
                        request_id.clone(),
                        stream_id.clone(),
                        seq,
                        cbor_payload
                    );
                    writer.write(&chunk_frame).map_err(|e| {
                        self.pending_requests.lock().unwrap().remove(&request_id);
                        RuntimeError::PeerRequest(format!("Failed to send CHUNK: {}", e))
                    })?;

                    offset += chunk_size;
                    seq += 1;
                }

                // STREAM_END: Close this stream
                let end_frame = Frame::stream_end(request_id.clone(), stream_id);
                writer.write(&end_frame).map_err(|e| {
                    self.pending_requests.lock().unwrap().remove(&request_id);
                    RuntimeError::PeerRequest(format!("Failed to send STREAM_END: {}", e))
                })?;
            }

            // 3. END: Close the entire request
            let request_end = Frame::end(request_id.clone(), None);
            writer.write(&request_end).map_err(|e| {
                self.pending_requests.lock().unwrap().remove(&request_id);
                RuntimeError::PeerRequest(format!("Failed to send END frame: {}", e))
            })?;
        }

        Ok(receiver)
    }
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
    /// Registered handlers by cap URN pattern (Arc for thread-safe sharing)
    handlers: HashMap<String, HandlerFn>,

    /// Plugin manifest JSON data - sent in HELLO response.
    /// This is REQUIRED - plugins must provide their manifest.
    manifest_data: Vec<u8>,

    /// Parsed manifest for CLI mode processing
    manifest: Option<CapManifest>,

    /// Negotiated protocol limits
    limits: Limits,
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
    pub fn new(manifest: &[u8]) -> Self {
        // Try to parse the manifest for CLI mode support
        let parsed_manifest = serde_json::from_slice::<CapManifest>(manifest).ok();

        Self {
            handlers: HashMap::new(),
            manifest_data: manifest.to_vec(),
            manifest: parsed_manifest,
            limits: Limits::default(),
        }
    }

    /// Create a new plugin runtime with a pre-built CapManifest.
    /// This is the preferred method as it ensures the manifest is valid.
    pub fn with_manifest(manifest: CapManifest) -> Self {
        let manifest_data = serde_json::to_vec(&manifest).unwrap_or_default();
        Self {
            handlers: HashMap::new(),
            manifest_data,
            manifest: Some(manifest),
            limits: Limits::default(),
        }
    }

    /// Create a new plugin runtime with manifest JSON string.
    pub fn with_manifest_json(manifest_json: &str) -> Self {
        Self::new(manifest_json.as_bytes())
    }

    /// Register a handler for a cap URN.
    ///
    /// The handler receives:
    /// - The request payload as bytes (typically JSON or CBOR)
    /// - An emitter for streaming output
    /// - A peer invoker for calling caps on the host
    ///
    /// It returns the final response payload bytes.
    ///
    /// Chunks emitted by the handler are written immediately to stdout.
    /// This is essential for progress updates and real-time token streaming.
    ///
    /// **Thread safety**: Handlers run in separate threads, so they must be
    /// Send + Sync. The emitter and peer invoker are thread-safe and can be used freely.
    ///
    /// **Peer invocation**: Use the `peer` parameter to invoke caps on the host.
    /// This is useful for sandboxed plugins that need to delegate operations
    /// (like network access) to the host.
    /// Register a streaming handler with automatic deserialization.
    ///
    /// NOTE: This method accumulates chunks to deserialize the complete request.
    /// For true streaming without deserialization, use register_raw instead.
    pub fn register<Req, F>(&mut self, cap_urn: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned + 'static,
        F: Fn(Req, &dyn StreamEmitter, &dyn PeerInvoker) -> Result<(), RuntimeError> + Send + Sync + 'static,
    {
        let streaming_handler = move |frames: Receiver<Frame>, emitter: &dyn StreamEmitter, peer: &dyn PeerInvoker| -> Result<(), RuntimeError> {
            // Accumulate all CHUNK frame payloads
            let mut accumulated = Vec::new();
            for frame in frames {
                match frame.frame_type {
                    FrameType::Chunk => {
                        if let Some(payload) = frame.payload {
                            accumulated.extend_from_slice(&payload);
                        }
                    }
                    FrameType::End => break,
                    FrameType::Err => {
                        let code = frame.error_code().unwrap_or("UNKNOWN");
                        let message = frame.error_message().unwrap_or("Unknown error");
                        return Err(RuntimeError::Handler(format!("[{}] {}", code, message)));
                    }
                    _ => {} // Ignore STREAM_START, STREAM_END, etc.
                }
            }

            // Deserialize complete request from accumulated bytes (JSON format)
            let request: Req = serde_json::from_slice(&accumulated)
                .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse request: {}", e)))?;

            handler(request, emitter, peer)
        };

        self.handlers.insert(cap_urn.to_string(), Arc::new(streaming_handler));
    }

    /// Register a raw handler for a cap URN.
    ///
    /// The handler receives bare CBOR Frame objects for streaming input.
    /// Frames include REQ, STREAM_START, CHUNK, STREAM_END, END.
    /// Handler has full control over when to consume frames and when to produce output.
    ///
    /// Benefits:
    /// - True streaming - process frames as they arrive
    /// - No forced accumulation
    /// - Perfect for infinite streams (video, audio, real-time data)
    pub fn register_raw<F>(&mut self, cap_urn: &str, handler: F)
    where
        F: Fn(Receiver<Frame>, &dyn StreamEmitter, &dyn PeerInvoker) -> Result<(), RuntimeError> + Send + Sync + 'static,
    {
        self.handlers.insert(cap_urn.to_string(), Arc::new(handler));
    }

    /// Find a handler for a cap URN.
    /// Returns the handler if found, None otherwise.
    pub fn find_handler(&self, cap_urn: &str) -> Option<HandlerFn> {
        // First try exact match
        if let Some(handler) = self.handlers.get(cap_urn) {
            return Some(Arc::clone(handler));
        }

        // Then try pattern matching via CapUrn
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        for (pattern, handler) in &self.handlers {
            if let Ok(pattern_urn) = CapUrn::from_string(pattern) {
                if pattern_urn.accepts(&request_urn) {
                    return Some(Arc::clone(handler));
                }
            }
        }

        None
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
    /// If stdin is piped (binary data), this streams it in chunks and accumulates
    /// via the same PendingIncomingRequest logic as CBOR mode, ensuring all modes
    /// converge to one unified processing path.
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

        // Find handler
        let handler = self.find_handler(&cap.urn_string()).ok_or_else(|| {
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
            // This uses the same PendingIncomingRequest logic as CBOR mode
            eprintln!("[PluginRuntime] CLI mode: streaming binary from stdin");
            self.build_payload_from_streaming_stdin(&cap)?
        } else {
            // No input provided
            return Err(RuntimeError::MissingArgument(
                "No input provided (expected CLI arguments or piped stdin)".to_string()
            ));
        };

        // Create CLI-mode emitter (no newlines for binary safety) and no-op peer invoker
        let emitter = CliStreamEmitter::without_ndjson();
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
                                // Value can be Bytes (scalar) or Array (list)
                                match v {
                                    ciborium::Value::Bytes(b) => {
                                        // Scalar value - pass raw bytes
                                        value_bytes = Some(b);
                                    }
                                    ciborium::Value::Array(_) => {
                                        // List value - encode as CBOR for handler to decode
                                        let mut cbor_bytes = Vec::new();
                                        ciborium::into_writer(&v, &mut cbor_bytes)
                                            .map_err(|e| RuntimeError::Serialize(format!("Failed to encode array: {}", e)))?;
                                        value_bytes = Some(cbor_bytes);
                                    }
                                    _ => {
                                        // Other CBOR types - encode as CBOR
                                        let mut cbor_bytes = Vec::new();
                                        ciborium::into_writer(&v, &mut cbor_bytes)
                                            .map_err(|e| RuntimeError::Serialize(format!("Failed to encode value: {}", e)))?;
                                        value_bytes = Some(cbor_bytes);
                                    }
                                }
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
                    if bytes.is_empty() {
                        // Empty value - send single empty chunk
                        let chunk_frame = Frame::chunk(request_id.clone(), stream_id.clone(), 0, vec![]);
                        tx.send(chunk_frame).map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                    } else {
                        // Non-empty value - chunk into max_chunk pieces
                        let mut offset = 0;
                        let mut seq = 0u64;
                        while offset < bytes.len() {
                            let chunk_size = (bytes.len() - offset).min(max_chunk);
                            let chunk_data = bytes[offset..offset + chunk_size].to_vec();
                            let chunk_frame = Frame::chunk(request_id.clone(), stream_id.clone(), seq, chunk_data);
                            tx.send(chunk_frame).map_err(|_| RuntimeError::Handler("Failed to send CHUNK".to_string()))?;
                            offset += chunk_size;
                            seq += 1;
                        }
                    }

                    // Send STREAM_END
                    let end_frame = Frame::stream_end(request_id.clone(), stream_id.clone());
                    tx.send(end_frame).map_err(|_| RuntimeError::Handler("Failed to send STREAM_END".to_string()))?;
                }
            }
        }

        // Send END frame to signal request completion
        let end_frame = Frame::end(request_id.clone(), None);
        tx.send(end_frame).map_err(|_| RuntimeError::Handler("Failed to send END".to_string()))?;
        drop(tx); // Close channel

        // Invoke streaming handler
        let result = handler(rx, &emitter, &peer);

        match result {
            Ok(()) => {
                // Handler emitted output via StreamEmitter
                Ok(())
            }
            Err(e) => {
                // Output error as JSON to stderr
                let error_json = serde_json::json!({
                    "error": e.to_string(),
                    "code": "HANDLER_ERROR"
                });
                eprintln!("{}", serde_json::to_string(&error_json).unwrap_or_default());
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
    /// - Accumulated via PendingIncomingRequest (same as CBOR mode)
    /// - Handler invoked when reader EOF (simulates END frame)
    ///
    /// This makes all 4 modes use the SAME PendingIncomingRequest code path:
    /// - CLI file path → read file → payload
    /// - CLI piped binary → chunk reader → PendingIncomingRequest → payload
    /// - CBOR chunked → PendingIncomingRequest → payload
    /// - CBOR file path → auto-convert → payload
    fn build_payload_from_streaming_reader<R: io::Read>(
        &self,
        cap: &Cap,
        mut reader: R,
        max_chunk: usize,
    ) -> Result<Vec<u8>, RuntimeError> {
        eprintln!("[PluginRuntime] CLI mode: streaming PURE BINARY from reader (NOT CBOR)");

        // Simulate PendingIncomingRequest structure (same as CBOR mode)
        struct PendingRequest {
            cap_urn: String,
            chunks: Vec<Vec<u8>>,
        }

        let mut pending = PendingRequest {
            cap_urn: cap.urn_string(),
            chunks: Vec::new(),
        };
        let mut total_bytes = 0;

        loop {
            let mut buffer = vec![0u8; max_chunk];
            match reader.read(&mut buffer) {
                Ok(0) => {
                    // EOF - simulate END frame
                    eprintln!("[PluginRuntime] Reader EOF (simulated END frame), accumulated {} bytes in {} chunks",
                        total_bytes, pending.chunks.len());
                    break;
                }
                Ok(n) => {
                    buffer.truncate(n);
                    total_bytes += n;

                    // Simulate receiving CHUNK frame - add to accumulator immediately
                    pending.chunks.push(buffer);
                    eprintln!("[PluginRuntime] Chunk {} received ({} bytes, total: {}) - simulated CHUNK frame",
                        pending.chunks.len(), n, total_bytes);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Err(RuntimeError::Io(e));
                }
            }
        }

        // Concatenate chunks (same as PendingIncomingRequest does on END frame)
        let complete_payload = pending.chunks.concat();
        eprintln!("[PluginRuntime] Accumulated payload complete: {} bytes from {} chunks",
            complete_payload.len(), pending.chunks.len());

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
    fn read_stdin_if_available(&self) -> Result<Option<Vec<u8>>, RuntimeError> {
        use std::io::IsTerminal;

        let stdin = io::stdin();
        // Don't read from stdin if it's a terminal (interactive)
        if stdin.is_terminal() {
            return Ok(None);
        }

        let mut data = Vec::new();
        stdin.lock().read_to_end(&mut data)?;

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(data))
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
    fn print_help(&self, manifest: &CapManifest) {
        eprintln!("{} v{}", manifest.name, manifest.version);
        eprintln!("{}", manifest.description);
        eprintln!();
        eprintln!("USAGE:");
        eprintln!("    {} <COMMAND> [OPTIONS]", manifest.name.to_lowercase());
        eprintln!();
        eprintln!("COMMANDS:");
        eprintln!("    manifest    Output the plugin manifest as JSON");

        for cap in &manifest.caps {
            let desc = cap.cap_description.as_deref().unwrap_or(&cap.title);
            eprintln!("    {:<12} {}", cap.command, desc);
        }

        eprintln!();
        eprintln!("Run '{} <COMMAND> --help' for more information on a command.", manifest.name.to_lowercase());
    }

    /// Print help for a specific cap.
    fn print_cap_help(&self, cap: &Cap) {
        eprintln!("{}", cap.title);
        if let Some(desc) = &cap.cap_description {
            eprintln!("{}", desc);
        }
        eprintln!();
        eprintln!("USAGE:");
        eprintln!("    plugin {} [OPTIONS]", cap.command);
        eprintln!();

        let args = cap.get_args();
        if !args.is_empty() {
            eprintln!("OPTIONS:");
            for arg in args {
                let required = if arg.required { " (required)" } else { "" };
                let desc = arg.arg_description.as_deref().unwrap_or("");

                for source in &arg.sources {
                    match source {
                        ArgSource::CliFlag { cli_flag } => {
                            eprintln!("    {:<16} {}{}", cli_flag, desc, required);
                        }
                        ArgSource::Position { position } => {
                            eprintln!("    <arg{}>          {}{}", position, desc, required);
                        }
                        ArgSource::Stdin { stdin } => {
                            eprintln!("    (stdin: {}) {}{}", stdin, desc, required);
                        }
                    }
                }
            }
        }
    }

    /// Run in Plugin CBOR mode - binary frame protocol via stdin/stdout.
    fn run_cbor_mode(&self) -> Result<(), RuntimeError> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        // Lock stdin for reading (single reader thread)
        let reader = BufReader::new(stdin.lock());
        // Use Stdout directly (not StdoutLock) so it can be shared across threads.
        // BufWriter provides buffering, our Mutex provides thread safety.
        let writer = BufWriter::new(stdout);

        let mut frame_reader = FrameReader::new(reader);
        let frame_writer = Arc::new(Mutex::new(FrameWriter::new(writer)));

        // Perform handshake - send our manifest in the HELLO response
        let negotiated_limits = {
            let mut writer_guard = frame_writer.lock().unwrap();
            let limits = handshake_accept(&mut frame_reader, &mut writer_guard, &self.manifest_data)?;
            frame_reader.set_limits(limits);
            writer_guard.set_limits(limits);
            limits
        };

        // Track pending peer requests (plugin invoking host caps)
        let pending_peer_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Track incoming requests with multiplexed streams
        #[derive(Clone)]
        struct PendingIncomingRequest {
            cap_urn: String,
            handler: HandlerFn,
            streams: Vec<(String, PendingStream)>,  // (stream_id, stream data) - ordered
            ended: bool,  // true after END frame - any stream activity after is FATAL
        }
        let pending_incoming: Arc<Mutex<HashMap<MessageId, PendingIncomingRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Track active handler threads for cleanup
        let mut active_handlers: Vec<JoinHandle<()>> = Vec::new();

        // Process requests - main loop stays responsive
        loop {
            // Clean up finished handlers periodically
            active_handlers.retain(|h| !h.is_finished());

            let frame = match frame_reader.read()? {
                Some(f) => f,
                None => break, // EOF - stdin closed, exit cleanly
            };

            match frame.frame_type {
                FrameType::Req => {
                    let cap_urn = match frame.cap.as_ref() {
                        Some(urn) => urn.clone(),
                        None => {
                            let err_frame = Frame::err(
                                frame.id,
                                "INVALID_REQUEST",
                                "Request missing cap URN",
                            );
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };

                    let handler = match self.find_handler(&cap_urn) {
                        Some(h) => h,
                        None => {
                            let err_frame = Frame::err(
                                frame.id.clone(),
                                "NO_HANDLER",
                                &format!("No handler registered for cap: {}", cap_urn),
                            );
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };

                    let raw_payload = frame.payload.clone().unwrap_or_default();

                    // NEW PROTOCOL: REQ must have empty payload - arguments come as streams
                    if !raw_payload.is_empty() {
                        let err_frame = Frame::err(
                            frame.id,
                            "PROTOCOL_ERROR",
                            "REQ frame must have empty payload - use STREAM_START for arguments"
                        );
                        let mut writer = frame_writer.lock().unwrap();
                        let _ = writer.write(&err_frame);
                        continue;
                    }

                    // Start tracking this request - streams will be added via STREAM_START
                    let mut pending = pending_incoming.lock().unwrap();
                    pending.insert(frame.id.clone(), PendingIncomingRequest {
                        cap_urn: cap_urn.clone(),
                        handler,
                        streams: Vec::new(),  // Streams added via STREAM_START
                        ended: false,
                    });
                    drop(pending);
                    eprintln!("[PluginRuntime] REQ: req_id={:?} cap={} - waiting for streams", frame.id, cap_urn);
                    continue; // Wait for STREAM_START/CHUNK/STREAM_END/END frames
                }
                FrameType::Chunk => {
                    // NEW PROTOCOL: CHUNK has stream_id identifying which stream
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            // FATAL: Remove request to unblock host
                            pending_incoming.lock().unwrap().remove(&frame.id);
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "CHUNK missing stream_id");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };

                    // STRICT: Check if this is a chunk for an incoming request with validation
                    let mut incoming = pending_incoming.lock().unwrap();
                    if let Some(pending_req) = incoming.get_mut(&frame.id) {
                        // FAIL HARD: Request already ended
                        if pending_req.ended {
                            incoming.remove(&frame.id); // Remove to unblock host
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "CHUNK after request END");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            drop(writer);
                            drop(incoming);
                            continue;
                        }

                        // Append chunk to the appropriate stream with STRICT validation
                        match pending_req.streams.iter_mut().find(|(id, _)| id == &stream_id).map(|(_, s)| s) {
                            Some(stream) if !stream.complete => {
                                // ✅ Valid chunk for active stream
                                if let Some(payload) = frame.payload {
                                    stream.chunks.push(payload);
                                }
                            }
                            Some(_stream) => {
                                // FAIL HARD: Chunk for ended stream
                                incoming.remove(&frame.id); // Remove to unblock host
                                let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR",
                                    &format!("CHUNK after STREAM_END for stream_id={}", stream_id));
                                let mut writer = frame_writer.lock().unwrap();
                                let _ = writer.write(&err_frame);
                                drop(writer);
                                drop(incoming);
                                continue;
                            }
                            None => {
                                // FAIL HARD: Unknown stream
                                incoming.remove(&frame.id); // Remove to unblock host
                                let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR",
                                    &format!("CHUNK for unknown stream_id={}", stream_id));
                                let mut writer = frame_writer.lock().unwrap();
                                let _ = writer.write(&err_frame);
                                drop(writer);
                                drop(incoming);
                                continue;
                            }
                        }
                        drop(incoming);
                        continue; // Wait for more chunks or STREAM_END/END
                    }
                    drop(incoming);

                    // Not an incoming request chunk - must be a peer response chunk
                    let pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = pending.get(&frame.id) {
                        if pending_req.ended {
                            // FAIL HARD: Response activity after END
                            drop(pending);
                            pending_peer_requests.lock().unwrap().remove(&frame.id);
                            continue;
                        }
                        // Forward the frame directly to the handler
                        let _ = pending_req.sender.send(frame.clone());
                    }
                }
                FrameType::Heartbeat => {
                    // Respond to heartbeat immediately - never blocked by handlers
                    let response = Frame::heartbeat(frame.id);
                    let mut writer = frame_writer.lock().unwrap();
                    writer.write(&response)?;
                }
                FrameType::Hello => {
                    // Unexpected HELLO after handshake - protocol error
                    let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "Unexpected HELLO after handshake");
                    let mut writer = frame_writer.lock().unwrap();
                    writer.write(&err_frame)?;
                }
                // FrameType::Res REMOVED - old protocol no longer supported
                // Responses now use stream multiplexing: STREAM_START + CHUNK + STREAM_END + END
                FrameType::Err => {
                    // Error frame from host - could be response to peer request
                    let mut pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = pending.remove(&frame.id) {
                        // Send the ERR frame to the handler, then close channel
                        let _ = pending_req.sender.send(frame.clone());
                        // Dropping pending_req.sender closes the channel
                    }
                }
                FrameType::Log => {
                    // Log frames from host - shouldn't normally receive these, ignore
                    continue;
                }
                FrameType::StreamStart => {
                    // NEW PROTOCOL: A new stream is starting for a request
                    let req_id = frame.id.clone();
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "STREAM_START missing stream_id");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };
                    let media_urn = match frame.media_urn.as_ref() {
                        Some(urn) => urn.clone(),
                        None => {
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "STREAM_START missing media_urn");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };

                    eprintln!("[PluginRuntime] STREAM_START: req_id={:?} stream_id={} media_urn={}",
                        req_id, stream_id, media_urn);

                    // STRICT: Add stream with validation
                    let mut incoming = pending_incoming.lock().unwrap();
                    if let Some(pending_req) = incoming.get_mut(&req_id) {
                        // FAIL HARD: Request already ended
                        if pending_req.ended {
                            incoming.remove(&frame.id); // Remove to unblock host
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "STREAM_START after request END");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            drop(writer);
                            drop(incoming);
                            continue;
                        }

                        // FAIL HARD: Duplicate stream ID
                        if pending_req.streams.iter().any(|(id, _)| id == &stream_id) {
                            incoming.remove(&frame.id); // Remove to unblock host
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR",
                                &format!("Duplicate stream_id: {}", stream_id));
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            drop(writer);
                            drop(incoming);
                            continue;
                        }

                        // ✅ Track new stream
                        pending_req.streams.push((stream_id.clone(), PendingStream {
                            media_urn: media_urn.clone(),
                            chunks: vec![],
                            complete: false,
                        }));
                        eprintln!("[PluginRuntime] Stream added: {} (total streams: {})",
                            stream_id, pending_req.streams.len());
                        drop(incoming);
                        continue;
                    }
                    drop(incoming);

                    // Not an incoming request - must be a peer response stream
                    let peer_pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = peer_pending.get(&req_id) {
                        if !pending_req.ended {
                            // Forward the STREAM_START frame to the handler
                            let _ = pending_req.sender.send(frame.clone());
                        }
                    }
                    drop(peer_pending);
                    continue;
                }
                FrameType::StreamEnd => {
                    // NEW PROTOCOL: A stream has ended for a request
                    let stream_id = match frame.stream_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "STREAM_END missing stream_id");
                            let mut writer = frame_writer.lock().unwrap();
                            let _ = writer.write(&err_frame);
                            continue;
                        }
                    };

                    eprintln!("[PluginRuntime] STREAM_END: stream_id={}", stream_id);

                    // STRICT: Mark stream as complete with validation
                    let mut incoming = pending_incoming.lock().unwrap();
                    if let Some(pending_req) = incoming.get_mut(&frame.id) {
                        match pending_req.streams.iter_mut().find(|(id, _)| id == &stream_id).map(|(_, s)| s) {
                            Some(stream) => {
                                stream.complete = true;
                                eprintln!("[PluginRuntime] Incoming stream marked complete: {}", stream_id);
                                drop(incoming);
                                continue;
                            }
                            None => {
                                // FAIL HARD: StreamEnd for unknown stream
                                incoming.remove(&frame.id); // Remove to unblock host
                                let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR",
                                    &format!("STREAM_END for unknown stream_id={}", stream_id));
                                let mut writer = frame_writer.lock().unwrap();
                                let _ = writer.write(&err_frame);
                                drop(writer);
                                drop(incoming);
                                continue;
                            }
                        }
                    }
                    drop(incoming);

                    // Not an incoming request - must be a peer response stream end
                    let peer_pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = peer_pending.get(&frame.id) {
                        if !pending_req.ended {
                            // Forward the STREAM_END frame to the handler
                            let _ = pending_req.sender.send(frame.clone());
                        }
                    }
                    drop(peer_pending);
                    continue;
                }
                FrameType::End => {
                    // NEW PROTOCOL: END signals all streams for request are complete
                    let mut incoming = pending_incoming.lock().unwrap();
                    if let Some(pending_req) = incoming.remove(&frame.id) {
                        drop(incoming);

                        let handler = pending_req.handler.clone();
                        let streams = pending_req.streams;
                        let cap_urn = pending_req.cap_urn.clone();
                        let request_id = frame.id.clone();
                        let writer_clone = Arc::clone(&frame_writer);
                        let pending_clone = Arc::clone(&pending_peer_requests);
                        let manifest_clone = self.manifest.clone(); // Pass manifest for file-path conversion
                        let max_chunk = negotiated_limits.max_chunk;

                        eprintln!("[PluginRuntime] END: req_id={:?} streams={}", request_id, streams.len());

                        let handle = thread::spawn(move || {
                            // Generate stream ID for plugin's response output
                            let response_stream_id = uuid::Uuid::new_v4().to_string();

                            // FAIL HARD: Extract output media URN from cap_urn - must be valid
                            let cap = match crate::cap_urn::CapUrn::from_string(&cap_urn) {
                                Ok(c) => c,
                                Err(e) => {
                                    let err_frame = Frame::err(request_id.clone(), "INVALID_CAP_URN", &format!("Failed to parse cap_urn: {}", e));
                                    let mut writer = writer_clone.lock().unwrap();
                                    let _ = writer.write(&err_frame);
                                    return;
                                }
                            };

                            let media_urn = cap.out_spec().to_string();

                            let emitter = ThreadSafeEmitter {
                                writer: Arc::clone(&writer_clone),
                                request_id: request_id.clone(),
                                stream_id: response_stream_id,
                                media_urn,
                                stream_started: AtomicBool::new(false),
                                seq: Mutex::new(0),
                                max_chunk,
                            };

                            let peer_invoker = PeerInvokerImpl {
                                writer: Arc::clone(&writer_clone),
                                pending_requests: Arc::clone(&pending_clone),
                                max_chunk,
                            };

                            // FILE-PATH AUTO-CONVERSION: Check each stream and convert file paths to file contents
                            // Pattern matching using URN accepts() - NO string comparison!
                            let file_path_pattern = match MediaUrn::from_string("media:file-path") {
                                Ok(p) => p,
                                Err(e) => {
                                    let err_frame = Frame::err(request_id.clone(), "FILE_PATH_PATTERN_ERROR", &format!("Failed to create file-path pattern: {}", e));
                                    let mut writer = writer_clone.lock().unwrap();
                                    let _ = writer.write(&err_frame);
                                    return;
                                }
                            };

                            let file_path_scalar = match MediaUrn::from_string("media:file-path;form=scalar") {
                                Ok(p) => p,
                                Err(e) => {
                                    let err_frame = Frame::err(request_id.clone(), "FILE_PATH_PATTERN_ERROR", &format!("Failed to create file-path;form=scalar pattern: {}", e));
                                    let mut writer = writer_clone.lock().unwrap();
                                    let _ = writer.write(&err_frame);
                                    return;
                                }
                            };

                            let file_path_list = match MediaUrn::from_string("media:file-path;form=list") {
                                Ok(p) => p,
                                Err(e) => {
                                    let err_frame = Frame::err(request_id.clone(), "FILE_PATH_PATTERN_ERROR", &format!("Failed to create file-path;form=list pattern: {}", e));
                                    let mut writer = writer_clone.lock().unwrap();
                                    let _ = writer.write(&err_frame);
                                    return;
                                }
                            };

                            // Convert streams, applying file-path auto-conversion
                            let mut converted_streams: Vec<(String, String, Vec<Vec<u8>>)> = Vec::new(); // (stream_id, media_urn, chunks)

                            for (stream_id, mut stream) in streams {
                                let stream_urn = match MediaUrn::from_string(&stream.media_urn) {
                                    Ok(u) => u,
                                    Err(e) => {
                                        let err_frame = Frame::err(request_id.clone(), "INVALID_STREAM_URN", &format!("Invalid stream media URN '{}': {}", stream.media_urn, e));
                                        let mut writer = writer_clone.lock().unwrap();
                                        let _ = writer.write(&err_frame);
                                        return;
                                    }
                                };

                                // Check if this stream is a file-path using URN accepts
                                let is_file_path = match file_path_pattern.accepts(&stream_urn) {
                                    Ok(result) => result,
                                    Err(e) => {
                                        let err_frame = Frame::err(request_id.clone(), "URN_MATCH_ERROR", &format!("Failed to check file-path pattern: {}", e));
                                        let mut writer = writer_clone.lock().unwrap();
                                        let _ = writer.write(&err_frame);
                                        return;
                                    }
                                };

                                if is_file_path {
                                    // FAIL HARD: Determine if scalar or list
                                    let is_scalar = file_path_scalar.accepts(&stream_urn).unwrap_or(false);
                                    let is_list = file_path_list.accepts(&stream_urn).unwrap_or(false);

                                    if !is_scalar && !is_list {
                                        let err_frame = Frame::err(request_id.clone(), "FILE_PATH_FORM_ERROR", &format!("File-path '{}' must be form=scalar or form=list", stream.media_urn));
                                        let mut writer = writer_clone.lock().unwrap();
                                        let _ = writer.write(&err_frame);
                                        return;
                                    }

                                    if is_scalar && is_list {
                                        let err_frame = Frame::err(request_id.clone(), "FILE_PATH_FORM_ERROR", &format!("File-path '{}' cannot be both scalar and list", stream.media_urn));
                                        let mut writer = writer_clone.lock().unwrap();
                                        let _ = writer.write(&err_frame);
                                        return;
                                    }

                                    // Get stdin source from manifest - FAIL HARD if not found
                                    let stdin_source = if let Some(ref manifest) = manifest_clone {
                                        // Find the cap definition by URN string comparison
                                        let cap_def = manifest.caps.iter().find(|c| c.urn.to_string() == cap_urn);
                                        if let Some(cap) = cap_def {
                                            // Find arg with this file-path media URN
                                            let arg_def = cap.args.iter().find(|a| a.media_urn == stream.media_urn);
                                            if let Some(arg) = arg_def {
                                                // Find stdin source
                                                arg.sources.iter().find_map(|s| {
                                                    if let crate::cap::ArgSource::Stdin { stdin } = s {
                                                        Some(stdin.clone())
                                                    } else {
                                                        None
                                                    }
                                                })
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    };

                                    let stdin_urn = match stdin_source {
                                        Some(urn) => urn,
                                        None => {
                                            let err_frame = Frame::err(request_id.clone(), "NO_STDIN_SOURCE", &format!("File-path argument '{}' has no stdin source defined", stream.media_urn));
                                            let mut writer = writer_clone.lock().unwrap();
                                            let _ = writer.write(&err_frame);
                                            return;
                                        }
                                    };

                                    // Read file and convert
                                    if is_scalar {
                                        let data = stream.chunks.concat();
                                        let path_str = String::from_utf8_lossy(&data);
                                        eprintln!("[PluginRuntime] Converting file-path '{}' to bytes with media_urn '{}'", path_str, stdin_urn);

                                        let file_bytes = match std::fs::read(path_str.as_ref()) {
                                            Ok(bytes) => bytes,
                                            Err(e) => {
                                                let err_frame = Frame::err(request_id.clone(), "FILE_READ_ERROR", &format!("Failed to read file '{}': {}", path_str, e));
                                                let mut writer = writer_clone.lock().unwrap();
                                                let _ = writer.write(&err_frame);
                                                return;
                                            }
                                        };

                                        eprintln!("[PluginRuntime] Read {} bytes from '{}'", file_bytes.len(), path_str);
                                        converted_streams.push((stream_id, stdin_urn, vec![file_bytes]));
                                    } else {
                                        // form=list - not yet implemented
                                        let err_frame = Frame::err(request_id.clone(), "NOT_IMPLEMENTED", "File-path form=list conversion not yet implemented in CBOR mode");
                                        let mut writer = writer_clone.lock().unwrap();
                                        let _ = writer.write(&err_frame);
                                        return;
                                    }
                                } else {
                                    // Not a file-path - pass through chunks as-is (each chunk is already CBOR-encoded)
                                    converted_streams.push((stream_id, stream.media_urn, stream.chunks));
                                }
                            }

                            // Send all streams as CBOR Frame messages (after conversion)
                            // Each chunk is already CBOR-encoded and must be sent individually
                            let (tx, rx) = crossbeam_channel::unbounded();
                            for (stream_id, media_urn, chunks) in converted_streams {
                                // Send STREAM_START
                                let start_frame = Frame::stream_start(request_id.clone(), stream_id.clone(), media_urn);
                                if tx.send(start_frame).is_err() {
                                    break;
                                }
                                // Send each CHUNK individually (each is already a complete CBOR value)
                                for (seq, chunk_data) in chunks.iter().enumerate() {
                                    let chunk_frame = Frame::chunk(request_id.clone(), stream_id.clone(), seq as u64, chunk_data.clone());
                                    if tx.send(chunk_frame).is_err() {
                                        break;
                                    }
                                }
                                // Send STREAM_END
                                let end_frame = Frame::stream_end(request_id.clone(), stream_id);
                                if tx.send(end_frame).is_err() {
                                    break;
                                }
                            }
                            // Send END frame
                            let end_frame = Frame::end(request_id.clone(), None);
                            let _ = tx.send(end_frame);
                            drop(tx);

                            let result = handler(rx, &emitter, &peer_invoker);

                            match result {
                                Ok(()) => {
                                    let mut writer = writer_clone.lock().unwrap();

                                    // STREAM MULTIPLEXING PROTOCOL: Send STREAM_END then END
                                    // Only send STREAM_END if STREAM_START was sent (stream_started == true)
                                    if emitter.stream_started.load(Ordering::SeqCst) {
                                        let stream_end = Frame::stream_end(request_id.clone(), emitter.stream_id.clone());
                                        let _ = writer.write(&stream_end);
                                    }

                                    let end_frame = Frame::end(request_id.clone(), None);
                                    let _ = writer.write(&end_frame);
                                }
                                Err(e) => {
                                    let err_frame = Frame::err(request_id, "HANDLER_ERROR", &e.to_string());
                                    let mut writer = writer_clone.lock().unwrap();
                                    let _ = writer.write(&err_frame);
                                }
                            }
                        });

                        active_handlers.push(handle);
                        continue;
                    }
                    drop(incoming);

                    // Not incoming request - must be peer response END
                    let mut pending = pending_peer_requests.lock().unwrap();
                    if let Some(mut pending_req) = pending.remove(&frame.id) {
                        // Send the END frame before closing the channel
                        let _ = pending_req.sender.send(frame.clone());
                        pending_req.ended = true;
                        // Dropping pending_req.sender closes the channel
                    }
                    drop(pending);
                }

                // Relay-level frames must never reach a plugin runtime.
                // If they do, it's a bug in the relay layer — fail hard.
                FrameType::RelayNotify | FrameType::RelayState => {
                    return Err(CborError::Protocol(format!(
                        "Relay frame {:?} must not reach plugin runtime",
                        frame.frame_type
                    )).into());
                }
            }
        }

        // Wait for all active handlers to complete before exiting
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
                r#"cap:in="media:bytes";op=process;out="media:void""#,
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
                r#"cap:in="media:pdf;bytes";op=process;out="*""#,
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
    fn create_test_manifest(name: &str, version: &str, description: &str, caps: Vec<Cap>) -> CapManifest {
        CapManifest::new(
            name.to_string(),
            version.to_string(),
            description.to_string(),
            caps,
        )
    }

    /// Test manifest JSON with a single cap for basic tests.
    /// Note: cap URN uses "cap:op=test" which lacks in/out tags, so CapManifest deserialization
    /// may fail because Cap requires in/out specs. For tests that only need raw manifest bytes
    /// (CBOR mode handshake), this is fine. For tests that need parsed CapManifest, use
    /// VALID_MANIFEST instead.
    const TEST_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:op=test","title":"Test","command":"test"}]}"#;

    /// Valid manifest with proper in/out specs for tests that need parsed CapManifest
    const VALID_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test"}]}"#;

    // TEST248: Test register handler by exact cap URN and find it by the same URN
    #[test]
    fn test_register_and_find_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register::<serde_json::Value, _>("cap:in=*;op=test;out=*", |_request, emitter, _peer| {
            emitter.emit_cbor(&ciborium::Value::Bytes(b"result".to_vec()));
            Ok(())
        });

        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    // TEST249: Test register_raw handler works with bytes directly without deserialization
    #[test]
    fn test_raw_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register_raw("cap:op=raw", move |frames, emitter, _peer| {
            // Stream through and track (now using Frame)
            let mut total = Vec::new();
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        total.extend_from_slice(&payload);
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            *received_clone.lock().unwrap() = total;
            Ok(())
        });

        let handler = runtime.find_handler("cap:op=raw").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();

        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        // Send STREAM_START
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        // Send CHUNK
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"echo this".to_vec())).ok();
        // Send STREAM_END
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        // Send END
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler(rx, &emitter, &no_peer).unwrap();
        assert_eq!(&*received.lock().unwrap(), b"echo this", "raw handler must echo payload");
    }

    // TEST250: Test register typed handler deserializes JSON and executes correctly
    #[test]
    fn test_typed_handler_deserialization() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register::<serde_json::Value, _>("cap:op=test", move |req, emitter, _peer| {
            let value = req.get("key").and_then(|v| v.as_str()).unwrap_or("missing");
            let bytes = value.as_bytes();
            emitter.emit_cbor(&ciborium::Value::Bytes(bytes.to_vec()));
            *received_clone.lock().unwrap() = bytes.to_vec();
            Ok(())
        });

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();

        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"{\"key\":\"hello\"}".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler(rx, &emitter, &no_peer).unwrap();
        assert_eq!(&*received.lock().unwrap(), b"hello");
    }

    // TEST251: Test typed handler returns RuntimeError::Deserialize for invalid JSON input
    #[test]
    fn test_typed_handler_rejects_invalid_json() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register::<serde_json::Value, _>("cap:op=test", |_req, _emitter, _peer| {
            Ok(())
        });

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();

        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"not json {{{{".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        let result = handler(rx, &emitter, &no_peer);
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::Deserialize(_) => {}
            other => panic!("Expected Deserialize error, got {:?}", other),
        }
    }

    // TEST252: Test find_handler returns None for unregistered cap URNs
    #[test]
    fn test_find_handler_unknown_cap() {
        let runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        assert!(runtime.find_handler("cap:op=nonexistent").is_none());
    }

    // TEST253: Test handler function can be cloned via Arc and sent across threads (Send + Sync)
    #[test]
    fn test_handler_is_send_sync() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        runtime.register::<serde_json::Value, _>("cap:op=threaded", move |_req, emitter, _peer| {
            emitter.emit_cbor(&ciborium::Value::Bytes(b"done".to_vec()));
            *received_clone.lock().unwrap() = b"done".to_vec();
            Ok(())
        });

        let handler = runtime.find_handler("cap:op=threaded").unwrap();
        let handler_clone = Arc::clone(&handler);

        let handle = std::thread::spawn(move || {
            let no_peer = NoPeerInvoker;
            let emitter = CliStreamEmitter::new();
            let (tx, rx) = crossbeam_channel::unbounded();
            let request_id = MessageId::new_uuid();
            let stream_id = "test-stream".to_string();
            tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
            tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"{}".to_vec())).ok();
            tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
            tx.send(Frame::end(request_id, None)).ok();
            drop(tx);
            handler_clone(rx, &emitter, &no_peer).unwrap();
        });

        handle.join().unwrap();
        assert_eq!(&*received.lock().unwrap(), b"done");
    }

    // TEST254: Test NoPeerInvoker always returns PeerRequest error regardless of arguments
    #[test]
    fn test_no_peer_invoker() {
        let no_peer = NoPeerInvoker;
        let result = no_peer.invoke("cap:op=test", &[]);
        assert!(result.is_err());
        match result {
            Err(RuntimeError::PeerRequest(msg)) => {
                assert!(msg.contains("not supported"), "error must indicate peer not supported");
            }
            _ => panic!("Expected PeerRequest error"),
        }
    }

    // TEST255: Test NoPeerInvoker returns error even with valid arguments
    #[test]
    fn test_no_peer_invoker_with_arguments() {
        let no_peer = NoPeerInvoker;
        let args = vec![CapArgumentValue::from_str("media:test", "value")];
        let result = no_peer.invoke("cap:op=test", &args);
        assert!(result.is_err());
    }

    // TEST256: Test PluginRuntime::with_manifest_json stores manifest data and parses when valid
    #[test]
    fn test_with_manifest_json() {
        // TEST_MANIFEST has "cap:op=test" which lacks in/out, so CapManifest parsing fails
        let runtime_basic = PluginRuntime::with_manifest_json(TEST_MANIFEST);
        assert!(!runtime_basic.manifest_data.is_empty());
        // The cap URN "cap:op=test" is invalid for CapManifest (missing in/out)
        // so manifest parse is expected to fail - this is correct behavior
        assert!(runtime_basic.manifest.is_none(), "cap:op=test lacks in/out, parse must fail");

        // VALID_MANIFEST has proper in/out specs
        let runtime_valid = PluginRuntime::with_manifest_json(VALID_MANIFEST);
        assert!(!runtime_valid.manifest_data.is_empty());
        assert!(runtime_valid.manifest.is_some(), "VALID_MANIFEST must parse into CapManifest");
    }

    // TEST257: Test PluginRuntime::new with invalid JSON still creates runtime (manifest is None)
    #[test]
    fn test_new_with_invalid_json() {
        let runtime = PluginRuntime::new(b"not json");
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_none(), "invalid JSON should leave manifest as None");
    }

    // TEST258: Test PluginRuntime::with_manifest creates runtime with valid manifest data
    #[test]
    fn test_with_manifest_struct() {
        let manifest: crate::manifest::CapManifest = serde_json::from_str(VALID_MANIFEST).unwrap();
        let runtime = PluginRuntime::with_manifest(manifest);
        assert!(!runtime.manifest_data.is_empty());
        assert!(runtime.manifest.is_some());
    }

    // TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged
    #[test]
    fn test_extract_effective_payload_non_cbor() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:void";op=test;out="media:void""#).unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, Some("application/json"), cap, true).unwrap();
        assert_eq!(result, payload, "non-CBOR must return raw payload");
    }

    // TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
    #[test]
    fn test_extract_effective_payload_no_content_type() {
        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:void";op=test;out="media:void""#).unwrap();
        let payload = b"raw data";
        let result = extract_effective_payload(payload, None, cap, true).unwrap();
        assert_eq!(result, payload);
    }

    // TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
    #[test]
    fn test_extract_effective_payload_cbor_match() {
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
    fn test_extract_effective_payload_cbor_no_match() {
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
    fn test_extract_effective_payload_invalid_cbor() {
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
    fn test_extract_effective_payload_cbor_not_array() {
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

    // TEST266: Test CliStreamEmitter writes to stdout and stderr correctly (basic construction)
    #[test]
    fn test_cli_stream_emitter_construction() {
        let emitter = CliStreamEmitter::new();
        assert!(emitter.ndjson, "default CLI emitter must use NDJSON");

        let emitter2 = CliStreamEmitter::without_ndjson();
        assert!(!emitter2.ndjson);
    }

    // TEST268: Test RuntimeError variants display correct messages
    #[test]
    fn test_runtime_error_display() {
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

    // TEST270: Test registering multiple handlers for different caps and finding each independently
    #[test]
    fn test_multiple_handlers() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_raw("cap:op=alpha", |frames, emitter, _| {
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            emitter.emit_cbor(&ciborium::Value::Bytes(b"a".to_vec()));
            Ok(())
        });
        runtime.register_raw("cap:op=beta", |frames, emitter, _| {
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            emitter.emit_cbor(&ciborium::Value::Bytes(b"b".to_vec()));
            Ok(())
        });
        runtime.register_raw("cap:op=gamma", |frames, emitter, _| {
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            emitter.emit_cbor(&ciborium::Value::Bytes(b"g".to_vec()));
            Ok(())
        });

        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();

        let h_alpha = runtime.find_handler("cap:op=alpha").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        h_alpha(rx, &emitter, &no_peer).unwrap();

        let h_beta = runtime.find_handler("cap:op=beta").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        h_beta(rx, &emitter, &no_peer).unwrap();

        let h_gamma = runtime.find_handler("cap:op=gamma").unwrap();
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        h_gamma(rx, &emitter, &no_peer).unwrap();
    }

    // TEST271: Test handler replacing an existing registration for the same cap URN
    #[test]
    fn test_handler_replacement() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        let result1 = Arc::new(Mutex::new(Vec::new()));
        let result1_clone = Arc::clone(&result1);
        let result2 = Arc::new(Mutex::new(Vec::new()));
        let result2_clone = Arc::clone(&result2);

        runtime.register_raw("cap:op=test", move |frames, emitter, _| {
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            emitter.emit_cbor(&ciborium::Value::Bytes(b"first".to_vec()));
            *result1_clone.lock().unwrap() = b"first".to_vec();
            Ok(())
        });
        runtime.register_raw("cap:op=test", move |frames, emitter, _| {
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));
                    }
                }
            }
            emitter.emit_cbor(&ciborium::Value::Bytes(b"second".to_vec()));
            *result2_clone.lock().unwrap() = b"second".to_vec();
            Ok(())
        });

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, b"".to_vec())).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler(rx, &emitter, &no_peer).unwrap();
        assert_eq!(&*result2.lock().unwrap(), b"second", "later registration must replace earlier");
    }

    // TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one
    #[test]
    fn test_extract_effective_payload_multiple_args() {
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
    fn test_extract_effective_payload_binary_value() {
        let binary_data: Vec<u8> = (0u8..=255).collect();
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:pdf;bytes".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(binary_data.clone())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let registry = MockRegistry::with_test_caps();
        let cap = registry.get(r#"cap:in="media:pdf;bytes";op=process;out="*""#).unwrap();
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let mut runtime = PluginRuntime::with_manifest(manifest);

        // Track what handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_raw(
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            move |frames, emitter, _peer| {
                // TRUE STREAMING: Relay frames as they arrive, track total for verification
                let mut total_received = Vec::new();
                for frame in frames {
                    if frame.frame_type == FrameType::Chunk {
                        if let Some(payload) = frame.payload {
                            total_received.extend_from_slice(&payload);
                            emitter.emit_cbor(&ciborium::Value::Bytes(payload));  // Stream through immediately
                        }
                    }
                }

                // After streaming complete, parse to verify what we received
                let cbor_val: ciborium::Value = ciborium::from_reader(&total_received[..]).unwrap();
                if let ciborium::Value::Array(args) = cbor_val {
                    let in_spec = MediaUrn::from_string("media:pdf;bytes").unwrap();
                    for arg in args {
                        if let ciborium::Value::Map(map) = arg {
                            let mut found_bytes = None;
                            for (k, v) in map {
                                if let (ciborium::Value::Text(key), ciborium::Value::Bytes(b)) = (k, v) {
                                    if key == "value" {
                                        found_bytes = Some(b);
                                        break;
                                    }
                                }
                            }
                            if let Some(bytes) = found_bytes {
                                *received_clone.lock().unwrap() = bytes;
                                return Ok(());
                            }
                        }
                    }
                }
                Ok(())
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

        let handler = runtime.find_handler(&cap.urn_string()).unwrap();
        let emitter = CliStreamEmitter::new();
        let peer = NoPeerInvoker;

        // STREAMING: Send payload via channel as Frame stream
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, payload)).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler(rx, &emitter, &peer).unwrap();

        // Verify handler received file bytes (not file path string)
        // File-path auto-conversion happened transparently
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Batch",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:pdf;bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },  // First
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Position { position: 0 },                     // First
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },  // Second
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::CliFlag { cli_flag: "--file".to_string() },  // First (not provided)
                    ArgSource::Position { position: 0 },                     // Second (provided)
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },  // Third (not used)
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:result;textable\"",
            "Process PDF",
            "process",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
                    ArgSource::Position { position: 0 },
                ],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap]);
        let mut runtime = PluginRuntime::with_manifest(manifest);

        // Track what the handler receives
        let received_payload = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received_payload);

        runtime.register_raw(
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:result;textable\"",
            move |frames, emitter, _peer| {
                // TRUE STREAMING: Relay frames immediately, verify after
                let mut total = Vec::new();
                for frame in frames {
                    if frame.frame_type == FrameType::Chunk {
                        if let Some(payload) = frame.payload {
                            total.extend_from_slice(&payload);
                            emitter.emit_cbor(&ciborium::Value::Bytes(payload));  // Stream through
                        }
                    }
                }

                // Verify after streaming complete
                let cbor_val: ciborium::Value = ciborium::from_reader(&total[..]).unwrap();
                if let ciborium::Value::Array(args) = cbor_val {
                    for arg in args {
                        if let ciborium::Value::Map(map) = arg {
                            for (k, v) in map {
                                if let (ciborium::Value::Text(key), ciborium::Value::Bytes(b)) = (k, v) {
                                    if key == "value" {
                                        *received_clone.lock().unwrap() = b;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(())
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

        let handler = runtime.find_handler(&cap.urn_string()).unwrap();
        let emitter = CliStreamEmitter::new();
        let peer = NoPeerInvoker;

        // STREAMING: Send payload via channel as Frame stream
        let (tx, rx) = crossbeam_channel::unbounded();
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();
        tx.send(Frame::chunk(request_id.clone(), stream_id.clone(), 0, payload)).ok();
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler(rx, &emitter, &peer).unwrap();

        // Verify handler received file bytes
        let received = received_payload.lock().unwrap();
        assert_eq!(&*received, test_content, "Handler receives file bytes after auto-conversion");

        std::fs::remove_file(test_file).ok();
    }

    // TEST351: file-path array with empty CBOR array returns empty (CBOR mode)
    #[test]
    fn test351_file_path_array_empty_array() {
        let cap = create_test_cap(
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                false,  // Not required
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=test;out=\"media:void\"",
            "Test",
            "test",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:file-path;textable;form=scalar",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
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
        let in_spec = MediaUrn::from_string("media:pdf;bytes").unwrap();
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() },
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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:pdf;bytes",
                true,
                vec![ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() }],
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

                    assert_eq!(media_urn, Some("media:pdf;bytes".to_string()), "Media URN matches cap in_spec");
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

        let handler = move |frames: Receiver<Frame>, emitter: &dyn StreamEmitter, _peer: &dyn PeerInvoker| {
            // TRUE STREAMING: Relay frames and verify
            let mut total = Vec::new();
            for frame in frames {
                if frame.frame_type == FrameType::Chunk {
                    if let Some(payload) = frame.payload {
                        total.extend_from_slice(&payload);
                        emitter.emit_cbor(&ciborium::Value::Bytes(payload));  // Stream through
                    }
                }
            }

            // Verify what we received
            let cbor_val: ciborium::Value = ciborium::from_reader(&total[..]).unwrap();
            if let ciborium::Value::Array(arr) = cbor_val {
                if let ciborium::Value::Map(map) = &arr[0] {
                    for (k, v) in map {
                        if let (ciborium::Value::Text(key), ciborium::Value::Bytes(data)) = (k, v) {
                            if key == "value" {
                                *received_clone.lock().unwrap() = data.clone();
                            }
                        }
                    }
                }
            }
            Ok(())
        };

        let cap = create_test_cap(
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                "media:pdf;bytes",
                true,
                vec![ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() }],
            )],
        );

        let manifest = create_test_manifest("TestPlugin", "1.0.0", "Test", vec![cap.clone()]);
        let mut runtime = PluginRuntime::with_manifest(manifest);
        runtime.register_raw(&cap.urn_string(), handler);

        // Verify handler can process chunked data (simulated by passing complete payload)
        let args = vec![CapArgumentValue::new("media:pdf;bytes".to_string(), pdf_content.clone())];
        let mut payload_bytes = Vec::new();
        let cbor_args: Vec<ciborium::Value> = args.iter().map(|arg| {
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text(arg.media_urn.clone())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(arg.value.clone())),
            ])
        }).collect();
        ciborium::into_writer(&ciborium::Value::Array(cbor_args), &mut payload_bytes).unwrap();

        // Simulate streaming: chunk payload and send via channel
        let handler_func = runtime.find_handler(&cap.urn_string()).unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = crate::CliStreamEmitter::new();

        let (tx, rx) = crossbeam_channel::unbounded();
        const MAX_CHUNK: usize = 262144;
        let request_id = MessageId::new_uuid();
        let stream_id = "test-stream".to_string();

        // Send STREAM_START
        tx.send(Frame::stream_start(request_id.clone(), stream_id.clone(), "media:bytes".to_string())).ok();

        // Send CHUNK frames
        let mut offset = 0;
        let mut seq = 0u64;
        while offset < payload_bytes.len() {
            let chunk_size = (payload_bytes.len() - offset).min(MAX_CHUNK);
            tx.send(Frame::chunk(
                request_id.clone(),
                stream_id.clone(),
                seq,
                payload_bytes[offset..offset + chunk_size].to_vec()
            )).ok();
            offset += chunk_size;
            seq += 1;
        }

        // Send STREAM_END and END
        tx.send(Frame::stream_end(request_id.clone(), stream_id)).ok();
        tx.send(Frame::end(request_id, None)).ok();
        drop(tx);
        handler_func(rx, &emitter, &no_peer).unwrap();

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
            "cap:in=\"media:pdf;bytes\";op=process;out=\"media:void\"",
            "Process",
            "process",
            vec![CapArg::new(
                MEDIA_FILE_PATH,
                true,
                vec![ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() }],
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
                assert_eq!(media_urn, Some(&"media:pdf;bytes".to_string()), "URN converted to expected input");
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
            "cap:in=\"media:bytes\";op=batch;out=\"media:void\"",
            "Test",
            "batch",
            vec![CapArg::new(
                "media:file-path;textable;form=list",
                true,
                vec![
                    ArgSource::Stdin { stdin: "media:bytes".to_string() },
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
        assert_eq!(media_urn, "media:bytes", "media_urn should be converted to stdin source");

        std::fs::remove_dir_all(temp_dir).ok();
    }

    // TEST395: Small payload (< max_chunk) produces correct CBOR arguments
    #[test]
    fn test_build_payload_small() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:bytes\";op=process;out=\"media:void\"",
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
    fn test_build_payload_large() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:bytes\";op=process;out=\"media:void\"",
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
    fn test_build_payload_empty() {
        use std::io::Cursor;

        let cap = create_test_cap(
            "cap:in=\"media:bytes\";op=process;out=\"media:void\"",
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
    fn test_build_payload_io_error() {
        struct ErrorReader;
        impl std::io::Read for ErrorReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "simulated read error"))
            }
        }

        let cap = create_test_cap(
            "cap:in=\"media:bytes\";op=process;out=\"media:void\"",
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
}
