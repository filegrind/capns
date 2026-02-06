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
//!         emitter.emit_status("processing", "Starting work...");
//!         // Do work, emit chunks in real-time
//!         emitter.emit_bytes(b"partial result");
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
use crate::media_urn::MediaUrn;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::{Arc, Mutex};
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
pub trait StreamEmitter: Send + Sync {
    /// Emit raw bytes as a chunk immediately.
    fn emit_bytes(&self, payload: &[u8]);

    /// Emit a JSON value as a chunk.
    /// The value is serialized to JSON bytes and sent as the chunk payload.
    fn emit(&self, payload: serde_json::Value) {
        match serde_json::to_vec(&payload) {
            Ok(bytes) => self.emit_bytes(&bytes),
            Err(e) => {
                eprintln!("[PluginRuntime] Failed to serialize payload: {}", e);
            }
        }
    }

    /// Emit a status/progress message.
    fn emit_status(&self, operation: &str, details: &str) {
        self.emit(serde_json::json!({
            "type": "status",
            "operation": operation,
            "details": details
        }));
    }

    /// Emit a log message at the given level.
    fn log(&self, level: &str, message: &str);
}

/// Allows handlers to invoke caps on the peer (host).
///
/// This trait enables bidirectional communication where a plugin handler can
/// invoke caps on the host while processing a request. This is essential for
/// sandboxed plugins that need to delegate certain operations (like model
/// downloading) to the host.
///
/// The `invoke` method sends a REQ frame to the host and returns a receiver
/// that yields response chunks as they arrive. The caller can iterate over
/// the receiver to collect all response data.
pub trait PeerInvoker: Send + Sync {
    /// Invoke a cap on the host with unified arguments.
    ///
    /// Sends a REQ frame to the host with the specified cap URN and arguments.
    /// Arguments are serialized as CBOR with native binary values.
    /// Returns a receiver that yields response chunks (Vec<u8>) or errors.
    /// The receiver will be closed when the response is complete (END frame received).
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke on the host
    /// * `arguments` - Arguments identified by media_urn
    ///
    /// # Returns
    /// A receiver that yields `Result<Vec<u8>, RuntimeError>` for each chunk.
    /// Iterate over it to collect all response data.
    ///
    /// # Example
    /// ```ignore
    /// let args = vec![CapArgumentValue::from_str("media:model-spec;textable;form=scalar", model_spec)];
    /// let rx = peer.invoke("cap:op=model_path;...", &args)?;
    /// let mut response_data = Vec::new();
    /// for chunk_result in rx {
    ///     response_data.extend(chunk_result?);
    /// }
    /// ```
    fn invoke(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Result<Vec<u8>, RuntimeError>>, RuntimeError>;
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported.
pub struct NoPeerInvoker;

impl PeerInvoker for NoPeerInvoker {
    fn invoke(
        &self,
        _cap_urn: &str,
        _arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Result<Vec<u8>, RuntimeError>>, RuntimeError> {
        Err(RuntimeError::PeerRequest(
            "Peer invocation not supported in this context".to_string(),
        ))
    }
}

/// Thread-safe implementation of StreamEmitter that writes CBOR frames.
/// Uses Arc<Mutex<>> for safe concurrent access from multiple handler threads.
struct ThreadSafeEmitter<W: Write + Send> {
    writer: Arc<Mutex<FrameWriter<W>>>,
    request_id: MessageId,
    seq: Mutex<u64>,
}

impl<W: Write + Send> StreamEmitter for ThreadSafeEmitter<W> {
    fn emit_bytes(&self, payload: &[u8]) {
        let seq = {
            let mut seq_guard = self.seq.lock().unwrap();
            let current = *seq_guard;
            *seq_guard += 1;
            current
        };

        let frame = Frame::chunk(self.request_id.clone(), seq, payload.to_vec());

        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write chunk: {}", e);
        }
    }

    fn log(&self, level: &str, message: &str) {
        let frame = Frame::log(self.request_id.clone(), level, message);

        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write log: {}", e);
        }
    }

    /// Override emit_status to send LOG frames, not CHUNK frames.
    /// Status messages are progress/status updates, not response data.
    fn emit_status(&self, operation: &str, details: &str) {
        // Use LOG frame for status updates - they should not be part of response data
        let message = format!("{}: {}", operation, details);
        let frame = Frame::log(self.request_id.clone(), "status", &message);

        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write status: {}", e);
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
    fn emit_bytes(&self, payload: &[u8]) {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(payload);
        if self.ndjson {
            let _ = handle.write_all(b"\n");
        }
        let _ = handle.flush();
    }

    /// In CLI mode, status messages go to stderr so only the final response is on stdout.
    /// This allows external callers to parse stdout as a single JSON response.
    fn emit_status(&self, operation: &str, details: &str) {
        let status = serde_json::json!({
            "type": "status",
            "operation": operation,
            "details": details
        });
        if let Ok(json) = serde_json::to_string(&status) {
            eprintln!("{}", json);
        }
    }

    fn log(&self, level: &str, message: &str) {
        // In CLI mode, logs go to stderr
        eprintln!("[{}] {}", level.to_uppercase(), message);
    }
}

/// Handler function type - must be Send + Sync for concurrent execution.
/// Receives request payload bytes, emitter, and peer invoker; returns response payload bytes.
///
/// The `PeerInvoker` allows the handler to invoke caps on the host (peer) during
/// request processing. This enables bidirectional communication for operations
/// like model downloading that sandboxed plugins cannot perform directly.
pub type HandlerFn = Arc<
    dyn Fn(&[u8], &dyn StreamEmitter, &dyn PeerInvoker) -> Result<Vec<u8>, RuntimeError> + Send + Sync,
>;

/// Internal struct to track pending peer requests (plugin invoking host caps).
struct PendingPeerRequest {
    sender: Sender<Result<Vec<u8>, RuntimeError>>,
}

/// Implementation of PeerInvoker that sends REQ frames to the host.
struct PeerInvokerImpl<W: Write + Send> {
    writer: Arc<Mutex<FrameWriter<W>>>,
    pending_requests: Arc<Mutex<HashMap<MessageId, PendingPeerRequest>>>,
}

/// Extract the effective payload from a REQ frame.
///
/// If the content_type is "application/cbor", the payload is expected to be
/// CBOR unified arguments: `[{media_urn: string, value: bytes}, ...]`
/// The function extracts the value whose media_urn matches the cap's input type.
///
/// For other content types (or if content_type is None), returns the raw payload.
fn extract_effective_payload(
    payload: &[u8],
    content_type: Option<&str>,
    cap_urn: &str,
) -> Result<Vec<u8>, RuntimeError> {
    // Check if this is CBOR unified arguments
    if content_type != Some("application/cbor") {
        // Not CBOR unified arguments - return raw payload
        return Ok(payload.to_vec());
    }

    // Parse the cap URN to get the expected input media URN
    let cap = match CapUrn::from_string(cap_urn) {
        Ok(urn) => urn,
        Err(e) => {
            return Err(RuntimeError::CapUrn(format!(
                "Failed to parse cap URN '{}': {}",
                cap_urn, e
            )));
        }
    };
    let expected_input = cap.in_spec().to_string();
    let expected_media_urn = MediaUrn::from_string(&expected_input).ok();

    // Parse the CBOR payload as an array of argument maps
    let cbor_value: ciborium::Value = ciborium::from_reader(payload).map_err(|e| {
        RuntimeError::Deserialize(format!("Failed to parse CBOR unified arguments: {}", e))
    })?;

    let arguments = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => {
            return Err(RuntimeError::Deserialize(
                "CBOR unified arguments must be an array".to_string(),
            ));
        }
    };

    // Find the argument with matching media_urn
    for arg in arguments {
        let arg_map = match arg {
            ciborium::Value::Map(m) => m,
            _ => continue,
        };

        let mut media_urn: Option<String> = None;
        let mut value: Option<Vec<u8>> = None;

        for (k, v) in arg_map {
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

        // Check if this argument matches the expected input using semantic URN matching
        if let (Some(urn_str), Some(val)) = (media_urn, value) {
            if let Some(ref expected) = expected_media_urn {
                if let Ok(arg_urn) = MediaUrn::from_string(&urn_str) {
                    // Use semantic matching in both directions
                    let fwd = arg_urn.matches(expected).unwrap_or(false);
                    let rev = expected.matches(&arg_urn).unwrap_or(false);
                    if fwd || rev {
                        return Ok(val);
                    }
                }
            }
        }
    }

    // No matching argument found - this is an error, no fallbacks
    Err(RuntimeError::Deserialize(format!(
        "No argument found matching expected input media type '{}' in CBOR unified arguments",
        expected_input
    )))
}

impl<W: Write + Send> PeerInvoker for PeerInvokerImpl<W> {
    fn invoke(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> Result<Receiver<Result<Vec<u8>, RuntimeError>>, RuntimeError> {
        // Generate a new message ID for this request
        let request_id = MessageId::new_uuid();

        // Create a bounded channel for responses (buffer up to 64 chunks)
        let (sender, receiver) = bounded(64);

        // Register the pending request before sending
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(request_id.clone(), PendingPeerRequest { sender });
        }

        // Serialize arguments as CBOR - binary values stay binary (no base64 needed)
        let payload = ciborium::Value::Array(
            arguments
                .iter()
                .map(|a| {
                    ciborium::Value::Map(vec![
                        (
                            ciborium::Value::Text("media_urn".to_string()),
                            ciborium::Value::Text(a.media_urn.clone()),
                        ),
                        (
                            ciborium::Value::Text("value".to_string()),
                            ciborium::Value::Bytes(a.value.clone()),
                        ),
                    ])
                })
                .collect(),
        );
        let mut payload_bytes = Vec::new();
        ciborium::into_writer(&payload, &mut payload_bytes).map_err(|e| {
            self.pending_requests.lock().unwrap().remove(&request_id);
            RuntimeError::Serialize(format!("Failed to serialize arguments: {}", e))
        })?;

        // Create and send the REQ frame with CBOR payload
        let frame = Frame::req(
            request_id.clone(),
            cap_urn,
            payload_bytes,
            "application/cbor",
        );

        {
            let mut writer = self.writer.lock().unwrap();
            writer.write(&frame).map_err(|e| {
                // Remove the pending request on send failure
                self.pending_requests.lock().unwrap().remove(&request_id);
                RuntimeError::PeerRequest(format!("Failed to send REQ frame: {}", e))
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
    pub fn register<Req, F>(&mut self, cap_urn: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned + 'static,
        F: Fn(Req, &dyn StreamEmitter, &dyn PeerInvoker) -> Result<Vec<u8>, RuntimeError> + Send + Sync + 'static,
    {
        let handler = move |payload: &[u8], emitter: &dyn StreamEmitter, peer: &dyn PeerInvoker| -> Result<Vec<u8>, RuntimeError> {
            // Deserialize request from payload bytes (JSON format for now)
            let request: Req = serde_json::from_slice(payload)
                .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse request: {}", e)))?;

            handler(request, emitter, peer)
        };

        self.handlers.insert(cap_urn.to_string(), Arc::new(handler));
    }

    /// Register a raw handler that works with bytes directly.
    ///
    /// Use this when you need full control over serialization.
    /// The handler receives the emitter and peer invoker in addition to the raw payload.
    pub fn register_raw<F>(&mut self, cap_urn: &str, handler: F)
    where
        F: Fn(&[u8], &dyn StreamEmitter, &dyn PeerInvoker) -> Result<Vec<u8>, RuntimeError> + Send + Sync + 'static,
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
                if pattern_urn.matches(&request_urn) {
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

        // Build arguments from CLI
        let cli_args = &args[2..];
        let payload = self.build_payload_from_cli(&cap, cli_args)?;

        // Create CLI-mode emitter and no-op peer invoker
        let emitter = CliStreamEmitter::new();
        let peer = NoPeerInvoker;

        // Invoke handler
        let result = handler(&payload, &emitter, &peer);

        match result {
            Ok(response) => {
                // Output final response if not empty
                if !response.is_empty() {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    let _ = handle.write_all(&response);
                    let _ = handle.write_all(b"\n");
                    let _ = handle.flush();
                }
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

    /// Build payload from CLI arguments based on cap's arg definitions.
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
            let value = self.extract_arg_value(&arg_def, cli_args, stdin_data.as_deref())?;

            if let Some(val) = value {
                arguments.push(CapArgumentValue {
                    media_urn: arg_def.media_urn.clone(),
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
        }

        // If we have structured arguments, serialize as JSON
        if !arguments.is_empty() {
            // Build a JSON object from the arguments
            let mut json_obj = serde_json::Map::new();
            for arg in &arguments {
                // Try to parse value as JSON first, fall back to string
                let value = if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&arg.value) {
                    parsed
                } else if let Ok(s) = String::from_utf8(arg.value.clone()) {
                    serde_json::Value::String(s)
                } else {
                    // Binary data - keep as raw bytes (the handler will deal with it)
                    // For JSON serialization, we need a string representation
                    // If it's truly binary and can't be represented as UTF-8,
                    // the handler should use a different approach (like stdin)
                    return Err(RuntimeError::Cli(
                        "Binary data cannot be passed via CLI flags. Use stdin instead.".to_string()
                    ));
                };
                // Use the last part of media_urn as key (e.g., "model-spec" from "media:model-spec;...")
                let key = arg.media_urn
                    .strip_prefix("media:")
                    .unwrap_or(&arg.media_urn)
                    .split(';')
                    .next()
                    .unwrap_or(&arg.media_urn)
                    .replace('-', "_");
                json_obj.insert(key, value);
            }
            serde_json::to_vec(&json_obj)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))
        } else {
            // No arguments, no stdin - return empty object
            Ok(b"{}".to_vec())
        }
    }

    /// Extract a single argument value from CLI args or stdin.
    fn extract_arg_value(
        &self,
        arg_def: &CapArg,
        cli_args: &[String],
        stdin_data: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, RuntimeError> {
        // Try each source in order
        for source in &arg_def.sources {
            match source {
                ArgSource::CliFlag { cli_flag } => {
                    if let Some(value) = self.get_cli_flag_value(cli_args, cli_flag) {
                        return Ok(Some(value.into_bytes()));
                    }
                }
                ArgSource::Position { position } => {
                    // Positional args: filter out flags and their values
                    let positional: Vec<_> = self.get_positional_args(cli_args);
                    if let Some(value) = positional.get(*position) {
                        return Ok(Some(value.clone().into_bytes()));
                    }
                }
                ArgSource::Stdin { .. } => {
                    if let Some(data) = stdin_data {
                        return Ok(Some(data.to_vec()));
                    }
                }
            }
        }

        // Try default value
        if let Some(default) = &arg_def.default_value {
            let bytes = serde_json::to_vec(default)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))?;
            return Ok(Some(bytes));
        }

        Ok(None)
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

                    // Clone what we need for the handler thread
                    let writer_clone = Arc::clone(&frame_writer);
                    let pending_clone = Arc::clone(&pending_peer_requests);
                    let request_id = frame.id.clone();
                    let raw_payload = frame.payload.clone().unwrap_or_default();
                    let content_type = frame.content_type.clone();
                    let cap_urn_clone = cap_urn.clone();
                    let max_chunk = negotiated_limits.max_chunk;

                    // Spawn handler in separate thread - main loop continues immediately
                    let handle = thread::spawn(move || {
                        let emitter = ThreadSafeEmitter {
                            writer: Arc::clone(&writer_clone),
                            request_id: request_id.clone(),
                            seq: Mutex::new(0),
                        };

                        // Create peer invoker for this handler
                        let peer_invoker = PeerInvokerImpl {
                            writer: Arc::clone(&writer_clone),
                            pending_requests: Arc::clone(&pending_clone),
                        };

                        // Extract effective payload from unified arguments if content_type is CBOR
                        let payload = match extract_effective_payload(
                            &raw_payload,
                            content_type.as_deref(),
                            &cap_urn_clone,
                        ) {
                            Ok(p) => p,
                            Err(e) => {
                                // Failed to extract payload - send error response
                                let err_frame = Frame::err(request_id, "PAYLOAD_ERROR", &e.to_string());
                                let mut writer = writer_clone.lock().unwrap();
                                if let Err(write_err) = writer.write(&err_frame) {
                                    eprintln!("[PluginRuntime] Failed to write error response: {}", write_err);
                                }
                                return;
                            }
                        };

                        let result = handler(&payload, &emitter, &peer_invoker);

                        // Send response with automatic chunking for large payloads
                        match result {
                            Ok(final_payload) => {
                                let mut writer = writer_clone.lock().unwrap();

                                // Automatic chunking: split large payloads into CHUNK frames
                                if final_payload.len() <= max_chunk {
                                    // Small payload: send single END frame
                                    let end_frame = Frame::end(request_id.clone(), Some(final_payload));
                                    if let Err(e) = writer.write(&end_frame) {
                                        eprintln!("[PluginRuntime] Failed to write END frame: {}", e);
                                    }
                                } else {
                                    // Large payload: send CHUNK frames + final END
                                    let mut offset = 0;
                                    let mut seq = 0u64;

                                    while offset < final_payload.len() {
                                        let remaining = final_payload.len() - offset;
                                        let chunk_size = remaining.min(max_chunk);
                                        let chunk_data = final_payload[offset..offset + chunk_size].to_vec();
                                        offset += chunk_size;

                                        if offset < final_payload.len() {
                                            // Not the last chunk - send CHUNK frame
                                            let chunk_frame = Frame::chunk(request_id.clone(), seq, chunk_data);
                                            if let Err(e) = writer.write(&chunk_frame) {
                                                eprintln!("[PluginRuntime] Failed to write CHUNK frame: {}", e);
                                                return;
                                            }
                                            seq += 1;
                                        } else {
                                            // Last chunk - send END frame with remaining data
                                            let end_frame = Frame::end(request_id.clone(), Some(chunk_data));
                                            if let Err(e) = writer.write(&end_frame) {
                                                eprintln!("[PluginRuntime] Failed to write END frame: {}", e);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let err_frame = Frame::err(request_id, "HANDLER_ERROR", &e.to_string());
                                let mut writer = writer_clone.lock().unwrap();
                                if let Err(e) = writer.write(&err_frame) {
                                    eprintln!("[PluginRuntime] Failed to write error response: {}", e);
                                }
                            }
                        }
                    });

                    active_handlers.push(handle);
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
                FrameType::Res | FrameType::Chunk | FrameType::End => {
                    // Response frames from host - route to pending peer request by frame.id
                    let pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = pending.get(&frame.id) {
                        // Send the payload to the waiting receiver
                        let payload = frame.payload.clone().unwrap_or_default();
                        let _ = pending_req.sender.send(Ok(payload));
                    }
                    drop(pending);

                    // Remove completed requests (RES or END frame marks completion)
                    if frame.frame_type == FrameType::Res || frame.frame_type == FrameType::End {
                        pending_peer_requests.lock().unwrap().remove(&frame.id);
                    }
                }
                FrameType::Err => {
                    // Error frame from host - could be response to peer request
                    let pending = pending_peer_requests.lock().unwrap();
                    if let Some(pending_req) = pending.get(&frame.id) {
                        let code = frame.error_code().unwrap_or("UNKNOWN");
                        let message = frame.error_message().unwrap_or("Unknown error");
                        let _ = pending_req.sender.send(Err(RuntimeError::PeerResponse(
                            format!("[{}] {}", code, message),
                        )));
                    }
                    drop(pending);
                    pending_peer_requests.lock().unwrap().remove(&frame.id);
                }
                FrameType::Log => {
                    // Log frames from host - shouldn't normally receive these, ignore
                    continue;
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

        runtime.register::<serde_json::Value, _>("cap:in=*;op=test;out=*", |_request, _emitter, _peer| {
            Ok(b"result".to_vec())
        });

        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    // TEST249: Test register_raw handler works with bytes directly without deserialization
    #[test]
    fn test_raw_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_raw("cap:op=raw", |payload, _emitter, _peer| {
            Ok(payload.to_vec())
        });

        let handler = runtime.find_handler("cap:op=raw").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();
        let result = handler(b"echo this", &emitter, &no_peer).unwrap();
        assert_eq!(result, b"echo this", "raw handler must echo payload");
    }

    // TEST250: Test register typed handler deserializes JSON and executes correctly
    #[test]
    fn test_typed_handler_deserialization() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register::<serde_json::Value, _>("cap:op=test", |req, _emitter, _peer| {
            let value = req.get("key").and_then(|v| v.as_str()).unwrap_or("missing");
            Ok(value.as_bytes().to_vec())
        });

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();
        let result = handler(b"{\"key\":\"hello\"}", &emitter, &no_peer).unwrap();
        assert_eq!(result, b"hello");
    }

    // TEST251: Test typed handler returns RuntimeError::Deserialize for invalid JSON input
    #[test]
    fn test_typed_handler_rejects_invalid_json() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register::<serde_json::Value, _>("cap:op=test", |_req, _emitter, _peer| {
            Ok(vec![])
        });

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();
        let result = handler(b"not json {{{{", &emitter, &no_peer);
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

        runtime.register::<serde_json::Value, _>("cap:op=threaded", |_req, _emitter, _peer| {
            Ok(b"done".to_vec())
        });

        let handler = runtime.find_handler("cap:op=threaded").unwrap();
        let handler_clone = Arc::clone(&handler);

        let handle = std::thread::spawn(move || {
            let no_peer = NoPeerInvoker;
            let emitter = CliStreamEmitter::new();
            let result = handler_clone(b"{}", &emitter, &no_peer).unwrap();
            assert_eq!(result, b"done");
        });

        handle.join().unwrap();
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
        let payload = b"raw data";
        let result = extract_effective_payload(payload, Some("application/json"), "cap:op=test").unwrap();
        assert_eq!(result, payload, "non-CBOR must return raw payload");
    }

    // TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
    #[test]
    fn test_extract_effective_payload_no_content_type() {
        let payload = b"raw data";
        let result = extract_effective_payload(payload, None, "cap:op=test").unwrap();
        assert_eq!(result, payload);
    }

    // TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
    #[test]
    fn test_extract_effective_payload_cbor_match() {
        // Build CBOR unified arguments: [{media_urn: "media:string;textable;form=scalar", value: bytes("hello")}]
        let args = ciborium::Value::Array(vec![
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("media_urn".to_string()), ciborium::Value::Text("media:string;textable;form=scalar".to_string())),
                (ciborium::Value::Text("value".to_string()), ciborium::Value::Bytes(b"hello".to_vec())),
            ]),
        ]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        // The cap URN has in=media:string;textable;form=scalar
        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "cap:in=media:string;textable;form=scalar;op=test;out=*",
        ).unwrap();
        assert_eq!(result, b"hello");
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

        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "cap:in=media:string;textable;form=scalar;op=test;out=*",
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
        let result = extract_effective_payload(
            b"not cbor",
            Some("application/cbor"),
            "cap:in=*;op=test;out=*",
        );
        assert!(result.is_err());
    }

    // TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error
    #[test]
    fn test_extract_effective_payload_cbor_not_array() {
        let value = ciborium::Value::Map(vec![]);
        let mut payload = Vec::new();
        ciborium::into_writer(&value, &mut payload).unwrap();

        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "cap:in=*;op=test;out=*",
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::Deserialize(msg) => {
                assert!(msg.contains("must be an array"), "{}", msg);
            }
            other => panic!("expected Deserialize, got {:?}", other),
        }
    }

    // TEST265: Test extract_effective_payload with invalid cap URN returns CapUrn error
    #[test]
    fn test_extract_effective_payload_invalid_cap_urn() {
        let args = ciborium::Value::Array(vec![]);
        let mut payload = Vec::new();
        ciborium::into_writer(&args, &mut payload).unwrap();

        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "not-a-cap-urn",
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::CapUrn(_) => {}
            other => panic!("expected CapUrn error, got {:?}", other),
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

    // TEST267: Test CliStreamEmitter::default creates NDJSON emitter
    #[test]
    fn test_cli_stream_emitter_default() {
        let emitter = CliStreamEmitter::default();
        assert!(emitter.ndjson);
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

    // TEST269: Test PluginRuntime limits returns default protocol limits
    #[test]
    fn test_runtime_limits_default() {
        let runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        let limits = runtime.limits();
        assert_eq!(limits.max_frame, crate::cbor_frame::DEFAULT_MAX_FRAME);
        assert_eq!(limits.max_chunk, crate::cbor_frame::DEFAULT_MAX_CHUNK);
    }

    // TEST270: Test registering multiple handlers for different caps and finding each independently
    #[test]
    fn test_multiple_handlers() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_raw("cap:op=alpha", |_, _, _| Ok(b"a".to_vec()));
        runtime.register_raw("cap:op=beta", |_, _, _| Ok(b"b".to_vec()));
        runtime.register_raw("cap:op=gamma", |_, _, _| Ok(b"g".to_vec()));

        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();

        let h_alpha = runtime.find_handler("cap:op=alpha").unwrap();
        assert_eq!(h_alpha(b"", &emitter, &no_peer).unwrap(), b"a");

        let h_beta = runtime.find_handler("cap:op=beta").unwrap();
        assert_eq!(h_beta(b"", &emitter, &no_peer).unwrap(), b"b");

        let h_gamma = runtime.find_handler("cap:op=gamma").unwrap();
        assert_eq!(h_gamma(b"", &emitter, &no_peer).unwrap(), b"g");
    }

    // TEST271: Test handler replacing an existing registration for the same cap URN
    #[test]
    fn test_handler_replacement() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_raw("cap:op=test", |_, _, _| Ok(b"first".to_vec()));
        runtime.register_raw("cap:op=test", |_, _, _| Ok(b"second".to_vec()));

        let handler = runtime.find_handler("cap:op=test").unwrap();
        let no_peer = NoPeerInvoker;
        let emitter = CliStreamEmitter::new();
        let result = handler(b"", &emitter, &no_peer).unwrap();
        assert_eq!(result, b"second", "later registration must replace earlier");
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

        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "cap:in=media:model-spec;textable;form=scalar;op=infer;out=*",
        ).unwrap();
        assert_eq!(result, b"correct");
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

        let result = extract_effective_payload(
            &payload,
            Some("application/cbor"),
            "cap:in=media:pdf;bytes;op=process;out=*",
        ).unwrap();
        assert_eq!(result, binary_data, "binary values must roundtrip through CBOR extraction");
    }
}
