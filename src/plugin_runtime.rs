//! Plugin Runtime - Unified I/O handling for plugin binaries
//!
//! The PluginRuntime provides a unified interface for plugin binaries to handle
//! cap invocations. Plugins register handlers for caps they provide, and the
//! runtime handles all I/O mechanics:
//!
//! - CBOR frame encoding/decoding
//! - Binary packet framing on stdin/stdout
//! - Handler routing by cap URN
//! - Real-time streaming response support
//! - HELLO handshake for limit negotiation
//! - **Multiplexed concurrent request handling**
//!
//! # Example
//!
//! ```ignore
//! use capns::{PluginRuntime, StreamEmitter};
//!
//! fn main() {
//!     let mut runtime = PluginRuntime::new();
//!
//!     runtime.register::<MyRequest, _>("cap:op=my_op;...", |request, emitter| {
//!         emitter.emit_status("processing", "Starting work...");
//!         // Do work, emit chunks in real-time
//!         emitter.emit_bytes(b"partial result");
//!         // Return final result
//!         Ok(b"final result".to_vec())
//!     });
//!
//!     runtime.run().unwrap();
//! }
//! ```

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake_accept, CborError, FrameReader, FrameWriter};
use crate::CapUrn;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Write};
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
    /// Invoke a cap on the host.
    ///
    /// Sends a REQ frame to the host with the specified cap URN and payload.
    /// Returns a receiver that yields response chunks (Vec<u8>) or errors.
    /// The receiver will be closed when the response is complete (END frame received).
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke on the host
    /// * `payload` - The request payload (typically JSON-encoded)
    ///
    /// # Returns
    /// A receiver that yields `Result<Vec<u8>, RuntimeError>` for each chunk.
    /// Iterate over it to collect all response data.
    ///
    /// # Example
    /// ```ignore
    /// let rx = peer.invoke("cap:op=model_path", json_payload.as_slice())?;
    /// let mut response_data = Vec::new();
    /// for chunk_result in rx {
    ///     response_data.extend(chunk_result?);
    /// }
    /// ```
    fn invoke(
        &self,
        cap_urn: &str,
        payload: &[u8],
    ) -> Result<Receiver<Result<Vec<u8>, RuntimeError>>, RuntimeError>;
}

/// A no-op PeerInvoker that always returns an error.
/// Used when peer invocation is not supported.
pub struct NoPeerInvoker;

impl PeerInvoker for NoPeerInvoker {
    fn invoke(
        &self,
        _cap_urn: &str,
        _payload: &[u8],
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

impl<W: Write + Send> PeerInvoker for PeerInvokerImpl<W> {
    fn invoke(
        &self,
        cap_urn: &str,
        payload: &[u8],
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

        // Create and send the REQ frame
        let frame = Frame::req(
            request_id.clone(),
            cap_urn,
            payload.to_vec(),
            "application/json",
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
/// **Multiplexed execution**: Multiple requests can be processed concurrently.
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

    /// Negotiated protocol limits
    limits: Limits,
}

impl PluginRuntime {
    /// Create a new plugin runtime with the required manifest.
    ///
    /// The manifest is JSON-encoded plugin metadata including:
    /// - name: Plugin name
    /// - version: Plugin version
    /// - caps: Array of capability definitions
    ///
    /// This manifest is sent in the HELLO response to the host.
    /// **Plugins MUST provide a manifest - there is no fallback.**
    pub fn new(manifest: &[u8]) -> Self {
        Self {
            handlers: HashMap::new(),
            manifest_data: manifest.to_vec(),
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

    /// Run the plugin runtime, processing requests until stdin closes.
    ///
    /// Protocol lifecycle:
    /// 1. Receive HELLO from host
    /// 2. Send HELLO back with manifest (handshake)
    /// 3. Main loop reads frames:
    ///    - REQ frames: spawn handler thread, continue reading
    ///    - HEARTBEAT frames: respond immediately
    ///    - RES/CHUNK/END frames: route to pending peer requests
    ///    - Other frames: ignore
    /// 4. Exit when stdin closes, wait for active handlers to complete
    ///
    /// **Multiplexing**: The main loop never blocks on handler execution.
    /// Handlers run in separate threads, allowing concurrent processing
    /// of multiple requests and immediate heartbeat responses.
    ///
    /// **Bidirectional communication**: Handlers can invoke caps on the host
    /// using the `PeerInvoker` parameter. Response frames from the host are
    /// routed to the appropriate pending request by MessageId.
    pub fn run(&self) -> Result<(), RuntimeError> {
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
        {
            let mut writer_guard = frame_writer.lock().unwrap();
            let limits = handshake_accept(&mut frame_reader, &mut writer_guard, &self.manifest_data)?;
            frame_reader.set_limits(limits);
            writer_guard.set_limits(limits);
        }

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
                    let payload = frame.payload.clone().unwrap_or_default();

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

                        let result = handler(&payload, &emitter, &peer_invoker);

                        // Write final response (END or ERR)
                        let response_frame = match result {
                            Ok(final_payload) => Frame::end(request_id, Some(final_payload)),
                            Err(e) => Frame::err(request_id, "HANDLER_ERROR", &e.to_string()),
                        };

                        let mut writer = writer_clone.lock().unwrap();
                        if let Err(e) = writer.write(&response_frame) {
                            eprintln!("[PluginRuntime] Failed to write response: {}", e);
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

    /// Test manifest JSON - plugins MUST include manifest
    const TEST_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:op=test","title":"Test","command":"test"}]}"#;

    #[test]
    fn test_register_and_find_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register::<serde_json::Value, _>("cap:in=*;op=test;out=*", |_request, _emitter, _peer| {
            Ok(b"result".to_vec())
        });

        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    #[test]
    fn test_raw_handler() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register_raw("cap:op=raw", |payload, _emitter, _peer| {
            // Echo the payload back
            Ok(payload.to_vec())
        });

        assert!(runtime.find_handler("cap:op=raw").is_some());
    }

    #[test]
    fn test_handler_streaming() {
        // Create runtime with handler
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());
        runtime.register::<serde_json::Value, _>("cap:op=test", |req, emitter, _peer| {
            emitter.emit_status("processing", "Starting...");
            emitter.emit_bytes(b"chunk1");
            emitter.emit_bytes(b"chunk2");

            let result = serde_json::json!({
                "received": req["value"],
                "status": "done"
            });
            serde_json::to_vec(&result)
                .map_err(|e| RuntimeError::Serialize(e.to_string()))
        });

        // Verify handler exists
        assert!(runtime.find_handler("cap:op=test").is_some());
    }

    #[test]
    fn test_handler_is_send_sync() {
        // This test verifies at compile time that handlers can be shared across threads
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register::<serde_json::Value, _>("cap:op=threaded", |_req, emitter, _peer| {
            // Emitter must be usable from handler thread
            emitter.emit_status("in_thread", "Processing in separate thread");
            Ok(b"done".to_vec())
        });

        // Get handler and verify it can be cloned (Arc) and sent to another thread
        let handler = runtime.find_handler("cap:op=threaded").unwrap();
        let handler_clone = Arc::clone(&handler);

        // Spawn a thread to prove handler is Send
        let handle = std::thread::spawn(move || {
            // Handler is usable in another thread
            let _ = &handler_clone;
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_no_peer_invoker() {
        let no_peer = NoPeerInvoker;
        let result = no_peer.invoke("cap:op=test", b"payload");
        assert!(result.is_err());
        match result {
            Err(RuntimeError::PeerRequest(_)) => {}
            _ => panic!("Expected PeerRequest error"),
        }
    }

    #[test]
    fn test_with_manifest_json() {
        let runtime = PluginRuntime::with_manifest_json(TEST_MANIFEST);
        // Verify it was created and manifest is stored
        assert!(!runtime.manifest_data.is_empty());
    }
}
