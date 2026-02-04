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
use crate::{CapManifest, CapUrn};
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
/// Receives request payload bytes and emitter, returns response payload bytes.
pub type HandlerFn = Arc<
    dyn Fn(&[u8], &dyn StreamEmitter) -> Result<Vec<u8>, RuntimeError> + Send + Sync,
>;

/// The plugin runtime that handles all I/O for plugin binaries.
///
/// Plugins create a runtime, register handlers for their caps,
/// then call `run()` to process requests.
///
/// **Multiplexed execution**: Multiple requests can be processed concurrently.
/// Each request handler runs in its own thread, allowing the runtime to:
/// - Respond to heartbeats while handlers are running
/// - Accept new requests while previous ones are still processing
/// - Handle multiple concurrent cap invocations
pub struct PluginRuntime {
    /// Registered handlers by cap URN pattern (Arc for thread-safe sharing)
    handlers: HashMap<String, HandlerFn>,

    /// Plugin manifest (caps this plugin provides)
    manifest: Option<CapManifest>,

    /// Negotiated protocol limits
    limits: Limits,
}

impl PluginRuntime {
    /// Create a new plugin runtime.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            manifest: None,
            limits: Limits::default(),
        }
    }

    /// Set the plugin manifest.
    pub fn with_manifest(mut self, manifest: CapManifest) -> Self {
        self.manifest = Some(manifest);
        self
    }

    /// Register a handler for a cap URN.
    ///
    /// The handler receives the request payload as bytes (typically JSON or CBOR)
    /// and an emitter for streaming output. It returns the final response payload bytes.
    ///
    /// Chunks emitted by the handler are written immediately to stdout.
    /// This is essential for progress updates and real-time token streaming.
    ///
    /// **Thread safety**: Handlers run in separate threads, so they must be
    /// Send + Sync. The emitter is thread-safe and can be used freely.
    pub fn register<Req, F>(&mut self, cap_urn: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned + 'static,
        F: Fn(Req, &dyn StreamEmitter) -> Result<Vec<u8>, RuntimeError> + Send + Sync + 'static,
    {
        let handler = move |payload: &[u8], emitter: &dyn StreamEmitter| -> Result<Vec<u8>, RuntimeError> {
            // Deserialize request from payload bytes (JSON format for now)
            let request: Req = serde_json::from_slice(payload)
                .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse request: {}", e)))?;

            handler(request, emitter)
        };

        self.handlers.insert(cap_urn.to_string(), Arc::new(handler));
    }

    /// Register a raw handler that works with bytes directly.
    ///
    /// Use this when you need full control over serialization.
    pub fn register_raw<F>(&mut self, cap_urn: &str, handler: F)
    where
        F: Fn(&[u8], &dyn StreamEmitter) -> Result<Vec<u8>, RuntimeError> + Send + Sync + 'static,
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
    /// 2. Send HELLO back (handshake)
    /// 3. Main loop reads frames:
    ///    - REQ frames: spawn handler thread, continue reading
    ///    - HEARTBEAT frames: respond immediately
    ///    - Other frames: ignore
    /// 4. Exit when stdin closes, wait for active handlers to complete
    ///
    /// **Multiplexing**: The main loop never blocks on handler execution.
    /// Handlers run in separate threads, allowing concurrent processing
    /// of multiple requests and immediate heartbeat responses.
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

        // Perform handshake
        {
            let mut writer_guard = frame_writer.lock().unwrap();
            let limits = handshake_accept(&mut frame_reader, &mut writer_guard)?;
            frame_reader.set_limits(limits);
            writer_guard.set_limits(limits);
        }

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
                    let request_id = frame.id.clone();
                    let payload = frame.payload.clone().unwrap_or_default();

                    // Spawn handler in separate thread - main loop continues immediately
                    let handle = thread::spawn(move || {
                        let emitter = ThreadSafeEmitter {
                            writer: Arc::clone(&writer_clone),
                            request_id: request_id.clone(),
                            seq: Mutex::new(0),
                        };

                        let result = handler(&payload, &emitter);

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
                    // These would be responses to plugin-initiated requests (cap invocation)
                    // For now, ignore - full cap invocation support would route these
                    // to waiting request handlers
                    continue;
                }
                FrameType::Log | FrameType::Err => {
                    // Shouldn't receive these from host, ignore
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

impl Default for PluginRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_find_handler() {
        let mut runtime = PluginRuntime::new();

        runtime.register::<serde_json::Value, _>("cap:in=*;op=test;out=*", |_request, _emitter| {
            Ok(b"result".to_vec())
        });

        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    #[test]
    fn test_raw_handler() {
        let mut runtime = PluginRuntime::new();

        runtime.register_raw("cap:op=raw", |payload, _emitter| {
            // Echo the payload back
            Ok(payload.to_vec())
        });

        assert!(runtime.find_handler("cap:op=raw").is_some());
    }

    #[test]
    fn test_handler_streaming() {
        // Create runtime with handler
        let mut runtime = PluginRuntime::new();
        runtime.register::<serde_json::Value, _>("cap:op=test", |req, emitter| {
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
        let mut runtime = PluginRuntime::new();

        runtime.register::<serde_json::Value, _>("cap:op=threaded", |_req, emitter| {
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
}
