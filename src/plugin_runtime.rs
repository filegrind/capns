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
/// Uses interior mutability so it can be called from within closures.
pub trait StreamEmitter {
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

/// Implementation of StreamEmitter that writes CBOR frames
struct FrameStreamEmitter<'a, W: Write> {
    writer: std::cell::RefCell<&'a mut FrameWriter<W>>,
    request_id: MessageId,
    seq: std::cell::Cell<u64>,
}

impl<'a, W: Write> StreamEmitter for FrameStreamEmitter<'a, W> {
    fn emit_bytes(&self, payload: &[u8]) {
        let seq = self.seq.get();
        let frame = Frame::chunk(self.request_id.clone(), seq, payload.to_vec());

        let mut writer = self.writer.borrow_mut();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write chunk: {}", e);
        }

        self.seq.set(seq + 1);
    }

    fn log(&self, level: &str, message: &str) {
        let frame = Frame::log(self.request_id.clone(), level, message);

        let mut writer = self.writer.borrow_mut();
        if let Err(e) = writer.write(&frame) {
            eprintln!("[PluginRuntime] Failed to write log: {}", e);
        }
    }
}

/// Handler function type
/// Receives request payload bytes and emitter, returns response payload bytes
type HandlerFn = Box<
    dyn Fn(&[u8], &dyn StreamEmitter) -> Result<Vec<u8>, RuntimeError> + Send + Sync,
>;

/// The plugin runtime that handles all I/O for plugin binaries.
///
/// Plugins create a runtime, register handlers for their caps,
/// then call `run()` to process requests.
///
/// All handlers use immediate streaming - chunks are written to stdout
/// in real-time as they are emitted.
pub struct PluginRuntime {
    /// Registered handlers by cap URN pattern
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

        self.handlers.insert(cap_urn.to_string(), Box::new(handler));
    }

    /// Register a raw handler that works with bytes directly.
    ///
    /// Use this when you need full control over serialization.
    pub fn register_raw<F>(&mut self, cap_urn: &str, handler: F)
    where
        F: Fn(&[u8], &dyn StreamEmitter) -> Result<Vec<u8>, RuntimeError> + Send + Sync + 'static,
    {
        self.handlers.insert(cap_urn.to_string(), Box::new(handler));
    }

    /// Find a handler for a cap URN.
    fn find_handler(&self, cap_urn: &str) -> Option<&HandlerFn> {
        // First try exact match
        if let Some(handler) = self.handlers.get(cap_urn) {
            return Some(handler);
        }

        // Then try pattern matching via CapUrn
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        for (pattern, handler) in &self.handlers {
            if let Ok(pattern_urn) = CapUrn::from_string(pattern) {
                if pattern_urn.matches(&request_urn) {
                    return Some(handler);
                }
            }
        }

        None
    }

    /// Process a single request frame and write response(s).
    fn process_request<W: Write>(
        &self,
        request: &Frame,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), RuntimeError> {
        let cap_urn = request.cap.as_ref().ok_or_else(|| {
            RuntimeError::Handler("Request missing cap URN".to_string())
        })?;

        let request_id = request.id.clone();

        // Find handler
        let handler = self.find_handler(cap_urn).ok_or_else(|| {
            RuntimeError::NoHandler(cap_urn.clone())
        })?;

        // Get request payload (empty if not present)
        let payload = request.payload.as_ref().map(|p| p.as_slice()).unwrap_or(&[]);

        // Create emitter for streaming output
        let emitter = FrameStreamEmitter {
            writer: std::cell::RefCell::new(writer),
            request_id: request_id.clone(),
            seq: std::cell::Cell::new(0),
        };

        // Execute handler
        match handler(payload, &emitter) {
            Ok(final_payload) => {
                // Send END frame with final payload
                let end_frame = Frame::end(request_id, Some(final_payload));
                writer.write(&end_frame)?;
            }
            Err(e) => {
                // Send ERR frame
                let err_frame = Frame::err(request_id, "HANDLER_ERROR", &e.to_string());
                writer.write(&err_frame)?;
            }
        }

        Ok(())
    }

    /// Run the plugin runtime, processing requests until stdin closes.
    ///
    /// Protocol lifecycle:
    /// 1. Receive HELLO from host
    /// 2. Send HELLO back (handshake)
    /// 3. Wait for REQ frames
    /// 4. For each REQ:
    ///    - Send CHUNK frames as data is produced (real-time)
    ///    - Send LOG frames for status/progress
    ///    - Send END frame when complete, or ERR frame on error
    /// 5. Loop back to step 3, or exit when stdin closes
    pub fn run(&self) -> Result<(), RuntimeError> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        let reader = BufReader::new(stdin.lock());
        let writer = BufWriter::new(stdout.lock());

        let mut frame_reader = FrameReader::new(reader);
        let mut frame_writer = FrameWriter::new(writer);

        // Perform handshake
        let limits = handshake_accept(&mut frame_reader, &mut frame_writer)?;
        frame_reader.set_limits(limits);
        frame_writer.set_limits(limits);

        // Process requests
        loop {
            let frame = match frame_reader.read()? {
                Some(f) => f,
                None => break, // EOF - stdin closed, exit cleanly
            };

            match frame.frame_type {
                FrameType::Req => {
                    // Process the request
                    if let Err(e) = self.process_request(&frame, &mut frame_writer) {
                        // Send error if we haven't already
                        let err_frame = Frame::err(frame.id, "RUNTIME_ERROR", &e.to_string());
                        let _ = frame_writer.write(&err_frame);
                    }
                }
                FrameType::Hello => {
                    // Unexpected HELLO after handshake - protocol error
                    let err_frame = Frame::err(frame.id, "PROTOCOL_ERROR", "Unexpected HELLO after handshake");
                    frame_writer.write(&err_frame)?;
                }
                _ => {
                    // Ignore other frame types (plugin shouldn't receive CHUNK/END/etc)
                    continue;
                }
            }
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
    use std::io::Cursor;

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
        use crate::cbor_io::encode_frame;

        // Create a mock request
        let request_id = MessageId::new_uuid();
        let request = Frame::req(
            request_id.clone(),
            "cap:op=test",
            br#"{"value": 42}"#.to_vec(),
            "application/json",
        );

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
}
