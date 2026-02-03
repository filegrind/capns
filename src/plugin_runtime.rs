//! Plugin Runtime - Unified I/O handling for plugin binaries
//!
//! The PluginRuntime provides a unified interface for plugin binaries to handle
//! cap invocations. Plugins register handlers for caps they provide, and the
//! runtime handles all I/O mechanics:
//!
//! - Binary packet framing on stdin/stdout
//! - Message envelope parsing/serialization
//! - CLI argument conversion to messages (for CLI invocation mode)
//! - Handler routing by cap URN
//! - Streaming response support
//!
//! # Example
//!
//! ```ignore
//! use capns::{PluginRuntime, Message};
//!
//! fn main() {
//!     let mut runtime = PluginRuntime::new();
//!
//!     runtime.register("cap:op=llm_inference;...", |request| {
//!         // Handle the request, return response(s)
//!         Ok(vec![Message::cap_response(&request.id, json!({"text": "hello"}))])
//!     });
//!
//!     runtime.run().unwrap();
//! }
//! ```

use crate::message::{Message, MessageError, MessageType};
use crate::packet::{read_packet, write_packet, PacketError};
use crate::{Cap, CapManifest, CapUrn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

/// Errors that can occur in the plugin runtime
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Packet error: {0}")]
    Packet(#[from] PacketError),

    #[error("Message error: {0}")]
    Message(#[from] MessageError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("No handler registered for cap: {0}")]
    NoHandler(String),

    #[error("Handler error: {0}")]
    Handler(String),

    #[error("Cap URN parse error: {0}")]
    CapUrn(String),

    #[error("CLI parse error: {0}")]
    CliParse(String),
}

/// Response from a cap handler
#[derive(Debug)]
pub enum HandlerResponse {
    /// Single complete response
    Single(JsonValue),
    /// Multiple streaming chunks followed by final response
    Stream(Vec<JsonValue>),
    /// Error response
    Error { code: String, message: String },
}

/// A cap handler function
pub type HandlerFn = Arc<dyn Fn(&Message) -> Result<HandlerResponse, RuntimeError> + Send + Sync>;

/// The plugin runtime that handles all I/O for plugin binaries.
///
/// Plugins create a runtime, register handlers for their caps,
/// then call `run()` to process requests.
pub struct PluginRuntime {
    /// Registered handlers by cap URN pattern
    handlers: HashMap<String, HandlerFn>,

    /// Plugin manifest (caps this plugin provides)
    manifest: Option<CapManifest>,

    /// Whether to run in CLI mode (single request from args) or streaming mode (stdin packets)
    cli_mode: bool,
}

impl PluginRuntime {
    /// Create a new plugin runtime.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            manifest: None,
            cli_mode: false,
        }
    }

    /// Set the plugin manifest.
    pub fn with_manifest(mut self, manifest: CapManifest) -> Self {
        self.manifest = Some(manifest);
        self
    }

    /// Set CLI mode (process single request from args, then exit).
    pub fn cli_mode(mut self) -> Self {
        self.cli_mode = true;
        self
    }

    /// Register a handler for a cap URN.
    ///
    /// The cap_urn can be a full URN or a pattern. The handler will be called
    /// for any request whose cap URN matches.
    pub fn register<F>(&mut self, cap_urn: &str, handler: F)
    where
        F: Fn(&Message) -> Result<HandlerResponse, RuntimeError> + Send + Sync + 'static,
    {
        self.handlers.insert(cap_urn.to_string(), Arc::new(handler));
    }

    /// Register a simple handler that takes typed request and returns typed response.
    pub fn register_typed<Req, Resp, F>(&mut self, cap_urn: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned,
        Resp: serde::Serialize,
        F: Fn(Req) -> Result<Resp, RuntimeError> + Send + Sync + 'static,
    {
        self.register(cap_urn, move |msg| {
            let request: Req = serde_json::from_value(msg.payload.clone())
                .map_err(|e| RuntimeError::Handler(format!("Failed to parse request: {}", e)))?;

            let response = handler(request)?;

            let json = serde_json::to_value(response)
                .map_err(|e| RuntimeError::Handler(format!("Failed to serialize response: {}", e)))?;

            Ok(HandlerResponse::Single(json))
        });
    }

    /// Register a streaming handler that yields multiple responses.
    pub fn register_streaming<Req, F>(&mut self, cap_urn: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned,
        F: Fn(Req, &mut dyn FnMut(JsonValue)) -> Result<JsonValue, RuntimeError> + Send + Sync + 'static,
    {
        self.register(cap_urn, move |msg| {
            let request: Req = serde_json::from_value(msg.payload.clone())
                .map_err(|e| RuntimeError::Handler(format!("Failed to parse request: {}", e)))?;

            let mut chunks = Vec::new();
            let final_response = handler(request, &mut |chunk| {
                chunks.push(chunk);
            })?;

            chunks.push(final_response);
            Ok(HandlerResponse::Stream(chunks))
        });
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

    /// Process a single request message and write response(s).
    fn process_request<W: Write>(
        &self,
        request: &Message,
        writer: &mut W,
    ) -> Result<(), RuntimeError> {
        let cap_urn = request.cap.as_ref().ok_or_else(|| {
            RuntimeError::Message(MessageError::MissingField("cap".to_string()))
        })?;

        let handler = self.find_handler(cap_urn).ok_or_else(|| {
            RuntimeError::NoHandler(cap_urn.clone())
        })?;

        match handler(request) {
            Ok(HandlerResponse::Single(payload)) => {
                let response = Message::cap_response(&request.id, payload);
                write_packet(writer, &response.to_bytes())?;
            }
            Ok(HandlerResponse::Stream(chunks)) => {
                let len = chunks.len();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let msg = if i == len - 1 {
                        // Last chunk is the stream end
                        Message::stream_end(&request.id, chunk)
                    } else {
                        Message::stream_chunk(&request.id, chunk)
                    };
                    write_packet(writer, &msg.to_bytes())?;
                }
            }
            Ok(HandlerResponse::Error { code, message }) => {
                let response = Message::error(&request.id, &code, &message);
                write_packet(writer, &response.to_bytes())?;
            }
            Err(e) => {
                let response = Message::error(&request.id, "HANDLER_ERROR", &e.to_string());
                write_packet(writer, &response.to_bytes())?;
            }
        }

        Ok(())
    }

    /// Run the plugin runtime, processing requests until stdin closes.
    ///
    /// In CLI mode, processes a single request from CLI args then exits.
    /// In streaming mode, reads binary packets from stdin continuously.
    pub fn run(&self) -> Result<(), RuntimeError> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        let mut reader = BufReader::new(stdin.lock());
        let mut writer = BufWriter::new(stdout.lock());

        loop {
            // Read next packet
            let packet = match read_packet(&mut reader)? {
                Some(p) => p,
                None => break, // Clean EOF
            };

            // Parse message
            let message = Message::from_bytes(&packet)?;

            // Only process requests
            if !message.is_request() {
                continue;
            }

            // Process and respond
            self.process_request(&message, &mut writer)?;
        }

        Ok(())
    }

    /// Run in "one-shot" mode: read a single JSON line from stdin, process, write response.
    ///
    /// This is for backward compatibility with simple NDJSON-style invocation.
    /// The cap URN must be provided.
    pub fn run_oneshot(&self, cap_urn: &str) -> Result<(), RuntimeError> {
        let stdin = io::stdin();
        let stdout = io::stdout();

        let mut input = String::new();
        stdin.lock().read_to_string(&mut input)?;

        // Parse input as JSON payload
        let payload: JsonValue = serde_json::from_str(input.trim())
            .map_err(|e| RuntimeError::CliParse(format!("Invalid JSON input: {}", e)))?;

        // Create request message
        let request = Message::cap_request(cap_urn, payload);

        // Find and call handler
        let handler = self.find_handler(cap_urn).ok_or_else(|| {
            RuntimeError::NoHandler(cap_urn.to_string())
        })?;

        let mut writer = BufWriter::new(stdout.lock());

        match handler(&request) {
            Ok(HandlerResponse::Single(payload)) => {
                // Write as NDJSON line (not binary packet) for backward compat
                let line = serde_json::to_string(&payload)
                    .map_err(|e| RuntimeError::Handler(e.to_string()))?;
                writeln!(writer, "{}", line)?;
            }
            Ok(HandlerResponse::Stream(chunks)) => {
                // Write each chunk as NDJSON line
                for chunk in chunks {
                    let line = serde_json::to_string(&chunk)
                        .map_err(|e| RuntimeError::Handler(e.to_string()))?;
                    writeln!(writer, "{}", line)?;
                }
            }
            Ok(HandlerResponse::Error { code, message }) => {
                let error = serde_json::json!({"type": "error", "code": code, "message": message});
                let line = serde_json::to_string(&error)
                    .map_err(|e| RuntimeError::Handler(e.to_string()))?;
                writeln!(writer, "{}", line)?;
            }
            Err(e) => {
                let error = serde_json::json!({"type": "error", "code": "HANDLER_ERROR", "message": e.to_string()});
                let line = serde_json::to_string(&error)
                    .map_err(|e| RuntimeError::Handler(e.to_string()))?;
                writeln!(writer, "{}", line)?;
            }
        }

        writer.flush()?;
        Ok(())
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

        runtime.register("cap:in=*;op=test;out=*", |_msg| {
            Ok(HandlerResponse::Single(serde_json::json!({"result": "ok"})))
        });

        assert!(runtime.find_handler("cap:in=*;op=test;out=*").is_some());
    }

    #[test]
    fn test_handler_response_types() {
        let single = HandlerResponse::Single(serde_json::json!({"key": "value"}));
        assert!(matches!(single, HandlerResponse::Single(_)));

        let stream = HandlerResponse::Stream(vec![
            serde_json::json!({"chunk": 1}),
            serde_json::json!({"chunk": 2}),
        ]);
        assert!(matches!(stream, HandlerResponse::Stream(_)));

        let error = HandlerResponse::Error {
            code: "TEST".to_string(),
            message: "test error".to_string(),
        };
        assert!(matches!(error, HandlerResponse::Error { .. }));
    }
}
