//! Plugin Host - Caller-side communication with plugin processes
//!
//! The PluginHost manages communication with a running plugin process.
//! It handles:
//!
//! - Sending cap requests via binary packets to plugin stdin
//! - Receiving responses via binary packets from plugin stdout
//! - Request/response correlation by message ID
//! - Streaming response handling
//!
//! # Example
//!
//! ```ignore
//! use capns::{PluginHost, Message};
//! use std::process::{Command, Stdio};
//!
//! // Spawn plugin process
//! let mut child = Command::new("./my-plugin")
//!     .stdin(Stdio::piped())
//!     .stdout(Stdio::piped())
//!     .spawn()?;
//!
//! let stdin = child.stdin.take().unwrap();
//! let stdout = child.stdout.take().unwrap();
//!
//! let mut host = PluginHost::new(stdin, stdout);
//!
//! // Send request and receive responses
//! let responses = host.call("cap:op=test;...", json!({"input": "data"}))?;
//! ```

use crate::message::{Message, MessageError, MessageType};
use crate::packet::{read_packet, write_packet, PacketError, PacketReader, PacketWriter};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{Read, Write};
use uuid::Uuid;

/// Errors that can occur in the plugin host
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("Packet error: {0}")]
    Packet(#[from] PacketError),

    #[error("Message error: {0}")]
    Message(#[from] MessageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("Plugin returned error: [{code}] {message}")]
    PluginError { code: String, message: String },

    #[error("Unexpected message type: {0:?}")]
    UnexpectedMessageType(MessageType),

    #[error("Response ID mismatch: expected {expected}, got {got}")]
    IdMismatch { expected: String, got: String },

    #[error("Plugin process exited unexpectedly")]
    ProcessExited,
}

/// A response from a plugin, which may be single or streaming.
#[derive(Debug)]
pub enum PluginResponse {
    /// Single complete response
    Single(JsonValue),
    /// Streaming response (iterator of chunks)
    Streaming(Vec<JsonValue>),
}

/// Host-side interface for communicating with a plugin process.
///
/// Manages the stdin/stdout pipes and handles message framing.
pub struct PluginHost<W: Write, R: Read> {
    writer: PacketWriter<W>,
    reader: PacketReader<R>,
    pending_requests: HashMap<String, ()>,
}

impl<W: Write, R: Read> PluginHost<W, R> {
    /// Create a new plugin host with the given stdin/stdout handles.
    pub fn new(stdin: W, stdout: R) -> Self {
        Self {
            writer: PacketWriter::new(stdin),
            reader: PacketReader::new(stdout),
            pending_requests: HashMap::new(),
        }
    }

    /// Send a cap request and wait for the complete response.
    ///
    /// For streaming caps, this collects all chunks and returns them together.
    pub fn call(&mut self, cap_urn: &str, payload: JsonValue) -> Result<PluginResponse, HostError> {
        let request_id = Uuid::new_v4().to_string();
        let request = Message::cap_request_with_id(&request_id, cap_urn, payload);

        // Send request
        self.writer.write(&request.to_bytes())?;
        self.pending_requests.insert(request_id.clone(), ());

        // Collect responses
        let mut chunks = Vec::new();

        loop {
            let packet = self.reader.read()?.ok_or(HostError::ProcessExited)?;
            let message = Message::from_bytes(&packet)?;

            // Verify this is for our request
            if message.id != request_id {
                // Could be for a different concurrent request - skip for now
                continue;
            }

            match message.message_type {
                MessageType::CapResponse => {
                    self.pending_requests.remove(&request_id);
                    return Ok(PluginResponse::Single(message.payload));
                }
                MessageType::StreamChunk => {
                    chunks.push(message.payload);
                }
                MessageType::StreamEnd => {
                    chunks.push(message.payload);
                    self.pending_requests.remove(&request_id);
                    return Ok(PluginResponse::Streaming(chunks));
                }
                MessageType::Error => {
                    self.pending_requests.remove(&request_id);
                    let code = message.payload["code"]
                        .as_str()
                        .unwrap_or("UNKNOWN")
                        .to_string();
                    let msg = message.payload["message"]
                        .as_str()
                        .unwrap_or("Unknown error")
                        .to_string();
                    return Err(HostError::PluginError {
                        code,
                        message: msg,
                    });
                }
                MessageType::CapRequest => {
                    // Plugin sent us a request (e.g., for tool call)
                    // This would be handled by a callback in a full implementation
                    return Err(HostError::UnexpectedMessageType(MessageType::CapRequest));
                }
            }
        }
    }

    /// Send a cap request and return an iterator over streaming responses.
    ///
    /// This allows processing chunks as they arrive without waiting for completion.
    pub fn call_streaming(
        &mut self,
        cap_urn: &str,
        payload: JsonValue,
    ) -> Result<StreamingResponse<'_, W, R>, HostError> {
        let request_id = Uuid::new_v4().to_string();
        let request = Message::cap_request_with_id(&request_id, cap_urn, payload);

        // Send request
        self.writer.write(&request.to_bytes())?;
        self.pending_requests.insert(request_id.clone(), ());

        Ok(StreamingResponse {
            host: self,
            request_id,
            finished: false,
        })
    }

    /// Send raw bytes as a packet (for low-level control).
    pub fn send_raw(&mut self, data: &[u8]) -> Result<(), HostError> {
        self.writer.write(data)?;
        Ok(())
    }

    /// Receive a raw packet (for low-level control).
    pub fn recv_raw(&mut self) -> Result<Option<Vec<u8>>, HostError> {
        Ok(self.reader.read()?)
    }

    /// Get mutable access to the writer for direct packet writing.
    pub fn writer_mut(&mut self) -> &mut PacketWriter<W> {
        &mut self.writer
    }

    /// Get mutable access to the reader for direct packet reading.
    pub fn reader_mut(&mut self) -> &mut PacketReader<R> {
        &mut self.reader
    }
}

/// Iterator over streaming responses from a plugin.
pub struct StreamingResponse<'a, W: Write, R: Read> {
    host: &'a mut PluginHost<W, R>,
    request_id: String,
    finished: bool,
}

impl<'a, W: Write, R: Read> StreamingResponse<'a, W, R> {
    /// Get the next chunk, or None if the stream is complete.
    pub fn next_chunk(&mut self) -> Result<Option<JsonValue>, HostError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let packet = self.host.reader.read()?.ok_or(HostError::ProcessExited)?;
            let message = Message::from_bytes(&packet)?;

            // Verify this is for our request
            if message.id != self.request_id {
                continue;
            }

            match message.message_type {
                MessageType::StreamChunk => {
                    return Ok(Some(message.payload));
                }
                MessageType::StreamEnd => {
                    self.finished = true;
                    self.host.pending_requests.remove(&self.request_id);
                    return Ok(Some(message.payload));
                }
                MessageType::CapResponse => {
                    // Single response treated as stream end
                    self.finished = true;
                    self.host.pending_requests.remove(&self.request_id);
                    return Ok(Some(message.payload));
                }
                MessageType::Error => {
                    self.finished = true;
                    self.host.pending_requests.remove(&self.request_id);
                    let code = message.payload["code"]
                        .as_str()
                        .unwrap_or("UNKNOWN")
                        .to_string();
                    let msg = message.payload["message"]
                        .as_str()
                        .unwrap_or("Unknown error")
                        .to_string();
                    return Err(HostError::PluginError {
                        code,
                        message: msg,
                    });
                }
                MessageType::CapRequest => {
                    return Err(HostError::UnexpectedMessageType(MessageType::CapRequest));
                }
            }
        }
    }

    /// Check if the stream is finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl<'a, W: Write, R: Read> Iterator for StreamingResponse<'a, W, R> {
    type Item = Result<JsonValue, HostError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_response(request_id: &str, payload: JsonValue) -> Vec<u8> {
        let msg = Message::cap_response(request_id, payload);
        let msg_bytes = msg.to_bytes();
        let mut packet = (msg_bytes.len() as u32).to_be_bytes().to_vec();
        packet.extend(msg_bytes);
        packet
    }

    #[test]
    fn test_single_response() {
        // Create a mock response
        let response_data = create_test_response("test-id", serde_json::json!({"result": "ok"}));

        let stdin = Vec::new();
        let stdout = Cursor::new(response_data);

        let mut host = PluginHost::new(stdin, stdout);

        // We need to manually set the request_id since we're testing with mock data
        // In real usage, call() generates the ID and sends the request first
    }
}
