//! Plugin Host - Caller-side communication with plugin processes
//!
//! The PluginHost manages communication with a running plugin process.
//! It handles:
//!
//! - CBOR frame encoding/decoding
//! - Sending cap requests via binary packets to plugin stdin
//! - Receiving responses via binary packets from plugin stdout
//! - Request/response correlation by message ID
//! - HELLO handshake for limit negotiation
//! - Streaming response handling
//!
//! # Example
//!
//! ```ignore
//! use capns::PluginHost;
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
//! let mut host = PluginHost::new(stdin, stdout)?;
//!
//! // Send request and receive responses
//! for chunk in host.call_streaming("cap:op=test;...", b"payload")? {
//!     println!("Received chunk: {:?}", chunk?);
//! }
//! ```

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake, CborError, FrameReader, FrameWriter};
use std::io::{Read, Write};

/// Errors that can occur in the plugin host
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("CBOR error: {0}")]
    Cbor(#[from] CborError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Plugin returned error: [{code}] {message}")]
    PluginError { code: String, message: String },

    #[error("Unexpected frame type: {0:?}")]
    UnexpectedFrameType(FrameType),

    #[error("Response ID mismatch: expected {expected}, got {got}")]
    IdMismatch { expected: String, got: String },

    #[error("Plugin process exited unexpectedly")]
    ProcessExited,

    #[error("Handshake failed: {0}")]
    Handshake(String),
}

/// A response chunk from a plugin
#[derive(Debug, Clone)]
pub struct ResponseChunk {
    /// The binary payload
    pub payload: Vec<u8>,
    /// Sequence number
    pub seq: u64,
    /// Offset in the stream (for chunked transfers)
    pub offset: Option<u64>,
    /// Total length (set on first chunk of chunked transfer)
    pub len: Option<u64>,
    /// Whether this is the final chunk
    pub is_eof: bool,
}

/// A complete response from a plugin, which may be single or streaming.
#[derive(Debug)]
pub enum PluginResponse {
    /// Single complete response
    Single(Vec<u8>),
    /// Streaming response (collected chunks)
    Streaming(Vec<ResponseChunk>),
}

impl PluginResponse {
    /// Get the final payload (single response or last chunk of streaming)
    pub fn final_payload(&self) -> Option<&[u8]> {
        match self {
            PluginResponse::Single(data) => Some(data),
            PluginResponse::Streaming(chunks) => {
                chunks.last().map(|c| c.payload.as_slice())
            }
        }
    }

    /// Concatenate all payloads into a single buffer
    pub fn concatenated(&self) -> Vec<u8> {
        match self {
            PluginResponse::Single(data) => data.clone(),
            PluginResponse::Streaming(chunks) => {
                let total_len: usize = chunks.iter().map(|c| c.payload.len()).sum();
                let mut result = Vec::with_capacity(total_len);
                for chunk in chunks {
                    result.extend_from_slice(&chunk.payload);
                }
                result
            }
        }
    }
}

/// Host-side interface for communicating with a plugin process.
///
/// Manages the stdin/stdout pipes and handles CBOR frame encoding.
pub struct PluginHost<W: Write, R: Read> {
    writer: FrameWriter<W>,
    reader: FrameReader<R>,
    limits: Limits,
}

impl<W: Write, R: Read> PluginHost<W, R> {
    /// Create a new plugin host and perform handshake.
    ///
    /// This sends a HELLO frame, waits for the plugin's HELLO,
    /// and negotiates protocol limits.
    pub fn new(stdin: W, stdout: R) -> Result<Self, HostError> {
        let mut writer = FrameWriter::new(stdin);
        let mut reader = FrameReader::new(stdout);

        // Perform handshake
        let limits = handshake(&mut reader, &mut writer)?;

        Ok(Self {
            writer,
            reader,
            limits,
        })
    }

    /// Create a plugin host without performing handshake.
    ///
    /// Use this if you need to handle handshake separately.
    pub fn new_without_handshake(stdin: W, stdout: R) -> Self {
        Self {
            writer: FrameWriter::new(stdin),
            reader: FrameReader::new(stdout),
            limits: Limits::default(),
        }
    }

    /// Perform handshake manually.
    pub fn handshake(&mut self) -> Result<Limits, HostError> {
        let limits = handshake(&mut self.reader, &mut self.writer)?;
        self.limits = limits;
        Ok(limits)
    }

    /// Send a cap request and wait for the complete response.
    ///
    /// For streaming caps, this collects all chunks and returns them together.
    pub fn call(&mut self, cap_urn: &str, payload: &[u8]) -> Result<PluginResponse, HostError> {
        let request_id = MessageId::new_uuid();
        let request = Frame::req(
            request_id.clone(),
            cap_urn,
            payload.to_vec(),
            "application/json",
        );

        // Send request
        self.writer.write(&request)?;

        // Collect responses
        let mut chunks = Vec::new();

        loop {
            let frame = self.reader.read()?.ok_or(HostError::ProcessExited)?;

            // Verify this is for our request
            if frame.id != request_id {
                // Could be for a different concurrent request - skip for now
                continue;
            }

            match frame.frame_type {
                FrameType::Chunk => {
                    let is_eof = frame.is_eof();
                    let chunk = ResponseChunk {
                        payload: frame.payload.unwrap_or_default(),
                        seq: frame.seq,
                        offset: frame.offset,
                        len: frame.len,
                        is_eof,
                    };
                    chunks.push(chunk);

                    if is_eof {
                        return Ok(PluginResponse::Streaming(chunks));
                    }
                }
                FrameType::Res => {
                    // Single complete response
                    return Ok(PluginResponse::Single(frame.payload.unwrap_or_default()));
                }
                FrameType::End => {
                    // Stream end
                    if let Some(payload) = frame.payload {
                        let chunk = ResponseChunk {
                            payload,
                            seq: frame.seq,
                            offset: frame.offset,
                            len: frame.len,
                            is_eof: true,
                        };
                        chunks.push(chunk);
                    }
                    return Ok(PluginResponse::Streaming(chunks));
                }
                FrameType::Log => {
                    // Log message - could emit via callback, for now just continue
                    continue;
                }
                FrameType::Err => {
                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                    return Err(HostError::PluginError { code, message });
                }
                FrameType::Hello => {
                    // Unexpected HELLO during request
                    return Err(HostError::UnexpectedFrameType(FrameType::Hello));
                }
                FrameType::Req => {
                    // Plugin sent us a request (e.g., for tool call)
                    // This would be handled by a callback in a full implementation
                    return Err(HostError::UnexpectedFrameType(FrameType::Req));
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
        payload: &[u8],
    ) -> Result<StreamingResponse<'_, W, R>, HostError> {
        let request_id = MessageId::new_uuid();
        let request = Frame::req(
            request_id.clone(),
            cap_urn,
            payload.to_vec(),
            "application/json",
        );

        // Send request
        self.writer.write(&request)?;

        Ok(StreamingResponse {
            host: self,
            request_id,
            finished: false,
        })
    }

    /// Send a raw CBOR frame.
    pub fn send_frame(&mut self, frame: &Frame) -> Result<(), HostError> {
        self.writer.write(frame)?;
        Ok(())
    }

    /// Receive the next CBOR frame.
    pub fn recv_frame(&mut self) -> Result<Option<Frame>, HostError> {
        Ok(self.reader.read()?)
    }

    /// Get the negotiated protocol limits.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Get mutable access to the frame writer.
    pub fn writer_mut(&mut self) -> &mut FrameWriter<W> {
        &mut self.writer
    }

    /// Get mutable access to the frame reader.
    pub fn reader_mut(&mut self) -> &mut FrameReader<R> {
        &mut self.reader
    }
}

/// Iterator over streaming responses from a plugin.
pub struct StreamingResponse<'a, W: Write, R: Read> {
    host: &'a mut PluginHost<W, R>,
    request_id: MessageId,
    finished: bool,
}

impl<'a, W: Write, R: Read> StreamingResponse<'a, W, R> {
    /// Get the next chunk, or None if the stream is complete.
    pub fn next_chunk(&mut self) -> Result<Option<ResponseChunk>, HostError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            let frame = self.host.reader.read()?.ok_or(HostError::ProcessExited)?;

            // Verify this is for our request
            if frame.id != self.request_id {
                continue;
            }

            match frame.frame_type {
                FrameType::Chunk => {
                    let is_eof = frame.is_eof();
                    let chunk = ResponseChunk {
                        payload: frame.payload.unwrap_or_default(),
                        seq: frame.seq,
                        offset: frame.offset,
                        len: frame.len,
                        is_eof,
                    };

                    if is_eof {
                        self.finished = true;
                    }

                    return Ok(Some(chunk));
                }
                FrameType::Res => {
                    // Single response treated as final chunk
                    self.finished = true;
                    return Ok(Some(ResponseChunk {
                        payload: frame.payload.unwrap_or_default(),
                        seq: 0,
                        offset: None,
                        len: None,
                        is_eof: true,
                    }));
                }
                FrameType::End => {
                    self.finished = true;
                    if let Some(payload) = frame.payload {
                        return Ok(Some(ResponseChunk {
                            payload,
                            seq: frame.seq,
                            offset: frame.offset,
                            len: frame.len,
                            is_eof: true,
                        }));
                    }
                    return Ok(None);
                }
                FrameType::Log => {
                    // Log message - continue waiting for data
                    continue;
                }
                FrameType::Err => {
                    self.finished = true;
                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                    return Err(HostError::PluginError { code, message });
                }
                FrameType::Hello | FrameType::Req => {
                    return Err(HostError::UnexpectedFrameType(frame.frame_type));
                }
            }
        }
    }

    /// Check if the stream is finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Get the request ID for this stream.
    pub fn request_id(&self) -> &MessageId {
        &self.request_id
    }
}

impl<'a, W: Write, R: Read> Iterator for StreamingResponse<'a, W, R> {
    type Item = Result<ResponseChunk, HostError>;

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
    use crate::cbor_io::{write_frame, encode_frame};
    use std::io::Cursor;

    fn create_hello_response() -> Vec<u8> {
        let limits = Limits::default();
        let frame = Frame::hello(limits.max_frame, limits.max_chunk);
        let cbor = encode_frame(&frame).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
        buf.extend_from_slice(&cbor);
        buf
    }

    fn create_end_response(id: MessageId, payload: &[u8]) -> Vec<u8> {
        let limits = Limits::default();
        let frame = Frame::end(id, Some(payload.to_vec()));
        let cbor = encode_frame(&frame).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
        buf.extend_from_slice(&cbor);
        buf
    }

    #[test]
    fn test_host_creation() {
        // Create mock response with HELLO
        let response_data = create_hello_response();

        let stdin = Vec::new();
        let stdout = Cursor::new(response_data);

        // Should succeed with handshake
        let host = PluginHost::new(stdin, stdout);
        assert!(host.is_ok());
    }

    #[test]
    fn test_host_without_handshake() {
        let stdin = Vec::new();
        let stdout = Cursor::new(Vec::<u8>::new());

        let host = PluginHost::new_without_handshake(stdin, stdout);
        assert_eq!(host.limits().max_frame, Limits::default().max_frame);
    }

    #[test]
    fn test_response_chunk() {
        let chunk = ResponseChunk {
            payload: b"hello".to_vec(),
            seq: 0,
            offset: None,
            len: None,
            is_eof: false,
        };

        assert_eq!(chunk.payload, b"hello");
        assert!(!chunk.is_eof);
    }

    #[test]
    fn test_plugin_response_single() {
        let response = PluginResponse::Single(b"result".to_vec());
        assert_eq!(response.final_payload(), Some(b"result".as_slice()));
        assert_eq!(response.concatenated(), b"result");
    }

    #[test]
    fn test_plugin_response_streaming() {
        let chunks = vec![
            ResponseChunk {
                payload: b"hello".to_vec(),
                seq: 0,
                offset: Some(0),
                len: Some(11),
                is_eof: false,
            },
            ResponseChunk {
                payload: b" world".to_vec(),
                seq: 1,
                offset: Some(5),
                len: None,
                is_eof: true,
            },
        ];

        let response = PluginResponse::Streaming(chunks);
        assert_eq!(response.concatenated(), b"hello world");
    }
}
