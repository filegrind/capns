//! Plugin Host - Caller-side runtime for communicating with plugin processes
//!
//! The PluginHost is the host-side runtime that manages all communication with
//! a running plugin process. It handles:
//!
//! - HELLO handshake and limit negotiation
//! - Sending cap requests
//! - Receiving and routing responses
//! - Heartbeat handling (transparent)
//! - Multiplexed concurrent requests (transparent)
//!
//! **This is the ONLY way for the host to communicate with plugins.**
//! No fallbacks, no alternative protocols.
//!
//! # Usage
//!
//! The host creates a PluginHost, then calls `request()` to invoke caps.
//! Responses arrive via a channel - the caller just iterates over chunks.
//! Multiple requests can be in flight simultaneously; the runtime handles
//! all correlation and routing.
//!
//! ```ignore
//! use capns::plugin_host::PluginHost;
//! use std::process::{Command, Stdio};
//!
//! let mut child = Command::new("./my-plugin")
//!     .stdin(Stdio::piped())
//!     .stdout(Stdio::piped())
//!     .spawn()?;
//!
//! let stdin = child.stdin.take().unwrap();
//! let stdout = child.stdout.take().unwrap();
//!
//! let host = PluginHost::new(stdin, stdout)?;
//!
//! // Send request - returns receiver for responses
//! let receiver = host.request("cap:op=test", b"payload")?;
//!
//! // Iterate over response chunks (blocks until complete)
//! for chunk in receiver {
//!     match chunk {
//!         Ok(data) => println!("Got chunk: {} bytes", data.len()),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! ```

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake, CborError, FrameReader, FrameWriter};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// Errors that can occur in the plugin host
#[derive(Debug, Clone, thiserror::Error)]
pub enum HostError {
    #[error("CBOR error: {0}")]
    Cbor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Plugin returned error: [{code}] {message}")]
    PluginError { code: String, message: String },

    #[error("Unexpected frame type: {0:?}")]
    UnexpectedFrameType(FrameType),

    #[error("Plugin process exited unexpectedly")]
    ProcessExited,

    #[error("Handshake failed: {0}")]
    Handshake(String),

    #[error("Host is closed")]
    Closed,
}

impl From<CborError> for HostError {
    fn from(e: CborError) -> Self {
        HostError::Cbor(e.to_string())
    }
}

impl From<std::io::Error> for HostError {
    fn from(e: std::io::Error) -> Self {
        HostError::Io(e.to_string())
    }
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
            PluginResponse::Streaming(chunks) => chunks.last().map(|c| c.payload.as_slice()),
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

/// Internal shared state for the plugin host
struct HostState {
    /// Pending requests waiting for responses
    pending: HashMap<MessageId, Sender<Result<ResponseChunk, HostError>>>,
    /// Whether the host is closed
    closed: bool,
}

/// Host-side runtime for communicating with a plugin process.
///
/// **This is the ONLY way for the host to communicate with plugins.**
///
/// The runtime handles:
/// - Multiplexed concurrent requests (transparent)
/// - Heartbeat handling (transparent)
/// - Request/response correlation (transparent)
///
/// Callers simply send requests and receive responses via channels.
pub struct PluginHost<W: Write + Send + 'static> {
    /// Writer for sending frames (protected by mutex for thread safety)
    writer: Arc<Mutex<FrameWriter<W>>>,
    /// Shared state for request tracking
    state: Arc<Mutex<HostState>>,
    /// Negotiated protocol limits
    limits: Limits,
    /// Background reader thread handle
    reader_handle: Option<JoinHandle<()>>,
}

impl<W: Write + Send + 'static> PluginHost<W> {
    /// Create a new plugin host and perform handshake.
    ///
    /// This sends a HELLO frame, waits for the plugin's HELLO,
    /// negotiates protocol limits, then starts the background reader.
    pub fn new<R: Read + Send + 'static>(stdin: W, stdout: R) -> Result<Self, HostError> {
        let mut writer = FrameWriter::new(stdin);
        let mut reader = FrameReader::new(stdout);

        // Perform handshake
        let limits = handshake(&mut reader, &mut writer)?;
        reader.set_limits(limits);
        writer.set_limits(limits);

        let writer = Arc::new(Mutex::new(writer));
        let state = Arc::new(Mutex::new(HostState {
            pending: HashMap::new(),
            closed: false,
        }));

        // Start background reader thread
        let reader_handle = {
            let writer_clone = Arc::clone(&writer);
            let state_clone = Arc::clone(&state);

            thread::spawn(move || {
                Self::reader_loop(reader, writer_clone, state_clone);
            })
        };

        Ok(Self {
            writer,
            state,
            limits,
            reader_handle: Some(reader_handle),
        })
    }

    /// Background reader loop - reads frames and dispatches to waiting requests.
    /// Handles heartbeats transparently.
    fn reader_loop<R: Read>(
        mut reader: FrameReader<R>,
        writer: Arc<Mutex<FrameWriter<W>>>,
        state: Arc<Mutex<HostState>>,
    ) {
        loop {
            let frame = match reader.read() {
                Ok(Some(f)) => f,
                Ok(None) => {
                    // EOF - plugin closed, notify all pending requests
                    let mut state = state.lock().unwrap();
                    state.closed = true;
                    for (_, sender) in state.pending.drain() {
                        let _ = sender.send(Err(HostError::ProcessExited));
                    }
                    break;
                }
                Err(e) => {
                    // Read error - notify all pending requests
                    let mut state = state.lock().unwrap();
                    state.closed = true;
                    let err = HostError::Cbor(e.to_string());
                    for (_, sender) in state.pending.drain() {
                        let _ = sender.send(Err(err.clone()));
                    }
                    break;
                }
            };

            // Handle heartbeats transparently - respond immediately
            if frame.frame_type == FrameType::Heartbeat {
                let response = Frame::heartbeat(frame.id.clone());
                if let Ok(mut w) = writer.lock() {
                    let _ = w.write(&response);
                }
                continue;
            }

            // Route frame to the appropriate pending request
            let request_id = frame.id.clone();
            let should_remove = {
                let state_guard = state.lock().unwrap();
                if let Some(sender) = state_guard.pending.get(&frame.id) {
                    let remove = match frame.frame_type {
                        FrameType::Chunk => {
                            let is_eof = frame.is_eof();
                            let chunk = ResponseChunk {
                                payload: frame.payload.unwrap_or_default(),
                                seq: frame.seq,
                                offset: frame.offset,
                                len: frame.len,
                                is_eof,
                            };
                            let _ = sender.send(Ok(chunk));
                            is_eof // Remove if final chunk
                        }
                        FrameType::Res => {
                            // Single complete response - send as final chunk
                            let chunk = ResponseChunk {
                                payload: frame.payload.unwrap_or_default(),
                                seq: 0,
                                offset: None,
                                len: None,
                                is_eof: true,
                            };
                            let _ = sender.send(Ok(chunk));
                            true // Remove - response complete
                        }
                        FrameType::End => {
                            // Stream end - send final payload if any
                            if let Some(payload) = frame.payload {
                                let chunk = ResponseChunk {
                                    payload,
                                    seq: frame.seq,
                                    offset: frame.offset,
                                    len: frame.len,
                                    is_eof: true,
                                };
                                let _ = sender.send(Ok(chunk));
                            }
                            true // Remove - stream complete
                        }
                        FrameType::Log => {
                            // Log frames don't produce response chunks, skip
                            false
                        }
                        FrameType::Err => {
                            let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                            let message = frame.error_message().unwrap_or("Unknown error").to_string();
                            let _ = sender.send(Err(HostError::PluginError { code, message }));
                            true // Remove - error terminates request
                        }
                        FrameType::Hello | FrameType::Req | FrameType::Heartbeat => {
                            // Protocol errors - Heartbeat is handled above, these should not happen
                            let _ = sender.send(Err(HostError::UnexpectedFrameType(frame.frame_type)));
                            true // Remove - error terminates request
                        }
                    };
                    remove
                } else {
                    false
                }
            };

            // Remove completed request outside the lock scope
            if should_remove {
                let mut state_guard = state.lock().unwrap();
                state_guard.pending.remove(&request_id);
            }
        }
    }

    /// Send a cap request and receive responses via a channel.
    ///
    /// Returns a receiver that yields response chunks. Iterate over it
    /// to receive all chunks until completion.
    ///
    /// Multiple requests can be sent concurrently - each gets its own
    /// response channel. The runtime handles all multiplexing and
    /// heartbeats transparently.
    pub fn request(
        &self,
        cap_urn: &str,
        payload: &[u8],
    ) -> Result<Receiver<Result<ResponseChunk, HostError>>, HostError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(HostError::Closed);
        }

        let request_id = MessageId::new_uuid();
        let request = Frame::req(
            request_id.clone(),
            cap_urn,
            payload.to_vec(),
            "application/json",
        );

        // Create channel for responses
        let (sender, receiver) = mpsc::channel();
        state.pending.insert(request_id, sender);
        drop(state);

        // Send request
        let mut writer = self.writer.lock().unwrap();
        writer.write(&request)?;
        drop(writer);

        Ok(receiver)
    }

    /// Send a cap request and wait for the complete response.
    ///
    /// This is a convenience method that collects all chunks.
    /// For streaming responses, use `request()` directly.
    pub fn call(&self, cap_urn: &str, payload: &[u8]) -> Result<PluginResponse, HostError> {
        let receiver = self.request(cap_urn, payload)?;

        let mut chunks = Vec::new();
        for result in receiver {
            let chunk = result?;
            let is_eof = chunk.is_eof;
            chunks.push(chunk);
            if is_eof {
                break;
            }
        }

        if chunks.len() == 1 && chunks[0].seq == 0 {
            Ok(PluginResponse::Single(chunks.into_iter().next().unwrap().payload))
        } else {
            Ok(PluginResponse::Streaming(chunks))
        }
    }

    /// Send a heartbeat to the plugin and wait for response.
    pub fn send_heartbeat(&self) -> Result<(), HostError> {
        let state = self.state.lock().unwrap();
        if state.closed {
            return Err(HostError::Closed);
        }
        drop(state);

        let heartbeat_id = MessageId::new_uuid();
        let heartbeat = Frame::heartbeat(heartbeat_id);

        let mut writer = self.writer.lock().unwrap();
        writer.write(&heartbeat)?;
        drop(writer);

        // Note: Heartbeat response is handled by reader loop, we don't wait for it
        Ok(())
    }

    /// Get the negotiated protocol limits
    pub fn limits(&self) -> Limits {
        self.limits
    }
}

impl<W: Write + Send + 'static> Drop for PluginHost<W> {
    fn drop(&mut self) {
        // Mark as closed
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }

        // Wait for reader thread to finish
        if let Some(handle) = self.reader_handle.take() {
            let _ = handle.join();
        }
    }
}

// Legacy compatibility - these are used by the integration tests
// In production, use PluginHost::request() which handles everything

/// Streaming response iterator - wraps the channel receiver
pub struct StreamingResponse<'a, W: Write + Send + 'static> {
    receiver: Receiver<Result<ResponseChunk, HostError>>,
    finished: bool,
    _phantom: std::marker::PhantomData<&'a W>,
}

impl<'a, W: Write + Send + 'static> StreamingResponse<'a, W> {
    /// Get the next chunk, or None if complete
    pub fn next_chunk(&mut self) -> Result<Option<ResponseChunk>, HostError> {
        if self.finished {
            return Ok(None);
        }

        match self.receiver.recv() {
            Ok(Ok(chunk)) => {
                if chunk.is_eof {
                    self.finished = true;
                }
                Ok(Some(chunk))
            }
            Ok(Err(e)) => {
                self.finished = true;
                Err(e)
            }
            Err(_) => {
                self.finished = true;
                Ok(None)
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl<'a, W: Write + Send + 'static> Iterator for StreamingResponse<'a, W> {
    type Item = Result<ResponseChunk, HostError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<W: Write + Send + 'static> PluginHost<W> {
    /// Send a cap request and return a streaming iterator.
    ///
    /// This wraps `request()` in an iterator interface for convenience.
    pub fn call_streaming(
        &self,
        cap_urn: &str,
        payload: &[u8],
    ) -> Result<StreamingResponse<'_, W>, HostError> {
        let receiver = self.request(cap_urn, payload)?;
        Ok(StreamingResponse {
            receiver,
            finished: false,
            _phantom: std::marker::PhantomData,
        })
    }
}

// Keep the old FrameWriter/FrameReader accessors for now, but they should not be used
impl<W: Write + Send + 'static> PluginHost<W> {
    /// Get mutable access to the frame writer (internal use only)
    pub fn writer_mut(&self) -> std::sync::MutexGuard<'_, FrameWriter<W>> {
        self.writer.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor_io::encode_frame;

    fn create_hello_response() -> Vec<u8> {
        let limits = Limits::default();
        let frame = Frame::hello(limits.max_frame, limits.max_chunk);
        let cbor = encode_frame(&frame).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
        buf.extend_from_slice(&cbor);
        buf
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
