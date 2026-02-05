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
use std::collections::{HashMap, HashSet};
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

    #[error("Unknown plugin request: {0}")]
    UnknownPluginRequest(String),
}

/// A request from the plugin to invoke a cap on the host.
///
/// When a plugin needs to perform an operation that requires host capabilities
/// (e.g., downloading a model, accessing the network), it sends a REQ frame to the host.
/// The host receives this as a `PluginCapRequest` and should process it accordingly.
#[derive(Debug, Clone)]
pub struct PluginCapRequest {
    /// The message ID for correlating responses
    pub id: MessageId,
    /// The cap URN being requested
    pub cap_urn: String,
    /// The request payload
    pub payload: Vec<u8>,
    /// Content type of the payload
    pub content_type: Option<String>,
}

/// Handler trait for processing plugin cap requests.
///
/// Implement this trait to handle caps that plugins invoke on the host.
/// The handler receives requests and can send streaming responses back.
pub trait PluginCapHandler: Send + Sync {
    /// Handle a cap request from the plugin.
    ///
    /// Returns a channel receiver that yields response chunks.
    /// The handler should process the request and send responses via the sender.
    fn handle(&self, request: PluginCapRequest) -> Receiver<Result<ResponseChunk, HostError>>;
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
    /// Pending requests waiting for responses (host -> plugin)
    pending: HashMap<MessageId, Sender<Result<ResponseChunk, HostError>>>,
    /// Pending heartbeat IDs we've sent (to avoid responding to our own heartbeat responses)
    pending_heartbeats: HashSet<MessageId>,
    /// Whether the host is closed
    closed: bool,
    /// Handler for plugin cap requests (plugin -> host)
    cap_handler: Option<Arc<dyn PluginCapHandler>>,
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
    /// Plugin manifest extracted from HELLO response.
    /// This is JSON-encoded plugin metadata including name, version, and caps.
    plugin_manifest: Vec<u8>,
    /// Background reader thread handle
    reader_handle: Option<JoinHandle<()>>,
}

impl<W: Write + Send + 'static> PluginHost<W> {
    /// Create a new plugin host and perform handshake.
    ///
    /// This sends a HELLO frame, waits for the plugin's HELLO (which MUST include manifest),
    /// negotiates protocol limits, then starts the background reader.
    ///
    /// Fails if the plugin's HELLO is missing the required manifest.
    pub fn new<R: Read + Send + 'static>(stdin: W, stdout: R) -> Result<Self, HostError> {
        let mut writer = FrameWriter::new(stdin);
        let mut reader = FrameReader::new(stdout);

        // Perform handshake - requires plugin to send manifest
        let handshake_result = handshake(&mut reader, &mut writer)?;
        let limits = handshake_result.limits;
        let plugin_manifest = handshake_result.manifest;

        reader.set_limits(limits);
        writer.set_limits(limits);

        let writer = Arc::new(Mutex::new(writer));
        let state = Arc::new(Mutex::new(HostState {
            pending: HashMap::new(),
            pending_heartbeats: HashSet::new(),
            closed: false,
            cap_handler: None,
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
            plugin_manifest,
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

            // Handle heartbeats transparently - before ID check
            if frame.frame_type == FrameType::Heartbeat {
                // Check if this is a response to a heartbeat we sent
                let is_our_heartbeat = {
                    let mut state_guard = state.lock().unwrap();
                    state_guard.pending_heartbeats.remove(&frame.id)
                };

                if is_our_heartbeat {
                    // This is a response to our heartbeat - don't respond
                    continue;
                }

                // This is a heartbeat request from the plugin - respond
                let response = Frame::heartbeat(frame.id.clone());
                if let Ok(mut w) = writer.lock() {
                    let _ = w.write(&response);
                }
                continue;
            }

            // Handle plugin-initiated REQ frames (plugin invoking host caps)
            if frame.frame_type == FrameType::Req {
                Self::handle_plugin_request(&frame, &writer, &state);
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
                        FrameType::Hello | FrameType::Heartbeat => {
                            // Protocol errors - Heartbeat is handled above, these should not happen
                            let _ = sender.send(Err(HostError::UnexpectedFrameType(frame.frame_type)));
                            true // Remove - error terminates request
                        }
                        FrameType::Req => {
                            // This shouldn't happen here - REQ from plugin is handled before pending lookup
                            let _ = sender.send(Err(HostError::UnexpectedFrameType(frame.frame_type)));
                            true
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

    /// Handle a REQ frame from the plugin (plugin invoking a cap on the host).
    ///
    /// This is called when the plugin sends a REQ frame to invoke a host capability.
    /// If a cap handler is registered, it delegates to the handler and streams
    /// responses back to the plugin. Otherwise, it sends an error.
    fn handle_plugin_request(
        frame: &Frame,
        writer: &Arc<Mutex<FrameWriter<W>>>,
        state: &Arc<Mutex<HostState>>,
    ) {
        let request_id = frame.id.clone();

        // Extract cap URN from the frame
        let cap_urn = match &frame.cap {
            Some(cap) => cap.clone(),
            None => {
                // Missing cap URN - send error back to plugin
                let err_frame = Frame::err(request_id, "INVALID_REQUEST", "Missing cap URN");
                if let Ok(mut w) = writer.lock() {
                    let _ = w.write(&err_frame);
                }
                return;
            }
        };

        let payload = frame.payload.clone().unwrap_or_default();
        let content_type = frame.content_type.clone();

        // Get the cap handler
        let handler = {
            let state_guard = state.lock().unwrap();
            state_guard.cap_handler.clone()
        };

        let Some(handler) = handler else {
            // No handler registered - send error
            let err_frame = Frame::err(
                request_id,
                "NO_HANDLER",
                "No cap handler registered on host",
            );
            if let Ok(mut w) = writer.lock() {
                let _ = w.write(&err_frame);
            }
            return;
        };

        // Create the request
        let request = PluginCapRequest {
            id: request_id.clone(),
            cap_urn,
            payload,
            content_type,
        };

        // Call the handler and stream responses back
        let writer_clone = Arc::clone(writer);
        thread::spawn(move || {
            let receiver = handler.handle(request);

            let mut seq: u64 = 0;
            for result in receiver {
                match result {
                    Ok(chunk) => {
                        // Send CHUNK or END frame back to plugin
                        if chunk.is_eof {
                            // Final chunk - send as END frame
                            let end_frame = Frame::end(request_id.clone(), Some(chunk.payload));
                            if let Ok(mut w) = writer_clone.lock() {
                                let _ = w.write(&end_frame);
                            }
                        } else {
                            // Intermediate chunk
                            let chunk_frame =
                                Frame::chunk(request_id.clone(), seq, chunk.payload);
                            if let Ok(mut w) = writer_clone.lock() {
                                let _ = w.write(&chunk_frame);
                            }
                            seq += 1;
                        }
                    }
                    Err(e) => {
                        // Error - send ERR frame and stop
                        let err_frame =
                            Frame::err(request_id.clone(), "CAP_INVOCATION_ERROR", &e.to_string());
                        if let Ok(mut w) = writer_clone.lock() {
                            let _ = w.write(&err_frame);
                        }
                        return;
                    }
                }
            }

            // If we get here without sending an END frame, send one with no payload
            // (This handles the case where receiver is empty)
        });
    }

    /// Set the handler for plugin cap requests.
    ///
    /// When the plugin sends REQ frames to invoke caps on the host,
    /// this handler will be called to process them.
    pub fn set_cap_handler(&self, handler: Arc<dyn PluginCapHandler>) {
        let mut state = self.state.lock().unwrap();
        state.cap_handler = Some(handler);
    }

    /// Send a cap request and receive responses via a channel.
    ///
    /// Returns a receiver that yields response chunks. Iterate over it
    /// to receive all chunks until completion.
    ///
    /// Multiple requests can be sent concurrently - each gets its own
    /// response channel. The runtime handles all multiplexing and
    /// heartbeats transparently.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke
    /// * `payload` - Request payload bytes
    /// * `content_type` - Content type of the payload (e.g., "application/json", "application/cbor")
    pub fn request(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
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
            content_type,
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

    /// Send a cap request with unified arguments.
    ///
    /// Serializes the arguments as CBOR: `[{media_urn: string, value: bytes}, ...]`
    /// and sends with content_type "application/cbor".
    ///
    /// The plugin's `extract_effective_payload` will extract the appropriate
    /// argument data based on the cap's input type.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke
    /// * `arguments` - Unified arguments as CapArgumentValue slice
    pub fn request_with_unified_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<Receiver<Result<ResponseChunk, HostError>>, HostError> {
        // Serialize arguments as CBOR array of maps
        // Format: [{media_urn: string, value: bytes}, ...]
        let cbor_args: Vec<ciborium::Value> = arguments
            .iter()
            .map(|arg| {
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
            })
            .collect();

        let cbor_payload = ciborium::Value::Array(cbor_args);
        let mut payload_bytes = Vec::new();
        ciborium::into_writer(&cbor_payload, &mut payload_bytes).map_err(|e| {
            HostError::Cbor(format!("Failed to serialize unified arguments: {}", e))
        })?;

        self.request(cap_urn, &payload_bytes, "application/cbor")
    }

    /// Send a cap request and wait for the complete response.
    ///
    /// This is a convenience method that collects all chunks.
    /// For streaming responses, use `request()` directly.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke
    /// * `payload` - Request payload bytes
    /// * `content_type` - Content type of the payload
    pub fn call(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<PluginResponse, HostError> {
        let receiver = self.request(cap_urn, payload, content_type)?;

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

    /// Send a heartbeat to the plugin.
    ///
    /// Note: Heartbeat response is handled by reader loop transparently.
    /// This method returns immediately after sending.
    pub fn send_heartbeat(&self) -> Result<(), HostError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(HostError::Closed);
        }

        let heartbeat_id = MessageId::new_uuid();

        // Track this heartbeat so we don't respond to the response
        state.pending_heartbeats.insert(heartbeat_id.clone());
        drop(state);

        let heartbeat = Frame::heartbeat(heartbeat_id.clone());

        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write(&heartbeat) {
            // Remove from tracking on failure
            let mut state = self.state.lock().unwrap();
            state.pending_heartbeats.remove(&heartbeat_id);
            return Err(e.into());
        }
        drop(writer);

        Ok(())
    }

    /// Get the negotiated protocol limits
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Get the plugin manifest extracted from HELLO handshake.
    /// This is JSON-encoded plugin metadata including name, version, and caps.
    pub fn plugin_manifest(&self) -> &[u8] {
        &self.plugin_manifest
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
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN to invoke
    /// * `payload` - Request payload bytes
    /// * `content_type` - Content type of the payload
    pub fn call_streaming(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<StreamingResponse<'_, W>, HostError> {
        let receiver = self.request(cap_urn, payload, content_type)?;
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
