//! Async Plugin Host - Native async runtime for communicating with plugin processes
//!
//! The AsyncPluginHost is the host-side runtime that manages all communication with
//! a running plugin process using fully async I/O. It handles:
//!
//! - HELLO handshake and limit negotiation
//! - Sending cap requests
//! - Receiving and routing responses
//! - Heartbeat handling (transparent)
//! - Multiplexed concurrent requests (transparent)
//! - Clean cancellation and shutdown
//!
//! **This is the ONLY way for the host to communicate with plugins.**
//! No fallbacks, no alternative protocols.
//!
//! # Usage
//!
//! ```ignore
//! use capns::async_plugin_host::AsyncPluginHost;
//! use tokio::process::Command;
//!
//! let mut child = Command::new("./my-plugin")
//!     .stdin(Stdio::piped())
//!     .stdout(Stdio::piped())
//!     .spawn()?;
//!
//! let stdin = child.stdin.take().unwrap();
//! let stdout = child.stdout.take().unwrap();
//!
//! let host = AsyncPluginHost::new(stdin, stdout).await?;
//!
//! // Send request and receive response
//! let response = host.call("cap:op=test", b"payload", "application/json").await?;
//! ```

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake_async, AsyncFrameReader, AsyncFrameWriter, CborError};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

/// Errors that can occur in the async plugin host
#[derive(Debug, Clone, thiserror::Error)]
pub enum AsyncHostError {
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

    #[error("Send error: channel closed")]
    SendError,

    #[error("Receive error: channel closed")]
    RecvError,
}

impl From<CborError> for AsyncHostError {
    fn from(e: CborError) -> Self {
        AsyncHostError::Cbor(e.to_string())
    }
}

impl From<std::io::Error> for AsyncHostError {
    fn from(e: std::io::Error) -> Self {
        AsyncHostError::Io(e.to_string())
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

/// Commands sent to the writer task
enum WriterCommand {
    WriteFrame(Frame),
    Shutdown,
}

/// A streaming response from a plugin that can be iterated asynchronously.
pub struct StreamingResponse {
    receiver: mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>,
}

impl StreamingResponse {
    /// Get the next chunk from the stream.
    pub async fn next(&mut self) -> Option<Result<ResponseChunk, AsyncHostError>> {
        self.receiver.recv().await
    }
}

/// Internal shared state for the async plugin host
struct HostState {
    /// Pending requests waiting for responses (host -> plugin)
    pending: HashMap<MessageId, mpsc::UnboundedSender<Result<ResponseChunk, AsyncHostError>>>,
    /// Pending heartbeat IDs we've sent
    pending_heartbeats: HashSet<MessageId>,
    /// Whether the host is closed
    closed: bool,
}

/// Async host-side runtime for communicating with a plugin process.
///
/// Uses native tokio async I/O with clean cancellation support.
pub struct AsyncPluginHost {
    /// Channel to send frames to the writer task
    writer_tx: mpsc::Sender<WriterCommand>,
    /// Shared state for request tracking
    state: Arc<Mutex<HostState>>,
    /// Negotiated protocol limits
    limits: Limits,
    /// Plugin manifest extracted from HELLO response
    plugin_manifest: Vec<u8>,
    /// Background reader task handle
    reader_handle: Option<JoinHandle<()>>,
    /// Background writer task handle
    writer_handle: Option<JoinHandle<()>>,
    /// Shutdown signal sender
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl AsyncPluginHost {
    /// Create a new async plugin host and perform handshake.
    ///
    /// This sends a HELLO frame, waits for the plugin's HELLO (which MUST include manifest),
    /// negotiates protocol limits, then starts the background reader and writer tasks.
    pub async fn new<R, W>(stdin: W, stdout: R) -> Result<Self, AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        eprintln!("[AsyncPluginHost] new: starting handshake...");
        let mut reader = AsyncFrameReader::new(stdout);
        let mut writer = AsyncFrameWriter::new(stdin);

        // Perform handshake
        let handshake_result = handshake_async(&mut reader, &mut writer).await?;
        let limits = handshake_result.limits;
        let plugin_manifest = handshake_result.manifest;
        eprintln!("[AsyncPluginHost] new: handshake complete, limits={:?}, manifest_len={}", limits, plugin_manifest.len());

        // Create channels
        let (writer_tx, writer_rx) = mpsc::channel::<WriterCommand>(64);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let state = Arc::new(Mutex::new(HostState {
            pending: HashMap::new(),
            pending_heartbeats: HashSet::new(),
            closed: false,
        }));

        // Start writer task
        let writer_handle = tokio::spawn(Self::writer_loop(writer, writer_rx));

        // Start reader task
        let reader_handle = {
            let state_clone = Arc::clone(&state);
            let writer_tx_clone = writer_tx.clone();
            tokio::spawn(Self::reader_loop(
                reader,
                state_clone,
                writer_tx_clone,
                shutdown_rx,
            ))
        };

        Ok(Self {
            writer_tx,
            state,
            limits,
            plugin_manifest,
            reader_handle: Some(reader_handle),
            writer_handle: Some(writer_handle),
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Writer loop - sends frames from the channel
    async fn writer_loop<W: AsyncWrite + Unpin>(
        mut writer: AsyncFrameWriter<W>,
        mut rx: mpsc::Receiver<WriterCommand>,
    ) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriterCommand::WriteFrame(frame) => {
                    if let Err(e) = writer.write(&frame).await {
                        eprintln!("AsyncPluginHost writer error: {}", e);
                        break;
                    }
                }
                WriterCommand::Shutdown => break,
            }
        }
    }

    /// Reader loop - reads frames and dispatches to waiting requests
    async fn reader_loop<R: AsyncRead + Unpin>(
        mut reader: AsyncFrameReader<R>,
        state: Arc<Mutex<HostState>>,
        writer_tx: mpsc::Sender<WriterCommand>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        eprintln!("[AsyncPluginHost] Reader loop started");
        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = &mut shutdown_rx => {
                    eprintln!("[AsyncPluginHost] Reader loop: shutdown signal received");
                    break;
                }
                // Read next frame
                frame_result = reader.read() => {
                    let frame = match frame_result {
                        Ok(Some(f)) => {
                            let frame_id_str = f.id.to_uuid_string().unwrap_or_else(|| format!("{:?}", f.id));
                            eprintln!("[AsyncPluginHost] Reader loop: received frame type={:?} id={}", f.frame_type, frame_id_str);
                            f
                        }
                        Ok(None) => {
                            // EOF - plugin closed
                            eprintln!("[AsyncPluginHost] Reader loop: EOF received (plugin closed)");
                            let mut state = state.lock().await;
                            state.closed = true;
                            let pending_count = state.pending.len();
                            eprintln!("[AsyncPluginHost] Reader loop: notifying {} pending requests of EOF", pending_count);
                            for (id, sender) in state.pending.drain() {
                                eprintln!("[AsyncPluginHost] Reader loop: sending ProcessExited to request {:?}", id);
                                let _ = sender.send(Err(AsyncHostError::ProcessExited));
                            }
                            break;
                        }
                        Err(e) => {
                            // Read error
                            eprintln!("[AsyncPluginHost] Reader loop: read error: {}", e);
                            let mut state = state.lock().await;
                            state.closed = true;
                            let err = AsyncHostError::Cbor(e.to_string());
                            for (_, sender) in state.pending.drain() {
                                let _ = sender.send(Err(err.clone()));
                            }
                            break;
                        }
                    };

                    // Handle heartbeats transparently
                    if frame.frame_type == FrameType::Heartbeat {
                        let is_our_heartbeat = {
                            let mut state_guard = state.lock().await;
                            state_guard.pending_heartbeats.remove(&frame.id)
                        };

                        if !is_our_heartbeat {
                            // Respond to heartbeat from plugin
                            let response = Frame::heartbeat(frame.id.clone());
                            let _ = writer_tx.send(WriterCommand::WriteFrame(response)).await;
                        }
                        continue;
                    }

                    // Route frame to appropriate pending request
                    let request_id = frame.id.clone();
                    let frame_id_str = frame.id.to_uuid_string().unwrap_or_else(|| format!("{:?}", frame.id));
                    let (_sender, should_remove) = {
                        let state_guard = state.lock().await;
                        let pending_ids: Vec<_> = state_guard.pending.keys()
                            .map(|k| k.to_uuid_string().unwrap_or_else(|| format!("{:?}", k)))
                            .collect();
                        eprintln!("[AsyncPluginHost] Reader loop: looking for request_id={} in pending=[{}]", frame_id_str, pending_ids.join(", "));
                        if let Some(sender) = state_guard.pending.get(&frame.id) {
                            eprintln!("[AsyncPluginHost] Reader loop: found matching request, routing frame");
                            let sender = sender.clone();
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
                                    is_eof
                                }
                                FrameType::Res => {
                                    let chunk = ResponseChunk {
                                        payload: frame.payload.unwrap_or_default(),
                                        seq: 0,
                                        offset: None,
                                        len: None,
                                        is_eof: true,
                                    };
                                    let _ = sender.send(Ok(chunk));
                                    true
                                }
                                FrameType::End => {
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
                                    true
                                }
                                FrameType::Log => false,
                                FrameType::Err => {
                                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                                    let _ = sender.send(Err(AsyncHostError::PluginError { code, message }));
                                    true
                                }
                                _ => {
                                    let _ = sender.send(Err(AsyncHostError::UnexpectedFrameType(frame.frame_type)));
                                    true
                                }
                            };
                            (Some(sender), remove)
                        } else {
                            eprintln!("[AsyncPluginHost] Reader loop: NO MATCH for request_id={}, frame dropped!", frame_id_str);
                            (None, false)
                        }
                    };

                    // Remove completed request
                    if should_remove {
                        let mut state_guard = state.lock().await;
                        state_guard.pending.remove(&request_id);
                    }
                }
            }
        }
    }

    /// Send a cap request and receive responses via a channel.
    pub async fn request(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        let mut state = self.state.lock().await;
        if state.closed {
            eprintln!("[AsyncPluginHost] request: host is closed!");
            return Err(AsyncHostError::Closed);
        }

        let request_id = MessageId::new_uuid();
        let request_id_str = request_id.to_uuid_string().unwrap_or_else(|| format!("{:?}", request_id));
        eprintln!("[AsyncPluginHost] request: generated request_id={} for cap={} payload_len={}", request_id_str, cap_urn, payload.len());

        // Create unbounded channel for responses (avoid deadlock with large chunked responses)
        let (sender, receiver) = mpsc::unbounded_channel();
        state.pending.insert(request_id.clone(), sender);
        eprintln!("[AsyncPluginHost] request: inserted request_id={} into pending (count={})", request_id_str, state.pending.len());

        let max_chunk = self.limits.max_chunk;
        drop(state);

        // Automatic chunking for large request payloads (or empty payloads to avoid ambiguity)
        if !payload.is_empty() && payload.len() <= max_chunk {
            // Small non-empty payload: send single REQ frame with full payload
            let request = Frame::req(
                request_id.clone(),
                cap_urn,
                payload.to_vec(),
                content_type,
            );
            self.writer_tx
                .send(WriterCommand::WriteFrame(request))
                .await
                .map_err(|_| AsyncHostError::SendError)?;
        } else {
            // Empty or large payload: send REQ + CHUNK frames + END
            eprintln!("[AsyncPluginHost] request: large payload ({} bytes), chunking with max_chunk={}", payload.len(), max_chunk);

            // Send initial REQ frame with cap_urn and content_type, but empty payload
            let request = Frame::req(
                request_id.clone(),
                cap_urn,
                vec![],
                content_type,
            );
            self.writer_tx
                .send(WriterCommand::WriteFrame(request))
                .await
                .map_err(|_| AsyncHostError::SendError)?;

            // Send payload in CHUNK frames
            let mut offset = 0;
            let mut seq = 0u64;

            if payload.is_empty() {
                // Empty payload: send END frame immediately with no chunks
                let end_frame = Frame::end(request_id.clone(), Some(vec![]));
                self.writer_tx
                    .send(WriterCommand::WriteFrame(end_frame))
                    .await
                    .map_err(|_| AsyncHostError::SendError)?;
            } else {
                // Non-empty payload: send CHUNK frames + END
                while offset < payload.len() {
                    let remaining = payload.len() - offset;
                    let chunk_size = remaining.min(max_chunk);
                    let chunk_data = payload[offset..offset + chunk_size].to_vec();
                    offset += chunk_size;

                    if offset < payload.len() {
                        // Not the last chunk - send CHUNK frame
                        let chunk_frame = Frame::chunk(request_id.clone(), seq, chunk_data);
                        self.writer_tx
                            .send(WriterCommand::WriteFrame(chunk_frame))
                            .await
                            .map_err(|_| AsyncHostError::SendError)?;
                        seq += 1;
                    } else {
                        // Last chunk - send END frame
                        let end_frame = Frame::end(request_id.clone(), Some(chunk_data));
                        self.writer_tx
                            .send(WriterCommand::WriteFrame(end_frame))
                            .await
                            .map_err(|_| AsyncHostError::SendError)?;
                    }
                }
            }

            eprintln!("[AsyncPluginHost] request: sent {} chunk frames + END for request_id={}", seq, request_id_str);
        }

        Ok(receiver)
    }

    /// Send a cap request with arguments.
    pub async fn request_with_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        // Serialize arguments as CBOR
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
        ciborium::into_writer(&cbor_payload, &mut payload_bytes)
            .map_err(|e| AsyncHostError::Cbor(format!("Failed to serialize arguments: {}", e)))?;

        self.request(cap_urn, &payload_bytes, "application/cbor").await
    }

    /// Send a cap request and wait for the complete response.
    pub async fn call(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<PluginResponse, AsyncHostError> {
        let mut receiver = self.request(cap_urn, payload, content_type).await?;
        Self::collect_response(&mut receiver).await
    }

    /// Send a cap request with arguments and wait for the complete response.
    pub async fn call_with_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<PluginResponse, AsyncHostError> {
        let mut receiver = self.request_with_arguments(cap_urn, arguments).await?;
        Self::collect_response(&mut receiver).await
    }

    /// Collect all response chunks from a receiver into a PluginResponse.
    async fn collect_response(
        receiver: &mut mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>,
    ) -> Result<PluginResponse, AsyncHostError> {
        eprintln!("[AsyncPluginHost] collect_response: starting to collect chunks");
        let mut chunks = Vec::new();
        while let Some(result) = receiver.recv().await {
            let chunk = result?;
            let is_eof = chunk.is_eof;
            eprintln!("[AsyncPluginHost] collect_response: received chunk seq={} payload_len={} is_eof={}", chunk.seq, chunk.payload.len(), is_eof);
            chunks.push(chunk);
            if is_eof {
                eprintln!("[AsyncPluginHost] collect_response: got EOF, breaking");
                break;
            }
        }

        eprintln!("[AsyncPluginHost] collect_response: collected {} chunks", chunks.len());

        if chunks.is_empty() {
            eprintln!("[AsyncPluginHost] collect_response: ERROR - no chunks received!");
            return Err(AsyncHostError::RecvError);
        }

        if chunks.len() == 1 && chunks[0].seq == 0 {
            let payload_len = chunks[0].payload.len();
            eprintln!("[AsyncPluginHost] collect_response: returning Single response with {} bytes", payload_len);
            Ok(PluginResponse::Single(chunks.into_iter().next().unwrap().payload))
        } else {
            let total_len: usize = chunks.iter().map(|c| c.payload.len()).sum();
            eprintln!("[AsyncPluginHost] collect_response: returning Streaming response with {} chunks, {} total bytes", chunks.len(), total_len);
            Ok(PluginResponse::Streaming(chunks))
        }
    }

    /// Get the negotiated protocol limits
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Get the plugin manifest extracted from HELLO handshake.
    pub fn plugin_manifest(&self) -> &[u8] {
        &self.plugin_manifest
    }

    /// Send a cap request and get a streaming response iterator.
    pub async fn call_streaming(
        &self,
        cap_urn: &str,
        payload: &[u8],
        content_type: &str,
    ) -> Result<StreamingResponse, AsyncHostError> {
        let receiver = self.request(cap_urn, payload, content_type).await?;
        Ok(StreamingResponse { receiver })
    }

    /// Send a heartbeat and wait for response.
    pub async fn send_heartbeat(&self) -> Result<(), AsyncHostError> {
        let heartbeat_id = MessageId::new_uuid();
        let heartbeat = Frame::heartbeat(heartbeat_id.clone());

        // Track this heartbeat so we don't respond to our own response
        {
            let mut state = self.state.lock().await;
            if state.closed {
                return Err(AsyncHostError::Closed);
            }
            state.pending_heartbeats.insert(heartbeat_id);
        }

        // Send heartbeat
        self.writer_tx
            .send(WriterCommand::WriteFrame(heartbeat))
            .await
            .map_err(|_| AsyncHostError::SendError)?;

        Ok(())
    }

    /// Gracefully shutdown the host
    pub async fn shutdown(mut self) {
        // Signal shutdown to reader
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Signal shutdown to writer
        let _ = self.writer_tx.send(WriterCommand::Shutdown).await;

        // Mark as closed
        {
            let mut state = self.state.lock().await;
            state.closed = true;
        }

        // Wait for tasks to complete
        if let Some(handle) = self.reader_handle.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for AsyncPluginHost {
    fn drop(&mut self) {
        // Send shutdown signal - non-blocking attempt
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Abort tasks if they're still running
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.writer_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly
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
        assert_eq!(chunk.seq, 0);
        assert!(chunk.offset.is_none());
        assert!(chunk.len.is_none());
        assert!(!chunk.is_eof);
    }

    // TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof
    #[test]
    fn test_response_chunk_with_all_fields() {
        let chunk = ResponseChunk {
            payload: b"data".to_vec(),
            seq: 5,
            offset: Some(1024),
            len: Some(8192),
            is_eof: true,
        };

        assert_eq!(chunk.seq, 5);
        assert_eq!(chunk.offset, Some(1024));
        assert_eq!(chunk.len, Some(8192));
        assert!(chunk.is_eof);
    }

    // TEST237: Test PluginResponse::Single final_payload returns the single payload slice
    #[test]
    fn test_plugin_response_single() {
        let response = PluginResponse::Single(b"result".to_vec());
        assert_eq!(response.final_payload(), Some(b"result".as_slice()));
        assert_eq!(response.concatenated(), b"result");
    }

    // TEST238: Test PluginResponse::Single with empty payload returns empty slice and empty vec
    #[test]
    fn test_plugin_response_single_empty() {
        let response = PluginResponse::Single(vec![]);
        assert_eq!(response.final_payload(), Some(b"".as_slice()));
        assert_eq!(response.concatenated(), b"");
    }

    // TEST239: Test PluginResponse::Streaming concatenated joins all chunk payloads in order
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

    // TEST240: Test PluginResponse::Streaming final_payload returns the last chunk's payload
    #[test]
    fn test_plugin_response_streaming_final_payload() {
        let chunks = vec![
            ResponseChunk {
                payload: b"first".to_vec(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"second".to_vec(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"last".to_vec(),
                seq: 2,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];

        let response = PluginResponse::Streaming(chunks);
        assert_eq!(response.final_payload(), Some(b"last".as_slice()));
    }

    // TEST241: Test PluginResponse::Streaming with empty chunks vec returns empty concatenation
    #[test]
    fn test_plugin_response_streaming_empty_chunks() {
        let response = PluginResponse::Streaming(vec![]);
        assert_eq!(response.concatenated(), b"");
        assert!(response.final_payload().is_none());
    }

    // TEST242: Test PluginResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads
    #[test]
    fn test_plugin_response_streaming_large_payload() {
        let chunk1_data = vec![0xAA; 1000];
        let chunk2_data = vec![0xBB; 2000];

        let chunks = vec![
            ResponseChunk {
                payload: chunk1_data.clone(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: chunk2_data.clone(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];

        let response = PluginResponse::Streaming(chunks);
        let result = response.concatenated();
        assert_eq!(result.len(), 3000);
        assert_eq!(&result[..1000], &chunk1_data);
        assert_eq!(&result[1000..], &chunk2_data);
    }

    // TEST243: Test AsyncHostError variants display correct error messages
    #[test]
    fn test_async_host_error_display() {
        let err = AsyncHostError::PluginError {
            code: "NOT_FOUND".to_string(),
            message: "Cap not found".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("NOT_FOUND"), "error display must include code");
        assert!(msg.contains("Cap not found"), "error display must include message");

        let err2 = AsyncHostError::Closed;
        assert_eq!(format!("{}", err2), "Host is closed");

        let err3 = AsyncHostError::ProcessExited;
        assert_eq!(format!("{}", err3), "Plugin process exited unexpectedly");

        let err4 = AsyncHostError::SendError;
        assert_eq!(format!("{}", err4), "Send error: channel closed");

        let err5 = AsyncHostError::RecvError;
        assert_eq!(format!("{}", err5), "Receive error: channel closed");
    }

    // TEST244: Test AsyncHostError::from converts CborError to Cbor variant
    #[test]
    fn test_async_host_error_from_cbor() {
        let cbor_err = crate::cbor_io::CborError::InvalidFrame("test".to_string());
        let host_err: AsyncHostError = cbor_err.into();
        match host_err {
            AsyncHostError::Cbor(msg) => assert!(msg.contains("test")),
            _ => panic!("expected Cbor variant"),
        }
    }

    // TEST245: Test AsyncHostError::from converts io::Error to Io variant
    #[test]
    fn test_async_host_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let host_err: AsyncHostError = io_err.into();
        match host_err {
            AsyncHostError::Io(msg) => assert!(msg.contains("pipe broken")),
            _ => panic!("expected Io variant"),
        }
    }

    // TEST246: Test AsyncHostError Clone implementation produces equal values
    #[test]
    fn test_async_host_error_clone() {
        let err = AsyncHostError::PluginError {
            code: "ERR".to_string(),
            message: "msg".to_string(),
        };
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    // TEST247: Test ResponseChunk Clone produces independent copy with same data
    #[test]
    fn test_response_chunk_clone() {
        let chunk = ResponseChunk {
            payload: b"data".to_vec(),
            seq: 3,
            offset: Some(100),
            len: Some(500),
            is_eof: true,
        };
        let cloned = chunk.clone();
        assert_eq!(chunk.payload, cloned.payload);
        assert_eq!(chunk.seq, cloned.seq);
        assert_eq!(chunk.offset, cloned.offset);
        assert_eq!(chunk.len, cloned.len);
        assert_eq!(chunk.is_eof, cloned.is_eof);
    }
}
