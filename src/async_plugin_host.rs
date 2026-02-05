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
    receiver: mpsc::Receiver<Result<ResponseChunk, AsyncHostError>>,
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
    pending: HashMap<MessageId, mpsc::Sender<Result<ResponseChunk, AsyncHostError>>>,
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
                                let _ = sender.send(Err(AsyncHostError::ProcessExited)).await;
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
                                let _ = sender.send(Err(err.clone())).await;
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
                                    let _ = sender.send(Ok(chunk)).await;
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
                                    let _ = sender.send(Ok(chunk)).await;
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
                                        let _ = sender.send(Ok(chunk)).await;
                                    }
                                    true
                                }
                                FrameType::Log => false,
                                FrameType::Err => {
                                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                                    let _ = sender.send(Err(AsyncHostError::PluginError { code, message })).await;
                                    true
                                }
                                _ => {
                                    let _ = sender.send(Err(AsyncHostError::UnexpectedFrameType(frame.frame_type))).await;
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
    ) -> Result<mpsc::Receiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        let mut state = self.state.lock().await;
        if state.closed {
            eprintln!("[AsyncPluginHost] request: host is closed!");
            return Err(AsyncHostError::Closed);
        }

        let request_id = MessageId::new_uuid();
        let request_id_str = request_id.to_uuid_string().unwrap_or_else(|| format!("{:?}", request_id));
        eprintln!("[AsyncPluginHost] request: generated request_id={} for cap={}", request_id_str, cap_urn);
        let request = Frame::req(
            request_id.clone(),
            cap_urn,
            payload.to_vec(),
            content_type,
        );

        // Create channel for responses
        let (sender, receiver) = mpsc::channel(32);
        state.pending.insert(request_id.clone(), sender);
        eprintln!("[AsyncPluginHost] request: inserted request_id={} into pending (count={})", request_id_str, state.pending.len());
        drop(state);

        // Send request
        self.writer_tx
            .send(WriterCommand::WriteFrame(request))
            .await
            .map_err(|_| AsyncHostError::SendError)?;

        Ok(receiver)
    }

    /// Send a cap request with unified arguments.
    pub async fn request_with_unified_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<mpsc::Receiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
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

    /// Send a cap request with unified arguments and wait for the complete response.
    pub async fn call_with_unified_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<PluginResponse, AsyncHostError> {
        let mut receiver = self.request_with_unified_arguments(cap_urn, arguments).await?;
        Self::collect_response(&mut receiver).await
    }

    /// Collect all response chunks from a receiver into a PluginResponse.
    async fn collect_response(
        receiver: &mut mpsc::Receiver<Result<ResponseChunk, AsyncHostError>>,
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
