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

    #[error("Protocol violation: Stream ID '{0}' already exists for request")]
    DuplicateStreamId(String),

    #[error("Protocol violation: Chunk for unknown stream ID '{0}'")]
    UnknownStreamId(String),

    #[error("Protocol violation: Chunk received for ended stream ID '{0}'")]
    ChunkAfterStreamEnd(String),

    #[error("Protocol violation: Stream activity after request END")]
    StreamAfterRequestEnd,

    #[error("Protocol violation: StreamStart missing stream_id")]
    StreamStartMissingId,

    #[error("Protocol violation: StreamStart missing media_urn")]
    StreamStartMissingUrn,

    #[error("Protocol violation: Chunk missing stream_id")]
    ChunkMissingStreamId,

    #[error("Protocol violation: {0}")]
    Protocol(String),

    #[error("Receive error: channel closed")]
    RecvError,

    #[error("Peer invoke not supported for cap: {0}")]
    PeerInvokeNotSupported(String),

    #[error("No handler found for cap: {0}")]
    NoHandler(String),
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

/// Stream tracking state for multiplexed streaming
#[derive(Debug, Clone)]
struct StreamState {
    media_urn: String,
    active: bool,  // false after StreamEnd
}

/// Per-request state tracking (host -> plugin responses)
struct RequestState {
    sender: mpsc::UnboundedSender<Result<ResponseChunk, AsyncHostError>>,
    streams: HashMap<String, StreamState>,  // stream_id -> state
    ended: bool,  // true after END frame - any stream activity after is FATAL
}

/// Internal shared state for the async plugin host
struct HostState {
    /// Pending requests with stream tracking (host -> plugin)
    pending: HashMap<MessageId, RequestState>,
    /// Active peer invoke request handles (plugin -> host)
    peer_handles: HashMap<MessageId, Box<dyn crate::cap_router::PeerRequestHandle>>,
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
    /// Router for handling peer invoke requests from plugin
    router: crate::cap_router::ArcCapRouter,
}

impl AsyncPluginHost {
    /// Create a new async plugin host with NO peer invoke support.
    ///
    /// This is a convenience constructor that uses NoPeerRouter.
    /// Use `new_with_router()` if you need peer invoke support.
    ///
    /// This sends a HELLO frame, waits for the plugin's HELLO (which MUST include manifest),
    /// negotiates protocol limits, then starts the background reader and writer tasks.
    pub async fn new<R, W>(stdin: W, stdout: R) -> Result<Self, AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::new_with_router(stdin, stdout, Arc::new(crate::cap_router::NoPeerRouter)).await
    }

    /// Create a new async plugin host with a custom router for peer invoke requests.
    ///
    /// The router will be called when the plugin sends REQ frames (peer invoke).
    /// Use NoPeerRouter to disable peer invoke, or PluginRepoRouter for automatic
    /// plugin discovery and spawning.
    ///
    /// # Arguments
    /// * `stdin` - Plugin's stdin for writing frames
    /// * `stdout` - Plugin's stdout for reading frames
    /// * `router` - Router for handling peer invoke requests
    ///
    /// # Returns
    /// AsyncPluginHost ready to communicate with the plugin
    pub async fn new_with_router<R, W>(
        stdin: W,
        stdout: R,
        router: crate::cap_router::ArcCapRouter,
    ) -> Result<Self, AsyncHostError>
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
            peer_handles: HashMap::new(),
            pending_heartbeats: HashSet::new(),
            closed: false,
        }));

        // Start writer task
        let writer_handle = tokio::spawn(Self::writer_loop(writer, writer_rx));

        // Start reader task
        let reader_handle = {
            let state_clone = Arc::clone(&state);
            let writer_tx_clone = writer_tx.clone();
            let router_clone = Arc::clone(&router);
            tokio::spawn(Self::reader_loop(
                reader,
                state_clone,
                writer_tx_clone,
                router_clone,
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
            router,
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
        router: crate::cap_router::ArcCapRouter,
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
                            for (id, req_state) in state.pending.drain() {
                                eprintln!("[AsyncPluginHost] Reader loop: sending ProcessExited to request {:?}", id);
                                let _ = req_state.sender.send(Err(AsyncHostError::ProcessExited));
                            }
                            break;
                        }
                        Err(e) => {
                            // Read error
                            eprintln!("[AsyncPluginHost] Reader loop: read error: {}", e);
                            let mut state = state.lock().await;
                            state.closed = true;
                            let err = AsyncHostError::Cbor(e.to_string());
                            for (_, req_state) in state.pending.drain() {
                                let _ = req_state.sender.send(Err(err.clone()));
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

                    // Handle peer invoke REQ frames from plugin
                    if frame.frame_type == FrameType::Req {
                        eprintln!("[AsyncPluginHost] Reader loop: received peer invoke REQ from plugin");

                        // Extract cap URN from frame
                        let cap_urn = match frame.cap.as_ref() {
                            Some(urn) => urn.clone(),
                            None => {
                                eprintln!("[AsyncPluginHost] Reader loop: REQ missing cap");
                                let err_frame = Frame::err(
                                    frame.id.clone(),
                                    "MISSING_CAP",
                                    "Peer invoke REQ must include cap URN",
                                );
                                let _ = writer_tx.send(WriterCommand::WriteFrame(err_frame)).await;
                                continue;
                            }
                        };

                        // Delegate to router - get handle for forwarding frames
                        eprintln!("[AsyncPluginHost] Delegating peer request to router: req_id={} cap={}",
                            frame.id.to_uuid_string().unwrap_or_else(|| format!("{:?}", frame.id)), cap_urn);

                        // Extract UUID bytes from MessageId
                        let req_id_bytes = match &frame.id {
                            crate::cbor_frame::MessageId::Uuid(bytes) => *bytes,
                            _ => {
                                eprintln!("[AsyncPluginHost] Peer invoke requires UUID message ID");
                                let err_frame = Frame::err(
                                    frame.id.clone(),
                                    "INVALID_REQ_ID",
                                    "Peer invoke requires UUID message ID",
                                );
                                let _ = writer_tx.send(WriterCommand::WriteFrame(err_frame)).await;
                                continue;
                            }
                        };

                        match router.begin_request(&cap_urn, &req_id_bytes) {
                            Ok(handle) => {
                                // Store handle for forwarding subsequent frames
                                let mut state_guard = state.lock().await;
                                state_guard.peer_handles.insert(frame.id.clone(), handle);
                            }
                            Err(e) => {
                                eprintln!("[AsyncPluginHost] Router rejected request: {}", e);
                                let err_frame = Frame::err(
                                    frame.id.clone(),
                                    "ROUTER_ERROR",
                                    &format!("{}", e),
                                );
                                let _ = writer_tx.send(WriterCommand::WriteFrame(err_frame)).await;
                            }
                        }

                        continue;
                    }

                    // Route frame to appropriate pending request with STRICT stream tracking
                    let request_id = frame.id.clone();
                    let frame_id_str = frame.id.to_uuid_string().unwrap_or_else(|| format!("{:?}", frame.id));
                    let should_remove = {
                        let mut state_guard = state.lock().await;

                        // Check if this is for an active peer request handle
                        if let Some(mut handle) = state_guard.peer_handles.remove(&frame.id) {
                            // Forward frame to router handle
                            eprintln!("[AsyncPluginHost] Forwarding frame to peer handle: type={:?}", frame.frame_type);
                            handle.forward_frame(frame.clone());

                            // If END frame, spawn task to forward responses back to plugin
                            let should_remove = frame.frame_type == FrameType::End;
                            if should_remove {
                                let peer_req_id = frame.id.clone();
                                let response_rx = handle.response_receiver();
                                let writer_tx_clone = writer_tx.clone();

                                // Spawn thread to handle blocking crossbeam receiver
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(async move {
                                        let stream_id = uuid::Uuid::new_v4().to_string();
                                        let mut stream_started = false;
                                        let mut seq = 0u64;

                                        // Read responses and forward to plugin
                                        for chunk_result in response_rx.iter() {
                                            match chunk_result {
                                                Ok(chunk) => {
                                                    if !stream_started {
                                                        let start_frame = Frame::stream_start(
                                                            peer_req_id.clone(),
                                                            stream_id.clone(),
                                                            String::from("media:bytes"),
                                                        );
                                                        let _ = writer_tx_clone.send(WriterCommand::WriteFrame(start_frame)).await;
                                                        stream_started = true;
                                                    }

                                                    let chunk_frame = Frame::chunk(peer_req_id.clone(), stream_id.clone(), seq, chunk.payload);
                                                    let _ = writer_tx_clone.send(WriterCommand::WriteFrame(chunk_frame)).await;
                                                    seq += 1;
                                                }
                                                Err(e) => {
                                                    let err_frame = Frame::err(peer_req_id.clone(), "PEER_ERROR", &format!("{:?}", e));
                                                    let _ = writer_tx_clone.send(WriterCommand::WriteFrame(err_frame)).await;
                                                    break;
                                                }
                                            }
                                        }

                                        if stream_started {
                                            let stream_end_frame = Frame::stream_end(peer_req_id.clone(), stream_id.clone());
                                            let _ = writer_tx_clone.send(WriterCommand::WriteFrame(stream_end_frame)).await;
                                        }

                                        let end_frame = Frame::end(peer_req_id.clone(), None);
                                        let _ = writer_tx_clone.send(WriterCommand::WriteFrame(end_frame)).await;
                                    })
                                });
                            } else {
                                // Not END frame - put handle back
                                state_guard.peer_handles.insert(frame.id.clone(), handle);
                            }

                            should_remove
                        } else {
                            // Not a peer request - check outgoing requests
                            let pending_ids: Vec<_> = state_guard.pending.keys()
                                .map(|k| k.to_uuid_string().unwrap_or_else(|| format!("{:?}", k)))
                                .collect();
                            eprintln!("[AsyncPluginHost] Reader loop: looking for request_id={} in pending=[{}]", frame_id_str, pending_ids.join(", "));

                            if let Some(req_state) = state_guard.pending.get_mut(&frame.id) {
                            eprintln!("[AsyncPluginHost] Reader loop: found matching request, routing frame");

                            let remove = match frame.frame_type {
                                FrameType::Chunk => {
                                    // STRICT: Validate chunk has stream_id and stream is active
                                    let stream_id = match frame.stream_id.as_ref() {
                                        Some(id) => id,
                                        None => {
                                            let _ = req_state.sender.send(Err(AsyncHostError::ChunkMissingStreamId));
                                            return; // Fatal: malformed chunk
                                        }
                                    };

                                    // FAIL HARD: Request already ended
                                    if req_state.ended {
                                        let _ = req_state.sender.send(Err(AsyncHostError::StreamAfterRequestEnd));
                                        return; // Fatal: chunk after END
                                    }

                                    // FAIL HARD: Unknown or inactive stream
                                    match req_state.streams.get(stream_id) {
                                        Some(stream_state) if stream_state.active => {
                                            // ✅ Valid chunk for active stream
                                            let is_eof = frame.is_eof();
                                            let chunk = ResponseChunk {
                                                payload: frame.payload.unwrap_or_default(),
                                                seq: frame.seq,
                                                offset: frame.offset,
                                                len: frame.len,
                                                is_eof,
                                            };
                                            let _ = req_state.sender.send(Ok(chunk));
                                            is_eof
                                        }
                                        Some(_stream_state) => {
                                            // FAIL HARD: Chunk for ended stream
                                            let _ = req_state.sender.send(Err(AsyncHostError::ChunkAfterStreamEnd(stream_id.clone())));
                                            return; // Fatal: chunk after StreamEnd
                                        }
                                        None => {
                                            // FAIL HARD: Unknown stream
                                            let _ = req_state.sender.send(Err(AsyncHostError::UnknownStreamId(stream_id.clone())));
                                            return; // Fatal: unknown stream
                                        }
                                    }
                                }
                                FrameType::End => {
                                    // STRICT: Mark request as ended - any stream activity after is FATAL
                                    req_state.ended = true;
                                    if let Some(payload) = frame.payload {
                                        let chunk = ResponseChunk {
                                            payload,
                                            seq: frame.seq,
                                            offset: frame.offset,
                                            len: frame.len,
                                            is_eof: true,
                                        };
                                        let _ = req_state.sender.send(Ok(chunk));
                                    }
                                    eprintln!("[AsyncPluginHost] Reader loop: Request ended, {} streams tracked", req_state.streams.len());
                                    true
                                }
                                FrameType::Log => false,
                                FrameType::Err => {
                                    let code = frame.error_code().unwrap_or("UNKNOWN").to_string();
                                    let message = frame.error_message().unwrap_or("Unknown error").to_string();
                                    let _ = req_state.sender.send(Err(AsyncHostError::PluginError { code, message }));
                                    req_state.ended = true;
                                    true
                                }
                                FrameType::StreamStart => {
                                    // STRICT: Track new stream, FAIL HARD on violations
                                    let stream_id = match frame.stream_id.as_ref() {
                                        Some(id) => id.clone(),
                                        None => {
                                            let _ = req_state.sender.send(Err(AsyncHostError::StreamStartMissingId));
                                            return; // Fatal: malformed StreamStart
                                        }
                                    };
                                    let media_urn = match frame.media_urn.as_ref() {
                                        Some(urn) => urn.clone(),
                                        None => {
                                            let _ = req_state.sender.send(Err(AsyncHostError::StreamStartMissingUrn));
                                            return; // Fatal: malformed StreamStart
                                        }
                                    };

                                    // FAIL HARD: Request already ended
                                    if req_state.ended {
                                        let _ = req_state.sender.send(Err(AsyncHostError::StreamAfterRequestEnd));
                                        return; // Fatal: stream after END
                                    }

                                    // FAIL HARD: Duplicate stream ID
                                    if req_state.streams.contains_key(&stream_id) {
                                        let _ = req_state.sender.send(Err(AsyncHostError::DuplicateStreamId(stream_id)));
                                        return; // Fatal: duplicate stream
                                    }

                                    // ✅ Track new stream
                                    req_state.streams.insert(stream_id.clone(), StreamState {
                                        media_urn: media_urn.clone(),
                                        active: true,
                                    });
                                    eprintln!("[AsyncPluginHost] Reader loop: StreamStart tracked stream_id={} media_urn={} (total={})",
                                        stream_id, media_urn, req_state.streams.len());
                                    false
                                }
                                FrameType::StreamEnd => {
                                    // STRICT: Mark stream as ended, FAIL HARD on violations
                                    let stream_id = match frame.stream_id.as_ref() {
                                        Some(id) => id.clone(),
                                        None => {
                                            let _ = req_state.sender.send(Err(AsyncHostError::Protocol("StreamEnd missing stream_id".to_string())));
                                            return; // Fatal: malformed StreamEnd
                                        }
                                    };

                                    // FAIL HARD: Unknown stream
                                    match req_state.streams.get_mut(&stream_id) {
                                        Some(stream_state) => {
                                            stream_state.active = false;
                                            eprintln!("[AsyncPluginHost] Reader loop: StreamEnd marked stream_id={} as ended", stream_id);
                                        }
                                        None => {
                                            let _ = req_state.sender.send(Err(AsyncHostError::UnknownStreamId(stream_id)));
                                            return; // Fatal: StreamEnd for unknown stream
                                        }
                                    }
                                    false
                                }
                                _ => {
                                    let _ = req_state.sender.send(Err(AsyncHostError::UnexpectedFrameType(frame.frame_type)));
                                    true
                                }
                            };
                            remove
                            } else {
                                eprintln!("[AsyncPluginHost] Reader loop: NO MATCH for request_id={}, frame dropped!", frame_id_str);
                                false
                            }
                        }
                    };

                    // Remove completed request (either outgoing or peer request)
                    if should_remove {
                        let mut state_guard = state.lock().await;
                        state_guard.pending.remove(&request_id);
                        state_guard.peer_handles.remove(&request_id);
                    }
                }
            }
        }
    }


    /// Parse CapArgumentValue array from CBOR payload.
    ///
    /// The payload should be a CBOR array of maps with "media_urn" and "value" fields.
    fn parse_arguments_from_cbor(payload: &[u8]) -> Result<Vec<crate::CapArgumentValue>, AsyncHostError> {
        let cbor_value: ciborium::Value = ciborium::from_reader(payload)
            .map_err(|e| AsyncHostError::Protocol(format!("Failed to parse CBOR arguments: {}", e)))?;

        let args_array = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => return Err(AsyncHostError::Protocol("Arguments must be a CBOR array".to_string())),
        };

        let mut arguments = Vec::new();
        for arg in args_array {
            if let ciborium::Value::Map(map) = arg {
                let mut media_urn: Option<String> = None;
                let mut value: Option<Vec<u8>> = None;

                for (k, v) in map {
                    if let ciborium::Value::Text(key) = k {
                        match key.as_str() {
                            "media_urn" => {
                                if let ciborium::Value::Text(s) = v {
                                    media_urn = Some(s);
                                }
                            }
                            "value" => {
                                if let ciborium::Value::Bytes(b) = v {
                                    value = Some(b);
                                }
                            }
                            _ => {}
                        }
                    }
                }

                if let (Some(urn), Some(val)) = (media_urn, value) {
                    arguments.push(crate::CapArgumentValue::new(&urn, val));
                }
            }
        }

        Ok(arguments)
    }

    /// Send a cap request with multiple argument streams.
    ///
    /// NEW PROTOCOL: Each argument becomes an independent stream with its own stream_id.
    /// Streams are sent: STREAM_START → CHUNK(s) → STREAM_END
    /// This allows multiplexed streaming of multiple large arguments.
    pub async fn request_with_arguments(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
    ) -> Result<mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(AsyncHostError::Closed);
        }

        let request_id = MessageId::new_uuid();
        let request_id_str = request_id.to_uuid_string().unwrap_or_else(|| format!("{:?}", request_id));
        eprintln!("[AsyncPluginHost] request_with_arguments: req_id={} cap={} args={}", request_id_str, cap_urn, arguments.len());

        // Create unbounded channel for responses with stream tracking
        let (sender, receiver) = mpsc::unbounded_channel();
        state.pending.insert(request_id.clone(), RequestState {
            sender,
            streams: HashMap::new(),
            ended: false,
        });

        let max_chunk = self.limits.max_chunk;
        drop(state);

        // Send REQ frame (no payload - arguments come as streams)
        let request = Frame::req(request_id.clone(), cap_urn, vec![], "application/cbor");
        self.writer_tx
            .send(WriterCommand::WriteFrame(request))
            .await
            .map_err(|_| AsyncHostError::SendError)?;

        // Send each argument as an independent stream
        for arg in arguments {
            let stream_id = uuid::Uuid::new_v4().to_string();
            eprintln!("[AsyncPluginHost] Starting stream: stream_id={} media_urn={} size={}",
                stream_id, arg.media_urn, arg.value.len());

            // STREAM_START: Announce new stream
            let start_frame = Frame::stream_start(
                request_id.clone(),
                stream_id.clone(),
                arg.media_urn.clone()
            );
            self.writer_tx
                .send(WriterCommand::WriteFrame(start_frame))
                .await
                .map_err(|_| AsyncHostError::SendError)?;

            // CHUNK(s): Send argument data in chunks
            let mut offset = 0;
            let mut seq = 0u64;
            while offset < arg.value.len() {
                let chunk_size = (arg.value.len() - offset).min(max_chunk);
                let chunk_data = arg.value[offset..offset + chunk_size].to_vec();

                let chunk_frame = Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    seq,
                    chunk_data
                );
                self.writer_tx
                    .send(WriterCommand::WriteFrame(chunk_frame))
                    .await
                    .map_err(|_| AsyncHostError::SendError)?;

                offset += chunk_size;
                seq += 1;
            }

            // STREAM_END: Close this stream
            let end_frame = Frame::stream_end(request_id.clone(), stream_id.clone());
            self.writer_tx
                .send(WriterCommand::WriteFrame(end_frame))
                .await
                .map_err(|_| AsyncHostError::SendError)?;

            eprintln!("[AsyncPluginHost] Stream ended: stream_id={} chunks={}", stream_id, seq);
        }

        // END: Close the entire request
        let request_end = Frame::end(request_id.clone(), None);
        self.writer_tx
            .send(WriterCommand::WriteFrame(request_end))
            .await
            .map_err(|_| AsyncHostError::SendError)?;

        eprintln!("[AsyncPluginHost] Request complete: req_id={} streams={}", request_id_str, arguments.len());
        Ok(receiver)
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
