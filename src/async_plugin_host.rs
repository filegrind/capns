//! Async Plugin Host Runtime — Multi-plugin management with frame routing
//!
//! The AsyncPluginHost manages multiple plugin binaries, routing CBOR protocol
//! frames between a relay connection (to the engine) and individual plugin processes.
//!
//! ## Architecture
//!
//! ```text
//! Relay (engine) ←→ AsyncPluginHost ←→ Plugin A (stdin/stdout)
//!                                   ←→ Plugin B (stdin/stdout)
//!                                   ←→ Plugin C (stdin/stdout)
//! ```
//!
//! ## Frame Routing
//!
//! Engine → Plugin:
//! - REQ: route by cap_urn to the plugin that handles it, spawn on demand
//! - STREAM_START/CHUNK/STREAM_END/END/ERR: route by req_id to the mapped plugin
//! - All other frame types: hard protocol error (must never arrive from engine)
//!
//! Plugin → Engine:
//! - HELLO: fatal error (consumed during handshake, never during run)
//! - HEARTBEAT: responded to locally, never forwarded
//! - REQ (peer invoke): registered in routing table, forwarded to relay
//! - RelayNotify/RelayState: fatal error (plugins must never send these)
//! - Everything else: forwarded to relay (pass-through)

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{handshake_async, AsyncFrameReader, AsyncFrameWriter, CborError};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// Interval between heartbeat probes sent to each running plugin.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time to wait for a heartbeat response before considering a plugin unhealthy.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// ERROR TYPES
// =============================================================================

/// Errors that can occur in the async plugin host runtime.
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

// =============================================================================
// RESPONSE TYPES (used by engine-side code reading from relay)
// =============================================================================

/// A response chunk from a plugin.
#[derive(Debug, Clone)]
pub struct ResponseChunk {
    pub payload: Vec<u8>,
    pub seq: u64,
    pub offset: Option<u64>,
    pub len: Option<u64>,
    pub is_eof: bool,
}

/// A complete response from a plugin, which may be single or streaming.
#[derive(Debug)]
pub enum PluginResponse {
    Single(Vec<u8>),
    Streaming(Vec<ResponseChunk>),
}

impl PluginResponse {
    pub fn final_payload(&self) -> Option<&[u8]> {
        match self {
            PluginResponse::Single(data) => Some(data),
            PluginResponse::Streaming(chunks) => chunks.last().map(|c| c.payload.as_slice()),
        }
    }

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

/// A streaming response that can be iterated asynchronously.
pub struct StreamingResponse {
    receiver: mpsc::UnboundedReceiver<Result<ResponseChunk, AsyncHostError>>,
}

impl StreamingResponse {
    pub async fn next(&mut self) -> Option<Result<ResponseChunk, AsyncHostError>> {
        self.receiver.recv().await
    }
}

// =============================================================================
// INTERNAL TYPES
// =============================================================================

/// Events from plugin reader loops, delivered to the main run() loop.
enum PluginEvent {
    /// A frame was received from a plugin's stdout.
    Frame { plugin_idx: usize, frame: Frame },
    /// A plugin's reader loop exited (process died or stdout closed).
    Death { plugin_idx: usize },
}

/// A managed plugin binary.
struct ManagedPlugin {
    /// Path to plugin binary (empty for attached/pre-connected plugins).
    path: PathBuf,
    /// Child process handle (None for attached plugins).
    process: Option<tokio::process::Child>,
    /// Channel to write frames to this plugin's stdin.
    writer_tx: Option<mpsc::UnboundedSender<Frame>>,
    /// Plugin manifest from HELLO handshake.
    manifest: Vec<u8>,
    /// Negotiated limits for this plugin.
    limits: Limits,
    /// Cap URNs this plugin handles (from manifest after HELLO).
    caps: Vec<String>,
    /// Known caps from registration (before HELLO, used for routing).
    known_caps: Vec<String>,
    /// Whether the plugin is currently running and healthy.
    running: bool,
    /// Reader task handle.
    reader_handle: Option<JoinHandle<()>>,
    /// Writer task handle.
    writer_handle: Option<JoinHandle<()>>,
    /// Whether HELLO handshake permanently failed (binary is broken, no relaunch).
    hello_failed: bool,
    /// Pending heartbeats sent to this plugin (ID → sent time).
    pending_heartbeats: HashMap<MessageId, Instant>,
}

impl ManagedPlugin {
    fn new_registered(path: PathBuf, known_caps: Vec<String>) -> Self {
        Self {
            path,
            process: None,
            writer_tx: None,
            manifest: Vec::new(),
            limits: Limits::default(),
            caps: Vec::new(),
            known_caps,
            running: false,
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            pending_heartbeats: HashMap::new(),
        }
    }

    fn new_attached(manifest: Vec<u8>, limits: Limits, caps: Vec<String>) -> Self {
        Self {
            path: PathBuf::new(),
            process: None,
            writer_tx: None,
            manifest,
            limits,
            caps: caps.clone(),
            known_caps: caps,
            running: true,
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            pending_heartbeats: HashMap::new(),
        }
    }
}

// =============================================================================
// ASYNC PLUGIN HOST RUNTIME
// =============================================================================

/// Async host-side runtime managing multiple plugin processes.
///
/// Routes CBOR protocol frames between a relay connection (engine) and
/// individual plugin processes. Handles HELLO handshake, heartbeat health
/// monitoring, spawn-on-demand, crash recovery, and capability advertisement.
pub struct AsyncPluginHost {
    /// Managed plugin binaries.
    plugins: Vec<ManagedPlugin>,
    /// Routing: cap_urn → plugin index (for finding which plugin handles a cap).
    cap_table: Vec<(String, usize)>,
    /// Routing: req_id → plugin index (for in-flight request frame correlation).
    request_routing: HashMap<MessageId, usize>,
    /// Request IDs initiated by plugins (peer invokes). These are tracked separately
    /// because when a plugin sends END for a peer invoke, that's the end of the
    /// *outgoing request body*, NOT the final response. The routing entry must survive
    /// until the relay sends back the response (END/ERR).
    peer_requests: HashSet<MessageId>,
    /// Aggregate capabilities (serialized JSON manifest of all plugin caps).
    capabilities: Vec<u8>,
    /// Channel sender for plugin events (shared with reader tasks).
    event_tx: mpsc::UnboundedSender<PluginEvent>,
    /// Channel receiver for plugin events (consumed by run()).
    event_rx: Option<mpsc::UnboundedReceiver<PluginEvent>>,
}

impl AsyncPluginHost {
    /// Create a new plugin host runtime.
    ///
    /// After creation, register plugins with `register_plugin()` or
    /// attach pre-connected plugins with `attach_plugin()`, then call `run()`.
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            plugins: Vec::new(),
            cap_table: Vec::new(),
            request_routing: HashMap::new(),
            peer_requests: HashSet::new(),
            capabilities: Vec::new(),
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    /// Register a plugin binary for on-demand spawning.
    ///
    /// The plugin is not spawned until a REQ arrives for one of its known caps.
    /// The `known_caps` are provisional — they allow routing before HELLO.
    /// After spawn + HELLO, the real caps from the manifest replace them.
    pub fn register_plugin(&mut self, path: &Path, known_caps: &[String]) {
        let plugin_idx = self.plugins.len();
        self.plugins.push(ManagedPlugin::new_registered(
            path.to_path_buf(),
            known_caps.to_vec(),
        ));
        for cap in known_caps {
            self.cap_table.push((cap.clone(), plugin_idx));
        }
    }

    /// Attach a pre-connected plugin (already running, e.g., pre-spawned or in tests).
    ///
    /// Performs HELLO handshake immediately. On success, the plugin is ready for requests.
    /// On HELLO failure, returns error (permanent — the binary is broken).
    pub async fn attach_plugin<R, W>(
        &mut self,
        plugin_read: R,
        plugin_write: W,
    ) -> Result<usize, AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut reader = AsyncFrameReader::new(plugin_read);
        let mut writer = AsyncFrameWriter::new(plugin_write);

        let result = handshake_async(&mut reader, &mut writer)
            .await
            .map_err(|e| AsyncHostError::Handshake(e.to_string()))?;

        let caps = parse_caps_from_manifest(&result.manifest)?;
        let plugin_idx = self.plugins.len();

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        // Start reader task
        let rh = Self::start_reader_task(plugin_idx, reader, self.event_tx.clone());

        let mut plugin = ManagedPlugin::new_attached(result.manifest, result.limits, caps);
        plugin.writer_tx = Some(writer_tx);
        plugin.reader_handle = Some(rh);
        plugin.writer_handle = Some(wh);

        self.plugins.push(plugin);
        self.update_cap_table();
        self.rebuild_capabilities();

        Ok(plugin_idx)
    }

    /// Get the aggregate capabilities of all running, healthy plugins.
    pub fn capabilities(&self) -> &[u8] {
        &self.capabilities
    }

    /// Main run loop — reads from relay, routes to plugins; reads from plugins,
    /// forwards to relay. Handles HELLO/heartbeats per plugin locally.
    ///
    /// Blocks until the relay closes or a fatal error occurs.
    /// On exit, all managed plugin processes are killed.
    pub async fn run<R, W>(
        &mut self,
        relay_read: R,
        relay_write: W,
        resource_fn: impl Fn() -> Vec<u8> + Send + 'static,
    ) -> Result<(), AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Frame>();

        // Spawn outbound writer task (runtime → relay)
        let outbound_writer = tokio::spawn(Self::outbound_writer_loop(relay_write, outbound_rx));

        // Spawn relay reader task — reads frames from the relay and sends them
        // through a channel. This MUST be a dedicated task because read_exact is
        // NOT cancel-safe: if a partially-complete read_exact is dropped (e.g.,
        // by tokio::select! choosing another branch), the bytes already read are
        // lost and the byte stream desynchronizes.
        let (relay_tx, mut relay_rx) = mpsc::unbounded_channel::<Result<Frame, AsyncHostError>>();
        let relay_reader_task = tokio::spawn(async move {
            let mut reader = AsyncFrameReader::new(relay_read);
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if relay_tx.send(Ok(frame)).is_err() {
                            break; // Main loop dropped
                        }
                    }
                    Ok(None) => break, // Relay closed cleanly
                    Err(e) => {
                        let _ = relay_tx.send(Err(e.into()));
                        break;
                    }
                }
            }
        });

        let mut event_rx = self.event_rx.take().expect("run() must only be called once");

        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_interval.tick().await; // skip initial tick

        let result = loop {
            tokio::select! {
                biased;

                // Plugin events (frames from plugins, death notifications)
                Some(event) = event_rx.recv() => {
                    match event {
                        PluginEvent::Frame { plugin_idx, frame } => {
                            if let Err(e) = self.handle_plugin_frame(plugin_idx, frame, &outbound_tx) {
                                break Err(e);
                            }
                        }
                        PluginEvent::Death { plugin_idx } => {
                            if let Err(e) = self.handle_plugin_death(plugin_idx, &outbound_tx).await {
                                break Err(e);
                            }
                        }
                    }
                }

                // Frames from relay reader task (cancel-safe: channel recv is cancel-safe)
                relay_result = relay_rx.recv() => {
                    match relay_result {
                        Some(Ok(frame)) => {
                            if let Err(e) = self.handle_relay_frame(frame, &outbound_tx, &resource_fn).await {
                                break Err(e);
                            }
                        }
                        Some(Err(e)) => break Err(e),
                        None => break Ok(()), // Relay reader task exited (clean EOF)
                    }
                }

                // Periodic heartbeat probes
                _ = heartbeat_interval.tick() => {
                    self.send_heartbeats_and_check_timeouts(&outbound_tx);
                }
            }
        };

        // Cleanup: kill all managed plugin processes
        self.kill_all_plugins().await;
        relay_reader_task.abort();
        outbound_writer.abort();

        result
    }

    // =========================================================================
    // FRAME HANDLING
    // =========================================================================

    /// Handle a frame arriving from the relay (engine → plugin direction).
    async fn handle_relay_frame(
        &mut self,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
        resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = match frame.cap.as_ref() {
                    Some(c) => c.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing cap URN".to_string(),
                        ));
                    }
                };

                let plugin_idx = match self.find_plugin_for_cap(&cap_urn) {
                    Some(idx) => idx,
                    None => {
                        // No plugin handles this cap — send ERR back and continue.
                        let err = Frame::err(
                            frame.id.clone(),
                            "NO_HANDLER",
                            &format!("no plugin handles cap: {}", cap_urn),
                        );
                        outbound_tx
                            .send(err)
                            .map_err(|_| AsyncHostError::SendError)?;
                        return Ok(());
                    }
                };

                // Spawn on demand if not running
                if !self.plugins[plugin_idx].running {
                    self.spawn_plugin(plugin_idx, resource_fn).await?;
                    self.rebuild_capabilities();
                }

                // Register request routing
                self.request_routing.insert(frame.id.clone(), plugin_idx);

                // Forward to plugin
                self.send_to_plugin(plugin_idx, frame)
            }

            FrameType::StreamStart | FrameType::Chunk | FrameType::StreamEnd
            | FrameType::End | FrameType::Err => {
                // If there's no routing entry, the request was already cleaned up
                // (e.g., plugin died and death handler already sent ERR + removed entry).
                // Just drop the frame silently — the engine already got an ERR.
                let plugin_idx = match self.request_routing.get(&frame.id).copied() {
                    Some(idx) => idx,
                    None => return Ok(()), // Already cleaned up
                };

                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                // If the plugin is dead, send ERR to engine and clean up routing.
                if self.send_to_plugin(plugin_idx, frame.clone()).is_err() {
                    let err = Frame::err(
                        frame.id.clone(),
                        "PLUGIN_DIED",
                        "Plugin exited while processing request",
                    );
                    let _ = outbound_tx.send(err);
                    self.request_routing.remove(&frame.id);
                    self.peer_requests.remove(&frame.id);
                    return Ok(());
                }

                // Only remove routing on terminal frames if this is a PEER response
                // (engine responding to a plugin's peer invoke). For engine-initiated
                // requests, the relay END is just the end of the request body — the
                // plugin still needs to respond, so routing must survive.
                if is_terminal && self.peer_requests.contains(&frame.id) {
                    self.request_routing.remove(&frame.id);
                    self.peer_requests.remove(&frame.id);
                }

                Ok(())
            }

            // Everything else is a hard protocol error — these must never reach the runtime.
            FrameType::Hello => Err(AsyncHostError::Protocol(
                "HELLO from relay — engine must not send HELLO to runtime".to_string(),
            )),
            FrameType::Heartbeat => Err(AsyncHostError::Protocol(
                "HEARTBEAT from relay — engine must not send heartbeats to runtime".to_string(),
            )),
            FrameType::Log => Err(AsyncHostError::Protocol(
                "LOG from relay — LOG frames flow plugin→engine, not engine→plugin".to_string(),
            )),
            FrameType::RelayNotify | FrameType::RelayState => Err(AsyncHostError::Protocol(
                format!(
                    "{:?} reached runtime — relay must intercept these, never forward",
                    frame.frame_type
                ),
            )),
        }
    }

    /// Handle a frame arriving from a plugin (plugin → engine direction).
    fn handle_plugin_frame(
        &mut self,
        plugin_idx: usize,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        match frame.frame_type {
            // HELLO after handshake is a fatal protocol error.
            FrameType::Hello => Err(AsyncHostError::Protocol(format!(
                "Plugin {} sent HELLO after handshake — fatal protocol violation",
                plugin_idx
            ))),

            // Heartbeat: handle locally, never forward.
            FrameType::Heartbeat => {
                let is_our_probe = self.plugins[plugin_idx]
                    .pending_heartbeats
                    .remove(&frame.id)
                    .is_some();

                if is_our_probe {
                    // Response to our health probe — plugin is alive
                } else {
                    // Plugin-initiated heartbeat — respond immediately
                    let response = Frame::heartbeat(frame.id.clone());
                    self.send_to_plugin(plugin_idx, response)?;
                }
                Ok(())
            }

            // Relay frames from a plugin: fatal protocol error.
            FrameType::RelayNotify | FrameType::RelayState => Err(AsyncHostError::Protocol(
                format!(
                    "Plugin {} sent {:?} — plugins must never send relay frames",
                    plugin_idx, frame.frame_type
                ),
            )),

            // REQ from plugin = peer invoke. Register routing for the response path.
            // Mark as peer-initiated so we don't prematurely remove routing when the
            // plugin sends END (which is the end of the outgoing request body, not
            // the final response).
            FrameType::Req => {
                self.request_routing.insert(frame.id.clone(), plugin_idx);
                self.peer_requests.insert(frame.id.clone());
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }

            // Everything else: pass through to relay (toward engine).
            // For END/ERR, also clean up routing if this was a response to an engine request.
            // But NOT for peer-initiated requests — the plugin's END is the end of the
            // outgoing request body, and the relay's response hasn't arrived yet.
            _ => {
                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                if is_terminal {
                    if !self.peer_requests.contains(&frame.id) {
                        // Engine-initiated request: plugin's END/ERR is the final response.
                        if let Some(&idx) = self.request_routing.get(&frame.id) {
                            if idx == plugin_idx {
                                self.request_routing.remove(&frame.id);
                            }
                        }
                    }
                    // Peer-initiated: don't remove routing — the relay's response
                    // (END/ERR from engine) will clean up in handle_relay_frame().
                }

                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }
        }
    }

    /// Handle a plugin death (reader loop exited).
    async fn handle_plugin_death(
        &mut self,
        plugin_idx: usize,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        let plugin = &mut self.plugins[plugin_idx];
        plugin.running = false;
        plugin.writer_tx = None;

        // Kill the process if it's still around
        if let Some(ref mut child) = plugin.process {
            let _ = child.kill().await;
        }
        plugin.process = None;

        // Send ERR for all pending requests routed to this plugin
        let failed_req_ids: Vec<MessageId> = self
            .request_routing
            .iter()
            .filter(|(_, &idx)| idx == plugin_idx)
            .map(|(id, _)| id.clone())
            .collect();

        for req_id in &failed_req_ids {
            let err_frame = Frame::err(
                req_id.clone(),
                "PLUGIN_DIED",
                &format!("Plugin {} exited unexpectedly", plugin.path.display()),
            );
            let _ = outbound_tx.send(err_frame);
            self.request_routing.remove(req_id);
            self.peer_requests.remove(req_id);
        }

        // Remove caps temporarily (will be re-added on relaunch)
        self.update_cap_table();
        self.rebuild_capabilities();

        Ok(())
    }

    // =========================================================================
    // PLUGIN LIFECYCLE
    // =========================================================================

    /// Spawn a registered plugin binary on demand.
    async fn spawn_plugin(
        &mut self,
        plugin_idx: usize,
        _resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        let plugin = &self.plugins[plugin_idx];

        if plugin.hello_failed {
            return Err(AsyncHostError::Protocol(format!(
                "Plugin '{}' permanently failed — HELLO failure, binary is broken",
                plugin.path.display()
            )));
        }

        if plugin.path.as_os_str().is_empty() {
            return Err(AsyncHostError::Protocol(format!(
                "Plugin {} has no binary path — cannot spawn",
                plugin_idx
            )));
        }

        let mut child = tokio::process::Command::new(&plugin.path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true) // No orphan processes
            .spawn()
            .map_err(|e| {
                AsyncHostError::Io(format!(
                    "Failed to spawn plugin '{}': {}",
                    plugin.path.display(),
                    e
                ))
            })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        // HELLO handshake
        let mut reader = AsyncFrameReader::new(stdout);
        let mut writer = AsyncFrameWriter::new(stdin);

        let handshake_result = match handshake_async(&mut reader, &mut writer).await {
            Ok(result) => result,
            Err(e) => {
                // HELLO failure = permanent removal. Binary is broken.
                self.plugins[plugin_idx].hello_failed = true;
                let _ = child.kill().await;
                return Err(AsyncHostError::Handshake(format!(
                    "Plugin '{}' HELLO failed: {} — permanently removed",
                    self.plugins[plugin_idx].path.display(),
                    e
                )));
            }
        };

        let caps = parse_caps_from_manifest(&handshake_result.manifest)?;

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        // Start reader task
        let rh = Self::start_reader_task(plugin_idx, reader, self.event_tx.clone());

        // Update plugin state
        let plugin = &mut self.plugins[plugin_idx];
        plugin.manifest = handshake_result.manifest;
        plugin.limits = handshake_result.limits;
        plugin.caps = caps;
        plugin.running = true;
        plugin.process = Some(child);
        plugin.writer_tx = Some(writer_tx);
        plugin.reader_handle = Some(rh);
        plugin.writer_handle = Some(wh);

        self.update_cap_table();

        Ok(())
    }

    /// Send a frame to a specific plugin's stdin.
    fn send_to_plugin(&self, plugin_idx: usize, frame: Frame) -> Result<(), AsyncHostError> {
        let plugin = &self.plugins[plugin_idx];
        let writer_tx = plugin.writer_tx.as_ref().ok_or_else(|| {
            AsyncHostError::Protocol(format!(
                "Plugin {} not running — no writer channel",
                plugin_idx
            ))
        })?;
        writer_tx.send(frame).map_err(|_| AsyncHostError::SendError)
    }

    /// Find which plugin handles a given cap URN.
    fn find_plugin_for_cap(&self, cap_urn: &str) -> Option<usize> {
        // Try exact match first
        for (registered_cap, plugin_idx) in &self.cap_table {
            if registered_cap == cap_urn {
                return Some(*plugin_idx);
            }
        }

        // Try URN-level matching: request is the pattern, registered cap is the instance
        if let Ok(request_urn) = crate::CapUrn::from_string(cap_urn) {
            for (registered_cap, plugin_idx) in &self.cap_table {
                if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                    if request_urn.accepts(&registered_urn) {
                        return Some(*plugin_idx);
                    }
                }
            }
        }

        None
    }

    // =========================================================================
    // HEARTBEAT HEALTH MONITORING
    // =========================================================================

    /// Send heartbeat probes to all running plugins and check for timeouts.
    fn send_heartbeats_and_check_timeouts(
        &mut self,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) {
        let now = Instant::now();

        for plugin_idx in 0..self.plugins.len() {
            let plugin = &mut self.plugins[plugin_idx];
            if !plugin.running {
                continue;
            }

            // Check for timed-out heartbeats
            let timed_out: Vec<MessageId> = plugin
                .pending_heartbeats
                .iter()
                .filter(|(_, sent)| now.duration_since(**sent) > HEARTBEAT_TIMEOUT)
                .map(|(id, _)| id.clone())
                .collect();

            if !timed_out.is_empty() {
                // Plugin is unresponsive — remove its caps temporarily
                for id in timed_out {
                    plugin.pending_heartbeats.remove(&id);
                }
                plugin.running = false;

                // Send ERR for pending requests
                let failed_req_ids: Vec<MessageId> = self
                    .request_routing
                    .iter()
                    .filter(|(_, &idx)| idx == plugin_idx)
                    .map(|(id, _)| id.clone())
                    .collect();

                for req_id in &failed_req_ids {
                    let err_frame = Frame::err(
                        req_id.clone(),
                        "PLUGIN_UNHEALTHY",
                        "Plugin stopped responding to heartbeats",
                    );
                    let _ = outbound_tx.send(err_frame);
                    self.request_routing.remove(req_id);
                    self.peer_requests.remove(req_id);
                }

                continue;
            }

            // Send a new heartbeat probe
            if let Some(ref writer_tx) = plugin.writer_tx {
                let hb_id = MessageId::new_uuid();
                let hb = Frame::heartbeat(hb_id.clone());
                if writer_tx.send(hb).is_ok() {
                    plugin.pending_heartbeats.insert(hb_id, now);
                }
            }
        }

        // Rebuild after potential cap changes
        self.update_cap_table();
        self.rebuild_capabilities();
    }

    // =========================================================================
    // INTERNAL HELPERS
    // =========================================================================

    /// Rebuild the cap_table from all plugins (running or registered).
    fn update_cap_table(&mut self) {
        self.cap_table.clear();
        for (idx, plugin) in self.plugins.iter().enumerate() {
            if plugin.hello_failed {
                continue; // Permanently removed
            }
            // Use real caps if available (from HELLO), otherwise known_caps
            let caps = if plugin.running && !plugin.caps.is_empty() {
                &plugin.caps
            } else {
                &plugin.known_caps
            };
            for cap in caps {
                self.cap_table.push((cap.clone(), idx));
            }
        }
    }

    /// Rebuild the aggregate capabilities JSON from all running, healthy plugins.
    fn rebuild_capabilities(&mut self) {
        let mut all_caps = Vec::new();
        for plugin in &self.plugins {
            if plugin.running && !plugin.hello_failed {
                for cap in &plugin.caps {
                    all_caps.push(cap.clone());
                }
            }
        }
        // Simple JSON array of cap URN strings
        let json = serde_json::json!({ "caps": all_caps });
        self.capabilities = serde_json::to_vec(&json).unwrap_or_default();
    }

    /// Kill all managed plugin processes.
    ///
    /// Order matters: drop writer_tx first (closes the channel), then AWAIT the
    /// writer handle (so it exits naturally and drops the write stream, which
    /// causes the plugin to see EOF). Only then abort the reader handle.
    /// Aborting the writer instead of awaiting it can leave the write stream
    /// open in a single-threaded runtime, deadlocking any sync thread that
    /// blocks on the plugin's read().
    async fn kill_all_plugins(&mut self) {
        for plugin in &mut self.plugins {
            if let Some(ref mut child) = plugin.process {
                let _ = child.kill().await;
            }
            plugin.process = None;
            plugin.running = false;

            // Close the channel → writer task's rx.recv() returns None → task exits
            plugin.writer_tx = None;

            // AWAIT (not abort) the writer handle so it drops the write stream cleanly.
            if let Some(handle) = plugin.writer_handle.take() {
                let _ = handle.await;
            }

            // Now the write stream is closed → plugin sees EOF.
            // Safe to abort the reader (it will exit on its own anyway).
            if let Some(handle) = plugin.reader_handle.take() {
                handle.abort();
            }
        }
    }

    /// Spawn a writer task that reads frames from a channel and writes to a plugin's stdin.
    fn start_writer_task<W: AsyncWrite + Unpin + Send + 'static>(
        mut writer: AsyncFrameWriter<W>,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if let Err(e) = writer.write(&frame).await {
                    eprintln!("[PluginWriter] write error: {}", e);
                    break;
                }
            }
        })
    }

    /// Spawn a reader task that reads frames from a plugin's stdout and sends events.
    fn start_reader_task<R: AsyncRead + Unpin + Send + 'static>(
        plugin_idx: usize,
        mut reader: AsyncFrameReader<R>,
        event_tx: mpsc::UnboundedSender<PluginEvent>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if event_tx
                            .send(PluginEvent::Frame {
                                plugin_idx,
                                frame,
                            })
                            .is_err()
                        {
                            break; // Runtime dropped
                        }
                    }
                    Ok(None) => {
                        // EOF — plugin closed stdout
                        let _ = event_tx.send(PluginEvent::Death { plugin_idx });
                        break;
                    }
                    Err(_) => {
                        // Read error — treat as death
                        let _ = event_tx.send(PluginEvent::Death { plugin_idx });
                        break;
                    }
                }
            }
        })
    }

    /// Outbound writer loop: reads frames from channel, writes to relay.
    async fn outbound_writer_loop<W: AsyncWrite + Unpin>(
        relay_write: W,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) {
        let mut writer = AsyncFrameWriter::new(relay_write);
        while let Some(frame) = rx.recv().await {
            if let Err(e) = writer.write(&frame).await {
                eprintln!("[PluginHostRuntime] outbound write error: {}", e);
                break;
            }
        }
    }
}

impl Drop for AsyncPluginHost {
    fn drop(&mut self) {
        // Drop cannot be async, so we close channels (triggering writer exit)
        // and abort reader tasks. Writer tasks exit naturally when writer_tx
        // is dropped (channel closes → rx.recv() returns None → task exits
        // → OwnedWriteHalf dropped → plugin sees EOF).
        // Child processes with kill_on_drop will be killed when Child is dropped.
        for plugin in &mut self.plugins {
            plugin.writer_tx = None; // Close channel → writer task exits naturally
            if let Some(handle) = plugin.reader_handle.take() {
                handle.abort();
            }
            // Don't abort writer — let it exit naturally so the stream closes cleanly.
        }
    }
}

// =============================================================================
// HELPERS
// =============================================================================

/// Parse cap URNs from a plugin manifest JSON.
///
/// Expected format:
/// ```json
/// {"name": "...", "caps": [{"urn": "cap:op=test", ...}, ...]}
/// ```
fn parse_caps_from_manifest(manifest: &[u8]) -> Result<Vec<String>, AsyncHostError> {
    let value: serde_json::Value = serde_json::from_slice(manifest).map_err(|e| {
        AsyncHostError::Protocol(format!("Invalid manifest JSON: {}", e))
    })?;

    let caps = value
        .get("caps")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|cap| cap.get("urn").and_then(|u| u.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    Ok(caps)
}

// =============================================================================
// TESTS
// =============================================================================

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
            ResponseChunk { payload: b"hello".to_vec(), seq: 0, offset: Some(0), len: Some(11), is_eof: false },
            ResponseChunk { payload: b" world".to_vec(), seq: 1, offset: Some(5), len: None, is_eof: true },
        ];
        let response = PluginResponse::Streaming(chunks);
        assert_eq!(response.concatenated(), b"hello world");
    }

    // TEST240: Test PluginResponse::Streaming final_payload returns the last chunk's payload
    #[test]
    fn test_plugin_response_streaming_final_payload() {
        let chunks = vec![
            ResponseChunk { payload: b"first".to_vec(), seq: 0, offset: None, len: None, is_eof: false },
            ResponseChunk { payload: b"last".to_vec(), seq: 1, offset: None, len: None, is_eof: true },
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
            ResponseChunk { payload: chunk1_data.clone(), seq: 0, offset: None, len: None, is_eof: false },
            ResponseChunk { payload: chunk2_data.clone(), seq: 1, offset: None, len: None, is_eof: true },
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
        let err = AsyncHostError::PluginError { code: "NOT_FOUND".to_string(), message: "Cap not found".to_string() };
        let msg = format!("{}", err);
        assert!(msg.contains("NOT_FOUND"));
        assert!(msg.contains("Cap not found"));

        assert_eq!(format!("{}", AsyncHostError::Closed), "Host is closed");
        assert_eq!(format!("{}", AsyncHostError::ProcessExited), "Plugin process exited unexpectedly");
        assert_eq!(format!("{}", AsyncHostError::SendError), "Send error: channel closed");
        assert_eq!(format!("{}", AsyncHostError::RecvError), "Receive error: channel closed");
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
        let err = AsyncHostError::PluginError { code: "ERR".to_string(), message: "msg".to_string() };
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    // TEST247: Test ResponseChunk Clone produces independent copy with same data
    #[test]
    fn test_response_chunk_clone() {
        let chunk = ResponseChunk { payload: b"data".to_vec(), seq: 3, offset: Some(100), len: Some(500), is_eof: true };
        let cloned = chunk.clone();
        assert_eq!(chunk.payload, cloned.payload);
        assert_eq!(chunk.seq, cloned.seq);
        assert_eq!(chunk.offset, cloned.offset);
        assert_eq!(chunk.len, cloned.len);
        assert_eq!(chunk.is_eof, cloned.is_eof);
    }

    // TEST413: Register plugin adds entries to cap_table
    #[test]
    fn test_register_plugin_adds_to_cap_table() {
        let mut runtime = AsyncPluginHost::new();
        runtime.register_plugin(Path::new("/usr/bin/test-plugin"), &[
            "cap:op=convert".to_string(),
            "cap:op=analyze".to_string(),
        ]);

        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(runtime.cap_table[0].0, "cap:op=convert");
        assert_eq!(runtime.cap_table[0].1, 0);
        assert_eq!(runtime.cap_table[1].0, "cap:op=analyze");
        assert_eq!(runtime.cap_table[1].1, 0);
        assert_eq!(runtime.plugins.len(), 1);
        assert!(!runtime.plugins[0].running);
    }

    // TEST414: capabilities() returns empty JSON initially (no running plugins)
    #[test]
    fn test_capabilities_empty_initially() {
        let runtime = AsyncPluginHost::new();
        assert!(runtime.capabilities().is_empty(), "No plugins registered = empty capabilities");

        let mut runtime2 = AsyncPluginHost::new();
        runtime2.register_plugin(Path::new("/usr/bin/test"), &["cap:op=test".to_string()]);
        // Plugin registered but not running — capabilities still empty
        assert!(runtime2.capabilities().is_empty(),
            "Registered but not running plugin should not appear in capabilities");
    }

    // TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary)
    #[tokio::test]
    async fn test_req_for_known_cap_triggers_spawn() {
        let mut runtime = AsyncPluginHost::new();
        runtime.register_plugin(
            Path::new("/nonexistent/plugin/binary"),
            &["cap:op=test".to_string()],
        );

        // Create relay pipe pair
        let (relay_runtime_read, relay_engine_write) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_engine_read, relay_runtime_write) =
            std::os::unix::net::UnixStream::pair().unwrap();

        relay_runtime_read.set_nonblocking(true).unwrap();
        relay_runtime_write.set_nonblocking(true).unwrap();
        relay_engine_write.set_nonblocking(true).unwrap();
        relay_engine_read.set_nonblocking(true).unwrap();

        let runtime_read = tokio::net::UnixStream::from_std(relay_runtime_read).unwrap();
        let runtime_write = tokio::net::UnixStream::from_std(relay_runtime_write).unwrap();
        let engine_write_stream = tokio::net::UnixStream::from_std(relay_engine_write).unwrap();

        let (runtime_read_half, _) = runtime_read.into_split();
        let (_, runtime_write_half) = runtime_write.into_split();
        let (_, engine_write_half) = engine_write_stream.into_split();

        // Send a REQ through the relay
        let send_handle = tokio::spawn(async move {
            let mut writer = AsyncFrameWriter::new(engine_write_half);
            let req = Frame::req(MessageId::new_uuid(), "cap:op=test", vec![], "text/plain");
            writer.write(&req).await.unwrap();
        });

        // Run the runtime — should attempt to spawn, fail (binary doesn't exist)
        let result = runtime.run(runtime_read_half, runtime_write_half, || vec![]).await;

        // The spawn failure is an Io error for the non-existent binary
        assert!(result.is_err(), "Should fail because binary doesn't exist");
        let err = result.unwrap_err();
        let err_str = format!("{}", err);
        assert!(
            err_str.contains("nonexistent") || err_str.contains("spawn"),
            "Error should mention spawn failure, got: {}",
            err_str
        );

        send_handle.await.unwrap();
    }

    // TEST416: Attach plugin performs HELLO handshake, extracts manifest, updates capabilities
    #[tokio::test]
    async fn test_attach_plugin_handshake_updates_capabilities() {
        let manifest = r#"{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=echo"}]}"#;

        // Plugin pipe pair
        let (plugin_to_runtime_std, runtime_from_plugin_std) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let (runtime_to_plugin_std, plugin_from_runtime_std) =
            std::os::unix::net::UnixStream::pair().unwrap();

        runtime_from_plugin_std.set_nonblocking(true).unwrap();
        runtime_to_plugin_std.set_nonblocking(true).unwrap();

        let runtime_from_plugin = tokio::net::UnixStream::from_std(runtime_from_plugin_std).unwrap();
        let runtime_to_plugin = tokio::net::UnixStream::from_std(runtime_to_plugin_std).unwrap();

        let (plugin_read, _) = runtime_from_plugin.into_split();
        let (_, plugin_write) = runtime_to_plugin.into_split();

        // Plugin thread does handshake
        let manifest_bytes = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut reader = FrameReader::new(BufReader::new(plugin_from_runtime_std));
            let mut writer = FrameWriter::new(BufWriter::new(plugin_to_runtime_std));
            handshake_accept(&mut reader, &mut writer, &manifest_bytes).unwrap();
        });

        let mut runtime = AsyncPluginHost::new();
        let idx = runtime.attach_plugin(plugin_read, plugin_write).await.unwrap();

        assert_eq!(idx, 0);
        assert!(runtime.plugins[0].running);
        assert_eq!(runtime.plugins[0].caps, vec!["cap:op=echo".to_string()]);
        assert!(!runtime.capabilities().is_empty());

        // Capabilities JSON should mention the cap
        let caps_str = std::str::from_utf8(runtime.capabilities()).unwrap();
        assert!(caps_str.contains("cap:op=echo"), "Capabilities must include attached plugin's cap");

        plugin_handle.join().unwrap();
    }

    // TEST417: Route REQ to correct plugin by cap_urn (with two attached plugins)
    #[tokio::test]
    async fn test_route_req_to_correct_plugin() {
        let manifest_a = r#"{"name":"PluginA","version":"1.0","caps":[{"urn":"cap:op=convert"}]}"#;
        let manifest_b = r#"{"name":"PluginB","version":"1.0","caps":[{"urn":"cap:op=analyze"}]}"#;

        // Create two plugin pipe pairs
        let (pa_to_rt_std, rt_from_pa_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_pa_std, pa_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (pb_to_rt_std, rt_from_pb_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_pb_std, pb_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();

        for s in [&rt_from_pa_std, &rt_to_pa_std, &rt_from_pb_std, &rt_to_pb_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_from_pa = tokio::net::UnixStream::from_std(rt_from_pa_std).unwrap();
        let rt_to_pa = tokio::net::UnixStream::from_std(rt_to_pa_std).unwrap();
        let rt_from_pb = tokio::net::UnixStream::from_std(rt_from_pb_std).unwrap();
        let rt_to_pb = tokio::net::UnixStream::from_std(rt_to_pb_std).unwrap();

        let (pa_read, _) = rt_from_pa.into_split();
        let (_, pa_write) = rt_to_pa.into_split();
        let (pb_read, _) = rt_from_pb.into_split();
        let (_, pb_write) = rt_to_pb.into_split();

        // Plugin A thread
        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(pa_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(pa_to_rt_std));
            handshake_accept(&mut r, &mut w, &ma).unwrap();
            // Read one REQ and verify cap
            let frame = r.read().unwrap().expect("expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some("cap:op=convert"), "Plugin A should receive convert REQ");
            // Send END response
            let stream_id = "s1".to_string();
            w.write(&Frame::stream_start(frame.id.clone(), stream_id.clone(), "media:bytes".to_string())).unwrap();
            w.write(&Frame::chunk(frame.id.clone(), stream_id.clone(), 0, b"converted".to_vec())).unwrap();
            w.write(&Frame::stream_end(frame.id.clone(), stream_id)).unwrap();
            w.write(&Frame::end(frame.id, None)).unwrap();
        });

        // Plugin B thread
        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(pb_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(pb_to_rt_std));
            handshake_accept(&mut r, &mut w, &mb).unwrap();
            // Plugin B should NOT receive the convert REQ — wait for EOF
            match r.read() {
                Ok(None) => {} // EOF is expected
                Ok(Some(f)) => panic!("Plugin B should not receive any frames, got {:?}", f.frame_type),
                Err(_) => {} // Also acceptable on close
            }
        });

        // Setup runtime
        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(pa_read, pa_write).await.unwrap();
        runtime.attach_plugin(pb_read, pb_write).await.unwrap();

        // Create relay pipes
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        // Engine: send REQ, read response, THEN close relay
        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let sid = uuid::Uuid::new_v4().to_string();
            w.write(&Frame::req(req_id.clone(), "cap:op=convert", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string())).await.unwrap();
            w.write(&Frame::chunk(req_id.clone(), sid.clone(), 0, b"input".to_vec())).await.unwrap();
            w.write(&Frame::stream_end(req_id.clone(), sid)).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk { payload.extend(f.payload.unwrap_or_default()); }
                        if f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            payload
        });

        // Run runtime
        let runtime_result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(runtime_result.is_ok(), "Runtime should exit cleanly: {:?}", runtime_result);

        let response_payload = engine_task.await.unwrap();
        assert_eq!(response_payload, b"converted");

        pa_handle.join().unwrap();
        pb_handle.join().unwrap();
    }

    // TEST419: Plugin HEARTBEAT handled locally (not forwarded to relay)
    #[tokio::test]
    async fn test_plugin_heartbeat_handled_locally() {
        let manifest = r#"{"name":"HBPlugin","version":"1.0","caps":[{"urn":"cap:op=hb"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();

            // Send a heartbeat from plugin
            let hb_id = MessageId::new_uuid();
            w.write(&Frame::heartbeat(hb_id.clone())).unwrap();

            // Read the heartbeat response
            let response = r.read().unwrap().expect("Expected heartbeat response");
            assert_eq!(response.frame_type, FrameType::Heartbeat);
            assert_eq!(response.id, hb_id, "Response must echo the same ID");

            drop(w); // Close to signal EOF
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Relay pipes
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        // Drop engine write to close relay after plugin finishes
        drop(relay_eng_write_std);

        // Engine reads — should NOT receive any heartbeat frame
        let engine_recv = tokio::spawn(async move {
            let mut r = AsyncFrameReader::new(eng_read_half);
            let mut frames = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => frames.push(f.frame_type),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            frames
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let received_types = engine_recv.await.unwrap();
        assert!(
            !received_types.contains(&FrameType::Heartbeat),
            "Heartbeat must NOT be forwarded to relay. Received frame types: {:?}",
            received_types
        );

        plugin_handle.join().unwrap();
    }

    // TEST420: Plugin non-HELLO/non-HB frames forwarded to relay (pass-through)
    #[tokio::test]
    async fn test_plugin_frames_forwarded_to_relay() {
        let manifest = r#"{"name":"FwdPlugin","version":"1.0","caps":[{"urn":"cap:op=fwd"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let req_id = MessageId::new_uuid();
        let req_id_for_plugin = req_id.clone();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();

            // Read the REQ
            let frame = r.read().unwrap().expect("Expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);

            // Consume incoming streams until END
            loop {
                let f = r.read().unwrap().expect("Expected frame");
                if f.frame_type == FrameType::End { break; }
            }

            // Send LOG + response (LOG should be forwarded too)
            w.write(&Frame::log(req_id_for_plugin.clone(), "info", "Processing")).unwrap();
            let sid = "rs".to_string();
            w.write(&Frame::stream_start(req_id_for_plugin.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            w.write(&Frame::chunk(req_id_for_plugin.clone(), sid.clone(), 0, b"result".to_vec())).unwrap();
            w.write(&Frame::stream_end(req_id_for_plugin.clone(), sid)).unwrap();
            w.write(&Frame::end(req_id_for_plugin, None)).unwrap();
            drop(w);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Relay
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        // Engine: send REQ, read response (keep relay open until response received)
        let req_id_send = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let sid = uuid::Uuid::new_v4().to_string();
            w.write(&Frame::req(req_id_send.clone(), "cap:op=fwd", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::stream_start(req_id_send.clone(), sid.clone(), "media:bytes".to_string())).await.unwrap();
            w.write(&Frame::stream_end(req_id_send.clone(), sid)).await.unwrap();
            w.write(&Frame::end(req_id_send, None)).await.unwrap();

            let mut types = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        let is_end = f.frame_type == FrameType::End;
                        types.push(f.frame_type);
                        if is_end { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            types
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let received_types = engine_task.await.unwrap();

        // Should see: LOG, STREAM_START, CHUNK, STREAM_END, END
        assert!(received_types.contains(&FrameType::Log), "LOG should be forwarded. Got: {:?}", received_types);
        assert!(received_types.contains(&FrameType::StreamStart), "STREAM_START should be forwarded");
        assert!(received_types.contains(&FrameType::Chunk), "CHUNK should be forwarded");
        assert!(received_types.contains(&FrameType::End), "END should be forwarded");

        plugin_handle.join().unwrap();
    }

    // TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn)
    // Verifies that after the initial REQ→plugin routing, all subsequent continuation
    // frames with the same req_id are routed to the same plugin — even though no cap_urn
    // is present on those frames.
    #[tokio::test]
    async fn test_route_continuation_frames_by_req_id() {
        let manifest = r#"{"name":"ContPlugin","version":"1.0","caps":[{"urn":"cap:op=cont"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();

            // Read REQ
            let req = r.read().unwrap().expect("Expected REQ");
            assert_eq!(req.frame_type, FrameType::Req);

            // Continuation frames must arrive with same req_id
            let mut received_types = Vec::new();
            let mut data = Vec::new();
            loop {
                let f = r.read().unwrap().expect("Expected frame");
                received_types.push(f.frame_type);
                if f.frame_type == FrameType::Chunk {
                    data.extend(f.payload.unwrap_or_default());
                }
                if f.frame_type == FrameType::End { break; }
                assert_eq!(f.id, req.id, "All continuation frames must have same req_id");
            }

            // Verify we got the full sequence
            assert!(received_types.contains(&FrameType::StreamStart), "Must receive STREAM_START");
            assert!(received_types.contains(&FrameType::Chunk), "Must receive CHUNK");
            assert!(received_types.contains(&FrameType::StreamEnd), "Must receive STREAM_END");
            assert!(received_types.contains(&FrameType::End), "Must receive END");
            assert_eq!(data, b"payload-data", "Must receive full payload");

            // Send response
            let sid = "rs".to_string();
            w.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            w.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"ok".to_vec())).unwrap();
            w.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            w.write(&Frame::end(req.id, None)).unwrap();
            drop(w);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Relay
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            // Send REQ + stream continuation frames
            w.write(&Frame::req(req_id.clone(), "cap:op=cont", vec![], "text/plain")).await.unwrap();
            let sid = uuid::Uuid::new_v4().to_string();
            w.write(&Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string())).await.unwrap();
            w.write(&Frame::chunk(req_id.clone(), sid.clone(), 0, b"payload-data".to_vec())).await.unwrap();
            w.write(&Frame::stream_end(req_id.clone(), sid)).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk { payload.extend(f.payload.unwrap_or_default()); }
                        if f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            payload
        });

        let result = runtime.run(rt_read_half, rt_write_half, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let response = engine_task.await.unwrap();
        assert_eq!(response, b"ok");

        plugin_handle.join().unwrap();
    }

    // TEST421: Plugin death updates capability list (caps removed)
    #[tokio::test]
    async fn test_plugin_death_updates_capabilities() {
        let manifest = r#"{"name":"Dying","version":"1.0","caps":[{"urn":"cap:op=die"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();
            // Die immediately after handshake
            drop(w);
            drop(r);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Before death: caps should include "cap:op=die"
        let caps_before = std::str::from_utf8(runtime.capabilities()).unwrap().to_string();
        assert!(caps_before.contains("cap:op=die"));

        // Relay (close immediately to let runtime exit after processing death)
        let (relay_rt_read_std, _relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (_relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        relay_rt_read_std.set_nonblocking(true).unwrap();
        relay_rt_write_std.set_nonblocking(true).unwrap();

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();

        // Drop engine write side to close relay
        drop(_relay_eng_write_std);

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        // After death: caps should be empty
        let caps_after = runtime.capabilities();
        if !caps_after.is_empty() {
            let caps_str = std::str::from_utf8(caps_after).unwrap();
            // Should either be empty or have empty caps array
            let parsed: serde_json::Value = serde_json::from_str(caps_str).unwrap();
            let arr = parsed["caps"].as_array().unwrap();
            assert!(arr.is_empty(), "Dead plugin's caps should be removed. Got: {}", caps_str);
        }

        plugin_handle.join().unwrap();
    }

    // TEST422: Plugin death sends ERR for all pending requests via relay
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_plugin_death_sends_err_for_pending_requests() {
        let manifest = r#"{"name":"DiePlugin","version":"1.0","caps":[{"urn":"cap:op=die"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();

            // Read REQ and consume all frames until END, then die
            let _req = r.read().unwrap().expect("Expected REQ");
            loop {
                match r.read() {
                    Ok(Some(f)) => { if f.frame_type == FrameType::End { break; } }
                    _ => break,
                }
            }
            // Die — drop everything
            drop(w);
            drop(r);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Relay
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }

        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            // Send REQ (plugin will die after reading it)
            w.write(&Frame::req(req_id.clone(), "cap:op=die", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            // Should receive ERR frame with PLUGIN_DIED code
            let mut err_code = String::new();
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match r.read().await {
                        Ok(Some(f)) => {
                            if f.frame_type == FrameType::Err {
                                err_code = f.error_code().unwrap_or("").to_string();
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
            }).await;
            assert!(result.is_ok(), "Engine must receive ERR within 5 seconds");

            drop(w);
            err_code
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let err_code = engine_task.await.unwrap();
        assert_eq!(err_code, "PLUGIN_DIED", "Engine must receive PLUGIN_DIED error for pending request");

        plugin_handle.join().unwrap();
    }

    // TEST423: Multiple plugins registered with distinct caps route independently
    #[tokio::test]
    async fn test_multiple_plugins_route_independently() {
        let manifest_a = r#"{"name":"PA","version":"1.0","caps":[{"urn":"cap:op=alpha"}]}"#;
        let manifest_b = r#"{"name":"PB","version":"1.0","caps":[{"urn":"cap:op=beta"}]}"#;

        // Plugin A
        let (pa_to_rt, rt_from_pa) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_pa, pa_from_rt) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_pa.set_nonblocking(true).unwrap();
        rt_to_pa.set_nonblocking(true).unwrap();
        let rt_from_pa_t = tokio::net::UnixStream::from_std(rt_from_pa).unwrap();
        let rt_to_pa_t = tokio::net::UnixStream::from_std(rt_to_pa).unwrap();
        let (pa_read, _) = rt_from_pa_t.into_split();
        let (_, pa_write) = rt_to_pa_t.into_split();

        // Plugin B
        let (pb_to_rt, rt_from_pb) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_pb, pb_from_rt) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_pb.set_nonblocking(true).unwrap();
        rt_to_pb.set_nonblocking(true).unwrap();
        let rt_from_pb_t = tokio::net::UnixStream::from_std(rt_from_pb).unwrap();
        let rt_to_pb_t = tokio::net::UnixStream::from_std(rt_to_pb).unwrap();
        let (pb_read, _) = rt_from_pb_t.into_split();
        let (_, pb_write) = rt_to_pb_t.into_split();

        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(pa_from_rt));
            let mut w = FrameWriter::new(BufWriter::new(pa_to_rt));
            handshake_accept(&mut r, &mut w, &ma).unwrap();
            let req = r.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:op=alpha"));
            loop { let f = r.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "a".to_string();
            w.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            w.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"from-A".to_vec())).unwrap();
            w.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            w.write(&Frame::end(req.id, None)).unwrap();
            drop(w);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(pb_from_rt));
            let mut w = FrameWriter::new(BufWriter::new(pb_to_rt));
            handshake_accept(&mut r, &mut w, &mb).unwrap();
            let req = r.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:op=beta"));
            loop { let f = r.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "b".to_string();
            w.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            w.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"from-B".to_vec())).unwrap();
            w.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            w.write(&Frame::end(req.id, None)).unwrap();
            drop(w);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(pa_read, pa_write).await.unwrap();
        runtime.attach_plugin(pb_read, pb_write).await.unwrap();

        // Relay
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }
        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();
        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let alpha_id = MessageId::new_uuid();
        let beta_id = MessageId::new_uuid();
        let alpha_c = alpha_id.clone();
        let beta_c = beta_id.clone();

        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            // Send two requests to different caps
            w.write(&Frame::req(alpha_c.clone(), "cap:op=alpha", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(alpha_c.clone(), None)).await.unwrap();
            w.write(&Frame::req(beta_c.clone(), "cap:op=beta", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(beta_c.clone(), None)).await.unwrap();

            // Collect responses by req_id
            let mut alpha_data = Vec::new();
            let mut beta_data = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == alpha_c { alpha_data.extend(f.payload.unwrap_or_default()); }
                            else if f.id == beta_c { beta_data.extend(f.payload.unwrap_or_default()); }
                        }
                        if f.frame_type == FrameType::End { ends += 1; if ends >= 2 { break; } }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            (alpha_data, beta_data)
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let (alpha_data, beta_data) = engine_task.await.unwrap();
        assert_eq!(alpha_data, b"from-A", "Alpha response from Plugin A");
        assert_eq!(beta_data, b"from-B", "Beta response from Plugin B");

        pa_handle.join().unwrap();
        pb_handle.join().unwrap();
    }

    // TEST424: Concurrent requests to the same plugin are handled independently
    #[tokio::test]
    async fn test_concurrent_requests_to_same_plugin() {
        let manifest = r#"{"name":"ConcPlugin","version":"1.0","caps":[{"urn":"cap:op=conc"}]}"#;

        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();
        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut r = FrameReader::new(BufReader::new(p_from_rt_std));
            let mut w = FrameWriter::new(BufWriter::new(p_to_rt_std));
            handshake_accept(&mut r, &mut w, &m).unwrap();

            // Read two REQs and their streams, then respond to each
            let mut pending: Vec<MessageId> = Vec::new();
            let mut active_requests = 0;
            loop {
                let f = r.read().unwrap().expect("frame");
                match f.frame_type {
                    FrameType::Req => { pending.push(f.id.clone()); active_requests += 1; }
                    FrameType::End => {
                        // When we've seen END for both requests, respond to both
                        active_requests -= 1;
                        if active_requests == 0 && pending.len() == 2 { break; }
                    }
                    _ => {}
                }
            }

            // Respond to each with different data
            for (i, req_id) in pending.iter().enumerate() {
                let data = format!("response-{}", i).into_bytes();
                let sid = format!("s{}", i);
                w.write(&Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
                w.write(&Frame::chunk(req_id.clone(), sid.clone(), 0, data)).unwrap();
                w.write(&Frame::stream_end(req_id.clone(), sid)).unwrap();
                w.write(&Frame::end(req_id.clone(), None)).unwrap();
            }
            drop(w);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Relay
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }
        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();
        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        let req_id_0 = MessageId::new_uuid();
        let req_id_1 = MessageId::new_uuid();
        let r0 = req_id_0.clone();
        let r1 = req_id_1.clone();

        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            // Send two REQs concurrently (same cap)
            w.write(&Frame::req(r0.clone(), "cap:op=conc", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(r0.clone(), None)).await.unwrap();
            w.write(&Frame::req(r1.clone(), "cap:op=conc", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(r1.clone(), None)).await.unwrap();

            // Collect responses by req_id
            let mut data_0 = Vec::new();
            let mut data_1 = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == r0 { data_0.extend(f.payload.unwrap_or_default()); }
                            else if f.id == r1 { data_1.extend(f.payload.unwrap_or_default()); }
                        }
                        if f.frame_type == FrameType::End { ends += 1; if ends >= 2 { break; } }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);
            (data_0, data_1)
        });

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        let (data_0, data_1) = engine_task.await.unwrap();
        assert_eq!(data_0, b"response-0", "First concurrent request response");
        assert_eq!(data_1, b"response-1", "Second concurrent request response");

        plugin_handle.join().unwrap();
    }

    // TEST425: find_plugin_for_cap returns None for unregistered cap
    #[test]
    fn test_find_plugin_for_cap_unknown() {
        let mut runtime = AsyncPluginHost::new();
        runtime.register_plugin(Path::new("/test"), &["cap:op=known".to_string()]);
        assert!(runtime.find_plugin_for_cap("cap:op=known").is_some());
        assert!(runtime.find_plugin_for_cap("cap:op=unknown").is_none());
    }
}
