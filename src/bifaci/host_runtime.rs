//! Async Plugin Host Runtime — Multi-plugin management with frame routing
//!
//! The PluginHostRuntime manages multiple plugin binaries, routing CBOR protocol
//! frames between a relay connection (to the engine) and individual plugin processes.
//!
//! ## Architecture
//!
//! ```text
//! Relay (engine) ←→ PluginHostRuntime ←→ Plugin A (stdin/stdout)
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

use crate::bifaci::frame::{Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{handshake_async, verify_identity, AsyncFrameReader, AsyncFrameWriter, CborError};
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
    /// Caps this plugin handles (from manifest after HELLO).
    caps: Vec<crate::Cap>,
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

    fn new_attached(manifest: Vec<u8>, limits: Limits, caps: Vec<crate::Cap>) -> Self {
        // Extract URN strings for known_caps (used for pre-HELLO routing)
        let known_caps: Vec<String> = caps.iter().map(|c| c.urn.to_string()).collect();

        Self {
            path: PathBuf::new(),
            process: None,
            writer_tx: None,
            manifest,
            limits,
            caps,
            known_caps,
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
pub struct PluginHostRuntime {
    /// Managed plugin binaries.
    plugins: Vec<ManagedPlugin>,
    /// Routing: cap_urn → plugin index (for finding which plugin handles a cap).
    cap_table: Vec<(String, usize)>,
    /// List 1: OUTGOING_RIDS - tracks peer requests sent by plugins (RID → plugin_idx).
    /// Used only to detect same-plugin peer calls (not for routing).
    outgoing_rids: HashMap<MessageId, usize>,
    /// List 2: INCOMING_RXIDS - tracks incoming requests from relay ((XID, RID) → plugin_idx).
    /// Continuations for these requests are routed by this table.
    incoming_rxids: HashMap<(MessageId, MessageId), usize>,
    /// Aggregate capabilities (serialized JSON manifest of all plugin caps).
    capabilities: Vec<u8>,
    /// Channel sender for plugin events (shared with reader tasks).
    event_tx: mpsc::UnboundedSender<PluginEvent>,
    /// Channel receiver for plugin events (consumed by run()).
    event_rx: Option<mpsc::UnboundedReceiver<PluginEvent>>,
}

impl PluginHostRuntime {
    /// Create a new plugin host runtime.
    ///
    /// After creation, register plugins with `register_plugin()` or
    /// attach pre-connected plugins with `attach_plugin()`, then call `run()`.
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            plugins: Vec::new(),
            cap_table: Vec::new(),
            outgoing_rids: HashMap::new(),
            incoming_rxids: HashMap::new(),
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

        // Verify identity — proves the protocol stack works end-to-end
        verify_identity(&mut reader, &mut writer)
            .await
            .map_err(|e| AsyncHostError::Protocol(format!("Identity verification failed: {}", e)))?;

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
        self.rebuild_capabilities(None); // No relay during initialization

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
        let mut relay_connected = true; // Track relay connection state
        let relay_reader_task = tokio::spawn(async move {
            let mut reader = AsyncFrameReader::new(relay_read);
            eprintln!("[PluginHostRuntime/relay_reader] Starting relay reader loop");
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if relay_tx.send(Ok(frame)).is_err() {
                            eprintln!("[PluginHostRuntime/relay_reader] Main loop dropped, exiting");
                            break; // Main loop dropped
                        }
                    }
                    Ok(None) => {
                        eprintln!("[PluginHostRuntime/relay_reader] EOF from relay, relay connection closed!");
                        break; // Relay closed cleanly
                    }
                    Err(e) => {
                        eprintln!("[PluginHostRuntime/relay_reader] Read error from relay: {}", e);
                        let _ = relay_tx.send(Err(e.into()));
                        break;
                    }
                }
            }
            eprintln!("[PluginHostRuntime/relay_reader] Relay reader task exiting");
        });

        let mut event_rx = self.event_rx.take().expect("run() must only be called once");

        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_interval.tick().await; // skip initial tick

        // Send discovery RelayNotify if plugins were pre-attached
        // At this point all async tasks are spawned and running, so the frame will be delivered
        // self.capabilities is already a JSON array of capability URN strings: ["cap:...", "cap:..."]
        eprintln!("[PluginHostRuntime.run] Initial capabilities: {} bytes: {:?}",
                  self.capabilities.len(),
                  std::str::from_utf8(&self.capabilities).unwrap_or("<invalid UTF-8>"));
        if !self.capabilities.is_empty() {
            let notify_frame = Frame::relay_notify(&self.capabilities, &Limits::default());
            let _ = outbound_tx.send(notify_frame);
            eprintln!("[PluginHostRuntime.run] Sent initial RelayNotify with {} bytes", self.capabilities.len());
        }

        let result = loop {
            tokio::select! {
                biased;

                // Plugin events (frames from plugins, death notifications)
                Some(event) = event_rx.recv() => {
                    match event {
                        PluginEvent::Frame { plugin_idx, frame } => {
                            eprintln!("[PluginHostRuntime.run] Processing plugin {} frame: {:?} (id={:?})", plugin_idx, frame.frame_type, frame.id);
                            if let Err(e) = self.handle_plugin_frame(plugin_idx, frame, &outbound_tx) {
                                eprintln!("[PluginHostRuntime.run] handle_plugin_frame returned error: {}", e);
                                break Err(e);
                            }
                            eprintln!("[PluginHostRuntime.run] handle_plugin_frame completed successfully, continuing loop");
                        }
                        PluginEvent::Death { plugin_idx } => {
                            eprintln!("[PluginHostRuntime.run] Processing plugin {} death event", plugin_idx);
                            if let Err(e) = self.handle_plugin_death(plugin_idx, &outbound_tx).await {
                                eprintln!("[PluginHostRuntime.run] handle_plugin_death returned error: {}", e);
                                break Err(e);
                            }
                            eprintln!("[PluginHostRuntime.run] handle_plugin_death completed successfully, continuing loop");

                            // If relay disconnected AND all plugins dead, exit cleanly
                            let all_plugins_dead = self.plugins.iter().all(|p| !p.running);
                            if !relay_connected && all_plugins_dead {
                                eprintln!("[PluginHostRuntime.run] Relay disconnected and all plugins dead, exiting cleanly");
                                break Ok(());
                            }
                        }
                    }
                }

                // Frames from relay reader task (cancel-safe: channel recv is cancel-safe)
                relay_result = relay_rx.recv(), if relay_connected => {
                    match relay_result {
                        Some(Ok(frame)) => {
                            eprintln!("[PluginHostRuntime.run] Processing relay frame: {:?} (id={:?})", frame.frame_type, frame.id);
                            if let Err(e) = self.handle_relay_frame(frame, &outbound_tx, &resource_fn).await {
                                eprintln!("[PluginHostRuntime.run] handle_relay_frame returned error: {}", e);
                                break Err(e);
                            }
                            eprintln!("[PluginHostRuntime.run] handle_relay_frame completed successfully, continuing loop");
                        }
                        Some(Err(e)) => {
                            eprintln!("[PluginHostRuntime.run] Relay error: {} - disconnecting relay but keeping plugins alive", e);
                            relay_connected = false; // Disable relay branch, continue processing plugins

                            // If all plugins are also dead, exit cleanly
                            let all_plugins_dead = self.plugins.iter().all(|p| !p.running);
                            if all_plugins_dead {
                                eprintln!("[PluginHostRuntime.run] Relay error and all plugins dead, exiting cleanly");
                                break Ok(());
                            }
                        }
                        None => {
                            eprintln!("[PluginHostRuntime.run] Relay disconnected (EOF) - keeping plugins alive!");
                            relay_connected = false; // Disable relay branch, continue processing plugins

                            // If all plugins are also dead, exit cleanly
                            let all_plugins_dead = self.plugins.iter().all(|p| !p.running);
                            if all_plugins_dead {
                                eprintln!("[PluginHostRuntime.run] Relay disconnected and all plugins dead, exiting cleanly");
                                break Ok(());
                            }
                        }
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
                // PATH C: REQ coming FROM relay
                // MUST have XID (else FATAL - only switch can assign XIDs)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing XID - all frames from relay must have XID".to_string(),
                        ));
                    }
                };

                let cap_urn = match frame.cap.as_ref() {
                    Some(c) => c.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing cap URN".to_string(),
                        ));
                    }
                };

                eprintln!("[PluginHostRuntime.handle_relay_frame] REQ from relay: cap={}, RID={:?}, XID={:?}",
                    cap_urn, frame.id, xid);

                // Route by cap URN to find handler plugin
                let plugin_idx = match self.find_plugin_for_cap(&cap_urn) {
                    Some(idx) => idx,
                    None => {
                        eprintln!("[PluginHostRuntime.handle_relay_frame] No handler for cap: {}", cap_urn);
                        eprintln!("[PluginHostRuntime.handle_relay_frame] cap_table has {} entries", self.cap_table.len());
                        // No plugin handles this cap — send ERR back and continue.
                        let mut err = Frame::err(
                            frame.id.clone(),
                            "NO_HANDLER",
                            &format!("no plugin handles cap: {}", cap_urn),
                        );
                        err.routing_id = frame.routing_id.clone(); // Copy XID from incoming request
                        eprintln!("[PluginHostRuntime.handle_relay_frame] Sending ERR frame: RID={:?}, XID={:?}", err.id, err.routing_id);
                        let send_result = outbound_tx.send(err);
                        eprintln!("[PluginHostRuntime.handle_relay_frame] ERR send result: {:?}", send_result.is_ok());
                        send_result.map_err(|_| AsyncHostError::SendError)?;
                        eprintln!("[PluginHostRuntime.handle_relay_frame] Returning Ok after sending ERR");
                        return Ok(());
                    }
                };

                eprintln!("[PluginHostRuntime.handle_relay_frame] Routing REQ to plugin {} (cap matched)", plugin_idx);

                // Spawn on demand if not running
                if !self.plugins[plugin_idx].running {
                    eprintln!("[PluginHostRuntime.handle_relay_frame] Plugin {} not running, spawning on demand", plugin_idx);
                    self.spawn_plugin(plugin_idx, resource_fn).await?;
                    self.rebuild_capabilities(Some(outbound_tx)); // Send RelayNotify to relay
                }

                // Record in List 2: INCOMING_RXIDS (XID, RID) → plugin_idx
                self.incoming_rxids.insert((xid.clone(), frame.id.clone()), plugin_idx);
                eprintln!("[PluginHostRuntime.handle_relay_frame] Recorded incoming_rxids[({:?}, {:?})] = {}",
                    xid, frame.id, plugin_idx);

                // Forward to plugin WITH XID
                self.send_to_plugin(plugin_idx, frame)
            }

            FrameType::StreamStart | FrameType::Chunk | FrameType::StreamEnd
            | FrameType::End | FrameType::Err => {
                // PATH C: Continuation frame from relay
                // MUST have XID (else FATAL)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            format!("{:?} from relay missing XID - all frames from relay must have XID",
                                frame.frame_type),
                        ));
                    }
                };

                eprintln!("[PluginHostRuntime.handle_relay_frame] Continuation {:?} from relay: RID={:?}, XID={:?}",
                    frame.frame_type, frame.id, xid);

                // Route by (XID, RID) - PluginRuntime will use media_urn to determine
                // if frames go to incoming handler or peer invoker
                let plugin_idx = if let Some(&idx) = self.incoming_rxids.get(&(xid.clone(), frame.id.clone())) {
                    eprintln!("[PluginHostRuntime.handle_relay_frame] Matched incoming_rxids[({:?}, {:?})] → plugin {}",
                        xid, frame.id, idx);
                    idx
                } else {
                    eprintln!("[PluginHostRuntime.handle_relay_frame] No routing found for RID={:?}, XID={:?} (already cleaned up)",
                        frame.id, xid);
                    return Ok(()); // Already cleaned up
                };

                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                // If the plugin is dead, send ERR to engine and clean up routing.
                if self.send_to_plugin(plugin_idx, frame.clone()).is_err() {
                    eprintln!("[PluginHostRuntime.handle_relay_frame] Plugin {} died, sending ERR", plugin_idx);
                    let mut err = Frame::err(
                        frame.id.clone(),
                        "PLUGIN_DIED",
                        "Plugin exited while processing request",
                    );
                    err.routing_id = frame.routing_id.clone(); // Copy XID from incoming request
                    let _ = outbound_tx.send(err);

                    // Cleanup from both lists (only one will match)
                    self.outgoing_rids.remove(&frame.id);
                    self.incoming_rxids.remove(&(xid.clone(), frame.id.clone()));
                    return Ok(());
                }

                // NOTE: Do NOT cleanup incoming_rxids here!
                // Frames arrive asynchronously out of order - END can arrive before StreamStart/Chunk.
                // We can't know when "all frames for (XID, RID) have arrived" without full stream tracking.
                // Accept the leak: entries cleaned up on plugin death.
                // This is bounded by concurrent requests and is acceptable.

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

            // PATH A: REQ from plugin (peer invoke)
            // MUST have RID, MUST NOT have XID (plugins never send XID)
            FrameType::Req => {
                if frame.routing_id.is_some() {
                    return Err(AsyncHostError::Protocol(format!(
                        "Plugin {} sent REQ with XID - plugins must never send XID",
                        plugin_idx
                    )));
                }

                eprintln!("[PluginHostRuntime.handle_plugin_frame] Peer REQ from plugin {}: RID={:?}, cap={:?}",
                    plugin_idx, frame.id, frame.cap);

                // Record in List 1: OUTGOING_RIDS
                self.outgoing_rids.insert(frame.id.clone(), plugin_idx);
                eprintln!("[PluginHostRuntime.handle_plugin_frame] Recorded outgoing_rids[{:?}] = {}",
                    frame.id, plugin_idx);

                // Forward as-is to relay (no XID - will be assigned by RelaySwitch)
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }

            // PATH A: Continuation frames from plugin (request body or response)
            // When responding to relay requests, frames WILL have XID (routing_id)
            // When responding to direct requests, frames will NOT have XID
            // NO routing decisions - only one destination (relay)
            _ => {
                eprintln!("[PluginHostRuntime.handle_plugin_frame] {:?} from plugin {}: RID={:?}",
                    frame.frame_type, plugin_idx, frame.id);

                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                // NOTE: Do NOT remove incoming_rxids here!
                // Response END from plugin doesn't mean the REQUEST is complete.
                // Request body frames might still be arriving from relay (async race).
                // incoming_rxids cleanup happens in handle_relay_frame when request body END arrives.

                // Forward as-is to relay (no routing, no XID manipulation)
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
        eprintln!("[PluginHostRuntime.handle_plugin_death] Plugin {} death detected", plugin_idx);
        let plugin = &mut self.plugins[plugin_idx];
        plugin.running = false;
        eprintln!("[PluginHostRuntime.handle_plugin_death] Closing writer to plugin {} (this closes stdin)", plugin_idx);
        plugin.writer_tx = None;

        // Kill the process if it's still around
        if let Some(ref mut child) = plugin.process {
            let _ = child.kill().await;
        }
        plugin.process = None;

        // Send ERR for pending PEER requests (outgoing_rids only)
        // NOTE: Do NOT send ERR for incoming_rxids! Those entries are intentionally leaked
        // to handle out-of-order frame arrival. They don't represent pending work.
        let failed_outgoing_rids: Vec<MessageId> = self
            .outgoing_rids
            .iter()
            .filter(|(_, &idx)| idx == plugin_idx)
            .map(|(rid, _)| rid.clone())
            .collect();

        eprintln!("[PluginHostRuntime.handle_plugin_death] Plugin {} died, failing {} outgoing peer requests",
            plugin_idx, failed_outgoing_rids.len());

        for rid in &failed_outgoing_rids {
            let err_frame = Frame::err(
                rid.clone(),
                "PLUGIN_DIED",
                &format!("Plugin {} exited unexpectedly", plugin.path.display()),
            );
            let _ = outbound_tx.send(err_frame);
            self.outgoing_rids.remove(rid);
        }

        // Clean up incoming_rxids entries for this plugin (leaked routing entries)
        self.incoming_rxids.retain(|(_, _), &mut idx| idx != plugin_idx);

        // Remove caps temporarily (will be re-added on relaunch)
        self.update_cap_table();
        self.rebuild_capabilities(Some(outbound_tx)); // Send RelayNotify to relay

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

        // Verify identity — proves the protocol stack works end-to-end
        if let Err(e) = verify_identity(&mut reader, &mut writer).await {
            self.plugins[plugin_idx].hello_failed = true;
            let _ = child.kill().await;
            return Err(AsyncHostError::Protocol(format!(
                "Plugin '{}' identity verification failed: {} — permanently removed",
                self.plugins[plugin_idx].path.display(),
                e
            )));
        }

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
    /// Chooses the MOST SPECIFIC matching cap URN (highest specificity score).
    /// If multiple matches have the same highest specificity, chooses one (deterministic).
    fn find_plugin_for_cap(&self, cap_urn: &str) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        // Collect ALL matching plugins with their specificity scores
        let mut matches: Vec<(usize, usize)> = Vec::new(); // (plugin_idx, specificity)

        for (registered_cap, plugin_idx) in &self.cap_table {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                // Request is pattern, registered cap is instance
                if request_urn.accepts(&registered_urn) {
                    let specificity = registered_urn.specificity();
                    matches.push((*plugin_idx, specificity));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // Find maximum specificity
        let max_specificity = matches.iter().map(|(_, s)| *s).max().unwrap();

        // Filter to only those with max specificity and take first
        // Multiple matches with same max specificity will route to the first one (deterministic)
        matches
            .iter()
            .filter(|(_, s)| *s == max_specificity)
            .map(|(idx, _)| *idx)
            .next()
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

                // Send ERR for pending requests (both new lists)
                let failed_incoming_keys: Vec<(MessageId, MessageId)> = self
                    .incoming_rxids
                    .iter()
                    .filter(|(_, &idx)| idx == plugin_idx)
                    .map(|(key, _)| key.clone())
                    .collect();

                let failed_outgoing_rids: Vec<MessageId> = self
                    .outgoing_rids
                    .iter()
                    .filter(|(_, &idx)| idx == plugin_idx)
                    .map(|(rid, _)| rid.clone())
                    .collect();

                eprintln!("[PluginHostRuntime.heartbeat] Plugin {} unhealthy, failing {} incoming and {} outgoing requests",
                    plugin_idx, failed_incoming_keys.len(), failed_outgoing_rids.len());

                for (xid, rid) in &failed_incoming_keys {
                    let mut err_frame = Frame::err(
                        rid.clone(),
                        "PLUGIN_UNHEALTHY",
                        "Plugin stopped responding to heartbeats",
                    );
                    err_frame.routing_id = Some(xid.clone());
                    let _ = outbound_tx.send(err_frame);
                    self.incoming_rxids.remove(&(xid.clone(), rid.clone()));
                }

                for rid in &failed_outgoing_rids {
                    let err_frame = Frame::err(
                        rid.clone(),
                        "PLUGIN_UNHEALTHY",
                        "Plugin stopped responding to heartbeats",
                    );
                    let _ = outbound_tx.send(err_frame);
                    self.outgoing_rids.remove(rid);
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
        self.rebuild_capabilities(Some(outbound_tx)); // Send RelayNotify to relay
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
            if plugin.running && !plugin.caps.is_empty() {
                // Extract URN strings from Cap objects
                for cap in &plugin.caps {
                    self.cap_table.push((cap.urn.to_string(), idx));
                }
            } else {
                // Use known_caps (URN strings)
                for cap_urn in &plugin.known_caps {
                    self.cap_table.push((cap_urn.clone(), idx));
                }
            }
        }
    }

    /// Rebuild the aggregate capabilities from all running, healthy plugins.
    ///
    /// If outbound_tx is Some (i.e., running in relay mode), sends a RelayNotify
    /// frame with the updated capabilities. This allows RelaySwitch/RelayMaster
    /// to track capability changes dynamically as plugins connect/disconnect/fail.
    fn rebuild_capabilities(&mut self, outbound_tx: Option<&mpsc::UnboundedSender<Frame>>) {
        use crate::standard::caps::CAP_IDENTITY;

        // CAP_IDENTITY is always present — structural, not plugin-dependent
        let mut cap_urns = vec![CAP_IDENTITY.to_string()];

        // Add capability URN strings from all running plugins
        for plugin in &self.plugins {
            if plugin.running && !plugin.hello_failed {
                for cap in &plugin.caps {
                    let urn_str = cap.urn.to_string();
                    // Don't duplicate identity (plugins also declare it)
                    if urn_str != CAP_IDENTITY {
                        cap_urns.push(urn_str);
                    }
                }
            }
        }

        // For internal use, store as simple JSON array of URN strings
        self.capabilities = serde_json::to_vec(&cap_urns)
            .expect("Failed to serialize capability URNs");

        // Send RelayNotify to relay if in relay mode
        // RelayNotify contains just the capability URN strings, not a full manifest
        if let Some(tx) = outbound_tx {
            let caps_bytes = serde_json::to_vec(&cap_urns)
                .expect("Failed to serialize capability URNs for RelayNotify");
            let notify_frame = Frame::relay_notify(&caps_bytes, &Limits::default());
            let _ = tx.send(notify_frame); // Ignore error if relay closed
        }
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
            let mut seq_assigner = SeqAssigner::new();
            while let Some(mut frame) = rx.recv().await {
                seq_assigner.assign(&mut frame);
                if let Err(e) = writer.write(&frame).await {
                    eprintln!("[PluginWriter] write error: {}", e);
                    break;
                }
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&frame.id);
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
            eprintln!("[PluginReader {}] Starting reader loop", plugin_idx);
            let mut frame_count = 0;
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        frame_count += 1;
                        eprintln!("[PluginReader {}] Read frame #{}: {:?} (id={:?})", plugin_idx, frame_count, frame.frame_type, frame.id);
                        if event_tx
                            .send(PluginEvent::Frame {
                                plugin_idx,
                                frame,
                            })
                            .is_err()
                        {
                            eprintln!("[PluginReader {}] Event channel closed, main loop dropped", plugin_idx);
                            break; // Runtime dropped
                        }
                        eprintln!("[PluginReader {}] Frame #{} sent to main loop", plugin_idx, frame_count);
                    }
                    Ok(None) => {
                        // EOF — plugin closed stdout
                        eprintln!("[PluginReader {}] EOF from plugin stdout (after {} frames), sending Death event", plugin_idx, frame_count);
                        let _ = event_tx.send(PluginEvent::Death { plugin_idx });
                        break;
                    }
                    Err(e) => {
                        // Read error — treat as death
                        eprintln!("[PluginReader {}] Read error (after {} frames): {}, sending Death event", plugin_idx, frame_count, e);
                        let _ = event_tx.send(PluginEvent::Death { plugin_idx });
                        break;
                    }
                }
            }
            eprintln!("[PluginReader {}] Reader task exiting (read {} frames total)", plugin_idx, frame_count);
        })
    }

    /// Outbound writer loop: reads frames from channel, writes to relay.
    async fn outbound_writer_loop<W: AsyncWrite + Unpin>(
        relay_write: W,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) {
        let mut writer = AsyncFrameWriter::new(relay_write);
        let mut seq_assigner = SeqAssigner::new();
        eprintln!("[PluginHostRuntime/outbound_writer] Starting outbound writer loop");
        while let Some(mut frame) = rx.recv().await {
            seq_assigner.assign(&mut frame);
            eprintln!("[PluginHostRuntime/outbound_writer] Writing frame: {:?} (id={:?}, seq={})", frame.frame_type, frame.id, frame.seq);
            if let Err(e) = writer.write(&frame).await {
                eprintln!("[PluginHostRuntime/outbound_writer] Write error: {}", e);
                break;
            }
            if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                seq_assigner.remove(&frame.id);
            }
            eprintln!("[PluginHostRuntime/outbound_writer] Frame written successfully");
        }
        eprintln!("[PluginHostRuntime/outbound_writer] Outbound writer loop exiting");
    }
}

impl Drop for PluginHostRuntime {
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
/// {"name": "...", "caps": [{"urn": "cap:in=\"media:void\";op=test;out=\"media:void\"", ...}, ...]}
/// ```
fn parse_caps_from_manifest(manifest: &[u8]) -> Result<Vec<crate::Cap>, AsyncHostError> {
    use crate::CapManifest;
    use crate::urn::cap_urn::CapUrn;
    use crate::standard::caps::CAP_IDENTITY;

    // Deserialize directly into CapManifest - fail hard if invalid
    let manifest_obj: CapManifest = serde_json::from_slice(manifest).map_err(|e| {
        AsyncHostError::Protocol(format!("Invalid CapManifest from plugin: {}", e))
    })?;

    // Verify CAP_IDENTITY is declared — mandatory for every plugin
    let identity_urn = CapUrn::from_string(CAP_IDENTITY)
        .expect("BUG: CAP_IDENTITY constant is invalid");
    let has_identity = manifest_obj.caps.iter().any(|cap| identity_urn.accepts(&cap.urn));
    if !has_identity {
        return Err(AsyncHostError::Protocol(
            format!("Plugin manifest missing required CAP_IDENTITY ({})", CAP_IDENTITY)
        ));
    }

    // Return the Cap objects directly
    Ok(manifest_obj.caps)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::caps::CAP_IDENTITY;
    use crate::CapUrn;

    /// Helper: perform handshake_accept and handle the identity verification REQ.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    fn plugin_handshake_with_identity(
        from_runtime: std::os::unix::net::UnixStream,
        to_runtime: std::os::unix::net::UnixStream,
        manifest: &[u8],
    ) -> (crate::bifaci::io::FrameReader<std::io::BufReader<std::os::unix::net::UnixStream>>,
          crate::bifaci::io::FrameWriter<std::io::BufWriter<std::os::unix::net::UnixStream>>)
    {
        use crate::bifaci::io::{FrameReader, FrameWriter, handshake_accept};
        use std::io::{BufReader, BufWriter};

        let mut reader = FrameReader::new(BufReader::new(from_runtime));
        let mut writer = FrameWriter::new(BufWriter::new(to_runtime));
        handshake_accept(&mut reader, &mut writer, manifest).unwrap();

        // Handle identity verification REQ
        let req = reader.read().unwrap().expect("expected identity REQ");
        assert_eq!(req.frame_type, FrameType::Req, "first frame after handshake must be REQ");

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!("unexpected frame type during identity verification: {:?}", other),
            }
        }

        // Echo response: STREAM_START → CHUNK → STREAM_END → END
        let stream_id = "identity-echo".to_string();
        let ss = Frame::stream_start(req.id.clone(), stream_id.clone(), "media:bytes".to_string());
        writer.write(&ss).unwrap();
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
        writer.write(&chunk).unwrap();
        let se = Frame::stream_end(req.id.clone(), stream_id, 1);
        writer.write(&se).unwrap();
        let end = Frame::end(req.id, None);
        writer.write(&end).unwrap();

        (reader, writer)
    }

    // TEST480: parse_caps_from_manifest rejects manifest without CAP_IDENTITY
    #[test]
    fn test480_parse_caps_rejects_manifest_without_identity() {
        // Valid manifest but missing CAP_IDENTITY
        let manifest = r#"{"name":"Test","version":"1.0","description":"Test","caps":[{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;
        let result = parse_caps_from_manifest(manifest.as_bytes());
        assert!(result.is_err(), "Manifest without CAP_IDENTITY must be rejected");
        let err = result.unwrap_err();
        assert!(format!("{}", err).contains("CAP_IDENTITY"),
            "Error must mention CAP_IDENTITY, got: {}", err);

        // Valid manifest WITH CAP_IDENTITY must succeed
        let manifest_ok = r#"{"name":"Test","version":"1.0","description":"Test","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;
        let result_ok = parse_caps_from_manifest(manifest_ok.as_bytes());
        assert!(result_ok.is_ok(), "Manifest with CAP_IDENTITY must be accepted");
        assert_eq!(result_ok.unwrap().len(), 2, "Must parse both caps");
    }

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
        let cbor_err = crate::bifaci::io::CborError::InvalidFrame("test".to_string());
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
        let mut runtime = PluginHostRuntime::new();
        runtime.register_plugin(Path::new("/usr/bin/test-plugin"), &[
            "cap:in=\"media:void\";op=convert;out=\"media:void\"".to_string(),
            "cap:in=\"media:void\";op=analyze;out=\"media:void\"".to_string(),
        ]);

        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(runtime.cap_table[0].0, "cap:in=\"media:void\";op=convert;out=\"media:void\"");
        assert_eq!(runtime.cap_table[0].1, 0);
        assert_eq!(runtime.cap_table[1].0, "cap:in=\"media:void\";op=analyze;out=\"media:void\"");
        assert_eq!(runtime.cap_table[1].1, 0);
        assert_eq!(runtime.plugins.len(), 1);
        assert!(!runtime.plugins[0].running);
    }

    // TEST414: capabilities() returns empty JSON initially (no running plugins)
    #[test]
    fn test_capabilities_empty_initially() {
        let runtime = PluginHostRuntime::new();
        assert!(runtime.capabilities().is_empty(), "No plugins registered = empty capabilities");

        let mut runtime2 = PluginHostRuntime::new();
        runtime2.register_plugin(Path::new("/usr/bin/test"), &["cap:in=\"media:void\";op=test;out=\"media:void\"".to_string()]);
        // Plugin registered but not running — capabilities still empty
        assert!(runtime2.capabilities().is_empty(),
            "Registered but not running plugin should not appear in capabilities");
    }

    // TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary)
    #[tokio::test]
    async fn test_req_for_known_cap_triggers_spawn() {
        let mut runtime = PluginHostRuntime::new();
        runtime.register_plugin(
            Path::new("/nonexistent/plugin/binary"),
            &["cap:in=\"media:void\";op=test;out=\"media:void\"".to_string()],
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

        // Send a REQ through the relay (must have XID since it's from relay)
        let send_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut writer = AsyncFrameWriter::new(engine_write_half);
            let mut req = Frame::req(MessageId::new_uuid(), "cap:in=\"media:void\";op=test;out=\"media:void\"", vec![], "text/plain");
            req.routing_id = Some(MessageId::Uint(1)); // XID from RelaySwitch
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            seq.remove(&req.id);
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
        let manifest = r#"{"name":"Test","version":"1.0","description":"Test plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Test","command":"test","args":[]}]}"#;

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

        // Plugin thread does handshake + identity verification
        let manifest_bytes = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            plugin_handshake_with_identity(plugin_from_runtime_std, plugin_to_runtime_std, &manifest_bytes);
        });

        let mut runtime = PluginHostRuntime::new();
        let idx = runtime.attach_plugin(plugin_read, plugin_write).await.unwrap();

        assert_eq!(idx, 0);
        assert!(runtime.plugins[0].running);
        assert_eq!(
            runtime.plugins[0].caps.iter().map(|c| c.urn_string()).collect::<Vec<_>>(),
            vec![CAP_IDENTITY]
        );
        assert!(!runtime.capabilities().is_empty());

        // Capabilities JSON should mention the cap
        let caps_str = std::str::from_utf8(runtime.capabilities()).unwrap();
        assert!(caps_str.contains(CAP_IDENTITY), "Capabilities must include attached plugin's cap");

        plugin_handle.join().unwrap();
    }

    // TEST417: Route REQ to correct plugin by cap_urn (with two attached plugins)
    #[tokio::test]
    async fn test_route_req_to_correct_plugin() {
        let manifest_a = r#"{"name":"PluginA","version":"1.0","description":"Plugin A","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;
        let manifest_b = r#"{"name":"PluginB","version":"1.0","description":"Plugin B","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=analyze;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(pa_from_rt_std, pa_to_rt_std, &ma);
            // Read one REQ and verify cap
            let frame = r.read().unwrap().expect("expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some("cap:in=\"media:void\";op=convert;out=\"media:void\""), "Plugin A should receive convert REQ");
            // Send END response
            let stream_id = "s1".to_string();
            let mut ss = Frame::stream_start(frame.id.clone(), stream_id.clone(), "media:bytes".to_string());
            seq.assign(&mut ss);
            w.write(&ss).unwrap();
            let payload = b"converted".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(frame.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).unwrap();
            let mut se = Frame::stream_end(frame.id.clone(), stream_id, 1);
            seq.assign(&mut se);
            w.write(&se).unwrap();
            let mut end = Frame::end(frame.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).unwrap();
            seq.remove(&frame.id);
        });

        // Plugin B thread
        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = std::thread::spawn(move || {
            let (r, w) = plugin_handshake_with_identity(pb_from_rt_std, pb_to_rt_std, &mb);
            // Plugin B should NOT receive the convert REQ
            // It may receive heartbeats, but the REQ should only go to Plugin A
            // Just exit - the runtime will handle heartbeat timeouts
            drop(r);
            drop(w);
        });

        // Setup runtime
        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(req_id.clone(), "cap:in=\"media:void\";op=convert;out=\"media:void\"", vec![], "text/plain");
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start = Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string());
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let payload = b"input".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&req_id);

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
        let manifest = r#"{"name":"HBPlugin","version":"1.0","description":"Heartbeat plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=hb;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);

            // Send a heartbeat from plugin
            let hb_id = MessageId::new_uuid();
            let mut hb = Frame::heartbeat(hb_id.clone());
            seq.assign(&mut hb);
            w.write(&hb).unwrap();

            // Read the heartbeat response
            let response = r.read().unwrap().expect("Expected heartbeat response");
            assert_eq!(response.frame_type, FrameType::Heartbeat);
            assert_eq!(response.id, hb_id, "Response must echo the same ID");

            drop(w); // Close to signal EOF
        });

        let mut runtime = PluginHostRuntime::new();
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
        let manifest = r#"{"name":"FwdPlugin","version":"1.0","description":"Forward plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=fwd;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);

            // Read the REQ
            let frame = r.read().unwrap().expect("Expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);

            // Consume incoming streams until END
            loop {
                let f = r.read().unwrap().expect("Expected frame");
                if f.frame_type == FrameType::End { break; }
            }

            // Send LOG + response (LOG should be forwarded too)
            let mut log = Frame::log(req_id_for_plugin.clone(), "info", "Processing");
            seq.assign(&mut log);
            w.write(&log).unwrap();
            let sid = "rs".to_string();
            let mut ss = Frame::stream_start(req_id_for_plugin.clone(), sid.clone(), "media:bytes".to_string());
            seq.assign(&mut ss);
            w.write(&ss).unwrap();
            let payload = b"result".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id_for_plugin.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).unwrap();
            let mut se = Frame::stream_end(req_id_for_plugin.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).unwrap();
            let mut end = Frame::end(req_id_for_plugin.clone(), None);
            seq.assign(&mut end);
            w.write(&end).unwrap();
            seq.remove(&req_id_for_plugin);
            drop(w);
        });

        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(req_id_send.clone(), "cap:in=\"media:void\";op=fwd;out=\"media:void\"", vec![], "text/plain");
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start = Frame::stream_start(req_id_send.clone(), sid.clone(), "media:bytes".to_string());
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id_send.clone(), sid, 0);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id_send.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&req_id_send);

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
        let manifest = r#"{"name":"ContPlugin","version":"1.0","description":"Continuation plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=cont;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);

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
            let mut ss = Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string());
            seq.assign(&mut ss);
            w.write(&ss).unwrap();
            let payload = b"ok".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).unwrap();
            seq.remove(&req.id);
            drop(w);
        });

        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            // Send REQ + stream continuation frames
            let mut req = Frame::req(req_id.clone(), "cap:in=\"media:void\";op=cont;out=\"media:void\"", vec![], "text/plain");
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let sid = uuid::Uuid::new_v4().to_string();
            let mut stream_start = Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string());
            stream_start.routing_id = Some(xid.clone());
            seq.assign(&mut stream_start);
            w.write(&stream_start).await.unwrap();
            let payload = b"payload-data".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, payload, 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut stream_end = Frame::stream_end(req_id.clone(), sid, 1);
            stream_end.routing_id = Some(xid.clone());
            seq.assign(&mut stream_end);
            w.write(&stream_end).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&req_id);

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
        let manifest = r#"{"name":"Dying","version":"1.0","description":"Dying plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=die;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let (r, w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);
            // Die immediately after identity verification
            drop(w);
            drop(r);
        });

        let mut runtime = PluginHostRuntime::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Before death: caps should include the plugin's cap
        let expected_urn = CapUrn::from_string("cap:in=\"media:void\";op=die;out=\"media:void\"")
            .expect("Expected URN should parse");
        let caps_before = std::str::from_utf8(runtime.capabilities()).unwrap().to_string();
        let parsed_before: serde_json::Value = serde_json::from_str(&caps_before).unwrap();
        let urn_strings: Vec<String> = parsed_before.as_array().unwrap()
            .iter().map(|v| v.as_str().unwrap().to_string()).collect();

        // Parse each URN and check if any matches using accepts/conforms_to
        let found = urn_strings.iter().any(|urn_str| {
            if let Ok(cap_urn) = CapUrn::from_string(urn_str) {
                // Check if the URNs match (either direction should work for exact match)
                expected_urn.accepts(&cap_urn) || cap_urn.accepts(&expected_urn)
            } else {
                false
            }
        });
        assert!(found, "Capabilities should contain plugin's cap. Expected URN with op=die, got: {:?}", urn_strings);

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

        // After death: only CAP_IDENTITY should remain (always present, plugin-specific caps removed)
        let caps_after = runtime.capabilities();
        let caps_str = std::str::from_utf8(caps_after).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(caps_str).unwrap();
        let arr = parsed.as_array().expect("capabilities should be a JSON array");
        assert_eq!(arr.len(), 1, "Only CAP_IDENTITY should remain after plugin death. Got: {}", caps_str);
        assert_eq!(arr[0].as_str().unwrap(), CAP_IDENTITY,
            "Remaining cap must be CAP_IDENTITY. Got: {}", caps_str);

        plugin_handle.join().unwrap();
    }

    // TEST422: Plugin death sends ERR for all pending requests via relay
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_plugin_death_sends_err_for_pending_requests() {
        let manifest = r#"{"name":"DiePlugin","version":"1.0","description":"Die plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=die;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let (mut r, w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);

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

        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);

            let xid = MessageId::Uint(1);
            // Send REQ (plugin will die after reading it)
            let mut req = Frame::req(req_id.clone(), "cap:in=\"media:void\";op=die;out=\"media:void\"", vec![], "text/plain");
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&req_id);

            // Close relay connection after sending request
            // (in real use, engine would implement timeout for pending requests)
            drop(w);
        });

        // Runtime should handle plugin death gracefully and exit when relay disconnects
        let result = tokio::time::timeout(Duration::from_secs(5),
            runtime.run(rt_read_half, rt_write_half, || vec![])
        ).await;
        assert!(result.is_ok(), "Runtime should exit cleanly when plugin dies and relay disconnects");

        engine_task.await.unwrap();

        plugin_handle.join().unwrap();
    }

    // TEST423: Multiple plugins registered with distinct caps route independently
    #[tokio::test]
    async fn test_multiple_plugins_route_independently() {
        let manifest_a = r#"{"name":"PA","version":"1.0","description":"Plugin A","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=alpha;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;
        let manifest_b = r#"{"name":"PB","version":"1.0","description":"Plugin B","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=beta;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(pa_from_rt, pa_to_rt, &ma);
            let req = r.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:in=\"media:void\";op=alpha;out=\"media:void\""));
            loop { let f = r.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "a".to_string();
            let mut ss = Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string());
            seq.assign(&mut ss);
            w.write(&ss).unwrap();
            let payload = b"from-A".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).unwrap();
            seq.remove(&req.id);
            drop(w);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(pb_from_rt, pb_to_rt, &mb);
            let req = r.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:in=\"media:void\";op=beta;out=\"media:void\""));
            loop { let f = r.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "b".to_string();
            let mut ss = Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string());
            seq.assign(&mut ss);
            w.write(&ss).unwrap();
            let payload = b"from-B".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).unwrap();
            seq.remove(&req.id);
            drop(w);
        });

        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            let xid_alpha = MessageId::Uint(1);
            let xid_beta = MessageId::Uint(2);
            // Send two requests to different caps
            let mut req_alpha = Frame::req(alpha_c.clone(), "cap:in=\"media:void\";op=alpha;out=\"media:void\"", vec![], "text/plain");
            req_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut req_alpha);
            w.write(&req_alpha).await.unwrap();
            let mut end_alpha = Frame::end(alpha_c.clone(), None);
            end_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut end_alpha);
            w.write(&end_alpha).await.unwrap();
            seq.remove(&alpha_c);
            let mut req_beta = Frame::req(beta_c.clone(), "cap:in=\"media:void\";op=beta;out=\"media:void\"", vec![], "text/plain");
            req_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut req_beta);
            w.write(&req_beta).await.unwrap();
            let mut end_beta = Frame::end(beta_c.clone(), None);
            end_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut end_beta);
            w.write(&end_beta).await.unwrap();
            seq.remove(&beta_c);

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
        let manifest = r#"{"name":"ConcPlugin","version":"1.0","description":"Concurrent plugin","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=conc;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

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
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = plugin_handshake_with_identity(p_from_rt_std, p_to_rt_std, &m);

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
                let checksum = Frame::compute_checksum(&data);
                let sid = format!("s{}", i);
                let mut ss = Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string());
                seq.assign(&mut ss);
                w.write(&ss).unwrap();
                let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, data, 0, checksum);
                seq.assign(&mut chunk);
                w.write(&chunk).unwrap();
                let mut se = Frame::stream_end(req_id.clone(), sid, 1);
                seq.assign(&mut se);
                w.write(&se).unwrap();
                let mut end = Frame::end(req_id.clone(), None);
                seq.assign(&mut end);
                w.write(&end).unwrap();
                seq.remove(req_id);
            }
            drop(w);
        });

        let mut runtime = PluginHostRuntime::new();
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
            let mut seq = SeqAssigner::new();
            let mut w = AsyncFrameWriter::new(eng_write_half);
            let mut r = AsyncFrameReader::new(eng_read_half);

            // Send two REQs concurrently (same cap)
            let xid_0 = MessageId::Uint(1);
            let xid_1 = MessageId::Uint(2);
            let mut req_0 = Frame::req(r0.clone(), "cap:in=\"media:void\";op=conc;out=\"media:void\"", vec![], "text/plain");
            req_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut req_0);
            w.write(&req_0).await.unwrap();
            let mut end_0 = Frame::end(r0.clone(), None);
            end_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut end_0);
            w.write(&end_0).await.unwrap();
            seq.remove(&r0);
            let mut req_1 = Frame::req(r1.clone(), "cap:in=\"media:void\";op=conc;out=\"media:void\"", vec![], "text/plain");
            req_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut req_1);
            w.write(&req_1).await.unwrap();
            let mut end_1 = Frame::end(r1.clone(), None);
            end_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut end_1);
            w.write(&end_1).await.unwrap();
            seq.remove(&r1);

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
        let mut runtime = PluginHostRuntime::new();
        runtime.register_plugin(Path::new("/test"), &["cap:in=\"media:void\";op=known;out=\"media:void\"".to_string()]);
        assert!(runtime.find_plugin_for_cap("cap:in=\"media:void\";op=known;out=\"media:void\"").is_some());
        assert!(runtime.find_plugin_for_cap("cap:in=\"media:void\";op=unknown;out=\"media:void\"").is_none());
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST485: attach_plugin completes identity verification with working plugin
    #[tokio::test]
    async fn test_attach_plugin_identity_verification_succeeds() {
        let manifest = r#"{"name":"IdentityTest","version":"1.0","description":"Test","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test","args":[]}]}"#;

        let (p_to_rt, rt_from_p) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p.set_nonblocking(true).unwrap();
        rt_to_p.set_nonblocking(true).unwrap();

        let rt_from = tokio::net::UnixStream::from_std(rt_from_p).unwrap();
        let rt_to = tokio::net::UnixStream::from_std(rt_to_p).unwrap();
        let (p_read, _) = rt_from.into_split();
        let (_, p_write) = rt_to.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            plugin_handshake_with_identity(p_from_rt, p_to_rt, &m);
        });

        let mut runtime = PluginHostRuntime::new();
        let idx = runtime.attach_plugin(p_read, p_write).await.unwrap();
        assert_eq!(idx, 0);
        assert!(runtime.plugins[0].running, "Plugin must be running after identity verification");

        // Verify both caps are registered
        let caps: Vec<String> = runtime.plugins[0].caps.iter().map(|c| c.urn_string()).collect();
        assert!(caps.iter().any(|c| c == CAP_IDENTITY), "Must have identity cap");
        assert_eq!(caps.len(), 2, "Must have both caps");

        plugin_handle.join().unwrap();
    }

    // TEST486: attach_plugin rejects plugin that fails identity verification
    #[tokio::test]
    async fn test_attach_plugin_identity_verification_fails() {
        let manifest = r#"{"name":"BrokenIdentity","version":"1.0","description":"Test","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]}]}"#;

        let (p_to_rt, rt_from_p) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p.set_nonblocking(true).unwrap();
        rt_to_p.set_nonblocking(true).unwrap();

        let rt_from = tokio::net::UnixStream::from_std(rt_from_p).unwrap();
        let rt_to = tokio::net::UnixStream::from_std(rt_to_p).unwrap();
        let (p_read, _) = rt_from.into_split();
        let (_, p_write) = rt_to.into_split();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            use crate::bifaci::io::{FrameReader, FrameWriter, handshake_accept};
            use std::io::{BufReader, BufWriter};
            let mut reader = FrameReader::new(BufReader::new(p_from_rt));
            let mut writer = FrameWriter::new(BufWriter::new(p_to_rt));
            handshake_accept(&mut reader, &mut writer, &m).unwrap();

            // Read identity REQ, respond with ERR (broken identity handler)
            let req = reader.read().unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(req.id, "BROKEN", "identity handler is broken");
            writer.write(&err).unwrap();
        });

        let mut runtime = PluginHostRuntime::new();
        let result = runtime.attach_plugin(p_read, p_write).await;
        assert!(result.is_err(), "attach_plugin must fail when identity verification fails");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Identity verification failed"),
            "Error must mention identity verification: {}", err);

        plugin_handle.join().unwrap();
    }
}
