//! RelaySwitch — Cap-aware routing multiplexer for multiple RelayMasters
//!
//! The RelaySwitch sits above multiple RelayMasters and provides deterministic
//! request routing based on cap URN matching. It plays the same role for RelayMasters
//! that PluginHost plays for plugins.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────┐
//! │   Test Engine / API Client  │
//! └──────────────┬──────────────┘
//!                │
//! ┌──────────────▼──────────────┐
//! │       RelaySwitch            │
//! │  • Aggregates capabilities   │
//! │  • Routes REQ by cap URN     │
//! │  • Routes frames by req_id   │
//! │  • Tracks peer requests      │
//! └─┬───┬───┬───┬───────────────┘
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  RM  RM  RM  RM   (RelayMasters)
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  RS  RS  RS  RS   (RelaySlaves)
//!   │   │   │   │
//!   ▼   ▼   ▼   ▼
//!  PH  PH  PH  PH   (PluginHosts)
//! ```
//!
//! ## Routing Rules
//!
//! **Engine → Plugin**:
//! - REQ: route by cap URN using `is_dispatchable` + closest-specificity matching
//! - Continuation frames: route by req_id
//!
//! **Plugin → Plugin** (peer invocations):
//! - REQ from master: route to destination master (may be same or different)
//! - Mark in peer_requests set (special cleanup semantics)
//! - Response frames: route back to source master
//!
//! **Cleanup**:
//! - Engine-initiated: plugin's END → cleanup immediately
//! - Peer-initiated: engine's response END → cleanup (wait for final response)
//!
//! ## Concurrency Model
//!
//! RelaySwitch uses interior mutability to allow concurrent DAG executions:
//! - All methods take `&self` (not `&mut self`)
//! - xid_counter: AtomicU64
//! - Routing maps: RwLock<HashMap<...>>
//! - Per-master socket writers: Mutex<FrameWriter<...>>
//! - Each execute_fanin spawns its own pump that cooperatively routes frames

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter, identity_nonce};
use crate::cap::registry::CapRegistry;
use crate::planner::live_cap_graph::{LiveCapGraph, ReachableTargetInfo, Strand};
use crate::urn::media_urn::MediaUrn;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info, warn};

// =============================================================================
// ERROR TYPES
// =============================================================================

/// Errors that can occur in the relay switch.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RelaySwitchError {
    #[error("CBOR error: {0}")]
    Cbor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("No handler found for cap: {0}")]
    NoHandler(String),

    #[error("Unknown request ID: {0:?}")]
    UnknownRequest(MessageId),

    #[error("Protocol violation: {0}")]
    Protocol(String),

    #[error("All masters are unhealthy")]
    AllMastersUnhealthy,
}

impl From<CborError> for RelaySwitchError {
    fn from(e: CborError) -> Self {
        RelaySwitchError::Cbor(e.to_string())
    }
}

impl From<std::io::Error> for RelaySwitchError {
    fn from(e: std::io::Error) -> Self {
        RelaySwitchError::Io(e.to_string())
    }
}

// =============================================================================
// DATA STRUCTURES
// =============================================================================

/// Routing entry tracking request source and destination.
#[derive(Debug, Clone)]
struct RoutingEntry {
    /// Source master index, or None if from external caller (execute_cap)
    source_master_idx: Option<usize>,
    /// Destination master index (where request is being handled)
    destination_master_idx: usize,
}

/// Health status snapshot for a single master connection.
/// Returned by `RelaySwitch::get_master_health()` for monitoring.
#[derive(Debug, Clone)]
pub struct MasterHealthStatus {
    /// Master index (0-based)
    pub index: usize,
    /// Whether the master is currently healthy
    pub healthy: bool,
    /// Number of capabilities registered by this master
    pub cap_count: usize,
    /// Time when this master was connected (seconds since connection)
    pub connected_seconds: u64,
    /// Last error that caused unhealthy state (if any)
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct InstalledPluginIdentity {
    pub id: String,
    pub version: String,
    pub sha256: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RelayNotifyCapabilitiesPayload {
    caps: Vec<String>,
    installed_plugins: Vec<InstalledPluginIdentity>,
}

/// Connection to a single RelayMaster with its socket I/O.
/// Interior mutability: writer and seq_assigner are behind Mutex.
struct MasterConnection {
    /// Writer for frames to slave (Mutex for concurrent access)
    socket_writer: Mutex<FrameWriter<BufWriter<OwnedWriteHalf>>>,
    /// Seq assigner for frames written to this master (Mutex for concurrent access)
    seq_assigner: Mutex<SeqAssigner>,
    /// Latest manifest from RelayNotify
    manifest: RwLock<Vec<u8>>,
    /// Latest limits from RelayNotify
    limits: RwLock<Limits>,
    /// Parsed capability URNs from manifest
    caps: RwLock<Vec<String>>,
    /// Installed plugin identities reported by this master
    installed_plugins: RwLock<Vec<InstalledPluginIdentity>>,
    /// Connection health status
    healthy: AtomicBool,
    /// Reader task handle
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Time when this master was connected
    connected_at: Instant,
    /// Last error message (if unhealthy)
    last_error: RwLock<Option<String>>,
}

impl std::fmt::Debug for MasterConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterConnection")
            .field("healthy", &self.healthy.load(Ordering::SeqCst))
            .finish()
    }
}

/// RelaySwitch — Cap-aware routing multiplexer for multiple RelayMasters.
///
/// Aggregates capabilities from multiple RelayMasters and routes requests
/// based on cap URN matching. Handles both engine→plugin and plugin→plugin
/// (peer) invocations with correct routing semantics.
///
/// Uses interior mutability for concurrent DAG execution support.
/// All public methods take `&self`, not `&mut self`.
pub struct RelaySwitch {
    /// Managed relay master connections
    masters: RwLock<Vec<MasterConnection>>,
    /// Routing: cap_urn → master index
    cap_table: RwLock<Vec<(String, usize)>>,
    /// Routing: (xid, rid) → source/destination masters
    /// Only populated when XID is present (between RelaySwitch hops)
    request_routing: RwLock<HashMap<(MessageId, MessageId), RoutingEntry>>,
    /// Peer-initiated request (xid, rid) pairs for cleanup tracking
    peer_requests: RwLock<HashSet<(MessageId, MessageId)>>,
    /// Origin tracking: (xid, rid) → upstream connection index (None = external caller)
    /// Used to know where to send frames back
    origin_map: RwLock<HashMap<(MessageId, MessageId), Option<usize>>>,
    /// Response channels for external execute_cap calls: (xid, rid) → sender
    external_response_channels: RwLock<HashMap<(MessageId, MessageId), mpsc::UnboundedSender<Frame>>>,
    /// Aggregate capabilities (union of all masters)
    aggregate_capabilities: RwLock<Vec<u8>>,
    /// Aggregate installed plugin identities (union of all healthy masters)
    aggregate_installed_plugins: RwLock<Vec<InstalledPluginIdentity>>,
    /// Negotiated limits (minimum across all masters)
    negotiated_limits: RwLock<Limits>,
    /// Channel receiver for frames from master reader tasks (Mutex for exclusive receive)
    frame_rx: Mutex<mpsc::UnboundedReceiver<(usize, Result<Frame, CborError>)>>,
    /// Channel sender for spawning new reader tasks (stored for add_master)
    frame_tx: mpsc::UnboundedSender<(usize, Result<Frame, CborError>)>,
    /// XID counter for assigning unique routing IDs (RelaySwitch assigns on first arrival)
    xid_counter: AtomicU64,
    /// RID → XID mapping for engine-initiated requests (so continuation frames can find their XID)
    rid_to_xid: RwLock<HashMap<MessageId, MessageId>>,
    /// Precomputed capability graph for path finding and reachability queries
    live_cap_graph: RwLock<LiveCapGraph>,
    /// Cap registry for looking up Cap definitions
    cap_registry: Arc<CapRegistry>,
}

impl std::fmt::Debug for RelaySwitch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelaySwitch")
            .field("xid_counter", &self.xid_counter.load(Ordering::SeqCst))
            .finish()
    }
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl RelaySwitch {
    /// Create a new RelaySwitch with the given socket streams and cap registry.
    ///
    /// Each UnixStream is split into read/write halves internally.
    /// Performs handshake with all masters and builds initial capability table.
    ///
    /// The cap_registry is used to look up Cap definitions for building the
    /// LiveCapGraph for path finding and reachability queries.
    pub async fn new(sockets: Vec<UnixStream>, cap_registry: Arc<CapRegistry>) -> Result<Self, RelaySwitchError> {
        let mut masters = Vec::new();
        let (frame_tx, frame_rx) = mpsc::unbounded_channel();
        let xid_counter = AtomicU64::new(0);

        // Phase 1: For each master, read RelayNotify and verify identity.
        // Reader tasks are spawned only after verification succeeds.
        let mut pending_readers: Vec<(usize, FrameReader<BufReader<OwnedReadHalf>>)> = Vec::new();

        for (master_idx, socket) in sockets.into_iter().enumerate() {
            let (read_half, write_half) = socket.into_split();
            let mut socket_reader = FrameReader::new(BufReader::new(read_half));
            let mut socket_writer = FrameWriter::new(BufWriter::new(write_half));

            // Read RelayNotify (first frame from each master)
            let notify_frame = socket_reader.read().await
                .map_err(|e| RelaySwitchError::Cbor(format!("master {}: {}", master_idx, e)))?
                .ok_or_else(|| RelaySwitchError::Protocol(
                    format!("master {}: connection closed before RelayNotify", master_idx)
                ))?;

            if notify_frame.frame_type != FrameType::RelayNotify {
                return Err(RelaySwitchError::Protocol(format!(
                    "master {}: expected RelayNotify, got {:?}",
                    master_idx, notify_frame.frame_type
                )));
            }

            let mut caps_payload = notify_frame.relay_notify_manifest()
                .ok_or_else(|| RelaySwitchError::Protocol(
                    format!("master {}: RelayNotify has no manifest", master_idx)
                ))?
                .to_vec();

            let mut payload = parse_relay_notify_payload(&caps_payload)?;
            let mut caps = payload.caps.clone();
            let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

            // Verify identity through the relay chain.
            let mut seq_assigner = SeqAssigner::new();
            let xid_val = xid_counter.fetch_add(1, Ordering::SeqCst) + 1;
            let xid = MessageId::Uint(xid_val);
            {
                use crate::standard::caps::CAP_IDENTITY;

                let nonce = identity_nonce();
                let req_id = MessageId::new_uuid();
                let stream_id = "identity-verify".to_string();

                // Send REQ + STREAM_START + CHUNK + STREAM_END + END with XID + seq
                let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
                req.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut req);
                socket_writer.write(&req).await.map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut ss = Frame::stream_start(req_id.clone(), stream_id.clone(), "media:".to_string(), None);
                ss.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut ss);
                socket_writer.write(&ss).await.map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let checksum = Frame::compute_checksum(&nonce);
                let mut chunk = Frame::chunk(req_id.clone(), stream_id.clone(), 0, nonce.clone(), 0, checksum);
                chunk.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut chunk);
                socket_writer.write(&chunk).await.map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut se = Frame::stream_end(req_id.clone(), stream_id, 1);
                se.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut se);
                socket_writer.write(&se).await.map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut end = Frame::end(req_id.clone(), None);
                end.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut end);
                socket_writer.write(&end).await.map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                seq_assigner.remove(&FlowKey { rid: req_id.clone(), xid: Some(xid.clone()) });

                // Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
                let mut accumulated = Vec::new();
                loop {
                    let frame = socket_reader.read().await
                        .map_err(|e| RelaySwitchError::Protocol(format!(
                            "master {}: identity verification read failed: {}", master_idx, e
                        )))?
                        .ok_or_else(|| RelaySwitchError::Protocol(format!(
                            "master {}: connection closed during identity verification", master_idx
                        )))?;

                    match frame.frame_type {
                        FrameType::RelayNotify => {
                            // PluginHostRuntime sends the full RelayNotify (with all caps)
                            // through RelaySlave during identity verification. Update caps.
                            if let Some(manifest) = frame.relay_notify_manifest() {
                                caps_payload = manifest.to_vec();
                                payload = parse_relay_notify_payload(&caps_payload)?;
                                caps = payload.caps.clone();
                            }
                            if let Some(l) = frame.relay_notify_limits() {
                                limits = l;
                            }
                        }
                        FrameType::StreamStart => {}
                        FrameType::Chunk => {
                            if let Some(payload) = frame.payload {
                                accumulated.extend_from_slice(&payload);
                            }
                        }
                        FrameType::StreamEnd => {}
                        FrameType::End => {
                            if accumulated != nonce {
                                return Err(RelaySwitchError::Protocol(format!(
                                    "master {}: identity verification payload mismatch (expected {} bytes, got {})",
                                    master_idx, nonce.len(), accumulated.len()
                                )));
                            }
                            break;
                        }
                        FrameType::Err => {
                            let code = frame.error_code().unwrap_or("UNKNOWN");
                            let msg = frame.error_message().unwrap_or("no message");
                            return Err(RelaySwitchError::Protocol(format!(
                                "master {}: identity verification failed: [{code}] {msg}", master_idx
                            )));
                        }
                        other => {
                            return Err(RelaySwitchError::Protocol(format!(
                                "master {}: identity verification: unexpected frame type {:?}",
                                master_idx, other
                            )));
                        }
                    }
                }
            }

            // Stash reader for spawning after all masters are verified
            pending_readers.push((master_idx, socket_reader));

            masters.push(MasterConnection {
                socket_writer: Mutex::new(socket_writer),
                seq_assigner: Mutex::new(seq_assigner),
                manifest: RwLock::new(caps_payload),
                limits: RwLock::new(limits),
                caps: RwLock::new(caps),
                installed_plugins: RwLock::new(payload.installed_plugins),
                healthy: AtomicBool::new(true),
                reader_handle: None, // Spawned in phase 2
                connected_at: Instant::now(),
                last_error: RwLock::new(None),
            });
        }

        // Phase 2: All masters verified — spawn reader tasks
        for (master_idx, socket_reader) in pending_readers {
            let tx = frame_tx.clone();
            let reader_handle = tokio::spawn(async move {
                let mut reader = socket_reader;
                loop {
                    match reader.read().await {
                        Ok(Some(frame)) => {
                            tracing::debug!(
                                "[RelaySwitch] master {} reader: {:?} id={:?} xid={:?}",
                                master_idx, frame.frame_type, frame.id, frame.routing_id
                            );
                            if tx.send((master_idx, Ok(frame))).is_err() {
                                tracing::warn!("[RelaySwitch] master {} reader: frame_tx closed", master_idx);
                                break;
                            }
                        }
                        Ok(None) => {
                            tracing::warn!("[RelaySwitch] master {} reader: socket closed (EOF)", master_idx);
                            let _ = tx.send((master_idx, Err(CborError::UnexpectedEof)));
                            break;
                        }
                        Err(e) => {
                            tracing::error!("[RelaySwitch] master {} reader: socket error: {}", master_idx, e);
                            let _ = tx.send((master_idx, Err(e)));
                            break;
                        }
                    }
                }
            });
            masters[master_idx].reader_handle = Some(reader_handle);
        }

        let switch = Self {
            masters: RwLock::new(masters),
            cap_table: RwLock::new(Vec::new()),
            request_routing: RwLock::new(HashMap::new()),
            peer_requests: RwLock::new(HashSet::new()),
            origin_map: RwLock::new(HashMap::new()),
            external_response_channels: RwLock::new(HashMap::new()),
            aggregate_capabilities: RwLock::new(Vec::new()),
            aggregate_installed_plugins: RwLock::new(Vec::new()),
            negotiated_limits: RwLock::new(Limits::default()),
            frame_rx: Mutex::new(frame_rx),
            frame_tx,
            xid_counter,
            rid_to_xid: RwLock::new(HashMap::new()),
            live_cap_graph: RwLock::new(LiveCapGraph::new()),
            cap_registry,
        };

        // Build routing tables from already-populated caps
        switch.rebuild_cap_table().await;
        switch.rebuild_capabilities().await;
        switch.rebuild_limits().await;

        Ok(switch)
    }

    /// Get the aggregate capabilities of all healthy masters.
    pub async fn capabilities(&self) -> Vec<u8> {
        self.aggregate_capabilities.read().await.clone()
    }

    /// Get the aggregate installed plugins of all healthy masters.
    pub async fn installed_plugins(&self) -> Vec<InstalledPluginIdentity> {
        self.aggregate_installed_plugins.read().await.clone()
    }

    /// Get the negotiated limits (minimum across all masters).
    pub async fn limits(&self) -> Limits {
        self.negotiated_limits.read().await.clone()
    }

    /// Get all reachable targets from a source media URN.
    ///
    /// Uses the prebuilt LiveCapGraph for efficient BFS traversal.
    /// Results are sorted by (min_path_length, display_name).
    pub async fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        let graph = self.live_cap_graph.read().await;
        graph.get_reachable_targets(source, max_depth)
    }

    /// Find all paths from source to an exact target media URN.
    ///
    /// Uses `is_equivalent()` for target matching - "media:X" will NOT match
    /// paths ending in "media:X;list". This ensures the Transform menu and
    /// Path Selection dialog show consistent results.
    ///
    /// Results are sorted by (total_steps, specificity desc, cap_urns).
    pub async fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<Strand> {
        let graph = self.live_cap_graph.read().await;
        graph.find_paths_to_exact_target(source, target, max_depth, max_paths)
    }

    /// Get the cap registry used by this switch.
    pub fn cap_registry(&self) -> &Arc<CapRegistry> {
        &self.cap_registry
    }

    /// Get health status of all masters.
    ///
    /// Returns a snapshot of each master's health status including:
    /// - Whether the master is healthy
    /// - Number of capabilities it provides
    /// - How long it has been connected
    /// - Last error message if unhealthy
    pub async fn get_master_health(&self) -> Vec<MasterHealthStatus> {
        let masters = self.masters.read().await;
        let mut result = Vec::with_capacity(masters.len());
        for (idx, master) in masters.iter().enumerate() {
            let healthy = master.healthy.load(Ordering::SeqCst);
            let cap_count = master.caps.read().await.len();
            let connected_seconds = master.connected_at.elapsed().as_secs();
            let last_error = master.last_error.read().await.clone();
            result.push(MasterHealthStatus {
                index: idx,
                healthy,
                cap_count,
                connected_seconds,
                last_error,
            });
        }
        result
    }

    /// Get the total count of masters (healthy and unhealthy).
    pub async fn master_count(&self) -> usize {
        self.masters.read().await.len()
    }

    /// Get the count of healthy masters.
    pub async fn healthy_master_count(&self) -> usize {
        let masters = self.masters.read().await;
        masters.iter().filter(|m| m.healthy.load(Ordering::SeqCst)).count()
    }

    /// Execute a cap and return a receiver for streaming response frames.
    ///
    /// This is the high-level API for calling caps programmatically.
    /// The returned receiver will receive all response frames (STREAM_START, CHUNK, END, ERR, etc.)
    /// until the request completes.
    ///
    /// # Arguments
    /// * `cap_urn` - The capability URN to execute
    /// * `payload` - The request payload bytes
    /// * `content_type` - The content type of the payload (e.g., "application/cbor", "application/json")
    ///
    /// # Returns
    /// A tuple of (request_id, receiver). The request_id can be used with
    /// `send_to_master()` to send streaming continuation frames (STREAM_START,
    /// CHUNK, STREAM_END, END) for this request. The receiver streams response
    /// frames — read from it until END or ERR.
    pub async fn execute_cap(
        &self,
        cap_urn: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> Result<(MessageId, mpsc::UnboundedReceiver<Frame>), RelaySwitchError> {
        // Generate unique request ID
        let rid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);

        // Build REQ frame
        let req_frame = Frame::req(rid.clone(), cap_urn, payload, content_type);

        // Create response channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Find master that can handle this cap (no preference for internal requests)
        let dest_idx = self.find_master_for_cap(cap_urn, None).await.ok_or_else(|| {
            RelaySwitchError::NoHandler(cap_urn.to_string())
        })?;

        // Assign XID
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let key = (xid.clone(), rid.clone());

        // Register response channel BEFORE sending
        self.external_response_channels.write().await.insert(key.clone(), tx);

        // Record origin (None = external execute_cap caller)
        self.origin_map.write().await.insert(key.clone(), None);

        // Register routing
        self.request_routing.write().await.insert(
            key.clone(),
            RoutingEntry {
                source_master_idx: None,
                destination_master_idx: dest_idx,
            },
        );

        // Record RID → XID mapping for continuation frames (if caller sends them)
        self.rid_to_xid.write().await.insert(rid.clone(), xid.clone());

        // Build frame with XID
        let mut frame_with_xid = req_frame;
        frame_with_xid.routing_id = Some(xid);

        // Forward to destination
        tracing::info!(
            "[RelaySwitch] execute_cap: dispatching to master {} cap='{}' rid={:?} xid={:?}",
            dest_idx, cap_urn, rid, frame_with_xid.routing_id
        );
        self.write_to_master_idx(dest_idx, &mut frame_with_xid).await?;

        Ok((rid, rx))
    }

    /// Register an external request and return the assigned XID and response channel.
    ///
    /// This is used when the caller builds their own frames via `build_request_frames`.
    /// The caller must stamp the returned XID onto all frames (via `frame.routing_id = Some(xid)`)
    /// before sending them via `send_to_master`. The switch will route responses to the
    /// returned channel.
    ///
    /// Unlike `execute_cap`, this doesn't send any frames - it only sets up routing.
    ///
    /// Returns `(xid, response_receiver)` - the caller MUST set `frame.routing_id = Some(xid)`
    /// on all frames before sending them.
    pub async fn register_external_request(
        &self,
        rid: MessageId,
        cap_urn: &str,
        preferred_cap: Option<&str>,
    ) -> Result<(MessageId, mpsc::UnboundedReceiver<Frame>), RelaySwitchError> {
        // Find master that can handle this cap
        let dest_idx = self.find_master_for_cap(cap_urn, preferred_cap).await.ok_or_else(|| {
            RelaySwitchError::NoHandler(cap_urn.to_string())
        })?;

        // Assign XID
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        let key = (xid.clone(), rid.clone());

        // Create response channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Register response channel BEFORE sending
        self.external_response_channels.write().await.insert(key.clone(), tx);

        // Record origin (None = external caller)
        self.origin_map.write().await.insert(key.clone(), None);

        // Register routing
        self.request_routing.write().await.insert(
            key.clone(),
            RoutingEntry {
                source_master_idx: None,
                destination_master_idx: dest_idx,
            },
        );

        // Record RID → XID mapping for continuation frames
        self.rid_to_xid.write().await.insert(rid.clone(), xid.clone());

        tracing::info!("[RelaySwitch] register_external_request: registered key=({:?}, {:?}) dest_master={}", xid, rid, dest_idx);

        Ok((xid, rx))
    }

    /// Dynamically add a new master connection to the switch.
    ///
    /// Performs handshake (reads RelayNotify, verifies identity) with the new master,
    /// spawns a reader task, and returns the master index.
    ///
    /// This is used for dynamically connecting new hosts (e.g., Mac client connecting via gRPC).
    pub async fn add_master(
        &self,
        socket: UnixStream,
    ) -> Result<usize, RelaySwitchError> {
        let master_idx = self.masters.read().await.len();
        let (read_half, write_half) = socket.into_split();
        let mut socket_reader = FrameReader::new(BufReader::new(read_half));
        let mut socket_writer = FrameWriter::new(BufWriter::new(write_half));

        // Read RelayNotify
        let notify_frame = socket_reader.read().await
            .map_err(|e| RelaySwitchError::Cbor(format!("new master {}: {}", master_idx, e)))?
            .ok_or_else(|| RelaySwitchError::Protocol(
                format!("new master {}: closed before RelayNotify", master_idx)
            ))?;

        if notify_frame.frame_type != FrameType::RelayNotify {
            return Err(RelaySwitchError::Protocol(format!(
                "new master {}: expected RelayNotify, got {:?}",
                master_idx, notify_frame.frame_type
            )));
        }

        let mut caps_payload = notify_frame.relay_notify_manifest()
            .ok_or_else(|| RelaySwitchError::Protocol(
                format!("new master {}: RelayNotify has no manifest", master_idx)
            ))?
            .to_vec();

        let mut payload = parse_relay_notify_payload(&caps_payload)?;
        let mut caps = payload.caps.clone();
        let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

        // Identity verification (same as in new())
        let mut seq_assigner = SeqAssigner::new();
        let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
        {
            use crate::standard::caps::CAP_IDENTITY;

            let nonce = identity_nonce();
            let req_id = MessageId::new_uuid();
            let stream_id = "identity-verify".to_string();

            let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
            req.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut req);
            socket_writer.write(&req).await.map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut ss = Frame::stream_start(req_id.clone(), stream_id.clone(), "media:".to_string(), None);
            ss.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut ss);
            socket_writer.write(&ss).await.map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let checksum = Frame::compute_checksum(&nonce);
            let mut chunk = Frame::chunk(req_id.clone(), stream_id.clone(), 0, nonce.clone(), 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut chunk);
            socket_writer.write(&chunk).await.map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut se = Frame::stream_end(req_id.clone(), stream_id, 1);
            se.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut se);
            socket_writer.write(&se).await.map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut end);
            socket_writer.write(&end).await.map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            seq_assigner.remove(&FlowKey { rid: req_id.clone(), xid: Some(xid.clone()) });

            // Read response
            let mut accumulated = Vec::new();
            loop {
                let frame = socket_reader.read().await
                    .map_err(|e| RelaySwitchError::Protocol(format!(
                        "new master {}: identity read failed: {}", master_idx, e
                    )))?
                    .ok_or_else(|| RelaySwitchError::Protocol(format!(
                        "new master {}: closed during identity verification", master_idx
                    )))?;

                match frame.frame_type {
                    FrameType::RelayNotify => {
                        if let Some(manifest) = frame.relay_notify_manifest() {
                            caps_payload = manifest.to_vec();
                            payload = parse_relay_notify_payload(&caps_payload)?;
                            caps = payload.caps.clone();
                        }
                        if let Some(l) = frame.relay_notify_limits() {
                            limits = l;
                        }
                    }
                    FrameType::StreamStart => {}
                    FrameType::Chunk => {
                        if let Some(payload) = frame.payload {
                            accumulated.extend_from_slice(&payload);
                        }
                    }
                    FrameType::StreamEnd => {}
                    FrameType::End => {
                        if accumulated != nonce {
                            return Err(RelaySwitchError::Protocol(format!(
                                "new master {}: identity payload mismatch ({} vs {} bytes)",
                                master_idx, nonce.len(), accumulated.len()
                            )));
                        }
                        break;
                    }
                    FrameType::Err => {
                        let code = frame.error_code().unwrap_or("UNKNOWN");
                        let msg = frame.error_message().unwrap_or("no message");
                        return Err(RelaySwitchError::Protocol(format!(
                            "new master {}: identity failed: [{code}] {msg}", master_idx
                        )));
                    }
                    other => {
                        return Err(RelaySwitchError::Protocol(format!(
                            "new master {}: identity: unexpected {:?}", master_idx, other
                        )));
                    }
                }
            }
        }

        // Spawn reader task
        let tx = self.frame_tx.clone();
        let reader_handle = tokio::spawn(async move {
            let mut reader = socket_reader;
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if frame.frame_type != crate::bifaci::frame::FrameType::Log {
                            tracing::debug!(
                                "[RelaySwitch] master {} reader: {:?} id={:?} xid={:?}",
                                master_idx, frame.frame_type, frame.id, frame.routing_id
                            );
                        }
                        if tx.send((master_idx, Ok(frame))).is_err() {
                            tracing::warn!("[RelaySwitch] master {} reader: frame_tx closed", master_idx);
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::warn!("[RelaySwitch] master {} reader: socket closed (EOF)", master_idx);
                        let _ = tx.send((master_idx, Err(CborError::UnexpectedEof)));
                        break;
                    }
                    Err(e) => {
                        tracing::error!("[RelaySwitch] master {} reader: socket error: {}", master_idx, e);
                        let _ = tx.send((master_idx, Err(e)));
                        break;
                    }
                }
            }
        });

        let cap_count = caps.len();
        self.masters.write().await.push(MasterConnection {
            socket_writer: Mutex::new(socket_writer),
            seq_assigner: Mutex::new(seq_assigner),
            manifest: RwLock::new(caps_payload),
            limits: RwLock::new(limits),
            caps: RwLock::new(caps),
            installed_plugins: RwLock::new(payload.installed_plugins),
            healthy: AtomicBool::new(true),
            reader_handle: Some(reader_handle),
            connected_at: Instant::now(),
            last_error: RwLock::new(None),
        });

        // Rebuild tables
        self.rebuild_cap_table().await;
        self.rebuild_capabilities().await;
        self.rebuild_limits().await;

        info!(
            master_idx = master_idx,
            cap_count = cap_count,
            "[RelaySwitch] Master connected successfully"
        );

        Ok(master_idx)
    }

    /// Send a frame to the appropriate master (engine → plugin direction).
    ///
    /// REQ frames: Assigned XID if absent, routed by cap URN.
    /// Continuation frames: Routed by (XID, RID) pair.
    /// Send a frame to the appropriate master.
    ///
    /// `preferred_cap`: when `Some`, uses `is_dispatchable` routing and prefers
    /// the master whose registered cap is equivalent to this URN.
    /// When `None`, uses standard `is_dispatchable` + closest-specificity routing.
    pub async fn send_to_master(
        &self,
        mut frame: Frame,
        preferred_cap: Option<&str>,
    ) -> Result<(), RelaySwitchError> {
        tracing::debug!("[RelaySwitch] send_to_master: {:?} id={:?} cap={:?} xid={:?}", frame.frame_type, frame.id, frame.cap, frame.routing_id);
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Find master that can handle this cap
                let dest_idx = self.find_master_for_cap(cap_urn, preferred_cap).await.ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Assign XID if absent (first arrival at RelaySwitch)
                let xid = if let Some(ref existing_xid) = frame.routing_id {
                    existing_xid.clone()
                } else {
                    let new_xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
                    frame.routing_id = Some(new_xid.clone());
                    new_xid
                };

                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Record origin (None = external caller via send_to_master)
                self.origin_map.write().await.insert(key.clone(), None);

                // Register routing (xid, rid) → destination
                self.request_routing.write().await.insert(
                    key,
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: dest_idx,
                    },
                );

                // Record RID → XID mapping for continuation frames from engine
                self.rid_to_xid.write().await.insert(rid, xid);

                // Forward to destination with XID
                self.write_to_master_idx(dest_idx, &mut frame).await?;
                Ok(())
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err => {
                // Continuation frames from engine: look up XID from RID if missing
                let xid = if let Some(ref existing_xid) = frame.routing_id {
                    existing_xid.clone()
                } else {
                    // Engine doesn't send XID - look it up from the REQ's RID → XID mapping
                    let rid = &frame.id;
                    let rid_to_xid = self.rid_to_xid.read().await;
                    let looked_up_xid = rid_to_xid.get(rid).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?.clone();
                    frame.routing_id = Some(looked_up_xid.clone());
                    looked_up_xid
                };

                let key = (xid.clone(), frame.id.clone());

                let dest_idx = {
                    let routing = self.request_routing.read().await;
                    let entry = routing.get(&key).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(frame.id.clone())
                    })?;
                    entry.destination_master_idx
                };

                // Forward to destination
                self.write_to_master_idx(dest_idx, &mut frame).await?;

                Ok(())
            }

            _ => Err(RelaySwitchError::Protocol(format!(
                "Unexpected frame type from engine: {:?}",
                frame.frame_type
            ))),
        }
    }

    /// Read the next frame from any master (plugin → engine direction).
    ///
    /// Awaits until a frame is available from any master. Returns Ok(None) when all masters have closed.
    /// Peer requests (plugin → plugin) are handled internally and not returned.
    pub async fn read_from_masters(&self) -> Result<Option<Frame>, RelaySwitchError> {
        loop {
            // Hold lock through handle_master_frame — see read_from_masters_timeout comment.
            let mut rx = self.frame_rx.lock().await;
            let frame_result = rx.recv().await;

            match frame_result {
                Some((master_idx, Ok(frame))) => {
                    let handle_result = self.handle_master_frame(master_idx, frame).await;
                    drop(rx);
                    if let Some(result_frame) = handle_result? {
                        return Ok(Some(result_frame));
                    }
                }
                Some((master_idx, Err(_e))) => {
                    drop(rx);
                    self.handle_master_death(master_idx).await?;
                }
                None => {
                    drop(rx);
                    return Ok(None);
                }
            }
        }
    }

    /// Process one pending frame from any master, non-blocking.
    ///
    /// Returns Ok(Some(frame)) if a frame was processed and should be returned to caller.
    /// Returns Ok(None) if no frame was available or the frame was handled internally.
    /// Use this in tokio::select! loops for concurrent frame processing.
    pub async fn pump_one(&self) -> Result<Option<Frame>, RelaySwitchError> {
        // Hold lock through handle_master_frame — see read_from_masters_timeout comment.
        let mut rx = self.frame_rx.lock().await;
        let frame_result = rx.try_recv();

        match frame_result {
            Ok((master_idx, Ok(frame))) => {
                let handle_result = self.handle_master_frame(master_idx, frame).await;
                drop(rx);
                if let Some(result_frame) = handle_result? {
                    return Ok(Some(result_frame));
                }
                Ok(None)
            }
            Ok((master_idx, Err(_e))) => {
                drop(rx);
                self.handle_master_death(master_idx).await?;
                Ok(None)
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No frames available
                Ok(None)
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // All reader tasks have exited
                Err(RelaySwitchError::Protocol("All masters disconnected".to_string()))
            }
        }
    }

    /// Wait for the next frame from any master with timeout.
    ///
    /// Returns Ok(Some(frame)) if a frame arrives, Ok(None) on timeout, Err on error.
    pub async fn read_from_masters_timeout(&self, timeout: std::time::Duration) -> Result<Option<Frame>, RelaySwitchError> {
        let start = std::time::Instant::now();
        loop {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Ok(None); // Timeout
            }

            // Receive and process under the same lock. Multiple pump tasks
            // call this method concurrently — the lock ensures that a REQ's
            // routing table writes (rid_to_xid, request_routing) complete
            // before the next frame is dequeued. Without this, a continuation
            // frame can be dequeued by a second pump before the first pump
            // finishes inserting the REQ's routing entries.
            let mut rx = self.frame_rx.lock().await;
            let frame_result = tokio::time::timeout(remaining, rx.recv()).await;
            // Extract the frame data, then drop the lock before async I/O
            let action = match frame_result {
                Ok(Some((master_idx, Ok(frame)))) => Some((master_idx, Ok(frame))),
                Ok(Some((master_idx, Err(e)))) => Some((master_idx, Err(e))),
                Ok(None) => {
                    drop(rx);
                    return Err(RelaySwitchError::Protocol("All masters disconnected".to_string()));
                }
                Err(_elapsed) => {
                    drop(rx);
                    return Ok(None); // Timeout
                }
            };
            // Process the frame while still holding the lock — this serializes
            // handle_master_frame so routing table mutations from a REQ are
            // visible before the next recv can return its continuation frames.
            if let Some((master_idx, result)) = action {
                match result {
                    Ok(frame) => {
                        let handle_result = self.handle_master_frame(master_idx, frame).await;
                        drop(rx);
                        if let Some(result_frame) = handle_result? {
                            return Ok(Some(result_frame));
                        }
                    }
                    Err(_e) => {
                        drop(rx);
                        self.handle_master_death(master_idx).await?;
                    }
                }
            }
        }
    }

    // =========================================================================
    // FRAME OUTPUT (all writes to masters go through this)
    // =========================================================================

    /// Low-level frame write that assigns seq and writes to the master transport.
    /// This helper does not perform master retirement on failure.
    async fn write_to_master_idx_raw(&self, master_idx: usize, frame: &mut Frame) -> Result<(), CborError> {
        tracing::debug!("[RelaySwitch] write_to_master_idx_raw: master={} {:?} id={:?} xid={:?}", master_idx, frame.frame_type, frame.id, frame.routing_id);

        let masters = self.masters.read().await;
        let master = &masters[master_idx];

        // Lock seq_assigner and socket_writer separately to minimize lock contention
        {
            let mut seq = master.seq_assigner.lock().await;
            seq.assign(frame);
        }

        let write_result = {
            let mut writer = master.socket_writer.lock().await;
            writer.write(frame).await
        };

        if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
            let mut seq = master.seq_assigner.lock().await;
            seq.remove(&FlowKey::from_frame(frame));
        }

        write_result
    }

    /// Write a frame to a master, assigning seq via the per-master SeqAssigner.
    /// Cleans up seq tracking on terminal frames (END/ERR).
    async fn write_to_master_idx(&self, master_idx: usize, frame: &mut Frame) -> Result<(), CborError> {
        tracing::debug!("[RelaySwitch] write_to_master_idx: master={} {:?} id={:?} xid={:?}", master_idx, frame.frame_type, frame.id, frame.routing_id);

        let write_result = self.write_to_master_idx_raw(master_idx, frame).await;

        match write_result {
            Ok(()) => Ok(()),
            Err(error) => {
                let reason = format!("Write to master {} failed: {}", master_idx, error);
                if let Err(cleanup_error) = self.handle_master_death_with_reason(master_idx, &reason).await {
                    tracing::error!(
                        master_idx = master_idx,
                        write_error = %error,
                        cleanup_error = %cleanup_error,
                        "[RelaySwitch] Failed to retire dead master after write failure"
                    );
                }
                Err(error)
            }
        }
    }

    // =========================================================================
    // INTERNAL ROUTING
    // =========================================================================

    /// Find which master handles a given cap URN.
    ///
    /// ## Routing semantics
    ///
    /// Uses `is_dispatchable(provider, request)` to find all masters that can
    /// legally handle the request. A provider is dispatchable if:
    /// - Its input handling is compatible with the request's input
    /// - Its output guarantees meet the request's output requirements
    /// - Its cap-tags satisfy all explicit request constraints
    ///
    /// Among dispatchable matches, ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific providers (positive distance) - refinements
    /// 3. More generic providers (negative distance) - fallbacks
    ///
    /// ## With preference (`preferred_cap = Some(cap_urn)`)
    ///
    /// Among dispatchable matches, the master whose registered cap is
    /// equivalent to the preferred cap wins. If no equivalent match, falls
    /// back to specificity-based ranking.
    async fn find_master_for_cap(&self, cap_urn: &str, preferred_cap: Option<&str>) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Parse preferred cap URN if provided
        let preferred_urn = preferred_cap.and_then(|p| crate::CapUrn::from_string(p).ok());

        // Collect ALL dispatchable masters with their specificity scores.
        let mut matches: Vec<(usize, isize, bool)> = Vec::new(); // (master_idx, signed_distance, is_preferred)

        let cap_table = self.cap_table.read().await;
        tracing::debug!(target: "find_master_for_cap", "Request: {} (specificity={})", cap_urn, request_specificity);
        tracing::debug!(target: "find_master_for_cap", "Checking {} registered caps", cap_table.len());
        for (registered_cap, master_idx) in cap_table.iter() {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                let dispatchable = registered_urn.is_dispatchable(&request_urn);
                tracing::debug!(target: "find_master_for_cap", "{} -> dispatchable={}", registered_cap, dispatchable);
                // Use is_dispatchable: can this provider handle this request?
                if dispatchable {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;
                    // Check if this registered cap is equivalent to the preferred cap
                    let is_preferred = preferred_urn.as_ref().map_or(false, |pref| {
                        pref.is_equivalent(&registered_urn)
                    });
                    matches.push((*master_idx, signed_distance, is_preferred));
                }
            }
        }

        tracing::debug!(target: "find_master_for_cap", "Found {} matches", matches.len());
        if matches.is_empty() {
            return None;
        }

        // If any match is preferred, pick the first preferred match.
        if let Some(&(idx, _, _)) = matches.iter().find(|(_, _, pref)| *pref) {
            return Some(idx);
        }

        // Ranking: prefer equivalent (0), then more specific (+), then more generic (-)
        // Sort by: (is_negative, abs_distance) so positives come before negatives at same abs
        matches.sort_by(|a, b| {
            let (_, dist_a, _) = a;
            let (_, dist_b, _) = b;

            // First: non-negative distances before negative
            match (dist_a >= &0, dist_b >= &0) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => {
                    // Same sign: prefer smaller absolute distance
                    dist_a.unsigned_abs().cmp(&dist_b.unsigned_abs())
                }
            }
        });

        matches.first().map(|(idx, _, _)| *idx)
    }

    /// Handle a frame arriving from a master (plugin → engine direction).
    ///
    /// Returns Some(frame) if the frame should be forwarded to the engine.
    /// Returns None if the frame was handled internally (peer request).
    async fn handle_master_frame(
        &self,
        source_idx: usize,
        mut frame: Frame,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        tracing::debug!("[RelaySwitch] handle_master_frame: from_master={} {:?} id={:?} xid={:?}", source_idx, frame.frame_type, frame.id, frame.routing_id);
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;


                // Find destination master (no preference for peer requests)
                let dest_idx = self.find_master_for_cap(cap_urn, None).await.ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Assign XID if absent (first arrival at RelaySwitch)
                // REQs from plugins should NOT have XID (per spec)
                if frame.routing_id.is_some() {
                    return Err(RelaySwitchError::Protocol(
                        "REQ from plugin should not have XID".to_string()
                    ));
                }

                let xid = MessageId::Uint(self.xid_counter.fetch_add(1, Ordering::SeqCst) + 1);
                frame.routing_id = Some(xid.clone());


                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Record RID → XID mapping for continuation frames
                self.rid_to_xid.write().await.insert(rid.clone(), xid.clone());

                // Record origin (where this request came from)
                tracing::info!("[RelaySwitch] PEER_REQ: master {} → master {} cap='{}' rid={:?} xid={:?}", source_idx, dest_idx, cap_urn, rid, xid);
                self.origin_map.write().await.insert(key.clone(), Some(source_idx));

                // Register routing
                self.request_routing.write().await.insert(
                    key.clone(),
                    RoutingEntry {
                        source_master_idx: Some(source_idx),
                        destination_master_idx: dest_idx,
                    },
                );

                // Mark as peer request (for cleanup tracking)
                self.peer_requests.write().await.insert(key);

                // Forward to destination with XID
                self.write_to_master_idx(dest_idx, &mut frame).await?;

                // Do NOT return to engine (internal routing)
                Ok(None)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Log => {
                if frame.frame_type == FrameType::Log {
                    tracing::debug!(target: "relay_switch", "Routing LOG frame: xid={:?} rid={:?}", frame.routing_id, frame.id);
                }
                // Branch based on XID presence to distinguish request vs response direction
                if frame.routing_id.is_some() {
                    // ========================================
                    // HAS XID = RESPONSE CONTINUATION
                    // ========================================
                    // Frame already has XID, so it's a response flowing back to origin
                    let xid = frame.routing_id.clone().unwrap();
                    let rid = frame.id.clone();
                    let key = (xid.clone(), rid.clone());

                    // Look up routing entry
                    let dest_idx = {
                        let routing = self.request_routing.read().await;
                        let entry = routing.get(&key).ok_or_else(|| {
                            RelaySwitchError::UnknownRequest(rid.clone())
                        })?;
                        entry.destination_master_idx
                    };

                    // Get origin (where request came from)
                    let origin_idx = {
                        let origin = self.origin_map.read().await;
                        let result = origin.get(&key).copied();
                        tracing::debug!(target: "relay_switch", "origin_map lookup: key=({:?}, {:?}) result={:?}", xid, rid, result);
                        result.ok_or_else(|| {
                            RelaySwitchError::Protocol(format!("No origin recorded for request ({:?}, {:?})", xid, rid))
                        })?
                    };

                    let is_terminal = frame.frame_type == FrameType::End
                        || frame.frame_type == FrameType::Err;

                    // Route back to origin
                    match origin_idx {
                        None => {
                            // External caller (via send_to_master or execute_cap)
                            // Check if there's a response channel registered
                            let tx_opt = {
                                let channels = self.external_response_channels.read().await;
                                tracing::debug!("[RelaySwitch] external_response_channels lookup: key=({:?}, {:?}) found={}", xid, rid, channels.contains_key(&key));
                                channels.get(&key).cloned()
                            };

                            if let Some(tx) = tx_opt {
                                // Send to external response channel (keep XID for now)
                                let send_result = tx.send(frame.clone());
                                if frame.frame_type != FrameType::Log {
                                    tracing::info!("[RelaySwitch] routed {:?} to executor: xid={:?} rid={:?} ok={}", frame.frame_type, xid, rid, send_result.is_ok());
                                }

                                // Cleanup on terminal frame
                                if is_terminal {
                                    self.external_response_channels.write().await.remove(&key);
                                    self.request_routing.write().await.remove(&key);
                                    self.origin_map.write().await.remove(&key);
                                    self.peer_requests.write().await.remove(&key);
                                    self.rid_to_xid.write().await.remove(&rid);
                                }

                                return Ok(None);
                            } else {
                                // No response channel (sent via send_to_master, not execute_cap)
                                // Strip XID and return to caller (final leg)
                                tracing::info!("[RelaySwitch] NO external_response_channel for key=({:?}, {:?}), returning frame to caller", xid, rid);
                                frame.routing_id = None;

                                // Cleanup on terminal frame
                                if is_terminal {
                                    self.request_routing.write().await.remove(&key);
                                    self.origin_map.write().await.remove(&key);
                                    self.peer_requests.write().await.remove(&key);
                                    self.rid_to_xid.write().await.remove(&rid);
                                }

                                return Ok(Some(frame));
                            }
                        }
                        Some(master_idx) => {
                            // Route back to source master — KEEP XID.
                            if is_terminal {
                                tracing::info!("[RelaySwitch] PEER_RESP: routing {:?} back to master {} xid={:?} rid={:?}", frame.frame_type, master_idx, xid, rid);
                            }
                            self.write_to_master_idx(master_idx, &mut frame).await?;
                            if is_terminal {
                                tracing::info!("[RelaySwitch] PEER_RESP: write to master {} completed", master_idx);
                            }

                            // Cleanup on terminal frame
                            if is_terminal {
                                self.request_routing.write().await.remove(&key);
                                self.origin_map.write().await.remove(&key);
                                self.peer_requests.write().await.remove(&key);
                                self.rid_to_xid.write().await.remove(&rid);
                            }

                            return Ok(None);
                        }
                    }
                } else {
                    // ========================================
                    // NO XID = REQUEST CONTINUATION
                    // ========================================
                    // Frame has no XID, so it's a request continuation flowing to destination
                    let rid = frame.id.clone();


                    // Look up XID from RID → XID mapping (added by the REQ)
                    let xid = {
                        let rid_to_xid = self.rid_to_xid.read().await;
                        rid_to_xid.get(&rid).ok_or_else(|| {
                            RelaySwitchError::UnknownRequest(rid.clone())
                        })?.clone()
                    };

                    let key = (xid.clone(), rid.clone());

                    // Look up routing entry
                    let dest_idx = {
                        let routing = self.request_routing.read().await;
                        let entry = routing.get(&key).ok_or_else(|| {
                            RelaySwitchError::UnknownRequest(rid.clone())
                        })?;
                        entry.destination_master_idx
                    };

                    // Add XID to frame for forwarding
                    frame.routing_id = Some(xid.clone());

                    // Forward to destination master (keep XID)
                    self.write_to_master_idx(dest_idx, &mut frame).await?;
                    return Ok(None);
                }
            }

            FrameType::RelayNotify => {
                // Capability update from host — update our cap table
                let caps_payload = frame.relay_notify_manifest()
                    .ok_or_else(|| RelaySwitchError::Protocol("RelayNotify has no payload".to_string()))?;

                let payload = parse_relay_notify_payload(caps_payload)?;
                let new_caps = payload.caps;

                // Update master's caps and limits
                {
                    let masters = self.masters.read().await;
                    if let Some(master) = masters.get(source_idx) {
                        *master.caps.write().await = new_caps;
                        *master.installed_plugins.write().await = payload.installed_plugins;
                        *master.manifest.write().await = caps_payload.to_vec();
                        // Extract and update limits from RelayNotify
                        if let Some(new_limits) = frame.relay_notify_limits() {
                            *master.limits.write().await = new_limits;
                        }
                    }
                }
                // Rebuild cap_table, aggregate capabilities, and limits from all masters
                self.rebuild_cap_table().await;
                self.rebuild_capabilities().await;
                self.rebuild_limits().await;

                // Pass through to engine (for visibility)
                Ok(Some(frame))
            }

            _ => {
                // All other frames: pass through to engine
                Ok(Some(frame))
            }
        }
    }

    /// Handle master death: ERR all pending requests, mark unhealthy, rebuild tables.
    async fn handle_master_death(&self, master_idx: usize) -> Result<(), RelaySwitchError> {
        self.handle_master_death_with_reason(master_idx, "Connection closed unexpectedly").await
    }

    /// Handle master death with a specific error reason.
    async fn handle_master_death_with_reason(
        &self,
        master_idx: usize,
        reason: &str,
    ) -> Result<(), RelaySwitchError> {
        // Own the reason string for use across await points
        let reason_owned = reason.to_string();

        // Get master info before marking unhealthy
        let (was_healthy, cap_count, connected_seconds) = {
            let masters = self.masters.read().await;
            if master_idx >= masters.len() {
                let total = masters.len();
                error!(
                    master_idx = master_idx,
                    total_masters = total,
                    "[RelaySwitch] handle_master_death called with invalid master index"
                );
                return Ok(());
            }
            let master = &masters[master_idx];
            let was_healthy = master.healthy.load(Ordering::SeqCst);
            let cap_count = master.caps.read().await.len();
            let connected_seconds = master.connected_at.elapsed().as_secs();
            (was_healthy, cap_count, connected_seconds)
        };

        if !was_healthy {
            return Ok(()); // Already handled
        }

        // Mark unhealthy and record error
        {
            let masters = self.masters.read().await;
            masters[master_idx].healthy.store(false, Ordering::SeqCst);
            *masters[master_idx].last_error.write().await = Some(reason_owned.clone());
        }

        error!(
            master_idx = master_idx,
            cap_count = cap_count,
            connected_seconds = connected_seconds,
            reason = %reason_owned,
            "[RelaySwitch] Master died - marking unhealthy and cleaning up"
        );

        // Find all pending requests for this master
        let dead_requests: Vec<((MessageId, MessageId), Option<usize>)> = {
            let routing = self.request_routing.read().await;
            routing.iter()
                .filter(|(_, entry)| entry.destination_master_idx == master_idx)
                .map(|(key, entry)| (key.clone(), entry.source_master_idx))
                .collect()
        };

        if !dead_requests.is_empty() {
            warn!(
                master_idx = master_idx,
                pending_requests = dead_requests.len(),
                "[RelaySwitch] Failing pending requests due to master death"
            );
        }

        // Send ERR for each pending request
        for (key, source_idx) in dead_requests {
            let (xid, rid) = &key;

            // Create ERR frame
            let mut err_frame = Frame::err(
                rid.clone(),
                "MASTER_DIED",
                &format!("Relay master {} connection closed: {}", master_idx, reason),
            );
            err_frame.routing_id = Some(xid.clone());

            match source_idx {
                None => {
                    // External caller - send to response channel if exists
                    let tx_opt = {
                        let channels = self.external_response_channels.read().await;
                        channels.get(&key).cloned()
                    };
                    if let Some(tx) = tx_opt {
                        let _ = tx.send(err_frame);
                        self.external_response_channels.write().await.remove(&key);
                    }
                }
                Some(src_master_idx) => {
                    // Send ERR back to source master
                    let is_healthy = {
                        let masters = self.masters.read().await;
                        masters[src_master_idx].healthy.load(Ordering::SeqCst)
                    };
                    if is_healthy {
                        let _ = self.write_to_master_idx_raw(src_master_idx, &mut err_frame).await;
                    }
                }
            }

            // Cleanup routing
            self.request_routing.write().await.remove(&key);
            self.origin_map.write().await.remove(&key);
            self.peer_requests.write().await.remove(&key);
        }

        // Rebuild tables
        self.rebuild_cap_table().await;
        self.rebuild_capabilities().await;
        self.rebuild_limits().await;

        // Log remaining healthy masters
        let (healthy_count, total_count) = {
            let masters = self.masters.read().await;
            let healthy = masters.iter().filter(|m| m.healthy.load(Ordering::SeqCst)).count();
            let total = masters.len();
            (healthy, total)
        };
        info!(
            healthy_masters = healthy_count,
            total_masters = total_count,
            "[RelaySwitch] Master health updated after death"
        );

        Ok(())
    }

    // =========================================================================
    // TABLE MANAGEMENT
    // =========================================================================

    /// Rebuild cap_table from all healthy masters.
    async fn rebuild_cap_table(&self) {
        let mut new_cap_table = Vec::new();
        let masters = self.masters.read().await;
        for (idx, master) in masters.iter().enumerate() {
            if master.healthy.load(Ordering::SeqCst) {
                let caps = master.caps.read().await;
                for cap in caps.iter() {
                    new_cap_table.push((cap.clone(), idx));
                }
            }
        }
        *self.cap_table.write().await = new_cap_table;
    }

    /// Rebuild aggregate capabilities (union of all healthy masters).
    /// Logs changes if the capability set differs from the previous state.
    async fn rebuild_capabilities(&self) {
        // Collect caps per master for detailed logging
        let mut caps_by_master: Vec<(usize, bool, Vec<String>)> = Vec::new();
        let mut installed_plugins_by_master: Vec<(bool, Vec<InstalledPluginIdentity>)> = Vec::new();
        let masters = self.masters.read().await;
        for (idx, master) in masters.iter().enumerate() {
            let healthy = master.healthy.load(Ordering::SeqCst);
            let caps = master.caps.read().await.clone();
            let installed_plugins = master.installed_plugins.read().await.clone();
            caps_by_master.push((idx, healthy, caps));
            installed_plugins_by_master.push((healthy, installed_plugins));
        }
        drop(masters);

        // Collect all caps from healthy masters
        let mut all_caps: Vec<String> = Vec::new();
        for (_, healthy, caps) in &caps_by_master {
            if *healthy {
                all_caps.extend(caps.iter().cloned());
            }
        }

        // Deduplicate
        all_caps.sort();
        all_caps.dedup();

        // Compare with previous capabilities
        let old_caps: Vec<String> = {
            let old_bytes = self.aggregate_capabilities.read().await;
            serde_json::from_slice(&old_bytes).unwrap_or_default()
        };

        let changed = old_caps != all_caps;

        // Build manifest as JSON array (same format as RelayNotify payloads)
        *self.aggregate_capabilities.write().await = serde_json::to_vec(&all_caps).unwrap_or_default();
        let mut all_installed_plugins: Vec<InstalledPluginIdentity> = Vec::new();
        for (healthy, installed_plugins) in installed_plugins_by_master {
            if healthy {
                all_installed_plugins.extend(installed_plugins);
            }
        }
        all_installed_plugins.sort();
        all_installed_plugins.dedup_by(|left, right| {
            left.id == right.id && left.version == right.version && left.sha256 == right.sha256
        });
        tracing::info!(
            target: "relay_switch",
            installed_plugins = ?all_installed_plugins,
            "RelaySwitch rebuilt installed plugins"
        );
        *self.aggregate_installed_plugins.write().await = all_installed_plugins;

        // Log only if changed
        if changed {
            info!(
                total_caps = all_caps.len(),
                previous_caps = old_caps.len(),
                "[RelaySwitch] Capabilities changed"
            );

            // Log per-master breakdown
            for (idx, healthy, caps) in &caps_by_master {
                let status = if *healthy { "healthy" } else { "unhealthy" };
                info!(
                    master_idx = idx,
                    status = status,
                    cap_count = caps.len(),
                    "[RelaySwitch] Master {} caps: {} ({})",
                    idx, caps.len(), status
                );
                // Log sample of caps (first 5) for debugging
                for (i, cap) in caps.iter().take(5).enumerate() {
                    info!(
                        master_idx = idx,
                        cap_idx = i,
                        cap_urn = cap.as_str(),
                        "[RelaySwitch]   cap[{}]: {}",
                        i, cap
                    );
                }
                if caps.len() > 5 {
                    info!(
                        master_idx = idx,
                        remaining = caps.len() - 5,
                        "[RelaySwitch]   ... and {} more caps",
                        caps.len() - 5
                    );
                }
            }

            // Rebuild the LiveCapGraph with the new set of available caps
            let mut graph = self.live_cap_graph.write().await;
            graph.sync_from_cap_urns(&all_caps, &self.cap_registry).await;
        }
    }

    /// Rebuild negotiated limits (minimum across all healthy masters).
    async fn rebuild_limits(&self) {
        let mut min_max_frame = usize::MAX;
        let mut min_max_chunk = usize::MAX;

        let masters = self.masters.read().await;
        for master in masters.iter() {
            if master.healthy.load(Ordering::SeqCst) {
                let limits = master.limits.read().await;
                min_max_frame = min_max_frame.min(limits.max_frame);
                min_max_chunk = min_max_chunk.min(limits.max_chunk);
            }
        }

        *self.negotiated_limits.write().await = Limits {
            max_frame: if min_max_frame == usize::MAX {
                crate::bifaci::frame::DEFAULT_MAX_FRAME
            } else {
                min_max_frame
            },
            max_chunk: if min_max_chunk == usize::MAX {
                crate::bifaci::frame::DEFAULT_MAX_CHUNK
            } else {
                min_max_chunk
            },
            ..Limits::default()
        };
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Parse capabilities payload from RelayNotify.
/// RelayNotify contains a JSON object with capability URNs and installed plugin identities.
/// Validates that CAP_IDENTITY is present — hard-fail if missing.
fn parse_relay_notify_payload(notify_payload: &[u8]) -> Result<RelayNotifyCapabilitiesPayload, RelaySwitchError> {
    use crate::urn::cap_urn::CapUrn;
    use crate::standard::caps::CAP_IDENTITY;

    let payload: RelayNotifyCapabilitiesPayload = serde_json::from_slice(notify_payload)
        .map_err(|e| RelaySwitchError::Protocol(format!("Invalid RelayNotify payload: {}", e)))?;

    // Verify CAP_IDENTITY is present — mandatory for every host
    let identity_urn = CapUrn::from_string(CAP_IDENTITY)
        .expect("BUG: CAP_IDENTITY constant is invalid");
    let has_identity = payload.caps.iter().any(|cap_str| {
        CapUrn::from_string(cap_str)
            .map(|cap_urn| identity_urn.conforms_to(&cap_urn))
            .unwrap_or(false)
    });
    if !has_identity {
        return Err(RelaySwitchError::Protocol(
            format!("RelayNotify missing required CAP_IDENTITY ({})", CAP_IDENTITY)
        ));
    }

    Ok(payload)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::{Frame, SeqAssigner};
    use crate::standard::caps::CAP_IDENTITY;
    use tokio::net::UnixStream;

    /// Create a test CapRegistry for use in tests.
    fn test_cap_registry() -> Arc<CapRegistry> {
        Arc::new(CapRegistry::new_for_test())
    }

    /// Helper: send RelayNotify with given caps/limits, then handle identity verification.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    async fn slave_notify_with_identity(
        socket: UnixStream,
        caps_json: &serde_json::Value,
        limits: &Limits,
    ) -> (FrameReader<BufReader<tokio::net::unix::OwnedReadHalf>>, FrameWriter<BufWriter<tokio::net::unix::OwnedWriteHalf>>) {
        let (read_half, write_half) = socket.into_split();
        let mut reader = FrameReader::new(BufReader::new(read_half));
        let mut writer = FrameWriter::new(BufWriter::new(write_half));

        // Send RelayNotify
        let notify_payload = serde_json::json!({
            "caps": caps_json,
            "installed_plugins": [],
        });
        let notify = Frame::relay_notify(&serde_json::to_vec(&notify_payload).unwrap(), limits);
        writer.write(&notify).await.unwrap();

        // Handle identity verification REQ
        let req = reader.read().await.unwrap().expect("expected identity REQ after RelayNotify");
        assert_eq!(req.frame_type, FrameType::Req, "first frame after RelayNotify must be identity REQ");

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().await.unwrap().expect("expected frame");
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
        let ss = Frame::stream_start(req.id.clone(), stream_id.clone(), "media:".to_string(), None);
        writer.write(&ss).await.unwrap();
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
        writer.write(&chunk).await.unwrap();
        let se = Frame::stream_end(req.id.clone(), stream_id, 1);
        writer.write(&se).await.unwrap();
        let end = Frame::end(req.id, None);
        writer.write(&end).await.unwrap();

        (reader, writer)
    }

    // TEST429: Cap routing logic (find_master_for_cap)
    #[tokio::test]
    async fn test429_find_master_for_cap() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        // Spawn slave 1 (identity cap only)
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock1,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default()).await;
        });

        // Spawn slave 2 (identity + double cap)
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default()).await;
        });

        // Constructor reads RelayNotify + verifies identity for both masters
        let switch = RelaySwitch::new(vec![engine_sock1, engine_sock2], test_cap_registry()).await.unwrap();

        // Verify routing (caps already populated during construction)
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None).await, Some(0));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=double;out=\"media:void\"", None).await, Some(1));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=unknown;out=\"media:void\"", None).await, None);

        // Verify aggregate capabilities (plain JSON array)
        let cap_list: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert_eq!(cap_list.len(), 2);
    }

    // TEST426: Single master REQ/response routing
    #[tokio::test]
    async fn test426_single_master_req_response() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default()).await;

            // Read REQ frame
            let (req_id, xid) = if let Some(frame) = reader.read().await.unwrap() {
                if frame.frame_type == FrameType::Req {
                    (Some(frame.id.clone()), frame.routing_id.clone())
                } else { (None, None) }
            } else { (None, None) };

            // Read END frame
            if let Some(frame) = reader.read().await.unwrap() {
                if frame.frame_type == FrameType::End && req_id.is_some() {
                    let rid = req_id.unwrap();
                    let xid_clone = xid.clone();
                    let mut response = Frame::end(rid.clone(), Some(vec![42]));
                    response.routing_id = xid;
                    seq.assign(&mut response);
                    writer.write(&response).await.unwrap();
                    seq.remove(&FlowKey { rid: rid.clone(), xid: xid_clone });
                }
            }
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        // Send REQ + END (caps already populated from construction)
        let req_id = MessageId::Uint(1);
        let req = Frame::req(req_id.clone(), "cap:in=media:;out=media:", vec![1, 2, 3], "text/plain");
        switch.send_to_master(req, None).await.unwrap();
        let end = Frame::end(req_id.clone(), None);
        switch.send_to_master(end, None).await.unwrap();

        let response = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.id, MessageId::Uint(1));
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST427: Multi-master cap routing
    #[tokio::test]
    async fn test427_multi_master_cap_routing() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock1,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default()).await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default()).await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(vec![engine_sock1, engine_sock2], test_cap_registry()).await.unwrap();

        // Caps already populated from construction — send requests directly
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), "cap:in=media:;out=media:", vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).await.unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        let req2_id = MessageId::Uint(2);
        switch.send_to_master(Frame::req(req2_id.clone(), "cap:in=\"media:void\";op=double;out=\"media:void\"", vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req2_id, None), None).await.unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![2]));
    }

    // TEST428: Unknown cap returns error
    #[tokio::test]
    async fn test428_unknown_cap_returns_error() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        let req = Frame::req(MessageId::Uint(1), "cap:in=\"media:void\";op=unknown;out=\"media:void\"", vec![], "text/plain");
        let result = switch.send_to_master(req, None).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }

    // TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent)
    #[tokio::test]
    async fn test430_tie_breaking_same_cap_multiple_masters() {
        let same_cap = "cap:in=media:;out=media:";

        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock1,
                &serde_json::json!([same_cap]), &Limits::default()).await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock2,
                &serde_json::json!([same_cap]), &Limits::default()).await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(vec![engine_sock1, engine_sock2], test_cap_registry()).await.unwrap();

        // First request — should go to master 0 (first match)
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), same_cap, vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).await.unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        // Second request — should ALSO go to master 0 (consistent routing)
        let req2_id = MessageId::Uint(2);
        switch.send_to_master(Frame::req(req2_id.clone(), same_cap, vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req2_id, None), None).await.unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![1]));
    }

    // TEST431: Continuation frame routing (CHUNK, END follow REQ)
    #[tokio::test]
    async fn test431_continuation_frame_routing() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=test;out=\"media:void\""]),
                &Limits::default()).await;

            let req = reader.read().await.unwrap().unwrap();
            assert_eq!(req.frame_type, FrameType::Req);
            let xid = req.routing_id.clone();

            let chunk = reader.read().await.unwrap().unwrap();
            assert_eq!(chunk.frame_type, FrameType::Chunk);
            assert_eq!(chunk.id, req.id);

            let end = reader.read().await.unwrap().unwrap();
            assert_eq!(end.frame_type, FrameType::End);
            assert_eq!(end.id, req.id);

            let rid = req.id.clone();
            let xid_clone = xid.clone();
            let mut response = Frame::end(rid.clone(), Some(vec![42]));
            response.routing_id = xid;
            seq.assign(&mut response);
            writer.write(&response).await.unwrap();
            seq.remove(&FlowKey { rid: rid.clone(), xid: xid_clone });
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        let req_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req_id.clone(), "cap:in=\"media:void\";op=test;out=\"media:void\"", vec![], "text/plain"), None).await.unwrap();
        let payload = vec![1, 2, 3];
        let checksum = Frame::compute_checksum(&payload);
        switch.send_to_master(Frame::chunk(req_id.clone(), "stream1".to_string(), 0, payload, 0, checksum), None).await.unwrap();
        switch.send_to_master(Frame::end(req_id.clone(), None), None).await.unwrap();

        let response = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST432: Empty masters list creates empty switch, add_master works
    #[tokio::test]
    async fn test432_empty_masters_allowed() {
        let switch = RelaySwitch::new(vec![], test_cap_registry()).await.unwrap();

        // Empty switch has no caps
        let caps: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(caps.is_empty(), "empty switch should have no caps");

        // No handler for any cap
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None).await, None);
    }

    // TEST433: Capability aggregation deduplicates caps
    #[tokio::test]
    async fn test433_capability_aggregation_deduplicates() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock1,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default()).await;
        });

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=triple;out=\"media:void\""]),
                &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock1, engine_sock2], test_cap_registry()).await.unwrap();

        // Caps already populated during construction (plain JSON array)
        let mut cap_list: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        cap_list.sort();

        assert_eq!(cap_list.len(), 3);
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=double;out=\"media:void\"".to_string()));
        assert!(cap_list.contains(&"cap:in=media:;out=media:".to_string()));
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=triple;out=\"media:void\"".to_string()));
    }

    // TEST434: Limits negotiation takes minimum
    #[tokio::test]
    async fn test434_limits_negotiation_minimum() {
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();
        let (engine_sock2, slave_sock2) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock1,
                &serde_json::json!(["cap:in=media:;out=media:"]),
                &Limits { max_frame: 1_000_000, max_chunk: 100_000, ..Limits::default() }).await;
        });

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock2,
                &serde_json::json!(["cap:in=media:;out=media:"]),
                &Limits { max_frame: 2_000_000, max_chunk: 50_000, ..Limits::default() }).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock1, engine_sock2], test_cap_registry()).await.unwrap();

        // Limits already negotiated during construction
        assert_eq!(switch.limits().await.max_frame, 1_000_000);
        assert_eq!(switch.limits().await.max_chunk, 50_000);
    }

    // TEST435: URN matching (exact vs accepts())
    #[tokio::test]
    async fn test435_urn_matching_exact_and_accepts() {
        let registered_cap = "cap:in=\"media:text;utf8\";op=process;out=\"media:text;utf8\"";

        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:", registered_cap]), &Limits::default()).await;
            loop {
                match reader.read().await.unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![42]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).await.unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        // Exact match should work
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), registered_cap, vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).await.unwrap();
        let resp1 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![42]));

        // Request with more specific input and less specific output SHOULD match
        // Input (contravariant): request's `media:text;utf8;normalized` conforms to provider's `media:text;utf8`
        // Output (covariant): provider's `media:text;utf8` conforms to request's `media:text`
        let req2_id = MessageId::Uint(2);
        switch.send_to_master(Frame::req(req2_id.clone(), "cap:in=\"media:text;utf8;normalized\";op=process;out=\"media:text\"", vec![], "text/plain"), None).await.unwrap();
        switch.send_to_master(Frame::end(req2_id, None), None).await.unwrap();
        let resp2 = switch.read_from_masters().await.unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![42]));

        // Request with INCOMPATIBLE input should NOT match (different type family)
        let req3 = Frame::req(MessageId::Uint(3), "cap:in=\"media:image;png\";op=process;out=\"media:text\"", vec![], "text/plain");
        let result = switch.send_to_master(req3, None).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }

    // =========================================================================
    // Preferred cap routing tests
    // =========================================================================

    // TEST437: find_master_for_cap with preferred_cap routes to generic handler
    //
    // With is_dispatchable semantics:
    // - Generic provider (in=media:) CAN dispatch specific request (in="media:pdf")
    //   because media: (wildcard) accepts any input type
    // - Preference routes to preferred among dispatchable candidates
    #[tokio::test]
    async fn test437_preferred_cap_routes_to_generic() {
        let (engine_sock0, slave_sock0) = UnixStream::pair().unwrap();
        let (engine_sock1, slave_sock1) = UnixStream::pair().unwrap();

        // Master 0: generic thumbnail handler (like internal ThumbnailProvider)
        let generic_cap = "cap:in=media:;op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock0,
                &serde_json::json!(["cap:in=media:;out=media:", generic_cap]),
                &Limits::default()).await;
        });

        // Master 1: specific thumbnail handler (like pdfcartridge)
        let specific_cap = "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock1,
                &serde_json::json!(["cap:in=media:;out=media:", specific_cap]),
                &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock0, engine_sock1], test_cap_registry()).await.unwrap();

        // Specific request for PDF thumbnail
        let request = "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";

        // Without preference: routes to master 1 (specific, closest-specificity)
        assert_eq!(switch.find_master_for_cap(request, None).await, Some(1));

        // With preference for generic cap: routes to master 0 (generic, via is_equivalent)
        assert_eq!(switch.find_master_for_cap(request, Some(generic_cap)).await, Some(0));

        // With preference for specific cap: routes to master 1 (specific, matches preference)
        assert_eq!(switch.find_master_for_cap(request, Some(specific_cap)).await, Some(1));
    }

    // TEST438: find_master_for_cap with preference falls back to closest-specificity
    //          when preferred cap is not in the comparable set
    #[tokio::test]
    async fn test438_preferred_cap_falls_back_when_not_comparable() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        // Master 0: only has a specific cap
        let registered = "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:", registered]),
                &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        let request = "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";

        // Preference for an unrelated cap — no equivalent match, falls back to closest-specificity
        let unrelated = "cap:in=\"media:txt;textable\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        assert_eq!(switch.find_master_for_cap(request, Some(unrelated)).await, Some(0));
    }

    // TEST439: Generic provider CAN dispatch specific request
    //          (but only matches if no more specific provider exists)
    //
    // With is_dispatchable: generic provider (in=media:) CAN handle specific
    // request (in="media:pdf") because media: accepts any input type.
    // With preference, can route to generic even when more specific exists.
    #[tokio::test]
    async fn test439_generic_provider_can_dispatch_specific_request() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        // Master 0: only generic handler (in=media: wildcard)
        let generic_cap = "cap:in=media:;op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:", generic_cap]),
                &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        // Specific PDF request — generic handler CAN dispatch it
        // because provider's wildcard input (media:) accepts any input type
        let request = "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"";
        assert_eq!(switch.find_master_for_cap(request, None).await, Some(0),
            "Generic provider can dispatch specific request as fallback");

        // With preference for generic — routes to master 0
        assert_eq!(switch.find_master_for_cap(request, Some(generic_cap)).await, Some(0),
            "Preference routes to generic provider");
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST487: RelaySwitch construction verifies identity through relay chain
    #[tokio::test]
    async fn test487_relay_switch_identity_verification_succeeds() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            slave_notify_with_identity(slave_sock,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=test;out=\"media:void\""]),
                &Limits::default()).await;
        });

        let switch = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await.unwrap();

        // Construction succeeded — caps are populated
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None).await, Some(0));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=test;out=\"media:void\"", None).await, Some(0));
    }

    // TEST488: RelaySwitch construction fails when master's identity verification fails
    #[tokio::test]
    async fn test488_relay_switch_identity_verification_fails() {
        let (engine_sock, slave_sock) = UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let (read_half, write_half) = slave_sock.into_split();
            let mut reader = FrameReader::new(BufReader::new(read_half));
            let mut writer = FrameWriter::new(BufWriter::new(write_half));

            // Send RelayNotify
            let caps = serde_json::json!({
                "caps": ["cap:in=media:;out=media:"],
                "installed_plugins": [],
            });
            let notify = Frame::relay_notify(&serde_json::to_vec(&caps).unwrap(), &Limits::default());
            writer.write(&notify).await.unwrap();

            // Read identity REQ, respond with ERR
            let req = reader.read().await.unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(req.id, "BROKEN", "identity verification broken");
            writer.write(&err).await.unwrap();
        });

        let result = RelaySwitch::new(vec![engine_sock], test_cap_registry()).await;
        assert!(result.is_err(), "construction must fail when identity verification fails");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("identity verification failed"),
            "error must mention identity verification: {}", err);
    }

    // TEST905: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessPluginHost roundtrip
    #[tokio::test]
    async fn test905_send_to_master_build_request_frames_roundtrip() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter, accumulate_input};
        use crate::bifaci::plugin_runtime::PeerInvoker;
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Echo handler: accumulates input, echoes raw bytes back.
        #[derive(Debug)]
        struct EchoHandler;

        #[async_trait]
        impl FrameHandler for EchoHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                match accumulate_input(&mut input).await {
                    Ok((args, meta)) => {
                        let data: Vec<u8> = args.iter().flat_map(|a| a.value.clone()).collect();
                        output.emit_response_with_meta("media:text", &data, meta);
                    }
                    Err(e) => output.emit_error("ACCUMULATE_ERROR", &e),
                }
            }
        }

        let cap_urn_str = "cap:in=\"media:text\";op=echo;out=\"media:text\"";
        let cap = Cap {
            urn: crate::CapUrn::from_string(cap_urn_str).unwrap(),
            title: "echo".to_string(),
            cap_description: None,
            metadata: std::collections::HashMap::new(),
            command: String::new(),
            args: Vec::new(),
            output: None,
            media_specs: Vec::new(),
            metadata_json: None,
            registered_by: None,
        };

        let host = InProcessPluginHost::new(vec![(
            "echo".to_string(),
            vec![cap],
            std::sync::Arc::new(EchoHandler) as std::sync::Arc<dyn FrameHandler>,
        )]);

        // Create socket pairs (one for host↔slave, one for slave↔switch)
        let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
        let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

        let (host_read, host_write) = host_sock.into_split();
        let host_task = tokio::spawn(async move {
            host.run(host_read, host_write).await.unwrap();
        });

        let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
        let slave = RelaySlave::new(slave_local_read, slave_local_write);
        let (slave_read, slave_write) = slave_sock.into_split();
        let slave_task = tokio::spawn(async move {
            let socket_reader = FrameReader::new(BufReader::new(slave_read));
            let socket_writer = FrameWriter::new(BufWriter::new(slave_write));
            slave.run(socket_reader, socket_writer, None).await.unwrap();
        });

        let switch = RelaySwitch::new(vec![switch_sock], test_cap_registry()).await.unwrap();

        // Verify the switch has our echo cap registered
        let caps_json: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(caps_json.iter().any(|c| c.contains("echo")),
            "switch should have echo cap, got: {:?}", caps_json);

        // Build request frames using the helper
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().await.max_chunk;
        let frames = CapArgumentValue::build_request_frames(
            &rid,
            cap_urn_str,
            &[CapArgumentValue::new("media:text", b"hello streaming world".to_vec())],
            max_chunk,
        );

        // Send each frame via send_to_master (no preference)
        for frame in frames {
            switch.send_to_master(frame, None).await.unwrap();
        }

        // Read response frames via read_from_masters_timeout
        // Response chunks are CBOR-encoded (matching emit_response)
        let mut response_data = Vec::new();
        let mut got_end = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch.read_from_masters_timeout(std::time::Duration::from_millis(200)).await {
                Ok(Some(frame)) if frame.id == rid => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            if let Some(payload) = &frame.payload {
                                // CBOR-decode chunk payload to get raw bytes
                                let value: ciborium::Value = ciborium::from_reader(&payload[..])
                                    .expect("response chunk not valid CBOR");
                                match value {
                                    ciborium::Value::Bytes(b) => response_data.extend_from_slice(&b),
                                    ciborium::Value::Text(s) => response_data.extend_from_slice(s.as_bytes()),
                                    other => panic!("unexpected CBOR type in response: {:?}", other),
                                }
                            }
                        }
                        FrameType::End => {
                            got_end = true;
                            break;
                        }
                        FrameType::Err => {
                            panic!("Got ERR: [{:?}] {:?}", frame.error_code(), frame.error_message());
                        }
                        _ => {} // STREAM_START, STREAM_END — skip
                    }
                }
                Ok(Some(_)) => {} // Frame for different RID (e.g., RelayNotify)
                Ok(None) => {}    // Timeout — retry
                Err(e) => panic!("read_from_masters_timeout error: {}", e),
            }
        }

        assert!(got_end, "should have received END frame");
        assert_eq!(response_data, b"hello streaming world", "echo handler should return input");

        drop(switch);
        drop(slave_task);
        drop(host_task);
    }

    // TEST489: add_master dynamically connects new host to running switch
    #[tokio::test]
    async fn test489_add_master_dynamic() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter};
        use crate::bifaci::plugin_runtime::PeerInvoker;
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Handler that returns a constant byte string (ignores input).
        #[derive(Debug)]
        struct ConstHandler(&'static str);

        #[async_trait]
        impl FrameHandler for ConstHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End { break; }
                }
                output.emit_response("media:", self.0.as_bytes());
            }
        }

        // Helper to wire up host + slave and return switch-side socket + task handles
        async fn wire_host(
            host: InProcessPluginHost,
        ) -> (UnixStream, tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
            let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
            let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

            let (host_read, host_write) = host_sock.into_split();
            let host_task = tokio::spawn(async move {
                host.run(host_read, host_write).await.unwrap();
            });

            let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let (slave_read, slave_write) = slave_sock.into_split();
            let slave_task = tokio::spawn(async move {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).await.unwrap();
            });

            (switch_sock, host_task, slave_task)
        }

        // Create initial switch with handler A
        let cap_a = "cap:in=\"media:void\";op=alpha;out=\"media:void\"";
        let host_a = InProcessPluginHost::new(vec![(
            "alpha".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_a).unwrap(),
                title: "alpha".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(ConstHandler("alpha")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        let (switch_sock_a, ht_a, st_a) = wire_host(host_a).await;
        let switch = RelaySwitch::new(vec![switch_sock_a], test_cap_registry()).await.unwrap();
        assert_eq!(switch.masters.read().await.len(), 1);

        // Add handler B dynamically
        let cap_b = "cap:in=\"media:void\";op=beta;out=\"media:void\"";
        let host_b = InProcessPluginHost::new(vec![(
            "beta".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_b).unwrap(),
                title: "beta".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(ConstHandler("beta")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        let (switch_sock_b, ht_b, st_b) = wire_host(host_b).await;
        let idx = switch.add_master(switch_sock_b).await.unwrap();
        assert_eq!(idx, 1);
        assert_eq!(switch.masters.read().await.len(), 2);

        // Verify both caps are in aggregate capabilities
        let caps_json: Vec<String> = serde_json::from_slice(&switch.capabilities().await).unwrap();
        assert!(caps_json.iter().any(|c| c.contains("alpha")));
        assert!(caps_json.iter().any(|c| c.contains("beta")));

        // Execute against beta (dynamically added master) using send_to_master + build_request_frames
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().await.max_chunk;
        let frames = CapArgumentValue::build_request_frames(&rid, cap_b, &[], max_chunk);
        for frame in frames {
            switch.send_to_master(frame, None).await.unwrap();
        }

        // Response chunks are CBOR-encoded
        let mut response_data = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch.read_from_masters_timeout(std::time::Duration::from_millis(200)).await {
                Ok(Some(frame)) if frame.id == rid => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            if let Some(p) = &frame.payload {
                                let value: ciborium::Value = ciborium::from_reader(&p[..])
                                    .expect("response chunk not valid CBOR");
                                match value {
                                    ciborium::Value::Bytes(b) => response_data.extend_from_slice(&b),
                                    other => panic!("unexpected CBOR: {:?}", other),
                                }
                            }
                        }
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {}
                Err(e) => panic!("read error: {}", e),
            }
        }

        assert_eq!(response_data, b"beta");

        drop(switch);
        drop(st_a);
        drop(ht_a);
        drop(st_b);
        drop(ht_b);
    }

    // TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match
    #[tokio::test]
    async fn test666_preferred_cap_routing() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter};
        use crate::bifaci::plugin_runtime::PeerInvoker;
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::definition::Cap;
        use async_trait::async_trait;
        use tokio::sync::mpsc;

        /// Handler that returns a marker string identifying itself
        #[derive(Debug)]
        struct MarkerHandler(&'static str);

        #[async_trait]
        impl FrameHandler for MarkerHandler {
            async fn handle_request(
                &self,
                _cap_urn: &str,
                mut input: mpsc::UnboundedReceiver<Frame>,
                output: ResponseWriter,
                _peer: Arc<dyn PeerInvoker>,
            ) {
                // Drain input
                while let Some(frame) = input.recv().await {
                    if frame.frame_type == FrameType::End { break; }
                }
                output.emit_response("media:", self.0.as_bytes());
            }
        }

        // Helper to wire up host + slave
        async fn wire_host(
            host: InProcessPluginHost,
        ) -> (UnixStream, tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
            let (host_sock, slave_local_sock) = UnixStream::pair().unwrap();
            let (slave_sock, switch_sock) = UnixStream::pair().unwrap();

            let (host_read, host_write) = host_sock.into_split();
            let host_task = tokio::spawn(async move {
                host.run(host_read, host_write).await.unwrap();
            });

            let (slave_local_read, slave_local_write) = slave_local_sock.into_split();
            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let (slave_read, slave_write) = slave_sock.into_split();
            let slave_task = tokio::spawn(async move {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).await.unwrap();
            });

            (switch_sock, host_task, slave_task)
        }

        // Master 1: Exact-match handler (matches request exactly — closest specificity)
        let cap_exact = "cap:in=\"media:void\";op=test;out=\"media:void\"";
        let host_exact = InProcessPluginHost::new(vec![(
            "exact".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_exact).unwrap(),
                title: "exact".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(MarkerHandler("EXACT")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        // Master 2: More-specific handler (has extra tag — also matches, but further from request)
        let cap_extra = "cap:in=\"media:void\";op=test;ext=pdf;out=\"media:void\"";
        let host_extra = InProcessPluginHost::new(vec![(
            "extra".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_extra).unwrap(),
                title: "extra".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(MarkerHandler("EXTRA")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        let (switch_sock_exact, ht_exact, st_exact) = wire_host(host_exact).await;
        let (switch_sock_extra, ht_extra, st_extra) = wire_host(host_extra).await;

        let switch = RelaySwitch::new(vec![switch_sock_exact, switch_sock_extra], test_cap_registry()).await.unwrap();
        assert_eq!(switch.masters.read().await.len(), 2);

        // Test 1: Without preferred_cap, routes to exact match (closest specificity)
        let req_cap = "cap:in=\"media:void\";op=test;out=\"media:void\"";
        let req1 = Frame::req(MessageId::Uint(1), req_cap, Vec::new(), "application/octet-stream");

        switch.send_to_master(req1.clone(), None).await.unwrap();
        switch.send_to_master(Frame::end(MessageId::Uint(1), None), None).await.unwrap();

        let mut response_data1 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters().await {
                Ok(Some(frame)) => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            // Chunk payload is CBOR-encoded bytes — decode it
                            let payload = frame.payload.as_ref().unwrap();
                            let val: ciborium::Value = ciborium::from_reader(payload.as_slice()).unwrap();
                            if let ciborium::Value::Bytes(b) = val {
                                response_data1.extend_from_slice(&b);
                            }
                        }
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Test 2: With preferred_cap = cap_extra, routes to extra handler (preferred override)
        let req2 = Frame::req(MessageId::Uint(2), req_cap, Vec::new(), "application/octet-stream");

        switch.send_to_master(req2.clone(), Some(cap_extra)).await.unwrap();
        switch.send_to_master(Frame::end(MessageId::Uint(2), None), None).await.unwrap();

        let mut response_data2 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters().await {
                Ok(Some(frame)) => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            let payload = frame.payload.as_ref().unwrap();
                            let val: ciborium::Value = ciborium::from_reader(payload.as_slice()).unwrap();
                            if let ciborium::Value::Bytes(b) = val {
                                response_data2.extend_from_slice(&b);
                            }
                        }
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Verify routing: without preference routes to exact match (closest), with preference routes to extra (override)
        assert_eq!(response_data1, b"EXACT", "Without preferred_cap, should route to exact-match handler (closest specificity)");
        assert_eq!(response_data2, b"EXTRA", "With preferred_cap, should route to extra handler (preferred override)");

        drop(switch);
        drop(st_exact);
        drop(ht_exact);
        drop(st_extra);
        drop(ht_extra);
    }
}
