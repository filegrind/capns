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
//! - REQ: route by cap URN using `request_urn.accepts(registered_urn)`
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

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter, identity_nonce};
use crate::bifaci::relay::RelayMaster;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::sync::mpsc;

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

/// Connection to a single RelayMaster with its socket I/O.
#[derive(Debug)]
struct MasterConnection {
    /// Writer for frames to slave
    socket_writer: FrameWriter<BufWriter<UnixStream>>,
    /// Seq assigner for frames written to this master (output stage)
    seq_assigner: SeqAssigner,
    /// Latest manifest from RelayNotify
    manifest: Vec<u8>,
    /// Latest limits from RelayNotify
    limits: Limits,
    /// Parsed capability URNs from manifest
    caps: Vec<String>,
    /// Connection health status
    healthy: bool,
    /// Reader thread handle
    reader_handle: Option<std::thread::JoinHandle<()>>,
}

/// RelaySwitch — Cap-aware routing multiplexer for multiple RelayMasters.
///
/// Aggregates capabilities from multiple RelayMasters and routes requests
/// based on cap URN matching. Handles both engine→plugin and plugin→plugin
/// (peer) invocations with correct routing semantics.
#[derive(Debug)]
pub struct RelaySwitch {
    /// Managed relay master connections
    masters: Vec<MasterConnection>,
    /// Routing: cap_urn → master index
    cap_table: Vec<(String, usize)>,
    /// Routing: (xid, rid) → source/destination masters
    /// Only populated when XID is present (between RelaySwitch hops)
    request_routing: HashMap<(MessageId, MessageId), RoutingEntry>,
    /// Peer-initiated request (xid, rid) pairs for cleanup tracking
    peer_requests: HashSet<(MessageId, MessageId)>,
    /// Origin tracking: (xid, rid) → upstream connection index (None = external caller)
    /// Used to know where to send frames back
    origin_map: HashMap<(MessageId, MessageId), Option<usize>>,
    /// Response channels for external execute_cap calls: (xid, rid) → sender
    external_response_channels: HashMap<(MessageId, MessageId), mpsc::Sender<Frame>>,
    /// Aggregate capabilities (union of all masters)
    aggregate_capabilities: Vec<u8>,
    /// Negotiated limits (minimum across all masters)
    negotiated_limits: Limits,
    /// Channel receiver for frames from master reader threads
    frame_rx: mpsc::Receiver<(usize, Result<Frame, CborError>)>,
    /// Channel sender for spawning new reader threads (stored for add_master)
    frame_tx: mpsc::Sender<(usize, Result<Frame, CborError>)>,
    /// XID counter for assigning unique routing IDs (RelaySwitch assigns on first arrival)
    xid_counter: u64,
    /// RID → XID mapping for engine-initiated requests (so continuation frames can find their XID)
    rid_to_xid: HashMap<MessageId, MessageId>,
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl RelaySwitch {
    /// Create a new RelaySwitch with the given socket pairs.
    ///
    /// Each tuple is (read_stream, write_stream) for one RelayMaster.
    /// Performs handshake with all masters and builds initial capability table.
    pub fn new(sockets: Vec<(UnixStream, UnixStream)>) -> Result<Self, RelaySwitchError> {
        let mut masters = Vec::new();
        let (frame_tx, frame_rx) = mpsc::channel();
        let mut xid_counter: u64 = 0;

        // Phase 1: For each master, read RelayNotify and verify identity (blocking).
        // Reader threads are spawned only after verification succeeds.
        let mut pending_readers: Vec<(usize, FrameReader<BufReader<UnixStream>>)> = Vec::new();

        for (master_idx, (read_sock, write_sock)) in sockets.into_iter().enumerate() {
            let mut socket_reader = FrameReader::new(BufReader::new(read_sock));
            let mut socket_writer = FrameWriter::new(BufWriter::new(write_sock));

            // Read RelayNotify (blocking — first frame from each master)
            let notify_frame = socket_reader.read()
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

            let mut caps = parse_caps_from_relay_notify(&caps_payload)?;
            let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

            // Verify identity through the relay chain. This is done inline (not via
            // verify_identity) because RelaySwitch is sync and needs its own
            // XID allocation + SeqAssigner per-master for the relay chain.
            let mut seq_assigner = SeqAssigner::new();
            xid_counter += 1;
            let xid = MessageId::Uint(xid_counter);
            {
                use crate::standard::caps::CAP_IDENTITY;

                let nonce = identity_nonce();
                let req_id = MessageId::new_uuid();
                let stream_id = "identity-verify".to_string();

                // Send REQ + STREAM_START + CHUNK + STREAM_END + END with XID + seq
                let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
                req.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut req);
                socket_writer.write(&req).map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut ss = Frame::stream_start(req_id.clone(), stream_id.clone(), "media:bytes".to_string());
                ss.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut ss);
                socket_writer.write(&ss).map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let checksum = Frame::compute_checksum(&nonce);
                let mut chunk = Frame::chunk(req_id.clone(), stream_id.clone(), 0, nonce.clone(), 0, checksum);
                chunk.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut chunk);
                socket_writer.write(&chunk).map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut se = Frame::stream_end(req_id.clone(), stream_id, 1);
                se.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut se);
                socket_writer.write(&se).map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                let mut end = Frame::end(req_id.clone(), None);
                end.routing_id = Some(xid.clone());
                seq_assigner.assign(&mut end);
                socket_writer.write(&end).map_err(|e| RelaySwitchError::Protocol(format!(
                    "master {}: identity verification send failed: {}", master_idx, e
                )))?;

                seq_assigner.remove(&FlowKey { rid: req_id.clone(), xid: Some(xid.clone()) });

                // Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
                let mut accumulated = Vec::new();
                loop {
                    let frame = socket_reader.read()
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
                                caps = parse_caps_from_relay_notify(&caps_payload)?;
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
                socket_writer,
                seq_assigner,
                manifest: caps_payload,
                limits,
                caps,
                healthy: true,
                reader_handle: None, // Spawned in phase 2
            });
        }

        // Phase 2: All masters verified — spawn reader threads
        for (master_idx, socket_reader) in pending_readers {
            let tx = frame_tx.clone();
            let reader_handle = std::thread::spawn(move || {
                let mut reader = socket_reader;
                loop {
                    match reader.read() {
                        Ok(Some(frame)) => {
                            if tx.send((master_idx, Ok(frame))).is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            let _ = tx.send((master_idx, Err(e)));
                            break;
                        }
                    }
                }
            });
            masters[master_idx].reader_handle = Some(reader_handle);
        }

        let mut switch = Self {
            masters,
            cap_table: Vec::new(),
            request_routing: HashMap::new(),
            peer_requests: HashSet::new(),
            origin_map: HashMap::new(),
            external_response_channels: HashMap::new(),
            aggregate_capabilities: Vec::new(),
            negotiated_limits: Limits::default(),
            frame_rx,
            frame_tx,
            xid_counter,
            rid_to_xid: HashMap::new(),
        };

        // Build routing tables from already-populated caps
        switch.rebuild_cap_table();
        switch.rebuild_capabilities();
        switch.rebuild_limits();

        Ok(switch)
    }

    /// Get the aggregate capabilities of all healthy masters.
    pub fn capabilities(&self) -> &[u8] {
        &self.aggregate_capabilities
    }

    /// Get the negotiated limits (minimum across all masters).
    pub fn limits(&self) -> &Limits {
        &self.negotiated_limits
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
    /// A receiver that streams response frames. The caller should read from this receiver
    /// until it receives an END or ERR frame.
    ///
    /// # Example
    /// ```ignore
    /// let receiver = switch.execute_cap(
    ///     "cap:in=\"media:void\";op=test;out=\"media:void\"",
    ///     vec![],
    ///     "application/cbor"
    /// )?;
    ///
    /// // Read responses until END
    /// while let Ok(frame) = receiver.recv() {
    ///     match frame.frame_type {
    ///         FrameType::End => {
    ///             println!("Got final response: {:?}", frame.payload);
    ///             break;
    ///         }
    ///         FrameType::Err => {
    ///             eprintln!("Got error: {:?}", frame.payload);
    ///             break;
    ///         }
    ///         _ => {
    ///             // Handle streaming frames
    ///         }
    ///     }
    /// }
    /// ```
    pub fn execute_cap(
        &mut self,
        cap_urn: &str,
        payload: Vec<u8>,
        content_type: &str,
    ) -> Result<mpsc::Receiver<Frame>, RelaySwitchError> {
        // Generate unique request ID
        self.xid_counter += 1;
        let rid = MessageId::Uint(self.xid_counter);

        // Build REQ frame
        let req_frame = Frame::req(rid.clone(), cap_urn, payload, content_type);

        // Create response channel
        let (tx, rx) = mpsc::channel();

        // Send the REQ frame - this will assign XID and route it
        // We need to register the response channel BEFORE sending, because
        // responses might arrive immediately

        // Find master that can handle this cap (no preference for internal requests)
        let dest_idx = self.find_master_for_cap(cap_urn, None).ok_or_else(|| {
            RelaySwitchError::NoHandler(cap_urn.to_string())
        })?;

        // Assign XID
        self.xid_counter += 1;
        let xid = MessageId::Uint(self.xid_counter);
        let key = (xid.clone(), rid.clone());

        // Register response channel BEFORE sending
        self.external_response_channels.insert(key.clone(), tx);

        // Record origin (None = external execute_cap caller)
        self.origin_map.insert(key.clone(), None);

        // Register routing
        self.request_routing.insert(
            key.clone(),
            RoutingEntry {
                source_master_idx: None,
                destination_master_idx: dest_idx,
            },
        );

        // Record RID → XID mapping for continuation frames (if caller sends them)
        self.rid_to_xid.insert(rid.clone(), xid.clone());

        // Build frame with XID
        let mut frame_with_xid = req_frame;
        frame_with_xid.routing_id = Some(xid);

        // Forward to destination
        self.write_to_master_idx(dest_idx, &mut frame_with_xid)?;

        Ok(rx)
    }

    /// Dynamically add a new master connection to the switch.
    ///
    /// Performs handshake (reads RelayNotify, verifies identity) with the new master,
    /// spawns a reader thread, and returns the master index.
    ///
    /// This is used for dynamically connecting new hosts (e.g., Mac client connecting via gRPC).
    pub fn add_master(
        &mut self,
        read_sock: UnixStream,
        write_sock: UnixStream,
    ) -> Result<usize, RelaySwitchError> {
        let master_idx = self.masters.len();
        let mut socket_reader = FrameReader::new(BufReader::new(read_sock));
        let mut socket_writer = FrameWriter::new(BufWriter::new(write_sock));

        // Read RelayNotify
        let notify_frame = socket_reader.read()
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

        let mut caps = parse_caps_from_relay_notify(&caps_payload)?;
        let mut limits = notify_frame.relay_notify_limits().unwrap_or_default();

        // Identity verification (same as in new())
        let mut seq_assigner = SeqAssigner::new();
        self.xid_counter += 1;
        let xid = MessageId::Uint(self.xid_counter);
        {
            use crate::standard::caps::CAP_IDENTITY;

            let nonce = identity_nonce();
            let req_id = MessageId::new_uuid();
            let stream_id = "identity-verify".to_string();

            let mut req = Frame::req(req_id.clone(), CAP_IDENTITY, vec![], "application/cbor");
            req.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut req);
            socket_writer.write(&req).map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut ss = Frame::stream_start(req_id.clone(), stream_id.clone(), "media:bytes".to_string());
            ss.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut ss);
            socket_writer.write(&ss).map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let checksum = Frame::compute_checksum(&nonce);
            let mut chunk = Frame::chunk(req_id.clone(), stream_id.clone(), 0, nonce.clone(), 0, checksum);
            chunk.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut chunk);
            socket_writer.write(&chunk).map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut se = Frame::stream_end(req_id.clone(), stream_id, 1);
            se.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut se);
            socket_writer.write(&se).map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq_assigner.assign(&mut end);
            socket_writer.write(&end).map_err(|e| RelaySwitchError::Protocol(format!(
                "new master {}: identity send failed: {}", master_idx, e
            )))?;

            seq_assigner.remove(&FlowKey { rid: req_id.clone(), xid: Some(xid.clone()) });

            // Read response
            let mut accumulated = Vec::new();
            loop {
                let frame = socket_reader.read()
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
                            caps = parse_caps_from_relay_notify(&caps_payload)?;
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

        // Spawn reader thread
        let tx = self.frame_tx.clone();
        let reader_handle = std::thread::spawn(move || {
            let mut reader = socket_reader;
            loop {
                match reader.read() {
                    Ok(Some(frame)) => {
                        if tx.send((master_idx, Ok(frame))).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send((master_idx, Err(e)));
                        break;
                    }
                }
            }
        });

        self.masters.push(MasterConnection {
            socket_writer,
            seq_assigner,
            manifest: caps_payload,
            limits,
            caps,
            healthy: true,
            reader_handle: Some(reader_handle),
        });

        // Rebuild tables
        self.rebuild_cap_table();
        self.rebuild_capabilities();
        self.rebuild_limits();

        Ok(master_idx)
    }

    /// Send a frame to the appropriate master (engine → plugin direction).
    ///
    /// REQ frames: Assigned XID if absent, routed by cap URN.
    /// Continuation frames: Routed by (XID, RID) pair.
    /// Send a frame to the appropriate master.
    ///
    /// `preferred_cap`: when `Some`, uses `is_comparable` routing and prefers
    /// the master whose registered cap is equivalent to this URN.
    /// When `None`, uses standard `accepts` + closest-specificity routing.
    pub fn send_to_master(
        &mut self,
        mut frame: Frame,
        preferred_cap: Option<&str>,
    ) -> Result<(), RelaySwitchError> {
        eprintln!("[RelaySwitch] send_to_master: {:?} id={:?} cap={:?} xid={:?}",
            frame.frame_type, frame.id, frame.cap, frame.routing_id);
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Find master that can handle this cap
                let dest_idx = self.find_master_for_cap(cap_urn, preferred_cap).ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Assign XID if absent (first arrival at RelaySwitch)
                let xid = if let Some(ref existing_xid) = frame.routing_id {
                    existing_xid.clone()
                } else {
                    self.xid_counter += 1;
                    let new_xid = MessageId::Uint(self.xid_counter);
                    frame.routing_id = Some(new_xid.clone());
                    new_xid
                };

                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Record origin (None = external caller via send_to_master)
                self.origin_map.insert(key.clone(), None);

                // Register routing (xid, rid) → destination
                self.request_routing.insert(
                    key,
                    RoutingEntry {
                        source_master_idx: None,
                        destination_master_idx: dest_idx,
                    },
                );

                // Record RID → XID mapping for continuation frames from engine
                self.rid_to_xid.insert(rid, xid);

                // Forward to destination with XID
                self.write_to_master_idx(dest_idx, &mut frame)?;
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
                    let looked_up_xid = self.rid_to_xid.get(rid).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?;
                    frame.routing_id = Some(looked_up_xid.clone());
                    looked_up_xid.clone()
                };

                let key = (xid.clone(), frame.id.clone());

                let entry = self.request_routing.get(&key).ok_or_else(|| {
                    RelaySwitchError::UnknownRequest(frame.id.clone())
                })?;

                let dest_idx = entry.destination_master_idx;

                // Forward to destination
                self.write_to_master_idx(dest_idx, &mut frame)?;

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
    /// Blocks until a frame is available from any master.  Returns Ok(None) when all masters have closed.
    /// Peer requests (plugin → plugin) are handled internally and not returned.
    pub fn read_from_masters(&mut self) -> Result<Option<Frame>, RelaySwitchError> {
        loop {
            // Block on channel - reader threads send frames here
            match self.frame_rx.recv() {
                Ok((master_idx, Ok(frame))) => {
                    // Got a frame from a master
                    if let Some(result_frame) = self.handle_master_frame(master_idx, frame)? {
                        return Ok(Some(result_frame));
                    }
                    // Peer request was handled internally, continue reading
                }
                Ok((master_idx, Err(e))) => {
                    // Error reading from master
                    self.handle_master_death(master_idx)?;
                    // Continue reading from other masters
                }
                Err(mpsc::RecvError) => {
                    // All reader threads have exited (all senders dropped)
                    return Ok(None);
                }
            }
        }
    }

    /// Read the next frame from any master with timeout (plugin → engine direction).
    ///
    /// Like read_from_masters() but returns Ok(None) after timeout instead of blocking forever.
    /// Returns Ok(Some(frame)) if a frame arrives, Ok(None) on timeout, Err on error.
    pub fn read_from_masters_timeout(&mut self, timeout: std::time::Duration) -> Result<Option<Frame>, RelaySwitchError> {
        let start = std::time::Instant::now();
        loop {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Ok(None); // Timeout
            }

            // Try to receive with timeout
            match self.frame_rx.recv_timeout(remaining) {
                Ok((master_idx, Ok(frame))) => {
                    // Got a frame from a master
                    if let Some(result_frame) = self.handle_master_frame(master_idx, frame)? {
                        return Ok(Some(result_frame));
                    }
                    // Peer request was handled internally, continue reading
                }
                Ok((master_idx, Err(e))) => {
                    // Error reading from master
                    self.handle_master_death(master_idx)?;
                    // Continue reading from other masters
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Ok(None); // Timeout
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // All reader threads have exited (all senders dropped)
                    return Err(RelaySwitchError::Protocol("All masters disconnected".to_string()));
                }
            }
        }
    }

    // =========================================================================
    // FRAME OUTPUT (all writes to masters go through this)
    // =========================================================================

    /// Write a frame to a master, assigning seq via the per-master SeqAssigner.
    /// Cleans up seq tracking on terminal frames (END/ERR).
    fn write_to_master_idx(&mut self, master_idx: usize, frame: &mut Frame) -> Result<(), CborError> {
        eprintln!("[RelaySwitch] write_to_master_idx: master={} {:?} id={:?} xid={:?}",
            master_idx, frame.frame_type, frame.id, frame.routing_id);
        let master = &mut self.masters[master_idx];
        master.seq_assigner.assign(frame);
        master.socket_writer.write(frame)?;
        if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
            master.seq_assigner.remove(&FlowKey::from_frame(frame));
        }
        Ok(())
    }

    // =========================================================================
    // INTERNAL ROUTING
    // =========================================================================

    /// Find which master handles a given cap URN.
    ///
    /// ## Without preference (`preferred_cap = None`)
    ///
    /// Uses `request.accepts(registered)` with closest-specificity matching.
    /// This is the standard routing mode: specific requests route to specific
    /// handlers, generic requests route to generic handlers.
    ///
    /// ## With preference (`preferred_cap = Some(cap_urn)`)
    ///
    /// Uses `is_comparable` (order-theoretic: `a.accepts(b) || b.accepts(a)`)
    /// to find ALL masters on the same specialization chain as the request.
    /// This is broader than `accepts` — it also finds masters whose caps are
    /// more general than the request.
    ///
    /// Among the comparable matches, the master whose registered cap is
    /// equivalent to the preferred cap wins. If no equivalent match, falls
    /// back to closest-specificity among the comparable set.
    fn find_master_for_cap(&self, cap_urn: &str, preferred_cap: Option<&str>) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Parse preferred cap URN if provided
        let preferred_urn = preferred_cap.and_then(|p| crate::CapUrn::from_string(p).ok());

        // Collect ALL matching masters with their specificity scores.
        // When preferred_cap is set, use is_comparable (broader); otherwise accepts (standard).
        let mut matches: Vec<(usize, usize, bool)> = Vec::new(); // (master_idx, specificity, is_preferred)

        for (registered_cap, master_idx) in &self.cap_table {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                let is_match = if preferred_urn.is_some() {
                    // Comparable: either side accepts the other (broader match set)
                    request_urn.accepts(&registered_urn) || registered_urn.accepts(&request_urn)
                } else {
                    // Standard: request is pattern, registered cap is instance
                    request_urn.accepts(&registered_urn)
                };

                if is_match {
                    let specificity = registered_urn.specificity();
                    // Check if this registered cap is equivalent to the preferred cap
                    let is_preferred = preferred_urn.as_ref().map_or(false, |pref| {
                        pref.accepts(&registered_urn) && registered_urn.accepts(pref)
                    });
                    matches.push((*master_idx, specificity, is_preferred));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // If any match is preferred, pick the first preferred match.
        if let Some(&(idx, _, _)) = matches.iter().find(|(_, _, pref)| *pref) {
            return Some(idx);
        }

        // Fall back to closest-specificity (ties broken by first match).
        let min_distance = matches.iter()
            .map(|(_, s, _)| (*s as isize - request_specificity as isize).unsigned_abs())
            .min()
            .unwrap();

        matches
            .iter()
            .find(|(_, s, _)| (*s as isize - request_specificity as isize).unsigned_abs() == min_distance)
            .map(|(idx, _, _)| *idx)
    }

    /// Handle a frame arriving from a master (plugin → engine direction).
    ///
    /// Returns Some(frame) if the frame should be forwarded to the engine.
    /// Returns None if the frame was handled internally (peer request).
    fn handle_master_frame(
        &mut self,
        source_idx: usize,
        mut frame: Frame,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        eprintln!("[RelaySwitch] handle_master_frame: from_master={} {:?} id={:?} cap={:?} xid={:?}",
            source_idx, frame.frame_type, frame.id, frame.cap, frame.routing_id);
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;


                // Find destination master (no preference for peer requests)
                let dest_idx = self.find_master_for_cap(cap_urn, None).ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Assign XID if absent (first arrival at RelaySwitch)
                // REQs from plugins should NOT have XID (per spec)
                if frame.routing_id.is_some() {
                    return Err(RelaySwitchError::Protocol(
                        "REQ from plugin should not have XID".to_string()
                    ));
                }

                self.xid_counter += 1;
                let xid = MessageId::Uint(self.xid_counter);
                frame.routing_id = Some(xid.clone());


                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Record RID → XID mapping for continuation frames
                self.rid_to_xid.insert(rid.clone(), xid.clone());

                // Record origin (where this request came from)
                self.origin_map.insert(key.clone(), Some(source_idx));

                // Register routing
                self.request_routing.insert(
                    key.clone(),
                    RoutingEntry {
                        source_master_idx: Some(source_idx),
                        destination_master_idx: dest_idx,
                    },
                );

                // Mark as peer request (for cleanup tracking)
                self.peer_requests.insert(key);

                // Forward to destination with XID
                self.write_to_master_idx(dest_idx, &mut frame)?;

                // Do NOT return to engine (internal routing)
                Ok(None)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Log => {
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
                    let entry = self.request_routing.get(&key).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?;

                    // Get origin (where request came from)
                    let origin_idx = self.origin_map.get(&key).copied().ok_or_else(|| {
                        RelaySwitchError::Protocol("No origin recorded for request".to_string())
                    })?;

                    let is_terminal = frame.frame_type == FrameType::End
                        || frame.frame_type == FrameType::Err;

                    // Route back to origin
                    match origin_idx {
                        None => {
                            // External caller (via send_to_master or execute_cap)
                            // Check if there's a response channel registered
                            if let Some(tx) = self.external_response_channels.get(&key) {
                                // Send to external response channel (keep XID for now)
                                let _ = tx.send(frame.clone());

                                // Cleanup on terminal frame
                                if is_terminal {
                                    self.external_response_channels.remove(&key);
                                    self.request_routing.remove(&key);
                                    self.origin_map.remove(&key);
                                    self.peer_requests.remove(&key);
                                    self.rid_to_xid.remove(&rid);
                                }

                                return Ok(None);
                            } else {
                                // No response channel (sent via send_to_master, not execute_cap)
                                // Strip XID and return to caller (final leg)
                                frame.routing_id = None;

                                // Cleanup on terminal frame
                                if is_terminal {
                                    self.request_routing.remove(&key);
                                    self.origin_map.remove(&key);
                                    self.peer_requests.remove(&key);
                                    self.rid_to_xid.remove(&rid);
                                }

                                return Ok(Some(frame));
                            }
                        }
                        Some(master_idx) => {
                            // Route back to source master — KEEP XID.
                            // The source master's PluginHost needs XID to not drop the frame
                            // (Swift PluginHost requires XID on all relay frames).
                            // The PluginHost uses outgoingRids[RID] for peer response routing.
                            self.write_to_master_idx(master_idx, &mut frame)?;

                            // Cleanup on terminal frame
                            if is_terminal {
                                self.request_routing.remove(&key);
                                self.origin_map.remove(&key);
                                self.peer_requests.remove(&key);
                                self.rid_to_xid.remove(&rid);
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
                    let xid = self.rid_to_xid.get(&rid).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?.clone();

                    let key = (xid.clone(), rid.clone());

                    // Look up routing entry
                    let entry = self.request_routing.get(&key).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?;

                    // Add XID to frame for forwarding
                    frame.routing_id = Some(xid.clone());

                    // Forward to destination master (keep XID)
                    let dest_idx = entry.destination_master_idx;

                    self.write_to_master_idx(dest_idx, &mut frame)?;
                    return Ok(None);
                }
            }

            FrameType::RelayNotify => {
                // Capability update from host — update our cap table
                let caps_payload = frame.relay_notify_manifest()
                    .ok_or_else(|| RelaySwitchError::Protocol("RelayNotify has no payload".to_string()))?;

                let new_caps = parse_caps_from_relay_notify(caps_payload)?;

                // Update master's caps and limits
                if let Some(master) = self.masters.get_mut(source_idx) {
                    master.caps = new_caps;
                    master.manifest = caps_payload.to_vec();
                    // Extract and update limits from RelayNotify
                    if let Some(new_limits) = frame.relay_notify_limits() {
                        master.limits = new_limits;
                    }
                }
                // Rebuild cap_table, aggregate capabilities, and limits from all masters
                self.rebuild_cap_table();
                self.rebuild_capabilities();
                self.rebuild_limits();

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
    fn handle_master_death(&mut self, master_idx: usize) -> Result<(), RelaySwitchError> {
        if !self.masters[master_idx].healthy {
            return Ok(()); // Already handled
        }

        // Mark unhealthy
        self.masters[master_idx].healthy = false;

        // Find all pending requests for this master
        let mut dead_requests = Vec::new();
        for (key, entry) in &self.request_routing {
            if entry.destination_master_idx == master_idx {
                dead_requests.push((key.clone(), entry.source_master_idx));
            }
        }

        // Send ERR for each pending request
        for (key, source_idx) in dead_requests {
            let (xid, rid) = &key;

            // Create ERR frame
            let mut err_frame = Frame::err(
                rid.clone(),
                "MASTER_DIED",
                "Relay master connection closed",
            );
            err_frame.routing_id = Some(xid.clone());

            match source_idx {
                None => {
                    // External caller - send to response channel if exists, otherwise log
                    if let Some(tx) = self.external_response_channels.get(&key) {
                        let _ = tx.send(err_frame);
                        self.external_response_channels.remove(&key);
                    }
                }
                Some(master_idx) => {
                    // Send ERR back to source master
                    if self.masters[master_idx].healthy {
                        let _ = self.write_to_master_idx(master_idx, &mut err_frame);
                    }
                }
            }

            // Cleanup routing
            self.request_routing.remove(&key);
            self.origin_map.remove(&key);
            self.peer_requests.remove(&key);
        }

        // Rebuild tables
        self.rebuild_cap_table();
        self.rebuild_capabilities();
        self.rebuild_limits();

        Ok(())
    }

    // =========================================================================
    // TABLE MANAGEMENT
    // =========================================================================

    /// Rebuild cap_table from all healthy masters.
    fn rebuild_cap_table(&mut self) {
        self.cap_table.clear();
        for (idx, master) in self.masters.iter().enumerate() {
            if master.healthy {
                for cap in &master.caps {
                    self.cap_table.push((cap.clone(), idx));
                }
            }
        }
    }

    /// Rebuild aggregate capabilities (union of all healthy masters).
    fn rebuild_capabilities(&mut self) {
        // Collect all caps from healthy masters
        let mut all_caps: Vec<String> = Vec::new();
        for master in &self.masters {
            if master.healthy {
                all_caps.extend(master.caps.iter().cloned());
            }
        }

        // Deduplicate
        all_caps.sort();
        all_caps.dedup();

        // Build manifest as JSON array (same format as RelayNotify payloads)
        self.aggregate_capabilities = serde_json::to_vec(&all_caps).unwrap_or_default();
    }

    /// Rebuild negotiated limits (minimum across all healthy masters).
    fn rebuild_limits(&mut self) {
        let mut min_max_frame = usize::MAX;
        let mut min_max_chunk = usize::MAX;

        for master in &self.masters {
            if master.healthy {
                min_max_frame = min_max_frame.min(master.limits.max_frame);
                min_max_chunk = min_max_chunk.min(master.limits.max_chunk);
            }
        }

        self.negotiated_limits = Limits {
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

/// Parse capability URNs from RelayNotify payload.
/// RelayNotify contains a simple JSON array of URN strings: ["cap:...", "cap:...", ...]
/// Validates that CAP_IDENTITY is present — hard-fail if missing.
fn parse_caps_from_relay_notify(notify_payload: &[u8]) -> Result<Vec<String>, RelaySwitchError> {
    use crate::urn::cap_urn::CapUrn;
    use crate::standard::caps::CAP_IDENTITY;

    // Deserialize simple JSON array of URN strings
    let cap_urns: Vec<String> = serde_json::from_slice(notify_payload)
        .map_err(|e| RelaySwitchError::Protocol(format!("Invalid RelayNotify capability array: {}", e)))?;

    // Verify CAP_IDENTITY is present — mandatory for every host
    let identity_urn = CapUrn::from_string(CAP_IDENTITY)
        .expect("BUG: CAP_IDENTITY constant is invalid");
    let has_identity = cap_urns.iter().any(|cap_str| {
        CapUrn::from_string(cap_str)
            .map(|cap_urn| identity_urn.conforms_to(&cap_urn))
            .unwrap_or(false)
    });
    if !has_identity {
        return Err(RelaySwitchError::Protocol(
            format!("RelayNotify missing required CAP_IDENTITY ({})", CAP_IDENTITY)
        ));
    }

    Ok(cap_urns)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::frame::{Frame, SeqAssigner};
    use crate::standard::caps::CAP_IDENTITY;
    use std::os::unix::net::UnixStream;

    /// Helper: send RelayNotify with given caps/limits, then handle identity verification.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    fn slave_notify_with_identity(
        read_sock: UnixStream,
        write_sock: UnixStream,
        caps_json: &serde_json::Value,
        limits: &Limits,
    ) -> (FrameReader<BufReader<UnixStream>>, FrameWriter<BufWriter<UnixStream>>) {
        let mut reader = FrameReader::new(BufReader::new(read_sock));
        let mut writer = FrameWriter::new(BufWriter::new(write_sock));

        // Send RelayNotify
        let notify = Frame::relay_notify(
            &serde_json::to_vec(caps_json).unwrap(),
            limits,
        );
        writer.write(&notify).unwrap();

        // Handle identity verification REQ
        let req = reader.read().unwrap().expect("expected identity REQ after RelayNotify");
        assert_eq!(req.frame_type, FrameType::Req, "first frame after RelayNotify must be identity REQ");

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

    // TEST429: Cap routing logic (find_master_for_cap)
    #[test]
    fn test429_find_master_for_cap() {
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        // Spawn slave 1 (identity cap only)
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default());
        });

        // Spawn slave 2 (identity + double cap)
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read2, slave_write2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default());
        });

        // Constructor reads RelayNotify + verifies identity for both masters
        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ]).unwrap();

        // Verify routing (caps already populated during construction)
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None), Some(0));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=double;out=\"media:void\"", None), Some(1));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=unknown;out=\"media:void\"", None), None);

        // Verify aggregate capabilities (plain JSON array)
        let cap_list: Vec<String> = serde_json::from_slice(switch.capabilities()).unwrap();
        assert_eq!(cap_list.len(), 2);
    }

    // TEST426: Single master REQ/response routing
    #[test]
    fn test426_single_master_req_response() {
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read, slave_write,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default());

            // Read REQ frame
            let (req_id, xid) = if let Some(frame) = reader.read().unwrap() {
                if frame.frame_type == FrameType::Req {
                    (Some(frame.id.clone()), frame.routing_id.clone())
                } else { (None, None) }
            } else { (None, None) };

            // Read END frame
            if let Some(frame) = reader.read().unwrap() {
                if frame.frame_type == FrameType::End && req_id.is_some() {
                    let rid = req_id.unwrap();
                    let xid_clone = xid.clone();
                    let mut response = Frame::end(rid.clone(), Some(vec![42]));
                    response.routing_id = xid;
                    seq.assign(&mut response);
                    writer.write(&response).unwrap();
                    seq.remove(&FlowKey { rid: rid.clone(), xid: xid_clone });
                }
            }
        });

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Send REQ + END (caps already populated from construction)
        let req_id = MessageId::Uint(1);
        let req = Frame::req(req_id.clone(), "cap:in=media:;out=media:", vec![1, 2, 3], "text/plain");
        switch.send_to_master(req, None).unwrap();
        let end = Frame::end(req_id.clone(), None);
        switch.send_to_master(end, None).unwrap();

        let response = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.id, MessageId::Uint(1));
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST427: Multi-master cap routing
    #[test]
    fn test427_multi_master_cap_routing() {
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default());
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read2, slave_write2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default());
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let mut switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ]).unwrap();

        // Caps already populated from construction — send requests directly
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), "cap:in=media:;out=media:", vec![], "text/plain"), None).unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).unwrap();
        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        let req2_id = MessageId::Uint(2);
        switch.send_to_master(Frame::req(req2_id.clone(), "cap:in=\"media:void\";op=double;out=\"media:void\"", vec![], "text/plain"), None).unwrap();
        switch.send_to_master(Frame::end(req2_id, None), None).unwrap();
        let resp2 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![2]));
    }

    // TEST428: Unknown cap returns error
    #[test]
    fn test428_unknown_cap_returns_error() {
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read, slave_write,
                &serde_json::json!(["cap:in=media:;out=media:"]), &Limits::default());
        });

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        let req = Frame::req(MessageId::Uint(1), "cap:in=\"media:void\";op=unknown;out=\"media:void\"", vec![], "text/plain");
        let result = switch.send_to_master(req, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }

    // TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent)
    #[test]
    fn test430_tie_breaking_same_cap_multiple_masters() {
        let same_cap = "cap:in=media:;out=media:";

        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!([same_cap]), &Limits::default());
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![1]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read2, slave_write2,
                &serde_json::json!([same_cap]), &Limits::default());
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![2]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let mut switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ]).unwrap();

        // First request — should go to master 0 (first match)
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), same_cap, vec![], "text/plain"), None).unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).unwrap();
        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        // Second request — should ALSO go to master 0 (consistent routing)
        let req2_id = MessageId::Uint(2);
        switch.send_to_master(Frame::req(req2_id.clone(), same_cap, vec![], "text/plain"), None).unwrap();
        switch.send_to_master(Frame::end(req2_id, None), None).unwrap();
        let resp2 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![1]));
    }

    // TEST431: Continuation frame routing (CHUNK, END follow REQ)
    #[test]
    fn test431_continuation_frame_routing() {
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read, slave_write,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=test;out=\"media:void\""]),
                &Limits::default());

            let req = reader.read().unwrap().unwrap();
            assert_eq!(req.frame_type, FrameType::Req);
            let xid = req.routing_id.clone();

            let chunk = reader.read().unwrap().unwrap();
            assert_eq!(chunk.frame_type, FrameType::Chunk);
            assert_eq!(chunk.id, req.id);

            let end = reader.read().unwrap().unwrap();
            assert_eq!(end.frame_type, FrameType::End);
            assert_eq!(end.id, req.id);

            let rid = req.id.clone();
            let xid_clone = xid.clone();
            let mut response = Frame::end(rid.clone(), Some(vec![42]));
            response.routing_id = xid;
            seq.assign(&mut response);
            writer.write(&response).unwrap();
            seq.remove(&FlowKey { rid: rid.clone(), xid: xid_clone });
        });

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        let req_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req_id.clone(), "cap:in=\"media:void\";op=test;out=\"media:void\"", vec![], "text/plain"), None).unwrap();
        let payload = vec![1, 2, 3];
        let checksum = Frame::compute_checksum(&payload);
        switch.send_to_master(Frame::chunk(req_id.clone(), "stream1".to_string(), 0, payload, 0, checksum), None).unwrap();
        switch.send_to_master(Frame::end(req_id.clone(), None), None).unwrap();

        let response = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST432: Empty masters list creates empty switch, add_master works
    #[test]
    fn test432_empty_masters_allowed() {
        let switch = RelaySwitch::new(vec![]).unwrap();

        // Empty switch has no caps
        let caps: Vec<String> = serde_json::from_slice(switch.capabilities()).unwrap();
        assert!(caps.is_empty(), "empty switch should have no caps");

        // No handler for any cap
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None), None);
    }

    // TEST433: Capability aggregation deduplicates caps
    #[test]
    fn test433_capability_aggregation_deduplicates() {
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=double;out=\"media:void\""]),
                &Limits::default());
        });

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read2, slave_write2,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=triple;out=\"media:void\""]),
                &Limits::default());
        });

        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ]).unwrap();

        // Caps already populated during construction (plain JSON array)
        let mut cap_list: Vec<String> = serde_json::from_slice(switch.capabilities()).unwrap();
        cap_list.sort();

        assert_eq!(cap_list.len(), 3);
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=double;out=\"media:void\"".to_string()));
        assert!(cap_list.contains(&"cap:in=media:;out=media:".to_string()));
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=triple;out=\"media:void\"".to_string()));
    }

    // TEST434: Limits negotiation takes minimum
    #[test]
    fn test434_limits_negotiation_minimum() {
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!(["cap:in=media:;out=media:"]),
                &Limits { max_frame: 1_000_000, max_chunk: 100_000, ..Limits::default() });
        });

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read2, slave_write2,
                &serde_json::json!(["cap:in=media:;out=media:"]),
                &Limits { max_frame: 2_000_000, max_chunk: 50_000, ..Limits::default() });
        });

        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ]).unwrap();

        // Limits already negotiated during construction
        assert_eq!(switch.limits().max_frame, 1_000_000);
        assert_eq!(switch.limits().max_chunk, 50_000);
    }

    // TEST435: URN matching (exact vs accepts())
    #[test]
    fn test435_urn_matching_exact_and_accepts() {
        let registered_cap = "cap:in=\"media:text;utf8\";op=process;out=\"media:text;utf8\"";

        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut seq = SeqAssigner::new();
            let (mut reader, mut writer) = slave_notify_with_identity(slave_read, slave_write,
                &serde_json::json!(["cap:in=media:;out=media:", registered_cap]), &Limits::default());
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let rid = frame.id.clone();
                        let xid = frame.routing_id.clone();
                        let mut response = Frame::end(rid.clone(), Some(vec![42]));
                        response.routing_id = xid.clone();
                        seq.assign(&mut response);
                        writer.write(&response).unwrap();
                        seq.remove(&FlowKey { rid: rid.clone(), xid });
                    }
                    Some(frame) if frame.frame_type == FrameType::End => {}
                    None => break,
                    _ => {}
                }
            }
        });

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Exact match should work
        let req1_id = MessageId::Uint(1);
        switch.send_to_master(Frame::req(req1_id.clone(), registered_cap, vec![], "text/plain"), None).unwrap();
        switch.send_to_master(Frame::end(req1_id, None), None).unwrap();
        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![42]));

        // More specific request should NOT match less specific registered cap
        let req2 = Frame::req(MessageId::Uint(2), "cap:in=\"media:text;utf8;normalized\";op=process;out=\"media:text\"", vec![], "text/plain");
        let result = switch.send_to_master(req2, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }

    // =========================================================================
    // Preferred cap routing tests
    // =========================================================================

    // TEST437: find_master_for_cap with preferred_cap routes to generic handler
    #[test]
    fn test437_preferred_cap_routes_to_generic() {
        let (engine_read0, slave_write0) = UnixStream::pair().unwrap();
        let (slave_read0, engine_write0) = UnixStream::pair().unwrap();
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();

        // Master 0: generic thumbnail handler (like internal ThumbnailProvider)
        let generic_cap = "cap:in=media:bytes;op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read0, slave_write0,
                &serde_json::json!(["cap:in=media:;out=media:", generic_cap]),
                &Limits::default());
        });

        // Master 1: specific thumbnail handler (like pdfcartridge)
        let specific_cap = "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read1, slave_write1,
                &serde_json::json!(["cap:in=media:;out=media:", specific_cap]),
                &Limits::default());
        });

        let switch = RelaySwitch::new(vec![
            (engine_read0, engine_write0),
            (engine_read1, engine_write1),
        ]).unwrap();

        // Specific request for PDF thumbnail
        let request = "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";

        // Without preference: routes to master 1 (specific, closest-specificity)
        assert_eq!(switch.find_master_for_cap(request, None), Some(1));

        // With preference for generic cap: routes to master 0 (generic, via is_comparable)
        assert_eq!(switch.find_master_for_cap(request, Some(generic_cap)), Some(0));

        // With preference for specific cap: routes to master 1 (specific, matches preference)
        assert_eq!(switch.find_master_for_cap(request, Some(specific_cap)), Some(1));
    }

    // TEST438: find_master_for_cap with preference falls back to closest-specificity
    //          when preferred cap is not in the comparable set
    #[test]
    fn test438_preferred_cap_falls_back_when_not_comparable() {
        let (engine_read0, slave_write0) = UnixStream::pair().unwrap();
        let (slave_read0, engine_write0) = UnixStream::pair().unwrap();

        // Master 0: only has a specific cap
        let registered = "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read0, slave_write0,
                &serde_json::json!(["cap:in=media:;out=media:", registered]),
                &Limits::default());
        });

        let switch = RelaySwitch::new(vec![(engine_read0, engine_write0)]).unwrap();

        let request = "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";

        // Preference for an unrelated cap — no equivalent match, falls back to closest-specificity
        let unrelated = "cap:in=\"media:txt;textable\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        assert_eq!(switch.find_master_for_cap(request, Some(unrelated)), Some(0));
    }

    // TEST439: Without preference, specific request does NOT match generic handler
    //          (confirms accepts semantics are unchanged)
    #[test]
    fn test439_no_preference_specific_rejects_generic() {
        let (engine_read0, slave_write0) = UnixStream::pair().unwrap();
        let (slave_read0, engine_write0) = UnixStream::pair().unwrap();

        // Master 0: only generic handler
        let generic_cap = "cap:in=media:bytes;op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read0, slave_write0,
                &serde_json::json!(["cap:in=media:;out=media:", generic_cap]),
                &Limits::default());
        });

        let switch = RelaySwitch::new(vec![(engine_read0, engine_write0)]).unwrap();

        // Specific PDF request — without preference, generic handler can't match
        // because request pattern requires pdf tag which generic doesn't have
        let request = "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\"";
        assert_eq!(switch.find_master_for_cap(request, None), None);

        // With preference for generic — now is_comparable finds it
        assert_eq!(switch.find_master_for_cap(request, Some(generic_cap)), Some(0));
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST487: RelaySwitch construction verifies identity through relay chain
    #[test]
    fn test487_relay_switch_identity_verification_succeeds() {
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            slave_notify_with_identity(slave_read, slave_write,
                &serde_json::json!(["cap:in=media:;out=media:", "cap:in=\"media:void\";op=test;out=\"media:void\""]),
                &Limits::default());
        });

        let switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Construction succeeded — caps are populated
        assert_eq!(switch.find_master_for_cap("cap:in=media:;out=media:", None), Some(0));
        assert_eq!(switch.find_master_for_cap("cap:in=\"media:void\";op=test;out=\"media:void\"", None), Some(0));
    }

    // TEST488: RelaySwitch construction fails when master's identity verification fails
    #[test]
    fn test488_relay_switch_identity_verification_fails() {
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write));

            // Send RelayNotify
            let caps = serde_json::json!(["cap:in=media:;out=media:"]);
            let notify = Frame::relay_notify(&serde_json::to_vec(&caps).unwrap(), &Limits::default());
            writer.write(&notify).unwrap();

            // Read identity REQ, respond with ERR
            let req = reader.read().unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(req.id, "BROKEN", "identity verification broken");
            writer.write(&err).unwrap();
        });

        let result = RelaySwitch::new(vec![(engine_read, engine_write)]);
        assert!(result.is_err(), "construction must fail when identity verification fails");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("identity verification failed"),
            "error must mention identity verification: {}", err);
    }

    // TEST488: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessPluginHost roundtrip
    #[test]
    fn test488_send_to_master_build_request_frames_roundtrip() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter, accumulate_input};
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;

        /// Echo handler: accumulates input, echoes raw bytes back.
        #[derive(Debug)]
        struct EchoHandler;
        impl FrameHandler for EchoHandler {
            fn handle_request(
                &self,
                _cap_urn: &str,
                input: std::sync::mpsc::Receiver<Frame>,
                output: ResponseWriter,
                _rt: &tokio::runtime::Handle,
            ) {
                match accumulate_input(&input) {
                    Ok(args) => {
                        let data: Vec<u8> = args.iter().flat_map(|a| a.value.clone()).collect();
                        output.emit_response("media:text;bytes", &data);
                    }
                    Err(e) => output.emit_error("ACCUMULATE_ERROR", &e),
                }
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let cap_urn_str = "cap:in=\"media:text;bytes\";op=echo;out=\"media:text;bytes\"";
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

        // Create socket pairs
        let (slave_write, switch_read) = UnixStream::pair().unwrap();
        let (switch_write, slave_read) = UnixStream::pair().unwrap();
        let (host_write, slave_local_read) = UnixStream::pair().unwrap();
        let (slave_local_write, host_read) = UnixStream::pair().unwrap();

        let host_handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_read, host_write, host_handle).unwrap();
        });

        let slave = RelaySlave::new(slave_local_read, slave_local_write);
        let slave_thread = std::thread::spawn(move || {
            let socket_reader = FrameReader::new(BufReader::new(slave_read));
            let socket_writer = FrameWriter::new(BufWriter::new(slave_write));
            slave.run(socket_reader, socket_writer, None).unwrap();
        });

        let mut switch = RelaySwitch::new(vec![(switch_read, switch_write)]).unwrap();

        // Verify the switch has our echo cap registered
        let caps_json: Vec<String> = serde_json::from_slice(switch.capabilities()).unwrap();
        assert!(caps_json.iter().any(|c| c.contains("echo")),
            "switch should have echo cap, got: {:?}", caps_json);

        // Build request frames using the helper
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().max_chunk;
        let frames = CapArgumentValue::build_request_frames(
            &rid,
            cap_urn_str,
            &[CapArgumentValue::new("media:text;bytes", b"hello streaming world".to_vec())],
            max_chunk,
        );

        // Send each frame via send_to_master (no preference)
        for frame in frames {
            switch.send_to_master(frame, None).unwrap();
        }

        // Read response frames via read_from_masters_timeout
        // Response chunks are CBOR-encoded (matching emit_response)
        let mut response_data = Vec::new();
        let mut got_end = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch.read_from_masters_timeout(std::time::Duration::from_millis(200)) {
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
        slave_thread.join().unwrap();
        host_thread.join().unwrap();
    }

    // TEST489: add_master dynamically connects new host to running switch
    #[test]
    fn test489_add_master_dynamic() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter};
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::caller::CapArgumentValue;
        use crate::cap::definition::Cap;

        /// Handler that returns a constant byte string (ignores input).
        #[derive(Debug)]
        struct ConstHandler(&'static str);
        impl FrameHandler for ConstHandler {
            fn handle_request(
                &self,
                _cap_urn: &str,
                input: std::sync::mpsc::Receiver<Frame>,
                output: ResponseWriter,
                _rt: &tokio::runtime::Handle,
            ) {
                // Drain input
                for frame in input.iter() {
                    if frame.frame_type == FrameType::End { break; }
                }
                output.emit_response("media:bytes", self.0.as_bytes());
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        // Helper to wire up host + slave and return switch-side sockets + thread handles
        fn wire_host(
            rt: &tokio::runtime::Runtime,
            host: InProcessPluginHost,
        ) -> (UnixStream, UnixStream, std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {
            let (slave_write, switch_read) = UnixStream::pair().unwrap();
            let (switch_write, slave_read) = UnixStream::pair().unwrap();
            let (host_write, slave_local_read) = UnixStream::pair().unwrap();
            let (slave_local_write, host_read) = UnixStream::pair().unwrap();

            let handle = rt.handle().clone();
            let host_thread = std::thread::spawn(move || {
                host.run(host_read, host_write, handle).unwrap();
            });

            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let slave_thread = std::thread::spawn(move || {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).unwrap();
            });

            (switch_read, switch_write, host_thread, slave_thread)
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

        let (sr_a, sw_a, ht_a, st_a) = wire_host(&rt, host_a);
        let mut switch = RelaySwitch::new(vec![(sr_a, sw_a)]).unwrap();
        assert_eq!(switch.masters.len(), 1);

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

        let (sr_b, sw_b, ht_b, st_b) = wire_host(&rt, host_b);
        let idx = switch.add_master(sr_b, sw_b).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(switch.masters.len(), 2);

        // Verify both caps are in aggregate capabilities
        let caps_json: Vec<String> = serde_json::from_slice(switch.capabilities()).unwrap();
        assert!(caps_json.iter().any(|c| c.contains("alpha")));
        assert!(caps_json.iter().any(|c| c.contains("beta")));

        // Execute against beta (dynamically added master) using send_to_master + build_request_frames
        let rid = MessageId::new_uuid();
        let max_chunk = switch.limits().max_chunk;
        let frames = CapArgumentValue::build_request_frames(&rid, cap_b, &[], max_chunk);
        for frame in frames {
            switch.send_to_master(frame, None).unwrap();
        }

        // Response chunks are CBOR-encoded
        let mut response_data = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match switch.read_from_masters_timeout(std::time::Duration::from_millis(200)) {
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
        st_a.join().unwrap();
        ht_a.join().unwrap();
        st_b.join().unwrap();
        ht_b.join().unwrap();
    }

    // TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match
    #[test]
    fn test666_preferred_cap_routing() {
        use crate::bifaci::in_process_host::{InProcessPluginHost, FrameHandler, ResponseWriter};
        use crate::bifaci::relay::RelaySlave;
        use crate::cap::definition::Cap;

        /// Handler that returns a marker string identifying itself
        #[derive(Debug)]
        struct MarkerHandler(&'static str);
        impl FrameHandler for MarkerHandler {
            fn handle_request(
                &self,
                _cap_urn: &str,
                input: std::sync::mpsc::Receiver<Frame>,
                output: ResponseWriter,
                _rt: &tokio::runtime::Handle,
            ) {
                // Drain input
                for frame in input.iter() {
                    if frame.frame_type == FrameType::End { break; }
                }
                output.emit_response("media:bytes", self.0.as_bytes());
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        // Helper to wire up host + slave
        fn wire_host(
            rt: &tokio::runtime::Runtime,
            host: InProcessPluginHost,
        ) -> (UnixStream, UnixStream, std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {
            let (slave_write, switch_read) = UnixStream::pair().unwrap();
            let (switch_write, slave_read) = UnixStream::pair().unwrap();
            let (host_write, slave_local_read) = UnixStream::pair().unwrap();
            let (slave_local_write, host_read) = UnixStream::pair().unwrap();

            let handle = rt.handle().clone();
            let host_thread = std::thread::spawn(move || {
                host.run(host_read, host_write, handle).unwrap();
            });

            let slave = RelaySlave::new(slave_local_read, slave_local_write);
            let slave_thread = std::thread::spawn(move || {
                let sr = FrameReader::new(BufReader::new(slave_read));
                let sw = FrameWriter::new(BufWriter::new(slave_write));
                slave.run(sr, sw, None).unwrap();
            });

            (switch_read, switch_write, host_thread, slave_thread)
        }

        // Master 1: Generic handler (accepts any input/output)
        let cap_generic = "cap:in=media:;out=media:";
        let host_generic = InProcessPluginHost::new(vec![(
            "generic".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_generic).unwrap(),
                title: "generic".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(MarkerHandler("GENERIC")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        // Master 2: Specific handler (exact match for specific cap)
        let cap_specific = "cap:in=\"media:void\";op=test;out=\"media:void\"";
        let host_specific = InProcessPluginHost::new(vec![(
            "specific".to_string(),
            vec![Cap {
                urn: crate::CapUrn::from_string(cap_specific).unwrap(),
                title: "specific".to_string(),
                cap_description: None,
                metadata: std::collections::HashMap::new(),
                command: String::new(),
                args: Vec::new(),
                output: None,
                media_specs: Vec::new(),
                metadata_json: None,
                registered_by: None,
            }],
            std::sync::Arc::new(MarkerHandler("SPECIFIC")) as std::sync::Arc<dyn FrameHandler>,
        )]);

        let (sr_generic, sw_generic, ht_generic, st_generic) = wire_host(&rt, host_generic);
        let (sr_specific, sw_specific, ht_specific, st_specific) = wire_host(&rt, host_specific);

        let mut switch = RelaySwitch::new(vec![(sr_generic, sw_generic), (sr_specific, sw_specific)]).unwrap();
        assert_eq!(switch.masters.len(), 2);

        // Test 1: Without preferred_cap, routes to generic (first match, lower specificity)
        let req_cap = "cap:in=\"media:void\";op=test;out=\"media:void\"";
        let mut req1 = Frame::req(MessageId::Uint(1), req_cap, None);
        req1.payload = Some(Vec::new());

        switch.send_to_master(req1.clone(), None).unwrap();
        switch.send_to_master(Frame::end(MessageId::Uint(1)), None).unwrap();

        let mut response_data1 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters() {
                Ok(Some(frame)) => {
                    match frame.frame_type {
                        FrameType::Chunk => response_data1.extend_from_slice(frame.payload.as_ref().unwrap()),
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Test 2: With preferred_cap = cap_specific, routes to specific (exact match)
        let mut req2 = Frame::req(MessageId::Uint(2), req_cap, None);
        req2.payload = Some(Vec::new());

        switch.send_to_master(req2.clone(), Some(cap_specific)).unwrap();
        switch.send_to_master(Frame::end(MessageId::Uint(2)), None).unwrap();

        let mut response_data2 = Vec::new();
        for _ in 0..10 {
            match switch.read_from_masters() {
                Ok(Some(frame)) => {
                    match frame.frame_type {
                        FrameType::Chunk => response_data2.extend_from_slice(frame.payload.as_ref().unwrap()),
                        FrameType::End => break,
                        FrameType::Err => panic!("ERR: {:?}", frame.error_message()),
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("read error: {}", e),
            }
        }

        // Verify routing: without preference routes to generic, with preference routes to specific
        assert_eq!(response_data1, b"GENERIC", "Without preferred_cap, should route to generic handler (first match)");
        assert_eq!(response_data2, b"SPECIFIC", "With preferred_cap, should route to specific handler (exact match)");

        drop(switch);
        st_generic.join().unwrap();
        ht_generic.join().unwrap();
        st_specific.join().unwrap();
        ht_specific.join().unwrap();
    }
}
