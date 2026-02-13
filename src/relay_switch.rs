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

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{CborError, FrameReader, FrameWriter};
use crate::plugin_relay::RelayMaster;
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
        if sockets.is_empty() {
            return Err(RelaySwitchError::Protocol(
                "RelaySwitch requires at least one master".to_string(),
            ));
        }

        let mut masters = Vec::new();
        let (frame_tx, frame_rx) = mpsc::channel();

        // Spawn reader threads for all masters
        // NO blocking handshake! Start with empty caps, update when RelayNotify arrives
        for (master_idx, (read_sock, write_sock)) in sockets.into_iter().enumerate() {
            let socket_reader = FrameReader::new(BufReader::new(read_sock));
            let socket_writer = FrameWriter::new(BufWriter::new(write_sock));

            // Spawn reader thread immediately - no handshake blocking
            let tx = frame_tx.clone();
            let reader_handle = std::thread::spawn(move || {
                let mut reader = socket_reader;
                loop {
                    match reader.read() {
                        Ok(Some(frame)) => {
                            if tx.send((master_idx, Ok(frame))).is_err() {
                                break; // Switch dropped, exit thread
                            }
                        }
                        Ok(None) => {
                            // EOF - send None indicator by dropping our end
                            eprintln!("[RelaySwitch/reader-{}] EOF from master", master_idx);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[RelaySwitch/reader-{}] Read error: {}", master_idx, e);
                            let _ = tx.send((master_idx, Err(e)));
                            break;
                        }
                    }
                }
            });

            // Start with empty manifest/caps - will be populated by RelayNotify
            masters.push(MasterConnection {
                socket_writer,
                manifest: Vec::new(),
                limits: Limits::default(),
                caps: Vec::new(),
                healthy: true,
                reader_handle: Some(reader_handle),
            });
        }

        // Drop the original tx so receiver knows when all threads are done
        drop(frame_tx);

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
            xid_counter: 0,
            rid_to_xid: HashMap::new(),
        };

        // Build initial routing tables
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

        // Find master that can handle this cap
        let dest_idx = self.find_master_for_cap(cap_urn).ok_or_else(|| {
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
        self.masters[dest_idx].socket_writer.write(&frame_with_xid)?;

        Ok(rx)
    }

    /// Send a frame to the appropriate master (engine → plugin direction).
    ///
    /// REQ frames: Assigned XID if absent, routed by cap URN.
    /// Continuation frames: Routed by (XID, RID) pair.
    pub fn send_to_master(&mut self, mut frame: Frame) -> Result<(), RelaySwitchError> {
        eprintln!("[RelaySwitch.send_to_master] Received {:?} (id={:?})", frame.frame_type, frame.id);
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;
                eprintln!("[RelaySwitch.send_to_master] REQ for cap: {}", cap_urn);

                // Find master that can handle this cap
                let dest_idx = self.find_master_for_cap(cap_urn).ok_or_else(|| {
                    eprintln!("[RelaySwitch.send_to_master] No handler found for cap: {}", cap_urn);
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;
                eprintln!("[RelaySwitch.send_to_master] Routing to master {}", dest_idx);

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
                self.masters[dest_idx].socket_writer.write(&frame)?;
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
                self.masters[dest_idx].socket_writer.write(&frame)?;

                // Cleanup on terminal frames
                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                if is_terminal {
                    // For engine-initiated requests, cleanup happens when plugin responds (in read_from_masters)
                    // This is the request END, not the response END
                }

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
                    eprintln!("Error reading from master {}: {}", master_idx, e);
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
                    eprintln!("Error reading from master {}: {}", master_idx, e);
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
    // INTERNAL ROUTING
    // =========================================================================

    /// Find which master handles a given cap URN.
    /// Chooses the MOST SPECIFIC matching cap URN (highest specificity score).
    /// If multiple matches have the same highest specificity, chooses one (deterministic).
    fn find_master_for_cap(&self, cap_urn: &str) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        // Collect ALL matching masters with their specificity scores
        let mut matches: Vec<(usize, usize)> = Vec::new(); // (master_idx, specificity)

        for (registered_cap, master_idx) in &self.cap_table {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                // Request is pattern, registered cap is instance
                if request_urn.accepts(&registered_urn) {
                    let specificity = registered_urn.specificity();
                    matches.push((*master_idx, specificity));
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

    /// Handle a frame arriving from a master (plugin → engine direction).
    ///
    /// Returns Some(frame) if the frame should be forwarded to the engine.
    /// Returns None if the frame was handled internally (peer request).
    fn handle_master_frame(
        &mut self,
        source_idx: usize,
        mut frame: Frame,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                eprintln!("[RelaySwitch.handle_master_frame] Peer REQ from master {} (id={:?}) for cap: {}",
                          source_idx, frame.id, cap_urn);

                // Find destination master
                let dest_idx = self.find_master_for_cap(cap_urn).ok_or_else(|| {
                    eprintln!("[RelaySwitch.handle_master_frame] No handler found for peer cap: {}", cap_urn);
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                eprintln!("[RelaySwitch.handle_master_frame] Routing peer REQ to master {}", dest_idx);

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

                eprintln!("[RelaySwitch.handle_master_frame] Assigned XID={:?} to peer REQ (RID={:?})", xid, frame.id);

                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());

                // Record RID → XID mapping for continuation frames
                self.rid_to_xid.insert(rid.clone(), xid.clone());
                eprintln!("[RelaySwitch.handle_master_frame] Recorded rid_to_xid mapping: {:?} -> {:?}", rid, xid);

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
                eprintln!("[RelaySwitch.handle_master_frame] Forwarding peer REQ to master {} (with XID={:?})", dest_idx, xid);
                self.masters[dest_idx].socket_writer.write(&frame)?;

                // Do NOT return to engine (internal routing)
                eprintln!("[RelaySwitch.handle_master_frame] Peer REQ routed internally (not forwarded to engine)");
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

                    eprintln!("[RelaySwitch.handle_master_frame] RESPONSE continuation {:?} from master {} (XID={:?}, payload len: {})",
                              frame.frame_type, source_idx, xid,
                              frame.payload.as_ref().map_or(0, |p| p.len()));

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
                                if tx.send(frame.clone()).is_err() {
                                    eprintln!("[RelaySwitch] External response channel closed, dropping frame");
                                }

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

                                eprintln!("[RelaySwitch.handle_master_frame] Returning {:?} to external caller (payload len: {})",
                                          frame.frame_type,
                                          frame.payload.as_ref().map_or(0, |p| p.len()));

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
                            // Route back to source master (keep XID - still in relay network)
                            eprintln!("[RelaySwitch.handle_master_frame] Routing response back to master {} (keeping XID for relay protocol)", master_idx);

                            self.masters[master_idx].socket_writer.write(&frame)?;

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

                    eprintln!("[RelaySwitch.handle_master_frame] REQUEST continuation {:?} from master {} (no XID, payload len: {})",
                              frame.frame_type, source_idx,
                              frame.payload.as_ref().map_or(0, |p| p.len()));

                    // Look up XID from RID → XID mapping (added by the REQ)
                    let xid = self.rid_to_xid.get(&rid).ok_or_else(|| {
                        eprintln!("[RelaySwitch.handle_master_frame] Unknown RID for request continuation: {:?}", rid);
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?.clone();

                    eprintln!("[RelaySwitch.handle_master_frame] Looked up XID={:?} for request continuation", xid);

                    let key = (xid.clone(), rid.clone());

                    // Look up routing entry
                    let entry = self.request_routing.get(&key).ok_or_else(|| {
                        RelaySwitchError::UnknownRequest(rid.clone())
                    })?;

                    // Add XID to frame for forwarding
                    frame.routing_id = Some(xid.clone());

                    // Forward to destination master (keep XID)
                    let dest_idx = entry.destination_master_idx;
                    eprintln!("[RelaySwitch.handle_master_frame] Forwarding request continuation to master {} (with XID={:?})", dest_idx, xid);

                    self.masters[dest_idx].socket_writer.write(&frame)?;
                    return Ok(None);
                }
            }

            FrameType::RelayNotify => {
                // Capability update from plugin - update our cap table
                eprintln!("[RelaySwitch] Received RelayNotify from master {}", source_idx);

                if let Some(caps_payload) = frame.relay_notify_manifest() {
                    eprintln!("[RelaySwitch] RelayNotify payload: {} bytes", caps_payload.len());
                    match parse_caps_from_relay_notify(caps_payload) {
                        Ok(new_caps) => {
                            eprintln!("[RelaySwitch] Parsed {} caps:", new_caps.len());
                            for (idx, cap) in new_caps.iter().enumerate() {
                                eprintln!("[RelaySwitch]   cap[{}]: {}", idx, cap);
                            }
                            // Update master's caps
                            if let Some(master) = self.masters.get_mut(source_idx) {
                                master.caps = new_caps;
                                master.manifest = caps_payload.to_vec();
                            }
                            // Rebuild cap_table from all masters
                            self.rebuild_cap_table();
                            eprintln!("[RelaySwitch] Cap table now has {} entries", self.cap_table.len());
                        }
                        Err(e) => {
                            eprintln!("[RelaySwitch] Failed to parse RelayNotify payload: {}", e);
                        }
                    }
                } else {
                    eprintln!("[RelaySwitch] RelayNotify has no payload!");
                }

                // Also pass through to engine (for visibility)
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

        eprintln!("Master {} died", master_idx);

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
                    } else {
                        eprintln!("Request {:?} failed: master died (no response channel)", rid);
                    }
                }
                Some(master_idx) => {
                    // Send ERR back to source master
                    if self.masters[master_idx].healthy {
                        let _ = self.masters[master_idx].socket_writer.write(&err_frame);
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

        // Build manifest JSON
        let manifest = serde_json::json!({
            "capabilities": all_caps
        });

        self.aggregate_capabilities = serde_json::to_vec(&manifest).unwrap_or_default();
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
                crate::cbor_frame::DEFAULT_MAX_FRAME
            } else {
                min_max_frame
            },
            max_chunk: if min_max_chunk == usize::MAX {
                crate::cbor_frame::DEFAULT_MAX_CHUNK
            } else {
                min_max_chunk
            },
        };
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Parse capability URNs from RelayNotify payload.
/// RelayNotify contains a simple JSON array of URN strings: ["cap:...", "cap:...", ...]
fn parse_caps_from_relay_notify(notify_payload: &[u8]) -> Result<Vec<String>, RelaySwitchError> {
    // Deserialize simple JSON array of URN strings
    let cap_urns: Vec<String> = serde_json::from_slice(notify_payload)
        .map_err(|e| RelaySwitchError::Protocol(format!("Invalid RelayNotify capability array: {}", e)))?;

    Ok(cap_urns)
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor_frame::Frame;
    use std::os::unix::net::UnixStream;

    // TEST429: Cap routing logic (find_master_for_cap)
    #[test]
    fn test429_find_master_for_cap() {
        use std::sync::mpsc;
        use std::time::Duration;

        // Create two masters with different caps
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();

        // Spawn slave 1 (echo cap)
        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read1));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write1));
            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=echo;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx1.send(()).unwrap();
        });

        // Spawn slave 2 (double cap)
        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read2));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write2));
            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=double;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx2.send(()).unwrap();
        });

        // Wait for both RelayNotify frames
        rx1.recv_timeout(Duration::from_secs(2)).unwrap();
        rx2.recv_timeout(Duration::from_secs(2)).unwrap();

        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ])
        .unwrap();

        // Verify routing
        assert_eq!(
            switch.find_master_for_cap("cap:in=\"media:void\";op=echo;out=\"media:void\""),
            Some(0)
        );
        assert_eq!(
            switch.find_master_for_cap("cap:in=\"media:void\";op=double;out=\"media:void\""),
            Some(1)
        );
        assert_eq!(
            switch.find_master_for_cap("cap:in=\"media:void\";op=unknown;out=\"media:void\""),
            None
        );

        // Verify aggregate capabilities
        let caps: serde_json::Value =
            serde_json::from_slice(switch.capabilities()).unwrap();
        let cap_list = caps["capabilities"].as_array().unwrap();
        assert_eq!(cap_list.len(), 2);
    }

    // TEST426: Single master REQ/response routing
    #[test]
    fn test426_single_master_req_response() {
        use std::sync::mpsc;
        use std::time::Duration;

        // Create socket pair for master
        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        // Use channel to synchronize test
        let (tx, rx) = mpsc::channel();

        // Spawn mock slave that sends RelayNotify then echoes one frame
        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write));

            // Send initial RelayNotify
            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=echo;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();

            // Signal that notify was sent
            tx.send(()).unwrap();

            // Read one REQ and send response
            match reader.read().unwrap() {
                Some(frame) => {
                    if frame.frame_type == FrameType::Req {
                        let response = Frame::end(frame.id.clone(), Some(vec![42]));
                        writer.write(&response).unwrap();
                    }
                }
                None => {}
            }
        });

        // Wait for RelayNotify
        rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Create RelaySwitch
        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Send REQ
        let req = Frame::req(
            MessageId::Uint(1),
            "cap:in=\"media:void\";op=echo;out=\"media:void\"",
            vec![1, 2, 3],
            "text/plain",
        );
        switch.send_to_master(req).unwrap();

        // Read response with timeout
        let response = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.id, MessageId::Uint(1));
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST427: Multi-master cap routing
    #[test]
    fn test427_multi_master_cap_routing() {
        // Create two masters with different caps
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        // Spawn mock slave 1 (echo cap)
        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read1));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write1));

            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=echo;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();

            loop {
                match reader.read().unwrap() {
                    Some(frame) => {
                        if frame.frame_type == FrameType::Req {
                            let response = Frame::end(frame.id.clone(), Some(vec![1]));
                            writer.write(&response).unwrap();
                        }
                    }
                    None => break,
                }
            }
        });

        // Spawn mock slave 2 (double cap)
        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read2));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write2));

            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=double;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();

            loop {
                match reader.read().unwrap() {
                    Some(frame) => {
                        if frame.frame_type == FrameType::Req {
                            let response = Frame::end(frame.id.clone(), Some(vec![2]));
                            writer.write(&response).unwrap();
                        }
                    }
                    None => break,
                }
            }
        });

        // Create RelaySwitch with both masters
        let mut switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ])
        .unwrap();

        // Send REQ for echo cap → routes to master 1
        let req1 = Frame::req(
            MessageId::Uint(1),
            "cap:in=\"media:void\";op=echo;out=\"media:void\"",
            vec![],
            "text/plain",
        );
        switch.send_to_master(req1).unwrap();

        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1]));

        // Send REQ for double cap → routes to master 2
        let req2 = Frame::req(
            MessageId::Uint(2),
            "cap:in=\"media:void\";op=double;out=\"media:void\"",
            vec![],
            "text/plain",
        );
        switch.send_to_master(req2).unwrap();

        let resp2 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![2]));
    }

    // TEST428: Unknown cap returns error
    #[test]
    fn test428_unknown_cap_returns_error() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write));

            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=echo;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx.send(()).unwrap();
            // Thread exits, slave will close
        });

        // Wait for RelayNotify
        rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Send REQ for unknown cap
        let req = Frame::req(
            MessageId::Uint(1),
            "cap:in=\"media:void\";op=unknown;out=\"media:void\"",
            vec![],
            "text/plain",
        );

        let result = switch.send_to_master(req);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }

    // TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent)
    #[test]
    fn test430_tie_breaking_same_cap_multiple_masters() {
        use std::sync::mpsc;
        use std::time::Duration;

        // Create two masters with the SAME cap
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();

        // Both slaves advertise the same cap
        let same_cap = "cap:in=\"media:void\";op=echo;out=\"media:void\"";

        // Spawn slave 1
        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read1));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write1));
            let manifest = serde_json::json!({
                "capabilities": [same_cap]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx1.send(()).unwrap();

            // Echo with marker 1
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let response = Frame::end(frame.id.clone(), Some(vec![1]));
                        writer.write(&response).unwrap();
                    }
                    None => break,
                    _ => {}
                }
            }
        });

        // Spawn slave 2
        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read2));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write2));
            let manifest = serde_json::json!({
                "capabilities": [same_cap]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx2.send(()).unwrap();

            // Echo with marker 2 (should never be called if routing is consistent)
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let response = Frame::end(frame.id.clone(), Some(vec![2]));
                        writer.write(&response).unwrap();
                    }
                    None => break,
                    _ => {}
                }
            }
        });

        rx1.recv_timeout(Duration::from_secs(2)).unwrap();
        rx2.recv_timeout(Duration::from_secs(2)).unwrap();

        let mut switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ])
        .unwrap();

        // Send first request - should go to master 0 (first match)
        let req1 = Frame::req(
            MessageId::Uint(1),
            same_cap,
            vec![],
            "text/plain",
        );
        switch.send_to_master(req1).unwrap();

        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![1])); // From master 0

        // Send second request - should ALSO go to master 0 (consistent routing)
        let req2 = Frame::req(
            MessageId::Uint(2),
            same_cap,
            vec![],
            "text/plain",
        );
        switch.send_to_master(req2).unwrap();

        let resp2 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp2.payload, Some(vec![1])); // Also from master 0
    }

    // TEST431: Continuation frame routing (CHUNK, END follow REQ)
    #[test]
    fn test431_continuation_frame_routing() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write));

            let manifest = serde_json::json!({
                "capabilities": ["cap:in=\"media:void\";op=test;out=\"media:void\""]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx.send(()).unwrap();

            // Read REQ
            let req = reader.read().unwrap().unwrap();
            assert_eq!(req.frame_type, FrameType::Req);

            // Read CHUNK continuation
            let chunk = reader.read().unwrap().unwrap();
            assert_eq!(chunk.frame_type, FrameType::Chunk);
            assert_eq!(chunk.id, req.id);

            // Read END continuation
            let end = reader.read().unwrap().unwrap();
            assert_eq!(end.frame_type, FrameType::End);
            assert_eq!(end.id, req.id);

            // Send response
            let response = Frame::end(req.id.clone(), Some(vec![42]));
            writer.write(&response).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        let req_id = MessageId::Uint(1);

        // Send REQ
        let req = Frame::req(
            req_id.clone(),
            "cap:in=\"media:void\";op=test;out=\"media:void\"",
            vec![],
            "text/plain",
        );
        switch.send_to_master(req).unwrap();

        // Send CHUNK continuation
        let payload = vec![1, 2, 3];
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(req_id.clone(), "stream1".to_string(), 0, payload, 0, checksum);
        switch.send_to_master(chunk).unwrap();

        // Send END continuation
        let end = Frame::end(req_id.clone(), None);
        switch.send_to_master(end).unwrap();

        // Read response
        let response = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload, Some(vec![42]));
    }

    // TEST432: Empty masters list returns error
    #[test]
    fn test432_empty_masters_list_error() {
        let result = RelaySwitch::new(vec![]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RelaySwitchError::Protocol(ref msg) if msg.contains("at least one master")
        ));
    }

    // TEST433: Capability aggregation deduplicates caps
    #[test]
    fn test433_capability_aggregation_deduplicates() {
        use std::sync::mpsc;
        use std::time::Duration;

        // Create two masters with overlapping caps
        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();

        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read1));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write1));
            let manifest = serde_json::json!({
                "capabilities": [
                    "cap:in=\"media:void\";op=echo;out=\"media:void\"",
                    "cap:in=\"media:void\";op=double;out=\"media:void\""
                ]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx1.send(()).unwrap();
        });

        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read2));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write2));
            let manifest = serde_json::json!({
                "capabilities": [
                    "cap:in=\"media:void\";op=echo;out=\"media:void\"",  // Duplicate
                    "cap:in=\"media:void\";op=triple;out=\"media:void\""
                ]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx2.send(()).unwrap();
        });

        rx1.recv_timeout(Duration::from_secs(2)).unwrap();
        rx2.recv_timeout(Duration::from_secs(2)).unwrap();

        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ])
        .unwrap();

        let caps: serde_json::Value =
            serde_json::from_slice(switch.capabilities()).unwrap();
        let mut cap_list: Vec<String> = caps["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        cap_list.sort();

        // Should have 3 unique caps (echo appears twice but deduplicated)
        assert_eq!(cap_list.len(), 3);
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=double;out=\"media:void\"".to_string()));
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=echo;out=\"media:void\"".to_string()));
        assert!(cap_list.contains(&"cap:in=\"media:void\";op=triple;out=\"media:void\"".to_string()));
    }

    // TEST434: Limits negotiation takes minimum
    #[test]
    fn test434_limits_negotiation_minimum() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (engine_read1, slave_write1) = UnixStream::pair().unwrap();
        let (slave_read1, engine_write1) = UnixStream::pair().unwrap();
        let (engine_read2, slave_write2) = UnixStream::pair().unwrap();
        let (slave_read2, engine_write2) = UnixStream::pair().unwrap();

        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();

        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read1));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write1));
            let manifest = serde_json::json!({"capabilities": []});
            let limits1 = Limits {
                max_frame: 1_000_000,
                max_chunk: 100_000,
            };
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &limits1,
            );
            writer.write(&notify).unwrap();
            tx1.send(()).unwrap();
        });

        std::thread::spawn(move || {
            let _reader = FrameReader::new(BufReader::new(slave_read2));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write2));
            let manifest = serde_json::json!({"capabilities": []});
            let limits2 = Limits {
                max_frame: 2_000_000,  // Larger
                max_chunk: 50_000,     // Smaller
            };
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &limits2,
            );
            writer.write(&notify).unwrap();
            tx2.send(()).unwrap();
        });

        rx1.recv_timeout(Duration::from_secs(2)).unwrap();
        rx2.recv_timeout(Duration::from_secs(2)).unwrap();

        let switch = RelaySwitch::new(vec![
            (engine_read1, engine_write1),
            (engine_read2, engine_write2),
        ])
        .unwrap();

        // Should take minimum of each limit
        assert_eq!(switch.limits().max_frame, 1_000_000); // min(1M, 2M)
        assert_eq!(switch.limits().max_chunk, 50_000);     // min(100K, 50K)
    }

    // TEST435: URN matching (exact vs accepts())
    #[test]
    fn test435_urn_matching_exact_and_accepts() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (engine_read, slave_write) = UnixStream::pair().unwrap();
        let (slave_read, engine_write) = UnixStream::pair().unwrap();

        let (tx, rx) = mpsc::channel();

        // Master advertises a specific cap
        let registered_cap = "cap:in=\"media:text;utf8\";op=process;out=\"media:text;utf8\"";

        std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_read));
            let mut writer = FrameWriter::new(BufWriter::new(slave_write));
            let manifest = serde_json::json!({
                "capabilities": [registered_cap]
            });
            let notify = Frame::relay_notify(
                &serde_json::to_vec(&manifest).unwrap(),
                &Limits::default(),
            );
            writer.write(&notify).unwrap();
            tx.send(()).unwrap();

            // Respond to request
            loop {
                match reader.read().unwrap() {
                    Some(frame) if frame.frame_type == FrameType::Req => {
                        let response = Frame::end(frame.id.clone(), Some(vec![42]));
                        writer.write(&response).unwrap();
                    }
                    None => break,
                    _ => {}
                }
            }
        });

        rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let mut switch = RelaySwitch::new(vec![(engine_read, engine_write)]).unwrap();

        // Exact match should work
        let req1 = Frame::req(
            MessageId::Uint(1),
            registered_cap,
            vec![],
            "text/plain",
        );
        switch.send_to_master(req1).unwrap();
        let resp1 = switch.read_from_masters().unwrap().unwrap();
        assert_eq!(resp1.payload, Some(vec![42]));

        // More specific request should NOT match less specific registered cap
        // (request is more specific, registered is less specific → no match)
        let req2 = Frame::req(
            MessageId::Uint(2),
            "cap:in=\"media:text;utf8;normalized\";op=process;out=\"media:text\"",
            vec![],
            "text/plain",
        );
        let result = switch.send_to_master(req2);
        // Request pattern (media:text;utf8;normalized) does NOT accept registered (media:text;utf8)
        // because request is asking for MORE specificity than what's registered
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RelaySwitchError::NoHandler(_)));
    }
}
