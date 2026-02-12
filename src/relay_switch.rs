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
    /// Source master index (usize::MAX = engine)
    source_master_idx: usize,
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
    /// Routing: req_id → source/destination masters
    request_routing: HashMap<MessageId, RoutingEntry>,
    /// Peer-initiated request IDs (special cleanup semantics)
    peer_requests: HashSet<MessageId>,
    /// Aggregate capabilities (union of all masters)
    aggregate_capabilities: Vec<u8>,
    /// Negotiated limits (minimum across all masters)
    negotiated_limits: Limits,
    /// Channel receiver for frames from master reader threads
    frame_rx: mpsc::Receiver<(usize, Result<Frame, CborError>)>,
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

/// Sentinel value for source_master_idx indicating engine-initiated request
const ENGINE_SOURCE: usize = usize::MAX;

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

        // Connect to all masters and spawn reader threads
        for (master_idx, (read_sock, write_sock)) in sockets.into_iter().enumerate() {
            let mut socket_reader = FrameReader::new(BufReader::new(read_sock));

            // Perform RelayMaster handshake (reads initial RelayNotify) - blocking is OK here
            let mut master = RelayMaster::connect(&mut socket_reader)?;

            let manifest = master.manifest().to_vec();
            let limits = *master.limits();
            let caps = parse_caps_from_manifest(&manifest)?;

            // Create socket writer after handshake
            let socket_writer = FrameWriter::new(BufWriter::new(write_sock));

            // Spawn reader thread for this master
            let tx = frame_tx.clone();
            let reader_handle = std::thread::spawn(move || {
                loop {
                    match master.read_frame(&mut socket_reader) {
                        Ok(Some(frame)) => {
                            if tx.send((master_idx, Ok(frame))).is_err() {
                                break; // Switch dropped, exit thread
                            }
                        }
                        Ok(None) => {
                            // EOF - send None indicator by dropping our end
                            break;
                        }
                        Err(e) => {
                            let _ = tx.send((master_idx, Err(e)));
                            break;
                        }
                    }
                }
            });

            masters.push(MasterConnection {
                socket_writer,
                manifest: manifest.clone(),
                limits,
                caps,
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
            aggregate_capabilities: Vec::new(),
            negotiated_limits: Limits::default(),
            frame_rx,
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

    /// Send a frame to the appropriate master (engine → plugin direction).
    ///
    /// REQ frames are routed by cap URN. Continuation frames are routed by req_id.
    pub fn send_to_master(&mut self, frame: Frame) -> Result<(), RelaySwitchError> {
        match frame.frame_type {
            FrameType::Req => {
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Find master that can handle this cap
                let dest_idx = self.find_master_for_cap(cap_urn).ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Register routing (source = engine)
                self.request_routing.insert(
                    frame.id.clone(),
                    RoutingEntry {
                        source_master_idx: ENGINE_SOURCE,
                        destination_master_idx: dest_idx,
                    },
                );

                // Forward to destination
                self.masters[dest_idx].socket_writer.write(&frame)?;
                Ok(())
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err => {
                // Route by request ID
                let entry = self.request_routing.get(&frame.id).ok_or_else(|| {
                    RelaySwitchError::UnknownRequest(frame.id.clone())
                })?;

                let dest_idx = entry.destination_master_idx;

                // Forward to destination
                self.masters[dest_idx].socket_writer.write(&frame)?;

                // Cleanup routing on terminal frames for peer responses
                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                if is_terminal && self.peer_requests.contains(&frame.id) {
                    // Peer-initiated: engine's response END = final
                    self.request_routing.remove(&frame.id);
                    self.peer_requests.remove(&frame.id);
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

    // =========================================================================
    // INTERNAL ROUTING
    // =========================================================================

    /// Find which master handles a given cap URN.
    fn find_master_for_cap(&self, cap_urn: &str) -> Option<usize> {
        // Try exact match first
        for (registered_cap, master_idx) in &self.cap_table {
            if registered_cap == cap_urn {
                return Some(*master_idx);
            }
        }

        // Try URN-level matching: request is pattern, registered cap is instance
        if let Ok(request_urn) = crate::CapUrn::from_string(cap_urn) {
            for (registered_cap, master_idx) in &self.cap_table {
                if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                    if request_urn.accepts(&registered_urn) {
                        return Some(*master_idx);
                    }
                }
            }
        }

        None
    }

    /// Handle a frame arriving from a master (plugin → engine direction).
    ///
    /// Returns Some(frame) if the frame should be forwarded to the engine.
    /// Returns None if the frame was handled internally (peer request).
    fn handle_master_frame(
        &mut self,
        source_idx: usize,
        frame: Frame,
    ) -> Result<Option<Frame>, RelaySwitchError> {
        match frame.frame_type {
            FrameType::Req => {
                // Peer request: plugin → plugin via switch
                let cap_urn = frame.cap.as_ref().ok_or_else(|| {
                    RelaySwitchError::Protocol("REQ frame missing cap URN".to_string())
                })?;

                // Find destination master
                let dest_idx = self.find_master_for_cap(cap_urn).ok_or_else(|| {
                    RelaySwitchError::NoHandler(cap_urn.clone())
                })?;

                // Register routing (source = plugin's master)
                self.request_routing.insert(
                    frame.id.clone(),
                    RoutingEntry {
                        source_master_idx: source_idx,
                        destination_master_idx: dest_idx,
                    },
                );

                // Mark as peer request
                self.peer_requests.insert(frame.id.clone());

                // Forward to destination
                self.masters[dest_idx].socket_writer.write(&frame)?;

                // Do NOT return to engine (internal routing)
                Ok(None)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err
            | FrameType::Log => {
                // Check if this is a response to a peer request
                if let Some(entry) = self.request_routing.get(&frame.id) {
                    if entry.source_master_idx != ENGINE_SOURCE {
                        // Response to peer request: route back to source master
                        let dest_idx = entry.source_master_idx;

                        let is_terminal = frame.frame_type == FrameType::End
                            || frame.frame_type == FrameType::Err;

                        // Forward to source master
                        self.masters[dest_idx].socket_writer.write(&frame)?;

                        // Cleanup routing on plugin's terminal frame
                        if is_terminal && !self.peer_requests.contains(&frame.id) {
                            // Plugin's END for engine-initiated request
                            self.request_routing.remove(&frame.id);
                        }

                        // Do NOT return to engine (internal routing)
                        return Ok(None);
                    }
                }

                // Response to engine request: forward to engine
                let is_terminal = frame.frame_type == FrameType::End
                    || frame.frame_type == FrameType::Err;

                if is_terminal && !self.peer_requests.contains(&frame.id) {
                    // Engine-initiated request: plugin's END = final response
                    self.request_routing.remove(&frame.id);
                }

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
        for (req_id, entry) in &self.request_routing {
            if entry.destination_master_idx == master_idx {
                dead_requests.push((req_id.clone(), entry.source_master_idx));
            }
        }

        // Send ERR for each pending request
        for (req_id, source_idx) in dead_requests {
            let err_frame = Frame::err(
                req_id.clone(),
                "MASTER_DIED",
                "Relay master connection closed",
            );

            if source_idx == ENGINE_SOURCE {
                // Can't send back to engine in this sync API — caller must handle
                // For now, just log it
                eprintln!("Request {:?} failed: master died", req_id);
            } else {
                // Send ERR back to source master
                if self.masters[source_idx].healthy {
                    let _ = self.masters[source_idx].socket_writer.write(&err_frame);
                }
            }

            // Cleanup routing
            self.request_routing.remove(&req_id);
            self.peer_requests.remove(&req_id);
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

/// Parse capability URNs from a manifest JSON.
fn parse_caps_from_manifest(manifest: &[u8]) -> Result<Vec<String>, RelaySwitchError> {
    let parsed: serde_json::Value = serde_json::from_slice(manifest)
        .map_err(|e| RelaySwitchError::Protocol(format!("Invalid manifest JSON: {}", e)))?;

    let caps = parsed
        .get("capabilities")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            RelaySwitchError::Protocol("Manifest missing capabilities array".to_string())
        })?;

    caps.iter()
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| RelaySwitchError::Protocol("Non-string capability".to_string()))
        })
        .collect()
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
        let chunk = Frame::chunk(req_id.clone(), "stream1".to_string(), 0, vec![1, 2, 3]);
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
