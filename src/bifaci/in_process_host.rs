//! In-Process Plugin Host — Direct dispatch to CapSet trait objects
//!
//! Sits where PluginHostRuntime sits (connected to RelaySlave via local socket pair),
//! but routes requests to `Arc<dyn CapSet>` trait objects instead of plugin binaries.
//!
//! ## Architecture
//!
//! ```text
//! RelaySlave ←→ InProcessPluginHost ←→ Provider A (direct call)
//!                                  ←→ Provider B (direct call)
//!                                  ←→ Provider C (direct call)
//! ```
//!
//! ## Key Differences from PluginHostRuntime
//!
//! - No HELLO handshake per provider (providers are in-process trait objects)
//! - No identity verification per provider (providers are trusted)
//! - No heartbeat monitoring (providers can't die independently)
//! - No process management (no spawn/kill)
//! - Provider execution via direct `CapSet::execute_cap()` calls
//! - Uses tokio runtime handle for async provider dispatch

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter};
use crate::cap::caller::{CapArgumentValue, CapSet};
use crate::cap::definition::Cap;
use crate::standard::caps::CAP_IDENTITY;
use crate::CapUrn;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::sync::mpsc;
use std::sync::Arc;

/// Entry for a registered in-process provider.
struct ProviderEntry {
    #[allow(dead_code)]
    name: String,
    caps: Vec<Cap>,
    cap_set: Arc<dyn CapSet>,
}

/// Cap table entry: (cap_urn_string, provider_index).
type CapTable = Vec<(String, usize)>;

/// State for a pending incoming request being accumulated.
struct PendingRequest {
    cap_urn: String,
    /// Index into providers vec, or usize::MAX for identity cap.
    provider_idx: usize,
    /// XID from the incoming request (must be propagated to all response frames).
    xid: Option<MessageId>,
    /// Accumulated argument streams: (stream_id, media_urn, data).
    streams: Vec<(String, String, Vec<u8>)>,
    /// Map from active stream_id to index in `streams`.
    active_streams: HashMap<String, usize>,
    /// Stream IDs that have received STREAM_END.
    ended_streams: HashSet<String>,
}

/// A plugin host that dispatches to in-process CapSet implementations.
///
/// Speaks the Frame protocol to a RelaySlave, but routes requests to
/// `Arc<dyn CapSet>` providers via direct Rust calls — no serialization
/// per provider, no socket pairs per provider.
pub struct InProcessPluginHost {
    providers: Vec<ProviderEntry>,
}

impl std::fmt::Debug for InProcessPluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessPluginHost")
            .field("provider_count", &self.providers.len())
            .finish()
    }
}

impl InProcessPluginHost {
    /// Create a new in-process plugin host with the given providers.
    ///
    /// Each provider is a tuple of (name, caps, cap_set).
    pub fn new(providers: Vec<(String, Vec<Cap>, Arc<dyn CapSet>)>) -> Self {
        let providers = providers
            .into_iter()
            .map(|(name, caps, cap_set)| ProviderEntry {
                name,
                caps,
                cap_set,
            })
            .collect();
        Self { providers }
    }

    /// Build the aggregate manifest as a JSON array of cap URN strings.
    /// Always includes CAP_IDENTITY as the first entry.
    fn build_manifest(&self) -> Vec<u8> {
        let mut cap_urns: Vec<String> = vec![CAP_IDENTITY.to_string()];
        for provider in &self.providers {
            for cap in &provider.caps {
                let urn = cap.urn.to_string();
                if urn != CAP_IDENTITY {
                    cap_urns.push(urn);
                }
            }
        }
        serde_json::to_vec(&cap_urns).unwrap_or_else(|_| b"[]".to_vec())
    }

    /// Build the cap table for routing: flat list of (cap_urn, provider_idx).
    fn build_cap_table(providers: &[ProviderEntry]) -> CapTable {
        let mut table = Vec::new();
        for (idx, provider) in providers.iter().enumerate() {
            for cap in &provider.caps {
                table.push((cap.urn.to_string(), idx));
            }
        }
        table
    }

    /// Find the best provider for a cap URN using closest-specificity matching.
    ///
    /// Mirrors `PluginHostRuntime::find_plugin_for_cap()` exactly:
    /// - Request is pattern, registered cap is instance
    /// - Closest specificity to request wins
    /// - Ties broken by first match (deterministic)
    fn find_provider_for_cap(cap_table: &CapTable, cap_urn: &str) -> Option<usize> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Collect ALL matching providers with their specificity scores
        let mut matches: Vec<(usize, usize)> = Vec::new(); // (provider_idx, specificity)

        for (registered_cap, provider_idx) in cap_table {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap) {
                // Request is pattern, registered cap is instance
                if request_urn.accepts(&registered_urn) {
                    let specificity = registered_urn.specificity();
                    matches.push((*provider_idx, specificity));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // Prefer the match with specificity closest to the request's specificity.
        // Ties broken by first match (deterministic).
        let min_distance = matches
            .iter()
            .map(|(_, s)| (*s as isize - request_specificity as isize).unsigned_abs())
            .min()
            .unwrap();

        matches
            .iter()
            .find(|(_, s)| {
                (*s as isize - request_specificity as isize).unsigned_abs() == min_distance
            })
            .map(|(idx, _)| *idx)
    }

    /// Run the host. Blocks until the local connection closes.
    ///
    /// `local_read` / `local_write` connect to the RelaySlave's local side.
    /// `rt` is a tokio runtime handle for spawning async provider calls.
    pub fn run<R: Read + Send + 'static, W: Write + Send + 'static>(
        self,
        local_read: R,
        local_write: W,
        rt: tokio::runtime::Handle,
    ) -> Result<(), CborError> {
        let mut reader = FrameReader::new(local_read);

        // Writer runs in a separate thread with SeqAssigner
        let (write_tx, write_rx) = mpsc::channel::<Frame>();
        let writer_thread = std::thread::spawn(move || {
            let mut writer = FrameWriter::new(local_write);
            let mut seq_assigner = SeqAssigner::new();

            while let Ok(mut frame) = write_rx.recv() {
                seq_assigner.assign(&mut frame);
                if let Err(e) = writer.write(&frame) {
                    eprintln!("[InProcessPluginHost] writer error: {}", e);
                    break;
                }
                // Clean up seq tracking on terminal frames
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
            }
        });

        // Send initial RelayNotify with aggregate caps
        let manifest = self.build_manifest();
        let notify = Frame::relay_notify(&manifest, &Limits::default());
        write_tx
            .send(notify)
            .map_err(|_| CborError::Protocol("writer channel closed on startup".into()))?;

        // Move providers to Arc for sharing with tokio tasks
        let providers = Arc::new(self.providers);
        let cap_table = Self::build_cap_table(&providers);

        // Request tracking
        let mut pending: HashMap<MessageId, PendingRequest> = HashMap::new();

        // Sentinel for identity cap routing
        const IDENTITY_SENTINEL: usize = usize::MAX;

        // Main read loop
        loop {
            let frame = match reader.read() {
                Ok(Some(f)) => f,
                Ok(None) => break, // EOF — RelaySlave closed
                Err(e) => {
                    eprintln!("[InProcessPluginHost] read error: {}", e);
                    break;
                }
            };

            match frame.frame_type {
                FrameType::Req => {
                    let rid = frame.id.clone();
                    let xid = frame.routing_id.clone();
                    let cap_urn = match &frame.cap {
                        Some(c) => c.clone(),
                        None => {
                            let mut err =
                                Frame::err(rid, "PROTOCOL_ERROR", "REQ missing cap URN");
                            err.routing_id = xid;
                            let _ = write_tx.send(err);
                            continue;
                        }
                    };

                    // Identity cap is "cap:" — sent by RelaySwitch for identity verification.
                    // Must be exact string match, NOT conforms_to (everything conforms to wildcard).
                    let is_identity = cap_urn == CAP_IDENTITY;

                    if is_identity {
                        pending.insert(
                            rid,
                            PendingRequest {
                                cap_urn,
                                provider_idx: IDENTITY_SENTINEL,
                                xid,
                                streams: Vec::new(),
                                active_streams: HashMap::new(),
                                ended_streams: HashSet::new(),
                            },
                        );
                    } else {
                        match Self::find_provider_for_cap(&cap_table, &cap_urn) {
                            Some(idx) => {
                                pending.insert(
                                    rid,
                                    PendingRequest {
                                        cap_urn,
                                        provider_idx: idx,
                                        xid,
                                        streams: Vec::new(),
                                        active_streams: HashMap::new(),
                                        ended_streams: HashSet::new(),
                                    },
                                );
                            }
                            None => {
                                let mut err = Frame::err(
                                    rid,
                                    "NO_HANDLER",
                                    &format!("no provider handles cap: {}", cap_urn),
                                );
                                err.routing_id = xid;
                                let _ = write_tx.send(err);
                            }
                        }
                    }
                }

                FrameType::StreamStart => {
                    let rid = frame.id.clone();
                    if let Some(req) = pending.get_mut(&rid) {
                        let stream_id = match &frame.stream_id {
                            Some(s) => s.clone(),
                            None => {
                                eprintln!(
                                    "[InProcessPluginHost] STREAM_START missing stream_id for {:?}",
                                    rid
                                );
                                continue;
                            }
                        };
                        let media_urn = frame.media_urn.clone().unwrap_or_default();
                        let idx = req.streams.len();
                        req.streams
                            .push((stream_id.clone(), media_urn, Vec::new()));
                        req.active_streams.insert(stream_id, idx);
                    }
                }

                FrameType::Chunk => {
                    let rid = frame.id.clone();
                    if let Some(req) = pending.get_mut(&rid) {
                        let stream_id = match &frame.stream_id {
                            Some(s) => s.clone(),
                            None => {
                                eprintln!(
                                    "[InProcessPluginHost] CHUNK missing stream_id for {:?}",
                                    rid
                                );
                                continue;
                            }
                        };
                        if let Some(&idx) = req.active_streams.get(&stream_id) {
                            if let Some(payload) = &frame.payload {
                                req.streams[idx].2.extend_from_slice(payload);
                            }
                        }
                    }
                }

                FrameType::StreamEnd => {
                    let rid = frame.id.clone();
                    if let Some(req) = pending.get_mut(&rid) {
                        if let Some(stream_id) = &frame.stream_id {
                            req.ended_streams.insert(stream_id.clone());
                        }
                    }
                }

                FrameType::End => {
                    let rid = frame.id.clone();
                    if let Some(req) = pending.remove(&rid) {
                        if req.provider_idx == IDENTITY_SENTINEL {
                            // Identity verification: echo all accumulated data back
                            Self::send_identity_response(&write_tx, rid, &req);
                        } else {
                            // Regular provider dispatch
                            Self::dispatch_to_provider(
                                &rt,
                                &write_tx,
                                &providers,
                                &cap_table,
                                rid,
                                req,
                            );
                        }
                    }
                }

                FrameType::Heartbeat => {
                    let response = Frame::heartbeat(frame.id.clone());
                    let _ = write_tx.send(response);
                }

                FrameType::Err => {
                    // Error from relay for a pending request — clean up
                    pending.remove(&frame.id);
                }

                _ => {
                    // RelayNotify, RelayState, etc. — not expected from relay side
                }
            }
        }

        drop(write_tx);
        let _ = writer_thread.join();
        Ok(())
    }

    /// Send identity response: echo all accumulated stream data back.
    fn send_identity_response(
        write_tx: &mpsc::Sender<Frame>,
        rid: MessageId,
        req: &PendingRequest,
    ) {
        let mut response_data = Vec::new();
        for (_, _, data) in &req.streams {
            response_data.extend_from_slice(data);
        }

        let xid = req.xid.clone();

        let stream_id = "identity".to_string();
        let mut start =
            Frame::stream_start(rid.clone(), stream_id.clone(), "media:bytes".to_string());
        start.routing_id = xid.clone();
        let _ = write_tx.send(start);

        let checksum = Frame::compute_checksum(&response_data);
        let mut chunk = Frame::chunk(rid.clone(), stream_id.clone(), 0, response_data, 0, checksum);
        chunk.routing_id = xid.clone();
        let _ = write_tx.send(chunk);

        let mut end_stream = Frame::stream_end(rid.clone(), stream_id, 1);
        end_stream.routing_id = xid.clone();
        let _ = write_tx.send(end_stream);

        let mut end = Frame::end(rid, None);
        end.routing_id = xid;
        let _ = write_tx.send(end);
    }

    /// Dispatch a completed request to the matching provider via tokio.
    fn dispatch_to_provider(
        rt: &tokio::runtime::Handle,
        write_tx: &mpsc::Sender<Frame>,
        providers: &Arc<Vec<ProviderEntry>>,
        cap_table: &CapTable,
        rid: MessageId,
        req: PendingRequest,
    ) {
        let provider = Arc::clone(&providers[req.provider_idx].cap_set);
        let cap_urn = req.cap_urn.clone();
        let xid = req.xid.clone();

        // Build CapArgumentValues from accumulated streams
        let arguments: Vec<CapArgumentValue> = req
            .streams
            .into_iter()
            .map(|(_, media_urn, data)| CapArgumentValue::new(media_urn, data))
            .collect();

        // Determine output media URN from the matched cap's out_spec
        let out_media = Self::find_matched_cap_out_spec(cap_table, providers, &cap_urn);

        let write_tx = write_tx.clone();
        let max_chunk = Limits::default().max_chunk;

        rt.spawn(async move {
            let result = provider.execute_cap(&cap_urn, &arguments).await;

            match result {
                Ok((binary, text)) => {
                    let response_data = if let Some(b) = binary {
                        b
                    } else if let Some(t) = text {
                        t.into_bytes()
                    } else {
                        Vec::new()
                    };

                    Self::send_response_frames(&write_tx, rid, xid.as_ref(), &out_media, &response_data, max_chunk);
                }
                Err(e) => {
                    let mut err = Frame::err(rid, "PROVIDER_ERROR", &e.to_string());
                    err.routing_id = xid;
                    let _ = write_tx.send(err);
                }
            }
        });
    }

    /// Find the out_spec of the cap that matched the request URN.
    fn find_matched_cap_out_spec(
        cap_table: &CapTable,
        providers: &[ProviderEntry],
        cap_urn: &str,
    ) -> String {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return "media:bytes".to_string(),
        };

        let request_specificity = request_urn.specificity();
        let mut best: Option<(&str, usize)> = None; // (out_spec, distance)

        for (registered_cap_str, provider_idx) in cap_table {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap_str) {
                if request_urn.accepts(&registered_urn) {
                    let distance = (registered_urn.specificity() as isize
                        - request_specificity as isize)
                        .unsigned_abs();
                    let should_replace = match &best {
                        None => true,
                        Some((_, best_d)) => distance < *best_d,
                    };
                    if should_replace {
                        // Find the Cap object for this entry to get out_spec
                        for cap in &providers[*provider_idx].caps {
                            if cap.urn.to_string() == *registered_cap_str {
                                best = Some((cap.urn.out_spec(), distance));
                                break;
                            }
                        }
                    }
                }
            }
        }

        best.map(|(s, _)| s.to_string())
            .unwrap_or_else(|| "media:bytes".to_string())
    }

    /// Send response frames: STREAM_START → CHUNK(s) → STREAM_END → END.
    /// All frames carry the XID from the original request for relay routing.
    fn send_response_frames(
        write_tx: &mpsc::Sender<Frame>,
        rid: MessageId,
        xid: Option<&MessageId>,
        media_urn: &str,
        data: &[u8],
        max_chunk: usize,
    ) {
        let stream_id = "result".to_string();

        let mut start = Frame::stream_start(rid.clone(), stream_id.clone(), media_urn.to_string());
        start.routing_id = xid.cloned();
        let _ = write_tx.send(start);

        // Chunk the response respecting max_chunk
        if data.is_empty() {
            let checksum = Frame::compute_checksum(&[]);
            let mut chunk = Frame::chunk(rid.clone(), stream_id.clone(), 0, Vec::new(), 0, checksum);
            chunk.routing_id = xid.cloned();
            let _ = write_tx.send(chunk);

            let mut end_stream = Frame::stream_end(rid.clone(), stream_id, 1);
            end_stream.routing_id = xid.cloned();
            let _ = write_tx.send(end_stream);
        } else {
            let chunks: Vec<&[u8]> = data.chunks(max_chunk).collect();
            let chunk_count = chunks.len() as u64;

            for (i, chunk_data) in chunks.iter().enumerate() {
                let checksum = Frame::compute_checksum(chunk_data);
                let mut chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    0, // seq assigned by writer thread's SeqAssigner
                    chunk_data.to_vec(),
                    i as u64,
                    checksum,
                );
                chunk.routing_id = xid.cloned();
                let _ = write_tx.send(chunk);
            }

            let mut end_stream = Frame::stream_end(rid.clone(), stream_id, chunk_count);
            end_stream.routing_id = xid.cloned();
            let _ = write_tx.send(end_stream);
        }

        let mut end = Frame::end(rid, None);
        end.routing_id = xid.cloned();
        let _ = write_tx.send(end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::io::{FrameReader, FrameWriter};
    use crate::cap::caller::CapSet;
    use crate::Cap;
    use std::future::Future;
    use std::os::unix::net::UnixStream;
    use std::pin::Pin;

    /// Simple echo provider for testing: returns binary input as binary output.
    #[derive(Debug)]
    struct EchoProvider;

    impl CapSet for EchoProvider {
        fn execute_cap(
            &self,
            _cap_urn: &str,
            arguments: &[CapArgumentValue],
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>
        {
            let data: Vec<u8> = arguments.iter().flat_map(|a| a.value.clone()).collect();
            Box::pin(async move { Ok((Some(data), None)) })
        }
    }

    fn make_test_cap(urn_str: &str) -> Cap {
        Cap {
            urn: CapUrn::from_string(urn_str).unwrap(),
            title: "test".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: String::new(),
            args: Vec::new(),
            output: None,
            media_specs: Vec::new(),
            metadata_json: None,
            registered_by: None,
        }
    }

    // TEST481: InProcessPluginHost routes REQ to matching provider and returns response
    #[test]
    fn test481_routes_req_to_provider() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cap_urn = "cap:in=\"media:text;bytes\";op=echo;out=\"media:text;bytes\"";
        let cap = make_test_cap(cap_urn);
        let providers = vec![(
            "echo".to_string(),
            vec![cap],
            Arc::new(EchoProvider) as Arc<dyn CapSet>,
        )];

        let host = InProcessPluginHost::new(providers);

        // Create socket pair for communication
        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        // Host reads from host_sock, writes to host_sock2
        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_sock, host_sock2, handle)
        });

        // Test side: reads from test_sock2, writes to test_sock
        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // First frame should be RelayNotify with manifest
        let notify = reader.read().unwrap().unwrap();
        assert_eq!(notify.frame_type, FrameType::RelayNotify);
        let manifest = notify.relay_notify_manifest().unwrap();
        let cap_urns: Vec<String> = serde_json::from_slice(manifest).unwrap();
        assert!(cap_urns.len() >= 2); // identity + echo cap
        assert_eq!(cap_urns[0], CAP_IDENTITY);

        // Send a REQ + STREAM_START + CHUNK + STREAM_END + END
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(1)); // XID
        writer.write(&req).unwrap();

        let ss = Frame::stream_start(rid.clone(), "arg0".to_string(), "media:text;bytes".to_string());
        writer.write(&ss).unwrap();

        let payload = b"hello world";
        let checksum = Frame::compute_checksum(payload);
        let chunk = Frame::chunk(rid.clone(), "arg0".to_string(), 0, payload.to_vec(), 0, checksum);
        writer.write(&chunk).unwrap();

        let se = Frame::stream_end(rid.clone(), "arg0".to_string(), 1);
        writer.write(&se).unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).unwrap();

        // Read response: STREAM_START + CHUNK + STREAM_END + END
        let resp_ss = reader.read().unwrap().unwrap();
        assert_eq!(resp_ss.frame_type, FrameType::StreamStart);
        assert_eq!(resp_ss.id, rid);
        assert_eq!(resp_ss.stream_id.as_deref(), Some("result"));

        let resp_chunk = reader.read().unwrap().unwrap();
        assert_eq!(resp_chunk.frame_type, FrameType::Chunk);
        assert_eq!(resp_chunk.payload.as_deref(), Some(payload.as_slice()));

        let resp_se = reader.read().unwrap().unwrap();
        assert_eq!(resp_se.frame_type, FrameType::StreamEnd);

        let resp_end = reader.read().unwrap().unwrap();
        assert_eq!(resp_end.frame_type, FrameType::End);

        // Close and wait
        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST482: InProcessPluginHost handles identity verification (echo nonce)
    #[test]
    fn test482_identity_verification() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_sock, host_sock2, handle)
        });

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // Skip RelayNotify
        let _notify = reader.read().unwrap().unwrap();

        // Send identity verification
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), CAP_IDENTITY, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(0));
        writer.write(&req).unwrap();

        // Send nonce via stream
        let nonce = crate::bifaci::io::identity_nonce();
        let ss = Frame::stream_start(rid.clone(), "identity-verify".to_string(), "media:bytes".to_string());
        writer.write(&ss).unwrap();

        let checksum = Frame::compute_checksum(&nonce);
        let chunk = Frame::chunk(rid.clone(), "identity-verify".to_string(), 0, nonce.clone(), 0, checksum);
        writer.write(&chunk).unwrap();

        let se = Frame::stream_end(rid.clone(), "identity-verify".to_string(), 1);
        writer.write(&se).unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).unwrap();

        // Read echoed response
        let resp_ss = reader.read().unwrap().unwrap();
        assert_eq!(resp_ss.frame_type, FrameType::StreamStart);

        let resp_chunk = reader.read().unwrap().unwrap();
        assert_eq!(resp_chunk.frame_type, FrameType::Chunk);
        assert_eq!(resp_chunk.payload.as_deref(), Some(nonce.as_slice()));

        let resp_se = reader.read().unwrap().unwrap();
        assert_eq!(resp_se.frame_type, FrameType::StreamEnd);

        let resp_end = reader.read().unwrap().unwrap();
        assert_eq!(resp_end.frame_type, FrameType::End);

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST483: InProcessPluginHost returns NO_HANDLER for unregistered cap
    #[test]
    fn test483_no_handler_returns_err() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_sock, host_sock2, handle)
        });

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // Skip RelayNotify
        let _notify = reader.read().unwrap().unwrap();

        let rid = MessageId::new_uuid();
        let mut req = Frame::req(
            rid.clone(),
            "cap:in=\"media:pdf;bytes\";op=unknown;out=\"media:text;bytes\"",
            vec![],
            "application/cbor",
        );
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).unwrap();

        // Should get ERR back
        let err_frame = reader.read().unwrap().unwrap();
        assert_eq!(err_frame.frame_type, FrameType::Err);
        assert_eq!(err_frame.id, rid);
        assert_eq!(err_frame.error_code(), Some("NO_HANDLER"));

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST484: InProcessPluginHost manifest includes identity cap and provider caps
    #[test]
    fn test484_manifest_includes_all_caps() {
        let cap_urn = "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessPluginHost::new(vec![(
            "thumb".to_string(),
            vec![cap],
            Arc::new(EchoProvider) as Arc<dyn CapSet>,
        )]);

        let manifest = host.build_manifest();
        let cap_urns: Vec<String> = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(cap_urns[0], CAP_IDENTITY);
        assert!(cap_urns.iter().any(|u| u.contains("thumbnail")));
    }

    // TEST485: InProcessPluginHost handles heartbeat by echoing same ID
    #[test]
    fn test485_heartbeat_response() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_sock, host_sock2, handle)
        });

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // Skip RelayNotify
        let _notify = reader.read().unwrap().unwrap();

        let hb_id = MessageId::new_uuid();
        let hb = Frame::heartbeat(hb_id.clone());
        writer.write(&hb).unwrap();

        let resp = reader.read().unwrap().unwrap();
        assert_eq!(resp.frame_type, FrameType::Heartbeat);
        assert_eq!(resp.id, hb_id);

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST486: InProcessPluginHost provider error returns ERR frame
    #[test]
    fn test486_provider_error_returns_err_frame() {
        /// Provider that always fails.
        #[derive(Debug)]
        struct FailProvider;

        impl CapSet for FailProvider {
            fn execute_cap(
                &self,
                _cap_urn: &str,
                _arguments: &[CapArgumentValue],
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>
            {
                Box::pin(async move { Err(anyhow::anyhow!("provider crashed")) })
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let cap_urn = "cap:in=\"media:void\";op=fail;out=\"media:void\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessPluginHost::new(vec![(
            "fail".to_string(),
            vec![cap],
            Arc::new(FailProvider) as Arc<dyn CapSet>,
        )]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || {
            host.run(host_sock, host_sock2, handle)
        });

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // Skip RelayNotify
        let _notify = reader.read().unwrap().unwrap();

        // Send REQ + END (no streams, void input)
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).unwrap();

        // Should get ERR frame
        let err_frame = reader.read().unwrap().unwrap();
        assert_eq!(err_frame.frame_type, FrameType::Err);
        assert_eq!(err_frame.id, rid);
        assert_eq!(err_frame.error_code(), Some("PROVIDER_ERROR"));
        assert!(err_frame.error_message().unwrap().contains("provider crashed"));

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST487: InProcessPluginHost closest-specificity routing prefers specific over identity
    #[test]
    fn test487_closest_specificity_routing() {
        let specific_urn = "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";
        let generic_urn = "cap:in=\"media:image;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";

        let specific_cap = make_test_cap(specific_urn);
        let generic_cap = make_test_cap(generic_urn);

        /// Provider that tags its output with its name.
        #[derive(Debug)]
        struct TaggedProvider(String);

        impl CapSet for TaggedProvider {
            fn execute_cap(
                &self,
                _cap_urn: &str,
                _arguments: &[CapArgumentValue],
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>
            {
                let tag = self.0.clone();
                Box::pin(async move { Ok((Some(tag.into_bytes()), None)) })
            }
        }

        let providers = vec![
            (
                "generic".to_string(),
                vec![generic_cap],
                Arc::new(TaggedProvider("generic".into())) as Arc<dyn CapSet>,
            ),
            (
                "specific".to_string(),
                vec![specific_cap],
                Arc::new(TaggedProvider("specific".into())) as Arc<dyn CapSet>,
            ),
        ];

        let host = InProcessPluginHost::new(providers);
        let providers_arc = Arc::new(host.providers);
        let cap_table = InProcessPluginHost::build_cap_table(&providers_arc);

        // Request for pdf thumbnail should match specific (pdf, specificity 3) over generic (image, specificity 2)
        let result = InProcessPluginHost::find_provider_for_cap(
            &cap_table,
            "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"",
        );
        assert_eq!(result, Some(1)); // specific provider
    }
}
