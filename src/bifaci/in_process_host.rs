//! In-Process Plugin Host — Direct dispatch to FrameHandler trait objects
//!
//! Sits where PluginHostRuntime sits (connected to RelaySlave via local socket pair),
//! but routes requests to `Arc<dyn FrameHandler>` trait objects instead of plugin binaries.
//!
//! ## Architecture
//!
//! ```text
//! RelaySlave ←→ InProcessPluginHost ←→ Handler A (streaming frames)
//!                                   ←→ Handler B (streaming frames)
//!                                   ←→ Handler C (streaming frames)
//! ```
//!
//! ## Design
//!
//! The host does NOT accumulate data. On REQ, it spawns a handler thread with
//! channels for frame I/O. All continuation frames (STREAM_START, CHUNK, STREAM_END,
//! END) are forwarded to the handler. The handler processes frames natively —
//! streaming or accumulating as it sees fit.
//!
//! This matches how real plugins work: PluginRuntime forwards frames to handlers,
//! and each handler decides how to consume/produce data.

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{CborError, FrameReader, FrameWriter};
use crate::cap::caller::CapArgumentValue;
use crate::cap::definition::Cap;
use crate::standard::caps::CAP_IDENTITY;
use crate::CapUrn;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::mpsc;
use std::sync::Arc;

// =============================================================================
// FRAME HANDLER TRAIT
// =============================================================================

/// Handler for streaming frame-based requests.
///
/// Handlers receive input frames (STREAM_START, CHUNK, STREAM_END, END) via a
/// channel and send response frames via a ResponseWriter. The host never
/// accumulates — handlers decide how to process input (stream or accumulate).
///
/// For handlers that don't need streaming, use `accumulate_input()` to collect
/// all input streams into `Vec<CapArgumentValue>`.
pub trait FrameHandler: Send + Sync + std::fmt::Debug {
    /// Handle a streaming request.
    ///
    /// Called in a dedicated thread for each incoming request. The handler reads
    /// input frames from `input` and sends response frames via `output`.
    ///
    /// The REQ frame has already been consumed by the host. `input` receives:
    /// STREAM_START, CHUNK, STREAM_END (per argument stream), then END.
    ///
    /// The handler MUST send a complete response: either response frames
    /// (STREAM_START + CHUNK(s) + STREAM_END + END) or an error (via `output.emit_error()`).
    fn handle_request(
        &self,
        cap_urn: &str,
        input: mpsc::Receiver<Frame>,
        output: ResponseWriter,
        rt: &tokio::runtime::Handle,
    );
}

// =============================================================================
// RESPONSE WRITER
// =============================================================================

/// Wraps an output channel with automatic request_id and routing_id stamping.
///
/// All frames sent via ResponseWriter get the correct request_id and routing_id
/// for relay routing. Seq is left at 0 — the wire writer's SeqAssigner handles it.
pub struct ResponseWriter {
    request_id: MessageId,
    routing_id: Option<MessageId>,
    tx: mpsc::Sender<Frame>,
    max_chunk: usize,
}

impl ResponseWriter {
    fn new(
        request_id: MessageId,
        routing_id: Option<MessageId>,
        tx: mpsc::Sender<Frame>,
        max_chunk: usize,
    ) -> Self {
        Self { request_id, routing_id, tx, max_chunk }
    }

    /// Send a frame, stamping it with the request_id and routing_id.
    pub fn send(&self, mut frame: Frame) {
        frame.id = self.request_id.clone();
        frame.routing_id = self.routing_id.clone();
        frame.seq = 0; // SeqAssigner handles this
        let _ = self.tx.send(frame);
    }

    /// Max chunk size for this connection.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Send a complete data response: STREAM_START + CBOR-encoded CHUNK(s) + STREAM_END + END.
    pub fn emit_response(&self, media_urn: &str, data: &[u8]) {
        let stream_id = "result".to_string();

        self.send(Frame::stream_start(
            MessageId::Uint(0),
            stream_id.clone(),
            media_urn.to_string(),
        ));

        if data.is_empty() {
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&ciborium::Value::Bytes(Vec::new()), &mut cbor_payload)
                .expect("BUG: CBOR encode empty bytes");
            let checksum = Frame::compute_checksum(&cbor_payload);
            self.send(Frame::chunk(
                MessageId::Uint(0),
                stream_id.clone(),
                0,
                cbor_payload,
                0,
                checksum,
            ));
            self.send(Frame::stream_end(MessageId::Uint(0), stream_id, 1));
        } else {
            let chunks: Vec<&[u8]> = data.chunks(self.max_chunk).collect();
            let chunk_count = chunks.len() as u64;
            for (i, chunk_data) in chunks.iter().enumerate() {
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(
                    &ciborium::Value::Bytes(chunk_data.to_vec()),
                    &mut cbor_payload,
                )
                .expect("BUG: CBOR encode chunk bytes");
                let checksum = Frame::compute_checksum(&cbor_payload);
                self.send(Frame::chunk(
                    MessageId::Uint(0),
                    stream_id.clone(),
                    0,
                    cbor_payload,
                    i as u64,
                    checksum,
                ));
            }
            self.send(Frame::stream_end(MessageId::Uint(0), stream_id, chunk_count));
        }

        self.send(Frame::end(MessageId::Uint(0), None));
    }

    /// Send an error response.
    pub fn emit_error(&self, code: &str, message: &str) {
        self.send(Frame::err(MessageId::Uint(0), code, message));
    }
}

// =============================================================================
// INPUT ACCUMULATION UTILITY
// =============================================================================

/// Accumulate all input streams from a frame channel into CapArgumentValues.
///
/// Reads frames until END. CBOR-decodes chunk payloads to extract raw bytes.
/// For handlers that don't need streaming — they accumulate all input, process,
/// then emit a response.
///
/// Returns Err on CBOR decode failure (protocol violation).
pub fn accumulate_input(
    input: &mpsc::Receiver<Frame>,
) -> Result<Vec<CapArgumentValue>, String> {
    let mut streams: Vec<(String, String, Vec<u8>)> = Vec::new(); // (stream_id, media_urn, data)
    let mut active: HashMap<String, usize> = HashMap::new();

    for frame in input.iter() {
        match frame.frame_type {
            FrameType::StreamStart => {
                let sid = frame.stream_id.clone().unwrap_or_default();
                let media_urn = frame.media_urn.clone().unwrap_or_default();
                let idx = streams.len();
                streams.push((sid.clone(), media_urn, Vec::new()));
                active.insert(sid, idx);
            }
            FrameType::Chunk => {
                let sid = frame.stream_id.clone().unwrap_or_default();
                if let Some(&idx) = active.get(&sid) {
                    if let Some(payload) = &frame.payload {
                        // CBOR-decode chunk payload to extract raw bytes
                        let value: ciborium::Value = ciborium::from_reader(&payload[..])
                            .map_err(|e| format!(
                                "chunk payload is not valid CBOR (stream={}, {} bytes): {}",
                                sid, payload.len(), e
                            ))?;
                        match value {
                            ciborium::Value::Bytes(b) => streams[idx].2.extend_from_slice(&b),
                            ciborium::Value::Text(s) => streams[idx].2.extend_from_slice(s.as_bytes()),
                            other => {
                                return Err(format!(
                                    "unexpected CBOR type in chunk payload: {:?}",
                                    other
                                ));
                            }
                        }
                    }
                }
            }
            FrameType::StreamEnd => {} // nothing to do
            FrameType::End => break,
            _ => {} // ignore unexpected frame types
        }
    }

    Ok(streams
        .into_iter()
        .map(|(_, media_urn, data)| CapArgumentValue::new(media_urn, data))
        .collect())
}

// =============================================================================
// BUILT-IN IDENTITY HANDLER
// =============================================================================

/// Identity handler: raw byte passthrough (no CBOR decode/encode).
///
/// Echoes all accumulated chunk payloads back as-is. This is the protocol-level
/// identity verification — it proves the transport works end-to-end.
#[derive(Debug)]
struct IdentityHandler;

impl FrameHandler for IdentityHandler {
    fn handle_request(
        &self,
        _cap_urn: &str,
        input: mpsc::Receiver<Frame>,
        output: ResponseWriter,
        _rt: &tokio::runtime::Handle,
    ) {
        // Accumulate raw payload bytes (no CBOR decode — identity is raw passthrough)
        let mut data = Vec::new();
        for frame in input.iter() {
            match frame.frame_type {
                FrameType::Chunk => {
                    if let Some(p) = &frame.payload {
                        data.extend_from_slice(p);
                    }
                }
                FrameType::End => break,
                _ => {} // STREAM_START, STREAM_END — skip
            }
        }

        // Echo back as a single stream (raw bytes, no CBOR encode)
        let stream_id = "identity".to_string();
        output.send(Frame::stream_start(
            MessageId::Uint(0),
            stream_id.clone(),
            "media:bytes".to_string(),
        ));

        let checksum = Frame::compute_checksum(&data);
        output.send(Frame::chunk(
            MessageId::Uint(0),
            stream_id.clone(),
            0,
            data,
            0,
            checksum,
        ));

        output.send(Frame::stream_end(MessageId::Uint(0), stream_id, 1));
        output.send(Frame::end(MessageId::Uint(0), None));
    }
}

// =============================================================================
// IN-PROCESS PLUGIN HOST
// =============================================================================

/// Entry for a registered in-process handler.
struct HandlerEntry {
    #[allow(dead_code)]
    name: String,
    caps: Vec<Cap>,
    handler: Arc<dyn FrameHandler>,
}

/// Cap table entry: (cap_urn_string, handler_index).
type CapTable = Vec<(String, usize)>;

/// A plugin host that dispatches to in-process FrameHandler implementations.
///
/// Speaks the Frame protocol to a RelaySlave, but routes requests to
/// `Arc<dyn FrameHandler>` trait objects via frame channels — no accumulation
/// at the host level, handlers own the streaming.
pub struct InProcessPluginHost {
    handlers: Vec<HandlerEntry>,
}

impl std::fmt::Debug for InProcessPluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessPluginHost")
            .field("handler_count", &self.handlers.len())
            .finish()
    }
}

impl InProcessPluginHost {
    /// Create a new in-process plugin host with the given handlers.
    ///
    /// Each handler is a tuple of (name, caps, handler).
    pub fn new(handlers: Vec<(String, Vec<Cap>, Arc<dyn FrameHandler>)>) -> Self {
        let handlers = handlers
            .into_iter()
            .map(|(name, caps, handler)| HandlerEntry { name, caps, handler })
            .collect();
        Self { handlers }
    }

    /// Build the aggregate manifest as a JSON array of cap URN strings.
    /// Always includes CAP_IDENTITY as the first entry.
    fn build_manifest(&self) -> Vec<u8> {
        let mut cap_urns: Vec<String> = vec![CAP_IDENTITY.to_string()];
        for entry in &self.handlers {
            for cap in &entry.caps {
                let urn = cap.urn.to_string();
                if urn != CAP_IDENTITY {
                    cap_urns.push(urn);
                }
            }
        }
        serde_json::to_vec(&cap_urns).unwrap_or_else(|_| b"[]".to_vec())
    }

    /// Build the cap table for routing: flat list of (cap_urn, handler_idx).
    fn build_cap_table(handlers: &[HandlerEntry]) -> CapTable {
        let mut table = Vec::new();
        for (idx, entry) in handlers.iter().enumerate() {
            for cap in &entry.caps {
                table.push((cap.urn.to_string(), idx));
            }
        }
        table
    }

    /// Find the best handler for a cap URN using closest-specificity matching.
    ///
    /// Mirrors `PluginHostRuntime::find_plugin_for_cap()` exactly:
    /// - Request is pattern, registered cap is instance
    /// - Closest specificity to request wins
    /// - Ties broken by first match (deterministic)
    fn find_handler_for_cap(cap_table: &CapTable, cap_urn: &str) -> Option<usize> {
        let request_urn = match CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();
        let mut matches: Vec<(usize, usize)> = Vec::new(); // (handler_idx, specificity)

        for (registered_cap, handler_idx) in cap_table {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap) {
                if request_urn.accepts(&registered_urn) {
                    let specificity = registered_urn.specificity();
                    matches.push((*handler_idx, specificity));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

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
    /// `rt` is a tokio runtime handle for async handler calls.
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

        // Move handlers to Arc for sharing with handler threads
        let handlers = Arc::new(self.handlers);
        let cap_table = Self::build_cap_table(&handlers);

        // Active request channels: request_id → input_tx for forwarding frames to handler
        let mut active: HashMap<MessageId, mpsc::Sender<Frame>> = HashMap::new();

        // Built-in identity handler
        let identity_handler: Arc<dyn FrameHandler> = Arc::new(IdentityHandler);

        // Main read loop — forward frames to handlers, no accumulation
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

                    // Identity cap is "cap:" — exact string match, NOT conforms_to.
                    let is_identity = cap_urn == CAP_IDENTITY;

                    let handler: Arc<dyn FrameHandler> = if is_identity {
                        Arc::clone(&identity_handler)
                    } else {
                        match Self::find_handler_for_cap(&cap_table, &cap_urn) {
                            Some(idx) => Arc::clone(&handlers[idx].handler),
                            None => {
                                let mut err = Frame::err(
                                    rid,
                                    "NO_HANDLER",
                                    &format!("no handler for cap: {}", cap_urn),
                                );
                                err.routing_id = xid;
                                let _ = write_tx.send(err);
                                continue;
                            }
                        }
                    };

                    // Create channel for forwarding frames to handler
                    let (input_tx, input_rx) = mpsc::channel::<Frame>();
                    active.insert(rid.clone(), input_tx);

                    // Spawn handler thread
                    let output = ResponseWriter::new(
                        rid,
                        xid,
                        write_tx.clone(),
                        Limits::default().max_chunk,
                    );
                    let cap_urn_owned = cap_urn;
                    let rt_clone = rt.clone();
                    std::thread::spawn(move || {
                        handler.handle_request(&cap_urn_owned, input_rx, output, &rt_clone);
                    });
                }

                // Continuation frames: forward to handler
                FrameType::StreamStart | FrameType::Chunk | FrameType::StreamEnd => {
                    if let Some(tx) = active.get(&frame.id) {
                        let _ = tx.send(frame);
                    }
                }

                FrameType::End => {
                    // Forward END to handler, then remove from active
                    if let Some(tx) = active.remove(&frame.id) {
                        let _ = tx.send(frame);
                        // tx dropped here — handler sees channel close after END
                    }
                }

                FrameType::Heartbeat => {
                    let response = Frame::heartbeat(frame.id.clone());
                    let _ = write_tx.send(response);
                }

                FrameType::Err => {
                    // Error from relay for a pending request — close handler's input
                    active.remove(&frame.id);
                    // input_tx dropped — handler sees channel close and should exit
                }

                _ => {
                    // RelayNotify, RelayState, etc. — not expected from relay side
                }
            }
        }

        // Drop all active channels to signal handlers to exit
        active.clear();

        drop(write_tx);
        let _ = writer_thread.join();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bifaci::io::{FrameReader, FrameWriter};
    use crate::Cap;
    use std::os::unix::net::UnixStream;

    /// Echo handler: accumulates input, echoes raw bytes back.
    #[derive(Debug)]
    struct EchoHandler;

    impl FrameHandler for EchoHandler {
        fn handle_request(
            &self,
            _cap_urn: &str,
            input: mpsc::Receiver<Frame>,
            output: ResponseWriter,
            _rt: &tokio::runtime::Handle,
        ) {
            match accumulate_input(&input) {
                Ok(args) => {
                    let data: Vec<u8> = args.iter().flat_map(|a| a.value.clone()).collect();
                    output.emit_response("media:bytes", &data);
                }
                Err(e) => {
                    output.emit_error("ACCUMULATE_ERROR", &e);
                }
            }
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

    /// Build a CBOR-encoded chunk payload from raw bytes (matching build_request_frames).
    fn cbor_bytes_payload(data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(&ciborium::Value::Bytes(data.to_vec()), &mut buf)
            .expect("BUG: CBOR encode");
        buf
    }

    /// CBOR-decode a response chunk payload to extract raw bytes.
    fn decode_chunk_payload(payload: &[u8]) -> Vec<u8> {
        let value: ciborium::Value =
            ciborium::from_reader(payload).expect("response chunk not valid CBOR");
        match value {
            ciborium::Value::Bytes(b) => b,
            ciborium::Value::Text(s) => s.into_bytes(),
            other => panic!("unexpected CBOR type in response chunk: {:?}", other),
        }
    }

    // TEST654: InProcessPluginHost routes REQ to matching handler and returns response
    #[test]
    fn test654_routes_req_to_handler() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cap_urn = "cap:in=\"media:text;bytes\";op=echo;out=\"media:text;bytes\"";
        let cap = make_test_cap(cap_urn);
        let handlers = vec![(
            "echo".to_string(),
            vec![cap],
            Arc::new(EchoHandler) as Arc<dyn FrameHandler>,
        )];

        let host = InProcessPluginHost::new(handlers);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || host.run(host_sock, host_sock2, handle));

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // First frame should be RelayNotify with manifest
        let notify = reader.read().unwrap().unwrap();
        assert_eq!(notify.frame_type, FrameType::RelayNotify);
        let manifest = notify.relay_notify_manifest().unwrap();
        let cap_urns: Vec<String> = serde_json::from_slice(manifest).unwrap();
        assert!(cap_urns.len() >= 2); // identity + echo cap
        assert_eq!(cap_urns[0], CAP_IDENTITY);

        // Send a REQ + STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), cap_urn, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(1));
        writer.write(&req).unwrap();

        let ss = Frame::stream_start(
            rid.clone(),
            "arg0".to_string(),
            "media:text;bytes".to_string(),
        );
        writer.write(&ss).unwrap();

        let payload = cbor_bytes_payload(b"hello world");
        let checksum = Frame::compute_checksum(&payload);
        let chunk = Frame::chunk(rid.clone(), "arg0".to_string(), 0, payload, 0, checksum);
        writer.write(&chunk).unwrap();

        let se = Frame::stream_end(rid.clone(), "arg0".to_string(), 1);
        writer.write(&se).unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).unwrap();

        // Read response: STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
        let resp_ss = reader.read().unwrap().unwrap();
        assert_eq!(resp_ss.frame_type, FrameType::StreamStart);
        assert_eq!(resp_ss.id, rid);
        assert_eq!(resp_ss.stream_id.as_deref(), Some("result"));

        let resp_chunk = reader.read().unwrap().unwrap();
        assert_eq!(resp_chunk.frame_type, FrameType::Chunk);
        let resp_data = decode_chunk_payload(resp_chunk.payload.as_deref().unwrap());
        assert_eq!(resp_data, b"hello world");

        let resp_se = reader.read().unwrap().unwrap();
        assert_eq!(resp_se.frame_type, FrameType::StreamEnd);

        let resp_end = reader.read().unwrap().unwrap();
        assert_eq!(resp_end.frame_type, FrameType::End);

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST655: InProcessPluginHost handles identity verification (echo nonce)
    #[test]
    fn test655_identity_verification() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || host.run(host_sock, host_sock2, handle));

        let mut reader = FrameReader::new(test_sock2);
        let mut writer = FrameWriter::new(test_sock);

        // Skip RelayNotify
        let _notify = reader.read().unwrap().unwrap();

        // Send identity verification
        let rid = MessageId::new_uuid();
        let mut req = Frame::req(rid.clone(), CAP_IDENTITY, vec![], "application/cbor");
        req.routing_id = Some(MessageId::Uint(0));
        writer.write(&req).unwrap();

        // Send nonce via stream (already CBOR-encoded by identity_nonce)
        let nonce = crate::bifaci::io::identity_nonce();
        let ss = Frame::stream_start(
            rid.clone(),
            "identity-verify".to_string(),
            "media:bytes".to_string(),
        );
        writer.write(&ss).unwrap();

        let checksum = Frame::compute_checksum(&nonce);
        let chunk = Frame::chunk(
            rid.clone(),
            "identity-verify".to_string(),
            0,
            nonce.clone(),
            0,
            checksum,
        );
        writer.write(&chunk).unwrap();

        let se = Frame::stream_end(rid.clone(), "identity-verify".to_string(), 1);
        writer.write(&se).unwrap();

        let end = Frame::end(rid.clone(), None);
        writer.write(&end).unwrap();

        // Read echoed response — identity echoes raw bytes (no CBOR decode/encode)
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

    // TEST656: InProcessPluginHost returns NO_HANDLER for unregistered cap
    #[test]
    fn test656_no_handler_returns_err() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || host.run(host_sock, host_sock2, handle));

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

    // TEST657: InProcessPluginHost manifest includes identity cap and handler caps
    #[test]
    fn test657_manifest_includes_all_caps() {
        let cap_urn = "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessPluginHost::new(vec![(
            "thumb".to_string(),
            vec![cap],
            Arc::new(EchoHandler) as Arc<dyn FrameHandler>,
        )]);

        let manifest = host.build_manifest();
        let cap_urns: Vec<String> = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(cap_urns[0], CAP_IDENTITY);
        assert!(cap_urns.iter().any(|u| u.contains("thumbnail")));
    }

    // TEST658: InProcessPluginHost handles heartbeat by echoing same ID
    #[test]
    fn test658_heartbeat_response() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let host = InProcessPluginHost::new(vec![]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || host.run(host_sock, host_sock2, handle));

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

    // TEST659: InProcessPluginHost handler error returns ERR frame
    #[test]
    fn test659_handler_error_returns_err_frame() {
        /// Handler that always fails.
        #[derive(Debug)]
        struct FailHandler;

        impl FrameHandler for FailHandler {
            fn handle_request(
                &self,
                _cap_urn: &str,
                input: mpsc::Receiver<Frame>,
                output: ResponseWriter,
                _rt: &tokio::runtime::Handle,
            ) {
                // Drain input
                for frame in input.iter() {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_error("PROVIDER_ERROR", "provider crashed");
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let cap_urn = "cap:in=\"media:void\";op=fail;out=\"media:void\"";
        let cap = make_test_cap(cap_urn);
        let host = InProcessPluginHost::new(vec![(
            "fail".to_string(),
            vec![cap],
            Arc::new(FailHandler) as Arc<dyn FrameHandler>,
        )]);

        let (host_sock, test_sock) = UnixStream::pair().unwrap();
        let (host_sock2, test_sock2) = UnixStream::pair().unwrap();

        let handle = rt.handle().clone();
        let host_thread = std::thread::spawn(move || host.run(host_sock, host_sock2, handle));

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
        assert!(err_frame
            .error_message()
            .unwrap()
            .contains("provider crashed"));

        drop(writer);
        drop(reader);
        host_thread.join().unwrap().unwrap();
    }

    // TEST660: InProcessPluginHost closest-specificity routing prefers specific over identity
    #[test]
    fn test660_closest_specificity_routing() {
        let specific_urn =
            "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";
        let generic_urn =
            "cap:in=\"media:image;bytes\";op=thumbnail;out=\"media:image;png;bytes\"";

        let specific_cap = make_test_cap(specific_urn);
        let generic_cap = make_test_cap(generic_urn);

        /// Handler that tags its output with its name.
        #[derive(Debug)]
        struct TaggedHandler(String);

        impl FrameHandler for TaggedHandler {
            fn handle_request(
                &self,
                _cap_urn: &str,
                input: mpsc::Receiver<Frame>,
                output: ResponseWriter,
                _rt: &tokio::runtime::Handle,
            ) {
                // Drain input
                for frame in input.iter() {
                    if frame.frame_type == FrameType::End {
                        break;
                    }
                }
                output.emit_response("media:text;bytes", self.0.as_bytes());
            }
        }

        let handlers = vec![
            (
                "generic".to_string(),
                vec![generic_cap],
                Arc::new(TaggedHandler("generic".into())) as Arc<dyn FrameHandler>,
            ),
            (
                "specific".to_string(),
                vec![specific_cap],
                Arc::new(TaggedHandler("specific".into())) as Arc<dyn FrameHandler>,
            ),
        ];

        let host = InProcessPluginHost::new(handlers);
        let cap_table = InProcessPluginHost::build_cap_table(&host.handlers);

        // Request for pdf thumbnail should match specific (pdf, specificity 3) over generic (image, specificity 2)
        let result = InProcessPluginHost::find_handler_for_cap(
            &cap_table,
            "cap:in=\"media:pdf;bytes\";op=thumbnail;out=\"media:image;png;bytes\"",
        );
        assert_eq!(result, Some(1)); // specific handler
    }
}
