//! Shared stream I/O operations for cap execution.
//!
//! These functions handle the bifaci protocol's CBOR transport layer:
//! sending input streams to cartridges and collecting/decoding their
//! responses. Used by both the machfab engine (capdag_service) and
//! the capdag CLI orchestrator executor.
//!
//! The key invariant: node data between caps is stored as raw bytes
//! (unwrapped from CBOR transport). Sequence-mode output is stored
//! as an RFC 8742 CBOR sequence where each item's CBOR Bytes/Text
//! wrapper has been unwrapped to raw bytes, then re-encoded as
//! CBOR Bytes for self-delimiting boundaries.

use crate::bifaci::frame::{Frame, FrameType, MessageId};
use crate::bifaci::relay_switch::RelaySwitch;
use crate::orchestrator::executor::CapProgressFn;
use crate::StreamMeta;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum StreamIoError {
    #[error("Stream I/O error: {0}")]
    Transport(String),

    #[error("CBOR encoding error: {0}")]
    CborEncode(String),

    #[error("CBOR decoding error: {0}")]
    CborDecode(String),

    #[error("Protocol error: expected Bytes or Text in CBOR transport at item {index}, got {description}")]
    UnexpectedCborType { index: usize, description: String },

    /// Cap-level failure: the cartridge returned END without success, ERR frame,
    /// or the response channel closed without an END. `cap_urn` identifies the
    /// failing cap; `details` carries the cartridge's error message or the
    /// protocol violation detail.
    #[error("Cap '{cap_urn}' failed: {details}")]
    Terminal { cap_urn: String, details: String },

    /// Writer failure — the `IncrementalWriter` returned an error while
    /// persisting chunk data.
    #[error("Writer error: {0}")]
    Writer(String),
}

// =============================================================================
// Activity tracking
// =============================================================================

/// Pipeline-level stall warning threshold in seconds.
///
/// If no progress LOG frame arrives from ANY body in the entire pipeline
/// for this duration, the runtime emits a one-shot warning ("no progress
/// from any body for Ns — continuing to wait. Use Cancel to abort.")
/// and keeps waiting. The pipeline is NOT aborted automatically —
/// long-running caps (model loads, vision/LLM inference, large audio
/// transcription) legitimately exceed this threshold, and aborting them
/// produced false negatives on every honest long workload. Cancellation
/// is the user's call, via the explicit cancel-task path.
///
/// The constant retains the historical "stall timeout" name (and
/// duration) so existing telemetry and log-grep dashboards keyed on
/// "120s" continue to land on the same event. Only the runtime's
/// reaction changed (warn-once vs abort).
pub const PIPELINE_STALL_TIMEOUT_SECS: u64 = 120;

/// Per-cap activity timer used by `collect_terminal_output`.
///
/// Tracks time since the last activity frame from a cap. A "queued" LOG frame
/// pauses the timer (the cartridge has confirmed receipt but is waiting for a
/// handler slot), and any other LOG, progress, or data frame unpauses it and
/// resets the clock.
pub struct ActivityTimer {
    last_activity: Instant,
    paused: bool,
    timeout: Duration,
}

impl ActivityTimer {
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            last_activity: Instant::now(),
            paused: false,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Record activity and resume if paused.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
        self.paused = false;
    }

    /// Pause the timeout (request is queued, no progress expected).
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Handle a LOG frame's level:
    /// - `"queued"` → pause (request waiting in cartridge queue)
    /// - anything else → touch (handler active, reset timer)
    pub fn handle_log_level(&mut self, level: &str) {
        match level {
            "queued" => self.pause(),
            _ => self.touch(),
        }
    }

    /// Check if the timeout has been exceeded. Returns false when paused.
    pub fn is_expired(&self) -> bool {
        !self.paused && self.last_activity.elapsed() > self.timeout
    }
}

/// Shared timestamp for pipeline-level stall detection.
///
/// Stores `Instant::elapsed().as_millis()` of the last progress event.
/// Updated by any body's progress callback. Read by the watchdog task.
pub struct PipelineProgressTracker {
    epoch: Instant,
    last_progress_ms: AtomicU64,
}

impl Default for PipelineProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineProgressTracker {
    pub fn new() -> Self {
        let epoch = Instant::now();
        Self {
            epoch,
            last_progress_ms: AtomicU64::new(epoch.elapsed().as_millis() as u64),
        }
    }

    /// Record that progress was observed.
    pub fn touch(&self) {
        self.last_progress_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Check if the stall timeout has been exceeded.
    pub fn is_stalled(&self) -> bool {
        let last_ms = self.last_progress_ms.load(Ordering::Relaxed);
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        now_ms.saturating_sub(last_ms) > PIPELINE_STALL_TIMEOUT_SECS * 1000
    }
}

// =============================================================================
// Logging callback
// =============================================================================

/// Pipeline-level log callback.
///
/// Arguments: `(cap_urn, level, message, body_index)`.
/// `body_index` is `Some` for pipeline bodies (ForEach parallelism) and `None`
/// for single-body / CLI execution.
pub type PipelineLogFn = Arc<dyn Fn(&str, &str, &str, Option<usize>) + Send + Sync>;

// =============================================================================
// Terminal output meta
// =============================================================================

/// Metadata collected from the terminal cap's output stream.
///
/// `stream_meta` is set from the STREAM_START frame.
/// `item_metas` collects per-item meta from each CHUNK frame that starts a new
/// sequence item (used by ForEach to propagate per-item provenance).
#[derive(Debug, Clone, Default)]
pub struct TerminalMeta {
    pub stream_meta: Option<StreamMeta>,
    pub item_metas: Vec<StreamMeta>,
}

// =============================================================================
// Incremental writer
// =============================================================================

/// Trait for streaming terminal output to disk as it arrives.
///
/// Implementations decide storage policy (blob vs sequence, provenance
/// sidecars, etc.). The collect loop calls these in order:
/// `on_stream_start` → 0..N `on_chunk_payload` → `on_stream_end`.
#[async_trait]
pub trait IncrementalWriter: Send {
    /// Called on STREAM_START. `is_sequence` mirrors the wire flag;
    /// `media_urn` is the stream's declared media URN; `meta` is the
    /// STREAM_START frame's meta map; `stream_id` is the wire stream id.
    async fn on_stream_start(
        &mut self,
        is_sequence: Option<bool>,
        media_urn: &str,
        meta: Option<StreamMeta>,
        stream_id: Option<String>,
    ) -> Result<(), StreamIoError>;

    /// Called on each CHUNK. `payload` is the raw CBOR payload of the chunk;
    /// `meta` is the CHUNK frame's meta (set on first chunk of each sequence
    /// item; None otherwise).
    async fn on_chunk_payload(
        &mut self,
        payload: &[u8],
        meta: Option<StreamMeta>,
    ) -> Result<(), StreamIoError>;

    /// Called on STREAM_END. Flushes buffered state.
    async fn on_stream_end(&mut self) -> Result<(), StreamIoError>;
}

/// Send a single input stream (STREAM_START → CHUNKs → STREAM_END) to a cartridge.
///
/// Handles both scalar and sequence mode:
/// - Scalar (`is_sequence=false`): wraps each chunk in `CBOR::Bytes`
/// - Sequence (`is_sequence=true`): sends raw CBOR item bytes directly
///   (matching `emit_list_item` semantics on the cartridge side)
pub async fn send_one_stream(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    media_urn: &str,
    data: &[u8],
    meta: Option<StreamMeta>,
    is_sequence: bool,
    max_chunk: usize,
) -> Result<(), StreamIoError> {
    let stream_id = uuid::Uuid::new_v4().to_string();

    let mut ss = Frame::stream_start(
        rid.clone(),
        stream_id.clone(),
        media_urn.to_string(),
        if is_sequence { Some(true) } else { None },
    );
    ss.meta = meta;
    switch
        .send_to_master(ss, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_START: {}", e)))?;

    let mut chunk_index = 0u64;

    if is_sequence {
        // Sequence mode: data is an RFC 8742 CBOR sequence.
        // Each self-delimiting CBOR value is sent as a separate chunk
        // payload. The chunk payload IS the raw CBOR bytes of the item
        // (not re-wrapped).
        if !data.is_empty() {
            let mut cursor = std::io::Cursor::new(data);
            while (cursor.position() as usize) < data.len() {
                let start_pos = cursor.position() as usize;
                let _value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                    StreamIoError::CborDecode(format!("sequence item {}: {}", chunk_index, e))
                })?;
                let end_pos = cursor.position() as usize;
                let item_cbor = &data[start_pos..end_pos];

                let checksum = Frame::compute_checksum(item_cbor);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    item_cbor.to_vec(),
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                chunk_index += 1;
            }
        }
    } else {
        // Scalar mode: data is raw bytes, wrapped as CBOR::Bytes per chunk.
        if data.is_empty() {
            let cbor_value = ciborium::Value::Bytes(vec![]);
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&cbor_value, &mut cbor_payload)
                .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
            let checksum = Frame::compute_checksum(&cbor_payload);
            let chunk =
                Frame::chunk(rid.clone(), stream_id.clone(), 0, cbor_payload, 0, checksum);
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
            chunk_index = 1;
        } else {
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + max_chunk).min(data.len());
                let chunk_data = &data[offset..end];
                let cbor_value = ciborium::Value::Bytes(chunk_data.to_vec());
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(&cbor_value, &mut cbor_payload)
                    .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
                let checksum = Frame::compute_checksum(&cbor_payload);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    cbor_payload,
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                offset = end;
                chunk_index += 1;
            }
        }
    }

    let se = Frame::stream_end(rid.clone(), stream_id, chunk_index);
    switch
        .send_to_master(se, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_END: {}", e)))?;

    Ok(())
}

/// Decode terminal output bytes based on is_sequence flag.
///
/// Returns `Vec<Vec<u8>>` — a list of unwrapped items:
/// - `is_sequence=true` (emit_list_item): each CBOR value in the
///   sequence is unwrapped (Bytes→raw, Text→UTF-8) into a separate item.
/// - `is_sequence=false/None` (write/emit_cbor): CBOR Bytes/Text
///   wrappers are unwrapped and concatenated into a single item.
pub fn decode_terminal_output(
    response_chunks: &[u8],
    is_sequence: Option<bool>,
) -> Result<Vec<Vec<u8>>, StreamIoError> {
    if response_chunks.is_empty() {
        return Ok(vec![vec![]]);
    }

    if is_sequence == Some(true) {
        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                StreamIoError::CborDecode(format!("sequence item {}: {}", items.len(), e))
            })?;
            let raw = unwrap_cbor_value(value, items.len())?;
            items.push(raw);
        }
        Ok(items)
    } else {
        let mut output_bytes = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                StreamIoError::CborDecode(format!("terminal response: {}", e))
            })?;
            let raw = unwrap_cbor_value(value, 0)?;
            output_bytes.extend(raw);
        }
        Ok(vec![output_bytes])
    }
}

/// Unwrap a CBOR transport value to raw bytes.
///
/// Bytes → inner bytes, Text → UTF-8 bytes. Anything else is a
/// protocol error.
pub fn unwrap_cbor_value(value: ciborium::Value, item_index: usize) -> Result<Vec<u8>, StreamIoError> {
    match value {
        ciborium::Value::Bytes(b) => Ok(b),
        ciborium::Value::Text(t) => Ok(t.into_bytes()),
        _ => Err(StreamIoError::UnexpectedCborType {
            index: item_index,
            description: format!("{:?}", value),
        }),
    }
}

// =============================================================================
// Terminal collect
// =============================================================================

/// Collect the terminal response from a cap, decoding frames as they arrive.
///
/// Walks the response stream (STREAM_START → CHUNK… → STREAM_END → END), with
/// optional per-cap progress callbacks, pipeline logging, a shared pipeline
/// stall tracker, and an optional `IncrementalWriter` that streams the bytes
/// to disk rather than buffering them in memory.
///
/// Returns `(response_bytes, is_sequence, terminal_meta)`. When a writer is
/// provided, `response_bytes` is empty — the data is already persisted via
/// the writer.
///
/// # Error semantics
/// - `END` with non-zero or absent `exit_code` → `StreamIoError::Terminal`.
/// - `ERR` frame → `StreamIoError::Terminal`.
/// - Response channel closed without `END` → `StreamIoError::Terminal`.
/// - No activity for `activity_timeout_secs` → cancel request at relay,
///   return `StreamIoError::ActivityTimeout`.
/// - Writer failures bubble up as their `StreamIoError::Writer`.
pub async fn collect_terminal_output(
    mut rx: mpsc::UnboundedReceiver<Frame>,
    progress_fn: Option<&CapProgressFn>,
    cap_urn: &str,
    log_fn: Option<&PipelineLogFn>,
    body_index: Option<usize>,
    stall_tracker: Option<&Arc<PipelineProgressTracker>>,
    writer: Option<&mut dyn IncrementalWriter>,
    activity_timeout_secs: u64,
) -> Result<(Vec<u8>, Option<bool>, TerminalMeta), StreamIoError> {
    let mut response_chunks: Vec<u8> = Vec::new();
    let mut is_sequence: Option<bool> = None;
    let mut timer = ActivityTimer::new(activity_timeout_secs);
    let has_writer = writer.is_some();
    let mut terminal_meta = TerminalMeta::default();
    // Whether we've already emitted the per-cap activity-silence warning
    // for the current quiet window. We do NOT abort on activity timeout:
    // long-running terminal caps (vision/LLM inference, large transcription)
    // legitimately sit silent for far longer than the threshold, and
    // aborting them produced false negatives on every honest workload.
    // The user cancels via the explicit cancel-task path; the timer's
    // job is now to surface the silence as a one-shot warning so the
    // operator knows the cap is alive-but-quiet, without log-spam every
    // 500 ms. The flag resets to false the next time any frame arrives.
    let mut activity_warning_logged = false;

    // Rebind writer as mutable — we pass it through as Option<&mut> but need
    // to call methods on it inside the loop.
    let mut writer = writer;

    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;

        match frame {
            Ok(Some(frame)) => {
                // Any frame from this cap means the pipeline is alive.
                if let Some(tracker) = stall_tracker {
                    tracker.touch();
                }
                // Re-arm the warn-once gate for any subsequent silence.
                activity_warning_logged = false;
                match frame.frame_type {
                    FrameType::Chunk => {
                        timer.touch();
                        if let Some(payload) = &frame.payload {
                            if let Some(ref mut w) = writer {
                                w.on_chunk_payload(payload, frame.meta.clone()).await?;
                            } else {
                                response_chunks.extend_from_slice(payload);
                                // Collect per-item meta for ForEach propagation.
                                // Each non-None chunk meta marks the first chunk
                                // of a new item.
                                if let Some(meta) = frame.meta.clone() {
                                    terminal_meta.item_metas.push(meta);
                                }
                            }
                        }
                    }
                    FrameType::End => {
                        // exit_code in END meta: 0 = success, absent or non-zero
                        // = failure. Absence means the cartridge died (OOM,
                        // crash) and the relay synthesized a bare END — treat
                        // as failure.
                        let exit_code = frame.exit_code();
                        if exit_code != Some(0) {
                            let detail = match exit_code {
                                Some(code) => format!("exit_code={}", code),
                                None => "exit_code absent (cartridge likely crashed)".to_string(),
                            };
                            let details =
                                format!("END without success: {}", detail);
                            if let Some(lfn) = &log_fn {
                                lfn(cap_urn, "error", &details, body_index);
                            }
                            return Err(StreamIoError::Terminal {
                                cap_urn: cap_urn.to_string(),
                                details,
                            });
                        }

                        if let Some(payload) = &frame.payload {
                            if let Some(ref mut w) = writer {
                                if !payload.is_empty() {
                                    w.on_chunk_payload(payload, frame.meta.clone()).await?;
                                }
                            } else {
                                response_chunks.extend_from_slice(payload);
                            }
                        }
                        if let Some(ref mut w) = writer {
                            w.on_stream_end().await?;
                        }
                        let _ = has_writer;
                        return Ok((response_chunks, is_sequence, terminal_meta));
                    }
                    FrameType::Err => {
                        let msg = frame
                            .error_message()
                            .unwrap_or("Unknown cartridge error")
                            .to_string();
                        if let Some(lfn) = &log_fn {
                            lfn(cap_urn, "error", &msg, body_index);
                        }
                        return Err(StreamIoError::Terminal {
                            cap_urn: cap_urn.to_string(),
                            details: msg,
                        });
                    }
                    FrameType::Log => {
                        let level = frame.log_level().unwrap_or("info");
                        timer.handle_log_level(level);

                        if let Some(p) = frame.log_progress() {
                            let cartridge_msg = frame.log_message().unwrap_or("");
                            if let Some(pfn) = &progress_fn {
                                pfn(p, cap_urn, cartridge_msg);
                            }
                            if let Some(lfn) = &log_fn {
                                lfn(cap_urn, "progress", cartridge_msg, body_index);
                            }
                        } else if let Some(msg) = frame.log_message() {
                            if let Some(lfn) = &log_fn {
                                lfn(cap_urn, level, msg, body_index);
                            }
                        }
                    }
                    FrameType::StreamStart => {
                        timer.touch();
                        if let Some(seq) = frame.is_sequence {
                            is_sequence = Some(seq);
                        }
                        if let Some(ref mut w) = writer {
                            let media = frame.media_urn.as_deref().unwrap_or("");
                            w.on_stream_start(
                                is_sequence,
                                media,
                                frame.meta.clone(),
                                frame.stream_id.clone(),
                            )
                            .await?;
                        } else {
                            // Capture stream-level meta for ForEach propagation
                            terminal_meta.stream_meta = frame.meta.clone();
                        }
                    }
                    _ => {
                        // STREAM_END and others — structural, skip
                    }
                }
            }
            Ok(None) => {
                let details = "response channel closed without END".to_string();
                if let Some(lfn) = &log_fn {
                    lfn(cap_urn, "error", &details, body_index);
                }
                return Err(StreamIoError::Terminal {
                    cap_urn: cap_urn.to_string(),
                    details,
                });
            }
            Err(_timeout) => {
                // Per-cap activity-silence observation, NOT an abort.
                // Long-running terminal caps legitimately sit silent
                // for far longer than the threshold; cancellation is
                // the user's call via the explicit cancel-task path.
                // The runtime keeps waiting and emits a one-shot
                // warning at the threshold so the log shows "no
                // activity for Ns from cap X" without log-spam every
                // 500 ms.
                if timer.is_expired() && !activity_warning_logged {
                    let details = format!(
                        "no activity for {}s — continuing to wait. Use Cancel to abort.",
                        activity_timeout_secs
                    );
                    if let Some(lfn) = &log_fn {
                        lfn(cap_urn, "warn", &details, body_index);
                    }
                    tracing::warn!(
                        cap_urn = %cap_urn,
                        "[cap] No activity for {}s; continuing to wait for completion or cancel",
                        activity_timeout_secs
                    );
                    activity_warning_logged = true;
                }
            }
        }
    }
}
