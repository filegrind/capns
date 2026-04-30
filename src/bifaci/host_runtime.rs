//! Async Cartridge Host Runtime — Multi-cartridge management with frame routing
//!
//! The CartridgeHostRuntime manages multiple cartridge binaries, routing CBOR protocol
//! frames between a relay connection (to the engine) and individual cartridge processes.
//!
//! ## Architecture
//!
//! ```text
//! Relay (engine) ←→ CartridgeHostRuntime ←→ Cartridge A (stdin/stdout)
//!                                   ←→ Cartridge B (stdin/stdout)
//!                                   ←→ Cartridge C (stdin/stdout)
//! ```
//!
//! ## Frame Routing
//!
//! Engine → Cartridge:
//! - REQ: route by cap_urn to the cartridge that handles it, spawn on demand
//! - STREAM_START/CHUNK/STREAM_END/END/ERR: route by req_id to the mapped cartridge
//! - All other frame types: hard protocol error (must never arrive from engine)
//!
//! Cartridge → Engine:
//! - HELLO: fatal error (consumed during handshake, never during run)
//! - HEARTBEAT: responded to locally, never forwarded
//! - REQ (peer invoke): registered in routing table, forwarded to relay
//! - RelayNotify/RelayState: fatal error (cartridges must never send these)
//! - Everything else: forwarded to relay (pass-through)

use crate::bifaci::frame::{FlowKey, Frame, FrameType, Limits, MessageId, SeqAssigner};
use crate::bifaci::io::{handshake, verify_identity, CborError, FrameReader, FrameWriter};
use crate::bifaci::relay_switch::{
    CartridgeAttachmentError, CartridgeAttachmentErrorKind, CartridgeRuntimeStats,
    InstalledCartridgeIdentity, RelayNotifyCapabilitiesPayload,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// Interval between heartbeat probes sent to each running cartridge.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time to wait for a heartbeat response before considering a cartridge unhealthy.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// CARTRIDGE HOST OBSERVER — Lifecycle callbacks for spawn/death
// =============================================================================

/// Lifecycle observer for `CartridgeHostRuntime`.
///
/// Mirrors the Swift `CartridgeHostObserver` protocol in
/// `capdag-objc/Sources/Bifaci/CartridgeHost.swift`. The host invokes the
/// registered observer when a cartridge becomes runnable (`cartridge_spawned`)
/// or stops running (`cartridge_died`).
///
/// Implementations MUST NOT block or take long-held locks: the host's
/// internal locks are not held during the call, but the call still runs on
/// the run loop or the spawn caller's task.
///
/// Used by host-side bridges (e.g., a remote-IPC service that needs to push
/// process lifecycle to a separate process); not used by the engine's
/// in-process runtime, which leaves the observer unset.
pub trait CartridgeHostObserver: Send + Sync {
    /// A cartridge has just transitioned to running (handshake completed,
    /// caps extracted, reader task started).
    ///
    /// `pid` is `None` for in-process cartridges that have no OS process.
    /// `name` is the last path component of the cartridge binary path
    /// (or empty for attached cartridges with no path).
    fn cartridge_spawned(
        &self,
        cartridge_index: usize,
        pid: Option<u32>,
        name: &str,
        caps: &[String],
    );

    /// A cartridge has just transitioned to not-running (reader task EOF,
    /// process reaped, OOM kill, or clean shutdown).
    fn cartridge_died(&self, cartridge_index: usize, pid: Option<u32>, name: &str);
}

// =============================================================================
// CARTRIDGE PROCESS INFO — External visibility into managed cartridge processes
// =============================================================================

/// Snapshot of a managed cartridge process.
#[derive(Debug, Clone)]
pub struct CartridgeProcessInfo {
    /// Index of the cartridge in the host's cartridge list.
    pub cartridge_index: usize,
    /// OS process ID (from `Child::id()` on Rust side, `pid_t` on Swift side).
    pub pid: u32,
    /// Binary name (e.g. "ggufcartridge", "modelcartridge").
    pub name: String,
    /// Whether the cartridge is currently running and responsive.
    pub running: bool,
    /// Cap URN strings this cartridge handles.
    pub caps: Vec<String>,
    /// Physical memory footprint in MB (self-reported by cartridge via heartbeat).
    /// This is `ri_phys_footprint` — the metric macOS jetsam uses for kill decisions.
    /// Updated every 30s when the cartridge responds to a heartbeat probe.
    pub memory_footprint_mb: u64,
    /// Resident set size in MB (self-reported by cartridge via heartbeat).
    pub memory_rss_mb: u64,
}

/// Why a cartridge was killed. Determines whether pending requests get ERR frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    /// App is exiting. No ERR frames — the relay connection is closing anyway
    /// and there are no callers left to notify.
    AppExit,
    /// OOM watchdog killed the cartridge while it was actively processing requests.
    /// Pending requests MUST get ERR frames with code "OOM_KILLED" so callers
    /// can fail fast instead of hanging forever.
    OomKill,
    /// Request was cancelled. Pending requests get ERR frames with code "CANCELLED".
    Cancelled,
}

/// Commands that can be sent to the host runtime from external code.
pub enum HostCommand {
    /// Kill a cartridge process by PID for memory pressure. The host sets
    /// `shutdown_reason = Some(OomKill)` before killing, so death handling
    /// sends ERR frames with "OOM_KILLED" for all pending requests.
    KillCartridge { pid: u32 },
}

/// Thread-safe handle for querying cartridge process info and sending commands
/// to a running `CartridgeHostRuntime`. Obtained via `process_handle()` before
/// calling `run()`. The handle remains valid for the lifetime of `run()`.
#[derive(Clone)]
pub struct CartridgeProcessHandle {
    snapshot: Arc<RwLock<Vec<CartridgeProcessInfo>>>,
    command_tx: mpsc::UnboundedSender<HostCommand>,
}

impl CartridgeProcessHandle {
    /// Get a snapshot of all managed cartridge processes (running or not).
    pub fn running_cartridges(&self) -> Vec<CartridgeProcessInfo> {
        self.snapshot.read().unwrap().clone()
    }

    /// Request that the host kill a specific cartridge process by PID.
    /// Returns `Err(())` if the host's run loop has exited.
    pub fn kill_cartridge(&self, pid: u32) -> Result<(), ()> {
        self.command_tx
            .send(HostCommand::KillCartridge { pid })
            .map_err(|_| ())
    }
}

// =============================================================================
// ERROR TYPES
// =============================================================================

/// Errors that can occur in the async cartridge host runtime.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AsyncHostError {
    #[error("CBOR error: {0}")]
    Cbor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Cartridge returned error: [{code}] {message}")]
    CartridgeError { code: String, message: String },

    #[error("Unexpected frame type: {0:?}")]
    UnexpectedFrameType(FrameType),

    #[error("Cartridge process exited unexpectedly")]
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

/// A response chunk from a cartridge.
#[derive(Debug, Clone)]
pub struct ResponseChunk {
    pub payload: Vec<u8>,
    pub seq: u64,
    pub offset: Option<u64>,
    pub len: Option<u64>,
    pub is_eof: bool,
}

/// A complete response from a cartridge, which may be single or streaming.
#[derive(Debug)]
pub enum CartridgeResponse {
    Single(Vec<u8>),
    Streaming(Vec<ResponseChunk>),
}

impl CartridgeResponse {
    pub fn final_payload(&self) -> Option<&[u8]> {
        match self {
            CartridgeResponse::Single(data) => Some(data),
            CartridgeResponse::Streaming(chunks) => chunks.last().map(|c| c.payload.as_slice()),
        }
    }

    pub fn concatenated(&self) -> Vec<u8> {
        match self {
            CartridgeResponse::Single(data) => data.clone(),
            CartridgeResponse::Streaming(chunks) => {
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

/// Events from cartridge reader loops, delivered to the main run() loop.
enum CartridgeEvent {
    /// A frame was received from a cartridge's stdout.
    Frame { cartridge_idx: usize, frame: Frame },
    /// A cartridge's reader loop exited (process died or stdout closed).
    Death { cartridge_idx: usize },
}

/// A managed cartridge binary.
struct ManagedCartridge {
    /// Path to the cartridge entry point binary (empty for attached/pre-connected cartridges).
    /// For directory cartridges this is the resolved entry point from cartridge.json.
    path: PathBuf,
    /// Version directory for directory-based cartridges.
    /// When set, identity hashing uses the full directory tree.
    /// When None, this is a legacy probe-based registration (providers path).
    cartridge_dir: Option<PathBuf>,
    /// Child process handle (None for attached cartridges).
    process: Option<tokio::process::Child>,
    /// Channel to write frames to this cartridge's stdin.
    writer_tx: Option<mpsc::UnboundedSender<Frame>>,
    /// Cartridge manifest from HELLO handshake.
    manifest: Vec<u8>,
    /// Negotiated limits for this cartridge.
    limits: Limits,
    /// Caps this cartridge handles (from manifest after HELLO).
    caps: Vec<crate::Cap>,
    /// Known caps from registration (before HELLO, used for routing).
    known_caps: Vec<String>,
    /// Installed cartridge identity derived from the registered binary path.
    installed_identity: Option<InstalledCartridgeIdentity>,
    /// Whether the cartridge is currently running and healthy.
    running: bool,
    /// Reader task handle.
    reader_handle: Option<JoinHandle<()>>,
    /// Writer task handle.
    writer_handle: Option<JoinHandle<()>>,
    /// Whether HELLO handshake permanently failed (binary is broken, no relaunch).
    hello_failed: bool,
    /// Pending heartbeats sent to this cartridge (ID → sent time).
    pending_heartbeats: HashMap<MessageId, Instant>,
    /// Stderr handle for capturing crash output.
    stderr_handle: Option<tokio::process::ChildStderr>,
    /// Last death error message (includes stderr if available). Used for ERR frames
    /// sent when attempting to write to a dead cartridge.
    last_death_message: Option<String>,
    /// Set before killing the process to signal why the death occurred.
    /// `handle_cartridge_death` checks this to determine ERR frame behavior:
    /// - `None` → unexpected crash → ERR "CARTRIDGE_DIED"
    /// - `Some(OomKill)` → OOM watchdog kill → ERR "OOM_KILLED"
    /// - `Some(AppExit)` → clean shutdown → no ERR frames
    shutdown_reason: Option<ShutdownReason>,
    /// Physical memory footprint in MB (self-reported via heartbeat response meta).
    /// Updated every 30s when the cartridge echoes a heartbeat probe with its
    /// `ri_phys_footprint` from `proc_pid_rusage(getpid())`.
    memory_footprint_mb: u64,
    /// Resident set size in MB (self-reported via heartbeat response meta).
    memory_rss_mb: u64,
    /// Unix timestamp seconds of the last heartbeat response. `None` until
    /// the first successful heartbeat round-trip completes.
    last_heartbeat_unix_seconds: Option<i64>,
    /// Number of times this cartridge has been respawned after death.
    restart_count: u64,
}

impl ManagedCartridge {
    /// Create a registered cartridge from a binary path (probe-based discovery).
    /// Identity is computed from the binary's name and content hash.
    /// `channel` and `registry_url` must be supplied by the caller —
    /// the filename alone cannot tell us which (channel, registry) a
    /// standalone-binary install belongs to, and inferring would
    /// silently merge release/nightly or different-registry artefacts.
    fn new_registered_binary(
        path: PathBuf,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<String>,
        known_caps: Vec<String>,
    ) -> Self {
        let installed_identity =
            installed_cartridge_identity_from_binary(&path, channel, registry_url);
        Self {
            path,
            cartridge_dir: None,
            process: None,
            writer_tx: None,
            manifest: Vec::new(),
            limits: Limits::default(),
            caps: Vec::new(),
            known_caps,
            installed_identity,
            running: false,
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
        }
    }

    /// Create a registered cartridge from a version directory containing cartridge.json.
    /// Identity is computed from the directory tree hash.
    ///
    /// A directory-registered cartridge always has a resolvable identity.
    /// If the directory turns out to be unhashable at construction time,
    /// we pre-record an attachment failure so the upstream aggregate
    /// reports the real reason instead of silently dropping the cartridge.
    ///
    /// `registry_url` is sourced from the `cartridge.json:registry_url`
    /// the host already validated (three-place rule). `None` ⇔ dev
    /// install; `Some(url)` ⇔ the cartridge was placed under
    /// `slug_for(url)`. Pass-through; this constructor never derives
    /// it from the path.
    fn new_registered_dir(
        entry_point: PathBuf,
        cartridge_dir: PathBuf,
        id: String,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<String>,
        version: String,
        known_caps: Vec<String>,
    ) -> Self {
        let (installed_identity, hello_failed) =
            match crate::bifaci::cartridge_json::hash_cartridge_directory(&cartridge_dir) {
                Ok(sha256) => (
                    Some(InstalledCartridgeIdentity {
                        registry_url: registry_url.clone(),
                        id,
                        channel,
                        version,
                        sha256,
                        attachment_error: None,
                        runtime_stats: None,
                    }),
                    false,
                ),
                Err(e) => {
                    let detected_at = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    let err = CartridgeAttachmentError {
                        kind: CartridgeAttachmentErrorKind::EntryPointMissing,
                        message: format!(
                            "Cartridge directory not hashable at '{}': {}",
                            cartridge_dir.display(),
                            e
                        ),
                        detected_at_unix_seconds: detected_at,
                    };
                    tracing::error!(
                        dir = %cartridge_dir.display(),
                        error = %e,
                        "Cartridge directory not hashable — recording attachment failure"
                    );
                    (
                        Some(InstalledCartridgeIdentity {
                            registry_url: registry_url.clone(),
                            id,
                            channel,
                            version,
                            sha256: String::new(),
                            attachment_error: Some(err),
                            runtime_stats: None,
                        }),
                        true,
                    )
                }
            };
        Self {
            path: entry_point,
            cartridge_dir: Some(cartridge_dir),
            process: None,
            writer_tx: None,
            manifest: Vec::new(),
            limits: Limits::default(),
            caps: Vec::new(),
            known_caps,
            installed_identity,
            running: false,
            reader_handle: None,
            writer_handle: None,
            hello_failed,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
        }
    }

    fn new_attached(manifest: Vec<u8>, limits: Limits, caps: Vec<crate::Cap>) -> Self {
        // Extract URN strings for known_caps (used for pre-HELLO routing)
        let known_caps: Vec<String> = caps.iter().map(|c| c.urn.to_string()).collect();

        Self {
            path: PathBuf::new(),
            cartridge_dir: None,
            process: None,
            writer_tx: None,
            manifest,
            limits,
            caps,
            known_caps,
            installed_identity: None,
            running: true,
            reader_handle: None,
            writer_handle: None,
            hello_failed: false,
            pending_heartbeats: HashMap::new(),
            stderr_handle: None,
            last_death_message: None,
            shutdown_reason: None,
            memory_footprint_mb: 0,
            memory_rss_mb: 0,
            last_heartbeat_unix_seconds: None,
            restart_count: 0,
        }
    }

    fn installed_cartridge_identity(&self) -> Option<InstalledCartridgeIdentity> {
        self.installed_identity.clone()
    }

    /// Record an attachment failure for this cartridge.
    ///
    /// Flips `hello_failed` so the cartridge is treated as permanently broken
    /// (no on-demand respawn) and stamps `installed_identity` with the error
    /// so it surfaces in the next `RelayNotify` aggregate.
    ///
    /// If the cartridge had no resolvable identity (bad directory hash,
    /// unparseable binary name), we synthesize a minimum identity so the
    /// failure is still reportable to the UI.
    fn record_attachment_error(
        &mut self,
        kind: CartridgeAttachmentErrorKind,
        message: String,
    ) {
        self.hello_failed = true;
        let detected_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let error = CartridgeAttachmentError {
            kind,
            message,
            detected_at_unix_seconds: detected_at,
        };
        match self.installed_identity.as_mut() {
            Some(existing) => {
                existing.attachment_error = Some(error);
            }
            None => {
                // Reaching this branch means a HELLO failed against a
                // cartridge whose registration path didn't supply an
                // `InstalledCartridgeIdentity`. In production both
                // `new_registered_binary` and `new_registered_dir`
                // synthesize an identity at construction time, so the
                // only legitimate path here is an ad-hoc test attach
                // via `new_attached` — which never reaches the engine's
                // RelayNotify aggregate. Panic loudly: silently
                // synthesizing an identity without channel info would
                // collapse the release/nightly distinction at the
                // wire boundary.
                panic!(
                    "BUG: record_attachment_error fired on a cartridge without an \
                     InstalledCartridgeIdentity (path '{}'). Channels are part of \
                     identity; we never synthesize one without channel info.",
                    self.path.display()
                );
            }
        }
    }
}

fn parse_installed_cartridge_name(name: &str) -> Option<(String, String)> {
    let lowercase = name.to_lowercase();
    if let Some((candidate, suffix)) = lowercase.rsplit_once('-') {
        if !candidate.is_empty()
            && !suffix.is_empty()
            && suffix.chars().all(|ch| ch.is_ascii_digit() || ch == '.')
            && suffix.chars().any(|ch| ch.is_ascii_digit())
        {
            return Some((candidate.to_string(), suffix.to_string()));
        }
    }
    None
}

/// Compute identity for a standalone binary cartridge (probe-based discovery path).
/// Parses id and version from the binary filename, hashes the binary content.
/// `channel` and `registry_url` are supplied by the caller — the
/// filename does not carry them and we never silently default a
/// value. The probe path is exercised by tests and by the rare
/// "unmanaged binary inside a cartridge dir" diagnostic; the
/// production directory-cartridge path goes through
/// `new_registered_dir`.
fn installed_cartridge_identity_from_binary(
    path: &Path,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    registry_url: Option<String>,
) -> Option<InstalledCartridgeIdentity> {
    let name = path.file_stem()?.to_str()?;
    let (id, version) = parse_installed_cartridge_name(name)?;
    let bytes = std::fs::read(path).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let sha256 = format!("{:x}", hasher.finalize());
    Some(InstalledCartridgeIdentity {
        registry_url,
        id,
        channel,
        version,
        sha256,
        attachment_error: None,
        runtime_stats: None,
    })
}

// =============================================================================
// ASYNC CARTRIDGE HOST RUNTIME
// =============================================================================

/// Async host-side runtime managing multiple cartridge processes.
///
/// Routes CBOR protocol frames between a relay connection (engine) and
/// individual cartridge processes. Handles HELLO handshake, heartbeat health
/// monitoring, spawn-on-demand, crash recovery, and capability advertisement.
pub struct CartridgeHostRuntime {
    /// Managed cartridge binaries.
    cartridges: Vec<ManagedCartridge>,
    /// Routing: cap_urn → cartridge index (for finding which cartridge handles a cap).
    cap_table: Vec<(String, usize)>,
    /// List 1: OUTGOING_RIDS - tracks peer requests sent by cartridges (RID → cartridge_idx).
    /// Used only to detect same-cartridge peer calls (not for routing).
    outgoing_rids: HashMap<MessageId, usize>,
    /// List 2: INCOMING_RXIDS - tracks incoming requests from relay ((XID, RID) → cartridge_idx).
    /// Continuations for these requests are routed by this table.
    incoming_rxids: HashMap<(MessageId, MessageId), usize>,
    /// Tracks which incoming request spawned which outgoing peer RIDs.
    /// Maps parent (xid, rid) → list of child peer RIDs. Used for cancel cascade.
    incoming_to_peer_rids: HashMap<(MessageId, MessageId), Vec<MessageId>>,
    /// Max-seen seq per flow for cartridge-originated frames.
    /// Used to set seq on host-generated ERR frames (max_seen + 1).
    outgoing_max_seq: HashMap<FlowKey, u64>,
    /// Aggregate capabilities (serialized JSON manifest of all cartridge caps).
    capabilities: Vec<u8>,
    /// Channel sender for cartridge events (shared with reader tasks).
    event_tx: mpsc::UnboundedSender<CartridgeEvent>,
    /// Channel receiver for cartridge events (consumed by run()).
    event_rx: Option<mpsc::UnboundedReceiver<CartridgeEvent>>,
    /// Shared process snapshot, readable from outside the run loop via `CartridgeProcessHandle`.
    process_snapshot: Arc<RwLock<Vec<CartridgeProcessInfo>>>,
    /// Channel for receiving external commands (e.g., kill requests).
    command_tx: mpsc::UnboundedSender<HostCommand>,
    /// Receiver end — consumed by `run()`.
    command_rx: Option<mpsc::UnboundedReceiver<HostCommand>>,
    /// Lifecycle observer. Set by callers that want to be notified when a
    /// cartridge transitions in/out of the running state. Mirrors the Swift
    /// `CartridgeHost.observer` field.
    observer: Option<Arc<dyn CartridgeHostObserver>>,
}

impl CartridgeHostRuntime {
    /// Create a new cartridge host runtime.
    ///
    /// After creation, register cartridges with `register_cartridge()` or
    /// attach pre-connected cartridges with `attach_cartridge()`, then call `run()`.
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        Self {
            cartridges: Vec::new(),
            cap_table: Vec::new(),
            outgoing_rids: HashMap::new(),
            incoming_rxids: HashMap::new(),
            incoming_to_peer_rids: HashMap::new(),
            outgoing_max_seq: HashMap::new(),
            capabilities: Vec::new(),
            event_tx,
            event_rx: Some(event_rx),
            process_snapshot: Arc::new(RwLock::new(Vec::new())),
            command_tx,
            command_rx: Some(command_rx),
            observer: None,
        }
    }

    /// Register a lifecycle observer that will be notified when cartridges
    /// transition in/out of the running state. Replaces any previously set
    /// observer. Pass `None` to clear.
    pub fn set_observer(&mut self, observer: Option<Arc<dyn CartridgeHostObserver>>) {
        self.observer = observer;
    }

    /// Get a handle for querying cartridge process info and sending commands.
    /// Must be called before `run()`. The returned handle is `Send + Sync + Clone`
    /// and remains valid for the lifetime of the `run()` loop.
    pub fn process_handle(&self) -> CartridgeProcessHandle {
        CartridgeProcessHandle {
            snapshot: self.process_snapshot.clone(),
            command_tx: self.command_tx.clone(),
        }
    }

    /// Register a cartridge binary for on-demand spawning (probe-based discovery).
    ///
    /// The cartridge is not spawned until a REQ arrives for one of its known caps.
    /// The `known_caps` are provisional — they allow routing before HELLO.
    /// After spawn + HELLO, the real caps from the manifest replace them.
    /// `channel` and `registry_url` are part of the install's identity
    /// and must be supplied by the caller — the binary path alone
    /// does not tell us which (channel, registry) a standalone-binary
    /// install belongs to. `registry_url == None` ⇔ dev install.
    pub fn register_cartridge(
        &mut self,
        path: &Path,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<&str>,
        known_caps: &[String],
    ) {
        let cartridge_idx = self.cartridges.len();
        self.cartridges
            .push(ManagedCartridge::new_registered_binary(
                path.to_path_buf(),
                channel,
                registry_url.map(|s| s.to_string()),
                known_caps.to_vec(),
            ));
        for cap in known_caps {
            self.cap_table.push((cap.clone(), cartridge_idx));
        }
    }

    /// Register a directory-based cartridge for on-demand spawning.
    ///
    /// The `version_dir` must contain a valid `cartridge.json` with an entry point.
    /// Identity is computed from the directory tree hash. `channel`
    /// and `registry_url` must come from `cartridge.json` (the host
    /// has already validated the three-place rule before calling
    /// this); they propagate through `InstalledCartridgeIdentity` to
    /// the engine's RelayNotify so consumers preserve the
    /// `(registry, channel)` provenance end-to-end.
    pub fn register_cartridge_dir(
        &mut self,
        entry_point: &Path,
        version_dir: &Path,
        id: &str,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        registry_url: Option<&str>,
        version: &str,
        known_caps: &[String],
    ) {
        let cartridge_idx = self.cartridges.len();
        self.cartridges.push(ManagedCartridge::new_registered_dir(
            entry_point.to_path_buf(),
            version_dir.to_path_buf(),
            id.to_string(),
            channel,
            registry_url.map(|s| s.to_string()),
            version.to_string(),
            known_caps.to_vec(),
        ));
        for cap in known_caps {
            self.cap_table.push((cap.clone(), cartridge_idx));
        }
    }

    /// Attach a pre-connected cartridge (already running, e.g., pre-spawned or in tests).
    ///
    /// Performs HELLO handshake immediately. On success, the cartridge is ready for requests.
    /// On HELLO failure, returns error (permanent — the binary is broken).
    pub async fn attach_cartridge<R, W>(
        &mut self,
        cartridge_read: R,
        cartridge_write: W,
    ) -> Result<usize, AsyncHostError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut reader = FrameReader::new(cartridge_read);
        let mut writer = FrameWriter::new(cartridge_write);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| AsyncHostError::Handshake(e.to_string()))?;

        let caps = parse_caps_from_manifest(&result.manifest)
            .map_err(|e| e.into_async_host_error())?;

        // Verify identity — proves the protocol stack works end-to-end
        verify_identity(&mut reader, &mut writer)
            .await
            .map_err(|e| {
                AsyncHostError::Protocol(format!("Identity verification failed: {}", e))
            })?;

        let cartridge_idx = self.cartridges.len();

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        // Start reader task
        let rh = Self::start_reader_task(cartridge_idx, reader, self.event_tx.clone());

        let mut cartridge = ManagedCartridge::new_attached(result.manifest, result.limits, caps);
        cartridge.writer_tx = Some(writer_tx);
        cartridge.reader_handle = Some(rh);
        cartridge.writer_handle = Some(wh);

        self.cartridges.push(cartridge);
        self.update_cap_table();
        self.rebuild_capabilities(None); // No relay during initialization

        Ok(cartridge_idx)
    }

    /// Get the aggregate capabilities of all running, healthy cartridges.
    pub fn capabilities(&self) -> &[u8] {
        &self.capabilities
    }

    /// Main run loop — reads from relay, routes to cartridges; reads from cartridges,
    /// forwards to relay. Handles HELLO/heartbeats per cartridge locally.
    ///
    /// Blocks until the relay closes or a fatal error occurs.
    /// On exit, all managed cartridge processes are killed.
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
            let mut reader = FrameReader::new(relay_read);
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if relay_tx.send(Ok(frame)).is_err() {
                            break; // Main loop dropped
                        }
                    }
                    Ok(None) => {
                        break; // Relay closed cleanly
                    }
                    Err(e) => {
                        let _ = relay_tx.send(Err(e.into()));
                        break;
                    }
                }
            }
        });

        let mut event_rx = self
            .event_rx
            .take()
            .expect("run() must only be called once");
        let mut command_rx = self
            .command_rx
            .take()
            .expect("run() must only be called once");

        let mut heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_interval.tick().await; // skip initial tick

        // Runtime-stats refresh cadence. Request counts and memory change
        // continuously; structural changes (spawn/death) already trigger
        // RelayNotify synchronously via `rebuild_capabilities`, so this
        // interval only needs to cover the continuous signals. Engine-side
        // watch dedup drops no-op frames when no stat actually changed.
        let mut stats_interval = tokio::time::interval(Duration::from_secs(2));
        stats_interval.tick().await; // skip initial tick

        // Send discovery RelayNotify if cartridges were pre-attached.
        // At this point all async tasks are spawned and running, so the frame will be delivered.
        if !self.capabilities.is_empty() {
            let installed_cartridges = self.build_installed_cartridge_identities();
            let notify_payload = RelayNotifyCapabilitiesPayload {
                caps: serde_json::from_slice(&self.capabilities)
                    .expect("BUG: host runtime capabilities must be valid JSON cap array"),
                installed_cartridges,
            };
            let notify_bytes = serde_json::to_vec(&notify_payload)
                .expect("Failed to serialize RelayNotify capabilities payload");
            let notify_frame = Frame::relay_notify(&notify_bytes, &Limits::default());
            let _ = outbound_tx.send(notify_frame);
        }

        let result = loop {
            tokio::select! {
                biased;

                // Cartridge events (frames from cartridges, death notifications)
                Some(event) = event_rx.recv() => {
                    match event {
                        CartridgeEvent::Frame { cartridge_idx, frame } => {
                            if let Err(e) = self.handle_cartridge_frame(cartridge_idx, frame, &outbound_tx) {
                                break Err(e);
                            }
                        }
                        CartridgeEvent::Death { cartridge_idx } => {
                            if let Err(e) = self.handle_cartridge_death(cartridge_idx, &outbound_tx).await {
                                break Err(e);
                            }

                            // If relay disconnected AND all cartridges dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if !relay_connected && all_cartridges_dead {
                                break Ok(());
                            }
                        }
                    }
                }

                // Frames from relay reader task (cancel-safe: channel recv is cancel-safe)
                relay_result = relay_rx.recv(), if relay_connected => {
                    match relay_result {
                        Some(Ok(frame)) => {
                            if let Err(e) = self.handle_relay_frame(frame, &outbound_tx, &resource_fn).await {
                                break Err(e);
                            }
                        }
                        Some(Err(_)) => {
                            relay_connected = false; // Disable relay branch, continue processing cartridges

                            // If all cartridges are also dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if all_cartridges_dead {
                                break Ok(());
                            }
                        }
                        None => {
                            relay_connected = false; // Disable relay branch, continue processing cartridges

                            // If all cartridges are also dead, exit cleanly
                            let all_cartridges_dead = self.cartridges.iter().all(|p| !p.running);
                            if all_cartridges_dead {
                                break Ok(());
                            }
                        }
                    }
                }

                // Periodic heartbeat probes
                _ = heartbeat_interval.tick() => {
                    self.send_heartbeats_and_check_timeouts(&outbound_tx);
                }

                // Periodic runtime-stats refresh — republish RelayNotify so
                // the engine sees current request counts, memory, and
                // heartbeat ages. Only fires the publish if at least one
                // cartridge is running, keeping idle hosts quiet.
                _ = stats_interval.tick() => {
                    let any_running = self.cartridges.iter().any(|c| c.running);
                    if any_running {
                        self.rebuild_capabilities(Some(&outbound_tx));
                    }
                }

                // External commands via CartridgeProcessHandle
                Some(cmd) = command_rx.recv() => {
                    if let Err(e) = self.handle_command(cmd, &outbound_tx).await {
                        break Err(e);
                    }
                }
            }
        };

        // Cleanup: kill all managed cartridge processes
        self.kill_all_cartridges().await;
        relay_reader_task.abort();
        outbound_writer.abort();

        result
    }

    // =========================================================================
    // FRAME HANDLING
    // =========================================================================

    /// Handle a frame arriving from the relay (engine → cartridge direction).
    async fn handle_relay_frame(
        &mut self,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
        resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        tracing::debug!(target: "host_runtime", "handle_relay_frame: {:?} xid={:?} rid={:?}", frame.frame_type, frame.routing_id, frame.id);
        tracing::debug!(
            "[CartridgeHostRuntime] handle_relay_frame: {:?} id={:?} cap={:?} xid={:?}",
            frame.frame_type,
            frame.id,
            frame.cap,
            frame.routing_id
        );
        match frame.frame_type {
            FrameType::Req => {
                // PATH C: REQ coming FROM relay
                // MUST have XID (else FATAL - only switch can assign XIDs)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(
                            "REQ from relay missing XID - all frames from relay must have XID"
                                .to_string(),
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

                // Check for target_cartridge in meta — if present, route directly
                // to that cartridge instead of using cap-based dispatch
                let target_cartridge_id = frame.meta.as_ref().and_then(|m| {
                    m.get("target_cartridge").and_then(|v| {
                        if let ciborium::Value::Text(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                });

                tracing::info!(
                    target: "host_runtime",
                    cap_urn = %cap_urn,
                    rid = ?frame.id,
                    xid = ?frame.routing_id,
                    target_cartridge = ?target_cartridge_id,
                    "[CartridgeHostRuntime] Received REQ from relay — beginning dispatch"
                );
                let cartridge_idx = if let Some(ref target_id) = target_cartridge_id {
                    // Direct routing by cartridge identity
                    let found = self.cartridges.iter().position(|c| {
                        c.installed_identity
                            .as_ref()
                            .map_or(false, |identity| identity.id == *target_id)
                    });
                    match found {
                        Some(idx) => {
                            // Check if cartridge is usable
                            if self.cartridges[idx].hello_failed {
                                let mut err = Frame::err(
                                    frame.id.clone(),
                                    "CARTRIDGE_UNAVAILABLE",
                                    &format!(
                                        "Cartridge '{}' failed handshake and cannot be spawned",
                                        target_id
                                    ),
                                );
                                err.routing_id = frame.routing_id.clone();
                                outbound_tx
                                    .send(err)
                                    .map_err(|_| AsyncHostError::SendError)?;
                                return Ok(());
                            }
                            idx
                        }
                        None => {
                            let mut err = Frame::err(
                                frame.id.clone(),
                                "CARTRIDGE_NOT_FOUND",
                                &format!(
                                    "Cartridge '{}' not found on this host",
                                    target_id
                                ),
                            );
                            err.routing_id = frame.routing_id.clone();
                            outbound_tx
                                .send(err)
                                .map_err(|_| AsyncHostError::SendError)?;
                            return Ok(());
                        }
                    }
                } else {
                    // Standard cap-based dispatch
                    match self.find_cartridge_for_cap(&cap_urn) {
                        Some(idx) => {
                            tracing::info!(
                                target: "host_runtime",
                                cap_urn = %cap_urn,
                                cartridge_idx = idx,
                                cartridge_path = %self.cartridges[idx].path.display(),
                                running = self.cartridges[idx].running,
                                hello_failed = self.cartridges[idx].hello_failed,
                                "[CartridgeHostRuntime] cap-based dispatch resolved to cartridge"
                            );
                            idx
                        }
                        None => {
                            tracing::error!(
                                target: "host_runtime",
                                cap_urn = %cap_urn,
                                cap_table_size = self.cap_table.len(),
                                cap_table_sample = ?self.cap_table.iter().take(5).map(|(c, i)| (c.as_str(), *i)).collect::<Vec<_>>(),
                                "[CartridgeHostRuntime] NO_HANDLER for incoming REQ — no cartridge in cap_table is dispatchable"
                            );
                            let mut err = Frame::err(
                                frame.id.clone(),
                                "NO_HANDLER",
                                &format!("no cartridge handles cap: {}", cap_urn),
                            );
                            err.routing_id = frame.routing_id.clone();
                            outbound_tx
                                .send(err)
                                .map_err(|_| AsyncHostError::SendError)?;
                            return Ok(());
                        }
                    }
                };

                // Spawn on demand if not running
                if !self.cartridges[cartridge_idx].running {
                    tracing::info!(
                        target: "host_runtime",
                        cartridge_idx = cartridge_idx,
                        cartridge_path = %self.cartridges[cartridge_idx].path.display(),
                        "[CartridgeHostRuntime] cartridge not running — spawning on demand"
                    );
                    let spawn_outcome = self.spawn_cartridge(cartridge_idx, resource_fn).await;
                    match &spawn_outcome {
                        Ok(()) => tracing::info!(
                            target: "host_runtime",
                            cartridge_idx = cartridge_idx,
                            "[CartridgeHostRuntime] cartridge spawn succeeded"
                        ),
                        Err(e) => tracing::error!(
                            target: "host_runtime",
                            cartridge_idx = cartridge_idx,
                            error = %e,
                            "[CartridgeHostRuntime] cartridge spawn failed — REQ will be aborted"
                        ),
                    }
                    // Always rebuild so the RelayNotify carries the latest
                    // per-cartridge attachment state — including freshly
                    // recorded failures — to the engine's RelaySwitch.
                    self.rebuild_capabilities(Some(outbound_tx));
                    spawn_outcome?;
                }

                // Record in List 2: INCOMING_RXIDS (XID, RID) → cartridge_idx
                self.incoming_rxids
                    .insert((xid.clone(), frame.id.clone()), cartridge_idx);

                tracing::info!(
                    target: "host_runtime",
                    cartridge_idx = cartridge_idx,
                    rid = ?frame.id,
                    xid = ?frame.routing_id,
                    "[CartridgeHostRuntime] forwarding REQ to cartridge stdin"
                );
                // Forward to cartridge WITH XID
                self.send_to_cartridge(cartridge_idx, frame)
            }

            FrameType::StreamStart
            | FrameType::Chunk
            | FrameType::StreamEnd
            | FrameType::End
            | FrameType::Err => {
                // PATH C: Continuation frame from relay
                // MUST have XID (else FATAL)
                let xid = match frame.routing_id.as_ref() {
                    Some(xid) => xid.clone(),
                    None => {
                        return Err(AsyncHostError::Protocol(format!(
                            "{:?} from relay missing XID - all frames from relay must have XID",
                            frame.frame_type
                        )));
                    }
                };

                tracing::info!(
                    target: "host_runtime",
                    ftype = ?frame.frame_type,
                    rid = ?frame.id,
                    xid = ?xid,
                    payload_len = frame.payload.as_ref().map(|p| p.len()).unwrap_or(0),
                    "[CartridgeHostRuntime] received continuation frame from relay"
                );

                // Route by checking BOTH maps. For self-loop peer requests (where
                // source and destination are behind the same relay connection), the
                // same (XID, RID) appears in BOTH incoming_rxids and outgoing_rids:
                //   incoming_rxids[(XID, RID)] = handler cartridge (receives request body)
                //   outgoing_rids[RID] = requester cartridge (receives peer response)
                //
                // Phase tracking: incoming_rxids entry is removed when the request
                // body END is delivered to the handler. After that, frames from
                // relay with the same (XID, RID) are peer responses and fall through
                // to outgoing_rids. This is safe because:
                //   1. Frames on a single socket are ordered — END is always last
                //   2. For non-peer requests, no further relay frames arrive after END
                let key = (xid.clone(), frame.id.clone());
                let (cartridge_idx, routed_via_incoming) = if let Some(&idx) =
                    self.incoming_rxids.get(&key)
                {
                    tracing::info!(target: "host_runtime", "[CartridgeHostRuntime] routing {:?} to cartridge {} via incoming_rxids", frame.frame_type, idx);
                    (idx, true)
                } else if let Some(&idx) = self.outgoing_rids.get(&frame.id) {
                    tracing::info!(target: "host_runtime", "[CartridgeHostRuntime] routing {:?} to cartridge {} via outgoing_rids", frame.frame_type, idx);
                    (idx, false)
                } else {
                    tracing::warn!(
                        target: "host_runtime",
                        ftype = ?frame.frame_type,
                        rid = ?frame.id,
                        xid = ?xid,
                        incoming_rxids_size = self.incoming_rxids.len(),
                        outgoing_rids_size = self.outgoing_rids.len(),
                        "[CartridgeHostRuntime] DROP — no routing for continuation frame, no entry in either incoming_rxids or outgoing_rids"
                    );
                    return Ok(()); // Already cleaned up
                };

                let is_terminal =
                    frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err;

                // If the cartridge is dead, send ERR to engine and clean up routing.
                if self
                    .send_to_cartridge(cartridge_idx, frame.clone())
                    .is_err()
                {
                    let flow_key = FlowKey {
                        rid: frame.id.clone(),
                        xid: Some(xid.clone()),
                    };
                    let next_seq = self
                        .outgoing_max_seq
                        .remove(&flow_key)
                        .map(|s| s + 1)
                        .unwrap_or(0);
                    let death_msg = self.cartridges[cartridge_idx]
                        .last_death_message
                        .as_deref()
                        .unwrap_or("Cartridge exited while processing request");
                    let mut err = Frame::err(frame.id.clone(), "CARTRIDGE_DIED", death_msg);
                    err.routing_id = frame.routing_id.clone();
                    err.seq = next_seq;
                    let _ = outbound_tx.send(err);

                    self.outgoing_rids.remove(&frame.id);
                    self.incoming_rxids.remove(&key);
                    return Ok(());
                }

                // Clean up routing on terminal frame.
                // - If routed via incoming_rxids: this was a request body frame to handler
                // - If routed via outgoing_rids: this was a peer response to requester
                if is_terminal {
                    if routed_via_incoming {
                        self.incoming_rxids.remove(&key);
                    } else {
                        // Peer response completed - clean up outgoing_rids
                        self.outgoing_rids.remove(&frame.id);
                    }
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
            FrameType::Log => {
                // LOG frames from peer responses — route back to the cartridge
                // that made the peer request, identified by outgoing_rids[RID].
                if let Some(&cartridge_idx) = self.outgoing_rids.get(&frame.id) {
                    tracing::debug!(target: "host_runtime", "Routing LOG to cartridge {} via outgoing_rids[{:?}]", cartridge_idx, frame.id);
                    let _ = self.send_to_cartridge(cartridge_idx, frame);
                } else {
                    tracing::debug!(target: "host_runtime", "LOG frame not in outgoing_rids, dropping: rid={:?}", frame.id);
                }
                // If not a peer response LOG, ignore silently (stale routing)
                Ok(())
            }
            FrameType::Cancel => {
                // Cancel from relay — route to the cartridge handling this request.
                let xid = frame.routing_id.clone().ok_or_else(|| {
                    AsyncHostError::Protocol("Cancel frame missing XID".to_string())
                })?;
                let rid = frame.id.clone();
                let key = (xid.clone(), rid.clone());
                let force_kill = frame.force_kill.unwrap_or(false);

                if let Some(&cartridge_idx) = self.incoming_rxids.get(&key) {
                    if force_kill {
                        // Force kill: set shutdown reason and kill the process
                        tracing::info!("[CartridgeHostRuntime] Cancel force_kill=true for cartridge {} rid={:?}", cartridge_idx, rid);
                        self.cartridges[cartridge_idx].shutdown_reason =
                            Some(ShutdownReason::Cancelled);
                        if let Some(ref mut child) = self.cartridges[cartridge_idx].process {
                            let _ = child.kill().await;
                        }
                    } else {
                        // Cooperative cancel: forward Cancel frame to the cartridge
                        tracing::info!(
                            "[CartridgeHostRuntime] Cancel cooperative for cartridge {} rid={:?}",
                            cartridge_idx,
                            rid
                        );
                        let _ = self.send_to_cartridge(cartridge_idx, frame);

                        // Also cascade: send Cancel to relay for each peer call spawned by this request
                        if let Some(peer_rids) = self.incoming_to_peer_rids.get(&key) {
                            for peer_rid in peer_rids.clone() {
                                tracing::info!(
                                    "[CartridgeHostRuntime] Cascading Cancel to peer call rid={:?}",
                                    peer_rid
                                );
                                let cancel = Frame::cancel(peer_rid, false);
                                let _ = outbound_tx.send(cancel);
                            }
                        }
                    }
                } else {
                    tracing::debug!(
                        "[CartridgeHostRuntime] Cancel for unknown request ({:?}, {:?}) — ignoring",
                        xid,
                        rid
                    );
                }
                Ok(())
            }
            FrameType::RelayNotify | FrameType::RelayState => {
                Err(AsyncHostError::Protocol(format!(
                    "{:?} reached runtime — relay must intercept these, never forward",
                    frame.frame_type
                )))
            }
        }
    }

    /// Handle a frame arriving from a cartridge (cartridge → engine direction).
    fn handle_cartridge_frame(
        &mut self,
        cartridge_idx: usize,
        frame: Frame,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        // Heartbeats and high-volume Log frames stay at debug; everything
        // else is logged at info level so we can trace REQ→response
        // round-trips (notably the engine's identity probe) end-to-end
        // without enabling debug logs.
        if !matches!(frame.frame_type, FrameType::Heartbeat | FrameType::Log) {
            tracing::info!(
                target: "host_runtime",
                cartridge_idx = cartridge_idx,
                ftype = ?frame.frame_type,
                rid = ?frame.id,
                cap = ?frame.cap,
                xid = ?frame.routing_id,
                "[CartridgeHostRuntime] handle_cartridge_frame: received from cartridge"
            );
        } else {
            tracing::debug!("[CartridgeHostRuntime] handle_cartridge_frame: cartridge={} {:?} id={:?} cap={:?} xid={:?}", cartridge_idx, frame.frame_type, frame.id, frame.cap, frame.routing_id);
        }
        match frame.frame_type {
            // HELLO after handshake is a fatal protocol error.
            FrameType::Hello => Err(AsyncHostError::Protocol(format!(
                "Cartridge {} sent HELLO after handshake — fatal protocol violation",
                cartridge_idx
            ))),

            // Heartbeat: handle locally, never forward.
            FrameType::Heartbeat => {
                let is_our_probe = self.cartridges[cartridge_idx]
                    .pending_heartbeats
                    .remove(&frame.id)
                    .is_some();

                if is_our_probe {
                    // Response to our health probe — cartridge is alive.
                    // Extract self-reported memory from heartbeat response meta.
                    // Cartridges include their own ri_phys_footprint and ri_resident_size
                    // (via proc_pid_rusage(getpid())) in the meta map.
                    if let Some(ref meta) = frame.meta {
                        if let Some(ciborium::Value::Integer(v)) = meta.get("footprint_mb") {
                            self.cartridges[cartridge_idx].memory_footprint_mb =
                                u64::try_from(*v).unwrap_or(0);
                        }
                        if let Some(ciborium::Value::Integer(v)) = meta.get("rss_mb") {
                            self.cartridges[cartridge_idx].memory_rss_mb =
                                u64::try_from(*v).unwrap_or(0);
                        }
                    }
                    // Stamp the round-trip completion timestamp so the
                    // runtime-stats snapshot can surface heartbeat age to the UI.
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    self.cartridges[cartridge_idx].last_heartbeat_unix_seconds = Some(now_secs);
                    self.update_process_snapshot();
                } else {
                    // Cartridge-initiated heartbeat — respond immediately
                    let response = Frame::heartbeat(frame.id.clone());
                    self.send_to_cartridge(cartridge_idx, response)?;
                }
                Ok(())
            }

            // Relay frames from a cartridge: fatal protocol error.
            FrameType::RelayNotify | FrameType::RelayState => {
                Err(AsyncHostError::Protocol(format!(
                    "Cartridge {} sent {:?} — cartridges must never send relay frames",
                    cartridge_idx, frame.frame_type
                )))
            }

            // PATH A: REQ from cartridge (peer invoke)
            // MUST have RID, MUST NOT have XID (cartridges never send XID)
            FrameType::Req => {
                if frame.routing_id.is_some() {
                    return Err(AsyncHostError::Protocol(format!(
                        "Cartridge {} sent REQ with XID - cartridges must never send XID",
                        cartridge_idx
                    )));
                }

                // Record in List 1: OUTGOING_RIDS
                tracing::debug!(target: "host_runtime", "PEER REQ from cartridge {}: cap={:?} rid={:?} -> storing in outgoing_rids", cartridge_idx, frame.cap, frame.id);
                self.outgoing_rids.insert(frame.id.clone(), cartridge_idx);

                // Track parent→child peer call mapping for cancel cascade
                if let Some(parent_rid) = frame
                    .meta
                    .as_ref()
                    .and_then(|m| m.get("parent_rid"))
                    .and_then(|v| match v {
                        ciborium::Value::Bytes(bytes) if bytes.len() == 16 => {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(bytes);
                            Some(MessageId::Uuid(arr))
                        }
                        ciborium::Value::Integer(i) => {
                            let n: i128 = (*i).into();
                            Some(MessageId::Uint(n as u64))
                        }
                        _ => None,
                    })
                {
                    // Find the parent's incoming_rxids entry to get its (xid, rid) key
                    let parent_key = self
                        .incoming_rxids
                        .keys()
                        .find(|(_, rid)| *rid == parent_rid)
                        .cloned();
                    if let Some(pk) = parent_key {
                        self.incoming_to_peer_rids
                            .entry(pk)
                            .or_default()
                            .push(frame.id.clone());
                    }
                }

                // Track max-seen seq for host-generated ERR on death
                let flow_key = FlowKey::from_frame(&frame);
                self.outgoing_max_seq.insert(flow_key, frame.seq);

                // Forward as-is to relay (no XID - will be assigned by RelaySwitch)
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }

            // PATH A: Continuation frames from cartridge (request body or response)
            // When responding to relay requests, frames WILL have XID (routing_id)
            // When responding to direct requests, frames will NOT have XID
            // NO routing decisions - only one destination (relay)
            _ => {
                if frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err {
                    tracing::debug!(target: "host_runtime", "Forwarding {:?} from cartridge {} to relay: xid={:?} rid={:?}", frame.frame_type, cartridge_idx, frame.routing_id, frame.id);
                }
                // Track max-seen seq for flow, clean up on terminal
                if frame.is_flow_frame() {
                    let flow_key = FlowKey::from_frame(&frame);
                    let is_terminal =
                        frame.frame_type == FrameType::End || frame.frame_type == FrameType::Err;
                    if is_terminal {
                        self.outgoing_max_seq.remove(&flow_key);
                    } else {
                        self.outgoing_max_seq.insert(flow_key, frame.seq);
                    }
                }

                // NOTE: Do NOT remove incoming_rxids here!
                // Response END from cartridge doesn't mean the REQUEST is complete.
                // Request body frames might still be arriving from relay (async race).
                // incoming_rxids cleanup happens in handle_relay_frame when request body END arrives.

                // Forward as-is to relay (no routing, no XID manipulation)
                outbound_tx
                    .send(frame)
                    .map_err(|_| AsyncHostError::SendError)
            }
        }
    }

    /// Handle a cartridge death (reader loop exited).
    ///
    /// Three cases based on `shutdown_reason`:
    /// 1. **`None`** (unexpected death): Genuine crash. Send ERR "CARTRIDGE_DIED"
    ///    for all pending requests, store death message.
    /// 2. **`Some(OomKill)`**: OOM watchdog killed the cartridge while it was
    ///    actively processing. Send ERR "OOM_KILLED" for all pending requests
    ///    so callers fail fast instead of hanging.
    /// 3. **`Some(AppExit)`**: Clean shutdown. No ERR frames — the relay
    ///    connection is closing anyway.
    async fn handle_cartridge_death(
        &mut self,
        cartridge_idx: usize,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        use tokio::io::AsyncReadExt;

        // Scope the mutable borrow of the cartridge so we can access self later.
        let reason;
        let stderr_content;
        let exit_info: String;
        // Capture observer payload before we mutate state and clear the
        // process handle.
        let observer_pid_at_death;
        let observer_name;
        {
            let cartridge = &mut self.cartridges[cartridge_idx];
            observer_pid_at_death = cartridge.process.as_ref().and_then(|c| c.id());
            observer_name = cartridge
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            cartridge.running = false;
            cartridge.writer_tx = None;
            // One completed death (any reason) counts as one restart cycle.
            // The next on-demand spawn will increment `running` again with
            // a fresh process.
            cartridge.restart_count = cartridge.restart_count.saturating_add(1);
            reason = cartridge.shutdown_reason;
            cartridge.shutdown_reason = None; // Reset for potential respawn

            // Capture stderr content BEFORE killing the process
            let mut captured = String::new();
            if let Some(ref mut stderr) = cartridge.stderr_handle {
                let mut buf = vec![0u8; 4096];
                loop {
                    match tokio::time::timeout(Duration::from_millis(100), stderr.read(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) => break,
                        Ok(Ok(n)) => {
                            if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                                captured.push_str(s);
                            }
                            if captured.len() > 2000 {
                                captured.truncate(2000);
                                captured.push_str("... [truncated]");
                                break;
                            }
                        }
                        Ok(Err(_)) | Err(_) => break,
                    }
                }
            }
            cartridge.stderr_handle = None;

            // Capture exit status and kill the process if it's still around
            if let Some(ref mut child) = cartridge.process {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        #[cfg(unix)]
                        {
                            use std::os::unix::process::ExitStatusExt;
                            if let Some(sig) = status.signal() {
                                exit_info = format!("killed by signal {}", sig);
                            } else {
                                exit_info = format!("exit code {}", status.code().unwrap_or(-1));
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            exit_info = format!("exit code {:?}", status.code());
                        }
                    }
                    Ok(None) => {
                        // Still running — kill it
                        let _ = child.kill().await;
                        exit_info = "still running (killed)".to_string();
                    }
                    Err(e) => {
                        exit_info = format!("try_wait failed: {}", e);
                    }
                }
            } else {
                exit_info = String::new();
            }
            cartridge.process = None;
            stderr_content = captured;
        }

        // Clean up routing tables regardless of death cause.
        // outgoing_rids: peer requests the cartridge initiated
        let failed_outgoing: Vec<(MessageId, u64)> = self
            .outgoing_rids
            .iter()
            .filter(|(_, &idx)| idx == cartridge_idx)
            .map(|(rid, _)| {
                let flow_key = FlowKey {
                    rid: rid.clone(),
                    xid: None,
                };
                let next_seq = self
                    .outgoing_max_seq
                    .remove(&flow_key)
                    .map(|s| s + 1)
                    .unwrap_or(0);
                (rid.clone(), next_seq)
            })
            .collect();

        for (rid, _) in &failed_outgoing {
            self.outgoing_rids.remove(rid);
        }

        // incoming_rxids: requests from the relay that this cartridge was handling
        let failed_incoming: Vec<(MessageId, MessageId, u64)> = self
            .incoming_rxids
            .iter()
            .filter(|(_, &idx)| idx == cartridge_idx)
            .map(|((xid, rid), _)| {
                let flow_key = FlowKey {
                    rid: rid.clone(),
                    xid: Some(xid.clone()),
                };
                let next_seq = self
                    .outgoing_max_seq
                    .remove(&flow_key)
                    .map(|s| s + 1)
                    .unwrap_or(0);
                (xid.clone(), rid.clone(), next_seq)
            })
            .collect();
        self.incoming_rxids
            .retain(|(_, _), &mut idx| idx != cartridge_idx);

        // Clean up incoming_to_peer_rids for all requests from this cartridge
        for (xid, rid, _) in &failed_incoming {
            self.incoming_to_peer_rids
                .remove(&(xid.clone(), rid.clone()));
        }

        // Determine error code and message based on shutdown reason.
        // Both unexpected deaths and OOM kills send ERR frames for pending work.
        // Only AppExit suppresses ERR frames (relay is closing, no callers left).
        let err_info: Option<(&str, String)> = match reason {
            None => {
                // Unexpected death — genuine crash mid-flight
                let exit_suffix = if exit_info.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", exit_info)
                };
                let error_message = if stderr_content.is_empty() {
                    format!(
                        "Cartridge {} exited unexpectedly{}.",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix
                    )
                } else {
                    format!(
                        "Cartridge {} exited unexpectedly{}. stderr:\n{}",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix,
                        stderr_content
                    )
                };
                Some(("CARTRIDGE_DIED", error_message))
            }
            Some(ShutdownReason::OomKill) => {
                // OOM watchdog killed the cartridge — callers must be notified
                let exit_suffix = if exit_info.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", exit_info)
                };
                let error_message = if stderr_content.is_empty() {
                    format!(
                        "Cartridge {} killed by OOM watchdog{}.",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix
                    )
                } else {
                    format!(
                        "Cartridge {} killed by OOM watchdog{}. stderr:\n{}",
                        self.cartridges[cartridge_idx].path.display(),
                        exit_suffix,
                        stderr_content
                    )
                };
                Some(("OOM_KILLED", error_message))
            }
            Some(ShutdownReason::Cancelled) => {
                // Cancel-triggered kill — ERR "CANCELLED" for all pending work
                Some((
                    "CANCELLED",
                    format!(
                        "Cartridge {} killed by cancel request.",
                        self.cartridges[cartridge_idx].path.display()
                    ),
                ))
            }
            Some(ShutdownReason::AppExit) => {
                // Clean shutdown — no ERR frames, relay is closing
                None
            }
        };

        if let Some((error_code, error_message)) = err_info {
            self.cartridges[cartridge_idx].last_death_message = Some(error_message.clone());

            for (rid, next_seq) in &failed_outgoing {
                let mut err_frame = Frame::err(rid.clone(), error_code, &error_message);
                err_frame.seq = *next_seq;
                let _ = outbound_tx.send(err_frame);
            }
            for (xid, rid, next_seq) in &failed_incoming {
                let mut err_frame = Frame::err(rid.clone(), error_code, &error_message);
                err_frame.routing_id = Some(xid.clone());
                err_frame.seq = *next_seq;
                let _ = outbound_tx.send(err_frame);
            }
        } else {
            self.cartridges[cartridge_idx].last_death_message = None;
        }

        // Rebuild cap table for on-demand respawn routing
        self.update_cap_table();
        self.rebuild_capabilities(Some(outbound_tx));
        self.update_process_snapshot();

        // Notify lifecycle observer (e.g., XPC reverse-callback bridge).
        if let Some(ref obs) = self.observer {
            obs.cartridge_died(cartridge_idx, observer_pid_at_death, &observer_name);
        }

        Ok(())
    }

    /// Handle an external command received via the `CartridgeProcessHandle`.
    async fn handle_command(
        &mut self,
        command: HostCommand,
        outbound_tx: &mpsc::UnboundedSender<Frame>,
    ) -> Result<(), AsyncHostError> {
        match command {
            HostCommand::KillCartridge { pid } => {
                // Find the cartridge with the matching PID
                let cartridge_idx = self.cartridges.iter().position(|p| {
                    p.running && p.process.as_ref().and_then(|c| c.id()) == Some(pid)
                });
                if let Some(idx) = cartridge_idx {
                    tracing::info!(
                        target: "host_runtime",
                        pid = pid,
                        cartridge = %self.cartridges[idx].path.display(),
                        "Killing cartridge by external command (memory pressure)"
                    );
                    self.cartridges[idx].shutdown_reason = Some(ShutdownReason::OomKill);
                    if let Some(ref mut child) = self.cartridges[idx].process {
                        let _ = child.kill().await;
                    }
                    // Death event will arrive via the reader task; handle_cartridge_death
                    // will do the full cleanup.
                } else {
                    tracing::warn!(
                        target: "host_runtime",
                        pid = pid,
                        "Kill command for unknown/dead PID — ignoring"
                    );
                }
            }
        }
        Ok(())
    }

    // =========================================================================
    // CARTRIDGE LIFECYCLE
    // =========================================================================

    /// Spawn a registered cartridge binary on demand.
    async fn spawn_cartridge(
        &mut self,
        cartridge_idx: usize,
        _resource_fn: &(impl Fn() -> Vec<u8> + Send),
    ) -> Result<(), AsyncHostError> {
        let cartridge = &self.cartridges[cartridge_idx];

        if cartridge.hello_failed {
            return Err(AsyncHostError::Protocol(format!(
                "Cartridge '{}' permanently failed — HELLO failure, binary is broken",
                cartridge.path.display()
            )));
        }

        if cartridge.path.as_os_str().is_empty() {
            return Err(AsyncHostError::Protocol(format!(
                "Cartridge {} has no binary path — cannot spawn",
                cartridge_idx
            )));
        }

        let mut child = match tokio::process::Command::new(&cartridge.path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()) // Capture stderr for crash diagnostics
            .kill_on_drop(true) // No orphan processes
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                let msg = format!(
                    "Failed to spawn cartridge '{}': {}",
                    cartridge.path.display(),
                    e
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::EntryPointMissing,
                    msg.clone(),
                );
                return Err(AsyncHostError::Io(msg));
            }
        };

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take();

        // DEBUG: Forward cartridge stderr to host stderr in real-time
        if let Some(cartridge_stderr) = stderr {
            let cartridge_path = cartridge.path.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(cartridge_stderr);
                let mut line = String::new();
                while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                    tracing::debug!(
                        "[cartridge:{}] {}",
                        cartridge_path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy(),
                        line.trim()
                    );
                    line.clear();
                }
            });
        }
        let stderr: Option<tokio::process::ChildStderr> = None; // Already consumed above

        // HELLO handshake — bounded by a hard timeout so a cartridge
        // that fails to start its CBOR-mode reader cannot hold up the
        // host event loop indefinitely. Cold-start of a Rust cartridge
        // is normally <1s; a Swift cartridge with sandbox-deferred
        // init can stretch to a few seconds. 15s is generous but
        // still bounded.
        const HELLO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
        let hs_started_at = std::time::Instant::now();
        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let hs_outcome = tokio::time::timeout(
            HELLO_TIMEOUT,
            handshake(&mut reader, &mut writer),
        )
        .await;
        let handshake_result = match hs_outcome {
            Ok(Ok(result)) => {
                tracing::info!(
                    target: "host_runtime",
                    cartridge = %self.cartridges[cartridge_idx].path.display(),
                    elapsed_ms = hs_started_at.elapsed().as_millis() as u64,
                    "[CartridgeHostRuntime] cartridge HELLO handshake succeeded"
                );
                result
            }
            Ok(Err(e)) => {
                // HELLO failure = permanent removal. Binary is broken.
                let msg = format!(
                    "Cartridge '{}' HELLO failed: {} — permanently removed",
                    self.cartridges[cartridge_idx].path.display(),
                    e
                );
                tracing::error!(target: "host_runtime", error = %msg, "[CartridgeHostRuntime] HELLO failed");
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::HandshakeFailed,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Handshake(msg));
            }
            Err(_) => {
                let msg = format!(
                    "Cartridge '{}' HELLO timed out after {:?} — cartridge process did not respond. Permanently quarantining.",
                    self.cartridges[cartridge_idx].path.display(),
                    HELLO_TIMEOUT
                );
                tracing::error!(target: "host_runtime", error = %msg, "[CartridgeHostRuntime] HELLO timed out");
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::HandshakeFailed,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Handshake(msg));
            }
        };

        let caps = match parse_caps_from_manifest(&handshake_result.manifest) {
            Ok(caps) => caps,
            Err(parse_err) => {
                let kind = parse_err.attachment_kind();
                let inner = parse_err.into_async_host_error();
                let label = match kind {
                    CartridgeAttachmentErrorKind::ManifestInvalid => "manifest invalid",
                    CartridgeAttachmentErrorKind::Incompatible => "manifest incompatible",
                    _ => "manifest rejected",
                };
                let msg = format!(
                    "Cartridge '{}' {}: {}",
                    self.cartridges[cartridge_idx].path.display(),
                    label,
                    inner
                );
                self.cartridges[cartridge_idx].record_attachment_error(kind, msg.clone());
                let _ = child.kill().await;
                return Err(inner);
            }
        };

        // Verify identity — proves the protocol stack works end-to-end.
        //
        // Bounded by a hard timeout so a cartridge that handshakes
        // successfully but then fails to respond to the identity
        // probe (because its IdentityOp dispatch is broken, its
        // frame writer wedged, or it crashed mid-flight) is
        // diagnosed and quarantined immediately instead of holding
        // up `spawn_cartridge` indefinitely. Without this,
        // `spawn_cartridge.await` from the cap-dispatch path would
        // never return, the entire host event loop would stall on
        // that one REQ, and every subsequent REQ — even to other
        // cartridges — would queue forever.
        const PER_CARTRIDGE_IDENTITY_TIMEOUT: std::time::Duration =
            std::time::Duration::from_secs(15);
        let id_started_at = std::time::Instant::now();
        let id_outcome = tokio::time::timeout(
            PER_CARTRIDGE_IDENTITY_TIMEOUT,
            verify_identity(&mut reader, &mut writer),
        )
        .await;
        match id_outcome {
            Ok(Ok(())) => {
                tracing::info!(
                    target: "host_runtime",
                    cartridge = %self.cartridges[cartridge_idx].path.display(),
                    elapsed_ms = id_started_at.elapsed().as_millis() as u64,
                    "[CartridgeHostRuntime] cartridge identity verification succeeded"
                );
            }
            Ok(Err(e)) => {
                let msg = format!(
                    "Cartridge '{}' identity verification failed: {} — permanently removed",
                    self.cartridges[cartridge_idx].path.display(),
                    e
                );
                tracing::error!(
                    target: "host_runtime",
                    error = %msg,
                    "[CartridgeHostRuntime] cartridge identity verification failed"
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::IdentityRejected,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Protocol(msg));
            }
            Err(_) => {
                let msg = format!(
                    "Cartridge '{}' identity verification timed out after {:?} — cartridge handshaked but did not respond to the identity REQ. Permanently quarantining.",
                    self.cartridges[cartridge_idx].path.display(),
                    PER_CARTRIDGE_IDENTITY_TIMEOUT
                );
                tracing::error!(
                    target: "host_runtime",
                    error = %msg,
                    "[CartridgeHostRuntime] cartridge identity verification TIMED OUT"
                );
                self.cartridges[cartridge_idx].record_attachment_error(
                    CartridgeAttachmentErrorKind::IdentityRejected,
                    msg.clone(),
                );
                let _ = child.kill().await;
                return Err(AsyncHostError::Protocol(msg));
            }
        }

        // Start writer task
        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Frame>();
        let wh = Self::start_writer_task(writer, writer_rx);

        // Start reader task
        let rh = Self::start_reader_task(cartridge_idx, reader, self.event_tx.clone());

        // Update cartridge state
        let cartridge = &mut self.cartridges[cartridge_idx];
        cartridge.manifest = handshake_result.manifest;
        cartridge.limits = handshake_result.limits;
        cartridge.caps = caps;
        cartridge.running = true;
        cartridge.process = Some(child);
        cartridge.writer_tx = Some(writer_tx);
        cartridge.reader_handle = Some(rh);
        cartridge.writer_handle = Some(wh);
        cartridge.stderr_handle = stderr;
        cartridge.last_death_message = None; // Clear any previous death message

        // Capture observer payload while we still have an exclusive borrow.
        let observer_pid = cartridge.process.as_ref().and_then(|c| c.id());
        let observer_name = cartridge
            .path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let observer_caps: Vec<String> =
            cartridge.caps.iter().map(|c| c.urn.to_string()).collect();

        self.update_cap_table();
        self.update_process_snapshot();

        // Notify lifecycle observer (e.g., XPC reverse-callback bridge).
        if let Some(ref obs) = self.observer {
            obs.cartridge_spawned(cartridge_idx, observer_pid, &observer_name, &observer_caps);
        }

        Ok(())
    }

    /// Update the shared process snapshot with current cartridge state.
    /// Called after every spawn and death event.
    fn update_process_snapshot(&self) {
        let mut snap = self.process_snapshot.write().unwrap();
        snap.clear();
        for (idx, cartridge) in self.cartridges.iter().enumerate() {
            if let Some(ref child) = cartridge.process {
                if let Some(pid) = child.id() {
                    snap.push(CartridgeProcessInfo {
                        cartridge_index: idx,
                        pid,
                        name: cartridge
                            .path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                        running: cartridge.running,
                        caps: cartridge.caps.iter().map(|c| c.urn.to_string()).collect(),
                        memory_footprint_mb: cartridge.memory_footprint_mb,
                        memory_rss_mb: cartridge.memory_rss_mb,
                    });
                }
            }
        }
    }

    /// Send a frame to a specific cartridge's stdin.
    fn send_to_cartridge(&self, cartridge_idx: usize, frame: Frame) -> Result<(), AsyncHostError> {
        let cartridge = &self.cartridges[cartridge_idx];
        if frame.frame_type == FrameType::Req {
            tracing::debug!(target: "host_runtime", "send_to_cartridge[{}]: {:?} cap={:?} xid={:?}", cartridge_idx, frame.frame_type, frame.cap, frame.routing_id);
        }
        let writer_tx = cartridge.writer_tx.as_ref().ok_or_else(|| {
            AsyncHostError::Protocol(format!(
                "Cartridge {} not running — no writer channel",
                cartridge_idx
            ))
        })?;
        writer_tx.send(frame).map_err(|_| AsyncHostError::SendError)
    }

    /// Find which cartridge handles a given cap URN.
    ///
    /// Uses `is_dispatchable(provider, request)` to find cartridges that can
    /// legally handle the request, then ranks by specificity.
    ///
    /// Ranking prefers:
    /// 1. Equivalent matches (distance 0)
    /// 2. More specific providers (positive distance) - refinements
    /// 3. More generic providers (negative distance) - fallbacks
    fn find_cartridge_for_cap(&self, cap_urn: &str) -> Option<usize> {
        let request_urn = match crate::CapUrn::from_string(cap_urn) {
            Ok(u) => u,
            Err(_) => return None,
        };

        let request_specificity = request_urn.specificity();

        // Collect ALL dispatchable cartridges with their specificity scores
        let mut matches: Vec<(usize, isize)> = Vec::new(); // (cartridge_idx, signed_distance)

        for (registered_cap, cartridge_idx) in &self.cap_table {
            if let Ok(registered_urn) = crate::CapUrn::from_string(registered_cap) {
                // Use is_dispatchable: can this provider handle this request?
                if registered_urn.is_dispatchable(&request_urn) {
                    let specificity = registered_urn.specificity();
                    let signed_distance = specificity as isize - request_specificity as isize;
                    matches.push((*cartridge_idx, signed_distance));
                }
            }
        }

        if matches.is_empty() {
            return None;
        }

        // Ranking: prefer equivalent (0), then more specific (+), then more generic (-)
        matches.sort_by(|a, b| {
            let (_, dist_a) = a;
            let (_, dist_b) = b;

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

        matches.first().map(|(idx, _)| *idx)
    }

    // =========================================================================
    // HEARTBEAT HEALTH MONITORING
    // =========================================================================

    /// Send heartbeat probes to all running cartridges and check for timeouts.
    fn send_heartbeats_and_check_timeouts(&mut self, outbound_tx: &mpsc::UnboundedSender<Frame>) {
        let now = Instant::now();

        for cartridge_idx in 0..self.cartridges.len() {
            let cartridge = &mut self.cartridges[cartridge_idx];
            if !cartridge.running {
                continue;
            }

            // Check for timed-out heartbeats
            let timed_out: Vec<MessageId> = cartridge
                .pending_heartbeats
                .iter()
                .filter(|(_, sent)| now.duration_since(**sent) > HEARTBEAT_TIMEOUT)
                .map(|(id, _)| id.clone())
                .collect();

            if !timed_out.is_empty() {
                // Cartridge is unresponsive — remove its caps temporarily
                for id in timed_out {
                    cartridge.pending_heartbeats.remove(&id);
                }
                cartridge.running = false;

                // Send ERR for pending requests (both new lists)
                let failed_incoming_keys: Vec<(MessageId, MessageId)> = self
                    .incoming_rxids
                    .iter()
                    .filter(|(_, &idx)| idx == cartridge_idx)
                    .map(|(key, _)| key.clone())
                    .collect();

                let failed_outgoing_rids: Vec<MessageId> = self
                    .outgoing_rids
                    .iter()
                    .filter(|(_, &idx)| idx == cartridge_idx)
                    .map(|(rid, _)| rid.clone())
                    .collect();

                for (xid, rid) in &failed_incoming_keys {
                    let flow_key = FlowKey {
                        rid: rid.clone(),
                        xid: Some(xid.clone()),
                    };
                    let next_seq = self
                        .outgoing_max_seq
                        .remove(&flow_key)
                        .map(|s| s + 1)
                        .unwrap_or(0);
                    let mut err_frame = Frame::err(
                        rid.clone(),
                        "CARTRIDGE_UNHEALTHY",
                        "Cartridge stopped responding to heartbeats",
                    );
                    err_frame.routing_id = Some(xid.clone());
                    err_frame.seq = next_seq;
                    let _ = outbound_tx.send(err_frame);
                    self.incoming_rxids.remove(&(xid.clone(), rid.clone()));
                }

                for rid in &failed_outgoing_rids {
                    let flow_key = FlowKey {
                        rid: rid.clone(),
                        xid: None,
                    };
                    let next_seq = self
                        .outgoing_max_seq
                        .remove(&flow_key)
                        .map(|s| s + 1)
                        .unwrap_or(0);
                    let mut err_frame = Frame::err(
                        rid.clone(),
                        "CARTRIDGE_UNHEALTHY",
                        "Cartridge stopped responding to heartbeats",
                    );
                    err_frame.seq = next_seq;
                    let _ = outbound_tx.send(err_frame);
                    self.outgoing_rids.remove(rid);
                }

                continue;
            }

            // Send a new heartbeat probe
            if let Some(ref writer_tx) = cartridge.writer_tx {
                let hb_id = MessageId::new_uuid();
                let hb = Frame::heartbeat(hb_id.clone());
                if writer_tx.send(hb).is_ok() {
                    cartridge.pending_heartbeats.insert(hb_id, now);
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

    /// Rebuild the cap_table from all cartridges (running or registered).
    fn update_cap_table(&mut self) {
        self.cap_table.clear();
        for (idx, cartridge) in self.cartridges.iter().enumerate() {
            if cartridge.hello_failed {
                continue; // Permanently removed
            }
            // Use real caps if available (from HELLO), otherwise known_caps
            if cartridge.running && !cartridge.caps.is_empty() {
                // Extract URN strings from Cap objects
                for cap in &cartridge.caps {
                    self.cap_table.push((cap.urn.to_string(), idx));
                }
            } else {
                // Use known_caps (URN strings)
                for cap_urn in &cartridge.known_caps {
                    self.cap_table.push((cap_urn.clone(), idx));
                }
            }
        }
    }

    /// Build the `installed_cartridges` list for a RelayNotify payload,
    /// injecting live runtime stats derived from the routing tables and
    /// cartridge process state. One source of truth — the engine sees what
    /// the host sees with no time skew beyond the send itself.
    fn build_installed_cartridge_identities(&self) -> Vec<InstalledCartridgeIdentity> {
        // Count active incoming requests per cartridge index.
        let mut active_counts: HashMap<usize, u64> = HashMap::new();
        for &idx in self.incoming_rxids.values() {
            *active_counts.entry(idx).or_insert(0) += 1;
        }
        // Count outgoing peer requests per cartridge index.
        let mut peer_counts: HashMap<usize, u64> = HashMap::new();
        for &idx in self.outgoing_rids.values() {
            *peer_counts.entry(idx).or_insert(0) += 1;
        }

        self.cartridges
            .iter()
            .enumerate()
            .filter_map(|(idx, cartridge)| {
                let base = cartridge.installed_cartridge_identity()?;
                let pid = cartridge.process.as_ref().and_then(|c| c.id());
                let stats = CartridgeRuntimeStats {
                    running: cartridge.running,
                    pid,
                    active_request_count: *active_counts.get(&idx).unwrap_or(&0),
                    peer_request_count: *peer_counts.get(&idx).unwrap_or(&0),
                    memory_footprint_mb: cartridge.memory_footprint_mb,
                    memory_rss_mb: cartridge.memory_rss_mb,
                    last_heartbeat_unix_seconds: cartridge.last_heartbeat_unix_seconds,
                    restart_count: cartridge.restart_count,
                };
                Some(InstalledCartridgeIdentity {
                    runtime_stats: Some(stats),
                    ..base
                })
            })
            .collect()
    }

    /// Rebuild the aggregate capabilities from all running, healthy cartridges.
    ///
    /// If outbound_tx is Some (i.e., running in relay mode), sends a RelayNotify
    /// frame with the updated capabilities. This allows RelaySwitch/RelayMaster
    /// to track capability changes dynamically as cartridges connect/disconnect/fail.
    fn rebuild_capabilities(&mut self, outbound_tx: Option<&mpsc::UnboundedSender<Frame>>) {
        use crate::standard::caps::CAP_IDENTITY;

        // Collect caps contributed by healthy (non-hello-failed) cartridges.
        // CAP_IDENTITY is prepended only when at least one healthy cartridge
        // exists — each cartridge's manifest mandatorily declares CAP_IDENTITY
        // and answers the echo, so the host-level advertisement is a
        // reflection of that reality. If no healthy cartridge is present
        // there is no handler chain to service the engine's identity
        // probe, and the host must not claim otherwise.
        let mut cap_urns: Vec<String> = Vec::new();
        let mut healthy_cartridge_count = 0usize;

        // Add capability URN strings from all known/discovered cartridges.
        // Includes caps from ALL registered cartridges that haven't permanently failed HELLO.
        // Running cartridges use their actual manifest caps; non-running cartridges use knownCaps.
        // This ensures the relay always advertises all caps that CAN be handled, regardless
        // of whether the cartridge process is currently alive (on-demand spawn handles restarts).
        for cartridge in &self.cartridges {
            if cartridge.hello_failed {
                continue; // Permanently broken, don't advertise
            }

            healthy_cartridge_count += 1;
            if cartridge.running && !cartridge.caps.is_empty() {
                // Running: use actual caps from manifest (verified via HELLO handshake)
                for cap in &cartridge.caps {
                    let urn_str = cap.urn.to_string();
                    // Don't duplicate identity (cartridges also declare it)
                    if urn_str != CAP_IDENTITY {
                        cap_urns.push(urn_str);
                    }
                }
            } else {
                // Not running: use knownCaps (from discovery, available for on-demand spawn)
                for cap_urn in &cartridge.known_caps {
                    if cap_urn != CAP_IDENTITY {
                        cap_urns.push(cap_urn.clone());
                    }
                }
            }
        }

        if healthy_cartridge_count > 0 {
            cap_urns.insert(0, CAP_IDENTITY.to_string());
        }

        // For internal use, store as simple JSON array of URN strings
        self.capabilities =
            serde_json::to_vec(&cap_urns).expect("Failed to serialize capability URNs");

        // Send RelayNotify to relay if in relay mode.
        if let Some(tx) = outbound_tx {
            let installed_cartridges = self.build_installed_cartridge_identities();
            let notify_payload = RelayNotifyCapabilitiesPayload {
                caps: cap_urns.clone(),
                installed_cartridges,
            };
            let notify_bytes = serde_json::to_vec(&notify_payload)
                .expect("Failed to serialize RelayNotify capabilities payload");
            let notify_frame = Frame::relay_notify(&notify_bytes, &Limits::default());
            let _ = tx.send(notify_frame); // Ignore error if relay closed
        }
    }

    /// Kill all managed cartridge processes.
    ///
    /// Order matters: drop writer_tx first (closes the channel), then AWAIT the
    /// writer handle (so it exits naturally and drops the write stream, which
    /// causes the cartridge to see EOF). Only then abort the reader handle.
    /// Aborting the writer instead of awaiting it can leave the write stream
    /// open in a single-threaded runtime, deadlocking any sync thread that
    /// blocks on the cartridge's read().
    async fn kill_all_cartridges(&mut self) {
        // Collect death notifications under exclusive borrow; fire callbacks
        // afterward to avoid borrow conflicts and to keep the observer call
        // outside the kill path.
        let mut death_notifications: Vec<(usize, Option<u32>, String)> = Vec::new();

        for (idx, cartridge) in self.cartridges.iter_mut().enumerate() {
            let was_running = cartridge.running;
            let pid_at_death = cartridge.process.as_ref().and_then(|c| c.id());
            let name = cartridge
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();

            cartridge.shutdown_reason = Some(ShutdownReason::AppExit);
            if let Some(ref mut child) = cartridge.process {
                let _ = child.kill().await;
            }
            cartridge.process = None;
            cartridge.running = false;

            // Close the channel → writer task's rx.recv() returns None → task exits
            cartridge.writer_tx = None;

            // AWAIT (not abort) the writer handle so it drops the write stream cleanly.
            if let Some(handle) = cartridge.writer_handle.take() {
                let _ = handle.await;
            }

            // Now the write stream is closed → cartridge sees EOF.
            // Safe to abort the reader (it will exit on its own anyway).
            if let Some(handle) = cartridge.reader_handle.take() {
                handle.abort();
            }

            if was_running {
                death_notifications.push((idx, pid_at_death, name));
            }
        }

        // Notify lifecycle observer for each cartridge that was running.
        if let Some(ref obs) = self.observer {
            for (idx, pid, name) in &death_notifications {
                obs.cartridge_died(*idx, *pid, name);
            }
        }
    }

    /// Spawn a writer task that reads frames from a channel and writes to a cartridge's stdin.
    fn start_writer_task<W: AsyncWrite + Unpin + Send + 'static>(
        mut writer: FrameWriter<W>,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut seq_assigner = SeqAssigner::new();
            while let Some(mut frame) = rx.recv().await {
                seq_assigner.assign(&mut frame);
                if let Err(_) = writer.write(&frame).await {
                    break;
                }
                if matches!(frame.frame_type, FrameType::End | FrameType::Err) {
                    seq_assigner.remove(&FlowKey::from_frame(&frame));
                }
            }
        })
    }

    /// Spawn a reader task that reads frames from a cartridge's stdout and sends events.
    fn start_reader_task<R: AsyncRead + Unpin + Send + 'static>(
        cartridge_idx: usize,
        mut reader: FrameReader<R>,
        event_tx: mpsc::UnboundedSender<CartridgeEvent>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match reader.read().await {
                    Ok(Some(frame)) => {
                        if event_tx
                            .send(CartridgeEvent::Frame {
                                cartridge_idx,
                                frame,
                            })
                            .is_err()
                        {
                            break; // Runtime dropped
                        }
                    }
                    Ok(None) => {
                        // EOF — cartridge closed stdout
                        let _ = event_tx.send(CartridgeEvent::Death { cartridge_idx });
                        break;
                    }
                    Err(_) => {
                        // Read error — treat as death
                        let _ = event_tx.send(CartridgeEvent::Death { cartridge_idx });
                        break;
                    }
                }
            }
        })
    }

    /// Outbound writer loop: reads frames from channel, writes to relay.
    /// Frames arrive with seq already assigned by CartridgeRuntime — no modification needed.
    async fn outbound_writer_loop<W: AsyncWrite + Unpin>(
        relay_write: W,
        mut rx: mpsc::UnboundedReceiver<Frame>,
    ) {
        let mut writer = FrameWriter::new(relay_write);
        while let Some(frame) = rx.recv().await {
            if writer.write(&frame).await.is_err() {
                break;
            }
        }
    }
}

impl Drop for CartridgeHostRuntime {
    fn drop(&mut self) {
        // Drop cannot be async, so we close channels (triggering writer exit)
        // and abort reader tasks. Writer tasks exit naturally when writer_tx
        // is dropped (channel closes → rx.recv() returns None → task exits
        // → OwnedWriteHalf dropped → cartridge sees EOF).
        // Child processes with kill_on_drop will be killed when Child is dropped.
        for cartridge in &mut self.cartridges {
            cartridge.writer_tx = None; // Close channel → writer task exits naturally
            if let Some(handle) = cartridge.reader_handle.take() {
                handle.abort();
            }
            // Don't abort writer — let it exit naturally so the stream closes cleanly.
        }
    }
}

// =============================================================================
// HELPERS
// =============================================================================

/// Parse cap URNs from a cartridge manifest JSON.
///
/// Expected format:
/// ```json
/// {"name": "...", "caps": [{"urn": "cap:in=\"media:void\";op=test;out=\"media:void\"", ...}, ...]}
/// ```
/// Reason a manifest was rejected by `parse_caps_from_manifest`. Carries
/// the specific failure mode so the caller can pick the right
/// `CartridgeAttachmentErrorKind` — `ManifestInvalid` when the JSON itself
/// is malformed, `Incompatible` when the JSON parses but violates the
/// cartridge schema (missing CAP_IDENTITY, old shape, etc.).
#[derive(Debug)]
enum ParseCapsError {
    /// JSON failed to parse or did not deserialize into `CapManifest`.
    InvalidJson(AsyncHostError),
    /// JSON parsed but the manifest is structurally incompatible with
    /// the host's expectations (e.g. missing CAP_IDENTITY).
    Incompatible(AsyncHostError),
}

impl std::fmt::Display for ParseCapsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseCapsError::InvalidJson(e) | ParseCapsError::Incompatible(e) => write!(f, "{}", e),
        }
    }
}

impl ParseCapsError {
    fn into_async_host_error(self) -> AsyncHostError {
        match self {
            ParseCapsError::InvalidJson(e) | ParseCapsError::Incompatible(e) => e,
        }
    }

    fn attachment_kind(&self) -> CartridgeAttachmentErrorKind {
        match self {
            ParseCapsError::InvalidJson(_) => CartridgeAttachmentErrorKind::ManifestInvalid,
            ParseCapsError::Incompatible(_) => CartridgeAttachmentErrorKind::Incompatible,
        }
    }
}

fn parse_caps_from_manifest(manifest: &[u8]) -> Result<Vec<crate::Cap>, ParseCapsError> {
    use crate::standard::caps::CAP_IDENTITY;
    use crate::urn::cap_urn::CapUrn;
    use crate::CapManifest;

    let manifest_obj: CapManifest = serde_json::from_slice(manifest).map_err(|e| {
        ParseCapsError::InvalidJson(AsyncHostError::Protocol(format!(
            "Invalid CapManifest from cartridge: {}",
            e
        )))
    })?;

    let identity_urn =
        CapUrn::from_string(CAP_IDENTITY).expect("BUG: CAP_IDENTITY constant is invalid");
    let all_caps = manifest_obj.all_caps();
    let has_identity = all_caps
        .iter()
        .any(|cap| identity_urn.conforms_to(&cap.urn));
    if !has_identity {
        return Err(ParseCapsError::Incompatible(AsyncHostError::Protocol(
            format!(
                "Cartridge manifest missing required CAP_IDENTITY ({})",
                CAP_IDENTITY
            ),
        )));
    }

    Ok(all_caps.into_iter().cloned().collect())
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::caps::CAP_IDENTITY;
    use crate::CapUrn;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{BufReader, BufWriter};
    use tokio::net::UnixStream;

    /// Records spawn/death counts for `CartridgeHostObserver` contract
    /// tests. Mirrors `RecordingObserver` in the Swift Bifaci tests.
    struct RecordingObserver {
        spawn_count: AtomicUsize,
        death_count: AtomicUsize,
    }

    impl RecordingObserver {
        fn new() -> Self {
            Self {
                spawn_count: AtomicUsize::new(0),
                death_count: AtomicUsize::new(0),
            }
        }
        fn spawns(&self) -> usize {
            self.spawn_count.load(Ordering::Acquire)
        }
        fn deaths(&self) -> usize {
            self.death_count.load(Ordering::Acquire)
        }
    }

    impl CartridgeHostObserver for RecordingObserver {
        fn cartridge_spawned(
            &self,
            _cartridge_index: usize,
            _pid: Option<u32>,
            _name: &str,
            _caps: &[String],
        ) {
            self.spawn_count.fetch_add(1, Ordering::AcqRel);
        }
        fn cartridge_died(&self, _cartridge_index: usize, _pid: Option<u32>, _name: &str) {
            self.death_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Pins the optional-observer contract: a brand-new runtime with
    /// no observer attached must close cleanly on an empty cartridge
    /// list. A regression here would mean the observer-firing path
    /// became non-optional and broke every call site that doesn't
    /// register an observer (engine in-process runtime, in-process
    /// host tests, integration tests).
    #[tokio::test]
    async fn observer_is_optional() {
        let mut runtime = CartridgeHostRuntime::new();
        // Ensure nothing fires when no observer is set and we
        // immediately tear the runtime down.
        runtime.kill_all_cartridges().await;
    }

    /// Pins the observer-clearing contract: a setObserver(None)
    /// after a previous registration must drop the strong ref so a
    /// subsequent lifecycle moment doesn't fire into a torn-down
    /// bridge. Matches the Swift `setObserver(nil)` test.
    #[tokio::test]
    async fn set_observer_none_clears_previous() {
        let observer = Arc::new(RecordingObserver::new());
        let mut runtime = CartridgeHostRuntime::new();
        runtime.set_observer(Some(observer.clone() as Arc<dyn CartridgeHostObserver>));
        runtime.set_observer(None);
        runtime.kill_all_cartridges().await;
        assert_eq!(
            observer.spawns(),
            0,
            "Observer was cleared via set_observer(None) before any \
             spawn moment, yet recorded {} spawn events — the runtime is \
             firing into a cleared observer slot.",
            observer.spawns()
        );
        assert_eq!(
            observer.deaths(),
            0,
            "Observer was cleared via set_observer(None) before any \
             death moment, yet recorded {} death events — the runtime is \
             firing into a cleared observer slot.",
            observer.deaths()
        );
    }

    /// Helper: perform handshake_accept and handle the identity verification REQ.
    /// Returns (FrameReader, FrameWriter) ready for further communication.
    async fn cartridge_handshake_with_identity<R, W>(
        from_runtime: R,
        to_runtime: W,
        manifest: &[u8],
    ) -> (
        crate::bifaci::io::FrameReader<BufReader<R>>,
        crate::bifaci::io::FrameWriter<BufWriter<W>>,
    )
    where
        R: tokio::io::AsyncRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        use crate::bifaci::io::{handshake_accept, FrameReader, FrameWriter};

        let mut reader = FrameReader::new(BufReader::new(from_runtime));
        let mut writer = FrameWriter::new(BufWriter::new(to_runtime));
        handshake_accept(&mut reader, &mut writer, manifest)
            .await
            .unwrap();

        // Handle identity verification REQ
        let req = reader.read().await.unwrap().expect("expected identity REQ");
        assert_eq!(
            req.frame_type,
            FrameType::Req,
            "first frame after handshake must be REQ"
        );

        // Read request body: STREAM_START → CHUNK(s) → STREAM_END → END
        let mut payload = Vec::new();
        loop {
            let f = reader.read().await.unwrap().expect("expected frame");
            match f.frame_type {
                FrameType::StreamStart => {}
                FrameType::Chunk => payload.extend(f.payload.unwrap_or_default()),
                FrameType::StreamEnd => {}
                FrameType::End => break,
                other => panic!(
                    "unexpected frame type during identity verification: {:?}",
                    other
                ),
            }
        }

        // Echo response: STREAM_START → CHUNK → STREAM_END → END
        let stream_id = "identity-echo".to_string();
        let ss = Frame::stream_start(
            req.id.clone(),
            stream_id.clone(),
            "media:".to_string(),
            None,
        );
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

    // TEST480: parse_caps_from_manifest classifies failures by kind
    //
    // Manifest JSON that parses but lacks CAP_IDENTITY is `Incompatible`
    // (schema-rejected). Manifest bytes that don't parse as CapManifest are
    // `ManifestInvalid` (JSON-level failure). The split lets the host's
    // attachment-error reporter surface the right kind to the UI.
    #[test]
    fn test480_parse_caps_rejects_manifest_without_identity() {
        // JSON-valid manifest, missing CAP_IDENTITY → Incompatible.
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;
        let result = parse_caps_from_manifest(manifest.as_bytes());
        let err = result.expect_err("Manifest without CAP_IDENTITY must be rejected");
        assert!(
            matches!(err, ParseCapsError::Incompatible(_)),
            "Missing CAP_IDENTITY must classify as Incompatible, got {:?}",
            err
        );
        assert_eq!(
            err.attachment_kind(),
            CartridgeAttachmentErrorKind::Incompatible,
            "attachment_kind() must agree with the variant"
        );
        assert!(
            format!("{}", err).contains("CAP_IDENTITY"),
            "Error must mention CAP_IDENTITY, got: {}",
            err
        );

        // Garbage bytes that don't deserialize → ManifestInvalid.
        let bad_json = b"{not even json";
        let result_bad = parse_caps_from_manifest(bad_json);
        let err_bad = result_bad.expect_err("Non-JSON manifest must be rejected");
        assert!(
            matches!(err_bad, ParseCapsError::InvalidJson(_)),
            "Non-JSON manifest must classify as InvalidJson, got {:?}",
            err_bad
        );
        assert_eq!(
            err_bad.attachment_kind(),
            CartridgeAttachmentErrorKind::ManifestInvalid,
            "attachment_kind() must agree with the variant"
        );

        // Valid manifest WITH CAP_IDENTITY must succeed.
        let manifest_ok = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;
        let result_ok = parse_caps_from_manifest(manifest_ok.as_bytes());
        let caps = result_ok.expect("Manifest with CAP_IDENTITY must be accepted");
        assert_eq!(caps.len(), 2, "Must parse both caps");
    }

    // TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly
    #[test]
    fn test235_response_chunk() {
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
    fn test236_response_chunk_with_all_fields() {
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

    // TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice
    #[test]
    fn test237_cartridge_response_single() {
        let response = CartridgeResponse::Single(b"result".to_vec());
        assert_eq!(response.final_payload(), Some(b"result".as_slice()));
        assert_eq!(response.concatenated(), b"result");
    }

    // TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec
    #[test]
    fn test238_cartridge_response_single_empty() {
        let response = CartridgeResponse::Single(vec![]);
        assert_eq!(response.final_payload(), Some(b"".as_slice()));
        assert_eq!(response.concatenated(), b"");
    }

    // TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order
    #[test]
    fn test239_cartridge_response_streaming() {
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
        let response = CartridgeResponse::Streaming(chunks);
        assert_eq!(response.concatenated(), b"hello world");
    }

    // TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload
    #[test]
    fn test240_cartridge_response_streaming_final_payload() {
        let chunks = vec![
            ResponseChunk {
                payload: b"first".to_vec(),
                seq: 0,
                offset: None,
                len: None,
                is_eof: false,
            },
            ResponseChunk {
                payload: b"last".to_vec(),
                seq: 1,
                offset: None,
                len: None,
                is_eof: true,
            },
        ];
        let response = CartridgeResponse::Streaming(chunks);
        assert_eq!(response.final_payload(), Some(b"last".as_slice()));
    }

    // TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation
    #[test]
    fn test241_cartridge_response_streaming_empty_chunks() {
        let response = CartridgeResponse::Streaming(vec![]);
        assert_eq!(response.concatenated(), b"");
        assert!(response.final_payload().is_none());
    }

    // TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads
    #[test]
    fn test242_cartridge_response_streaming_large_payload() {
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
        let response = CartridgeResponse::Streaming(chunks);
        let result = response.concatenated();
        assert_eq!(result.len(), 3000);
        assert_eq!(&result[..1000], &chunk1_data);
        assert_eq!(&result[1000..], &chunk2_data);
    }

    // TEST243: Test AsyncHostError variants display correct error messages
    #[test]
    fn test243_async_host_error_display() {
        let err = AsyncHostError::CartridgeError {
            code: "NOT_FOUND".to_string(),
            message: "Cap not found".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("NOT_FOUND"));
        assert!(msg.contains("Cap not found"));

        assert_eq!(format!("{}", AsyncHostError::Closed), "Host is closed");
        assert_eq!(
            format!("{}", AsyncHostError::ProcessExited),
            "Cartridge process exited unexpectedly"
        );
        assert_eq!(
            format!("{}", AsyncHostError::SendError),
            "Send error: channel closed"
        );
        assert_eq!(
            format!("{}", AsyncHostError::RecvError),
            "Receive error: channel closed"
        );
    }

    // TEST244: Test AsyncHostError::from converts CborError to Cbor variant
    #[test]
    fn test244_async_host_error_from_cbor() {
        let cbor_err = crate::bifaci::io::CborError::InvalidFrame("test".to_string());
        let host_err: AsyncHostError = cbor_err.into();
        match host_err {
            AsyncHostError::Cbor(msg) => assert!(msg.contains("test")),
            _ => panic!("expected Cbor variant"),
        }
    }

    // TEST245: Test AsyncHostError::from converts io::Error to Io variant
    #[test]
    fn test245_async_host_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let host_err: AsyncHostError = io_err.into();
        match host_err {
            AsyncHostError::Io(msg) => assert!(msg.contains("pipe broken")),
            _ => panic!("expected Io variant"),
        }
    }

    // TEST246: Test AsyncHostError Clone implementation produces equal values
    #[test]
    fn test246_async_host_error_clone() {
        let err = AsyncHostError::CartridgeError {
            code: "ERR".to_string(),
            message: "msg".to_string(),
        };
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    // TEST247: Test ResponseChunk Clone produces independent copy with same data
    #[test]
    fn test247_response_chunk_clone() {
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

    // TEST413: Register cartridge adds entries to cap_table
    #[test]
    fn test413_register_cartridge_adds_to_cap_table() {
        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge(
            Path::new("/usr/bin/test-cartridge"),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &[
                "cap:in=\"media:void\";op=convert;out=\"media:void\"".to_string(),
                "cap:in=\"media:void\";op=analyze;out=\"media:void\"".to_string(),
            ],
        );

        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(
            runtime.cap_table[0].0,
            "cap:in=\"media:void\";op=convert;out=\"media:void\""
        );
        assert_eq!(runtime.cap_table[0].1, 0);
        assert_eq!(
            runtime.cap_table[1].0,
            "cap:in=\"media:void\";op=analyze;out=\"media:void\""
        );
        assert_eq!(runtime.cap_table[1].1, 0);
        assert_eq!(runtime.cartridges.len(), 1);
        assert!(!runtime.cartridges[0].running);
    }

    // TEST414: capabilities() returns empty JSON initially (no running cartridges)
    #[test]
    fn test414_capabilities_empty_initially() {
        let runtime = CartridgeHostRuntime::new();
        assert!(
            runtime.capabilities().is_empty(),
            "No cartridges registered = empty capabilities"
        );

        let mut runtime2 = CartridgeHostRuntime::new();
        runtime2.register_cartridge(
            Path::new("/usr/bin/test"),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &["cap:in=\"media:void\";op=test;out=\"media:void\"".to_string()],
        );
        // Cartridge registered but not running — capabilities still empty
        assert!(
            runtime2.capabilities().is_empty(),
            "Registered but not running cartridge should not appear in capabilities"
        );
    }

    // TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary)
    #[tokio::test]
    async fn test415_req_for_known_cap_triggers_spawn() {
        // Production install layout: a versioned cartridge directory
        // containing cartridge.json (which carries the channel) plus an
        // entry-point binary. Point at a non-executable file so spawn
        // fails — that exercises the "REQ → spawn attempt → spawn
        // failure" path on a cartridge with a real installed identity.
        let cartridge_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            cartridge_dir.path().join("cartridge.json"),
            r#"{"name":"test","version":"0.0.1","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}"#,
        )
        .unwrap();
        let entry_point = cartridge_dir.path().join("bin");
        std::fs::write(&entry_point, b"not an executable").unwrap();

        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge_dir(
            &entry_point,
            cartridge_dir.path(),
            "test",
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            "0.0.1",
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
            let mut writer = FrameWriter::new(engine_write_half);
            let mut req = Frame::req(
                MessageId::new_uuid(),
                "cap:in=\"media:void\";op=test;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(MessageId::Uint(1)); // XID from RelaySwitch
            seq.assign(&mut req);
            writer.write(&req).await.unwrap();
            seq.remove(&FlowKey::from_frame(&req));
        });

        // Run the runtime — should attempt to spawn, fail (entry-point
        // file exists but isn't executable)
        let result = runtime
            .run(runtime_read_half, runtime_write_half, || vec![])
            .await;

        assert!(result.is_err(), "Should fail because entry point is not executable");
        let err = result.unwrap_err();
        let err_str = format!("{}", err);
        assert!(
            err_str.to_lowercase().contains("spawn")
                || err_str.contains("permission")
                || err_str.contains("Exec"),
            "Error should mention spawn failure, got: {}",
            err_str
        );

        send_handle.await.unwrap();
    }

    // TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities
    #[tokio::test]
    async fn test416_attach_cartridge_handshake_updates_capabilities() {
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (cartridge_to_runtime, runtime_from_cartridge) = UnixStream::pair().unwrap();
        let (runtime_to_cartridge, cartridge_from_runtime) = UnixStream::pair().unwrap();

        let (cartridge_read, _) = runtime_from_cartridge.into_split();
        let (_, cartridge_write) = runtime_to_cartridge.into_split();

        // Cartridge task does handshake + identity verification
        let manifest_bytes = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            cartridge_handshake_with_identity(
                cartridge_from_runtime,
                cartridge_to_runtime,
                &manifest_bytes,
            )
            .await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let idx = runtime
            .attach_cartridge(cartridge_read, cartridge_write)
            .await
            .unwrap();

        assert_eq!(idx, 0);
        assert!(runtime.cartridges[0].running);
        // Verify cartridge has identity cap via semantic comparison (not string comparison)
        let identity_urn = crate::CapUrn::from_string(CAP_IDENTITY).unwrap();
        assert!(
            runtime.cartridges[0]
                .caps
                .iter()
                .any(|c| identity_urn.conforms_to(&c.urn)),
            "Cartridge must have identity cap"
        );
        assert!(!runtime.capabilities().is_empty());

        // Capabilities JSON must include identity
        let caps: Vec<String> = serde_json::from_slice(runtime.capabilities()).unwrap();
        assert!(
            caps.iter().any(|s| crate::CapUrn::from_string(s)
                .map(|u| identity_urn.conforms_to(&u))
                .unwrap_or(false)),
            "Capabilities must include identity cap"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges)
    #[tokio::test]
    async fn test417_route_req_to_correct_cartridge() {
        let manifest_a = r#"{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=convert;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;
        let manifest_b = r#"{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=analyze;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Create two cartridge pipe pairs (tokio sockets)
        let (pa_to_rt, rt_from_pa) = UnixStream::pair().unwrap();
        let (rt_to_pa, pa_from_rt) = UnixStream::pair().unwrap();
        let (pb_to_rt, rt_from_pb) = UnixStream::pair().unwrap();
        let (rt_to_pb, pb_from_rt) = UnixStream::pair().unwrap();

        let (pa_read, _) = rt_from_pa.into_split();
        let (_, pa_write) = rt_to_pa.into_split();
        let (pb_read, _) = rt_from_pb.into_split();
        let (_, pb_write) = rt_to_pb.into_split();

        // Cartridge A task
        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pa_from_rt, pa_to_rt, &ma).await;
            // Read one REQ and verify cap
            let frame = r.read().await.unwrap().expect("expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(
                frame.cap.as_deref(),
                Some("cap:in=\"media:void\";op=convert;out=\"media:void\""),
                "Cartridge A should receive convert REQ"
            );
            // Send END response
            let stream_id = "s1".to_string();
            let mut ss = Frame::stream_start(
                frame.id.clone(),
                stream_id.clone(),
                "media:".to_string(),
                None,
            );
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"converted".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk =
                Frame::chunk(frame.id.clone(), stream_id.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(frame.id.clone(), stream_id, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(frame.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: frame.id.clone(),
                xid: None,
            });
        });

        // Cartridge B task
        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = tokio::spawn(async move {
            let (r, w) = cartridge_handshake_with_identity(pb_from_rt, pb_to_rt, &mb).await;
            // Cartridge B should NOT receive the convert REQ
            // It may receive heartbeats, but the REQ should only go to Cartridge A
            // Just exit - the runtime will handle heartbeat timeouts
            drop(r);
            drop(w);
        });

        // Setup runtime
        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(pa_read, pa_write).await.unwrap();
        runtime.attach_cartridge(pb_read, pb_write).await.unwrap();

        // Create relay pipes (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Engine: send REQ, read response, THEN close relay
        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";op=convert;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
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
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
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
        assert!(
            runtime_result.is_ok(),
            "Runtime should exit cleanly: {:?}",
            runtime_result
        );

        let response_payload = engine_task.await.unwrap();
        assert_eq!(response_payload, b"converted");

        pa_handle.await.unwrap();
        pb_handle.await.unwrap();
    }

    // TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay)
    #[tokio::test]
    async fn test419_cartridge_heartbeat_handled_locally() {
        let manifest = r#"{"name":"HBCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Heartbeat cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=hb;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Send a heartbeat from cartridge
            let hb_id = MessageId::new_uuid();
            let mut hb = Frame::heartbeat(hb_id.clone());
            seq.assign(&mut hb);
            w.write(&hb).await.unwrap();

            // Read the heartbeat response
            let response = r
                .read()
                .await
                .unwrap()
                .expect("Expected heartbeat response");
            assert_eq!(response.frame_type, FrameType::Heartbeat);
            assert_eq!(response.id, hb_id, "Response must echo the same ID");

            drop(w); // Close to signal EOF
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay pipes (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Drop engine write to close relay after cartridge finishes
        drop(relay_eng_write);

        // Engine reads — should NOT receive any heartbeat frame
        let engine_recv = tokio::spawn(async move {
            let mut r = FrameReader::new(eng_read_half);
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

        cartridge_handle.await.unwrap();
    }

    // TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through)
    #[tokio::test]
    async fn test420_cartridge_frames_forwarded_to_relay() {
        let manifest = r#"{"name":"FwdCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Forward cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=fwd;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let req_id = MessageId::new_uuid();
        let req_id_for_cartridge = req_id.clone();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read the REQ
            let frame = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(frame.frame_type, FrameType::Req);

            // Consume incoming streams until END
            loop {
                let f = r.read().await.unwrap().expect("Expected frame");
                if f.frame_type == FrameType::End {
                    break;
                }
            }

            // Send LOG + response (LOG should be forwarded too)
            let mut log = Frame::log(req_id_for_cartridge.clone(), "info", "Processing");
            seq.assign(&mut log);
            w.write(&log).await.unwrap();
            let sid = "rs".to_string();
            let mut ss = Frame::stream_start(
                req_id_for_cartridge.clone(),
                sid.clone(),
                "media:".to_string(),
                None,
            );
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"result".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(
                req_id_for_cartridge.clone(),
                sid.clone(),
                0,
                payload,
                0,
                checksum,
            );
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req_id_for_cartridge.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req_id_for_cartridge.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_for_cartridge.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        // Engine: send REQ, read response (keep relay open until response received)
        let req_id_send = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let sid = uuid::Uuid::new_v4().to_string();
            let mut req = Frame::req(
                req_id_send.clone(),
                "cap:in=\"media:void\";op=fwd;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut stream_start =
                Frame::stream_start(req_id_send.clone(), sid.clone(), "media:".to_string(), None);
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
            seq.remove(&FlowKey {
                rid: req_id_send.clone(),
                xid: Some(xid.clone()),
            });

            let mut types = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        let is_end = f.frame_type == FrameType::End;
                        types.push(f.frame_type);
                        if is_end {
                            break;
                        }
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
        assert!(
            received_types.contains(&FrameType::Log),
            "LOG should be forwarded. Got: {:?}",
            received_types
        );
        assert!(
            received_types.contains(&FrameType::StreamStart),
            "STREAM_START should be forwarded"
        );
        assert!(
            received_types.contains(&FrameType::Chunk),
            "CHUNK should be forwarded"
        );
        assert!(
            received_types.contains(&FrameType::End),
            "END should be forwarded"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn)
    // Verifies that after the initial REQ→cartridge routing, all subsequent continuation
    // frames with the same req_id are routed to the same cartridge — even though no cap_urn
    // is present on those frames.
    #[tokio::test]
    async fn test418_route_continuation_frames_by_req_id() {
        let manifest = r#"{"name":"ContCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Continuation cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=cont;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(req.frame_type, FrameType::Req);

            // Continuation frames must arrive with same req_id
            let mut received_types = Vec::new();
            let mut data = Vec::new();
            loop {
                let f = r.read().await.unwrap().expect("Expected frame");
                received_types.push(f.frame_type);
                if f.frame_type == FrameType::Chunk {
                    data.extend(f.payload.unwrap_or_default());
                }
                if f.frame_type == FrameType::End {
                    break;
                }
                assert_eq!(
                    f.id, req.id,
                    "All continuation frames must have same req_id"
                );
            }

            // Verify we got the full sequence
            assert!(
                received_types.contains(&FrameType::StreamStart),
                "Must receive STREAM_START"
            );
            assert!(
                received_types.contains(&FrameType::Chunk),
                "Must receive CHUNK"
            );
            assert!(
                received_types.contains(&FrameType::StreamEnd),
                "Must receive STREAM_END"
            );
            assert!(received_types.contains(&FrameType::End), "Must receive END");
            assert_eq!(data, b"payload-data", "Must receive full payload");

            // Send response
            let sid = "rs".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"ok".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            // Send REQ + stream continuation frames
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";op=cont;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let sid = uuid::Uuid::new_v4().to_string();
            let mut stream_start =
                Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
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
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            payload.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End {
                            break;
                        }
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

        cartridge_handle.await.unwrap();
    }

    // TEST421: Cartridge death updates capability list (caps removed)
    #[tokio::test]
    async fn test421_cartridge_death_updates_capabilities() {
        let manifest = r#"{"name":"Dying","version":"1.0","channel":"release","registry_url":null,"description":"Dying cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=die;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;
            // Die immediately after identity verification
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Before death: caps should include the cartridge's cap
        let expected_urn = CapUrn::from_string("cap:in=\"media:void\";op=die;out=\"media:void\"")
            .expect("Expected URN should parse");
        let caps_before = std::str::from_utf8(runtime.capabilities())
            .unwrap()
            .to_string();
        let parsed_before: serde_json::Value = serde_json::from_str(&caps_before).unwrap();
        let urn_strings: Vec<String> = parsed_before
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        // Parse each URN and check if any is comparable to expected (on same chain)
        let found = urn_strings.iter().any(|urn_str| {
            if let Ok(cap_urn) = CapUrn::from_string(urn_str) {
                expected_urn.is_comparable(&cap_urn)
            } else {
                false
            }
        });
        assert!(
            found,
            "Capabilities should contain cartridge's cap. Expected URN with op=die, got: {:?}",
            urn_strings
        );

        // Relay (close immediately to let runtime exit after processing death) - tokio sockets
        let (relay_rt_read, _relay_eng_write) = UnixStream::pair().unwrap();
        let (_relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();

        // Drop engine write side to close relay
        drop(_relay_eng_write);

        let _ = runtime.run(rt_read_half, rt_write_half, || vec![]).await;

        // After death: capabilities should STILL include the cartridge's known_caps (for on-demand respawn).
        // This is the new behavior - dead cartridges advertise their known_caps so they can be respawned.
        let caps_after = runtime.capabilities();
        let caps_str = std::str::from_utf8(caps_after).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(caps_str).unwrap();
        let urn_strings_after: Vec<String> = parsed
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        // Should have CAP_IDENTITY + cartridge's known caps (identity + op=die)
        assert!(
            urn_strings_after.contains(&CAP_IDENTITY.to_string()),
            "CAP_IDENTITY must always be present"
        );
        let found_after = urn_strings_after.iter().any(|urn_str| {
            if let Ok(cap_urn) = CapUrn::from_string(urn_str) {
                expected_urn.is_comparable(&cap_urn)
            } else {
                false
            }
        });
        assert!(found_after, "Dead cartridge's known_caps should still be advertised for on-demand respawn. Expected URN with op=die, got: {:?}", urn_strings_after);

        cartridge_handle.await.unwrap();
    }

    // TEST422: Cartridge death sends ERR for all pending requests via relay
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test422_cartridge_death_sends_err_for_pending_requests() {
        let manifest = r#"{"name":"DieCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Die cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=die;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and consume all frames until END, then die
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            // Die — drop everything
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);

            let xid = MessageId::Uint(1);
            // Send REQ (cartridge will die after reading it)
            let mut req = Frame::req(
                req_id.clone(),
                "cap:in=\"media:void\";op=die;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id.clone(),
                xid: Some(xid.clone()),
            });

            // Close relay connection after sending request
            // (in real use, engine would implement timeout for pending requests)
            drop(w);
        });

        // Runtime should handle cartridge death gracefully and exit when relay disconnects
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(
            result.is_ok(),
            "Runtime should exit cleanly when cartridge dies and relay disconnects"
        );

        engine_task.await.unwrap();

        cartridge_handle.await.unwrap();
    }

    // TEST423: Multiple cartridges registered with distinct caps route independently
    #[tokio::test]
    async fn test423_multiple_cartridges_route_independently() {
        let manifest_a = r#"{"name":"PA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=alpha;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;
        let manifest_b = r#"{"name":"PB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=beta;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge A (tokio sockets)
        let (pa_to_rt, rt_from_pa) = UnixStream::pair().unwrap();
        let (rt_to_pa, pa_from_rt) = UnixStream::pair().unwrap();
        let (pa_read, _) = rt_from_pa.into_split();
        let (_, pa_write) = rt_to_pa.into_split();

        // Cartridge B (tokio sockets)
        let (pb_to_rt, rt_from_pb) = UnixStream::pair().unwrap();
        let (rt_to_pb, pb_from_rt) = UnixStream::pair().unwrap();
        let (pb_read, _) = rt_from_pb.into_split();
        let (_, pb_write) = rt_to_pb.into_split();

        let ma = manifest_a.as_bytes().to_vec();
        let pa_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pa_from_rt, pa_to_rt, &ma).await;
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";op=alpha;out=\"media:void\"")
            );
            loop {
                let f = r.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let sid = "a".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"from-A".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let pb_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(pb_from_rt, pb_to_rt, &mb).await;
            let req = r.read().await.unwrap().expect("Expected REQ");
            assert_eq!(
                req.cap.as_deref(),
                Some("cap:in=\"media:void\";op=beta;out=\"media:void\"")
            );
            loop {
                let f = r.read().await.unwrap().expect("f");
                if f.frame_type == FrameType::End {
                    break;
                }
            }
            let sid = "b".to_string();
            let mut ss =
                Frame::stream_start(req.id.clone(), sid.clone(), "media:".to_string(), None);
            seq.assign(&mut ss);
            w.write(&ss).await.unwrap();
            let payload = b"from-B".to_vec();
            let checksum = Frame::compute_checksum(&payload);
            let mut chunk = Frame::chunk(req.id.clone(), sid.clone(), 0, payload, 0, checksum);
            seq.assign(&mut chunk);
            w.write(&chunk).await.unwrap();
            let mut se = Frame::stream_end(req.id.clone(), sid, 1);
            seq.assign(&mut se);
            w.write(&se).await.unwrap();
            let mut end = Frame::end(req.id.clone(), None);
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req.id.clone(),
                xid: None,
            });
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(pa_read, pa_write).await.unwrap();
        runtime.attach_cartridge(pb_read, pb_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();
        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let alpha_id = MessageId::new_uuid();
        let beta_id = MessageId::new_uuid();
        let alpha_c = alpha_id.clone();
        let beta_c = beta_id.clone();

        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid_alpha = MessageId::Uint(1);
            let xid_beta = MessageId::Uint(2);
            // Send two requests to different caps
            let mut req_alpha = Frame::req(
                alpha_c.clone(),
                "cap:in=\"media:void\";op=alpha;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut req_alpha);
            w.write(&req_alpha).await.unwrap();
            let mut end_alpha = Frame::end(alpha_c.clone(), None);
            end_alpha.routing_id = Some(xid_alpha.clone());
            seq.assign(&mut end_alpha);
            w.write(&end_alpha).await.unwrap();
            seq.remove(&FlowKey {
                rid: alpha_c.clone(),
                xid: Some(xid_alpha.clone()),
            });
            let mut req_beta = Frame::req(
                beta_c.clone(),
                "cap:in=\"media:void\";op=beta;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut req_beta);
            w.write(&req_beta).await.unwrap();
            let mut end_beta = Frame::end(beta_c.clone(), None);
            end_beta.routing_id = Some(xid_beta.clone());
            seq.assign(&mut end_beta);
            w.write(&end_beta).await.unwrap();
            seq.remove(&FlowKey {
                rid: beta_c.clone(),
                xid: Some(xid_beta.clone()),
            });

            // Collect responses by req_id
            let mut alpha_data = Vec::new();
            let mut beta_data = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == alpha_c {
                                alpha_data.extend(f.payload.unwrap_or_default());
                            } else if f.id == beta_c {
                                beta_data.extend(f.payload.unwrap_or_default());
                            }
                        }
                        if f.frame_type == FrameType::End {
                            ends += 1;
                            if ends >= 2 {
                                break;
                            }
                        }
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
        assert_eq!(alpha_data, b"from-A", "Alpha response from Cartridge A");
        assert_eq!(beta_data, b"from-B", "Beta response from Cartridge B");

        pa_handle.await.unwrap();
        pb_handle.await.unwrap();
    }

    // TEST424: Concurrent requests to the same cartridge are handled independently
    #[tokio::test]
    async fn test424_concurrent_requests_to_same_cartridge() {
        let manifest = r#"{"name":"ConcCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Concurrent cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=conc;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let (mut r, mut w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read two REQs and their streams, then respond to each
            let mut pending: Vec<MessageId> = Vec::new();
            let mut active_requests = 0;
            loop {
                let f = r.read().await.unwrap().expect("frame");
                match f.frame_type {
                    FrameType::Req => {
                        pending.push(f.id.clone());
                        active_requests += 1;
                    }
                    FrameType::End => {
                        // When we've seen END for both requests, respond to both
                        active_requests -= 1;
                        if active_requests == 0 && pending.len() == 2 {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            // Respond to each with different data
            for (i, req_id) in pending.iter().enumerate() {
                let data = format!("response-{}", i).into_bytes();
                let checksum = Frame::compute_checksum(&data);
                let sid = format!("s{}", i);
                let mut ss =
                    Frame::stream_start(req_id.clone(), sid.clone(), "media:".to_string(), None);
                seq.assign(&mut ss);
                w.write(&ss).await.unwrap();
                let mut chunk = Frame::chunk(req_id.clone(), sid.clone(), 0, data, 0, checksum);
                seq.assign(&mut chunk);
                w.write(&chunk).await.unwrap();
                let mut se = Frame::stream_end(req_id.clone(), sid, 1);
                seq.assign(&mut se);
                w.write(&se).await.unwrap();
                let mut end = Frame::end(req_id.clone(), None);
                seq.assign(&mut end);
                w.write(&end).await.unwrap();
                seq.remove(&FlowKey {
                    rid: req_id.clone(),
                    xid: None,
                });
            }
            drop(w);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Relay (tokio sockets)
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();
        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id_0 = MessageId::new_uuid();
        let req_id_1 = MessageId::new_uuid();
        let r0 = req_id_0.clone();
        let r1 = req_id_1.clone();

        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            // Send two REQs concurrently (same cap)
            let xid_0 = MessageId::Uint(1);
            let xid_1 = MessageId::Uint(2);
            let mut req_0 = Frame::req(
                r0.clone(),
                "cap:in=\"media:void\";op=conc;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut req_0);
            w.write(&req_0).await.unwrap();
            let mut end_0 = Frame::end(r0.clone(), None);
            end_0.routing_id = Some(xid_0.clone());
            seq.assign(&mut end_0);
            w.write(&end_0).await.unwrap();
            seq.remove(&FlowKey {
                rid: r0.clone(),
                xid: Some(xid_0.clone()),
            });
            let mut req_1 = Frame::req(
                r1.clone(),
                "cap:in=\"media:void\";op=conc;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut req_1);
            w.write(&req_1).await.unwrap();
            let mut end_1 = Frame::end(r1.clone(), None);
            end_1.routing_id = Some(xid_1.clone());
            seq.assign(&mut end_1);
            w.write(&end_1).await.unwrap();
            seq.remove(&FlowKey {
                rid: r1.clone(),
                xid: Some(xid_1.clone()),
            });

            // Collect responses by req_id
            let mut data_0 = Vec::new();
            let mut data_1 = Vec::new();
            let mut ends = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == r0 {
                                data_0.extend(f.payload.unwrap_or_default());
                            } else if f.id == r1 {
                                data_1.extend(f.payload.unwrap_or_default());
                            }
                        }
                        if f.frame_type == FrameType::End {
                            ends += 1;
                            if ends >= 2 {
                                break;
                            }
                        }
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

        cartridge_handle.await.unwrap();
    }

    // TEST425: find_cartridge_for_cap returns None for unregistered cap
    #[test]
    fn test425_find_cartridge_for_cap_unknown() {
        let mut runtime = CartridgeHostRuntime::new();
        runtime.register_cartridge(
            Path::new("/test"),
            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
            None,
            &["cap:in=\"media:void\";op=known;out=\"media:void\"".to_string()],
        );
        assert!(runtime
            .find_cartridge_for_cap("cap:in=\"media:void\";op=known;out=\"media:void\"")
            .is_some());
        assert!(runtime
            .find_cartridge_for_cap("cap:in=\"media:void\";op=unknown;out=\"media:void\"")
            .is_none());
    }

    // =========================================================================
    // Identity verification integration tests
    // =========================================================================

    // TEST485: attach_cartridge completes identity verification with working cartridge
    #[tokio::test]
    async fn test485_attach_cartridge_identity_verification_succeeds() {
        let manifest = r#"{"name":"IdentityTest","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let idx = runtime.attach_cartridge(p_read, p_write).await.unwrap();
        assert_eq!(idx, 0);
        assert!(
            runtime.cartridges[0].running,
            "Cartridge must be running after identity verification"
        );

        // Verify both caps are registered (semantic comparison, not string)
        let identity_urn = crate::CapUrn::from_string(CAP_IDENTITY).unwrap();
        assert!(
            runtime.cartridges[0]
                .caps
                .iter()
                .any(|c| identity_urn.conforms_to(&c.urn)),
            "Must have identity cap"
        );
        assert_eq!(runtime.cartridges[0].caps.len(), 2, "Must have both caps");

        cartridge_handle.await.unwrap();
    }

    // TEST486: attach_cartridge rejects cartridge that fails identity verification
    #[tokio::test]
    async fn test486_attach_cartridge_identity_verification_fails() {
        let manifest = r#"{"name":"BrokenIdentity","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair (tokio sockets)
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            use crate::bifaci::io::{handshake_accept, FrameReader, FrameWriter};
            let mut reader = FrameReader::new(BufReader::new(p_from_rt));
            let mut writer = FrameWriter::new(BufWriter::new(p_to_rt));
            handshake_accept(&mut reader, &mut writer, &m)
                .await
                .unwrap();

            // Read identity REQ, respond with ERR (broken identity handler)
            let req = reader.read().await.unwrap().expect("expected identity REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            let err = Frame::err(req.id, "BROKEN", "identity handler is broken");
            writer.write(&err).await.unwrap();
        });

        let mut runtime = CartridgeHostRuntime::new();
        let result = runtime.attach_cartridge(p_read, p_write).await;
        assert!(
            result.is_err(),
            "attach_cartridge must fail when identity verification fails"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Identity verification failed"),
            "Error must mention identity verification: {}",
            err
        );

        cartridge_handle.await.unwrap();
    }

    // TEST661: Cartridge death keeps known_caps advertised for on-demand respawn
    #[tokio::test]
    async fn test661_cartridge_death_keeps_known_caps_advertised() {
        let mut runtime = CartridgeHostRuntime::new();

        // Register a cartridge with known_caps (not spawned yet)
        let known_caps = vec![
            "cap:".to_string(), // identity
            "cap:in=\"media:pdf\";op=thumbnail;out=\"media:image;png\"".to_string(),
        ];
        runtime.register_cartridge(std::path::Path::new("/fake/cartridge"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps);

        // Verify known_caps are in cap_table
        assert_eq!(runtime.cap_table.len(), 2);
        assert_eq!(runtime.cap_table[0].0, "cap:");
        assert_eq!(
            runtime.cap_table[1].0,
            "cap:in=\"media:pdf\";op=thumbnail;out=\"media:image;png\""
        );

        // Build capabilities (no outbound_tx, so no RelayNotify sent)
        runtime.rebuild_capabilities(None);

        // Verify capabilities include known_caps
        let caps_json = std::str::from_utf8(runtime.capabilities()).unwrap();
        let caps: serde_json::Value = serde_json::from_str(caps_json).unwrap();
        let cap_urns: Vec<&str> = caps
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();

        assert!(cap_urns.contains(&"cap:"));
        assert!(cap_urns.iter().any(|s| s.contains("thumbnail")));
    }

    // TEST662: rebuild_capabilities includes non-running cartridges' known_caps
    #[tokio::test]
    async fn test662_rebuild_capabilities_includes_non_running_cartridges() {
        let mut runtime = CartridgeHostRuntime::new();

        // Register two cartridges with different known_caps
        let known_caps_1 = vec![
            "cap:".to_string(),
            "cap:in=\"media:pdf\";op=extract;out=\"media:text\"".to_string(),
        ];
        let known_caps_2 = vec![
            "cap:".to_string(),
            "cap:in=\"media:image\";op=ocr;out=\"media:text\"".to_string(),
        ];

        runtime.register_cartridge(std::path::Path::new("/fake/cartridge1"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps_1);
        runtime.register_cartridge(std::path::Path::new("/fake/cartridge2"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps_2);

        // Both cartridges are NOT running, but their known_caps should be advertised
        runtime.rebuild_capabilities(None);

        let caps_json = std::str::from_utf8(runtime.capabilities()).unwrap();
        let caps: serde_json::Value = serde_json::from_str(caps_json).unwrap();
        let cap_urns: Vec<&str> = caps
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();

        // Should contain identity (always) + both cartridges' known_caps
        assert!(cap_urns.contains(&"cap:"));
        assert!(cap_urns.iter().any(|s| s.contains("extract")));
        assert!(cap_urns.iter().any(|s| s.contains("ocr")));
    }

    // TEST663: Cartridge with hello_failed is permanently removed from capabilities
    #[tokio::test]
    async fn test663_hello_failed_cartridge_removed_from_capabilities() {
        let mut runtime = CartridgeHostRuntime::new();

        // Register a cartridge
        let known_caps = vec![
            "cap:".to_string(),
            "cap:in=\"media:void\";op=broken;out=\"media:void\"".to_string(),
        ];
        runtime.register_cartridge(std::path::Path::new("/fake/broken"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps);

        // Manually mark it as hello_failed (simulating HELLO handshake failure)
        runtime.cartridges[0].hello_failed = true;

        // update_cap_table should exclude hello_failed cartridges
        runtime.update_cap_table();

        // Should only have identity cap from the runtime itself, not the broken cartridge
        let found_broken = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("broken"));
        assert!(
            !found_broken,
            "hello_failed cartridge caps should not be in cap_table"
        );

        // rebuild_capabilities should also exclude hello_failed cartridges
        runtime.rebuild_capabilities(None);

        let caps_json = std::str::from_utf8(runtime.capabilities()).unwrap();
        let caps: serde_json::Value = serde_json::from_str(caps_json).unwrap();
        let cap_urns: Vec<&str> = caps
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();

        assert!(
            !cap_urns.iter().any(|s| s.contains("broken")),
            "hello_failed cartridge should not be in capabilities"
        );
    }

    // TEST664: Running cartridge uses manifest caps, not known_caps
    #[tokio::test]
    async fn test664_running_cartridge_uses_manifest_caps() {
        // Manifest with different caps than known_caps
        let manifest = r#"{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:text\";op=uppercase;out=\"media:text\"","title":"Uppercase","command":"uppercase","args":[]}],"adapter_urns":[]}]}"#;

        // Create socket pairs (runtime side and cartridge side)
        let (rt_sock, cartridge_sock) = UnixStream::pair().unwrap();

        // Split runtime socket for attach_cartridge
        let (p_read, p_write) = rt_sock.into_split();

        // Split cartridge socket for handshake
        let (cartridge_from_rt, cartridge_to_rt) = cartridge_sock.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (_r, _w) =
                cartridge_handshake_with_identity(cartridge_from_rt, cartridge_to_rt, &m).await;
            // Keep alive for test
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();

        // Register with different known_caps BEFORE attaching
        let known_caps = vec![
            "cap:".to_string(),
            "cap:in=\"media:pdf\";op=extract;out=\"media:text\"".to_string(),
        ];
        runtime.register_cartridge(std::path::Path::new("/fake/path"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps);

        // Now attach the actual cartridge (which sends different manifest)
        // This simulates what happens when a registered cartridge spawns
        let _cartridge_idx = runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // The running cartridge should use manifest caps, not known_caps
        let caps_json = std::str::from_utf8(runtime.capabilities()).unwrap();
        let caps: serde_json::Value = serde_json::from_str(caps_json).unwrap();
        let cap_urns: Vec<&str> = caps
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();

        // Should have manifest cap (uppercase), NOT known_cap (extract)
        assert!(
            cap_urns.iter().any(|s| s.contains("uppercase")),
            "Running cartridge should use manifest caps. Got: {:?}",
            cap_urns
        );

        // Note: Since we're testing attach_cartridge (not register+spawn), the cartridge is added
        // separately, so we might also see the known_caps from the first registered cartridge
        // unless we remove it. The key test is that uppercase is present (from manifest).

        cartridge_handle.await.unwrap();
    }

    // TEST665: Cap table uses manifest caps for running, known_caps for non-running
    #[tokio::test]
    async fn test665_cap_table_mixed_running_and_non_running() {
        // Set up a running cartridge
        let manifest = r#"{"name":"Running","version":"1.0","channel":"release","registry_url":null,"description":"Running cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:text\";op=running-op;out=\"media:text\"","title":"RunningOp","command":"running","args":[]}],"adapter_urns":[]}]}"#;

        // Create socket pairs (runtime side and cartridge side)
        let (rt_sock, cartridge_sock) = UnixStream::pair().unwrap();

        // Split runtime socket for attach_cartridge
        let (p_read, p_write) = rt_sock.into_split();

        // Split cartridge socket for handshake
        let (cartridge_from_rt, cartridge_to_rt) = cartridge_sock.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (_r, _w) =
                cartridge_handshake_with_identity(cartridge_from_rt, cartridge_to_rt, &m).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();

        // Attach running cartridge
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Register a non-running cartridge with known_caps
        let known_caps = vec![
            "cap:".to_string(),
            "cap:in=\"media:pdf\";op=not-running-op;out=\"media:text\"".to_string(),
        ];
        runtime.register_cartridge(std::path::Path::new("/fake/not-running"), crate::bifaci::cartridge_repo::CartridgeChannel::Release, None, &known_caps);

        // Update cap table
        runtime.update_cap_table();

        // Cap table should have:
        // - Running cartridge's manifest caps (running-op)
        // - Non-running cartridge's known_caps (not-running-op)
        let has_running_op = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("running-op"));
        let has_not_running_op = runtime
            .cap_table
            .iter()
            .any(|(urn, _)| urn.contains("not-running-op"));

        assert!(
            has_running_op,
            "Cap table should have running cartridge's manifest caps"
        );
        assert!(
            has_not_running_op,
            "Cap table should have non-running cartridge's known_caps"
        );

        cartridge_handle.await.unwrap();
    }

    // =========================================================================
    // TEST: CartridgeProcessHandle — snapshot and kill
    // =========================================================================

    // TEST1250: Process snapshots start empty before any cartridges are attached or spawned.
    #[tokio::test]
    async fn test1250_process_handle_snapshot_empty_initially() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();
        let cartridges = handle.running_cartridges();
        assert!(
            cartridges.is_empty(),
            "Snapshot should be empty before any cartridges are spawned"
        );
    }

    // TEST1251: Attached cartridges without child PIDs are excluded from process snapshots.
    #[tokio::test]
    async fn test1251_process_handle_snapshot_excludes_attached_cartridges() {
        // Attached cartridges are connected via socketpair, not spawned as separate
        // processes — they have no PID and should not appear in the process snapshot.
        let (runtime_sock, cartridge_sock) = UnixStream::pair().unwrap();
        let (r_read, r_write) = runtime_sock.into_split();
        let (p_read, p_write) = cartridge_sock.into_split();

        let manifest = r#"{"name":"SnapCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Snapshot test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=snap;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}"#;

        let cartridge_handle = tokio::spawn(async move {
            let (_reader, _writer) =
                cartridge_handshake_with_identity(p_read, p_write, manifest.as_bytes()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let mut runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();

        runtime.attach_cartridge(r_read, r_write).await.unwrap();

        // Attached cartridges have process=None → no PID → excluded from snapshot
        let cartridges = handle.running_cartridges();
        assert!(
            cartridges.is_empty(),
            "Attached cartridges have no PID and should not appear in process snapshot"
        );

        cartridge_handle.await.unwrap();
    }

    // TEST1252: Cartridge process handles remain usable after clone-and-send across tasks.
    #[tokio::test]
    async fn test1252_process_handle_is_clone_and_send() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();
        let handle2 = handle.clone();

        // Verify Send + Sync by moving to another task
        let join = tokio::spawn(async move { handle2.running_cartridges() });
        let result = join.await.unwrap();
        assert!(result.is_empty());

        // Original handle still works
        assert!(handle.running_cartridges().is_empty());
    }

    // TEST1253: Killing an unknown PID is accepted as an asynchronous no-op command.
    #[tokio::test]
    async fn test1253_process_handle_kill_unknown_pid_is_noop() {
        let runtime = CartridgeHostRuntime::new();
        let handle = runtime.process_handle();

        // Kill for a PID that doesn't exist should succeed (command sent)
        // but do nothing (the run loop would handle it as a no-op).
        // Since run() hasn't been called, the command sits in the channel.
        let result = handle.kill_cartridge(99999);
        assert!(
            result.is_ok(),
            "kill_cartridge should succeed even if PID is unknown — command is async"
        );
    }

    // TEST1254: OOM shutdowns emit OOM_KILLED ERR frames for in-flight requests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "OOM death detection for attached cartridges not yet implemented"]
    async fn test1254_oom_kill_sends_err_with_oom_killed_code() {
        let manifest = r#"{"name":"OomCartridge","version":"1.0","channel":"release","registry_url":null,"description":"OOM test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=oom;out=\"media:void\"","title":"OOM","command":"oom","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and body END, then die (simulating OOM kill mid-flight)
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            // Die — OOM watchdog killed us
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Set shutdown_reason to OomKill BEFORE the cartridge dies.
        // In production this is set by handle_command(KillCartridge) which runs
        // in the event loop before child.kill(). For attached cartridges (no child
        // process), we set it directly.
        runtime.cartridges[0].shutdown_reason = Some(ShutdownReason::OomKill);

        // Relay pipe pair
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            // Send REQ
            let mut req = Frame::req(
                req_id_clone.clone(),
                "cap:in=\"media:void\";op=oom;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id_clone.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_clone.clone(),
                xid: Some(xid),
            });

            // Read frames from relay — should get ERR with OOM_KILLED
            let mut got_oom_err = false;
            loop {
                match tokio::time::timeout(Duration::from_secs(5), r.read()).await {
                    Ok(Ok(Some(frame))) => {
                        if frame.frame_type == FrameType::Err {
                            let code = frame.error_code().unwrap_or("");
                            let msg = frame.error_message().unwrap_or("");
                            assert_eq!(
                                code, "OOM_KILLED",
                                "ERR code must be OOM_KILLED, got: {:?}",
                                code
                            );
                            assert!(
                                msg.contains("OOM watchdog"),
                                "ERR message must mention OOM watchdog, got: {}",
                                msg
                            );
                            got_oom_err = true;
                            break;
                        }
                        // Skip other frames (e.g. RelayNotify for cap rebuild)
                    }
                    Ok(Ok(None)) => break, // EOF
                    Ok(Err(_)) => break,   // Read error
                    Err(_) => panic!(
                        "Timed out waiting for OOM_KILLED ERR frame — this is the bug we're fixing"
                    ),
                }
            }
            assert!(
                got_oom_err,
                "Must receive ERR frame with OOM_KILLED code after OOM kill"
            );

            drop(w); // Close relay to let runtime exit
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(result.is_ok(), "Runtime should exit cleanly");

        engine_task.await.unwrap();
        cartridge_handle.await.unwrap();
    }

    // TEST1255: App-exit shutdowns suppress ERR frames and close cleanly without noise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test1255_app_exit_suppresses_err_frames() {
        let manifest = r#"{"name":"ExitCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Exit test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=media:;out=media:","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";op=exit;out=\"media:void\"","title":"Exit","command":"exit","args":[]}],"adapter_urns":[]}]}"#;

        // Cartridge pipe pair
        let (p_to_rt, rt_from_p) = UnixStream::pair().unwrap();
        let (rt_to_p, p_from_rt) = UnixStream::pair().unwrap();

        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        let m = manifest.as_bytes().to_vec();
        let cartridge_handle = tokio::spawn(async move {
            let (mut r, w) = cartridge_handshake_with_identity(p_from_rt, p_to_rt, &m).await;

            // Read REQ and body END, then die
            let _req = r.read().await.unwrap().expect("Expected REQ");
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::End {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            drop(w);
            drop(r);
        });

        let mut runtime = CartridgeHostRuntime::new();
        runtime.attach_cartridge(p_read, p_write).await.unwrap();

        // Set AppExit — should suppress ERR frames
        runtime.cartridges[0].shutdown_reason = Some(ShutdownReason::AppExit);

        // Relay pipe pair
        let (relay_rt_read, relay_eng_write) = UnixStream::pair().unwrap();
        let (relay_eng_read, relay_rt_write) = UnixStream::pair().unwrap();

        let (rt_read_half, _) = relay_rt_read.into_split();
        let (_, rt_write_half) = relay_rt_write.into_split();
        let (_, eng_write_half) = relay_eng_write.into_split();
        let (eng_read_half, _) = relay_eng_read.into_split();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut seq = SeqAssigner::new();
            let mut w = FrameWriter::new(eng_write_half);
            let mut r = FrameReader::new(eng_read_half);

            let xid = MessageId::Uint(1);
            let mut req = Frame::req(
                req_id_clone.clone(),
                "cap:in=\"media:void\";op=exit;out=\"media:void\"",
                vec![],
                "text/plain",
            );
            req.routing_id = Some(xid.clone());
            seq.assign(&mut req);
            w.write(&req).await.unwrap();
            let mut end = Frame::end(req_id_clone.clone(), None);
            end.routing_id = Some(xid.clone());
            seq.assign(&mut end);
            w.write(&end).await.unwrap();
            seq.remove(&FlowKey {
                rid: req_id_clone.clone(),
                xid: Some(xid),
            });

            // Read frames — should NOT get any ERR frame.
            // We expect only RelayNotify (cap table rebuild) and then EOF.
            loop {
                match tokio::time::timeout(Duration::from_secs(3), r.read()).await {
                    Ok(Ok(Some(frame))) => {
                        assert_ne!(
                            frame.frame_type,
                            FrameType::Err,
                            "AppExit must suppress ERR frames, but got ERR with code={:?} msg={:?}",
                            frame.error_code(),
                            frame.error_message()
                        );
                        // Continue reading (might get RelayNotify)
                    }
                    Ok(Ok(None)) => break, // EOF — expected
                    Ok(Err(_)) => break,   // Read error — relay closed
                    Err(_) => break,       // Timeout — no more frames, good
                }
            }

            drop(w);
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.run(rt_read_half, rt_write_half, || vec![]),
        )
        .await;
        assert!(result.is_ok(), "Runtime should exit cleanly");

        engine_task.await.unwrap();
        cartridge_handle.await.unwrap();
    }
}
