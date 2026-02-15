//! Cap SDK — URN system, cap definitions, and the Bifaci protocol
//!
//! This library provides:
//!
//! - **URN system** (`urn`): Cap URNs, media URNs, cap matrix
//! - **Cap definitions** (`cap`): Cap types, validation, registry, caller
//! - **Media types** (`media`): Media spec resolution, registry, profile schemas
//! - **Bifaci protocol** (`bifaci`): Binary Frame Cap Invocation — plugin runtime,
//!   host runtime, relay, relay switch, plugin repo
//! - **Standard** (`standard`): Standard cap and media URN constants
//!
//! ## Architecture
//!
//! ```text
//! Router:      (RelaySwitch + RelayMaster × N)
//! Host × N:    (RelaySlave + PluginHostRuntime)
//! Plugin × N:  (PluginRuntime + handler × N)
//! ```
//!
//! ## Protocol Overview
//!
//! Plugins communicate via length-prefixed CBOR frames over stdin/stdout:
//!
//! 1. Host sends HELLO, plugin responds with HELLO (negotiate limits)
//! 2. Host sends REQ frames to invoke caps
//! 3. Plugin responds with STREAM_START/CHUNK/STREAM_END/END frames
//! 4. Plugin sends END frame when complete, or ERR on error
//! 5. Plugin can send LOG frames for progress/status
//! 6. Relay-specific: RelayNotify (slave→master) and RelayState (master→slave)

pub mod urn;
pub mod cap;
pub mod media;
pub mod bifaci;
pub mod standard;

// URN types
pub use urn::cap_urn::*;
pub use urn::media_urn::*;
pub use urn::cap_matrix::*;

// Cap definitions
pub use cap::definition::*;
pub use cap::validation::*;
pub use cap::schema_validation::{SchemaValidator as JsonSchemaValidator, SchemaValidationError, SchemaResolver, FileSchemaResolver};
pub use cap::registry::*;
pub use cap::caller::{CapArgumentValue, CapCaller, CapSet, StdinSource};
pub use cap::response::*;

// Media types
pub use media::spec::*;
pub use media::registry::{MediaUrnRegistry, MediaRegistryError, StoredMediaSpec};
pub use media::profile::{ProfileSchemaRegistry, ProfileSchemaError};

// Standard caps and media
pub use standard::*;

// Bifaci protocol — frames, I/O, runtimes
pub use bifaci::frame::{Frame, FrameType, MessageId, Limits, FlowKey, SeqAssigner, ReorderBuffer, PROTOCOL_VERSION, DEFAULT_MAX_FRAME, DEFAULT_MAX_CHUNK, DEFAULT_MAX_REORDER_BUFFER};
pub use bifaci::io::{
    CborError, FrameReader, FrameWriter, HandshakeResult,
    encode_frame, decode_frame, read_frame, write_frame,
    handshake, handshake_accept,
    AsyncFrameReader, AsyncFrameWriter, handshake_async,
    read_frame_async, write_frame_async,
    verify_identity,
};
pub use bifaci::manifest::*;
pub use bifaci::plugin_runtime::{PluginRuntime, RuntimeError, FrameSender, PeerInvoker, NoPeerInvoker, CliStreamEmitter, InputStream, InputPackage, OutputStream, PeerCall, StreamError};
pub use bifaci::plugin_repo::{
    PluginRepo, PluginRepoError,
    PluginCapSummary, PluginInfo, PluginSuggestion, PluginRegistryResponse,
    PluginPackageInfo, PluginVersionInfo,
};

// PluginHost is the primary API for host-side plugin communication (async/tokio-native)
pub use bifaci::host_runtime::{
    PluginHostRuntime as PluginHost,
    AsyncHostError as HostError,
    PluginResponse,
    ResponseChunk,
    StreamingResponse,
};

// Also export with explicit Async prefix for clarity when needed
pub use bifaci::host_runtime::PluginHostRuntime;
pub use bifaci::host_runtime::AsyncHostError;

// Relay exports
pub use bifaci::relay::{RelaySlave, RelayMaster, AsyncRelayMaster};
pub use bifaci::relay_switch::{RelaySwitch, RelaySwitchError};
