//! Cap SDK - Core cap URN and definition system
//!
//! This library provides the fundamental cap URN system used across
//! all MACINA plugins and providers. It defines the formal structure for cap
//! identifiers with flat tag-based naming, wildcard support, and specificity comparison.
//!
//! ## Plugin Communication
//!
//! The library provides unified plugin communication infrastructure using CBOR:
//!
//! - **CBOR Frame Types** (`cbor_frame`): Frame definitions with integer keys
//! - **CBOR I/O** (`cbor_io`): Streaming CBOR read/write with handshake
//! - **Plugin Runtime** (`plugin_runtime`): For plugin binaries - handles all I/O
//! - **PluginHost** (`plugin_host_runtime`): For host callers - async communication with plugins
//!
//! ## Architecture
//!
//! ```ignore
//! // Engine side: RelayMaster connects to the runtime via socket
//! let master = AsyncRelayMaster::connect(socket_read, socket_write).await?;
//!
//! // Runtime side: PluginHostRuntime manages multiple plugins
//! let mut runtime = PluginHostRuntime::new();
//! runtime.register_plugin(Path::new("./my-plugin"), &["cap:op=test".into()]);
//! runtime.run(relay_read, relay_write, || vec![]).await?;
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

pub mod cap_urn;
pub mod media_urn;
pub mod cap;
pub mod manifest;
pub mod validation;
pub mod schema_validation;
pub mod registry;
pub mod media_registry;
pub mod standard;
pub mod caller;
pub mod response;
pub mod cap_matrix;
pub mod media_spec;
pub mod profile_schema_registry;

// CBOR-based plugin communication infrastructure
pub mod cbor_frame;
pub mod cbor_io;
pub mod plugin_runtime;
pub mod plugin_repo;
pub mod plugin_host_runtime;
pub mod cap_router;
pub mod plugin_relay;
pub mod relay_switch;

// Integration tests for CBOR protocol
#[cfg(test)]
mod cbor_integration_tests;

pub use cap_urn::*;
pub use media_urn::*;
pub use cap::*;
pub use manifest::*;
pub use validation::*;
pub use schema_validation::{SchemaValidator as JsonSchemaValidator, SchemaValidationError, SchemaResolver, FileSchemaResolver};
pub use registry::*;
pub use media_registry::{MediaUrnRegistry, MediaRegistryError, StoredMediaSpec};
pub use standard::*;
pub use caller::{CapArgumentValue, CapCaller, CapSet, StdinSource};
pub use response::*;
pub use cap_matrix::*;
pub use media_spec::*;
pub use profile_schema_registry::{ProfileSchemaRegistry, ProfileSchemaError};

// CBOR protocol exports
pub use cbor_frame::{Frame, FrameType, MessageId, Limits, PROTOCOL_VERSION, DEFAULT_MAX_FRAME, DEFAULT_MAX_CHUNK};
pub use cbor_io::{
    CborError, FrameReader, FrameWriter, HandshakeResult,
    encode_frame, decode_frame, read_frame, write_frame,
    handshake, handshake_accept,
    AsyncFrameReader, AsyncFrameWriter, handshake_async,
    read_frame_async, write_frame_async,
};
pub use plugin_runtime::{PluginRuntime, RuntimeError, StreamEmitter, FrameSender, PeerInvoker, NoPeerInvoker, CliStreamEmitter};
pub use plugin_repo::{
    PluginRepo, PluginRepoError,
    PluginCapSummary, PluginInfo, PluginSuggestion, PluginRegistryResponse,
    PluginPackageInfo, PluginVersionInfo,
};

// PluginHost is the primary API for host-side plugin communication (async/tokio-native)
pub use plugin_host_runtime::{
    PluginHostRuntime as PluginHost,
    AsyncHostError as HostError,
    PluginResponse,
    ResponseChunk,
    StreamingResponse,
};

// Also export with explicit Async prefix for clarity when needed
pub use plugin_host_runtime::PluginHostRuntime;
pub use plugin_host_runtime::AsyncHostError;

// Relay exports
pub use plugin_relay::{RelaySlave, RelayMaster, AsyncRelayMaster};
pub use relay_switch::{RelaySwitch, RelaySwitchError};
