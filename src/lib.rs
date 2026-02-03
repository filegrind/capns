//! Cap SDK - Core cap URN and definition system
//!
//! This library provides the fundamental cap URN system used across
//! all FGND plugins and providers. It defines the formal structure for cap
//! identifiers with flat tag-based naming, wildcard support, and specificity comparison.
//!
//! ## Plugin Communication
//!
//! The library also provides unified plugin communication infrastructure:
//!
//! - **Binary Packet Framing** (`packet`): Length-prefixed binary packets for stdin/stdout
//! - **Message Envelope** (`message`): JSON message types for requests/responses
//! - **Plugin Runtime** (`plugin_runtime`): For plugin binaries - handles all I/O
//! - **Plugin Host** (`plugin_host`): For callers - communicates with plugin processes

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

// Plugin communication infrastructure
pub mod packet;
pub mod message;
pub mod plugin_runtime;
pub mod plugin_host;

pub use cap_urn::*;
pub use media_urn::*;
pub use cap::*;
pub use manifest::*;
pub use validation::*;
pub use schema_validation::{SchemaValidator as JsonSchemaValidator, SchemaValidationError, SchemaResolver, FileSchemaResolver};
pub use registry::*;
pub use media_registry::{MediaUrnRegistry, MediaRegistryError, StoredMediaSpec};
pub use standard::*;
pub use caller::{CapCaller, CapSet, StdinSource};
pub use response::*;
pub use cap_matrix::*;
pub use media_spec::*;
pub use profile_schema_registry::{ProfileSchemaRegistry, ProfileSchemaError};

// Plugin communication exports
pub use packet::{read_packet, write_packet, PacketReader, PacketWriter, PacketError, MAX_PACKET_SIZE};
pub use message::{Message, MessageType, MessageError, ErrorPayload};
pub use plugin_runtime::{PluginRuntime, HandlerResponse, RuntimeError};
pub use plugin_host::{PluginHost, PluginResponse, StreamingResponse, HostError};