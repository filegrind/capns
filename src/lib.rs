//! Cap SDK — URN system, cap definitions, and the Bifaci protocol
//!
//! This library provides:
//!
//! - **URN system** (`urn`): Cap URNs, media URNs, cap matrix
//! - **Cap definitions** (`cap`): Cap types, validation, registry, caller
//! - **Media types** (`media`): Media spec resolution, registry, profile schemas
//! - **Bifaci protocol** (`bifaci`): Binary Frame Cap Invocation — cartridge runtime,
//!   host runtime, relay, relay switch, cartridge repo
//! - **Standard** (`standard`): Standard cap and media URN constants
//!
//! ## Architecture
//!
//! ```text
//! Router:      (RelaySwitch + RelayMaster × N)
//! Host × N:    (RelaySlave + CartridgeHostRuntime)
//! Cartridge × N:  (CartridgeRuntime + handler × N)
//! ```
//!
//! ## Protocol Overview
//!
//! Cartridges communicate via length-prefixed CBOR frames over stdin/stdout:
//!
//! 1. Host sends HELLO, cartridge responds with HELLO (negotiate limits)
//! 2. Host sends REQ frames to invoke caps
//! 3. Cartridge responds with STREAM_START/CHUNK/STREAM_END/END frames
//! 4. Cartridge sends END frame when complete, or ERR on error
//! 5. Cartridge can send LOG frames for progress/status
//! 6. Relay-specific: RelayNotify (slave→master) and RelayState (master→slave)

pub mod bifaci;
pub mod cap;
pub mod input_resolver;
pub mod machine;
pub mod media;
pub mod orchestrator;
pub mod planner;
pub mod standard;
pub mod urn;

// URN types
pub use urn::cap_matrix::*;
pub use urn::cap_urn::*;
pub use urn::media_urn::*;

// Cap definitions
pub use cap::caller::{CapArgumentValue, CapCaller, CapResult, CapSet, StdinSource};
pub use cap::definition::*;
pub use cap::registry::*;
pub use cap::response::*;
pub use cap::schema_validation::{
    FileSchemaResolver, SchemaResolver, SchemaValidationError,
    SchemaValidator as JsonSchemaValidator,
};
pub use cap::validation::*;

// Media types
pub use media::profile::{ProfileSchemaError, ProfileSchemaRegistry};
pub use media::registry::{MediaRegistryError, MediaUrnRegistry, StoredMediaSpec};
pub use media::spec::*;

// Standard caps and media
pub use standard::*;

// Bifaci protocol — frames, I/O, runtimes
pub use bifaci::cartridge_runtime::{
    find_stream, find_stream_meta, find_stream_str, require_stream, require_stream_str,
    AdapterSelectionOp, CapacityHandle, CartridgeRuntime, CliStreamEmitter, DiscardOp, FrameSender,
    IdentityOp,
    InputPackage, InputStream, NoPeerInvoker, OpFactory, OutputStream, PeerCall, PeerInvoker,
    PeerResponse, PeerResponseItem, ProgressSender, Request, RuntimeError, StreamError, StreamMeta,
    StreamSender, WET_KEY_REQUEST,
};
pub use bifaci::decode_chunk_payload;
pub use bifaci::frame::{
    FlowKey, Frame, FrameType, Limits, MessageId, ReorderBuffer, SeqAssigner, DEFAULT_MAX_CHUNK,
    DEFAULT_MAX_FRAME, DEFAULT_MAX_REORDER_BUFFER, PROTOCOL_VERSION,
};
pub use bifaci::io::{
    decode_frame, encode_frame, handshake, handshake_accept, read_frame, verify_identity,
    write_frame, CborError, FrameReader, FrameWriter, HandshakeResult,
};
pub use bifaci::manifest::*;

// Re-export ops crate types used by Op-based handlers
pub use async_trait::async_trait;
pub use bifaci::cartridge_repo::{
    CartridgeBuild, CartridgeCapSummary, CartridgeDistributionInfo, CartridgeInfo,
    CartridgePackageInfo, CartridgeRegistry, CartridgeRegistryEntry, CartridgeRegistryResponse,
    CartridgeRepo, CartridgeRepoError, CartridgeSuggestion, CartridgeVersionData,
};
pub use ops::{DryContext, Op, OpError, OpMetadata, OpResult, WetContext};

// CartridgeHost is the primary API for host-side cartridge communication (async/tokio-native)
pub use bifaci::host_runtime::{
    AsyncHostError as HostError, CartridgeHostRuntime as CartridgeHost, CartridgeResponse,
    ResponseChunk, StreamingResponse,
};

// Also export with explicit Async prefix for clarity when needed
pub use bifaci::host_runtime::AsyncHostError;
pub use bifaci::host_runtime::CartridgeHostRuntime;

// Cartridge process monitoring
pub use bifaci::host_runtime::{CartridgeProcessHandle, CartridgeProcessInfo, HostCommand};

// Cartridge install metadata
pub use bifaci::cartridge_json::{
    hash_cartridge_directory, CartridgeInstallSource, CartridgeJson, CartridgeJsonError,
};

// Relay exports
pub use bifaci::in_process_host::{
    accumulate_input, FrameHandler, InProcessCartridgeHost, ResponseWriter,
};
pub use bifaci::relay::{RelayMaster, RelaySlave};
pub use bifaci::relay_switch::{
    InstalledCartridgeIdentity, MasterHealthStatus, RelayNotifyCapabilitiesPayload, RelaySwitch,
    RelaySwitchError,
};

// Planner — planning, discovery, and execution for machines
pub use planner::{
    // Argument binding
    ArgumentBinding,
    ArgumentBindings,
    ArgumentInfo,
    ArgumentResolution,
    ArgumentResolutionContext,
    ArgumentSource,
    BodyOutcome,
    CapExecutor,
    CapFileMetadata,
    // Collection input
    CapInputCollection,
    CapInputFile,
    CapSettingsProvider,
    CapShapeInfo,
    CardinalityCompatibility,
    CardinalityPattern,
    CollectionFile,
    EdgeType,
    ExecutionNodeType,
    // Shape (cardinality + structure)
    InputCardinality,
    InputStructure,
    // Live cap graph (unified path finding)
    LiveCapGraph,
    LiveMachinePlanEdge,
    // Executor
    MachineExecutor,
    MachineNode,
    // Execution plan
    MachinePlan,
    // Plan builder
    MachinePlanBuilder,
    MachinePlanEdge,
    MachineResult,
    MediaShape,
    MergeStrategy,
    NodeExecutionResult,
    NodeId,
    PathArgumentRequirements,
    PlannerError,
    PlannerResult,
    ReachableTargetInfo,
    ResolvedArgument,
    ShapeCompatibility,
    SourceEntityType,
    StepArgumentRequirements,
    Strand,
    StrandInput,
    StrandShapeAnalysis,
    StrandStep,
    StructureCompatibility,
};

// Machine notation — typed DAG path identifiers
pub use machine::{
    parse_machine, parse_machine_with_node_names, Machine, MachineAbstractionError, MachineEdge,
    MachineParseError, MachineRun, MachineRunStatus, MachineStrand, MachineSyntaxError,
    NotationFormat, StrandNodeNames,
};

// Orchestrator — machine notation parsing and DAG execution
pub use orchestrator::{
    assemble_cbor_array, assemble_cbor_sequence, execute_dag, map_progress,
    parse_machine_to_cap_dag, plan_to_resolved_graph, split_cbor_array, split_cbor_sequence,
    CapProgressFn, CartridgeManager, CborUtilError, EdgeGroup, ExecutionContext, ExecutionError,
    NodeData, ParseOrchestrationError, ProgressMapper, ResolvedEdge, ResolvedGraph,
    // Stream I/O — shared between orchestrator executor and machfab engine
    decode_terminal_output, send_one_stream, unwrap_cbor_value, StreamIoError,
};

// InputResolver — unified input resolution with media detection
pub use input_resolver::{
    detect_file, detect_file_confirmed, detect_file_with_media_registry,
    discriminate_candidates_by_validation, resolve_input, resolve_inputs,
    resolve_inputs_confirmed, resolve_paths, AdapterResult, CartridgeAdapterInvoker,
    ContentStructure, InputItem, InputResolverError, MediaAdapterRegistry, ResolvedFile,
    ResolvedInputSet, ValueAdapter, ValueAdapterRegistry, ValueAdapterResult,
};
