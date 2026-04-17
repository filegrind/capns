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

pub mod urn;
pub mod cap;
pub mod media;
pub mod bifaci;
pub mod standard;
pub mod planner;
pub mod orchestrator;
pub mod machine;
pub mod input_resolver;

// URN types
pub use urn::cap_urn::*;
pub use urn::media_urn::*;
pub use urn::cap_matrix::*;

// Cap definitions
pub use cap::definition::*;
pub use cap::validation::*;
pub use cap::schema_validation::{SchemaValidator as JsonSchemaValidator, SchemaValidationError, SchemaResolver, FileSchemaResolver};
pub use cap::registry::*;
pub use cap::caller::{CapArgumentValue, CapCaller, CapResult, CapSet, StdinSource};
pub use cap::response::*;

// Media types
pub use media::spec::*;
pub use media::registry::{MediaUrnRegistry, MediaRegistryError, StoredMediaSpec};
pub use media::profile::{ProfileSchemaRegistry, ProfileSchemaError};

// Standard caps and media
pub use standard::*;

// Bifaci protocol — frames, I/O, runtimes
pub use bifaci::decode_chunk_payload;
pub use bifaci::frame::{Frame, FrameType, MessageId, Limits, FlowKey, SeqAssigner, ReorderBuffer, PROTOCOL_VERSION, DEFAULT_MAX_FRAME, DEFAULT_MAX_CHUNK, DEFAULT_MAX_REORDER_BUFFER};
pub use bifaci::io::{
    CborError, FrameReader, FrameWriter, HandshakeResult,
    encode_frame, decode_frame, read_frame, write_frame,
    handshake, handshake_accept,
    verify_identity,
};
pub use bifaci::manifest::*;
pub use bifaci::cartridge_runtime::{CartridgeRuntime, RuntimeError, FrameSender, PeerInvoker, NoPeerInvoker, CliStreamEmitter, InputStream, InputPackage, OutputStream, ProgressSender, StreamSender, StreamMeta, PeerCall, PeerResponse, PeerResponseItem, StreamError, Request, OpFactory, IdentityOp, DiscardOp, CapacityHandle, WET_KEY_REQUEST, find_stream, find_stream_str, find_stream_meta, require_stream, require_stream_str};

// Re-export ops crate types used by Op-based handlers
pub use ops::{Op, OpMetadata, DryContext, WetContext, OpResult, OpError};
pub use async_trait::async_trait;
pub use bifaci::cartridge_repo::{
    CartridgeRepo, CartridgeRepoError,
    CartridgeCapSummary, CartridgeInfo, CartridgeSuggestion, CartridgeRegistryResponse,
    CartridgePackageInfo, CartridgeBuild, CartridgeVersionData, CartridgeDistributionInfo,
    CartridgeRegistry, CartridgeRegistryEntry,
};

// CartridgeHost is the primary API for host-side cartridge communication (async/tokio-native)
pub use bifaci::host_runtime::{
    CartridgeHostRuntime as CartridgeHost,
    AsyncHostError as HostError,
    CartridgeResponse,
    ResponseChunk,
    StreamingResponse,
};

// Also export with explicit Async prefix for clarity when needed
pub use bifaci::host_runtime::CartridgeHostRuntime;
pub use bifaci::host_runtime::AsyncHostError;

// Cartridge process monitoring
pub use bifaci::host_runtime::{CartridgeProcessInfo, CartridgeProcessHandle, HostCommand};

// Cartridge install metadata
pub use bifaci::cartridge_json::{CartridgeJson, CartridgeJsonError, CartridgeInstallSource, hash_cartridge_directory};

// Relay exports
pub use bifaci::relay::{RelaySlave, RelayMaster};
pub use bifaci::relay_switch::{InstalledCartridgeIdentity, RelaySwitch, RelaySwitchError, MasterHealthStatus};
pub use bifaci::in_process_host::{InProcessCartridgeHost, FrameHandler, ResponseWriter, accumulate_input};

// Planner — planning, discovery, and execution for machines
pub use planner::{
    PlannerError, PlannerResult, CapExecutor, CapSettingsProvider,
    // Shape (cardinality + structure)
    InputCardinality, InputStructure, MediaShape,
    CardinalityCompatibility, CardinalityPattern, StructureCompatibility, ShapeCompatibility,
    CapShapeInfo, StrandShapeAnalysis,
    // Argument binding
    ArgumentBinding, ArgumentBindings, ArgumentResolutionContext, ArgumentSource,
    StrandInput, CapFileMetadata, CapInputFile, ResolvedArgument, SourceEntityType,
    // Collection input
    CapInputCollection, CollectionFile,
    // Execution plan
    MachinePlan, MachineNode, MachinePlanEdge, EdgeType, ExecutionNodeType, MergeStrategy,
    NodeExecutionResult, MachineResult, BodyOutcome, NodeId,
    // Plan builder
    MachinePlanBuilder,
    ArgumentResolution, ArgumentInfo, StepArgumentRequirements, PathArgumentRequirements,
    // Live cap graph (unified path finding)
    LiveCapGraph, LiveMachinePlanEdge, ReachableTargetInfo, StrandStep, Strand,
    // Executor
    MachineExecutor,
};

// Machine notation — typed DAG path identifiers
pub use machine::{
    Machine, MachineAbstractionError, MachineEdge, MachineParseError, MachineRun,
    MachineRunStatus, MachineSyntaxError, MachineStrand, NotationFormat,
    parse_machine, parse_machine_with_node_names, StrandNodeNames,
};

// Orchestrator — machine notation parsing and DAG execution
pub use orchestrator::{
    ParseOrchestrationError, ResolvedEdge, ResolvedGraph,
    parse_machine_to_cap_dag, plan_to_resolved_graph, execute_dag, NodeData, ExecutionError,
    EdgeGroup, CartridgeManager, ExecutionContext, CapProgressFn, ProgressMapper, map_progress,
    split_cbor_array, assemble_cbor_array, split_cbor_sequence, assemble_cbor_sequence, CborUtilError,
};

// InputResolver — unified input resolution with media detection
pub use input_resolver::{
    InputItem, ContentStructure, ResolvedFile, ResolvedInputSet, InputResolverError,
    MediaAdapter, AdapterResult, AdapterSelection, MediaAdapterRegistry,
    ValueAdapter, ValueAdapterResult, ValueAdapterRegistry,
    resolve_input, resolve_inputs, resolve_paths, detect_file,
    discriminate_candidates_by_validation,
};
