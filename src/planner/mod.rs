//! Planner — planning, discovery, and execution for machines
//!
//! This module provides:
//! - **Shape analysis** from media URNs (cardinality + structure)
//! - **Argument binding** and resolution for cap execution
//! - **Execution plan** structures (DAG of caps)
//! - **Plan builder** — path finding and plan construction
//! - **Plan executor** — generic execution engine with pluggable cap backends
//!
//! ## Shape Dimensions
//!
//! Media shapes have two orthogonal dimensions:
//!
//! 1. **Cardinality** - scalar (Single) vs list (Sequence)
//!    - Detected from `list` marker tag
//! 2. **Structure** - opaque vs record
//!    - Detected from `record` marker tag
//!
//! Both machfab (desktop app) and macino (CLI harness) use this same code.

use thiserror::Error;

pub mod argument_binding;
pub mod cardinality;
pub mod collection_input;
pub mod executor;
pub mod live_cap_graph;
pub mod plan;
pub mod plan_builder;

// Re-exports - Shape types (cardinality + structure)
pub use argument_binding::{
    resolve_binding, ArgumentBinding, ArgumentBindings, ArgumentResolutionContext, ArgumentSource,
    CapFileMetadata, CapInputFile, ResolvedArgument, SourceEntityType, StrandInput,
};
pub use cardinality::{
    // Per-cap shape info and chain analysis
    CapShapeInfo,
    CardinalityCompatibility,
    CardinalityPattern,
    // Cardinality dimension
    InputCardinality,
    // Structure dimension
    InputStructure,
    // Combined shape
    MediaShape,
    ShapeCompatibility,
    StrandShapeAnalysis,
    StructureCompatibility,
};
pub use collection_input::{CapInputCollection, CollectionFile};
pub use executor::MachineExecutor;
pub use live_cap_graph::{
    LiveCapGraph, LiveMachinePlanEdge, LiveMachinePlanEdgeType, PathFindingEvent,
    ReachableTargetInfo, Strand, StrandStep, StrandStepType,
};
pub use plan::{
    BodyOutcome, EdgeType, ExecutionNodeType, MachineNode, MachinePlan, MachinePlanEdge,
    MachineResult, MergeStrategy, NodeExecutionResult, NodeId,
};
pub use plan_builder::{
    ArgumentInfo, ArgumentResolution, MachinePlanBuilder, PathArgumentRequirements,
    StepArgumentRequirements,
};

// =============================================================================
// Error Type
// =============================================================================

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Registry error: {0}")]
    RegistryError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

pub type PlannerResult<T> = Result<T, PlannerError>;

// =============================================================================
// CapExecutor Trait
// =============================================================================

/// Abstracts cap invocation so different backends can be plugged in.
///
/// - **machfab** implements via `CapService.execute_cap()` through the relay
/// - **macino** implements by spawning cartridge binaries
#[async_trait::async_trait]
pub trait CapExecutor: Send + Sync {
    /// Execute a cap and return the raw output bytes.
    async fn execute_cap(
        &self,
        cap_urn: &str,
        arguments: &[crate::CapArgumentValue],
        preferred_cap: Option<&str>,
    ) -> PlannerResult<Vec<u8>>;

    /// Check if a cap is available (has a provider).
    async fn has_cap(&self, cap_urn: &str) -> bool;

    /// Get the cap definition from the registry.
    async fn get_cap(&self, cap_urn: &str) -> PlannerResult<crate::Cap>;
}

// =============================================================================
// CapSettingsProvider Trait
// =============================================================================

/// Provides overridden default values for cap arguments.
///
/// The planner resolves arg defaults from cap definitions first,
/// then checks the settings provider for overrides.
///
/// - **machfab** implements via DB adapter (`cap_setting_repo.find_by_cap_urn()`)
/// - **macino** implements via NDJSON file reader
#[async_trait::async_trait]
pub trait CapSettingsProvider: Send + Sync {
    /// Get overridden default values for a cap's arguments.
    /// Keys are media URNs (argument identifiers), values are JSON values.
    async fn get_settings(
        &self,
        cap_urn: &str,
    ) -> PlannerResult<std::collections::HashMap<String, serde_json::Value>>;
}
