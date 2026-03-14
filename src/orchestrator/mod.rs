//! Orchestrator: DOT Parser with CapDag Orchestration
//!
//! This module parses DOT digraphs and interprets edge labels starting with `cap:`
//! as Cap URNs. It resolves each Cap URN via a CapDag registry, validates the graph,
//! and produces a validated, executable DAG IR.
//!
//! # Example
//!
//! ```ignore
//! use capdag::orchestrator::{parse_dot_to_cap_dag, CapRegistryTrait};
//! use capdag::CapRegistry;
//!
//! let dot = r#"
//!     digraph G {
//!         A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
//!     }
//! "#;
//!
//! let registry = CapRegistry::new().await?;
//! let graph = parse_dot_to_cap_dag(dot, &registry).await?;
//! ```

pub mod types;
pub mod validation;
pub mod parser;
pub mod executor;
pub mod plan_converter;
pub mod cbor_util;

// Re-export key types
pub use types::{
    ParseOrchestrationError,
    ResolvedEdge,
    ResolvedGraph,
    CapRegistryTrait,
};

pub use parser::parse_dot_to_cap_dag;

pub use plan_converter::plan_to_resolved_graph;

pub use cbor_util::{split_cbor_array, assemble_cbor_array, split_cbor_sequence, assemble_cbor_sequence, CborUtilError};

pub use executor::{
    ExecutionError,
    NodeData,
    EdgeGroup,
    PluginManager,
    ExecutionContext,
    CapProgressFn,
    ProgressMapper,
    map_progress,
    execute_dag,
};
