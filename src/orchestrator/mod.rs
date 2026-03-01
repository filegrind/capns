//! Orchestrator: DOT Parser with CapNS Orchestration
//!
//! This module parses DOT digraphs and interprets edge labels starting with `cap:`
//! as Cap URNs. It resolves each Cap URN via a CapNS registry, validates the graph,
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

// Re-export key types
pub use types::{
    ParseOrchestrationError,
    ResolvedEdge,
    ResolvedGraph,
    CapRegistryTrait,
};

pub use parser::parse_dot_to_cap_dag;

pub use executor::{
    ExecutionError,
    NodeData,
    EdgeGroup,
    PluginManager,
    ExecutionContext,
    execute_dag,
};
