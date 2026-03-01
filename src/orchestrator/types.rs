//! Types for DOT parsing and orchestration
//!
//! This module defines the error types and IR structures used by the orchestrator.

use crate::{Cap, InputStructure};
use std::collections::HashMap;
use thiserror::Error;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during DOT parsing and orchestration
#[derive(Debug, Error)]
pub enum ParseOrchestrationError {
    /// DOT parsing failed
    #[error("DOT parse failed: {0}")]
    DotParseFailed(String),

    /// Edge is missing the required 'label' attribute
    #[error("Edge from '{from}' to '{to}' is missing label attribute")]
    EdgeMissingLabel { from: String, to: String },

    /// Edge label does not start with 'cap:'
    #[error("Edge from '{from}' to '{to}' has label '{label}' that does not start with 'cap:'")]
    EdgeLabelNotCapUrn {
        from: String,
        to: String,
        label: String,
    },

    /// Cap URN not found in registry
    #[error("Cap URN '{cap_urn}' not found in registry")]
    CapNotFound { cap_urn: String },

    /// Cap URN is invalid
    #[error("Cap URN '{cap_urn}' is invalid: {details}")]
    CapInvalid { cap_urn: String, details: String },

    /// Node media URN conflicts with existing assignment
    #[error(
        "Node '{node}' has conflicting media URNs: existing='{existing}', required_by_cap='{required_by_cap}'"
    )]
    NodeMediaConflict {
        node: String,
        existing: String,
        required_by_cap: String,
    },

    /// Node media attribute conflicts with derived media URN
    #[error(
        "Node '{node}' has media attribute '{attr_value}' that conflicts with derived media URN '{existing}'"
    )]
    NodeMediaAttrConflict {
        node: String,
        existing: String,
        attr_value: String,
    },

    /// Graph contains a cycle (not a DAG)
    #[error("Graph is not a DAG, contains cycle involving nodes: {cycle_nodes:?}")]
    NotADag { cycle_nodes: Vec<String> },

    /// Cap URN parsing error
    #[error("Failed to parse Cap URN: {0}")]
    CapUrnParseError(String),

    /// Media URN parsing error
    #[error("Failed to parse Media URN: {0}")]
    MediaUrnParseError(String),

    /// Registry error
    #[error("Registry error: {0}")]
    RegistryError(String),

    /// Structure mismatch between connected nodes (record vs opaque)
    #[error(
        "Structure mismatch at node '{node}': source is {source_structure:?} but cap expects {expected_structure:?}"
    )]
    StructureMismatch {
        node: String,
        source_structure: InputStructure,
        expected_structure: InputStructure,
    },
}

// =============================================================================
// IR Structures
// =============================================================================

/// A resolved edge in the orchestration graph
#[derive(Debug, Clone)]
pub struct ResolvedEdge {
    /// Source node DOT ID
    pub from: String,
    /// Target node DOT ID
    pub to: String,
    /// Cap URN string from label
    pub cap_urn: String,
    /// Resolved cap definition
    pub cap: Cap,
    /// Input media URN from cap definition
    pub in_media: String,
    /// Output media URN from cap definition
    pub out_media: String,
}

/// A resolved orchestration graph
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    /// Map from DOT node ID to derived media URN
    pub nodes: HashMap<String, String>,
    /// Resolved edges with cap definitions
    pub edges: Vec<ResolvedEdge>,
    /// Original graph name (if any)
    pub graph_name: Option<String>,
}

// =============================================================================
// Cap Registry Trait
// =============================================================================

/// Trait for Cap registry abstraction
///
/// This allows dependency injection and testing without network access
#[async_trait::async_trait]
pub trait CapRegistryTrait: Send + Sync {
    /// Look up a cap by URN
    async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError>;
}

/// Implementation for capdag::CapRegistry
#[async_trait::async_trait]
impl CapRegistryTrait for crate::CapRegistry {
    async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
        self.get_cap(urn)
            .await
            .map_err(|_e| ParseOrchestrationError::CapNotFound {
                cap_urn: urn.to_string(),
            })
    }
}
