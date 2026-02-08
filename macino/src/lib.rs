//! Macino: DOT Parser with CapNS Orchestration
//!
//! This library parses DOT digraphs and interprets edge labels starting with `cap:`
//! as Cap URNs. It resolves each Cap URN via a CapNS registry, validates the graph,
//! and produces a validated, executable DAG IR.
//!
//! # Example
//!
//! ```ignore
//! use macino::{parse_dot_to_cap_dag, CapRegistry};
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

use capns::{Cap, CapUrn};
use dot_parser::ast::Graph as AstGraph;
use dot_parser::canonical::Graph as CanonicalGraph;
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

///// Implementation for capns::CapRegistry
#[async_trait::async_trait]
impl CapRegistryTrait for capns::CapRegistry {
    async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
        self.get_cap(urn)
            .await
            .map_err(|_e| ParseOrchestrationError::CapNotFound {
                cap_urn: urn.to_string(),
            })
    }
}

// =============================================================================
// Parsing Logic
// =============================================================================

/// Parse a DOT digraph and produce a validated orchestration graph
///
/// # Arguments
///
/// * `dot` - DOT source code
/// * `registry` - Cap registry for resolving Cap URNs
///
/// # Errors
///
/// Returns `ParseOrchestrationError` for any validation failure
pub async fn parse_dot_to_cap_dag(
    dot: &str,
    registry: &dyn CapRegistryTrait,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    // Step 1: Parse DOT
    let ast = AstGraph::read_dot(dot).map_err(|e| {
        ParseOrchestrationError::DotParseFailed(format!("{:?}", e))
    })?;

    // Convert to canonical graph for easy edge iteration
    let graph_name = ast.name.map(|s| s.to_string());
    let canonical: CanonicalGraph<(&str, &str)> = ast.into();

    // Step 2-5: Process edges and resolve caps
    let mut node_media: HashMap<String, String> = HashMap::new();
    let mut resolved_edges = Vec::new();

    for edge in &canonical.edges.set {
        let from = edge.from.to_string();
        let to = edge.to.to_string();

        // Step 3: Extract and validate edge label
        let label = edge
            .attr
            .elems
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("label"))
            .map(|(_, v)| *v)
            .ok_or_else(|| ParseOrchestrationError::EdgeMissingLabel {
                from: from.clone(),
                to: to.clone(),
            })?;

        // DOT parser may return quoted strings - remove outer quotes and unescape
        let label = if label.starts_with('"') && label.ends_with('"') {
            &label[1..label.len() - 1]
        } else {
            label
        };

        // Unescape the label (replace \" with ")
        let label = label.replace("\\\"", "\"");

        // Validate label starts with "cap:"
        if !label.starts_with("cap:") {
            return Err(ParseOrchestrationError::EdgeLabelNotCapUrn {
                from,
                to,
                label: label.to_string(),
            });
        }

        let cap_urn = label.as_str();

        // Step 4: Resolve Cap URN via registry
        let cap = registry.lookup(cap_urn).await?;

        // Parse the cap URN to extract in/out specs
        let parsed_cap_urn = CapUrn::from_string(cap_urn).map_err(|e| {
            ParseOrchestrationError::CapUrnParseError(format!("{:?}", e))
        })?;

        let cap_in_media = parsed_cap_urn.in_spec().to_string();
        let cap_out_media = parsed_cap_urn.out_spec().to_string();

        // Step 5: Derive node media URNs from incident caps
        // Check 'from' node
        if let Some(existing) = node_media.get(&from) {
            if existing != &cap_in_media {
                return Err(ParseOrchestrationError::NodeMediaConflict {
                    node: from.clone(),
                    existing: existing.clone(),
                    required_by_cap: cap_in_media.clone(),
                });
            }
        } else {
            node_media.insert(from.clone(), cap_in_media.clone());
        }

        // Check 'to' node
        if let Some(existing) = node_media.get(&to) {
            if existing != &cap_out_media {
                return Err(ParseOrchestrationError::NodeMediaConflict {
                    node: to.clone(),
                    existing: existing.clone(),
                    required_by_cap: cap_out_media.clone(),
                });
            }
        } else {
            node_media.insert(to.clone(), cap_out_media.clone());
        }

        resolved_edges.push(ResolvedEdge {
            from: from.clone(),
            to: to.clone(),
            cap_urn: cap_urn.to_string(),
            cap,
            in_media: cap_in_media,
            out_media: cap_out_media,
        });
    }

    // Step 6: Optional node attribute validation
    for (node_id, node) in &canonical.nodes.set {
        let node_id = node_id.to_string();

        if let Some(media_attr_raw) = node
            .attr
            .elems
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("media"))
            .map(|(_, v)| *v)
        {
            // Remove quotes and unescape like we do for edge labels
            let media_attr = if media_attr_raw.starts_with('"') && media_attr_raw.ends_with('"') {
                &media_attr_raw[1..media_attr_raw.len() - 1]
            } else {
                media_attr_raw
            };
            let media_attr = media_attr.replace("\\\"", "\"");

            // Validate media attribute if present
            if let Some(existing) = node_media.get(&node_id) {
                if existing != &media_attr {
                    return Err(ParseOrchestrationError::NodeMediaAttrConflict {
                        node: node_id.clone(),
                        existing: existing.clone(),
                        attr_value: media_attr.clone(),
                    });
                }
            } else {
                // Set the mapping if not already derived from caps
                node_media.insert(node_id.clone(), media_attr);
            }
        }
    }

    // Step 7: DAG validation (topological sort to detect cycles)
    validate_dag(&node_media, &resolved_edges)?;

    Ok(ResolvedGraph {
        nodes: node_media,
        edges: resolved_edges,
        graph_name,
    })
}

/// Validate that the graph is a DAG (no cycles)
fn validate_dag(
    nodes: &HashMap<String, String>,
    edges: &[ResolvedEdge],
) -> Result<(), ParseOrchestrationError> {
    // Build adjacency list
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();

    // Initialize all nodes
    for node in nodes.keys() {
        in_degree.insert(node.as_str(), 0);
        adj.insert(node.as_str(), Vec::new());
    }

    // Build graph
    for edge in edges {
        adj.entry(edge.from.as_str())
            .or_insert_with(Vec::new)
            .push(edge.to.as_str());
        *in_degree.entry(edge.to.as_str()).or_insert(0) += 1;
    }

    // Kahn's algorithm for topological sort
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter_map(|(node, &deg)| if deg == 0 { Some(*node) } else { None })
        .collect();

    let mut sorted_count = 0;

    while let Some(node) = queue.pop() {
        sorted_count += 1;

        if let Some(neighbors) = adj.get(node) {
            for &neighbor in neighbors {
                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }
    }

    // If we couldn't sort all nodes, there's a cycle
    if sorted_count != nodes.len() {
        let cycle_nodes: Vec<String> = in_degree
            .iter()
            .filter_map(|(node, &deg)| {
                if deg > 0 {
                    Some(node.to_string())
                } else {
                    None
                }
            })
            .collect();

        return Err(ParseOrchestrationError::NotADag { cycle_nodes });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock registry for testing
    struct MockRegistry {
        caps: HashMap<String, Cap>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                caps: HashMap::new(),
            }
        }

        fn add_cap(&mut self, urn: &str, _in_spec: &str, _out_spec: &str) {
            let cap_urn = CapUrn::from_string(urn).unwrap();
            let cap = Cap {
                urn: cap_urn,
                title: "Test Cap".to_string(),
                cap_description: None,
                metadata: HashMap::new(),
                command: "test".to_string(),
                media_specs: vec![],
                args: vec![],
                output: None,
                metadata_json: None,
                registered_by: None,
            };
            self.caps.insert(urn.to_string(), cap);
        }
    }

    #[async_trait::async_trait]
    impl CapRegistryTrait for MockRegistry {
        async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
            // Normalize the URN for lookup
            let normalized = CapUrn::from_string(urn)
                .map_err(|e| ParseOrchestrationError::CapUrnParseError(format!("{:?}", e)))?
                .to_string();

            self.caps
                .iter()
                .find(|(k, _)| {
                    // Try to normalize both keys and compare
                    if let Ok(k_norm) = CapUrn::from_string(k) {
                        k_norm.to_string() == normalized
                    } else {
                        false
                    }
                })
                .map(|(_, v)| v.clone())
                .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                    cap_urn: urn.to_string(),
                })
        }
    }

    // TEST001: Parse valid simple graph with one edge
    #[tokio::test]
    async fn test001_parse_simple_graph() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(result.is_ok());

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.get("A").unwrap(), "media:pdf;bytes");
        assert_eq!(graph.nodes.get("B").unwrap(), "media:txt;textable");
    }

    // TEST002: Fail on edge missing label
    #[tokio::test]
    async fn test002_fail_missing_label() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B;
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::EdgeMissingLabel { .. })
        ));
    }

    // TEST003: Fail on label not starting with cap:
    #[tokio::test]
    async fn test003_fail_label_not_cap_urn() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B [label="some-other-label"];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::EdgeLabelNotCapUrn { .. })
        ));
    }

    // TEST004: Fail on cap not found in registry
    #[tokio::test]
    async fn test004_fail_cap_not_found() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:unknown\";op=test;out=\"media:unknown\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::CapNotFound { .. })
        ));
    }

    // TEST005: Fail on node media conflict
    #[tokio::test]
    async fn test005_fail_node_media_conflict() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );
        registry.add_cap(
            r#"cap:in="media:md;textable";op=convert;out="media:html;textable""#,
            "media:md;textable",
            "media:html;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
                A -> C [label="cap:in=\"media:md;textable\";op=convert;out=\"media:html;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NodeMediaConflict { .. })
        ));
    }

    // TEST006: Fail on cycle detection
    #[tokio::test]
    async fn test006_fail_cycle_detection() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:txt;textable";op=process;out="media:txt;textable""#,
            "media:txt;textable",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
                B -> C [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
                C -> A [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NotADag { .. })
        ));
    }

    // TEST007: Parse graph with media node attributes
    #[tokio::test]
    async fn test007_parse_with_node_media_attributes() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A [media="media:pdf;bytes"];
                B [media="media:txt;textable"];
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(result.is_ok());
    }

    // TEST008: Fail on conflicting media node attribute
    #[tokio::test]
    async fn test008_fail_conflicting_media_attribute() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A [media="media:md;textable"];
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NodeMediaAttrConflict { .. })
        ));
    }
}

// =============================================================================
// Execution Module
// =============================================================================

pub mod executor;
