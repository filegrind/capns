//! Convert CapExecutionPlan to ResolvedGraph
//!
//! This module bridges the planner's CapExecutionPlan (node-centric) to the
//! orchestrator's ResolvedGraph (edge-centric) format for execution via execute_dag.
//!
//! The planner creates execution plans where caps are nodes with edges representing
//! data flow. The orchestrator expects caps to be edge labels connecting data nodes.
//!
//! Conversion strategy:
//! - InputSlot nodes become source data nodes
//! - Cap nodes become edges from their input source to their output target
//! - Output nodes mark terminal data nodes
//! - ForEach/Collect/Merge/Split nodes are rejected — the caller must decompose
//!   ForEach plans into sub-plans before conversion (see CapExecutionPlan::extract_*)

use std::collections::HashMap;
use crate::planner::{CapExecutionPlan, ExecutionNodeType};
use super::types::{ResolvedEdge, ResolvedGraph, CapRegistryTrait, ParseOrchestrationError};

/// Convert a CapExecutionPlan to a ResolvedGraph for execution.
///
/// This transforms the node-centric plan (where caps are nodes) into the
/// edge-centric graph (where caps are edge labels) that execute_dag expects.
///
/// # Arguments
/// * `plan` - The execution plan from the planner
/// * `registry` - Cap registry for resolving full Cap definitions
///
/// # Returns
/// A ResolvedGraph suitable for execute_dag, or an error if conversion fails
pub async fn plan_to_resolved_graph(
    plan: &CapExecutionPlan,
    registry: &dyn CapRegistryTrait,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    let mut nodes: HashMap<String, String> = HashMap::new();
    let mut resolved_edges: Vec<ResolvedEdge> = Vec::new();

    // First pass: identify all data nodes (InputSlots and cap outputs)
    // and their media URNs
    for (node_id, node) in &plan.nodes {
        match &node.node_type {
            ExecutionNodeType::InputSlot { expected_media_urn, .. } => {
                nodes.insert(node_id.clone(), expected_media_urn.clone());
            }
            ExecutionNodeType::Cap { cap_urn, .. } => {
                // Cap nodes produce output - get the out_spec from the cap URN
                let cap = registry.lookup(cap_urn).await?;
                let out_media = cap.urn.out_spec().to_string();
                // The cap's output is associated with this node's ID
                nodes.insert(node_id.clone(), out_media);
            }
            ExecutionNodeType::Output { source_node, .. } => {
                // Output nodes inherit media from their source
                if let Some(source) = plan.nodes.get(source_node) {
                    if let ExecutionNodeType::Cap { cap_urn, .. } = &source.node_type {
                        let cap = registry.lookup(cap_urn).await?;
                        nodes.insert(node_id.clone(), cap.urn.out_spec().to_string());
                    }
                }
            }
            ExecutionNodeType::WrapInList { list_media_urn, .. } => {
                // WrapInList is a pass-through at execution time — the data flows
                // unchanged, only the type annotation changes. Register the node
                // with the list media URN so downstream edges can find data at it.
                nodes.insert(node_id.clone(), list_media_urn.clone());
            }
            ExecutionNodeType::ForEach { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains ForEach node '{}'. Decompose the plan using \
                         extract_prefix_to/extract_foreach_body/extract_suffix_from \
                         before converting to ResolvedGraph.",
                        node_id
                    ),
                });
            }
            ExecutionNodeType::Collect { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains Collect node '{}'. Decompose the plan using \
                         extract_prefix_to/extract_foreach_body/extract_suffix_from \
                         before converting to ResolvedGraph.",
                        node_id
                    ),
                });
            }
            ExecutionNodeType::Merge { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains Merge node '{}' which is not yet supported for execution.",
                        node_id
                    ),
                });
            }
            ExecutionNodeType::Split { .. } => {
                return Err(ParseOrchestrationError::InvalidGraph {
                    message: format!(
                        "Plan contains Split node '{}' which is not yet supported for execution.",
                        node_id
                    ),
                });
            }
        }
    }

    // Build a map from WrapInList nodes to their input predecessors.
    // WrapInList is a pass-through: data at the predecessor flows through unchanged.
    // When an edge's from_node is a WrapInList, we resolve it to the actual data source.
    let mut wrap_predecessors: HashMap<String, String> = HashMap::new();
    for edge in &plan.edges {
        if let Some(to_node) = plan.nodes.get(&edge.to_node) {
            if matches!(to_node.node_type, ExecutionNodeType::WrapInList { .. }) {
                wrap_predecessors.insert(edge.to_node.clone(), edge.from_node.clone());
            }
        }
    }

    // Second pass: convert edges that lead INTO Cap nodes into ResolvedEdges
    // In CapExecutionPlan, data flows: source_node --edge--> cap_node
    // In ResolvedGraph, this becomes: source_node --cap_edge--> cap_node
    // The cap's output is stored AT the cap node (cap_0, cap_1, etc.)
    for edge in &plan.edges {
        let to_node = plan.nodes.get(&edge.to_node).ok_or_else(|| {
            ParseOrchestrationError::CapNotFound {
                cap_urn: format!("Node '{}' not found in plan", edge.to_node),
            }
        })?;

        // Only create ResolvedEdges for edges that point to Cap nodes
        if let ExecutionNodeType::Cap { cap_urn, .. } = &to_node.node_type {
            let cap = registry.lookup(cap_urn).await?;
            let in_media = cap.urn.in_spec().to_string();
            let out_media = cap.urn.out_spec().to_string();

            // If the source is a WrapInList node, resolve through to the actual
            // data source. WrapInList is transparent — data at the predecessor
            // flows unchanged through it.
            let from = if wrap_predecessors.contains_key(&edge.from_node) {
                wrap_predecessors[&edge.from_node].clone()
            } else {
                edge.from_node.clone()
            };

            // The cap's output is stored at the cap node itself
            // This allows the next edge (cap_0 → cap_1) to find data at cap_0
            resolved_edges.push(ResolvedEdge {
                from,
                to: edge.to_node.clone(),  // Store output at the cap node
                cap_urn: cap_urn.clone(),
                cap: cap.clone(),
                in_media,
                out_media,
            });
        }
    }

    Ok(ResolvedGraph {
        nodes,
        edges: resolved_edges,
        graph_name: Some(plan.name.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{CapExecutionPlan, CapNode, CapEdge, InputCardinality};
    use crate::{Cap, CapUrn};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct MockRegistry {
        caps: Arc<Mutex<HashMap<String, Cap>>>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self { caps: Arc::new(Mutex::new(HashMap::new())) }
        }

        async fn add_cap(&self, urn: &str) {
            let cap_urn = CapUrn::from_string(urn).unwrap();
            let cap = Cap::new(cap_urn, urn.to_string(), "test".to_string());
            self.caps.lock().await.insert(urn.to_string(), cap);
        }
    }

    #[async_trait::async_trait]
    impl CapRegistryTrait for MockRegistry {
        async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
            self.caps.lock().await.get(urn).cloned().ok_or_else(|| {
                ParseOrchestrationError::CapNotFound { cap_urn: urn.to_string() }
            })
        }
    }

    #[tokio::test]
    async fn test_simple_linear_chain_conversion() {
        let registry = MockRegistry::new();
        registry.add_cap("cap:in=media:pdf;op=extract;out=media:text").await;
        registry.add_cap("cap:in=media:text;op=summarize;out=media:summary").await;

        let mut plan = CapExecutionPlan::new("test_chain");

        // Add input slot
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf", InputCardinality::Single));

        // Add two caps in sequence
        plan.add_node(CapNode::cap("cap_0", "cap:in=media:pdf;op=extract;out=media:text"));
        plan.add_node(CapNode::cap("cap_1", "cap:in=media:text;op=summarize;out=media:summary"));

        // Add output
        plan.add_node(CapNode::output("output", "result", "cap_1"));

        // Connect them
        plan.add_edge(CapEdge::direct("input", "cap_0"));
        plan.add_edge(CapEdge::direct("cap_0", "cap_1"));
        plan.add_edge(CapEdge::direct("cap_1", "output"));

        let graph = plan_to_resolved_graph(&plan, &registry).await.unwrap();

        // Edges: input→cap_0 (extract), cap_0→cap_1 (summarize)
        // The output edge (cap_1→output) doesn't generate a ResolvedEdge
        // because output nodes are not Cap nodes
        assert_eq!(graph.edges.len(), 2);

        // First edge: input → cap_0 via extract cap
        assert_eq!(graph.edges[0].from, "input");
        assert_eq!(graph.edges[0].to, "cap_0");  // Output stored at cap_0
        assert_eq!(graph.edges[0].cap_urn, "cap:in=media:pdf;op=extract;out=media:text");

        // Second edge: cap_0 → cap_1 via summarize cap
        assert_eq!(graph.edges[1].from, "cap_0");
        assert_eq!(graph.edges[1].to, "cap_1");  // Output stored at cap_1
        assert_eq!(graph.edges[1].cap_urn, "cap:in=media:text;op=summarize;out=media:summary");
    }

    // TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes
    #[tokio::test]
    async fn test770_rejects_foreach() {
        let registry = MockRegistry::new();
        registry.add_cap("cap:in=media:pdf;op=disbind;out=media:pdf-page").await;
        registry.add_cap("cap:in=media:pdf-page;op=process;out=media:text").await;

        let mut plan = CapExecutionPlan::new("foreach_plan");
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf", InputCardinality::Single));
        plan.add_node(CapNode::cap("cap_0", "cap:in=media:pdf;op=disbind;out=media:pdf-page"));
        plan.add_node(CapNode::for_each("foreach_0", "cap_0", "cap_1", "cap_1"));
        plan.add_node(CapNode::cap("cap_1", "cap:in=media:pdf-page;op=process;out=media:text"));
        plan.add_node(CapNode::output("output", "result", "cap_1"));

        plan.add_edge(CapEdge::direct("input", "cap_0"));
        plan.add_edge(CapEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(CapEdge::iteration("foreach_0", "cap_1"));
        plan.add_edge(CapEdge::direct("cap_1", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("ForEach node"), "Expected ForEach rejection, got: {}", err);
        assert!(err.contains("Decompose"), "Should mention decomposition, got: {}", err);
    }

    // TEST771: plan_to_resolved_graph rejects plans containing Collect nodes
    #[tokio::test]
    async fn test771_rejects_collect() {
        let registry = MockRegistry::new();
        registry.add_cap("cap:in=media:pdf;op=disbind;out=media:pdf-page").await;
        registry.add_cap("cap:in=media:pdf-page;op=process;out=media:text").await;

        let mut plan = CapExecutionPlan::new("collect_plan");
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf", InputCardinality::Single));
        plan.add_node(CapNode::cap("cap_0", "cap:in=media:pdf;op=disbind;out=media:pdf-page"));
        plan.add_node(CapNode::for_each("foreach_0", "cap_0", "cap_1", "cap_1"));
        plan.add_node(CapNode::cap("cap_1", "cap:in=media:pdf-page;op=process;out=media:text"));
        plan.add_node(CapNode::collect("collect_0", vec!["cap_1".to_string()]));
        plan.add_node(CapNode::output("output", "result", "collect_0"));

        plan.add_edge(CapEdge::direct("input", "cap_0"));
        plan.add_edge(CapEdge::direct("cap_0", "foreach_0"));
        plan.add_edge(CapEdge::iteration("foreach_0", "cap_1"));
        plan.add_edge(CapEdge::collection("cap_1", "collect_0"));
        plan.add_edge(CapEdge::direct("collect_0", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Could hit either ForEach or Collect first depending on HashMap iteration order
        assert!(err.contains("ForEach node") || err.contains("Collect node"),
            "Expected ForEach or Collect rejection, got: {}", err);
    }

    // TEST772: Linear plans (no ForEach/Collect) still convert successfully
    #[tokio::test]
    async fn test772_linear_plan_still_works() {
        let registry = MockRegistry::new();
        registry.add_cap("cap:in=media:pdf;op=extract;out=media:text").await;

        let mut plan = CapExecutionPlan::new("linear_plan");
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf", InputCardinality::Single));
        plan.add_node(CapNode::cap("cap_0", "cap:in=media:pdf;op=extract;out=media:text"));
        plan.add_node(CapNode::output("output", "result", "cap_0"));

        plan.add_edge(CapEdge::direct("input", "cap_0"));
        plan.add_edge(CapEdge::direct("cap_0", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_ok(), "Linear plan should still convert: {:?}", result.err());
        assert_eq!(result.unwrap().edges.len(), 1);
    }

    // TEST773: WrapInList nodes are handled as pass-through
    // Plan: input → cap_0 → WrapInList → cap_1 → output
    // The WrapInList is transparent — the resolved edge from WrapInList to cap_1
    // should be rewritten to go from cap_0 to cap_1 directly.
    #[tokio::test]
    async fn test773_wrap_in_list_passthrough() {
        let registry = MockRegistry::new();
        registry.add_cap(r#"cap:in=media:pdf;op=extract;out="media:text;textable""#).await;
        registry.add_cap(r#"cap:in="media:list;text;textable";op=embed;out="media:embedding-vector;record;textable""#).await;

        let mut plan = CapExecutionPlan::new("wrap_plan");
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf", InputCardinality::Single));
        plan.add_node(CapNode::cap("cap_0", r#"cap:in=media:pdf;op=extract;out="media:text;textable""#));
        plan.add_node(CapNode::wrap_in_list("wrap_0", "media:text;textable", "media:list;text;textable"));
        plan.add_node(CapNode::cap("cap_1", r#"cap:in="media:list;text;textable";op=embed;out="media:embedding-vector;record;textable""#));
        plan.add_node(CapNode::output("output", "result", "cap_1"));

        plan.add_edge(CapEdge::direct("input", "cap_0"));
        plan.add_edge(CapEdge::direct("cap_0", "wrap_0"));
        plan.add_edge(CapEdge::direct("wrap_0", "cap_1"));
        plan.add_edge(CapEdge::direct("cap_1", "output"));

        let result = plan_to_resolved_graph(&plan, &registry).await;
        assert!(result.is_ok(), "Plan with WrapInList should convert: {:?}", result.err());

        let graph = result.unwrap();
        // Two resolved edges: input→cap_0, cap_0→cap_1 (WrapInList resolved through)
        assert_eq!(graph.edges.len(), 2, "Expected 2 edges, got {}: {:?}",
            graph.edges.len(), graph.edges.iter().map(|e| format!("{}→{}", e.from, e.to)).collect::<Vec<_>>());

        // First edge: input → cap_0
        assert_eq!(graph.edges[0].from, "input");
        assert_eq!(graph.edges[0].to, "cap_0");

        // Second edge: cap_0 → cap_1 (NOT wrap_0 → cap_1)
        assert_eq!(graph.edges[1].from, "cap_0",
            "WrapInList should be resolved through — edge should come from cap_0, not wrap_0");
        assert_eq!(graph.edges[1].to, "cap_1");

        // has_foreach_or_collect should be false (WrapInList is NOT ForEach/Collect)
        assert!(!plan.has_foreach_or_collect(),
            "Plan with only WrapInList should NOT trigger ForEach execution path");
    }
}
