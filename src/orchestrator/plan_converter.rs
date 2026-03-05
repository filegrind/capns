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
//! - ForEach/Collect patterns are preserved through edge grouping

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
            ExecutionNodeType::ForEach { .. } |
            ExecutionNodeType::Collect { .. } |
            ExecutionNodeType::Merge { .. } |
            ExecutionNodeType::Split { .. } => {
                // These are control-flow nodes; their media URNs are derived from context
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

            // The cap's output is stored at the cap node itself
            // This allows the next edge (cap_0 → cap_1) to find data at cap_0
            resolved_edges.push(ResolvedEdge {
                from: edge.from_node.clone(),
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
    use crate::planner::{CapExecutionPlan, CapNode, CapEdge};
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
            let cap = Cap::new(cap_urn, vec![]);
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
        plan.add_node(CapNode::input_slot("input", "input", "media:pdf"));

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
}
