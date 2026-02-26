//! Cap Execution Plan structures
//!
//! Defines the structured execution plan for cap chains.
//! This plan describes a DAG of caps to execute, with data flow between them.
//!
//! The execution model supports:
//! - Linear chains of caps
//! - Parallel branches (diamond patterns)
//! - Fan-out (ForEach) for sequence -> single transitions
//! - Fan-in (Collect) for gathering results back into sequences

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use super::argument_binding::ArgumentBindings;
use super::cardinality::InputCardinality;
use super::PlannerError;

/// Unique identifier for a node in the execution plan
pub type NodeId = String;

/// Node type in the execution DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "node_type", rename_all = "snake_case")]
pub enum ExecutionNodeType {
    /// Execute a single cap
    Cap {
        /// The cap URN to execute
        cap_urn: String,
        /// Argument bindings for this cap
        #[serde(default)]
        arg_bindings: ArgumentBindings,
        /// Optional preferred cap URN for routing.
        /// When set, the RelaySwitch uses `is_comparable` matching and prefers
        /// the master whose registered cap is equivalent to this URN.
        /// When absent, standard `accepts` + closest-specificity routing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        preferred_cap: Option<String>,
    },

    /// Fan-out: run downstream path for each item in input sequence
    ///
    /// Auto-inserted when sequence feeds into single-input cap.
    /// Creates parallel execution branches for each input item.
    ForEach {
        /// Node that provides the input sequence
        input_node: NodeId,
        /// Entry point of the per-item sub-graph
        body_entry: NodeId,
        /// Exit point of the per-item sub-graph (where results are collected)
        body_exit: NodeId,
    },

    /// Fan-in: collect outputs from parallel ForEach branches into sequence
    ///
    /// Auto-inserted to gather results after fan-out.
    Collect {
        /// Nodes whose outputs should be collected
        input_nodes: Vec<NodeId>,
        /// Output media URN for the collected sequence
        #[serde(skip_serializing_if = "Option::is_none")]
        output_media_urn: Option<String>,
    },

    /// Merge: combine outputs from parallel branches
    ///
    /// User-designed node for combining outputs from parallel paths.
    Merge {
        /// Nodes whose outputs should be merged
        input_nodes: Vec<NodeId>,
        /// Strategy for merging
        merge_strategy: MergeStrategy,
    },

    /// Split: route input to multiple downstream paths
    ///
    /// User-designed node for parallel processing.
    Split {
        /// Node that provides the input
        input_node: NodeId,
        /// Number of output branches
        output_count: usize,
    },

    /// Input slot: entry point for user-provided files
    InputSlot {
        /// Name of this input slot
        slot_name: String,
        /// Expected media URN for inputs
        expected_media_urn: String,
        /// Expected cardinality
        cardinality: InputCardinality,
    },

    /// Output: terminal node marking a result
    Output {
        /// Name of this output
        output_name: String,
        /// Source node for this output
        source_node: NodeId,
    },
}

/// Strategy for merging outputs from parallel branches
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Concatenate all outputs into a sequence
    Concat,
    /// Zip outputs together (requires same length)
    ZipWith,
    /// Take first successful output
    FirstSuccess,
    /// Take all successful outputs
    AllSuccessful,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Concat
    }
}

/// A node in the execution DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapNode {
    /// Unique identifier for this node
    pub id: NodeId,
    /// The type of node and its specific configuration
    pub node_type: ExecutionNodeType,
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl CapNode {
    /// Create a cap execution node
    pub fn cap(id: &str, cap_urn: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                cap_urn: cap_urn.to_string(),
                arg_bindings: ArgumentBindings::new(),
                preferred_cap: None,
            },
            description: None,
        }
    }

    /// Create a cap node with argument bindings
    pub fn cap_with_bindings(id: &str, cap_urn: &str, bindings: ArgumentBindings) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                cap_urn: cap_urn.to_string(),
                arg_bindings: bindings,
                preferred_cap: None,
            },
            description: None,
        }
    }

    /// Create a cap node with argument bindings and routing preference
    pub fn cap_with_preference(id: &str, cap_urn: &str, bindings: ArgumentBindings, preferred_cap: Option<String>) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Cap {
                cap_urn: cap_urn.to_string(),
                arg_bindings: bindings,
                preferred_cap,
            },
            description: None,
        }
    }

    /// Create a ForEach (fan-out) node
    pub fn for_each(id: &str, input_node: &str, body_entry: &str, body_exit: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::ForEach {
                input_node: input_node.to_string(),
                body_entry: body_entry.to_string(),
                body_exit: body_exit.to_string(),
            },
            description: Some("Fan-out: process each item in vector".to_string()),
        }
    }

    /// Create a Collect (fan-in) node
    pub fn collect(id: &str, input_nodes: Vec<String>) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Collect {
                input_nodes,
                output_media_urn: None,
            },
            description: Some("Fan-in: collect results into vector".to_string()),
        }
    }

    /// Create an input slot node
    pub fn input_slot(id: &str, slot_name: &str, media_urn: &str, cardinality: InputCardinality) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::InputSlot {
                slot_name: slot_name.to_string(),
                expected_media_urn: media_urn.to_string(),
                cardinality,
            },
            description: Some(format!("Input: {}", slot_name)),
        }
    }

    /// Create an output node
    pub fn output(id: &str, output_name: &str, source_node: &str) -> Self {
        Self {
            id: id.to_string(),
            node_type: ExecutionNodeType::Output {
                output_name: output_name.to_string(),
                source_node: source_node.to_string(),
            },
            description: Some(format!("Output: {}", output_name)),
        }
    }

    /// Check if this is a cap execution node
    pub fn is_cap(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::Cap { .. })
    }

    /// Check if this is a fan-out node
    pub fn is_fan_out(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::ForEach { .. })
    }

    /// Check if this is a fan-in node
    pub fn is_fan_in(&self) -> bool {
        matches!(self.node_type, ExecutionNodeType::Collect { .. })
    }

    /// Get cap URN if this is a cap node
    pub fn cap_urn(&self) -> Option<&str> {
        match &self.node_type {
            ExecutionNodeType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    /// Get preferred cap URN if this is a cap node with a routing preference
    pub fn preferred_cap(&self) -> Option<&str> {
        match &self.node_type {
            ExecutionNodeType::Cap { preferred_cap, .. } => preferred_cap.as_deref(),
            _ => None,
        }
    }
}

/// Edge type for execution plans
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Direct data flow
    Direct,
    /// Extract field from JSON output
    JsonField { field: String },
    /// Extract via JSONPath
    JsonPath { path: String },
    /// Iteration edge (from ForEach to body)
    Iteration,
    /// Collection edge (from body to Collect)
    Collection,
}

impl Default for EdgeType {
    fn default() -> Self {
        Self::Direct
    }
}

/// An edge in the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapEdge {
    /// Source node
    pub from_node: NodeId,
    /// Target node
    pub to_node: NodeId,
    /// Type of data flow
    #[serde(default)]
    pub edge_type: EdgeType,
}

impl CapEdge {
    /// Create a direct edge
    pub fn direct(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Direct,
        }
    }

    /// Create an iteration edge (ForEach -> body)
    pub fn iteration(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Iteration,
        }
    }

    /// Create a collection edge (body -> Collect)
    pub fn collection(from: &str, to: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::Collection,
        }
    }

    /// Create a JSON field extraction edge
    pub fn json_field(from: &str, to: &str, field: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::JsonField { field: field.to_string() },
        }
    }

    /// Create a JSON path extraction edge
    pub fn json_path(from: &str, to: &str, path: &str) -> Self {
        Self {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: EdgeType::JsonPath { path: path.to_string() },
        }
    }
}

/// The structured execution plan for a cap chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapExecutionPlan {
    /// Human-readable name for this execution plan
    pub name: String,

    /// All nodes in the DAG
    pub nodes: HashMap<NodeId, CapNode>,

    /// Edges describing data flow
    pub edges: Vec<CapEdge>,

    /// Entry point nodes (InputSlots)
    pub entry_nodes: Vec<NodeId>,

    /// Output nodes
    pub output_nodes: Vec<NodeId>,

    /// Plan metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl CapExecutionPlan {
    /// Create an empty plan
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            nodes: HashMap::new(),
            edges: Vec::new(),
            entry_nodes: Vec::new(),
            output_nodes: Vec::new(),
            metadata: None,
        }
    }

    /// Add a node to the plan
    pub fn add_node(&mut self, node: CapNode) {
        let id = node.id.clone();

        // Track entry/output nodes
        match &node.node_type {
            ExecutionNodeType::InputSlot { .. } => {
                self.entry_nodes.push(id.clone());
            }
            ExecutionNodeType::Output { .. } => {
                self.output_nodes.push(id.clone());
            }
            _ => {}
        }

        self.nodes.insert(id, node);
    }

    /// Add an edge to the plan
    pub fn add_edge(&mut self, edge: CapEdge) {
        self.edges.push(edge);
    }

    /// Get a node by ID
    pub fn get_node(&self, id: &str) -> Option<&CapNode> {
        self.nodes.get(id)
    }

    /// Validate the plan structure
    pub fn validate(&self) -> Result<(), PlannerError> {
        // Check all edge references exist
        for edge in &self.edges {
            if !self.nodes.contains_key(&edge.from_node) {
                return Err(PlannerError::Internal(format!(
                    "Edge from_node '{}' not found in plan",
                    edge.from_node
                )));
            }
            if !self.nodes.contains_key(&edge.to_node) {
                return Err(PlannerError::Internal(format!(
                    "Edge to_node '{}' not found in plan",
                    edge.to_node
                )));
            }
        }

        // Check entry nodes exist
        for entry in &self.entry_nodes {
            if !self.nodes.contains_key(entry) {
                return Err(PlannerError::Internal(format!(
                    "Entry node '{}' not found in plan",
                    entry
                )));
            }
        }

        // Check output nodes exist
        for output in &self.output_nodes {
            if !self.nodes.contains_key(output) {
                return Err(PlannerError::Internal(format!(
                    "Output node '{}' not found in plan",
                    output
                )));
            }
        }

        Ok(())
    }

    /// Get topological ordering of nodes
    pub fn topological_order(&self) -> Result<Vec<&CapNode>, PlannerError> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

        for (id, _) in &self.nodes {
            in_degree.entry(id.as_str()).or_insert(0);
            adj.entry(id.as_str()).or_insert_with(Vec::new);
        }

        for edge in &self.edges {
            *in_degree.entry(edge.to_node.as_str()).or_insert(0) += 1;
            adj.entry(edge.from_node.as_str())
                .or_insert_with(Vec::new)
                .push(edge.to_node.as_str());
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut result: Vec<&CapNode> = Vec::new();

        while let Some(node_id) = queue.pop_front() {
            if let Some(node) = self.nodes.get(node_id) {
                result.push(node);
            }

            if let Some(neighbors) = adj.get(node_id) {
                for &neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(neighbor);
                        }
                    }
                }
            }
        }

        if result.len() != self.nodes.len() {
            return Err(PlannerError::Internal(
                "Cycle detected in execution plan".to_string(),
            ));
        }

        Ok(result)
    }

    /// Create a plan for a single cap execution.
    /// `file_path_arg_name` is the name of the argument that receives the input file path.
    pub fn single_cap(cap_urn: &str, input_media: &str, _output_media: &str, file_path_arg_name: &str) -> Self {
        let mut plan = Self::new(&format!("Single cap: {}", cap_urn));

        // Add input slot
        let input_id = "input_slot";
        plan.add_node(CapNode::input_slot(
            input_id,
            "input",
            input_media,
            InputCardinality::Single,
        ));

        // Add cap node
        let cap_id = "cap_0";
        let mut bindings = ArgumentBindings::new();
        bindings.add_file_path(file_path_arg_name);
        plan.add_node(CapNode::cap_with_bindings(cap_id, cap_urn, bindings));
        plan.add_edge(CapEdge::direct(input_id, cap_id));

        // Add output node
        let output_id = "output";
        plan.add_node(CapNode::output(output_id, "result", cap_id));
        plan.add_edge(CapEdge::direct(cap_id, output_id));

        plan
    }

    /// Create a linear chain of caps (each output feeds into next input).
    /// `file_path_arg_names` provides the argument name for each cap in the chain.
    pub fn linear_chain(cap_urns: &[&str], input_media: &str, _output_media: &str, file_path_arg_names: &[&str]) -> Self {
        let mut plan = Self::new("Linear cap chain");

        if cap_urns.is_empty() {
            return plan;
        }

        // Add input slot
        let input_id = "input_slot";
        plan.add_node(CapNode::input_slot(
            input_id,
            "input",
            input_media,
            InputCardinality::Single,
        ));

        let mut prev_id = input_id.to_string();

        // Add cap nodes
        for (i, urn) in cap_urns.iter().enumerate() {
            let cap_id = format!("cap_{}", i);
            let mut bindings = ArgumentBindings::new();
            // Use the corresponding arg name, or skip if not provided
            if let Some(arg_name) = file_path_arg_names.get(i) {
                bindings.add_file_path(arg_name);
            }
            plan.add_node(CapNode::cap_with_bindings(&cap_id, urn, bindings));
            plan.add_edge(CapEdge::direct(&prev_id, &cap_id));
            prev_id = cap_id;
        }

        // Add output node
        let output_id = "output";
        plan.add_node(CapNode::output(output_id, "result", &prev_id));
        plan.add_edge(CapEdge::direct(&prev_id, output_id));

        plan
    }
}

/// Result of executing a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionResult {
    /// The node that was executed
    pub node_id: NodeId,

    /// Whether execution succeeded
    pub success: bool,

    /// Binary output data (if any)
    pub binary_output: Option<Vec<u8>>,

    /// Text/JSON output (if any)
    pub text_output: Option<String>,

    /// Error message if execution failed
    pub error: Option<String>,

    /// Execution duration in milliseconds
    pub duration_ms: u64,
}

/// Overall result of executing a cap chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapChainExecutionResult {
    /// Whether the entire chain executed successfully
    pub success: bool,

    /// Results from each node
    pub node_results: HashMap<NodeId, NodeExecutionResult>,

    /// Outputs from output nodes (structured JSON)
    pub outputs: HashMap<String, serde_json::Value>,

    /// Error message if the chain failed
    pub error: Option<String>,

    /// Total execution duration in milliseconds
    pub total_duration_ms: u64,
}

impl CapChainExecutionResult {
    /// Get the primary output (first output node's result)
    pub fn primary_output(&self) -> Option<&serde_json::Value> {
        self.outputs.values().next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // TEST721: Tests creation of a simple execution plan with a single capability
    // Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node
    #[test]
    fn test721_single_cap_plan() {
        let plan = CapExecutionPlan::single_cap(
            "cap:test",
            "media:pdf",
            "media:png",
            "input_file",  // file-path argument name for this cap
        );
        assert_eq!(plan.nodes.len(), 3); // input_slot, cap, output
        assert_eq!(plan.entry_nodes.len(), 1);
        assert_eq!(plan.output_nodes.len(), 1);
        assert!(plan.validate().is_ok());
    }

    // TEST722: Tests creation of a linear chain of capabilities connected in sequence
    // Verifies that linear_chain() correctly links multiple caps with proper edges and topological order
    #[test]
    fn test722_linear_chain_plan() {
        let plan = CapExecutionPlan::linear_chain(
            &["cap:a", "cap:b", "cap:c"],
            "media:pdf",
            "media:png",
            &["input_a", "input_b", "input_c"],  // file-path argument names for each cap
        );
        assert_eq!(plan.nodes.len(), 5); // input_slot, 3 caps, output
        assert_eq!(plan.edges.len(), 4);
        assert!(plan.validate().is_ok());

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 5);
    }

    // TEST723: Tests creation and validation of an empty execution plan with no nodes
    // Verifies that plans without capabilities are valid and handle zero nodes correctly
    #[test]
    fn test723_empty_plan() {
        let plan = CapExecutionPlan::new("empty");
        assert_eq!(plan.nodes.len(), 0);
        assert!(plan.validate().is_ok());
    }

    // TEST724: Tests storing and retrieving metadata attached to an execution plan
    // Verifies that arbitrary JSON metadata can be associated with a plan for context preservation
    #[test]
    fn test724_plan_with_metadata() {
        let mut plan = CapExecutionPlan::new("test");
        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), json!("pdf"));
        metadata.insert("version".to_string(), json!(1));
        plan.metadata = Some(metadata);

        assert!(plan.metadata.is_some());
        assert_eq!(
            plan.metadata.as_ref().unwrap().get("source"),
            Some(&json!("pdf"))
        );
    }

    // TEST725: Tests plan validation detects edges pointing to non-existent nodes
    // Verifies that validate() returns an error when an edge references a missing to_node
    #[test]
    fn test725_validate_invalid_edge() {
        let mut plan = CapExecutionPlan::new("invalid");
        plan.nodes.insert(
            "node_0".to_string(),
            CapNode::cap("node_0", "cap:test"),
        );
        plan.edges.push(CapEdge::direct("node_0", "nonexistent"));

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Edge to_node 'nonexistent' not found"));
    }

    // TEST726: Tests topological sort correctly orders a diamond-shaped DAG (A→B,C→D)
    // Verifies that nodes with multiple paths respect dependency constraints (A first, D last)
    #[test]
    fn test726_topological_order_diamond() {
        // Diamond: A -> B, A -> C, B -> D, C -> D
        let mut plan = CapExecutionPlan::new("diamond");

        plan.nodes.insert("A".to_string(), CapNode::cap("A", "cap:a"));
        plan.nodes.insert("B".to_string(), CapNode::cap("B", "cap:b"));
        plan.nodes.insert("C".to_string(), CapNode::cap("C", "cap:c"));
        plan.nodes.insert("D".to_string(), CapNode::cap("D", "cap:d"));

        plan.edges.push(CapEdge::direct("A", "B"));
        plan.edges.push(CapEdge::direct("A", "C"));
        plan.edges.push(CapEdge::direct("B", "D"));
        plan.edges.push(CapEdge::direct("C", "D"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 4);

        // A must come first
        assert_eq!(order[0].id, "A");
        // D must come last
        assert_eq!(order[3].id, "D");
    }

    // TEST727: Tests topological sort detects and rejects cyclic dependencies (A→B→C→A)
    // Verifies that circular references produce a "Cycle detected" error
    #[test]
    fn test727_topological_order_detects_cycle() {
        // Cycle: A -> B -> C -> A
        let mut plan = CapExecutionPlan::new("cyclic");

        plan.nodes.insert("A".to_string(), CapNode::cap("A", "cap:a"));
        plan.nodes.insert("B".to_string(), CapNode::cap("B", "cap:b"));
        plan.nodes.insert("C".to_string(), CapNode::cap("C", "cap:c"));

        plan.edges.push(CapEdge::direct("A", "B"));
        plan.edges.push(CapEdge::direct("B", "C"));
        plan.edges.push(CapEdge::direct("C", "A"));

        let result = plan.topological_order();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Cycle detected"));
    }

    // TEST728: Tests CapNode helper methods for identifying node types (cap, fan-out, fan-in)
    // Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types
    #[test]
    fn test728_cap_node_helpers() {
        let cap_node = CapNode::cap("test", "cap:test");
        assert!(cap_node.is_cap());
        assert!(!cap_node.is_fan_out());
        assert!(!cap_node.is_fan_in());
        assert_eq!(cap_node.cap_urn(), Some("cap:test"));

        let foreach_node = CapNode::for_each("foreach", "input", "body", "body");
        assert!(!foreach_node.is_cap());
        assert!(foreach_node.is_fan_out());
        assert!(!foreach_node.is_fan_in());
        assert_eq!(foreach_node.cap_urn(), None);

        let collect_node = CapNode::collect("collect", vec!["a".to_string()]);
        assert!(!collect_node.is_cap());
        assert!(!collect_node.is_fan_out());
        assert!(collect_node.is_fan_in());
    }

    // TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField)
    // Verifies that edge constructors produce correct EdgeType variants
    #[test]
    fn test729_edge_types() {
        let direct = CapEdge::direct("a", "b");
        assert!(matches!(direct.edge_type, EdgeType::Direct));

        let iteration = CapEdge::iteration("foreach", "body");
        assert!(matches!(iteration.edge_type, EdgeType::Iteration));

        let collection = CapEdge::collection("body", "collect");
        assert!(matches!(collection.edge_type, EdgeType::Collection));

        let json_field = CapEdge::json_field("a", "b", "data");
        assert!(matches!(json_field.edge_type, EdgeType::JsonField { field } if field == "data"));
    }

    // TEST730: Tests CapChainExecutionResult structure for successful execution outcomes
    // Verifies that success status, outputs, and primary_output() accessor work correctly
    #[test]
    fn test730_execution_result() {
        let mut outputs = HashMap::new();
        outputs.insert("output".to_string(), json!({"result": "success"}));

        let result = CapChainExecutionResult {
            success: true,
            node_results: HashMap::new(),
            outputs,
            error: None,
            total_duration_ms: 100,
        };

        assert!(result.success);
        assert!(result.primary_output().is_some());
    }

    // TEST731: Tests plan validation detects edges originating from non-existent nodes
    // Verifies that validate() returns an error when an edge references a missing from_node
    #[test]
    fn test731_validate_invalid_from_node() {
        let mut plan = CapExecutionPlan::new("invalid");
        plan.nodes.insert(
            "node_0".to_string(),
            CapNode::cap("node_0", "cap:test"),
        );
        plan.edges.push(CapEdge::direct("nonexistent", "node_0"));

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Edge from_node 'nonexistent' not found"));
    }

    // TEST732: Tests plan validation detects invalid entry node references
    // Verifies that validate() returns an error when entry_nodes contains a non-existent node ID
    #[test]
    fn test732_validate_invalid_entry_node() {
        let mut plan = CapExecutionPlan::new("invalid_entry");
        plan.nodes.insert(
            "cap_0".to_string(),
            CapNode::cap("cap_0", "cap:test"),
        );
        // Manually add invalid entry node reference
        plan.entry_nodes.push("nonexistent_entry".to_string());

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Entry node 'nonexistent_entry' not found"));
    }

    // TEST733: Tests plan validation detects invalid output node references
    // Verifies that validate() returns an error when output_nodes contains a non-existent node ID
    #[test]
    fn test733_validate_invalid_output_node() {
        let mut plan = CapExecutionPlan::new("invalid_output");
        plan.nodes.insert(
            "cap_0".to_string(),
            CapNode::cap("cap_0", "cap:test"),
        );
        // Manually add invalid output node reference
        plan.output_nodes.push("nonexistent_output".to_string());

        let result = plan.validate();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Output node 'nonexistent_output' not found"));
    }

    // TEST734: Tests topological sort detects self-referencing cycles (A→A)
    // Verifies that self-loops are recognized as cycles and produce an error
    #[test]
    fn test734_topological_order_self_loop() {
        // Self-loop: A -> A
        let mut plan = CapExecutionPlan::new("self_loop");

        plan.nodes.insert("A".to_string(), CapNode::cap("A", "cap:a"));
        plan.edges.push(CapEdge::direct("A", "A"));

        let result = plan.topological_order();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Cycle detected"));
    }

    // TEST735: Tests topological sort handles graphs with multiple independent starting nodes
    // Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering
    #[test]
    fn test735_topological_order_multiple_entry_points() {
        // Multiple entry points: A -> C, B -> C, C -> D
        let mut plan = CapExecutionPlan::new("multi_entry");

        plan.nodes.insert("A".to_string(), CapNode::cap("A", "cap:a"));
        plan.nodes.insert("B".to_string(), CapNode::cap("B", "cap:b"));
        plan.nodes.insert("C".to_string(), CapNode::cap("C", "cap:c"));
        plan.nodes.insert("D".to_string(), CapNode::cap("D", "cap:d"));

        plan.edges.push(CapEdge::direct("A", "C"));
        plan.edges.push(CapEdge::direct("B", "C"));
        plan.edges.push(CapEdge::direct("C", "D"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 4);

        // A and B must come before C
        let a_pos = order.iter().position(|n| n.id == "A").unwrap();
        let b_pos = order.iter().position(|n| n.id == "B").unwrap();
        let c_pos = order.iter().position(|n| n.id == "C").unwrap();
        let d_pos = order.iter().position(|n| n.id == "D").unwrap();

        assert!(a_pos < c_pos);
        assert!(b_pos < c_pos);
        assert!(c_pos < d_pos);
    }

    // TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes
    // Verifies that all dependency constraints are satisfied in a graph with multiple converging paths
    #[test]
    fn test736_topological_order_complex_dag() {
        // Complex DAG:
        //   A --> B --> D
        //   |     |     |
        //   v     v     v
        //   C --> E --> F
        let mut plan = CapExecutionPlan::new("complex");

        for name in ["A", "B", "C", "D", "E", "F"] {
            plan.nodes.insert(
                name.to_string(),
                CapNode::cap(name, &format!("cap:{}", name.to_lowercase())),
            );
        }

        plan.edges.push(CapEdge::direct("A", "B"));
        plan.edges.push(CapEdge::direct("A", "C"));
        plan.edges.push(CapEdge::direct("B", "D"));
        plan.edges.push(CapEdge::direct("B", "E"));
        plan.edges.push(CapEdge::direct("C", "E"));
        plan.edges.push(CapEdge::direct("D", "F"));
        plan.edges.push(CapEdge::direct("E", "F"));

        let order = plan.topological_order().unwrap();
        assert_eq!(order.len(), 6);

        // Verify ordering constraints
        let pos = |name: &str| order.iter().position(|n| n.id == name).unwrap();

        // A must be first
        assert_eq!(pos("A"), 0);
        // F must be last
        assert_eq!(pos("F"), 5);
        // B must come before D and E
        assert!(pos("B") < pos("D"));
        assert!(pos("B") < pos("E"));
        // C must come before E
        assert!(pos("C") < pos("E"));
        // D and E must come before F
        assert!(pos("D") < pos("F"));
        assert!(pos("E") < pos("F"));
    }

    // TEST737: Tests linear_chain() with exactly one capability
    // Verifies that a single-element chain produces a valid plan with input_slot, cap, and output
    #[test]
    fn test737_linear_chain_single_cap() {
        let plan = CapExecutionPlan::linear_chain(
            &["cap:only"],
            "media:pdf",
            "media:png",
            &["source_file"],  // file-path argument name
        );
        assert_eq!(plan.nodes.len(), 3); // input_slot, 1 cap, output
        assert_eq!(plan.edges.len(), 2);
        assert!(plan.validate().is_ok());
    }

    // TEST738: Tests linear_chain() with empty capability list
    // Verifies that an empty chain produces a plan with zero nodes and edges
    #[test]
    fn test738_linear_chain_empty() {
        let plan = CapExecutionPlan::linear_chain(
            &[],
            "media:pdf",
            "media:png",
            &[],  // no caps, no arg names
        );
        assert_eq!(plan.nodes.len(), 0);
        assert_eq!(plan.edges.len(), 0);
    }

    // TEST739: Tests NodeExecutionResult structure for successful node execution
    // Verifies that success status, outputs (binary and text), and error fields work correctly
    #[test]
    fn test739_node_execution_result_success() {
        let result = NodeExecutionResult {
            node_id: "node_0".to_string(),
            success: true,
            binary_output: Some(vec![1, 2, 3]),
            text_output: Some("output".to_string()),
            error: None,
            duration_ms: 50,
        };

        assert!(result.success);
        assert!(result.binary_output.is_some());
        assert_eq!(result.error, None);
    }

    // TEST740: Tests NodeExecutionResult structure for failed node execution
    // Verifies that failure status, error message, and absence of outputs are correctly represented
    #[test]
    fn test740_node_execution_result_failure() {
        let result = NodeExecutionResult {
            node_id: "node_0".to_string(),
            success: false,
            binary_output: None,
            text_output: None,
            error: Some("Cap execution failed".to_string()),
            duration_ms: 10,
        };

        assert!(!result.success);
        assert!(result.binary_output.is_none());
        assert_eq!(result.error, Some("Cap execution failed".to_string()));
    }

    // TEST741: Tests CapChainExecutionResult structure for failed chain execution
    // Verifies that failure status, error message, and absence of outputs are correctly represented
    #[test]
    fn test741_execution_result_failure() {
        let result = CapChainExecutionResult {
            success: false,
            node_results: HashMap::new(),
            outputs: HashMap::new(),
            error: Some("Chain failed".to_string()),
            total_duration_ms: 100,
        };

        assert!(!result.success);
        assert_eq!(result.error, Some("Chain failed".to_string()));
        assert!(result.primary_output().is_none());
    }

    // TEST742: Tests EdgeType enum serialization and deserialization to/from JSON
    // Verifies that edge types like Direct and JsonField correctly round-trip through serde_json
    #[test]
    fn test742_edge_type_serialization() {
        let direct = EdgeType::Direct;
        let json = serde_json::to_string(&direct).unwrap();
        assert_eq!(json, "\"direct\"");

        let deserialized: EdgeType = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, EdgeType::Direct));

        let json_field = EdgeType::JsonField { field: "data".to_string() };
        let json = serde_json::to_string(&json_field).unwrap();
        assert!(json.contains("json_field"));
        assert!(json.contains("data"));
    }

    // TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON
    // Verifies that node types like Cap and ForEach correctly serialize with their fields
    #[test]
    fn test743_execution_node_type_serialization() {
        let cap_node = ExecutionNodeType::Cap {
            cap_urn: "cap:test".to_string(),
            arg_bindings: ArgumentBindings::new(),
            preferred_cap: None,
        };
        let json = serde_json::to_string(&cap_node).unwrap();
        assert!(json.contains("cap"));
        assert!(json.contains("cap:test"));

        let foreach_node = ExecutionNodeType::ForEach {
            input_node: "input".to_string(),
            body_entry: "body".to_string(),
            body_exit: "body".to_string(),
        };
        let json = serde_json::to_string(&foreach_node).unwrap();
        assert!(json.contains("for_each"));
    }

    // TEST744: Tests CapExecutionPlan serialization and deserialization to/from JSON
    // Verifies that complete plans with nodes and edges correctly round-trip through JSON
    #[test]
    fn test744_plan_serialization() {
        let plan = CapExecutionPlan::single_cap(
            "cap:test",
            "media:pdf",
            "media:png",
            "input_file",  // file-path argument name
        );

        let json = serde_json::to_string(&plan).unwrap();
        assert!(json.contains("cap:test"));
        assert!(json.contains("input_slot"));
        assert!(json.contains("output"));

        let deserialized: CapExecutionPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.nodes.len(), plan.nodes.len());
        assert_eq!(deserialized.edges.len(), plan.edges.len());
    }

    // TEST745: Tests MergeStrategy enum serialization to JSON
    // Verifies that merge strategies like Concat and ZipWith serialize to correct string values
    #[test]
    fn test745_merge_strategy_serialization() {
        let concat = MergeStrategy::Concat;
        let json = serde_json::to_string(&concat).unwrap();
        assert_eq!(json, "\"concat\"");

        let zip = MergeStrategy::ZipWith;
        let json = serde_json::to_string(&zip).unwrap();
        assert_eq!(json, "\"zip_with\"");
    }

    // TEST746: Tests creation of Output node type that references a source node
    // Verifies that CapNode::output() correctly constructs an Output node with name and source
    #[test]
    fn test746_cap_node_output() {
        let output = CapNode::output("out", "result", "source");
        match &output.node_type {
            ExecutionNodeType::Output { output_name, source_node } => {
                assert_eq!(output_name, "result");
                assert_eq!(source_node, "source");
            }
            _ => panic!("Expected Output node type"),
        }
    }

    // TEST747: Tests creation and validation of Merge node that combines multiple inputs
    // Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans
    #[test]
    fn test747_cap_node_merge() {
        let mut plan = CapExecutionPlan::new("merge_test");

        // Create merge node manually
        let merge_node = CapNode {
            id: "merge".to_string(),
            node_type: ExecutionNodeType::Merge {
                input_nodes: vec!["a".to_string(), "b".to_string()],
                merge_strategy: MergeStrategy::Concat,
            },
            description: Some("Merge outputs".to_string()),
        };

        plan.nodes.insert("a".to_string(), CapNode::cap("a", "cap:a"));
        plan.nodes.insert("b".to_string(), CapNode::cap("b", "cap:b"));
        plan.nodes.insert("merge".to_string(), merge_node);
        plan.edges.push(CapEdge::direct("a", "merge"));
        plan.edges.push(CapEdge::direct("b", "merge"));

        assert!(plan.validate().is_ok());
    }

    // TEST748: Tests creation of Split node that distributes input to multiple outputs
    // Verifies that Split nodes correctly specify an input node and output count
    #[test]
    fn test748_cap_node_split() {
        let split_node = CapNode {
            id: "split".to_string(),
            node_type: ExecutionNodeType::Split {
                input_node: "input".to_string(),
                output_count: 3,
            },
            description: Some("Split input".to_string()),
        };

        match &split_node.node_type {
            ExecutionNodeType::Split { input_node, output_count } => {
                assert_eq!(input_node, "input");
                assert_eq!(*output_count, 3);
            }
            _ => panic!("Expected Split node type"),
        }
    }

    // TEST749: Tests get_node() method for looking up nodes by ID in a plan
    // Verifies that existing nodes are found and non-existent nodes return None
    #[test]
    fn test749_get_node() {
        let plan = CapExecutionPlan::single_cap(
            "cap:test",
            "media:pdf",
            "media:png",
            "doc_path",  // file-path argument name
        );

        assert!(plan.get_node("cap_0").is_some());
        assert!(plan.get_node("input_slot").is_some());
        assert!(plan.get_node("output").is_some());
        assert!(plan.get_node("nonexistent").is_none());
    }
}
