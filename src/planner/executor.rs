//! Plan Executor — generic execution engine for cap execution plans
//!
//! Executes a structured DAG of caps using a pluggable `CapExecutor` backend.
//! The executor handles:
//! - Structural node dispatch (InputSlot, Output, ForEach, Collect, Merge, Split)
//! - Argument binding resolution
//! - Edge type transforms (JSON field/path extraction)
//! - Cap node execution via the `CapExecutor` trait
//!
//! The `CapExecutor` trait is implemented by:
//! - **macina**: via `CapService.execute_cap()` through the relay
//! - **macino**: by spawning plugin binaries

use std::collections::HashMap;
use std::time::Instant;
use serde_json::json;
use crate::CapArgumentValue;
use super::{
    PlannerError, PlannerResult, CapExecutor, CapSettingsProvider,
    argument_binding::{
        ArgumentBinding, ArgumentResolutionContext, ArgumentSource,
        CapInputFile, resolve_binding,
    },
    plan::{
        CapChainExecutionResult, CapExecutionPlan, CapNode, EdgeType,
        ExecutionNodeType, NodeExecutionResult, NodeId,
    },
};

/// Generic plan executor parameterized by a cap execution backend.
pub struct PlanExecutor<E: CapExecutor> {
    executor: E,
    plan: CapExecutionPlan,
    input_files: Vec<CapInputFile>,
    slot_values: HashMap<String, Vec<u8>>,
    settings_provider: Option<Box<dyn CapSettingsProvider>>,
}

impl<E: CapExecutor> PlanExecutor<E> {
    /// Create a new plan executor.
    pub fn new(
        executor: E,
        plan: CapExecutionPlan,
        input_files: Vec<CapInputFile>,
    ) -> Self {
        Self {
            executor,
            plan,
            input_files,
            slot_values: HashMap::new(),
            settings_provider: None,
        }
    }

    /// Set user-provided slot values for argument binding (raw bytes).
    pub fn with_slot_values(mut self, slot_values: HashMap<String, Vec<u8>>) -> Self {
        self.slot_values = slot_values;
        self
    }

    /// Set the settings provider for cap argument overrides.
    pub fn with_settings_provider(mut self, provider: Box<dyn CapSettingsProvider>) -> Self {
        self.settings_provider = Some(provider);
        self
    }

    /// Execute the plan and return the result.
    pub async fn execute(&self) -> PlannerResult<CapChainExecutionResult> {
        let start = Instant::now();

        self.plan.validate().map_err(|e| PlannerError::Internal(e.to_string()))?;

        let ordered_nodes = self.plan.topological_order()
            .map_err(|e| PlannerError::Internal(e.to_string()))?;

        let mut node_results: HashMap<NodeId, NodeExecutionResult> = HashMap::new();
        let mut node_outputs: HashMap<NodeId, serde_json::Value> = HashMap::new();

        for node in ordered_nodes.iter() {
            let result = self
                .execute_node(node, &node_results, &node_outputs)
                .await;

            match result {
                Ok((exec_result, output)) => {
                    if !exec_result.success {
                        return Ok(CapChainExecutionResult {
                            success: false,
                            node_results,
                            outputs: HashMap::new(),
                            error: Some(format!(
                                "Node '{}' failed: {}",
                                node.id,
                                exec_result.error.as_deref().unwrap_or("unknown error")
                            )),
                            total_duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }

                    if let Some(out) = output {
                        node_outputs.insert(node.id.clone(), out);
                    }
                    node_results.insert(node.id.clone(), exec_result);
                }
                Err(e) => {
                    return Ok(CapChainExecutionResult {
                        success: false,
                        node_results,
                        outputs: HashMap::new(),
                        error: Some(format!("Node '{}' execution error: {}", node.id, e)),
                        total_duration_ms: start.elapsed().as_millis() as u64,
                    });
                }
            }
        }

        let mut outputs: HashMap<String, serde_json::Value> = HashMap::new();
        for output_id in &self.plan.output_nodes {
            if let Some(output) = node_outputs.get(output_id) {
                outputs.insert(output_id.clone(), output.clone());
            }
        }

        Ok(CapChainExecutionResult {
            success: true,
            node_results,
            outputs,
            error: None,
            total_duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Execute a single node.
    async fn execute_node(
        &self,
        node: &CapNode,
        _node_results: &HashMap<NodeId, NodeExecutionResult>,
        node_outputs: &HashMap<NodeId, serde_json::Value>,
    ) -> PlannerResult<(NodeExecutionResult, Option<serde_json::Value>)> {
        let start = Instant::now();

        match &node.node_type {
            ExecutionNodeType::Cap { cap_urn, arg_bindings, preferred_cap } => {
                self.execute_cap_node(
                    &node.id,
                    cap_urn,
                    arg_bindings,
                    preferred_cap.as_deref(),
                    node_outputs,
                )
                .await
            }

            ExecutionNodeType::InputSlot { .. } => {
                let output = if self.input_files.len() == 1 {
                    json!({
                        "file_path": self.input_files[0].file_path,
                        "media_urn": self.input_files[0].media_urn,
                    })
                } else {
                    json!(self.input_files.iter().map(|f| {
                        json!({
                            "file_path": f.file_path,
                            "media_urn": f.media_urn,
                        })
                    }).collect::<Vec<_>>())
                };

                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: Some(output.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    Some(output),
                ))
            }

            ExecutionNodeType::Output { source_node, .. } => {
                let source_output = node_outputs.get(source_node);
                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: source_output.map(|o| o.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    source_output.cloned(),
                ))
            }

            ExecutionNodeType::ForEach { input_node, body_entry, body_exit } => {
                let input = node_outputs.get(input_node);
                let items: Vec<serde_json::Value> = if let Some(input) = input {
                    if let Some(arr) = input.as_array() {
                        arr.clone()
                    } else {
                        vec![input.clone()]
                    }
                } else {
                    vec![]
                };

                let output = json!({
                    "iteration_count": items.len(),
                    "items": items,
                    "body_entry": body_entry,
                    "body_exit": body_exit,
                });

                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: Some(output.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    Some(output),
                ))
            }

            ExecutionNodeType::Collect { input_nodes, .. } => {
                let mut collected: Vec<serde_json::Value> = Vec::new();
                for input_id in input_nodes {
                    if let Some(output) = node_outputs.get(input_id) {
                        if let Some(arr) = output.as_array() {
                            collected.extend(arr.clone());
                        } else {
                            collected.push(output.clone());
                        }
                    }
                }

                let output = json!({
                    "collected": collected,
                    "count": collected.len(),
                });

                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: Some(output.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    Some(output),
                ))
            }

            ExecutionNodeType::Merge { input_nodes, merge_strategy } => {
                let mut merged: Vec<serde_json::Value> = Vec::new();
                for input_id in input_nodes {
                    if let Some(output) = node_outputs.get(input_id) {
                        merged.push(output.clone());
                    }
                }

                let output = json!({
                    "merged": merged,
                    "strategy": format!("{:?}", merge_strategy),
                });

                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: Some(output.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    Some(output),
                ))
            }

            ExecutionNodeType::Split { input_node, output_count } => {
                let input = node_outputs.get(input_node);
                let output = json!({
                    "input": input,
                    "output_count": output_count,
                });

                Ok((
                    NodeExecutionResult {
                        node_id: node.id.clone(),
                        success: true,
                        binary_output: None,
                        text_output: Some(output.to_string()),
                        error: None,
                        duration_ms: start.elapsed().as_millis() as u64,
                    },
                    Some(output),
                ))
            }
        }
    }

    /// Execute a cap node with argument binding resolution.
    async fn execute_cap_node(
        &self,
        node_id: &str,
        cap_urn: &str,
        arg_bindings: &super::argument_binding::ArgumentBindings,
        preferred_cap: Option<&str>,
        node_outputs: &HashMap<NodeId, serde_json::Value>,
    ) -> PlannerResult<(NodeExecutionResult, Option<serde_json::Value>)> {
        let start = Instant::now();

        // Check cap availability
        if !self.executor.has_cap(cap_urn).await {
            return Ok((
                NodeExecutionResult {
                    node_id: node_id.to_string(),
                    success: false,
                    binary_output: None,
                    text_output: None,
                    error: Some(format!("No capability available for '{}'", cap_urn)),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                None,
            ));
        }

        // Get cap definition to resolve argument metadata
        let cap_def = self.executor.get_cap(cap_urn).await?;
        let cap_args = cap_def.get_args();

        let arg_defaults: HashMap<String, serde_json::Value> = cap_args
            .iter()
            .filter_map(|arg| {
                arg.default_value.as_ref().map(|v| (arg.media_urn.clone(), v.clone()))
            })
            .collect();

        let arg_required: HashMap<String, bool> = cap_args
            .iter()
            .map(|arg| (arg.media_urn.clone(), arg.required))
            .collect();

        // Load cap settings from provider
        let cap_settings_map = if let Some(ref provider) = self.settings_provider {
            match provider.get_settings(cap_urn).await {
                Ok(settings) if !settings.is_empty() => {
                    let mut map: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();
                    map.insert(cap_urn.to_string(), settings);
                    map
                }
                _ => HashMap::new(),
            }
        } else {
            HashMap::new()
        };

        // Resolve argument bindings
        let context = ArgumentResolutionContext {
            input_files: &self.input_files,
            current_file_index: 0,
            previous_outputs: node_outputs,
            plan_metadata: self.plan.metadata.as_ref(),
            cap_settings: if cap_settings_map.is_empty() { None } else { Some(&cap_settings_map) },
            slot_values: if self.slot_values.is_empty() { None } else { Some(&self.slot_values) },
        };

        // Build arguments
        let mut arguments: Vec<CapArgumentValue> = Vec::new();

        for (name, binding) in &arg_bindings.bindings {
            let is_required = arg_required.get(name).copied().unwrap_or(false);

            match resolve_binding(binding, &context, cap_urn, arg_defaults.get(name), is_required) {
                Ok(Some(resolved)) => {
                    let arg_media_urn = if resolved.source == ArgumentSource::InputFile {
                        crate::MEDIA_FILE_PATH.to_string()
                    } else {
                        name.clone()
                    };
                    arguments.push(CapArgumentValue::new(arg_media_urn, resolved.value));
                }
                Ok(None) => {
                    // Optional arg with no value - skip
                }
                Err(e) => {
                    return Err(PlannerError::Internal(format!(
                        "Failed to resolve binding '{}' for cap '{}': {}",
                        name, cap_urn, e
                    )));
                }
            }
        }

        // Check if we need to pass file content as stdin argument
        let stdin_arg_already_bound = cap_def.get_args().iter().any(|arg| {
            let has_stdin_source = arg.sources.iter().any(|s| matches!(s, crate::ArgSource::Stdin { .. }));
            has_stdin_source && arg_bindings.bindings.contains_key(&arg.media_urn)
        });
        let has_file_path_binding = arg_bindings.bindings.values().any(|b| {
            matches!(b, ArgumentBinding::InputFilePath)
        });

        if !self.input_files.is_empty() && cap_def.accepts_stdin() && !stdin_arg_already_bound && !has_file_path_binding {
            let input_file = &self.input_files[0];
            let stdin_media_urn = cap_def
                .get_stdin_media_urn()
                .map(|s| s.to_string())
                .unwrap_or_else(|| input_file.media_urn.clone());

            match tokio::fs::read(&input_file.file_path).await {
                Ok(data) => {
                    arguments.push(CapArgumentValue::new(stdin_media_urn, data));
                }
                Err(e) => {
                    return Err(PlannerError::Internal(format!(
                        "Failed to read input file '{}': {}",
                        input_file.file_path, e
                    )));
                }
            }
        }

        // Execute the cap
        let result = self.executor.execute_cap(cap_urn, &arguments, preferred_cap).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(response_bytes) => {
                let text_output = String::from_utf8(response_bytes.clone()).ok();
                let binary_output = Some(response_bytes.clone());

                let output_json = text_output
                    .as_ref()
                    .and_then(|t| serde_json::from_str(t).ok())
                    .unwrap_or_else(|| {
                        json!({ "text": text_output })
                    });

                Ok((
                    NodeExecutionResult {
                        node_id: node_id.to_string(),
                        success: true,
                        binary_output,
                        text_output,
                        error: None,
                        duration_ms,
                    },
                    Some(output_json),
                ))
            }
            Err(e) => Ok((
                NodeExecutionResult {
                    node_id: node_id.to_string(),
                    success: false,
                    binary_output: None,
                    text_output: None,
                    error: Some(e.to_string()),
                    duration_ms,
                },
                None,
            )),
        }
    }
}

/// Apply edge type transformation to extract data from a source output.
pub fn apply_edge_type(
    source_output: &serde_json::Value,
    edge_type: &EdgeType,
) -> PlannerResult<serde_json::Value> {
    match edge_type {
        EdgeType::Direct => Ok(source_output.clone()),

        EdgeType::JsonField { field } => {
            source_output.get(field).cloned().ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Field '{}' not found in source output",
                    field
                ))
            })
        }

        EdgeType::JsonPath { path } => extract_json_path(source_output, path),

        EdgeType::Iteration => Ok(source_output.clone()),
        EdgeType::Collection => Ok(source_output.clone()),
    }
}

/// Extract a value using a simple JSON path expression.
pub fn extract_json_path(
    json: &serde_json::Value,
    path: &str,
) -> PlannerResult<serde_json::Value> {
    let mut current = json.clone();

    for segment in path.split('.') {
        if let Some(bracket_pos) = segment.find('[') {
            let field_name = &segment[..bracket_pos];
            let index_str = &segment[bracket_pos + 1..segment.len() - 1];
            let index: usize = index_str.parse().map_err(|_| {
                PlannerError::Internal(format!("Invalid array index: {}", index_str))
            })?;

            current = current.get(field_name).cloned().ok_or_else(|| {
                PlannerError::Internal(format!("Field '{}' not found in path", field_name))
            })?;

            current = current.get(index).cloned().ok_or_else(|| {
                PlannerError::Internal(format!("Array index {} out of bounds", index))
            })?;
        } else {
            current = current.get(segment).cloned().ok_or_else(|| {
                PlannerError::Internal(format!("Field '{}' not found in path", segment))
            })?;
        }
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan::{CapExecutionPlan, EdgeType};

    #[test]
    fn test_extract_json_path_simple() {
        let json = json!({
            "data": {
                "message": "hello world"
            }
        });
        let result = extract_json_path(&json, "data.message");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!("hello world"));
    }

    #[test]
    fn test_extract_json_path_with_array() {
        let json = json!({
            "items": [
                {"name": "first"},
                {"name": "second"}
            ]
        });
        let result = extract_json_path(&json, "items[0].name");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!("first"));
    }

    #[test]
    fn test_extract_json_path_missing_field() {
        let json = json!({"data": {}});
        let result = extract_json_path(&json, "data.nonexistent");
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Field 'nonexistent' not found"));
    }

    #[test]
    fn test_apply_edge_type_direct() {
        let value = json!({"test": "value"});
        let result = apply_edge_type(&value, &EdgeType::Direct);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_apply_edge_type_json_field() {
        let value = json!({"test": "value", "other": "data"});
        let result = apply_edge_type(&value, &EdgeType::JsonField { field: "test".to_string() });
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!("value"));
    }

    #[test]
    fn test_apply_edge_type_json_field_missing() {
        let value = json!({"test": "value"});
        let result = apply_edge_type(&value, &EdgeType::JsonField { field: "missing".to_string() });
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_edge_type_json_path() {
        let value = json!({"data": {"nested": {"value": 42}}});
        let result = apply_edge_type(&value, &EdgeType::JsonPath { path: "data.nested.value".to_string() });
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!(42));
    }

    #[test]
    fn test_apply_edge_type_iteration() {
        let value = json!([1, 2, 3]);
        let result = apply_edge_type(&value, &EdgeType::Iteration);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_apply_edge_type_collection() {
        let value = json!({"collected": [1, 2, 3]});
        let result = apply_edge_type(&value, &EdgeType::Collection);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_extract_json_path_deeply_nested() {
        let json = json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        });
        let result = extract_json_path(&json, "level1.level2.level3.level4.value");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!("deep"));
    }

    #[test]
    fn test_extract_json_path_array_out_of_bounds() {
        let json = json!({
            "items": [{"name": "first"}]
        });
        let result = extract_json_path(&json, "items[5].name");
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("out of bounds"));
    }

    #[test]
    fn test_extract_json_path_single_segment() {
        let json = json!({"value": 123});
        let result = extract_json_path(&json, "value");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!(123));
    }

    #[test]
    fn test_extract_json_path_with_special_characters() {
        let json = json!({
            "data": {
                "message": "hello \"world\" with 'quotes' and \\ backslashes"
            }
        });
        let result = extract_json_path(&json, "data.message");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!("hello \"world\" with 'quotes' and \\ backslashes"));
    }

    #[test]
    fn test_extract_json_path_with_null_value() {
        let json = json!({"data": {"nullable": null}});
        let result = extract_json_path(&json, "data.nullable");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_extract_json_path_with_empty_array() {
        let json = json!({"data": {"items": []}});
        let result = extract_json_path(&json, "data.items");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!([]));
    }

    #[test]
    fn test_extract_json_path_with_numeric_types() {
        let json = json!({
            "integers": 42,
            "floats": 3.14159,
            "negative": -100,
            "zero": 0
        });
        assert_eq!(extract_json_path(&json, "integers").unwrap(), json!(42));
        assert_eq!(extract_json_path(&json, "floats").unwrap(), json!(3.14159));
        assert_eq!(extract_json_path(&json, "negative").unwrap(), json!(-100));
        assert_eq!(extract_json_path(&json, "zero").unwrap(), json!(0));
    }

    #[test]
    fn test_extract_json_path_with_boolean() {
        let json = json!({
            "flags": {
                "enabled": true,
                "disabled": false
            }
        });
        assert_eq!(extract_json_path(&json, "flags.enabled").unwrap(), json!(true));
        assert_eq!(extract_json_path(&json, "flags.disabled").unwrap(), json!(false));
    }

    #[test]
    fn test_extract_json_path_with_nested_arrays() {
        let json = json!({
            "matrix": [
                [1, 2, 3],
                [4, 5, 6]
            ]
        });
        let result = extract_json_path(&json, "matrix[1]");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!([4, 5, 6]));
    }

    #[test]
    fn test_extract_json_path_invalid_array_index() {
        let json = json!({"items": [1, 2, 3]});
        let result = extract_json_path(&json, "items[abc]");
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Invalid array index"));
    }
}
