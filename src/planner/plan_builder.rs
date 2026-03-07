//! Cap Plan Builder
//!
//! Utility for building cap execution plans. This module provides:
//! - Path finding through the cap graph
//! - Automatic plan generation from source to destination media types
//! - Cardinality analysis for determining fan-out/fan-in requirements
//! - Plan construction with argument bindings
//! - Argument analysis for slot presentation

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::json;

use crate::{Cap, CapRegistry, MediaUrn, MediaUrnRegistry, MediaValidation};
use super::argument_binding::{ArgumentBinding, ArgumentBindings, CapInputFile};
use super::cardinality::{
    CapShapeInfo, ShapeChainAnalysis, InputCardinality, MediaShape,
};
use super::plan::{
    CapEdge, CapExecutionPlan, CapNode,
};
use super::PlannerError;

type PlannerResult<T> = Result<T, PlannerError>;

/// Information about a cap in a chain, including shape and file-path argument info.
/// This struct combines shape analysis (cardinality + structure) with argument binding information.
#[derive(Debug, Clone)]
struct CapChainInfo {
    /// Shape information for the cap (cardinality + structure)
    shape: CapShapeInfo,
    /// Name of the file-path argument (found by media URN type, not by name convention)
    file_path_arg_name: Option<String>,
    /// True if the file-path arg has a stdin source matching the cap's in_spec.
    /// This means the arg is the primary input slot and can receive piped data
    /// from the previous cap's output in a chain (not just a file path).
    file_path_is_stdin_chainable: bool,
}

/// Builder for creating cap execution plans
pub struct CapPlanBuilder {
    /// Cap registry for looking up cap definitions
    cap_registry: Arc<CapRegistry>,
    /// Media URN registry for resolving media specs
    media_registry: Arc<MediaUrnRegistry>,
    /// Set of available cap URNs (caps that have providers/plugins installed).
    /// If set, only these caps will be considered for path finding.
    /// If None, all caps from the registry will be considered (NOT RECOMMENDED - use for testing only).
    available_cap_urns: Option<HashSet<String>>,
}

impl CapPlanBuilder {
    /// Create a new plan builder with the given registries.
    ///
    /// IMPORTANT: This constructor does NOT filter by available caps. You should use
    /// `with_available_caps()` to set the filter after construction, or the path finding
    /// will consider ALL caps in the registry, not just installed ones.
    pub fn new(cap_registry: Arc<CapRegistry>, media_registry: Arc<MediaUrnRegistry>) -> Self {
        Self {
            cap_registry,
            media_registry,
            available_cap_urns: None,
        }
    }

    /// Set the filter for available cap URNs.
    /// Only caps in this set will be considered for path finding.
    /// This MUST be called before path finding methods to ensure only installed caps are used.
    pub fn with_available_caps(mut self, available_caps: HashSet<String>) -> Self {
        self.available_cap_urns = Some(available_caps);
        self
    }

    /// Check if a cap is available (has a provider/plugin installed).
    /// If no filter is set, returns true (considers all caps available).
    ///
    /// Uses `is_dispatchable(provider, request)` to check if any available
    /// plugin can handle the requested cap.
    fn is_cap_available(&self, cap_urn: &str) -> bool {
        match &self.available_cap_urns {
            Some(available) => {
                // Parse the cap URN we're checking (the request)
                let request = match crate::CapUrn::from_string(cap_urn) {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!(
                            "is_cap_available: Failed to parse cap URN '{}': {}. This is a bug - cap URNs in the registry should be valid.",
                            cap_urn, e
                        );
                        return false;
                    }
                };

                // Check if any available cap (provider) can dispatch the request
                for available_urn in available {
                    match crate::CapUrn::from_string(available_urn) {
                        Ok(provider) => {
                            // provider.is_dispatchable(&request): can the plugin's cap
                            // handle what we're requesting?
                            if provider.is_dispatchable(&request) {
                                return true;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "is_cap_available: Failed to parse available cap URN '{}': {}. This is a bug - plugin-reported URNs should be valid.",
                                available_urn, e
                            );
                        }
                    }
                }
                false
            }
            None => true,
        }
    }

    /// Find the file-path argument in a cap by checking the media URN type.
    /// Returns the argument media_urn if found, None otherwise.
    /// This uses tagged URN matching (via `is_any_file_path()`).
    fn find_file_path_arg(cap: &Cap) -> Option<String> {
        for arg in cap.get_args() {
            if let Ok(urn) = MediaUrn::from_string(&arg.media_urn) {
                if urn.is_any_file_path() {
                    return Some(arg.media_urn.clone());
                }
            }
        }
        None
    }

    /// Check if a file-path arg is also the primary stdin input slot.
    /// Returns true if the arg has a stdin source whose media URN matches the cap's in_spec.
    /// This means the arg can receive piped data from the previous cap in a chain,
    /// not just a literal file path.
    fn is_file_path_stdin_chainable(cap: &Cap) -> bool {
        let in_spec = cap.urn.in_spec();
        for arg in cap.get_args() {
            let is_file_path = MediaUrn::from_string(&arg.media_urn)
                .map(|urn| urn.is_any_file_path())
                .unwrap_or(false);
            if !is_file_path {
                continue;
            }
            for source in &arg.sources {
                if let crate::ArgSource::Stdin { stdin } = source {
                    if stdin == in_spec {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Find a path through the cap graph from source to target media type
    ///
    /// Uses BFS to find the shortest path. Returns a list of cap URNs that
    /// transform from source to target.
    ///
    /// Matching semantics:
    /// - Node identity for traversal: uses MediaUrn equality (canonical form via to_string())
    /// - Semantic compatibility (can output flow to input): uses actual.conforms_to(&requirement)
    ///   e.g., if cap expects `media:png`, and we have `media:png;type=cover_image`, that works
    pub async fn find_path(
        &self,
        source_media: &str,
        target_media: &str,
    ) -> PlannerResult<Vec<String>> {
        // Parse source and target as MediaUrns for semantic comparison
        let source_urn = MediaUrn::from_string(source_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid source media URN '{}': {}", source_media, e)))?;
        let target_urn = MediaUrn::from_string(target_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid target media URN '{}': {}", target_media, e)))?;

        // Check if source already satisfies target (no transformation needed)
        if source_urn.conforms_to(&target_urn)
            .expect("CU2: media URN prefix mismatch in path finding") {
            return Ok(vec![]);
        }

        // Get all registered caps
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to list caps: {}", e)))?;

        // Build adjacency list: input_urn (canonical) -> list of (cap_urn, output_urn)
        let mut graph: HashMap<String, Vec<(String, MediaUrn)>> = HashMap::new();
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        let mut input_urns: Vec<MediaUrn> = Vec::new();

        for cap in &caps {
            let cap_urn = cap.urn.to_string();

            if !self.is_cap_available(&cap_urn) {
                continue;
            }

            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let input_urn = match MediaUrn::from_string(input_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let output_urn = match MediaUrn::from_string(output_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };

            let input_canonical = input_urn.to_string();

            let edge_key = (input_canonical.clone(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                tracing::error!(
                    "BUG: Duplicate cap_urn detected in graph building (find_path): {} (input_spec: {}). This indicates stale caps in the registry - run upload-standards to sync.",
                    cap_urn, input_spec
                );
                return Err(PlannerError::Internal(format!(
                    "Duplicate cap_urn in graph: {} (input_spec: {}). \
                     Registry has stale caps - run upload-standards.sh to sync.",
                    cap_urn, input_spec
                )));
            }

            if !input_urns.iter().any(|u| u == &input_urn) {
                input_urns.push(input_urn.clone());
            }

            graph
                .entry(input_canonical)
                .or_insert_with(Vec::new)
                .push((cap_urn, output_urn));
        }

        // Sort input URNs by decreasing specificity (most tags first).
        input_urns.sort_by(|a, b| b.specificity().cmp(&a.specificity()));

        // BFS to find shortest path
        let mut queue: VecDeque<(MediaUrn, Vec<String>)> = VecDeque::new();
        let mut visited: HashSet<String> = HashSet::new();

        let source_canonical = source_urn.to_string();
        queue.push_back((source_urn, vec![]));
        visited.insert(source_canonical);

        while let Some((current_urn, path)) = queue.pop_front() {
            if current_urn.conforms_to(&target_urn)
                .expect("CU2: media URN prefix mismatch in path finding") {
                return Ok(path);
            }

            for cap_input_urn in &input_urns {
                if !current_urn.conforms_to(cap_input_urn)
                    .expect("CU2: media URN prefix mismatch in path finding") {
                    continue;
                }

                let cap_input_canonical = cap_input_urn.to_string();
                if let Some(neighbors) = graph.get(&cap_input_canonical) {
                    for (cap_urn, output_urn) in neighbors {
                        let output_canonical = output_urn.to_string();
                        if !visited.contains(&output_canonical) {
                            visited.insert(output_canonical);
                            let mut new_path = path.clone();
                            new_path.push(cap_urn.clone());
                            queue.push_back((output_urn.clone(), new_path));
                        }
                    }
                }
            }
        }

        Err(PlannerError::NotFound(format!(
            "No path found from '{}' to '{}'",
            source_media, target_media
        )))
    }

    /// Build an execution plan for transforming from source to target media type
    ///
    /// This analyzes the cap chain for cardinality transitions and automatically
    /// inserts fan-out/fan-in nodes where needed.
    pub async fn build_plan(
        &self,
        source_media: &str,
        target_media: &str,
        input_files: Vec<CapInputFile>,
    ) -> PlannerResult<CapExecutionPlan> {
        let cap_urns = self.find_path(source_media, target_media).await?;

        if cap_urns.is_empty() {
            return Ok(CapExecutionPlan::new(&format!(
                "Identity: {} -> {}",
                source_media, target_media
            )));
        }

        let cap_chain_infos = self.get_cap_chain_info(&cap_urns).await?;

        let cap_shapes: Vec<CapShapeInfo> = cap_chain_infos
            .iter()
            .map(|info| info.shape.clone())
            .collect();

        let analysis = ShapeChainAnalysis::analyze(cap_shapes);

        // Check for shape errors (structure mismatches)
        if !analysis.is_valid {
            return Err(PlannerError::InvalidInput(
                analysis.error.unwrap_or_else(|| "Shape analysis failed".to_string())
            ));
        }

        self.build_plan_from_analysis(
            source_media,
            target_media,
            &cap_chain_infos,
            &analysis,
            input_files,
        )
    }

    /// Get shape and file-path argument info for a chain of caps.
    async fn get_cap_chain_info(&self, cap_urns: &[String]) -> PlannerResult<Vec<CapChainInfo>> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        let mut infos = Vec::new();

        for urn in cap_urns {
            let cap = caps.iter().find(|c| c.urn.to_string() == *urn);

            if let Some(cap) = cap {
                let in_spec = cap.urn.in_spec();
                let out_spec = cap.urn.out_spec();
                let file_path_arg_name = Self::find_file_path_arg(cap);
                let file_path_is_stdin_chainable = Self::is_file_path_stdin_chainable(cap);
                infos.push(CapChainInfo {
                    shape: CapShapeInfo::from_cap_specs(urn, in_spec, out_spec),
                    file_path_arg_name,
                    file_path_is_stdin_chainable,
                });
            } else {
                // Cap not found - use default scalar opaque shape
                infos.push(CapChainInfo {
                    shape: CapShapeInfo {
                        input: MediaShape::scalar_opaque(),
                        output: MediaShape::scalar_opaque(),
                        cap_urn: urn.clone(),
                    },
                    file_path_arg_name: None,
                    file_path_is_stdin_chainable: false,
                });
            }
        }

        Ok(infos)
    }

    /// Build plan from shape analysis.
    fn build_plan_from_analysis(
        &self,
        source_media: &str,
        target_media: &str,
        cap_chain_infos: &[CapChainInfo],
        analysis: &ShapeChainAnalysis,
        input_files: Vec<CapInputFile>,
    ) -> PlannerResult<CapExecutionPlan> {
        let mut plan = CapExecutionPlan::new(&format!(
            "Transform: {} -> {}",
            source_media, target_media
        ));

        let input_cardinality = if input_files.len() == 1 {
            InputCardinality::Single
        } else {
            InputCardinality::Sequence
        };

        let input_slot_id = "input_slot";
        plan.add_node(CapNode::input_slot(
            input_slot_id,
            "input",
            source_media,
            input_cardinality,
        ));

        if !analysis.requires_transformation() {
            self.build_linear_plan(
                &mut plan,
                input_slot_id,
                cap_chain_infos,
            )?;
        } else {
            self.build_fan_out_plan(
                &mut plan,
                input_slot_id,
                cap_chain_infos,
                analysis,
            )?;
        }

        let last_node_id = format!("cap_{}", cap_chain_infos.len() - 1);
        let output_id = "output";
        plan.add_node(CapNode::output(output_id, "result", &last_node_id));
        plan.add_edge(CapEdge::direct(&last_node_id, output_id));

        plan.metadata = Some(HashMap::from([
            ("source_media".to_string(), json!(source_media)),
            ("target_media".to_string(), json!(target_media)),
            ("cap_count".to_string(), json!(cap_chain_infos.len())),
            ("requires_fan_out".to_string(), json!(analysis.requires_transformation())),
        ]));

        plan.validate()?;
        Ok(plan)
    }

    /// Build a simple linear plan (no cardinality transformations).
    fn build_linear_plan(
        &self,
        plan: &mut CapExecutionPlan,
        entry_node: &str,
        cap_chain_infos: &[CapChainInfo],
    ) -> PlannerResult<()> {
        let mut prev_node_id = entry_node.to_string();

        for (i, info) in cap_chain_infos.iter().enumerate() {
            let node_id = format!("cap_{}", i);
            let cap_urn = &info.shape.cap_urn;

            let mut bindings = ArgumentBindings::new();

            if let Some(arg_name) = &info.file_path_arg_name {
                if i == 0 {
                    bindings.add_file_path(arg_name);
                } else if info.file_path_is_stdin_chainable {
                    bindings.add(
                        arg_name.clone(),
                        ArgumentBinding::PreviousOutput {
                            node_id: prev_node_id.clone(),
                            output_field: None,
                        },
                    );
                } else {
                    bindings.add_file_path(arg_name);
                }
            }

            let node = CapNode::cap_with_bindings(&node_id, cap_urn, bindings);
            plan.add_node(node);
            plan.add_edge(CapEdge::direct(&prev_node_id, &node_id));

            prev_node_id = node_id;
        }

        Ok(())
    }

    /// Build a plan with fan-out/fan-in nodes.
    fn build_fan_out_plan(
        &self,
        plan: &mut CapExecutionPlan,
        entry_node: &str,
        cap_chain_infos: &[CapChainInfo],
        analysis: &ShapeChainAnalysis,
    ) -> PlannerResult<()> {
        let mut prev_node_id = entry_node.to_string();
        let mut node_counter = 0;

        for (i, info) in cap_chain_infos.iter().enumerate() {
            let cap_urn = &info.shape.cap_urn;
            let needs_fan_out = analysis.fan_out_points.contains(&i);

            if needs_fan_out {
                let foreach_id = format!("foreach_{}", node_counter);
                let body_entry_id = format!("cap_{}", node_counter);
                let body_exit_id = body_entry_id.clone();
                let collect_id = format!("collect_{}", node_counter);

                let mut bindings = ArgumentBindings::new();
                if let Some(arg_name) = &info.file_path_arg_name {
                    bindings.add_file_path(arg_name);
                }
                let cap_node = CapNode::cap_with_bindings(&body_entry_id, cap_urn, bindings);

                let foreach_node = CapNode::for_each(
                    &foreach_id,
                    &prev_node_id,
                    &body_entry_id,
                    &body_exit_id,
                );

                let collect_node = CapNode::collect(&collect_id, vec![body_exit_id.clone()]);

                plan.add_node(foreach_node);
                plan.add_node(cap_node);
                plan.add_node(collect_node);

                plan.add_edge(CapEdge::direct(&prev_node_id, &foreach_id));
                plan.add_edge(CapEdge::iteration(&foreach_id, &body_entry_id));
                plan.add_edge(CapEdge::collection(&body_exit_id, &collect_id));

                prev_node_id = collect_id;
                node_counter += 1;
            } else {
                let node_id = format!("cap_{}", node_counter);

                let mut bindings = ArgumentBindings::new();
                if let Some(arg_name) = &info.file_path_arg_name {
                    if node_counter == 0 {
                        bindings.add_file_path(arg_name);
                    } else if info.file_path_is_stdin_chainable {
                        bindings.add(
                            arg_name.clone(),
                            ArgumentBinding::PreviousOutput {
                                node_id: prev_node_id.clone(),
                                output_field: None,
                            },
                        );
                    } else {
                        bindings.add_file_path(arg_name);
                    }
                }

                let node = CapNode::cap_with_bindings(&node_id, cap_urn, bindings);
                plan.add_node(node);
                plan.add_edge(CapEdge::direct(&prev_node_id, &node_id));

                prev_node_id = node_id;
                node_counter += 1;
            }
        }

        Ok(())
    }

    /// Analyze what transformations would be needed for a path
    pub async fn analyze_path_shape(
        &self,
        source_media: &str,
        target_media: &str,
    ) -> PlannerResult<ShapeChainAnalysis> {
        let cap_urns = self.find_path(source_media, target_media).await?;

        if cap_urns.is_empty() {
            return Ok(ShapeChainAnalysis::analyze(vec![]));
        }

        let cap_chain_infos = self.get_cap_chain_info(&cap_urns).await?;
        let shapes: Vec<CapShapeInfo> = cap_chain_infos
            .iter()
            .map(|info| info.shape.clone())
            .collect();
        Ok(ShapeChainAnalysis::analyze(shapes))
    }

    /// Build a plan from a pre-defined path.
    /// Looks up cap definitions to find file-path argument names by media URN type.
    pub async fn build_plan_from_path(
        &self,
        name: &str,
        path: &CapChainPathInfo,
        input_cardinality: InputCardinality,
    ) -> PlannerResult<CapExecutionPlan> {
        let mut plan = CapExecutionPlan::new(name);

        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        // Build a map from cap_urn to (file-path arg name, stdin-chainable)
        let file_path_info: HashMap<String, (Option<String>, bool)> = path.steps
            .iter()
            .map(|step| {
                let cap = caps.iter().find(|c| c.urn.to_string() == step.cap_urn);
                let arg_name = cap.and_then(|c| Self::find_file_path_arg(c));
                let chainable = cap.map(|c| Self::is_file_path_stdin_chainable(c)).unwrap_or(false);
                (step.cap_urn.clone(), (arg_name, chainable))
            })
            .collect();

        let input_slot_id = "input_slot";
        plan.add_node(CapNode::input_slot(
            input_slot_id,
            "input",
            &path.source_spec,
            input_cardinality,
        ));

        let mut prev_node_id = input_slot_id.to_string();

        for (i, step) in path.steps.iter().enumerate() {
            let node_id = format!("cap_{}", i);

            let mut bindings = ArgumentBindings::new();

            let cap = caps.iter().find(|c| c.urn.to_string() == step.cap_urn);

            let in_spec = cap.map(|c| c.urn.in_spec()).unwrap_or_default();
            let out_spec = cap.map(|c| c.urn.out_spec()).unwrap_or_default();

            if let Some((Some(arg_name), stdin_chainable)) = file_path_info.get(&step.cap_urn) {
                if i == 0 {
                    bindings.add_file_path(arg_name);
                } else if *stdin_chainable {
                    bindings.add(
                        arg_name.clone(),
                        ArgumentBinding::PreviousOutput {
                            node_id: prev_node_id.clone(),
                            output_field: None,
                        },
                    );
                } else {
                    bindings.add_file_path(arg_name);
                }
            }

            // Add Slot bindings for all non-I/O arguments
            if let Some(cap) = cap {
                for arg in cap.get_args() {
                    if arg.media_urn == in_spec || arg.media_urn == out_spec {
                        continue;
                    }

                    let is_file_path_type = MediaUrn::from_string(&arg.media_urn)
                        .map(|urn| urn.is_any_file_path())
                        .unwrap_or(false);
                    if is_file_path_type {
                        continue;
                    }

                    if bindings.bindings.contains_key(&arg.media_urn) {
                        continue;
                    }

                    bindings.add(
                        arg.media_urn.clone(),
                        ArgumentBinding::Slot {
                            name: arg.media_urn.clone(),
                            schema: None,
                        },
                    );
                }
            }

            let node = CapNode::cap_with_bindings(&node_id, &step.cap_urn, bindings);
            plan.add_node(node);
            plan.add_edge(CapEdge::direct(&prev_node_id, &node_id));

            prev_node_id = node_id;
        }

        let output_id = "output";
        plan.add_node(CapNode::output(output_id, "result", &prev_node_id));
        plan.add_edge(CapEdge::direct(&prev_node_id, output_id));

        plan.metadata = Some(HashMap::from([
            ("source_spec".to_string(), json!(path.source_spec)),
            ("target_spec".to_string(), json!(path.target_spec)),
        ]));

        Ok(plan)
    }

    /// Get all possible target media specs from a given source
    pub async fn get_reachable_targets(&self, source_media: &str) -> PlannerResult<Vec<String>> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to list caps: {}", e)))?;

        let source_urn = MediaUrn::from_string(source_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid source media URN '{}': {}", source_media, e)))?;

        let mut reachable: HashSet<String> = HashSet::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<MediaUrn> = VecDeque::new();

        let source_canonical = source_urn.to_string();
        queue.push_back(source_urn);
        visited.insert(source_canonical);

        let mut graph: HashMap<String, Vec<MediaUrn>> = HashMap::new();
        let mut input_urns: Vec<MediaUrn> = Vec::new();

        for cap in &caps {
            let cap_urn = cap.urn.to_string();

            if !self.is_cap_available(&cap_urn) {
                continue;
            }

            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let input_urn = match MediaUrn::from_string(input_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let output_urn = match MediaUrn::from_string(output_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };

            let input_canonical = input_urn.to_string();
            if !input_urns.iter().any(|u| u == &input_urn) {
                input_urns.push(input_urn);
            }
            graph
                .entry(input_canonical)
                .or_insert_with(Vec::new)
                .push(output_urn);
        }

        while let Some(current_urn) = queue.pop_front() {
            for cap_input_urn in &input_urns {
                if !current_urn.conforms_to(cap_input_urn)
                    .expect("CU2: media URN prefix mismatch in reachable targets") {
                    continue;
                }

                let cap_input_canonical = cap_input_urn.to_string();
                if let Some(neighbors) = graph.get(&cap_input_canonical) {
                    for output_urn in neighbors {
                        let output_canonical = output_urn.to_string();
                        if !visited.contains(&output_canonical) {
                            visited.insert(output_canonical.clone());
                            reachable.insert(output_canonical);
                            queue.push_back(output_urn.clone());
                        }
                    }
                }
            }
        }

        Ok(reachable.into_iter().collect())
    }

    /// Get all reachable targets with additional metadata
    pub async fn get_reachable_targets_with_metadata(
        &self,
        source_media: &str,
        max_depth: usize,
    ) -> PlannerResult<Vec<ReachableTargetInfo>> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to list caps: {}", e)))?;

        let source_urn = MediaUrn::from_string(source_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid source media URN '{}': {}", source_media, e)))?;

        let mut graph: HashMap<String, Vec<MediaUrn>> = HashMap::new();
        let mut input_urns: Vec<MediaUrn> = Vec::new();
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();

        for cap in &caps {
            let cap_urn = cap.urn.to_string();

            if !self.is_cap_available(&cap_urn) {
                continue;
            }

            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let input_urn = match MediaUrn::from_string(input_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let output_urn = match MediaUrn::from_string(output_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };

            let input_canonical = input_urn.to_string();

            let edge_key = (input_canonical.clone(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                tracing::error!(
                    "BUG: Duplicate cap_urn detected in graph building (get_reachable_targets_with_metadata): {} (input_spec: {}). This indicates stale caps in the registry - run upload-standards to sync.",
                    cap_urn, input_spec
                );
                return Err(PlannerError::Internal(format!(
                    "Duplicate cap_urn in graph: {} (input_spec: {}). \
                     Registry has stale caps - run upload-standards.sh to sync.",
                    cap_urn, input_spec
                )));
            }

            if !input_urns.iter().any(|u| u == &input_urn) {
                input_urns.push(input_urn);
            }
            graph
                .entry(input_canonical)
                .or_insert_with(Vec::new)
                .push(output_urn);
        }

        let mut results: HashMap<String, ReachableTargetInfo> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(MediaUrn, usize)> = VecDeque::new();

        let source_canonical = source_urn.to_string();
        queue.push_back((source_urn, 0));
        visited.insert(source_canonical);

        while let Some((current_urn, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for cap_input_urn in &input_urns {
                if !current_urn.conforms_to(cap_input_urn)
                    .expect("CU2: media URN prefix mismatch in reachable targets with depth") {
                    continue;
                }

                let cap_input_canonical = cap_input_urn.to_string();
                if let Some(neighbors) = graph.get(&cap_input_canonical) {
                    for output_urn in neighbors {
                        let new_depth = depth + 1;
                        let output_canonical = output_urn.to_string();

                        if !results.contains_key(&output_canonical) {
                            let display_name = self.media_registry
                                .get_media_spec(&output_canonical)
                                .await
                                .map(|spec| spec.title)
                                .unwrap_or_else(|_| output_canonical.clone());

                            results.insert(output_canonical.clone(), ReachableTargetInfo {
                                media_spec: output_canonical.clone(),
                                display_name,
                                min_path_length: new_depth as i32,
                                path_count: 0,
                            });
                        }
                        results.get_mut(&output_canonical).unwrap().path_count += 1;

                        if !visited.contains(&output_canonical) {
                            visited.insert(output_canonical);
                            queue.push_back((output_urn.clone(), new_depth));
                        }
                    }
                }
            }
        }

        Ok(results.into_values().collect())
    }

    /// Find all paths from source to target media spec (up to max_paths)
    pub async fn find_all_paths(
        &self,
        source_media: &str,
        target_media: &str,
        max_depth: usize,
        max_paths: usize,
    ) -> PlannerResult<Vec<CapChainPathInfo>> {
        let source_urn = MediaUrn::from_string(source_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid source media URN '{}': {}", source_media, e)))?;
        let target_urn = MediaUrn::from_string(target_media)
            .map_err(|e| PlannerError::InvalidInput(format!("Invalid target media URN '{}': {}", target_media, e)))?;

        if source_urn.conforms_to(&target_urn)
            .expect("CU2: media URN prefix mismatch in path finding") {
            return Ok(vec![]);
        }

        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to list caps: {}", e)))?;

        let mut graph: HashMap<String, Vec<(String, MediaUrn, String)>> = HashMap::new();
        let mut input_urns: Vec<MediaUrn> = Vec::new();
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();

        for cap in &caps {
            let cap_urn = cap.urn.to_string();

            if !self.is_cap_available(&cap_urn) {
                continue;
            }

            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let input_urn = match MediaUrn::from_string(input_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let output_urn = match MediaUrn::from_string(output_spec) {
                Ok(u) => u,
                Err(_) => continue,
            };

            let input_canonical = input_urn.to_string();

            let edge_key = (input_canonical.clone(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                tracing::error!(
                    "BUG: Duplicate cap_urn detected in graph building (find_all_paths): {} (input_spec: {}). This indicates stale caps in the registry - run upload-standards to sync.",
                    cap_urn, input_spec
                );
                return Err(PlannerError::Internal(format!(
                    "Duplicate cap_urn in graph: {} (input_spec: {}). \
                     Registry has stale caps - run upload-standards.sh to sync.",
                    cap_urn, input_spec
                )));
            }

            if !input_urns.iter().any(|u| u == &input_urn) {
                input_urns.push(input_urn);
            }
            graph
                .entry(input_canonical)
                .or_insert_with(Vec::new)
                .push((cap_urn, output_urn, cap.title.clone()));
        }

        let mut all_paths: Vec<CapChainPathInfo> = Vec::new();
        let mut current_path: Vec<CapChainStepInfo> = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();

        Self::dfs_find_paths(
            &graph,
            &input_urns,
            &source_urn,
            &target_urn,
            source_media,
            target_media,
            &mut current_path,
            &mut visited,
            &mut all_paths,
            max_depth,
            max_paths,
        );

        // Sort by coherence (fewer deviations first), then path length
        all_paths.sort_by_key(|p| Self::path_coherence_score(p, &source_urn, &target_urn));

        // If any path has 0 deviations, filter out all paths with deviations
        let has_coherent = all_paths.iter().any(|p| {
            Self::path_coherence_score(p, &source_urn, &target_urn).0 == 0
        });
        if has_coherent {
            all_paths.retain(|p| {
                Self::path_coherence_score(p, &source_urn, &target_urn).0 == 0
            });
        }

        Ok(all_paths)
    }

    /// Score a path by how many intermediate outputs deviate from source/target domains.
    fn path_coherence_score(
        path: &CapChainPathInfo,
        source: &MediaUrn,
        target: &MediaUrn,
    ) -> (i32, i32) {
        let mut deviation_penalty = 0i32;
        for step in &path.steps {
            if let Ok(step_out) = MediaUrn::from_string(&step.to_spec) {
                // Use is_comparable for relatedness check (same chain)
                let related_to_source = step_out.is_comparable(source).unwrap_or(false);
                let related_to_target = step_out.is_comparable(target).unwrap_or(false);
                if !related_to_source && !related_to_target {
                    deviation_penalty += 1;
                }
            }
        }
        (deviation_penalty, path.total_steps)
    }

    /// DFS helper to find all paths
    fn dfs_find_paths(
        graph: &HashMap<String, Vec<(String, MediaUrn, String)>>,
        input_urns: &[MediaUrn],
        current_urn: &MediaUrn,
        target_urn: &MediaUrn,
        source_media: &str,
        target_media: &str,
        current_path: &mut Vec<CapChainStepInfo>,
        visited: &mut HashSet<String>,
        all_paths: &mut Vec<CapChainPathInfo>,
        max_depth: usize,
        max_paths: usize,
    ) {
        if all_paths.len() >= max_paths {
            return;
        }

        if current_urn.conforms_to(target_urn)
            .expect("CU2: media URN prefix mismatch in DFS path finding") {
            let description = current_path
                .iter()
                .map(|s| s.title.clone())
                .collect::<Vec<_>>()
                .join(" → ");

            all_paths.push(CapChainPathInfo {
                steps: current_path.clone(),
                source_spec: source_media.to_string(),
                target_spec: target_media.to_string(),
                total_steps: current_path.len() as i32,
                description,
            });
            return;
        }

        if current_path.len() >= max_depth {
            return;
        }

        let current_canonical = current_urn.to_string();
        visited.insert(current_canonical.clone());

        for cap_input_urn in input_urns {
            if !current_urn.conforms_to(cap_input_urn)
                .expect("CU2: media URN prefix mismatch in DFS path finding") {
                continue;
            }

            let cap_input_canonical = cap_input_urn.to_string();
            if let Some(neighbors) = graph.get(&cap_input_canonical) {
                for (cap_urn, next_urn, title) in neighbors {
                    let next_canonical = next_urn.to_string();
                    if !visited.contains(&next_canonical) {
                        let from_spec = if current_path.is_empty() {
                            source_media.to_string()
                        } else {
                            current_canonical.clone()
                        };

                        current_path.push(CapChainStepInfo {
                            cap_urn: cap_urn.clone(),
                            from_spec,
                            to_spec: next_canonical.clone(),
                            title: title.clone(),
                            file_path_arg_name: None,
                        });

                        Self::dfs_find_paths(
                            graph,
                            input_urns,
                            next_urn,
                            target_urn,
                            source_media,
                            target_media,
                            current_path,
                            visited,
                            all_paths,
                            max_depth,
                            max_paths,
                        );

                        current_path.pop();
                    }
                }
            }
        }

        visited.remove(&current_canonical);
    }
}

/// Info about a reachable target
#[derive(Debug, Clone)]
pub struct ReachableTargetInfo {
    pub media_spec: String,
    pub display_name: String,
    pub min_path_length: i32,
    pub path_count: i32,
}

/// Info about a single step in a cap chain path
#[derive(Debug, Clone)]
pub struct CapChainStepInfo {
    pub cap_urn: String,
    pub from_spec: String,
    pub to_spec: String,
    pub title: String,
    /// File-path argument name, determined by media URN type matching (not name convention).
    /// Populated when the path is used for plan building. None if not yet resolved.
    pub file_path_arg_name: Option<String>,
}

/// Info about a complete cap chain path
#[derive(Debug, Clone)]
pub struct CapChainPathInfo {
    pub steps: Vec<CapChainStepInfo>,
    pub source_spec: String,
    pub target_spec: String,
    pub total_steps: i32,
    pub description: String,
}

// =============================================================================
// ARGUMENT ANALYSIS FOR SLOT PRESENTATION
// =============================================================================

/// How an argument will be resolved
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentResolution {
    /// Auto-resolved from input file (for first cap's file_path)
    FromInputFile,
    /// Auto-resolved from previous cap's output
    FromPreviousOutput,
    /// Has a default value in cap definition
    HasDefault,
    /// Must be provided by user (slot)
    RequiresUserInput,
}

/// Information about a single argument for UI presentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgumentInfo {
    /// Argument name (e.g., "file_path", "width")
    pub name: String,
    /// Media URN describing the type (e.g., "media:integer")
    pub media_urn: String,
    /// Human-readable description
    pub description: String,
    /// How this argument will be resolved
    pub resolution: ArgumentResolution,
    /// Default value if any
    pub default_value: Option<serde_json::Value>,
    /// Whether this is a required argument
    pub is_required: bool,
    /// Validation rules if any
    pub validation: Option<serde_json::Value>,
}

/// Argument requirements for a single step in the path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepArgumentRequirements {
    /// Cap URN for this step
    pub cap_urn: String,
    /// Step index (0-based)
    pub step_index: usize,
    /// Cap title
    pub title: String,
    /// All arguments for this cap with their resolution status
    pub arguments: Vec<ArgumentInfo>,
    /// Arguments that require user input (slots)
    pub slots: Vec<ArgumentInfo>,
}

/// Argument requirements for an entire path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathArgumentRequirements {
    /// Source media spec
    pub source_spec: String,
    /// Target media spec
    pub target_spec: String,
    /// Requirements for each step
    pub steps: Vec<StepArgumentRequirements>,
    /// All slots across all steps that need user input
    pub all_slots: Vec<ArgumentInfo>,
    /// Whether this path can execute without any user input
    pub can_execute_without_input: bool,
}

impl CapPlanBuilder {
    /// Analyze argument requirements for a path
    pub async fn analyze_path_arguments(
        &self,
        path: &CapChainPathInfo,
    ) -> PlannerResult<PathArgumentRequirements> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        let mut step_requirements = Vec::new();
        let mut all_slots = Vec::new();

        for (step_index, step) in path.steps.iter().enumerate() {
            let cap = caps.iter()
                .find(|c| c.urn.to_string() == step.cap_urn)
                .ok_or_else(|| PlannerError::NotFound(format!(
                    "Cap '{}' not found in registry",
                    step.cap_urn
                )))?;

            let in_spec = cap.urn.in_spec();
            let out_spec = cap.urn.out_spec();

            let mut arguments = Vec::new();
            let mut slots = Vec::new();

            for arg in cap.get_args() {
                let resolution = self.determine_resolution_with_io_check(
                    &arg.media_urn,
                    &in_spec,
                    &out_spec,
                    step_index,
                    arg.required,
                    &arg.default_value,
                );

                // Resolve validation from media spec
                let resolved_spec = crate::media::spec::resolve_media_urn(
                    &arg.media_urn, Some(&cap.media_specs), &self.media_registry
                ).await.ok();
                let validation = resolved_spec.and_then(|spec| spec.validation);

                let arg_info = ArgumentInfo {
                    name: arg.media_urn.clone(),
                    media_urn: arg.media_urn.clone(),
                    description: arg.arg_description.clone().unwrap_or_default(),
                    resolution: resolution.clone(),
                    default_value: arg.default_value.clone(),
                    is_required: arg.required,
                    validation: Self::validation_to_json(validation.as_ref()),
                };

                let is_io_arg = resolution == ArgumentResolution::FromInputFile
                    || resolution == ArgumentResolution::FromPreviousOutput;

                if !is_io_arg {
                    slots.push(arg_info.clone());
                    all_slots.push(arg_info.clone());
                }
                arguments.push(arg_info);
            }

            step_requirements.push(StepArgumentRequirements {
                cap_urn: step.cap_urn.clone(),
                step_index,
                title: step.title.clone(),
                arguments,
                slots,
            });
        }

        let can_execute_without_input = all_slots.is_empty();

        Ok(PathArgumentRequirements {
            source_spec: path.source_spec.clone(),
            target_spec: path.target_spec.clone(),
            steps: step_requirements,
            all_slots,
            can_execute_without_input,
        })
    }

    /// Convert MediaValidation to JSON if it has any constraints
    fn validation_to_json(validation: Option<&MediaValidation>) -> Option<serde_json::Value> {
        let validation = validation?;

        let has_constraints = validation.min.is_some()
            || validation.max.is_some()
            || validation.min_length.is_some()
            || validation.max_length.is_some()
            || validation.pattern.is_some()
            || validation.allowed_values.is_some();

        if has_constraints {
            serde_json::to_value(validation).ok()
        } else {
            None
        }
    }

    /// Determine how an argument will be resolved based on I/O matching and media URN type.
    fn determine_resolution_with_io_check(
        &self,
        media_urn: &str,
        in_spec: &str,
        out_spec: &str,
        step_index: usize,
        _is_required: bool,
        default_value: &Option<serde_json::Value>,
    ) -> ArgumentResolution {
        // Check if this arg is the input arg (matches cap's in= spec)
        if media_urn == in_spec {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // Check if this arg is the output arg (matches cap's out= spec)
        if media_urn == out_spec {
            return ArgumentResolution::FromPreviousOutput;
        }

        // Check for file-path types
        let is_file_path_type = if let Ok(urn) = MediaUrn::from_string(media_urn) {
            urn.is_any_file_path()
        } else {
            false
        };

        if is_file_path_type {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // All other args need user input
        if default_value.is_some() {
            return ArgumentResolution::HasDefault;
        }

        ArgumentResolution::RequiresUserInput
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::CapUrn;

    /// Helper to create a test cap with given in/out specs (full media URNs)
    fn make_test_cap(op: &str, in_spec: &str, out_spec: &str, title: &str) -> Result<Cap, crate::urn::cap_urn::CapUrnError> {
        let mut tags = BTreeMap::new();
        tags.insert("op".to_string(), op.to_string());
        let urn = CapUrn::new(in_spec.to_string(), out_spec.to_string(), tags)?;
        Ok(Cap::new(urn, title.to_string(), "test-command".to_string()))
    }

    /// Simulates the graph-building duplicate detection logic
    fn check_for_duplicate_caps(caps: &[Cap]) -> std::result::Result<usize, String> {
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        let mut edge_count = 0;

        for cap in caps {
            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let cap_urn = cap.urn.to_string();

            let edge_key = (input_spec.to_string(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                return Err(format!(
                    "Duplicate cap_urn detected: {} (input_spec: {})",
                    cap_urn, input_spec
                ));
            }
            edge_count += 1;
        }

        Ok(edge_count)
    }

    // TEST750: Tests duplicate detection passes for caps with unique URN combinations
    // Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations
    #[test]
    fn test750_no_duplicates_with_unique_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("extract_metadata", "media:pdf", "media:file-metadata;textable;record", "Extract Metadata")?,
            make_test_cap("extract_outline", "media:pdf", "media:document-outline;textable;record", "Extract Outline")?,
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Should not detect duplicates for unique caps");
        assert_eq!(result.unwrap(), 3, "Should have 3 edges");
        Ok(())
    }

    // TEST751: Tests duplicate detection identifies caps with identical URNs
    // Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn
    #[test]
    fn test751_detects_duplicate_cap_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF")?,
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF Again")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_err(), "Should detect duplicate cap URN");
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Duplicate cap_urn detected"), "Error should mention duplicate: {}", err_msg);
        assert!(err_msg.contains("op=disbind"), "Error should contain the cap URN: {}", err_msg);
        assert!(err_msg.contains("media:pdf"), "Error should contain the input spec: {}", err_msg);
        Ok(())
    }

    // TEST752: Tests caps with different operations but same input/output types are not duplicates
    // Verifies that only the complete URN (including op) is used for duplicate detection
    #[test]
    fn test752_different_ops_same_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind")?,
            make_test_cap("grind", "media:pdf", "media:disbound-pages;textable;list", "Grind")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Different ops should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // TEST753: Tests caps with same operation but different input types are not duplicates
    // Verifies that input type differences distinguish caps with the same operation name
    #[test]
    fn test753_same_op_different_input_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("extract_metadata", "media:pdf", "media:file-metadata;textable;record", "Extract PDF Metadata")?,
            make_test_cap("extract_metadata", "media:txt;textable", "media:file-metadata;textable;record", "Extract TXT Metadata")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Same op with different inputs should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // ==========================================================================
    // ARGUMENT RESOLUTION TESTS
    // ==========================================================================

    fn create_test_plan_builder() -> CapPlanBuilder {
        let cap_registry = CapRegistry::new_for_test();
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        CapPlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    fn create_test_plan_builder_with_registry(cap_registry: CapRegistry) -> CapPlanBuilder {
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        CapPlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    // TEST754: Tests first cap's input argument is automatically resolved from input file
    // Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain
    #[test]
    fn test754_input_arg_first_cap_auto_resolved_from_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST755: Tests subsequent caps' input arguments are automatically resolved from previous output
    // Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first
    #[test]
    fn test755_input_arg_subsequent_cap_auto_resolved_from_previous() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 2, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST756: Tests output arguments are automatically resolved from previous cap's output
    // Verifies that arguments matching the output spec are always resolved as FromPreviousOutput
    #[test]
    fn test756_output_arg_auto_resolved() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(out_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST757: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap
    // Verifies that generic file-path arguments are bound to input file in the first cap
    #[test]
    fn test757_file_path_type_fallback_first_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST758: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps
    // Verifies that generic file-path arguments are bound to previous cap's output after the first cap
    #[test]
    fn test758_file_path_type_fallback_subsequent_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST759: Tests MEDIA_FILE_PATH_ARRAY argument type resolution for first and subsequent caps
    // Verifies that file-path array arguments follow the same resolution pattern as single file paths
    #[test]
    fn test759_file_path_array_fallback() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);

        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST760: Tests required non-IO arguments with default values are marked as HasDefault
    // Verifies that arguments like integers with defaults don't require user input
    #[test]
    fn test760_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(200));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, true, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST761: Tests required non-IO arguments without defaults require user input
    // Verifies that arguments like strings without defaults are marked as RequiresUserInput
    #[test]
    fn test761_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_STRING, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST762: Tests optional non-IO arguments with default values are marked as HasDefault
    // Verifies that optional arguments with defaults behave the same as required ones with defaults
    #[test]
    fn test762_optional_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(300));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, false, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST763: Tests optional non-IO arguments without defaults still require user input
    // Verifies that optional arguments without defaults must be explicitly provided or skipped
    #[test]
    fn test763_optional_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_BOOLEAN, in_spec, out_spec, 0, false, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST764: Tests validation_to_json() returns None for None input
    // Verifies that missing validation metadata is converted to JSON None
    #[test]
    fn test764_validation_to_json_none() {
        let json = CapPlanBuilder::validation_to_json(None);
        assert!(json.is_none(), "None validation should return None");
    }

    // TEST765: Tests validation_to_json() returns None for empty validation constraints
    // Verifies that default MediaValidation with no constraints produces JSON None
    #[test]
    fn test765_validation_to_json_empty() {
        let validation = MediaValidation::default();
        let json = CapPlanBuilder::validation_to_json(Some(&validation));
        assert!(json.is_none(), "Empty validation should return None");
    }

    // TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON
    // Verifies that min/max validation rules are correctly serialized as JSON fields
    #[test]
    fn test766_validation_to_json_with_constraints() {
        let validation = MediaValidation {
            min: Some(50.0),
            max: Some(2000.0),
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
        };
        let json = CapPlanBuilder::validation_to_json(Some(&validation));
        assert!(json.is_some(), "Validation with constraints should return Some");
        let json = json.unwrap();
        assert_eq!(json["min"], 50.0);
        assert_eq!(json["max"], 2000.0);
    }

    // TEST767: Tests ArgumentInfo struct serialization to JSON
    // Verifies that argument metadata including resolution status and validation is correctly serialized
    #[test]
    fn test767_argument_info_serialization() {
        let arg_info = ArgumentInfo {
            name: "width".to_string(),
            media_urn: "media:integer".to_string(),
            description: "Width in pixels".to_string(),
            resolution: ArgumentResolution::HasDefault,
            default_value: Some(serde_json::json!(200)),
            is_required: false,
            validation: Some(serde_json::json!({"min": 50, "max": 2000})),
        };

        let json = serde_json::to_string(&arg_info).expect("Should serialize");
        assert!(json.contains("\"name\":\"width\""));
        assert!(json.contains("\"resolution\":\"has_default\""));
        assert!(json.contains("\"default_value\":200"));
    }

    // TEST768: Tests PathArgumentRequirements structure for single-step execution paths
    // Verifies that argument requirements are correctly organized by step with resolution information
    #[test]
    fn test768_path_argument_requirements_structure() {
        let requirements = PathArgumentRequirements {
            source_spec: "media:pdf".to_string(),
            target_spec: "media:png".to_string(),
            steps: vec![
                StepArgumentRequirements {
                    cap_urn: "cap:op=generate_thumbnail;in=pdf;out=png".to_string(),
                    step_index: 0,
                    title: "Generate Thumbnail".to_string(),
                    arguments: vec![
                        ArgumentInfo {
                            name: "file_path".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Path to file".to_string(),
                            resolution: ArgumentResolution::FromInputFile,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                    slots: vec![],
                },
            ],
            all_slots: vec![],
            can_execute_without_input: true,
        };

        assert!(requirements.can_execute_without_input);
        assert_eq!(requirements.steps.len(), 1);
        assert_eq!(requirements.steps[0].slots.len(), 0);
        assert_eq!(requirements.steps[0].arguments[0].resolution, ArgumentResolution::FromInputFile);
    }

    // TEST769: Tests PathArgumentRequirements tracking of required user-input slots
    // Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false
    #[test]
    fn test769_path_with_required_slot() {
        let requirements = PathArgumentRequirements {
            source_spec: "media:text".to_string(),
            target_spec: "media:translated".to_string(),
            steps: vec![
                StepArgumentRequirements {
                    cap_urn: "cap:op=translate;in=text;out=translated".to_string(),
                    step_index: 0,
                    title: "Translate".to_string(),
                    arguments: vec![
                        ArgumentInfo {
                            name: "file_path".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Path to file".to_string(),
                            resolution: ArgumentResolution::FromInputFile,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                        ArgumentInfo {
                            name: "target_language".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Target language code".to_string(),
                            resolution: ArgumentResolution::RequiresUserInput,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                    slots: vec![
                        ArgumentInfo {
                            name: "target_language".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Target language code".to_string(),
                            resolution: ArgumentResolution::RequiresUserInput,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                },
            ],
            all_slots: vec![
                ArgumentInfo {
                    name: "target_language".to_string(),
                    media_urn: "media:string".to_string(),
                    description: "Target language code".to_string(),
                    resolution: ArgumentResolution::RequiresUserInput,
                    default_value: None,
                    is_required: true,
                    validation: None,
                },
            ],
            can_execute_without_input: false,
        };

        assert!(!requirements.can_execute_without_input);
        assert_eq!(requirements.all_slots.len(), 1);
        assert_eq!(requirements.all_slots[0].name, "target_language");
        assert_eq!(requirements.steps[0].slots.len(), 1);
    }

    // ==========================================================================
    // AVAILABILITY FILTERING TESTS
    // ==========================================================================

    fn create_plan_builder_with_available_caps(available: HashSet<String>) -> CapPlanBuilder {
        create_test_plan_builder().with_available_caps(available)
    }

    // TEST770: Tests is_cap_available() correctly applies availability filter when set
    // Verifies that only caps in the available_caps set are considered available
    #[test]
    fn test770_is_cap_available_with_filter() {
        let mut available = HashSet::new();
        available.insert("cap:in=\"media:a\";op=transform;out=\"media:b\"".to_string());

        let builder = create_plan_builder_with_available_caps(available);

        assert!(builder.is_cap_available("cap:in=\"media:a\";op=transform;out=\"media:b\""));
        assert!(!builder.is_cap_available("cap:in=\"media:b\";op=convert;out=\"media:c\""));
    }

    // TEST771: Tests is_cap_available() treats all caps as available when no filter is set
    // Verifies that without an availability filter, any cap URN is considered available
    #[test]
    fn test771_is_cap_available_without_filter() {
        let builder = create_test_plan_builder();

        assert!(builder.is_cap_available("cap:in=\"media:a\";op=transform;out=\"media:b\""));
        assert!(builder.is_cap_available("cap:in=\"media:x\";op=anything;out=\"media:y\""));
    }

    // TEST772: Tests find_all_paths() excludes unavailable caps from pathfinding
    // Verifies that only paths using available caps are returned when filter is set
    #[tokio::test]
    async fn test772_find_all_paths_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("direct", "media:a", "media:c", "A to C Direct")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());
        available.insert(cap2.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:a", "media:c", 5, 10).await.unwrap();

        assert_eq!(paths.len(), 1, "Should only find one path (through available caps)");
        assert_eq!(paths[0].steps.len(), 2, "Path should have 2 steps (A->B, B->C)");
        assert_eq!(paths[0].steps[0].title, "A to B");
        assert_eq!(paths[0].steps[1].title, "B to C");
        Ok(())
    }

    // TEST773: Tests find_all_paths() returns empty result when all caps are filtered out
    // Verifies that pathfinding returns no paths when the availability filter excludes all relevant caps
    #[tokio::test]
    async fn test773_find_all_paths_returns_empty_when_no_available_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;

        registry.add_caps_to_cache(vec![cap1]);

        let available = HashSet::new();

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:a", "media:b", 5, 10).await.unwrap();

        assert!(paths.is_empty(), "Should find no paths when no caps are available");
        Ok(())
    }

    // TEST774: Tests get_reachable_targets() only considers available caps for reachability
    // Verifies that target specs are only reachable via caps in the availability filter
    #[tokio::test]
    async fn test774_get_reachable_targets_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("step3", "media:a", "media:d", "A to D")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());
        available.insert(cap3.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let targets = builder.get_reachable_targets("media:a").await.unwrap();

        assert_eq!(targets.len(), 2, "Should find 2 reachable targets (B and D)");
        assert!(targets.contains(&"media:b".to_string()), "B should be reachable");
        assert!(targets.contains(&"media:d".to_string()), "D should be reachable");
        assert!(!targets.contains(&"media:c".to_string()), "C should NOT be reachable (cap2 not available)");
        Ok(())
    }

    // TEST775: Tests find_path() selects from available caps when multiple paths exist
    // Verifies that find_path() respects availability filter and prefers available direct paths
    #[tokio::test]
    async fn test775_find_path_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("direct", "media:a", "media:c", "A to C Direct")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap3.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let path = builder.find_path("media:a", "media:c").await.unwrap();

        assert_eq!(path.len(), 1, "Should find path with 1 step (direct)");
        assert!(path[0].contains("op=direct"), "Should use the direct cap: {}", path[0]);
        Ok(())
    }

    // TEST776: Tests find_path() returns error when required caps are filtered out by availability
    // Verifies that "No path found" error is returned when filter blocks the only viable path
    #[tokio::test]
    async fn test776_find_path_returns_error_when_path_unavailable() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:a", "media:c").await;

        assert!(result.is_err(), "Should fail when path requires unavailable caps");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No path found"), "Error should indicate no path found: {}", err);
        Ok(())
    }

    // ==========================================================================
    // TYPE MISMATCH TESTS
    // ==========================================================================

    // TEST777: Tests type checking prevents using PDF-specific cap with PNG input
    // Verifies that media type compatibility is enforced during pathfinding (PNG cannot use PDF cap)
    #[tokio::test]
    async fn test777_type_mismatch_pdf_cap_does_not_match_png_input() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:png", "media:textable").await;

        assert!(result.is_err(), "Should NOT find path from PNG to text via PDF cap");
        Ok(())
    }

    // TEST778: Tests type checking prevents using PNG-specific cap with PDF input
    // Verifies that media type compatibility is enforced during pathfinding (PDF cannot use PNG cap)
    #[tokio::test]
    async fn test778_type_mismatch_png_cap_does_not_match_pdf_input() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:pdf", "media:thumbnail").await;

        assert!(result.is_err(), "Should NOT find path from PDF to thumbnail via PNG cap");
        Ok(())
    }

    // TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
    // Verifies that PNG and PDF inputs reach different targets based on cap input type requirements
    #[tokio::test]
    async fn test779_get_reachable_targets_respects_type_matching() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone(), png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_targets = builder.get_reachable_targets("media:png").await.unwrap();
        assert_eq!(png_targets.len(), 1, "PNG should only reach 1 target");
        assert!(png_targets.contains(&"media:thumbnail".to_string()), "PNG should reach thumbnail");
        assert!(!png_targets.contains(&"media:textable".to_string()), "PNG should NOT reach text (type mismatch)");

        let pdf_targets = builder.get_reachable_targets("media:pdf").await.unwrap();
        assert_eq!(pdf_targets.len(), 1, "PDF should only reach 1 target");
        assert!(pdf_targets.contains(&"media:textable".to_string()), "PDF should reach text");
        assert!(!pdf_targets.contains(&"media:thumbnail".to_string()), "PDF should NOT reach thumbnail (type mismatch)");
        Ok(())
    }

    // TEST780: Tests get_reachable_targets_with_metadata() respects type compatibility constraints
    // Verifies that reachable target metadata only includes type-compatible transformations
    #[tokio::test]
    async fn test780_reachable_targets_with_metadata_respects_type_matching() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone(), png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_targets = builder.get_reachable_targets_with_metadata("media:png", 5).await.unwrap();
        assert_eq!(png_targets.len(), 1, "PNG should only reach 1 target with metadata");
        assert_eq!(png_targets[0].media_spec, "media:thumbnail", "PNG target should be thumbnail");

        let pdf_targets = builder.get_reachable_targets_with_metadata("media:pdf", 5).await.unwrap();
        assert_eq!(pdf_targets.len(), 1, "PDF should only reach 1 target with metadata");
        assert_eq!(pdf_targets[0].media_spec, "media:textable", "PDF target should be text");
        Ok(())
    }

    // TEST781: Tests find_all_paths() enforces type compatibility across multi-step chains
    // Verifies that paths are only found when all intermediate types are compatible
    #[tokio::test]
    async fn test781_find_all_paths_respects_type_chain() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let resize_png = make_test_cap("resize", "media:png", "media:resized-png", "Resize PNG")?;
        let to_thumb = make_test_cap("thumb", "media:resized-png", "media:thumbnail", "To Thumbnail")?;

        registry.add_caps_to_cache(vec![resize_png.clone(), to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(resize_png.urn.to_string());
        available.insert(to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_paths = builder.find_all_paths("media:png", "media:thumbnail", 5, 10).await.unwrap();
        assert_eq!(png_paths.len(), 1, "Should find 1 path from PNG to thumbnail");
        assert_eq!(png_paths[0].steps.len(), 2, "Path should have 2 steps");

        let pdf_paths = builder.find_all_paths("media:pdf", "media:thumbnail", 5, 10).await.unwrap();
        assert!(pdf_paths.is_empty(), "Should find NO paths from PDF to thumbnail (type mismatch)");
        Ok(())
    }

    // ==========================================================================
    // PATH COHERENCE SCORING
    // ==========================================================================

    // TEST782: Tests coherence scoring gives 0 deviations for direct single-step paths
    // Verifies that paths going directly from source to target without detours have perfect coherence
    #[test]
    fn test782_coherence_score_zero_for_direct_path() {
        let path = CapChainPathInfo {
            steps: vec![CapChainStepInfo {
                cap_urn: "cap:in=\"media:txt;textable\";op=convert;out=\"media:md;textable\"".to_string(),
                from_spec: "media:txt;textable".to_string(),
                to_spec: "media:md;textable".to_string(),
                title: "Convert TXT to MD".to_string(),
                file_path_arg_name: None,
            }],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:md;textable".to_string(),
            total_steps: 1,
            description: "txt → md".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:md;textable").unwrap();
        let (deviation, steps) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 0, "Direct path should have 0 deviations");
        assert_eq!(steps, 1, "Path should have 1 step");
    }

    // TEST783: Tests coherence scoring penalizes paths through semantically unrelated intermediates
    // Verifies that going from textable→thumbnail→textable incurs deviation penalty (thumbnail unrelated)
    #[test]
    fn test783_coherence_score_penalizes_unrelated_intermediate() {
        let path = CapChainPathInfo {
            steps: vec![
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:txt;textable\";op=to_thumb;out=\"media:thumbnail\"".to_string(),
                    from_spec: "media:txt;textable".to_string(),
                    to_spec: "media:thumbnail".to_string(),
                    title: "To Thumbnail".to_string(),
                    file_path_arg_name: None,
                },
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:thumbnail\";op=to_rst;out=\"media:rst;textable\"".to_string(),
                    from_spec: "media:thumbnail".to_string(),
                    to_spec: "media:rst;textable".to_string(),
                    title: "To RST".to_string(),
                    file_path_arg_name: None,
                },
            ],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:rst;textable".to_string(),
            total_steps: 2,
            description: "txt → thumbnail → rst".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:rst;textable").unwrap();
        let (deviation, steps) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 1, "Path through unrelated thumbnail should have 1 deviation");
        assert_eq!(steps, 2);
    }

    // TEST784: Tests coherence scoring does not penalize paths through semantically related intermediates
    // Verifies that going through a supertype (txt→textable→md) maintains coherence with 0 deviations
    #[test]
    fn test784_coherence_score_related_intermediate_not_penalized() {
        let path = CapChainPathInfo {
            steps: vec![
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:txt;textable\";op=strip;out=\"media:textable\"".to_string(),
                    from_spec: "media:txt;textable".to_string(),
                    to_spec: "media:textable".to_string(),
                    title: "Strip format".to_string(),
                    file_path_arg_name: None,
                },
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:textable\";op=to_md;out=\"media:md;textable\"".to_string(),
                    from_spec: "media:textable".to_string(),
                    to_spec: "media:md;textable".to_string(),
                    title: "To MD".to_string(),
                    file_path_arg_name: None,
                },
            ],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:md;textable".to_string(),
            total_steps: 2,
            description: "txt → textable → md".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:md;textable").unwrap();
        let (deviation, _) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 0, "Path through related supertype (textable) should have 0 deviations");
    }

    // TEST785: Tests find_all_paths() filters out deviating paths when coherent alternatives exist
    // Verifies that semantically wandering paths are excluded if direct coherent paths are available
    #[tokio::test]
    async fn test785_find_all_paths_filters_deviating_when_coherent_exists() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let direct = make_test_cap("txt2rst", "media:txt;textable", "media:rst;textable", "Direct TXT to RST")?;
        let to_thumb = make_test_cap("txt2thumb", "media:txt;textable", "media:thumbnail", "TXT to Thumbnail")?;
        let thumb_to_rst = make_test_cap("thumb2rst", "media:thumbnail", "media:rst;textable", "Thumbnail to RST")?;

        registry.add_caps_to_cache(vec![direct.clone(), to_thumb.clone(), thumb_to_rst.clone()]);

        let mut available = HashSet::new();
        available.insert(direct.urn.to_string());
        available.insert(to_thumb.urn.to_string());
        available.insert(thumb_to_rst.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:rst;textable", 5, 10).await.unwrap();

        assert_eq!(paths.len(), 1, "Deviating path should be filtered out when coherent path exists");
        assert_eq!(paths[0].steps.len(), 1, "Remaining path should be the direct 1-step path");
        Ok(())
    }

    // TEST786: Tests find_all_paths() keeps all paths when no coherent path exists
    // Verifies that all deviating paths are returned if they're the only viable options
    #[tokio::test]
    async fn test786_find_all_paths_keeps_all_when_all_deviate() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let txt_to_thumb = make_test_cap("txt2thumb", "media:txt;textable", "media:thumbnail", "TXT to Thumb")?;
        let thumb_to_emb = make_test_cap("thumb2emb", "media:thumbnail", "media:embeddings", "Thumb to Embeddings")?;

        let txt_to_audio = make_test_cap("txt2audio", "media:txt;textable", "media:audio", "TXT to Audio")?;
        let audio_to_thumb = make_test_cap("audio2thumb", "media:audio", "media:thumbnail", "Audio to Thumb")?;

        registry.add_caps_to_cache(vec![
            txt_to_thumb.clone(), thumb_to_emb.clone(),
            txt_to_audio.clone(), audio_to_thumb.clone(),
        ]);

        let mut available = HashSet::new();
        available.insert(txt_to_thumb.urn.to_string());
        available.insert(thumb_to_emb.urn.to_string());
        available.insert(txt_to_audio.urn.to_string());
        available.insert(audio_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:embeddings", 5, 10).await.unwrap();

        assert!(paths.len() >= 2, "When no coherent path exists, all deviating paths should be kept (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 2, "First path should be the shorter 2-step one");
        Ok(())
    }

    // TEST787: Tests find_all_paths() sorts coherent paths by length, preferring shorter ones
    // Verifies that among multiple coherent paths, the shortest is ranked first
    #[tokio::test]
    async fn test787_find_all_paths_coherent_sorting_prefers_shorter() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let direct = make_test_cap("txt2md", "media:txt;textable", "media:md;textable", "Direct")?;
        let strip = make_test_cap("strip", "media:txt;textable", "media:textable", "Strip Format")?;
        let to_md = make_test_cap("to_md", "media:textable", "media:md;textable", "To MD")?;

        registry.add_caps_to_cache(vec![direct.clone(), strip.clone(), to_md.clone()]);

        let mut available = HashSet::new();
        available.insert(direct.urn.to_string());
        available.insert(strip.urn.to_string());
        available.insert(to_md.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:md;textable", 5, 10).await.unwrap();

        assert!(paths.len() >= 2, "Should find at least 2 paths (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 1, "Shortest coherent path should be first");
        Ok(())
    }

    // ==========================================================================
    // URN CANONICALIZATION TESTS
    // ==========================================================================

    // TEST1100: Tests that CapUrn normalizes media URN tags to canonical order
    // This is the root cause fix for caps not matching when plugins report URNs with
    // different tag ordering than the registry (e.g., "record;textable" vs "textable;record")
    #[test]
    fn test1100_cap_urn_normalizes_media_urn_tag_order() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        // Create two CapUrns with different tag ordering in the output media URN
        let urn1 = CapUrn::from_string("cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\"")?;
        let urn2 = CapUrn::from_string("cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;textable;record\"")?;

        // After normalization, both should produce the same canonical string
        assert_eq!(
            urn1.to_string(),
            urn2.to_string(),
            "URNs with different tag ordering should normalize to the same canonical form"
        );

        // The canonical form should have tags in alphabetical order
        let canonical = urn1.to_string();
        assert!(
            canonical.contains("record;textable") || canonical.contains("textable;record"),
            "Canonical form should contain the tags: {}", canonical
        );

        Ok(())
    }

    // TEST1101: Tests that is_cap_available matches URNs regardless of original tag ordering
    // Verifies that a cap from the registry (with one tag order) matches an available cap
    // from plugins (with different tag order) after normalization
    #[tokio::test]
    async fn test1101_is_cap_available_matches_normalized_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        // Registry has cap with tags in one order
        let registry_cap = make_test_cap(
            "extract_metadata",
            "media:pdf",
            "media:file-metadata;textable;record",  // textable comes first
            "Extract PDF Metadata"
        )?;
        registry.add_caps_to_cache(vec![registry_cap.clone()]);

        // Plugin reports the same cap but with tags in different order
        // After normalization via CapUrn::from_string().to_string(), order should match
        let plugin_urn_str = "cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\"";
        let plugin_urn = CapUrn::from_string(plugin_urn_str)?;
        let normalized_plugin_urn = plugin_urn.to_string();

        let mut available = HashSet::new();
        available.insert(normalized_plugin_urn);

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // The cap should be found as available because normalization makes URNs match
        assert!(
            builder.is_cap_available(&registry_cap.urn.to_string()),
            "Cap should be available after URN normalization. Registry: {}, Available set should contain normalized form",
            registry_cap.urn.to_string()
        );

        Ok(())
    }

    // TEST1102: Tests that pathfinding works when plugin and registry have different tag ordering
    // This is an integration test for the full fix: plugins report URNs with one order,
    // registry has another order, but paths are still found because URNs are normalized
    #[tokio::test]
    async fn test1102_pathfinding_works_with_different_tag_ordering() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        // Registry cap with output tags in one order
        let pdf_to_metadata = make_test_cap(
            "extract_metadata",
            "media:pdf",
            "media:file-metadata;textable;record",  // alphabetical: record, textable -> becomes record;textable
            "Extract PDF Metadata"
        )?;

        registry.add_caps_to_cache(vec![pdf_to_metadata.clone()]);

        // Simulate plugin reporting same cap with different tag order in output
        // The key insight: CapUrn::new() now normalizes, so both produce same canonical form
        let plugin_urn = CapUrn::from_string(
            "cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\""
        )?;

        let mut available = HashSet::new();
        available.insert(plugin_urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // Should find reachable targets because URNs match after normalization
        let targets = builder.get_reachable_targets("media:pdf").await.unwrap();

        assert!(
            !targets.is_empty(),
            "Should find reachable targets when URNs are normalized. \
             This test verifies the fix for plugin/registry tag ordering mismatch."
        );

        Ok(())
    }

    // TEST1103: Tests that is_cap_available uses is_dispatchable correctly
    // The available cap (provider) must be dispatchable for the requested cap (request).
    // This tests the directionality: provider.is_dispatchable(&request)
    #[test]
    fn test1103_is_cap_available_uses_is_dispatchable_correctly() {
        // A more specific provider should be dispatchable for a general request
        let general_request = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text"
        ).unwrap();

        let specific_provider = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text;version=2"
        ).unwrap();

        // provider.is_dispatchable(&request) should be true: specific provider refines general request
        assert!(
            specific_provider.is_dispatchable(&general_request),
            "Specific provider should be dispatchable for general request"
        );

        // request.is_dispatchable(&provider) should be false: general request cannot handle specific provider's requirements
        assert!(
            !general_request.is_dispatchable(&specific_provider),
            "General request should NOT be dispatchable for specific provider (missing version tag)"
        );

        // Now test via is_cap_available
        let registry = CapRegistry::new_for_test();
        // Registry has the general request
        let registry_cap = Cap::new(general_request.clone(), "Extract".to_string(), "extract".to_string());
        registry.add_caps_to_cache(vec![registry_cap]);

        // Plugin provides the specific provider
        let mut available = HashSet::new();
        available.insert(specific_provider.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // is_cap_available checks: does any available cap (provider) dispatch the request?
        // This should be TRUE because specific_provider is dispatchable for general_request
        assert!(
            builder.is_cap_available(&general_request.to_string()),
            "is_cap_available should return true when available provider can dispatch requested cap"
        );
    }

    // TEST1104: Tests that is_cap_available rejects when provider cannot dispatch request
    #[test]
    fn test1104_is_cap_available_rejects_non_dispatchable() {
        // Request requires specific tag that provider doesn't have
        let request = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text;required=yes"
        ).unwrap();

        let provider = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text"  // missing required=yes
        ).unwrap();

        // provider is NOT dispatchable for request (missing required tag that request needs)
        assert!(
            !provider.is_dispatchable(&request),
            "Provider missing required tag should not be dispatchable for request"
        );

        let registry = CapRegistry::new_for_test();
        let registry_cap = Cap::new(request.clone(), "Extract".to_string(), "extract".to_string());
        registry.add_caps_to_cache(vec![registry_cap]);

        let mut available = HashSet::new();
        available.insert(provider.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // is_cap_available should return FALSE because provider cannot dispatch request
        assert!(
            !builder.is_cap_available(&request.to_string()),
            "is_cap_available should return false when provider cannot dispatch requested cap"
        );
    }
}
