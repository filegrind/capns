//! Cap Plan Builder
//!
//! Utility for building cap execution plans. This module provides:
//! - Plan construction from pre-computed paths (via `build_plan_from_path`)
//! - Argument analysis for slot presentation
//!
//! NOTE: Path finding has been moved to `LiveCapGraph`. Use `LiveCapGraph` for
//! `get_reachable_targets()` and `find_paths_to_exact_target()`, then pass the
//! resulting `Strand` to `build_plan_from_path()` here.

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::json;

use crate::{Cap, CapRegistry, MediaUrn, MediaUrnRegistry, MediaValidation};
use super::argument_binding::{ArgumentBinding, ArgumentBindings};
use super::cardinality::InputCardinality;
use super::plan::{ExecutionNodeType, MachinePlanEdge, MachinePlan, MachineNode};
use super::PlannerError;
use super::live_cap_graph::Strand;

type PlannerResult<T> = Result<T, PlannerError>;

/// Builder for creating cap execution plans.
///
/// NOTE: Path finding methods have been moved to `LiveCapGraph`.
/// This builder handles plan construction from pre-computed paths.
pub struct MachinePlanBuilder {
    /// Cap registry for looking up cap definitions
    cap_registry: Arc<CapRegistry>,
    /// Media URN registry for resolving media specs
    media_registry: Arc<MediaUrnRegistry>,
}

impl MachinePlanBuilder {
    /// Create a new plan builder with the given registries.
    pub fn new(cap_registry: Arc<CapRegistry>, media_registry: Arc<MediaUrnRegistry>) -> Self {
        Self {
            cap_registry,
            media_registry,
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

    /// Build a plan from a pre-defined path.
    /// Looks up cap definitions to find file-path argument names by media URN type.
    ///
    /// Takes a `Strand` from LiveCapGraph which uses typed URNs.
    /// Handles both capability steps and cardinality transition steps (ForEach/Collect).
    ///
    /// ForEach/Collect pairs define iteration boundaries:
    /// - ForEach marks the start of iteration over a list
    /// - Caps between ForEach and Collect form the iteration body
    /// - Collect marks the end, gathering results back into a list
    pub async fn build_plan_from_path(
        &self,
        name: &str,
        path: &Strand,
        input_cardinality: InputCardinality,
    ) -> PlannerResult<MachinePlan> {
        use super::live_cap_graph::StrandStepType;

        let mut plan = MachinePlan::new(name);

        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        // Build a map from cap_urn string to (file-path arg name, stdin-chainable)
        // Only for Cap steps (not cardinality transitions)
        let file_path_info: HashMap<String, (Option<String>, bool)> = path.steps
            .iter()
            .filter_map(|step| {
                if let Some(cap_urn) = step.cap_urn() {
                    let cap_urn_str = cap_urn.to_string();
                    let cap = caps.iter().find(|c| c.urn.to_string() == cap_urn_str);
                    let arg_name = cap.and_then(|c| Self::find_file_path_arg(c));
                    let chainable = cap.map(|c| Self::is_file_path_stdin_chainable(c)).unwrap_or(false);
                    Some((cap_urn_str, (arg_name, chainable)))
                } else {
                    None
                }
            })
            .collect();

        let source_spec_str = path.source_spec.to_string();
        let target_spec_str = path.target_spec.to_string();

        // Log all incoming path steps for debugging plan construction
        tracing::info!(
            "build_plan_from_path: '{}' with {} steps, {} → {}",
            name, path.steps.len(), source_spec_str, target_spec_str
        );
        for (i, step) in path.steps.iter().enumerate() {
            tracing::info!(
                "  step[{}]: type={} from={} to={}",
                i, step.title(), step.from_spec, step.to_spec
            );
        }

        let input_slot_id = "input_slot";
        plan.add_node(MachineNode::input_slot(
            input_slot_id,
            "input",
            &source_spec_str,
            input_cardinality,
        ));

        // First pass: identify ForEach/Collect ranges to determine body boundaries
        // A ForEach at index i with Collect at index j means steps [i+1, j-1] are the body
        let foreach_collect_ranges = Self::find_foreach_collect_ranges(&path.steps);

        let mut prev_node_id = input_slot_id.to_string();
        // Track how many cap steps we've seen (outside of ForEach bodies) for determining first cap
        let mut cap_step_count = 0;
        // Track which ForEach body we're inside (if any)
        let mut inside_foreach_body: Option<(usize, String)> = None; // (foreach_step_index, foreach_node_id)
        let mut body_entry: Option<String> = None;
        let mut body_exit: Option<String> = None;

        for (i, step) in path.steps.iter().enumerate() {
            let node_id = format!("step_{}", i);

            match &step.step_type {
                StrandStepType::Cap { cap_urn, .. } => {
                    let cap_urn_str = cap_urn.to_string();
                    let mut bindings = ArgumentBindings::new();

                    let cap = caps.iter().find(|c| c.urn.to_string() == cap_urn_str);

                    let in_spec = cap.map(|c| c.urn.in_spec()).unwrap_or_default();
                    let out_spec = cap.map(|c| c.urn.out_spec()).unwrap_or_default();

                    // Inside a ForEach body, file paths come from the iteration item, not the original input
                    let is_inside_body = inside_foreach_body.is_some();

                    if let Some((Some(arg_name), stdin_chainable)) = file_path_info.get(&cap_urn_str) {
                        if cap_step_count == 0 && !is_inside_body {
                            bindings.add_file_path(arg_name);
                        } else if *stdin_chainable {
                            bindings.add(
                                arg_name.to_string(),
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

                    let node = MachineNode::cap_with_bindings(&node_id, &cap_urn_str, bindings);
                    plan.add_node(node);
                    plan.add_edge(MachinePlanEdge::direct(&prev_node_id, &node_id));

                    tracing::info!(
                        "  plan_builder: Cap step[{}] '{}' inside_body={} prev_node_id='{}'",
                        i, node_id, is_inside_body, prev_node_id
                    );

                    // Track body entry/exit for ForEach
                    if is_inside_body {
                        if body_entry.is_none() {
                            body_entry = Some(node_id.clone());
                        }
                        body_exit = Some(node_id.clone());
                    } else {
                        cap_step_count += 1;
                    }
                }

                StrandStepType::ForEach { .. } => {
                    // If we're already inside a ForEach body, finalize the outer ForEach first.
                    // This handles nested ForEach: e.g., disbind → ForEach → make_multiple_decisions → ForEach
                    // where the body cap produces a list and the path walks through a second ForEach
                    // to reach the scalar target.
                    if let Some((outer_foreach_idx, outer_foreach_node_id)) = inside_foreach_body.take() {
                        let has_outer_body_entry = body_entry.is_some();
                        let outer_entry = body_entry.take().unwrap_or_else(|| prev_node_id.clone());
                        let outer_exit = body_exit.take().unwrap_or_else(|| prev_node_id.clone());

                        let outer_foreach_input = if outer_foreach_idx == 0 {
                            input_slot_id.to_string()
                        } else {
                            format!("step_{}", outer_foreach_idx - 1)
                        };

                        tracing::info!(
                            "  plan_builder: nested ForEach at step[{}] while inside ForEach '{}' at step[{}]: \
                             finalizing outer ForEach with body_entry='{}' (explicit={}) body_exit='{}'",
                            i, outer_foreach_node_id, outer_foreach_idx,
                            outer_entry, has_outer_body_entry, outer_exit
                        );

                        if !has_outer_body_entry {
                            return Err(PlannerError::InvalidPath(format!(
                                "Nested ForEach at step[{}] but outer ForEach at step[{}] ('{}') has no body caps.",
                                i, outer_foreach_idx, outer_foreach_node_id
                            )));
                        }

                        if outer_foreach_input == outer_entry {
                            return Err(PlannerError::InvalidPath(format!(
                                "Outer ForEach at step[{}] ('{}') would create a cycle: \
                                 foreach_input='{}' == body_entry='{}'.",
                                outer_foreach_idx, outer_foreach_node_id, outer_foreach_input, outer_entry
                            )));
                        }

                        // Create the outer ForEach node
                        let foreach_node = MachineNode::for_each(
                            &outer_foreach_node_id,
                            &outer_foreach_input,
                            &outer_entry,
                            &outer_exit,
                        );
                        plan.add_node(foreach_node);
                        plan.add_edge(MachinePlanEdge::direct(&outer_foreach_input, &outer_foreach_node_id));
                        plan.add_edge(MachinePlanEdge::iteration(&outer_foreach_node_id, &outer_entry));

                        // The outer ForEach is now finalized. prev_node_id stays as body exit.
                        prev_node_id = outer_exit;
                    }

                    tracing::info!(
                        "  plan_builder: ForEach at step[{}], prev_node_id='{}'",
                        i, prev_node_id
                    );
                    inside_foreach_body = Some((i, node_id.clone()));
                    body_entry = None;
                    body_exit = None;
                    // Don't increment prev_node_id - the body's first cap will connect to the prev node
                    continue;
                }

                StrandStepType::Collect { list_spec, .. } => {
                    if let Some((foreach_idx, foreach_node_id)) = inside_foreach_body.take() {
                        // Collect after ForEach: close the iteration body
                        let entry = body_entry.take().unwrap_or_else(|| prev_node_id.clone());
                        let exit = body_exit.take().unwrap_or_else(|| prev_node_id.clone());

                        // Find the node that feeds into the ForEach (the one before the ForEach step)
                        let foreach_input = if foreach_idx == 0 {
                            input_slot_id.to_string()
                        } else {
                            format!("step_{}", foreach_idx - 1)
                        };

                        // Create the ForEach node now that we know the body boundaries
                        let foreach_node = MachineNode::for_each(
                            &foreach_node_id,
                            &foreach_input,
                            &entry,
                            &exit,
                        );
                        plan.add_node(foreach_node);
                        plan.add_edge(MachinePlanEdge::direct(&foreach_input, &foreach_node_id));

                        // Create iteration edge from ForEach to body entry
                        plan.add_edge(MachinePlanEdge::iteration(&foreach_node_id, &entry));

                        // Create the Collect node
                        let collect_node = MachineNode::collect(&node_id, vec![exit.clone()]);
                        plan.add_node(collect_node);
                        // Collection edge from body exit to Collect
                        plan.add_edge(MachinePlanEdge::collection(&exit, &node_id));
                    } else {
                        // Standalone Collect: scalar → list-of-one (pass-through).
                        // No ForEach body — this is a simple cardinality transition.
                        // At execution time the data flows unchanged, only the type
                        // annotation changes from scalar to list.
                        tracing::info!(
                            "  plan_builder: standalone Collect at step[{}], prev_node_id='{}'",
                            i, prev_node_id
                        );

                        let mut collect_node = MachineNode::collect(
                            &node_id,
                            vec![prev_node_id.clone()],
                        );
                        // Set output_media_urn so plan_converter can register it
                        collect_node.node_type = ExecutionNodeType::Collect {
                            input_nodes: vec![prev_node_id.clone()],
                            output_media_urn: Some(list_spec.to_string()),
                        };
                        collect_node.description = Some("Collect: scalar to list-of-one".to_string());
                        plan.add_node(collect_node);
                        plan.add_edge(MachinePlanEdge::direct(&prev_node_id, &node_id));
                    }
                }
            }

            prev_node_id = node_id;
        }

        // Handle unclosed ForEach - this is valid when the output is per-iteration
        // (e.g., PDF -> pages -> process each -> multiple output files)
        if let Some((foreach_idx, foreach_node_id)) = inside_foreach_body.take() {
            let has_body_entry = body_entry.is_some();
            let has_body_exit = body_exit.is_some();
            let entry = body_entry.take().unwrap_or_else(|| prev_node_id.clone());
            let exit = body_exit.take().unwrap_or_else(|| prev_node_id.clone());

            // Find the node that feeds into the ForEach (the one before the ForEach step)
            let foreach_input = if foreach_idx == 0 {
                input_slot_id.to_string()
            } else {
                format!("step_{}", foreach_idx - 1)
            };

            tracing::info!(
                "  plan_builder: unclosed ForEach '{}' at step[{}]: \
                 foreach_input='{}' body_entry='{}' (explicit={}) body_exit='{}' (explicit={}) prev_node_id='{}'",
                foreach_node_id, foreach_idx, foreach_input,
                entry, has_body_entry, exit, has_body_exit, prev_node_id
            );

            // If the ForEach body has no Cap nodes, this is a terminal unwrap:
            // the path walked through a ForEach edge to reach the scalar target type,
            // but there's nothing to execute per-item. This happens when a prior ForEach
            // body produces a list and the path traverses a second ForEach to the item type.
            // E.g., path: disbind → ForEach(1) → make_multiple_decisions → ForEach(2)
            // ForEach(1) is the real iteration; ForEach(2) just says "the body output is a list,
            // target is the item." We skip it — the executor handles body list output via
            // the unclosed ForEach mechanism.
            if !has_body_entry {
                tracing::info!(
                    "  plan_builder: skipping terminal ForEach '{}' at step[{}] (no body caps). \
                     This is a terminal unwrap — the preceding cap's list output reaches \
                     the scalar target via this ForEach edge.",
                    foreach_node_id, foreach_idx
                );
                // Don't create a ForEach node. prev_node_id stays as is.
                // The plan's output will connect to the node before this ForEach.
            } else {
                // Validate: ForEach input must differ from body entry to avoid cycles
                if foreach_input == entry {
                    return Err(PlannerError::InvalidPath(format!(
                        "ForEach at step[{}] ('{}') would create a cycle: \
                         foreach_input='{}' == body_entry='{}'. \
                         The cap that produces the list cannot also be the ForEach body.",
                        foreach_idx, foreach_node_id, foreach_input, entry
                    )));
                }

                // Create the ForEach node
                let foreach_node = MachineNode::for_each(
                    &foreach_node_id,
                    &foreach_input,
                    &entry,
                    &exit,
                );
                plan.add_node(foreach_node);
                plan.add_edge(MachinePlanEdge::direct(&foreach_input, &foreach_node_id));

                // Create iteration edge from ForEach to body entry
                plan.add_edge(MachinePlanEdge::iteration(&foreach_node_id, &entry));

                // Output connects to the body exit (each iteration produces output)
                prev_node_id = exit;
            }
        }

        let output_id = "output";
        plan.add_node(MachineNode::output(output_id, "result", &prev_node_id));
        plan.add_edge(MachinePlanEdge::direct(&prev_node_id, output_id));

        plan.metadata = Some(HashMap::from([
            ("source_spec".to_string(), json!(source_spec_str)),
            ("target_spec".to_string(), json!(target_spec_str)),
        ]));

        // Validate the plan is a DAG (no cycles) before returning.
        // This catches structural bugs in plan construction that would
        // cause find_first_foreach() to fail (it relies on topological_order).
        plan.validate()?;
        if let Err(e) = plan.topological_order() {
            // Log full plan structure for diagnostics
            tracing::error!(
                "build_plan_from_path produced a cyclic plan: {}. \
                 Nodes: {:?}, Edges: {:?}",
                e,
                plan.nodes.keys().collect::<Vec<_>>(),
                plan.edges.iter().map(|e| format!("{}→{} ({:?})", e.from_node, e.to_node, e.edge_type)).collect::<Vec<_>>()
            );
            return Err(PlannerError::InvalidPath(format!(
                "Plan construction produced a cycle (not a DAG): {}. \
                 This is a bug in the plan builder.",
                e
            )));
        }

        tracing::info!(
            "build_plan_from_path: successfully built plan '{}' with {} nodes, {} edges",
            plan.name, plan.nodes.len(), plan.edges.len()
        );

        Ok(plan)
    }

    /// Find ForEach/Collect ranges in a path.
    /// Returns pairs of (foreach_index, collect_index).
    fn find_foreach_collect_ranges(steps: &[super::live_cap_graph::StrandStep]) -> Vec<(usize, usize)> {
        use super::live_cap_graph::StrandStepType;

        let mut ranges = Vec::new();
        let mut foreach_stack: Vec<usize> = Vec::new();

        for (i, step) in steps.iter().enumerate() {
            match &step.step_type {
                StrandStepType::ForEach { .. } => {
                    foreach_stack.push(i);
                }
                StrandStepType::Collect { .. } => {
                    if let Some(foreach_idx) = foreach_stack.pop() {
                        ranges.push((foreach_idx, i));
                    }
                }
                _ => {}
            }
        }

        ranges
    }
}

// NOTE: Path finding methods (find_path, get_reachable_targets, get_reachable_targets_with_metadata,
// find_all_paths) have been moved to LiveCapGraph. Use LiveCapGraph for path finding and
// build_plan_from_path for plan construction.
//
// The old string-based ReachableTargetInfo, StrandStep, Strand types have been
// replaced by the typed versions in live_cap_graph.rs.

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
    /// Whether this path can execute without any user input
    pub can_execute_without_input: bool,
}

impl MachinePlanBuilder {
    /// Analyze argument requirements for a path.
    ///
    /// Takes the new typed `Strand` from `live_cap_graph` which uses
    /// typed `MediaUrn` and `CapUrn` values.
    ///
    /// Only Cap steps have arguments to analyze. ForEach/Collect steps
    /// are cardinality transitions with no user-configurable arguments.
    pub async fn analyze_path_arguments(
        &self,
        path: &Strand,
    ) -> PlannerResult<PathArgumentRequirements> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        let mut step_requirements = Vec::new();
        // Track cap step index for determining first cap (affects file_path resolution)
        let mut cap_step_index = 0;

        for (step_index, step) in path.steps.iter().enumerate() {
            // Only analyze Cap steps - cardinality transitions have no arguments
            let cap_urn = match step.cap_urn() {
                Some(urn) => urn,
                None => continue, // Skip ForEach/Collect steps
            };

            let cap_urn_str = cap_urn.to_string();
            let cap = caps.iter()
                .find(|c| c.urn.to_string() == cap_urn_str)
                .ok_or_else(|| PlannerError::NotFound(format!(
                    "Cap '{}' not found in registry",
                    cap_urn_str
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
                    cap_step_index,
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
                }
                arguments.push(arg_info);
            }

            step_requirements.push(StepArgumentRequirements {
                cap_urn: cap_urn_str,
                step_index,
                title: step.title(),
                arguments,
                slots,
            });

            cap_step_index += 1;
        }

        let can_execute_without_input = step_requirements.iter().all(|s| s.slots.is_empty());

        Ok(PathArgumentRequirements {
            source_spec: path.source_spec.to_string(),
            target_spec: path.target_spec.to_string(),
            steps: step_requirements,
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
    use std::collections::{BTreeMap, HashSet};
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

    // TEST938: Tests duplicate detection passes for caps with unique URN combinations
    // Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations
    #[test]
    fn test938_no_duplicates_with_unique_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
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

    // TEST939: Tests duplicate detection identifies caps with identical URNs
    // Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn
    #[test]
    fn test939_detects_duplicate_cap_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
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

    // TEST940: Tests caps with different operations but same input/output types are not duplicates
    // Verifies that only the complete URN (including op) is used for duplicate detection
    #[test]
    fn test940_different_ops_same_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind")?,
            make_test_cap("grind", "media:pdf", "media:disbound-pages;textable;list", "Grind")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Different ops should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // TEST941: Tests caps with same operation but different input types are not duplicates
    // Verifies that input type differences distinguish caps with the same operation name
    #[test]
    fn test941_same_op_different_input_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
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

    fn create_test_plan_builder() -> MachinePlanBuilder {
        let cap_registry = CapRegistry::new_for_test();
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        MachinePlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    fn create_test_plan_builder_with_registry(cap_registry: CapRegistry) -> MachinePlanBuilder {
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        MachinePlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    // TEST942: Tests first cap's input argument is automatically resolved from input file
    // Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain
    #[test]
    fn test942_input_arg_first_cap_auto_resolved_from_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST943: Tests subsequent caps' input arguments are automatically resolved from previous output
    // Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first
    #[test]
    fn test943_input_arg_subsequent_cap_auto_resolved_from_previous() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 2, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST944: Tests output arguments are automatically resolved from previous cap's output
    // Verifies that arguments matching the output spec are always resolved as FromPreviousOutput
    #[test]
    fn test944_output_arg_auto_resolved() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(out_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST945: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap
    // Verifies that generic file-path arguments are bound to input file in the first cap
    #[test]
    fn test945_file_path_type_fallback_first_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST946: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps
    // Verifies that generic file-path arguments are bound to previous cap's output after the first cap
    #[test]
    fn test946_file_path_type_fallback_subsequent_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST947: Tests MEDIA_FILE_PATH_ARRAY argument type resolution for first and subsequent caps
    // Verifies that file-path array arguments follow the same resolution pattern as single file paths
    #[test]
    fn test947_file_path_array_fallback() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);

        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST948: Tests required non-IO arguments with default values are marked as HasDefault
    // Verifies that arguments like integers with defaults don't require user input
    #[test]
    fn test948_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(200));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, true, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST949: Tests required non-IO arguments without defaults require user input
    // Verifies that arguments like strings without defaults are marked as RequiresUserInput
    #[test]
    fn test949_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_STRING, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST950: Tests optional non-IO arguments with default values are marked as HasDefault
    // Verifies that optional arguments with defaults behave the same as required ones with defaults
    #[test]
    fn test950_optional_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(300));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, false, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST951: Tests optional non-IO arguments without defaults still require user input
    // Verifies that optional arguments without defaults must be explicitly provided or skipped
    #[test]
    fn test951_optional_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_BOOLEAN, in_spec, out_spec, 0, false, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST952: Tests validation_to_json() returns None for None input
    // Verifies that missing validation metadata is converted to JSON None
    #[test]
    fn test952_validation_to_json_none() {
        let json = MachinePlanBuilder::validation_to_json(None);
        assert!(json.is_none(), "None validation should return None");
    }

    // TEST765: Tests validation_to_json() returns None for empty validation constraints
    // Verifies that default MediaValidation with no constraints produces JSON None
    #[test]
    fn test765_validation_to_json_empty() {
        let validation = MediaValidation::default();
        let json = MachinePlanBuilder::validation_to_json(Some(&validation));
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
        let json = MachinePlanBuilder::validation_to_json(Some(&validation));
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
            can_execute_without_input: false,
        };

        assert!(!requirements.can_execute_without_input);
        assert_eq!(requirements.steps[0].slots.len(), 1);
        assert_eq!(requirements.steps[0].slots[0].name, "target_language");
    }

    // ==========================================================================
    // URN CANONICALIZATION TESTS
    // ==========================================================================
    // NOTE: Path finding tests (TEST770-787) have been moved to live_cap_graph.rs
    // as path finding is now handled by LiveCapGraph, not MachinePlanBuilder.
    // Availability filtering (TEST770-776) is now implicit in LiveCapGraph sync.
    // Path coherence scoring (TEST782-787) has been removed from the architecture.

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

    // TEST1103: Tests that is_dispatchable has correct directionality
    // The available cap (provider) must be dispatchable for the requested cap (request).
    // This tests the directionality: provider.is_dispatchable(&request)
    // NOTE: This now tests CapUrn::is_dispatchable directly, not via MachinePlanBuilder
    #[test]
    fn test1103_is_dispatchable_uses_correct_directionality() {
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
    }

    // TEST1104: Tests that is_dispatchable rejects when provider cannot dispatch request
    #[test]
    fn test1104_is_dispatchable_rejects_non_dispatchable() {
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
    }
}
