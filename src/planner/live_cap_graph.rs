//! LiveCapGraph — Precomputed capability graph for path finding
//!
//! This module provides a live, incrementally-updated graph of capabilities
//! for efficient path finding and reachability queries. Unlike MachinePlanBuilder
//! which rebuilds the graph for each query, LiveCapGraph maintains a persistent
//! graph structure that is updated when capabilities change.
//!
//! ## Design Principles
//!
//! 1. **Typed URNs**: Store MediaUrn and CapUrn directly, not strings.
//!    This avoids reparsing and provides order-theoretic methods.
//!
//! 2. **Exact matching**: For target matching, use `is_equivalent()` not `conforms_to()`.
//!    This ensures "media:X" does NOT match paths ending in "media:X;list".
//!
//! 3. **Conformance for traversal**: Use `conforms_to()` only for graph traversal
//!    (can this output feed into that input?).
//!
//! 4. **Deterministic ordering**: Results are sorted by (path_length, specificity, urn).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::cap::registry::CapRegistry;
use crate::planner::cardinality::InputCardinality;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::Cap;

// =============================================================================
// DATA STRUCTURES
// =============================================================================

/// Type of edge in the capability graph.
///
/// Most edges represent actual capabilities, but some are synthetic edges
/// that represent cardinality transitions (fan-out, collect, wrap-in-list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveMachinePlanEdgeType {
    /// A real capability that transforms media
    Cap {
        cap_urn: CapUrn,
        cap_title: String,
        specificity: usize,
    },
    /// Fan-out: splits a list into individual items for iteration
    /// Inserted when list output feeds into singular-expecting cap
    ForEach,
    /// Collect: gathers iteration results back into a list
    /// Paired with ForEach to complete the fan-out/fan-in pattern
    Collect,
    /// Wrap: wraps a single item into a list
    /// Inserted when singular output feeds into list-expecting cap
    WrapInList,
}

/// An edge in the live capability graph.
///
/// Each edge represents either:
/// - A capability that transforms from one media type to another
/// - A cardinality transition (ForEach/Collect/WrapInList)
///
/// URNs are stored as typed values, not strings, for efficient order-theoretic operations.
#[derive(Debug, Clone)]
pub struct LiveMachinePlanEdge {
    /// Input media type (what this edge consumes)
    pub from_spec: MediaUrn,
    /// Output media type (what this edge produces)
    pub to_spec: MediaUrn,
    /// Type of edge (cap or cardinality transition)
    pub edge_type: LiveMachinePlanEdgeType,
    /// Input cardinality (derived from from_spec)
    pub input_cardinality: InputCardinality,
    /// Output cardinality (derived from to_spec)
    pub output_cardinality: InputCardinality,
}

/// Precomputed graph of capabilities for path finding.
///
/// This graph is designed to be:
/// - Updated incrementally when caps change
/// - Queried efficiently for reachability and path finding
/// - Deterministic in its results
#[derive(Debug)]
pub struct LiveCapGraph {
    /// All edges in the graph
    edges: Vec<LiveMachinePlanEdge>,
    /// Index: from_spec (canonical string) → edge indices
    /// Uses canonical string as key because MediaUrn doesn't implement Hash
    outgoing: HashMap<String, Vec<usize>>,
    /// Index: to_spec (canonical string) → edge indices
    incoming: HashMap<String, Vec<usize>>,
    /// All unique media URN nodes (canonical strings)
    nodes: HashSet<String>,
    /// Cap URN (canonical string) → edge indices for removal
    cap_to_edges: HashMap<String, Vec<usize>>,
}

/// Information about a reachable target from a source media type.
#[derive(Debug, Clone)]
pub struct ReachableTargetInfo {
    /// The target media URN
    pub media_spec: MediaUrn,
    /// Human-readable display name (from media registry)
    pub display_name: String,
    /// Minimum number of steps to reach this target
    pub min_path_length: i32,
    /// Number of distinct paths to this target
    pub path_count: i32,
}

impl LiveMachinePlanEdge {
    /// Get the title for this edge (for display purposes)
    pub fn title(&self) -> String {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { cap_title, .. } => cap_title.clone(),
            LiveMachinePlanEdgeType::ForEach => "ForEach (iterate over list)".to_string(),
            LiveMachinePlanEdgeType::Collect => "Collect (gather results)".to_string(),
            LiveMachinePlanEdgeType::WrapInList => "WrapInList (create single-item list)".to_string(),
        }
    }

    /// Get the specificity of this edge (for ordering purposes)
    pub fn specificity(&self) -> usize {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { specificity, .. } => *specificity,
            // Cardinality transitions have no specificity preference
            LiveMachinePlanEdgeType::ForEach | LiveMachinePlanEdgeType::Collect | LiveMachinePlanEdgeType::WrapInList => 0,
        }
    }

    /// Check if this is a cap edge (not a cardinality transition)
    pub fn is_cap(&self) -> bool {
        matches!(self.edge_type, LiveMachinePlanEdgeType::Cap { .. })
    }

    /// Get the cap URN if this is a cap edge
    pub fn cap_urn(&self) -> Option<&CapUrn> {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }
}

/// Type of step in a capability chain path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StrandStepType {
    /// A real capability step
    Cap {
        cap_urn: CapUrn,
        title: String,
        specificity: usize,
    },
    /// Fan-out: iterate over list items
    ForEach {
        /// The list media type being split
        list_spec: MediaUrn,
        /// The item media type (list without ;list marker)
        item_spec: MediaUrn,
    },
    /// Collect: gather iteration results
    Collect {
        /// The item media type being collected
        item_spec: MediaUrn,
        /// The list media type (item with ;list marker)
        list_spec: MediaUrn,
    },
    /// Wrap single item in list
    WrapInList {
        /// The item media type
        item_spec: MediaUrn,
        /// The list media type
        list_spec: MediaUrn,
    },
}

/// Information about a single step in a capability chain path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrandStep {
    /// Type of step (cap or cardinality transition)
    pub step_type: StrandStepType,
    /// Input media type for this step
    pub from_spec: MediaUrn,
    /// Output media type for this step
    pub to_spec: MediaUrn,
}

impl StrandStep {
    /// Get the title for this step (for display purposes)
    pub fn title(&self) -> String {
        match &self.step_type {
            StrandStepType::Cap { title, .. } => title.clone(),
            StrandStepType::ForEach { .. } => "ForEach".to_string(),
            StrandStepType::Collect { .. } => "Collect".to_string(),
            StrandStepType::WrapInList { .. } => "WrapInList".to_string(),
        }
    }

    /// Get the specificity of this step (for ordering purposes)
    pub fn specificity(&self) -> usize {
        match &self.step_type {
            StrandStepType::Cap { specificity, .. } => *specificity,
            _ => 0,
        }
    }

    /// Get the cap URN if this is a cap step
    pub fn cap_urn(&self) -> Option<&CapUrn> {
        match &self.step_type {
            StrandStepType::Cap { cap_urn, .. } => Some(cap_urn),
            _ => None,
        }
    }

    /// Check if this is a cap step
    pub fn is_cap(&self) -> bool {
        matches!(self.step_type, StrandStepType::Cap { .. })
    }
}

/// Information about a complete capability chain path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Strand {
    /// Steps in the path, in order
    pub steps: Vec<StrandStep>,
    /// Source media URN
    pub source_spec: MediaUrn,
    /// Target media URN
    pub target_spec: MediaUrn,
    /// Total number of steps (including cardinality transitions)
    pub total_steps: i32,
    /// Number of cap steps only (excluding ForEach/Collect/WrapInList)
    /// This is used for sorting - cardinality transitions don't count as "steps" for user display
    pub cap_step_count: i32,
    /// Human-readable description
    pub description: String,
}

impl Strand {
    /// Convert this resolved path to a machine graph.
    ///
    /// Cap steps become edges; ForEach sets `is_loop` on the next cap;
    /// Collect and WrapInList are elided (implicit in transitions).
    pub fn knit(&self) -> crate::machine::Machine {
        crate::machine::Machine::from_path(self)
    }

    /// Serialize this resolved path to canonical one-line machine notation.
    ///
    /// This is the primary identifier for accessibility and comparison.
    pub fn to_machine_notation(&self) -> String {
        self.knit().to_machine_notation()
    }
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl LiveCapGraph {
    /// Create a new empty capability graph.
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            nodes: HashSet::new(),
            cap_to_edges: HashMap::new(),
        }
    }

    /// Clear the graph completely.
    pub fn clear(&mut self) {
        self.edges.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.nodes.clear();
        self.cap_to_edges.clear();
    }

    /// Rebuild the graph from a list of capabilities.
    ///
    /// This completely replaces the current graph contents.
    /// Call this when the set of available capabilities changes.
    ///
    /// After adding all cap edges, this method inserts cardinality transition
    /// edges (ForEach/Collect) to enable paths through list→singular boundaries.
    pub fn sync_from_caps(&mut self, caps: &[Cap]) {
        self.clear();

        for cap in caps {
            self.add_cap(cap);
        }

        // Insert cardinality transition edges (ForEach/Collect)
        self.insert_cardinality_transitions();

        tracing::debug!(
            edge_count = self.edges.len(),
            node_count = self.nodes.len(),
            "[LiveCapGraph] Synced from {} caps",
            caps.len()
        );
    }

    /// Rebuild the graph from a list of cap URN strings using the registry.
    ///
    /// This is the primary method for RelaySwitch integration. Given the list of
    /// available cap URN strings (from plugins), it looks up the Cap definitions
    /// from the registry and builds the graph.
    ///
    /// Caps are matched by equivalence (`is_equivalent`): the plugin's reported URN
    /// must have an exact semantic match in the registry. Unmatched caps are rejected
    /// with an error and excluded from the graph — a plugin advertising an unregistered
    /// capability is a configuration bug that must be fixed.
    pub async fn sync_from_cap_urns(&mut self, cap_urns: &[String], registry: &Arc<CapRegistry>) {
        self.clear();

        // Get all cached caps from registry
        let all_caps = match registry.get_cached_caps().await {
            Ok(caps) => caps,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "[LiveCapGraph] Failed to get cached caps from registry"
                );
                return;
            }
        };

        tracing::info!(
            registry_cap_count = all_caps.len(),
            "[LiveCapGraph] Got caps from registry"
        );

        let mut matched_count = 0;
        let mut identity_count = 0;
        let mut rejected_count = 0;

        for cap_urn_str in cap_urns.iter() {
            // Parse the cap URN
            let cap_urn = match CapUrn::from_string(cap_urn_str) {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(
                        cap_urn = cap_urn_str,
                        error = %e,
                        "[LiveCapGraph] Plugin reported invalid cap URN - this is a bug in the plugin"
                    );
                    continue;
                }
            };

            // Skip identity caps - they don't contribute to path finding
            if cap_urn.is_equivalent(&crate::standard::caps::identity_urn()) {
                identity_count += 1;
                continue;
            }

            // Find the exact matching Cap in registry using is_equivalent.
            // The plugin reports the specific cap URN it implements — we need to find
            // that same cap in the registry. Using is_dispatchable here was wrong because
            // it would match a wildcard registry cap (e.g. in=media:) before reaching
            // the specific one (e.g. in=media:txt;textable), since .find() returns the
            // first match.
            let matching_cap = all_caps.iter().find(|registry_cap| {
                cap_urn.is_equivalent(&registry_cap.urn)
            });

            match matching_cap {
                Some(cap) => {
                    self.add_cap(cap);
                    matched_count += 1;
                }
                None => {
                    rejected_count += 1;
                    tracing::error!(
                        cap_urn = %cap_urn,
                        cap_urn_raw = cap_urn_str,
                        "[LiveCapGraph] REJECTED: plugin reported cap URN has no equivalent \
                        in the registry. Every cap a plugin provides must have a matching \
                        registry definition. Either the plugin is advertising an unknown \
                        capability or the registry is missing a cap definition for this URN. \
                        This cap will NOT be added to the graph."
                    );
                }
            }
        }

        // Insert cardinality transition edges (ForEach/Collect)
        self.insert_cardinality_transitions();

        tracing::info!(
            edge_count = self.edges.len(),
            node_count = self.nodes.len(),
            matched_count,
            identity_count,
            rejected_count,
            total_urns = cap_urns.len(),
            "[LiveCapGraph] Synced from cap URNs"
        );
    }

    /// Add a capability as an edge in the graph.
    pub fn add_cap(&mut self, cap: &Cap) {
        let in_spec_str = cap.urn.in_spec();
        let out_spec_str = cap.urn.out_spec();

        // Skip caps with empty specs
        if in_spec_str.is_empty() || out_spec_str.is_empty() {
            tracing::warn!(
                cap_urn = %cap.urn,
                in_spec = in_spec_str,
                out_spec = out_spec_str,
                "[LiveCapGraph] Skipping cap with empty spec"
            );
            return;
        }

        // Skip identity caps (passthrough caps that don't transform anything)
        // These are is_equivalent to the CAP_IDENTITY constant
        if cap.urn.is_equivalent(&crate::standard::caps::identity_urn()) {
            return;
        }

        // Parse media URNs
        let from_spec = match MediaUrn::from_string(in_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    in_spec = in_spec_str,
                    error = %e,
                    "[LiveCapGraph] Failed to parse in_spec, skipping cap"
                );
                return;
            }
        };

        let to_spec = match MediaUrn::from_string(out_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    out_spec = out_spec_str,
                    error = %e,
                    "[LiveCapGraph] Failed to parse out_spec, skipping cap"
                );
                return;
            }
        };

        let from_canonical = from_spec.to_string();
        let to_canonical = to_spec.to_string();
        let cap_canonical = cap.urn.to_string();

        // Determine cardinality from media URNs
        let input_cardinality = InputCardinality::from_media_urn(&from_canonical);
        let output_cardinality = InputCardinality::from_media_urn(&to_canonical);

        // Create edge
        let edge_idx = self.edges.len();
        let edge = LiveMachinePlanEdge {
            from_spec,
            to_spec,
            edge_type: LiveMachinePlanEdgeType::Cap {
                cap_urn: cap.urn.clone(),
                cap_title: cap.title.clone(),
                specificity: cap.urn.specificity(),
            },
            input_cardinality,
            output_cardinality,
        };
        self.edges.push(edge);

        // Update indices
        self.outgoing.entry(from_canonical.clone()).or_default().push(edge_idx);
        self.incoming.entry(to_canonical.clone()).or_default().push(edge_idx);
        self.nodes.insert(from_canonical);
        self.nodes.insert(to_canonical);
        self.cap_to_edges.entry(cap_canonical).or_default().push(edge_idx);
    }

    /// Get all edges originating from a source media URN.
    ///
    /// Uses `conforms_to()` matching: returns edges where the source
    /// conforms to the edge's from_spec requirement.
    fn get_outgoing_edges(&self, source: &MediaUrn) -> Vec<&LiveMachinePlanEdge> {
        self.edges
            .iter()
            .filter(|edge| {
                // Check cardinality compatibility:
                // - If edge expects singular (no list), source must also be singular
                // - If edge expects list, source must be a list
                let edge_expects_list = edge.from_spec.is_list();
                let source_is_list = source.is_list();

                // Cardinality must match for Cap edges
                // ForEach edges handle list→singular, Collect edges handle singular→list
                let cardinality_compatible = match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { .. } => edge_expects_list == source_is_list,
                    LiveMachinePlanEdgeType::ForEach => source_is_list && !edge.to_spec.is_list(),
                    LiveMachinePlanEdgeType::Collect => !source_is_list && edge.to_spec.is_list(),
                    LiveMachinePlanEdgeType::WrapInList => !source_is_list && edge.to_spec.is_list(),
                };

                if !cardinality_compatible {
                    return false;
                }

                // Then check type conformance (ignoring list for the base type check)
                source.conforms_to(&edge.from_spec).unwrap_or(false)
            })
            .collect()
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> (usize, usize) {
        (self.nodes.len(), self.edges.len())
    }

    /// Insert ForEach/Collect edges for cardinality transitions.
    ///
    /// This enables paths like: `pdf → disbind → page;list → ForEach → page → analyze → Collect → result;list`
    ///
    /// For each list-typed node in the graph, we check if there are caps that accept
    /// the singular (non-list) version. If so, we insert:
    /// - ForEach edge: list_spec → item_spec (fan-out)
    ///
    /// Collect edges are NOT pre-inserted. Instead, when a path requires collecting
    /// results back into a list, the path finding will need to handle this dynamically
    /// or the executor will handle the implicit collection.
    fn insert_cardinality_transitions(&mut self) {
        // Collect all unique list-typed output specs from existing edges
        // Use BTreeSet for deterministic iteration order (sorted by MediaUrn)
        let list_outputs: Vec<MediaUrn> = self.edges
            .iter()
            .filter(|edge| edge.to_spec.is_list())
            .map(|edge| edge.to_spec.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        tracing::info!(
            list_output_count = list_outputs.len(),
            edge_count = self.edges.len(),
            "[LiveCapGraph] insert_cardinality_transitions: found list-typed outputs"
        );

        if list_outputs.is_empty() {
            return;
        }

        // For each list output, check if we have caps that accept the singular version
        let mut foreach_edges_to_add: Vec<(MediaUrn, MediaUrn)> = Vec::new();

        for list_spec in &list_outputs {
            // Get the item spec by removing the list tag
            let item_spec = list_spec.without_tag("list");

            // Check if any edge accepts this item spec (or something it conforms to)
            let has_singular_consumer = self.edges.iter().any(|edge| {
                // The item must conform to the edge's input spec
                item_spec.conforms_to(&edge.from_spec).unwrap_or(false)
            });

            if has_singular_consumer {
                foreach_edges_to_add.push((list_spec.clone(), item_spec));
            }
        }

        // Now add the ForEach edges
        for (list_spec, item_spec) in foreach_edges_to_add {
            let list_canonical = list_spec.to_string();
            let item_canonical = item_spec.to_string();

            // Add ForEach edge: list → item
            let foreach_edge_idx = self.edges.len();
            let foreach_edge = LiveMachinePlanEdge {
                from_spec: list_spec.clone(),
                to_spec: item_spec.clone(),
                edge_type: LiveMachinePlanEdgeType::ForEach,
                input_cardinality: InputCardinality::Sequence,
                output_cardinality: InputCardinality::Single,
            };
            self.edges.push(foreach_edge);

            // Update indices
            self.outgoing.entry(list_canonical.clone()).or_default().push(foreach_edge_idx);
            self.incoming.entry(item_canonical.clone()).or_default().push(foreach_edge_idx);
            self.nodes.insert(list_canonical);
            self.nodes.insert(item_canonical.clone());

            tracing::info!(
                list_spec = %list_spec,
                item_spec = %item_spec,
                "[LiveCapGraph] Inserted ForEach edge for cardinality transition"
            );
        }

        // Note: Collect edges are inserted lazily - only for list specs that actually
        // exist as outputs from caps. This avoids creating invalid media specs.
        self.insert_collect_edges_for_existing_lists();
    }

    /// Insert Collect edges only for list specs that already exist as cap outputs.
    ///
    /// This is more conservative than creating arbitrary list versions of specs.
    /// It only creates Collect edges where:
    /// 1. A cap outputs a list (e.g., disbind outputs page;list)
    /// 2. We have a ForEach edge that unwraps it (page;list → page)
    /// 3. After processing the singular items, we need to collect back to page;list
    fn insert_collect_edges_for_existing_lists(&mut self) {
        // Find all list specs that exist as cap outputs (these are "real" list types)
        // Use BTreeSet for deterministic iteration order (sorted by MediaUrn)
        let existing_list_outputs: Vec<MediaUrn> = self.edges
            .iter()
            .filter(|edge| {
                matches!(edge.edge_type, LiveMachinePlanEdgeType::Cap { .. }) && edge.to_spec.is_list()
            })
            .map(|edge| edge.to_spec.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let mut collect_edges_to_add: Vec<(MediaUrn, MediaUrn)> = Vec::new();

        for list_spec in &existing_list_outputs {
            // The item spec is the list spec without the list tag
            let item_spec = list_spec.without_tag("list");

            // Check if we have any cap that outputs the singular version
            // (this would be the result of processing each item)
            let has_singular_output = self.edges.iter().any(|edge| {
                matches!(edge.edge_type, LiveMachinePlanEdgeType::Cap { .. }) &&
                !edge.to_spec.is_list() &&
                edge.to_spec.is_equivalent(&item_spec).unwrap_or(false)
            });

            // Also check if we have any caps that could process the item
            // and produce something we'd want to collect
            let has_item_consumer = self.edges.iter().any(|edge| {
                matches!(edge.edge_type, LiveMachinePlanEdgeType::Cap { .. }) &&
                item_spec.conforms_to(&edge.from_spec).unwrap_or(false)
            });

            if has_singular_output || has_item_consumer {
                collect_edges_to_add.push((item_spec, list_spec.clone()));
            }
        }

        // Add the Collect edges
        for (item_spec, list_spec) in collect_edges_to_add {
            let item_canonical = item_spec.to_string();
            let list_canonical = list_spec.to_string();

            // Check if this collect edge already exists
            let already_exists = self.edges.iter().any(|edge| {
                matches!(&edge.edge_type, LiveMachinePlanEdgeType::Collect) &&
                edge.from_spec.is_equivalent(&item_spec).unwrap_or(false) &&
                edge.to_spec.is_equivalent(&list_spec).unwrap_or(false)
            });

            if already_exists {
                continue;
            }

            // Add Collect edge: item → list
            let collect_edge_idx = self.edges.len();
            let collect_edge = LiveMachinePlanEdge {
                from_spec: item_spec.clone(),
                to_spec: list_spec.clone(),
                edge_type: LiveMachinePlanEdgeType::Collect,
                input_cardinality: InputCardinality::Single,
                output_cardinality: InputCardinality::Sequence,
            };
            self.edges.push(collect_edge);

            // Update indices
            self.outgoing.entry(item_canonical.clone()).or_default().push(collect_edge_idx);
            self.incoming.entry(list_canonical.clone()).or_default().push(collect_edge_idx);
            self.nodes.insert(item_canonical);
            self.nodes.insert(list_canonical);

            tracing::info!(
                item_spec = %item_spec,
                list_spec = %list_spec,
                "[LiveCapGraph] Inserted Collect edge for existing list type"
            );
        }
    }

    // =========================================================================
    // REACHABLE TARGETS (BFS)
    // =========================================================================

    /// Find all reachable targets from a source media URN.
    ///
    /// Uses BFS to explore the graph up to max_depth steps.
    /// Returns targets sorted by (min_path_length, display_name).
    pub fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        let mut results: HashMap<String, ReachableTargetInfo> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(MediaUrn, usize)> = VecDeque::new();

        let source_canonical = source.to_string();
        queue.push_back((source.clone(), 0));
        visited.insert(source_canonical);

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for edge in self.get_outgoing_edges(&current) {
                let new_depth = depth + 1;
                let output_canonical = edge.to_spec.to_string();

                // Record this target
                let entry = results.entry(output_canonical.clone()).or_insert_with(|| {
                    ReachableTargetInfo {
                        media_spec: edge.to_spec.clone(),
                        display_name: output_canonical.clone(), // Will be enriched by caller
                        min_path_length: new_depth as i32,
                        path_count: 0,
                    }
                });
                entry.path_count += 1;

                // Continue BFS if not visited
                if !visited.contains(&output_canonical) {
                    visited.insert(output_canonical);
                    queue.push_back((edge.to_spec.clone(), new_depth));
                }
            }
        }

        // Sort by (min_path_length, display_name)
        let mut targets: Vec<_> = results.into_values().collect();
        targets.sort_by(|a, b| {
            a.min_path_length.cmp(&b.min_path_length)
                .then_with(|| a.display_name.cmp(&b.display_name))
        });

        targets
    }

    // =========================================================================
    // PATH FINDING (DFS with exact target matching)
    // =========================================================================

    /// Find all paths from source to target media URN.
    ///
    /// **Critical**: Uses `is_equivalent()` for target matching, NOT `conforms_to()`.
    /// This ensures exact matching: "media:X" will NOT match "media:X;list".
    ///
    /// Returns paths sorted by (total_steps, total_specificity desc, cap_urns).
    pub fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<Strand> {
        // Check if source already satisfies target
        if source.is_equivalent(target).unwrap_or(false) {
            return vec![];
        }

        tracing::info!(
            "find_paths_to_exact_target: source={} target={} max_depth={} max_paths={}",
            source, target, max_depth, max_paths
        );

        // Iterative deepening: find ALL paths at depth N before any at depth N+1.
        // This guarantees completeness at each depth level regardless of edge order,
        // preventing the old DFS bug where max_paths budget was exhausted by deep
        // subtrees before shorter paths through later edges were discovered.
        let mut all_paths: Vec<Strand> = Vec::new();

        for depth_limit in 1..=max_depth {
            if all_paths.len() >= max_paths {
                break;
            }

            let mut current_path: Vec<StrandStep> = Vec::new();
            let mut visited: HashSet<String> = HashSet::new();

            self.iddfs_find_paths(
                source,
                target,
                source,
                &mut current_path,
                &mut visited,
                &mut all_paths,
                depth_limit,
                max_paths,
            );
        }

        tracing::info!(
            "find_paths_to_exact_target: found {} paths (max_paths was {})",
            all_paths.len(), max_paths
        );

        // Sort paths deterministically
        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        all_paths
    }

    /// Depth-limited DFS helper for iterative deepening path finding.
    ///
    /// Only records paths whose length equals `depth_limit` exactly.
    /// Paths shorter than `depth_limit` were already found in earlier iterations.
    fn iddfs_find_paths(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        current: &MediaUrn,
        current_path: &mut Vec<StrandStep>,
        visited: &mut HashSet<String>,
        all_paths: &mut Vec<Strand>,
        depth_limit: usize,
        max_paths: usize,
    ) {
        if all_paths.len() >= max_paths {
            return;
        }

        // Check if we've reached the EXACT target using is_equivalent()
        // is_equivalent: same tag set, order-independent
        if current.is_equivalent(target).unwrap_or(false) {
            // Only record paths at exactly this depth limit.
            // Shorter paths were recorded in earlier iterations.
            if current_path.len() == depth_limit {
                let description = current_path
                    .iter()
                    .map(|s| s.title())
                    .collect::<Vec<_>>()
                    .join(" → ");

                let cap_step_count = current_path.iter().filter(|s| s.is_cap()).count() as i32;

                all_paths.push(Strand {
                    steps: current_path.clone(),
                    source_spec: source.clone(),
                    target_spec: target.clone(),
                    total_steps: current_path.len() as i32,
                    cap_step_count,
                    description,
                });
            }
            // Don't explore past target regardless of depth
            return;
        }

        // Can't go deeper — haven't reached target at this depth
        if current_path.len() >= depth_limit {
            return;
        }

        let current_canonical = current.to_string();
        visited.insert(current_canonical.clone());

        for edge in self.get_outgoing_edges(current) {
            let next_canonical = edge.to_spec.to_string();

            if !visited.contains(&next_canonical) {
                let step_type = match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { cap_urn, cap_title, specificity } => {
                        StrandStepType::Cap {
                            cap_urn: cap_urn.clone(),
                            title: cap_title.clone(),
                            specificity: *specificity,
                        }
                    }
                    LiveMachinePlanEdgeType::ForEach => {
                        StrandStepType::ForEach {
                            list_spec: edge.from_spec.clone(),
                            item_spec: edge.to_spec.clone(),
                        }
                    }
                    LiveMachinePlanEdgeType::Collect => {
                        StrandStepType::Collect {
                            item_spec: edge.from_spec.clone(),
                            list_spec: edge.to_spec.clone(),
                        }
                    }
                    LiveMachinePlanEdgeType::WrapInList => {
                        StrandStepType::WrapInList {
                            item_spec: edge.from_spec.clone(),
                            list_spec: edge.to_spec.clone(),
                        }
                    }
                };

                current_path.push(StrandStep {
                    step_type,
                    from_spec: edge.from_spec.clone(),
                    to_spec: edge.to_spec.clone(),
                });

                self.iddfs_find_paths(
                    source,
                    target,
                    &edge.to_spec,
                    current_path,
                    visited,
                    all_paths,
                    depth_limit,
                    max_paths,
                );

                current_path.pop();
            }
        }

        visited.remove(&current_canonical);
    }

    /// Compare two paths for deterministic ordering.
    ///
    /// Sort by:
    /// 1. cap_step_count (ascending - fewer actual cap steps first)
    ///    Note: ForEach/Collect/WrapInList don't count as "steps" for sorting
    /// 2. total specificity (descending - more specific first)
    /// 3. cap URNs lexicographically (for tie-breaking stability)
    fn compare_paths(a: &Strand, b: &Strand) -> Ordering {
        a.cap_step_count.cmp(&b.cap_step_count)
            .then_with(|| {
                // Higher specificity first
                let spec_a: usize = a.steps.iter().map(|s| s.specificity()).sum();
                let spec_b: usize = b.steps.iter().map(|s| s.specificity()).sum();
                spec_b.cmp(&spec_a)
            })
            .then_with(|| {
                // Lexicographic by step type (only for tie-breaking)
                // For cap steps, use cap URN; for cardinality steps, use type name
                let step_key = |s: &StrandStep| -> String {
                    match &s.step_type {
                        StrandStepType::Cap { cap_urn, .. } => cap_urn.to_string(),
                        StrandStepType::ForEach { .. } => "foreach".to_string(),
                        StrandStepType::Collect { .. } => "collect".to_string(),
                        StrandStepType::WrapInList { .. } => "wrapinlist".to_string(),
                    }
                };
                let keys_a: Vec<String> = a.steps.iter().map(step_key).collect();
                let keys_b: Vec<String> = b.steps.iter().map(step_key).collect();
                keys_a.cmp(&keys_b)
            })
    }
}

impl Default for LiveCapGraph {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::Cap;
    use crate::urn::cap_urn::CapUrn;

    fn make_test_cap(in_spec: &str, out_spec: &str, op: &str, title: &str) -> Cap {
        use crate::urn::cap_urn::CapUrnBuilder;

        let cap_urn = CapUrnBuilder::new()
            .in_spec(in_spec)
            .out_spec(out_spec)
            .tag("op", op)
            .build()
            .expect("Failed to build test cap URN");

        Cap {
            urn: cap_urn,
            title: title.to_string(),
            cap_description: None,
            metadata: Default::default(),
            command: "test".to_string(),
            media_specs: vec![],
            output: None,
            args: vec![],
            metadata_json: None,
            registered_by: None,
        }
    }

    #[test]
    fn test_add_cap_and_basic_traversal() {
        let mut graph = LiveCapGraph::new();

        let cap = make_test_cap("media:pdf", "media:extracted-text", "extract_text", "Extract Text");
        graph.add_cap(&cap);

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let targets = graph.get_reachable_targets(&source, 5);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].min_path_length, 1);
    }

    #[test]
    fn test_exact_vs_conformance_matching() {
        // First verify our assumption about is_equivalent
        let singular = MediaUrn::from_string("media:analysis-result").unwrap();
        let list = MediaUrn::from_string("media:analysis-result;list").unwrap();

        // These should NOT be equivalent
        assert!(
            !singular.is_equivalent(&list).unwrap(),
            "singular and list should NOT be equivalent"
        );
        assert!(
            !list.is_equivalent(&singular).unwrap(),
            "list and singular should NOT be equivalent (reverse check)"
        );

        let mut graph = LiveCapGraph::new();

        // Add cap: pdf -> result (singular)
        let cap1 = make_test_cap(
            "media:pdf",
            "media:analysis-result",
            "analyze",
            "Analyze PDF"
        );
        graph.add_cap(&cap1);

        // Add cap: pdf -> result;list (plural)
        let cap2 = make_test_cap(
            "media:pdf",
            "media:analysis-result;list",
            "analyze_multi",
            "Analyze PDF Multi"
        );
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();

        // Query for EXACT target: singular result
        let target_singular = MediaUrn::from_string("media:analysis-result").unwrap();
        let paths_singular = graph.find_paths_to_exact_target(&source, &target_singular, 5, 10);

        // Should find exactly 1 path (not both!)
        assert_eq!(paths_singular.len(), 1, "singular query should find exactly 1 path");
        assert_eq!(paths_singular[0].steps[0].title(), "Analyze PDF");

        // Query for EXACT target: result;list (plural)
        let target_plural = MediaUrn::from_string("media:analysis-result;list").unwrap();
        let paths_plural = graph.find_paths_to_exact_target(&source, &target_plural, 5, 10);

        // Should find exactly 1 path (not both!)
        assert_eq!(paths_plural.len(), 1, "list query should find exactly 1 path");
        assert_eq!(paths_plural[0].steps[0].title(), "Analyze PDF Multi");
    }

    #[test]
    fn test_multi_step_path() {
        let mut graph = LiveCapGraph::new();

        // pdf -> extracted-text
        let cap1 = make_test_cap("media:pdf", "media:extracted-text", "extract", "Extract");
        // extracted-text -> summary-text
        let cap2 = make_test_cap("media:extracted-text", "media:summary-text", "summarize", "Summarize");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:summary-text").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_steps, 2);
        assert_eq!(paths[0].steps[0].title(), "Extract");
        assert_eq!(paths[0].steps[1].title(), "Summarize");
    }

    #[test]
    fn test_deterministic_ordering() {
        let mut graph = LiveCapGraph::new();

        // Two paths to the same target with different specificities
        let cap1 = make_test_cap("media:pdf", "media:extracted-text", "extract_a", "Extract A");
        let cap2 = make_test_cap("media:pdf", "media:extracted-text", "extract_b", "Extract B");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:extracted-text").unwrap();

        // Run multiple times - should always get the same order
        let paths1 = graph.find_paths_to_exact_target(&source, &target, 5, 10);
        let paths2 = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert_eq!(paths1.len(), paths2.len());
        for (p1, p2) in paths1.iter().zip(paths2.iter()) {
            // Compare cap URNs for cap steps
            let urn1 = p1.steps[0].cap_urn().map(|u| u.to_string());
            let urn2 = p2.steps[0].cap_urn().map(|u| u.to_string());
            assert_eq!(urn1, urn2);
        }
    }

    #[test]
    fn test_sync_from_caps() {
        let mut graph = LiveCapGraph::new();

        let caps = vec![
            make_test_cap("media:pdf", "media:extracted-text", "op1", "Op1"),
            make_test_cap("media:extracted-text", "media:summary-text", "op2", "Op2"),
        ];

        graph.sync_from_caps(&caps);

        assert_eq!(graph.edges.len(), 2);
        assert_eq!(graph.nodes.len(), 3);

        // Sync again with different caps - should replace
        let new_caps = vec![
            make_test_cap("media:image", "media:extracted-text", "ocr", "OCR"),
        ];

        graph.sync_from_caps(&new_caps);

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);
    }

    // ==========================================================================
    // PATH FINDING TESTS (moved from plan_builder.rs)
    // ==========================================================================
    // These tests verify path finding behavior. Availability filtering is now
    // implicit - only caps added to the graph via sync_from_caps are available.

    // TEST772: Tests find_paths_to_exact_target() finds multi-step paths
    // Verifies that paths through intermediate nodes are found correctly
    #[test]
    fn test772_find_paths_finds_multi_step_paths() {
        let mut graph = LiveCapGraph::new();

        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        let cap2 = make_test_cap("media:b", "media:c", "step2", "B to C");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert_eq!(paths.len(), 1, "Should find one path through intermediate node");
        assert_eq!(paths[0].steps.len(), 2, "Path should have 2 steps (A->B, B->C)");
        assert_eq!(paths[0].steps[0].title(), "A to B");
        assert_eq!(paths[0].steps[1].title(), "B to C");
    }

    // TEST773: Tests find_paths_to_exact_target() returns empty when no path exists
    // Verifies that pathfinding returns no paths when target is unreachable
    #[test]
    fn test773_find_paths_returns_empty_when_no_path() {
        let mut graph = LiveCapGraph::new();

        // Only add cap A->B, not B->C
        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        graph.add_cap(&cap1);

        let source = MediaUrn::from_string("media:a").unwrap();
        let target = MediaUrn::from_string("media:c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert!(paths.is_empty(), "Should find no paths when target is unreachable");
    }

    // TEST774: Tests get_reachable_targets() returns all reachable targets
    // Verifies that reachable targets include direct and multi-step targets
    #[test]
    fn test774_get_reachable_targets_finds_all_targets() {
        let mut graph = LiveCapGraph::new();

        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        let cap2 = make_test_cap("media:a", "media:d", "step3", "A to D");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:a").unwrap();
        let targets = graph.get_reachable_targets(&source, 5);

        assert_eq!(targets.len(), 2, "Should find 2 reachable targets (B and D)");

        let target_specs: Vec<String> = targets.iter()
            .map(|t| t.media_spec.to_string())
            .collect();
        assert!(target_specs.contains(&"media:b".to_string()), "B should be reachable");
        assert!(target_specs.contains(&"media:d".to_string()), "D should be reachable");
    }

    // TEST777: Tests type checking prevents using PDF-specific cap with PNG input
    // Verifies that media type compatibility is enforced during pathfinding
    #[test]
    fn test777_type_mismatch_pdf_cap_does_not_match_png_input() {
        let mut graph = LiveCapGraph::new();

        // Only add PDF->textable cap
        let pdf_to_text = make_test_cap("media:pdf", "media:textable", "pdf2text", "PDF to Text");
        graph.add_cap(&pdf_to_text);

        // Try to find path from PNG (not PDF)
        let source = MediaUrn::from_string("media:png").unwrap();
        let target = MediaUrn::from_string("media:textable").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert!(paths.is_empty(), "Should NOT find path from PNG to text via PDF cap");
    }

    // TEST778: Tests type checking prevents using PNG-specific cap with PDF input
    // Verifies that media type compatibility is enforced during pathfinding
    #[test]
    fn test778_type_mismatch_png_cap_does_not_match_pdf_input() {
        let mut graph = LiveCapGraph::new();

        // Only add PNG->thumbnail cap
        let png_to_thumb = make_test_cap("media:png", "media:thumbnail", "png2thumb", "PNG to Thumbnail");
        graph.add_cap(&png_to_thumb);

        // Try to find path from PDF (not PNG)
        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:thumbnail").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert!(paths.is_empty(), "Should NOT find path from PDF to thumbnail via PNG cap");
    }

    // TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
    // Verifies that PNG and PDF inputs reach different targets based on cap input type requirements
    #[test]
    fn test779_get_reachable_targets_respects_type_matching() {
        let mut graph = LiveCapGraph::new();

        let pdf_to_text = make_test_cap("media:pdf", "media:textable", "pdf2text", "PDF to Text");
        let png_to_thumb = make_test_cap("media:png", "media:thumbnail", "png2thumb", "PNG to Thumbnail");

        graph.add_cap(&pdf_to_text);
        graph.add_cap(&png_to_thumb);

        // PNG should only reach thumbnail
        let png_source = MediaUrn::from_string("media:png").unwrap();
        let png_targets = graph.get_reachable_targets(&png_source, 5);
        assert_eq!(png_targets.len(), 1, "PNG should only reach 1 target");
        assert_eq!(png_targets[0].media_spec.to_string(), "media:thumbnail", "PNG should reach thumbnail");

        // PDF should only reach textable
        let pdf_source = MediaUrn::from_string("media:pdf").unwrap();
        let pdf_targets = graph.get_reachable_targets(&pdf_source, 5);
        assert_eq!(pdf_targets.len(), 1, "PDF should only reach 1 target");
        assert_eq!(pdf_targets[0].media_spec.to_string(), "media:textable", "PDF should reach text");
    }

    // TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains
    // Verifies that paths are only found when all intermediate types are compatible
    #[test]
    fn test781_find_paths_respects_type_chain() {
        let mut graph = LiveCapGraph::new();

        let resize_png = make_test_cap("media:png", "media:resized-png", "resize", "Resize PNG");
        let to_thumb = make_test_cap("media:resized-png", "media:thumbnail", "thumb", "To Thumbnail");

        graph.add_cap(&resize_png);
        graph.add_cap(&to_thumb);

        // PNG should find path through resized-png to thumbnail
        let png_source = MediaUrn::from_string("media:png").unwrap();
        let thumb_target = MediaUrn::from_string("media:thumbnail").unwrap();
        let png_paths = graph.find_paths_to_exact_target(&png_source, &thumb_target, 5, 10);
        assert_eq!(png_paths.len(), 1, "Should find 1 path from PNG to thumbnail");
        assert_eq!(png_paths[0].steps.len(), 2, "Path should have 2 steps");

        // PDF should NOT find path to thumbnail (no PDF->resized-png cap)
        let pdf_source = MediaUrn::from_string("media:pdf").unwrap();
        let pdf_paths = graph.find_paths_to_exact_target(&pdf_source, &thumb_target, 5, 10);
        assert!(pdf_paths.is_empty(), "Should find NO paths from PDF to thumbnail (type mismatch)");
    }

    // TEST788: Tests that ForEach edges are inserted for list→singular transitions
    // This is crucial for paths like: pdf → disbind → page;list → ForEach → page → analyze
    #[test]
    fn test788_foreach_edges_inserted_for_list_to_singular() {
        let mut graph = LiveCapGraph::new();

        // Cap 1: pdf → page;list (like disbind)
        let disbind = make_test_cap(
            "media:pdf",
            "media:page;textable;list",
            "disbind",
            "Disbind PDF"
        );

        // Cap 2: textable → decision (like make_multiple_decisions, accepts singular textable)
        let choose = make_test_cap(
            "media:textable",
            "media:decision;bool;textable",
            "choose",
            "Make a Decision"
        );

        // Sync caps - this should insert ForEach edge
        graph.sync_from_caps(&[disbind, choose]);

        // Verify ForEach edge was inserted
        let foreach_edges: Vec<_> = graph.edges.iter()
            .filter(|e| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach))
            .collect();

        assert!(!foreach_edges.is_empty(), "Should have inserted ForEach edge(s)");

        // The ForEach edge should go from page;textable;list → page;textable
        let has_page_foreach = foreach_edges.iter().any(|e| {
            e.from_spec.is_list() &&
            e.from_spec.to_string().contains("page") &&
            !e.to_spec.is_list() &&
            e.to_spec.to_string().contains("page")
        });
        assert!(has_page_foreach, "Should have ForEach edge for page;list → page");

        // Now verify the full path can be found
        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:decision;bool;textable").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 10, 20);

        // Should find at least one path that goes through ForEach
        let path_with_foreach = paths.iter().any(|p| {
            p.steps.iter().any(|s| matches!(s.step_type, StrandStepType::ForEach { .. }))
        });

        assert!(
            path_with_foreach,
            "Should find path from pdf to decision (via disbind → ForEach → choose). Found {} paths, none with ForEach",
            paths.len()
        );
    }

    // TEST791: Tests sync_from_cap_urns actually adds edges
    #[tokio::test]
    async fn test791_sync_from_cap_urns_adds_edges() {
        use std::sync::Arc;
        use crate::CapRegistry;

        // Create a registry with test caps
        let registry = CapRegistry::new_for_test();
        let disbind = make_test_cap(
            "media:pdf",
            "media:page;textable;list",
            "disbind",
            "Disbind PDF"
        );
        let choose = make_test_cap(
            "media:textable",
            "media:decision;bool;textable",
            "choose",
            "Make a Decision"
        );
        registry.add_caps_to_cache(vec![disbind.clone(), choose.clone()]);

        // Create cap URN strings as plugins would report them
        let cap_urns: Vec<String> = vec![
            disbind.urn.to_string(),
            choose.urn.to_string(),
        ];

        eprintln!("Cap URNs to sync: {:?}", cap_urns);

        // Sync from URNs
        let mut graph = LiveCapGraph::new();
        graph.sync_from_cap_urns(&cap_urns, &Arc::new(registry)).await;

        eprintln!("Graph edges: {}", graph.edges.len());
        eprintln!("Graph nodes: {}", graph.nodes.len());

        // Should have edges from the caps
        assert!(
            graph.edges.len() >= 2,
            "Should have at least 2 edges (2 caps), got {}",
            graph.edges.len()
        );
    }

    // TEST790: Tests identity_urn is specific and doesn't match everything
    #[test]
    fn test790_identity_urn_is_specific() {
        let identity = crate::standard::caps::identity_urn();
        eprintln!("Identity URN: {}", identity);
        eprintln!("Identity in_spec: '{}'", identity.in_spec());
        eprintln!("Identity out_spec: '{}'", identity.out_spec());

        // The identity URN should have wildcard in/out specs (media:)
        assert_eq!(identity.in_spec(), "media:");
        assert_eq!(identity.out_spec(), "media:");

        // A specific cap should NOT be equivalent to identity
        let specific_cap = crate::CapUrn::from_string(
            r#"cap:in=media:pdf;op=disbind;out="media:disbound-page;list;textable""#
        ).unwrap();

        eprintln!("Specific cap: {}", specific_cap);
        eprintln!("specific.is_equivalent(&identity): {}", specific_cap.is_equivalent(&identity));
        eprintln!("identity.accepts(&specific): {}", identity.accepts(&specific_cap));
        eprintln!("specific.accepts(&identity): {}", specific_cap.accepts(&identity));

        assert!(
            !specific_cap.is_equivalent(&identity),
            "A specific disbind cap should NOT be equivalent to identity"
        );
    }

    // TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec
    #[test]
    fn test789_cap_from_json_has_valid_specs() {
        let json = r#"{
            "urn": "cap:in=media:pdf;op=disbind;out=\"media:disbound-page;textable;list\"",
            "command": "disbind",
            "title": "Disbind PDF",
            "args": [],
            "output": null
        }"#;

        let cap: crate::Cap = serde_json::from_str(json).expect("Failed to parse cap JSON");

        let in_spec = cap.urn.in_spec();
        let out_spec = cap.urn.out_spec();

        eprintln!("Cap URN: {}", cap.urn);
        eprintln!("in_spec: '{}'", in_spec);
        eprintln!("out_spec: '{}'", out_spec);

        assert!(!in_spec.is_empty(), "in_spec should not be empty");
        assert!(!out_spec.is_empty(), "out_spec should not be empty");
        assert_eq!(in_spec, "media:pdf");
        assert!(out_spec.contains("disbound-page"), "out_spec should contain disbound-page: {}", out_spec);
    }

    // TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones
    // Verifies that among multiple paths, the shortest is ranked first
    #[test]
    fn test787_find_paths_sorting_prefers_shorter() {
        let mut graph = LiveCapGraph::new();

        // Direct path: format-a -> format-c
        let direct = make_test_cap("media:format-a", "media:format-c", "direct", "Direct");
        // Indirect path: format-a -> format-b -> format-c
        let step1 = make_test_cap("media:format-a", "media:format-b", "step1", "Step 1");
        let step2 = make_test_cap("media:format-b", "media:format-c", "step2", "Step 2");

        graph.add_cap(&direct);
        graph.add_cap(&step1);
        graph.add_cap(&step2);

        let source = MediaUrn::from_string("media:format-a").unwrap();
        let target = MediaUrn::from_string("media:format-c").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert!(paths.len() >= 2, "Should find at least 2 paths (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 1, "Shortest path should be first (1 step)");
        assert_eq!(paths[0].steps[0].title(), "Direct");
    }

    #[test]
    fn test790_strand_round_trips_through_serde_without_losing_step_types() {
        let strand = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: CapUrn::from_string(
                            r#"cap:in=media:pdf;op=disbind;out="media:list;page;textable""#,
                        )
                        .unwrap(),
                        title: "Disbind PDF Into Pages".to_string(),
                        specificity: 4,
                    },
                    from_spec: MediaUrn::from_string("media:pdf").unwrap(),
                    to_spec: MediaUrn::from_string("media:list;page;textable").unwrap(),
                },
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        list_spec: MediaUrn::from_string("media:list;page;textable").unwrap(),
                        item_spec: MediaUrn::from_string("media:page;textable").unwrap(),
                    },
                    from_spec: MediaUrn::from_string("media:list;page;textable").unwrap(),
                    to_spec: MediaUrn::from_string("media:page;textable").unwrap(),
                },
            ],
            source_spec: MediaUrn::from_string("media:pdf").unwrap(),
            target_spec: MediaUrn::from_string("media:page;textable").unwrap(),
            total_steps: 2,
            cap_step_count: 1,
            description: "Transform PDF into text pages".to_string(),
        };

        let json = serde_json::to_string(&strand).expect("strand should serialize");
        let recovered: Strand = serde_json::from_str(&json).expect("strand should deserialize");

        assert_eq!(recovered.source_spec.to_string(), "media:pdf");
        assert_eq!(recovered.target_spec.to_string(), "media:page;textable");
        assert_eq!(recovered.steps.len(), 2);
        assert!(matches!(recovered.steps[0].step_type, StrandStepType::Cap { .. }));
        assert!(matches!(recovered.steps[1].step_type, StrandStepType::ForEach { .. }));
    }
}
