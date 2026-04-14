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
//!
//! 5. **Cardinality is not topology**: The `list` tag is a cardinality marker, not a
//!    type identity tag. ForEach (list→item) and Collect (item→list) are universal
//!    operations that apply to any media URN based solely on whether it has the `list`
//!    tag. They are synthesized dynamically during traversal, not stored as graph edges.
//!    Collect is the single scalar→list transition — whether wrapping 1 item or
//!    gathering N ForEach results, it is the same concept.

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
/// Cap edges are stored in the graph. Cardinality transitions (ForEach, Collect)
/// are synthesized dynamically by `get_outgoing_edges()` — they are universal
/// operations derived from the `list` tag, not graph contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveMachinePlanEdgeType {
    /// A real capability that transforms media
    Cap {
        cap_urn: CapUrn,
        cap_title: String,
        specificity: usize,
        /// Whether the cap's main input expects a sequence of items
        input_is_sequence: bool,
        /// Whether the cap's output produces a sequence of items
        output_is_sequence: bool,
    },
    /// Fan-out: iterate over list items (list → item, remove `list` tag)
    /// Synthesized for any list-typed source.
    ForEach,
    /// Collect: scalar → list (item → list, add `list` tag)
    /// The universal scalar-to-list transition. Synthesized for any scalar source.
    /// Works in two contexts: standalone (wrap scalar in list-of-one) or after
    /// ForEach (gather iteration results).
    Collect,
}

/// Event emitted during streaming path finding.
#[derive(Debug, Clone)]
pub enum PathFindingEvent {
    /// A depth level of IDDFS has completed
    DepthComplete {
        depth: usize,
        max_depth: usize,
        nodes_explored: u64,
        paths_found: usize,
    },
    /// A new path was discovered
    PathFound(Strand),
    /// Search is complete
    Complete {
        total_paths: usize,
        total_nodes_explored: u64,
    },
}

/// An edge in the live capability graph.
///
/// Stored edges represent capabilities that transform one media type to another.
/// Cardinality transitions (ForEach/Collect) are synthesized dynamically
/// and use the same struct for uniformity in path traversal.
///
/// URNs are stored as typed values, not strings, for order-theoretic operations.
///
/// Cardinality (single vs sequence) is NOT stored on edges. It is a property
/// of the data flow tracked by `is_sequence` on the wire protocol, determined
/// by context (how many input files), not by URN tags.
#[derive(Debug, Clone)]
pub struct LiveMachinePlanEdge {
    /// Input media type (what this edge consumes)
    pub from_spec: MediaUrn,
    /// Output media type (what this edge produces)
    pub to_spec: MediaUrn,
    /// Type of edge (cap or cardinality transition)
    pub edge_type: LiveMachinePlanEdgeType,
}

/// Precomputed graph of capabilities for path finding.
///
/// The graph stores only Cap edges. Cardinality transitions (ForEach, Collect)
/// are universal shape transitions synthesized dynamically by
/// `get_outgoing_edges()` during traversal based on the `is_sequence` state.
///
/// This graph is designed to be:
/// - Updated incrementally when caps change
/// - Queried efficiently for reachability and path finding
/// - Deterministic in its results
///
/// The graph's indexes are keyed on `MediaUrn` / `CapUrn`
/// directly via their derived `Hash`/`Eq` impls (which route
/// to `TaggedUrn`'s structural `(prefix, tags-BTreeMap)`
/// identity). No index key is ever a flat URN string.
#[derive(Debug)]
pub struct LiveCapGraph {
    /// Cap edges only — cardinality transitions are synthesized during traversal
    edges: Vec<LiveMachinePlanEdge>,
    /// Index: from_spec → edge indices.
    outgoing: HashMap<MediaUrn, Vec<usize>>,
    /// Index: to_spec → edge indices.
    incoming: HashMap<MediaUrn, Vec<usize>>,
    /// All unique media URN nodes reachable in the graph.
    nodes: HashSet<MediaUrn>,
    /// Cap URN → edge indices for removal.
    cap_to_edges: HashMap<CapUrn, Vec<usize>>,
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
            LiveMachinePlanEdgeType::Collect => "Collect (scalar to list)".to_string(),
        }
    }

    /// Get the specificity of this edge (for ordering purposes)
    pub fn specificity(&self) -> usize {
        match &self.edge_type {
            LiveMachinePlanEdgeType::Cap { specificity, .. } => *specificity,
            // Cardinality transitions have no specificity preference
            LiveMachinePlanEdgeType::ForEach | LiveMachinePlanEdgeType::Collect => 0,
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
        /// Whether the cap's main input expects a sequence
        input_is_sequence: bool,
        /// Whether the cap's output produces a sequence
        output_is_sequence: bool,
    },
    /// Fan-out: iterate over sequence items (is_sequence flips true → false).
    /// The media URN does not change — ForEach is a shape transition, not a type transition.
    ForEach {
        /// The media type being iterated over
        media_spec: MediaUrn,
    },
    /// Collect: gather items into a sequence (is_sequence flips false → true).
    /// The media URN does not change — Collect is a shape transition, not a type transition.
    Collect {
        /// The media type being collected
        media_spec: MediaUrn,
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
    /// Number of cap steps only (excluding ForEach/Collect)
    /// This is used for sorting - cardinality transitions don't count as "steps" for user display
    pub cap_step_count: i32,
    /// Human-readable description
    pub description: String,
}

impl Strand {
    /// Convert this resolved strand into a single-strand
    /// `Machine`. Each `Cap` step becomes one resolved
    /// `MachineEdge`; `ForEach` sets `is_loop` on the next cap;
    /// `Collect` is elided.
    ///
    /// Resolution requires the cap registry to look up each
    /// cap's argument list (used by the Hungarian source-to-
    /// arg matching algorithm).
    ///
    /// Fails if the strand contains no capability steps, if
    /// any cap is not in the registry, if a source cannot be
    /// matched to a cap arg, if the matching is ambiguous, or
    /// if the resolved data-flow graph contains a cycle.
    pub fn knit(
        &self,
        registry: &crate::cap::registry::CapRegistry,
    ) -> Result<crate::machine::Machine, crate::machine::MachineAbstractionError> {
        crate::machine::Machine::from_strand(self, registry)
    }

    /// Serialize this resolved strand to canonical one-line
    /// machine notation. This is the primary identifier used
    /// for accessibility and persistence.
    ///
    /// Same failure modes as `knit`, since this method first
    /// builds the `Machine` and then serializes it.
    pub fn to_machine_notation(
        &self,
        registry: &crate::cap::registry::CapRegistry,
    ) -> Result<String, crate::machine::MachineAbstractionError> {
        self.knit(registry)?.to_machine_notation()
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
    /// Only Cap edges are stored in the graph. Cardinality transitions
    /// (ForEach/Collect) are synthesized dynamically by
    /// `get_outgoing_edges()` based on source cardinality.
    pub fn sync_from_caps(&mut self, caps: &[Cap]) {
        self.clear();

        for cap in caps {
            self.add_cap(cap);
        }

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
    /// available cap URN strings (from cartridges), it looks up the Cap definitions
    /// from the registry and builds the graph.
    ///
    /// Caps are matched by equivalence (`is_equivalent`): the cartridge's reported URN
    /// must have an exact semantic match in the registry. Unmatched caps are rejected
    /// with an error and excluded from the graph — a cartridge advertising an unregistered
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
                        "[LiveCapGraph] Cartridge reported invalid cap URN - this is a bug in the cartridge"
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
            // The cartridge reports the specific cap URN it implements — we need to find
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
                        "[LiveCapGraph] REJECTED: cartridge reported cap URN has no equivalent \
                        in the registry. Every cap a cartridge provides must have a matching \
                        registry definition. Either the cartridge is advertising an unknown \
                        capability or the registry is missing a cap definition for this URN. \
                        This cap will NOT be added to the graph."
                    );
                }
            }
        }

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

        // Create edge
        let edge_idx = self.edges.len();
        // Main input arg: the one with a stdin source
        let input_is_sequence = cap.args.iter()
            .find(|arg| arg.sources.iter().any(|s| matches!(s, crate::cap::definition::ArgSource::Stdin { .. })))
            .map_or(false, |arg| arg.is_sequence);
        let output_is_sequence = cap.output.as_ref().map_or(false, |o| o.is_sequence);

        // Update indices with URN clones — MediaUrn and CapUrn
        // are the HashMap keys directly via their derived
        // `Hash`/`Eq` impls; no string intermediaries.
        self.outgoing.entry(from_spec.clone()).or_default().push(edge_idx);
        self.incoming.entry(to_spec.clone()).or_default().push(edge_idx);
        self.nodes.insert(from_spec.clone());
        self.nodes.insert(to_spec.clone());
        self.cap_to_edges.entry(cap.urn.clone()).or_default().push(edge_idx);

        let edge = LiveMachinePlanEdge {
            from_spec,
            to_spec,
            edge_type: LiveMachinePlanEdgeType::Cap {
                cap_urn: cap.urn.clone(),
                cap_title: cap.title.clone(),
                specificity: cap.urn.specificity(),
                input_is_sequence,
                output_is_sequence,
            },
        };
        self.edges.push(edge);
    }

    /// Get all edges reachable from a source media URN.
    ///
    /// Returns Cap edges where the source conforms to the edge's input requirement
    /// (with matching cardinality), plus synthesized cardinality transitions.
    ///
    /// Get outgoing edges from a source media URN at a given `is_sequence` state.
    ///
    /// Cap edges are matched purely on `conforms_to` — cardinality is irrelevant
    /// to type matching. Cardinality transitions (ForEach/Collect) are synthesized
    /// based on the current `is_sequence` state:
    ///
    /// - **ForEach** (is_sequence=true → false): iterate over sequence items.
    ///   The media URN does not change — ForEach is a shape transition, not a type transition.
    /// - **Collect** (is_sequence=false → true): gather items into a sequence.
    ///   The media URN does not change — Collect is a shape transition, not a type transition.
    fn get_outgoing_edges(&self, source: &MediaUrn, is_sequence: bool) -> Vec<(LiveMachinePlanEdge, bool)> {
        let mut result: Vec<(LiveMachinePlanEdge, bool)> = self.edges
            .iter()
            .filter(|edge| {
                debug_assert!(
                    edge.is_cap(),
                    "Non-cap edge found in graph storage: {:?}",
                    edge.edge_type
                );
                if !source.conforms_to(&edge.from_spec).unwrap_or(false) {
                    return false;
                }
                // Check cardinality compatibility:
                // - sequence data can only go to caps that expect sequences
                // - scalar data can go to scalar or sequence caps (single item wraps into 1-item sequence)
                match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { input_is_sequence, .. } => {
                        if is_sequence && !input_is_sequence {
                            // Sequence data → scalar cap: needs ForEach first, skip direct match
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            })
            .map(|edge| {
                // Determine outgoing is_sequence from the cap's output flag
                let out_is_seq = match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { output_is_sequence, .. } => {
                        *output_is_sequence
                    }
                    _ => is_sequence,
                };
                (edge.clone(), out_is_seq)
            })
            .collect();

        // Synthesize ForEach when data is a sequence
        if is_sequence {
            // ForEach: sequence → scalar (same media URN, is_sequence flips to false)
            // Check if any scalar cap could consume items after ForEach
            let has_scalar_consumers = self.edges.iter().any(|edge| {
                if let LiveMachinePlanEdgeType::Cap { input_is_sequence, .. } = &edge.edge_type {
                    !input_is_sequence && source.conforms_to(&edge.from_spec).unwrap_or(false)
                } else {
                    false
                }
            });
            if has_scalar_consumers {
                result.push((LiveMachinePlanEdge {
                    from_spec: source.clone(),
                    to_spec: source.clone(),
                    edge_type: LiveMachinePlanEdgeType::ForEach,
                }, false));
            }
        }
        // Collect is NOT synthesized during path finding. It pairs with ForEach
        // implicitly at execution time — the plan builder handles it.
        // Synthesizing Collect here creates ForEach↔Collect cycles that cause
        // infinite loops in the DFS.

        result
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> (usize, usize) {
        (self.nodes.len(), self.edges.len())
    }


    // =========================================================================
    // REACHABLE TARGETS (BFS)
    // =========================================================================

    /// Find all reachable targets from a source media URN.
    ///
    /// Uses BFS to explore the graph up to max_depth steps.
    /// `is_sequence` is the initial cardinality state of the input (from context).
    /// Returns targets sorted by (min_path_length, display_name).
    pub fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        // `results` and `visited` are keyed on `MediaUrn`
        // directly — their derived `Hash`/`Eq` go through
        // `TaggedUrn`'s structural tag-set identity.
        let mut results: HashMap<MediaUrn, ReachableTargetInfo> = HashMap::new();
        let mut visited: HashSet<(MediaUrn, bool)> = HashSet::new();
        let mut queue: VecDeque<(MediaUrn, bool, usize)> = VecDeque::new();

        queue.push_back((source.clone(), is_sequence, 0));
        visited.insert((source.clone(), is_sequence));

        while let Some((current, current_is_seq, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (edge, next_is_seq) in self.get_outgoing_edges(&current, current_is_seq) {
                let new_depth = depth + 1;

                // Record this target — the `MediaUrn` entry
                // key collapses tag-set-equal URNs
                // automatically via the structural `Hash`/`Eq`.
                let entry = results
                    .entry(edge.to_spec.clone())
                    .or_insert_with(|| ReachableTargetInfo {
                        media_spec: edge.to_spec.clone(),
                        // display_name is a fallback the caller
                        // may override via the registry; here
                        // we just serialize the URN for
                        // presentation. This is **not** used
                        // for identity, only for display.
                        display_name: edge.to_spec.to_string(),
                        min_path_length: new_depth as i32,
                        path_count: 0,
                    });
                entry.path_count += 1;

                // Continue BFS if not visited at this is_sequence state
                let visit_key = (edge.to_spec.clone(), next_is_seq);
                if !visited.contains(&visit_key) {
                    visited.insert(visit_key);
                    queue.push_back((edge.to_spec.clone(), next_is_seq, new_depth));
                }
            }
        }

        // Sort by (min_path_length, display_name).
        //
        // `display_name` is a presentation string (not an
        // identity key), so lex-comparing it as a String is
        // the correct semantics — this is user-visible
        // alphabetical sort, not URN equivalence.
        let mut targets: Vec<_> = results.into_values().collect();
        targets.sort_by(|a, b| {
            a.min_path_length
                .cmp(&b.min_path_length)
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
    /// `is_sequence` is the initial cardinality state (from input context).
    ///
    /// Returns paths sorted by (total_steps, total_specificity desc, cap_urns).
    pub fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<Strand> {
        // Check if source already satisfies target
        if source.is_equivalent(target).unwrap_or(false) {
            return vec![];
        }

        // Log outgoing edges from source to understand branching
        let source_edges = self.get_outgoing_edges(source, is_sequence);
        tracing::info!(
            "find_paths_to_exact_target: source={} target={} is_sequence={} max_depth={} max_paths={} source_outgoing={}",
            source, target, is_sequence, max_depth, max_paths, source_edges.len()
        );
        for (edge, _) in &source_edges {
            tracing::info!(
                "  outgoing: {} -> {} ({})",
                edge.from_spec, edge.to_spec,
                match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { cap_title, .. } => cap_title.as_str(),
                    LiveMachinePlanEdgeType::ForEach => "ForEach",
                    LiveMachinePlanEdgeType::Collect => "Collect",
                }
            );
        }

        // Iterative deepening: find ALL paths at depth N before any at depth N+1.
        let mut all_paths: Vec<Strand> = Vec::new();
        let mut total_nodes_explored: u64 = 0;
        let not_cancelled = std::sync::atomic::AtomicBool::new(false);

        for depth_limit in 1..=max_depth {
            if all_paths.len() >= max_paths {
                break;
            }

            let mut current_path: Vec<StrandStep> = Vec::new();
            let mut visited: HashSet<(MediaUrn, bool)> = HashSet::new();
            let paths_before = all_paths.len();
            let mut nodes_this_depth: u64 = 0;

            self.iddfs_find_paths(
                source,
                target,
                source,
                is_sequence,
                &mut current_path,
                &mut visited,
                &mut all_paths,
                depth_limit,
                max_paths,
                &mut nodes_this_depth,
                &not_cancelled,
            );

            total_nodes_explored += nodes_this_depth;
            let new_paths = all_paths.len() - paths_before;
            if new_paths > 0 || nodes_this_depth > 1000 {
                tracing::info!(
                    "  IDDFS depth={}: explored {} nodes, found {} new paths (total {})",
                    depth_limit, nodes_this_depth, new_paths, all_paths.len()
                );
            }

            // Safety: abort if exploring too many nodes (combinatorial explosion)
            if total_nodes_explored > 100_000 {
                tracing::warn!(
                    "find_paths_to_exact_target: aborting after {} nodes explored. \
                     Returning {} paths found so far.",
                    total_nodes_explored, all_paths.len()
                );
                break;
            }
        }

        tracing::info!(
            "find_paths_to_exact_target: found {} paths, explored {} total nodes (max_paths was {})",
            all_paths.len(), total_nodes_explored, max_paths
        );

        // Sort paths deterministically
        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        all_paths
    }

    /// Find paths with streaming progress reporting.
    ///
    /// Calls `on_event` for each progress update and each path found.
    /// Returns the final sorted list of paths.
    pub fn find_paths_streaming<F>(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        is_sequence: bool,
        max_depth: usize,
        max_paths: usize,
        cancelled: &std::sync::atomic::AtomicBool,
        mut on_event: F,
    ) -> Vec<Strand>
    where
        F: FnMut(PathFindingEvent),
    {
        if source.is_equivalent(target).unwrap_or(false) {
            on_event(PathFindingEvent::Complete {
                total_paths: 0,
                total_nodes_explored: 0,
            });
            return vec![];
        }

        let mut all_paths: Vec<Strand> = Vec::new();
        let mut total_nodes_explored: u64 = 0;

        for depth_limit in 1..=max_depth {
            if all_paths.len() >= max_paths {
                break;
            }
            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let mut current_path: Vec<StrandStep> = Vec::new();
            let mut visited: HashSet<(MediaUrn, bool)> = HashSet::new();
            let paths_before = all_paths.len();
            let mut nodes_this_depth: u64 = 0;

            self.iddfs_find_paths(
                source, target, source, is_sequence,
                &mut current_path, &mut visited, &mut all_paths,
                depth_limit, max_paths, &mut nodes_this_depth,
                cancelled,
            );

            total_nodes_explored += nodes_this_depth;

            // Report progress after each depth
            on_event(PathFindingEvent::DepthComplete {
                depth: depth_limit,
                max_depth,
                nodes_explored: total_nodes_explored,
                paths_found: all_paths.len(),
            });

            // Report each new path found at this depth
            for path in &all_paths[paths_before..] {
                on_event(PathFindingEvent::PathFound(path.clone()));
            }

            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            if total_nodes_explored > 100_000 {
                break;
            }
        }

        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        on_event(PathFindingEvent::Complete {
            total_paths: all_paths.len(),
            total_nodes_explored,
        });

        all_paths
    }

    /// Depth-limited DFS helper for iterative deepening path finding.
    ///
    /// `is_sequence` tracks the current cardinality state through the path.
    /// Only records paths whose length equals `depth_limit` exactly.
    fn iddfs_find_paths(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        current: &MediaUrn,
        is_sequence: bool,
        current_path: &mut Vec<StrandStep>,
        visited: &mut HashSet<(MediaUrn, bool)>,
        all_paths: &mut Vec<Strand>,
        depth_limit: usize,
        max_paths: usize,
        nodes_explored: &mut u64,
        cancelled: &std::sync::atomic::AtomicBool,
    ) {
        *nodes_explored += 1;
        if all_paths.len() >= max_paths {
            return;
        }
        if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Safety: bail out if exploring too many nodes
        if *nodes_explored > 100_000 {
            return;
        }

        // Check if we've reached the EXACT target using is_equivalent()
        if current.is_equivalent(target).unwrap_or(false) {
            if current_path.len() == depth_limit {
                let cap_step_count = current_path.iter().filter(|s| s.is_cap()).count() as i32;

                // A valid machine requires at least one capability step.
                if cap_step_count == 0 {
                    return;
                }

                let description = current_path
                    .iter()
                    .map(|s| s.title())
                    .collect::<Vec<_>>()
                    .join(" → ");

                all_paths.push(Strand {
                    steps: current_path.clone(),
                    source_spec: source.clone(),
                    target_spec: target.clone(),
                    total_steps: current_path.len() as i32,
                    cap_step_count,
                    description,
                });
            }
            return;
        }

        if current_path.len() >= depth_limit {
            return;
        }

        let visit_key = (current.clone(), is_sequence);
        visited.insert(visit_key.clone());

        for (edge, next_is_seq) in self.get_outgoing_edges(current, is_sequence) {
            let next_visit_key = (edge.to_spec.clone(), next_is_seq);

            if !visited.contains(&next_visit_key) {
                let step_type = match &edge.edge_type {
                    LiveMachinePlanEdgeType::Cap { cap_urn, cap_title, specificity, input_is_sequence, output_is_sequence } => {
                        StrandStepType::Cap {
                            cap_urn: cap_urn.clone(),
                            title: cap_title.clone(),
                            specificity: *specificity,
                            input_is_sequence: *input_is_sequence,
                            output_is_sequence: *output_is_sequence,
                        }
                    }
                    LiveMachinePlanEdgeType::ForEach => {
                        StrandStepType::ForEach {
                            media_spec: edge.from_spec.clone(),
                        }
                    }
                    LiveMachinePlanEdgeType::Collect => {
                        StrandStepType::Collect {
                            media_spec: edge.from_spec.clone(),
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
                    next_is_seq,
                    current_path,
                    visited,
                    all_paths,
                    depth_limit,
                    max_paths,
                    nodes_explored,
                    cancelled,
                );

                current_path.pop();
            }
        }

        visited.remove(&visit_key);
    }

    /// Compare two paths for deterministic ordering.
    ///
    /// Sort by:
    /// 1. `cap_step_count` (ascending — fewer actual cap
    ///    steps first; ForEach/Collect don't count)
    /// 2. total specificity (descending — more specific first)
    /// 3. structural step-sequence ordering (for tie-breaking
    ///    stability)
    ///
    /// The step-sequence comparison routes cap steps through
    /// the `CapUrn` structural `Ord` impl, cardinality steps
    /// through a fixed discriminator (Cap < ForEach < Collect),
    /// and falls through to the step's `from_spec` / `to_spec`
    /// via `MediaUrn`'s structural `Ord`. No URN is ever
    /// compared as a flat string.
    fn compare_paths(a: &Strand, b: &Strand) -> Ordering {
        a.cap_step_count.cmp(&b.cap_step_count)
            .then_with(|| {
                // Higher specificity first.
                let spec_a: usize = a.steps.iter().map(|s| s.specificity()).sum();
                let spec_b: usize = b.steps.iter().map(|s| s.specificity()).sum();
                spec_b.cmp(&spec_a)
            })
            .then_with(|| Self::compare_step_sequences(&a.steps, &b.steps))
    }

    /// Lexicographic comparison over step sequences using the
    /// structural step ordering. Stable and deterministic
    /// because every component routes through `MediaUrn` /
    /// `CapUrn` structural `Ord` — never flat-string
    /// comparison.
    fn compare_step_sequences(a: &[StrandStep], b: &[StrandStep]) -> Ordering {
        for (step_a, step_b) in a.iter().zip(b.iter()) {
            match Self::compare_steps(step_a, step_b) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        a.len().cmp(&b.len())
    }

    /// Structural comparison of two strand steps. Routes
    /// through the structural `Ord` of `CapUrn` / `MediaUrn`;
    /// cardinality step discriminators use fixed integer
    /// ranks (Cap = 0, ForEach = 1, Collect = 2).
    fn compare_steps(a: &StrandStep, b: &StrandStep) -> Ordering {
        const RANK_CAP: u8 = 0;
        const RANK_FOREACH: u8 = 1;
        const RANK_COLLECT: u8 = 2;

        let rank = |s: &StrandStep| -> u8 {
            match &s.step_type {
                StrandStepType::Cap { .. } => RANK_CAP,
                StrandStepType::ForEach { .. } => RANK_FOREACH,
                StrandStepType::Collect { .. } => RANK_COLLECT,
            }
        };

        match rank(a).cmp(&rank(b)) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Same rank — compare structural details.
        match (&a.step_type, &b.step_type) {
            (
                StrandStepType::Cap { cap_urn: ca, .. },
                StrandStepType::Cap { cap_urn: cb, .. },
            ) => match ca.cmp(cb) {
                Ordering::Equal => {}
                ord => return ord,
            },
            (
                StrandStepType::ForEach { media_spec: ma },
                StrandStepType::ForEach { media_spec: mb },
            )
            | (
                StrandStepType::Collect { media_spec: ma },
                StrandStepType::Collect { media_spec: mb },
            ) => match ma.cmp(mb) {
                Ordering::Equal => {}
                ord => return ord,
            },
            _ => unreachable!("rank comparison already discriminated mismatched step types"),
        }

        // Final tiebreaker: structural from_spec / to_spec.
        match a.from_spec.cmp(&b.from_spec) {
            Ordering::Equal => {}
            ord => return ord,
        }
        a.to_spec.cmp(&b.to_spec)
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
            documentation: None,
            metadata: Default::default(),
            command: "test".to_string(),
            media_specs: vec![],
            output: None,
            args: vec![],
            metadata_json: None,
            registered_by: None,
        }
    }

    /// Build a `Cap` whose `args` list is populated with a
    /// stdin arg matching the `in_spec`. Required for tests
    /// that pass a strand built from this cap into
    /// `Strand::knit` or `Strand::to_machine_notation`, since
    /// the resolver looks up the cap's args list to compute
    /// the source-to-arg matching.
    fn make_test_cap_with_arg(
        in_spec: &str,
        out_spec: &str,
        op: &str,
        title: &str,
    ) -> Cap {
        use crate::cap::definition::{ArgSource, CapArg, CapOutput};
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
            documentation: None,
            metadata: Default::default(),
            command: "test".to_string(),
            media_specs: vec![],
            output: Some(CapOutput::new(out_spec.to_string(), title.to_string())),
            args: vec![CapArg::new(
                in_spec.to_string(),
                true,
                vec![ArgSource::Stdin {
                    stdin: in_spec.to_string(),
                }],
            )],
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
        let targets = graph.get_reachable_targets(&source, false, 5);

        // Reachable targets include only media:extracted-text
        // (via the cap, depth 1). Collect is not synthesized
        // during reachability traversal — cardinality variants
        // are handled by the plan builder at execution time.
        let extracted_text = MediaUrn::from_string("media:extracted-text").unwrap();
        let cap_target = targets
            .iter()
            .find(|t| t.media_spec.is_equivalent(&extracted_text).unwrap_or(false));
        assert!(cap_target.is_some(), "extracted-text should be reachable");
        assert_eq!(cap_target.unwrap().min_path_length, 1);
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
        // Two valid paths exist:
        // 1. Direct: pdf → result (via analyze) — 1 cap step, 1 total step
        // 2. Indirect: pdf → result;list (via analyze_multi) → ForEach → result — 1 cap step, 2 total steps
        // Both are valid. Path 1 ranks first (fewer total steps at same cap count).
        let target_singular = MediaUrn::from_string("media:analysis-result").unwrap();
        let paths_singular = graph.find_paths_to_exact_target(&source, &target_singular, false, 5, 10);

        assert!(paths_singular.len() >= 1, "singular query should find at least 1 path");
        assert_eq!(paths_singular[0].steps[0].title(), "Analyze PDF",
            "First path should be the direct cap (fewer total steps)");

        // Query for EXACT target: result;list (plural)
        // Two valid paths exist:
        // 1. Direct: pdf → result;list (via analyze_multi) — 1 cap step
        // 2. Indirect: pdf → result (via analyze) + Collect → result;list — 1 cap step + Collect
        // Both are valid. The direct path is shorter (fewer total steps).
        let target_plural = MediaUrn::from_string("media:analysis-result;list").unwrap();
        let paths_plural = graph.find_paths_to_exact_target(&source, &target_plural, false, 5, 10);

        assert!(paths_plural.len() >= 1, "list query should find at least 1 path");
        // The shortest path (fewest cap steps, then fewest total steps) should be the direct one
        assert_eq!(paths_plural[0].steps[0].title(), "Analyze PDF Multi",
            "First path should be the direct cap (fewer total steps)");
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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

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
        let paths1 = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);
        let paths2 = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert_eq!(paths1.len(), paths2.len());
        for (p1, p2) in paths1.iter().zip(paths2.iter()) {
            // Determinism: two runs of find_paths_to_exact_target
            // over the same input must produce paths in the
            // same order with the same cap URNs at each step.
            // CapUrn equivalence is checked structurally via
            // `is_equivalent`, not via string comparison.
            let u1 = p1.steps[0].cap_urn().expect("first step is a cap");
            let u2 = p2.steps[0].cap_urn().expect("first step is a cap");
            assert!(
                u1.is_equivalent(u2),
                "determinism: first cap URN differs across runs: {} vs {}",
                u1, u2
            );
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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(paths.is_empty(), "Should find no paths when target is unreachable");
    }

    // TEST774: Tests get_reachable_targets() returns all reachable targets
    // Verifies that reachable targets include direct cap targets and
    // cardinality variants (list versions via Collect)
    #[test]
    fn test774_get_reachable_targets_finds_all_targets() {
        let mut graph = LiveCapGraph::new();

        let cap1 = make_test_cap("media:a", "media:b", "step1", "A to B");
        let cap2 = make_test_cap("media:a", "media:d", "step3", "A to D");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:a").unwrap();
        let targets = graph.get_reachable_targets(&source, false, 5);

        let media_b = MediaUrn::from_string("media:b").unwrap();
        let media_d = MediaUrn::from_string("media:d").unwrap();
        let reaches = |needle: &MediaUrn| -> bool {
            targets
                .iter()
                .any(|t| t.media_spec.is_equivalent(needle).unwrap_or(false))
        };
        assert!(reaches(&media_b), "B should be reachable");
        assert!(reaches(&media_d), "D should be reachable");
        // Collect is not synthesized during reachability
        // traversal — see `get_outgoing_edges`. Cardinality
        // variants (e.g. `media:a;list`) therefore are NOT in
        // the reachability graph. The plan builder pairs
        // Collect with ForEach implicitly at execution time.
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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(paths.is_empty(), "Should NOT find path from PDF to thumbnail via PNG cap");
    }

    // TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
    // Verifies that PNG and PDF inputs reach different cap targets (not each other's)
    #[test]
    fn test779_get_reachable_targets_respects_type_matching() {
        let mut graph = LiveCapGraph::new();

        let pdf_to_text = make_test_cap("media:pdf", "media:textable", "pdf2text", "PDF to Text");
        let png_to_thumb = make_test_cap("media:png", "media:thumbnail", "png2thumb", "PNG to Thumbnail");

        graph.add_cap(&pdf_to_text);
        graph.add_cap(&png_to_thumb);

        // PNG should reach thumbnail (cap target) but NOT textable (PDF-only cap)
        let png_source = MediaUrn::from_string("media:png").unwrap();
        let png_targets = graph.get_reachable_targets(&png_source, false, 5);
        let media_thumbnail = MediaUrn::from_string("media:thumbnail").unwrap();
        let media_textable = MediaUrn::from_string("media:textable").unwrap();
        assert!(
            png_targets.iter().any(|t| t
                .media_spec
                .is_equivalent(&media_thumbnail)
                .unwrap_or(false)),
            "PNG should reach thumbnail"
        );
        assert!(
            !png_targets.iter().any(|t| t
                .media_spec
                .is_equivalent(&media_textable)
                .unwrap_or(false)),
            "PNG should NOT reach textable"
        );

        // PDF should reach textable (cap target) but NOT thumbnail (PNG-only cap)
        let pdf_source = MediaUrn::from_string("media:pdf").unwrap();
        let pdf_targets = graph.get_reachable_targets(&pdf_source, false, 5);
        assert!(
            pdf_targets.iter().any(|t| t
                .media_spec
                .is_equivalent(&media_textable)
                .unwrap_or(false)),
            "PDF should reach textable"
        );
        assert!(
            !pdf_targets.iter().any(|t| t
                .media_spec
                .is_equivalent(&media_thumbnail)
                .unwrap_or(false)),
            "PDF should NOT reach thumbnail"
        );
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
        let png_paths = graph.find_paths_to_exact_target(&png_source, &thumb_target, false, 5, 10);
        assert_eq!(png_paths.len(), 1, "Should find 1 path from PNG to thumbnail");
        assert_eq!(png_paths[0].steps.len(), 2, "Path should have 2 steps");

        // PDF should NOT find path to thumbnail (no PDF->resized-png cap)
        let pdf_source = MediaUrn::from_string("media:pdf").unwrap();
        let pdf_paths = graph.find_paths_to_exact_target(&pdf_source, &thumb_target, false, 5, 10);
        assert!(pdf_paths.is_empty(), "Should find NO paths from PDF to thumbnail (type mismatch)");
    }

    // TEST788: ForEach is only synthesized when is_sequence=true
    // With scalar input (is_sequence=false), disbind output goes directly to choose
    // since media:page;textable conforms to media:textable.
    // With sequence input (is_sequence=true), ForEach splits the sequence so each
    // item can be processed by disbind individually, then choose.
    #[test]
    fn test788_foreach_only_with_sequence_input() {
        let mut graph = LiveCapGraph::new();

        let disbind = make_test_cap(
            "media:pdf",
            "media:page;textable",
            "disbind",
            "Disbind PDF"
        );

        let choose = make_test_cap(
            "media:textable",
            "media:decision;json;record;textable",
            "choose",
            "Make a Decision"
        );

        graph.sync_from_caps(&[disbind, choose]);
        assert_eq!(graph.edges.len(), 2, "Graph should contain exactly 2 Cap edges");

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:decision;json;record;textable").unwrap();

        // Scalar input: no ForEach, direct path disbind → choose
        let scalar_paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        let has_foreach_scalar = scalar_paths.iter().any(|p| {
            p.steps.iter().any(|s| matches!(s.step_type, StrandStepType::ForEach { .. }))
        });
        assert!(!has_foreach_scalar, "Scalar input should NOT produce ForEach");
        assert!(!scalar_paths.is_empty(), "Should find direct path disbind → choose");

        // Sequence input: ForEach should appear
        let seq_paths = graph.find_paths_to_exact_target(&source, &target, true, 10, 20);
        let has_foreach_seq = seq_paths.iter().any(|p| {
            p.steps.iter().any(|s| matches!(s.step_type, StrandStepType::ForEach { .. }))
        });
        assert!(has_foreach_seq, "Sequence input should produce ForEach step");
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
            "media:page;textable",
            "disbind",
            "Disbind PDF"
        );
        let choose = make_test_cap(
            "media:textable",
            "media:decision;json;record;textable",
            "choose",
            "Make a Decision"
        );
        registry.add_caps_to_cache(vec![disbind.clone(), choose.clone()]);

        // Create cap URN strings as cartridges would report them
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

        // Should have exactly 2 Cap edges (no pre-computed cardinality edges)
        assert_eq!(
            graph.edges.len(), 2,
            "Should have exactly 2 Cap edges, got {}",
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
            r#"cap:in=media:pdf;op=disbind;out="media:disbound-page;textable""#
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
            "urn": "cap:in=media:pdf;op=disbind;out=\"media:disbound-page;textable\"",
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

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 5, 10);

        assert!(paths.len() >= 2, "Should find at least 2 paths (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 1, "Shortest path should be first (1 step)");
        assert_eq!(paths[0].steps[0].title(), "Direct");
    }

    #[test]
    fn test1110_strand_round_trips_through_serde_without_losing_step_types() {
        let strand = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: CapUrn::from_string(
                            r#"cap:in=media:pdf;op=disbind;out="media:page;textable""#,
                        )
                        .unwrap(),
                        title: "Disbind PDF Into Pages".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: true,
                    },
                    from_spec: MediaUrn::from_string("media:pdf").unwrap(),
                    to_spec: MediaUrn::from_string("media:page;textable").unwrap(),
                },
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: MediaUrn::from_string("media:page;textable").unwrap(),
                    },
                    from_spec: MediaUrn::from_string("media:page;textable").unwrap(),
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

        let expected_source = MediaUrn::from_string("media:pdf").unwrap();
        let expected_target = MediaUrn::from_string("media:page;textable").unwrap();
        assert!(
            recovered
                .source_spec
                .is_equivalent(&expected_source)
                .expect("URN equivalence check"),
            "source_spec must round-trip structurally as media:pdf"
        );
        assert!(
            recovered
                .target_spec
                .is_equivalent(&expected_target)
                .expect("URN equivalence check"),
            "target_spec must round-trip structurally as media:page;textable"
        );
        assert_eq!(recovered.steps.len(), 2);
        assert!(matches!(recovered.steps[0].step_type, StrandStepType::Cap { .. }));
        assert!(matches!(recovered.steps[1].step_type, StrandStepType::ForEach { .. }));
    }

    // TEST1111: ForEach works for user-provided list sources not in the graph.
    // This is the original bug — media:list;textable;txt is a user import source,
    // not a cap output. Previously, no ForEach edge existed for it because
    // insert_cardinality_transitions() only pre-computed edges for cap outputs.
    // With dynamic synthesis, ForEach is available for ANY list source.
    #[test]
    fn test1111_foreach_for_user_provided_list_source() {
        let mut graph = LiveCapGraph::new();

        // Cap: textable → decision (accepts singular textable)
        let make_decision = make_test_cap(
            "media:textable",
            "media:decision;json;record;textable",
            "make_decision",
            "Make Decision"
        );
        graph.sync_from_caps(&[make_decision]);

        // Source is a user-provided list that no cap outputs
        let source = MediaUrn::from_string("media:list;textable;txt").unwrap();
        let target = MediaUrn::from_string("media:decision;json;record;textable").unwrap();

        // User provides multiple files → is_sequence=true
        let paths = graph.find_paths_to_exact_target(&source, &target, true, 10, 20);

        // Expected path: ForEach → make_decision
        // ForEach iterates over items, make_decision accepts media:textable
        let path = paths.iter().find(|p| {
            p.steps.len() == 2
                && matches!(p.steps[0].step_type, StrandStepType::ForEach { .. })
                && matches!(p.steps[1].step_type, StrandStepType::Cap { .. })
        });

        assert!(
            path.is_some(),
            "Should find path: ForEach → make_decision. \
             User-provided list source media:list;textable;txt must be iterable. \
             Found {} paths: {:?}",
            paths.len(),
            paths.iter().map(|p| &p.description).collect::<Vec<_>>()
        );

        let path = path.unwrap();
        // Verify the ForEach step correctly derives item type from list source
        if let StrandStepType::ForEach { media_spec } = &path.steps[0].step_type {
            // ForEach doesn't change the media URN — same type, different shape (is_sequence)
            assert!(
                media_spec.is_equivalent(&source).unwrap(),
                "ForEach media_spec should be the same as source"
            );
        }
    }

    // TEST1112: Collect is not synthesized during path finding.
    // Reaching a list target type requires the cap itself to output a list type.
    #[test]
    fn test1112_no_collect_in_path_finding() {
        let mut graph = LiveCapGraph::new();

        let summarize = make_test_cap(
            "media:textable",
            "media:summary;textable",
            "summarize",
            "Summarize"
        );
        graph.sync_from_caps(&[summarize]);

        let source = MediaUrn::from_string("media:textable").unwrap();
        // list;summary;textable is a different semantic type — can't reach it
        // without a cap that outputs it or a Collect step (not synthesized)
        let target = MediaUrn::from_string("media:list;summary;textable").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        assert!(paths.is_empty(), "Should NOT find path to list type without a cap that produces it");
    }

    // TEST1113: Multi-cap path without Collect — Collect is not synthesized
    #[test]
    fn test1113_multi_cap_path_no_collect() {
        let mut graph = LiveCapGraph::new();

        let disbind = make_test_cap(
            "media:pdf",
            "media:page;textable",
            "disbind",
            "Disbind PDF"
        );
        let summarize = make_test_cap(
            "media:page;textable",
            "media:summary;textable",
            "summarize",
            "Summarize Page"
        );
        graph.sync_from_caps(&[disbind, summarize]);

        // Scalar path: pdf → disbind → page;textable → summarize → summary;textable
        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:summary;textable").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, false, 10, 20);
        assert!(!paths.is_empty(), "Should find direct cap path");
        assert_eq!(paths[0].cap_step_count, 2, "Should have 2 cap steps");
    }

    // TEST1114: Graph stores only Cap edges after sync
    #[test]
    fn test1114_graph_stores_only_cap_edges() {
        let mut graph = LiveCapGraph::new();

        let caps = vec![
            make_test_cap("media:pdf", "media:page;textable", "disbind", "Disbind"),
            make_test_cap("media:page;textable", "media:summary;textable", "summarize", "Summarize"),
            make_test_cap("media:textable", "media:decision;json;record;textable", "decide", "Decide"),
        ];

        graph.sync_from_caps(&caps);

        // All stored edges must be Cap edges
        assert_eq!(graph.edges.len(), 3, "Should have exactly 3 Cap edges");
        for edge in &graph.edges {
            assert!(
                edge.is_cap(),
                "Stored edge {:?} should be a Cap edge, not a cardinality transition",
                edge.edge_type
            );
        }
    }

    // TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items
    #[test]
    fn test1115_dynamic_foreach_with_is_sequence() {
        let mut graph = LiveCapGraph::new();

        // Need a cap that accepts the source type for ForEach to be synthesized
        let cap = make_test_cap("media:textable", "media:summary;textable", "summarize", "Summarize");
        graph.sync_from_caps(&[cap]);

        let source = MediaUrn::from_string("media:textable").unwrap();
        let edges = graph.get_outgoing_edges(&source, true);

        let foreach_edge = edges.iter().find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(foreach_edge.is_some(), "Should synthesize ForEach when is_sequence=true and caps exist");

        let (fe, next_is_seq) = foreach_edge.unwrap();
        assert!(!next_is_seq, "ForEach should flip is_sequence to false");
        assert!(fe.from_spec.is_equivalent(&source).unwrap(), "ForEach from_spec should be the source");
        assert!(fe.to_spec.is_equivalent(&source).unwrap(), "ForEach to_spec should be the same URN");
    }

    // TEST1116: Collect is never synthesized during path finding
    #[test]
    fn test1116_collect_never_synthesized() {
        let graph = LiveCapGraph::new();

        let source = MediaUrn::from_string("media:page;textable").unwrap();

        // Neither scalar nor sequence should produce Collect
        let edges_scalar = graph.get_outgoing_edges(&source, false);
        let collect_scalar = edges_scalar.iter().find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::Collect));
        assert!(collect_scalar.is_none(), "Should NOT synthesize Collect for scalar");

        let edges_seq = graph.get_outgoing_edges(&source, true);
        let collect_seq = edges_seq.iter().find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::Collect));
        assert!(collect_seq.is_none(), "Should NOT synthesize Collect for sequence");
    }

    // TEST1117: ForEach is NOT synthesized when is_sequence=false
    #[test]
    fn test1117_no_foreach_when_not_sequence() {
        let mut graph = LiveCapGraph::new();

        // Even with caps that could consume, ForEach requires is_sequence=true
        let cap = make_test_cap("media:textable", "media:summary;textable", "summarize", "Summarize");
        graph.sync_from_caps(&[cap]);

        let source = MediaUrn::from_string("media:textable").unwrap();
        let edges = graph.get_outgoing_edges(&source, false);

        let foreach_edge = edges.iter().find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(foreach_edge.is_none(), "Should NOT synthesize ForEach when is_sequence=false");
    }

    // TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true
    #[test]
    fn test1118_no_foreach_without_cap_consumers() {
        let graph = LiveCapGraph::new();

        let source = MediaUrn::from_string("media:textable").unwrap();
        // Empty graph — no caps to consume items
        let edges = graph.get_outgoing_edges(&source, true);

        let foreach_edge = edges.iter().find(|(e, _)| matches!(e.edge_type, LiveMachinePlanEdgeType::ForEach));
        assert!(foreach_edge.is_none(), "Should NOT synthesize ForEach without cap consumers");
    }

    // TEST1119: Strand::knit returns a single-strand Machine via the new
    // resolver. Smoke test the registry-threaded API end-to-end.
    #[test]
    fn test1119_strand_knit_with_registry_returns_single_strand_machine() {
        use crate::cap::registry::CapRegistry;

        let cap = make_test_cap_with_arg(
            "media:pdf",
            "media:txt;textable",
            "extract",
            "Extract",
        );
        let registry = CapRegistry::new_for_test();
        registry.add_caps_to_cache(vec![cap]);

        let cap_urn = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
        )
        .unwrap();
        let strand = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::Cap {
                    cap_urn: cap_urn.clone(),
                    title: "Extract".to_string(),
                    specificity: 0,
                    input_is_sequence: false,
                    output_is_sequence: false,
                },
                from_spec: MediaUrn::from_string("media:pdf").unwrap(),
                to_spec: MediaUrn::from_string("media:txt;textable").unwrap(),
            }],
            source_spec: MediaUrn::from_string("media:pdf").unwrap(),
            target_spec: MediaUrn::from_string("media:txt;textable").unwrap(),
            total_steps: 1,
            cap_step_count: 1,
            description: "pdf to txt".to_string(),
        };

        let machine = strand.knit(&registry).expect("knit must succeed");
        assert_eq!(machine.strand_count(), 1);
        assert_eq!(machine.strands()[0].edges().len(), 1);

        // Same registry → `to_machine_notation` produces the
        // same canonical form as the explicit knit + serialize.
        let direct = strand.to_machine_notation(&registry).expect("must serialize");
        let via_machine = machine.to_machine_notation().unwrap();
        assert_eq!(direct, via_machine);
    }

    // TEST1120: Strand::knit fails hard when the cap is not in
    // the registry — the planner produces strands referencing
    // caps that must be present in the cap registry's cache for
    // resolution to succeed.
    #[test]
    fn test1120_strand_knit_unknown_cap_fails_hard() {
        use crate::cap::registry::CapRegistry;
        use crate::machine::MachineAbstractionError;

        let registry = CapRegistry::new_for_test();
        // Note: no caps added to the registry.

        let cap_urn = CapUrn::from_string(
            "cap:in=media:pdf;op=ghost;out=\"media:txt;textable\"",
        )
        .unwrap();
        let strand = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::Cap {
                    cap_urn: cap_urn.clone(),
                    title: "Ghost".to_string(),
                    specificity: 0,
                    input_is_sequence: false,
                    output_is_sequence: false,
                },
                from_spec: MediaUrn::from_string("media:pdf").unwrap(),
                to_spec: MediaUrn::from_string("media:txt;textable").unwrap(),
            }],
            source_spec: MediaUrn::from_string("media:pdf").unwrap(),
            target_spec: MediaUrn::from_string("media:txt;textable").unwrap(),
            total_steps: 1,
            cap_step_count: 1,
            description: "ghost strand".to_string(),
        };

        let err = strand.knit(&registry).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::UnknownCap { .. }));
    }
}
