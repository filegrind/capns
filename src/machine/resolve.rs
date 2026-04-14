//! Anchor-realization for `MachineStrand`s.
//!
//! This module turns either:
//!
//! - a planner-produced `Strand` (linear sequence of cap steps,
//!   one source per step), or
//! - a parser-produced wiring set (potentially multi-source
//!   per wiring),
//!
//! into a fully-resolved `MachineStrand`: a connected sub-graph
//! whose every edge has explicit source-to-cap-arg assignment,
//! anchored by the input root URNs and output leaf URNs of the
//! strand.
//!
//! Resolution requires `&CapRegistry` access to look up each
//! cap's full argument list (`cap.args`) so the matching
//! algorithm has the per-arg media URN identities to match
//! against.
//!
//! ## Source-to-cap-arg matching
//!
//! For each edge being resolved, the algorithm runs a
//! Hungarian-style minimum-cost bipartite matching:
//!
//! - **Sources**: the URNs feeding this edge (one for the
//!   planner path; one or more for the parser path).
//! - **Cap arguments**: the cap definition's `args` list, each
//!   identified by its `media_urn` (RULE1 in
//!   `10-VALIDATION-RULES` enforces uniqueness across the args
//!   of a single cap).
//! - **Cost** of pairing source `s` with arg `a`:
//!   `spec(s) - spec(a)` if `s.conforms_to(a)` (always
//!   non-negative because `s` is at least as specific as `a`).
//!   If `s` does not conform to `a`, the pair is impossible.
//! - The minimum-cost assignment must be **unique**: among the
//!   set of assignments with the minimum total cost, only one
//!   bipartite matching may exist. If two distinct assignments
//!   tie, the result is `AmbiguousMachineNotation` and resolution
//!   fails hard. Source-vec position is NOT used as a tiebreaker.
//!
//! ## Connected components and the strand boundary
//!
//! The planner only ever produces one strand per call to
//! `resolve_strand`. The parser may produce multiple strands
//! (one per connected component of the wiring graph) and calls
//! `resolve_wiring_set` once per component. Each call yields a
//! single `MachineStrand` whose anchors and edges are derived
//! solely from that component.

use std::collections::HashMap;

use crate::cap::registry::CapRegistry;
use crate::planner::{Strand, StrandStepType};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::MachineAbstractionError;
use super::graph::{EdgeAssignmentBinding, MachineEdge, MachineStrand, NodeId};

/// One wiring after the caller has pre-interned its source and
/// target slots into `NodeId`s against a parallel
/// `nodes: Vec<MediaUrn>` table.
///
/// The resolver consumes this shape via `resolve_pre_interned`
/// and does NOT do any URN-based interning of its own. Two
/// distinct `NodeId`s whose underlying URNs are
/// `is_equivalent` stay distinct — this is what the notation
/// parser needs in order to honor the user's node-name
/// identity contract (two different node names are two
/// different data positions even if they share a URN).
///
/// The planner-strand path uses `resolve_wiring_set`, which
/// translates `ResolvedWiring`s into `PreInternedWiring`s by
/// interning equivalent URNs into the same `NodeId`.
#[derive(Debug, Clone)]
pub struct PreInternedWiring {
    pub cap_urn: CapUrn,
    /// Source NodeIds in the order the upstream layer wrote
    /// them. Position carries no semantics — the matching
    /// algorithm assigns each source slot to a cap arg by
    /// minimum-cost bipartite matching, not by index.
    pub source_node_ids: Vec<NodeId>,
    /// Target NodeId.
    pub target_node_id: NodeId,
    pub is_loop: bool,
}

/// Resolve a planner-produced `Strand` into a single
/// `MachineStrand`.
///
/// Walks the strand step-by-step and pre-interns `NodeId`s
/// using **positional flow** — each cap step's input position
/// is linked to the preceding cap step's output position iff
/// their URNs are on the same specialization chain
/// (`is_comparable`). Each step's output always allocates a
/// FRESH `NodeId`.
///
/// This is the correct interning policy for planner-produced
/// strands because the planner chains caps by conformance:
/// cap A's `out_spec` may be more specific than cap B's
/// `in_spec`, but at runtime the more-specific data flows
/// through both. The more-specific URN wins as the canonical
/// representative of the shared data position.
///
/// `ForEach` sets `is_loop = true` on the next cap and
/// preserves the boundary position through the cardinality
/// transition. `Collect` is elided.
pub fn resolve_strand(
    strand: &Strand,
    registry: &CapRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    let mut nodes: Vec<MediaUrn> = Vec::new();
    let mut pre_interned: Vec<PreInternedWiring> = Vec::new();
    let mut pending_loop = false;

    // The NodeId of the most recently produced output
    // position. The NEXT cap step's source will try to
    // reuse this if the URNs are comparable.
    let mut prev_target: Option<NodeId> = None;

    for step in &strand.steps {
        match &step.step_type {
            StrandStepType::Cap { cap_urn, .. } => {
                // Source: reuse prev_target if comparable
                // to this step's from_spec, otherwise
                // allocate a new root position.
                let source_id = match prev_target {
                    Some(pt)
                        if nodes[pt as usize]
                            .is_comparable(&step.from_spec)
                            .unwrap_or(false) =>
                    {
                        // Same data position. If from_spec
                        // is more specific, refine the node.
                        if step.from_spec.specificity()
                            > nodes[pt as usize].specificity()
                        {
                            nodes[pt as usize] = step.from_spec.clone();
                        }
                        pt
                    }
                    _ => {
                        // No comparable predecessor — new
                        // root position.
                        let id = nodes.len() as NodeId;
                        nodes.push(step.from_spec.clone());
                        id
                    }
                };

                // Target: always a fresh position.
                let target_id = nodes.len() as NodeId;
                nodes.push(step.to_spec.clone());

                pre_interned.push(PreInternedWiring {
                    cap_urn: cap_urn.clone(),
                    source_node_ids: vec![source_id],
                    target_node_id: target_id,
                    is_loop: pending_loop,
                });
                pending_loop = false;

                // This target becomes the boundary for the
                // next cap step's source.
                prev_target = Some(target_id);
            }
            StrandStepType::ForEach { .. } => {
                pending_loop = true;
                // prev_target passes through ForEach
                // unchanged — the data position is the
                // same, only the cardinality changes.
            }
            StrandStepType::Collect { .. } => {
                // Elided — cardinality transitions are
                // implicit in the resolved data flow.
                // prev_target passes through unchanged.
            }
        }
    }

    if pre_interned.is_empty() {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    resolve_pre_interned(nodes, &pre_interned, registry, strand_index)
}

/// Resolve a planner-path wiring set into a `MachineStrand`.
///
/// The caller supplies `ResolvedWiring`s whose sources and
/// targets are concrete `MediaUrn`s (no NodeIds yet). This
/// function interns equivalent URNs into shared `NodeId`s —
/// the planner-path identity rule — and then delegates to
/// `resolve_pre_interned` for matching, ordering, anchor
/// computation, and cycle detection.
///
/// The notation parser path uses `resolve_pre_interned`
/// directly with `NodeId`s pre-allocated by user node name, so
/// that two distinct names that share a `MediaUrn` stay
/// distinct nodes.

/// Resolve a pre-interned wiring set into a `MachineStrand`.
///
/// The caller has already allocated `NodeId`s for every
/// distinct data position in the strand and built the parallel
/// `nodes: Vec<MediaUrn>` table. The resolver does NOT touch
/// the interning policy — two NodeIds whose URNs happen to be
/// `is_equivalent` stay distinct — and runs:
///
/// 1. Per-wiring source-to-cap-arg matching (Hungarian, with
///    uniqueness check).
/// 2. Cycle detection via Kahn's algorithm over the resulting
///    NodeId-keyed dependency graph.
/// 3. Canonical edge ordering with a structural tiebreaker.
/// 4. Anchor computation (NodeIds with no producer / no
///    consumer in the strand).
pub fn resolve_pre_interned(
    nodes: Vec<MediaUrn>,
    wirings: &[PreInternedWiring],
    registry: &CapRegistry,
    strand_index: usize,
) -> Result<MachineStrand, MachineAbstractionError> {
    if wirings.is_empty() {
        return Err(MachineAbstractionError::NoCapabilitySteps);
    }

    // Step 1: per-wiring source-to-cap-arg matching. The
    // matching is computed against the URNs of the source
    // NodeIds (looked up from the `nodes` table); the result
    // is a sorted assignment of cap-arg → NodeId pairs.
    let mut indexed_edges: Vec<MachineEdge> = Vec::with_capacity(wirings.len());
    for wiring in wirings {
        let cap = registry
            .get_cached_cap(&wiring.cap_urn.to_string())
            .ok_or_else(|| MachineAbstractionError::UnknownCap {
                cap_urn: wiring.cap_urn.to_string(),
            })?;

        // Build the list of data-flow input slots for this cap.
        //
        // Each cap arg may declare any of three sources:
        //   - `Stdin { stdin: <media URN> }` — runtime delivers
        //     the named-typed data to the arg slot via the
        //     bifaci stdin stream. THIS is the data-flow input.
        //   - `Position { ... }` — positional CLI argument.
        //   - `CliFlag { ... }` — named CLI flag.
        //
        // Args with NO stdin source are CLI / positional config
        // only — they receive their values at execution time
        // from cap_settings, slot_values, or default_value.
        // They are never matched against a wiring's source
        // URNs.
        //
        // For args that DO have a stdin source, the URN that
        // matters for matching is the stdin source's inner
        // type (e.g. `media:image;png`), NOT the arg's outer
        // `media_urn` (e.g. `media:file-path;textable`). The
        // outer is the slot identity that cartridge_runtime uses
        // to label the stream and to drive file-path
        // auto-conversion; the inner is the type the runtime
        // actually delivers into the slot. The resolver
        // matches against the inner type because that is what
        // upstream caps actually produce.
        //
        // We build two parallel vecs: `stdin_arg_urns` (the
        // URNs to match against, in the order they appear in
        // `cap.args`) and `stdin_arg_slot_urns` (the
        // corresponding slot identities the bindings will
        // record).
        let mut stdin_arg_urns: Vec<MediaUrn> = Vec::new();
        let mut stdin_arg_slot_urns: Vec<MediaUrn> = Vec::new();
        for arg in &cap.args {
            // Find the FIRST Stdin source on this arg (an
            // arg that lists multiple delivery routes still
            // has at most one stdin URN — the protocol gives
            // each arg a single stream).
            let stdin_urn_str = arg.sources.iter().find_map(|s| match s {
                crate::cap::definition::ArgSource::Stdin { stdin } => Some(stdin.clone()),
                _ => None,
            });
            if let Some(stdin_str) = stdin_urn_str {
                let stdin_urn = MediaUrn::from_string(&stdin_str).expect(
                    "cap registry invariant: every Stdin source URN is a valid MediaUrn",
                );
                let slot_urn = MediaUrn::from_string(&arg.media_urn).expect(
                    "cap registry invariant: every cap arg media_urn is a valid MediaUrn",
                );
                stdin_arg_urns.push(stdin_urn);
                stdin_arg_slot_urns.push(slot_urn);
            }
        }

        // Pull the source URNs out of the nodes table for
        // this wiring's source NodeIds.
        let source_urns: Vec<MediaUrn> = wiring
            .source_node_ids
            .iter()
            .map(|id| nodes[*id as usize].clone())
            .collect();

        // Run the bipartite minimum-cost matching against
        // the stdin URNs. The matching returns
        // `(matched_arg_urn, source_urn)` pairs where
        // `matched_arg_urn` is the stdin URN that the source
        // was assigned to. We then translate each matched
        // stdin URN back to its slot identity for the binding.
        let sorted_assignment = match_sources_to_args(
            &source_urns,
            &stdin_arg_urns,
            &wiring.cap_urn,
            strand_index,
        )?;

        // Build the bindings. The `cap_arg_media_urn` field
        // on each binding records the **slot identity**
        // (the cap arg's outer `media_urn`), since that is
        // the canonical identifier per RULE1. We look up the
        // slot identity by matching the assignment's stdin
        // URN back to the position in `stdin_arg_urns`.
        //
        // We also map each source URN back to its NodeId
        // position in `wiring.source_node_ids`, walking the
        // unconsumed positions to handle the case where two
        // source NodeIds happen to share a URN.
        let mut bindings: Vec<EdgeAssignmentBinding> =
            Vec::with_capacity(sorted_assignment.len());
        let mut consumed_positions: Vec<bool> = vec![false; wiring.source_node_ids.len()];
        for (matched_stdin_urn, source_urn) in &sorted_assignment {
            // Find the slot identity for this matched stdin URN.
            let slot_urn = stdin_arg_urns
                .iter()
                .zip(stdin_arg_slot_urns.iter())
                .find(|(stdin, _)| stdin.is_equivalent(matched_stdin_urn).unwrap_or(false))
                .map(|(_, slot)| slot.clone())
                .expect(
                    "matching returned a stdin URN that isn't in the cap's stdin args list",
                );

            // Find the source NodeId position by URN equivalence.
            let mut chosen_pos: Option<usize> = None;
            for (pos, sid) in wiring.source_node_ids.iter().enumerate() {
                if consumed_positions[pos] {
                    continue;
                }
                if nodes[*sid as usize]
                    .is_equivalent(source_urn)
                    .unwrap_or(false)
                {
                    chosen_pos = Some(pos);
                    break;
                }
            }
            let pos = chosen_pos.expect(
                "matching returned a source URN that doesn't appear in the wiring's source positions",
            );
            consumed_positions[pos] = true;
            bindings.push(EdgeAssignmentBinding {
                cap_arg_media_urn: slot_urn,
                source: wiring.source_node_ids[pos],
            });
        }

        // The bindings vec is currently in the order produced by
        // `sorted_assignment` (sorted by stdin URN). To keep the
        // canonical equivalence comparison stable, re-sort by
        // slot identity (`cap_arg_media_urn`) before storing.
        bindings.sort_by(|a, b| a.cap_arg_media_urn.cmp(&b.cap_arg_media_urn));

        indexed_edges.push(MachineEdge {
            cap_urn: wiring.cap_urn.clone(),
            assignment: bindings,
            target: wiring.target_node_id,
            is_loop: wiring.is_loop,
        });
    }

    // Step 2: cycle detection + canonical edge order.
    //
    // The data-flow dependency relation: edge B depends on
    // edge A iff some binding in B's assignment has
    // `source == A.target` (NodeId equality).
    let canonical_order = topo_sort(&indexed_edges, &nodes, strand_index)?;
    let edges: Vec<MachineEdge> = canonical_order
        .into_iter()
        .map(|i| indexed_edges[i].clone())
        .collect();

    // Step 3: anchor computation.
    let mut produced_node_ids: std::collections::HashSet<NodeId> = Default::default();
    let mut consumed_node_ids: std::collections::HashSet<NodeId> = Default::default();
    for e in &edges {
        produced_node_ids.insert(e.target);
        for b in &e.assignment {
            consumed_node_ids.insert(b.source);
        }
    }

    let mut input_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !produced_node_ids.contains(id) && consumed_node_ids.contains(id))
        .collect();
    let mut output_anchor_ids: Vec<NodeId> = (0..nodes.len() as NodeId)
        .filter(|id| !consumed_node_ids.contains(id) && produced_node_ids.contains(id))
        .collect();

    // Sort anchors by canonical (URN, NodeId) order so the
    // result is stable across different node-allocation orders
    // that nevertheless yield equivalent strands.
    input_anchor_ids.sort_by(|a, b| {
        let urn_cmp = nodes[*a as usize].cmp(&nodes[*b as usize]);
        if urn_cmp == std::cmp::Ordering::Equal {
            a.cmp(b)
        } else {
            urn_cmp
        }
    });
    output_anchor_ids.sort_by(|a, b| {
        let urn_cmp = nodes[*a as usize].cmp(&nodes[*b as usize]);
        if urn_cmp == std::cmp::Ordering::Equal {
            a.cmp(b)
        } else {
            urn_cmp
        }
    });

    Ok(MachineStrand::from_resolved(
        nodes,
        edges,
        input_anchor_ids,
        output_anchor_ids,
    ))
}

// =============================================================================
// Source-to-cap-arg matching (Hungarian-style minimum-cost bipartite assignment
// with brute-force uniqueness check)
// =============================================================================

/// Match a wiring's sources to a cap's input args by minimum
/// total specificity-distance, with a uniqueness requirement.
///
/// Returns the matched pairs as `(cap_arg_media_urn, source_urn)`,
/// sorted by `cap_arg_media_urn` (via `MediaUrn`'s structural
/// `Ord`). Returns errors when:
///
/// - A source has no candidate arg (`UnmatchedSourceInCapArgs`).
/// - The minimum-cost matching is not unique
///   (`AmbiguousMachineNotation`).
fn match_sources_to_args(
    sources: &[MediaUrn],
    args: &[MediaUrn],
    cap_urn: &CapUrn,
    strand_index: usize,
) -> Result<Vec<(MediaUrn, MediaUrn)>, MachineAbstractionError> {
    if sources.len() > args.len() {
        // Pigeonhole: at least one source has no arg slot.
        // Find the first source with no candidate arg and
        // report it. (If all sources DO conform to some arg,
        // we still can't match — but that's still a
        // structural unmatched-source condition.)
        for source in sources {
            if !args.iter().any(|a| source.conforms_to(a).unwrap_or(false)) {
                return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
                    strand_index,
                    cap_urn: cap_urn.to_string(),
                    source_urn: source.to_string(),
                });
            }
        }
        // All sources have a candidate, but there are more
        // sources than args — at least one source MUST end up
        // unmatched. Treat the first source as unmatched.
        return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
            strand_index,
            cap_urn: cap_urn.to_string(),
            source_urn: sources[0].to_string(),
        });
    }

    // Build the candidate matrix. cost[s][a] is Some(distance)
    // if `sources[s]` conforms to `args[a]`, else None.
    let n_sources = sources.len();
    let n_args = args.len();
    let mut cost: Vec<Vec<Option<i64>>> = vec![vec![None; n_args]; n_sources];

    for (s_idx, source) in sources.iter().enumerate() {
        for (a_idx, arg) in args.iter().enumerate() {
            if source.conforms_to(arg).unwrap_or(false) {
                let distance = source.specificity() as i64 - arg.specificity() as i64;
                // Always non-negative since source ⪯ arg implies
                // spec(source) ≥ spec(arg).
                debug_assert!(
                    distance >= 0,
                    "source {} conforms to arg {} but distance {} is negative",
                    source,
                    arg,
                    distance
                );
                cost[s_idx][a_idx] = Some(distance);
            }
        }
        // Per-source: at least one candidate, else unmatched.
        if cost[s_idx].iter().all(|c| c.is_none()) {
            return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index,
                cap_urn: cap_urn.to_string(),
                source_urn: source.to_string(),
            });
        }
    }

    // Brute-force enumeration of perfect matchings of sources
    // to a subset of args.
    //
    // For each ordered injection f: [0..n_sources) ↣ [0..n_args)
    // such that cost[s][f(s)].is_some() for all s, compute
    // total cost. Track the minimum and how many matchings
    // achieve it.
    //
    // For the input sizes the system actually encounters (a
    // cap typically has 1–5 args, edges typically have 1–5
    // sources), brute force enumerates at most A_n_args choose
    // n_sources permutations — bounded.
    let mut best_cost: Option<i64> = None;
    let mut best_assignments: Vec<Vec<usize>> = Vec::new();

    let mut current: Vec<usize> = vec![usize::MAX; n_sources];
    let mut used: Vec<bool> = vec![false; n_args];
    enumerate_matchings(
        &cost,
        0,
        &mut current,
        &mut used,
        &mut best_cost,
        &mut best_assignments,
    );

    if best_cost.is_none() {
        // No injection covers all sources: every per-source
        // candidate set is non-empty, but the candidate sets
        // collectively can't all be claimed by distinct args
        // (Hall's theorem violation). Pick the first source as
        // the canonical "unmatched" representative.
        return Err(MachineAbstractionError::UnmatchedSourceInCapArgs {
            strand_index,
            cap_urn: cap_urn.to_string(),
            source_urn: sources[0].to_string(),
        });
    }

    if best_assignments.len() != 1 {
        return Err(MachineAbstractionError::AmbiguousMachineNotation {
            strand_index,
            cap_urn: cap_urn.to_string(),
        });
    }

    // Convert the unique assignment into (cap_arg, source)
    // pairs sorted by cap_arg_media_urn.
    let assignment = &best_assignments[0];
    let mut pairs: Vec<(MediaUrn, MediaUrn)> = (0..n_sources)
        .map(|s_idx| {
            let a_idx = assignment[s_idx];
            (args[a_idx].clone(), sources[s_idx].clone())
        })
        .collect();
    pairs.sort_by(|x, y| x.0.cmp(&y.0));
    Ok(pairs)
}

/// Recursively enumerate all injections of sources into args
/// with a defined cost, tracking the minimum total cost and
/// the assignments that achieve it.
fn enumerate_matchings(
    cost: &[Vec<Option<i64>>],
    s_idx: usize,
    current: &mut Vec<usize>,
    used: &mut Vec<bool>,
    best_cost: &mut Option<i64>,
    best_assignments: &mut Vec<Vec<usize>>,
) {
    let n_sources = cost.len();
    if s_idx == n_sources {
        // Compute total cost of `current`.
        let total: i64 = (0..n_sources)
            .map(|s| cost[s][current[s]].expect("matchings filter on Some(_)"))
            .sum();
        match best_cost {
            None => {
                *best_cost = Some(total);
                best_assignments.clear();
                best_assignments.push(current.clone());
            }
            Some(prev) if total < *prev => {
                *best_cost = Some(total);
                best_assignments.clear();
                best_assignments.push(current.clone());
            }
            Some(prev) if total == *prev => {
                best_assignments.push(current.clone());
            }
            Some(_) => {} // total > prev — discard
        }
        return;
    }

    for a_idx in 0..cost[s_idx].len() {
        if used[a_idx] {
            continue;
        }
        if cost[s_idx][a_idx].is_none() {
            continue;
        }
        used[a_idx] = true;
        current[s_idx] = a_idx;
        enumerate_matchings(cost, s_idx + 1, current, used, best_cost, best_assignments);
        used[a_idx] = false;
    }
}

// =============================================================================
// Topological sort with structural tiebreaker
// =============================================================================

/// Kahn's algorithm over the resolved data-flow dependency
/// graph. Returns the canonical ordering of edge indices.
///
/// Edge B depends on edge A iff some binding in B.assignment
/// has `source == A.target` (NodeId equality, since interning
/// has already collapsed equivalent URNs).
fn topo_sort(
    edges: &[MachineEdge],
    nodes: &[MediaUrn],
    strand_index: usize,
) -> Result<Vec<usize>, MachineAbstractionError> {
    let n = edges.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    // Map: NodeId → list of edge indices that produce this
    // NodeId as their target. (In a well-formed strand at most
    // one edge produces a given target node, but we don't
    // assume that — multiple producers would mean non-
    // deterministic data flow at runtime, which is itself a
    // structural error worth being permissive about and
    // letting the cycle / unmatched checks catch.)
    let mut producers_of: HashMap<NodeId, Vec<usize>> = HashMap::new();
    for (idx, e) in edges.iter().enumerate() {
        producers_of.entry(e.target).or_default().push(idx);
    }

    // Edge B's predecessors: any edge whose target is the
    // source of any binding in B.assignment.
    let mut indegree: Vec<usize> = vec![0; n];
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (b_idx, b) in edges.iter().enumerate() {
        for binding in &b.assignment {
            if let Some(producers) = producers_of.get(&binding.source) {
                for &a_idx in producers {
                    if a_idx == b_idx {
                        continue;
                    }
                    successors[a_idx].push(b_idx);
                    indegree[b_idx] += 1;
                }
            }
        }
    }

    let mut result: Vec<usize> = Vec::with_capacity(n);
    let mut ready: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    sort_ready(&mut ready, edges, nodes);

    while let Some(idx) = ready.first().copied() {
        ready.remove(0);
        result.push(idx);
        for &succ in &successors[idx] {
            indegree[succ] -= 1;
            if indegree[succ] == 0 {
                ready.push(succ);
                sort_ready(&mut ready, edges, nodes);
            }
        }
    }

    if result.len() < n {
        return Err(MachineAbstractionError::CyclicMachineStrand { strand_index });
    }

    Ok(result)
}

/// Sort the ready set in canonical structural order so Kahn's
/// algorithm produces a deterministic output. The order is:
///
/// 1. cap URN (structural `CapUrn::Ord`)
/// 2. assignment vec (element-wise structural `MediaUrn::Ord`
///    on `cap_arg_media_urn` then on the source's URN)
/// 3. target node URN (structural `MediaUrn::Ord`)
/// 4. `is_loop` flag
fn sort_ready(ready: &mut Vec<usize>, edges: &[MachineEdge], nodes: &[MediaUrn]) {
    ready.sort_by(|&a, &b| {
        let ea = &edges[a];
        let eb = &edges[b];
        match ea.cap_urn.cmp(&eb.cap_urn) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        // Compare assignments element-wise; pre-sorted by
        // cap_arg_media_urn so positional comparison is
        // canonical.
        for (ba, bb) in ea.assignment.iter().zip(eb.assignment.iter()) {
            match ba.cap_arg_media_urn.cmp(&bb.cap_arg_media_urn) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            match nodes[ba.source as usize].cmp(&nodes[bb.source as usize]) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
        }
        match ea.assignment.len().cmp(&eb.assignment.len()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match nodes[ea.target as usize].cmp(&nodes[eb.target as usize]) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        ea.is_loop.cmp(&eb.is_loop)
    });
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::{match_sources_to_args, resolve_strand};
    use crate::machine::error::MachineAbstractionError;
    use crate::machine::test_fixtures::{
        build_cap, build_cap_with_slot_stdin_pairs, cap, cap_step, collect_step, for_each_step,
        media, registry_with, strand_from_steps,
    };

    // ----- match_sources_to_args -------------------------------------------

    #[test]
    fn match_single_source_picks_unique_arg() {
        // Single source `media:pdf` against a one-arg cap. Trivial
        // bipartite matching: one source, one arg, exact tag-set
        // equivalence → distance 0 → unique → assignment is the
        // single pair.
        let sources = vec![media("media:pdf")];
        let args = vec![media("media:pdf")];
        let cap_urn = cap("cap:in=media:pdf;op=extract;out=\"media:txt;textable\"");
        let pairs = match_sources_to_args(&sources, &args, &cap_urn, 0)
            .expect("trivial single-source match must succeed");
        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].0.is_equivalent(&media("media:pdf")).unwrap());
        assert!(pairs[0].1.is_equivalent(&media("media:pdf")).unwrap());
    }

    #[test]
    fn match_more_specific_source_assigned_to_general_arg() {
        // The cap declares `media:textable`. The source is the
        // more-specific `media:page;textable`. The source must
        // conform (it does, page;textable ⪯ textable) and get
        // assigned to that arg with distance > 0.
        let sources = vec![media("media:page;textable")];
        let args = vec![media("media:textable")];
        let cap_urn = cap("cap:in=media:textable;op=make_decision;out=\"media:decision;textable\"");
        let pairs = match_sources_to_args(&sources, &args, &cap_urn, 0)
            .expect("more-specific source must be matched to its arg");
        assert!(pairs[0].0.is_equivalent(&media("media:textable")).unwrap());
        assert!(pairs[0].1.is_equivalent(&media("media:page;textable")).unwrap());
    }

    #[test]
    fn match_unmatched_source_fails_hard() {
        // The source URN does not conform to any of the cap's
        // args. Must surface as `UnmatchedSourceInCapArgs` —
        // never as a silent zero-cost or random pairing.
        let sources = vec![media("media:numeric")];
        let args = vec![media("media:textable")];
        let cap_urn = cap("cap:in=media:textable;op=t;out=media:textable");
        let err = match_sources_to_args(&sources, &args, &cap_urn, 7).unwrap_err();
        match err {
            MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index,
                cap_urn: cu,
                source_urn,
            } => {
                assert_eq!(strand_index, 7);
                assert!(cu.contains("make_decision") || cu.contains("op=t"));
                assert_eq!(source_urn, "media:numeric");
            }
            other => panic!("expected UnmatchedSourceInCapArgs, got {:?}", other),
        }
    }

    #[test]
    fn match_two_sources_disambiguated_by_specificity() {
        // Two sources, two args. One source perfectly matches one
        // arg with distance 0; the other can only conform to the
        // remaining arg. The resolver picks the unique minimum-
        // cost matching.
        //
        // sources: [media:image;png, media:model-spec;textable]
        // args:    [media:image;png, media:textable]
        //
        // image;png ⪯ image;png (dist 0); image;png ⪯ textable? no
        // model-spec;textable ⪯ image;png? no
        // model-spec;textable ⪯ textable (dist 1)
        //
        // Unique optimum: (image;png → image;png), (model-spec;textable → textable)
        let sources = vec![media("media:image;png"), media("media:model-spec;textable")];
        let args = vec![media("media:image;png"), media("media:textable")];
        let cap_urn = cap("cap:in=\"media:image;png\";op=describe;out=\"media:image-description;textable\"");
        let pairs = match_sources_to_args(&sources, &args, &cap_urn, 0).unwrap();
        assert_eq!(pairs.len(), 2);
        // Pairs are sorted by cap_arg_media_urn structurally.
        // image;png and textable: structural Ord places
        // image;png first (more tags / different prefix string).
        // Don't depend on the exact sort order; check the
        // mapping by content instead.
        let mut found_image = false;
        let mut found_text = false;
        for (arg, src) in &pairs {
            if arg.is_equivalent(&media("media:image;png")).unwrap() {
                assert!(src.is_equivalent(&media("media:image;png")).unwrap());
                found_image = true;
            } else if arg.is_equivalent(&media("media:textable")).unwrap() {
                assert!(src.is_equivalent(&media("media:model-spec;textable")).unwrap());
                found_text = true;
            }
        }
        assert!(found_image && found_text, "both arg slots must be assigned");
    }

    #[test]
    fn match_ambiguous_when_two_sources_could_swap() {
        // Two sources that can both feed both args at exactly
        // the same total cost. The minimum-cost matching is not
        // unique → AmbiguousMachineNotation.
        //
        // Both sources are `media:textable`; both args are
        // `media:textable`. Distance 0 either way; both
        // permutations are tied at total cost 0.
        let sources = vec![media("media:textable"), media("media:textable")];
        let args = vec![media("media:textable"), media("media:textable")];
        let cap_urn = cap("cap:in=media:textable;op=t;out=media:textable");
        let err = match_sources_to_args(&sources, &args, &cap_urn, 0).unwrap_err();
        assert!(
            matches!(err, MachineAbstractionError::AmbiguousMachineNotation { .. }),
            "expected ambiguous, got {:?}",
            err
        );
    }

    #[test]
    fn match_more_sources_than_args_fails_hard() {
        let sources = vec![media("media:pdf"), media("media:pdf"), media("media:pdf")];
        let args = vec![media("media:pdf"), media("media:pdf")];
        let cap_urn = cap("cap:in=media:pdf;op=t;out=media:pdf");
        let err = match_sources_to_args(&sources, &args, &cap_urn, 0).unwrap_err();
        assert!(matches!(
            err,
            MachineAbstractionError::UnmatchedSourceInCapArgs { .. }
        ));
    }

    // ----- resolve_strand (planner path) -----------------------------------

    #[test]
    fn resolve_strand_single_cap_produces_one_edge() {
        let extract_cap = build_cap(
            "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
            "extract",
            &["media:pdf"],
            "media:txt;textable",
        );
        let registry = registry_with(vec![extract_cap]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
                "extract",
                "media:pdf",
                "media:txt;textable",
            )],
            "pdf to txt",
        );
        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 1);
        assert_eq!(resolved.edges()[0].assignment.len(), 1);
        // The single edge's assignment maps the cap arg
        // media:pdf to a node whose URN is media:pdf.
        let binding = &resolved.edges()[0].assignment[0];
        assert!(binding
            .cap_arg_media_urn
            .is_equivalent(&media("media:pdf"))
            .unwrap());
        let src_urn = resolved.node_urn(binding.source);
        assert!(src_urn.is_equivalent(&media("media:pdf")).unwrap());
        // Anchors: input is media:pdf, output is media:txt;textable.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(outputs[0].is_equivalent(&media("media:txt;textable")).unwrap());
    }

    #[test]
    fn resolve_strand_chained_caps_share_intermediate_node() {
        // Two-step strand: pdf → extract → txt → embed → vec.
        // The intermediate node `media:txt;textable` is produced
        // by extract and consumed by embed. The resolver must
        // intern these as the SAME NodeId, so the strand has
        // exactly three node positions, not four.
        let extract = build_cap(
            "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
            "extract",
            &["media:pdf"],
            "media:txt;textable",
        );
        let embed = build_cap(
            "cap:in=media:textable;op=embed;out=\"media:vec;record\"",
            "embed",
            &["media:textable"],
            "media:vec;record",
        );
        let registry = registry_with(vec![extract, embed]);

        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
                    "extract",
                    "media:pdf",
                    "media:txt;textable",
                ),
                cap_step(
                    "cap:in=media:textable;op=embed;out=\"media:vec;record\"",
                    "embed",
                    "media:txt;textable",
                    "media:vec;record",
                ),
            ],
            "pdf to vec",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 2);
        assert_eq!(
            resolved.nodes().len(),
            3,
            "three distinct data positions: pdf, txt;textable, vec;record"
        );

        // The first edge's target NodeId must equal the second
        // edge's primary source NodeId.
        let extract_target = resolved.edges()[0].target;
        let embed_source = resolved.edges()[1].assignment[0].source;
        assert_eq!(
            extract_target, embed_source,
            "intermediate data position must be one shared NodeId"
        );

        // Anchors.
        let inputs = resolved.input_anchors();
        let outputs = resolved.output_anchors();
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert!(inputs[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(outputs[0].is_equivalent(&media("media:vec;record")).unwrap());
    }

    #[test]
    fn resolve_strand_foreach_marks_following_cap_as_loop() {
        // ForEach immediately followed by a cap. The cap's edge
        // must have is_loop=true. Collect at the end is elided.
        let disbind = build_cap(
            "cap:in=media:pdf;op=disbind;out=\"media:page;textable\"",
            "disbind",
            &["media:pdf"],
            "media:page;textable",
        );
        let make_decision = build_cap(
            "cap:in=media:textable;op=make_decision;out=\"media:decision;json;record;textable\"",
            "make_decision",
            &["media:textable"],
            "media:decision;json;record;textable",
        );
        let registry = registry_with(vec![disbind, make_decision]);

        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=media:pdf;op=disbind;out=\"media:page;textable\"",
                    "disbind",
                    "media:pdf",
                    "media:page;textable",
                ),
                for_each_step("media:page;textable"),
                cap_step(
                    "cap:in=media:textable;op=make_decision;out=\"media:decision;json;record;textable\"",
                    "make_decision",
                    "media:textable",
                    "media:decision;json;record;textable",
                ),
                collect_step("media:decision;json;record;textable"),
            ],
            "disbind+foreach+make_decision",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect("must resolve");
        assert_eq!(resolved.edges().len(), 2);
        // First edge (disbind) is not a loop; second
        // (make_decision) is.
        let disbind_edge = resolved
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("disbind"))
            .expect("disbind edge present");
        let decision_edge = resolved
            .edges()
            .iter()
            .find(|e| e.cap_urn.to_string().contains("make_decision"))
            .expect("make_decision edge present");
        assert!(!disbind_edge.is_loop, "disbind is not in a loop");
        assert!(decision_edge.is_loop, "make_decision is inside ForEach");

        // Critical: disbind's target NodeId must be the same
        // as make_decision's source NodeId — the intermediate
        // data position (media:page;textable) is shared even
        // though disbind declares out=media:page;textable and
        // make_decision declares in=media:textable (less
        // specific, but on the same specialization chain).
        // Positional interning collapses them.
        let disbind_target = disbind_edge.target;
        let decision_source = decision_edge.assignment[0].source;
        assert_eq!(
            disbind_target, decision_source,
            "disbind target and make_decision source must share the same NodeId (positional interning)"
        );
        // The canonical URN at that shared node must be
        // the more-specific one: media:page;textable.
        assert!(
            resolved
                .node_urn(disbind_target)
                .is_equivalent(&media("media:page;textable"))
                .unwrap(),
            "shared node URN must be the more-specific media:page;textable, got: {}",
            resolved.node_urn(disbind_target)
        );
    }

    #[test]
    fn resolve_strand_unknown_cap_fails_hard() {
        let registry = registry_with(vec![]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
                "extract",
                "media:pdf",
                "media:txt;textable",
            )],
            "pdf to txt with empty registry",
        );
        let err = resolve_strand(&strand, &registry, 0).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::UnknownCap { .. }));
    }

    #[test]
    fn resolve_strand_no_cap_steps_fails_hard() {
        let registry = registry_with(vec![]);
        let strand = strand_from_steps(
            vec![for_each_step("media:pdf"), collect_step("media:pdf")],
            "no caps at all",
        );
        let err = resolve_strand(&strand, &registry, 0).unwrap_err();
        assert!(matches!(err, MachineAbstractionError::NoCapabilitySteps));
    }

    #[test]
    fn resolve_strand_canonical_anchor_order_is_stable() {
        // Two strands built from identical caps in identical
        // positions must produce byte-identical canonical
        // anchor URN order. This pins the structural sort.
        let extract = build_cap(
            "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
            "extract",
            &["media:pdf"],
            "media:txt;textable",
        );
        let registry = registry_with(vec![extract]);
        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
                "extract",
                "media:pdf",
                "media:txt;textable",
            )],
            "pdf to txt",
        );
        let r1 = resolve_strand(&strand, &registry, 0).unwrap();
        let r2 = resolve_strand(&strand, &registry, 0).unwrap();
        let i1 = r1.input_anchors();
        let i2 = r2.input_anchors();
        assert_eq!(i1.len(), i2.len());
        for (a, b) in i1.iter().zip(i2.iter()) {
            assert!(a.is_equivalent(b).unwrap());
        }
    }

    #[test]
    fn resolve_strand_inverse_format_converters_no_cycle() {
        // A strand that visits two inverse format converters
        // (numeric;textable → integer;numeric;textable →
        // numeric;textable). Under positional interning, each
        // cap step's target is a FRESH NodeId, so the strand's
        // source NodeId(0) (numeric;textable) and the second
        // step's target NodeId(2) (also numeric;textable) are
        // DISTINCT positions. There is no cycle.
        //
        // The planner's visited-set prevents the path finder
        // from producing this strand in practice (it would
        // revisit the same visited key). But programmatic
        // strand construction can produce it, and the resolver
        // must handle it correctly.
        let to_int = build_cap(
            "cap:in=\"media:numeric;textable\";op=coerce_int;out=\"media:integer;numeric;textable\"",
            "coerce_int",
            &["media:numeric;textable"],
            "media:integer;numeric;textable",
        );
        let to_num = build_cap(
            "cap:in=\"media:integer;numeric;textable\";op=coerce_num;out=\"media:numeric;textable\"",
            "coerce_num",
            &["media:integer;numeric;textable"],
            "media:numeric;textable",
        );
        let registry = registry_with(vec![to_int, to_num]);
        let strand = strand_from_steps(
            vec![
                cap_step(
                    "cap:in=\"media:numeric;textable\";op=coerce_int;out=\"media:integer;numeric;textable\"",
                    "coerce_int",
                    "media:numeric;textable",
                    "media:integer;numeric;textable",
                ),
                cap_step(
                    "cap:in=\"media:integer;numeric;textable\";op=coerce_num;out=\"media:numeric;textable\"",
                    "coerce_num",
                    "media:integer;numeric;textable",
                    "media:numeric;textable",
                ),
            ],
            "round-trip numeric coercion",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect(
            "inverse format converters must resolve without cycle under positional interning",
        );
        // Three distinct data positions: input
        // (numeric;textable), intermediate
        // (integer;numeric;textable), and output
        // (numeric;textable). Input and output share a URN
        // but are distinct NodeIds.
        assert_eq!(resolved.nodes().len(), 3);
        assert_eq!(resolved.edges().len(), 2);
        // coerce_int's target (intermediate) is shared with
        // coerce_num's source — same positional boundary.
        let int_target = resolved.edges()[0].target;
        let num_source = resolved.edges()[1].assignment[0].source;
        assert_eq!(int_target, num_source);
    }

    #[test]
    fn resolve_strand_disbind_pdf_with_file_path_slot_identity() {
        // Regression: a cap whose arg slot identity differs
        // from its stdin source URN. The disbind cap declares
        // `media:file-path;textable` as the slot identity but
        // its stdin source delivers `media:pdf` (this is the
        // wire-level wraparound: cartridge_runtime auto-converts
        // a file-path argument into a stdin byte stream of
        // the inner type).
        //
        // The resolver MUST match the wiring's `media:pdf`
        // source against the stdin URN of the arg, NOT against
        // the slot identity. Before this fix the resolver
        // would have returned `UnmatchedSourceInCapArgs`
        // because `media:pdf` does not conform to
        // `media:file-path;textable`.
        let disbind = build_cap_with_slot_stdin_pairs(
            "cap:in=media:pdf;op=disbind;out=\"media:textable;page\"",
            "disbind",
            &[("media:file-path;textable", "media:pdf")],
            "media:textable;page",
        );
        let registry = registry_with(vec![disbind]);

        let strand = strand_from_steps(
            vec![cap_step(
                "cap:in=media:pdf;op=disbind;out=\"media:textable;page\"",
                "disbind",
                "media:pdf",
                "media:textable;page",
            )],
            "pdf to pages",
        );

        let resolved = resolve_strand(&strand, &registry, 0).expect(
            "disbind strand must resolve via stdin URN matching, not slot identity",
        );
        assert_eq!(resolved.edges().len(), 1);
        let binding = &resolved.edges()[0].assignment[0];

        // The binding's `cap_arg_media_urn` must be the SLOT
        // identity (`media:file-path;textable`), since that is
        // what the cap definition uses to label the arg slot
        // (RULE1).
        assert!(
            binding
                .cap_arg_media_urn
                .is_equivalent(&media("media:file-path;textable"))
                .unwrap(),
            "binding cap_arg_media_urn must be the slot identity, got: {}",
            binding.cap_arg_media_urn
        );

        // The source NodeId must point at a node whose URN is
        // `media:pdf` — the data-type URN, what the planner
        // sees flowing on the wire.
        let source_urn = resolved.node_urn(binding.source);
        assert!(
            source_urn.is_equivalent(&media("media:pdf")).unwrap(),
            "source node URN must be media:pdf (the data-type URN), got: {}",
            source_urn
        );
    }
}
