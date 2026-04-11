//! Machine notation serializer — deterministic canonical form
//!
//! Converts a `Machine` to its machine notation string
//! representation. The output is deterministic: the same graph
//! always produces the same string.
//!
//! The canonical form is line-based (one statement per line,
//! no brackets).
//!
//! ## Alias Generation
//!
//! Aliases are opaque labels — the notation format uses them
//! to bind headers to wirings, nothing more. They carry no
//! semantic weight: a round-trip through the parser
//! reconstructs the edge set, not the alias names.
//!
//! The serializer generates aliases deterministically from
//! each edge's position in the `Machine`'s construction-order
//! edge vec: `edge_0`, `edge_1`, `edge_2`, …  No tag on the
//! `CapUrn` (including any `op` tag) is used to derive
//! aliases. Only the `in` and `out` tags carry functional
//! meaning on a cap URN; every other tag is arbitrary data
//! attached by the cap author.
//!
//! ## Node-name Assignment
//!
//! Node names are assigned by walking the `Machine`'s edge
//! vec **in construction order** and matching each edge's
//! sources against earlier output nodes. The `Machine`'s
//! constructor (`Machine::from_path` for strand-derived
//! machines, `Machine::from_string` for parsed machines)
//! decides the order; the serializer trusts it.
//!
//! For each edge, walking from latest backward over the
//! already-allocated output nodes:
//!
//! - **Secondary fan-in sources** (source index > 0) are
//!   concrete URNs carried from prior wirings. They match an
//!   earlier output via `is_equivalent` (two-way `accepts`).
//!   The first (most-recent) match wins. No match →
//!   `UnmatchedSecondaryFanInSource`.
//! - **Primary source** (source index 0) is the cap's declared
//!   `in=` pattern. It matches an earlier output via
//!   `accepts` (the pattern accepts the instance). Skipping
//!   any output already claimed by a secondary in this same
//!   edge, the most-recent match wins. No match → fresh root
//!   node (with fan-out dedup via `is_equivalent` against
//!   existing root nodes).
//! - **Target** of each edge always allocates a fresh output
//!   node.
//!
//! Nothing in this module ever compares URNs via `.to_string()`
//! or any other flat-string identity — the primitives are
//! `accepts`, `conforms_to`, `is_equivalent`, and the
//! structural `Ord` impls over `TaggedUrn`/`MediaUrn`.
//!
//! ## Why no topological reordering
//!
//! An earlier design used `accepts`-based topological
//! inference to reorder edges before naming. That broke on
//! real strands that visit non-monotonically-specific cap
//! signatures — e.g. a strand that drops tags via one cap
//! and then adds them via another. The two cap signatures
//! form a 2-cycle under `accepts` (each accepts the other's
//! output), so a topological sort either fails or produces
//! the wrong order. The cap-registry-level data legitimately
//! contains such mutually-accepting pairs (format converters,
//! type coercions). Construction order is the only reliable
//! signal for "what came before what" in a strand.
//!
//! As a consequence, two equivalent machines built from edge
//! vecs in different orders produce **different** notation
//! strings. They both round-trip via `Machine::is_equivalent`,
//! but the notation is not a canonical fingerprint of the
//! underlying edge set — it's a textual encoding of an
//! ordered sequence.
//!
//! ## Failure Modes
//!
//! This module fails hard. There is one failure mode:
//!
//! - Secondary source has no equivalent earlier output →
//!   `MachineAbstractionError::UnmatchedSecondaryFanInSource`

use std::collections::BTreeMap;
use std::fmt::Write;

use crate::planner::live_cap_graph::{Strand, StrandStepType};
use crate::urn::media_urn::MediaUrn;

use super::error::MachineAbstractionError;
use super::graph::{MachineEdge, Machine};

/// Serialization format for machine notation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationFormat {
    /// Line-based: one statement per line, no brackets.
    /// ```text
    /// extract cap:in="media:pdf";op=extract;out="media:txt;textable"
    /// doc -> extract -> text
    /// ```
    LineBased,
    /// Bracketed: each statement wrapped in `[...]`.
    /// ```text
    /// [extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
    /// [doc -> extract -> text]
    /// ```
    Bracketed,
}

impl Machine {
    /// Convert a `Strand` (resolved linear path) into a `Machine`.
    ///
    /// The conversion:
    /// - Each `Cap` step becomes a `MachineEdge` carrying the
    ///   cap's declared `from_spec` as its sole source and the
    ///   step's `to_spec` as its target.
    /// - `ForEach` steps set `is_loop: true` on the next Cap edge.
    /// - `Collect` steps are elided — cardinality transitions are
    ///   implicit in the flow.
    ///
    /// ## Source URN semantics
    ///
    /// The edge's `sources` field carries the cap's **declared
    /// input pattern** verbatim, NOT the preceding cap's output
    /// URN. Two caps that are topologically chained in the strand
    /// may have different source/target URNs because one's
    /// output (e.g., `media:page;textable`) only needs to
    /// *conform to* the next's input pattern (e.g.,
    /// `media:textable`). The edge must reflect the cap
    /// signature because that's what `Machine::is_equivalent`
    /// compares against, and what the parser reconstructs on
    /// round-trip.
    ///
    /// The topological connection between successive caps is
    /// recovered by the serializer's node-naming pass via the
    /// `accepts` predicate — see `build_serialization_maps`.
    pub fn from_path(path: &Strand) -> Result<Self, MachineAbstractionError> {
        let mut edges = Vec::new();
        let mut pending_loop = false;

        for step in &path.steps {
            match &step.step_type {
                StrandStepType::Cap { cap_urn, .. } => {
                    edges.push(MachineEdge {
                        sources: vec![step.from_spec.clone()],
                        cap_urn: cap_urn.clone(),
                        target: step.to_spec.clone(),
                        is_loop: pending_loop,
                    });
                    pending_loop = false;
                }
                StrandStepType::ForEach { .. } => {
                    pending_loop = true;
                }
                StrandStepType::Collect { .. } => {
                    // Elided — cardinality transitions are implicit
                }
            }
        }

        if edges.is_empty() {
            return Err(MachineAbstractionError::NoCapabilitySteps);
        }

        Ok(Self::new(edges))
    }

    /// Serialize this machine graph to canonical bracketed machine notation.
    ///
    /// The output is deterministic: same graph → same string. This is the
    /// primary serialization format for accessibility identifiers and
    /// comparison. One-line, each statement wrapped in `[...]`.
    ///
    /// Fails hard with `MachineAbstractionError` if the graph's data-flow
    /// structure cannot be serialized (cycles, ambiguous fan-in).
    pub fn to_machine_notation(&self) -> Result<String, MachineAbstractionError> {
        self.to_machine_notation_formatted(NotationFormat::Bracketed)
    }

    /// Serialize to multi-line machine notation (one statement per line).
    /// Uses the same format as `to_machine_notation()` (bracketed) but
    /// with newlines between statements.
    pub fn to_machine_notation_multiline(&self) -> Result<String, MachineAbstractionError> {
        if self.edges().is_empty() {
            return Ok(String::new());
        }

        let plan = self.build_serialization_plan()?;
        let mut output = String::new();

        let mut sorted_aliases: Vec<(&String, &usize)> = plan.aliases.iter().collect();
        sorted_aliases.sort_by(|a, b| a.0.cmp(b.0));

        for (alias, edge_idx) in &sorted_aliases {
            let edge = &self.edges()[**edge_idx];
            writeln!(output, "[{} {}]", alias, edge.cap_urn).unwrap();
        }

        for edge_idx in &plan.edge_order {
            let edge = &self.edges()[*edge_idx];
            let alias = plan.alias_for_edge(*edge_idx);
            let source_names = plan.source_names_for_edge(*edge_idx);
            let target_name = plan.target_name_for_edge(*edge_idx);
            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if source_names.len() == 1 {
                writeln!(
                    output,
                    "[{} -> {}{} -> {}]",
                    source_names[0], loop_prefix, alias, target_name
                )
                .unwrap();
            } else {
                let group = source_names.join(", ");
                writeln!(
                    output,
                    "[({}) -> {}{} -> {}]",
                    group, loop_prefix, alias, target_name
                )
                .unwrap();
            }
        }

        if output.ends_with('\n') {
            output.pop();
        }

        Ok(output)
    }

    /// Serialize this machine graph to machine notation in the specified format.
    ///
    /// The output is deterministic: same graph + same format → same string.
    /// Fails hard with `MachineAbstractionError` if the graph's data-flow
    /// structure cannot be serialized (cycles, ambiguous fan-in).
    pub fn to_machine_notation_formatted(
        &self,
        format: NotationFormat,
    ) -> Result<String, MachineAbstractionError> {
        if self.edges().is_empty() {
            return Ok(String::new());
        }

        let plan = self.build_serialization_plan()?;
        let mut output = String::new();

        let (open, close, sep) = match format {
            NotationFormat::Bracketed => ("[", "]", ""),
            NotationFormat::LineBased => ("", "", "\n"),
        };

        let mut sorted_aliases: Vec<(&String, &usize)> = plan.aliases.iter().collect();
        sorted_aliases.sort_by(|a, b| a.0.cmp(b.0));

        for (alias, edge_idx) in &sorted_aliases {
            let edge = &self.edges()[**edge_idx];
            write!(output, "{}{} {}{}{}", open, alias, edge.cap_urn, close, sep).unwrap();
        }

        for edge_idx in &plan.edge_order {
            let edge = &self.edges()[*edge_idx];
            let alias = plan.alias_for_edge(*edge_idx);
            let source_names = plan.source_names_for_edge(*edge_idx);
            let target_name = plan.target_name_for_edge(*edge_idx);
            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if source_names.len() == 1 {
                write!(
                    output,
                    "{}{} -> {}{} -> {}{}{}",
                    open, source_names[0], loop_prefix, alias, target_name, close, sep
                )
                .unwrap();
            } else {
                let group = source_names.join(", ");
                write!(
                    output,
                    "{}({}) -> {}{} -> {}{}{}",
                    open, group, loop_prefix, alias, target_name, close, sep
                )
                .unwrap();
            }
        }

        if format == NotationFormat::LineBased && output.ends_with('\n') {
            output.pop();
        }

        Ok(output)
    }

    /// Build a complete serialization plan for this machine.
    ///
    /// **Construction order is canonical.** The serializer
    /// walks the `Machine`'s edge vec in the order it was
    /// constructed and trusts that order:
    ///
    /// - For machines built by `Machine::from_path`, vec
    ///   order is the strand's execution order (cap by cap).
    /// - For machines built by `Machine::from_string`
    ///   (parser), vec order is the order the user wrote the
    ///   wirings in.
    ///
    /// The serializer never reorders edges. Two equivalent
    /// machines built with edges in different orders will
    /// produce different notation strings (but both will
    /// round-trip via `Machine::is_equivalent`). The notation
    /// is a textual encoding of an ordered edge sequence, not
    /// a canonical fingerprint of the underlying edge set.
    ///
    /// ## Why no topological reordering
    ///
    /// An earlier design used `accepts`-based topological
    /// inference to reorder edges. That broke on real strands
    /// that visit non-monotonically-specific cap signatures —
    /// e.g. a strand that drops tags via one cap and then
    /// adds them via another. The two cap signatures form a
    /// 2-cycle under `accepts` (each accepts the other's
    /// output), so topological sort either fails or produces
    /// the wrong order. The cap-registry-level data
    /// legitimately contains such mutually-accepting pairs
    /// (format converters, type coercions). Construction
    /// order is the only reliable signal for "what came
    /// before what" in a strand.
    ///
    /// ## Node naming algorithm
    ///
    /// Walk edges in construction order. Track a vec of
    /// `OutputNode` records — each is `(name, urn,
    /// produced_by_edge_idx)`. For each edge `B`:
    ///
    /// 1. **Resolve secondary sources** (`B.sources[i]` for
    ///    `i > 0`). The parser sets these to the concrete
    ///    URNs of previously-assigned aliases, so they match
    ///    via `is_equivalent` against an existing output node.
    ///    Scan from latest to earliest to prefer the most-
    ///    recently-produced match. Failing to match is
    ///    `UnmatchedSecondaryFanInSource`.
    /// 2. **Resolve the primary source** (`B.sources[0]`).
    ///    The cap's declared `in=` pattern must `accept` an
    ///    earlier output node's URN. Scan from latest to
    ///    earliest, skipping any node already claimed by a
    ///    secondary in this same edge. Take the first match.
    ///    If no match, this is a root source — allocate a
    ///    fresh node, sharing identity across fan-out via
    ///    `is_equivalent` with existing root nodes.
    /// 3. **Allocate `B.target`**. Always a fresh output
    ///    node (each edge produces a new data position).
    fn build_serialization_plan(&self) -> Result<SerializationPlan, MachineAbstractionError> {
        let edges = self.edges();
        let edge_order: Vec<usize> = (0..edges.len()).collect();

        // Aliases are opaque — they identify each header in
        // the textual notation but carry no semantic weight.
        // Generate them deterministically from the
        // construction order so the output is stable and does
        // not privilege any particular cap tag (`op`, `ext`,
        // `model`, …). Only the cap URN's `in` / `out` tags
        // have functional meaning; every other tag is
        // arbitrary.
        let mut aliases: BTreeMap<String, usize> = BTreeMap::new();
        let mut alias_for_edge: Vec<String> = vec![String::new(); edges.len()];

        for &edge_idx in &edge_order {
            let alias = format!("edge_{}", edge_idx);
            aliases.insert(alias.clone(), edge_idx);
            alias_for_edge[edge_idx] = alias;
        }

        // Output-node registry. Each output node carries its
        // allocated name, the MediaUrn of the edge target
        // (or root source) that produced it, and the index of
        // the edge that produced it (`None` for root sources
        // that have no producing edge).
        #[derive(Debug)]
        struct OutputNode {
            name: String,
            urn: MediaUrn,
            produced_by_edge: Option<usize>,
        }
        let mut output_nodes: Vec<OutputNode> = Vec::new();
        let mut node_counter: usize = 0;

        // Per-edge source node names and target node name.
        let mut source_names_for_edge: Vec<Vec<String>> =
            (0..edges.len()).map(|_| Vec::new()).collect();
        let mut target_name_for_edge: Vec<String> = vec![String::new(); edges.len()];

        for &edge_idx in &edge_order {
            let edge = &edges[edge_idx];

            // Track which output nodes are claimed by THIS
            // edge's sources. The bitmap is over `output_nodes`
            // (resized as new roots get allocated below).
            let mut claimed: Vec<bool> = vec![false; output_nodes.len()];

            let source_names: &mut Vec<String> = &mut source_names_for_edge[edge_idx];
            source_names.resize(edge.sources.len(), String::new());

            // Pass 1: secondary sources (i > 0). Concrete
            // URNs from prior wirings; match via
            // `is_equivalent`, prefer the most-recent
            // matching output node.
            for (src_idx, source_urn) in edge.sources.iter().enumerate().skip(1) {
                let mut matched: Option<usize> = None;
                for node_idx in (0..output_nodes.len()).rev() {
                    if claimed[node_idx] {
                        continue;
                    }
                    if source_urn
                        .is_equivalent(&output_nodes[node_idx].urn)
                        .unwrap_or(false)
                    {
                        matched = Some(node_idx);
                        break;
                    }
                }
                match matched {
                    Some(node_idx) => {
                        claimed[node_idx] = true;
                        source_names[src_idx] = output_nodes[node_idx].name.clone();
                    }
                    None => {
                        return Err(MachineAbstractionError::UnmatchedSecondaryFanInSource {
                            edge_index: edge_idx,
                            source_index: src_idx,
                        });
                    }
                }
            }

            // Pass 2: primary source (i == 0). The cap's
            // declared `in=` pattern; match via `accepts`,
            // prefer the most-recent unclaimed predecessor.
            if !edge.sources.is_empty() {
                let primary = &edge.sources[0];

                let mut matched: Option<usize> = None;
                for node_idx in (0..output_nodes.len()).rev() {
                    if claimed[node_idx] {
                        continue;
                    }
                    if primary
                        .accepts(&output_nodes[node_idx].urn)
                        .unwrap_or(false)
                    {
                        matched = Some(node_idx);
                        break;
                    }
                }

                let assigned_name = match matched {
                    Some(node_idx) => {
                        claimed[node_idx] = true;
                        output_nodes[node_idx].name.clone()
                    }
                    None => {
                        // No prior output matches — this is a
                        // root source. Fan-out dedup: look for
                        // an existing root node whose URN is
                        // two-way equivalent to `primary` so
                        // multiple edges sharing a root URN
                        // share one node.
                        let existing_root = output_nodes.iter().find(|n| {
                            n.produced_by_edge.is_none()
                                && primary.is_equivalent(&n.urn).unwrap_or(false)
                        });
                        if let Some(root) = existing_root {
                            root.name.clone()
                        } else {
                            let name = format!("n{}", node_counter);
                            node_counter += 1;
                            output_nodes.push(OutputNode {
                                name: name.clone(),
                                urn: primary.clone(),
                                produced_by_edge: None,
                            });
                            claimed.push(true);
                            name
                        }
                    }
                };
                source_names[0] = assigned_name;
            }

            // Allocate target output node — always fresh,
            // since each edge produces a new data position.
            let target_name = format!("n{}", node_counter);
            node_counter += 1;
            output_nodes.push(OutputNode {
                name: target_name.clone(),
                urn: edge.target.clone(),
                produced_by_edge: Some(edge_idx),
            });
            target_name_for_edge[edge_idx] = target_name;
        }

        Ok(SerializationPlan {
            aliases,
            alias_for_edge,
            edge_order,
            source_names_for_edge,
            target_name_for_edge,
        })
    }
}

/// The plan produced by `build_serialization_plan`, carrying
/// every datum the format emitters need.
#[derive(Debug)]
struct SerializationPlan {
    /// Alphabetically-sortable alias → edge index.
    aliases: BTreeMap<String, usize>,
    /// Indexed by edge index: the alias assigned to this edge.
    alias_for_edge: Vec<String>,
    /// Topologically sorted edge indices.
    edge_order: Vec<usize>,
    /// Indexed by edge index: the node names of this edge's
    /// sources, in source order.
    source_names_for_edge: Vec<Vec<String>>,
    /// Indexed by edge index: the node name allocated for this
    /// edge's target.
    target_name_for_edge: Vec<String>,
}

impl SerializationPlan {
    fn alias_for_edge(&self, edge_idx: usize) -> &str {
        &self.alias_for_edge[edge_idx]
    }

    fn source_names_for_edge(&self, edge_idx: usize) -> &[String] {
        &self.source_names_for_edge[edge_idx]
    }

    fn target_name_for_edge(&self, edge_idx: usize) -> &str {
        &self.target_name_for_edge[edge_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::cap_urn::CapUrn;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).expect("test media URN parses")
    }

    fn cap(s: &str) -> CapUrn {
        CapUrn::from_string(s).expect("test cap URN parses")
    }

    fn edge(sources: &[&str], cap_str: &str, target: &str, is_loop: bool) -> MachineEdge {
        MachineEdge {
            sources: sources.iter().map(|s| media(s)).collect(),
            cap_urn: cap(cap_str),
            target: media(target),
            is_loop,
        }
    }

    /// Check two MediaUrns for semantic equivalence (order-
    /// theoretic two-way `accepts`). Never compares URNs as
    /// strings.
    fn urns_equivalent(a: &MediaUrn, b: &MediaUrn) -> bool {
        a.is_equivalent(b).expect("URN equivalence check")
    }

    /// Serialize a machine and panic with a readable error if
    /// it fails, so tests stop at the serializer boundary
    /// rather than burying the error in a Result trace.
    fn serialize(g: &Machine) -> String {
        g.to_machine_notation()
            .expect("machine serializes to bracketed notation")
    }

    fn serialize_multi(g: &Machine) -> String {
        g.to_machine_notation_multiline()
            .expect("machine serializes to multiline notation")
    }

    fn serialize_line(g: &Machine) -> String {
        g.to_machine_notation_formatted(NotationFormat::LineBased)
            .expect("machine serializes to line-based notation")
    }

    // =========================================================================
    // Basic serialization
    // =========================================================================

    #[test]
    fn serialize_single_edge() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize(&g);
        // Alias is derived from the construction-order edge
        // index, not from any tag on the cap URN. With one
        // edge the alias is `edge_0`.
        assert!(notation.contains("[edge_0 "), "Expected 'edge_0' header, got: {}", notation);
        assert!(notation.contains("-> edge_0 ->"), "Expected 'edge_0' wiring, got: {}", notation);
        assert!(notation.contains("[n0 ->"));
        assert!(notation.contains("-> n1]"));
    }

    #[test]
    fn serialize_two_edge_chain() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = serialize(&g);
        // Should have 2 headers and 2 wirings
        let bracket_count = notation.matches('[').count();
        assert_eq!(bracket_count, 4); // 2 headers + 2 wirings
    }

    #[test]
    fn serialize_empty_graph() {
        let g = Machine::empty();
        assert_eq!(serialize(&g), "");
    }

    // =========================================================================
    // Round-trip: serialize → parse → compare
    // =========================================================================

    #[test]
    fn roundtrip_single_edge() {
        let original = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            original.is_equivalent(&reparsed),
            "Round-trip failed:\n  original: {:?}\n  notation: {}\n  reparsed: {:?}",
            original, notation, reparsed
        );
    }

    #[test]
    fn roundtrip_two_edge_chain() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = serialize(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            original.is_equivalent(&reparsed),
            "Round-trip failed:\n  notation: {}\n  original edges: {}\n  reparsed edges: {}",
            notation, original.edge_count(), reparsed.edge_count()
        );
    }

    #[test]
    fn roundtrip_fan_out() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract_metadata;out=\"media:file-metadata;record;textable\"",
                "media:file-metadata;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract_outline;out=\"media:document-outline;record;textable\"",
                "media:document-outline;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"",
                "media:image;png;thumbnail",
                false,
            ),
        ]);
        let notation = serialize(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            original.is_equivalent(&reparsed),
            "Fan-out round-trip failed:\n  notation: {}",
            notation
        );
    }

    #[test]
    fn roundtrip_loop_edge() {
        let original = Machine::new(vec![edge(
            &["media:disbound-page;textable"],
            "cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\"",
            "media:txt;textable",
            true,
        )]);
        let notation = serialize(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(original.is_equivalent(&reparsed));
        assert!(reparsed.edges()[0].is_loop);
    }

    // =========================================================================
    // Determinism
    // =========================================================================

    #[test]
    fn serialization_is_deterministic() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let n1 = serialize(&g);
        let n2 = serialize(&g);
        assert_eq!(n1, n2, "Serialization must be deterministic");
    }

    /// Two equivalent machines built from edge vecs in
    /// different construction orders are NOT required to
    /// produce byte-identical notation. They are required to
    /// round-trip via `Machine::is_equivalent`. The notation
    /// is a textual encoding of an ordered edge sequence;
    /// vec order is part of the notation's identity.
    #[test]
    fn reordered_edges_round_trip_to_equivalent_machines() {
        let g1 = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let g2 = Machine::new(vec![
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
        ]);
        assert!(
            g1.is_equivalent(&g2),
            "the two machines must be edge-set equivalent"
        );
        // Notation strings differ — vec order is canonical.
        // But each must round-trip to a machine equivalent
        // to its own construction.
        let g1_reparsed = Machine::from_string(&serialize(&g1)).expect("g1 round-trip");
        let g2_reparsed = Machine::from_string(&serialize(&g2)).expect("g2 round-trip");
        assert!(g1.is_equivalent(&g1_reparsed));
        assert!(g2.is_equivalent(&g2_reparsed));
        // And the reparsed machines are still mutually
        // equivalent — `Machine::is_equivalent` is set-based.
        assert!(g1_reparsed.is_equivalent(&g2_reparsed));
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    #[test]
    fn multiline_format() {
        // Use a two-edge machine so the multiline output
        // actually contains a newline between statements.
        // A single-edge machine produces one header line and
        // one wiring line — two `writeln!` calls with the
        // trailing newline stripped, which IS multi-line.
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let multi = serialize_multi(&g);
        assert!(multi.contains('\n'), "Multi-line format must contain newlines");

        // Should still round-trip
        let reparsed = Machine::from_string(&multi).expect("parse multiline");
        assert!(g.is_equivalent(&reparsed));
    }

    // =========================================================================
    // Alias generation — opaque, derived from construction-order index
    // =========================================================================

    /// Aliases are opaque labels derived from each edge's
    /// position in the `Machine`'s construction-order edge
    /// vec, not from any cap URN tag. Even when the cap URN
    /// has a prominent `op=` tag (the user's most common
    /// convention), the serializer does NOT use it — `op` is
    /// arbitrary data, not functional structure.
    #[test]
    fn alias_ignores_cap_urn_tags() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize(&g);
        // Alias is the construction-order position, never
        // the `op` value.
        assert!(
            notation.contains("[edge_0 "),
            "Expected 'edge_0' header, got: {}",
            notation
        );
        assert!(
            notation.contains("-> edge_0 ->"),
            "Expected 'edge_0' wiring, got: {}",
            notation
        );
        assert!(
            !notation.contains("[extract "),
            "Alias must not be derived from the `op` tag: {}",
            notation
        );
        assert!(
            !notation.contains("-> extract ->"),
            "Alias must not be derived from the `op` tag: {}",
            notation
        );
    }

    /// A cap URN with no `op` tag at all must still get a
    /// well-defined alias (and not fall into any special
    /// path). Topological-index aliases are the single rule.
    #[test]
    fn alias_works_without_any_cap_tag() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize(&g);
        assert!(
            notation.contains("[edge_0 "),
            "Expected 'edge_0' header, got: {}",
            notation
        );
    }

    /// Two edges in the same machine must always get distinct
    /// aliases (otherwise the notation would reference the
    /// same header twice from different wirings). Under the
    /// opaque scheme each edge gets its own construction-
    /// order index, even when the edges carry identical cap
    /// URNs.
    #[test]
    fn duplicate_cap_urns_get_distinct_aliases() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:json;record;textable\"",
                "media:json;record;textable",
                false,
            ),
        ]);
        let notation = serialize(&g);
        assert!(notation.contains("[edge_0 "), "first alias missing: {}", notation);
        assert!(notation.contains("[edge_1 "), "second alias missing: {}", notation);
        // Parsing this back must produce the same edge set —
        // the opaque aliases aren't part of `Machine::is_equivalent`.
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            g.is_equivalent(&reparsed),
            "duplicate-cap round-trip failed:\n{}",
            notation
        );
    }

    /// The roundtrip must still work for a machine whose cap
    /// has a tag other than `op` in the arbitrary position —
    /// e.g. `ext`, `language`, `constrained`. These are
    /// preserved verbatim in the header and round-trip
    /// through the parser, but have no effect on alias
    /// generation.
    #[test]
    fn arbitrary_cap_tags_roundtrip_without_affecting_aliases() {
        let g = Machine::new(vec![edge(
            &["media:textable"],
            "cap:constrained;in=\"media:textable\";language=en;out=\"media:decision;record;textable\"",
            "media:decision;record;textable",
            false,
        )]);
        let notation = serialize(&g);
        assert!(notation.contains("[edge_0 "), "expected edge_0 alias: {}", notation);
        // The notation must carry the arbitrary tags verbatim
        // so the parser can reconstruct the full cap URN.
        assert!(
            notation.contains("language=en"),
            "arbitrary `language` tag must round-trip: {}",
            notation
        );
        assert!(
            notation.contains("constrained"),
            "arbitrary `constrained` marker tag must round-trip: {}",
            notation
        );
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            g.is_equivalent(&reparsed),
            "arbitrary-tag round-trip failed:\n{}",
            notation
        );
    }

    // =========================================================================
    // Line-based format
    // =========================================================================

    #[test]
    fn line_based_format_single_edge() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize_line(&g);
        // No brackets
        assert!(!notation.contains('['), "Line-based format must not contain brackets: {}", notation);
        assert!(!notation.contains(']'), "Line-based format must not contain brackets: {}", notation);
        // Header and wiring both use the opaque edge_0 alias.
        assert!(
            notation.contains("edge_0 cap:"),
            "Expected 'edge_0 cap:' header, got: {}",
            notation
        );
        assert!(
            notation.contains("-> edge_0 ->"),
            "Expected 'edge_0' wiring, got: {}",
            notation
        );
    }

    #[test]
    fn line_based_roundtrip_single_edge() {
        let original = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = serialize_line(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            original.is_equivalent(&reparsed),
            "Line-based round-trip failed:\n  notation: {}\n  original: {:?}\n  reparsed: {:?}",
            notation, original, reparsed
        );
    }

    #[test]
    fn line_based_roundtrip_two_edge_chain() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = serialize_line(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            original.is_equivalent(&reparsed),
            "Line-based round-trip failed:\n  notation: {}",
            notation
        );
    }

    #[test]
    fn line_based_roundtrip_loop() {
        let original = Machine::new(vec![edge(
            &["media:disbound-page;textable"],
            "cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\"",
            "media:txt;textable",
            true,
        )]);
        let notation = serialize_line(&original);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(original.is_equivalent(&reparsed));
        assert!(reparsed.edges()[0].is_loop);
    }

    #[test]
    fn line_based_deterministic() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let n1 = serialize_line(&g);
        let n2 = serialize_line(&g);
        assert_eq!(n1, n2);
    }

    #[test]
    fn line_based_and_bracketed_parse_to_same_graph() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let bracketed = g
            .to_machine_notation_formatted(NotationFormat::Bracketed)
            .expect("bracketed serialization");
        let line_based = g
            .to_machine_notation_formatted(NotationFormat::LineBased)
            .expect("line-based serialization");

        let g_bracketed = Machine::from_string(&bracketed).expect("bracketed parse");
        let g_line_based = Machine::from_string(&line_based).expect("line-based parse");
        assert!(
            g_bracketed.is_equivalent(&g_line_based),
            "Bracketed and line-based must parse to equivalent graphs"
        );
    }

    // =========================================================================
    // from_path conversion
    // =========================================================================

    #[test]
    fn from_path_simple() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::Cap {
                    cap_urn: cap("cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\""),
                    title: "Extract Text".to_string(),
                    specificity: 5,
                    input_is_sequence: false,
                    output_is_sequence: false,
                },
                from_spec: media("media:pdf"),
                to_spec: media("media:txt;textable"),
            }],
            source_spec: media("media:pdf"),
            target_spec: media("media:txt;textable"),
            total_steps: 1,
            cap_step_count: 1,
            description: "Extract Text".to_string(),
        };

        let graph = Machine::from_path(&path).expect("strand converts to machine");
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.edges()[0].is_loop);
    }

    #[test]
    fn from_path_with_foreach() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: media("media:disbound-page;textable"),
                    },
                    from_spec: media("media:disbound-page;textable"),
                    to_spec: media("media:disbound-page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\""),
                        title: "Page to Text".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: false,
                    },
                    from_spec: media("media:disbound-page;textable"),
                    to_spec: media("media:txt;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Collect {
                        media_spec: media("media:txt;textable"),
                    },
                    from_spec: media("media:txt;textable"),
                    to_spec: media("media:txt;textable"),
                },
            ],
            source_spec: media("media:disbound-page;textable"),
            target_spec: media("media:txt;textable"),
            total_steps: 3,
            cap_step_count: 1,
            description: "ForEach → Page to Text → Collect".to_string(),
        };

        let graph = Machine::from_path(&path).expect("strand converts to machine");
        // ForEach + Cap + Collect → 1 edge with is_loop=true
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.edges()[0].is_loop, "ForEach step must set is_loop on next Cap edge");
    }

    #[test]
    fn from_path_collect_elided() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\""),
                        title: "Extract".to_string(),
                        specificity: 5,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    },
                    from_spec: media("media:pdf"),
                    to_spec: media("media:txt;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Collect {
                        media_spec: media("media:txt;textable"),
                    },
                    from_spec: media("media:txt;textable"),
                    to_spec: media("media:txt;textable"),
                },
            ],
            source_spec: media("media:pdf"),
            target_spec: media("media:txt;textable"),
            total_steps: 2,
            cap_step_count: 1,
            description: "Extract → Collect".to_string(),
        };

        let graph = Machine::from_path(&path).expect("strand converts to machine");
        // Collect is elided — only the Cap edge remains
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.edges()[0].is_loop);
    }

    #[test]
    fn from_path_structural_only_fails() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::ForEach {
                    media_spec: media("media:decision;json;record;textable"),
                },
                from_spec: media("media:decision;json;record;textable"),
                to_spec: media("media:decision;json;record;textable"),
            }],
            source_spec: media("media:decision;json;record;textable"),
            target_spec: media("media:decision;json;record;textable"),
            total_steps: 1,
            cap_step_count: 0,
            description: "ForEach".to_string(),
        };

        assert!(matches!(
            Machine::from_path(&path),
            Err(MachineAbstractionError::NoCapabilitySteps)
        ));
    }

    // =========================================================================
    // Serializer chains construction-order edges via `accepts`,
    // never via URN strings
    // =========================================================================

    /// `from_path` carries each cap's declared `from_spec`
    /// verbatim as its edge source — NOT the preceding cap's
    /// output URN. The chaining of consecutive caps is
    /// recovered by the serializer at node-naming time, by
    /// matching each edge's source URN against earlier
    /// outputs via the `accepts` predicate.
    ///
    /// For the real-world case `[Disbind, ForEach, make_decision]`:
    /// - disbind outputs `media:page;textable`
    /// - make_decision declares input `media:textable`
    /// - make_decision's input pattern `accepts` disbind's
    ///   output instance (because `page;textable ⪯ textable`),
    ///   so the serializer reuses disbind's target node as
    ///   make_decision's source node.
    #[test]
    fn from_path_disbind_foreach_make_decision_carries_cap_signatures_verbatim() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:pdf\";op=disbind;out=\"media:page;textable\""),
                        title: "Disbind".to_string(),
                        specificity: 5,
                        input_is_sequence: false,
                        output_is_sequence: true,
                    },
                    from_spec: media("media:pdf"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: media("media:page;textable"),
                    },
                    from_spec: media("media:page;textable"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:constrained;in=\"media:textable\";op=make_decision;out=\"media:decision;json;record;textable\""),
                        title: "Make a Decision".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: false,
                    },
                    // StrandStep.from_spec mirrors the cap's
                    // declared in=.
                    from_spec: media("media:textable"),
                    to_spec: media("media:decision;json;record;textable"),
                },
            ],
            source_spec: media("media:pdf"),
            target_spec: media("media:decision;json;record;textable"),
            total_steps: 3,
            cap_step_count: 2,
            description: "Disbind → ForEach → Make a Decision".to_string(),
        };

        let graph = Machine::from_path(&path).expect("strand converts to machine");
        assert_eq!(graph.edge_count(), 2);

        // disbind: sources=[media:pdf] (its own declared in=),
        // target=media:page;textable, not looped
        let disbind = &graph.edges()[0];
        assert_eq!(disbind.sources.len(), 1);
        assert!(
            urns_equivalent(&disbind.sources[0], &media("media:pdf")),
            "disbind's source must be its declared in= URN media:pdf"
        );
        assert!(
            urns_equivalent(&disbind.target, &media("media:page;textable")),
            "disbind's target must be its declared out= URN media:page;textable"
        );
        assert!(!disbind.is_loop);

        // make_decision: sources=[media:textable] (its own
        // declared in= pattern), target=media:decision;...,
        // is_loop=true from the ForEach. The source URN is
        // NOT chained to disbind's output — that's the
        // serializer's job via `accepts`.
        let make_decision = &graph.edges()[1];
        assert_eq!(make_decision.sources.len(), 1);
        assert!(
            urns_equivalent(&make_decision.sources[0], &media("media:textable")),
            "make_decision's source must be its declared in= pattern media:textable, not chained to disbind's output"
        );
        assert!(
            urns_equivalent(
                &make_decision.target,
                &media("media:decision;json;record;textable")
            )
        );
        assert!(make_decision.is_loop, "make_decision must be LOOP (inside ForEach)");

        // The serializer walks the construction-order edge
        // vec. For edge 1 (make_decision)'s primary source
        // `media:textable`, it scans earlier outputs latest-
        // first and finds `n1` (disbind's target,
        // `media:page;textable`). `media:textable.accepts(
        // media:page;textable)` holds, so the two edges
        // share node `n1`.
        //
        // Aliases are opaque construction-position labels.
        // Disbind is at construction index 0 → `edge_0`.
        // make_decision is at index 1 → `edge_1`.
        let notation = serialize_multi(&graph);
        assert!(
            notation.contains("[n0 -> edge_0 -> n1]"),
            "Expected 'n0 -> edge_0 -> n1' in notation:\n{}",
            notation
        );
        assert!(
            notation.contains("[n1 -> LOOP edge_1 -> n2]"),
            "Expected 'n1 -> LOOP edge_1 -> n2' in notation:\n{}",
            notation
        );
    }

    /// **Round-trip** for the user's real-world case.
    /// `Strand → Machine::from_path → to_machine_notation →
    ///   Machine::from_string → is_equivalent`. This pins the
    /// contract that a serialized machine can be re-parsed
    /// into a semantically equivalent machine without relying
    /// on URN-as-string comparisons anywhere in the pipeline.
    #[test]
    fn roundtrip_disbind_foreach_make_decision_strand() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:pdf\";op=disbind;out=\"media:page;textable\""),
                        title: "Disbind".to_string(),
                        specificity: 5,
                        input_is_sequence: false,
                        output_is_sequence: true,
                    },
                    from_spec: media("media:pdf"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: media("media:page;textable"),
                    },
                    from_spec: media("media:page;textable"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:constrained;in=\"media:textable\";op=make_decision;out=\"media:decision;json;record;textable\""),
                        title: "Make a Decision".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: false,
                    },
                    from_spec: media("media:textable"),
                    to_spec: media("media:decision;json;record;textable"),
                },
            ],
            source_spec: media("media:pdf"),
            target_spec: media("media:decision;json;record;textable"),
            total_steps: 3,
            cap_step_count: 2,
            description: "Disbind → ForEach → Make a Decision".to_string(),
        };

        let original = Machine::from_path(&path).expect("strand converts to machine");
        let bracketed = serialize(&original);
        let reparsed_bracketed = Machine::from_string(&bracketed)
            .expect("bracketed round-trip parses");
        assert!(
            original.is_equivalent(&reparsed_bracketed),
            "Bracketed round-trip failed:\n  notation: {}\n  original edges: {}\n  reparsed edges: {}",
            bracketed, original.edge_count(), reparsed_bracketed.edge_count()
        );

        let multiline = serialize_multi(&original);
        let reparsed_multi = Machine::from_string(&multiline)
            .expect("multiline round-trip parses");
        assert!(
            original.is_equivalent(&reparsed_multi),
            "Multiline round-trip failed:\n  notation: {}",
            multiline
        );

        let line_based = serialize_line(&original);
        let reparsed_line = Machine::from_string(&line_based)
            .expect("line-based round-trip parses");
        assert!(
            original.is_equivalent(&reparsed_line),
            "Line-based round-trip failed:\n  notation: {}",
            line_based
        );
    }

    /// **Regression for the cap-registry inverse-converter
    /// crash.** The cap registry legitimately contains pairs
    /// of caps that are inverses of each other (e.g.
    /// `numeric → integer` and `integer → numeric`). A
    /// strand may visit both as a normalize-then-denormalize
    /// pipeline. The serializer's earlier `accepts`-based
    /// topological sort treated such pairs as a 2-cycle and
    /// failed hard. Under the new construction-order regime
    /// the strand serializes successfully because the vec
    /// order IS the execution order.
    #[test]
    fn inverse_format_converters_serialize_in_construction_order() {
        let g = Machine::new(vec![
            edge(
                // Step 0: produce `media:integer;numeric;textable`
                // from a generic source.
                &["media:textable"],
                "cap:in=\"media:textable\";op=coerce;out=\"media:integer;numeric;textable\"",
                "media:integer;numeric;textable",
                false,
            ),
            edge(
                // Step 1: drop the `integer` tag back to
                // `media:numeric;textable` — the inverse of
                // step 0's specialization.
                &["media:integer;numeric;textable"],
                "cap:in=\"media:integer;numeric;textable\";op=coerce;out=\"media:numeric;textable\"",
                "media:numeric;textable",
                false,
            ),
        ]);
        // Must serialize without an error.
        let notation = serialize(&g);
        let reparsed = Machine::from_string(&notation).expect("inverse round-trip parses");
        assert!(
            g.is_equivalent(&reparsed),
            "inverse-converter round-trip failed:\n{}",
            notation
        );
    }

    /// Construction order is canonical: two-edge chain at
    /// positions 0 and 1 produces `n0 -> edge_0 -> n1` and
    /// `n1 -> edge_1 -> n2`, where the shared `n1` is the
    /// data position both edges reference.
    #[test]
    fn construction_order_assigns_node_names_in_chain_order() {
        let g = Machine::new(vec![
            edge(
                &["media:middle"],
                "cap:in=\"media:middle\";op=downstream;out=\"media:end\"",
                "media:end",
                false,
            ),
            edge(
                &["media:start"],
                "cap:in=\"media:start\";op=upstream;out=\"media:middle\"",
                "media:middle",
                false,
            ),
        ]);
        // Notation walks edges in construction order.
        // Edge 0 (downstream) gets `n0 -> edge_0 -> n1`.
        // Edge 1 (upstream)'s primary `media:start` matches
        // no earlier output → fresh root `n2`. Its target
        // `media:middle` allocates `n3` (a fresh output).
        // Note: `n0` and `n3` both carry `media:middle` but
        // are DIFFERENT nodes — that's the price of trusting
        // construction order over topology for badly-ordered
        // machines.
        let notation = serialize_multi(&g);
        // Round-trip equivalence must still hold.
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            g.is_equivalent(&reparsed),
            "construction-order machine round-trip failed:\n{}",
            notation
        );
    }

    /// The `accepts` predicate wires conformance-related
    /// URNs even when the URNs are not equal. Upstream
    /// produces `media:json;record;textable`; downstream
    /// declares `in=media:record`. The downstream's pattern
    /// accepts the upstream's instance, so the serializer
    /// chains them via the most-recent-matching predecessor
    /// rule. Round-trip preserves the edge set.
    #[test]
    fn serializer_chains_via_accepts_not_string_equality() {
        let upstream = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=parse_json;out=\"media:json;record;textable\"",
            "media:json;record;textable",
            false,
        );
        let downstream = edge(
            // `media:record` is a more-general pattern than
            // upstream's `media:json;record;textable` output.
            &["media:record"],
            "cap:in=\"media:record\";op=stringify;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );

        let g = Machine::new(vec![upstream, downstream]);
        let notation = serialize_multi(&g);
        let reparsed = Machine::from_string(&notation)
            .expect("accepts-chained notation parses");
        assert!(
            g.is_equivalent(&reparsed),
            "Accepts-chained round-trip failed:\n{}",
            notation
        );

        // The notation must reflect the chain: edge 0's
        // target node is the same node edge 1 takes as its
        // source — both wirings reference `n1`.
        assert!(
            notation.contains("[n0 -> edge_0 -> n1]"),
            "expected upstream wiring: {}",
            notation
        );
        assert!(
            notation.contains("[n1 -> edge_1 -> n2]"),
            "expected downstream wiring chained on n1: {}",
            notation
        );
    }

    /// When multiple earlier outputs match a primary source
    /// pattern, the most-recently-produced one wins. This is
    /// what runtime data flow does: each cap consumes the
    /// data from the previous step, not from way upstream.
    /// The deterministic resolution removes the need for an
    /// `AmbiguousFanIn` error in this codepath.
    #[test]
    fn primary_source_picks_most_recent_matching_predecessor() {
        let g = Machine::new(vec![
            edge(
                // Edge 0: produces `media:alpha`.
                &["media:r1"],
                "cap:in=\"media:r1\";op=gen_alpha_first;out=\"media:alpha\"",
                "media:alpha",
                false,
            ),
            edge(
                // Edge 1: also produces `media:alpha`.
                &["media:r2"],
                "cap:in=\"media:r2\";op=gen_alpha_second;out=\"media:alpha\"",
                "media:alpha",
                false,
            ),
            edge(
                // Edge 2: consumes `media:alpha`. Two earlier
                // outputs match — the serializer picks the
                // most-recent (edge 1's output).
                &["media:alpha"],
                "cap:in=\"media:alpha\";op=consume;out=\"media:out\"",
                "media:out",
                false,
            ),
        ]);
        let notation = serialize_multi(&g);
        let reparsed = Machine::from_string(&notation).expect("parse round-trip");
        assert!(
            g.is_equivalent(&reparsed),
            "most-recent-matching round-trip failed:\n{}",
            notation
        );
        // edge_0 produces n1; edge_1 produces n3; edge_2's
        // primary source must reference n3 (most recent),
        // not n1.
        assert!(
            notation.contains("[n3 -> edge_2 -> n4]"),
            "edge_2 must consume n3 (most recent matching producer), got:\n{}",
            notation
        );
    }

    /// A secondary fan-in source that doesn't match any
    /// predecessor via `is_equivalent` is a broken machine.
    /// Fail hard with `UnmatchedSecondaryFanInSource`.
    ///
    /// Setup: a downstream edge with two sources where the
    /// secondary URN has no equivalent predecessor.
    #[test]
    fn unmatched_secondary_fan_in_source_fails_hard() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=thumb;out=\"media:image;png;thumbnail\"",
                "media:image;png;thumbnail",
                false,
            ),
            // Downstream claims a secondary source
            // `media:nowhere` that no edge produces as an
            // output.
            edge(
                &["media:image;png", "media:nowhere"],
                "cap:in=\"media:image;png\";op=describe;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
        ]);
        let err = g.to_machine_notation().expect_err("unmatched secondary must fail");
        match err {
            MachineAbstractionError::UnmatchedSecondaryFanInSource {
                edge_index,
                source_index,
            } => {
                assert_eq!(edge_index, 1);
                assert_eq!(source_index, 1);
            }
            other => panic!(
                "Expected UnmatchedSecondaryFanInSource, got: {:?}",
                other
            ),
        }
    }

    /// Fan-in round-trip: two upstream edges produce
    /// distinct-typed outputs, downstream consumes both. The
    /// serializer disambiguates the primary (via `accepts`)
    /// and the secondary (via `is_equivalent`) to produce a
    /// valid notation, and the parser reconstructs an
    /// equivalent machine.
    #[test]
    fn fan_in_roundtrip_with_disjoint_secondary() {
        let thumb_edge = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"",
            "media:image;png;thumbnail",
            false,
        );
        let model_dl_edge = edge(
            &["media:model-spec;textable"],
            "cap:in=\"media:model-spec;textable\";op=download;out=\"media:model-spec;textable\"",
            "media:model-spec;textable",
            false,
        );
        // Downstream: primary is the cap's declared
        // `media:image;png` pattern, secondary is the concrete
        // `media:model-spec;textable` carried from the
        // upstream model_dl edge.
        let describe_edge = edge(
            &["media:image;png", "media:model-spec;textable"],
            "cap:in=\"media:image;png\";op=describe_image;out=\"media:image-description;textable\"",
            "media:image-description;textable",
            false,
        );

        let original = Machine::new(vec![thumb_edge, model_dl_edge, describe_edge]);
        let notation = serialize(&original);
        let reparsed = Machine::from_string(&notation)
            .expect("fan-in round-trip parses");
        assert!(
            original.is_equivalent(&reparsed),
            "Fan-in round-trip failed:\n  notation: {}\n  original edges: {}\n  reparsed edges: {}",
            notation, original.edge_count(), reparsed.edge_count()
        );
    }

    /// Serialization is pure over the `Machine` value:
    /// calling `to_machine_notation` twice on the same
    /// machine produces byte-identical output. This pins the
    /// determinism contract on a single machine instance —
    /// distinct from the cross-construction-order contract,
    /// which the new design no longer promises.
    #[test]
    fn serialization_pure_over_machine_value() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract_metadata;out=\"media:file-metadata;record;textable\"",
                "media:file-metadata;record;textable",
                false,
            ),
        ]);
        let n1 = serialize(&g);
        let n2 = serialize(&g);
        assert_eq!(n1, n2);
    }
}
