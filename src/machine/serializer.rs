//! Machine notation serializer — canonical text encoding of
//! a `Machine`.
//!
//! A `Machine` is a `Vec<MachineStrand>` (see `graph.rs`). The
//! serializer walks the strands in declaration order and emits
//! one notation document covering all of them. Two
//! strictly-equivalent `Machine`s produce byte-identical
//! notation, because both the canonical edge order within each
//! strand and the global alias / node-name allocation are
//! deterministic functions of the resolved DAG structure.
//!
//! ## Layout
//!
//! ```text
//! [<global alias 0> <cap-urn 0>]
//! [<global alias 1> <cap-urn 1>]
//! ...
//! [<source nodes> -> [LOOP] <global alias 0> -> <target node>]
//! [<source nodes> -> [LOOP] <global alias 1> -> <target node>]
//! ...
//! ```
//!
//! All headers come first, then all wirings. Both sections
//! traverse strands in `Machine::strands()` order, and within
//! each strand the resolved canonical edge order from
//! `MachineStrand::edges()`.
//!
//! ## Aliases and node names
//!
//! Aliases and node names are opaque labels — see
//! `09-MACHINE-NOTATION.md` §4 for the rationale. The
//! serializer generates them as:
//!
//! - `edge_<global_index>` for each cap edge in the order it
//!   appears in the global walk (across all strands).
//! - `n<global_index>` for each `NodeId` allocated as the walk
//!   visits new data positions.
//!
//! Strand boundaries are unmarked in the notation. The parser
//! recovers them via connected-components analysis on shared
//! node names — and because the serializer assigns each strand
//! a fresh disjoint range of node names, the parser's
//! connected-components partition matches the serializer's
//! strand list exactly. Round-trip preserves both strand order
//! and intra-strand canonical edge order.
//!
//! ## Failure modes
//!
//! Serialization is infallible for any `Machine` that was built
//! through one of the legitimate constructors (`from_strand`,
//! `from_strands`, `from_string`). The `Machine`'s internal
//! invariants (every `NodeId` referenced is in range, every
//! resolved edge points at valid nodes) are established at
//! construction time and cannot be violated by the serializer.

use std::fmt::Write;

use super::error::MachineAbstractionError;
use super::graph::{Machine, MachineEdge, MachineStrand};

/// Serialization format for machine notation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationFormat {
    /// Bracketed: each statement wrapped in `[...]`. The
    /// canonical, single-line form used as a stable
    /// identifier. The default for `Machine::to_machine_notation`.
    Bracketed,
    /// Line-based: one statement per line, no brackets. Used
    /// for human-readable / human-editable display.
    LineBased,
}

impl Machine {
    /// Serialize this machine to canonical bracketed machine
    /// notation. Two strictly-equivalent machines produce
    /// byte-identical output.
    pub fn to_machine_notation(&self) -> Result<String, MachineAbstractionError> {
        self.to_machine_notation_formatted(NotationFormat::Bracketed)
    }

    /// Serialize to multi-line bracketed machine notation —
    /// one statement per line, each wrapped in `[...]`.
    /// Functionally equivalent to the canonical bracketed form
    /// but with newlines between statements for readability.
    pub fn to_machine_notation_multiline(&self) -> Result<String, MachineAbstractionError> {
        let plan = build_serialization_plan(self);
        emit_multiline(self, &plan)
    }

    /// Serialize this machine to machine notation in the
    /// specified format. Two strictly-equivalent machines
    /// produce byte-identical output for a given format.
    pub fn to_machine_notation_formatted(
        &self,
        format: NotationFormat,
    ) -> Result<String, MachineAbstractionError> {
        if self.is_empty() {
            return Ok(String::new());
        }
        let plan = build_serialization_plan(self);
        match format {
            NotationFormat::Bracketed => emit_bracketed(self, &plan),
            NotationFormat::LineBased => emit_line_based(self, &plan),
        }
    }
}

/// Per-machine serialization plan: per-strand alias and node-
/// name allocations, all keyed by global indices.
struct SerializationPlan {
    /// One entry per strand. Each strand contributes its own
    /// edge alias names and node names from the global
    /// counters.
    strands: Vec<StrandPlan>,
}

struct StrandPlan {
    /// Alias for each edge in `MachineStrand::edges()`, in the
    /// strand's canonical edge order. Indexed by edge index
    /// within the strand.
    edge_aliases: Vec<String>,
    /// Node name for each `NodeId` in the strand. Indexed by
    /// `NodeId as usize`.
    node_names: Vec<String>,
}

fn build_serialization_plan(machine: &Machine) -> SerializationPlan {
    let mut strand_plans: Vec<StrandPlan> = Vec::with_capacity(machine.strand_count());
    let mut next_alias: usize = 0;
    let mut next_node: usize = 0;

    for strand in machine.strands() {
        let mut edge_aliases: Vec<String> = Vec::with_capacity(strand.edges().len());
        for _ in strand.edges() {
            edge_aliases.push(format!("edge_{}", next_alias));
            next_alias += 1;
        }
        let mut node_names: Vec<String> = Vec::with_capacity(strand.nodes().len());
        for _ in strand.nodes() {
            node_names.push(format!("n{}", next_node));
            next_node += 1;
        }
        strand_plans.push(StrandPlan {
            edge_aliases,
            node_names,
        });
    }

    SerializationPlan {
        strands: strand_plans,
    }
}

/// Emit one wiring statement (without enclosing brackets or
/// trailing newline) for a single edge inside a strand.
fn format_wiring(
    edge: &MachineEdge,
    alias: &str,
    strand_plan: &StrandPlan,
) -> String {
    // Sources, in the canonical (cap-arg-sorted) assignment
    // order. The serializer surfaces this canonical form so
    // round-trip is byte-stable.
    let source_names: Vec<&String> = edge
        .assignment
        .iter()
        .map(|b| &strand_plan.node_names[b.source as usize])
        .collect();
    let target_name = &strand_plan.node_names[edge.target as usize];
    let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

    if source_names.len() == 1 {
        format!(
            "{} -> {}{} -> {}",
            source_names[0], loop_prefix, alias, target_name
        )
    } else {
        let group: Vec<&str> = source_names.iter().map(|s| s.as_str()).collect();
        format!(
            "({}) -> {}{} -> {}",
            group.join(", "),
            loop_prefix,
            alias,
            target_name
        )
    }
}

fn emit_bracketed(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut output = String::new();

    // Headers across all strands.
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            write!(
                output,
                "[{} {}]",
                strand_plan.edge_aliases[edge_idx],
                edge.cap_urn
            )
            .unwrap();
        }
    }

    // Wirings across all strands.
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            let wiring = format_wiring(
                edge,
                &strand_plan.edge_aliases[edge_idx],
                strand_plan,
            );
            write!(output, "[{}]", wiring).unwrap();
        }
    }

    Ok(output)
}

fn emit_line_based(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut lines: Vec<String> = Vec::new();

    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            lines.push(format!(
                "{} {}",
                strand_plan.edge_aliases[edge_idx], edge.cap_urn
            ));
        }
    }
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            lines.push(format_wiring(
                edge,
                &strand_plan.edge_aliases[edge_idx],
                strand_plan,
            ));
        }
    }

    Ok(lines.join("\n"))
}

fn emit_multiline(
    machine: &Machine,
    plan: &SerializationPlan,
) -> Result<String, MachineAbstractionError> {
    let mut lines: Vec<String> = Vec::new();

    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            lines.push(format!(
                "[{} {}]",
                strand_plan.edge_aliases[edge_idx], edge.cap_urn
            ));
        }
    }
    for (strand, strand_plan) in machine.strands().iter().zip(plan.strands.iter()) {
        for (edge_idx, edge) in strand.edges().iter().enumerate() {
            let wiring = format_wiring(
                edge,
                &strand_plan.edge_aliases[edge_idx],
                strand_plan,
            );
            lines.push(format!("[{}]", wiring));
        }
    }

    Ok(lines.join("\n"))
}

// =============================================================================
// Render-payload JSON for the JS renderer
// =============================================================================
//
// The Swift / JS visualization layer no longer reads
// `Machine.abstract_strand` (which has been deleted). Instead
// the gRPC layer ships the canonical machine notation as the
// machine's identity AND a render-payload JSON computed by the
// Rust side, which the JS renderer consumes directly.
//
// The render payload is a list of strands, each with its
// nodes, edges, and anchor sets. The JS renderer iterates the
// strands and draws each as a sub-graph.

impl Machine {
    /// Build the JSON payload the JS strand-graph renderer
    /// consumes. Shape (top-level array of strands):
    ///
    /// ```json
    /// {
    ///   "strands": [
    ///     {
    ///       "nodes": [{"id": "n0", "urn": "media:pdf"}, ...],
    ///       "edges": [
    ///         {
    ///           "alias": "edge_0",
    ///           "cap_urn": "cap:in=...;...;out=...",
    ///           "is_loop": false,
    ///           "assignment": [
    ///             {
    ///               "cap_arg_media_urn": "media:pdf",
    ///               "source_node": "n0"
    ///             }
    ///           ],
    ///           "target_node": "n1"
    ///         },
    ///         ...
    ///       ],
    ///       "input_anchor_nodes": ["n0"],
    ///       "output_anchor_nodes": ["n1"]
    ///     },
    ///     ...
    ///   ]
    /// }
    /// ```
    ///
    /// Node names use the same global counter as the canonical
    /// notation, so a notation string and its render payload
    /// share the same node identities.
    pub fn to_render_payload_json(&self) -> Result<String, MachineAbstractionError> {
        if self.is_empty() {
            return Ok("{\"strands\":[]}".to_string());
        }
        let plan = build_serialization_plan(self);
        let mut json = String::new();
        write!(json, "{{\"strands\":[").unwrap();
        for (s_idx, (strand, strand_plan)) in
            self.strands().iter().zip(plan.strands.iter()).enumerate()
        {
            if s_idx > 0 {
                json.push(',');
            }
            emit_strand_json(&mut json, strand, strand_plan);
        }
        write!(json, "]}}").unwrap();
        Ok(json)
    }
}

fn emit_strand_json(json: &mut String, strand: &MachineStrand, plan: &StrandPlan) {
    write!(json, "{{").unwrap();

    // nodes
    write!(json, "\"nodes\":[").unwrap();
    for (id, urn) in strand.nodes().iter().enumerate() {
        if id > 0 {
            json.push(',');
        }
        write!(
            json,
            "{{\"id\":\"{}\",\"urn\":\"{}\"}}",
            plan.node_names[id],
            json_escape(&urn.to_string())
        )
        .unwrap();
    }
    write!(json, "],").unwrap();

    // edges
    write!(json, "\"edges\":[").unwrap();
    for (e_idx, edge) in strand.edges().iter().enumerate() {
        if e_idx > 0 {
            json.push(',');
        }
        write!(
            json,
            "{{\"alias\":\"{}\",\"cap_urn\":\"{}\",\"is_loop\":{},\"assignment\":[",
            plan.edge_aliases[e_idx],
            json_escape(&edge.cap_urn.to_string()),
            edge.is_loop
        )
        .unwrap();
        for (b_idx, b) in edge.assignment.iter().enumerate() {
            if b_idx > 0 {
                json.push(',');
            }
            write!(
                json,
                "{{\"cap_arg_media_urn\":\"{}\",\"source_node\":\"{}\"}}",
                json_escape(&b.cap_arg_media_urn.to_string()),
                plan.node_names[b.source as usize]
            )
            .unwrap();
        }
        write!(
            json,
            "],\"target_node\":\"{}\"}}",
            plan.node_names[edge.target as usize]
        )
        .unwrap();
    }
    write!(json, "],").unwrap();

    // input_anchor_nodes
    write!(json, "\"input_anchor_nodes\":[").unwrap();
    for (i, id) in strand.input_anchor_ids().iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        write!(json, "\"{}\"", plan.node_names[*id as usize]).unwrap();
    }
    write!(json, "],").unwrap();

    // output_anchor_nodes
    write!(json, "\"output_anchor_nodes\":[").unwrap();
    for (i, id) in strand.output_anchor_ids().iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        write!(json, "\"{}\"", plan.node_names[*id as usize]).unwrap();
    }
    write!(json, "]").unwrap();

    write!(json, "}}").unwrap();
}

/// Minimal JSON string-escape: only `\` and `"` need escaping
/// here because `MediaUrn::to_string()` and `CapUrn::to_string()`
/// produce ASCII-safe canonical text, and the only metacharacters
/// that can appear are quoted attribute values (which use `"`).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::NotationFormat;
    use crate::machine::graph::Machine;
    use crate::machine::test_fixtures::{
        build_cap, cap_step, registry_with, strand_from_steps,
    };

    fn extract_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:in=media:pdf;op=extract;out=\"media:txt;textable\"",
            "extract",
            &["media:pdf"],
            "media:txt;textable",
        )
    }

    fn embed_cap_def() -> crate::cap::definition::Cap {
        build_cap(
            "cap:in=media:textable;op=embed;out=\"media:vec;record\"",
            "embed",
            &["media:textable"],
            "media:vec;record",
        )
    }

    fn pdf_to_vec_strand() -> crate::planner::Strand {
        strand_from_steps(
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
        )
    }

    #[test]
    fn serialize_two_step_strand_emits_global_aliases_and_node_names() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let machine = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let notation = machine.to_machine_notation().unwrap();
        // Two header brackets, two wiring brackets — `edge_0`
        // and `edge_1` from the global alias counter, `n0..n2`
        // from the global node counter.
        assert!(
            notation.contains("[edge_0 cap:") && notation.contains("[edge_1 cap:"),
            "headers must use edge_0 / edge_1 aliases, got: {notation}"
        );
        assert!(
            notation.contains("[n0 -> edge_0 -> n1]"),
            "first wiring should be `n0 -> edge_0 -> n1`, got: {notation}"
        );
        assert!(
            notation.contains("[n1 -> edge_1 -> n2]"),
            "second wiring should be `n1 -> edge_1 -> n2`, got: {notation}"
        );
    }

    #[test]
    fn serialize_then_parse_round_trip_preserves_strict_equivalence() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let m1 = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let notation = m1.to_machine_notation().unwrap();
        let m2 = Machine::from_string(&notation, &registry)
            .expect("re-parse must succeed");
        assert!(
            m1.is_equivalent(&m2),
            "machine and its parse-reserialize must be strictly equivalent"
        );
        // And the second-pass notation must be byte-identical
        // to the first — canonical form.
        let notation2 = m2.to_machine_notation().unwrap();
        assert_eq!(
            notation, notation2,
            "canonical notation is a fixed point of parse-then-serialize"
        );
    }

    #[test]
    fn line_based_format_round_trips_to_same_machine() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let m1 = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let line_based = m1
            .to_machine_notation_formatted(NotationFormat::LineBased)
            .unwrap();
        // Should not contain `[` brackets — line-based form
        // is one statement per line, no enclosing brackets.
        assert!(
            !line_based.contains('['),
            "line-based form must not contain brackets, got: {line_based}"
        );
        let m2 = Machine::from_string(&line_based, &registry)
            .expect("line-based form must parse");
        assert!(m1.is_equivalent(&m2));
    }

    #[test]
    fn empty_machine_serializes_to_empty_string() {
        let machine = Machine::from_resolved_strands(vec![]);
        let notation = machine.to_machine_notation().unwrap();
        assert!(notation.is_empty());
    }

    #[test]
    fn render_payload_json_includes_strand_with_anchors() {
        let registry = registry_with(vec![extract_cap_def(), embed_cap_def()]);
        let machine = Machine::from_strand(&pdf_to_vec_strand(), &registry).unwrap();
        let payload = machine.to_render_payload_json().unwrap();
        // Should have a `strands` array, containing one strand
        // with `nodes`, `edges`, `input_anchor_nodes`,
        // `output_anchor_nodes`.
        assert!(payload.starts_with("{\"strands\":["));
        assert!(payload.contains("\"nodes\":["));
        assert!(payload.contains("\"edges\":["));
        assert!(payload.contains("\"input_anchor_nodes\":["));
        assert!(payload.contains("\"output_anchor_nodes\":["));
        // The two cap URNs should appear in the payload as
        // edge.cap_urn entries.
        assert!(payload.contains("op=extract"));
        assert!(payload.contains("op=embed"));
    }

    #[test]
    fn render_payload_for_empty_machine_has_empty_strands_array() {
        let machine = Machine::from_resolved_strands(vec![]);
        let payload = machine.to_render_payload_json().unwrap();
        assert_eq!(payload, "{\"strands\":[]}");
    }
}
