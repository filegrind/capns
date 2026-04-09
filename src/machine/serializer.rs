//! Machine notation serializer — deterministic canonical form
//!
//! Converts a `Machine` to its machine notation string representation.
//! The output is deterministic: the same graph always produces the same string.
//!
//! The canonical form is line-based (one statement per line, no brackets).
//!
//! ## Alias Generation
//!
//! Aliases are derived from the cap URN's `op=` tag value. If no `op=` tag
//! exists, aliases are generated as `edge_0`, `edge_1`, etc. Duplicate
//! aliases from identical op tags are disambiguated with numeric suffixes.
//!
//! ## Node Name Generation
//!
//! Node names are generated deterministically from topological position.
//! The first root source is `n0`, etc. Intermediate nodes get names based
//! on their topological order.
//!
//! ## Canonical Ordering
//!
//! Edges are sorted by (cap_urn canonical string, sources canonical, target canonical)
//! for stable output. Headers are emitted first (sorted by alias), then wirings
//! in the same edge order.

use std::collections::{BTreeMap, HashMap};
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
    /// - Each `Cap` step becomes a `MachineEdge` with a single source
    /// - `ForEach` steps set `is_loop: true` on the next Cap edge
    /// - `Collect` steps are elided (implicit in transitions)
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
    pub fn to_machine_notation(&self) -> String {
        self.to_machine_notation_formatted(NotationFormat::Bracketed)
    }

    /// Serialize to multi-line machine notation (one statement per line).
    /// Uses the same format as `to_machine_notation()` (bracketed) but
    /// with newlines between statements.
    pub fn to_machine_notation_multiline(&self) -> String {
        if self.edges().is_empty() {
            return String::new();
        }

        let (aliases, node_names, edge_order) = self.build_serialization_maps();
        let mut output = String::new();

        let mut sorted_aliases: Vec<(&String, &(usize, String))> = aliases.iter().collect();
        sorted_aliases.sort_by_key(|(alias, _)| *alias);

        for (alias, (edge_idx, _)) in &sorted_aliases {
            let edge = &self.edges()[*edge_idx];
            writeln!(output, "[{} {}]", alias, edge.cap_urn).unwrap();
        }

        for edge_idx in &edge_order {
            let edge = &self.edges()[*edge_idx];
            let (alias, _) = aliases.iter().find(|(_, (idx, _))| idx == edge_idx).unwrap();

            let sources: Vec<&String> = edge.sources.iter().map(|s| {
                let key = s.to_string();
                node_names.get(&key).unwrap()
            }).collect();

            let target_key = edge.target.to_string();
            let target_name = node_names.get(&target_key).unwrap();

            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if sources.len() == 1 {
                writeln!(output, "[{} -> {}{} -> {}]", sources[0], loop_prefix, alias, target_name).unwrap();
            } else {
                let group = sources.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
                writeln!(output, "[({}) -> {}{} -> {}]", group, loop_prefix, alias, target_name).unwrap();
            }
        }

        if output.ends_with('\n') {
            output.pop();
        }

        output
    }

    /// Serialize this machine graph to machine notation in the specified format.
    ///
    /// The output is deterministic: same graph + same format → same string.
    pub fn to_machine_notation_formatted(&self, format: NotationFormat) -> String {
        if self.edges().is_empty() {
            return String::new();
        }

        let (aliases, node_names, edge_order) = self.build_serialization_maps();
        let mut output = String::new();

        let (open, close, sep) = match format {
            NotationFormat::Bracketed => ("[", "]", ""),
            NotationFormat::LineBased => ("", "", "\n"),
        };

        // Emit headers in alias-sorted order
        let mut sorted_aliases: Vec<(&String, &(usize, String))> = aliases.iter().collect();
        sorted_aliases.sort_by_key(|(alias, _)| *alias);

        for (alias, (edge_idx, _cap_str)) in &sorted_aliases {
            let edge = &self.edges()[*edge_idx];
            write!(output, "{}{} {}{}{}", open, alias, edge.cap_urn, close, sep).unwrap();
        }

        // Emit wirings in edge order
        for edge_idx in &edge_order {
            let edge = &self.edges()[*edge_idx];
            let (alias, _) = aliases.iter().find(|(_, (idx, _))| idx == edge_idx).unwrap();

            let sources: Vec<&String> = edge.sources.iter().map(|s| {
                let key = s.to_string();
                node_names.get(&key).unwrap()
            }).collect();

            let target_key = edge.target.to_string();
            let target_name = node_names.get(&target_key).unwrap();

            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if sources.len() == 1 {
                write!(output, "{}{} -> {}{} -> {}{}{}", open, sources[0], loop_prefix, alias, target_name, close, sep).unwrap();
            } else {
                let group = sources.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
                write!(output, "{}({}) -> {}{} -> {}{}{}", open, group, loop_prefix, alias, target_name, close, sep).unwrap();
            }
        }

        // Remove trailing separator
        if format == NotationFormat::LineBased && output.ends_with('\n') {
            output.pop();
        }

        output
    }

    /// Build the alias map, node name map, and edge ordering for serialization.
    ///
    /// Returns:
    /// - `aliases`: alias → (edge_index, cap_urn_string)
    /// - `node_names`: media_urn_canonical_string → node_name
    /// - `edge_order`: edge indices in canonical order
    fn build_serialization_maps(&self) -> (BTreeMap<String, (usize, String)>, HashMap<String, String>, Vec<usize>) {
        // Step 1: Generate canonical edge ordering
        let mut edge_order: Vec<usize> = (0..self.edges().len()).collect();
        edge_order.sort_by(|a, b| {
            let ea = &self.edges()[*a];
            let eb = &self.edges()[*b];

            let cap_cmp = ea.cap_urn.to_string().cmp(&eb.cap_urn.to_string());
            if cap_cmp != std::cmp::Ordering::Equal {
                return cap_cmp;
            }

            let src_a: Vec<String> = ea.sources.iter().map(|s| s.to_string()).collect();
            let src_b: Vec<String> = eb.sources.iter().map(|s| s.to_string()).collect();
            let src_cmp = src_a.cmp(&src_b);
            if src_cmp != std::cmp::Ordering::Equal {
                return src_cmp;
            }

            ea.target.to_string().cmp(&eb.target.to_string())
        });

        // Step 2: Generate aliases from op= tag
        let mut aliases: BTreeMap<String, (usize, String)> = BTreeMap::new();
        let mut alias_counts: HashMap<String, usize> = HashMap::new();

        for &idx in &edge_order {
            let edge = &self.edges()[idx];
            let base_alias = edge.cap_urn.get_tag("op")
                .map(|s| s.clone())
                .unwrap_or_else(|| format!("edge_{}", idx));

            let count = alias_counts.entry(base_alias.clone()).or_insert(0);
            let alias = if *count == 0 {
                base_alias.clone()
            } else {
                format!("{}_{}", base_alias, count)
            };
            *count += 1;

            let cap_str = edge.cap_urn.to_string();
            aliases.insert(alias, (idx, cap_str));
        }

        // Step 3: Generate node names
        // Collect all unique media URNs, assign names in order of first appearance
        let mut node_names: HashMap<String, String> = HashMap::new();
        let mut node_counter = 0usize;

        for &idx in &edge_order {
            let edge = &self.edges()[idx];
            for src in &edge.sources {
                let key = src.to_string();
                if !node_names.contains_key(&key) {
                    node_names.insert(key, format!("n{}", node_counter));
                    node_counter += 1;
                }
            }
            let target_key = edge.target.to_string();
            if !node_names.contains_key(&target_key) {
                node_names.insert(target_key, format!("n{}", node_counter));
                node_counter += 1;
            }
        }

        (aliases, node_names, edge_order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::cap_urn::CapUrn;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap()
    }

    fn cap(s: &str) -> CapUrn {
        CapUrn::from_string(s).unwrap()
    }

    fn edge(sources: &[&str], cap_str: &str, target: &str, is_loop: bool) -> MachineEdge {
        MachineEdge {
            sources: sources.iter().map(|s| media(s)).collect(),
            cap_urn: cap(cap_str),
            target: media(target),
            is_loop,
        }
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
        let notation = g.to_machine_notation();
        assert!(notation.contains("[extract "));
        assert!(notation.contains("-> extract ->"));
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
        let notation = g.to_machine_notation();
        // Should have 2 headers and 2 wirings
        let bracket_count = notation.matches('[').count();
        assert_eq!(bracket_count, 4); // 2 headers + 2 wirings
    }

    #[test]
    fn serialize_empty_graph() {
        let g = Machine::empty();
        assert_eq!(g.to_machine_notation(), "");
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
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let n1 = g.to_machine_notation();
        let n2 = g.to_machine_notation();
        assert_eq!(n1, n2, "Serialization must be deterministic");
    }

    #[test]
    fn reordered_edges_produce_same_notation() {
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
        // Same edges, reversed order
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
        assert_eq!(
            g1.to_machine_notation(),
            g2.to_machine_notation(),
            "Same graph with reordered edges must produce identical notation"
        );
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    #[test]
    fn multiline_format() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let multi = g.to_machine_notation_multiline();
        assert!(multi.contains('\n'), "Multi-line format must contain newlines");

        // Should still round-trip
        let reparsed = Machine::from_string(&multi).unwrap();
        assert!(g.is_equivalent(&reparsed));
    }

    // =========================================================================
    // Alias generation
    // =========================================================================

    #[test]
    fn alias_from_op_tag() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation();
        // Should use "extract" as alias (from op= tag)
        assert!(notation.contains("[extract "), "Expected 'extract' alias, got: {}", notation);
    }

    #[test]
    fn alias_fallback_without_op_tag() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation();
        // Should use fallback alias "edge_N"
        assert!(notation.contains("edge_"), "Expected fallback alias, got: {}", notation);
    }

    #[test]
    fn duplicate_op_tags_disambiguated() {
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
        let notation = g.to_machine_notation();
        // Should have "extract" and "extract_1"
        assert!(notation.contains("extract_1") || notation.contains("extract_2"),
            "Duplicate ops must be disambiguated: {}", notation);
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
        let notation = g.to_machine_notation_formatted(NotationFormat::LineBased);
        // No brackets
        assert!(!notation.contains('['), "Line-based format must not contain brackets: {}", notation);
        assert!(!notation.contains(']'), "Line-based format must not contain brackets: {}", notation);
        // Contains content
        assert!(notation.contains("extract cap:"));
        assert!(notation.contains("-> extract ->"));
    }

    #[test]
    fn line_based_roundtrip_single_edge() {
        let original = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
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
        let n1 = g.to_machine_notation_formatted(NotationFormat::LineBased);
        let n2 = g.to_machine_notation_formatted(NotationFormat::LineBased);
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
        let bracketed = g.to_machine_notation_formatted(NotationFormat::Bracketed);
        let line_based = g.to_machine_notation_formatted(NotationFormat::LineBased);

        let g_bracketed = Machine::from_string(&bracketed).unwrap();
        let g_line_based = Machine::from_string(&line_based).unwrap();
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

        let graph = Machine::from_path(&path).unwrap();
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

        let graph = Machine::from_path(&path).unwrap();
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

        let graph = Machine::from_path(&path).unwrap();
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
}
