//! Route graph — typed DAG representation for route notation
//!
//! A `RouteGraph` is the semantic model behind route notation. It represents
//! a directed acyclic graph of capability edges, where each edge transforms
//! one or more source media types into a target media type via a capability.
//!
//! ## Equivalence
//!
//! Two `RouteGraph`s are equivalent if they have the same set of edges,
//! compared using `MediaUrn::is_equivalent()` for media types and
//! `CapUrn` BTreeMap equality for capabilities. Alias names and statement
//! ordering are serialization concerns only — they do not affect equivalence.
//!
//! This follows the same pattern as `TaggedUrn` where `BTreeMap` ensures
//! canonical ordering and `is_equivalent()` compares semantics, not characters.

use std::fmt;

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

/// A single edge in the route graph.
///
/// Each edge represents a capability that transforms one or more source
/// media types into a target media type. The `is_loop` flag indicates
/// ForEach semantics (the capability is applied to each item in a list).
#[derive(Debug, Clone)]
pub struct RouteEdge {
    /// Input media URN(s) — from connected cap's in-spec.
    /// Multiple sources represent fan-in (e.g., text + model-spec → embeddings).
    pub sources: Vec<MediaUrn>,
    /// The capability URN (edge label)
    pub cap_urn: CapUrn,
    /// Output media URN — from cap's out-spec
    pub target: MediaUrn,
    /// Whether this edge has ForEach semantics (iterate over list items)
    pub is_loop: bool,
}

impl RouteEdge {
    /// Check if two edges are semantically equivalent.
    ///
    /// Equivalence is defined as:
    /// - Same number of sources, and each source in self has an equivalent source in other
    /// - Equivalent cap URNs (via CapUrn::is_equivalent which uses BTreeMap equality)
    /// - Equivalent target media URNs (via MediaUrn::is_equivalent)
    /// - Same is_loop flag
    ///
    /// Source order does not matter — fan-in sources are compared as sets.
    pub fn is_equivalent(&self, other: &RouteEdge) -> bool {
        if self.is_loop != other.is_loop {
            return false;
        }

        if !self.cap_urn.is_equivalent(&other.cap_urn) {
            return false;
        }

        // Target equivalence — fail hard on parse errors since these URNs
        // were already validated during construction
        match self.target.is_equivalent(&other.target) {
            Ok(eq) => {
                if !eq {
                    return false;
                }
            }
            Err(_) => return false,
        }

        // Source set equivalence — order-independent comparison
        if self.sources.len() != other.sources.len() {
            return false;
        }

        // For each source in self, find a matching source in other.
        // Track which indices in `other` have been matched to avoid double-counting.
        let mut matched = vec![false; other.sources.len()];
        for self_src in &self.sources {
            let found = other.sources.iter().enumerate().any(|(j, other_src)| {
                if matched[j] {
                    return false;
                }
                match self_src.is_equivalent(other_src) {
                    Ok(true) => {
                        matched[j] = true;
                        true
                    }
                    _ => false,
                }
            });
            if !found {
                return false;
            }
        }

        true
    }
}

impl PartialEq for RouteEdge {
    fn eq(&self, other: &Self) -> bool {
        self.is_equivalent(other)
    }
}

impl Eq for RouteEdge {}

/// A route graph — the semantic model behind route notation.
///
/// The graph is a collection of directed edges where each edge is a capability
/// that transforms source media types into a target media type. The graph
/// structure captures the full transformation pipeline.
///
/// ## Equivalence
///
/// Two graphs are equivalent if they have the same set of edges, regardless
/// of ordering. Alias names used in the textual notation are not part of
/// the graph model.
#[derive(Debug, Clone)]
pub struct RouteGraph {
    /// Edges in the graph, ordered for deterministic serialization.
    /// Comparison is order-independent (set semantics).
    edges: Vec<RouteEdge>,
}

impl RouteGraph {
    /// Create a new route graph from a vector of edges.
    pub fn new(edges: Vec<RouteEdge>) -> Self {
        Self { edges }
    }

    /// Create an empty route graph.
    pub fn empty() -> Self {
        Self { edges: Vec::new() }
    }

    /// Get the edges of this graph.
    pub fn edges(&self) -> &[RouteEdge] {
        &self.edges
    }

    /// Get a mutable reference to the edges (for building during parsing).
    pub fn edges_mut(&mut self) -> &mut Vec<RouteEdge> {
        &mut self.edges
    }

    /// Number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Check if the graph has no edges.
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Check if two route graphs are semantically equivalent.
    ///
    /// Two graphs are equivalent if they have the same set of edges
    /// (compared using `RouteEdge::is_equivalent`). Edge ordering
    /// does not matter.
    pub fn is_equivalent(&self, other: &RouteGraph) -> bool {
        if self.edges.len() != other.edges.len() {
            return false;
        }

        // For each edge in self, find a matching edge in other.
        let mut matched = vec![false; other.edges.len()];
        for self_edge in &self.edges {
            let found = other.edges.iter().enumerate().any(|(j, other_edge)| {
                if matched[j] {
                    return false;
                }
                if self_edge.is_equivalent(other_edge) {
                    matched[j] = true;
                    true
                } else {
                    false
                }
            });
            if !found {
                return false;
            }
        }

        true
    }

    /// Collect all unique source media URNs across all edges that are not
    /// also produced as targets by any other edge. These are the "root"
    /// inputs to the graph.
    pub fn root_sources(&self) -> Vec<&MediaUrn> {
        let mut roots = Vec::new();
        for edge in &self.edges {
            for src in &edge.sources {
                // Check if any edge produces this source as a target
                let is_produced = self.edges.iter().any(|e| {
                    e.target.is_equivalent(src).unwrap_or(false)
                });
                if !is_produced {
                    // Avoid duplicates (by equivalence)
                    let already_added = roots.iter().any(|r: &&MediaUrn| {
                        r.is_equivalent(src).unwrap_or(false)
                    });
                    if !already_added {
                        roots.push(src);
                    }
                }
            }
        }
        roots
    }

    /// Collect all unique target media URNs that are not consumed as sources
    /// by any other edge. These are the "leaf" outputs of the graph.
    pub fn leaf_targets(&self) -> Vec<&MediaUrn> {
        let mut leaves = Vec::new();
        for edge in &self.edges {
            let is_consumed = self.edges.iter().any(|e| {
                e.sources.iter().any(|s| {
                    s.is_equivalent(&edge.target).unwrap_or(false)
                })
            });
            if !is_consumed {
                let already_added = leaves.iter().any(|l: &&MediaUrn| {
                    l.is_equivalent(&edge.target).unwrap_or(false)
                });
                if !already_added {
                    leaves.push(&edge.target);
                }
            }
        }
        leaves
    }
}

impl PartialEq for RouteGraph {
    fn eq(&self, other: &Self) -> bool {
        self.is_equivalent(other)
    }
}

impl Eq for RouteGraph {}

impl fmt::Display for RouteGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.edges.is_empty() {
            return write!(f, "RouteGraph(empty)");
        }
        write!(f, "RouteGraph({} edges)", self.edges.len())
    }
}

impl fmt::Display for RouteEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sources: Vec<String> = self.sources.iter().map(|s| s.to_string()).collect();
        let loop_prefix = if self.is_loop { "LOOP " } else { "" };
        write!(
            f,
            "({}) -{}{}-> {}",
            sources.join(", "),
            loop_prefix,
            self.cap_urn,
            self.target
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap()
    }

    fn cap(s: &str) -> CapUrn {
        CapUrn::from_string(s).unwrap()
    }

    fn edge(sources: &[&str], cap_str: &str, target: &str, is_loop: bool) -> RouteEdge {
        RouteEdge {
            sources: sources.iter().map(|s| media(s)).collect(),
            cap_urn: cap(cap_str),
            target: media(target),
            is_loop,
        }
    }

    // =========================================================================
    // RouteEdge equivalence
    // =========================================================================

    #[test]
    fn edge_equivalence_same_urns() {
        let e1 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        let e2 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        assert!(e1.is_equivalent(&e2));
        assert_eq!(e1, e2);
    }

    #[test]
    fn edge_equivalence_different_cap_urns() {
        let e1 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        let e2 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=summarize;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        assert!(!e1.is_equivalent(&e2));
        assert_ne!(e1, e2);
    }

    #[test]
    fn edge_equivalence_different_targets() {
        let e1 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        let e2 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:json;record;textable",
            false,
        );
        assert!(!e1.is_equivalent(&e2));
    }

    #[test]
    fn edge_equivalence_different_loop_flag() {
        let e1 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        let e2 = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            true,
        );
        assert!(!e1.is_equivalent(&e2));
    }

    #[test]
    fn edge_equivalence_source_order_independent() {
        let e1 = edge(
            &["media:txt;textable", "media:model-spec;textable"],
            "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
            "media:embedding-vector;record;textable",
            false,
        );
        let e2 = edge(
            &["media:model-spec;textable", "media:txt;textable"],
            "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
            "media:embedding-vector;record;textable",
            false,
        );
        assert!(e1.is_equivalent(&e2));
    }

    #[test]
    fn edge_equivalence_different_source_count() {
        let e1 = edge(
            &["media:txt;textable"],
            "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
            "media:embedding-vector;record;textable",
            false,
        );
        let e2 = edge(
            &["media:txt;textable", "media:model-spec;textable"],
            "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
            "media:embedding-vector;record;textable",
            false,
        );
        assert!(!e1.is_equivalent(&e2));
    }

    // =========================================================================
    // RouteGraph equivalence
    // =========================================================================

    #[test]
    fn graph_equivalence_same_edges() {
        let g1 = RouteGraph::new(vec![
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
        let g2 = RouteGraph::new(vec![
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
        assert!(g1.is_equivalent(&g2));
        assert_eq!(g1, g2);
    }

    #[test]
    fn graph_equivalence_reordered_edges() {
        let g1 = RouteGraph::new(vec![
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
        let g2 = RouteGraph::new(vec![
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
        assert!(g1.is_equivalent(&g2));
        assert_eq!(g1, g2);
    }

    #[test]
    fn graph_not_equivalent_different_edge_count() {
        let g1 = RouteGraph::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let g2 = RouteGraph::new(vec![
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
        assert!(!g1.is_equivalent(&g2));
        assert_ne!(g1, g2);
    }

    #[test]
    fn graph_not_equivalent_different_cap() {
        let g1 = RouteGraph::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let g2 = RouteGraph::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=summarize;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        assert!(!g1.is_equivalent(&g2));
    }

    #[test]
    fn graph_empty() {
        let g = RouteGraph::empty();
        assert!(g.is_empty());
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn graph_empty_equivalence() {
        let g1 = RouteGraph::empty();
        let g2 = RouteGraph::empty();
        assert!(g1.is_equivalent(&g2));
        assert_eq!(g1, g2);
    }

    // =========================================================================
    // Root sources and leaf targets
    // =========================================================================

    #[test]
    fn root_sources_linear_chain() {
        let g = RouteGraph::new(vec![
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
        let roots = g.root_sources();
        assert_eq!(roots.len(), 1);
        assert!(roots[0].is_equivalent(&media("media:pdf")).unwrap());
    }

    #[test]
    fn leaf_targets_linear_chain() {
        let g = RouteGraph::new(vec![
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
        let leaves = g.leaf_targets();
        assert_eq!(leaves.len(), 1);
        assert!(leaves[0]
            .is_equivalent(&media("media:embedding-vector;record;textable"))
            .unwrap());
    }

    #[test]
    fn root_sources_fan_in() {
        // Two sources feed into one cap
        let g = RouteGraph::new(vec![edge(
            &["media:txt;textable", "media:model-spec;textable"],
            "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
            "media:embedding-vector;record;textable",
            false,
        )]);
        let roots = g.root_sources();
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn display_edge() {
        let e = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        );
        let display = format!("{}", e);
        assert!(display.contains("media:pdf"));
        assert!(display.contains("media:textable;txt")); // canonical form
    }

    #[test]
    fn display_graph() {
        let g = RouteGraph::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let display = format!("{}", g);
        assert_eq!(display, "RouteGraph(1 edges)");
    }

    #[test]
    fn display_empty_graph() {
        let g = RouteGraph::empty();
        let display = format!("{}", g);
        assert_eq!(display, "RouteGraph(empty)");
    }
}
