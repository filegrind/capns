//! Machine notation parser — pest-generated PEG parser plus
//! anchor-realization layer.
//!
//! Parses the machine notation format into a `Machine` using a
//! formal PEG grammar defined in `machine.pest`. The parser is
//! a two-phase pipeline:
//!
//! 1. **Lexical / grammatical** — pest produces an AST of
//!    headers and wirings. Failures here surface as
//!    `MachineSyntaxError`.
//! 2. **Resolution** — the wirings are partitioned into
//!    connected components (one component per maximal set of
//!    wirings sharing node names), each component is fed to
//!    `resolve::resolve_wiring_set`, and the resulting
//!    `MachineStrand`s are assembled into a `Machine` in the
//!    order each component first appears textually. Failures
//!    here surface as `MachineAbstractionError`.
//!
//! The combined result type is `MachineParseError`.
//!
//! ## Grammar (PEG / EBNF)
//!
//! ```ebnf
//! program      = stmt*
//! stmt         = "[" inner "]" | inner
//! inner        = wiring | header
//! header       = alias cap_urn
//! wiring       = source arrow loop_cap arrow alias
//! source       = group | alias
//! group        = "(" alias ("," alias)+ ")"
//! arrow        = "-"+ ">"
//! loop_cap     = "LOOP" alias | alias
//! alias        = (ALPHA | "_") (ALNUM | "_" | "-")*
//! cap_urn      = "cap:" cap_urn_body*
//! cap_urn_body = quoted_value | !("]" | NEWLINE) ANY
//! quoted_value = '"' ('\\"' | '\\\\' | !'"' ANY)* '"'
//! ```
//!
//! ## Strand boundary discovery
//!
//! Two wirings belong to the same `MachineStrand` iff there
//! exists a path through the wiring set, hopping along shared
//! node-name endpoints, that connects them. Connected
//! components are computed via union-find. The strand list in
//! the resulting `Machine` is in **first-appearance order**:
//! the strand whose earliest wiring appears first in the
//! textual input comes first.
//!
//! ## Media URN derivation per node name
//!
//! For each wiring, the parser derives the media URNs that
//! its source node names and target node name are bound to:
//!
//! - **Primary source** (slot 0 in the wiring's source group):
//!   bound to the cap's declared `in=` URN. If the same node
//!   name was already bound to a different URN by an earlier
//!   wiring, the two URNs must be `is_comparable` (on the same
//!   specialization chain).
//! - **Secondary sources** (slots 1+): take whichever URN was
//!   previously bound to that node name. If unbound, default
//!   to `media:` (the wildcard); the resolver / orchestrator
//!   will distinguish concrete arg URNs at run time.
//! - **Target**: bound to the cap's declared `out=` URN, with
//!   the same `is_comparable` check.

use std::collections::HashMap;

use pest::Parser;
use pest_derive::Parser;

use crate::cap::registry::CapRegistry;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::{MachineParseError, MachineSyntaxError};
use super::graph::{Machine, MachineStrand, NodeId};
use super::resolve::{resolve_pre_interned, PreInternedWiring};

#[derive(Parser)]
#[grammar = "machine/machine.pest"]
pub struct MachineParser;

/// One wiring as it comes off the AST walk, with raw alias
/// names. Resolution happens in two more passes after this.
struct RawWiring {
    /// Node-name aliases for the source slots, in the order
    /// the user wrote them. Slot 0 is the primary.
    sources: Vec<String>,
    /// Cap header alias.
    cap_alias: String,
    /// Node-name alias for the target.
    target: String,
    is_loop: bool,
    /// Index of this wiring in the textual input. Used to
    /// order connected components by first appearance.
    position: usize,
}

/// Per-strand mapping from user-written node name to the
/// `NodeId` the parser allocated for that name. Returned by
/// `parse_machine_with_node_names` for callers that need to
/// preserve the user's node-name identity through the
/// resolved-machine layer (the orchestrator's
/// `ResolvedGraph` is keyed on these names).
pub type StrandNodeNames = HashMap<String, NodeId>;

/// Parse machine notation into a `Machine`, discarding the
/// per-strand user node names.
///
/// Two-phase: pest grammar parsing → resolver. Either phase
/// may fail; the combined error type is `MachineParseError`.
/// The cap registry is required by the resolver to look up
/// each cap's `args` list and run source-to-arg matching.
pub fn parse_machine(input: &str, registry: &CapRegistry) -> Result<Machine, MachineParseError> {
    let (machine, _names) = parse_machine_with_node_names(input, registry)?;
    Ok(machine)
}

/// Parse machine notation into a `Machine` AND a per-strand
/// mapping from user-written node name to the resolved
/// `NodeId`. The strand vec and the names vec are aligned —
/// `names[i]` is the name map for `machine.strands()[i]`.
///
/// Used by callers that need to preserve user-facing node
/// identity through the resolved-machine layer (the
/// orchestrator's `ResolvedGraph`, the `bin/capdag` binary's
/// input-node finder).
pub fn parse_machine_with_node_names(
    input: &str,
    registry: &CapRegistry,
) -> Result<(Machine, Vec<StrandNodeNames>), MachineParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MachineSyntaxError::Empty.into());
    }

    // Phase 1: pest grammar parse.
    let pairs =
        MachineParser::parse(Rule::program, input).map_err(|e| MachineSyntaxError::ParseError {
            details: format!("{}", e),
        })?;

    // Phase 2: walk AST collecting headers and wirings.
    let mut headers: Vec<(String, CapUrn, usize)> = Vec::new();
    let mut wirings: Vec<RawWiring> = Vec::new();

    let program = pairs
        .into_iter()
        .next()
        .expect("pest produces a program rule");
    for (stmt_idx, pair) in program.into_inner().enumerate() {
        if pair.as_rule() != Rule::stmt {
            continue; // skip EOI
        }
        let inner = pair.into_inner().next().expect("stmt wraps inner");
        let content = inner
            .into_inner()
            .next()
            .expect("inner wraps header or wiring");

        match content.as_rule() {
            Rule::header => {
                let mut inner_pairs = content.into_inner();
                let alias = inner_pairs
                    .next()
                    .expect("header has alias")
                    .as_str()
                    .to_string();
                let cap_urn_str = inner_pairs.next().expect("header has cap_urn").as_str();
                let cap_urn = CapUrn::from_string(cap_urn_str).map_err(|e| {
                    MachineSyntaxError::InvalidCapUrn {
                        alias: alias.clone(),
                        details: format!("{}", e),
                    }
                })?;
                headers.push((alias, cap_urn, stmt_idx));
            }
            Rule::wiring => {
                let mut inner_pairs = content.into_inner();
                let source_pair = inner_pairs.next().expect("wiring has source");
                let sources = parse_source(source_pair);
                inner_pairs.next(); // arrow
                let loop_cap_pair = inner_pairs.next().expect("wiring has loop_cap");
                let (is_loop, cap_alias) = parse_loop_cap(loop_cap_pair);
                inner_pairs.next(); // arrow
                let target = inner_pairs
                    .next()
                    .expect("wiring has target")
                    .as_str()
                    .to_string();
                wirings.push(RawWiring {
                    sources,
                    cap_alias,
                    target,
                    is_loop,
                    position: stmt_idx,
                });
            }
            _ => unreachable!("grammar guarantees inner is header or wiring"),
        }
    }

    // Phase 3: alias map with duplicate check.
    let mut alias_map: HashMap<String, (CapUrn, usize)> = HashMap::new();
    for (alias, cap_urn, position) in &headers {
        if let Some((_, first_pos)) = alias_map.get(alias) {
            return Err(MachineSyntaxError::DuplicateAlias {
                alias: alias.clone(),
                first_position: *first_pos,
            }
            .into());
        }
        alias_map.insert(alias.clone(), (cap_urn.clone(), *position));
    }

    if wirings.is_empty() && !headers.is_empty() {
        return Err(MachineSyntaxError::NoEdges.into());
    }
    if wirings.is_empty() {
        return Err(MachineSyntaxError::Empty.into());
    }

    // Phase 4: derive node-name → MediaUrn bindings.
    //
    // Walk wirings in textual order. For each wiring:
    //   - Primary source: bind cap.in=
    //   - Secondary sources: bind to whatever they already
    //     hold (or media: wildcard if unbound)
    //   - Target: bind cap.out=
    // Re-binding is allowed iff the new URN is_comparable to
    // the existing one (same specialization chain).
    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();
    let wildcard = MediaUrn::from_string("media:").expect("wildcard media URN parses");

    for w in &wirings {
        let (cap_urn, _) =
            alias_map
                .get(&w.cap_alias)
                .ok_or_else(|| MachineSyntaxError::UndefinedAlias {
                    alias: w.cap_alias.clone(),
                })?;

        for src in &w.sources {
            if alias_map.contains_key(src) {
                return Err(MachineSyntaxError::NodeAliasCollision {
                    name: src.clone(),
                    alias: src.clone(),
                }
                .into());
            }
        }
        if alias_map.contains_key(&w.target) {
            return Err(MachineSyntaxError::NodeAliasCollision {
                name: w.target.clone(),
                alias: w.target.clone(),
            }
            .into());
        }

        let cap_in_media =
            cap_urn
                .in_media_urn()
                .map_err(|e| MachineSyntaxError::InvalidMediaUrn {
                    alias: w.cap_alias.clone(),
                    details: format!("in= spec: {}", e),
                })?;
        let cap_out_media =
            cap_urn
                .out_media_urn()
                .map_err(|e| MachineSyntaxError::InvalidMediaUrn {
                    alias: w.cap_alias.clone(),
                    details: format!("out= spec: {}", e),
                })?;

        // Primary source: bind to cap.in=
        if !w.sources.is_empty() {
            assign_or_check_node(&w.sources[0], &cap_in_media, &mut node_media, w.position)?;
            // Secondaries: bind to wildcard if unbound, leave
            // alone otherwise. The bound value is what
            // resolution will see.
            for src in w.sources.iter().skip(1) {
                if !node_media.contains_key(src) {
                    node_media.insert(src.clone(), wildcard.clone());
                }
            }
        }
        assign_or_check_node(&w.target, &cap_out_media, &mut node_media, w.position)?;
    }

    // Phase 5: connected-components partition by shared node
    // name. Union-find over wiring indices, where two wirings
    // are unioned iff they share at least one node name.
    let n = wirings.len();
    let mut union = UnionFind::new(n);

    // Map: node name → index of the first wiring that touched
    // it. As we process wirings in order, any wiring that
    // touches a previously-seen node name is unioned with the
    // earlier wiring.
    let mut node_first_wiring: HashMap<String, usize> = HashMap::new();
    for (w_idx, w) in wirings.iter().enumerate() {
        let mut node_names: Vec<&String> = Vec::with_capacity(w.sources.len() + 1);
        node_names.extend(w.sources.iter());
        node_names.push(&w.target);
        for node_name in node_names {
            if let Some(&earlier) = node_first_wiring.get(node_name) {
                union.union(earlier, w_idx);
            } else {
                node_first_wiring.insert(node_name.clone(), w_idx);
            }
        }
    }

    // Group wirings by their union-find root. Order roots by
    // the smallest wiring index in each group (= first-
    // appearance order).
    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for w_idx in 0..n {
        let root = union.find(w_idx);
        groups.entry(root).or_default().push(w_idx);
    }
    let mut group_min_idx: Vec<(usize, usize)> = groups
        .iter()
        .map(|(&root, members)| {
            let min_idx = *members.iter().min().expect("non-empty group");
            (root, min_idx)
        })
        .collect();
    group_min_idx.sort_by_key(|(_, min_idx)| *min_idx);

    // Phase 6: per-component pre-interning + resolution.
    //
    // For each connected component (= strand), allocate
    // `NodeId`s in the order user node names are encountered
    // (walking the wirings in their textual order). The
    // resolver receives `PreInternedWiring`s that already
    // reference NodeIds, plus the parallel `nodes: Vec<MediaUrn>`
    // table. Two distinct user node names that happen to share
    // a media URN stay distinct NodeIds — that's the parser's
    // identity contract.
    let mut strands: Vec<MachineStrand> = Vec::with_capacity(group_min_idx.len());
    let mut strand_node_names: Vec<StrandNodeNames> = Vec::with_capacity(group_min_idx.len());
    for (strand_index, (root, _)) in group_min_idx.iter().enumerate() {
        let mut member_indices = groups[root].clone();
        member_indices.sort();

        let mut nodes: Vec<MediaUrn> = Vec::new();
        let mut name_to_id: StrandNodeNames = HashMap::new();

        // Allocate a NodeId for `name`. If `name` is already
        // bound to a NodeId in this strand, return it.
        // Otherwise allocate a new NodeId, push the name's
        // bound URN onto the nodes table, and return.
        fn intern_named(
            name: &str,
            node_media: &HashMap<String, MediaUrn>,
            nodes: &mut Vec<MediaUrn>,
            name_to_id: &mut StrandNodeNames,
        ) -> NodeId {
            if let Some(id) = name_to_id.get(name) {
                return *id;
            }
            let urn = node_media
                .get(name)
                .cloned()
                .expect("every node name was bound during phase 4");
            let id = nodes.len() as NodeId;
            nodes.push(urn);
            name_to_id.insert(name.to_string(), id);
            id
        }

        let mut pre_interned: Vec<PreInternedWiring> = Vec::with_capacity(member_indices.len());
        for &w_idx in &member_indices {
            let w = &wirings[w_idx];
            let (cap_urn, _) = alias_map
                .get(&w.cap_alias)
                .expect("cap alias was validated above");

            let source_node_ids: Vec<NodeId> = w
                .sources
                .iter()
                .map(|name| intern_named(name, &node_media, &mut nodes, &mut name_to_id))
                .collect();
            let target_node_id = intern_named(&w.target, &node_media, &mut nodes, &mut name_to_id);

            pre_interned.push(PreInternedWiring {
                cap_urn: cap_urn.clone(),
                source_node_ids,
                target_node_id,
                is_loop: w.is_loop,
            });
        }

        let strand = resolve_pre_interned(nodes, &pre_interned, registry, strand_index)?;
        strands.push(strand);
        strand_node_names.push(name_to_id);
    }

    Ok((Machine::from_resolved_strands(strands), strand_node_names))
}

impl Machine {
    /// Parse machine notation into a `Machine`.
    ///
    /// Combined lexical / grammatical / resolution parse. The
    /// cap registry is required to resolve each cap's argument
    /// structure during anchor realization.
    pub fn from_string(input: &str, registry: &CapRegistry) -> Result<Self, MachineParseError> {
        parse_machine(input, registry)
    }
}

/// Extract source node names from a source pair (single alias
/// or group).
fn parse_source(pair: pest::iterators::Pair<Rule>) -> Vec<String> {
    let inner = pair.into_inner().next().expect("source has inner");
    match inner.as_rule() {
        Rule::group => inner
            .into_inner()
            .filter(|p| p.as_rule() == Rule::alias)
            .map(|p| p.as_str().to_string())
            .collect(),
        Rule::alias => vec![inner.as_str().to_string()],
        _ => unreachable!("source is group or alias"),
    }
}

/// Extract is_loop flag and cap alias from a loop_cap pair.
fn parse_loop_cap(pair: pest::iterators::Pair<Rule>) -> (bool, String) {
    let mut is_loop = false;
    let mut cap_alias = String::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::loop_keyword => is_loop = true,
            Rule::alias => cap_alias = inner.as_str().to_string(),
            _ => {}
        }
    }
    (is_loop, cap_alias)
}

/// Bind a media URN to a node, or check that an existing
/// binding is comparable. Two URNs bound to the same node name
/// must be on the same specialization chain (`is_comparable`);
/// the resolver will pick the more-specific one when it runs.
fn assign_or_check_node(
    node: &str,
    media_urn: &MediaUrn,
    node_media: &mut HashMap<String, MediaUrn>,
    position: usize,
) -> Result<(), MachineSyntaxError> {
    if let Some(existing) = node_media.get(node) {
        let compatible = existing.is_comparable(media_urn).unwrap_or(false);
        if !compatible {
            return Err(MachineSyntaxError::InvalidWiring {
                position,
                details: format!(
                    "node '{}' has conflicting media types: existing '{}', new '{}'",
                    node, existing, media_urn
                ),
            });
        }
        // The more-specific URN wins (so a downstream cap with
        // a tighter pattern bound to the same node refines the
        // type at that data position).
        if media_urn.specificity() > existing.specificity() {
            node_media.insert(node.to_string(), media_urn.clone());
        }
    } else {
        node_media.insert(node.to_string(), media_urn.clone());
    }
    Ok(())
}

/// Tiny union-find used for connected-components partition.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra] < self.rank[rb] {
            self.parent[ra] = rb;
        } else if self.rank[ra] > self.rank[rb] {
            self.parent[rb] = ra;
        } else {
            self.parent[rb] = ra;
            self.rank[ra] += 1;
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::parse_machine;
    use crate::cap::registry::CapRegistry;
    use crate::machine::error::{MachineAbstractionError, MachineParseError, MachineSyntaxError};
    use crate::machine::test_fixtures::{build_cap, registry_with};

    fn pdf_extract_embed_registry() -> CapRegistry {
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
        registry_with(vec![extract, embed])
    }

    // TEST1163: Parsing one connected strand yields a single machine strand with both caps connected by the shared node.
    #[test]
    fn test1163_parse_single_strand_two_caps_connected_via_shared_node() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:in=media:pdf;op=extract;out=\"media:txt;textable\"]\
[embed cap:in=media:textable;op=embed;out=\"media:vec;record\"]\
[doc -> extract -> txt]\
[txt -> embed -> vec]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        // Two wirings, one shared node `txt` → ONE connected
        // component → ONE strand.
        assert_eq!(machine.strand_count(), 1);
        let strand = &machine.strands()[0];
        assert_eq!(strand.edges().len(), 2);
        // The intermediate node must be the same NodeId for
        // both edges.
        let extract_target = strand.edges()[0].target;
        let embed_source = strand.edges()[1].assignment[0].source;
        assert_eq!(extract_target, embed_source);
    }

    // TEST1164: Parsing two disconnected strand definitions yields two separate machine strands.
    #[test]
    fn test1164_parse_two_disconnected_strands_yields_two_machine_strands() {
        // Two strands sharing no node names. The parser must
        // partition them into two `MachineStrand`s and order
        // them by first appearance in the textual input.
        let convert_a = build_cap(
            "cap:in=media:json;op=convert_a;out=media:csv",
            "convert_a",
            &["media:json"],
            "media:csv",
        );
        let convert_b = build_cap(
            "cap:in=media:html;op=convert_b;out=media:txt",
            "convert_b",
            &["media:html"],
            "media:txt",
        );
        let registry = registry_with(vec![convert_a, convert_b]);
        let notation = "\
[ca cap:in=media:json;op=convert_a;out=media:csv]\
[cb cap:in=media:html;op=convert_b;out=media:txt]\
[input_a -> ca -> output_a]\
[input_b -> cb -> output_b]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        assert_eq!(
            machine.strand_count(),
            2,
            "two wirings sharing no nodes must produce two strands"
        );
        // Strand order is first-appearance order. The first
        // wiring `input_a -> ca -> output_a` belongs to strand 0;
        // the second to strand 1.
        assert_eq!(machine.strands()[0].edges().len(), 1);
        assert_eq!(machine.strands()[1].edges().len(), 1);
        // First strand uses convert_a, second uses convert_b.
        assert!(machine.strands()[0].edges()[0]
            .cap_urn
            .to_string()
            .contains("convert_a"));
        assert!(machine.strands()[1].edges()[0]
            .cap_urn
            .to_string()
            .contains("convert_b"));
    }

    // TEST1165: Parsing fails hard when a referenced cap is missing from the registry cache.
    #[test]
    fn test1165_parse_unknown_cap_in_registry_fails_hard() {
        let registry = registry_with(vec![]);
        let notation = "\
[ghost cap:in=media:pdf;op=ghost;out=\"media:txt;textable\"]\
[a -> ghost -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        match err {
            MachineParseError::Resolution(MachineAbstractionError::UnknownCap { cap_urn }) => {
                assert!(cap_urn.contains("ghost"));
            }
            other => panic!("expected Resolution(UnknownCap), got {:?}", other),
        }
    }

    // TEST1166: Duplicate header aliases are reported as syntax errors.
    #[test]
    fn test1166_parse_duplicate_alias_is_syntax_error() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:in=media:pdf;op=extract;out=\"media:txt;textable\"]\
[extract cap:in=media:textable;op=embed;out=\"media:vec;record\"]\
[a -> extract -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::DuplicateAlias { .. })
        ));
    }

    // TEST1167: Wiring that references an undefined alias is reported as a syntax error.
    #[test]
    fn test1167_parse_undefined_alias_is_syntax_error() {
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:in=media:pdf;op=extract;out=\"media:txt;textable\"]\
[a -> notDefined -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { .. })
        ));
    }

    // TEST1168: Parsing rejects node names that collide with declared cap aliases.
    #[test]
    fn test1168_parse_node_alias_collision_with_header_alias_fails_hard() {
        // The user wrote `extract` as a NODE name in a wiring
        // but `extract` is also a header alias. This is
        // structurally ambiguous: is `extract` the cap or the
        // node? The parser must reject it.
        let registry = pdf_extract_embed_registry();
        let notation = "\
[extract cap:in=media:pdf;op=extract;out=\"media:txt;textable\"]\
[extract -> extract -> b]";
        let err = parse_machine(notation, &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::NodeAliasCollision { .. })
        ));
    }

    // TEST1169: Loop markers in notation set the resolved edge loop flag on the following cap step.
    #[test]
    fn test1169_parse_loop_marker_sets_is_loop_on_resolved_edge() {
        let cap_def = build_cap(
            "cap:in=media:textable;op=t;out=media:textable",
            "t",
            &["media:textable"],
            "media:textable",
        );
        let registry = registry_with(vec![cap_def]);
        let notation = "\
[t cap:in=media:textable;op=t;out=media:textable]\
[a -> LOOP t -> b]";
        let machine = parse_machine(notation, &registry).expect("must parse");
        assert_eq!(machine.strand_count(), 1);
        let strand = &machine.strands()[0];
        assert_eq!(strand.edges().len(), 1);
        assert!(
            strand.edges()[0].is_loop,
            "LOOP marker must propagate to MachineEdge::is_loop"
        );
    }

    // TEST1170: Parsing and then serializing machine notation round-trips to the canonical form.
    #[test]
    fn test1170_parse_then_serialize_round_trips_to_canonical_form() {
        // The user can write any aliases / node names; the
        // parse-then-reserialize cycle normalizes them to
        // edge_<i> / n<i> from the global counters. Round-tripping
        // a serializer-produced notation through parse-then-
        // serialize is a fixed point.
        let registry = pdf_extract_embed_registry();
        let user_input = "\
[user_extract cap:in=media:pdf;op=extract;out=\"media:txt;textable\"]\
[user_embed cap:in=media:textable;op=embed;out=\"media:vec;record\"]\
[doc -> user_extract -> txt]\
[txt -> user_embed -> vec]";
        let m1 = parse_machine(user_input, &registry).expect("must parse");
        let canonical = m1.to_machine_notation().expect("must serialize");
        // Canonical form should NOT contain user aliases /
        // node names — they get rewritten to edge_N / nN.
        assert!(!canonical.contains("user_extract"));
        assert!(!canonical.contains("user_embed"));
        assert!(canonical.contains("edge_0"));
        let m2 = parse_machine(&canonical, &registry).expect("canonical must re-parse");
        assert!(m1.is_equivalent(&m2));
        let canonical2 = m2.to_machine_notation().unwrap();
        assert_eq!(canonical, canonical2);
    }

    // TEST1171: Empty machine notation is rejected as a syntax error.
    #[test]
    fn test1171_parse_empty_notation_is_syntax_error() {
        let registry = registry_with(vec![]);
        let err = parse_machine("   ", &registry).unwrap_err();
        assert!(matches!(
            err,
            MachineParseError::Syntax(MachineSyntaxError::Empty)
        ));
    }
}
