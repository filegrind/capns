//! Machine notation parser — pest-generated PEG parser
//!
//! Parses the machine notation format into a `Machine` using a formal
//! PEG grammar defined in `machine.pest`.
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
//! Two equally valid statement forms:
//!
//! - **Bracketed**: `[alias cap:...]` / `[src -> alias -> dst]`
//! - **Line-based**: `alias cap:...` / `src -> alias -> dst`
//!
//! Both can be freely mixed. Whitespace between tokens is handled
//! implicitly by pest's `WHITESPACE` rule. The `alias` and `cap_urn`
//! rules are atomic (`@{}`), so whitespace is not skipped inside them.
//!
//! ## Media URN Derivation
//!
//! Node media URNs are derived from the cap's `in=` and `out=` specs:
//!
//! - For `src -> cap_alias -> dst`: src gets cap's `in=`, dst gets cap's `out=`
//! - For fan-in `(primary, secondary) -> cap_alias -> dst`:
//!   - First group member gets cap's `in=` spec
//!   - Additional members must have types already assigned by prior wirings.
//!     If unassigned, the parser fails — no guessing.

use std::collections::{BTreeMap, HashMap};

use pest::Parser;
use pest_derive::Parser;

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::MachineSyntaxError;
use super::graph::{MachineEdge, Machine};

#[derive(Parser)]
#[grammar = "machine/machine.pest"]
pub struct MachineParser;

/// Parse machine notation into a `Machine`.
///
/// Uses the pest-generated PEG parser to parse the input, then resolves
/// cap URNs and derives media URNs from cap in/out specs.
///
/// # Errors
///
/// Returns `MachineSyntaxError` for any parse failure. Fails hard — no
/// fallbacks, no guessing, no recovery.
pub fn parse_machine(input: &str) -> Result<Machine, MachineSyntaxError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MachineSyntaxError::Empty);
    }

    // Phase 1: Parse with pest grammar
    let pairs = MachineParser::parse(Rule::program, input).map_err(|e| {
        MachineSyntaxError::ParseError {
            details: format!("{}", e),
        }
    })?;

    // Phase 2: Walk the AST and collect headers + wirings
    let mut headers: Vec<(String, CapUrn, usize)> = Vec::new(); // (alias, cap_urn, position)
    let mut wirings: Vec<(Vec<String>, String, String, bool, usize)> = Vec::new(); // (sources, cap_alias, target, is_loop, position)

    let program = pairs.into_iter().next().unwrap(); // program rule
    for (stmt_idx, pair) in program.into_inner().enumerate() {
        if pair.as_rule() != Rule::stmt {
            continue; // skip EOI
        }

        let inner = pair.into_inner().next().unwrap(); // inner rule
        let content = inner.into_inner().next().unwrap(); // header or wiring

        match content.as_rule() {
            Rule::header => {
                let mut inner_pairs = content.into_inner();
                let alias = inner_pairs.next().unwrap().as_str().to_string();
                let cap_urn_str = inner_pairs.next().unwrap().as_str();

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

                // Parse source (single alias or group)
                let source_pair = inner_pairs.next().unwrap();
                let sources = parse_source(source_pair);

                // Skip first arrow
                inner_pairs.next(); // arrow

                // Parse loop_cap (optional LOOP + alias)
                let loop_cap_pair = inner_pairs.next().unwrap();
                let (is_loop, cap_alias) = parse_loop_cap(loop_cap_pair);

                // Skip second arrow
                inner_pairs.next(); // arrow

                // Parse target alias
                let target = inner_pairs.next().unwrap().as_str().to_string();

                wirings.push((sources, cap_alias, target, is_loop, stmt_idx));
            }
            _ => unreachable!("grammar guarantees inner is header or wiring"),
        }
    }

    // Phase 3: Build alias → CapUrn map, checking for duplicates
    let mut alias_map: BTreeMap<String, (CapUrn, usize)> = BTreeMap::new();
    for (alias, cap_urn, position) in &headers {
        if let Some((_, first_pos)) = alias_map.get(alias) {
            return Err(MachineSyntaxError::DuplicateAlias {
                alias: alias.clone(),
                first_position: *first_pos,
            });
        }
        alias_map.insert(alias.clone(), (cap_urn.clone(), *position));
    }

    // Phase 4: Resolve wirings into MachineEdges
    if wirings.is_empty() && !headers.is_empty() {
        return Err(MachineSyntaxError::NoEdges);
    }

    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();
    let mut edges = Vec::new();

    for (sources, cap_alias, target, is_loop, position) in &wirings {
        // Look up the cap alias
        let (cap_urn, _) = alias_map.get(cap_alias).ok_or_else(|| {
            MachineSyntaxError::UndefinedAlias {
                alias: cap_alias.clone(),
            }
        })?;

        // Check node-alias collisions
        for src in sources {
            if alias_map.contains_key(src) {
                return Err(MachineSyntaxError::NodeAliasCollision {
                    name: src.clone(),
                    alias: src.clone(),
                });
            }
        }
        if alias_map.contains_key(target) {
            return Err(MachineSyntaxError::NodeAliasCollision {
                name: target.clone(),
                alias: target.clone(),
            });
        }

        // Derive media URNs from cap's in=/out= specs
        let cap_in_media = cap_urn.in_media_urn().map_err(|e| {
            MachineSyntaxError::InvalidMediaUrn {
                alias: cap_alias.clone(),
                details: format!("in= spec: {}", e),
            }
        })?;
        let cap_out_media = cap_urn.out_media_urn().map_err(|e| {
            MachineSyntaxError::InvalidMediaUrn {
                alias: cap_alias.clone(),
                details: format!("out= spec: {}", e),
            }
        })?;

        // Resolve source media URNs
        let mut source_urns = Vec::new();
        for (i, src) in sources.iter().enumerate() {
            if i == 0 {
                // Primary source: use cap's in= spec
                assign_or_check_node(src, &cap_in_media, &mut node_media, *position)?;
                source_urns.push(cap_in_media.clone());
            } else {
                // Secondary source (fan-in): use existing type if assigned,
                // otherwise use wildcard media: — the orchestrator parser will
                // resolve the real type from the cap's args via registry lookup.
                let secondary_media = node_media.get(src)
                    .cloned()
                    .unwrap_or_else(|| {
                        let wildcard = MediaUrn::from_string("media:").expect("wildcard media URN");
                        node_media.insert(src.to_string(), wildcard.clone());
                        wildcard
                    });
                source_urns.push(secondary_media);
            }
        }

        // Assign target media URN
        assign_or_check_node(target, &cap_out_media, &mut node_media, *position)?;

        edges.push(MachineEdge {
            sources: source_urns,
            cap_urn: cap_urn.clone(),
            target: cap_out_media.clone(),
            is_loop: *is_loop,
        });
    }

    Ok(Machine::new(edges))
}

/// Extract source node names from a source pair (single alias or group).
fn parse_source(pair: pest::iterators::Pair<Rule>) -> Vec<String> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::group => {
            inner.into_inner()
                .filter(|p| p.as_rule() == Rule::alias)
                .map(|p| p.as_str().to_string())
                .collect()
        }
        Rule::alias => {
            vec![inner.as_str().to_string()]
        }
        _ => unreachable!("source is group or alias"),
    }
}

/// Extract is_loop flag and cap alias from a loop_cap pair.
fn parse_loop_cap(pair: pest::iterators::Pair<Rule>) -> (bool, String) {
    let mut is_loop = false;
    let mut cap_alias = String::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::loop_keyword => {
                is_loop = true;
            }
            Rule::alias => {
                cap_alias = inner.as_str().to_string();
            }
            _ => {}
        }
    }

    (is_loop, cap_alias)
}

/// Assign a media URN to a node, or check consistency if already assigned.
///
/// Uses `MediaUrn::is_comparable()` — two types on the same specialization
/// chain are compatible.
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
    } else {
        node_media.insert(node.to_string(), media_urn.clone());
    }
    Ok(())
}

impl Machine {
    /// Parse machine notation into a `Machine`.
    pub fn from_string(input: &str) -> Result<Self, MachineSyntaxError> {
        parse_machine(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap()
    }

    // =========================================================================
    // Empty / whitespace
    // =========================================================================

    #[test]
    fn empty_input() {
        assert!(matches!(
            parse_machine(""),
            Err(MachineSyntaxError::Empty)
        ));
    }

    #[test]
    fn whitespace_only() {
        assert!(matches!(
            parse_machine("   \n  \t  "),
            Err(MachineSyntaxError::Empty)
        ));
    }

    // =========================================================================
    // Header parsing
    // =========================================================================

    #[test]
    fn header_only_no_wirings() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#;
        assert!(matches!(
            Machine::from_string(input),
            Err(MachineSyntaxError::NoEdges)
        ));
    }

    #[test]
    fn duplicate_alias() {
        let input = concat!(
            r#"[ex cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            r#"[ex cap:in="media:pdf";op=summarize;out="media:txt;textable"]"#,
            "[a -> ex -> b]"
        );
        assert!(matches!(
            Machine::from_string(input),
            Err(MachineSyntaxError::DuplicateAlias { .. })
        ));
    }

    // =========================================================================
    // Simple linear chain
    // =========================================================================

    #[test]
    fn simple_linear_chain() {
        let input = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[doc -> extract -> text]"
        );
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 1);

        let edge = &graph.edges()[0];
        assert_eq!(edge.sources.len(), 1);
        assert!(edge.sources[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(edge.target.is_equivalent(&media("media:txt;textable")).unwrap());
        assert!(!edge.is_loop);
    }

    #[test]
    fn two_step_chain() {
        let input = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            r#"[embed cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"]"#,
            "[doc -> extract -> text]",
            "[text -> embed -> vectors]"
        );
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 2);
        assert!(graph.edges()[0].sources[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(graph.edges()[1].target
            .is_equivalent(&media("media:embedding-vector;record;textable"))
            .unwrap());
    }

    // =========================================================================
    // Fan-out
    // =========================================================================

    #[test]
    fn fan_out() {
        let input = concat!(
            r#"[meta cap:in="media:pdf";op=extract_metadata;out="media:file-metadata;record;textable"]"#,
            r#"[outline cap:in="media:pdf";op=extract_outline;out="media:document-outline;record;textable"]"#,
            r#"[thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"]"#,
            "[doc -> meta -> metadata]",
            "[doc -> outline -> outline_data]",
            "[doc -> thumb -> thumbnail]"
        );
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 3);
        for edge in graph.edges() {
            assert_eq!(edge.sources.len(), 1);
            assert!(edge.sources[0].is_equivalent(&media("media:pdf")).unwrap());
        }
    }

    // =========================================================================
    // Fan-in
    // =========================================================================

    #[test]
    fn fan_in_secondary_assigned_by_prior_wiring() {
        let input = concat!(
            r#"[thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"]"#,
            r#"[model_dl cap:in="media:model-spec;textable";op=download;out="media:model-spec;textable"]"#,
            r#"[describe cap:in="media:image;png";op=describe_image;out="media:image-description;textable"]"#,
            "[doc -> thumb -> thumbnail]",
            "[spec_input -> model_dl -> model_spec]",
            "[(thumbnail, model_spec) -> describe -> description]"
        );
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 3);
        assert_eq!(graph.edges()[2].sources.len(), 2);
    }

    #[test]
    fn fan_in_secondary_unassigned_gets_wildcard() {
        // Unassigned secondary sources get wildcard media: at the machine level.
        // The orchestrator parser resolves the real type from cap.args.
        let input = concat!(
            r#"[describe cap:in="media:image;png";op=describe_image;out="media:image-description;textable"]"#,
            "\n[(thumbnail, model_spec) -> describe -> description]"
        );
        let graph = Machine::from_string(input).expect("should parse with wildcard secondary");
        assert_eq!(graph.edges().len(), 1);
        // Secondary source gets wildcard media:
        assert_eq!(graph.edges()[0].sources.len(), 2);
        assert_eq!(graph.edges()[0].sources[0].to_string(), "media:image;png");
        assert_eq!(graph.edges()[0].sources[1].to_string(), "media:");
    }

    // =========================================================================
    // LOOP
    // =========================================================================

    #[test]
    fn loop_edge() {
        let input = concat!(
            r#"[p2t cap:in="media:disbound-page;textable";op=page_to_text;out="media:txt;textable"]"#,
            "[pages -> LOOP p2t -> texts]"
        );
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.edges()[0].is_loop);
    }

    // =========================================================================
    // Undefined alias
    // =========================================================================

    #[test]
    fn undefined_alias_fails() {
        let input = "[doc -> nonexistent -> text]";
        assert!(matches!(
            Machine::from_string(input),
            Err(MachineSyntaxError::UndefinedAlias { alias }) if alias == "nonexistent"
        ));
    }

    // =========================================================================
    // Node-alias collision
    // =========================================================================

    #[test]
    fn node_alias_collision() {
        let input = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[extract -> extract -> text]"
        );
        assert!(matches!(
            Machine::from_string(input),
            Err(MachineSyntaxError::NodeAliasCollision { .. })
        ));
    }

    // =========================================================================
    // Media type consistency
    // =========================================================================

    #[test]
    fn conflicting_media_types_fail() {
        let input = concat!(
            r#"[cap1 cap:in="media:txt;textable";op=a;out="media:pdf"]"#,
            r#"[cap2 cap:in="media:audio;wav";op=b;out="media:txt;textable"]"#,
            "[src -> cap1 -> mid]",
            "[mid -> cap2 -> dst]"
        );
        assert!(matches!(
            Machine::from_string(input),
            Err(MachineSyntaxError::InvalidWiring { .. })
        ));
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    #[test]
    fn multiline_format() {
        let input = r#"
[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
[embed cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"]
[doc -> extract -> text]
[text -> embed -> vectors]
"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 2);
    }

    // =========================================================================
    // Equivalence: different aliases, same graph
    // =========================================================================

    #[test]
    fn different_aliases_same_graph() {
        let input1 = concat!(
            r#"[ex cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[a -> ex -> b]"
        );
        let input2 = concat!(
            r#"[xt cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[x -> xt -> y]"
        );
        let g1 = Machine::from_string(input1).unwrap();
        let g2 = Machine::from_string(input2).unwrap();
        assert!(g1.is_equivalent(&g2));
    }

    // =========================================================================
    // Parse error (malformed input)
    // =========================================================================

    #[test]
    fn malformed_input_fails() {
        let result = parse_machine("not valid machine notation");
        assert!(matches!(result, Err(MachineSyntaxError::ParseError { .. })));
    }

    #[test]
    fn unterminated_bracket_fails() {
        let result = parse_machine("[extract cap:in=media:pdf");
        assert!(matches!(result, Err(MachineSyntaxError::ParseError { .. })));
    }

    // =========================================================================
    // Line-based mode (no brackets)
    // =========================================================================

    #[test]
    fn line_based_simple_chain() {
        let input = r#"extract cap:in="media:pdf";op=extract;out="media:txt;textable"
doc -> extract -> text"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 1);

        let edge = &graph.edges()[0];
        assert_eq!(edge.sources.len(), 1);
        assert!(edge.sources[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(edge.target.is_equivalent(&media("media:txt;textable")).unwrap());
        assert!(!edge.is_loop);
    }

    #[test]
    fn line_based_two_step_chain() {
        let input = r#"extract cap:in="media:pdf";op=extract;out="media:txt;textable"
embed cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"
doc -> extract -> text
text -> embed -> vectors"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 2);
        assert!(graph.edges()[0].sources[0].is_equivalent(&media("media:pdf")).unwrap());
        assert!(graph.edges()[1].target
            .is_equivalent(&media("media:embedding-vector;record;textable"))
            .unwrap());
    }

    #[test]
    fn line_based_loop() {
        let input = r#"p2t cap:in="media:disbound-page;textable";op=page_to_text;out="media:txt;textable"
pages -> LOOP p2t -> texts"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.edges()[0].is_loop);
    }

    #[test]
    fn line_based_fan_in() {
        let input = r#"thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"
model_dl cap:in="media:model-spec;textable";op=download;out="media:model-spec;textable"
describe cap:in="media:image;png";op=describe_image;out="media:image-description;textable"
doc -> thumb -> thumbnail
spec_input -> model_dl -> model_spec
(thumbnail, model_spec) -> describe -> description"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 3);
        assert_eq!(graph.edges()[2].sources.len(), 2);
    }

    #[test]
    fn line_based_fan_out() {
        let input = r#"meta cap:in="media:pdf";op=extract_metadata;out="media:file-metadata;record;textable"
outline cap:in="media:pdf";op=extract_outline;out="media:document-outline;record;textable"
thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"
doc -> meta -> metadata
doc -> outline -> outline_data
doc -> thumb -> thumbnail"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 3);
        for edge in graph.edges() {
            assert_eq!(edge.sources.len(), 1);
            assert!(edge.sources[0].is_equivalent(&media("media:pdf")).unwrap());
        }
    }

    #[test]
    fn mixed_bracketed_and_line_based() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
doc -> extract -> text"#;
        let graph = Machine::from_string(input).unwrap();
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn line_based_equivalent_to_bracketed() {
        let bracketed = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[doc -> extract -> text]"
        );
        let line_based = r#"extract cap:in="media:pdf";op=extract;out="media:txt;textable"
doc -> extract -> text"#;
        let g1 = Machine::from_string(bracketed).unwrap();
        let g2 = Machine::from_string(line_based).unwrap();
        assert!(g1.is_equivalent(&g2));
    }
}
