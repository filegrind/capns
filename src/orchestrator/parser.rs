//! DOT graph parsing and Cap URN resolution
//!
//! Parses DOT digraphs and interprets edge labels starting with `cap:` as Cap URNs.
//! Resolves each Cap URN via a CapDag registry, validates the graph, and produces
//! a validated, executable DAG IR.

use super::types::{CapRegistryTrait, ParseOrchestrationError, ResolvedEdge, ResolvedGraph};
use super::validation::validate_dag;
use crate::{Cap, CapUrn, InputStructure, MediaUrn};
use dot_parser::ast::Graph as AstGraph;
use dot_parser::canonical::Graph as CanonicalGraph;
use std::collections::{HashMap, HashSet};

/// Check if two media URN strings are on the same specialization chain.
///
/// Returns true if either URN accepts the other, meaning they represent
/// related media types where one may be more specific than the other.
/// For example, `media:image;png` and `media:image;png;bytes` are compatible
/// because the less-specific one accepts the more-specific one.
fn media_urns_compatible(a: &str, b: &str) -> Result<bool, ParseOrchestrationError> {
    let a_urn = MediaUrn::from_string(a)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;
    let b_urn = MediaUrn::from_string(b)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;
    a_urn
        .is_comparable(&b_urn)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))
}

/// Check if two media URN strings have compatible structures (record/opaque).
///
/// Structure compatibility is strict:
/// - Opaque → Opaque: Compatible
/// - Record → Record: Compatible
/// - Opaque → Record: Incompatible (cannot add structure to opaque data)
/// - Record → Opaque: Incompatible (cannot discard structure from record)
///
/// Returns Ok(()) if compatible, Err with details if not.
fn check_structure_compatibility(
    source_urn: &str,
    target_urn: &str,
    node_name: &str,
) -> Result<(), ParseOrchestrationError> {
    let source = MediaUrn::from_string(source_urn)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;
    let target = MediaUrn::from_string(target_urn)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;

    let source_structure = if source.is_record() {
        InputStructure::Record
    } else {
        InputStructure::Opaque
    };

    let target_structure = if target.is_record() {
        InputStructure::Record
    } else {
        InputStructure::Opaque
    };

    if source_structure != target_structure {
        return Err(ParseOrchestrationError::StructureMismatch {
            node: node_name.to_string(),
            source_structure,
            expected_structure: target_structure,
        });
    }

    Ok(())
}

/// Parse a DOT digraph and produce a validated orchestration graph
///
/// # Arguments
///
/// * `dot` - DOT source code
/// * `registry` - Cap registry for resolving Cap URNs
///
/// # Errors
///
/// Returns `ParseOrchestrationError` for any validation failure
pub async fn parse_dot_to_cap_dag(
    dot: &str,
    registry: &dyn CapRegistryTrait,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    // Step 1: Parse DOT
    let ast = AstGraph::read_dot(dot)
        .map_err(|e| ParseOrchestrationError::DotParseFailed(format!("{:?}", e)))?;

    // Convert to canonical graph for easy edge iteration
    let graph_name = ast.name.map(|s| s.to_string());
    let canonical: CanonicalGraph<(&str, &str)> = ast.into();

    // Step 2: Process node attributes first.
    //
    // Nodes with an explicit `media="..."` attribute declare their actual data type.
    // This takes priority over the cap's in= spec when deriving stream labels.
    // Explicitly-typed nodes are tracked in `attr_nodes` — for these, the edge's
    // `in_media` stream label uses the node's declared type rather than the cap's
    // in= spec. This enables fan-in secondary args (e.g., model_spec alongside the
    // primary image input to a vision cap) to carry the correct stream label.
    let mut node_media: HashMap<String, String> = HashMap::new();
    let mut attr_nodes: HashSet<String> = HashSet::new();
    let mut resolved_edges = Vec::new();

    for (node_id, node) in &canonical.nodes.set {
        let node_id = node_id.to_string();
        if let Some(media_attr_raw) = node
            .attr
            .elems
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("media"))
            .map(|(_, v)| *v)
        {
            let media_attr = if media_attr_raw.starts_with('"') && media_attr_raw.ends_with('"') {
                &media_attr_raw[1..media_attr_raw.len() - 1]
            } else {
                media_attr_raw
            };
            let media_attr = media_attr.replace("\\\"", "\"");
            node_media.insert(node_id.clone(), media_attr);
            attr_nodes.insert(node_id);
        }
    }

    // Step 3: Pre-scan edges to identify fan-in groups (multiple edges to same `to` node).
    // Fan-in secondary args may have types incompatible with the cap's primary in= spec —
    // that's intentional (e.g., model_spec to a vision cap). We skip the compatibility
    // check for explicitly-typed nodes that feed fan-in targets.
    let mut to_edge_count: HashMap<String, usize> = HashMap::new();
    for edge in &canonical.edges.set {
        *to_edge_count.entry(edge.to.to_string()).or_insert(0) += 1;
    }

    // Step 4-5: Process edges and resolve caps
    for edge in &canonical.edges.set {
        let from = edge.from.to_string();
        let to = edge.to.to_string();

        // Extract and validate edge label
        let label = edge
            .attr
            .elems
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("label"))
            .map(|(_, v)| *v)
            .ok_or_else(|| ParseOrchestrationError::EdgeMissingLabel {
                from: from.clone(),
                to: to.clone(),
            })?;

        // DOT parser may return quoted strings - remove outer quotes and unescape
        let label = if label.starts_with('"') && label.ends_with('"') {
            &label[1..label.len() - 1]
        } else {
            label
        };

        // Unescape the label (replace \" with ")
        let label = label.replace("\\\"", "\"");

        // Validate label starts with "cap:"
        if !label.starts_with("cap:") {
            return Err(ParseOrchestrationError::EdgeLabelNotCapUrn {
                from,
                to,
                label: label.to_string(),
            });
        }

        let cap_urn = label.as_str();

        // Resolve Cap URN via registry
        let cap = registry.lookup(cap_urn).await?;

        // Parse the cap URN to extract in/out specs
        let parsed_cap_urn = CapUrn::from_string(cap_urn)
            .map_err(|e| ParseOrchestrationError::CapUrnParseError(format!("{:?}", e)))?;

        let cap_in_media = parsed_cap_urn.in_spec().to_string();
        let cap_out_media = parsed_cap_urn.out_spec().to_string();

        // Determine the stream label for this edge's input.
        //
        // If the `from` node has an explicit media declaration, use that as the
        // stream label — no compatibility check against cap_in_media. The node
        // author declares exactly what type they are providing; the cap handler
        // decides how to consume it. This is needed for fan-in secondary args
        // (e.g., model_spec node providing media:model-spec;textable
        // to a cap whose primary in= spec is media:image;png).
        //
        // If the `from` node has no explicit declaration, derive it from the cap's
        // in= spec as before (with compatibility check against any existing type).
        let edge_in_media = if attr_nodes.contains(&from) {
            let declared = node_media[&from].clone();
            // For single-edge targets (not fan-in), validate compatibility.
            // Fan-in secondary args are allowed to have types incompatible with
            // the cap's primary in= spec — the handler identifies them by label.
            let is_fanin = to_edge_count.get(&to).copied().unwrap_or(1) > 1;
            if !is_fanin {
                if !media_urns_compatible(&declared, &cap_in_media)? {
                    return Err(ParseOrchestrationError::NodeMediaAttrConflict {
                        node: from.clone(),
                        existing: declared.clone(),
                        attr_value: cap_in_media.clone(),
                    });
                }
                // Check structure compatibility (record/opaque must match)
                check_structure_compatibility(&declared, &cap_in_media, &from)?;
            }
            declared
        } else {
            // Implicitly-typed node: use cap's in= spec as stream label.
            if let Some(existing) = node_media.get(&from) {
                if !media_urns_compatible(existing, &cap_in_media)? {
                    return Err(ParseOrchestrationError::NodeMediaConflict {
                        node: from.clone(),
                        existing: existing.clone(),
                        required_by_cap: cap_in_media.clone(),
                    });
                }
                // Check structure compatibility (record/opaque must match)
                check_structure_compatibility(existing, &cap_in_media, &from)?;
            } else {
                node_media.insert(from.clone(), cap_in_media.clone());
            }
            cap_in_media.clone()
        };

        // Check 'to' node output type — use semantic accepts() matching
        if let Some(existing) = node_media.get(&to) {
            if !media_urns_compatible(existing, &cap_out_media)? {
                return Err(ParseOrchestrationError::NodeMediaConflict {
                    node: to.clone(),
                    existing: existing.clone(),
                    required_by_cap: cap_out_media.clone(),
                });
            }
            // Check structure compatibility (record/opaque must match)
            check_structure_compatibility(&cap_out_media, existing, &to)?;
        } else {
            node_media.insert(to.clone(), cap_out_media.clone());
        }

        resolved_edges.push(ResolvedEdge {
            from: from.clone(),
            to: to.clone(),
            cap_urn: cap_urn.to_string(),
            cap,
            in_media: edge_in_media,
            out_media: cap_out_media,
        });
    }

    // Step 6: DAG validation (topological sort to detect cycles)
    validate_dag(&node_media, &resolved_edges)?;

    Ok(ResolvedGraph {
        nodes: node_media,
        edges: resolved_edges,
        graph_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MediaUrn;

    // Mock registry for testing
    struct MockRegistry {
        caps: HashMap<String, Cap>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                caps: HashMap::new(),
            }
        }

        fn add_cap(&mut self, urn: &str, _in_spec: &str, _out_spec: &str) {
            let cap_urn = CapUrn::from_string(urn).unwrap();
            let cap = Cap {
                urn: cap_urn,
                title: "Test Cap".to_string(),
                cap_description: None,
                metadata: HashMap::new(),
                command: "test".to_string(),
                media_specs: vec![],
                args: vec![],
                output: None,
                metadata_json: None,
                registered_by: None,
            };
            self.caps.insert(urn.to_string(), cap);
        }
    }

    #[async_trait::async_trait]
    impl CapRegistryTrait for MockRegistry {
        async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
            // Normalize the URN for lookup
            let normalized = CapUrn::from_string(urn)
                .map_err(|e| ParseOrchestrationError::CapUrnParseError(format!("{:?}", e)))?
                .to_string();

            self.caps
                .iter()
                .find(|(k, _)| {
                    // Try to normalize both keys and compare
                    if let Ok(k_norm) = CapUrn::from_string(k) {
                        k_norm.to_string() == normalized
                    } else {
                        false
                    }
                })
                .map(|(_, v)| v.clone())
                .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                    cap_urn: urn.to_string(),
                })
        }
    }

    // TEST920: Parse valid simple graph with one edge
    #[tokio::test]
    async fn test920_parse_simple_graph() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(result.is_ok());

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        // Compare using MediaUrn equivalence rather than string equality (tag order doesn't matter)
        let node_a = MediaUrn::from_string(graph.nodes.get("A").unwrap()).unwrap();
        let expected_a = MediaUrn::from_string("media:pdf;bytes").unwrap();
        assert!(node_a.is_equivalent(&expected_a).unwrap(), "Node A media URN mismatch: {:?} vs {:?}", node_a, expected_a);
        let node_b = MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
        let expected_b = MediaUrn::from_string("media:txt;textable").unwrap();
        assert!(node_b.is_equivalent(&expected_b).unwrap(), "Node B media URN mismatch: {:?} vs {:?}", node_b, expected_b);
    }

    // TEST921: Fail on edge missing label
    #[tokio::test]
    async fn test921_fail_missing_label() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B;
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::EdgeMissingLabel { .. })
        ));
    }

    // TEST922: Fail on label not starting with cap:
    #[tokio::test]
    async fn test922_fail_label_not_cap_urn() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B [label="some-other-label"];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::EdgeLabelNotCapUrn { .. })
        ));
    }

    // TEST923: Fail on cap not found in registry
    #[tokio::test]
    async fn test923_fail_cap_not_found() {
        let registry = MockRegistry::new();

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:unknown\";op=test;out=\"media:unknown\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::CapNotFound { .. })
        ));
    }

    // TEST924: Fail on node media conflict
    #[tokio::test]
    async fn test924_fail_node_media_conflict() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );
        registry.add_cap(
            r#"cap:in="media:md;textable";op=convert;out="media:html;textable""#,
            "media:md;textable",
            "media:html;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
                A -> C [label="cap:in=\"media:md;textable\";op=convert;out=\"media:html;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NodeMediaConflict { .. })
        ));
    }

    // TEST925: Fail on cycle detection
    #[tokio::test]
    async fn test925_fail_cycle_detection() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:txt;textable";op=process;out="media:txt;textable""#,
            "media:txt;textable",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
                B -> C [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
                C -> A [label="cap:in=\"media:txt;textable\";op=process;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NotADag { .. })
        ));
    }

    // TEST926: Parse graph with media node attributes
    #[tokio::test]
    async fn test926_parse_with_node_media_attributes() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A [media="media:pdf;bytes"];
                B [media="media:txt;textable"];
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(result.is_ok());
    }

    // TEST927: Fail on conflicting media node attribute
    #[tokio::test]
    async fn test927_fail_conflicting_media_attribute() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf;bytes";op=extract;out="media:txt;textable""#,
            "media:pdf;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A [media="media:md;textable"];
                A -> B [label="cap:in=\"media:pdf;bytes\";op=extract;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(matches!(
            result,
            Err(ParseOrchestrationError::NodeMediaAttrConflict { .. })
        ));
    }

    // TEST928: Accept compatible but not identical media URNs at shared node
    // This is the key test for the semantic matching fix: when cap A outputs
    // media:image;png and cap B inputs media:image;png;bytes, the intermediate
    // node should NOT conflict because the less-specific URN accepts the more-specific one.
    #[tokio::test]
    async fn test928_accept_compatible_media_urns() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:pdf";op=thumbnail;out="media:image;png""#,
            "media:pdf",
            "media:image;png",
        );
        registry.add_cap(
            r#"cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable""#,
            "media:image;png;bytes",
            "media:embedding-vector;record;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:pdf\";op=thumbnail;out=\"media:image;png\""];
                B -> C [label="cap:in=\"media:image;png;bytes\";op=embed_image;out=\"media:embedding-vector;record;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(
            result.is_ok(),
            "Compatible media URNs (subset/superset) should not cause NodeMediaConflict: {:?}",
            result.err()
        );

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.edges.len(), 2);
    }

    // TEST929: Reject truly incompatible media URNs at shared node
    // media:pdf;bytes and media:audio;wav have no overlap — neither accepts the other.
    #[tokio::test]
    async fn test929_reject_incompatible_media_urns() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:void";op=produce_pdf;out="media:pdf;bytes""#,
            "media:void",
            "media:pdf;bytes",
        );
        registry.add_cap(
            r#"cap:in="media:audio;wav";op=transcribe;out="media:txt;textable""#,
            "media:audio;wav",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:void\";op=produce_pdf;out=\"media:pdf;bytes\""];
                B -> C [label="cap:in=\"media:audio;wav\";op=transcribe;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(
            matches!(result, Err(ParseOrchestrationError::NodeMediaConflict { .. })),
            "Incompatible media URNs (pdf vs audio) must cause NodeMediaConflict"
        );
    }

    // TEST930: Accept compatible media node attribute (subset/superset)
    #[tokio::test]
    async fn test930_accept_compatible_media_attribute() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:image;png;bytes";op=process;out="media:txt;textable""#,
            "media:image;png;bytes",
            "media:txt;textable",
        );

        let dot = r#"
            digraph G {
                A [media="media:image;png"];
                A -> B [label="cap:in=\"media:image;png;bytes\";op=process;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(
            result.is_ok(),
            "Compatible media attribute (superset of cap input) should be accepted: {:?}",
            result.err()
        );
    }

    // TEST931: Reject structure mismatch - opaque to record
    // A cap expecting record input cannot accept opaque data
    // The mismatch is caught by media_urns_compatible since record is a marker tag
    #[tokio::test]
    async fn test931_reject_opaque_to_record_mismatch() {
        let mut registry = MockRegistry::new();
        // Cap expects record input but produces opaque output
        registry.add_cap(
            r#"cap:in="media:json;record";op=process;out="media:txt;textable""#,
            "media:json;record",
            "media:txt;textable",
        );
        // First cap produces opaque output
        registry.add_cap(
            r#"cap:in="media:void";op=produce;out="media:json;textable""#,
            "media:void",
            "media:json;textable",
        );

        // Chain: void -> opaque json -> record json (structure mismatch!)
        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:void\";op=produce;out=\"media:json;textable\""];
                B -> C [label="cap:in=\"media:json;record\";op=process;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        // Structure mismatch is detected via media URN incompatibility
        assert!(
            matches!(result, Err(ParseOrchestrationError::NodeMediaConflict { .. })),
            "Opaque to record structure mismatch must be detected: {:?}",
            result
        );
    }

    // TEST932: Reject structure mismatch - record to opaque
    // A cap expecting opaque input cannot accept record data
    // The mismatch is caught by check_structure_compatibility
    #[tokio::test]
    async fn test932_reject_record_to_opaque_mismatch() {
        let mut registry = MockRegistry::new();
        // Cap expects opaque input
        registry.add_cap(
            r#"cap:in="media:json;textable";op=process;out="media:txt;textable""#,
            "media:json;textable",
            "media:txt;textable",
        );
        // First cap produces record output
        registry.add_cap(
            r#"cap:in="media:void";op=produce;out="media:json;record;textable""#,
            "media:void",
            "media:json;record;textable",
        );

        // Chain: void -> record json -> opaque json (structure mismatch!)
        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:void\";op=produce;out=\"media:json;record;textable\""];
                B -> C [label="cap:in=\"media:json;textable\";op=process;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        // Structure mismatch is detected by structure compatibility check
        assert!(
            matches!(result, Err(ParseOrchestrationError::StructureMismatch { .. })),
            "Record to opaque structure mismatch must be detected: {:?}",
            result
        );
    }

    // TEST933: Accept matching structures - both opaque
    #[tokio::test]
    async fn test933_accept_opaque_to_opaque() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:json;textable";op=format;out="media:txt;textable""#,
            "media:json;textable",
            "media:txt;textable",
        );
        registry.add_cap(
            r#"cap:in="media:void";op=produce;out="media:json;textable""#,
            "media:void",
            "media:json;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:void\";op=produce;out=\"media:json;textable\""];
                B -> C [label="cap:in=\"media:json;textable\";op=format;out=\"media:txt;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(
            result.is_ok(),
            "Opaque to opaque should be accepted: {:?}",
            result.err()
        );
    }

    // TEST934: Accept matching structures - both record
    #[tokio::test]
    async fn test934_accept_record_to_record() {
        let mut registry = MockRegistry::new();
        registry.add_cap(
            r#"cap:in="media:json;record;textable";op=transform;out="media:result;record;textable""#,
            "media:json;record;textable",
            "media:result;record;textable",
        );
        registry.add_cap(
            r#"cap:in="media:void";op=produce;out="media:json;record;textable""#,
            "media:void",
            "media:json;record;textable",
        );

        let dot = r#"
            digraph G {
                A -> B [label="cap:in=\"media:void\";op=produce;out=\"media:json;record;textable\""];
                B -> C [label="cap:in=\"media:json;record;textable\";op=transform;out=\"media:result;record;textable\""];
            }
        "#;

        let result = parse_dot_to_cap_dag(dot, &registry).await;
        assert!(
            result.is_ok(),
            "Record to record should be accepted: {:?}",
            result.err()
        );
    }
}
