//! Machine notation parsing and Cap URN resolution for orchestration
//!
//! Parses machine notation and resolves cap URNs via a registry, validates
//! the graph, and produces a validated, executable DAG IR.

use super::types::{CapRegistryTrait, ParseOrchestrationError, ResolvedEdge, ResolvedGraph};
use super::validation::validate_dag;
use crate::{InputStructure, MediaUrn};
use crate::machine::graph::Machine;
use std::collections::HashMap;

/// Check if two media URNs are on the same specialization chain.
///
/// Returns true if either URN accepts the other, meaning they represent
/// related media types where one may be more specific than the other.
fn media_urns_compatible(a: &MediaUrn, b: &MediaUrn) -> Result<bool, ParseOrchestrationError> {
    a.is_comparable(b)
        .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))
}

/// Check if two media URNs have compatible structures (record/opaque).
fn check_structure_compatibility(
    source: &MediaUrn,
    target: &MediaUrn,
    node_name: &str,
) -> Result<(), ParseOrchestrationError> {
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

/// Parse machine notation and produce a validated orchestration graph.
///
/// Machine notation format (both forms are equally valid):
///
/// ```text
/// extract cap:in="media:pdf";op=extract;out="media:txt;textable"
/// doc -> extract -> text
/// ```
///
/// Each cap URN is resolved via the registry. Node media URNs are derived
/// from the cap's in=/out= specs. Media type consistency and structure
/// compatibility (record vs opaque) are validated at each node.
///
/// # Errors
///
/// Returns `ParseOrchestrationError` for any validation failure.
pub async fn parse_machine_to_cap_dag(
    notation: &str,
    registry: &dyn CapRegistryTrait,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    // Step 1: Parse machine notation into a Machine.
    // This validates syntax, resolves aliases, checks media type consistency,
    // and derives media URNs from cap in/out specs.
    let machine = Machine::from_string(notation)
        .map_err(|e| ParseOrchestrationError::MachineSyntaxParseFailed(format!("{}", e)))?;

    // Step 2: Extract node names from the machine notation.
    // Machine discards node names (they're serialization concerns), but
    // the executor uses them as data-flow keys.
    let wiring_info = extract_wiring_info(notation)?;

    // Validate that wiring count matches edge count. These must align because
    // the machine parser builds edges in wiring statement order.
    if wiring_info.len() != machine.edges().len() {
        return Err(ParseOrchestrationError::MachineSyntaxParseFailed(format!(
            "internal error: {} wirings but {} edges — machine parser edge ordering invariant violated",
            wiring_info.len(),
            machine.edges().len()
        )));
    }

    // Step 3: For each edge in the machine graph, resolve the cap via registry
    // and build ResolvedEdge entries. Validate media type and structure
    // compatibility at every node.
    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();
    let mut resolved_edges = Vec::new();

    for (edge_idx, edge) in machine.edges().iter().enumerate() {
        let cap_urn_str = edge.cap_urn.to_string();
        let cap = registry.lookup(&cap_urn_str).await?;

        let cap_in_media = edge.cap_urn.in_media_urn()
            .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;
        let cap_out_media = edge.cap_urn.out_media_urn()
            .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;

        let wiring = &wiring_info[edge_idx];

        // Build resolved edges — one per source (fan-in produces multiple edges
        // pointing to the same target, matching the executor's expectations)
        for (i, src_name) in wiring.source_names.iter().enumerate() {
            let edge_in_media = if i == 0 {
                // Primary source: use cap's in= spec
                cap_in_media.clone()
            } else {
                // Secondary source (fan-in): resolve from existing assignment
                // or from the cap's args list (e.g., model-spec inputs).
                let existing = node_media.get(src_name);
                let is_wildcard = existing.map_or(false, |m| m.to_string() == "media:");
                if let Some(media) = existing.filter(|_| !is_wildcard) {
                    media.clone()
                } else {
                    // Resolve from cap.args — secondary sources map to args
                    // beyond the primary in= spec (arg index i-1 for source i).
                    let arg_idx = i - 1;
                    let arg_media = cap.args.get(arg_idx).and_then(|arg| {
                        MediaUrn::from_string(&arg.media_urn).ok()
                    });
                    match arg_media {
                        Some(media) => media,
                        None => {
                            return Err(ParseOrchestrationError::MachineSyntaxParseFailed(format!(
                                "fan-in secondary source '{}' (index {}) has no media type and \
                                 cap '{}' has no matching arg at index {}",
                                src_name, i, cap_urn_str, arg_idx
                            )));
                        }
                    }
                }
            };

            // Validate source node media compatibility
            if let Some(existing) = node_media.get(src_name) {
                if !media_urns_compatible(existing, &edge_in_media)? {
                    return Err(ParseOrchestrationError::NodeMediaConflict {
                        node: src_name.clone(),
                        existing: existing.to_string(),
                        required_by_cap: edge_in_media.to_string(),
                    });
                }
                check_structure_compatibility(existing, &edge_in_media, src_name)?;
            } else {
                node_media.insert(src_name.clone(), edge_in_media.clone());
            }

            // Validate target node media compatibility
            if let Some(existing) = node_media.get(&wiring.target_name) {
                if !media_urns_compatible(existing, &cap_out_media)? {
                    return Err(ParseOrchestrationError::NodeMediaConflict {
                        node: wiring.target_name.clone(),
                        existing: existing.to_string(),
                        required_by_cap: cap_out_media.to_string(),
                    });
                }
                check_structure_compatibility(&cap_out_media, existing, &wiring.target_name)?;
            } else {
                node_media.insert(wiring.target_name.clone(), cap_out_media.clone());
            }

            resolved_edges.push(ResolvedEdge {
                from: src_name.clone(),
                to: wiring.target_name.clone(),
                cap_urn: cap_urn_str.clone(),
                cap: cap.clone(),
                in_media: edge_in_media.to_string(),
                out_media: cap_out_media.to_string(),
            });
        }
    }

    // Step 4: DAG validation (cycle detection via topological sort)
    let node_media_strings: HashMap<String, String> = node_media
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    validate_dag(&node_media_strings, &resolved_edges)?;

    Ok(ResolvedGraph {
        nodes: node_media_strings,
        edges: resolved_edges,
        graph_name: None,
    })
}

/// Information about a single wiring statement's node names.
struct WiringInfo {
    source_names: Vec<String>,
    target_name: String,
}

/// Extract wiring node names from machine notation via the pest parser.
///
/// The Machine model intentionally discards alias/node names (they're
/// serialization concerns). But the executor uses node names as data-flow
/// keys. This function extracts them from the wiring statements in order.
fn extract_wiring_info(notation: &str) -> Result<Vec<WiringInfo>, ParseOrchestrationError> {
    use pest::Parser;
    use crate::machine::parser::{MachineParser, Rule};

    let pairs = MachineParser::parse(Rule::program, notation.trim())
        .map_err(|e| ParseOrchestrationError::MachineSyntaxParseFailed(format!("{}", e)))?;

    let mut wirings: Vec<WiringInfo> = Vec::new();

    let program = pairs.into_iter().next().unwrap();

    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue;
        }

        let inner = pair.into_inner().next().unwrap();
        let content = inner.into_inner().next().unwrap();

        if content.as_rule() != Rule::wiring {
            continue; // Skip headers — we only need wiring node names
        }

        let mut inner_pairs = content.into_inner();

        // Parse source (single alias or group)
        let source_pair = inner_pairs.next().unwrap();
        let source_names = match source_pair.as_rule() {
            Rule::source => {
                let source_inner = source_pair.into_inner().next().unwrap();
                match source_inner.as_rule() {
                    Rule::group => {
                        source_inner.into_inner()
                            .filter(|p| p.as_rule() == Rule::alias)
                            .map(|p| p.as_str().to_string())
                            .collect()
                    }
                    Rule::alias => {
                        vec![source_inner.as_str().to_string()]
                    }
                    other => panic!("BUG: source contains unexpected rule {:?}", other),
                }
            }
            other => panic!("BUG: expected source rule, got {:?}", other),
        };

        // Skip arrow
        inner_pairs.next();

        // Skip loop_cap
        inner_pairs.next();

        // Skip arrow
        inner_pairs.next();

        // Target alias
        let target_name = inner_pairs.next().unwrap().as_str().to_string();

        wirings.push(WiringInfo {
            source_names,
            target_name,
        });
    }

    Ok(wirings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cap, CapUrn};
    use std::collections::HashMap;

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

        fn add_cap(&mut self, urn: &str) {
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
            let normalized = CapUrn::from_string(urn)
                .map_err(|e| ParseOrchestrationError::CapUrnParseError(format!("{:?}", e)))?
                .to_string();

            self.caps
                .iter()
                .find(|(k, _)| {
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

    // =========================================================================
    // Simple parsing
    // =========================================================================

    #[tokio::test]
    async fn parse_simple_machine() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#);

        let notation = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            "[A -> extract -> B]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);

        // Verify node media using semantic comparison
        let node_a = MediaUrn::from_string(graph.nodes.get("A").unwrap()).unwrap();
        let expected_a = MediaUrn::from_string("media:pdf").unwrap();
        assert!(node_a.is_equivalent(&expected_a).unwrap(),
            "Node A: expected media:pdf, got {}", node_a);

        let node_b = MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
        let expected_b = MediaUrn::from_string("media:txt;textable").unwrap();
        assert!(node_b.is_equivalent(&expected_b).unwrap(),
            "Node B: expected media:txt;textable, got {}", node_b);
    }

    #[tokio::test]
    async fn parse_two_step_chain() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#);
        registry.add_cap(r#"cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable""#);

        let notation = concat!(
            r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]"#,
            r#"[embed cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"]"#,
            "[A -> extract -> B]",
            "[B -> embed -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.edges.len(), 2);

        // Verify the intermediate node B has the correct media type
        let node_b = MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
        let expected_b = MediaUrn::from_string("media:txt;textable").unwrap();
        assert!(node_b.is_equivalent(&expected_b).unwrap(),
            "Intermediate node B should be media:txt;textable, got {}", node_b);
    }

    // =========================================================================
    // Fan-out: one source, multiple caps
    // =========================================================================

    #[tokio::test]
    async fn parse_fan_out() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:pdf";op=extract_metadata;out="media:file-metadata;record;textable""#);
        registry.add_cap(r#"cap:in="media:pdf";op=extract_outline;out="media:document-outline;record;textable""#);
        registry.add_cap(r#"cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail""#);

        let notation = concat!(
            r#"[meta cap:in="media:pdf";op=extract_metadata;out="media:file-metadata;record;textable"]"#,
            r#"[outline cap:in="media:pdf";op=extract_outline;out="media:document-outline;record;textable"]"#,
            r#"[thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"]"#,
            "[doc -> meta -> metadata]",
            "[doc -> outline -> outline_data]",
            "[doc -> thumb -> thumbnail]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let graph = result.unwrap();
        assert_eq!(graph.nodes.len(), 4); // doc + 3 targets
        assert_eq!(graph.edges.len(), 3);
    }

    // =========================================================================
    // Fan-in: multiple sources to one cap
    // =========================================================================

    #[tokio::test]
    async fn parse_fan_in() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail""#);
        registry.add_cap(r#"cap:in="media:model-spec;textable";op=download;out="media:model-spec;textable""#);
        registry.add_cap(r#"cap:in="media:image;png";op=describe_image;out="media:image-description;textable""#);

        let notation = concat!(
            r#"[thumb cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail"]"#,
            r#"[model_dl cap:in="media:model-spec;textable";op=download;out="media:model-spec;textable"]"#,
            r#"[describe cap:in="media:image;png";op=describe_image;out="media:image-description;textable"]"#,
            "[doc -> thumb -> thumbnail]",
            "[spec_input -> model_dl -> model_spec]",
            "[(thumbnail, model_spec) -> describe -> description]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let graph = result.unwrap();
        // Fan-in produces 2 resolved edges for the describe cap (one per source)
        // plus 2 edges for thumb and model_dl = 4 total
        assert_eq!(graph.edges.len(), 4);
    }

    // =========================================================================
    // LOOP wiring
    // =========================================================================

    #[tokio::test]
    async fn parse_loop_wiring() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:disbound-page;textable";op=page_to_text;out="media:txt;textable""#);

        let notation = concat!(
            r#"[p2t cap:in="media:disbound-page;textable";op=page_to_text;out="media:txt;textable"]"#,
            "[pages -> LOOP p2t -> texts]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());

        let graph = result.unwrap();
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);
    }

    // =========================================================================
    // Cap not found in registry
    // =========================================================================

    #[tokio::test]
    async fn cap_not_found_in_registry() {
        let registry = MockRegistry::new();
        let notation = concat!(
            r#"[ex cap:in="media:unknown";op=test;out="media:unknown"]"#,
            "[A -> ex -> B]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(matches!(result, Err(ParseOrchestrationError::CapNotFound { .. })),
            "Expected CapNotFound, got {:?}", result);
    }

    // =========================================================================
    // Invalid machine notation
    // =========================================================================

    #[tokio::test]
    async fn invalid_machine_notation() {
        let registry = MockRegistry::new();
        let result = parse_machine_to_cap_dag("not valid", &registry).await;
        assert!(matches!(result, Err(ParseOrchestrationError::MachineSyntaxParseFailed(_))),
            "Expected MachineSyntaxParseFailed, got {:?}", result);
    }

    // =========================================================================
    // Cycle detection — NotADag
    // =========================================================================

    #[tokio::test]
    async fn cycle_detection() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:txt;textable";op=process;out="media:txt;textable""#);

        // A -> B -> C -> A creates a cycle
        let notation = concat!(
            r#"[proc cap:in="media:txt;textable";op=process;out="media:txt;textable"]"#,
            "[A -> proc -> B]",
            "[B -> proc -> C]",
            "[C -> proc -> A]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(matches!(result, Err(ParseOrchestrationError::NotADag { .. })),
            "Expected NotADag for cyclic graph, got {:?}", result);
    }

    // =========================================================================
    // Media type conflict at shared node
    // =========================================================================

    #[tokio::test]
    async fn incompatible_media_types_at_shared_node() {
        let mut registry = MockRegistry::new();
        // Cap A outputs media:pdf
        registry.add_cap(r#"cap:in="media:void";op=produce_pdf;out="media:pdf""#);
        // Cap B inputs media:audio;wav — completely incompatible with pdf
        registry.add_cap(r#"cap:in="media:audio;wav";op=transcribe;out="media:txt;textable""#);

        // B is the shared node: cap A says it should be media:pdf,
        // cap B says it should be media:audio;wav. These are incompatible.
        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce_pdf;out="media:pdf"]"#,
            r#"[transcribe cap:in="media:audio;wav";op=transcribe;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> transcribe -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        // Machine notation catches media conflicts during parsing, before orchestrator validation
        assert!(matches!(result, Err(ParseOrchestrationError::MachineSyntaxParseFailed(_))),
            "Expected MachineSyntaxParseFailed for pdf vs audio at shared node, got {:?}", result);
    }

    // =========================================================================
    // Compatible media URNs at shared node (subset/superset)
    // =========================================================================

    #[tokio::test]
    async fn compatible_media_urns_at_shared_node() {
        let mut registry = MockRegistry::new();
        // Cap A outputs media:image;png (less specific)
        registry.add_cap(r#"cap:in="media:pdf";op=thumbnail;out="media:image;png""#);
        // Cap B inputs media:image;png;bytes (more specific, but on same chain)
        registry.add_cap(r#"cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable""#);

        let notation = concat!(
            r#"[thumb cap:in="media:pdf";op=thumbnail;out="media:image;png"]"#,
            r#"[embed_image cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable"]"#,
            "[A -> thumb -> B]",
            "[B -> embed_image -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(),
            "Compatible media URNs (image;png vs image;png;bytes) should not conflict: {:?}",
            result.err());
    }

    // =========================================================================
    // Structure mismatch — record vs opaque
    // =========================================================================

    #[tokio::test]
    async fn structure_mismatch_record_to_opaque() {
        let mut registry = MockRegistry::new();
        // Cap A outputs record
        registry.add_cap(r#"cap:in="media:void";op=produce;out="media:json;record;textable""#);
        // Cap B inputs opaque (no record tag)
        registry.add_cap(r#"cap:in="media:json;textable";op=process;out="media:txt;textable""#);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;record;textable"]"#,
            r#"[process cap:in="media:json;textable";op=process;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> process -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(matches!(result, Err(ParseOrchestrationError::StructureMismatch { .. })),
            "Record to opaque structure mismatch must be detected: {:?}", result);
    }

    // =========================================================================
    // Structure match — both record (should succeed)
    // =========================================================================

    #[tokio::test]
    async fn structure_match_both_record() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:void";op=produce;out="media:json;record;textable""#);
        registry.add_cap(r#"cap:in="media:json;record;textable";op=transform;out="media:result;record;textable""#);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;record;textable"]"#,
            r#"[transform cap:in="media:json;record;textable";op=transform;out="media:result;record;textable"]"#,
            "[A -> produce -> B]",
            "[B -> transform -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(),
            "Record to record should be accepted: {:?}", result.err());
    }

    // =========================================================================
    // Structure match — both opaque (should succeed)
    // =========================================================================

    #[tokio::test]
    async fn structure_match_both_opaque() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:void";op=produce;out="media:json;textable""#);
        registry.add_cap(r#"cap:in="media:json;textable";op=format;out="media:txt;textable""#);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;textable"]"#,
            r#"[format cap:in="media:json;textable";op=format;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> format -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(),
            "Opaque to opaque should be accepted: {:?}", result.err());
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    #[tokio::test]
    async fn parse_multiline_machine() {
        let mut registry = MockRegistry::new();
        registry.add_cap(r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#);

        let notation = r#"
[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
[doc -> extract -> text]
"#;

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(result.is_ok(), "Multi-line parse failed: {:?}", result.err());
    }
}
