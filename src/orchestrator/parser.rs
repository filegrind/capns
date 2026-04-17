//! Machine notation parsing and Cap URN resolution for orchestration.
//!
//! The orchestrator parses machine notation through
//! `parse_machine_with_node_names`, then walks the resolved
//! `Machine`'s strands to build a `ResolvedGraph` keyed on the
//! user's original node names. The resolved Machine carries
//! the source-to-cap-arg assignment (computed by the resolver's
//! Hungarian matching) and the canonical edge order; the
//! orchestrator's job here is to translate those into the
//! edge-centric `ResolvedGraph` shape that the executor consumes.

use super::types::{ParseOrchestrationError, ResolvedEdge, ResolvedGraph};
use crate::cap::registry::CapRegistry;
use crate::machine::{parse_machine_with_node_names, MachineParseError, NodeId, StrandNodeNames};
use crate::{InputStructure, MediaUrn};
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
/// Notation parsing goes through `Machine::from_string` (via
/// `parse_machine_with_node_names`), which performs the
/// resolver's source-to-cap-arg matching, cycle detection, and
/// canonical edge ordering. The orchestrator then walks the
/// resolved strands and translates each cap binding into a
/// `ResolvedEdge`, keying nodes on the user's original node
/// names (preserved by the parser via the per-strand
/// `StrandNodeNames` map).
///
/// # Errors
///
/// Returns `ParseOrchestrationError` for any validation failure.
pub async fn parse_machine_to_cap_dag(
    notation: &str,
    registry: &CapRegistry,
) -> Result<ResolvedGraph, ParseOrchestrationError> {
    // Phase 1: Parse + resolve. The resolver does the
    // syntactic parse, the source-to-cap-arg matching, the
    // cycle detection, and the canonical edge ordering. It
    // also rejects unknown caps. The orchestrator's
    // contributions are: the user node-name keying, the
    // per-binding `ResolvedEdge` shape the executor consumes,
    // and the structure-compatibility check (record vs
    // opaque).
    let (machine, strand_node_names) =
        parse_machine_with_node_names(notation, registry).map_err(translate_machine_parse_error)?;

    // Phase 2: For each strand, build a reverse `NodeId →
    // user node name` map so we can produce `ResolvedEdge`s
    // keyed on names. Then walk the strand's edges and emit
    // one `ResolvedEdge` per binding (cap arg).
    let mut node_media: HashMap<String, MediaUrn> = HashMap::new();
    let mut resolved_edges: Vec<ResolvedEdge> = Vec::new();

    for (strand, name_to_id) in machine.strands().iter().zip(strand_node_names.iter()) {
        let id_to_name = invert_node_names(name_to_id);

        for edge in strand.edges() {
            let cap_urn_str = edge.cap_urn.to_string();
            let cap = registry.get_cached_cap(&cap_urn_str).ok_or_else(|| {
                // The resolver already verified the cap is in
                // the cache; if it isn't reachable here we have
                // a registry race or a programming error.
                ParseOrchestrationError::CapNotFound {
                    cap_urn: cap_urn_str.clone(),
                }
            })?;

            // The cap's declared output URN (`cap.urn.out_spec()`)
            // is the data-type URN of what flows out of this cap
            // on the wire. The target node's URN in the resolved
            // strand is computed by the parser from the same
            // cap.out spec, so they're consistent.
            let cap_out_media = edge
                .cap_urn
                .out_media_urn()
                .map_err(|e| ParseOrchestrationError::MediaUrnParseError(format!("{:?}", e)))?;

            let target_name = lookup_node_name(&id_to_name, edge.target)?;

            // Each binding becomes one ResolvedEdge. The
            // executor expects fan-in caps to surface as
            // multiple parallel edges into the same target.
            //
            // The binding's `cap_arg_media_urn` is the cap's
            // **slot identity** (the arg's outer `media_urn`,
            // e.g. `media:file-path;textable`). At the
            // wire-level the executor labels each input stream
            // with the source's data-type URN — what was
            // declared as the arg's stdin source URN, which
            // equals the source node's resolved URN. We use
            // `strand.node_urn(binding.source)` for both the
            // orchestrator's `node_media` map (the user's view
            // of "what type of data flows here") AND the
            // `ResolvedEdge.in_media` field (the wire URN the
            // executor will tag the stream with).
            for binding in &edge.assignment {
                let source_name = lookup_node_name(&id_to_name, binding.source)?;
                let source_node_urn = strand.node_urn(binding.source).clone();

                // Source node media compatibility check.
                if let Some(existing) = node_media.get(&source_name) {
                    if !media_urns_compatible(existing, &source_node_urn)? {
                        return Err(ParseOrchestrationError::NodeMediaConflict {
                            node: source_name.clone(),
                            existing: existing.to_string(),
                            required_by_cap: source_node_urn.to_string(),
                        });
                    }
                    check_structure_compatibility(existing, &source_node_urn, &source_name)?;
                } else {
                    node_media.insert(source_name.clone(), source_node_urn.clone());
                }

                // Target node media compatibility check.
                if let Some(existing) = node_media.get(&target_name) {
                    if !media_urns_compatible(existing, &cap_out_media)? {
                        return Err(ParseOrchestrationError::NodeMediaConflict {
                            node: target_name.clone(),
                            existing: existing.to_string(),
                            required_by_cap: cap_out_media.to_string(),
                        });
                    }
                    check_structure_compatibility(&cap_out_media, existing, &target_name)?;
                } else {
                    node_media.insert(target_name.clone(), cap_out_media.clone());
                }

                resolved_edges.push(ResolvedEdge {
                    from: source_name,
                    to: target_name.clone(),
                    cap_urn: cap_urn_str.clone(),
                    cap: cap.clone(),
                    in_media: source_node_urn.to_string(),
                    out_media: cap_out_media.to_string(),
                });
            }
        }
    }

    // Cycle detection happens inside the resolver per-strand
    // (Kahn's algorithm over the resolved data-flow NodeId
    // graph). Since the parser pre-interns by user node name,
    // a cycle in the user's name graph is identical to a
    // cycle in the NodeId graph — and since two strands by
    // definition share NO node names (interpretation A:
    // strands are connected components of the user's wiring
    // graph), there is no cross-strand cycle to worry about
    // either. The orchestrator therefore does not run a
    // second cycle pass.
    let node_media_strings: HashMap<String, String> = node_media
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    Ok(ResolvedGraph {
        nodes: node_media_strings,
        edges: resolved_edges,
        graph_name: None,
    })
}

/// Translate a `MachineParseError` from the resolver into the
/// orchestrator's error type. The resolver's cap / cycle /
/// matching failures map onto the orchestrator's existing
/// public error variants so callers see one consistent error
/// surface for "this notation can't be turned into a DAG."
fn translate_machine_parse_error(err: MachineParseError) -> ParseOrchestrationError {
    use crate::machine::MachineAbstractionError;
    match err {
        MachineParseError::Resolution(MachineAbstractionError::UnknownCap { cap_urn }) => {
            ParseOrchestrationError::CapNotFound { cap_urn }
        }
        MachineParseError::Resolution(MachineAbstractionError::CyclicMachineStrand {
            strand_index,
        }) => ParseOrchestrationError::NotADag {
            cycle_nodes: vec![format!("strand {}", strand_index)],
        },
        other => ParseOrchestrationError::MachineSyntaxParseFailed(format!("{}", other)),
    }
}

/// Invert a per-strand `name → NodeId` map into `NodeId → name`.
/// The forward map is built by the parser when allocating
/// `NodeId`s; the inverse is built once per strand here so we
/// can label each binding with its user-written node name.
fn invert_node_names(name_to_id: &StrandNodeNames) -> HashMap<NodeId, String> {
    let mut out = HashMap::with_capacity(name_to_id.len());
    for (name, id) in name_to_id {
        out.insert(*id, name.clone());
    }
    out
}

fn lookup_node_name(
    id_to_name: &HashMap<NodeId, String>,
    id: NodeId,
) -> Result<String, ParseOrchestrationError> {
    id_to_name.get(&id).cloned().ok_or_else(|| {
        ParseOrchestrationError::MachineSyntaxParseFailed(format!(
            "internal error: NodeId {} has no user-written node name",
            id
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
    use crate::cap::registry::CapRegistry;
    use crate::urn::cap_urn::CapUrn;
    use std::collections::HashMap;

    /// Build a `CapRegistry::new_for_test()` populated with the
    /// supplied `(cap_urn, args, out_media_urn)` triples. Each
    /// arg gets a stdin source so the resolver's source-to-arg
    /// matching can find it. The first arg is the primary
    /// (data-flow) input; additional args become fan-in slots.
    fn build_test_registry(caps: &[(&str, &[&str], &str)]) -> CapRegistry {
        let registry = CapRegistry::new_for_test();
        let mut cap_values = Vec::new();
        for (cap_urn_str, args, out_media_urn) in caps {
            let cap_urn = CapUrn::from_string(cap_urn_str)
                .unwrap_or_else(|e| panic!("invalid test cap URN {}: {:?}", cap_urn_str, e));
            let arg_values: Vec<CapArg> = args
                .iter()
                .map(|m| {
                    CapArg::new(
                        m.to_string(),
                        true,
                        vec![ArgSource::Stdin {
                            stdin: m.to_string(),
                        }],
                    )
                })
                .collect();
            cap_values.push(Cap {
                urn: cap_urn,
                title: "Test Cap".to_string(),
                cap_description: None,
                documentation: None,
                metadata: HashMap::new(),
                command: "test".to_string(),
                media_specs: vec![],
                args: arg_values,
                output: Some(CapOutput::new(
                    out_media_urn.to_string(),
                    "Test output".to_string(),
                )),
                metadata_json: None,
                registered_by: None,
            });
        }
        registry.add_caps_to_cache(cap_values);
        registry
    }

    // =========================================================================
    // Simple parsing
    // =========================================================================

    // TEST1256: A single declared cap and one wiring parse into a two-node one-edge DAG.
    #[tokio::test]
    async fn test1256_parse_simple_machine() {
        let registry = build_test_registry(&[(
            r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#,
            &["media:pdf"],
            "media:txt;textable",
        )]);

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
        assert!(
            node_a.is_equivalent(&expected_a).unwrap(),
            "Node A: expected media:pdf, got {}",
            node_a
        );

        let node_b = MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
        let expected_b = MediaUrn::from_string("media:txt;textable").unwrap();
        assert!(
            node_b.is_equivalent(&expected_b).unwrap(),
            "Node B: expected media:txt;textable, got {}",
            node_b
        );
    }

    // TEST1257: Two sequential wirings preserve the intermediate node media type.
    #[tokio::test]
    async fn test1257_parse_two_step_chain() {
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#,
                &["media:pdf"],
                "media:txt;textable",
            ),
            (
                r#"cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable""#,
                &["media:txt;textable"],
                "media:embedding-vector;record;textable",
            ),
        ]);

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
        assert!(
            node_b.is_equivalent(&expected_b).unwrap(),
            "Intermediate node B should be media:txt;textable, got {}",
            node_b
        );
    }

    // =========================================================================
    // Fan-out: one source, multiple caps
    // =========================================================================

    // TEST1258: One source node can fan out into multiple caps and target nodes.
    #[tokio::test]
    async fn test1258_parse_fan_out() {
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:pdf";op=extract_metadata;out="media:file-metadata;record;textable""#,
                &["media:pdf"],
                "media:file-metadata;record;textable",
            ),
            (
                r#"cap:in="media:pdf";op=extract_outline;out="media:document-outline;record;textable""#,
                &["media:pdf"],
                "media:document-outline;record;textable",
            ),
            (
                r#"cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail""#,
                &["media:pdf"],
                "media:image;png;thumbnail",
            ),
        ]);

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

    // TEST1259: Fan-in wiring resolves multiple upstream outputs into one multi-arg cap.
    #[tokio::test]
    async fn test1259_parse_fan_in() {
        // The describe cap has TWO input args: image;png (the
        // primary, declared in= spec) and model-spec;textable
        // (a secondary fan-in input). The resolver's matching
        // assigns each source URN to the right arg slot.
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:pdf";op=generate_thumbnail;out="media:image;png;thumbnail""#,
                &["media:pdf"],
                "media:image;png;thumbnail",
            ),
            (
                r#"cap:in="media:model-spec;textable";op=download;out="media:model-spec;textable""#,
                &["media:model-spec;textable"],
                "media:model-spec;textable",
            ),
            (
                r#"cap:in="media:image;png";op=describe_image;out="media:image-description;textable""#,
                &["media:image;png", "media:model-spec;textable"],
                "media:image-description;textable",
            ),
        ]);

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
        // plus 2 edges for thumb and model_dl = 4 total.
        assert_eq!(graph.edges.len(), 4);
    }

    // =========================================================================
    // LOOP wiring
    // =========================================================================

    // TEST1260: LOOP wiring parses as a single edge while preserving the loop marker semantics.
    #[tokio::test]
    async fn test1260_parse_loop_wiring() {
        let registry = build_test_registry(&[(
            r#"cap:in="media:disbound-page;textable";op=page_to_text;out="media:txt;textable""#,
            &["media:disbound-page;textable"],
            "media:txt;textable",
        )]);

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

    // TEST1261: Parsing fails with CapNotFound when a declared cap is absent from the registry.
    #[tokio::test]
    async fn test1261_cap_not_found_in_registry() {
        let registry = build_test_registry(&[]);
        let notation = concat!(
            r#"[ex cap:in="media:unknown";op=test;out="media:unknown"]"#,
            "[A -> ex -> B]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            matches!(result, Err(ParseOrchestrationError::CapNotFound { .. })),
            "Expected CapNotFound, got {:?}",
            result
        );
    }

    // =========================================================================
    // Invalid machine notation
    // =========================================================================

    // TEST1262: Non-machine text fails with a machine syntax parse error.
    #[tokio::test]
    async fn test1262_invalid_machine_notation() {
        let registry = build_test_registry(&[]);
        let result = parse_machine_to_cap_dag("not valid", &registry).await;
        assert!(
            matches!(
                result,
                Err(ParseOrchestrationError::MachineSyntaxParseFailed(_))
            ),
            "Expected MachineSyntaxParseFailed, got {:?}",
            result
        );
    }

    // =========================================================================
    // Cycle detection — NotADag
    // =========================================================================

    // TEST1263: Cyclic wirings are rejected as non-DAG orchestrations.
    #[tokio::test]
    async fn test1263_cycle_detection() {
        let registry = build_test_registry(&[(
            r#"cap:in="media:txt;textable";op=process;out="media:txt;textable""#,
            &["media:txt;textable"],
            "media:txt;textable",
        )]);

        // A -> B -> C -> A creates a cycle (three wirings
        // sharing nodes form one connected component → one
        // strand → resolver detects the cycle).
        let notation = concat!(
            r#"[proc cap:in="media:txt;textable";op=process;out="media:txt;textable"]"#,
            "[A -> proc -> B]",
            "[B -> proc -> C]",
            "[C -> proc -> A]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            matches!(result, Err(ParseOrchestrationError::NotADag { .. })),
            "Expected NotADag for cyclic graph, got {:?}",
            result
        );
    }

    // =========================================================================
    // Media type conflict at shared node
    // =========================================================================

    // TEST1264: Shared nodes with incompatible upstream and downstream media fail during parsing.
    #[tokio::test]
    async fn test1264_incompatible_media_types_at_shared_node() {
        // Cap A outputs media:pdf; cap B inputs media:audio;wav.
        // These are completely incompatible at the shared node B,
        // and the parser's lexical assign-or-check-node step
        // catches it via `is_comparable`.
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:void";op=produce_pdf;out="media:pdf""#,
                &["media:void"],
                "media:pdf",
            ),
            (
                r#"cap:in="media:audio;wav";op=transcribe;out="media:txt;textable""#,
                &["media:audio;wav"],
                "media:txt;textable",
            ),
        ]);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce_pdf;out="media:pdf"]"#,
            r#"[transcribe cap:in="media:audio;wav";op=transcribe;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> transcribe -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            matches!(
                result,
                Err(ParseOrchestrationError::MachineSyntaxParseFailed(_))
            ),
            "Expected MachineSyntaxParseFailed for pdf vs audio at shared node, got {:?}",
            result
        );
    }

    // =========================================================================
    // Compatible media URNs at shared node (subset/superset)
    // =========================================================================

    // TEST1265: Shared nodes accept compatible media URNs when one is a more specific form of the other.
    #[tokio::test]
    async fn test1265_compatible_media_urns_at_shared_node() {
        // Cap A outputs media:image;png; cap B inputs
        // media:image;png;bytes. The parser's lexical
        // is_comparable accepts the chain (bytes is more
        // specific). The resolver's matching then assigns the
        // image;png;bytes source URN (held at the shared node)
        // to cap B's image;png;bytes arg slot.
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:pdf";op=thumbnail;out="media:image;png""#,
                &["media:pdf"],
                "media:image;png",
            ),
            (
                r#"cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable""#,
                &["media:image;png;bytes"],
                "media:embedding-vector;record;textable",
            ),
        ]);

        let notation = concat!(
            r#"[thumb cap:in="media:pdf";op=thumbnail;out="media:image;png"]"#,
            r#"[embed_image cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable"]"#,
            "[A -> thumb -> B]",
            "[B -> embed_image -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            result.is_ok(),
            "Compatible media URNs (image;png vs image;png;bytes) should not conflict: {:?}",
            result.err()
        );
    }

    // =========================================================================
    // Structure mismatch — record vs opaque
    // =========================================================================

    // TEST1266: Record-to-opaque structure mismatches are rejected once structure checking is enabled.
    #[tokio::test]
    #[ignore = "structure mismatch detection between node media and cap input not yet implemented"]
    async fn test1266_structure_mismatch_record_to_opaque() {
        // Cap A outputs record (media:json;record;textable),
        // cap B inputs opaque (media:json;textable, no record).
        // The parser's lexical is_comparable check passes
        // because both URNs are on the same `textable` chain
        // (one with `record`, one without — `record` is the
        // additional tag). The orchestrator's
        // structure-compatibility check is what catches the
        // mismatch.
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:void";op=produce;out="media:json;record;textable""#,
                &["media:void"],
                "media:json;record;textable",
            ),
            (
                r#"cap:in="media:json;textable";op=process;out="media:txt;textable""#,
                &["media:json;textable"],
                "media:txt;textable",
            ),
        ]);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;record;textable"]"#,
            r#"[process cap:in="media:json;textable";op=process;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> process -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            matches!(
                result,
                Err(ParseOrchestrationError::StructureMismatch { .. })
            ),
            "Record to opaque structure mismatch must be detected: {:?}",
            result
        );
    }

    // =========================================================================
    // Structure match — both record (should succeed)
    // =========================================================================

    // TEST1267: Record-shaped outputs can feed record-shaped inputs without error.
    #[tokio::test]
    async fn test1267_structure_match_both_record() {
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:void";op=produce;out="media:json;record;textable""#,
                &["media:void"],
                "media:json;record;textable",
            ),
            (
                r#"cap:in="media:json;record;textable";op=transform;out="media:result;record;textable""#,
                &["media:json;record;textable"],
                "media:result;record;textable",
            ),
        ]);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;record;textable"]"#,
            r#"[transform cap:in="media:json;record;textable";op=transform;out="media:result;record;textable"]"#,
            "[A -> produce -> B]",
            "[B -> transform -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            result.is_ok(),
            "Record to record should be accepted: {:?}",
            result.err()
        );
    }

    // =========================================================================
    // Structure match — both opaque (should succeed)
    // =========================================================================

    // TEST1268: Opaque outputs can feed opaque inputs without triggering structure conflicts.
    #[tokio::test]
    async fn test1268_structure_match_both_opaque() {
        let registry = build_test_registry(&[
            (
                r#"cap:in="media:void";op=produce;out="media:json;textable""#,
                &["media:void"],
                "media:json;textable",
            ),
            (
                r#"cap:in="media:json;textable";op=format;out="media:txt;textable""#,
                &["media:json;textable"],
                "media:txt;textable",
            ),
        ]);

        let notation = concat!(
            r#"[produce cap:in="media:void";op=produce;out="media:json;textable"]"#,
            r#"[format cap:in="media:json;textable";op=format;out="media:txt;textable"]"#,
            "[A -> produce -> B]",
            "[B -> format -> C]"
        );

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            result.is_ok(),
            "Opaque to opaque should be accepted: {:?}",
            result.err()
        );
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    // TEST1269: Multi-line machine notation parses successfully with the same semantics as inline notation.
    #[tokio::test]
    async fn test1269_parse_multiline_machine() {
        let registry = build_test_registry(&[(
            r#"cap:in="media:pdf";op=extract;out="media:txt;textable""#,
            &["media:pdf"],
            "media:txt;textable",
        )]);

        let notation = r#"
[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
[doc -> extract -> text]
"#;

        let result = parse_machine_to_cap_dag(notation, &registry).await;
        assert!(
            result.is_ok(),
            "Multi-line parse failed: {:?}",
            result.err()
        );
    }
}
