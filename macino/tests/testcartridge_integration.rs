//! Integration tests for macino using testcartridge
//!
//! These tests verify macino's ability to:
//! 1. Parse and validate DOT graphs with Cap URNs
//! 2. Execute DAGs using testcartridge capabilities
//! 3. Handle data flow between nodes
//! 4. Work with CBOR protocol via PluginHost
//!
//! testcartridge provides simple, predictable test caps without heavy dependencies

use macino::{parse_dot_to_cap_dag, executor::{execute_dag, NodeData}, CapRegistryTrait, ParseOrchestrationError};
use capns::{Cap, CapUrn};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

// =============================================================================
// Mock Registry for testcartridge Caps
// =============================================================================

/// Mock registry that contains testcartridge caps
struct TestcartridgeRegistry {
    caps: HashMap<String, Cap>,
}

impl TestcartridgeRegistry {
    fn new() -> Self {
        let mut caps = HashMap::new();

        // Helper to add a cap
        let mut add_cap = |urn_str: &str| {
            let cap_urn = CapUrn::from_string(urn_str).expect("Invalid test cap URN");
            let cap = Cap {
                urn: cap_urn.clone(),
                title: format!("Test {}", cap_urn.get_tag("op").map_or("unknown", |s| s.as_str())),
                cap_description: None,
                metadata: HashMap::new(),
                command: "testcartridge".to_string(),
                media_specs: vec![],
                args: vec![],
                output: None,
                metadata_json: None,
                registered_by: None,
            };
            caps.insert(cap_urn.to_string(), cap);
        };

        // Register all testcartridge caps
        add_cap(r#"cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable""#);
        add_cap(r#"cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable""#);
        add_cap(r#"cap:in="media:node3;textable";op=test_edge3;out="media:node4;textable;form=list""#);
        add_cap(r#"cap:in="media:node4;textable;form=list";op=test_edge4;out="media:node5;textable""#);
        add_cap(r#"cap:in="media:void";op=test_large;out="media:bytes""#);
        add_cap(r#"cap:in="media:node1;textable";op=test_peer;out="media:node3;textable""#);

        // Add identity cap for cycle testing
        add_cap(r#"cap:in="media:node1;textable";op=identity;out="media:node1;textable""#);

        Self { caps }
    }
}

#[async_trait::async_trait]
impl CapRegistryTrait for TestcartridgeRegistry {
    async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
        // Normalize the URN for lookup
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

// =============================================================================
// Test Helpers
// =============================================================================

/// Get path to testcartridge binary
fn testcartridge_bin() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let bin_path = PathBuf::from(&manifest_dir)
        .parent()
        .expect("No parent dir")
        .join("testcartridge")
        .join("target")
        .join("release")
        .join("testcartridge");

    if !bin_path.exists() {
        panic!("testcartridge binary not found at {:?}. Run: cd ../testcartridge && cargo build --release", bin_path);
    }

    bin_path
}

/// Create a temporary plugin directory for tests
fn setup_test_env() -> (TempDir, PathBuf, Vec<PathBuf>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let plugin_dir = temp_dir.path().join("plugins");
    fs::create_dir_all(&plugin_dir).expect("Failed to create plugin dir");

    // Use testcartridge as dev binary (no registry lookup needed)
    let dev_binaries = vec![testcartridge_bin()];

    (temp_dir, plugin_dir, dev_binaries)
}

// =============================================================================
// Phase 1: Basic macino Functionality with testcartridge
// =============================================================================

// TEST001: Parse simple DOT graph with test-edge1
#[tokio::test]
async fn test001_parse_simple_testcartridge_graph() {
    let registry = TestcartridgeRegistry::new();

    let dot = r#"
        digraph G {
            A -> B [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
        }
    "#;

    let result = parse_dot_to_cap_dag(dot, &registry).await;
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let graph = result.unwrap();
    assert_eq!(graph.nodes.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    assert_eq!(graph.nodes.get("A").unwrap(), "media:node1;textable");
    assert_eq!(graph.nodes.get("B").unwrap(), "media:node2;textable");
}

// TEST002: Execute single-edge DAG (test-edge1)
#[tokio::test]
async fn test002_execute_single_edge_dag() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let dot = r#"
        digraph G {
            input -> output [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
        }
    "#;

    let graph = parse_dot_to_cap_dag(dot, &registry).await.expect("Parse failed");

    // Create initial input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Text("TEST".to_string()));

    // Execute DAG
    let result = execute_dag(
        &graph,
        plugin_dir,
        "https://filegrind.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
    ).await;

    assert!(result.is_ok(), "Execution failed: {:?}", result.err());

    let outputs = result.unwrap();
    let output_data = outputs.get("output").expect("No output node");

    match output_data {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            assert_eq!(output_str, "[PREPEND]TEST");
        }
        _ => panic!("Expected Bytes output, got {:?}", output_data),
    }
}

// TEST003: Execute two-edge chain (test-edge1 → test-edge2)
#[tokio::test]
async fn test003_execute_edge1_to_edge2_chain() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let dot = r#"
        digraph G {
            A -> B [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
            B -> C [label="cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\""];
        }
    "#;

    let graph = parse_dot_to_cap_dag(dot, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("CHAIN".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://filegrind.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
    ).await.expect("Execution failed");

    let final_output = outputs.get("C").expect("No final output");

    match final_output {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            // edge1: [PREPEND]CHAIN, edge2: [PREPEND]CHAIN[APPEND]
            assert_eq!(output_str, "[PREPEND]CHAIN[APPEND]");
        }
        _ => panic!("Expected Bytes output"),
    }
}

// TEST004: Execute with file-path input
#[tokio::test]
async fn test004_execute_with_file_input() {
    let registry = TestcartridgeRegistry::new();
    let (temp, plugin_dir, dev_binaries) = setup_test_env();

    let dot = r#"
        digraph G {
            input -> output [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
        }
    "#;

    let graph = parse_dot_to_cap_dag(dot, &registry).await.expect("Parse failed");

    // Create test input file
    let input_file = temp.path().join("input.txt");
    fs::write(&input_file, "FILE_CONTENT").expect("Failed to write file");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::FilePath(input_file));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://filegrind.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
    ).await.expect("Execution failed");

    let output = outputs.get("output").expect("No output");

    match output {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            assert_eq!(output_str, "[PREPEND]FILE_CONTENT");
        }
        _ => panic!("Expected Bytes output"),
    }
}

// TEST005: Execute large payload (test-large cap)
#[tokio::test]
async fn test005_execute_large_payload() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let dot = r#"
        digraph G {
            input -> output [label="cap:in=\"media:void\";op=test_large;out=\"media:bytes\""];
        }
    "#;

    let graph = parse_dot_to_cap_dag(dot, &registry).await.expect("Parse failed");

    // test-large generates payload based on size, but with media:void input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Bytes(vec![]));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://filegrind.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
    ).await.expect("Execution failed");

    let output = outputs.get("output").expect("No output");

    match output {
        NodeData::Bytes(b) => {
            // Default size is 1MB
            assert_eq!(b.len(), 1_048_576);
            // Verify pattern: repeating 0-255
            for (i, &byte) in b.iter().enumerate() {
                assert_eq!(byte, (i % 256) as u8, "Pattern mismatch at byte {}", i);
            }
        }
        _ => panic!("Expected Bytes output"),
    }
}

// TEST006: Multi-input DAG (fan-in pattern)
#[tokio::test]
async fn test006_fan_in_pattern() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    // Two parallel paths that merge
    let dot = r#"
        digraph G {
            A -> B [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
            C -> D [label="cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\""];
            B -> E [label="cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\""];
            D -> E [label="cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\""];
        }
    "#;

    let graph = parse_dot_to_cap_dag(dot, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("PATH1".to_string()));
    initial_inputs.insert("C".to_string(), NodeData::Text("PATH2".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://filegrind.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
    ).await.expect("Execution failed");

    // Both paths should reach E (one will overwrite the other)
    assert!(outputs.contains_key("E"));

    // Verify intermediate nodes
    let b_output = outputs.get("B").expect("No B output");
    match b_output {
        NodeData::Bytes(b) => {
            let s = String::from_utf8(b.clone()).unwrap();
            assert_eq!(s, "[PREPEND]PATH1");
        }
        _ => panic!("Expected Bytes"),
    }
}

// TEST007: Validate that cycles are rejected
#[tokio::test]
async fn test007_reject_cycles() {
    let registry = TestcartridgeRegistry::new();

    // Create a self-loop using identity cap
    let dot = r#"
        digraph G {
            A -> A [label="cap:in=\"media:node1;textable\";op=identity;out=\"media:node1;textable\""];
        }
    "#;

    let result = parse_dot_to_cap_dag(dot, &registry).await;
    assert!(result.is_err(), "Should reject cycle");

    match result.err() {
        Some(macino::ParseOrchestrationError::NotADag { .. }) => {
            // Expected error
        }
        other => panic!("Expected NotADag error, got: {:?}", other),
    }
}

// TEST008: Empty graph (no edges)
#[tokio::test]
async fn test008_empty_graph() {
    let registry = TestcartridgeRegistry::new();

    let dot = r#"
        digraph G {
            A;
            B;
        }
    "#;

    let result = parse_dot_to_cap_dag(dot, &registry).await;
    assert!(result.is_ok(), "Failed to parse empty graph: {:?}", result.err());

    let graph = result.unwrap();
    assert_eq!(graph.edges.len(), 0);
    // Nodes without caps won't have media URNs derived
    assert!(graph.nodes.is_empty());
}

// TEST009: Invalid cap URN in label
#[tokio::test]
async fn test009_invalid_cap_urn() {
    let registry = TestcartridgeRegistry::new();

    let dot = r#"
        digraph G {
            A -> B [label="cap:INVALID"];
        }
    "#;

    let result = parse_dot_to_cap_dag(dot, &registry).await;
    assert!(result.is_err(), "Should reject invalid cap URN");
}

// TEST010: Cap not found in registry
#[tokio::test]
async fn test010_cap_not_found() {
    let registry = TestcartridgeRegistry::new();

    let dot = r#"
        digraph G {
            A -> B [label="cap:in=\"media:unknown\";op=nonexistent;out=\"media:unknown\""];
        }
    "#;

    let result = parse_dot_to_cap_dag(dot, &registry).await;
    assert!(result.is_err(), "Should fail when cap not found");

    match result.err() {
        Some(macino::ParseOrchestrationError::CapNotFound { .. }) => {
            // Expected
        }
        other => panic!("Expected CapNotFound, got: {:?}", other),
    }
}

// =============================================================================
// Phase 2: Peer Invoke Testing (TEST403)
// =============================================================================

// TEST403: Test peer invoke round-trip (testcartridge calls itself)
#[tokio::test]
async fn test403_peer_invoke_roundtrip() {
    use capns::{PluginHost, CapArgumentValue};
    use capns::local_plugin_router::LocalPluginRouter;
    use tokio::process::Command;
    use std::process::Stdio;
    use std::sync::Arc;

    let testcartridge = testcartridge_bin();

    // Create LocalPluginRouter for routing peer invoke requests
    let router = Arc::new(LocalPluginRouter::new());
    let router_arc: Arc<dyn capns::cap_router::CapRouter> = router.clone();

    // Spawn testcartridge
    let mut child = Command::new(&testcartridge)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn testcartridge");

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();

    // Create host with router
    let host = PluginHost::new_with_router(stdin, stdout, router_arc)
        .await
        .expect("Failed to create host");

    // Get manifest to discover all caps
    let manifest_bytes = host.plugin_manifest();
    let manifest: capns::CapManifest = serde_json::from_slice(manifest_bytes)
        .expect("Failed to parse manifest");

    eprintln!("[TEST403] Discovered {} caps from testcartridge", manifest.caps.len());

    // Register all caps with the router (pointing to this same host)
    let host_arc = Arc::new(host);
    for cap in &manifest.caps {
        let cap_urn = cap.urn.to_string();
        eprintln!("[TEST403] Registering cap: {}", cap_urn);
        router.register_plugin(&cap_urn, Arc::clone(&host_arc)).await;
    }

    // Now call test-peer, which will peer invoke test-edge1 and test-edge2
    let test_peer_urn = r#"cap:in="media:node1;textable";op=test_peer;out="media:node5;textable""#;
    let input_data = b"CHAIN".to_vec();
    let arguments = vec![
        CapArgumentValue::new("media:node1;textable", input_data),
    ];

    eprintln!("[TEST403] Calling test-peer with input: CHAIN");

    let mut response = host_arc
        .request_with_arguments(test_peer_urn, &arguments)
        .await
        .expect("Failed to call test-peer");

    // Collect response chunks
    let mut result_data = Vec::new();
    while let Some(chunk_result) = response.recv().await {
        match chunk_result {
            Ok(chunk) => {
                eprintln!("[TEST403] Received chunk: {} bytes", chunk.payload.len());
                result_data.extend_from_slice(&chunk.payload);
            }
            Err(e) => {
                panic!("Peer invoke failed: {:?}", e);
            }
        }
    }

    // Shutdown host (try_unwrap to get ownership)
    match Arc::try_unwrap(host_arc) {
        Ok(host) => host.shutdown().await,
        Err(_) => eprintln!("[TEST403] Warning: Could not unwrap host Arc, skipping shutdown"),
    }

    // Debug: print raw bytes
    eprintln!("[TEST403] Raw response bytes: {:?}", &result_data[..std::cmp::min(result_data.len(), 30)]);

    // Decode CBOR response
    let cbor_value: ciborium::Value = ciborium::from_reader(&result_data[..])
        .expect("Failed to decode CBOR response");

    eprintln!("[TEST403] Decoded CBOR value: {:?}", cbor_value);

    // Extract bytes from CBOR value
    let result_bytes = match cbor_value {
        ciborium::Value::Bytes(b) => b,
        _ => panic!("Expected CBOR Bytes, got: {:?}", cbor_value),
    };

    let result_str = String::from_utf8(result_bytes)
        .expect("Invalid UTF-8 in result");

    eprintln!("[TEST403] Final result: {}", result_str);

    // Expected flow:
    // 1. test-peer receives "CHAIN"
    // 2. Calls peer.invoke(test-edge1, "CHAIN") → "[PREPEND]CHAIN"
    // 3. Calls peer.invoke(test-edge2, "[PREPEND]CHAIN") → "[PREPEND]CHAIN[APPEND]"
    // 4. Returns final result
    assert_eq!(result_str, "[PREPEND]CHAIN[APPEND]",
        "Peer invoke chain should prepend and append correctly");
}
