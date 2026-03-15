//! Integration tests for capdag orchestrator using testcartridge
//!
//! These tests verify the orchestrator's ability to:
//! 1. Parse and validate route notation graphs with Cap URNs
//! 2. Execute DAGs using testcartridge capabilities
//! 3. Handle data flow between nodes
//! 4. Work with CBOR protocol via PluginHost
//!
//! testcartridge provides simple, predictable test caps without heavy dependencies
//! The testcartridge binary will be auto-built if missing or outdated

use capdag::orchestrator::{parse_route_to_cap_dag, execute_dag, NodeData, CapRegistryTrait, ParseOrchestrationError};
use capdag::{Cap, CapUrn, CapRegistry};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
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
        add_cap(r#"cap:in="media:node3;textable";op=test_edge3;out="media:node4;list;textable""#);
        add_cap(r#"cap:in="media:node4;list;textable";op=test_edge4;out="media:node5;textable""#);
        add_cap(r#"cap:in="media:node3;textable";op=test_edge7;out="media:node6;textable""#);
        add_cap(r#"cap:in="media:node6;textable";op=test_edge8;out="media:node7;textable""#);
        add_cap(r#"cap:in="media:node7;textable";op=test_edge9;out="media:node8;textable""#);
        add_cap(r#"cap:in="media:node8;textable";op=test_edge10;out="media:node1;textable""#);
        add_cap(r#"cap:in="media:void";op=test_large;out="media:""#);
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

/// Get the testcartridge source directory
fn testcartridge_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(&manifest_dir)
        .parent()
        .expect("No parent dir")
        .join("capdag")
        .join("testcartridge")
}

/// Check if testcartridge needs rebuilding
fn testcartridge_needs_rebuild(binary_path: &PathBuf) -> bool {
    let binary_mtime = match binary_path.metadata().and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return true,
    };

    let cart_dir = testcartridge_dir();

    // Check Cargo.toml
    let cargo_toml = cart_dir.join("Cargo.toml");
    if let Ok(meta) = cargo_toml.metadata() {
        if let Ok(mtime) = meta.modified() {
            if mtime > binary_mtime {
                eprintln!("[TestcartridgeTest] Cargo.toml is newer than binary");
                return true;
            }
        }
    }

    // Check src/ directory
    let src_dir = cart_dir.join("src");
    if src_dir.exists() {
        if check_dir_newer(&src_dir, &binary_mtime) {
            eprintln!("[TestcartridgeTest] src/ has files newer than binary");
            return true;
        }
    }

    false
}

/// Check if any file in directory is newer than reference time
fn check_dir_newer(dir: &PathBuf, reference: &std::time::SystemTime) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if check_dir_newer(&path, reference) {
                    return true;
                }
            } else if path.is_file() {
                if let Ok(meta) = path.metadata() {
                    if let Ok(mtime) = meta.modified() {
                        if mtime > *reference {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// Build testcartridge in release mode
fn build_testcartridge() {
    let cart_dir = testcartridge_dir();
    eprintln!("[TestcartridgeTest] Building testcartridge in release mode...");
    eprintln!("[TestcartridgeTest]   Directory: {:?}", cart_dir);
    eprintln!("[TestcartridgeTest]   Running: cargo build --release");

    let output = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .current_dir(&cart_dir)
        .output()
        .expect("Failed to run cargo build for testcartridge");

    // Print stdout if any
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        for line in stdout.lines() {
            eprintln!("[TestcartridgeTest]   {}", line);
        }
    }

    // Print stderr (cargo output goes here)
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        for line in stderr.lines() {
            eprintln!("[TestcartridgeTest]   {}", line);
        }
    }

    if !output.status.success() {
        panic!("Failed to build testcartridge (exit code: {:?})", output.status.code());
    }

    eprintln!("[TestcartridgeTest] Successfully built testcartridge");
}

/// Get path to testcartridge binary, building if necessary
fn testcartridge_bin() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let bin_path = PathBuf::from(&manifest_dir)
        .parent()
        .expect("No parent dir")
        .join("capdag")
        .join("testcartridge")
        .join("target")
        .join("release")
        .join("testcartridge");

    let needs_build = if !bin_path.exists() {
        eprintln!("[TestcartridgeTest] Binary not found, will build");
        true
    } else {
        testcartridge_needs_rebuild(&bin_path)
    };

    if needs_build {
        build_testcartridge();
    }

    if !bin_path.exists() {
        panic!("testcartridge binary not found at {:?} after build attempt", bin_path);
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

/// Create an Arc<CapRegistry> with all test caps for execute_dag
fn create_test_cap_registry() -> Arc<CapRegistry> {
    let registry = CapRegistry::new_for_test();

    // Helper to add a cap
    let add_cap = |urn_str: &str| -> Cap {
        let cap_urn = CapUrn::from_string(urn_str).expect("Invalid test cap URN");
        Cap {
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
        }
    };

    // Register all testcartridge caps
    let caps = vec![
        add_cap(r#"cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable""#),
        add_cap(r#"cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable""#),
        add_cap(r#"cap:in="media:node3;textable";op=test_edge3;out="media:node4;list;textable""#),
        add_cap(r#"cap:in="media:node4;list;textable";op=test_edge4;out="media:node5;textable""#),
        add_cap(r#"cap:in="media:node3;textable";op=test_edge7;out="media:node6;textable""#),
        add_cap(r#"cap:in="media:node6;textable";op=test_edge8;out="media:node7;textable""#),
        add_cap(r#"cap:in="media:node7;textable";op=test_edge9;out="media:node8;textable""#),
        add_cap(r#"cap:in="media:node8;textable";op=test_edge10;out="media:node1;textable""#),
        add_cap(r#"cap:in="media:void";op=test_large;out="media:""#),
        add_cap(r#"cap:in="media:node1;textable";op=test_peer;out="media:node3;textable""#),
        add_cap(r#"cap:in="media:node1;textable";op=identity;out="media:node1;textable""#),
    ];

    registry.add_caps_to_cache(caps);
    Arc::new(registry)
}

// =============================================================================
// Phase 1: Basic macino Functionality with testcartridge
// =============================================================================

// TEST001: Parse simple route notation graph with test-edge1
#[tokio::test]
async fn test935_parse_simple_testcartridge_graph() {
    let registry = TestcartridgeRegistry::new();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[A -> test_edge1 -> B]
"#;

    let result = parse_route_to_cap_dag(route, &registry).await;
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let graph = result.unwrap();
    assert_eq!(graph.nodes.len(), 2);
    assert_eq!(graph.edges.len(), 1);
    let node_a = capdag::MediaUrn::from_string(graph.nodes.get("A").unwrap()).unwrap();
    let expected_a = capdag::MediaUrn::from_string("media:node1;textable").unwrap();
    assert!(node_a.is_equivalent(&expected_a).unwrap());
    let node_b = capdag::MediaUrn::from_string(graph.nodes.get("B").unwrap()).unwrap();
    let expected_b = capdag::MediaUrn::from_string("media:node2;textable").unwrap();
    assert!(node_b.is_equivalent(&expected_b).unwrap());
}

// TEST002: Execute single-edge DAG (test-edge1)
#[tokio::test]
async fn test936_execute_single_edge_dag() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[input -> test_edge1 -> output]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    // Create initial input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Text("TEST".to_string()));

    // Execute DAG
    let cap_registry = create_test_cap_registry();
    let result = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        cap_registry,
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

// TEST003: Execute two-edge chain (test-edge1 -> test-edge2)
#[tokio::test]
async fn test937_execute_edge1_to_edge2_chain() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[test_edge2 cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("CHAIN".to_string()));

    let cap_registry = create_test_cap_registry();
    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        cap_registry,
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
async fn test938_execute_with_file_input() {
    let registry = TestcartridgeRegistry::new();
    let (temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[input -> test_edge1 -> output]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    // Create test input file
    let input_file = temp.path().join("input.txt");
    fs::write(&input_file, "FILE_CONTENT").expect("Failed to write file");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::FilePath(input_file));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
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
async fn test939_execute_large_payload() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_large cap:in="media:void";op=test_large;out="media:"]
[input -> test_large -> output]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    // test-large generates payload based on size, but with media:void input
    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("input".to_string(), NodeData::Bytes(vec![]));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
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
async fn test940_fan_in_pattern() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    // Two parallel paths that merge
    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[test_edge2 cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable"]
[A -> test_edge1 -> B]
[C -> test_edge1 -> D]
[B -> test_edge2 -> E]
[D -> test_edge2 -> E]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("PATH1".to_string()));
    initial_inputs.insert("C".to_string(), NodeData::Text("PATH2".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
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
async fn test941_reject_cycles() {
    let registry = TestcartridgeRegistry::new();

    // Create a self-loop using identity cap
    let route = r#"
[identity cap:in="media:node1;textable";op=identity;out="media:node1;textable"]
[A -> identity -> A]
"#;

    let result = parse_route_to_cap_dag(route, &registry).await;
    assert!(result.is_err(), "Should reject cycle");

    match result.err() {
        Some(ParseOrchestrationError::NotADag { .. }) => {
            // Expected error
        }
        other => panic!("Expected NotADag error, got: {:?}", other),
    }
}

// TEST008: Empty route notation (no edges)
#[tokio::test]
async fn test942_empty_graph() {
    let registry = TestcartridgeRegistry::new();

    let route = "";

    let result = parse_route_to_cap_dag(route, &registry).await;
    assert!(result.is_err(), "Should fail on empty route notation");

    match result.err() {
        Some(ParseOrchestrationError::RouteNotationParseFailed(_)) => {
            // Expected error
        }
        other => panic!("Expected RouteNotationParseFailed, got: {:?}", other),
    }
}

// TEST009: Invalid cap URN in route notation
#[tokio::test]
async fn test943_invalid_cap_urn() {
    let registry = TestcartridgeRegistry::new();

    let route = concat!(
        r#"[bad cap:INVALID]"#,
        "[A -> bad -> B]"
    );

    let result = parse_route_to_cap_dag(route, &registry).await;
    assert!(result.is_err(), "Should reject invalid cap URN");
}

// TEST010: Cap not found in registry
#[tokio::test]
async fn test944_cap_not_found() {
    let registry = TestcartridgeRegistry::new();

    let route = r#"
[nonexistent cap:in="media:unknown";op=nonexistent;out="media:unknown"]
[A -> nonexistent -> B]
"#;

    let result = parse_route_to_cap_dag(route, &registry).await;
    assert!(result.is_err(), "Should fail when cap not found");

    match result.err() {
        Some(ParseOrchestrationError::CapNotFound { .. }) => {
            // Expected
        }
        other => panic!("Expected CapNotFound, got: {:?}", other),
    }
}

// =============================================================================
// Phase 2: Long Chain Tests (4-6 caps)
// =============================================================================

// TEST011: 4-cap chain: edge1 -> edge2 -> edge7 -> edge8
// node1 -> node2 -> node3 -> node6 -> node7
// "hello" -> "[PREPEND]hello" -> "[PREPEND]hello[APPEND]" -> "[PREPEND]HELLO[APPEND]" -> "]DNEPPA[OLLEH]DNEPERP["
#[tokio::test]
async fn test945_four_cap_chain() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[test_edge2 cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable"]
[test_edge7 cap:in="media:node3;textable";op=test_edge7;out="media:node6;textable"]
[test_edge8 cap:in="media:node6;textable";op=test_edge8;out="media:node7;textable"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
    ).await.expect("Execution failed");

    let final_output = outputs.get("E").expect("No final output");

    match final_output {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            // edge1: [PREPEND]hello
            // edge2: [PREPEND]hello[APPEND]
            // edge7 (uppercase): [PREPEND]HELLO[APPEND]
            // edge8 (reverse): ]DNEPPA[OLLEH]DNEPERP[
            assert_eq!(output_str, "]DNEPPA[OLLEH]DNEPERP[");
        }
        _ => panic!("Expected Bytes output"),
    }
}

// TEST012: 5-cap chain: edge1 -> edge2 -> edge7 -> edge8 -> edge9
// node1 -> node2 -> node3 -> node6 -> node7 -> node8
// adds <<...>> wrapping around the reversed string
#[tokio::test]
async fn test946_five_cap_chain() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[test_edge2 cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable"]
[test_edge7 cap:in="media:node3;textable";op=test_edge7;out="media:node6;textable"]
[test_edge8 cap:in="media:node6;textable";op=test_edge8;out="media:node7;textable"]
[test_edge9 cap:in="media:node7;textable";op=test_edge9;out="media:node8;textable"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
[E -> test_edge9 -> F]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
    ).await.expect("Execution failed");

    let final_output = outputs.get("F").expect("No final output");

    match final_output {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            // Previous 4 caps: ]DNEPPA[OLLEH]DNEPERP[
            // edge9 (wrap): <<]DNEPPA[OLLEH]DNEPERP[>>
            assert_eq!(output_str, "<<]DNEPPA[OLLEH]DNEPERP[>>");
        }
        _ => panic!("Expected Bytes output"),
    }
}

// TEST013: 6-cap chain: edge1 -> edge2 -> edge7 -> edge8 -> edge9 -> edge10
// Full cycle: node1 -> node2 -> node3 -> node6 -> node7 -> node8 -> node1
// Completes the round trip: unwrap markers + lowercase
#[tokio::test]
async fn test947_six_cap_chain() {
    let registry = TestcartridgeRegistry::new();
    let (_temp, plugin_dir, dev_binaries) = setup_test_env();

    let route = r#"
[test_edge1 cap:in="media:node1;textable";op=test_edge1;out="media:node2;textable"]
[test_edge2 cap:in="media:node2;textable";op=test_edge2;out="media:node3;textable"]
[test_edge7 cap:in="media:node3;textable";op=test_edge7;out="media:node6;textable"]
[test_edge8 cap:in="media:node6;textable";op=test_edge8;out="media:node7;textable"]
[test_edge9 cap:in="media:node7;textable";op=test_edge9;out="media:node8;textable"]
[test_edge10 cap:in="media:node8;textable";op=test_edge10;out="media:node1;textable"]
[A -> test_edge1 -> B]
[B -> test_edge2 -> C]
[C -> test_edge7 -> D]
[D -> test_edge8 -> E]
[E -> test_edge9 -> F]
[F -> test_edge10 -> G]
"#;

    let graph = parse_route_to_cap_dag(route, &registry).await.expect("Parse failed");

    let mut initial_inputs = HashMap::new();
    initial_inputs.insert("A".to_string(), NodeData::Text("hello".to_string()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        initial_inputs,
        dev_binaries,
        create_test_cap_registry(),
    ).await.expect("Execution failed");

    let final_output = outputs.get("G").expect("No final output");

    match final_output {
        NodeData::Bytes(b) => {
            let output_str = String::from_utf8(b.clone()).expect("Invalid UTF-8");
            // Previous 5 caps: <<]DNEPPA[OLLEH]DNEPERP[>>
            // edge10 (unwrap+lowercase): ]dneppa[olleh]dneperp[
            assert_eq!(output_str, "]dneppa[olleh]dneperp[");
        }
        _ => panic!("Expected Bytes output"),
    }

    // Also verify all intermediate nodes have data
    assert!(outputs.contains_key("B"), "Missing node B (after edge1)");
    assert!(outputs.contains_key("C"), "Missing node C (after edge2)");
    assert!(outputs.contains_key("D"), "Missing node D (after edge7)");
    assert!(outputs.contains_key("E"), "Missing node E (after edge8)");
    assert!(outputs.contains_key("F"), "Missing node F (after edge9)");

    // Verify intermediate values
    if let NodeData::Bytes(b) = outputs.get("B").unwrap() {
        assert_eq!(String::from_utf8(b.clone()).unwrap(), "[PREPEND]hello");
    }
    if let NodeData::Bytes(b) = outputs.get("C").unwrap() {
        assert_eq!(String::from_utf8(b.clone()).unwrap(), "[PREPEND]hello[APPEND]");
    }
    if let NodeData::Bytes(b) = outputs.get("D").unwrap() {
        assert_eq!(String::from_utf8(b.clone()).unwrap(), "[PREPEND]HELLO[APPEND]");
    }
    if let NodeData::Bytes(b) = outputs.get("E").unwrap() {
        assert_eq!(String::from_utf8(b.clone()).unwrap(), "]DNEPPA[OLLEH]DNEPERP[");
    }
    if let NodeData::Bytes(b) = outputs.get("F").unwrap() {
        assert_eq!(String::from_utf8(b.clone()).unwrap(), "<<]DNEPPA[OLLEH]DNEPERP[>>");
    }
}

// =============================================================================
// Phase 3: Peer Invoke Testing (TEST403)
// =============================================================================

// TEST403: Test peer invoke round-trip (testcartridge calls itself)
// Disabled: LocalPluginRouter feature not implemented - uses non-existent modules
#[cfg(feature = "__disabled_local_plugin_router")]
#[tokio::test]
#[ignore]
async fn test403_peer_invoke_roundtrip() {
    use capdag::{PluginHost, CapArgumentValue};
    use capdag::local_plugin_router::LocalPluginRouter;
    use tokio::process::Command;
    use std::process::Stdio;
    use std::sync::Arc;

    let testcartridge = testcartridge_bin();

    // Create LocalPluginRouter for routing peer invoke requests
    let router = Arc::new(LocalPluginRouter::new());
    let router_arc: Arc<dyn capdag::cap_router::CapRouter> = router.clone();

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
    let manifest: capdag::CapManifest = serde_json::from_slice(manifest_bytes)
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
    // 2. Calls peer.invoke(test-edge1, "CHAIN") -> "[PREPEND]CHAIN"
    // 3. Calls peer.invoke(test-edge2, "[PREPEND]CHAIN") -> "[PREPEND]CHAIN[APPEND]"
    // 4. Returns final result
    assert_eq!(result_str, "[PREPEND]CHAIN[APPEND]",
        "Peer invoke chain should prepend and append correctly");
}
