//! testcartridge - Integration test plugin for verifying stream multiplexing protocol
//!
//! Implements all 6 test-edge caps plus special test caps for:
//! - File-path auto-conversion (scalar and list)
//! - Large payload auto-chunking
//! - PeerInvoker protocol verification
//! - Multi-argument handling
//!
//! ## Invocation Modes
//!
//! The cartridge supports two communication modes, automatically detected by PluginRuntime:
//! 1. **Plugin CBOR Mode** (no CLI args): Length-prefixed CBOR frames via stdin/stdout
//! 2. **CLI Mode** (any CLI args): Command-line invocation with args parsed from manifest

use anyhow::Result;
use capns::{
    ArgSource, Cap, CapArg, CapManifest, CapUrn, PluginRuntime,
    OutputStream, Request, WET_KEY_REQUEST,
    Op, OpMetadata, DryContext, WetContext, OpResult, OpError, async_trait,
    find_stream_str, require_stream,
};
use serde_json::json;
use std::sync::Arc;

// =============================================================================
// Manifest Building
// =============================================================================

fn build_manifest() -> CapManifest {
    let mut caps = Vec::new();

    // IDENTITY: Required for every plugin manifest
    let identity_urn = CapUrn::from_string("cap:")
        .expect("Valid identity URN");
    caps.push(Cap::new(
        identity_urn,
        "Identity".to_string(),
        "identity".to_string(),
    ));

    // TEST-EDGE1: Transform node1 to node2 by prepending text
    let edge1_urn = CapUrn::from_string("cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"")
        .expect("Valid edge1 URN");
    let mut edge1 = Cap::with_description(
        edge1_urn,
        "Test Edge 1 (Prepend Transform)".to_string(),
        "test-edge1".to_string(),
        "Transform node1 to node2 by prepending optional text argument".to_string(),
    );
    edge1.add_arg(CapArg::with_description(
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node1;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the input text file".to_string(),
    ));
    let mut prefix_arg = CapArg::with_description(
        "media:edge1arg1;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--prefix".to_string() }],
        "Text to prepend before the input content".to_string(),
    );
    prefix_arg.default_value = Some(json!("[PREPEND]"));
    edge1.add_arg(prefix_arg);
    caps.push(edge1);

    // TEST-EDGE2: Transform node2 to node3 by appending text
    let edge2_urn = CapUrn::from_string("cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"")
        .expect("Valid edge2 URN");
    let mut edge2 = Cap::with_description(
        edge2_urn,
        "Test Edge 2 (Append Transform)".to_string(),
        "test-edge2".to_string(),
        "Transform node2 to node3 by appending optional text argument".to_string(),
    );
    edge2.add_arg(CapArg::with_description(
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node2;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the intermediate text file".to_string(),
    ));
    let mut suffix_arg = CapArg::with_description(
        "media:edge2arg1;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--suffix".to_string() }],
        "Text to append after the input content".to_string(),
    );
    suffix_arg.default_value = Some(json!("[APPEND]"));
    edge2.add_arg(suffix_arg);
    caps.push(edge2);

    // TEST-EDGE3: Transform list of node1 files to list of node4 items
    let edge3_urn = CapUrn::from_string("cap:in=\"media:node1;textable;form=list\";op=test_edge3;out=\"media:node4;textable;form=list\"")
        .expect("Valid edge3 URN");
    let mut edge3 = Cap::with_description(
        edge3_urn,
        "Test Edge 3 (Folder Fan-Out)".to_string(),
        "test-edge3".to_string(),
        "Transform folder of node1 files to list of node4 items".to_string(),
    );
    edge3.add_arg(CapArg::with_description(
        "media:file-path;textable;form=list",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node1;textable;form=list".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Paths to the input text files in folder".to_string(),
    ));
    let mut transform_arg = CapArg::with_description(
        "media:edge3arg1;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--transform".to_string() }],
        "Text to add to each file during fan-out".to_string(),
    );
    transform_arg.default_value = Some(json!("[TRANSFORMED]"));
    edge3.add_arg(transform_arg);
    caps.push(edge3);

    // TEST-EDGE4: Collect list of node4 items into single node5
    let edge4_urn = CapUrn::from_string("cap:in=\"media:node4;textable;form=list\";op=test_edge4;out=\"media:node5;textable\"")
        .expect("Valid edge4 URN");
    let mut edge4 = Cap::with_description(
        edge4_urn,
        "Test Edge 4 (Fan-In Collect)".to_string(),
        "test-edge4".to_string(),
        "Collect list of node4 items into single node5 output".to_string(),
    );
    edge4.add_arg(CapArg::with_description(
        "media:file-path;textable;form=list",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node4;textable;form=list".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "List of text items to collect".to_string(),
    ));
    let mut separator_arg = CapArg::with_description(
        "media:edge4arg1;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--separator".to_string() }],
        "Separator text between collected items".to_string(),
    );
    separator_arg.default_value = Some(json!(" "));
    edge4.add_arg(separator_arg);
    caps.push(edge4);

    // TEST-EDGE5: Merge node2 and node3 into node5
    let edge5_urn = CapUrn::from_string("cap:in=\"media:node2;textable\";in2=\"media:node3;textable\";op=test_edge5;out=\"media:node5;textable\"")
        .expect("Valid edge5 URN");
    let mut edge5 = Cap::with_description(
        edge5_urn,
        "Test Edge 5 (Multi-Input Merge)".to_string(),
        "test-edge5".to_string(),
        "Merge node2 and node3 inputs into single node5 output".to_string(),
    );
    edge5.add_arg(CapArg::with_description(
        "media:file-path;node2;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node2;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the first input text file (node2)".to_string(),
    ));
    // No stdin source — stdin is claimed by the first arg (RULE3).
    // In CBOR mode, the file path passes through as-is (no file-reading).
    edge5.add_arg(CapArg::with_description(
        "media:file-path;node3;textable;form=scalar",
        true,
        vec![
            ArgSource::CliFlag { cli_flag: "--second-input".to_string() },
        ],
        "Path to the second input text file (node3)".to_string(),
    ));
    let mut edge5_separator = CapArg::with_description(
        "media:edge5arg3;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--separator".to_string() }],
        "Separator text between merged inputs".to_string(),
    );
    edge5_separator.default_value = Some(json!(" "));
    edge5.add_arg(edge5_separator);
    caps.push(edge5);

    // TEST-EDGE6: Transform single node1 to list of node4 items
    let edge6_urn = CapUrn::from_string("cap:in=\"media:node1;textable\";op=test_edge6;out=\"media:node4;textable;form=list\"")
        .expect("Valid edge6 URN");
    let mut edge6 = Cap::with_description(
        edge6_urn,
        "Test Edge 6 (Single to List)".to_string(),
        "test-edge6".to_string(),
        "Transform single node1 input to list of node4 items".to_string(),
    );
    edge6.add_arg(CapArg::with_description(
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node1;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the input text file".to_string(),
    ));
    let mut count_arg = CapArg::with_description(
        "media:edge6arg1;textable;numeric;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--count".to_string() }],
        "Number of times to duplicate input in list".to_string(),
    );
    count_arg.default_value = Some(json!(1));
    edge6.add_arg(count_arg);
    let mut item_prefix_arg = CapArg::with_description(
        "media:edge6arg2;textable;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--item-prefix".to_string() }],
        "Prefix to add to each list item".to_string(),
    );
    item_prefix_arg.default_value = Some(json!(""));
    edge6.add_arg(item_prefix_arg);
    caps.push(edge6);

    // TEST-LARGE: Generate large payloads to test auto-chunking
    let large_urn = CapUrn::from_string("cap:in=\"media:void\";op=test_large;out=\"media:\"")
        .expect("Valid large URN");
    let mut large = Cap::with_description(
        large_urn,
        "Test Large Payload".to_string(),
        "test-large".to_string(),
        "Generate large payloads to test auto-chunking".to_string(),
    );
    let mut size_arg = CapArg::with_description(
        "media:payload-size;textable;numeric;form=scalar",
        false,
        vec![ArgSource::CliFlag { cli_flag: "--size".to_string() }],
        "Size of payload in bytes".to_string(),
    );
    size_arg.default_value = Some(json!(1048576)); // 1MB default
    large.add_arg(size_arg);
    caps.push(large);

    // TEST-PEER: Test PeerInvoker by calling edge1 and edge2
    let peer_urn = CapUrn::from_string("cap:in=\"media:node1;textable\";op=test_peer;out=\"media:node5;textable\"")
        .expect("Valid peer URN");
    let mut peer_test = Cap::with_description(
        peer_urn,
        "Test Peer Invoker".to_string(),
        "test-peer".to_string(),
        "Test PeerInvoker by chaining edge1 and edge2 calls".to_string(),
    );
    peer_test.add_arg(CapArg::with_description(
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node1;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the input text file".to_string(),
    ));
    caps.push(peer_test);

    CapManifest::new(
        "testcartridge".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        "Integration test plugin for stream multiplexing protocol verification".to_string(),
        caps,
    )
    .with_author("https://github.com/filegrind".to_string())
    .with_page_url("https://github.com/filegrind/testcartridge".to_string())
}

// =============================================================================
// Helper: collect all input streams by media_urn
// =============================================================================

fn collect_args(req: &Request) -> std::result::Result<Vec<(String, Vec<u8>)>, OpError> {
    req.take_input()
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?
        .collect_streams()
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))
}

fn get_req(wet: &mut WetContext) -> std::result::Result<Arc<Request>, OpError> {
    wet.get_required::<Request>(WET_KEY_REQUEST)
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))
}

fn emit(output: &OutputStream, value: &ciborium::Value) -> OpResult<()> {
    output.emit_cbor(value)
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))
}

// =============================================================================
// Op Implementations
// =============================================================================

#[derive(Default)]
struct Edge1Op;

#[async_trait]
impl Op<()> for Edge1Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input = require_stream(&streams, "media:node1;textable")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let prefix = find_stream_str(&streams, "media:edge1arg1;textable;form=scalar")
            .unwrap_or_else(|| "[PREPEND]".to_string());

        let result = format!("{}{}", prefix, String::from_utf8_lossy(input));
        emit(req.output(), &ciborium::Value::Bytes(result.into_bytes()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge1Op").build() }
}

#[derive(Default)]
struct Edge2Op;

#[async_trait]
impl Op<()> for Edge2Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input = require_stream(&streams, "media:node2;textable")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let suffix = find_stream_str(&streams, "media:edge2arg1;textable;form=scalar")
            .unwrap_or_else(|| "[APPEND]".to_string());

        let result = format!("{}{}", String::from_utf8_lossy(input), suffix);
        emit(req.output(), &ciborium::Value::Bytes(result.into_bytes()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge2Op").build() }
}

#[derive(Default)]
struct Edge3Op;

#[async_trait]
impl Op<()> for Edge3Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input_list = require_stream(&streams, "media:node1;textable;form=list")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let transform = find_stream_str(&streams, "media:edge3arg1;textable;form=scalar")
            .unwrap_or_else(|| "[TRANSFORMED]".to_string());

        let cbor_value: ciborium::Value = ciborium::from_reader(input_list)
            .map_err(|e| OpError::ExecutionFailed(format!("Failed to parse CBOR: {}", e)))?;
        let items = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => return Err(OpError::ExecutionFailed("Expected CBOR array".to_string())),
        };

        let mut results = Vec::new();
        for item in items {
            if let ciborium::Value::Bytes(bytes) = item {
                let transformed = format!("{}{}", transform, String::from_utf8_lossy(&bytes));
                results.push(ciborium::Value::Bytes(transformed.into_bytes()));
            }
        }
        emit(req.output(), &ciborium::Value::Array(results))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge3Op").build() }
}

#[derive(Default)]
struct Edge4Op;

#[async_trait]
impl Op<()> for Edge4Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input_list = require_stream(&streams, "media:node4;textable;form=list")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let separator = find_stream_str(&streams, "media:edge4arg1;textable;form=scalar")
            .unwrap_or_else(|| " ".to_string());

        let cbor_value: ciborium::Value = ciborium::from_reader(input_list)
            .map_err(|e| OpError::ExecutionFailed(format!("Failed to parse CBOR: {}", e)))?;
        let items = match cbor_value {
            ciborium::Value::Array(arr) => arr,
            _ => return Err(OpError::ExecutionFailed("Expected CBOR array".to_string())),
        };

        let parts: Vec<String> = items.iter().filter_map(|item| {
            if let ciborium::Value::Bytes(bytes) = item {
                Some(String::from_utf8_lossy(bytes).to_string())
            } else { None }
        }).collect();
        let result = parts.join(&separator);
        emit(req.output(), &ciborium::Value::Bytes(result.into_bytes()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge4Op").build() }
}

#[derive(Default)]
struct Edge5Op;

#[async_trait]
impl Op<()> for Edge5Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        // First input: file-path arg with stdin source → PluginRuntime read file, relabeled
        let input1 = require_stream(&streams, "media:node2;textable")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        // Second input: file-path arg without stdin source → passed through as file path
        let input2_path = require_stream(&streams, "media:file-path;node3;textable;form=scalar")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let input2 = std::fs::read(String::from_utf8_lossy(input2_path).as_ref())
            .map_err(|e| OpError::ExecutionFailed(format!("Failed to read second input: {}", e)))?;
        let separator = find_stream_str(&streams, "media:edge5arg3;textable;form=scalar")
            .unwrap_or_else(|| " ".to_string());

        let result = format!("{}{}{}", String::from_utf8_lossy(input1), separator, String::from_utf8_lossy(&input2));
        emit(req.output(), &ciborium::Value::Bytes(result.into_bytes()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge5Op").build() }
}

#[derive(Default)]
struct Edge6Op;

#[async_trait]
impl Op<()> for Edge6Op {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input = require_stream(&streams, "media:node1;textable")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        let count = find_stream_str(&streams, "media:edge6arg1;textable;numeric;form=scalar")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let item_prefix = find_stream_str(&streams, "media:edge6arg2;textable;form=scalar")
            .unwrap_or_default();

        let input_str = String::from_utf8_lossy(input);
        let mut results = Vec::new();
        for _ in 0..count {
            let item = format!("{}{}", item_prefix, input_str);
            results.push(ciborium::Value::Bytes(item.into_bytes()));
        }
        emit(req.output(), &ciborium::Value::Array(results))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("Edge6Op").build() }
}

#[derive(Default)]
struct LargeOp;

#[async_trait]
impl Op<()> for LargeOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let size = find_stream_str(&streams, "media:payload-size;textable;numeric;form=scalar")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1_048_576);

        let mut payload = Vec::with_capacity(size);
        for i in 0..size {
            payload.push((i % 256) as u8);
        }
        emit(req.output(), &ciborium::Value::Bytes(payload))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("LargeOp").build() }
}

#[derive(Default)]
struct PeerOp;

#[async_trait]
impl Op<()> for PeerOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let streams = collect_args(&req)?;

        let input = require_stream(&streams, "media:node1;textable")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;

        // Call edge1 via PeerInvoker (node1 → node2)
        let edge1_urn = "cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"";
        let edge1_response = req.peer().call_with_bytes(
            edge1_urn,
            &[("media:node1;textable", input)],
        ).map_err(|e| OpError::ExecutionFailed(e.to_string()))?;

        // Collect edge1 response and decode CBOR
        let edge1_cbor = edge1_response.collect_value()
            .map_err(|e| OpError::ExecutionFailed(format!("Edge1 response error: {}", e)))?;
        let edge1_bytes = match edge1_cbor {
            ciborium::Value::Bytes(b) => b,
            _ => return Err(OpError::ExecutionFailed("Expected Bytes from edge1".to_string())),
        };

        // Call edge2 via PeerInvoker (node2 → node3)
        let edge2_urn = "cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"";
        let edge2_response = req.peer().call_with_bytes(
            edge2_urn,
            &[("media:node2;textable", &edge1_bytes)],
        ).map_err(|e| OpError::ExecutionFailed(e.to_string()))?;

        let edge2_cbor = edge2_response.collect_value()
            .map_err(|e| OpError::ExecutionFailed(format!("Edge2 response error: {}", e)))?;
        emit(req.output(), &edge2_cbor)
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("PeerOp").build() }
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() -> Result<()> {
    let manifest = build_manifest();
    let mut runtime = PluginRuntime::with_manifest(manifest);

    // Register all handlers as Op types
    runtime.register_op_type::<Edge1Op>(
        "cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"",
    );
    runtime.register_op_type::<Edge2Op>(
        "cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"",
    );
    runtime.register_op_type::<Edge3Op>(
        "cap:in=\"media:node1;textable;form=list\";op=test_edge3;out=\"media:node4;textable;form=list\"",
    );
    runtime.register_op_type::<Edge4Op>(
        "cap:in=\"media:node4;textable;form=list\";op=test_edge4;out=\"media:node5;textable\"",
    );
    runtime.register_op_type::<Edge5Op>(
        "cap:in=\"media:node2;textable\";in2=\"media:node3;textable\";op=test_edge5;out=\"media:node5;textable\"",
    );
    runtime.register_op_type::<Edge6Op>(
        "cap:in=\"media:node1;textable\";op=test_edge6;out=\"media:node4;textable;form=list\"",
    );
    runtime.register_op_type::<LargeOp>(
        "cap:in=\"media:void\";op=test_large;out=\"media:\"",
    );
    runtime.register_op_type::<PeerOp>(
        "cap:in=\"media:node1;textable\";op=test_peer;out=\"media:node5;textable\"",
    );

    // Run the plugin runtime (handles both CLI and CBOR modes)
    runtime.run()?;

    Ok(())
}
