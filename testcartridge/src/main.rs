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
    ArgSource, Cap, CapArg, CapManifest, CapUrn, PeerInvoker, PluginRuntime, RuntimeError,
    StreamEmitter,
};
use capns::plugin_runtime::StreamChunk;
use serde_json::json;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;

// =============================================================================
// Manifest Building
// =============================================================================

fn build_manifest() -> CapManifest {
    let mut caps = Vec::new();

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
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node2;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the first input text file (node2)".to_string(),
    ));
    edge5.add_arg(CapArg::with_description(
        "media:file-path;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node3;textable".to_string() },
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
    let large_urn = CapUrn::from_string("cap:in=\"media:void\";op=test_large;out=\"media:bytes\"")
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
// Handler Implementations
// =============================================================================

fn handle_edge1(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    // Collect all argument streams by media_urn
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    // REQUIRED: node1 bytes (file-path already converted)
    let input = args
        .get("media:node1;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node1 input required".to_string()))?;

    // OPTIONAL: prefix
    let prefix = args
        .get("media:edge1arg1;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "[PREPEND]".to_string());

    let input_str = String::from_utf8_lossy(input);
    let result = format!("{}{}", prefix, input_str);

    // Emit as CBOR bytes (required - handlers must always emit CBOR)
    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    let mut output = Vec::new();
    ciborium::into_writer(&cbor_value, &mut output)
        .map_err(|e| RuntimeError::Serialize(format!("CBOR serialization failed: {}", e)))?;

    emitter.emit_bytes(&output);
    Ok(())
}

fn handle_edge2(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let input = args
        .get("media:node2;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node2 input required".to_string()))?;

    let suffix = args
        .get("media:edge2arg1;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "[APPEND]".to_string());

    let input_str = String::from_utf8_lossy(input);
    let result = format!("{}{}", input_str, suffix);

    // Emit as CBOR bytes (required - handlers must always emit CBOR)
    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    let mut output = Vec::new();
    ciborium::into_writer(&cbor_value, &mut output)
        .map_err(|e| RuntimeError::Serialize(format!("CBOR serialization failed: {}", e)))?;

    emitter.emit_bytes(&output);
    Ok(())
}

fn handle_edge3(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    // Input is a list of node1 items (file-path array already expanded and converted)
    let input_list = args
        .get("media:node1;textable;form=list")
        .ok_or_else(|| RuntimeError::MissingArgument("node1 list required".to_string()))?;

    let transform = args
        .get("media:edge3arg1;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "[TRANSFORMED]".to_string());

    // Parse CBOR array
    let mut cursor = std::io::Cursor::new(input_list);
    let cbor_value: ciborium::Value = ciborium::from_reader(&mut cursor)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR: {}", e)))?;

    let items = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => return Err(RuntimeError::Deserialize("Expected CBOR array".to_string())),
    };

    // Transform each item
    let mut results = Vec::new();
    for item in items {
        if let ciborium::Value::Bytes(bytes) = item {
            let content = String::from_utf8_lossy(&bytes);
            let transformed = format!("{}{}", transform, content);
            results.push(ciborium::Value::Bytes(transformed.into_bytes()));
        }
    }

    // Emit as CBOR array
    let result_array = ciborium::Value::Array(results);
    let mut output = Vec::new();
    ciborium::into_writer(&result_array, &mut output)
        .map_err(|e| RuntimeError::Serialize(format!("CBOR serialization failed: {}", e)))?;

    emitter.emit_bytes(&output);
    Ok(())
}

fn handle_edge4(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let input_list = args
        .get("media:node4;textable;form=list")
        .ok_or_else(|| RuntimeError::MissingArgument("node4 list required".to_string()))?;

    let separator = args
        .get("media:edge4arg1;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| " ".to_string());

    // Parse CBOR array
    let mut cursor = std::io::Cursor::new(input_list);
    let cbor_value: ciborium::Value = ciborium::from_reader(&mut cursor)
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to parse CBOR: {}", e)))?;

    let items = match cbor_value {
        ciborium::Value::Array(arr) => arr,
        _ => return Err(RuntimeError::Deserialize("Expected CBOR array".to_string())),
    };

    // Collect all items into single output
    let mut parts = Vec::new();
    for item in items {
        if let ciborium::Value::Bytes(bytes) = item {
            parts.push(String::from_utf8_lossy(&bytes).to_string());
        }
    }

    let result = parts.join(&separator);
    emitter.emit_bytes(result.as_bytes());
    Ok(())
}

fn handle_edge5(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let input1 = args
        .get("media:node2;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node2 input required".to_string()))?;

    let input2 = args
        .get("media:node3;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node3 input required (--second-input)".to_string()))?;

    let separator = args
        .get("media:edge5arg3;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| " ".to_string());

    let input1_str = String::from_utf8_lossy(input1);
    let input2_str = String::from_utf8_lossy(input2);
    let result = format!("{}{}{}", input1_str, separator, input2_str);

    emitter.emit_bytes(result.as_bytes());
    Ok(())
}

fn handle_edge6(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let input = args
        .get("media:node1;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node1 input required".to_string()))?;

    let count = args
        .get("media:edge6arg1;textable;numeric;form=scalar")
        .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok())
        .unwrap_or(1);

    let item_prefix = args
        .get("media:edge6arg2;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "".to_string());

    let input_str = String::from_utf8_lossy(input);

    // Create list with count copies
    let mut results = Vec::new();
    for _ in 0..count {
        let item = format!("{}{}", item_prefix, input_str);
        results.push(ciborium::Value::Bytes(item.into_bytes()));
    }

    // Emit as CBOR array
    let result_array = ciborium::Value::Array(results);
    let mut output = Vec::new();
    ciborium::into_writer(&result_array, &mut output)
        .map_err(|e| RuntimeError::Serialize(format!("CBOR serialization failed: {}", e)))?;

    emitter.emit_bytes(&output);
    Ok(())
}

fn handle_large(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let size = args
        .get("media:payload-size;textable;numeric;form=scalar")
        .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok())
        .unwrap_or(1_048_576); // 1MB default

    // Generate predictable pattern: byte at index i is (i % 256)
    let mut payload = Vec::with_capacity(size);
    for i in 0..size {
        payload.push((i % 256) as u8);
    }

    // Emit as CBOR bytes (required - handlers must always emit CBOR)
    let cbor_value = ciborium::Value::Bytes(payload);
    let mut output = Vec::new();
    ciborium::into_writer(&cbor_value, &mut output)
        .map_err(|e| RuntimeError::Serialize(format!("CBOR serialization failed: {}", e)))?;

    emitter.emit_bytes(&output);
    Ok(())
}

fn handle_peer(
    stream_chunks: Receiver<StreamChunk>,
    emitter: &dyn StreamEmitter,
    peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    for chunk in stream_chunks.iter() {
        args.entry(chunk.media_urn.clone())
            .or_insert_with(Vec::new)
            .extend_from_slice(&chunk.data);
    }

    let input = args
        .get("media:node1;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node1 input required".to_string()))?;

    // Call edge1 via PeerInvoker (node1 → node2)
    let edge1_urn = "cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"";
    let edge1_rx = peer.invoke(edge1_urn, &[
        capns::CapArgumentValue::new("media:node1;textable", input.to_vec())
    ])?;

    // Collect edge1 response chunks
    let mut edge1_response = Vec::new();
    for chunk_result in edge1_rx.iter() {
        let chunk = chunk_result.map_err(|e| RuntimeError::PeerRequest(format!("edge1 failed: {}", e)))?;
        edge1_response.extend_from_slice(&chunk);
    }

    // Call edge2 via PeerInvoker (node2 → node3)
    let edge2_urn = "cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"";
    let edge2_rx = peer.invoke(edge2_urn, &[
        capns::CapArgumentValue::new("media:node2;textable", edge1_response)
    ])?;

    // Collect edge2 response chunks and emit
    for chunk_result in edge2_rx.iter() {
        let chunk = chunk_result.map_err(|e| RuntimeError::PeerRequest(format!("edge2 failed: {}", e)))?;
        emitter.emit_bytes(&chunk);
    }

    Ok(())
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() -> Result<()> {
    let manifest = build_manifest();
    let mut runtime = PluginRuntime::with_manifest(manifest);

    // Register all handlers
    runtime.register_raw(
        "cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"",
        handle_edge1,
    );

    runtime.register_raw(
        "cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"",
        handle_edge2,
    );

    runtime.register_raw(
        "cap:in=\"media:node1;textable;form=list\";op=test_edge3;out=\"media:node4;textable;form=list\"",
        handle_edge3,
    );

    runtime.register_raw(
        "cap:in=\"media:node4;textable;form=list\";op=test_edge4;out=\"media:node5;textable\"",
        handle_edge4,
    );

    runtime.register_raw(
        "cap:in=\"media:node2;textable\";in2=\"media:node3;textable\";op=test_edge5;out=\"media:node5;textable\"",
        handle_edge5,
    );

    runtime.register_raw(
        "cap:in=\"media:node1;textable\";op=test_edge6;out=\"media:node4;textable;form=list\"",
        handle_edge6,
    );

    runtime.register_raw(
        "cap:in=\"media:void\";op=test_large;out=\"media:bytes\"",
        handle_large,
    );

    runtime.register_raw(
        "cap:in=\"media:node1;textable\";op=test_peer;out=\"media:node5;textable\"",
        handle_peer,
    );

    // Run the plugin runtime (handles both CLI and CBOR modes)
    runtime.run()?;

    Ok(())
}
