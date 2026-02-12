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
use capns::cbor_frame::{Frame, FrameType};
use crossbeam_channel::Receiver;
use serde_json::json;
use std::collections::HashMap;

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
        "media:file-path;node2;textable;form=scalar",
        true,
        vec![
            ArgSource::Stdin { stdin: "media:node2;textable".to_string() },
            ArgSource::Position { position: 0 },
        ],
        "Path to the first input text file (node2)".to_string(),
    ));
    edge5.add_arg(CapArg::with_description(
        "media:file-path;node3;textable;form=scalar",
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

/// Helper to collect CBOR frames into a HashMap of media_urn → bytes.
/// Processes STREAM_START, CHUNK, STREAM_END, END frames.
/// Returns error if ERR frame is encountered.
fn collect_args_by_media_urn(frames: Receiver<Frame>) -> Result<HashMap<String, Vec<u8>>, RuntimeError> {
    let mut args: HashMap<String, Vec<u8>> = HashMap::new();
    let mut current_stream: Option<(String, String)> = None; // (stream_id, media_urn)

    for frame in frames {
        match frame.frame_type {
            FrameType::StreamStart => {
                let stream_id = frame.stream_id.ok_or_else(|| {
                    RuntimeError::Handler("STREAM_START missing stream_id".to_string())
                })?;
                let media_urn = frame.media_urn.ok_or_else(|| {
                    RuntimeError::Handler("STREAM_START missing media_urn".to_string())
                })?;
                current_stream = Some((stream_id, media_urn.clone()));
                args.entry(media_urn).or_insert_with(Vec::new);
            }
            FrameType::Chunk => {
                if let Some((_, media_urn)) = &current_stream {
                    if let Some(payload) = frame.payload {
                        args.entry(media_urn.clone())
                            .or_insert_with(Vec::new)
                            .extend_from_slice(&payload);
                    }
                }
            }
            FrameType::StreamEnd => {
                current_stream = None;
            }
            FrameType::End => break,
            FrameType::Err => {
                let code = frame.error_code().unwrap_or("UNKNOWN");
                let message = frame.error_message().unwrap_or("Unknown error");
                return Err(RuntimeError::Handler(format!("[{}] {}", code, message)));
            }
            _ => {} // Ignore other frame types
        }
    }

    Ok(args)
}

/// Helper to collect CHUNK frames from peer response into bytes.
fn collect_peer_response(frames: Receiver<Frame>) -> Result<Vec<u8>, RuntimeError> {
    let mut data = Vec::new();
    for frame in frames {
        match frame.frame_type {
            FrameType::Chunk => {
                if let Some(payload) = frame.payload {
                    data.extend_from_slice(&payload);
                }
            }
            FrameType::End => break,
            FrameType::Err => {
                let code = frame.error_code().unwrap_or("UNKNOWN");
                let message = frame.error_message().unwrap_or("Unknown error");
                return Err(RuntimeError::PeerRequest(format!("[{}] {}", code, message)));
            }
            _ => {} // Ignore STREAM_START, STREAM_END
        }
    }
    Ok(data)
}

fn handle_edge1(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    // Collect all argument streams by media_urn
    let args = collect_args_by_media_urn(frames)?;

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

    // Emit CBOR Value directly (handlers MUST emit CBOR Values)
    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    emitter.emit_cbor(&cbor_value);
    Ok(())
}

fn handle_edge2(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

    let input = args
        .get("media:node2;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node2 input required".to_string()))?;

    eprintln!("[edge2] Input bytes: {:?}", input);
    eprintln!("[edge2] Input as string: {}", String::from_utf8_lossy(input));

    let suffix = args
        .get("media:edge2arg1;textable;form=scalar")
        .map(|b| String::from_utf8_lossy(b).to_string())
        .unwrap_or_else(|| "[APPEND]".to_string());

    let input_str = String::from_utf8_lossy(input);
    let result = format!("{}{}", input_str, suffix);

    eprintln!("[edge2] Result: {}", result);

    // Emit CBOR Value directly (handlers MUST emit CBOR Values)
    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    emitter.emit_cbor(&cbor_value);
    Ok(())
}

fn handle_edge3(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

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

    // Emit CBOR Value directly (handlers MUST emit CBOR Values)
    let result_array = ciborium::Value::Array(results);
    emitter.emit_cbor(&result_array);
    Ok(())
}

fn handle_edge4(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

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
    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    emitter.emit_cbor(&cbor_value);
    Ok(())
}

fn handle_edge5(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

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

    let cbor_value = ciborium::Value::Bytes(result.into_bytes());
    emitter.emit_cbor(&cbor_value);
    Ok(())
}

fn handle_edge6(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

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

    // Emit CBOR Value directly (handlers MUST emit CBOR Values)
    let result_array = ciborium::Value::Array(results);
    emitter.emit_cbor(&result_array);
    Ok(())
}

fn handle_large(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    _peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

    let size = args
        .get("media:payload-size;textable;numeric;form=scalar")
        .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok())
        .unwrap_or(1_048_576); // 1MB default

    // Generate predictable pattern: byte at index i is (i % 256)
    let mut payload = Vec::with_capacity(size);
    for i in 0..size {
        payload.push((i % 256) as u8);
    }

    // Emit CBOR Value directly (handlers MUST emit CBOR Values)
    let cbor_value = ciborium::Value::Bytes(payload);
    emitter.emit_cbor(&cbor_value);
    Ok(())
}

fn handle_peer(
    frames: Receiver<Frame>,
    emitter: &dyn StreamEmitter,
    peer: &dyn PeerInvoker,
) -> Result<(), RuntimeError> {
    let args = collect_args_by_media_urn(frames)?;

    let input = args
        .get("media:node1;textable")
        .ok_or_else(|| RuntimeError::MissingArgument("node1 input required".to_string()))?;

    // Call edge1 via PeerInvoker (node1 → node2)
    let edge1_urn = "cap:in=\"media:node1;textable\";op=test_edge1;out=\"media:node2;textable\"";
    let edge1_rx = peer.invoke(edge1_urn, &[
        capns::CapArgumentValue::new("media:node1;textable", input.to_vec())
    ])?;

    // Collect edge1 response frames (raw CBOR bytes)
    let edge1_response = collect_peer_response(edge1_rx)?;

    // Decode edge1 CBOR response to get raw bytes (point of consumption)
    let edge1_value: ciborium::Value = ciborium::from_reader(&edge1_response[..])
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to decode edge1 response: {}", e)))?;
    let edge1_bytes = match edge1_value {
        ciborium::Value::Bytes(b) => b,
        _ => return Err(RuntimeError::Deserialize("Expected Bytes from edge1".to_string())),
    };

    // Call edge2 via PeerInvoker (node2 → node3)
    let edge2_urn = "cap:in=\"media:node2;textable\";op=test_edge2;out=\"media:node3;textable\"";
    let edge2_rx = peer.invoke(edge2_urn, &[
        capns::CapArgumentValue::new("media:node2;textable", edge1_bytes)
    ])?;

    // Collect edge2 response frames (raw CBOR bytes)
    let response_data = collect_peer_response(edge2_rx)?;

    // Decode and re-emit (point of consumption → point of production)
    let cbor_value: ciborium::Value = ciborium::from_reader(&response_data[..])
        .map_err(|e| RuntimeError::Deserialize(format!("Failed to decode peer response: {}", e)))?;
    emitter.emit_cbor(&cbor_value)?;

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
