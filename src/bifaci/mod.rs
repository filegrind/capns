//! Bifaci — Binary Frame Cap Invocation protocol
//!
//! Three-layer architecture:
//! - **Router** (`relay_switch`): (RelaySwitch + RelayMaster × N)
//! - **Host × N** (`host_runtime`, `relay`): (RelaySlave + PluginHostRuntime)
//! - **Plugin × N** (`plugin_runtime`): (PluginRuntime + handler × N)

pub mod frame;
pub mod io;
pub mod manifest;
pub mod router;
pub mod plugin_runtime;
pub mod host_runtime;
pub mod relay;
pub mod relay_switch;
pub mod in_process_host;
pub mod plugin_repo;

#[cfg(test)]
mod integration_tests;

/// CBOR-decode a response chunk payload to extract raw bytes.
///
/// Converts any CBOR value to its byte representation:
/// - Bytes: raw binary data (returned as-is)
/// - Text: UTF-8 bytes (e.g., JSON/NDJSON content)
/// - Integer: decimal string representation as bytes
/// - Float: decimal string representation as bytes
/// - Bool: "true" or "false" as bytes
/// - Null: empty vec
/// - Array/Map: not supported (returns None)
/// - Tagged: unwraps and decodes inner value
///
/// Returns `None` if the payload is not valid CBOR or contains an unsupported type.
pub fn decode_chunk_payload(payload: &[u8]) -> Option<Vec<u8>> {
	let value: ciborium::Value = ciborium::from_reader(payload).ok()?;
	decode_cbor_value(value)
}

/// Convert a CBOR Value to bytes
fn decode_cbor_value(value: ciborium::Value) -> Option<Vec<u8>> {
	match value {
		ciborium::Value::Bytes(b) => Some(b),
		ciborium::Value::Text(s) => Some(s.into_bytes()),
		ciborium::Value::Integer(i) => {
			let n: i128 = i.into();
			Some(n.to_string().into_bytes())
		}
		ciborium::Value::Float(f) => Some(f.to_string().into_bytes()),
		ciborium::Value::Bool(b) => Some(if b { b"true".to_vec() } else { b"false".to_vec() }),
		ciborium::Value::Null => Some(Vec::new()),
		ciborium::Value::Tag(_tag, boxed) => decode_cbor_value(*boxed),
		// Array and Map are not directly convertible to bytes
		ciborium::Value::Array(_) | ciborium::Value::Map(_) => None,
		_ => None,
	}
}