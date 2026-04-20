//! Shared stream I/O operations for cap execution.
//!
//! These functions handle the bifaci protocol's CBOR transport layer:
//! sending input streams to cartridges and collecting/decoding their
//! responses. Used by both the machfab engine (capdag_service) and
//! the capdag CLI orchestrator executor.
//!
//! The key invariant: node data between caps is stored as raw bytes
//! (unwrapped from CBOR transport). Sequence-mode output is stored
//! as an RFC 8742 CBOR sequence where each item's CBOR Bytes/Text
//! wrapper has been unwrapped to raw bytes, then re-encoded as
//! CBOR Bytes for self-delimiting boundaries.

use crate::bifaci::frame::{Frame, FrameType, MessageId};
use crate::bifaci::relay_switch::RelaySwitch;
use crate::StreamMeta;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamIoError {
    #[error("Stream I/O error: {0}")]
    Transport(String),

    #[error("CBOR encoding error: {0}")]
    CborEncode(String),

    #[error("CBOR decoding error: {0}")]
    CborDecode(String),

    #[error("Protocol error: expected Bytes or Text in CBOR transport at item {index}, got {description}")]
    UnexpectedCborType { index: usize, description: String },
}

/// Send a single input stream (STREAM_START → CHUNKs → STREAM_END) to a cartridge.
///
/// Handles both scalar and sequence mode:
/// - Scalar (`is_sequence=false`): wraps each chunk in `CBOR::Bytes`
/// - Sequence (`is_sequence=true`): sends raw CBOR item bytes directly
///   (matching `emit_list_item` semantics on the cartridge side)
pub async fn send_one_stream(
    switch: &Arc<RelaySwitch>,
    rid: &MessageId,
    media_urn: &str,
    data: &[u8],
    meta: Option<StreamMeta>,
    is_sequence: bool,
    max_chunk: usize,
) -> Result<(), StreamIoError> {
    let stream_id = uuid::Uuid::new_v4().to_string();

    let mut ss = Frame::stream_start(
        rid.clone(),
        stream_id.clone(),
        media_urn.to_string(),
        if is_sequence { Some(true) } else { None },
    );
    ss.meta = meta;
    switch
        .send_to_master(ss, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_START: {}", e)))?;

    let mut chunk_index = 0u64;

    if is_sequence {
        // Sequence mode: data is an RFC 8742 CBOR sequence.
        // Each self-delimiting CBOR value is sent as a separate chunk
        // payload. The chunk payload IS the raw CBOR bytes of the item
        // (not re-wrapped).
        if !data.is_empty() {
            let mut cursor = std::io::Cursor::new(data);
            while (cursor.position() as usize) < data.len() {
                let start_pos = cursor.position() as usize;
                let _value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                    StreamIoError::CborDecode(format!("sequence item {}: {}", chunk_index, e))
                })?;
                let end_pos = cursor.position() as usize;
                let item_cbor = &data[start_pos..end_pos];

                let checksum = Frame::compute_checksum(item_cbor);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    item_cbor.to_vec(),
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                chunk_index += 1;
            }
        }
    } else {
        // Scalar mode: data is raw bytes, wrapped as CBOR::Bytes per chunk.
        if data.is_empty() {
            let cbor_value = ciborium::Value::Bytes(vec![]);
            let mut cbor_payload = Vec::new();
            ciborium::into_writer(&cbor_value, &mut cbor_payload)
                .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
            let checksum = Frame::compute_checksum(&cbor_payload);
            let chunk =
                Frame::chunk(rid.clone(), stream_id.clone(), 0, cbor_payload, 0, checksum);
            switch
                .send_to_master(chunk, None)
                .await
                .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
            chunk_index = 1;
        } else {
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + max_chunk).min(data.len());
                let chunk_data = &data[offset..end];
                let cbor_value = ciborium::Value::Bytes(chunk_data.to_vec());
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(&cbor_value, &mut cbor_payload)
                    .map_err(|e| StreamIoError::CborEncode(format!("{}", e)))?;
                let checksum = Frame::compute_checksum(&cbor_payload);
                let chunk = Frame::chunk(
                    rid.clone(),
                    stream_id.clone(),
                    chunk_index,
                    cbor_payload,
                    chunk_index,
                    checksum,
                );
                switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| StreamIoError::Transport(format!("CHUNK: {}", e)))?;
                offset = end;
                chunk_index += 1;
            }
        }
    }

    let se = Frame::stream_end(rid.clone(), stream_id, chunk_index);
    switch
        .send_to_master(se, None)
        .await
        .map_err(|e| StreamIoError::Transport(format!("STREAM_END: {}", e)))?;

    Ok(())
}

/// Decode terminal output bytes based on is_sequence flag.
///
/// Returns `Vec<Vec<u8>>` — a list of unwrapped items:
/// - `is_sequence=true` (emit_list_item): each CBOR value in the
///   sequence is unwrapped (Bytes→raw, Text→UTF-8) into a separate item.
/// - `is_sequence=false/None` (write/emit_cbor): CBOR Bytes/Text
///   wrappers are unwrapped and concatenated into a single item.
pub fn decode_terminal_output(
    response_chunks: &[u8],
    is_sequence: Option<bool>,
) -> Result<Vec<Vec<u8>>, StreamIoError> {
    if response_chunks.is_empty() {
        return Ok(vec![vec![]]);
    }

    if is_sequence == Some(true) {
        let mut items: Vec<Vec<u8>> = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                StreamIoError::CborDecode(format!("sequence item {}: {}", items.len(), e))
            })?;
            let raw = unwrap_cbor_value(value, items.len())?;
            items.push(raw);
        }
        Ok(items)
    } else {
        let mut output_bytes = Vec::new();
        let mut cursor = std::io::Cursor::new(response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                StreamIoError::CborDecode(format!("terminal response: {}", e))
            })?;
            let raw = unwrap_cbor_value(value, 0)?;
            output_bytes.extend(raw);
        }
        Ok(vec![output_bytes])
    }
}

/// Unwrap a CBOR transport value to raw bytes.
///
/// Bytes → inner bytes, Text → UTF-8 bytes. Anything else is a
/// protocol error.
pub fn unwrap_cbor_value(value: ciborium::Value, item_index: usize) -> Result<Vec<u8>, StreamIoError> {
    match value {
        ciborium::Value::Bytes(b) => Ok(b),
        ciborium::Value::Text(t) => Ok(t.into_bytes()),
        _ => Err(StreamIoError::UnexpectedCborType {
            index: item_index,
            description: format!("{:?}", value),
        }),
    }
}
