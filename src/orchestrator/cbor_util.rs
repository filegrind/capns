//! CBOR split/assemble utilities for ForEach execution.
//!
//! The bifaci protocol uses CBOR throughout. These utilities handle:
//!
//! **CBOR Sequence (RFC 8742)** — the primary format for list data in the DAG:
//! - Splitting an RFC 8742 CBOR sequence (concatenated self-delimiting CBOR values) into items
//! - Assembling individually-serialized CBOR items into a CBOR sequence (concatenation, no wrapper)
//!
//! A CBOR sequence is the natural format for list-tagged media URNs: each `emit_cbor()` call
//! by a plugin produces one CBOR value, and concatenating all chunk payloads yields a sequence.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CborUtilError {
    #[error("Failed to deserialize CBOR data: {0}")]
    DeserializeError(String),

    #[error("CBOR data is not an array (expected array for splitting)")]
    NotAnArray,

    #[error("Failed to serialize CBOR value: {0}")]
    SerializeError(String),

    #[error("Empty CBOR array — nothing to split")]
    EmptyArray,
}

/// Split a CBOR-encoded array into individually-serialized CBOR items.
///
/// Each returned `Vec<u8>` is a complete, independently-parseable CBOR value.
///
/// # Errors
/// - `NotAnArray` if the input is not a CBOR array
/// - `EmptyArray` if the array has zero elements
/// - `DeserializeError` if the input bytes are not valid CBOR
pub fn split_cbor_array(data: &[u8]) -> Result<Vec<Vec<u8>>, CborUtilError> {
    let value: ciborium::Value = ciborium::de::from_reader(data)
        .map_err(|e| CborUtilError::DeserializeError(e.to_string()))?;

    let items = match value {
        ciborium::Value::Array(items) => items,
        _ => return Err(CborUtilError::NotAnArray),
    };

    if items.is_empty() {
        return Err(CborUtilError::EmptyArray);
    }

    let mut result = Vec::with_capacity(items.len());
    for item in items {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&item, &mut buf)
            .map_err(|e| CborUtilError::SerializeError(e.to_string()))?;
        result.push(buf);
    }

    Ok(result)
}

/// Assemble individually-serialized CBOR items into a single CBOR array.
///
/// Each input `Vec<u8>` must be a complete CBOR value. The result is a CBOR array
/// containing all items in order.
///
/// # Errors
/// - `DeserializeError` if any item is not valid CBOR
/// - `SerializeError` if the assembled array cannot be serialized
pub fn assemble_cbor_array(items: &[Vec<u8>]) -> Result<Vec<u8>, CborUtilError> {
    let mut values = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let value: ciborium::Value = ciborium::de::from_reader(item.as_slice())
            .map_err(|e| CborUtilError::DeserializeError(
                format!("Item {}: {}", i, e)
            ))?;
        values.push(value);
    }

    let array = ciborium::Value::Array(values);
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&array, &mut buf)
        .map_err(|e| CborUtilError::SerializeError(e.to_string()))?;

    Ok(buf)
}

/// Split an RFC 8742 CBOR sequence into individually-serialized CBOR items.
///
/// A CBOR sequence is a concatenation of independently-encoded CBOR data items
/// with no array wrapper. Each item is a complete, self-delimiting CBOR value.
/// This function iterates through the sequence by decoding values one at a time.
///
/// Returns each item re-serialized as an independent `Vec<u8>`.
///
/// # Errors
/// - `EmptyArray` if the input is empty or contains no decodable items
/// - `DeserializeError` if any CBOR value is malformed (including truncation)
pub fn split_cbor_sequence(data: &[u8]) -> Result<Vec<Vec<u8>>, CborUtilError> {
    if data.is_empty() {
        return Err(CborUtilError::EmptyArray);
    }

    let mut items = Vec::new();
    let mut cursor = std::io::Cursor::new(data);

    while (cursor.position() as usize) < data.len() {
        let value: ciborium::Value = ciborium::de::from_reader(&mut cursor)
            .map_err(|e| CborUtilError::DeserializeError(e.to_string()))?;
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&value, &mut buf)
            .map_err(|e| CborUtilError::SerializeError(e.to_string()))?;
        items.push(buf);
    }

    if items.is_empty() {
        return Err(CborUtilError::EmptyArray);
    }

    Ok(items)
}

/// Assemble individually-serialized CBOR items into an RFC 8742 CBOR sequence.
///
/// Each input item must be a complete CBOR value. The result is their raw concatenation
/// (no array wrapper). This is the inverse of `split_cbor_sequence`.
///
/// # Errors
/// - `DeserializeError` if any item is not valid CBOR
pub fn assemble_cbor_sequence(items: &[Vec<u8>]) -> Result<Vec<u8>, CborUtilError> {
    let mut result = Vec::new();
    for (i, item) in items.iter().enumerate() {
        // Validate each item is valid CBOR
        let _: ciborium::Value = ciborium::de::from_reader(item.as_slice())
            .map_err(|e| CborUtilError::DeserializeError(format!("Item {}: {}", i, e)))?;
        result.extend_from_slice(item);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cbor_encode(value: &ciborium::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(value, &mut buf).unwrap();
        buf
    }

    // TEST780: split_cbor_array splits a simple array of integers
    #[test]
    fn test780_split_integer_array() {
        let array = ciborium::Value::Array(vec![
            ciborium::Value::Integer(1.into()),
            ciborium::Value::Integer(2.into()),
            ciborium::Value::Integer(3.into()),
        ]);
        let data = cbor_encode(&array);

        let items = split_cbor_array(&data).unwrap();
        assert_eq!(items.len(), 3);

        // Each item should be a valid CBOR integer
        for (i, item) in items.iter().enumerate() {
            let value: ciborium::Value = ciborium::de::from_reader(item.as_slice()).unwrap();
            assert_eq!(value, ciborium::Value::Integer(((i + 1) as i64).into()));
        }
    }

    // TEST781: split_cbor_array with nested maps
    #[test]
    fn test781_split_map_array() {
        let map1 = ciborium::Value::Map(vec![
            (ciborium::Value::Text("name".to_string()), ciborium::Value::Text("Alice".to_string())),
        ]);
        let map2 = ciborium::Value::Map(vec![
            (ciborium::Value::Text("name".to_string()), ciborium::Value::Text("Bob".to_string())),
        ]);
        let array = ciborium::Value::Array(vec![map1.clone(), map2.clone()]);
        let data = cbor_encode(&array);

        let items = split_cbor_array(&data).unwrap();
        assert_eq!(items.len(), 2);

        let decoded1: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(decoded1, map1);
        let decoded2: ciborium::Value = ciborium::de::from_reader(items[1].as_slice()).unwrap();
        assert_eq!(decoded2, map2);
    }

    // TEST782: split_cbor_array rejects non-array input
    #[test]
    fn test782_split_non_array() {
        let text = ciborium::Value::Text("not an array".to_string());
        let data = cbor_encode(&text);

        let result = split_cbor_array(&data);
        assert!(matches!(result, Err(CborUtilError::NotAnArray)));
    }

    // TEST783: split_cbor_array rejects empty array
    #[test]
    fn test783_split_empty_array() {
        let array = ciborium::Value::Array(vec![]);
        let data = cbor_encode(&array);

        let result = split_cbor_array(&data);
        assert!(matches!(result, Err(CborUtilError::EmptyArray)));
    }

    // TEST784: split_cbor_array rejects invalid CBOR bytes
    #[test]
    fn test784_split_invalid_cbor() {
        let result = split_cbor_array(&[0xFF, 0xFE, 0xFD]);
        assert!(matches!(result, Err(CborUtilError::DeserializeError(_))));
    }

    // TEST785: assemble_cbor_array creates array from individual items
    #[test]
    fn test785_assemble_integer_array() {
        let items: Vec<Vec<u8>> = vec![
            cbor_encode(&ciborium::Value::Integer(10.into())),
            cbor_encode(&ciborium::Value::Integer(20.into())),
            cbor_encode(&ciborium::Value::Integer(30.into())),
        ];

        let assembled = assemble_cbor_array(&items).unwrap();

        // Decode and verify
        let value: ciborium::Value = ciborium::de::from_reader(assembled.as_slice()).unwrap();
        match value {
            ciborium::Value::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], ciborium::Value::Integer(10.into()));
                assert_eq!(items[1], ciborium::Value::Integer(20.into()));
                assert_eq!(items[2], ciborium::Value::Integer(30.into()));
            }
            _ => panic!("Expected CBOR array"),
        }
    }

    // TEST786: split then assemble roundtrip preserves data
    #[test]
    fn test786_roundtrip_split_assemble() {
        let original = ciborium::Value::Array(vec![
            ciborium::Value::Text("hello".to_string()),
            ciborium::Value::Bool(true),
            ciborium::Value::Integer(42.into()),
            ciborium::Value::Bytes(vec![1, 2, 3]),
        ]);
        let original_bytes = cbor_encode(&original);

        let items = split_cbor_array(&original_bytes).unwrap();
        assert_eq!(items.len(), 4);

        let reassembled = assemble_cbor_array(&items).unwrap();
        let decoded: ciborium::Value = ciborium::de::from_reader(reassembled.as_slice()).unwrap();
        assert_eq!(decoded, original);
    }

    // TEST787: assemble then split roundtrip preserves data
    #[test]
    fn test787_roundtrip_assemble_split() {
        let items: Vec<Vec<u8>> = vec![
            cbor_encode(&ciborium::Value::Text("a".to_string())),
            cbor_encode(&ciborium::Value::Text("b".to_string())),
        ];

        let assembled = assemble_cbor_array(&items).unwrap();
        let split_back = split_cbor_array(&assembled).unwrap();

        assert_eq!(split_back.len(), 2);
        assert_eq!(split_back[0], items[0]);
        assert_eq!(split_back[1], items[1]);
    }

    // TEST788: assemble empty list produces empty CBOR array
    #[test]
    fn test788_assemble_empty() {
        let assembled = assemble_cbor_array(&[]).unwrap();
        let value: ciborium::Value = ciborium::de::from_reader(assembled.as_slice()).unwrap();
        assert_eq!(value, ciborium::Value::Array(vec![]));
    }

    // TEST789: assemble rejects invalid CBOR item
    #[test]
    fn test789_assemble_invalid_item() {
        let items: Vec<Vec<u8>> = vec![
            cbor_encode(&ciborium::Value::Integer(1.into())),
            vec![0xFF, 0xFE], // invalid CBOR
        ];

        let result = assemble_cbor_array(&items);
        assert!(matches!(result, Err(CborUtilError::DeserializeError(_))));
    }

    // TEST790: split preserves CBOR byte strings (binary data — the common case in bifaci)
    #[test]
    fn test790_split_binary_items() {
        let pdf_bytes = vec![0x25, 0x50, 0x44, 0x46]; // %PDF
        let png_bytes = vec![0x89, 0x50, 0x4E, 0x47]; // .PNG

        let array = ciborium::Value::Array(vec![
            ciborium::Value::Bytes(pdf_bytes.clone()),
            ciborium::Value::Bytes(png_bytes.clone()),
        ]);
        let data = cbor_encode(&array);

        let items = split_cbor_array(&data).unwrap();
        assert_eq!(items.len(), 2);

        let decoded0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(decoded0, ciborium::Value::Bytes(pdf_bytes));
        let decoded1: ciborium::Value = ciborium::de::from_reader(items[1].as_slice()).unwrap();
        assert_eq!(decoded1, ciborium::Value::Bytes(png_bytes));
    }

    // =========================================================================
    // CBOR Sequence (RFC 8742) tests
    // =========================================================================

    /// Helper: build a CBOR sequence by concatenating individually-encoded values.
    fn build_cbor_sequence(values: &[ciborium::Value]) -> Vec<u8> {
        let mut result = Vec::new();
        for v in values {
            ciborium::ser::into_writer(v, &mut result).unwrap();
        }
        result
    }

    // TEST791: split_cbor_sequence splits concatenated CBOR Bytes values
    #[test]
    fn test791_split_sequence_bytes() {
        let page1 = b"page1 json data";
        let page2 = b"page2 json data";
        let page3 = b"page3 json data";

        let seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(page1.to_vec()),
            ciborium::Value::Bytes(page2.to_vec()),
            ciborium::Value::Bytes(page3.to_vec()),
        ]);

        let items = split_cbor_sequence(&seq).unwrap();
        assert_eq!(items.len(), 3);

        let d0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(d0, ciborium::Value::Bytes(page1.to_vec()));
        let d1: ciborium::Value = ciborium::de::from_reader(items[1].as_slice()).unwrap();
        assert_eq!(d1, ciborium::Value::Bytes(page2.to_vec()));
        let d2: ciborium::Value = ciborium::de::from_reader(items[2].as_slice()).unwrap();
        assert_eq!(d2, ciborium::Value::Bytes(page3.to_vec()));
    }

    // TEST792: split_cbor_sequence splits concatenated CBOR Text values
    #[test]
    fn test792_split_sequence_text() {
        let seq = build_cbor_sequence(&[
            ciborium::Value::Text("hello".to_string()),
            ciborium::Value::Text("world".to_string()),
        ]);

        let items = split_cbor_sequence(&seq).unwrap();
        assert_eq!(items.len(), 2);

        let d0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(d0, ciborium::Value::Text("hello".to_string()));
        let d1: ciborium::Value = ciborium::de::from_reader(items[1].as_slice()).unwrap();
        assert_eq!(d1, ciborium::Value::Text("world".to_string()));
    }

    // TEST793: split_cbor_sequence handles mixed types
    #[test]
    fn test793_split_sequence_mixed() {
        let seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(vec![1, 2, 3]),
            ciborium::Value::Text("mixed".to_string()),
            ciborium::Value::Map(vec![
                (ciborium::Value::Text("key".to_string()), ciborium::Value::Integer(42.into())),
            ]),
            ciborium::Value::Integer(99.into()),
        ]);

        let items = split_cbor_sequence(&seq).unwrap();
        assert_eq!(items.len(), 4);

        let d0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(d0, ciborium::Value::Bytes(vec![1, 2, 3]));
        let d3: ciborium::Value = ciborium::de::from_reader(items[3].as_slice()).unwrap();
        assert_eq!(d3, ciborium::Value::Integer(99.into()));
    }

    // TEST794: split_cbor_sequence single-item sequence
    #[test]
    fn test794_split_sequence_single() {
        let seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(vec![0xDE, 0xAD]),
        ]);

        let items = split_cbor_sequence(&seq).unwrap();
        assert_eq!(items.len(), 1);
        let d0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(d0, ciborium::Value::Bytes(vec![0xDE, 0xAD]));
    }

    // TEST795: roundtrip — assemble then split preserves items
    #[test]
    fn test795_roundtrip_assemble_split_sequence() {
        let item_values = vec![
            ciborium::Value::Bytes(b"first".to_vec()),
            ciborium::Value::Bytes(b"second".to_vec()),
            ciborium::Value::Text("third".to_string()),
        ];
        let items: Vec<Vec<u8>> = item_values.iter().map(|v| cbor_encode(v)).collect();

        let assembled = assemble_cbor_sequence(&items).unwrap();
        let split_back = split_cbor_sequence(&assembled).unwrap();

        assert_eq!(split_back.len(), 3);
        assert_eq!(split_back[0], items[0]);
        assert_eq!(split_back[1], items[1]);
        assert_eq!(split_back[2], items[2]);
    }

    // TEST796: roundtrip — split then assemble preserves byte-for-byte
    #[test]
    fn test796_roundtrip_split_assemble_sequence() {
        let seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(b"alpha".to_vec()),
            ciborium::Value::Bytes(b"beta".to_vec()),
        ]);

        let items = split_cbor_sequence(&seq).unwrap();
        let reassembled = assemble_cbor_sequence(&items).unwrap();

        assert_eq!(reassembled, seq, "split then assemble must preserve bytes exactly");
    }

    // TEST797: split_cbor_sequence rejects empty data
    #[test]
    fn test797_split_sequence_empty() {
        let result = split_cbor_sequence(&[]);
        assert!(matches!(result, Err(CborUtilError::EmptyArray)));
    }

    // TEST798: split_cbor_sequence rejects truncated CBOR
    #[test]
    fn test798_split_sequence_truncated() {
        // Build a valid CBOR Bytes value, then truncate it
        let mut seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(b"complete".to_vec()),
        ]);
        // Add a truncated CBOR item: major type 2 (bytes), length 10, but only 3 bytes of content
        seq.push(0x4A); // bytes(10)
        seq.extend_from_slice(&[0x01, 0x02, 0x03]); // only 3 of 10 bytes

        let result = split_cbor_sequence(&seq);
        assert!(matches!(result, Err(CborUtilError::DeserializeError(_))),
            "truncated CBOR at end must produce DeserializeError, got {:?}", result);
    }

    // TEST799: assemble_cbor_sequence rejects invalid CBOR item
    #[test]
    fn test799_assemble_sequence_invalid_item() {
        let items: Vec<Vec<u8>> = vec![
            cbor_encode(&ciborium::Value::Integer(1.into())),
            vec![0xFF, 0xFE], // invalid CBOR
        ];

        let result = assemble_cbor_sequence(&items);
        assert!(matches!(result, Err(CborUtilError::DeserializeError(_))));
    }

    // TEST800: assemble_cbor_sequence with empty items list produces empty bytes
    #[test]
    fn test800_assemble_sequence_empty() {
        let assembled = assemble_cbor_sequence(&[]).unwrap();
        assert!(assembled.is_empty(), "empty sequence must produce empty bytes");
    }

    // TEST801: CBOR sequence is NOT a CBOR array — split_cbor_array rejects a sequence
    #[test]
    fn test801_sequence_is_not_array() {
        let seq = build_cbor_sequence(&[
            ciborium::Value::Bytes(b"item1".to_vec()),
            ciborium::Value::Bytes(b"item2".to_vec()),
        ]);

        // A CBOR sequence with >1 items is not a single CBOR value,
        // so from_reader will read only the first item (which is Bytes, not Array)
        let result = split_cbor_array(&seq);
        assert!(matches!(result, Err(CborUtilError::NotAnArray)),
            "CBOR sequence must not be parseable as a CBOR array, got {:?}", result);
    }

    // TEST802: split_cbor_sequence works on data that is also a valid CBOR array (single top-level value)
    #[test]
    fn test802_single_value_sequence() {
        // A single CBOR value is both a valid CBOR sequence (of 1 item) and a valid CBOR value
        let single = cbor_encode(&ciborium::Value::Bytes(b"solo".to_vec()));

        let items = split_cbor_sequence(&single).unwrap();
        assert_eq!(items.len(), 1);
        let d0: ciborium::Value = ciborium::de::from_reader(items[0].as_slice()).unwrap();
        assert_eq!(d0, ciborium::Value::Bytes(b"solo".to_vec()));
    }
}
