//! Data interchange content inspection adapters
//!
//! These adapters inspect file content to determine list/record markers.
//! The base URN is provided by MediaUrnRegistry; adapters refine it.

use std::path::Path;

use crate::input_resolver::adapter::{AdapterResult, MediaAdapter};
use crate::input_resolver::ContentStructure;

/// JSON adapter — inspects content to determine list/record markers
pub struct JsonAdapter;

impl MediaAdapter for JsonAdapter {
    fn name(&self) -> &'static str {
        "json"
    }

    fn extensions(&self) -> &'static [&'static str] {
        // Empty - extensions handled by MediaUrnRegistry
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_json_structure(content)
    }
}

/// Detect JSON structure from content
fn detect_json_structure(content: &[u8]) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            return AdapterResult::scalar_opaque("media:json;textable");
        }
    };

    let trimmed = text.trim_start();

    if trimmed.is_empty() {
        return AdapterResult::scalar_opaque("media:json;textable");
    }

    match trimmed.chars().next() {
        Some('{') => AdapterResult::scalar_record("media:json;record;textable"),
        Some('[') => detect_json_array_structure(trimmed),
        Some('"') | Some('0'..='9') | Some('-') | Some('t') | Some('f') | Some('n') => {
            AdapterResult::scalar_opaque("media:json;textable")
        }
        _ => AdapterResult::scalar_opaque("media:json;textable"),
    }
}

/// Check if a JSON array contains objects
fn detect_json_array_structure(trimmed: &str) -> AdapterResult {
    let after_bracket = trimmed[1..].trim_start();

    if after_bracket.is_empty() || after_bracket.starts_with(']') {
        return AdapterResult::list_opaque("media:json;list;textable");
    }

    if after_bracket.starts_with('{') {
        AdapterResult::list_record("media:json;list;record;textable")
    } else {
        AdapterResult::list_opaque("media:json;list;textable")
    }
}

/// NDJSON (Newline-delimited JSON) adapter
pub struct NdjsonAdapter;

impl MediaAdapter for NdjsonAdapter {
    fn name(&self) -> &'static str {
        "ndjson"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_ndjson_structure(content)
    }
}

/// Detect NDJSON structure from content
fn detect_ndjson_structure(content: &[u8]) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            return AdapterResult::list_opaque("media:ndjson;list;textable");
        }
    };

    let mut has_object = false;

    for line in text.lines().take(10) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('{') {
            has_object = true;
            break;
        }
    }

    if has_object {
        AdapterResult::list_record("media:ndjson;list;record;textable")
    } else {
        AdapterResult::list_opaque("media:ndjson;list;textable")
    }
}

/// CSV adapter — inspects content to determine record marker
pub struct CsvAdapter;

impl MediaAdapter for CsvAdapter {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_csv_structure(content, ',')
    }
}

/// TSV (Tab-separated values) adapter
pub struct TsvAdapter;

impl MediaAdapter for TsvAdapter {
    fn name(&self) -> &'static str {
        "tsv"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        let result = detect_csv_structure(content, '\t');
        AdapterResult {
            media_urn: result.media_urn.replace("csv", "tsv"),
            content_structure: result.content_structure,
        }
    }
}

/// PSV (Pipe-separated values) adapter
pub struct PsvAdapter;

impl MediaAdapter for PsvAdapter {
    fn name(&self) -> &'static str {
        "psv"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        let result = detect_csv_structure(content, '|');
        AdapterResult {
            media_urn: result.media_urn.replace("csv", "psv"),
            content_structure: result.content_structure,
        }
    }
}

/// Detect CSV structure — list of records if multiple columns
fn detect_csv_structure(content: &[u8], delimiter: char) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            return AdapterResult::list_opaque("media:csv;list;textable");
        }
    };

    let first_line = match text.lines().next() {
        Some(line) => line,
        None => {
            return AdapterResult::list_opaque("media:csv;list;textable");
        }
    };

    let column_count = count_csv_columns(first_line, delimiter);

    if column_count > 1 {
        AdapterResult::list_record("media:csv;list;record;textable")
    } else {
        AdapterResult::list_opaque("media:csv;list;textable")
    }
}

/// Count columns in a CSV line (handles basic quoting)
fn count_csv_columns(line: &str, delimiter: char) -> usize {
    let mut count = 1;
    let mut in_quotes = false;

    for ch in line.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch == delimiter && !in_quotes {
            count += 1;
        }
    }

    count
}

/// YAML adapter — inspects content to determine structure
pub struct YamlAdapter;

impl MediaAdapter for YamlAdapter {
    fn name(&self) -> &'static str {
        "yaml"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_yaml_structure(content)
    }
}

/// Detect YAML structure from content
fn detect_yaml_structure(content: &[u8]) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            return AdapterResult::scalar_opaque("media:yaml;textable");
        }
    };

    let trimmed = text.trim_start();

    // Check for document separators (multi-document)
    let doc_count =
        text.matches("\n---").count() + if trimmed.starts_with("---") { 1 } else { 0 };

    if doc_count > 1 {
        let first_doc = trimmed.split("\n---").next().unwrap_or("");
        let first_doc = first_doc
            .strip_prefix("---")
            .unwrap_or(first_doc)
            .trim_start();

        if looks_like_yaml_mapping(first_doc) {
            return AdapterResult::list_record("media:yaml;list;record;textable");
        } else {
            return AdapterResult::list_opaque("media:yaml;list;textable");
        }
    }

    // Single document
    let doc = trimmed.strip_prefix("---").unwrap_or(trimmed).trim_start();

    if doc.is_empty() {
        return AdapterResult::scalar_opaque("media:yaml;textable");
    }

    if doc.starts_with('-') {
        let first_item = doc
            .lines()
            .find(|l| l.trim_start().starts_with('-'))
            .map(|l| l.trim_start().strip_prefix('-').unwrap_or("").trim_start())
            .unwrap_or("");

        if looks_like_yaml_mapping(first_item) || first_item.contains(':') {
            AdapterResult::list_record("media:yaml;list;record;textable")
        } else {
            AdapterResult::list_opaque("media:yaml;list;textable")
        }
    } else if doc.starts_with('{') {
        AdapterResult::scalar_record("media:yaml;record;textable")
    } else if doc.starts_with('[') {
        if doc.contains('{') {
            AdapterResult::list_record("media:yaml;list;record;textable")
        } else {
            AdapterResult::list_opaque("media:yaml;list;textable")
        }
    } else if doc.contains(':') {
        AdapterResult::scalar_record("media:yaml;record;textable")
    } else {
        AdapterResult::scalar_opaque("media:yaml;textable")
    }
}

/// Check if content looks like a YAML mapping
fn looks_like_yaml_mapping(content: &str) -> bool {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(colon_pos) = trimmed.find(':') {
            let before_colon = &trimmed[..colon_pos];
            if !before_colon.is_empty() && !before_colon.contains(' ') {
                return true;
            }
        }
    }
    false
}

/// TOML adapter — always record (config file, no inspection needed)
pub struct TomlAdapter;

impl MediaAdapter for TomlAdapter {
    fn name(&self) -> &'static str {
        "toml"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_record("media:toml;record;textable")
    }
}

/// XML adapter — inspects content to determine structure
pub struct XmlAdapter;

impl MediaAdapter for XmlAdapter {
    fn name(&self) -> &'static str {
        "xml"
    }

    fn extensions(&self) -> &'static [&'static str] {
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_xml_structure(content)
    }
}

/// Detect XML structure from content
fn detect_xml_structure(content: &[u8]) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            return AdapterResult::scalar_opaque("media:xml;textable");
        }
    };

    // Skip XML declaration
    let body = if let Some(pos) = text.find("?>") {
        &text[pos + 2..]
    } else {
        text
    };

    let trimmed = body.trim();

    // Find root element
    if let Some(start) = trimmed.find('<') {
        if let Some(end) = trimmed[start..].find(|c| c == '>' || c == ' ' || c == '/') {
            let tag_name = &trimmed[start + 1..start + end];

            // Look for repeated child elements
            let child_pattern = format!("<{}", tag_name.chars().take(1).collect::<String>());

            let child_count = trimmed.matches(&child_pattern).count();

            if child_count > 2 {
                return AdapterResult::list_record("media:xml;list;record;textable");
            }
        }
    }

    if trimmed.contains('=') || (trimmed.matches('<').count() > 2) {
        AdapterResult::scalar_record("media:xml;record;textable")
    } else {
        AdapterResult::scalar_opaque("media:xml;textable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // JSON Detection Tests (TEST1030-TEST1044)

    // TEST1030: Empty object
    #[test]
    fn test1030_json_empty_object() {
        let adapter = JsonAdapter;
        let path = PathBuf::from("data.json");
        let content = b"{}";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:json;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ScalarRecord);
    }

    // TEST1031: Simple object
    #[test]
    fn test1031_json_simple_object() {
        let adapter = JsonAdapter;
        let path = PathBuf::from("data.json");
        let content = br#"{"a": 1}"#;

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:json;record;textable");
    }

    // TEST1033: Empty array
    #[test]
    fn test1033_json_empty_array() {
        let adapter = JsonAdapter;
        let path = PathBuf::from("data.json");
        let content = b"[]";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:json;list;textable");
        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
    }

    // TEST1036: Array of objects
    #[test]
    fn test1036_json_array_of_objects() {
        let adapter = JsonAdapter;
        let path = PathBuf::from("data.json");
        let content = br#"[{"a": 1}]"#;

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:json;list;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    // TEST1039: Number primitive
    #[test]
    fn test1039_json_number_primitive() {
        let adapter = JsonAdapter;
        let path = PathBuf::from("data.json");
        let content = b"42";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:json;textable");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    // NDJSON Detection Tests (TEST1045-TEST1054)

    // TEST1045: Objects only
    #[test]
    fn test1045_ndjson_objects() {
        let adapter = NdjsonAdapter;
        let path = PathBuf::from("data.ndjson");
        let content = b"{\"a\":1}\n{\"b\":2}";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:ndjson;list;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    // TEST1047: Primitives only
    #[test]
    fn test1047_ndjson_primitives() {
        let adapter = NdjsonAdapter;
        let path = PathBuf::from("data.ndjson");
        let content = b"1\n2\n3";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:ndjson;list;textable");
        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
    }

    // CSV Detection Tests (TEST1055-TEST1064)

    // TEST1055: Multi-column with header
    #[test]
    fn test1055_csv_multi_column() {
        let adapter = CsvAdapter;
        let path = PathBuf::from("data.csv");
        let content = b"a,b\n1,2";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:csv;list;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    // TEST1056: Single column
    #[test]
    fn test1056_csv_single_column() {
        let adapter = CsvAdapter;
        let path = PathBuf::from("data.csv");
        let content = b"value\n1\n2";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:csv;list;textable");
        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
    }

    // YAML Detection Tests (TEST1065-TEST1074)

    // TEST1065: Simple mapping
    #[test]
    fn test1065_yaml_mapping() {
        let adapter = YamlAdapter;
        let path = PathBuf::from("config.yaml");
        let content = b"a: 1";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:yaml;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ScalarRecord);
    }

    // TEST1067: Sequence of scalars
    #[test]
    fn test1067_yaml_sequence_of_scalars() {
        let adapter = YamlAdapter;
        let path = PathBuf::from("list.yaml");
        let content = b"- a\n- b";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:yaml;list;textable");
        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
    }

    // TEST1068: Sequence of mappings
    #[test]
    fn test1068_yaml_sequence_of_mappings() {
        let adapter = YamlAdapter;
        let path = PathBuf::from("list.yaml");
        let content = b"- a: 1\n- b: 2";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:yaml;list;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    // TEST1087: TOML always record
    #[test]
    fn test1087_toml_always_record() {
        let adapter = TomlAdapter;
        let path = PathBuf::from("config.toml");
        let content = b"[section]\nkey = \"value\"";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:toml;record;textable");
        assert_eq!(result.content_structure, ContentStructure::ScalarRecord);
    }
}
