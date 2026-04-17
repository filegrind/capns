//! Data interchange content inspection adapters
//!
//! These adapters inspect file content to determine structure (list/record markers).
//! Given candidate URNs from the media spec registry, each adapter selects the
//! candidate whose marker tags best match the detected content structure.

use std::path::Path;

use crate::input_resolver::adapter::{select_by_structure, AdapterSelection, MediaAdapter};
use crate::urn::media_urn::MediaUrn;

// =============================================================================
// JSON
// =============================================================================

pub struct JsonAdapter;

impl MediaAdapter for JsonAdapter {
    fn name(&self) -> &'static str {
        "json"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:json"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_json_structure(content);
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

/// Detect JSON structure from content → (is_list, is_record)
fn detect_json_structure(content: &[u8]) -> (bool, bool) {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return (false, false),
    };

    let trimmed = text.trim_start();
    if trimmed.is_empty() {
        return (false, false);
    }

    match trimmed.chars().next() {
        Some('{') => (false, true),
        Some('[') => {
            let after_bracket = trimmed[1..].trim_start();
            if after_bracket.is_empty() || after_bracket.starts_with(']') {
                (true, false)
            } else if after_bracket.starts_with('{') {
                (true, true)
            } else {
                (true, false)
            }
        }
        _ => (false, false),
    }
}

// =============================================================================
// NDJSON
// =============================================================================

pub struct NdjsonAdapter;

impl MediaAdapter for NdjsonAdapter {
    fn name(&self) -> &'static str {
        "ndjson"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:ndjson"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_ndjson_structure(content);
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

/// Detect NDJSON structure → (is_list, is_record)
/// NDJSON is always a list; check if items are objects
fn detect_ndjson_structure(content: &[u8]) -> (bool, bool) {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return (true, false),
    };

    let has_object = text
        .lines()
        .take(10)
        .any(|line| line.trim().starts_with('{'));
    (true, has_object)
}

// =============================================================================
// CSV
// =============================================================================

pub struct CsvAdapter;

impl MediaAdapter for CsvAdapter {
    fn name(&self) -> &'static str {
        "csv"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:csv"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_delimited_structure(content, ',');
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

// =============================================================================
// TSV
// =============================================================================

pub struct TsvAdapter;

impl MediaAdapter for TsvAdapter {
    fn name(&self) -> &'static str {
        "tsv"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:tsv"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_delimited_structure(content, '\t');
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

// =============================================================================
// PSV
// =============================================================================

pub struct PsvAdapter;

impl MediaAdapter for PsvAdapter {
    fn name(&self) -> &'static str {
        "psv"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:psv"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_delimited_structure(content, '|');
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

/// Detect delimited (CSV/TSV/PSV) structure → (is_list, is_record)
/// Delimited data is always a list; record if multiple columns
fn detect_delimited_structure(content: &[u8], delimiter: char) -> (bool, bool) {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return (true, false),
    };

    let first_line = match text.lines().next() {
        Some(line) => line,
        None => return (true, false),
    };

    let column_count = count_delimited_columns(first_line, delimiter);
    (true, column_count > 1)
}

/// Count columns in a delimited line (handles basic quoting)
fn count_delimited_columns(line: &str, delimiter: char) -> usize {
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

// =============================================================================
// YAML
// =============================================================================

pub struct YamlAdapter;

impl MediaAdapter for YamlAdapter {
    fn name(&self) -> &'static str {
        "yaml"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:yaml"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_yaml_structure(content);
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

/// Detect YAML structure from content → (is_list, is_record)
fn detect_yaml_structure(content: &[u8]) -> (bool, bool) {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return (false, false),
    };

    let trimmed = text.trim_start();

    // Check for document separators (multi-document)
    let doc_count = text.matches("\n---").count() + if trimmed.starts_with("---") { 1 } else { 0 };

    if doc_count > 1 {
        let first_doc = trimmed.split("\n---").next().unwrap_or("");
        let first_doc = first_doc
            .strip_prefix("---")
            .unwrap_or(first_doc)
            .trim_start();
        return (true, looks_like_yaml_mapping(first_doc));
    }

    // Single document
    let doc = trimmed.strip_prefix("---").unwrap_or(trimmed).trim_start();

    if doc.is_empty() {
        return (false, false);
    }

    if doc.starts_with('-') {
        let first_item = doc
            .lines()
            .find(|l| l.trim_start().starts_with('-'))
            .map(|l| l.trim_start().strip_prefix('-').unwrap_or("").trim_start())
            .unwrap_or("");

        let is_record = looks_like_yaml_mapping(first_item) || first_item.contains(':');
        (true, is_record)
    } else if doc.starts_with('{') {
        (false, true)
    } else if doc.starts_with('[') {
        (true, doc.contains('{'))
    } else if doc.contains(':') {
        (false, true)
    } else {
        (false, false)
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

// =============================================================================
// TOML
// =============================================================================

pub struct TomlAdapter;

impl MediaAdapter for TomlAdapter {
    fn name(&self) -> &'static str {
        "toml"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:toml"
    }

    // TOML is always a record — no content inspection needed.
    // The registry will pick the most specific conforming candidate,
    // which should be the record-tagged one.
}

// =============================================================================
// XML
// =============================================================================

pub struct XmlAdapter;

impl MediaAdapter for XmlAdapter {
    fn name(&self) -> &'static str {
        "xml"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:xml"
    }
    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn select_candidate(
        &self,
        candidates: &[&MediaUrn],
        _path: &Path,
        content: &[u8],
    ) -> Option<AdapterSelection> {
        let (is_list, is_record) = detect_xml_structure(content);
        select_by_structure(candidates, is_list, is_record).map(|(idx, structure)| {
            AdapterSelection {
                candidate_index: idx,
                content_structure: structure,
            }
        })
    }
}

/// Detect XML structure from content → (is_list, is_record)
fn detect_xml_structure(content: &[u8]) -> (bool, bool) {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return (false, false),
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
            let child_pattern = format!("<{}", tag_name.chars().take(1).collect::<String>());
            let child_count = trimmed.matches(&child_pattern).count();

            if child_count > 2 {
                return (true, true);
            }
        }
    }

    if trimmed.contains('=') || (trimmed.matches('<').count() > 2) {
        (false, true)
    } else {
        (false, false)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_resolver::ContentStructure;

    // Helper: create candidate MediaUrn refs for testing
    fn yaml_candidates() -> Vec<MediaUrn> {
        vec![
            MediaUrn::from_string("media:textable;yaml").unwrap(),
            MediaUrn::from_string("media:list;textable;yaml").unwrap(),
            MediaUrn::from_string("media:record;textable;yaml").unwrap(),
            MediaUrn::from_string("media:list;record;textable;yaml").unwrap(),
        ]
    }

    fn json_candidates() -> Vec<MediaUrn> {
        vec![
            MediaUrn::from_string("media:json;textable").unwrap(),
            MediaUrn::from_string("media:json;list;textable").unwrap(),
            MediaUrn::from_string("media:json;record;textable").unwrap(),
            MediaUrn::from_string("media:json;list;record;textable").unwrap(),
        ]
    }

    fn refs(urns: &[MediaUrn]) -> Vec<&MediaUrn> {
        urns.iter().collect()
    }

    // JSON tests

    #[test]
    fn test1030_json_empty_object() {
        let candidates = json_candidates();
        let adapter = JsonAdapter;
        let path = std::path::PathBuf::from("data.json");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"{}")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ScalarRecord);
        assert!(candidates[sel.candidate_index].has_marker_tag("record"));
        assert!(!candidates[sel.candidate_index].has_marker_tag("list"));
    }

    #[test]
    fn test1033_json_empty_array() {
        let candidates = json_candidates();
        let adapter = JsonAdapter;
        let path = std::path::PathBuf::from("data.json");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"[]")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListOpaque);
        assert!(candidates[sel.candidate_index].has_marker_tag("list"));
        assert!(!candidates[sel.candidate_index].has_marker_tag("record"));
    }

    #[test]
    fn test1036_json_array_of_objects() {
        let candidates = json_candidates();
        let adapter = JsonAdapter;
        let path = std::path::PathBuf::from("data.json");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, br#"[{"a": 1}]"#)
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListRecord);
        assert!(candidates[sel.candidate_index].has_marker_tag("list"));
        assert!(candidates[sel.candidate_index].has_marker_tag("record"));
    }

    #[test]
    fn test1039_json_number_primitive() {
        let candidates = json_candidates();
        let adapter = JsonAdapter;
        let path = std::path::PathBuf::from("data.json");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"42")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ScalarOpaque);
        assert!(!candidates[sel.candidate_index].has_marker_tag("list"));
        assert!(!candidates[sel.candidate_index].has_marker_tag("record"));
    }

    // NDJSON tests

    #[test]
    fn test1045_ndjson_objects() {
        let candidates = vec![
            MediaUrn::from_string("media:ndjson;textable").unwrap(),
            MediaUrn::from_string("media:ndjson;list;record;textable").unwrap(),
            MediaUrn::from_string("media:ndjson;list;textable").unwrap(),
        ];
        let adapter = NdjsonAdapter;
        let path = std::path::PathBuf::from("data.ndjson");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"{\"a\":1}\n{\"b\":2}")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListRecord);
    }

    #[test]
    fn test1047_ndjson_primitives() {
        let candidates = vec![
            MediaUrn::from_string("media:ndjson;textable").unwrap(),
            MediaUrn::from_string("media:ndjson;list;textable").unwrap(),
        ];
        let adapter = NdjsonAdapter;
        let path = std::path::PathBuf::from("data.ndjson");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"1\n2\n3")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListOpaque);
    }

    // CSV tests

    #[test]
    fn test1055_csv_multi_column() {
        let candidates = vec![
            MediaUrn::from_string("media:csv;list;textable").unwrap(),
            MediaUrn::from_string("media:csv;list;record;textable").unwrap(),
        ];
        let adapter = CsvAdapter;
        let path = std::path::PathBuf::from("data.csv");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"a,b\n1,2")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListRecord);
    }

    #[test]
    fn test1056_csv_single_column() {
        let candidates = vec![
            MediaUrn::from_string("media:csv;list;textable").unwrap(),
            MediaUrn::from_string("media:csv;list;record;textable").unwrap(),
        ];
        let adapter = CsvAdapter;
        let path = std::path::PathBuf::from("data.csv");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"value\n1\n2")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListOpaque);
    }

    // YAML tests

    #[test]
    fn test1065_yaml_mapping() {
        let candidates = yaml_candidates();
        let adapter = YamlAdapter;
        let path = std::path::PathBuf::from("config.yaml");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"a: 1")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ScalarRecord);
    }

    #[test]
    fn test1067_yaml_sequence_of_scalars() {
        let candidates = yaml_candidates();
        let adapter = YamlAdapter;
        let path = std::path::PathBuf::from("list.yaml");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"- a\n- b")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListOpaque);
    }

    #[test]
    fn test1068_yaml_sequence_of_mappings() {
        let candidates = yaml_candidates();
        let adapter = YamlAdapter;
        let path = std::path::PathBuf::from("list.yaml");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"- a: 1\n- b: 2")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListRecord);
    }

    // TOML test — TomlAdapter doesn't inspect content, so no select_candidate test.
    // The registry handles it by picking the most specific conforming candidate.
}
