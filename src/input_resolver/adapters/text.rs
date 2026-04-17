//! Plain text content inspection adapter
//!
//! PlainTextAdapter inspects content to determine if text is
//! single-line (scalar) or multi-line (list). It selects from
//! candidate URNs provided by the media spec registry.

use std::path::Path;

use crate::input_resolver::adapter::{select_by_structure, AdapterSelection, MediaAdapter};
use crate::urn::media_urn::MediaUrn;

/// Plain text adapter — inspects content for structure
///
/// Determines if a .txt file is:
/// - Single line → ScalarOpaque
/// - Multi-line → ListOpaque
pub struct PlainTextAdapter;

impl MediaAdapter for PlainTextAdapter {
    fn name(&self) -> &'static str {
        "txt"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:txt"
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
        let text = match std::str::from_utf8(content) {
            Ok(s) => s,
            Err(_) => return None, // Not valid UTF-8 — not a text file
        };

        let line_count = text.lines().count();
        let is_list = line_count > 1;

        select_by_structure(candidates, is_list, false).map(|(idx, structure)| AdapterSelection {
            candidate_index: idx,
            content_structure: structure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input_resolver::ContentStructure;

    fn txt_candidates() -> Vec<MediaUrn> {
        vec![
            MediaUrn::from_string("media:textable;txt").unwrap(),
            MediaUrn::from_string("media:list;textable;txt").unwrap(),
        ]
    }

    fn refs(urns: &[MediaUrn]) -> Vec<&MediaUrn> {
        urns.iter().collect()
    }

    // TEST1239: Single-line UTF-8 text selects the scalar TXT candidate.
    #[test]
    fn test1239_plain_text_single_line() {
        let candidates = txt_candidates();
        let adapter = PlainTextAdapter;
        let path = std::path::PathBuf::from("note.txt");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"just a single line")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ScalarOpaque);
        assert!(!candidates[sel.candidate_index].has_marker_tag("list"));
    }

    // TEST1240: Multi-line UTF-8 text selects the list-shaped TXT candidate.
    #[test]
    fn test1240_plain_text_multi_line() {
        let candidates = txt_candidates();
        let adapter = PlainTextAdapter;
        let path = std::path::PathBuf::from("note.txt");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"line one\nline two\nline three")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ListOpaque);
        assert!(candidates[sel.candidate_index].has_marker_tag("list"));
    }

    // TEST1241: Empty UTF-8 text is treated as scalar opaque content.
    #[test]
    fn test1241_plain_text_empty() {
        let candidates = txt_candidates();
        let adapter = PlainTextAdapter;
        let path = std::path::PathBuf::from("empty.txt");
        let sel = adapter
            .select_candidate(&refs(&candidates), &path, b"")
            .unwrap();
        assert_eq!(sel.content_structure, ContentStructure::ScalarOpaque);
    }

    // TEST1242: Invalid UTF-8 content is rejected by the plain text adapter.
    #[test]
    fn test1242_plain_text_binary_returns_none() {
        let candidates = txt_candidates();
        let adapter = PlainTextAdapter;
        let path = std::path::PathBuf::from("data.txt");
        let content = &[0xFF, 0xFE, 0x00, 0x01]; // Invalid UTF-8
        let sel = adapter.select_candidate(&refs(&candidates), &path, content);
        assert!(
            sel.is_none(),
            "Binary content should not be handled by PlainTextAdapter"
        );
    }
}
