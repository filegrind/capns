//! MediaAdapter trait — pluggable file type detection
//!
//! Each adapter is responsible for:
//! 1. Declaring a pattern URN that describes what media it handles
//! 2. Given candidate URNs from the registry, selecting the best match based on content inspection

use crate::input_resolver::ContentStructure;
use crate::urn::media_urn::MediaUrn;
use std::path::Path;

/// Result of adapter detection — a selected candidate URN and its structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterResult {
    /// The selected media URN (always one of the registry's own URNs, never constructed)
    pub media_urn: String,

    /// The detected content structure
    pub content_structure: ContentStructure,
}

/// Result of candidate selection — index into the candidates slice + structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterSelection {
    /// Index into the candidates slice passed to select_candidate
    pub candidate_index: usize,

    /// Detected content structure
    pub content_structure: ContentStructure,
}

/// Trait for media type adapters
///
/// Each adapter handles a family of media types identified by a pattern URN.
/// When given candidate URNs from the registry (all of which conform to the
/// adapter's pattern), the adapter inspects file content and selects the
/// most appropriate candidate.
pub trait MediaAdapter: Send + Sync {
    /// Unique name for this adapter (for debugging/logging)
    fn name(&self) -> &'static str;

    /// The pattern URN string this adapter handles.
    /// Candidates that `conforms_to` this pattern are offered to this adapter.
    /// e.g., "media:yaml" matches any URN with the yaml tag.
    fn pattern_urn(&self) -> &'static str;

    /// Whether this adapter requires content inspection to determine structure.
    /// If false, the most specific conforming candidate is selected automatically.
    /// If true, `select_candidate` is called with file content.
    fn requires_content_inspection(&self) -> bool {
        false
    }

    /// Given conforming candidate URNs and file content, select the best match.
    ///
    /// Only called when `requires_content_inspection()` returns true.
    /// The `candidates` slice contains only URNs that conform to this adapter's pattern.
    ///
    /// Returns `Some(selection)` with the index of the selected candidate and
    /// the detected content structure, or `None` if no candidate matches.
    fn select_candidate(
        &self,
        _candidates: &[&MediaUrn],
        _path: &Path,
        _content: &[u8],
    ) -> Option<AdapterSelection> {
        None
    }
}

/// Find the candidate whose marker tags best match the detected structure.
///
/// This is the standard selection logic used by content-inspecting adapters:
/// given a boolean for list and record, find the candidate that has matching
/// marker tags and is most specific.
pub fn select_by_structure(
    candidates: &[&MediaUrn],
    is_list: bool,
    is_record: bool,
) -> Option<(usize, ContentStructure)> {
    let structure = match (is_list, is_record) {
        (true, true) => ContentStructure::ListRecord,
        (true, false) => ContentStructure::ListOpaque,
        (false, true) => ContentStructure::ScalarRecord,
        (false, false) => ContentStructure::ScalarOpaque,
    };

    // Find candidates whose marker tags match the detected structure
    let mut best_index: Option<usize> = None;
    let mut best_specificity: usize = 0;

    for (i, candidate) in candidates.iter().enumerate() {
        let has_list = candidate.has_marker_tag("list");
        let has_record = candidate.has_marker_tag("record");

        if has_list == is_list && has_record == is_record {
            let spec = candidate.specificity();
            if best_index.is_none() || spec > best_specificity {
                best_index = Some(i);
                best_specificity = spec;
            }
        }
    }

    // If no exact structure match, prefer the most general candidate
    // (no list/record markers) so the URN doesn't overclaim structure.
    if best_index.is_none() {
        // First try: candidate with no structure markers (most general)
        best_index = candidates
            .iter()
            .position(|c| !c.has_marker_tag("list") && !c.has_marker_tag("record"));

        // Last resort: just use the least specific candidate
        if best_index.is_none() {
            best_index = candidates
                .iter()
                .enumerate()
                .min_by_key(|(_, c)| c.specificity())
                .map(|(i, _)| i);
        }
    }

    best_index.map(|i| (i, structure))
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST1227: Structure-based candidate selection picks the exact marker-tag variant for each shape.
    #[test]
    fn test1227_select_by_structure_exact_match() {
        let scalar = MediaUrn::from_string("media:textable;yaml").unwrap();
        let list = MediaUrn::from_string("media:list;textable;yaml").unwrap();
        let record = MediaUrn::from_string("media:record;textable;yaml").unwrap();
        let list_record = MediaUrn::from_string("media:list;record;textable;yaml").unwrap();
        let candidates: Vec<&MediaUrn> = vec![&scalar, &list, &record, &list_record];

        // Scalar opaque → picks scalar (no list, no record)
        let (idx, structure) = select_by_structure(&candidates, false, false).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(structure, ContentStructure::ScalarOpaque);

        // List opaque → picks list
        let (idx, structure) = select_by_structure(&candidates, true, false).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(structure, ContentStructure::ListOpaque);

        // Scalar record → picks record
        let (idx, structure) = select_by_structure(&candidates, false, true).unwrap();
        assert_eq!(idx, 2);
        assert_eq!(structure, ContentStructure::ScalarRecord);

        // List record → picks list_record
        let (idx, structure) = select_by_structure(&candidates, true, true).unwrap();
        assert_eq!(idx, 3);
        assert_eq!(structure, ContentStructure::ListRecord);
    }
}
