//! MediaAdapterRegistry — collection of content inspection adapters
//!
//! The registry integrates with MediaUrnRegistry for extension-to-URN mapping.
//! Adapters provide content inspection to select the most appropriate URN
//! from the registry's candidates for a given file.

use std::path::Path;
use std::sync::Arc;

use crate::input_resolver::adapter::{AdapterResult, AdapterSelection, MediaAdapter, select_by_structure};
use crate::input_resolver::ContentStructure;
use crate::media::registry::MediaUrnRegistry;
use crate::urn::media_urn::MediaUrn;

use super::data::*;
use super::text::*;

/// A registered adapter with its parsed pattern URN
struct RegisteredAdapter {
    pattern: MediaUrn,
    adapter: Arc<dyn MediaAdapter>,
}

/// Registry of media content inspection adapters
///
/// This registry works with MediaUrnRegistry:
/// 1. MediaUrnRegistry provides extension -> candidate URN mapping (from specs)
/// 2. Adapters declare a pattern URN — candidates that conform to it are offered
/// 3. Adapters inspect content and select the best candidate
pub struct MediaAdapterRegistry {
    /// Adapters with their parsed pattern URNs
    adapters: Vec<RegisteredAdapter>,

    /// Reference to the media URN registry for extension lookups
    media_registry: Arc<MediaUrnRegistry>,
}

impl MediaAdapterRegistry {
    /// Create a new registry with the given MediaUrnRegistry
    pub fn new(media_registry: Arc<MediaUrnRegistry>) -> Self {
        let mut adapters = Vec::new();

        // Register content inspection adapters
        let adapter_defs: Vec<Arc<dyn MediaAdapter>> = vec![
            Arc::new(JsonAdapter),
            Arc::new(NdjsonAdapter),
            Arc::new(CsvAdapter),
            Arc::new(TsvAdapter),
            Arc::new(PsvAdapter),
            Arc::new(YamlAdapter),
            Arc::new(XmlAdapter),
            Arc::new(TomlAdapter),
            Arc::new(PlainTextAdapter),
        ];

        for adapter in adapter_defs {
            let pattern = MediaUrn::from_string(adapter.pattern_urn())
                .unwrap_or_else(|e| panic!(
                    "Adapter '{}' has invalid pattern URN '{}': {}",
                    adapter.name(), adapter.pattern_urn(), e
                ));
            adapters.push(RegisteredAdapter { pattern, adapter });
        }

        MediaAdapterRegistry {
            adapters,
            media_registry,
        }
    }

    /// Get the media URN registry
    pub fn media_registry(&self) -> &MediaUrnRegistry {
        &self.media_registry
    }

    /// Check if any content-inspecting adapter handles the given URN string.
    /// Returns true if the URN conforms to any registered adapter's pattern
    /// AND that adapter requires content inspection.
    pub fn has_content_adapter_for(&self, urn_str: &str) -> bool {
        let urn = match MediaUrn::from_string(urn_str) {
            Ok(u) => u,
            Err(_) => return false,
        };
        self.adapters.iter().any(|reg| {
            reg.adapter.requires_content_inspection()
                && urn.conforms_to(&reg.pattern).unwrap_or(false)
        })
    }

    /// Detect media type for a file
    ///
    /// Resolution flow:
    /// 1. Extract extension from path
    /// 2. Query MediaUrnRegistry for candidate URN(s) via extension
    /// 3. Parse candidate URN strings into MediaUrn objects
    /// 4. For each adapter whose pattern is accepted by any candidate,
    ///    ask the adapter to select the best candidate via content inspection
    /// 5. If exactly one adapter selects → return its result
    /// 6. If no adapter selects → return the least-specific candidate with
    ///    default structure derived from its marker tags
    /// 7. If multiple adapters select (tie) → fail hard
    pub fn detect(&self, path: &Path, content: &[u8]) -> AdapterResult {
        // Step 1: Get extension
        let ext = match path.extension().and_then(|e| e.to_str()) {
            Some(e) => e.to_lowercase(),
            None => {
                return AdapterResult {
                    media_urn: "media:".to_string(),
                    content_structure: ContentStructure::ScalarOpaque,
                };
            }
        };

        // Step 2: Query registry for candidate URN strings
        let candidate_strings = match self.media_registry.media_urns_for_extension(&ext) {
            Ok(urns) if !urns.is_empty() => urns,
            _ => {
                return AdapterResult {
                    media_urn: "media:".to_string(),
                    content_structure: ContentStructure::ScalarOpaque,
                };
            }
        };

        // Step 3: Parse candidate URN strings into MediaUrn objects
        let candidates: Vec<MediaUrn> = candidate_strings
            .iter()
            .filter_map(|s| MediaUrn::from_string(s).ok())
            .collect();

        if candidates.is_empty() {
            return AdapterResult {
                media_urn: "media:".to_string(),
                content_structure: ContentStructure::ScalarOpaque,
            };
        }

        // Step 4: For each adapter, find conforming candidates and ask for selection
        let mut selections: Vec<(String, AdapterResult)> = Vec::new();

        for reg in &self.adapters {
            // Find candidates that conform to this adapter's pattern
            let conforming: Vec<(usize, &MediaUrn)> = candidates
                .iter()
                .enumerate()
                .filter(|(_, c)| c.conforms_to(&reg.pattern).unwrap_or(false))
                .collect();

            if conforming.is_empty() {
                continue;
            }

            if reg.adapter.requires_content_inspection() {
                // Build a slice of conforming candidate refs for the adapter
                let conforming_refs: Vec<&MediaUrn> = conforming.iter().map(|(_, c)| *c).collect();
                if let Some(selection) = reg.adapter.select_candidate(&conforming_refs, path, content) {
                    let selected_urn = &conforming[selection.candidate_index].1;
                    selections.push((
                        reg.adapter.name().to_string(),
                        AdapterResult {
                            media_urn: selected_urn.to_string(),
                            content_structure: selection.content_structure,
                        },
                    ));
                }
            } else {
                // Non-inspecting adapter: pick the most specific conforming candidate
                let (best_idx, _) = conforming
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, (_, c))| c.specificity())
                    .unwrap(); // conforming is non-empty
                let selected = conforming[best_idx].1;
                let structure = structure_from_marker_tags(selected);
                selections.push((
                    reg.adapter.name().to_string(),
                    AdapterResult {
                        media_urn: selected.to_string(),
                        content_structure: structure,
                    },
                ));
            }
        }

        // Step 5-7: Select result
        match selections.len() {
            0 => {
                // No adapter matched — use least-specific candidate with default structure
                let least_specific = candidates
                    .iter()
                    .min_by_key(|c| c.specificity())
                    .unwrap(); // candidates is non-empty
                let structure = structure_from_marker_tags(least_specific);
                AdapterResult {
                    media_urn: least_specific.to_string(),
                    content_structure: structure,
                }
            }
            1 => selections.into_iter().next().unwrap().1,
            _ => {
                // Multiple adapters selected — fail hard
                let adapter_names: Vec<&str> = selections.iter().map(|(n, _)| n.as_str()).collect();
                panic!(
                    "Ambiguous adapter selection for '{}': adapters {:?} all claim to handle this file. \
                     The media spec registry must be disambiguated.",
                    path.display(),
                    adapter_names
                );
            }
        }
    }
}

/// Determine content structure from a MediaUrn's marker tags
fn structure_from_marker_tags(urn: &MediaUrn) -> ContentStructure {
    let has_list = urn.has_marker_tag("list");
    let has_record = urn.has_marker_tag("record");

    match (has_list, has_record) {
        (true, true) => ContentStructure::ListRecord,
        (true, false) => ContentStructure::ListOpaque,
        (false, true) => ContentStructure::ScalarRecord,
        (false, false) => ContentStructure::ScalarOpaque,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_registry() -> (Arc<MediaUrnRegistry>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let registry = MediaUrnRegistry::new_for_test(cache_dir).unwrap();
        (Arc::new(registry), temp_dir)
    }

    // TEST983: JSON object detection via MediaAdapterRegistry produces ScalarRecord
    #[test]
    fn test983_json_detection_via_adapter_registry() {
        let (media_registry, _temp) = create_test_registry();
        let adapter_registry = MediaAdapterRegistry::new(media_registry);

        let path = PathBuf::from("test.json");
        let content = br#"{"key": "value"}"#;
        let result = adapter_registry.detect(&path, content);

        assert_eq!(result.content_structure, ContentStructure::ScalarRecord);
    }

    // TEST984: YAML sequence detection via MediaAdapterRegistry produces ListOpaque
    #[test]
    fn test984_yaml_detection_via_adapter_registry() {
        let (media_registry, _temp) = create_test_registry();
        let adapter_registry = MediaAdapterRegistry::new(media_registry);

        let path = PathBuf::from("list.yaml");
        let content = b"- item1\n- item2\n- item3";
        let result = adapter_registry.detect(&path, content);

        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
    }

    // TEST985: CSV detection via MediaAdapterRegistry
    #[test]
    fn test985_csv_detection_via_adapter_registry() {
        let (media_registry, _temp) = create_test_registry();
        let adapter_registry = MediaAdapterRegistry::new(media_registry);

        let path = PathBuf::from("data.csv");
        let content = b"name,age\nAlice,30\nBob,25";
        let result = adapter_registry.detect(&path, content);

        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    // TEST986: Unknown extension returns generic media URN
    #[test]
    fn test986_unknown_extension_returns_generic() {
        let (media_registry, _temp) = create_test_registry();
        let adapter_registry = MediaAdapterRegistry::new(media_registry);

        let path = PathBuf::from("file.xyz123");
        let content = b"some content";
        let result = adapter_registry.detect(&path, content);

        assert_eq!(result.media_urn, "media:");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }
}
