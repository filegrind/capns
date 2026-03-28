//! MediaAdapterRegistry — collection of content inspection adapters
//!
//! The registry integrates with MediaUrnRegistry for extension-to-URN mapping.
//! Adapters only provide content inspection to refine the base URN with markers.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};
use crate::input_resolver::ContentStructure;
use crate::media::registry::MediaUrnRegistry;

use super::data::*;
use super::text::*;

/// Registry of media content inspection adapters
///
/// This registry works with MediaUrnRegistry:
/// 1. MediaUrnRegistry provides extension -> base URN mapping (from TOML specs)
/// 2. Adapters are registered for base URNs that need content inspection
/// 3. Adapters refine the URN with list/record markers based on content
pub struct MediaAdapterRegistry {
    /// Adapters indexed by base URN they can refine
    /// e.g., "media:json" -> JsonAdapter
    adapters_by_urn: HashMap<String, Arc<dyn MediaAdapter>>,

    /// Reference to the media URN registry for extension lookups
    media_registry: Arc<MediaUrnRegistry>,
}

impl MediaAdapterRegistry {
    /// Create a new registry with the given MediaUrnRegistry
    pub fn new(media_registry: Arc<MediaUrnRegistry>) -> Self {
        let mut adapters_by_urn: HashMap<String, Arc<dyn MediaAdapter>> = HashMap::new();

        // Register content inspection adapters for URNs that need them
        // These adapters determine list/record markers based on content

        // Data interchange formats - require content inspection
        adapters_by_urn.insert("media:json".to_string(), Arc::new(JsonAdapter));
        adapters_by_urn.insert("media:ndjson".to_string(), Arc::new(NdjsonAdapter));
        adapters_by_urn.insert("media:csv".to_string(), Arc::new(CsvAdapter));
        adapters_by_urn.insert("media:tsv".to_string(), Arc::new(TsvAdapter));
        adapters_by_urn.insert("media:psv".to_string(), Arc::new(PsvAdapter));
        adapters_by_urn.insert("media:yaml".to_string(), Arc::new(YamlAdapter));
        adapters_by_urn.insert("media:xml".to_string(), Arc::new(XmlAdapter));

        // Text files that may need inspection
        adapters_by_urn.insert("media:txt".to_string(), Arc::new(PlainTextAdapter));

        MediaAdapterRegistry {
            adapters_by_urn,
            media_registry,
        }
    }

    /// Get the media URN registry
    pub fn media_registry(&self) -> &MediaUrnRegistry {
        &self.media_registry
    }

    /// Detect media type for a file
    ///
    /// Resolution flow:
    /// 1. Extract extension from path
    /// 2. Query MediaUrnRegistry for base URN(s) via extension
    /// 3. If adapter exists for base URN and needs inspection, use it
    /// 4. Otherwise return base URN with default structure
    ///
    /// - `path`: File path
    /// - `content`: File content (full or prefix for inspection)
    pub fn detect(&self, path: &Path, content: &[u8]) -> AdapterResult {
        // Step 1: Get extension
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase());

        let ext = match ext {
            Some(e) => e,
            None => {
                // No extension - return generic media URN
                return AdapterResult {
                    media_urn: "media:".to_string(),
                    content_structure: ContentStructure::ScalarOpaque,
                };
            }
        };

        // Step 2: Query registry for base URN(s)
        let base_urns = match self.media_registry.media_urns_for_extension(&ext) {
            Ok(urns) => urns,
            Err(_) => {
                // Extension not found in registry - return generic
                return AdapterResult {
                    media_urn: "media:".to_string(),
                    content_structure: ContentStructure::ScalarOpaque,
                };
            }
        };

        if base_urns.is_empty() {
            return AdapterResult {
                media_urn: "media:".to_string(),
                content_structure: ContentStructure::ScalarOpaque,
            };
        }

        // Step 3: Find the best URN - prefer ones with adapters for content inspection
        // Also prefer URNs that match the extension more closely (e.g., "media:json" for .json)
        let (selected_urn, adapter) = self.select_best_urn_for_extension(&ext, &base_urns);

        // Step 4: If adapter exists and needs inspection, use it
        if let Some(adapter) = adapter {
            if adapter.requires_content_inspection() {
                return adapter.detect(path, content);
            }
        }

        // Step 5: No adapter or no inspection needed - determine default structure
        let content_structure = determine_default_structure(&selected_urn);

        AdapterResult {
            media_urn: selected_urn,
            content_structure,
        }
    }

    /// Select the best URN for an extension from multiple candidates
    ///
    /// Priority order:
    /// 1. URNs with matching adapters (for content inspection)
    /// 2. URNs where the base type matches the extension (e.g., "media:json" for .json)
    /// 3. First URN in the list
    fn select_best_urn_for_extension(
        &self,
        ext: &str,
        urns: &[String],
    ) -> (String, Option<Arc<dyn MediaAdapter>>) {
        // First, try to find a URN with a matching adapter where base type matches extension
        for urn in urns {
            let base_key = extract_base_urn(urn);
            // Check if base type matches extension (e.g., "media:json" for "json" extension)
            if base_key == format!("media:{}", ext) {
                if let Some(adapter) = self.adapters_by_urn.get(&base_key) {
                    return (urn.clone(), Some(adapter.clone()));
                }
            }
        }

        // Second, try any URN with a matching adapter
        for urn in urns {
            let base_key = extract_base_urn(urn);
            if let Some(adapter) = self.adapters_by_urn.get(&base_key) {
                return (urn.clone(), Some(adapter.clone()));
            }
        }

        // Third, try to find a URN where base type matches extension (even without adapter)
        for urn in urns {
            let base_key = extract_base_urn(urn);
            if base_key == format!("media:{}", ext) {
                return (urn.clone(), None);
            }
        }

        // Fallback: use first URN
        (urns[0].clone(), None)
    }

    /// Check if an adapter exists for the given base URN
    pub fn has_adapter(&self, base_urn: &str) -> bool {
        let key = extract_base_urn(base_urn);
        self.adapters_by_urn.contains_key(&key)
    }

    /// Get all registered adapter URNs
    pub fn adapter_urns(&self) -> Vec<&str> {
        self.adapters_by_urn.keys().map(|s| s.as_str()).collect()
    }
}

/// Extract base URN without markers
/// e.g., "media:json;textable;record" -> "media:json"
pub fn extract_base_urn(urn: &str) -> String {
    if let Some(semicolon_pos) = urn.find(';') {
        urn[..semicolon_pos].to_string()
    } else {
        urn.to_string()
    }
}

/// Determine default content structure based on URN markers
fn determine_default_structure(urn: &str) -> ContentStructure {
    let has_list = urn.contains(";list");
    let has_record = urn.contains(";record");

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

    // Internal tests for MediaAdapterRegistry helper functions
    // These test internal implementation details and integration with MediaUrnRegistry

    fn create_test_registry() -> (Arc<MediaUrnRegistry>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let registry = MediaUrnRegistry::new_for_test(cache_dir).unwrap();
        (Arc::new(registry), temp_dir)
    }

    // TEST983 (registry integration): JSON detection via MediaAdapterRegistry
    #[test]
    fn test983_json_detection_via_adapter_registry() {
        let (media_registry, _temp) = create_test_registry();
        let adapter_registry = MediaAdapterRegistry::new(media_registry.clone());

        // Check what URNs are registered for .json extension
        let urns = media_registry.media_urns_for_extension("json");
        tracing::info!("URNs for .json: {:?}", urns);

        // Now detect a JSON file
        let path = PathBuf::from("test.json");
        let content = br#"{"key": "value"}"#;
        let result = adapter_registry.detect(&path, content);

        tracing::info!("Detection result: {:?}", result);
        assert_eq!(result.content_structure, ContentStructure::ScalarRecord);
        assert!(result.media_urn.contains("record"));
    }

    // Internal helper test: extract_base_urn
    #[test]
    fn test_extract_base_urn() {
        assert_eq!(extract_base_urn("media:json"), "media:json");
        assert_eq!(extract_base_urn("media:json;textable"), "media:json");
        assert_eq!(extract_base_urn("media:json;list;record;textable"), "media:json");
        assert_eq!(extract_base_urn("media:"), "media:");
    }

    #[test]
    fn test_determine_default_structure() {
        assert_eq!(
            determine_default_structure("media:pdf"),
            ContentStructure::ScalarOpaque
        );
        assert_eq!(
            determine_default_structure("media:json;record;textable"),
            ContentStructure::ScalarRecord
        );
        assert_eq!(
            determine_default_structure("media:csv;list;textable"),
            ContentStructure::ListOpaque
        );
        assert_eq!(
            determine_default_structure("media:csv;list;record;textable"),
            ContentStructure::ListRecord
        );
    }

    #[test]
    fn test_registry_has_adapters() {
        let (media_registry, _temp) = create_test_registry();
        let registry = MediaAdapterRegistry::new(media_registry);

        // Should have adapters for data interchange formats
        assert!(registry.has_adapter("media:json"));
        assert!(registry.has_adapter("media:yaml"));
        assert!(registry.has_adapter("media:csv"));

        // Should NOT have adapters for binary formats (no inspection needed)
        assert!(!registry.has_adapter("media:pdf"));
        assert!(!registry.has_adapter("media:png"));
    }
}
