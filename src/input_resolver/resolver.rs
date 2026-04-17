//! Main resolver — combines path resolution with media detection

use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use crate::input_resolver::{
    ContentStructure, InputItem, InputResolverError, ResolvedFile, ResolvedInputSet,
};
use crate::input_resolver::adapters::MediaAdapterRegistry;
use crate::input_resolver::path_resolver;
use crate::media::registry::MediaUrnRegistry;

/// Maximum content to read for inspection (64 KB)
const MAX_INSPECTION_SIZE: usize = 64 * 1024;

/// Discriminate candidate media URNs by validation rules in their specs.
///
/// Given file content and a set of candidate URN strings (e.g. all URNs for
/// a file extension), eliminates candidates whose media spec validation
/// rules reject the content. Candidates with no validation rules survive
/// (no rules = no basis for elimination).
///
/// Returns the surviving candidate URN strings in their original order.
/// Discriminate candidates using validation rules and adapter baseline.
///
/// The baseline URN is the adapter's structural detection result (e.g., "media:json;record;textable"
/// for a JSON object). Candidates more specific than the baseline must have validation rules
/// that positively match the content — otherwise they're eliminated (they overclaim without proof).
/// Candidates equivalent to or less specific than the baseline survive without validation.
pub fn discriminate_candidates_by_validation(
    content: &[u8],
    candidate_urns: &[String],
    media_registry: &MediaUrnRegistry,
    baseline_urn: &str,
) -> Vec<String> {
    let content_str = std::str::from_utf8(content).ok();
    let content_len = content.len();

    let baseline = crate::urn::media_urn::MediaUrn::from_string(baseline_urn)
        .unwrap_or_else(|e| panic!(
            "discriminate_candidates_by_validation: invalid baseline URN '{}': {}",
            baseline_urn, e
        ));

    candidate_urns
        .iter()
        .filter(|urn| {
            let spec = match media_registry.get_cached_spec(urn) {
                Some(spec) => spec,
                None => return true, // No spec in cache → cannot eliminate
            };

            let validation = match &spec.validation {
                Some(v) if !v.is_empty() => v,
                _ => {
                    // No validation rules. Only keep if the candidate is not more
                    // specific than the baseline (more specific without validation = overclaiming).
                    let candidate_urn = match crate::urn::media_urn::MediaUrn::from_string(urn) {
                        Ok(u) => u,
                        Err(_) => return true, // Can't parse → keep
                    };
                    // Keep if baseline conforms to candidate (candidate is same or more general)
                    return baseline.conforms_to(&candidate_urn).unwrap_or(true);
                }
            };

            // Check pattern (regex against content as UTF-8)
            if let Some(ref pattern) = validation.pattern {
                match content_str {
                    Some(text) => {
                        match regex::Regex::new(pattern) {
                            Ok(re) => {
                                if !re.is_match(text) {
                                    return false; // Pattern didn't match → eliminate
                                }
                            }
                            Err(_) => {
                                // Invalid regex in spec is a spec authoring bug — hard fail
                                // would block all candidates with that spec. Log and keep
                                // the candidate (don't eliminate based on broken rule).
                                tracing::error!(
                                    "Media spec '{}' has invalid validation pattern '{}' — \
                                     fix the TOML definition in capgraph/src/media",
                                    urn, pattern
                                );
                            }
                        }
                    }
                    None => {
                        // Binary content cannot match a text pattern → eliminate
                        return false;
                    }
                }
            }

            // Check min_length (byte length)
            if let Some(min_len) = validation.min_length {
                if content_len < min_len {
                    return false;
                }
            }

            // Check max_length (byte length)
            if let Some(max_len) = validation.max_length {
                if content_len > max_len {
                    return false;
                }
            }

            // Check allowed_values
            if let Some(ref allowed) = validation.allowed_values {
                match content_str {
                    Some(text) => {
                        let trimmed = text.trim();
                        if !allowed.iter().any(|v| v == trimmed) {
                            return false;
                        }
                    }
                    None => return false, // Binary content can't match allowed text values
                }
            }

            true // Survived all checks
        })
        .cloned()
        .collect()
}

/// Global adapter registry (lazily initialized with bundled MediaUrnRegistry)
fn get_registry() -> &'static MediaAdapterRegistry {
    static REGISTRY: OnceLock<MediaAdapterRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        // Create MediaUrnRegistry synchronously for bundled specs
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join("capdag_media_registry"),
        )
        .expect("Failed to create MediaUrnRegistry");
        MediaAdapterRegistry::new(Arc::new(media_registry))
    })
}

/// Resolve a single input item
pub fn resolve_input(item: InputItem) -> Result<ResolvedInputSet, InputResolverError> {
    resolve_inputs(vec![item])
}

/// Resolve multiple input items
pub fn resolve_inputs(items: Vec<InputItem>) -> Result<ResolvedInputSet, InputResolverError> {
    // Step 1: Resolve paths to file list
    let paths = path_resolver::resolve_items(&items)?;

    // Step 2: Detect media type for each file
    let registry = get_registry();
    let mut files = Vec::with_capacity(paths.len());

    for path in paths {
        let resolved = detect_file_with_registry(&path, registry)?;
        files.push(resolved);
    }

    if files.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }

    Ok(ResolvedInputSet::new(files))
}

/// Resolve from string paths with a custom MediaUrnRegistry
///
/// Use this when you have your own MediaUrnRegistry instance
pub fn resolve_paths_with_registry(
    paths: &[&str],
    media_registry: Arc<MediaUrnRegistry>,
) -> Result<ResolvedInputSet, InputResolverError> {
    let items: Vec<InputItem> = paths.iter().map(|s| InputItem::from_string(s)).collect();

    let adapter_registry = MediaAdapterRegistry::new(media_registry);

    // Step 1: Resolve paths to file list
    let resolved_paths = path_resolver::resolve_items(&items)?;

    // Step 2: Detect media type for each file
    let mut files = Vec::with_capacity(resolved_paths.len());

    for path in resolved_paths {
        let resolved = detect_file_with_adapter_registry(&path, &adapter_registry)?;
        files.push(resolved);
    }

    if files.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }

    Ok(ResolvedInputSet::new(files))
}

/// Convenience: resolve from string paths (auto-detect file/dir/glob)
pub fn resolve_paths(paths: &[&str]) -> Result<ResolvedInputSet, InputResolverError> {
    let items: Vec<InputItem> = paths.iter().map(|s| InputItem::from_string(s)).collect();

    resolve_inputs(items)
}

/// Detect media type for a single file
pub fn detect_file(path: &Path) -> Result<ResolvedFile, InputResolverError> {
    detect_file_with_registry(path, get_registry())
}

/// Detect media type for a single file with a custom MediaUrnRegistry
pub fn detect_file_with_media_registry(
    path: &Path,
    media_registry: Arc<MediaUrnRegistry>,
) -> Result<ResolvedFile, InputResolverError> {
    let adapter_registry = MediaAdapterRegistry::new(media_registry);
    detect_file_with_adapter_registry(path, &adapter_registry)
}

/// Detect media type using a specific adapter registry
fn detect_file_with_registry(
    path: &Path,
    registry: &MediaAdapterRegistry,
) -> Result<ResolvedFile, InputResolverError> {
    detect_file_with_adapter_registry(path, registry)
}

/// Detect media type using a specific adapter registry
fn detect_file_with_adapter_registry(
    path: &Path,
    registry: &MediaAdapterRegistry,
) -> Result<ResolvedFile, InputResolverError> {
    // Get file metadata
    let metadata = fs::metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            InputResolverError::NotFound(path.to_path_buf())
        } else if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(path.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            }
        }
    })?;

    let size_bytes = metadata.len();

    // Read content for inspection (up to MAX_INSPECTION_SIZE)
    let content = read_content_for_inspection(path, size_bytes)?;

    // Detect media type
    let result = registry.detect(path, &content);

    Ok(ResolvedFile {
        path: path.to_path_buf(),
        media_urn: result.media_urn,
        size_bytes,
        content_structure: result.content_structure,
    })
}

/// Read file content for inspection
fn read_content_for_inspection(path: &Path, size: u64) -> Result<Vec<u8>, InputResolverError> {
    use std::io::Read;

    let mut file = fs::File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(path.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            }
        }
    })?;

    let read_size = (size as usize).min(MAX_INSPECTION_SIZE);
    let mut buffer = vec![0u8; read_size];

    let bytes_read = file
        .read(&mut buffer)
        .map_err(|e| InputResolverError::IoError {
            path: path.to_path_buf(),
            error: e,
        })?;

    buffer.truncate(bytes_read);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn create_file(dir: &TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(content).unwrap();
        path
    }

    // Aggregate Cardinality Tests (TEST1090-TEST1099)

    // TEST1090: 1 file scalar content → is_sequence=false (one file)
    #[test]
    fn test1090_single_file_scalar() {
        let dir = create_test_dir();
        let path = create_file(&dir, "doc.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(!result.is_sequence, "single file must be is_sequence=false");
    }

    // TEST1091: 1 file with list content (CSV) → is_sequence=false.
    // Content structure is ListRecord (the file contains tabular data),
    // but is_sequence is false because there is only one file.
    // Content structure ≠ input cardinality.
    #[test]
    fn test1091_single_file_list_content() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.csv", b"a,b,c\n1,2,3\n4,5,6");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(!result.is_sequence, "single file must be is_sequence=false regardless of content structure");
        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListRecord
        );
    }

    // TEST1092: 2 files → is_sequence=true
    #[test]
    fn test1092_two_files() {
        let dir = create_test_dir();
        let path1 = create_file(&dir, "a.pdf", b"%PDF-1.4");
        let path2 = create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result =
            resolve_paths(&[path1.to_str().unwrap(), path2.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 2);
        assert!(result.is_sequence, "multiple files must be is_sequence=true");
    }

    // TEST1093: 1 dir with 1 file → is_sequence=false
    #[test]
    fn test1093_dir_single_file() {
        let dir = create_test_dir();
        create_file(&dir, "only.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(!result.is_sequence, "directory with single file must be is_sequence=false");
    }

    // TEST1094: 1 dir with 3 files → is_sequence=true
    #[test]
    fn test1094_dir_multiple_files() {
        let dir = create_test_dir();
        create_file(&dir, "a.txt", b"hello");
        create_file(&dir, "b.txt", b"world");
        create_file(&dir, "c.txt", b"test");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 3);
        assert!(result.is_sequence, "directory with multiple files must be is_sequence=true");
    }

    // TEST1098: Common media (all same type)
    #[test]
    fn test1098_common_media() {
        let dir = create_test_dir();
        create_file(&dir, "a.pdf", b"%PDF-1.4");
        create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert!(result.is_homogeneous(), "two PDFs must be homogeneous");
        // common_media is the full equivalent URN, not a string-split base
        let common = result.common_media.as_ref().unwrap();
        let common_urn = crate::urn::media_urn::MediaUrn::from_string(common).unwrap();
        assert!(
            common_urn.has_marker_tag("pdf"),
            "common media for PDFs must have pdf tag, got: {}",
            common
        );
    }

    // TEST1099: Heterogeneous (mixed types)
    #[test]
    fn test1099_heterogeneous() {
        let dir = create_test_dir();
        create_file(&dir, "doc.pdf", b"%PDF-1.4");
        create_file(&dir, "img.png", b"\x89PNG\r\n\x1a\n");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.common_media, None);
        assert!(!result.is_homogeneous());
    }

    // Integration Tests - resolve_paths with content detection

    // TEST978 (integration): JSON object via resolve_paths
    #[test]
    fn test978_resolve_json_object() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"key": "value"}"#);

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ScalarRecord
        );
        assert!(result.files[0].media_urn.contains("record"));
    }

    // TEST979 (integration): JSON array of objects via resolve_paths
    #[test]
    fn test979_resolve_json_array_of_objects() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"[{"a": 1}, {"b": 2}]"#);

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListRecord
        );
        assert!(result.files[0].media_urn.contains("list"));
        assert!(result.files[0].media_urn.contains("record"));
    }

    // TEST980 (integration): NDJSON via resolve_paths
    #[test]
    fn test980_resolve_ndjson() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.ndjson", b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListRecord
        );
        assert!(result.files[0].media_urn.contains("ndjson"));
    }

    // TEST981 (integration): YAML mapping via resolve_paths
    #[test]
    fn test981_resolve_yaml_mapping() {
        let dir = create_test_dir();
        let path = create_file(&dir, "config.yaml", b"key: value\nother: data");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ScalarRecord
        );
    }

    // TEST982 (integration): YAML sequence via resolve_paths
    #[test]
    fn test982_resolve_yaml_sequence() {
        let dir = create_test_dir();
        let path = create_file(&dir, "list.yaml", b"- item1\n- item2\n- item3");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListOpaque
        );
    }

    // TEST977 (integration): OS files excluded in resolve_paths
    #[test]
    fn test977_os_files_excluded_integration() {
        let dir = create_test_dir();
        create_file(&dir, ".DS_Store", b"");
        create_file(&dir, "real.txt", b"content");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert!(result.files[0].path.to_str().unwrap().contains("real.txt"));
    }

    // TEST1095/1096 (integration): Glob with detection
    #[test]
    fn test1095_glob_with_detection() {
        let dir = create_test_dir();
        create_file(&dir, "a.json", br#"{"x": 1}"#);
        create_file(&dir, "b.json", br#"[1, 2, 3]"#);

        let pattern = format!("{}/*.json", dir.path().display());
        let result = resolve_paths(&[&pattern]).unwrap();

        assert_eq!(result.files.len(), 2);
        // Both should be detected as JSON with correct structures
    }

    // Content Analysis Tests (TEST_CA_1 through TEST_CA_4)
    // These test detect_file_with_media_registry — the function used by the
    // AnalyzeFileContent gRPC handler to determine precise media URNs.

    // TEST_CA_1: JSON object file resolves to record URN
    #[test]
    fn test_ca_1_json_object_detection() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"name": "test", "count": 42}"#);

        let (registry, _temp) = create_test_media_registry();
        let resolved = detect_file_with_media_registry(&path, registry).unwrap();

        assert!(
            resolved.media_urn.contains("record"),
            "JSON object should be detected as record, got: {}",
            resolved.media_urn
        );
        assert_eq!(
            resolved.content_structure,
            ContentStructure::ScalarRecord,
            "JSON object should have ScalarRecord structure"
        );
    }

    // TEST_CA_2: JSON array-of-objects resolves to list+record URN
    #[test]
    fn test_ca_2_json_array_detection() {
        let dir = create_test_dir();
        let path = create_file(&dir, "items.json", br#"[{"a":1},{"b":2},{"c":3}]"#);

        let (registry, _temp) = create_test_media_registry();
        let resolved = detect_file_with_media_registry(&path, registry).unwrap();

        assert!(
            resolved.media_urn.contains("list"),
            "JSON array should be detected as list, got: {}",
            resolved.media_urn
        );
        assert!(
            resolved.media_urn.contains("record"),
            "JSON array of objects should be detected as record, got: {}",
            resolved.media_urn
        );
        assert_eq!(
            resolved.content_structure,
            ContentStructure::ListRecord,
            "JSON array of objects should have ListRecord structure"
        );
    }

    // TEST_CA_3: Directory of JSON object files → all resolve to record
    #[test]
    fn test_ca_3_directory_json_objects_lub() {
        let dir = create_test_dir();
        create_file(&dir, "a.json", br#"{"key": "alpha"}"#);
        create_file(&dir, "b.json", br#"{"key": "beta"}"#);
        create_file(&dir, "c.json", br#"{"key": "gamma"}"#);

        let (registry, _temp) = create_test_media_registry();
        let dir_files = super::path_resolver::resolve_directory(dir.path()).unwrap();
        assert_eq!(dir_files.len(), 3);

        let mut detected_urns = Vec::new();
        for file_path in &dir_files {
            let resolved = detect_file_with_media_registry(file_path, registry.clone()).unwrap();
            let urn = crate::MediaUrn::from_string(&resolved.media_urn).unwrap();
            detected_urns.push(urn);
        }

        let lub = crate::MediaUrn::least_upper_bound(&detected_urns);
        let lub_str = lub.to_string();
        assert!(
            lub_str.contains("json"),
            "LUB of all JSON files should contain json tag, got: {}",
            lub_str
        );
        assert!(
            lub_str.contains("record"),
            "LUB of all JSON object files should contain record tag, got: {}",
            lub_str
        );
    }

    // TEST_CA_4: Directory with mixed JSON and CSV → LUB drops format-specific tags
    #[test]
    fn test_ca_4_directory_mixed_types_lub() {
        let dir = create_test_dir();
        create_file(&dir, "data.json", br#"{"key": "value"}"#);
        create_file(&dir, "data.csv", b"a,b,c\n1,2,3\n4,5,6");

        let (registry, _temp) = create_test_media_registry();
        let dir_files = super::path_resolver::resolve_directory(dir.path()).unwrap();
        assert_eq!(dir_files.len(), 2);

        let mut detected_urns = Vec::new();
        for file_path in &dir_files {
            let resolved = detect_file_with_media_registry(file_path, registry.clone()).unwrap();
            let urn = crate::MediaUrn::from_string(&resolved.media_urn).unwrap();
            detected_urns.push(urn);
        }

        let lub = crate::MediaUrn::least_upper_bound(&detected_urns);
        let lub_str = lub.to_string();
        // JSON and CSV are both textable+record but have different base types
        // LUB should drop json and csv tags, keeping only shared markers
        assert!(
            !lub_str.contains("json"),
            "LUB of JSON+CSV should not contain json tag, got: {}",
            lub_str
        );
        assert!(
            !lub_str.contains("csv"),
            "LUB of JSON+CSV should not contain csv tag, got: {}",
            lub_str
        );
        assert!(
            lub_str.contains("record") || lub_str.contains("textable"),
            "LUB of JSON+CSV should contain shared markers (record or textable), got: {}",
            lub_str
        );
    }

    fn create_test_media_registry() -> (Arc<crate::media::registry::MediaUrnRegistry>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let registry = crate::media::registry::MediaUrnRegistry::new_for_test(cache_dir).unwrap();
        (Arc::new(registry), temp_dir)
    }

    // Discrimination Tests (TEST_DISC_1 through TEST_DISC_6)
    // These test discriminate_candidates_by_validation against real bundled media specs.

    /// Helper: get all txt-extension URNs from the bundled registry.
    fn txt_extension_urns(registry: &crate::media::registry::MediaUrnRegistry) -> Vec<String> {
        registry.media_urns_for_extension("txt").unwrap()
    }

    // TEST_DISC_1: Plain text content ("Hello world") with baseline media:list;textable;txt
    // eliminates model-spec variants (they fail pattern ".*:.*") AND eliminates
    // URNs more specific than the baseline that lack validation.
    #[test]
    fn test_disc_1_plain_text_eliminates_model_specs() {
        let (registry, _temp) = create_test_media_registry();
        let all_txt_urns = txt_extension_urns(&registry);

        let content = b"Hello world\nThis is a plain text file\nNo colons here";
        // Baseline: adapter detected multi-line plain text
        let baseline = "media:list;textable;txt";
        let survivors = discriminate_candidates_by_validation(content, &all_txt_urns, &registry, baseline);

        // model-spec variants have pattern ".*:.*" — plain text without colons fails this
        for survivor in &survivors {
            assert!(
                !survivor.contains("model-spec"),
                "model-spec URN '{}' should have been eliminated — content has no colon",
                survivor
            );
        }
    }

    // TEST_DISC_2: Model spec content ("hf:MaziyarPanahi/Mistral-7B") passes the pattern.
    #[test]
    fn test_disc_2_model_spec_content_survives_pattern() {
        let (registry, _temp) = create_test_media_registry();
        let all_txt_urns = txt_extension_urns(&registry);

        let content = b"hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF";
        // Baseline: adapter detected single-line plain text
        let baseline = "media:textable;txt";
        let survivors = discriminate_candidates_by_validation(content, &all_txt_urns, &registry, baseline);

        // Content has a colon → model-spec pattern matches → model-spec variants survive
        assert!(
            survivors.iter().any(|u| u.contains("model-spec")),
            "At least one model-spec URN should survive — content contains a colon, survivors: {:?}",
            survivors
        );
    }

    // TEST_DISC_5: Empty candidate list → empty result.
    #[test]
    fn test_disc_5_empty_candidates() {
        let (registry, _temp) = create_test_media_registry();
        let survivors = discriminate_candidates_by_validation(b"anything", &[], &registry, "media:");
        assert!(survivors.is_empty());
    }

    // TEST_DISC_6: URN not in registry cache → survives (cannot eliminate what we can't look up).
    #[test]
    fn test_disc_6_unknown_urn_survives() {
        let (registry, _temp) = create_test_media_registry();
        let candidates = vec!["media:nonexistent;fake".to_string()];
        let survivors = discriminate_candidates_by_validation(b"anything", &candidates, &registry, "media:");
        assert_eq!(survivors, candidates, "Unknown URN should survive — no spec to eliminate it");
    }
}
