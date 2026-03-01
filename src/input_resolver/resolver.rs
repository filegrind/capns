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

    // TEST1090: 1 file scalar content
    #[test]
    fn test1090_single_file_scalar() {
        let dir = create_test_dir();
        let path = create_file(&dir, "doc.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert_eq!(result.cardinality, crate::planner::InputCardinality::Single);
    }

    // TEST1091: 1 file list content (CSV)
    #[test]
    fn test1091_single_file_list_content() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.csv", b"a,b,c\n1,2,3\n4,5,6");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert_eq!(
            result.cardinality,
            crate::planner::InputCardinality::Sequence
        );
        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListRecord
        );
    }

    // TEST1092: 2 files
    #[test]
    fn test1092_two_files() {
        let dir = create_test_dir();
        let path1 = create_file(&dir, "a.pdf", b"%PDF-1.4");
        let path2 = create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result =
            resolve_paths(&[path1.to_str().unwrap(), path2.to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 2);
        assert_eq!(
            result.cardinality,
            crate::planner::InputCardinality::Sequence
        );
    }

    // TEST1093: 1 dir with 1 file
    #[test]
    fn test1093_dir_single_file() {
        let dir = create_test_dir();
        create_file(&dir, "only.pdf", b"%PDF-1.4");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 1);
        assert_eq!(result.cardinality, crate::planner::InputCardinality::Single);
    }

    // TEST1094: 1 dir with 3 files
    #[test]
    fn test1094_dir_multiple_files() {
        let dir = create_test_dir();
        create_file(&dir, "a.txt", b"hello");
        create_file(&dir, "b.txt", b"world");
        create_file(&dir, "c.txt", b"test");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.files.len(), 3);
        assert_eq!(
            result.cardinality,
            crate::planner::InputCardinality::Sequence
        );
    }

    // TEST1098: Common media (all same type)
    #[test]
    fn test1098_common_media() {
        let dir = create_test_dir();
        create_file(&dir, "a.pdf", b"%PDF-1.4");
        create_file(&dir, "b.pdf", b"%PDF-1.5");

        let result = resolve_paths(&[dir.path().to_str().unwrap()]).unwrap();

        assert_eq!(result.common_media, Some("pdf".to_string()));
        assert!(result.is_homogeneous());
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

    // TEST1031 (integration): JSON object via resolve_paths
    #[test]
    fn test1031_resolve_json_object() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.json", br#"{"key": "value"}"#);

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ScalarRecord
        );
        assert!(result.files[0].media_urn.contains("record"));
    }

    // TEST1036 (integration): JSON array of objects via resolve_paths
    #[test]
    fn test1036_resolve_json_array_of_objects() {
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

    // TEST1045 (integration): NDJSON via resolve_paths
    #[test]
    fn test1045_resolve_ndjson() {
        let dir = create_test_dir();
        let path = create_file(&dir, "data.ndjson", b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListRecord
        );
        assert!(result.files[0].media_urn.contains("ndjson"));
    }

    // TEST1065 (integration): YAML mapping via resolve_paths
    #[test]
    fn test1065_resolve_yaml_mapping() {
        let dir = create_test_dir();
        let path = create_file(&dir, "config.yaml", b"key: value\nother: data");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ScalarRecord
        );
    }

    // TEST1067 (integration): YAML sequence via resolve_paths
    #[test]
    fn test1067_resolve_yaml_sequence() {
        let dir = create_test_dir();
        let path = create_file(&dir, "list.yaml", b"- item1\n- item2\n- item3");

        let result = resolve_paths(&[path.to_str().unwrap()]).unwrap();

        assert_eq!(
            result.files[0].content_structure,
            ContentStructure::ListOpaque
        );
    }

    // TEST1020 (integration): OS files excluded in resolve_paths
    #[test]
    fn test1020_os_files_excluded_integration() {
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
}
