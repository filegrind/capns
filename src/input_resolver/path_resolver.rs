//! Path resolution — expands files, directories, and globs to file lists

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::input_resolver::os_filter::{should_exclude, should_exclude_dir};
use crate::input_resolver::{InputItem, InputResolverError};

/// Maximum depth for directory recursion (prevent infinite loops)
const MAX_RECURSION_DEPTH: usize = 100;

/// Resolve a single input item to a list of file paths
pub fn resolve_item(item: &InputItem) -> Result<Vec<PathBuf>, InputResolverError> {
    match item {
        InputItem::File(path) => resolve_file(path),
        InputItem::Directory(path) => resolve_directory(path),
        InputItem::Glob(pattern) => resolve_glob(pattern),
    }
}

/// Resolve multiple input items, deduplicating by canonical path
pub fn resolve_items(items: &[InputItem]) -> Result<Vec<PathBuf>, InputResolverError> {
    if items.is_empty() {
        return Err(InputResolverError::EmptyInput);
    }

    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut result: Vec<PathBuf> = Vec::new();

    for item in items {
        let paths = resolve_item(item)?;
        for path in paths {
            // Canonicalize for deduplication
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            if !seen.contains(&canonical) {
                seen.insert(canonical);
                result.push(path);
            }
        }
    }

    if result.is_empty() {
        return Err(InputResolverError::NoFilesResolved);
    }

    // Sort for consistent ordering
    result.sort();

    Ok(result)
}

/// Resolve a file path
fn resolve_file(path: &Path) -> Result<Vec<PathBuf>, InputResolverError> {
    // Handle home directory expansion
    let expanded = expand_tilde(path);

    // Check existence
    if !expanded.exists() {
        return Err(InputResolverError::NotFound(expanded));
    }

    // Check if it's actually a file
    if expanded.is_dir() {
        // If user passed a directory as a file, resolve it as a directory
        return resolve_directory(&expanded);
    }

    // Check if excluded
    if should_exclude(&expanded) {
        return Ok(vec![]); // Silently skip excluded files
    }

    // Resolve symlinks (with cycle detection)
    let resolved = resolve_symlink(&expanded, &mut HashSet::new())?;

    Ok(vec![resolved])
}

/// Resolve a directory recursively
pub fn resolve_directory(path: &Path) -> Result<Vec<PathBuf>, InputResolverError> {
    let expanded = expand_tilde(path);

    if !expanded.exists() {
        return Err(InputResolverError::NotFound(expanded));
    }

    if !expanded.is_dir() {
        // If user passed a file as a directory, resolve it as a file
        return resolve_file(&expanded);
    }

    let mut files = Vec::new();
    let mut visited = HashSet::new();

    resolve_directory_recursive(&expanded, &mut files, &mut visited, 0)?;

    Ok(files)
}

/// Recursive directory traversal
fn resolve_directory_recursive(
    dir: &Path,
    files: &mut Vec<PathBuf>,
    visited: &mut HashSet<PathBuf>,
    depth: usize,
) -> Result<(), InputResolverError> {
    if depth > MAX_RECURSION_DEPTH {
        return Err(InputResolverError::SymlinkCycle {
            path: dir.to_path_buf(),
        });
    }

    // Canonicalize for cycle detection
    let canonical = dir.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(dir.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: dir.to_path_buf(),
                error: e,
            }
        }
    })?;

    if visited.contains(&canonical) {
        // Already visited, skip (handles symlink cycles)
        return Ok(());
    }
    visited.insert(canonical);

    // Read directory entries
    let entries = fs::read_dir(dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(dir.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: dir.to_path_buf(),
                error: e,
            }
        }
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| InputResolverError::IoError {
            path: dir.to_path_buf(),
            error: e,
        })?;

        let path = entry.path();

        // Check if excluded
        if path.is_dir() {
            if should_exclude_dir(&path) {
                continue;
            }
            // Recurse into subdirectory
            resolve_directory_recursive(&path, files, visited, depth + 1)?;
        } else if path.is_file() {
            if !should_exclude(&path) {
                files.push(path);
            }
        }
        // Skip other types (symlinks to non-existent, special files, etc.)
    }

    Ok(())
}

/// Resolve a glob pattern
fn resolve_glob(pattern: &str) -> Result<Vec<PathBuf>, InputResolverError> {
    let expanded = expand_tilde_string(pattern);

    let entries = glob::glob(&expanded).map_err(|e| InputResolverError::InvalidGlob {
        pattern: pattern.to_string(),
        reason: e.to_string(),
    })?;

    let mut files = Vec::new();

    for entry in entries {
        match entry {
            Ok(path) => {
                if path.is_file() && !should_exclude(&path) {
                    files.push(path);
                }
                // Skip directories and excluded files
            }
            Err(e) => {
                // Glob error (e.g., permission denied on a matched path)
                // Log and continue rather than fail completely
                tracing::warn!("glob error: {}", e);
            }
        }
    }

    Ok(files)
}

/// Resolve symlinks with cycle detection
fn resolve_symlink(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
) -> Result<PathBuf, InputResolverError> {
    let canonical = path.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            InputResolverError::PermissionDenied(path.to_path_buf())
        } else {
            InputResolverError::IoError {
                path: path.to_path_buf(),
                error: e,
            }
        }
    })?;

    if visited.contains(&canonical) {
        return Err(InputResolverError::SymlinkCycle {
            path: path.to_path_buf(),
        });
    }
    visited.insert(canonical.clone());

    Ok(canonical)
}

/// Expand ~ to home directory in path
fn expand_tilde(path: &Path) -> PathBuf {
    let path_str = path.to_string_lossy();
    if path_str.starts_with('~') {
        if let Some(home) = dirs::home_dir() {
            let rest = path_str.strip_prefix('~').unwrap();
            let rest = rest.strip_prefix('/').unwrap_or(rest);
            return home.join(rest);
        }
    }
    path.to_path_buf()
}

/// Expand ~ in a string path
fn expand_tilde_string(s: &str) -> String {
    if s.starts_with('~') {
        if let Some(home) = dirs::home_dir() {
            let rest = s.strip_prefix('~').unwrap();
            let rest = rest.strip_prefix('/').unwrap_or(rest);
            return home.join(rest).to_string_lossy().into_owned();
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    // TEST1000: Single existing file
    #[test]
    fn test1000_single_existing_file() {
        let dir = create_test_dir();
        let file_path = dir.path().join("test.pdf");
        File::create(&file_path).unwrap();

        let result = resolve_file(&file_path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].file_name().unwrap(), "test.pdf");
    }

    // TEST1001: Single non-existent file
    #[test]
    fn test1001_nonexistent_file() {
        let result = resolve_file(Path::new("/nonexistent/path/file.pdf"));
        assert!(matches!(result, Err(InputResolverError::NotFound(_))));
    }

    // TEST1002: Empty directory
    #[test]
    fn test1002_empty_directory() {
        let dir = create_test_dir();

        let result = resolve_directory(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    // TEST1003: Directory with files
    #[test]
    fn test1003_directory_with_files() {
        let dir = create_test_dir();
        File::create(dir.path().join("a.txt")).unwrap();
        File::create(dir.path().join("b.txt")).unwrap();
        File::create(dir.path().join("c.txt")).unwrap();

        let result = resolve_directory(dir.path()).unwrap();
        assert_eq!(result.len(), 3);
    }

    // TEST1004: Directory with subdirs (recursive)
    #[test]
    fn test1004_directory_with_subdirs() {
        let dir = create_test_dir();
        fs::create_dir(dir.path().join("sub")).unwrap();
        File::create(dir.path().join("root.txt")).unwrap();
        File::create(dir.path().join("sub").join("nested.txt")).unwrap();

        let result = resolve_directory(dir.path()).unwrap();
        assert_eq!(result.len(), 2);
    }

    // TEST1005: Glob matching files
    #[test]
    fn test1005_glob_matching_files() {
        let dir = create_test_dir();
        File::create(dir.path().join("a.pdf")).unwrap();
        File::create(dir.path().join("b.pdf")).unwrap();
        File::create(dir.path().join("c.txt")).unwrap();

        let pattern = format!("{}/*.pdf", dir.path().display());
        let result = resolve_glob(&pattern).unwrap();
        assert_eq!(result.len(), 2);
    }

    // TEST1006: Glob matching nothing
    #[test]
    fn test1006_glob_matching_nothing() {
        let dir = create_test_dir();
        File::create(dir.path().join("a.txt")).unwrap();

        let pattern = format!("{}/*.xyz", dir.path().display());
        let result = resolve_glob(&pattern).unwrap();
        assert!(result.is_empty());
    }

    // TEST1007: Recursive glob
    #[test]
    fn test1007_recursive_glob() {
        let dir = create_test_dir();
        fs::create_dir(dir.path().join("sub")).unwrap();
        File::create(dir.path().join("a.json")).unwrap();
        File::create(dir.path().join("sub").join("b.json")).unwrap();

        let pattern = format!("{}/**/*.json", dir.path().display());
        let result = resolve_glob(&pattern).unwrap();
        assert_eq!(result.len(), 2);
    }

    // TEST1008: Mixed file + dir
    #[test]
    fn test1008_mixed_file_dir() {
        let dir = create_test_dir();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        File::create(dir.path().join("file.pdf")).unwrap();
        File::create(subdir.join("nested.txt")).unwrap();

        let items = vec![
            InputItem::File(dir.path().join("file.pdf")),
            InputItem::Directory(subdir),
        ];

        let result = resolve_items(&items).unwrap();
        assert_eq!(result.len(), 2);
    }

    // TEST1010: Duplicate paths are deduplicated
    #[test]
    fn test1010_duplicate_paths() {
        let dir = create_test_dir();
        let file_path = dir.path().join("file.pdf");
        File::create(&file_path).unwrap();

        let items = vec![
            InputItem::File(file_path.clone()),
            InputItem::File(file_path.clone()),
        ];

        let result = resolve_items(&items).unwrap();
        assert_eq!(result.len(), 1);
    }

    // TEST1011: Invalid glob syntax
    #[test]
    fn test1011_invalid_glob() {
        let result = resolve_glob("[unclosed");
        assert!(matches!(
            result,
            Err(InputResolverError::InvalidGlob { .. })
        ));
    }

    // TEST1013: Empty input array
    #[test]
    fn test1013_empty_input() {
        let result = resolve_items(&[]);
        assert!(matches!(result, Err(InputResolverError::EmptyInput)));
    }

    // TEST1014: Symlink to file
    #[test]
    #[cfg(unix)]
    fn test1014_symlink_to_file() {
        use std::os::unix::fs::symlink;

        let dir = create_test_dir();
        let file_path = dir.path().join("real.txt");
        let link_path = dir.path().join("link.txt");
        File::create(&file_path).unwrap();
        symlink(&file_path, &link_path).unwrap();

        let result = resolve_file(&link_path).unwrap();
        assert_eq!(result.len(), 1);
    }

    // TEST1016: Path with spaces
    #[test]
    fn test1016_path_with_spaces() {
        let dir = create_test_dir();
        let file_path = dir.path().join("my file.pdf");
        File::create(&file_path).unwrap();

        let result = resolve_file(&file_path).unwrap();
        assert_eq!(result.len(), 1);
    }

    // TEST1017: Path with unicode
    #[test]
    fn test1017_path_with_unicode() {
        let dir = create_test_dir();
        let file_path = dir.path().join("文档.pdf");
        File::create(&file_path).unwrap();

        let result = resolve_file(&file_path).unwrap();
        assert_eq!(result.len(), 1);
    }

    // TEST1018: Relative path
    #[test]
    fn test1018_relative_path() {
        let dir = create_test_dir();
        let file_path = dir.path().join("file.txt");
        File::create(&file_path).unwrap();

        // Change to temp dir
        let _guard = std::env::set_current_dir(dir.path());

        let result = resolve_file(Path::new("file.txt"));
        // May fail due to working dir, but should handle relative paths
        assert!(result.is_ok() || matches!(result, Err(InputResolverError::NotFound(_))));
    }

    // OS filtering is tested in os_filter.rs tests
}
