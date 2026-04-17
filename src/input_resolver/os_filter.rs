//! OS File Filter — excludes OS artifacts from input resolution
//!
//! This module filters out operating system artifacts that are not user content:
//! - macOS: .DS_Store, ._*, .Spotlight-V100, .Trashes, etc.
//! - Windows: Thumbs.db, desktop.ini, ehthumbs.db, etc.
//! - Common: .git, .svn, .hg, temp files, etc.

use std::path::Path;

/// Files that are always excluded (exact match)
const EXCLUDED_FILES: &[&str] = &[
    // macOS
    ".DS_Store",
    ".localized",
    ".AppleDouble",
    ".LSOverride",
    ".DocumentRevisions-V100",
    ".fseventsd",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".Trashes",
    ".VolumeIcon.icns",
    ".com.apple.timemachine.donotpresent",
    ".AppleDB",
    ".AppleDesktop",
    "Network Trash Folder",
    "Temporary Items",
    ".apdisk",
    // Windows
    "Thumbs.db",
    "Thumbs.db:encryptable",
    "ehthumbs.db",
    "ehthumbs_vista.db",
    "desktop.ini",
    // Linux
    ".directory",
    // Editor/IDE
    ".project",
    ".settings",
    ".classpath",
];

/// Directory names that are always excluded (entire subtree)
const EXCLUDED_DIRS: &[&str] = &[
    // Version control
    ".git",
    ".svn",
    ".hg",
    ".bzr",
    "_darcs",
    ".fossil",
    // macOS
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    ".TemporaryItems",
    "__MACOSX",
    ".DocumentRevisions-V100",
    // IDE/Editor
    ".idea",
    ".vscode",
    ".vs",
    "__pycache__",
    "node_modules",
    ".tox",
    ".nox",
    ".eggs",
    "*.egg-info",
    ".mypy_cache",
    ".pytest_cache",
    ".hypothesis",
    // Build artifacts (optional - may want to keep these)
    // "target",
    // "build",
    // "dist",
];

/// File extensions that indicate temp/backup files
const EXCLUDED_EXTENSIONS: &[&str] = &["tmp", "temp", "swp", "swo", "swn", "bak", "backup", "orig"];

/// Check if a path should be excluded from input resolution
///
/// Returns `true` if the path is an OS artifact and should be skipped.
pub fn should_exclude(path: &Path) -> bool {
    // Get filename
    let filename = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return false, // Can't determine, include it
    };

    // Check exact filename matches
    if EXCLUDED_FILES.contains(&filename) {
        return true;
    }

    // Check for macOS resource fork files (._*)
    if filename.starts_with("._") {
        return true;
    }

    // Check for Office lock files (~$*)
    if filename.starts_with("~$") {
        return true;
    }

    // Check for macOS Icon file (Icon\r)
    if filename == "Icon\r" || filename == "Icon\x0d" {
        return true;
    }

    // Check for temp/backup extensions
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext_lower = ext.to_lowercase();
        if EXCLUDED_EXTENSIONS.contains(&ext_lower.as_str()) {
            return true;
        }
    }

    // Check for hidden files on Unix (but not .gitignore, etc. which are config)
    // We specifically exclude known OS hidden files, not all hidden files
    // because .env, .gitignore, etc. are valid user content

    false
}

/// Check if a directory should be excluded (won't descend into it)
///
/// Returns `true` if the directory is an OS artifact or should be skipped.
pub fn should_exclude_dir(path: &Path) -> bool {
    let dirname = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return false,
    };

    // Check exact directory name matches
    if EXCLUDED_DIRS.contains(&dirname) {
        return true;
    }

    // Check for macOS bundle internals we want to skip
    // (but allow .app, .framework as they might be intentional)

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // TEST1020: macOS .DS_Store is excluded
    #[test]
    fn test1020_ds_store_excluded() {
        assert!(should_exclude(Path::new("/some/path/.DS_Store")));
        assert!(should_exclude(Path::new(".DS_Store")));
    }

    // TEST1021: Windows Thumbs.db is excluded
    #[test]
    fn test1021_thumbs_db_excluded() {
        assert!(should_exclude(Path::new("/some/path/Thumbs.db")));
        assert!(should_exclude(Path::new("Thumbs.db")));
    }

    // TEST1022: macOS resource fork files are excluded
    #[test]
    fn test1022_resource_fork_excluded() {
        assert!(should_exclude(Path::new("/path/._file.txt")));
        assert!(should_exclude(Path::new("._anything")));
    }

    // TEST1023: Office lock files are excluded
    #[test]
    fn test1023_office_lock_excluded() {
        assert!(should_exclude(Path::new("/path/~$document.docx")));
        assert!(should_exclude(Path::new("~$spreadsheet.xlsx")));
    }

    // TEST1024: .git directory is excluded
    #[test]
    fn test1024_git_dir_excluded() {
        assert!(should_exclude_dir(Path::new("/repo/.git")));
        assert!(should_exclude_dir(Path::new(".git")));
    }

    // TEST1025: __MACOSX archive artifact is excluded
    #[test]
    fn test1025_macosx_dir_excluded() {
        assert!(should_exclude_dir(Path::new("/extracted/__MACOSX")));
        assert!(should_exclude_dir(Path::new("__MACOSX")));
    }

    // TEST1026: Temp files are excluded
    #[test]
    fn test1026_temp_files_excluded() {
        assert!(should_exclude(Path::new("/path/file.tmp")));
        assert!(should_exclude(Path::new("/path/file.temp")));
        assert!(should_exclude(Path::new("/path/file.swp")));
        assert!(should_exclude(Path::new("/path/file.bak")));
    }

    // TEST1027: .localized is excluded
    #[test]
    fn test1027_localized_excluded() {
        assert!(should_exclude(Path::new("/path/.localized")));
    }

    // TEST1028: desktop.ini is excluded
    #[test]
    fn test1028_desktop_ini_excluded() {
        assert!(should_exclude(Path::new("/path/desktop.ini")));
    }

    // TEST1029: Normal files are NOT excluded
    #[test]
    fn test1029_normal_files_not_excluded() {
        // These should NOT be excluded
        assert!(!should_exclude(Path::new("/path/file.txt")));
        assert!(!should_exclude(Path::new("/path/data.json")));
        assert!(!should_exclude(Path::new("/path/notes.md")));
        assert!(!should_exclude(Path::new("/path/.gitignore"))); // Config file, keep
        assert!(!should_exclude(Path::new("/path/.env"))); // Config file, keep
        assert!(!should_exclude(Path::new("/path/README.md")));
    }
}
