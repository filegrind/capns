//! CartridgeJson — install-context metadata for installed cartridges.
//!
//! Every installed cartridge version directory contains a `cartridge.json` file
//! that records how the cartridge was installed and where its entry point is.
//! This is analogous to `provenance.json` for run artifacts.
//!
//! Layout:
//! ```text
//! cartridges/{name}/{version}/
//!   cartridge.json       ← this file
//!   <entry_point_binary>
//!   <supporting_files>
//! ```

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// How a cartridge was installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CartridgeInstallSource {
    Registry,
    Dev,
    Bundle,
}

/// Install-context metadata stored in `cartridge.json` inside each cartridge
/// version directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeJson {
    /// Cartridge name (e.g., "pdfcartridge").
    pub name: String,
    /// Version string (e.g., "0.168.411").
    pub version: String,
    /// Relative path from the version directory to the executable entry point.
    /// For single-binary cartridges this is just the binary filename.
    /// For directory cartridges it may be a nested path.
    pub entry: String,
    /// RFC3339 timestamp of when the cartridge was installed.
    pub installed_at: String,
    /// How the cartridge was installed.
    pub installed_from: CartridgeInstallSource,
    /// URL the package was downloaded from (empty for dev/bundle installs).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_url: String,
    /// SHA256 hash of the original package (tarball or binary).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub package_sha256: String,
    /// Size in bytes of the original package.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub package_size: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Errors when reading or validating a `cartridge.json`.
#[derive(Debug, thiserror::Error)]
pub enum CartridgeJsonError {
    #[error("cartridge.json not found at {0}")]
    NotFound(PathBuf),
    #[error("failed to read cartridge.json at {path}: {source}")]
    ReadFailed {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid cartridge.json at {path}: {source}")]
    InvalidJson {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("cartridge.json at {path}: entry point '{entry}' does not exist")]
    EntryPointMissing { path: PathBuf, entry: String },
    #[error("cartridge.json at {path}: entry point '{entry}' is not executable")]
    EntryPointNotExecutable { path: PathBuf, entry: String },
    #[error("cartridge.json at {path}: entry path '{entry}' escapes version directory")]
    EntryPathEscape { path: PathBuf, entry: String },
    #[error("failed to write cartridge.json at {path}: {source}")]
    WriteFailed {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl CartridgeJson {
    /// Read and validate a `cartridge.json` from a version directory.
    ///
    /// Validates:
    /// - File exists and is valid JSON
    /// - Entry point path does not escape the version directory
    /// - Entry point binary exists and is executable
    pub fn read_from_dir(version_dir: &Path) -> Result<Self, CartridgeJsonError> {
        let json_path = version_dir.join("cartridge.json");

        if !json_path.exists() {
            return Err(CartridgeJsonError::NotFound(json_path));
        }

        let contents = std::fs::read_to_string(&json_path).map_err(|e| {
            CartridgeJsonError::ReadFailed {
                path: json_path.clone(),
                source: e,
            }
        })?;

        let cartridge_json: CartridgeJson =
            serde_json::from_str(&contents).map_err(|e| CartridgeJsonError::InvalidJson {
                path: json_path.clone(),
                source: e,
            })?;

        // Validate entry point exists
        let entry_path = version_dir.join(&cartridge_json.entry);
        if !entry_path.exists() {
            return Err(CartridgeJsonError::EntryPointMissing {
                path: json_path,
                entry: cartridge_json.entry,
            });
        }

        // Validate entry path does not escape version directory
        let canonical_dir = version_dir
            .canonicalize()
            .unwrap_or_else(|_| version_dir.to_path_buf());
        let canonical_entry = entry_path
            .canonicalize()
            .unwrap_or_else(|_| entry_path.clone());

        if !canonical_entry.starts_with(&canonical_dir) {
            return Err(CartridgeJsonError::EntryPathEscape {
                path: json_path,
                entry: cartridge_json.entry,
            });
        }

        // Validate entry point is executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(&entry_path).map_err(|e| {
                CartridgeJsonError::ReadFailed {
                    path: json_path.clone(),
                    source: e,
                }
            })?;
            if meta.permissions().mode() & 0o111 == 0 {
                return Err(CartridgeJsonError::EntryPointNotExecutable {
                    path: json_path,
                    entry: cartridge_json.entry,
                });
            }
        }

        Ok(cartridge_json)
    }

    /// Resolve the absolute path to the entry point binary.
    pub fn resolve_entry_point(&self, version_dir: &Path) -> PathBuf {
        version_dir.join(&self.entry)
    }

    /// Write this `cartridge.json` to a version directory.
    pub fn write_to_dir(&self, version_dir: &Path) -> Result<(), CartridgeJsonError> {
        let json_path = version_dir.join("cartridge.json");
        let contents = serde_json::to_string_pretty(self).expect("CartridgeJson serialization cannot fail");
        std::fs::write(&json_path, contents.as_bytes()).map_err(|e| {
            CartridgeJsonError::WriteFailed {
                path: json_path,
                source: e,
            }
        })
    }
}

/// Compute a deterministic SHA256 hash of a directory tree.
///
/// Walks all files in the directory recursively, sorts them by relative path,
/// then hashes each file's relative path (UTF-8 bytes) followed by its contents.
/// This produces a stable identity hash regardless of filesystem ordering.
///
/// Symbolic links are followed (their targets are hashed, not the links).
/// `cartridge.json` itself is excluded from the hash — it contains install-time
/// metadata (like `installed_at`) that changes between installs of the same content.
pub fn hash_cartridge_directory(dir: &Path) -> Result<String, std::io::Error> {
    use sha2::{Digest, Sha256};

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    collect_files(dir, dir, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    for (relative_path, full_path) in &files {
        hasher.update(relative_path.as_bytes());
        let contents = std::fs::read(full_path)?;
        hasher.update(&contents);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Recursively collect all files in a directory with their relative paths.
fn collect_files(
    base: &Path,
    current: &Path,
    out: &mut Vec<(String, PathBuf)>,
) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            collect_files(base, &path, out)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            let relative = path
                .strip_prefix(base)
                .expect("BUG: path must be under base");
            let relative_str = relative.to_string_lossy().to_string();

            // Exclude cartridge.json from identity hash — it contains
            // install-time metadata that varies between installs of identical content.
            if relative_str == "cartridge.json" {
                continue;
            }

            out.push((relative_str, path));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn test_roundtrip_serialize_deserialize() {
        let cj = CartridgeJson {
            name: "pdfcartridge".to_string(),
            version: "0.168.411".to_string(),
            entry: "pdfcartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: CartridgeInstallSource::Registry,
            source_url: "https://machinefabric.com/api/cartridges/packages/pdfcartridge-0.168.411.pkg".to_string(),
            package_sha256: "abc123".to_string(),
            package_size: 12345,
        };

        let json = serde_json::to_string_pretty(&cj).unwrap();
        let parsed: CartridgeJson = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "pdfcartridge");
        assert_eq!(parsed.version, "0.168.411");
        assert_eq!(parsed.entry, "pdfcartridge");
        assert_eq!(parsed.installed_from, CartridgeInstallSource::Registry);
    }

    #[test]
    fn test_dev_install_omits_optional_fields() {
        let cj = CartridgeJson {
            name: "testcartridge".to_string(),
            version: "0.1.0".to_string(),
            entry: "testcartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: CartridgeInstallSource::Dev,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
        };

        let json = serde_json::to_string(&cj).unwrap();
        assert!(!json.contains("source_url"));
        assert!(!json.contains("package_sha256"));
        assert!(!json.contains("package_size"));
    }

    #[test]
    fn test_read_from_dir_validates_entry_exists() {
        let dir = tempfile::tempdir().unwrap();
        let cj = CartridgeJson {
            name: "test".to_string(),
            version: "1.0".to_string(),
            entry: "nonexistent_binary".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: CartridgeInstallSource::Dev,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
        };
        let json = serde_json::to_string_pretty(&cj).unwrap();
        std::fs::write(dir.path().join("cartridge.json"), &json).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, CartridgeJsonError::EntryPointMissing { .. }));
    }

    #[test]
    fn test_read_from_dir_rejects_path_escape() {
        let dir = tempfile::tempdir().unwrap();

        // Create a binary outside the version dir
        let outside = dir.path().parent().unwrap().join("escaped_binary");
        std::fs::write(&outside, b"#!/bin/sh").unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "test".to_string(),
            version: "1.0".to_string(),
            entry: "../escaped_binary".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: CartridgeInstallSource::Dev,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
        };
        let json = serde_json::to_string_pretty(&cj).unwrap();
        std::fs::write(dir.path().join("cartridge.json"), &json).unwrap();

        let err = CartridgeJson::read_from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, CartridgeJsonError::EntryPathEscape { .. }));

        // Cleanup
        let _ = std::fs::remove_file(&outside);
    }

    #[test]
    fn test_read_from_dir_succeeds_with_valid_cartridge() {
        let dir = tempfile::tempdir().unwrap();
        let binary_path = dir.path().join("mycartridge");
        std::fs::write(&binary_path, b"#!/bin/sh\necho hello").unwrap();
        std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cj = CartridgeJson {
            name: "mycartridge".to_string(),
            version: "1.0.0".to_string(),
            entry: "mycartridge".to_string(),
            installed_at: "2026-04-12T10:00:00Z".to_string(),
            installed_from: CartridgeInstallSource::Bundle,
            source_url: String::new(),
            package_sha256: String::new(),
            package_size: 0,
        };
        cj.write_to_dir(dir.path()).unwrap();

        let loaded = CartridgeJson::read_from_dir(dir.path()).unwrap();
        assert_eq!(loaded.name, "mycartridge");
        assert_eq!(loaded.version, "1.0.0");
        assert_eq!(loaded.resolve_entry_point(dir.path()), binary_path);
    }

    #[test]
    fn test_hash_cartridge_directory_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("binary"), b"executable content").unwrap();
        std::fs::write(dir.path().join("data.bin"), b"some data").unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        std::fs::write(dir.path().join("subdir/nested.txt"), b"nested file").unwrap();

        // Write cartridge.json — should be excluded from hash
        std::fs::write(dir.path().join("cartridge.json"), b"{}").unwrap();

        let hash1 = hash_cartridge_directory(dir.path()).unwrap();
        let hash2 = hash_cartridge_directory(dir.path()).unwrap();
        assert_eq!(hash1, hash2);

        // Changing cartridge.json should NOT change the hash
        std::fs::write(dir.path().join("cartridge.json"), b"{\"different\": true}").unwrap();
        let hash3 = hash_cartridge_directory(dir.path()).unwrap();
        assert_eq!(hash1, hash3);

        // Changing actual content SHOULD change the hash
        std::fs::write(dir.path().join("binary"), b"different content").unwrap();
        let hash4 = hash_cartridge_directory(dir.path()).unwrap();
        assert_ne!(hash1, hash4);
    }

    #[test]
    fn test_hash_single_binary_matches_flat_layout() {
        // A directory with just one binary should hash consistently
        let dir = tempfile::tempdir().unwrap();
        let content = b"binary content here";
        std::fs::write(dir.path().join("pdfcartridge"), content).unwrap();

        let hash = hash_cartridge_directory(dir.path()).unwrap();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // SHA256 hex length
    }
}
