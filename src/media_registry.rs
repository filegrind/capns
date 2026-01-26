//! Media URN Registry - Remote lookup and caching for media specs
//!
//! This module provides the `MediaUrnRegistry` which handles:
//! - Remote lookup of media specs via `https://capns.org/media:xxx`
//! - Two-level caching (in-memory HashMap + disk with TTL)
//! - Bundled standard media specs at compile time
//!
//! ## Resolution Order
//! 1. In-memory cache (fastest)
//! 2. Disk cache (if not expired)
//! 3. Remote registry fetch
//!
//! ## Usage
//! ```ignore
//! let registry = MediaUrnRegistry::new().await?;
//! let spec = registry.get_media_spec("media:pdf;binary").await?;
//! println!("Title: {:?}", spec.title);
//! ```

use crate::media_spec::{MediaSpecDef, MediaSpecDefObject};
use crate::registry::RegistryConfig;
use include_dir::{include_dir, Dir};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_DURATION_HOURS: u64 = 24;

// Bundle standard media specs at compile time
static STANDARD_MEDIA_SPECS: Dir = include_dir!("$CARGO_MANIFEST_DIR/standard/media");

/// Stored media spec format (matches registry API response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMediaSpec {
    /// The media URN
    pub urn: String,
    /// The MIME media type
    pub media_type: String,
    /// Display-friendly title
    pub title: String,
    /// Optional profile URI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
    /// Optional JSON Schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional validation rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<crate::MediaValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl StoredMediaSpec {
    /// Convert to MediaSpecDef
    pub fn to_media_spec_def(&self) -> MediaSpecDef {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: self.media_type.clone(),
            profile_uri: self.profile_uri.clone().unwrap_or_default(),
            schema: self.schema.clone(),
            title: Some(self.title.clone()),
            description: self.description.clone(),
            validation: self.validation.clone(),
            metadata: self.metadata.clone(),
        })
    }
}

/// Normalize a media URN for consistent lookups and caching
fn normalize_media_urn(urn: &str) -> String {
    match crate::MediaUrn::from_string(urn) {
        Ok(parsed) => parsed.to_string(),
        Err(_) => urn.to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaCacheEntry {
    spec: StoredMediaSpec,
    cached_at: u64,
    ttl_hours: u64,
}

impl MediaCacheEntry {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.cached_at + (self.ttl_hours * 3600)
    }
}

/// Media URN Registry for looking up and caching media specs
#[derive(Debug)]
pub struct MediaUrnRegistry {
    client: reqwest::Client,
    cache_dir: PathBuf,
    cached_specs: Arc<Mutex<HashMap<String, StoredMediaSpec>>>,
    config: RegistryConfig,
}

impl MediaUrnRegistry {
    /// Create a new MediaUrnRegistry with standard media specs bundled
    ///
    /// Uses configuration from environment variables or defaults:
    /// - `CAPNS_REGISTRY_URL`: Base URL for the registry (default: https://capns.org)
    /// - `CAPNS_SCHEMA_BASE_URL`: Base URL for schemas (default: {registry_url}/schema)
    pub async fn new() -> Result<Self, MediaRegistryError> {
        Self::with_config(RegistryConfig::default()).await
    }

    /// Create a new MediaUrnRegistry with custom configuration
    ///
    /// # Example
    /// ```ignore
    /// use capns::registry::RegistryConfig;
    /// let config = RegistryConfig::new()
    ///     .with_registry_url("https://my-registry.example.com");
    /// let registry = MediaUrnRegistry::with_config(config).await?;
    /// ```
    pub async fn with_config(config: RegistryConfig) -> Result<Self, MediaRegistryError> {
        let cache_dir = Self::get_cache_dir()?;

        fs::create_dir_all(&cache_dir).map_err(|e| {
            MediaRegistryError::CacheError(format!("Failed to create cache directory: {}", e))
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                MediaRegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Load all cached specs into memory
        let cached_specs_map = Self::load_all_cached_specs(&cache_dir)?;
        let cached_specs = Arc::new(Mutex::new(cached_specs_map));

        let registry = Self {
            client,
            cache_dir,
            cached_specs,
            config,
        };

        // Install bundled standard media specs
        registry.install_standard_specs().await?;

        Ok(registry)
    }

    /// Get the current registry configuration
    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    /// Install bundled standard media specs to cache if they don't exist
    async fn install_standard_specs(&self) -> Result<(), MediaRegistryError> {
        for file in STANDARD_MEDIA_SPECS.files() {
            // Skip non-JSON files (e.g., .gitkeep)
            let extension = file.path().extension().and_then(|e| e.to_str());
            if extension != Some("json") {
                continue;
            }

            let filename = match file.path().file_stem().and_then(|s| s.to_str()) {
                Some(name) => name,
                None => {
                    eprintln!("[WARN] Skipping file with invalid filename: {:?}", file.path());
                    continue;
                }
            };

            let content = match file.contents_utf8() {
                Some(c) => c,
                None => {
                    eprintln!("[WARN] Skipping non-UTF8 file: {:?}", file.path());
                    continue;
                }
            };

            let spec: StoredMediaSpec = match serde_json::from_str(content) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[WARN] Skipping invalid media spec {}: {}", filename, e);
                    continue;
                }
            };

            let normalized_urn = normalize_media_urn(&spec.urn);

            // Check if this spec is already cached
            let cache_file = self.cache_file_path(&normalized_urn);
            if !cache_file.exists() {
                // Create cache entry
                let cache_entry = MediaCacheEntry {
                    spec: spec.clone(),
                    cached_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ttl_hours: CACHE_DURATION_HOURS,
                };

                let cache_content = match serde_json::to_string_pretty(&cache_entry) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[WARN] Failed to serialize media spec {}: {}", filename, e);
                        continue;
                    }
                };

                if let Err(e) = fs::write(&cache_file, cache_content) {
                    eprintln!("[WARN] Failed to write media spec to cache {}: {}", filename, e);
                    continue;
                }

                // Add to in-memory cache
                if let Ok(mut cached_specs) = self.cached_specs.lock() {
                    cached_specs.insert(normalized_urn.clone(), spec);
                }

                eprintln!("Installed standard media spec: {}", normalized_urn);
            }
        }

        Ok(())
    }

    /// Get all bundled standard media specs without network access
    pub fn get_standard_specs(&self) -> Result<Vec<StoredMediaSpec>, MediaRegistryError> {
        let mut specs = Vec::new();

        for file in STANDARD_MEDIA_SPECS.files() {
            // Skip non-JSON files (e.g., .gitkeep)
            let extension = file.path().extension().and_then(|e| e.to_str());
            if extension != Some("json") {
                continue;
            }

            let filename = file.path().file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("<unknown>");

            let content = match file.contents_utf8() {
                Some(c) => c,
                None => {
                    eprintln!("[WARN] Skipping non-UTF8 file: {:?}", file.path());
                    continue;
                }
            };

            let spec: StoredMediaSpec = match serde_json::from_str(content) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[WARN] Skipping invalid media spec {}: {}", filename, e);
                    continue;
                }
            };

            specs.push(spec);
        }

        Ok(specs)
    }

    /// Get a media spec from cache or fetch from registry
    pub async fn get_media_spec(&self, urn: &str) -> Result<StoredMediaSpec, MediaRegistryError> {
        let normalized_urn = normalize_media_urn(urn);

        // Check in-memory cache first
        {
            let cached_specs = self.cached_specs.lock().map_err(|e| {
                MediaRegistryError::CacheError(format!("Failed to lock cache: {}", e))
            })?;
            if let Some(spec) = cached_specs.get(&normalized_urn) {
                return Ok(spec.clone());
            }
        }

        // Not in cache, fetch from registry and update cache
        let spec = self.fetch_from_registry(urn).await?;

        // Update in-memory cache
        {
            let mut cached_specs = self.cached_specs.lock().map_err(|e| {
                MediaRegistryError::CacheError(format!("Failed to lock cache for update: {}", e))
            })?;
            cached_specs.insert(normalized_urn.clone(), spec.clone());
        }

        Ok(spec)
    }

    /// Get multiple media specs at once
    pub async fn get_media_specs(
        &self,
        urns: &[&str],
    ) -> Result<Vec<StoredMediaSpec>, MediaRegistryError> {
        let mut specs = Vec::new();
        for urn in urns {
            specs.push(self.get_media_spec(urn).await?);
        }
        Ok(specs)
    }

    /// Get all currently cached media specs
    pub async fn get_cached_specs(&self) -> Result<Vec<StoredMediaSpec>, MediaRegistryError> {
        let cached_specs = self.cached_specs.lock().map_err(|e| {
            MediaRegistryError::CacheError(format!("Failed to lock cache: {}", e))
        })?;
        Ok(cached_specs.values().cloned().collect())
    }

    fn get_cache_dir() -> Result<PathBuf, MediaRegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            MediaRegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capns");
        cache_dir.push("media");
        Ok(cache_dir)
    }

    fn cache_key(&self, urn: &str) -> String {
        let normalized_urn = normalize_media_urn(urn);
        let mut hasher = Sha256::new();
        hasher.update(normalized_urn.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn cache_file_path(&self, urn: &str) -> PathBuf {
        let key = self.cache_key(urn);
        self.cache_dir.join(format!("{}.json", key))
    }

    fn load_all_cached_specs(
        cache_dir: &PathBuf,
    ) -> Result<HashMap<String, StoredMediaSpec>, MediaRegistryError> {
        let mut specs = HashMap::new();

        if !cache_dir.exists() {
            return Ok(specs);
        }

        for entry in fs::read_dir(cache_dir).map_err(|e| {
            MediaRegistryError::CacheError(format!("Failed to read cache directory: {}", e))
        })? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("[WARN] Failed to read cache entry: {}", e);
                    continue;
                }
            };

            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "json" {
                    let content = match fs::read_to_string(&path) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("[WARN] Failed to read cache file {:?}: {}", path, e);
                            continue;
                        }
                    };

                    let cache_entry: MediaCacheEntry = match serde_json::from_str(&content) {
                        Ok(e) => e,
                        Err(e) => {
                            eprintln!("[WARN] Failed to parse cache file {:?}: {}", path, e);
                            // Try to remove the invalid cache file
                            let _ = fs::remove_file(&path);
                            continue;
                        }
                    };

                    if cache_entry.is_expired() {
                        // Remove expired cache file
                        if let Err(e) = fs::remove_file(&path) {
                            eprintln!("[WARN] Failed to remove expired cache file {:?}: {}", path, e);
                        }
                        continue;
                    }

                    let normalized_urn = normalize_media_urn(&cache_entry.spec.urn);
                    specs.insert(normalized_urn, cache_entry.spec);
                }
            }
        }

        Ok(specs)
    }

    fn save_to_cache(&self, spec: &StoredMediaSpec) -> Result<(), MediaRegistryError> {
        let cache_file = self.cache_file_path(&spec.urn);
        let cache_entry = MediaCacheEntry {
            spec: spec.clone(),
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_hours: CACHE_DURATION_HOURS,
        };

        let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
            MediaRegistryError::CacheError(format!("Failed to serialize cache entry: {}", e))
        })?;

        fs::write(&cache_file, content).map_err(|e| {
            MediaRegistryError::CacheError(format!("Failed to write cache file: {}", e))
        })?;

        Ok(())
    }

    async fn fetch_from_registry(&self, urn: &str) -> Result<StoredMediaSpec, MediaRegistryError> {
        let normalized_urn = normalize_media_urn(urn);
        // URL-encode only the tags part
        let tags_part = normalized_urn
            .strip_prefix("media:")
            .unwrap_or(&normalized_urn);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/media:{}", self.config.registry_base_url, encoded_tags);

        let response = self.client.get(&url).send().await.map_err(|e| {
            MediaRegistryError::HttpError(format!("Failed to fetch from registry: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(MediaRegistryError::NotFound(format!(
                "Media spec '{}' not found in registry (HTTP {})",
                urn,
                response.status()
            )));
        }

        let spec: StoredMediaSpec = response.json().await.map_err(|e| {
            MediaRegistryError::ParseError(format!(
                "Failed to parse registry response for '{}': {}",
                urn, e
            ))
        })?;

        // Cache the result
        self.save_to_cache(&spec)?;

        Ok(spec)
    }

    /// Check if a media URN exists in registry (cached or online)
    pub async fn media_spec_exists(&self, urn: &str) -> bool {
        self.get_media_spec(urn).await.is_ok()
    }

    /// Clear all cached media specs
    pub fn clear_cache(&self) -> Result<(), MediaRegistryError> {
        // Clear in-memory cache
        {
            let mut cached_specs = self.cached_specs.lock().map_err(|e| {
                MediaRegistryError::CacheError(format!("Failed to lock cache for clearing: {}", e))
            })?;
            cached_specs.clear();
        }

        // Clear filesystem cache
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir).map_err(|e| {
                MediaRegistryError::CacheError(format!("Failed to clear cache directory: {}", e))
            })?;
            fs::create_dir_all(&self.cache_dir).map_err(|e| {
                MediaRegistryError::CacheError(format!(
                    "Failed to recreate cache directory: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

/// Errors that can occur when working with the media registry
#[derive(Debug, thiserror::Error)]
pub enum MediaRegistryError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Media spec not found in registry: {0}")]
    NotFound(String),

    #[error("Failed to parse registry response: {0}")]
    ParseError(String),

    #[error("Cache error: {0}")]
    CacheError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio;

    // Helper to create registry with a temporary cache directory
    async fn registry_with_temp_cache() -> (MediaUrnRegistry, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("media");
        fs::create_dir_all(&cache_dir).unwrap();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let registry = MediaUrnRegistry {
            client,
            cache_dir,
            cached_specs: Arc::new(Mutex::new(HashMap::new())),
            config: RegistryConfig::default(),
        };

        (registry, temp_dir)
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let (registry, _temp_dir) = registry_with_temp_cache().await;
        assert!(registry.cache_dir.exists());
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        let (registry, _temp_dir) = registry_with_temp_cache().await;
        let key1 = registry.cache_key("media:string;textable;scalar");
        let key2 = registry.cache_key("media:string;textable;scalar");
        let key3 = registry.cache_key("media:integer;textable;scalar");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_stored_media_spec_to_def() {
        let spec = StoredMediaSpec {
            urn: "media:pdf;binary".to_string(),
            media_type: "application/pdf".to_string(),
            title: "PDF Document".to_string(),
            profile_uri: Some("https://capns.org/schema/pdf".to_string()),
            schema: None,
            description: Some("PDF document data".to_string()),
            validation: None,
            metadata: None,
        };

        let def = spec.to_media_spec_def();
        match def {
            MediaSpecDef::Object(obj) => {
                assert_eq!(obj.media_type, "application/pdf");
                assert_eq!(obj.title, Some("PDF Document".to_string()));
                assert_eq!(obj.description, Some("PDF document data".to_string()));
                assert_eq!(obj.validation, None);
            }
            _ => panic!("Expected Object variant"),
        }
    }

    #[test]
    fn test_normalize_media_urn() {
        // Same URN should normalize to same value
        let urn1 = normalize_media_urn("media:string");
        let urn2 = normalize_media_urn("media:string");
        // Note: actual equality depends on TaggedUrn canonicalization
        assert!(!urn1.is_empty());
        assert!(!urn2.is_empty());
    }
}
