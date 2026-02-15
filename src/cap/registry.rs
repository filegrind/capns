use crate::Cap;
use include_dir::{include_dir, Dir};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_REGISTRY_BASE_URL: &str = "https://capns.org";
const CACHE_DURATION_HOURS: u64 = 24;

/// Configuration for the CAPNS registry
///
/// Supports configuration via:
/// 1. Builder methods (highest priority)
/// 2. Environment variables (CAPNS_REGISTRY_URL, CAPNS_SCHEMA_BASE_URL)
/// 3. Default values (https://capns.org)
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Base URL for the registry API (e.g., "https://capns.org")
    pub registry_base_url: String,
    /// Base URL for schema profiles (defaults to {registry_base_url}/schema)
    pub schema_base_url: String,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        let registry_base = env::var("CAPNS_REGISTRY_URL")
            .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string());
        let schema_base = env::var("CAPNS_SCHEMA_BASE_URL")
            .unwrap_or_else(|_| format!("{}/schema", registry_base));
        Self {
            registry_base_url: registry_base,
            schema_base_url: schema_base,
        }
    }
}

impl RegistryConfig {
    /// Create a new RegistryConfig with values from environment or defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a custom registry base URL
    ///
    /// This also updates the schema base URL to {url}/schema unless
    /// schema_base_url was explicitly set.
    pub fn with_registry_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        // If schema_base_url was derived from the old registry URL, update it
        if self.schema_base_url == format!("{}/schema", self.registry_base_url) {
            self.schema_base_url = format!("{}/schema", url);
        }
        self.registry_base_url = url;
        self
    }

    /// Set a custom schema base URL
    pub fn with_schema_url(mut self, url: impl Into<String>) -> Self {
        self.schema_base_url = url.into();
        self
    }
}

// Bundle standard capabilities at compile time
static STANDARD_CAPS: Dir = include_dir!("$CARGO_MANIFEST_DIR/standard");

/// Normalize a Cap URN for consistent lookups and caching
/// This ensures that URNs with different tag ordering or trailing semicolons
/// are treated as the same capability
fn normalize_cap_urn(urn: &str) -> String {
    // Use the proper CapUrn parser which handles quoted values correctly
    match crate::CapUrn::from_string(urn) {
        Ok(parsed) => parsed.to_string(),
        Err(_) => {
            // If parsing fails, return original URN (will likely fail later with better error)
            urn.to_string()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    definition: Cap,
    cached_at: u64,
    ttl_hours: u64,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.cached_at + (self.ttl_hours * 3600)
    }
}

#[derive(Debug)]
pub struct CapRegistry {
    client: reqwest::Client,
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    config: RegistryConfig,
}

impl CapRegistry {
    /// Create a new CapRegistry with standard capabilities bundled
    ///
    /// Uses configuration from environment variables or defaults:
    /// - `CAPNS_REGISTRY_URL`: Base URL for the registry (default: https://capns.org)
    /// - `CAPNS_SCHEMA_BASE_URL`: Base URL for schemas (default: {registry_url}/schema)
    pub async fn new() -> Result<Self, RegistryError> {
        Self::with_config(RegistryConfig::default()).await
    }

    /// Create a new CapRegistry with custom configuration
    ///
    /// # Example
    /// ```ignore
    /// let config = RegistryConfig::new()
    ///     .with_registry_url("https://my-registry.example.com");
    /// let registry = CapRegistry::with_config(config).await?;
    /// ```
    pub async fn with_config(config: RegistryConfig) -> Result<Self, RegistryError> {
        let cache_dir = Self::get_cache_dir()?;

        fs::create_dir_all(&cache_dir).map_err(|e| {
            RegistryError::CacheError(format!("Failed to create cache directory: {}", e))
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                RegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Load all cached caps into memory
        let cached_caps_map = Self::load_all_cached_caps(&cache_dir)?;
        let cached_caps = Arc::new(Mutex::new(cached_caps_map));

        let registry = Self {
            client,
            cache_dir,
            cached_caps,
            config,
        };

        // Copy bundled standard capabilities to cache if they don't exist
        registry.install_standard_caps().await?;

        Ok(registry)
    }

    /// Get the current registry configuration
    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }
    
    /// Install bundled standard capabilities to cache directory if they don't exist
    async fn install_standard_caps(&self) -> Result<(), RegistryError> {
        for file in STANDARD_CAPS.files() {
            // Skip non-JSON files (e.g., .gitkeep)
            let extension = file.path().extension().and_then(|e| e.to_str());
            if extension != Some("json") {
                continue;
            }

            // Get filename without extension for URN construction
            let filename = match file.path().file_stem().and_then(|s| s.to_str()) {
                Some(name) => name,
                None => {
                    eprintln!("[WARN] Skipping file with invalid filename: {:?}", file.path());
                    continue;
                }
            };

            // Parse the JSON content to get the Cap definition
            let content = match file.contents_utf8() {
                Some(c) => c,
                None => {
                    eprintln!("[WARN] Skipping non-UTF8 file: {:?}", file.path());
                    continue;
                }
            };

            let cap: Cap = match serde_json::from_str(content) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[WARN] Skipping invalid cap definition {}: {}", filename, e);
                    continue;
                }
            };

            // Get normalized URN from the cap definition
            let urn = cap.urn_string();
            let normalized_urn = normalize_cap_urn(&urn);

            // Check if this capability is already cached
            let cache_file = self.cache_file_path(&normalized_urn);
            if !cache_file.exists() {
                // Create cache entry with current timestamp
                let cache_entry = CacheEntry {
                    definition: cap.clone(),
                    cached_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ttl_hours: CACHE_DURATION_HOURS,
                };

                let cache_content = match serde_json::to_string_pretty(&cache_entry) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[WARN] Failed to serialize standard cap {}: {}", filename, e);
                        continue;
                    }
                };

                if let Err(e) = fs::write(&cache_file, cache_content) {
                    eprintln!("[WARN] Failed to write standard cap to cache {}: {}", filename, e);
                    continue;
                }

                // Also add to in-memory cache
                if let Ok(mut cached_caps) = self.cached_caps.lock() {
                    cached_caps.insert(normalized_urn.clone(), cap);
                }

                eprintln!("Standard: {}", normalized_urn);
            }
        }

        Ok(())
    }
    
    /// Get all bundled standard capabilities without network access
    pub fn get_standard_caps(&self) -> Result<Vec<Cap>, RegistryError> {
        let mut caps = Vec::new();

        for file in STANDARD_CAPS.files() {
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

            let cap: Cap = match serde_json::from_str(content) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[WARN] Skipping invalid cap definition {}: {}", filename, e);
                    continue;
                }
            };

            caps.push(cap);
        }

        Ok(caps)
    }

    /// Get a cap from in-memory cache or fetch from registry
    pub async fn get_cap(&self, urn: &str) -> Result<Cap, RegistryError> {
        let normalized_urn = normalize_cap_urn(urn);
        
        // Check in-memory cache first
        {
            let cached_caps = self.cached_caps.lock().map_err(|e| {
                RegistryError::CacheError(format!("Failed to lock cache: {}", e))
            })?;
            if let Some(cap) = cached_caps.get(&normalized_urn) {
                return Ok(cap.clone());
            }
        }
        
        // Not in cache, fetch from registry and update in-memory cache
        let cap = self.fetch_from_registry(urn).await?;
        
        // Update in-memory cache
        {
            let mut cached_caps = self.cached_caps.lock().map_err(|e| {
                RegistryError::CacheError(format!("Failed to lock cache for update: {}", e))
            })?;
            cached_caps.insert(normalized_urn.clone(), cap.clone());
        }
        
        Ok(cap)
    }

    /// Get multiple caps at once - fails if any cap is not available
    pub async fn get_caps(&self, urns: &[&str]) -> Result<Vec<Cap>, RegistryError> {
        let mut caps = Vec::new();
        for urn in urns {
            caps.push(self.get_cap(urn).await?);
        }
        Ok(caps)
    }

    /// Get all currently cached caps from in-memory cache
    pub async fn get_cached_caps(&self) -> Result<Vec<Cap>, RegistryError> {
        let cached_caps = self.cached_caps.lock().map_err(|e| {
            eprintln!("Stack trace: {}", std::backtrace::Backtrace::capture());
            RegistryError::CacheError(format!("Failed to lock cache: {}", e))
        })?;
        Ok(cached_caps.values().cloned().collect())
    }

    fn get_cache_dir() -> Result<PathBuf, RegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            RegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capns");
        Ok(cache_dir)
    }

    fn cache_key(&self, urn: &str) -> String {
        let normalized_urn = normalize_cap_urn(urn);
        let mut hasher = Sha256::new();
        hasher.update(normalized_urn.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn cache_file_path(&self, urn: &str) -> PathBuf {
        let key: String = self.cache_key(urn);
        self.cache_dir.join(format!("{}.json", key))
    }

    fn load_all_cached_caps(cache_dir: &PathBuf) -> Result<HashMap<String, Cap>, RegistryError> {
        let mut caps = HashMap::new();
        
        if !cache_dir.exists() {
            return Ok(caps);
        }
        
        for entry in fs::read_dir(cache_dir).map_err(|e| {
            RegistryError::CacheError(format!("Failed to read cache directory: {}", e))
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

                    let cache_entry: CacheEntry = match serde_json::from_str(&content) {
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

                    let urn = cache_entry.definition.urn_string();
                    let normalized_urn = normalize_cap_urn(&urn);
                    caps.insert(normalized_urn, cache_entry.definition);
                }
            }
        }
        
        Ok(caps)
    }

    fn save_to_cache(&self, cap: &Cap) -> Result<(), RegistryError> {
        let urn = cap.urn_string();
        let cache_file = self.cache_file_path(&urn);
        let cache_entry = CacheEntry {
            definition: cap.clone(),
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_hours: CACHE_DURATION_HOURS,
        };

        let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
            RegistryError::CacheError(format!("Failed to serialize cache entry: {}", e))
        })?;

        fs::write(&cache_file, content)
            .map_err(|e| RegistryError::CacheError(format!("Failed to write cache file: {}", e)))?;

        Ok(())
    }

    async fn fetch_from_registry(&self, urn: &str) -> Result<Cap, RegistryError> {
        let normalized_urn = normalize_cap_urn(urn);
        // URL-encode only the tags part (after "cap:") since the path prefix must be literal
        // The path is /cap:... where "cap:" is literal and the rest is URL-encoded
        let tags_part = normalized_urn.strip_prefix("cap:").unwrap_or(&normalized_urn);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", self.config.registry_base_url, encoded_tags);
        let response = self.client.get(&url).send().await.map_err(|e| {
            RegistryError::HttpError(format!("Failed to fetch from registry: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(RegistryError::NotFound(format!(
                "Cap '{}' not found in registry (HTTP {})",
                urn, response.status()
            )));
        }

        let cap: Cap = response.json().await.map_err(|e| {
            RegistryError::ParseError(format!("Failed to parse registry response for '{}': {}", urn, e))
        })?;

        // Cache the result
        self.save_to_cache(&cap)?;

        Ok(cap)
    }

    /// Validate a local cap against its canonical definition
    pub async fn validate_cap(&self, cap: &Cap) -> Result<(), RegistryError> {
        let canonical_cap = self.get_cap(&cap.urn_string()).await?;


        if cap.command != canonical_cap.command {
            return Err(RegistryError::ValidationError(format!(
                "Command mismatch. Local: {}, Canonical: {}",
                cap.command, canonical_cap.command
            )));
        }

        // Validate args match (check stdin via args)
        let local_stdin = cap.get_stdin_media_urn();
        let canonical_stdin = canonical_cap.get_stdin_media_urn();
        if local_stdin != canonical_stdin {
            return Err(RegistryError::ValidationError(format!(
                "stdin mismatch. Local: {:?}, Canonical: {:?}",
                local_stdin, canonical_stdin
            )));
        }

        Ok(())
    }

    /// Check if a cap URN exists in registry (either cached or available online)
    pub async fn cap_exists(&self, urn: &str) -> bool {
        self.get_cap(urn).await.is_ok()
    }

    pub fn clear_cache(&self) -> Result<(), RegistryError> {
        // Clear in-memory cache
        {
            let mut cached_caps = self.cached_caps.lock().map_err(|e| {
                RegistryError::CacheError(format!("Failed to lock cache for clearing: {}", e))
            })?;
            cached_caps.clear();
        }

        // Clear filesystem cache
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)
                .map_err(|e| RegistryError::CacheError(format!("Failed to clear cache directory: {}", e)))?;
            fs::create_dir_all(&self.cache_dir).map_err(|e| {
                RegistryError::CacheError(format!("Failed to recreate cache directory: {}", e))
            })?;
        }
        Ok(())
    }

    // ==========================================================================
    // TEST HELPERS - Available for integration tests in dependent crates
    // ==========================================================================

    /// Create an empty registry for testing purposes.
    /// This is a synchronous constructor that doesn't perform any initialization.
    /// Intended for use in tests only - creates a registry with no network configuration.
    pub fn new_for_test() -> Self {
        use std::path::PathBuf;
        Self {
            client: reqwest::Client::new(),
            cache_dir: PathBuf::from("/tmp/capns-test-cache"),
            cached_caps: Arc::new(Mutex::new(HashMap::new())),
            config: RegistryConfig::default(),
        }
    }

    /// Create a registry for testing with a custom configuration
    pub fn new_for_test_with_config(config: RegistryConfig) -> Self {
        use std::path::PathBuf;
        Self {
            client: reqwest::Client::new(),
            cache_dir: PathBuf::from("/tmp/capns-test-cache"),
            cached_caps: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Add caps to the in-memory cache for testing purposes.
    /// This allows tests to set up specific caps without network access.
    /// Intended for use in tests only.
    pub fn add_caps_to_cache(&self, caps: Vec<Cap>) {
        if let Ok(mut cached_caps) = self.cached_caps.lock() {
            for cap in caps {
                let urn = cap.urn_string();
                let normalized_urn = normalize_cap_urn(&urn);
                cached_caps.insert(normalized_urn, cap);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Cap not found in registry: {0}")]
    NotFound(String),

    #[error("Failed to parse registry response: {0}")]
    ParseError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use tempfile::TempDir;

    // Helper to create registry with a temporary cache directory
    async fn registry_with_temp_cache() -> (CapRegistry, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let registry = CapRegistry {
            client,
            cache_dir,
            cached_caps: Arc::new(Mutex::new(HashMap::new())),
            config: RegistryConfig::default(),
        };

        (registry, temp_dir)
    }

    // TEST135: Test registry creation with temporary cache directory succeeds
    #[tokio::test]
    async fn test_registry_creation() {
        let (registry, _temp_dir) = registry_with_temp_cache().await;
        assert!(registry.cache_dir.exists());
    }

    // TEST136: Test cache key generation produces consistent hashes for same URN
    #[tokio::test]
    async fn test_cache_key_generation() {
        let (registry, _temp_dir) = registry_with_temp_cache().await;
        // Use URNs with required in/out (new media URN format)
        let key1 = registry.cache_key("cap:in=media:void;op=extract;out=media:form=map;target=metadata");
        let key2 = registry.cache_key("cap:in=media:void;op=extract;out=media:form=map;target=metadata");
        let key3 = registry.cache_key("cap:in=media:void;op=different;out=media:object");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }
}

#[cfg(test)]
mod json_parse_tests {
    use crate::Cap;

    // TEST137: Test parsing registry JSON without stdin args verifies cap structure
    #[test]
    fn test_parse_registry_json() {
        // JSON without stdin args - means cap doesn't accept stdin
        // media_specs is now an array of media spec objects with urn field
        let json = r#"{"urn":"cap:in=\"media:listing-id\";op=use_grinder;out=\"media:task-id\"","command":"grinder_task","title":"Create Grinder Tool Task","cap_description":"Create a task for initial document analysis - first glance phase","metadata":{},"media_specs":[{"urn":"media:listing-id","media_type":"text/plain","title":"Listing ID","profile_uri":"https://filegrind.com/schema/listing-id","schema":{"type":"string","pattern":"[0-9a-f-]{36}","description":"FileGrind listing UUID"}},{"urn":"media:task-id","media_type":"application/json","title":"Task ID","profile_uri":"https://capns.org/schema/grinder_task-output","schema":{"type":"object","additionalProperties":false,"properties":{"task_id":{"type":"string","description":"ID of the created task"},"task_type":{"type":"string","description":"Type of task created"}},"required":["task_id","task_type"]}}],"args":[{"media_urn":"media:listing-id","required":true,"sources":[{"cli_flag":"--listing-id"}],"arg_description":"ID of the listing to analyze"}],"output":{"media_urn":"media:task-id","output_description":"Created task information"},"registered_by":{"username":"joeharshamshiri","registered_at":"2026-01-15T00:44:29.851Z"}}"#;

        let cap: Cap = serde_json::from_str(json).expect("Failed to parse JSON");
        assert_eq!(cap.title, "Create Grinder Tool Task");
        assert_eq!(cap.command, "grinder_task");
        assert!(cap.get_stdin_media_urn().is_none()); // No stdin source in args means no stdin support
    }

    // TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction
    #[test]
    fn test_parse_registry_json_with_stdin() {
        // JSON with stdin args - means cap accepts stdin of specified media type
        let json = r#"{"urn":"cap:in=\"media:pdf;bytes\";op=extract_metadata;out=\"media:file-metadata;textable;form=map\"","command":"extract-metadata","title":"Extract Metadata","args":[{"media_urn":"media:pdf;bytes","required":true,"sources":[{"stdin":"media:pdf;bytes"}]}]}"#;

        let cap: Cap = serde_json::from_str(json).expect("Failed to parse JSON");
        assert_eq!(cap.title, "Extract Metadata");
        assert!(cap.accepts_stdin());
        assert_eq!(cap.get_stdin_media_urn(), Some("media:pdf;bytes"));
    }
}

#[cfg(test)]
mod url_encoding_tests {
    use super::*;

    /// Test that URL construction keeps "cap:" literal and only encodes the tags part
    /// This guards against the bug where encoding "cap:" as "cap%3A" causes 404s
    // TEST139: Test URL construction keeps cap prefix literal and only encodes tags part
    #[test]
    fn test_url_keeps_cap_prefix_literal() {
        let config = RegistryConfig::default();
        let urn = r#"cap:in="media:string";op=test;out="media:object""#;
        let normalized = normalize_cap_urn(urn);
        let tags_part = normalized.strip_prefix("cap:").unwrap_or(&normalized);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", config.registry_base_url, encoded_tags);

        // URL must start with literal "cap:" not "cap%3A"
        assert!(url.contains("/cap:"), "URL must contain literal '/cap:' not encoded");
        assert!(!url.contains("cap%3A"), "URL must not encode 'cap:' as 'cap%3A'");
    }

    /// Test that media URNs in cap URNs are properly URL-encoded
    // TEST140: Test URL encodes media URNs with proper percent encoding for special characters
    #[test]
    fn test_url_encodes_quoted_media_urns() {
        let config = RegistryConfig::default();
        // Simple media URNs without semicolons don't need quotes (colons don't need quoting)
        let urn = r#"cap:in=media:listing-id;op=use_grinder;out=media:task-id"#;
        let normalized = normalize_cap_urn(urn);
        let tags_part = normalized.strip_prefix("cap:").unwrap_or(&normalized);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", config.registry_base_url, encoded_tags);

        // Equals must be encoded as %3D
        assert!(url.contains("%3D"), "Equals signs must be URL-encoded as %3D");
        // Semicolons must be encoded as %3B
        assert!(url.contains("%3B"), "Semicolons must be URL-encoded as %3B");
        // Colons in media URNs must be encoded as %3A
        assert!(url.contains("%3A"), "Colons must be URL-encoded as %3A");
    }

    /// Test the URL format for a simple cap URN
    // TEST141: Test exact URL format contains properly encoded media URN components
    #[test]
    fn test_exact_url_format() {
        let config = RegistryConfig::default();
        // Simple media URNs without semicolons don't need quotes (colons don't need quoting)
        let urn = r#"cap:in=media:listing-id;op=use_grinder;out=media:task-id"#;
        let normalized = normalize_cap_urn(urn);
        let tags_part = normalized.strip_prefix("cap:").unwrap_or(&normalized);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", config.registry_base_url, encoded_tags);

        // Just verify URL contains the encoded media URNs
        assert!(url.contains("media%3Alisting-id"), "URL should contain encoded media URN");
        assert!(url.contains("media%3Atask-id"), "URL should contain encoded media URN");
    }

    /// Test that normalization handles various input formats
    // TEST142: Test normalize handles different tag orders producing same canonical form
    #[test]
    fn test_normalize_handles_different_tag_orders() {
        // Different tag orders should normalize to the same canonical form
        let urn1 = r#"cap:op=test;in="media:string";out="media:object""#;
        let urn2 = r#"cap:in="media:string";out="media:object";op=test"#;

        let normalized1 = normalize_cap_urn(urn1);
        let normalized2 = normalize_cap_urn(urn2);

        assert_eq!(normalized1, normalized2, "Different tag orders should normalize to same form");
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    // TEST143: Test default config uses capns.org or environment variable values
    #[test]
    fn test_default_config() {
        let config = RegistryConfig::default();
        // Default should use capns.org (unless env var is set)
        assert!(config.registry_base_url.contains("capns.org") ||
                env::var("CAPNS_REGISTRY_URL").is_ok(),
                "Default registry URL should be capns.org or from env var");
        assert!(config.schema_base_url.contains("/schema"),
                "Schema URL should contain /schema");
    }

    // TEST144: Test custom registry URL updates both registry and schema base URLs
    #[test]
    fn test_custom_registry_url() {
        let config = RegistryConfig::new()
            .with_registry_url("https://localhost:8888");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://localhost:8888/schema");
    }

    // TEST145: Test custom registry and schema URLs set independently
    #[test]
    fn test_custom_registry_and_schema_url() {
        let config = RegistryConfig::new()
            .with_registry_url("https://localhost:8888")
            .with_schema_url("https://schemas.example.com");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://schemas.example.com");
    }

    // TEST146: Test schema URL not overwritten when set explicitly before registry URL
    #[test]
    fn test_schema_url_not_overwritten_when_explicit() {
        // If schema URL is set explicitly first, changing registry URL shouldn't change it
        let config = RegistryConfig::new()
            .with_schema_url("https://schemas.example.com")
            .with_registry_url("https://localhost:8888");
        assert_eq!(config.registry_base_url, "https://localhost:8888");
        assert_eq!(config.schema_base_url, "https://schemas.example.com");
    }

    // TEST147: Test registry for test with custom config creates registry with specified URLs
    #[test]
    fn test_registry_for_test_with_config() {
        let config = RegistryConfig::new()
            .with_registry_url("https://test-registry.local");
        let registry = CapRegistry::new_for_test_with_config(config);
        assert_eq!(registry.config().registry_base_url, "https://test-registry.local");
    }
}
