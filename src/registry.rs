use crate::Cap;
use include_dir::{include_dir, Dir};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REGISTRY_BASE_URL: &str = "https://capns.org";
const CACHE_DURATION_HOURS: u64 = 24;

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
}

impl CapRegistry {
    /// Create a new CapRegistry with standard capabilities bundled
    pub async fn new() -> Result<Self, RegistryError> {
        let _client = reqwest::Client::new();
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
        };
        
        // Copy bundled standard capabilities to cache if they don't exist
        registry.install_standard_caps().await?;
        
        Ok(registry)
    }
    
    /// Install bundled standard capabilities to cache directory if they don't exist
    async fn install_standard_caps(&self) -> Result<(), RegistryError> {
        for file in STANDARD_CAPS.files() {
            // Get filename without extension for URN construction
            let filename = file.path().file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| RegistryError::CacheError(
                    format!("Invalid filename: {:?}", file.path())
                ))?;
            
            // Parse the JSON content to get the Cap definition
            let content = file.contents_utf8()
                .ok_or_else(|| RegistryError::CacheError(
                    format!("File is not valid UTF-8: {:?}", file.path())
                ))?;
            
            let cap: Cap = serde_json::from_str(content).map_err(|e| {
                RegistryError::ParseError(format!("Failed to parse bundled cap {}: {}", filename, e))
            })?;
            
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
                
                let cache_content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
                    RegistryError::CacheError(format!("Failed to serialize standard cap {}: {}", filename, e))
                })?;
                
                fs::write(&cache_file, cache_content).map_err(|e| {
                    RegistryError::CacheError(format!("Failed to write standard cap to cache {}: {}", filename, e))
                })?;
                
                // Also add to in-memory cache
                if let Ok(mut cached_caps) = self.cached_caps.lock() {
                    cached_caps.insert(normalized_urn.clone(), cap);
                }
                
                eprintln!("Installed standard capability: {}", normalized_urn);
            }
        }
        
        Ok(())
    }
    
    /// Get all bundled standard capabilities without network access
    pub fn get_standard_caps(&self) -> Result<Vec<Cap>, RegistryError> {
        let mut caps = Vec::new();
        
        for file in STANDARD_CAPS.files() {
            let content = file.contents_utf8()
                .ok_or_else(|| RegistryError::CacheError(
                    format!("File is not valid UTF-8: {:?}", file.path())
                ))?;
            
            let cap: Cap = serde_json::from_str(content).map_err(|e| {
                RegistryError::ParseError(format!("Failed to parse bundled cap: {}", e))
            })?;
            
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
            let entry = entry.map_err(|e| {
                RegistryError::CacheError(format!("Failed to read cache entry: {}", e))
            })?;
            
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "json" {
                    let content = fs::read_to_string(&path)
                        .map_err(|e| RegistryError::CacheError(format!("Failed to read cache file {:?}: {}", path, e)))?;
                    
                    let cache_entry: CacheEntry = serde_json::from_str(&content)
                        .map_err(|e| RegistryError::CacheError(format!("Failed to parse cache file {:?}: {}", path, e)))?;

                    if cache_entry.is_expired() {
                        // Remove expired cache file
                        fs::remove_file(&path)
                            .map_err(|e| RegistryError::CacheError(format!("Failed to remove expired cache file {:?}: {}", path, e)))?;
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
        let url = format!("{}/cap:{}", REGISTRY_BASE_URL, encoded_tags);
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

        if cap.accepts_stdin != canonical_cap.accepts_stdin {
            return Err(RegistryError::ValidationError(format!(
                "accepts_stdin mismatch. Local: {}, Canonical: {}",
                cap.accepts_stdin, canonical_cap.accepts_stdin
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
        // Use URNs with required in/out (new media URN format)
        let key1 = registry.cache_key("cap:in=\"media:type=void;v=1\";op=extract;out=\"media:type=object;v=1\";target=metadata");
        let key2 = registry.cache_key("cap:in=\"media:type=void;v=1\";op=extract;out=\"media:type=object;v=1\";target=metadata");
        let key3 = registry.cache_key("cap:in=\"media:type=void;v=1\";op=different;out=\"media:type=object;v=1\"");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }
}

#[cfg(test)]
mod json_parse_tests {
    use crate::Cap;
    
    #[test]
    fn test_parse_registry_json() {
        let json = r#"{"urn":{"tags":{"op":"use_grinder","in":"media:type=listing-id;v=1","out":"media:type=task-id;v=1"}},"command":"grinder_task","title":"Create Grinder Tool Task","cap_description":"Create a task for initial document analysis - first glance phase","metadata":{},"media_specs":{"media:type=listing-id;v=1":{"media_type":"text/plain","profile_uri":"https://filegrind.com/schema/listing-id","schema":{"type":"string","pattern":"[0-9a-f-]{36}","description":"FileGrind listing UUID"}},"media:type=task-id;v=1":{"media_type":"application/json","profile_uri":"https://capns.org/schema/grinder_task-output","schema":{"type":"object","additionalProperties":false,"properties":{"task_id":{"type":"string","description":"ID of the created task"},"task_type":{"type":"string","description":"Type of task created"}},"required":["task_id","task_type"]}}},"accepts_stdin":false,"arguments":{"required":[{"name":"listing_id","media_urn":"media:type=listing-id;v=1","arg_description":"ID of the listing to analyze","cli_flag":"--listing-id"}]},"output":{"media_urn":"media:type=task-id;v=1","output_description":"Created task information"},"registered_by":{"username":"joeharshamshiri","registered_at":"2026-01-15T00:44:29.851Z"}}"#;
        
        let cap: Cap = serde_json::from_str(json).expect("Failed to parse JSON");
        assert_eq!(cap.title, "Create Grinder Tool Task");
        assert_eq!(cap.command, "grinder_task");
    }
}

#[cfg(test)]
mod url_tests {
    use super::*;
    
    #[test]
    fn test_url_construction() {
        let urn = r#"cap:in="media:type=listing-id;v=1";op=use_grinder;out="media:type=task-id;v=1""#;
        let normalized = normalize_cap_urn(urn);

        // Only encode the tags part after "cap:"
        let tags_part = normalized.strip_prefix("cap:").unwrap_or(&normalized);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", REGISTRY_BASE_URL, encoded_tags);

        // The URL should have literal "cap:" prefix and encoded tags
        let expected_url = "https://capns.org/cap:in%3D%22media%3Atype%3Dlisting-id%3Bv%3D1%22%3Bop%3Duse_grinder%3Bout%3D%22media%3Atype%3Dtask-id%3Bv%3D1%22";
        assert_eq!(url, expected_url);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_fetch_from_registry() {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();

        let urn = r#"cap:in="media:type=listing-id;v=1";op=use_grinder;out="media:type=task-id;v=1""#;
        let normalized_urn = normalize_cap_urn(urn);
        // Only encode the tags part after "cap:"
        let tags_part = normalized_urn.strip_prefix("cap:").unwrap_or(&normalized_urn);
        let encoded_tags = urlencoding::encode(tags_part);
        let url = format!("{}/cap:{}", REGISTRY_BASE_URL, encoded_tags);

        println!("Fetching URL: {}", url);

        let response = client.get(&url).send().await.expect("Request failed");

        println!("Status: {}", response.status());

        let body = response.text().await.expect("Failed to get body");
        println!("Body length: {}", body.len());
        println!("Body (first 500 chars): {}", &body[..body.len().min(500)]);

        // Try to parse as Cap
        match serde_json::from_str::<crate::Cap>(&body) {
            Ok(cap) => println!("Successfully parsed cap: {}", cap.title),
            Err(e) => println!("Parse error: {}", e),
        }
    }
}
