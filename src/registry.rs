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
    // Remove cap: prefix
    let tags_string = urn.strip_prefix("cap:").unwrap_or(urn);
    
    // Remove trailing semicolon if present
    let clean_tags_string = tags_string.trim_end_matches(';');
    
    // Split into tag pairs and filter out empty ones
    let tag_pairs: Vec<&str> = clean_tags_string.split(';')
        .filter(|pair| !pair.trim().is_empty())
        .collect();
    
    // Parse into key-value pairs
    let mut tags = std::collections::BTreeMap::new(); // BTreeMap for sorted keys
    for pair in tag_pairs {
        if let Some((key, value)) = pair.split_once('=') {
            tags.insert(key.trim(), value.trim());
        }
    }
    
    // Rebuild normalized CAPURN with sorted tags
    if tags.is_empty() {
        "cap:".to_string()
    } else {
        let sorted_tags: Vec<String> = tags.iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect();
        format!("cap:{}", sorted_tags.join(";"))
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
        let client = reqwest::Client::new();
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
        let url = format!("{}/{}", REGISTRY_BASE_URL, normalized_urn);
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

        if cap.version != canonical_cap.version {
            return Err(RegistryError::ValidationError(format!(
                "Version mismatch. Local: {}, Canonical: {}",
                cap.version, canonical_cap.version
            )));
        }

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

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = CapRegistry::new().await.unwrap();
        assert!(registry.cache_dir.exists());
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        let registry = CapRegistry::new().await.unwrap();
        let key1 = registry.cache_key("cap:action=extract;target=metadata");
        let key2 = registry.cache_key("cap:action=extract;target=metadata");
        let key3 = registry.cache_key("cap:action=different");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }
}
