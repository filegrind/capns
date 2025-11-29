use crate::Cap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REGISTRY_BASE_URL: &str = "https://capns.org";
const CACHE_DURATION_HOURS: u64 = 24;

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
}

impl CapRegistry {
    /// Get a cap from registry or cache. Never returns None - either returns a Cap or an error.
    pub async fn get_cap(&self, urn: &str) -> Result<Cap, RegistryError> {
        // Try cache first
        match self.load_from_cache(urn) {
            Ok(cap) => return Ok(cap),
            Err(RegistryError::NotFound(_)) => {
                // Cache miss or expired, fetch from registry
                return self.fetch_from_registry(urn).await;
            },
            Err(e) => return Err(e), // Other cache errors are real errors
        }
    }

    /// Get multiple caps at once - fails if any cap is not available
    pub async fn get_caps(&self, urns: &[&str]) -> Result<Vec<Cap>, RegistryError> {
        let mut caps = Vec::new();
        for urn in urns {
            caps.push(self.get_cap(urn).await?);
        }
        Ok(caps)
    }

    /// Get all currently cached caps
    pub async fn get_cached_caps(&self) -> Result<Vec<Cap>, RegistryError> {
        let mut caps = Vec::new();
        if !self.cache_dir.exists() {
            return Ok(caps);
        }
        
        for entry in fs::read_dir(&self.cache_dir).map_err(|e| {
            RegistryError::CacheError(format!("Failed to read cache directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                RegistryError::CacheError(format!("Failed to read cache entry: {}", e))
            })?;
            
            if let Some(extension) = entry.path().extension() {
                if extension == "json" {
                    if let Some(stem) = entry.path().file_stem() {
                        if let Some(filename) = stem.to_str() {
                            // Try to load each cached cap, skip expired ones silently
                            if let Ok(cap) = self.load_from_cache(&format!("cached-{}", filename)) {
                                caps.push(cap);
                            }
                        }
                    }
                }
            }
        }
        Ok(caps)
    }

    pub fn new() -> Result<Self, RegistryError> {
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

        Ok(Self { client, cache_dir })
    }

    fn get_cache_dir() -> Result<PathBuf, RegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            RegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capns");
        Ok(cache_dir)
    }

    fn cache_key(&self, urn: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(urn.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn cache_file_path(&self, urn: &str) -> PathBuf {
        let key = self.cache_key(urn);
        self.cache_dir.join(format!("{}.json", key))
    }

    fn load_from_cache(&self, urn: &str) -> Result<Cap, RegistryError> {
        let cache_file = self.cache_file_path(urn);
        if !cache_file.exists() {
            return Err(RegistryError::NotFound(format!("Cap '{}' not found in cache", urn)));
        }

        let content = fs::read_to_string(&cache_file)
            .map_err(|e| RegistryError::CacheError(format!("Failed to read cache file: {}", e)))?;
        
        let cache_entry: CacheEntry = serde_json::from_str(&content)
            .map_err(|e| RegistryError::CacheError(format!("Failed to parse cache file: {}", e)))?;

        if cache_entry.is_expired() {
            fs::remove_file(&cache_file)
                .map_err(|e| RegistryError::CacheError(format!("Failed to remove expired cache file: {}", e)))?;
            return Err(RegistryError::NotFound(format!("Cached cap '{}' has expired", urn)));
        }

        Ok(cache_entry.definition)
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
        let url = format!("{}/{}", REGISTRY_BASE_URL, urn);
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
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)
                .map_err(|e| RegistryError::CacheError(format!("Failed to clear cache: {}", e)))?;
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
        let registry = CapRegistry::new().unwrap();
        assert!(registry.cache_dir.exists());
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        let registry = CapRegistry::new().unwrap();
        let key1 = registry.cache_key("cap:action=extract;target=metadata");
        let key2 = registry.cache_key("cap:action=extract;target=metadata");
        let key3 = registry.cache_key("cap:action=different");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }
}
