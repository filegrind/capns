//! Profile Schema Registry
//!
//! Registry for JSON Schema profiles. Downloads and caches schemas from profile URLs
//! for validating data against media spec type definitions.
//! Embeds default schemas for standard types (string, integer, number, boolean, object, arrays).
//! Uses a two-level cache: disk-based cached schemas and in-memory compiled schemas.

use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::standard::media::{
    PROFILE_STR, PROFILE_INT, PROFILE_NUM, PROFILE_BOOL,
    PROFILE_OBJ, PROFILE_STR_ARRAY, PROFILE_NUM_ARRAY,
    PROFILE_BOOL_ARRAY, PROFILE_OBJ_ARRAY,
};

const CACHE_DURATION_HOURS: u64 = 24 * 7; // Cache for 1 week

/// Embedded default schemas
mod embedded_schemas {
    pub const STR_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/str",
        "title": "String",
        "description": "A JSON string value",
        "type": "string"
    }"#;

    pub const INT_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/int",
        "title": "Integer",
        "description": "A JSON integer value",
        "type": "integer"
    }"#;

    pub const NUM_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/num",
        "title": "Number",
        "description": "A JSON number value (integer or floating point)",
        "type": "number"
    }"#;

    pub const BOOL_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/bool",
        "title": "Boolean",
        "description": "A JSON boolean value (true or false)",
        "type": "boolean"
    }"#;

    pub const OBJ_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/obj",
        "title": "Object",
        "description": "A JSON object value",
        "type": "object"
    }"#;

    pub const STR_ARRAY_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/str-array",
        "title": "String Array",
        "description": "A JSON array of string values",
        "type": "array",
        "items": { "type": "string" }
    }"#;

    pub const NUM_ARRAY_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/num-array",
        "title": "Number Array",
        "description": "A JSON array of number values",
        "type": "array",
        "items": { "type": "number" }
    }"#;

    pub const BOOL_ARRAY_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/bool-array",
        "title": "Boolean Array",
        "description": "A JSON array of boolean values",
        "type": "array",
        "items": { "type": "boolean" }
    }"#;

    pub const OBJ_ARRAY_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://capns.org/schema/obj-array",
        "title": "Object Array",
        "description": "A JSON array of object values",
        "type": "array",
        "items": { "type": "object" }
    }"#;

    /// Get all embedded schemas as (profile_url, schema_json) pairs
    pub fn all() -> Vec<(&'static str, &'static str)> {
        vec![
            (super::PROFILE_STR, STR_SCHEMA),
            (super::PROFILE_INT, INT_SCHEMA),
            (super::PROFILE_NUM, NUM_SCHEMA),
            (super::PROFILE_BOOL, BOOL_SCHEMA),
            (super::PROFILE_OBJ, OBJ_SCHEMA),
            (super::PROFILE_STR_ARRAY, STR_ARRAY_SCHEMA),
            (super::PROFILE_NUM_ARRAY, NUM_ARRAY_SCHEMA),
            (super::PROFILE_BOOL_ARRAY, BOOL_ARRAY_SCHEMA),
            (super::PROFILE_OBJ_ARRAY, OBJ_ARRAY_SCHEMA),
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    schema_json: JsonValue,
    profile_url: String,
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

/// Compiled schema with its source JSON
struct CompiledSchema {
    compiled: JSONSchema,
    #[allow(dead_code)]
    source: JsonValue,
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSchema")
            .field("source", &self.source)
            .finish()
    }
}

#[derive(Debug)]
pub struct ProfileSchemaRegistry {
    client: reqwest::Client,
    cache_dir: PathBuf,
    /// In-memory cache of compiled schemas
    compiled_schemas: Arc<Mutex<HashMap<String, Arc<CompiledSchema>>>>,
}

impl ProfileSchemaRegistry {
    /// Create a new ProfileSchemaRegistry with standard schemas bundled
    pub async fn new() -> Result<Self, ProfileSchemaError> {
        let cache_dir = Self::get_cache_dir()?;
        Self::new_with_cache_dir(cache_dir).await
    }

    /// Create a new ProfileSchemaRegistry with a custom cache directory
    pub async fn new_with_cache_dir(cache_dir: PathBuf) -> Result<Self, ProfileSchemaError> {
        fs::create_dir_all(&cache_dir).map_err(|e| {
            ProfileSchemaError::CacheError(format!("Failed to create cache directory: {}", e))
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| {
                ProfileSchemaError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        // Load all cached schemas into memory
        let compiled_schemas_map = Self::load_all_cached_schemas(&cache_dir)?;
        let compiled_schemas = Arc::new(Mutex::new(compiled_schemas_map));

        let registry = Self {
            client,
            cache_dir,
            compiled_schemas,
        };

        // Install bundled standard schemas to cache if they don't exist
        registry.install_standard_schemas().await?;

        Ok(registry)
    }

    /// Install bundled standard schemas to cache directory if they don't exist
    async fn install_standard_schemas(&self) -> Result<(), ProfileSchemaError> {
        for (profile_url, schema_json_str) in embedded_schemas::all() {
            let cache_file = self.cache_file_path(profile_url);

            if !cache_file.exists() {
                let schema_json: JsonValue = serde_json::from_str(schema_json_str)
                    .map_err(|e| ProfileSchemaError::ParseError(format!(
                        "Failed to parse embedded schema for {}: {}", profile_url, e
                    )))?;

                // Compile to verify it's valid
                let compiled = JSONSchema::compile(&schema_json)
                    .map_err(|e| ProfileSchemaError::InvalidSchema(format!(
                        "Failed to compile embedded schema for {}: {}", profile_url, e
                    )))?;

                // Create cache entry
                let cache_entry = CacheEntry {
                    schema_json: schema_json.clone(),
                    profile_url: profile_url.to_string(),
                    cached_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ttl_hours: CACHE_DURATION_HOURS,
                };

                let cache_content = serde_json::to_string_pretty(&cache_entry)
                    .map_err(|e| ProfileSchemaError::CacheError(format!(
                        "Failed to serialize schema for {}: {}", profile_url, e
                    )))?;

                fs::write(&cache_file, cache_content).map_err(|e| {
                    ProfileSchemaError::CacheError(format!(
                        "Failed to write schema to cache for {}: {}", profile_url, e
                    ))
                })?;

                // Add to in-memory cache
                if let Ok(mut schemas) = self.compiled_schemas.lock() {
                    schemas.insert(
                        profile_url.to_string(),
                        Arc::new(CompiledSchema { compiled, source: schema_json }),
                    );
                }
            }
        }

        Ok(())
    }

    fn get_cache_dir() -> Result<PathBuf, ProfileSchemaError> {
        let cache_dir = dirs::cache_dir()
            .ok_or_else(|| ProfileSchemaError::CacheError(
                "Could not determine cache directory".to_string()
            ))?;
        Ok(cache_dir.join("capns").join("profile_schemas"))
    }

    fn cache_key(&self, profile_url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(profile_url.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn cache_file_path(&self, profile_url: &str) -> PathBuf {
        let key = self.cache_key(profile_url);
        self.cache_dir.join(format!("{}.json", &key[..16]))
    }

    fn load_all_cached_schemas(cache_dir: &PathBuf) -> Result<HashMap<String, Arc<CompiledSchema>>, ProfileSchemaError> {
        let mut schemas = HashMap::new();

        if !cache_dir.exists() {
            return Ok(schemas);
        }

        for entry in fs::read_dir(cache_dir).map_err(|e| {
            ProfileSchemaError::CacheError(format!("Failed to read cache directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                ProfileSchemaError::CacheError(format!("Failed to read cache entry: {}", e))
            })?;

            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "json" {
                    let content = fs::read_to_string(&path)
                        .map_err(|e| ProfileSchemaError::CacheError(format!(
                            "Failed to read cache file {:?}: {}", path, e
                        )))?;

                    let cache_entry: CacheEntry = match serde_json::from_str(&content) {
                        Ok(entry) => entry,
                        Err(_) => continue, // Skip invalid cache files
                    };

                    if cache_entry.is_expired() {
                        // Remove expired cache file
                        let _ = fs::remove_file(&path);
                        continue;
                    }

                    // Compile the schema
                    if let Ok(compiled) = JSONSchema::compile(&cache_entry.schema_json) {
                        schemas.insert(
                            cache_entry.profile_url.clone(),
                            Arc::new(CompiledSchema {
                                compiled,
                                source: cache_entry.schema_json,
                            }),
                        );
                    }
                }
            }
        }

        Ok(schemas)
    }

    fn save_to_cache(&self, profile_url: &str, schema_json: &JsonValue) -> Result<(), ProfileSchemaError> {
        let cache_file = self.cache_file_path(profile_url);
        let cache_entry = CacheEntry {
            schema_json: schema_json.clone(),
            profile_url: profile_url.to_string(),
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl_hours: CACHE_DURATION_HOURS,
        };

        let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
            ProfileSchemaError::CacheError(format!("Failed to serialize cache entry: {}", e))
        })?;

        fs::write(&cache_file, content)
            .map_err(|e| ProfileSchemaError::CacheError(format!("Failed to write cache file: {}", e)))?;

        Ok(())
    }

    /// Get a compiled schema for a profile URL.
    /// Returns None if the profile can't be fetched or isn't a valid schema.
    async fn get_schema(&self, profile_url: &str) -> Option<Arc<CompiledSchema>> {
        // Check in-memory cache first
        {
            let schemas = self.compiled_schemas.lock().ok()?;
            if let Some(schema) = schemas.get(profile_url) {
                return Some(Arc::clone(schema));
            }
        }

        // Not in memory cache - try to fetch from URL
        match self.fetch_schema(profile_url).await {
            Ok((schema_json, compiled)) => {
                let compiled_schema = Arc::new(CompiledSchema {
                    compiled,
                    source: schema_json.clone(),
                });

                // Save to disk cache
                let _ = self.save_to_cache(profile_url, &schema_json);

                // Add to memory cache
                if let Ok(mut schemas) = self.compiled_schemas.lock() {
                    schemas.insert(profile_url.to_string(), Arc::clone(&compiled_schema));
                }

                Some(compiled_schema)
            }
            Err(_) => None, // Fetch failed - skip validation for this profile
        }
    }

    async fn fetch_schema(&self, profile_url: &str) -> Result<(JsonValue, JSONSchema), ProfileSchemaError> {
        let response = self.client.get(profile_url).send().await.map_err(|e| {
            ProfileSchemaError::HttpError(format!("Failed to fetch schema from {}: {}", profile_url, e))
        })?;

        if !response.status().is_success() {
            return Err(ProfileSchemaError::NotFound(format!(
                "Schema not found at {} (HTTP {})", profile_url, response.status()
            )));
        }

        let content = response.text().await.map_err(|e| {
            ProfileSchemaError::HttpError(format!("Failed to read response from {}: {}", profile_url, e))
        })?;

        let schema_json: JsonValue = serde_json::from_str(&content).map_err(|e| {
            ProfileSchemaError::ParseError(format!("Invalid JSON from {}: {}", profile_url, e))
        })?;

        let compiled = JSONSchema::compile(&schema_json).map_err(|e| {
            ProfileSchemaError::InvalidSchema(format!("Invalid JSON Schema from {}: {}", profile_url, e))
        })?;

        Ok((schema_json, compiled))
    }

    /// Validate a value against a profile's schema.
    /// Returns Ok(()) if valid or if schema not available (logs warning and skips validation).
    /// Returns Err with validation errors if invalid.
    pub async fn validate(&self, profile_url: &str, value: &JsonValue) -> Result<(), Vec<String>> {
        match self.get_schema(profile_url).await {
            Some(schema) => {
                match schema.compiled.validate(value) {
                    Ok(()) => Ok(()),
                    Err(errors) => {
                        let error_messages: Vec<String> = errors
                            .map(|e| e.to_string())
                            .collect();
                        Err(error_messages)
                    }
                }
            }
            None => {
                eprintln!("[WARN] Schema not available for profile '{}' - skipping validation", profile_url);
                Ok(())
            }
        }
    }

    /// Validate synchronously using only cached schemas.
    /// Returns Ok(()) if valid or if schema not cached.
    /// Returns Err with validation errors if invalid.
    pub fn validate_cached(&self, profile_url: &str, value: &JsonValue) -> Result<(), Vec<String>> {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return Ok(()), // Lock failed - skip validation
        };

        match schemas.get(profile_url) {
            Some(schema) => {
                match schema.compiled.validate(value) {
                    Ok(()) => Ok(()),
                    Err(errors) => {
                        let error_messages: Vec<String> = errors
                            .map(|e| e.to_string())
                            .collect();
                        Err(error_messages)
                    }
                }
            }
            None => Ok(()), // Schema not cached - skip validation
        }
    }

    /// Check if a profile URL exists in cache (either embedded or downloaded)
    pub fn schema_exists(&self, profile_url: &str) -> bool {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return false,
        };
        schemas.contains_key(profile_url)
    }

    /// Get all cached profile URLs
    pub fn get_cached_profiles(&self) -> Vec<String> {
        let schemas = match self.compiled_schemas.lock() {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        schemas.keys().cloned().collect()
    }

    /// Clear all caches (memory and disk)
    pub fn clear_cache(&self) -> Result<(), ProfileSchemaError> {
        // Clear in-memory cache
        {
            let mut schemas = self.compiled_schemas.lock()
                .map_err(|e| ProfileSchemaError::CacheError(format!("Failed to lock cache: {}", e)))?;
            schemas.clear();
        }

        // Clear disk cache
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)
                .map_err(|e| ProfileSchemaError::CacheError(format!("Failed to clear cache: {}", e)))?;
            fs::create_dir_all(&self.cache_dir)
                .map_err(|e| ProfileSchemaError::CacheError(format!("Failed to recreate cache: {}", e)))?;
        }

        Ok(())
    }

    /// Check if a profile URL is one of the embedded defaults
    pub fn is_embedded_profile(profile_url: &str) -> bool {
        embedded_schemas::all().iter().any(|(url, _)| *url == profile_url)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileSchemaError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Schema not found: {0}")]
    NotFound(String),

    #[error("Failed to parse schema: {0}")]
    ParseError(String),

    #[error("Invalid JSON Schema: {0}")]
    InvalidSchema(String),

    #[error("Cache error: {0}")]
    CacheError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    /// Create a registry with an isolated temporary cache directory
    async fn create_test_registry() -> (ProfileSchemaRegistry, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let registry = ProfileSchemaRegistry::new_with_cache_dir(temp_dir.path().to_path_buf())
            .await
            .expect("Failed to create registry");
        (registry, temp_dir)
    }

    // TEST618: Verify profile schema registry creation succeeds with temp cache
    #[tokio::test]
    async fn test618_registry_creation() {
        let (registry, _temp_dir) = create_test_registry().await;
        assert!(registry.cache_dir.exists());
    }

    // TEST619: Verify all 9 embedded standard schemas are loaded on creation
    #[tokio::test]
    async fn test619_embedded_schemas_loaded() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Check that embedded schemas are available
        assert!(registry.schema_exists(PROFILE_STR));
        assert!(registry.schema_exists(PROFILE_INT));
        assert!(registry.schema_exists(PROFILE_NUM));
        assert!(registry.schema_exists(PROFILE_BOOL));
        assert!(registry.schema_exists(PROFILE_OBJ));
        assert!(registry.schema_exists(PROFILE_STR_ARRAY));
        assert!(registry.schema_exists(PROFILE_NUM_ARRAY));
        assert!(registry.schema_exists(PROFILE_BOOL_ARRAY));
        assert!(registry.schema_exists(PROFILE_OBJ_ARRAY));
    }

    // TEST620: Verify string schema validates strings and rejects non-strings
    #[tokio::test]
    async fn test620_string_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string
        assert!(registry.validate(PROFILE_STR, &json!("hello")).await.is_ok());

        // Invalid: not a string
        assert!(registry.validate(PROFILE_STR, &json!(42)).await.is_err());
    }

    // TEST621: Verify integer schema validates integers and rejects floats and strings
    #[tokio::test]
    async fn test621_integer_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid integer
        assert!(registry.validate(PROFILE_INT, &json!(42)).await.is_ok());

        // Invalid: not an integer (float)
        assert!(registry.validate(PROFILE_INT, &json!(3.14)).await.is_err());

        // Invalid: not a number
        assert!(registry.validate(PROFILE_INT, &json!("hello")).await.is_err());
    }

    // TEST622: Verify number schema validates integers and floats, rejects strings
    #[tokio::test]
    async fn test622_number_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid number (integer)
        assert!(registry.validate(PROFILE_NUM, &json!(42)).await.is_ok());

        // Valid number (float)
        assert!(registry.validate(PROFILE_NUM, &json!(3.14)).await.is_ok());

        // Invalid: not a number
        assert!(registry.validate(PROFILE_NUM, &json!("hello")).await.is_err());
    }

    // TEST623: Verify boolean schema validates true/false and rejects string "true"
    #[tokio::test]
    async fn test623_boolean_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid boolean
        assert!(registry.validate(PROFILE_BOOL, &json!(true)).await.is_ok());
        assert!(registry.validate(PROFILE_BOOL, &json!(false)).await.is_ok());

        // Invalid: not a boolean
        assert!(registry.validate(PROFILE_BOOL, &json!("true")).await.is_err());
    }

    // TEST624: Verify object schema validates objects and rejects arrays
    #[tokio::test]
    async fn test624_object_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid object
        assert!(registry.validate(PROFILE_OBJ, &json!({"key": "value"})).await.is_ok());

        // Invalid: not an object
        assert!(registry.validate(PROFILE_OBJ, &json!([1, 2, 3])).await.is_err());
    }

    // TEST625: Verify string array schema validates string arrays and rejects mixed arrays
    #[tokio::test]
    async fn test625_string_array_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string array
        assert!(registry.validate(PROFILE_STR_ARRAY, &json!(["a", "b", "c"])).await.is_ok());

        // Invalid: contains non-strings
        assert!(registry.validate(PROFILE_STR_ARRAY, &json!(["a", 1, "c"])).await.is_err());

        // Invalid: not an array
        assert!(registry.validate(PROFILE_STR_ARRAY, &json!("hello")).await.is_err());
    }

    // TEST626: Verify unknown profile URL skips validation and returns Ok
    #[tokio::test]
    async fn test626_unknown_profile_skips_validation() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Unknown profile should return Ok - skip validation
        let result = registry.validate("https://example.com/unknown-profile", &json!("anything")).await;
        assert!(result.is_ok());
    }

    // TEST627: Verify is_embedded_profile recognizes standard and rejects custom URLs
    #[test]
    fn test627_is_embedded_profile() {
        assert!(ProfileSchemaRegistry::is_embedded_profile(PROFILE_STR));
        assert!(ProfileSchemaRegistry::is_embedded_profile(PROFILE_INT));
        assert!(!ProfileSchemaRegistry::is_embedded_profile("https://example.com/custom"));
    }

    // TEST611: is_embedded_profile recognizes all 9 embedded profiles and rejects non-embedded
    #[test]
    fn test611_is_embedded_profile_comprehensive() {
        let embedded = [
            PROFILE_STR, PROFILE_INT, PROFILE_NUM, PROFILE_BOOL, PROFILE_OBJ,
            PROFILE_STR_ARRAY, PROFILE_NUM_ARRAY, PROFILE_BOOL_ARRAY, PROFILE_OBJ_ARRAY,
        ];

        for url in &embedded {
            assert!(
                ProfileSchemaRegistry::is_embedded_profile(url),
                "'{}' should be recognized as embedded", url
            );
        }

        // Non-embedded profiles
        assert!(!ProfileSchemaRegistry::is_embedded_profile("https://capns.org/schema/custom"));
        assert!(!ProfileSchemaRegistry::is_embedded_profile(""));
        assert!(!ProfileSchemaRegistry::is_embedded_profile("https://example.com/schema/str"));
    }

    // TEST612: clear_cache empties all in-memory schemas
    #[tokio::test]
    async fn test612_clear_cache() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Standard schemas should be loaded
        assert!(registry.schema_exists(PROFILE_STR));
        assert!(!registry.get_cached_profiles().is_empty());

        // Clear
        registry.clear_cache().expect("clear_cache should succeed");

        // All schemas should be gone
        assert!(!registry.schema_exists(PROFILE_STR));
        assert!(registry.get_cached_profiles().is_empty());
    }

    // TEST613: validate_cached validates against cached standard schemas
    #[tokio::test]
    async fn test613_validate_cached() {
        let (registry, _temp_dir) = create_test_registry().await;

        // Valid string against string schema
        assert!(registry.validate_cached(PROFILE_STR, &json!("hello")).is_ok());

        // Invalid: number against string schema
        let result = registry.validate_cached(PROFILE_STR, &json!(42));
        assert!(result.is_err(), "Number should not validate as string");

        // Valid integer
        assert!(registry.validate_cached(PROFILE_INT, &json!(42)).is_ok());

        // Valid object array
        assert!(registry.validate_cached(PROFILE_OBJ_ARRAY, &json!([{"a": 1}])).is_ok());

        // Invalid: string array against object array schema
        let result = registry.validate_cached(PROFILE_OBJ_ARRAY, &json!(["a", "b"]));
        assert!(result.is_err());

        // Non-cached profile returns Ok (skip validation)
        assert!(registry.validate_cached("https://example.com/unknown", &json!("anything")).is_ok());
    }
}
