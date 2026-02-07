//! Plugin Repository
//!
//! Fetches and caches plugin registry data from configured plugin repositories.
//! Provides plugin suggestions when a cap isn't available but a plugin exists that could provide it.

use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use thiserror::Error;

/// Plugin repository errors
#[derive(Debug, Error)]
pub enum PluginRepoError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    #[error("Failed to parse registry response: {0}")]
    ParseError(String),
    #[error("Registry request failed with status {0}")]
    StatusError(u16),
}

pub type Result<T> = std::result::Result<T, PluginRepoError>;

/// Deserialize a possibly-null string as an empty string.
/// Handles API responses where string fields may be `null` instead of absent.
fn null_as_empty_string<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<String, D::Error> {
    Option::<String>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// A plugin's capability summary from the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginCapSummary {
    pub urn: String,
    pub title: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub description: String,
}

/// A plugin version's package info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginPackageInfo {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

/// A plugin version entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginVersionInfo {
    pub release_date: String,
    #[serde(default)]
    pub changelog: Vec<String>,
    pub platform: String,
    pub package: PluginPackageInfo,
    #[serde(default)]
    pub binary: Option<PluginPackageInfo>,
}

/// A plugin entry from the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginInfo {
    pub id: String,
    pub name: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub version: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub description: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub author: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub homepage: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub team_id: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub signed_at: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub min_app_version: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub page_url: String,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub caps: Vec<PluginCapSummary>,
    // Distribution fields - required for plugin installation
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub platform: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub package_name: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub package_sha256: String,
    #[serde(default)]
    pub package_size: u64,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub binary_name: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub binary_sha256: String,
    #[serde(default)]
    pub binary_size: u64,
    /// Changelog entries keyed by version
    #[serde(default)]
    pub changelog: HashMap<String, Vec<String>>,
    /// All available versions (newest first)
    #[serde(default)]
    pub available_versions: Vec<String>,
}

/// The plugin registry response from the API
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRegistryResponse {
    pub plugins: Vec<PluginInfo>,
}

/// A plugin suggestion for a missing cap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginSuggestion {
    pub plugin_id: String,
    pub plugin_name: String,
    pub plugin_description: String,
    pub cap_urn: String,
    pub cap_title: String,
    pub latest_version: String,
    pub repo_url: String,
    pub page_url: String,
}

/// Cached plugin repository data
struct PluginRepoCache {
    /// All plugins indexed by plugin ID
    plugins: HashMap<String, PluginInfo>,
    /// Cap URN to plugin IDs that provide it
    cap_to_plugins: HashMap<String, Vec<String>>,
    /// When the cache was last updated
    last_updated: Instant,
    /// The repo URL this cache is from
    repo_url: String,
}

/// Service for fetching and caching plugin repository data
pub struct PluginRepo {
    http_client: Client,
    /// Cache per repo URL
    caches: Arc<RwLock<HashMap<String, PluginRepoCache>>>,
    /// Cache TTL in seconds
    cache_ttl: Duration,
}

impl PluginRepo {
    /// Create a new plugin repo service
    pub fn new(cache_ttl_seconds: u64) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("FileGrindEngine/1.0.0")
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            caches: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(cache_ttl_seconds),
        }
    }

    /// Fetch plugin registry from a URL
    async fn fetch_registry(&self, repo_url: &str) -> Result<PluginRegistryResponse> {
        eprintln!("[DEBUG] Fetching plugin registry from: {}", repo_url);

        let response = self.http_client
            .get(repo_url)
            .send()
            .await
            .map_err(|e| PluginRepoError::HttpError(format!("Failed to fetch from {}: {}", repo_url, e)))?;

        if !response.status().is_success() {
            return Err(PluginRepoError::StatusError(response.status().as_u16()));
        }

        let registry: PluginRegistryResponse = response
            .json()
            .await
            .map_err(|e| PluginRepoError::ParseError(format!("Failed to parse from {}: {}", repo_url, e)))?;

        eprintln!("[INFO] Fetched {} plugins from {}", registry.plugins.len(), repo_url);
        Ok(registry)
    }

    /// Update cache from a registry response
    fn update_cache(caches: &mut HashMap<String, PluginRepoCache>, repo_url: &str, registry: PluginRegistryResponse) {
        let mut plugins: HashMap<String, PluginInfo> = HashMap::new();
        let mut cap_to_plugins: HashMap<String, Vec<String>> = HashMap::new();

        for plugin_info in registry.plugins {
            let plugin_id = plugin_info.id.clone();
            for cap in &plugin_info.caps {
                cap_to_plugins
                    .entry(cap.urn.clone())
                    .or_default()
                    .push(plugin_id.clone());
            }
            plugins.insert(plugin_id, plugin_info);
        }

        caches.insert(repo_url.to_string(), PluginRepoCache {
            plugins,
            cap_to_plugins,
            last_updated: Instant::now(),
            repo_url: repo_url.to_string(),
        });
    }

    /// Sync plugin data from the given repository URLs
    pub async fn sync_repos(&self, repo_urls: &[String]) {
        for repo_url in repo_urls {
            match self.fetch_registry(repo_url).await {
                Ok(registry) => {
                    let mut caches = self.caches.write().await;
                    Self::update_cache(&mut caches, repo_url, registry);
                }
                Err(e) => {
                    eprintln!("[WARN] Failed to sync plugin repo {}: {}", repo_url, e);
                    // Continue with other repos
                }
            }
        }
    }

    /// Check if a cache is stale
    fn is_cache_stale(&self, cache: &PluginRepoCache) -> bool {
        cache.last_updated.elapsed() > self.cache_ttl
    }

    /// Get plugin suggestions for a cap URN that isn't available
    pub async fn get_suggestions_for_cap(&self, cap_urn: &str) -> Vec<PluginSuggestion> {
        let caches = self.caches.read().await;
        let mut suggestions = Vec::new();

        for cache in caches.values() {
            if let Some(plugin_ids) = cache.cap_to_plugins.get(cap_urn) {
                for plugin_id in plugin_ids {
                    if let Some(plugin) = cache.plugins.get(plugin_id) {
                        // Find the matching cap info
                        if let Some(cap_info) = plugin.caps.iter().find(|c| c.urn == cap_urn) {
                            // Use page_url if available, otherwise fall back to repo_url
                            let page_url = if plugin.page_url.is_empty() {
                                cache.repo_url.clone()
                            } else {
                                plugin.page_url.clone()
                            };
                            suggestions.push(PluginSuggestion {
                                plugin_id: plugin_id.clone(),
                                plugin_name: plugin.name.clone(),
                                plugin_description: plugin.description.clone(),
                                cap_urn: cap_urn.to_string(),
                                cap_title: cap_info.title.clone(),
                                latest_version: plugin.version.clone(),
                                repo_url: cache.repo_url.clone(),
                                page_url,
                            });
                        }
                    }
                }
            }
        }

        suggestions
    }

    /// Get all available plugins from all repos
    pub async fn get_all_plugins(&self) -> Vec<(String, PluginInfo)> {
        let caches = self.caches.read().await;
        let mut plugins = Vec::new();

        for cache in caches.values() {
            for (plugin_id, plugin_info) in &cache.plugins {
                plugins.push((plugin_id.clone(), plugin_info.clone()));
            }
        }

        plugins
    }

    /// Get all caps available from plugins (not necessarily installed)
    pub async fn get_all_available_caps(&self) -> Vec<String> {
        let caches = self.caches.read().await;
        let mut caps: Vec<String> = caches
            .values()
            .flat_map(|cache| cache.cap_to_plugins.keys().cloned())
            .collect();
        caps.sort();
        caps.dedup();
        caps
    }

    /// Check if any repo needs syncing (cache is stale or missing)
    pub async fn needs_sync(&self, repo_urls: &[String]) -> bool {
        let caches = self.caches.read().await;

        for repo_url in repo_urls {
            match caches.get(repo_url) {
                None => return true,
                Some(cache) if self.is_cache_stale(cache) => return true,
                _ => {}
            }
        }

        false
    }

    /// Get plugin info by ID
    pub async fn get_plugin(&self, plugin_id: &str) -> Option<PluginInfo> {
        let caches = self.caches.read().await;

        for cache in caches.values() {
            if let Some(plugin) = cache.plugins.get(plugin_id) {
                return Some(plugin.clone());
            }
        }

        None
    }

    /// Get suggestions for caps that could be provided by plugins but aren't currently available
    /// Takes a list of currently available cap URNs and returns suggestions for missing ones
    pub async fn get_suggestions_for_missing_caps(
        &self,
        available_caps: &[String],
        requested_caps: &[String],
    ) -> Vec<PluginSuggestion> {
        let available_set: std::collections::HashSet<&String> = available_caps.iter().collect();
        let mut suggestions = Vec::new();

        for cap_urn in requested_caps {
            if !available_set.contains(cap_urn) {
                let cap_suggestions = self.get_suggestions_for_cap(cap_urn).await;
                suggestions.extend(cap_suggestions);
            }
        }

        suggestions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_plugin_repo_creation() {
        let repo = PluginRepo::new(3600);
        assert!(repo.get_all_plugins().await.is_empty());
    }

    #[tokio::test]
    async fn test_needs_sync_empty_cache() {
        let repo = PluginRepo::new(3600);
        let urls = vec!["https://example.com/plugins".to_string()];
        assert!(repo.needs_sync(&urls).await);
    }

    #[test]
    fn test_deserialize_cap_summary_with_null_description() {
        let json = r#"{"urn": "media:text;llm;gen", "title": "Generate Text", "description": null}"#;
        let cap: PluginCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.urn, "media:text;llm;gen");
        assert_eq!(cap.title, "Generate Text");
        assert_eq!(cap.description, "");
    }

    #[test]
    fn test_deserialize_cap_summary_with_missing_description() {
        let json = r#"{"urn": "media:text;llm;gen", "title": "Generate Text"}"#;
        let cap: PluginCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.description, "");
    }

    #[test]
    fn test_deserialize_cap_summary_with_present_description() {
        let json = r#"{"urn": "media:text;llm;gen", "title": "Generate Text", "description": "A real description"}"#;
        let cap: PluginCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.description, "A real description");
    }

    #[test]
    fn test_deserialize_plugin_info_with_null_fields() {
        let json = r#"{
            "id": "mlxcartridge",
            "name": "MLX Cartridge",
            "version": null,
            "description": null,
            "author": null,
            "caps": [
                {"urn": "media:text;llm;gen", "title": "Generate Text", "description": null}
            ]
        }"#;
        let plugin: PluginInfo = serde_json::from_str(json).unwrap();
        assert_eq!(plugin.id, "mlxcartridge");
        assert_eq!(plugin.name, "MLX Cartridge");
        assert_eq!(plugin.version, "");
        assert_eq!(plugin.description, "");
        assert_eq!(plugin.author, "");
        assert_eq!(plugin.caps.len(), 1);
        assert_eq!(plugin.caps[0].description, "");
    }

    #[test]
    fn test_deserialize_registry_with_null_descriptions() {
        let json = r#"{
            "plugins": [{
                "id": "test-plugin",
                "name": "Test Plugin",
                "description": "A test plugin",
                "caps": [
                    {"urn": "media:text;llm;gen", "title": "Gen Text", "description": null},
                    {"urn": "media:image;vision", "title": "Vision", "description": "Analyze images"}
                ]
            }],
            "total": 1,
            "registryVersion": "3.0"
        }"#;
        let registry: PluginRegistryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(registry.plugins.len(), 1);
        assert_eq!(registry.plugins[0].caps[0].description, "");
        assert_eq!(registry.plugins[0].caps[1].description, "Analyze images");
    }
}
