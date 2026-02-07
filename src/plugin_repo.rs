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

/// The plugin registry response from the API (flat format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRegistryResponse {
    pub plugins: Vec<PluginInfo>,
}

/// A plugin version's distribution data (v3.0 schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginVersionData {
    pub release_date: String,
    #[serde(default)]
    pub changelog: Vec<String>,
    #[serde(default)]
    pub min_app_version: String,
    pub platform: String,
    pub package: PluginDistributionInfo,
    pub binary: PluginDistributionInfo,
}

/// Distribution file info (package or binary)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDistributionInfo {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

/// A plugin entry in the v3.0 registry (nested format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRegistryEntry {
    pub name: String,
    pub description: String,
    pub author: String,
    #[serde(default)]
    pub page_url: String,
    pub team_id: String,
    #[serde(default)]
    pub min_app_version: String,
    #[serde(default)]
    pub caps: Vec<PluginCapSummary>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub latest_version: String,
    pub versions: HashMap<String, PluginVersionData>,
}

/// The v3.0 plugin registry (nested schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRegistryV3 {
    pub schema_version: String,
    pub last_updated: String,
    pub plugins: HashMap<String, PluginRegistryEntry>,
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

impl PluginInfo {
    /// Check if plugin is signed (has team_id and signed_at)
    pub fn is_signed(&self) -> bool {
        !self.team_id.is_empty() && !self.signed_at.is_empty()
    }

    /// Check if binary download info is available
    pub fn has_binary(&self) -> bool {
        !self.binary_name.is_empty() && !self.binary_sha256.is_empty()
    }
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

/// Plugin repository server - serves registry data with queries
/// Transforms v3.0 nested registry schema to flat API response format
#[derive(Debug)]
pub struct PluginRepoServer {
    registry: PluginRegistryV3,
}

impl PluginRepoServer {
    /// Create a new server instance from v3.0 registry
    pub fn new(registry: PluginRegistryV3) -> Result<Self> {
        // Validate schema version - fail hard
        if registry.schema_version != "3.0" {
            return Err(PluginRepoError::ParseError(format!(
                "Unsupported registry schema version: {}. Required: 3.0",
                registry.schema_version
            )));
        }

        Ok(Self { registry })
    }

    /// Validate version data has all required fields
    fn validate_version_data(id: &str, version: &str, version_data: &PluginVersionData) -> Result<()> {
        if version_data.platform.is_empty() {
            return Err(PluginRepoError::ParseError(format!(
                "Plugin {} v{}: missing required field 'platform'",
                id, version
            )));
        }
        if version_data.package.name.is_empty() {
            return Err(PluginRepoError::ParseError(format!(
                "Plugin {} v{}: missing required field 'package.name'",
                id, version
            )));
        }
        if version_data.binary.name.is_empty() {
            return Err(PluginRepoError::ParseError(format!(
                "Plugin {} v{}: missing required field 'binary.name'",
                id, version
            )));
        }
        Ok(())
    }

    /// Compare semantic version strings
    fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
        let parts_a: Vec<u32> = a.split('.').filter_map(|p| p.parse().ok()).collect();
        let parts_b: Vec<u32> = b.split('.').filter_map(|p| p.parse().ok()).collect();

        let max_len = parts_a.len().max(parts_b.len());

        for i in 0..max_len {
            let num_a = parts_a.get(i).copied().unwrap_or(0);
            let num_b = parts_b.get(i).copied().unwrap_or(0);

            match num_a.cmp(&num_b) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }

        std::cmp::Ordering::Equal
    }

    /// Build changelog map from versions
    fn build_changelog_map(versions: &HashMap<String, PluginVersionData>) -> HashMap<String, Vec<String>> {
        let mut changelog = HashMap::new();
        for (version, data) in versions {
            if !data.changelog.is_empty() {
                changelog.insert(version.clone(), data.changelog.clone());
            }
        }
        changelog
    }

    /// Transform registry to flat plugin array
    pub fn transform_to_plugin_array(&self) -> Result<Vec<PluginInfo>> {
        let mut plugins = Vec::new();

        for (id, plugin) in &self.registry.plugins {
            let latest_version = &plugin.latest_version;
            let version_data = plugin.versions.get(latest_version)
                .ok_or_else(|| PluginRepoError::ParseError(format!(
                    "Plugin {}: latest version {} not found in versions",
                    id, latest_version
                )))?;

            // Validate required fields - fail hard
            Self::validate_version_data(id, latest_version, version_data)?;

            // Get all versions sorted descending
            let mut available_versions: Vec<String> = plugin.versions.keys().cloned().collect();
            available_versions.sort_by(|a, b| Self::compare_versions(b, a));

            // Build flat plugin object
            let package_url = format!("https://filegrind.com/plugins/packages/{}", version_data.package.name);
            plugins.push(PluginInfo {
                id: id.clone(),
                name: plugin.name.clone(),
                version: latest_version.clone(),
                description: plugin.description.clone(),
                author: plugin.author.clone(),
                homepage: String::new(),
                team_id: plugin.team_id.clone(),
                signed_at: version_data.release_date.clone(),
                min_app_version: if !version_data.min_app_version.is_empty() {
                    version_data.min_app_version.clone()
                } else {
                    plugin.min_app_version.clone()
                },
                page_url: if !plugin.page_url.is_empty() {
                    plugin.page_url.clone()
                } else {
                    package_url
                },
                categories: plugin.categories.clone(),
                tags: plugin.tags.clone(),
                caps: plugin.caps.clone(),
                // Distribution fields - ALL REQUIRED
                platform: version_data.platform.clone(),
                package_name: version_data.package.name.clone(),
                package_sha256: version_data.package.sha256.clone(),
                package_size: version_data.package.size,
                binary_name: version_data.binary.name.clone(),
                binary_sha256: version_data.binary.sha256.clone(),
                binary_size: version_data.binary.size,
                changelog: Self::build_changelog_map(&plugin.versions),
                available_versions,
            });
        }

        Ok(plugins)
    }

    /// Get all plugins (API response format)
    pub fn get_plugins(&self) -> Result<PluginRegistryResponse> {
        let plugins = self.transform_to_plugin_array()?;
        Ok(PluginRegistryResponse { plugins })
    }

    /// Get plugin by ID
    pub fn get_plugin_by_id(&self, id: &str) -> Result<Option<PluginInfo>> {
        let plugins = self.transform_to_plugin_array()?;
        Ok(plugins.into_iter().find(|p| p.id == id))
    }

    /// Search plugins by query
    pub fn search_plugins(&self, query: &str) -> Result<Vec<PluginInfo>> {
        let plugins = self.transform_to_plugin_array()?;
        let lower_query = query.to_lowercase();

        Ok(plugins.into_iter().filter(|p| {
            p.name.to_lowercase().contains(&lower_query)
                || p.description.to_lowercase().contains(&lower_query)
                || p.tags.iter().any(|t| t.to_lowercase().contains(&lower_query))
                || p.caps.iter().any(|c| {
                    c.urn.to_lowercase().contains(&lower_query)
                        || c.title.to_lowercase().contains(&lower_query)
                })
        }).collect())
    }

    /// Get plugins by category
    pub fn get_plugins_by_category(&self, category: &str) -> Result<Vec<PluginInfo>> {
        let plugins = self.transform_to_plugin_array()?;
        Ok(plugins.into_iter().filter(|p| p.categories.contains(&category.to_string())).collect())
    }

    /// Get plugins that provide a specific cap
    pub fn get_plugins_by_cap(&self, cap_urn: &str) -> Result<Vec<PluginInfo>> {
        let plugins = self.transform_to_plugin_array()?;
        Ok(plugins.into_iter().filter(|p| p.caps.iter().any(|c| c.urn == cap_urn)).collect())
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

    #[test]
    fn test_deserialize_full_plugin_with_signature() {
        let json = r#"{
            "id": "pdfcartridge",
            "name": "pdfcartridge",
            "version": "0.81.5325",
            "description": "PDF document processor",
            "author": "https://github.com/jowharshamshiri",
            "pageUrl": "https://github.com/filegrind/pdfcartridge",
            "teamId": "P336JK947M",
            "signedAt": "2026-02-07T16:40:28Z",
            "minAppVersion": "1.0.0",
            "caps": [
                {
                    "urn": "cap:in=\"media:pdf;bytes\";op=disbind;out=\"media:disbound-page;textable;form=list\"",
                    "title": "Disbind PDF",
                    "description": "Extract pages from PDF"
                }
            ],
            "categories": [],
            "tags": [],
            "changelog": {},
            "platform": "darwin-arm64",
            "packageName": "pdfcartridge-0.81.5325.pkg",
            "packageSha256": "9b68724eb9220ecf01e8ed4f5f80c594fbac2239bc5bf675005ec882ecc5eba0",
            "packageSize": 5187485,
            "binaryName": "pdfcartridge-0.81.5325-darwin-arm64",
            "binarySha256": "908187ec35632758f1a00452ff4755ba01020ea288619098b6998d5d33851d19",
            "binarySize": 12980288,
            "availableVersions": ["0.81.5325"]
        }"#;

        let plugin: PluginInfo = serde_json::from_str(json).unwrap();
        assert_eq!(plugin.id, "pdfcartridge");
        assert_eq!(plugin.team_id, "P336JK947M");
        assert_eq!(plugin.signed_at, "2026-02-07T16:40:28Z");
        assert_eq!(plugin.binary_name, "pdfcartridge-0.81.5325-darwin-arm64");
        assert_eq!(plugin.binary_sha256, "908187ec35632758f1a00452ff4755ba01020ea288619098b6998d5d33851d19");
        assert_eq!(plugin.binary_size, 12980288);
        assert!(!plugin.team_id.is_empty(), "Plugin must have team_id for signature verification");
        assert!(!plugin.signed_at.is_empty(), "Plugin must have signed_at timestamp");
        assert!(!plugin.binary_sha256.is_empty(), "Plugin must have SHA256 hash");
    }

    // TEST320-335: PluginRepoServer and PluginRepoClient tests

    #[test]
    fn test320_plugin_info_construction() {
        // TEST320: Construct PluginInfo and verify fields
        let plugin = PluginInfo {
            id: "testplugin".to_string(),
            name: "Test Plugin".to_string(),
            version: "1.0.0".to_string(),
            description: "A test plugin".to_string(),
            author: "Test Author".to_string(),
            homepage: "https://example.com".to_string(),
            team_id: "TEAM123".to_string(),
            signed_at: "2026-02-07T00:00:00Z".to_string(),
            min_app_version: "1.0.0".to_string(),
            page_url: "https://example.com/plugin".to_string(),
            categories: vec!["test".to_string()],
            tags: vec!["testing".to_string()],
            caps: vec![],
            platform: "darwin-arm64".to_string(),
            package_name: "test-1.0.0.pkg".to_string(),
            package_sha256: "abc123".to_string(),
            package_size: 1000,
            binary_name: "test-1.0.0-darwin-arm64".to_string(),
            binary_sha256: "def456".to_string(),
            binary_size: 2000,
            changelog: HashMap::new(),
            available_versions: vec!["1.0.0".to_string()],
        };

        assert_eq!(plugin.id, "testplugin");
        assert_eq!(plugin.name, "Test Plugin");
        assert_eq!(plugin.version, "1.0.0");
    }

    #[test]
    fn test321_plugin_info_is_signed() {
        // TEST321: Verify is_signed() method
        let mut plugin = PluginInfo {
            id: "testplugin".to_string(),
            name: "Test".to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            homepage: String::new(),
            team_id: "TEAM123".to_string(),
            signed_at: "2026-02-07T00:00:00Z".to_string(),
            min_app_version: String::new(),
            page_url: String::new(),
            categories: vec![],
            tags: vec![],
            caps: vec![],
            platform: String::new(),
            package_name: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            binary_name: String::new(),
            binary_sha256: String::new(),
            binary_size: 0,
            changelog: HashMap::new(),
            available_versions: vec![],
        };

        assert!(plugin.is_signed());

        plugin.team_id = String::new();
        assert!(!plugin.is_signed());

        plugin.team_id = "TEAM123".to_string();
        plugin.signed_at = String::new();
        assert!(!plugin.is_signed());
    }

    #[test]
    fn test322_plugin_info_has_binary() {
        // TEST322: Verify has_binary() method
        let mut plugin = PluginInfo {
            id: "testplugin".to_string(),
            name: "Test".to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            homepage: String::new(),
            team_id: String::new(),
            signed_at: String::new(),
            min_app_version: String::new(),
            page_url: String::new(),
            categories: vec![],
            tags: vec![],
            caps: vec![],
            platform: String::new(),
            package_name: String::new(),
            package_sha256: String::new(),
            package_size: 0,
            binary_name: "test-1.0.0".to_string(),
            binary_sha256: "abc123".to_string(),
            binary_size: 0,
            changelog: HashMap::new(),
            available_versions: vec![],
        };

        assert!(plugin.has_binary());

        plugin.binary_name = String::new();
        assert!(!plugin.has_binary());

        plugin.binary_name = "test-1.0.0".to_string();
        plugin.binary_sha256 = String::new();
        assert!(!plugin.has_binary());
    }

    #[test]
    fn test323_plugin_repo_server_validate_registry() {
        // TEST323: Validate registry schema version
        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins: HashMap::new(),
        };

        let server = PluginRepoServer::new(registry);
        assert!(server.is_ok());

        // Test v2.0 schema rejection
        let old_registry = PluginRegistryV3 {
            schema_version: "2.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins: HashMap::new(),
        };

        let result = PluginRepoServer::new(old_registry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("3.0"));
    }

    #[test]
    fn test324_plugin_repo_server_transform_to_array() {
        // TEST324: Transform v3 registry to flat plugin array
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec!["Initial release".to_string()],
            min_app_version: "1.0.0".to_string(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        plugins.insert("testplugin".to_string(), PluginRegistryEntry {
            name: "Test Plugin".to_string(),
            description: "A test plugin".to_string(),
            author: "Test Author".to_string(),
            page_url: "https://example.com".to_string(),
            team_id: "TEAM123".to_string(),
            min_app_version: "1.0.0".to_string(),
            caps: vec![],
            categories: vec!["test".to_string()],
            tags: vec!["testing".to_string()],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let result = server.transform_to_plugin_array();
        assert!(result.is_ok());

        let plugins_array = result.unwrap();
        assert_eq!(plugins_array.len(), 1);
        assert_eq!(plugins_array[0].id, "testplugin");
        assert_eq!(plugins_array[0].name, "Test Plugin");
        assert_eq!(plugins_array[0].version, "1.0.0");
        assert_eq!(plugins_array[0].binary_name, "test-1.0.0-darwin-arm64");
    }

    #[test]
    fn test325_plugin_repo_server_get_plugins() {
        // TEST325: Get all plugins via get_plugins()
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        plugins.insert("testplugin".to_string(), PluginRegistryEntry {
            name: "Test Plugin".to_string(),
            description: "A test plugin".to_string(),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![],
            categories: vec![],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let response = server.get_plugins().unwrap();
        assert_eq!(response.plugins.len(), 1);
        assert_eq!(response.plugins[0].id, "testplugin");
    }

    #[test]
    fn test326_plugin_repo_server_get_plugin_by_id() {
        // TEST326: Get plugin by ID
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        plugins.insert("testplugin".to_string(), PluginRegistryEntry {
            name: "Test Plugin".to_string(),
            description: "A test plugin".to_string(),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![],
            categories: vec![],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let result = server.get_plugin_by_id("testplugin").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, "testplugin");

        let not_found = server.get_plugin_by_id("nonexistent").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test327_plugin_repo_server_search_plugins() {
        // TEST327: Search plugins by text query
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "pdf-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "pdf-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        plugins.insert("pdfplugin".to_string(), PluginRegistryEntry {
            name: "PDF Plugin".to_string(),
            description: "Process PDF documents".to_string(),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![],
            categories: vec![],
            tags: vec!["document".to_string()],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let results = server.search_plugins("pdf").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "pdfplugin");

        let no_match = server.search_plugins("nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test328_plugin_repo_server_get_by_category() {
        // TEST328: Filter plugins by category
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        plugins.insert("docplugin".to_string(), PluginRegistryEntry {
            name: "Doc Plugin".to_string(),
            description: "Process documents".to_string(),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![],
            categories: vec!["document".to_string()],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let results = server.get_plugins_by_category("document").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "docplugin");

        let no_match = server.get_plugins_by_category("nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test329_plugin_repo_server_get_by_cap() {
        // TEST329: Find plugins by cap URN
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        let cap_urn = r#"cap:in="media:pdf;bytes";op=disbind;out="media:disbound-page;textable;form=list""#;
        plugins.insert("pdfplugin".to_string(), PluginRegistryEntry {
            name: "PDF Plugin".to_string(),
            description: "Process PDFs".to_string(),
            author: "Test Author".to_string(),
            page_url: String::new(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![PluginCapSummary {
                urn: cap_urn.to_string(),
                title: "Disbind PDF".to_string(),
                description: "Extract pages".to_string(),
            }],
            categories: vec![],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        let server = PluginRepoServer::new(registry).unwrap();
        let results = server.get_plugins_by_cap(cap_urn).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "pdfplugin");

        let no_match = server.get_plugins_by_cap("cap:nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[tokio::test]
    async fn test330_plugin_repo_client_update_cache() {
        // TEST330: PluginRepoClient cache update
        let repo = PluginRepo::new(3600);

        // Create a mock registry response
        let registry = PluginRegistryResponse {
            plugins: vec![
                PluginInfo {
                    id: "testplugin".to_string(),
                    name: "Test Plugin".to_string(),
                    version: "1.0.0".to_string(),
                    description: String::new(),
                    author: String::new(),
                    homepage: String::new(),
                    team_id: "TEAM123".to_string(),
                    signed_at: "2026-02-07".to_string(),
                    min_app_version: String::new(),
                    page_url: String::new(),
                    categories: vec![],
                    tags: vec![],
                    caps: vec![],
                    platform: String::new(),
                    package_name: String::new(),
                    package_sha256: String::new(),
                    package_size: 0,
                    binary_name: "test-binary".to_string(),
                    binary_sha256: "abc123".to_string(),
                    binary_size: 0,
                    changelog: HashMap::new(),
                    available_versions: vec![],
                }
            ],
        };

        // Update cache directly (simulating a fetch)
        let mut caches = repo.caches.write().await;
        PluginRepo::update_cache(&mut caches, "https://example.com/plugins", registry);
        drop(caches);

        // Verify cache was updated
        let plugin = repo.get_plugin("testplugin").await;
        assert!(plugin.is_some());
        assert_eq!(plugin.unwrap().id, "testplugin");
    }

    #[tokio::test]
    async fn test331_plugin_repo_client_get_suggestions() {
        // TEST331: Get suggestions for missing cap
        let repo = PluginRepo::new(3600);

        let cap_urn = r#"cap:in="media:pdf;bytes";op=disbind;out="media:disbound-page;textable;form=list""#;
        let registry = PluginRegistryResponse {
            plugins: vec![
                PluginInfo {
                    id: "pdfplugin".to_string(),
                    name: "PDF Plugin".to_string(),
                    version: "1.0.0".to_string(),
                    description: "Process PDFs".to_string(),
                    author: String::new(),
                    homepage: String::new(),
                    team_id: "TEAM123".to_string(),
                    signed_at: "2026-02-07".to_string(),
                    min_app_version: String::new(),
                    page_url: "https://example.com/pdf".to_string(),
                    categories: vec![],
                    tags: vec![],
                    caps: vec![PluginCapSummary {
                        urn: cap_urn.to_string(),
                        title: "Disbind PDF".to_string(),
                        description: "Extract pages".to_string(),
                    }],
                    platform: String::new(),
                    package_name: String::new(),
                    package_sha256: String::new(),
                    package_size: 0,
                    binary_name: String::new(),
                    binary_sha256: String::new(),
                    binary_size: 0,
                    changelog: HashMap::new(),
                    available_versions: vec![],
                }
            ],
        };

        let mut caches = repo.caches.write().await;
        PluginRepo::update_cache(&mut caches, "https://example.com/plugins", registry);
        drop(caches);

        let suggestions = repo.get_suggestions_for_cap(cap_urn).await;
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].plugin_id, "pdfplugin");
        assert_eq!(suggestions[0].cap_urn, cap_urn);
    }

    #[tokio::test]
    async fn test332_plugin_repo_client_get_plugin() {
        // TEST332: Get plugin by ID from client
        let repo = PluginRepo::new(3600);

        let registry = PluginRegistryResponse {
            plugins: vec![
                PluginInfo {
                    id: "testplugin".to_string(),
                    name: "Test Plugin".to_string(),
                    version: "1.0.0".to_string(),
                    description: String::new(),
                    author: String::new(),
                    homepage: String::new(),
                    team_id: String::new(),
                    signed_at: String::new(),
                    min_app_version: String::new(),
                    page_url: String::new(),
                    categories: vec![],
                    tags: vec![],
                    caps: vec![],
                    platform: String::new(),
                    package_name: String::new(),
                    package_sha256: String::new(),
                    package_size: 0,
                    binary_name: String::new(),
                    binary_sha256: String::new(),
                    binary_size: 0,
                    changelog: HashMap::new(),
                    available_versions: vec![],
                }
            ],
        };

        let mut caches = repo.caches.write().await;
        PluginRepo::update_cache(&mut caches, "https://example.com/plugins", registry);
        drop(caches);

        let plugin = repo.get_plugin("testplugin").await;
        assert!(plugin.is_some());
        assert_eq!(plugin.unwrap().id, "testplugin");

        let not_found = repo.get_plugin("nonexistent").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test333_plugin_repo_client_get_all_caps() {
        // TEST333: Get all available caps
        let repo = PluginRepo::new(3600);

        let cap1 = "cap:in=\"media:pdf;bytes\";op=disbind;out=\"media:disbound-page;textable;form=list\"";
        let cap2 = "cap:in=\"media:txt;textable\";op=disbind;out=\"media:disbound-page;textable;form=list\"";

        let registry = PluginRegistryResponse {
            plugins: vec![
                PluginInfo {
                    id: "plugin1".to_string(),
                    name: "Plugin 1".to_string(),
                    version: "1.0.0".to_string(),
                    description: String::new(),
                    author: String::new(),
                    homepage: String::new(),
                    team_id: String::new(),
                    signed_at: String::new(),
                    min_app_version: String::new(),
                    page_url: String::new(),
                    categories: vec![],
                    tags: vec![],
                    caps: vec![PluginCapSummary {
                        urn: cap1.to_string(),
                        title: "Cap 1".to_string(),
                        description: String::new(),
                    }],
                    platform: String::new(),
                    package_name: String::new(),
                    package_sha256: String::new(),
                    package_size: 0,
                    binary_name: String::new(),
                    binary_sha256: String::new(),
                    binary_size: 0,
                    changelog: HashMap::new(),
                    available_versions: vec![],
                },
                PluginInfo {
                    id: "plugin2".to_string(),
                    name: "Plugin 2".to_string(),
                    version: "1.0.0".to_string(),
                    description: String::new(),
                    author: String::new(),
                    homepage: String::new(),
                    team_id: String::new(),
                    signed_at: String::new(),
                    min_app_version: String::new(),
                    page_url: String::new(),
                    categories: vec![],
                    tags: vec![],
                    caps: vec![PluginCapSummary {
                        urn: cap2.to_string(),
                        title: "Cap 2".to_string(),
                        description: String::new(),
                    }],
                    platform: String::new(),
                    package_name: String::new(),
                    package_sha256: String::new(),
                    package_size: 0,
                    binary_name: String::new(),
                    binary_sha256: String::new(),
                    binary_size: 0,
                    changelog: HashMap::new(),
                    available_versions: vec![],
                }
            ],
        };

        let mut caches = repo.caches.write().await;
        PluginRepo::update_cache(&mut caches, "https://example.com/plugins", registry);
        drop(caches);

        let caps = repo.get_all_available_caps().await;
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&cap1.to_string()));
        assert!(caps.contains(&cap2.to_string()));
    }

    #[tokio::test]
    async fn test334_plugin_repo_client_needs_sync() {
        // TEST334: Check if client needs sync
        let repo = PluginRepo::new(3600);

        let urls = vec!["https://example.com/plugins".to_string()];

        // Empty cache should need sync
        assert!(repo.needs_sync(&urls).await);

        // After update, should not need sync
        let registry = PluginRegistryResponse { plugins: vec![] };
        let mut caches = repo.caches.write().await;
        PluginRepo::update_cache(&mut caches, "https://example.com/plugins", registry);
        drop(caches);

        assert!(!repo.needs_sync(&urls).await);
    }

    #[test]
    fn test335_plugin_repo_server_client_integration() {
        // TEST335: Server creates response, client consumes it
        let mut plugins = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert("1.0.0".to_string(), PluginVersionData {
            release_date: "2026-02-07".to_string(),
            changelog: vec![],
            min_app_version: String::new(),
            platform: "darwin-arm64".to_string(),
            package: PluginDistributionInfo {
                name: "test-1.0.0.pkg".to_string(),
                sha256: "abc123".to_string(),
                size: 1000,
            },
            binary: PluginDistributionInfo {
                name: "test-1.0.0-darwin-arm64".to_string(),
                sha256: "def456".to_string(),
                size: 2000,
            },
        });

        let cap_urn = "cap:in=\"media:test;bytes\";op=test;out=\"media:result;bytes\"";
        plugins.insert("testplugin".to_string(), PluginRegistryEntry {
            name: "Test Plugin".to_string(),
            description: "A test plugin".to_string(),
            author: "Test Author".to_string(),
            page_url: "https://example.com".to_string(),
            team_id: "TEAM123".to_string(),
            min_app_version: String::new(),
            caps: vec![PluginCapSummary {
                urn: cap_urn.to_string(),
                title: "Test Cap".to_string(),
                description: "Test capability".to_string(),
            }],
            categories: vec!["test".to_string()],
            tags: vec![],
            latest_version: "1.0.0".to_string(),
            versions,
        });

        let registry = PluginRegistryV3 {
            schema_version: "3.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            plugins,
        };

        // Server transforms registry
        let server = PluginRepoServer::new(registry).unwrap();
        let response = server.get_plugins().unwrap();

        // Verify response structure
        assert_eq!(response.plugins.len(), 1);
        let plugin = &response.plugins[0];
        assert_eq!(plugin.id, "testplugin");
        assert_eq!(plugin.name, "Test Plugin");
        assert!(plugin.is_signed());
        assert!(plugin.has_binary());
        assert_eq!(plugin.caps.len(), 1);
        assert_eq!(plugin.caps[0].urn, cap_urn);

        // Simulate client consuming this response
        // (Client would deserialize the JSON and cache it)
        assert_eq!(plugin.binary_name, "test-1.0.0-darwin-arm64");
        assert_eq!(plugin.binary_sha256, "def456");
    }
}
