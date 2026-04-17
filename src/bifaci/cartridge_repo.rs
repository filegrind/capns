//! Cartridge Repository
//!
//! Fetches and caches cartridge registry data from configured cartridge repositories.
//! Provides cartridge suggestions when a cap isn't available but a cartridge exists that could provide it.

use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::RwLock;

/// Cartridge repository errors
#[derive(Debug, Error)]
pub enum CartridgeRepoError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    #[error("Failed to parse registry response: {0}")]
    ParseError(String),
    #[error("Registry request failed with status {0}")]
    StatusError(u16),
    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),
}

pub type Result<T> = std::result::Result<T, CartridgeRepoError>;

/// Deserialize a possibly-null string as an empty string.
/// Handles API responses where string fields may be `null` instead of absent.
fn null_as_empty_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    Option::<String>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// A cartridge's capability summary from the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeCapSummary {
    pub urn: String,
    pub title: String,
    #[serde(default, deserialize_with = "null_as_empty_string")]
    pub description: String,
}

/// A cartridge version's package info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgePackageInfo {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

/// A cartridge entry from the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeInfo {
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
    pub caps: Vec<CartridgeCapSummary>,
    /// All versions with their builds (platform-specific packages).
    pub versions: HashMap<String, CartridgeVersionData>,
    /// All available versions (newest first)
    #[serde(default)]
    pub available_versions: Vec<String>,
}

/// The cartridge registry response from the API (flat format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistryResponse {
    pub cartridges: Vec<CartridgeInfo>,
}

/// A platform-specific build within a version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeBuild {
    pub platform: String,
    pub package: CartridgeDistributionInfo,
}

/// A cartridge version's data (v4.0 schema).
/// Each version has one or more platform-specific builds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeVersionData {
    pub release_date: String,
    #[serde(default)]
    pub changelog: Vec<String>,
    #[serde(default)]
    pub min_app_version: String,
    pub builds: Vec<CartridgeBuild>,
}

/// Distribution file info (package)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeDistributionInfo {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

/// A cartridge entry in the v4.0 registry (nested format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistryEntry {
    pub name: String,
    pub description: String,
    pub author: String,
    #[serde(default)]
    pub page_url: String,
    pub team_id: String,
    #[serde(default)]
    pub min_app_version: String,
    #[serde(default)]
    pub caps: Vec<CartridgeCapSummary>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub latest_version: String,
    pub versions: HashMap<String, CartridgeVersionData>,
}

/// The v4.0 cartridge registry (nested schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRegistry {
    pub schema_version: String,
    pub last_updated: String,
    pub cartridges: HashMap<String, CartridgeRegistryEntry>,
}

/// A cartridge suggestion for a missing cap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartridgeSuggestion {
    pub cartridge_id: String,
    pub cartridge_name: String,
    pub cartridge_description: String,
    pub cap_urn: String,
    pub cap_title: String,
    pub latest_version: String,
    pub repo_url: String,
    pub page_url: String,
}

/// Cached cartridge repository data
struct CartridgeRepoCache {
    /// All cartridges indexed by cartridge ID
    cartridges: HashMap<String, CartridgeInfo>,
    /// Cap URN to cartridge IDs that provide it
    cap_to_cartridges: HashMap<String, Vec<String>>,
    /// When the cache was last updated
    last_updated: Instant,
    /// The repo URL this cache is from
    repo_url: String,
}

/// Service for fetching and caching cartridge repository data
pub struct CartridgeRepo {
    http_client: Client,
    /// Cache per repo URL
    caches: Arc<RwLock<HashMap<String, CartridgeRepoCache>>>,
    /// Cache TTL in seconds
    cache_ttl: Duration,
    offline_flag: Arc<AtomicBool>,
}

impl CartridgeInfo {
    /// Check if cartridge is signed (has team_id and signed_at)
    pub fn is_signed(&self) -> bool {
        !self.team_id.is_empty() && !self.signed_at.is_empty()
    }

    /// Get the build for a specific platform from the latest version.
    pub fn build_for_platform(&self, platform: &str) -> Option<&CartridgeBuild> {
        self.versions
            .get(&self.version)
            .and_then(|v| v.builds.iter().find(|b| b.platform == platform))
    }

    /// Get all platforms available across all versions.
    pub fn available_platforms(&self) -> Vec<String> {
        let mut platforms: Vec<String> = self
            .versions
            .values()
            .flat_map(|v| v.builds.iter().map(|b| b.platform.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        platforms.sort();
        platforms
    }
}

impl CartridgeRepo {
    /// Create a new cartridge repo service
    pub fn new(cache_ttl_seconds: u64) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("MachineFabricEngine/1.0.0")
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            caches: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(cache_ttl_seconds),
            offline_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the offline flag. When true, all registry fetches are blocked.
    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    /// Fetch cartridge registry from a URL
    async fn fetch_registry(&self, repo_url: &str) -> Result<CartridgeRegistryResponse> {
        if self.offline_flag.load(Ordering::Relaxed) {
            return Err(CartridgeRepoError::NetworkBlocked(format!(
                "Network access blocked by policy — cannot fetch cartridge registry '{}'",
                repo_url
            )));
        }
        let response = self.http_client.get(repo_url).send().await.map_err(|e| {
            CartridgeRepoError::HttpError(format!("Failed to fetch from {}: {}", repo_url, e))
        })?;

        if !response.status().is_success() {
            return Err(CartridgeRepoError::StatusError(response.status().as_u16()));
        }

        let registry: CartridgeRegistryResponse = response.json().await.map_err(|e| {
            CartridgeRepoError::ParseError(format!("Failed to parse from {}: {}", repo_url, e))
        })?;

        Ok(registry)
    }

    /// Update cache from a registry response
    fn update_cache(
        caches: &mut HashMap<String, CartridgeRepoCache>,
        repo_url: &str,
        registry: CartridgeRegistryResponse,
    ) {
        let mut cartridges: HashMap<String, CartridgeInfo> = HashMap::new();
        let mut cap_to_cartridges: HashMap<String, Vec<String>> = HashMap::new();

        for cartridge_info in registry.cartridges {
            let cartridge_id = cartridge_info.id.clone();
            for cap in &cartridge_info.caps {
                cap_to_cartridges
                    .entry(cap.urn.clone())
                    .or_default()
                    .push(cartridge_id.clone());
            }
            cartridges.insert(cartridge_id, cartridge_info);
        }

        caches.insert(
            repo_url.to_string(),
            CartridgeRepoCache {
                cartridges,
                cap_to_cartridges,
                last_updated: Instant::now(),
                repo_url: repo_url.to_string(),
            },
        );
    }

    /// Sync cartridge data from the given repository URLs
    pub async fn sync_repos(&self, repo_urls: &[String]) {
        for repo_url in repo_urls {
            match self.fetch_registry(repo_url).await {
                Ok(registry) => {
                    let mut caches = self.caches.write().await;
                    Self::update_cache(&mut caches, repo_url, registry);
                }
                Err(e) => {
                    tracing::error!("Failed to sync cartridge repo {}: {}", repo_url, e);
                    // Continue with other repos
                }
            }
        }
    }

    /// Check if a cache is stale
    fn is_cache_stale(&self, cache: &CartridgeRepoCache) -> bool {
        cache.last_updated.elapsed() > self.cache_ttl
    }

    /// Get cartridge suggestions for a cap URN that isn't available
    pub async fn get_suggestions_for_cap(&self, cap_urn: &str) -> Vec<CartridgeSuggestion> {
        let caches = self.caches.read().await;
        let mut suggestions = Vec::new();

        for cache in caches.values() {
            if let Some(cartridge_ids) = cache.cap_to_cartridges.get(cap_urn) {
                for cartridge_id in cartridge_ids {
                    if let Some(cartridge) = cache.cartridges.get(cartridge_id) {
                        // Find the matching cap info
                        if let Some(cap_info) = cartridge.caps.iter().find(|c| c.urn == cap_urn) {
                            // Use page_url if available, otherwise fall back to repo_url
                            let page_url = if cartridge.page_url.is_empty() {
                                cache.repo_url.clone()
                            } else {
                                cartridge.page_url.clone()
                            };
                            suggestions.push(CartridgeSuggestion {
                                cartridge_id: cartridge_id.clone(),
                                cartridge_name: cartridge.name.clone(),
                                cartridge_description: cartridge.description.clone(),
                                cap_urn: cap_urn.to_string(),
                                cap_title: cap_info.title.clone(),
                                latest_version: cartridge.version.clone(),
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

    /// Get all available cartridges from all repos
    pub async fn get_all_cartridges(&self) -> Vec<(String, CartridgeInfo)> {
        let caches = self.caches.read().await;
        let mut all_cartridges = Vec::new();

        for cache in caches.values() {
            for (cartridge_id, cartridge_info) in &cache.cartridges {
                all_cartridges.push((cartridge_id.clone(), cartridge_info.clone()));
            }
        }

        all_cartridges
    }

    /// Get all caps available from cartridges (not necessarily installed)
    pub async fn get_all_available_caps(&self) -> Vec<String> {
        let caches = self.caches.read().await;
        let mut caps: Vec<String> = caches
            .values()
            .flat_map(|cache| cache.cap_to_cartridges.keys().cloned())
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

    /// Get cartridge info by ID
    pub async fn get_cartridge(&self, cartridge_id: &str) -> Option<CartridgeInfo> {
        let caches = self.caches.read().await;

        for cache in caches.values() {
            if let Some(cartridge) = cache.cartridges.get(cartridge_id) {
                return Some(cartridge.clone());
            }
        }

        None
    }

    /// Get suggestions for caps that could be provided by cartridges but aren't currently available
    /// Takes a list of currently available cap URNs and returns suggestions for missing ones
    pub async fn get_suggestions_for_missing_caps(
        &self,
        available_caps: &[String],
        requested_caps: &[String],
    ) -> Vec<CartridgeSuggestion> {
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

/// Cartridge repository server - serves registry data with queries
/// Transforms v3.0 nested registry schema to flat API response format
#[derive(Debug)]
pub struct CartridgeRepoServer {
    registry: CartridgeRegistry,
}

impl CartridgeRepoServer {
    /// Create a new server instance from v3.0 registry
    pub fn new(registry: CartridgeRegistry) -> Result<Self> {
        // Validate schema version - fail hard
        if registry.schema_version != "4.0" {
            return Err(CartridgeRepoError::ParseError(format!(
                "Unsupported registry schema version: {}. Required: 4.0",
                registry.schema_version
            )));
        }

        Ok(Self { registry })
    }

    /// Validate version data has all required fields
    fn validate_version_data(
        id: &str,
        version: &str,
        version_data: &CartridgeVersionData,
    ) -> Result<()> {
        if version_data.builds.is_empty() {
            return Err(CartridgeRepoError::ParseError(format!(
                "Cartridge {} v{}: no builds",
                id, version
            )));
        }
        for (i, build) in version_data.builds.iter().enumerate() {
            if build.platform.is_empty() {
                return Err(CartridgeRepoError::ParseError(format!(
                    "Cartridge {} v{}: build[{}] missing platform",
                    id, version, i
                )));
            }
            if build.package.name.is_empty() {
                return Err(CartridgeRepoError::ParseError(format!(
                    "Cartridge {} v{}: build[{}] ({}) missing package.name",
                    id, version, i, build.platform
                )));
            }
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
    fn build_changelog_map(
        versions: &HashMap<String, CartridgeVersionData>,
    ) -> HashMap<String, Vec<String>> {
        let mut changelog = HashMap::new();
        for (version, data) in versions {
            if !data.changelog.is_empty() {
                changelog.insert(version.clone(), data.changelog.clone());
            }
        }
        changelog
    }

    /// Transform registry to flat cartridge array
    pub fn transform_to_cartridge_array(&self) -> Result<Vec<CartridgeInfo>> {
        let mut result = Vec::new();

        for (id, entry) in &self.registry.cartridges {
            let latest_version = &entry.latest_version;
            let version_data = entry.versions.get(latest_version).ok_or_else(|| {
                CartridgeRepoError::ParseError(format!(
                    "Cartridge {}: latest version {} not found in versions",
                    id, latest_version
                ))
            })?;

            // Validate required fields - fail hard
            Self::validate_version_data(id, latest_version, version_data)?;

            // Get all versions sorted descending
            let mut available_versions: Vec<String> = entry.versions.keys().cloned().collect();
            available_versions.sort_by(|a, b| Self::compare_versions(b, a));

            result.push(CartridgeInfo {
                id: id.clone(),
                name: entry.name.clone(),
                version: latest_version.clone(),
                description: entry.description.clone(),
                author: entry.author.clone(),
                homepage: String::new(),
                team_id: entry.team_id.clone(),
                signed_at: version_data.release_date.clone(),
                min_app_version: if !version_data.min_app_version.is_empty() {
                    version_data.min_app_version.clone()
                } else {
                    entry.min_app_version.clone()
                },
                page_url: entry.page_url.clone(),
                categories: entry.categories.clone(),
                tags: entry.tags.clone(),
                caps: entry.caps.clone(),
                versions: entry.versions.clone(),
                available_versions,
            });
        }

        Ok(result)
    }

    /// Get all cartridges (API response format)
    pub fn get_cartridges(&self) -> Result<CartridgeRegistryResponse> {
        let cartridges = self.transform_to_cartridge_array()?;
        Ok(CartridgeRegistryResponse { cartridges })
    }

    /// Get cartridge by ID
    pub fn get_cartridge_by_id(&self, id: &str) -> Result<Option<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        Ok(all.into_iter().find(|p| p.id == id))
    }

    /// Search cartridges by query
    pub fn search_cartridges(&self, query: &str) -> Result<Vec<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        let lower_query = query.to_lowercase();

        Ok(all
            .into_iter()
            .filter(|p| {
                p.name.to_lowercase().contains(&lower_query)
                    || p.description.to_lowercase().contains(&lower_query)
                    || p.tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&lower_query))
                    || p.caps.iter().any(|c| {
                        c.urn.to_lowercase().contains(&lower_query)
                            || c.title.to_lowercase().contains(&lower_query)
                    })
            })
            .collect())
    }

    /// Get cartridges by category
    pub fn get_cartridges_by_category(&self, category: &str) -> Result<Vec<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        Ok(all
            .into_iter()
            .filter(|p| p.categories.contains(&category.to_string()))
            .collect())
    }

    /// Get cartridges that provide a specific cap
    pub fn get_cartridges_by_cap(&self, cap_urn: &str) -> Result<Vec<CartridgeInfo>> {
        let all = self.transform_to_cartridge_array()?;
        Ok(all
            .into_iter()
            .filter(|p| p.caps.iter().any(|c| c.urn == cap_urn))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEST630: Verify CartridgeRepo creation starts with empty cartridge list
    #[tokio::test]
    async fn test630_cartridge_repo_creation() {
        let repo = CartridgeRepo::new(3600);
        assert!(repo.get_all_cartridges().await.is_empty());
    }

    // TEST631: Verify needs_sync returns true with empty cache and non-empty URLs
    #[tokio::test]
    async fn test631_needs_sync_empty_cache() {
        let repo = CartridgeRepo::new(3600);
        let urls = vec!["https://example.com/cartridges".to_string()];
        assert!(repo.needs_sync(&urls).await);
    }

    // TEST632: Verify CartridgeCapSummary deserializes null description as empty string
    #[test]
    fn test632_deserialize_cap_summary_with_null_description() {
        let json =
            r#"{"urn": "media:text;llm;gen", "title": "Generate Text", "description": null}"#;
        let cap: CartridgeCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.urn, "media:text;llm;gen");
        assert_eq!(cap.title, "Generate Text");
        assert_eq!(cap.description, "");
    }

    // TEST633: Verify CartridgeCapSummary deserializes missing description as empty string
    #[test]
    fn test633_deserialize_cap_summary_with_missing_description() {
        let json = r#"{"urn": "media:text;llm;gen", "title": "Generate Text"}"#;
        let cap: CartridgeCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.description, "");
    }

    // TEST634: Verify CartridgeCapSummary deserializes present description correctly
    #[test]
    fn test634_deserialize_cap_summary_with_present_description() {
        let json = r#"{"urn": "media:text;llm;gen", "title": "Generate Text", "description": "A real description"}"#;
        let cap: CartridgeCapSummary = serde_json::from_str(json).unwrap();
        assert_eq!(cap.description, "A real description");
    }

    // TEST635: Verify CartridgeInfo deserializes null version/description/author as empty strings
    #[test]
    fn test635_deserialize_cartridge_info_with_null_fields() {
        let json = r#"{
            "id": "mlxcartridge",
            "name": "MLX Cartridge",
            "version": null,
            "description": null,
            "author": null,
            "caps": [
                {"urn": "media:text;llm;gen", "title": "Generate Text", "description": null}
            ],
            "versions": {}
        }"#;
        let cartridge: CartridgeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(cartridge.id, "mlxcartridge");
        assert_eq!(cartridge.name, "MLX Cartridge");
        assert_eq!(cartridge.version, "");
        assert_eq!(cartridge.description, "");
        assert_eq!(cartridge.author, "");
        assert_eq!(cartridge.caps.len(), 1);
        assert_eq!(cartridge.caps[0].description, "");
    }

    // TEST636: Verify CartridgeRegistryResponse deserializes with mixed null/present descriptions
    #[test]
    fn test636_deserialize_registry_with_null_descriptions() {
        let json = r#"{
            "cartridges": [{
                "id": "test-cartridge",
                "name": "Test Cartridge",
                "description": "A test cartridge",
                "caps": [
                    {"urn": "media:text;llm;gen", "title": "Gen Text", "description": null},
                    {"urn": "media:image;vision", "title": "Vision", "description": "Analyze images"}
                ],
                "versions": {}
            }],
            "total": 1,
            "registryVersion": "4.0"
        }"#;
        let registry: CartridgeRegistryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(registry.cartridges.len(), 1);
        assert_eq!(registry.cartridges[0].caps[0].description, "");
        assert_eq!(registry.cartridges[0].caps[1].description, "Analyze images");
    }

    // TEST637: Verify full CartridgeInfo deserialization with signature and package fields
    #[test]
    fn test637_deserialize_full_cartridge_with_signature() {
        let json = r#"{
            "id": "pdfcartridge",
            "name": "pdfcartridge",
            "version": "0.81.5325",
            "description": "PDF document processor",
            "author": "https://github.com/machinefabric",
            "pageUrl": "https://github.com/machinefabric/pdfcartridge",
            "teamId": "P336JK947M",
            "signedAt": "2026-02-07T16:40:28Z",
            "minAppVersion": "1.0.0",
            "caps": [
                {
                    "urn": "cap:in=\"media:pdf\";op=disbind;out=\"media:disbound-page;textable;list\"",
                    "title": "Disbind PDF",
                    "description": "Extract pages from PDF"
                }
            ],
            "categories": [],
            "tags": [],
            "versions": {
                "0.81.5325": {
                    "releaseDate": "2026-02-07T16:40:28Z",
                    "changelog": [],
                    "builds": [
                        {
                            "platform": "darwin-arm64",
                            "package": {
                                "name": "pdfcartridge-0.81.5325.pkg",
                                "sha256": "9b68724eb9220ecf01e8ed4f5f80c594fbac2239bc5bf675005ec882ecc5eba0",
                                "size": 5187485
                            }
                        }
                    ]
                }
            },
            "availableVersions": ["0.81.5325"]
        }"#;

        let cartridge: CartridgeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(cartridge.id, "pdfcartridge");
        assert_eq!(cartridge.team_id, "P336JK947M");
        assert_eq!(cartridge.signed_at, "2026-02-07T16:40:28Z");
        assert!(
            !cartridge.team_id.is_empty(),
            "Cartridge must have team_id for signature verification"
        );
        assert!(
            !cartridge.signed_at.is_empty(),
            "Cartridge must have signed_at timestamp"
        );
    }

    // TEST320-335: CartridgeRepoServer and CartridgeRepoClient tests

    #[test]
    fn test320_cartridge_info_construction() {
        // TEST320: Construct CartridgeInfo and verify fields
        let cartridge = CartridgeInfo {
            id: "testcartridge".to_string(),
            name: "Test Cartridge".to_string(),
            version: "1.0.0".to_string(),
            description: "A test cartridge".to_string(),
            author: "Test Author".to_string(),
            homepage: "https://example.com".to_string(),
            team_id: "TEAM123".to_string(),
            signed_at: "2026-02-07T00:00:00Z".to_string(),
            min_app_version: "1.0.0".to_string(),
            page_url: "https://example.com/cartridge".to_string(),
            categories: vec!["test".to_string()],
            tags: vec!["testing".to_string()],
            caps: vec![],
            versions: HashMap::from([(
                "1.0.0".to_string(),
                CartridgeVersionData {
                    release_date: "2026-02-07".to_string(),
                    changelog: vec![],
                    min_app_version: "1.0.0".to_string(),
                    builds: vec![CartridgeBuild {
                        platform: "darwin-arm64".to_string(),
                        package: CartridgeDistributionInfo {
                            name: "test-1.0.0.pkg".to_string(),
                            sha256: "abc123".to_string(),
                            size: 1000,
                        },
                    }],
                },
            )]),
            available_versions: vec!["1.0.0".to_string()],
        };

        assert_eq!(cartridge.id, "testcartridge");
        assert_eq!(cartridge.name, "Test Cartridge");
        assert_eq!(cartridge.version, "1.0.0");
    }

    #[test]
    fn test321_cartridge_info_is_signed() {
        // TEST321: Verify is_signed() method
        let mut cartridge = CartridgeInfo {
            id: "testcartridge".to_string(),
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
            versions: HashMap::new(),
            available_versions: vec![],
        };

        assert!(cartridge.is_signed());

        cartridge.team_id = String::new();
        assert!(!cartridge.is_signed());

        cartridge.team_id = "TEAM123".to_string();
        cartridge.signed_at = String::new();
        assert!(!cartridge.is_signed());
    }

    #[test]
    fn test322_cartridge_info_build_for_platform() {
        // TEST322: Verify build_for_platform() method
        let cartridge = CartridgeInfo {
            id: "testcartridge".to_string(),
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
            versions: HashMap::from([(
                "1.0.0".to_string(),
                CartridgeVersionData {
                    release_date: "2026-02-07".to_string(),
                    changelog: vec![],
                    min_app_version: String::new(),
                    builds: vec![CartridgeBuild {
                        platform: "darwin-arm64".to_string(),
                        package: CartridgeDistributionInfo {
                            name: "test-1.0.0.pkg".to_string(),
                            sha256: "abc123".to_string(),
                            size: 1000,
                        },
                    }],
                },
            )]),
            available_versions: vec!["1.0.0".to_string()],
        };

        // Should find build for matching platform
        let build = cartridge.build_for_platform("darwin-arm64");
        assert!(build.is_some());
        assert_eq!(build.unwrap().package.name, "test-1.0.0.pkg");

        // Should return None for non-matching platform
        let no_build = cartridge.build_for_platform("linux-x86_64");
        assert!(no_build.is_none());

        // Cartridge with no versions should return None
        let empty_cartridge = CartridgeInfo {
            id: "empty".to_string(),
            name: "Empty".to_string(),
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
            versions: HashMap::new(),
            available_versions: vec![],
        };
        assert!(empty_cartridge.build_for_platform("darwin-arm64").is_none());
    }

    #[test]
    fn test323_cartridge_repo_server_validate_registry() {
        // TEST323: Validate registry schema version
        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: HashMap::new(),
        };

        let server = CartridgeRepoServer::new(registry);
        assert!(server.is_ok());

        // Test v2.0 schema rejection
        let old_registry = CartridgeRegistry {
            schema_version: "2.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: HashMap::new(),
        };

        let result = CartridgeRepoServer::new(old_registry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("4.0"));
    }

    #[test]
    fn test324_cartridge_repo_server_transform_to_array() {
        // TEST324: Transform v3 registry to flat cartridge array
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec!["Initial release".to_string()],
                min_app_version: "1.0.0".to_string(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        cartridges_map.insert(
            "testcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "Test Cartridge".to_string(),
                description: "A test cartridge".to_string(),
                author: "Test Author".to_string(),
                page_url: "https://example.com".to_string(),
                team_id: "TEAM123".to_string(),
                min_app_version: "1.0.0".to_string(),
                caps: vec![],
                categories: vec!["test".to_string()],
                tags: vec!["testing".to_string()],
                latest_version: "1.0.0".to_string(),
                versions,
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let result = server.transform_to_cartridge_array();
        assert!(result.is_ok());

        let cartridges_array = result.unwrap();
        assert_eq!(cartridges_array.len(), 1);
        assert_eq!(cartridges_array[0].id, "testcartridge");
        assert_eq!(cartridges_array[0].name, "Test Cartridge");
        assert_eq!(cartridges_array[0].version, "1.0.0");
    }

    #[test]
    fn test325_cartridge_repo_server_get_cartridges() {
        // TEST325: Get all cartridges via get_cartridges()
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        cartridges_map.insert(
            "testcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "Test Cartridge".to_string(),
                description: "A test cartridge".to_string(),
                author: "Test Author".to_string(),
                page_url: String::new(),
                team_id: "TEAM123".to_string(),
                min_app_version: String::new(),
                caps: vec![],
                categories: vec![],
                tags: vec![],
                latest_version: "1.0.0".to_string(),
                versions,
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let response = server.get_cartridges().unwrap();
        assert_eq!(response.cartridges.len(), 1);
        assert_eq!(response.cartridges[0].id, "testcartridge");
    }

    #[test]
    fn test326_cartridge_repo_server_get_cartridge_by_id() {
        // TEST326: Get cartridge by ID
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        cartridges_map.insert(
            "testcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "Test Cartridge".to_string(),
                description: "A test cartridge".to_string(),
                author: "Test Author".to_string(),
                page_url: String::new(),
                team_id: "TEAM123".to_string(),
                min_app_version: String::new(),
                caps: vec![],
                categories: vec![],
                tags: vec![],
                latest_version: "1.0.0".to_string(),
                versions,
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let result = server.get_cartridge_by_id("testcartridge").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, "testcartridge");

        let not_found = server.get_cartridge_by_id("nonexistent").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test327_cartridge_repo_server_search_cartridges() {
        // TEST327: Search cartridges by text query
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "pdf-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        cartridges_map.insert(
            "pdfcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "PDF Cartridge".to_string(),
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
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let results = server.search_cartridges("pdf").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "pdfcartridge");

        let no_match = server.search_cartridges("nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test328_cartridge_repo_server_get_by_category() {
        // TEST328: Filter cartridges by category
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        cartridges_map.insert(
            "doccartridge".to_string(),
            CartridgeRegistryEntry {
                name: "Doc Cartridge".to_string(),
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
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let results = server.get_cartridges_by_category("document").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "doccartridge");

        let no_match = server.get_cartridges_by_category("nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test329_cartridge_repo_server_get_by_cap() {
        // TEST329: Find cartridges by cap URN
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        let cap_urn = r#"cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list""#;
        cartridges_map.insert(
            "pdfcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "PDF Cartridge".to_string(),
                description: "Process PDFs".to_string(),
                author: "Test Author".to_string(),
                page_url: String::new(),
                team_id: "TEAM123".to_string(),
                min_app_version: String::new(),
                caps: vec![CartridgeCapSummary {
                    urn: cap_urn.to_string(),
                    title: "Disbind PDF".to_string(),
                    description: "Extract pages".to_string(),
                }],
                categories: vec![],
                tags: vec![],
                latest_version: "1.0.0".to_string(),
                versions,
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        let server = CartridgeRepoServer::new(registry).unwrap();
        let results = server.get_cartridges_by_cap(cap_urn).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "pdfcartridge");

        let no_match = server.get_cartridges_by_cap("cap:nonexistent").unwrap();
        assert_eq!(no_match.len(), 0);
    }

    #[tokio::test]
    async fn test330_cartridge_repo_client_update_cache() {
        // TEST330: CartridgeRepoClient cache update
        let repo = CartridgeRepo::new(3600);

        // Create a mock registry response
        let registry = CartridgeRegistryResponse {
            cartridges: vec![CartridgeInfo {
                id: "testcartridge".to_string(),
                name: "Test Cartridge".to_string(),
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
                versions: HashMap::new(),
                available_versions: vec![],
            }],
        };

        // Update cache directly (simulating a fetch)
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry);
        drop(caches);

        // Verify cache was updated
        let cartridge = repo.get_cartridge("testcartridge").await;
        assert!(cartridge.is_some());
        assert_eq!(cartridge.unwrap().id, "testcartridge");
    }

    #[tokio::test]
    async fn test331_cartridge_repo_client_get_suggestions() {
        // TEST331: Get suggestions for missing cap
        let repo = CartridgeRepo::new(3600);

        let cap_urn = r#"cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list""#;
        let registry = CartridgeRegistryResponse {
            cartridges: vec![CartridgeInfo {
                id: "pdfcartridge".to_string(),
                name: "PDF Cartridge".to_string(),
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
                caps: vec![CartridgeCapSummary {
                    urn: cap_urn.to_string(),
                    title: "Disbind PDF".to_string(),
                    description: "Extract pages".to_string(),
                }],
                versions: HashMap::new(),
                available_versions: vec![],
            }],
        };

        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry);
        drop(caches);

        let suggestions = repo.get_suggestions_for_cap(cap_urn).await;
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].cartridge_id, "pdfcartridge");
        assert_eq!(suggestions[0].cap_urn, cap_urn);
    }

    #[tokio::test]
    async fn test332_cartridge_repo_client_get_cartridge() {
        // TEST332: Get cartridge by ID from client
        let repo = CartridgeRepo::new(3600);

        let registry = CartridgeRegistryResponse {
            cartridges: vec![CartridgeInfo {
                id: "testcartridge".to_string(),
                name: "Test Cartridge".to_string(),
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
                versions: HashMap::new(),
                available_versions: vec![],
            }],
        };

        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry);
        drop(caches);

        let cartridge = repo.get_cartridge("testcartridge").await;
        assert!(cartridge.is_some());
        assert_eq!(cartridge.unwrap().id, "testcartridge");

        let not_found = repo.get_cartridge("nonexistent").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test333_cartridge_repo_client_get_all_caps() {
        // TEST333: Get all available caps
        let repo = CartridgeRepo::new(3600);

        let cap1 = "cap:in=\"media:pdf\";op=disbind;out=\"media:disbound-page;textable;list\"";
        let cap2 =
            "cap:in=\"media:txt;textable\";op=disbind;out=\"media:disbound-page;textable;list\"";

        let registry = CartridgeRegistryResponse {
            cartridges: vec![
                CartridgeInfo {
                    id: "cartridge1".to_string(),
                    name: "Cartridge 1".to_string(),
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
                    caps: vec![CartridgeCapSummary {
                        urn: cap1.to_string(),
                        title: "Cap 1".to_string(),
                        description: String::new(),
                    }],
                    versions: HashMap::new(),
                    available_versions: vec![],
                },
                CartridgeInfo {
                    id: "cartridge2".to_string(),
                    name: "Cartridge 2".to_string(),
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
                    caps: vec![CartridgeCapSummary {
                        urn: cap2.to_string(),
                        title: "Cap 2".to_string(),
                        description: String::new(),
                    }],
                    versions: HashMap::new(),
                    available_versions: vec![],
                },
            ],
        };

        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry);
        drop(caches);

        let caps = repo.get_all_available_caps().await;
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&cap1.to_string()));
        assert!(caps.contains(&cap2.to_string()));
    }

    #[tokio::test]
    async fn test334_cartridge_repo_client_needs_sync() {
        // TEST334: Check if client needs sync
        let repo = CartridgeRepo::new(3600);

        let urls = vec!["https://example.com/cartridges".to_string()];

        // Empty cache should need sync
        assert!(repo.needs_sync(&urls).await);

        // After update, should not need sync
        let registry = CartridgeRegistryResponse { cartridges: vec![] };
        let mut caches = repo.caches.write().await;
        CartridgeRepo::update_cache(&mut caches, "https://example.com/cartridges", registry);
        drop(caches);

        assert!(!repo.needs_sync(&urls).await);
    }

    #[test]
    fn test335_cartridge_repo_server_client_integration() {
        // TEST335: Server creates response, client consumes it
        let mut cartridges_map = HashMap::new();
        let mut versions = HashMap::new();

        versions.insert(
            "1.0.0".to_string(),
            CartridgeVersionData {
                release_date: "2026-02-07".to_string(),
                changelog: vec![],
                min_app_version: String::new(),
                builds: vec![CartridgeBuild {
                    platform: "darwin-arm64".to_string(),
                    package: CartridgeDistributionInfo {
                        name: "test-1.0.0.pkg".to_string(),
                        sha256: "abc123".to_string(),
                        size: 1000,
                    },
                }],
            },
        );

        let cap_urn = "cap:in=\"media:test\";op=test;out=\"media:result\"";
        cartridges_map.insert(
            "testcartridge".to_string(),
            CartridgeRegistryEntry {
                name: "Test Cartridge".to_string(),
                description: "A test cartridge".to_string(),
                author: "Test Author".to_string(),
                page_url: "https://example.com".to_string(),
                team_id: "TEAM123".to_string(),
                min_app_version: String::new(),
                caps: vec![CartridgeCapSummary {
                    urn: cap_urn.to_string(),
                    title: "Test Cap".to_string(),
                    description: "Test capability".to_string(),
                }],
                categories: vec!["test".to_string()],
                tags: vec![],
                latest_version: "1.0.0".to_string(),
                versions,
            },
        );

        let registry = CartridgeRegistry {
            schema_version: "4.0".to_string(),
            last_updated: "2026-02-07".to_string(),
            cartridges: cartridges_map,
        };

        // Server transforms registry
        let server = CartridgeRepoServer::new(registry).unwrap();
        let response = server.get_cartridges().unwrap();

        // Verify response structure
        assert_eq!(response.cartridges.len(), 1);
        let cartridge = &response.cartridges[0];
        assert_eq!(cartridge.id, "testcartridge");
        assert_eq!(cartridge.name, "Test Cartridge");
        assert!(cartridge.is_signed());
        assert!(!cartridge.versions.is_empty());
        assert_eq!(cartridge.caps.len(), 1);
        assert_eq!(cartridge.caps[0].urn, cap_urn);
    }
}
