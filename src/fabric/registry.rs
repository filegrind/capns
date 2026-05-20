//! Unified fabric registry: caps + media defs.
//!
//! Two domain payload types:
//! - `Cap` (cap definitions) at `<base>/caps/<sha256-of-canonical-urn>`
//! - `StoredMediaDef` (media defs) at `<base>/media/<sha256-of-canonical-urn>`
//!
//! On disk:
//! - `<cache_dir>/caps/<sha256>.json`
//! - `<cache_dir>/media/<sha256>.json`
//!
//! Resolution policy (same for both domains):
//!   1. In-memory cache hit → return immediately.
//!   2. Synchronous fetch attempt with hard 500 ms deadline.
//!   3. Deadline miss / error → enqueue for background consumer, return
//!      `None` (sync surface) or `Err` (async surface).
//!
//! The cap fetch is **atomic**: if any media URN referenced by a cap fails
//! to fetch, the cap is NOT cached. This guarantees that any cap landing
//! in the cap cache has every one of its referenced media defs already in
//! the media cache (and the extension index).

use crate::cap::definition::ArgSource;
use crate::media::spec::MediaDef;
use crate::Cap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const DEFAULT_REGISTRY_BASE_URL: &str = "https://fabric.capdag.com";
const CACHE_DURATION_HOURS: u64 = 24;

/// Hard wall-clock budget for the synchronous fetch attempt that
/// `get_cached_cap` and `get_cached_media_def` each make on a cache
/// miss. Anything that doesn't return inside this window times out and
/// falls through to the queue path; the next call hits warm cache.
const SYNC_FETCH_DEADLINE: Duration = Duration::from_millis(500);

// =============================================================================
// CONFIGURATION
// =============================================================================

/// Configuration for the fabric registry.
///
/// Sources, in priority order:
/// 1. Builder methods.
/// 2. Environment variables (`CAPDAG_REGISTRY_URL`, `CAPDAG_SCHEMA_BASE_URL`).
/// 3. Defaults: `https://fabric.capdag.com` for the registry, `<registry>/schema`
///    for schemas.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub registry_base_url: String,
    pub schema_base_url: String,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        let registry_base = env::var("CAPDAG_REGISTRY_URL")
            .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string());
        let schema_base = env::var("CAPDAG_SCHEMA_BASE_URL")
            .unwrap_or_else(|_| format!("{}/schema", registry_base));
        Self {
            registry_base_url: registry_base,
            schema_base_url: schema_base,
        }
    }
}

impl RegistryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_registry_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        if self.schema_base_url == format!("{}/schema", self.registry_base_url) {
            self.schema_base_url = format!("{}/schema", url);
        }
        self.registry_base_url = url;
        self
    }

    pub fn with_schema_url(mut self, url: impl Into<String>) -> Self {
        self.schema_base_url = url.into();
        self
    }
}

// =============================================================================
// PAYLOAD TYPES
// =============================================================================

/// Stored media def format (matches registry API response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMediaDef {
    pub urn: String,
    pub media_type: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<crate::MediaValidation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

impl StoredMediaDef {
    pub fn to_media_def_def(&self) -> MediaDef {
        MediaDef {
            urn: self.urn.clone(),
            media_type: self.media_type.clone(),
            title: self.title.clone(),
            profile_uri: self.profile_uri.clone(),
            schema: self.schema.clone(),
            description: self.description.clone(),
            documentation: self.documentation.clone(),
            validation: self.validation.clone(),
            metadata: self.metadata.clone(),
            extensions: self.extensions.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapCacheEntry {
    definition: Cap,
    cached_at: u64,
    ttl_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaCacheEntry {
    spec: StoredMediaDef,
    cached_at: u64,
    ttl_hours: u64,
}

trait CacheEntryExt {
    fn cached_at(&self) -> u64;
    fn ttl_hours(&self) -> u64;
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.cached_at() + (self.ttl_hours() * 3600)
    }
}
impl CacheEntryExt for CapCacheEntry {
    fn cached_at(&self) -> u64 { self.cached_at }
    fn ttl_hours(&self) -> u64 { self.ttl_hours }
}
impl CacheEntryExt for MediaCacheEntry {
    fn cached_at(&self) -> u64 { self.cached_at }
    fn ttl_hours(&self) -> u64 { self.ttl_hours }
}

// =============================================================================
// URN NORMALISATION
// =============================================================================

fn normalize_cap_urn(urn: &str) -> String {
    match crate::CapUrn::from_string(urn) {
        Ok(parsed) => parsed.to_string(),
        Err(_) => urn.to_string(),
    }
}

fn normalize_media_urn(urn: &str) -> String {
    match crate::MediaUrn::from_string(urn) {
        Ok(parsed) => parsed.to_string(),
        Err(_) => urn.to_string(),
    }
}

/// Distinguishes domain on the background-fetch queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum FetchKey {
    Cap(String),
    Media(String),
}

// =============================================================================
// REGISTRY
// =============================================================================

#[derive(Debug)]
pub struct FabricRegistry {
    client: reqwest::Client,
    /// Root cache directory. Caps and media defs live in `caps/` and
    /// `media/` subdirectories respectively, mirroring the registry's
    /// own URL layout.
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    /// Lower-case extension → list of canonical media URNs.
    extension_index: Arc<Mutex<HashMap<String, Vec<String>>>>,
    config: RegistryConfig,
    offline_flag: Arc<AtomicBool>,
    fetch_queue_tx: Option<mpsc::UnboundedSender<FetchKey>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
}

impl FabricRegistry {
    /// Create a new fabric registry with default configuration.
    pub async fn new() -> Result<Self, FabricRegistryError> {
        Self::with_config(RegistryConfig::default()).await
    }

    /// Create a new fabric registry with custom configuration.
    pub async fn with_config(config: RegistryConfig) -> Result<Self, FabricRegistryError> {
        let cache_dir = Self::default_cache_root()?;
        let caps_dir = cache_dir.join("caps");
        let media_dir = cache_dir.join("media");
        for d in [&caps_dir, &media_dir] {
            fs::create_dir_all(d).map_err(|e| {
                FabricRegistryError::CacheError(format!(
                    "Failed to create cache directory {:?}: {}",
                    d, e
                ))
            })?;
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                FabricRegistryError::HttpError(format!("Failed to create HTTP client: {}", e))
            })?;

        let cached_caps_map = Self::load_all_cached_caps(&caps_dir)?;
        let cached_specs_map = Self::load_all_cached_media_defs(&media_dir)?;
        let extension_index_map = Self::build_extension_index(&cached_specs_map);

        let cached_caps = Arc::new(Mutex::new(cached_caps_map));
        let cached_media_defs = Arc::new(Mutex::new(cached_specs_map));
        let extension_index = Arc::new(Mutex::new(extension_index_map));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
        let offline_flag = Arc::new(AtomicBool::new(false));

        let fetch_queue_tx = match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let (tx, rx) = mpsc::unbounded_channel::<FetchKey>();
                tokio::spawn(run_fetch_consumer(
                    rx,
                    client.clone(),
                    cache_dir.clone(),
                    Arc::clone(&cached_caps),
                    Arc::clone(&cached_media_defs),
                    Arc::clone(&extension_index),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&offline_flag),
                    config.clone(),
                ));
                Some(tx)
            }
            Err(_) => None,
        };

        let registry = Self {
            client,
            cache_dir,
            cached_caps,
            cached_media_defs,
            extension_index,
            config,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
        };

        // The identity cap is the protocol-mandatory categorical
        // identity morphism — every capset must contain it. Seed it
        // into the in-memory cap cache directly (no network round-trip,
        // no disk write) so it is always available even on a fresh
        // install with no prior cache.
        registry.ensure_identity_cap();

        Ok(registry)
    }

    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    pub fn set_offline(&self, offline: bool) {
        self.offline_flag.store(offline, Ordering::Relaxed);
    }

    fn default_cache_root() -> Result<PathBuf, FabricRegistryError> {
        let mut cache_dir = dirs::cache_dir().ok_or_else(|| {
            FabricRegistryError::CacheError("Could not determine cache directory".to_string())
        })?;
        cache_dir.push("capdag");
        Ok(cache_dir)
    }

    fn ensure_identity_cap(&self) {
        use crate::standard::caps::identity_cap;
        let identity = identity_cap();
        let urn = identity.urn_string();
        let normalized_urn = normalize_cap_urn(&urn);
        if let Ok(mut cached_caps) = self.cached_caps.lock() {
            if !cached_caps.contains_key(&normalized_urn) {
                cached_caps.insert(normalized_urn, identity);
            }
        }
    }

    // -------------------------------------------------------------------------
    // CAP API
    // -------------------------------------------------------------------------

    /// Get a cap from in-memory cache or fetch from registry. Atomic with
    /// respect to referenced media defs: a cap whose media-def footprint
    /// can't be fully fetched is not cached and the call returns `Err`.
    pub async fn get_cap(&self, urn: &str) -> Result<Cap, FabricRegistryError> {
        let normalized_urn = normalize_cap_urn(urn);
        if let Some(cap) = self.cached_caps.lock().ok().and_then(|m| m.get(&normalized_urn).cloned()) {
            return Ok(cap);
        }
        fetch_one_cap_atomic(
            &self.client,
            &self.cache_dir,
            &self.cached_caps,
            &self.cached_media_defs,
            &self.extension_index,
            &self.offline_flag,
            &self.config,
            &normalized_urn,
        )
        .await
    }

    /// Get multiple caps at once - fails if any cap is not available.
    pub async fn get_caps(&self, urns: &[&str]) -> Result<Vec<Cap>, FabricRegistryError> {
        let mut caps = Vec::new();
        for urn in urns {
            caps.push(self.get_cap(urn).await?);
        }
        Ok(caps)
    }

    /// Get all currently cached caps from in-memory cache.
    pub async fn get_cached_caps(&self) -> Result<Vec<Cap>, FabricRegistryError> {
        let cached_caps = self.cached_caps.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock cap cache: {}", e))
        })?;
        Ok(cached_caps.values().cloned().collect())
    }

    /// Synchronous cap lookup that warms its own cache. See module docs.
    pub fn get_cached_cap(&self, urn: &str) -> Option<Cap> {
        let normalized_urn = normalize_cap_urn(urn);
        if let Some(cap) = self.cached_caps.lock().ok().and_then(|m| m.get(&normalized_urn).cloned()) {
            return Some(cap);
        }
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        if !matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            self.enqueue_for_background_fetch(FetchKey::Cap(normalized_urn));
            return None;
        }
        let sync_attempt = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                tokio::time::timeout(
                    SYNC_FETCH_DEADLINE,
                    fetch_one_cap_atomic(
                        &self.client,
                        &self.cache_dir,
                        &self.cached_caps,
                        &self.cached_media_defs,
                        &self.extension_index,
                        &self.offline_flag,
                        &self.config,
                        &normalized_urn,
                    ),
                )
                .await
            })
        });
        match sync_attempt {
            Ok(Ok(cap)) => return Some(cap),
            Ok(Err(e)) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized_urn, error = %e,
                    "Synchronous cap fetch errored within deadline; enqueueing for background fetch."
                );
            }
            Err(_elapsed) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized_urn,
                    "Synchronous cap fetch did not complete within deadline; enqueueing for background fetch."
                );
            }
        }
        self.enqueue_for_background_fetch(FetchKey::Cap(normalized_urn));
        None
    }

    /// Validate a local cap against its canonical definition.
    pub async fn validate_cap(&self, cap: &Cap) -> Result<(), FabricRegistryError> {
        let canonical_cap = self.get_cap(&cap.urn_string()).await?;
        if cap.command != canonical_cap.command {
            return Err(FabricRegistryError::ValidationError(format!(
                "Command mismatch. Local: {}, Canonical: {}",
                cap.command, canonical_cap.command
            )));
        }
        let local_stdin = cap.get_stdin_media_urn();
        let canonical_stdin = canonical_cap.get_stdin_media_urn();
        if local_stdin != canonical_stdin {
            return Err(FabricRegistryError::ValidationError(format!(
                "stdin mismatch. Local: {:?}, Canonical: {:?}",
                local_stdin, canonical_stdin
            )));
        }
        Ok(())
    }

    /// Check whether a cap URN exists in the registry (cached or online).
    pub async fn cap_exists(&self, urn: &str) -> bool {
        self.get_cap(urn).await.is_ok()
    }

    /// Add caps to the in-memory cache. Test helper.
    pub fn add_caps_to_cache(&self, caps: Vec<Cap>) {
        if let Ok(mut cached_caps) = self.cached_caps.lock() {
            for cap in caps {
                let urn = cap.urn_string();
                let normalized_urn = normalize_cap_urn(&urn);
                cached_caps.insert(normalized_urn, cap);
            }
        }
    }

    // -------------------------------------------------------------------------
    // MEDIA-DEF API
    // -------------------------------------------------------------------------

    /// Get a media def from cache or fetch from registry.
    pub async fn get_media_def(
        &self,
        urn: &str,
    ) -> Result<StoredMediaDef, FabricRegistryError> {
        let normalized = normalize_media_urn(urn);
        if let Some(spec) = self
            .cached_media_defs
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Ok(spec);
        }
        fetch_one_media_def(
            &self.client,
            &self.cache_dir,
            &self.cached_media_defs,
            &self.extension_index,
            &self.offline_flag,
            &self.config,
            &normalized,
        )
        .await
    }

    /// Get multiple media defs at once.
    pub async fn get_media_defs(
        &self,
        urns: &[&str],
    ) -> Result<Vec<StoredMediaDef>, FabricRegistryError> {
        let mut specs = Vec::new();
        for urn in urns {
            specs.push(self.get_media_def(urn).await?);
        }
        Ok(specs)
    }

    /// Get all currently cached media defs.
    pub async fn get_cached_media_defs(&self) -> Result<Vec<StoredMediaDef>, FabricRegistryError> {
        let cached_specs = self.cached_media_defs.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock media-def cache: {}", e))
        })?;
        Ok(cached_specs.values().cloned().collect())
    }


    /// Synchronous media-def lookup that warms its own cache.
    pub fn get_cached_media_def(&self, urn: &str) -> Option<StoredMediaDef> {
        let normalized = normalize_media_urn(urn);
        if let Some(spec) = self
            .cached_media_defs
            .lock()
            .ok()
            .and_then(|m| m.get(&normalized).cloned())
        {
            return Some(spec);
        }
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        if !matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            self.enqueue_for_background_fetch(FetchKey::Media(normalized));
            return None;
        }
        let sync_attempt = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                tokio::time::timeout(
                    SYNC_FETCH_DEADLINE,
                    fetch_one_media_def(
                        &self.client,
                        &self.cache_dir,
                        &self.cached_media_defs,
                        &self.extension_index,
                        &self.offline_flag,
                        &self.config,
                        &normalized,
                    ),
                )
                .await
            })
        });
        match sync_attempt {
            Ok(Ok(spec)) => return Some(spec),
            Ok(Err(e)) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized, error = %e,
                    "Synchronous media-def fetch errored within deadline; enqueueing for background fetch."
                );
            }
            Err(_elapsed) => {
                tracing::debug!(
                    target: "capdag::fabric::registry",
                    urn = %normalized,
                    "Synchronous media-def fetch did not complete within deadline; enqueueing for background fetch."
                );
            }
        }
        self.enqueue_for_background_fetch(FetchKey::Media(normalized));
        None
    }

    /// Returns `true` if the URN is a bookend-eligible file format — its
    /// stored spec has at least one registered file extension.
    pub fn is_bookend(&self, urn: &str) -> bool {
        match self.get_cached_media_def(urn) {
            Some(spec) => !spec.extensions.is_empty(),
            None => false,
        }
    }

    /// Snapshot of every bookend-eligible URN currently in the cache.
    pub fn bookend_urns(&self) -> std::collections::HashSet<crate::MediaUrn> {
        let cached = match self.cached_media_defs.lock() {
            Ok(g) => g,
            Err(_) => return Default::default(),
        };
        cached
            .values()
            .filter(|spec| !spec.extensions.is_empty())
            .filter_map(|spec| crate::MediaUrn::from_string(&spec.urn).ok())
            .collect()
    }

    /// Returns all media URNs registered for the given file extension.
    pub fn media_urns_for_extension(
        &self,
        extension: &str,
    ) -> Result<Vec<String>, FabricRegistryError> {
        let ext_lower = extension.to_lowercase();
        let index = self.extension_index.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock extension index: {}", e))
        })?;
        index.get(&ext_lower).cloned().ok_or_else(|| {
            FabricRegistryError::ExtensionNotFound(format!(
                "No media def registered for extension '{}'",
                extension
            ))
        })
    }

    /// Get all extension → URNs mappings.
    pub fn get_extension_mappings(
        &self,
    ) -> Result<Vec<(String, Vec<String>)>, FabricRegistryError> {
        let index = self.extension_index.lock().map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to lock extension index: {}", e))
        })?;
        Ok(index.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// Insert a media def into the in-memory cache. Test helper.
    pub fn insert_cached_media_def_for_test(&self, spec: StoredMediaDef) {
        let normalized = normalize_media_urn(&spec.urn);
        if let Ok(mut cache) = self.cached_media_defs.lock() {
            cache.insert(normalized, spec.clone());
        }
        if let Ok(mut idx) = self.extension_index.lock() {
            for ext in &spec.extensions {
                let ext_lower = ext.to_lowercase();
                let urns = idx.entry(ext_lower).or_default();
                if !urns.contains(&spec.urn) {
                    urns.push(spec.urn.clone());
                }
            }
        }
    }

    /// Check if a media URN exists in registry (cached or online).
    pub async fn media_def_exists(&self, urn: &str) -> bool {
        self.get_media_def(urn).await.is_ok()
    }

    // -------------------------------------------------------------------------
    // SHARED ADMIN API
    // -------------------------------------------------------------------------

    /// Clear both caches (in-memory and on disk).
    pub fn clear_cache(&self) -> Result<(), FabricRegistryError> {
        if let Ok(mut g) = self.cached_caps.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.cached_media_defs.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.extension_index.lock() {
            g.clear();
        }
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir).map_err(|e| {
                FabricRegistryError::CacheError(format!("Failed to clear cache directory: {}", e))
            })?;
            for sub in ["caps", "media"] {
                fs::create_dir_all(self.cache_dir.join(sub)).map_err(|e| {
                    FabricRegistryError::CacheError(format!(
                        "Failed to recreate cache directory: {}",
                        e
                    ))
                })?;
            }
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // QUEUE
    // -------------------------------------------------------------------------

    fn enqueue_for_background_fetch(&self, key: FetchKey) {
        let Some(tx) = self.fetch_queue_tx.as_ref() else {
            return;
        };
        let mut in_queue = match self.fetch_in_queue.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        if !in_queue.insert(key.clone()) {
            return;
        }
        if let Err(e) = tx.send(key.clone()) {
            in_queue.remove(&key);
            tracing::warn!(
                target: "capdag::fabric::registry",
                key = ?key, error = %e,
                "Background fetch queue send failed (consumer task is gone); dropping URN."
            );
        }
    }

    // -------------------------------------------------------------------------
    // DISK LOAD
    // -------------------------------------------------------------------------

    fn load_all_cached_caps(caps_dir: &Path) -> Result<HashMap<String, Cap>, FabricRegistryError> {
        let mut caps = HashMap::new();
        if !caps_dir.exists() {
            return Ok(caps);
        }
        for entry in fs::read_dir(caps_dir).map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to read cap cache directory: {}", e))
        })? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to read cap cache entry: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Failed to read cap cache file {:?}: {}", path, e);
                    continue;
                }
            };
            let cache_entry: CapCacheEntry = match serde_json::from_str(&content) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to parse cap cache file {:?}: {}", path, e);
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            if cache_entry.is_expired() {
                let _ = fs::remove_file(&path);
                continue;
            }
            let urn = cache_entry.definition.urn_string();
            caps.insert(normalize_cap_urn(&urn), cache_entry.definition);
        }
        Ok(caps)
    }

    fn load_all_cached_media_defs(
        media_dir: &Path,
    ) -> Result<HashMap<String, StoredMediaDef>, FabricRegistryError> {
        let mut specs = HashMap::new();
        if !media_dir.exists() {
            return Ok(specs);
        }
        for entry in fs::read_dir(media_dir).map_err(|e| {
            FabricRegistryError::CacheError(format!("Failed to read media cache directory: {}", e))
        })? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to read media cache entry: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Failed to read media cache file {:?}: {}", path, e);
                    continue;
                }
            };
            let cache_entry: MediaCacheEntry = match serde_json::from_str(&content) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to parse media cache file {:?}: {}", path, e);
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            if cache_entry.is_expired() {
                let _ = fs::remove_file(&path);
                continue;
            }
            specs.insert(normalize_media_urn(&cache_entry.spec.urn), cache_entry.spec);
        }
        Ok(specs)
    }

    fn build_extension_index(
        specs: &HashMap<String, StoredMediaDef>,
    ) -> HashMap<String, Vec<String>> {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for spec in specs.values() {
            for ext in &spec.extensions {
                let ext_lower = ext.to_lowercase();
                index.entry(ext_lower).or_default().push(spec.urn.clone());
            }
        }
        index
    }

    // -------------------------------------------------------------------------
    // TEST HELPERS
    // -------------------------------------------------------------------------

    /// Synchronous test constructor with a fresh empty cache. Spawns a
    /// fetch consumer when called inside a tokio runtime; otherwise leaves
    /// the queue inert.
    pub fn new_for_test() -> Self {
        Self::new_for_test_with_config(RegistryConfig::default())
    }

    pub fn new_for_test_with_config(config: RegistryConfig) -> Self {
        let cache_dir = PathBuf::from("/tmp/capdag-test-cache");
        let _ = fs::create_dir_all(cache_dir.join("caps"));
        let _ = fs::create_dir_all(cache_dir.join("media"));
        let cached_caps = Arc::new(Mutex::new(HashMap::new()));
        let cached_media_defs = Arc::new(Mutex::new(HashMap::new()));
        let extension_index = Arc::new(Mutex::new(HashMap::new()));
        let fetch_in_queue = Arc::new(Mutex::new(HashSet::new()));
        let offline_flag = Arc::new(AtomicBool::new(false));
        let client = reqwest::Client::new();

        let fetch_queue_tx = match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                let (tx, rx) = mpsc::unbounded_channel::<FetchKey>();
                tokio::spawn(run_fetch_consumer(
                    rx,
                    client.clone(),
                    cache_dir.clone(),
                    Arc::clone(&cached_caps),
                    Arc::clone(&cached_media_defs),
                    Arc::clone(&extension_index),
                    Arc::clone(&fetch_in_queue),
                    Arc::clone(&offline_flag),
                    config.clone(),
                ));
                Some(tx)
            }
            Err(_) => None,
        };

        let registry = Self {
            client,
            cache_dir,
            cached_caps,
            cached_media_defs,
            extension_index,
            config,
            offline_flag,
            fetch_queue_tx,
            fetch_in_queue,
        };
        registry.ensure_identity_cap();
        registry
    }
}

// =============================================================================
// ATOMIC FETCH HELPERS (free functions)
// =============================================================================

/// Atomic cap fetcher. Fetches the cap body, then ensures every media URN
/// it references is in the media cache. Caches the cap only on full
/// success; otherwise returns `Err` and writes nothing.
async fn fetch_one_cap_atomic(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_caps: &Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: &Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    extension_index: &Arc<Mutex<HashMap<String, Vec<String>>>>,
    offline_flag: &Arc<AtomicBool>,
    config: &RegistryConfig,
    normalized_urn: &str,
) -> Result<Cap, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "Network access blocked by policy — cannot fetch cap '{}'",
            normalized_urn
        )));
    }

    let mut hasher = Sha256::new();
    hasher.update(normalized_urn.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    let url = format!("{}/caps/{}", config.registry_base_url, hash);

    let response = client.get(&url).send().await.map_err(|e| {
        FabricRegistryError::HttpError(format!("Failed to fetch cap: {}", e))
    })?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "Cap '{}' not found in registry (HTTP {})",
            normalized_urn,
            response.status()
        )));
    }
    let cap: Cap = response.json().await.map_err(|e| {
        FabricRegistryError::ParseError(format!(
            "Failed to parse cap '{}': {}",
            normalized_urn, e
        ))
    })?;

    // Walk every media URN referenced by the cap. Empty/wildcard URN
    // (`media:`) is the identity / wildcard sentinel — it has no
    // fetchable spec and must be skipped.
    let mut referenced: Vec<String> = Vec::new();
    let push = |v: &mut Vec<String>, s: &str| {
        let n = normalize_media_urn(s);
        if n != "media:" && !v.contains(&n) {
            v.push(n);
        }
    };
    push(&mut referenced, cap.urn.in_spec());
    push(&mut referenced, cap.urn.out_spec());
    for arg in &cap.args {
        push(&mut referenced, &arg.media_urn);
        for source in &arg.sources {
            if let ArgSource::Stdin { stdin } = source {
                push(&mut referenced, stdin);
            }
        }
    }
    if let Some(out) = &cap.output {
        push(&mut referenced, &out.media_urn);
    }

    for media_urn in &referenced {
        let already_cached = cached_media_defs
            .lock()
            .ok()
            .map(|m| m.contains_key(media_urn))
            .unwrap_or(false);
        if already_cached {
            continue;
        }
        if let Err(e) = fetch_one_media_def(
            client,
            cache_dir,
            cached_media_defs,
            extension_index,
            offline_flag,
            config,
            media_urn,
        )
        .await
        {
            tracing::warn!(
                target: "capdag::fabric::registry",
                cap_urn = %normalized_urn,
                missing_media_urn = %media_urn,
                error = %e,
                "Aborting cap cache write: a referenced media def could not be fetched. \
                 The cap is NOT cached so the next attempt re-tries cleanly."
            );
            return Err(FabricRegistryError::NotFound(format!(
                "cap '{}' references media URN '{}' which could not be fetched: {}",
                normalized_urn, media_urn, e
            )));
        }
    }

    // All referenced media defs in cache. Write the cap.
    let cache_entry = CapCacheEntry {
        definition: cap.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    let cache_file = cache_dir.join("caps").join(format!("{}.json", hash));
    let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to serialize cap cache entry: {}", e))
    })?;
    fs::write(&cache_file, content).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to write cap cache file: {}", e))
    })?;

    if let Ok(mut cached) = cached_caps.lock() {
        cached.insert(normalized_urn.to_string(), cap.clone());
    }

    Ok(cap)
}

/// Atomic media-def fetcher.
pub(crate) async fn fetch_one_media_def(
    client: &reqwest::Client,
    cache_dir: &Path,
    cached_media_defs: &Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    extension_index: &Arc<Mutex<HashMap<String, Vec<String>>>>,
    offline_flag: &Arc<AtomicBool>,
    config: &RegistryConfig,
    normalized_urn: &str,
) -> Result<StoredMediaDef, FabricRegistryError> {
    if offline_flag.load(Ordering::Relaxed) {
        return Err(FabricRegistryError::NetworkBlocked(format!(
            "Network access blocked by policy — cannot fetch media def '{}'",
            normalized_urn
        )));
    }

    let mut hasher = Sha256::new();
    hasher.update(normalized_urn.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    let url = format!("{}/media/{}", config.registry_base_url, hash);

    let response = client.get(&url).send().await.map_err(|e| {
        FabricRegistryError::HttpError(format!("Failed to fetch media def: {}", e))
    })?;
    if !response.status().is_success() {
        return Err(FabricRegistryError::NotFound(format!(
            "Media def '{}' not found in registry (HTTP {})",
            normalized_urn,
            response.status()
        )));
    }
    let spec: StoredMediaDef = response.json().await.map_err(|e| {
        FabricRegistryError::ParseError(format!(
            "Failed to parse media def '{}': {}",
            normalized_urn, e
        ))
    })?;

    let cache_entry = MediaCacheEntry {
        spec: spec.clone(),
        cached_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        ttl_hours: CACHE_DURATION_HOURS,
    };
    let cache_file = cache_dir.join("media").join(format!("{}.json", hash));
    let content = serde_json::to_string_pretty(&cache_entry).map_err(|e| {
        FabricRegistryError::CacheError(format!(
            "Failed to serialize media cache entry: {}",
            e
        ))
    })?;
    fs::write(&cache_file, content).map_err(|e| {
        FabricRegistryError::CacheError(format!("Failed to write media cache file: {}", e))
    })?;

    if let Ok(mut cached) = cached_media_defs.lock() {
        cached.insert(normalized_urn.to_string(), spec.clone());
    }
    if let Ok(mut idx) = extension_index.lock() {
        for ext in &spec.extensions {
            let ext_lower = ext.to_lowercase();
            let urns = idx.entry(ext_lower).or_default();
            if !urns.contains(&spec.urn) {
                urns.push(spec.urn.clone());
            }
        }
    }
    Ok(spec)
}

/// Single shared background fetch consumer for both cap and media URNs.
/// Drains the queue serially; failures are logged and dropped.
#[allow(clippy::too_many_arguments)]
async fn run_fetch_consumer(
    mut rx: mpsc::UnboundedReceiver<FetchKey>,
    client: reqwest::Client,
    cache_dir: PathBuf,
    cached_caps: Arc<Mutex<HashMap<String, Cap>>>,
    cached_media_defs: Arc<Mutex<HashMap<String, StoredMediaDef>>>,
    extension_index: Arc<Mutex<HashMap<String, Vec<String>>>>,
    fetch_in_queue: Arc<Mutex<HashSet<FetchKey>>>,
    offline_flag: Arc<AtomicBool>,
    config: RegistryConfig,
) {
    while let Some(key) = rx.recv().await {
        match &key {
            FetchKey::Cap(normalized_urn) => {
                let already_cached = cached_caps
                    .lock()
                    .ok()
                    .map(|m| m.contains_key(normalized_urn))
                    .unwrap_or(false);
                if !already_cached {
                    match fetch_one_cap_atomic(
                        &client,
                        &cache_dir,
                        &cached_caps,
                        &cached_media_defs,
                        &extension_index,
                        &offline_flag,
                        &config,
                        normalized_urn,
                    )
                    .await
                    {
                        Ok(_) => {
                            tracing::debug!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn,
                                "Background-fetched cap; cache is now warm."
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, error = %e,
                                "Background cap fetch failed; URN dropped from queue (no retry)."
                            );
                        }
                    }
                }
            }
            FetchKey::Media(normalized_urn) => {
                let already_cached = cached_media_defs
                    .lock()
                    .ok()
                    .map(|m| m.contains_key(normalized_urn))
                    .unwrap_or(false);
                if !already_cached {
                    match fetch_one_media_def(
                        &client,
                        &cache_dir,
                        &cached_media_defs,
                        &extension_index,
                        &offline_flag,
                        &config,
                        normalized_urn,
                    )
                    .await
                    {
                        Ok(_) => {
                            tracing::debug!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn,
                                "Background-fetched media def; cache is now warm."
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "capdag::fabric::registry::fetch_consumer",
                                urn = %normalized_urn, error = %e,
                                "Background media-def fetch failed; URN dropped from queue (no retry)."
                            );
                        }
                    }
                }
            }
        }
        if let Ok(mut in_queue) = fetch_in_queue.lock() {
            in_queue.remove(&key);
        }
    }
}

// =============================================================================
// ERROR
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum FabricRegistryError {
    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Not found in registry: {0}")]
    NotFound(String),

    #[error("Failed to parse registry response: {0}")]
    ParseError(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Network access blocked: {0}")]
    NetworkBlocked(String),

    #[error("No media def registered for extension: {0}")]
    ExtensionNotFound(String),
}
