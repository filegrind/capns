//! DAG Execution Engine
//!
//! Executes a resolved DOT DAG by:
//! 1. Discovering and downloading cartridges that provide the required caps
//! 2. Connecting all cartridges to a single CartridgeHostRuntime
//! 3. Routing cap requests through a RelaySwitch
//! 4. Executing edge groups in topological order, streaming frames between caps
//!
//! Fan-in: multiple edges pointing to the same `(to, cap_urn)` are grouped and
//! executed as ONE cap invocation with multiple input streams. The cartridge handler
//! receives all streams and decides how to handle partial availability — it may
//! wait for all, use whatever arrives, or fail.
//!
//! Architecture:
//! ```text
//!   macino ←→ RelaySwitch ←→ RelaySlave ←→ CartridgeHostRuntime ←→ Cartridge A
//!                                                             ←→ Cartridge B
//!                                                             ←→ Cartridge C
//! ```

use super::types::{ResolvedEdge, ResolvedGraph};
use crate::{
    handshake, CapManifest, CapRegistry, CapUrn, CartridgeHostRuntime, CartridgeRepo, Frame,
    FrameReader, FrameWriter, Limits, RelayNotifyCapabilitiesPayload, RelaySlave, RelaySwitch,
    DEFAULT_MAX_CHUNK,
};
use super::stream_io::StreamIoError;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Detect the current platform in the format used by the registry (e.g., "darwin-arm64").
fn detect_platform() -> String {
    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        std::env::consts::OS
    };

    let arch = std::env::consts::ARCH; // "aarch64", "x86_64", etc.
    let arch_normalized = match arch {
        "aarch64" => "arm64",
        other => other,
    };

    format!("{}-{}", os, arch_normalized)
}

/// Default cap-level activity timeout in seconds.
/// If a cartridge sends no frames (Chunk, Log, progress, peer requests) for this
/// duration, the executor aborts with `ExecutionError::ActivityTimeout`.
const DEFAULT_ACTIVITY_TIMEOUT_SECS: u64 = 120;

/// Cap metadata key for per-cap activity timeout override.
const ACTIVITY_TIMEOUT_METADATA_KEY: &str = "activity_timeout_secs";
use tokio::io::{BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::mpsc;

/// Callback for reporting per-cap progress.
/// Parameters: (progress 0.0–1.0, cap URN string, human-readable message)
pub type CapProgressFn = Arc<dyn Fn(f32, &str, &str) + Send + Sync>;

/// Maps child progress [0.0, 1.0] into a parent range [base, base + weight].
///
/// This is the single progress mapping computation used everywhere:
/// - DAG execution: per-group subdivision
/// - ForEach plans: per-item subdivision
/// - Peer calls: caller's progress range delegation
/// - LLM cartridge client: frame-to-callback mapping
///
/// All child progress values are clamped to [0.0, 1.0] before mapping.
/// The mapped result is `base + child_progress.clamp(0.0, 1.0) * weight`.

/// Map child progress [0.0, 1.0] into parent range [base, base + weight].
///
/// This is the canonical progress mapping formula. Every place in the system
/// that subdivides progress must use this function — no ad-hoc derivations.
#[inline]
pub fn map_progress(child_progress: f32, base: f32, weight: f32) -> f32 {
    base + child_progress.clamp(0.0, 1.0) * weight
}

/// Wraps a `CapProgressFn` with a progress range subdivision.
#[derive(Clone)]
pub struct ProgressMapper {
    base: f32,
    weight: f32,
    parent: CapProgressFn,
}

impl ProgressMapper {
    /// Create a mapper that maps child [0.0, 1.0] into parent [base, base + weight].
    pub fn new(parent: &CapProgressFn, base: f32, weight: f32) -> Self {
        Self {
            base,
            weight,
            parent: Arc::clone(parent),
        }
    }

    /// Report child progress. The value is clamped to [0.0, 1.0] and mapped.
    pub fn report(&self, child_progress: f32, cap_urn: &str, msg: &str) {
        let overall = map_progress(child_progress, self.base, self.weight);
        (self.parent)(overall, cap_urn, msg);
    }

    /// Convert into a `CapProgressFn` for passing to APIs that expect one.
    pub fn as_cap_progress_fn(&self) -> CapProgressFn {
        let mapper = self.clone();
        Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
            mapper.report(p, cap_urn, msg);
        })
    }

    /// Create a sub-mapper that maps a child range within this mapper's range.
    ///
    /// Example: if this mapper maps to [0.2, 0.8] (base=0.2, weight=0.6),
    /// and you create a sub-mapper with sub_base=0.5, sub_weight=0.5,
    /// the sub-mapper maps to [0.5, 0.8] in the parent's coordinate space.
    pub fn sub_mapper(&self, sub_base: f32, sub_weight: f32) -> Self {
        Self {
            base: self.base + sub_base * self.weight,
            weight: sub_weight * self.weight,
            parent: Arc::clone(&self.parent),
        }
    }
}

/// Cap URN for the identity capability (always available from any cartridge runtime).
const CAP_IDENTITY: &str = "cap:";

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Cartridge not found for cap: {cap_urn}")]
    CartridgeNotFound { cap_urn: String },

    #[error("Cartridge download failed: {0}")]
    CartridgeDownloadFailed(String),

    #[error("Cartridge execution failed for cap {cap_urn}: {details}")]
    CartridgeExecutionFailed { cap_urn: String, details: String },

    #[error("Node {node} has no incoming data")]
    NoIncomingData { node: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Host error: {0}")]
    HostError(String),

    #[error("Registry error: {0}")]
    RegistryError(String),

    #[error(
        "Activity timeout for cap {cap_urn}: no activity for {idle_secs}s (limit: {limit_secs}s)"
    )]
    ActivityTimeout {
        cap_urn: String,
        idle_secs: u64,
        limit_secs: u64,
    },
}

// =============================================================================
// Node Data (public API — resolved to raw bytes internally)
// =============================================================================

/// Runtime data associated with a DAG node.
#[derive(Debug, Clone)]
pub enum NodeData {
    /// Raw binary data
    Bytes(Vec<u8>),
    /// Text data
    Text(String),
    /// File path — read into bytes before execution
    FilePath(PathBuf),
}

impl NodeData {
    /// Resolve to raw bytes. FilePath reads the file, Text converts to UTF-8 bytes.
    async fn into_bytes(self) -> Result<Vec<u8>, ExecutionError> {
        match self {
            NodeData::Bytes(b) => Ok(b),
            NodeData::Text(t) => Ok(t.into_bytes()),
            NodeData::FilePath(path) => tokio::fs::read(&path).await.map_err(|e| {
                ExecutionError::HostError(format!(
                    "Failed to read file '{}': {}",
                    path.display(),
                    e
                ))
            }),
        }
    }
}

// =============================================================================
// Edge Grouping — fan-in detection
// =============================================================================

/// A group of edges that share the same `(to, cap_urn)`.
///
/// Single-edge groups are standard single-input cap invocations.
/// Multi-edge groups are fan-in: all edges' inputs are sent as separate streams
/// in ONE cap invocation so the handler can consume them together.
pub struct EdgeGroup {
    /// Destination node (same for all edges in the group)
    pub to: String,
    /// Cap URN (same for all edges in the group)
    pub cap_urn: String,
    /// All edges in this group (one or more)
    pub edges: Vec<ResolvedEdge>,
}

/// Group DAG edges by `(to, cap_urn)`.
///
/// Edges that share the same destination node and cap URN form a fan-in group
/// and will be sent as multiple streams in a single cap invocation.
fn build_edge_groups(edges: &[ResolvedEdge]) -> Vec<EdgeGroup> {
    // Preserve insertion order for determinism
    let mut order: Vec<(String, String)> = Vec::new();
    let mut map: HashMap<(String, String), Vec<ResolvedEdge>> = HashMap::new();

    for edge in edges {
        let key = (edge.to.clone(), edge.cap_urn.clone());
        if !map.contains_key(&key) {
            order.push(key.clone());
        }
        map.entry(key).or_default().push(edge.clone());
    }

    order
        .into_iter()
        .map(|key| {
            let edges = map.remove(&key).unwrap();
            EdgeGroup {
                to: key.0,
                cap_urn: key.1,
                edges,
            }
        })
        .collect()
}

/// Topological sort of edge groups.
///
/// A group can execute when all groups that produce its `from` nodes have completed.
/// Returns group indices in execution order.
fn topological_sort_groups(groups: &[EdgeGroup]) -> Result<Vec<usize>, ExecutionError> {
    let n = groups.len();

    // Map each produced node to the group index that produces it
    let mut produced_by: HashMap<&str, usize> = HashMap::new();
    for (i, g) in groups.iter().enumerate() {
        produced_by.insert(g.to.as_str(), i);
    }

    // Compute in-degree for each group and reverse-dependency map
    let mut in_degree: Vec<usize> = vec![0; n];
    // dependents[i] = set of group indices that depend on group i completing first
    let mut dependents: Vec<HashSet<usize>> = (0..n).map(|_| HashSet::new()).collect();

    for (i, g) in groups.iter().enumerate() {
        let mut seen: HashSet<usize> = HashSet::new();
        for edge in &g.edges {
            if let Some(&dep) = produced_by.get(edge.from.as_str()) {
                if dep != i && seen.insert(dep) {
                    in_degree[i] += 1;
                    dependents[dep].insert(i);
                }
            }
        }
    }

    let mut queue: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut sorted: Vec<usize> = Vec::with_capacity(n);

    while let Some(i) = queue.pop() {
        sorted.push(i);
        for &j in &dependents[i] {
            in_degree[j] -= 1;
            if in_degree[j] == 0 {
                queue.push(j);
            }
        }
    }

    if sorted.len() != n {
        return Err(ExecutionError::CartridgeExecutionFailed {
            cap_urn: String::new(),
            details: "Cycle detected in graph".to_string(),
        });
    }

    Ok(sorted)
}

// =============================================================================
// Cartridge Manager
// =============================================================================

/// Manages cartridge discovery, download, and caching.
pub struct CartridgeManager {
    cartridge_repo: CartridgeRepo,
    /// Channel-partitioned root: cartridges install under
    /// `{cartridge_dir}/{channel}/{cartridge_id}/{version}/`.
    cartridge_dir: PathBuf,
    registry_url: String,
    /// Channel this manager is operating in. The orchestrator can only
    /// install/run cartridges that match its channel — release builds
    /// never touch nightly artefacts and vice versa.
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    dev_cartridges: HashMap<PathBuf, CapManifest>,
}

impl CartridgeManager {
    pub fn new(
        cartridge_dir: PathBuf,
        registry_url: String,
        channel: crate::bifaci::cartridge_repo::CartridgeChannel,
        dev_binaries: Vec<PathBuf>,
    ) -> Self {
        use crate::bifaci::cartridge_json::CartridgeJson;

        // Resolve dev paths: directories with cartridge.json → resolve entry point.
        // Files → standalone binary. Directories without cartridge.json → each
        // executable file inside is a separate binary cartridge.
        let mut resolved: Vec<PathBuf> = Vec::new();
        for p in dev_binaries {
            if p.is_file() {
                resolved.push(p);
            } else if p.is_dir() {
                match CartridgeJson::read_from_dir(&p) {
                    Ok(cj) => {
                        let entry_point = cj.resolve_entry_point(&p);
                        tracing::info!(
                            "[DevMode] Directory cartridge at {:?}: entry point {:?}",
                            p,
                            entry_point
                        );
                        resolved.push(entry_point);
                    }
                    Err(crate::bifaci::cartridge_json::CartridgeJsonError::NotFound(_)) => {
                        // No cartridge.json — treat each executable file as a separate binary cartridge
                        if let Ok(entries) = std::fs::read_dir(&p) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    #[cfg(unix)]
                                    {
                                        use std::os::unix::fs::PermissionsExt;
                                        if let Ok(meta) = std::fs::metadata(&path) {
                                            if meta.permissions().mode() & 0o111 != 0 {
                                                resolved.push(path);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "[DevMode] Invalid cartridge.json in {:?}: {} — skipping",
                            p,
                            e
                        );
                    }
                }
            }
        }

        Self {
            cartridge_repo: CartridgeRepo::new(3600),
            cartridge_dir,
            registry_url,
            channel,
            dev_cartridges: resolved
                .into_iter()
                .map(|p| {
                    (
                        p,
                        CapManifest::new(
                            String::new(),
                            String::new(),
                            crate::bifaci::cartridge_repo::CartridgeChannel::Release,
                            String::new(),
                            Vec::new(),
                        ),
                    )
                })
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), ExecutionError> {
        fs::create_dir_all(&self.cartridge_dir)?;

        for (bin_path, _) in &self.dev_cartridges.clone() {
            tracing::info!("[DevMode] Discovering manifest from {:?}...", bin_path);
            match self.discover_manifest(bin_path).await {
                Ok(manifest) => {
                    tracing::info!("[DevMode] Cartridge: {}", manifest.name);
                    for cap in manifest.all_caps() {
                        tracing::info!("[DevMode]   - {}", cap.urn);
                    }
                    self.dev_cartridges.insert(bin_path.clone(), manifest);
                }
                Err(e) => {
                    tracing::error!("[DevMode] Failed: {:?}: {}", bin_path, e);
                    return Err(e);
                }
            }
        }

        self.cartridge_repo
            .sync_repos(&[self.registry_url.clone()])
            .await;

        Ok(())
    }

    async fn discover_manifest(&self, bin_path: &Path) -> Result<CapManifest, ExecutionError> {
        let mut child = Command::new(bin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ExecutionError::CartridgeExecutionFailed {
                cap_urn: "manifest-discovery".to_string(),
                details: format!("Failed to spawn cartridge: {}", e),
            })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;

        let manifest: CapManifest = serde_json::from_slice(&result.manifest)
            .map_err(|e| ExecutionError::HostError(format!("Bad manifest: {}", e)))?;

        let _ = child.kill().await;
        Ok(manifest)
    }

    /// Resolve all cap URNs from the graph to unique (binary_path, known_caps) pairs.
    ///
    /// For dev cartridges (with discovered manifests), registers ALL manifest caps —
    /// not just the DAG edge caps. This is critical because cartridges send peer requests
    /// for caps that aren't in the DAG (e.g., candlecartridge peer-invokes modelcartridge's
    /// download-model cap during ML inference). Without full cap registration, the
    /// CartridgeHostRuntime can't route these peer requests.
    pub async fn resolve_cartridges(
        &self,
        cap_urns: &[&str],
    ) -> Result<Vec<(PathBuf, Vec<String>)>, ExecutionError> {
        // Collect unique cartridge binaries needed for the DAG
        let mut cartridge_paths: HashSet<PathBuf> = HashSet::new();

        for &cap_urn in cap_urns {
            let (bin_path, _cartridge_id) = self.find_cartridge_binary(cap_urn).await?;
            cartridge_paths.insert(bin_path);
        }

        // Also include ALL dev cartridge binaries — they may be needed for peer request
        // routing even if they don't directly appear in the DAG. For example, ML
        // cartridges send peer requests to modelcartridge for model downloading.
        for dev_path in self.dev_cartridges.keys() {
            cartridge_paths.insert(dev_path.clone());
        }

        // For each cartridge, register ALL manifest caps (not just DAG caps)
        let result: Vec<(PathBuf, Vec<String>)> = cartridge_paths
            .into_iter()
            .map(|path| {
                let mut caps: HashSet<String> = HashSet::new();

                // Use full manifest caps for dev cartridges (including cap groups)
                if let Some(manifest) = self.dev_cartridges.get(&path) {
                    for cap in manifest.all_caps() {
                        caps.insert(cap.urn.to_string());
                    }
                }

                // Always include identity
                caps.insert(CAP_IDENTITY.to_string());

                (path, caps.into_iter().collect())
            })
            .collect();

        Ok(result)
    }

    /// Find the binary path for a cap URN.
    async fn find_cartridge_binary(
        &self,
        cap_urn: &str,
    ) -> Result<(PathBuf, String), ExecutionError> {
        let requested_urn =
            CapUrn::from_string(cap_urn).map_err(|e| ExecutionError::CartridgeNotFound {
                cap_urn: format!("Invalid URN: {}: {}", cap_urn, e),
            })?;

        // Check dev cartridges first - use is_dispatchable to find any cartridge
        // that can legally handle the requested cap.
        for (bin_path, manifest) in &self.dev_cartridges {
            for cap in manifest.all_caps() {
                // cap.urn is the provider, requested_urn is the request
                if cap.urn.is_dispatchable(&requested_urn) {
                    return Ok((bin_path.clone(), format!("dev:{}", bin_path.display())));
                }
            }
        }

        // Fall back to registry
        let suggestions = self.cartridge_repo.get_suggestions_for_cap(cap_urn).await;
        if suggestions.is_empty() {
            return Err(ExecutionError::CartridgeNotFound {
                cap_urn: cap_urn.to_string(),
            });
        }

        let cartridge_id = &suggestions[0].cartridge_id;
        let bin_path = self.get_cartridge_path(cartridge_id).await?;
        Ok((bin_path, cartridge_id.clone()))
    }

    pub async fn get_cartridge_path(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        if let Some(dev_path) = cartridge_id.strip_prefix("dev:") {
            let path = PathBuf::from(dev_path);
            if !path.exists() {
                return Err(ExecutionError::CartridgeExecutionFailed {
                    cap_urn: cartridge_id.to_string(),
                    details: format!("Dev binary not found: {:?}", path),
                });
            }
            return Ok(path);
        }

        // Look for an existing installed cartridge in the channel-partitioned
        // versioned layout: {cartridge_dir}/{channel}/{cartridge_id}/{version}/cartridge.json
        let name_dir = self
            .cartridge_dir
            .join(self.channel.as_str())
            .join(cartridge_id);
        if name_dir.is_dir() {
            if let Some(entry_point) = self.find_latest_installed_entry_point(&name_dir) {
                return Ok(entry_point);
            }
        }

        self.download_cartridge(cartridge_id).await
    }

    /// Find the entry point of the latest installed version in a cartridge name directory.
    fn find_latest_installed_entry_point(&self, name_dir: &Path) -> Option<PathBuf> {
        let mut versions: Vec<(String, PathBuf)> = Vec::new();

        for entry in fs::read_dir(name_dir).ok()? {
            let entry = entry.ok()?;
            let version_dir = entry.path();
            if !version_dir.is_dir() {
                continue;
            }
            match crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(&version_dir) {
                Ok(cj) => {
                    // Hard mismatch — never run a cartridge from a different
                    // channel even if it landed under our channel's tree.
                    if cj.channel != self.channel {
                        tracing::warn!(
                            "Skipping cartridge at {:?}: cartridge.json channel '{}' \
                             does not match orchestrator channel '{}'",
                            version_dir,
                            cj.channel,
                            self.channel
                        );
                        continue;
                    }
                    let entry_point = cj.resolve_entry_point(&version_dir);
                    versions.push((cj.version, entry_point));
                }
                Err(_) => continue,
            }
        }

        if versions.is_empty() {
            return None;
        }

        // Sort by version descending (latest first)
        versions.sort_by(|a, b| {
            let parts_a: Vec<u32> = a.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let parts_b: Vec<u32> = b.0.split('.').filter_map(|p| p.parse().ok()).collect();
            let max_len = parts_a.len().max(parts_b.len());
            for i in 0..max_len {
                let na = parts_a.get(i).copied().unwrap_or(0);
                let nb = parts_b.get(i).copied().unwrap_or(0);
                match nb.cmp(&na) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        Some(versions.into_iter().next()?.1)
    }

    async fn verify_cartridge_integrity(&self, cartridge_id: &str) -> Result<(), ExecutionError> {
        let cartridge_info = self
            .cartridge_repo
            .get_cartridge(self.channel, cartridge_id)
            .await
            .ok_or_else(|| ExecutionError::CartridgeNotFound {
                cap_urn: format!(
                    "Cartridge {} not found in {} registry",
                    cartridge_id, self.channel
                ),
            })?;

        if cartridge_info.team_id.is_empty() || cartridge_info.signed_at.is_empty() {
            return Err(ExecutionError::CartridgeExecutionFailed {
                cap_urn: cartridge_id.to_string(),
                details: format!(
                    "SECURITY: Cartridge {} is not signed. Refusing to execute.",
                    cartridge_id
                ),
            });
        }

        Ok(())
    }

    /// Download a cartridge package from the registry and install it into the
    /// versioned directory layout: {cartridge_dir}/{id}/{version}/cartridge.json + binary.
    async fn download_cartridge(&self, cartridge_id: &str) -> Result<PathBuf, ExecutionError> {
        let cartridge_info = self
            .cartridge_repo
            .get_cartridge(self.channel, cartridge_id)
            .await
            .ok_or_else(|| ExecutionError::CartridgeNotFound {
                cap_urn: format!(
                    "Cartridge {} not found in {} registry",
                    cartridge_id, self.channel
                ),
            })?;

        if cartridge_info.team_id.is_empty() || cartridge_info.signed_at.is_empty() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: Cartridge {} is not signed.",
                cartridge_id
            )));
        }

        // Find the build for this platform
        let platform = detect_platform();
        let build = cartridge_info
            .build_for_platform(&platform)
            .ok_or_else(|| {
                ExecutionError::CartridgeDownloadFailed(format!(
                    "Cartridge {} v{} has no build for platform '{}'. Available: {:?}",
                    cartridge_id,
                    cartridge_info.version,
                    platform,
                    cartridge_info.available_platforms()
                ))
            })?;

        let package = &build.package;

        // The v5 manifest carries the absolute R2 URL on the package itself.
        // No URL derivation: if the manifest's URL is wrong, we want to fail
        // hard against the URL the publisher actually committed to.
        let download_url = package.url.as_str();

        tracing::info!(
            "Downloading cartridge {} v{} ({}) from {}",
            cartridge_id,
            cartridge_info.version,
            platform,
            download_url
        );

        let response = reqwest::get(download_url).await.map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!("Download failed: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "HTTP {} from {}",
                response.status(),
                download_url
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| ExecutionError::CartridgeDownloadFailed(format!("Read failed: {}", e)))?
            .to_vec();

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let computed = format!("{:x}", hasher.finalize());

        if computed != package.sha256 {
            return Err(ExecutionError::CartridgeDownloadFailed(format!(
                "SECURITY: SHA256 mismatch for {}!\n  Expected: {}\n  Computed: {}",
                cartridge_id, package.sha256, computed
            )));
        }

        // Channel-partitioned versioned layout:
        // {cartridge_dir}/{channel}/{cartridge_id}/{version}/{binary} + cartridge.json
        let version_dir = self
            .cartridge_dir
            .join(self.channel.as_str())
            .join(cartridge_id)
            .join(&cartridge_info.version);
        fs::create_dir_all(&version_dir)?;

        let binary_name = cartridge_id;
        let binary_path = version_dir.join(binary_name);
        fs::write(&binary_path, &bytes)?;

        let mut perms = fs::metadata(&binary_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&binary_path, perms)?;

        // Write cartridge.json
        let cj = crate::CartridgeJson {
            name: cartridge_id.to_string(),
            version: cartridge_info.version.clone(),
            channel: self.channel,
            entry: binary_name.to_string(),
            installed_at: {
                use std::time::SystemTime;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("system clock before epoch");
                format!("{}Z", now.as_secs())
            },
            installed_from: crate::CartridgeInstallSource::Registry,
            source_url: download_url.to_string(),
            package_sha256: package.sha256.clone(),
            package_size: package.size,
        };
        cj.write_to_dir(&version_dir).map_err(|e| {
            ExecutionError::CartridgeDownloadFailed(format!(
                "Failed to write cartridge.json: {}",
                e
            ))
        })?;

        tracing::info!(
            "Installed cartridge {} v{} ({}) to {:?}",
            cartridge_id,
            cartridge_info.version,
            platform,
            version_dir
        );

        Ok(binary_path)
    }
}

// =============================================================================
// Execution Context — Arc<RelaySwitch> for concurrent DAG execution
// =============================================================================

/// Handle for cleanup of a master's associated resources.
struct MasterCleanupHandle {
    /// Task handles to abort after shutdown.
    task_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Execution context for DAG execution.
///
/// Each `ExecutionContext` is an isolated execution environment that:
/// - Shares the `RelaySwitch` via `Arc` for concurrent access
/// - Owns its own `node_data` HashMap (isolated per execution)
/// - Tracks cleanup handles for managed tasks
///
/// This design enables concurrent DAG executions:
/// - Multiple contexts can share the same switch
/// - Each context has isolated node data
/// - The switch handles concurrent frame routing internally
pub struct ExecutionContext {
    /// Shared relay switch (interior mutability)
    switch: Arc<RelaySwitch>,
    /// Raw bytes at each DAG node. Isolated per execution context.
    node_data: HashMap<String, Vec<u8>>,
    /// Per-node stream metadata. Carries provenance context (e.g. {"title": "page_3"})
    /// through ForEach splits so body caps receive the upstream item's metadata.
    node_meta: HashMap<String, crate::StreamMeta>,
    /// Tracks which nodes hold sequence data (CBOR sequence of items).
    /// When true, the node's data is an RFC 8742 CBOR sequence that should be
    /// sent with is_sequence=true on STREAM_START so the receiver gets
    /// properly framed per-item chunks.
    node_is_sequence: HashMap<String, bool>,
    /// Cached max chunk size from the relay.
    max_chunk: usize,
    /// Cleanup handles for masters added via add_cartridge_host.
    cleanup_handles: Vec<MasterCleanupHandle>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with an empty RelaySwitch.
    ///
    /// The RelaySwitch starts with no masters. Use `add_master()` or
    /// `add_cartridge_host()` to add masters before executing caps.
    ///
    /// Requires a CapRegistry for the RelaySwitch to use when building
    /// the LiveCapGraph for path finding queries.
    pub async fn new(cap_registry: Arc<CapRegistry>) -> Result<Self, ExecutionError> {
        let switch = RelaySwitch::new(vec![], cap_registry)
            .await
            .map_err(|e| ExecutionError::HostError(format!("RelaySwitch init: {}", e)))?;

        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        Ok(Self {
            switch: Arc::new(switch),
            node_data: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Create a new ExecutionContext from an existing shared RelaySwitch.
    ///
    /// This is used for concurrent DAG executions that share the same infrastructure.
    /// Each context has its own isolated node_data.
    pub async fn from_switch(switch: Arc<RelaySwitch>) -> Result<Self, ExecutionError> {
        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            max_chunk
        };

        Ok(Self {
            switch,
            node_data: HashMap::new(),
            node_meta: HashMap::new(),
            node_is_sequence: HashMap::new(),
            max_chunk,
            cleanup_handles: Vec::new(),
        })
    }

    /// Get the shared RelaySwitch.
    pub fn switch(&self) -> &Arc<RelaySwitch> {
        &self.switch
    }

    /// Add a master connection from an externally managed socket.
    ///
    /// The caller is responsible for the lifecycle of the connected endpoint
    /// (e.g., an InProcessCartridgeHost or external cartridge connection).
    ///
    /// Returns the master index on success.
    pub async fn add_master(&mut self, socket: UnixStream) -> Result<usize, ExecutionError> {
        let idx = self
            .switch
            .add_master(socket)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        self.update_max_chunk().await;
        Ok(idx)
    }

    /// Add a CartridgeHostRuntime as a master, spawning all required infrastructure.
    ///
    /// This creates:
    /// - CartridgeHostRuntime (async, in tokio task)
    /// - RelaySlave (async, in tokio task)
    /// - Socket pairs connecting them to the switch
    ///
    /// The ExecutionContext manages cleanup of these resources.
    ///
    /// # Arguments
    /// * `channel` - Distribution channel of every cartridge in this batch.
    ///   Channel partitions identity, so all cartridges that a single host
    ///   serves must share a channel.
    /// * `cartridges` - Vec of (binary_path, cap_urns) to register with the host
    pub async fn add_cartridge_host(
        &mut self,
        cartridges: Vec<(PathBuf, Vec<String>)>,
    ) -> Result<usize, ExecutionError> {
        // Create socket pairs:
        //   switch_sock <-> slave_ext_sock (switch to slave)
        //   slave_int_sock <-> host_sock (slave to host runtime)
        let (switch_sock, slave_ext_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;
        let (slave_int_sock, host_sock) = UnixStream::pair().map_err(ExecutionError::IoError)?;

        // --- CartridgeHostRuntime (async, in tokio task) ---
        // Channel is discovered from each cartridge's `cartridge.json`
        // provenance file written by pkg.sh at install time. The binary
        // sits at .../{channel}/{name}/{version}/{entry}; cartridge.json
        // sits in the same {version} directory. No cartridge.json → not
        // a properly-installed cartridge → refuse to register, rather
        // than silently merging release/nightly under a guessed channel.
        let mut host = CartridgeHostRuntime::new();
        for (path, caps) in &cartridges {
            let version_dir = path.parent().ok_or_else(|| {
                ExecutionError::HostError(format!(
                    "cartridge binary {} has no parent directory; cannot locate cartridge.json",
                    path.display()
                ))
            })?;
            let provenance =
                crate::bifaci::cartridge_json::CartridgeJson::read_from_dir(version_dir)
                    .map_err(|e| {
                        ExecutionError::HostError(format!(
                            "reading cartridge.json for {}: {}",
                            path.display(),
                            e
                        ))
                    })?;
            host.register_cartridge(path, provenance.channel, caps);
        }

        let (host_read, host_write) = host_sock.into_split();

        let host_handle = tokio::spawn(async move {
            if let Err(e) = host.run(host_read, host_write, || Vec::new()).await {
                tracing::error!("[CartridgeHostRuntime] Fatal: {}", e);
            }
        });

        // --- RelaySlave (async, in tokio task) ---
        let (slave_int_read, slave_int_write) = slave_int_sock.into_split();
        let slave = RelaySlave::new(
            BufReader::new(slave_int_read),
            BufWriter::new(slave_int_write),
        );

        // Initial caps: just CAP_IDENTITY for handshake verification.
        // CartridgeHostRuntime sends full caps via RelayNotify.
        let initial_caps_json = serde_json::to_vec(&RelayNotifyCapabilitiesPayload {
            caps: vec![CAP_IDENTITY.to_string()],
            installed_cartridges: vec![],
        })
        .map_err(|e| ExecutionError::HostError(format!("serialize caps: {}", e)))?;

        let (slave_ext_read, slave_ext_write) = slave_ext_sock.into_split();

        let slave_handle = tokio::spawn(async move {
            if let Err(e) = slave
                .run(
                    FrameReader::new(BufReader::new(slave_ext_read)),
                    FrameWriter::new(BufWriter::new(slave_ext_write)),
                    Some((&initial_caps_json, &Limits::default())),
                )
                .await
            {
                tracing::error!("[RelaySlave] Fatal: {}", e);
            }
        });

        // --- Add to switch ---
        let master_idx = self
            .switch
            .add_master(switch_sock)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        // Store cleanup handles
        self.cleanup_handles.push(MasterCleanupHandle {
            task_handles: vec![host_handle, slave_handle],
        });

        self.update_max_chunk().await;
        Ok(master_idx)
    }

    /// Update max_chunk from current switch limits.
    async fn update_max_chunk(&mut self) {
        let chunk = self.switch.limits().await.max_chunk as usize;
        self.max_chunk = if chunk == 0 {
            DEFAULT_MAX_CHUNK as usize
        } else {
            chunk
        };
    }

    /// Get the current max chunk size.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Get the aggregate capabilities of all connected masters.
    pub async fn capabilities(&self) -> Vec<u8> {
        self.switch.capabilities().await
    }

    /// Get the negotiated limits.
    pub async fn limits(&self) -> Limits {
        self.switch.limits().await
    }

    /// Set data for a node.
    pub fn set_node_data(&mut self, node: String, data: Vec<u8>) {
        self.node_data.insert(node, data);
    }

    /// Set stream metadata for a node (provenance context for ForEach propagation).
    pub fn set_node_meta(&mut self, node: String, meta: crate::StreamMeta) {
        self.node_meta.insert(node, meta);
    }

    /// Get stream metadata for a node.
    pub fn get_node_meta(&self, node: &str) -> Option<&crate::StreamMeta> {
        self.node_meta.get(node)
    }

    /// Get immutable reference to node_meta map.
    pub fn node_meta(&self) -> &HashMap<String, crate::StreamMeta> {
        &self.node_meta
    }

    /// Get mutable reference to node_meta map.
    pub fn node_meta_mut(&mut self) -> &mut HashMap<String, crate::StreamMeta> {
        &mut self.node_meta
    }

    /// Mark a node as holding sequence data.
    pub fn set_node_is_sequence(&mut self, node: String, is_sequence: bool) {
        self.node_is_sequence.insert(node, is_sequence);
    }

    /// Check if a node holds sequence data.
    pub fn is_node_sequence(&self, node: &str) -> bool {
        self.node_is_sequence.get(node).copied().unwrap_or(false)
    }

    /// Get the full node_is_sequence map.
    pub fn node_is_sequence(&self) -> &HashMap<String, bool> {
        &self.node_is_sequence
    }

    /// Get data for a node.
    pub fn get_node_data(&self, node: &str) -> Option<&Vec<u8>> {
        self.node_data.get(node)
    }

    /// Get immutable reference to node_data map.
    pub fn node_data(&self) -> &HashMap<String, Vec<u8>> {
        &self.node_data
    }

    /// Get mutable reference to node_data map.
    pub fn node_data_mut(&mut self) -> &mut HashMap<String, Vec<u8>> {
        &mut self.node_data
    }

    /// Consume and return the node_data map.
    pub fn into_node_data(self) -> HashMap<String, Vec<u8>> {
        // Abort all managed tasks
        for handle in self.cleanup_handles {
            for task in handle.task_handles {
                task.abort();
            }
        }
        self.node_data
    }

    /// Shut down the infrastructure and return accumulated node data.
    ///
    /// This:
    /// 1. Drops the switch reference (may or may not release the switch)
    /// 2. Aborts all managed tasks
    ///
    /// For masters added via `add_master()`, the caller is responsible for
    /// shutting down their endpoints.
    pub fn shutdown(self) -> HashMap<String, Vec<u8>> {
        self.into_node_data()
    }

    /// Execute an edge group as a single cap invocation with one or more input streams.
    ///
    /// All edges in the group share the same `(to, cap_urn)`. Each edge contributes
    /// one input stream using its `from` node's data and its own `in_media` URN.
    /// The cap handler receives all streams and decides when/how to process them.
    ///
    /// Additional arguments can be provided via `extra_args` - these are sent as
    /// additional input streams with the specified media_urn. This is used for
    /// slot values, cap settings, etc. that don't flow through edges.
    ///
    /// Protocol:
    ///   REQ(cap_urn)
    ///   STREAM_START(stream_id_1, in_media_1) + CHUNK... + STREAM_END(stream_id_1)
    ///   STREAM_START(stream_id_2, in_media_2) + CHUNK... + STREAM_END(stream_id_2)
    ///   ...
    ///   END
    ///   → collect response chunks → decode CBOR → store at `to` node
    pub async fn execute_fanin(
        &mut self,
        edges: &[ResolvedEdge],
        extra_args: &[(String, Vec<u8>)],
        progress_fn: Option<&CapProgressFn>,
    ) -> Result<(), ExecutionError> {
        assert!(
            !edges.is_empty(),
            "execute_fanin requires at least one edge"
        );

        let cap_urn = &edges[0].cap_urn;
        let to = &edges[0].to;

        let activity_timeout_secs = edges[0]
            .cap
            .metadata
            .get(ACTIVITY_TIMEOUT_METADATA_KEY)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_ACTIVITY_TIMEOUT_SECS);

        let total_streams = edges.len() + extra_args.len();
        tracing::debug!(target: "execute_fanin", "cap={} streams={} to={}", cap_urn, total_streams, to);
        tracing::info!(
            "Executing cap: {} ({} input stream(s) -> {})",
            cap_urn,
            total_streams,
            to
        );

        // Collect all input data upfront — fail fast if any source is missing
        let mut inputs: Vec<(&[u8], &str, bool)> = Vec::new();
        for edge in edges {
            tracing::debug!(target: "execute_fanin", "Edge: {} -> {} (in_media={})", edge.from, edge.to, edge.in_media);
            let data = self
                .node_data
                .get(&edge.from)
                .ok_or_else(|| ExecutionError::NoIncomingData {
                    node: edge.from.clone(),
                })?;
            let is_seq = self
                .node_is_sequence
                .get(&edge.from)
                .copied()
                .unwrap_or(false);
            inputs.push((data.as_slice(), edge.in_media.as_str(), is_seq));
        }
        tracing::debug!(target: "execute_fanin", "Collected {} inputs + {} extra args", inputs.len(), extra_args.len());

        // Open ONE cap invocation for all inputs
        tracing::debug!(target: "execute_fanin", "Calling execute_cap...");
        let (request_id, mut rx) = self
            .switch
            .execute_cap(cap_urn, vec![], "application/cbor")
            .await
            .map_err(|e| ExecutionError::HostError(format!("execute_cap: {}", e)))?;
        tracing::info!(
            "[execute_fanin] dispatched cap='{}' request_id={:?}",
            cap_urn,
            request_id
        );

        // Send each input as a separate named stream using shared stream I/O
        for (data, in_media, is_seq) in &inputs {
            super::stream_io::send_one_stream(
                &self.switch,
                &request_id,
                in_media,
                data,
                None,
                *is_seq,
                self.max_chunk,
            )
            .await
            .map_err(|e| ExecutionError::HostError(format!("{}", e)))?;
        }
        // Extra args are always scalar
        for (media_urn, data) in extra_args {
            super::stream_io::send_one_stream(
                &self.switch,
                &request_id,
                media_urn,
                data,
                None,
                false,
                self.max_chunk,
            )
            .await
            .map_err(|e| ExecutionError::HostError(format!("{}", e)))?;
        }

        // END — no more input streams
        tracing::debug!(target: "execute_fanin", "Sending END frame...");
        let end_frame = Frame::end(request_id.clone(), None);
        self.switch
            .send_to_master(end_frame, None)
            .await
            .map_err(|e| ExecutionError::HostError(format!("END: {}", e)))?;
        tracing::debug!(target: "execute_fanin", "END frame sent, waiting for response...");

        // Spawn a pump task that drains `read_from_masters_timeout` so peer
        // requests get routed while we wait for this cap's terminal frames.
        // Without the pump, cartridge→cartridge peer calls deadlock. The
        // pump exits via the stop flag — never via `abort()`, which can
        // drop frames mid-route.
        let pump_stop = Arc::new(AtomicBool::new(false));
        let pump_stop_flag = pump_stop.clone();
        let pump_switch = self.switch.clone();
        let pump_handle = tokio::spawn(async move {
            loop {
                if pump_stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                match pump_switch
                    .read_from_masters_timeout(Duration::from_millis(200))
                    .await
                {
                    Ok(Some(_frame)) => {
                        // Routed internally by handle_master_frame — nothing to do.
                    }
                    Ok(None) => {
                        // Timeout — loop again (also checks stop flag).
                    }
                    Err(e) => {
                        // Per-frame relay errors aren't fatal to the pump.
                        // Log and continue so other requests keep routing.
                        tracing::warn!("[executor pump] relay error (continuing): {}", e);
                    }
                }
            }
        });

        // Delegate the terminal-collect loop to the shared `stream_io`
        // implementation used by the machfab engine. This is the single
        // source of truth for frame decoding, activity timeouts, progress
        // callbacks, and END/ERR handling.
        let collect_result = super::stream_io::collect_terminal_output(
            rx,
            progress_fn,
            cap_urn,
            None,
            None,
            None,
            None,
            activity_timeout_secs,
            &self.switch,
            &request_id,
        )
        .await;

        // Stop the pump before returning — either on success or error.
        pump_stop.store(true, Ordering::Relaxed);
        let _ = pump_handle.await;

        let (response_chunks, is_sequence, _terminal_meta) =
            collect_result.map_err(|e| match e {
                StreamIoError::ActivityTimeout {
                    cap_urn,
                    idle_secs,
                    limit_secs,
                } => ExecutionError::ActivityTimeout {
                    cap_urn,
                    idle_secs,
                    limit_secs,
                },
                StreamIoError::Terminal { cap_urn, details } => {
                    ExecutionError::CartridgeExecutionFailed { cap_urn, details }
                }
                other => ExecutionError::HostError(format!("{}", other)),
            })?;

        tracing::info!(
            "[execute_fanin] got End for cap='{}' request_id={:?} response_len={}",
            cap_urn,
            request_id,
            response_chunks.len()
        );

        // Decode response using shared stream I/O (matches machfab engine behavior).
        // Unwraps CBOR transport wrappers so node_data always contains raw bytes.
        let decoded_items =
            super::stream_io::decode_terminal_output(&response_chunks, is_sequence)
                .map_err(|e| ExecutionError::HostError(format!("{}", e)))?;

        if is_sequence == Some(true) {
            tracing::debug!(
                target: "execute_fanin",
                "Sequence output ({}): {} items decoded",
                edges[0].out_media, decoded_items.len()
            );
            // Re-encode as CBOR sequence for storage: each unwrapped item
            // becomes a CBOR Bytes value so the sequence remains self-delimiting.
            let mut cbor_seq = Vec::new();
            for item in &decoded_items {
                let cbor_value = ciborium::Value::Bytes(item.clone());
                ciborium::into_writer(&cbor_value, &mut cbor_seq).map_err(|e| {
                    ExecutionError::HostError(format!("CBOR re-encode sequence item: {}", e))
                })?;
            }
            self.node_data.insert(to.clone(), cbor_seq);
            self.node_is_sequence.insert(to.clone(), true);
        } else {
            // Scalar: decoded_items has one entry with concatenated raw bytes
            let output_bytes = decoded_items.into_iter().next().unwrap_or_default();
            self.node_data.insert(to.clone(), output_bytes);
        }
        Ok(())
    }
}

// =============================================================================
// DAG Executor
// =============================================================================

/// Execute a resolved DAG: discover cartridges, set up infrastructure, run edge groups.
pub async fn execute_dag(
    graph: &ResolvedGraph,
    cartridge_dir: PathBuf,
    registry_url: String,
    channel: crate::bifaci::cartridge_repo::CartridgeChannel,
    initial_inputs: HashMap<String, NodeData>,
    dev_binaries: Vec<PathBuf>,
    cap_registry: Arc<CapRegistry>,
    progress_fn: Option<&CapProgressFn>,
    node_values: &HashMap<String, HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, NodeData>, ExecutionError> {
    tracing::debug!(target: "execute_dag", "Starting...");

    // 1. Initialize cartridge manager and discover/download all needed cartridges
    let mut cartridge_manager =
        CartridgeManager::new(cartridge_dir, registry_url, channel, dev_binaries);
    cartridge_manager.init().await?;
    tracing::debug!(target: "execute_dag", "Cartridge manager initialized");

    let cap_urns: Vec<&str> = graph.edges.iter().map(|e| e.cap_urn.as_str()).collect();
    let cartridges = cartridge_manager.resolve_cartridges(&cap_urns).await?;
    tracing::debug!(target: "execute_dag", "Resolved {} cartridges", cartridges.len());

    tracing::info!("Resolved {} unique cartridge binaries:", cartridges.len());
    for (path, caps) in &cartridges {
        tracing::info!("  {:?} -> {} caps", path, caps.len());
    }

    // 2. Create execution context and add cartridge host as master
    tracing::debug!(target: "execute_dag", "Creating execution context...");
    let mut ctx = ExecutionContext::new(cap_registry).await?;
    tracing::debug!(target: "execute_dag", "Adding cartridge host...");
    // Channel is discovered per-cartridge from each cartridge.json
    // inside add_cartridge_host; the scope-level `channel` parameter
    // above only governs which channel's cartridges CartridgeManager
    // resolves in the first place.
    ctx.add_cartridge_host(cartridges).await?;
    tracing::debug!(target: "execute_dag", "Cartridge host added");

    // 3. Resolve initial inputs to raw bytes and set on nodes
    for (node, data) in initial_inputs {
        let bytes = data.into_bytes().await?;
        ctx.set_node_data(node, bytes);
    }
    tracing::debug!(target: "execute_dag", "Initial inputs set");

    // 4. Group edges by (to, cap_urn) to detect fan-in, then sort groups topologically.
    //    Fan-in groups are executed as ONE cap invocation with multiple input streams —
    //    the handler decides how to handle each stream as it arrives.
    let groups = build_edge_groups(&graph.edges);
    let group_order = topological_sort_groups(&groups)
        .map_err(|e| ExecutionError::HostError(format!("Topological sort failed: {}", e)))?;
    let n_groups = group_order.len();
    tracing::debug!(target: "execute_dag", "{} edge groups to execute", n_groups);

    tracing::info!("Executing {} cap group(s) in topological order", n_groups);

    // Pre-compute group boundaries for deterministic progress subdivision
    let group_boundaries: Vec<f32> = if n_groups > 0 {
        (0..=n_groups).map(|i| i as f32 / n_groups as f32).collect()
    } else {
        vec![0.0]
    };

    // Execute groups in topological order
    for (i, idx) in group_order.iter().enumerate() {
        tracing::debug!(target: "execute_dag", "Executing group {}/{}: cap={}", i+1, n_groups, groups[*idx].edges[0].cap_urn);

        // Per-group progress subdivision
        let group_pfn: Option<CapProgressFn> = progress_fn.map(|parent| {
            let base = group_boundaries[i];
            let weight = group_boundaries[i + 1] - base;
            ProgressMapper::new(parent, base, weight).as_cap_progress_fn()
        });

        // Resolve extra args from node_values for this group's target node.
        // Only explicitly provided values are sent — default values are the
        // cartridge's responsibility per the cap contract.
        let extra_args: Vec<(String, Vec<u8>)> = match node_values.get(&groups[*idx].to) {
            Some(args) => {
                let mut resolved = Vec::with_capacity(args.len());
                for (media_urn, value) in args {
                    let bytes = match value {
                        serde_json::Value::String(s) => s.as_bytes().to_vec(),
                        other => serde_json::to_vec(other).map_err(|e| {
                            ExecutionError::HostError(format!(
                                "Failed to serialize node_values value for arg '{}' on node '{}': {}",
                                media_urn, groups[*idx].to, e
                            ))
                        })?,
                    };
                    resolved.push((media_urn.clone(), bytes));
                }
                resolved
            }
            None => Vec::new(),
        };

        ctx.execute_fanin(&groups[*idx].edges, &extra_args, group_pfn.as_ref())
            .await?;

        // Report group completion
        if let Some(pfn) = &progress_fn {
            pfn(
                group_boundaries[i + 1],
                &groups[*idx].edges[0].cap_urn,
                "Completed",
            );
        }

        tracing::debug!(target: "execute_dag", "Group {} complete", i+1);
    }

    tracing::info!("\nExecution complete!\n");

    // Explicitly shut down infrastructure
    let node_data = ctx.shutdown();

    // Convert back to NodeData for the public API
    let result: HashMap<String, NodeData> = node_data
        .into_iter()
        .map(|(k, v)| (k, NodeData::Bytes(v)))
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // TEST1125: map_progress clamps child to [0.0, 1.0] and maps to [base, base+weight]
    #[test]
    fn test1125_map_progress_basic_mapping() {
        // Identity mapping: base=0, weight=1
        assert_eq!(map_progress(0.0, 0.0, 1.0), 0.0);
        assert_eq!(map_progress(0.5, 0.0, 1.0), 0.5);
        assert_eq!(map_progress(1.0, 0.0, 1.0), 1.0);

        // Subdivision: base=0.2, weight=0.6 → range [0.2, 0.8]
        assert_eq!(map_progress(0.0, 0.2, 0.6), 0.2);
        assert_eq!(map_progress(0.5, 0.2, 0.6), 0.5);
        assert_eq!(map_progress(1.0, 0.2, 0.6), 0.8);

        // Clamping: values outside [0, 1] are clamped before mapping
        assert_eq!(map_progress(-0.5, 0.2, 0.6), 0.2); // clamp to 0 → base
        assert_eq!(map_progress(1.5, 0.2, 0.6), 0.8); // clamp to 1 → base+weight
    }

    // TEST1126: map_progress is deterministic — same inputs always produce same output
    #[test]
    fn test1126_map_progress_deterministic() {
        for i in 0..100 {
            let p = i as f32 / 100.0;
            let a = map_progress(p, 0.1, 0.8);
            let b = map_progress(p, 0.1, 0.8);
            assert_eq!(a, b, "map_progress must be deterministic for p={}", p);
        }
    }

    // TEST910: map_progress output is monotonic for monotonically increasing input
    #[test]
    fn test910_map_progress_monotonic() {
        let mut prev = map_progress(0.0, 0.1, 0.7);
        for i in 1..=100 {
            let p = i as f32 / 100.0;
            let curr = map_progress(p, 0.1, 0.7);
            assert!(
                curr >= prev,
                "map_progress must be monotonic: p={}, prev={}, curr={}",
                p,
                prev,
                curr
            );
            prev = curr;
        }
    }

    // TEST911: map_progress output is bounded within [base, base+weight]
    #[test]
    fn test911_map_progress_bounded() {
        let base = 0.15;
        let weight = 0.55;
        for i in -10..=110 {
            let p = i as f32 / 100.0;
            let result = map_progress(p, base, weight);
            assert!(
                result >= base && result <= base + weight,
                "map_progress({}, {}, {}) = {} must be in [{}, {}]",
                p,
                base,
                weight,
                result,
                base,
                base + weight
            );
        }
    }

    // TEST912: ProgressMapper correctly maps through a CapProgressFn
    #[test]
    fn test912_progress_mapper_reports_through_parent() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, msg: &str| {
            reported_clone.lock().unwrap().push((p, msg.to_string()));
        });

        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);
        mapper.report(0.0, "", "start");
        mapper.report(0.5, "", "half");
        mapper.report(1.0, "", "done");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0].0 - 0.2).abs() < 0.001, "0% maps to base=0.2");
        assert!((reports[1].0 - 0.5).abs() < 0.001, "50% maps to 0.5");
        assert!(
            (reports[2].0 - 0.8).abs() < 0.001,
            "100% maps to base+weight=0.8"
        );
    }

    // TEST913: ProgressMapper.as_cap_progress_fn produces same mapping
    #[test]
    fn test913_progress_mapper_as_cap_progress_fn() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.3);
        let pfn = mapper.as_cap_progress_fn();

        pfn(0.0, "", "a");
        pfn(0.5, "", "b");
        pfn(1.0, "", "c");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert!((reports[0] - 0.1).abs() < 0.001);
        assert!((reports[1] - 0.25).abs() < 0.001);
        assert!((reports[2] - 0.4).abs() < 0.001);
    }

    // TEST914: ProgressMapper.sub_mapper chains correctly
    #[test]
    fn test914_progress_mapper_sub_mapper() {
        let reported = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reported_clone = Arc::clone(&reported);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            reported_clone.lock().unwrap().push(p);
        });

        // Parent maps [0, 1] to [0.2, 0.8] (base=0.2, weight=0.6)
        let mapper = ProgressMapper::new(&parent, 0.2, 0.6);

        // Sub-mapper maps [0, 1] to the second half of parent's range
        // sub_base=0.5, sub_weight=0.5 → [0.2 + 0.5*0.6, 0.2 + (0.5+0.5)*0.6] = [0.5, 0.8]
        let sub = mapper.sub_mapper(0.5, 0.5);
        sub.report(0.0, "", "sub_start");
        sub.report(1.0, "", "sub_end");

        let reports = reported.lock().unwrap();
        assert_eq!(reports.len(), 2);
        assert!((reports[0] - 0.5).abs() < 0.001, "sub 0% maps to 0.5");
        assert!((reports[1] - 0.8).abs() < 0.001, "sub 100% maps to 0.8");
    }

    // TEST915: Per-group subdivision produces monotonic, bounded progress for N groups
    //
    // Uses pre-computed boundaries (same pattern as production code) to guarantee
    // monotonicity regardless of f32 rounding.
    #[test]
    fn test915_per_group_subdivision_monotonic_bounded() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        let n_groups: usize = 5;
        let boundaries: Vec<f32> = (0..=n_groups).map(|i| i as f32 / n_groups as f32).collect();

        for i in 0..n_groups {
            let base = boundaries[i];
            let weight = boundaries[i + 1] - base;
            let mapper = ProgressMapper::new(&parent, base, weight);

            // Each group reports 0%, 50%, 100%
            mapper.report(0.0, "", "start");
            mapper.report(0.5, "", "half");
            mapper.report(1.0, "", "done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 15); // 5 groups * 3 reports

        // Verify monotonicity
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }

        // Verify bounded [0.0, 1.0]
        for (i, &p) in progress.iter().enumerate() {
            assert!(
                p >= 0.0 && p <= 1.0,
                "Progress[{}]={} must be in [0.0, 1.0]",
                i,
                p
            );
        }

        // First should be 0.0 (group 0, 0%)
        assert!((progress[0] - 0.0).abs() < 0.001);
        // Last should be 1.0 (group 4, 100%)
        assert!((progress[14] - 1.0).abs() < 0.001);
    }

    // TEST916: ForEach item subdivision produces correct, monotonic ranges
    //
    // Mirrors the production code in interpreter.rs: pre-compute item boundaries
    // from the same formula so the end of item N and the start of item N+1 are
    // the same f32 value (no divergent accumulation paths).
    #[test]
    fn test916_foreach_item_subdivision() {
        let all_progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let all_clone = Arc::clone(&all_progress);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            all_clone.lock().unwrap().push(p);
        });

        // ForEach: prefix [0.0, 0.05), body [0.05, 0.95), suffix [0.95, 1.0)
        let body_base = 0.05_f32;
        let body_weight = 0.90_f32;
        let item_count: usize = 4;

        // Pre-compute boundaries from a single formula — same as production code
        let item_boundaries: Vec<f32> = (0..=item_count)
            .map(|i| body_base + body_weight * (i as f32 / item_count as f32))
            .collect();

        for i in 0..item_count {
            let item_base = item_boundaries[i];
            let item_weight = item_boundaries[i + 1] - item_base;
            let mapper = ProgressMapper::new(&parent, item_base, item_weight);

            // Each item reports 0% and 100%
            mapper.report(0.0, "", "item_start");
            mapper.report(1.0, "", "item_done");
        }

        let progress = all_progress.lock().unwrap();
        assert_eq!(progress.len(), 8); // 4 items * 2 reports

        // Item 0 start: body_base = 0.05
        assert!(
            (progress[0] - 0.05).abs() < 0.01,
            "item 0 start: got {}",
            progress[0]
        );
        // Item 0 end: boundary[1] = 0.05 + 0.90 * 0.25 = 0.275
        assert!(
            (progress[1] - 0.275).abs() < 0.01,
            "item 0 end: got {}",
            progress[1]
        );
        // Item 3 end: boundary[4] = 0.05 + 0.90 * 1.0 = 0.95
        assert!(
            (progress[7] - 0.95).abs() < 0.01,
            "item 3 end: got {}",
            progress[7]
        );

        // All monotonic — this is the core invariant
        for i in 1..progress.len() {
            assert!(
                progress[i] >= progress[i - 1],
                "monotonic violation at index {}: {} < {}",
                i,
                progress[i],
                progress[i - 1]
            );
        }
    }

    // TEST917: High-frequency progress emission does not violate bounds
    // (Regression test for the deadlock scenario — verifies computation stays bounded)
    #[test]
    fn test917_high_frequency_progress_bounded() {
        let count = Arc::new(AtomicU32::new(0));
        let max_val = Arc::new(std::sync::Mutex::new(f32::MIN));
        let min_val = Arc::new(std::sync::Mutex::new(f32::MAX));

        let count_clone = Arc::clone(&count);
        let max_clone = Arc::clone(&max_val);
        let min_clone = Arc::clone(&min_val);
        let parent: CapProgressFn = Arc::new(move |p: f32, _cap: &str, _msg: &str| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            let mut max = max_clone.lock().unwrap();
            if p > *max {
                *max = p;
            }
            let mut min = min_clone.lock().unwrap();
            if p < *min {
                *min = p;
            }
        });

        let mapper = ProgressMapper::new(&parent, 0.1, 0.8);

        // Simulate 100,000 rapid progress updates (like model download without throttle)
        for i in 0..100_000 {
            let p = i as f32 / 100_000.0;
            mapper.report(p, "", "downloading");
        }

        assert_eq!(count.load(Ordering::Relaxed), 100_000);
        let min = *min_val.lock().unwrap();
        let max = *max_val.lock().unwrap();
        assert!(min >= 0.1, "min {} must be >= base 0.1", min);
        assert!(max <= 0.9, "max {} must be <= base+weight 0.9", max);
    }

    // TEST918: ActivityTimeout error formats correctly
    #[test]
    fn test918_activity_timeout_error_display() {
        let err = ExecutionError::ActivityTimeout {
            cap_urn: "cap:op=describe_image".to_string(),
            idle_secs: 125,
            limit_secs: 120,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Activity timeout"), "msg: {}", msg);
        assert!(msg.contains("cap:op=describe_image"), "msg: {}", msg);
        assert!(msg.contains("125s"), "msg: {}", msg);
        assert!(msg.contains("120s"), "msg: {}", msg);
    }
}
