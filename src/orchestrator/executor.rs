//! DAG Execution Engine
//!
//! Executes a resolved DOT DAG by:
//! 1. Discovering and downloading plugins that provide the required caps
//! 2. Connecting all plugins to a single PluginHostRuntime
//! 3. Routing cap requests through a RelaySwitch
//! 4. Executing edge groups in topological order, streaming frames between caps
//!
//! Fan-in: multiple edges pointing to the same `(to, cap_urn)` are grouped and
//! executed as ONE cap invocation with multiple input streams. The plugin handler
//! receives all streams and decides how to handle partial availability — it may
//! wait for all, use whatever arrives, or fail.
//!
//! Architecture:
//! ```text
//!   macino ←→ RelaySwitch ←→ RelaySlave ←→ PluginHostRuntime ←→ Plugin A
//!                                                             ←→ Plugin B
//!                                                             ←→ Plugin C
//! ```

use super::types::{ResolvedEdge, ResolvedGraph};
use crate::{
    Frame, FrameType, FrameReader, FrameWriter, Limits,
    PluginHostRuntime, RelaySlave, RelaySwitch, PluginRepo,
    CapManifest, CapUrn, handshake, DEFAULT_MAX_CHUNK,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::mpsc;

/// Cap URN for the identity capability (always available from any plugin runtime).
const CAP_IDENTITY: &str = "cap:";

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Plugin not found for cap: {cap_urn}")]
    PluginNotFound { cap_urn: String },

    #[error("Plugin download failed: {0}")]
    PluginDownloadFailed(String),

    #[error("Plugin execution failed for cap {cap_urn}: {details}")]
    PluginExecutionFailed { cap_urn: String, details: String },

    #[error("Node {node} has no incoming data")]
    NoIncomingData { node: String },

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Host error: {0}")]
    HostError(String),

    #[error("Registry error: {0}")]
    RegistryError(String),

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
            NodeData::FilePath(path) => {
                tokio::fs::read(&path).await.map_err(|e| {
                    ExecutionError::HostError(format!(
                        "Failed to read file '{}': {}", path.display(), e
                    ))
                })
            }
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
        return Err(ExecutionError::PluginExecutionFailed {
            cap_urn: String::new(),
            details: "Cycle detected in graph".to_string(),
        });
    }

    Ok(sorted)
}

// =============================================================================
// Plugin Manager
// =============================================================================

/// Manages plugin discovery, download, and caching.
pub struct PluginManager {
    plugin_repo: PluginRepo,
    plugin_dir: PathBuf,
    registry_url: String,
    dev_plugins: HashMap<PathBuf, CapManifest>,
}

impl PluginManager {
    pub fn new(plugin_dir: PathBuf, registry_url: String, dev_binaries: Vec<PathBuf>) -> Self {
        Self {
            plugin_repo: PluginRepo::new(3600),
            plugin_dir,
            registry_url,
            dev_plugins: dev_binaries
                .into_iter()
                .map(|p| {
                    (p, CapManifest::new(
                        String::new(), String::new(), String::new(), vec![],
                    ))
                })
                .collect(),
        }
    }

    pub async fn init(&mut self) -> Result<(), ExecutionError> {
        fs::create_dir_all(&self.plugin_dir)?;

        for (bin_path, _) in &self.dev_plugins.clone() {
            tracing::info!("[DevMode] Discovering manifest from {:?}...", bin_path);
            match self.discover_manifest(bin_path).await {
                Ok(manifest) => {
                    tracing::info!("[DevMode] Plugin: {}", manifest.name);
                    for cap in &manifest.caps {
                        tracing::info!("[DevMode]   - {}", cap.urn);
                    }
                    self.dev_plugins.insert(bin_path.clone(), manifest);
                }
                Err(e) => {
                    tracing::error!("[DevMode] Failed: {:?}: {}", bin_path, e);
                    return Err(e);
                }
            }
        }

        self.plugin_repo
            .sync_repos(&[self.registry_url.clone()])
            .await;

        Ok(())
    }

    async fn discover_manifest(
        &self,
        bin_path: &Path,
    ) -> Result<CapManifest, ExecutionError> {

        let mut child = Command::new(bin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ExecutionError::PluginExecutionFailed {
                cap_urn: "manifest-discovery".to_string(),
                details: format!("Failed to spawn plugin: {}", e),
            })?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut reader = FrameReader::new(stdout);
        let mut writer = FrameWriter::new(stdin);

        let result = handshake(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;

        let manifest: CapManifest =
            serde_json::from_slice(&result.manifest)
                .map_err(|e| ExecutionError::HostError(format!("Bad manifest: {}", e)))?;

        let _ = child.kill().await;
        Ok(manifest)
    }

    /// Resolve all cap URNs from the graph to unique (binary_path, known_caps) pairs.
    ///
    /// For dev plugins (with discovered manifests), registers ALL manifest caps —
    /// not just the DAG edge caps. This is critical because plugins send peer requests
    /// for caps that aren't in the DAG (e.g., candlecartridge peer-invokes modelcartridge's
    /// download-model cap during ML inference). Without full cap registration, the
    /// PluginHostRuntime can't route these peer requests.
    pub async fn resolve_plugins(
        &self,
        cap_urns: &[&str],
    ) -> Result<Vec<(PathBuf, Vec<String>)>, ExecutionError> {
        // Collect unique plugin binaries needed for the DAG
        let mut plugin_paths: HashSet<PathBuf> = HashSet::new();

        for &cap_urn in cap_urns {
            let (bin_path, _plugin_id) = self.find_plugin_binary(cap_urn).await?;
            plugin_paths.insert(bin_path);
        }

        // Also include ALL dev plugin binaries — they may be needed for peer request
        // routing even if they don't directly appear in the DAG. For example, ML
        // cartridges send peer requests to modelcartridge for model downloading.
        for dev_path in self.dev_plugins.keys() {
            plugin_paths.insert(dev_path.clone());
        }

        // For each plugin, register ALL manifest caps (not just DAG caps)
        let result: Vec<(PathBuf, Vec<String>)> = plugin_paths
            .into_iter()
            .map(|path| {
                let mut caps: HashSet<String> = HashSet::new();

                // Use full manifest caps for dev plugins
                if let Some(manifest) = self.dev_plugins.get(&path) {
                    for cap in &manifest.caps {
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
    async fn find_plugin_binary(
        &self,
        cap_urn: &str,
    ) -> Result<(PathBuf, String), ExecutionError> {
        let requested_urn = CapUrn::from_string(cap_urn).map_err(|e| {
            ExecutionError::PluginNotFound {
                cap_urn: format!("Invalid URN: {}: {}", cap_urn, e),
            }
        })?;

        // Check dev plugins first - use is_dispatchable to find any plugin
        // that can legally handle the requested cap.
        for (bin_path, manifest) in &self.dev_plugins {
            for cap in &manifest.caps {
                // cap.urn is the provider, requested_urn is the request
                if cap.urn.is_dispatchable(&requested_urn) {
                    return Ok((bin_path.clone(), format!("dev:{}", bin_path.display())));
                }
            }
        }

        // Fall back to registry
        let suggestions = self.plugin_repo.get_suggestions_for_cap(cap_urn).await;
        if suggestions.is_empty() {
            return Err(ExecutionError::PluginNotFound {
                cap_urn: cap_urn.to_string(),
            });
        }

        let plugin_id = &suggestions[0].plugin_id;
        let bin_path = self.get_plugin_path(plugin_id).await?;
        Ok((bin_path, plugin_id.clone()))
    }

    pub async fn get_plugin_path(&self, plugin_id: &str) -> Result<PathBuf, ExecutionError> {
        if let Some(dev_path) = plugin_id.strip_prefix("dev:") {
            let path = PathBuf::from(dev_path);
            if !path.exists() {
                return Err(ExecutionError::PluginExecutionFailed {
                    cap_urn: plugin_id.to_string(),
                    details: format!("Dev binary not found: {:?}", path),
                });
            }
            return Ok(path);
        }

        let plugin_path = self.plugin_dir.join(plugin_id);

        if plugin_path.exists() {
            self.verify_plugin_integrity(plugin_id, &plugin_path).await?;
            return Ok(plugin_path);
        }

        self.download_plugin(plugin_id).await?;
        Ok(plugin_path)
    }

    async fn verify_plugin_integrity(
        &self,
        plugin_id: &str,
        plugin_path: &Path,
    ) -> Result<(), ExecutionError> {
        let plugin_info = self.plugin_repo.get_plugin(plugin_id).await.ok_or_else(|| {
            ExecutionError::PluginNotFound {
                cap_urn: format!("Plugin {} not found in registry", plugin_id),
            }
        })?;

        if plugin_info.team_id.is_empty() || plugin_info.signed_at.is_empty() {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: Plugin {} is not signed. Refusing to execute.",
                    plugin_id
                ),
            });
        }

        if plugin_info.binary_sha256.is_empty() {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: Plugin {} has no SHA256 hash. Cannot verify.",
                    plugin_id
                ),
            });
        }

        let bytes = fs::read(plugin_path)?;
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let computed = format!("{:x}", hasher.finalize());

        if computed != plugin_info.binary_sha256 {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: SHA256 mismatch for {}!\n  Expected: {}\n  Computed: {}",
                    plugin_id, plugin_info.binary_sha256, computed
                ),
            });
        }

        Ok(())
    }

    async fn download_plugin(&self, plugin_id: &str) -> Result<(), ExecutionError> {
        let plugin_info = self.plugin_repo.get_plugin(plugin_id).await.ok_or_else(|| {
            ExecutionError::PluginNotFound {
                cap_urn: format!("Plugin {} not found in registry", plugin_id),
            }
        })?;

        if plugin_info.team_id.is_empty() || plugin_info.signed_at.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: Plugin {} is not signed.",
                plugin_id
            )));
        }

        if plugin_info.binary_name.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "Plugin {} has no binary available",
                plugin_id
            )));
        }

        if plugin_info.binary_sha256.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: Plugin {} has no SHA256 hash.",
                plugin_id
            )));
        }

        let base_url = self
            .registry_url
            .trim_end_matches("/api/plugins")
            .trim_end_matches('/');
        let download_url = format!("{}/plugins/binaries/{}", base_url, plugin_info.binary_name);

        tracing::info!(
            "Downloading plugin {} v{} from {}",
            plugin_id, plugin_info.version, download_url
        );

        let response = reqwest::get(&download_url)
            .await
            .map_err(|e| ExecutionError::PluginDownloadFailed(format!("Download failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "HTTP {} from {}",
                response.status(),
                download_url
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| ExecutionError::PluginDownloadFailed(format!("Read failed: {}", e)))?
            .to_vec();

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let computed = format!("{:x}", hasher.finalize());

        if computed != plugin_info.binary_sha256 {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: SHA256 mismatch for {}!\n  Expected: {}\n  Computed: {}",
                plugin_id, plugin_info.binary_sha256, computed
            )));
        }

        let plugin_path = self.plugin_dir.join(plugin_id);
        fs::write(&plugin_path, bytes)?;

        let mut perms = fs::metadata(&plugin_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&plugin_path, perms)?;

        tracing::info!(
            "Installed plugin {} v{} to {:?}",
            plugin_id, plugin_info.version, plugin_path
        );

        Ok(())
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
    /// Cached max chunk size from the relay.
    max_chunk: usize,
    /// Cleanup handles for masters added via add_plugin_host.
    cleanup_handles: Vec<MasterCleanupHandle>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with an empty RelaySwitch.
    ///
    /// The RelaySwitch starts with no masters. Use `add_master()` or
    /// `add_plugin_host()` to add masters before executing caps.
    pub async fn new() -> Result<Self, ExecutionError> {
        let switch = RelaySwitch::new(vec![])
            .await
            .map_err(|e| ExecutionError::HostError(format!("RelaySwitch init: {}", e)))?;

        let max_chunk = switch.limits().await.max_chunk as usize;
        let max_chunk = if max_chunk == 0 { DEFAULT_MAX_CHUNK as usize } else { max_chunk };

        Ok(Self {
            switch: Arc::new(switch),
            node_data: HashMap::new(),
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
        let max_chunk = if max_chunk == 0 { DEFAULT_MAX_CHUNK as usize } else { max_chunk };

        Ok(Self {
            switch,
            node_data: HashMap::new(),
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
    /// (e.g., an InProcessPluginHost or external plugin connection).
    ///
    /// Returns the master index on success.
    pub async fn add_master(
        &mut self,
        socket: UnixStream,
    ) -> Result<usize, ExecutionError> {
        let idx = self.switch.add_master(socket)
            .await
            .map_err(|e| ExecutionError::HostError(format!("add_master: {}", e)))?;

        self.update_max_chunk().await;
        Ok(idx)
    }

    /// Add a PluginHostRuntime as a master, spawning all required infrastructure.
    ///
    /// This creates:
    /// - PluginHostRuntime (async, in tokio task)
    /// - RelaySlave (async, in tokio task)
    /// - Socket pairs connecting them to the switch
    ///
    /// The ExecutionContext manages cleanup of these resources.
    ///
    /// # Arguments
    /// * `plugins` - Vec of (binary_path, cap_urns) to register with the host
    pub async fn add_plugin_host(
        &mut self,
        plugins: Vec<(PathBuf, Vec<String>)>,
    ) -> Result<usize, ExecutionError> {
        // Create socket pairs:
        //   switch_sock <-> slave_ext_sock (switch to slave)
        //   slave_int_sock <-> host_sock (slave to host runtime)
        let (switch_sock, slave_ext_sock) =
            UnixStream::pair().map_err(ExecutionError::IoError)?;
        let (slave_int_sock, host_sock) =
            UnixStream::pair().map_err(ExecutionError::IoError)?;

        // --- PluginHostRuntime (async, in tokio task) ---
        let mut host = PluginHostRuntime::new();
        for (path, caps) in &plugins {
            host.register_plugin(path, caps);
        }

        let (host_read, host_write) = host_sock.into_split();

        let host_handle = tokio::spawn(async move {
            if let Err(e) = host.run(host_read, host_write, || Vec::new()).await {
                tracing::error!("[PluginHostRuntime] Fatal: {}", e);
            }
        });

        // --- RelaySlave (async, in tokio task) ---
        let (slave_int_read, slave_int_write) = slave_int_sock.into_split();
        let slave = RelaySlave::new(
            BufReader::new(slave_int_read),
            BufWriter::new(slave_int_write),
        );

        // Initial caps: just CAP_IDENTITY for handshake verification.
        // PluginHostRuntime sends full caps via RelayNotify.
        let initial_caps_json = serde_json::to_vec(&[CAP_IDENTITY])
            .map_err(|e| ExecutionError::HostError(format!("serialize caps: {}", e)))?;

        let (slave_ext_read, slave_ext_write) = slave_ext_sock.into_split();

        let slave_handle = tokio::spawn(async move {
            if let Err(e) = slave.run(
                FrameReader::new(BufReader::new(slave_ext_read)),
                FrameWriter::new(BufWriter::new(slave_ext_write)),
                Some((&initial_caps_json, &Limits::default())),
            ).await {
                tracing::error!("[RelaySlave] Fatal: {}", e);
            }
        });

        // --- Add to switch ---
        let master_idx = self.switch.add_master(switch_sock)
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
        self.max_chunk = if chunk == 0 { DEFAULT_MAX_CHUNK as usize } else { chunk };
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

    /// Get data for a node.
    pub fn get_node_data(&self, node: &str) -> Option<&Vec<u8>> {
        self.node_data.get(node)
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
    ) -> Result<(), ExecutionError> {
        assert!(!edges.is_empty(), "execute_fanin requires at least one edge");

        let cap_urn = &edges[0].cap_urn;
        let to = &edges[0].to;

        let total_streams = edges.len() + extra_args.len();
        tracing::info!(
            "Executing cap: {} ({} input stream(s) -> {})",
            cap_urn,
            total_streams,
            to
        );

        // Collect all input data upfront — fail fast if any source is missing
        let mut inputs: Vec<(Vec<u8>, String)> = edges
            .iter()
            .map(|edge| {
                let data = self
                    .node_data
                    .get(&edge.from)
                    .ok_or_else(|| ExecutionError::NoIncomingData {
                        node: edge.from.clone(),
                    })?
                    .clone();
                Ok((data, edge.in_media.clone()))
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        // Add extra arguments as additional input streams
        for (media_urn, data) in extra_args {
            inputs.push((data.clone(), media_urn.clone()));
        }

        // Open ONE cap invocation for all inputs
        let (request_id, mut rx) = self
            .switch
            .execute_cap(cap_urn, vec![], "application/cbor")
            .await
            .map_err(|e| ExecutionError::HostError(format!("execute_cap: {}", e)))?;

        // Send each input as a separate named stream
        for (data, in_media) in &inputs {
            let stream_id = uuid::Uuid::new_v4().to_string();

            let ss = Frame::stream_start(
                request_id.clone(),
                stream_id.clone(),
                in_media.clone(),
            );
            self.switch
                .send_to_master(ss, None)
                .await
                .map_err(|e| ExecutionError::HostError(format!("STREAM_START: {}", e)))?;

            let mut offset = 0;
            let mut seq = 0u64;

            if data.is_empty() {
                // Send one empty chunk so the stream is well-formed
                let cbor_value = ciborium::Value::Bytes(vec![]);
                let mut cbor_payload = Vec::new();
                ciborium::into_writer(&cbor_value, &mut cbor_payload)
                    .map_err(|e| ExecutionError::HostError(format!("CBOR encode: {}", e)))?;
                let checksum = Frame::compute_checksum(&cbor_payload);
                let chunk = Frame::chunk(
                    request_id.clone(),
                    stream_id.clone(),
                    0,
                    cbor_payload,
                    0,
                    checksum,
                );
                self.switch
                    .send_to_master(chunk, None)
                    .await
                    .map_err(|e| ExecutionError::HostError(format!("CHUNK: {}", e)))?;
                seq = 1;
            } else {
                while offset < data.len() {
                    let end = (offset + self.max_chunk).min(data.len());
                    let chunk_data = &data[offset..end];

                    // CBOR-encode each chunk as Bytes
                    let cbor_value = ciborium::Value::Bytes(chunk_data.to_vec());
                    let mut cbor_payload = Vec::new();
                    ciborium::into_writer(&cbor_value, &mut cbor_payload)
                        .map_err(|e| ExecutionError::HostError(format!("CBOR encode: {}", e)))?;

                    let checksum = Frame::compute_checksum(&cbor_payload);
                    let chunk = Frame::chunk(
                        request_id.clone(),
                        stream_id.clone(),
                        seq,
                        cbor_payload,
                        seq,
                        checksum,
                    );
                    self.switch
                        .send_to_master(chunk, None)
                        .await
                        .map_err(|e| ExecutionError::HostError(format!("CHUNK: {}", e)))?;

                    offset = end;
                    seq += 1;
                }
            }

            let se = Frame::stream_end(request_id.clone(), stream_id, seq);
            self.switch
                .send_to_master(se, None)
                .await
                .map_err(|e| ExecutionError::HostError(format!("STREAM_END: {}", e)))?;
        }

        // END — no more input streams
        let end_frame = Frame::end(request_id.clone(), None);
        self.switch
            .send_to_master(end_frame, None)
            .await
            .map_err(|e| ExecutionError::HostError(format!("END: {}", e)))?;

        // Collect response using tokio::select! to concurrently:
        // 1. Pump read_from_masters (routes peer requests internally)
        // 2. Receive response frames on rx
        //
        // This is the KEY FIX for the deadlock: we no longer block on
        // read_from_masters_timeout in a sync loop. Instead, we use async
        // select! so peer requests (internal provider → external plugin) can
        // be processed while we wait for the response.
        let mut response_chunks: Vec<u8> = Vec::new();
        let mut got_end = false;

        while !got_end {
            tokio::select! {
                biased;

                // Pump one frame from masters — routes peer requests internally
                pump_result = self.switch.read_from_masters_timeout(Duration::from_millis(200)) => {
                    match pump_result {
                        Ok(Some(frame)) => {
                            tracing::debug!(
                                "  [engine] {:?} id={:?} cap={:?}",
                                frame.frame_type, frame.id, frame.cap
                            );
                        }
                        Ok(None) => {
                            // Timeout or internal frame — peer routing happened, continue
                        }
                        Err(e) => {
                            return Err(ExecutionError::HostError(format!(
                                "read_from_masters: {}",
                                e
                            )));
                        }
                    }
                }

                // Receive response frame
                Some(frame) = rx.recv() => {
                    match frame.frame_type {
                        FrameType::Chunk => {
                            if let Some(payload) = &frame.payload {
                                response_chunks.extend_from_slice(payload);
                            }
                        }
                        FrameType::End => {
                            if let Some(payload) = &frame.payload {
                                response_chunks.extend_from_slice(payload);
                            }
                            got_end = true;
                        }
                        FrameType::Err => {
                            let msg = frame
                                .error_message()
                                .unwrap_or("Unknown plugin error")
                                .to_string();
                            return Err(ExecutionError::PluginExecutionFailed {
                                cap_urn: cap_urn.clone(),
                                details: msg,
                            });
                        }
                        FrameType::Log => {
                            if let Some(payload) = &frame.payload {
                                let text = String::from_utf8_lossy(payload);
                                tracing::info!("  [plugin log] {}", text);
                            }
                        }
                        _ => {
                            // STREAM_START, STREAM_END — structural, skip
                        }
                    }
                }
            }
        }

        // Decode CBOR response chunks → raw output bytes
        let mut output_bytes = Vec::new();
        let mut cursor = std::io::Cursor::new(&response_chunks);
        while (cursor.position() as usize) < response_chunks.len() {
            let value: ciborium::Value = ciborium::from_reader(&mut cursor).map_err(|e| {
                ExecutionError::HostError(format!("CBOR decode response: {}", e))
            })?;
            match value {
                ciborium::Value::Bytes(b) => output_bytes.extend(b),
                ciborium::Value::Text(t) => output_bytes.extend(t.into_bytes()),
                _ => {
                    return Err(ExecutionError::HostError(format!(
                        "Expected Bytes or Text in response, got {:?}",
                        value
                    )));
                }
            }
        }

        self.node_data.insert(to.clone(), output_bytes);
        Ok(())
    }
}

// =============================================================================
// DAG Executor
// =============================================================================

/// Execute a resolved DAG: discover plugins, set up infrastructure, run edge groups.
pub async fn execute_dag(
    graph: &ResolvedGraph,
    plugin_dir: PathBuf,
    registry_url: String,
    initial_inputs: HashMap<String, NodeData>,
    dev_binaries: Vec<PathBuf>,
) -> Result<HashMap<String, NodeData>, ExecutionError> {
    // 1. Initialize plugin manager and discover/download all needed plugins
    let mut plugin_manager = PluginManager::new(plugin_dir, registry_url, dev_binaries);
    plugin_manager.init().await?;

    let cap_urns: Vec<&str> = graph.edges.iter().map(|e| e.cap_urn.as_str()).collect();
    let plugins = plugin_manager.resolve_plugins(&cap_urns).await?;

    tracing::info!("Resolved {} unique plugin binaries:", plugins.len());
    for (path, caps) in &plugins {
        tracing::info!("  {:?} -> {} caps", path, caps.len());
    }

    // 2. Create execution context and add plugin host as master
    let mut ctx = ExecutionContext::new().await?;
    ctx.add_plugin_host(plugins).await?;

    // 3. Resolve initial inputs to raw bytes and set on nodes
    for (node, data) in initial_inputs {
        let bytes = data.into_bytes().await?;
        ctx.set_node_data(node, bytes);
    }

    // 4. Group edges by (to, cap_urn) to detect fan-in, then sort groups topologically.
    //    Fan-in groups are executed as ONE cap invocation with multiple input streams —
    //    the handler decides how to handle each stream as it arrives.
    let groups = build_edge_groups(&graph.edges);
    let group_order = topological_sort_groups(&groups)
        .map_err(|e| ExecutionError::HostError(format!("Topological sort failed: {}", e)))?;

    tracing::info!(
        "Executing {} cap group(s) in topological order",
        group_order.len()
    );

    // Execute groups - now fully async!
    for idx in group_order {
        // No extra arguments in CLI mode - all data flows through edges
        ctx.execute_fanin(&groups[idx].edges, &[]).await?;
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
