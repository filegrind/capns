//! DAG Execution Engine
//!
//! This module provides functionality to execute a resolved DAG by:
//! 1. Discovering plugins that provide the required caps
//! 2. Downloading and installing plugins as needed
//! 3. Executing caps in topological order
//! 4. Passing data between nodes

use crate::{ResolvedEdge, ResolvedGraph};
use capns::PluginRepo;
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::process::Command;

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

    #[error("Data type mismatch: expected {expected}, got {actual}")]
    DataTypeMismatch { expected: String, actual: String },

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
// Plugin Manager
// =============================================================================

/// Manages plugin discovery, download, and caching
pub struct PluginManager {
    plugin_repo: PluginRepo,
    plugin_dir: PathBuf,
    registry_url: String,
    dev_plugins: HashMap<PathBuf, capns::CapManifest>,
}

impl PluginManager {
    /// Create a new plugin manager
    pub fn new(plugin_dir: PathBuf, registry_url: String, dev_binaries: Vec<PathBuf>) -> Self {
        Self {
            plugin_repo: PluginRepo::new(3600), // 1 hour cache
            plugin_dir,
            registry_url,
            dev_plugins: dev_binaries.into_iter().map(|p| (p, capns::CapManifest::new(String::new(), String::new(), String::new(), vec![]))).collect(),
        }
    }

    /// Initialize the plugin manager
    pub async fn init(&mut self) -> Result<(), ExecutionError> {
        // Create plugin directory
        fs::create_dir_all(&self.plugin_dir)?;

        // Discover manifests from dev binaries
        for (bin_path, _) in &self.dev_plugins.clone() {
            eprintln!("[DevMode] Discovering manifest from {:?}...", bin_path);
            match self.discover_manifest(bin_path).await {
                Ok(manifest) => {
                    eprintln!("[DevMode] ✓ Plugin: {}", manifest.name);
                    eprintln!("[DevMode]   Caps: {}", manifest.caps.len());
                    for cap in &manifest.caps {
                        eprintln!("[DevMode]     - {}", cap.urn);
                    }
                    self.dev_plugins.insert(bin_path.clone(), manifest);
                }
                Err(e) => {
                    eprintln!("[DevMode] ✗ Failed to discover manifest from {:?}: {}", bin_path, e);
                    return Err(e);
                }
            }
        }

        // Sync with registry
        self.plugin_repo
            .sync_repos(&[self.registry_url.clone()])
            .await;

        Ok(())
    }

    /// Discover manifest from a binary by running it and doing handshake
    async fn discover_manifest(&self, bin_path: &Path) -> Result<capns::CapManifest, ExecutionError> {
        use capns::{AsyncFrameReader, AsyncFrameWriter, handshake_async};
        use tokio::process::Command;

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

        let mut reader = AsyncFrameReader::new(stdout);
        let mut writer = AsyncFrameWriter::new(stdin);

        let result = handshake_async(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;

        let manifest: capns::CapManifest = serde_json::from_slice(&result.manifest)
            .map_err(|e| ExecutionError::HostError(format!("Failed to parse manifest: {}", e)))?;

        let _ = child.kill().await;

        Ok(manifest)
    }

    /// Find a plugin that provides the given cap
    pub async fn find_plugin_for_cap(&self, cap_urn: &str) -> Result<String, ExecutionError> {
        // Parse the requested cap URN
        let requested_urn = capns::CapUrn::from_string(cap_urn).map_err(|e| {
            ExecutionError::PluginNotFound {
                cap_urn: format!("Invalid URN: {}: {}", cap_urn, e),
            }
        })?;

        // Check dev plugins first
        for (bin_path, manifest) in &self.dev_plugins {
            for cap in &manifest.caps {
                // Use URN equivalence check (bidirectional conforms_to), not string comparison
                // Both directions must match for equivalence
                if requested_urn.conforms_to(&cap.urn) && cap.urn.conforms_to(&requested_urn) {
                    eprintln!("[DevMode] Using dev binary for {}: {:?}", cap_urn, bin_path);
                    return Ok(format!("dev:{}", bin_path.display()));
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

        // Use the first suggestion
        Ok(suggestions[0].plugin_id.clone())
    }

    /// Get the path to a plugin binary, downloading if necessary
    pub async fn get_plugin_path(&self, plugin_id: &str) -> Result<PathBuf, ExecutionError> {
        // Handle dev binaries (prefixed with "dev:")
        if let Some(dev_path) = plugin_id.strip_prefix("dev:") {
            let path = PathBuf::from(dev_path);
            if !path.exists() {
                return Err(ExecutionError::PluginExecutionFailed {
                    cap_urn: plugin_id.to_string(),
                    details: format!("Dev binary not found: {:?}", path),
                });
            }
            eprintln!("[DevMode] Using dev binary: {:?}", path);
            return Ok(path);
        }

        let plugin_path = self.plugin_dir.join(&plugin_id);

        // Check if plugin already exists
        if plugin_path.exists() {
            // SECURITY: Verify existing binary hasn't been tampered with
            self.verify_plugin_integrity(plugin_id, &plugin_path).await?;
            return Ok(plugin_path);
        }

        // Download plugin
        self.download_plugin(plugin_id).await?;

        Ok(plugin_path)
    }

    /// Verify plugin binary integrity before execution
    async fn verify_plugin_integrity(&self, plugin_id: &str, plugin_path: &Path) -> Result<(), ExecutionError> {
        let plugin_info = self
            .plugin_repo
            .get_plugin(plugin_id)
            .await
            .ok_or_else(|| ExecutionError::PluginNotFound {
                cap_urn: format!("Plugin {} not found in registry", plugin_id),
            })?;

        // SECURITY: Refuse to run unsigned binaries
        if plugin_info.team_id.is_empty() || plugin_info.signed_at.is_empty() {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: Plugin {} is not signed. Refusing to execute unsigned binaries.",
                    plugin_id
                ),
            });
        }

        if plugin_info.binary_sha256.is_empty() {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: Plugin {} has no SHA256 hash in registry. Cannot verify integrity.",
                    plugin_id
                ),
            });
        }

        // Read and hash the binary
        let bytes = fs::read(plugin_path)?;

        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();
        let computed_sha256 = format!("{:x}", hash);

        if computed_sha256 != plugin_info.binary_sha256 {
            return Err(ExecutionError::PluginExecutionFailed {
                cap_urn: plugin_id.to_string(),
                details: format!(
                    "SECURITY: SHA256 mismatch for plugin {} at {:?}!\n  Expected: {}\n  Computed: {}\nBinary has been tampered with. Refusing to execute.",
                    plugin_id,
                    plugin_path,
                    plugin_info.binary_sha256,
                    computed_sha256
                ),
            });
        }

        Ok(())
    }

    /// Download and install a plugin
    async fn download_plugin(&self, plugin_id: &str) -> Result<(), ExecutionError> {
        let plugin_info = self
            .plugin_repo
            .get_plugin(plugin_id)
            .await
            .ok_or_else(|| ExecutionError::PluginNotFound {
                cap_urn: format!("Plugin {} not found in registry", plugin_id),
            })?;

        // SECURITY: Verify plugin is signed
        if plugin_info.team_id.is_empty() || plugin_info.signed_at.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: Plugin {} is not signed (missing team_id or signed_at). Refusing to install unsigned binaries.",
                plugin_id
            )));
        }

        // Verify binary information is available
        if plugin_info.binary_name.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "Plugin {} has no binary available for download",
                plugin_id
            )));
        }

        if plugin_info.binary_sha256.is_empty() {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: Plugin {} has no SHA256 hash. Refusing to download without integrity verification.",
                plugin_id
            )));
        }

        // Construct download URL
        // API is at https://filegrind.com/api/plugins
        // Binaries are at https://filegrind.com/plugins/binaries/{binary_name}
        let base_url = self.registry_url
            .trim_end_matches("/api/plugins")
            .trim_end_matches("/");

        let download_url = format!(
            "{}/plugins/binaries/{}",
            base_url,
            plugin_info.binary_name
        );

        eprintln!("Downloading plugin {} v{} from {}",
            plugin_id, plugin_info.version, download_url);
        eprintln!("  Signed by team: {} at {}",
            plugin_info.team_id, plugin_info.signed_at);
        eprintln!("  Expected SHA256: {}", plugin_info.binary_sha256);

        // Download plugin binary
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

        // SECURITY: Verify SHA256
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();
        let computed_sha256 = format!("{:x}", hash);

        if computed_sha256 != plugin_info.binary_sha256 {
            return Err(ExecutionError::PluginDownloadFailed(format!(
                "SECURITY: SHA256 mismatch for plugin {}!\n  Expected: {}\n  Computed: {}\nRefusing to install corrupted or tampered binary.",
                plugin_id,
                plugin_info.binary_sha256,
                computed_sha256
            )));
        }

        eprintln!("✓ SHA256 verified");

        // Write to plugin directory
        let plugin_path = self.plugin_dir.join(&plugin_id);
        fs::write(&plugin_path, bytes)?;

        // Make executable
        let mut perms = fs::metadata(&plugin_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&plugin_path, perms)?;

        eprintln!("✓ Plugin {} v{} installed to {:?}",
            plugin_id, plugin_info.version, plugin_path);

        Ok(())
    }
}

// =============================================================================
// Node Data
// =============================================================================

/// Runtime data associated with a node
#[derive(Debug, Clone)]
pub enum NodeData {
    /// Binary data
    Bytes(Vec<u8>),
    /// Text data  
    Text(String),
    /// File path (for file-based processing)
    FilePath(PathBuf),
}

impl NodeData {
    /// Get bytes representation
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            NodeData::Bytes(b) => b,
            NodeData::Text(t) => t.as_bytes(),
            NodeData::FilePath(_) => &[],
        }
    }
}

// =============================================================================
// Execution Context
// =============================================================================

/// Execution context that tracks node data and execution state
pub struct ExecutionContext {
    /// Data at each node
    node_data: HashMap<String, NodeData>,
    /// Plugin manager
    plugin_manager: PluginManager,
}

impl ExecutionContext {
    /// Create a new execution context
    pub async fn new(plugin_dir: PathBuf, registry_url: String, dev_binaries: Vec<PathBuf>) -> Result<Self, ExecutionError> {
        let mut plugin_manager = PluginManager::new(plugin_dir, registry_url, dev_binaries);
        plugin_manager.init().await?;

        Ok(Self {
            node_data: HashMap::new(),
            plugin_manager,
        })
    }

    /// Set data for a node
    pub fn set_node_data(&mut self, node: String, data: NodeData) {
        self.node_data.insert(node, data);
    }

    /// Get data for a node
    pub fn get_node_data(&self, node: &str) -> Option<&NodeData> {
        self.node_data.get(node)
    }

    /// Execute a single edge (cap invocation)
    pub async fn execute_edge(&mut self, edge: &ResolvedEdge) -> Result<NodeData, ExecutionError> {
        // Get input data
        let input_data = self
            .get_node_data(&edge.from)
            .ok_or_else(|| ExecutionError::NoIncomingData {
                node: edge.from.clone(),
            })?;

        eprintln!("Executing cap: {} ({} -> {})", edge.cap_urn, edge.from, edge.to);

        // Find plugin for this cap
        let plugin_id = self
            .plugin_manager
            .find_plugin_for_cap(&edge.cap_urn)
            .await?;

        // Get plugin binary path
        let plugin_path = self.plugin_manager.get_plugin_path(&plugin_id).await?;

        // Execute plugin (pass edge for media URN info)
        let output = self.execute_plugin(&plugin_path, &edge.cap_urn, input_data, edge).await?;

        Ok(output)
    }

    /// Execute a plugin binary using raw CBOR frame protocol.
    ///
    /// Spawns the plugin, performs HELLO handshake, sends REQ with arguments
    /// using stream multiplexing, collects response, then kills the process.
    async fn execute_plugin(
        &self,
        plugin_path: &Path,
        cap_urn: &str,
        input_data: &NodeData,
        edge: &ResolvedEdge,
    ) -> Result<NodeData, ExecutionError> {
        use capns::{
            CapArgumentValue, AsyncFrameReader, AsyncFrameWriter,
            Frame, FrameType, MessageId, handshake_async,
        };

        // Spawn plugin process
        let mut child = Command::new(plugin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let mut reader = AsyncFrameReader::new(stdout);
        let mut writer = AsyncFrameWriter::new(stdin);

        // HELLO handshake
        let result = handshake_async(&mut reader, &mut writer)
            .await
            .map_err(|e| ExecutionError::HostError(format!("Handshake failed: {:?}", e)))?;
        writer.set_limits(result.limits.clone());
        let max_chunk = result.limits.max_chunk as usize;

        // Prepare argument based on input data type
        let (media_urn, value) = match input_data {
            NodeData::FilePath(path) => {
                let path_str = path.to_str().ok_or_else(|| ExecutionError::PluginExecutionFailed {
                    cap_urn: cap_urn.to_string(),
                    details: "Invalid UTF-8 in file path".to_string(),
                })?;
                (capns::MEDIA_FILE_PATH.to_string(), path_str.as_bytes().to_vec())
            }
            NodeData::Bytes(bytes) => {
                (edge.in_media.clone(), bytes.clone())
            }
            NodeData::Text(text) => {
                (edge.in_media.clone(), text.as_bytes().to_vec())
            }
        };

        let arguments = vec![CapArgumentValue::new(media_urn, value)];
        let request_id = MessageId::new_uuid();

        // Send REQ with stream-multiplexed arguments
        {
            // REQ (empty payload)
            let req = Frame::req(request_id.clone(), cap_urn, vec![], "application/cbor");
            writer.write(&req).await
                .map_err(|e| ExecutionError::HostError(format!("Failed to send REQ: {:?}", e)))?;

            // Each argument as a stream
            for arg in &arguments {
                let stream_id = uuid::Uuid::new_v4().to_string();

                let start = Frame::stream_start(request_id.clone(), stream_id.clone(), arg.media_urn.clone());
                writer.write(&start).await
                    .map_err(|e| ExecutionError::HostError(format!("Failed to send STREAM_START: {:?}", e)))?;

                let mut offset = 0;
                let mut seq = 0u64;
                while offset < arg.value.len() {
                    let end = (offset + max_chunk).min(arg.value.len());
                    let payload = arg.value[offset..end].to_vec();
                    let checksum = Frame::compute_checksum(&payload);
                    let chunk = Frame::chunk(request_id.clone(), stream_id.clone(), seq, payload, seq, checksum);
                    writer.write(&chunk).await
                        .map_err(|e| ExecutionError::HostError(format!("Failed to send CHUNK: {:?}", e)))?;
                    offset = end;
                    seq += 1;
                }

                let end_stream = Frame::stream_end(request_id.clone(), stream_id, seq);
                writer.write(&end_stream).await
                    .map_err(|e| ExecutionError::HostError(format!("Failed to send STREAM_END: {:?}", e)))?;
            }

            // END frame
            let end = Frame::end(request_id.clone(), None);
            writer.write(&end).await
                .map_err(|e| ExecutionError::HostError(format!("Failed to send END: {:?}", e)))?;
        }

        // Read response frames
        let mut collected = Vec::new();
        loop {
            let frame = reader.read().await
                .map_err(|e| ExecutionError::HostError(format!("Failed to read response: {:?}", e)))?
                .ok_or_else(|| ExecutionError::HostError("Plugin closed connection before response".to_string()))?;

            if frame.id != request_id { continue; }

            match frame.frame_type {
                FrameType::Chunk => {
                    if let Some(payload) = &frame.payload {
                        collected.extend_from_slice(payload);
                    }
                }
                FrameType::StreamStart | FrameType::StreamEnd => {}
                FrameType::End => {
                    if let Some(payload) = &frame.payload {
                        collected.extend_from_slice(payload);
                    }
                    break;
                }
                FrameType::Err => {
                    let message = frame.meta.as_ref()
                        .and_then(|m| m.get("message"))
                        .and_then(|v| match v { ciborium::Value::Text(s) => Some(s.clone()), _ => None })
                        .unwrap_or_else(|| "Unknown error".to_string());
                    return Err(ExecutionError::HostError(format!("Plugin error: {}", message)));
                }
                _ => {}
            }
        }

        // Kill plugin
        let _ = child.kill().await;

        // Decode CBOR response
        let output_value: ciborium::Value = ciborium::from_reader(&collected[..])
            .map_err(|e| ExecutionError::HostError(format!("Failed to decode CBOR response: {}", e)))?;

        let output_bytes = match output_value {
            ciborium::Value::Bytes(b) => b,
            ciborium::Value::Text(t) => t.into_bytes(),
            _ => return Err(ExecutionError::HostError(format!(
                "Expected Bytes or Text from cap output, got {:?}",
                output_value
            ))),
        };

        Ok(NodeData::Bytes(output_bytes))
    }
}

// =============================================================================
// DAG Executor
// =============================================================================

/// Execute a resolved graph
pub async fn execute_dag(
    graph: &ResolvedGraph,
    plugin_dir: PathBuf,
    registry_url: String,
    initial_inputs: HashMap<String, NodeData>,
    dev_binaries: Vec<PathBuf>,
) -> Result<HashMap<String, NodeData>, ExecutionError> {
    let mut ctx = ExecutionContext::new(plugin_dir, registry_url, dev_binaries).await?;

    // Set initial inputs
    for (node, data) in initial_inputs {
        ctx.set_node_data(node, data);
    }

    // Get topological order
    let order = topological_sort(&graph.edges)?;

    eprintln!("\nExecuting {} edges in topological order\n", order.len());

    // Execute edges in order
    for edge in order {
        let output_data = ctx.execute_edge(edge).await?;
        ctx.set_node_data(edge.to.clone(), output_data);
    }

    eprintln!("\nExecution complete!\n");

    Ok(ctx.node_data)
}

/// Topological sort of edges
fn topological_sort(edges: &[ResolvedEdge]) -> Result<Vec<&ResolvedEdge>, ExecutionError> {
    // Build adjacency list and in-degree map
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&ResolvedEdge>> = HashMap::new();

    // Initialize nodes
    for edge in edges {
        in_degree.entry(edge.from.as_str()).or_insert(0);
        *in_degree.entry(edge.to.as_str()).or_insert(0) += 1;
        adj.entry(edge.from.as_str())
            .or_insert_with(Vec::new)
            .push(edge);
    }

    // Find nodes with no incoming edges
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter_map(|(node, &deg)| if deg == 0 { Some(*node) } else { None })
        .collect();

    let mut sorted = Vec::new();

    while let Some(node) = queue.pop() {
        // Add all outgoing edges from this node
        if let Some(outgoing) = adj.get(node) {
            for edge in outgoing {
                sorted.push(*edge);

                // Decrease in-degree of target node
                if let Some(degree) = in_degree.get_mut(edge.to.as_str()) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push(edge.to.as_str());
                    }
                }
            }
        }
    }

    // Check if all edges were processed
    if sorted.len() != edges.len() {
        return Err(ExecutionError::PluginExecutionFailed {
            cap_urn: "".to_string(),
            details: "Cycle detected in graph".to_string(),
        });
    }

    Ok(sorted)
}
