//! Plugin Repository Router - Routes peer invoke requests via plugin repository
//!
//! This router implementation queries the plugin repository to find plugins that
//! provide requested capabilities, downloads and spawns them on-demand, and routes
//! requests to the appropriate plugin instance.
//!
//! # Architecture
//!
//! - Queries PluginRepo for cap availability
//! - Spawns AsyncPluginHost instances for needed plugins
//! - Caches running plugin instances by plugin_id
//! - Routes cap requests to appropriate plugin
//! - Handles plugin lifecycle (spawn, error, cleanup)
//!
//! # Example
//!
//! ```ignore
//! let repo_client = PluginRepo::new("https://plugins.example.com").await?;
//! let router = PluginRepoRouter::new(repo_client);
//!
//! // Router automatically downloads and spawns plugins as needed
//! let response_rx = router.route_request(
//!     "cap:in=\"media:pdf;bytes\";op=extract-text;out=\"media:text;textable\"",
//!     vec![CapArgumentValue::new("media:pdf;bytes", pdf_data)],
//! )?;
//! ```

use crate::async_plugin_host::{AsyncHostError, AsyncPluginHost, ResponseChunk};
use crate::cap_router::CapRouter;
use crate::plugin_repo::PluginRepo;
use crate::{CapArgumentValue, CapUrn};
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::RwLock;

/// Information about a running plugin instance
struct RunningPlugin {
    /// The AsyncPluginHost managing this plugin
    host: Arc<AsyncPluginHost>,
    /// Plugin binary path
    binary_path: PathBuf,
    /// Caps this plugin provides (for validation)
    provided_caps: Vec<String>,
}

/// Router that uses plugin repository to find and spawn plugins on-demand.
///
/// This router:
/// 1. Queries PluginRepo for plugins providing requested cap
/// 2. Downloads plugin binary if not cached
/// 3. Spawns AsyncPluginHost for the plugin
/// 4. Routes request to the plugin
/// 5. Caches running plugins for reuse
///
/// # Thread Safety
///
/// Uses RwLock for plugin cache to allow concurrent reads while spawning new plugins.
pub struct PluginRepoRouter {
    /// Plugin repository client for querying and downloading plugins
    repo_client: Arc<PluginRepo>,
    /// Cache of running plugins by plugin_id
    running_plugins: Arc<RwLock<HashMap<String, Arc<RunningPlugin>>>>,
    /// Directory for downloaded plugin binaries
    plugin_cache_dir: PathBuf,
}

impl PluginRepoRouter {
    /// Create a new plugin repository router.
    ///
    /// # Arguments
    /// * `repo_client` - Plugin repository client for querying/downloading plugins
    /// * `cache_dir` - Directory to store downloaded plugin binaries (defaults to temp if None)
    ///
    /// # Returns
    /// A new router instance ready to handle peer invoke requests
    pub fn new(repo_client: PluginRepo, cache_dir: Option<PathBuf>) -> Self {
        let plugin_cache_dir = cache_dir.unwrap_or_else(|| {
            std::env::temp_dir().join("capns_plugins")
        });

        // Ensure cache directory exists
        std::fs::create_dir_all(&plugin_cache_dir)
            .expect("Failed to create plugin cache directory");

        Self {
            repo_client: Arc::new(repo_client),
            running_plugins: Arc::new(RwLock::new(HashMap::new())),
            plugin_cache_dir,
        }
    }

    /// Find a running plugin that provides the requested cap.
    ///
    /// Checks all running plugins to see if any provide a cap that accepts the request.
    /// Uses CapUrn matching (not string comparison).
    async fn find_running_plugin(&self, cap_urn: &str) -> Option<Arc<RunningPlugin>> {
        let request_urn = CapUrn::from_string(cap_urn).ok()?;
        let plugins = self.running_plugins.read().await;

        for plugin in plugins.values() {
            for provided_cap_str in &plugin.provided_caps {
                if let Ok(provided_urn) = CapUrn::from_string(provided_cap_str) {
                    // Check if this plugin's cap can handle the request
                    if provided_urn.is_compatible_with(&request_urn) {
                        return Some(Arc::clone(plugin));
                    }
                }
            }
        }

        None
    }

    /// Download and spawn a plugin from the repository.
    ///
    /// # Arguments
    /// * `plugin_id` - Plugin identifier from repository
    ///
    /// # Returns
    /// Arc to the running plugin instance
    ///
    /// # Errors
    /// - NoHandler if plugin not found in repository
    /// - Io if download or spawn fails
    async fn spawn_plugin(&self, plugin_id: &str) -> Result<Arc<RunningPlugin>, AsyncHostError> {
        eprintln!("[PluginRepoRouter] Spawning plugin: {}", plugin_id);

        // Query repository for plugin info
        let plugins = self.repo_client.get_plugins().await
            .map_err(|e| AsyncHostError::Io(format!("Failed to query plugin repository: {}", e)))?;

        let plugin_info = plugins.iter()
            .find(|p| p.id == plugin_id)
            .ok_or_else(|| AsyncHostError::NoHandler(format!("Plugin '{}' not found in repository", plugin_id)))?;

        // Determine binary path (download if needed)
        let binary_path = self.plugin_cache_dir.join(&plugin_info.id);

        if !binary_path.exists() {
            eprintln!("[PluginRepoRouter] Downloading plugin binary: {}", plugin_id);

            // Download binary
            let binary_data = self.repo_client.download_plugin_binary(&plugin_info.id).await
                .map_err(|e| AsyncHostError::Io(format!("Failed to download plugin binary: {}", e)))?;

            // Write to cache
            std::fs::write(&binary_path, binary_data)
                .map_err(|e| AsyncHostError::Io(format!("Failed to write plugin binary: {}", e)))?;

            // Make executable (Unix)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&binary_path)
                    .map_err(|e| AsyncHostError::Io(format!("Failed to get binary metadata: {}", e)))?
                    .permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&binary_path, perms)
                    .map_err(|e| AsyncHostError::Io(format!("Failed to set binary permissions: {}", e)))?;
            }
        }

        // Spawn plugin process
        eprintln!("[PluginRepoRouter] Executing plugin: {:?}", binary_path);
        let mut child = Command::new(&binary_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| AsyncHostError::Io(format!("Failed to spawn plugin process: {}", e)))?;

        let stdin = child.stdin.take()
            .ok_or_else(|| AsyncHostError::Io("Failed to capture plugin stdin".to_string()))?;
        let stdout = child.stdout.take()
            .ok_or_else(|| AsyncHostError::Io("Failed to capture plugin stdout".to_string()))?;

        // Create AsyncPluginHost with NoPeerRouter (plugins spawned by router don't get peer invoke)
        let host = AsyncPluginHost::new(stdin, stdout).await
            .map_err(|e| AsyncHostError::Handshake(format!("Plugin handshake failed: {}", e)))?;

        let provided_caps = plugin_info.caps.iter()
            .map(|cap| cap.urn.clone())
            .collect();

        let running_plugin = Arc::new(RunningPlugin {
            host: Arc::new(host),
            binary_path,
            provided_caps,
        });

        // Cache the running plugin
        {
            let mut plugins = self.running_plugins.write().await;
            plugins.insert(plugin_id.to_string(), Arc::clone(&running_plugin));
        }

        eprintln!("[PluginRepoRouter] Plugin spawned and ready: {}", plugin_id);
        Ok(running_plugin)
    }
}

impl CapRouter for PluginRepoRouter {
    fn route_request(
        &self,
        cap_urn: &str,
        arguments: Vec<CapArgumentValue>,
    ) -> Result<Receiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        eprintln!("[PluginRepoRouter] Routing request for cap: {}", cap_urn);

        // Need async context - spawn blocking task
        let cap_urn = cap_urn.to_string();
        let self_clone = PluginRepoRouter {
            repo_client: Arc::clone(&self.repo_client),
            running_plugins: Arc::clone(&self.running_plugins),
            plugin_cache_dir: self.plugin_cache_dir.clone(),
        };

        // Create channel for responses
        let (tx, rx) = crossbeam_channel::bounded(64);

        // Spawn async task to handle routing
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("Failed to create tokio runtime");

            runtime.block_on(async move {
                // Try to find running plugin first
                let plugin = if let Some(running) = self_clone.find_running_plugin(&cap_urn).await {
                    eprintln!("[PluginRepoRouter] Found running plugin for cap");
                    running
                } else {
                    // Need to spawn new plugin - query repository
                    eprintln!("[PluginRepoRouter] No running plugin found, querying repository");

                    let plugins = match self_clone.repo_client.get_plugins().await {
                        Ok(p) => p,
                        Err(e) => {
                            let _ = tx.send(Err(AsyncHostError::Io(format!("Failed to query repository: {}", e))));
                            return;
                        }
                    };

                    // Find plugin that provides this cap
                    let request_urn = match CapUrn::from_string(&cap_urn) {
                        Ok(u) => u,
                        Err(e) => {
                            let _ = tx.send(Err(AsyncHostError::NoHandler(format!("Invalid cap URN: {}", e))));
                            return;
                        }
                    };

                    let plugin_id = plugins.iter()
                        .find(|p| {
                            p.caps.iter().any(|cap| {
                                CapUrn::from_string(&cap.urn)
                                    .map(|provided| provided.is_compatible_with(&request_urn))
                                    .unwrap_or(false)
                            })
                        })
                        .map(|p| p.id.as_str());

                    let plugin_id = match plugin_id {
                        Some(id) => id,
                        None => {
                            let _ = tx.send(Err(AsyncHostError::NoHandler(format!("No plugin provides cap: {}", cap_urn))));
                            return;
                        }
                    };

                    // Spawn the plugin
                    match self_clone.spawn_plugin(plugin_id).await {
                        Ok(p) => p,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    }
                };

                // Execute cap on the plugin
                eprintln!("[PluginRepoRouter] Executing cap on plugin");
                match plugin.host.request_with_arguments(&cap_urn, &arguments).await {
                    Ok(mut response_rx) => {
                        // Forward all response chunks to the caller
                        while let Some(chunk_result) = response_rx.recv().await {
                            if tx.send(chunk_result).is_err() {
                                eprintln!("[PluginRepoRouter] Receiver dropped, stopping forwarding");
                                break;
                            }
                        }
                        eprintln!("[PluginRepoRouter] Request completed");
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_repo_router_creation() {
        let repo_client = PluginRepo::new_with_base_url("http://localhost:8080".to_string());
        let router = PluginRepoRouter::new(repo_client, None);

        // Verify cache directory was created
        assert!(router.plugin_cache_dir.exists());
    }
}
