//! Cap Router - Pluggable routing for peer invoke requests
//!
//! When a plugin sends a peer invoke REQ (calling another cap), the host needs to route
//! that request to an appropriate handler. This module provides a trait-based abstraction
//! for different routing strategies:
//!
//! - PluginRepoRouter: Default implementation that queries plugin repository
//! - DevBinsRouter: For macino's --dev-bins local development workflow
//! - Custom implementations for testing or specialized routing logic

use crate::async_plugin_host::{AsyncHostError, ResponseChunk};
use crate::CapArgumentValue;
use crossbeam_channel::Receiver;
use std::sync::Arc;

/// Trait for routing cap invocation requests to appropriate handlers.
///
/// When a plugin issues a peer invoke (via PeerInvoker), the host receives a REQ frame.
/// The CapRouter determines how to handle that request - whether to route it to an
/// existing plugin, spawn a new plugin from the repository, or handle it locally.
///
/// # Thread Safety
/// Implementations must be Send + Sync as they're shared across async tasks.
///
/// # Example
/// ```ignore
/// // Create router
/// let router = PluginRepoRouter::new(repo_client);
///
/// // Route a peer invoke request
/// let response_rx = router.route_request(
///     "cap:in=\"media:pdf;bytes\";op=extract-text;out=\"media:text;textable\"",
///     vec![CapArgumentValue::new("media:pdf;bytes", pdf_data)],
/// )?;
///
/// // Collect response
/// for chunk_result in response_rx.iter() {
///     let chunk = chunk_result?;
///     process_chunk(chunk);
/// }
/// ```
pub trait CapRouter: Send + Sync {
    /// Route a cap invocation request to an appropriate handler.
    ///
    /// # Arguments
    /// * `cap_urn` - The cap URN being requested (e.g., "cap:in=\"media:pdf\";op=extract-text;...")
    /// * `arguments` - Arguments for the cap invocation
    ///
    /// # Returns
    /// A receiver that yields response chunks or errors. The receiver will be closed
    /// when the response is complete (END frame received).
    ///
    /// # Errors
    /// - `NoHandler` - No plugin provides the requested cap
    /// - `PluginSpawnFailed` - Failed to download/start a plugin
    /// - `PluginError` - Plugin returned an error during execution
    fn route_request(
        &self,
        cap_urn: &str,
        arguments: Vec<CapArgumentValue>,
    ) -> Result<Receiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError>;
}

/// No-op router that rejects all peer invoke requests.
///
/// Used as a default when peer invocation is not supported or desired.
/// Useful for:
/// - Testing plugins that shouldn't invoke peers
/// - Sandboxed execution environments
/// - Debugging to prevent cascading plugin calls
///
/// # Example
/// ```
/// use capns::NoPeerRouter;
///
/// let router = NoPeerRouter;
/// let host = AsyncPluginHost::with_router(stdin, stdout, router).await?;
/// ```
pub struct NoPeerRouter;

impl CapRouter for NoPeerRouter {
    fn route_request(
        &self,
        cap_urn: &str,
        _arguments: Vec<CapArgumentValue>,
    ) -> Result<Receiver<Result<ResponseChunk, AsyncHostError>>, AsyncHostError> {
        Err(AsyncHostError::PeerInvokeNotSupported(cap_urn.to_string()))
    }
}

/// Arc wrapper for trait objects to enable cloning.
///
/// Since CapRouter is a trait, we can't clone trait objects directly.
/// This wrapper allows AsyncPluginHost to store and share routers across tasks.
pub type ArcCapRouter = Arc<dyn CapRouter>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_peer_router_rejects_all() {
        let router = NoPeerRouter;
        let result = router.route_request(
            "cap:in=\"media:void\";op=test;out=\"media:void\"",
            vec![],
        );

        assert!(result.is_err());
        match result {
            Err(AsyncHostError::PeerInvokeNotSupported(urn)) => {
                assert!(urn.contains("test"));
            }
            _ => panic!("Expected PeerInvokeNotSupported error"),
        }
    }
}
