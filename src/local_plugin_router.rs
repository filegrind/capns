//! Local Plugin Router - Routes peer invoke requests to local plugin instances
//!
//! This router maintains a registry of local AsyncPluginHost instances and routes
//! peer invoke requests to them based on cap URN matching.

use crate::async_plugin_host::{AsyncHostError, AsyncPluginHost, ResponseChunk};
use crate::cap_router::{CapRouter, PeerRequestHandle};
use crate::cbor_frame::{Frame, FrameType};
use crate::{CapArgumentValue, CapUrn};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// Router that routes to locally registered plugin instances.
pub struct LocalPluginRouter {
    routes: Arc<RwLock<HashMap<String, Arc<AsyncPluginHost>>>>,
}

impl LocalPluginRouter {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_plugin(&self, cap_urn: &str, host: Arc<AsyncPluginHost>) {
        let mut routes = self.routes.write().await;
        routes.insert(cap_urn.to_string(), host);
    }

    async fn find_plugin(&self, cap_urn: &str) -> Option<Arc<AsyncPluginHost>> {
        let request_urn = CapUrn::from_string(cap_urn).ok()?;
        let routes = self.routes.read().await;

        for (registered_cap, host) in routes.iter() {
            if let Ok(registered_urn) = CapUrn::from_string(registered_cap) {
                if registered_urn.is_compatible_with(&request_urn) {
                    return Some(Arc::clone(host));
                }
            }
        }

        None
    }
}

impl Default for LocalPluginRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for an active peer request being routed to a local plugin.
struct LocalPluginRequestHandle {
    cap_urn: String,
    plugin: Arc<AsyncPluginHost>,
    streams: Arc<Mutex<Vec<(String, String, Vec<u8>)>>>,  // (stream_id, media_urn, data) - ordered
    response_tx: Sender<Result<ResponseChunk, AsyncHostError>>,
    response_rx: Receiver<Result<ResponseChunk, AsyncHostError>>,
}

impl LocalPluginRequestHandle {
    fn new(cap_urn: String, plugin: Arc<AsyncPluginHost>) -> Self {
        let (response_tx, response_rx) = bounded(64);
        Self {
            cap_urn,
            plugin,
            streams: Arc::new(Mutex::new(Vec::new())),
            response_tx,
            response_rx,
        }
    }
}

impl PeerRequestHandle for LocalPluginRequestHandle {
    fn forward_frame(&mut self, frame: Frame) {
        match frame.frame_type {
            FrameType::StreamStart => {
                let stream_id = frame.stream_id.unwrap_or_default();
                let media_urn = frame.media_urn.unwrap_or_default();
                eprintln!("[LocalPluginRequestHandle] STREAM_START: stream_id={} media_urn={}", stream_id, media_urn);

                self.streams.lock().unwrap().push((stream_id, media_urn, Vec::new()));
            }
            FrameType::Chunk => {
                let stream_id = frame.stream_id.unwrap_or_default();
                if let Some(payload) = frame.payload {
                    let mut streams = self.streams.lock().unwrap();
                    if let Some((_, _, ref mut data)) = streams.iter_mut().find(|(id, _, _)| id == &stream_id) {
                        data.extend_from_slice(&payload);
                        eprintln!("[LocalPluginRequestHandle] CHUNK: stream_id={} total_size={}", stream_id, data.len());
                    }
                }
            }
            FrameType::StreamEnd => {
                let stream_id = frame.stream_id.unwrap_or_default();
                eprintln!("[LocalPluginRequestHandle] STREAM_END: stream_id={}", stream_id);
            }
            FrameType::End => {
                eprintln!("[LocalPluginRequestHandle] END: executing cap with accumulated arguments");

                // Build CapArgumentValues from accumulated streams
                let streams = self.streams.lock().unwrap();

                let arguments: Vec<CapArgumentValue> = streams.iter()
                    .map(|(_stream_id, media_urn, data)| {
                        CapArgumentValue::new(media_urn.clone(), data.clone())
                    })
                    .collect();

                eprintln!("[LocalPluginRequestHandle] Executing cap with {} arguments", arguments.len());

                // Execute on plugin in separate thread (sync context for crossbeam channel)
                let plugin = Arc::clone(&self.plugin);
                let cap_urn = self.cap_urn.clone();
                let response_tx = self.response_tx.clone();

                std::thread::spawn(move || {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async move {
                        match plugin.request_with_arguments(&cap_urn, &arguments).await {
                            Ok(mut rx) => {
                                while let Some(chunk_result) = rx.recv().await {
                                    if response_tx.send(chunk_result).is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = response_tx.send(Err(e));
                            }
                        }
                    });
                });
            }
            _ => {}
        }
    }

    fn response_receiver(&self) -> Receiver<Result<ResponseChunk, AsyncHostError>> {
        self.response_rx.clone()
    }
}

impl CapRouter for LocalPluginRouter {
    fn begin_request(
        &self,
        cap_urn: &str,
        _req_id: &[u8; 16],
    ) -> Result<Box<dyn PeerRequestHandle>, AsyncHostError> {
        eprintln!("[LocalPluginRouter] begin_request for cap: {}", cap_urn);

        let cap_urn = cap_urn.to_string();
        let routes_clone = Arc::clone(&self.routes);

        // Create channel for responses
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);

        // Find plugin in separate thread (sync trait method needs async lookup)
        let cap_urn_for_find = cap_urn.clone();
        let cap_urn_for_handle = cap_urn.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let plugin = runtime.block_on(async move {
                let router = LocalPluginRouter { routes: routes_clone };
                router.find_plugin(&cap_urn_for_find).await
            });

            let result = match plugin {
                Some(p) => Ok(LocalPluginRequestHandle::new(cap_urn_for_handle.clone(), p)),
                None => Err(AsyncHostError::NoHandler(format!("No plugin registered for: {}", cap_urn_for_handle))),
            };

            let _ = result_tx.send(result);
        });

        // Wait for result
        match result_rx.recv() {
            Ok(Ok(handle)) => Ok(Box::new(handle)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AsyncHostError::NoHandler("Router thread died".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_plugin_router_creation() {
        let router = LocalPluginRouter::new();
        assert!(router.routes.try_read().is_ok());
    }

    // TEST385: LocalPluginRouter starts with empty routes
    #[test]
    fn test_router_empty_initially() {
        let router = LocalPluginRouter::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            let routes = router.routes.read().await;
            assert_eq!(routes.len(), 0, "Router should start empty");
        });
    }

    // TEST386: LocalPluginRouter::Default creates empty router
    #[test]
    fn test_router_default() {
        let router = LocalPluginRouter::default();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            let routes = router.routes.read().await;
            assert_eq!(routes.len(), 0, "Default router should be empty");
        });
    }

    // TEST387: LocalPluginRouter::find_plugin returns None for empty router
    #[test]
    fn test_find_plugin_empty_router() {
        let router = LocalPluginRouter::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            let found = router.find_plugin("cap:in=\"media:void\";op=missing;out=\"media:void\"").await;
            assert!(found.is_none(), "Should return None for empty router");
        });
    }

    // TEST388: CapRouter::begin_request called with NoPeerRouter fails with correct error
    #[test]
    fn test_no_peer_router_returns_error() {
        use crate::cap_router::{CapRouter, NoPeerRouter};
        let router = NoPeerRouter;
        let req_id = [0u8; 16];
        let result = router.begin_request("cap:in=\"media:void\";op=test;out=\"media:void\"", &req_id);

        assert!(result.is_err(), "NoPeerRouter should reject all requests");
        match result {
            Err(AsyncHostError::PeerInvokeNotSupported(urn)) => {
                assert!(urn.contains("test"), "Error should contain the cap URN");
            }
            _ => panic!("Expected PeerInvokeNotSupported error"),
        }
    }

    // Tests 389-393: LocalPluginRequestHandle end-to-end functionality
    // THESE ARE TESTED IN INTEGRATION TESTS - Cannot unit test without full handshake
    // See capns-interop-tests for peer invoke routing tests

    // TEST389-TEST393: PLACEHOLDER - LocalPluginRequestHandle stream accumulation
    // These tests would require a full AsyncPluginHost with proper handshake.
    // The functionality IS tested in integration tests (cbor_integration_tests.rs).
    // For cross-language parity, these behaviors are verified in:
    // - Integration tests in cbor_integration_tests.rs (Rust)
    // - capns-interop-tests (cross-language)
    //
    // What needs testing (covered in integration tests):
    // - LocalPluginRequestHandle creates streams on STREAM_START
    // - LocalPluginRequestHandle accumulates CHUNK data for streams
    // - LocalPluginRequestHandle handles multiple chunks per stream
    // - LocalPluginRequestHandle handles multiple independent streams
    // - LocalPluginRequestHandle preserves stream data after STREAM_END
    //
    // To add these as unit tests, we would need to either:
    // 1. Mock AsyncPluginHost (complex, requires mocking tokio channels)
    // 2. Add a test-only constructor to LocalPluginRequestHandle (pollutes production code)
    // 3. Keep testing at integration level (current approach)
}
