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
    streams: Arc<Mutex<HashMap<String, Vec<u8>>>>,  // stream_id -> accumulated data
    media_urns: Arc<Mutex<HashMap<String, String>>>,  // stream_id -> media_urn
    response_tx: Sender<Result<ResponseChunk, AsyncHostError>>,
    response_rx: Receiver<Result<ResponseChunk, AsyncHostError>>,
}

impl LocalPluginRequestHandle {
    fn new(cap_urn: String, plugin: Arc<AsyncPluginHost>) -> Self {
        let (response_tx, response_rx) = bounded(64);
        Self {
            cap_urn,
            plugin,
            streams: Arc::new(Mutex::new(HashMap::new())),
            media_urns: Arc::new(Mutex::new(HashMap::new())),
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

                self.streams.lock().unwrap().insert(stream_id.clone(), Vec::new());
                self.media_urns.lock().unwrap().insert(stream_id, media_urn);
            }
            FrameType::Chunk => {
                let stream_id = frame.stream_id.unwrap_or_default();
                if let Some(payload) = frame.payload {
                    if let Some(stream_data) = self.streams.lock().unwrap().get_mut(&stream_id) {
                        stream_data.extend_from_slice(&payload);
                        eprintln!("[LocalPluginRequestHandle] CHUNK: stream_id={} total_size={}", stream_id, stream_data.len());
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
                let media_urns = self.media_urns.lock().unwrap();

                let arguments: Vec<CapArgumentValue> = streams.iter()
                    .filter_map(|(stream_id, data)| {
                        media_urns.get(stream_id).map(|urn| {
                            CapArgumentValue::new(urn.clone(), data.clone())
                        })
                    })
                    .collect();

                eprintln!("[LocalPluginRequestHandle] Executing cap with {} arguments", arguments.len());

                // Execute on plugin in separate thread
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

        // Find plugin in separate thread
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
}
