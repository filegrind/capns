//! Integration tests for CBOR plugin communication protocol.
//!
//! These tests verify that PluginRuntime (plugin side) and AsyncPluginHost (host side)
//! can communicate correctly through pipes. This is the ONLY supported communication
//! method - no fallbacks, no alternatives.
//!
//! Tests use Unix sockets to simulate real stdin/stdout communication between processes.

#[cfg(test)]
mod tests {
    use crate::async_plugin_host::AsyncPluginHost;
    use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
    use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept};
    use crate::plugin_runtime::PluginRuntime;
    use std::io::{BufReader, BufWriter};
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// Test manifest JSON - plugins MUST include manifest in HELLO response
    const TEST_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:op=test","title":"Test","command":"test"}]}"#;

    /// Create properly split async streams for AsyncPluginHost.
    /// Returns (host_write, plugin_read, plugin_write, host_read)
    /// where host_write/host_read are async (tokio) and plugin_read/write are sync (std) for thread-based simulation.
    fn create_async_pipe_pair() -> (
        tokio::net::unix::OwnedWriteHalf,  // host writes to plugin
        std::os::unix::net::UnixStream,    // plugin reads from host (sync)
        std::os::unix::net::UnixStream,    // plugin writes to host (sync)
        tokio::net::unix::OwnedReadHalf,   // host reads from plugin
    ) {
        // Host -> Plugin pipe
        let (host_write_std, plugin_read) =
            std::os::unix::net::UnixStream::pair().expect("Failed to create pipe pair");
        // Plugin -> Host pipe
        let (plugin_write, host_read_std) =
            std::os::unix::net::UnixStream::pair().expect("Failed to create pipe pair");

        // Set non-blocking for tokio conversion
        host_write_std.set_nonblocking(true).expect("Failed to set non-blocking");
        host_read_std.set_nonblocking(true).expect("Failed to set non-blocking");

        // Create tokio streams
        let host_write_stream = tokio::net::UnixStream::from_std(host_write_std).expect("Failed to create tokio stream");
        let host_read_stream = tokio::net::UnixStream::from_std(host_read_std).expect("Failed to create tokio stream");

        // Split into owned halves
        let (_, host_write) = host_write_stream.into_split();
        let (host_read, _) = host_read_stream.into_split();

        (host_write, plugin_read, plugin_write, host_read)
    }

    #[tokio::test]
    async fn test_handshake_host_plugin() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side in separate thread (sync I/O - simulating real plugin process)
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Accept handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes())
                .expect("Plugin handshake failed");

            assert!(limits.max_frame > 0);
            assert!(limits.max_chunk > 0);
            limits
        });

        // Host side (async)
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Verify manifest was received
        assert_eq!(host.plugin_manifest(), TEST_MANIFEST.as_bytes());

        let host_limits = host.limits();

        // Shutdown cleanly
        host.shutdown().await;

        let plugin_limits = plugin_handle.join().expect("Plugin thread panicked");

        // Both should have negotiated the same limits
        assert_eq!(host_limits.max_frame, plugin_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_limits.max_chunk);
    }

    #[tokio::test]
    async fn test_request_response_simple() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: accept handshake and respond to request
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some("cap:op=echo"));

            let payload = frame.payload.unwrap_or_default();
            assert_eq!(&payload, b"hello");

            // Send response
            let response = Frame::end(frame.id, Some(b"hello back".to_vec()));
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Verify manifest was received
        assert_eq!(host.plugin_manifest(), TEST_MANIFEST.as_bytes());

        // Send request
        let response = host.call("cap:op=echo", b"hello", "application/json").await.expect("Call failed");

        assert_eq!(response.concatenated(), b"hello back");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_streaming_chunks() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: send multiple chunks
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Send 3 chunks
            let chunks = vec![b"chunk1".to_vec(), b"chunk2".to_vec(), b"chunk3".to_vec()];
            for (i, chunk) in chunks.iter().enumerate() {
                let is_last = i == chunks.len() - 1;
                let mut chunk_frame = Frame::chunk(request_id.clone(), i as u64, chunk.clone());
                if i == 0 {
                    chunk_frame.len = Some(18); // total length
                }
                if is_last {
                    chunk_frame.eof = Some(true);
                }
                frame_writer.write(&chunk_frame).unwrap();
            }
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Call and collect streaming response
        let mut streaming = host
            .call_streaming("cap:op=stream", b"go", "application/json")
            .await
            .expect("Call streaming failed");

        let mut chunks = Vec::new();
        while let Some(result) = streaming.next().await {
            chunks.push(result.expect("Chunk error"));
        }

        assert_eq!(chunks.len(), 3);

        // Verify chunk contents
        assert_eq!(chunks[0].payload, b"chunk1");
        assert_eq!(chunks[1].payload, b"chunk2");
        assert_eq!(chunks[2].payload, b"chunk3");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_heartbeat_from_host() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Use a channel to signal when plugin has sent response
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        // Plugin side: respond to heartbeats
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read heartbeat
            let frame = frame_reader.read().unwrap().expect("Expected heartbeat frame");
            assert_eq!(frame.frame_type, FrameType::Heartbeat);

            // Respond with heartbeat (same ID)
            let response = Frame::heartbeat(frame.id);
            frame_writer.write(&response).unwrap();

            // Signal done
            done_tx.send(()).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Send heartbeat - yield first to ensure writer task is running
        tokio::task::yield_now().await;
        host.send_heartbeat().await.expect("Heartbeat failed");

        // Wait for plugin to finish responding with timeout
        let timeout = std::time::Duration::from_secs(5);
        done_rx.recv_timeout(timeout).expect("Plugin did not signal done in time");

        // Small delay to let the heartbeat response be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_plugin_error_response() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: respond with error
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Send error
            let err = Frame::err(frame.id, "NOT_FOUND", "Cap not found: cap:op=missing");
            frame_writer.write(&err).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Call should fail with plugin error
        let result = host.call("cap:op=missing", b"", "application/json").await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            crate::async_plugin_host::AsyncHostError::PluginError { code, message } => {
                assert_eq!(code, "NOT_FOUND");
                assert!(message.contains("Cap not found"));
            }
            _ => panic!("Expected PluginError, got {:?}", err),
        }

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_log_frames_during_request() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: send logs before response
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Send log frames
            let log1 = Frame::log(request_id.clone(), "info", "Processing started");
            frame_writer.write(&log1).unwrap();

            let log2 = Frame::log(request_id.clone(), "debug", "Step 1 complete");
            frame_writer.write(&log2).unwrap();

            // Send final response
            let response = Frame::end(request_id, Some(b"done".to_vec()));
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Call should succeed despite log frames
        let response = host.call("cap:op=test", b"", "application/json").await.expect("Call failed");
        assert_eq!(response.concatenated(), b"done");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_limits_negotiation() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: use smaller limits
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Read host's HELLO
            let host_hello = frame_reader.read().unwrap().expect("Expected HELLO");
            assert_eq!(host_hello.frame_type, FrameType::Hello);

            // Send our HELLO with smaller limits AND manifest (required)
            let small_limits = Limits {
                max_frame: 500_000,
                max_chunk: 50_000,
            };
            let our_hello = Frame::hello_with_manifest(
                small_limits.max_frame,
                small_limits.max_chunk,
                TEST_MANIFEST.as_bytes(),
            );
            frame_writer.write(&our_hello).unwrap();

            small_limits
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        let host_limits = host.limits();

        host.shutdown().await;
        let plugin_small_limits = plugin_handle.join().expect("Plugin thread panicked");

        // Host should have negotiated to the smaller limits
        assert_eq!(host_limits.max_frame, plugin_small_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_small_limits.max_chunk);
        assert_eq!(host_limits.max_frame, 500_000);
        assert_eq!(host_limits.max_chunk, 50_000);
    }

    #[tokio::test]
    async fn test_binary_payload_roundtrip() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Create binary test data with all byte values
        let mut binary_data = Vec::new();
        for i in 0u8..=255 {
            binary_data.push(i);
        }
        let binary_clone = binary_data.clone();

        // Plugin side: echo back binary data
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let payload = frame.payload.clone().unwrap_or_default();

            // Verify we received all bytes correctly
            assert_eq!(payload.len(), 256);
            for (i, &byte) in payload.iter().enumerate() {
                assert_eq!(byte, i as u8, "Byte mismatch at position {}", i);
            }

            // Echo back
            let response = Frame::end(frame.id, Some(payload));
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Send binary data
        let response = host.call("cap:op=binary", &binary_clone, "application/octet-stream").await.expect("Call failed");
        let result = response.concatenated();

        // Verify response matches
        assert_eq!(result.len(), 256);
        for (i, &byte) in result.iter().enumerate() {
            assert_eq!(byte, i as u8, "Response byte mismatch at position {}", i);
        }

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_message_id_uniqueness() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let received_ids = Arc::new(Mutex::new(Vec::<MessageId>::new()));
        let received_ids_clone = received_ids.clone();

        // Plugin side: collect request IDs
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read 3 requests and respond with same IDs
            for _ in 0..3 {
                let frame = frame_reader.read().unwrap().expect("Expected frame");
                received_ids_clone.lock().unwrap().push(frame.id.clone());

                let response = Frame::end(frame.id, Some(b"ok".to_vec()));
                frame_writer.write(&response).unwrap();
            }
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Send requests one by one
        for i in 0..3 {
            let _response = host.call(&format!("cap:op=test{}", i), b"", "application/json").await.expect("Call failed");
        }

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");

        // Verify IDs were received and are unique
        let received = received_ids.lock().unwrap();
        assert_eq!(received.len(), 3);

        // All IDs should be unique
        for i in 0..received.len() {
            for j in (i + 1)..received.len() {
                assert_ne!(received[i], received[j], "IDs should be unique");
            }
        }
    }

    #[test]
    fn test_plugin_runtime_handler_registration() {
        // Test that handler registration and lookup works
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register::<serde_json::Value, _>("cap:op=echo", |req, _emitter, _peer| {
            // Serialize back to bytes
            Ok(serde_json::to_vec(&req).unwrap_or_default())
        });

        runtime.register::<serde_json::Value, _>("cap:op=transform", |_req, _emitter, _peer| {
            Ok(b"transformed".to_vec())
        });

        // Exact match
        assert!(runtime.find_handler("cap:op=echo").is_some());
        assert!(runtime.find_handler("cap:op=transform").is_some());

        // Non-existent
        assert!(runtime.find_handler("cap:op=unknown").is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_during_streaming() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: send heartbeat between chunks
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Send chunk 1
            let chunk1 = Frame::chunk(request_id.clone(), 0, b"part1".to_vec());
            frame_writer.write(&chunk1).unwrap();

            // Send heartbeat (plugin-initiated)
            let heartbeat_id = MessageId::new_uuid();
            let heartbeat = Frame::heartbeat(heartbeat_id.clone());
            frame_writer.write(&heartbeat).unwrap();

            // Wait for heartbeat response
            let hb_response = frame_reader.read().unwrap().expect("Expected heartbeat response");
            assert_eq!(hb_response.frame_type, FrameType::Heartbeat);
            assert_eq!(hb_response.id, heartbeat_id);

            // Send final chunk
            let mut chunk2 = Frame::chunk(request_id.clone(), 1, b"part2".to_vec());
            chunk2.eof = Some(true);
            frame_writer.write(&chunk2).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Call streaming - should handle heartbeat mid-stream
        let mut streaming = host.call_streaming("cap:op=stream", b"", "application/json").await.expect("Call streaming failed");

        let mut chunks = Vec::new();
        while let Some(result) = streaming.next().await {
            if let Ok(chunk) = result {
                chunks.push(chunk);
            }
        }

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].payload, b"part1");
        assert_eq!(chunks[1].payload, b"part2");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_res_frame_single_response() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: respond with RES frame (not END)
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Send RES frame (single complete response)
            let response = Frame::res(frame.id, b"single response".to_vec(), "application/octet-stream");
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        let response = host.call("cap:op=single", b"", "application/json").await.expect("Call failed");
        assert_eq!(response.concatenated(), b"single response");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[tokio::test]
    async fn test_host_initiated_heartbeat_no_ping_pong() {
        // This test verifies that when the HOST sends a heartbeat and the plugin responds,
        // the host does NOT respond back (which would cause infinite ping-pong or SIGPIPE).
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        // Plugin side: respond to heartbeat, then send a request response, then close
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake with manifest
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let request_frame = frame_reader.read().unwrap().expect("Expected request frame");
            assert_eq!(request_frame.frame_type, FrameType::Req);
            let request_id = request_frame.id;

            // Read heartbeat from host
            let heartbeat_frame = frame_reader.read().unwrap().expect("Expected heartbeat frame");
            assert_eq!(heartbeat_frame.frame_type, FrameType::Heartbeat);
            let heartbeat_id = heartbeat_frame.id;

            // Respond to heartbeat
            let heartbeat_response = Frame::heartbeat(heartbeat_id);
            frame_writer.write(&heartbeat_response).unwrap();

            // Now send the request response
            let response = Frame::res(request_id, b"done".to_vec(), "text/plain");
            frame_writer.write(&response).unwrap();

            // Close the writer to signal EOF to host
            drop(frame_writer);
        });

        // Host side
        let host = AsyncPluginHost::new(host_write, host_read).await.expect("Host creation failed");

        // Verify manifest was received
        assert_eq!(host.plugin_manifest(), TEST_MANIFEST.as_bytes());

        // Start a streaming request
        let mut streaming = host.call_streaming("cap:op=test", b"", "application/json").await.expect("Request failed");

        // Send heartbeat while request is in flight
        host.send_heartbeat().await.expect("Heartbeat failed");

        // Collect response
        let mut chunks = Vec::new();
        while let Some(result) = streaming.next().await {
            match result {
                Ok(chunk) => {
                    let is_eof = chunk.is_eof;
                    chunks.push(chunk);
                    if is_eof {
                        break;
                    }
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].payload, b"done");

        host.shutdown().await;
        plugin_handle.join().expect("Plugin thread panicked");
    }
}
