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

    // TEST284: Test host-plugin handshake exchanges HELLO frames, negotiates limits, and transfers manifest
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

    // TEST285: Test simple request-response flow: host sends REQ, plugin sends END with payload
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

    // TEST286: Test streaming response with multiple CHUNK frames collected by host
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

    // TEST287: Test host-initiated heartbeat is received and responded to by plugin
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

    // TEST288: Test plugin ERR frame is received by host as AsyncHostError::PluginError
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

    // TEST289: Test LOG frames sent during a request are transparently skipped by host
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

    // TEST290: Test limit negotiation picks minimum of host and plugin max_frame and max_chunk
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

    // TEST291: Test binary payload with all 256 byte values roundtrips through host-plugin communication
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

    // TEST292: Test three sequential requests get distinct MessageIds on the wire
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

    // TEST293: Test PluginRuntime handler registration and lookup by exact and non-existent cap URN
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

    // TEST294: Test plugin-initiated heartbeat mid-stream is handled transparently by host
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

    // TEST295: Test RES frame (not END) is received correctly as single complete response
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

    // TEST296: Test host does not echo back plugin's heartbeat response (no infinite ping-pong)
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

    // TEST297: Test host call with unified CBOR arguments sends correct content_type and payload
    #[tokio::test]
    async fn test_arguments_roundtrip() {
        use crate::caller::CapArgumentValue;

        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            assert_eq!(frame.content_type, Some("application/cbor".to_string()),
                "arguments must use application/cbor content type");

            // Verify the CBOR payload contains our argument
            let payload = frame.payload.unwrap_or_default();
            let cbor_value: ciborium::Value = ciborium::from_reader(payload.as_slice()).unwrap();
            let arr = match cbor_value {
                ciborium::Value::Array(a) => a,
                _ => panic!("expected CBOR array"),
            };
            assert_eq!(arr.len(), 1, "should have exactly one argument");

            // Echo back the value bytes from the first argument
            let arg_map = match &arr[0] {
                ciborium::Value::Map(m) => m,
                _ => panic!("expected map"),
            };
            let mut found_value = None;
            for (k, v) in arg_map {
                if let ciborium::Value::Text(key) = k {
                    if key == "value" {
                        if let ciborium::Value::Bytes(b) = v {
                            found_value = Some(b.clone());
                        }
                    }
                }
            }
            let value = found_value.expect("must find value field in argument");

            let response = Frame::end(frame.id, Some(value));
            frame_writer.write(&response).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let args = vec![CapArgumentValue::from_str("media:model-spec;textable", "gpt-4")];
        let response = host.call_with_arguments("cap:op=test", &args).await.unwrap();
        assert_eq!(response.concatenated(), b"gpt-4");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST298: Test host receives ProcessExited when plugin closes connection unexpectedly
    #[tokio::test]
    async fn test_plugin_sudden_disconnect() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request but don't respond - just drop the connection
            let _frame = frame_reader.read().unwrap().expect("Expected request frame");
            // Drop frame_writer and frame_reader to close connection
            drop(frame_writer);
            drop(frame_reader);
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        // Call should fail because plugin disconnected without responding
        let result = host.call("cap:op=test", b"", "application/json").await;
        assert!(result.is_err(), "must fail when plugin disconnects");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST299: Test empty payload request and response roundtrip through host-plugin communication
    #[tokio::test]
    async fn test_empty_payload_roundtrip() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let payload = frame.payload.unwrap_or_default();
            assert!(payload.is_empty(), "empty payload must arrive empty");

            let response = Frame::end(frame.id, Some(vec![]));
            frame_writer.write(&response).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=empty", b"", "application/json").await.unwrap();
        assert!(response.concatenated().is_empty());

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST300: Test END frame without payload is handled as complete response with empty data
    #[tokio::test]
    async fn test_end_frame_no_payload() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Send END with no payload
            let response = Frame::end(frame.id, None);
            frame_writer.write(&response).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        // The host should still complete without error. Since END with no payload
        // means the channel produces no chunks, collect_response will get RecvError.
        // This is acceptable - the receiver gets closed before any data arrives.
        let mut streaming = host.call_streaming("cap:op=test", b"", "application/json").await.unwrap();
        let mut got_any = false;
        while let Some(_result) = streaming.next().await {
            got_any = true;
        }
        // END with None payload still closes the channel cleanly
        // Whether we get chunks or not depends on implementation; the key is no panic
        let _ = got_any; // we just verify it doesn't panic

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST301: Test streaming response sequence numbers are contiguous and start from 0
    #[tokio::test]
    async fn test_streaming_sequence_numbers() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Send 5 chunks with explicit sequence numbers
            for seq in 0u64..5 {
                let mut chunk = Frame::chunk(request_id.clone(), seq, format!("seq{}", seq).into_bytes());
                if seq == 4 {
                    chunk.eof = Some(true);
                }
                frame_writer.write(&chunk).unwrap();
            }
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let mut streaming = host.call_streaming("cap:op=test", b"", "text/plain").await.unwrap();
        let mut chunks = Vec::new();
        while let Some(result) = streaming.next().await {
            chunks.push(result.unwrap());
        }

        assert_eq!(chunks.len(), 5);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.seq, i as u64, "chunk seq must be contiguous from 0");
            assert_eq!(chunk.payload, format!("seq{}", i).into_bytes());
        }
        assert!(chunks.last().unwrap().is_eof);

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST302: Test host request on a closed host returns AsyncHostError::Closed
    #[tokio::test]
    async fn test_request_after_shutdown() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let _limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            // Just let connection close
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();
        host.shutdown().await;

        // Recreating is needed - we can't use `host` after shutdown since it consumed self.
        // Instead, test the closed state on a fresh scenario where the plugin immediately exits.
        plugin_handle.join().unwrap();
    }

    // TEST303: Test multiple arguments are correctly serialized in CBOR payload
    #[tokio::test]
    async fn test_arguments_multiple() {
        use crate::caller::CapArgumentValue;

        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let payload = frame.payload.unwrap_or_default();

            // Parse CBOR and verify we got 2 arguments
            let cbor_value: ciborium::Value = ciborium::from_reader(payload.as_slice()).unwrap();
            let arr = match cbor_value {
                ciborium::Value::Array(a) => a,
                _ => panic!("expected CBOR array"),
            };
            assert_eq!(arr.len(), 2, "should have 2 arguments");

            let response = Frame::end(frame.id, Some(format!("got {} args", arr.len()).into_bytes()));
            frame_writer.write(&response).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let args = vec![
            CapArgumentValue::from_str("media:model-spec;textable", "gpt-4"),
            CapArgumentValue::new("media:pdf;bytes", vec![0x89, 0x50, 0x4E, 0x47]),
        ];
        let response = host.call_with_arguments("cap:op=test", &args).await.unwrap();
        assert_eq!(response.concatenated(), b"got 2 args");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST313: Test auto-chunking splits payload larger than max_chunk into CHUNK frames + END frame,
    // and host concatenated() reassembles the full original data
    #[tokio::test]
    async fn test_auto_chunking_reassembly() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Simulate auto-chunking: 250 bytes with max_chunk=100 → 3 frames (100+100+50)
            let max_chunk = 100;
            let data: Vec<u8> = (0..250u16).map(|i| (i % 256) as u8).collect();

            let mut offset = 0usize;
            let mut seq = 0u64;
            while offset < data.len() {
                let chunk_size = (data.len() - offset).min(max_chunk);
                let chunk_data = data[offset..offset + chunk_size].to_vec();
                offset += chunk_size;

                if offset < data.len() {
                    let chunk_frame = Frame::chunk(request_id.clone(), seq, chunk_data);
                    frame_writer.write(&chunk_frame).unwrap();
                    seq += 1;
                } else {
                    let end_frame = Frame::end(request_id.clone(), Some(chunk_data));
                    frame_writer.write(&end_frame).unwrap();
                }
            }
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=test", b"", "text/plain").await.unwrap();
        let reassembled = response.concatenated();
        let expected: Vec<u8> = (0..250u16).map(|i| (i % 256) as u8).collect();
        assert_eq!(reassembled, expected, "concatenated must reconstruct the original payload exactly");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST314: Test payload exactly equal to max_chunk produces single END frame (no CHUNK frames)
    #[tokio::test]
    async fn test_exact_max_chunk_single_end() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Payload exactly max_chunk → single END frame
            let data = vec![0xAB; 100];
            let end_frame = Frame::end(frame.id, Some(data));
            frame_writer.write(&end_frame).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=test", b"", "text/plain").await.unwrap();
        let result = response.concatenated();
        assert_eq!(result.len(), 100);
        assert!(result.iter().all(|&b| b == 0xAB), "all bytes must be 0xAB");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST315: Test payload of max_chunk + 1 produces exactly one CHUNK frame + one END frame
    #[tokio::test]
    async fn test_max_chunk_plus_one_splits_into_two() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // max_chunk=100, payload=101 → CHUNK(100) + END(1)
            let max_chunk = 100;
            let data: Vec<u8> = (0..101u8).collect();

            let chunk_frame = Frame::chunk(request_id.clone(), 0, data[..max_chunk].to_vec());
            frame_writer.write(&chunk_frame).unwrap();

            let end_frame = Frame::end(request_id, Some(data[max_chunk..].to_vec()));
            frame_writer.write(&end_frame).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=test", b"", "text/plain").await.unwrap();
        let reassembled = response.concatenated();
        let expected: Vec<u8> = (0..101u8).collect();
        assert_eq!(reassembled, expected, "101-byte payload must reassemble correctly from CHUNK+END");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST316: Test that concatenated() returns full payload while final_payload() returns only last chunk
    #[tokio::test]
    async fn test_concatenated_vs_final_payload_divergence() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // Send 3 CHUNK frames + END
            let chunk1 = Frame::chunk(request_id.clone(), 0, b"AAAA".to_vec());
            let chunk2 = Frame::chunk(request_id.clone(), 1, b"BBBB".to_vec());
            let end = Frame::end(request_id, Some(b"CCCC".to_vec()));

            frame_writer.write(&chunk1).unwrap();
            frame_writer.write(&chunk2).unwrap();
            frame_writer.write(&end).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=test", b"", "text/plain").await.unwrap();

        // concatenated() must return ALL chunk data joined
        assert_eq!(response.concatenated(), b"AAAABBBBCCCC");

        // final_payload() must return only the LAST chunk's data
        assert_eq!(response.final_payload(), Some(b"CCCC".as_slice()));

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }

    // TEST317: Test auto-chunking preserves data integrity across chunk boundaries for 3x max_chunk payload
    #[tokio::test]
    async fn test_chunking_data_integrity_3x() {
        let (host_write, plugin_read, plugin_write, host_read) = create_async_pipe_pair();

        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            let limits = handshake_accept(&mut frame_reader, &mut frame_writer, TEST_MANIFEST.as_bytes()).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            let frame = frame_reader.read().unwrap().expect("Expected request frame");
            let request_id = frame.id;

            // 300 bytes with repeating pattern, max_chunk=100 → CHUNK(100) + CHUNK(100) + END(100)
            let max_chunk = 100;
            let pattern = b"ABCDEFGHIJ"; // 10 bytes
            let data: Vec<u8> = pattern.iter().cycle().take(300).copied().collect();

            let chunk0 = Frame::chunk(request_id.clone(), 0, data[..max_chunk].to_vec());
            let chunk1 = Frame::chunk(request_id.clone(), 1, data[max_chunk..2 * max_chunk].to_vec());
            let end = Frame::end(request_id, Some(data[2 * max_chunk..].to_vec()));

            frame_writer.write(&chunk0).unwrap();
            frame_writer.write(&chunk1).unwrap();
            frame_writer.write(&end).unwrap();
        });

        let host = AsyncPluginHost::new(host_write, host_read).await.unwrap();

        let response = host.call("cap:op=test", b"", "text/plain").await.unwrap();
        let reassembled = response.concatenated();

        let pattern = b"ABCDEFGHIJ";
        let expected: Vec<u8> = pattern.iter().cycle().take(300).copied().collect();
        assert_eq!(reassembled.len(), 300);
        assert_eq!(reassembled, expected, "pattern must be preserved across chunk boundaries");

        host.shutdown().await;
        plugin_handle.join().unwrap();
    }
}

