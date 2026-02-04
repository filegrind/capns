//! Integration tests for CBOR plugin communication protocol.
//!
//! These tests verify that PluginRuntime (plugin side) and PluginHost (host side)
//! can communicate correctly through pipes. This is the ONLY supported communication
//! method - no fallbacks, no alternatives.
//!
//! Tests use OS pipes to simulate real stdin/stdout communication between processes.

#[cfg(test)]
mod tests {
    use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
    use crate::cbor_io::{FrameReader, FrameWriter, handshake, handshake_accept};
    use crate::plugin_host::PluginHost;
    use crate::plugin_runtime::PluginRuntime;
    use std::io::{BufReader, BufWriter};
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// Create a pair of connected pipes for testing.
    /// Returns (host_write, plugin_read, plugin_write, host_read)
    fn create_pipe_pair() -> (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        // Host -> Plugin pipe
        let (host_write, plugin_read) =
            std::os::unix::net::UnixStream::pair().expect("Failed to create pipe pair");
        // Plugin -> Host pipe
        let (plugin_write, host_read) =
            std::os::unix::net::UnixStream::pair().expect("Failed to create pipe pair");

        (host_write, plugin_read, plugin_write, host_read)
    }

    #[test]
    fn test_handshake_host_plugin() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side in separate thread
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Accept handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer)
                .expect("Plugin handshake failed");

            assert!(limits.max_frame > 0);
            assert!(limits.max_chunk > 0);
            limits
        });

        // Host side
        let reader = BufReader::new(host_read);
        let writer = BufWriter::new(host_write);
        let mut frame_reader = FrameReader::new(reader);
        let mut frame_writer = FrameWriter::new(writer);

        // Initiate handshake
        let host_limits =
            handshake(&mut frame_reader, &mut frame_writer).expect("Host handshake failed");

        let plugin_limits = plugin_handle.join().expect("Plugin thread panicked");

        // Both should have negotiated the same limits
        assert_eq!(host_limits.max_frame, plugin_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_limits.max_chunk);
    }

    #[test]
    fn test_request_response_simple() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: accept handshake and respond to request
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Send request
        let response = host.call("cap:op=echo", b"hello").expect("Call failed");

        assert_eq!(response.concatenated(), b"hello back");

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_streaming_chunks() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: send multiple chunks
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Call and collect streaming response
        let response = host
            .call_streaming("cap:op=stream", b"go")
            .expect("Call streaming failed");

        let chunks: Vec<_> = response.collect();
        assert_eq!(chunks.len(), 3);

        // Verify chunk contents
        let payloads: Vec<_> = chunks
            .into_iter()
            .map(|r| r.expect("Chunk error").payload)
            .collect();
        assert_eq!(payloads[0], b"chunk1");
        assert_eq!(payloads[1], b"chunk2");
        assert_eq!(payloads[2], b"chunk3");

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_heartbeat_from_host() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: respond to heartbeats
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read heartbeat
            let frame = frame_reader.read().unwrap().expect("Expected heartbeat frame");
            assert_eq!(frame.frame_type, FrameType::Heartbeat);

            // Respond with heartbeat (same ID)
            let response = Frame::heartbeat(frame.id);
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Send heartbeat
        host.send_heartbeat().expect("Heartbeat failed");

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_plugin_error_response() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: respond with error
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Send error
            let err = Frame::err(frame.id, "NOT_FOUND", "Cap not found: cap:op=missing");
            frame_writer.write(&err).unwrap();
        });

        // Host side
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Call should fail with plugin error
        let result = host.call("cap:op=missing", b"");
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            crate::plugin_host::HostError::PluginError { code, message } => {
                assert_eq!(code, "NOT_FOUND");
                assert!(message.contains("Cap not found"));
            }
            _ => panic!("Expected PluginError, got {:?}", err),
        }

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_log_frames_during_request() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: send logs before response
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Call should succeed despite log frames
        let response = host.call("cap:op=test", b"").expect("Call failed");
        assert_eq!(response.concatenated(), b"done");

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_limits_negotiation() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: use smaller limits
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Read host's HELLO
            let host_hello = frame_reader.read().unwrap().expect("Expected HELLO");
            assert_eq!(host_hello.frame_type, FrameType::Hello);

            // Send our HELLO with smaller limits
            let small_limits = Limits {
                max_frame: 500_000,
                max_chunk: 50_000,
            };
            let our_hello = Frame::hello(small_limits.max_frame, small_limits.max_chunk);
            frame_writer.write(&our_hello).unwrap();

            small_limits
        });

        // Host side
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");

        let host_limits = host.limits();
        let plugin_small_limits = plugin_handle.join().expect("Plugin thread panicked");

        // Host should have negotiated to the smaller limits
        assert_eq!(host_limits.max_frame, plugin_small_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_small_limits.max_chunk);
        assert_eq!(host_limits.max_frame, 500_000);
        assert_eq!(host_limits.max_chunk, 50_000);
    }

    #[test]
    fn test_binary_payload_roundtrip() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

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

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Send binary data
        let response = host.call("cap:op=binary", &binary_clone).expect("Call failed");
        let result = response.concatenated();

        // Verify response matches
        assert_eq!(result.len(), 256);
        for (i, &byte) in result.iter().enumerate() {
            assert_eq!(byte, i as u8, "Response byte mismatch at position {}", i);
        }

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_message_id_uniqueness() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        let received_ids = Arc::new(Mutex::new(Vec::<MessageId>::new()));
        let received_ids_clone = received_ids.clone();

        // Plugin side: collect request IDs
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Send requests one by one
        for i in 0..3 {
            let _response = host.call(&format!("cap:op=test{}", i), b"").expect("Call failed");
        }

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
        let mut runtime = PluginRuntime::new();

        runtime.register::<serde_json::Value, _>("cap:op=echo", |req, _emitter| {
            // Serialize back to bytes
            Ok(serde_json::to_vec(&req).unwrap_or_default())
        });

        runtime.register::<serde_json::Value, _>("cap:op=transform", |_req, _emitter| {
            Ok(b"transformed".to_vec())
        });

        // Exact match
        assert!(runtime.find_handler("cap:op=echo").is_some());
        assert!(runtime.find_handler("cap:op=transform").is_some());

        // Non-existent
        assert!(runtime.find_handler("cap:op=unknown").is_none());
    }

    #[test]
    fn test_heartbeat_during_streaming() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: send heartbeat between chunks
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
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
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        // Call streaming - should handle heartbeat mid-stream
        let response = host.call_streaming("cap:op=stream", b"").expect("Call streaming failed");
        let chunks: Vec<_> = response.filter_map(|r| r.ok()).collect();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].payload, b"part1");
        assert_eq!(chunks[1].payload, b"part2");

        plugin_handle.join().expect("Plugin thread panicked");
    }

    #[test]
    fn test_res_frame_single_response() {
        let (host_write, plugin_read, plugin_write, host_read) = create_pipe_pair();

        // Plugin side: respond with RES frame (not END)
        let plugin_handle = thread::spawn(move || {
            let reader = BufReader::new(plugin_read);
            let writer = BufWriter::new(plugin_write);
            let mut frame_reader = FrameReader::new(reader);
            let mut frame_writer = FrameWriter::new(writer);

            // Handshake
            let limits = handshake_accept(&mut frame_reader, &mut frame_writer).unwrap();
            frame_reader.set_limits(limits);
            frame_writer.set_limits(limits);

            // Read request
            let frame = frame_reader.read().unwrap().expect("Expected request frame");

            // Send RES frame (single complete response)
            let response = Frame::res(frame.id, b"single response".to_vec(), "application/octet-stream");
            frame_writer.write(&response).unwrap();
        });

        // Host side
        let host = PluginHost::new(host_write, host_read).expect("Host creation failed");
        let mut host = host;

        let response = host.call("cap:op=single", b"").expect("Call failed");
        assert_eq!(response.concatenated(), b"single response");

        plugin_handle.join().expect("Plugin thread panicked");
    }
}
