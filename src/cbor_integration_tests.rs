//! Integration tests for CBOR plugin communication protocol.
//!
//! These tests verify the full path: engine → relay → runtime → plugin → response back.
//! The AsyncPluginHost manages multiple plugins and routes frames between a relay
//! connection and individual plugin processes.

#[cfg(test)]
mod tests {
    use crate::cbor_frame::{Frame, FrameType, MessageId};
    use crate::cbor_io::{FrameReader, FrameWriter, handshake_accept, AsyncFrameReader, AsyncFrameWriter};
    use crate::plugin_runtime::PluginRuntime;
    use std::io::{BufReader, BufWriter};

    /// Test manifest JSON - plugins MUST include manifest in HELLO response
    const TEST_MANIFEST: &str = r#"{"name":"TestPlugin","version":"1.0.0","description":"Test plugin","caps":[{"urn":"cap:op=test","title":"Test","command":"test"}]}"#;

    // TEST293: Test PluginRuntime handler registration and lookup by exact and non-existent cap URN
    #[test]
    fn test_plugin_runtime_handler_registration() {
        let mut runtime = PluginRuntime::new(TEST_MANIFEST.as_bytes());

        runtime.register::<serde_json::Value, _>("cap:op=echo", |req, emitter, _peer| {
            let bytes = serde_json::to_vec(&req).unwrap_or_default();
            let cbor_value = ciborium::Value::Bytes(bytes);
            emitter.emit_cbor(&cbor_value);
            Ok(())
        });

        runtime.register::<serde_json::Value, _>("cap:op=transform", |_req, emitter, _peer| {
            let cbor_value = ciborium::Value::Bytes(b"transformed".to_vec());
            emitter.emit_cbor(&cbor_value);
            Ok(())
        });

        // Exact match
        assert!(runtime.find_handler("cap:op=echo").is_some());
        assert!(runtime.find_handler("cap:op=transform").is_some());

        // Non-existent
        assert!(runtime.find_handler("cap:op=unknown").is_none());
    }

    /// Helper: create async socket pairs for relay (engine↔runtime).
    fn create_relay_pair() -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
        tokio::net::unix::OwnedWriteHalf,
        tokio::net::unix::OwnedReadHalf,
    ) {
        let (relay_rt_read_std, relay_eng_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (relay_eng_read_std, relay_rt_write_std) = std::os::unix::net::UnixStream::pair().unwrap();
        for s in [&relay_rt_read_std, &relay_rt_write_std, &relay_eng_write_std, &relay_eng_read_std] {
            s.set_nonblocking(true).unwrap();
        }
        let rt_read = tokio::net::UnixStream::from_std(relay_rt_read_std).unwrap();
        let rt_write = tokio::net::UnixStream::from_std(relay_rt_write_std).unwrap();
        let eng_write = tokio::net::UnixStream::from_std(relay_eng_write_std).unwrap();
        let eng_read = tokio::net::UnixStream::from_std(relay_eng_read_std).unwrap();

        let (rt_read_half, _) = rt_read.into_split();
        let (_, rt_write_half) = rt_write.into_split();
        let (_, eng_write_half) = eng_write.into_split();
        let (eng_read_half, _) = eng_read.into_split();

        (rt_read_half, rt_write_half, eng_write_half, eng_read_half)
    }

    /// Helper: create async+sync socket pairs for plugin↔runtime.
    fn create_plugin_pair() -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        let (p_to_rt_std, rt_from_p_std) = std::os::unix::net::UnixStream::pair().unwrap();
        let (rt_to_p_std, p_from_rt_std) = std::os::unix::net::UnixStream::pair().unwrap();
        rt_from_p_std.set_nonblocking(true).unwrap();
        rt_to_p_std.set_nonblocking(true).unwrap();

        let rt_from_p = tokio::net::UnixStream::from_std(rt_from_p_std).unwrap();
        let rt_to_p = tokio::net::UnixStream::from_std(rt_to_p_std).unwrap();
        let (p_read, _) = rt_from_p.into_split();
        let (_, p_write) = rt_to_p.into_split();

        (p_read, p_write, p_from_rt_std, p_to_rt_std)
    }

    /// Helper: do handshake on plugin side (sync, in a thread).
    fn plugin_handshake(
        from_runtime: std::os::unix::net::UnixStream,
        to_runtime: std::os::unix::net::UnixStream,
        manifest: &[u8],
    ) -> (FrameReader<BufReader<std::os::unix::net::UnixStream>>, FrameWriter<BufWriter<std::os::unix::net::UnixStream>>) {
        let mut reader = FrameReader::new(BufReader::new(from_runtime));
        let mut writer = FrameWriter::new(BufWriter::new(to_runtime));
        let limits = handshake_accept(&mut reader, &mut writer, manifest).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);
        (reader, writer)
    }

    // TEST426: Full path: engine REQ → runtime → plugin → response back through relay
    #[tokio::test]
    async fn test_full_path_engine_req_to_plugin_response() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"EchoPlugin","version":"1.0","caps":[{"urn":"cap:op=echo"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(p_from_rt, p_to_rt, &m);

            let req = reader.read().unwrap().expect("Expected REQ");
            assert_eq!(req.frame_type, FrameType::Req);
            assert_eq!(req.cap.as_deref(), Some("cap:op=echo"));

            let mut arg_data = Vec::new();
            loop {
                let f = reader.read().unwrap().expect("Expected frame");
                match f.frame_type {
                    FrameType::Chunk => arg_data.extend(f.payload.unwrap_or_default()),
                    FrameType::End => break,
                    _ => {}
                }
            }

            let sid = "resp".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            writer.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, arg_data)).unwrap();
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        // Engine task: send request, wait for response, THEN close relay
        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            let sid = uuid::Uuid::new_v4().to_string();
            w.write(&Frame::req(req_id.clone(), "cap:op=echo", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string())).await.unwrap();
            w.write(&Frame::chunk(req_id.clone(), sid.clone(), 0, b"hello world".to_vec())).await.unwrap();
            w.write(&Frame::stream_end(req_id.clone(), sid)).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            // Read response
            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk { payload.extend(f.payload.unwrap_or_default()); }
                        if f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w); // Close relay AFTER response received
            payload
        });

        let result = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;
        assert!(result.is_ok(), "Runtime should exit cleanly: {:?}", result);

        let response = engine_task.await.unwrap();
        assert_eq!(response, b"hello world", "Plugin should echo back the argument data");

        plugin_handle.join().unwrap();
    }

    // TEST427: Plugin ERR frame flows back to engine through relay
    #[tokio::test]
    async fn test_plugin_error_flows_to_engine() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"ErrPlugin","version":"1.0","caps":[{"urn":"cap:op=fail"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(p_from_rt, p_to_rt, &m);

            let req = reader.read().unwrap().expect("Expected REQ");
            writer.write(&Frame::err(req.id, "FAIL_CODE", "Something went wrong")).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            w.write(&Frame::req(req_id.clone(), "cap:op=fail", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            let mut err_code = String::new();
            let mut err_msg = String::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Err {
                            err_code = f.error_code().unwrap_or("").to_string();
                            err_msg = f.error_message().unwrap_or("").to_string();
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            (err_code, err_msg)
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let (code, msg) = engine_task.await.unwrap();
        assert_eq!(code, "FAIL_CODE");
        assert_eq!(msg, "Something went wrong");

        plugin_handle.join().unwrap();
    }

    // TEST428: Binary data integrity through full relay path (256 byte values)
    #[tokio::test]
    async fn test_binary_integrity_through_relay() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"BinPlugin","version":"1.0","caps":[{"urn":"cap:op=binary"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let binary_data: Vec<u8> = (0u16..=255).map(|i| i as u8).collect();
        let binary_clone = binary_data.clone();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(p_from_rt, p_to_rt, &m);

            let req = reader.read().unwrap().expect("Expected REQ");

            let mut received = Vec::new();
            loop {
                let f = reader.read().unwrap().expect("frame");
                match f.frame_type {
                    FrameType::Chunk => received.extend(f.payload.unwrap_or_default()),
                    FrameType::End => break,
                    _ => {}
                }
            }

            assert_eq!(received.len(), 256, "Must receive all 256 bytes");
            for (i, &b) in received.iter().enumerate() {
                assert_eq!(b, i as u8, "Byte mismatch at position {}", i);
            }

            let sid = "resp".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            writer.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, received)).unwrap();
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            let sid = uuid::Uuid::new_v4().to_string();
            w.write(&Frame::req(req_id.clone(), "cap:op=binary", vec![], "application/octet-stream")).await.unwrap();
            w.write(&Frame::stream_start(req_id.clone(), sid.clone(), "media:bytes".to_string())).await.unwrap();
            w.write(&Frame::chunk(req_id.clone(), sid.clone(), 0, binary_clone)).await.unwrap();
            w.write(&Frame::stream_end(req_id.clone(), sid)).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            let mut payload = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk { payload.extend(f.payload.unwrap_or_default()); }
                        if f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            payload
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let response = engine_task.await.unwrap();
        assert_eq!(response.len(), 256);
        for (i, &b) in response.iter().enumerate() {
            assert_eq!(b, i as u8, "Response byte mismatch at position {}", i);
        }

        plugin_handle.join().unwrap();
    }

    // TEST429: Streaming chunks flow through relay without accumulation
    #[tokio::test]
    async fn test_streaming_chunks_through_relay() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"StreamPlugin","version":"1.0","caps":[{"urn":"cap:op=stream"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(p_from_rt, p_to_rt, &m);

            let req = reader.read().unwrap().expect("Expected REQ");

            loop {
                let f = reader.read().unwrap().expect("frame");
                if f.frame_type == FrameType::End { break; }
            }

            let sid = "resp".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            for seq in 0u64..5 {
                let data = format!("chunk{}", seq).into_bytes();
                writer.write(&Frame::chunk(req.id.clone(), sid.clone(), seq, data)).unwrap();
            }
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            w.write(&Frame::req(req_id.clone(), "cap:op=stream", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(req_id.clone(), None)).await.unwrap();

            let mut chunks = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            chunks.push((f.seq, f.payload.unwrap_or_default()));
                        }
                        if f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            chunks
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let chunks = engine_task.await.unwrap();
        assert_eq!(chunks.len(), 5, "All 5 chunks must arrive");
        for (i, (seq, data)) in chunks.iter().enumerate() {
            assert_eq!(*seq, i as u64, "Chunk seq must be contiguous from 0");
            assert_eq!(data, &format!("chunk{}", i).into_bytes(), "Chunk data must match");
        }

        plugin_handle.join().unwrap();
    }

    // TEST430: Peer invoke: plugin REQ flows to engine through relay
    #[tokio::test]
    async fn test_peer_invoke_flows_to_engine() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"PeerPlugin","version":"1.0","caps":[{"urn":"cap:op=peer"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(p_from_rt, p_to_rt, &m);

            let req = reader.read().unwrap().expect("Expected REQ");
            loop {
                let f = reader.read().unwrap().expect("frame");
                if f.frame_type == FrameType::End { break; }
            }

            // Plugin issues peer invoke REQ
            let peer_req_id = MessageId::new_uuid();
            writer.write(&Frame::req(peer_req_id.clone(), "cap:op=other", vec![], "text/plain")).unwrap();
            writer.write(&Frame::end(peer_req_id.clone(), None)).unwrap();

            // Read peer invoke response (ERR from engine)
            loop {
                match reader.read() {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Err || f.frame_type == FrameType::End { break; }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Send original response
            let sid = "resp".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            writer.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"done".to_vec())).unwrap();
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let req_id_c = req_id.clone();
        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            // Send REQ to plugin
            w.write(&Frame::req(req_id_c.clone(), "cap:op=peer", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(req_id_c.clone(), None)).await.unwrap();

            let mut saw_peer_req = false;
            let mut response_data = Vec::new();
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Req && f.cap.as_deref() == Some("cap:op=other") {
                            saw_peer_req = true;
                            w.write(&Frame::err(f.id.clone(), "NOT_FOUND", "No handler")).await.unwrap();
                        }
                        if f.frame_type == FrameType::Chunk && f.id == req_id_c {
                            response_data.extend(f.payload.unwrap_or_default());
                        }
                        if f.frame_type == FrameType::End && f.id == req_id_c {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            drop(w);

            (saw_peer_req, response_data)
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let (saw_peer_req, response_data) = engine_task.await.unwrap();
        assert!(saw_peer_req, "Engine must see peer invoke REQ from plugin");
        assert_eq!(response_data, b"done", "Original response must flow back");

        plugin_handle.join().unwrap();
    }

    // TEST431: Two plugins routed independently by cap_urn
    #[tokio::test]
    async fn test_two_plugins_routed_independently() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest_a = r#"{"name":"PluginA","version":"1.0","caps":[{"urn":"cap:op=alpha"}]}"#;
        let manifest_b = r#"{"name":"PluginB","version":"1.0","caps":[{"urn":"cap:op=beta"}]}"#;

        let (pa_read, pa_write, pa_from_rt, pa_to_rt) = create_plugin_pair();
        let (pb_read, pb_write, pb_from_rt, pb_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let ma = manifest_a.as_bytes().to_vec();
        let plugin_a = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(pa_from_rt, pa_to_rt, &ma);
            let req = reader.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:op=alpha"), "Plugin A must receive alpha REQ");
            loop { let f = reader.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "a".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            writer.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"from-alpha".to_vec())).unwrap();
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mb = manifest_b.as_bytes().to_vec();
        let plugin_b = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(pb_from_rt, pb_to_rt, &mb);
            let req = reader.read().unwrap().expect("Expected REQ");
            assert_eq!(req.cap.as_deref(), Some("cap:op=beta"), "Plugin B must receive beta REQ");
            loop { let f = reader.read().unwrap().expect("f"); if f.frame_type == FrameType::End { break; } }
            let sid = "b".to_string();
            writer.write(&Frame::stream_start(req.id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            writer.write(&Frame::chunk(req.id.clone(), sid.clone(), 0, b"from-beta".to_vec())).unwrap();
            writer.write(&Frame::stream_end(req.id.clone(), sid)).unwrap();
            writer.write(&Frame::end(req.id, None)).unwrap();
            drop(writer);
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(pa_read, pa_write).await.unwrap();
        runtime.attach_plugin(pb_read, pb_write).await.unwrap();

        let alpha_id = MessageId::new_uuid();
        let beta_id = MessageId::new_uuid();
        let alpha_id_c = alpha_id.clone();
        let beta_id_c = beta_id.clone();

        let engine_task = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            let mut r = AsyncFrameReader::new(eng_read);

            w.write(&Frame::req(alpha_id_c.clone(), "cap:op=alpha", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(alpha_id_c.clone(), None)).await.unwrap();
            w.write(&Frame::req(beta_id_c.clone(), "cap:op=beta", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(beta_id_c.clone(), None)).await.unwrap();

            let mut alpha_data = Vec::new();
            let mut beta_data = Vec::new();
            let mut ends_received = 0;
            loop {
                match r.read().await {
                    Ok(Some(f)) => {
                        if f.frame_type == FrameType::Chunk {
                            if f.id == alpha_id_c { alpha_data.extend(f.payload.unwrap_or_default()); }
                            else if f.id == beta_id_c { beta_data.extend(f.payload.unwrap_or_default()); }
                        }
                        if f.frame_type == FrameType::End {
                            ends_received += 1;
                            if ends_received >= 2 { break; }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            drop(w);
            (alpha_data, beta_data)
        });

        let _ = runtime.run(rt_relay_read, rt_relay_write, || vec![]).await;

        let (alpha_data, beta_data) = engine_task.await.unwrap();
        assert_eq!(alpha_data, b"from-alpha", "Alpha response must come from Plugin A");
        assert_eq!(beta_data, b"from-beta", "Beta response must come from Plugin B");

        plugin_a.join().unwrap();
        plugin_b.join().unwrap();
    }

    // TEST432: REQ for unknown cap returns ERR frame (not fatal)
    #[tokio::test]
    async fn test_req_for_unknown_cap_returns_err_frame() {
        use crate::async_plugin_host::AsyncPluginHost;

        let manifest = r#"{"name":"OnePlugin","version":"1.0","caps":[{"urn":"cap:op=known"}]}"#;

        let (p_read, p_write, p_from_rt, p_to_rt) = create_plugin_pair();
        let (rt_relay_read, rt_relay_write, eng_write, eng_read) = create_relay_pair();

        let m = manifest.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, _writer) = plugin_handshake(p_from_rt, p_to_rt, &m);
            // Plugin waits for EOF — no REQ should arrive since cap is unknown
            match reader.read() {
                Ok(None) => {}
                Ok(Some(f)) => panic!("Plugin should not receive frames for unknown cap, got {:?}", f.frame_type),
                Err(_) => {}
            }
        });

        let mut runtime = AsyncPluginHost::new();
        runtime.attach_plugin(p_read, p_write).await.unwrap();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();
        let engine_send = tokio::spawn(async move {
            let mut w = AsyncFrameWriter::new(eng_write);
            w.write(&Frame::req(req_id_clone.clone(), "cap:op=unknown", vec![], "text/plain")).await.unwrap();
            w.write(&Frame::end(req_id_clone, None)).await.unwrap();
        });

        // Read ERR frame from the host on the engine side
        let engine_recv = tokio::spawn(async move {
            let mut r = AsyncFrameReader::new(eng_read);
            let frame = r.read().await.unwrap().expect("Expected ERR frame");
            assert_eq!(frame.frame_type, FrameType::Err, "Should get ERR for unknown cap");
            assert_eq!(frame.id, req_id, "ERR should reference the original request ID");
            let meta = frame.meta.as_ref().expect("ERR should have meta");
            let code = meta.get("code").and_then(|v| v.as_text()).unwrap_or("");
            assert_eq!(code, "NO_HANDLER", "Error code should be NO_HANDLER, got: {}", code);
        });

        // Host run should NOT return an error — it sends ERR frame and continues
        let run_handle = tokio::spawn(async move {
            runtime.run(rt_relay_read, rt_relay_write, || vec![]).await
        });

        engine_send.await.unwrap();
        engine_recv.await.unwrap();

        // Host is still running (waiting for more frames). Drop is fine — relay EOF will close it.
        drop(run_handle);
        plugin_handle.join().unwrap();
    }

    // =============================================================================
    // Low-level Frame-based Integration Tests (TEST284-299)
    // Ported from Go integration_test.go
    // =============================================================================

    /// Helper to create sync socket pairs for host-plugin communication
    fn create_sync_pipe_pair() -> (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        use std::os::unix::net::UnixStream;
        let (host_write, plugin_read) = UnixStream::pair().unwrap();
        let (plugin_write, host_read) = UnixStream::pair().unwrap();
        (host_write, plugin_read, plugin_write, host_read)
    }

    // TEST284: Handshake exchanges HELLO frames, negotiates limits
    #[test]
    fn test_handshake_host_plugin() {
        use crate::cbor_io::{handshake_initiate, handshake_accept};
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(plugin_read));
            let mut writer = FrameWriter::new(BufWriter::new(plugin_write));
            let limits = handshake_accept(&mut reader, &mut writer, &manifest).unwrap();
            assert!(limits.max_frame > 0);
            assert!(limits.max_chunk > 0);
            limits
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (received_manifest, host_limits) = handshake_initiate(&mut reader, &mut writer).unwrap();

        assert_eq!(received_manifest, TEST_MANIFEST.as_bytes());

        let plugin_limits = plugin_handle.join().unwrap();
        assert_eq!(host_limits.max_frame, plugin_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_limits.max_chunk);
    }

    // TEST285: Simple request-response flow (REQ → END with payload)
    #[test]
    fn test_request_response_simple() {
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            let frame = reader.read().unwrap().unwrap();
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some("cap:op=echo"));
            assert_eq!(frame.payload.as_deref(), Some(b"hello".as_ref()));

            writer.write(&Frame::end(frame.id, Some(b"hello back".to_vec()))).unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        let request_id = MessageId::new_uuid();
        writer.write(&Frame::req(request_id.clone(), "cap:op=echo", b"hello".to_vec(), "application/json")).unwrap();

        let response = reader.read().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::End);
        assert_eq!(response.payload.as_deref(), Some(b"hello back".as_ref()));

        plugin_handle.join().unwrap();
    }

    // TEST286: Streaming response with multiple CHUNK frames
    #[test]
    fn test_streaming_chunks() {
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            let frame = reader.read().unwrap().unwrap();
            let request_id = frame.id.clone();

            let sid = "response".to_string();
            writer.write(&Frame::stream_start(request_id.clone(), sid.clone(), "media:bytes".to_string())).unwrap();
            for (seq, data) in [b"chunk1", b"chunk2", b"chunk3"].iter().enumerate() {
                writer.write(&Frame::chunk(request_id.clone(), sid.clone(), seq as u64, data.to_vec())).unwrap();
            }
            writer.write(&Frame::stream_end(request_id.clone(), sid)).unwrap();
            writer.write(&Frame::end(request_id, None)).unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        let request_id = MessageId::new_uuid();
        writer.write(&Frame::req(request_id.clone(), "cap:op=stream", b"go".to_vec(), "application/json")).unwrap();

        // Collect chunks
        let mut chunks = Vec::new();
        loop {
            let frame = reader.read().unwrap().unwrap();
            if frame.frame_type == FrameType::Chunk {
                chunks.push(frame.payload.unwrap_or_default());
            }
            if frame.frame_type == FrameType::End {
                break;
            }
        }

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"chunk1");
        assert_eq!(chunks[1], b"chunk2");
        assert_eq!(chunks[2], b"chunk3");

        plugin_handle.join().unwrap();
    }

    // TEST287: Host-initiated heartbeat
    #[test]
    fn test_heartbeat_from_host() {
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            let frame = reader.read().unwrap().unwrap();
            assert_eq!(frame.frame_type, FrameType::Heartbeat);

            writer.write(&Frame::heartbeat(frame.id)).unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        let heartbeat_id = MessageId::new_uuid();
        writer.write(&Frame::heartbeat(heartbeat_id.clone())).unwrap();

        let response = reader.read().unwrap().unwrap();
        assert_eq!(response.frame_type, FrameType::Heartbeat);
        assert_eq!(response.id, heartbeat_id);

        plugin_handle.join().unwrap();
    }

    // TEST290: Limit negotiation picks minimum
    #[test]
    fn test_limits_negotiation() {
        use crate::cbor_io::{handshake_initiate, handshake_accept};
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(plugin_read));
            let mut writer = FrameWriter::new(BufWriter::new(plugin_write));
            handshake_accept(&mut reader, &mut writer, &manifest).unwrap()
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, host_limits) = handshake_initiate(&mut reader, &mut writer).unwrap();

        let plugin_limits = plugin_handle.join().unwrap();

        assert_eq!(host_limits.max_frame, plugin_limits.max_frame);
        assert_eq!(host_limits.max_chunk, plugin_limits.max_chunk);
        assert!(host_limits.max_frame > 0);
        assert!(host_limits.max_chunk > 0);
    }

    // TEST291: Binary payload roundtrip (all 256 byte values)
    #[test]
    fn test_binary_payload_roundtrip() {
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let binary_data: Vec<u8> = (0u16..=255).map(|i| i as u8).collect();
        let binary_clone = binary_data.clone();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            let frame = reader.read().unwrap().unwrap();
            let payload = frame.payload.unwrap();

            assert_eq!(payload.len(), 256);
            for (i, &byte) in payload.iter().enumerate() {
                assert_eq!(byte, i as u8, "Byte mismatch at position {}", i);
            }

            writer.write(&Frame::end(frame.id, Some(payload))).unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        let request_id = MessageId::new_uuid();
        writer.write(&Frame::req(request_id.clone(), "cap:op=binary", binary_clone, "application/octet-stream")).unwrap();

        let response = reader.read().unwrap().unwrap();
        let result = response.payload.unwrap();

        assert_eq!(result.len(), 256);
        for (i, &byte) in result.iter().enumerate() {
            assert_eq!(byte, i as u8, "Response byte mismatch at position {}", i);
        }

        plugin_handle.join().unwrap();
    }

    // TEST292: Sequential requests get distinct MessageIds
    #[test]
    fn test_message_id_uniqueness() {
        use std::io::{BufReader, BufWriter};
        use std::sync::{Arc, Mutex};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let received_ids = Arc::new(Mutex::new(Vec::new()));
        let received_ids_clone = Arc::clone(&received_ids);

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            for _ in 0..3 {
                let frame = reader.read().unwrap().unwrap();
                received_ids_clone.lock().unwrap().push(frame.id.clone());
                writer.write(&Frame::end(frame.id, Some(b"ok".to_vec()))).unwrap();
            }
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        for _ in 0..3 {
            let request_id = MessageId::new_uuid();
            writer.write(&Frame::req(request_id.clone(), "cap:op=test", vec![], "application/json")).unwrap();
            reader.read().unwrap().unwrap();
        }

        plugin_handle.join().unwrap();

        let ids = received_ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j], "IDs should be unique");
            }
        }
    }

    // TEST299: Empty payload request/response roundtrip
    #[test]
    fn test_empty_payload_roundtrip() {
        use std::io::{BufReader, BufWriter};

        let (host_write, plugin_read, plugin_write, host_read) = create_sync_pipe_pair();

        let manifest = TEST_MANIFEST.as_bytes().to_vec();
        let plugin_handle = std::thread::spawn(move || {
            let (mut reader, mut writer) = plugin_handshake(plugin_read, plugin_write, &manifest);

            let frame = reader.read().unwrap().unwrap();
            assert!(frame.payload.is_none() || frame.payload.as_ref().unwrap().is_empty(),
                    "empty payload must arrive empty");

            writer.write(&Frame::end(frame.id, Some(vec![]))).unwrap();
        });

        let mut reader = FrameReader::new(BufReader::new(host_read));
        let mut writer = FrameWriter::new(BufWriter::new(host_write));
        let (_, limits) = handshake_initiate(&mut reader, &mut writer).unwrap();
        reader.set_limits(limits);
        writer.set_limits(limits);

        let request_id = MessageId::new_uuid();
        writer.write(&Frame::req(request_id.clone(), "cap:op=empty", vec![], "application/json")).unwrap();

        let response = reader.read().unwrap().unwrap();
        assert!(response.payload.is_none() || response.payload.as_ref().unwrap().is_empty());

        plugin_handle.join().unwrap();
    }
}
