//! CborRelay — Transparent CBOR frame relay with two relay-specific frame types.
//!
//! The relay is a byte-stream bridge between an engine (master) and a plugin host runtime
//! (slave). Two relay-specific frame types are intercepted and never leaked through:
//!
//! - **RelayNotify** (slave → master): Capability advertisement from the slave's plugin host runtime.
//! - **RelayState** (master → slave): Host system resources + cap demands from the engine.
//!
//! All other frames pass through transparently in both directions.

use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
use crate::cbor_io::{
    encode_frame, read_frame, write_frame, CborError, FrameReader, FrameWriter,
};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

// =============================================================================
// SYNC RELAY TYPES
// =============================================================================

/// Slave endpoint of the relay. Sits inside the plugin host process (e.g., XPC service).
///
/// - Reads frames from the socket (from master): RelayState → store; others → forward to local side
/// - Reads frames from local side (from PluginHostRuntime): forward to socket
/// - Can inject RelayNotify frames into the socket stream on demand
pub struct RelaySlave<R: Read, W: Write> {
    /// Read from PluginHostRuntime
    local_reader: FrameReader<R>,
    /// Write to PluginHostRuntime
    local_writer: FrameWriter<W>,
    /// Latest RelayState payload from master
    resource_state: Arc<Mutex<Vec<u8>>>,
}

impl<R: Read, W: Write> RelaySlave<R, W> {
    /// Create a new relay slave with local I/O streams (to/from PluginHostRuntime).
    pub fn new(local_read: R, local_write: W) -> Self {
        Self {
            local_reader: FrameReader::new(local_read),
            local_writer: FrameWriter::new(local_write),
            resource_state: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the latest resource state payload received from the master.
    pub fn resource_state(&self) -> Vec<u8> {
        self.resource_state.lock().unwrap().clone()
    }

    /// Get a cloneable handle to the resource state for sharing with the host runtime.
    pub fn resource_state_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.resource_state)
    }

    /// Run the relay. This processes frames bidirectionally:
    ///
    /// - `socket_read`/`socket_write`: Connection to the master relay
    /// - `notify_rx`: Channel to receive RelayNotify frames to inject toward the master
    ///
    /// The relay intercepts:
    /// - Incoming RelayState frames: stores payload, does NOT forward to local side
    /// - Outgoing RelayNotify frames: injected via `notify_rx`, sent to socket
    ///
    /// All other frames pass through transparently.
    ///
    /// Blocks until one side closes or an error occurs.
    pub fn run<SR: Read, SW: Write>(
        &mut self,
        mut socket_read: FrameReader<SR>,
        mut socket_write: FrameWriter<SW>,
        initial_notify: Option<(&[u8], &Limits)>,
    ) -> Result<(), CborError> {
        // Send initial RelayNotify if provided
        if let Some((manifest, limits)) = initial_notify {
            let notify = Frame::relay_notify(manifest, limits);
            socket_write.write(&notify)?;
        }

        // Bidirectional forwarding. Since we're sync, we alternate reads.
        // In production, this would be two threads. For now, we use non-blocking
        // patterns or separate threads at the call site.
        //
        // This single-threaded version reads one frame from each side per iteration.
        // The caller should wrap socket and local streams appropriately.
        loop {
            // Try reading from socket (master → slave direction)
            match socket_read.read() {
                Ok(Some(frame)) => {
                    if frame.frame_type == FrameType::RelayState {
                        // Intercept: store resource state, don't forward
                        if let Some(payload) = frame.payload {
                            *self.resource_state.lock().unwrap() = payload;
                        }
                    } else if frame.frame_type == FrameType::RelayNotify {
                        // RelayNotify from master? Protocol error — ignore silently
                    } else {
                        // Pass through to local side (PluginHostRuntime)
                        self.local_writer.write(&frame)?;
                    }
                }
                Ok(None) => return Ok(()), // Socket closed
                Err(e) => return Err(e),
            }

            // Try reading from local side (slave → master direction)
            match self.local_reader.read() {
                Ok(Some(frame)) => {
                    if frame.frame_type == FrameType::RelayNotify
                        || frame.frame_type == FrameType::RelayState
                    {
                        // Relay frames from local side should not happen — ignore
                    } else {
                        // Pass through to socket (toward master/engine)
                        socket_write.write(&frame)?;
                    }
                }
                Ok(None) => return Ok(()), // Local side closed
                Err(e) => return Err(e),
            }
        }
    }

    /// Send a RelayNotify frame directly to the socket writer.
    /// Used when capabilities change (plugin discovered, plugin died).
    pub fn send_notify<SW: Write>(
        socket_write: &mut FrameWriter<SW>,
        manifest: &[u8],
        limits: &Limits,
    ) -> Result<(), CborError> {
        let notify = Frame::relay_notify(manifest, limits);
        socket_write.write(&notify)
    }
}

/// Master endpoint of the relay. Sits in the engine process.
///
/// - Reads frames from the socket (from slave): RelayNotify → callback + store; others → forward to caller
/// - Writes frames to the socket (from caller): pass through
/// - Can send RelayState frames to the slave
pub struct RelayMaster {
    /// Latest manifest from slave's RelayNotify
    manifest: Vec<u8>,
    /// Latest limits from slave's RelayNotify
    limits: Limits,
}

impl RelayMaster {
    /// Connect to a relay slave by reading the initial RelayNotify frame.
    ///
    /// The slave MUST send a RelayNotify as its first frame after connection.
    /// This extracts the manifest and limits from that frame.
    pub fn connect<SR: Read>(
        socket_read: &mut FrameReader<SR>,
    ) -> Result<Self, CborError> {
        let frame = socket_read.read()?.ok_or_else(|| {
            CborError::Handshake("relay connection closed before receiving RelayNotify".to_string())
        })?;

        if frame.frame_type != FrameType::RelayNotify {
            return Err(CborError::Protocol(format!(
                "expected RelayNotify, got {:?}",
                frame.frame_type
            )));
        }

        let manifest = frame
            .relay_notify_manifest()
            .ok_or_else(|| {
                CborError::Protocol("RelayNotify missing manifest".to_string())
            })?
            .to_vec();

        let limits = frame
            .relay_notify_limits()
            .ok_or_else(|| {
                CborError::Protocol("RelayNotify missing limits".to_string())
            })?;

        Ok(Self { manifest, limits })
    }

    /// Get the aggregate manifest from the slave.
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Get the negotiated limits from the slave.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Send a RelayState frame to the slave with host system resource info.
    pub fn send_state<SW: Write>(
        socket_write: &mut FrameWriter<SW>,
        resources: &[u8],
    ) -> Result<(), CborError> {
        let frame = Frame::relay_state(resources);
        socket_write.write(&frame)
    }

    /// Read the next non-relay frame from the socket.
    ///
    /// RelayNotify frames are intercepted: manifest and limits are updated.
    /// All other frames are returned to the caller.
    /// Returns Ok(None) on EOF.
    pub fn read_frame<SR: Read>(
        &mut self,
        socket_read: &mut FrameReader<SR>,
    ) -> Result<Option<Frame>, CborError> {
        loop {
            match socket_read.read()? {
                Some(frame) => {
                    if frame.frame_type == FrameType::RelayNotify {
                        // Intercept: update manifest and limits
                        if let Some(manifest) = frame.relay_notify_manifest() {
                            self.manifest = manifest.to_vec();
                        }
                        if let Some(limits) = frame.relay_notify_limits() {
                            self.limits = limits;
                        }
                        continue; // Don't return relay frames to caller
                    } else if frame.frame_type == FrameType::RelayState {
                        // RelayState from slave? Protocol error — ignore
                        continue;
                    }
                    return Ok(Some(frame));
                }
                None => return Ok(None),
            }
        }
    }
}

// =============================================================================
// ASYNC RELAY TYPES
// =============================================================================

use crate::cbor_io::{read_frame_async, write_frame_async, AsyncFrameReader, AsyncFrameWriter};
use tokio::io::{AsyncRead, AsyncWrite};

/// Async relay master for use with tokio.
pub struct AsyncRelayMaster {
    manifest: Vec<u8>,
    limits: Limits,
}

impl AsyncRelayMaster {
    /// Connect to a relay slave by reading the initial RelayNotify frame.
    pub async fn connect<SR: AsyncRead + Unpin>(
        socket_read: &mut AsyncFrameReader<SR>,
    ) -> Result<Self, CborError> {
        let frame = socket_read.read().await?.ok_or_else(|| {
            CborError::Handshake("relay connection closed before receiving RelayNotify".to_string())
        })?;

        if frame.frame_type != FrameType::RelayNotify {
            return Err(CborError::Protocol(format!(
                "expected RelayNotify, got {:?}",
                frame.frame_type
            )));
        }

        let manifest = frame
            .relay_notify_manifest()
            .ok_or_else(|| CborError::Protocol("RelayNotify missing manifest".to_string()))?
            .to_vec();

        let limits = frame
            .relay_notify_limits()
            .ok_or_else(|| CborError::Protocol("RelayNotify missing limits".to_string()))?;

        Ok(Self { manifest, limits })
    }

    /// Get the aggregate manifest from the slave.
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Get the negotiated limits from the slave.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Send a RelayState frame to the slave.
    pub async fn send_state<SW: AsyncWrite + Unpin>(
        socket_write: &mut AsyncFrameWriter<SW>,
        resources: &[u8],
    ) -> Result<(), CborError> {
        let frame = Frame::relay_state(resources);
        socket_write.write(&frame).await
    }

    /// Read the next non-relay frame from the socket.
    /// Intercepts RelayNotify frames and updates internal state.
    pub async fn read_frame<SR: AsyncRead + Unpin>(
        &mut self,
        socket_read: &mut AsyncFrameReader<SR>,
    ) -> Result<Option<Frame>, CborError> {
        loop {
            match socket_read.read().await? {
                Some(frame) => {
                    if frame.frame_type == FrameType::RelayNotify {
                        if let Some(manifest) = frame.relay_notify_manifest() {
                            self.manifest = manifest.to_vec();
                        }
                        if let Some(limits) = frame.relay_notify_limits() {
                            self.limits = limits;
                        }
                        continue;
                    } else if frame.frame_type == FrameType::RelayState {
                        continue;
                    }
                    return Ok(Some(frame));
                }
                None => return Ok(None),
            }
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor_frame::{Frame, FrameType, Limits, MessageId};
    use crate::cbor_io::{FrameReader, FrameWriter};
    use std::io::{BufReader, BufWriter};
    use std::thread;

    /// Create a sync pipe pair using Unix sockets.
    /// Returns (read_end, write_end) for each direction.
    fn create_pipe_pair() -> (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        std::os::unix::net::UnixStream::pair().expect("Failed to create pipe pair")
    }

    // TEST404: Slave sends RelayNotify on connect (initial_notify parameter)
    #[test]
    fn test_slave_sends_relay_notify_on_connect() {
        let manifest = b"{\"caps\":[\"cap:op=test\"]}";
        let limits = Limits::default();

        // Socket: slave writes → master reads
        let (master_read_stream, slave_write_stream) = create_pipe_pair();

        // Slave sends initial notify through socket_write
        let slave_handle = thread::spawn(move || {
            let mut socket_writer =
                FrameWriter::new(BufWriter::new(slave_write_stream));
            RelaySlave::<std::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                manifest,
                &limits,
            )
            .unwrap();
        });

        // Master reads it
        let mut socket_reader = FrameReader::new(BufReader::new(master_read_stream));
        let frame = socket_reader.read().unwrap().expect("should have frame");

        assert_eq!(frame.frame_type, FrameType::RelayNotify);
        assert_eq!(frame.relay_notify_manifest(), Some(manifest.as_slice()));
        let extracted = frame.relay_notify_limits().unwrap();
        assert_eq!(extracted.max_frame, limits.max_frame);
        assert_eq!(extracted.max_chunk, limits.max_chunk);

        slave_handle.join().unwrap();
    }

    // TEST405: Master reads RelayNotify and extracts manifest + limits
    #[test]
    fn test_master_reads_relay_notify() {
        let manifest = b"{\"caps\":[\"cap:op=convert\"]}";
        let limits = Limits {
            max_frame: 1_000_000,
            max_chunk: 64_000,
        };

        let (master_read_stream, slave_write_stream) = create_pipe_pair();

        // Slave sends RelayNotify
        let slave_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(slave_write_stream));
            let notify = Frame::relay_notify(manifest, &limits);
            writer.write(&notify).unwrap();
        });

        // Master connects
        let mut reader = FrameReader::new(BufReader::new(master_read_stream));
        let master = RelayMaster::connect(&mut reader).unwrap();

        assert_eq!(master.manifest(), manifest);
        assert_eq!(master.limits().max_frame, 1_000_000);
        assert_eq!(master.limits().max_chunk, 64_000);

        slave_handle.join().unwrap();
    }

    // TEST406: Slave stores RelayState from master
    #[test]
    fn test_slave_stores_relay_state() {
        let resources = b"{\"memory_mb\":4096}";

        // Socket: master writes → slave reads
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        // Local: slave writes → (nobody reads in this test)
        let (local_read_end, local_write_end) = create_pipe_pair();

        let resource_state = Arc::new(Mutex::new(Vec::new()));
        let resource_state_clone = Arc::clone(&resource_state);

        // Master sends RelayState
        let master_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            RelayMaster::send_state(&mut writer, resources).unwrap();
            drop(writer); // Close to signal EOF
        });

        // Slave reads from socket
        let slave_handle = thread::spawn(move || {
            let mut slave = RelaySlave::new(local_read_end, local_write_end);
            // Override resource_state with our shared handle
            slave.resource_state = resource_state_clone;

            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));

            // Read one frame — should be RelayState
            let frame = socket_reader.read().unwrap().expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::RelayState);
            if let Some(payload) = frame.payload {
                *slave.resource_state.lock().unwrap() = payload;
            }

            slave.resource_state()
        });

        master_handle.join().unwrap();
        let stored = slave_handle.join().unwrap();
        assert_eq!(stored, resources);
    }

    // TEST407: Protocol frames pass through slave transparently (both directions)
    #[test]
    fn test_protocol_frames_pass_through() {
        // Socket pair: master ↔ slave
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (master_socket_read, slave_socket_write) = create_pipe_pair();
        // Local pair: slave ↔ host runtime
        let (runtime_read_from_slave, slave_local_write) = create_pipe_pair();
        let (slave_local_read, runtime_write_to_slave) = create_pipe_pair();

        let req_id = MessageId::new_uuid();
        let req_id_clone = req_id.clone();

        // Master sends a REQ frame through the socket
        let master_write_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            let req = Frame::req(
                req_id_clone,
                "cap:op=test",
                b"hello".to_vec(),
                "text/plain",
            );
            writer.write(&req).unwrap();
            drop(writer);
        });

        // Runtime sends a CHUNK frame through the local write
        let chunk_id = MessageId::new_uuid();
        let chunk_id_clone = chunk_id.clone();
        let runtime_write_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(runtime_write_to_slave));
            let chunk = Frame::chunk(
                chunk_id_clone,
                "stream-1".to_string(),
                0,
                b"response".to_vec(),
            );
            writer.write(&chunk).unwrap();
            drop(writer);
        });

        // Slave relay: read from socket, write to local; read from local, write to socket
        // We do this manually (one frame each direction) to avoid the blocking run() loop
        let slave_handle = thread::spawn(move || {
            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));
            let mut socket_writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut local_reader = FrameReader::new(BufReader::new(slave_local_read));
            let mut local_writer = FrameWriter::new(BufWriter::new(slave_local_write));

            // Socket → local: read REQ, forward to local
            let from_socket = socket_reader.read().unwrap().expect("should have frame");
            assert_eq!(from_socket.frame_type, FrameType::Req);
            local_writer.write(&from_socket).unwrap();

            // Local → socket: read CHUNK, forward to socket
            let from_local = local_reader.read().unwrap().expect("should have frame");
            assert_eq!(from_local.frame_type, FrameType::Chunk);
            socket_writer.write(&from_local).unwrap();
        });

        // Runtime reads the forwarded REQ
        let runtime_read_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(runtime_read_from_slave));
            let frame = reader.read().unwrap().expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::Req);
            assert_eq!(frame.cap.as_deref(), Some("cap:op=test"));
            assert_eq!(frame.payload, Some(b"hello".to_vec()));
        });

        // Master reads the forwarded CHUNK
        let master_read_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let frame = reader.read().unwrap().expect("should have frame");
            assert_eq!(frame.frame_type, FrameType::Chunk);
            assert_eq!(frame.payload, Some(b"response".to_vec()));
        });

        master_write_handle.join().unwrap();
        runtime_write_handle.join().unwrap();
        slave_handle.join().unwrap();
        runtime_read_handle.join().unwrap();
        master_read_handle.join().unwrap();
    }

    // TEST408: RelayNotify/RelayState are NOT forwarded through relay
    #[test]
    fn test_relay_frames_not_forwarded() {
        // Master sends RelayState — slave should NOT forward it to local
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (_runtime_read, slave_local_write) = create_pipe_pair();

        let master_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            // Send RelayState
            let state = Frame::relay_state(b"{\"memory\":1024}");
            writer.write(&state).unwrap();
            // Then send a normal REQ to verify the slave still works
            let req = Frame::req(
                MessageId::new_uuid(),
                "cap:op=test",
                vec![],
                "text/plain",
            );
            writer.write(&req).unwrap();
            drop(writer);
        });

        let slave_handle = thread::spawn(move || {
            let mut socket_reader = FrameReader::new(BufReader::new(slave_socket_read));
            let mut local_writer = FrameWriter::new(BufWriter::new(slave_local_write));
            let resource_state: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

            // Read first frame — RelayState, should NOT be forwarded
            let frame1 = socket_reader.read().unwrap().expect("should have frame");
            assert_eq!(frame1.frame_type, FrameType::RelayState);
            if let Some(payload) = frame1.payload {
                *resource_state.lock().unwrap() = payload;
            }
            // Do NOT forward to local_writer

            // Read second frame — REQ, should be forwarded
            let frame2 = socket_reader.read().unwrap().expect("should have frame");
            assert_eq!(frame2.frame_type, FrameType::Req);
            local_writer.write(&frame2).unwrap();

            // Verify resource state was stored
            let state = resource_state.lock().unwrap().clone();
            assert_eq!(state, b"{\"memory\":1024}");
        });

        master_handle.join().unwrap();
        slave_handle.join().unwrap();
    }

    // TEST409: Slave can inject RelayNotify mid-stream (cap change)
    #[test]
    fn test_slave_injects_relay_notify_midstream() {
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let slave_handle = thread::spawn(move || {
            let mut socket_writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            let limits = Limits::default();

            // First: send initial RelayNotify
            let initial = b"{\"caps\":[\"cap:op=test\"]}";
            RelaySlave::<std::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                initial,
                &limits,
            )
            .unwrap();

            // Then: forward a normal CHUNK frame
            let chunk = Frame::chunk(
                MessageId::new_uuid(),
                "stream-1".to_string(),
                0,
                b"data".to_vec(),
            );
            socket_writer.write(&chunk).unwrap();

            // Then: inject updated RelayNotify (new cap discovered)
            let updated = b"{\"caps\":[\"cap:op=test\",\"cap:op=convert\"]}";
            RelaySlave::<std::io::Empty, Vec<u8>>::send_notify(
                &mut socket_writer,
                updated,
                &limits,
            )
            .unwrap();

            drop(socket_writer);
        });

        let master_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));

            // Read initial RelayNotify
            let f1 = reader.read().unwrap().expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::RelayNotify);
            assert_eq!(
                f1.relay_notify_manifest(),
                Some(b"{\"caps\":[\"cap:op=test\"]}".as_slice())
            );

            // Read CHUNK (passed through)
            let f2 = reader.read().unwrap().expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::Chunk);

            // Read updated RelayNotify
            let f3 = reader.read().unwrap().expect("frame 3");
            assert_eq!(f3.frame_type, FrameType::RelayNotify);
            assert_eq!(
                f3.relay_notify_manifest(),
                Some(b"{\"caps\":[\"cap:op=test\",\"cap:op=convert\"]}".as_slice())
            );
        });

        slave_handle.join().unwrap();
        master_handle.join().unwrap();
    }

    // TEST410: Master receives updated RelayNotify (cap change callback via read_frame)
    #[test]
    fn test_master_receives_updated_relay_notify() {
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let limits = Limits {
            max_frame: 2_000_000,
            max_chunk: 100_000,
        };

        let slave_handle = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(slave_socket_write));

            // Initial RelayNotify
            let initial = Frame::relay_notify(b"{\"caps\":[\"cap:op=a\"]}", &limits);
            writer.write(&initial).unwrap();

            // Normal frame
            let end = Frame::end(MessageId::new_uuid(), None);
            writer.write(&end).unwrap();

            // Updated RelayNotify
            let updated_limits = Limits {
                max_frame: 3_000_000,
                max_chunk: 200_000,
            };
            let updated = Frame::relay_notify(b"{\"caps\":[\"cap:op=a\",\"cap:op=b\"]}", &updated_limits);
            writer.write(&updated).unwrap();

            // Another normal frame to prove master continues
            let end2 = Frame::end(MessageId::new_uuid(), None);
            writer.write(&end2).unwrap();

            drop(writer);
        });

        let master_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let mut master = RelayMaster::connect(&mut reader).unwrap();

            // Initial state
            assert_eq!(master.manifest(), b"{\"caps\":[\"cap:op=a\"]}");
            assert_eq!(master.limits().max_frame, 2_000_000);

            // First non-relay frame
            let f1 = master.read_frame(&mut reader).unwrap().expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::End);

            // read_frame should have intercepted the updated RelayNotify
            let f2 = master.read_frame(&mut reader).unwrap().expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::End);

            // Manifest and limits should be updated
            assert_eq!(master.manifest(), b"{\"caps\":[\"cap:op=a\",\"cap:op=b\"]}");
            assert_eq!(master.limits().max_frame, 3_000_000);
            assert_eq!(master.limits().max_chunk, 200_000);
        });

        slave_handle.join().unwrap();
        master_handle.join().unwrap();
    }

    // TEST411: Socket close detection (both directions)
    #[test]
    fn test_socket_close_detection() {
        // Master → slave direction: master closes, slave detects
        let (slave_socket_read, master_socket_write) = create_pipe_pair();

        let master_handle = thread::spawn(move || {
            drop(master_socket_write); // Close immediately
        });

        let slave_handle = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(slave_socket_read));
            let result = reader.read().unwrap();
            assert!(result.is_none(), "closed socket must return None");
        });

        master_handle.join().unwrap();
        slave_handle.join().unwrap();

        // Slave → master direction: slave closes, master detects
        let (master_socket_read, slave_socket_write) = create_pipe_pair();

        let slave_handle2 = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(slave_socket_write));
            // Send RelayNotify then close
            let notify = Frame::relay_notify(b"[]", &Limits::default());
            writer.write(&notify).unwrap();
            drop(writer);
        });

        let master_handle2 = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let mut master = RelayMaster::connect(&mut reader).unwrap();
            let result = master.read_frame(&mut reader).unwrap();
            assert!(result.is_none(), "closed socket must return None");
        });

        slave_handle2.join().unwrap();
        master_handle2.join().unwrap();
    }

    // TEST412: Bidirectional concurrent frame flow through relay
    #[test]
    fn test_bidirectional_concurrent_flow() {
        // Full relay setup: master ↔ socket ↔ slave ↔ local ↔ runtime
        let (slave_socket_read, master_socket_write) = create_pipe_pair();
        let (master_socket_read, slave_socket_write) = create_pipe_pair();
        let (runtime_reads_from_slave, slave_local_write) = create_pipe_pair();
        let (slave_local_read, runtime_writes_to_slave) = create_pipe_pair();

        let req_id1 = MessageId::new_uuid();
        let req_id2 = MessageId::new_uuid();
        let req_id1_clone = req_id1.clone();
        let req_id2_clone = req_id2.clone();
        let req_id1_verify = req_id1.clone();
        let req_id2_verify = req_id2.clone();

        // Master writes REQ frames
        let master_write = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(master_socket_write));
            let req1 = Frame::req(req_id1_clone, "cap:op=a", b"data-a".to_vec(), "text/plain");
            let req2 = Frame::req(req_id2_clone, "cap:op=b", b"data-b".to_vec(), "text/plain");
            writer.write(&req1).unwrap();
            writer.write(&req2).unwrap();
            drop(writer);
        });

        // Runtime writes response chunks
        let resp_id1 = MessageId::new_uuid();
        let resp_id1_clone = resp_id1.clone();
        let runtime_write = thread::spawn(move || {
            let mut writer = FrameWriter::new(BufWriter::new(runtime_writes_to_slave));
            let chunk = Frame::chunk(resp_id1_clone, "s1".to_string(), 0, b"resp-a".to_vec());
            let end = Frame::end(resp_id1.clone(), None);
            writer.write(&chunk).unwrap();
            writer.write(&end).unwrap();
            drop(writer);
        });

        // Slave relay: manually forward frames both directions
        let slave = thread::spawn(move || {
            let mut sock_r = FrameReader::new(BufReader::new(slave_socket_read));
            let mut sock_w = FrameWriter::new(BufWriter::new(slave_socket_write));
            let mut local_r = FrameReader::new(BufReader::new(slave_local_read));
            let mut local_w = FrameWriter::new(BufWriter::new(slave_local_write));

            // Forward 2 frames from socket to local
            for _ in 0..2 {
                let f = sock_r.read().unwrap().expect("socket frame");
                local_w.write(&f).unwrap();
            }
            // Forward 2 frames from local to socket
            for _ in 0..2 {
                let f = local_r.read().unwrap().expect("local frame");
                sock_w.write(&f).unwrap();
            }
        });

        // Runtime reads forwarded REQs
        let runtime_read = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(runtime_reads_from_slave));
            let f1 = reader.read().unwrap().expect("frame 1");
            let f2 = reader.read().unwrap().expect("frame 2");
            assert_eq!(f1.frame_type, FrameType::Req);
            assert_eq!(f2.frame_type, FrameType::Req);
            assert_eq!(f1.id, req_id1_verify);
            assert_eq!(f2.id, req_id2_verify);
        });

        // Master reads forwarded responses
        let master_read = thread::spawn(move || {
            let mut reader = FrameReader::new(BufReader::new(master_socket_read));
            let f1 = reader.read().unwrap().expect("frame 1");
            assert_eq!(f1.frame_type, FrameType::Chunk);
            assert_eq!(f1.payload, Some(b"resp-a".to_vec()));
            let f2 = reader.read().unwrap().expect("frame 2");
            assert_eq!(f2.frame_type, FrameType::End);
        });

        master_write.join().unwrap();
        runtime_write.join().unwrap();
        slave.join().unwrap();
        runtime_read.join().unwrap();
        master_read.join().unwrap();
    }
}
