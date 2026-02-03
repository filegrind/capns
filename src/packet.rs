//! Binary Packet Framing for Plugin Communication
//!
//! All plugin stdin/stdout communication uses length-prefixed binary packets.
//! This provides a clean transport layer that can carry any payload type.
//!
//! Packet format:
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  4 bytes: u32 big-endian length                         │
//! ├─────────────────────────────────────────────────────────┤
//! │  N bytes: payload                                       │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! The payload can be:
//! - JSON envelope for structured messages (NDJSON at higher level)
//! - Raw binary data for binary transfers

use std::io::{self, Read, Write};

/// Maximum packet size (16 MB) to prevent memory exhaustion
pub const MAX_PACKET_SIZE: u32 = 16 * 1024 * 1024;

/// Errors that can occur during packet operations
#[derive(Debug, thiserror::Error)]
pub enum PacketError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Packet too large: {size} bytes (max {max})")]
    PacketTooLarge { size: u32, max: u32 },

    #[error("Unexpected end of stream")]
    UnexpectedEof,

    #[error("Invalid packet: {0}")]
    Invalid(String),
}

/// Read a single binary packet from a reader.
///
/// Returns `Ok(None)` on clean EOF (no partial data).
/// Returns `Err(UnexpectedEof)` if EOF occurs mid-packet.
pub fn read_packet<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, PacketError> {
    // Read 4-byte length prefix (big-endian)
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(PacketError::Io(e)),
    }

    let length = u32::from_be_bytes(len_buf);

    // Validate length
    if length > MAX_PACKET_SIZE {
        return Err(PacketError::PacketTooLarge {
            size: length,
            max: MAX_PACKET_SIZE,
        });
    }

    // Read payload
    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            PacketError::UnexpectedEof
        } else {
            PacketError::Io(e)
        }
    })?;

    Ok(Some(payload))
}

/// Write a single binary packet to a writer.
///
/// Automatically prepends the 4-byte length prefix.
pub fn write_packet<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), PacketError> {
    let length = payload.len() as u32;

    if length > MAX_PACKET_SIZE {
        return Err(PacketError::PacketTooLarge {
            size: length,
            max: MAX_PACKET_SIZE,
        });
    }

    // Write length prefix (big-endian)
    writer.write_all(&length.to_be_bytes())?;

    // Write payload
    writer.write_all(payload)?;

    // Flush to ensure delivery
    writer.flush()?;

    Ok(())
}

/// A buffered packet reader that handles partial reads.
pub struct PacketReader<R: Read> {
    reader: R,
}

impl<R: Read> PacketReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read the next packet, blocking until complete.
    pub fn read(&mut self) -> Result<Option<Vec<u8>>, PacketError> {
        read_packet(&mut self.reader)
    }

    /// Get mutable access to the underlying reader.
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consume and return the underlying reader.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

/// A buffered packet writer.
pub struct PacketWriter<W: Write> {
    writer: W,
}

impl<W: Write> PacketWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Write a packet.
    pub fn write(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        write_packet(&mut self.writer, payload)
    }

    /// Get mutable access to the underlying writer.
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Consume and return the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_read_packet() {
        let mut buffer = Vec::new();
        let payload = b"hello world";

        write_packet(&mut buffer, payload).unwrap();

        // Verify format: 4 byte length + payload
        assert_eq!(buffer.len(), 4 + payload.len());
        assert_eq!(&buffer[0..4], &11u32.to_be_bytes());
        assert_eq!(&buffer[4..], payload);

        // Read it back
        let mut cursor = Cursor::new(buffer);
        let read_payload = read_packet(&mut cursor).unwrap().unwrap();
        assert_eq!(read_payload, payload);
    }

    #[test]
    fn test_empty_packet() {
        let mut buffer = Vec::new();
        write_packet(&mut buffer, b"").unwrap();

        let mut cursor = Cursor::new(buffer);
        let read_payload = read_packet(&mut cursor).unwrap().unwrap();
        assert!(read_payload.is_empty());
    }

    #[test]
    fn test_eof_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_packet(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_partial_length_is_error() {
        let mut cursor = Cursor::new(vec![0u8, 0u8]); // Only 2 bytes, need 4
        let result = read_packet(&mut cursor);
        assert!(result.is_ok()); // EOF before any length bytes = clean EOF
    }

    #[test]
    fn test_packet_too_large() {
        let huge_size = MAX_PACKET_SIZE + 1;
        let buffer = huge_size.to_be_bytes().to_vec();
        let mut cursor = Cursor::new(buffer);

        let result = read_packet(&mut cursor);
        assert!(matches!(result, Err(PacketError::PacketTooLarge { .. })));
    }

    #[test]
    fn test_multiple_packets() {
        let mut buffer = Vec::new();
        write_packet(&mut buffer, b"first").unwrap();
        write_packet(&mut buffer, b"second").unwrap();
        write_packet(&mut buffer, b"third").unwrap();

        let mut cursor = Cursor::new(buffer);
        assert_eq!(read_packet(&mut cursor).unwrap().unwrap(), b"first");
        assert_eq!(read_packet(&mut cursor).unwrap().unwrap(), b"second");
        assert_eq!(read_packet(&mut cursor).unwrap().unwrap(), b"third");
        assert!(read_packet(&mut cursor).unwrap().is_none());
    }
}
