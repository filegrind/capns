//! Message Envelope Types for Plugin Communication
//!
//! Messages are JSON envelopes that travel inside binary packets.
//! They provide routing (cap URN), correlation (request ID), and typing.
//!
//! Message flow:
//! ```text
//! Host → Plugin:  CapRequest  (invoke a cap)
//! Plugin → Host:  CapResponse (single response) or StreamChunk (streaming)
//! Either → Either: Error (error condition)
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// Message types for the envelope
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    /// Request to invoke a cap (host → plugin)
    CapRequest,
    /// Single complete response (plugin → host)
    CapResponse,
    /// Streaming chunk (plugin → host)
    StreamChunk,
    /// Stream complete marker (plugin → host)
    StreamEnd,
    /// Error message (either direction)
    Error,
}

/// The message envelope that wraps all plugin communication.
///
/// This is serialized as JSON inside binary packets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message ID for correlation
    pub id: String,

    /// Message type
    #[serde(rename = "type")]
    pub message_type: MessageType,

    /// Cap URN being invoked (for requests) or responded to (for responses)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap: Option<String>,

    /// The actual payload data (request args, response data, etc.)
    /// Interpretation depends on message_type and cap's media specs
    pub payload: JsonValue,
}

impl Message {
    /// Create a new cap request message.
    pub fn cap_request(cap_urn: &str, payload: JsonValue) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            message_type: MessageType::CapRequest,
            cap: Some(cap_urn.to_string()),
            payload,
        }
    }

    /// Create a new cap request with a specific ID.
    pub fn cap_request_with_id(id: &str, cap_urn: &str, payload: JsonValue) -> Self {
        Self {
            id: id.to_string(),
            message_type: MessageType::CapRequest,
            cap: Some(cap_urn.to_string()),
            payload,
        }
    }

    /// Create a response message.
    pub fn cap_response(request_id: &str, payload: JsonValue) -> Self {
        Self {
            id: request_id.to_string(),
            message_type: MessageType::CapResponse,
            cap: None,
            payload,
        }
    }

    /// Create a streaming chunk message.
    pub fn stream_chunk(request_id: &str, payload: JsonValue) -> Self {
        Self {
            id: request_id.to_string(),
            message_type: MessageType::StreamChunk,
            cap: None,
            payload,
        }
    }

    /// Create a stream end marker.
    pub fn stream_end(request_id: &str, payload: JsonValue) -> Self {
        Self {
            id: request_id.to_string(),
            message_type: MessageType::StreamEnd,
            cap: None,
            payload,
        }
    }

    /// Create an error message.
    pub fn error(request_id: &str, code: &str, message: &str) -> Self {
        Self {
            id: request_id.to_string(),
            message_type: MessageType::Error,
            cap: None,
            payload: serde_json::json!({
                "code": code,
                "message": message,
            }),
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Message serialization should never fail")
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MessageError> {
        serde_json::from_slice(bytes).map_err(MessageError::Json)
    }

    /// Check if this is a request message.
    pub fn is_request(&self) -> bool {
        self.message_type == MessageType::CapRequest
    }

    /// Check if this is a response message (complete or streaming).
    pub fn is_response(&self) -> bool {
        matches!(
            self.message_type,
            MessageType::CapResponse | MessageType::StreamChunk | MessageType::StreamEnd
        )
    }

    /// Check if this is an error message.
    pub fn is_error(&self) -> bool {
        self.message_type == MessageType::Error
    }

    /// Check if this is a streaming message.
    pub fn is_streaming(&self) -> bool {
        matches!(
            self.message_type,
            MessageType::StreamChunk | MessageType::StreamEnd
        )
    }

    /// Check if this is a stream end marker.
    pub fn is_stream_end(&self) -> bool {
        self.message_type == MessageType::StreamEnd
    }
}

/// Errors that can occur during message operations
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid message type: {0}")]
    InvalidType(String),
}

/// Helper struct for error payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
}

impl ErrorPayload {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: JsonValue) -> Self {
        self.details = Some(details);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_request_serialization() {
        let msg = Message::cap_request(
            "cap:op=llm_inference;llm",
            serde_json::json!({"prompt": "hello"}),
        );

        let bytes = msg.to_bytes();
        let parsed = Message::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.message_type, MessageType::CapRequest);
        assert_eq!(parsed.cap, Some("cap:op=llm_inference;llm".to_string()));
        assert_eq!(parsed.payload["prompt"], "hello");
    }

    #[test]
    fn test_stream_chunk_serialization() {
        let msg = Message::stream_chunk("req-123", serde_json::json!({"token": "hello"}));

        let bytes = msg.to_bytes();
        let parsed = Message::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.message_type, MessageType::StreamChunk);
        assert_eq!(parsed.id, "req-123");
        assert!(parsed.is_streaming());
        assert!(!parsed.is_stream_end());
    }

    #[test]
    fn test_error_message() {
        let msg = Message::error("req-123", "NOT_FOUND", "Cap not found");

        assert!(msg.is_error());
        assert_eq!(msg.payload["code"], "NOT_FOUND");
        assert_eq!(msg.payload["message"], "Cap not found");
    }

    #[test]
    fn test_message_type_checks() {
        let request = Message::cap_request("cap:test", serde_json::json!({}));
        assert!(request.is_request());
        assert!(!request.is_response());

        let response = Message::cap_response("id", serde_json::json!({}));
        assert!(response.is_response());
        assert!(!response.is_request());

        let chunk = Message::stream_chunk("id", serde_json::json!({}));
        assert!(chunk.is_streaming());
        assert!(!chunk.is_stream_end());

        let end = Message::stream_end("id", serde_json::json!({}));
        assert!(end.is_streaming());
        assert!(end.is_stream_end());
    }
}
