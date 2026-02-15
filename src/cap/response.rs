//! Response wrapper for unified plugin output handling with validation

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use crate::{Cap, ValidationError};

/// Unified response wrapper for all plugin operations
/// Provides type-safe deserialization of plugin output
#[derive(Debug, Clone)]
pub struct ResponseWrapper {
    raw_bytes: Vec<u8>,
    content_type: ResponseContentType,
}

#[derive(Debug, Clone, PartialEq)]
enum ResponseContentType {
    Json,
    Text,
    Binary,
}

impl ResponseWrapper {
    /// Create from JSON output
    pub fn from_json(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Json,
        }
    }

    /// Create from text output
    pub fn from_text(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Text,
        }
    }

    /// Create from binary output (like PNG images)
    pub fn from_binary(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Binary,
        }
    }

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw_bytes
    }

    /// Convert to string
    pub fn as_string(&self) -> Result<String> {
        String::from_utf8(self.raw_bytes.clone())
            .map_err(|e| anyhow!("Failed to convert response to string: {}", e))
    }

    /// Convert to integer
    pub fn as_int(&self) -> Result<i64> {
        let text = self.as_string()?;
        let trimmed = text.trim();

        // Try parsing as JSON number first
        if let Ok(json_val) = serde_json::from_str::<JsonValue>(trimmed) {
            if let Some(num) = json_val.as_i64() {
                return Ok(num);
            }
        }

        // Fall back to direct parsing
        trimmed.parse::<i64>()
            .map_err(|e| anyhow!("Failed to parse '{}' as integer: {}", trimmed, e))
    }

    /// Convert to float
    pub fn as_float(&self) -> Result<f64> {
        let text = self.as_string()?;
        let trimmed = text.trim();

        // Try parsing as JSON number first
        if let Ok(json_val) = serde_json::from_str::<JsonValue>(trimmed) {
            if let Some(num) = json_val.as_f64() {
                return Ok(num);
            }
        }

        // Fall back to direct parsing
        trimmed.parse::<f64>()
            .map_err(|e| anyhow!("Failed to parse '{}' as float: {}", trimmed, e))
    }

    /// Convert to boolean
    pub fn as_bool(&self) -> Result<bool> {
        let text = self.as_string()?;
        let trimmed = text.trim().to_lowercase();

        match trimmed.as_str() {
            "true" | "1" | "yes" | "y" => Ok(true),
            "false" | "0" | "no" | "n" => Ok(false),
            _ => {
                // Try parsing as JSON boolean
                if let Ok(json_val) = serde_json::from_str::<JsonValue>(&trimmed) {
                    if let Some(bool_val) = json_val.as_bool() {
                        return Ok(bool_val);
                    }
                }
                Err(anyhow!("Failed to parse '{}' as boolean", trimmed))
            }
        }
    }

    /// Deserialize to any type implementing serde::Deserialize
    pub fn as_type<T: DeserializeOwned>(&self) -> Result<T> {
        match self.content_type {
            ResponseContentType::Json => {
                let text = self.as_string()?;
                serde_json::from_str(&text)
                    .map_err(|e| anyhow!("Failed to deserialize JSON response: {}\\nResponse: {}", e, text))
            }
            ResponseContentType::Text => {
                // For text responses, try to deserialize the string directly
                let text = self.as_string()?;
                serde_json::from_str(&format!("\"{}\"", text.replace("\"", "\\\"")))
                    .map_err(|e| anyhow!("Failed to deserialize text response as JSON string: {}\\nResponse: {}", e, text))
            }
            ResponseContentType::Binary => {
                Err(anyhow!("Cannot deserialize binary response to structured type"))
            }
        }
    }

    /// Check if response is empty
    pub fn is_empty(&self) -> bool {
        self.raw_bytes.is_empty()
    }

    /// Get response size in bytes
    pub fn size(&self) -> usize {
        self.raw_bytes.len()
    }

    /// Validate response against cap output definition (basic validation)
    pub async fn validate_against_cap(
        &self,
        cap: &Cap,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<(), ValidationError> {
        let media_specs = cap.get_media_specs();

        // Convert response to JSON value for validation
        let _json_value = match self.content_type {
            ResponseContentType::Json => {
                let text = self.as_string().map_err(|e| {
                    ValidationError::JsonParseError {
                        cap_urn: cap.urn_string(),
                        error: format!("Failed to convert response to string: {}", e),
                    }
                })?;
                serde_json::from_str::<JsonValue>(&text).map_err(|e| {
                    ValidationError::JsonParseError {
                        cap_urn: cap.urn_string(),
                        error: format!("Failed to parse JSON: {}", e),
                    }
                })?
            },
            ResponseContentType::Text => {
                let text = self.as_string().map_err(|e| {
                    ValidationError::JsonParseError {
                        cap_urn: cap.urn_string(),
                        error: format!("Failed to convert response to string: {}", e),
                    }
                })?;
                JsonValue::String(text)
            },
            ResponseContentType::Binary => {
                // Binary outputs can't be validated as JSON, validate the response type instead
                if let Some(output_def) = cap.get_output() {
                    let is_binary = output_def.is_binary(Some(media_specs), registry)
                        .await
                        .map_err(|e| ValidationError::InvalidMediaSpec {
                            cap_urn: cap.urn_string(),
                            field_name: "output".to_string(),
                            error: e.to_string(),
                        })?;
                    if !is_binary {
                        return Err(ValidationError::InvalidOutputType {
                            cap_urn: cap.urn_string(),
                            expected_media_spec: output_def.media_urn.clone(),
                            actual_value: JsonValue::String(format!("{} bytes of binary data", self.raw_bytes.len())),
                            schema_errors: vec!["Expected non-binary output but received binary data".to_string()],
                        });
                    }
                }
                return Ok(());
            }
        };

        Ok(())
    }

    /// Get content type for validation purposes
    pub fn get_content_type(&self) -> &str {
        match self.content_type {
            ResponseContentType::Json => "application/json",
            ResponseContentType::Text => "text/plain",
            ResponseContentType::Binary => "application/octet-stream",
        }
    }

    /// Check if response matches expected output type based on media_spec
    ///
    /// # Errors
    /// Returns error if the output spec ID cannot be resolved
    pub async fn matches_output_type(
        &self,
        cap: &Cap,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, crate::media::spec::MediaSpecError> {
        let output_def = cap.get_output()
            .ok_or_else(|| crate::media::spec::MediaSpecError::UnresolvableMediaUrn(
                "cap has no output definition".to_string()
            ))?;

        let media_specs = cap.get_media_specs();
        let is_output_binary = output_def.is_binary(Some(media_specs), registry).await?;
        let is_output_structured = output_def.is_structured(Some(media_specs), registry).await?;

        Ok(match &self.content_type {
            // JSON response matches structured outputs (map/list)
            ResponseContentType::Json => is_output_structured,
            // Text response matches non-binary, non-structured outputs (scalars)
            ResponseContentType::Text => !is_output_binary && !is_output_structured,
            // Binary response matches binary outputs
            ResponseContentType::Binary => is_output_binary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        value: i32,
    }

    // TEST168: Test ResponseWrapper from JSON deserializes to correct structured type
    #[test]
    fn test_json_response() {
        let test_data = TestStruct {
            name: "test".to_string(),
            value: 42,
        };
        let json_str = serde_json::to_string(&test_data).unwrap();
        let response = ResponseWrapper::from_json(json_str.into_bytes());

        let parsed: TestStruct = response.as_type().unwrap();
        assert_eq!(parsed, test_data);
    }

    // TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string
    #[test]
    fn test_primitive_types() {
        // Test integer
        let response = ResponseWrapper::from_text(b"42".to_vec());
        assert_eq!(response.as_int().unwrap(), 42);

        // Test float
        let response = ResponseWrapper::from_text(b"3.14".to_vec());
        assert_eq!(response.as_float().unwrap(), 3.14);

        // Test boolean
        let response = ResponseWrapper::from_text(b"true".to_vec());
        assert_eq!(response.as_bool().unwrap(), true);

        // Test string
        let response = ResponseWrapper::from_text(b"hello world".to_vec());
        assert_eq!(response.as_string().unwrap(), "hello world");
    }

    // TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly
    #[test]
    fn test_binary_response() {
        let binary_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG header
        let response = ResponseWrapper::from_binary(binary_data.clone());

        assert_eq!(response.as_bytes(), &binary_data);
        assert_eq!(response.size(), 4);
    }
}
