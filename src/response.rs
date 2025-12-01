//! Response wrapper for unified plugin output handling with validation

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use crate::{Cap, validation::{ValidationError, OutputValidator}};

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
    
    /// Validate response against cap output definition
    pub fn validate_against_cap(&self, cap: &Cap) -> Result<(), ValidationError> {
        // Convert response to JSON value for validation
        let json_value = match self.content_type {
            ResponseContentType::Json => {
                let text = self.as_string().map_err(|e| {
                    ValidationError::JsonParseError {
                        cap_urn: cap.urn_string(),
                        error: format!("Failed to convert response to string: {}", e),
                    }
                })?;
                serde_json::from_str(&text).map_err(|e| {
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
                    if output_def.output_type != crate::OutputType::Binary {
                        return Err(ValidationError::InvalidOutputType {
                            cap_urn: cap.urn_string(),
                            expected_type: output_def.output_type.clone(),
                            actual_type: "binary".to_string(),
                            actual_value: JsonValue::String(format!("{} bytes of binary data", self.raw_bytes.len())),
                        });
                    }
                }
                return Ok(());
            }
        };
        
        OutputValidator::validate_output(cap, &json_value)
    }
    
    /// Get content type for validation purposes
    pub fn get_content_type(&self) -> &str {
        match self.content_type {
            ResponseContentType::Json => "application/json",
            ResponseContentType::Text => "text/plain",
            ResponseContentType::Binary => "application/octet-stream",
        }
    }
    
    /// Check if response matches expected output type
    pub fn matches_output_type(&self, cap: &Cap) -> bool {
        if let Some(output_def) = cap.get_output() {
            match (&self.content_type, &output_def.output_type) {
                (ResponseContentType::Json, crate::OutputType::Object) => true,
                (ResponseContentType::Json, crate::OutputType::Array) => true,
                (ResponseContentType::Text, crate::OutputType::String) => true,
                (ResponseContentType::Binary, crate::OutputType::Binary) => true,
                // Also allow JSON for primitives
                (ResponseContentType::Json, crate::OutputType::String) => true,
                (ResponseContentType::Json, crate::OutputType::Integer) => true,
                (ResponseContentType::Json, crate::OutputType::Number) => true,
                (ResponseContentType::Json, crate::OutputType::Boolean) => true,
                _ => false,
            }
        } else {
            false
        }
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
    
    #[test]
    fn test_binary_response() {
        let binary_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG header
        let response = ResponseWrapper::from_binary(binary_data.clone());
        
        assert_eq!(response.as_bytes(), &binary_data);
        assert_eq!(response.size(), 4);
    }
}