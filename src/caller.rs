//! Pure cap-based execution

use anyhow::Result;
use serde_json::Value as JsonValue;
use crate::{CapUrn, ResponseWrapper};

/// Cap caller that executes via XPC service
pub struct CapCaller {
    cap: String,
    cap_host: Box<dyn CapHost>,
}

/// Trait for Cap Host communication
pub trait CapHost: Send + Sync {
    fn execute_cap(
        &self,
        cap_urn: &str,
        positional_args: &[String],
        named_args: &[(String, String)],
        stdin_data: Option<Vec<u8>>
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>;
}

impl CapCaller {
    /// Create a new cap caller
    pub fn new(
        cap: String,
        cap_host: Box<dyn CapHost>,
    ) -> Self {
        Self {
            cap,
            cap_host,
        }
    }
    
    /// Call the cap with structured arguments and optional stdin data
    pub async fn call(
        &self,
        positional_args: Vec<JsonValue>,
        named_args: Vec<JsonValue>,
        stdin_data: Option<Vec<u8>>
    ) -> Result<ResponseWrapper> {
        // Convert JsonValue positional args to strings
        let string_positional_args: Vec<String> = positional_args
            .into_iter()
            .map(|arg| match arg {
                JsonValue::String(s) => s,
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                JsonValue::Array(_) | JsonValue::Object(_) => {
                    serde_json::to_string(&arg).unwrap_or_default()
                }
                JsonValue::Null => String::new(),
            })
            .collect();

        // Convert JsonValue named args to (String, String) tuples
        let string_named_args: Vec<(String, String)> = named_args
            .into_iter()
            .filter_map(|arg| {
                if let JsonValue::Object(map) = arg {
                    if let (Some(JsonValue::String(name)), Some(value)) = 
                        (map.get("name"), map.get("value")) {
                        let value_str = match value {
                            JsonValue::String(s) => s.clone(),
                            JsonValue::Number(n) => n.to_string(),
                            JsonValue::Bool(b) => b.to_string(),
                            _ => serde_json::to_string(value).unwrap_or_default(),
                        };
                        return Some((name.clone(), value_str));
                    }
                }
                None
            })
            .collect();

        // Execute via cap host method with stdin support
        let (binary_output, text_output) = self.cap_host.execute_cap(
            &self.cap, 
            &string_positional_args,
            &string_named_args,
            stdin_data
        ).await?;
        
        // Determine response type based on what was returned
        let response = if let Some(binary_data) = binary_output {
            ResponseWrapper::from_binary(binary_data)
        } else if let Some(text_data) = text_output {
            if self.is_json_cap() {
                ResponseWrapper::from_json(text_data.into_bytes())
            } else {
                ResponseWrapper::from_text(text_data.into_bytes())
            }
        } else {
            return Err(anyhow::anyhow!("Cap returned no output"));
        };
        
        Ok(response)
    }
    
    /// Convert cap name to command
    fn cap_to_command(&self, cap: &str) -> String {
        // Extract operation part (everything before the last colon)
        let operation = if let Some(colon_pos) = cap.rfind(':') {
            &cap[..colon_pos]
        } else {
            cap
        };
        
        // Convert underscores to hyphens for command name
        operation.replace('_', "-")
    }
    
    /// Check if this cap produces binary output
    fn is_binary_cap(&self) -> bool {
        // Use the formal cap URN system to detect binary caps
        let cap_urn = CapUrn::from_string(&self.cap)
            .expect("Invalid cap URN");
        cap_urn.get_tag("output") == Some(&"binary".to_string())
    }
    
    /// Check if this cap should produce JSON output
    fn is_json_cap(&self) -> bool {
        let cap_urn = CapUrn::from_string(&self.cap)
            .expect("Invalid cap URN");
        cap_urn.get_tag("output") != Some(&"binary".to_string())
    }
}