//! Pure cap-based execution with strict input validation

use anyhow::Result;
use serde_json::Value as JsonValue;
use crate::{CapUrn, ResponseWrapper, Cap, MediaSpec};

/// Cap caller that executes via XPC service with strict validation
pub struct CapCaller {
    cap: String,
    cap_host: Box<dyn CapHost>,
    cap_definition: Cap,
}

/// Trait for Cap Host communication
pub trait CapHost: Send + Sync + std::fmt::Debug {
    fn execute_cap(
        &self,
        cap_urn: &str,
        positional_args: &[String],
        named_args: &[(String, String)],
        stdin_data: Option<Vec<u8>>
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>;
}

impl CapCaller {
    /// Create a new cap caller with validation
    pub fn new(
        cap: String,
        cap_host: Box<dyn CapHost>,
        cap_definition: Cap,
    ) -> Self {
        Self {
            cap,
            cap_host,
            cap_definition,
        }
    }
    
    /// Call the cap with structured arguments and optional stdin data
    /// Validates inputs against cap definition before execution
    pub async fn call(
        &self,
        positional_args: Vec<JsonValue>,
        named_args: Vec<JsonValue>,
        stdin_data: Option<Vec<u8>>
    ) -> Result<ResponseWrapper> {
        // Validate inputs against cap definition
        self.validate_inputs(&positional_args, &named_args)?;
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
        
        // Validate output against cap definition
        self.validate_output(&response)?;
        
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
    
    /// Check if this cap produces binary output based on 'out' tag
    fn is_binary_cap(&self) -> bool {
        let cap_urn = CapUrn::from_string(&self.cap)
            .expect("Invalid cap URN");

        match cap_urn.get_tag("out") {
            Some(spec) => {
                MediaSpec::parse(spec)
                    .map(|ms| ms.is_binary())
                    .unwrap_or(false)
            }
            None => false
        }
    }

    /// Check if this cap should produce JSON output based on 'out' tag
    fn is_json_cap(&self) -> bool {
        let cap_urn = CapUrn::from_string(&self.cap)
            .expect("Invalid cap URN");

        match cap_urn.get_tag("out") {
            Some(spec) => {
                MediaSpec::parse(spec)
                    .map(|ms| ms.is_json())
                    .unwrap_or(false)
            }
            // Default to text/plain (not JSON) if no 'out' tag is specified
            None => false
        }
    }
    
    /// Validate input arguments against cap definition
    fn validate_inputs(
        &self,
        positional_args: &[JsonValue],
        named_args: &[JsonValue],
    ) -> Result<()> {
        // Determine if this capability expects positional or named arguments
        let expects_positional = self.cap_definition.arguments.required.iter()
            .any(|arg| arg.position.is_some()) || 
            !positional_args.is_empty();
        
        if expects_positional {
            // Validate as positional arguments
            crate::validation::InputValidator::validate_positional_arguments(&self.cap_definition, positional_args)
                .map_err(|e| anyhow::anyhow!("Input validation failed for {}: {}", self.cap, e))?;
        } else {
            // Validate as named arguments
            crate::validation::InputValidator::validate_named_arguments(&self.cap_definition, named_args)
                .map_err(|e| anyhow::anyhow!("Input validation failed for {}: {}", self.cap, e))?;
        }
        
        Ok(())
    }
    
    /// Validate output against cap definition
    fn validate_output(&self, response: &ResponseWrapper) -> Result<()> {
        // For binary outputs, check type compatibility
        if let Ok(text) = response.as_string() {
            // For JSON outputs, parse as JSON and validate
            if self.is_json_cap() {
                let output_value: JsonValue = serde_json::from_str(&text)
                    .map_err(|e| anyhow::anyhow!("Output is not valid JSON for cap {}: {}", self.cap, e))?;
                
                crate::validation::OutputValidator::validate_output(&self.cap_definition, &output_value)
                    .map_err(|e| anyhow::anyhow!("Output validation failed for {}: {}", self.cap, e))?;
            } else {
                // For text outputs, wrap in JSON string and validate
                let output_value = JsonValue::String(text);
                crate::validation::OutputValidator::validate_output(&self.cap_definition, &output_value)
                    .map_err(|e| anyhow::anyhow!("Output validation failed for {}: {}", self.cap, e))?;
            }
        } else {
            // For binary outputs, validate that the cap expects binary output
            if let Some(output_def) = self.cap_definition.get_output() {
                if output_def.output_type != crate::OutputType::Binary {
                    return Err(anyhow::anyhow!(
                        "Cap {} expects {:?} output but received binary data", 
                        self.cap, 
                        output_def.output_type
                    ));
                }
            }
        }
        
        Ok(())
    }
}