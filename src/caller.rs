//! Pure cap-based execution with strict input validation

use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use crate::{CapUrn, ResponseWrapper, Cap};
use crate::media_spec::{resolve_spec_id, ResolvedMediaSpec};

/// Cap caller that executes via XPC service with strict validation
pub struct CapCaller {
    cap: String,
    cap_set: Box<dyn CapSet>,
    cap_definition: Cap,
}

/// Trait for Cap Host communication
pub trait CapSet: Send + Sync + std::fmt::Debug {
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
        cap_set: Box<dyn CapSet>,
        cap_definition: Cap,
    ) -> Self {
        Self {
            cap,
            cap_set,
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
        // Note: Full async validation with ProfileSchemaRegistry would be done at a higher level
        // Here we do basic structural validation synchronously
        self.validate_inputs_basic(&positional_args, &named_args)?;

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
        let (binary_output, text_output) = self.cap_set.execute_cap(
            &self.cap,
            &string_positional_args,
            &string_named_args,
            stdin_data
        ).await?;

        // Resolve output spec to determine response type
        let output_spec = self.resolve_output_spec()?;

        // Determine response type based on what was returned and resolved output spec
        let response = if let Some(binary_data) = binary_output {
            if !output_spec.is_binary() {
                return Err(anyhow!("Cap {} returned binary data but output spec '{}' is not binary",
                    self.cap, output_spec.spec_id));
            }
            ResponseWrapper::from_binary(binary_data)
        } else if let Some(text_data) = text_output {
            if output_spec.is_binary() {
                return Err(anyhow!("Cap {} returned text data but output spec '{}' expects binary",
                    self.cap, output_spec.spec_id));
            }
            if output_spec.is_json() {
                ResponseWrapper::from_json(text_data.into_bytes())
            } else {
                ResponseWrapper::from_text(text_data.into_bytes())
            }
        } else {
            return Err(anyhow!("Cap returned no output"));
        };

        // Validate output against cap definition (basic type check)
        self.validate_output_basic(&response)?;

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

    /// Resolve the output spec ID from the cap URN's 'out' tag.
    ///
    /// This method fails hard if:
    /// - The cap URN is invalid
    /// - The 'out' tag is missing (caps must declare their output type)
    /// - The spec ID cannot be resolved (not in media_specs and not a built-in)
    fn resolve_output_spec(&self) -> Result<ResolvedMediaSpec> {
        let cap_urn = CapUrn::from_string(&self.cap)
            .map_err(|e| anyhow!("Invalid cap URN '{}': {}", self.cap, e))?;

        let spec_id = cap_urn.get_tag("out")
            .ok_or_else(|| anyhow!(
                "Cap URN '{}' is missing required 'out' tag - caps must declare their output type",
                self.cap
            ))?;

        resolve_spec_id(spec_id, self.cap_definition.get_media_specs())
            .map_err(|e| anyhow!(
                "Failed to resolve output spec ID '{}' for cap '{}': {} - check that media_specs contains this spec ID or it is a built-in",
                spec_id, self.cap, e
            ))
    }

    /// Basic input validation (argument count, required args present)
    /// Full async validation with ProfileSchemaRegistry should be done at a higher level
    fn validate_inputs_basic(
        &self,
        positional_args: &[JsonValue],
        named_args: &[JsonValue],
    ) -> Result<()> {
        let args = &self.cap_definition.arguments;

        // Check if positional arguments are being used
        let using_positional = !positional_args.is_empty() ||
            args.required.iter().any(|arg| arg.position.is_some());

        if using_positional {
            // Validate positional arguments
            let max_args = args.required.len() + args.optional.len();
            if positional_args.len() > max_args {
                return Err(anyhow::anyhow!(
                    "Too many arguments: expected at most {}, got {}",
                    max_args, positional_args.len()
                ));
            }

            // Check required arguments are present
            if positional_args.len() < args.required.len() {
                let missing = &args.required[positional_args.len()];
                return Err(anyhow::anyhow!(
                    "Missing required argument: {}",
                    missing.name
                ));
            }
        } else {
            // Validate named arguments
            let mut provided_names = std::collections::HashSet::new();
            for arg in named_args {
                if let JsonValue::Object(map) = arg {
                    if let Some(JsonValue::String(name)) = map.get("name") {
                        provided_names.insert(name.clone());
                    }
                }
            }

            // Check all required arguments are provided
            for req_arg in &args.required {
                if !provided_names.contains(&req_arg.name) {
                    return Err(anyhow::anyhow!(
                        "Missing required argument: {}",
                        req_arg.name
                    ));
                }
            }
        }

        Ok(())
    }

    /// Basic output validation
    /// Full async validation with ProfileSchemaRegistry should be done at a higher level
    fn validate_output_basic(&self, response: &ResponseWrapper) -> Result<()> {
        let output_spec = self.resolve_output_spec()?;

        // For text/JSON outputs, check it's parseable if JSON expected
        if let Ok(text) = response.as_string() {
            if output_spec.is_json() {
                // Verify it's valid JSON
                let _: JsonValue = serde_json::from_str(&text)
                    .map_err(|e| anyhow!("Output is not valid JSON for cap {}: {}", self.cap, e))?;
            }
        }
        // Binary validation already done in call() before creating the response

        Ok(())
    }
}
