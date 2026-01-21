//! Pure cap-based execution with strict input validation

use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use crate::{CapUrn, ResponseWrapper, Cap};
use crate::media_spec::{resolve_media_urn, ResolvedMediaSpec};

/// Source for stdin data - either raw bytes or a file reference.
///
/// For plugins (via gRPC/XPC), using FileReference avoids the 4MB gRPC limit
/// by letting the Swift/XPC side read the file locally instead of sending
/// bytes over the wire.
#[derive(Debug, Clone)]
pub enum StdinSource {
    /// Raw byte data - used for providers (in-process) or small inline data
    Data(Vec<u8>),
    /// File reference - used for plugins to read files locally on Mac side
    FileReference {
        tracked_file_id: String,
        original_path: String,
        security_bookmark: Vec<u8>,
        media_urn: String,
    },
}

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
        stdin_source: Option<StdinSource>
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

    /// Get the cap definition
    pub fn cap_definition(&self) -> &Cap {
        &self.cap_definition
    }

    /// Get a map of argument media_urn to position for positional arguments
    /// Returns only arguments that have a position source set
    pub fn get_positional_arg_positions(&self) -> std::collections::HashMap<String, usize> {
        use crate::ArgSource;
        let mut positions = std::collections::HashMap::new();
        for arg in self.cap_definition.get_args() {
            for source in &arg.sources {
                if let ArgSource::Position { position } = source {
                    positions.insert(arg.media_urn.clone(), *position);
                    break;
                }
            }
        }
        positions
    }

    /// Call the cap with structured arguments and optional stdin source
    /// Validates inputs against cap definition before execution
    pub async fn call(
        &self,
        positional_args: Vec<JsonValue>,
        named_args: Vec<JsonValue>,
        stdin_source: Option<StdinSource>
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
            stdin_source
        ).await?;

        // Resolve output spec to determine response type
        let output_spec = self.resolve_output_spec()?;

        // Determine response type based on what was returned and resolved output spec
        let response = if let Some(binary_data) = binary_output {
            if !output_spec.is_binary() {
                return Err(anyhow!("Cap {} returned binary data but output spec '{}' is not binary",
                    self.cap, output_spec.media_urn));
            }
            ResponseWrapper::from_binary(binary_data)
        } else if let Some(text_data) = text_output {
            if output_spec.is_binary() {
                return Err(anyhow!("Cap {} returned text data but output spec '{}' expects binary",
                    self.cap, output_spec.media_urn));
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

    /// Resolve the output spec ID from the cap URN's out_spec.
    ///
    /// This method fails hard if:
    /// - The cap URN is invalid
    /// - The spec ID cannot be resolved (not in media_specs and not a built-in)
    fn resolve_output_spec(&self) -> Result<ResolvedMediaSpec> {
        let cap_urn = CapUrn::from_string(&self.cap)
            .map_err(|e| anyhow!("Invalid cap URN '{}': {}", self.cap, e))?;

        // Direction specs are now required first-class fields
        let spec_id = cap_urn.out_spec();

        resolve_media_urn(spec_id, self.cap_definition.get_media_specs())
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
        use crate::ArgSource;
        let args = self.cap_definition.get_args();

        // Get positional args sorted by position
        let mut positional_arg_defs: Vec<_> = args.iter()
            .filter_map(|arg| {
                arg.sources.iter()
                    .find_map(|s| if let ArgSource::Position { position } = s {
                        Some((arg, *position))
                    } else {
                        None
                    })
            })
            .collect();
        positional_arg_defs.sort_by_key(|(_, pos)| *pos);

        // Check if positional arguments are being used
        let using_positional = !positional_args.is_empty() || !positional_arg_defs.is_empty();

        if using_positional {
            // Validate positional arguments
            let max_args = positional_arg_defs.len();
            if positional_args.len() > max_args {
                return Err(anyhow::anyhow!(
                    "Too many arguments: expected at most {}, got {}",
                    max_args, positional_args.len()
                ));
            }

            // Check required arguments are present
            let required_count = positional_arg_defs.iter()
                .filter(|(arg, _)| arg.required)
                .count();
            if positional_args.len() < required_count {
                if let Some((missing_arg, _)) = positional_arg_defs.get(positional_args.len()) {
                    return Err(anyhow::anyhow!(
                        "Missing required argument: {}",
                        missing_arg.media_urn
                    ));
                }
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
            for arg in args {
                if arg.required {
                    // Find cli_flag for this arg
                    let cli_flag = arg.sources.iter()
                        .find_map(|s| if let ArgSource::CliFlag { cli_flag } = s {
                            Some(cli_flag.as_str())
                        } else {
                            None
                        });

                    if let Some(flag) = cli_flag {
                        if !provided_names.contains(flag) {
                            return Err(anyhow::anyhow!(
                                "Missing required argument: {}",
                                arg.media_urn
                            ));
                        }
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stdin_source_data_creation() {
        let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let source = StdinSource::Data(data.clone());

        match source {
            StdinSource::Data(d) => assert_eq!(d, data),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    #[test]
    fn test_stdin_source_file_reference_creation() {
        let tracked_file_id = "tracked-file-123".to_string();
        let original_path = "/path/to/original.pdf".to_string();
        let security_bookmark = vec![0x62, 0x6f, 0x6f, 0x6b]; // "book"
        let media_urn = "media:type=pdf;v=1;binary".to_string();

        let source = StdinSource::FileReference {
            tracked_file_id: tracked_file_id.clone(),
            original_path: original_path.clone(),
            security_bookmark: security_bookmark.clone(),
            media_urn: media_urn.clone(),
        };

        match source {
            StdinSource::FileReference {
                tracked_file_id: tid,
                original_path: op,
                security_bookmark: sb,
                media_urn: mu,
            } => {
                assert_eq!(tid, tracked_file_id);
                assert_eq!(op, original_path);
                assert_eq!(sb, security_bookmark);
                assert_eq!(mu, media_urn);
            }
            StdinSource::Data(_) => panic!("Expected FileReference variant"),
        }
    }

    #[test]
    fn test_stdin_source_empty_data() {
        let source = StdinSource::Data(vec![]);

        match source {
            StdinSource::Data(d) => assert!(d.is_empty()),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    #[test]
    fn test_stdin_source_binary_content() {
        // PNG header bytes
        let png_header = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let source = StdinSource::Data(png_header.clone());

        match source {
            StdinSource::Data(d) => {
                assert_eq!(d.len(), 8);
                assert_eq!(d[0], 0x89);
                assert_eq!(d[1], 0x50); // 'P'
                assert_eq!(d, png_header);
            }
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    #[test]
    fn test_stdin_source_clone() {
        let data = vec![1, 2, 3, 4, 5];
        let source = StdinSource::Data(data.clone());
        let cloned = source.clone();

        match (source, cloned) {
            (StdinSource::Data(d1), StdinSource::Data(d2)) => assert_eq!(d1, d2),
            _ => panic!("Expected both to be Data variants"),
        }
    }

    #[test]
    fn test_stdin_source_file_reference_clone() {
        let source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![1, 2, 3],
            media_urn: "media:type=pdf;v=1".to_string(),
        };
        let cloned = source.clone();

        match (source, cloned) {
            (
                StdinSource::FileReference {
                    tracked_file_id: tid1,
                    original_path: op1,
                    security_bookmark: sb1,
                    media_urn: mu1,
                },
                StdinSource::FileReference {
                    tracked_file_id: tid2,
                    original_path: op2,
                    security_bookmark: sb2,
                    media_urn: mu2,
                },
            ) => {
                assert_eq!(tid1, tid2);
                assert_eq!(op1, op2);
                assert_eq!(sb1, sb2);
                assert_eq!(mu1, mu2);
            }
            _ => panic!("Expected both to be FileReference variants"),
        }
    }

    #[test]
    fn test_stdin_source_debug() {
        let data_source = StdinSource::Data(vec![1, 2, 3]);
        let debug_str = format!("{:?}", data_source);
        assert!(debug_str.contains("Data"));

        let file_source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![],
            media_urn: "media:type=pdf;v=1".to_string(),
        };
        let debug_str = format!("{:?}", file_source);
        assert!(debug_str.contains("FileReference"));
        assert!(debug_str.contains("test-id"));
        assert!(debug_str.contains("/test/path.pdf"));
    }
}
