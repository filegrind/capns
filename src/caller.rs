//! Pure cap-based execution with strict input validation

use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use crate::{CapUrn, ResponseWrapper, Cap};
use crate::media_spec::{resolve_media_urn, ResolvedMediaSpec};
use crate::media_registry::MediaUrnRegistry;

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

/// Unified argument type - arguments are identified by media_urn.
/// The cap definition's sources specify how to extract values (stdin, position, cli_flag).
#[derive(Debug, Clone)]
pub struct CapArgumentValue {
    /// Semantic identifier, e.g., "media:model-spec;textable;form=scalar"
    pub media_urn: String,
    /// Value bytes (UTF-8 for text, raw for binary)
    pub value: Vec<u8>,
}

impl CapArgumentValue {
    /// Create a new CapArgumentValue
    pub fn new(media_urn: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            media_urn: media_urn.into(),
            value,
        }
    }

    /// Create a new CapArgumentValue from a string value
    pub fn from_str(media_urn: impl Into<String>, value: &str) -> Self {
        Self {
            media_urn: media_urn.into(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Get the value as a UTF-8 string (may fail for binary data)
    pub fn value_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.value)
    }
}

/// Cap caller that executes via XPC service with strict validation
pub struct CapCaller {
    cap: String,
    cap_set: Box<dyn CapSet>,
    cap_definition: Cap,
    media_registry: Arc<MediaUrnRegistry>,
}

/// Trait for Cap Host communication
pub trait CapSet: Send + Sync + std::fmt::Debug {
    /// Execute a cap with unified arguments identified by media_urn.
    /// The cap definition's sources specify how to extract values (stdin, position, cli_flag).
    fn execute_cap(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>;
}

impl CapCaller {
    /// Create a new cap caller with validation
    pub fn new(
        cap: String,
        cap_set: Box<dyn CapSet>,
        cap_definition: Cap,
        media_registry: Arc<MediaUrnRegistry>,
    ) -> Self {
        Self {
            cap,
            cap_set,
            cap_definition,
            media_registry,
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

    /// Call the cap with unified arguments identified by media_urn.
    /// Validates arguments against cap definition before execution.
    pub async fn call(&self, arguments: Vec<CapArgumentValue>) -> Result<ResponseWrapper> {
        // Validate arguments against cap definition
        self.validate_arguments(&arguments)?;

        // Execute via cap host method
        let (binary_output, text_output) = self.cap_set.execute_cap(
            &self.cap,
            &arguments,
        ).await?;

        // Resolve output spec to determine response type
        let output_spec = self.resolve_output_spec().await?;

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
            // Structured data (map/list) is serialized as JSON
            if output_spec.is_structured() {
                ResponseWrapper::from_json(text_data.into_bytes())
            } else {
                ResponseWrapper::from_text(text_data.into_bytes())
            }
        } else {
            return Err(anyhow!("Cap returned no output"));
        };

        // Validate output against cap definition (basic type check)
        self.validate_output_basic(&response).await?;

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
    /// - The spec ID cannot be resolved (not in media_specs, not in registry)
    async fn resolve_output_spec(&self) -> Result<ResolvedMediaSpec> {
        let cap_urn = CapUrn::from_string(&self.cap)
            .map_err(|e| anyhow!("Invalid cap URN '{}': {}", self.cap, e))?;

        // Direction specs are now required first-class fields
        let spec_id = cap_urn.out_spec();

        resolve_media_urn(spec_id, Some(self.cap_definition.get_media_specs()), &self.media_registry)
            .await
            .map_err(|e| anyhow!(
                "Failed to resolve output spec ID '{}' for cap '{}': {} - check that media_specs contains this spec ID or it is in the registry",
                spec_id, self.cap, e
            ))
    }

    /// Validate unified arguments against cap definition.
    /// Checks that all required arguments are provided (by media_urn).
    fn validate_arguments(&self, arguments: &[CapArgumentValue]) -> Result<()> {
        let arg_defs = self.cap_definition.get_args();

        // Build set of provided media_urns
        let provided_urns: std::collections::HashSet<_> = arguments
            .iter()
            .map(|a| a.media_urn.as_str())
            .collect();

        // Check all required arguments are provided
        for arg_def in arg_defs {
            if arg_def.required && !provided_urns.contains(arg_def.media_urn.as_str()) {
                return Err(anyhow!(
                    "Missing required argument: {}",
                    arg_def.media_urn
                ));
            }
        }

        // Check for unknown arguments
        let known_urns: std::collections::HashSet<_> = arg_defs
            .iter()
            .map(|a| a.media_urn.as_str())
            .collect();

        for arg in arguments {
            if !known_urns.contains(arg.media_urn.as_str()) {
                return Err(anyhow!(
                    "Unknown argument media_urn: {} (cap {} accepts: {:?})",
                    arg.media_urn,
                    self.cap,
                    known_urns
                ));
            }
        }

        Ok(())
    }

    /// Basic output validation
    /// Full async validation with ProfileSchemaRegistry should be done at a higher level
    async fn validate_output_basic(&self, response: &ResponseWrapper) -> Result<()> {
        let output_spec = self.resolve_output_spec().await?;

        // For structured outputs (map/list), verify it's valid JSON
        if let Ok(text) = response.as_string() {
            if output_spec.is_structured() {
                // Structured data must be valid JSON
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

    // TEST156: Test creating StdinSource Data variant with byte vector
    #[test]
    fn test_stdin_source_data_creation() {
        let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let source = StdinSource::Data(data.clone());

        match source {
            StdinSource::Data(d) => assert_eq!(d, data),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    // TEST157: Test creating StdinSource FileReference variant with all required fields
    #[test]
    fn test_stdin_source_file_reference_creation() {
        let tracked_file_id = "tracked-file-123".to_string();
        let original_path = "/path/to/original.pdf".to_string();
        let security_bookmark = vec![0x62, 0x6f, 0x6f, 0x6b]; // "book"
        let media_urn = "media:pdf;bytes".to_string();

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

    // TEST158: Test StdinSource Data with empty vector stores and retrieves correctly
    #[test]
    fn test_stdin_source_empty_data() {
        let source = StdinSource::Data(vec![]);

        match source {
            StdinSource::Data(d) => assert!(d.is_empty()),
            StdinSource::FileReference { .. } => panic!("Expected Data variant"),
        }
    }

    // TEST159: Test StdinSource Data with binary content like PNG header bytes
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

    // TEST160: Test StdinSource Data clone creates independent copy with same data
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

    // TEST161: Test StdinSource FileReference clone creates independent copy with same fields
    #[test]
    fn test_stdin_source_file_reference_clone() {
        let source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![1, 2, 3],
            media_urn: "media:pdf".to_string(),
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

    // TEST162: Test StdinSource Debug format displays variant type and relevant fields
    #[test]
    fn test_stdin_source_debug() {
        let data_source = StdinSource::Data(vec![1, 2, 3]);
        let debug_str = format!("{:?}", data_source);
        assert!(debug_str.contains("Data"));

        let file_source = StdinSource::FileReference {
            tracked_file_id: "test-id".to_string(),
            original_path: "/test/path.pdf".to_string(),
            security_bookmark: vec![],
            media_urn: "media:pdf".to_string(),
        };
        let debug_str = format!("{:?}", file_source);
        assert!(debug_str.contains("FileReference"));
        assert!(debug_str.contains("test-id"));
        assert!(debug_str.contains("/test/path.pdf"));
    }

    // TEST274: Test CapArgumentValue::new stores media_urn and raw byte value
    #[test]
    fn test_cap_argument_value_new() {
        let arg = CapArgumentValue::new("media:model-spec;textable;form=scalar", b"gpt-4".to_vec());
        assert_eq!(arg.media_urn, "media:model-spec;textable;form=scalar");
        assert_eq!(arg.value, b"gpt-4");
    }

    // TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes
    #[test]
    fn test_cap_argument_value_from_str() {
        let arg = CapArgumentValue::from_str("media:string;textable", "hello world");
        assert_eq!(arg.media_urn, "media:string;textable");
        assert_eq!(arg.value, b"hello world");
    }

    // TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data
    #[test]
    fn test_cap_argument_value_as_str_valid() {
        let arg = CapArgumentValue::from_str("media:string", "test");
        assert_eq!(arg.value_as_str().unwrap(), "test");
    }

    // TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data
    #[test]
    fn test_cap_argument_value_as_str_invalid_utf8() {
        let arg = CapArgumentValue::new("media:pdf;bytes", vec![0xFF, 0xFE, 0x80]);
        assert!(arg.value_as_str().is_err(), "non-UTF-8 data must fail");
    }

    // TEST278: Test CapArgumentValue::new with empty value stores empty vec
    #[test]
    fn test_cap_argument_value_empty() {
        let arg = CapArgumentValue::new("media:void", vec![]);
        assert!(arg.value.is_empty());
        assert_eq!(arg.value_as_str().unwrap(), "");
    }

    // TEST279: Test CapArgumentValue Clone produces independent copy with same data
    #[test]
    fn test_cap_argument_value_clone() {
        let arg = CapArgumentValue::new("media:test", b"data".to_vec());
        let cloned = arg.clone();
        assert_eq!(arg.media_urn, cloned.media_urn);
        assert_eq!(arg.value, cloned.value);
    }

    // TEST280: Test CapArgumentValue Debug format includes media_urn and value
    #[test]
    fn test_cap_argument_value_debug() {
        let arg = CapArgumentValue::from_str("media:test", "val");
        let debug = format!("{:?}", arg);
        assert!(debug.contains("media:test"), "debug must include media_urn");
    }

    // TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str)
    #[test]
    fn test_cap_argument_value_into_string() {
        let s = String::from("media:owned");
        let arg1 = CapArgumentValue::new(s, vec![]);
        assert_eq!(arg1.media_urn, "media:owned");

        let arg2 = CapArgumentValue::new("media:borrowed", vec![]);
        assert_eq!(arg2.media_urn, "media:borrowed");
    }

    // TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters
    #[test]
    fn test_cap_argument_value_unicode() {
        let arg = CapArgumentValue::from_str("media:string", "hello 世界 🌍");
        assert_eq!(arg.value_as_str().unwrap(), "hello 世界 🌍");
    }

    // TEST283: Test CapArgumentValue with large binary payload preserves all bytes
    #[test]
    fn test_cap_argument_value_large_binary() {
        let data: Vec<u8> = (0u8..=255).cycle().take(10000).collect();
        let arg = CapArgumentValue::new("media:pdf;bytes", data.clone());
        assert_eq!(arg.value.len(), 10000);
        assert_eq!(arg.value, data);
    }
}
