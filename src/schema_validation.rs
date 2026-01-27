//! JSON Schema validation for capability arguments and outputs
//!
//! Provides comprehensive validation of JSON data against JSON Schema Draft-07.
//! Schemas are located in the `media_specs` table of the cap definition or in the registry.

use crate::{Cap, CapOutput, CapArg};
use crate::media_spec::resolve_media_urn;
use crate::media_registry::MediaUrnRegistry;
use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use thiserror::Error;

/// Schema validation error
#[derive(Error, Debug)]
pub enum SchemaValidationError {
    #[error("Schema compilation failed: {0}")]
    SchemaCompilation(String),

    #[error("Validation failed for argument '{argument}': {details}")]
    MediaValidation { argument: String, details: String },

    #[error("Validation failed for output: {details}")]
    OutputValidation { details: String },

    #[error("Media URN '{media_urn}' could not be resolved: {error}")]
    MediaUrnNotResolved { media_urn: String, error: String },

    #[error("Invalid JSON value for validation")]
    InvalidJson,
}

/// Schema validator that resolves schemas from media_specs and registry
pub struct SchemaValidator {
    /// Cache of compiled schemas for performance
    schema_cache: HashMap<String, JSONSchema>,
}

/// Trait for resolving external schema references (for legacy/external schemas)
pub trait SchemaResolver: Send + Sync {
    /// Resolve a schema reference to a JSON schema
    fn resolve_schema(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError>;
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new() -> Self {
        Self {
            schema_cache: HashMap::new(),
        }
    }

    /// Validate all arguments for a capability against their schemas
    pub async fn validate_arguments(
        &mut self,
        cap: &Cap,
        arguments: &[JsonValue],
        registry: &MediaUrnRegistry,
    ) -> Result<(), SchemaValidationError> {
        let args = cap.get_args();

        // Get positional args sorted by position
        let mut positional_args: Vec<(&CapArg, usize)> = args.iter()
            .filter_map(|arg| {
                arg.sources.iter()
                    .find_map(|s| if let crate::ArgSource::Position { position } = s {
                        Some((arg, *position))
                    } else {
                        None
                    })
            })
            .collect();
        positional_args.sort_by_key(|(_, pos)| *pos);

        // Validate positional arguments
        for (arg_def, position) in positional_args {
            if let Some(arg_value) = arguments.get(position) {
                self.validate_argument_with_cap(cap, arg_def, arg_value, registry).await?;
            }
        }

        Ok(())
    }

    /// Validate a single argument against its schema from media_specs or registry
    pub async fn validate_argument_with_cap(
        &mut self,
        cap: &Cap,
        arg_def: &CapArg,
        value: &JsonValue,
        registry: &MediaUrnRegistry,
    ) -> Result<(), SchemaValidationError> {
        // Resolve the spec ID to get the schema
        let resolved = resolve_media_urn(&arg_def.media_urn, Some(cap.get_media_specs()), registry)
            .await
            .map_err(|e| SchemaValidationError::MediaUrnNotResolved {
                media_urn: arg_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // If no schema in the resolved spec, skip validation
        let schema = match resolved.schema {
            Some(s) => s,
            None => return Ok(()),
        };

        self.validate_value_against_schema(&arg_def.media_urn, value, &schema)
    }

    /// Validate output against its schema from media_specs or registry
    pub async fn validate_output_with_cap(
        &mut self,
        cap: &Cap,
        output_def: &CapOutput,
        value: &JsonValue,
        registry: &MediaUrnRegistry,
    ) -> Result<(), SchemaValidationError> {
        // Resolve the spec ID to get the schema
        let resolved = resolve_media_urn(&output_def.media_urn, Some(cap.get_media_specs()), registry)
            .await
            .map_err(|e| SchemaValidationError::MediaUrnNotResolved {
                media_urn: output_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // If no schema in the resolved spec, skip validation
        let schema = match resolved.schema {
            Some(s) => s,
            None => return Ok(()),
        };

        self.validate_value_against_schema("output", value, &schema)
    }

    /// Validate a JSON value against a schema
    fn validate_value_against_schema(
        &mut self,
        name: &str,
        value: &JsonValue,
        schema: &JsonValue,
    ) -> Result<(), SchemaValidationError> {
        let schema_key = serde_json::to_string(schema)
            .map_err(|_| SchemaValidationError::InvalidJson)?;

        // Use cached compiled schema or compile new one
        let compiled_schema = if let Some(cached) = self.schema_cache.get(&schema_key) {
            cached
        } else {
            let compiled = JSONSchema::compile(schema)
                .map_err(|e| SchemaValidationError::SchemaCompilation(e.to_string()))?;
            self.schema_cache.insert(schema_key.clone(), compiled);
            self.schema_cache.get(&schema_key).unwrap()
        };

        // Validate the value
        if let Err(validation_errors) = compiled_schema.validate(value) {
            let error_details = validation_errors
                .map(|e| format!("  - {}", e))
                .collect::<Vec<_>>()
                .join("\n");

            if name == "output" {
                return Err(SchemaValidationError::OutputValidation {
                    details: error_details,
                });
            } else {
                return Err(SchemaValidationError::MediaValidation {
                    argument: name.to_string(),
                    details: error_details,
                });
            }
        }

        Ok(())
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple file-based schema resolver for external schemas
pub struct FileSchemaResolver {
    base_path: std::path::PathBuf,
}

impl FileSchemaResolver {
    /// Create a new file-based schema resolver
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
    }
}

impl SchemaResolver for FileSchemaResolver {
    fn resolve_schema(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError> {
        let schema_path = self.base_path.join(schema_ref);
        let schema_content = std::fs::read_to_string(&schema_path)
            .map_err(|_| SchemaValidationError::MediaUrnNotResolved {
                media_urn: schema_ref.to_string(),
                error: "File not found".to_string(),
            })?;

        serde_json::from_str(&schema_content)
            .map_err(|_| SchemaValidationError::MediaUrnNotResolved {
                media_urn: schema_ref.to_string(),
                error: "Invalid JSON".to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::media::MEDIA_STRING;
    use crate::media_spec::{MediaSpecDef, MediaSpecDefObject};
    use crate::{CapUrn, CapArg, ArgSource};
    use serde_json::json;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!("cap:in=media:void;out=media:object;{}", tags)
    }

    // Helper to create a test registry
    async fn test_registry() -> MediaUrnRegistry {
        MediaUrnRegistry::new().await.expect("Failed to create test registry")
    }

    #[tokio::test]
    async fn test_argument_schema_validation_success() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name"]
        });

        // Create cap with media_specs containing the schema
        let urn = CapUrn::from_string(&test_urn("type=test;op=validate")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "test".to_string());
        cap.add_media_spec(
            "my:user-data.v1",
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "application/json".to_string(),
                profile_uri: "https://example.com/schema/user-data".to_string(),
                schema: Some(schema),
                title: None,
                description: None,
                validation: None,
                metadata: None,
            }),
        );

        let arg = CapArg::new(
            "my:user-data.v1",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let valid_value = json!({"name": "John", "age": 30});
        assert!(validator.validate_argument_with_cap(&cap, &arg, &valid_value, &registry).await.is_ok());
    }

    #[tokio::test]
    async fn test_argument_schema_validation_failure() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        // Create cap with media_specs containing the schema
        let urn = CapUrn::from_string(&test_urn("type=test;op=validate")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "test".to_string());
        cap.add_media_spec(
            "my:user-data.v1",
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "application/json".to_string(),
                profile_uri: "https://example.com/schema/user-data".to_string(),
                schema: Some(schema),
                title: None,
                description: None,
                validation: None,
                metadata: None,
            }),
        );

        let arg = CapArg::new(
            "my:user-data.v1",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let invalid_value = json!({"age": 30}); // Missing required "name"
        assert!(validator.validate_argument_with_cap(&cap, &arg, &invalid_value, &registry).await.is_err());
    }

    #[tokio::test]
    async fn test_output_schema_validation_success() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "result": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"}
            },
            "required": ["result"]
        });

        // Create cap with media_specs containing the schema
        let urn = CapUrn::from_string(&test_urn("type=test;op=validate")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "test".to_string());
        cap.add_media_spec(
            "my:query-result.v1",
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "application/json".to_string(),
                profile_uri: "https://example.com/schema/query-result".to_string(),
                schema: Some(schema),
                title: None,
                description: None,
                validation: None,
                metadata: None,
            }),
        );

        let output = CapOutput::new("my:query-result.v1", "Query result");

        let valid_value = json!({"result": "success", "timestamp": "2023-01-01T00:00:00Z"});
        assert!(validator.validate_output_with_cap(&cap, &output, &valid_value, &registry).await.is_ok());
    }

    #[tokio::test]
    async fn test_skip_validation_without_schema() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        // Create cap - using built-in spec ID which has no local schema
        let urn = CapUrn::from_string(&test_urn("type=test;op=validate")).unwrap();
        let cap = Cap::new(urn, "Test".to_string(), "test".to_string());

        // Argument using built-in spec ID (should resolve from registry, no schema)
        let arg = CapArg::new(
            MEDIA_STRING,
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let value = json!("any string value");
        // Should succeed - MEDIA_STRING resolves from registry and has no schema
        assert!(validator.validate_argument_with_cap(&cap, &arg, &value, &registry).await.is_ok());
    }

    #[tokio::test]
    async fn test_unresolvable_media_urn_fails_hard() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let urn = CapUrn::from_string(&test_urn("type=test;op=validate")).unwrap();
        let cap = Cap::new(urn, "Test".to_string(), "test".to_string());

        // Argument with unknown media URN - not in media_specs and not in registry
        let arg = CapArg::new(
            "media:completely-unknown-urn-that-does-not-exist",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let value = json!("test");
        let result = validator.validate_argument_with_cap(&cap, &arg, &value, &registry).await;
        assert!(result.is_err());

        if let Err(SchemaValidationError::MediaUrnNotResolved { media_urn, .. }) = result {
            assert_eq!(media_urn, "media:completely-unknown-urn-that-does-not-exist");
        } else {
            panic!("Expected MediaUrnNotResolved error");
        }
    }
}
