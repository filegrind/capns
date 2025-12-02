//! JSON Schema validation for capability arguments and outputs
//!
//! Provides comprehensive validation of JSON data against JSON Schema Draft-07.

use crate::{Cap, CapArgument, CapOutput};
use jsonschema::{JSONSchema, ValidationError as JsonSchemaError};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use thiserror::Error;

/// Schema validation error
#[derive(Error, Debug)]
pub enum SchemaValidationError {
    #[error("Schema compilation failed: {0}")]
    SchemaCompilation(String),
    
    #[error("Validation failed for argument '{argument}': {details}")]
    ArgumentValidation { argument: String, details: String },
    
    #[error("Validation failed for output: {details}")]
    OutputValidation { details: String },
    
    #[error("Schema not found for argument '{argument}'")]
    SchemaNotFound { argument: String },
    
    #[error("Schema reference '{schema_ref}' could not be resolved")]
    SchemaRefNotResolved { schema_ref: String },
    
    #[error("Invalid JSON value for validation")]
    InvalidJson,
}

/// Schema validator that handles both embedded schemas and schema references
pub struct SchemaValidator {
    /// Cache of compiled schemas for performance
    schema_cache: HashMap<String, JSONSchema>,
    
    /// External schema resolver (for schema_ref support)
    schema_resolver: Option<Box<dyn SchemaResolver>>,
}

/// Trait for resolving external schema references
pub trait SchemaResolver: Send + Sync {
    /// Resolve a schema reference to a JSON schema
    fn resolve_schema(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError>;
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new() -> Self {
        Self {
            schema_cache: HashMap::new(),
            schema_resolver: None,
        }
    }
    
    /// Create a schema validator with an external schema resolver
    pub fn with_resolver(resolver: Box<dyn SchemaResolver>) -> Self {
        Self {
            schema_cache: HashMap::new(),
            schema_resolver: Some(resolver),
        }
    }
    
    /// Validate all arguments for a capability against their schemas
    pub fn validate_arguments(
        &mut self,
        cap: &Cap,
        arguments: &[JsonValue],
    ) -> Result<(), SchemaValidationError> {
        let cap_args = &cap.arguments;
        // Validate required arguments
        for (index, arg_def) in cap_args.required.iter().enumerate() {
            if let Some(position) = arg_def.position {
                if let Some(arg_value) = arguments.get(position) {
                    self.validate_argument(arg_def, arg_value)?;
                }
            } else if index < arguments.len() {
                self.validate_argument(arg_def, &arguments[index])?;
            }
        }
        
        // Note: Optional arguments validation would need more complex logic
        // to handle named arguments or default values
        
        Ok(())
    }
    
    /// Validate a single argument against its schema
    pub fn validate_argument(
        &mut self,
        arg_def: &CapArgument,
        value: &JsonValue,
    ) -> Result<(), SchemaValidationError> {
        // Only validate if we have object or array types with schemas
        if !matches!(arg_def.arg_type, crate::ArgumentType::Object | crate::ArgumentType::Array) {
            return Ok(());
        }
        
        let schema = if let Some(embedded_schema) = &arg_def.schema {
            embedded_schema.clone()
        } else if let Some(schema_ref) = &arg_def.schema_ref {
            self.resolve_schema_ref(schema_ref)?
        } else {
            // No schema specified, skip validation
            return Ok(());
        };
        
        self.validate_value_against_schema(&arg_def.name, value, &schema)
    }
    
    /// Validate output against its schema
    pub fn validate_output(
        &mut self,
        output_def: &CapOutput,
        value: &JsonValue,
    ) -> Result<(), SchemaValidationError> {
        // Only validate structured outputs
        if !matches!(output_def.output_type, crate::OutputType::Object | crate::OutputType::Array) {
            return Ok(());
        }
        
        let schema = if let Some(embedded_schema) = &output_def.schema {
            embedded_schema.clone()
        } else if let Some(schema_ref) = &output_def.schema_ref {
            self.resolve_schema_ref(schema_ref)?
        } else {
            // No schema specified, skip validation
            return Ok(());
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
                return Err(SchemaValidationError::ArgumentValidation {
                    argument: name.to_string(),
                    details: error_details,
                });
            }
        }
        
        Ok(())
    }
    
    /// Resolve a schema reference using the configured resolver
    fn resolve_schema_ref(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError> {
        if let Some(resolver) = &self.schema_resolver {
            resolver.resolve_schema(schema_ref)
        } else {
            Err(SchemaValidationError::SchemaRefNotResolved {
                schema_ref: schema_ref.to_string(),
            })
        }
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
            .map_err(|_| SchemaValidationError::SchemaRefNotResolved {
                schema_ref: schema_ref.to_string(),
            })?;
        
        serde_json::from_str(&schema_content)
            .map_err(|_| SchemaValidationError::SchemaRefNotResolved {
                schema_ref: schema_ref.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArgumentType, OutputType, CapArgument, CapOutput};
    use serde_json::json;

    #[test]
    fn test_argument_schema_validation_success() {
        let mut validator = SchemaValidator::new();
        
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name"]
        });
        
        let arg = CapArgument {
            name: "user_data".to_string(),
            arg_type: ArgumentType::Object,
            arg_description: "User data".to_string(),
            cli_flag: "--user-data".to_string(),
            position: Some(0),
            validation: Default::default(),
            default_value: None,
            schema_ref: None,
            schema: Some(schema),
        };
        
        let valid_value = json!({"name": "John", "age": 30});
        assert!(validator.validate_argument(&arg, &valid_value).is_ok());
    }
    
    #[test]
    fn test_argument_schema_validation_failure() {
        let mut validator = SchemaValidator::new();
        
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });
        
        let arg = CapArgument {
            name: "user_data".to_string(),
            arg_type: ArgumentType::Object,
            arg_description: "User data".to_string(),
            cli_flag: "--user-data".to_string(),
            position: Some(0),
            validation: Default::default(),
            default_value: None,
            schema_ref: None,
            schema: Some(schema),
        };
        
        let invalid_value = json!({"age": 30}); // Missing required "name"
        assert!(validator.validate_argument(&arg, &invalid_value).is_err());
    }
    
    #[test]
    fn test_output_schema_validation_success() {
        let mut validator = SchemaValidator::new();
        
        let schema = json!({
            "type": "object",
            "properties": {
                "result": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"}
            },
            "required": ["result"]
        });
        
        let output = CapOutput {
            output_type: OutputType::Object,
            output_description: "Query result".to_string(),
            schema_ref: None,
            schema: Some(schema),
            content_type: Some("application/json".to_string()),
            validation: Default::default(),
        };
        
        let valid_value = json!({"result": "success", "timestamp": "2023-01-01T00:00:00Z"});
        assert!(validator.validate_output(&output, &valid_value).is_ok());
    }
    
    #[test]
    fn test_skip_validation_for_simple_types() {
        let mut validator = SchemaValidator::new();
        
        // String argument without schema should not be validated
        let arg = CapArgument {
            name: "simple_string".to_string(),
            arg_type: ArgumentType::String,
            arg_description: "Simple string".to_string(),
            cli_flag: "--string".to_string(),
            position: Some(0),
            validation: Default::default(),
            default_value: None,
            schema_ref: None,
            schema: None,
        };
        
        let value = json!("any string value");
        assert!(validator.validate_argument(&arg, &value).is_ok());
    }
}