//! Cap schema validation infrastructure
//!
//! This module provides strict validation of inputs and outputs against
//! cap schemas, ensuring adherence to advertised specifications.
//! Uses spec ID resolution to get media types and schemas from the media_specs table.
//! Uses ProfileSchemaRegistry for JSON Schema-based validation of profiles.

use crate::{Cap, CapArgument, CapOutput};
use crate::media_spec::resolve_spec_id;
use crate::profile_schema_cache::ProfileSchemaRegistry;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// Validation error types with descriptive failure information
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Unknown cap requested
    UnknownCap {
        cap_urn: String,
    },
    /// Missing required argument
    MissingRequiredArgument {
        cap_urn: String,
        argument_name: String,
    },
    /// Unknown argument provided
    UnknownArgument {
        cap_urn: String,
        argument_name: String,
    },
    /// Invalid argument type
    InvalidArgumentType {
        cap_urn: String,
        argument_name: String,
        expected_media_spec: String,
        actual_value: Value,
        schema_errors: Vec<String>,
    },
    /// Argument validation rule violation
    ArgumentValidationFailed {
        cap_urn: String,
        argument_name: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Invalid output type
    InvalidOutputType {
        cap_urn: String,
        expected_media_spec: String,
        actual_value: Value,
        schema_errors: Vec<String>,
    },
    /// Output validation rule violation
    OutputValidationFailed {
        cap_urn: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Malformed cap schema
    InvalidCapSchema {
        cap_urn: String,
        issue: String,
    },
    /// Too many arguments provided
    TooManyArguments {
        cap_urn: String,
        max_expected: usize,
        actual_count: usize,
    },
    /// JSON parsing error
    JsonParseError {
        cap_urn: String,
        error: String,
    },
    /// JSON schema validation error
    SchemaValidationFailed {
        cap_urn: String,
        field_name: String,
        schema_errors: String,
    },
    /// Invalid MediaSpec
    InvalidMediaSpec {
        cap_urn: String,
        field_name: String,
        error: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::UnknownCap { cap_urn } => {
                write!(f, "Unknown cap '{}' - cap not registered or advertised", cap_urn)
            }
            ValidationError::MissingRequiredArgument { cap_urn, argument_name } => {
                write!(f, "Cap '{}' requires argument '{}' but it was not provided", cap_urn, argument_name)
            }
            ValidationError::UnknownArgument { cap_urn, argument_name } => {
                write!(f, "Cap '{}' does not accept argument '{}' - check capability definition for valid arguments", cap_urn, argument_name)
            }
            ValidationError::InvalidArgumentType { cap_urn, argument_name, expected_media_spec, actual_value, schema_errors } => {
                write!(f, "Cap '{}' argument '{}' expects media_spec '{}' but validation failed for value {}: {}",
                       cap_urn, argument_name, expected_media_spec, actual_value, schema_errors.join(", "))
            }
            ValidationError::ArgumentValidationFailed { cap_urn, argument_name, validation_rule, actual_value } => {
                write!(f, "Cap '{}' argument '{}' failed validation rule '{}' with value: {}",
                       cap_urn, argument_name, validation_rule, actual_value)
            }
            ValidationError::InvalidOutputType { cap_urn, expected_media_spec, actual_value, schema_errors } => {
                write!(f, "Cap '{}' output expects media_spec '{}' but validation failed for value {}: {}",
                       cap_urn, expected_media_spec, actual_value, schema_errors.join(", "))
            }
            ValidationError::OutputValidationFailed { cap_urn, validation_rule, actual_value } => {
                write!(f, "Cap '{}' output failed validation rule '{}' with value: {}",
                       cap_urn, validation_rule, actual_value)
            }
            ValidationError::InvalidCapSchema { cap_urn, issue } => {
                write!(f, "Cap '{}' has invalid schema: {}", cap_urn, issue)
            }
            ValidationError::TooManyArguments { cap_urn, max_expected, actual_count } => {
                write!(f, "Cap '{}' expects at most {} arguments but received {}",
                       cap_urn, max_expected, actual_count)
            }
            ValidationError::JsonParseError { cap_urn, error } => {
                write!(f, "Cap '{}' JSON parsing failed: {}", cap_urn, error)
            }
            ValidationError::SchemaValidationFailed { cap_urn, field_name, schema_errors } => {
                write!(f, "Cap '{}' schema validation failed for '{}': {}", cap_urn, field_name, schema_errors)
            }
            ValidationError::InvalidMediaSpec { cap_urn, field_name, error } => {
                write!(f, "Cap '{}' has invalid media_spec for '{}': {}", cap_urn, field_name, error)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Input argument validator using ProfileSchemaRegistry
pub struct InputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
}

impl InputValidator {
    /// Create a new InputValidator with the given ProfileSchemaRegistry
    pub fn new(schema_registry: Arc<ProfileSchemaRegistry>) -> Self {
        Self { schema_registry }
    }

    /// Validate arguments against cap input schema
    pub async fn validate_positional_arguments(
        &self,
        cap: &Cap,
        arguments: &[Value],
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = &cap.arguments;

        // Check if too many arguments provided
        let max_args = args.required.len() + args.optional.len();
        if arguments.len() > max_args {
            return Err(ValidationError::TooManyArguments {
                cap_urn,
                max_expected: max_args,
                actual_count: arguments.len(),
            });
        }

        // Validate required arguments
        for (index, req_arg) in args.required.iter().enumerate() {
            if index >= arguments.len() {
                return Err(ValidationError::MissingRequiredArgument {
                    cap_urn: cap_urn.clone(),
                    argument_name: req_arg.name.clone(),
                });
            }

            self.validate_single_argument(cap, req_arg, &arguments[index]).await?;
        }

        // Validate optional arguments if provided
        let required_count = args.required.len();
        for (index, opt_arg) in args.optional.iter().enumerate() {
            let arg_index = required_count + index;
            if arg_index < arguments.len() {
                self.validate_single_argument(cap, opt_arg, &arguments[arg_index]).await?;
            }
        }

        Ok(())
    }

    /// Validate named arguments against cap input schema
    pub async fn validate_named_arguments(
        &self,
        cap: &Cap,
        named_args: &[Value],
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = &cap.arguments;

        // Extract named argument values into a map
        let mut provided_args = std::collections::HashMap::new();
        for arg in named_args {
            if let Value::Object(map) = arg {
                if let (Some(Value::String(name)), Some(value)) = (map.get("name"), map.get("value")) {
                    provided_args.insert(name.clone(), value.clone());
                }
            }
        }

        // Check that all required arguments are provided as named arguments
        for req_arg in &args.required {
            if !provided_args.contains_key(&req_arg.name) {
                return Err(ValidationError::MissingRequiredArgument {
                    cap_urn: cap_urn.clone(),
                    argument_name: format!("{} (expected as named argument)", req_arg.name),
                });
            }

            // Validate the provided argument value
            let provided_value = &provided_args[&req_arg.name];
            self.validate_single_argument(cap, req_arg, provided_value).await?;
        }

        // Validate optional arguments if provided
        for opt_arg in &args.optional {
            if let Some(provided_value) = provided_args.get(&opt_arg.name) {
                self.validate_single_argument(cap, opt_arg, provided_value).await?;
            }
        }

        // Check for unknown arguments
        let known_arg_names: std::collections::HashSet<String> = args.required.iter()
            .chain(args.optional.iter())
            .map(|arg| arg.name.clone())
            .collect();

        for provided_name in provided_args.keys() {
            if !known_arg_names.contains(provided_name) {
                return Err(ValidationError::UnknownArgument {
                    cap_urn: cap_urn.clone(),
                    argument_name: provided_name.clone(),
                });
            }
        }

        Ok(())
    }

    async fn validate_single_argument(
        &self,
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Type validation via resolved spec (includes local schema validation if present)
        self.validate_argument_type(cap, arg_def, value).await?;

        // Validation rules (min/max, length, pattern, allowed_values)
        self.validate_argument_rules(cap, arg_def, value)?;

        Ok(())
    }

    async fn validate_argument_type(
        &self,
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let media_specs = cap.get_media_specs();

        // Resolve the spec ID from the argument definition
        let resolved = resolve_spec_id(&arg_def.media_spec, media_specs)
            .map_err(|e| ValidationError::InvalidMediaSpec {
                cap_urn: cap_urn.clone(),
                field_name: arg_def.name.clone(),
                error: e.to_string(),
            })?;

        // For binary media types, we expect a base64-encoded string - no profile validation
        if resolved.is_binary() {
            if !matches!(value, Value::String(_)) {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.name.clone(),
                    expected_media_spec: arg_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: vec!["Expected base64-encoded string for binary type".to_string()],
                });
            }
            return Ok(());
        }

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.name.clone(),
                    expected_media_spec: arg_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(());
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.name.clone(),
                    expected_media_spec: arg_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(())
    }

    /// Validate a value against a local JSON schema
    fn validate_with_local_schema(&self, schema: &Value, value: &Value) -> Result<(), Vec<String>> {
        // Use jsonschema crate for validation
        let compiled = match jsonschema::JSONSchema::compile(schema) {
            Ok(c) => c,
            Err(e) => return Err(vec![format!("Failed to compile schema: {}", e)]),
        };

        let result = compiled.validate(value);
        match result {
            Ok(_) => Ok(()),
            Err(errors) => {
                let error_strings: Vec<String> = errors
                    .map(|e| format!("{}: {}", e.instance_path, e))
                    .collect();
                Err(error_strings)
            }
        }
    }

    fn validate_argument_rules(
        &self,
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let validation = &arg_def.validation;

        // Numeric validation
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::ArgumentValidationFailed {
                        cap_urn,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("minimum value {}", min),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max) = validation.max {
            if let Some(num) = value.as_f64() {
                if num > max {
                    return Err(ValidationError::ArgumentValidationFailed {
                        cap_urn,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("maximum value {}", max),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        // Length validation (for strings and arrays)
        if let Some(min_length) = validation.min_length {
            match (value.as_str(), value.as_array()) {
                (Some(s), _) => {
                    if s.len() < min_length {
                        return Err(ValidationError::ArgumentValidationFailed {
                            cap_urn,
                            argument_name: arg_def.name.clone(),
                            validation_rule: format!("minimum length {}", min_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                (_, Some(arr)) => {
                    if arr.len() < min_length {
                        return Err(ValidationError::ArgumentValidationFailed {
                            cap_urn,
                            argument_name: arg_def.name.clone(),
                            validation_rule: format!("minimum array length {}", min_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                _ => {}
            }
        }

        if let Some(max_length) = validation.max_length {
            match (value.as_str(), value.as_array()) {
                (Some(s), _) => {
                    if s.len() > max_length {
                        return Err(ValidationError::ArgumentValidationFailed {
                            cap_urn,
                            argument_name: arg_def.name.clone(),
                            validation_rule: format!("maximum length {}", max_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                (_, Some(arr)) => {
                    if arr.len() > max_length {
                        return Err(ValidationError::ArgumentValidationFailed {
                            cap_urn,
                            argument_name: arg_def.name.clone(),
                            validation_rule: format!("maximum array length {}", max_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                _ => {}
            }
        }

        // Pattern validation
        if let Some(pattern) = &validation.pattern {
            if let Some(s) = value.as_str() {
                if let Ok(regex) = regex::Regex::new(pattern) {
                    if !regex.is_match(s) {
                        return Err(ValidationError::ArgumentValidationFailed {
                            cap_urn,
                            argument_name: arg_def.name.clone(),
                            validation_rule: format!("pattern '{}'", pattern),
                            actual_value: value.clone(),
                        });
                    }
                }
            }
        }

        // Allowed values validation
        if let Some(allowed_values) = &validation.allowed_values {
            if let Some(s) = value.as_str() {
                if !allowed_values.contains(&s.to_string()) {
                    return Err(ValidationError::ArgumentValidationFailed {
                        cap_urn,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

/// Output validator using ProfileSchemaRegistry
pub struct OutputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
}

impl OutputValidator {
    /// Create a new OutputValidator with the given ProfileSchemaRegistry
    pub fn new(schema_registry: Arc<ProfileSchemaRegistry>) -> Self {
        Self { schema_registry }
    }

    /// Validate output against cap output schema
    pub async fn validate_output(
        &self,
        cap: &Cap,
        output: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        let output_def = cap.get_output()
            .ok_or_else(|| ValidationError::InvalidCapSchema {
                cap_urn: cap_urn.clone(),
                issue: "No output definition specified".to_string(),
            })?;

        // Type validation via resolved spec (includes local schema validation if present)
        self.validate_output_type(cap, output_def, output).await?;

        // Validation rules (min/max, length, pattern, allowed_values)
        self.validate_output_rules(cap, output_def, output)?;

        Ok(())
    }

    async fn validate_output_type(
        &self,
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let media_specs = cap.get_media_specs();

        // Resolve the spec ID from the output definition
        let resolved = resolve_spec_id(&output_def.media_spec, media_specs)
            .map_err(|e| ValidationError::InvalidMediaSpec {
                cap_urn: cap_urn.clone(),
                field_name: "output".to_string(),
                error: e.to_string(),
            })?;

        // For binary media types, we expect a base64-encoded string - no profile validation
        if resolved.is_binary() {
            if !matches!(value, Value::String(_)) {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_spec: output_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: vec!["Expected base64-encoded string for binary type".to_string()],
                });
            }
            return Ok(());
        }

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_spec: output_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(());
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_spec: output_def.media_spec.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(())
    }

    /// Validate a value against a local JSON schema
    fn validate_with_local_schema(&self, schema: &Value, value: &Value) -> Result<(), Vec<String>> {
        // Use jsonschema crate for validation
        let compiled = match jsonschema::JSONSchema::compile(schema) {
            Ok(c) => c,
            Err(e) => return Err(vec![format!("Failed to compile schema: {}", e)]),
        };

        let result = compiled.validate(value);
        match result {
            Ok(_) => Ok(()),
            Err(errors) => {
                let error_strings: Vec<String> = errors
                    .map(|e| format!("{}: {}", e.instance_path, e))
                    .collect();
                Err(error_strings)
            }
        }
    }

    fn validate_output_rules(
        &self,
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let validation = &output_def.validation;

        // Apply same validation rules as arguments
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::OutputValidationFailed {
                        cap_urn,
                        validation_rule: format!("minimum value {}", min),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max) = validation.max {
            if let Some(num) = value.as_f64() {
                if num > max {
                    return Err(ValidationError::OutputValidationFailed {
                        cap_urn,
                        validation_rule: format!("maximum value {}", max),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(min_length) = validation.min_length {
            if let Some(s) = value.as_str() {
                if s.len() < min_length {
                    return Err(ValidationError::OutputValidationFailed {
                        cap_urn,
                        validation_rule: format!("minimum length {}", min_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max_length) = validation.max_length {
            if let Some(s) = value.as_str() {
                if s.len() > max_length {
                    return Err(ValidationError::OutputValidationFailed {
                        cap_urn,
                        validation_rule: format!("maximum length {}", max_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(pattern) = &validation.pattern {
            if let Some(s) = value.as_str() {
                if let Ok(regex) = regex::Regex::new(pattern) {
                    if !regex.is_match(s) {
                        return Err(ValidationError::OutputValidationFailed {
                            cap_urn,
                            validation_rule: format!("pattern '{}'", pattern),
                            actual_value: value.clone(),
                        });
                    }
                }
            }
        }

        if let Some(allowed_values) = &validation.allowed_values {
            if let Some(s) = value.as_str() {
                if !allowed_values.contains(&s.to_string()) {
                    return Err(ValidationError::OutputValidationFailed {
                        cap_urn,
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

/// Cap schema validator
pub struct CapValidator;

impl CapValidator {
    /// Validate a cap definition itself
    pub fn validate_cap(cap: &Cap) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let media_specs = cap.get_media_specs();

        // Validate that required arguments don't have default values
        for arg in &cap.arguments.required {
            if arg.default_value.is_some() {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn: cap_urn.clone(),
                    issue: format!("Required argument '{}' cannot have a default value", arg.name),
                });
            }
        }

        // Validate argument position uniqueness
        let mut positions = std::collections::HashSet::new();
        for arg in cap.arguments.required.iter().chain(cap.arguments.optional.iter()) {
            if let Some(pos) = arg.position {
                if !positions.insert(pos) {
                    return Err(ValidationError::InvalidCapSchema {
                        cap_urn: cap_urn.clone(),
                        issue: format!("Duplicate argument position {} for argument '{}'", pos, arg.name),
                    });
                }
            }
        }

        // Validate CLI flag uniqueness
        let mut cli_flags = std::collections::HashSet::new();
        for arg in cap.arguments.required.iter().chain(cap.arguments.optional.iter()) {
            let flag = &arg.cli_flag;
            if !flag.is_empty() {
                if !cli_flags.insert(flag) {
                    return Err(ValidationError::InvalidCapSchema {
                        cap_urn: cap_urn.clone(),
                        issue: format!("Duplicate CLI flag '{}' for argument '{}'", flag, arg.name),
                    });
                }
            }
        }

        // Validate that all media_spec IDs can be resolved
        for arg in cap.arguments.required.iter().chain(cap.arguments.optional.iter()) {
            resolve_spec_id(&arg.media_spec, media_specs)
                .map_err(|e| ValidationError::InvalidMediaSpec {
                    cap_urn: cap_urn.clone(),
                    field_name: arg.name.clone(),
                    error: e.to_string(),
                })?;
        }

        if let Some(output) = cap.get_output() {
            resolve_spec_id(&output.media_spec, media_specs)
                .map_err(|e| ValidationError::InvalidMediaSpec {
                    cap_urn: cap_urn.clone(),
                    field_name: "output".to_string(),
                    error: e.to_string(),
                })?;
        }

        Ok(())
    }
}

/// Main validation coordinator that orchestrates input and output validation
#[derive(Debug, Clone)]
pub struct SchemaValidator {
    caps: std::collections::HashMap<String, Cap>,
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self {
            caps: std::collections::HashMap::new(),
        }
    }

    /// Register a cap schema for validation
    pub fn register_cap(&mut self, cap: Cap) {
        let urn = cap.urn_string();
        self.caps.insert(urn, cap);
    }

    /// Get a cap by URN
    pub fn get_cap(&self, cap_urn: &str) -> Option<&Cap> {
        self.caps.get(cap_urn)
    }

    /// Validate arguments against a cap's input schema
    pub async fn validate_inputs(
        &self,
        cap_urn: &str,
        arguments: &[serde_json::Value],
        schema_registry: Arc<ProfileSchemaRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = InputValidator::new(schema_registry);
        validator.validate_positional_arguments(cap, arguments).await
    }

    /// Validate output against a cap's output schema
    pub async fn validate_output(
        &self,
        cap_urn: &str,
        output: &serde_json::Value,
        schema_registry: Arc<ProfileSchemaRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = OutputValidator::new(schema_registry);
        validator.validate_output(cap, output).await
    }

    /// Validate a cap definition itself
    pub fn validate_cap_schema(
        &self,
        cap: &Cap,
    ) -> Result<(), ValidationError> {
        CapValidator::validate_cap(cap)
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapUrn, CapArguments};
    use crate::standard::media::{SPEC_ID_STR, SPEC_ID_INT};
    use serde_json::json;

    #[tokio::test]
    async fn test_input_validation_success() {
        let schema_registry = Arc::new(ProfileSchemaRegistry::new().await.unwrap());
        let validator = InputValidator::new(schema_registry);

        let urn = CapUrn::from_string("cap:type=test;op=cap").unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "file_path",
            SPEC_ID_STR,
            "Path to file",
            "--file",
        ));

        cap.set_arguments(args);

        let input_args = vec![json!("/path/to/file.txt")];

        assert!(validator.validate_positional_arguments(&cap, &input_args).await.is_ok());
    }

    #[tokio::test]
    async fn test_input_validation_missing_required() {
        let schema_registry = Arc::new(ProfileSchemaRegistry::new().await.unwrap());
        let validator = InputValidator::new(schema_registry);

        let urn = CapUrn::from_string("cap:type=test;op=cap").unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "file_path",
            SPEC_ID_STR,
            "Path to file",
            "--file",
        ));

        cap.set_arguments(args);

        let input_args = vec![]; // Missing required argument

        let result = validator.validate_positional_arguments(&cap, &input_args).await;
        assert!(result.is_err());

        if let Err(ValidationError::MissingRequiredArgument { argument_name, .. }) = result {
            assert_eq!(argument_name, "file_path");
        } else {
            panic!("Expected MissingRequiredArgument error");
        }
    }

    #[tokio::test]
    async fn test_input_validation_wrong_type() {
        let schema_registry = Arc::new(ProfileSchemaRegistry::new().await.unwrap());
        let validator = InputValidator::new(schema_registry);

        let urn = CapUrn::from_string("cap:type=test;op=cap").unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "width",
            SPEC_ID_INT,
            "Width value",
            "--width",
        ));

        cap.set_arguments(args);

        let input_args = vec![json!("not_a_number")]; // Wrong type

        let result = validator.validate_positional_arguments(&cap, &input_args).await;
        assert!(result.is_err());

        if let Err(ValidationError::InvalidArgumentType { .. }) = result {
            // Expected
        } else {
            panic!("Expected InvalidArgumentType error");
        }
    }
}

/// Validate cap arguments against canonical definition
pub async fn validate_cap_arguments(
    registry: &crate::registry::CapRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    cap_urn: &str,
    arguments: &[Value],
) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    let validator = InputValidator::new(schema_registry);
    validator.validate_positional_arguments(&canonical_cap, arguments).await
}

/// Validate cap output against canonical definition
pub async fn validate_cap_output(
    registry: &crate::registry::CapRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    cap_urn: &str,
    output: &Value,
) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    let validator = OutputValidator::new(schema_registry);
    validator.validate_output(&canonical_cap, output).await
}

/// Validate that a local cap matches its canonical definition
pub async fn validate_cap_canonical(registry: &crate::registry::CapRegistry, cap: &Cap) -> Result<(), ValidationError> {
    registry.validate_cap(cap).await
        .map_err(|e| ValidationError::InvalidCapSchema {
            cap_urn: cap.urn_string(),
            issue: e.to_string(),
        })
}
