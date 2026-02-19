//! Cap schema validation infrastructure
//!
//! This module provides strict validation of inputs and outputs against
//! cap schemas, ensuring adherence to advertised specifications.
//! Uses spec ID resolution to get media types and schemas from the media_specs table.
//! Uses ProfileSchemaRegistry for JSON Schema-based validation of profiles.

use crate::{Cap, CapOutput, CapArg, ArgSource};
use crate::media::spec::{resolve_media_urn, ResolvedMediaSpec, MediaValidation};
use std::collections::HashSet;
use crate::media::profile::ProfileSchemaRegistry;
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
    /// Media spec validation rule violation (inherent to the semantic type)
    MediaSpecValidationFailed {
        cap_urn: String,
        argument_name: String,
        media_urn: String,
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
    /// Output media spec validation rule violation (inherent to the semantic type)
    OutputMediaSpecValidationFailed {
        cap_urn: String,
        media_urn: String,
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
    /// Unresolvable Media URN reference
    UnresolvableMediaUrn {
        cap_urn: String,
        media_urn: String,
        location: String,
    },
    /// XV5: Inline media spec redefines an existing registry spec
    InlineMediaSpecRedefinesRegistry {
        cap_urn: String,
        media_urn: String,
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
            ValidationError::MediaSpecValidationFailed { cap_urn, argument_name, media_urn, validation_rule, actual_value } => {
                write!(f, "Cap '{}' argument '{}' failed media spec '{}' validation rule '{}' with value: {}",
                       cap_urn, argument_name, media_urn, validation_rule, actual_value)
            }
            ValidationError::InvalidOutputType { cap_urn, expected_media_spec, actual_value, schema_errors } => {
                write!(f, "Cap '{}' output expects media_spec '{}' but validation failed for value {}: {}",
                       cap_urn, expected_media_spec, actual_value, schema_errors.join(", "))
            }
            ValidationError::OutputMediaSpecValidationFailed { cap_urn, media_urn, validation_rule, actual_value } => {
                write!(f, "Cap '{}' output failed media spec '{}' validation rule '{}' with value: {}",
                       cap_urn, media_urn, validation_rule, actual_value)
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
            ValidationError::UnresolvableMediaUrn { cap_urn, media_urn, location } => {
                write!(f, "Cap '{}' references unresolvable media URN '{}' in {}", cap_urn, media_urn, location)
            }
            ValidationError::InlineMediaSpecRedefinesRegistry { cap_urn, media_urn } => {
                write!(f, "XV5: Cap '{}' inline media spec '{}' redefines existing registry spec", cap_urn, media_urn)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Input argument validator using ProfileSchemaRegistry and MediaUrnRegistry
pub struct InputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
    media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
}

impl InputValidator {
    /// Create a new InputValidator with the given registries
    pub fn new(
        schema_registry: Arc<ProfileSchemaRegistry>,
        media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    ) -> Self {
        Self { schema_registry, media_registry }
    }

    /// Validate arguments against cap input schema
    pub async fn validate_positional_arguments(
        &self,
        cap: &Cap,
        arguments: &[Value],
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = cap.get_args();

        // Get positional args sorted by position
        let positional_args: Vec<&CapArg> = args.iter()
            .filter(|arg| arg.sources.iter().any(|s| matches!(s, ArgSource::Position { .. })))
            .collect();

        // Sort by position
        let mut sorted_positional: Vec<(&CapArg, usize)> = positional_args.iter()
            .filter_map(|arg| {
                arg.sources.iter()
                    .find_map(|s| if let ArgSource::Position { position } = s { Some((*arg, *position)) } else { None })
            })
            .collect();
        sorted_positional.sort_by_key(|(_, pos)| *pos);

        // Check if too many arguments provided
        if arguments.len() > sorted_positional.len() {
            return Err(ValidationError::TooManyArguments {
                cap_urn,
                max_expected: sorted_positional.len(),
                actual_count: arguments.len(),
            });
        }

        // Validate each positional argument
        for (index, (arg_def, _pos)) in sorted_positional.iter().enumerate() {
            if index >= arguments.len() {
                // Missing argument - check if required
                if arg_def.required {
                    return Err(ValidationError::MissingRequiredArgument {
                        cap_urn: cap_urn.clone(),
                        argument_name: arg_def.media_urn.clone(),
                    });
                }
                continue;
            }

            self.validate_single_argument(cap, arg_def, &arguments[index]).await?;
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
        let args = cap.get_args();

        // Extract named argument values into a map (map by media_urn)
        let mut provided_args = std::collections::HashMap::new();
        for arg in named_args {
            if let Value::Object(map) = arg {
                if let (Some(Value::String(media_urn)), Some(value)) = (map.get("name"), map.get("value")) {
                    provided_args.insert(media_urn.clone(), value.clone());
                }
            }
        }

        // Check all cap args - match by media_urn
        for arg_def in args {
            if let Some(provided_value) = provided_args.get(&arg_def.media_urn) {
                self.validate_single_argument(cap, arg_def, provided_value).await?;
            } else if arg_def.required {
                // Check if it has a cli_flag source (meaning it can be provided as named arg)
                let has_cli_flag = arg_def.sources.iter()
                    .any(|s| matches!(s, ArgSource::CliFlag { .. }));
                if has_cli_flag {
                    return Err(ValidationError::MissingRequiredArgument {
                        cap_urn: cap_urn.clone(),
                        argument_name: format!("{} (expected as named argument)", arg_def.media_urn),
                    });
                }
            }
        }

        // Check for unknown arguments - match by media_urn
        let known_media_urns: HashSet<String> = args.iter()
            .map(|arg| arg.media_urn.clone())
            .collect();

        for provided_media_urn in provided_args.keys() {
            if !known_media_urns.contains(provided_media_urn) {
                return Err(ValidationError::UnknownArgument {
                    cap_urn: cap_urn.clone(),
                    argument_name: provided_media_urn.clone(),
                });
            }
        }

        Ok(())
    }

    async fn validate_single_argument(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Type validation via resolved spec (includes local schema validation if present)
        // Returns the resolved media spec so we can access its validation rules
        let resolved = self.validate_argument_type(cap, arg_def, value).await?;

        // Media spec validation rules (inherent to the semantic type)
        if let Some(ref validation) = resolved.validation {
            self.validate_media_spec_rules(cap, arg_def, &resolved, validation, value)?;
        }

        Ok(())
    }

    async fn validate_argument_type(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        value: &Value,
    ) -> Result<ResolvedMediaSpec, ValidationError> {
        let cap_urn = cap.urn_string();

        // Resolve the spec ID from the argument definition
        let resolved = resolve_media_urn(&arg_def.media_urn, Some(cap.get_media_specs()), &self.media_registry)
            .await
            .map_err(|e| ValidationError::InvalidMediaSpec {
                cap_urn: cap_urn.clone(),
                field_name: arg_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // For binary media types, we expect a base64-encoded string - no profile validation
        if resolved.is_binary() {
            if !matches!(value, Value::String(_)) {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.media_urn.clone(),
                    expected_media_spec: arg_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: vec!["Expected base64-encoded string for binary type".to_string()],
                });
            }
            return Ok(resolved);
        }

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.media_urn.clone(),
                    expected_media_spec: arg_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(resolved);
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.media_urn.clone(),
                    expected_media_spec: arg_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(resolved)
    }

    /// Validate value against media spec's inherent validation rules (first pass)
    fn validate_media_spec_rules(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        resolved: &ResolvedMediaSpec,
        validation: &MediaValidation,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        // Numeric validation
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::MediaSpecValidationFailed {
                        cap_urn,
                        argument_name: arg_def.media_urn.clone(),
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("minimum value {}", min),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max) = validation.max {
            if let Some(num) = value.as_f64() {
                if num > max {
                    return Err(ValidationError::MediaSpecValidationFailed {
                        cap_urn,
                        argument_name: arg_def.media_urn.clone(),
                        media_urn: resolved.media_urn.clone(),
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
                        return Err(ValidationError::MediaSpecValidationFailed {
                            cap_urn,
                            argument_name: arg_def.media_urn.clone(),
                            media_urn: resolved.media_urn.clone(),
                            validation_rule: format!("minimum length {}", min_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                (_, Some(arr)) => {
                    if arr.len() < min_length {
                        return Err(ValidationError::MediaSpecValidationFailed {
                            cap_urn,
                            argument_name: arg_def.media_urn.clone(),
                            media_urn: resolved.media_urn.clone(),
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
                        return Err(ValidationError::MediaSpecValidationFailed {
                            cap_urn,
                            argument_name: arg_def.media_urn.clone(),
                            media_urn: resolved.media_urn.clone(),
                            validation_rule: format!("maximum length {}", max_length),
                            actual_value: value.clone(),
                        });
                    }
                },
                (_, Some(arr)) => {
                    if arr.len() > max_length {
                        return Err(ValidationError::MediaSpecValidationFailed {
                            cap_urn,
                            argument_name: arg_def.media_urn.clone(),
                            media_urn: resolved.media_urn.clone(),
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
                let regex = regex::Regex::new(pattern)
                    .map_err(|e| ValidationError::InvalidCapSchema {
                        cap_urn: cap_urn.clone(),
                        issue: format!("Invalid regex pattern '{}' in media spec '{}': {}", pattern, resolved.media_urn, e),
                    })?;
                if !regex.is_match(s) {
                    return Err(ValidationError::MediaSpecValidationFailed {
                        cap_urn,
                        argument_name: arg_def.media_urn.clone(),
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("pattern '{}'", pattern),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        // Allowed values validation
        if let Some(allowed_values) = &validation.allowed_values {
            if let Some(s) = value.as_str() {
                if !allowed_values.contains(&s.to_string()) {
                    return Err(ValidationError::MediaSpecValidationFailed {
                        cap_urn,
                        argument_name: arg_def.media_urn.clone(),
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }

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
}

/// Output validator using ProfileSchemaRegistry and MediaUrnRegistry
pub struct OutputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
    media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
}

impl OutputValidator {
    /// Create a new OutputValidator with the given registries
    pub fn new(
        schema_registry: Arc<ProfileSchemaRegistry>,
        media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    ) -> Self {
        Self { schema_registry, media_registry }
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
        // Returns the resolved media spec so we can access its validation rules
        let resolved = self.validate_output_type(cap, output_def, output).await?;

        // Media spec validation rules (inherent to the semantic type)
        if let Some(ref validation) = resolved.validation {
            self.validate_output_media_spec_rules(cap, &resolved, validation, output)?;
        }

        Ok(())
    }

    async fn validate_output_type(
        &self,
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<ResolvedMediaSpec, ValidationError> {
        let cap_urn = cap.urn_string();

        // Resolve the spec ID from the output definition
        let resolved = resolve_media_urn(&output_def.media_urn, Some(cap.get_media_specs()), &self.media_registry)
            .await
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
                    expected_media_spec: output_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: vec!["Expected base64-encoded string for binary type".to_string()],
                });
            }
            return Ok(resolved);
        }

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_spec: output_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(resolved);
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_spec: output_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(resolved)
    }

    /// Validate output value against media spec's inherent validation rules (first pass)
    fn validate_output_media_spec_rules(
        &self,
        cap: &Cap,
        resolved: &ResolvedMediaSpec,
        validation: &MediaValidation,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        // Numeric validation
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("minimum value {}", min),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max) = validation.max {
            if let Some(num) = value.as_f64() {
                if num > max {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("maximum value {}", max),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        // Length validation
        if let Some(min_length) = validation.min_length {
            if let Some(s) = value.as_str() {
                if s.len() < min_length {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("minimum length {}", min_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        if let Some(max_length) = validation.max_length {
            if let Some(s) = value.as_str() {
                if s.len() > max_length {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("maximum length {}", max_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        // Pattern validation
        if let Some(pattern) = &validation.pattern {
            if let Some(s) = value.as_str() {
                let regex = regex::Regex::new(pattern)
                    .map_err(|e| ValidationError::InvalidCapSchema {
                        cap_urn: cap_urn.clone(),
                        issue: format!("Invalid regex pattern '{}' in media spec '{}': {}", pattern, resolved.media_urn, e),
                    })?;
                if !regex.is_match(s) {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("pattern '{}'", pattern),
                        actual_value: value.clone(),
                    });
                }
            }
        }

        // Allowed values validation
        if let Some(allowed_values) = &validation.allowed_values {
            if let Some(s) = value.as_str() {
                if !allowed_values.contains(&s.to_string()) {
                    return Err(ValidationError::OutputMediaSpecValidationFailed {
                        cap_urn,
                        media_urn: resolved.media_urn.clone(),
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }

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
}

/// Cap schema validator
pub struct CapValidator;

impl CapValidator {
    /// Validate a cap definition itself
    pub async fn validate_cap(
        cap: &Cap,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = cap.get_args();

        // Validate that required arguments don't have default values
        for arg in args {
            if arg.required && arg.default_value.is_some() {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn: cap_urn.clone(),
                    issue: format!("Required argument '{}' cannot have a default value", arg.media_urn),
                });
            }
        }

        // Validate argument position uniqueness
        let mut positions = std::collections::HashSet::new();
        for arg in args {
            for source in &arg.sources {
                if let ArgSource::Position { position } = source {
                    if !positions.insert(*position) {
                        return Err(ValidationError::InvalidCapSchema {
                            cap_urn: cap_urn.clone(),
                            issue: format!("Duplicate argument position {} for argument '{}'", position, arg.media_urn),
                        });
                    }
                }
            }
        }

        // Validate CLI flag uniqueness
        let mut cli_flags = std::collections::HashSet::new();
        for arg in args {
            for source in &arg.sources {
                if let ArgSource::CliFlag { cli_flag } = source {
                    if !cli_flag.is_empty() {
                        if !cli_flags.insert(cli_flag) {
                            return Err(ValidationError::InvalidCapSchema {
                                cap_urn: cap_urn.clone(),
                                issue: format!("Duplicate CLI flag '{}' for argument '{}'", cli_flag, arg.media_urn),
                            });
                        }
                    }
                }
            }
        }

        // Validate that all media_spec IDs can be resolved
        for arg in args {
            resolve_media_urn(&arg.media_urn, Some(cap.get_media_specs()), registry)
                .await
                .map_err(|e| ValidationError::InvalidMediaSpec {
                    cap_urn: cap_urn.clone(),
                    field_name: arg.media_urn.clone(),
                    error: e.to_string(),
                })?;
        }

        if let Some(output) = cap.get_output() {
            resolve_media_urn(&output.media_urn, Some(cap.get_media_specs()), registry)
                .await
                .map_err(|e| ValidationError::InvalidMediaSpec {
                    cap_urn: cap_urn.clone(),
                    field_name: "output".to_string(),
                    error: e.to_string(),
                })?;
        }

        Ok(())
    }
}

/// Reserved CLI flags that cannot be used
pub const RESERVED_CLI_FLAGS: &[&str] = &["manifest", "--help", "--version", "-v", "-h"];

/// Validate cap args against the 12 validation rules for the new args format
///
/// # Validation Rules
/// - RULE1: No duplicate media_urns
/// - RULE2: sources must not be null or empty
/// - RULE3: If multiple args have stdin source, stdin media_urns must be identical
/// - RULE4: No arg may specify same source type more than once
/// - RULE5: No two args may have same position
/// - RULE6: Positions must be sequential (0-based, no gaps when aggregated)
/// - RULE7: No arg may have both position and cli_flag
/// - RULE8: No unknown keys in source objects (enforced by serde)
/// - RULE9: No two args may have same cli_flag
/// - RULE10: Reserved cli_flags cannot be used
/// - RULE11: cli_flag used verbatim as specified (enforced by design)
/// - RULE12: media_urn is the key, no name field (enforced by CapArg structure)
pub fn validate_cap_args(cap: &Cap) -> Result<(), ValidationError> {
    let cap_urn = cap.urn_string();
    let args = cap.get_args();

    // RULE1: No duplicate media_urns
    let mut media_urns = HashSet::new();
    for arg in args {
        if !media_urns.insert(&arg.media_urn) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE1: Duplicate media_urn '{}'", arg.media_urn),
            });
        }
    }

    // RULE2: sources must not be null or empty
    for arg in args {
        if arg.sources.is_empty() {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE2: Argument '{}' has empty sources", arg.media_urn),
            });
        }
    }

    // Collect stdin URNs, positions, and cli_flags for cross-arg validation
    let mut stdin_urns: Vec<String> = Vec::new();
    let mut positions: Vec<(usize, String)> = Vec::new();
    let mut cli_flags: Vec<(String, String)> = Vec::new();

    for arg in args {
        let mut source_types = HashSet::new();
        let mut has_position = false;
        let mut has_cli_flag = false;

        for source in &arg.sources {
            let source_type = source.get_type();

            // RULE4: No arg may specify same source type more than once
            if !source_types.insert(source_type) {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn,
                    issue: format!("RULE4: Argument '{}' has duplicate source type '{}'", arg.media_urn, source_type),
                });
            }

            match source {
                ArgSource::Stdin { stdin } => {
                    stdin_urns.push(stdin.clone());
                }
                ArgSource::Position { position } => {
                    has_position = true;
                    positions.push((*position, arg.media_urn.clone()));
                }
                ArgSource::CliFlag { cli_flag } => {
                    has_cli_flag = true;
                    cli_flags.push((cli_flag.clone(), arg.media_urn.clone()));

                    // RULE10: Reserved cli_flags
                    if RESERVED_CLI_FLAGS.contains(&cli_flag.as_str()) {
                        return Err(ValidationError::InvalidCapSchema {
                            cap_urn,
                            issue: format!("RULE10: Argument '{}' uses reserved cli_flag '{}'", arg.media_urn, cli_flag),
                        });
                    }
                }
            }
        }

        // RULE7: No arg may have both position and cli_flag
        if has_position && has_cli_flag {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE7: Argument '{}' has both position and cli_flag sources", arg.media_urn),
            });
        }
    }

    // RULE3: If multiple args have stdin source, stdin media_urns must be identical
    if stdin_urns.len() > 1 {
        let first_stdin = &stdin_urns[0];
        for stdin in &stdin_urns[1..] {
            if stdin != first_stdin {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn,
                    issue: format!("RULE3: Multiple args have different stdin media_urns: '{}' vs '{}'", first_stdin, stdin),
                });
            }
        }
    }

    // RULE5: No two args may have same position
    let mut position_set = HashSet::new();
    for (position, media_urn) in &positions {
        if !position_set.insert(*position) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE5: Duplicate position {} in argument '{}'", position, media_urn),
            });
        }
    }

    // RULE6: Positions must be sequential (0-based, no gaps when aggregated)
    if !positions.is_empty() {
        let mut sorted_positions = positions.clone();
        sorted_positions.sort_by_key(|(pos, _)| *pos);
        for (i, (position, _)) in sorted_positions.iter().enumerate() {
            if *position != i {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn,
                    issue: format!("RULE6: Position gap - expected {} but found {}", i, position),
                });
            }
        }
    }

    // RULE9: No two args may have same cli_flag
    let mut flag_set = HashSet::new();
    for (flag, media_urn) in &cli_flags {
        if !flag_set.insert(flag) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE9: Duplicate cli_flag '{}' in argument '{}'", flag, media_urn),
            });
        }
    }

    // RULE8: No unknown keys in source objects - enforced by serde(deny_unknown_fields)
    // RULE11: cli_flag used verbatim as specified - enforced by design
    // RULE12: media_urn is the key, no name field - enforced by CapArg structure

    Ok(())
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
        media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = InputValidator::new(schema_registry, media_registry);
        validator.validate_positional_arguments(cap, arguments).await
    }

    /// Validate output against a cap's output schema
    pub async fn validate_output(
        &self,
        cap_urn: &str,
        output: &serde_json::Value,
        schema_registry: Arc<ProfileSchemaRegistry>,
        media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = OutputValidator::new(schema_registry, media_registry);
        validator.validate_output(cap, output).await
    }

    /// Validate a cap definition itself
    pub async fn validate_cap_schema(
        &self,
        cap: &Cap,
        media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    ) -> Result<(), ValidationError> {
        CapValidator::validate_cap(cap, &media_registry).await
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
    use crate::CapUrn;
    use crate::standard::media::{MEDIA_STRING, MEDIA_INTEGER};
    use crate::media::registry::MediaUrnRegistry;
    use serde_json::json;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:form=map";{}"#, tags)
    }

    // Helper to create test registries
    async fn test_registries() -> (Arc<ProfileSchemaRegistry>, Arc<MediaUrnRegistry>) {
        let schema_registry = Arc::new(ProfileSchemaRegistry::new().await.expect("Failed to create schema registry"));
        let media_registry = Arc::new(MediaUrnRegistry::new().await.expect("Failed to create media registry"));
        (schema_registry, media_registry)
    }

    // TEST051: Test input validation succeeds with valid positional argument
    #[tokio::test]
    async fn test051_input_validation_success() {
        let (schema_registry, media_registry) = test_registries().await;
        let validator = InputValidator::new(schema_registry, media_registry);

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let arg = CapArg::new(
            MEDIA_STRING,
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![json!("/path/to/file.txt")];

        assert!(validator.validate_positional_arguments(&cap, &input_args).await.is_ok());
    }

    // TEST052: Test input validation fails with MissingRequiredArgument when required arg missing
    #[tokio::test]
    async fn test052_input_validation_missing_required() {
        let (schema_registry, media_registry) = test_registries().await;
        let validator = InputValidator::new(schema_registry, media_registry);

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let arg = CapArg::new(
            MEDIA_STRING,
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![]; // Missing required argument

        let result = validator.validate_positional_arguments(&cap, &input_args).await;
        assert!(result.is_err());

        if let Err(ValidationError::MissingRequiredArgument { argument_name, .. }) = result {
            assert_eq!(argument_name, MEDIA_STRING);
        } else {
            panic!("Expected MissingRequiredArgument error");
        }
    }

    // TEST053: Test input validation fails with InvalidArgumentType when wrong type provided
    #[tokio::test]
    async fn test053_input_validation_wrong_type() {
        let (schema_registry, media_registry) = test_registries().await;
        let validator = InputValidator::new(schema_registry, media_registry);

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        // Add local schema to media_specs so validation doesn't depend on network
        let integer_spec = crate::media::spec::MediaSpecDef {
            urn: MEDIA_INTEGER.to_string(),
            media_type: "text/plain".to_string(),
            title: "Integer".to_string(),
            profile_uri: Some("https://capns.org/schema/integer".to_string()),
            schema: Some(json!({"type": "integer"})),
            description: Some("Integer value".to_string()),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        cap.set_media_specs(vec![integer_spec]);

        let arg = CapArg::new(
            MEDIA_INTEGER,
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![json!("not_a_number")]; // Wrong type

        let result = validator.validate_positional_arguments(&cap, &input_args).await;
        assert!(result.is_err());

        if let Err(ValidationError::InvalidArgumentType { .. }) = result {
            // Expected
        } else {
            panic!("Expected InvalidArgumentType error");
        }
    }

    // TEST054: XV5 - Test inline media spec redefinition of existing registry spec is detected and rejected
    #[tokio::test]
    async fn test054_xv5_inline_spec_redefinition_detected() {
        // Create a cap that tries to redefine a standard media spec (MEDIA_STRING)
        let (_schema_registry, media_registry) = test_registries().await;

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        // Try to redefine MEDIA_STRING which exists in the registry
        let string_spec = crate::media::spec::MediaSpecDef {
            urn: MEDIA_STRING.to_string(),
            media_type: "text/plain".to_string(),
            title: "My Custom String".to_string(),
            profile_uri: Some("https://example.com/my-string".to_string()),
            schema: None,
            description: Some("Trying to redefine string".to_string()),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        cap.set_media_specs(vec![string_spec]);

        let result = validate_no_inline_media_spec_redefinition(&cap, &media_registry).await;

        // Should fail because MEDIA_STRING is already in the registry
        assert!(result.is_err());
        if let Err(ValidationError::InlineMediaSpecRedefinesRegistry { media_urn, .. }) = result {
            assert_eq!(media_urn, MEDIA_STRING);
        } else {
            panic!("Expected InlineMediaSpecRedefinesRegistry error");
        }
    }

    // TEST055: XV5 - Test new inline media spec (not in registry) is allowed
    #[tokio::test]
    async fn test055_xv5_new_inline_spec_allowed() {
        // Create a cap with a new media spec that doesn't exist in the registry
        let (_schema_registry, media_registry) = test_registries().await;

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let mut cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        // Define a completely new media spec that doesn't exist in registry
        let custom_spec = crate::media::spec::MediaSpecDef {
            // Use a URN that definitely doesn't exist in the standard registry
            urn: "media:my-unique-custom-type-xyz123".to_string(),
            media_type: "application/json".to_string(),
            title: "My Custom Output".to_string(),
            profile_uri: Some("https://example.com/my-custom-output".to_string()),
            schema: Some(json!({"type": "object"})),
            description: Some("A custom output type".to_string()),
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        cap.set_media_specs(vec![custom_spec]);

        let result = validate_no_inline_media_spec_redefinition(&cap, &media_registry).await;

        // Should succeed because the spec doesn't exist in the registry
        assert!(result.is_ok());
    }

    // TEST056: XV5 - Test empty media_specs (no inline specs) passes XV5 validation
    #[tokio::test]
    async fn test056_xv5_empty_media_specs_allowed() {
        // A cap without inline media_specs should pass XV5 validation
        let (_schema_registry, media_registry) = test_registries().await;

        let urn = CapUrn::from_string(&test_urn("type=test;op=cap")).unwrap();
        let cap = Cap::new(urn, "Test Capability".to_string(), "test-command".to_string());

        let result = validate_no_inline_media_spec_redefinition(&cap, &media_registry).await;
        assert!(result.is_ok());
    }

    // =========================================================================
    // validate_cap_args RULE tests (TEST578-TEST590)
    // =========================================================================

    fn make_test_cap_with_args(args: Vec<CapArg>) -> Cap {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "cmd".to_string());
        for arg in args {
            cap.add_arg(arg);
        }
        cap
    }

    // TEST578: RULE1 - duplicate media_urns rejected
    #[test]
    fn test578_rule1_duplicate_media_urns() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Position { position: 0 }]),
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Position { position: 1 }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE1"), "Error should mention RULE1: {}", err);
    }

    // TEST579: RULE2 - empty sources rejected
    #[test]
    fn test579_rule2_empty_sources() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![]), // empty sources
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE2"), "Error should mention RULE2: {}", err);
    }

    // TEST580: RULE3 - multiple stdin sources with different URNs rejected
    #[test]
    fn test580_rule3_different_stdin_urns() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Stdin { stdin: "media:txt;textable".to_string() }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::Stdin { stdin: "media:bytes".to_string() }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE3"), "Error should mention RULE3: {}", err);
    }

    // TEST581: RULE3 - multiple stdin sources with same URN is OK
    #[test]
    fn test581_rule3_same_stdin_urns_ok() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Stdin { stdin: "media:txt;textable".to_string() }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::Stdin { stdin: "media:txt;textable".to_string() }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_ok(), "Same stdin URNs should be allowed: {:?}", result.err());
    }

    // TEST582: RULE4 - duplicate source type in single arg rejected
    #[test]
    fn test582_rule4_duplicate_source_type() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![
                ArgSource::Position { position: 0 },
                ArgSource::Position { position: 1 }, // same source type twice
            ]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE4"), "Error should mention RULE4: {}", err);
    }

    // TEST583: RULE5 - duplicate position across args rejected
    #[test]
    fn test583_rule5_duplicate_position() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Position { position: 0 }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::Position { position: 0 }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE5"), "Error should mention RULE5: {}", err);
    }

    // TEST584: RULE6 - position gap rejected (0, 2 without 1)
    #[test]
    fn test584_rule6_position_gap() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Position { position: 0 }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::Position { position: 2 }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE6"), "Error should mention RULE6: {}", err);
    }

    // TEST585: RULE6 - sequential positions (0, 1, 2) pass
    #[test]
    fn test585_rule6_sequential_ok() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::Position { position: 0 }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::Position { position: 1 }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_ok(), "Sequential positions should pass: {:?}", result.err());
    }

    // TEST586: RULE7 - arg with both position and cli_flag rejected
    #[test]
    fn test586_rule7_position_and_cli_flag() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![
                ArgSource::Position { position: 0 },
                ArgSource::CliFlag { cli_flag: "--file".to_string() },
            ]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE7"), "Error should mention RULE7: {}", err);
    }

    // TEST587: RULE9 - duplicate cli_flag across args rejected
    #[test]
    fn test587_rule9_duplicate_cli_flag() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::CliFlag { cli_flag: "--file".to_string() }]),
            CapArg::new(MEDIA_INTEGER, true, vec![ArgSource::CliFlag { cli_flag: "--file".to_string() }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE9"), "Error should mention RULE9: {}", err);
    }

    // TEST588: RULE10 - reserved cli_flags rejected
    #[test]
    fn test588_rule10_reserved_cli_flags() {
        for &reserved in RESERVED_CLI_FLAGS {
            let cap = make_test_cap_with_args(vec![
                CapArg::new(MEDIA_STRING, true, vec![ArgSource::CliFlag { cli_flag: reserved.to_string() }]),
            ]);
            let result = validate_cap_args(&cap);
            assert!(result.is_err(), "Reserved flag '{}' should be rejected", reserved);
            let err = format!("{}", result.unwrap_err());
            assert!(err.contains("RULE10"), "Error for '{}' should mention RULE10: {}", reserved, err);
        }
    }

    // TEST589: valid cap args with mixed sources pass all rules
    #[test]
    fn test589_all_rules_pass() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![
                ArgSource::Position { position: 0 },
                ArgSource::Stdin { stdin: "media:txt;textable".to_string() },
            ]),
            CapArg::new(MEDIA_INTEGER, false, vec![
                ArgSource::Position { position: 1 },
            ]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_ok(), "Valid cap args should pass: {:?}", result.err());
    }

    // TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions)
    #[test]
    fn test590_cli_flag_only_args() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, true, vec![ArgSource::CliFlag { cli_flag: "--input".to_string() }]),
            CapArg::new(MEDIA_INTEGER, false, vec![ArgSource::CliFlag { cli_flag: "--count".to_string() }]),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_ok(), "CLI-flag-only args should pass: {:?}", result.err());
    }
}

/// Validate cap arguments against canonical definition
pub async fn validate_cap_arguments(
    registry: &crate::cap::registry::CapRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    cap_urn: &str,
    arguments: &[Value],
) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    let validator = InputValidator::new(schema_registry, media_registry);
    validator.validate_positional_arguments(&canonical_cap, arguments).await
}

/// Validate cap output against canonical definition
pub async fn validate_cap_output(
    registry: &crate::cap::registry::CapRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    media_registry: Arc<crate::media::registry::MediaUrnRegistry>,
    cap_urn: &str,
    output: &Value,
) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    let validator = OutputValidator::new(schema_registry, media_registry);
    validator.validate_output(&canonical_cap, output).await
}

/// Validate that a local cap matches its canonical definition
pub async fn validate_cap_canonical(registry: &crate::cap::registry::CapRegistry, cap: &Cap) -> Result<(), ValidationError> {
    registry.validate_cap(cap).await
        .map_err(|e| ValidationError::InvalidCapSchema {
            cap_urn: cap.urn_string(),
            issue: e.to_string(),
        })
}

/// Validate that all media URN references in a cap definition are resolvable
///
/// This function checks:
/// - The 'in' tag media URN (unless '*')
/// - The 'out' tag media URN (unless '*')
/// - Every argument's media_urn
/// - Every output's media_urn
///
/// Resolution order:
/// 1. Cap's local media_specs table (cap-specific overrides)
/// 2. Registry's local cache (bundled standard specs)
/// 3. Online registry fetch (with graceful degradation if unreachable)
pub async fn validate_media_urn_references(
    cap: &Cap,
    registry: &crate::media::registry::MediaUrnRegistry,
) -> Result<(), ValidationError> {
    let cap_urn = cap.urn_string();

    // Collect all media URNs to validate
    let mut urns_to_check: Vec<(String, String)> = Vec::new();

    // Check in/out tags from the cap URN
    let in_spec = cap.urn.in_spec();
    if in_spec != "*" {
        urns_to_check.push(("in tag".to_string(), in_spec.to_string()));
    }

    let out_spec = cap.urn.out_spec();
    if out_spec != "*" {
        urns_to_check.push(("out tag".to_string(), out_spec.to_string()));
    }

    // Check all argument media URNs
    for arg in cap.get_args() {
        urns_to_check.push((format!("argument '{}'", arg.media_urn), arg.media_urn.clone()));
    }

    // Check output media URN
    if let Some(output) = cap.get_output() {
        urns_to_check.push(("output".to_string(), output.media_urn.clone()));
    }

    // Validate each media URN using the single resolution path
    for (location, media_urn) in urns_to_check {
        if let Err(_) = crate::media::spec::resolve_media_urn(&media_urn, Some(cap.get_media_specs()), registry).await {
            return Err(ValidationError::UnresolvableMediaUrn {
                cap_urn: cap_urn.clone(),
                media_urn,
                location: location.to_string(),
            });
        }
    }

    Ok(())
}

/// XV5: Validate that inline media_specs don't redefine existing registry specs.
///
/// Behavior:
/// - With network access: strictly enforced - fail if any inline spec exists in registry
/// - Without network access: check against cached/bundled specs only.
///   If conflict found with cached specs: fail.
///   If no conflict with cached but registry unreachable: log warning, allow operation.
///
/// Resolution order for checking:
/// 1. Registry's in-memory cache (fast)
/// 2. Registry's disk cache (bundled standard specs)
/// 3. Online registry fetch (if reachable)
pub async fn validate_no_inline_media_spec_redefinition(
    cap: &Cap,
    registry: &crate::media::registry::MediaUrnRegistry,
) -> Result<(), ValidationError> {
    use crate::media::registry::MediaRegistryError;

    let cap_urn = cap.urn_string();
    let inline_specs = cap.get_media_specs();

    if inline_specs.is_empty() {
        return Ok(());
    }

    for spec in inline_specs {
        let media_urn = &spec.urn;
        // Check if this media URN exists in the registry (without using cap's local specs)
        match registry.get_media_spec(media_urn).await {
            Ok(_) => {
                // Found in registry - this is a redefinition, which is an error
                return Err(ValidationError::InlineMediaSpecRedefinesRegistry {
                    cap_urn: cap_urn.clone(),
                    media_urn: media_urn.clone(),
                });
            }
            Err(MediaRegistryError::NotFound(_)) => {
                // Not found in registry (cache + online) - this is fine, it's a new spec
                continue;
            }
            Err(MediaRegistryError::HttpError(e)) => {
                // Network error - check if we can verify against cached/bundled specs
                if let Some(_) = registry.get_cached_spec(media_urn) {
                    // Found in cache - this is a redefinition, fail hard
                    return Err(ValidationError::InlineMediaSpecRedefinesRegistry {
                        cap_urn: cap_urn.clone(),
                        media_urn: media_urn.clone(),
                    });
                }

                // Not found in cache, and online registry unreachable
                // Log warning and allow operation (graceful degradation)
                eprintln!(
                    "[WARN] XV5: Could not verify inline spec '{}' against online registry ({}). Allowing operation in offline mode.",
                    media_urn, e
                );
                continue;
            }
            Err(e) => {
                // Other errors (cache error, parse error) - treat as unable to verify
                if let Some(_) = registry.get_cached_spec(media_urn) {
                    return Err(ValidationError::InlineMediaSpecRedefinesRegistry {
                        cap_urn: cap_urn.clone(),
                        media_urn: media_urn.clone(),
                    });
                }

                eprintln!(
                    "[WARN] XV5: Could not verify inline spec '{}' against registry ({}). Allowing operation.",
                    media_urn, e
                );
                continue;
            }
        }
    }

    Ok(())
}

