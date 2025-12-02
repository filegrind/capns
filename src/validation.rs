//! Cap schema validation infrastructure
//!
//! This module provides strict validation of inputs and outputs against
//! cap schemas, ensuring adherence to advertised specifications.

use crate::{Cap, CapArgument, CapOutput, ArgumentType, OutputType};
use serde_json::Value;
use std::fmt;

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
    /// Invalid argument type
    InvalidArgumentType {
        cap_urn: String,
        argument_name: String,
        expected_type: ArgumentType,
        actual_type: String,
        actual_value: Value,
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
        expected_type: OutputType,
        actual_type: String,
        actual_value: Value,
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
            ValidationError::InvalidArgumentType { cap_urn, argument_name, expected_type, actual_type, actual_value } => {
                write!(f, "Cap '{}' argument '{}' expects type '{:?}' but received '{}' with value: {}", 
                       cap_urn, argument_name, expected_type, actual_type, actual_value)
            }
            ValidationError::ArgumentValidationFailed { cap_urn, argument_name, validation_rule, actual_value } => {
                write!(f, "Cap '{}' argument '{}' failed validation rule '{}' with value: {}", 
                       cap_urn, argument_name, validation_rule, actual_value)
            }
            ValidationError::InvalidOutputType { cap_urn, expected_type, actual_type, actual_value } => {
                write!(f, "Cap '{}' output expects type '{:?}' but received '{}' with value: {}", 
                       cap_urn, expected_type, actual_type, actual_value)
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
        }
    }
}

impl std::error::Error for ValidationError {}

/// Input argument validator
pub struct InputValidator;

impl InputValidator {
    /// Validate arguments against cap input schema
    pub fn validate_arguments(
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
            
            Self::validate_single_argument(cap, req_arg, &arguments[index])?;
        }
        
        // Validate optional arguments if provided
        let required_count = args.required.len();
        for (index, opt_arg) in args.optional.iter().enumerate() {
            let arg_index = required_count + index;
            if arg_index < arguments.len() {
                Self::validate_single_argument(cap, opt_arg, &arguments[arg_index])?;
            }
        }
        
        Ok(())
    }
    
    fn validate_single_argument(
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Type validation
        Self::validate_argument_type(cap, arg_def, value)?;
        
        // Validation rules
        Self::validate_argument_rules(cap, arg_def, value)?;
        
        // Schema validation for structured types
        Self::validate_argument_schema(cap, arg_def, value)?;
        
        Ok(())
    }
    
    fn validate_argument_type(
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let actual_type = Self::get_json_type_name(value);
        
        let type_matches = match (&arg_def.arg_type, value) {
            (ArgumentType::String, Value::String(_)) => true,
            (ArgumentType::Integer, Value::Number(n)) => n.is_i64(),
            (ArgumentType::Number, Value::Number(_)) => true,
            (ArgumentType::Boolean, Value::Bool(_)) => true,
            (ArgumentType::Array, Value::Array(_)) => true,
            (ArgumentType::Object, Value::Object(_)) => true,
            (ArgumentType::Binary, Value::String(_)) => true, // Binary as base64 string
            _ => false,
        };
        
        if !type_matches {
            return Err(ValidationError::InvalidArgumentType {
                cap_urn,
                argument_name: arg_def.name.clone(),
                expected_type: arg_def.arg_type.clone(),
                actual_type,
                actual_value: value.clone(),
            });
        }
        
        Ok(())
    }
    
    fn validate_argument_rules(
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
    
    fn validate_argument_schema(
        cap: &Cap,
        arg_def: &CapArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Only validate structured types that have schemas
        if !matches!(arg_def.arg_type, ArgumentType::Object | ArgumentType::Array) {
            return Ok(());
        }
        
        // Skip if no schema is defined
        if arg_def.schema.is_none() && arg_def.schema_ref.is_none() {
            return Ok(());
        }
        
        // Use the schema validation module
        let mut schema_validator = crate::schema_validation::SchemaValidator::new();
        
        match schema_validator.validate_argument(arg_def, value) {
            Ok(()) => Ok(()),
            Err(crate::schema_validation::SchemaValidationError::ArgumentValidation { details, .. }) => {
                Err(ValidationError::SchemaValidationFailed {
                    cap_urn: cap.urn_string(),
                    field_name: arg_def.name.clone(),
                    schema_errors: details,
                })
            }
            Err(err) => {
                Err(ValidationError::SchemaValidationFailed {
                    cap_urn: cap.urn_string(),
                    field_name: arg_def.name.clone(),
                    schema_errors: err.to_string(),
                })
            }
        }
    }
    
    fn get_json_type_name(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(_) => "boolean".to_string(),
            Value::Number(n) => {
                if n.is_i64() {
                    "integer".to_string()
                } else {
                    "number".to_string()
                }
            }
            Value::String(_) => "string".to_string(),
            Value::Array(_) => "array".to_string(),
            Value::Object(_) => "object".to_string(),
        }
    }
}

/// Output validator
pub struct OutputValidator;

impl OutputValidator {
    /// Validate output against cap output schema
    pub fn validate_output(
        cap: &Cap,
        output: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        
        let output_def = cap.get_output()
            .ok_or_else(|| ValidationError::InvalidCapSchema {
                cap_urn: cap_urn.clone(),
                issue: "No output definition specified".to_string(),
            })?;
        
        // Type validation
        Self::validate_output_type(cap, output_def, output)?;
        
        // Validation rules
        Self::validate_output_rules(cap, output_def, output)?;
        
        // Schema validation for structured outputs
        Self::validate_output_schema(cap, output_def, output)?;
        
        Ok(())
    }
    
    fn validate_output_type(
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let actual_type = InputValidator::get_json_type_name(value);
        
        let type_matches = match (&output_def.output_type, value) {
            (OutputType::String, Value::String(_)) => true,
            (OutputType::Integer, Value::Number(n)) => n.is_i64(),
            (OutputType::Number, Value::Number(_)) => true,
            (OutputType::Boolean, Value::Bool(_)) => true,
            (OutputType::Array, Value::Array(_)) => true,
            (OutputType::Object, Value::Object(_)) => true,
            (OutputType::Binary, Value::String(_)) => true, // Binary as base64 string
            _ => false,
        };
        
        if !type_matches {
            return Err(ValidationError::InvalidOutputType {
                cap_urn,
                expected_type: output_def.output_type.clone(),
                actual_type,
                actual_value: value.clone(),
            });
        }
        
        Ok(())
    }
    
    fn validate_output_rules(
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
    
    fn validate_output_schema(
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Only validate structured types that have schemas
        if !matches!(output_def.output_type, OutputType::Object | OutputType::Array) {
            return Ok(());
        }
        
        // Skip if no schema is defined
        if output_def.schema.is_none() && output_def.schema_ref.is_none() {
            return Ok(());
        }
        
        // Use the schema validation module
        let mut schema_validator = crate::schema_validation::SchemaValidator::new();
        
        match schema_validator.validate_output(output_def, value) {
            Ok(()) => Ok(()),
            Err(crate::schema_validation::SchemaValidationError::OutputValidation { details }) => {
                Err(ValidationError::SchemaValidationFailed {
                    cap_urn: cap.urn_string(),
                    field_name: "output".to_string(),
                    schema_errors: details,
                })
            }
            Err(err) => {
                Err(ValidationError::SchemaValidationFailed {
                    cap_urn: cap.urn_string(),
                    field_name: "output".to_string(),
                    schema_errors: err.to_string(),
                })
            }
        }
    }
}

/// Cap schema validator
pub struct CapValidator;

impl CapValidator {
    /// Validate a cap definition itself
    pub fn validate_cap(cap: &Cap) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        
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
    pub fn validate_inputs(
        &self,
        cap_urn: &str,
        arguments: &[serde_json::Value],
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        InputValidator::validate_arguments(cap, arguments)
    }

    /// Validate output against a cap's output schema
    pub fn validate_output(
        &self,
        cap_urn: &str,
        output: &serde_json::Value,
    ) -> Result<(), ValidationError> {
        let cap = self.get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        OutputValidator::validate_output(cap, output)
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
    use serde_json::json;

    #[test]
    fn test_input_validation_success() {
        let urn = CapUrn::from_string("cap:type=test;action=cap").unwrap();
        let mut cap = Cap::new(urn, "test-command".to_string());
        
        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "file_path".to_string(),
            ArgumentType::String,
            "Path to file".to_string(),
            "--file".to_string(),
        ));
        
        cap.set_arguments(args);
        
        let input_args = vec![json!("/path/to/file.txt")];
        
        assert!(InputValidator::validate_arguments(&cap, &input_args).is_ok());
    }
    
    #[test]
    fn test_input_validation_missing_required() {
        let urn = CapUrn::from_string("cap:type=test;action=cap").unwrap();
        let mut cap = Cap::new(urn, "test-command".to_string());
        
        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "file_path".to_string(),
            ArgumentType::String,
            "Path to file".to_string(),
            "--file".to_string(),
        ));
        
        cap.set_arguments(args);
        
        let input_args = vec![]; // Missing required argument
        
        let result = InputValidator::validate_arguments(&cap, &input_args);
        assert!(result.is_err());
        
        if let Err(ValidationError::MissingRequiredArgument { argument_name, .. }) = result {
            assert_eq!(argument_name, "file_path");
        } else {
            panic!("Expected MissingRequiredArgument error");
        }
    }
    
    #[test]
    fn test_input_validation_wrong_type() {
        let urn = CapUrn::from_string("cap:type=test;action=cap").unwrap();
        let mut cap = Cap::new(urn, "test-command".to_string());
        
        let mut args = CapArguments::new();
        args.add_required(CapArgument::new(
            "width".to_string(),
            ArgumentType::Integer,
            "Width value".to_string(),
            "--width".to_string(),
        ));
        
        cap.set_arguments(args);
        
        let input_args = vec![json!("not_a_number")]; // Wrong type
        
        let result = InputValidator::validate_arguments(&cap, &input_args);
        assert!(result.is_err());
        
        if let Err(ValidationError::InvalidArgumentType { expected_type, .. }) = result {
            assert_eq!(expected_type, ArgumentType::Integer);
        } else {
            panic!("Expected InvalidArgumentType error");
        }
    }
}

/// Validate cap arguments against canonical definition
pub async fn validate_cap_arguments(registry: &crate::registry::CapRegistry, cap_urn: &str, arguments: &[Value]) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    InputValidator::validate_arguments(&canonical_cap, arguments)
}

/// Validate cap output against canonical definition
pub async fn validate_cap_output(registry: &crate::registry::CapRegistry, cap_urn: &str, output: &Value) -> Result<(), ValidationError> {
    let canonical_cap = registry.get_cap(cap_urn).await
        .map_err(|_| ValidationError::UnknownCap { cap_urn: cap_urn.to_string() })?;
    OutputValidator::validate_output(&canonical_cap, output)
}

/// Validate that a local cap matches its canonical definition
pub async fn validate_cap_canonical(registry: &crate::registry::CapRegistry, cap: &Cap) -> Result<(), ValidationError> {
    registry.validate_cap(cap).await
        .map_err(|e| ValidationError::InvalidCapSchema {
            cap_urn: cap.urn_string(),
            issue: e.to_string(),
        })
}