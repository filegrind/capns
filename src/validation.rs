//! Capability schema validation infrastructure
//!
//! This module provides strict validation of inputs and outputs against
//! capability schemas, ensuring adherence to advertised specifications.

use crate::{Capability, CapabilityArgument, CapabilityOutput, ArgumentType, OutputType, ArgumentValidation};
use serde_json::Value;
use std::fmt;

/// Validation error types with descriptive failure information
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Unknown capability requested
    UnknownCapability {
        capability_key: String,
    },
    /// Missing required argument
    MissingRequiredArgument {
        capability_key: String,
        argument_name: String,
    },
    /// Invalid argument type
    InvalidArgumentType {
        capability_key: String,
        argument_name: String,
        expected_type: ArgumentType,
        actual_type: String,
        actual_value: Value,
    },
    /// Argument validation rule violation
    ArgumentValidationFailed {
        capability_key: String,
        argument_name: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Invalid output type
    InvalidOutputType {
        capability_key: String,
        expected_type: OutputType,
        actual_type: String,
        actual_value: Value,
    },
    /// Output validation rule violation
    OutputValidationFailed {
        capability_key: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Malformed capability schema
    InvalidCapabilitySchema {
        capability_key: String,
        issue: String,
    },
    /// Too many arguments provided
    TooManyArguments {
        capability_key: String,
        max_expected: usize,
        actual_count: usize,
    },
    /// JSON parsing error
    JsonParseError {
        capability_key: String,
        error: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::UnknownCapability { capability_key } => {
                write!(f, "Unknown capability '{}' - capability not registered or advertised", capability_key)
            }
            ValidationError::MissingRequiredArgument { capability_key, argument_name } => {
                write!(f, "Capability '{}' requires argument '{}' but it was not provided", capability_key, argument_name)
            }
            ValidationError::InvalidArgumentType { capability_key, argument_name, expected_type, actual_type, actual_value } => {
                write!(f, "Capability '{}' argument '{}' expects type '{:?}' but received '{}' with value: {}", 
                       capability_key, argument_name, expected_type, actual_type, actual_value)
            }
            ValidationError::ArgumentValidationFailed { capability_key, argument_name, validation_rule, actual_value } => {
                write!(f, "Capability '{}' argument '{}' failed validation rule '{}' with value: {}", 
                       capability_key, argument_name, validation_rule, actual_value)
            }
            ValidationError::InvalidOutputType { capability_key, expected_type, actual_type, actual_value } => {
                write!(f, "Capability '{}' output expects type '{:?}' but received '{}' with value: {}", 
                       capability_key, expected_type, actual_type, actual_value)
            }
            ValidationError::OutputValidationFailed { capability_key, validation_rule, actual_value } => {
                write!(f, "Capability '{}' output failed validation rule '{}' with value: {}", 
                       capability_key, validation_rule, actual_value)
            }
            ValidationError::InvalidCapabilitySchema { capability_key, issue } => {
                write!(f, "Capability '{}' has invalid schema: {}", capability_key, issue)
            }
            ValidationError::TooManyArguments { capability_key, max_expected, actual_count } => {
                write!(f, "Capability '{}' expects at most {} arguments but received {}", 
                       capability_key, max_expected, actual_count)
            }
            ValidationError::JsonParseError { capability_key, error } => {
                write!(f, "Capability '{}' JSON parsing failed: {}", capability_key, error)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Input argument validator
pub struct InputValidator;

impl InputValidator {
    /// Validate arguments against capability input schema
    pub fn validate_arguments(
        capability: &Capability,
        arguments: &[Value],
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        let args = &capability.arguments;
        
        // Check if too many arguments provided
        let max_args = args.required.len() + args.optional.len();
        if arguments.len() > max_args {
            return Err(ValidationError::TooManyArguments {
                capability_key,
                max_expected: max_args,
                actual_count: arguments.len(),
            });
        }
        
        // Validate required arguments
        for (index, req_arg) in args.required.iter().enumerate() {
            if index >= arguments.len() {
                return Err(ValidationError::MissingRequiredArgument {
                    capability_key: capability_key.clone(),
                    argument_name: req_arg.name.clone(),
                });
            }
            
            Self::validate_single_argument(capability, req_arg, &arguments[index])?;
        }
        
        // Validate optional arguments if provided
        let required_count = args.required.len();
        for (index, opt_arg) in args.optional.iter().enumerate() {
            let arg_index = required_count + index;
            if arg_index < arguments.len() {
                Self::validate_single_argument(capability, opt_arg, &arguments[arg_index])?;
            }
        }
        
        Ok(())
    }
    
    fn validate_single_argument(
        capability: &Capability,
        arg_def: &CapabilityArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        
        // Type validation
        Self::validate_argument_type(capability, arg_def, value)?;
        
        // Validation rules
        Self::validate_argument_rules(capability, arg_def, value)?;
        
        Ok(())
    }
    
    fn validate_argument_type(
        capability: &Capability,
        arg_def: &CapabilityArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
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
                capability_key,
                argument_name: arg_def.name.clone(),
                expected_type: arg_def.arg_type.clone(),
                actual_type,
                actual_value: value.clone(),
            });
        }
        
        Ok(())
    }
    
    fn validate_argument_rules(
        capability: &Capability,
        arg_def: &CapabilityArgument,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        let validation = &arg_def.validation;
        
        // Numeric validation
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::ArgumentValidationFailed {
                        capability_key,
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
                        capability_key,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("maximum value {}", max),
                        actual_value: value.clone(),
                    });
                }
            }
        }
        
        // String length validation
        if let Some(min_length) = validation.min_length {
            if let Some(s) = value.as_str() {
                if s.len() < min_length {
                    return Err(ValidationError::ArgumentValidationFailed {
                        capability_key,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("minimum length {}", min_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }
        
        if let Some(max_length) = validation.max_length {
            if let Some(s) = value.as_str() {
                if s.len() > max_length {
                    return Err(ValidationError::ArgumentValidationFailed {
                        capability_key,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("maximum length {}", max_length),
                        actual_value: value.clone(),
                    });
                }
            }
        }
        
        // Pattern validation
        if let Some(pattern) = &validation.pattern {
            if let Some(s) = value.as_str() {
                if let Ok(regex) = regex::Regex::new(pattern) {
                    if !regex.is_match(s) {
                        return Err(ValidationError::ArgumentValidationFailed {
                            capability_key,
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
                        capability_key,
                        argument_name: arg_def.name.clone(),
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }
        
        Ok(())
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
    /// Validate output against capability output schema
    pub fn validate_output(
        capability: &Capability,
        output: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        
        let output_def = capability.get_output()
            .ok_or_else(|| ValidationError::InvalidCapabilitySchema {
                capability_key: capability_key.clone(),
                issue: "No output definition specified".to_string(),
            })?;
        
        // Type validation
        Self::validate_output_type(capability, output_def, output)?;
        
        // Validation rules
        Self::validate_output_rules(capability, output_def, output)?;
        
        Ok(())
    }
    
    fn validate_output_type(
        capability: &Capability,
        output_def: &CapabilityOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
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
                capability_key,
                expected_type: output_def.output_type.clone(),
                actual_type,
                actual_value: value.clone(),
            });
        }
        
        Ok(())
    }
    
    fn validate_output_rules(
        capability: &Capability,
        output_def: &CapabilityOutput,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        let validation = &output_def.validation;
        
        // Apply same validation rules as arguments
        if let Some(min) = validation.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    return Err(ValidationError::OutputValidationFailed {
                        capability_key,
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
                        capability_key,
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
                        capability_key,
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
                        capability_key,
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
                            capability_key,
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
                        capability_key,
                        validation_rule: format!("allowed values: {:?}", allowed_values),
                        actual_value: value.clone(),
                    });
                }
            }
        }
        
        Ok(())
    }
}

/// Capability schema validator
pub struct CapabilityValidator;

impl CapabilityValidator {
    /// Validate a capability definition itself
    pub fn validate_capability(capability: &Capability) -> Result<(), ValidationError> {
        let capability_key = capability.id_string();
        
        // Validate that required arguments don't have default values
        for arg in &capability.arguments.required {
            if arg.default.is_some() {
                return Err(ValidationError::InvalidCapabilitySchema {
                    capability_key: capability_key.clone(),
                    issue: format!("Required argument '{}' cannot have a default value", arg.name),
                });
            }
        }
        
        // Validate argument position uniqueness
        let mut positions = std::collections::HashSet::new();
        for arg in capability.arguments.required.iter().chain(capability.arguments.optional.iter()) {
            if let Some(pos) = arg.position {
                if !positions.insert(pos) {
                    return Err(ValidationError::InvalidCapabilitySchema {
                        capability_key: capability_key.clone(),
                        issue: format!("Duplicate argument position {} for argument '{}'", pos, arg.name),
                    });
                }
            }
        }
        
        // Validate CLI flag uniqueness
        let mut cli_flags = std::collections::HashSet::new();
        for arg in capability.arguments.required.iter().chain(capability.arguments.optional.iter()) {
            let flag = &arg.cli_flag;
            if !flag.is_empty() {
                if !cli_flags.insert(flag) {
                    return Err(ValidationError::InvalidCapabilitySchema {
                        capability_key: capability_key.clone(),
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
    capabilities: std::collections::HashMap<String, Capability>,
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self {
            capabilities: std::collections::HashMap::new(),
        }
    }

    /// Register a capability schema for validation
    pub fn register_capability(&mut self, capability: Capability) {
        let id = capability.id_string();
        self.capabilities.insert(id, capability);
    }

    /// Get a capability by ID
    pub fn get_capability(&self, capability_key: &str) -> Option<&Capability> {
        self.capabilities.get(capability_key)
    }

    /// Validate arguments against a capability's input schema
    pub fn validate_inputs(
        &self,
        capability_key: &str,
        arguments: &[serde_json::Value],
    ) -> Result<(), ValidationError> {
        let capability = self.get_capability(capability_key)
            .ok_or_else(|| ValidationError::UnknownCapability {
                capability_key: capability_key.to_string(),
            })?;

        InputValidator::validate_arguments(capability, arguments)
    }

    /// Validate output against a capability's output schema
    pub fn validate_output(
        &self,
        capability_key: &str,
        output: &serde_json::Value,
    ) -> Result<(), ValidationError> {
        let capability = self.get_capability(capability_key)
            .ok_or_else(|| ValidationError::UnknownCapability {
                capability_key: capability_key.to_string(),
            })?;

        OutputValidator::validate_output(capability, output)
    }

    /// Validate a capability definition itself
    pub fn validate_capability_schema(
        &self,
        capability: &Capability,
    ) -> Result<(), ValidationError> {
        CapabilityValidator::validate_capability(capability)
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
    use crate::{CapabilityKey, CapabilityArguments};
    use serde_json::json;

    #[test]
    fn test_input_validation_success() {
        let id = CapabilityKey::from_string("test:capability").unwrap();
        let mut capability = Capability::new(id, "1.0.0".to_string());
        
        let mut args = CapabilityArguments::new();
        args.add_required(CapabilityArgument::new(
            "file_path".to_string(),
            ArgumentType::String,
            "Path to file".to_string(),
        ));
        
        capability.set_arguments(args);
        
        let input_args = vec![json!("/path/to/file.txt")];
        
        assert!(InputValidator::validate_arguments(&capability, &input_args).is_ok());
    }
    
    #[test]
    fn test_input_validation_missing_required() {
        let id = CapabilityKey::from_string("test:capability").unwrap();
        let mut capability = Capability::new(id, "1.0.0".to_string());
        
        let mut args = CapabilityArguments::new();
        args.add_required(CapabilityArgument::new(
            "file_path".to_string(),
            ArgumentType::String,
            "Path to file".to_string(),
        ));
        
        capability.set_arguments(args);
        
        let input_args = vec![]; // Missing required argument
        
        let result = InputValidator::validate_arguments(&capability, &input_args);
        assert!(result.is_err());
        
        if let Err(ValidationError::MissingRequiredArgument { argument_name, .. }) = result {
            assert_eq!(argument_name, "file_path");
        } else {
            panic!("Expected MissingRequiredArgument error");
        }
    }
    
    #[test]
    fn test_input_validation_wrong_type() {
        let id = CapabilityKey::from_string("test:capability").unwrap();
        let mut capability = Capability::new(id, "1.0.0".to_string());
        
        let mut args = CapabilityArguments::new();
        args.add_required(CapabilityArgument::new(
            "width".to_string(),
            ArgumentType::Integer,
            "Width value".to_string(),
        ));
        
        capability.set_arguments(args);
        
        let input_args = vec![json!("not_a_number")]; // Wrong type
        
        let result = InputValidator::validate_arguments(&capability, &input_args);
        assert!(result.is_err());
        
        if let Err(ValidationError::InvalidArgumentType { expected_type, .. }) = result {
            assert_eq!(expected_type, ArgumentType::Integer);
        } else {
            panic!("Expected InvalidArgumentType error");
        }
    }
}