//! Formal capability definition
//!
//! This module defines the structure for formal capability definitions that include
//! the capability identifier, versioning, and metadata. Capabilities are general-purpose
//! and do not assume any specific domain like files or documents.

use crate::capability_id::CapabilityId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Argument type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ArgumentType {
    String,
    Integer,
    Number,
    Boolean,
    Array,
    Object,
    Binary,
}

/// Argument validation rules
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArgumentValidation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<String>>,
}

/// Capability argument definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityArgument {
    pub name: String,
    
    #[serde(rename = "type")]
    pub arg_type: ArgumentType,
    
    pub description: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
    
    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

impl ArgumentValidation {
    fn is_empty(&self) -> bool {
        self.min.is_none() &&
        self.max.is_none() &&
        self.min_length.is_none() &&
        self.max_length.is_none() &&
        self.pattern.is_none() &&
        self.allowed_values.is_none()
    }
}

/// Capability arguments collection
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CapabilityArguments {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required: Vec<CapabilityArgument>,
    
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub optional: Vec<CapabilityArgument>,
}


/// Output type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OutputType {
    String,
    Integer,
    Number,
    Boolean,
    Array,
    Object,
    Binary,
}

/// Output definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityOutput {
    #[serde(rename = "type")]
    pub output_type: OutputType,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    
    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,
    
    pub description: String,
}

impl CapabilityOutput {
    /// Create a new output definition
    pub fn new(output_type: OutputType, description: String) -> Self {
        Self {
            output_type,
            description,
            schema_ref: None,
            content_type: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with content type
    pub fn with_content_type(output_type: OutputType, description: String, content_type: String) -> Self {
        Self {
            output_type,
            description,
            content_type: Some(content_type),
            schema_ref: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with schema reference
    pub fn with_schema(output_type: OutputType, description: String, schema_ref: String) -> Self {
        Self {
            output_type,
            description,
            schema_ref: Some(schema_ref),
            content_type: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with validation
    pub fn with_validation(output_type: OutputType, description: String, validation: ArgumentValidation) -> Self {
        Self {
            output_type,
            description,
            validation,
            schema_ref: None,
            content_type: None,
        }
    }
    
    /// Create a fully specified output
    pub fn with_full_definition(
        output_type: OutputType,
        description: String,
        schema_ref: Option<String>,
        content_type: Option<String>,
        validation: ArgumentValidation,
    ) -> Self {
        Self {
            output_type,
            description,
            schema_ref,
            content_type,
            validation,
        }
    }
}

/// Formal capability definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    /// Formal capability identifier with hierarchical naming
    pub id: CapabilityId,
    
    /// Capability version
    pub version: String,
    
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    
    /// Optional metadata as key-value pairs
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
    
    /// Command string for CLI execution
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    
    /// Capability arguments
    #[serde(skip_serializing_if = "CapabilityArguments::is_empty", default)]
    pub arguments: CapabilityArguments,
    
    /// Output definition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<CapabilityOutput>,
}

impl CapabilityArgument {
    /// Create a new capability argument
    pub fn new(name: String, arg_type: ArgumentType, description: String) -> Self {
        Self {
            name,
            arg_type,
            description,
            command: None,
            position: None,
            validation: ArgumentValidation::default(),
            default: None,
        }
    }
    
    /// Create argument with CLI flag
    pub fn with_command(name: String, arg_type: ArgumentType, description: String, command: String) -> Self {
        Self {
            name,
            arg_type,
            description,
            command: Some(command),
            position: None,
            validation: ArgumentValidation::default(),
            default: None,
        }
    }
    
    /// Create argument with position
    pub fn with_position(name: String, arg_type: ArgumentType, description: String, position: usize) -> Self {
        Self {
            name,
            arg_type,
            description,
            command: None,
            position: Some(position),
            validation: ArgumentValidation::default(),
            default: None,
        }
    }
    
    /// Create argument with validation
    pub fn with_validation(name: String, arg_type: ArgumentType, description: String, validation: ArgumentValidation) -> Self {
        Self {
            name,
            arg_type,
            description,
            command: None,
            position: None,
            validation,
            default: None,
        }
    }
    
    /// Create argument with default value
    pub fn with_default(name: String, arg_type: ArgumentType, description: String, default: serde_json::Value) -> Self {
        Self {
            name,
            arg_type,
            description,
            command: None,
            position: None,
            validation: ArgumentValidation::default(),
            default: Some(default),
        }
    }
    
    /// Create a fully specified argument
    pub fn with_full_definition(
        name: String,
        arg_type: ArgumentType,
        description: String,
        command: Option<String>,
        position: Option<usize>,
        validation: ArgumentValidation,
        default: Option<serde_json::Value>,
    ) -> Self {
        Self {
            name,
            arg_type,
            description,
            command,
            position,
            validation,
            default,
        }
    }
}

impl ArgumentValidation {
    /// Create validation with min/max numeric constraints
    pub fn numeric_range(min: Option<f64>, max: Option<f64>) -> Self {
        Self {
            min,
            max,
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
        }
    }
    
    /// Create validation with string length constraints
    pub fn string_length(min_length: Option<usize>, max_length: Option<usize>) -> Self {
        Self {
            min: None,
            max: None,
            min_length,
            max_length,
            pattern: None,
            allowed_values: None,
        }
    }
    
    /// Create validation with pattern
    pub fn pattern(pattern: String) -> Self {
        Self {
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: Some(pattern),
            allowed_values: None,
        }
    }
    
    /// Create validation with allowed values
    pub fn allowed_values(values: Vec<String>) -> Self {
        Self {
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: Some(values),
        }
    }
}

impl CapabilityArguments {
    pub fn new() -> Self {
        Self {
            required: Vec::new(),
            optional: Vec::new(),
        }
    }
    
    pub fn is_empty(&self) -> bool {
        self.required.is_empty() && self.optional.is_empty()
    }
    
    pub fn add_required(&mut self, arg: CapabilityArgument) {
        self.required.push(arg);
    }
    
    pub fn add_optional(&mut self, arg: CapabilityArgument) {
        self.optional.push(arg);
    }
    
    pub fn find_argument(&self, name: &str) -> Option<&CapabilityArgument> {
        self.required.iter().find(|arg| arg.name == name)
            .or_else(|| self.optional.iter().find(|arg| arg.name == name))
    }
    
    pub fn get_positional_args(&self) -> Vec<&CapabilityArgument> {
        let mut args: Vec<&CapabilityArgument> = self.required.iter()
            .chain(self.optional.iter())
            .filter(|arg| arg.position.is_some())
            .collect();
        args.sort_by_key(|arg| arg.position.unwrap());
        args
    }
    
    pub fn get_flag_args(&self) -> Vec<&CapabilityArgument> {
        self.required.iter()
            .chain(self.optional.iter())
            .filter(|arg| arg.command.is_some())
            .collect()
    }
}

impl Capability {
    /// Create a new capability
    pub fn new(id: CapabilityId, version: String) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata: HashMap::new(),
            command: None,
            arguments: CapabilityArguments::new(),
            output: None,
        }
    }

    /// Create a new capability with description
    pub fn with_description(id: CapabilityId, version: String, description: String) -> Self {
        Self {
            id,
            version,
            description: Some(description),
            metadata: HashMap::new(),
            command: None,
            arguments: CapabilityArguments::new(),
            output: None,
        }
    }

    /// Create a new capability with metadata
    pub fn with_metadata(
        id: CapabilityId, 
        version: String, 
        metadata: HashMap<String, String>
    ) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata,
            command: None,
            arguments: CapabilityArguments::new(),
            output: None,
        }
    }

    /// Create a new capability with description and metadata
    pub fn with_description_and_metadata(
        id: CapabilityId,
        version: String,
        description: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            version,
            description: Some(description),
            metadata,
            command: None,
            arguments: CapabilityArguments::new(),
            output: None,
        }
    }
    
    /// Create a new capability with arguments
    pub fn with_arguments(
        id: CapabilityId,
        version: String,
        arguments: CapabilityArguments,
    ) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata: HashMap::new(),
            command: None,
            arguments,
            output: None,
        }
    }
    
    /// Create a new capability with command
    pub fn with_command(
        id: CapabilityId,
        version: String,
        command: String,
    ) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata: HashMap::new(),
            command: Some(command),
            arguments: CapabilityArguments::new(),
            output: None,
        }
    }
    
    /// Create a fully specified capability
    pub fn with_full_definition(
        id: CapabilityId,
        version: String,
        description: Option<String>,
        metadata: HashMap<String, String>,
        command: Option<String>,
        arguments: CapabilityArguments,
        output: Option<CapabilityOutput>,
    ) -> Self {
        Self {
            id,
            version,
            description,
            metadata,
            command,
            arguments,
            output,
        }
    }
    
    /// Check if this capability matches a request string
    pub fn matches_request(&self, request: &str) -> bool {
        let request_id = CapabilityId::from_string(request).expect("Invalid capability identifier in request");
        self.id.can_handle(&request_id)
    }

    /// Get the capability identifier as a string
    pub fn id_string(&self) -> String {
        self.id.to_string()
    }

    /// Check if this capability is more specific than another for the same request
    pub fn is_more_specific_than(&self, other: &Capability, request: &str) -> bool {
        if !self.matches_request(request) || !other.matches_request(request) {
            return false;
        }
        self.id.is_more_specific_than(&other.id)
    }

    /// Get a metadata value by key
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Set a metadata value
    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Remove a metadata value
    pub fn remove_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }

    /// Check if this capability has specific metadata
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }
    
    /// Get the command if defined
    pub fn get_command(&self) -> Option<&String> {
        self.command.as_ref()
    }
    
    /// Set the command
    pub fn set_command(&mut self, command: String) {
        self.command = Some(command);
    }
    
    /// Get the arguments
    pub fn get_arguments(&self) -> &CapabilityArguments {
        &self.arguments
    }
    
    /// Set the arguments
    pub fn set_arguments(&mut self, arguments: CapabilityArguments) {
        self.arguments = arguments;
    }
    
    /// Add a required argument
    pub fn add_required_argument(&mut self, arg: CapabilityArgument) {
        self.arguments.add_required(arg);
    }
    
    /// Add an optional argument
    pub fn add_optional_argument(&mut self, arg: CapabilityArgument) {
        self.arguments.add_optional(arg);
    }
    
    /// Get the output definition if defined
    pub fn get_output(&self) -> Option<&CapabilityOutput> {
        self.output.as_ref()
    }
    
    /// Set the output definition
    pub fn set_output(&mut self, output: CapabilityOutput) {
        self.output = Some(output);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_creation() {
        let id = CapabilityId::from_string("data_processing:transform:json").unwrap();
        let cap = Capability::new(id, "1.0.0".to_string());
        
        assert_eq!(cap.id_string(), "data_processing:transform:json");
        assert_eq!(cap.version, "1.0.0");
        assert!(cap.metadata.is_empty());
    }

    #[test]
    fn test_capability_with_metadata() {
        let id = CapabilityId::from_string("compute:math:arithmetic").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("precision".to_string(), "double".to_string());
        metadata.insert("operations".to_string(), "add,subtract,multiply,divide".to_string());
        
        let cap = Capability::with_metadata(id, "2.1.0".to_string(), metadata);
        
        assert_eq!(cap.get_metadata("precision"), Some(&"double".to_string()));
        assert_eq!(cap.get_metadata("operations"), Some(&"add,subtract,multiply,divide".to_string()));
        assert!(cap.has_metadata("precision"));
        assert!(!cap.has_metadata("nonexistent"));
    }

    #[test]
    fn test_capability_matching() {
        let id = CapabilityId::from_string("data_processing:transform:json").unwrap();
        let cap = Capability::new(id, "1.0.0".to_string());
        
        assert!(cap.matches_request("data_processing:transform:json"));
        assert!(cap.matches_request("data_processing:transform:*"));
        assert!(cap.matches_request("data_processing:*"));
        assert!(!cap.matches_request("compute:*"));
    }

}