//! Formal cap definition
//!
//! This module defines the structure for formal cap definitions that include
//! the cap URN, versioning, and metadata. Caps are general-purpose
//! and do not assume any specific domain like files or documents.

use crate::cap_urn::CapUrn;
use serde::{Deserialize, Deserializer, Serialize};
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

/// Cap argument definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapArgument {
    pub name: String,
    
    pub arg_type: ArgumentType,
    
    pub arg_description: String,
    
    #[serde(rename = "cli_flag")]
    pub cli_flag: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
    
    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
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

/// Cap arguments collection
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CapArguments {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required: Vec<CapArgument>,
    
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub optional: Vec<CapArgument>,
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
pub struct CapOutput {
    pub output_type: OutputType,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    
    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,
    
    pub output_description: String,
}

impl CapOutput {
    /// Create a new output definition
    pub fn new(output_type: OutputType, description: String) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref: None,
            content_type: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with content type
    pub fn with_content_type(output_type: OutputType, description: String, content_type: String) -> Self {
        Self {
            output_type,
            output_description: description,
            content_type: Some(content_type),
            schema_ref: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with schema reference
    pub fn with_schema(output_type: OutputType, description: String, schema_ref: String) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref: Some(schema_ref),
            content_type: None,
            validation: ArgumentValidation::default(),
        }
    }
    
    /// Create output with validation
    pub fn with_validation(output_type: OutputType, description: String, validation: ArgumentValidation) -> Self {
        Self {
            output_type,
            output_description: description,
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
            output_description: description,
            schema_ref,
            content_type,
            validation,
        }
    }
}

/// Formal cap definition
#[derive(Debug, Clone, Serialize)]
pub struct Cap {
    /// Formal cap URN with hierarchical naming
    pub urn: CapUrn,
    
    
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap_description: Option<String>,
    
    /// Optional metadata as key-value pairs
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
    
    /// Command string for CLI execution
    pub command: String,
    
    /// Cap arguments
    #[serde(skip_serializing_if = "CapArguments::is_empty", default)]
    pub arguments: CapArguments,
    
    /// Output definition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<CapOutput>,
    
    /// Whether this cap accepts input via stdin
    #[serde(default)]
    pub accepts_stdin: bool,
}

// Custom deserializer to handle registry format where urn is an object
impl<'de> Deserialize<'de> for Cap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CapRegistry {
            urn: serde_json::Value,  // Can be either string or object
            cap_description: Option<String>,
            #[serde(default)]
            metadata: HashMap<String, String>,
            command: String,
            #[serde(default)]
            arguments: CapArguments,
            output: Option<CapOutput>,
            #[serde(default)]
            accepts_stdin: bool,
        }

        let registry_cap = CapRegistry::deserialize(deserializer)?;
        
        // Handle urn field - can be string or object with tags
        let urn = match registry_cap.urn {
            serde_json::Value::String(urn_str) => {
                CapUrn::from_string(&urn_str).map_err(serde::de::Error::custom)?
            },
            serde_json::Value::Object(urn_obj) => {
                if let Some(tags) = urn_obj.get("tags").and_then(|t| t.as_object()) {
                    let mut urn_builder = crate::cap_urn::CapUrnBuilder::new();
                    for (key, value) in tags {
                        if let Some(val_str) = value.as_str() {
                            urn_builder = urn_builder.tag(key, val_str);
                        }
                    }
                    urn_builder.build().map_err(serde::de::Error::custom)?
                } else {
                    return Err(serde::de::Error::custom("Invalid urn object format"));
                }
            },
            _ => return Err(serde::de::Error::custom("urn must be string or object")),
        };

        Ok(Cap {
            urn,
            cap_description: registry_cap.cap_description,
            metadata: registry_cap.metadata,
            command: registry_cap.command,
            arguments: registry_cap.arguments,
            output: registry_cap.output,
            accepts_stdin: registry_cap.accepts_stdin,
        })
    }
}

impl CapArgument {
    /// Create a new cap argument
    pub fn new(name: String, arg_type: ArgumentType, description: String, cli_flag: String) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position: None,
            validation: ArgumentValidation::default(),
            default_value: None,
        }
    }
    
    /// Create argument with CLI flag (deprecated - use new() instead)
    pub fn with_cli_flag(name: String, arg_type: ArgumentType, description: String, cli_flag: String) -> Self {
        Self::new(name, arg_type, description, cli_flag)
    }
    
    /// Create argument with position
    pub fn with_position(name: String, arg_type: ArgumentType, description: String, cli_flag: String, position: usize) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position: Some(position),
            validation: ArgumentValidation::default(),
            default_value: None,
        }
    }
    
    /// Create argument with validation
    pub fn with_validation(name: String, arg_type: ArgumentType, description: String, cli_flag: String, validation: ArgumentValidation) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position: None,
            validation,
            default_value: None,
        }
    }
    
    /// Create argument with default value
    pub fn with_default(name: String, arg_type: ArgumentType, description: String, cli_flag: String, default: serde_json::Value) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position: None,
            validation: ArgumentValidation::default(),
            default_value: Some(default),
        }
    }
    
    /// Create a fully specified argument
    pub fn with_full_definition(
        name: String,
        arg_type: ArgumentType,
        description: String,
        cli_flag: String,
        position: Option<usize>,
        validation: ArgumentValidation,
        default: Option<serde_json::Value>,
    ) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position,
            validation,
            default_value: default,
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

impl CapArguments {
    pub fn new() -> Self {
        Self {
            required: Vec::new(),
            optional: Vec::new(),
        }
    }
    
    pub fn is_empty(&self) -> bool {
        self.required.is_empty() && self.optional.is_empty()
    }
    
    pub fn add_required(&mut self, arg: CapArgument) {
        self.required.push(arg);
    }
    
    pub fn add_optional(&mut self, arg: CapArgument) {
        self.optional.push(arg);
    }
    
    pub fn find_argument(&self, name: &str) -> Option<&CapArgument> {
        self.required.iter().find(|arg| arg.name == name)
            .or_else(|| self.optional.iter().find(|arg| arg.name == name))
    }
    
    pub fn get_positional_args(&self) -> Vec<&CapArgument> {
        let mut args: Vec<&CapArgument> = self.required.iter()
            .chain(self.optional.iter())
            .filter(|arg| arg.position.is_some())
            .collect();
        args.sort_by_key(|arg| arg.position.unwrap());
        args
    }
    
    pub fn get_flag_args(&self) -> Vec<&CapArgument> {
        self.required.iter()
            .chain(self.optional.iter())
            .filter(|arg| !arg.cli_flag.is_empty())
            .collect()
    }
}

impl Cap {
    /// Create a new cap
    pub fn new(urn: CapUrn, command: String) -> Self {
        Self {
            urn,
            cap_description: None,
            metadata: HashMap::new(),
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
        }
    }

    /// Create a new cap with description
    pub fn with_description(urn: CapUrn, command: String, description: String) -> Self {
        Self {
            urn,
            cap_description: Some(description),
            metadata: HashMap::new(),
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
        }
    }

    /// Create a new cap with metadata
    pub fn with_metadata(
        urn: CapUrn, 
        command: String,
        metadata: HashMap<String, String>
    ) -> Self {
        Self {
            urn,
            cap_description: None,
            metadata,
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
        }
    }

    /// Create a new cap with description and metadata
    pub fn with_description_and_metadata(
        urn: CapUrn,
        command: String,
        description: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            urn,
            cap_description: Some(description),
            metadata,
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
        }
    }
    
    /// Create a new cap with arguments
    pub fn with_arguments(
        urn: CapUrn,
        command: String,
        arguments: CapArguments,
    ) -> Self {
        Self {
            urn,
            cap_description: None,
            metadata: HashMap::new(),
            command,
            arguments,
            output: None,
            accepts_stdin: false,
        }
    }
    
    /// Create a new cap with command (deprecated - use new() instead)
    pub fn with_command(
        urn: CapUrn,
        command: String,
    ) -> Self {
        Self::new(urn, command)
    }
    
    /// Create a fully specified cap
    pub fn with_full_definition(
        urn: CapUrn,
        description: Option<String>,
        metadata: HashMap<String, String>,
        command: String,
        arguments: CapArguments,
        output: Option<CapOutput>,
    ) -> Self {
        Self {
            urn,
            cap_description: description,
            metadata,
            command,
            arguments,
            output,
            accepts_stdin: false,
        }
    }
    
    /// Check if this cap matches a request string
    pub fn matches_request(&self, request: &str) -> bool {
        let request_urn = CapUrn::from_string(request).expect("Invalid cap URN in request");
        self.urn.can_handle(&request_urn)
    }

    /// Get the cap URN as a string
    pub fn urn_string(&self) -> String {
        self.urn.to_string()
    }

    /// Check if this cap is more specific than another for the same request
    pub fn is_more_specific_than(&self, other: &Cap, request: &str) -> bool {
        if !self.matches_request(request) || !other.matches_request(request) {
            return false;
        }
        self.urn.is_more_specific_than(&other.urn)
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

    /// Check if this cap has specific metadata
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }
    
    /// Get the command
    pub fn get_command(&self) -> &String {
        &self.command
    }
    
    /// Set the command
    pub fn set_command(&mut self, command: String) {
        self.command = command;
    }
    
    /// Get the arguments
    pub fn get_arguments(&self) -> &CapArguments {
        &self.arguments
    }
    
    /// Set the arguments
    pub fn set_arguments(&mut self, arguments: CapArguments) {
        self.arguments = arguments;
    }
    
    /// Add a required argument
    pub fn add_required_argument(&mut self, arg: CapArgument) {
        self.arguments.add_required(arg);
    }
    
    /// Add an optional argument
    pub fn add_optional_argument(&mut self, arg: CapArgument) {
        self.arguments.add_optional(arg);
    }
    
    /// Get the output definition if defined
    pub fn get_output(&self) -> Option<&CapOutput> {
        self.output.as_ref()
    }
    
    /// Set the output definition
    pub fn set_output(&mut self, output: CapOutput) {
        self.output = Some(output);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_creation() {
        let urn = CapUrn::from_string("cap:action=transform;format=json;type=data_processing").unwrap();
        let cap = Cap::new(urn, "test-command".to_string());
        
        assert_eq!(cap.urn_string(), "cap:action=transform;format=json;type=data_processing");
        assert!(cap.metadata.is_empty());
    }

    #[test]
    fn test_cap_with_metadata() {
        let urn = CapUrn::from_string("cap:action=arithmetic;type=compute;subtype=math").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("precision".to_string(), "double".to_string());
        metadata.insert("operations".to_string(), "add,subtract,multiply,divide".to_string());
        
        let cap = Cap::with_metadata(urn, "test-command".to_string(), metadata);
        
        assert_eq!(cap.get_metadata("precision"), Some(&"double".to_string()));
        assert_eq!(cap.get_metadata("operations"), Some(&"add,subtract,multiply,divide".to_string()));
        assert!(cap.has_metadata("precision"));
        assert!(!cap.has_metadata("nonexistent"));
    }

    #[test]
    fn test_cap_matching() {
        let urn = CapUrn::from_string("cap:action=transform;format=json;type=data_processing").unwrap();
        let cap = Cap::new(urn, "test-command".to_string());
        
        assert!(cap.matches_request("cap:action=transform;format=json;type=data_processing"));
        assert!(cap.matches_request("cap:action=transform;format=*;type=data_processing")); // Request wants any format, cap handles json specifically
        assert!(cap.matches_request("cap:type=data_processing")); // Request is subset, cap has all required tags
        assert!(!cap.matches_request("cap:type=compute"));
    }

    #[test]
    fn test_cap_accepts_stdin() {
        let urn = CapUrn::from_string("cap:action=generate;target=embeddings").unwrap();
        let mut cap = Cap::new(urn, "generate".to_string());
        
        // By default, caps should not accept stdin
        assert!(!cap.accepts_stdin);
        
        // Enable stdin support
        cap.accepts_stdin = true;
        assert!(cap.accepts_stdin);
        
        // Test serialization/deserialization preserves the field
        let serialized = serde_json::to_string(&cap).unwrap();
        let deserialized: Cap = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cap.accepts_stdin, deserialized.accepts_stdin);
    }

}