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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    
    /// Reference to external JSON schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    
    /// Embedded JSON schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    
    /// Arbitrary metadata as JSON object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapOutput {
    pub output_type: OutputType,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    
    /// Embedded JSON schema for output validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    
    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,
    
    pub output_description: String,
    
    /// Arbitrary metadata as JSON object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapOutput {
    /// Create a new output definition
    pub fn new(output_type: OutputType, description: String) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref: None,
            schema: None,
            content_type: None,
            validation: ArgumentValidation::default(),
            metadata: None,
        }
    }
    
    /// Create output with content type
    pub fn with_content_type(output_type: OutputType, description: String, content_type: String) -> Self {
        Self {
            output_type,
            output_description: description,
            content_type: Some(content_type),
            schema_ref: None,
            schema: None,
            validation: ArgumentValidation::default(),
            metadata: None,
        }
    }
    
    /// Create output with schema reference
    pub fn with_schema(output_type: OutputType, description: String, schema_ref: String) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref: Some(schema_ref),
            schema: None,
            content_type: None,
            validation: ArgumentValidation::default(),
            metadata: None,
        }
    }
    
    /// Create output with embedded schema
    pub fn with_embedded_schema(output_type: OutputType, description: String, schema: serde_json::Value) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref: None,
            schema: Some(schema),
            content_type: None,
            validation: ArgumentValidation::default(),
            metadata: None,
        }
    }
    
    /// Create output with validation
    pub fn with_validation(output_type: OutputType, description: String, validation: ArgumentValidation) -> Self {
        Self {
            output_type,
            output_description: description,
            validation,
            schema_ref: None,
            schema: None,
            content_type: None,
            metadata: None,
        }
    }
    
    /// Create a fully specified output
    pub fn with_full_definition(
        output_type: OutputType,
        description: String,
        schema_ref: Option<String>,
        schema: Option<serde_json::Value>,
        content_type: Option<String>,
        validation: ArgumentValidation,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            output_type,
            output_description: description,
            schema_ref,
            schema,
            content_type,
            validation,
            metadata,
        }
    }
}

/// Formal cap definition
#[derive(Debug, Clone)]
pub struct Cap {
    /// Formal cap URN with hierarchical naming
    pub urn: CapUrn,
    
    /// Human-readable title of the capability (required)
    pub title: String,
    
    /// Category for visual grouping and display of caps (optional)
    pub category: Option<String>,
    
    /// Optional description
    pub cap_description: Option<String>,
    
    /// Optional metadata as key-value pairs
    pub metadata: HashMap<String, String>,
    
    /// Command string for CLI execution
    pub command: String,
    
    /// Cap arguments
    pub arguments: CapArguments,
    
    /// Output definition
    pub output: Option<CapOutput>,
    
    /// Whether this cap accepts input via stdin
    pub accepts_stdin: bool,
    
    /// Arbitrary metadata as JSON object
    pub metadata_json: Option<serde_json::Value>,
}

// Custom PartialEq implementation that includes all fields
impl PartialEq for Cap {
    fn eq(&self, other: &Self) -> bool {
        self.urn == other.urn &&
        self.title == other.title &&
        self.category == other.category &&
        self.cap_description == other.cap_description &&
        self.metadata == other.metadata &&
        self.command == other.command &&
        self.arguments == other.arguments &&
        self.output == other.output &&
        self.accepts_stdin == other.accepts_stdin &&
        self.metadata_json == other.metadata_json
    }
}

// Custom serializer for Cap that serializes urn as tags object (not canonical string)
impl Serialize for Cap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Cap", 10)?;
        
        // Serialize urn as tags object
        state.serialize_field("urn", &serde_json::json!({
            "tags": self.urn.tags
        }))?;
        
        state.serialize_field("title", &self.title)?;
        state.serialize_field("command", &self.command)?;
        
        if self.category.is_some() {
            state.serialize_field("category", &self.category)?;
        }
        
        if self.cap_description.is_some() {
            state.serialize_field("cap_description", &self.cap_description)?;
        }
        
        if !self.metadata.is_empty() {
            state.serialize_field("metadata", &self.metadata)?;
        }
        
        if !self.arguments.is_empty() {
            state.serialize_field("arguments", &self.arguments)?;
        }
        
        if self.output.is_some() {
            state.serialize_field("output", &self.output)?;
        }
        
        if self.accepts_stdin {
            state.serialize_field("accepts_stdin", &self.accepts_stdin)?;
        }
        
        if self.metadata_json.is_some() {
            state.serialize_field("metadata_json", &self.metadata_json)?;
        }
        
        state.end()
    }
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
            title: String,
            category: Option<String>,
            cap_description: Option<String>,
            #[serde(default)]
            metadata: HashMap<String, String>,
            command: String,
            #[serde(default)]
            arguments: CapArguments,
            output: Option<CapOutput>,
            #[serde(default)]
            accepts_stdin: bool,
            metadata_json: Option<serde_json::Value>,
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
            title: registry_cap.title,
            category: registry_cap.category,
            cap_description: registry_cap.cap_description,
            metadata: registry_cap.metadata,
            command: registry_cap.command,
            arguments: registry_cap.arguments,
            output: registry_cap.output,
            accepts_stdin: registry_cap.accepts_stdin,
            metadata_json: registry_cap.metadata_json,
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
            schema_ref: None,
            schema: None,
            metadata: None,
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
            schema_ref: None,
            schema: None,
            metadata: None,
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
            schema_ref: None,
            schema: None,
            metadata: None,
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
            schema_ref: None,
            schema: None,
            metadata: None,
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
        schema_ref: Option<String>,
        schema: Option<serde_json::Value>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            name,
            arg_type,
            arg_description: description,
            cli_flag,
            position,
            validation,
            default_value: default,
            schema_ref,
            schema,
            metadata,
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
    pub fn new(urn: CapUrn, title: String, command: String) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: None,
            metadata: HashMap::new(),
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
        }
    }

    /// Create a new cap with description
    pub fn with_description(urn: CapUrn, title: String, command: String, description: String) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: Some(description),
            metadata: HashMap::new(),
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
        }
    }

    /// Create a new cap with metadata
    pub fn with_metadata(
        urn: CapUrn,
        title: String,
        command: String,
        metadata: HashMap<String, String>
    ) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: None,
            metadata,
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
        }
    }

    /// Create a new cap with description and metadata
    pub fn with_description_and_metadata(
        urn: CapUrn,
        title: String,
        command: String,
        description: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: Some(description),
            metadata,
            command,
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
        }
    }
    
    /// Create a new cap with arguments
    pub fn with_arguments(
        urn: CapUrn,
        title: String,
        command: String,
        arguments: CapArguments,
    ) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: None,
            metadata: HashMap::new(),
            command,
            arguments,
            output: None,
            accepts_stdin: false,
            metadata_json: None,
        }
    }
    
    /// Create a new cap with command (deprecated - use new() instead)
    pub fn with_command(
        urn: CapUrn,
        title: String,
        command: String,
    ) -> Self {
        Self::new(urn, title, command)
    }
    
    /// Create a fully specified cap
    pub fn with_full_definition(
        urn: CapUrn,
        title: String,
        description: Option<String>,
        metadata: HashMap<String, String>,
        command: String,
        arguments: CapArguments,
        output: Option<CapOutput>,
        metadata_json: Option<serde_json::Value>,
    ) -> Self {
        Self {
            urn,
            title,
            category: None,
            cap_description: description,
            metadata,
            command,
            arguments,
            output,
            accepts_stdin: false,
            metadata_json,
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
    
    /// Get the title
    pub fn get_title(&self) -> &String {
        &self.title
    }
    
    /// Set the title
    pub fn set_title(&mut self, title: String) {
        self.title = title;
    }
    
    /// Get the category
    pub fn get_category(&self) -> Option<&String> {
        self.category.as_ref()
    }
    
    /// Set the category
    pub fn set_category(&mut self, category: String) {
        self.category = Some(category);
    }
    
    /// Clear the category
    pub fn clear_category(&mut self) {
        self.category = None;
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
    
    /// Get metadata JSON
    pub fn get_metadata_json(&self) -> Option<&serde_json::Value> {
        self.metadata_json.as_ref()
    }
    
    /// Set metadata JSON
    pub fn set_metadata_json(&mut self, metadata: serde_json::Value) {
        self.metadata_json = Some(metadata);
    }
    
    /// Clear metadata JSON
    pub fn clear_metadata_json(&mut self) {
        self.metadata_json = None;
    }
}

impl CapArgument {
    /// Get metadata JSON
    pub fn get_metadata(&self) -> Option<&serde_json::Value> {
        self.metadata.as_ref()
    }
    
    /// Set metadata JSON
    pub fn set_metadata(&mut self, metadata: serde_json::Value) {
        self.metadata = Some(metadata);
    }
    
    /// Clear metadata JSON
    pub fn clear_metadata(&mut self) {
        self.metadata = None;
    }
}

impl CapOutput {
    /// Get metadata JSON
    pub fn get_metadata(&self) -> Option<&serde_json::Value> {
        self.metadata.as_ref()
    }
    
    /// Set metadata JSON
    pub fn set_metadata(&mut self, metadata: serde_json::Value) {
        self.metadata = Some(metadata);
    }
    
    /// Clear metadata JSON
    pub fn clear_metadata(&mut self) {
        self.metadata = None;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_creation() {
        let urn = CapUrn::from_string("cap:action=transform;format=json;type=data_processing").unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());
        
        assert_eq!(cap.urn_string(), "cap:action=transform;format=json;type=data_processing");
        assert_eq!(cap.title, "Transform JSON Data");
        assert!(cap.metadata.is_empty());
    }

    #[test]
    fn test_cap_with_metadata() {
        let urn = CapUrn::from_string("cap:action=arithmetic;type=compute;subtype=math").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("precision".to_string(), "double".to_string());
        metadata.insert("operations".to_string(), "add,subtract,multiply,divide".to_string());
        
        let cap = Cap::with_metadata(urn, "Perform Mathematical Operations".to_string(), "test-command".to_string(), metadata);
        
        assert_eq!(cap.title, "Perform Mathematical Operations");
        assert_eq!(cap.get_metadata("precision"), Some(&"double".to_string()));
        assert_eq!(cap.get_metadata("operations"), Some(&"add,subtract,multiply,divide".to_string()));
        assert!(cap.has_metadata("precision"));
        assert!(!cap.has_metadata("nonexistent"));
    }

    #[test]
    fn test_cap_matching() {
        let urn = CapUrn::from_string("cap:action=transform;format=json;type=data_processing").unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());
        
        assert!(cap.matches_request("cap:action=transform;format=json;type=data_processing"));
        assert!(cap.matches_request("cap:action=transform;format=*;type=data_processing")); // Request wants any format, cap handles json specifically
        assert!(cap.matches_request("cap:type=data_processing")); // Request is subset, cap has all required tags
        assert!(!cap.matches_request("cap:type=compute"));
    }

    #[test]
    fn test_cap_title() {
        let urn = CapUrn::from_string("cap:action=extract;target=metadata").unwrap();
        let mut cap = Cap::new(urn, "Extract Document Metadata".to_string(), "extract-metadata".to_string());
        
        // Test title getter
        assert_eq!(cap.get_title(), &"Extract Document Metadata".to_string());
        assert_eq!(cap.title, "Extract Document Metadata");
        
        // Test title setter
        cap.set_title("Extract File Metadata".to_string());
        assert_eq!(cap.get_title(), &"Extract File Metadata".to_string());
        assert_eq!(cap.title, "Extract File Metadata");
    }

    #[test]
    fn test_cap_definition_equality() {
        let urn1 = CapUrn::from_string("cap:action=transform;format=json").unwrap();
        let urn2 = CapUrn::from_string("cap:action=transform;format=json").unwrap();
        
        let cap1 = Cap::new(urn1, "Transform JSON Data".to_string(), "transform".to_string());
        let cap2 = Cap::new(urn2.clone(), "Transform JSON Data".to_string(), "transform".to_string());
        let cap3 = Cap::new(urn2, "Convert JSON Format".to_string(), "transform".to_string());
        
        // Same cap definition (all fields identical) should be equal
        assert_eq!(cap1, cap2);
        
        // Cap definitions with different titles should not be equal (like any JSON object comparison)
        assert_ne!(cap1, cap3);
        assert_ne!(cap2, cap3);
    }

    #[test]
    fn test_cap_accepts_stdin() {
        let urn = CapUrn::from_string("cap:action=generate;target=embeddings").unwrap();
        let mut cap = Cap::new(urn, "Generate Embeddings".to_string(), "generate".to_string());
        
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