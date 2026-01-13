//! Formal cap definition
//!
//! This module defines the structure for formal cap definitions that include
//! the cap URN, versioning, and metadata. Caps are general-purpose
//! and do not assume any specific domain like files or documents.
//!
//! ## Cap Definition Format
//!
//! Caps now use spec IDs in `media_spec` fields that reference definitions
//! in the `media_specs` table. Example:
//!
//! ```json
//! {
//!   "urn": { "tags": { "op": "conversation", "in": "std:str.v1", "out": "my:output.v1" } },
//!   "media_specs": {
//!     "my:output.v1": {
//!       "media_type": "application/json",
//!       "profile_uri": "https://example.com/schema",
//!       "schema": { "type": "object", ... }
//!     }
//!   },
//!   "arguments": {
//!     "required": [{ "name": "input", "media_spec": "std:str.v1", ... }]
//!   },
//!   "output": { "media_spec": "my:output.v1", ... }
//! }
//! ```

use crate::cap_urn::CapUrn;
use crate::media_spec::{resolve_spec_id, MediaSpecDef, MediaSpecError, ResolvedMediaSpec};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

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
///
/// The `media_spec` field contains a spec ID (e.g., "std:str.v1") that
/// references a definition in the cap's `media_specs` table or a built-in primitive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapArgument {
    pub name: String,

    /// Spec ID referencing a media spec definition
    /// e.g., "std:str.v1", "std:int.v1", or a custom spec ID
    pub media_spec: String,

    pub arg_description: String,

    #[serde(rename = "cli_flag")]
    pub cli_flag: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,

    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,

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


/// Output definition
///
/// The `media_spec` field contains a spec ID (e.g., "std:obj.v1") that
/// references a definition in the cap's `media_specs` table or a built-in primitive.
/// Any output schema should be defined in the media_specs entry, not inline here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapOutput {
    /// Spec ID referencing a media spec definition
    /// e.g., "std:obj.v1" or a custom spec ID like "my:output-spec.v1"
    pub media_spec: String,

    #[serde(skip_serializing_if = "ArgumentValidation::is_empty", default)]
    pub validation: ArgumentValidation,

    pub output_description: String,

    /// Arbitrary metadata as JSON object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapOutput {
    /// Create a new output definition with spec ID
    ///
    /// # Arguments
    /// * `media_spec` - Spec ID referencing a media_specs entry (e.g., "std:obj.v1")
    /// * `description` - Human-readable description of the output
    pub fn new(media_spec: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            media_spec: media_spec.into(),
            output_description: description.into(),
            validation: ArgumentValidation::default(),
            metadata: None,
        }
    }

    /// Create output with validation
    pub fn with_validation(media_spec: impl Into<String>, description: impl Into<String>, validation: ArgumentValidation) -> Self {
        Self {
            media_spec: media_spec.into(),
            output_description: description.into(),
            validation,
            metadata: None,
        }
    }

    /// Create a fully specified output
    pub fn with_full_definition(
        media_spec: impl Into<String>,
        description: impl Into<String>,
        validation: ArgumentValidation,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            media_spec: media_spec.into(),
            output_description: description.into(),
            validation,
            metadata,
        }
    }

    /// Get the spec ID
    pub fn spec_id(&self) -> &str {
        &self.media_spec
    }

    /// Resolve this output's media spec using the provided media_specs table
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns `MediaSpecError::UnresolvableSpecId` if the spec ID cannot be resolved.
    pub fn resolve(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_spec_id(&self.media_spec, media_specs)
    }

    /// Check if output is binary based on resolved media spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn is_binary(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.is_binary())
    }

    /// Check if output is JSON based on resolved media spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn is_json(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.is_json())
    }

    /// Get the media type from resolved spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn media_type(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<String, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.media_type)
    }

    /// Get the profile URI from resolved spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn profile_uri(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<Option<String>, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.profile_uri)
    }

    /// Get the schema from resolved spec (if any)
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn schema(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<Option<serde_json::Value>, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.schema)
    }
}

/// Registration attribution - who registered this capability and when
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegisteredBy {
    /// Username of the user who registered this capability
    pub username: String,

    /// ISO 8601 timestamp of when the capability was registered
    pub registered_at: String,
}

impl RegisteredBy {
    /// Create a new registration attribution
    pub fn new(username: impl Into<String>, registered_at: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            registered_at: registered_at.into(),
        }
    }
}

/// Formal cap definition
///
/// A cap definition includes:
/// - URN with tags (including `op`, `in`, `out` which use spec IDs)
/// - `media_specs` table mapping spec IDs to definitions
/// - Arguments with spec ID references
/// - Output with spec ID reference
#[derive(Debug, Clone)]
pub struct Cap {
    /// Formal cap URN with hierarchical naming
    /// Tags can include `op`, `in`, `out` (which should be spec IDs)
    pub urn: CapUrn,

    /// Human-readable title of the capability (required)
    pub title: String,

    /// Optional description
    pub cap_description: Option<String>,

    /// Optional metadata as key-value pairs
    pub metadata: HashMap<String, String>,

    /// Command string for CLI execution
    pub command: String,

    /// Media spec definitions table
    /// Maps spec IDs to their definitions (string or object form)
    /// Arguments and output `media_spec` fields reference entries here
    pub media_specs: HashMap<String, MediaSpecDef>,

    /// Cap arguments
    pub arguments: CapArguments,

    /// Output definition
    pub output: Option<CapOutput>,

    /// Whether this cap accepts input via stdin
    pub accepts_stdin: bool,

    /// Arbitrary metadata as JSON object
    pub metadata_json: Option<serde_json::Value>,

    /// Registration attribution - who registered this capability and when
    pub registered_by: Option<RegisteredBy>,
}

// Custom PartialEq implementation that includes all fields
impl PartialEq for Cap {
    fn eq(&self, other: &Self) -> bool {
        self.urn == other.urn &&
        self.title == other.title &&
        self.cap_description == other.cap_description &&
        self.metadata == other.metadata &&
        self.command == other.command &&
        self.media_specs == other.media_specs &&
        self.arguments == other.arguments &&
        self.output == other.output &&
        self.accepts_stdin == other.accepts_stdin &&
        self.metadata_json == other.metadata_json &&
        self.registered_by == other.registered_by
    }
}

// Custom serializer for Cap that serializes urn as tags object (not canonical string)
impl Serialize for Cap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Cap", 11)?;

        // Serialize urn as tags object (including in/out direction specs)
        let mut all_tags = serde_json::Map::new();
        all_tags.insert("in".to_string(), serde_json::Value::String(self.urn.in_spec.clone()));
        all_tags.insert("out".to_string(), serde_json::Value::String(self.urn.out_spec.clone()));
        for (k, v) in &self.urn.tags {
            all_tags.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        state.serialize_field("urn", &serde_json::json!({
            "tags": all_tags
        }))?;

        state.serialize_field("title", &self.title)?;
        state.serialize_field("command", &self.command)?;

        if self.cap_description.is_some() {
            state.serialize_field("cap_description", &self.cap_description)?;
        }

        if !self.metadata.is_empty() {
            state.serialize_field("metadata", &self.metadata)?;
        }

        // Always serialize media_specs (can be empty)
        if !self.media_specs.is_empty() {
            state.serialize_field("media_specs", &self.media_specs)?;
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

        if self.registered_by.is_some() {
            state.serialize_field("registered_by", &self.registered_by)?;
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
            cap_description: Option<String>,
            #[serde(default)]
            metadata: HashMap<String, String>,
            command: String,
            #[serde(default)]
            media_specs: HashMap<String, MediaSpecDef>,
            #[serde(default)]
            arguments: CapArguments,
            output: Option<CapOutput>,
            #[serde(default)]
            accepts_stdin: bool,
            metadata_json: Option<serde_json::Value>,
            registered_by: Option<RegisteredBy>,
        }

        let registry_cap = CapRegistry::deserialize(deserializer)?;

        // Handle urn field - can be string or object with tags
        let urn = match registry_cap.urn {
            serde_json::Value::String(urn_str) => {
                CapUrn::from_string(&urn_str).map_err(serde::de::Error::custom)?
            },
            serde_json::Value::Object(urn_obj) => {
                if let Some(tags) = urn_obj.get("tags").and_then(|t| t.as_object()) {
                    // Extract required in/out specs first
                    let in_spec = tags.get("in")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| serde::de::Error::custom("Missing required 'in' tag in urn - caps must declare their input type (use std:void.v1 for no input)"))?
                        .to_string();
                    let out_spec = tags.get("out")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| serde::de::Error::custom("Missing required 'out' tag in urn - caps must declare their output type"))?
                        .to_string();

                    // Build CapUrn with direction specs and other tags
                    let mut urn_builder = crate::cap_urn::CapUrnBuilder::new()
                        .in_spec(&in_spec)
                        .out_spec(&out_spec);
                    for (key, value) in tags {
                        // Skip in/out as they're handled via in_spec/out_spec
                        if key == "in" || key == "out" {
                            continue;
                        }
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
            cap_description: registry_cap.cap_description,
            metadata: registry_cap.metadata,
            command: registry_cap.command,
            media_specs: registry_cap.media_specs,
            arguments: registry_cap.arguments,
            output: registry_cap.output,
            accepts_stdin: registry_cap.accepts_stdin,
            metadata_json: registry_cap.metadata_json,
            registered_by: registry_cap.registered_by,
        })
    }
}

impl CapArgument {
    /// Create a new cap argument with spec ID
    ///
    /// # Arguments
    /// * `name` - Argument name
    /// * `media_spec` - Spec ID referencing a media_specs entry (e.g., "std:str.v1")
    /// * `description` - Human-readable description
    /// * `cli_flag` - CLI flag for this argument (e.g., "--input")
    pub fn new(name: impl Into<String>, media_spec: impl Into<String>, description: impl Into<String>, cli_flag: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            media_spec: media_spec.into(),
            arg_description: description.into(),
            cli_flag: cli_flag.into(),
            position: None,
            validation: ArgumentValidation::default(),
            default_value: None,
            metadata: None,
        }
    }

    /// Create argument with position
    pub fn with_position(name: impl Into<String>, media_spec: impl Into<String>, description: impl Into<String>, cli_flag: impl Into<String>, position: usize) -> Self {
        Self {
            name: name.into(),
            media_spec: media_spec.into(),
            arg_description: description.into(),
            cli_flag: cli_flag.into(),
            position: Some(position),
            validation: ArgumentValidation::default(),
            default_value: None,
            metadata: None,
        }
    }

    /// Create argument with validation
    pub fn with_validation(name: impl Into<String>, media_spec: impl Into<String>, description: impl Into<String>, cli_flag: impl Into<String>, validation: ArgumentValidation) -> Self {
        Self {
            name: name.into(),
            media_spec: media_spec.into(),
            arg_description: description.into(),
            cli_flag: cli_flag.into(),
            position: None,
            validation,
            default_value: None,
            metadata: None,
        }
    }

    /// Create argument with default value
    pub fn with_default(name: impl Into<String>, media_spec: impl Into<String>, description: impl Into<String>, cli_flag: impl Into<String>, default: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            media_spec: media_spec.into(),
            arg_description: description.into(),
            cli_flag: cli_flag.into(),
            position: None,
            validation: ArgumentValidation::default(),
            default_value: Some(default),
            metadata: None,
        }
    }

    /// Create a fully specified argument
    pub fn with_full_definition(
        name: impl Into<String>,
        media_spec: impl Into<String>,
        description: impl Into<String>,
        cli_flag: impl Into<String>,
        position: Option<usize>,
        validation: ArgumentValidation,
        default: Option<serde_json::Value>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            name: name.into(),
            media_spec: media_spec.into(),
            arg_description: description.into(),
            cli_flag: cli_flag.into(),
            position,
            validation,
            default_value: default,
            metadata,
        }
    }

    /// Get the spec ID
    pub fn spec_id(&self) -> &str {
        &self.media_spec
    }

    /// Resolve this argument's media spec using the provided media_specs table
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns `MediaSpecError::UnresolvableSpecId` if the spec ID cannot be resolved.
    pub fn resolve(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_spec_id(&self.media_spec, media_specs)
    }

    /// Check if argument is binary based on resolved media spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn is_binary(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.is_binary())
    }

    /// Check if argument is JSON based on resolved media spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn is_json(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.is_json())
    }

    /// Get the media type from resolved spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn media_type(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<String, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.media_type)
    }

    /// Get the profile URI from resolved spec
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn profile_uri(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<Option<String>, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.profile_uri)
    }

    /// Get the schema from resolved spec (if any)
    ///
    /// # Arguments
    /// * `media_specs` - The media_specs map from the cap definition
    ///
    /// # Errors
    /// Returns error if the spec ID cannot be resolved
    pub fn schema(&self, media_specs: &HashMap<String, MediaSpecDef>) -> Result<Option<serde_json::Value>, MediaSpecError> {
        self.resolve(media_specs).map(|ms| ms.schema)
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
            cap_description: None,
            metadata: HashMap::new(),
            command,
            media_specs: HashMap::new(),
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        }
    }

    /// Create a new cap with description
    pub fn with_description(urn: CapUrn, title: String, command: String, description: String) -> Self {
        Self {
            urn,
            title,
            cap_description: Some(description),
            metadata: HashMap::new(),
            command,
            media_specs: HashMap::new(),
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
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
            cap_description: None,
            metadata,
            command,
            media_specs: HashMap::new(),
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
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
            cap_description: Some(description),
            metadata,
            command,
            media_specs: HashMap::new(),
            arguments: CapArguments::new(),
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
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
            cap_description: None,
            metadata: HashMap::new(),
            command,
            media_specs: HashMap::new(),
            arguments,
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        }
    }

    /// Create a new cap with command (deprecated - use new() instead)
    #[deprecated(note = "use new() instead")]
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
        media_specs: HashMap<String, MediaSpecDef>,
        arguments: CapArguments,
        output: Option<CapOutput>,
        metadata_json: Option<serde_json::Value>,
    ) -> Self {
        Self {
            urn,
            title,
            cap_description: description,
            metadata,
            command,
            media_specs,
            arguments,
            output,
            accepts_stdin: false,
            metadata_json,
            registered_by: None,
        }
    }

    /// Get the media_specs table
    pub fn get_media_specs(&self) -> &HashMap<String, MediaSpecDef> {
        &self.media_specs
    }

    /// Set media_specs
    pub fn set_media_specs(&mut self, media_specs: HashMap<String, MediaSpecDef>) {
        self.media_specs = media_specs;
    }

    /// Add a media spec definition
    pub fn add_media_spec(&mut self, spec_id: impl Into<String>, def: MediaSpecDef) {
        self.media_specs.insert(spec_id.into(), def);
    }

    /// Resolve a spec ID using this cap's media_specs
    pub fn resolve_spec_id(&self, spec_id: &str) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_spec_id(spec_id, &self.media_specs)
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

    /// Get the registration attribution
    pub fn get_registered_by(&self) -> Option<&RegisteredBy> {
        self.registered_by.as_ref()
    }

    /// Set the registration attribution
    pub fn set_registered_by(&mut self, registered_by: RegisteredBy) {
        self.registered_by = Some(registered_by);
    }

    /// Clear the registration attribution
    pub fn clear_registered_by(&mut self) {
        self.registered_by = None;
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

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!("cap:in=std:void.v1;out=std:obj.v1;{}", tags)
    }

    #[test]
    fn test_cap_creation() {
        let urn = CapUrn::from_string(&test_urn("op=transform;format=json;type=data_processing")).unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());

        // Alphabetical order: format < in < op < out < type
        assert!(cap.urn_string().contains("op=transform"));
        assert!(cap.urn_string().contains("in=std:void.v1"));
        assert!(cap.urn_string().contains("out=std:obj.v1"));
        assert_eq!(cap.title, "Transform JSON Data");
        assert!(cap.metadata.is_empty());
    }

    #[test]
    fn test_cap_with_metadata() {
        let urn = CapUrn::from_string(&test_urn("op=arithmetic;type=compute;subtype=math")).unwrap();
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
        let urn = CapUrn::from_string(&test_urn("op=transform;format=json;type=data_processing")).unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());

        assert!(cap.matches_request(&test_urn("op=transform;format=json;type=data_processing")));
        assert!(cap.matches_request(&test_urn("op=transform;format=*;type=data_processing"))); // Request wants any format, cap handles json specifically
        assert!(cap.matches_request(&test_urn("type=data_processing"))); // Request is subset, cap has all required tags
        assert!(!cap.matches_request(&test_urn("type=compute")));
    }

    #[test]
    fn test_cap_title() {
        let urn = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
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
        let urn1 = CapUrn::from_string(&test_urn("op=transform;format=json")).unwrap();
        let urn2 = CapUrn::from_string(&test_urn("op=transform;format=json")).unwrap();

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
        let urn = CapUrn::from_string(&test_urn("op=generate;target=embeddings")).unwrap();
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