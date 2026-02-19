//! Formal cap definition
//!
//! This module defines the structure for formal cap definitions that include
//! the cap URN, versioning, and metadata. Caps are general-purpose
//! and do not assume any specific domain like files or documents.
//!
//! ## Cap Definition Format
//!
//! Caps use media URNs in `media_urn` fields that reference either:
//! - Standard media specs from the registry (e.g., "media:string")
//! - Inline media specs defined in the `media_specs` array
//!
//! Example:
//!
//! ```json
//! {
//!   "urn": "cap:in=\"media:string\";op=conversation;out=\"media:my-output;json;form=map\"",
//!   "media_specs": [
//!     {
//!       "urn": "media:my-output;json;form=map",
//!       "media_type": "application/json",
//!       "title": "My Output",
//!       "profile_uri": "https://example.com/schema",
//!       "schema": { "type": "object", ... }
//!     }
//!   ],
//!   "args": [
//!     { "media_urn": "media:string", "required": true, "sources": [{"cli_flag": "--input"}] }
//!   ],
//!   "output": { "media_urn": "media:my-output;json;form=map", ... }
//! }
//! ```

use crate::urn::cap_urn::CapUrn;
use crate::media::spec::{resolve_media_urn, MediaSpecDef, MediaSpecError, ResolvedMediaSpec};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Source specification for argument input
///
/// Each variant serializes to a distinct JSON object with a unique key:
/// - `{"stdin": "media:..."}`
/// - `{"position": 0}`
/// - `{"cli_flag": "--flag-name"}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged, deny_unknown_fields)]
pub enum ArgSource {
    /// Argument can be provided via stdin
    Stdin {
        /// Media URN for stdin input
        stdin: String,
    },
    /// Argument is positional
    Position {
        /// 0-based position in argument list
        position: usize,
    },
    /// Argument uses a CLI flag
    CliFlag {
        /// CLI flag (e.g., "--input" or "-i")
        cli_flag: String,
    },
}

impl ArgSource {
    pub fn get_type(&self) -> &'static str {
        match self {
            ArgSource::Stdin { .. } => "stdin",
            ArgSource::Position { .. } => "position",
            ArgSource::CliFlag { .. } => "cli_flag",
        }
    }

    pub fn stdin_media_urn(&self) -> Option<&str> {
        match self {
            ArgSource::Stdin { stdin } => Some(stdin),
            _ => None,
        }
    }

    pub fn position(&self) -> Option<usize> {
        match self {
            ArgSource::Position { position } => Some(*position),
            _ => None,
        }
    }

    pub fn cli_flag(&self) -> Option<&str> {
        match self {
            ArgSource::CliFlag { cli_flag } => Some(cli_flag),
            _ => None,
        }
    }
}

/// Cap argument definition - media_urn is the unique identifier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapArg {
    /// Unique media URN for this argument
    pub media_urn: String,

    /// Whether this argument is required
    pub required: bool,

    /// How this argument can be provided
    pub sources: Vec<ArgSource>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arg_description: Option<String>,

    /// Default value for optional arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,

    /// Arbitrary metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapArg {
    /// Create a new cap argument
    pub fn new(media_urn: impl Into<String>, required: bool, sources: Vec<ArgSource>) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            sources,
            arg_description: None,
            default_value: None,
            metadata: None,
        }
    }

    /// Create a new cap argument with description
    pub fn with_description(
        media_urn: impl Into<String>,
        required: bool,
        sources: Vec<ArgSource>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            sources,
            arg_description: Some(description.into()),
            default_value: None,
            metadata: None,
        }
    }

    /// Create a fully specified argument
    pub fn with_full_definition(
        media_urn: impl Into<String>,
        required: bool,
        sources: Vec<ArgSource>,
        description: Option<String>,
        default: Option<serde_json::Value>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            required,
            sources,
            arg_description: description,
            default_value: default,
            metadata,
        }
    }

    /// Get the media URN
    pub fn get_media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Resolve this argument's media spec
    ///
    /// Resolution order:
    /// 1. Cap's local media_specs (cap-specific overrides)
    /// 2. Registry's local cache (bundled standard specs)
    /// 3. Online registry fetch (with graceful degradation)
    ///
    /// # Arguments
    /// * `media_specs` - Optional media_specs map from the cap definition
    /// * `registry` - The media URN registry
    ///
    /// # Errors
    /// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved.
    pub async fn resolve(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_media_urn(&self.media_urn, media_specs, registry).await
    }

    /// Check if argument is binary based on resolved media spec
    pub async fn is_binary(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_binary())
    }

    /// Check if argument is JSON based on resolved media spec
    /// Note: This checks for explicit JSON format marker only.
    pub async fn is_json(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_json())
    }

    /// Check if argument is structured (map or list) based on resolved media spec
    /// Structured data can be serialized as JSON when transmitted as text.
    pub async fn is_structured(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_structured())
    }

    /// Get the media type from resolved spec
    pub async fn media_type(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<String, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.media_type)
    }

    /// Get the profile URI from resolved spec
    pub async fn profile_uri(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<Option<String>, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.profile_uri)
    }

    /// Get the schema from resolved spec (if any)
    pub async fn schema(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<Option<serde_json::Value>, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.schema)
    }

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

/// Output definition
///
/// The `media_urn` field contains a media URN (e.g., "media:object") that
/// references a definition in the cap's `media_specs` table or a built-in primitive.
/// Any output schema should be defined in the media_specs entry, not inline here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapOutput {
    /// Media URN referencing a media spec definition
    /// e.g., "media:object" or a custom media URN like "media:my-output"
    pub media_urn: String,

    pub output_description: String,

    /// Arbitrary metadata as JSON object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl CapOutput {
    /// Create a new output definition with media URN
    ///
    /// # Arguments
    /// * `media_urn` - Media URN referencing a media_specs entry (e.g., "media:object")
    /// * `description` - Human-readable description of the output
    pub fn new(media_urn: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            media_urn: media_urn.into(),
            output_description: description.into(),
            metadata: None,
        }
    }

    /// Create a fully specified output
    pub fn with_full_definition(
        media_urn: impl Into<String>,
        description: impl Into<String>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            media_urn: media_urn.into(),
            output_description: description.into(),
            metadata,
        }
    }

    /// Get the media URN
    pub fn get_media_urn(&self) -> &str {
        &self.media_urn
    }

    /// Resolve this output's media spec
    ///
    /// Resolution order:
    /// 1. Cap's local media_specs (cap-specific overrides)
    /// 2. Registry's local cache (bundled standard specs)
    /// 3. Online registry fetch (with graceful degradation)
    ///
    /// # Arguments
    /// * `media_specs` - Optional media_specs map from the cap definition
    /// * `registry` - The media URN registry
    ///
    /// # Errors
    /// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved.
    pub async fn resolve(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_media_urn(&self.media_urn, media_specs, registry).await
    }

    /// Check if output is binary based on resolved media spec
    pub async fn is_binary(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_binary())
    }

    /// Check if output is JSON based on resolved media spec
    /// Note: This checks for explicit JSON format marker only.
    pub async fn is_json(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_json())
    }

    /// Check if output is structured (map or list) based on resolved media spec
    /// Structured data can be serialized as JSON when transmitted as text.
    pub async fn is_structured(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<bool, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.is_structured())
    }

    /// Get the media type from resolved spec
    pub async fn media_type(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<String, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.media_type)
    }

    /// Get the profile URI from resolved spec
    pub async fn profile_uri(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<Option<String>, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.profile_uri)
    }

    /// Get the schema from resolved spec (if any)
    pub async fn schema(
        &self,
        media_specs: Option<&[MediaSpecDef]>,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<Option<serde_json::Value>, MediaSpecError> {
        self.resolve(media_specs, registry).await.map(|ms| ms.schema)
    }

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
/// - URN with tags (including `op`, `in`, `out` which use media URNs)
/// - `media_specs` array of inline media spec definitions
/// - Arguments with media URN references
/// - Output with media URN reference
#[derive(Debug, Clone, PartialEq)]
pub struct Cap {
    /// Formal cap URN with hierarchical naming
    /// Tags can include `op`, `in`, `out` (which should be media URNs)
    pub urn: CapUrn,

    /// Human-readable title of the capability (required)
    pub title: String,

    /// Optional description
    pub cap_description: Option<String>,

    /// Optional metadata as key-value pairs
    pub metadata: HashMap<String, String>,

    /// Command string for CLI execution
    pub command: String,

    /// Inline media spec definitions array
    /// Each spec has its own URN. Arguments and output reference these by URN.
    /// Duplicate URNs are not allowed.
    pub media_specs: Vec<MediaSpecDef>,

    /// Cap arguments
    pub args: Vec<CapArg>,

    /// Output definition
    pub output: Option<CapOutput>,

    /// Arbitrary metadata as JSON object
    pub metadata_json: Option<serde_json::Value>,

    /// Registration attribution - who registered this capability and when
    pub registered_by: Option<RegisteredBy>,
}

impl Serialize for Cap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Cap", 10)?;

        // Serialize urn as canonical string format
        state.serialize_field("urn", &self.urn.to_string())?;

        state.serialize_field("title", &self.title)?;
        state.serialize_field("command", &self.command)?;

        if self.cap_description.is_some() {
            state.serialize_field("cap_description", &self.cap_description)?;
        }

        if !self.metadata.is_empty() {
            state.serialize_field("metadata", &self.metadata)?;
        }

        if !self.media_specs.is_empty() {
            state.serialize_field("media_specs", &self.media_specs)?;
        }

        if !self.args.is_empty() {
            state.serialize_field("args", &self.args)?;
        }

        if self.output.is_some() {
            state.serialize_field("output", &self.output)?;
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

impl<'de> Deserialize<'de> for Cap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CapRegistry {
            urn: serde_json::Value,
            title: String,
            cap_description: Option<String>,
            #[serde(default)]
            metadata: HashMap<String, String>,
            command: String,
            #[serde(default)]
            media_specs: Vec<MediaSpecDef>,
            #[serde(default)]
            args: Vec<CapArg>,
            output: Option<CapOutput>,
            metadata_json: Option<serde_json::Value>,
            registered_by: Option<RegisteredBy>,
        }

        let registry_cap = CapRegistry::deserialize(deserializer)?;

        // URN must be a string in canonical format
        let urn = match registry_cap.urn {
            serde_json::Value::String(urn_str) => {
                CapUrn::from_string(&urn_str).map_err(serde::de::Error::custom)?
            },
            _ => return Err(serde::de::Error::custom("urn must be a string in canonical format (e.g., 'cap:in=\"media:...\";op=...;out=\"media:...\"')")),
        };

        Ok(Cap {
            urn,
            title: registry_cap.title,
            cap_description: registry_cap.cap_description,
            metadata: registry_cap.metadata,
            command: registry_cap.command,
            media_specs: registry_cap.media_specs,
            args: registry_cap.args,
            output: registry_cap.output,
            metadata_json: registry_cap.metadata_json,
            registered_by: registry_cap.registered_by,
        })
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
            media_specs: Vec::new(),
            args: Vec::new(),
            output: None,
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
            media_specs: Vec::new(),
            args: Vec::new(),
            output: None,
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
            media_specs: Vec::new(),
            args: Vec::new(),
            output: None,
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
            media_specs: Vec::new(),
            args: Vec::new(),
            output: None,
            metadata_json: None,
            registered_by: None,
        }
    }

    /// Create a new cap with args
    pub fn with_args(
        urn: CapUrn,
        title: String,
        command: String,
        args: Vec<CapArg>,
    ) -> Self {
        Self {
            urn,
            title,
            cap_description: None,
            metadata: HashMap::new(),
            command,
            media_specs: Vec::new(),
            args,
            output: None,
            metadata_json: None,
            registered_by: None,
        }
    }

    /// Create a fully specified cap
    pub fn with_full_definition(
        urn: CapUrn,
        title: String,
        description: Option<String>,
        metadata: HashMap<String, String>,
        command: String,
        media_specs: Vec<MediaSpecDef>,
        args: Vec<CapArg>,
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
            args,
            output,
            metadata_json,
            registered_by: None,
        }
    }

    /// Get the stdin media URN from args (first stdin source found)
    pub fn get_stdin_media_urn(&self) -> Option<&str> {
        for arg in &self.args {
            for source in &arg.sources {
                if let ArgSource::Stdin { stdin } = source {
                    return Some(stdin);
                }
            }
        }
        None
    }

    /// Check if this cap accepts stdin
    pub fn accepts_stdin(&self) -> bool {
        self.get_stdin_media_urn().is_some()
    }

    /// Get the media_specs array
    pub fn get_media_specs(&self) -> &[MediaSpecDef] {
        &self.media_specs
    }

    /// Set media_specs
    pub fn set_media_specs(&mut self, media_specs: Vec<MediaSpecDef>) {
        self.media_specs = media_specs;
    }

    /// Add a media spec definition
    pub fn add_media_spec(&mut self, def: MediaSpecDef) {
        self.media_specs.push(def);
    }

    /// Resolve a media URN using this cap's media_specs and the registry
    pub async fn resolve_media_urn(
        &self,
        media_urn: &str,
        registry: &crate::media::registry::MediaUrnRegistry,
    ) -> Result<ResolvedMediaSpec, MediaSpecError> {
        resolve_media_urn(media_urn, Some(&self.media_specs), registry).await
    }

    /// Check if this cap accepts a request string
    pub fn accepts_request(&self, request: &str) -> bool {
        let request_urn = CapUrn::from_string(request).expect("Invalid cap URN in request");
        request_urn.accepts(&self.urn)
    }

    /// Get the cap URN as a string
    pub fn urn_string(&self) -> String {
        self.urn.to_string()
    }

    /// Check if this cap is more specific than another for the same request
    pub fn is_more_specific_than(&self, other: &Cap, request: &str) -> bool {
        if !self.accepts_request(request) || !other.accepts_request(request) {
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

    /// Get the args
    pub fn get_args(&self) -> &Vec<CapArg> {
        &self.args
    }

    /// Set the args
    pub fn set_args(&mut self, args: Vec<CapArg>) {
        self.args = args;
    }

    /// Add an argument
    pub fn add_arg(&mut self, arg: CapArg) {
        self.args.push(arg);
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


#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:form=map";{}"#, tags)
    }

    // TEST108: Test creating new cap with URN, title, and command verifies correct initialization
    #[test]
    fn test108_cap_creation() {
        let urn = CapUrn::from_string(&test_urn("op=transform;format=json;data_processing")).unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());

        assert!(cap.urn_string().contains("op=transform"));
        // Check that in/out specs are present (format may vary due to canonicalization)
        assert!(cap.urn_string().contains("in="));
        assert!(cap.urn_string().contains("media:void"));
        assert!(cap.urn_string().contains("out="));
        assert!(cap.urn_string().contains("form=map"));
        assert_eq!(cap.title, "Transform JSON Data");
        assert!(cap.metadata.is_empty());
    }

    // TEST109: Test creating cap with metadata initializes and retrieves metadata correctly
    #[test]
    fn test109_cap_with_metadata() {
        let urn = CapUrn::from_string(&test_urn("op=arithmetic;compute;subtype=math")).unwrap();
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

    // TEST110: Test cap matching with subset semantics for request fulfillment
    #[test]
    fn test110_cap_matching() {
        // Use type=data_processing key-value instead of flag for proper matching
        let urn = CapUrn::from_string(&test_urn("op=transform;format=json;type=data_processing")).unwrap();
        let cap = Cap::new(urn, "Transform JSON Data".to_string(), "test-command".to_string());

        assert!(cap.accepts_request(&test_urn("op=transform;format=json;type=data_processing")));
        assert!(cap.accepts_request(&test_urn("op=transform;format=*;type=data_processing")));
        assert!(cap.accepts_request(&test_urn("type=data_processing")));
        assert!(!cap.accepts_request(&test_urn("type=compute")));
    }

    // TEST111: Test getting and setting cap title updates correctly
    #[test]
    fn test111_cap_title() {
        let urn = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
        let mut cap = Cap::new(urn, "Extract Document Metadata".to_string(), "extract-metadata".to_string());

        assert_eq!(cap.get_title(), &"Extract Document Metadata".to_string());
        assert_eq!(cap.title, "Extract Document Metadata");

        cap.set_title("Extract File Metadata".to_string());
        assert_eq!(cap.get_title(), &"Extract File Metadata".to_string());
        assert_eq!(cap.title, "Extract File Metadata");
    }

    // TEST112: Test cap equality based on URN and title matching
    #[test]
    fn test112_cap_definition_equality() {
        let urn1 = CapUrn::from_string(&test_urn("op=transform;format=json")).unwrap();
        let urn2 = CapUrn::from_string(&test_urn("op=transform;format=json")).unwrap();

        let cap1 = Cap::new(urn1, "Transform JSON Data".to_string(), "transform".to_string());
        let cap2 = Cap::new(urn2.clone(), "Transform JSON Data".to_string(), "transform".to_string());
        let cap3 = Cap::new(urn2, "Convert JSON Format".to_string(), "transform".to_string());

        assert_eq!(cap1, cap2);
        assert_ne!(cap1, cap3);
        assert_ne!(cap2, cap3);
    }

    // TEST113: Test cap stdin support via args with stdin source and serialization roundtrip
    #[test]
    fn test113_cap_stdin() {
        let urn = CapUrn::from_string(&test_urn("op=generate;target=embeddings")).unwrap();
        let mut cap = Cap::new(urn, "Generate Embeddings".to_string(), "generate".to_string());

        // By default, caps should not accept stdin
        assert!(!cap.accepts_stdin());
        assert!(cap.get_stdin_media_urn().is_none());

        // Enable stdin support by adding an arg with a stdin source
        let stdin_arg = CapArg {
            media_urn: "media:textable".to_string(),
            required: true,
            sources: vec![ArgSource::Stdin { stdin: "media:textable".to_string() }],
            arg_description: Some("Input text".to_string()),
            default_value: None,
            metadata: None,
        };
        cap.add_arg(stdin_arg);

        assert!(cap.accepts_stdin());
        assert_eq!(cap.get_stdin_media_urn(), Some("media:textable"));

        // Test serialization/deserialization preserves the args
        let serialized = serde_json::to_string(&cap).unwrap();
        assert!(serialized.contains("\"args\""));
        assert!(serialized.contains("\"stdin\""));
        let deserialized: Cap = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.accepts_stdin());
        assert_eq!(deserialized.get_stdin_media_urn(), Some("media:textable"));
    }

    // TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors
    #[test]
    fn test114_arg_source_types() {
        // Test stdin source
        let stdin_source = ArgSource::Stdin { stdin: "media:text".to_string() };
        assert_eq!(stdin_source.get_type(), "stdin");
        assert_eq!(stdin_source.stdin_media_urn(), Some("media:text"));
        assert_eq!(stdin_source.position(), None);
        assert_eq!(stdin_source.cli_flag(), None);

        // Test position source
        let position_source = ArgSource::Position { position: 0 };
        assert_eq!(position_source.get_type(), "position");
        assert_eq!(position_source.stdin_media_urn(), None);
        assert_eq!(position_source.position(), Some(0));
        assert_eq!(position_source.cli_flag(), None);

        // Test cli_flag source
        let cli_flag_source = ArgSource::CliFlag { cli_flag: "--input".to_string() };
        assert_eq!(cli_flag_source.get_type(), "cli_flag");
        assert_eq!(cli_flag_source.stdin_media_urn(), None);
        assert_eq!(cli_flag_source.position(), None);
        assert_eq!(cli_flag_source.cli_flag(), Some("--input"));
    }

    // TEST115: Test CapArg serialization and deserialization with multiple sources
    #[test]
    fn test115_cap_arg_serialization() {
        let arg = CapArg {
            media_urn: "media:string".to_string(),
            required: true,
            sources: vec![
                ArgSource::CliFlag { cli_flag: "--name".to_string() },
                ArgSource::Position { position: 0 },
            ],
            arg_description: Some("The name argument".to_string()),
            default_value: None,
            metadata: None,
        };

        let serialized = serde_json::to_string(&arg).unwrap();
        assert!(serialized.contains("\"media_urn\":\"media:string\""));
        assert!(serialized.contains("\"required\":true"));
        assert!(serialized.contains("\"cli_flag\":\"--name\""));
        assert!(serialized.contains("\"position\":0"));

        let deserialized: CapArg = serde_json::from_str(&serialized).unwrap();
        assert_eq!(arg, deserialized);
    }

    // TEST116: Test CapArg constructor methods basic and with_description create args correctly
    #[test]
    fn test116_cap_arg_constructors() {
        // Test basic constructor
        let arg = CapArg::new(
            "media:string",
            true,
            vec![ArgSource::CliFlag { cli_flag: "--name".to_string() }],
        );
        assert_eq!(arg.media_urn, "media:string");
        assert!(arg.required);
        assert_eq!(arg.sources.len(), 1);
        assert!(arg.arg_description.is_none());

        // Test with description
        let arg = CapArg::with_description(
            "media:integer",
            false,
            vec![ArgSource::Position { position: 0 }],
            "The count argument",
        );
        assert_eq!(arg.media_urn, "media:integer");
        assert!(!arg.required);
        assert_eq!(arg.arg_description, Some("The count argument".to_string()));
    }

    // TEST591: is_more_specific_than returns true when self has more tags for same request
    #[test]
    fn test591_is_more_specific_than() {
        let general = Cap::new(
            CapUrn::from_string(&test_urn("op=transform")).unwrap(),
            "General".to_string(),
            "cmd".to_string(),
        );
        let specific = Cap::new(
            CapUrn::from_string(&test_urn("op=transform;format=json")).unwrap(),
            "Specific".to_string(),
            "cmd".to_string(),
        );
        let unrelated = Cap::new(
            CapUrn::from_string(&test_urn("op=convert")).unwrap(),
            "Unrelated".to_string(),
            "cmd".to_string(),
        );

        // Specific is more specific than general for the general request
        assert!(
            specific.is_more_specific_than(&general, &test_urn("op=transform")),
            "specific cap must be more specific than general"
        );
        assert!(
            !general.is_more_specific_than(&specific, &test_urn("op=transform")),
            "general cap must not be more specific than specific"
        );

        // If either doesn't accept the request, returns false
        assert!(
            !general.is_more_specific_than(&unrelated, &test_urn("op=transform")),
            "unrelated cap doesn't accept request, so no comparison possible"
        );
    }

    // TEST592: remove_metadata adds then removes metadata correctly
    #[test]
    fn test592_remove_metadata() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "cmd".to_string());

        cap.set_metadata("key1".to_string(), "val1".to_string());
        cap.set_metadata("key2".to_string(), "val2".to_string());
        assert!(cap.has_metadata("key1"));
        assert!(cap.has_metadata("key2"));

        let removed = cap.remove_metadata("key1");
        assert_eq!(removed, Some("val1".to_string()));
        assert!(!cap.has_metadata("key1"));
        assert!(cap.has_metadata("key2"));

        // Removing non-existent returns None
        assert_eq!(cap.remove_metadata("nonexistent"), None);
    }

    // TEST593: registered_by lifecycle — set, get, clear
    #[test]
    fn test593_registered_by_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "cmd".to_string());

        // Initially None
        assert!(cap.get_registered_by().is_none());

        // Set
        let reg = RegisteredBy::new("alice", "2026-02-19T10:00:00Z");
        cap.set_registered_by(reg);
        let got = cap.get_registered_by().expect("should have registered_by");
        assert_eq!(got.username, "alice");
        assert_eq!(got.registered_at, "2026-02-19T10:00:00Z");

        // Clear
        cap.clear_registered_by();
        assert!(cap.get_registered_by().is_none());
    }

    // TEST594: metadata_json lifecycle — set, get, clear
    #[test]
    fn test594_metadata_json_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "cmd".to_string());

        // Initially None
        assert!(cap.get_metadata_json().is_none());

        // Set
        let json = serde_json::json!({"version": 2, "tags": ["experimental"]});
        cap.set_metadata_json(json.clone());
        assert_eq!(cap.get_metadata_json(), Some(&json));

        // Clear
        cap.clear_metadata_json();
        assert!(cap.get_metadata_json().is_none());
    }

    // TEST595: with_args constructor stores args correctly
    #[test]
    fn test595_with_args_constructor() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let args = vec![
            CapArg::new("media:string", true, vec![ArgSource::Position { position: 0 }]),
            CapArg::new("media:integer", false, vec![ArgSource::CliFlag { cli_flag: "--count".to_string() }]),
        ];

        let cap = Cap::with_args(urn, "Test".to_string(), "cmd".to_string(), args);
        assert_eq!(cap.get_args().len(), 2);
        assert_eq!(cap.get_args()[0].media_urn, "media:string");
        assert!(cap.get_args()[0].required);
        assert_eq!(cap.get_args()[1].media_urn, "media:integer");
        assert!(!cap.get_args()[1].required);
    }

    // TEST596: with_full_definition constructor stores all fields
    #[test]
    fn test596_with_full_definition_constructor() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("env".to_string(), "prod".to_string());
        let args = vec![CapArg::new("media:string", true, vec![])];
        let output = CapOutput::new("media:object", "Output object");
        let json_meta = serde_json::json!({"v": 1});

        let cap = Cap::with_full_definition(
            urn, "Full Cap".to_string(), Some("Description".to_string()),
            metadata, "full-cmd".to_string(), Vec::new(), args,
            Some(output), Some(json_meta.clone()),
        );

        assert_eq!(cap.title, "Full Cap");
        assert_eq!(cap.cap_description, Some("Description".to_string()));
        assert_eq!(cap.get_metadata("env"), Some(&"prod".to_string()));
        assert_eq!(cap.get_command(), &"full-cmd".to_string());
        assert_eq!(cap.get_args().len(), 1);
        assert!(cap.get_output().is_some());
        assert_eq!(cap.get_output().unwrap().media_urn, "media:object");
        assert_eq!(cap.get_metadata_json(), Some(&json_meta));
        // registered_by is not set by with_full_definition
        assert!(cap.get_registered_by().is_none());
    }

    // TEST597: CapArg::with_full_definition stores all fields including optional ones
    #[test]
    fn test597_cap_arg_with_full_definition() {
        let default_val = serde_json::json!("default_text");
        let meta = serde_json::json!({"hint": "enter name"});

        let arg = CapArg::with_full_definition(
            "media:string", true,
            vec![ArgSource::CliFlag { cli_flag: "--name".to_string() }],
            Some("User name".to_string()),
            Some(default_val.clone()),
            Some(meta.clone()),
        );

        assert_eq!(arg.media_urn, "media:string");
        assert!(arg.required);
        assert_eq!(arg.arg_description, Some("User name".to_string()));
        assert_eq!(arg.default_value, Some(default_val));
        assert_eq!(arg.get_metadata(), Some(&meta));

        // Metadata lifecycle
        let mut arg2 = arg.clone();
        arg2.clear_metadata();
        assert!(arg2.get_metadata().is_none());
        arg2.set_metadata(serde_json::json!("new"));
        assert_eq!(arg2.get_metadata(), Some(&serde_json::json!("new")));
    }

    // TEST598: CapOutput lifecycle — set_output, set/clear metadata
    #[test]
    fn test598_cap_output_lifecycle() {
        let urn = CapUrn::from_string(&test_urn("op=test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), "cmd".to_string());

        // Initially no output
        assert!(cap.get_output().is_none());

        // Set output
        let mut output = CapOutput::new("media:string", "Text output");
        output.set_metadata(serde_json::json!({"format": "plain"}));
        cap.set_output(output);

        let got = cap.get_output().expect("output should be set");
        assert_eq!(got.get_media_urn(), "media:string");
        assert_eq!(got.output_description, "Text output");
        assert!(got.get_metadata().is_some());

        // CapOutput with_full_definition
        let output2 = CapOutput::with_full_definition(
            "media:json", "JSON output", Some(serde_json::json!({"v": 2})),
        );
        assert_eq!(output2.get_media_urn(), "media:json");
        assert!(output2.get_metadata().is_some());

        // Clear metadata on output
        let mut output3 = output2.clone();
        output3.clear_metadata();
        assert!(output3.get_metadata().is_none());
    }
}
