//! Argument Binding for Cap Execution
//!
//! This module provides the file presentation layer and argument binding system.
//!
//! Design principles:
//! 1. **No domain leakage**: Caps see FILES only, never listings/chips/blocks
//! 2. **Pure data flow**: Caps receive only declared outputs from predecessors
//! 3. **Explicit sources**: Arguments come from explicit bindings, no ambient context

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use super::cardinality::InputCardinality;
use super::PlannerError;

/// A file presented to a cap for processing.
///
/// This is the uniform interface caps see - they never see listings, chips, or blocks directly.
/// Everything is converted to CapInputFile before being passed to a cap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapInputFile {
    /// Actual filesystem path to the file
    pub file_path: String,
    /// Media URN describing the file type (e.g., "media:pdf")
    pub media_urn: String,
    /// Optional file metadata
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub metadata: Option<CapFileMetadata>,
    /// Original source entity ID (for traceability, not passed to cap)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_id: Option<String>,
    /// Type of source entity
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source_type: Option<SourceEntityType>,
    /// Tracked file ID for file lifecycle management with plugins.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tracked_file_id: Option<String>,
    /// Security bookmark for accessing the file from the sandboxed plugin.
    /// Runtime-only — never serialized (macOS sandbox bookmark, opaque binary).
    #[serde(skip)]
    pub security_bookmark: Option<Vec<u8>>,
    /// Original file path before container path resolution.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_path: Option<String>,
}

/// Metadata about a cap input file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapFileMetadata {
    /// File name (without path)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// File size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// MIME type if known
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Additional metadata as JSON
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

/// Type of source entity (for internal tracking, not exposed to caps)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEntityType {
    Listing,
    Chip,
    Block,
    CapOutput,
    Temporary,
}

impl CapInputFile {
    pub fn new(file_path: String, media_urn: String) -> Self {
        Self {
            file_path,
            media_urn,
            metadata: None,
            source_id: None,
            source_type: None,
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
        }
    }

    pub fn from_listing(listing_id: &str, file_path: &str, media_urn: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
            media_urn: media_urn.to_string(),
            metadata: None,
            source_id: Some(listing_id.to_string()),
            source_type: Some(SourceEntityType::Listing),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
        }
    }

    pub fn from_chip(chip_id: &str, cache_path: &str, media_urn: &str) -> Self {
        Self {
            file_path: cache_path.to_string(),
            media_urn: media_urn.to_string(),
            metadata: None,
            source_id: Some(chip_id.to_string()),
            source_type: Some(SourceEntityType::Chip),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
        }
    }

    pub fn from_cap_output(output_path: String, media_urn: String) -> Self {
        Self {
            file_path: output_path,
            media_urn,
            metadata: None,
            source_id: None,
            source_type: Some(SourceEntityType::CapOutput),
            tracked_file_id: None,
            security_bookmark: None,
            original_path: None,
        }
    }

    pub fn with_metadata(mut self, metadata: CapFileMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_file_reference(
        mut self,
        tracked_file_id: String,
        security_bookmark: Vec<u8>,
        original_path: String,
    ) -> Self {
        self.tracked_file_id = Some(tracked_file_id);
        self.security_bookmark = Some(security_bookmark);
        self.original_path = Some(original_path);
        self
    }

    pub fn filename(&self) -> Option<&str> {
        std::path::Path::new(&self.file_path)
            .file_name()
            .and_then(|s| s.to_str())
    }

    pub fn has_file_reference(&self) -> bool {
        self.tracked_file_id.is_some() && self.security_bookmark.is_some()
    }
}

/// How to resolve an argument value for cap execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArgumentBinding {
    InputFile { index: usize },
    InputFilePath,
    InputMediaUrn,
    PreviousOutput {
        node_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        output_field: Option<String>,
    },
    CapDefault,
    CapSetting { setting_urn: String },
    Literal { value: serde_json::Value },
    Slot {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    PlanMetadata { key: String },
}

impl ArgumentBinding {
    pub fn literal_string(s: &str) -> Self {
        Self::Literal { value: serde_json::Value::String(s.to_string()) }
    }

    pub fn literal_number(n: i64) -> Self {
        Self::Literal { value: serde_json::Value::Number(n.into()) }
    }

    pub fn literal_bool(b: bool) -> Self {
        Self::Literal { value: serde_json::Value::Bool(b) }
    }

    pub fn requires_input(&self) -> bool {
        matches!(self, Self::Slot { .. })
    }

    pub fn references_previous(&self) -> bool {
        matches!(self, Self::PreviousOutput { .. })
    }
}

/// A resolved argument ready for cap execution.
#[derive(Debug, Clone)]
pub struct ResolvedArgument {
    pub name: String,
    pub value: Vec<u8>,
    pub source: ArgumentSource,
}

/// Source of a resolved argument value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentSource {
    InputFile,
    PreviousOutput,
    CapDefault,
    CapSetting,
    Literal,
    Slot,
    PlanMetadata,
}

/// Context for resolving argument bindings during execution.
#[derive(Debug)]
pub struct ArgumentResolutionContext<'a> {
    pub input_files: &'a [CapInputFile],
    pub current_file_index: usize,
    pub previous_outputs: &'a HashMap<String, serde_json::Value>,
    pub plan_metadata: Option<&'a HashMap<String, serde_json::Value>>,
    pub cap_settings: Option<&'a HashMap<String, HashMap<String, serde_json::Value>>>,
    pub slot_values: Option<&'a HashMap<String, Vec<u8>>>,
}

/// Static empty HashMap for use in context creation
static EMPTY_OUTPUTS: std::sync::LazyLock<HashMap<String, serde_json::Value>> = std::sync::LazyLock::new(HashMap::new);

impl<'a> ArgumentResolutionContext<'a> {
    pub fn with_inputs(input_files: &'a [CapInputFile]) -> Self {
        Self {
            input_files,
            current_file_index: 0,
            previous_outputs: &EMPTY_OUTPUTS,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        }
    }

    pub fn current_file(&self) -> Option<&CapInputFile> {
        self.input_files.get(self.current_file_index)
    }
}

/// Convert a serde_json::Value to raw bytes.
fn json_value_to_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

/// Resolve an argument binding to raw bytes.
pub fn resolve_binding(
    binding: &ArgumentBinding,
    context: &ArgumentResolutionContext,
    cap_urn: &str,
    default_value: Option<&serde_json::Value>,
    is_required: bool,
) -> Result<Option<ResolvedArgument>, PlannerError> {
    let (value, source) = match binding {
        ArgumentBinding::InputFile { index } => {
            let file = context.input_files.get(*index).ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Input file index {} out of bounds (have {} files)",
                    index, context.input_files.len()
                ))
            })?;
            (file.file_path.as_bytes().to_vec(), ArgumentSource::InputFile)
        }

        ArgumentBinding::InputFilePath => {
            let file = context.current_file().ok_or_else(|| {
                PlannerError::Internal("No current input file available".to_string())
            })?;
            (file.file_path.as_bytes().to_vec(), ArgumentSource::InputFile)
        }

        ArgumentBinding::InputMediaUrn => {
            let file = context.current_file().ok_or_else(|| {
                PlannerError::Internal("No current input file available".to_string())
            })?;
            (file.media_urn.as_bytes().to_vec(), ArgumentSource::InputFile)
        }

        ArgumentBinding::PreviousOutput { node_id, output_field } => {
            let output = context
                .previous_outputs
                .get(node_id)
                .ok_or_else(|| {
                    PlannerError::Internal(format!("No output from node '{}'", node_id))
                })?;

            let json_value = if let Some(field) = output_field {
                output.get(field).ok_or_else(|| {
                    PlannerError::Internal(format!(
                        "Field '{}' not found in output from node '{}'",
                        field, node_id
                    ))
                })?
            } else {
                output
            };

            (json_value_to_bytes(json_value), ArgumentSource::PreviousOutput)
        }

        ArgumentBinding::CapDefault => {
            let value = default_value
                .ok_or_else(|| {
                    PlannerError::Internal(format!(
                        "Cap '{}' has no default value for argument",
                        cap_urn
                    ))
                })?;
            (json_value_to_bytes(value), ArgumentSource::CapDefault)
        }

        ArgumentBinding::CapSetting { setting_urn } => {
            let cap_settings = context.cap_settings.ok_or_else(|| {
                PlannerError::Internal("No cap settings available".to_string())
            })?;

            let settings = cap_settings.get(cap_urn).ok_or_else(|| {
                PlannerError::Internal(format!("No settings for cap '{}'", cap_urn))
            })?;

            let value = settings.get(setting_urn).ok_or_else(|| {
                PlannerError::Internal(format!(
                    "Setting '{}' not found for cap '{}'",
                    setting_urn, cap_urn
                ))
            })?;

            (json_value_to_bytes(value), ArgumentSource::CapSetting)
        }

        ArgumentBinding::Literal { value } => (json_value_to_bytes(value), ArgumentSource::Literal),

        ArgumentBinding::Slot { name, .. } => {
            let key = format!("{}:{}", cap_urn, name);

            if let Some(slot_values) = context.slot_values {
                if let Some(bytes) = slot_values.get(&key) {
                    return Ok(Some(ResolvedArgument {
                        name: String::new(),
                        value: bytes.clone(),
                        source: ArgumentSource::Slot,
                    }));
                }
            }

            if let Some(cap_settings) = context.cap_settings {
                if let Some(settings) = cap_settings.get(cap_urn) {
                    if let Some(value) = settings.get(name) {
                        return Ok(Some(ResolvedArgument {
                            name: String::new(),
                            value: json_value_to_bytes(value),
                            source: ArgumentSource::CapSetting,
                        }));
                    }
                }
            }

            if let Some(default) = default_value {
                return Ok(Some(ResolvedArgument {
                    name: String::new(),
                    value: json_value_to_bytes(default),
                    source: ArgumentSource::CapDefault,
                }));
            }

            if is_required {
                return Err(PlannerError::Internal(format!(
                    "Missing required argument '{}': no value in slot_values (key: {}), settings, or default",
                    name, key
                )));
            } else {
                return Ok(None);
            }
        }

        ArgumentBinding::PlanMetadata { key } => {
            let metadata = context.plan_metadata.ok_or_else(|| {
                PlannerError::Internal("No plan metadata available".to_string())
            })?;

            let value = metadata.get(key).ok_or_else(|| {
                PlannerError::Internal(format!("Key '{}' not found in plan metadata", key))
            })?;

            (json_value_to_bytes(value), ArgumentSource::PlanMetadata)
        }
    };

    Ok(Some(ResolvedArgument {
        name: String::new(),
        value,
        source,
    }))
}

/// Collection of argument bindings for a cap node
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArgumentBindings {
    pub bindings: HashMap<String, ArgumentBinding>,
}

impl ArgumentBindings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: String, binding: ArgumentBinding) {
        self.bindings.insert(name, binding);
    }

    pub fn add_file_path(&mut self, arg_name: &str) {
        self.bindings.insert(arg_name.to_string(), ArgumentBinding::InputFilePath);
    }

    pub fn add_literal(&mut self, arg_name: &str, value: serde_json::Value) {
        self.bindings.insert(arg_name.to_string(), ArgumentBinding::Literal { value });
    }

    pub fn has_unresolved_slots(&self) -> bool {
        self.bindings.values().any(|b| b.requires_input())
    }

    pub fn get_unresolved_slots(&self) -> Vec<&str> {
        self.bindings
            .iter()
            .filter_map(|(name, b)| {
                if b.requires_input() { Some(name.as_str()) } else { None }
            })
            .collect()
    }

    pub fn resolve_all(
        &self,
        context: &ArgumentResolutionContext,
        cap_urn: &str,
        cap_defaults: Option<&HashMap<String, serde_json::Value>>,
        arg_required: Option<&HashMap<String, bool>>,
    ) -> Result<Vec<ResolvedArgument>, PlannerError> {
        let mut resolved = Vec::new();

        for (name, binding) in &self.bindings {
            let default = cap_defaults.and_then(|d| d.get(name));
            let is_required = arg_required.and_then(|r| r.get(name)).copied().unwrap_or(false);

            if let Some(mut arg) = resolve_binding(binding, context, cap_urn, default, is_required)? {
                arg.name = name.clone();
                resolved.push(arg);
            }
        }

        Ok(resolved)
    }
}

/// Input specification for cap chain execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapChainInput {
    pub files: Vec<CapInputFile>,
    pub expected_media_urn: String,
    pub cardinality: InputCardinality,
}

impl CapChainInput {
    pub fn single(file: CapInputFile) -> Self {
        let media_urn = file.media_urn.clone();
        Self {
            files: vec![file],
            expected_media_urn: media_urn,
            cardinality: InputCardinality::Single,
        }
    }

    pub fn sequence(files: Vec<CapInputFile>, media_urn: String) -> Self {
        Self {
            files,
            expected_media_urn: media_urn,
            cardinality: InputCardinality::Sequence,
        }
    }

    pub fn is_valid(&self) -> bool {
        match self.cardinality {
            InputCardinality::Single => self.files.len() == 1,
            InputCardinality::Sequence => !self.files.is_empty(),
            InputCardinality::AtLeastOne => !self.files.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cap_input_file_new() {
        let file = CapInputFile::new("/path/to/file.pdf".to_string(), "media:pdf".to_string());
        assert_eq!(file.file_path, "/path/to/file.pdf");
        assert_eq!(file.media_urn, "media:pdf");
        assert!(file.metadata.is_none());
        assert!(file.source_id.is_none());
    }

    #[test]
    fn test_cap_input_file_from_listing() {
        let file = CapInputFile::from_listing("listing-123", "/path/to/file.pdf", "media:pdf");
        assert_eq!(file.source_id, Some("listing-123".to_string()));
        assert_eq!(file.source_type, Some(SourceEntityType::Listing));
    }

    #[test]
    fn test_cap_input_file_filename() {
        let file = CapInputFile::new("/path/to/document.pdf".to_string(), "media:pdf".to_string());
        assert_eq!(file.filename(), Some("document.pdf"));
    }

    #[test]
    fn test_argument_binding_literal_string() {
        let binding = ArgumentBinding::literal_string("test");
        if let ArgumentBinding::Literal { value } = binding {
            assert_eq!(value, serde_json::Value::String("test".to_string()));
        } else {
            panic!("Expected Literal binding");
        }
    }

    #[test]
    fn test_argument_binding_requires_input() {
        let slot = ArgumentBinding::Slot { name: "width".to_string(), schema: None };
        assert!(slot.requires_input());
        let literal = ArgumentBinding::Literal { value: json!(100) };
        assert!(!literal.requires_input());
    }

    #[test]
    fn test_argument_binding_serialization() {
        let binding = ArgumentBinding::PreviousOutput {
            node_id: "node_0".to_string(),
            output_field: Some("result_path".to_string()),
        };
        let json = serde_json::to_string(&binding).unwrap();
        assert!(json.contains("previous_output"));
        assert!(json.contains("node_0"));
        let deserialized: ArgumentBinding = serde_json::from_str(&json).unwrap();
        if let ArgumentBinding::PreviousOutput { node_id, output_field } = deserialized {
            assert_eq!(node_id, "node_0");
            assert_eq!(output_field, Some("result_path".to_string()));
        } else {
            panic!("Expected PreviousOutput binding");
        }
    }

    #[test]
    fn test_argument_bindings_add_file_path() {
        let mut bindings = ArgumentBindings::new();
        bindings.add_file_path("input");
        assert!(bindings.bindings.contains_key("input"));
        assert!(matches!(bindings.bindings.get("input"), Some(ArgumentBinding::InputFilePath)));
    }

    #[test]
    fn test_argument_bindings_unresolved_slots() {
        let mut bindings = ArgumentBindings::new();
        bindings.add("width".to_string(), ArgumentBinding::Slot { name: "width".to_string(), schema: None });
        bindings.add("height".to_string(), ArgumentBinding::Literal { value: json!(100) });
        assert!(bindings.has_unresolved_slots());
        assert_eq!(bindings.get_unresolved_slots(), vec!["width"]);
    }

    #[test]
    fn test_resolve_input_file_path() {
        let files = vec![CapInputFile::new("/path/to/file.pdf".to_string(), "media:pdf".to_string())];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::InputFilePath;
        let result = resolve_binding(&binding, &context, "cap:test", None, true).unwrap().unwrap();
        assert_eq!(result.value, b"/path/to/file.pdf".to_vec());
        assert_eq!(result.source, ArgumentSource::InputFile);
    }

    #[test]
    fn test_resolve_literal() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Literal { value: json!(42) };
        let result = resolve_binding(&binding, &context, "cap:test", None, true).unwrap().unwrap();
        assert_eq!(result.value, serde_json::to_vec(&json!(42)).unwrap());
        assert_eq!(result.source, ArgumentSource::Literal);
    }

    #[test]
    fn test_resolve_previous_output() {
        let files = vec![];
        let mut prev_outputs = HashMap::new();
        prev_outputs.insert("node_0".to_string(), json!({"result_path": "/output/result.png"}));
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::PreviousOutput {
            node_id: "node_0".to_string(),
            output_field: Some("result_path".to_string()),
        };
        let result = resolve_binding(&binding, &context, "cap:test", None, true).unwrap().unwrap();
        assert_eq!(result.value, b"/output/result.png".to_vec());
        assert_eq!(result.source, ArgumentSource::PreviousOutput);
    }

    #[test]
    fn test_cap_chain_input_single() {
        let file = CapInputFile::new("/path/to/file.pdf".to_string(), "media:pdf".to_string());
        let input = CapChainInput::single(file);
        assert_eq!(input.files.len(), 1);
        assert_eq!(input.cardinality, InputCardinality::Single);
        assert!(input.is_valid());
    }

    #[test]
    fn test_cap_chain_input_vector() {
        let files = vec![
            CapInputFile::new("/path/1.pdf".to_string(), "media:pdf".to_string()),
            CapInputFile::new("/path/2.pdf".to_string(), "media:pdf".to_string()),
        ];
        let input = CapChainInput::sequence(files, "media:pdf".to_string());
        assert_eq!(input.files.len(), 2);
        assert_eq!(input.cardinality, InputCardinality::Sequence);
        assert!(input.is_valid());
    }

    #[test]
    fn test_cap_input_file_deserialization_from_dry_context() {
        let json_str = r#"[
            {
                "file_path": "/Users/bahram/ws/prj/filegrind/pdfcartridge/test_files/aws_in_action.pdf",
                "media_urn": "media:pdf",
                "source_id": "1b964d3b-f409-4f51-8684-884348ec2501",
                "source_type": "listing"
            }
        ]"#;
        let result: std::result::Result<Vec<CapInputFile>, _> = serde_json::from_str(json_str);
        assert!(result.is_ok(), "Deserialization should succeed: {:?}", result.err());
        let files = result.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].source_type, Some(SourceEntityType::Listing));
    }

    #[test]
    fn test_cap_input_file_deserialization_via_value() {
        let json_str = r#"[{"file_path": "/path/to/file.pdf","media_urn": "media:pdf","source_id": "abc123","source_type": "listing"}]"#;
        let value: serde_json::Value = serde_json::from_str(json_str).expect("Parse to Value");
        let result: std::result::Result<Vec<CapInputFile>, _> = serde_json::from_value(value);
        assert!(result.is_ok());
    }

    #[test]
    fn test668_resolve_slot_with_populated_byte_slot_values() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let mut slot_values: HashMap<String, Vec<u8>> = HashMap::new();
        slot_values.insert(
            "cap:in=\"media:pdf\";op=resize;out=\"media:pdf\":media:width;textable;numeric;form=scalar".to_string(),
            b"800".to_vec(),
        );
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: Some(&slot_values),
        };
        let binding = ArgumentBinding::Slot {
            name: "media:width;textable;numeric;form=scalar".to_string(),
            schema: None,
        };
        let result = resolve_binding(
            &binding, &context,
            "cap:in=\"media:pdf\";op=resize;out=\"media:pdf\"",
            None, true,
        ).unwrap().unwrap();
        assert_eq!(result.value, b"800".to_vec());
        assert_eq!(result.source, ArgumentSource::Slot);
    }

    #[test]
    fn test669_resolve_slot_falls_back_to_default() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:quality;textable;numeric;form=scalar".to_string(),
            schema: None,
        };
        let default = json!(85);
        let result = resolve_binding(&binding, &context, "cap:op=compress", Some(&default), false)
            .unwrap().unwrap();
        assert_eq!(result.value, serde_json::to_vec(&json!(85)).unwrap());
        assert_eq!(result.source, ArgumentSource::CapDefault);
    }

    #[test]
    fn test670_resolve_required_slot_no_value_returns_err() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:question;textable;form=scalar".to_string(),
            schema: None,
        };
        let result = resolve_binding(&binding, &context, "cap:op=generate", None, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("media:question;textable;form=scalar"));
    }

    #[test]
    fn test671_resolve_optional_slot_no_value_returns_none() {
        let files = vec![];
        let prev_outputs = HashMap::new();
        let context = ArgumentResolutionContext {
            input_files: &files,
            current_file_index: 0,
            previous_outputs: &prev_outputs,
            plan_metadata: None,
            cap_settings: None,
            slot_values: None,
        };
        let binding = ArgumentBinding::Slot {
            name: "media:suffix;textable;form=scalar".to_string(),
            schema: None,
        };
        let result = resolve_binding(&binding, &context, "cap:op=rename", None, false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_cap_chain_input_invalid_single() {
        let files = vec![
            CapInputFile::new("/path/1.pdf".to_string(), "media:pdf".to_string()),
            CapInputFile::new("/path/2.pdf".to_string(), "media:pdf".to_string()),
        ];
        let input = CapChainInput {
            files,
            expected_media_urn: "media:pdf".to_string(),
            cardinality: InputCardinality::Single,
        };
        assert!(!input.is_valid());
    }
}
