//! Media URN - Data type specification using tagged URN format
//!
//! Media URNs use the tagged URN format with "media" prefix to describe
//! data types. They replace the old spec ID system (e.g., `media:string`).
//!
//! Format: `media:<type>[;subtype=<subtype>][;v=<version>][;profile=<url>][;...]`
//!
//! Examples:
//! - `media:string`
//! - `media:object`
//! - `media:application;subtype=json;profile="https://example.com/schema"`
//! - `media:image;subtype=png`
//!
//! Media URNs are just tagged URNs with the "media" prefix. Comparison and
//! matching use standard tagged URN semantics. Specific behaviors (like
//! profile resolution) are triggered by the presence of certain tags.

use std::fmt;
use std::str::FromStr;
use tagged_urn::{TaggedUrn, TaggedUrnBuilder, TaggedUrnError};

// =============================================================================
// STANDARD MEDIA URN CONSTANTS
// =============================================================================

// Primitive types
/// Media URN for void (no input/output) - no coercion tags
pub const MEDIA_VOID: &str = "media:void";
/// Media URN for string type - textable (can become text), scalar (single value)
pub const MEDIA_STRING: &str = "media:string;textable;scalar";
/// Media URN for integer type - textable, numeric (math ops valid), scalar
pub const MEDIA_INTEGER: &str = "media:integer;textable;numeric;scalar";
/// Media URN for number type - textable, numeric, scalar
pub const MEDIA_NUMBER: &str = "media:number;textable;numeric;scalar";
/// Media URN for boolean type - textable, scalar
pub const MEDIA_BOOLEAN: &str = "media:boolean;textable;scalar";
/// Media URN for JSON object type - textable (via JSON.stringify), keyed (key-value structure)
pub const MEDIA_OBJECT: &str = "media:object;textable;keyed";
/// Media URN for binary data - binary (raw bytes)
pub const MEDIA_BINARY: &str = "media:raw;binary";

// Array types
/// Media URN for string array type - textable, sequence (ordered collection)
pub const MEDIA_STRING_ARRAY: &str = "media:string-array;textable;sequence";
/// Media URN for integer array type - textable, numeric, sequence
pub const MEDIA_INTEGER_ARRAY: &str = "media:integer-array;textable;numeric;sequence";
/// Media URN for number array type - textable, numeric, sequence
pub const MEDIA_NUMBER_ARRAY: &str = "media:number-array;textable;numeric;sequence";
/// Media URN for boolean array type - textable, sequence
pub const MEDIA_BOOLEAN_ARRAY: &str = "media:boolean-array;textable;sequence";
/// Media URN for object array type - textable, keyed, sequence
pub const MEDIA_OBJECT_ARRAY: &str = "media:object-array;textable;keyed;sequence";

// Semantic media types for specialized content
/// Media URN for image data (png, jpg, gif, webp, etc.)
pub const MEDIA_PNG: &str = "media:png;binary";
/// Media URN for audio data (wav, mp3, flac, etc.)
pub const MEDIA_AUDIO: &str = "media:wav;audio;binary;";
/// Media URN for video data (mp4, webm, mov, etc.)
pub const MEDIA_VIDEO: &str = "media:video;binary";
/// Media URN for generic text (semantic type)
pub const MEDIA_TEXT: &str = "media:text;textable";

// Document types (PRIMARY naming - type IS the format)
/// Media URN for PDF documents
pub const MEDIA_PDF: &str = "media:pdf;binary";
/// Media URN for EPUB documents
pub const MEDIA_EPUB: &str = "media:epub;binary";

// Text format types (PRIMARY naming - type IS the format)
/// Media URN for Markdown text
pub const MEDIA_MD: &str = "media:md;textable";
/// Media URN for plain text
pub const MEDIA_TXT: &str = "media:txt;textable";
/// Media URN for reStructuredText
pub const MEDIA_RST: &str = "media:rst;textable";
/// Media URN for log files
pub const MEDIA_LOG: &str = "media:log;textable";
/// Media URN for HTML documents
pub const MEDIA_HTML: &str = "media:html;textable";
/// Media URN for XML documents
pub const MEDIA_XML: &str = "media:xml;textable";
/// Media URN for JSON data
pub const MEDIA_JSON: &str = "media:json;textable;keyed";
/// Media URN for YAML data
pub const MEDIA_YAML: &str = "media:yaml;textable;keyed";

// File path types - for arguments that represent filesystem paths
/// Media URN for a single file path - textable, scalar, and marked as a file-path for special handling
pub const MEDIA_FILE_PATH: &str = "media:file-path;textable;scalar";
/// Media URN for an array of file paths - textable, sequence, marked as file-path for special handling
pub const MEDIA_FILE_PATH_ARRAY: &str = "media:file-path-array;textable;sequence";

// Semantic text input types - distinguished by their purpose/context
/// Media URN for input text to generate embeddings
pub const MEDIA_INPUT_TEXT: &str = "media:input-text;textable;scalar";
/// Media URN for prompt text for LLM inference
pub const MEDIA_PROMPT_TEXT: &str = "media:prompt-text;textable;scalar";
/// Media URN for query text for searches/questions
pub const MEDIA_QUERY_TEXT: &str = "media:query-text;textable;scalar";
/// Media URN for content text for summarization/analysis
pub const MEDIA_CONTENT_TEXT: &str = "media:content-text;textable;scalar";
/// Media URN for frontmatter text (book metadata)
pub const MEDIA_FRONTMATTER_TEXT: &str = "media:frontmatter-text;textable;scalar";
/// Media URN for model identifier/name
pub const MEDIA_MODEL_ID: &str = "media:model-id;textable;scalar";
/// Media URN for model spec (provider:model format)
pub const MEDIA_MODEL_SPEC: &str = "media:model-spec;textable;scalar";
/// Media URN for HuggingFace model name
pub const MEDIA_HF_MODEL_NAME: &str = "media:hf-model-name;textable;scalar";
/// Media URN for MLX model path
pub const MEDIA_MLX_MODEL_PATH: &str = "media:mlx-model-path;textable;scalar";
/// Media URN for management operation type
pub const MEDIA_MANAGEMENT_OPERATION: &str = "media:management-operation;textable;scalar";

/// Helper to build binary media URN with extension
pub fn binary_media_urn_for_ext(ext: &str) -> String {
    format!("media:binary;ext={}", ext)
}

/// Helper to build text media URN with extension
pub fn text_media_urn_for_ext(ext: &str) -> String {
    format!("media:text;ext={};textable", ext)
}

/// Helper to build image media URN with extension
pub fn image_media_urn_for_ext(ext: &str) -> String {
    format!("media:image;ext={};binary", ext)
}

/// Helper to build audio media URN with extension
pub fn audio_media_urn_for_ext(ext: &str) -> String {
    format!("media:audio;ext={};binary", ext)
}

// CAPNS output types - all keyed structures (JSON objects)
/// Media URN for model download output - textable, keyed
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:download-result;textable;keyed";
/// Media URN for model load output - textable, keyed
pub const MEDIA_LOAD_OUTPUT: &str = "media:load-output;textable;keyed";
/// Media URN for model unload output - textable, keyed
pub const MEDIA_UNLOAD_OUTPUT: &str = "media:unload-output;textable;keyed";
/// Media URN for model list output - textable, keyed
pub const MEDIA_LIST_OUTPUT: &str = "media:list-output;textable;keyed";
/// Media URN for model status output - textable, keyed
pub const MEDIA_STATUS_OUTPUT: &str = "media:status-output;textable;keyed";
/// Media URN for model contents output - textable, keyed
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:model-contents;textable;keyed";
/// Media URN for embeddings generate output - textable, keyed
pub const MEDIA_GENERATE_OUTPUT: &str = "media:embedding-vector;textable;keyed";
/// Media URN for structured query output - textable, keyed
pub const MEDIA_STRUCTURED_QUERY_OUTPUT: &str = "media:json;textable;keyed";
/// Media URN for questions array - textable, sequence
pub const MEDIA_QUESTIONS_ARRAY: &str = "media:string-array;textable;sequence";
/// Media URN for LLM inference output - textable, keyed
pub const MEDIA_LLM_INFERENCE_OUTPUT: &str = "media:llm-inference-output;textable;keyed";
/// Media URN for extracted metadata - textable, keyed
pub const MEDIA_FILE_METADATA: &str = "media:file-metadata;textable;keyed";
/// Media URN for extracted outline - textable, keyed
pub const MEDIA_DOCUMENT_OUTLINE: &str = "media:document-outline;textable;keyed";
/// Media URN for disbound pages - textable, keyed, sequence (array of chunks)
pub const MEDIA_DISBOUND_PAGES: &str = "media:disbound-pages;textable;keyed;sequence";
/// Media URN for embeddings output - textable, keyed
pub const MEDIA_EMBEDDINGS_OUTPUT: &str = "media:embedding-vector;textable;keyed";
/// Media URN for image embeddings output - textable, keyed
pub const MEDIA_PNG_EMBEDDINGS_OUTPUT: &str = "media:embedding-vector;textable;keyed";
/// Media URN for caption output - textable, keyed
pub const MEDIA_CAPTION_OUTPUT: &str = "media:image-caption;textable;keyed";
/// Media URN for transcription output - textable, keyed
pub const MEDIA_TRANSCRIPTION_OUTPUT: &str = "media:transcription-output;textable;keyed";
/// Media URN for vision inference output - textable, keyed
pub const MEDIA_VISION_INFERENCE_OUTPUT: &str = "media:vision-inference-output;textable;keyed";
/// Media URN for model management output - textable, keyed
pub const MEDIA_MANAGE_OUTPUT: &str = "media:manage-output;textable;keyed";

// =============================================================================
// MEDIA URN TYPE
// =============================================================================

/// A media URN representing a data type specification
///
/// Media URNs are tagged URNs with the "media" prefix. They describe data
/// types using tags like `type`, `subtype`, `v` (version), and `profile`.
///
/// This is a newtype wrapper around `TaggedUrn` that enforces the "media"
/// prefix and provides convenient accessors for common tags.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MediaUrn(TaggedUrn);

impl MediaUrn {
    /// The required prefix for all media URNs
    pub const PREFIX: &'static str = "media";

    /// Create a new MediaUrn from a TaggedUrn
    ///
    /// Returns an error if the TaggedUrn doesn't have the "media" prefix.
    pub fn new(urn: TaggedUrn) -> Result<Self, MediaUrnError> {
        if urn.prefix != Self::PREFIX {
            return Err(MediaUrnError::InvalidPrefix {
                expected: Self::PREFIX.to_string(),
                actual: urn.prefix.clone(),
            });
        }
        Ok(Self(urn))
    }

    /// Create a MediaUrn from a string representation
    ///
    /// The string must be a valid tagged URN with the "media" prefix.
    pub fn from_string(s: &str) -> Result<Self, MediaUrnError> {
        let urn = TaggedUrn::from_string(s).map_err(MediaUrnError::Parse)?;
        Self::new(urn)
    }

    /// Create a simple MediaUrn with just type and version
    pub fn simple(type_name: &str, version: u32) -> Self {
        let urn = TaggedUrnBuilder::new(Self::PREFIX)
            .solo_tag(type_name)
            .tag("v", &version.to_string())
            .expect("version is non-empty")
            .build()
            .expect("valid media URN");
        Self(urn)
    }

    /// Create a MediaUrn with type, subtype, and optional version
    pub fn with_subtype(type_name: &str, subtype: &str, version: Option<u32>) -> Self {
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX)
            .solo_tag(type_name)
            .tag("subtype", subtype)
            .expect("subtype is non-empty");
        if let Some(v) = version {
            builder = builder.tag("v", &v.to_string()).expect("version is non-empty");
        }
        let urn = builder.build().expect("valid media URN");
        Self(urn)
    }

    /// Get the inner TaggedUrn
    pub fn inner(&self) -> &TaggedUrn {
        &self.0
    }

    /// Get the type tag value (e.g., "string", "object", "application")
    pub fn type_name(&self) -> Option<&str> {
        self.0.get_tag("type").map(|s| s.as_str())
    }

    /// Get the subtype tag value (e.g., "json", "pdf", "png")
    pub fn subtype(&self) -> Option<&str> {
        self.0.get_tag("subtype").map(|s| s.as_str())
    }

    /// Get the version tag value
    pub fn version(&self) -> Option<u32> {
        self.0.get_tag("v").and_then(|v| v.parse().ok())
    }

    /// Get the profile tag value (URL)
    pub fn profile(&self) -> Option<&str> {
        self.0.get_tag("profile").map(|s| s.as_str())
    }

    /// Get any tag value by key
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.0.get_tag(key).map(|s| s.as_str())
    }

    /// Check if this media URN has a specific tag
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        self.0.has_tag(key, value)
    }

    /// Create a new MediaUrn with an additional or updated tag
    /// Returns error if value is empty (use "*" for wildcard)
    pub fn with_tag(&self, key: &str, value: &str) -> Result<Self, tagged_urn::TaggedUrnError> {
        Ok(Self(self.0.clone().with_tag(key.to_string(), value.to_string())?))
    }

    /// Create a new MediaUrn without a specific tag
    pub fn without_tag(&self, key: &str) -> Self {
        Self(self.0.clone().without_tag(key))
    }

    /// Get the canonical string representation
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// Check if this media URN matches a request using tagged URN semantics
    ///
    /// Matching follows standard tagged URN rules:
    /// - Missing tags are treated as implicit wildcards
    /// - Explicit "*" values are wildcards
    /// - All present tags must match
    pub fn matches(&self, request: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.matches(&request.0).map_err(MediaUrnError::Match)
    }

    /// Get the specificity of this media URN
    ///
    /// Specificity is the count of non-wildcard tags.
    pub fn specificity(&self) -> usize {
        self.0.specificity()
    }

    /// Check if this media URN satisfies a cap's input requirement
    ///
    /// Returns true if all required tags in `requirement` are present in `self`
    /// with matching values. This is used to determine if an item (listing, chip, block)
    /// with this media URN can provide input to a cap.
    ///
    /// # Matching rules:
    /// - Type must match exactly
    /// Check if this media URN satisfies a requirement.
    ///
    /// For media URNs, satisfies uses STRICT matching for type flags:
    /// - Both must have the exact same set of flag tags (tags with value "*")
    /// - Non-flag tags follow standard wildcard matching (missing = wildcard)
    ///
    /// This prevents media:object from matching media:string since they have
    /// different type flags.
    pub fn satisfies(&self, requirement: &MediaUrn) -> bool {
        // Get all flag tags (value == "*") from both URNs
        let self_flags: std::collections::HashSet<_> = self.0.tags.iter()
            .filter(|(_, v)| v.as_str() == "*")
            .map(|(k, _)| k.as_str())
            .collect();

        let req_flags: std::collections::HashSet<_> = requirement.0.tags.iter()
            .filter(|(_, v)| v.as_str() == "*")
            .map(|(k, _)| k.as_str())
            .collect();

        // Flags must match exactly - this ensures media:object != media:string
        if self_flags != req_flags {
            return false;
        }

        // For non-flag tags, requirement's tags must be present in self
        for (key, req_value) in &requirement.0.tags {
            if req_value == "*" {
                // Already checked flags above
                continue;
            }

            match self.0.tags.get(key) {
                Some(self_value) => {
                    if self_value != "*" && self_value != req_value {
                        return false;
                    }
                }
                None => {
                    // Self is missing a required non-flag tag
                    return false;
                }
            }
        }

        true
    }

    /// Get the extension tag value (e.g., "pdf", "epub", "md")
    pub fn extension(&self) -> Option<&str> {
        self.get_tag("ext")
    }

    // =========================================================================
    // Behavior helpers (triggered by tag presence)
    // =========================================================================

    /// Check if this represents binary data.
    /// Returns true if the "binary" marker tag is present.
    pub fn is_binary(&self) -> bool {
        self.get_tag("binary").is_some()
    }

    /// Check if this represents JSON/keyed data.
    /// Returns true if the "keyed" marker tag is present.
    pub fn is_json(&self) -> bool {
        self.get_tag("keyed").is_some()
    }

    /// Check if this represents text data.
    /// Returns true if the "textable" marker tag is present.
    pub fn is_text(&self) -> bool {
        self.get_tag("textable").is_some()
    }

    /// Check if this represents a void (no data) type
    pub fn is_void(&self) -> bool {
        // Check for "void" flag (solo tag) or type=void
        self.0.tags.contains_key("void") || self.type_name() == Some("void")
    }

    /// Check if this represents a file path type.
    /// Returns true if the "file-path" marker tag is present.
    pub fn is_file_path(&self) -> bool {
        self.0.tags.get("file-path").map_or(false, |v| v == "*")
    }

    /// Check if this represents a file path array type.
    /// Returns true if the "file-path-array" marker tag is present.
    pub fn is_file_path_array(&self) -> bool {
        self.0.tags.get("file-path-array").map_or(false, |v| v == "*")
    }

    /// Check if this represents any file path type (single or array).
    /// Returns true if either "file-path" or "file-path-array" marker tag is present.
    pub fn is_any_file_path(&self) -> bool {
        self.is_file_path() || self.is_file_path_array()
    }

    /// Check if this media URN can provide input for a cap with the given requirement.
    ///
    /// This is used for path finding to determine if our current output (self) can
    /// flow into a cap that requires the given input media URN (requirement).
    ///
    /// The check ensures that all marker tags (wildcard tags) in the requirement
    /// are also present in self. For example:
    /// - `media:pdf;binary` can_provide_input_for `media:pdf;binary` -> TRUE
    /// - `media:pdf;binary` can_provide_input_for `media:pdf` -> TRUE (has extra binary)
    /// - `media:png;binary` can_provide_input_for `media:pdf;binary` -> FALSE (missing pdf)
    /// - `media:binary` can_provide_input_for `media:pdf;binary` -> FALSE (missing pdf)
    ///
    /// Marker tags are tags with value "*" (wildcards). These represent type/format
    /// markers like "pdf", "png", "binary", "textable", etc.
    pub fn can_provide_input_for(&self, requirement: &MediaUrn) -> bool {
        // Get all marker tags (wildcards) from the requirement
        // A marker tag is one with value "*"
        for (key, value) in &requirement.0.tags {
            if value == "*" {
                // This is a marker tag - self must also have this marker
                match self.0.tags.get(key) {
                    Some(self_value) if self_value == "*" => {
                        // Self also has this marker - OK
                        continue;
                    }
                    _ => {
                        // Self doesn't have this marker or has a different value - FAIL
                        return false;
                    }
                }
            } else {
                // Non-marker tag (has specific value) - check if values match
                match self.0.tags.get(key) {
                    Some(self_value) => {
                        if self_value != value && self_value != "*" {
                            // Values don't match and self doesn't have wildcard
                            return false;
                        }
                    }
                    None => {
                        // Self doesn't have this tag - treated as wildcard, OK
                        continue;
                    }
                }
            }
        }
        true
    }
}

impl fmt::Display for MediaUrn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MediaUrn {
    type Err = MediaUrnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}

// =============================================================================
// ERROR TYPE
// =============================================================================

/// Errors that can occur when working with media URNs
#[derive(Debug, Clone, PartialEq)]
pub enum MediaUrnError {
    /// The URN doesn't have the required "media" prefix
    InvalidPrefix { expected: String, actual: String },
    /// Error parsing the underlying tagged URN
    Parse(TaggedUrnError),
    /// Error during matching operation
    Match(TaggedUrnError),
}

impl fmt::Display for MediaUrnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaUrnError::InvalidPrefix { expected, actual } => {
                write!(
                    f,
                    "invalid media URN prefix: expected '{}', got '{}'",
                    expected, actual
                )
            }
            MediaUrnError::Parse(e) => write!(f, "failed to parse media URN: {}", e),
            MediaUrnError::Match(e) => write!(f, "media URN match error: {}", e),
        }
    }
}

impl std::error::Error for MediaUrnError {}

impl From<TaggedUrnError> for MediaUrnError {
    fn from(e: TaggedUrnError) -> Self {
        MediaUrnError::Parse(e)
    }
}

// =============================================================================
// SERDE SUPPORT
// =============================================================================

impl serde::Serialize for MediaUrn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for MediaUrn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MediaUrn::from_string(&s).map_err(serde::de::Error::custom)
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let urn = MediaUrn::from_string("media:string").unwrap();
        // type_name() returns None for flag-based types (requires type= tag)
        // Use to_string() to verify the URN is parsed correctly
        assert!(urn.to_string().contains("string"));
        assert!(urn.version().is_none());
        assert!(urn.subtype().is_none());
        assert!(urn.profile().is_none());
    }

    #[test]
    fn test_parse_with_subtype() {
        // Subtype is extracted from subtype= tag
        let urn = MediaUrn::from_string("media:application;subtype=json").unwrap();
        assert_eq!(urn.subtype(), Some("json"));
        assert!(urn.to_string().contains("application"));
    }

    #[test]
    fn test_parse_with_profile() {
        // Profile is extracted from profile= tag
        let urn = MediaUrn::from_string(
            r#"media:object;profile="https://example.com/schema.json""#,
        )
        .unwrap();
        assert_eq!(urn.profile(), Some("https://example.com/schema.json"));
        assert!(urn.to_string().contains("object"));
    }

    #[test]
    fn test_wrong_prefix_fails() {
        let result = MediaUrn::from_string("cap:string");
        assert!(result.is_err());
        if let Err(MediaUrnError::InvalidPrefix { expected, actual }) = result {
            assert_eq!(expected, "media");
            assert_eq!(actual, "cap");
        } else {
            panic!("expected InvalidPrefix error");
        }
    }

    #[test]
    fn test_is_binary() {
        // is_binary returns true only if "binary" marker tag is present
        assert!(MediaUrn::from_string("media:raw;binary").unwrap().is_binary());
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_binary()); // "media:png;binary"
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_binary()); // "media:pdf;binary"
        assert!(MediaUrn::from_string(MEDIA_BINARY).unwrap().is_binary()); // "media:raw;binary"
        // Without binary tag, is_binary is false
        assert!(!MediaUrn::from_string("media:string;textable").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:object;textable;keyed").unwrap().is_binary());
    }

    #[test]
    fn test_is_json() {
        // is_json returns true only if "keyed" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_json()); // "media:object;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_json()); // "media:json;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_json()); // "media:object-array;textable;keyed;sequence"
        assert!(MediaUrn::from_string("media:custom;keyed").unwrap().is_json());
        // Without keyed tag, is_json is false
        assert!(!MediaUrn::from_string("media:string;textable").unwrap().is_json());
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_json()); // string-array has textable;sequence but not keyed
    }

    #[test]
    fn test_is_text() {
        // is_text returns true only if "textable" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_text()); // "media:string;textable;scalar"
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_text()); // "media:integer;textable;numeric;scalar"
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_text()); // "media:object;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_TEXT).unwrap().is_text()); // "media:text;textable"
        // Without textable tag, is_text is false
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_text()); // "media:raw;binary"
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_text()); // "media:png;binary"
    }

    #[test]
    fn test_is_void() {
        assert!(MediaUrn::from_string("media:void").unwrap().is_void());
        assert!(!MediaUrn::from_string("media:string").unwrap().is_void());
    }

    #[test]
    fn test_simple_constructor() {
        let urn = MediaUrn::simple("string", 1);
        // Constructor creates URN with flag (solo tag) for type, not type= tag
        // So type_name() returns None, but the string representation has it
        assert!(urn.to_string().contains("string"));
        assert_eq!(urn.version(), Some(1));
    }

    #[test]
    fn test_with_subtype_constructor() {
        let urn = MediaUrn::with_subtype("application", "json", Some(1));
        // Constructor creates URN with flag (solo tag) for type, not type= tag
        assert!(urn.to_string().contains("application"));
        assert_eq!(urn.subtype(), Some("json"));
        // Constructor with version adds it explicitly
        assert_eq!(urn.version(), Some(1));
    }

    #[test]
    fn test_to_string_roundtrip() {
        let original = "media:string";
        let urn = MediaUrn::from_string(original).unwrap();
        let s = urn.to_string();
        let urn2 = MediaUrn::from_string(&s).unwrap();
        assert_eq!(urn, urn2);
    }

    #[test]
    fn test_constants_parse() {
        // Verify all constants are valid media URNs
        assert!(MediaUrn::from_string(MEDIA_VOID).is_ok());
        assert!(MediaUrn::from_string(MEDIA_STRING).is_ok());
        assert!(MediaUrn::from_string(MEDIA_INTEGER).is_ok());
        assert!(MediaUrn::from_string(MEDIA_NUMBER).is_ok());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).is_ok());
        assert!(MediaUrn::from_string(MEDIA_OBJECT).is_ok());
        assert!(MediaUrn::from_string(MEDIA_BINARY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_STRING_ARRAY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_INTEGER_ARRAY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_NUMBER_ARRAY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN_ARRAY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_OBJECT_ARRAY).is_ok());
        // Semantic types
        assert!(MediaUrn::from_string(MEDIA_PNG).is_ok());
        assert!(MediaUrn::from_string(MEDIA_AUDIO).is_ok());
        assert!(MediaUrn::from_string(MEDIA_VIDEO).is_ok());
        assert!(MediaUrn::from_string(MEDIA_TEXT).is_ok());
        // Document types (PRIMARY naming)
        assert!(MediaUrn::from_string(MEDIA_PDF).is_ok());
        assert!(MediaUrn::from_string(MEDIA_EPUB).is_ok());
        // Text format types (PRIMARY naming)
        assert!(MediaUrn::from_string(MEDIA_MD).is_ok());
        assert!(MediaUrn::from_string(MEDIA_TXT).is_ok());
        assert!(MediaUrn::from_string(MEDIA_RST).is_ok());
        assert!(MediaUrn::from_string(MEDIA_LOG).is_ok());
        assert!(MediaUrn::from_string(MEDIA_HTML).is_ok());
        assert!(MediaUrn::from_string(MEDIA_XML).is_ok());
        assert!(MediaUrn::from_string(MEDIA_JSON).is_ok());
        assert!(MediaUrn::from_string(MEDIA_YAML).is_ok());
    }

    #[test]
    fn test_extension_helpers() {
        // Test binary_media_urn_for_ext
        let pdf_urn = binary_media_urn_for_ext("pdf");
        // Extension helper creates URN with ext= tag
        assert!(pdf_urn.contains("ext=pdf"));
        let parsed = MediaUrn::from_string(&pdf_urn).unwrap();
        assert_eq!(parsed.extension(), Some("pdf"));

        // Test text_media_urn_for_ext
        let md_urn = text_media_urn_for_ext("md");
        // Extension helper creates URN with ext= tag
        assert!(md_urn.contains("ext=md"));
        let parsed = MediaUrn::from_string(&md_urn).unwrap();
        assert_eq!(parsed.extension(), Some("md"));
    }

    #[test]
    fn test_satisfies() {
        // PDF listing satisfies PDF requirement (PRIMARY type naming)
        let pdf_listing = MediaUrn::from_string(MEDIA_PDF).unwrap(); // "media:pdf;binary"
        let pdf_requirement = MediaUrn::from_string("media:pdf").unwrap();
        assert!(pdf_listing.satisfies(&pdf_requirement));

        // Markdown listing satisfies md requirement (PRIMARY type naming)
        let md_listing = MediaUrn::from_string(MEDIA_MD).unwrap(); // "media:md;textable"
        let md_requirement = MediaUrn::from_string("media:md").unwrap();
        assert!(md_listing.satisfies(&md_requirement));

        // Same URNs should satisfy each other
        let string_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let string_req = MediaUrn::from_string(MEDIA_STRING).unwrap();
        assert!(string_urn.satisfies(&string_req));
    }

    #[test]
    fn test_matching() {
        let handler = MediaUrn::from_string("media:string").unwrap();
        let request = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.matches(&request).unwrap());

        // Handler with fewer tags can handle more requests (implicit wildcards)
        let general_handler = MediaUrn::from_string("media:string").unwrap();
        assert!(general_handler.matches(&request).unwrap());

        // Same URN should match
        let same = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.matches(&same).unwrap());
    }

    #[test]
    fn test_specificity() {
        // More tags = higher specificity
        let urn1 = MediaUrn::from_string("media:string").unwrap();
        let urn2 = MediaUrn::from_string("media:string;textable").unwrap();
        let urn3 = MediaUrn::from_string("media:string;textable;scalar").unwrap();

        // Verify specificity increases with more tags
        // Note: The exact values may depend on implementation, but relative order should hold
        let s1 = urn1.specificity();
        let s2 = urn2.specificity();
        let s3 = urn3.specificity();

        // At minimum, more tags should not have less specificity
        assert!(s2 >= s1, "urn2 ({}) should have >= specificity than urn1 ({})", s2, s1);
        assert!(s3 >= s2, "urn3 ({}) should have >= specificity than urn2 ({})", s3, s2);
    }

    #[test]
    fn test_serde_roundtrip() {
        let urn = MediaUrn::from_string("media:string").unwrap();
        let json = serde_json::to_string(&urn).unwrap();
        assert_eq!(json, "\"media:string\"");
        let urn2: MediaUrn = serde_json::from_str(&json).unwrap();
        assert_eq!(urn, urn2);
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;
    use crate::standard::media::{MEDIA_BINARY, MEDIA_STRING, MEDIA_OBJECT};

    #[test]
    fn debug_matching_behavior() {
        println!("MEDIA_BINARY = {}", MEDIA_BINARY);
        println!("MEDIA_STRING = {}", MEDIA_STRING);
        println!("MEDIA_OBJECT = {}", MEDIA_OBJECT);
        
        let str_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let obj_urn = MediaUrn::from_string(MEDIA_OBJECT).unwrap();
        let bin_urn = MediaUrn::from_string(MEDIA_BINARY).unwrap();
        
        println!("string.matches(string) = {:?}", str_urn.matches(&str_urn));
        println!("object.matches(string) = {:?}", obj_urn.matches(&str_urn));
        println!("object.matches(object) = {:?}", obj_urn.matches(&obj_urn));
        println!("string.matches(object) = {:?}", str_urn.matches(&obj_urn));
        
        println!("string.satisfies(string) = {}", str_urn.satisfies(&str_urn));
        println!("object.satisfies(string) = {}", obj_urn.satisfies(&str_urn));
        
        // The failing test: MEDIA_OBJECT should NOT satisfy MEDIA_STRING
        assert!(!obj_urn.satisfies(&str_urn), "MEDIA_OBJECT should NOT satisfy MEDIA_STRING");
    }
}
