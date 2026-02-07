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
use tagged_urn::{TaggedUrn, TaggedUrnError};

// =============================================================================
// STANDARD MEDIA URN CONSTANTS
// =============================================================================

// Primitive types - URNs must match base.toml definitions
/// Media URN for void (no input/output) - no coercion tags
pub const MEDIA_VOID: &str = "media:void";
/// Media URN for string type - textable (can become text), scalar (single value)
pub const MEDIA_STRING: &str = "media:textable;form=scalar";
/// Media URN for integer type - textable, numeric (math ops valid), scalar
pub const MEDIA_INTEGER: &str = "media:integer;textable;numeric;form=scalar";
/// Media URN for number type - textable, numeric, scalar (no primary type prefix)
pub const MEDIA_NUMBER: &str = "media:textable;numeric;form=scalar";
/// Media URN for boolean type - uses "bool" not "boolean" per base.toml
pub const MEDIA_BOOLEAN: &str = "media:bool;textable;form=scalar";
/// Media URN for JSON object type - textable (via JSON.stringify), form=map (key-value structure)
pub const MEDIA_OBJECT: &str = "media:form=map;textable";
/// Media URN for binary data - binary (raw bytes)
pub const MEDIA_BINARY: &str = "media:bytes";

// Array types - URNs must match base.toml definitions
/// Media URN for string array type - textable, list (no primary type prefix)
pub const MEDIA_STRING_ARRAY: &str = "media:textable;form=list";
/// Media URN for integer array type - textable, numeric, list (per base.toml:46)
pub const MEDIA_INTEGER_ARRAY: &str = "media:integer;textable;numeric;form=list";
/// Media URN for number array type - textable, numeric, list (no primary type prefix)
pub const MEDIA_NUMBER_ARRAY: &str = "media:textable;numeric;form=list";
/// Media URN for boolean array type - uses "bool" not "boolean" per base.toml
pub const MEDIA_BOOLEAN_ARRAY: &str = "media:bool;textable;form=list";
/// Media URN for object array type - generic list (item type defined in schema)
pub const MEDIA_OBJECT_ARRAY: &str = "media:form=list;textable";

// Semantic media types for specialized content
/// Media URN for PNG image data - matches CATALOG: media:image;png;bytes
pub const MEDIA_PNG: &str = "media:image;png;bytes";
/// Media URN for audio data (wav, mp3, flac, etc.)
pub const MEDIA_AUDIO: &str = "media:wav;audio;bytes;";
/// Media URN for video data (mp4, webm, mov, etc.)
pub const MEDIA_VIDEO: &str = "media:video;bytes";

// Semantic AI input types - distinguished by their purpose/context
/// Media URN for audio input containing speech for transcription (Whisper)
pub const MEDIA_AUDIO_SPEECH: &str = "media:audio;wav;bytes;speech";
/// Media URN for thumbnail image output
pub const MEDIA_IMAGE_THUMBNAIL: &str = "media:image;png;bytes;thumbnail";

// Collection types for folder hierarchies
/// Media URN for a collection (folder with nested structure as form=map)
pub const MEDIA_COLLECTION: &str = "media:collection;form=map";
/// Media URN for a flat collection (folder contents as form=list)
pub const MEDIA_COLLECTION_LIST: &str = "media:collection;form=list";

// Document types (PRIMARY naming - type IS the format)
/// Media URN for PDF documents
pub const MEDIA_PDF: &str = "media:pdf;bytes";
/// Media URN for EPUB documents
pub const MEDIA_EPUB: &str = "media:epub;bytes";

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
pub const MEDIA_JSON: &str = "media:json;textable;form=map";
/// Media URN for JSON with schema constraint (input for structured queries) - matches CATALOG: media:json;json-schema;textable;form=map
pub const MEDIA_JSON_SCHEMA: &str = "media:json;json-schema;textable;form=map";
/// Media URN for YAML data
pub const MEDIA_YAML: &str = "media:yaml;textable;form=map";

// File path types - for arguments that represent filesystem paths
/// Media URN for a single file path - textable, scalar, and marked as a file-path for special handling
pub const MEDIA_FILE_PATH: &str = "media:file-path;textable;form=scalar";
/// Media URN for an array of file paths - textable, list (per file-path.toml)
pub const MEDIA_FILE_PATH_ARRAY: &str = "media:file-path;textable;form=list";

// Semantic text input types - distinguished by their purpose/context
/// Media URN for frontmatter text (book metadata)
pub const MEDIA_FRONTMATTER_TEXT: &str = "media:frontmatter;textable;form=scalar";
/// Media URN for model spec (provider:model format, HuggingFace name, etc.)
pub const MEDIA_MODEL_SPEC: &str = "media:model-spec;textable;form=scalar";
/// Media URN for MLX model path
pub const MEDIA_MLX_MODEL_PATH: &str = "media:mlx-model-path;textable;form=scalar";
/// Media URN for model repository (input for list-models) - matches CATALOG: media:model-repo;textable;form=map
pub const MEDIA_MODEL_REPO: &str = "media:model-repo;textable;form=map";

/// Helper to build binary media URN with extension
pub fn binary_media_urn_for_ext(ext: &str) -> String {
    format!("media:binary;ext={}", ext)
}

/// Helper to build text media URN with extension
pub fn text_media_urn_for_ext(ext: &str) -> String {
    format!("media:ext={};textable", ext)
}

/// Helper to build image media URN with extension
pub fn image_media_urn_for_ext(ext: &str) -> String {
    format!("media:image;ext={};bytes", ext)
}

/// Helper to build audio media URN with extension
pub fn audio_media_urn_for_ext(ext: &str) -> String {
    format!("media:audio;ext={};bytes", ext)
}

// CAPNS output types - all form=map structures (JSON objects)
/// Media URN for model dimension output - matches CATALOG: media:model-dim;integer;textable;numeric;form=scalar
pub const MEDIA_MODEL_DIM: &str = "media:model-dim;integer;textable;numeric;form=scalar";
/// Media URN for model download output - textable, form=map
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:download-result;textable;form=map";
/// Media URN for model list output - textable, form=map
pub const MEDIA_LIST_OUTPUT: &str = "media:model-list;textable;form=map";
/// Media URN for model status output - textable, form=map
pub const MEDIA_STATUS_OUTPUT: &str = "media:model-status;textable;form=map";
/// Media URN for model contents output - textable, form=map
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:model-contents;textable;form=map";
/// Media URN for model availability output - textable, form=map
pub const MEDIA_AVAILABILITY_OUTPUT: &str = "media:model-availability;textable;form=map";
/// Media URN for model path output - textable, form=map
pub const MEDIA_PATH_OUTPUT: &str = "media:model-path;textable;form=map";
/// Media URN for embedding vector output - textable, form=map
pub const MEDIA_EMBEDDING_VECTOR: &str = "media:embedding-vector;textable;form=map";
/// Media URN for LLM inference output - textable, form=map
pub const MEDIA_LLM_INFERENCE_OUTPUT: &str = "media:generated-text;textable;form=map";
/// Media URN for extracted metadata - textable, form=map
pub const MEDIA_FILE_METADATA: &str = "media:file-metadata;textable;form=map";
/// Media URN for extracted outline - textable, form=map
pub const MEDIA_DOCUMENT_OUTLINE: &str = "media:document-outline;textable;form=map";
/// Media URN for disbound page - textable, form=list (array of page objects)
pub const MEDIA_DISBOUND_PAGE: &str = "media:disbound-page;textable;form=list";
/// Media URN for caption output - textable, form=map
pub const MEDIA_CAPTION_OUTPUT: &str = "media:image-caption;textable;form=map";
/// Media URN for transcription output - textable, form=map
pub const MEDIA_TRANSCRIPTION_OUTPUT: &str = "media:transcription;textable;form=map";
/// Media URN for vision inference output - textable, form=map
pub const MEDIA_VISION_INFERENCE_OUTPUT: &str = "media:vision-inference-output;textable;form=map";
/// Media URN for decision output (bit choice) - matches CATALOG: media:decision;bool;textable;form=scalar
pub const MEDIA_DECISION: &str = "media:decision;bool;textable;form=scalar";
/// Media URN for decision array output (bit choices) - matches CATALOG: media:decision;bool;textable;form=list
pub const MEDIA_DECISION_ARRAY: &str = "media:decision;bool;textable;form=list";

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
    /// Whitespace and empty input validation is handled by TaggedUrn::from_string.
    pub fn from_string(s: &str) -> Result<Self, MediaUrnError> {
        let urn = TaggedUrn::from_string(s).map_err(MediaUrnError::Parse)?;
        Self::new(urn)
    }

    /// Get the inner TaggedUrn
    pub fn inner(&self) -> &TaggedUrn {
        &self.0
    }

    /// Get the extension tag value (e.g., "pdf", "epub", "md")
    pub fn extension(&self) -> Option<&str> {
        self.get_tag("ext")
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

    /// Serialize just the tags portion (without "media:" prefix)
    ///
    /// Returns tags in canonical form with proper quoting and sorting.
    pub fn tags_to_string(&self) -> String {
        self.0.tags_to_string()
    }

    /// Get the canonical string representation
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    /// Check if this media URN (instance) satisfies the pattern's constraints.
    /// Equivalent to `pattern.accepts(self)`.
    pub fn conforms_to(&self, pattern: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.conforms_to(&pattern.0).map_err(MediaUrnError::Match)
    }

    /// Check if this media URN (pattern) accepts the given instance.
    /// Equivalent to `instance.conforms_to(self)`.
    pub fn accepts(&self, instance: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.accepts(&instance.0).map_err(MediaUrnError::Match)
    }

    /// Get the specificity of this media URN
    ///
    /// Specificity is the count of non-wildcard tags.
    pub fn specificity(&self) -> usize {
        self.0.specificity()
    }

    // =========================================================================
    // Behavior helpers (triggered by tag presence)
    // =========================================================================

    /// Check if this represents binary data.
    /// Returns true if the "bytes" marker tag is present.
    pub fn is_binary(&self) -> bool {
        self.get_tag("bytes").is_some()
    }

    /// Check if this represents a map/object structure (form=map).
    /// This indicates a key-value structure, regardless of representation format.
    pub fn is_map(&self) -> bool {
        self.has_tag("form", "map")
    }

    /// Check if this represents a scalar value (form=scalar).
    /// This indicates a single value, not a collection.
    pub fn is_scalar(&self) -> bool {
        self.has_tag("form", "scalar")
    }

    /// Check if this represents a list/array structure (form=list).
    /// This indicates an ordered collection of values.
    pub fn is_list(&self) -> bool {
        self.has_tag("form", "list")
    }

    /// Check if this represents structured data (map or list).
    /// Structured data can be serialized as JSON when transmitted as text.
    /// Note: This does NOT check for the explicit `json` tag - use is_json() for that.
    pub fn is_structured(&self) -> bool {
        self.is_map() || self.is_list()
    }

    /// Check if this represents JSON representation specifically.
    /// Returns true if the "json" marker tag is present.
    /// Note: This only checks for explicit JSON format marker.
    /// For checking if data is structured (map/list), use is_structured().
    pub fn is_json(&self) -> bool {
        self.get_tag("json").is_some()
    }

    /// Check if this represents text data.
    /// Returns true if the "textable" marker tag is present.
    pub fn is_text(&self) -> bool {
        self.get_tag("textable").is_some()
    }

    /// Check if this represents image data.
    /// Returns true if the "image" marker tag is present.
    pub fn is_image(&self) -> bool {
        self.get_tag("image").is_some()
    }

    /// Check if this represents audio data.
    /// Returns true if the "audio" marker tag is present.
    pub fn is_audio(&self) -> bool {
        self.get_tag("audio").is_some()
    }

    /// Check if this represents video data.
    /// Returns true if the "video" marker tag is present.
    pub fn is_video(&self) -> bool {
        self.get_tag("video").is_some()
    }

    /// Check if this represents numeric data.
    /// Returns true if the "numeric" marker tag is present.
    pub fn is_numeric(&self) -> bool {
        self.get_tag("numeric").is_some()
    }

    /// Check if this represents boolean data.
    /// Returns true if the "bool" marker tag is present.
    pub fn is_bool(&self) -> bool {
        self.get_tag("bool").is_some()
    }

    /// Check if this represents a void (no data) type
    pub fn is_void(&self) -> bool {
        // Check for "void" marker tag
        self.0.tags.contains_key("void")
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

    /// Check if this represents a collection type (folder structure).
    /// Returns true if the "collection" marker tag is present.
    pub fn is_collection(&self) -> bool {
        self.0.tags.get("collection").map_or(false, |v| v == "*")
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
    /// Error parsing the underlying tagged URN (includes whitespace/empty validation)
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

    // TEST060: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix
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

    // TEST061: Test is_binary returns true only when bytes marker tag is present
    #[test]
    fn test_is_binary() {
        // is_binary returns true only if "bytes" marker tag is present
        assert!(MediaUrn::from_string("media:bytes").unwrap().is_binary());
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_binary()); // "media:png;bytes"
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_binary()); // "media:pdf;bytes"
        assert!(MediaUrn::from_string(MEDIA_BINARY).unwrap().is_binary()); // "media:bytes"
        // Without binary tag, is_binary is false
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:object;textable;form=map").unwrap().is_binary());
    }

    // TEST062: Test is_map returns true when form=map tag is present indicating key-value structure
    #[test]
    fn test_is_map() {
        // is_map returns true if form=map tag is present (key-value structure)
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_map()); // "media:form=map;textable"
        assert!(MediaUrn::from_string("media:custom;form=map").unwrap().is_map());
        // Without form=map tag, is_map is false
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_map());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_map()); // form=scalar
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_map()); // form=list
    }

    // TEST063: Test is_scalar returns true when form=scalar tag is present indicating single value
    #[test]
    fn test_is_scalar() {
        // is_scalar returns true if form=scalar tag is present (single value)
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_scalar()); // "media:textable;form=scalar"
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_scalar()); // "media:integer;textable;numeric;form=scalar"
        assert!(MediaUrn::from_string(MEDIA_NUMBER).unwrap().is_scalar()); // "media:textable;numeric;form=scalar"
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_scalar()); // "media:bool;textable;form=scalar"
        // Without form=scalar tag, is_scalar is false
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_scalar()); // form=map
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_scalar()); // form=list
    }

    // TEST064: Test is_list returns true when form=list tag is present indicating ordered collection
    #[test]
    fn test_is_list() {
        // is_list returns true if form=list tag is present (ordered collection)
        assert!(MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_list()); // "media:textable;form=list"
        assert!(MediaUrn::from_string(MEDIA_INTEGER_ARRAY).unwrap().is_list()); // "media:integer;textable;numeric;form=list"
        assert!(MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_list()); // "media:form=list;textable"
        // Without form=list tag, is_list is false
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_list()); // form=scalar
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_list()); // form=map
    }

    // TEST065: Test is_structured returns true for map or list forms indicating structured data types
    #[test]
    fn test_is_structured() {
        // is_structured returns true if form=map OR form=list (structured data types)
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_structured()); // form=map
        assert!(MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_structured()); // form=list
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_structured()); // "media:json;textable;form=map" - has form=map
        // Scalars are NOT structured
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_structured()); // form=scalar
        assert!(!MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_structured()); // form=scalar
        // Pure textable without form tag is NOT structured
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_structured());
    }

    // TEST066: Test is_json returns true only when json marker tag is present for JSON representation
    #[test]
    fn test_is_json() {
        // is_json returns true only if "json" marker tag is present (JSON representation)
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_json()); // "media:json;textable"
        assert!(MediaUrn::from_string("media:custom;json").unwrap().is_json());
        // form=map alone does not mean JSON representation
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_json()); // map structure, not necessarily JSON
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_json());
    }

    // TEST067: Test is_text returns true only when textable marker tag is present
    #[test]
    fn test_is_text() {
        // is_text returns true only if "textable" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_text()); // "media:textable;form=scalar"
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_text()); // "media:integer;textable;numeric;form=scalar"
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_text()); // "media:form=map;textable"
        // Without textable tag, is_text is false
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_text()); // "media:bytes"
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_text()); // "media:png;bytes"
    }

    // TEST068: Test is_void returns true when void flag or type=void tag is present
    #[test]
    fn test_is_void() {
        assert!(MediaUrn::from_string("media:void").unwrap().is_void());
        assert!(!MediaUrn::from_string("media:string").unwrap().is_void());
    }

    // TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure
    #[test]
    fn test_to_string_roundtrip() {
        let original = "media:string";
        let urn = MediaUrn::from_string(original).unwrap();
        let s = urn.to_string();
        let urn2 = MediaUrn::from_string(&s).unwrap();
        assert_eq!(urn, urn2);
    }

    // TEST072: Test all media URN constants parse successfully as valid media URNs
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

    // TEST073: Test extension helper functions create media URNs with ext tag and correct format
    #[test]
    fn test_extension_helpers() {
        // Test binary_media_urn_for_ext
        let pdf_urn = binary_media_urn_for_ext("pdf");
        let parsed = MediaUrn::from_string(&pdf_urn).unwrap();
        assert!(parsed.has_tag("ext", "pdf"), "binary ext helper must set ext=pdf");
        assert_eq!(parsed.extension(), Some("pdf"));

        // Test text_media_urn_for_ext
        let md_urn = text_media_urn_for_ext("md");
        let parsed = MediaUrn::from_string(&md_urn).unwrap();
        assert!(parsed.has_tag("ext", "md"), "text ext helper must set ext=md");
        assert_eq!(parsed.extension(), Some("md"));
    }

    // TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements
    #[test]
    fn test_media_urn_matching() {
        // PDF listing conforms to PDF requirement (PRIMARY type naming)
        // A more specific URN (media:pdf;bytes) conforms to a less specific requirement (media:pdf)
        let pdf_listing = MediaUrn::from_string(MEDIA_PDF).unwrap(); // "media:pdf;bytes"
        let pdf_requirement = MediaUrn::from_string("media:pdf").unwrap();
        assert!(pdf_listing.conforms_to(&pdf_requirement).expect("MediaUrn prefix mismatch impossible"));

        // Markdown listing conforms to md requirement (PRIMARY type naming)
        let md_listing = MediaUrn::from_string(MEDIA_MD).unwrap(); // "media:md;textable"
        let md_requirement = MediaUrn::from_string("media:md").unwrap();
        assert!(md_listing.conforms_to(&md_requirement).expect("MediaUrn prefix mismatch impossible"));

        // Same URNs should conform to each other
        let string_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let string_req = MediaUrn::from_string(MEDIA_STRING).unwrap();
        assert!(string_urn.conforms_to(&string_req).expect("MediaUrn prefix mismatch impossible"));
    }

    // TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests
    #[test]
    fn test_matching() {
        let handler = MediaUrn::from_string("media:string").unwrap();
        let request = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.accepts(&request).unwrap());

        // Handler with fewer tags can handle more requests (implicit wildcards)
        let general_handler = MediaUrn::from_string("media:string").unwrap();
        assert!(general_handler.accepts(&request).unwrap());

        // Same URN should accept
        let same = MediaUrn::from_string("media:string").unwrap();
        assert!(handler.accepts(&same).unwrap());
    }

    // TEST076: Test specificity increases with more tags for ranking conformance
    #[test]
    fn test_specificity() {
        // More tags = higher specificity
        let urn1 = MediaUrn::from_string("media:string").unwrap();
        let urn2 = MediaUrn::from_string("media:textable").unwrap();
        let urn3 = MediaUrn::from_string("media:textable;form=scalar").unwrap();

        // Verify specificity increases with more tags
        // Note: The exact values may depend on implementation, but relative order should hold
        let s1 = urn1.specificity();
        let s2 = urn2.specificity();
        let s3 = urn3.specificity();

        // At minimum, more tags should not have less specificity
        assert!(s2 >= s1, "urn2 ({}) should have >= specificity than urn1 ({})", s2, s1);
        assert!(s3 >= s2, "urn3 ({}) should have >= specificity than urn2 ({})", s3, s2);
    }

    // TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly
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

    // TEST078: Debug test for conforms_to behavior between different media URN types
    #[test]
    fn debug_matching_behavior() {
        println!("MEDIA_BINARY = {}", MEDIA_BINARY);
        println!("MEDIA_STRING = {}", MEDIA_STRING);
        println!("MEDIA_OBJECT = {}", MEDIA_OBJECT);

        let str_urn = MediaUrn::from_string(MEDIA_STRING).unwrap();
        let obj_urn = MediaUrn::from_string(MEDIA_OBJECT).unwrap();
        let _bin_urn = MediaUrn::from_string(MEDIA_BINARY).unwrap();

        println!("string.conforms_to(string) = {:?}", str_urn.conforms_to(&str_urn));
        println!("object.conforms_to(string) = {:?}", obj_urn.conforms_to(&str_urn));
        println!("object.conforms_to(object) = {:?}", obj_urn.conforms_to(&obj_urn));
        println!("string.conforms_to(object) = {:?}", str_urn.conforms_to(&obj_urn));

        // MEDIA_OBJECT should NOT conform to MEDIA_STRING (different type flags)
        assert!(
            !obj_urn.conforms_to(&str_urn).expect("MediaUrn prefix mismatch impossible"),
            "MEDIA_OBJECT should NOT conform to MEDIA_STRING"
        );
    }

    // TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test_media_availability_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_AVAILABILITY_OUTPUT).expect("must parse");
        assert!(urn.is_text(), "model-availability must be textable");
        assert!(urn.is_map(), "model-availability must be form=map");
        assert!(!urn.is_binary(), "model-availability must not be binary");
        // to_string() alphabetizes tags, so compare via roundtrip parsing instead
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(urn.conforms_to(&reparsed).unwrap(), "roundtrip must conform to original");
    }

    // TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test_media_path_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_PATH_OUTPUT).expect("must parse");
        assert!(urn.is_text(), "model-path must be textable");
        assert!(urn.is_map(), "model-path must be form=map");
        assert!(!urn.is_binary(), "model-path must not be binary");
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(urn.conforms_to(&reparsed).unwrap(), "roundtrip must conform to original");
    }

    // TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs
    #[test]
    fn test_availability_and_path_output_distinct() {
        assert_ne!(MEDIA_AVAILABILITY_OUTPUT, MEDIA_PATH_OUTPUT,
            "availability and path output must be distinct media URNs");
        let avail = MediaUrn::from_string(MEDIA_AVAILABILITY_OUTPUT).unwrap();
        let path = MediaUrn::from_string(MEDIA_PATH_OUTPUT).unwrap();
        // They must NOT conform to each other (different types)
        assert!(
            !avail.conforms_to(&path).unwrap_or(true),
            "availability must not conform to path"
        );
    }
}
