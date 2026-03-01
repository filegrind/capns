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
//
// Cardinality and Structure use orthogonal marker tags:
// - `list` marker: presence = list/array, absence = scalar (default)
// - `record` marker: presence = has internal fields, absence = opaque (default)
//
// Examples:
// - `media:pdf` → scalar, opaque (no markers)
// - `media:textable;list` → list, opaque (has list marker)
// - `media:json;textable;record` → scalar, record (has record marker)
// - `media:json;list;record;textable` → list of records (has both markers)

// Primitive types - URNs must match base.toml definitions
/// Media URN for void (no input/output) - no coercion tags
pub const MEDIA_VOID: &str = "media:void";
/// Media URN for string type - textable (can become text), scalar by default (no list marker)
pub const MEDIA_STRING: &str = "media:textable";
/// Media URN for integer type - textable, numeric (math ops valid), scalar by default
pub const MEDIA_INTEGER: &str = "media:integer;textable;numeric";
/// Media URN for number type - textable, numeric, scalar by default
pub const MEDIA_NUMBER: &str = "media:textable;numeric";
/// Media URN for boolean type - uses "bool" not "boolean" per base.toml
pub const MEDIA_BOOLEAN: &str = "media:bool;textable";
/// Media URN for a generic record/object type - has internal key-value structure but NOT textable
/// Use MEDIA_JSON for textable JSON objects.
pub const MEDIA_OBJECT: &str = "media:record";
/// Media URN for binary data - the most general media type (no constraints)
pub const MEDIA_BINARY: &str = "media:";

// Array types - URNs must match base.toml definitions
/// Media URN for string array type - textable with list marker
pub const MEDIA_STRING_ARRAY: &str = "media:list;textable";
/// Media URN for integer array type - textable, numeric with list marker
pub const MEDIA_INTEGER_ARRAY: &str = "media:integer;list;textable;numeric";
/// Media URN for number array type - textable, numeric with list marker
pub const MEDIA_NUMBER_ARRAY: &str = "media:list;textable;numeric";
/// Media URN for boolean array type - uses "bool" with list marker
pub const MEDIA_BOOLEAN_ARRAY: &str = "media:bool;list;textable";
/// Media URN for object array type - list of records (NOT textable)
/// Use a specific format like JSON array for textable object arrays.
pub const MEDIA_OBJECT_ARRAY: &str = "media:list;record";

// Semantic media types for specialized content
/// Media URN for PNG image data
pub const MEDIA_PNG: &str = "media:image;png";
/// Media URN for audio data (wav, mp3, flac, etc.)
pub const MEDIA_AUDIO: &str = "media:wav;audio";
/// Media URN for video data (mp4, webm, mov, etc.)
pub const MEDIA_VIDEO: &str = "media:video";

// Semantic AI input types - distinguished by their purpose/context
/// Media URN for audio input containing speech for transcription (Whisper)
pub const MEDIA_AUDIO_SPEECH: &str = "media:audio;wav;speech";
/// Media URN for thumbnail image output
pub const MEDIA_IMAGE_THUMBNAIL: &str = "media:image;png;thumbnail";

// Document types (PRIMARY naming - type IS the format)
/// Media URN for PDF documents
pub const MEDIA_PDF: &str = "media:pdf";
/// Media URN for EPUB documents
pub const MEDIA_EPUB: &str = "media:epub";

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
/// Media URN for JSON data - has record marker (structured key-value)
pub const MEDIA_JSON: &str = "media:json;record;textable";
/// Media URN for JSON with schema constraint (input for structured queries)
pub const MEDIA_JSON_SCHEMA: &str = "media:json;json-schema;record;textable";
/// Media URN for YAML data - has record marker (structured key-value)
pub const MEDIA_YAML: &str = "media:record;textable;yaml";

// File path types - for arguments that represent filesystem paths
/// Media URN for a single file path - textable, scalar by default (no list marker)
pub const MEDIA_FILE_PATH: &str = "media:file-path;textable";
/// Media URN for an array of file paths - textable with list marker
pub const MEDIA_FILE_PATH_ARRAY: &str = "media:file-path;list;textable";

// Semantic text input types - distinguished by their purpose/context
/// Media URN for frontmatter text (book metadata) - scalar by default
pub const MEDIA_FRONTMATTER_TEXT: &str = "media:frontmatter;textable";
/// Media URN for model spec (provider:model format, HuggingFace name, etc.) - scalar by default
pub const MEDIA_MODEL_SPEC: &str = "media:model-spec;textable";
/// Media URN for MLX model path - scalar by default
pub const MEDIA_MLX_MODEL_PATH: &str = "media:mlx-model-path;textable";
/// Media URN for model repository (input for list-models) - has record marker
pub const MEDIA_MODEL_REPO: &str = "media:model-repo;record;textable";

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
    format!("media:image;ext={}", ext)
}

/// Helper to build audio media URN with extension
pub fn audio_media_urn_for_ext(ext: &str) -> String {
    format!("media:audio;ext={}", ext)
}

// CAPDAG output types - record marker for structured JSON objects, list marker for arrays
/// Media URN for model dimension output - scalar by default (no list marker)
pub const MEDIA_MODEL_DIM: &str = "media:integer;model-dim;numeric;textable";
/// Media URN for model download output - has record marker
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:download-result;record;textable";
/// Media URN for model list output - has record marker
pub const MEDIA_LIST_OUTPUT: &str = "media:model-list;record;textable";
/// Media URN for model status output - has record marker
pub const MEDIA_STATUS_OUTPUT: &str = "media:model-status;record;textable";
/// Media URN for model contents output - has record marker
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:model-contents;record;textable";
/// Media URN for model availability output - has record marker
pub const MEDIA_AVAILABILITY_OUTPUT: &str = "media:model-availability;record;textable";
/// Media URN for model path output - has record marker
pub const MEDIA_PATH_OUTPUT: &str = "media:model-path;record;textable";
/// Media URN for embedding vector output - has record marker
pub const MEDIA_EMBEDDING_VECTOR: &str = "media:embedding-vector;record;textable";
/// Media URN for LLM inference output - has record marker
pub const MEDIA_LLM_INFERENCE_OUTPUT: &str = "media:generated-text;record;textable";
/// Media URN for extracted metadata - has record marker
pub const MEDIA_FILE_METADATA: &str = "media:file-metadata;record;textable";
/// Media URN for extracted outline - has record marker
pub const MEDIA_DOCUMENT_OUTLINE: &str = "media:document-outline;record;textable";
/// Media URN for disbound page - has list marker (array of page objects)
pub const MEDIA_DISBOUND_PAGE: &str = "media:disbound-page;list;textable";
/// Media URN for vision inference output - textable, scalar by default
pub const MEDIA_IMAGE_DESCRIPTION: &str = "media:image-description;textable";
/// Media URN for transcription output - has record marker
pub const MEDIA_TRANSCRIPTION_OUTPUT: &str = "media:record;textable;transcription";
/// Media URN for decision output (bit choice) - scalar by default
pub const MEDIA_DECISION: &str = "media:bool;decision;textable";
/// Media URN for decision array output (bit choices) - has list marker
pub const MEDIA_DECISION_ARRAY: &str = "media:bool;decision;list;textable";

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

    /// Check if two media URNs have the exact same tag set (order-independent).
    /// Equivalent to `self.accepts(other) && other.accepts(self)`.
    pub fn is_equivalent(&self, other: &MediaUrn) -> Result<bool, MediaUrnError> {
        self.0.is_equivalent(&other.0).map_err(MediaUrnError::Match)
    }

    /// String variant of `is_equivalent`.
    pub fn is_equivalent_str(&self, other_str: &str) -> Result<bool, MediaUrnError> {
        let other = MediaUrn::from_string(other_str)?;
        self.is_equivalent(&other)
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

    /// Check if this represents binary (non-text) data.
    /// Returns true if the "textable" marker tag is NOT present.
    /// All data is binary at the byte level; textable is the subset
    /// that is natively representable as human-readable unicode text.
    pub fn is_binary(&self) -> bool {
        self.get_tag("textable").is_none()
    }

    // =========================================================================
    // CARDINALITY (list marker)
    // =========================================================================

    /// Returns true if this media is a list (has `list` marker tag).
    /// Returns false if scalar (no `list` marker = default).
    pub fn is_list(&self) -> bool {
        self.has_marker_tag("list")
    }

    /// Returns true if this media is a scalar (no `list` marker).
    /// Scalar is the default cardinality.
    pub fn is_scalar(&self) -> bool {
        !self.has_marker_tag("list")
    }

    // =========================================================================
    // STRUCTURE (record marker)
    // =========================================================================

    /// Returns true if this media is a record (has `record` marker tag).
    /// A record has internal key-value structure (e.g., JSON object).
    pub fn is_record(&self) -> bool {
        self.has_marker_tag("record")
    }

    /// Returns true if this media is opaque (no `record` marker).
    /// Opaque is the default structure - no internal fields recognized.
    pub fn is_opaque(&self) -> bool {
        !self.has_marker_tag("record")
    }

    // =========================================================================
    // HELPER: Check for marker tag presence
    // =========================================================================

    /// Check if a marker tag (tag with wildcard/no value) is present.
    /// A marker tag is stored as key="*" in the tagged URN.
    fn has_marker_tag(&self, tag_name: &str) -> bool {
        self.0.tags.get(tag_name).map_or(false, |v| v == "*")
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

    /// Check if this represents a single file path type (not array).
    /// Returns true if the "file-path" marker tag is present AND no list marker.
    pub fn is_file_path(&self) -> bool {
        self.has_marker_tag("file-path") && !self.is_list()
    }

    /// Check if this represents a file path array type.
    /// Returns true if the "file-path" marker tag is present AND has list marker.
    pub fn is_file_path_array(&self) -> bool {
        self.has_marker_tag("file-path") && self.is_list()
    }

    /// Check if this represents any file path type (single or array).
    /// Returns true if "file-path" marker tag is present.
    pub fn is_any_file_path(&self) -> bool {
        self.has_marker_tag("file-path")
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
    fn test060_wrong_prefix_fails() {
        let result = MediaUrn::from_string("cap:string");
        assert!(result.is_err());
        if let Err(MediaUrnError::InvalidPrefix { expected, actual }) = result {
            assert_eq!(expected, "media");
            assert_eq!(actual, "cap");
        } else {
            panic!("expected InvalidPrefix error");
        }
    }

    // TEST061: Test is_binary returns true when textable tag is absent (binary = not textable)
    #[test]
    fn test061_is_binary() {
        // Binary types: no textable tag
        assert!(MediaUrn::from_string(MEDIA_BINARY).unwrap().is_binary()); // "media:"
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_binary()); // "media:image;png"
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_binary()); // "media:pdf"
        assert!(MediaUrn::from_string("media:video").unwrap().is_binary());
        assert!(MediaUrn::from_string("media:epub").unwrap().is_binary());
        // Textable types: is_binary is false
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:textable;record").unwrap().is_binary());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_binary());
        assert!(!MediaUrn::from_string(MEDIA_JSON).unwrap().is_binary());
        assert!(!MediaUrn::from_string(MEDIA_MD).unwrap().is_binary());
    }

    // TEST062: Test is_record returns true when record marker tag is present indicating key-value structure
    #[test]
    fn test062_is_record() {
        // is_record returns true if record marker tag is present (key-value structure)
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_record()); // "media:record;textable"
        assert!(MediaUrn::from_string("media:custom;record").unwrap().is_record());
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_record()); // "media:json;record;textable"
        // Without record marker, is_record is false
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_record());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_record()); // scalar, no record marker
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_record()); // list, no record marker
    }

    // TEST063: Test is_scalar returns true when list marker tag is absent (scalar is default)
    #[test]
    fn test063_is_scalar() {
        // is_scalar returns true if NO list marker (scalar is default cardinality)
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_scalar()); // "media:textable" - no list marker
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_NUMBER).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_scalar()); // no list marker
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_scalar()); // record but scalar
        assert!(MediaUrn::from_string("media:textable").unwrap().is_scalar()); // plain textable is scalar
        // With list marker, is_scalar is false
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_scalar()); // has list marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_scalar()); // has list marker
    }

    // TEST064: Test is_list returns true when list marker tag is present indicating ordered collection
    #[test]
    fn test064_is_list() {
        // is_list returns true if list marker tag is present (ordered collection)
        assert!(MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_list()); // "media:list;textable"
        assert!(MediaUrn::from_string(MEDIA_INTEGER_ARRAY).unwrap().is_list()); // has list marker
        assert!(MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_list()); // "media:list;record;textable"
        assert!(MediaUrn::from_string("media:custom;list").unwrap().is_list());
        // Without list marker, is_list is false
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_list()); // no list marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_list()); // record but no list marker
    }

    // TEST065: Test is_opaque returns true when record marker is absent (opaque is default)
    #[test]
    fn test065_is_opaque() {
        // is_opaque returns true if NO record marker (opaque is default structure)
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_opaque()); // no record marker
        assert!(MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_opaque()); // list but no record
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_opaque()); // binary, no record
        assert!(MediaUrn::from_string("media:textable").unwrap().is_opaque()); // no record marker
        // With record marker, is_opaque is false
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_opaque()); // has record marker
        assert!(!MediaUrn::from_string(MEDIA_JSON).unwrap().is_opaque()); // has record marker
        assert!(!MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_opaque()); // has record marker
    }

    // TEST066: Test is_json returns true only when json marker tag is present for JSON representation
    #[test]
    fn test066_is_json() {
        // is_json returns true only if "json" marker tag is present (JSON representation)
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_json()); // "media:json;textable"
        assert!(MediaUrn::from_string("media:custom;json").unwrap().is_json());
        // record alone does not mean JSON representation
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_json()); // map structure, not necessarily JSON
        assert!(!MediaUrn::from_string("media:textable").unwrap().is_json());
    }

    // TEST067: Test is_text returns true only when textable marker tag is present
    #[test]
    fn test067_is_text() {
        // is_text returns true only if "textable" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_text()); // "media:textable"
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_text()); // "media:integer;textable;numeric"
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_text()); // "media:json;record;textable"
        // Without textable tag, is_text is false
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_text()); // "media:"
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_text()); // "media:image;png"
        assert!(!MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_text()); // "media:record" (no textable)
    }

    // TEST068: Test is_void returns true when void flag or type=void tag is present
    #[test]
    fn test068_is_void() {
        assert!(MediaUrn::from_string("media:void").unwrap().is_void());
        assert!(!MediaUrn::from_string("media:string").unwrap().is_void());
    }

    // TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure
    #[test]
    fn test071_to_string_roundtrip() {
        let original = "media:string";
        let urn = MediaUrn::from_string(original).unwrap();
        let s = urn.to_string();
        let urn2 = MediaUrn::from_string(&s).unwrap();
        assert_eq!(urn, urn2);
    }

    // TEST072: Test all media URN constants parse successfully as valid media URNs
    #[test]
    fn test072_constants_parse() {
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
    fn test073_extension_helpers() {
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
    fn test074_media_urn_matching() {
        // PDF listing conforms to PDF requirement (PRIMARY type naming)
        // A more specific URN (media:pdf) conforms to a less specific requirement (media:pdf)
        let pdf_listing = MediaUrn::from_string(MEDIA_PDF).unwrap(); // "media:pdf"
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
    fn test075_matching() {
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
    fn test076_specificity() {
        // More tags = higher specificity
        let urn1 = MediaUrn::from_string("media:string").unwrap();
        let urn2 = MediaUrn::from_string("media:textable").unwrap();
        let urn3 = MediaUrn::from_string("media:textable;numeric").unwrap();

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
    fn test077_serde_roundtrip() {
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

        // MEDIA_OBJECT (media:record) should NOT conform to MEDIA_STRING (media:textable)
        // because MEDIA_OBJECT lacks textable - records are not inherently textable.
        assert!(
            !obj_urn.conforms_to(&str_urn).expect("MediaUrn prefix mismatch impossible"),
            "MEDIA_OBJECT should NOT conform to MEDIA_STRING (missing textable)"
        );
    }

    // TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test304_media_availability_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_AVAILABILITY_OUTPUT).expect("must parse");
        assert!(urn.is_text(), "model-availability must be textable");
        assert!(urn.is_record(), "model-availability must have record marker");
        assert!(!urn.is_binary(), "model-availability must not be binary");
        // to_string() alphabetizes tags, so compare via roundtrip parsing instead
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(urn.conforms_to(&reparsed).unwrap(), "roundtrip must conform to original");
    }

    // TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags
    #[test]
    fn test305_media_path_output_constant() {
        let urn = MediaUrn::from_string(MEDIA_PATH_OUTPUT).expect("must parse");
        assert!(urn.is_text(), "model-path must be textable");
        assert!(urn.is_record(), "model-path must have record marker");
        assert!(!urn.is_binary(), "model-path must not be binary");
        let reparsed = MediaUrn::from_string(&urn.to_string()).expect("roundtrip must parse");
        assert!(urn.conforms_to(&reparsed).unwrap(), "roundtrip must conform to original");
    }

    // TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs
    #[test]
    fn test306_availability_and_path_output_distinct() {
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

    // TEST546: is_image returns true only when image marker tag is present
    #[test]
    fn test546_is_image() {
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_image());
        assert!(MediaUrn::from_string(MEDIA_IMAGE_THUMBNAIL).unwrap().is_image());
        assert!(MediaUrn::from_string("media:image;jpg").unwrap().is_image());
        // Non-image types
        assert!(!MediaUrn::from_string(MEDIA_PDF).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_image());
        assert!(!MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_image());
    }

    // TEST547: is_audio returns true only when audio marker tag is present
    #[test]
    fn test547_is_audio() {
        assert!(MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_audio());
        assert!(MediaUrn::from_string(MEDIA_AUDIO_SPEECH).unwrap().is_audio());
        assert!(MediaUrn::from_string("media:audio;mp3").unwrap().is_audio());
        // Non-audio types
        assert!(!MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_audio());
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_audio());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_audio());
    }

    // TEST548: is_video returns true only when video marker tag is present
    #[test]
    fn test548_is_video() {
        assert!(MediaUrn::from_string(MEDIA_VIDEO).unwrap().is_video());
        assert!(MediaUrn::from_string("media:video;mp4").unwrap().is_video());
        // Non-video types
        assert!(!MediaUrn::from_string(MEDIA_AUDIO).unwrap().is_video());
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_video());
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_video());
    }

    // TEST549: is_numeric returns true only when numeric marker tag is present
    #[test]
    fn test549_is_numeric() {
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_numeric());
        assert!(MediaUrn::from_string(MEDIA_NUMBER).unwrap().is_numeric());
        assert!(MediaUrn::from_string(MEDIA_INTEGER_ARRAY).unwrap().is_numeric());
        assert!(MediaUrn::from_string(MEDIA_NUMBER_ARRAY).unwrap().is_numeric());
        // Non-numeric types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_numeric());
        assert!(!MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_numeric());
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_numeric());
    }

    // TEST550: is_bool returns true only when bool marker tag is present
    #[test]
    fn test550_is_bool() {
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN).unwrap().is_bool());
        assert!(MediaUrn::from_string(MEDIA_BOOLEAN_ARRAY).unwrap().is_bool());
        assert!(MediaUrn::from_string(MEDIA_DECISION).unwrap().is_bool());
        assert!(MediaUrn::from_string(MEDIA_DECISION_ARRAY).unwrap().is_bool());
        // Non-bool types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_bool());
        assert!(!MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_bool());
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_bool());
    }

    // TEST551: is_file_path returns true for scalar file-path, false for array
    #[test]
    fn test551_is_file_path() {
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH).unwrap().is_file_path());
        // Array file-path is NOT is_file_path (it's is_file_path_array)
        assert!(!MediaUrn::from_string(MEDIA_FILE_PATH_ARRAY).unwrap().is_file_path());
        // Non-file-path types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_file_path());
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_file_path());
    }

    // TEST552: is_file_path_array returns true for list file-path, false for scalar
    #[test]
    fn test552_is_file_path_array() {
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH_ARRAY).unwrap().is_file_path_array());
        // Scalar file-path is NOT is_file_path_array
        assert!(!MediaUrn::from_string(MEDIA_FILE_PATH).unwrap().is_file_path_array());
        // Non-file-path types
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_file_path_array());
    }

    // TEST553: is_any_file_path returns true for both scalar and array file-path
    #[test]
    fn test553_is_any_file_path() {
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH).unwrap().is_any_file_path());
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH_ARRAY).unwrap().is_any_file_path());
        // Non-file-path types
        assert!(!MediaUrn::from_string(MEDIA_STRING).unwrap().is_any_file_path());
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_any_file_path());
    }

    // TEST555: with_tag adds a tag and without_tag removes it
    #[test]
    fn test555_with_tag_and_without_tag() {
        let urn = MediaUrn::from_string("media:string").unwrap();
        let with_ext = urn.with_tag("ext", "pdf").unwrap();
        assert_eq!(with_ext.extension(), Some("pdf"));
        // Original unchanged
        assert_eq!(urn.extension(), None);

        // Remove the tag
        let without_ext = with_ext.without_tag("ext");
        assert_eq!(without_ext.extension(), None);
        // Removing non-existent tag is a no-op
        let same = urn.without_tag("nonexistent");
        assert_eq!(same, urn);
    }

    // TEST556: image_media_urn_for_ext creates valid image media URN
    #[test]
    fn test556_image_media_urn_for_ext() {
        let jpg_urn = image_media_urn_for_ext("jpg");
        let parsed = MediaUrn::from_string(&jpg_urn).unwrap();
        assert!(parsed.is_image(), "image helper must set image tag");
        assert!(parsed.is_binary(), "image URN must be binary (no textable tag)");
        assert_eq!(parsed.extension(), Some("jpg"));
    }

    // TEST557: audio_media_urn_for_ext creates valid audio media URN
    #[test]
    fn test557_audio_media_urn_for_ext() {
        let mp3_urn = audio_media_urn_for_ext("mp3");
        let parsed = MediaUrn::from_string(&mp3_urn).unwrap();
        assert!(parsed.is_audio(), "audio helper must set audio tag");
        assert!(parsed.is_binary(), "audio URN must be binary (no textable tag)");
        assert_eq!(parsed.extension(), Some("mp3"));
    }

    // TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates
    #[test]
    fn test558_predicate_constant_consistency() {
        // MEDIA_INTEGER must be numeric, text, scalar, NOT binary/bool/image/audio/video
        let int = MediaUrn::from_string(MEDIA_INTEGER).unwrap();
        assert!(int.is_numeric());
        assert!(int.is_text());
        assert!(int.is_scalar());
        assert!(!int.is_binary());
        assert!(!int.is_bool());
        assert!(!int.is_image());
        assert!(!int.is_list());

        // MEDIA_BOOLEAN must be bool, text, scalar, NOT numeric
        let bool_urn = MediaUrn::from_string(MEDIA_BOOLEAN).unwrap();
        assert!(bool_urn.is_bool());
        assert!(bool_urn.is_text());
        assert!(bool_urn.is_scalar());
        assert!(!bool_urn.is_numeric());

        // MEDIA_JSON must be json, text, record, scalar, NOT binary
        let json_urn = MediaUrn::from_string(MEDIA_JSON).unwrap();
        assert!(json_urn.is_json());
        assert!(json_urn.is_text());
        assert!(json_urn.is_record());
        assert!(json_urn.is_scalar(), "MEDIA_JSON is a scalar record (single object)");
        assert!(!json_urn.is_binary());
        assert!(!json_urn.is_list());

        // MEDIA_VOID is void, NOT text/numeric — but IS binary (no textable tag)
        let void = MediaUrn::from_string(MEDIA_VOID).unwrap();
        assert!(void.is_void());
        assert!(!void.is_text());
        assert!(void.is_binary(), "void has no textable tag, so is_binary is true");
        assert!(!void.is_numeric());
    }
}
