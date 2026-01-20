//! Media URN - Data type specification using tagged URN format
//!
//! Media URNs use the tagged URN format with "media" prefix to describe
//! data types. They replace the old spec ID system (e.g., `media:type=string;v=1`).
//!
//! Format: `media:type=<type>[;subtype=<subtype>][;v=<version>][;profile=<url>][;...]`
//!
//! Examples:
//! - `media:type=string;v=1`
//! - `media:type=object;v=1`
//! - `media:type=application;subtype=json;profile="https://example.com/schema"`
//! - `media:type=image;subtype=png`
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
pub const MEDIA_VOID: &str = "media:type=void;v=1";
/// Media URN for string type - textable (can become text), scalar (single value)
pub const MEDIA_STRING: &str = "media:type=string;v=1;textable;scalar";
/// Media URN for integer type - textable, numeric (math ops valid), scalar
pub const MEDIA_INTEGER: &str = "media:type=integer;v=1;textable;numeric;scalar";
/// Media URN for number type - textable, numeric, scalar
pub const MEDIA_NUMBER: &str = "media:type=number;v=1;textable;numeric;scalar";
/// Media URN for boolean type - textable, scalar
pub const MEDIA_BOOLEAN: &str = "media:type=boolean;v=1;textable;scalar";
/// Media URN for JSON object type - textable (via JSON.stringify), keyed (key-value structure)
pub const MEDIA_OBJECT: &str = "media:type=object;v=1;textable;keyed";
/// Media URN for binary data - binary (raw bytes)
pub const MEDIA_BINARY: &str = "media:type=raw;v=1;binary";

// Array types
/// Media URN for string array type - textable, sequence (ordered collection)
pub const MEDIA_STRING_ARRAY: &str = "media:type=string-array;v=1;textable;sequence";
/// Media URN for integer array type - textable, numeric, sequence
pub const MEDIA_INTEGER_ARRAY: &str = "media:type=integer-array;v=1;textable;numeric;sequence";
/// Media URN for number array type - textable, numeric, sequence
pub const MEDIA_NUMBER_ARRAY: &str = "media:type=number-array;v=1;textable;numeric;sequence";
/// Media URN for boolean array type - textable, sequence
pub const MEDIA_BOOLEAN_ARRAY: &str = "media:type=boolean-array;v=1;textable;sequence";
/// Media URN for object array type - textable, keyed, sequence
pub const MEDIA_OBJECT_ARRAY: &str = "media:type=object-array;v=1;textable;keyed;sequence";

// Semantic media types for specialized content
/// Media URN for image data (png, jpg, gif, webp, etc.)
pub const MEDIA_PNG: &str = "media:type=png;v=1;binary";
/// Media URN for audio data (wav, mp3, flac, etc.)
pub const MEDIA_AUDIO: &str = "media:type=wav;audio;binary;v=1;";
/// Media URN for video data (mp4, webm, mov, etc.)
pub const MEDIA_VIDEO: &str = "media:type=video;v=1;binary";
/// Media URN for generic text (semantic type)
pub const MEDIA_TEXT: &str = "media:type=text;v=1;textable";

// Document types (PRIMARY naming - type IS the format)
/// Media URN for PDF documents
pub const MEDIA_PDF: &str = "media:type=pdf;v=1;binary";
/// Media URN for EPUB documents
pub const MEDIA_EPUB: &str = "media:type=epub;v=1;binary";

// Text format types (PRIMARY naming - type IS the format)
/// Media URN for Markdown text
pub const MEDIA_MD: &str = "media:type=md;v=1;textable";
/// Media URN for plain text
pub const MEDIA_TXT: &str = "media:type=txt;v=1;textable";
/// Media URN for reStructuredText
pub const MEDIA_RST: &str = "media:type=rst;v=1;textable";
/// Media URN for log files
pub const MEDIA_LOG: &str = "media:type=log;v=1;textable";
/// Media URN for HTML documents
pub const MEDIA_HTML: &str = "media:type=html;v=1;textable";
/// Media URN for XML documents
pub const MEDIA_XML: &str = "media:type=xml;v=1;textable";
/// Media URN for JSON data
pub const MEDIA_JSON: &str = "media:type=json;v=1;textable;keyed";
/// Media URN for YAML data
pub const MEDIA_YAML: &str = "media:type=yaml;v=1;textable;keyed";

/// Helper to build binary media URN with extension
pub fn binary_media_urn_for_ext(ext: &str) -> String {
    format!("media:type=binary;ext={};v=1;binary", ext)
}

/// Helper to build text media URN with extension
pub fn text_media_urn_for_ext(ext: &str) -> String {
    format!("media:type=text;ext={};v=1;textable", ext)
}

/// Helper to build image media URN with extension
pub fn image_media_urn_for_ext(ext: &str) -> String {
    format!("media:type=image;ext={};v=1;binary", ext)
}

/// Helper to build audio media URN with extension
pub fn audio_media_urn_for_ext(ext: &str) -> String {
    format!("media:type=audio;ext={};v=1;binary", ext)
}

// CAPNS output types - all keyed structures (JSON objects)
/// Media URN for model download output - textable, keyed
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:type=download-result;v=1;textable;keyed";
/// Media URN for model load output - textable, keyed
pub const MEDIA_LOAD_OUTPUT: &str = "media:type=load-output;v=1;textable;keyed";
/// Media URN for model unload output - textable, keyed
pub const MEDIA_UNLOAD_OUTPUT: &str = "media:type=unload-output;v=1;textable;keyed";
/// Media URN for model list output - textable, keyed
pub const MEDIA_LIST_OUTPUT: &str = "media:type=list-output;v=1;textable;keyed";
/// Media URN for model status output - textable, keyed
pub const MEDIA_STATUS_OUTPUT: &str = "media:type=status-output;v=1;textable;keyed";
/// Media URN for model contents output - textable, keyed
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:type=model-contents;v=1;textable;keyed";
/// Media URN for embeddings generate output - textable, keyed
pub const MEDIA_GENERATE_OUTPUT: &str = "media:type=embedding-vector;v=1;textable;keyed";
/// Media URN for structured query output - textable, keyed
pub const MEDIA_STRUCTURED_QUERY_OUTPUT: &str = "media:type=json;v=1;textable;keyed";
/// Media URN for questions array - textable, sequence
pub const MEDIA_QUESTIONS_ARRAY: &str = "media:type=string-array;v=1;textable;sequence";
/// Media URN for LLM inference output - textable, keyed
pub const MEDIA_LLM_INFERENCE_OUTPUT: &str = "media:type=llm-inference-output;v=1;textable;keyed";
/// Media URN for extracted metadata - textable, keyed
pub const MEDIA_FILE_METADATA: &str = "media:type=file-metadata;v=1;textable;keyed";
/// Media URN for extracted outline - textable, keyed
pub const MEDIA_DOCUMENT_OUTLINE: &str = "media:type=document-outline;v=1;textable;keyed";
/// Media URN for disbound pages - textable, keyed, sequence (array of chunks)
pub const MEDIA_DISBOUND_PAGES: &str = "media:type=disbound-pages;v=1;textable;keyed;sequence";
/// Media URN for embeddings output - textable, keyed
pub const MEDIA_EMBEDDINGS_OUTPUT: &str = "media:type=embedding-vector;v=1;textable;keyed";
/// Media URN for image embeddings output - textable, keyed
pub const MEDIA_PNG_EMBEDDINGS_OUTPUT: &str = "media:type=embedding-vector;v=1;textable;keyed";
/// Media URN for caption output - textable, keyed
pub const MEDIA_CAPTION_OUTPUT: &str = "media:type=image-caption;v=1;textable;keyed";
/// Media URN for transcription output - textable, keyed
pub const MEDIA_TRANSCRIPTION_OUTPUT: &str = "media:type=transcription-output;v=1;textable;keyed";
/// Media URN for vision inference output - textable, keyed
pub const MEDIA_VISION_INFERENCE_OUTPUT: &str = "media:type=vision-inference-output;v=1;textable;keyed";
/// Media URN for model management output - textable, keyed
pub const MEDIA_MANAGE_OUTPUT: &str = "media:type=manage-output;v=1;textable;keyed";

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
            .tag("type", type_name)
            .tag("v", &version.to_string())
            .build()
            .expect("valid media URN");
        Self(urn)
    }

    /// Create a MediaUrn with type, subtype, and optional version
    pub fn with_subtype(type_name: &str, subtype: &str, version: Option<u32>) -> Self {
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX)
            .tag("type", type_name)
            .tag("subtype", subtype);
        if let Some(v) = version {
            builder = builder.tag("v", &v.to_string());
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
    pub fn with_tag(&self, key: &str, value: &str) -> Self {
        Self(self.0.clone().with_tag(key.to_string(), value.to_string()))
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
    /// - Extension must match if specified in requirement
    /// - Version must match if specified in requirement
    pub fn satisfies(&self, requirement: &MediaUrn) -> bool {
        // Type must match
        match (self.type_name(), requirement.type_name()) {
            (Some(self_type), Some(req_type)) if self_type != req_type => return false,
            (None, Some(_)) => return false,
            _ => {}
        }

        // Extension must match if specified in requirement
        if let Some(req_ext) = requirement.get_tag("ext") {
            match self.get_tag("ext") {
                Some(self_ext) if self_ext != req_ext => return false,
                None => return false,
                _ => {}
            }
        }

        // Version must match if specified in requirement
        match (self.version(), requirement.version()) {
            (Some(self_v), Some(req_v)) if self_v != req_v => return false,
            (None, Some(_)) => return false,
            _ => {}
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
        self.type_name() == Some("void")
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
        let urn = MediaUrn::from_string("media:type=string;v=1").unwrap();
        assert_eq!(urn.type_name(), Some("string"));
        assert_eq!(urn.version(), Some(1));
        assert!(urn.subtype().is_none());
        assert!(urn.profile().is_none());
    }

    #[test]
    fn test_parse_with_subtype() {
        let urn = MediaUrn::from_string("media:subtype=json;type=application").unwrap();
        assert_eq!(urn.type_name(), Some("application"));
        assert_eq!(urn.subtype(), Some("json"));
    }

    #[test]
    fn test_parse_with_profile() {
        let urn = MediaUrn::from_string(
            r#"media:profile="https://example.com/schema.json";type=object;v=1"#,
        )
        .unwrap();
        assert_eq!(urn.type_name(), Some("object"));
        assert_eq!(urn.profile(), Some("https://example.com/schema.json"));
    }

    #[test]
    fn test_wrong_prefix_fails() {
        let result = MediaUrn::from_string("cap:type=string;v=1");
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
        assert!(MediaUrn::from_string("media:type=raw;v=1;binary").unwrap().is_binary());
        assert!(MediaUrn::from_string(MEDIA_PNG).unwrap().is_binary()); // "media:type=png;v=1;binary"
        assert!(MediaUrn::from_string(MEDIA_PDF).unwrap().is_binary()); // "media:type=pdf;v=1;binary"
        assert!(MediaUrn::from_string(MEDIA_BINARY).unwrap().is_binary()); // "media:type=raw;v=1;binary"
        // Without binary tag, is_binary is false
        assert!(!MediaUrn::from_string("media:type=string;v=1;textable").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:type=object;v=1;textable;keyed").unwrap().is_binary());
    }

    #[test]
    fn test_is_json() {
        // is_json returns true only if "keyed" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_json()); // "media:type=object;v=1;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_JSON).unwrap().is_json()); // "media:type=json;v=1;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_OBJECT_ARRAY).unwrap().is_json()); // "media:type=object-array;v=1;textable;keyed;sequence"
        assert!(MediaUrn::from_string("media:type=custom;v=1;keyed").unwrap().is_json());
        // Without keyed tag, is_json is false
        assert!(!MediaUrn::from_string("media:type=string;v=1;textable").unwrap().is_json());
        assert!(!MediaUrn::from_string(MEDIA_STRING_ARRAY).unwrap().is_json()); // string-array has textable;sequence but not keyed
    }

    #[test]
    fn test_is_text() {
        // is_text returns true only if "textable" marker tag is present
        assert!(MediaUrn::from_string(MEDIA_STRING).unwrap().is_text()); // "media:type=string;v=1;textable;scalar"
        assert!(MediaUrn::from_string(MEDIA_INTEGER).unwrap().is_text()); // "media:type=integer;v=1;textable;numeric;scalar"
        assert!(MediaUrn::from_string(MEDIA_OBJECT).unwrap().is_text()); // "media:type=object;v=1;textable;keyed"
        assert!(MediaUrn::from_string(MEDIA_TEXT).unwrap().is_text()); // "media:type=text;v=1;textable"
        // Without textable tag, is_text is false
        assert!(!MediaUrn::from_string(MEDIA_BINARY).unwrap().is_text()); // "media:type=raw;v=1;binary"
        assert!(!MediaUrn::from_string(MEDIA_PNG).unwrap().is_text()); // "media:type=png;v=1;binary"
    }

    #[test]
    fn test_is_void() {
        assert!(MediaUrn::from_string("media:type=void;v=1").unwrap().is_void());
        assert!(!MediaUrn::from_string("media:type=string;v=1").unwrap().is_void());
    }

    #[test]
    fn test_simple_constructor() {
        let urn = MediaUrn::simple("string", 1);
        assert_eq!(urn.type_name(), Some("string"));
        assert_eq!(urn.version(), Some(1));
    }

    #[test]
    fn test_with_subtype_constructor() {
        let urn = MediaUrn::with_subtype("application", "json", Some(1));
        assert_eq!(urn.type_name(), Some("application"));
        assert_eq!(urn.subtype(), Some("json"));
        assert_eq!(urn.version(), Some(1));
    }

    #[test]
    fn test_to_string_roundtrip() {
        let original = "media:type=string;v=1";
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
        assert_eq!(pdf_urn, "media:type=binary;ext=pdf;v=1;binary");
        let parsed = MediaUrn::from_string(&pdf_urn).unwrap();
        assert_eq!(parsed.type_name(), Some("binary"));
        assert_eq!(parsed.extension(), Some("pdf"));

        // Test text_media_urn_for_ext
        let md_urn = text_media_urn_for_ext("md");
        assert_eq!(md_urn, "media:type=text;ext=md;v=1;textable");
        let parsed = MediaUrn::from_string(&md_urn).unwrap();
        assert_eq!(parsed.type_name(), Some("text"));
        assert_eq!(parsed.extension(), Some("md"));
    }

    #[test]
    fn test_satisfies() {
        // PDF listing satisfies PDF requirement (PRIMARY type naming)
        let pdf_listing = MediaUrn::from_string(MEDIA_PDF).unwrap();
        let pdf_requirement = MediaUrn::from_string("media:type=pdf;v=1").unwrap();
        assert!(pdf_listing.satisfies(&pdf_requirement));

        // PDF listing does NOT satisfy binary requirement (different type)
        let binary_requirement = MediaUrn::from_string("media:type=binary;v=1").unwrap();
        assert!(!pdf_listing.satisfies(&binary_requirement));

        // PDF listing does NOT satisfy EPUB requirement
        let epub_requirement = MediaUrn::from_string("media:type=epub;v=1").unwrap();
        assert!(!pdf_listing.satisfies(&epub_requirement));

        // PDF listing does NOT satisfy text requirement
        let text_requirement = MediaUrn::from_string("media:type=text;v=1").unwrap();
        assert!(!pdf_listing.satisfies(&text_requirement));

        // Markdown listing satisfies md requirement (PRIMARY type naming)
        let md_listing = MediaUrn::from_string(MEDIA_MD).unwrap();
        let md_requirement = MediaUrn::from_string("media:type=md;v=1").unwrap();
        assert!(md_listing.satisfies(&md_requirement));

        // Markdown listing does NOT satisfy txt requirement (different type)
        let txt_requirement = MediaUrn::from_string("media:type=txt;v=1").unwrap();
        assert!(!md_listing.satisfies(&txt_requirement));
    }

    #[test]
    fn test_matching() {
        let handler = MediaUrn::from_string("media:type=string;v=1").unwrap();
        let request = MediaUrn::from_string("media:type=string;v=1").unwrap();
        assert!(handler.matches(&request).unwrap());

        // Handler with fewer tags can handle more requests (implicit wildcards)
        let general_handler = MediaUrn::from_string("media:type=string").unwrap();
        assert!(general_handler.matches(&request).unwrap());

        // Mismatch
        let other = MediaUrn::from_string("media:type=object;v=1").unwrap();
        assert!(!handler.matches(&other).unwrap());
    }

    #[test]
    fn test_specificity() {
        let urn1 = MediaUrn::from_string("media:type=string").unwrap();
        let urn2 = MediaUrn::from_string("media:type=string;v=1").unwrap();
        let urn3 = MediaUrn::from_string("media:profile=\"https://x.com\";type=string;v=1").unwrap();

        assert!(urn1.specificity() < urn2.specificity());
        assert!(urn2.specificity() < urn3.specificity());
    }

    #[test]
    fn test_serde_roundtrip() {
        let urn = MediaUrn::from_string("media:type=string;v=1").unwrap();
        let json = serde_json::to_string(&urn).unwrap();
        assert_eq!(json, "\"media:type=string;v=1\"");
        let urn2: MediaUrn = serde_json::from_str(&json).unwrap();
        assert_eq!(urn, urn2);
    }
}
