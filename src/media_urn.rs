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
pub const MEDIA_BINARY: &str = "media:type=binary;v=1;binary";

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

// FGND-specific types
/// Media URN for listing ID (UUID) - textable, scalar
pub const MEDIA_LISTING_ID: &str = "media:type=listing-id;v=1;textable;scalar";
/// Media URN for file path array - textable, sequence
pub const MEDIA_FILE_PATH_ARRAY: &str = "media:type=file-path-array;v=1;textable;sequence";
/// Media URN for task ID (UUID) - textable, scalar
pub const MEDIA_TASK_ID: &str = "media:type=task-id;v=1;textable;scalar";

// CAPNS output types - all keyed structures (JSON objects)
/// Media URN for model download output - textable, keyed
pub const MEDIA_DOWNLOAD_OUTPUT: &str = "media:type=download-output;v=1;textable;keyed";
/// Media URN for model load output - textable, keyed
pub const MEDIA_LOAD_OUTPUT: &str = "media:type=load-output;v=1;textable;keyed";
/// Media URN for model unload output - textable, keyed
pub const MEDIA_UNLOAD_OUTPUT: &str = "media:type=unload-output;v=1;textable;keyed";
/// Media URN for model list output - textable, keyed
pub const MEDIA_LIST_OUTPUT: &str = "media:type=list-output;v=1;textable;keyed";
/// Media URN for model status output - textable, keyed
pub const MEDIA_STATUS_OUTPUT: &str = "media:type=status-output;v=1;textable;keyed";
/// Media URN for model contents output - textable, keyed
pub const MEDIA_CONTENTS_OUTPUT: &str = "media:type=contents-output;v=1;textable;keyed";
/// Media URN for embeddings generate output - textable, keyed
pub const MEDIA_GENERATE_OUTPUT: &str = "media:type=generate-output;v=1;textable;keyed";
/// Media URN for structured query output - textable, keyed
pub const MEDIA_STRUCTURED_QUERY_OUTPUT: &str = "media:type=structured-query-output;v=1;textable;keyed";
/// Media URN for questions array - textable, sequence
pub const MEDIA_QUESTIONS_ARRAY: &str = "media:type=questions-array;v=1;textable;sequence";
/// Media URN for LLM inference output - textable, keyed
pub const MEDIA_LLM_INFERENCE_OUTPUT: &str = "media:type=llm-inference-output;v=1;textable;keyed";

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

    // =========================================================================
    // Behavior helpers (triggered by tag presence)
    // =========================================================================

    /// Check if this represents binary data based on tags
    ///
    /// Returns true if:
    /// - type=binary
    /// - type=image, type=audio, type=video
    /// - type=application with subtype=octet-stream, pdf, etc.
    pub fn is_binary(&self) -> bool {
        let type_name = self.type_name().unwrap_or("");
        let subtype = self.subtype().unwrap_or("");

        // Binary primitive
        if type_name == "binary" {
            return true;
        }

        // Media types that are binary
        if matches!(type_name, "image" | "audio" | "video") {
            return true;
        }

        // Application types that are binary
        if type_name == "application" {
            return matches!(
                subtype,
                "octet-stream" | "pdf" | "zip" | "gzip" | "tar"
            ) || subtype.starts_with("x-")
                || subtype.ends_with("+zip")
                || subtype.ends_with("+gzip");
        }

        false
    }

    /// Check if this represents JSON data based on tags
    ///
    /// Returns true if:
    /// - type=object or type=object-array
    /// - type=application with subtype=json
    /// - Any array type (typically serialized as JSON)
    pub fn is_json(&self) -> bool {
        let type_name = self.type_name().unwrap_or("");
        let subtype = self.subtype().unwrap_or("");

        // Object types
        if type_name == "object" || type_name == "object-array" {
            return true;
        }

        // Array types (serialized as JSON)
        if type_name.ends_with("-array") {
            return true;
        }

        // Application/json
        if type_name == "application" && (subtype == "json" || subtype.ends_with("+json")) {
            return true;
        }

        false
    }

    /// Check if this represents text data based on tags
    pub fn is_text(&self) -> bool {
        let type_name = self.type_name().unwrap_or("");

        // Text types
        if type_name == "text" || type_name == "string" {
            return true;
        }

        // Primitives that are text
        if matches!(type_name, "integer" | "number" | "boolean") {
            return true;
        }

        // Void is neither text nor binary
        if type_name == "void" {
            return false;
        }

        // If not binary and not JSON, assume text
        !self.is_binary() && !self.is_json()
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
        assert!(MediaUrn::from_string("media:type=binary;v=1").unwrap().is_binary());
        assert!(MediaUrn::from_string("media:type=image;subtype=png").unwrap().is_binary());
        assert!(MediaUrn::from_string("media:type=application;subtype=pdf").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:type=string;v=1").unwrap().is_binary());
        assert!(!MediaUrn::from_string("media:type=object;v=1").unwrap().is_binary());
    }

    #[test]
    fn test_is_json() {
        assert!(MediaUrn::from_string("media:type=object;v=1").unwrap().is_json());
        assert!(MediaUrn::from_string("media:type=object-array;v=1").unwrap().is_json());
        assert!(MediaUrn::from_string("media:type=string-array;v=1").unwrap().is_json());
        assert!(MediaUrn::from_string("media:type=application;subtype=json").unwrap().is_json());
        assert!(!MediaUrn::from_string("media:type=string;v=1").unwrap().is_json());
    }

    #[test]
    fn test_is_text() {
        assert!(MediaUrn::from_string("media:type=string;v=1").unwrap().is_text());
        assert!(MediaUrn::from_string("media:type=integer;v=1").unwrap().is_text());
        assert!(!MediaUrn::from_string("media:type=object;v=1").unwrap().is_text());
        assert!(!MediaUrn::from_string("media:type=binary;v=1").unwrap().is_text());
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
        assert!(MediaUrn::from_string(MEDIA_LISTING_ID).is_ok());
        assert!(MediaUrn::from_string(MEDIA_FILE_PATH_ARRAY).is_ok());
        assert!(MediaUrn::from_string(MEDIA_TASK_ID).is_ok());
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
