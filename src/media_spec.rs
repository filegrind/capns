//! MediaSpec parsing and media URN resolution
//!
//! This module provides:
//! - Media URN resolution (e.g., `media:string` → resolved media spec)
//! - MediaSpec parsing (canonical form: `text/plain; profile=https://...`)
//! - MediaSpecDef for defining specs in cap definitions
//! - MediaValidation for validation rules inherent to media types
//!
//! ## Media URN Format
//! Media URNs are tagged URNs with "media" prefix, e.g., `media:string`
//! Built-in primitives are available without explicit declaration.
//!
//! ## MediaSpec Format
//! Canonical form: `<media-type>; profile=<url>`
//! Example: `text/plain; profile=https://capns.org/schema/str`

use crate::media_urn::{
    MEDIA_VOID, MEDIA_STRING, MEDIA_INTEGER, MEDIA_NUMBER, MEDIA_BOOLEAN, MEDIA_OBJECT,
    MEDIA_STRING_ARRAY, MEDIA_INTEGER_ARRAY, MEDIA_NUMBER_ARRAY, MEDIA_BOOLEAN_ARRAY, MEDIA_OBJECT_ARRAY,
    MEDIA_BINARY,
    // Document types (PRIMARY naming)
    MEDIA_PDF, MEDIA_EPUB,
    // Text format types (PRIMARY naming)
    MEDIA_MD, MEDIA_TXT, MEDIA_RST, MEDIA_LOG, MEDIA_HTML, MEDIA_XML, MEDIA_JSON, MEDIA_YAML,
    // Semantic media types for specialized content
    MEDIA_PNG, MEDIA_AUDIO, MEDIA_VIDEO,
    // CAPNS output types
    MEDIA_DOWNLOAD_OUTPUT,
    MEDIA_LIST_OUTPUT, MEDIA_STATUS_OUTPUT, MEDIA_CONTENTS_OUTPUT,
    MEDIA_EMBEDDING_VECTOR,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// =============================================================================
// PROFILE URLS (canonical /schema/ path)
// =============================================================================

/// Base URL for capns schemas (default, use `get_schema_base()` for configurable version)
pub const SCHEMA_BASE: &str = "https://capns.org/schema";

/// Get the schema base URL from environment variables or default
///
/// Checks in order:
/// 1. `CAPNS_SCHEMA_BASE_URL` environment variable
/// 2. `CAPNS_REGISTRY_URL` environment variable + "/schema"
/// 3. Default: "https://capns.org/schema"
pub fn get_schema_base() -> String {
    if let Ok(schema_url) = std::env::var("CAPNS_SCHEMA_BASE_URL") {
        return schema_url;
    }
    if let Ok(registry_url) = std::env::var("CAPNS_REGISTRY_URL") {
        return format!("{}/schema", registry_url);
    }
    SCHEMA_BASE.to_string()
}

/// Get a profile URL for the given profile name
///
/// # Example
/// ```ignore
/// let url = get_profile_url("str"); // Returns "{schema_base}/str"
/// ```
pub fn get_profile_url(profile_name: &str) -> String {
    format!("{}/{}", get_schema_base(), profile_name)
}

/// Profile URL for string type
pub const PROFILE_STR: &str = "https://capns.org/schema/str";
/// Profile URL for integer type
pub const PROFILE_INT: &str = "https://capns.org/schema/int";
/// Profile URL for number type
pub const PROFILE_NUM: &str = "https://capns.org/schema/num";
/// Profile URL for boolean type
pub const PROFILE_BOOL: &str = "https://capns.org/schema/bool";
/// Profile URL for JSON object type
pub const PROFILE_OBJ: &str = "https://capns.org/schema/obj";
/// Profile URL for string array type
pub const PROFILE_STR_ARRAY: &str = "https://capns.org/schema/str-array";
/// Profile URL for integer array type
pub const PROFILE_INT_ARRAY: &str = "https://capns.org/schema/int-array";
/// Profile URL for number array type
pub const PROFILE_NUM_ARRAY: &str = "https://capns.org/schema/num-array";
/// Profile URL for boolean array type
pub const PROFILE_BOOL_ARRAY: &str = "https://capns.org/schema/bool-array";
/// Profile URL for object array type
pub const PROFILE_OBJ_ARRAY: &str = "https://capns.org/schema/obj-array";
/// Profile URL for void (no input)
pub const PROFILE_VOID: &str = "https://capns.org/schema/void";

// =============================================================================
// SEMANTIC CONTENT TYPE PROFILE URLS
// =============================================================================

/// Profile URL for image data (png, jpg, gif, etc.)
pub const PROFILE_IMAGE: &str = "https://capns.org/schema/image";
/// Profile URL for audio data (wav, mp3, flac, etc.)
pub const PROFILE_AUDIO: &str = "https://capns.org/schema/audio";
/// Profile URL for video data (mp4, webm, mov, etc.)
pub const PROFILE_VIDEO: &str = "https://capns.org/schema/video";
/// Profile URL for generic text
pub const PROFILE_TEXT: &str = "https://capns.org/schema/text";

// =============================================================================
// DOCUMENT TYPE PROFILE URLS (PRIMARY naming)
// =============================================================================

/// Profile URL for PDF documents
pub const PROFILE_PDF: &str = "https://capns.org/schema/pdf";
/// Profile URL for EPUB documents
pub const PROFILE_EPUB: &str = "https://capns.org/schema/epub";

// =============================================================================
// TEXT FORMAT TYPE PROFILE URLS (PRIMARY naming)
// =============================================================================

/// Profile URL for Markdown text
pub const PROFILE_MD: &str = "https://capns.org/schema/md";
/// Profile URL for plain text
pub const PROFILE_TXT: &str = "https://capns.org/schema/txt";
/// Profile URL for reStructuredText
pub const PROFILE_RST: &str = "https://capns.org/schema/rst";
/// Profile URL for log files
pub const PROFILE_LOG: &str = "https://capns.org/schema/log";
/// Profile URL for HTML documents
pub const PROFILE_HTML: &str = "https://capns.org/schema/html";
/// Profile URL for XML documents
pub const PROFILE_XML: &str = "https://capns.org/schema/xml";
/// Profile URL for JSON data
pub const PROFILE_JSON: &str = "https://capns.org/schema/json";
/// Profile URL for YAML data
pub const PROFILE_YAML: &str = "https://capns.org/schema/yaml";

// =============================================================================
// CAPNS OUTPUT PROFILE URLS
// =============================================================================

/// Profile URL for model download output
pub const PROFILE_CAPNS_DOWNLOAD_OUTPUT: &str = "https://capns.org/schema/download-output";
/// Profile URL for model load output
pub const PROFILE_CAPNS_LOAD_OUTPUT: &str = "https://capns.org/schema/load-output";
/// Profile URL for model unload output
pub const PROFILE_CAPNS_UNLOAD_OUTPUT: &str = "https://capns.org/schema/unload-output";
/// Profile URL for model list output
pub const PROFILE_CAPNS_LIST_OUTPUT: &str = "https://capns.org/schema/model-list";
/// Profile URL for model status output
pub const PROFILE_CAPNS_STATUS_OUTPUT: &str = "https://capns.org/schema/status-output";
/// Profile URL for model contents output
pub const PROFILE_CAPNS_CONTENTS_OUTPUT: &str = "https://capns.org/schema/contents-output";
/// Profile URL for embeddings generate output
pub const PROFILE_CAPNS_GENERATE_OUTPUT: &str = "https://capns.org/schema/embeddings";
/// Profile URL for structured query output
pub const PROFILE_CAPNS_STRUCTURED_QUERY_OUTPUT: &str = "https://capns.org/schema/structured-query-output";
/// Profile URL for questions array
pub const PROFILE_CAPNS_QUESTIONS_ARRAY: &str = "https://capns.org/schema/questions-array";

// =============================================================================
// MEDIA VALIDATION (for media spec definitions)
// =============================================================================

/// Validation rules for media types
///
/// These rules are inherent to the semantic media type and are defined
/// in the media spec, not on individual arguments or outputs.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MediaValidation {
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

impl MediaValidation {
    /// Check if all validation fields are empty/None
    pub fn is_empty(&self) -> bool {
        self.min.is_none() &&
        self.max.is_none() &&
        self.min_length.is_none() &&
        self.max_length.is_none() &&
        self.pattern.is_none() &&
        self.allowed_values.is_none()
    }

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
    pub fn with_pattern(pattern: String) -> Self {
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
    pub fn with_allowed_values(values: Vec<String>) -> Self {
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

// =============================================================================
// MEDIA SPEC DEFINITION (for cap definitions)
// =============================================================================

/// Media spec definition - can be string (compact) or object (rich)
///
/// Used in the `media_specs` map of a cap definition.
///
/// ## String Form (compact)
/// ```json
/// "media:string": "text/plain; profile=https://capns.org/schema/str"
/// ```
///
/// ## Object Form (rich, with optional local schema)
/// ```json
/// "media:my-output": {
///   "media_type": "application/json",
///   "profile_uri": "https://example.com/schema/my-output",
///   "schema": { "type": "object", ... }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MediaSpecDef {
    /// Compact form: "text/plain; profile=https://..."
    String(String),
    /// Rich form with optional local schema
    Object(MediaSpecDefObject),
}

/// Object form of media spec definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaSpecDefObject {
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Profile URI for schema reference
    pub profile_uri: String,
    /// Human-readable title for the media type (required - MS1)
    pub title: String,
    /// Optional local JSON Schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Optional description of the media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional validation rules for this media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<MediaValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl MediaSpecDef {
    /// Create a compact string form definition
    pub fn string(media_spec: impl Into<String>) -> Self {
        MediaSpecDef::String(media_spec.into())
    }

    /// Create a rich object form definition (title required - MS1)
    pub fn object(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            title: title.into(),
            schema: None,
            description: None,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with local schema (title required - MS1)
    pub fn object_with_schema(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        title: impl Into<String>,
        schema: serde_json::Value,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            title: title.into(),
            schema: Some(schema),
            description: None,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with all fields (title required - MS1)
    pub fn object_full(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        title: impl Into<String>,
        schema: Option<serde_json::Value>,
        description: Option<String>,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            title: title.into(),
            schema,
            description,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with validation (title required - MS1)
    pub fn object_with_validation(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        title: impl Into<String>,
        validation: MediaValidation,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            title: title.into(),
            schema: None,
            description: None,
            validation: Some(validation),
            metadata: None,
        })
    }
}

// =============================================================================
// RESOLVED MEDIA SPEC
// =============================================================================

/// Fully resolved media spec with all fields populated
///
/// This is the result of resolving a media URN through the media_specs table
/// or from a built-in definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMediaSpec {
    /// The media URN that was resolved
    pub media_urn: String,
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Optional profile URI
    pub profile_uri: Option<String>,
    /// Optional local JSON Schema for validation
    pub schema: Option<serde_json::Value>,
    /// Display-friendly title for the media type
    pub title: Option<String>,
    /// Optional description of the media type
    pub description: Option<String>,
    /// Optional validation rules from the media spec definition
    pub validation: Option<MediaValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    pub metadata: Option<serde_json::Value>,
}

impl ResolvedMediaSpec {
    /// Check if this represents binary data.
    /// Returns true if the "bytes" marker tag is present in the source media URN.
    pub fn is_binary(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_binary())
            .unwrap_or(false)
    }

    /// Check if this represents a map/object structure (form=map).
    /// This indicates a key-value structure, regardless of representation format.
    pub fn is_map(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_map())
            .unwrap_or(false)
    }

    /// Check if this represents a scalar value (form=scalar).
    /// This indicates a single value, not a collection.
    pub fn is_scalar(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_scalar())
            .unwrap_or(false)
    }

    /// Check if this represents a list/array structure (form=list).
    /// This indicates an ordered collection of values.
    pub fn is_list(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_list())
            .unwrap_or(false)
    }

    /// Check if this represents structured data (map or list).
    /// Structured data can be serialized as JSON when transmitted as text.
    /// Note: This does NOT check for the explicit `json` tag - use is_json() for that.
    pub fn is_structured(&self) -> bool {
        self.is_map() || self.is_list()
    }

    /// Check if this represents JSON representation specifically.
    /// Returns true if the "json" marker tag is present in the source media URN.
    /// Note: This only checks for explicit JSON format marker.
    /// For checking if data is structured (map/list), use is_structured().
    pub fn is_json(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_json())
            .unwrap_or(false)
    }

    /// Check if this represents text data.
    /// Returns true if the "textable" marker tag is present in the source media URN.
    pub fn is_text(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_text())
            .unwrap_or(false)
    }
}

// =============================================================================
// MEDIA URN RESOLUTION
// =============================================================================

/// Resolve a media URN to a full media spec definition.
///
/// This is the SINGLE resolution path for all media URN lookups.
///
/// Resolution order:
/// 1. Cap's local `media_specs` table (HIGHEST - cap-specific overrides)
/// 2. Registry's local cache (bundled standard specs)
/// 3. Online registry fetch (with graceful degradation if unreachable)
/// 4. If none resolve → Error
///
/// # Arguments
/// * `media_urn` - The media URN to resolve (e.g., "media:textable;form=scalar")
/// * `media_specs` - Optional media_specs map from the cap definition
/// * `registry` - The MediaUrnRegistry for cache and remote lookups
///
/// # Errors
/// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved
/// from any source.
pub async fn resolve_media_urn(
    media_urn: &str,
    media_specs: Option<&HashMap<String, MediaSpecDef>>,
    registry: &crate::media_registry::MediaUrnRegistry,
) -> Result<ResolvedMediaSpec, MediaSpecError> {
    // 1. First, try cap's local media_specs (highest priority - cap-specific overrides)
    if let Some(specs) = media_specs {
        if let Some(def) = specs.get(media_urn) {
            return resolve_def(media_urn, def);
        }
    }

    // 2. Try registry (checks local cache first, then online with graceful degradation)
    match registry.get_media_spec(media_urn).await {
        Ok(stored_spec) => {
            return Ok(ResolvedMediaSpec {
                media_urn: media_urn.to_string(),
                media_type: stored_spec.media_type,
                profile_uri: stored_spec.profile_uri,
                schema: stored_spec.schema,
                title: Some(stored_spec.title),
                description: stored_spec.description,
                validation: stored_spec.validation,
                metadata: stored_spec.metadata,
            });
        }
        Err(e) => {
            // Registry lookup failed (not in cache, online unreachable or not found)
            // Log and continue to error
            eprintln!(
                "[WARN] Media URN '{}' not found in registry: {} - \
                ensure it's defined in capns_dot_org/standard/media/",
                media_urn, e
            );
        }
    }

    // Fail - not found in any source
    Err(MediaSpecError::UnresolvableMediaUrn(format!(
        "cannot resolve media URN '{}' - not found in cap's media_specs or registry",
        media_urn
    )))
}


/// Resolve a MediaSpecDef to a ResolvedMediaSpec
fn resolve_def(media_urn: &str, def: &MediaSpecDef) -> Result<ResolvedMediaSpec, MediaSpecError> {
    match def {
        MediaSpecDef::String(s) => {
            let parsed = MediaSpec::parse(s)?;
            Ok(ResolvedMediaSpec {
                media_urn: media_urn.to_string(),
                media_type: parsed.media_type,
                profile_uri: parsed.profile,
                schema: None,
                title: None,
                description: None,
                validation: None, // String form has no validation
                metadata: None,   // String form has no metadata
            })
        }
        MediaSpecDef::Object(obj) => Ok(ResolvedMediaSpec {
            media_urn: media_urn.to_string(),
            media_type: obj.media_type.clone(),
            profile_uri: Some(obj.profile_uri.clone()),
            schema: obj.schema.clone(),
            title: Some(obj.title.clone()),
            description: obj.description.clone(),
            validation: obj.validation.clone(), // Propagate validation
            metadata: obj.metadata.clone(),     // Propagate metadata
        }),
    }
}


// =============================================================================
// MEDIA SPEC (parsed form)
// =============================================================================

/// Parsed MediaSpec structure
///
/// Canonical format: `<media-type>; profile=<url>`
///
/// Example: `text/plain; profile=https://capns.org/schema/str`
#[derive(Debug, Clone, PartialEq)]
pub struct MediaSpec {
    /// The MIME media type (e.g., "application/json", "image/png")
    pub media_type: String,
    /// Optional profile URL
    pub profile: Option<String>,
}

impl MediaSpec {
    /// Parse a media_spec string
    ///
    /// Canonical format: `<media-type>; profile=<url>`
    ///
    /// Examples:
    /// - `text/plain; profile=https://capns.org/schema/str`
    /// - `application/json; profile=https://capns.org/schema/obj`
    /// - `application/octet-stream`
    pub fn parse(s: &str) -> Result<Self, MediaSpecError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(MediaSpecError::EmptyMediaType);
        }

        // Split by semicolon to separate media type from parameters
        let parts: Vec<&str> = s.splitn(2, ';').collect();

        let media_type = parts[0].trim().to_string();
        if media_type.is_empty() {
            return Err(MediaSpecError::EmptyMediaType);
        }

        // Validate media type format (should contain '/')
        if !media_type.contains('/') {
            return Err(MediaSpecError::InvalidMediaType(format!(
                "media type '{}' must contain '/'",
                media_type
            )));
        }

        // Parse profile if present
        let profile = if parts.len() > 1 {
            let params = parts[1].trim();
            MediaSpec::parse_profile(params)?
        } else {
            None
        };

        Ok(MediaSpec { media_type, profile })
    }

    /// Parse profile parameter from params string
    fn parse_profile(params: &str) -> Result<Option<String>, MediaSpecError> {
        // Look for profile= (case-insensitive)
        let lower = params.to_lowercase();
        if let Some(pos) = lower.find("profile=") {
            let after_profile = &params[pos + 8..];

            // Handle quoted value
            let value = if after_profile.starts_with('"') {
                // Find closing quote
                let rest = &after_profile[1..];
                if let Some(end) = rest.find('"') {
                    rest[..end].to_string()
                } else {
                    return Err(MediaSpecError::UnterminatedQuote);
                }
            } else {
                // Unquoted value - take until semicolon or end
                after_profile
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            };

            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        } else {
            Ok(None)
        }
    }

    /// Get the primary type (e.g., "image" from "image/png")
    pub fn primary_type(&self) -> &str {
        self.media_type
            .split('/')
            .next()
            .unwrap_or(&self.media_type)
    }

    /// Get the subtype (e.g., "png" from "image/png")
    pub fn subtype(&self) -> Option<&str> {
        self.media_type.split('/').nth(1)
    }

    /// Create from resolved media spec
    pub fn from_resolved(resolved: &ResolvedMediaSpec) -> Self {
        MediaSpec {
            media_type: resolved.media_type.clone(),
            profile: resolved.profile_uri.clone(),
        }
    }
}

impl fmt::Display for MediaSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.media_type)?;
        if let Some(ref profile) = self.profile {
            write!(f, "; profile={}", profile)?;
        }
        Ok(())
    }
}

// =============================================================================
// ERRORS
// =============================================================================

/// Errors that can occur when parsing or resolving media specs
#[derive(Debug, Clone, PartialEq)]
pub enum MediaSpecError {
    /// Media type is empty
    EmptyMediaType,
    /// Invalid media type format
    InvalidMediaType(String),
    /// Unterminated quote in profile value
    UnterminatedQuote,
    /// Media URN cannot be resolved (not in media_specs and not a built-in)
    UnresolvableMediaUrn(String),
}

impl fmt::Display for MediaSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaSpecError::EmptyMediaType => {
                write!(f, "media type cannot be empty")
            }
            MediaSpecError::InvalidMediaType(msg) => {
                write!(f, "invalid media type: {}", msg)
            }
            MediaSpecError::UnterminatedQuote => {
                write!(f, "unterminated quote in profile value")
            }
            MediaSpecError::UnresolvableMediaUrn(urn) => {
                write!(
                    f,
                    "cannot resolve media URN '{}' - not found in media_specs and not a built-in primitive",
                    urn
                )
            }
        }
    }
}

impl std::error::Error for MediaSpecError {}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // MediaSpec parsing tests
    // -------------------------------------------------------------------------

    // TEST079: Test parsing JSON media spec with profile extracts media type and profile URL correctly
    #[test]
    fn test_parse_json_with_profile() {
        let spec =
            MediaSpec::parse("application/json; profile=https://capns.org/schema/obj").unwrap();
        assert_eq!(spec.media_type, "application/json");
        assert_eq!(
            spec.profile,
            Some("https://capns.org/schema/obj".to_string())
        );
    }

    // TEST080: Test parsing text media spec with profile extracts media type and profile URL correctly
    #[test]
    fn test_parse_text_with_profile() {
        let spec = MediaSpec::parse("text/plain; profile=https://capns.org/schema/str").unwrap();
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(
            spec.profile,
            Some("https://capns.org/schema/str".to_string())
        );
    }

    // TEST081: Test parsing binary media spec without profile extracts media type only
    #[test]
    fn test_parse_binary() {
        let spec = MediaSpec::parse("application/octet-stream").unwrap();
        assert_eq!(spec.media_type, "application/octet-stream");
        assert!(spec.profile.is_none());
    }

    // TEST082: Test parsing image media spec with profile extracts media type correctly
    #[test]
    fn test_parse_image() {
        let spec = MediaSpec::parse("image/png; profile=https://example.com/thumbnail").unwrap();
        assert_eq!(spec.media_type, "image/png");
    }

    // TEST083: Test parsing media spec without profile returns None for profile field
    #[test]
    fn test_parse_no_profile() {
        let spec = MediaSpec::parse("text/html").unwrap();
        assert_eq!(spec.media_type, "text/html");
        assert!(spec.profile.is_none());
    }

    // TEST084: Test parsing media spec with quoted profile URL extracts profile without quotes
    #[test]
    fn test_parse_quoted_profile() {
        let spec =
            MediaSpec::parse("application/json; profile=\"https://example.com/schema\"").unwrap();
        assert_eq!(
            spec.profile,
            Some("https://example.com/schema".to_string())
        );
    }

    // TEST085: Test invalid media type without slash returns InvalidMediaType error
    #[test]
    fn test_invalid_media_type() {
        let result = MediaSpec::parse("invalid");
        assert!(result.is_err());
        if let Err(MediaSpecError::InvalidMediaType(_)) = result {
            // Expected
        } else {
            panic!("Expected InvalidMediaType error");
        }
    }

    // TEST086: Test display format outputs media type with profile in canonical format
    #[test]
    fn test_display() {
        let spec = MediaSpec {
            media_type: "application/json".to_string(),
            profile: Some("https://example.com/schema".to_string()),
        };
        assert_eq!(
            spec.to_string(),
            "application/json; profile=https://example.com/schema"
        );
    }

    // TEST087: Test display format outputs media type only when profile is None
    #[test]
    fn test_display_no_profile() {
        let spec = MediaSpec {
            media_type: "text/plain".to_string(),
            profile: None,
        };
        assert_eq!(spec.to_string(), "text/plain");
    }

    // -------------------------------------------------------------------------
    // Media URN resolution tests
    // -------------------------------------------------------------------------

    // Helper to create a test registry
    async fn test_registry() -> crate::media_registry::MediaUrnRegistry {
        crate::media_registry::MediaUrnRegistry::new().await.expect("Failed to create test registry")
    }

    // TEST088: Test resolving string media URN from registry returns correct media type and profile
    #[tokio::test]
    async fn test_resolve_from_registry_str() {
        let registry = test_registry().await;
        let resolved = resolve_media_urn(MEDIA_STRING, None, &registry).await.unwrap();
        assert_eq!(resolved.media_urn, MEDIA_STRING);
        assert_eq!(resolved.media_type, "text/plain");
        // Registry provides the full spec including profile
        assert!(resolved.profile_uri.is_some());
    }

    // TEST089: Test resolving object media URN from registry returns JSON media type
    #[tokio::test]
    async fn test_resolve_from_registry_obj() {
        let registry = test_registry().await;
        let resolved = resolve_media_urn(MEDIA_OBJECT, None, &registry).await.unwrap();
        assert_eq!(resolved.media_type, "application/json");
    }

    // TEST090: Test resolving binary media URN from registry returns octet-stream and is_binary true
    #[tokio::test]
    async fn test_resolve_from_registry_binary() {
        let registry = test_registry().await;
        let resolved = resolve_media_urn(MEDIA_BINARY, None, &registry).await.unwrap();
        assert_eq!(resolved.media_type, "application/octet-stream");
        assert!(resolved.is_binary());
    }

    // TEST091: Test resolving custom media URN from local media_specs takes precedence over registry
    #[tokio::test]
    async fn test_resolve_custom_string_form() {
        let registry = test_registry().await;
        let mut media_specs = HashMap::new();
        media_specs.insert(
            "media:custom-spec".to_string(),
            MediaSpecDef::String("application/json; profile=https://example.com/schema".to_string()),
        );

        // Local media_specs takes precedence over registry
        let resolved = resolve_media_urn("media:custom-spec", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.media_urn, "media:custom-spec");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema".to_string())
        );
        assert!(resolved.schema.is_none());
    }

    // TEST092: Test resolving custom object form media spec with schema from local media_specs
    #[tokio::test]
    async fn test_resolve_custom_object_form() {
        let registry = test_registry().await;
        let mut media_specs = HashMap::new();
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            }
        });
        media_specs.insert(
            "media:output-spec".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "application/json".to_string(),
                profile_uri: "https://example.com/schema/output".to_string(),
                title: "Output Spec".to_string(),
                schema: Some(schema.clone()),
                description: None,
                validation: None,
                metadata: None,
            }),
        );

        let resolved = resolve_media_urn("media:output-spec", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.media_urn, "media:output-spec");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema/output".to_string())
        );
        assert_eq!(resolved.schema, Some(schema));
    }

    // TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error
    #[tokio::test]
    async fn test_resolve_unresolvable_fails_hard() {
        let registry = test_registry().await;
        // URN not in local media_specs and not in registry
        let result = resolve_media_urn("media:completely-unknown-urn-not-in-registry", None, &registry).await;
        assert!(result.is_err());
        if let Err(MediaSpecError::UnresolvableMediaUrn(msg)) = result {
            assert!(msg.contains("media:completely-unknown-urn-not-in-registry"));
        } else {
            panic!("Expected UnresolvableMediaUrn error");
        }
    }

    // TEST094: Test local media_specs definition overrides registry definition for same URN
    #[tokio::test]
    async fn test_local_overrides_registry() {
        let registry = test_registry().await;
        // Custom definition in media_specs takes precedence over registry
        let mut media_specs = HashMap::new();
        media_specs.insert(
            MEDIA_STRING.to_string(),
            MediaSpecDef::String("application/json; profile=https://custom.example.com/str".to_string()),
        );

        let resolved = resolve_media_urn(MEDIA_STRING, Some(&media_specs), &registry).await.unwrap();
        // Custom definition used, not registry
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://custom.example.com/str".to_string())
        );
    }

    // -------------------------------------------------------------------------
    // MediaSpecDef serialization tests
    // -------------------------------------------------------------------------

    // TEST095: Test MediaSpecDef string form serializes as JSON string
    #[test]
    fn test_media_spec_def_string_serialize() {
        let def = MediaSpecDef::String("text/plain; profile=https://example.com".to_string());
        let json = serde_json::to_string(&def).unwrap();
        assert_eq!(json, "\"text/plain; profile=https://example.com\"");
    }

    // TEST096: Test MediaSpecDef object form serializes with required fields and skips None fields
    #[test]
    fn test_media_spec_def_object_serialize() {
        let def = MediaSpecDef::Object(MediaSpecDefObject {
            media_type: "application/json".to_string(),
            profile_uri: "https://example.com/profile".to_string(),
            title: "Test Media".to_string(),
            schema: None,
            description: None,
            validation: None,
            metadata: None,
        });
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"media_type\":\"application/json\""));
        assert!(json.contains("\"profile_uri\":\"https://example.com/profile\""));
        assert!(json.contains("\"title\":\"Test Media\""));
        // None schema is skipped - check it's not serialized as "schema":null or "schema":...
        assert!(!json.contains("\"schema\":"));
        // None description is also skipped
        assert!(!json.contains("\"description\":"));
        // None validation is also skipped
        assert!(!json.contains("\"validation\":"));
    }

    // TEST097: Test deserializing MediaSpecDef string form from JSON string
    #[test]
    fn test_media_spec_def_deserialize_string() {
        let json = "\"text/plain; profile=https://example.com\"";
        let def: MediaSpecDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def, MediaSpecDef::String(_)));
    }

    // TEST098: Test deserializing MediaSpecDef object form from JSON object
    #[test]
    fn test_media_spec_def_deserialize_object() {
        let json = r#"{"media_type":"application/json","profile_uri":"https://example.com","title":"Test"}"#;
        let def: MediaSpecDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def, MediaSpecDef::Object(_)));
    }

    // -------------------------------------------------------------------------
    // ResolvedMediaSpec tests
    // -------------------------------------------------------------------------

    // TEST099: Test ResolvedMediaSpec is_binary returns true for bytes media URN
    #[test]
    fn test_resolved_is_binary() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:bytes".to_string(),
            media_type: "application/octet-stream".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_binary());
        assert!(!resolved.is_map());
        assert!(!resolved.is_json());
    }

    // TEST100: Test ResolvedMediaSpec is_map returns true for form=map media URN
    #[test]
    fn test_resolved_is_map() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=map".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_map());
        assert!(!resolved.is_binary());
        assert!(!resolved.is_scalar());
        assert!(!resolved.is_list());
    }

    // TEST101: Test ResolvedMediaSpec is_scalar returns true for form=scalar media URN
    #[test]
    fn test_resolved_is_scalar() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=scalar".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_scalar());
        assert!(!resolved.is_map());
        assert!(!resolved.is_list());
    }

    // TEST102: Test ResolvedMediaSpec is_list returns true for form=list media URN
    #[test]
    fn test_resolved_is_list() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=list".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_list());
        assert!(!resolved.is_map());
        assert!(!resolved.is_scalar());
    }

    // TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present
    #[test]
    fn test_resolved_is_json() {
        // is_json checks for the "json" tag specifically, not form=map
        let resolved = ResolvedMediaSpec {
            media_urn: "media:json;textable;form=map".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_json());
        assert!(resolved.is_map()); // also a map
        assert!(!resolved.is_binary());
    }

    // TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present
    #[test]
    fn test_resolved_is_text() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_text());
        assert!(!resolved.is_binary());
        assert!(!resolved.is_json());
    }

    // -------------------------------------------------------------------------
    // Metadata propagation tests
    // -------------------------------------------------------------------------

    // TEST105: Test metadata propagates from object def to resolved media spec
    #[tokio::test]
    async fn test_metadata_propagation_from_object_def() {
        let registry = test_registry().await;
        // Create a media spec definition with metadata
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:custom-setting;setting".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "text/plain".to_string(),
                profile_uri: "https://example.com/schema".to_string(),
                title: "Custom Setting".to_string(),
                schema: None,
                description: Some("A custom setting".to_string()),
                validation: None,
                metadata: Some(serde_json::json!({
                    "category_key": "interface",
                    "ui_type": "SETTING_UI_TYPE_CHECKBOX",
                    "subcategory_key": "appearance",
                    "display_index": 5
                })),
            }),
        );

        // Resolve and verify metadata is propagated
        let resolved = resolve_media_urn("media:custom-setting;setting", Some(&media_specs), &registry).await.unwrap();
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "interface");
        assert_eq!(metadata.get("ui_type").unwrap(), "SETTING_UI_TYPE_CHECKBOX");
        assert_eq!(metadata.get("subcategory_key").unwrap(), "appearance");
        assert_eq!(metadata.get("display_index").unwrap(), 5);
    }

    // TEST106: Test metadata is None for string form media spec definitions
    #[tokio::test]
    async fn test_metadata_none_for_string_def() {
        let registry = test_registry().await;
        // String form definitions should have no metadata
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:simple;textable".to_string(),
            MediaSpecDef::String("text/plain; profile=https://example.com".to_string()),
        );

        let resolved = resolve_media_urn("media:simple;textable", Some(&media_specs), &registry).await.unwrap();
        assert!(resolved.metadata.is_none());
    }

    // TEST107: Test metadata and validation can coexist in media spec definition
    #[tokio::test]
    async fn test_metadata_with_validation() {
        let registry = test_registry().await;
        // Ensure metadata and validation can coexist
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:bounded-number;numeric;setting".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "text/plain".to_string(),
                profile_uri: "https://example.com/schema".to_string(),
                title: "Bounded Number".to_string(),
                schema: None,
                description: None,
                validation: Some(MediaValidation {
                    min: Some(0.0),
                    max: Some(100.0),
                    min_length: None,
                    max_length: None,
                    pattern: None,
                    allowed_values: None,
                }),
                metadata: Some(serde_json::json!({
                    "category_key": "inference",
                    "ui_type": "SETTING_UI_TYPE_SLIDER"
                })),
            }),
        );

        let resolved = resolve_media_urn("media:bounded-number;numeric;setting", Some(&media_specs), &registry).await.unwrap();

        // Verify validation
        assert!(resolved.validation.is_some());
        let validation = resolved.validation.unwrap();
        assert_eq!(validation.min, Some(0.0));
        assert_eq!(validation.max, Some(100.0));

        // Verify metadata
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "inference");
        assert_eq!(metadata.get("ui_type").unwrap(), "SETTING_UI_TYPE_SLIDER");
    }
}
