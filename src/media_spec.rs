//! MediaSpec parsing and media URN resolution
//!
//! This module provides:
//! - Media URN resolution (e.g., `media:string` → resolved media spec)
//! - MediaSpec parsing (canonical form: `text/plain; profile=https://...`)
//! - MediaSpecDef for defining specs in cap definitions
//! - ArgumentValidation for validation rules inherent to media types
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
    MEDIA_PNG, MEDIA_AUDIO, MEDIA_VIDEO, MEDIA_TEXT,
    // CAPNS output types
    MEDIA_DOWNLOAD_OUTPUT, MEDIA_LOAD_OUTPUT, MEDIA_UNLOAD_OUTPUT,
    MEDIA_LIST_OUTPUT, MEDIA_STATUS_OUTPUT, MEDIA_CONTENTS_OUTPUT,
    MEDIA_GENERATE_OUTPUT, MEDIA_STRUCTURED_QUERY_OUTPUT, MEDIA_QUESTIONS_ARRAY,
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
pub const PROFILE_CAPNS_LIST_OUTPUT: &str = "https://capns.org/schema/list-output";
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
// ARGUMENT VALIDATION (for media spec definitions)
// =============================================================================

/// Validation rules for media types
///
/// These rules are inherent to the semantic media type and are defined
/// in the media spec, not on individual arguments or outputs.
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

impl ArgumentValidation {
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
    /// Optional local JSON Schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Display-friendly title for the media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional description of the media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional validation rules for this media type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<ArgumentValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl MediaSpecDef {
    /// Create a compact string form definition
    pub fn string(media_spec: impl Into<String>) -> Self {
        MediaSpecDef::String(media_spec.into())
    }

    /// Create a rich object form definition
    pub fn object(media_type: impl Into<String>, profile_uri: impl Into<String>) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with local schema
    pub fn object_with_schema(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        schema: serde_json::Value,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            schema: Some(schema),
            title: None,
            description: None,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with title
    pub fn object_with_title(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            schema: None,
            title: Some(title.into()),
            description: None,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with all fields
    pub fn object_full(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        schema: Option<serde_json::Value>,
        title: Option<String>,
        description: Option<String>,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            schema,
            title,
            description,
            validation: None,
            metadata: None,
        })
    }

    /// Create a rich object form definition with validation
    pub fn object_with_validation(
        media_type: impl Into<String>,
        profile_uri: impl Into<String>,
        validation: ArgumentValidation,
    ) -> Self {
        MediaSpecDef::Object(MediaSpecDefObject {
            media_type: media_type.into(),
            profile_uri: profile_uri.into(),
            schema: None,
            title: None,
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
    pub validation: Option<ArgumentValidation>,
    /// Optional metadata (arbitrary key-value pairs for display/categorization)
    pub metadata: Option<serde_json::Value>,
}

impl ResolvedMediaSpec {
    /// Check if this represents binary data.
    /// Returns true if the "binary" marker tag is present in the source media URN.
    pub fn is_binary(&self) -> bool {
        crate::MediaUrn::from_string(&self.media_urn)
            .map(|urn| urn.is_binary())
            .unwrap_or(false)
    }

    /// Check if this represents JSON/keyed data.
    /// Returns true if the "keyed" marker tag is present in the source media URN.
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

/// Resolve a media URN to a full media spec definition (sync version)
///
/// Resolution order:
/// 1. Look up in provided media_specs map (HIGHEST - overrides everything)
/// 2. If not found, check if it's a built-in primitive
/// 3. If neither, fail hard with an error
///
/// For async resolution with registry support, use `resolve_media_urn_with_registry`.
///
/// # Arguments
/// * `media_urn` - The media URN to resolve (e.g., "media:string")
/// * `media_specs` - The media_specs map from the cap definition
///
/// # Errors
/// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved.
pub fn resolve_media_urn(
    media_urn: &str,
    media_specs: &HashMap<String, MediaSpecDef>,
) -> Result<ResolvedMediaSpec, MediaSpecError> {
    // First, try to look up in the provided media_specs
    if let Some(def) = media_specs.get(media_urn) {
        return resolve_def(media_urn, def);
    }

    // Second, check if it's a built-in primitive
    if let Some(resolved) = resolve_builtin(media_urn) {
        return Ok(resolved);
    }

    // Fail hard - no fallbacks
    Err(MediaSpecError::UnresolvableMediaUrn(media_urn.to_string()))
}

/// Resolve a media URN with registry support (async version)
///
/// Resolution order:
/// 1. Check cap's local `media_specs` table (HIGHEST - overrides everything)
/// 2. Check built-in constants
/// 3. Check registry cache (in-memory → disk)
/// 4. Fetch from remote registry
/// 5. If none resolve → Error
///
/// # Arguments
/// * `media_urn` - The media URN to resolve (e.g., "media:string")
/// * `media_specs` - Optional media_specs map from the cap definition
/// * `registry` - Optional MediaUrnRegistry for remote lookups
///
/// # Errors
/// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved.
pub async fn resolve_media_urn_with_registry(
    media_urn: &str,
    media_specs: Option<&HashMap<String, MediaSpecDef>>,
    registry: Option<&crate::media_registry::MediaUrnRegistry>,
) -> Result<ResolvedMediaSpec, MediaSpecError> {
    // First, try to look up in the provided media_specs (if any)
    if let Some(specs) = media_specs {
        if let Some(def) = specs.get(media_urn) {
            return resolve_def(media_urn, def);
        }
    }

    // Second, check if it's a built-in primitive
    if let Some(resolved) = resolve_builtin(media_urn) {
        return Ok(resolved);
    }

    // Third, try the registry if available
    if let Some(registry) = registry {
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
            Err(_) => {
                // Registry lookup failed, continue to error
            }
        }
    }

    // Fail hard - no fallbacks
    Err(MediaSpecError::UnresolvableMediaUrn(media_urn.to_string()))
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
            title: obj.title.clone(),
            description: obj.description.clone(),
            validation: obj.validation.clone(), // Propagate validation
            metadata: obj.metadata.clone(),     // Propagate metadata
        }),
    }
}

/// Resolve a built-in media URN
fn resolve_builtin(media_urn: &str) -> Option<ResolvedMediaSpec> {
    let (media_type, profile_uri) = match media_urn {
        // Standard primitives
        MEDIA_STRING => ("text/plain", Some(PROFILE_STR)),
        MEDIA_INTEGER => ("text/plain", Some(PROFILE_INT)),
        MEDIA_NUMBER => ("text/plain", Some(PROFILE_NUM)),
        MEDIA_BOOLEAN => ("text/plain", Some(PROFILE_BOOL)),
        MEDIA_OBJECT => ("application/json", Some(PROFILE_OBJ)),
        MEDIA_STRING_ARRAY => ("application/json", Some(PROFILE_STR_ARRAY)),
        MEDIA_INTEGER_ARRAY => ("application/json", Some(PROFILE_INT_ARRAY)),
        MEDIA_NUMBER_ARRAY => ("application/json", Some(PROFILE_NUM_ARRAY)),
        MEDIA_BOOLEAN_ARRAY => ("application/json", Some(PROFILE_BOOL_ARRAY)),
        MEDIA_OBJECT_ARRAY => ("application/json", Some(PROFILE_OBJ_ARRAY)),
        MEDIA_BINARY => ("application/octet-stream", None),
        MEDIA_VOID => ("application/x-void", Some(PROFILE_VOID)),
        // Semantic content types
        MEDIA_PNG => ("image/png", Some(PROFILE_IMAGE)),
        MEDIA_AUDIO => ("audio/wav", Some(PROFILE_AUDIO)),
        MEDIA_VIDEO => ("video/mp4", Some(PROFILE_VIDEO)),
        MEDIA_TEXT => ("text/plain", Some(PROFILE_TEXT)),
        // Document types (PRIMARY naming)
        MEDIA_PDF => ("application/pdf", Some(PROFILE_PDF)),
        MEDIA_EPUB => ("application/epub+zip", Some(PROFILE_EPUB)),
        // Text format types (PRIMARY naming)
        MEDIA_MD => ("text/markdown", Some(PROFILE_MD)),
        MEDIA_TXT => ("text/plain", Some(PROFILE_TXT)),
        MEDIA_RST => ("text/x-rst", Some(PROFILE_RST)),
        MEDIA_LOG => ("text/plain", Some(PROFILE_LOG)),
        MEDIA_HTML => ("text/html", Some(PROFILE_HTML)),
        MEDIA_XML => ("application/xml", Some(PROFILE_XML)),
        MEDIA_JSON => ("application/json", Some(PROFILE_JSON)),
        MEDIA_YAML => ("application/x-yaml", Some(PROFILE_YAML)),
        // CAPNS output types
        MEDIA_DOWNLOAD_OUTPUT => ("application/json", Some(PROFILE_CAPNS_DOWNLOAD_OUTPUT)),
        MEDIA_LOAD_OUTPUT => ("application/json", Some(PROFILE_CAPNS_LOAD_OUTPUT)),
        MEDIA_UNLOAD_OUTPUT => ("application/json", Some(PROFILE_CAPNS_UNLOAD_OUTPUT)),
        MEDIA_LIST_OUTPUT => ("application/json", Some(PROFILE_CAPNS_LIST_OUTPUT)),
        MEDIA_STATUS_OUTPUT => ("application/json", Some(PROFILE_CAPNS_STATUS_OUTPUT)),
        MEDIA_CONTENTS_OUTPUT => ("application/json", Some(PROFILE_CAPNS_CONTENTS_OUTPUT)),
        MEDIA_GENERATE_OUTPUT => ("application/json", Some(PROFILE_CAPNS_GENERATE_OUTPUT)),
        MEDIA_STRUCTURED_QUERY_OUTPUT => ("application/json", Some(PROFILE_CAPNS_STRUCTURED_QUERY_OUTPUT)),
        MEDIA_QUESTIONS_ARRAY => ("application/json", Some(PROFILE_CAPNS_QUESTIONS_ARRAY)),
        _ => return None,
    };

    Some(ResolvedMediaSpec {
        media_urn: media_urn.to_string(),
        media_type: media_type.to_string(),
        profile_uri: profile_uri.map(String::from),
        schema: None,     // Built-ins don't have local schemas
        title: None,      // Will be populated from registry
        description: None,
        validation: None, // Built-ins don't have validation rules
        metadata: None,   // Built-ins don't have metadata
    })
}

/// Check if a media URN is a built-in primitive
pub fn is_builtin_media_urn(media_urn: &str) -> bool {
    matches!(
        media_urn,
        MEDIA_STRING
            | MEDIA_INTEGER
            | MEDIA_NUMBER
            | MEDIA_BOOLEAN
            | MEDIA_OBJECT
            | MEDIA_STRING_ARRAY
            | MEDIA_INTEGER_ARRAY
            | MEDIA_NUMBER_ARRAY
            | MEDIA_BOOLEAN_ARRAY
            | MEDIA_OBJECT_ARRAY
            | MEDIA_BINARY
            | MEDIA_VOID
            // Semantic content types
            | MEDIA_PNG
            | MEDIA_AUDIO
            | MEDIA_VIDEO
            | MEDIA_TEXT
            // Document types (PRIMARY naming)
            | MEDIA_PDF
            | MEDIA_EPUB
            // Text format types (PRIMARY naming)
            | MEDIA_MD
            | MEDIA_TXT
            | MEDIA_RST
            | MEDIA_LOG
            | MEDIA_HTML
            | MEDIA_XML
            | MEDIA_JSON
            | MEDIA_YAML
            // CAPNS output types
            | MEDIA_DOWNLOAD_OUTPUT
            | MEDIA_LOAD_OUTPUT
            | MEDIA_UNLOAD_OUTPUT
            | MEDIA_LIST_OUTPUT
            | MEDIA_STATUS_OUTPUT
            | MEDIA_CONTENTS_OUTPUT
            | MEDIA_GENERATE_OUTPUT
            | MEDIA_STRUCTURED_QUERY_OUTPUT
            | MEDIA_QUESTIONS_ARRAY
    )
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

    #[test]
    fn test_parse_text_with_profile() {
        let spec = MediaSpec::parse("text/plain; profile=https://capns.org/schema/str").unwrap();
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(
            spec.profile,
            Some("https://capns.org/schema/str".to_string())
        );
    }

    #[test]
    fn test_parse_binary() {
        let spec = MediaSpec::parse("application/octet-stream").unwrap();
        assert_eq!(spec.media_type, "application/octet-stream");
        assert!(spec.profile.is_none());
    }

    #[test]
    fn test_parse_image() {
        let spec = MediaSpec::parse("image/png; profile=https://example.com/thumbnail").unwrap();
        assert_eq!(spec.media_type, "image/png");
    }

    #[test]
    fn test_parse_no_profile() {
        let spec = MediaSpec::parse("text/html").unwrap();
        assert_eq!(spec.media_type, "text/html");
        assert!(spec.profile.is_none());
    }

    #[test]
    fn test_parse_quoted_profile() {
        let spec =
            MediaSpec::parse("application/json; profile=\"https://example.com/schema\"").unwrap();
        assert_eq!(
            spec.profile,
            Some("https://example.com/schema".to_string())
        );
    }

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

    #[test]
    fn test_resolve_builtin_str() {
        let media_specs = HashMap::new();
        let resolved = resolve_media_urn(MEDIA_STRING, &media_specs).unwrap();
        assert_eq!(resolved.media_urn, MEDIA_STRING);
        assert_eq!(resolved.media_type, "text/plain");
        assert_eq!(resolved.profile_uri, Some(PROFILE_STR.to_string()));
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_builtin_obj() {
        let media_specs = HashMap::new();
        let resolved = resolve_media_urn(MEDIA_OBJECT, &media_specs).unwrap();
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(resolved.profile_uri, Some(PROFILE_OBJ.to_string()));
    }

    #[test]
    fn test_resolve_builtin_binary() {
        let media_specs = HashMap::new();
        let resolved = resolve_media_urn(MEDIA_BINARY, &media_specs).unwrap();
        assert_eq!(resolved.media_type, "application/octet-stream");
        assert!(resolved.profile_uri.is_none());
        assert!(resolved.is_binary());
    }

    #[test]
    fn test_resolve_custom_string_form() {
        let mut media_specs = HashMap::new();
        media_specs.insert(
            "media:custom-spec".to_string(),
            MediaSpecDef::String("application/json; profile=https://example.com/schema".to_string()),
        );

        let resolved = resolve_media_urn("media:custom-spec", &media_specs).unwrap();
        assert_eq!(resolved.media_urn, "media:custom-spec");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema".to_string())
        );
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_custom_object_form() {
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
                schema: Some(schema.clone()),
                title: None,
                description: None,
                validation: None,
                metadata: None,
            }),
        );

        let resolved = resolve_media_urn("media:output-spec", &media_specs).unwrap();
        assert_eq!(resolved.media_urn, "media:output-spec");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema/output".to_string())
        );
        assert_eq!(resolved.schema, Some(schema));
    }

    #[test]
    fn test_resolve_unresolvable_fails_hard() {
        let media_specs = HashMap::new();
        let result = resolve_media_urn("media:unknown", &media_specs);
        assert!(result.is_err());
        if let Err(MediaSpecError::UnresolvableMediaUrn(urn)) = result {
            assert_eq!(urn, "media:unknown");
        } else {
            panic!("Expected UnresolvableMediaUrn error");
        }
    }

    #[test]
    fn test_custom_overrides_builtin() {
        // Custom definition in media_specs takes precedence over built-in
        let mut media_specs = HashMap::new();
        media_specs.insert(
            MEDIA_STRING.to_string(),
            MediaSpecDef::String("application/json; profile=https://custom.example.com/str".to_string()),
        );

        let resolved = resolve_media_urn(MEDIA_STRING, &media_specs).unwrap();
        // Custom definition used, not built-in
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://custom.example.com/str".to_string())
        );
    }

    #[test]
    fn test_is_builtin_media_urn() {
        assert!(is_builtin_media_urn(MEDIA_STRING));
        assert!(is_builtin_media_urn(MEDIA_INTEGER));
        assert!(is_builtin_media_urn(MEDIA_BINARY));
        assert!(!is_builtin_media_urn("media:custom-spec"));
        assert!(!is_builtin_media_urn("random-string"));
    }

    // -------------------------------------------------------------------------
    // MediaSpecDef serialization tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_media_spec_def_string_serialize() {
        let def = MediaSpecDef::String("text/plain; profile=https://example.com".to_string());
        let json = serde_json::to_string(&def).unwrap();
        assert_eq!(json, "\"text/plain; profile=https://example.com\"");
    }

    #[test]
    fn test_media_spec_def_object_serialize() {
        let def = MediaSpecDef::Object(MediaSpecDefObject {
            media_type: "application/json".to_string(),
            profile_uri: "https://example.com/profile".to_string(),
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        });
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"media_type\":\"application/json\""));
        assert!(json.contains("\"profile_uri\":\"https://example.com/profile\""));
        // None schema is skipped - check it's not serialized as "schema":null or "schema":...
        assert!(!json.contains("\"schema\":"));
        // None title/description are also skipped
        assert!(!json.contains("\"title\":"));
        assert!(!json.contains("\"description\":"));
        // None validation is also skipped
        assert!(!json.contains("\"validation\":"));
    }

    #[test]
    fn test_media_spec_def_deserialize_string() {
        let json = "\"text/plain; profile=https://example.com\"";
        let def: MediaSpecDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def, MediaSpecDef::String(_)));
    }

    #[test]
    fn test_media_spec_def_deserialize_object() {
        let json = r#"{"media_type":"application/json","profile_uri":"https://example.com"}"#;
        let def: MediaSpecDef = serde_json::from_str(json).unwrap();
        assert!(matches!(def, MediaSpecDef::Object(_)));
    }

    // -------------------------------------------------------------------------
    // ResolvedMediaSpec tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_resolved_is_binary() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:raw;binary".to_string(),
            media_type: "application/octet-stream".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_binary());
        assert!(!resolved.is_json());
    }

    #[test]
    fn test_resolved_is_json() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:object;textable;keyed".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
        };
        assert!(resolved.is_json());
        assert!(!resolved.is_binary());
    }

    #[test]
    fn test_resolved_is_text() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:string;textable".to_string(),
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

    #[test]
    fn test_metadata_propagation_from_object_def() {
        // Create a media spec definition with metadata
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:custom-setting;setting".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "text/plain".to_string(),
                profile_uri: "https://example.com/schema".to_string(),
                schema: None,
                title: Some("Custom Setting".to_string()),
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
        let resolved = resolve_media_urn("media:custom-setting;setting", &media_specs).unwrap();
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "interface");
        assert_eq!(metadata.get("ui_type").unwrap(), "SETTING_UI_TYPE_CHECKBOX");
        assert_eq!(metadata.get("subcategory_key").unwrap(), "appearance");
        assert_eq!(metadata.get("display_index").unwrap(), 5);
    }

    #[test]
    fn test_metadata_none_for_string_def() {
        // String form definitions should have no metadata
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:simple;textable".to_string(),
            MediaSpecDef::String("text/plain; profile=https://example.com".to_string()),
        );

        let resolved = resolve_media_urn("media:simple;textable", &media_specs).unwrap();
        assert!(resolved.metadata.is_none());
    }

    #[test]
    fn test_metadata_none_for_builtin() {
        // Built-in media URNs should have no metadata
        let media_specs = std::collections::HashMap::new();
        let resolved = resolve_media_urn(crate::MEDIA_STRING, &media_specs).unwrap();
        assert!(resolved.metadata.is_none());
    }

    #[test]
    fn test_metadata_with_validation() {
        // Ensure metadata and validation can coexist
        let mut media_specs = std::collections::HashMap::new();
        media_specs.insert(
            "media:bounded-number;numeric;setting".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "text/plain".to_string(),
                profile_uri: "https://example.com/schema".to_string(),
                schema: None,
                title: Some("Bounded Number".to_string()),
                description: None,
                validation: Some(ArgumentValidation {
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

        let resolved = resolve_media_urn("media:bounded-number;numeric;setting", &media_specs).unwrap();

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
