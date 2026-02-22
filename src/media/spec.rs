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

use serde::{Deserialize, Serialize};
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
/// Media spec definition for inline media_specs in cap definitions
///
/// This is the same structure as media spec JSON files in the registry.
/// Each media spec has a unique URN that identifies it.
///
/// ## Example
/// ```json
/// {
///   "urn": "media:my-output;json;form=map",
///   "media_type": "application/json",
///   "title": "My Output",
///   "profile_uri": "https://example.com/schema/my-output",
///   "schema": { "type": "object", ... }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaSpecDef {
    /// The media URN identifier (e.g., "media:pdf;binary")
    pub urn: String,
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Human-readable title for the media type (required)
    pub title: String,
    /// Profile URI for schema reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
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
    /// File extensions for storing this media type (e.g., ["pdf"], ["jpg", "jpeg"])
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
}

impl MediaSpecDef {
    /// Create a new media spec definition
    pub fn new(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: None,
            schema: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media spec definition with profile URI
    pub fn with_profile(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        profile_uri: impl Into<String>,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: Some(profile_uri.into()),
            schema: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media spec definition with schema
    pub fn with_schema(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        profile_uri: impl Into<String>,
        schema: serde_json::Value,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: Some(profile_uri.into()),
            schema: Some(schema),
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        }
    }

    /// Create a media spec definition with validation rules
    pub fn with_validation(
        urn: impl Into<String>,
        media_type: impl Into<String>,
        title: impl Into<String>,
        validation: MediaValidation,
    ) -> Self {
        Self {
            urn: urn.into(),
            media_type: media_type.into(),
            title: title.into(),
            profile_uri: None,
            schema: None,
            description: None,
            validation: Some(validation),
            metadata: None,
            extensions: Vec::new(),
        }
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
    /// File extensions for storing this media type (e.g., ["pdf"], ["jpg", "jpeg"])
    pub extensions: Vec<String>,
}

impl ResolvedMediaSpec {
    /// Parse the media URN, panicking if invalid (should never happen for resolved specs)
    fn parse_media_urn(&self) -> crate::MediaUrn {
        crate::MediaUrn::from_string(&self.media_urn)
            .expect("ResolvedMediaSpec has invalid media_urn - this indicates a bug in resolution")
    }

    /// Check if this represents binary (non-text) data.
    /// Returns true if the "textable" marker tag is NOT present in the source media URN.
    pub fn is_binary(&self) -> bool {
        self.parse_media_urn().is_binary()
    }

    /// Check if this represents a map/object structure (form=map).
    /// This indicates a key-value structure, regardless of representation format.
    pub fn is_map(&self) -> bool {
        self.parse_media_urn().is_map()
    }

    /// Check if this represents a scalar value (form=scalar).
    /// This indicates a single value, not a collection.
    pub fn is_scalar(&self) -> bool {
        self.parse_media_urn().is_scalar()
    }

    /// Check if this represents a list/array structure (form=list).
    /// This indicates an ordered collection of values.
    pub fn is_list(&self) -> bool {
        self.parse_media_urn().is_list()
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
        self.parse_media_urn().is_json()
    }

    /// Check if this represents text data.
    /// Returns true if the "textable" marker tag is present in the source media URN.
    pub fn is_text(&self) -> bool {
        self.parse_media_urn().is_text()
    }

    /// Check if this represents image data.
    /// Returns true if the "image" marker tag is present in the source media URN.
    pub fn is_image(&self) -> bool {
        self.parse_media_urn().is_image()
    }

    /// Check if this represents audio data.
    /// Returns true if the "audio" marker tag is present in the source media URN.
    pub fn is_audio(&self) -> bool {
        self.parse_media_urn().is_audio()
    }

    /// Check if this represents video data.
    /// Returns true if the "video" marker tag is present in the source media URN.
    pub fn is_video(&self) -> bool {
        self.parse_media_urn().is_video()
    }

    /// Check if this represents numeric data.
    /// Returns true if the "numeric" marker tag is present in the source media URN.
    pub fn is_numeric(&self) -> bool {
        self.parse_media_urn().is_numeric()
    }

    /// Check if this represents boolean data.
    /// Returns true if the "bool" marker tag is present in the source media URN.
    pub fn is_bool(&self) -> bool {
        self.parse_media_urn().is_bool()
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
/// 1. Cap's local `media_specs` array (HIGHEST - cap-specific definitions)
/// 2. Registry's local cache (bundled standard specs)
/// 3. Online registry fetch (with graceful degradation if unreachable)
/// 4. If none resolve → Error
///
/// # Arguments
/// * `media_urn` - The media URN to resolve (e.g., "media:textable;form=scalar")
/// * `media_specs` - Optional media_specs array from the cap definition
/// * `registry` - The MediaUrnRegistry for cache and remote lookups
///
/// # Errors
/// Returns `MediaSpecError::UnresolvableMediaUrn` if the media URN cannot be resolved
/// from any source.
pub async fn resolve_media_urn(
    media_urn: &str,
    media_specs: Option<&[MediaSpecDef]>,
    registry: &crate::media::registry::MediaUrnRegistry,
) -> Result<ResolvedMediaSpec, MediaSpecError> {
    // 1. First, try cap's local media_specs (highest priority - cap-specific definitions)
    if let Some(specs) = media_specs {
        if let Some(def) = specs.iter().find(|s| s.urn == media_urn) {
            return Ok(ResolvedMediaSpec {
                media_urn: def.urn.clone(),
                media_type: def.media_type.clone(),
                profile_uri: def.profile_uri.clone(),
                schema: def.schema.clone(),
                title: Some(def.title.clone()),
                description: def.description.clone(),
                validation: def.validation.clone(),
                metadata: def.metadata.clone(),
                extensions: def.extensions.clone(),
            });
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
                extensions: stored_spec.extensions,
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

/// Validate that media_specs array has no duplicate URNs.
///
/// # Arguments
/// * `media_specs` - The media_specs array to validate
///
/// # Errors
/// Returns `MediaSpecError::DuplicateMediaUrn` if any URN appears more than once.
pub fn validate_media_specs_no_duplicates(media_specs: &[MediaSpecDef]) -> Result<(), MediaSpecError> {
    let mut seen = std::collections::HashSet::new();
    for spec in media_specs {
        if !seen.insert(&spec.urn) {
            return Err(MediaSpecError::DuplicateMediaUrn(spec.urn.clone()));
        }
    }
    Ok(())
}

// =============================================================================
// ERRORS
// =============================================================================

/// Errors that can occur when resolving media specs
#[derive(Debug, Clone, PartialEq)]
pub enum MediaSpecError {
    /// Media URN cannot be resolved (not in media_specs and not in registry)
    UnresolvableMediaUrn(String),
    /// Duplicate media URN in media_specs array
    DuplicateMediaUrn(String),
}

impl fmt::Display for MediaSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaSpecError::UnresolvableMediaUrn(urn) => {
                write!(
                    f,
                    "cannot resolve media URN '{}' - not found in media_specs or registry",
                    urn
                )
            }
            MediaSpecError::DuplicateMediaUrn(urn) => {
                write!(
                    f,
                    "duplicate media URN '{}' in media_specs - each URN must be unique",
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
    // Media URN resolution tests
    // -------------------------------------------------------------------------

    // Helper to create a test registry
    async fn test_registry() -> crate::media::registry::MediaUrnRegistry {
        crate::media::registry::MediaUrnRegistry::new().await.expect("Failed to create test registry")
    }

    // Helper to create media specs vec for tests
    fn create_media_specs(specs: Vec<MediaSpecDef>) -> Vec<MediaSpecDef> {
        specs
    }

    // TEST088: Test resolving string media URN from registry returns correct media type and profile
    #[tokio::test]
    async fn test088_resolve_from_registry_str() {
        let registry = test_registry().await;
        let resolved = resolve_media_urn("media:textable;form=scalar", None, &registry).await.unwrap();
        assert_eq!(resolved.media_type, "text/plain");
        // Registry provides the full spec including profile
        assert!(resolved.profile_uri.is_some());
    }

    // TEST089: Test resolving object media URN from registry returns JSON media type
    #[tokio::test]
    async fn test089_resolve_from_registry_obj() {
        let registry = test_registry().await;
        let resolved = resolve_media_urn("media:form=map;textable", None, &registry).await.unwrap();
        assert_eq!(resolved.media_type, "application/json");
    }

    // TEST090: Test resolving binary media URN returns octet-stream and is_binary true
    #[tokio::test]
    async fn test090_resolve_from_registry_binary() {
        let registry = test_registry().await;
        // media: (wildcard binary) is provided via local media_specs since the registry
        // stores specs under specific URNs, not the wildcard
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:".to_string(),
                media_type: "application/octet-stream".to_string(),
                title: "Binary".to_string(),
                profile_uri: None,
                schema: None,
                description: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            },
        ]);
        let resolved = resolve_media_urn("media:", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.media_type, "application/octet-stream");
        assert!(resolved.is_binary());
    }

    // TEST091: Test resolving custom media URN from local media_specs takes precedence over registry
    #[tokio::test]
    async fn test091_resolve_custom_media_spec() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:custom-spec;json".to_string(),
                media_type: "application/json".to_string(),
                title: "Custom Spec".to_string(),
                profile_uri: Some("https://example.com/schema".to_string()),
                schema: None,
                description: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            }
        ]);

        // Local media_specs takes precedence over registry
        let resolved = resolve_media_urn("media:custom-spec;json", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.media_urn, "media:custom-spec;json");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema".to_string())
        );
        assert!(resolved.schema.is_none());
    }

    // TEST092: Test resolving custom object form media spec with schema from local media_specs
    #[tokio::test]
    async fn test092_resolve_custom_with_schema() {
        let registry = test_registry().await;
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            }
        });
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:output-spec;json;form=map".to_string(),
                media_type: "application/json".to_string(),
                title: "Output Spec".to_string(),
                profile_uri: Some("https://example.com/schema/output".to_string()),
                schema: Some(schema.clone()),
                description: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            }
        ]);

        let resolved = resolve_media_urn("media:output-spec;json;form=map", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.media_urn, "media:output-spec;json;form=map");
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://example.com/schema/output".to_string())
        );
        assert_eq!(resolved.schema, Some(schema));
    }

    // TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error
    #[tokio::test]
    async fn test093_resolve_unresolvable_fails_hard() {
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
    async fn test094_local_overrides_registry() {
        let registry = test_registry().await;
        // Custom definition in media_specs takes precedence over registry
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:textable;form=scalar".to_string(),
                media_type: "application/json".to_string(), // Override: normally text/plain
                title: "Custom String".to_string(),
                profile_uri: Some("https://custom.example.com/str".to_string()),
                schema: None,
                description: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            }
        ]);

        let resolved = resolve_media_urn("media:textable;form=scalar", Some(&media_specs), &registry).await.unwrap();
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

    // TEST095: Test MediaSpecDef serializes with required fields and skips None fields
    #[test]
    fn test095_media_spec_def_serialize() {
        let def = MediaSpecDef {
            urn: "media:test;json".to_string(),
            media_type: "application/json".to_string(),
            title: "Test Media".to_string(),
            profile_uri: Some("https://example.com/profile".to_string()),
            schema: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"urn\":\"media:test;json\""));
        assert!(json.contains("\"media_type\":\"application/json\""));
        assert!(json.contains("\"profile_uri\":\"https://example.com/profile\""));
        assert!(json.contains("\"title\":\"Test Media\""));
        // None schema is skipped
        assert!(!json.contains("\"schema\":"));
        // None description is also skipped
        assert!(!json.contains("\"description\":"));
    }

    // TEST096: Test deserializing MediaSpecDef from JSON object
    #[test]
    fn test096_media_spec_def_deserialize() {
        let json = r#"{"urn":"media:test;json","media_type":"application/json","title":"Test"}"#;
        let def: MediaSpecDef = serde_json::from_str(json).unwrap();
        assert_eq!(def.urn, "media:test;json");
        assert_eq!(def.media_type, "application/json");
        assert_eq!(def.title, "Test");
        assert!(def.profile_uri.is_none());
    }

    // -------------------------------------------------------------------------
    // Duplicate URN validation tests
    // -------------------------------------------------------------------------

    // TEST097: Test duplicate URN validation catches duplicates
    #[test]
    fn test097_validate_no_duplicate_urns_catches_duplicates() {
        let media_specs = vec![
            MediaSpecDef::new("media:dup;json", "application/json", "First"),
            MediaSpecDef::new("media:dup;json", "application/json", "Second"), // duplicate
        ];
        let result = validate_media_specs_no_duplicates(&media_specs);
        assert!(result.is_err());
        if let Err(MediaSpecError::DuplicateMediaUrn(urn)) = result {
            assert_eq!(urn, "media:dup;json");
        } else {
            panic!("Expected DuplicateMediaUrn error");
        }
    }

    // TEST098: Test duplicate URN validation passes for unique URNs
    #[test]
    fn test098_validate_no_duplicate_urns_passes_for_unique() {
        let media_specs = vec![
            MediaSpecDef::new("media:first;json", "application/json", "First"),
            MediaSpecDef::new("media:second;json", "application/json", "Second"),
        ];
        let result = validate_media_specs_no_duplicates(&media_specs);
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------------
    // ResolvedMediaSpec tests
    // -------------------------------------------------------------------------

    // TEST099: Test ResolvedMediaSpec is_binary returns true when textable tag is absent
    #[test]
    fn test099_resolved_is_binary() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:".to_string(),
            media_type: "application/octet-stream".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_binary());
        assert!(!resolved.is_map());
        assert!(!resolved.is_json());
    }

    // TEST100: Test ResolvedMediaSpec is_map returns true for form=map media URN
    #[test]
    fn test100_resolved_is_map() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=map".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_map());
        assert!(!resolved.is_binary());
        assert!(!resolved.is_scalar());
        assert!(!resolved.is_list());
    }

    // TEST101: Test ResolvedMediaSpec is_scalar returns true for form=scalar media URN
    #[test]
    fn test101_resolved_is_scalar() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=scalar".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_scalar());
        assert!(!resolved.is_map());
        assert!(!resolved.is_list());
    }

    // TEST102: Test ResolvedMediaSpec is_list returns true for form=list media URN
    #[test]
    fn test102_resolved_is_list() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable;form=list".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_list());
        assert!(!resolved.is_map());
        assert!(!resolved.is_scalar());
    }

    // TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present
    #[test]
    fn test103_resolved_is_json() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:json;textable;form=map".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_json());
        assert!(resolved.is_map());
        assert!(!resolved.is_binary());
    }

    // TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present
    #[test]
    fn test104_resolved_is_text() {
        let resolved = ResolvedMediaSpec {
            media_urn: "media:textable".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
            title: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        };
        assert!(resolved.is_text());
        assert!(!resolved.is_binary());
        assert!(!resolved.is_json());
    }

    // -------------------------------------------------------------------------
    // Metadata propagation tests
    // -------------------------------------------------------------------------

    // TEST105: Test metadata propagates from media spec def to resolved media spec
    #[tokio::test]
    async fn test105_metadata_propagation() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:custom-setting;setting".to_string(),
                media_type: "text/plain".to_string(),
                title: "Custom Setting".to_string(),
                profile_uri: Some("https://example.com/schema".to_string()),
                schema: None,
                description: Some("A custom setting".to_string()),
                validation: None,
                metadata: Some(serde_json::json!({
                    "category_key": "interface",
                    "ui_type": "SETTING_UI_TYPE_CHECKBOX"
                })),
                extensions: Vec::new(),
            }
        ]);

        let resolved = resolve_media_urn("media:custom-setting;setting", Some(&media_specs), &registry).await.unwrap();
        assert!(resolved.metadata.is_some());
        let metadata = resolved.metadata.unwrap();
        assert_eq!(metadata.get("category_key").unwrap(), "interface");
        assert_eq!(metadata.get("ui_type").unwrap(), "SETTING_UI_TYPE_CHECKBOX");
    }

    // TEST106: Test metadata and validation can coexist in media spec definition
    #[tokio::test]
    async fn test106_metadata_with_validation() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:bounded-number;numeric;setting".to_string(),
                media_type: "text/plain".to_string(),
                title: "Bounded Number".to_string(),
                profile_uri: Some("https://example.com/schema".to_string()),
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
                extensions: Vec::new(),
            }
        ]);

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
    }

    // -------------------------------------------------------------------------
    // Extension field tests
    // -------------------------------------------------------------------------

    // TEST107: Test extensions field propagates from media spec def to resolved
    #[tokio::test]
    async fn test107_extensions_propagation() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:custom-pdf".to_string(),
                media_type: "application/pdf".to_string(),
                title: "PDF Document".to_string(),
                profile_uri: Some("https://capns.org/schema/pdf".to_string()),
                schema: None,
                description: Some("A PDF document".to_string()),
                validation: None,
                metadata: None,
                extensions: vec!["pdf".to_string()],
            }
        ]);

        let resolved = resolve_media_urn("media:custom-pdf", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.extensions, vec!["pdf".to_string()]);
    }

    // TEST108: Test extensions serializes/deserializes correctly in MediaSpecDef
    #[test]
    fn test108_extensions_serialization() {
        let def = MediaSpecDef {
            urn: "media:json-data".to_string(),
            media_type: "application/json".to_string(),
            title: "JSON Data".to_string(),
            profile_uri: Some("https://example.com/profile".to_string()),
            schema: None,
            description: None,
            validation: None,
            metadata: None,
            extensions: vec!["json".to_string()],
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"extensions\":[\"json\"]"));

        // Deserialize and verify
        let parsed: MediaSpecDef = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extensions, vec!["json".to_string()]);
    }

    // TEST109: Test extensions can coexist with metadata and validation
    #[tokio::test]
    async fn test109_extensions_with_metadata_and_validation() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:custom-output;json".to_string(),
                media_type: "application/json".to_string(),
                title: "Custom Output".to_string(),
                profile_uri: Some("https://example.com/schema".to_string()),
                schema: None,
                description: None,
                validation: Some(MediaValidation {
                    min: None,
                    max: None,
                    min_length: Some(1),
                    max_length: Some(1000),
                    pattern: None,
                    allowed_values: None,
                }),
                metadata: Some(serde_json::json!({
                    "category": "output"
                })),
                extensions: vec!["json".to_string()],
            }
        ]);

        let resolved = resolve_media_urn("media:custom-output;json", Some(&media_specs), &registry).await.unwrap();

        // Verify all fields are present
        assert!(resolved.validation.is_some());
        assert!(resolved.metadata.is_some());
        assert_eq!(resolved.extensions, vec!["json".to_string()]);
    }

    // TEST110: Test multiple extensions in a media spec
    #[tokio::test]
    async fn test110_multiple_extensions() {
        let registry = test_registry().await;
        let media_specs = create_media_specs(vec![
            MediaSpecDef {
                urn: "media:image;jpeg".to_string(),
                media_type: "image/jpeg".to_string(),
                title: "JPEG Image".to_string(),
                profile_uri: Some("https://capns.org/schema/jpeg".to_string()),
                schema: None,
                description: Some("JPEG image data".to_string()),
                validation: None,
                metadata: None,
                extensions: vec!["jpg".to_string(), "jpeg".to_string()],
            }
        ]);

        let resolved = resolve_media_urn("media:image;jpeg", Some(&media_specs), &registry).await.unwrap();
        assert_eq!(resolved.extensions, vec!["jpg".to_string(), "jpeg".to_string()]);
        assert_eq!(resolved.extensions.len(), 2);
    }
}
