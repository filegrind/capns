//! MediaSpec parsing, spec ID resolution, and media type handling
//!
//! This module provides:
//! - Spec ID resolution (e.g., `capns:ms:str.v1` → resolved media spec)
//! - MediaSpec parsing (canonical form: `text/plain; profile=https://...`)
//! - MediaSpecDef for defining specs in cap definitions
//!
//! ## Spec ID Format
//! Spec IDs are references like `capns:ms:str.v1` that resolve to media type definitions.
//! Built-in primitives are available without explicit declaration.
//!
//! ## MediaSpec Format
//! Canonical form: `<media-type>; profile=<url>`
//! Example: `text/plain; profile=https://capns.org/schema/str`
//!
//! Note: The old `content-type:` prefix is NO LONGER SUPPORTED.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// =============================================================================
// SPEC ID CONSTANTS
// =============================================================================

/// Spec ID for string type
pub const SPEC_ID_STR: &str = "capns:ms:str.v1";
/// Spec ID for integer type
pub const SPEC_ID_INT: &str = "capns:ms:int.v1";
/// Spec ID for number type
pub const SPEC_ID_NUM: &str = "capns:ms:num.v1";
/// Spec ID for boolean type
pub const SPEC_ID_BOOL: &str = "capns:ms:bool.v1";
/// Spec ID for JSON object type
pub const SPEC_ID_OBJ: &str = "capns:ms:obj.v1";
/// Spec ID for string array type
pub const SPEC_ID_STR_ARRAY: &str = "capns:ms:str-array.v1";
/// Spec ID for integer array type
pub const SPEC_ID_INT_ARRAY: &str = "capns:ms:int-array.v1";
/// Spec ID for number array type
pub const SPEC_ID_NUM_ARRAY: &str = "capns:ms:num-array.v1";
/// Spec ID for boolean array type
pub const SPEC_ID_BOOL_ARRAY: &str = "capns:ms:bool-array.v1";
/// Spec ID for object array type
pub const SPEC_ID_OBJ_ARRAY: &str = "capns:ms:obj-array.v1";
/// Spec ID for binary data
pub const SPEC_ID_BINARY: &str = "capns:ms:binary.v1";

// =============================================================================
// PROFILE URLS (new canonical /schema/ path)
// =============================================================================

/// Base URL for capns schemas
pub const SCHEMA_BASE: &str = "https://capns.org/schema";

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

// =============================================================================
// MEDIA SPEC DEFINITION (for cap definitions)
// =============================================================================

/// Media spec definition - can be string (compact) or object (rich)
///
/// Used in the `media_specs` map of a cap definition.
///
/// ## String Form (compact)
/// ```json
/// "capns:ms:str.v1": "text/plain; profile=https://capns.org/schema/str"
/// ```
///
/// ## Object Form (rich, with optional local schema)
/// ```json
/// "my:output-spec.v1": {
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
        })
    }
}

// =============================================================================
// RESOLVED MEDIA SPEC
// =============================================================================

/// Fully resolved media spec with all fields populated
///
/// This is the result of resolving a spec ID through the media_specs table
/// or from a built-in definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMediaSpec {
    /// The spec ID that was resolved
    pub spec_id: String,
    /// The MIME media type (e.g., "application/json", "text/plain")
    pub media_type: String,
    /// Optional profile URI
    pub profile_uri: Option<String>,
    /// Optional local JSON Schema for validation
    pub schema: Option<serde_json::Value>,
}

impl ResolvedMediaSpec {
    /// Check if this represents binary data
    pub fn is_binary(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt.starts_with("image/")
            || mt.starts_with("audio/")
            || mt.starts_with("video/")
            || mt == "application/octet-stream"
            || mt == "application/pdf"
            || mt.starts_with("application/x-")
            || mt.contains("+zip")
            || mt.contains("+gzip")
    }

    /// Check if this represents JSON data
    pub fn is_json(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt == "application/json" || mt.ends_with("+json")
    }

    /// Check if this represents text data
    pub fn is_text(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt.starts_with("text/") || (!self.is_binary() && !self.is_json())
    }
}

// =============================================================================
// SPEC ID RESOLUTION
// =============================================================================

/// Resolve a spec ID to a full media spec definition
///
/// Resolution order:
/// 1. Look up in provided media_specs map
/// 2. If not found, check if it's a built-in primitive
/// 3. If neither, fail hard with an error
///
/// # Arguments
/// * `spec_id` - The spec ID to resolve (e.g., "capns:ms:str.v1")
/// * `media_specs` - The media_specs map from the cap definition
///
/// # Errors
/// Returns `MediaSpecError::UnresolvableSpecId` if the spec ID cannot be resolved.
pub fn resolve_spec_id(
    spec_id: &str,
    media_specs: &HashMap<String, MediaSpecDef>,
) -> Result<ResolvedMediaSpec, MediaSpecError> {
    // First, try to look up in the provided media_specs
    if let Some(def) = media_specs.get(spec_id) {
        return resolve_def(spec_id, def);
    }

    // Second, check if it's a built-in primitive
    if let Some(resolved) = resolve_builtin(spec_id) {
        return Ok(resolved);
    }

    // Fail hard - no fallbacks
    Err(MediaSpecError::UnresolvableSpecId(spec_id.to_string()))
}

/// Resolve a MediaSpecDef to a ResolvedMediaSpec
fn resolve_def(spec_id: &str, def: &MediaSpecDef) -> Result<ResolvedMediaSpec, MediaSpecError> {
    match def {
        MediaSpecDef::String(s) => {
            let parsed = MediaSpec::parse(s)?;
            Ok(ResolvedMediaSpec {
                spec_id: spec_id.to_string(),
                media_type: parsed.media_type,
                profile_uri: parsed.profile,
                schema: None,
            })
        }
        MediaSpecDef::Object(obj) => Ok(ResolvedMediaSpec {
            spec_id: spec_id.to_string(),
            media_type: obj.media_type.clone(),
            profile_uri: Some(obj.profile_uri.clone()),
            schema: obj.schema.clone(),
        }),
    }
}

/// Resolve a built-in spec ID
fn resolve_builtin(spec_id: &str) -> Option<ResolvedMediaSpec> {
    let (media_type, profile_uri) = match spec_id {
        SPEC_ID_STR => ("text/plain", Some(PROFILE_STR)),
        SPEC_ID_INT => ("text/plain", Some(PROFILE_INT)),
        SPEC_ID_NUM => ("text/plain", Some(PROFILE_NUM)),
        SPEC_ID_BOOL => ("text/plain", Some(PROFILE_BOOL)),
        SPEC_ID_OBJ => ("application/json", Some(PROFILE_OBJ)),
        SPEC_ID_STR_ARRAY => ("application/json", Some(PROFILE_STR_ARRAY)),
        SPEC_ID_INT_ARRAY => ("application/json", Some(PROFILE_INT_ARRAY)),
        SPEC_ID_NUM_ARRAY => ("application/json", Some(PROFILE_NUM_ARRAY)),
        SPEC_ID_BOOL_ARRAY => ("application/json", Some(PROFILE_BOOL_ARRAY)),
        SPEC_ID_OBJ_ARRAY => ("application/json", Some(PROFILE_OBJ_ARRAY)),
        SPEC_ID_BINARY => ("application/octet-stream", None),
        _ => return None,
    };

    Some(ResolvedMediaSpec {
        spec_id: spec_id.to_string(),
        media_type: media_type.to_string(),
        profile_uri: profile_uri.map(String::from),
        schema: None, // Built-ins don't have local schemas
    })
}

/// Check if a spec ID is a built-in primitive
pub fn is_builtin_spec_id(spec_id: &str) -> bool {
    matches!(
        spec_id,
        SPEC_ID_STR
            | SPEC_ID_INT
            | SPEC_ID_NUM
            | SPEC_ID_BOOL
            | SPEC_ID_OBJ
            | SPEC_ID_STR_ARRAY
            | SPEC_ID_INT_ARRAY
            | SPEC_ID_NUM_ARRAY
            | SPEC_ID_BOOL_ARRAY
            | SPEC_ID_OBJ_ARRAY
            | SPEC_ID_BINARY
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
    ///
    /// Note: The old `content-type:` prefix is NO LONGER supported.
    pub fn parse(s: &str) -> Result<Self, MediaSpecError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(MediaSpecError::EmptyMediaType);
        }

        // Check for old content-type: prefix and FAIL HARD
        let lower = s.to_lowercase();
        if lower.starts_with("content-type:") {
            return Err(MediaSpecError::LegacyContentTypePrefix(
                "media_spec must not start with 'content-type:' - use canonical form '<media-type>; profile=<url>'".to_string()
            ));
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

    /// Check if this media spec represents binary output
    pub fn is_binary(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt.starts_with("image/")
            || mt.starts_with("audio/")
            || mt.starts_with("video/")
            || mt == "application/octet-stream"
            || mt == "application/pdf"
            || mt.starts_with("application/x-")
            || mt.contains("+zip")
            || mt.contains("+gzip")
    }

    /// Check if this media spec represents JSON output
    pub fn is_json(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt == "application/json" || mt.ends_with("+json")
    }

    /// Check if this media spec represents text output
    pub fn is_text(&self) -> bool {
        let mt = self.media_type.to_lowercase();
        mt.starts_with("text/") || (!self.is_binary() && !self.is_json())
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
    /// Spec ID cannot be resolved (not in media_specs and not a built-in)
    UnresolvableSpecId(String),
    /// Legacy content-type: prefix detected (not supported)
    LegacyContentTypePrefix(String),
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
            MediaSpecError::UnresolvableSpecId(id) => {
                write!(
                    f,
                    "cannot resolve spec ID '{}' - not found in media_specs and not a built-in primitive",
                    id
                )
            }
            MediaSpecError::LegacyContentTypePrefix(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

impl std::error::Error for MediaSpecError {}

// =============================================================================
// BACKWARD COMPATIBILITY ALIASES (for old field name)
// =============================================================================

// These are provided so existing code that uses `content_type` field can still work
// during migration. The canonical field name is now `media_type`.
impl MediaSpec {
    /// Alias for media_type (backward compatibility)
    #[deprecated(note = "use media_type instead")]
    pub fn content_type(&self) -> &str {
        &self.media_type
    }
}

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
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_parse_text_with_profile() {
        let spec = MediaSpec::parse("text/plain; profile=https://capns.org/schema/str").unwrap();
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(
            spec.profile,
            Some("https://capns.org/schema/str".to_string())
        );
        assert!(spec.is_text());
        assert!(!spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_parse_binary() {
        let spec = MediaSpec::parse("application/octet-stream").unwrap();
        assert_eq!(spec.media_type, "application/octet-stream");
        assert!(spec.profile.is_none());
        assert!(spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_parse_image() {
        let spec = MediaSpec::parse("image/png; profile=https://example.com/thumbnail").unwrap();
        assert_eq!(spec.media_type, "image/png");
        assert!(spec.is_binary());
        assert!(!spec.is_json());
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
    fn test_legacy_content_type_prefix_fails() {
        let result =
            MediaSpec::parse("content-type: application/json; profile=https://example.com");
        assert!(result.is_err());
        if let Err(MediaSpecError::LegacyContentTypePrefix(_)) = result {
            // Expected
        } else {
            panic!("Expected LegacyContentTypePrefix error");
        }
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
    // Spec ID resolution tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_resolve_builtin_str() {
        let media_specs = HashMap::new();
        let resolved = resolve_spec_id(SPEC_ID_STR, &media_specs).unwrap();
        assert_eq!(resolved.spec_id, SPEC_ID_STR);
        assert_eq!(resolved.media_type, "text/plain");
        assert_eq!(resolved.profile_uri, Some(PROFILE_STR.to_string()));
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_builtin_obj() {
        let media_specs = HashMap::new();
        let resolved = resolve_spec_id(SPEC_ID_OBJ, &media_specs).unwrap();
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(resolved.profile_uri, Some(PROFILE_OBJ.to_string()));
    }

    #[test]
    fn test_resolve_builtin_binary() {
        let media_specs = HashMap::new();
        let resolved = resolve_spec_id(SPEC_ID_BINARY, &media_specs).unwrap();
        assert_eq!(resolved.media_type, "application/octet-stream");
        assert!(resolved.profile_uri.is_none());
        assert!(resolved.is_binary());
    }

    #[test]
    fn test_resolve_custom_string_form() {
        let mut media_specs = HashMap::new();
        media_specs.insert(
            "my:custom-spec.v1".to_string(),
            MediaSpecDef::String("application/json; profile=https://example.com/schema".to_string()),
        );

        let resolved = resolve_spec_id("my:custom-spec.v1", &media_specs).unwrap();
        assert_eq!(resolved.spec_id, "my:custom-spec.v1");
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
            "my:output-spec.v1".to_string(),
            MediaSpecDef::Object(MediaSpecDefObject {
                media_type: "application/json".to_string(),
                profile_uri: "https://example.com/schema/output".to_string(),
                schema: Some(schema.clone()),
            }),
        );

        let resolved = resolve_spec_id("my:output-spec.v1", &media_specs).unwrap();
        assert_eq!(resolved.spec_id, "my:output-spec.v1");
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
        let result = resolve_spec_id("unknown:spec.v1", &media_specs);
        assert!(result.is_err());
        if let Err(MediaSpecError::UnresolvableSpecId(id)) = result {
            assert_eq!(id, "unknown:spec.v1");
        } else {
            panic!("Expected UnresolvableSpecId error");
        }
    }

    #[test]
    fn test_custom_overrides_builtin() {
        // Custom definition in media_specs takes precedence over built-in
        let mut media_specs = HashMap::new();
        media_specs.insert(
            SPEC_ID_STR.to_string(),
            MediaSpecDef::String("application/json; profile=https://custom.example.com/str".to_string()),
        );

        let resolved = resolve_spec_id(SPEC_ID_STR, &media_specs).unwrap();
        // Custom definition used, not built-in
        assert_eq!(resolved.media_type, "application/json");
        assert_eq!(
            resolved.profile_uri,
            Some("https://custom.example.com/str".to_string())
        );
    }

    #[test]
    fn test_is_builtin_spec_id() {
        assert!(is_builtin_spec_id(SPEC_ID_STR));
        assert!(is_builtin_spec_id(SPEC_ID_INT));
        assert!(is_builtin_spec_id(SPEC_ID_BINARY));
        assert!(!is_builtin_spec_id("my:custom-spec.v1"));
        assert!(!is_builtin_spec_id("random-string"));
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
        });
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("\"media_type\":\"application/json\""));
        assert!(json.contains("\"profile_uri\":\"https://example.com/profile\""));
        // None schema is skipped - check it's not serialized as "schema":null or "schema":...
        assert!(!json.contains("\"schema\":"));
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
            spec_id: "test".to_string(),
            media_type: "application/octet-stream".to_string(),
            profile_uri: None,
            schema: None,
        };
        assert!(resolved.is_binary());
        assert!(!resolved.is_json());
    }

    #[test]
    fn test_resolved_is_json() {
        let resolved = ResolvedMediaSpec {
            spec_id: "test".to_string(),
            media_type: "application/json".to_string(),
            profile_uri: None,
            schema: None,
        };
        assert!(resolved.is_json());
        assert!(!resolved.is_binary());
    }

    #[test]
    fn test_resolved_is_text() {
        let resolved = ResolvedMediaSpec {
            spec_id: "test".to_string(),
            media_type: "text/plain".to_string(),
            profile_uri: None,
            schema: None,
        };
        assert!(resolved.is_text());
        assert!(!resolved.is_binary());
        assert!(!resolved.is_json());
    }
}
