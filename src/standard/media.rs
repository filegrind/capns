//! Standard media URN definitions for common data types
//!
//! This module re-exports the standard media URNs and profile URLs.
//!
//! ## Media URNs
//!
//! Use media URN constants (e.g., `MEDIA_STRING`) in `media_urn` fields of arguments and outputs.
//! These are well-known built-ins that don't need to be declared in `media_specs`.
//!
//! ## Example
//!
//! ```rust
//! use capns::{CapArgument, CapOutput};
//! use capns::standard::media::{MEDIA_STRING, MEDIA_OBJECT};
//!
//! let arg = CapArgument::new("input", MEDIA_STRING, "Input text", "--input");
//! let output = CapOutput::new(MEDIA_OBJECT, "JSON output");
//! ```

// Re-export media URN constants from media_urn module
pub use crate::media_urn::{
    MediaUrn, MediaUrnError,
    MEDIA_VOID, MEDIA_STRING, MEDIA_INTEGER, MEDIA_NUMBER, MEDIA_BOOLEAN, MEDIA_OBJECT,
    MEDIA_STRING_ARRAY, MEDIA_INTEGER_ARRAY, MEDIA_NUMBER_ARRAY, MEDIA_BOOLEAN_ARRAY, MEDIA_OBJECT_ARRAY,
    MEDIA_BINARY, MEDIA_FILE_PATH, MEDIA_FILE_PATH_ARRAY,
};

// Re-export profile URLs from media_spec (these are still valid)
pub use crate::media_spec::{
    SCHEMA_BASE,
    PROFILE_STR, PROFILE_INT, PROFILE_NUM, PROFILE_BOOL, PROFILE_OBJ,
    PROFILE_STR_ARRAY, PROFILE_INT_ARRAY, PROFILE_NUM_ARRAY, PROFILE_BOOL_ARRAY, PROFILE_OBJ_ARRAY,
    PROFILE_VOID,
};

// Re-export types from media_spec
pub use crate::media_spec::{
    MediaSpec, MediaSpecDef, MediaSpecDefObject, MediaSpecError,
    ResolvedMediaSpec, resolve_media_urn, is_builtin_media_urn,
};

// =============================================================================
// MEDIA SPEC BUILDERS (create resolved specs for testing/utility)
// =============================================================================

use std::collections::HashMap;

/// Create a ResolvedMediaSpec for string type
pub fn string_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_STRING, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for integer type
pub fn integer_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_INTEGER, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for number type
pub fn number_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_NUMBER, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for boolean type
pub fn boolean_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_BOOLEAN, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for JSON object type
pub fn json_object_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_OBJECT, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for binary/octet-stream type
pub fn octet_stream_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_BINARY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for string array type
pub fn string_array_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_STRING_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for integer array type
pub fn integer_array_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_INTEGER_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for number array type
pub fn number_array_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_NUMBER_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for boolean array type
pub fn boolean_array_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_BOOLEAN_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for JSON object array type
pub fn json_object_array_spec() -> ResolvedMediaSpec {
    resolve_media_urn(MEDIA_OBJECT_ARRAY, &HashMap::new()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_spec() {
        let spec = string_spec();
        assert_eq!(spec.media_urn, MEDIA_STRING);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_STR.to_string()));
        assert!(spec.is_text());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_integer_spec() {
        let spec = integer_spec();
        assert_eq!(spec.media_urn, MEDIA_INTEGER);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_INT.to_string()));
    }

    #[test]
    fn test_number_spec() {
        let spec = number_spec();
        assert_eq!(spec.media_urn, MEDIA_NUMBER);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_NUM.to_string()));
    }

    #[test]
    fn test_boolean_spec() {
        let spec = boolean_spec();
        assert_eq!(spec.media_urn, MEDIA_BOOLEAN);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_BOOL.to_string()));
    }

    #[test]
    fn test_json_object_spec() {
        let spec = json_object_spec();
        assert_eq!(spec.media_urn, MEDIA_OBJECT);
        assert_eq!(spec.media_type, "application/json");
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_octet_stream_spec() {
        let spec = octet_stream_spec();
        assert_eq!(spec.media_urn, MEDIA_BINARY);
        assert_eq!(spec.media_type, "application/octet-stream");
        assert!(spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_array_specs() {
        // Primitive arrays are NOT keyed (no keyed tag), but are textable sequences
        let str_array = string_array_spec();
        assert!(!str_array.is_json()); // No keyed tag
        assert!(str_array.is_text()); // Has textable tag
        assert_eq!(str_array.profile_uri, Some(PROFILE_STR_ARRAY.to_string()));

        let int_array = integer_array_spec();
        assert!(!int_array.is_json()); // No keyed tag
        assert!(int_array.is_text()); // Has textable tag

        let num_array = number_array_spec();
        assert!(!num_array.is_json()); // No keyed tag
        assert!(num_array.is_text()); // Has textable tag

        let bool_array = boolean_array_spec();
        assert!(!bool_array.is_json()); // No keyed tag
        assert!(bool_array.is_text()); // Has textable tag

        // Object arrays ARE keyed (have keyed tag)
        let obj_array = json_object_array_spec();
        assert!(obj_array.is_json()); // Has keyed tag
        assert!(obj_array.is_text()); // Also has textable tag
    }

    #[test]
    fn test_media_urn_constants() {
        // Verify media URNs have expected format
        assert!(MEDIA_STRING.starts_with("media:"));
        assert!(MEDIA_INTEGER.starts_with("media:"));
        assert!(MEDIA_OBJECT.starts_with("media:"));
        assert!(MEDIA_BINARY.starts_with("media:"));
    }

    #[test]
    fn test_profile_constants() {
        // Verify profile URLs have expected format
        assert!(PROFILE_STR.starts_with("https://capns.org/schema/"));
        assert!(PROFILE_OBJ.starts_with("https://capns.org/schema/"));
    }
}
