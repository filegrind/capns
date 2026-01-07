//! Standard media spec definitions for common data types
//!
//! This module re-exports the standard spec IDs and profile URLs from media_spec.rs
//! and provides convenience functions for creating MediaSpec values.
//!
//! ## Spec IDs
//!
//! Use spec IDs (e.g., `SPEC_ID_STR`) in `media_spec` fields of arguments and outputs.
//! These spec IDs are well-known built-ins that don't need to be declared in `media_specs`.
//!
//! ## Example
//!
//! ```rust
//! use capns::{CapArgument, CapOutput};
//! use capns::standard::media::{SPEC_ID_STR, SPEC_ID_OBJ};
//!
//! let arg = CapArgument::new("input", SPEC_ID_STR, "Input text", "--input");
//! let output = CapOutput::new(SPEC_ID_OBJ, "JSON output");
//! ```

// Re-export spec IDs from media_spec
pub use crate::media_spec::{
    SPEC_ID_STR, SPEC_ID_INT, SPEC_ID_NUM, SPEC_ID_BOOL, SPEC_ID_OBJ,
    SPEC_ID_STR_ARRAY, SPEC_ID_INT_ARRAY, SPEC_ID_NUM_ARRAY, SPEC_ID_BOOL_ARRAY, SPEC_ID_OBJ_ARRAY,
    SPEC_ID_BINARY,
};

// Re-export profile URLs from media_spec
pub use crate::media_spec::{
    SCHEMA_BASE,
    PROFILE_STR, PROFILE_INT, PROFILE_NUM, PROFILE_BOOL, PROFILE_OBJ,
    PROFILE_STR_ARRAY, PROFILE_INT_ARRAY, PROFILE_NUM_ARRAY, PROFILE_BOOL_ARRAY, PROFILE_OBJ_ARRAY,
};

// Re-export types from media_spec
pub use crate::media_spec::{
    MediaSpec, MediaSpecDef, MediaSpecDefObject, MediaSpecError,
    ResolvedMediaSpec, resolve_spec_id, is_builtin_spec_id,
};

// =============================================================================
// LEGACY ALIASES (for backward compatibility during migration)
// =============================================================================

/// Alias for SPEC_ID_STR (legacy name)
#[deprecated(note = "use SPEC_ID_STR instead")]
pub const MEDIA_STRING: &str = SPEC_ID_STR;

/// Alias for SPEC_ID_INT (legacy name)
#[deprecated(note = "use SPEC_ID_INT instead")]
pub const MEDIA_INTEGER: &str = SPEC_ID_INT;

/// Alias for SPEC_ID_NUM (legacy name)
#[deprecated(note = "use SPEC_ID_NUM instead")]
pub const MEDIA_NUMBER: &str = SPEC_ID_NUM;

/// Alias for SPEC_ID_BOOL (legacy name)
#[deprecated(note = "use SPEC_ID_BOOL instead")]
pub const MEDIA_BOOLEAN: &str = SPEC_ID_BOOL;

/// Alias for SPEC_ID_OBJ (legacy name)
#[deprecated(note = "use SPEC_ID_OBJ instead")]
pub const MEDIA_JSON_OBJECT: &str = SPEC_ID_OBJ;

/// Alias for SPEC_ID_BINARY (legacy name)
#[deprecated(note = "use SPEC_ID_BINARY instead")]
pub const MEDIA_OCTET_STREAM: &str = SPEC_ID_BINARY;

/// Alias for SPEC_ID_STR_ARRAY (legacy name)
#[deprecated(note = "use SPEC_ID_STR_ARRAY instead")]
pub const MEDIA_STRING_ARRAY: &str = SPEC_ID_STR_ARRAY;

/// Alias for SPEC_ID_NUM_ARRAY (legacy name)
#[deprecated(note = "use SPEC_ID_NUM_ARRAY instead")]
pub const MEDIA_NUMBER_ARRAY: &str = SPEC_ID_NUM_ARRAY;

/// Alias for SPEC_ID_BOOL_ARRAY (legacy name)
#[deprecated(note = "use SPEC_ID_BOOL_ARRAY instead")]
pub const MEDIA_BOOLEAN_ARRAY: &str = SPEC_ID_BOOL_ARRAY;

/// Alias for SPEC_ID_OBJ_ARRAY (legacy name)
#[deprecated(note = "use SPEC_ID_OBJ_ARRAY instead")]
pub const MEDIA_JSON_OBJECT_ARRAY: &str = SPEC_ID_OBJ_ARRAY;

// =============================================================================
// LEGACY PROFILE ALIASES (for backward compatibility during migration)
// =============================================================================

/// Alias for PROFILE_STR (old name)
#[deprecated(note = "use PROFILE_STR instead")]
pub const PROFILE_STRING: &str = PROFILE_STR;

/// Alias for PROFILE_INT (old name)
#[deprecated(note = "use PROFILE_INT instead")]
pub const PROFILE_INTEGER: &str = PROFILE_INT;

/// Alias for PROFILE_NUM (old name)
#[deprecated(note = "use PROFILE_NUM instead")]
pub const PROFILE_NUMBER: &str = PROFILE_NUM;

/// Alias for PROFILE_BOOL (old name)
#[deprecated(note = "use PROFILE_BOOL instead")]
pub const PROFILE_BOOLEAN: &str = PROFILE_BOOL;

/// Alias for PROFILE_OBJ (old name)
#[deprecated(note = "use PROFILE_OBJ instead")]
pub const PROFILE_JSON_OBJECT: &str = PROFILE_OBJ;

/// Alias for PROFILE_STR_ARRAY (old name)
#[deprecated(note = "use PROFILE_STR_ARRAY instead")]
pub const PROFILE_STRING_ARRAY: &str = PROFILE_STR_ARRAY;

/// Alias for PROFILE_NUM_ARRAY (old name)
#[deprecated(note = "use PROFILE_NUM_ARRAY instead")]
pub const PROFILE_NUMBER_ARRAY: &str = PROFILE_NUM_ARRAY;

/// Alias for PROFILE_BOOL_ARRAY (old name)
#[deprecated(note = "use PROFILE_BOOL_ARRAY instead")]
pub const PROFILE_BOOLEAN_ARRAY: &str = PROFILE_BOOL_ARRAY;

/// Alias for PROFILE_OBJ_ARRAY (old name)
#[deprecated(note = "use PROFILE_OBJ_ARRAY instead")]
pub const PROFILE_JSON_OBJECT_ARRAY: &str = PROFILE_OBJ_ARRAY;

// =============================================================================
// MEDIA SPEC BUILDERS (create resolved specs for testing/utility)
// =============================================================================

use std::collections::HashMap;

/// Create a ResolvedMediaSpec for string type
pub fn string_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_STR, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for integer type
pub fn integer_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_INT, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for number type
pub fn number_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_NUM, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for boolean type
pub fn boolean_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_BOOL, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for JSON object type
pub fn json_object_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_OBJ, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for binary/octet-stream type
pub fn octet_stream_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_BINARY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for string array type
pub fn string_array_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_STR_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for integer array type
pub fn integer_array_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_INT_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for number array type
pub fn number_array_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_NUM_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for boolean array type
pub fn boolean_array_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_BOOL_ARRAY, &HashMap::new()).unwrap()
}

/// Create a ResolvedMediaSpec for JSON object array type
pub fn json_object_array_spec() -> ResolvedMediaSpec {
    resolve_spec_id(SPEC_ID_OBJ_ARRAY, &HashMap::new()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_spec() {
        let spec = string_spec();
        assert_eq!(spec.spec_id, SPEC_ID_STR);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_STR.to_string()));
        assert!(spec.is_text());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_integer_spec() {
        let spec = integer_spec();
        assert_eq!(spec.spec_id, SPEC_ID_INT);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_INT.to_string()));
    }

    #[test]
    fn test_number_spec() {
        let spec = number_spec();
        assert_eq!(spec.spec_id, SPEC_ID_NUM);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_NUM.to_string()));
    }

    #[test]
    fn test_boolean_spec() {
        let spec = boolean_spec();
        assert_eq!(spec.spec_id, SPEC_ID_BOOL);
        assert_eq!(spec.media_type, "text/plain");
        assert_eq!(spec.profile_uri, Some(PROFILE_BOOL.to_string()));
    }

    #[test]
    fn test_json_object_spec() {
        let spec = json_object_spec();
        assert_eq!(spec.spec_id, SPEC_ID_OBJ);
        assert_eq!(spec.media_type, "application/json");
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_octet_stream_spec() {
        let spec = octet_stream_spec();
        assert_eq!(spec.spec_id, SPEC_ID_BINARY);
        assert_eq!(spec.media_type, "application/octet-stream");
        assert!(spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_array_specs() {
        let str_array = string_array_spec();
        assert!(str_array.is_json());
        assert_eq!(str_array.profile_uri, Some(PROFILE_STR_ARRAY.to_string()));

        let int_array = integer_array_spec();
        assert!(int_array.is_json());

        let num_array = number_array_spec();
        assert!(num_array.is_json());

        let bool_array = boolean_array_spec();
        assert!(bool_array.is_json());

        let obj_array = json_object_array_spec();
        assert!(obj_array.is_json());
    }

    #[test]
    fn test_spec_id_constants() {
        // Verify spec IDs have expected format
        assert!(SPEC_ID_STR.starts_with("capns:ms:"));
        assert!(SPEC_ID_INT.starts_with("capns:ms:"));
        assert!(SPEC_ID_OBJ.starts_with("capns:ms:"));
        assert!(SPEC_ID_BINARY.starts_with("capns:ms:"));
    }

    #[test]
    fn test_profile_constants() {
        // Verify profile URLs have expected format
        assert!(PROFILE_STR.starts_with("https://capns.org/schema/"));
        assert!(PROFILE_OBJ.starts_with("https://capns.org/schema/"));
    }
}
