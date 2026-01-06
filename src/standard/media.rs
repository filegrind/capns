//! Standard media spec definitions for common data types
//!
//! This module provides convenience functions for creating MediaSpec values
//! for standard data types used across capabilities.

use crate::MediaSpec;

// =============================================================================
// PROFILE URLs
// =============================================================================

/// Base URL for capns schemas
pub const SCHEMA_BASE: &str = "https://capns.org/schemas";

/// Profile URL for string type
pub const PROFILE_STRING: &str = "https://capns.org/schemas/str";

/// Profile URL for integer type
pub const PROFILE_INTEGER: &str = "https://capns.org/schemas/int";

/// Profile URL for number type
pub const PROFILE_NUMBER: &str = "https://capns.org/schemas/num";

/// Profile URL for boolean type
pub const PROFILE_BOOLEAN: &str = "https://capns.org/schemas/bool";

/// Profile URL for JSON object type
pub const PROFILE_JSON_OBJECT: &str = "https://capns.org/schemas/obj";

/// Profile URL for string array type
pub const PROFILE_STRING_ARRAY: &str = "https://capns.org/schemas/str-array";

/// Profile URL for number array type
pub const PROFILE_NUMBER_ARRAY: &str = "https://capns.org/schemas/num-array";

/// Profile URL for boolean array type
pub const PROFILE_BOOLEAN_ARRAY: &str = "https://capns.org/schemas/bool-array";

/// Profile URL for JSON object array type
pub const PROFILE_JSON_OBJECT_ARRAY: &str = "https://capns.org/schemas/obj-array";

// =============================================================================
// MEDIA SPEC STRINGS
// =============================================================================

/// Media spec string for JSON string output
pub const MEDIA_STRING: &str = "content-type: application/json; profile=\"https://capns.org/schemas/str\"";

/// Media spec string for JSON integer output
pub const MEDIA_INTEGER: &str = "content-type: application/json; profile=\"https://capns.org/schemas/int\"";

/// Media spec string for JSON number output
pub const MEDIA_NUMBER: &str = "content-type: application/json; profile=\"https://capns.org/schemas/num\"";

/// Media spec string for JSON boolean output
pub const MEDIA_BOOLEAN: &str = "content-type: application/json; profile=\"https://capns.org/schemas/bool\"";

/// Media spec string for JSON object output
pub const MEDIA_JSON_OBJECT: &str = "content-type: application/json; profile=\"https://capns.org/schemas/obj\"";

/// Media spec string for binary/octet-stream output
pub const MEDIA_OCTET_STREAM: &str = "content-type: application/octet-stream";

/// Media spec string for string array output
pub const MEDIA_STRING_ARRAY: &str = "content-type: application/json; profile=\"https://capns.org/schemas/str-array\"";

/// Media spec string for number array output
pub const MEDIA_NUMBER_ARRAY: &str = "content-type: application/json; profile=\"https://capns.org/schemas/num-array\"";

/// Media spec string for boolean array output
pub const MEDIA_BOOLEAN_ARRAY: &str = "content-type: application/json; profile=\"https://capns.org/schemas/bool-array\"";

/// Media spec string for JSON object array output
pub const MEDIA_JSON_OBJECT_ARRAY: &str = "content-type: application/json; profile=\"https://capns.org/schemas/obj-array\"";

// =============================================================================
// MEDIA SPEC BUILDERS
// =============================================================================

/// Create MediaSpec for JSON string
pub fn string_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_STRING.to_string()),
    }
}

/// Create MediaSpec for JSON integer
pub fn integer_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_INTEGER.to_string()),
    }
}

/// Create MediaSpec for JSON number
pub fn number_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_NUMBER.to_string()),
    }
}

/// Create MediaSpec for JSON boolean
pub fn boolean_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_BOOLEAN.to_string()),
    }
}

/// Create MediaSpec for JSON object
pub fn json_object_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_JSON_OBJECT.to_string()),
    }
}

/// Create MediaSpec for binary/octet-stream
pub fn octet_stream_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/octet-stream".to_string(),
        profile: None,
    }
}

/// Create MediaSpec for string array
pub fn string_array_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_STRING_ARRAY.to_string()),
    }
}

/// Create MediaSpec for number array
pub fn number_array_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_NUMBER_ARRAY.to_string()),
    }
}

/// Create MediaSpec for boolean array
pub fn boolean_array_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_BOOLEAN_ARRAY.to_string()),
    }
}

/// Create MediaSpec for JSON object array
pub fn json_object_array_spec() -> MediaSpec {
    MediaSpec {
        content_type: "application/json".to_string(),
        profile: Some(PROFILE_JSON_OBJECT_ARRAY.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_spec() {
        let spec = string_spec();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some(PROFILE_STRING.to_string()));
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_integer_spec() {
        let spec = integer_spec();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some(PROFILE_INTEGER.to_string()));
        assert!(spec.is_json());
    }

    #[test]
    fn test_number_spec() {
        let spec = number_spec();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some(PROFILE_NUMBER.to_string()));
        assert!(spec.is_json());
    }

    #[test]
    fn test_boolean_spec() {
        let spec = boolean_spec();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some(PROFILE_BOOLEAN.to_string()));
        assert!(spec.is_json());
    }

    #[test]
    fn test_json_object_spec() {
        let spec = json_object_spec();
        assert_eq!(spec.content_type, "application/json");
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_octet_stream_spec() {
        let spec = octet_stream_spec();
        assert_eq!(spec.content_type, "application/octet-stream");
        assert!(spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_array_specs() {
        let str_array = string_array_spec();
        assert!(str_array.is_json());
        assert_eq!(str_array.profile, Some(PROFILE_STRING_ARRAY.to_string()));

        let num_array = number_array_spec();
        assert!(num_array.is_json());

        let bool_array = boolean_array_spec();
        assert!(bool_array.is_json());

        let obj_array = json_object_array_spec();
        assert!(obj_array.is_json());
    }

    #[test]
    fn test_media_string_constants() {
        // Verify the string constants can be parsed
        let spec = MediaSpec::parse(MEDIA_STRING).unwrap();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some(PROFILE_STRING.to_string()));

        let json_spec = MediaSpec::parse(MEDIA_JSON_OBJECT).unwrap();
        assert_eq!(json_spec.content_type, "application/json");

        let binary_spec = MediaSpec::parse(MEDIA_OCTET_STREAM).unwrap();
        assert!(binary_spec.is_binary());
    }
}
