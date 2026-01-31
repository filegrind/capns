//! Standard media URN definitions for common data types
//!
//! This module re-exports the standard media URNs and profile URLs.
//!
//! ## Media URNs
//!
//! Use media URN constants (e.g., `MEDIA_STRING`) in `media_urn` fields of arguments and outputs.
//! These are well-known types defined in the media registry (capns_dot_org/standard/media/).
//!
//! ## Resolution
//!
//! To resolve a media URN to its full spec, use `resolve_media_urn` with a `MediaUrnRegistry`.
//! The resolution order is:
//! 1. Cap's local media_specs (cap-specific overrides)
//! 2. Registry's local cache (bundled standard specs)
//! 3. Online registry fetch (with graceful degradation if unreachable)
//!
//! ## Example
//!
//! ```rust
//! use capns::{CapArg, ArgSource, CapOutput};
//! use capns::standard::media::{MEDIA_STRING, MEDIA_OBJECT};
//!
//! let arg = CapArg::new(MEDIA_STRING, true, vec![ArgSource::CliFlag { cli_flag: "--input".to_string() }]);
//! let output = CapOutput::new(MEDIA_OBJECT, "JSON output");
//! ```

// Re-export media URN constants from media_urn module
pub use crate::media_urn::{
    MediaUrn, MediaUrnError,
    MEDIA_VOID, MEDIA_STRING, MEDIA_INTEGER, MEDIA_NUMBER, MEDIA_BOOLEAN, MEDIA_OBJECT,
    MEDIA_STRING_ARRAY, MEDIA_INTEGER_ARRAY, MEDIA_NUMBER_ARRAY, MEDIA_BOOLEAN_ARRAY, MEDIA_OBJECT_ARRAY,
    MEDIA_BINARY, MEDIA_FILE_PATH, MEDIA_FILE_PATH_ARRAY,
    // Semantic AI input types
    MEDIA_PNG, MEDIA_AUDIO_SPEECH,
    MEDIA_MODEL_SPEC, MEDIA_MODEL_REPO, MEDIA_JSON_SCHEMA,
    // Semantic output types
    MEDIA_IMAGE_THUMBNAIL,
    MEDIA_MODEL_DIM, MEDIA_DECISION, MEDIA_DECISION_ARRAY,
};

// Re-export profile URLs from media_spec
pub use crate::media_spec::{
    SCHEMA_BASE,
    PROFILE_STR, PROFILE_INT, PROFILE_NUM, PROFILE_BOOL, PROFILE_OBJ,
    PROFILE_STR_ARRAY, PROFILE_INT_ARRAY, PROFILE_NUM_ARRAY, PROFILE_BOOL_ARRAY, PROFILE_OBJ_ARRAY,
    PROFILE_VOID,
};

// Re-export types and resolution function from media_spec
pub use crate::media_spec::{
    MediaSpecDef, MediaSpecError, ResolvedMediaSpec,
    resolve_media_urn, validate_media_specs_no_duplicates,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_media_urn_constants_format() {
        // Verify media URNs have expected format
        assert!(MEDIA_STRING.starts_with("media:"));
        assert!(MEDIA_INTEGER.starts_with("media:"));
        assert!(MEDIA_OBJECT.starts_with("media:"));
        assert!(MEDIA_BINARY.starts_with("media:"));
    }

    #[test]
    fn test_profile_constants_format() {
        // Verify profile URLs have expected format
        assert!(PROFILE_STR.starts_with("https://capns.org/schema/"));
        assert!(PROFILE_OBJ.starts_with("https://capns.org/schema/"));
    }
}
