//! Standard media URN definitions for common data types
//!
//! This module re-exports the standard media URNs and profile URLs.
//!
//! ## Media URNs
//!
//! Use media URN constants (e.g., `MEDIA_STRING`) in `media_urn` fields of arguments and outputs.
//! These are well-known types defined in the media registry (capgraph/src/media/).
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
//! use capdag::{CapArg, ArgSource, CapOutput};
//! use capdag::standard::media::{MEDIA_STRING, MEDIA_OBJECT};
//!
//! let arg = CapArg::new(MEDIA_STRING, true, vec![ArgSource::CliFlag { cli_flag: "--input".to_string() }]);
//! let output = CapOutput::new(MEDIA_OBJECT, "JSON output");
//! ```

// Re-export media URN constants from media_urn module
pub use crate::urn::media_urn::{
    MediaUrn,
    MediaUrnError,
    MEDIA_AUDIO_SPEECH,
    MEDIA_BOOLEAN,
    MEDIA_BOOLEAN_LIST,
    MEDIA_DECISION,
    MEDIA_FILE_PATH,
    MEDIA_FILE_PATH_ARRAY,
    MEDIA_HTML,
    MEDIA_IDENTITY,
    MEDIA_INTEGER,
    MEDIA_INTEGER_LIST,
    MEDIA_JSON,
    MEDIA_JSON_SCHEMA,
    MEDIA_LOG,
    // Text format types
    MEDIA_MD,
    MEDIA_MODEL_DIM,
    MEDIA_MODEL_REPO,
    MEDIA_MODEL_SPEC,
    MEDIA_MODEL_SPEC_CANDLE_EMBEDDINGS,
    MEDIA_MODEL_SPEC_CANDLE_IMAGE_EMBEDDINGS,
    MEDIA_MODEL_SPEC_CANDLE_LLM,
    MEDIA_MODEL_SPEC_CANDLE_TRANSCRIPTION,
    MEDIA_MODEL_SPEC_CANDLE_VISION,
    MEDIA_MODEL_SPEC_GGUF_EMBEDDINGS,
    MEDIA_MODEL_SPEC_GGUF_LLM,
    // Backend+use-case model-spec variants
    MEDIA_MODEL_SPEC_GGUF_VISION,
    MEDIA_MODEL_SPEC_LLM,
    MEDIA_MODEL_SPEC_MLX_EMBEDDINGS,
    MEDIA_MODEL_SPEC_MLX_LLM,
    MEDIA_MODEL_SPEC_MLX_VISION,
    MEDIA_NUMBER,
    MEDIA_NUMBER_LIST,
    MEDIA_OBJECT,
    MEDIA_OBJECT_LIST,
    // Semantic AI input types
    MEDIA_PNG,
    MEDIA_RST,
    MEDIA_STRING,
    MEDIA_STRING_LIST,
    // Semantic output types
    MEDIA_TEXTABLE_PAGE,
    MEDIA_TXT,
    MEDIA_VOID,
    MEDIA_XML,
    MEDIA_YAML,
};

// Re-export profile URLs from media_spec
pub use crate::media::spec::{
    PROFILE_BOOL, PROFILE_BOOL_ARRAY, PROFILE_INT, PROFILE_INT_ARRAY, PROFILE_NUM,
    PROFILE_NUM_ARRAY, PROFILE_OBJ, PROFILE_OBJ_ARRAY, PROFILE_STR, PROFILE_STR_ARRAY,
    PROFILE_VOID, SCHEMA_BASE,
};

// Re-export types and resolution function from media_spec
pub use crate::media::spec::{
    resolve_media_urn, validate_media_specs_no_duplicates, MediaSpecDef, MediaSpecError,
    ResolvedMediaSpec,
};

#[cfg(test)]
mod tests {
    use super::*;

    // TEST628: Verify media URN constants all start with "media:" prefix
    #[test]
    fn test628_media_urn_constants_format() {
        // Verify media URNs have expected format
        assert!(MEDIA_STRING.starts_with("media:"));
        assert!(MEDIA_INTEGER.starts_with("media:"));
        assert!(MEDIA_OBJECT.starts_with("media:"));
        assert!(MEDIA_IDENTITY.starts_with("media:"));
    }

    // TEST629: Verify profile URL constants all start with capdag.com schema prefix
    #[test]
    fn test629_profile_constants_format() {
        // Verify profile URLs have expected format
        assert!(PROFILE_STR.starts_with("https://capdag.com/schema/"));
        assert!(PROFILE_OBJ.starts_with("https://capdag.com/schema/"));
    }
}
