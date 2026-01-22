//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all FGND providers, including their formal argument specifications.
//! These definitions should match the TOML definitions in capns_dot_org/standard/

use crate::{
    Cap, CapRegistry, CapUrn, CapUrnBuilder, MEDIA_DISBOUND_PAGES, MEDIA_DOCUMENT_OUTLINE, MEDIA_FILE_METADATA, MediaUrn, RegistryError
};
use crate::media_urn::{
    MEDIA_VOID, MEDIA_STRING, MEDIA_INTEGER, MEDIA_BOOLEAN, MEDIA_OBJECT, MEDIA_BINARY,
    MEDIA_BOOLEAN_ARRAY,
    MEDIA_PNG, MEDIA_AUDIO, MEDIA_VIDEO, MEDIA_TEXT,
    // Document types (PRIMARY naming)
    MEDIA_PDF, MEDIA_EPUB,
    // Text format types (PRIMARY naming)
    MEDIA_MD, MEDIA_TXT, MEDIA_RST, MEDIA_LOG,
    // Semantic text input types
    MEDIA_INPUT_TEXT, MEDIA_PROMPT_TEXT, MEDIA_QUERY_TEXT, MEDIA_CONTENT_TEXT, MEDIA_FRONTMATTER_TEXT,
    MEDIA_MODEL_ID, MEDIA_MODEL_SPEC, MEDIA_HF_MODEL_NAME, MEDIA_MLX_MODEL_PATH, MEDIA_MANAGEMENT_OPERATION,
    // CAPNS output types
    MEDIA_DOWNLOAD_OUTPUT, MEDIA_LOAD_OUTPUT, MEDIA_UNLOAD_OUTPUT,
    MEDIA_LIST_OUTPUT, MEDIA_STATUS_OUTPUT, MEDIA_CONTENTS_OUTPUT,
    MEDIA_GENERATE_OUTPUT, MEDIA_STRUCTURED_QUERY_OUTPUT, MEDIA_LLM_INFERENCE_OUTPUT,
};
use std::sync::Arc;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================


/// Get the input media URN for a file extension
///
/// Uses PRIMARY type naming where the type IS the format.
/// - Document files (pdf, epub): type=pdf, type=epub
/// - Text format files (md, txt, rst, log): type=md, type=txt, etc.
/// - Generic/unknown: type=binary (fallback)
pub fn input_media_urn_for_ext(ext: Option<&str>) -> &'static str {
    match ext {
        // Document types (PRIMARY naming)
        Some("pdf") => MEDIA_PDF,
        Some("epub") => MEDIA_EPUB,
        // Text format types (PRIMARY naming)
        Some("md") => MEDIA_MD,
        Some("txt") => MEDIA_TXT,
        Some("rst") => MEDIA_RST,
        Some("log") => MEDIA_LOG,
        // Semantic types
        Some("text") => MEDIA_TEXT,
        // Fallbacks
        None => MEDIA_BINARY,
        Some(_) => MEDIA_BINARY,
    }
}

// =============================================================================
// URN BUILDER FUNCTIONS (synchronous, return CapUrn directly)
// =============================================================================
// These are the SINGLE SOURCE OF TRUTH for URN construction.
// All _cap functions below MUST use these to build URNs.

// -----------------------------------------------------------------------------
// LLM URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for conversation capability
pub fn llm_conversation_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "conversation")
        .solo_tag("constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_PROMPT_TEXT)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build conversation cap URN")
}

/// Build URN for multiplechoice capability
pub fn llm_multiplechoice_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "multiplechoice")
        .solo_tag("constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_QUERY_TEXT)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build multiplechoice cap URN")
}

/// Build URN for codegeneration capability
pub fn llm_codegeneration_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "codegeneration")
        .solo_tag("constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_PROMPT_TEXT)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build codegeneration cap URN")
}

/// Build URN for creative capability
pub fn llm_creative_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "creative")
        .solo_tag("constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_PROMPT_TEXT)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build creative cap URN")
}

/// Build URN for summarization capability
pub fn llm_summarization_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "summarization")
        .solo_tag("constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_INPUT_TEXT)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build summarization cap URN")
}

// -----------------------------------------------------------------------------
// EMBEDDING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for embeddings-dimensions capability
pub fn embeddings_dimensions_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "embeddings_dimensions")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_INTEGER)
        .build()
        .expect("Failed to build embeddings-dimensions cap URN")
}

/// Build URN for embeddings-generation capability
pub fn embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .in_spec(MEDIA_INPUT_TEXT)
        .out_spec(MEDIA_GENERATE_OUTPUT)
        .build()
        .expect("Failed to build embeddings-generation cap URN")
}

// -----------------------------------------------------------------------------
// MODEL MANAGEMENT URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for model-download capability
pub fn model_download_urn() -> CapUrn {
		CapUrnBuilder::new()
		.tag("op", "download-model")
        .in_spec(MEDIA_MODEL_ID)
        .out_spec(MEDIA_DOWNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-download cap URN")
}

/// Build URN for model-load capability
pub fn model_load_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "load-model")
        .in_spec(MEDIA_MODEL_ID)
        .out_spec(MEDIA_LOAD_OUTPUT)
        .build()
        .expect("Failed to build model-load cap URN")
}

/// Build URN for model-unload capability
pub fn model_unload_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "unload-model")
        .in_spec(MEDIA_MODEL_ID)
        .out_spec(MEDIA_UNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-unload cap URN")
}

/// Build URN for model-list capability
pub fn model_list_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "list-models")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_LIST_OUTPUT)
        .build()
        .expect("Failed to build model-list cap URN")
}

/// Build URN for model-status capability
pub fn model_status_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-status")
        .in_spec(MEDIA_MODEL_ID)
        .out_spec(MEDIA_STATUS_OUTPUT)
        .build()
        .expect("Failed to build model-status cap URN")
}

/// Build URN for model-contents capability
pub fn model_contents_urn() -> CapUrn {
    CapUrnBuilder::new()
		.tag("op", "model-contents")
        .in_spec(MEDIA_MODEL_ID)
        .out_spec(MEDIA_CONTENTS_OUTPUT)
        .build()
        .expect("Failed to build model-contents cap URN")
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for generate-thumbnail capability
///
/// If `ext` is Some, builds a URN with the extension tag and appropriate input type.
/// If `ext` is None, builds a generic fallback URN that matches binary files.
/// Output is always an image (PNG thumbnail).
///
/// Input types by extension (PRIMARY type naming):
/// - pdf: media:pdf;binary
/// - epub: media:epub;binary
/// - md: media:md;textable
/// - txt: media:txt;textable
/// - rst: media:rst;textable
/// - log: media:log;textable
/// - text: media:text;textable
/// - None/other: media:raw;binary
pub fn generate_thumbnail_urn(ext: Option<&str>) -> CapUrn {
    let input_spec = input_media_urn_for_ext(ext);

    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec(input_spec)
        .out_spec(MEDIA_PNG)
        .build()
        .expect("Failed to build generate-thumbnail cap URN")
}

/// Build URN for disbind capability
pub fn disbind_urn(ext: Option<&str>) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "disbind")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(MEDIA_DISBOUND_PAGES)
        .build()
        .expect("Failed to build disbind cap URN")
}

/// Build URN for extract-metadata capability
pub fn extract_metadata_urn(ext: Option<&str>) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(MEDIA_FILE_METADATA)
        .build()
        .expect("Failed to build extract-metadata cap URN")
}

/// Build URN for extract-outline capability
pub fn extract_outline_urn(ext: Option<&str>) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(MEDIA_DOCUMENT_OUTLINE)
        .build()
        .expect("Failed to build extract-outline cap URN")
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for frontmatter-summarization capability
pub fn frontmatter_summarization_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_frontmatter_summary")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_FRONTMATTER_TEXT)
        .out_spec(MEDIA_STRING)
        .build()
        .expect("Failed to build frontmatter-summarization cap URN")
}

/// Build URN for structured-query capability
pub fn structured_query_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "query_structured")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_OBJECT)
        .out_spec(MEDIA_STRUCTURED_QUERY_OUTPUT)
        .build()
        .expect("Failed to build structured-query cap URN")
}

/// Build URN for bit-choice capability
pub fn bit_choice_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "choose_bit")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_CONTENT_TEXT)
        .out_spec(MEDIA_BOOLEAN)
        .build()
        .expect("Failed to build bit-choice cap URN")
}

/// Build URN for bit-choices capability
pub fn bit_choices_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "choose_bits")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_CONTENT_TEXT)
        .out_spec(MEDIA_BOOLEAN_ARRAY)
        .build()
        .expect("Failed to build bit-choices cap URN")
}

// -----------------------------------------------------------------------------
// FGND-SPECIFIC TASK URN BUILDERS
// -----------------------------------------------------------------------------
// Note: These are legitimate task capabilities for document analysis workflows.
// They represent phases of document processing, NOT tool wrappers.

/// Build URN for recategorization-task capability
/// Input: binary document data
/// Output: categorization result object
pub fn recategorization_task_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "recategorize_listing")
        .tag("language", lang_code)
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Categorization results
        .build()
        .expect("Failed to build recategorization-task cap URN")
}

/// Build URN for listing-analysis-task capability
/// Input: binary document data
/// Output: analysis result object
pub fn listing_analysis_task_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "request_listing_analysis")
        .tag("language", lang_code)
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Analysis results
        .build()
        .expect("Failed to build listing-analysis-task cap URN")
}

// -----------------------------------------------------------------------------
// COERCION URN BUILDERS
// -----------------------------------------------------------------------------
// Coercion is converting data from one media type to another.
// Each coercion is a cap with a specific input and output type.

/// Build URN for coercing any type to string
/// Input: source data (any textable type)
/// Output: string representation
pub fn coerce_to_string_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "string")
}

/// Build URN for coercing to integer
/// Input: source data (numeric or parseable string)
/// Output: integer
pub fn coerce_to_integer_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "integer")
}

/// Build URN for coercing to number
/// Input: source data (numeric or parseable string)
/// Output: number
pub fn coerce_to_number_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "number")
}

/// Build URN for coercing to object
/// Input: any data type
/// Output: JSON object (possibly wrapped)
pub fn coerce_to_object_urn(source_type: &str) -> CapUrn {
    coercion_urn(source_type, "object")
}

/// Map a type name to its full media URN constant
fn media_urn_for_type(type_name: &str) -> &'static str {
    match type_name {
        "string" => MEDIA_STRING,
        "integer" => MEDIA_INTEGER,
        "number" => crate::media_urn::MEDIA_NUMBER,
        "boolean" => MEDIA_BOOLEAN,
        "object" => MEDIA_OBJECT,
        "string-array" => crate::media_urn::MEDIA_STRING_ARRAY,
        "integer-array" => crate::media_urn::MEDIA_INTEGER_ARRAY,
        "number-array" => crate::media_urn::MEDIA_NUMBER_ARRAY,
        "boolean-array" => crate::media_urn::MEDIA_BOOLEAN_ARRAY,
        "object-array" => crate::media_urn::MEDIA_OBJECT_ARRAY,
        other => panic!("Unknown media type: {}. Valid types are: string, integer, number, boolean, object, string-array, integer-array, number-array, boolean-array, object-array", other),
    }
}

/// Build a generic coercion URN given source and target types
/// Panics if source_type or target_type is not a known media type
pub fn coercion_urn(source_type: &str, target_type: &str) -> CapUrn {
    let in_spec = media_urn_for_type(source_type);
    let out_spec = media_urn_for_type(target_type);
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", target_type)
        .in_spec(in_spec)
        .out_spec(out_spec)
        .build()
        .expect("Failed to build coercion cap URN")
}

/// Get list of all valid coercion paths
/// Returns (source_type, target_type) pairs for all supported coercions
pub fn all_coercion_paths() -> Vec<(&'static str, &'static str)> {
    vec![
        // To string (from all textable types)
        ("integer", "string"),
        ("number", "string"),
        ("boolean", "string"),
        ("object", "string"),
        ("string-array", "string"),
        ("integer-array", "string"),
        ("number-array", "string"),
        ("boolean-array", "string"),
        ("object-array", "string"),
        // To integer
        ("string", "integer"),
        ("number", "integer"),
        ("boolean", "integer"),
        // To number
        ("string", "number"),
        ("integer", "number"),
        ("boolean", "number"),
        // To object (wrap in object)
        ("string", "object"),
        ("integer", "object"),
        ("number", "object"),
        ("boolean", "object"),
    ]
}

// =============================================================================
// REGISTRY LOOKUP FUNCTIONS (async, return Cap from registry)
// =============================================================================
// These functions use the _urn functions above to build URNs, then look up
// the capability from the registry.

// -----------------------------------------------------------------------------
// LLM CAPABILITIES
// -----------------------------------------------------------------------------

/// Get conversation cap from registry with language
pub async fn llm_conversation(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = llm_conversation_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get multiplechoice cap from registry with language
pub async fn llm_multiplechoice(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = llm_multiplechoice_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get codegeneration cap from registry with language
pub async fn llm_codegeneration(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = llm_codegeneration_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get creative cap from registry with language
pub async fn llm_creative(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = llm_creative_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get summarization cap from registry with language
pub async fn llm_summarization(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = llm_summarization_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// EMBEDDING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get embeddings-dimensions cap from registry
pub async fn embeddings_dimensions_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = embeddings_dimensions_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get embeddings-generation cap from registry
pub async fn embeddings_generation_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = embeddings_generation_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// MODEL MANAGEMENT CAPABILITIES
// -----------------------------------------------------------------------------

/// Get model download cap from registry
pub async fn model_download_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_download_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model load cap from registry
pub async fn model_load_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_load_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model unload cap from registry
pub async fn model_unload_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_unload_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model list cap from registry
pub async fn model_list_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_list_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model status cap from registry
pub async fn model_status_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_status_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model contents cap from registry
pub async fn model_contents_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_contents_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get extract-metadata cap from registry
pub async fn extract_metadata_cap(registry: Arc<CapRegistry>, ext: Option<&str>) -> Result<Cap, RegistryError> {
    let urn = extract_metadata_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get generate-thumbnail cap from registry
pub async fn generate_thumbnail_cap(registry: Arc<CapRegistry>, ext: Option<&str>) -> Result<Cap, RegistryError> {
    let urn = generate_thumbnail_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get extract-outline cap from registry
pub async fn extract_outline_cap(registry: Arc<CapRegistry>, ext: Option<&str>) -> Result<Cap, RegistryError> {
    let urn = extract_outline_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get disbind cap from registry
pub async fn disbind_cap(registry: Arc<CapRegistry>, ext: Option<&str>) -> Result<Cap, RegistryError> {
    let urn = disbind_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get frontmatter-summarization cap from registry
pub async fn frontmatter_summarization_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = frontmatter_summarization_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get structured-query cap from registry
pub async fn structured_query_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = structured_query_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get bit-choice cap from registry
pub async fn bit_choice_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = bit_choice_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get bit-choices cap from registry
pub async fn bit_choices_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = bit_choices_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// COERCION CAPABILITIES
// -----------------------------------------------------------------------------

/// Get a single coercion cap from registry
pub async fn coercion_cap(registry: Arc<CapRegistry>, source_type: &str, target_type: &str) -> Result<Cap, RegistryError> {
    let urn = coercion_urn(source_type, target_type);
    registry.get_cap(&urn.to_string()).await
}

/// Get all coercion caps from registry
/// Returns a vector of (source_type, target_type, Cap) tuples
/// Fails if any coercion cap is missing from the registry
pub async fn all_coercion_caps(registry: Arc<CapRegistry>) -> Result<Vec<(&'static str, &'static str, Cap)>, RegistryError> {
    let mut caps = Vec::new();
    for (source_type, target_type) in all_coercion_paths() {
        let cap = coercion_cap(registry.clone(), source_type, target_type).await?;
        caps.push((source_type, target_type, cap));
    }
    Ok(caps)
}
