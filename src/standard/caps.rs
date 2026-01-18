//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all FGND providers, including their formal argument specifications.
//! These definitions should match the TOML definitions in capns_dot_org/standard/

use crate::{
    Cap, CapUrn, CapUrnBuilder, CapRegistry, RegistryError
};
use crate::media_urn::{
    MEDIA_VOID, MEDIA_STRING, MEDIA_INTEGER, MEDIA_BOOLEAN, MEDIA_OBJECT, MEDIA_BINARY,
    MEDIA_BOOLEAN_ARRAY, MEDIA_STRING_ARRAY,
    MEDIA_IMAGE, MEDIA_AUDIO, MEDIA_VIDEO, MEDIA_TEXT,
    // Document types (PRIMARY naming)
    MEDIA_PDF, MEDIA_EPUB,
    // Text format types (PRIMARY naming)
    MEDIA_MD, MEDIA_TXT, MEDIA_RST, MEDIA_LOG,
    // CAPNS output types
    MEDIA_DOWNLOAD_OUTPUT, MEDIA_LOAD_OUTPUT, MEDIA_UNLOAD_OUTPUT,
    MEDIA_LIST_OUTPUT, MEDIA_STATUS_OUTPUT, MEDIA_CONTENTS_OUTPUT,
    MEDIA_GENERATE_OUTPUT, MEDIA_STRUCTURED_QUERY_OUTPUT, MEDIA_LLM_INFERENCE_OUTPUT,
    MEDIA_EXTRACT_METADATA_OUTPUT, MEDIA_EXTRACT_OUTLINE_OUTPUT, MEDIA_GRIND_OUTPUT,
};
use std::sync::Arc;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Get the input media URN for a given file extension
///
/// Returns media URNs using PRIMARY type naming (type IS the format):
/// - pdf: media:type=pdf;v=1;binary
/// - md: media:type=md;v=1;textable
/// - txt/text: media:type=txt;v=1;textable
/// - rst: media:type=rst;v=1;textable
/// - log: media:type=log;v=1;textable
pub fn input_media_urn_for_ext(ext: &str) -> &'static str {
    match ext {
        "pdf" => MEDIA_PDF,
        "epub" => MEDIA_EPUB,
        "md" => MEDIA_MD,
        "txt" | "text" => MEDIA_TXT,
        "rst" => MEDIA_RST,
        "log" => MEDIA_LOG,
        _ => MEDIA_STRING,
    }
}

/// Get the output media URN for extract-metadata operation by extension
///
/// - PDF files: MEDIA_EXTRACT_METADATA_OUTPUT (has full schema)
/// - Text files: MEDIA_OBJECT (generic JSON object)
pub fn extract_metadata_output_media_urn_for_ext(ext: &str) -> &'static str {
    match ext {
        "pdf" => MEDIA_EXTRACT_METADATA_OUTPUT,
        "md" | "rst" | "log" | "txt" | "text" | _ => MEDIA_OBJECT,
    }
}

/// Get the output media URN for extract-outline operation by extension
///
/// - PDF files: MEDIA_EXTRACT_OUTLINE_OUTPUT (has full schema)
/// - Text files: MEDIA_OBJECT (generic JSON object)
pub fn extract_outline_output_media_urn_for_ext(ext: &str) -> &'static str {
    match ext {
        "pdf" => MEDIA_EXTRACT_OUTLINE_OUTPUT,
        "md" | "rst" | "log" | "txt" | "text" | _ => MEDIA_OBJECT,
    }
}

/// Get the output media URN for grind operation by extension
///
/// - PDF files: MEDIA_GRIND_OUTPUT (has full schema, array of chunks)
/// - Text files: MEDIA_OBJECT (generic JSON object)
pub fn grind_output_media_urn_for_ext(ext: &str) -> &'static str {
    match ext {
        "pdf" => MEDIA_GRIND_OUTPUT,
        "md" | "rst" | "log" | "txt" | "text" | _ => MEDIA_OBJECT,
    }
}

/// Get the input media URN for thumbnail generation by extension
///
/// Uses PRIMARY type naming where the type IS the format.
/// - Document files (pdf, epub): type=pdf, type=epub
/// - Text format files (md, txt, rst, log): type=md, type=txt, etc.
/// - Generic/unknown: type=binary (fallback)
pub fn thumbnail_input_media_urn_for_ext(ext: Option<&str>) -> &'static str {
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
        .tag("type", "constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build conversation cap URN")
}

/// Build URN for multiplechoice capability
pub fn llm_multiplechoice_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "multiplechoice")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build multiplechoice cap URN")
}

/// Build URN for codegeneration capability
pub fn llm_codegeneration_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "codegeneration")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build codegeneration cap URN")
}

/// Build URN for creative capability
pub fn llm_creative_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "creative")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_LLM_INFERENCE_OUTPUT)
        .build()
        .expect("Failed to build creative cap URN")
}

/// Build URN for summarization capability
pub fn llm_summarization_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "summarization")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .in_spec(MEDIA_STRING)
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
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_INTEGER)
        .build()
        .expect("Failed to build embeddings-dimensions cap URN")
}

/// Build URN for embeddings-generation capability
pub fn embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .in_spec(MEDIA_STRING)
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
        .tag("op", "download")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_DOWNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-download cap URN")
}

/// Build URN for model-load capability
pub fn model_load_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "load")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_LOAD_OUTPUT)
        .build()
        .expect("Failed to build model-load cap URN")
}

/// Build URN for model-unload capability
pub fn model_unload_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "unload")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_UNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-unload cap URN")
}

/// Build URN for model-list capability
pub fn model_list_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "list")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_LIST_OUTPUT)
        .build()
        .expect("Failed to build model-list cap URN")
}

/// Build URN for model-status capability
pub fn model_status_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "status")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
        .out_spec(MEDIA_STATUS_OUTPUT)
        .build()
        .expect("Failed to build model-status cap URN")
}

/// Build URN for model-contents capability
pub fn model_contents_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "contents")
        .tag("type", "model")
        .in_spec(MEDIA_VOID)
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
/// - pdf: media:type=pdf;v=1;binary
/// - epub: media:type=epub;v=1;binary
/// - md: media:type=md;v=1;textable
/// - txt: media:type=txt;v=1;textable
/// - rst: media:type=rst;v=1;textable
/// - log: media:type=log;v=1;textable
/// - text: media:type=text;v=1;textable
/// - None/other: media:type=binary;v=1;binary
pub fn generate_thumbnail_urn(ext: Option<&str>) -> CapUrn {
    let input_spec = thumbnail_input_media_urn_for_ext(ext);

    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec(input_spec)
        .out_spec(MEDIA_IMAGE)
        .build()
        .expect("Failed to build generate-thumbnail cap URN")
}

/// Build URN for grind capability
pub fn grind_urn(ext: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "grind")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(grind_output_media_urn_for_ext(ext))
        .build()
        .expect("Failed to build grind cap URN")
}

/// Build URN for extract-metadata capability
pub fn extract_metadata_urn(ext: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(extract_metadata_output_media_urn_for_ext(ext))
        .build()
        .expect("Failed to build extract-metadata cap URN")
}

/// Build URN for extract-outline capability
pub fn extract_outline_urn(ext: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec(input_media_urn_for_ext(ext))
        .out_spec(extract_outline_output_media_urn_for_ext(ext))
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
        .tag("type", "constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_STRING)
        .build()
        .expect("Failed to build frontmatter-summarization cap URN")
}

/// Build URN for structured-query capability
pub fn structured_query_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "query_structured")
        .tag("language", lang_code)
        .tag("type", "constrained")
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
        .tag("type", "constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_BOOLEAN)
        .build()
        .expect("Failed to build bit-choice cap URN")
}

/// Build URN for bit-choices capability
pub fn bit_choices_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "choose_bits")
        .tag("language", lang_code)
        .tag("type", "constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_BOOLEAN_ARRAY)
        .build()
        .expect("Failed to build bit-choices cap URN")
}

// -----------------------------------------------------------------------------
// FGND-SPECIFIC TASK URN BUILDERS
// -----------------------------------------------------------------------------
// Note: These caps now use proper media types instead of entity IDs.
// Tasks are implicit - every cap application produces a task.
// The output describes what the COMPLETED task produces, not the task itself.

/// Build URN for scan-files-task capability
/// Input: array of file paths to scan
/// Output: result object with scan summary
pub fn scan_files_task_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "scan_files")
        .in_spec(MEDIA_STRING_ARRAY) // Array of file paths
        .out_spec(MEDIA_OBJECT) // Scan results
        .build()
        .expect("Failed to build scan-files-task cap URN")
}

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
// FGND TOOL URN BUILDERS
// -----------------------------------------------------------------------------
// These caps describe what data they need, not entity IDs.
// The delivery mechanism (file paths) is an implementation detail.

/// Build URN for grinder tool capability
/// Input: binary document data
/// Output: grind results (chunks)
pub fn grinder_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_grinder")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Grind results
        .build()
        .expect("Failed to build grinder tool cap URN")
}

/// Build URN for quick-summary tool capability
/// Input: binary document data
/// Output: summary result object
pub fn quick_summary_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_quick_summary")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Summary results
        .build()
        .expect("Failed to build quick-summary tool cap URN")
}

/// Build URN for detailed-analysis tool capability
/// Input: binary document data
/// Output: detailed analysis result object
pub fn detailed_analysis_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_detailed_analysis")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Analysis results
        .build()
        .expect("Failed to build detailed-analysis tool cap URN")
}

/// Build URN for outline-extraction tool capability
/// Input: binary document data
/// Output: outline result object
pub fn outline_extraction_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_outline_extraction")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Outline results
        .build()
        .expect("Failed to build outline-extraction tool cap URN")
}

/// Build URN for embedding-generation tool capability
/// Input: binary document data
/// Output: embeddings result object
pub fn embedding_generation_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_embedding_generation")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Embeddings results
        .build()
        .expect("Failed to build embedding-generation tool cap URN")
}

/// Build URN for recategorize tool capability
/// Input: binary document data
/// Output: categorization result object
pub fn recategorize_tool_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "use_recategorize")
        .in_spec(MEDIA_BINARY) // Binary document
        .out_spec(MEDIA_OBJECT) // Categorization results
        .build()
        .expect("Failed to build recategorize tool cap URN")
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
    let in_spec = format!("media:type={};v=1", source_type);
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", "string")
        .in_spec(&in_spec)
        .out_spec(MEDIA_STRING)
        .build()
        .expect("Failed to build coerce-to-string cap URN")
}

/// Build URN for coercing to integer
/// Input: source data (numeric or parseable string)
/// Output: integer
pub fn coerce_to_integer_urn(source_type: &str) -> CapUrn {
    let in_spec = format!("media:type={};v=1", source_type);
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", "integer")
        .in_spec(&in_spec)
        .out_spec(MEDIA_INTEGER)
        .build()
        .expect("Failed to build coerce-to-integer cap URN")
}

/// Build URN for coercing to number
/// Input: source data (numeric or parseable string)
/// Output: number
pub fn coerce_to_number_urn(source_type: &str) -> CapUrn {
    let in_spec = format!("media:type={};v=1", source_type);
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", "number")
        .in_spec(&in_spec)
        .out_spec(crate::media_urn::MEDIA_NUMBER)
        .build()
        .expect("Failed to build coerce-to-number cap URN")
}

/// Build URN for coercing to object
/// Input: any data type
/// Output: JSON object (possibly wrapped)
pub fn coerce_to_object_urn(source_type: &str) -> CapUrn {
    let in_spec = format!("media:type={};v=1", source_type);
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", "object")
        .in_spec(&in_spec)
        .out_spec(MEDIA_OBJECT)
        .build()
        .expect("Failed to build coerce-to-object cap URN")
}

/// Build a generic coercion URN given source and target types
/// Panics if target_type is not a known media type
pub fn coercion_urn(source_type: &str, target_type: &str) -> CapUrn {
    let in_spec = format!("media:type={};v=1", source_type);
    let out_spec = match target_type {
        "string" => MEDIA_STRING,
        "integer" => MEDIA_INTEGER,
        "number" => crate::media_urn::MEDIA_NUMBER,
        "boolean" => MEDIA_BOOLEAN,
        "object" => MEDIA_OBJECT,
        other => panic!("Unknown coercion target type: {}. Valid types are: string, integer, number, boolean, object", other),
    };
    CapUrnBuilder::new()
        .tag("op", "coerce")
        .tag("target", target_type)
        .in_spec(&in_spec)
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
pub async fn extract_metadata_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = extract_metadata_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get generate-thumbnail cap from registry
pub async fn generate_thumbnail_cap(registry: Arc<CapRegistry>, ext: Option<&str>) -> Result<Cap, RegistryError> {
    let urn = generate_thumbnail_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get extract-outline cap from registry
pub async fn extract_outline_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = extract_outline_urn(ext);
    registry.get_cap(&urn.to_string()).await
}

/// Get grind cap from registry
pub async fn grind_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = grind_urn(ext);
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
