//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all MACINA providers, including their formal argument specifications.
//! These definitions should match the TOML definitions in capgraph/src/

use crate::{Cap, CapOutput, CapRegistry, CapUrn, CapUrnBuilder, RegistryError};
use crate::urn::media_urn::{
    // Primitives (needed for coercion functions)
    MEDIA_STRING, MEDIA_INTEGER, MEDIA_BOOLEAN, MEDIA_OBJECT, MEDIA_IDENTITY,
    // Semantic media types
    MEDIA_PNG,
    // Document types
    MEDIA_PDF, MEDIA_EPUB,
    // Text format types
    MEDIA_MD, MEDIA_TXT, MEDIA_RST, MEDIA_LOG,
    // Semantic input types
    MEDIA_MODEL_SPEC,
    MEDIA_MODEL_REPO, MEDIA_JSON_SCHEMA,
    // Semantic output types
    MEDIA_TEXTABLE_PAGE_LIST,
    // CAPDAG output types
    MEDIA_MODEL_DIM, MEDIA_DOWNLOAD_OUTPUT,
    MEDIA_LIST_OUTPUT, MEDIA_STATUS_OUTPUT, MEDIA_CONTENTS_OUTPUT,
    MEDIA_AVAILABILITY_OUTPUT, MEDIA_PATH_OUTPUT,
    MEDIA_EMBEDDING_VECTOR, MEDIA_JSON, MEDIA_LLM_INFERENCE_OUTPUT,
    MEDIA_DECISION, MEDIA_DECISION_ARRAY, MEDIA_VOID,
    // Format conversion types (JSON, YAML, CSV variants)
    MEDIA_JSON_VALUE, MEDIA_JSON_RECORD, MEDIA_JSON_LIST, MEDIA_JSON_LIST_RECORD,
    MEDIA_YAML_VALUE, MEDIA_YAML_RECORD, MEDIA_YAML_LIST, MEDIA_YAML_LIST_RECORD,
    MEDIA_CSV,
    // Bare list types (no format tag)
    MEDIA_TEXTABLE_LIST,
};
use std::sync::Arc;

// =============================================================================
// STANDARD CAP URN CONSTANTS
// =============================================================================

/// Identity capability — the categorical identity morphism. MANDATORY in every capset.
/// Accepts any media type as input and outputs any media type.
pub const CAP_IDENTITY: &str = "cap:";

/// Discard capability — the terminal morphism. Standard, NOT mandatory.
/// Accepts any media type as input and produces void output.
/// The capdag lib provides a default implementation; plugins may override.
pub const CAP_DISCARD: &str = "cap:in=media:;out=media:void";

/// Parse and return the canonical identity `CapUrn` from `CAP_IDENTITY`.
pub fn identity_urn() -> CapUrn {
    CapUrn::from_string(CAP_IDENTITY)
        .unwrap_or_else(|e| panic!("BUG: CAP_IDENTITY constant is invalid: {}", e))
}

/// Parse and return the canonical discard `CapUrn` from `CAP_DISCARD`.
pub fn discard_urn() -> CapUrn {
    CapUrn::from_string(CAP_DISCARD)
        .unwrap_or_else(|e| panic!("BUG: CAP_DISCARD constant is invalid: {}", e))
}

/// Construct the canonical Identity `Cap` definition.
pub fn identity_cap() -> Cap {
    let urn = identity_urn();

    let mut cap = Cap::with_description(
        urn,
        "Identity".to_string(),
        "identity".to_string(),
        "The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set.".to_string(),
    );

    cap.set_output(crate::cap::definition::CapOutput::new("media:", "The input data, unchanged"));
    cap
}

/// Construct the canonical Discard `Cap` definition.
pub fn discard_cap() -> Cap {
    let urn = discard_urn();

    let mut cap = Cap::with_description(
        urn,
        "Discard".to_string(),
        "discard".to_string(),
        "The terminal morphism. Accepts any input and produces void output. Standard but not mandatory.".to_string(),
    );

    cap.set_output(crate::cap::definition::CapOutput::new(MEDIA_VOID, "Void (no output)"));
    cap
}


// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// CAP_IDENTITY: the categorical identity morphism — MANDATORY in every capset
// Canonical form of 'cap:' after wildcard expansion

// const IDENTITY_DEFINITION = {
//   urn: 'cap:in=media:;out=media:',
//   command: 'identity',
//   title: 'Identity',
//   cap_description: 'The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set.',
//   args: [],
//   output: {
//     media_urn: 'media:',
//     output_description: 'The input data, unchanged'
//   }
// };

// // CAP_DISCARD: the terminal morphism — standard, NOT mandatory
// const DISCARD_DEFINITION = {
//   urn: 'cap:in=media:;out=media:void',
//   command: 'discard',
//   title: 'Discard',
//   cap_description: 'The terminal morphism. Accepts any input and produces void output. Standard but not mandatory.',
//   args: [],
//   output: {
//     media_urn: 'media:void',
//     output_description: 'Void (no output)'
//   }
// };

// =============================================================================
// URN BUILDER FUNCTIONS (synchronous, return CapUrn directly)
// =============================================================================
// These are the SINGLE SOURCE OF TRUTH for URN construction.
// All _cap functions below MUST use these to build URNs.

// -----------------------------------------------------------------------------
// LLM URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for generic text-generation capability.
pub fn llm_generate_text_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_text")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_STRING)
        .build()
        .expect("Failed to build generate_text cap URN")
}

/// Build URN for multiplechoice capability
pub fn llm_multiplechoice_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "multiplechoice")
        .solo_tag("constrained")
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
        .solo_tag("constrained")
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
        .solo_tag("constrained")
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
        .solo_tag("constrained")
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
/// Output uses MEDIA_MODEL_DIM per CATALOG: media:model-dim;integer;textable;numeric
pub fn embeddings_dimensions_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "embeddings_dimensions")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_MODEL_DIM)
        .build()
        .expect("Failed to build embeddings-dimensions cap URN")
}

/// Build URN for text embeddings-generation capability
/// Input: media:textable (text)
/// Output: media:embedding-vector;textable;record
pub fn embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_EMBEDDING_VECTOR)
        .build()
        .expect("Failed to build embeddings-generation cap URN")
}

/// Build URN for image embeddings-generation capability
/// Input: media:image;png
/// Output: media:embedding-vector;textable;record
pub fn image_embeddings_generation_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_image_embeddings")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec(MEDIA_PNG)
        .out_spec(MEDIA_EMBEDDING_VECTOR)
        .build()
        .expect("Failed to build image-embeddings-generation cap URN")
}

// -----------------------------------------------------------------------------
// MODEL MANAGEMENT URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for model-download capability
pub fn model_download_urn() -> CapUrn {
		CapUrnBuilder::new()
		.tag("op", "download-model")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_DOWNLOAD_OUTPUT)
        .build()
        .expect("Failed to build model-download cap URN")
}

/// Build URN for model-list capability
/// Input uses MEDIA_MODEL_REPO per CATALOG: media:model-repo;textable;record
pub fn model_list_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "list-models")
        .in_spec(MEDIA_MODEL_REPO)
        .out_spec(MEDIA_LIST_OUTPUT)
        .build()
        .expect("Failed to build model-list cap URN")
}

/// Build URN for model-status capability
pub fn model_status_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-status")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_STATUS_OUTPUT)
        .build()
        .expect("Failed to build model-status cap URN")
}

/// Build URN for model-contents capability
pub fn model_contents_urn() -> CapUrn {
    CapUrnBuilder::new()
		.tag("op", "model-contents")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_CONTENTS_OUTPUT)
        .build()
        .expect("Failed to build model-contents cap URN")
}

/// Build URN for model-availability capability
pub fn model_availability_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-availability")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_AVAILABILITY_OUTPUT)
        .build()
        .expect("Failed to build model-availability cap URN")
}

/// Build URN for model-path capability
pub fn model_path_urn() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-path")
        .in_spec(MEDIA_MODEL_SPEC)
        .out_spec(MEDIA_PATH_OUTPUT)
        .build()
        .expect("Failed to build model-path cap URN")
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for page-image rendering capability.
///
/// `input_media` is the media URN for the input type (e.g., MEDIA_PDF, MEDIA_IDENTITY).
/// Output is always a PNG page image.
pub fn render_page_image_urn(input_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "render_page_image")
        .in_spec(input_media)
        .out_spec(MEDIA_PNG)
        .build()
        .expect("Failed to build render_page_image cap URN")
}

/// Build URN for disbind capability.
///
/// `input_media` is the media URN for the input type (e.g., MEDIA_PDF, MEDIA_TXT).
pub fn disbind_urn(input_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "disbind")
        .in_spec(input_media)
        .out_spec(MEDIA_TEXTABLE_PAGE_LIST)
        .build()
        .expect("Failed to build disbind cap URN")
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING URN BUILDERS
// -----------------------------------------------------------------------------

/// Build URN for structured-query capability
/// Input uses MEDIA_JSON_SCHEMA per CATALOG: media:json;json-schema;textable;record
pub fn structured_query_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "query_structured")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_JSON_SCHEMA)
        .out_spec(MEDIA_JSON)
        .build()
        .expect("Failed to build structured-query cap URN")
}

/// Build URN for make-decision capability
/// Output uses MEDIA_DECISION per CATALOG: media:decision;bool;textable
pub fn make_decision_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "make_decision")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_DECISION)
        .build()
        .expect("Failed to build make-decision cap URN")
}

/// Build URN for make-multiple-decisions capability
/// Output uses MEDIA_DECISION_ARRAY per CATALOG: media:decision;bool;textable;list
pub fn make_multiple_decisions_urn(lang_code: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "make_multiple_decisions")
        .tag("language", lang_code)
        .solo_tag("constrained")
        .in_spec(MEDIA_STRING)
        .out_spec(MEDIA_DECISION_ARRAY)
        .build()
        .expect("Failed to build make-multiple-decisions cap URN")
}

// -----------------------------------------------------------------------------
// MACINA-SPECIFIC TASK URN BUILDERS
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
        .in_spec(MEDIA_IDENTITY) // Binary document
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
        .in_spec(MEDIA_IDENTITY) // Binary document
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
pub fn media_urn_for_type(type_name: &str) -> &'static str {
    match type_name {
        "string" => MEDIA_STRING,
        "integer" => MEDIA_INTEGER,
        "number" => crate::urn::media_urn::MEDIA_NUMBER,
        "boolean" => MEDIA_BOOLEAN,
        "object" => MEDIA_OBJECT,
        "string-array" => crate::urn::media_urn::MEDIA_STRING_ARRAY,
        "integer-array" => crate::urn::media_urn::MEDIA_INTEGER_ARRAY,
        "number-array" => crate::urn::media_urn::MEDIA_NUMBER_ARRAY,
        "boolean-array" => crate::urn::media_urn::MEDIA_BOOLEAN_ARRAY,
        "object-array" => crate::urn::media_urn::MEDIA_OBJECT_ARRAY,
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

// -----------------------------------------------------------------------------
// FORMAT CONVERSION URN BUILDERS
// -----------------------------------------------------------------------------
// Format conversion is transforming data between JSON, YAML, and CSV formats.
// Each conversion is a cap with a specific input and output media type.

/// Build a format conversion URN for a specific input → output media type pair.
/// All format conversions use op="convert_format".
pub fn format_conversion_urn(in_media: &str, out_media: &str) -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "convert_format")
        .in_spec(in_media)
        .out_spec(out_media)
        .build()
        .expect("Failed to build format conversion cap URN")
}

/// A format conversion path with authoritative title and description.
pub struct FormatConversionPath {
    pub in_media: &'static str,
    pub out_media: &'static str,
    pub title: &'static str,
    pub description: &'static str,
}

/// All valid format conversion paths between JSON, YAML, CSV, and textable lists.
pub fn all_format_conversion_paths() -> Vec<FormatConversionPath> {
    vec![
        // JSON <-> YAML value
        FormatConversionPath { in_media: MEDIA_JSON_VALUE, out_media: MEDIA_YAML_VALUE,
            title: "Convert JSON Value to YAML", description: "Convert a JSON scalar value to YAML format" },
        FormatConversionPath { in_media: MEDIA_YAML_VALUE, out_media: MEDIA_JSON_VALUE,
            title: "Convert YAML Value to JSON", description: "Convert a YAML scalar value to JSON format" },
        // JSON <-> YAML record
        FormatConversionPath { in_media: MEDIA_JSON_RECORD, out_media: MEDIA_YAML_RECORD,
            title: "Convert JSON Object to YAML Mapping", description: "Convert a JSON object to a YAML mapping" },
        FormatConversionPath { in_media: MEDIA_YAML_RECORD, out_media: MEDIA_JSON_RECORD,
            title: "Convert YAML Mapping to JSON Object", description: "Convert a YAML mapping to a JSON object" },
        // JSON <-> YAML list
        FormatConversionPath { in_media: MEDIA_JSON_LIST, out_media: MEDIA_YAML_LIST,
            title: "Convert JSON Array to YAML Sequence", description: "Convert a JSON array to a YAML sequence" },
        FormatConversionPath { in_media: MEDIA_YAML_LIST, out_media: MEDIA_JSON_LIST,
            title: "Convert YAML Sequence to JSON Array", description: "Convert a YAML sequence to a JSON array" },
        // JSON <-> YAML list of records
        FormatConversionPath { in_media: MEDIA_JSON_LIST_RECORD, out_media: MEDIA_YAML_LIST_RECORD,
            title: "Convert JSON Array of Objects to YAML List of Mappings", description: "Convert a JSON array of objects to a YAML list of mappings" },
        FormatConversionPath { in_media: MEDIA_YAML_LIST_RECORD, out_media: MEDIA_JSON_LIST_RECORD,
            title: "Convert YAML List of Mappings to JSON Array of Objects", description: "Convert a YAML list of mappings to a JSON array of objects" },
        // JSON list of records <-> CSV
        FormatConversionPath { in_media: MEDIA_JSON_LIST_RECORD, out_media: MEDIA_CSV,
            title: "Convert JSON Array of Objects to CSV", description: "Convert a JSON array of objects to CSV format" },
        FormatConversionPath { in_media: MEDIA_CSV, out_media: MEDIA_JSON_LIST_RECORD,
            title: "Convert CSV to JSON Array of Objects", description: "Convert CSV data to a JSON array of objects" },
        // YAML list of records <-> CSV
        FormatConversionPath { in_media: MEDIA_YAML_LIST_RECORD, out_media: MEDIA_CSV,
            title: "Convert YAML List of Mappings to CSV", description: "Convert a YAML list of mappings to CSV format" },
        FormatConversionPath { in_media: MEDIA_CSV, out_media: MEDIA_YAML_LIST_RECORD,
            title: "Convert CSV to YAML List of Mappings", description: "Convert CSV data to a YAML list of mappings" },
        // Textable list <-> JSON list
        FormatConversionPath { in_media: MEDIA_TEXTABLE_LIST, out_media: MEDIA_JSON_LIST,
            title: "Convert Text List to JSON Array", description: "Convert a list of textable values to a JSON array" },
        FormatConversionPath { in_media: MEDIA_JSON_LIST, out_media: MEDIA_TEXTABLE_LIST,
            title: "Convert JSON Array to Text List", description: "Convert a JSON array to a list of textable values" },
        // Textable list <-> YAML list
        FormatConversionPath { in_media: MEDIA_TEXTABLE_LIST, out_media: MEDIA_YAML_LIST,
            title: "Convert Text List to YAML Sequence", description: "Convert a list of textable values to a YAML sequence" },
        FormatConversionPath { in_media: MEDIA_YAML_LIST, out_media: MEDIA_TEXTABLE_LIST,
            title: "Convert YAML Sequence to Text List", description: "Convert a YAML sequence to a list of textable values" },
        // Textable list <-> CSV
        FormatConversionPath { in_media: MEDIA_TEXTABLE_LIST, out_media: MEDIA_CSV,
            title: "Convert Text List to CSV", description: "Convert a list of textable values to CSV format" },
        FormatConversionPath { in_media: MEDIA_CSV, out_media: MEDIA_TEXTABLE_LIST,
            title: "Convert CSV to Text List", description: "Convert CSV data to a list of textable values" },
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

/// Get generic text-generation cap from registry.
pub async fn llm_generate_text_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = llm_generate_text_urn();
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

/// Get text embeddings-generation cap from registry
pub async fn embeddings_generation_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = embeddings_generation_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get image embeddings-generation cap from registry
pub async fn image_embeddings_generation_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = image_embeddings_generation_urn();
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

/// Get model availability cap from registry
pub async fn model_availability_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_availability_urn();
    registry.get_cap(&urn.to_string()).await
}

/// Get model path cap from registry
pub async fn model_path_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = model_path_urn();
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// DOCUMENT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get page-image rendering cap from registry.
pub async fn render_page_image_cap(registry: Arc<CapRegistry>, input_media: &str) -> Result<Cap, RegistryError> {
    let urn = render_page_image_urn(input_media);
    registry.get_cap(&urn.to_string()).await
}

/// Get disbind cap from registry
pub async fn disbind_cap(registry: Arc<CapRegistry>, input_media: &str) -> Result<Cap, RegistryError> {
    let urn = disbind_urn(input_media);
    registry.get_cap(&urn.to_string()).await
}

// -----------------------------------------------------------------------------
// TEXT PROCESSING CAPABILITIES
// -----------------------------------------------------------------------------

/// Get structured-query cap from registry
pub async fn structured_query_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = structured_query_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get make-decision cap from registry
pub async fn make_decision_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = make_decision_urn(lang_code);
    registry.get_cap(&urn.to_string()).await
}

/// Get make-multiple-decisions cap from registry
pub async fn make_multiple_decisions_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = make_multiple_decisions_urn(lang_code);
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

// -----------------------------------------------------------------------------
// FORMAT CONVERSION CAPABILITIES
// -----------------------------------------------------------------------------

/// Get a single format conversion cap from the registry
pub async fn format_conversion_cap(
    registry: Arc<CapRegistry>,
    in_media: &str,
    out_media: &str,
) -> Result<Cap, RegistryError> {
    let urn = format_conversion_urn(in_media, out_media);
    registry.get_cap(&urn.to_string()).await
}

/// Get all format conversion caps from the registry
/// Returns a vector of (in_media, out_media, Cap) tuples
/// Fails if any conversion cap is missing from the registry
pub async fn all_format_conversion_caps(
    registry: Arc<CapRegistry>,
) -> Result<Vec<(&'static str, &'static str, Cap)>, RegistryError> {
    let mut caps = Vec::new();
    for path in all_format_conversion_paths() {
        let cap = format_conversion_cap(registry.clone(), path.in_media, path.out_media).await?;
        caps.push((path.in_media, path.out_media, cap));
    }
    Ok(caps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::media_urn::{MEDIA_MODEL_SPEC, MEDIA_AVAILABILITY_OUTPUT, MEDIA_PATH_OUTPUT, MEDIA_LLM_INFERENCE_OUTPUT};
    use crate::standard::media::MEDIA_STRING;

    // TEST307: Test model_availability_urn builds valid cap URN with correct op and media specs
    #[test]
    fn test307_model_availability_urn() {
        let urn = model_availability_urn();
        assert!(urn.has_tag("op", "model-availability"), "URN must have op=model-availability");
        assert_eq!(urn.in_spec(), MEDIA_MODEL_SPEC, "input must be model-spec");
        assert_eq!(urn.out_spec(), MEDIA_AVAILABILITY_OUTPUT, "output must be availability output");
    }

    // TEST308: Test model_path_urn builds valid cap URN with correct op and media specs
    #[test]
    fn test308_model_path_urn() {
        let urn = model_path_urn();
        assert!(urn.has_tag("op", "model-path"), "URN must have op=model-path");
        assert_eq!(urn.in_spec(), MEDIA_MODEL_SPEC, "input must be model-spec");
        assert_eq!(urn.out_spec(), MEDIA_PATH_OUTPUT, "output must be path output");
    }

    // TEST309: Test model_availability_urn and model_path_urn produce distinct URNs
    #[test]
    fn test309_model_availability_and_path_are_distinct() {
        let avail = model_availability_urn();
        let path = model_path_urn();
        assert_ne!(avail.to_string(), path.to_string(),
            "availability and path must be distinct cap URNs");
    }

    #[test]
    fn test310_llm_generate_text_urn_shape() {
        use crate::urn::media_urn::MediaUrn;
        let urn = llm_generate_text_urn();

        let in_spec = MediaUrn::from_string(urn.in_spec()).expect("in_spec must parse");
        let out_spec = MediaUrn::from_string(urn.out_spec()).expect("out_spec must parse");
        let expected = MediaUrn::from_string(MEDIA_STRING).expect("MEDIA_STRING must parse");

        assert!(urn.has_tag("op", "generate_text"), "must have op=generate_text");
        assert!(urn.has_marker_tag("llm"), "must have llm tag");
        assert!(urn.has_marker_tag("ml-model"), "must have ml-model tag");
        assert!(in_spec.conforms_to(&expected).unwrap(),
            "in_spec '{}' must match MEDIA_STRING '{}'", urn.in_spec(), MEDIA_STRING);
        assert!(out_spec.conforms_to(&expected).unwrap(),
            "out_spec '{}' must match MEDIA_STRING '{}'", urn.out_spec(), MEDIA_STRING);
    }

    // TEST312: Test all URN builders produce parseable cap URNs
    #[test]
    fn test312_all_urn_builders_produce_valid_urns() {
        // Each of these must not panic
        let _avail = model_availability_urn();
        let _path = model_path_urn();
        let _conv = llm_generate_text_urn();

        // Verify they roundtrip through CapUrn parsing
        let avail_str = model_availability_urn().to_string();
        let parsed = crate::urn::cap_urn::CapUrn::from_string(&avail_str);
        assert!(parsed.is_ok(), "model_availability_urn must be parseable: {:?}", parsed.err());

        let path_str = model_path_urn().to_string();
        let parsed = crate::urn::cap_urn::CapUrn::from_string(&path_str);
        assert!(parsed.is_ok(), "model_path_urn must be parseable: {:?}", parsed.err());
    }

    // TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void
    #[test]
    fn test473_cap_discard_parses_as_valid_urn() {
        use crate::urn::cap_urn::CapUrn;
        use crate::urn::media_urn::MEDIA_VOID;

        let urn = CapUrn::from_string(CAP_DISCARD).expect("CAP_DISCARD must parse");
        assert_eq!(urn.in_spec(), "media:", "CAP_DISCARD input must be wildcard media:");
        assert_eq!(urn.out_spec(), MEDIA_VOID, "CAP_DISCARD output must be media:void");
    }

    // TEST474: CAP_DISCARD accepts specific-input/void-output caps
    #[test]
    fn test474_cap_discard_accepts_specific_void_cap() {
        use crate::urn::cap_urn::CapUrn;

        let discard = CapUrn::from_string(CAP_DISCARD).expect("CAP_DISCARD must parse");
        let specific = CapUrn::from_string("cap:in=\"media:pdf\";op=shred;out=\"media:void\"")
            .expect("specific cap must parse");

        // discard (pattern) accepts specific (instance)? No — discard has no op tag,
        // but the specific cap has op=shred. As pattern, discard accepts instances
        // that are at least as specific. The specific cap IS more specific.
        // As instance, does the specific cap conform to the discard pattern?
        // specific.conforms_to(discard) == discard.accepts(specific)
        assert!(discard.accepts(&specific),
            "CAP_DISCARD must accept a more specific cap with void output");

        // But a cap with non-void output must NOT conform to discard
        let non_void = CapUrn::from_string("cap:in=\"media:pdf\";op=convert;out=\"media:string\"")
            .expect("non-void cap must parse");
        assert!(!discard.accepts(&non_void),
            "CAP_DISCARD must NOT accept a cap with non-void output");
    }

    // TEST605: all_coercion_paths each entry builds a valid parseable CapUrn
    #[test]
    fn test605_all_coercion_paths_build_valid_urns() {
        let paths = all_coercion_paths();
        assert!(!paths.is_empty(), "Coercion paths must not be empty");

        for (source, target) in &paths {
            let urn = coercion_urn(source, target);
            assert!(urn.has_tag("op", "coerce"),
                "Coercion URN for {}→{} must have op=coerce", source, target);

            // Verify roundtrip through string parsing
            let urn_str = urn.to_string();
            let reparsed = crate::urn::cap_urn::CapUrn::from_string(&urn_str);
            assert!(reparsed.is_ok(),
                "Coercion URN for {}→{} must roundtrip through parsing: {:?}",
                source, target, reparsed.err());
        }
    }

    // TEST606: coercion_urn in/out specs match the type's media URN constant
    #[test]
    fn test606_coercion_urn_specs() {
        use crate::urn::media_urn::MediaUrn;

        let urn = coercion_urn("string", "integer");
        // in_spec should conform to MEDIA_STRING
        let in_urn = MediaUrn::from_string(urn.in_spec()).expect("in_spec should parse");
        let expected_in = MediaUrn::from_string(MEDIA_STRING).expect("MEDIA_STRING should parse");
        assert!(in_urn.conforms_to(&expected_in).unwrap(),
            "in_spec '{}' should conform to '{}'", urn.in_spec(), MEDIA_STRING);

        // out_spec should conform to MEDIA_INTEGER
        let out_urn = MediaUrn::from_string(urn.out_spec()).expect("out_spec should parse");
        let expected_out = MediaUrn::from_string(MEDIA_INTEGER).expect("MEDIA_INTEGER should parse");
        assert!(out_urn.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' should conform to '{}'", urn.out_spec(), MEDIA_INTEGER);
    }

    // TEST850: all_format_conversion_paths each entry builds a valid parseable CapUrn
    #[test]
    fn test850_all_format_conversion_paths_build_valid_urns() {
        let paths = all_format_conversion_paths();
        assert_eq!(paths.len(), 18, "Expected 18 format conversion paths");

        for path in &paths {
            let urn = format_conversion_urn(path.in_media, path.out_media);
            assert!(urn.has_tag("op", "convert_format"),
                "Format conversion URN for {}→{} must have op=convert_format", path.in_media, path.out_media);

            // Verify roundtrip through string parsing
            let urn_str = urn.to_string();
            let reparsed = crate::urn::cap_urn::CapUrn::from_string(&urn_str);
            assert!(reparsed.is_ok(),
                "Format conversion URN for {}→{} must roundtrip through parsing: {:?}",
                path.in_media, path.out_media, reparsed.err());
        }
    }

    // TEST851: format_conversion_urn in/out specs match the input constants
    #[test]
    fn test851_format_conversion_urn_specs() {
        use crate::urn::media_urn::MediaUrn;

        let urn = format_conversion_urn(MEDIA_JSON_VALUE, MEDIA_YAML_VALUE);
        let in_urn = MediaUrn::from_string(urn.in_spec()).expect("in_spec should parse");
        let expected_in = MediaUrn::from_string(MEDIA_JSON_VALUE).expect("MEDIA_JSON_VALUE should parse");
        assert!(in_urn.conforms_to(&expected_in).unwrap(),
            "in_spec '{}' should conform to '{}'", urn.in_spec(), MEDIA_JSON_VALUE);

        let out_urn = MediaUrn::from_string(urn.out_spec()).expect("out_spec should parse");
        let expected_out = MediaUrn::from_string(MEDIA_YAML_VALUE).expect("MEDIA_YAML_VALUE should parse");
        assert!(out_urn.conforms_to(&expected_out).unwrap(),
            "out_spec '{}' should conform to '{}'", urn.out_spec(), MEDIA_YAML_VALUE);
    }
}
