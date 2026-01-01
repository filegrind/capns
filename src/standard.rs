//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all FGRND providers, including their formal argument specifications.
//! These definitions should match the TOML definitions in capns_dot_org/standard/

use crate::{
    Cap, CapUrnBuilder, CapRegistry, RegistryError
};
use std::sync::Arc;

// =============================================================================
// LLM CAPABILITIES
// =============================================================================

/// Convenience function to get conversation cap from registry with language
pub async fn llm_conversation(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "conversation")
        .tag("type", "unconstrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get multiplechoice cap from registry with language
pub async fn llm_multiplechoice(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "multiplechoice")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get codegeneration cap from registry with language
pub async fn llm_codegeneration(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "codegeneration")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get creative cap from registry with language
pub async fn llm_creative(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "creative")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get translation cap from registry with language
pub async fn llm_translation(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "translation")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get summarization cap from registry with language
pub async fn llm_summarization(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "summarization")
        .tag("type", "unconstrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}


// =============================================================================
// EMBEDDING CAPABILITIES
// =============================================================================

/// Get the standard embeddings-dimensions cap from registry
pub async fn embeddings_dimensions_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "dimensions")
        .tag("target", "embeddings")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the standard embeddings-generation cap from registry
pub async fn embeddings_generation_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "generate")
        .tag("target", "embeddings")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

// =============================================================================
// MODEL MANAGEMENT CAPABILITIES
// =============================================================================

/// Get the model download capability from registry
pub async fn model_download_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "download")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the model load capability from registry
pub async fn model_load_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "load")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the model unload capability from registry
pub async fn model_unload_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "unload")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the model list capability from registry
pub async fn model_list_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "list")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the model status capability from registry
pub async fn model_status_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "status")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the model contents capability from registry
pub async fn model_contents_cap(registry: Arc<CapRegistry>) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "contents")
        .tag("type", "model")
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the standard extract-metadata cap from registry
pub async fn extract_metadata_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "extract")
        .tag("target", "metadata")
		.tag("ext", ext)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the standard generate-thumbnail cap from registry
pub async fn generate_thumbnail_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "generate")
        .tag("output", "binary")
        .tag("target", "thumbnail")
		.tag("ext", ext)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the standard extract-outline cap from registry
pub async fn extract_outline_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "extract")
        .tag("target", "outline")
        .tag("ext", ext)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Get the standard extract-pages cap from registry
pub async fn extract_pages_cap(registry: Arc<CapRegistry>, ext: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "extract")
        .tag("target", "pages")
        .tag("ext", ext)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

pub async fn frontmatter_summarization_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
	let urn = CapUrnBuilder::new()
		.tag("action", "generate")
		.tag("target", "summary")
		.tag("input", "frontmatter")
		.tag("output", "text")
		.tag("language", lang_code)
		.tag("type", "unconstrained")
		.build()
		.map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
	registry.get_cap(&urn.to_string()).await
}

pub async fn structured_query_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
	let urn = CapUrnBuilder::new()
		.tag("action", "query")
		.tag("target", "structured")
		.tag("output", "json")
		.tag("language", lang_code)
		.tag("type", "constrained")
		.build()
		.map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
	registry.get_cap(&urn.to_string()).await
}

pub async fn bit_choice_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
	let urn = CapUrnBuilder::new()
		.tag("action", "choose")
		.tag("target", "bit")
		.tag("output", "boolean")
		.tag("language", lang_code)
		.tag("type", "constrained")
		.build()
		.map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
	registry.get_cap(&urn.to_string()).await
}

pub async fn bit_choices_cap(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
	let urn = CapUrnBuilder::new()
		.tag("action", "choose")
		.tag("target", "bits")
		.tag("output", "booleans")
		.tag("language", lang_code)
		.tag("type", "constrained")
		.build()
		.map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
	registry.get_cap(&urn.to_string()).await
}
