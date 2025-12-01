//! Standard capability definitions with arguments
//!
//! This module provides the standard capability definitions used across
//! all FMIO providers, including their formal argument specifications.
//! These definitions should match the TOML definitions in capns_dot_org/standard/

use crate::{
    ArgumentType, ArgumentValidation, Cap, CapArgument, CapArguments, CapOutput, CapUrn, CapUrnBuilder, OutputType, CapRegistry, RegistryError
};
use std::{collections::HashMap, sync::Arc};

// =============================================================================
// LLM CAPABILITIES
// =============================================================================

/// Create the standard conversation cap with full argument definition
pub fn llm_conversation_cap() -> Cap {
    let id = CapUrn::from_string("cap:action=conversation;type=unconstrained")
        .expect("Invalid cap URN");

    let command = "conversation".to_string();
    let mut arguments = CapArguments::new();

    // Optional language argument
    let language_arg = CapArgument {
        name: "language".to_string(),
        arg_type: ArgumentType::String,
        arg_description: "Target language for conversation".to_string(),
        cli_flag: "--language".to_string(),
        position: None,
        validation: ArgumentValidation {
            allowed_values: Some(vec![
                "en".to_string(), "es".to_string(), "fr".to_string(), 
                "de".to_string(), "zh".to_string(), "ja".to_string(),
                "ko".to_string(), "it".to_string(), "pt".to_string(),
                "ru".to_string(), "ar".to_string()
            ]),
            ..Default::default()
        },
        default_value: Some(serde_json::Value::String("en".to_string())),
    };
    arguments.add_optional(language_arg);

    let output = CapOutput {
        output_type: OutputType::String,
        schema_ref: None,
        content_type: Some("text/plain".to_string()),
        validation: ArgumentValidation::default(),
        output_description: "Generated conversational response".to_string(),
    };

    Cap::with_full_definition(
        id,
        "1.0.0".to_string(),
        Some("Interactive conversational text generation".to_string()),
        HashMap::new(),
        command,
        arguments,
        Some(output),
    )
}

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

/// Create the standard analysis cap with full argument definition
pub fn llm_analysis_cap() -> Cap {
    let id = CapUrn::from_string("cap:action=analysis;type=constrained")
        .expect("Invalid cap URN");

    let command = "analysis".to_string();
    let mut arguments = CapArguments::new();

    // Required text argument
    let text_arg = CapArgument {
        name: "text".to_string(),
        arg_type: ArgumentType::String,
        arg_description: "Text to analyze".to_string(),
        cli_flag: "--text".to_string(),
        position: Some(0),
        validation: ArgumentValidation {
            min_length: Some(1),
            ..Default::default()
        },
        default_value: None,
    };
    arguments.add_required(text_arg);

    // Optional language argument
    let language_arg = CapArgument {
        name: "language".to_string(),
        arg_type: ArgumentType::String,
        arg_description: "Target language for analysis".to_string(),
        cli_flag: "--language".to_string(),
        position: None,
        validation: ArgumentValidation {
            allowed_values: Some(vec![
                "en".to_string(), "es".to_string(), "fr".to_string(), 
                "de".to_string(), "zh".to_string(), "ja".to_string(),
                "ko".to_string(), "it".to_string(), "pt".to_string(),
                "ru".to_string(), "ar".to_string()
            ]),
            ..Default::default()
        },
        default_value: Some(serde_json::Value::String("en".to_string())),
    };
    arguments.add_optional(language_arg);

    let output = CapOutput {
        output_type: OutputType::Object,
        schema_ref: None,
        content_type: Some("application/json".to_string()),
        validation: ArgumentValidation::default(),
        output_description: "Structured analysis of the input text".to_string(),
    };

    Cap::with_full_definition(
        id,
        "1.0.0".to_string(),
        Some("Analyze text content and extract insights".to_string()),
        HashMap::new(),
        command,
        arguments,
        Some(output),
    )
}

/// Convenience function to get analysis cap from registry with language
pub async fn llm_analysis(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "analysis")
        .tag("type", "constrained")
        .tag("language", lang_code)
        .build()
        .map_err(|e| RegistryError::ValidationError(format!("Failed to build cap URN: {}", e)))?;
    
    registry.get_cap(&urn.to_string()).await
}

/// Convenience function to get bitlogic cap from registry with language
pub async fn llm_bitlogic(registry: Arc<CapRegistry>, lang_code: &str) -> Result<Cap, RegistryError> {
    let urn = CapUrnBuilder::new()
        .tag("action", "bitlogic")
        .tag("type", "constrained")
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

/// Get the standard frontmatter summarization cap from registry
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



