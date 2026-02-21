//! Unified cap-based manifest interface
//! 
//! This module defines the unified manifest interface with standardized cap-based declarations.
//! This replaces the separate ProviderManifest and PluginManifest types with a single canonical format.

use crate::Cap;
use crate::urn::cap_urn::CapUrn;
use crate::standard::caps::CAP_IDENTITY;
use serde::{Deserialize, Serialize};

/// Unified cap manifest for --manifest output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapManifest {
    /// Component name
    pub name: String,

    /// Component version
    pub version: String,

    /// Component description
    pub description: String,

    /// Component caps with formal definitions
    pub caps: Vec<Cap>,

    /// Component author/maintainer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Human-readable page URL for the plugin (e.g., repository page, documentation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_url: Option<String>,
}

impl CapManifest {
    /// Create a new cap manifest
    pub fn new(
        name: String,
        version: String,
        description: String,
        caps: Vec<Cap>,
    ) -> Self {
        Self {
            name,
            version,
            description,
            caps,
            author: None,
            page_url: None,
        }
    }

    /// Set the author of the component
    pub fn with_author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    /// Set the page URL for the plugin (human-readable page, e.g., repository)
    pub fn with_page_url(mut self, page_url: String) -> Self {
        self.page_url = Some(page_url);
        self
    }

    /// Validate that CAP_IDENTITY is declared in this manifest.
    /// Fails hard if missing — identity is mandatory in every capset.
    pub fn validate(&self) -> Result<(), String> {
        let identity_urn = CapUrn::from_string(CAP_IDENTITY)
            .map_err(|e| format!("BUG: CAP_IDENTITY constant is invalid: {}", e))?;
        let has_identity = self.caps.iter().any(|cap| identity_urn.conforms_to(&cap.urn));
        if !has_identity {
            return Err(format!("Manifest missing required CAP_IDENTITY ({})", CAP_IDENTITY));
        }
        Ok(())
    }
}

/// Trait for components to provide metadata about themselves
pub trait ComponentMetadata {
    /// Get component manifest
    fn component_manifest(&self) -> CapManifest;
    
    /// Get component caps
    fn caps(&self) -> Vec<Cap> {
        self.component_manifest().caps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapUrn, Cap};
    use std::collections::HashMap;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:form=map";{}"#, tags)
    }

    // TEST148: Test creating cap manifest with name, version, description, and caps
    #[test]
    fn test148_cap_manifest_creation() {
        let urn = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
        let cap = Cap::new(urn, "Extract Metadata".to_string(), "extract-metadata".to_string());

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![cap],
        );

        assert_eq!(manifest.name, "TestComponent");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(manifest.description, "A test component for validation");
        assert_eq!(manifest.caps.len(), 1);
        assert!(manifest.author.is_none());
    }

    // TEST149: Test cap manifest with author field sets author correctly
    #[test]
    fn test149_cap_manifest_with_author() {
        let urn = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
        let cap = Cap::new(urn, "Extract Metadata".to_string(), "extract-metadata".to_string());

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![cap],
        ).with_author("Test Author".to_string());

        assert_eq!(manifest.author, Some("Test Author".to_string()));
    }

    // TEST150: Test cap manifest JSON serialization and deserialization roundtrip
    #[test]
    fn test150_cap_manifest_json_serialization() {
        use crate::{CapArg, ArgSource};

        let urn = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
        let mut cap = Cap::new(urn, "Extract Metadata".to_string(), "extract-metadata".to_string());

        // Add stdin via args architecture
        let stdin_arg = CapArg::new(
            "media:pdf;bytes",
            true,
            vec![ArgSource::Stdin { stdin: "media:pdf;bytes".to_string() }],
        );
        cap.add_arg(stdin_arg);

        let manifest = CapManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![cap],
        ).with_author("Test Author".to_string());

        // Test serialization
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"name\":\"TestComponent\""));
        assert!(json.contains("\"version\":\"0.1.0\""));
        assert!(json.contains("\"author\":\"Test Author\""));
        assert!(json.contains("\"stdin\":\"media:pdf;bytes\""));

        // Test deserialization
        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, manifest.name);
        assert_eq!(deserialized.version, manifest.version);
        assert_eq!(deserialized.description, manifest.description);
        assert_eq!(deserialized.author, manifest.author);
        assert_eq!(deserialized.caps.len(), manifest.caps.len());
        assert_eq!(deserialized.caps[0].get_stdin_media_urn(), manifest.caps[0].get_stdin_media_urn());
    }

    // TEST151: Test cap manifest deserialization fails when required fields are missing
    #[test]
    fn test151_cap_manifest_required_fields() {
        // Test that deserialization fails when required fields are missing
        let invalid_json = r#"{"name": "TestComponent"}"#;
        let result: Result<CapManifest, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());

        let invalid_json2 = r#"{"name": "TestComponent", "version": "1.0.0"}"#;
        let result2: Result<CapManifest, _> = serde_json::from_str(invalid_json2);
        assert!(result2.is_err());
    }

    // TEST152: Test cap manifest with multiple caps stores and retrieves all capabilities
    #[test]
    fn test152_cap_manifest_with_multiple_caps() {
        let id1 = CapUrn::from_string(&test_urn("op=extract;target=metadata")).unwrap();
        let cap1 = Cap::new(id1, "Extract Metadata".to_string(), "extract-metadata".to_string());

        let id2 = CapUrn::from_string(&test_urn("op=extract;target=outline")).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("supports_outline".to_string(), "true".to_string());
        let cap2 = Cap::with_metadata(id2, "Extract Outline".to_string(), "extract-outline".to_string(), metadata);

        let manifest = CapManifest::new(
            "MultiCapComponent".to_string(),
            "1.0.0".to_string(),
            "Component with multiple caps".to_string(),
            vec![cap1, cap2],
        );

        assert_eq!(manifest.caps.len(), 2);
        // urn_string now includes in/out
        assert!(manifest.caps[0].urn_string().contains("op=extract"));
        assert!(manifest.caps[0].urn_string().contains("target=metadata"));
        assert!(manifest.caps[1].urn_string().contains("op=extract"));
        assert!(manifest.caps[1].urn_string().contains("target=outline"));
        assert!(manifest.caps[1].has_metadata("supports_outline"));
    }

    // TEST153: Test cap manifest with empty caps list serializes and deserializes correctly
    #[test]
    fn test153_cap_manifest_empty_caps() {
        let manifest = CapManifest::new(
            "EmptyComponent".to_string(),
            "1.0.0".to_string(),
            "Component with no caps".to_string(),
            vec![],
        );

        assert_eq!(manifest.caps.len(), 0);

        // Should still serialize/deserialize correctly
        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.caps.len(), 0);
    }

    // TEST154: Test cap manifest optional author field skipped in serialization when None
    #[test]
    fn test154_cap_manifest_optional_author_field() {
        let urn = CapUrn::from_string(&test_urn("op=validate;file")).unwrap();
        let cap = Cap::new(urn, "Validate".to_string(), "validate".to_string());

        let manifest_without_author = CapManifest::new(
            "ValidatorComponent".to_string(),
            "1.0.0".to_string(),
            "File validation component".to_string(),
            vec![cap],
        );

        // Serialize manifest without author
        let json = serde_json::to_string(&manifest_without_author).unwrap();
        assert!(!json.contains("\"author\""));

        // Should deserialize correctly
        let deserialized: CapManifest = serde_json::from_str(&json).unwrap();
        assert!(deserialized.author.is_none());
    }

    // TEST155: Test ComponentMetadata trait provides manifest and caps accessor methods
    #[test]
    fn test155_component_metadata_trait() {
        struct TestComponent {
            name: String,
            caps: Vec<Cap>,
        }

        impl ComponentMetadata for TestComponent {
            fn component_manifest(&self) -> CapManifest {
                CapManifest::new(
                    self.name.clone(),
                    "1.0.0".to_string(),
                    "Test component implementation".to_string(),
                    self.caps.clone(),
                )
            }
        }

        // Use type=component key-value instead of flag
        let urn = CapUrn::from_string(&test_urn("op=test;type=component")).unwrap();
        let cap = Cap::new(urn, "Test Component".to_string(), "test".to_string());

        let component = TestComponent {
            name: "TestImpl".to_string(),
            caps: vec![cap],
        };

        let manifest = component.component_manifest();
        assert_eq!(manifest.name, "TestImpl");

        let caps = component.caps();
        assert_eq!(caps.len(), 1);
        assert!(caps[0].urn_string().contains("op=test"));
        assert!(caps[0].urn_string().contains("type=component"));
    }

    // TEST475: CapManifest::validate() passes when CAP_IDENTITY is present
    #[test]
    fn test475_validate_passes_with_identity() {
        let identity_urn = CapUrn::from_string(CAP_IDENTITY).unwrap();
        let cap = Cap::new(identity_urn, "Identity".to_string(), "identity".to_string());
        let manifest = CapManifest::new(
            "TestPlugin".to_string(),
            "1.0.0".to_string(),
            "Test".to_string(),
            vec![cap],
        );
        assert!(manifest.validate().is_ok(), "Manifest with CAP_IDENTITY must validate");
    }

    // TEST476: CapManifest::validate() fails when CAP_IDENTITY is missing
    #[test]
    fn test476_validate_fails_without_identity() {
        let specific_urn = CapUrn::from_string(&test_urn("op=convert")).unwrap();
        let cap = Cap::new(specific_urn, "Convert".to_string(), "convert".to_string());
        let manifest = CapManifest::new(
            "TestPlugin".to_string(),
            "1.0.0".to_string(),
            "Test".to_string(),
            vec![cap],
        );
        let result = manifest.validate();
        assert!(result.is_err(), "Manifest without CAP_IDENTITY must fail validation");
        assert!(result.unwrap_err().contains("CAP_IDENTITY"),
            "Error message must mention CAP_IDENTITY");
    }
}