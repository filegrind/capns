//! Unified capability-based manifest interface
//! 
//! This module defines the unified manifest interface with standardized capability-based declarations.
//! This replaces the separate ProviderManifest and PluginManifest types with a single canonical format.

use crate::Capability;
use serde::{Deserialize, Serialize};

/// Unified capability manifest for --manifest output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityManifest {
    /// Component name
    pub name: String,
    
    /// Component version
    pub version: String,
    
    /// Component description
    pub description: String,
    
    /// Component capabilities with formal definitions
    pub capabilities: Vec<Capability>,
    
    /// Component author/maintainer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

impl CapabilityManifest {
    /// Create a new capability manifest
    pub fn new(
        name: String,
        version: String,
        description: String,
        capabilities: Vec<Capability>,
    ) -> Self {
        Self {
            name,
            version,
            description,
            capabilities,
            author: None,
        }
    }
    
    /// Set the author of the component
    pub fn with_author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }
}

/// Trait for components to provide metadata about themselves
pub trait ComponentMetadata {
    /// Get component manifest
    fn component_manifest(&self) -> CapabilityManifest;
    
    /// Get component capabilities
    fn capabilities(&self) -> Vec<Capability> {
        self.component_manifest().capabilities
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityKey, Capability};
    use std::collections::HashMap;

    #[test]
    fn test_capability_manifest_creation() {
        let id = CapabilityKey::from_string("action=extract;target=metadata;type=document").unwrap();
        let capability = Capability::new(id, "1.0.0".to_string(), "extract-metadata".to_string());
        
        let manifest = CapabilityManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![capability],
        );
        
        assert_eq!(manifest.name, "TestComponent");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(manifest.description, "A test component for validation");
        assert_eq!(manifest.capabilities.len(), 1);
        assert!(manifest.author.is_none());
    }

    #[test]
    fn test_capability_manifest_with_author() {
        let id = CapabilityKey::from_string("action=extract;target=metadata;type=document").unwrap();
        let capability = Capability::new(id, "1.0.0".to_string(), "extract-metadata".to_string());
        
        let manifest = CapabilityManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![capability],
        ).with_author("Test Author".to_string());
        
        assert_eq!(manifest.author, Some("Test Author".to_string()));
    }

    #[test]
    fn test_capability_manifest_json_serialization() {
        let id = CapabilityKey::from_string("action=extract;target=metadata;type=document").unwrap();
        let mut capability = Capability::new(id, "1.0.0".to_string(), "extract-metadata".to_string());
        capability.accepts_stdin = true;
        
        let manifest = CapabilityManifest::new(
            "TestComponent".to_string(),
            "0.1.0".to_string(),
            "A test component for validation".to_string(),
            vec![capability],
        ).with_author("Test Author".to_string());
        
        // Test serialization
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"name\":\"TestComponent\""));
        assert!(json.contains("\"version\":\"0.1.0\""));
        assert!(json.contains("\"author\":\"Test Author\""));
        assert!(json.contains("\"accepts_stdin\":true"));
        
        // Test deserialization
        let deserialized: CapabilityManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, manifest.name);
        assert_eq!(deserialized.version, manifest.version);
        assert_eq!(deserialized.description, manifest.description);
        assert_eq!(deserialized.author, manifest.author);
        assert_eq!(deserialized.capabilities.len(), manifest.capabilities.len());
        assert_eq!(deserialized.capabilities[0].accepts_stdin, manifest.capabilities[0].accepts_stdin);
    }

    #[test]
    fn test_capability_manifest_required_fields() {
        // Test that deserialization fails when required fields are missing
        let invalid_json = r#"{"name": "TestComponent"}"#;
        let result: Result<CapabilityManifest, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
        
        let invalid_json2 = r#"{"name": "TestComponent", "version": "1.0.0"}"#;
        let result2: Result<CapabilityManifest, _> = serde_json::from_str(invalid_json2);
        assert!(result2.is_err());
    }

    #[test]
    fn test_capability_manifest_with_multiple_capabilities() {
        let id1 = CapabilityKey::from_string("action=extract;target=metadata;type=document").unwrap();
        let capability1 = Capability::new(id1, "1.0.0".to_string(), "extract-metadata".to_string());
        
        let id2 = CapabilityKey::from_string("action=extract;target=outline;type=document").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("supports_toc".to_string(), "true".to_string());
        let capability2 = Capability::with_metadata(id2, "1.0.0".to_string(), "extract-outline".to_string(), metadata);
        
        let manifest = CapabilityManifest::new(
            "MultiCapComponent".to_string(),
            "1.0.0".to_string(),
            "Component with multiple capabilities".to_string(),
            vec![capability1, capability2],
        );
        
        assert_eq!(manifest.capabilities.len(), 2);
        assert_eq!(manifest.capabilities[0].id_string(), "action=extract;target=metadata;type=document");
        assert_eq!(manifest.capabilities[1].id_string(), "action=extract;target=outline;type=document");
        assert!(manifest.capabilities[1].has_metadata("supports_toc"));
    }

    #[test]
    fn test_capability_manifest_empty_capabilities() {
        let manifest = CapabilityManifest::new(
            "EmptyComponent".to_string(),
            "1.0.0".to_string(),
            "Component with no capabilities".to_string(),
            vec![],
        );
        
        assert_eq!(manifest.capabilities.len(), 0);
        
        // Should still serialize/deserialize correctly
        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: CapabilityManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.capabilities.len(), 0);
    }

    #[test]
    fn test_capability_manifest_optional_author_field() {
        let id = CapabilityKey::from_string("action=validate;type=file").unwrap();
        let capability = Capability::new(id, "1.0.0".to_string(), "validate".to_string());
        
        let manifest_without_author = CapabilityManifest::new(
            "ValidatorComponent".to_string(),
            "1.0.0".to_string(),
            "File validation component".to_string(),
            vec![capability],
        );
        
        // Serialize manifest without author
        let json = serde_json::to_string(&manifest_without_author).unwrap();
        assert!(!json.contains("\"author\""));
        
        // Should deserialize correctly
        let deserialized: CapabilityManifest = serde_json::from_str(&json).unwrap();
        assert!(deserialized.author.is_none());
    }

    #[test]
    fn test_component_metadata_trait() {
        struct TestComponent {
            name: String,
            capabilities: Vec<Capability>,
        }
        
        impl ComponentMetadata for TestComponent {
            fn component_manifest(&self) -> CapabilityManifest {
                CapabilityManifest::new(
                    self.name.clone(),
                    "1.0.0".to_string(),
                    "Test component implementation".to_string(),
                    self.capabilities.clone(),
                )
            }
        }
        
        let id = CapabilityKey::from_string("action=test;type=component").unwrap();
        let capability = Capability::new(id, "1.0.0".to_string(), "test".to_string());
        
        let component = TestComponent {
            name: "TestImpl".to_string(),
            capabilities: vec![capability],
        };
        
        let manifest = component.component_manifest();
        assert_eq!(manifest.name, "TestImpl");
        
        let capabilities = component.capabilities();
        assert_eq!(capabilities.len(), 1);
        assert_eq!(capabilities[0].id_string(), "action=test;type=component");
    }
}