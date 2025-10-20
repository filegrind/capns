//! Formal capability definition
//!
//! This module defines the structure for formal capability definitions that include
//! the capability identifier, versioning, and metadata. Capabilities are general-purpose
//! and do not assume any specific domain like files or documents.

use crate::capability_id::CapabilityId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Formal capability definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    /// Formal capability identifier with hierarchical naming
    pub id: CapabilityId,
    
    /// Capability version
    pub version: String,
    
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    
    /// Optional metadata as key-value pairs
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
}

impl Capability {
    /// Create a new capability
    pub fn new(id: CapabilityId, version: String) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new capability with description
    pub fn with_description(id: CapabilityId, version: String, description: String) -> Self {
        Self {
            id,
            version,
            description: Some(description),
            metadata: HashMap::new(),
        }
    }

    /// Create a new capability with metadata
    pub fn with_metadata(
        id: CapabilityId, 
        version: String, 
        metadata: HashMap<String, String>
    ) -> Self {
        Self {
            id,
            version,
            description: None,
            metadata,
        }
    }

    /// Create a new capability with description and metadata
    pub fn with_description_and_metadata(
        id: CapabilityId,
        version: String,
        description: String,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            version,
            description: Some(description),
            metadata,
        }
    }
    
    /// Check if this capability matches a request string
    pub fn matches_request(&self, request: &str) -> bool {
        let request_id = CapabilityId::from_string(request).expect("Invalid capability identifier in request");
        self.id.can_handle(&request_id)
    }

    /// Get the capability identifier as a string
    pub fn id_string(&self) -> String {
        self.id.to_string()
    }

    /// Check if this capability is more specific than another for the same request
    pub fn is_more_specific_than(&self, other: &Capability, request: &str) -> bool {
        if !self.matches_request(request) || !other.matches_request(request) {
            return false;
        }
        self.id.is_more_specific_than(&other.id)
    }

    /// Get a metadata value by key
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Set a metadata value
    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Remove a metadata value
    pub fn remove_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }

    /// Check if this capability has specific metadata
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }
}

/// Plugin capabilities collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginCapabilities {
    pub capabilities: Vec<Capability>,
}

impl PluginCapabilities {
    /// Create a new empty capabilities collection
    pub fn new() -> Self {
        Self {
            capabilities: Vec::new(),
        }
    }

    /// Create capabilities collection from a list of capabilities
    pub fn from_capabilities(capabilities: Vec<Capability>) -> Self {
        Self { capabilities }
    }

    /// Add a capability to the collection
    pub fn add_capability(&mut self, capability: Capability) {
        self.capabilities.push(capability);
    }

    /// Check if the plugin has a specific capability
    pub fn can(&self, capability_request: &str) -> bool {
        self.capabilities.iter().any(|c| c.matches_request(capability_request))
    }
    
    /// Get all capability identifiers as strings
    pub fn get_capability_identifiers(&self) -> Vec<String> {
        self.capabilities.iter().map(|c| c.id.to_string()).collect()
    }
    
    /// Find a capability by identifier
    pub fn find_capability(&self, id: &str) -> Option<&Capability> {
        let search_id = CapabilityId::from_string(id).ok()?;
        self.capabilities.iter().find(|c| c.id == search_id)
    }
    
    /// Find the most specific capability that can handle a request
    pub fn find_best_capability(&self, request: &str) -> Option<&Capability> {
        let request_id = CapabilityId::from_string(request).ok()?;
        let capability_ids: Vec<CapabilityId> = self.capabilities.iter().map(|c| c.id.clone()).collect();
        let best_id = crate::capability_id::CapabilityMatcher::find_best_match(&capability_ids, &request_id)?;
        self.capabilities.iter().find(|c| &c.id == best_id)
    }

    /// Get capabilities that have specific metadata
    pub fn capabilities_with_metadata(&self, key: &str, value: Option<&str>) -> Vec<&Capability> {
        self.capabilities
            .iter()
            .filter(|c| {
                if let Some(expected_value) = value {
                    c.get_metadata(key) == Some(&expected_value.to_string())
                } else {
                    c.has_metadata(key)
                }
            })
            .collect()
    }

    /// Get all unique metadata keys across all capabilities
    pub fn get_all_metadata_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for capability in &self.capabilities {
            for key in capability.metadata.keys() {
                if !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
        }
        keys.sort();
        keys
    }

    /// Get capabilities by version
    pub fn capabilities_by_version(&self, version: &str) -> Vec<&Capability> {
        self.capabilities
            .iter()
            .filter(|c| c.version == version)
            .collect()
    }
}

impl Default for PluginCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_creation() {
        let id = CapabilityId::from_string("data_processing:transform:json").unwrap();
        let cap = Capability::new(id, "1.0.0".to_string());
        
        assert_eq!(cap.id_string(), "data_processing:transform:json");
        assert_eq!(cap.version, "1.0.0");
        assert!(cap.metadata.is_empty());
    }

    #[test]
    fn test_capability_with_metadata() {
        let id = CapabilityId::from_string("compute:math:arithmetic").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("precision".to_string(), "double".to_string());
        metadata.insert("operations".to_string(), "add,subtract,multiply,divide".to_string());
        
        let cap = Capability::with_metadata(id, "2.1.0".to_string(), metadata);
        
        assert_eq!(cap.get_metadata("precision"), Some(&"double".to_string()));
        assert_eq!(cap.get_metadata("operations"), Some(&"add,subtract,multiply,divide".to_string()));
        assert!(cap.has_metadata("precision"));
        assert!(!cap.has_metadata("nonexistent"));
    }

    #[test]
    fn test_capability_matching() {
        let id = CapabilityId::from_string("data_processing:transform:json").unwrap();
        let cap = Capability::new(id, "1.0.0".to_string());
        
        assert!(cap.matches_request("data_processing:transform:json"));
        assert!(cap.matches_request("data_processing:transform:*"));
        assert!(cap.matches_request("data_processing:*"));
        assert!(!cap.matches_request("compute:*"));
    }

    #[test]
    fn test_plugin_capabilities() {
        let mut capabilities = PluginCapabilities::new();
        
        let id1 = CapabilityId::from_string("data_processing:transform:json").unwrap();
        let cap1 = Capability::new(id1, "1.0.0".to_string());
        
        let id2 = CapabilityId::from_string("data_processing:validate:*").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("formats".to_string(), "json,xml,yaml".to_string());
        let cap2 = Capability::with_metadata(id2, "1.0.0".to_string(), metadata);
        
        capabilities.add_capability(cap1);
        capabilities.add_capability(cap2);
        
        assert!(capabilities.can("data_processing:transform:json"));
        assert!(capabilities.can("data_processing:validate:xml"));
        assert!(!capabilities.can("compute:math"));
        
        let metadata_caps = capabilities.capabilities_with_metadata("formats", None);
        assert_eq!(metadata_caps.len(), 1);
        
        let version_caps = capabilities.capabilities_by_version("1.0.0");
        assert_eq!(version_caps.len(), 2);
    }
}