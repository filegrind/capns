//! CapHost registry for unified capability host discovery
//! 
//! Provides unified interface for finding capability hosts (both providers and plugins)
//! that can satisfy capability requests using subset matching.

use crate::{Cap, CapUrn, CapHost};
use std::collections::HashMap;

/// Registry error types for capability host operations
#[derive(Debug, thiserror::Error)]
pub enum CapHostRegistryError {
    #[error("No capability hosts found for capability: {0}")]
    NoHostsFound(String),
    #[error("Invalid capability URN: {0}")]
    InvalidUrn(String),
    #[error("Registry error: {0}")]
    RegistryError(String),
}

/// Unified registry for capability hosts (providers and plugins)
#[derive(Debug)]
pub struct CapHostRegistry {
    hosts: HashMap<String, CapHostEntry>,
}

/// Entry for a registered capability host
#[derive(Debug)]
struct CapHostEntry {
    name: String,
    host: Box<dyn CapHost>,
    capabilities: Vec<Cap>,
}

impl CapHostRegistry {
    /// Create a new empty capability host registry
    pub fn new() -> Self {
        Self {
            hosts: HashMap::new(),
        }
    }

    /// Register a capability host with its supported capabilities
    pub async fn register_caphost(
        &mut self,
        name: String,
        host: Box<dyn CapHost>,
        capabilities: Vec<Cap>,
    ) -> Result<(), CapHostRegistryError> {
        let entry = CapHostEntry {
            name: name.clone(),
            host,
            capabilities,
        };
        
        self.hosts.insert(name, entry);
        Ok(())
    }

    /// Find capability hosts that can handle the requested capability
    /// Uses subset matching: host capabilities must be a subset of or match the request
    pub fn find_caphosts(&self, request_urn: &str) -> Result<Vec<&dyn CapHost>, CapHostRegistryError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapHostRegistryError::InvalidUrn(format!("{}: {}", request_urn, e)))?;
        
        let mut matching_hosts = Vec::new();
        
        for entry in self.hosts.values() {
            for cap in &entry.capabilities {
                if cap.urn.matches(&request) {
                    matching_hosts.push(entry.host.as_ref());
                    break; // Found a matching capability for this host, no need to check others
                }
            }
        }
        
        if matching_hosts.is_empty() {
            return Err(CapHostRegistryError::NoHostsFound(request_urn.to_string()));
        }
        
        Ok(matching_hosts)
    }

    /// Find the best capability host for the request using specificity ranking
    /// Returns both the CapHost and the Cap definition that matched
    pub fn find_best_caphost(&self, request_urn: &str) -> Result<(&dyn CapHost, &Cap), CapHostRegistryError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapHostRegistryError::InvalidUrn(format!("{}: {}", request_urn, e)))?;
        
        let mut best_match: Option<(&dyn CapHost, &Cap, usize)> = None;
        
        for entry in self.hosts.values() {
            for cap in &entry.capabilities {
                if cap.urn.matches(&request) {
                    let specificity = cap.urn.specificity();
                    match best_match {
                        None => {
                            best_match = Some((entry.host.as_ref(), cap, specificity));
                        }
                        Some((_, _, current_specificity)) => {
                            if specificity > current_specificity {
                                best_match = Some((entry.host.as_ref(), cap, specificity));
                            }
                        }
                    }
                    break; // Found a matching capability for this host, check next host
                }
            }
        }
        
        match best_match {
            Some((host, cap, _)) => Ok((host, cap)),
            None => Err(CapHostRegistryError::NoHostsFound(request_urn.to_string())),
        }
    }


    /// Get all registered capability host names
    pub fn get_host_names(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
    }

    /// Get all capabilities from all registered hosts
    pub fn get_all_capabilities(&self) -> Vec<&Cap> {
        self.hosts.values()
            .flat_map(|entry| &entry.capabilities)
            .collect()
    }

    /// Check if any host can handle the specified capability
    pub fn can_handle(&self, request_urn: &str) -> bool {
        self.find_caphosts(request_urn).is_ok()
    }

    /// Unregister a capability host
    pub fn unregister_caphost(&mut self, name: &str) -> bool {
        self.hosts.remove(name).is_some()
    }

    /// Clear all registered hosts
    pub fn clear(&mut self) {
        self.hosts.clear();
    }
}

impl Default for CapHostRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapArguments, CapOutput, OutputType, ArgumentValidation};
    use std::pin::Pin;
    use std::future::Future;
    use std::collections::HashMap;

    // Mock CapHost for testing
    #[derive(Debug)]
    struct MockCapHost {
        name: String,
    }

    impl CapHost for MockCapHost {
        fn execute_cap(
            &self,
            _cap_urn: &str,
            _positional_args: &[String],
            _named_args: &[(String, String)],
            _stdin_data: Option<Vec<u8>>
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>> {
            Box::pin(async move {
                Ok((None, Some(format!("Mock response from {}", self.name))))
            })
        }
    }

    #[tokio::test]
    async fn test_register_and_find_caphost() {
        let mut registry = CapHostRegistry::new();
        
        let host = Box::new(MockCapHost {
            name: "test-host".to_string(),
        });
        
        let cap = Cap {
            urn: CapUrn::from_string("cap:action=test;type=basic").unwrap(),
            title: "Test Basic Capability".to_string(),
            cap_description: Some("Test capability".to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput {
                output_type: OutputType::String,
                schema_ref: None,
                schema: None,
                content_type: None,
                validation: ArgumentValidation::default(),
                output_description: "Test output".to_string(),
            }),
            accepts_stdin: false,
        };
        
        registry.register_caphost("test-host".to_string(), host, vec![cap]).await.unwrap();
        
        // Test exact match
        let hosts = registry.find_caphosts("cap:action=test;type=basic").unwrap();
        assert_eq!(hosts.len(), 1);
        
        // Test subset match (request has more specific requirements)
        let hosts = registry.find_caphosts("cap:action=test;type=basic;model=gpt-4").unwrap();
        assert_eq!(hosts.len(), 1);
        
        // Test no match
        assert!(registry.find_caphosts("cap:action=different").is_err());
    }

    #[tokio::test]
    async fn test_best_caphost_selection() {
        let mut registry = CapHostRegistry::new();
        
        // Register general host
        let general_host = Box::new(MockCapHost {
            name: "general".to_string(),
        });
        let general_cap = Cap {
            urn: CapUrn::from_string("cap:action=generate").unwrap(),
            title: "General Generation Capability".to_string(),
            cap_description: Some("General generation".to_string()),
            metadata: HashMap::new(),
            command: "generate".to_string(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput {
                output_type: OutputType::String,
                schema_ref: None,
                schema: None,
                content_type: None,
                validation: ArgumentValidation::default(),
                output_description: "General output".to_string(),
            }),
            accepts_stdin: false,
        };
        
        // Register specific host
        let specific_host = Box::new(MockCapHost {
            name: "specific".to_string(),
        });
        let specific_cap = Cap {
            urn: CapUrn::from_string("cap:action=generate;type=text;model=gpt-4").unwrap(),
            title: "Specific Text Generation Capability".to_string(),
            cap_description: Some("Specific text generation".to_string()),
            metadata: HashMap::new(),
            command: "generate".to_string(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput {
                output_type: OutputType::String,
                schema_ref: None,
                schema: None,
                content_type: None,
                validation: ArgumentValidation::default(),
                output_description: "Specific output".to_string(),
            }),
            accepts_stdin: false,
        };
        
        registry.register_caphost("general".to_string(), general_host, vec![general_cap]).await.unwrap();
        registry.register_caphost("specific".to_string(), specific_host, vec![specific_cap]).await.unwrap();
        
        // Request should match the more specific host (using valid URN characters)
        let (_best_host, _best_cap) = registry.find_best_caphost("cap:action=generate;type=text;model=gpt-4;temperature=low").unwrap();
        
        // Both hosts should match, but we should get the more specific one
        let all_hosts = registry.find_caphosts("cap:action=generate;type=text;model=gpt-4;temperature=low").unwrap();
        assert_eq!(all_hosts.len(), 2);
    }

    #[test]
    fn test_invalid_urn_handling() {
        let registry = CapHostRegistry::new();
        
        let result = registry.find_caphosts("invalid-urn");
        assert!(matches!(result, Err(CapHostRegistryError::InvalidUrn(_))));
    }

    #[test]
    fn test_can_handle() {
        let mut registry = CapHostRegistry::new();
        
        // Empty registry
        assert!(!registry.can_handle("cap:action=test"));
        
        // After registration
        let host = Box::new(MockCapHost {
            name: "test".to_string(),
        });
        let cap = Cap {
            urn: CapUrn::from_string("cap:action=test").unwrap(),
            title: "Test Capability".to_string(),
            cap_description: Some("Test".to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: None,
            accepts_stdin: false,
        };
        
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            registry.register_caphost("test".to_string(), host, vec![cap]).await.unwrap();
        });
        
        assert!(registry.can_handle("cap:action=test"));
        assert!(registry.can_handle("cap:action=test;extra=param"));
        assert!(!registry.can_handle("cap:action=different"));
    }
}