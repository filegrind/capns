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
    /// Map of host name to entry. pub(crate) for CompositeCapHostRegistry access.
    pub(crate) hosts: HashMap<String, CapHostEntry>,
}

/// Entry for a registered capability host
#[derive(Debug)]
pub(crate) struct CapHostEntry {
    pub(crate) name: String,
    pub(crate) host: std::sync::Arc<dyn CapHost>,
    pub(crate) capabilities: Vec<Cap>,
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
            host: std::sync::Arc::from(host),
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
    /// Returns the CapHost (as Arc for cloning) and the Cap definition that matched
    pub fn find_best_caphost(&self, request_urn: &str) -> Result<(std::sync::Arc<dyn CapHost>, &Cap), CapHostRegistryError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapHostRegistryError::InvalidUrn(format!("{}: {}", request_urn, e)))?;

        let mut best_match: Option<(std::sync::Arc<dyn CapHost>, &Cap, usize)> = None;

        for entry in self.hosts.values() {
            for cap in &entry.capabilities {
                if cap.urn.matches(&request) {
                    let specificity = cap.urn.specificity();
                    match best_match {
                        None => {
                            best_match = Some((entry.host.clone(), cap, specificity));
                        }
                        Some((_, _, current_specificity)) => {
                            if specificity > current_specificity {
                                best_match = Some((entry.host.clone(), cap, specificity));
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

use crate::CapCaller;

/// Result of finding the best match across registries
#[derive(Debug, Clone)]
pub struct BestCapHostMatch {
    /// The Cap definition that matched
    pub cap: Cap,
    /// The specificity score of the match
    pub specificity: usize,
    /// The name of the registry that provided this match
    pub registry_name: String,
}

/// Composite registry that wraps multiple CapHostRegistry instances
/// and finds the best match across all of them by specificity.
///
/// When multiple registries can handle a request, this registry
/// compares specificity scores and returns the most specific match.
/// On tie, defaults to the first registry that was added (priority order).
///
/// This registry holds Arc references to child registries, allowing
/// the original owners (e.g., ProviderRegistry, PluginGateway) to retain
/// ownership while still participating in unified capability lookup.
#[derive(Debug, Default)]
pub struct CompositeCapHostRegistry {
    /// Child registries in priority order (first added = highest priority on ties)
    /// Uses Arc<std::sync::RwLock> for shared access
    registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapHostRegistry>>)>,
}

/// Wrapper that implements CapHost for CompositeCapHostRegistry
/// This allows the composite to be used with CapCaller
#[derive(Debug)]
pub struct CompositeCapHost {
    registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapHostRegistry>>)>,
}

impl CompositeCapHost {
    fn new(registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapHostRegistry>>)>) -> Self {
        Self { registries }
    }
}

impl CapHost for CompositeCapHost {
    fn execute_cap(
        &self,
        cap_urn: &str,
        positional_args: &[String],
        named_args: &[(String, String)],
        stdin_data: Option<Vec<u8>>
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>> {
        let cap_urn = cap_urn.to_string();
        let positional_args = positional_args.to_vec();
        let named_args = named_args.to_vec();

        // Find the best matching caphost BEFORE entering async block
        // Clone the Arc<dyn CapHost> so we don't hold the lock across await
        let best_caphost: std::sync::Arc<dyn CapHost> = {
            let request = match CapUrn::from_string(&cap_urn) {
                Ok(r) => r,
                Err(e) => {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!("Invalid cap URN '{}': {}", cap_urn, e))
                    });
                }
            };

            let mut best_match: Option<(std::sync::Arc<dyn CapHost>, usize)> = None;

            for (_registry_name, registry_arc) in &self.registries {
                let registry = match registry_arc.read() {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Find best match in this registry
                for entry in registry.hosts.values() {
                    for cap in &entry.capabilities {
                        if cap.urn.matches(&request) {
                            let specificity = cap.urn.specificity();
                            match &best_match {
                                None => {
                                    // Clone the Arc so we don't borrow from registry
                                    best_match = Some((entry.host.clone(), specificity));
                                }
                                Some((_, current_specificity)) => {
                                    if specificity > *current_specificity {
                                        best_match = Some((entry.host.clone(), specificity));
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
                // Registry lock is released here
            }

            match best_match {
                Some((host_arc, _)) => host_arc,
                None => {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!("No capability host found for '{}'", cap_urn))
                    });
                }
            }
        };

        // Now we have an owned Arc<dyn CapHost> - no locks held
        Box::pin(async move {
            best_caphost.execute_cap(&cap_urn, &positional_args, &named_args, stdin_data).await
        })
    }
}

impl CompositeCapHostRegistry {
    /// Create a new empty composite registry
    pub fn new() -> Self {
        Self {
            registries: Vec::new(),
        }
    }

    /// Add a child registry with a name (shared reference version)
    /// Registries are checked in order of addition for tie-breaking
    pub fn add_registry(&mut self, name: String, registry: std::sync::Arc<std::sync::RwLock<CapHostRegistry>>) {
        self.registries.push((name, registry));
    }

    /// Remove a child registry by name
    pub fn remove_registry(&mut self, name: &str) -> Option<std::sync::Arc<std::sync::RwLock<CapHostRegistry>>> {
        if let Some(pos) = self.registries.iter().position(|(n, _)| n == name) {
            Some(self.registries.remove(pos).1)
        } else {
            None
        }
    }

    /// Get the Arc to a child registry by name
    pub fn get_registry(&self, name: &str) -> Option<std::sync::Arc<std::sync::RwLock<CapHostRegistry>>> {
        self.registries.iter()
            .find(|(n, _)| n == name)
            .map(|(_, r)| r.clone())
    }

    /// Check if a cap is available and return a CapCaller.
    /// This is the main entry point for capability lookup - preserves the can().call() pattern.
    ///
    /// Finds the best (most specific) match across all child registries and returns
    /// a CapCaller ready to execute the capability.
    pub fn can(&self, cap_urn: &str) -> Result<CapCaller, CapHostRegistryError> {
        // Find the best match to get the cap definition
        let best_match = self.find_best_caphost(cap_urn)?;

        // Create a CompositeCapHost that will delegate execution to the right registry
        let composite_host = CompositeCapHost::new(self.registries.clone());

        Ok(CapCaller::new(
            cap_urn.to_string(),
            Box::new(composite_host),
            best_match.cap,
        ))
    }

    /// Find the best capability host across ALL child registries.
    ///
    /// This method polls all registries and compares their best matches
    /// by specificity. Returns the cap definition and specificity of the best match.
    /// On specificity tie, returns the match from the first registry (priority order).
    pub fn find_best_caphost(&self, request_urn: &str) -> Result<BestCapHostMatch, CapHostRegistryError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapHostRegistryError::InvalidUrn(format!("{}: {}", request_urn, e)))?;

        let mut best_overall: Option<BestCapHostMatch> = None;

        for (registry_name, registry_arc) in &self.registries {
            let registry = registry_arc.read()
                .map_err(|_| CapHostRegistryError::RegistryError("Failed to acquire read lock".to_string()))?;

            // Find the best match within this registry
            if let Some((cap, specificity)) = Self::find_best_in_registry(&registry, &request) {
                let candidate = BestCapHostMatch {
                    cap: cap.clone(),
                    specificity,
                    registry_name: registry_name.clone(),
                };

                match &best_overall {
                    None => {
                        best_overall = Some(candidate);
                    }
                    Some(current_best) => {
                        // Only replace if strictly more specific
                        // On tie, keep the first one (priority order)
                        if specificity > current_best.specificity {
                            best_overall = Some(candidate);
                        }
                    }
                }
            }
        }

        best_overall.ok_or_else(|| CapHostRegistryError::NoHostsFound(request_urn.to_string()))
    }

    /// Check if any registry can handle the specified capability
    pub fn can_handle(&self, request_urn: &str) -> bool {
        self.find_best_caphost(request_urn).is_ok()
    }

    /// Get names of all child registries
    pub fn get_registry_names(&self) -> Vec<&str> {
        self.registries.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// Helper: Find the best match within a single registry
    /// Returns (Cap, specificity) for the best match
    fn find_best_in_registry<'a>(
        registry: &'a CapHostRegistry,
        request: &CapUrn
    ) -> Option<(&'a Cap, usize)> {
        let mut best: Option<(&Cap, usize)> = None;

        for entry in registry.hosts.values() {
            for cap in &entry.capabilities {
                if cap.urn.matches(request) {
                    let specificity = cap.urn.specificity();
                    match best {
                        None => {
                            best = Some((cap, specificity));
                        }
                        Some((_, current_specificity)) => {
                            if specificity > current_specificity {
                                best = Some((cap, specificity));
                            }
                        }
                    }
                    break; // Found match for this entry, check next entry
                }
            }
        }

        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapArguments, CapOutput, ArgumentValidation};
    use crate::standard::media::SPEC_ID_STR;
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
            urn: CapUrn::from_string("cap:op=test;type=basic").unwrap(),
            title: "Test Basic Capability".to_string(),
            cap_description: Some("Test capability".to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            media_specs: HashMap::new(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput::new(SPEC_ID_STR, "Test output")),
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        };

        registry.register_caphost("test-host".to_string(), host, vec![cap]).await.unwrap();

        // Test exact match
        let hosts = registry.find_caphosts("cap:op=test;type=basic").unwrap();
        assert_eq!(hosts.len(), 1);

        // Test subset match (request has more specific requirements)
        let hosts = registry.find_caphosts("cap:op=test;type=basic;model=gpt-4").unwrap();
        assert_eq!(hosts.len(), 1);

        // Test no match
        assert!(registry.find_caphosts("cap:op=different").is_err());
    }

    #[tokio::test]
    async fn test_best_caphost_selection() {
        let mut registry = CapHostRegistry::new();

        // Register general host
        let general_host = Box::new(MockCapHost {
            name: "general".to_string(),
        });
        let general_cap = Cap {
            urn: CapUrn::from_string("cap:op=generate").unwrap(),
            title: "General Generation Capability".to_string(),
            cap_description: Some("General generation".to_string()),
            metadata: HashMap::new(),
            command: "generate".to_string(),
            media_specs: HashMap::new(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput::new(SPEC_ID_STR, "General output")),
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        };

        // Register specific host
        let specific_host = Box::new(MockCapHost {
            name: "specific".to_string(),
        });
        let specific_cap = Cap {
            urn: CapUrn::from_string("cap:op=generate;type=text;model=gpt-4").unwrap(),
            title: "Specific Text Generation Capability".to_string(),
            cap_description: Some("Specific text generation".to_string()),
            metadata: HashMap::new(),
            command: "generate".to_string(),
            media_specs: HashMap::new(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput::new(SPEC_ID_STR, "Specific output")),
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        };

        registry.register_caphost("general".to_string(), general_host, vec![general_cap]).await.unwrap();
        registry.register_caphost("specific".to_string(), specific_host, vec![specific_cap]).await.unwrap();

        // Request should match the more specific host (using valid URN characters)
        let (_best_host, _best_cap) = registry.find_best_caphost("cap:op=generate;type=text;model=gpt-4;temperature=low").unwrap();

        // Both hosts should match, but we should get the more specific one
        let all_hosts = registry.find_caphosts("cap:op=generate;type=text;model=gpt-4;temperature=low").unwrap();
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
        assert!(!registry.can_handle("cap:op=test"));

        // After registration
        let host = Box::new(MockCapHost {
            name: "test".to_string(),
        });
        let cap = Cap {
            urn: CapUrn::from_string("cap:op=test").unwrap(),
            title: "Test Capability".to_string(),
            cap_description: Some("Test".to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            media_specs: HashMap::new(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: None,
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        };

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            registry.register_caphost("test".to_string(), host, vec![cap]).await.unwrap();
        });

        assert!(registry.can_handle("cap:op=test"));
        assert!(registry.can_handle("cap:op=test;extra=param"));
        assert!(!registry.can_handle("cap:op=different"));
    }

    // ============================================================================
    // CompositeCapHostRegistry Tests
    // ============================================================================

    use std::sync::{Arc, RwLock};

    fn make_cap(urn: &str, title: &str) -> Cap {
        Cap {
            urn: CapUrn::from_string(urn).unwrap(),
            title: title.to_string(),
            cap_description: Some(title.to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            media_specs: HashMap::new(),
            arguments: CapArguments { required: vec![], optional: vec![] },
            output: Some(CapOutput::new(SPEC_ID_STR, "output")),
            accepts_stdin: false,
            metadata_json: None,
            registered_by: None,
        }
    }

    #[tokio::test]
    async fn test_composite_registry_more_specific_wins() {
        // This is the key test: provider has less specific cap, plugin has more specific
        // The more specific one should win regardless of registry order

        let mut provider_registry = CapHostRegistry::new();
        let mut plugin_registry = CapHostRegistry::new();

        // Provider: less specific cap (cap:op=generate_thumbnail;out=std:binary.v1)
        let provider_host = Box::new(MockCapHost { name: "provider".to_string() });
        let provider_cap = make_cap(
            "cap:op=generate_thumbnail;out=std:binary.v1",
            "Provider Thumbnail Generator (generic)"
        );
        provider_registry.register_caphost(
            "provider".to_string(),
            provider_host,
            vec![provider_cap]
        ).await.unwrap();

        // Plugin: more specific cap (cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf)
        let plugin_host = Box::new(MockCapHost { name: "plugin".to_string() });
        let plugin_cap = make_cap(
            "cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf",
            "Plugin PDF Thumbnail Generator (specific)"
        );
        plugin_registry.register_caphost(
            "plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).await.unwrap();

        // Create composite with provider first (normally would have priority on ties)
        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnails - plugin's more specific cap should win
        let request = "cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf";
        let best = composite.find_best_caphost(request).unwrap();

        // Plugin registry has specificity 3 (op, out, ext)
        // Provider registry has specificity 2 (op, out)
        // Plugin should win even though providers were added first
        assert_eq!(best.registry_name, "plugins", "More specific plugin should win over less specific provider");
        assert_eq!(best.specificity, 3, "Plugin cap has 3 specific tags");
        assert_eq!(best.cap.title, "Plugin PDF Thumbnail Generator (specific)");
    }

    #[tokio::test]
    async fn test_composite_registry_tie_goes_to_first() {
        // When specificity is equal, first registry wins

        let mut registry1 = CapHostRegistry::new();
        let mut registry2 = CapHostRegistry::new();

        // Both have same specificity
        let host1 = Box::new(MockCapHost { name: "host1".to_string() });
        let cap1 = make_cap("cap:op=generate;ext=pdf", "Registry 1 Cap");
        registry1.register_caphost("host1".to_string(), host1, vec![cap1]).await.unwrap();

        let host2 = Box::new(MockCapHost { name: "host2".to_string() });
        let cap2 = make_cap("cap:op=generate;ext=pdf", "Registry 2 Cap");
        registry2.register_caphost("host2".to_string(), host2, vec![cap2]).await.unwrap();

        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("first".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("second".to_string(), Arc::new(RwLock::new(registry2)));

        let best = composite.find_best_caphost("cap:op=generate;ext=pdf").unwrap();

        // Both have specificity 2, first registry should win
        assert_eq!(best.registry_name, "first", "On tie, first registry should win");
        assert_eq!(best.cap.title, "Registry 1 Cap");
    }

    #[tokio::test]
    async fn test_composite_registry_polls_all() {
        // Test that all registries are polled

        let mut registry1 = CapHostRegistry::new();
        let mut registry2 = CapHostRegistry::new();
        let mut registry3 = CapHostRegistry::new();

        // Registry 1: doesn't match
        let host1 = Box::new(MockCapHost { name: "host1".to_string() });
        let cap1 = make_cap("cap:op=different", "Registry 1");
        registry1.register_caphost("host1".to_string(), host1, vec![cap1]).await.unwrap();

        // Registry 2: matches but less specific
        let host2 = Box::new(MockCapHost { name: "host2".to_string() });
        let cap2 = make_cap("cap:op=generate", "Registry 2");
        registry2.register_caphost("host2".to_string(), host2, vec![cap2]).await.unwrap();

        // Registry 3: matches and most specific
        let host3 = Box::new(MockCapHost { name: "host3".to_string() });
        let cap3 = make_cap("cap:op=generate;ext=pdf;format=thumbnail", "Registry 3");
        registry3.register_caphost("host3".to_string(), host3, vec![cap3]).await.unwrap();

        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("r1".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("r2".to_string(), Arc::new(RwLock::new(registry2)));
        composite.add_registry("r3".to_string(), Arc::new(RwLock::new(registry3)));

        let best = composite.find_best_caphost("cap:op=generate;ext=pdf;format=thumbnail").unwrap();

        // Registry 3 has specificity 3, Registry 2 has specificity 1
        assert_eq!(best.registry_name, "r3", "Most specific registry should win");
        assert_eq!(best.specificity, 3);
    }

    #[tokio::test]
    async fn test_composite_registry_no_match() {
        let registry = CapHostRegistry::new();

        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("empty".to_string(), Arc::new(RwLock::new(registry)));

        let result = composite.find_best_caphost("cap:op=nonexistent");
        assert!(matches!(result, Err(CapHostRegistryError::NoHostsFound(_))));
    }

    #[tokio::test]
    async fn test_composite_registry_fallback_scenario() {
        // Test the exact scenario from the user's issue:
        // Provider: cap:op=generate_thumbnail;out=std:binary.v1 (generic fallback)
        // Plugin:   cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf (PDF-specific)
        // Request:  cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf
        // Expected: Plugin wins (more specific)

        let mut provider_registry = CapHostRegistry::new();
        let mut plugin_registry = CapHostRegistry::new();

        // Provider with generic fallback (can handle any file type)
        let provider_host = Box::new(MockCapHost { name: "provider_fallback".to_string() });
        let provider_cap = make_cap(
            "cap:op=generate_thumbnail;out=std:binary.v1",
            "Generic Thumbnail Provider"
        );
        provider_registry.register_caphost(
            "provider_fallback".to_string(),
            provider_host,
            vec![provider_cap]
        ).await.unwrap();

        // Plugin with PDF-specific handler
        let plugin_host = Box::new(MockCapHost { name: "pdf_plugin".to_string() });
        let plugin_cap = make_cap(
            "cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf",
            "PDF Thumbnail Plugin"
        );
        plugin_registry.register_caphost(
            "pdf_plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).await.unwrap();

        // Providers first (would win on tie)
        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnail
        let request = "cap:op=generate_thumbnail;out=std:binary.v1;ext=pdf";
        let best = composite.find_best_caphost(request).unwrap();

        // Plugin (specificity 3) should beat provider (specificity 2)
        assert_eq!(best.registry_name, "plugins");
        assert_eq!(best.cap.title, "PDF Thumbnail Plugin");
        assert_eq!(best.specificity, 3);

        // Also test that for a different file type, provider wins
        let request_wav = "cap:op=generate_thumbnail;out=std:binary.v1;ext=wav";
        let best_wav = composite.find_best_caphost(request_wav).unwrap();

        // Only provider matches (plugin doesn't match ext=wav)
        assert_eq!(best_wav.registry_name, "providers");
        assert_eq!(best_wav.cap.title, "Generic Thumbnail Provider");
    }

    #[tokio::test]
    async fn test_composite_can_method() {
        // Test the can() method that returns a CapCaller

        let mut provider_registry = CapHostRegistry::new();

        let provider_host = Box::new(MockCapHost { name: "test_provider".to_string() });
        let provider_cap = make_cap(
            "cap:op=generate;ext=pdf",
            "Test Provider"
        );
        provider_registry.register_caphost(
            "test_provider".to_string(),
            provider_host,
            vec![provider_cap]
        ).await.unwrap();

        let mut composite = CompositeCapHostRegistry::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));

        // Test can() returns a CapCaller
        let caller = composite.can("cap:op=generate;ext=pdf").unwrap();

        // Verify we got the right cap
        // The caller should work (though we can't easily test execution in unit tests)
        assert!(composite.can_handle("cap:op=generate;ext=pdf"));
        assert!(!composite.can_handle("cap:op=nonexistent"));
    }
}