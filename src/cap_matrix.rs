//! CapSet registry for unified capability host discovery
//! 
//! Provides unified interface for finding cap sets (both providers and plugins)
//! that can satisfy capability requests using subset matching.

use crate::{Cap, CapUrn, CapSet};
use std::collections::HashMap;

/// Registry error types for capability host operations
#[derive(Debug, thiserror::Error)]
pub enum CapMatrixError {
    #[error("No cap sets found for capability: {0}")]
    NoSetsFound(String),
    #[error("Invalid capability URN: {0}")]
    InvalidUrn(String),
    #[error("Registry error: {0}")]
    RegistryError(String),
}

/// Unified registry for cap sets (providers and plugins)
#[derive(Debug)]
pub struct CapMatrix {
    /// Map of host name to entry. pub(crate) for CapCube access.
    pub(crate) sets: HashMap<String, CapSetEntry>,
}

/// Entry for a registered capability host
#[derive(Debug)]
pub(crate) struct CapSetEntry {
    pub(crate) name: String,
    pub(crate) host: std::sync::Arc<dyn CapSet>,
    pub(crate) capabilities: Vec<Cap>,
}

impl CapMatrix {
    /// Create a new empty capability host registry
    pub fn new() -> Self {
        Self {
            sets: HashMap::new(),
        }
    }

    /// Register a capability host with its supported capabilities
    pub fn register_cap_set(
        &mut self,
        name: String,
        host: Box<dyn CapSet>,
        capabilities: Vec<Cap>,
    ) -> Result<(), CapMatrixError> {
        let entry = CapSetEntry {
            name: name.clone(),
            host: std::sync::Arc::from(host),
            capabilities,
        };

        self.sets.insert(name, entry);
        Ok(())
    }

    /// Find cap sets that can handle the requested capability
    /// Uses subset matching: host capabilities must be a subset of or match the request
    pub fn find_cap_sets(&self, request_urn: &str) -> Result<Vec<&dyn CapSet>, CapMatrixError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapMatrixError::InvalidUrn(format!("{}: {}", request_urn, e)))?;
        
        let mut matching_sets = Vec::new();
        
        for entry in self.sets.values() {
            for cap in &entry.capabilities {
                if cap.urn.matches(&request) {
                    matching_sets.push(entry.host.as_ref());
                    break; // Found a matching capability for this host, no need to check others
                }
            }
        }
        
        if matching_sets.is_empty() {
            return Err(CapMatrixError::NoSetsFound(request_urn.to_string()));
        }
        
        Ok(matching_sets)
    }

    /// Find the best capability host for the request using specificity ranking
    /// Returns the CapSet (as Arc for cloning) and the Cap definition that matched
    pub fn find_best_cap_set(&self, request_urn: &str) -> Result<(std::sync::Arc<dyn CapSet>, &Cap), CapMatrixError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapMatrixError::InvalidUrn(format!("{}: {}", request_urn, e)))?;

        let mut best_match: Option<(std::sync::Arc<dyn CapSet>, &Cap, usize)> = None;

        for entry in self.sets.values() {
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
            None => Err(CapMatrixError::NoSetsFound(request_urn.to_string())),
        }
    }


    /// Get all registered capability host names
    pub fn get_host_names(&self) -> Vec<String> {
        self.sets.keys().cloned().collect()
    }

    /// Get all capabilities from all registered sets
    pub fn get_all_capabilities(&self) -> Vec<&Cap> {
        self.sets.values()
            .flat_map(|entry| &entry.capabilities)
            .collect()
    }

    /// Check if any host can handle the specified capability
    pub fn can_handle(&self, request_urn: &str) -> bool {
        self.find_cap_sets(request_urn).is_ok()
    }

    /// Unregister a capability host
    pub fn unregister_cap_set(&mut self, name: &str) -> bool {
        self.sets.remove(name).is_some()
    }

    /// Clear all registered sets
    pub fn clear(&mut self) {
        self.sets.clear();
    }
}

impl Default for CapMatrix {
    fn default() -> Self {
        Self::new()
    }
}

use crate::CapCaller;

/// Result of finding the best match across registries
#[derive(Debug, Clone)]
pub struct BestCapSetMatch {
    /// The Cap definition that matched
    pub cap: Cap,
    /// The specificity score of the match
    pub specificity: usize,
    /// The name of the registry that provided this match
    pub registry_name: String,
}

/// Composite registry that wraps multiple CapMatrix instances
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
pub struct CapCube {
    /// Child registries in priority order (first added = highest priority on ties)
    /// Uses Arc<std::sync::RwLock> for shared access
    registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)>,
}

/// Wrapper that implements CapSet for CapCube
/// This allows the composite to be used with CapCaller
#[derive(Debug)]
pub struct CompositeCapSet {
    registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)>,
}

impl CompositeCapSet {
    fn new(registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)>) -> Self {
        Self { registries }
    }
}

impl CapSet for CompositeCapSet {
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

        // Find the best matching cap_set BEFORE entering async block
        // Clone the Arc<dyn CapSet> so we don't hold the lock across await
        let best_cap_set: std::sync::Arc<dyn CapSet> = {
            let request = match CapUrn::from_string(&cap_urn) {
                Ok(r) => r,
                Err(e) => {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!("Invalid cap URN '{}': {}", cap_urn, e))
                    });
                }
            };

            let mut best_match: Option<(std::sync::Arc<dyn CapSet>, usize)> = None;

            for (_registry_name, registry_arc) in &self.registries {
                let registry = match registry_arc.read() {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Find best match in this registry
                for entry in registry.sets.values() {
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

        // Now we have an owned Arc<dyn CapSet> - no locks held
        Box::pin(async move {
            best_cap_set.execute_cap(&cap_urn, &positional_args, &named_args, stdin_data).await
        })
    }
}

impl CapCube {
    /// Create a new empty composite registry
    pub fn new() -> Self {
        Self {
            registries: Vec::new(),
        }
    }

    /// Add a child registry with a name (shared reference version)
    /// Registries are checked in order of addition for tie-breaking
    pub fn add_registry(&mut self, name: String, registry: std::sync::Arc<std::sync::RwLock<CapMatrix>>) {
        self.registries.push((name, registry));
    }

    /// Remove a child registry by name
    pub fn remove_registry(&mut self, name: &str) -> Option<std::sync::Arc<std::sync::RwLock<CapMatrix>>> {
        if let Some(pos) = self.registries.iter().position(|(n, _)| n == name) {
            Some(self.registries.remove(pos).1)
        } else {
            None
        }
    }

    /// Get the Arc to a child registry by name
    pub fn get_registry(&self, name: &str) -> Option<std::sync::Arc<std::sync::RwLock<CapMatrix>>> {
        self.registries.iter()
            .find(|(n, _)| n == name)
            .map(|(_, r)| r.clone())
    }

    /// Check if a cap is available and return a CapCaller.
    /// This is the main entry point for capability lookup - preserves the can().call() pattern.
    ///
    /// Finds the best (most specific) match across all child registries and returns
    /// a CapCaller ready to execute the capability.
    pub fn can(&self, cap_urn: &str) -> Result<CapCaller, CapMatrixError> {
        // Find the best match to get the cap definition
        let best_match = self.find_best_cap_set(cap_urn)?;

        // Create a CompositeCapSet that will delegate execution to the right registry
        let composite_host = CompositeCapSet::new(self.registries.clone());

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
    pub fn find_best_cap_set(&self, request_urn: &str) -> Result<BestCapSetMatch, CapMatrixError> {
        let request = CapUrn::from_string(request_urn)
            .map_err(|e| CapMatrixError::InvalidUrn(format!("{}: {}", request_urn, e)))?;

        let mut best_overall: Option<BestCapSetMatch> = None;

        for (registry_name, registry_arc) in &self.registries {
            let registry = registry_arc.read()
                .map_err(|_| CapMatrixError::RegistryError("Failed to acquire read lock".to_string()))?;

            // Find the best match within this registry
            if let Some((cap, specificity)) = Self::find_best_in_registry(&registry, &request) {
                let candidate = BestCapSetMatch {
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

        best_overall.ok_or_else(|| CapMatrixError::NoSetsFound(request_urn.to_string()))
    }

    /// Check if any registry can handle the specified capability
    pub fn can_handle(&self, request_urn: &str) -> bool {
        self.find_best_cap_set(request_urn).is_ok()
    }

    /// Get names of all child registries
    pub fn get_registry_names(&self) -> Vec<&str> {
        self.registries.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// Helper: Find the best match within a single registry
    /// Returns (Cap, specificity) for the best match
    fn find_best_in_registry<'a>(
        registry: &'a CapMatrix,
        request: &CapUrn
    ) -> Option<(&'a Cap, usize)> {
        let mut best: Option<(&Cap, usize)> = None;

        for entry in registry.sets.values() {
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
    use crate::{CapArguments, CapOutput};
    use crate::standard::media::SPEC_ID_STR;
    use std::pin::Pin;
    use std::future::Future;
    use std::collections::HashMap;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!("cap:in=std:void.v1;out=std:obj.v1;{}", tags)
    }

    // Mock CapSet for testing
    #[derive(Debug)]
    struct MockCapSet {
        name: String,
    }

    impl CapSet for MockCapSet {
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
    async fn test_register_and_find_cap_set() {
        let mut registry = CapMatrix::new();

        let host = Box::new(MockCapSet {
            name: "test-host".to_string(),
        });

        let cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=test;type=basic")).unwrap(),
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

        registry.register_cap_set("test-host".to_string(), host, vec![cap]).unwrap();

        // Test exact match
        let sets = registry.find_cap_sets(&test_urn("op=test;type=basic")).unwrap();
        assert_eq!(sets.len(), 1);

        // Test subset match (request has more specific requirements)
        let sets = registry.find_cap_sets(&test_urn("op=test;type=basic;model=gpt-4")).unwrap();
        assert_eq!(sets.len(), 1);

        // Test no match
        assert!(registry.find_cap_sets(&test_urn("op=different")).is_err());
    }

    #[tokio::test]
    async fn test_best_cap_set_selection() {
        let mut registry = CapMatrix::new();

        // Register general host
        let general_host = Box::new(MockCapSet {
            name: "general".to_string(),
        });
        let general_cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=generate")).unwrap(),
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
        let specific_host = Box::new(MockCapSet {
            name: "specific".to_string(),
        });
        let specific_cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=generate;type=text;model=gpt-4")).unwrap(),
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

        registry.register_cap_set("general".to_string(), general_host, vec![general_cap]).unwrap();
        registry.register_cap_set("specific".to_string(), specific_host, vec![specific_cap]).unwrap();

        // Request should match the more specific host (using valid URN characters)
        let (_best_host, _best_cap) = registry.find_best_cap_set(&test_urn("op=generate;type=text;model=gpt-4;temperature=low")).unwrap();

        // Both sets should match, but we should get the more specific one
        let all_sets = registry.find_cap_sets(&test_urn("op=generate;type=text;model=gpt-4;temperature=low")).unwrap();
        assert_eq!(all_sets.len(), 2);
    }

    #[test]
    fn test_invalid_urn_handling() {
        let registry = CapMatrix::new();

        let result = registry.find_cap_sets("invalid-urn");
        assert!(matches!(result, Err(CapMatrixError::InvalidUrn(_))));
    }

    #[test]
    fn test_can_handle() {
        let mut registry = CapMatrix::new();

        // Empty registry - need valid URN with in/out
        assert!(!registry.can_handle(&test_urn("op=test")));

        // After registration
        let host = Box::new(MockCapSet {
            name: "test".to_string(),
        });
        let cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=test")).unwrap(),
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
            registry.register_cap_set("test".to_string(), host, vec![cap]).unwrap();
        });

        assert!(registry.can_handle(&test_urn("op=test")));
        assert!(registry.can_handle(&test_urn("op=test;extra=param")));
        assert!(!registry.can_handle(&test_urn("op=different")));
    }

    // ============================================================================
    // CapCube Tests
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
    async fn test_cap_cube_more_specific_wins() {
        // This is the key test: provider has less specific cap, plugin has more specific
        // The more specific one should win regardless of registry order

        let mut provider_registry = CapMatrix::new();
        let mut plugin_registry = CapMatrix::new();

        // Provider: less specific cap
        let provider_host = Box::new(MockCapSet { name: "provider".to_string() });
        let provider_cap = make_cap(
            "cap:in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1",
            "Provider Thumbnail Generator (generic)"
        );
        provider_registry.register_cap_set(
            "provider".to_string(),
            provider_host,
            vec![provider_cap]
        ).unwrap();

        // Plugin: more specific cap (has ext=pdf)
        let plugin_host = Box::new(MockCapSet { name: "plugin".to_string() });
        let plugin_cap = make_cap(
            "cap:ext=pdf;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1",
            "Plugin PDF Thumbnail Generator (specific)"
        );
        plugin_registry.register_cap_set(
            "plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).unwrap();

        // Create composite with provider first (normally would have priority on ties)
        let mut composite = CapCube::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnails - plugin's more specific cap should win
        let request = "cap:ext=pdf;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1";
        let best = composite.find_best_cap_set(request).unwrap();

        // Plugin registry has specificity 4 (in, op, out, ext)
        // Provider registry has specificity 3 (in, op, out)
        // Plugin should win even though providers were added first
        assert_eq!(best.registry_name, "plugins", "More specific plugin should win over less specific provider");
        assert_eq!(best.specificity, 4, "Plugin cap has 4 specific tags");
        assert_eq!(best.cap.title, "Plugin PDF Thumbnail Generator (specific)");
    }

    #[tokio::test]
    async fn test_cap_cube_tie_goes_to_first() {
        // When specificity is equal, first registry wins

        let mut registry1 = CapMatrix::new();
        let mut registry2 = CapMatrix::new();

        // Both have same specificity
        let host1 = Box::new(MockCapSet { name: "host1".to_string() });
        let cap1 = make_cap(&test_urn("op=generate;ext=pdf"), "Registry 1 Cap");
        registry1.register_cap_set("host1".to_string(), host1, vec![cap1]).unwrap();

        let host2 = Box::new(MockCapSet { name: "host2".to_string() });
        let cap2 = make_cap(&test_urn("op=generate;ext=pdf"), "Registry 2 Cap");
        registry2.register_cap_set("host2".to_string(), host2, vec![cap2]).unwrap();

        let mut composite = CapCube::new();
        composite.add_registry("first".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("second".to_string(), Arc::new(RwLock::new(registry2)));

        let best = composite.find_best_cap_set(&test_urn("op=generate;ext=pdf")).unwrap();

        // Both have same specificity, first registry should win
        assert_eq!(best.registry_name, "first", "On tie, first registry should win");
        assert_eq!(best.cap.title, "Registry 1 Cap");
    }

    #[tokio::test]
    async fn test_cap_cube_polls_all() {
        // Test that all registries are polled

        let mut registry1 = CapMatrix::new();
        let mut registry2 = CapMatrix::new();
        let mut registry3 = CapMatrix::new();

        // Registry 1: doesn't match
        let host1 = Box::new(MockCapSet { name: "host1".to_string() });
        let cap1 = make_cap(&test_urn("op=different"), "Registry 1");
        registry1.register_cap_set("host1".to_string(), host1, vec![cap1]).unwrap();

        // Registry 2: matches but less specific
        let host2 = Box::new(MockCapSet { name: "host2".to_string() });
        let cap2 = make_cap(&test_urn("op=generate"), "Registry 2");
        registry2.register_cap_set("host2".to_string(), host2, vec![cap2]).unwrap();

        // Registry 3: matches and most specific
        let host3 = Box::new(MockCapSet { name: "host3".to_string() });
        let cap3 = make_cap(&test_urn("op=generate;ext=pdf;format=thumbnail"), "Registry 3");
        registry3.register_cap_set("host3".to_string(), host3, vec![cap3]).unwrap();

        let mut composite = CapCube::new();
        composite.add_registry("r1".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("r2".to_string(), Arc::new(RwLock::new(registry2)));
        composite.add_registry("r3".to_string(), Arc::new(RwLock::new(registry3)));

        let best = composite.find_best_cap_set(&test_urn("op=generate;ext=pdf;format=thumbnail")).unwrap();

        // Registry 3 has more specific tags
        assert_eq!(best.registry_name, "r3", "Most specific registry should win");
    }

    #[tokio::test]
    async fn test_cap_cube_no_match() {
        let registry = CapMatrix::new();

        let mut composite = CapCube::new();
        composite.add_registry("empty".to_string(), Arc::new(RwLock::new(registry)));

        let result = composite.find_best_cap_set(&test_urn("op=nonexistent"));
        assert!(matches!(result, Err(CapMatrixError::NoSetsFound(_))));
    }

    #[tokio::test]
    async fn test_cap_cube_fallback_scenario() {
        // Test the exact scenario from the user's issue:
        // Provider: generic fallback (can handle any file type)
        // Plugin:   PDF-specific handler
        // Request:  PDF thumbnail
        // Expected: Plugin wins (more specific)

        let mut provider_registry = CapMatrix::new();
        let mut plugin_registry = CapMatrix::new();

        // Provider with generic fallback (can handle any file type)
        let provider_host = Box::new(MockCapSet { name: "provider_fallback".to_string() });
        let provider_cap = make_cap(
            "cap:in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1",
            "Generic Thumbnail Provider"
        );
        provider_registry.register_cap_set(
            "provider_fallback".to_string(),
            provider_host,
            vec![provider_cap]
        ).unwrap();

        // Plugin with PDF-specific handler
        let plugin_host = Box::new(MockCapSet { name: "pdf_plugin".to_string() });
        let plugin_cap = make_cap(
            "cap:ext=pdf;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1",
            "PDF Thumbnail Plugin"
        );
        plugin_registry.register_cap_set(
            "pdf_plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).unwrap();

        // Providers first (would win on tie)
        let mut composite = CapCube::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnail
        let request = "cap:ext=pdf;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1";
        let best = composite.find_best_cap_set(request).unwrap();

        // Plugin (specificity 4) should beat provider (specificity 3)
        assert_eq!(best.registry_name, "plugins");
        assert_eq!(best.cap.title, "PDF Thumbnail Plugin");
        assert_eq!(best.specificity, 4);

        // Also test that for a different file type, provider wins
        let request_wav = "cap:ext=wav;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1";
        let best_wav = composite.find_best_cap_set(request_wav).unwrap();

        // Only provider matches (plugin doesn't match ext=wav)
        assert_eq!(best_wav.registry_name, "providers");
        assert_eq!(best_wav.cap.title, "Generic Thumbnail Provider");
    }

    #[tokio::test]
    async fn test_composite_can_method() {
        // Test the can() method that returns a CapCaller

        let mut provider_registry = CapMatrix::new();

        let provider_host = Box::new(MockCapSet { name: "test_provider".to_string() });
        let provider_cap = make_cap(
            &test_urn("op=generate;ext=pdf"),
            "Test Provider"
        );
        provider_registry.register_cap_set(
            "test_provider".to_string(),
            provider_host,
            vec![provider_cap]
        ).unwrap();

        let mut composite = CapCube::new();
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));

        // Test can() returns a CapCaller
        let _caller = composite.can(&test_urn("op=generate;ext=pdf")).unwrap();

        // Verify we got the right cap
        // The caller should work (though we can't easily test execution in unit tests)
        assert!(composite.can_handle(&test_urn("op=generate;ext=pdf")));
        assert!(!composite.can_handle(&test_urn("op=nonexistent")));
    }
}