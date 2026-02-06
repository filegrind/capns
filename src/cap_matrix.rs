//! CapSet registry for unified capability host discovery
//!
//! Provides unified interface for finding cap sets (both providers and plugins)
//! that can satisfy capability requests using subset matching.
//!
//! Also provides CapGraph for representing capabilities as a directed graph
//! where nodes are MediaSpec IDs and edges are capabilities that convert
//! from one spec to another.

use crate::{Cap, CapArgumentValue, CapUrn, CapSet};
use crate::media_urn::MediaUrn;
use std::collections::{HashMap, HashSet, VecDeque};

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

// ============================================================================
// CapGraph - Directed graph of capability conversions
// ============================================================================

/// An edge in the capability graph representing a conversion from one MediaSpec to another.
///
/// Each edge corresponds to a capability that can transform data from `from_spec` format
/// to `to_spec` format. The edge stores the full Cap definition for execution.
#[derive(Debug, Clone)]
pub struct CapGraphEdge {
    /// The input MediaSpec ID (e.g., "media:binary")
    pub from_spec: String,
    /// The output MediaSpec ID (e.g., "media:string")
    pub to_spec: String,
    /// The capability that performs this conversion
    pub cap: Cap,
    /// The registry that provided this capability
    pub registry_name: String,
    /// Specificity score for ranking multiple paths
    pub specificity: usize,
}

/// A directed graph where nodes are MediaSpec IDs and edges are capabilities.
///
/// This graph enables discovering conversion paths between different media formats.
/// For example, finding how to convert from "media:binary" to "media:string" through
/// intermediate transformations.
///
/// The graph is built from capabilities in registries, where each cap's `in_spec`
/// and `out_spec` define the edge direction.
#[derive(Debug, Clone)]
pub struct CapGraph {
    /// All edges in the graph
    edges: Vec<CapGraphEdge>,
    /// Index: from_spec -> indices into edges vec
    outgoing: HashMap<String, Vec<usize>>,
    /// Index: to_spec -> indices into edges vec
    incoming: HashMap<String, Vec<usize>>,
    /// All unique spec IDs (nodes in the graph)
    nodes: HashSet<String>,
}

impl CapGraph {
    /// Create a new empty capability graph
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            nodes: HashSet::new(),
        }
    }

    /// Add a capability as an edge in the graph.
    ///
    /// The cap's `in_spec` becomes the source node and `out_spec` becomes the target node.
    pub fn add_cap(&mut self, cap: &Cap, registry_name: &str) {
        let from_spec = cap.urn.in_spec().to_string();
        let to_spec = cap.urn.out_spec().to_string();
        let specificity = cap.urn.specificity();

        // Add nodes
        self.nodes.insert(from_spec.clone());
        self.nodes.insert(to_spec.clone());

        // Create edge
        let edge_index = self.edges.len();
        let edge = CapGraphEdge {
            from_spec: from_spec.clone(),
            to_spec: to_spec.clone(),
            cap: cap.clone(),
            registry_name: registry_name.to_string(),
            specificity,
        };
        self.edges.push(edge);

        // Update indices
        self.outgoing.entry(from_spec).or_default().push(edge_index);
        self.incoming.entry(to_spec).or_default().push(edge_index);
    }

    /// Build a graph from multiple registries.
    ///
    /// Iterates through all capabilities in all registries and adds them as edges.
    pub fn build_from_registries(
        registries: &[(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)]
    ) -> Result<Self, CapMatrixError> {
        let mut graph = Self::new();

        for (registry_name, registry_arc) in registries {
            let registry = registry_arc.read()
                .map_err(|_| CapMatrixError::RegistryError(
                    format!("Failed to acquire read lock for registry '{}'", registry_name)
                ))?;

            for entry in registry.sets.values() {
                for cap in &entry.capabilities {
                    graph.add_cap(cap, registry_name);
                }
            }
        }

        Ok(graph)
    }

    /// Get all nodes (MediaSpec IDs) in the graph.
    pub fn get_nodes(&self) -> &HashSet<String> {
        &self.nodes
    }

    /// Get all edges in the graph.
    pub fn get_edges(&self) -> &[CapGraphEdge] {
        &self.edges
    }

    /// Get all edges originating from a spec (all caps that take this spec as input).
    ///
    /// Uses MediaUrn::satisfies() matching: returns edges where the provided spec
    /// satisfies the edge's from_spec requirement. This allows a specific media URN
    /// like "media:pdf;bytes" to match caps that accept "media:pdf".
    pub fn get_outgoing(&self, spec: &str) -> Vec<&CapGraphEdge> {
        let provided_urn = match MediaUrn::from_string(spec) {
            Ok(urn) => urn,
            Err(_) => return Vec::new(),
        };

        self.edges
            .iter()
            .filter(|edge| {
                match MediaUrn::from_string(&edge.from_spec) {
                    Ok(requirement_urn) => provided_urn.matches(&requirement_urn).expect("MediaUrn prefix mismatch impossible"),
                    Err(_) => false,
                }
            })
            .collect()
    }

    /// Get all edges targeting a spec (all caps that produce this spec as output).
    ///
    /// Uses MediaUrn::satisfies() matching: returns edges where the edge's to_spec
    /// satisfies the requested spec requirement.
    pub fn get_incoming(&self, spec: &str) -> Vec<&CapGraphEdge> {
        let requirement_urn = match MediaUrn::from_string(spec) {
            Ok(urn) => urn,
            Err(_) => return Vec::new(),
        };

        self.edges
            .iter()
            .filter(|edge| {
                match MediaUrn::from_string(&edge.to_spec) {
                    Ok(produced_urn) => produced_urn.matches(&requirement_urn).expect("MediaUrn prefix mismatch impossible"),
                    Err(_) => false,
                }
            })
            .collect()
    }

    /// Check if there's any direct edge from one spec to another.
    ///
    /// Uses satisfies matching: from_spec must satisfy edge input, edge output must satisfy to_spec.
    pub fn has_direct_edge(&self, from_spec: &str, to_spec: &str) -> bool {
        let to_requirement = match MediaUrn::from_string(to_spec) {
            Ok(urn) => urn,
            Err(_) => return false,
        };

        self.get_outgoing(from_spec)
            .iter()
            .any(|edge| {
                match MediaUrn::from_string(&edge.to_spec) {
                    Ok(produced_urn) => produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible"),
                    Err(_) => false,
                }
            })
    }

    /// Get all direct edges from one spec to another.
    ///
    /// Returns all capabilities that can directly convert from `from_spec` to `to_spec`.
    /// Uses satisfies matching for both input and output specs.
    /// Sorted by specificity (highest first).
    pub fn get_direct_edges(&self, from_spec: &str, to_spec: &str) -> Vec<&CapGraphEdge> {
        let to_requirement = match MediaUrn::from_string(to_spec) {
            Ok(urn) => urn,
            Err(_) => return Vec::new(),
        };

        let mut edges: Vec<&CapGraphEdge> = self.get_outgoing(from_spec)
            .into_iter()
            .filter(|edge| {
                match MediaUrn::from_string(&edge.to_spec) {
                    Ok(produced_urn) => produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible"),
                    Err(_) => false,
                }
            })
            .collect();

        // Sort by specificity (highest first)
        edges.sort_by(|a, b| b.specificity.cmp(&a.specificity));
        edges
    }

    /// Check if a conversion path exists from one spec to another.
    ///
    /// Uses BFS to find if there's any path (direct or through intermediates).
    /// Uses satisfies matching for both input and output specs.
    pub fn can_convert(&self, from_spec: &str, to_spec: &str) -> bool {
        if from_spec == to_spec {
            return true;
        }

        let to_requirement = match MediaUrn::from_string(to_spec) {
            Ok(urn) => urn,
            Err(_) => return false,
        };

        // Check if from_spec can satisfy any edge's input
        let initial_edges = self.get_outgoing(from_spec);
        if initial_edges.is_empty() {
            return false;
        }

        let mut visited = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Start by checking edges from the initial spec
        for edge in &initial_edges {
            if let Ok(produced_urn) = MediaUrn::from_string(&edge.to_spec) {
                if produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible") {
                    return true;
                }
            }
            if !visited.contains(&edge.to_spec) {
                visited.insert(edge.to_spec.clone());
                queue.push_back(edge.to_spec.clone());
            }
        }

        // BFS through the graph using actual node specs
        while let Some(current) = queue.pop_front() {
            for edge in self.get_outgoing(&current) {
                if let Ok(produced_urn) = MediaUrn::from_string(&edge.to_spec) {
                    if produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible") {
                        return true;
                    }
                }
                if !visited.contains(&edge.to_spec) {
                    visited.insert(edge.to_spec.clone());
                    queue.push_back(edge.to_spec.clone());
                }
            }
        }

        false
    }

    /// Find the shortest conversion path from one spec to another.
    ///
    /// Returns a sequence of edges representing the conversion chain.
    /// Uses satisfies matching for both input and output specs.
    /// Returns None if no path exists.
    pub fn find_path(&self, from_spec: &str, to_spec: &str) -> Option<Vec<&CapGraphEdge>> {
        if from_spec == to_spec {
            return Some(Vec::new());
        }

        let to_requirement = match MediaUrn::from_string(to_spec) {
            Ok(urn) => urn,
            Err(_) => return None,
        };

        // Track visited nodes and parent edges for path reconstruction
        // Key: node spec, Value: (parent node spec, edge index in self.edges)
        let mut visited: HashMap<String, Option<(String, usize)>> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();

        // Find edges that the input spec satisfies
        let initial_edges = self.get_outgoing(from_spec);
        if initial_edges.is_empty() {
            return None;
        }

        // Process initial edges
        for edge in &initial_edges {
            // Find actual edge index
            let edge_idx = self.edges.iter().position(|e| std::ptr::eq(e, *edge))?;

            if let Ok(produced_urn) = MediaUrn::from_string(&edge.to_spec) {
                if produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible") {
                    // Direct path found
                    return Some(vec![&self.edges[edge_idx]]);
                }
            }

            if !visited.contains_key(&edge.to_spec) {
                visited.insert(edge.to_spec.clone(), Some((from_spec.to_string(), edge_idx)));
                queue.push_back(edge.to_spec.clone());
            }
        }

        // BFS through the graph
        while let Some(current) = queue.pop_front() {
            for edge in self.get_outgoing(&current) {
                let edge_idx = self.edges.iter().position(|e| std::ptr::eq(e, edge))?;

                if let Ok(produced_urn) = MediaUrn::from_string(&edge.to_spec) {
                    if produced_urn.matches(&to_requirement).expect("MediaUrn prefix mismatch impossible") {
                        // Found target - reconstruct path
                        let mut path_indices = vec![edge_idx];
                        let mut backtrack = current.clone();

                        while let Some(Some((prev, prev_edge_idx))) = visited.get(&backtrack) {
                            path_indices.push(*prev_edge_idx);
                            backtrack = prev.clone();
                        }

                        path_indices.reverse();
                        return Some(path_indices.iter().map(|&i| &self.edges[i]).collect());
                    }
                }

                if !visited.contains_key(&edge.to_spec) {
                    visited.insert(edge.to_spec.clone(), Some((current.clone(), edge_idx)));
                    queue.push_back(edge.to_spec.clone());
                }
            }
        }

        None
    }

    /// Find all conversion paths from one spec to another (up to a maximum depth).
    ///
    /// Returns all possible paths, sorted by total path length (shortest first).
    /// Uses satisfies matching for both input and output specs.
    /// Limits search to `max_depth` edges to prevent infinite loops in cyclic graphs.
    pub fn find_all_paths(
        &self,
        from_spec: &str,
        to_spec: &str,
        max_depth: usize,
    ) -> Vec<Vec<&CapGraphEdge>> {
        let to_requirement = match MediaUrn::from_string(to_spec) {
            Ok(urn) => urn,
            Err(_) => return Vec::new(),
        };

        // Check if from_spec can satisfy any edge's input
        let initial_edges = self.get_outgoing(from_spec);
        if initial_edges.is_empty() {
            return Vec::new();
        }

        let mut all_paths = Vec::new();
        let mut current_path: Vec<usize> = Vec::new();
        let mut visited = HashSet::new();

        self.dfs_find_paths(
            from_spec,
            &to_requirement,
            max_depth,
            &mut current_path,
            &mut visited,
            &mut all_paths,
        );

        // Sort by path length (shortest first)
        all_paths.sort_by(|a, b| a.len().cmp(&b.len()));

        // Convert indices to edge references
        all_paths
            .into_iter()
            .map(|indices| indices.into_iter().map(|i| &self.edges[i]).collect())
            .collect()
    }

    /// DFS helper for finding all paths
    /// Uses satisfies matching for output spec comparison
    fn dfs_find_paths(
        &self,
        current: &str,
        target: &MediaUrn,
        remaining_depth: usize,
        current_path: &mut Vec<usize>,
        visited: &mut HashSet<String>,
        all_paths: &mut Vec<Vec<usize>>,
    ) {
        if remaining_depth == 0 {
            return;
        }

        for edge in self.get_outgoing(current) {
            // Find edge index
            let edge_idx = match self.edges.iter().position(|e| std::ptr::eq(e, edge)) {
                Some(idx) => idx,
                None => continue,
            };

            // Check if edge output satisfies target
            let output_satisfies = match MediaUrn::from_string(&edge.to_spec) {
                Ok(produced) => produced.matches(target).expect("MediaUrn prefix mismatch impossible"),
                Err(_) => false,
            };

            if output_satisfies {
                // Found a path
                let mut path = current_path.clone();
                path.push(edge_idx);
                all_paths.push(path);
            } else if !visited.contains(&edge.to_spec) {
                // Continue searching
                visited.insert(edge.to_spec.clone());
                current_path.push(edge_idx);

                self.dfs_find_paths(
                    &edge.to_spec,
                    target,
                    remaining_depth - 1,
                    current_path,
                    visited,
                    all_paths,
                );

                current_path.pop();
                visited.remove(&edge.to_spec);
            }
        }
    }

    /// Find the best (highest specificity) conversion path from one spec to another.
    ///
    /// Unlike `find_path` which finds the shortest path, this finds the path with
    /// the highest total specificity score (sum of all edge specificities).
    pub fn find_best_path(&self, from_spec: &str, to_spec: &str, max_depth: usize) -> Option<Vec<&CapGraphEdge>> {
        let all_paths = self.find_all_paths(from_spec, to_spec, max_depth);

        all_paths
            .into_iter()
            .max_by_key(|path| path.iter().map(|e| e.specificity).sum::<usize>())
    }

    /// Get all input specs (specs that have at least one outgoing edge).
    pub fn get_input_specs(&self) -> Vec<&str> {
        self.outgoing.keys().map(|s| s.as_str()).collect()
    }

    /// Get all output specs (specs that have at least one incoming edge).
    pub fn get_output_specs(&self) -> Vec<&str> {
        self.incoming.keys().map(|s| s.as_str()).collect()
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> CapGraphStats {
        CapGraphStats {
            node_count: self.nodes.len(),
            edge_count: self.edges.len(),
            input_spec_count: self.outgoing.len(),
            output_spec_count: self.incoming.len(),
        }
    }
}

impl Default for CapGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about a capability graph.
#[derive(Debug, Clone)]
pub struct CapGraphStats {
    /// Number of unique MediaSpec nodes
    pub node_count: usize,
    /// Number of edges (capabilities)
    pub edge_count: usize,
    /// Number of specs that serve as inputs
    pub input_spec_count: usize,
    /// Number of specs that serve as outputs
    pub output_spec_count: usize,
}

/// Unified registry for cap sets (providers and plugins)
#[derive(Debug)]
pub struct CapMatrix {
    /// Map of host name to entry. pub(crate) for CapCube access.
    pub(crate) sets: HashMap<String, CapSetEntry>,
    /// Media URN registry for resolving media specs
    pub(crate) media_registry: std::sync::Arc<crate::media_registry::MediaUrnRegistry>,
}

/// Entry for a registered capability host
#[derive(Debug)]
pub(crate) struct CapSetEntry {
    pub(crate) name: String,
    pub(crate) host: std::sync::Arc<dyn CapSet>,
    pub(crate) capabilities: Vec<Cap>,
}

impl CapMatrix {
    /// Create a new capability host registry with the given media registry
    pub fn new(media_registry: std::sync::Arc<crate::media_registry::MediaUrnRegistry>) -> Self {
        Self {
            sets: HashMap::new(),
            media_registry,
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

    /// Get capabilities for a specific host
    pub fn get_capabilities_for_host(&self, host_name: &str) -> Option<&[Cap]> {
        self.sets.get(host_name).map(|entry| entry.capabilities.as_slice())
    }

    /// Iterate over all hosts and their capabilities
    pub fn iter_hosts_and_caps(&self) -> impl Iterator<Item = (&str, &[Cap])> {
        self.sets.iter().map(|(name, entry)| (name.as_str(), entry.capabilities.as_slice()))
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

// CapMatrix cannot implement Default since it requires a MediaUrnRegistry

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
#[derive(Debug)]
pub struct CapCube {
    /// Child registries in priority order (first added = highest priority on ties)
    /// Uses Arc<std::sync::RwLock> for shared access
    registries: Vec<(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)>,
    /// Media URN registry for resolving media specs
    media_registry: std::sync::Arc<crate::media_registry::MediaUrnRegistry>,
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

    /// Build a directed graph from all capabilities in the registries.
    ///
    /// The graph represents all possible conversions where:
    /// - Nodes are MediaSpec IDs (e.g., "media:string", "media:binary")
    /// - Edges are capabilities that convert from one spec to another
    ///
    /// This enables discovering conversion paths between different media formats.
    pub fn graph(&self) -> Result<CapGraph, CapMatrixError> {
        CapGraph::build_from_registries(&self.registries)
    }

    /// Get a reference to the underlying registries.
    pub fn registries(&self) -> &[(String, std::sync::Arc<std::sync::RwLock<CapMatrix>>)] {
        &self.registries
    }
}

impl CapSet for CompositeCapSet {
    fn execute_cap(
        &self,
        cap_urn: &str,
        arguments: &[CapArgumentValue],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>> {
        let cap_urn = cap_urn.to_string();
        let arguments = arguments.to_vec();

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
            best_cap_set.execute_cap(&cap_urn, &arguments).await
        })
    }
}

impl CapCube {
    /// Create a new composite registry with the given media registry
    pub fn new(media_registry: std::sync::Arc<crate::media_registry::MediaUrnRegistry>) -> Self {
        Self {
            registries: Vec::new(),
            media_registry,
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
            self.media_registry.clone(),
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

    /// Build a directed graph from all capabilities across all registries.
    ///
    /// The graph represents all possible conversions where:
    /// - Nodes are MediaSpec IDs (e.g., "media:string", "media:binary")
    /// - Edges are capabilities that convert from one spec to another
    ///
    /// This enables discovering conversion paths between different media formats.
    ///
    /// # Example
    /// ```ignore
    /// let cube = CapCube::new();
    /// // ... add registries ...
    /// let graph = cube.graph()?;
    ///
    /// // Find all ways to convert binary to text
    /// let paths = graph.find_all_paths("media:binary", "media:string", 3);
    ///
    /// // Check if conversion is possible
    /// if graph.can_convert("media:binary", "media:object") {
    ///     // conversion exists
    /// }
    /// ```
    pub fn graph(&self) -> Result<CapGraph, CapMatrixError> {
        CapGraph::build_from_registries(&self.registries)
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
    use crate::CapOutput;
    use crate::standard::media::{MEDIA_STRING, MEDIA_OBJECT, MEDIA_BINARY};
    use crate::media_registry::MediaUrnRegistry;
    use std::pin::Pin;
    use std::future::Future;
    use std::collections::HashMap;
    use tempfile::TempDir;

    // Helper to create a test MediaUrnRegistry wrapped in Arc
    fn test_media_registry() -> (std::sync::Arc<MediaUrnRegistry>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("media");

        let registry = MediaUrnRegistry::new_for_test(cache_dir).unwrap();

        (std::sync::Arc::new(registry), temp_dir)
    }

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:form=map";{}"#, tags)
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
            _arguments: &[CapArgumentValue],
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>> {
            Box::pin(async move {
                Ok((None, Some(format!("Mock response from {}", self.name))))
            })
        }
    }

    // TEST117: Test registering cap set and finding by exact and subset matching
    #[tokio::test]
    async fn test_register_and_find_cap_set() {
        let (media_registry, _temp_dir) = test_media_registry();
        let mut registry = CapMatrix::new(media_registry);

        let host = Box::new(MockCapSet {
            name: "test-host".to_string(),
        });

        let cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=test;basic")).unwrap(),
            title: "Test Basic Capability".to_string(),
            cap_description: Some("Test capability".to_string()),
            metadata: HashMap::new(),
            command: "test".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "Test output")),
            metadata_json: None,
            registered_by: None,
        };

        registry.register_cap_set("test-host".to_string(), host, vec![cap]).unwrap();

        // Test exact match
        let sets = registry.find_cap_sets(&test_urn("op=test;basic")).unwrap();
        assert_eq!(sets.len(), 1);

        // Test subset match (request has more specific requirements)
        let sets = registry.find_cap_sets(&test_urn("op=test;basic;model=gpt-4")).unwrap();
        assert_eq!(sets.len(), 1);

        // Test no match
        assert!(registry.find_cap_sets(&test_urn("op=different")).is_err());
    }

    // TEST118: Test selecting best cap set based on specificity ranking
    #[tokio::test]
    async fn test_best_cap_set_selection() {
        let (media_registry, _temp_dir) = test_media_registry();
        let mut registry = CapMatrix::new(media_registry);

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
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "General output")),
            metadata_json: None,
            registered_by: None,
        };

        // Register specific host
        let specific_host = Box::new(MockCapSet {
            name: "specific".to_string(),
        });
        let specific_cap = Cap {
            urn: CapUrn::from_string(&test_urn("op=generate;text;model=gpt-4")).unwrap(),
            title: "Specific Text Generation Capability".to_string(),
            cap_description: Some("Specific text generation".to_string()),
            metadata: HashMap::new(),
            command: "generate".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "Specific output")),
            metadata_json: None,
            registered_by: None,
        };

        registry.register_cap_set("general".to_string(), general_host, vec![general_cap]).unwrap();
        registry.register_cap_set("specific".to_string(), specific_host, vec![specific_cap]).unwrap();

        // Request should match the more specific host (using valid URN characters)
        let (_best_host, _best_cap) = registry.find_best_cap_set(&test_urn("op=generate;text;model=gpt-4;temperature=low")).unwrap();

        // Both sets should match, but we should get the more specific one
        let all_sets = registry.find_cap_sets(&test_urn("op=generate;text;model=gpt-4;temperature=low")).unwrap();
        assert_eq!(all_sets.len(), 2);
    }

    // TEST119: Test invalid URN returns InvalidUrn error
    #[tokio::test]
    async fn test_invalid_urn_handling() {
        let (media_registry, _temp_dir) = test_media_registry();
        let registry = CapMatrix::new(media_registry);

        let result = registry.find_cap_sets("invalid-urn");
        assert!(matches!(result, Err(CapMatrixError::InvalidUrn(_))));
    }

    // TEST120: Test can_handle checks if registry can handle a capability request
    #[tokio::test]
    async fn test_can_handle() {
        let (media_registry, _temp_dir) = test_media_registry();
        let mut registry = CapMatrix::new(media_registry);

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
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        registry.register_cap_set("test".to_string(), host, vec![cap]).unwrap();

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
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "output")),
            metadata_json: None,
            registered_by: None,
        }
    }

    // TEST121: Test CapCube selects more specific cap over less specific regardless of registry order
    #[tokio::test]
    async fn test_cap_cube_more_specific_wins() {
        // This is the key test: provider has less specific cap, plugin has more specific
        // The more specific one should win regardless of registry order
        let (media_registry, _temp_dir) = test_media_registry();

        let mut provider_registry = CapMatrix::new(media_registry.clone());
        let mut plugin_registry = CapMatrix::new(media_registry.clone());

        // Provider: less specific cap
        let provider_host = Box::new(MockCapSet { name: "provider".to_string() });
        let provider_cap = make_cap(
            r#"cap:in="media:binary";op=generate_thumbnail;out="media:binary""#,
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
            r#"cap:ext=pdf;in="media:binary";op=generate_thumbnail;out="media:binary""#,
            "Plugin PDF Thumbnail Generator (specific)"
        );
        plugin_registry.register_cap_set(
            "plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).unwrap();

        // Create composite with provider first (normally would have priority on ties)
        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnails - plugin's more specific cap should win
        let request = r#"cap:ext=pdf;in="media:binary";op=generate_thumbnail;out="media:binary""#;
        let best = composite.find_best_cap_set(request).unwrap();

        // Plugin registry has specificity 4 (in, op, out, ext)
        // Provider registry has specificity 3 (in, op, out)
        // Plugin should win even though providers were added first
        assert_eq!(best.registry_name, "plugins", "More specific plugin should win over less specific provider");
        assert_eq!(best.specificity, 4, "Plugin cap has 4 specific tags");
        assert_eq!(best.cap.title, "Plugin PDF Thumbnail Generator (specific)");
    }

    // TEST122: Test CapCube breaks specificity ties by first registered registry
    #[tokio::test]
    async fn test_cap_cube_tie_goes_to_first() {
        // When specificity is equal, first registry wins
        let (media_registry, _temp_dir) = test_media_registry();

        let mut registry1 = CapMatrix::new(media_registry.clone());
        let mut registry2 = CapMatrix::new(media_registry.clone());

        // Both have same specificity
        let host1 = Box::new(MockCapSet { name: "host1".to_string() });
        let cap1 = make_cap(&test_urn("op=generate;ext=pdf"), "Registry 1 Cap");
        registry1.register_cap_set("host1".to_string(), host1, vec![cap1]).unwrap();

        let host2 = Box::new(MockCapSet { name: "host2".to_string() });
        let cap2 = make_cap(&test_urn("op=generate;ext=pdf"), "Registry 2 Cap");
        registry2.register_cap_set("host2".to_string(), host2, vec![cap2]).unwrap();

        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("first".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("second".to_string(), Arc::new(RwLock::new(registry2)));

        let best = composite.find_best_cap_set(&test_urn("op=generate;ext=pdf")).unwrap();

        // Both have same specificity, first registry should win
        assert_eq!(best.registry_name, "first", "On tie, first registry should win");
        assert_eq!(best.cap.title, "Registry 1 Cap");
    }

    // TEST123: Test CapCube polls all registries to find most specific match
    #[tokio::test]
    async fn test_cap_cube_polls_all() {
        // Test that all registries are polled
        let (media_registry, _temp_dir) = test_media_registry();

        let mut registry1 = CapMatrix::new(media_registry.clone());
        let mut registry2 = CapMatrix::new(media_registry.clone());
        let mut registry3 = CapMatrix::new(media_registry.clone());

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

        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("r1".to_string(), Arc::new(RwLock::new(registry1)));
        composite.add_registry("r2".to_string(), Arc::new(RwLock::new(registry2)));
        composite.add_registry("r3".to_string(), Arc::new(RwLock::new(registry3)));

        let best = composite.find_best_cap_set(&test_urn("op=generate;ext=pdf;format=thumbnail")).unwrap();

        // Registry 3 has more specific tags
        assert_eq!(best.registry_name, "r3", "Most specific registry should win");
    }

    // TEST124: Test CapCube returns error when no registries match the request
    #[tokio::test]
    async fn test_cap_cube_no_match() {
        let (media_registry, _temp_dir) = test_media_registry();
        let registry = CapMatrix::new(media_registry.clone());

        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("empty".to_string(), Arc::new(RwLock::new(registry)));

        let result = composite.find_best_cap_set(&test_urn("op=nonexistent"));
        assert!(matches!(result, Err(CapMatrixError::NoSetsFound(_))));
    }

    // TEST125: Test CapCube prefers specific plugin over generic provider fallback
    #[tokio::test]
    async fn test_cap_cube_fallback_scenario() {
        // Test the exact scenario from the user's issue:
        // Provider: generic fallback (can handle any file type)
        // Plugin:   PDF-specific handler
        // Request:  PDF thumbnail
        // Expected: Plugin wins (more specific)
        let (media_registry, _temp_dir) = test_media_registry();

        let mut provider_registry = CapMatrix::new(media_registry.clone());
        let mut plugin_registry = CapMatrix::new(media_registry.clone());

        // Provider with generic fallback (can handle any file type)
        let provider_host = Box::new(MockCapSet { name: "provider_fallback".to_string() });
        let provider_cap = make_cap(
            r#"cap:in="media:binary";op=generate_thumbnail;out="media:binary""#,
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
            r#"cap:ext=pdf;in="media:binary";op=generate_thumbnail;out="media:binary""#,
            "PDF Thumbnail Plugin"
        );
        plugin_registry.register_cap_set(
            "pdf_plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).unwrap();

        // Providers first (would win on tie)
        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        composite.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Request for PDF thumbnail
        let request = r#"cap:ext=pdf;in="media:binary";op=generate_thumbnail;out="media:binary""#;
        let best = composite.find_best_cap_set(request).unwrap();

        // Plugin (specificity 4) should beat provider (specificity 3)
        assert_eq!(best.registry_name, "plugins");
        assert_eq!(best.cap.title, "PDF Thumbnail Plugin");
        assert_eq!(best.specificity, 4);

        // Also test that for a different file type, provider wins
        let request_wav = r#"cap:ext=wav;in="media:binary";op=generate_thumbnail;out="media:binary""#;
        let best_wav = composite.find_best_cap_set(request_wav).unwrap();

        // Only provider matches (plugin doesn't match ext=wav)
        assert_eq!(best_wav.registry_name, "providers");
        assert_eq!(best_wav.cap.title, "Generic Thumbnail Provider");
    }

    // TEST126: Test composite can method returns CapCaller for capability execution
    #[tokio::test]
    async fn test_composite_can_method() {
        // Test the can() method that returns a CapCaller
        let (media_registry, _temp_dir) = test_media_registry();

        let mut provider_registry = CapMatrix::new(media_registry.clone());

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

        let mut composite = CapCube::new(media_registry.clone());
        composite.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));

        // Test can() returns a CapCaller
        let _caller = composite.can(&test_urn("op=generate;ext=pdf")).unwrap();

        // Verify we got the right cap
        // The caller should work (though we can't easily test execution in unit tests)
        assert!(composite.can_handle(&test_urn("op=generate;ext=pdf")));
        assert!(!composite.can_handle(&test_urn("op=nonexistent")));
    }

    // ============================================================================
    // CapGraph Tests
    // ============================================================================

    // TEST127: Test CapGraph adds nodes and edges from capability definitions
    #[test]
    fn test_cap_graph_basic_construction() {
        let mut graph = CapGraph::new();

        // Create a cap that converts binary to str
        // Use full media URN strings for proper matching
        let media_binary = "media:bytes";
        let cap = Cap {
            urn: CapUrn::from_string(&format!(r#"cap:in="{}";op=extract_text;out="{}""#, media_binary, MEDIA_STRING)).unwrap(),
            title: "Text Extractor".to_string(),
            cap_description: Some("Extract text from binary".to_string()),
            metadata: HashMap::new(),
            command: "extract".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "output")),
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap, "test_registry");

        // Check nodes were created
        assert!(graph.get_nodes().len() >= 2, "Should have at least 2 nodes");

        // Check edge was created
        assert!(graph.get_edges().len() >= 1, "Should have at least 1 edge");
        assert!(graph.has_direct_edge(media_binary, MEDIA_STRING), "Should have edge from binary to string");
    }

    // TEST128: Test CapGraph tracks outgoing and incoming edges for spec conversions
    #[test]
    fn test_cap_graph_outgoing_incoming() {
        let mut graph = CapGraph::new();

        // binary -> str - use full constants for proper matching
        let cap1 = Cap {
            urn: CapUrn::from_string(&format!(r#"cap:in="{}";op=extract_text;out="{}""#, MEDIA_BINARY, MEDIA_STRING)).unwrap(),
            title: "Text Extractor".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "extract".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        // binary -> obj (JSON)
        let cap2 = Cap {
            urn: CapUrn::from_string(&format!(r#"cap:in="{}";op=parse_json;out="{}""#, MEDIA_BINARY, MEDIA_OBJECT)).unwrap(),
            title: "JSON Parser".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "parse".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry1");
        graph.add_cap(&cap2, "registry2");

        // Check outgoing from binary - use full constant
        let outgoing = graph.get_outgoing(MEDIA_BINARY);
        assert_eq!(outgoing.len(), 2);

        // Check incoming to str
        let incoming_str = graph.get_incoming(MEDIA_STRING);
        assert_eq!(incoming_str.len(), 1);

        // Check incoming to obj
        let incoming_obj = graph.get_incoming(MEDIA_OBJECT);
        assert_eq!(incoming_obj.len(), 1);
    }

    // TEST129: Test CapGraph detects direct and indirect conversion paths between specs
    #[test]
    fn test_cap_graph_can_convert() {
        let mut graph = CapGraph::new();

        // binary -> str - use full constants
        let cap1 = Cap {
            urn: CapUrn::from_string(&format!(r#"cap:in="{}";op=extract;out="{}""#, MEDIA_BINARY, MEDIA_STRING)).unwrap(),
            title: "Binary to Str".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "convert".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        // str -> obj
        let cap2 = Cap {
            urn: CapUrn::from_string(&format!(r#"cap:in="{}";op=parse;out="{}""#, MEDIA_STRING, MEDIA_OBJECT)).unwrap(),
            title: "Str to Obj".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "parse".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry");
        graph.add_cap(&cap2, "registry");

        // Direct conversions
        assert!(graph.can_convert(MEDIA_BINARY, MEDIA_STRING));
        assert!(graph.can_convert(MEDIA_STRING, MEDIA_OBJECT));

        // Indirect conversion (through intermediate)
        assert!(graph.can_convert(MEDIA_BINARY, MEDIA_OBJECT));

        // Same spec
        assert!(graph.can_convert(MEDIA_BINARY, MEDIA_BINARY));

        // No path
        assert!(!graph.can_convert(MEDIA_OBJECT, MEDIA_BINARY));

        // Unknown spec
        assert!(!graph.can_convert(MEDIA_BINARY, "unknown:spec.v1"));
    }

    // TEST130: Test CapGraph finds shortest path for spec conversion chain
    #[test]
    fn test_cap_graph_find_path() {
        let mut graph = CapGraph::new();

        // Create a chain: binary -> str -> obj
        let cap1 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=extract;out="media:string""#).unwrap(),
            title: "Binary to Str".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "extract".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        let cap2 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:string";op=parse;out="media:object""#).unwrap(),
            title: "Str to Obj".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "parse".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry");
        graph.add_cap(&cap2, "registry");

        // Find path from binary to obj (should be 2 edges)
        let path = graph.find_path("media:binary", "media:object").unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].from_spec, "media:binary");
        assert_eq!(path[0].to_spec, "media:string");
        assert_eq!(path[1].from_spec, "media:string");
        assert_eq!(path[1].to_spec, "media:object");

        // Find direct path
        let direct = graph.find_path("media:binary", "media:string").unwrap();
        assert_eq!(direct.len(), 1);

        // No path
        let no_path = graph.find_path("media:object", "media:binary");
        assert!(no_path.is_none());

        // Same spec (empty path)
        let same = graph.find_path("media:binary", "media:binary").unwrap();
        assert!(same.is_empty());
    }

    // TEST131: Test CapGraph finds all conversion paths sorted by length
    #[test]
    fn test_cap_graph_find_all_paths() {
        let mut graph = CapGraph::new();

        // Create multiple paths: A -> B -> C and A -> C directly
        let cap1 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=step1;out="media:string""#).unwrap(),
            title: "A to B".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "step1".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        let cap2 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:string";op=step2;out="media:object""#).unwrap(),
            title: "B to C".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "step2".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        let cap3 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=direct;out="media:object""#).unwrap(),
            title: "A to C Direct".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "direct".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry");
        graph.add_cap(&cap2, "registry");
        graph.add_cap(&cap3, "registry");

        // Find all paths from binary to obj
        let all_paths = graph.find_all_paths("media:binary", "media:object", 5);
        assert_eq!(all_paths.len(), 2);

        // Paths should be sorted by length (shortest first)
        assert_eq!(all_paths[0].len(), 1); // Direct path
        assert_eq!(all_paths[1].len(), 2); // Through intermediate
    }

    // TEST132: Test CapGraph returns direct edges sorted by specificity
    #[test]
    fn test_cap_graph_get_direct_edges_sorted() {
        let mut graph = CapGraph::new();

        // Add multiple caps with different specificities for same conversion
        let cap1 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=generic;out="media:string""#).unwrap(),
            title: "Generic".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "generic".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        let cap2 = Cap {
            urn: CapUrn::from_string(r#"cap:ext=pdf;in="media:binary";op=specific;out="media:string""#).unwrap(),
            title: "Specific PDF".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "specific".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry");
        graph.add_cap(&cap2, "registry");

        // Get direct edges - should be sorted by specificity (highest first)
        let edges = graph.get_direct_edges("media:binary", "media:string");
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].cap.title, "Specific PDF"); // Higher specificity
        assert_eq!(edges[1].cap.title, "Generic"); // Lower specificity
    }

    // TEST133: Test CapCube graph integration with multiple registries and conversion paths
    #[tokio::test]
    async fn test_cap_cube_graph_integration() {
        // Test that CapCube.graph() works correctly
        let (media_registry, _temp_dir) = test_media_registry();

        let mut provider_registry = CapMatrix::new(media_registry.clone());
        let mut plugin_registry = CapMatrix::new(media_registry.clone());

        // Provider: binary -> str
        let provider_host = Box::new(MockCapSet { name: "provider".to_string() });
        let provider_cap = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=extract;out="media:string""#).unwrap(),
            title: "Provider Text Extractor".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "extract".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: Some(CapOutput::new(MEDIA_STRING, "output")),
            metadata_json: None,
            registered_by: None,
        };
        provider_registry.register_cap_set(
            "provider".to_string(),
            provider_host,
            vec![provider_cap]
        ).unwrap();

        // Plugin: str -> obj
        let plugin_host = Box::new(MockCapSet { name: "plugin".to_string() });
        let plugin_cap = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:string";op=parse;out="media:object""#).unwrap(),
            title: "Plugin JSON Parser".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "parse".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };
        plugin_registry.register_cap_set(
            "plugin".to_string(),
            plugin_host,
            vec![plugin_cap]
        ).unwrap();

        let mut cube = CapCube::new(media_registry.clone());
        cube.add_registry("providers".to_string(), Arc::new(RwLock::new(provider_registry)));
        cube.add_registry("plugins".to_string(), Arc::new(RwLock::new(plugin_registry)));

        // Build graph
        let graph = cube.graph().unwrap();

        // Check nodes
        assert!(graph.get_nodes().contains("media:binary"));
        assert!(graph.get_nodes().contains("media:string"));
        assert!(graph.get_nodes().contains("media:object"));

        // Check edges
        assert_eq!(graph.get_edges().len(), 2);

        // Check conversion paths
        assert!(graph.can_convert("media:binary", "media:string"));
        assert!(graph.can_convert("media:string", "media:object"));
        assert!(graph.can_convert("media:binary", "media:object")); // Through intermediate

        // Find path from binary to obj
        let path = graph.find_path("media:binary", "media:object").unwrap();
        assert_eq!(path.len(), 2);

        // Check registry names in edges
        let provider_edges: Vec<_> = graph.get_edges().iter()
            .filter(|e| e.registry_name == "providers")
            .collect();
        assert_eq!(provider_edges.len(), 1);

        let plugin_edges: Vec<_> = graph.get_edges().iter()
            .filter(|e| e.registry_name == "plugins")
            .collect();
        assert_eq!(plugin_edges.len(), 1);
    }

    // TEST134: Test CapGraph stats provides counts of nodes and edges
    #[test]
    fn test_cap_graph_stats() {
        let mut graph = CapGraph::new();

        let cap1 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:binary";op=a;out="media:string""#).unwrap(),
            title: "Cap 1".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "a".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        let cap2 = Cap {
            urn: CapUrn::from_string(r#"cap:in="media:string";op=b;out="media:object""#).unwrap(),
            title: "Cap 2".to_string(),
            cap_description: None,
            metadata: HashMap::new(),
            command: "b".to_string(),
            media_specs: Vec::new(),
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };

        graph.add_cap(&cap1, "registry");
        graph.add_cap(&cap2, "registry");

        let stats = graph.stats();
        assert_eq!(stats.node_count, 3); // binary, str, obj
        assert_eq!(stats.edge_count, 2);
        assert_eq!(stats.input_spec_count, 2); // binary, str
        assert_eq!(stats.output_spec_count, 2); // str, obj
    }
}