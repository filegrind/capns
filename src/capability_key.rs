//! Flat Tag-Based Capability Identifier System
//!
//! This module provides a flat, tag-based capability identifier system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional capability classification.

use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A capability identifier using flat, ordered tags
///
/// Examples:
/// - `action=generate;format=pdf;output=binary;target=thumbnail;type=document`
/// - `action=extract;target=metadata;type=document`
/// - `action=analysis;format=en;type=inference`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapabilityKey {
    /// The tags that define this capability, stored in sorted order for canonical representation
    pub tags: BTreeMap<String, String>,
}

impl CapabilityKey {
    /// Create a new capability identifier from tags
    pub fn new(tags: BTreeMap<String, String>) -> Self {
        Self { tags }
    }

    /// Create an empty capability identifier
    pub fn empty() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    /// Create a capability identifier from a string representation
    ///
    /// Format: `key1=value1;key2=value2;...`
    /// Tags are automatically sorted alphabetically for canonical form
    pub fn from_string(s: &str) -> Result<Self, CapabilityKeyError> {
        if s.is_empty() {
            return Err(CapabilityKeyError::Empty);
        }

        let mut tags = BTreeMap::new();

        for tag_str in s.split(';') {
            let tag_str = tag_str.trim();
            if tag_str.is_empty() {
                continue;
            }

            let parts: Vec<&str> = tag_str.split('=').collect();
            if parts.len() != 2 {
                return Err(CapabilityKeyError::InvalidTagFormat(tag_str.to_string()));
            }

            let key = parts[0].trim();
            let value = parts[1].trim();

            if key.is_empty() || value.is_empty() {
                return Err(CapabilityKeyError::EmptyTagComponent(tag_str.to_string()));
            }

            // Validate key and value characters
            if !Self::is_valid_tag_component(key) || !Self::is_valid_tag_component(value) {
                return Err(CapabilityKeyError::InvalidCharacter(tag_str.to_string()));
            }

            tags.insert(key.to_string(), value.to_string());
        }

        if tags.is_empty() {
            return Err(CapabilityKeyError::Empty);
        }

        Ok(Self { tags })
    }

    /// Validate that a tag component contains only allowed characters
    fn is_valid_tag_component(s: &str) -> bool {
        s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '*')
    }

    /// Get the canonical string representation of this capability identifier
    ///
    /// Tags are sorted alphabetically for consistent representation
    pub fn to_string(&self) -> String {
        self.tags
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(";")
    }

    /// Get a specific tag value
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }

    /// Check if this capability has a specific tag with a specific value
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        self.tags.get(key).map_or(false, |v| v == value)
    }

    /// Add or update a tag
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Remove a tag
    pub fn without_tag(mut self, key: &str) -> Self {
        self.tags.remove(key);
        self
    }

    /// Check if this capability matches another based on tag compatibility
    ///
    /// A capability matches a request if:
    /// - For each tag in the request: capability has same value, wildcard (*), or missing tag
    /// - For each tag in the capability: if request is missing that tag, that's fine (capability is more specific)
    /// Missing tags are treated as wildcards (less specific, can handle any value).
    pub fn matches(&self, request: &CapabilityKey) -> bool {
        // Check all tags that the request specifies
        for (request_key, request_value) in &request.tags {
            match self.tags.get(request_key) {
                Some(cap_value) => {
                    if cap_value == "*" {
                        // Capability has wildcard - can handle any value
                        continue;
                    }
                    if request_value == "*" {
                        // Request accepts any value - capability's specific value matches
                        continue;
                    }
                    if cap_value != request_value {
                        // Capability has specific value that doesn't match request's specific value
                        return false;
                    }
                }
                None => {
                    // Missing tag in capability is treated as wildcard - can handle any value
                    continue;
                }
            }
        }
        
        // If capability has additional specific tags that request doesn't specify, that's fine
        // The capability is just more specific than needed
        true
    }

    /// Check if this capability can handle a request
    ///
    /// This is used when a request comes in with a capability identifier
    /// and we need to see if this capability can fulfill it
    pub fn can_handle(&self, request: &CapabilityKey) -> bool {
        self.matches(request)
    }

    /// Calculate specificity score for capability matching
    ///
    /// More specific capabilities have higher scores and are preferred
    pub fn specificity(&self) -> usize {
        // Count non-wildcard tags
        self.tags.values().filter(|v| v.as_str() != "*").count()
    }

    /// Check if this capability is more specific than another
    pub fn is_more_specific_than(&self, other: &CapabilityKey) -> bool {
        // First check if they're compatible
        if !self.is_compatible_with(other) {
            return false;
        }
        
        self.specificity() > other.specificity()
    }

    /// Check if this capability is compatible with another
    ///
    /// Two capabilities are compatible if they can potentially match
    /// the same types of requests (considering wildcards and missing tags as wildcards)
    pub fn is_compatible_with(&self, other: &CapabilityKey) -> bool {
        // Get all unique tag keys from both capabilities
        let mut all_keys = self.tags.keys().cloned().collect::<std::collections::HashSet<_>>();
        all_keys.extend(other.tags.keys().cloned());

        for key in all_keys {
            match (self.tags.get(&key), other.tags.get(&key)) {
                (Some(v1), Some(v2)) => {
                    // Both have the tag - they must match or one must be wildcard
                    if v1 != "*" && v2 != "*" && v1 != v2 {
                        return false;
                    }
                }
                (Some(_), None) | (None, Some(_)) => {
                    // One has the tag, the other doesn't - missing tag is wildcard, so compatible
                    continue;
                }
                (None, None) => {
                    // Neither has the tag - shouldn't happen in this loop
                    continue;
                }
            }
        }

        true
    }

    /// Get the type of this capability (convenience method)
    pub fn capability_type(&self) -> Option<&String> {
        self.get_tag("type")
    }

    /// Get the action of this capability (convenience method)
    pub fn action(&self) -> Option<&String> {
        self.get_tag("action")
    }

    /// Get the target of this capability (convenience method)
    pub fn target(&self) -> Option<&String> {
        self.get_tag("target")
    }

    /// Get the format of this capability (convenience method)
    pub fn format(&self) -> Option<&String> {
        self.get_tag("format")
    }

    /// Get the output type of this capability (convenience method)
    pub fn output(&self) -> Option<&String> {
        self.get_tag("output")
    }

    /// Check if this capability produces binary output
    pub fn is_binary(&self) -> bool {
        self.has_tag("output", "binary")
    }

    /// Create a wildcard version by replacing specific values with wildcards
    pub fn with_wildcard_tag(mut self, key: &str) -> Self {
        if self.tags.contains_key(key) {
            self.tags.insert(key.to_string(), "*".to_string());
        }
        self
    }

    /// Create a subset capability with only specified tags
    pub fn subset(&self, keys: &[&str]) -> Self {
        let mut tags = BTreeMap::new();
        for &key in keys {
            if let Some(value) = self.tags.get(key) {
                tags.insert(key.to_string(), value.clone());
            }
        }
        Self { tags }
    }

    /// Merge with another capability (other takes precedence for conflicts)
    pub fn merge(&self, other: &CapabilityKey) -> Self {
        let mut tags = self.tags.clone();
        for (key, value) in &other.tags {
            tags.insert(key.clone(), value.clone());
        }
        Self { tags }
    }
}

/// Errors that can occur when parsing capability identifiers
#[derive(Debug, Clone, PartialEq)]
pub enum CapabilityKeyError {
    Empty,
    InvalidTagFormat(String),
    EmptyTagComponent(String),
    InvalidCharacter(String),
}

impl fmt::Display for CapabilityKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapabilityKeyError::Empty => {
                write!(f, "Capability identifier cannot be empty")
            }
            CapabilityKeyError::InvalidTagFormat(tag) => {
                write!(f, "Invalid tag format (must be key=value): {}", tag)
            }
            CapabilityKeyError::EmptyTagComponent(tag) => {
                write!(f, "Tag key or value cannot be empty: {}", tag)
            }
            CapabilityKeyError::InvalidCharacter(tag) => {
                write!(f, "Invalid character in tag (use alphanumeric, _, -): {}", tag)
            }
        }
    }
}

impl std::error::Error for CapabilityKeyError {}

impl FromStr for CapabilityKey {
    type Err = CapabilityKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CapabilityKey::from_string(s)
    }
}

impl fmt::Display for CapabilityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Serde serialization support
impl Serialize for CapabilityKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CapabilityKey {
    fn deserialize<D>(deserializer: D) -> Result<CapabilityKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CapabilityKey::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// Capability matching and selection utilities
pub struct CapabilityMatcher;

impl CapabilityMatcher {
    /// Find the most specific capability that can handle a request
    pub fn find_best_match<'a>(
        capabilities: &'a [CapabilityKey],
        request: &CapabilityKey,
    ) -> Option<&'a CapabilityKey> {
        capabilities
            .iter()
            .filter(|cap| cap.can_handle(request))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all capabilities that can handle a request, sorted by specificity
    pub fn find_all_matches<'a>(
        capabilities: &'a [CapabilityKey],
        request: &CapabilityKey,
    ) -> Vec<&'a CapabilityKey> {
        let mut matches: Vec<&CapabilityKey> = capabilities
            .iter()
            .filter(|cap| cap.can_handle(request))
            .collect();
        
        // Sort by specificity (most specific first)
        matches.sort_by_key(|cap| std::cmp::Reverse(cap.specificity()));
        matches
    }

    /// Check if two capability sets are compatible
    pub fn are_compatible(caps1: &[CapabilityKey], caps2: &[CapabilityKey]) -> bool {
        caps1
            .iter()
            .any(|c1| caps2.iter().any(|c2| c1.is_compatible_with(c2)))
    }
}

/// Builder for creating capability keys fluently
pub struct CapabilityKeyBuilder {
    tags: BTreeMap<String, String>,
}

impl CapabilityKeyBuilder {
    pub fn new() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    pub fn type_tag(self, value: &str) -> Self {
        self.tag("type", value)
    }

    pub fn action(self, value: &str) -> Self {
        self.tag("action", value)
    }

    pub fn target(self, value: &str) -> Self {
        self.tag("target", value)
    }

    pub fn format(self, value: &str) -> Self {
        self.tag("format", value)
    }

    pub fn output(self, value: &str) -> Self {
        self.tag("output", value)
    }

    pub fn binary_output(self) -> Self {
        self.output("binary")
    }

    pub fn json_output(self) -> Self {
        self.output("json")
    }

    pub fn build(self) -> Result<CapabilityKey, CapabilityKeyError> {
        if self.tags.is_empty() {
            return Err(CapabilityKeyError::Empty);
        }
        Ok(CapabilityKey::new(self.tags))
    }
}

impl Default for CapabilityKeyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_key_creation() {
        let cap = CapabilityKey::from_string("action=generate;format=pdf;target=thumbnail;type=document").unwrap();
        assert_eq!(cap.get_tag("type"), Some(&"document".to_string()));
        assert_eq!(cap.get_tag("action"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("format"), Some(&"pdf".to_string()));
    }

    #[test]
    fn test_canonical_string_format() {
        let cap = CapabilityKey::from_string("type=document;action=generate;target=thumbnail;format=pdf").unwrap();
        // Should be sorted alphabetically
        assert_eq!(cap.to_string(), "action=generate;format=pdf;target=thumbnail;type=document");
    }

    #[test]
    fn test_tag_matching() {
        let cap = CapabilityKey::from_string("action=generate;format=pdf;target=thumbnail;type=document").unwrap();
        
        // Exact match
        let request1 = CapabilityKey::from_string("action=generate;format=pdf;target=thumbnail;type=document").unwrap();
        assert!(cap.matches(&request1));
        
        // Subset match
        let request2 = CapabilityKey::from_string("type=document;action=generate").unwrap();
        assert!(cap.matches(&request2));
        
        // Wildcard request should match specific capability  
        let request3 = CapabilityKey::from_string("type=document;format=*").unwrap();
        assert!(cap.matches(&request3)); // Capability has format=pdf, request accepts any format
        
        // No match - conflicting value
        let request4 = CapabilityKey::from_string("type=image").unwrap();
        assert!(!cap.matches(&request4));
    }

    #[test]
    fn test_missing_tag_handling() {
        let cap = CapabilityKey::from_string("type=document;action=generate").unwrap();
        
        // Request with tag should match capability without tag (treated as wildcard)
        let request1 = CapabilityKey::from_string("type=document;format=pdf").unwrap();
        assert!(cap.matches(&request1)); // cap missing format tag = wildcard, can handle any format
        
        // But capability with extra tags can match subset requests
        let cap2 = CapabilityKey::from_string("type=document;action=generate;format=pdf").unwrap();
        let request2 = CapabilityKey::from_string("type=document;action=generate").unwrap();
        assert!(cap2.matches(&request2));
    }

    #[test]
    fn test_specificity() {
        let cap1 = CapabilityKey::from_string("type=document").unwrap();
        let cap2 = CapabilityKey::from_string("type=document;action=generate").unwrap();
        let cap3 = CapabilityKey::from_string("type=document;action=*;format=pdf").unwrap();
        
        assert_eq!(cap1.specificity(), 1);
        assert_eq!(cap2.specificity(), 2);
        assert_eq!(cap3.specificity(), 2); // wildcard doesn't count
        
        assert!(cap2.is_more_specific_than(&cap1));
    }

    #[test]
    fn test_builder() {
        let cap = CapabilityKeyBuilder::new()
            .type_tag("document")
            .action("generate")
            .target("thumbnail")
            .format("pdf")
            .binary_output()
            .build()
            .unwrap();
        
        assert_eq!(cap.capability_type(), Some(&"document".to_string()));
        assert_eq!(cap.action(), Some(&"generate".to_string()));
        assert!(cap.is_binary());
    }

    #[test]
    fn test_compatibility() {
        let cap1 = CapabilityKey::from_string("type=document;action=generate;format=pdf").unwrap();
        let cap2 = CapabilityKey::from_string("type=document;action=generate;format=*").unwrap();
        let cap3 = CapabilityKey::from_string("type=image;action=generate").unwrap();
        
        assert!(cap1.is_compatible_with(&cap2));
        assert!(cap2.is_compatible_with(&cap1));
        assert!(!cap1.is_compatible_with(&cap3));
        
        // Missing tags are treated as wildcards for compatibility
        let cap4 = CapabilityKey::from_string("type=document;action=generate").unwrap();
        assert!(cap1.is_compatible_with(&cap4));
        assert!(cap4.is_compatible_with(&cap1));
    }

    #[test]
    fn test_best_match() {
        let capabilities = vec![
            CapabilityKey::from_string("type=document").unwrap(),
            CapabilityKey::from_string("type=document;action=generate").unwrap(),
            CapabilityKey::from_string("type=document;action=generate;format=pdf").unwrap(),
        ];
        
        let request = CapabilityKey::from_string("type=document;action=generate").unwrap();
        let best = CapabilityMatcher::find_best_match(&capabilities, &request).unwrap();
        
        // Most specific capability that can handle the request
        assert_eq!(best.to_string(), "action=generate;format=pdf;type=document");
    }

    #[test]
    fn test_merge_and_subset() {
        let cap1 = CapabilityKey::from_string("type=document;action=generate").unwrap();
        let cap2 = CapabilityKey::from_string("format=pdf;output=binary").unwrap();
        
        let merged = cap1.merge(&cap2);
        assert_eq!(merged.to_string(), "action=generate;format=pdf;output=binary;type=document");
        
        let subset = merged.subset(&["type", "format"]);
        assert_eq!(subset.to_string(), "format=pdf;type=document");
    }

    #[test]
    fn test_wildcard_tag() {
        let cap = CapabilityKey::from_string("type=document;format=pdf").unwrap();
        let wildcarded = cap.clone().with_wildcard_tag("format");
        
        assert_eq!(wildcarded.to_string(), "format=*;type=document");
        
        // Test that wildcarded capability can match more requests
        let request = CapabilityKey::from_string("type=document;format=jpg").unwrap();
        assert!(!cap.matches(&request));
        assert!(wildcarded.matches(&CapabilityKey::from_string("type=document;format=*").unwrap()));
    }
}