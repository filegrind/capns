//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap identifier system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.

use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A cap identifier using flat, ordered tags
///
/// Examples:
/// - `action=generate;ext=pdf;output=binary;target=thumbnail;`
/// - `action=extract;target=metadata;`
/// - `action=analysis;format=en;type=constrained`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapCard {
    /// The tags that define this cap, stored in sorted order for canonical representation
    pub tags: BTreeMap<String, String>,
}

impl CapCard {
    /// Create a new cap identifier from tags
    pub fn new(tags: BTreeMap<String, String>) -> Self {
        Self { tags }
    }

    /// Create an empty cap identifier
    pub fn empty() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    /// Create a cap identifier from a string representation
    ///
    /// Format: `key1=value1;key2=value2;...`
    /// Tags are automatically sorted alphabetically for canonical form
    pub fn from_string(s: &str) -> Result<Self, CapCardError> {
        if s.is_empty() {
            return Err(CapCardError::Empty);
        }

        let mut tags = BTreeMap::new();

        for tag_str in s.split(';') {
            let tag_str = tag_str.trim();
            if tag_str.is_empty() {
                continue;
            }

            let parts: Vec<&str> = tag_str.split('=').collect();
            if parts.len() != 2 {
                return Err(CapCardError::InvalidTagFormat(tag_str.to_string()));
            }

            let key = parts[0].trim();
            let value = parts[1].trim();

            if key.is_empty() || value.is_empty() {
                return Err(CapCardError::EmptyTagComponent(tag_str.to_string()));
            }

            // Validate key and value characters
            if !Self::is_valid_tag_component(key) || !Self::is_valid_tag_component(value) {
                return Err(CapCardError::InvalidCharacter(tag_str.to_string()));
            }

            tags.insert(key.to_string(), value.to_string());
        }

        if tags.is_empty() {
            return Err(CapCardError::Empty);
        }

        Ok(Self { tags })
    }

    /// Validate that a tag component contains only allowed characters
    fn is_valid_tag_component(s: &str) -> bool {
        s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '*')
    }

    /// Get the canonical string representation of this cap identifier
    ///
    /// Tags are already sorted alphabetically due to BTreeMap
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

    /// Check if this cap has a specific tag with a specific value
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

    /// Check if this cap matches another based on tag compatibility
    ///
    /// A cap matches a request if:
    /// - For each tag in the request: cap has same value, wildcard (*), or missing tag
    /// - For each tag in the cap: if request is missing that tag, that's fine (cap is more specific)
    /// Missing tags are treated as wildcards (less specific, can handle any value).
    pub fn matches(&self, request: &CapCard) -> bool {
        // Check all tags that the request specifies
        for (request_key, request_value) in &request.tags {
            match self.tags.get(request_key) {
                Some(cap_value) => {
                    if cap_value == "*" {
                        // Cap has wildcard - can handle any value
                        continue;
                    }
                    if request_value == "*" {
                        // Request accepts any value - cap's specific value matches
                        continue;
                    }
                    if cap_value != request_value {
                        // Cap has specific value that doesn't match request's specific value
                        return false;
                    }
                }
                None => {
                    // Missing tag in cap is treated as wildcard - can handle any value
                    continue;
                }
            }
        }
        
        // If cap has additional specific tags that request doesn't specify, that's fine
        // The cap is just more specific than needed
        true
    }

    /// Check if this cap can handle a request
    ///
    /// This is used when a request comes in with a cap identifier
    /// and we need to see if this cap can fulfill it
    pub fn can_handle(&self, request: &CapCard) -> bool {
        self.matches(request)
    }

    /// Calculate specificity score for cap matching
    ///
    /// More specific caps have higher scores and are preferred
    pub fn specificity(&self) -> usize {
        // Count non-wildcard tags
        self.tags.values().filter(|v| v.as_str() != "*").count()
    }

    /// Check if this cap is more specific than another
    pub fn is_more_specific_than(&self, other: &CapCard) -> bool {
        // First check if they're compatible
        if !self.is_compatible_with(other) {
            return false;
        }
        
        self.specificity() > other.specificity()
    }

    /// Check if this cap is compatible with another
    ///
    /// Two caps are compatible if they can potentially match
    /// the same types of requests (considering wildcards and missing tags as wildcards)
    pub fn is_compatible_with(&self, other: &CapCard) -> bool {
        // Get all unique tag keys from both caps
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

    /// Create a wildcard version by replacing specific values with wildcards
    pub fn with_wildcard_tag(mut self, key: &str) -> Self {
        if self.tags.contains_key(key) {
            self.tags.insert(key.to_string(), "*".to_string());
        }
        self
    }

    /// Create a subset cap with only specified tags
    pub fn subset(&self, keys: &[&str]) -> Self {
        let mut tags = BTreeMap::new();
        for &key in keys {
            if let Some(value) = self.tags.get(key) {
                tags.insert(key.to_string(), value.clone());
            }
        }
        Self { tags }
    }

    /// Merge with another cap (other takes precedence for conflicts)
    pub fn merge(&self, other: &CapCard) -> Self {
        let mut tags = self.tags.clone();
        for (key, value) in &other.tags {
            tags.insert(key.clone(), value.clone());
        }
        Self { tags }
    }
}

/// Errors that can occur when parsing cap identifiers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CapCardError {
    Empty,
    InvalidTagFormat(String),
    EmptyTagComponent(String),
    InvalidCharacter(String),
}

impl fmt::Display for CapCardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapCardError::Empty => {
                write!(f, "Cap identifier cannot be empty")
            }
            CapCardError::InvalidTagFormat(tag) => {
                write!(f, "Invalid tag format (must be key=value): {}", tag)
            }
            CapCardError::EmptyTagComponent(tag) => {
                write!(f, "Tag key or value cannot be empty: {}", tag)
            }
            CapCardError::InvalidCharacter(tag) => {
                write!(f, "Invalid character in tag (use alphanumeric, _, -): {}", tag)
            }
        }
    }
}

impl std::error::Error for CapCardError {}

impl FromStr for CapCard {
    type Err = CapCardError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CapCard::from_string(s)
    }
}

impl fmt::Display for CapCard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Serde serialization support
impl Serialize for CapCard {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CapCard {
    fn deserialize<D>(deserializer: D) -> Result<CapCard, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CapCard::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// Cap matching and selection utilities
pub struct CapMatcher;

impl CapMatcher {
    /// Find the most specific cap that can handle a request
    pub fn find_best_match<'a>(
        caps: &'a [CapCard],
        request: &CapCard,
    ) -> Option<&'a CapCard> {
        caps
            .iter()
            .filter(|cap| cap.can_handle(request))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all caps that can handle a request, sorted by specificity
    pub fn find_all_matches<'a>(
        caps: &'a [CapCard],
        request: &CapCard,
    ) -> Vec<&'a CapCard> {
        let mut matches: Vec<&CapCard> = caps
            .iter()
            .filter(|cap| cap.can_handle(request))
            .collect();
        
        // Sort by specificity (most specific first)
        matches.sort_by_key(|cap| std::cmp::Reverse(cap.specificity()));
        matches
    }

    /// Check if two cap sets are compatible
    pub fn are_compatible(caps1: &[CapCard], caps2: &[CapCard]) -> bool {
        caps1
            .iter()
            .any(|c1| caps2.iter().any(|c2| c1.is_compatible_with(c2)))
    }
}

/// Builder for creating cap cards fluently
pub struct CapCardBuilder {
    tags: BTreeMap<String, String>,
}

impl CapCardBuilder {
    pub fn new() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(self) -> Result<CapCard, CapCardError> {
        if self.tags.is_empty() {
            return Err(CapCardError::Empty);
        }
        Ok(CapCard::new(self.tags))
    }
}

impl Default for CapCardBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_card_creation() {
        let cap = CapCard::from_string("action=generate;ext=pdf;target=thumbnail;").unwrap();
        assert_eq!(cap.get_tag("action"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
    }

    #[test]
    fn test_canonical_string_format() {
        let cap = CapCard::from_string("action=generate;target=thumbnail;ext=pdf").unwrap();
        // Should be sorted alphabetically
        assert_eq!(cap.to_string(), "action=generate;ext=pdf;target=thumbnail");
    }

    #[test]
    fn test_tag_matching() {
        let cap = CapCard::from_string("action=generate;ext=pdf;target=thumbnail;").unwrap();
        
        // Exact match
        let request1 = CapCard::from_string("action=generate;ext=pdf;target=thumbnail;").unwrap();
        assert!(cap.matches(&request1));
        
        // Subset match
        let request2 = CapCard::from_string("action=generate").unwrap();
        assert!(cap.matches(&request2));
        
        // Wildcard request should match specific cap  
        let request3 = CapCard::from_string("format=*").unwrap();
        assert!(cap.matches(&request3)); // Cap has ext=pdf, request accepts any format
        
        // No match - conflicting value
        let request4 = CapCard::from_string("action=extract").unwrap(); // Different action should not match
        assert!(!cap.matches(&request4));
    }

    #[test]
    fn test_missing_tag_handling() {
        let cap = CapCard::from_string("action=generate").unwrap();
        
        // Request with tag should match cap without tag (treated as wildcard)
        let request1 = CapCard::from_string("ext=pdf").unwrap();
        assert!(cap.matches(&request1)); // cap missing format tag = wildcard, can handle any format
        
        // But cap with extra tags can match subset requests
        let cap2 = CapCard::from_string("action=generate;ext=pdf").unwrap();
        let request2 = CapCard::from_string("action=generate").unwrap();
        assert!(cap2.matches(&request2));
    }

    #[test]
    fn test_specificity() {
        let cap1 = CapCard::from_string("type=general").unwrap();
        let cap2 = CapCard::from_string("action=generate").unwrap();
        let cap3 = CapCard::from_string("action=*;ext=pdf").unwrap();
        
        assert_eq!(cap1.specificity(), 1);
        assert_eq!(cap2.specificity(), 1);
        assert_eq!(cap3.specificity(), 1); // wildcard doesn't count
        
        assert!(!cap2.is_more_specific_than(&cap1)); // Different tags, not compatible
    }

    #[test]
    fn test_builder() {
        let cap = CapCardBuilder::new()
            
            .tag("action", "generate")
            .tag("target", "thumbnail")
            .tag("ext", "pdf")
            .tag("output", "binary")
            .build()
            .unwrap();
        
        assert_eq!(cap.get_tag("action"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("output"), Some(&"binary".to_string()));
    }

    #[test]
    fn test_compatibility() {
        let cap1 = CapCard::from_string("action=generate;ext=pdf").unwrap();
        let cap2 = CapCard::from_string("action=generate;format=*").unwrap();
        let cap3 = CapCard::from_string("type=image;action=extract").unwrap();
        
        assert!(cap1.is_compatible_with(&cap2));
        assert!(cap2.is_compatible_with(&cap1));
        assert!(!cap1.is_compatible_with(&cap3));
        
        // Missing tags are treated as wildcards for compatibility
        let cap4 = CapCard::from_string("action=generate").unwrap();
        assert!(cap1.is_compatible_with(&cap4));
        assert!(cap4.is_compatible_with(&cap1));
    }

    #[test]
    fn test_best_match() {
        let caps = vec![
            CapCard::from_string("action=*").unwrap(),
            CapCard::from_string("action=generate").unwrap(),
            CapCard::from_string("action=generate;ext=pdf").unwrap(),
        ];
        
        let request = CapCard::from_string("action=generate").unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();
        
        // Most specific cap that can handle the request
        assert_eq!(best.to_string(), "action=generate;ext=pdf");
    }

    #[test]
    fn test_merge_and_subset() {
        let cap1 = CapCard::from_string("action=generate").unwrap();
        let cap2 = CapCard::from_string("ext=pdf;output=binary").unwrap();
        
        let merged = cap1.merge(&cap2);
        assert_eq!(merged.to_string(), "action=generate;ext=pdf;output=binary");
        
        let subset = merged.subset(&["type", "ext"]);
        assert_eq!(subset.to_string(), "ext=pdf");
    }

    #[test]
    fn test_wildcard_tag() {
        let cap = CapCard::from_string("ext=pdf").unwrap();
        let wildcarded = cap.clone().with_wildcard_tag("ext");
        
        assert_eq!(wildcarded.to_string(), "format=*");
        
        // Test that wildcarded cap can match more requests
        let request = CapCard::from_string("format=jpg").unwrap();
        assert!(!cap.matches(&request));
        assert!(wildcarded.matches(&CapCard::from_string("format=*").unwrap()));
    }
}