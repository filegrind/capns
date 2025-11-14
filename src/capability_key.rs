//! Formal Capability Identifier System
//!
//! This module provides a reference implementation for hierarchical capability identifiers
//! with wildcard support, compatibility checking, and specificity comparison.

use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::fmt;
use std::str::FromStr;

/// A formal capability identifier with hierarchical naming and wildcard support
/// 
/// Examples:
/// - `file_handling:thumbnail_generation:pdf`
/// - `file_handling:thumbnail_generation:*`
/// - `file_handling:*`
/// - `*`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapabilityKey {
    /// The hierarchical components of the capability identifier
    pub components: Vec<String>,
}

impl CapabilityKey {
    /// Create a new capability identifier from components
    pub fn new(components: Vec<String>) -> Self {
        Self { components }
    }

    /// Create a capability identifier from a string representation
    pub fn from_string(s: &str) -> Result<Self, CapabilityKeyError> {
        if s.is_empty() {
            return Err(CapabilityKeyError::Empty);
        }

        let components: Vec<String> = s.split(':')
            .map(|c| c.trim().to_string())
            .collect();

        // Validate components
        for component in &components {
            if component.is_empty() {
                return Err(CapabilityKeyError::EmptyComponent);
            }
            if component.contains(char::is_whitespace) {
                return Err(CapabilityKeyError::InvalidCharacter(component.clone()));
            }
        }

        Ok(Self { components })
    }

    /// Get the string representation of this capability identifier
    pub fn to_string(&self) -> String {
        self.components.join(":")
    }

    /// Check if this capability is a wildcard (ends with "*")
    pub fn is_wildcard(&self) -> bool {
        self.components.last().map_or(false, |c| c == "*")
    }

    /// Check if this capability is fully specified (no wildcards)
    pub fn is_fully_specified(&self) -> bool {
        !self.components.iter().any(|c| c == "*")
    }

    /// Get the specificity level (number of non-wildcard components)
    pub fn specificity(&self) -> usize {
        self.components.iter().take_while(|&c| c != "*").count()
    }

    /// Check if this capability produces binary output
    /// 
    /// Binary capabilities are identified by the "bin:" prefix convention
    pub fn is_binary(&self) -> bool {
        self.components.first().map_or(false, |c| c == "bin")
    }

    /// Check if this capability is compatible with another
    /// 
    /// Two capabilities are compatible if:
    /// 1. They are identical
    /// 2. One is a prefix of the other with wildcard matching
    /// 3. Both have wildcards that can match
    pub fn is_compatible_with(&self, other: &CapabilityKey) -> bool {
        let max_len = self.components.len().max(other.components.len());
        
        for i in 0..max_len {
            let self_component = self.components.get(i);
            let other_component = other.components.get(i);
            
            match (self_component, other_component) {
                // Both have components at this level
                (Some(self_comp), Some(other_comp)) => {
                    if self_comp == "*" || other_comp == "*" {
                        // Wildcard matches anything
                        continue;
                    } else if self_comp == other_comp {
                        // Exact match
                        continue;
                    } else {
                        // Different non-wildcard components - not compatible
                        return false;
                    }
                }
                // One has more components than the other
                (Some(comp), None) | (None, Some(comp)) => {
                    // Compatible only if the shorter one ended with a wildcard
                    if comp == "*" {
                        return true;
                    }
                    // Check if the shorter capability had a trailing wildcard
                    let shorter = if self_component.is_some() { other } else { self };
                    return shorter.is_wildcard();
                }
                // Both are None (shouldn't happen in this loop)
                (None, None) => break,
            }
        }
        
        true
    }

    /// Check if this capability is more specific than another compatible capability
    /// 
    /// Returns true if both are compatible and this capability is more specific
    pub fn is_more_specific_than(&self, other: &CapabilityKey) -> bool {
        if !self.is_compatible_with(other) {
            return false;
        }
        
        self.specificity() > other.specificity()
    }

    /// Check if this capability matches a request pattern
    /// 
    /// This is used when a request comes in with a capability identifier
    /// and we need to see if this capability can handle it
    pub fn can_handle(&self, request: &CapabilityKey) -> bool {
        // A capability can handle a request if the request is compatible
        // and the capability is at least as specific as needed
        if !self.is_compatible_with(request) {
            return false;
        }

        // If request is fully specified, we must match exactly or be more general
        if request.is_fully_specified() {
            return self.is_compatible_with(request);
        }

        // If request has wildcards, we can handle it if we're compatible
        true
    }

    /// Get the parent capability identifier (remove last component)
    pub fn parent(&self) -> Option<CapabilityKey> {
        if self.components.len() <= 1 {
            return None;
        }
        
        let mut parent_components = self.components.clone();
        parent_components.pop();
        Some(CapabilityKey::new(parent_components))
    }

    /// Get all ancestor capability identifiers (all parents up to root)
    pub fn ancestors(&self) -> Vec<CapabilityKey> {
        let mut ancestors = Vec::new();
        let mut current = self.parent();
        
        while let Some(parent) = current {
            ancestors.push(parent.clone());
            current = parent.parent();
        }
        
        ancestors
    }

    /// Check if this capability is a child of another
    pub fn is_child_of(&self, parent: &CapabilityKey) -> bool {
        if self.components.len() != parent.components.len() + 1 {
            return false;
        }
        
        for (i, parent_comp) in parent.components.iter().enumerate() {
            if parent_comp != "*" && &self.components[i] != parent_comp {
                return false;
            }
        }
        
        true
    }

    /// Create a wildcard version of this capability at a specific level
    pub fn wildcard_at_level(&self, level: usize) -> CapabilityKey {
        let mut components = self.components.clone();
        
        // Truncate to the specified level and add wildcard
        components.truncate(level);
        components.push("*".to_string());
        
        CapabilityKey::new(components)
    }
}

/// Errors that can occur when parsing capability identifiers
#[derive(Debug, Clone, PartialEq)]
pub enum CapabilityKeyError {
    Empty,
    EmptyComponent,
    InvalidCharacter(String),
}

impl fmt::Display for CapabilityKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapabilityKeyError::Empty => write!(f, "Capability identifier cannot be empty"),
            CapabilityKeyError::EmptyComponent => write!(f, "Capability identifier cannot have empty components"),
            CapabilityKeyError::InvalidCharacter(comp) => write!(f, "Invalid character in component: {}", comp),
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
        caps1.iter().any(|c1| {
            caps2.iter().any(|c2| c1.is_compatible_with(c2))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_key_creation() {
        let cap = CapabilityKey::from_string("file_handling:thumbnail_generation:pdf").unwrap();
        assert_eq!(cap.components, vec!["file_handling", "thumbnail_generation", "pdf"]);
        assert_eq!(cap.to_string(), "file_handling:thumbnail_generation:pdf");
    }

    #[test]
    fn test_wildcard_detection() {
        let wildcard = CapabilityKey::from_string("file_handling:*").unwrap();
        assert!(wildcard.is_wildcard());
        assert!(!wildcard.is_fully_specified());

        let specific = CapabilityKey::from_string("file_handling:pdf").unwrap();
        assert!(!specific.is_wildcard());
        assert!(specific.is_fully_specified());
    }

    #[test]
    fn test_specificity() {
        let cap1 = CapabilityKey::from_string("file_handling").unwrap();
        let cap2 = CapabilityKey::from_string("file_handling:thumbnail").unwrap();
        let cap3 = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let cap4 = CapabilityKey::from_string("file_handling:*").unwrap();

        assert_eq!(cap1.specificity(), 1);
        assert_eq!(cap2.specificity(), 2);
        assert_eq!(cap3.specificity(), 3);
        assert_eq!(cap4.specificity(), 1);
    }

    #[test]
    fn test_compatibility() {
        let specific = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let wildcard1 = CapabilityKey::from_string("file_handling:thumbnail:*").unwrap();
        let wildcard2 = CapabilityKey::from_string("file_handling:*").unwrap();
        let unrelated = CapabilityKey::from_string("data_processing:transform").unwrap();

        assert!(specific.is_compatible_with(&wildcard1));
        assert!(specific.is_compatible_with(&wildcard2));
        assert!(wildcard1.is_compatible_with(&specific));
        assert!(wildcard2.is_compatible_with(&specific));
        assert!(!specific.is_compatible_with(&unrelated));
    }

    #[test]
    fn test_can_handle() {
        let capability = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let request1 = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let request2 = CapabilityKey::from_string("file_handling:thumbnail:*").unwrap();
        let request3 = CapabilityKey::from_string("file_handling:*").unwrap();
        let request4 = CapabilityKey::from_string("data_processing:*").unwrap();

        assert!(capability.can_handle(&request1));
        assert!(capability.can_handle(&request2));
        assert!(capability.can_handle(&request3));
        assert!(!capability.can_handle(&request4));
    }

    #[test]
    fn test_best_match() {
        let capabilities = vec![
            CapabilityKey::from_string("file_handling:*").unwrap(),
            CapabilityKey::from_string("file_handling:thumbnail:*").unwrap(),
            CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap(),
        ];

        let request = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let best = CapabilityMatcher::find_best_match(&capabilities, &request).unwrap();
        
        assert_eq!(best.to_string(), "file_handling:thumbnail:pdf");
    }

    #[test]
    fn test_parent_child() {
        let child = CapabilityKey::from_string("file_handling:thumbnail:pdf").unwrap();
        let parent = CapabilityKey::from_string("file_handling:thumbnail").unwrap();
        
        assert!(child.is_child_of(&parent));
        assert_eq!(child.parent().unwrap(), parent);
    }
}