//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap URN system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.

use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A cap URN using flat, ordered tags
///
/// Examples:
/// - `action=generate;ext=pdf;output=binary;target=thumbnail;`
/// - `action=extract;target=metadata;`
/// - `action=analysis;format=en;type=constrained`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapUrn {
    /// The tags that define this cap, stored in sorted order for canonical representation
    pub tags: BTreeMap<String, String>,
}

impl CapUrn {
    /// Create a new cap URN from tags
    /// All keys and values are normalized to lowercase for case-insensitive matching
    pub fn new(tags: BTreeMap<String, String>) -> Self {
        let normalized_tags = tags
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
            .collect();
        Self { tags: normalized_tags }
    }

    /// Create an empty cap URN
    pub fn empty() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    /// Create a cap URN from a string representation
    ///
    /// Format: `cap:key1=value1;key2=value2;...`
    /// The "cap:" prefix is mandatory
    /// Trailing semicolons are optional and ignored
    /// Tags are automatically sorted alphabetically for canonical form
    /// All input is normalized to lowercase for case-insensitive matching
    pub fn from_string(s: &str) -> Result<Self, CapUrnError> {
        if s.is_empty() {
            return Err(CapUrnError::Empty);
        }

        // Normalize to lowercase for case-insensitive handling
        let s = s.to_lowercase();

        // Ensure "cap:" prefix is present
        if !s.starts_with("cap:") {
            return Err(CapUrnError::MissingCapPrefix);
        }

        // Remove the "cap:" prefix
        let tags_part = &s[4..];

        let mut tags = BTreeMap::new();

        // Remove trailing semicolon if present
        let normalized_tags_part = tags_part.trim_end_matches(';');
        
        // Handle empty cap URN (cap: with no tags)
        if normalized_tags_part.is_empty() {
            return Ok(Self { tags });
        }
        
        for tag_str in normalized_tags_part.split(';') {
            let tag_str = tag_str.trim();
            if tag_str.is_empty() {
                continue;
            }

            let parts: Vec<&str> = tag_str.split('=').collect();
            if parts.len() != 2 {
                return Err(CapUrnError::InvalidTagFormat(tag_str.to_string()));
            }

            let key = parts[0].trim();
            let value = parts[1].trim();

            if key.is_empty() || value.is_empty() {
                return Err(CapUrnError::EmptyTagComponent(tag_str.to_string()));
            }

            // Check for duplicate keys
            if tags.contains_key(key) {
                return Err(CapUrnError::DuplicateKey(key.to_string()));
            }

            // Validate key cannot be purely numeric
            if Self::is_purely_numeric(key) {
                return Err(CapUrnError::NumericKey(key.to_string()));
            }

            // Validate key and value characters
            if !Self::is_valid_tag_component(key, true) || !Self::is_valid_tag_component(value, false) {
                return Err(CapUrnError::InvalidCharacter(tag_str.to_string()));
            }

            tags.insert(key.to_string(), value.to_string());
        }

        Ok(Self { tags })
    }

    /// Validate that a tag component contains only allowed characters
    /// Allowed: alphanumeric, underscore, dash, slash, colon, dot, asterisk (asterisk only in values)
    fn is_valid_tag_component(s: &str, is_key: bool) -> bool {
        s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '/' || c == ':' || c == '.' || (!is_key && c == '*'))
    }
    
    /// Check if a string is purely numeric
    fn is_purely_numeric(s: &str) -> bool {
        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
    }

    /// Get the canonical string representation of this cap URN
    ///
    /// Always includes "cap:" prefix
    /// Tags are already sorted alphabetically due to BTreeMap
    /// No trailing semicolon in canonical form
    pub fn to_string(&self) -> String {
        let tags_str = self.tags
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(";");
        format!("cap:{}", tags_str)
    }

    /// Get a specific tag value
    /// Key is normalized to lowercase for case-insensitive lookup
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(&key.to_lowercase())
    }

    /// Check if this cap has a specific tag with a specific value
    /// Both key and value are normalized to lowercase for case-insensitive comparison
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        self.tags.get(&key.to_lowercase()).map_or(false, |v| v == &value.to_lowercase())
    }

    /// Add or update a tag
    /// Both key and value are normalized to lowercase
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key.to_lowercase(), value.to_lowercase());
        self
    }

    /// Remove a tag
    /// Key is normalized to lowercase for case-insensitive removal
    pub fn without_tag(mut self, key: &str) -> Self {
        self.tags.remove(&key.to_lowercase());
        self
    }

    /// Check if this cap matches another based on tag compatibility
    ///
    /// A cap matches a request if:
    /// - For each tag in the request: cap has same value, wildcard (*), or missing tag
    /// - For each tag in the cap: if request is missing that tag, that's fine (cap is more specific)
    /// Missing tags are treated as wildcards (less specific, can handle any value).
    pub fn matches(&self, request: &CapUrn) -> bool {
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

	pub fn matches_str(&self, request_str: &str) -> Result<bool, CapUrnError> {
		let request = CapUrn::from_string(request_str)?;
		Ok(self.matches(&request))
	}

    /// Check if this cap can handle a request
    ///
    /// This is used when a request comes in with a cap URN
    /// and we need to see if this cap can fulfill it
    pub fn can_handle(&self, request: &CapUrn) -> bool {
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
    pub fn is_more_specific_than(&self, other: &CapUrn) -> bool {
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
    pub fn is_compatible_with(&self, other: &CapUrn) -> bool {
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
    pub fn merge(&self, other: &CapUrn) -> Self {
        let mut tags = self.tags.clone();
        for (key, value) in &other.tags {
            tags.insert(key.clone(), value.clone());
        }
        Self { tags }
    }
}

/// Errors that can occur when parsing cap URNs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CapUrnError {
    Empty,
    MissingCapPrefix,
    InvalidTagFormat(String),
    EmptyTagComponent(String),
    InvalidCharacter(String),
    DuplicateKey(String),
    NumericKey(String),
}

impl fmt::Display for CapUrnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapUrnError::Empty => {
                write!(f, "Cap identifier cannot be empty")
            }
            CapUrnError::MissingCapPrefix => {
                write!(f, "Cap identifier must start with 'cap:'")
            }
            CapUrnError::InvalidTagFormat(tag) => {
                write!(f, "Invalid tag format (must be key=value): {}", tag)
            }
            CapUrnError::EmptyTagComponent(tag) => {
                write!(f, "Tag key or value cannot be empty: {}", tag)
            }
            CapUrnError::InvalidCharacter(tag) => {
                write!(f, "Invalid character in tag (use alphanumeric, _, -, /, :, ., * in values only): {}", tag)
            }
            CapUrnError::DuplicateKey(key) => {
                write!(f, "Duplicate tag key: {}", key)
            }
            CapUrnError::NumericKey(key) => {
                write!(f, "Tag key cannot be purely numeric: {}", key)
            }
        }
    }
}

impl std::error::Error for CapUrnError {}

impl FromStr for CapUrn {
    type Err = CapUrnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CapUrn::from_string(s)
    }
}

impl fmt::Display for CapUrn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Serde serialization support
impl Serialize for CapUrn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CapUrn {
    fn deserialize<D>(deserializer: D) -> Result<CapUrn, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CapUrn::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// Cap matching and selection utilities
pub struct CapMatcher;

impl CapMatcher {
    /// Find the most specific cap that can handle a request
    pub fn find_best_match<'a>(
        caps: &'a [CapUrn],
        request: &CapUrn,
    ) -> Option<&'a CapUrn> {
        caps
            .iter()
            .filter(|cap| cap.can_handle(request))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all caps that can handle a request, sorted by specificity
    pub fn find_all_matches<'a>(
        caps: &'a [CapUrn],
        request: &CapUrn,
    ) -> Vec<&'a CapUrn> {
        let mut matches: Vec<&CapUrn> = caps
            .iter()
            .filter(|cap| cap.can_handle(request))
            .collect();
        
        // Sort by specificity (most specific first)
        matches.sort_by_key(|cap| std::cmp::Reverse(cap.specificity()));
        matches
    }

    /// Check if two cap sets are compatible
    pub fn are_compatible(caps1: &[CapUrn], caps2: &[CapUrn]) -> bool {
        caps1
            .iter()
            .any(|c1| caps2.iter().any(|c2| c1.is_compatible_with(c2)))
    }
}

/// Builder for creating cap URNs fluently
pub struct CapUrnBuilder {
    tags: BTreeMap<String, String>,
}

impl CapUrnBuilder {
    pub fn new() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_lowercase(), value.to_lowercase());
        self
    }

    pub fn build(self) -> Result<CapUrn, CapUrnError> {
        if self.tags.is_empty() {
            return Err(CapUrnError::Empty);
        }
        Ok(CapUrn::new(self.tags))
    }
}

impl Default for CapUrnBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_urn_creation() {
        let cap = CapUrn::from_string("cap:action=generate;ext=pdf;target=thumbnail;").unwrap();
        assert_eq!(cap.get_tag("action"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
    }

    #[test]
    fn test_cap_urn_case_insensitive() {
        // Test that different casing produces the same URN
        let cap1 = CapUrn::from_string("cap:ACTION=Generate;EXT=PDF;Target=Thumbnail;").unwrap();
        let cap2 = CapUrn::from_string("cap:action=generate;ext=pdf;target=thumbnail;").unwrap();
        
        // Both should be normalized to lowercase
        assert_eq!(cap1.get_tag("action"), Some(&"generate".to_string()));
        assert_eq!(cap1.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(cap1.get_tag("target"), Some(&"thumbnail".to_string()));
        
        // URNs should be identical after normalization
        assert_eq!(cap1.to_string(), cap2.to_string());
        
        // PartialEq should work correctly - URNs with different case should be equal
        assert_eq!(cap1, cap2);
        
        // Case-insensitive tag lookup should work
        assert_eq!(cap1.get_tag("ACTION"), Some(&"generate".to_string()));
        assert_eq!(cap1.get_tag("Action"), Some(&"generate".to_string()));
        assert!(cap1.has_tag("ACTION", "Generate"));
        assert!(cap1.has_tag("action", "GENERATE"));
        
        // Matching should work case-insensitively
        assert!(cap1.matches(&cap2));
        assert!(cap2.matches(&cap1));
        assert!(cap1.matches_str("cap:Action=Generate;Ext=PDF;Target=Thumbnail").unwrap());
        assert!(cap1.matches_str("cap:action=generate;ext=pdf;target=thumbnail").unwrap());
    }

    #[test]
    fn test_cap_prefix_required() {
        // Missing cap: prefix should fail
        assert!(CapUrn::from_string("action=generate;ext=pdf").is_err());
        
        // Valid cap: prefix should work
        let cap = CapUrn::from_string("cap:action=generate;ext=pdf").unwrap();
        assert_eq!(cap.get_tag("action"), Some(&"generate".to_string()));
    }

    #[test]
    fn test_trailing_semicolon_equivalence() {
        // Both with and without trailing semicolon should be equivalent
        let cap1 = CapUrn::from_string("cap:action=generate;ext=pdf").unwrap();
        let cap2 = CapUrn::from_string("cap:action=generate;ext=pdf;").unwrap();
        
        // They should be equal
        assert_eq!(cap1, cap2);
        
        // They should have same hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher1 = DefaultHasher::new();
        cap1.hash(&mut hasher1);
        let hash1 = hasher1.finish();
        
        let mut hasher2 = DefaultHasher::new();
        cap2.hash(&mut hasher2);
        let hash2 = hasher2.finish();
        
        assert_eq!(hash1, hash2);
        
        // They should have same string representation (canonical form)
        assert_eq!(cap1.to_string(), cap2.to_string());
        
        // They should match each other
        assert!(cap1.matches(&cap2));
        assert!(cap2.matches(&cap1));
    }

    #[test]
    fn test_canonical_string_format() {
        let cap = CapUrn::from_string("cap:action=generate;target=thumbnail;ext=pdf").unwrap();
        // Should be sorted alphabetically and have no trailing semicolon in canonical form
        assert_eq!(cap.to_string(), "cap:action=generate;ext=pdf;target=thumbnail");
    }

    #[test]
    fn test_tag_matching() {
        let cap = CapUrn::from_string("cap:action=generate;ext=pdf;target=thumbnail;").unwrap();
        
        // Exact match
        let request1 = CapUrn::from_string("cap:action=generate;ext=pdf;target=thumbnail;").unwrap();
        assert!(cap.matches(&request1));
        
        // Subset match
        let request2 = CapUrn::from_string("cap:action=generate").unwrap();
        assert!(cap.matches(&request2));
        
        // Wildcard request should match specific cap  
        let request3 = CapUrn::from_string("cap:ext=*").unwrap();
        assert!(cap.matches(&request3)); // Cap has ext=pdf, request accepts any ext
        
        // No match - conflicting value
        let request4 = CapUrn::from_string("cap:action=extract").unwrap(); // Different action should not match
        assert!(!cap.matches(&request4));
    }

    #[test]
    fn test_missing_tag_handling() {
        let cap = CapUrn::from_string("cap:action=generate").unwrap();
        
        // Request with tag should match cap without tag (treated as wildcard)
        let request1 = CapUrn::from_string("cap:ext=pdf").unwrap();
        assert!(cap.matches(&request1)); // cap missing ext tag = wildcard, can handle any ext
        
        // But cap with extra tags can match subset requests
        let cap2 = CapUrn::from_string("cap:action=generate;ext=pdf").unwrap();
        let request2 = CapUrn::from_string("cap:action=generate").unwrap();
        assert!(cap2.matches(&request2));
    }

    #[test]
    fn test_specificity() {
        let cap1 = CapUrn::from_string("cap:type=general").unwrap();
        let cap2 = CapUrn::from_string("cap:action=generate").unwrap();
        let cap3 = CapUrn::from_string("cap:action=*;ext=pdf").unwrap();
        
        assert_eq!(cap1.specificity(), 1);
        assert_eq!(cap2.specificity(), 1);
        assert_eq!(cap3.specificity(), 1); // wildcard doesn't count
        
        assert!(!cap2.is_more_specific_than(&cap1)); // Different tags, not compatible
    }

    #[test]
    fn test_builder() {
        let cap = CapUrnBuilder::new()
            
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
        let cap1 = CapUrn::from_string("cap:action=generate;ext=pdf").unwrap();
        let cap2 = CapUrn::from_string("cap:action=generate;format=*").unwrap();
        let cap3 = CapUrn::from_string("cap:type=image;action=extract").unwrap();
        
        assert!(cap1.is_compatible_with(&cap2));
        assert!(cap2.is_compatible_with(&cap1));
        assert!(!cap1.is_compatible_with(&cap3));
        
        // Missing tags are treated as wildcards for compatibility
        let cap4 = CapUrn::from_string("cap:action=generate").unwrap();
        assert!(cap1.is_compatible_with(&cap4));
        assert!(cap4.is_compatible_with(&cap1));
    }

    #[test]
    fn test_best_match() {
        let caps = vec![
            CapUrn::from_string("cap:action=*").unwrap(),
            CapUrn::from_string("cap:action=generate").unwrap(),
            CapUrn::from_string("cap:action=generate;ext=pdf").unwrap(),
        ];
        
        let request = CapUrn::from_string("cap:action=generate").unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();
        
        // Most specific cap that can handle the request
        assert_eq!(best.to_string(), "cap:action=generate;ext=pdf");
    }

    #[test]
    fn test_merge_and_subset() {
        let cap1 = CapUrn::from_string("cap:action=generate").unwrap();
        let cap2 = CapUrn::from_string("cap:ext=pdf;output=binary").unwrap();
        
        let merged = cap1.merge(&cap2);
        assert_eq!(merged.to_string(), "cap:action=generate;ext=pdf;output=binary");
        
        let subset = merged.subset(&["type", "ext"]);
        assert_eq!(subset.to_string(), "cap:ext=pdf");
    }

    #[test]
    fn test_wildcard_tag() {
        let cap = CapUrn::from_string("cap:ext=pdf").unwrap();
        let wildcarded = cap.clone().with_wildcard_tag("ext");
        
        assert_eq!(wildcarded.to_string(), "cap:ext=*");
        
        // Test that wildcarded cap can match more requests
        let request = CapUrn::from_string("cap:ext=jpg").unwrap();
        assert!(!cap.matches(&request));
        assert!(wildcarded.matches(&CapUrn::from_string("cap:ext=*").unwrap()));
    }

    #[test]
    fn test_empty_cap_urn() {
        // Empty cap URN should be valid and match everything
        let empty_cap = CapUrn::from_string("cap:").unwrap();
        assert_eq!(empty_cap.tags.len(), 0);
        assert_eq!(empty_cap.to_string(), "cap:");
        
        // Should match any other cap
        let specific_cap = CapUrn::from_string("cap:action=generate;ext=pdf").unwrap();
        assert!(empty_cap.matches(&specific_cap));
        assert!(empty_cap.matches(&empty_cap));
    }

    #[test]
    fn test_extended_character_support() {
        // Test forward slashes and colons in tag components
        let cap = CapUrn::from_string("cap:url=https://example_org/api;path=/some/file").unwrap();
        assert_eq!(cap.get_tag("url"), Some(&"https://example_org/api".to_string()));
        assert_eq!(cap.get_tag("path"), Some(&"/some/file".to_string()));
    }

    #[test]
    fn test_wildcard_restrictions() {
        // Wildcard should be rejected in keys
        assert!(CapUrn::from_string("cap:*=value").is_err());
        
        // Wildcard should be accepted in values
        let cap = CapUrn::from_string("cap:key=*").unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"*".to_string()));
    }

    #[test]
    fn test_duplicate_key_rejection() {
        // This should fail in real parsing - simulating with manual insertion check
        let result = CapUrn::from_string("cap:key=value1;key=value2");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::DuplicateKey(_)));
        }
    }

    #[test]
    fn test_numeric_key_restriction() {
        // Pure numeric keys should be rejected
        assert!(CapUrn::from_string("cap:123=value").is_err());
        
        // Mixed alphanumeric keys should be allowed
        assert!(CapUrn::from_string("cap:key123=value").is_ok());
        assert!(CapUrn::from_string("cap:123key=value").is_ok());
        
        // Pure numeric values should be allowed
        assert!(CapUrn::from_string("cap:key=123").is_ok());
    }
}