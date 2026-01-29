//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap URN system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.
//!
//! Cap URNs use the tagged URN format with "cap" prefix and require mandatory
//! `in` and `out` tags that specify the input and output media URNs.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use tagged_urn::{TaggedUrn, TaggedUrnBuilder, TaggedUrnError};

use crate::media_urn::{MediaUrn, MediaUrnError, MEDIA_VOID, MEDIA_OBJECT};

/// A cap URN using flat, ordered tags with required direction specifiers
///
/// Direction (in→out) is integral to a cap's identity. The `in_urn` and `out_urn`
/// fields specify the input and output media URNs respectively.
///
/// Examples:
/// - `cap:in="media:binary";op=generate;out="media:binary";target=thumbnail`
/// - `cap:in="media:void";op=dimensions;out="media:integer"`
/// - `cap:in="media:string";out="media:object";key="Value With Spaces"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapUrn {
    /// Input media URN - required (use media:void for caps with no input)
    in_urn: String,
    /// Output media URN - required
    out_urn: String,
    /// Additional tags that define this cap, stored in sorted order for canonical representation
    /// Note: 'in' and 'out' are NOT stored here - they are in in_urn/out_urn
    pub tags: BTreeMap<String, String>,
}

impl CapUrn {
    /// The required prefix for all cap URNs
    pub const PREFIX: &'static str = "cap";

    /// Create a new cap URN from direction specs and additional tags
    /// Keys are normalized to lowercase; values are preserved as-is
    /// in_urn and out_urn are required direction specifiers (media URN strings)
    pub fn new(in_urn: String, out_urn: String, tags: BTreeMap<String, String>) -> Self {
        let normalized_tags: BTreeMap<String, String> = tags
            .into_iter()
            .filter(|(k, _)| {
                let k_lower = k.to_lowercase();
                k_lower != "in" && k_lower != "out"
            })
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        Self {
            in_urn,
            out_urn,
            tags: normalized_tags,
        }
    }

    /// Create a cap URN from tags map that must contain 'in' and 'out'
    /// This is a convenience method for TOML deserialization
    pub fn from_tags(mut tags: BTreeMap<String, String>) -> Result<Self, CapUrnError> {
        let in_urn = tags.remove("in").ok_or(CapUrnError::MissingInSpec)?;
        let out_urn = tags.remove("out").ok_or(CapUrnError::MissingOutSpec)?;
        Ok(Self::new(in_urn, out_urn, tags))
    }

    /// Create a cap URN from a string representation
    ///
    /// Format: `cap:in="media:...";out="media:...";key1=value1;...`
    /// The "cap:" prefix is mandatory
    /// The 'in' and 'out' tags are REQUIRED (direction is part of cap identity)
    /// Trailing semicolons are optional and ignored
    /// Tags are automatically sorted alphabetically for canonical form
    ///
    /// Case handling (inherited from TaggedUrn):
    /// - Keys: Always normalized to lowercase
    /// - Unquoted values: Normalized to lowercase
    /// - Quoted values: Case preserved exactly as specified
    pub fn from_string(s: &str) -> Result<Self, CapUrnError> {
        // Parse using TaggedUrn
        let tagged = TaggedUrn::from_string(s).map_err(CapUrnError::from_tagged_urn_error)?;

        // Verify cap prefix
        if tagged.prefix != Self::PREFIX {
            return Err(CapUrnError::MissingCapPrefix);
        }

        // Extract required in and out tags
        let in_urn = tagged
            .tags
            .get("in")
            .ok_or(CapUrnError::MissingInSpec)?
            .clone();
        let out_urn = tagged
            .tags
            .get("out")
            .ok_or(CapUrnError::MissingOutSpec)?
            .clone();

        // Collect remaining tags (excluding in/out)
        let tags: BTreeMap<String, String> = tagged
            .tags
            .into_iter()
            .filter(|(k, _)| k != "in" && k != "out")
            .collect();

        Ok(Self {
            in_urn,
            out_urn,
            tags,
        })
    }

    /// Get the canonical string representation of this cap URN
    ///
    /// Always includes "cap:" prefix
    /// All tags (including in/out) are sorted alphabetically
    /// No trailing semicolon in canonical form
    /// Values are quoted only when necessary (smart quoting via TaggedUrn)
    pub fn to_string(&self) -> String {
        // Build using TaggedUrnBuilder
        // in_urn and out_urn are guaranteed non-empty by constructor
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX)
            .tag("in", &self.in_urn).expect("in_urn guaranteed non-empty")
            .tag("out", &self.out_urn).expect("out_urn guaranteed non-empty");

        for (k, v) in &self.tags {
            // Tags are validated at construction time
            builder = builder.tag(k, v).expect("tag values validated at construction");
        }

        // Use build_allow_empty which returns TaggedUrn directly
        builder.build_allow_empty().to_string()
    }

    /// Get a specific tag value
    /// Key is normalized to lowercase for lookup
    /// For 'in' and 'out', returns the direction spec fields
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => Some(&self.in_urn),
            "out" => Some(&self.out_urn),
            _ => self.tags.get(&key_lower),
        }
    }

    /// Get the input media URN string
    pub fn in_spec(&self) -> &str {
        &self.in_urn
    }

    /// Get the output media URN string
    pub fn out_spec(&self) -> &str {
        &self.out_urn
    }

    /// Get the input as a parsed MediaUrn
    pub fn in_media_urn(&self) -> Result<MediaUrn, MediaUrnError> {
        MediaUrn::from_string(&self.in_urn)
    }

    /// Get the output as a parsed MediaUrn
    pub fn out_media_urn(&self) -> Result<MediaUrn, MediaUrnError> {
        MediaUrn::from_string(&self.out_urn)
    }

    /// Check if this cap has a specific tag with a specific value
    /// Key is normalized to lowercase; value comparison is case-sensitive
    /// For 'in' and 'out', checks the direction spec fields
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => self.in_urn == value,
            "out" => self.out_urn == value,
            _ => self.tags.get(&key_lower).map_or(false, |v| v == value),
        }
    }

    /// Add or update a tag
    /// Key is normalized to lowercase; value is preserved as-is
    /// Note: Cannot modify 'in' or 'out' tags - use with_in_spec/with_out_spec
    /// Returns error if value is empty (use "*" for wildcard)
    pub fn with_tag(mut self, key: String, value: String) -> Result<Self, CapUrnError> {
        if value.is_empty() {
            return Err(CapUrnError::EmptyValue(key));
        }
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" {
            // Silently ignore attempts to set in/out via with_tag
            // Use with_in_spec/with_out_spec instead
            return Ok(self);
        }
        self.tags.insert(key_lower, value);
        Ok(self)
    }

    /// Create a new cap URN with a different input spec
    pub fn with_in_spec(mut self, in_urn: String) -> Self {
        self.in_urn = in_urn;
        self
    }

    /// Create a new cap URN with a different output spec
    pub fn with_out_spec(mut self, out_urn: String) -> Self {
        self.out_urn = out_urn;
        self
    }

    /// Remove a tag
    /// Key is normalized to lowercase for case-insensitive removal
    /// Note: Cannot remove 'in' or 'out' tags - they are required
    pub fn without_tag(mut self, key: &str) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" {
            // Silently ignore attempts to remove in/out
            return self;
        }
        self.tags.remove(&key_lower);
        self
    }

    /// Check if this cap matches another based on tag compatibility
    ///
    /// Direction (in/out) is ALWAYS part of matching - they must match exactly or with wildcards.
    /// For other tags:
    /// - For each tag in the request: cap has same value, wildcard (*), or missing tag
    /// - For each tag in the cap: if request is missing that tag, that's fine (cap is more specific)
    /// Missing tags (except in/out) are treated as wildcards (less specific, can handle any value).
    pub fn matches(&self, request: &CapUrn) -> bool {
        // Direction specs must match (wildcards allowed)
        // Check in_urn
        if self.in_urn != "*" && request.in_urn != "*" && self.in_urn != request.in_urn {
            return false;
        }

        // Check out_urn
        if self.out_urn != "*" && request.out_urn != "*" && self.out_urn != request.out_urn {
            return false;
        }

        // Check all other tags that the request specifies
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
    /// Includes direction specs (in/out) in the count
    pub fn specificity(&self) -> usize {
        // Count non-wildcard direction specs
        let mut count = 0;
        if self.in_urn != "*" {
            count += 1;
        }
        if self.out_urn != "*" {
            count += 1;
        }
        // Count non-wildcard tags
        count + self.tags.values().filter(|v| v.as_str() != "*").count()
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
    /// Direction specs must be compatible (same value or one is wildcard)
    pub fn is_compatible_with(&self, other: &CapUrn) -> bool {
        // Check in_urn compatibility
        if self.in_urn != "*" && other.in_urn != "*" && self.in_urn != other.in_urn {
            return false;
        }

        // Check out_urn compatibility
        if self.out_urn != "*" && other.out_urn != "*" && self.out_urn != other.out_urn {
            return false;
        }

        // Get all unique tag keys from both caps
        let mut all_keys = self
            .tags
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
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
    /// For 'in' or 'out', sets the corresponding direction spec to wildcard
    pub fn with_wildcard_tag(mut self, key: &str) -> Self {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => {
                self.in_urn = "*".to_string();
            }
            "out" => {
                self.out_urn = "*".to_string();
            }
            _ => {
                if self.tags.contains_key(&key_lower) {
                    self.tags.insert(key_lower, "*".to_string());
                }
            }
        }
        self
    }

    /// Create a subset cap with only specified tags
    /// Note: 'in' and 'out' are always included as they are required
    pub fn subset(&self, keys: &[&str]) -> Self {
        let mut tags = BTreeMap::new();
        for &key in keys {
            let key_lower = key.to_lowercase();
            // Skip in/out as they're handled separately
            if key_lower == "in" || key_lower == "out" {
                continue;
            }
            if let Some(value) = self.tags.get(&key_lower) {
                tags.insert(key_lower, value.clone());
            }
        }
        Self {
            in_urn: self.in_urn.clone(),
            out_urn: self.out_urn.clone(),
            tags,
        }
    }

    /// Merge with another cap (other takes precedence for conflicts)
    /// Direction specs from other override this one's
    pub fn merge(&self, other: &CapUrn) -> Self {
        let mut tags = self.tags.clone();
        for (key, value) in &other.tags {
            tags.insert(key.clone(), value.clone());
        }
        Self {
            in_urn: other.in_urn.clone(),
            out_urn: other.out_urn.clone(),
            tags,
        }
    }

    pub fn canonical(cap_urn: &str) -> Result<String, CapUrnError> {
        let cap_urn_deserialized = CapUrn::from_string(cap_urn)?;
        Ok(cap_urn_deserialized.to_string())
    }

    pub fn canonical_option(cap_urn: Option<&str>) -> Result<Option<String>, CapUrnError> {
        if let Some(cu) = cap_urn {
            let cap_urn_deserialized = CapUrn::from_string(cu)?;
            Ok(Some(cap_urn_deserialized.to_string()))
        } else {
            Ok(None)
        }
    }
}

/// Errors that can occur when parsing cap URNs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CapUrnError {
    /// Error code 1: Empty or malformed URN
    Empty,
    /// Error code 5: URN does not start with `cap:`
    MissingCapPrefix,
    /// Error code 4: Tag not in key=value format
    InvalidTagFormat(String),
    /// Error code 2: Empty key or value component
    EmptyTagComponent(String),
    /// Error code 3: Disallowed character in key/value
    InvalidCharacter(String),
    /// Error code 6: Same key appears twice
    DuplicateKey(String),
    /// Error code 7: Key is purely numeric
    NumericKey(String),
    /// Error code 8: Quoted value never closed
    UnterminatedQuote(usize),
    /// Error code 9: Invalid escape in quoted value (only \" and \\ allowed)
    InvalidEscapeSequence(usize),
    /// Error code 10: Missing required 'in' tag - caps must declare their input type
    MissingInSpec,
    /// Error code 11: Missing required 'out' tag - caps must declare their output type
    MissingOutSpec,
    /// Error code 12: Empty value provided (use "*" for wildcard)
    EmptyValue(String),
}

impl CapUrnError {
    /// Convert from TaggedUrnError to CapUrnError
    fn from_tagged_urn_error(e: TaggedUrnError) -> Self {
        match e {
            TaggedUrnError::Empty => CapUrnError::Empty,
            TaggedUrnError::MissingPrefix => CapUrnError::MissingCapPrefix,
            TaggedUrnError::EmptyPrefix => CapUrnError::MissingCapPrefix,
            TaggedUrnError::InvalidTagFormat(s) => CapUrnError::InvalidTagFormat(s),
            TaggedUrnError::EmptyTagComponent(s) => CapUrnError::EmptyTagComponent(s),
            TaggedUrnError::InvalidCharacter(s) => CapUrnError::InvalidCharacter(s),
            TaggedUrnError::DuplicateKey(s) => CapUrnError::DuplicateKey(s),
            TaggedUrnError::NumericKey(s) => CapUrnError::NumericKey(s),
            TaggedUrnError::UnterminatedQuote(pos) => CapUrnError::UnterminatedQuote(pos),
            TaggedUrnError::InvalidEscapeSequence(pos) => CapUrnError::InvalidEscapeSequence(pos),
            TaggedUrnError::PrefixMismatch { .. } => CapUrnError::MissingCapPrefix,
        }
    }
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
                write!(f, "Invalid character in tag: {}", tag)
            }
            CapUrnError::DuplicateKey(key) => {
                write!(f, "Duplicate tag key: {}", key)
            }
            CapUrnError::NumericKey(key) => {
                write!(f, "Tag key cannot be purely numeric: {}", key)
            }
            CapUrnError::UnterminatedQuote(pos) => {
                write!(f, "Unterminated quote at position {}", pos)
            }
            CapUrnError::InvalidEscapeSequence(pos) => {
                write!(
                    f,
                    "Invalid escape sequence at position {} (only \\\" and \\\\ allowed)",
                    pos
                )
            }
            CapUrnError::MissingInSpec => {
                write!(f, "Cap URN is missing required 'in' tag - caps must declare their input type (use {} for no input)", MEDIA_VOID)
            }
            CapUrnError::MissingOutSpec => {
                write!(
                    f,
                    "Cap URN is missing required 'out' tag - caps must declare their output type"
                )
            }
            CapUrnError::EmptyValue(key) => {
                write!(
                    f,
                    "Empty value for key '{}' (use '*' for wildcard)",
                    key
                )
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
    pub fn find_best_match<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Option<&'a CapUrn> {
        caps.iter()
            .filter(|cap| cap.can_handle(request))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all caps that can handle a request, sorted by specificity
    pub fn find_all_matches<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Vec<&'a CapUrn> {
        let mut matches: Vec<&CapUrn> = caps.iter().filter(|cap| cap.can_handle(request)).collect();

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
/// Direction specs (in/out) are required and must be set before building
pub struct CapUrnBuilder {
    in_urn: Option<String>,
    out_urn: Option<String>,
    tags: BTreeMap<String, String>,
}

impl CapUrnBuilder {
    pub fn new() -> Self {
        Self {
            in_urn: None,
            out_urn: None,
            tags: BTreeMap::new(),
        }
    }

    /// Set the input media URN (required)
    pub fn in_spec(mut self, spec: &str) -> Self {
        self.in_urn = Some(spec.to_string());
        self
    }

    /// Set the output media URN (required)
    pub fn out_spec(mut self, spec: &str) -> Self {
        self.out_urn = Some(spec.to_string());
        self
    }

    /// Add a tag with key (normalized to lowercase) and value (preserved as-is)
    /// Note: 'in' and 'out' are ignored here - use in_spec() and out_spec()
    pub fn tag(mut self, key: &str, value: &str) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" {
            return self;
        }
        self.tags.insert(key_lower, value.to_string());
        self
    }

	pub fn solo_tag(mut self, key: &str) -> Self {
		let key_lower = key.to_lowercase();
		if key_lower == "in" || key_lower == "out" {
			return self;
		}
		self.tags.insert(key_lower, "*".to_string());
		self
	}

    pub fn build(self) -> Result<CapUrn, CapUrnError> {
        let in_urn = self.in_urn.ok_or(CapUrnError::MissingInSpec)?;
        let out_urn = self.out_urn.ok_or(CapUrnError::MissingOutSpec)?;
        Ok(CapUrn::new(in_urn, out_urn, self.tags))
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

    // All cap URNs now require in and out specs. Use these helpers:
    fn test_urn(tags: &str) -> String {
        if tags.is_empty() {
            format!("cap:in=\"{}\";out=\"{}\"", MEDIA_VOID, MEDIA_OBJECT)
        } else {
            format!("cap:in=\"{}\";out=\"{}\";{}", MEDIA_VOID, MEDIA_OBJECT, tags)
        }
    }

    fn test_urn_with_io(in_spec: &str, out_spec: &str, tags: &str) -> String {
        if tags.is_empty() {
            format!("cap:in=\"{}\";out=\"{}\"", in_spec, out_spec)
        } else {
            format!("cap:in=\"{}\";out=\"{}\";{}", in_spec, out_spec, tags)
        }
    }

    #[test]
    fn test_cap_urn_creation() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        // Direction specs are required and accessible
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    #[test]
    fn test_direction_specs_required() {
        // Missing 'in' should fail
        let result = CapUrn::from_string(&format!("cap:out=\"{}\";op=test", MEDIA_OBJECT));
        assert!(result.is_err());
        assert!(matches!(result, Err(CapUrnError::MissingInSpec)));

        // Missing 'out' should fail
        let result = CapUrn::from_string(&format!("cap:in=\"{}\";op=test", MEDIA_VOID));
        assert!(result.is_err());
        assert!(matches!(result, Err(CapUrnError::MissingOutSpec)));

        // Both present should succeed
        let result = CapUrn::from_string(&format!("cap:in=\"{}\";out=\"{}\";op=test", MEDIA_VOID, MEDIA_OBJECT));
        assert!(result.is_ok());
    }

    #[test]
    fn test_direction_matching() {
        let in_str = "media:string";
        let out_obj = "media:object";
        let in_bin = "media:binary";
        let out_int = "media:integer";

        // Direction specs must match for caps to match
        let cap1 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_obj)).unwrap();
        let cap2 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_obj)).unwrap();
        assert!(cap1.matches(&cap2));

        // Different in_urn should not match
        let cap3 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_bin, out_obj)).unwrap();
        assert!(!cap1.matches(&cap3));

        // Different out_urn should not match
        let cap4 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_int)).unwrap();
        assert!(!cap1.matches(&cap4));

        // Wildcard in direction should match
        let cap5 = CapUrn::from_string(&format!("cap:in=*;op=test;out=\"{}\"", out_obj)).unwrap();
        assert!(cap1.matches(&cap5));
        assert!(cap5.matches(&cap1));
    }

    #[test]
    fn test_unquoted_values_lowercased() {
        // Unquoted values are normalized to lowercase
        let cap = CapUrn::from_string(&test_urn("OP=Generate;EXT=PDF;Target=Thumbnail")).unwrap();

        // Keys are always lowercase
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));

        // Key lookup is case-insensitive
        assert_eq!(cap.get_tag("OP"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("Op"), Some(&"generate".to_string()));

        // Both URNs parse to same lowercase values (same tags, same values)
        let cap2 = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
        assert_eq!(cap.to_string(), cap2.to_string());
        assert_eq!(cap, cap2);
    }

    #[test]
    fn test_quoted_values_preserve_case() {
        // Quoted values preserve their case
        let cap = CapUrn::from_string(&test_urn(r#"key="Value With Spaces""#)).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Key is still lowercase
        let cap2 = CapUrn::from_string(&test_urn(r#"KEY="Value With Spaces""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Unquoted vs quoted case difference
        let unquoted = CapUrn::from_string(&test_urn("key=UPPERCASE")).unwrap();
        let quoted = CapUrn::from_string(&test_urn(r#"key="UPPERCASE""#)).unwrap();
        assert_eq!(unquoted.get_tag("key"), Some(&"uppercase".to_string())); // lowercase
        assert_eq!(quoted.get_tag("key"), Some(&"UPPERCASE".to_string())); // preserved
        assert_ne!(unquoted, quoted); // NOT equal
    }

    #[test]
    fn test_quoted_value_special_chars() {
        // Semicolons in quoted values
        let cap = CapUrn::from_string(&test_urn(r#"key="value;with;semicolons""#)).unwrap();
        assert_eq!(
            cap.get_tag("key"),
            Some(&"value;with;semicolons".to_string())
        );

        // Equals in quoted values
        let cap2 = CapUrn::from_string(&test_urn(r#"key="value=with=equals""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"value=with=equals".to_string()));

        // Spaces in quoted values
        let cap3 = CapUrn::from_string(&test_urn(r#"key="hello world""#)).unwrap();
        assert_eq!(cap3.get_tag("key"), Some(&"hello world".to_string()));
    }

    #[test]
    fn test_quoted_value_escape_sequences() {
        // Escaped quotes
        let cap = CapUrn::from_string(&test_urn(r#"key="value\"quoted\"""#)).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&r#"value"quoted""#.to_string()));

        // Escaped backslashes
        let cap2 = CapUrn::from_string(&test_urn(r#"key="path\\file""#)).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&r#"path\file"#.to_string()));

        // Mixed escapes
        let cap3 = CapUrn::from_string(&test_urn(r#"key="say \"hello\\world\"""#)).unwrap();
        assert_eq!(
            cap3.get_tag("key"),
            Some(&r#"say "hello\world""#.to_string())
        );
    }

    #[test]
    fn test_mixed_quoted_unquoted() {
        let cap = CapUrn::from_string(&test_urn(r#"a="Quoted";b=simple"#)).unwrap();
        assert_eq!(cap.get_tag("a"), Some(&"Quoted".to_string()));
        assert_eq!(cap.get_tag("b"), Some(&"simple".to_string()));
    }

    #[test]
    fn test_unterminated_quote_error() {
        let result = CapUrn::from_string(&test_urn(r#"key="unterminated"#));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::UnterminatedQuote(_)));
        }
    }

    #[test]
    fn test_invalid_escape_sequence_error() {
        let result = CapUrn::from_string(&test_urn(r#"key="bad\n""#));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }

        // Invalid escape at end
        let result2 = CapUrn::from_string(&test_urn(r#"key="bad\x""#));
        assert!(result2.is_err());
        if let Err(e) = result2 {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }
    }

    #[test]
    fn test_serialization_smart_quoting() {
        // Simple lowercase value - no quoting needed
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "simple")
            .build()
            .unwrap();
        // The serialized form should contain key=simple (unquoted)
        let s = cap.to_string();
        assert!(s.contains("key=simple"));

        // Value with spaces - needs quoting
        let cap2 = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "has spaces")
            .build()
            .unwrap();
        let s2 = cap2.to_string();
        assert!(s2.contains(r#"key="has spaces""#));

        // Value with uppercase - needs quoting to preserve
        let cap4 = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("key", "HasUpper")
            .build()
            .unwrap();
        let s4 = cap4.to_string();
        assert!(s4.contains(r#"key="HasUpper""#));
    }

    #[test]
    fn test_round_trip_simple() {
        let original = test_urn("op=generate;ext=pdf");
        let cap = CapUrn::from_string(&original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    #[test]
    fn test_round_trip_quoted() {
        let original = test_urn(r#"key="Value With Spaces""#);
        let cap = CapUrn::from_string(&original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
        assert_eq!(
            reparsed.get_tag("key"),
            Some(&"Value With Spaces".to_string())
        );
    }

    #[test]
    fn test_round_trip_escapes() {
        let original = test_urn(r#"key="value\"with\\escapes""#);
        let cap = CapUrn::from_string(&original).unwrap();
        assert_eq!(
            cap.get_tag("key"),
            Some(&r#"value"with\escapes"#.to_string())
        );
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    #[test]
    fn test_cap_prefix_required() {
        // Missing cap: prefix should fail
        assert!(CapUrn::from_string(&format!(
            "in=\"{}\";out=\"{}\";op=generate",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .is_err());

        // Valid cap: prefix should work
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));

        // Case-insensitive prefix
        let cap2 = CapUrn::from_string(&format!(
            "CAP:in=\"{}\";out=\"{}\";op=generate",
            MEDIA_VOID, MEDIA_OBJECT
        ))
        .unwrap();
        assert_eq!(cap2.get_tag("op"), Some(&"generate".to_string()));
    }

    #[test]
    fn test_trailing_semicolon_equivalence() {
        // Both with and without trailing semicolon should be equivalent
        let cap1 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let cap2 =
            CapUrn::from_string(&format!("{};", test_urn("op=generate;ext=pdf"))).unwrap();

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
    fn test_tag_matching() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();

        // Exact match
        let request1 =
            CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
        assert!(cap.matches(&request1));

        // Subset match (other tags)
        let request2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(cap.matches(&request2));

        // Wildcard request should match specific cap
        let request3 = CapUrn::from_string(&test_urn("ext=*")).unwrap();
        assert!(cap.matches(&request3)); // Cap has ext=pdf, request accepts any ext

        // No match - conflicting value
        let request4 = CapUrn::from_string(&test_urn("op=extract")).unwrap();
        assert!(!cap.matches(&request4));
    }

    #[test]
    fn test_matching_case_sensitive_values() {
        // Values with different case should NOT match
        let cap1 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        let cap2 = CapUrn::from_string(&test_urn(r#"key="value""#)).unwrap();
        assert!(!cap1.matches(&cap2));
        assert!(!cap2.matches(&cap1));

        // Same case should match
        let cap3 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        assert!(cap1.matches(&cap3));
    }

    #[test]
    fn test_missing_tag_handling() {
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();

        // Request with tag should match cap without tag (treated as wildcard)
        let request1 = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        assert!(cap.matches(&request1)); // cap missing ext tag = wildcard, can handle any ext

        // But cap with extra tags can match subset requests
        let cap2 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(cap2.matches(&request2));
    }

    #[test]
    fn test_specificity() {
        // Specificity now includes in/out (2 base) + other tags
        let cap1 = CapUrn::from_string(&test_urn("type=general")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("op=*;ext=pdf")).unwrap();

        assert_eq!(cap1.specificity(), 3); // in + out + type
        assert_eq!(cap2.specificity(), 3); // in + out + op
        assert_eq!(cap3.specificity(), 3); // in + out + ext (wildcard op doesn't count)

        // Wildcard in direction doesn't count
        let cap4 =
            CapUrn::from_string(&format!("cap:in=*;out=\"{}\";op=test", MEDIA_OBJECT)).unwrap();
        assert_eq!(cap4.specificity(), 2); // out + op (in wildcard doesn't count)
    }

    #[test]
    fn test_builder() {
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("op", "generate")
            .tag("target", "thumbnail")
            .tag("ext", "pdf")
            .build()
            .unwrap();

        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    #[test]
    fn test_builder_requires_direction() {
        // Missing in_spec should fail
        let result = CapUrnBuilder::new()
            .out_spec(MEDIA_OBJECT)
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Missing out_spec should fail
        let result = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Both present should succeed
        let result = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_preserves_case() {
        let cap = CapUrnBuilder::new()
            .in_spec(MEDIA_VOID)
            .out_spec(MEDIA_OBJECT)
            .tag("KEY", "ValueWithCase")
            .build()
            .unwrap();

        // Key is lowercase
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
    }

    #[test]
    fn test_compatibility() {
        let cap1 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("op=generate;format=*")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("type=image;op=extract")).unwrap();

        assert!(cap1.is_compatible_with(&cap2));
        assert!(cap2.is_compatible_with(&cap1));
        assert!(!cap1.is_compatible_with(&cap3));

        // Missing tags are treated as wildcards for compatibility
        let cap4 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(cap1.is_compatible_with(&cap4));
        assert!(cap4.is_compatible_with(&cap1));

        // Different direction specs are incompatible
        let cap5 = CapUrn::from_string(&format!(
            "cap:in=media:bytes;out=\"{}\";op=generate",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(!cap1.is_compatible_with(&cap5));
    }

    #[test]
    fn test_best_match() {
        let caps = vec![
            CapUrn::from_string(&test_urn("op=*")).unwrap(),
            CapUrn::from_string(&test_urn("op=generate")).unwrap(),
            CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap(),
        ];

        let request = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();

        // Most specific cap that can handle the request
        assert_eq!(best.get_tag("ext"), Some(&"pdf".to_string()));
    }

    #[test]
    fn test_merge_and_subset() {
        let cap1 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let cap2 = CapUrn::from_string(&format!(
            "cap:in=media:bytes;out=media:integer;ext=pdf;output=binary"
        ))
        .unwrap();

        let merged = cap1.merge(&cap2);
        // Merged takes in/out from cap2
        assert_eq!(merged.in_spec(), "media:bytes");
        assert_eq!(merged.out_spec(), "media:integer");
        // Has tags from both
        assert_eq!(merged.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(merged.get_tag("ext"), Some(&"pdf".to_string()));

        let subset = merged.subset(&["type", "ext"]);
        // subset keeps in/out from merged
        assert_eq!(subset.in_spec(), "media:bytes");
        assert_eq!(subset.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(subset.get_tag("type"), None);
    }

    #[test]
    fn test_wildcard_tag() {
        let cap = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        let wildcarded = cap.clone().with_wildcard_tag("ext");

        assert_eq!(wildcarded.get_tag("ext"), Some(&"*".to_string()));

        // Test wildcarding in/out
        let wildcard_in = cap.clone().with_wildcard_tag("in");
        assert_eq!(wildcard_in.in_spec(), "*");

        let wildcard_out = cap.clone().with_wildcard_tag("out");
        assert_eq!(wildcard_out.out_spec(), "*");
    }

    #[test]
    fn test_empty_cap_urn_not_allowed() {
        // Empty cap URN is no longer valid since in/out are required
        let result = CapUrn::from_string("cap:");
        assert!(result.is_err());
        assert!(matches!(result, Err(CapUrnError::MissingInSpec)));

        // With trailing semicolon - still fails
        let result = CapUrn::from_string("cap:;");
        assert!(result.is_err());
    }

    #[test]
    fn test_minimal_cap_urn() {
        // Minimal valid cap URN has just in and out
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";out=\"{}\"", MEDIA_VOID, MEDIA_OBJECT))
            .unwrap();
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
        assert!(cap.tags.is_empty());
    }

    #[test]
    fn test_extended_character_support() {
        // Test forward slashes and colons in tag components
        let cap = CapUrn::from_string(&test_urn("url=https://example_org/api;path=/some/file"))
            .unwrap();
        assert_eq!(
            cap.get_tag("url"),
            Some(&"https://example_org/api".to_string())
        );
        assert_eq!(cap.get_tag("path"), Some(&"/some/file".to_string()));
    }

    #[test]
    fn test_wildcard_restrictions() {
        // Wildcard should be rejected in keys
        assert!(CapUrn::from_string(&test_urn("*=value")).is_err());

        // Wildcard should be accepted in values
        let cap = CapUrn::from_string(&test_urn("key=*")).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"*".to_string()));
    }

    #[test]
    fn test_duplicate_key_rejection() {
        let result = CapUrn::from_string(&test_urn("key=value1;key=value2"));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::DuplicateKey(_)));
        }
    }

    #[test]
    fn test_numeric_key_restriction() {
        // Pure numeric keys should be rejected
        assert!(CapUrn::from_string(&test_urn("123=value")).is_err());

        // Mixed alphanumeric keys should be allowed
        assert!(CapUrn::from_string(&test_urn("key123=value")).is_ok());
        assert!(CapUrn::from_string(&test_urn("123key=value")).is_ok());

        // Pure numeric values should be allowed
        assert!(CapUrn::from_string(&test_urn("key=123")).is_ok());
    }

    #[test]
    fn test_empty_value_error() {
        assert!(CapUrn::from_string(&test_urn("key=")).is_err());
        assert!(CapUrn::from_string(&test_urn("key=;other=value")).is_err());
    }

    #[test]
    fn test_has_tag_case_sensitive() {
        let cap = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();

        // Exact case match works
        assert!(cap.has_tag("key", "Value"));

        // Different case does not match
        assert!(!cap.has_tag("key", "value"));
        assert!(!cap.has_tag("key", "VALUE"));

        // Key lookup is case-insensitive
        assert!(cap.has_tag("KEY", "Value"));
        assert!(cap.has_tag("Key", "Value"));

        // has_tag works for in/out
        assert!(cap.has_tag("in", MEDIA_VOID));
        assert!(cap.has_tag("out", MEDIA_OBJECT));
    }

    #[test]
    fn test_with_tag_preserves_value() {
        let cap = CapUrn::new(MEDIA_VOID.to_string(), MEDIA_OBJECT.to_string(), BTreeMap::new())
            .with_tag("key".to_string(), "ValueWithCase".to_string()).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
    }

    #[test]
    fn test_with_tag_rejects_empty_value() {
        let cap = CapUrn::new(MEDIA_VOID.to_string(), MEDIA_OBJECT.to_string(), BTreeMap::new());
        let result = cap.with_tag("key".to_string(), "".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_semantic_equivalence() {
        // Unquoted and quoted simple lowercase values are equivalent
        let unquoted = CapUrn::from_string(&test_urn("key=simple")).unwrap();
        let quoted = CapUrn::from_string(&test_urn(r#"key="simple""#)).unwrap();
        assert_eq!(unquoted, quoted);

        // Both serialize the same way (unquoted for simple values)
        assert!(unquoted.to_string().contains("key=simple"));
        assert!(quoted.to_string().contains("key=simple"));
    }

    #[test]
    fn test_get_tag_returns_direction_specs() {
        let in_str = "media:string";
        let out_int = "media:integer";
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";op=test;out=\"{}\"",
            in_str, out_int
        ))
        .unwrap();

        // get_tag works for in/out
        assert_eq!(cap.get_tag("in"), Some(&in_str.to_string()));
        assert_eq!(cap.get_tag("out"), Some(&out_int.to_string()));
        assert_eq!(cap.get_tag("op"), Some(&"test".to_string()));

        // Case-insensitive lookup for in/out
        assert_eq!(cap.get_tag("IN"), Some(&in_str.to_string()));
        assert_eq!(cap.get_tag("OUT"), Some(&out_int.to_string()));
    }

    // ============================================================================
    // MATCHING SEMANTICS SPECIFICATION TESTS
    // These tests verify the exact matching semantics
    // All implementations (Rust, Go, JS, ObjC) must pass these identically
    // Note: All tests now require in/out direction specs using media URNs
    // ============================================================================

    #[test]
    fn test_matching_semantics_test1_exact_match() {
        // Test 1: Exact match
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 1: Exact match should succeed");
    }

    #[test]
    fn test_matching_semantics_test2_cap_missing_tag() {
        // Test 2: Cap missing tag (implicit wildcard for other tags, not direction)
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(
            cap.matches(&request),
            "Test 2: Cap missing tag should match (implicit wildcard)"
        );
    }

    #[test]
    fn test_matching_semantics_test3_cap_has_extra_tag() {
        // Test 3: Cap has extra tag
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;version=2")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(
            cap.matches(&request),
            "Test 3: Cap with extra tag should match"
        );
    }

    #[test]
    fn test_matching_semantics_test4_request_has_wildcard() {
        // Test 4: Request has wildcard
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        assert!(
            cap.matches(&request),
            "Test 4: Request wildcard should match"
        );
    }

    #[test]
    fn test_matching_semantics_test5_cap_has_wildcard() {
        // Test 5: Cap has wildcard
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 5: Cap wildcard should match");
    }

    #[test]
    fn test_matching_semantics_test6_value_mismatch() {
        // Test 6: Value mismatch
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=docx")).unwrap();
        assert!(
            !cap.matches(&request),
            "Test 6: Value mismatch should not match"
        );
    }

    #[test]
    fn test_matching_semantics_test7_fallback_pattern() {
        // Test 7: Fallback pattern
        let in_bin = "media:binary";
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";op=generate_thumbnail;out=\"{}\"",
            in_bin, in_bin
        ))
        .unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=wav;in=\"{}\";op=generate_thumbnail;out=\"{}\"",
            in_bin, in_bin
        ))
        .unwrap();
        assert!(
            cap.matches(&request),
            "Test 7: Fallback pattern should match (cap missing ext = implicit wildcard)"
        );
    }

    #[test]
    fn test_matching_semantics_test7b_thumbnail_void_input() {
        // Test 7b: Thumbnail fallback with void input (real-world scenario)
        let out_bin = "media:binary";
        let cap = CapUrn::from_string(&format!(
            "cap:in=\"{}\";op=generate_thumbnail;out=\"{}\"",
            MEDIA_VOID, out_bin
        ))
        .unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=wav;in=\"{}\";op=generate_thumbnail;out=\"{}\"",
            MEDIA_VOID, out_bin
        ))
        .unwrap();
        assert!(
            cap.matches(&request),
            "Test 7b: Thumbnail fallback with void input should match"
        );
    }

    #[test]
    fn test_matching_semantics_test8_wildcard_direction_matches_anything() {
        // Test 8: Wildcard direction matches anything
        let cap = CapUrn::from_string("cap:in=*;out=*").unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:ext=pdf;in=media:string;op=generate;out=\"{}\"",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(
            cap.matches(&request),
            "Test 8: Wildcard direction should match any direction"
        );
    }

    #[test]
    fn test_matching_semantics_test9_cross_dimension_independence() {
        // Test 9: Cross-dimension independence (for other tags)
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        assert!(
            cap.matches(&request),
            "Test 9: Cross-dimension independence should match"
        );
    }

    #[test]
    fn test_matching_semantics_test10_direction_mismatch() {
        // Test 10: Direction mismatch prevents matching
        let cap = CapUrn::from_string(&format!(
            "cap:in=media:string;op=generate;out=\"{}\"",
            MEDIA_OBJECT
        ))
        .unwrap();
        let request = CapUrn::from_string(&format!(
            "cap:in=media:bytes;op=generate;out=\"{}\"",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(
            !cap.matches(&request),
            "Test 10: Direction mismatch should not match"
        );
    }
}
