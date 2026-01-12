//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap URN system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A cap URN using flat, ordered tags
///
/// Examples:
/// - `cap:op=generate;ext=pdf;output=binary;target=thumbnail`
/// - `cap:op=extract;target=metadata`
/// - `cap:key="Value With Spaces"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapUrn {
    /// The tags that define this cap, stored in sorted order for canonical representation
    pub tags: BTreeMap<String, String>,
}

/// Parser states for the state machine
#[derive(Debug, Clone, Copy, PartialEq)]
enum ParseState {
    ExpectingKey,
    InKey,
    ExpectingValue,
    InUnquotedValue,
    InQuotedValue,
    InQuotedValueEscape,
    ExpectingSemiOrEnd,
}

impl CapUrn {
    /// Create a new cap URN from tags
    /// Keys are normalized to lowercase; values are preserved as-is
    pub fn new(tags: BTreeMap<String, String>) -> Self {
        let normalized_tags = tags
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        Self {
            tags: normalized_tags,
        }
    }

    /// Create an empty cap URN
    pub fn empty() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    /// Create a cap URN from a string representation
    ///
    /// Format: `cap:key1=value1;key2=value2;...` or `cap:key1="value with spaces";key2=simple`
    /// The "cap:" prefix is mandatory
    /// Trailing semicolons are optional and ignored
    /// Tags are automatically sorted alphabetically for canonical form
    ///
    /// Case handling:
    /// - Keys: Always normalized to lowercase
    /// - Unquoted values: Normalized to lowercase
    /// - Quoted values: Case preserved exactly as specified
    pub fn from_string(s: &str) -> Result<Self, CapUrnError> {
        if s.is_empty() {
            return Err(CapUrnError::Empty);
        }

        // Check for "cap:" prefix (case-insensitive)
        if s.len() < 4 || !s[..4].eq_ignore_ascii_case("cap:") {
            return Err(CapUrnError::MissingCapPrefix);
        }

        let tags_part = &s[4..];
        let mut tags = BTreeMap::new();

        // Handle empty cap URN (cap: with no tags)
        if tags_part.is_empty() || tags_part == ";" {
            return Ok(Self { tags });
        }

        let mut state = ParseState::ExpectingKey;
        let mut current_key = String::new();
        let mut current_value = String::new();
        let chars: Vec<char> = tags_part.chars().collect();
        let mut pos = 0;

        while pos < chars.len() {
            let c = chars[pos];

            match state {
                ParseState::ExpectingKey => {
                    if c == ';' {
                        // Empty segment, skip
                        pos += 1;
                        continue;
                    } else if Self::is_valid_key_char(c) {
                        current_key.push(c.to_ascii_lowercase());
                        state = ParseState::InKey;
                    } else {
                        return Err(CapUrnError::InvalidCharacter(format!(
                            "invalid character '{}' at position {}",
                            c, pos
                        )));
                    }
                }

                ParseState::InKey => {
                    if c == '=' {
                        if current_key.is_empty() {
                            return Err(CapUrnError::EmptyTagComponent(
                                "empty key".to_string(),
                            ));
                        }
                        state = ParseState::ExpectingValue;
                    } else if Self::is_valid_key_char(c) {
                        current_key.push(c.to_ascii_lowercase());
                    } else {
                        return Err(CapUrnError::InvalidCharacter(format!(
                            "invalid character '{}' in key at position {}",
                            c, pos
                        )));
                    }
                }

                ParseState::ExpectingValue => {
                    if c == '"' {
                        state = ParseState::InQuotedValue;
                    } else if c == ';' {
                        return Err(CapUrnError::EmptyTagComponent(format!(
                            "empty value for key '{}'",
                            current_key
                        )));
                    } else if Self::is_valid_unquoted_value_char(c) {
                        current_value.push(c.to_ascii_lowercase());
                        state = ParseState::InUnquotedValue;
                    } else {
                        return Err(CapUrnError::InvalidCharacter(format!(
                            "invalid character '{}' in value at position {}",
                            c, pos
                        )));
                    }
                }

                ParseState::InUnquotedValue => {
                    if c == ';' {
                        Self::finish_tag(&mut tags, &mut current_key, &mut current_value)?;
                        state = ParseState::ExpectingKey;
                    } else if Self::is_valid_unquoted_value_char(c) {
                        current_value.push(c.to_ascii_lowercase());
                    } else {
                        return Err(CapUrnError::InvalidCharacter(format!(
                            "invalid character '{}' in unquoted value at position {}",
                            c, pos
                        )));
                    }
                }

                ParseState::InQuotedValue => {
                    if c == '"' {
                        state = ParseState::ExpectingSemiOrEnd;
                    } else if c == '\\' {
                        state = ParseState::InQuotedValueEscape;
                    } else {
                        // Any character allowed in quoted value, preserve case
                        current_value.push(c);
                    }
                }

                ParseState::InQuotedValueEscape => {
                    if c == '"' || c == '\\' {
                        current_value.push(c);
                        state = ParseState::InQuotedValue;
                    } else {
                        return Err(CapUrnError::InvalidEscapeSequence(pos));
                    }
                }

                ParseState::ExpectingSemiOrEnd => {
                    if c == ';' {
                        Self::finish_tag(&mut tags, &mut current_key, &mut current_value)?;
                        state = ParseState::ExpectingKey;
                    } else {
                        return Err(CapUrnError::InvalidCharacter(format!(
                            "expected ';' or end after quoted value, got '{}' at position {}",
                            c, pos
                        )));
                    }
                }
            }

            pos += 1;
        }

        // Handle end of input
        match state {
            ParseState::InUnquotedValue | ParseState::ExpectingSemiOrEnd => {
                Self::finish_tag(&mut tags, &mut current_key, &mut current_value)?;
            }
            ParseState::ExpectingKey => {
                // Valid - trailing semicolon or empty input after prefix
            }
            ParseState::InQuotedValue | ParseState::InQuotedValueEscape => {
                return Err(CapUrnError::UnterminatedQuote(pos));
            }
            ParseState::InKey => {
                return Err(CapUrnError::InvalidTagFormat(format!(
                    "incomplete tag '{}'",
                    current_key
                )));
            }
            ParseState::ExpectingValue => {
                return Err(CapUrnError::EmptyTagComponent(format!(
                    "empty value for key '{}'",
                    current_key
                )));
            }
        }

        Ok(Self { tags })
    }

    /// Finish a tag by validating and inserting it
    fn finish_tag(
        tags: &mut BTreeMap<String, String>,
        key: &mut String,
        value: &mut String,
    ) -> Result<(), CapUrnError> {
        if key.is_empty() {
            return Err(CapUrnError::EmptyTagComponent("empty key".to_string()));
        }
        if value.is_empty() {
            return Err(CapUrnError::EmptyTagComponent(format!(
                "empty value for key '{}'",
                key
            )));
        }

        // Check for duplicate keys
        if tags.contains_key(key.as_str()) {
            return Err(CapUrnError::DuplicateKey(key.clone()));
        }

        // Validate key cannot be purely numeric
        if Self::is_purely_numeric(key) {
            return Err(CapUrnError::NumericKey(key.clone()));
        }

        tags.insert(std::mem::take(key), std::mem::take(value));
        Ok(())
    }

    /// Check if character is valid for a key
    fn is_valid_key_char(c: char) -> bool {
        c.is_alphanumeric() || c == '_' || c == '-' || c == '/' || c == ':' || c == '.'
    }

    /// Check if character is valid for an unquoted value
    fn is_valid_unquoted_value_char(c: char) -> bool {
        c.is_alphanumeric()
            || c == '_'
            || c == '-'
            || c == '/'
            || c == ':'
            || c == '.'
            || c == '*'
    }

    /// Check if a string is purely numeric
    fn is_purely_numeric(s: &str) -> bool {
        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
    }

    /// Check if a value needs quoting for serialization
    fn needs_quoting(value: &str) -> bool {
        value.chars().any(|c| {
            c == ';' || c == '=' || c == '"' || c == '\\' || c == ' ' || c.is_uppercase()
        })
    }

    /// Quote a value for serialization
    fn quote_value(value: &str) -> String {
        let mut result = String::with_capacity(value.len() + 2);
        result.push('"');
        for c in value.chars() {
            if c == '"' || c == '\\' {
                result.push('\\');
            }
            result.push(c);
        }
        result.push('"');
        result
    }

    /// Get the canonical string representation of this cap URN
    ///
    /// Always includes "cap:" prefix
    /// Tags are already sorted alphabetically due to BTreeMap
    /// No trailing semicolon in canonical form
    /// Values are quoted only when necessary (smart quoting)
    pub fn to_string(&self) -> String {
        let tags_str = self
            .tags
            .iter()
            .map(|(k, v)| {
                if Self::needs_quoting(v) {
                    format!("{}={}", k, Self::quote_value(v))
                } else {
                    format!("{}={}", k, v)
                }
            })
            .collect::<Vec<_>>()
            .join(";");
        format!("cap:{}", tags_str)
    }

    /// Get a specific tag value
    /// Key is normalized to lowercase for lookup
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.tags.get(&key.to_lowercase())
    }

    /// Check if this cap has a specific tag with a specific value
    /// Key is normalized to lowercase; value comparison is case-sensitive
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        self.tags
            .get(&key.to_lowercase())
            .map_or(false, |v| v == value)
    }

    /// Add or update a tag
    /// Key is normalized to lowercase; value is preserved as-is
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key.to_lowercase(), value);
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
pub struct CapUrnBuilder {
    tags: BTreeMap<String, String>,
}

impl CapUrnBuilder {
    pub fn new() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    /// Add a tag with key (normalized to lowercase) and value (preserved as-is)
    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_lowercase(), value.to_string());
        self
    }

    pub fn build(self) -> Result<CapUrn, CapUrnError> {
        if self.tags.is_empty() {
            return Err(CapUrnError::Empty);
        }
        Ok(CapUrn { tags: self.tags })
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
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf;target=thumbnail;").unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
    }

    #[test]
    fn test_unquoted_values_lowercased() {
        // Unquoted values are normalized to lowercase
        let cap = CapUrn::from_string("cap:OP=Generate;EXT=PDF;Target=Thumbnail;").unwrap();

        // Keys are always lowercase
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));

        // Key lookup is case-insensitive
        assert_eq!(cap.get_tag("OP"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("Op"), Some(&"generate".to_string()));

        // Both URNs parse to same lowercase values (same tags, same values)
        let cap2 = CapUrn::from_string("cap:op=generate;ext=pdf;target=thumbnail;").unwrap();
        assert_eq!(cap.to_string(), cap2.to_string());
        assert_eq!(cap, cap2);
    }

    #[test]
    fn test_quoted_values_preserve_case() {
        // Quoted values preserve their case
        let cap = CapUrn::from_string(r#"cap:key="Value With Spaces""#).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Key is still lowercase
        let cap2 = CapUrn::from_string(r#"cap:KEY="Value With Spaces""#).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"Value With Spaces".to_string()));

        // Unquoted vs quoted case difference
        let unquoted = CapUrn::from_string("cap:key=UPPERCASE").unwrap();
        let quoted = CapUrn::from_string(r#"cap:key="UPPERCASE""#).unwrap();
        assert_eq!(unquoted.get_tag("key"), Some(&"uppercase".to_string())); // lowercase
        assert_eq!(quoted.get_tag("key"), Some(&"UPPERCASE".to_string())); // preserved
        assert_ne!(unquoted, quoted); // NOT equal
    }

    #[test]
    fn test_quoted_value_special_chars() {
        // Semicolons in quoted values
        let cap = CapUrn::from_string(r#"cap:key="value;with;semicolons""#).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"value;with;semicolons".to_string()));

        // Equals in quoted values
        let cap2 = CapUrn::from_string(r#"cap:key="value=with=equals""#).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&"value=with=equals".to_string()));

        // Spaces in quoted values
        let cap3 = CapUrn::from_string(r#"cap:key="hello world""#).unwrap();
        assert_eq!(cap3.get_tag("key"), Some(&"hello world".to_string()));
    }

    #[test]
    fn test_quoted_value_escape_sequences() {
        // Escaped quotes
        let cap = CapUrn::from_string(r#"cap:key="value\"quoted\"""#).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&r#"value"quoted""#.to_string()));

        // Escaped backslashes
        let cap2 = CapUrn::from_string(r#"cap:key="path\\file""#).unwrap();
        assert_eq!(cap2.get_tag("key"), Some(&r#"path\file"#.to_string()));

        // Mixed escapes
        let cap3 = CapUrn::from_string(r#"cap:key="say \"hello\\world\"""#).unwrap();
        assert_eq!(cap3.get_tag("key"), Some(&r#"say "hello\world""#.to_string()));
    }

    #[test]
    fn test_mixed_quoted_unquoted() {
        let cap = CapUrn::from_string(r#"cap:a="Quoted";b=simple"#).unwrap();
        assert_eq!(cap.get_tag("a"), Some(&"Quoted".to_string()));
        assert_eq!(cap.get_tag("b"), Some(&"simple".to_string()));
    }

    #[test]
    fn test_unterminated_quote_error() {
        let result = CapUrn::from_string(r#"cap:key="unterminated"#);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::UnterminatedQuote(_)));
        }
    }

    #[test]
    fn test_invalid_escape_sequence_error() {
        let result = CapUrn::from_string(r#"cap:key="bad\n""#);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }

        // Invalid escape at end
        let result2 = CapUrn::from_string(r#"cap:key="bad\x""#);
        assert!(result2.is_err());
        if let Err(e) = result2 {
            assert!(matches!(e, CapUrnError::InvalidEscapeSequence(_)));
        }
    }

    #[test]
    fn test_serialization_smart_quoting() {
        // Simple lowercase value - no quoting needed
        let cap = CapUrnBuilder::new().tag("key", "simple").build().unwrap();
        assert_eq!(cap.to_string(), "cap:key=simple");

        // Value with spaces - needs quoting
        let cap2 = CapUrnBuilder::new()
            .tag("key", "has spaces")
            .build()
            .unwrap();
        assert_eq!(cap2.to_string(), r#"cap:key="has spaces""#);

        // Value with semicolons - needs quoting
        let cap3 = CapUrnBuilder::new()
            .tag("key", "has;semi")
            .build()
            .unwrap();
        assert_eq!(cap3.to_string(), r#"cap:key="has;semi""#);

        // Value with uppercase - needs quoting to preserve
        let cap4 = CapUrnBuilder::new()
            .tag("key", "HasUpper")
            .build()
            .unwrap();
        assert_eq!(cap4.to_string(), r#"cap:key="HasUpper""#);

        // Value with quotes - needs quoting and escaping
        let cap5 = CapUrnBuilder::new()
            .tag("key", r#"has"quote"#)
            .build()
            .unwrap();
        assert_eq!(cap5.to_string(), r#"cap:key="has\"quote""#);

        // Value with backslashes - needs quoting and escaping
        let cap6 = CapUrnBuilder::new()
            .tag("key", r#"path\file"#)
            .build()
            .unwrap();
        assert_eq!(cap6.to_string(), r#"cap:key="path\\file""#);
    }

    #[test]
    fn test_round_trip_simple() {
        let original = "cap:op=generate;ext=pdf";
        let cap = CapUrn::from_string(original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    #[test]
    fn test_round_trip_quoted() {
        let original = r#"cap:key="Value With Spaces""#;
        let cap = CapUrn::from_string(original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
        assert_eq!(reparsed.get_tag("key"), Some(&"Value With Spaces".to_string()));
    }

    #[test]
    fn test_round_trip_escapes() {
        let original = r#"cap:key="value\"with\\escapes""#;
        let cap = CapUrn::from_string(original).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&r#"value"with\escapes"#.to_string()));
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    #[test]
    fn test_cap_prefix_required() {
        // Missing cap: prefix should fail
        assert!(CapUrn::from_string("op=generate;ext=pdf").is_err());

        // Valid cap: prefix should work
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));

        // Case-insensitive prefix
        let cap2 = CapUrn::from_string("CAP:op=generate").unwrap();
        assert_eq!(cap2.get_tag("op"), Some(&"generate".to_string()));
    }

    #[test]
    fn test_trailing_semicolon_equivalence() {
        // Both with and without trailing semicolon should be equivalent
        let cap1 = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let cap2 = CapUrn::from_string("cap:op=generate;ext=pdf;").unwrap();

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
        let cap = CapUrn::from_string("cap:op=generate;target=thumbnail;ext=pdf").unwrap();
        // Should be sorted alphabetically and have no trailing semicolon in canonical form
        // Alphabetical order: ext < op < target
        assert_eq!(
            cap.to_string(),
            "cap:ext=pdf;op=generate;target=thumbnail"
        );
    }

    #[test]
    fn test_tag_matching() {
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf;target=thumbnail;").unwrap();

        // Exact match
        let request1 =
            CapUrn::from_string("cap:op=generate;ext=pdf;target=thumbnail;").unwrap();
        assert!(cap.matches(&request1));

        // Subset match
        let request2 = CapUrn::from_string("cap:op=generate").unwrap();
        assert!(cap.matches(&request2));

        // Wildcard request should match specific cap
        let request3 = CapUrn::from_string("cap:ext=*").unwrap();
        assert!(cap.matches(&request3)); // Cap has ext=pdf, request accepts any ext

        // No match - conflicting value
        let request4 = CapUrn::from_string("cap:op=extract").unwrap();
        assert!(!cap.matches(&request4));
    }

    #[test]
    fn test_matching_case_sensitive_values() {
        // Values with different case should NOT match
        let cap1 = CapUrn::from_string(r#"cap:key="Value""#).unwrap();
        let cap2 = CapUrn::from_string(r#"cap:key="value""#).unwrap();
        assert!(!cap1.matches(&cap2));
        assert!(!cap2.matches(&cap1));

        // Same case should match
        let cap3 = CapUrn::from_string(r#"cap:key="Value""#).unwrap();
        assert!(cap1.matches(&cap3));
    }

    #[test]
    fn test_missing_tag_handling() {
        let cap = CapUrn::from_string("cap:op=generate").unwrap();

        // Request with tag should match cap without tag (treated as wildcard)
        let request1 = CapUrn::from_string("cap:ext=pdf").unwrap();
        assert!(cap.matches(&request1)); // cap missing ext tag = wildcard, can handle any ext

        // But cap with extra tags can match subset requests
        let cap2 = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let request2 = CapUrn::from_string("cap:op=generate").unwrap();
        assert!(cap2.matches(&request2));
    }

    #[test]
    fn test_specificity() {
        let cap1 = CapUrn::from_string("cap:type=general").unwrap();
        let cap2 = CapUrn::from_string("cap:op=generate").unwrap();
        let cap3 = CapUrn::from_string("cap:op=*;ext=pdf").unwrap();

        assert_eq!(cap1.specificity(), 1);
        assert_eq!(cap2.specificity(), 1);
        assert_eq!(cap3.specificity(), 1); // wildcard doesn't count

        assert!(!cap2.is_more_specific_than(&cap1)); // Different tags, not compatible
    }

    #[test]
    fn test_builder() {
        let cap = CapUrnBuilder::new()
            .tag("op", "generate")
            .tag("target", "thumbnail")
            .tag("ext", "pdf")
            .tag("output", "binary")
            .build()
            .unwrap();

        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("output"), Some(&"binary".to_string()));
    }

    #[test]
    fn test_builder_preserves_case() {
        let cap = CapUrnBuilder::new()
            .tag("KEY", "ValueWithCase")
            .build()
            .unwrap();

        // Key is lowercase
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
        // Value case preserved, so needs quoting
        assert_eq!(cap.to_string(), r#"cap:key="ValueWithCase""#);
    }

    #[test]
    fn test_compatibility() {
        let cap1 = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let cap2 = CapUrn::from_string("cap:op=generate;format=*").unwrap();
        let cap3 = CapUrn::from_string("cap:type=image;op=extract").unwrap();

        assert!(cap1.is_compatible_with(&cap2));
        assert!(cap2.is_compatible_with(&cap1));
        assert!(!cap1.is_compatible_with(&cap3));

        // Missing tags are treated as wildcards for compatibility
        let cap4 = CapUrn::from_string("cap:op=generate").unwrap();
        assert!(cap1.is_compatible_with(&cap4));
        assert!(cap4.is_compatible_with(&cap1));
    }

    #[test]
    fn test_best_match() {
        let caps = vec![
            CapUrn::from_string("cap:op=*").unwrap(),
            CapUrn::from_string("cap:op=generate").unwrap(),
            CapUrn::from_string("cap:op=generate;ext=pdf").unwrap(),
        ];

        let request = CapUrn::from_string("cap:op=generate").unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();

        // Most specific cap that can handle the request
        // Alphabetical order: ext < op
        assert_eq!(best.to_string(), "cap:ext=pdf;op=generate");
    }

    #[test]
    fn test_merge_and_subset() {
        let cap1 = CapUrn::from_string("cap:op=generate").unwrap();
        let cap2 = CapUrn::from_string("cap:ext=pdf;output=binary").unwrap();

        let merged = cap1.merge(&cap2);
        // Alphabetical order: ext < op < output
        assert_eq!(
            merged.to_string(),
            "cap:ext=pdf;op=generate;output=binary"
        );

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
        let specific_cap = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(empty_cap.matches(&specific_cap));
        assert!(empty_cap.matches(&empty_cap));

        // With trailing semicolon
        let empty_cap2 = CapUrn::from_string("cap:;").unwrap();
        assert_eq!(empty_cap2.tags.len(), 0);
    }

    #[test]
    fn test_extended_character_support() {
        // Test forward slashes and colons in tag components
        let cap = CapUrn::from_string("cap:url=https://example_org/api;path=/some/file").unwrap();
        assert_eq!(
            cap.get_tag("url"),
            Some(&"https://example_org/api".to_string())
        );
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

    #[test]
    fn test_empty_value_error() {
        assert!(CapUrn::from_string("cap:key=").is_err());
        assert!(CapUrn::from_string("cap:key=;other=value").is_err());
    }

    #[test]
    fn test_has_tag_case_sensitive() {
        let cap = CapUrn::from_string(r#"cap:key="Value""#).unwrap();

        // Exact case match works
        assert!(cap.has_tag("key", "Value"));

        // Different case does not match
        assert!(!cap.has_tag("key", "value"));
        assert!(!cap.has_tag("key", "VALUE"));

        // Key lookup is case-insensitive
        assert!(cap.has_tag("KEY", "Value"));
        assert!(cap.has_tag("Key", "Value"));
    }

    #[test]
    fn test_with_tag_preserves_value() {
        let cap = CapUrn::empty().with_tag("key".to_string(), "ValueWithCase".to_string());
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
    }

    #[test]
    fn test_semantic_equivalence() {
        // Unquoted and quoted simple lowercase values are equivalent
        let unquoted = CapUrn::from_string("cap:key=simple").unwrap();
        let quoted = CapUrn::from_string(r#"cap:key="simple""#).unwrap();
        assert_eq!(unquoted, quoted);

        // Both serialize the same way (unquoted)
        assert_eq!(unquoted.to_string(), "cap:key=simple");
        assert_eq!(quoted.to_string(), "cap:key=simple");
    }

    // ============================================================================
    // MATCHING SEMANTICS SPECIFICATION TESTS
    // These 9 tests verify the exact matching semantics from RULES.md Sections 12-17
    // All implementations (Rust, Go, JS, ObjC) must pass these identically
    // ============================================================================

    #[test]
    fn test_matching_semantics_test1_exact_match() {
        // Test 1: Exact match
        // Cap:     cap:op=generate;ext=pdf
        // Request: cap:op=generate;ext=pdf
        // Result:  MATCH
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 1: Exact match should succeed");
    }

    #[test]
    fn test_matching_semantics_test2_cap_missing_tag() {
        // Test 2: Cap missing tag (implicit wildcard)
        // Cap:     cap:op=generate
        // Request: cap:op=generate;ext=pdf
        // Result:  MATCH (cap can handle any ext)
        let cap = CapUrn::from_string("cap:op=generate").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 2: Cap missing tag should match (implicit wildcard)");
    }

    #[test]
    fn test_matching_semantics_test3_cap_has_extra_tag() {
        // Test 3: Cap has extra tag
        // Cap:     cap:op=generate;ext=pdf;version=2
        // Request: cap:op=generate;ext=pdf
        // Result:  MATCH (request doesn't constrain version)
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf;version=2").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 3: Cap with extra tag should match");
    }

    #[test]
    fn test_matching_semantics_test4_request_has_wildcard() {
        // Test 4: Request has wildcard
        // Cap:     cap:op=generate;ext=pdf
        // Request: cap:op=generate;ext=*
        // Result:  MATCH (request accepts any ext)
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=*").unwrap();
        assert!(cap.matches(&request), "Test 4: Request wildcard should match");
    }

    #[test]
    fn test_matching_semantics_test5_cap_has_wildcard() {
        // Test 5: Cap has wildcard
        // Cap:     cap:op=generate;ext=*
        // Request: cap:op=generate;ext=pdf
        // Result:  MATCH (cap handles any ext)
        let cap = CapUrn::from_string("cap:op=generate;ext=*").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 5: Cap wildcard should match");
    }

    #[test]
    fn test_matching_semantics_test6_value_mismatch() {
        // Test 6: Value mismatch
        // Cap:     cap:op=generate;ext=pdf
        // Request: cap:op=generate;ext=docx
        // Result:  NO MATCH
        let cap = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=docx").unwrap();
        assert!(!cap.matches(&request), "Test 6: Value mismatch should not match");
    }

    #[test]
    fn test_matching_semantics_test7_fallback_pattern() {
        // Test 7: Fallback pattern
        // Cap:     cap:op=generate_thumbnail;out=std:binary.v1
        // Request: cap:op=generate_thumbnail;out=std:binary.v1;ext=wav
        // Result:  MATCH (cap has implicit ext=*)
        let cap = CapUrn::from_string("cap:op=generate_thumbnail;out=std:binary.v1").unwrap();
        let request = CapUrn::from_string("cap:op=generate_thumbnail;out=std:binary.v1;ext=wav").unwrap();
        assert!(cap.matches(&request), "Test 7: Fallback pattern should match (cap missing ext = implicit wildcard)");
    }

    #[test]
    fn test_matching_semantics_test8_empty_cap_matches_anything() {
        // Test 8: Empty cap matches anything
        // Cap:     cap:
        // Request: cap:op=generate;ext=pdf
        // Result:  MATCH
        let cap = CapUrn::from_string("cap:").unwrap();
        let request = CapUrn::from_string("cap:op=generate;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 8: Empty cap should match anything");
    }

    #[test]
    fn test_matching_semantics_test9_cross_dimension_independence() {
        // Test 9: Cross-dimension independence
        // Cap:     cap:op=generate
        // Request: cap:ext=pdf
        // Result:  MATCH (both have implicit wildcards for missing tags)
        let cap = CapUrn::from_string("cap:op=generate").unwrap();
        let request = CapUrn::from_string("cap:ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 9: Cross-dimension independence should match");
    }
}
