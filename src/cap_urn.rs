//! Flat Tag-Based Cap Identifier System
//!
//! This module provides a flat, tag-based cap URN system that replaces
//! hierarchical naming with key-value tags to handle cross-cutting concerns and
//! multi-dimensional cap classification.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// A cap URN using flat, ordered tags with required direction specifiers
///
/// Direction (in→out) is integral to a cap's identity. The `in_spec` and `out_spec`
/// fields specify the input and output media spec IDs respectively.
///
/// Examples:
/// - `cap:in=std:binary.v1;op=generate;out=std:binary.v1;target=thumbnail`
/// - `cap:in=std:void.v1;op=dimensions;out=std:int.v1`
/// - `cap:in=std:str.v1;out=std:obj.v1;key="Value With Spaces"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapUrn {
    /// Input media spec ID - required (use std:void.v1 for caps with no input)
    pub in_spec: String,
    /// Output media spec ID - required
    pub out_spec: String,
    /// Additional tags that define this cap, stored in sorted order for canonical representation
    /// Note: 'in' and 'out' are NOT stored here - they are in in_spec/out_spec
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
    /// Create a new cap URN from direction specs and additional tags
    /// Keys are normalized to lowercase; values are preserved as-is
    /// in_spec and out_spec are required direction specifiers
    pub fn new(in_spec: String, out_spec: String, tags: BTreeMap<String, String>) -> Self {
        let mut normalized_tags: BTreeMap<String, String> = tags
            .into_iter()
            .filter(|(k, _)| k.to_lowercase() != "in" && k.to_lowercase() != "out")
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        // Ensure in and out are not in tags
        normalized_tags.remove("in");
        normalized_tags.remove("out");
        Self {
            in_spec,
            out_spec,
            tags: normalized_tags,
        }
    }

    /// Create a cap URN from tags map that must contain 'in' and 'out'
    /// This is a convenience method for TOML deserialization
    pub fn from_tags(tags: BTreeMap<String, String>) -> Result<Self, CapUrnError> {
        let mut tags = tags;
        let in_spec = tags.remove("in").ok_or(CapUrnError::MissingInSpec)?;
        let out_spec = tags.remove("out").ok_or(CapUrnError::MissingOutSpec)?;
        Ok(Self::new(in_spec, out_spec, tags))
    }

    /// Create a cap URN from a string representation
    ///
    /// Format: `cap:in=spec;out=spec;key1=value1;...` or `cap:in=spec;key="value";out=spec`
    /// The "cap:" prefix is mandatory
    /// The 'in' and 'out' tags are REQUIRED (direction is part of cap identity)
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

        // Handle empty cap URN (cap: with no tags) - this is now an error since in/out are required
        if tags_part.is_empty() || tags_part == ";" {
            return Err(CapUrnError::MissingInSpec);
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

        // Extract required in and out specs
        let in_spec = tags.remove("in").ok_or(CapUrnError::MissingInSpec)?;
        let out_spec = tags.remove("out").ok_or(CapUrnError::MissingOutSpec)?;

        Ok(Self { in_spec, out_spec, tags })
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
    /// All tags (including in/out) are sorted alphabetically
    /// No trailing semicolon in canonical form
    /// Values are quoted only when necessary (smart quoting)
    pub fn to_string(&self) -> String {
        // Build a full sorted list of all tags including in/out
        let mut all_tags: Vec<(&str, &str)> = Vec::new();

        // Add in_spec and out_spec as tags
        all_tags.push(("in", &self.in_spec));
        all_tags.push(("out", &self.out_spec));

        // Add all other tags
        for (k, v) in &self.tags {
            all_tags.push((k.as_str(), v.as_str()));
        }

        // Sort alphabetically by key
        all_tags.sort_by(|a, b| a.0.cmp(b.0));

        // Build the string
        let parts: Vec<String> = all_tags
            .into_iter()
            .map(|(k, v)| {
                if Self::needs_quoting(v) {
                    format!("{}={}", k, Self::quote_value(v))
                } else {
                    format!("{}={}", k, v)
                }
            })
            .collect();

        format!("cap:{}", parts.join(";"))
    }

    /// Get a specific tag value
    /// Key is normalized to lowercase for lookup
    /// For 'in' and 'out', returns the direction spec fields
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => Some(&self.in_spec),
            "out" => Some(&self.out_spec),
            _ => self.tags.get(&key_lower),
        }
    }

    /// Get the input spec ID
    pub fn in_spec(&self) -> &str {
        &self.in_spec
    }

    /// Get the output spec ID
    pub fn out_spec(&self) -> &str {
        &self.out_spec
    }

    /// Check if this cap has a specific tag with a specific value
    /// Key is normalized to lowercase; value comparison is case-sensitive
    /// For 'in' and 'out', checks the direction spec fields
    pub fn has_tag(&self, key: &str, value: &str) -> bool {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "in" => self.in_spec == value,
            "out" => self.out_spec == value,
            _ => self.tags.get(&key_lower).map_or(false, |v| v == value),
        }
    }

    /// Add or update a tag
    /// Key is normalized to lowercase; value is preserved as-is
    /// Note: Cannot modify 'in' or 'out' tags - use with_in_spec/with_out_spec
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        let key_lower = key.to_lowercase();
        if key_lower == "in" || key_lower == "out" {
            // Silently ignore attempts to set in/out via with_tag
            // Use with_in_spec/with_out_spec instead
            return self;
        }
        self.tags.insert(key_lower, value);
        self
    }

    /// Create a new cap URN with a different input spec
    pub fn with_in_spec(mut self, in_spec: String) -> Self {
        self.in_spec = in_spec;
        self
    }

    /// Create a new cap URN with a different output spec
    pub fn with_out_spec(mut self, out_spec: String) -> Self {
        self.out_spec = out_spec;
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
        // Check in_spec
        if self.in_spec != "*" && request.in_spec != "*" && self.in_spec != request.in_spec {
            return false;
        }

        // Check out_spec
        if self.out_spec != "*" && request.out_spec != "*" && self.out_spec != request.out_spec {
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
        if self.in_spec != "*" {
            count += 1;
        }
        if self.out_spec != "*" {
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
        // Check in_spec compatibility
        if self.in_spec != "*" && other.in_spec != "*" && self.in_spec != other.in_spec {
            return false;
        }

        // Check out_spec compatibility
        if self.out_spec != "*" && other.out_spec != "*" && self.out_spec != other.out_spec {
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
                self.in_spec = "*".to_string();
            }
            "out" => {
                self.out_spec = "*".to_string();
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
            in_spec: self.in_spec.clone(),
            out_spec: self.out_spec.clone(),
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
            in_spec: other.in_spec.clone(),
            out_spec: other.out_spec.clone(),
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
                write!(f, "Cap URN is missing required 'in' tag - caps must declare their input type (use std:void.v1 for no input)")
            }
            CapUrnError::MissingOutSpec => {
                write!(f, "Cap URN is missing required 'out' tag - caps must declare their output type")
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
    in_spec: Option<String>,
    out_spec: Option<String>,
    tags: BTreeMap<String, String>,
}

impl CapUrnBuilder {
    pub fn new() -> Self {
        Self {
            in_spec: None,
            out_spec: None,
            tags: BTreeMap::new(),
        }
    }

    /// Set the input spec ID (required)
    pub fn in_spec(mut self, spec: &str) -> Self {
        self.in_spec = Some(spec.to_string());
        self
    }

    /// Set the output spec ID (required)
    pub fn out_spec(mut self, spec: &str) -> Self {
        self.out_spec = Some(spec.to_string());
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

    pub fn build(self) -> Result<CapUrn, CapUrnError> {
        let in_spec = self.in_spec.ok_or(CapUrnError::MissingInSpec)?;
        let out_spec = self.out_spec.ok_or(CapUrnError::MissingOutSpec)?;
        Ok(CapUrn::new(in_spec, out_spec, self.tags))
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
        format!("cap:in=std:void.v1;out=std:obj.v1;{}", tags)
    }

    fn test_urn_with_io(in_spec: &str, out_spec: &str, tags: &str) -> String {
        if tags.is_empty() {
            format!("cap:in={};out={}", in_spec, out_spec)
        } else {
            format!("cap:in={};out={};{}", in_spec, out_spec, tags)
        }
    }

    #[test]
    fn test_cap_urn_creation() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.get_tag("target"), Some(&"thumbnail".to_string()));
        assert_eq!(cap.get_tag("ext"), Some(&"pdf".to_string()));
        // Direction specs are required and accessible
        assert_eq!(cap.in_spec(), "std:void.v1");
        assert_eq!(cap.out_spec(), "std:obj.v1");
    }

    #[test]
    fn test_direction_specs_required() {
        // Missing 'in' should fail
        let result = CapUrn::from_string("cap:out=std:obj.v1;op=test");
        assert!(result.is_err());
        assert!(matches!(result, Err(CapUrnError::MissingInSpec)));

        // Missing 'out' should fail
        let result = CapUrn::from_string("cap:in=std:void.v1;op=test");
        assert!(result.is_err());
        assert!(matches!(result, Err(CapUrnError::MissingOutSpec)));

        // Both present should succeed
        let result = CapUrn::from_string("cap:in=std:void.v1;out=std:obj.v1;op=test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_direction_matching() {
        // Direction specs must match for caps to match
        let cap1 = CapUrn::from_string("cap:in=std:str.v1;out=std:obj.v1;op=test").unwrap();
        let cap2 = CapUrn::from_string("cap:in=std:str.v1;out=std:obj.v1;op=test").unwrap();
        assert!(cap1.matches(&cap2));

        // Different in_spec should not match
        let cap3 = CapUrn::from_string("cap:in=std:binary.v1;out=std:obj.v1;op=test").unwrap();
        assert!(!cap1.matches(&cap3));

        // Different out_spec should not match
        let cap4 = CapUrn::from_string("cap:in=std:str.v1;out=std:int.v1;op=test").unwrap();
        assert!(!cap1.matches(&cap4));

        // Wildcard in direction should match
        let cap5 = CapUrn::from_string("cap:in=*;out=std:obj.v1;op=test").unwrap();
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
        assert_eq!(cap.get_tag("key"), Some(&"value;with;semicolons".to_string()));

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
        assert_eq!(cap3.get_tag("key"), Some(&r#"say "hello\world""#.to_string()));
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
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
            .tag("key", "simple")
            .build()
            .unwrap();
        assert_eq!(cap.to_string(), "cap:in=std:void.v1;key=simple;out=std:obj.v1");

        // Value with spaces - needs quoting
        let cap2 = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
            .tag("key", "has spaces")
            .build()
            .unwrap();
        assert_eq!(cap2.to_string(), r#"cap:in=std:void.v1;key="has spaces";out=std:obj.v1"#);

        // Value with uppercase - needs quoting to preserve
        let cap4 = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
            .tag("key", "HasUpper")
            .build()
            .unwrap();
        assert_eq!(cap4.to_string(), r#"cap:in=std:void.v1;key="HasUpper";out=std:obj.v1"#);
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
        assert_eq!(reparsed.get_tag("key"), Some(&"Value With Spaces".to_string()));
    }

    #[test]
    fn test_round_trip_escapes() {
        let original = test_urn(r#"key="value\"with\\escapes""#);
        let cap = CapUrn::from_string(&original).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&r#"value"with\escapes"#.to_string()));
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    #[test]
    fn test_cap_prefix_required() {
        // Missing cap: prefix should fail
        assert!(CapUrn::from_string("in=std:void.v1;out=std:obj.v1;op=generate").is_err());

        // Valid cap: prefix should work
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));

        // Case-insensitive prefix
        let cap2 = CapUrn::from_string("CAP:in=std:void.v1;out=std:obj.v1;op=generate").unwrap();
        assert_eq!(cap2.get_tag("op"), Some(&"generate".to_string()));
    }

    #[test]
    fn test_trailing_semicolon_equivalence() {
        // Both with and without trailing semicolon should be equivalent
        let cap1 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let cap2 = CapUrn::from_string(&format!("{};", test_urn("op=generate;ext=pdf"))).unwrap();

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
        let cap = CapUrn::from_string(&test_urn("op=generate;target=thumbnail;ext=pdf")).unwrap();
        // Should be sorted alphabetically with in/out in their sorted positions
        // Alphabetical order: ext < in < op < out < target
        assert_eq!(
            cap.to_string(),
            "cap:ext=pdf;in=std:void.v1;op=generate;out=std:obj.v1;target=thumbnail"
        );
    }

    #[test]
    fn test_tag_matching() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();

        // Exact match
        let request1 = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
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
        let cap4 = CapUrn::from_string("cap:in=*;out=std:obj.v1;op=test").unwrap();
        assert_eq!(cap4.specificity(), 2); // out + op (in wildcard doesn't count)
    }

    #[test]
    fn test_builder() {
        let cap = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
            .tag("op", "generate")
            .tag("target", "thumbnail")
            .tag("ext", "pdf")
            .build()
            .unwrap();

        assert_eq!(cap.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(cap.in_spec(), "std:void.v1");
        assert_eq!(cap.out_spec(), "std:obj.v1");
    }

    #[test]
    fn test_builder_requires_direction() {
        // Missing in_spec should fail
        let result = CapUrnBuilder::new()
            .out_spec("std:obj.v1")
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Missing out_spec should fail
        let result = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .tag("op", "test")
            .build();
        assert!(result.is_err());

        // Both present should succeed
        let result = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_preserves_case() {
        let cap = CapUrnBuilder::new()
            .in_spec("std:void.v1")
            .out_spec("std:obj.v1")
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
        let cap5 = CapUrn::from_string("cap:in=std:binary.v1;out=std:obj.v1;op=generate").unwrap();
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
        let cap2 = CapUrn::from_string("cap:in=std:binary.v1;out=std:int.v1;ext=pdf;output=binary").unwrap();

        let merged = cap1.merge(&cap2);
        // Merged takes in/out from cap2
        assert_eq!(merged.in_spec(), "std:binary.v1");
        assert_eq!(merged.out_spec(), "std:int.v1");
        // Has tags from both
        assert_eq!(merged.get_tag("op"), Some(&"generate".to_string()));
        assert_eq!(merged.get_tag("ext"), Some(&"pdf".to_string()));

        let subset = merged.subset(&["type", "ext"]);
        // subset keeps in/out from merged
        assert_eq!(subset.in_spec(), "std:binary.v1");
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
        let cap = CapUrn::from_string("cap:in=std:void.v1;out=std:obj.v1").unwrap();
        assert_eq!(cap.in_spec(), "std:void.v1");
        assert_eq!(cap.out_spec(), "std:obj.v1");
        assert!(cap.tags.is_empty());
    }

    #[test]
    fn test_extended_character_support() {
        // Test forward slashes and colons in tag components
        let cap = CapUrn::from_string(&test_urn("url=https://example_org/api;path=/some/file")).unwrap();
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
        assert!(cap.has_tag("in", "std:void.v1"));
        assert!(cap.has_tag("out", "std:obj.v1"));
    }

    #[test]
    fn test_with_tag_preserves_value() {
        let cap = CapUrn::new(
            "std:void.v1".to_string(),
            "std:obj.v1".to_string(),
            BTreeMap::new()
        ).with_tag("key".to_string(), "ValueWithCase".to_string());
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
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
        let cap = CapUrn::from_string("cap:in=std:str.v1;out=std:int.v1;op=test").unwrap();

        // get_tag works for in/out
        assert_eq!(cap.get_tag("in"), Some(&"std:str.v1".to_string()));
        assert_eq!(cap.get_tag("out"), Some(&"std:int.v1".to_string()));
        assert_eq!(cap.get_tag("op"), Some(&"test".to_string()));

        // Case-insensitive lookup for in/out
        assert_eq!(cap.get_tag("IN"), Some(&"std:str.v1".to_string()));
        assert_eq!(cap.get_tag("OUT"), Some(&"std:int.v1".to_string()));
    }

    // ============================================================================
    // MATCHING SEMANTICS SPECIFICATION TESTS
    // These tests verify the exact matching semantics from RULES.md Sections 12-17
    // All implementations (Rust, Go, JS, ObjC) must pass these identically
    // Note: All tests now require in/out direction specs
    // ============================================================================

    #[test]
    fn test_matching_semantics_test1_exact_match() {
        // Test 1: Exact match
        // Cap:     cap:in=X;out=Y;op=generate;ext=pdf
        // Request: cap:in=X;out=Y;op=generate;ext=pdf
        // Result:  MATCH
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 1: Exact match should succeed");
    }

    #[test]
    fn test_matching_semantics_test2_cap_missing_tag() {
        // Test 2: Cap missing tag (implicit wildcard for other tags, not direction)
        // Cap:     cap:in=X;out=Y;op=generate
        // Request: cap:in=X;out=Y;op=generate;ext=pdf
        // Result:  MATCH (cap can handle any ext)
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 2: Cap missing tag should match (implicit wildcard)");
    }

    #[test]
    fn test_matching_semantics_test3_cap_has_extra_tag() {
        // Test 3: Cap has extra tag
        // Cap:     cap:in=X;out=Y;op=generate;ext=pdf;version=2
        // Request: cap:in=X;out=Y;op=generate;ext=pdf
        // Result:  MATCH (request doesn't constrain version)
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;version=2")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 3: Cap with extra tag should match");
    }

    #[test]
    fn test_matching_semantics_test4_request_has_wildcard() {
        // Test 4: Request has wildcard
        // Cap:     cap:in=X;out=Y;op=generate;ext=pdf
        // Request: cap:in=X;out=Y;op=generate;ext=*
        // Result:  MATCH (request accepts any ext)
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        assert!(cap.matches(&request), "Test 4: Request wildcard should match");
    }

    #[test]
    fn test_matching_semantics_test5_cap_has_wildcard() {
        // Test 5: Cap has wildcard
        // Cap:     cap:in=X;out=Y;op=generate;ext=*
        // Request: cap:in=X;out=Y;op=generate;ext=pdf
        // Result:  MATCH (cap handles any ext)
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 5: Cap wildcard should match");
    }

    #[test]
    fn test_matching_semantics_test6_value_mismatch() {
        // Test 6: Value mismatch
        // Cap:     cap:in=X;out=Y;op=generate;ext=pdf
        // Request: cap:in=X;out=Y;op=generate;ext=docx
        // Result:  NO MATCH
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=docx")).unwrap();
        assert!(!cap.matches(&request), "Test 6: Value mismatch should not match");
    }

    #[test]
    fn test_matching_semantics_test7_fallback_pattern() {
        // Test 7: Fallback pattern
        // Cap:     cap:in=std:binary.v1;out=std:binary.v1;op=generate_thumbnail
        // Request: cap:in=std:binary.v1;out=std:binary.v1;op=generate_thumbnail;ext=wav
        // Result:  MATCH (cap has implicit ext=*)
        let cap = CapUrn::from_string("cap:in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1").unwrap();
        let request = CapUrn::from_string("cap:ext=wav;in=std:binary.v1;op=generate_thumbnail;out=std:binary.v1").unwrap();
        assert!(cap.matches(&request), "Test 7: Fallback pattern should match (cap missing ext = implicit wildcard)");
    }

    #[test]
    fn test_matching_semantics_test7b_thumbnail_void_input() {
        // Test 7b: Thumbnail fallback with std:void.v1 input (real-world scenario)
        // Cap:     cap:in=std:void.v1;op=generate_thumbnail;out=std:binary.v1
        // Request: cap:ext=wav;in=std:void.v1;op=generate_thumbnail;out=std:binary.v1
        // Result:  MATCH (cap has implicit ext=*)
        let cap = CapUrn::from_string("cap:in=std:void.v1;op=generate_thumbnail;out=std:binary.v1").unwrap();
        let request = CapUrn::from_string("cap:ext=wav;in=std:void.v1;op=generate_thumbnail;out=std:binary.v1").unwrap();
        assert!(cap.matches(&request), "Test 7b: Thumbnail fallback with void input should match");
    }

    #[test]
    fn test_matching_semantics_test8_wildcard_direction_matches_anything() {
        // Test 8: Wildcard direction matches anything (replaces empty cap test)
        // Cap:     cap:in=*;out=*
        // Request: cap:in=std:str.v1;out=std:obj.v1;op=generate;ext=pdf
        // Result:  MATCH (wildcard in/out accepts any direction)
        let cap = CapUrn::from_string("cap:in=*;out=*").unwrap();
        let request = CapUrn::from_string("cap:in=std:str.v1;op=generate;out=std:obj.v1;ext=pdf").unwrap();
        assert!(cap.matches(&request), "Test 8: Wildcard direction should match any direction");
    }

    #[test]
    fn test_matching_semantics_test9_cross_dimension_independence() {
        // Test 9: Cross-dimension independence (for other tags)
        // Cap:     cap:in=X;out=Y;op=generate
        // Request: cap:in=X;out=Y;ext=pdf
        // Result:  MATCH (both have implicit wildcards for missing tags)
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        assert!(cap.matches(&request), "Test 9: Cross-dimension independence should match");
    }

    #[test]
    fn test_matching_semantics_test10_direction_mismatch() {
        // Test 10: Direction mismatch prevents matching
        // Cap:     cap:in=std:str.v1;out=std:obj.v1;op=generate
        // Request: cap:in=std:binary.v1;out=std:obj.v1;op=generate
        // Result:  NO MATCH (direction must match)
        let cap = CapUrn::from_string("cap:in=std:str.v1;op=generate;out=std:obj.v1").unwrap();
        let request = CapUrn::from_string("cap:in=std:binary.v1;op=generate;out=std:obj.v1").unwrap();
        assert!(!cap.matches(&request), "Test 10: Direction mismatch should not match");
    }
}
