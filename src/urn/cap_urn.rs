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

use crate::urn::media_urn::{MediaUrn, MediaUrnError, MEDIA_VOID, MEDIA_OBJECT};

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
    ///
    /// **Wildcard expansion for in/out tags:**
    /// - Missing `in` or `out` tag → defaults to `media:`
    /// - `in` or `out` without `=` → becomes `media:` (TaggedUrn treats `tag` as `tag=*`, we replace `*` with `media:`)
    /// - `in=*` or `out=*` → replaced with `media:`
    /// - `cap:` → `cap:in=media:;out=media:`
    /// - `cap:in` → `cap:in=media:;out=media:`
    /// - `cap:in=media:bytes;out` → `cap:in=media:bytes;out=media:`
    ///
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

        // Process in and out tags with wildcard expansion
        // Missing tag or tag=* → "media:" (the wildcard)
        let in_urn = Self::process_direction_tag(&tagged, "in")?;
        let out_urn = Self::process_direction_tag(&tagged, "out")?;

        // Validate that in and out specs are valid media URNs (or wildcard "media:")
        // After processing, "media:" is the wildcard (not "*")
        use crate::urn::media_urn::MediaUrn;
        if in_urn != "media:" {
            MediaUrn::from_string(&in_urn)
                .map_err(|e| CapUrnError::InvalidInSpec(format!("Invalid media URN for in spec '{}': {}", in_urn, e)))?;
        }
        if out_urn != "media:" {
            MediaUrn::from_string(&out_urn)
                .map_err(|e| CapUrnError::InvalidOutSpec(format!("Invalid media URN for out spec '{}': {}", out_urn, e)))?;
        }

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

    /// Process a direction tag (in or out) with wildcard expansion
    ///
    /// - Missing tag → "media:" (wildcard)
    /// - tag=* → "media:" (wildcard)
    /// - tag= (empty) → error
    /// - tag=value → value (validated later)
    fn process_direction_tag(tagged: &TaggedUrn, tag_name: &str) -> Result<String, CapUrnError> {
        match tagged.tags.get(tag_name) {
            Some(value) => {
                if value == "*" {
                    // Replace * with media: wildcard
                    Ok("media:".to_string())
                } else if value.is_empty() {
                    // Empty value is not allowed (in= or out= with nothing after =)
                    if tag_name == "in" {
                        Err(CapUrnError::InvalidInSpec("Empty value for 'in' tag is not allowed".to_string()))
                    } else {
                        Err(CapUrnError::InvalidOutSpec("Empty value for 'out' tag is not allowed".to_string()))
                    }
                } else {
                    // Regular value - will be validated as MediaUrn later
                    Ok(value.clone())
                }
            }
            None => {
                // Tag is missing - default to media: wildcard
                Ok("media:".to_string())
            }
        }
    }

    /// Get the canonical string representation of this cap URN
    ///
    /// Always includes "cap:" prefix
    /// All tags (including in/out) are sorted alphabetically
    /// No trailing semicolon in canonical form
    /// Values are quoted only when necessary (smart quoting via TaggedUrn)
    /// Build a TaggedUrn representation of this CapUrn (internal helper)
    fn build_tagged_urn(&self) -> TaggedUrn {
        let mut builder = TaggedUrnBuilder::new(Self::PREFIX)
            .tag("in", &self.in_urn).expect("in_urn guaranteed non-empty")
            .tag("out", &self.out_urn).expect("out_urn guaranteed non-empty");

        for (k, v) in &self.tags {
            // Tags are validated at construction time
            builder = builder.tag(k, v).expect("tag values validated at construction");
        }

        // Use build_allow_empty which returns TaggedUrn directly
        builder.build_allow_empty()
    }

    /// Serialize just the tags portion (without "cap:" prefix)
    ///
    /// Returns tags in canonical form with proper quoting and sorting.
    pub fn tags_to_string(&self) -> String {
        self.build_tagged_urn().tags_to_string()
    }

    pub fn to_string(&self) -> String {
        self.build_tagged_urn().to_string()
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

    /// Check if this cap (pattern/handler) accepts the given request (instance).
    ///
    /// Direction specs use semantic TaggedUrn matching via MediaUrn:
    /// - Input: `cap_in.accepts(request_in)` — does request's data satisfy cap's input requirement?
    /// - Output: `request_out.accepts(cap_out)` — does cap's output satisfy what request expects?
    ///
    /// For other tags: cap satisfies request's tag constraints.
    /// Missing cap tags are wildcards (cap accepts any value for that tag).
    pub fn accepts(&self, request: &CapUrn) -> bool {
        // Input direction: self.in_urn is pattern, request.in_urn is instance
        // "media:" on the PATTERN side means "I accept any input" — skip check.
        // "media:" on the INSTANCE side is just the least specific — still check.
        if self.in_urn != "media:" {
            let cap_in = MediaUrn::from_string(&self.in_urn)
                .unwrap_or_else(|e| panic!("CU2: cap in_spec '{}' is not a valid MediaUrn: {}", self.in_urn, e));
            let request_in = MediaUrn::from_string(&request.in_urn)
                .unwrap_or_else(|e| panic!("CU2: request in_spec '{}' is not a valid MediaUrn: {}", request.in_urn, e));
            if !cap_in.accepts(&request_in)
                .expect("CU2: media URN prefix mismatch in direction spec matching") {
                return false;
            }
        }

        // Output direction: self.out_urn is pattern, request.out_urn is instance
        // "media:" on the PATTERN side means "I accept any output" — skip check.
        // "media:" on the INSTANCE side is just the least specific — still check.
        if self.out_urn != "media:" {
            let cap_out = MediaUrn::from_string(&self.out_urn)
                .unwrap_or_else(|e| panic!("CU2: cap out_spec '{}' is not a valid MediaUrn: {}", self.out_urn, e));
            let request_out = MediaUrn::from_string(&request.out_urn)
                .unwrap_or_else(|e| panic!("CU2: request out_spec '{}' is not a valid MediaUrn: {}", request.out_urn, e));
            if !cap_out.conforms_to(&request_out)
                .expect("CU2: media URN prefix mismatch in direction spec matching") {
                return false;
            }
        }

        // Check all tags that the pattern (self) requires.
        // The instance (request param) must satisfy every pattern constraint.
        // Missing tag in instance → instance doesn't satisfy constraint → reject.
        for (self_key, self_value) in &self.tags {
            match request.tags.get(self_key) {
                Some(req_value) => {
                    if self_value == "*" { continue; }
                    if req_value == "*" { continue; }
                    if self_value != req_value { return false; }
                }
                None => { return false; } // Instance missing a tag the pattern requires
            }
        }

        true
    }

    /// Check if this request conforms to (can be handled by) the given cap.
    /// Equivalent to `cap.accepts(self)`.
    pub fn conforms_to(&self, cap: &CapUrn) -> bool {
        cap.accepts(self)
    }

    pub fn accepts_str(&self, request_str: &str) -> Result<bool, CapUrnError> {
        let request = CapUrn::from_string(request_str)?;
        Ok(self.accepts(&request))
    }

    pub fn conforms_to_str(&self, cap_str: &str) -> Result<bool, CapUrnError> {
        let cap = CapUrn::from_string(cap_str)?;
        Ok(self.conforms_to(&cap))
    }

    /// Calculate specificity score for cap matching
    ///
    /// More specific caps have higher scores and are preferred.
    /// Direction specs contribute their MediaUrn tag count (more tags = more specific).
    /// Other tags contribute 1 per non-wildcard value.
    pub fn specificity(&self) -> usize {
        let mut count = 0;
        // "media:" is the wildcard (contributes 0 to specificity)
        if self.in_urn != "media:" {
            let in_media = MediaUrn::from_string(&self.in_urn)
                .unwrap_or_else(|e| panic!("CU2: in_spec '{}' is not a valid MediaUrn: {}", self.in_urn, e));
            count += in_media.inner().tags.len();
        }
        if self.out_urn != "media:" {
            let out_media = MediaUrn::from_string(&self.out_urn)
                .unwrap_or_else(|e| panic!("CU2: out_spec '{}' is not a valid MediaUrn: {}", self.out_urn, e));
            count += out_media.inner().tags.len();
        }
        // Count non-wildcard tags
        count + self.tags.values().filter(|v| v.as_str() != "*").count()
    }

    /// Check if this cap is more specific than another
    ///
    /// Compares specificity scores. Only meaningful when both caps
    /// already matched the same request.
    pub fn is_more_specific_than(&self, other: &CapUrn) -> bool {
        self.specificity() > other.specificity()
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
    /// Error code 13: Invalid media URN in 'in' spec
    InvalidInSpec(String),
    /// Error code 14: Invalid media URN in 'out' spec
    InvalidOutSpec(String),
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
            TaggedUrnError::WhitespaceInInput(s) => CapUrnError::InvalidCharacter(s),
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
            CapUrnError::InvalidInSpec(msg) => {
                write!(f, "Invalid 'in' spec: {}", msg)
            }
            CapUrnError::InvalidOutSpec(msg) => {
                write!(f, "Invalid 'out' spec: {}", msg)
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
    /// Find the most specific cap that accepts a request
    pub fn find_best_match<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Option<&'a CapUrn> {
        caps.iter()
            .filter(|cap| request.accepts(cap))
            .max_by_key(|cap| cap.specificity())
    }

    /// Find all caps that match a request, sorted by specificity
    pub fn find_all_matches<'a>(caps: &'a [CapUrn], request: &CapUrn) -> Vec<&'a CapUrn> {
        let mut matches: Vec<&CapUrn> = caps.iter().filter(|cap| request.accepts(cap)).collect();

        // Sort by specificity (most specific first)
        matches.sort_by_key(|cap| std::cmp::Reverse(cap.specificity()));
        matches
    }

    /// Check if two cap sets overlap (any pair where one accepts the other)
    pub fn are_compatible(caps1: &[CapUrn], caps2: &[CapUrn]) -> bool {
        caps1
            .iter()
            .any(|c1| caps2.iter().any(|c2| c1.accepts(c2) || c2.accepts(c1)))
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

    // TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible
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

    // TEST002: Test that missing 'in' or 'out' defaults to media: wildcard
    #[test]
    fn test_direction_specs_default_to_wildcard() {
        // Missing 'in' defaults to media:
        let cap = CapUrn::from_string(&format!("cap:out=\"{}\";op=test", MEDIA_OBJECT))
            .expect("Missing in should default to media:");
        assert_eq!(cap.in_spec(), "media:");
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);

        // Missing 'out' defaults to media:
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";op=test", MEDIA_VOID))
            .expect("Missing out should default to media:");
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), "media:");

        // Both present should succeed
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";out=\"{}\";op=test", MEDIA_VOID, MEDIA_OBJECT))
            .expect("Both specs present should succeed");
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
    }

    // TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any
    #[test]
    fn test_direction_matching() {
        let in_str = "media:string";
        let out_obj = "media:object";
        let in_bin = "media:binary";
        let out_int = "media:integer";

        // Direction specs must match for caps to match
        let cap1 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_obj)).unwrap();
        let cap2 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_obj)).unwrap();
        assert!(cap1.accepts(&cap2));

        // Different in_urn should not match
        let cap3 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_bin, out_obj)).unwrap();
        assert!(!cap1.accepts(&cap3));

        // Different out_urn should not match
        let cap4 = CapUrn::from_string(&format!("cap:in=\"{}\";op=test;out=\"{}\"", in_str, out_int)).unwrap();
        assert!(!cap1.accepts(&cap4));

        // Wildcard in=* direction: cap5 has media: for in, specific for out
        let cap5 = CapUrn::from_string(&format!("cap:in=*;op=test;out=\"{}\"", out_obj)).unwrap();
        // cap1 (specific in) as pattern rejects cap5 (bare media: in) — specific pattern doesn't accept broad instance
        assert!(!cap1.accepts(&cap5));
        // cap5 (wildcard in) as pattern accepts cap1 (specific in) — wildcard pattern accepts anything
        assert!(cap5.accepts(&cap1));
    }

    // TEST004: Test that unquoted keys and values are normalized to lowercase
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

    // TEST005: Test that quoted values preserve case while unquoted are lowercased
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

    // TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces)
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

    // TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly
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

    // TEST008: Test that mixed quoted and unquoted values in same URN parse correctly
    #[test]
    fn test_mixed_quoted_unquoted() {
        let cap = CapUrn::from_string(&test_urn(r#"a="Quoted";b=simple"#)).unwrap();
        assert_eq!(cap.get_tag("a"), Some(&"Quoted".to_string()));
        assert_eq!(cap.get_tag("b"), Some(&"simple".to_string()));
    }

    // TEST009: Test that unterminated quote produces UnterminatedQuote error
    #[test]
    fn test_unterminated_quote_error() {
        let result = CapUrn::from_string(&test_urn(r#"key="unterminated"#));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::UnterminatedQuote(_)));
        }
    }

    // TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error
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

    // TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase)
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

    // TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original)
    #[test]
    fn test_round_trip_simple() {
        let original = test_urn("op=generate;ext=pdf");
        let cap = CapUrn::from_string(&original).unwrap();
        let serialized = cap.to_string();
        let reparsed = CapUrn::from_string(&serialized).unwrap();
        assert_eq!(cap, reparsed);
    }

    // TEST013: Test that quoted values round-trip preserving case and spaces
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

    // TEST014: Test that escape sequences round-trip correctly
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

    // TEST015: Test that cap: prefix is required and case-insensitive
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

    // TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches)
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
        assert!(cap1.accepts(&cap2));
        assert!(cap2.accepts(&cap1));
    }

    // TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch
    #[test]
    fn test_tag_matching() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();

        // Exact match — both directions accept
        let request1 =
            CapUrn::from_string(&test_urn("op=generate;ext=pdf;target=thumbnail")).unwrap();
        assert!(cap.accepts(&request1));
        assert!(request1.accepts(&cap));

        // Routing direction: request(op=generate) accepts cap(op,ext,target) — request only needs op
        let request2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(request2.accepts(&cap));
        // Reverse: cap(op,ext,target) as pattern rejects request missing ext,target
        assert!(!cap.accepts(&request2));

        // Routing direction: request(ext=*) accepts cap(ext=pdf) — wildcard matches specific
        let request3 = CapUrn::from_string(&test_urn("ext=*")).unwrap();
        assert!(request3.accepts(&cap));

        // Conflicting value — neither direction accepts
        let request4 = CapUrn::from_string(&test_urn("op=extract")).unwrap();
        assert!(!cap.accepts(&request4));
        assert!(!request4.accepts(&cap));
    }

    // TEST018: Test that quoted values with different case do NOT match (case-sensitive)
    #[test]
    fn test_matching_case_sensitive_values() {
        // Values with different case should NOT match
        let cap1 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        let cap2 = CapUrn::from_string(&test_urn(r#"key="value""#)).unwrap();
        assert!(!cap1.accepts(&cap2));
        assert!(!cap2.accepts(&cap1));

        // Same case should match
        let cap3 = CapUrn::from_string(&test_urn(r#"key="Value""#)).unwrap();
        assert!(cap1.accepts(&cap3));
    }

    // TEST019: Missing tag in instance causes rejection — pattern's tags are constraints
    #[test]
    fn test_missing_tag_handling() {
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request1 = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();

        // cap(op) as pattern: instance(ext) missing op → reject
        assert!(!cap.accepts(&request1));
        // request(ext) as pattern: instance(cap) missing ext → reject
        assert!(!request1.accepts(&cap));

        // Routing: request(op) accepts cap(op,ext) — instance has op → match
        let cap2 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(request2.accepts(&cap2));
        // Reverse: cap(op,ext) as pattern rejects request missing ext
        assert!(!cap2.accepts(&request2));
    }

    // TEST020: Test specificity calculation (direction specs use MediaUrn tag count, wildcards don't count)
    #[test]
    fn test_specificity() {
        // Direction specs contribute their MediaUrn tag count:
        // MEDIA_VOID = "media:void" -> 1 tag (void)
        // MEDIA_OBJECT = "media:form=map;textable" -> 2 tags (form, textable)
        let cap1 = CapUrn::from_string(&test_urn("type=general")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("op=*;ext=pdf")).unwrap();

        assert_eq!(cap1.specificity(), 4); // void(1) + object(2) + type(1)
        assert_eq!(cap2.specificity(), 4); // void(1) + object(2) + op(1)
        assert_eq!(cap3.specificity(), 4); // void(1) + object(2) + ext(1) (wildcard op doesn't count)

        // Wildcard in direction doesn't count
        let cap4 =
            CapUrn::from_string(&format!("cap:in=*;out=\"{}\";op=test", MEDIA_OBJECT)).unwrap();
        assert_eq!(cap4.specificity(), 3); // object(2) + op(1) (in wildcard doesn't count)
    }

    // TEST021: Test builder creates cap URN with correct tags and direction specs
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

    // TEST022: Test builder requires both in_spec and out_spec
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

    // TEST023: Test builder lowercases keys but preserves value case
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

    // TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy
    #[test]
    fn test_directional_accepts() {
        let cap1 = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let cap2 = CapUrn::from_string(&test_urn("op=generate;format=*")).unwrap();
        let cap3 = CapUrn::from_string(&test_urn("type=image;op=extract")).unwrap();

        // cap1(op,ext) as pattern: cap2 missing ext → reject
        assert!(!cap1.accepts(&cap2));
        // cap2(op,format) as pattern: cap1 missing format → reject
        assert!(!cap2.accepts(&cap1));
        // op mismatch: neither direction accepts
        assert!(!cap1.accepts(&cap3));
        assert!(!cap3.accepts(&cap1));

        // Routing: general request(op) accepts specific cap(op,ext) — instance has op
        let cap4 = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        assert!(cap4.accepts(&cap1)); // cap4 only requires op, cap1 has it
        // Reverse: specific cap(op,ext) rejects general request missing ext
        assert!(!cap1.accepts(&cap4));

        // Different direction specs: neither accepts the other
        let cap5 = CapUrn::from_string(&format!(
            "cap:in=media:bytes;out=\"{}\";op=generate",
            MEDIA_OBJECT
        ))
        .unwrap();
        assert!(!cap1.accepts(&cap5));
        assert!(!cap5.accepts(&cap1));
    }

    // TEST025: Test find_best_match returns most specific matching cap
    #[test]
    fn test_best_match() {
        let caps = vec![
            CapUrn::from_string(&test_urn("op=*")).unwrap(),
            CapUrn::from_string(&test_urn("op=generate")).unwrap(),
            CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap(),
        ];

        let request = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let best = CapMatcher::find_best_match(&caps, &request).unwrap();

        // Most specific cap that accepts the request
        assert_eq!(best.get_tag("ext"), Some(&"pdf".to_string()));
    }

    // TEST026: Test merge combines tags from both caps, subset keeps only specified tags
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

    // TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out
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

    // TEST028: Test empty cap URN defaults to media: wildcard
    #[test]
    fn test_empty_cap_urn_defaults_to_wildcard() {
        // Empty cap URN defaults to media: for both in and out
        let cap = CapUrn::from_string("cap:").expect("Empty cap should default to media: wildcard");
        assert_eq!(cap.in_spec(), "media:");
        assert_eq!(cap.out_spec(), "media:");

        // With trailing semicolon - same behavior
        let cap = CapUrn::from_string("cap:;").expect("cap:; should default to media: wildcard");
        assert_eq!(cap.in_spec(), "media:");
        assert_eq!(cap.out_spec(), "media:");
    }

    // TEST029: Test minimal valid cap URN has just in and out, empty tags
    #[test]
    fn test_minimal_cap_urn() {
        // Minimal valid cap URN has just in and out
        let cap = CapUrn::from_string(&format!("cap:in=\"{}\";out=\"{}\"", MEDIA_VOID, MEDIA_OBJECT))
            .unwrap();
        assert_eq!(cap.in_spec(), MEDIA_VOID);
        assert_eq!(cap.out_spec(), MEDIA_OBJECT);
        assert!(cap.tags.is_empty());
    }

    // TEST030: Test extended characters (forward slashes, colons) in tag values
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

    // TEST031: Test wildcard rejected in keys but accepted in values
    #[test]
    fn test_wildcard_restrictions() {
        // Wildcard should be rejected in keys
        assert!(CapUrn::from_string(&test_urn("*=value")).is_err());

        // Wildcard should be accepted in values
        let cap = CapUrn::from_string(&test_urn("key=*")).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"*".to_string()));
    }

    // TEST032: Test duplicate keys are rejected with DuplicateKey error
    #[test]
    fn test_duplicate_key_rejection() {
        let result = CapUrn::from_string(&test_urn("key=value1;key=value2"));
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CapUrnError::DuplicateKey(_)));
        }
    }

    // TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed
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

    // TEST034: Test empty values are rejected
    #[test]
    fn test_empty_value_error() {
        assert!(CapUrn::from_string(&test_urn("key=")).is_err());
        assert!(CapUrn::from_string(&test_urn("key=;other=value")).is_err());
    }

    // TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out
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

    // TEST036: Test with_tag preserves value case
    #[test]
    fn test_with_tag_preserves_value() {
        let cap = CapUrn::new(MEDIA_VOID.to_string(), MEDIA_OBJECT.to_string(), BTreeMap::new())
            .with_tag("key".to_string(), "ValueWithCase".to_string()).unwrap();
        assert_eq!(cap.get_tag("key"), Some(&"ValueWithCase".to_string()));
    }

    // TEST037: Test with_tag rejects empty value
    #[test]
    fn test_with_tag_rejects_empty_value() {
        let cap = CapUrn::new(MEDIA_VOID.to_string(), MEDIA_OBJECT.to_string(), BTreeMap::new());
        let result = cap.with_tag("key".to_string(), "".to_string());
        assert!(result.is_err());
    }

    // TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values
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

    // TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup
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

    // TEST040: Matching semantics - exact match succeeds
    #[test]
    fn test_matching_semantics_test1_exact_match() {
        // Test 1: Exact match
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.accepts(&request), "Test 1: Exact match should succeed");
    }

    // TEST041: Matching semantics - cap missing tag matches (implicit wildcard)
    #[test]
    fn test_matching_semantics_test2_cap_missing_tag() {
        // Test 2: Cap missing tag (implicit wildcard for other tags, not direction)
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(
            cap.accepts(&request),
            "Test 2: Cap missing tag should match (implicit wildcard)"
        );
    }

    // TEST042: Pattern rejects instance missing required tags
    #[test]
    fn test_matching_semantics_test3_cap_has_extra_tag() {
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf;version=2")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        // cap(op,ext,version) as pattern rejects request missing version
        assert!(!cap.accepts(&request), "Pattern rejects instance missing required tag");
        // Routing: request(op,ext) accepts cap(op,ext,version) — instance has all request needs
        assert!(request.accepts(&cap), "Request pattern satisfied by more-specific cap");
    }

    // TEST043: Matching semantics - request wildcard matches specific cap value
    #[test]
    fn test_matching_semantics_test4_request_has_wildcard() {
        // Test 4: Request has wildcard
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        assert!(
            cap.accepts(&request),
            "Test 4: Request wildcard should match"
        );
    }

    // TEST044: Matching semantics - cap wildcard matches specific request value
    #[test]
    fn test_matching_semantics_test5_cap_has_wildcard() {
        // Test 5: Cap has wildcard
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=*")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        assert!(cap.accepts(&request), "Test 5: Cap wildcard should match");
    }

    // TEST045: Matching semantics - value mismatch does not match
    #[test]
    fn test_matching_semantics_test6_value_mismatch() {
        // Test 6: Value mismatch
        let cap = CapUrn::from_string(&test_urn("op=generate;ext=pdf")).unwrap();
        let request = CapUrn::from_string(&test_urn("op=generate;ext=docx")).unwrap();
        assert!(
            !cap.accepts(&request),
            "Test 6: Value mismatch should not match"
        );
    }

    // TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard)
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
            cap.accepts(&request),
            "Test 7: Fallback pattern should match (cap missing ext = implicit wildcard)"
        );
    }

    // TEST047: Matching semantics - thumbnail fallback with void input
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
            cap.accepts(&request),
            "Test 7b: Thumbnail fallback with void input should match"
        );
    }

    // TEST048: Matching semantics - wildcard direction matches anything
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
            cap.accepts(&request),
            "Test 8: Wildcard direction should match any direction"
        );
    }

    // TEST049: Non-overlapping tags — neither direction accepts
    #[test]
    fn test_matching_semantics_test9_cross_dimension_independence() {
        let cap = CapUrn::from_string(&test_urn("op=generate")).unwrap();
        let request = CapUrn::from_string(&test_urn("ext=pdf")).unwrap();
        // cap(op) rejects request missing op; request(ext) rejects cap missing ext
        assert!(!cap.accepts(&request), "Pattern rejects instance missing required tag");
        assert!(!request.accepts(&cap), "Reverse also rejects — non-overlapping tags");
    }

    // TEST050: Matching semantics - direction mismatch prevents matching
    #[test]
    fn test_matching_semantics_test10_direction_mismatch() {
        // Test 10: Direction mismatch prevents matching
        // media:string has tags {textable:*, form:scalar}, media:bytes has tags {bytes:*}
        // Neither can provide input for the other (completely different marker tags)
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
            !cap.accepts(&request),
            "Test 10: Direction mismatch should not match"
        );
    }

    // TEST051: Semantic direction matching - generic provider matches specific request
    #[test]
    fn test_direction_semantic_matching() {
        // A cap accepting media:bytes (generic) should match a request with media:pdf;bytes (specific)
        // because media:pdf;bytes has all marker tags that media:bytes requires (bytes=*)
        let generic_cap = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        let pdf_request = CapUrn::from_string(
            "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        assert!(generic_cap.accepts(&pdf_request),
            "Generic bytes provider must match specific pdf;bytes request");

        // Generic cap also matches epub;bytes (any bytes subtype)
        let epub_request = CapUrn::from_string(
            "cap:in=\"media:epub;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        assert!(generic_cap.accepts(&epub_request),
            "Generic bytes provider must match epub;bytes request");

        // Reverse: specific cap does NOT match generic request
        // A pdf-only handler cannot accept arbitrary bytes
        let pdf_cap = CapUrn::from_string(
            "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        let generic_request = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        assert!(!pdf_cap.accepts(&generic_request),
            "Specific pdf;bytes cap must NOT match generic bytes request");

        // Incompatible types: pdf cap does NOT match epub request
        assert!(!pdf_cap.accepts(&epub_request),
            "PDF-specific cap must NOT match epub request (epub lacks pdf marker)");

        // Output direction: cap producing more specific output matches less specific request
        let specific_out_cap = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        let generic_out_request = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;bytes\""
        ).unwrap();
        assert!(specific_out_cap.accepts(&generic_out_request),
            "Cap producing image;png;bytes;thumbnail must satisfy request for image;bytes");

        // Reverse output: generic output cap does NOT match specific output request
        let generic_out_cap = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;bytes\""
        ).unwrap();
        let specific_out_request = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        assert!(!generic_out_cap.accepts(&specific_out_request),
            "Cap producing generic image;bytes must NOT satisfy request requiring image;png;bytes;thumbnail");
    }

    // TEST052: Semantic direction specificity - more media URN tags = higher specificity
    #[test]
    fn test_direction_semantic_specificity() {
        // media:bytes has 1 tag, media:pdf;bytes has 2 tags
        // media:image;png;bytes;thumbnail has 4 tags
        let generic_cap = CapUrn::from_string(
            "cap:in=\"media:bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        let specific_cap = CapUrn::from_string(
            "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();

        // generic: bytes(1) + image;png;bytes;thumbnail(4) + op(1) = 6
        assert_eq!(generic_cap.specificity(), 6);
        // specific: pdf;bytes(2) + image;png;bytes;thumbnail(4) + op(1) = 7
        assert_eq!(specific_cap.specificity(), 7);

        assert!(specific_cap.specificity() > generic_cap.specificity(),
            "pdf;bytes cap must be more specific than bytes cap");

        // CapMatcher should prefer the more specific cap when both match
        let pdf_request = CapUrn::from_string(
            "cap:in=\"media:pdf;bytes\";op=generate_thumbnail;out=\"media:image;png;bytes;thumbnail\""
        ).unwrap();
        let caps = vec![generic_cap.clone(), specific_cap.clone()];
        let best = CapMatcher::find_best_match(&caps, &pdf_request).unwrap();
        assert_eq!(best.in_spec(), "media:pdf;bytes",
            "CapMatcher must prefer the more specific pdf;bytes provider");
    }
}

// TEST_WILDCARD_001: cap: (empty) defaults to in=media:;out=media:
#[test]
fn test_wildcard_001_empty_cap_defaults_to_media_wildcard() {
    let cap = CapUrn::from_string("cap:").expect("Empty cap should default to media: wildcard");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
    assert_eq!(cap.tags.len(), 0);
}

// TEST_WILDCARD_002: cap:in defaults out to media:
#[test]
fn test_wildcard_002_in_only_defaults_out_to_media() {
    let cap = CapUrn::from_string("cap:in").expect("in without out should default out to media:");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
}

// TEST_WILDCARD_003: cap:out defaults in to media:
#[test]
fn test_wildcard_003_out_only_defaults_in_to_media() {
    let cap = CapUrn::from_string("cap:out").expect("out without in should default in to media:");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
}

// TEST_WILDCARD_004: cap:in;out both become media:
#[test]
fn test_wildcard_004_in_out_no_values_become_media() {
    let cap = CapUrn::from_string("cap:in;out").expect("in;out should both become media:");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
}

// TEST_WILDCARD_005: cap:in=*;out=* becomes media:
#[test]
fn test_wildcard_005_explicit_asterisk_becomes_media() {
    let cap = CapUrn::from_string("cap:in=*;out=*").expect("in=*;out=* should become media:");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
}

// TEST_WILDCARD_006: cap:in=media:bytes;out=* has specific in, wildcard out
#[test]
fn test_wildcard_006_specific_in_wildcard_out() {
    let cap = CapUrn::from_string("cap:in=media:bytes;out=*").expect("Specific in with wildcard out");
    assert_eq!(cap.in_spec(), "media:bytes");
    assert_eq!(cap.out_spec(), "media:");
}

// TEST_WILDCARD_007: cap:in=*;out=media:text has wildcard in, specific out
#[test]
fn test_wildcard_007_wildcard_in_specific_out() {
    let cap = CapUrn::from_string("cap:in=*;out=media:text").expect("Wildcard in with specific out");
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:text");
}

// TEST_WILDCARD_008: cap:in=foo fails (invalid media URN)
#[test]
fn test_wildcard_008_invalid_in_spec_fails() {
    let result = CapUrn::from_string("cap:in=foo;out=media:");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, CapUrnError::InvalidInSpec(_)));
}

// TEST_WILDCARD_009: cap:in=media:bytes;out=bar fails (invalid media URN)
#[test]
fn test_wildcard_009_invalid_out_spec_fails() {
    let result = CapUrn::from_string("cap:in=media:bytes;out=bar");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, CapUrnError::InvalidOutSpec(_)));
}

// TEST_WILDCARD_010: Wildcard in/out match specific caps
#[test]
fn test_wildcard_010_wildcard_accepts_specific() {
    let wildcard = CapUrn::from_string("cap:").unwrap();
    let specific = CapUrn::from_string("cap:in=media:bytes;out=media:text").unwrap();
    
    assert!(wildcard.accepts(&specific), "Wildcard should accept specific cap");
    assert!(specific.conforms_to(&wildcard), "Specific should conform to wildcard");
}

// TEST_WILDCARD_011: Specificity - wildcard has 0, specific has tag count
#[test]
fn test_wildcard_011_specificity_scoring() {
    let wildcard = CapUrn::from_string("cap:").unwrap();
    let specific = CapUrn::from_string("cap:in=media:bytes;out=media:text").unwrap();
    
    assert_eq!(wildcard.specificity(), 0, "Wildcard should have 0 specificity");
    assert!(specific.specificity() > 0, "Specific cap should have non-zero specificity");
}

// TEST_WILDCARD_012: cap:in;out;op=test preserves other tags
#[test]
fn test_wildcard_012_preserve_other_tags() {
    let cap = CapUrn::from_string("cap:in;out;op=test").unwrap();
    assert_eq!(cap.in_spec(), "media:");
    assert_eq!(cap.out_spec(), "media:");
    assert_eq!(cap.get_tag("op").map(|s| s.as_str()), Some("test"));
}

// TEST_WILDCARD_013: All identity forms produce the same CapUrn
#[test]
fn test_wildcard_013_identity_forms_equivalent() {
    let forms = [
        "cap:",
        "cap:in;out",
        "cap:in=*;out=*",
        "cap:in=media:;out=media:",
        "cap:in;out=media:",
        "cap:in=*;out=media:",
        "cap:in=media:;out",
        "cap:in=media:;out=*",
    ];
    let reference = CapUrn::from_string(forms[0]).unwrap();
    for form in &forms[1..] {
        let parsed = CapUrn::from_string(form).unwrap();
        assert_eq!(parsed.in_spec(), "media:", "in_spec mismatch for '{}'", form);
        assert_eq!(parsed.out_spec(), "media:", "out_spec mismatch for '{}'", form);
        assert!(parsed.tags.is_empty(), "unexpected tags for '{}'", form);
        // Bidirectional accepts — equivalent caps
        assert!(reference.accepts(&parsed), "'cap:' must accept '{}' as instance", form);
        assert!(parsed.accepts(&reference), "'{}' must accept 'cap:' as instance", form);
    }
}

// TEST_WILDCARD_014: CAP_IDENTITY constant matches identity caps regardless of string form
#[test]
fn test_wildcard_014_cap_identity_constant_works() {
    use crate::standard::caps::CAP_IDENTITY;
    let identity = CapUrn::from_string(CAP_IDENTITY).unwrap();

    // Identity accepts itself
    assert!(identity.accepts(&identity));

    // Identity parsed from different string forms is equivalent
    let long_form = CapUrn::from_string("cap:in=media:;out=media:").unwrap();
    assert!(identity.accepts(&long_form));
    assert!(long_form.accepts(&identity));

    // Identity as pattern accepts any specific cap (wildcard in/out, no tags)
    let specific = CapUrn::from_string("cap:in=\"media:void\";op=test;out=\"media:void\"").unwrap();
    assert!(identity.accepts(&specific), "Identity pattern must accept specific cap");

    // Specific as pattern does NOT accept identity — specific requires things identity lacks
    assert!(!specific.accepts(&identity), "Specific pattern must reject identity");

    // conforms_to is the reverse of accepts
    // identity.conforms_to(specific) = specific.accepts(identity) → false
    // (specific requires void in/out + op=test, identity has bare media: + no tags)
    assert!(!identity.conforms_to(&specific), "Identity does not conform to specific cap");
    // specific.conforms_to(identity) = identity.accepts(specific) → true
    // (identity accepts everything — broadest pattern)
    assert!(specific.conforms_to(&identity), "Specific conforms to identity (identity accepts all)");
}

// TEST_WILDCARD_015: Identity (no tags) does not match specific requests via routing
#[test]
fn test_wildcard_015_identity_routing_isolation() {
    let identity = CapUrn::from_string("cap:").unwrap();
    let specific_request = CapUrn::from_string("cap:in=\"media:void\";op=test;out=\"media:void\"").unwrap();

    // Routing direction: request.accepts(registered_cap)
    // Specific request rejects identity — identity's bare media: doesn't satisfy specific in/out
    assert!(!specific_request.accepts(&identity),
        "Specific request must not route to identity handler");

    // Identity request accepts identity — exact match (no constraints)
    let identity_request = CapUrn::from_string("cap:").unwrap();
    assert!(identity_request.accepts(&identity),
        "Identity request must route to identity handler");

    // Identity request does NOT accept specific cap — direction spec mismatch
    // identity has media: for in (wildcard, skips check), BUT the direction check
    // for output: identity out=media: → skip (wildcard). For tags: identity has no tags → match.
    // So identity request DOES match specific via accepts. BUT closest-specificity
    // routing ensures the identity handler is preferred.
    // This is correct: identity request has no constraints, matches everything.
}
