//! Capability ID Builder API
//!
//! Provides a fluent builder interface for constructing and manipulating capability identifiers.
//! This replaces manual creation and manipulation of capability IDs with a type-safe API.

use crate::capability_key::{CapabilityKey, CapabilityKeyError};

/// Builder for constructing CapabilityKey instances with a fluent API
#[derive(Debug, Clone)]
pub struct CapabilityKeyBuilder {
    segments: Vec<String>,
}

impl CapabilityKeyBuilder {
    /// Create a new empty builder
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Create a builder starting with a base capability ID
    pub fn from_capability_key(capability_key: &CapabilityKey) -> Self {
        Self {
            segments: capability_key.components.clone(),
        }
    }

    /// Create a builder from a capability string
    pub fn from_string(s: &str) -> Result<Self, CapabilityKeyError> {
        let capability_key = CapabilityKey::from_string(s)?;
        Ok(Self::from_capability_key(&capability_key))
    }

    /// Add a segment to the capability ID
    pub fn sub<S: AsRef<str>>(mut self, segment: S) -> Self {
        self.segments.push(segment.as_ref().to_string());
        self
    }

    /// Add multiple segments to the capability ID
    pub fn subs<I, S>(mut self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for segment in segments {
            self.segments.push(segment.as_ref().to_string());
        }
        self
    }

    /// Replace a segment at the given index
    pub fn replace_segment<S: AsRef<str>>(mut self, index: usize, segment: S) -> Self {
        if index < self.segments.len() {
            self.segments[index] = segment.as_ref().to_string();
        }
        self
    }

    /// Remove the last segment (make more general)
    pub fn make_more_general(mut self) -> Self {
        self.segments.pop();
        self
    }

    /// Remove segments from the given index onwards (make more general to that level)
    pub fn make_general_to_level(mut self, level: usize) -> Self {
        self.segments.truncate(level);
        self
    }

    /// Add a wildcard segment
    pub fn add_wildcard(self) -> Self {
        self.sub("*")
    }

    /// Replace the last segment with a wildcard
    pub fn make_wildcard(mut self) -> Self {
        if !self.segments.is_empty() {
            let last_index = self.segments.len() - 1;
            self.segments[last_index] = "*".to_string();
        }
        self
    }

    /// Replace all segments from the given index with a wildcard
    pub fn make_wildcard_from_level(mut self, level: usize) -> Self {
        if level < self.segments.len() {
            self.segments.truncate(level + 1);
            self.segments[level] = "*".to_string();
        } else if level == self.segments.len() {
            self.segments.push("*".to_string());
        }
        self
    }

    /// Get the current segments as a slice
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Get the number of segments
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Check if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Clear all segments
    pub fn clear(mut self) -> Self {
        self.segments.clear();
        self
    }

    /// Build the final CapabilityKey
    pub fn build(self) -> Result<CapabilityKey, CapabilityKeyError> {
        if self.segments.is_empty() {
            return Err(CapabilityKeyError::Empty);
        }
        Ok(CapabilityKey::new(self.segments))
    }

    /// Build the final CapabilityKey as a string
    pub fn build_string(self) -> Result<String, CapabilityKeyError> {
        let capability_key = self.build()?;
        Ok(capability_key.to_string())
    }
}

impl Default for CapabilityKeyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience trait for creating builders from various types
pub trait IntoCapabilityKeyBuilder {
    fn into_builder(self) -> Result<CapabilityKeyBuilder, CapabilityKeyError>;
}

impl IntoCapabilityKeyBuilder for &str {
    fn into_builder(self) -> Result<CapabilityKeyBuilder, CapabilityKeyError> {
        CapabilityKeyBuilder::from_string(self)
    }
}

impl IntoCapabilityKeyBuilder for String {
    fn into_builder(self) -> Result<CapabilityKeyBuilder, CapabilityKeyError> {
        CapabilityKeyBuilder::from_string(&self)
    }
}

impl IntoCapabilityKeyBuilder for &CapabilityKey {
    fn into_builder(self) -> Result<CapabilityKeyBuilder, CapabilityKeyError> {
        Ok(CapabilityKeyBuilder::from_capability_key(self))
    }
}

impl IntoCapabilityKeyBuilder for CapabilityKey {
    fn into_builder(self) -> Result<CapabilityKeyBuilder, CapabilityKeyError> {
        Ok(CapabilityKeyBuilder::from_capability_key(&self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic_construction() {
        let capability_key = CapabilityKeyBuilder::new()
            .sub("data_processing")
            .sub("transform")
            .sub("json")
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data_processing:transform:json");
    }

    #[test]
    fn test_builder_from_string() {
        let builder = CapabilityKeyBuilder::from_string("extract:metadata:pdf").unwrap();
        let capability_key = builder.build().unwrap();

        assert_eq!(capability_key.to_string(), "extract:metadata:pdf");
    }

    #[test]
    fn test_builder_make_more_general() {
        let capability_key = CapabilityKeyBuilder::from_string("data_processing:transform:json")
            .unwrap()
            .make_more_general()
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data_processing:transform");
    }

    #[test]
    fn test_builder_make_wildcard() {
        let capability_key = CapabilityKeyBuilder::from_string("data_processing:transform:json")
            .unwrap()
            .make_wildcard()
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data_processing:transform:*");
    }

    #[test]
    fn test_builder_add_wildcard() {
        let capability_key = CapabilityKeyBuilder::new()
            .sub("data_processing")
            .add_wildcard()
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data_processing:*");
    }

    #[test]
    fn test_builder_replace_segment() {
        let capability_key = CapabilityKeyBuilder::from_string("extract:metadata:pdf")
            .unwrap()
            .replace_segment(2, "xml")
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "extract:metadata:xml");
    }

    #[test]
    fn test_builder_subs() {
        let capability_key = CapabilityKeyBuilder::new()
            .subs(vec!["data", "processing"])
            .sub("json")
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data:processing:json");
    }

    #[test]
    fn test_builder_make_general_to_level() {
        let capability_key = CapabilityKeyBuilder::from_string("a:b:c:d:e")
            .unwrap()
            .make_general_to_level(2)
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "a:b");
    }

    #[test]
    fn test_builder_make_wildcard_from_level() {
        let capability_key = CapabilityKeyBuilder::from_string("data:processing:transform:json")
            .unwrap()
            .make_wildcard_from_level(2)
            .build()
            .unwrap();

        assert_eq!(capability_key.to_string(), "data:processing:*");
    }

    #[test]
    fn test_into_builder_trait() {
        let capability_key1 = "extract:metadata:pdf".into_builder().unwrap().build().unwrap();
        let capability_key2 = String::from("extract:metadata:pdf").into_builder().unwrap().build().unwrap();

        assert_eq!(capability_key1.to_string(), "extract:metadata:pdf");
        assert_eq!(capability_key2.to_string(), "extract:metadata:pdf");
    }
}