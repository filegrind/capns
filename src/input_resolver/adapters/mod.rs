//! Media Content Inspection Adapters
//!
//! This module provides adapters for media types. Each adapter declares a
//! pattern URN and can optionally inspect file content to select the most
//! appropriate candidate URN from the media spec registry.
//!
//! ## Architecture
//!
//! The MediaAdapterRegistry integrates with MediaUrnRegistry:
//! 1. MediaUrnRegistry provides extension -> candidate URN mapping (from specs)
//! 2. Each adapter declares a pattern URN — candidates that conform to it are offered
//! 3. Content-inspecting adapters select the best candidate based on structure
//! 4. Non-inspecting adapters let the registry pick the most specific candidate

pub(crate) mod archives;
pub(crate) mod audio;
pub(crate) mod code;
pub(crate) mod data;
pub(crate) mod documents;
pub(crate) mod images;
pub(crate) mod other;
mod registry;
pub(crate) mod text;
pub(crate) mod video;

pub use registry::MediaAdapterRegistry;

// Re-export content inspection adapters
pub use data::{
    CsvAdapter, JsonAdapter, NdjsonAdapter, PsvAdapter, TomlAdapter, TsvAdapter, XmlAdapter,
    YamlAdapter,
};
pub use text::PlainTextAdapter;
