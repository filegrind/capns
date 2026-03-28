//! Media Content Inspection Adapters
//!
//! This module provides adapters for file types that require content inspection
//! to determine their full media URN (e.g., list/record markers for JSON, CSV, YAML).
//!
//! ## Architecture
//!
//! The MediaAdapterRegistry integrates with MediaUrnRegistry:
//! 1. MediaUrnRegistry (from TOML specs) provides extension -> base URN mapping
//! 2. Adapters are registered for specific base URNs that need content inspection
//! 3. Adapters refine the base URN with list/record markers based on content
//!
//! ## Which types need adapters?
//!
//! - **Data interchange** (JSON, NDJSON, CSV, TSV, PSV, YAML, XML): Need inspection
//!   to determine if content is scalar/list and opaque/record
//! - **Plain text** (.txt): May be single-line or multi-line (list)
//! - **Binary formats** (PDF, PNG, MP3, etc.): No adapters needed - structure is
//!   defined in the TOML spec and is fixed (always ScalarOpaque)

mod registry;
pub(crate) mod data;
pub(crate) mod text;

pub use registry::MediaAdapterRegistry;
pub use registry::extract_base_urn;

// Re-export content inspection adapters for testing
pub use data::{
    CsvAdapter, JsonAdapter, NdjsonAdapter, PsvAdapter, TomlAdapter, TsvAdapter, XmlAdapter,
    YamlAdapter,
};
pub use text::PlainTextAdapter;
