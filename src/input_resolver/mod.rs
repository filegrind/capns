//! InputResolver — Unified input resolution with pluggable content inspection
//!
//! This module provides two parallel content-inspection systems that solve the
//! same general problem: determining a specific media URN variant from content.
//!
//! ## File Content Resolution
//!
//! Resolves mixed file/directory/glob inputs into files with detected media types:
//!
//! ```text
//! MediaAdapterRegistry (file content)
//!   ├── JsonAdapter (inspects bytes → list/record markers)
//!   ├── CsvAdapter (inspects bytes → list/record markers)
//!   ├── YamlAdapter, XmlAdapter, PlainTextAdapter, ...
//!   └── Extension-only fallback
//! ```
//!
//! ## Value Content Resolution
//!
//! Inspects string argument values to refine a base media URN with domain markers:
//!
//! ```text
//! ValueAdapterRegistry (string values)
//!   └── (domain-specific adapters registered by consumers)
//!       e.g., ModelSpecValueAdapter: "hf:.../Mistral-7B..." → adds "mistral" tag
//! ```
//!
//! Both registries follow the same pattern: base media URN + content inspection →
//! refined media URN with additional marker tags.

mod types;
mod os_filter;
pub(crate) mod path_resolver;
mod adapter;
mod adapters;
mod resolver;
pub mod value_adapter;
pub mod value_adapter_registry;

pub use types::{
    InputItem,
    ContentStructure,
    ResolvedFile,
    ResolvedInputSet,
    InputResolverError,
};

pub use adapter::{
    MediaAdapter,
    AdapterMatch,
    AdapterResult,
};

pub use resolver::{
    resolve_input,
    resolve_inputs,
    resolve_paths,
    detect_file,
    detect_file_with_media_registry,
};

pub use path_resolver::resolve_directory;

// Re-export adapter registries for extensibility
pub use adapters::MediaAdapterRegistry;
pub use adapters::extract_base_urn;
pub use value_adapter::{ValueAdapter, ValueAdapterResult};
pub use value_adapter_registry::ValueAdapterRegistry;
