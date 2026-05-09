//! InputResolver — Unified input resolution with pluggable content inspection
//!
//! This module provides two parallel content-inspection systems that solve the
//! same general problem: determining a specific media URN variant from content.
//!
//! ## File Content Resolution
//!
//! Resolves mixed file/directory/glob inputs into files with detected media types.
//! Content inspection is performed by cartridge-provided adapters via the
//! `CartridgeAdapterInvoker` trait.
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

mod adapter;
mod adapters;
mod os_filter;
pub(crate) mod path_resolver;
mod resolver;
mod types;
pub mod value_adapter;
pub mod value_adapter_registry;

pub use types::{ContentStructure, InputItem, InputResolverError, ResolvedFile, ResolvedInputSet};

pub use adapter::{AdapterResult, CartridgeAdapterInvoker, MAX_CONTENT_INSPECTION_BYTES};

pub use resolver::{
    detect_file_confirmed, detect_file_with_fabric_registry,
    discriminate_candidates_by_validation, resolve_input, resolve_inputs,
    resolve_inputs_confirmed, resolve_paths,
};

pub use path_resolver::resolve_directory;

pub use adapters::MediaAdapterRegistry;
pub use value_adapter::{ValueAdapter, ValueAdapterResult};
pub use value_adapter_registry::ValueAdapterRegistry;
