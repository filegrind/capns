//! Capability SDK - Core capability identifier and definition system
//!
//! This library provides the fundamental capability identifier system used across
//! all LBVR plugins and providers. It defines the formal structure for capability
//! identifiers with hierarchical naming, wildcard support, and specificity comparison.

pub mod capability_id;
pub mod capability_id_builder;
pub mod capability;

pub use capability_id::*;
pub use capability_id_builder::*;
pub use capability::*;