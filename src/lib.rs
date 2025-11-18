//! Capability SDK - Core capability identifier and definition system
//!
//! This library provides the fundamental capability identifier system used across
//! all LBVR plugins and providers. It defines the formal structure for capability
//! identifiers with flat tag-based naming, wildcard support, and specificity comparison.

pub mod capability_key;
pub mod capability;
pub mod manifest;
pub mod validation;

pub use capability_key::*;
pub use capability::*;
pub use manifest::*;
pub use validation::*;