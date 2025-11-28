//! Cap SDK - Core cap identifier and definition system
//!
//! This library provides the fundamental cap identifier system used across
//! all FMIO plugins and providers. It defines the formal structure for cap
//! identifiers with flat tag-based naming, wildcard support, and specificity comparison.

pub mod cap_urn;
pub mod cap;
pub mod manifest;
pub mod validation;

pub use cap_urn::*;
pub use cap::*;
pub use manifest::*;
pub use validation::*;