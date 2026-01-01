//! Cap SDK - Core cap URN and definition system
//!
//! This library provides the fundamental cap URN system used across
//! all FGRND plugins and providers. It defines the formal structure for cap
//! identifiers with flat tag-based naming, wildcard support, and specificity comparison.

pub mod cap_urn;
pub mod cap;
pub mod manifest;
pub mod validation;
pub mod schema_validation;
pub mod registry;
pub mod standard;
pub mod caller;
pub mod response;
pub mod caphost_registry;

pub use cap_urn::*;
pub use cap::*;
pub use manifest::*;
pub use validation::*;
pub use schema_validation::{SchemaValidator as JsonSchemaValidator, SchemaValidationError, SchemaResolver, FileSchemaResolver};
pub use registry::*;
pub use standard::*;
pub use caller::*;
pub use response::*;
pub use caphost_registry::*;