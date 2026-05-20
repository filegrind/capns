//! Re-exports for the unified fabric registry.
//!
//! Both cap and media-def storage live in `crate::fabric::registry` now.
//! This module is kept as a re-export so existing module paths
//! (`crate::media::registry::FabricRegistry`, etc.) still resolve.

pub use crate::fabric::registry::{
    FabricRegistry, FabricRegistryError, RegistryConfig, StoredMediaDef,
};
