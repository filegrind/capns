//! Error types for route notation parsing and serialization

use thiserror::Error;

/// Errors that can occur during route notation parsing
#[derive(Debug, Error)]
pub enum RouteNotationError {
    /// Input string is empty or contains only whitespace
    #[error("route notation is empty")]
    Empty,

    /// A statement bracket `[` was opened but never closed with `]`
    #[error("unterminated statement starting at byte {position}")]
    UnterminatedStatement { position: usize },

    /// A cap URN in a header statement failed to parse
    #[error("invalid cap URN in header '{alias}': {details}")]
    InvalidCapUrn { alias: String, details: String },

    /// A wiring statement references an alias that was never defined in a header
    #[error("wiring references undefined alias '{alias}'")]
    UndefinedAlias { alias: String },

    /// Two header statements define the same alias
    #[error("duplicate alias '{alias}' (first defined at statement {first_position})")]
    DuplicateAlias { alias: String, first_position: usize },

    /// A wiring statement has invalid structure (wrong number of arrows, missing parts)
    #[error("invalid wiring at statement {position}: {details}")]
    InvalidWiring { position: usize, details: String },

    /// A media URN referenced in a header failed to parse
    #[error("invalid media URN in cap '{alias}': {details}")]
    InvalidMediaUrn { alias: String, details: String },

    /// A header statement has invalid structure
    #[error("invalid header at statement {position}: {details}")]
    InvalidHeader { position: usize, details: String },

    /// The parsed route graph has no edges (headers were defined but no wirings)
    #[error("route has headers but no wirings — define at least one edge")]
    NoEdges,

    /// A wiring references an alias used as a node name that collides with a header alias
    #[error("node name '{name}' collides with cap alias '{alias}'")]
    NodeAliasCollision { name: String, alias: String },

    /// PEG parse error from the pest grammar
    #[error("parse error: {details}")]
    ParseError { details: String },
}
