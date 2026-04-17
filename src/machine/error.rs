//! Error types for machine notation parsing, resolution, and serialization.

use thiserror::Error;

/// Errors raised when building or resolving a `Machine`.
///
/// These cover anchor-realization (computing each `MachineStrand`'s
/// resolved DAG via the source-to-arg matching algorithm) and
/// downstream invariants on the resulting machine. They are
/// distinct from `MachineSyntaxError`, which covers lexical and
/// grammatical failures of the notation parser.
#[derive(Debug, Error)]
pub enum MachineAbstractionError {
    /// The strand or wiring set contains no Cap step (no edge to
    /// resolve). A machine must declare at least one capability.
    #[error("strand or wiring set contains no capability steps")]
    NoCapabilitySteps,

    /// A cap URN referenced by a strand or a wiring could not be
    /// found in the cap registry's in-memory cache. Resolution
    /// requires the cap definition (specifically its `args` list)
    /// to compute source-to-arg assignment.
    #[error("cap URN '{cap_urn}' is not in the cap registry cache")]
    UnknownCap { cap_urn: String },

    /// A source URN does not conform to any of the cap's input
    /// argument media URNs. The source has no valid slot to be
    /// assigned to.
    #[error(
        "in strand {strand_index}, cap '{cap_urn}': source URN '{source_urn}' does not conform to any of the cap's input arguments"
    )]
    UnmatchedSourceInCapArgs {
        strand_index: usize,
        cap_urn: String,
        source_urn: String,
    },

    /// The bipartite minimum-cost source-to-arg assignment is not
    /// unique. Either two distinct assignments tie at the same
    /// total specificity-distance cost, or the equality subgraph
    /// admits more than one perfect matching at the minimum cost.
    /// The notation cannot be resolved deterministically; no
    /// fall-back to source-vec position is permitted.
    #[error(
        "in strand {strand_index}, cap '{cap_urn}': source-to-cap-arg assignment is ambiguous (multiple minimum-cost matchings exist)"
    )]
    AmbiguousMachineNotation {
        strand_index: usize,
        cap_urn: String,
    },

    /// The resolved data-flow graph of a strand contains a cycle.
    /// A planner-produced strand cannot trigger this; only
    /// programmatic misconstruction or notation that wires a cap's
    /// output back into one of its own ancestors can.
    #[error("strand {strand_index}: resolved data-flow graph contains a cycle")]
    CyclicMachineStrand { strand_index: usize },
}

/// Errors raised during lexical / grammatical parsing of machine
/// notation.
///
/// These represent failures BEFORE the parser hands the wiring
/// set to the resolver. Resolution-level failures (cap not in
/// registry, ambiguous matching, cyclic strand, …) are reported
/// as `MachineAbstractionError`.
#[derive(Debug, Error)]
pub enum MachineSyntaxError {
    /// Input string is empty or contains only whitespace.
    #[error("machine notation is empty")]
    Empty,

    /// A statement bracket `[` was opened but never closed with `]`.
    #[error("unterminated statement starting at byte {position}")]
    UnterminatedStatement { position: usize },

    /// A cap URN in a header statement failed to parse.
    #[error("invalid cap URN in header '{alias}': {details}")]
    InvalidCapUrn { alias: String, details: String },

    /// A wiring statement references an alias that was never defined in a header.
    #[error("wiring references undefined alias '{alias}'")]
    UndefinedAlias { alias: String },

    /// Two header statements define the same alias.
    #[error("duplicate alias '{alias}' (first defined at statement {first_position})")]
    DuplicateAlias {
        alias: String,
        first_position: usize,
    },

    /// A wiring statement has invalid structure (wrong number of
    /// arrows, missing parts).
    #[error("invalid wiring at statement {position}: {details}")]
    InvalidWiring { position: usize, details: String },

    /// A media URN referenced in a header failed to parse.
    #[error("invalid media URN in cap '{alias}': {details}")]
    InvalidMediaUrn { alias: String, details: String },

    /// A header statement has invalid structure.
    #[error("invalid header at statement {position}: {details}")]
    InvalidHeader { position: usize, details: String },

    /// The parsed machine has headers but no wirings.
    #[error("machine has headers but no wirings — define at least one edge")]
    NoEdges,

    /// A wiring references an alias used as a node name that
    /// collides with a header alias.
    #[error("node name '{name}' collides with cap alias '{alias}'")]
    NodeAliasCollision { name: String, alias: String },

    /// PEG parse error from the pest grammar.
    #[error("parse error: {details}")]
    ParseError { details: String },
}

/// Combined error returned by `Machine::from_string`.
///
/// Notation parsing has two phases: lexical/grammatical (yields
/// `MachineSyntaxError`) and resolution (yields
/// `MachineAbstractionError`). This enum is the union returned
/// from `Machine::from_string` and `parse_machine` so callers can
/// branch on either kind of failure.
#[derive(Debug, Error)]
pub enum MachineParseError {
    #[error(transparent)]
    Syntax(#[from] MachineSyntaxError),

    #[error(transparent)]
    Resolution(#[from] MachineAbstractionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1147_machine_syntax_error_display_is_specific() {
        let err = MachineSyntaxError::InvalidWiring {
            position: 7,
            details: "expected source -> cap -> target".to_string(),
        };

        assert_eq!(
            err.to_string(),
            "invalid wiring at statement 7: expected source -> cap -> target"
        );
    }

    #[test]
    fn test1148_machine_parse_error_from_syntax_preserves_variant() {
        let parse_error: MachineParseError = MachineSyntaxError::UndefinedAlias {
            alias: "extract".to_string(),
        }
        .into();

        match parse_error {
            MachineParseError::Syntax(MachineSyntaxError::UndefinedAlias { alias }) => {
                assert_eq!(alias, "extract");
            }
            other => panic!("expected syntax undefined alias, got {other:?}"),
        }
    }

    #[test]
    fn test1149_machine_parse_error_from_resolution_preserves_variant() {
        let parse_error: MachineParseError = MachineAbstractionError::AmbiguousMachineNotation {
            strand_index: 2,
            cap_urn: "cap:in=media:pdf;out=media:text".to_string(),
        }
        .into();

        match parse_error {
            MachineParseError::Resolution(MachineAbstractionError::AmbiguousMachineNotation {
                strand_index,
                cap_urn,
            }) => {
                assert_eq!(strand_index, 2);
                assert_eq!(cap_urn, "cap:in=media:pdf;out=media:text");
            }
            other => panic!("expected ambiguous resolution error, got {other:?}"),
        }
    }
}
