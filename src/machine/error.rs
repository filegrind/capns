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

    /// A media URN appearing as a node in the resolved strand has
    /// no cached entry in the media registry. Render-payload
    /// emission must have a display title for every node and we
    /// never synthesize titles from URN strings.
    #[error("media URN '{media_urn}' has no cached spec — cannot emit a display title for render payload")]
    UncachedMediaDef { media_urn: String },

    /// A cap URN referenced by an edge has no cached entry in the
    /// cap registry. Render-payload emission must have a display
    /// title for every edge.
    #[error("cap URN '{cap_urn}' has no cached definition — cannot emit a display title for render payload")]
    UncachedCap { cap_urn: String },
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

    // TEST1134: All MachineAbstractionError variants are of type MachineAbstractionError and
    // are convertible to MachineParseError::Resolution. This pins the error hierarchy so a
    // refactor that accidentally changes the type relationship is caught immediately.
    #[test]
    fn test1134_all_abstraction_error_variants_are_machine_abstraction_error() {
        let variants: Vec<MachineAbstractionError> = vec![
            MachineAbstractionError::NoCapabilitySteps,
            MachineAbstractionError::UnknownCap { cap_urn: "cap:x".to_string() },
            MachineAbstractionError::UnmatchedSourceInCapArgs {
                strand_index: 0,
                cap_urn: "cap:x".to_string(),
                source_urn: "media:pdf".to_string(),
            },
            MachineAbstractionError::AmbiguousMachineNotation {
                strand_index: 1,
                cap_urn: "cap:y".to_string(),
            },
            MachineAbstractionError::CyclicMachineStrand { strand_index: 2 },
        ];

        for variant in variants {
            let parse_error: MachineParseError = variant.into();
            assert!(
                matches!(parse_error, MachineParseError::Resolution(_)),
                "every MachineAbstractionError must convert to MachineParseError::Resolution"
            );
        }
    }

    // TEST1147: MachineSyntaxError Display includes position and detail for each variant
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

    // TEST1148: MachineParseError::from(MachineSyntaxError) preserves the syntax error variant
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

    // TEST1149: MachineParseError::from(MachineAbstractionError) preserves the resolution error variant
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
