//! Shared test fixtures for `machine/` unit tests.
//!
//! Provides helpers for building `Cap` definitions, `Strand`s, and
//! a populated `CapRegistry` so test code in `resolve.rs`,
//! `parser.rs`, `serializer.rs`, and `graph.rs` doesn't have to
//! repeat the boilerplate. Every helper here is registered as
//! `pub(crate)` and only compiled under `#[cfg(test)]`.

use std::collections::HashMap;

use crate::cap::definition::{ArgSource, Cap, CapArg, CapOutput};
use crate::cap::registry::CapRegistry;
use crate::planner::{Strand, StrandStep, StrandStepType};
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

/// Build a `Cap` from a string URN, a list of input arg media
/// URNs, and the output media URN. Each arg gets a stdin source
/// pointing at its own URN — slot identity and stdin URN are
/// the same. Use `build_cap_with_slot_stdin_pairs` when you
/// need to test the case where they differ (e.g. file-path
/// auto-conversion).
pub(crate) fn build_cap(
    cap_urn_str: &str,
    title: &str,
    arg_media_urns: &[&str],
    output_media_urn: &str,
) -> Cap {
    let pairs: Vec<(&str, &str)> = arg_media_urns.iter().map(|m| (*m, *m)).collect();
    build_cap_with_slot_stdin_pairs(cap_urn_str, title, &pairs, output_media_urn)
}

/// Build a `Cap` whose args declare distinct **slot identity**
/// and **stdin source URN** per arg. Each tuple is
/// `(slot_media_urn, stdin_media_urn)`. The resolver matches
/// wiring sources against the stdin URN, not the slot identity
/// — this is the regression-test path for caps like
/// `disbind-pdf` where the slot is `media:file-path;textable`
/// but the stdin source delivers `media:pdf`.
pub(crate) fn build_cap_with_slot_stdin_pairs(
    cap_urn_str: &str,
    title: &str,
    args: &[(&str, &str)],
    output_media_urn: &str,
) -> Cap {
    let urn = CapUrn::from_string(cap_urn_str)
        .unwrap_or_else(|e| panic!("test fixture: invalid cap URN {cap_urn_str}: {e}"));
    let arg_values: Vec<CapArg> = args
        .iter()
        .map(|(slot, stdin)| {
            CapArg::new(
                slot.to_string(),
                true,
                vec![ArgSource::Stdin {
                    stdin: stdin.to_string(),
                }],
            )
        })
        .collect();
    Cap {
        urn,
        title: title.to_string(),
        cap_description: None,
        documentation: None,
        metadata: HashMap::new(),
        command: format!("test-fixture://{title}"),
        media_specs: Vec::new(),
        args: arg_values,
        output: Some(CapOutput::new(
            output_media_urn.to_string(),
            format!("output of {title}"),
        )),
        metadata_json: None,
        registered_by: None,
    }
}

/// Build a `CapRegistry` pre-populated with the supplied caps.
pub(crate) fn registry_with(caps: Vec<Cap>) -> CapRegistry {
    let registry = CapRegistry::new_for_test();
    registry.add_caps_to_cache(caps);
    registry
}

/// Convenience: parse a media URN string. Panics on parse
/// failure with the failing literal in the message.
pub(crate) fn media(urn: &str) -> MediaUrn {
    MediaUrn::from_string(urn)
        .unwrap_or_else(|e| panic!("test fixture: invalid media URN {urn}: {e}"))
}

/// Convenience: parse a cap URN string.
pub(crate) fn cap(urn: &str) -> CapUrn {
    CapUrn::from_string(urn).unwrap_or_else(|e| panic!("test fixture: invalid cap URN {urn}: {e}"))
}

/// Build a one-cap `StrandStep`. `from`/`to` are the runtime
/// data URN at this step's input and output positions; in the
/// new regime they should match the cap's declared in/out
/// patterns (or a more-specific URN that conforms).
pub(crate) fn cap_step(cap_urn_str: &str, title: &str, from: &str, to: &str) -> StrandStep {
    StrandStep {
        step_type: StrandStepType::Cap {
            cap_urn: cap(cap_urn_str),
            title: title.to_string(),
            specificity: 0,
            input_is_sequence: false,
            output_is_sequence: false,
        },
        from_spec: media(from),
        to_spec: media(to),
    }
}

/// Build a `ForEach` strand step.
pub(crate) fn for_each_step(media_urn: &str) -> StrandStep {
    StrandStep {
        step_type: StrandStepType::ForEach {
            media_spec: media(media_urn),
        },
        from_spec: media(media_urn),
        to_spec: media(media_urn),
    }
}

/// Build a `Collect` strand step.
pub(crate) fn collect_step(media_urn: &str) -> StrandStep {
    StrandStep {
        step_type: StrandStepType::Collect {
            media_spec: media(media_urn),
        },
        from_spec: media(media_urn),
        to_spec: media(media_urn),
    }
}

/// Wrap a list of steps into a `Strand`. Source/target specs
/// are taken from the first step's `from_spec` and the last
/// step's `to_spec`.
pub(crate) fn strand_from_steps(steps: Vec<StrandStep>, description: &str) -> Strand {
    let total_steps = steps.len() as i32;
    let cap_step_count = steps.iter().filter(|s| s.is_cap()).count() as i32;
    let source_spec = steps.first().expect("non-empty").from_spec.clone();
    let target_spec = steps.last().expect("non-empty").to_spec.clone();
    Strand {
        steps,
        source_spec,
        target_spec,
        total_steps,
        cap_step_count,
        description: description.to_string(),
    }
}
