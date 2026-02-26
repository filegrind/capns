//! Cardinality Detection from Media URNs
//!
//! This module provides cardinality analysis for cap inputs and outputs.
//! Cardinality is detected from the `sequence` tag in media URNs:
//! - `media:pdf` → Single file
//! - `media:pdf;form=list` → Array of files
//!
//! Design principle: URN handling uses proper parsing, never string comparison.

use serde::{Serialize, Deserialize};

/// Cardinality of cap inputs/outputs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputCardinality {
    /// Exactly 1 item (no ;form=list tag)
    Single,
    /// Array of items (has ;form=list tag)
    Sequence,
    /// 1 or more items (cap can handle either)
    AtLeastOne,
}

impl Default for InputCardinality {
    fn default() -> Self {
        Self::Single
    }
}

impl InputCardinality {
    /// Parse cardinality from a media URN string.
    ///
    /// Looks for the `;form=list` tag to determine if this represents an array.
    pub fn from_media_urn(urn: &str) -> Self {
        if Self::has_vector_tag(urn) {
            InputCardinality::Sequence
        } else {
            InputCardinality::Single
        }
    }

    /// Check if a media URN has the sequence tag (form=list).
    fn has_vector_tag(urn: &str) -> bool {
        if let Some(params) = urn.strip_prefix("media:") {
            for param in params.split(';') {
                if param == "form=list" {
                    return true;
                }
            }
        }
        false
    }

    /// Check if this cardinality accepts multiple items
    pub fn is_multiple(&self) -> bool {
        matches!(self, Self::Sequence | Self::AtLeastOne)
    }

    /// Check if this cardinality can accept a single item
    pub fn accepts_single(&self) -> bool {
        matches!(self, Self::Single | Self::AtLeastOne)
    }

    /// Check if cardinalities are compatible for data flow
    ///
    /// Returns true if data with `source` cardinality can flow into
    /// an input expecting `self` cardinality.
    pub fn is_compatible_with(&self, source: InputCardinality) -> CardinalityCompatibility {
        match (source, self) {
            (InputCardinality::Single, InputCardinality::Single) => {
                CardinalityCompatibility::Direct
            }
            (InputCardinality::Single, InputCardinality::Sequence) => {
                CardinalityCompatibility::WrapInArray
            }
            (InputCardinality::Sequence, InputCardinality::Single) => {
                CardinalityCompatibility::RequiresFanOut
            }
            (InputCardinality::Sequence, InputCardinality::Sequence) => {
                CardinalityCompatibility::Direct
            }
            (InputCardinality::AtLeastOne, _) | (_, InputCardinality::AtLeastOne) => {
                CardinalityCompatibility::Direct
            }
        }
    }

    /// Create a media URN with this cardinality from a base URN
    pub fn apply_to_urn(&self, base_urn: &str) -> String {
        let has_vector = Self::has_vector_tag(base_urn);

        match self {
            InputCardinality::Single | InputCardinality::AtLeastOne => {
                if has_vector {
                    Self::remove_vector_tag(base_urn)
                } else {
                    base_urn.to_string()
                }
            }
            InputCardinality::Sequence => {
                if has_vector {
                    base_urn.to_string()
                } else {
                    format!("{};form=list", base_urn)
                }
            }
        }
    }

    /// Remove the sequence tag (form=list) from a URN
    fn remove_vector_tag(urn: &str) -> String {
        if let Some(params) = urn.strip_prefix("media:") {
            let filtered: Vec<&str> = params
                .split(';')
                .filter(|p| *p != "form=list")
                .collect();
            format!("media:{}", filtered.join(";"))
        } else {
            urn.to_string()
        }
    }
}

/// Result of checking cardinality compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityCompatibility {
    /// Direct flow, no transformation needed
    Direct,
    /// Need to wrap single item in array
    WrapInArray,
    /// Need to fan-out: iterate over sequence, run for each item
    RequiresFanOut,
}

/// Cardinality analysis for a cap transformation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapCardinalityInfo {
    /// Input cardinality from cap's in_spec
    pub input: InputCardinality,
    /// Output cardinality from cap's out_spec
    pub output: InputCardinality,
    /// Cap URN this applies to
    pub cap_urn: String,
}

impl CapCardinalityInfo {
    /// Create cardinality info by parsing a cap's input and output specs
    pub fn from_cap_specs(cap_urn: &str, in_spec: &str, out_spec: &str) -> Self {
        Self {
            input: InputCardinality::from_media_urn(in_spec),
            output: InputCardinality::from_media_urn(out_spec),
            cap_urn: cap_urn.to_string(),
        }
    }

    /// Describe the cardinality transformation pattern
    pub fn pattern(&self) -> CardinalityPattern {
        match (self.input, self.output) {
            (InputCardinality::Single, InputCardinality::Single) => CardinalityPattern::OneToOne,
            (InputCardinality::Single, InputCardinality::Sequence) => CardinalityPattern::OneToMany,
            (InputCardinality::Sequence, InputCardinality::Single) => CardinalityPattern::ManyToOne,
            (InputCardinality::Sequence, InputCardinality::Sequence) => {
                CardinalityPattern::ManyToMany
            }
            (InputCardinality::AtLeastOne, InputCardinality::Single) => CardinalityPattern::OneToOne,
            (InputCardinality::AtLeastOne, InputCardinality::Sequence) => {
                CardinalityPattern::OneToMany
            }
            (InputCardinality::Single, InputCardinality::AtLeastOne) => CardinalityPattern::OneToOne,
            (InputCardinality::Sequence, InputCardinality::AtLeastOne) => {
                CardinalityPattern::ManyToMany
            }
            (InputCardinality::AtLeastOne, InputCardinality::AtLeastOne) => {
                CardinalityPattern::OneToOne
            }
        }
    }
}

/// Pattern describing input/output cardinality relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardinalityPattern {
    /// Single input → Single output (e.g., resize image)
    OneToOne,
    /// Single input → Multiple outputs (e.g., PDF to pages)
    OneToMany,
    /// Multiple inputs → Single output (e.g., merge PDFs)
    ManyToOne,
    /// Multiple inputs → Multiple outputs (e.g., batch process)
    ManyToMany,
}

impl CardinalityPattern {
    /// Check if this pattern may produce multiple outputs
    pub fn produces_vector(&self) -> bool {
        matches!(self, Self::OneToMany | Self::ManyToMany)
    }

    /// Check if this pattern requires multiple inputs
    pub fn requires_vector(&self) -> bool {
        matches!(self, Self::ManyToOne | Self::ManyToMany)
    }
}

/// Analyze cardinality chain for a sequence of caps
#[derive(Debug, Clone)]
pub struct CardinalityChainAnalysis {
    /// Per-cap cardinality info
    pub cap_infos: Vec<CapCardinalityInfo>,
    /// Points where fan-out is needed (index into cap_infos)
    pub fan_out_points: Vec<usize>,
    /// Points where fan-in/collect is needed (index into cap_infos)
    pub fan_in_points: Vec<usize>,
    /// Whether the chain is valid (no impossible transitions)
    pub is_valid: bool,
    /// Error message if chain is invalid
    pub error: Option<String>,
}

impl CardinalityChainAnalysis {
    /// Analyze a chain of caps for cardinality transitions
    pub fn analyze(cap_infos: Vec<CapCardinalityInfo>) -> Self {
        if cap_infos.is_empty() {
            return Self {
                cap_infos: vec![],
                fan_out_points: vec![],
                fan_in_points: vec![],
                is_valid: true,
                error: None,
            };
        }

        let mut fan_out_points = Vec::new();
        let mut fan_in_points = Vec::new();

        let mut current_cardinality = cap_infos[0].input;

        for (i, info) in cap_infos.iter().enumerate() {
            let compatibility = info.input.is_compatible_with(current_cardinality);

            match compatibility {
                CardinalityCompatibility::Direct => {}
                CardinalityCompatibility::WrapInArray => {}
                CardinalityCompatibility::RequiresFanOut => {
                    fan_out_points.push(i);
                }
            }

            current_cardinality = info.output;
        }

        if !fan_out_points.is_empty() {
            fan_in_points.push(cap_infos.len());
        }

        Self {
            cap_infos,
            fan_out_points,
            fan_in_points,
            is_valid: true,
            error: None,
        }
    }

    /// Check if this chain requires any cardinality transformations
    pub fn requires_transformation(&self) -> bool {
        !self.fan_out_points.is_empty() || !self.fan_in_points.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== InputCardinality Tests ====================

    // TEST684: Tests InputCardinality correctly identifies single-value media URNs
    // Verifies that URNs without form=list are parsed as Single cardinality
    #[test]
    fn test684_from_media_urn_single() {
        assert_eq!(InputCardinality::from_media_urn("media:pdf"), InputCardinality::Single);
        assert_eq!(InputCardinality::from_media_urn("media:textable"), InputCardinality::Single);
        assert_eq!(InputCardinality::from_media_urn("media:integer"), InputCardinality::Single);
    }

    // TEST685: Tests InputCardinality correctly identifies list/vector media URNs
    // Verifies that URNs with form=list tag are parsed as Sequence cardinality
    #[test]
    fn test685_from_media_urn_vector() {
        assert_eq!(InputCardinality::from_media_urn("media:pdf;form=list"), InputCardinality::Sequence);
        assert_eq!(InputCardinality::from_media_urn("media:png;form=list"), InputCardinality::Sequence);
        assert_eq!(InputCardinality::from_media_urn("media:disbound-pages;textable;form=list"), InputCardinality::Sequence);
    }

    // TEST686: Tests that form=list tag position doesn't affect vector detection
    // Verifies cardinality parsing is independent of tag order in URN
    #[test]
    fn test686_from_media_urn_vector_tag_position() {
        assert_eq!(InputCardinality::from_media_urn("media:pdf;form=list"), InputCardinality::Sequence);
        assert_eq!(InputCardinality::from_media_urn("media:form=list;pdf"), InputCardinality::Sequence);
    }

    // TEST687: Tests that URN content doesn't cause false positive vector detection
    // Verifies that "sequence" in media type name doesn't trigger Sequence cardinality
    #[test]
    fn test687_from_media_urn_no_false_positives() {
        assert_eq!(InputCardinality::from_media_urn("media:sequence-data"), InputCardinality::Single);
    }

    // TEST688: Tests is_multiple method correctly identifies multi-value cardinalities
    // Verifies Single returns false while Sequence and AtLeastOne return true
    #[test]
    fn test688_is_multiple() {
        assert!(!InputCardinality::Single.is_multiple());
        assert!(InputCardinality::Sequence.is_multiple());
        assert!(InputCardinality::AtLeastOne.is_multiple());
    }

    // TEST689: Tests accepts_single method identifies cardinalities that accept single values
    // Verifies Single and AtLeastOne accept singles while Sequence does not
    #[test]
    fn test689_accepts_single() {
        assert!(InputCardinality::Single.accepts_single());
        assert!(!InputCardinality::Sequence.accepts_single());
        assert!(InputCardinality::AtLeastOne.accepts_single());
    }

    // ==================== Compatibility Tests ====================

    // TEST690: Tests cardinality compatibility for single-to-single data flow
    // Verifies Direct compatibility when both input and output are Single
    #[test]
    fn test690_compatibility_single_to_single() {
        assert_eq!(InputCardinality::Single.is_compatible_with(InputCardinality::Single), CardinalityCompatibility::Direct);
    }

    // TEST691: Tests cardinality compatibility when wrapping single value into array
    // Verifies WrapInArray compatibility when Sequence expects Single input
    #[test]
    fn test691_compatibility_single_to_vector() {
        assert_eq!(InputCardinality::Sequence.is_compatible_with(InputCardinality::Single), CardinalityCompatibility::WrapInArray);
    }

    // TEST692: Tests cardinality compatibility when unwrapping array to singles
    // Verifies RequiresFanOut compatibility when Single expects Sequence input
    #[test]
    fn test692_compatibility_vector_to_single() {
        assert_eq!(InputCardinality::Single.is_compatible_with(InputCardinality::Sequence), CardinalityCompatibility::RequiresFanOut);
    }

    // TEST693: Tests cardinality compatibility for sequence-to-sequence data flow
    // Verifies Direct compatibility when both input and output are Sequence
    #[test]
    fn test693_compatibility_vector_to_vector() {
        assert_eq!(InputCardinality::Sequence.is_compatible_with(InputCardinality::Sequence), CardinalityCompatibility::Direct);
    }

    // ==================== URN Manipulation Tests ====================

    // TEST694: Tests applying Sequence cardinality adds form=list tag to URN
    // Verifies that apply_to_urn correctly modifies URN to indicate vector form
    #[test]
    fn test694_apply_to_urn_add_vector() {
        let result = InputCardinality::Sequence.apply_to_urn("media:pdf");
        assert_eq!(result, "media:pdf;form=list");
    }

    // TEST695: Tests applying Single cardinality removes form=list tag from URN
    // Verifies that apply_to_urn correctly strips vector form indicator
    #[test]
    fn test695_apply_to_urn_remove_vector() {
        let result = InputCardinality::Single.apply_to_urn("media:pdf;form=list");
        assert_eq!(result, "media:pdf");
    }

    // TEST696: Tests apply_to_urn is idempotent when URN already matches cardinality
    // Verifies that URN remains unchanged when form already matches desired cardinality
    #[test]
    fn test696_apply_to_urn_no_change_needed() {
        let urn = "media:pdf";
        assert_eq!(InputCardinality::Single.apply_to_urn(urn), urn);
        let urn_seq = "media:pdf;form=list";
        assert_eq!(InputCardinality::Sequence.apply_to_urn(urn_seq), urn_seq);
    }

    // ==================== CapCardinalityInfo Tests ====================

    // TEST697: Tests CapCardinalityInfo correctly identifies one-to-one pattern
    // Verifies Single input and Single output result in OneToOne pattern
    #[test]
    fn test697_cap_cardinality_info_one_to_one() {
        let info = CapCardinalityInfo::from_cap_specs("cap:test", "media:pdf", "media:png");
        assert_eq!(info.input, InputCardinality::Single);
        assert_eq!(info.output, InputCardinality::Single);
        assert_eq!(info.pattern(), CardinalityPattern::OneToOne);
    }

    // TEST698: Tests CapCardinalityInfo correctly identifies one-to-many pattern
    // Verifies Single input and Sequence output result in OneToMany pattern
    #[test]
    fn test698_cap_cardinality_info_one_to_many() {
        let info = CapCardinalityInfo::from_cap_specs("cap:pdf-to-pages", "media:pdf", "media:png;form=list");
        assert_eq!(info.input, InputCardinality::Single);
        assert_eq!(info.output, InputCardinality::Sequence);
        assert_eq!(info.pattern(), CardinalityPattern::OneToMany);
    }

    // TEST699: Tests CapCardinalityInfo correctly identifies many-to-one pattern
    // Verifies Sequence input and Single output result in ManyToOne pattern
    #[test]
    fn test699_cap_cardinality_info_many_to_one() {
        let info = CapCardinalityInfo::from_cap_specs("cap:merge-pdfs", "media:pdf;form=list", "media:pdf");
        assert_eq!(info.input, InputCardinality::Sequence);
        assert_eq!(info.output, InputCardinality::Single);
        assert_eq!(info.pattern(), CardinalityPattern::ManyToOne);
    }

    // ==================== CardinalityPattern Tests ====================

    // TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors
    // Verifies OneToMany and ManyToMany return true, others return false
    #[test]
    fn test709_pattern_produces_vector() {
        assert!(!CardinalityPattern::OneToOne.produces_vector());
        assert!(CardinalityPattern::OneToMany.produces_vector());
        assert!(!CardinalityPattern::ManyToOne.produces_vector());
        assert!(CardinalityPattern::ManyToMany.produces_vector());
    }

    // TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors
    // Verifies ManyToOne and ManyToMany return true, others return false
    #[test]
    fn test710_pattern_requires_vector() {
        assert!(!CardinalityPattern::OneToOne.requires_vector());
        assert!(!CardinalityPattern::OneToMany.requires_vector());
        assert!(CardinalityPattern::ManyToOne.requires_vector());
        assert!(CardinalityPattern::ManyToMany.requires_vector());
    }

    // ==================== Chain Analysis Tests ====================

    // TEST711: Tests chain analysis for simple linear one-to-one capability chains
    // Verifies chains with no fan-out are valid and require no transformation
    #[test]
    fn test711_chain_analysis_simple_linear() {
        let infos = vec![
            CapCardinalityInfo::from_cap_specs("cap:pdf-to-png", "media:pdf", "media:png"),
            CapCardinalityInfo::from_cap_specs("cap:resize", "media:png", "media:png"),
        ];
        let analysis = CardinalityChainAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert!(analysis.fan_out_points.is_empty());
        assert!(!analysis.requires_transformation());
    }

    // TEST712: Tests chain analysis detects fan-out points in capability chains
    // Verifies chains with one-to-many transitions are marked for transformation
    #[test]
    fn test712_chain_analysis_with_fan_out() {
        let infos = vec![
            CapCardinalityInfo::from_cap_specs("cap:pdf-to-pages", "media:pdf", "media:png;form=list"),
            CapCardinalityInfo::from_cap_specs("cap:thumbnail", "media:png", "media:png"),
        ];
        let analysis = CardinalityChainAnalysis::analyze(infos);
        assert!(analysis.is_valid);
        assert_eq!(analysis.fan_out_points, vec![1]);
        assert!(analysis.requires_transformation());
    }

    // TEST713: Tests chain analysis handles empty capability chains correctly
    // Verifies empty chains are valid and require no transformation
    #[test]
    fn test713_chain_analysis_empty() {
        let analysis = CardinalityChainAnalysis::analyze(vec![]);
        assert!(analysis.is_valid);
        assert!(!analysis.requires_transformation());
    }

    // ==================== Serialization Tests ====================

    // TEST714: Tests InputCardinality serializes and deserializes correctly to/from JSON
    // Verifies JSON round-trip preserves cardinality values
    #[test]
    fn test714_cardinality_serialization() {
        let single = InputCardinality::Single;
        let json = serde_json::to_string(&single).unwrap();
        assert_eq!(json, "\"single\"");
        let deserialized: InputCardinality = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, InputCardinality::Single);
    }

    // TEST715: Tests CardinalityPattern serializes and deserializes correctly to/from JSON
    // Verifies JSON round-trip preserves pattern values with snake_case formatting
    #[test]
    fn test715_pattern_serialization() {
        let pattern = CardinalityPattern::OneToMany;
        let json = serde_json::to_string(&pattern).unwrap();
        assert_eq!(json, "\"one_to_many\"");
        let deserialized: CardinalityPattern = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, CardinalityPattern::OneToMany);
    }
}
