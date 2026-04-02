//! Machine notation — compact, round-trippable DAG path identifiers
//!
//! Machine notation replaces the DOT file format for describing capability
//! transformation paths. It provides:
//!
//! - A typed graph model (`Machine`, `MachineEdge`) with semantic equivalence
//! - A compact textual format for serialization
//! - Conversion from resolved paths (`Strand`)
//!
//! ## Format
//!
//! ```text
//! [extract cap:in="media:pdf";op=extract_text;out="media:txt;textable"]
//! [embed cap:in="media:textable";op=generate_embeddings;out="media:embedding-vector;record;textable"]
//! [doc -> extract -> text]
//! [text -> embed -> vectors]
//! ```
//!
//! Statements are enclosed in `[...]`. There are two kinds:
//!
//! - **Headers**: `[alias cap:...]` — define a capability with an alias
//! - **Wirings**: `[src -> alias -> dst]` — connect nodes through capabilities
//!
//! Fan-in groups: `[(a, b) -> alias -> dst]` — multiple sources feed one cap.
//! Loop edges: `[src -> LOOP alias -> dst]` — ForEach iteration semantics.

pub mod error;
pub mod graph;
pub mod parser;
pub mod serializer;

pub use error::MachineSyntaxError;
pub use graph::{Machine, MachineEdge, MachineRun, MachineRunStatus};
pub use parser::parse_machine;
