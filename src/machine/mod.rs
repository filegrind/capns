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
//! Two equally valid statement forms — bracketed and line-based:
//!
//! ```text
//! extract cap:in="media:pdf";op=extract_text;out="media:txt;textable"
//! embed cap:in="media:textable";op=generate_embeddings;out="media:embedding-vector;record;textable"
//! doc -> extract -> text
//! text -> embed -> vectors
//! ```
//!
//! Bracketed form wraps each statement in `[...]` and can be freely mixed:
//!
//! ```text
//! [extract cap:in="media:pdf";op=extract_text;out="media:txt;textable"]
//! [doc -> extract -> text]
//! ```
//!
//! There are two kinds of statement:
//!
//! - **Headers**: `alias cap:...` — define a capability with an alias
//! - **Wirings**: `src -> alias -> dst` — connect nodes through capabilities
//!
//! Fan-in groups: `(a, b) -> alias -> dst` — multiple sources feed one cap.
//! Loop edges: `src -> LOOP alias -> dst` — ForEach iteration semantics.

pub mod error;
pub mod graph;
pub mod notation_ast;
pub mod parser;
pub mod serializer;

pub use error::{MachineAbstractionError, MachineSyntaxError};
pub use graph::{Machine, MachineEdge, MachineRun, MachineRunStatus};
pub use notation_ast::{
    parse_notation_ast, get_completion_context, get_hover_info, emit_semantic_tokens,
    byte_offset_to_position,
    CompletionContextType, NotationAST, NotationPosition, NotationSpan,
    ParsedHeader, ParsedStatement, ParsedWiring, SemanticTokenInfo, SemanticTokenType,
};
pub use parser::parse_machine;
pub use serializer::NotationFormat;
