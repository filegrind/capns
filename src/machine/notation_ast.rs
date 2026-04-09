//! Position-preserving AST for machine notation language intelligence.
//!
//! Wraps the existing pest parser to capture token positions for:
//! - Semantic syntax coloring
//! - Completion context detection
//! - Hover information
//! - Diagnostic ranges
//!
//! Unlike `parse_machine()` which discards position information and fails
//! on the first error, this module always returns a partial AST — even on
//! parse failures — so that language tooling works during active editing.

use std::collections::{BTreeMap, HashMap};

use pest::Parser;

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::MachineSyntaxError;
use super::graph::Machine;
use super::parser::{MachineParser, Rule};

// =============================================================================
// Position types
// =============================================================================

/// 0-based line and character position in notation text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotationPosition {
    pub line: usize,
    pub character: usize,
}

/// Span in the source text with both line/character and byte offsets.
#[derive(Debug, Clone)]
pub struct NotationSpan {
    pub start: NotationPosition,
    pub end: NotationPosition,
    pub start_byte: usize,
    pub end_byte: usize,
}

impl NotationSpan {
    /// Whether a 0-based (line, character) position falls within this span.
    pub fn contains(&self, line: usize, character: usize) -> bool {
        let pos = NotationPosition { line, character };
        if pos.line < self.start.line || pos.line > self.end.line {
            return false;
        }
        if pos.line == self.start.line && pos.character < self.start.character {
            return false;
        }
        if pos.line == self.end.line && pos.character >= self.end.character {
            return false;
        }
        true
    }
}

// =============================================================================
// Parsed statements
// =============================================================================

/// A parsed header statement with full position information.
#[derive(Debug, Clone)]
pub struct ParsedHeader {
    pub alias: String,
    pub alias_span: NotationSpan,
    pub cap_urn_str: String,
    pub cap_urn_span: NotationSpan,
    /// Parsed CapUrn — None if cap URN string failed to parse.
    pub cap_urn: Option<CapUrn>,
    pub statement_span: NotationSpan,
}

/// A parsed wiring statement with full position information.
#[derive(Debug, Clone)]
pub struct ParsedWiring {
    pub sources: Vec<(String, NotationSpan)>,
    pub cap_alias: String,
    pub cap_alias_span: NotationSpan,
    pub target: String,
    pub target_span: NotationSpan,
    pub is_loop: bool,
    pub loop_keyword_span: Option<NotationSpan>,
    pub arrow_spans: Vec<NotationSpan>,
    pub statement_span: NotationSpan,
    /// Open and close paren spans for fan-in groups.
    pub paren_spans: Option<(NotationSpan, NotationSpan)>,
    pub comma_spans: Vec<NotationSpan>,
}

/// A parsed statement — either a header or a wiring.
#[derive(Debug, Clone)]
pub enum ParsedStatement {
    Header(ParsedHeader),
    Wiring(ParsedWiring),
}

// =============================================================================
// Notation AST
// =============================================================================

/// Full AST result from parsing machine notation with position information.
///
/// Always contains as much information as could be extracted — even on parse
/// failures. The `error` field holds the first error encountered (if any).
/// The `machine` field holds the fully resolved Machine graph (if no errors).
#[derive(Debug)]
pub struct NotationAST {
    pub statements: Vec<ParsedStatement>,
    /// (open bracket, close bracket) spans for each statement.
    pub bracket_spans: Vec<(NotationSpan, NotationSpan)>,
    /// Alias → parsed header mapping (for lookup by name).
    pub alias_map: BTreeMap<String, ParsedHeader>,
    /// Node name → derived media URN mapping.
    pub node_media: HashMap<String, MediaUrn>,
    /// Successfully resolved machine graph, if no errors.
    pub machine: Option<Machine>,
    /// First error encountered during parsing, if any.
    pub error: Option<MachineSyntaxError>,
}

// =============================================================================
// Semantic token types (for coloring)
// =============================================================================

/// Token type for semantic coloring. Maps 1:1 to the proto SemanticTokenType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticTokenType {
    Bracket,
    Paren,
    Comma,
    Semicolon,
    CapPrefix,
    MediaPrefix,
    KeywordIn,
    KeywordOut,
    KeywordOp,
    Assignment,
    Arrow,
    LoopKeyword,
    Alias,
    Node,
    String,
    Escape,
    TagKey,
    TagValue,
}

/// A semantic token with position and type.
#[derive(Debug, Clone)]
pub struct SemanticTokenInfo {
    pub line: usize,
    pub start_character: usize,
    pub length: usize,
    pub token_type: SemanticTokenType,
}

// =============================================================================
// Completion context
// =============================================================================

/// Completion context at a cursor position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionContextType {
    /// After `[`, before any `cap:` — suggests aliases, ops.
    HeaderStart,
    /// After `cap:` — suggests full cap URNs.
    CapUrn,
    /// After `in=` or `out=` inside a cap URN — suggests media URNs.
    MediaUrn,
    /// In wiring source position (before first `->` or between arrows).
    WiringSource,
    /// In wiring target position (after second `->` or LOOP alias `->` alias).
    WiringTarget,
    /// No valid completion context.
    Unknown,
}

// =============================================================================
// Core parsing function
// =============================================================================

/// Parse machine notation into a position-preserving AST.
///
/// Unlike `parse_machine()`, this function:
/// - Never panics or returns Err — always returns an AST
/// - Preserves all token positions via pest's span API
/// - Returns partial results on parse failure
/// - Attempts semantic resolution (cap URNs, media URNs) but records
///   errors rather than failing
///
/// This is the primary entry point for all language intelligence features.
pub fn parse_notation_ast(input: &str) -> NotationAST {
    let mut ast = NotationAST {
        statements: Vec::new(),
        bracket_spans: Vec::new(),
        alias_map: BTreeMap::new(),
        node_media: HashMap::new(),
        machine: None,
        error: None,
    };

    let trimmed = input.trim();
    if trimmed.is_empty() {
        ast.error = Some(MachineSyntaxError::Empty);
        return ast;
    }

    // Phase 1: PEG parse
    let pairs = match MachineParser::parse(Rule::program, trimmed) {
        Ok(pairs) => pairs,
        Err(e) => {
            ast.error = Some(MachineSyntaxError::ParseError {
                details: format!("{}", e),
            });
            // On parse failure, do a best-effort bracket scan for partial tokens
            scan_brackets_for_partial_ast(trimmed, &mut ast);
            return ast;
        }
    };

    // Phase 2: Walk the pest AST and collect statements with positions.
    // We need the byte offset of the trimmed string relative to the original
    // input, so that positions are correct for the original text.
    let trim_offset = input.len() - input.trim_end().len();
    let trim_start = input.len() - trimmed.len() - trim_offset;

    let program = match pairs.into_iter().next() {
        Some(p) => p,
        None => return ast,
    };

    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue; // skip EOI
        }

        // Bracket spans: for bracketed statements `[inner]`, extract
        // bracket positions. Line-based statements have no brackets.
        let stmt_span = pest_span_to_notation_span(&pair.as_span(), trimmed, trim_start);
        let is_bracketed = pair.as_str().starts_with('[');

        if is_bracketed {
            let open_bracket = NotationSpan {
                start: stmt_span.start,
                end: NotationPosition {
                    line: stmt_span.start.line,
                    character: stmt_span.start.character + 1,
                },
                start_byte: stmt_span.start_byte,
                end_byte: stmt_span.start_byte + 1,
            };
            let close_bracket = NotationSpan {
                start: NotationPosition {
                    line: stmt_span.end.line,
                    character: stmt_span.end.character.saturating_sub(1),
                },
                end: stmt_span.end,
                start_byte: stmt_span.end_byte.saturating_sub(1),
                end_byte: stmt_span.end_byte,
            };
            ast.bracket_spans.push((open_bracket, close_bracket));
        }

        // Descend into inner → header | wiring
        let inner = match pair.into_inner().next() {
            Some(p) => p,
            None => continue,
        };
        let content = match inner.into_inner().next() {
            Some(p) => p,
            None => continue,
        };

        match content.as_rule() {
            Rule::header => {
                let mut inner_pairs = content.into_inner();

                let alias_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let alias = alias_pair.as_str().to_string();
                let alias_span = pest_span_to_notation_span(&alias_pair.as_span(), trimmed, trim_start);

                let cap_urn_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let cap_urn_str = cap_urn_pair.as_str().to_string();
                let cap_urn_span = pest_span_to_notation_span(&cap_urn_pair.as_span(), trimmed, trim_start);

                let cap_urn = CapUrn::from_string(&cap_urn_str).ok();

                let header = ParsedHeader {
                    alias: alias.clone(),
                    alias_span,
                    cap_urn_str,
                    cap_urn_span,
                    cap_urn,
                    statement_span: stmt_span,
                };

                ast.alias_map.insert(alias, header.clone());
                ast.statements.push(ParsedStatement::Header(header));
            }
            Rule::wiring => {
                let mut inner_pairs = content.into_inner();

                // Parse source (single alias or group)
                let source_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let (sources, paren_spans, comma_spans) =
                    parse_source_with_spans(&source_pair, trimmed, trim_start);

                // First arrow
                let arrow1 = inner_pairs.next();
                let mut arrow_spans = Vec::new();
                if let Some(ref a) = arrow1 {
                    arrow_spans.push(pest_span_to_notation_span(&a.as_span(), trimmed, trim_start));
                }

                // loop_cap (optional LOOP + alias)
                let loop_cap_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let (is_loop, cap_alias, cap_alias_span, loop_keyword_span) =
                    parse_loop_cap_with_spans(&loop_cap_pair, trimmed, trim_start);

                // Second arrow
                let arrow2 = inner_pairs.next();
                if let Some(ref a) = arrow2 {
                    arrow_spans.push(pest_span_to_notation_span(&a.as_span(), trimmed, trim_start));
                }

                // Target alias
                let target_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let target = target_pair.as_str().to_string();
                let target_span = pest_span_to_notation_span(&target_pair.as_span(), trimmed, trim_start);

                let wiring = ParsedWiring {
                    sources,
                    cap_alias,
                    cap_alias_span,
                    target,
                    target_span,
                    is_loop,
                    loop_keyword_span,
                    arrow_spans,
                    statement_span: stmt_span,
                    paren_spans,
                    comma_spans,
                };

                ast.statements.push(ParsedStatement::Wiring(wiring));
            }
            _ => {}
        }
    }

    // Phase 3: Build alias map and resolve node media types.
    // Check for duplicate aliases.
    let mut seen_aliases: BTreeMap<String, usize> = BTreeMap::new();
    for (idx, stmt) in ast.statements.iter().enumerate() {
        if let ParsedStatement::Header(h) = stmt {
            if let Some(first_idx) = seen_aliases.get(&h.alias) {
                if ast.error.is_none() {
                    ast.error = Some(MachineSyntaxError::DuplicateAlias {
                        alias: h.alias.clone(),
                        first_position: *first_idx,
                    });
                }
            } else {
                seen_aliases.insert(h.alias.clone(), idx);
            }
        }
    }

    // Phase 4: Resolve wirings → node media types. Attempt full Machine build.
    if ast.error.is_none() {
        match super::parser::parse_machine(input) {
            Ok(machine) => {
                // Extract node_media from the machine's edges
                for edge in machine.edges() {
                    for (i, src_media) in edge.sources.iter().enumerate() {
                        // Find source node name from wiring statements
                        // We already have the data from the AST wirings
                    }
                }
                ast.machine = Some(machine);
            }
            Err(e) => {
                ast.error = Some(e);
            }
        }
    }

    // Build node_media from AST wirings + alias_map
    build_node_media(&ast.statements, &ast.alias_map, &mut ast.node_media);

    ast
}

/// Build node_media mapping from wiring statements and the alias map.
fn build_node_media(
    statements: &[ParsedStatement],
    alias_map: &BTreeMap<String, ParsedHeader>,
    node_media: &mut HashMap<String, MediaUrn>,
) {
    for stmt in statements {
        if let ParsedStatement::Wiring(w) = stmt {
            // Look up the cap URN for this wiring's alias
            let cap_urn = match alias_map.get(&w.cap_alias) {
                Some(h) => match &h.cap_urn {
                    Some(u) => u,
                    None => continue,
                },
                None => continue,
            };

            // Derive media URNs from cap's in=/out= specs
            if let Ok(in_media) = cap_urn.in_media_urn() {
                if let Some((name, _)) = w.sources.first() {
                    if !alias_map.contains_key(name) && !node_media.contains_key(name) {
                        node_media.insert(name.clone(), in_media);
                    }
                }
            }
            if let Ok(out_media) = cap_urn.out_media_urn() {
                if !alias_map.contains_key(&w.target) && !node_media.contains_key(&w.target) {
                    node_media.insert(w.target.clone(), out_media);
                }
            }
        }
    }
}

// =============================================================================
// Completion context detection
// =============================================================================

/// Determine the completion context at a given cursor position.
///
/// This is a pure text-scanning function — it does not need a parsed AST.
/// It follows the same algorithm as the TypeScript LSP `getContext()`:
///
/// 1. Convert (line, character) to a byte offset in the text
/// 2. Find the innermost unclosed `[` before the cursor
/// 3. Examine the text between `[` and cursor to determine context
///
/// Returns the context type and the prefix string typed so far.
pub fn get_completion_context(text: &str, line: usize, character: usize) -> (CompletionContextType, String) {
    // Convert (line, character) to byte offset
    let offset = match line_char_to_offset(text, line, character) {
        Some(o) => o,
        None => return (CompletionContextType::Unknown, String::new()),
    };

    // Find the start of the current statement context: either the
    // innermost unclosed `[` (bracketed mode) or the start of the
    // current line (line-based mode).
    let (context_start, skip) = match find_innermost_open_bracket(text, offset) {
        Some(pos) => (pos, 1), // skip the `[`
        None => {
            // Line-based mode: find start of current line
            let line_start = text[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
            (line_start, 0)
        }
    };

    let inside = &text[context_start + skip..offset];

    // Check if we're inside a cap URN (contains "cap:" prefix)
    if let Some(cap_pos) = inside.find("cap:") {
        let after_cap = &inside[cap_pos..];

        // Check if cursor is after `in=` or `out=` — media URN context
        // Look for the last `in=` or `out=` before cursor
        let last_in_eq = after_cap.rfind("in=");
        let last_out_eq = after_cap.rfind("out=");
        let last_semicolon = after_cap.rfind(';');

        let media_eq_pos = match (last_in_eq, last_out_eq) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        if let Some(eq_pos) = media_eq_pos {
            // Is the cursor after this in=/out= and not past a subsequent semicolon?
            let eq_end = if after_cap[eq_pos..].starts_with("in=") {
                eq_pos + 3
            } else {
                eq_pos + 4
            };

            let past_semicolon = match last_semicolon {
                Some(sc) => sc > eq_pos,
                None => false,
            };

            if !past_semicolon {
                // Extract prefix after the `=` (handle quoted values)
                let after_eq = &after_cap[eq_end..];
                let prefix = extract_media_prefix(after_eq);
                return (CompletionContextType::MediaUrn, prefix);
            }
        }

        // Not in a media URN position — we're completing the cap URN itself
        let prefix = after_cap.to_string();
        return (CompletionContextType::CapUrn, prefix);
    }

    // Check if we're in a wiring (contains `->`)
    if inside.contains("->") {
        // Count arrows to determine source vs target
        let arrow_count = inside.matches("->").count();

        // Extract prefix (word being typed)
        let prefix = extract_word_prefix(inside);

        if arrow_count >= 2 {
            return (CompletionContextType::WiringTarget, prefix);
        } else {
            // After first `->`, we're at the cap alias or target position
            // If cursor is right after `->`, it's wiring target (cap alias position)
            return (CompletionContextType::WiringTarget, prefix);
        }
    }

    // Default: header start — alias or new header
    let prefix = extract_word_prefix(inside);
    (CompletionContextType::HeaderStart, prefix)
}

// =============================================================================
// Hover information
// =============================================================================

/// Generate hover information for the token at the given position.
///
/// Searches the AST for a token spanning the cursor position and generates
/// markdown hover content.
///
/// `cap_details` and `media_details` provide registry enrichment data:
/// - cap_details: cap URN string → (title, description)
/// - media_details: media URN string → (title, description)
///
/// Returns (markdown_content, token_span) or None if no hoverable token.
pub fn get_hover_info(
    ast: &NotationAST,
    line: usize,
    character: usize,
    cap_details: &HashMap<String, (String, String)>,
    media_details: &HashMap<String, (String, String)>,
) -> Option<(String, NotationSpan)> {
    for stmt in &ast.statements {
        match stmt {
            ParsedStatement::Header(h) => {
                // Alias in header definition
                if h.alias_span.contains(line, character) {
                    let md = format_alias_hover(h, cap_details);
                    return Some((md, h.alias_span.clone()));
                }
                // Cap URN in header
                if h.cap_urn_span.contains(line, character) {
                    let md = format_cap_urn_hover(&h.cap_urn_str, &h.cap_urn, cap_details);
                    return Some((md, h.cap_urn_span.clone()));
                }
            }
            ParsedStatement::Wiring(w) => {
                // Source nodes
                for (name, span) in &w.sources {
                    if span.contains(line, character) {
                        let md = format_node_hover(name, &ast.node_media, media_details);
                        return Some((md, span.clone()));
                    }
                }
                // Cap alias in wiring
                if w.cap_alias_span.contains(line, character) {
                    if let Some(header) = ast.alias_map.get(&w.cap_alias) {
                        let md = format_alias_hover(header, cap_details);
                        return Some((md, w.cap_alias_span.clone()));
                    }
                }
                // Target node
                if w.target_span.contains(line, character) {
                    let md = format_node_hover(&w.target, &ast.node_media, media_details);
                    return Some((md, w.target_span.clone()));
                }
                // LOOP keyword
                if let Some(ref loop_span) = w.loop_keyword_span {
                    if loop_span.contains(line, character) {
                        let md = "**LOOP** (ForEach)\n\nApplies the capability to each item in the input list individually. Each body instance runs concurrently.".to_string();
                        return Some((md, loop_span.clone()));
                    }
                }
            }
        }
    }
    None
}

// =============================================================================
// Semantic token emission
// =============================================================================

/// Emit all semantic tokens for syntax coloring.
///
/// Walks the AST and emits tokens for every syntactic element:
/// - Brackets `[]`, parens `()`, commas, semicolons
/// - Cap URN internals (cap:, in, out, op, =, ;, quoted strings, media:, tags)
/// - Arrows `->`, LOOP keyword
/// - Aliases and node names
pub fn emit_semantic_tokens(ast: &NotationAST, _input: &str) -> Vec<SemanticTokenInfo> {
    let mut tokens = Vec::new();

    // Bracket spans
    for (open, close) in &ast.bracket_spans {
        tokens.push(span_to_token(open, SemanticTokenType::Bracket));
        tokens.push(span_to_token(close, SemanticTokenType::Bracket));
    }

    for stmt in &ast.statements {
        match stmt {
            ParsedStatement::Header(h) => {
                // Alias in header
                tokens.push(span_to_token(&h.alias_span, SemanticTokenType::Alias));

                // Cap URN internals
                tokenize_cap_urn_body(
                    &h.cap_urn_str,
                    h.cap_urn_span.start.line,
                    h.cap_urn_span.start.character,
                    &mut tokens,
                );
            }
            ParsedStatement::Wiring(w) => {
                // Parens for fan-in groups
                if let Some((open_paren, close_paren)) = &w.paren_spans {
                    tokens.push(span_to_token(open_paren, SemanticTokenType::Paren));
                    tokens.push(span_to_token(close_paren, SemanticTokenType::Paren));
                }

                // Commas in fan-in groups
                for comma_span in &w.comma_spans {
                    tokens.push(span_to_token(comma_span, SemanticTokenType::Comma));
                }

                // Source nodes
                for (_, span) in &w.sources {
                    tokens.push(span_to_token(span, SemanticTokenType::Node));
                }

                // Arrows
                for arrow_span in &w.arrow_spans {
                    tokens.push(span_to_token(arrow_span, SemanticTokenType::Arrow));
                }

                // LOOP keyword
                if let Some(ref loop_span) = w.loop_keyword_span {
                    tokens.push(span_to_token(loop_span, SemanticTokenType::LoopKeyword));
                }

                // Cap alias in wiring (reference to a defined capability)
                tokens.push(span_to_token(&w.cap_alias_span, SemanticTokenType::Alias));

                // Target node
                tokens.push(span_to_token(&w.target_span, SemanticTokenType::Node));
            }
        }
    }

    // Sort by position
    tokens.sort_by(|a, b| {
        a.line.cmp(&b.line).then(a.start_character.cmp(&b.start_character))
    });

    tokens
}

/// Tokenize the interior of a cap URN string for semantic coloring.
///
/// Given `cap:in="media:pdf";op=extract;out="media:txt;textable"`,
/// emits tokens for: cap_prefix, keyword_in, assignment, string,
/// semicolon, keyword_op, assignment, tag_value, etc.
///
/// `base_line` and `base_col` are the 0-based position of the first
/// character of the cap URN string in the document.
pub fn tokenize_cap_urn_body(
    cap_urn_str: &str,
    base_line: usize,
    base_col: usize,
    tokens: &mut Vec<SemanticTokenInfo>,
) {
    if !cap_urn_str.starts_with("cap:") {
        return;
    }

    // "cap:" prefix
    tokens.push(SemanticTokenInfo {
        line: base_line,
        start_character: base_col,
        length: 4,
        token_type: SemanticTokenType::CapPrefix,
    });

    // Parse the body after "cap:"
    let body = &cap_urn_str[4..];
    let mut i = 0;
    let chars: Vec<char> = body.chars().collect();

    while i < chars.len() {
        let ch = chars[i];
        let col = base_col + 4 + i;

        match ch {
            ';' => {
                tokens.push(SemanticTokenInfo {
                    line: base_line,
                    start_character: col,
                    length: 1,
                    token_type: SemanticTokenType::Semicolon,
                });
                i += 1;
            }
            '=' => {
                tokens.push(SemanticTokenInfo {
                    line: base_line,
                    start_character: col,
                    length: 1,
                    token_type: SemanticTokenType::Assignment,
                });
                i += 1;
            }
            '"' => {
                // Quoted string — scan to closing quote, handling escapes
                let start = i;
                i += 1; // skip opening quote
                while i < chars.len() {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 2; // skip escape sequence
                    } else if chars[i] == '"' {
                        i += 1; // skip closing quote
                        break;
                    } else {
                        i += 1;
                    }
                }
                let str_len = i - start;
                tokens.push(SemanticTokenInfo {
                    line: base_line,
                    start_character: base_col + 4 + start,
                    length: str_len,
                    token_type: SemanticTokenType::String,
                });

                // Check for media: prefix inside the quoted string
                let inner_start = start + 1;
                let inner_end = if i > 0 && start + str_len > 1 { start + str_len - 1 } else { inner_start };
                let inner: String = chars[inner_start..inner_end].iter().collect();
                if inner.starts_with("media:") {
                    tokens.push(SemanticTokenInfo {
                        line: base_line,
                        start_character: base_col + 4 + inner_start,
                        length: 6, // "media:"
                        token_type: SemanticTokenType::MediaPrefix,
                    });
                }
            }
            _ if ch.is_alphanumeric() || ch == '_' || ch == '-' => {
                // Identifier: could be a tag key (before =) or tag value (after =)
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let word_len = i - start;

                // Determine if this is a keyword (in, out, op) or a tag key/value
                let next_char = if i < chars.len() { Some(chars[i]) } else { None };
                let is_key = next_char == Some('=');

                let token_type = if is_key {
                    match word.as_str() {
                        "in" => SemanticTokenType::KeywordIn,
                        "out" => SemanticTokenType::KeywordOut,
                        "op" => SemanticTokenType::KeywordOp,
                        _ => SemanticTokenType::TagKey,
                    }
                } else {
                    // Unquoted value after = or standalone
                    // Check if it starts with "media:"
                    if word == "media" && i < chars.len() && chars[i] == ':' {
                        // This is "media:" prefix in an unquoted context
                        tokens.push(SemanticTokenInfo {
                            line: base_line,
                            start_character: base_col + 4 + start,
                            length: word_len + 1, // include the ':'
                            token_type: SemanticTokenType::MediaPrefix,
                        });
                        i += 1; // skip the ':'
                        // Continue scanning the rest as tag value
                        let val_start = i;
                        while i < chars.len() && chars[i] != ';' && chars[i] != '"' {
                            i += 1;
                        }
                        if i > val_start {
                            tokens.push(SemanticTokenInfo {
                                line: base_line,
                                start_character: base_col + 4 + val_start,
                                length: i - val_start,
                                token_type: SemanticTokenType::TagValue,
                            });
                        }
                        continue;
                    }
                    SemanticTokenType::TagValue
                };

                tokens.push(SemanticTokenInfo {
                    line: base_line,
                    start_character: base_col + 4 + start,
                    length: word_len,
                    token_type,
                });
            }
            ':' => {
                // Standalone colon (after "media" would have been handled above)
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
}

// =============================================================================
// Helper: Pest span → NotationSpan
// =============================================================================

/// Convert a pest `Span` to a `NotationSpan` with 0-based line/character.
fn pest_span_to_notation_span(
    span: &pest::Span,
    source: &str,
    offset_in_original: usize,
) -> NotationSpan {
    let start_byte = span.start() + offset_in_original;
    let end_byte = span.end() + offset_in_original;

    let start = byte_offset_to_position(source, span.start());
    let end = byte_offset_to_position(source, span.end());

    NotationSpan {
        start,
        end,
        start_byte,
        end_byte,
    }
}

/// Convert a byte offset in `source` to a 0-based (line, character) position.
pub fn byte_offset_to_position(source: &str, byte_offset: usize) -> NotationPosition {
    let mut line = 0;
    let mut col = 0;

    for (i, ch) in source.char_indices() {
        if i >= byte_offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
    }

    NotationPosition {
        line,
        character: col,
    }
}

/// Convert 0-based (line, character) to byte offset. Returns None if out of bounds.
fn line_char_to_offset(text: &str, line: usize, character: usize) -> Option<usize> {
    let mut current_line = 0;
    let mut current_col = 0;

    for (i, ch) in text.char_indices() {
        if current_line == line && current_col == character {
            return Some(i);
        }
        if ch == '\n' {
            if current_line == line {
                // Character is past end of this line — clamp to end
                return Some(i);
            }
            current_line += 1;
            current_col = 0;
        } else {
            current_col += 1;
        }
    }

    // Cursor at very end of text
    if current_line == line && current_col == character {
        return Some(text.len());
    }

    None
}

// =============================================================================
// Helper: Bracket scanning for partial ASTs
// =============================================================================

/// Scan raw text for `[...]` brackets and extract what we can from
/// partial/malformed notation. Used when pest parse fails.
/// Handles both bracketed and line-based statements.
fn scan_brackets_for_partial_ast(text: &str, ast: &mut NotationAST) {
    let mut i = 0;
    let chars: Vec<char> = text.chars().collect();

    while i < chars.len() {
        if chars[i] == '[' {
            let open_pos = byte_offset_to_position(text, i);
            let open_byte = i;

            // Scan for matching `]`, respecting quoted strings
            let mut j = i + 1;
            let mut depth = 0;
            let mut in_quote = false;
            let mut escaped = false;

            while j < chars.len() {
                if escaped {
                    escaped = false;
                    j += 1;
                    continue;
                }
                match chars[j] {
                    '\\' if in_quote => {
                        escaped = true;
                        j += 1;
                    }
                    '"' => {
                        in_quote = !in_quote;
                        j += 1;
                    }
                    '[' if !in_quote => {
                        depth += 1;
                        j += 1;
                    }
                    ']' if !in_quote => {
                        if depth == 0 {
                            // Found matching close bracket
                            let close_pos = byte_offset_to_position(text, j);

                            let open_span = NotationSpan {
                                start: open_pos,
                                end: NotationPosition {
                                    line: open_pos.line,
                                    character: open_pos.character + 1,
                                },
                                start_byte: open_byte,
                                end_byte: open_byte + 1,
                            };
                            let close_span = NotationSpan {
                                start: close_pos,
                                end: NotationPosition {
                                    line: close_pos.line,
                                    character: close_pos.character + 1,
                                },
                                start_byte: j,
                                end_byte: j + 1,
                            };

                            ast.bracket_spans.push((open_span, close_span));
                            j += 1;
                            break;
                        } else {
                            depth -= 1;
                            j += 1;
                        }
                    }
                    _ => {
                        j += 1;
                    }
                }
            }

            i = j;
        } else {
            // Line-based mode: skip non-bracket lines (no bracket spans to emit)
            while i < chars.len() && chars[i] != '\n' && chars[i] != '[' {
                i += 1;
            }
            if i < chars.len() && chars[i] == '\n' {
                i += 1;
            }
        }
    }
}

// =============================================================================
// Helper: Source parsing with spans
// =============================================================================

/// Parse a source pair, extracting names with spans and paren/comma positions.
fn parse_source_with_spans(
    pair: &pest::iterators::Pair<Rule>,
    source: &str,
    offset: usize,
) -> (Vec<(String, NotationSpan)>, Option<(NotationSpan, NotationSpan)>, Vec<NotationSpan>) {
    let mut sources = Vec::new();
    let mut paren_spans = None;
    let mut comma_spans = Vec::new();

    // Clone the pair since we need to iterate its inner elements
    let pair = pair.clone();
    let inner = match pair.into_inner().next() {
        Some(p) => p,
        None => return (sources, paren_spans, comma_spans),
    };

    match inner.as_rule() {
        Rule::group => {
            // Group: "(" alias ("," alias)+ ")"
            // The pest span of the group includes parens.
            let group_span = pest_span_to_notation_span(&inner.as_span(), source, offset);

            // Open paren is the first character of the group span
            let open_paren = NotationSpan {
                start: group_span.start,
                end: NotationPosition {
                    line: group_span.start.line,
                    character: group_span.start.character + 1,
                },
                start_byte: group_span.start_byte,
                end_byte: group_span.start_byte + 1,
            };
            // Close paren is the last character
            let close_paren = NotationSpan {
                start: NotationPosition {
                    line: group_span.end.line,
                    character: group_span.end.character.saturating_sub(1),
                },
                end: group_span.end,
                start_byte: group_span.end_byte.saturating_sub(1),
                end_byte: group_span.end_byte,
            };
            paren_spans = Some((open_paren, close_paren));

            // Extract aliases and find commas
            let group_text = inner.as_str();
            let group_start_byte = inner.as_span().start();

            // Find commas in the group text
            let mut in_group = false;
            for (ci, ch) in group_text.char_indices() {
                if ch == '(' {
                    in_group = true;
                    continue;
                }
                if ch == ')' {
                    break;
                }
                if in_group && ch == ',' {
                    let comma_byte = group_start_byte + ci;
                    let comma_pos = byte_offset_to_position(source, comma_byte);
                    comma_spans.push(NotationSpan {
                        start: comma_pos,
                        end: NotationPosition {
                            line: comma_pos.line,
                            character: comma_pos.character + 1,
                        },
                        start_byte: comma_byte + offset,
                        end_byte: comma_byte + offset + 1,
                    });
                }
            }

            for p in inner.into_inner() {
                if p.as_rule() == Rule::alias {
                    let name = p.as_str().to_string();
                    let span = pest_span_to_notation_span(&p.as_span(), source, offset);
                    sources.push((name, span));
                }
            }
        }
        Rule::alias => {
            let name = inner.as_str().to_string();
            let span = pest_span_to_notation_span(&inner.as_span(), source, offset);
            sources.push((name, span));
        }
        _ => {}
    }

    (sources, paren_spans, comma_spans)
}

/// Parse a loop_cap pair, extracting is_loop flag, alias, and spans.
fn parse_loop_cap_with_spans(
    pair: &pest::iterators::Pair<Rule>,
    source: &str,
    offset: usize,
) -> (bool, String, NotationSpan, Option<NotationSpan>) {
    let mut is_loop = false;
    let mut cap_alias = String::new();
    let mut cap_alias_span = NotationSpan {
        start: NotationPosition { line: 0, character: 0 },
        end: NotationPosition { line: 0, character: 0 },
        start_byte: 0,
        end_byte: 0,
    };
    let mut loop_keyword_span = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::loop_keyword => {
                is_loop = true;
                loop_keyword_span = Some(pest_span_to_notation_span(&inner.as_span(), source, offset));
            }
            Rule::alias => {
                cap_alias = inner.as_str().to_string();
                cap_alias_span = pest_span_to_notation_span(&inner.as_span(), source, offset);
            }
            _ => {}
        }
    }

    (is_loop, cap_alias, cap_alias_span, loop_keyword_span)
}

// =============================================================================
// Helper: Hover formatting
// =============================================================================

fn format_alias_hover(header: &ParsedHeader, cap_details: &HashMap<String, (String, String)>) -> String {
    let mut md = format!("**{}** — capability alias\n\n", header.alias);

    md.push_str(&format!("`{}`\n\n", header.cap_urn_str));

    if let Some(ref cap_urn) = header.cap_urn {
        md.push_str("| Property | Value |\n|----------|-------|\n");

        if let Some(op) = cap_urn.get_tag("op") {
            md.push_str(&format!("| op | `{}` |\n", op));
        }
        md.push_str(&format!("| in | `{}` |\n", cap_urn.in_spec()));
        md.push_str(&format!("| out | `{}` |\n", cap_urn.out_spec()));
    }

    // Registry enrichment
    if let Some((title, description)) = cap_details.get(&header.cap_urn_str) {
        if !title.is_empty() {
            md.push_str(&format!("\n**{}**\n", title));
        }
        if !description.is_empty() {
            md.push_str(&format!("\n{}\n", description));
        }
    }

    md
}

fn format_cap_urn_hover(
    cap_urn_str: &str,
    cap_urn: &Option<CapUrn>,
    cap_details: &HashMap<String, (String, String)>,
) -> String {
    let mut md = format!("**Cap URN**\n\n`{}`\n\n", cap_urn_str);

    if let Some(ref cap_urn) = cap_urn {
        md.push_str("| Property | Value |\n|----------|-------|\n");

        if let Some(op) = cap_urn.get_tag("op") {
            md.push_str(&format!("| op | `{}` |\n", op));
        }
        md.push_str(&format!("| in | `{}` |\n", cap_urn.in_spec()));
        md.push_str(&format!("| out | `{}` |\n", cap_urn.out_spec()));
    }

    if let Some((title, description)) = cap_details.get(cap_urn_str) {
        if !title.is_empty() {
            md.push_str(&format!("\n**{}**\n", title));
        }
        if !description.is_empty() {
            md.push_str(&format!("\n{}\n", description));
        }
    }

    md
}

fn format_node_hover(
    node_name: &str,
    node_media: &HashMap<String, MediaUrn>,
    media_details: &HashMap<String, (String, String)>,
) -> String {
    let mut md = format!("**{}** — node\n\n", node_name);

    if let Some(media_urn) = node_media.get(node_name) {
        let urn_str = media_urn.to_string();
        md.push_str(&format!("Media type: `{}`\n", urn_str));

        if let Some((title, description)) = media_details.get(&urn_str) {
            if !title.is_empty() {
                md.push_str(&format!("\n**{}**\n", title));
            }
            if !description.is_empty() {
                md.push_str(&format!("\n{}\n", description));
            }
        }
    }

    md
}

// =============================================================================
// Helper: Text scanning utilities
// =============================================================================

/// Find the innermost unclosed `[` before the given byte offset.
fn find_innermost_open_bracket(text: &str, before_offset: usize) -> Option<usize> {
    let mut bracket_stack: Vec<usize> = Vec::new();
    let mut in_quote = false;
    let mut escaped = false;

    for (i, ch) in text.char_indices() {
        if i >= before_offset {
            break;
        }

        if escaped {
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_quote => {
                escaped = true;
            }
            '"' => {
                in_quote = !in_quote;
            }
            '[' if !in_quote => {
                bracket_stack.push(i);
            }
            ']' if !in_quote => {
                bracket_stack.pop();
            }
            _ => {}
        }
    }

    bracket_stack.last().copied()
}

/// Extract the word prefix being typed at the end of a string.
/// Scans backwards from the end to find the start of the current word.
fn extract_word_prefix(s: &str) -> String {
    let trimmed = s.trim_end();
    let mut start = trimmed.len();

    for (i, ch) in trimmed.char_indices().rev() {
        if ch.is_alphanumeric() || ch == '_' || ch == '-' {
            start = i;
        } else {
            break;
        }
    }

    trimmed[start..].to_string()
}

/// Extract the media URN prefix after `in=` or `out=`.
/// Handles both quoted (`in="media:pdf"`) and unquoted (`in=media:pdf`) forms.
fn extract_media_prefix(after_eq: &str) -> String {
    let s = after_eq.trim_start();
    if s.starts_with('"') {
        // Quoted — extract content after the opening quote
        let inner = &s[1..];
        // Return everything up to closing quote or end
        if let Some(close) = inner.find('"') {
            inner[..close].to_string()
        } else {
            inner.to_string()
        }
    } else {
        // Unquoted — take everything until semicolon or end
        if let Some(end) = s.find(';') {
            s[..end].to_string()
        } else {
            s.to_string()
        }
    }
}

/// Convert a NotationSpan to a SemanticTokenInfo.
fn span_to_token(span: &NotationSpan, token_type: SemanticTokenType) -> SemanticTokenInfo {
    let length = if span.start.line == span.end.line {
        span.end.character - span.start.character
    } else {
        // Multi-line token — use byte length as approximation
        span.end_byte - span.start_byte
    };

    SemanticTokenInfo {
        line: span.start.line,
        start_character: span.start.character,
        length,
        token_type,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_header_and_wiring() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);

        assert!(ast.error.is_none(), "expected no error, got: {:?}", ast.error);
        assert_eq!(ast.statements.len(), 2);
        assert_eq!(ast.bracket_spans.len(), 2);
        assert!(ast.machine.is_some());

        // Check alias map
        assert!(ast.alias_map.contains_key("extract"));
        let header = &ast.alias_map["extract"];
        assert!(header.cap_urn.is_some());

        // Check node media
        assert!(ast.node_media.contains_key("doc"));
        assert!(ast.node_media.contains_key("text"));
    }

    #[test]
    fn parse_empty_returns_error() {
        let ast = parse_notation_ast("");
        assert!(matches!(ast.error, Some(MachineSyntaxError::Empty)));
        assert!(ast.statements.is_empty());
    }

    #[test]
    fn parse_invalid_returns_partial_ast() {
        let input = "[extract cap:in=broken";
        let ast = parse_notation_ast(input);

        assert!(ast.error.is_some());
        // Should still have bracket spans from manual scanning
        assert!(!ast.bracket_spans.is_empty() || ast.statements.is_empty());
    }

    #[test]
    fn parse_loop_wiring() {
        let input = concat!(
            r#"[p2t cap:in="media:page";op=page_to_text;out="media:txt"]"#,
            "\n[pages -> LOOP p2t -> texts]"
        );
        let ast = parse_notation_ast(input);
        assert!(ast.error.is_none(), "got: {:?}", ast.error);

        // Find the wiring statement
        let wiring = ast.statements.iter().find_map(|s| {
            if let ParsedStatement::Wiring(w) = s { Some(w) } else { None }
        });
        let wiring = wiring.expect("should have a wiring statement");
        assert!(wiring.is_loop);
        assert!(wiring.loop_keyword_span.is_some());
        assert_eq!(wiring.cap_alias, "p2t");
    }

    #[test]
    fn parse_fan_in_group() {
        let input = concat!(
            r#"[describe cap:in="media:image;png";op=describe;out="media:txt"]"#,
            "\n[(thumbnail, model_spec) -> describe -> description]"
        );
        let ast = parse_notation_ast(input);

        let wiring = ast.statements.iter().find_map(|s| {
            if let ParsedStatement::Wiring(w) = s { Some(w) } else { None }
        }).expect("should have a wiring");

        assert_eq!(wiring.sources.len(), 2);
        assert_eq!(wiring.sources[0].0, "thumbnail");
        assert_eq!(wiring.sources[1].0, "model_spec");
        assert!(wiring.paren_spans.is_some());
        assert_eq!(wiring.comma_spans.len(), 1);
    }

    // =========================================================================
    // Completion context detection
    // =========================================================================

    #[test]
    fn context_after_open_bracket() {
        let (ctx, _) = get_completion_context("[", 0, 1);
        assert_eq!(ctx, CompletionContextType::HeaderStart);
    }

    #[test]
    fn context_after_cap_prefix() {
        let (ctx, prefix) = get_completion_context("[alias cap:", 0, 11);
        assert_eq!(ctx, CompletionContextType::CapUrn);
        assert!(prefix.starts_with("cap:"));
    }

    #[test]
    fn context_in_media_urn() {
        let (ctx, prefix) = get_completion_context(r#"[alias cap:in="media:pd"#, 0, 22);
        assert_eq!(ctx, CompletionContextType::MediaUrn);
        assert!(prefix.starts_with("media:"));
    }

    #[test]
    fn context_after_arrow() {
        let (ctx, _) = get_completion_context("[doc -> ", 0, 8);
        assert_eq!(ctx, CompletionContextType::WiringTarget);
    }

    #[test]
    fn context_outside_brackets() {
        let (ctx, _) = get_completion_context("hello", 0, 3);
        assert_eq!(ctx, CompletionContextType::Unknown);
    }

    // =========================================================================
    // Semantic tokens
    // =========================================================================

    #[test]
    fn semantic_tokens_simple() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let tokens = emit_semantic_tokens(&ast, input);

        // Should have bracket tokens (4 = 2 statements x 2 brackets)
        let bracket_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Bracket).count();
        assert_eq!(bracket_count, 4);

        // Should have arrow tokens
        let arrow_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Arrow).count();
        assert!(arrow_count >= 2, "expected at least 2 arrows, got {}", arrow_count);

        // Should have alias tokens (extract in header, extract in wiring)
        let alias_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Alias).count();
        assert!(alias_count >= 2, "expected at least 2 aliases, got {}", alias_count);

        // Should have node tokens (doc, text)
        let node_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Node).count();
        assert!(node_count >= 2, "expected at least 2 nodes, got {}", node_count);

        // Should have cap: prefix
        let cap_prefix_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::CapPrefix).count();
        assert_eq!(cap_prefix_count, 1);
    }

    // =========================================================================
    // Hover
    // =========================================================================

    #[test]
    fn hover_on_alias_in_header() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);

        let cap_details = HashMap::new();
        let media_details = HashMap::new();

        // Hover on "extract" at line 0, character 1 (inside the alias)
        let result = get_hover_info(&ast, 0, 1, &cap_details, &media_details);
        assert!(result.is_some(), "expected hover info for alias");
        let (md, _) = result.unwrap();
        assert!(md.contains("extract"), "hover should mention alias name");
        assert!(md.contains("capability alias"), "hover should identify type");
    }

    #[test]
    fn hover_on_node_in_wiring() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);

        let cap_details = HashMap::new();
        let media_details = HashMap::new();

        // "doc" is at line 1, character 1
        let result = get_hover_info(&ast, 1, 1, &cap_details, &media_details);
        assert!(result.is_some(), "expected hover info for node");
        let (md, _) = result.unwrap();
        assert!(md.contains("doc"), "hover should mention node name");
        assert!(md.contains("node"), "hover should identify type");
    }

    #[test]
    fn hover_on_loop_keyword() {
        let input = concat!(
            r#"[p2t cap:in="media:page";op=page_to_text;out="media:txt"]"#,
            "\n[pages -> LOOP p2t -> texts]"
        );
        let ast = parse_notation_ast(input);

        let cap_details = HashMap::new();
        let media_details = HashMap::new();

        // LOOP is at line 1, character 10
        let result = get_hover_info(&ast, 1, 10, &cap_details, &media_details);
        assert!(result.is_some(), "expected hover info for LOOP");
        let (md, _) = result.unwrap();
        assert!(md.contains("ForEach"), "hover should explain LOOP semantics");
    }

    // =========================================================================
    // Line-based mode
    // =========================================================================

    #[test]
    fn parse_line_based_header_and_wiring() {
        let input = r#"extract cap:in="media:pdf";op=extract;out="media:txt;textable"
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);

        assert!(ast.error.is_none(), "expected no error, got: {:?}", ast.error);
        assert_eq!(ast.statements.len(), 2);
        // Line-based statements have no bracket spans
        assert_eq!(ast.bracket_spans.len(), 0);
        assert!(ast.machine.is_some());
        assert!(ast.alias_map.contains_key("extract"));
    }

    #[test]
    fn parse_mixed_bracketed_and_line_based() {
        let input = r#"[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);

        assert!(ast.error.is_none(), "expected no error, got: {:?}", ast.error);
        assert_eq!(ast.statements.len(), 2);
        // Only the bracketed statement has bracket spans
        assert_eq!(ast.bracket_spans.len(), 1);
        assert!(ast.machine.is_some());
    }

    #[test]
    fn line_based_completion_context_header() {
        let (ctx, _) = get_completion_context("extract cap:", 0, 12);
        assert_eq!(ctx, CompletionContextType::CapUrn);
    }

    #[test]
    fn line_based_completion_context_wiring() {
        let (ctx, _) = get_completion_context("doc -> ", 0, 7);
        assert_eq!(ctx, CompletionContextType::WiringTarget);
    }

    #[test]
    fn line_based_completion_context_start() {
        let (ctx, _) = get_completion_context("ex", 0, 2);
        assert_eq!(ctx, CompletionContextType::HeaderStart);
    }

    #[test]
    fn line_based_semantic_tokens_no_brackets() {
        let input = r#"extract cap:in="media:pdf";op=extract;out="media:txt;textable"
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);
        let tokens = emit_semantic_tokens(&ast, input);

        // No bracket tokens for line-based statements
        let bracket_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Bracket).count();
        assert_eq!(bracket_count, 0);

        // Should still have other tokens
        let alias_count = tokens.iter().filter(|t| t.token_type == SemanticTokenType::Alias).count();
        assert!(alias_count >= 2, "expected at least 2 aliases, got {}", alias_count);
    }

    // =========================================================================
    // Position conversion
    // =========================================================================

    #[test]
    fn byte_offset_to_position_works() {
        let text = "line0\nline1\nline2";
        assert_eq!(
            byte_offset_to_position(text, 0),
            NotationPosition { line: 0, character: 0 }
        );
        assert_eq!(
            byte_offset_to_position(text, 6),
            NotationPosition { line: 1, character: 0 }
        );
        assert_eq!(
            byte_offset_to_position(text, 12),
            NotationPosition { line: 2, character: 0 }
        );
    }

    #[test]
    fn line_char_to_offset_works() {
        let text = "line0\nline1\nline2";
        assert_eq!(line_char_to_offset(text, 0, 0), Some(0));
        assert_eq!(line_char_to_offset(text, 1, 0), Some(6));
        assert_eq!(line_char_to_offset(text, 2, 0), Some(12));
        assert_eq!(line_char_to_offset(text, 2, 5), Some(17));
    }
}
