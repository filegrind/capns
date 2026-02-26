# Cardinality and Structure Design Document

This document defines the two orthogonal dimensions for describing media data shapes using marker tags in capns.

## The Two Dimensions

### 1. Cardinality

How many items are we dealing with?

| Marker Tag | Meaning | Examples |
|------------|---------|----------|
| (none) | Exactly one item (scalar) | one string, one number, one file |
| `list` | Zero or more items (ordered) | array of strings, list of files |

### 2. Structure

What is the internal structure of each item?

| Marker Tag | Meaning | Examples |
|------------|---------|----------|
| (none) | Indivisible, no internal fields (opaque) | raw bytes, a string, a number, a binary blob |
| `record` | Has internal key-value fields | JSON object, config file, CSV row, INI section |

## Marker Tag Syntax

Marker tags are standalone tags (no `=value`) in media URNs:

```
media:textable              # scalar, opaque (defaults)
media:textable;list         # list of opaque items
media:json;record           # scalar record (one JSON object)
media:json;list;record      # list of records (array of JSON objects)
```

## The Four Combinations

| Cardinality | Structure | Media URN Example | Real-World Example |
|-------------|-----------|-------------------|-------------------|
| scalar | opaque | `media:textable` | A single string, one PDF file's bytes |
| scalar | record | `media:json;record` | One JSON object `{"name":"foo","value":42}` |
| list | opaque | `media:file-path;list` | Array of file paths `["/a.txt", "/b.txt"]` |
| list | record | `media:json;list;record` | Array of objects, CSV rows, ndjson stream |

## Default Values

When marker tags are absent:

| Dimension | Default | Rationale |
|-----------|---------|-----------|
| Cardinality | scalar | Most caps process one thing at a time |
| Structure | opaque | Most data is treated as indivisible unless specified |

So `media:pdf` is equivalent to "one opaque PDF".

## Resolution Matrix

### Input Sources

| Source Type | Description |
|-------------|-------------|
| Direct value(s) | In-memory data passed to cap |
| File(s) | File paths provided |
| Directory/ies | Directory paths (resolve to files) |

### File Content Patterns

| Pattern | Cardinality | Structure | Detection |
|---------|-------------|-----------|-----------|
| Single blob | scalar | opaque | Default for binary files |
| Single JSON primitive | scalar | opaque | JSON string/number/bool/null |
| Single JSON object | scalar | record | JSON `{...}` |
| Single JSON array | list | (depends on elements) | JSON `[...]` |
| Line-delimited text | list | opaque | Lines of plain text |
| NDJSON (primitives) | list | opaque | JSON primitives per line |
| NDJSON (objects) | list | record | JSON objects per line |
| Single-column CSV | list | opaque | CSV with 1 column |
| Multi-column CSV | list | record | CSV with headers → each row is a record |
| INI/TOML/YAML config | scalar | record | Parsed as single config object |

---

## Resolution Tables

### Cap Expects Scalar Opaque (no marker tags)

The cap wants exactly ONE indivisible value.

| # | Input Given | Resolution | Notes |
|---|-------------|------------|-------|
| SO1 | 1 direct opaque value | Pass directly | Exact match |
| SO2 | N direct opaque values | Execute cap N times | Fan-out |
| SO3 | 1 direct record value | **Error** | Structure mismatch |
| SO4 | 1 file, opaque content | Read and pass | |
| SO5 | 1 file, list of opaques (lines) | Execute cap N times, one per line | Unwrap + fan-out |
| SO6 | 1 file, single record (JSON obj) | **Error** | Structure mismatch |
| SO7 | 1 file, list of records (CSV rows) | **Error** | Structure mismatch |
| SO8 | N files, each opaque | Execute cap N times | Fan-out |
| SO9 | 1 directory | Resolve to files, apply SO8 | |

### Cap Expects Scalar Record (`record` marker)

The cap wants exactly ONE structured value with fields.

| # | Input Given | Resolution | Notes |
|---|-------------|------------|-------|
| SR1 | 1 direct record value | Pass directly | Exact match |
| SR2 | N direct record values | Execute cap N times | Fan-out |
| SR3 | 1 direct opaque value | **Error** | Structure mismatch |
| SR4 | 1 file, single record (JSON obj) | Parse and pass | |
| SR5 | 1 file, single record (INI/TOML) | Parse and pass | |
| SR6 | 1 file, list of records (CSV) | Execute cap N times, one per row | Unwrap + fan-out |
| SR7 | 1 file, list of records (NDJSON) | Execute cap N times, one per line | Unwrap + fan-out |
| SR8 | 1 file, opaque content | **Error** | Structure mismatch |
| SR9 | N files, each single record | Execute cap N times | Fan-out |
| SR10 | 1 directory | Resolve to files, apply SR9 | |

### Cap Expects List Opaque (`list` marker)

The cap wants a LIST of indivisible values (e.g., to merge, archive, batch).

| # | Input Given | Resolution | Notes |
|---|-------------|------------|-------|
| LO1 | 1 direct opaque value | Wrap as `[value]` | Promote scalar to list |
| LO2 | N direct opaque values | Pass as list | Exact match |
| LO3 | 1 direct record value | **Error** | Structure mismatch |
| LO4 | 1 file, opaque content | Wrap as `[content]` | Promote |
| LO5 | 1 file, list of opaques (lines) | Pass as list | |
| LO6 | 1 file, JSON array of primitives | Pass as list | |
| LO7 | 1 file, list of records | **Error** | Structure mismatch |
| LO8 | N files, each opaque | Create list of contents | |
| LO9 | 1 directory | Resolve to file list | |
| LO10 | N directories | Flatten all files to list | |

### Cap Expects List Record (`list;record` markers)

The cap wants a LIST of structured values (e.g., bulk record processing).

| # | Input Given | Resolution | Notes |
|---|-------------|------------|-------|
| LR1 | 1 direct record value | Wrap as `[record]` | Promote |
| LR2 | N direct record values | Pass as list | Exact match |
| LR3 | 1 direct opaque value | **Error** | Structure mismatch |
| LR4 | 1 file, single record | Wrap as `[record]` | Promote |
| LR5 | 1 file, list of records (CSV) | Pass rows as list | |
| LR6 | 1 file, list of records (NDJSON) | Pass objects as list | |
| LR7 | 1 file, JSON array of objects | Pass as list | |
| LR8 | 1 file, opaque content | **Error** | Structure mismatch |
| LR9 | N files, each single record | Create list of records | |
| LR10 | N files, each list of records | Flatten all into single list | |
| LR11 | 1 directory of record files | Resolve and create list | |

---

## Output Handling

When fan-out occurs (cap executed N times), outputs are collected:

| Cap Output | Collection Strategy |
|------------|---------------------|
| scalar opaque | Collect as `list` opaque |
| scalar `record` | Collect as `list;record` |
| `list` opaque | Flatten into single `list` opaque |
| `list;record` | Flatten into single `list;record` |

---

## Implementation Status

### Completed

1. **Marker tag parsing in `MediaUrn`**
   - `is_list()` - checks for `list` marker tag
   - `is_record()` - checks for `record` marker tag
   - `is_scalar()` - returns `!is_list()`
   - `is_opaque()` - returns `!is_record()`

2. **Constants updated**
   - All `MEDIA_*` constants use marker tags
   - Example: `MEDIA_DISBOUND_PAGE = "media:disbound-page;list;textable"`

3. **Legacy `form=` migration**
   - `form=scalar` → removed (scalar is default)
   - `form=list` → `list` marker tag
   - `form=map` → `record` marker tag

### Remaining Work

1. **Cardinality module updates**
   - `src/planner/cardinality.rs` - handle both dimensions
   - Add structure compatibility checking

2. **Resolution logic**
   - `src/planner/plan_builder.rs` - consider both cardinality AND structure
   - `src/bifaci/plugin_runtime.rs` - structure-based coercion

3. **Fan-out implementation**
   - Automatic execution multiplication when cardinality mismatches
   - Output collection and flattening

---

## API Reference

### MediaUrn Methods

```rust
impl MediaUrn {
    /// Returns true if this URN has the `list` marker tag
    pub fn is_list(&self) -> bool;

    /// Returns true if this URN does NOT have the `list` marker tag
    pub fn is_scalar(&self) -> bool;

    /// Returns true if this URN has the `record` marker tag
    pub fn is_record(&self) -> bool;

    /// Returns true if this URN does NOT have the `record` marker tag
    pub fn is_opaque(&self) -> bool;
}
```

### Standard Media Constants

```rust
// Scalar opaque (defaults)
pub const MEDIA_TEXTABLE: &str = "media:textable";
pub const MEDIA_PDF: &str = "media:pdf";

// Scalar record
pub const MEDIA_FILE_METADATA: &str = "media:file-metadata;record;textable";
pub const MEDIA_DOCUMENT_OUTLINE: &str = "media:document-outline;record;textable";

// List opaque
pub const MEDIA_FILE_PATH_ARRAY: &str = "media:file-path;list";

// List record
pub const MEDIA_DISBOUND_PAGE: &str = "media:disbound-page;list;textable";
```

---

## Open Questions

1. **Structure coercion** - Can a record ever be coerced to opaque (by serializing to JSON string)? Or is this always an error?
   no. yes it's an error.

2. **Nested structures** - What about `record` where a field value is itself a record? Is this just "record" or do we need depth?
   no. no need to worry about depth.

3. **Arrays in records** - A JSON object with an array field - is this `record` containing a list? How deep does structure annotation go?
   first level only.

4. **Binary records** - Structured binary formats (protobuf, msgpack) - are these `record` or opaque?
   opaque

---

*Document updated to reflect marker tag implementation.*
