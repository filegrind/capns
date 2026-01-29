# Media URN Catalog

This document catalogs all distinct Media URNs used across the codebase.

## Format

```
media:<type>[;subtype=<subtype>][;v=<version>][;<coercion-tag>]...
```

---

## Core Primitive Types

These are the fundamental data types with defined constants in `capns::media_urn`.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:void` | `MEDIA_VOID` | No data / empty |
| `media:textable;form=scalar` | `MEDIA_STRING` | UTF-8 text string |
| `media:integer` | `MEDIA_INTEGER` | Integer value |
| `media:textable;numeric;form=scalar` | `MEDIA_NUMBER` | Floating-point number |
| `media:bool;textable;form=scalar` | `MEDIA_BOOLEAN` | Boolean value (true/false) |
| `media:textable;form=map` | `MEDIA_OBJECT` | JSON object |
| `media:bytes` | `MEDIA_BINARY` | Raw binary data |

---

## Array Types

Array variants of primitive types.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:textable;form=list` | `MEDIA_STRING_ARRAY` | Array of strings |
| `media:integer;textable;form=list` | `MEDIA_INTEGER_ARRAY` | Array of integers |
| `media:textable;numeric;form=list` | `MEDIA_NUMBER_ARRAY` | Array of numbers |
| `media:bool;textable;form=list` | `MEDIA_BOOLEAN_ARRAY` | Array of booleans |
| `media:object-array;textable;form=list` | `MEDIA_OBJECT_ARRAY` | Array of objects |

---

## Domain-Specific Types

### File & Listing References

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:listing-id;textable;form=scalar` | `MEDIA_LISTING_ID` | Reference to a listing UUID |
| `media:task-id;textable;form=scalar` | `MEDIA_TASK_ID` | Reference to a task UUID |
| `media:file-path-array;textable;form=list` | `MEDIA_FILE_PATH_ARRAY` | Array of file paths |

### Model Management Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:download-result;textable;form=map` | `MEDIA_DOWNLOAD_OUTPUT` | Model download result |
| `media:load-output;textable;form=map` | `MEDIA_LOAD_OUTPUT` | Model load result |
| `media:unload-output;textable;form=map` | `MEDIA_UNLOAD_OUTPUT` | Model unload result |
| `media:model-list;textable;form=map` | `MEDIA_LIST_OUTPUT` | Model listing result |
| `media:model-status;textable;form=map` | `MEDIA_STATUS_OUTPUT` | Status query result |
| `media:model-contents;textable;form=map` | `MEDIA_CONTENTS_OUTPUT` | Contents listing result |
| `media:form=list;real;continuous;signed;numeric;textable;embedding` | `MEDIA_GENERATE_OUTPUT` | Generation result |
| `media:manage-output;textable;form=map` | `MEDIA_MANAGE_OUTPUT` | Model management result |

### LLM & Inference Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:generated-text;textable;form=map` | `MEDIA_LLM_INFERENCE_OUTPUT` | LLM text generation result |
| `media:vision-inference-output;textable;form=map` | - | Vision model analysis result |
| `media:form=list;real;continuous;signed;numeric;textable;embedding` | - | Embedding generation result |
| `media:json;textable;form=map` | `MEDIA_STRUCTURED_QUERY_OUTPUT` | Structured query result |
| `media:textable;form=list` | `MEDIA_QUESTIONS_ARRAY` | Array of generated questions |

### Document Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:extract-metadata-output;textable;form=map` | - | Document metadata extraction result |
| `media:extract-outline-output;textable;form=map` | - | Document outline extraction result |
| `media:disbound-pages;textable;form=list` | - | Document grinding/chunking result |
| `media:frontmatter-summary-output;textable;form=map` | - | Frontmatter summary result |
| `media:thumbnail-output;bytes;visual` | - | Thumbnail image output |

### Audio/Video Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:transcription;textable;form=map` | - | Audio transcription result |
| `media:image-caption;textable;form=map` | - | Image caption result |

### Image Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:form=list;real;continuous;signed;numeric;textable;embedding` | - | Image embedding result |
| `media:embedding-vector;textable;form=list` | - | Batch image embedding result |

---

## Image Types

Image types use subtype to specify format.

| Media URN | Description |
|-----------|-------------|
| `media:image;subtype=png;bytes;visual` | PNG image |
| `media:image;subtype=jpeg;bytes;visual` | JPEG image |
| `media:image;subtype=gif;bytes;visual` | GIF image |
| `media:image;subtype=webp;bytes;visual` | WebP image |

---

## Application Types

Application-specific formats using subtype.

| Media URN | Description |
|-----------|-------------|
| `media:application;subtype=pdf;bytes;visual` | PDF document |
| `media:application;subtype=json;textable;form=map` | JSON data |
| `media:application;subtype=xml;textable;form=map` | XML data |
| `media:application;subtype=epub+zip;bytes` | EPUB e-book |

---

## Text Types

Text formats with subtype for specific languages/formats.

| Media URN | Description |
|-----------|-------------|
| `media:subtype=plain;textable;form=scalar` | Plain text |
| `media:subtype=html;textable;form=map` | HTML markup |
| `media:subtype=markdown;textable` | Markdown |
| `media:subtype=x-rust;textable` | Rust source code |
| `media:subtype=x-rst;textable` | reStructuredText |

---

## Special Types

| Media URN | Description |
|-----------|-------------|
| `media:unknown` | Unknown/unrecognized type |
| `media:result;textable;form=map` | Generic result wrapper |

---

## Usage Guidelines

### When to Use Each Type

1. **For chip content storage:**
   - Text content: `MEDIA_STRING`
   - JSON/structured data: `MEDIA_OBJECT`
   - Binary/embeddings: `MEDIA_BINARY`
   - Images: `media:image;subtype=<format>;bytes;visual`

2. **For cap arguments:**
   - Use primitive types (`string`, `integer`, `number`, `boolean`)
   - Use array types for lists
   - Use domain types for specific references (`listing-id`, `task-id`)

3. **For cap outputs:**
   - Use specific output types where defined
   - Fall back to `MEDIA_OBJECT` for generic JSON responses
   - Use `MEDIA_BINARY` for raw binary outputs

### Type Detection Methods

The `MediaUrn` struct provides these detection methods:

```rust
let urn = MediaUrn::from_string("media:textable;form=scalar")?;

urn.is_text()    // true for string, text/*
urn.is_json()    // true for object, object-array, *-array
urn.is_binary()  // true for binary, image/*
```

---

## Adding New Types

When adding a new media URN type:

1. Add constant to `capns/src/media_urn.rs` if frequently used
2. Update `media_urn_to_extension()` in chip storage if it needs file extension mapping
3. Update Swift `String+MediaUrn.swift` helpers if needed for UI
4. Document in this catalog

---

## Coercion Tags

Media URNs include coercion tags that enable type coercion and matching. Tags declare what representations a type can be reduced to.

### Standard Coercion Tags

| Tag | Meaning | When to Use |
|-----|---------|-------------|
| `textable` | Can be coerced to plain UTF-8 text | strings, numbers, booleans, IDs (via .toString()) |
| `binary` | Raw bytes representation | images, PDFs, audio, video |
| `numeric` | Numeric operations valid | integers, numbers (NOT numeric strings) |
| `scalar` | Single atomic value | primitives, IDs (NOT arrays, NOT objects) |
| `sequence` | Ordered collection of items | arrays of any type |
| `map` | Key-value structure | objects, maps |
| `visual` | Has visual rendering | images, PDFs (renderable) |

### Matching with Coercion Tags

A cap requiring textable input can specify `media:textable` and match ANY type reducible to text:

```
cap:in=media:textable;op=prompt;out=media:textable;form=map
```

This matches: string, integer, number, boolean, object, arrays - anything with `textable`.

**A cap requiring numeric input:**
```
cap:in=media:numeric;op=calculate;out=media:textable;numeric;form=scalar
```
Matches: integer, number, integer-array, number-array

**A cap requiring map structure:**
```
cap:in=media:form=map;op=transform;out=media:textable;form=map
```
Matches: object, text/html (has DOM structure)

---

## Type Coercion Rules

When a cap requires `media:textable`, the system can automatically coerce:

| Source Type | Coercion Method |
|-------------|-----------------|
| `integer`, `number` | `ToString()` |
| `boolean` | `"true"` / `"false"` |
| `object`, `*-array` | JSON stringify |
| `listing-id`, `task-id` | UUID string |
| `text/*` | Direct (already text) |
| `string` | Direct (already text) |

Types with only `binary` (images, PDF, etc.) cannot be coerced to text without an explicit conversion cap.
