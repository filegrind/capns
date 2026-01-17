# Media URN Catalog

This document catalogs all distinct Media URNs used across the codebase.

## Format

```
media:type=<type>[;subtype=<subtype>][;v=<version>][;<coercion-tag>]...
```

---

## Core Primitive Types

These are the fundamental data types with defined constants in `capns::media_urn`.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=void;v=1` | `MEDIA_VOID` | No data / empty |
| `media:type=string;v=1;textable;scalar` | `MEDIA_STRING` | UTF-8 text string |
| `media:type=integer;v=1;textable;numeric;scalar` | `MEDIA_INTEGER` | Integer value |
| `media:type=number;v=1;textable;numeric;scalar` | `MEDIA_NUMBER` | Floating-point number |
| `media:type=boolean;v=1;textable;scalar` | `MEDIA_BOOLEAN` | Boolean value (true/false) |
| `media:type=object;v=1;textable;keyed` | `MEDIA_OBJECT` | JSON object |
| `media:type=binary;v=1;binary` | `MEDIA_BINARY` | Raw binary data |

---

## Array Types

Array variants of primitive types.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=string-array;v=1;textable;sequence` | `MEDIA_STRING_ARRAY` | Array of strings |
| `media:type=integer-array;v=1;textable;numeric;sequence` | `MEDIA_INTEGER_ARRAY` | Array of integers |
| `media:type=number-array;v=1;textable;numeric;sequence` | `MEDIA_NUMBER_ARRAY` | Array of numbers |
| `media:type=boolean-array;v=1;textable;sequence` | `MEDIA_BOOLEAN_ARRAY` | Array of booleans |
| `media:type=object-array;v=1;textable;keyed;sequence` | `MEDIA_OBJECT_ARRAY` | Array of objects |

---

## Domain-Specific Types

### File & Listing References

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=listing-id;v=1;textable;scalar` | `MEDIA_LISTING_ID` | Reference to a listing UUID |
| `media:type=task-id;v=1;textable;scalar` | `MEDIA_TASK_ID` | Reference to a task UUID |
| `media:type=file-path-array;v=1;textable;sequence` | `MEDIA_FILE_PATH_ARRAY` | Array of file paths |

### Model Management Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=download-output;v=1;textable;keyed` | `MEDIA_DOWNLOAD_OUTPUT` | Model download result |
| `media:type=load-output;v=1;textable;keyed` | `MEDIA_LOAD_OUTPUT` | Model load result |
| `media:type=unload-output;v=1;textable;keyed` | `MEDIA_UNLOAD_OUTPUT` | Model unload result |
| `media:type=list-output;v=1;textable;keyed` | `MEDIA_LIST_OUTPUT` | Model listing result |
| `media:type=status-output;v=1;textable;keyed` | `MEDIA_STATUS_OUTPUT` | Status query result |
| `media:type=contents-output;v=1;textable;keyed` | `MEDIA_CONTENTS_OUTPUT` | Contents listing result |
| `media:type=generate-output;v=1;textable;keyed` | `MEDIA_GENERATE_OUTPUT` | Generation result |
| `media:type=manage-output;v=1;textable;keyed` | `MEDIA_MANAGE_OUTPUT` | Model management result |

### LLM & Inference Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=llm-inference-output;v=1;textable;keyed` | `MEDIA_LLM_INFERENCE_OUTPUT` | LLM text generation result |
| `media:type=vision-inference-output;v=1;textable;keyed` | - | Vision model analysis result |
| `media:type=embeddings-output;v=1;textable;keyed` | - | Embedding generation result |
| `media:type=structured-query-output;v=1;textable;keyed` | `MEDIA_STRUCTURED_QUERY_OUTPUT` | Structured query result |
| `media:type=questions-array;v=1;textable;sequence` | `MEDIA_QUESTIONS_ARRAY` | Array of generated questions |

### Document Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=extract-metadata-output;v=1;textable;keyed` | - | Document metadata extraction result |
| `media:type=extract-outline-output;v=1;textable;keyed` | - | Document outline extraction result |
| `media:type=grind-output;v=1;textable;keyed;sequence` | - | Document grinding/chunking result |
| `media:type=frontmatter-summary-output;v=1;textable;keyed` | - | Frontmatter summary result |
| `media:type=thumbnail-output;v=1;binary;visual` | - | Thumbnail image output |

### Audio/Video Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=transcription-output;v=1;textable;keyed` | - | Audio transcription result |
| `media:type=caption-output;v=1;textable;keyed` | - | Image caption result |

### Image Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=image-embeddings-output;v=1;textable;keyed` | - | Image embedding result |
| `media:type=image-embeddings-batch-output;v=1;textable;keyed;sequence` | - | Batch image embedding result |

---

## Image Types

Image types use subtype to specify format.

| Media URN | Description |
|-----------|-------------|
| `media:type=image;subtype=png;binary;visual` | PNG image |
| `media:type=image;subtype=jpeg;binary;visual` | JPEG image |
| `media:type=image;subtype=gif;binary;visual` | GIF image |
| `media:type=image;subtype=webp;binary;visual` | WebP image |

---

## Application Types

Application-specific formats using subtype.

| Media URN | Description |
|-----------|-------------|
| `media:type=application;subtype=pdf;binary;visual` | PDF document |
| `media:type=application;subtype=json;textable;keyed` | JSON data |
| `media:type=application;subtype=xml;textable;keyed` | XML data |
| `media:type=application;subtype=epub+zip;binary` | EPUB e-book |

---

## Text Types

Text formats with subtype for specific languages/formats.

| Media URN | Description |
|-----------|-------------|
| `media:type=text;subtype=plain;textable;scalar` | Plain text |
| `media:type=text;subtype=html;textable;keyed` | HTML markup |
| `media:type=text;subtype=markdown;textable` | Markdown |
| `media:type=text;subtype=x-rust;textable` | Rust source code |
| `media:type=text;subtype=x-rst;textable` | reStructuredText |

---

## Special Types

| Media URN | Description |
|-----------|-------------|
| `media:type=unknown;v=1` | Unknown/unrecognized type |
| `media:type=result;v=1;textable;keyed` | Generic result wrapper |

---

## Usage Guidelines

### When to Use Each Type

1. **For chip content storage:**
   - Text content: `MEDIA_STRING`
   - JSON/structured data: `MEDIA_OBJECT`
   - Binary/embeddings: `MEDIA_BINARY`
   - Images: `media:type=image;subtype=<format>;binary;visual`

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
let urn = MediaUrn::from_string("media:type=string;v=1;textable;scalar")?;

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
| `keyed` | Key-value structure | objects, maps |
| `visual` | Has visual rendering | images, PDFs (renderable) |

### Matching with Coercion Tags

A cap requiring textable input can specify `media:textable` and match ANY type reducible to text:

```
cap:in=media:textable;op=prompt;out=media:type=object;v=1;textable;keyed
```

This matches: string, integer, number, boolean, object, arrays - anything with `textable`.

**A cap requiring numeric input:**
```
cap:in=media:numeric;op=calculate;out=media:type=number;v=1;textable;numeric;scalar
```
Matches: integer, number, integer-array, number-array

**A cap requiring keyed structure:**
```
cap:in=media:keyed;op=transform;out=media:type=object;v=1;textable;keyed
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
