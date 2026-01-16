# Media URN Catalog

This document catalogs all distinct Media URNs used across the codebase.

## Format

```
media:type=<type>[;subtype=<subtype>][;v=<version>]
```

---

## Core Primitive Types

These are the fundamental data types with defined constants in `capns::media_urn`.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=void;v=1` | `MEDIA_VOID` | No data / empty |
| `media:type=string;v=1` | `MEDIA_STRING` | UTF-8 text string |
| `media:type=integer;v=1` | `MEDIA_INTEGER` | Integer value |
| `media:type=number;v=1` | `MEDIA_NUMBER` | Floating-point number |
| `media:type=boolean;v=1` | `MEDIA_BOOLEAN` | Boolean value (true/false) |
| `media:type=object;v=1` | `MEDIA_OBJECT` | JSON object |
| `media:type=binary;v=1` | `MEDIA_BINARY` | Raw binary data |

---

## Array Types

Array variants of primitive types.

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=string-array;v=1` | `MEDIA_STRING_ARRAY` | Array of strings |
| `media:type=integer-array;v=1` | `MEDIA_INTEGER_ARRAY` | Array of integers |
| `media:type=number-array;v=1` | `MEDIA_NUMBER_ARRAY` | Array of numbers |
| `media:type=boolean-array;v=1` | `MEDIA_BOOLEAN_ARRAY` | Array of booleans |
| `media:type=object-array;v=1` | `MEDIA_OBJECT_ARRAY` | Array of objects |

---

## Domain-Specific Types

### File & Listing References

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=listing-id;v=1` | `MEDIA_LISTING_ID` | Reference to a listing UUID |
| `media:type=task-id;v=1` | `MEDIA_TASK_ID` | Reference to a task UUID |
| `media:type=file-path-array;v=1` | `MEDIA_FILE_PATH_ARRAY` | Array of file paths |

### Model Management Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=download-output;v=1` | `MEDIA_DOWNLOAD_OUTPUT` | Model download result |
| `media:type=load-output;v=1` | `MEDIA_LOAD_OUTPUT` | Model load result |
| `media:type=unload-output;v=1` | `MEDIA_UNLOAD_OUTPUT` | Model unload result |
| `media:type=list-output;v=1` | `MEDIA_LIST_OUTPUT` | Model listing result |
| `media:type=status-output;v=1` | `MEDIA_STATUS_OUTPUT` | Status query result |
| `media:type=contents-output;v=1` | `MEDIA_CONTENTS_OUTPUT` | Contents listing result |
| `media:type=generate-output;v=1` | `MEDIA_GENERATE_OUTPUT` | Generation result |

### LLM & Inference Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=llm-inference-output;v=1` | `MEDIA_LLM_INFERENCE_OUTPUT` | LLM text generation result |
| `media:type=vision-inference-output;v=1` | - | Vision model analysis result |
| `media:type=embeddings-output;v=1` | - | Embedding generation result |
| `media:type=structured-query-output;v=1` | `MEDIA_STRUCTURED_QUERY_OUTPUT` | Structured query result |
| `media:type=questions-array;v=1` | `MEDIA_QUESTIONS_ARRAY` | Array of generated questions |

### Document Processing Outputs

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:type=extract-metadata-output;v=1` | - | Document metadata extraction result |
| `media:type=extract-outline-output;v=1` | - | Document outline extraction result |
| `media:type=grind-output;v=1` | - | Document grinding/chunking result |

---

## Image Types

Image types use subtype to specify format.

| Media URN | Description |
|-----------|-------------|
| `media:type=image;subtype=png` | PNG image |
| `media:type=image;subtype=jpeg` | JPEG image |
| `media:type=image;subtype=gif` | GIF image |
| `media:type=image;subtype=webp` | WebP image |

---

## Application Types

Application-specific formats using subtype.

| Media URN | Description |
|-----------|-------------|
| `media:type=application;subtype=pdf` | PDF document |
| `media:type=application;subtype=json` | JSON data |
| `media:type=application;subtype=xml` | XML data |
| `media:type=application;subtype=epub+zip` | EPUB e-book |

---

## Text Types

Text formats with subtype for specific languages/formats.

| Media URN | Description |
|-----------|-------------|
| `media:type=text;subtype=plain` | Plain text |
| `media:type=text;subtype=html` | HTML markup |
| `media:type=text;subtype=markdown` | Markdown |
| `media:type=text;subtype=x-rust` | Rust source code |
| `media:type=text;subtype=x-rst` | reStructuredText |

---

## Special Types

| Media URN | Description |
|-----------|-------------|
| `media:type=unknown;v=1` | Unknown/unrecognized type |
| `media:type=result;v=1` | Generic result wrapper |

---

## Usage Guidelines

### When to Use Each Type

1. **For chip content storage:**
   - Text content: `MEDIA_STRING`
   - JSON/structured data: `MEDIA_OBJECT`
   - Binary/embeddings: `MEDIA_BINARY`
   - Images: `media:type=image;subtype=<format>`

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
let urn = MediaUrn::from_string("media:type=string;v=1")?;

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

## Coercion Tags (v2 Format)

The new media URN format includes coercion tags that enable type coercion and matching. Tags declare what representations a type can be reduced to.

### Standard Coercion Tags

| Tag | Meaning |
|-----|---------|
| `text` | Reducible to UTF-8 string |
| `json` | Serializable as JSON |
| `binary` | Raw bytes representation |
| `numeric` | Numeric value |
| `scalar` | Single value (not a collection) |
| `collection` | Contains multiple items |
| `structured` | Has internal structure |
| `visual` | Has visual representation |

### Matching with Coercion Tags

A cap requiring text input can specify `media:text` and match ANY type reducible to text:

```
cap:in=media:text;op=prompt;out=media:type=object;v=1
```

This matches: string, integer, number, boolean, object, arrays - anything with `text`.

---

## Conversion Table: Old → New Format

### Core Primitive Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=void;v=1` | `media:type=void;v=1` |
| `media:type=string;v=1` | `media:type=string;v=1;text;json;scalar` |
| `media:type=integer;v=1` | `media:type=integer;v=1;text;json;numeric;scalar` |
| `media:type=number;v=1` | `media:type=number;v=1;text;json;numeric;scalar` |
| `media:type=boolean;v=1` | `media:type=boolean;v=1;text;json;scalar` |
| `media:type=object;v=1` | `media:type=object;v=1;text;json;structured` |
| `media:type=binary;v=1` | `media:type=binary;v=1;binary` |

### Array Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=string-array;v=1` | `media:type=string-array;v=1;text;json;collection` |
| `media:type=integer-array;v=1` | `media:type=integer-array;v=1;text;json;numeric;collection` |
| `media:type=number-array;v=1` | `media:type=number-array;v=1;text;json;numeric;collection` |
| `media:type=boolean-array;v=1` | `media:type=boolean-array;v=1;text;json;collection` |
| `media:type=object-array;v=1` | `media:type=object-array;v=1;text;json;structured;collection` |

### Image Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=image;subtype=png` | `media:type=image;subtype=png;binary;visual` |
| `media:type=image;subtype=jpeg` | `media:type=image;subtype=jpeg;binary;visual` |
| `media:type=image;subtype=gif` | `media:type=image;subtype=gif;binary;visual` |
| `media:type=image;subtype=webp` | `media:type=image;subtype=webp;binary;visual` |

### Application Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=application;subtype=pdf` | `media:type=application;subtype=pdf;binary` |
| `media:type=application;subtype=json` | `media:type=application;subtype=json;text;json;structured` |
| `media:type=application;subtype=xml` | `media:type=application;subtype=xml;text;structured` |
| `media:type=application;subtype=epub+zip` | `media:type=application;subtype=epub+zip;binary` |

### Text Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=text;subtype=plain` | `media:type=text;subtype=plain;text;scalar` |
| `media:type=text;subtype=html` | `media:type=text;subtype=html;text;structured` |
| `media:type=text;subtype=markdown` | `media:type=text;subtype=markdown;text` |
| `media:type=text;subtype=x-rust` | `media:type=text;subtype=x-rust;text` |
| `media:type=text;subtype=x-rst` | `media:type=text;subtype=x-rst;text` |

### Domain-Specific Types

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=listing-id;v=1` | `media:type=listing-id;v=1;text;scalar` |
| `media:type=task-id;v=1` | `media:type=task-id;v=1;text;scalar` |
| `media:type=file-path-array;v=1` | `media:type=file-path-array;v=1;text;json;collection` |

### Output Types (Structured JSON)

| Old Format | New Format with Coercion Tags |
|------------|--------------------------------|
| `media:type=llm-inference-output;v=1` | `media:type=llm-inference-output;v=1;text;json;structured` |
| `media:type=vision-inference-output;v=1` | `media:type=vision-inference-output;v=1;text;json;structured` |
| `media:type=embeddings-output;v=1` | `media:type=embeddings-output;v=1;text;json;structured` |
| `media:type=extract-metadata-output;v=1` | `media:type=extract-metadata-output;v=1;text;json;structured` |
| `media:type=extract-outline-output;v=1` | `media:type=extract-outline-output;v=1;text;json;structured` |
| `media:type=grind-output;v=1` | `media:type=grind-output;v=1;text;json;structured;collection` |
| `media:type=download-output;v=1` | `media:type=download-output;v=1;text;json;structured` |
| `media:type=load-output;v=1` | `media:type=load-output;v=1;text;json;structured` |
| `media:type=unload-output;v=1` | `media:type=unload-output;v=1;text;json;structured` |
| `media:type=list-output;v=1` | `media:type=list-output;v=1;text;json;structured;collection` |
| `media:type=status-output;v=1` | `media:type=status-output;v=1;text;json;structured` |
| `media:type=contents-output;v=1` | `media:type=contents-output;v=1;text;json;structured;collection` |
| `media:type=generate-output;v=1` | `media:type=generate-output;v=1;text;json;structured` |
| `media:type=structured-query-output;v=1` | `media:type=structured-query-output;v=1;text;json;structured` |
| `media:type=questions-array;v=1` | `media:type=questions-array;v=1;text;json;collection` |

---

## Type Coercion Rules

When a cap requires `media:text`, the system can automatically coerce:

| Source Type | Coercion Method |
|-------------|-----------------|
| `integer`, `number` | `ToString()` |
| `boolean` | `"true"` / `"false"` |
| `object`, `*-array` | JSON stringify |
| `listing-id`, `task-id` | UUID string |
| `text/*` | Direct (already text) |
| `string` | Direct (already text) |

Types with only `binary` (images, PDF, etc.) cannot be coerced to text without an explicit conversion cap.
