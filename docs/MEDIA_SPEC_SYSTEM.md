# Media Spec System

This document describes the media specification system used in capns for defining input and output types in capability definitions.

## Overview

The media spec system provides a type-safe way to define and validate the inputs and outputs of capabilities. It uses **spec IDs** that resolve to media type definitions, with optional JSON Schema for offline validation.

## Spec IDs

Spec IDs are short, versioned identifiers that reference media type definitions. They follow the format `<namespace>:<name>.v<version>`.

### Built-in Spec IDs

These spec IDs are implicitly available and do not need to be declared in `media_specs`:

| Spec ID | Media Type | Profile URI | Description |
|---------|------------|-------------|-------------|
| `media:string` | `text/plain` | `https://capns.org/schema/str` | String value |
| `media:integer` | `text/plain` | `https://capns.org/schema/int` | Integer value |
| `media:number` | `text/plain` | `https://capns.org/schema/num` | Number value |
| `media:boolean` | `text/plain` | `https://capns.org/schema/bool` | Boolean value |
| `media:object` | `application/json` | `https://capns.org/schema/obj` | JSON object |
| `media:string-array` | `application/json` | `https://capns.org/schema/str-array` | String array |
| `media:integer-array` | `application/json` | `https://capns.org/schema/int-array` | Integer array |
| `media:number-array` | `application/json` | `https://capns.org/schema/num-array` | Number array |
| `media:boolean-array` | `application/json` | `https://capns.org/schema/bool-array` | Boolean array |
| `media:object-array` | `application/json` | `https://capns.org/schema/obj-array` | Object array |
| `media:binary` | `application/octet-stream` | - | Binary data |

### Domain-Specific Spec IDs

Additional well-known spec IDs for specific domains:

#### FileGrind Types (`fgnd:`)

| Spec ID | Media Type | Profile URI | Description |
|---------|------------|-------------|-------------|
| `fgnd:listing-id.v1` | `text/plain` | `https://filegrind.com/schema/listing-id` | Listing UUID |
| `fgnd:task-id.v1` | `text/plain` | `https://filegrind.com/schema/task-id` | Task UUID |
| `fgnd:file-path-array.v1` | `application/json` | `https://filegrind.com/schema/file-path-array` | Array of file paths |

#### CapNS Output Types (`capns:`)

| Spec ID | Media Type | Description |
|---------|------------|-------------|
| `media:extract-metadata-output` | `application/json` | Document metadata extraction output |
| `media:extract-outline-output` | `application/json` | Document outline extraction output |
| `media:disbound-pages` | `application/json` | File chips extraction output |
| `media:llm-inference-output` | `application/json` | LLM inference response |
| `media:embeddings` | `application/json` | Embeddings generation output |
| `media:structured-query-output` | `application/json` | Structured query output |
| `media:questions-array` | `application/json` | Questions array for bit choices |
| `media:download-output` | `application/json` | Model download output |
| `media:load-output` | `application/json` | Model load output |
| `media:unload-output` | `application/json` | Model unload output |
| `media:list-output` | `application/json` | Model list output |
| `media:status-output` | `application/json` | Model status output |
| `media:contents-output` | `application/json` | Model contents output |

## Media Specs Table

The `media_specs` field in a cap definition maps spec IDs to their definitions. Definitions can be in two forms:

### String Form (Compact)

```toml
[media_specs]
"my:input-spec.v1" = "text/plain; profile=https://example.com/schema/input"
```

### Object Form (Rich)

```toml
[media_specs."my:output-spec.v1"]
media_type = "application/json"
profile_uri = "https://example.com/schema/output"

[media_specs."my:output-spec.v1".schema]
type = "object"
properties.result = { type = "string" }
required = ["result"]
```

The object form allows embedding a JSON Schema for offline validation.

## URN Tags

Cap URNs use `in` and `out` tags to specify input and output types via spec IDs:

```
cap:ext=pdf;in=media:binary;op=extract_metadata;out=media:extract-metadata-output
```

### Tag Reference

| Tag | Description | Example |
|-----|-------------|---------|
| `op` | Operation name | `extract_metadata` |
| `in` | Input spec ID | `media:binary` |
| `out` | Output spec ID | `media:extract-metadata-output` |
| `ext` | File extension (for document ops) | `pdf` |
| `language` | Language code (for LLM ops) | `en` |
| `type` | Constraint type | `constrained`, `task_creation`, `model` |

## Spec ID Resolution

When resolving a spec ID:

1. Look up spec ID in the cap's `media_specs` table
2. If not found, check if it's a built-in primitive
3. If not found and not a built-in: **fail hard** with an error
4. If found as string: parse as media spec string (`<media-type>; profile=<url>`)
5. If found as object: compose media spec from `media_type` + `profile_uri`, use `schema` if present

```rust
// Resolution never falls back or guesses
let resolved = resolve_spec_id(spec_id, &cap.media_specs)?;
```

## Cap Definition Example

```toml
title = "Extract Document Metadata"
cap_description = "Extract metadata from PDF documents"
command = "extract-metadata"
stdin = "media:pdf;binary"  # Specifies the media type stdin expects

[media_specs."media:extract-metadata-output"]
media_type = "application/json"
profile_uri = "https://capns.org/schema/extract-metadata-output"

[media_specs."media:extract-metadata-output".schema]
type = "object"
additionalProperties = false
properties.title = { type = "string" }
properties.author = { type = "string" }
properties.page_count = { type = "integer", minimum = 0 }
required = ["title", "page_count"]

[urn.tags]
op = "extract_metadata"
ext = "pdf"
in = "media:binary"
out = "media:extract-metadata-output"

[[arguments.required]]
name = "file_path"
media_spec = "media:string"
arg_description = "Path to the document file"
cli_flag = "file_path"
position = 0

[output]
media_spec = "media:extract-metadata-output"
output_description = "Structured metadata extracted from the document"
```

## Binary Type Detection

The `isBinary()` method on resolved media specs checks if the output is binary:

- Media types starting with `image/`, `audio/`, `video/`
- `application/octet-stream`
- `application/pdf`
- Media types starting with `application/x-`
- Media types containing `+zip` or `+gzip`

## Validation

Input and output validation follows these steps:

1. Resolve the spec ID to get the full media spec definition
2. If the resolved spec has a `schema`, validate against it
3. Apply any validation rules from the argument/output definition
4. For binary types, ensure the value is a base64-encoded string

Validation always fails hard on:
- Unresolvable spec IDs
- Schema validation failures
- Type mismatches

## Implementation Files

### Rust (capns)
- `src/media_spec.rs` - Spec ID constants, resolution, and built-ins
- `src/validation.rs` - Input/output validation
- `src/standard/caps.rs` - URN builder functions

### Go (capns-go)
- `media_spec.go` - Spec ID constants and resolution
- `validation.go` - Input/output validation
- `standard.go` - URN builder functions

### Objective-C (capns-objc)
- `CSMediaSpec.h/m` - Spec ID constants and resolution
- `CSCapValidator.h/m` - Input/output validation

### JavaScript (capns-js)
- `capns.js` - Spec ID constants, resolution, and validation

## Key Principles

1. **Fail hard** - Unresolvable spec IDs cause immediate errors with clear messages
2. **No fallbacks** - No silent degradation or guessing
3. **Single source of truth** - The `media_specs` table is the authoritative spec definition
4. **Offline validation** - Use local schemas, no network fetching required
5. **Spec ID indirection** - Arguments and outputs reference specs by ID, not inline definitions
