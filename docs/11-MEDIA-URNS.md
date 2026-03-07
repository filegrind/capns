# Media URNs

## 1. Structure

A Media URN has the form:

```
media:<type>[;tag=value]...
```

Examples:
```
media:                          # Identity (any media)
media:pdf                       # PDF type
media:pdf;bytes                 # PDF with bytes marker
media:textable;form=scalar      # String type
media:image;subtype=png;visual  # PNG image
```

---

## 2. The Identity

### 2.1 Definition

The **media identity** is:

```
media:
```

This URN:
- Has no tags
- Represents "any media type"
- Is the **top** of the media partial order
- Has specificity 0

### 2.2 Constant

```rust
pub const MEDIA_IDENTITY: &str = "media:";
```

### 2.3 Properties

```
∀m ∈ MediaUrn, m ⪯ media:     (all media URNs conform to identity)
media: ⪯ media:               (reflexive)
```

---

## 3. Coercion Tags

Media URNs use **coercion tags** to declare type capabilities. These enable polymorphic matching.

### 3.1 Standard Coercion Tags

| Tag | Meaning | Examples |
|-----|---------|----------|
| `textable` | Can be represented as UTF-8 text | strings, numbers, booleans, JSON |
| `binary` | Raw bytes representation | images, PDFs, audio |
| `numeric` | Supports numeric operations | integers, floats |
| `scalar` | Single atomic value | primitives (not arrays/objects) |
| `sequence` | Ordered collection | arrays |
| `map` | Key-value structure | objects |
| `visual` | Has visual rendering | images, PDFs |

### 3.2 How Coercion Works

A capability requiring `media:textable` matches ANY type with the `textable` tag:

```
cap:in="media:textable";op=prompt;out="media:textable;form=map"
```

This matches:
- `media:textable;form=scalar` (string)
- `media:integer` (if it has textable)
- `media:bool;textable;form=scalar` (boolean)
- `media:textable;form=map` (object via JSON.stringify)

### 3.3 Coercion Rules

| Source Type | Can Coerce To | Method |
|-------------|---------------|--------|
| integer, number | textable | `.toString()` |
| boolean | textable | `"true"` / `"false"` |
| object, array | textable | JSON stringify |
| string | textable | Direct (already text) |
| image, PDF, audio | textable | **NO** (requires explicit conversion cap) |

---

## 4. Form Tags

The `form` tag specifies structural shape:

| Value | Meaning |
|-------|---------|
| `form=scalar` | Single value |
| `form=list` | Array/sequence |
| `form=map` | Object/dictionary |

### 4.1 Examples

```
media:textable;form=scalar         # String
media:textable;form=list           # Array of strings
media:textable;form=map            # JSON object
media:integer;textable;form=list   # Array of integers
```

---

## 5. Common Media Types

### 5.1 Primitives

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:` | `MEDIA_IDENTITY` | Any/raw binary |
| `media:void` | `MEDIA_VOID` | No data |
| `media:textable;form=scalar` | `MEDIA_STRING` | UTF-8 string |
| `media:integer` | `MEDIA_INTEGER` | Integer |
| `media:textable;numeric;form=scalar` | `MEDIA_NUMBER` | Float |
| `media:bool;textable;form=scalar` | `MEDIA_BOOLEAN` | Boolean |
| `media:textable;form=map` | `MEDIA_OBJECT` | JSON object |

### 5.2 Arrays

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:textable;form=list` | `MEDIA_STRING_ARRAY` | String array |
| `media:integer;textable;form=list` | `MEDIA_INTEGER_ARRAY` | Integer array |
| `media:textable;numeric;form=list` | `MEDIA_NUMBER_ARRAY` | Number array |
| `media:bool;textable;form=list` | `MEDIA_BOOLEAN_ARRAY` | Boolean array |

### 5.3 Visual Types

| Media URN | Description |
|-----------|-------------|
| `media:image;subtype=png;visual` | PNG image |
| `media:image;subtype=jpeg;visual` | JPEG image |
| `media:application;subtype=pdf;visual` | PDF document |

---

## 6. Matching Semantics

Media URN matching follows Tagged URN semantics from [01-TAGGED-URN-DOMAIN](./01-TAGGED-URN-DOMAIN.md).

### 6.1 Pattern Matching

```
Pattern:  media:bytes
Instance: media:pdf;bytes

Does instance have all tags pattern requires?
- Pattern requires: bytes=*
- Instance has: pdf=*, bytes=*
- bytes present? Yes → MATCH ✓
```

### 6.2 Specificity

More tags = more specific:

```
spec(media:) = 0
spec(media:bytes) = 2           # bytes=* is must-have-any
spec(media:pdf;bytes) = 4       # two must-have-any tags
spec(media:pdf;v=2.0) = 5       # must-have-any + exact value
```

### 6.3 Conformance

```
media:pdf;bytes ⪯ media:bytes   (pdf;bytes conforms to bytes)
media:bytes ⪯ media:            (bytes conforms to identity)
media:pdf ⪯ media:image         ✗ (not on same chain)
```

---

## 7. Direction Specs in Cap URNs

When used as `in` or `out` values in Cap URNs:

### 7.1 Quoting

Media URNs containing `;` must be quoted:

```
cap:in="media:pdf;bytes";op=extract;out="media:object"
```

### 7.2 Identity Expansion

`in=*` and `out=*` expand to `media:`:

```
cap:in=*;op=convert;out=*
→ cap:in=media:;op=convert;out=media:
```

### 7.3 Dispatch

For dispatch (see [05-DISPATCH](./05-DISPATCH.md)):

- **Input**: Request input must conform to provider input (contravariant)
- **Output**: Provider output must conform to request output (covariant)

---

## 8. Type Detection

### 8.1 Helper Methods

```rust
let urn = MediaUrn::from_string("media:textable;form=scalar")?;

urn.is_text()    // true for string, text/*
urn.is_json()    // true for object, object-array
urn.is_binary()  // true for raw binary, images
```

### 8.2 Tag Queries

```rust
urn.has_tag("textable")     // true
urn.tag_value("form")       // Some("scalar")
```

---

## 9. Adding New Types

When defining a new media type:

1. **Determine coercion tags**: What can this type be coerced to?
2. **Determine form**: scalar, list, or map?
3. **Add constant** if frequently used
4. **Document** in media catalog

### 9.1 Example: Custom Type

```rust
// A new type for structured logs
const MEDIA_LOG_ENTRY: &str = "media:log-entry;textable;form=map";

// Coercible to text (via JSON), structured as map
```

---

## 10. Partial Order Position

Media URNs form a partial order (specialization order):

```
                    media:                    (top - any)
                      |
              media:textable
                /         \
    media:textable;form=scalar   media:textable;form=map
            |                            |
    media:integer;textable       media:json;textable;form=map
```

More tags = lower in the order = more specific.

---

## 11. Summary

| Concept | Definition |
|---------|------------|
| Structure | `media:<type>[;tag=value]...` |
| Identity | `media:` (any media, top of partial order) |
| Coercion tags | textable, binary, numeric, scalar, sequence, map, visual |
| Form tag | scalar, list, map |
| Matching | Tagged URN semantics (truth table) |
| Specificity | Sum of per-tag scores |

Media URNs describe data types. They are used:
- In Cap URN `in`/`out` specs
- As argument identifiers
- For type matching in dispatch
