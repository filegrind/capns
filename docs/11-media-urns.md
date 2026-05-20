---
title: "Media Urns"
layout: doc
permalink: /11-media-urns/
---
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
media:textable                  # String type (scalar by default)
media:image;png                 # PNG image
```

---

## 2. Top and Unit Types

The media URN order has two distinguished anchors. They are **not**
interchangeable: confusing them flips the meaning of every cap that
uses them.

### 2.1 `media:` — the Top Type

```
media:
```

The bare prefix with no tags. Reads as "any data type."

- Has no tags.
- Every other media URN `conforms_to` it.
- Specificity 0.
- The **top** of the media partial order.

```rust
pub const MEDIA_IDENTITY: &str = "media:";
```

```
∀m ∈ MediaUrn, m ⪯ media:     (every media URN conforms to top)
media: ⪯ media:               (reflexive)
```

A cap with `in=media:` says "I accept any input." A cap with
`out=media:` says "I may produce any output." Used on both sides,
these are just the fully generic top types for the directional axes.
They do **not** by themselves mean identity.

Under the four-axis cap model:

- `cap:effect=none` is the explicit [identity morphism](/docs/06-cap-urn-structure#4-cap-kinds)
- `cap:` and `cap:in=media:;out=media:` are illegal bare top forms
- `cap:<y-tags>` with both directional sides at `media:` is a generic
  top-to-top transform, not identity

### 2.2 `media:void` — the Unit Type

```
media:void
```

Reads as "no data" — the nullary value, the type with exactly one
inhabitant.

- Has the `void` marker tag.
- Distinct from `media:`. Top means "any A flows here"; unit means
  "() flows here, no meaningful payload."

```rust
pub const MEDIA_VOID: &str = "media:void";
```

A cap with `in=media:void` does not consume data — it is driven
entirely by its non-directional tags and any peer state. A cap with
`out=media:void` produces no data; it exists for the side effect.
Caps with `media:void` on both sides are pure side-effect commands
(see [CapKind](/docs/06-cap-urn-structure#4-cap-kinds): Source,
Sink, Effect).

#### Atomicity

`media:void` is **atomic**. The parser rejects any media URN that
combines the `void` marker tag with any other tag:

```
media:void                ✓
media:void;text           ✗  (parse error)
media:void;pdf            ✗  (parse error)
media:void;reason=warmup  ✗  (parse error)
media:void;heartbeat      ✗  (parse error)
```

There is no lattice underneath the unit. Permitting refinements
would manufacture a fake taxonomy of unit values
(`media:void;warmup` vs `media:void;heartbeat` etc.) and dispatch
semantics would silently fork: are these different units? different
effects? different commands? Refusing the syntax forecloses the
question.

When a cap needs to express *why* or *how* it uses void, that
information goes on the **cap URN's non-directional axis** (or in
cap args), never as a refinement of the media URN:

```
✓ cap:in=media:void;out=media:void;warmup
✓ cap:in=media:void;out=media:void;heartbeat
✓ cap:in=media:void;out=media:image;generate;target=thumbnail

✗ cap:in=media:void;reason=warmup;out=media:void
✗ cap:in=media:void;text;out=media:textable
```

Each of the first three describes a distinct morphism (different
operation tags). The last two try to pack the same distinction into
the unit type itself; the parser rejects them at the media-URN
layer before the cap URN ever forms.

This rule is enforced at the deepest layer — every `MediaUrn`
constructor and `from_string` parse path returns a parse error on
violation:

| Port      | Error                                |
|-----------|--------------------------------------|
| Rust      | `MediaUrnError::VoidNotAtomic`       |
| Go        | `MediaUrnError{Code: ErrorMediaVoidNotAtomic}` |
| Python    | `MediaUrnError("media:void is atomic …")` |
| Swift/ObjC| `CSMediaUrnErrorVoidNotAtomic`       |
| JS        | `MediaUrnError(VOID_NOT_ATOMIC, …)`  |

Cross-language parity is pinned by `test1810`.

### 2.3 Top vs Unit at a Glance

| Side type     | Reads as           | Used for                       |
|---------------|--------------------|--------------------------------|
| `media:`      | "any A"            | wildcards, generic passthrough |
| `media:void`  | "()"               | sources, sinks, effects        |
| concrete      | a specific type    | normal data flow               |

`media:` and `media:void` look superficially similar (both are
"unspecific" in some sense) but they sit at opposite ends of the
type lattice. `media:` is the **maximum** of the order; `media:void`
is a leaf carrying the nullary value.

---

## 3. Semantic and Coercion Markers

Media URNs use marker tags to declare type capabilities and content
properties. Some of these participate in coercion-style matching
(`textable`, `numeric`); others describe modality (`visual`, `audio`).

### 3.1 Standard Markers

| Tag | Meaning | Examples |
|-----|---------|----------|
| `textable` | Can be represented as UTF-8 text | strings, numbers, booleans, JSON |
| `numeric` | Supports numeric operations | integers, floats |
| `visual` | Has visual rendering | images, PDFs |
| `audio` | Represents audio content | wav, mp3, flac |

There is **no `binary` marker dim** in the media model. The definition
is positive: `textable` means the value is faithfully representable as
UTF-8 text without loss. A media URN that lacks `textable` simply makes
no such promise.

### 3.2 How Coercion Works

A capability requiring `media:textable` matches ANY type with the `textable` tag:

```
cap:in="media:textable";prompt;out="media:json;record;textable"
```

This matches:
- `media:textable` (string)
- `media:integer` (if it has textable)
- `media:bool;textable` (boolean)
- `media:json;record;textable` (object via JSON.stringify)

### 3.3 Coercion Rules

| Source Type | Can Coerce To | Method |
|-------------|---------------|--------|
| integer, number | textable | `.toString()` |
| boolean | textable | `"true"` / `"false"` |
| object, array | textable | JSON stringify |
| string | textable | Direct (already text) |
| image, PDF, audio | textable | **NO** (requires explicit conversion cap) |

---

## 4. Structural Markers and Defaults

Media URNs no longer use a `form=` axis. Structure is represented by
marker tags and by default absence:

| Shape property | Encoding | Meaning |
|----------------|----------|---------|
| scalar | no `list` tag | Single value (default cardinality) |
| list | `list` marker | Ordered collection |
| opaque | no `record` tag | No internal key-value structure (default) |
| record | `record` marker | Key-value structure |

So:

- `media:textable` means scalar + opaque
- `media:json;record;textable` means scalar + record
- `media:list;textable` means list + opaque
- `media:json;list;record;textable` means list + record

### 4.1 Examples

```
media:textable                     # String
media:list;textable                # Array of strings
media:json;record;textable         # JSON object
media:integer;list;textable;numeric # Array of integers
```

---

## 5. Common Media Types

### 5.1 Primitives

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:` | `MEDIA_IDENTITY` | **Top** — any data type (universal wildcard) |
| `media:void` | `MEDIA_VOID` | **Unit** — the nullary value (no data flows here) |
| `media:textable` | `MEDIA_STRING` | UTF-8 string |
| `media:integer` | `MEDIA_INTEGER` | Integer |
| `media:textable;numeric` | `MEDIA_NUMBER` | Float |
| `media:bool;textable` | `MEDIA_BOOLEAN` | Boolean |
| `media:record` | `MEDIA_OBJECT` | Generic record/object |
| `media:json;record;textable` | `MEDIA_JSON` | JSON object |

### 5.2 Arrays

| Media URN | Constant | Description |
|-----------|----------|-------------|
| `media:list;textable` | `MEDIA_STRING_LIST` | String array |
| `media:integer;list;textable;numeric` | `MEDIA_INTEGER_LIST` | Integer array |
| `media:list;numeric;textable` | `MEDIA_NUMBER_LIST` | Number array |
| `media:bool;list;textable` | `MEDIA_BOOLEAN_LIST` | Boolean array |
| `media:list;record` | `MEDIA_OBJECT_LIST` | List of opaque records |

### 5.3 Visual Types

| Media URN | Description |
|-----------|-------------|
| `media:image;png` | PNG image |
| `media:jpeg;image` | JPEG image |
| `media:pdf` | PDF document |

---

## 6. Matching Semantics

Media URN matching follows Tagged URN semantics from [01-TAGGED-URN-DOMAIN](/docs/03-tagged-urn-domain).

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
cap:in="media:pdf;bytes";extract;out="media:record"
```

### 7.2 Identity Expansion

`in=*` and `out=*` expand to `media:`:

```
cap:in=*;convert;out=*
→ cap:in=media:;convert;out=media:
```

### 7.3 Dispatch

For dispatch (see [05-DISPATCH](/docs/07-dispatch)):

- **Input**: Request input must conform to provider input (contravariant)
- **Output**: Provider output must conform to request output (covariant)

---

## 8. Type Detection

### 8.1 Helper Methods

```rust
let urn = MediaUrn::from_string("media:textable")?;

urn.is_text()    // true when the `textable` marker is present
urn.is_json()    // true for JSON-flavoured URNs
urn.is_binary()  // implementation convenience: true when `textable` is absent
urn.is_void()    // true iff the `void` marker tag is present (unit type)
urn.is_top()     // true iff the URN has no tags at all (top type)
```

`is_void` and `is_top` are the predicates the [CapKind](/docs/06-cap-urn-structure#4-cap-kinds)
classifier consults. Together they let any caller reason about
whether a media URN is a concrete type, the wildcard, or the unit
without parsing strings.

`is_binary()` is a helper some implementations expose, but it is not a
first-class structural axis in the media def system. The normative
question is whether `textable` is present, not whether the URN belongs
to a separate "binary" category.

### 8.2 Tag Queries

```rust
urn.has_tag("textable")     // true
urn.has_tag("list")         // false
urn.has_tag("record")       // false
```

---

## 9. Adding New Types

When defining a new media type:

1. **Determine coercion tags**: What can this type be coerced to?
2. **Determine structure/cardinality**: default scalar vs `list`, default opaque vs `record`
3. **Add constant** if frequently used
4. **Document** in media catalog

### 9.1 Example: Custom Type

```rust
// A new type for structured logs
const MEDIA_LOG_ENTRY: &str = "media:log-entry;record;textable";

// Coercible to text, structured as record
```

---

## 10. Partial Order Position

Media URNs form a partial order (specialization order). `media:` is
the unique top; `media:void` is a leaf, distinct from every concrete
type:

```
                    media:                          (top — any A)
                  /        \
          media:textable    media:void              (leaf — unit ())
            /         \
     media:textable    media:json;record;textable
        |                            |
media:integer;textable;numeric  media:json;list;record;textable
```

More tags = lower in the order = more specific. `media:void` carries
exactly one tag (`void`) and so sits at specificity 2 (one
must-have-any tag, plus the prefix); it is not a refinement of any
concrete type.

---

## 11. Summary

| Concept | Definition |
|---------|------------|
| Structure | `media:<type>[;tag=value]...` |
| Top | `media:` — universal wildcard, max of the partial order, "any A" |
| Unit | `media:void` — nullary value, leaf of the partial order, "()" |
| Coercion / semantic markers | textable, numeric, visual, audio |
| Structural markers | `list` and `record`; scalar and opaque are defaults by absence |
| Matching | Tagged URN semantics (truth table) |
| Specificity | Sum of per-tag scores |

Media URNs describe data types. They are used:
- In Cap URN `in`/`out` specs (where the choice between `media:`,
  `media:void`, and a concrete type determines the cap's
  [kind](/docs/06-cap-urn-structure#4-cap-kinds))
- As argument identifiers
- For type matching in dispatch
