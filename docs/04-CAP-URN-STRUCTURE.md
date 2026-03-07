# Cap URN Structure

## 1. Product Structure

A Cap URN is a **triple** over the Tagged URN domain:

```
C = U × U × U
```

For a Cap URN `c ∈ C`:

```
c = (i, o, y)
```

Where:
- `i ∈ U` — Input dimension (the `in` tag value, a Media URN)
- `o ∈ U` — Output dimension (the `out` tag value, a Media URN)
- `y ∈ U` — Non-direction cap-tags (op, ext, model, language, etc.)

---

## 2. String Representation

### 2.1 Canonical Form

A Cap URN serializes as:
```
cap:in="<media-urn>";out="<media-urn>";<cap-tags>
```

Examples:
```
cap:in=media:;out=media:
cap:in="media:pdf";op=extract;out="media:object"
cap:in="media:textable;form=scalar";op=prompt;out="media:textable;form=map"
```

### 2.2 Direction Tags

The `in` and `out` tags are **required** in the canonical form:

| Tag | Purpose | Default |
|-----|---------|---------|
| `in` | Input media type | `media:` (any) |
| `out` | Output media type | `media:` (any) |

### 2.3 Non-Direction Tags

All other tags form the `y` dimension:

| Common Tag | Purpose |
|------------|---------|
| `op` | Operation name |
| `ext` | File extension hint |
| `model` | Model identifier |
| `language` | Language code |
| `constrained` | Constrained output flag |

---

## 3. Parsing and Normalization

Cap URN processing distinguishes three forms:

| Form | Description |
|------|-------------|
| **Surface syntax** | What users may write (may omit `in`/`out`) |
| **Canonical form** | Normalized representation (always has `in`/`out`) |
| **Validation target** | Post-normalization structure that rules check |

### 3.1 Surface Syntax (Accepted Input)

Users may omit direction tags. These are all valid surface syntax:
```
cap:op=test
cap:in;op=test
cap:in=*;op=test;out=*
```

### 3.2 Normalization to Canonical Form

During parsing, missing or wildcard direction tags are filled with `media:`:

| Surface Syntax | Canonical Form |
|----------------|----------------|
| `cap:op=test` | `cap:in=media:;op=test;out=media:` |
| `cap:in;op=test` | `cap:in=media:;op=test;out=media:` |
| `cap:in=*;op=test;out=*` | `cap:in=media:;op=test;out=media:` |

The value `*` in direction tags expands to `media:`:
```
in=*  →  in=media:
out=* →  out=media:
```

This ensures `media:` is the unique identity for "any media type".

### 3.3 Validation Target

Validation rules (CU1, CU2 in [10-VALIDATION-RULES](./10-VALIDATION-RULES.md)) apply to the **canonical form**, not surface syntax. After normalization:
- `in` and `out` are always present
- Their values are valid Media URNs

### 3.4 Quoting

Direction spec values containing `;` must be quoted:
```
cap:in="media:pdf;bytes";op=extract;out="media:object"
```

Without quotes, `media:pdf;bytes` would parse incorrectly.

---

## 4. Identity Cap

### 4.1 Definition

The **identity cap** is:
```
cap:
```

Which normalizes to:
```
cap:in=media:;out=media:
```

### 4.2 Semantics

The identity cap:
- Accepts any input (`in=media:`)
- Produces any output (`out=media:`)
- Has no operation constraints
- Has specificity 0
- Is the **top** of the Cap partial order

### 4.3 Constant

```rust
pub const CAP_IDENTITY: &str = "cap:";
```

### 4.4 Requirement

Every capset **must** include the identity cap (see CU1 in [10-VALIDATION-RULES](./10-VALIDATION-RULES.md)).

---

## 5. Dimension Semantics

### 5.1 Input Dimension (i)

The `in` tag specifies what input the capability accepts:

```
cap:in="media:pdf";...
```

Meaning: "This capability requires PDF input."

Wildcard:
```
cap:in=media:;...
```

Meaning: "This capability accepts any input."

### 5.2 Output Dimension (o)

The `out` tag specifies what output the capability produces:

```
cap:..;out="media:object"
```

Meaning: "This capability produces a JSON object."

Wildcard:
```
cap:...;out=media:
```

Meaning: "This capability may produce any output."

### 5.3 Cap-Tags Dimension (y)

Non-direction tags specify operation identity and constraints:

```
cap:...;op=extract;target=metadata
```

The `y` dimension is itself a Tagged URN (without prefix), using the same matching semantics.

---

## 6. Accessing Components

### 6.1 Extracting Dimensions

Given a Cap URN string, extract:

```rust
let cap = CapUrn::from_string("cap:in=media:pdf;op=extract;out=media:object")?;

let input: &str = cap.in_spec();    // "media:pdf"
let output: &str = cap.out_spec();  // "media:object"
let op: Option<&str> = cap.tag("op"); // Some("extract")
```

### 6.2 Component Types

| Component | Type | Access |
|-----------|------|--------|
| Input | Media URN string | `in_spec()` |
| Output | Media URN string | `out_spec()` |
| Cap-tags | Key-value map | `tags`, `tag(key)` |

---

## 7. Specificity

Cap URN specificity is defined in [03-SPECIFICITY](./03-SPECIFICITY.md):

```
spec_C(i, o, y) = tags(i) + tags(o) + count(non-* y-tags)
```

Examples:
```
cap:                                    → 0
cap:op=extract                          → 1
cap:in=media:pdf;op=extract;out=media:object → 3
```

---

## 8. Partial Order Structure

Cap URNs form a partial order (specialization order) in the product space:

```
                        cap:                          (top)
                         |
              cap:op=extract
                /              \
cap:in=media:pdf;op=extract    cap:op=extract;out=media:object
                \              /
        cap:in=media:pdf;op=extract;out=media:object
                         |
cap:in=media:pdf;v=2.0;op=extract;out=media:object;target=metadata  (more specific)
```

The ordering follows from the dispatch relation (see [05-DISPATCH](./05-DISPATCH.md)).

---

## 9. Relationship to Media URNs

### 9.1 Direction Values Are Media URNs

The `in` and `out` tag values are themselves Media URNs:

```
cap:in="media:pdf;bytes";out="media:object"
        ↑                     ↑
    Media URN             Media URN
```

### 9.2 Matching Uses Media URN Semantics

When matching direction specs, use Media URN matching:

```rust
let provider_in = MediaUrn::from_string("media:bytes")?;
let request_in = MediaUrn::from_string("media:pdf;bytes")?;

// For dispatch: request_in must conform to provider_in
request_in.conforms_to(&provider_in)  // true
```

---

## 10. Validation Rules

Cap URNs must satisfy (from [10-VALIDATION-RULES](./10-VALIDATION-RULES.md)):

- **CU1**: Must have `in` and `out` tags (enforced via normalization)
- **CU2**: Direction values must be valid Media URNs or `*`

---

## 11. Common Patterns

### 11.1 Generic Capability

```
cap:op=transform
```

Accepts any input, produces any output, performs "transform".

### 11.2 Typed Transformer

```
cap:in="media:pdf";op=extract;out="media:object"
```

Takes PDF, produces object.

### 11.3 Constrained Generation

```
cap:in="media:textable;form=scalar";op=generate;out="media:textable;form=map";constrained
```

Takes text prompt, produces structured output with constraints.

### 11.4 Identity (Pass-through)

```
cap:
```

The identity morphism. Required in all capsets.

---

## 12. Summary

| Concept | Definition |
|---------|------------|
| Structure | C = U × U × U (product of Tagged URN domain) |
| Components | (in, out, y) |
| Identity | `cap:` → `cap:in=media:;out=media:` |
| Direction defaults | Missing or `*` → `media:` |
| Canonical form | Always includes `in` and `out` |

Cap URNs extend Tagged URNs with three-dimensional structure. The dispatch relation (next document) defines how these dimensions interact for routing.
