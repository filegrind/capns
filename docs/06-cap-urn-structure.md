---
title: "Cap URN Structure"
layout: doc
permalink: /06-cap-urn-structure/
---
# Cap URN Structure

## 1. Product Structure

A Cap URN is a **quadruple** over the Tagged URN domain:

```
C = U × U × U × E
```

For a Cap URN `c ∈ C`:

```
c = (i, o, y, e)
```

Where:
- `i ∈ U` — Input dimension (the `in` tag value, a Media URN)
- `o ∈ U` — Output dimension (the `out` tag value, a Media URN)
- `y ∈ U` — Non-direction cap-tags (op, ext, model, language, etc.)
- `e ∈ E` — Effect on runtime media/type identity

---

## 2. String Representation

### 2.1 Canonical Form

A Cap URN serializes as:
```
cap:in="<media-urn>";out="<media-urn>";effect=<effect>;<cap-tags>
```

Examples:
```
cap:effect=none
cap:extract;in="media:pdf";out="media:object"
cap:in="media:textable;form=scalar";out="media:textable;form=map";prompt
```

### 2.2 Direction Tags

The `in`, `out`, and `effect` coordinates are structural:

| Tag | Purpose | Default |
|-----|---------|---------|
| `in` | Input media type | `media:` (any) |
| `out` | Output media type | `media:` (any) |
| `effect` | Runtime media/type inference rule | `declared` |

### 2.3 Non-Direction Tags

All other tags form the `y` dimension. **No tag in `y` has functional
meaning to the protocol** — only `in`, `out`, and `effect` participate in
dispatch, conformance, and runtime output inference. Cap-tags are arbitrary descriptive
metadata: they refine the cap's identity (so two caps with different
`y` are different caps), but no tag key is privileged. Common
descriptive tags include operation names (`extract`, `generate`),
language codes, model identifiers, hints — all are equal under the
protocol.

---

## 3. Parsing and Normalization

Cap URN processing distinguishes three forms:

| Form | Description |
|------|-------------|
| **Surface syntax** | What users may write (may omit `in`/`out`/`effect`) |
| **Canonical form** | Normalized representation (structural defaults applied, default effect omitted) |
| **Validation target** | Post-normalization structure that rules check |

### 3.1 Surface Syntax (Accepted Input)

Users may omit direction tags or write the trivial wildcard
explicitly. These are all valid surface syntax:
```
cap:test
cap:in=media:;out=media:;test
cap:in=*;test;out=*
cap:effect=none
```

### 3.2 Normalization to Canonical Form

Parsing produces a unique canonical representative per cap. Two
rules govern the directional axes:

1. Missing or wildcard direction tags resolve to `media:` internally.
2. When `in` resolves to the top media URN (`media:`), the segment
   is **omitted** in canonical form. Same for `out`. The internal
   value is still `media:`; the rendered form just doesn't show it.
3. Missing `effect` resolves to `declared` internally and is omitted
   in canonical form. Non-default effects are preserved.

| Surface Syntax | Canonical Form |
|----------------|----------------|
| `cap:test` | `cap:test` |
| `cap:in=media:;test;out=media:` | `cap:test` |
| `cap:in=*;test;out=*` | `cap:test` |
| `cap:in=media:pdf;extract;out=media:textable` | `cap:extract;in=media:pdf;out=media:textable` |
| `cap:in=media:;out=media:;effect=none` | `cap:effect=none` |

The value `*` in direction tags expands to `media:`:
```
in=*  →  in=media:
out=* →  out=media:
```

This ensures `media:` is the unique top of the directional order, and
the canonical form is byte-equal across every way of writing it.

### 3.3 Validation Target

Validation rules (CU1, CU2 in [10-VALIDATION-RULES](/docs/10-validation-rules)) apply to the **canonical form**, not surface syntax. After normalization:
- `in` and `out` are always present
- Their values are valid Media URNs

### 3.4 Quoting

Direction spec values containing `;` must be quoted:
```
cap:in="media:pdf;bytes";extract;out="media:object"
```

Without quotes, `media:pdf;bytes` would parse incorrectly.

---

## 4. Cap Kinds

The `(i, o, y, e)` structure admits a five-way classification by inspecting
the directional axes. The classification is **logical only** — the
dispatch protocol does not branch on kind. Tools, UIs, planners, and
human readers use it to talk about what a cap *does* without
re-deriving the rules each time.

Two anchor types make the taxonomy fall out:

- **`media:`** is the **top type** — the universal wildcard over the
  media URN order. Every other media URN `conforms_to` this one. A
  side typed as `media:` reads as "any A": there is no constraint on
  what data flows there.
- **`media:void`** is the **unit type** — the nullary value. A side
  typed as `media:void` reads as "()": no meaningful data flows
  there. It is *not* "invalid" or "absent"; it is the type with
  exactly one value.

### 4.1 The Five Kinds

| Kind        | `i`           | `o`           | `e`            | `y`   | Reads as  |
|-------------|---------------|---------------|----------------|-------|-----------|
| `Identity`  | `media:`      | `media:`      | `none`         | empty | `A → A`   |
| `Source`    | `media:void`  | not `void`    | any            | any   | `() → B`  |
| `Sink`      | not `void`    | `media:void`  | any            | any   | `A → ()`  |
| `Effect`    | `media:void`  | `media:void`  | any            | any   | `() → ()` |
| `Transform` | anything else | anything else | anything legal | any   | `A → B`   |

Each implementation exposes this classification via a `kind()` method
on `CapUrn` (or its language-port equivalent), returning a `CapKind`
enum value.

### 4.2 Identity Is Explicit

With the `effect` coordinate, identity is no longer the same thing as
the fully generic top-to-top cap.

- `cap:effect=none` means true categorical identity

So:

| URN                | Kind               | Reading                                |
|--------------------|--------------------|----------------------------------------|
| `cap:effect=none`  | Identity           | `A → A` for any `A`                    |
| `cap:passthrough`  | Transform          | generic top-to-top transform with y-axis refinement |

The all-default declared top form is illegal:

| URN | Status |
|-----|--------|
| `cap:` | inadmissible / illegal |
| `cap:in=media:;out=media:` | inadmissible / illegal |

The identity cap is:

```rust
pub const CAP_IDENTITY: &str = "cap:effect=none";
```

Every capset **must** include the identity cap (see CU1 in
[10-VALIDATION-RULES](/docs/10-validation-rules)).

### 4.3 Source, Sink, Effect: void as Unit

`media:void` lets the `(i, o, y, e)` structure express caps that are not
data transformers in the conventional sense.

- A **Source** has `i = media:void` and `o ≠ media:void`. It produces
  a value with no meaningful input. Examples: warming a model
  (`cap:in=media:void;out=media:model-artifact;warm`), search-models,
  list-compatible-models, generators driven by configuration alone.
- A **Sink** has `i ≠ media:void` and `o = media:void`. It absorbs a
  value with no meaningful output. Examples: discard caps, log-to-
  telemetry, append-to-index.
- An **Effect** has both sides `media:void`. Reads as `() → ()`. A
  nullary side-effect cap: warm-cache, ping, health-check,
  initialize-index, sync-registry, log-heartbeat. Valid in the type
  theory; useful in practice for command-style operations whose
  whole purpose is the side effect.

In all three cases the `y` dimension may carry any tags. `media:void`
on a side is a directional decision; `y` continues to refine the
identity of the cap.

### 4.4 Transform: The Default

`Transform` is the catch-all: at least one side is a non-void media
URN, and the cap is neither the explicit identity nor the generic
top-to-top cap. Transform covers the
overwhelming majority of caps in practice — the actual data
processors (extract, render, generate-text, embed, convert).

### 4.5 Why the Distinction Is Logical Only

Dispatch (the `accepts` / `conforms_to` predicates) operates on the
`(i, o, y, effect)` cap identity uniformly. It does not consult `CapKind`. A
`Source` and a `Transform` whose `in` happens to specialize a
pattern's `media:void` are matched by the same rules; the kind is a
description of the result, not a routing dimension.

This separation matters because:

- The protocol stays simple (one matching rule over four structural coordinates).
- Tools and humans can still reason about caps in plain terms
  ("this is a Source — it doesn't take input").
- The kind cannot drift: it is always derivable from the URN. There
  is no separate field to keep in sync, and no flag a cartridge
  could set wrongly.

---

## 5. Dimension Semantics

The four structural coordinates `(i, o, y, e)` correspond to four
independent questions. The kind taxonomy from §4 is what falls out when those
questions are answered.

### 5.1 Input Dimension (i)

`in` answers: *what data does this cap consume?*

| Value          | Meaning                                          |
|----------------|--------------------------------------------------|
| `media:pdf`    | "Requires a PDF."                                |
| `media:`       | "Accepts any input." (top — Identity / generic)  |
| `media:void`   | "Takes no data input." (unit — Source / Effect)  |

`media:` and `media:void` are not interchangeable: one says "anything
goes here," the other says "nothing flows here."

### 5.2 Output Dimension (o)

`out` answers: *what data does this cap produce?*

| Value          | Meaning                                          |
|----------------|--------------------------------------------------|
| `media:json`   | "Produces JSON."                                 |
| `media:`       | "May produce any output." (top) |
| `media:void`   | "Produces no data." (unit — Sink / Effect)       |

### 5.3 Cap-Tags Dimension (y)

`y` answers: *what specifies, refines, or labels this cap beyond its
data signature?*

```
cap:...;extract;target=metadata
```

`y` is itself a Tagged URN (without prefix), with the same matching
semantics as any other Tagged URN. Tags in `y` are arbitrary — no
key has functional meaning to the protocol. They distinguish caps
with the same data signature (e.g. an `extract` cap and a `summarize`
cap can both have `media:pdf → media:textable` and remain distinct
because their `y` differs).

A non-empty `y` is also what distinguishes `cap:passthrough`
(Transform) from the rejected bare top form, even though the
directional axes match.

---

## 6. Accessing Components

### 6.1 Extracting Dimensions

Given a Cap URN string, extract:

```rust
let cap = CapUrn::from_string("cap:extract;in=media:pdf;out=media:textable")?;

let input: &str = cap.in_spec();    // "media:pdf"
let output: &str = cap.out_spec();  // "media:textable"
let has_extract: bool = cap.has_marker_tag("extract"); // true
let kind: CapKind = cap.kind()?;    // CapKind::Transform
```

`kind()` derives the [CapKind](#4-cap-kinds) classification from the
parsed `(i, o, y, e)` structure. It returns an error only on internally
inconsistent state (which `CapUrn` construction prevents) — a hard
signal that something upstream is broken.

### 6.2 Component Types

| Component | Type             | Access            |
|-----------|------------------|-------------------|
| Input     | Media URN string | `in_spec()`       |
| Output    | Media URN string | `out_spec()`      |
| Cap-tags  | Key-value map    | `tags`, `tag(key)`|
| Kind      | `CapKind` enum   | `kind()`          |

---

## 7. Specificity

Cap URN specificity is defined in
[05-SPECIFICITY](/docs/05-specificity). Cap URNs have four structural
coordinates, but the numeric specificity score only ranks the three
tag-bearing weighted coordinates `out`, `in`, and `y`. Those three use
the same six-form per-tag ladder (`?x`:0, `x?=v`:1, `x` (=`x=*`):2,
`x!=v`:3, `x=v`:4, `!x`:5), with axis weights:

```
spec_C(c) = 10_000 * spec_U(c.out)
          +    100 * spec_U(c.in)
          +          spec_U(c.y)
```

The lexicographic priority `(out, in, y)` reflects routing intent:
producing different things is the largest semantic difference between
two caps; consuming different things is next; descriptive y-axis
metadata is last. Two orders of magnitude separate each axis so
per-axis sums up to ~99 stay in their own digit slot, making the
integer both totally ordered and visually decodable (`40205` reads
as out=4, in=2, y=5).

The `effect` coordinate is structural but unscored. It changes
dispatch/runtime behavior, but it is not treated as a graded
refinement dimension for ranking.

Examples by kind, showing the three weighted-coordinate sums
`(out, in, y)` and the
weighted total:

| URN                                              | Kind      | (out, in, y) | spec_C |
|--------------------------------------------------|-----------|:------------:|-------:|
| `cap:?effect`                                   | Transform | (0, 0, 0)    |      0 |
| `cap:extract`                                    | Transform | (0, 0, 2)    |      2 |
| `cap:extract;in=media:pdf;out=media:textable`    | Transform | (2, 2, 2)    |  20202 |
| `cap:in=media:void;out=media:void;ping`          | Effect    | (2, 2, 2)    |  20202 |
| `cap:extract;target=metadata`                    | Transform | (0, 0, 6)    |      6 |

The fully unconstrained explicit request `cap:?effect` sits at
specificity 0. Identity is explicit `cap:effect=none`; it is no
longer the bare top form.

---

## 8. Partial Order Structure

Cap URNs form a partial order (specialization order) in the product
space. The fully unconstrained explicit request `cap:?effect` is the top:

```
                         cap:?effect                   (top request)
                             |
                        cap:extract                       (Transform)
                       /            \
       cap:extract;in=media:pdf       cap:extract;out=media:textable
                       \            /
            cap:extract;in=media:pdf;out=media:textable
                             |
   cap:extract;in=media:pdf;out=media:textable;target=metadata     (more specific)
```

The ordering follows from the dispatch relation (see
[07-DISPATCH](/docs/07-dispatch)). Note that the kind can change as
you move down the lattice — `cap:effect=none` is Identity, while
other refinements are Transform/Source/Sink/Effect as their structure dictates.

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

Cap URNs must satisfy (from [10-VALIDATION-RULES](/docs/10-validation-rules)):

- **CU1**: Must have `in` and `out` tags (enforced via normalization)
- **CU2**: Direction values must be valid Media URNs or `*`

---

## 11. Common Patterns by Kind

This section walks the five kinds with concrete, idiomatic examples.

### 11.1 Identity

```
cap:effect=none
```

The identity morphism. Fully generic on `in`, `out`, and `y`, with an
explicit `effect=none` promise. Required in all capsets.

### 11.2 Transform — typed data processor

```
cap:extract;in=media:pdf;out=media:textable
cap:generate;constrained;in=media:textable;language=en;out=media:json
cap:render-page-image;in=media:pdf;out=media:image
```

The bread and butter: real data flows in, real data flows out, the
`y` dimension labels the operation and any modifiers.

### 11.3 Source — generator

```
cap:in=media:void;out=media:model-artifact;warm
cap:in=media:void;out=media:model-list;list-compatible-models
cap:in=media:void;out=media:embeddings-dim;numeric;target=embeddings-dim
```

`media:void` on the input side: the cap produces a value driven by
its `y` (configuration tags, args, peer state) rather than a piped
input.

### 11.4 Sink — consumer

```
cap:discard;in=media:;out=media:void
cap:in=media:json;log;out=media:void
```

`media:void` on the output side: the cap absorbs a value but
contributes no data to the downstream flow. Useful as a graph
terminator.

### 11.5 Effect — nullary side-effect

```
cap:in=media:void;out=media:void;ping
cap:in=media:void;out=media:void;warm-cache
cap:in=media:void;out=media:void;health-check
```

Both sides `media:void`. The whole point of the cap is the side
effect: the protocol carries no data either way, only the
invocation. Read as `() → ()`.

---

## 12. Summary

| Concept            | Definition                                                  |
|--------------------|-------------------------------------------------------------|
| Structure          | `C = U × U × U × E`                                         |
| Components         | `(in, out, y, effect)`                                      |
| Top type           | `media:` — the universal wildcard for either direction      |
| Unit type          | `media:void` — nullary value (no data flows on this side)   |
| Identity           | `cap:effect=none`                                           |
| Illegal bare form  | `cap:` and `cap:in=media:;out=media:` are inadmissible      |
| Direction defaults | Missing or `*` → `media:`; canonical drops `in`/`out` when both are `media:` on admissible caps |
| Functional axes    | `in`, `out`, and `effect` participate in dispatch/runtime typing; `y` is arbitrary metadata |
| Kinds              | Identity, Source, Sink, Effect, Transform — logical-only    |

Cap URNs extend Tagged URNs with four-dimensional structure. The
dispatch relation (next document) defines how these dimensions
interact for routing. Once dispatch is in place, multiple Cap URNs
can be wired into a data-flow graph and serialized via
[09-MACHINE-NOTATION](/docs/09-machine-notation).
