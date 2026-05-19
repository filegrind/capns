---
title: "Specificity"
layout: doc
permalink: /05-specificity/
---
# Specificity

## 1. Purpose

Specificity is a numeric score measuring how constrained a URN is. It is used for:

1. **Ranking** among dispatch-valid providers
2. **Tie-breaking** when multiple providers match
3. **Distance calculation** for preference ordering

Specificity is **NOT** used for dispatch validity. A provider with higher specificity is not automatically dispatchable.

---

## 2. Tagged URN Specificity

### 2.1 Definition

```
spec_U : U → ℕ
```

The specificity of a Tagged URN is the sum of per-tag scores.

### 2.2 Per-Tag Scoring

The constraint alphabet has six canonical forms. Each contributes a
fixed integer to the URN's specificity score:

| Authored alias              | Canonical | Stored value | Score | Reading                                  |
|-----------------------------|-----------|--------------|------:|------------------------------------------|
| `?x` ≡ `x?` ≡ `x=?`         | `?x`      | `"?"`        |     0 | no constraint                            |
| `?x=v` ≡ `x?=v` ≡ `x=?v`    | `x?=v`    | `"?=v"`      |     1 | absent OR (present and not v)            |
| `x` ≡ `x=*`                 | `x`       | `"*"`        |     2 | present with any value (must-have-any)   |
| `!x=v` ≡ `x!=v` ≡ `x=!v`    | `x!=v`    | `"!=v"`      |     3 | present and not v                        |
| `x=v`                       | `x=v`     | `"v"`        |     4 | present and exactly v                    |
| `!x` ≡ `x!` ≡ `x=!`         | `!x`      | `"!"`        |     5 | absent (must-not-have)                   |

Distinct scores for every form aid disambiguation when ties would
otherwise occur. The ladder is monotone within each branch:

- **Positive chain** (presence claims):
  `?x` (0) → `x` (2) → `x=v` (4) — from no constraint to exact identification.
- **Negative chain** (absence/exclusion claims):
  `?x` (0) → `x?=v` (1) → `x!=v` (3) → `!x` (5) — from no constraint to enforced absence.

A missing key entry in the tag map scores 0 (same as the explicit
`?x`); the parser treats `?x`, `x?`, and `x=?` as exact aliases of
"no-constraint", and serialization always emits the canonical `?x`.

### 2.3 Formula

```
spec_U(u) = Σ score(tag) for all tags in u
```

### 2.4 Examples

```
media:                           → 0        # no tags
media:bytes                      → 2        # bytes=* (bare = must-have-any)
media:pdf;bytes                  → 2 + 2 = 4
media:pdf;v=2.0                  → 2 + 4 = 6  # pdf=*, v=2.0 (exact)
media:pdf;v=2.0;!compressed      → 2 + 4 + 5 = 11
media:textable;form=scalar       → 2 + 4 = 6
media:pdf;v?=draft               → 2 + 1 = 3  # absent OR not draft
media:pdf;v!=draft               → 2 + 3 = 5  # present and not draft
```

---

## 3. Cap URN Specificity

### 3.1 Definition

```
spec_C : C → ℕ
```

Cap URNs have four structural coordinates `(in, out, y, effect)`, but
the numeric specificity score is the weighted sum of the three
tag-bearing coordinates:
1. Input dimension specificity
2. Output dimension specificity
3. Non-direction tag specificity

The `effect` coordinate is structural and participates in dispatch,
conformance, and runtime output inference, but it is **not** part of
the numeric specificity score. Effect differences are handled as exact
structural distinctions, not as a stronger-or-weaker refinement ladder.

### 3.2 Formula

```
spec_C(c) = WEIGHT_OUT * spec_U(c.out)
          + WEIGHT_IN  * spec_U(c.in)
          + spec_U(c.y)

WEIGHT_OUT = 10_000
WEIGHT_IN  =    100
```

The three weighted coordinates are not equally weighted. Two orders of magnitude
separate each, producing a single integer whose digit slots
encode `(out, in, y)` for visual decoding (`40205` reads as
out=4, in=2, y=5). The lexicographic priority `(out, in, y)`
reflects routing intent:

1. **Producing different things** is the largest semantic
   difference between two caps — they are not substitutable to
   downstream consumers.
2. **Consuming different things** is next — same artifact, alternative
   producers; the planner ranks among them.
3. **Differing in y-axis metadata** is the smallest difference — `y`
   is descriptive, not structural.

`spec_U` is the **same** Tagged URN specificity function from §2.1.
All three weighted coordinates — `in`, `out`, and `y` — go through identical per-tag
scoring; there is no cap-axis carve-out, no privileged tag key, no
"y is just a count" simplification. The axis weights apply *after*
the per-axis sums are computed.

The `effect` coordinate is intentionally outside this arithmetic. A cap
with `effect=none` is not "more specific" than the same `(in, out, y)`
with `effect=declared`; it is a different structural promise about
runtime media identity, and dispatch treats that difference exactly.

### 3.3 Direction Dimension Scoring

`in` and `out` are Media URNs. Specificity is the sum of per-tag
scores from the truth table (§2.2):

```
spec_U(media:)                       = 0           (no tags, top of order)
spec_U(media:pdf)                    = 2           (pdf=*, must-have-any)
spec_U(media:pdf;bytes)              = 2 + 2 = 4
spec_U(media:pdf;v=2.0)              = 2 + 4 = 6   (pdf=*, v=2.0 exact)
spec_U(media:pdf;!compressed)        = 2 + 5 = 7   (pdf=*, !compressed must-not-have)
spec_U(media:pdf;?v)                 = 2 + 0 = 2   (pdf=*, v explicit no-constraint)
spec_U(media:void)                   = 2           (void marker = must-have-any)
```

The top type `media:` carries 0 tags and so contributes 0; the unit
type `media:void` carries one marker (`void`, value `*`) and so
contributes 2.

### 3.4 Cap-Tag Scoring

The cap-tag dimension `y` is itself a Tagged URN of arbitrary
descriptive tags — no key in `y` has functional meaning to the
protocol, but each tag's *form* is scored exactly like any other
Tagged URN tag (§2.2):

| Tag form in `y`                         | Score | Reading                           |
|-----------------------------------------|------:|-----------------------------------|
| missing (key absent)                    |     0 | no constraint                     |
| `?key`                                  |     0 | explicit no-constraint            |
| `key?=v`                                |     1 | absent OR (present and not v)     |
| bare marker (e.g. `extract`, value `*`) |     2 | must-have-any                     |
| `key=*`                                 |     2 | must-have-any (same as bare)      |
| `key!=v`                                |     3 | present and not v                 |
| exact (`key=value`)                     |     4 | must-have-this-value              |
| `!key`                                  |     5 | must-not-have                     |

Bare segments and `key=*` are the **same** form: a marker tag
written `extract` parses as `extract=*`, and contributes 2 just like
any other must-have-any tag.

This uniformity is deliberate. Specificity is a structural property
of the URN's tag set, not of which axis the tag lives on. A cap that
constrains `target=metadata` (exact, score 4) is more specific than
one that just declares `target=*` (must-have-any, score 2), exactly
the same way `media:pdf;v=2.0` is more specific than `media:pdf;v=*`.

### 3.5 Examples

Each example shows the three weighted-coordinate sums `(out, in, y)`,
then the
weighted total `10000*out + 100*in + y`:

```
cap:effect=none                              → (0, 0, 0)         = 0
# in=media: (0), out=media: (0), y empty (0); effect is structural, not scored

cap:extract                                   → (0, 0, 2)         = 2
# in=media: (0), out=media: (0), extract=* (must-have-any → 2)

cap:extract;in=media:pdf;out=media:textable   → (2, 2, 2)         = 20202
# out: textable=* (2); in: pdf=* (2); y: extract=* (2)

cap:in="media:pdf;bytes";extract;out="media:textable;record"
                                              → (4, 4, 2)         = 40402
# out: textable=* + record=* (2+2); in: pdf=* + bytes=* (2+2); y: extract=* (2)

cap:in=*;extract;out=*                        → (0, 0, 2)         = 2
# in=* normalizes to media: (0); out=* normalizes to media: (0); y: extract=* (2)

cap:extract;target=metadata                   → (0, 0, 6)         = 6
# y: extract=* (2) + target=metadata (exact → 4)

cap:extract;?target                           → (0, 0, 2)         = 2
# y: extract=* (2) + ?target (explicit no-constraint → 0)

cap:extract;!constrained                      → (0, 0, 7)         = 7
# y: extract=* (2) + !constrained (must-not-have → 5)

cap:in=media:void;out=media:void;ping         → (2, 2, 2)         = 20202
# out: void=* (2); in: void=* (2); y: ping=* (2)
```

Two caps with the same `out` axis but different `in` axes will
*always* compare on the `in` axis before the `y` axis, because
`WEIGHT_IN = 100 > 1`. Two caps with different `out` axes always
compare there first, regardless of `in` and `y`.

---

## 4. Properties

### 4.1 Non-Negativity

```
∀u ∈ U, spec_U(u) ≥ 0
∀c ∈ C, spec_C(c) ≥ 0
```

### 4.2 Identity Has Zero Specificity

```
spec_U(media:) = 0
spec_C(cap:effect=none) = 0
```

### 4.3 More Constraints = More Specific

Adding any tag whose form scores above 0 (everything except
`?` / missing) increases specificity:
```
spec_U(media:)              < spec_U(media:pdf)              # added must-have-any
spec_U(media:pdf)           < spec_U(media:pdf;bytes)        # added another must-have-any
spec_U(media:pdf;v=*)       < spec_U(media:pdf;v=2.0)        # tightened * (2) → exact (4)
spec_U(media:pdf;v?=draft)  < spec_U(media:pdf;v!=draft)     # tightened ?=v (1) → !=v (3)
spec_U(media:pdf;v!=draft)  < spec_U(media:pdf;!v)           # tightened !=v (3) → ! (5)
```

The ladder is monotone within each branch:
- Positive: `?x` (0) → `x` (2) → `x=v` (4)
- Negative: `?x` (0) → `x?=v` (1) → `x!=v` (3) → `!x` (5)

Cross-branch transitions (e.g. `*` → `!`) also only increase the
score, since `!` (5) > `*` (2).

### 4.4 Specificity Does Not Determine ⪯

**Important**: Higher specificity does NOT imply the refinement relation.

```
spec_U(media:pdf) = 2
spec_U(media:image) = 2
```

These have equal specificity but are NOT comparable (different branches).

---

## 5. Usage

### 5.1 Ranking

After dispatch validity is established, rank providers by specificity distance:

```
dist(provider, request) = spec_C(provider) - spec_C(request)
```

Interpretation:
- `dist = 0`: Same numeric specificity score
- `dist > 0`: Provider is more specific (refinement)
- `dist < 0`: Provider is more generic (fallback)

### 5.2 Selection Priority

1. **Exact score match** (dist = 0) — most preferred among equally scored dispatch-valid providers
2. **Refinement** (dist > 0) — provider specializes request
3. **Fallback** (dist < 0) — provider is generic, use as last resort

See [06-RANKING](/docs/08-ranking) for complete ranking policy.

---

## 6. One Scoring Function, Three Weighted Axes

There is exactly one per-tag scoring function (§2.2's six-form
ladder). It is applied uniformly to every tag in every axis of every
URN. There is no separate "graded" vs "tag-count" mode.

What differs between Tagged URN and Cap URN specificity is only the
**axis weighting**:

- Tagged URN: one axis (the URN's own tag set), weight 1.
- Cap URN: four structural coordinates; specificity ranks the three weighted coordinates (`out`, `in`, `y`) with weights `(10000, 100, 1)`, while `effect` remains an exact structural gate outside the numeric score
- This gives `out` and `in` lexicographic priority over `y` while collapsing ranking into a single comparable integer

Same per-tag ladder. Same arithmetic. Just different axis weights.

---

## 7. Edge Cases

### 7.1 Empty URN

```
spec_U(prefix:) = 0
spec_C(cap:effect=none) = 0
```

The identity has zero specificity. `cap:effect=none` is the canonical
identity-cap form. The bare declared top forms `cap:` and
`cap:in=media:;out=media:` are illegal.

### 7.2 All Wildcards

```
spec_U(media:a;b;c) = 2 + 2 + 2 = 6
```

Bare markers (`x` ≡ `x=*`) still contribute to specificity (they are
must-have-any constraints, not absence).

### 7.3 Must-Not-Have

```
spec_U(media:!pdf)        = 5    # !x (must-not-have, top of negative chain)
spec_U(media:pdf;v?=draft) = 2 + 1 = 3   # absent OR not draft (weakest negative)
spec_U(media:pdf;v!=draft) = 2 + 3 = 5   # present and not draft
```

The negative branch (`?x=v` → `x!=v` → `!x`) provides graded
specificity for absence/exclusion claims.

---

## 8. Summary

| Concept | Definition |
|---------|------------|
| Tagged URN spec | Σ score(tag) over six-form ladder (0–5) |
| Cap URN spec   | `10000 * spec_U(out) + 100 * spec_U(in) + spec_U(y)` |
| Distance | spec(provider) - spec(request) |
| Use | Ranking, not validity |

Specificity measures how constrained a URN is. It enables ranking
among valid providers but does not determine dispatch validity.
