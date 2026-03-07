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

| Tag Value | Score | Meaning |
|-----------|-------|---------|
| `?` or missing | 0 | No constraint |
| `!` | 1 | Must-not-have |
| `*` | 2 | Must-have-any |
| exact value | 3 | Exact match required |

### 2.3 Formula

```
spec_U(u) = Σ score(tag) for all tags in u
```

### 2.4 Examples

```
media:                           → 0        # no tags
media:bytes                      → 2        # bytes=* (valueless = *)
media:pdf;bytes                  → 2 + 2 = 4
media:pdf;v=2.0                  → 2 + 3 = 5  # pdf=*, v=2.0 (exact)
media:pdf;v=2.0;!compressed      → 2 + 3 + 1 = 6
media:textable;form=scalar       → 2 + 3 = 5
```

---

## 3. Cap URN Specificity

### 3.1 Definition

```
spec_C : C → ℕ
```

Cap URN specificity combines three components:
1. Input dimension specificity
2. Output dimension specificity
3. Non-direction tag specificity

### 3.2 Formula

```
spec_C(in, out, y) = tags(in) + tags(out) + count(non-* y-tags)
```

Where:
- `tags(x)` = number of tags in media URN x (0 if x = "media:")
- `count(non-* y-tags)` = number of cap tags with non-wildcard values

### 3.3 Direction Dimension Scoring

For `in` and `out` dimensions:
- If the value is `media:` (identity), contribution is **0**
- Otherwise, count the number of tags in the media URN

This differs from Tagged URN scoring because:
- Direction specs are Media URNs, not arbitrary tag values
- The identity `media:` represents "any" and should not add specificity

### 3.4 Cap-Tag Scoring

For non-direction tags (op, ext, model, etc.):
- `*` value: **0** (wildcard, no constraint)
- Any other value: **1** (constraint present)

### 3.5 Examples

```
cap:                                         → 0 + 0 + 0 = 0
# in=media: (0), out=media: (0), no cap-tags (0)

cap:op=extract                               → 0 + 0 + 1 = 1
# in=media: (0), out=media: (0), op=extract (1)

cap:in=media:pdf;op=extract;out=media:object → 1 + 1 + 1 = 3
# in has 1 tag (pdf), out has 1 tag (object), op (1)

cap:in="media:pdf;bytes";op=extract;out="media:textable;form=map"
→ 2 + 2 + 1 = 5
# in has 2 tags (pdf, bytes), out has 2 tags (textable, form), op (1)

cap:in=*;op=extract;out=*                    → 0 + 0 + 1 = 1
# in=* normalizes to media: (0), out=* normalizes to media: (0), op (1)
```

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
spec_C(cap:) = 0
```

### 4.3 More Tags = More Specific

Adding tags (with non-wildcard values) increases specificity:
```
spec_U(media:pdf) < spec_U(media:pdf;bytes)
```

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
- `dist = 0`: Equivalent specificity (exact match)
- `dist > 0`: Provider is more specific (refinement)
- `dist < 0`: Provider is more generic (fallback)

### 5.2 Selection Priority

1. **Exact match** (dist = 0) — most preferred
2. **Refinement** (dist > 0) — provider specializes request
3. **Fallback** (dist < 0) — provider is generic, use as last resort

See [06-RANKING](./06-RANKING.md) for complete ranking policy.

---

## 6. Graded vs Tag-Count Scoring

The system uses two different scoring methods:

### 6.1 Graded Scoring (Tagged URN internals)

Used for comparing wildcards within a single URN:
```
? = 0, ! = 1, * = 2, value = 3
```

This captures the semantic strength of constraints.

### 6.2 Tag-Count Scoring (Cap URN dimensions)

Used for comparing Cap URNs:
```
count tags in media URNs + count non-* cap tags
```

This simplifies ranking while maintaining useful ordering.

### 6.3 Why Different?

- Graded scoring preserves semantic distinctions (`!` vs `*` vs exact)
- Tag-count scoring is simpler for multi-dimensional comparison
- For dispatch validity, the exact values matter (via truth table)
- For ranking, coarser granularity is sufficient

---

## 7. Edge Cases

### 7.1 Empty URN

```
spec_U(prefix:) = 0
spec_C(cap:) = 0
```

The identity has zero specificity.

### 7.2 All Wildcards

```
spec_U(media:*;*;*) = 2 + 2 + 2 = 6
```

Wildcards still contribute to specificity (they are constraints).

### 7.3 Must-Not-Have

```
spec_U(media:!pdf) = 1
```

The `!` constraint is weaker than `*` or exact values.

---

## 8. Summary

| Concept | Definition |
|---------|------------|
| Tagged URN spec | Sum of per-tag scores (0/1/2/3) |
| Cap URN spec | tags(in) + tags(out) + count(non-* cap-tags) |
| Distance | spec(provider) - spec(request) |
| Use | Ranking, not validity |

Specificity measures how constrained a URN is. It enables ranking among valid providers but does not determine dispatch validity.
