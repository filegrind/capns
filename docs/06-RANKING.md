# Ranking

## 1. Separation of Concerns

The system distinguishes two phases:

1. **Dispatch Validity** — Is this provider legal for this request?
2. **Ranking** — Among valid providers, which should be selected?

This document covers ranking. Dispatch validity is defined in [05-DISPATCH](./05-DISPATCH.md).

**Critical**: Ranking applies ONLY to dispatch-valid providers. Never rank before validating.

---

## 2. The Valid Set

Given a request `r`, define the valid set:

```
Valid(r) = { p ∈ C | Dispatch(p, r) }
```

Ranking is a total order over `Valid(r)`.

---

## 3. Specificity Distance

### 3.1 Definition

For a provider `p` and request `r`:

```
dist(p, r) = spec_C(p) - spec_C(r)
```

Where `spec_C` is Cap URN specificity (see [03-SPECIFICITY](./03-SPECIFICITY.md)).

### 3.2 Interpretation

| Distance | Meaning | Provider Relationship |
|----------|---------|----------------------|
| `dist = 0` | Equivalent | Provider matches request exactly |
| `dist > 0` | Refinement | Provider is more specific |
| `dist < 0` | Fallback | Provider is more generic |

---

## 4. Preference Order

### 4.1 Standard Policy

The default ranking policy prefers:

1. **Exact match** (`dist = 0`) — Most preferred
2. **Refinement** (`dist > 0`) — Provider specializes request
3. **Fallback** (`dist < 0`) — Provider is generic, last resort

Within each category, prefer smaller absolute distance.

### 4.2 Formal Ordering

For providers `a` and `b` in `Valid(r)`:

```
a ≺ b  (a preferred over b)  iff:
    dist(a,r) ≥ 0 ∧ dist(b,r) < 0           # a is refinement, b is fallback
  ∨ (dist(a,r) ≥ 0 ∧ dist(b,r) ≥ 0 ∧ dist(a,r) < dist(b,r))  # both refinements, a closer
  ∨ (dist(a,r) < 0 ∧ dist(b,r) < 0 ∧ |dist(a,r)| < |dist(b,r)|)  # both fallbacks, a closer
```

### 4.3 Simplified Rule

```
preferred = min(valid_providers, key=ranking_key)

def ranking_key(p, r):
    d = dist(p, r)
    if d >= 0:
        return (0, d)      # refinement tier, prefer smaller positive
    else:
        return (1, -d)     # fallback tier, prefer smaller negative
```

---

## 5. Tie-Breaking

When multiple providers have the same distance:

### 5.1 Default Policy

**First registered wins** — The provider that was registered first is selected.

### 5.2 Alternative Policies

Some subsystems use different tie-breaking:

| Subsystem | Policy |
|-----------|--------|
| UrnMatcher | First by specificity, then by registration order |
| CapMatrix | Strict `>` comparison (no ties possible) |
| CapBlock | Strict `>` comparison (no ties possible) |
| RelaySwitch | First registered wins |

### 5.3 Avoiding Ties

Design caps to avoid ties by ensuring distinct specificities:
- Add distinguishing tags
- Use consistent naming conventions
- Register more specific caps before generic ones

---

## 6. Examples

### 6.1 Exact Match Preferred

```
Request: cap:in=media:pdf;op=extract;out=media:object
         spec = 3

Valid providers:
  A: cap:in=media:pdf;op=extract;out=media:object     spec=3, dist=0
  B: cap:in=media:pdf;op=extract;out=media:object;v=2 spec=4, dist=+1
  C: cap:op=extract                                    spec=1, dist=-2

Ranking: A (dist=0) ≺ B (dist=+1) ≺ C (dist=-2)
Selected: A
```

### 6.2 Refinement When No Exact Match

```
Request: cap:op=convert
         spec = 1

Valid providers:
  A: cap:in=media:pdf;op=convert;out=media:html   spec=3, dist=+2
  B: cap:in=media:image;op=convert;out=media:png  spec=3, dist=+2
  C: cap:op=convert                                spec=1, dist=0

Ranking: C (dist=0) ≺ {A, B} (dist=+2, tie)
Selected: C

If C not available:
Ranking: {A, B} tied at dist=+2
Selected: First registered of A or B
```

### 6.3 Fallback Only

```
Request: cap:in=media:pdf;v=2.0;op=extract;out=media:object;format=json
         spec = 5

Valid providers:
  A: cap:in=media:pdf;op=extract;out=media:object  spec=3, dist=-2
  B: cap:op=extract                                 spec=1, dist=-4

Ranking: A (dist=-2) ≺ B (dist=-4)
Selected: A (closer to request despite being less specific)
```

---

## 7. Preferred Cap Hints

### 7.1 User Preference

The system supports an optional **preferred cap** hint:

```rust
RelaySwitch::route_request(request, preferred_cap: Option<&CapUrn>)
```

When `preferred_cap` is provided:
1. Find valid providers via `is_dispatchable`
2. Among valid providers, check if any is `is_equivalent` to preferred
3. If found, select it regardless of specificity ranking
4. Otherwise, fall back to normal ranking

### 7.2 Use Cases

- User explicitly requests a specific capability version
- Configuration specifies a particular provider
- Testing with known handler

---

## 8. Properties

### 8.1 Determinism

Given the same:
- Request
- Set of registered providers
- Registration order

The selected provider is deterministic.

### 8.2 Stability

Adding a new provider P to the system:
- If P is not in `Valid(r)`, selection unchanged
- If P is in `Valid(r)`, P may be selected only if it ranks higher

### 8.3 Monotonicity

If provider `p` is selected for request `r`, then `p` remains selected for any request `r'` where:
- `r' ⪯ r` (r' is more specific than r)
- `Dispatch(p, r')` still holds

---

## 9. Implementation Notes

### 9.1 Efficient Selection

For small provider sets, linear scan is sufficient:

```rust
fn select_best(providers: &[CapUrn], request: &CapUrn) -> Option<&CapUrn> {
    let request_spec = request.specificity();

    providers.iter()
        .filter(|p| p.is_dispatchable(request))
        .min_by_key(|p| {
            let dist = p.specificity() as isize - request_spec as isize;
            if dist >= 0 {
                (0, dist)
            } else {
                (1, -dist)
            }
        })
}
```

### 9.2 Caching

For frequently queried requests, cache:
- The valid set `Valid(r)`
- The sorted ranking
- The selected provider

Invalidate on provider registration/unregistration.

---

## 10. Relationship to is_comparable

The `is_comparable` predicate from [02-PREDICATES](./02-PREDICATES.md) is sometimes used for **discovery** but NOT for ranking:

```
is_comparable(p, r)  ⟹̸  Dispatch(p, r)
```

Comparable providers may not be dispatchable. Always check `is_dispatchable` first.

---

## 11. Summary

| Concept | Definition |
|---------|------------|
| Valid set | `{ p | Dispatch(p, r) }` |
| Distance | `spec_C(p) - spec_C(r)` |
| Preference | Exact (0) > Refinement (+) > Fallback (-) |
| Tie-break | First registered (default) |

Ranking is policy, not semantics. The dispatch predicate defines validity; ranking determines selection among valid options.
