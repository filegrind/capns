# Cap Matching Semantics

## Overview

Cap matching extends Tagged URN matching with direction specifier awareness. For base matching algorithm details, see the Tagged URN documentation.

- **Base Specification:** [Tagged URN RULES.md](https://github.com/filegrind/tagged-urn-rs/blob/main/docs/RULES.md) (Matching Semantics section)
- **Cap-Specific Rules:** [Cap URN RULES.md](./RULES.md)
- **Reference Implementation:** `capns/src/cap_urn.rs`

## Per-Tag Value Semantics

Cap matching uses the same per-tag semantics as Tagged URN:

| Pattern Value | Meaning | Instance Missing | Instance=v | Instance=x≠v |
|---------------|---------|------------------|------------|--------------|
| (missing) | No constraint | OK | OK | OK |
| `K=?` | No constraint (explicit) | OK | OK | OK |
| `K=!` | Must-not-have | OK | NO | NO |
| `K=*` | Must-have, any value | NO | OK | OK |
| `K=v` | Must-have, exact value | NO | OK | NO |

Special values work symmetrically on both instance and pattern sides.

## Cap-Specific Matching Behavior

### Direction Specifiers in Matching

Cap URNs have required `in` and `out` tags (direction specifiers) whose values are Media URNs. Direction specs use **`TaggedUrn::accepts()`** / **`TaggedUrn::conforms_to()`** (via `MediaUrn`):

- **Input**: `cap_input.accepts(&request_input)` — the cap's input (pattern) checks whether the request's input (instance) satisfies it. All tagged URN matching semantics apply: `*` (must-have-any), `!` (must-not-have), `?` (no constraint), exact values, missing tags (no constraint). A cap accepting `media:bytes` accepts a request with `media:pdf;bytes` because `pdf;bytes` has the `bytes` marker.
- **Output**: `cap_output.conforms_to(&request_output)` — the cap's output (instance) checks whether it conforms to the request's output expectation (pattern).

Direction specs are Media URNs whose internal tags are compared using the full `TaggedUrn::accepts()` / `conforms_to()` semantics, not as opaque strings.

### Test Cases

```
Test 1: Exact match with direction specifiers
  Cap:     cap:in="media:binary";op=extract;out="media:object"
  Request: cap:in="media:binary";op=extract;out="media:object"
  Result:  MATCH

Test 2: Cap has wildcard direction specifiers (fallback provider)
  Cap:     cap:in=*;op=extract;out=*
  Request: cap:in="media:binary";op=extract;out="media:object"
  Result:  MATCH (cap requires any input/output, request has them)

Test 3: Direction specifier mismatch (incompatible types)
  Cap:     cap:in="media:string";op=extract;out="media:object"
  Request: cap:in="media:bytes";op=extract;out="media:object"
  Result:  NO MATCH (completely different marker tags)

Test 4: Cap has extra tags (more specific)
  Cap:     cap:ext=pdf;in="media:binary";op=extract;out="media:object"
  Request: cap:in="media:binary";op=extract;out="media:object"
  Result:  MATCH (request doesn't constrain ext)

Test 5: Request requires tag cap doesn't have
  Cap:     cap:in="media:binary";op=extract;out="media:object"
  Request: cap:ext=*;in="media:binary";op=extract;out="media:object"
  Result:  NO MATCH (request requires ext, cap doesn't have it)

Test 6: Must-not-have in request
  Cap:     cap:in="media:binary";op=extract;out="media:object"
  Request: cap:debug=!;in="media:binary";op=extract;out="media:object"
  Result:  MATCH (cap lacks debug, request wants it absent)

Test 7: Semantic direction matching - generic provider accepts specific input
  Cap:     cap:in="media:bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Request: cap:in="media:pdf;bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Result:  MATCH (request's pdf;bytes has all markers that cap's bytes requires)

Test 8: Semantic direction matching - specific provider rejects generic input
  Cap:     cap:in="media:pdf;bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Request: cap:in="media:bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Result:  NO MATCH (request's bytes lacks pdf marker required by cap)

Test 9: Semantic direction matching - incompatible subtypes
  Cap:     cap:in="media:pdf;bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Request: cap:in="media:epub;bytes";op=generate_thumbnail;out="media:image;png;bytes;thumbnail"
  Result:  NO MATCH (request's epub;bytes lacks pdf marker required by cap)
```

### Graded Specificity

When multiple caps match a request, select by graded specificity:

| Value Type | Score |
|------------|-------|
| Exact value (K=v) | 3 |
| Must-have-any (K=*) | 2 |
| Must-not-have (K=!) | 1 |
| Unspecified (K=?) or missing | 0 |

**Total specificity** = sum of all tag scores

```
Request: cap:ext=pdf;in="media:binary";op=extract;out="media:object"

Cap A: cap:in=*;op=extract;out=*                    (specificity: 2+3+2 = 7)
Cap B: cap:in="media:binary";op=extract;out="media:object"  (specificity: 3+3+3 = 9)
Cap C: cap:ext=pdf;in="media:binary";op=extract;out="media:object"  (specificity: 3+3+3+3 = 12)

Winner: Cap C (highest specificity)
```

### Selection Algorithm

1. Collect all caps where `cap.accepts(&request)` is true
2. Calculate graded specificity for each
3. Select highest specificity
4. Ties use specificity tuple `(exact_count, must_have_any_count, must_not_count)`
5. If still tied, first registered wins
