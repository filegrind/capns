# Cap Matching Semantics

## Overview

Cap matching extends Tagged URN matching with direction specifier awareness. For base matching algorithm details, see the Tagged URN documentation.

- **Base Specification:** [Tagged URN RULES.md](../../tagged-urn-rs/docs/RULES.md) (Matching Semantics section)
- **Cap-Specific Rules:** [Cap URN RULES.md](./RULES.md)
- **Reference Implementation:** `capns/src/cap_urn.rs`

## Cap-Specific Matching Behavior

### Direction Specifiers in Matching

Cap URNs have required `in` and `out` tags (direction specifiers) whose values are Media URNs. These are **always** part of matching:

```rust
/// Check if this cap matches a request
/// Direction specifiers (in/out) are always considered in matching
pub fn matches(&self, request: &CapUrn) -> bool {
    // Standard tagged URN matching applies to all tags including in/out
    // Direction specifiers must match (or be wildcards) for a cap to handle a request
    self.urn.matches(&request.urn)
}
```

### Test Cases

```
Test 1: Exact match with direction specifiers
  Cap:     cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Request: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Result:  MATCH

Test 2: Cap has wildcard direction specifiers (fallback provider)
  Cap:     cap:in=*;op=extract;out=*
  Request: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Result:  MATCH (cap can handle any input/output)

Test 3: Direction specifier mismatch
  Cap:     cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Request: cap:in="media:type=text;v=1";op=extract;out="media:type=object;v=1"
  Result:  NO MATCH (input media types differ)

Test 4: Cap has extra tags (more specific)
  Cap:     cap:ext=pdf;in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Request: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
  Result:  MATCH (request doesn't constrain ext)

Test 5: Void input (generation cap)
  Cap:     cap:in="media:type=void;v=1";op=generate;out="media:type=binary;v=1"
  Request: cap:in="media:type=void;v=1";op=generate;out="media:type=binary;v=1"
  Result:  MATCH
```

### Provider Selection

When multiple caps match a request, select by specificity:

1. Collect all caps where `cap.matches(request)` is true
2. Calculate specificity (count of non-wildcard tags, including direction specifiers)
3. Select highest specificity; ties go to first registered

```
Request: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1";ext=pdf

Cap A: cap:in=*;op=extract;out=*                    (specificity: 1)
Cap B: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"  (specificity: 3)
Cap C: cap:ext=pdf;in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"  (specificity: 4)

Winner: Cap C (highest specificity)
```
