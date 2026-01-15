# Matching Semantics

## Overview

- **Specification:** `/Users/bahram/ws/prj/fgnd-ws/capns/docs/RULES.md` (Sections 12-17: Matching Semantics)
- **Reference Implementation:** `/Users/bahram/ws/prj/fgnd-ws/capns/src/cap_urn.rs` (lines 348-389)

## Reference Implementation (Rust)

Location: `/Users/bahram/ws/prj/fgnd-ws/capns/src/cap_urn.rs`

```rust
/// Check if this cap matches another based on tag compatibility
///
/// A cap matches a request if:
/// - For each tag in the request: cap has same value, wildcard (*), or missing tag
/// - For each tag in the cap: if request is missing that tag, that's fine (cap is more specific)
/// Missing tags are treated as wildcards (less specific, can handle any value).
pub fn matches(&self, request: &CapUrn) -> bool {
    // Check all tags that the request specifies
    for (request_key, request_value) in &request.tags {
        match self.tags.get(request_key) {
            Some(cap_value) => {
                if cap_value == "*" {
                    // Cap has wildcard - can handle any value
                    continue;
                }
                if request_value == "*" {
                    // Request accepts any value - cap's specific value matches
                    continue;
                }
                if cap_value != request_value {
                    // Cap has specific value that doesn't match request's specific value
                    return false;
                }
            }
            None => {
                // Missing tag in cap is treated as wildcard - can handle any value
                continue;
            }
        }
    }

    // If cap has additional specific tags that request doesn't specify, that's fine
    // The cap is just more specific than needed
    true
}
```

## Key Semantics

1. **Missing tag in CAP = implicit wildcard** - cap can handle any value for that dimension
2. **Missing tag in REQUEST = "don't care"** - request accepts any value
3. **Wildcard `*` in CAP** - cap can handle any value
4. **Wildcard `*` in REQUEST** - request accepts any value
5. **Cap with extra tags** - still matches if request's constraints are satisfied

## Test Cases

```
Test 1: Exact match
  Cap:     cap:op=generate;ext=pdf
  Request: cap:op=generate;ext=pdf
  Result:  MATCH

Test 2: Cap missing tag (implicit wildcard)
  Cap:     cap:op=generate
  Request: cap:op=generate;ext=pdf
  Result:  MATCH (cap can handle any ext)

Test 3: Cap has extra tag
  Cap:     cap:op=generate;ext=pdf;version=2
  Request: cap:op=generate;ext=pdf
  Result:  MATCH (request doesn't constrain version)

Test 4: Request has wildcard
  Cap:     cap:op=generate;ext=pdf
  Request: cap:op=generate;ext=*
  Result:  MATCH (request accepts any ext)

Test 5: Cap has wildcard
  Cap:     cap:op=generate;ext=*
  Request: cap:op=generate;ext=pdf
  Result:  MATCH (cap handles any ext)

Test 6: Value mismatch
  Cap:     cap:op=generate;ext=pdf
  Request: cap:op=generate;ext=docx
  Result:  NO MATCH

Test 7: Fallback pattern
  Cap:     cap:op=generate_thumbnail;out=media:type=binary;v=1
  Request: cap:op=generate_thumbnail;out=media:type=binary;v=1;ext=wav
  Result:  MATCH (cap has implicit ext=*)

Test 8: Empty cap matches anything
  Cap:     cap:
  Request: cap:op=generate;ext=pdf
  Result:  MATCH

Test 9: Cross-dimension independence
  Cap:     cap:op=generate
  Request: cap:ext=pdf
  Result:  MATCH (both have implicit wildcards for missing tags)
```

---