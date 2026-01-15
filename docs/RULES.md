# Cap URN Rules

## Definitive specification for Cap URN format and behavior

### 1. Case Handling
- **Tag keys:** Always normalized to lowercase
- **Unquoted values:** Normalized to lowercase
- **Quoted values:** Case is preserved exactly as specified
- Example: `cap:key=VALUE` stores `{key: "value"}` (lowercase)
- Example: `cap:key="VALUE"` stores `{key: "VALUE"}` (preserved)

### 2. Tag Order Independence  
The order of tags in the URN string does not matter. Tags are always sorted alphabetically by key in canonical form.

### 3. Mandatory Prefix
Cap URNs must always be preceded by `cap:` which is the signifier of a cap URN. The prefix is case-insensitive for parsing (`CAP:`, `Cap:`, `cap:` are all accepted) but always lowercase in canonical form.

### 4. Tag Separator
Tags are separated by semicolons (`;`).

### 5. Trailing Semicolon Optional
Presence or absence of the final trailing semicolon does not matter. Both `cap:key=value` and `cap:key=value;` are equivalent.

### 6. Character Restrictions

**Unquoted values:**
- Allowed characters in tag keys: alphanumeric, dashes (`-`), underscores (`_`), slashes (`/`), colons (`:`), dots (`.`)
- Allowed characters in unquoted values: same as keys plus asterisk (`*` for wildcards)
- No spaces, quotes, semicolons, equals signs, or backslashes in unquoted values

**Quoted values:**
- Values may be enclosed in double quotes: `key="value"`
- ANY character is allowed inside quoted values (including spaces, semicolons, equals signs)
- Escape sequences inside quotes: `\"` for literal quote, `\\` for literal backslash
- Only `\"` and `\\` are valid escape sequences; other backslash sequences are errors
- Example: `cap:key="value with spaces"` is valid
- Example: `cap:key="value;with=special"` is valid
- Example: `cap:key="quote: \"hello\""` stores `quote: "hello"`

### 7. Tag Structure
- Tag separator within a tag: `=` separates tag key from tag value
- Tag separator between tags: `;` separates key-value pairs
- After the initial `cap:` prefix, colons (`:`) are treated as normal characters, not separators

### 8. No Special Tags
No reserved tag names - anything goes for tag keys.

### 9. Canonical Form and Serialization
- Tags are sorted alphabetically by key
- No final trailing semicolon
- Tag keys are always lowercase
- **Smart quoting on serialization:** Values are quoted only when necessary:
  - Quote if value contains special characters: `;`, `=`, `"`, `\`, or space
  - Quote if value contains any uppercase characters (to preserve case on round-trip)
  - Simple lowercase-only values are serialized without quotes
- Examples:
  - `{key: "simple"}` serializes to `cap:key=simple`
  - `{key: "Has Upper"}` serializes to `cap:key="Has Upper"`
  - `{key: "has;special"}` serializes to `cap:key="has;special"`

### 10. Wildcard Support
- Wildcard `*` is accepted only as tag value, not as tag key
- When used as a tag value, `*` matches any value for that tag key (see Matching Semantics below)

### 11. No Empty Components
Tags with no values are not accepted. Both key and value must be non-empty after trimming whitespace.

## Matching Semantics (CRITICAL)

This section defines how cap URN matching works when finding a provider/capability that can satisfy a request. This is the core algorithm that ALL implementations MUST follow exactly.

### 12. Missing Tags as Implicit Wildcards
A missing tag is semantically equivalent to `tag=*`. This means:
- `cap:op=generate` is equivalent to `cap:op=generate;ext=*;in=*;out=*;...` for all possible tags
- A cap with fewer tags can match requests with more tags (the cap can handle "any" value for missing dimensions)

### 13. The `matches(cap, request)` Function
Given a **cap** (what a provider offers) and a **request** (what is being asked for), the cap matches the request if and only if:

**Step 1: Check all tags in the REQUEST**
For each `(key, value)` in request.tags:
- If cap has the same key:
  - If cap's value is `*` → OK (cap can handle any value)
  - If request's value is `*` → OK (request accepts any value)
  - If cap's value equals request's value → OK
  - Otherwise → NO MATCH
- If cap is missing this key → OK (missing = implicit `*`, cap can handle any value)

**Step 2: Check all tags in the CAP that request doesn't have**
For each `(key, value)` in cap.tags where request doesn't have this key:
- This is fine - the cap is just more specific than needed
- The request "doesn't care" about this dimension, so any value is acceptable

**Result:** If all checks pass, the cap matches the request.

### 14. Matching Examples

```
# Example 1: Basic matching
Cap:     cap:op=generate;ext=pdf
Request: cap:op=generate;ext=pdf
Result:  MATCH (exact match)

# Example 2: Cap can handle any ext
Cap:     cap:op=generate           (missing ext = implicit ext=*)
Request: cap:op=generate;ext=pdf
Result:  MATCH (cap can handle any ext, including pdf)

# Example 3: Cap is more specific than needed
Cap:     cap:op=generate;ext=pdf;version=2
Request: cap:op=generate;ext=pdf
Result:  MATCH (request doesn't care about version)

# Example 4: Request uses wildcard
Cap:     cap:op=generate;ext=pdf
Request: cap:op=generate;ext=*
Result:  MATCH (request accepts any ext, pdf is acceptable)

# Example 5: Value mismatch
Cap:     cap:op=generate;ext=pdf
Request: cap:op=generate;ext=docx
Result:  NO MATCH (pdf ≠ docx)

# Example 6: Fallback provider pattern
Cap:     cap:op=generate_thumbnail;out=media:type=binary;v=1   (no ext)
Request: cap:op=generate_thumbnail;out=media:type=binary;v=1;ext=wav
Result:  MATCH (cap can handle any ext including wav)
```

### 15. Specificity for Best Match Selection
When multiple caps match a request, select the one with highest specificity:
- Specificity = count of non-wildcard tags
- `cap:op=generate;ext=pdf` has specificity 2
- `cap:op=generate;ext=*` has specificity 1 (wildcard doesn't count)
- `cap:op=generate` has specificity 1
- Higher specificity wins

### 16. Provider Selection Algorithm (`find_best_cap_set`)
1. For each registered provider, check if `provider.cap.matches(request)`
2. Collect all matching providers
3. Select the one with highest specificity
4. If tie, implementation-defined (typically first registered wins)

### 17. Key Insight: Asymmetric Matching
The matching is intentionally asymmetric:
- A cap with FEWER tags (more general) can match requests with MORE tags
- A cap with MORE tags (more specific) can also match requests with FEWER tags (request doesn't constrain those dimensions)

This enables the fallback pattern:
- Specific providers: `cap:op=generate_thumbnail;ext=pdf` (only handles PDF)
- Fallback provider: `cap:op=generate_thumbnail` (handles any ext)
- Request for `ext=wav` → specific provider doesn't match, fallback does

### 18. Duplicate Keys
Duplicate keys in the same URN result in an error - last occurrence does not win.

### 19. UTF-8 Support
Full UTF-8 character support within the allowed character set restrictions.

### 20. Numeric Values
- Tag keys cannot be pure numeric
- Tag values can be pure numeric

### 21. Empty Cap URN
`cap:` with no tags is valid and means "matches all URNs" (universal matcher).

### 22. Length Restrictions
The only length restriction is that the URL `https://capns.org/{cap_urn}` must be a valid URL. This imposes practical limits based on URL length constraints (typically ~2000 characters).

### 23. Wildcard Restrictions
Asterisk (`*`) in tag keys is not valid. Asterisk is only valid in tag values to signify wildcard matching.

### 24. Colon Treatment
Forward slashes (`/`) and colons (`:`) are valid anywhere in tag components and treated as normal characters, except for the mandatory `cap:` prefix which is not part of the tag structure.

### 25. Quote Errors
- **Unterminated Quote:** A quoted value that starts with `"` but never closes is an error
- **Invalid Escape Sequence:** Inside a quoted value, `\` followed by anything other than `"` or `\` is an error
- Examples of errors:
  - `cap:key="unterminated` → UnterminatedQuote error
  - `cap:key="bad\n"` → InvalidEscapeSequence error (only `\"` and `\\` allowed)

### 26. Semantic Equivalence
- `cap:key=simple` and `cap:key="simple"` both parse to `{key: "simple"}` (lowercase)
- `cap:key="Simple"` parses to `{key: "Simple"}` (preserved) - NOT equal to unquoted
- The quoting information is not stored; serialization re-determines quoting based on value content

## Implementation Notes

- All implementations must normalize tag keys to lowercase
- All implementations must normalize unquoted values to lowercase
- All implementations must preserve case in quoted values
- All implementations must sort tags alphabetically in canonical output
- All implementations must handle trailing semicolons consistently
- All implementations must validate character restrictions identically
- All implementations must implement matching logic identically
- All implementations must reject duplicate keys with appropriate error messages
- All implementations must use state-machine parsing for quoted value support
- All implementations must implement smart quoting on serialization

## Error Codes (Consistent Across All Implementations)

| Code | Name | Description |
|------|------|-------------|
| 1 | InvalidFormat | Empty or malformed URN |
| 2 | EmptyTag | Empty key or value component |
| 3 | InvalidCharacter | Disallowed character in key/value |
| 4 | InvalidTagFormat | Tag not in key=value format |
| 5 | MissingCapPrefix | URN does not start with `cap:` |
| 6 | DuplicateKey | Same key appears twice |
| 7 | NumericKey | Key is purely numeric |
| 8 | UnterminatedQuote | Quoted value never closed |
| 9 | InvalidEscapeSequence | Invalid escape in quoted value |