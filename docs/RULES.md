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
- When used as a tag value, `*` matches any value for that tag key

### 11. No Empty Components
Tags with no values are not accepted. Both key and value must be non-empty after trimming whitespace.

### 12. Matching Specificity
As more tags are specified, URNs become more specific:
- `cap:` matches any URN
- `cap:prop=*` matches any URN that has a `prop` tag with any value
- `cap:prop=1` matches only URNs that have `prop=1`, regardless of other tags

### 13. Exact Tag Matching
`cap:prop=1` matches only URNs that have `prop=1` irrespective of other tags present.

### 14. Subset Matching
Only the tags specified in the criteria affect matching. URNs with extra tags not mentioned in the criteria still match if they satisfy all specified criteria.

### 15. Duplicate Keys
Duplicate keys in the same URN result in an error - last occurrence does not win.

### 16. UTF-8 Support
Full UTF-8 character support within the allowed character set restrictions.

### 17. Numeric Values
- Tag keys cannot be pure numeric
- Tag values can be pure numeric

### 18. Empty Cap URN
`cap:` with no tags is valid and means "matches all URNs" (universal matcher).

### 19. Length Restrictions
The only length restriction is that the URL `https://capns.org/{cap_urn}` must be a valid URL. This imposes practical limits based on URL length constraints (typically ~2000 characters).

### 20. Wildcard Restrictions
Asterisk (`*`) in tag keys is not valid. Asterisk is only valid in tag values to signify wildcard matching.

### 21. Colon Treatment
Forward slashes (`/`) and colons (`:`) are valid anywhere in tag components and treated as normal characters, except for the mandatory `cap:` prefix which is not part of the tag structure.

### 22. Quote Errors
- **Unterminated Quote:** A quoted value that starts with `"` but never closes is an error
- **Invalid Escape Sequence:** Inside a quoted value, `\` followed by anything other than `"` or `\` is an error
- Examples of errors:
  - `cap:key="unterminated` → UnterminatedQuote error
  - `cap:key="bad\n"` → InvalidEscapeSequence error (only `\"` and `\\` allowed)

### 23. Semantic Equivalence
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