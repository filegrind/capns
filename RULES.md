# Cap URN Rules

## Definitive specification for Cap URN format and behavior

### 1. Case Insensitivity
Cap URNs are case insensitive. All input is normalized to lowercase for storage and comparison.

### 2. Tag Order Independence  
The order of tags in the URN string does not matter. Tags are always sorted alphabetically by key in canonical form.

### 3. Mandatory Prefix
Cap URNs must always be preceded by `cap:` which is the signifier of a cap URN.

### 4. Tag Separator
Tags are separated by semicolons (`;`).

### 5. Trailing Semicolon Optional
Presence or absence of the final trailing semicolon does not matter. Both `cap:key=value` and `cap:key=value;` are equivalent.

### 6. Character Restrictions
- No spaces in cap URNs
- Allowed characters in tag keys and values: alphanumeric, dashes (`-`), underscores (`_`), slashes (`/`), colons (`:`), and asterisk (`*` in values only)
- Quote marks (`"`, `'`), hash (`#`), and other special characters are not accepted

### 7. Tag Structure
- Tag separator within a tag: `=` separates tag key from tag value
- Tag separator between tags: `;` separates key-value pairs
- After the initial `cap:` prefix, colons (`:`) are treated as normal characters, not separators

### 8. No Special Tags
No reserved tag names - anything goes for tag keys.

### 9. Canonical Form
The canonical form of caps is all lowercase, with tags sorted by keys alphabetically, and no final trailing semicolon.

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

## Implementation Notes

- All implementations must normalize input to lowercase
- All implementations must sort tags alphabetically in canonical output
- All implementations must handle trailing semicolons consistently
- All implementations must validate character restrictions identically
- All implementations must implement matching logic identically
- All implementations must reject duplicate keys with appropriate error messages