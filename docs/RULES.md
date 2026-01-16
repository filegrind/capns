# Cap URN Rules

## Overview

Cap URNs extend Tagged URNs with capability-specific requirements. This document covers **only** the cap-specific rules. For base Tagged URN rules (case handling, tag ordering, wildcards, quoting, character restrictions, etc.), see the [Tagged URN RULES.md](../../tagged-urn-rs/docs/RULES.md).

## Cap-Specific Rules

### 1. Required Direction Specifiers

Cap URNs **must** include `in` and `out` tags that specify input/output media types:

```
cap:in="media:type=void;v=1";op=generate;out="media:type=object;v=1"
cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1";target=metadata
```

- `in` - The input media type (what the cap accepts)
- `out` - The output media type (what the cap produces)
- Values must be valid Media URNs (starting with `media:`) or wildcard `*`
- Caps without `in` and `out` are invalid

### 2. No Value-less Tags

Unlike base Tagged URNs which allow value-less tags (e.g., `cap:optimize` as shorthand for `cap:optimize=*`), Cap URNs require explicit `key=value` format for all tags. This ensures clarity about capability specifications.

### 3. Media URN Validation

Direction specifier values (`in` and `out`) must be:
- A valid Media URN: `media:type=<type>;v=<version>[;profile=<profile>]`
- Or a wildcard: `*`

Invalid direction specifier values cause parsing to fail.

### 4. URL Length Constraint

The URL `https://capns.org/{cap_urn}` must be valid, imposing practical length limits (~2000 characters).

## Matching Semantics

Cap matching follows Tagged URN matching with additional direction-aware behavior.

### Direction Specs Are Always Part of Matching

Unlike regular tags where missing tags are implicit wildcards, direction specs (`in`/`out`) are **always** considered in matching:

```
# Both caps must have compatible in/out for a match
Cap:     cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
Request: cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
Result:  MATCH

# Direction mismatch prevents match
Cap:     cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"
Request: cap:in="media:type=text;v=1";op=extract;out="media:type=object;v=1"
Result:  NO MATCH (input types differ)
```

### Wildcard Direction Specs

Wildcard `*` in direction specs matches any media type:

```
Cap:     cap:in=*;op=convert;out=*
Request: cap:in="media:type=binary;v=1";op=convert;out="media:type=text;v=1"
Result:  MATCH (cap accepts any input/output)
```

### Specificity Calculation

Specificity for caps includes direction specs:
- Each non-wildcard direction spec contributes to specificity
- `cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"` has specificity 3
- `cap:in=*;op=extract;out=*` has specificity 1 (only `op` is specific)

## Cap-Specific Error Codes

In addition to Tagged URN error codes:

| Code | Name | Description |
|------|------|-------------|
| 10 | MissingInSpec | Cap URN missing required `in` tag |
| 11 | MissingOutSpec | Cap URN missing required `out` tag |
| 12 | InvalidMediaUrn | Direction spec value is not a valid Media URN |

## Implementation Notes

- All implementations must validate presence of `in` and `out` tags
- All implementations must validate Media URN format for direction specs
- All implementations must include direction specs in matching logic
- Direction specs use quoted values to preserve Media URN case and special characters
