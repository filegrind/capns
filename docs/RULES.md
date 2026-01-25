# Cap URN Rules

## Overview

Cap URNs extend Tagged URNs with capability-specific requirements. This document covers **only** the cap-specific rules. For base Tagged URN rules (case handling, tag ordering, wildcards, quoting, character restrictions, etc.), see the [Tagged URN RULES.md](https://github.com/filegrind/tagged-urn-rs/blob/main/docs/RULES.md).

## Cap-Specific Rules

### 1. Required Direction Specifiers

Cap URNs **must** include `in` and `out` tags that specify input/output media types:

```
cap:in="media:void";op=generate;out="media:object"
cap:in="media:binary";op=extract;out="media:object";target=metadata
```

- `in` - The input media type (what the cap accepts)
- `out` - The output media type (what the cap produces)
- Values must be valid Media URNs (starting with `media:`) or wildcard `*`
- Caps without `in` and `out` are invalid

### 2. No Value-less Tags

Unlike base Tagged URNs which allow value-less tags (e.g., `cap:optimize` as shorthand for `cap:optimize=*`), Cap URNs require explicit `key=value` format for all tags. This ensures clarity about capability specifications.

### 3. Media URN Validation

Direction specifier values (`in` and `out`) must be:
- A valid Media URN: `media:<type>;v=<version>[;profile=<profile>]`
- Or a wildcard: `*`

Invalid direction specifier values cause parsing to fail.

### 4. URL Length Constraint

The URL `https://capns.org/{cap_urn}` must be valid, imposing practical length limits (~2000 characters).

## Matching Semantics

Cap matching follows Tagged URN matching semantics. See [MATCHING.md](./MATCHING.md) for full details.

### Per-Tag Value Semantics

| Pattern Value | Meaning | Instance Missing | Instance=v | Instance=x≠v |
|---------------|---------|------------------|------------|--------------|
| (missing) | No constraint | OK | OK | OK |
| `K=?` | No constraint (explicit) | OK | OK | OK |
| `K=!` | Must-not-have | OK | NO | NO |
| `K=*` | Must-have, any value | NO | OK | OK |
| `K=v` | Must-have, exact value | NO | OK | NO |

### Direction Specifier Matching

Direction specs (`in`/`out`) follow standard tag matching:

```
# Exact match
Cap:     cap:in="media:binary";op=extract;out="media:object"
Request: cap:in="media:binary";op=extract;out="media:object"
Result:  MATCH

# Direction mismatch
Cap:     cap:in="media:binary";op=extract;out="media:object"
Request: cap:in="media:text";op=extract;out="media:object"
Result:  NO MATCH (input types differ)
```

### Must-Have-Any Direction Specs

`*` in direction specs means "must have any value":

```
Cap:     cap:in=*;op=convert;out=*
Request: cap:in="media:binary";op=convert;out="media:text"
Result:  MATCH (request has in/out, cap accepts any values)

Cap:     cap:in=*;op=convert;out=*
Request: cap:op=convert
Result:  NO MATCH (cap requires in/out presence, request lacks them)
```

### Graded Specificity

Specificity uses graded scoring:
- Exact value (K=v): 3 points
- Must-have-any (K=*): 2 points
- Must-not-have (K=!): 1 point
- Unspecified (K=?) or missing: 0 points

Examples:
- `cap:in="media:binary";op=extract;out="media:object"` → 3+3+3 = 9
- `cap:in=*;op=extract;out=*` → 2+3+2 = 7

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
