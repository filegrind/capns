# Validation Rules

This document specifies all validation rules enforced by capns implementations. All implementations (Rust, JavaScript, server-side) MUST enforce these rules identically. No fallbacks, no exceptions.

## Overview

Validation is organized into categories:
1. **Cap URN Rules** - Rules for cap URN format (CU1-CU2)
2. **Cap Definition Rules** - Rules for capability definitions (RULE1-RULE12)
3. **Media Spec Rules** - Rules for media specifications (MS1-MS3)
4. **Cross-Validation Rules** - Rules for reference integrity (XV1-XV4)

For base Tagged URN rules (case handling, tag ordering, wildcards, quoting, character restrictions, etc.), see the [Tagged URN RULES.md](https://github.com/filegrind/tagged-urn-rs/blob/main/docs/RULES.md).

---

## Cap URN Rules

Cap URNs extend Tagged URNs with capability-specific requirements.

### CU1: Required Direction Specifiers

Cap URNs **must** include `in` and `out` tags that specify input/output media types:

```
cap:in="media:void";op=generate;out="media:object"
cap:in="media:binary";op=extract;out="media:object";target=metadata
```

- `in` - The input media type (what the cap accepts)
- `out` - The output media type (what the cap produces)
- Values must be valid Media URNs (starting with `media:`) or wildcard `*`
- Caps without `in` and `out` are invalid

**Error**: `Cap URN requires 'in' tag` / `Cap URN requires 'out' tag`

### CU2: Valid Media URN References

Direction specifier values (`in` and `out`) must be:
- A valid Media URN: `media:<type>;v=<version>[;profile=<profile>]`
- Or a wildcard: `*`

Invalid direction specifier values cause parsing to fail.

**Error**: `Invalid 'in' media URN: <value>. Must start with 'media:' or be '*'`

### URL Length Constraint

The URL `https://capns.org/{cap_urn}` must be valid, imposing practical length limits (~2000 characters).

---

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

---

## Cap-Specific Error Codes

In addition to Tagged URN error codes:

| Code | Name | Description |
|------|------|-------------|
| 10 | MissingInSpec | Cap URN missing required `in` tag |
| 11 | MissingOutSpec | Cap URN missing required `out` tag |
| 12 | InvalidMediaUrn | Direction spec value is not a valid Media URN |

---

## Cap Definition Rules (RULE1-RULE12)

These rules validate the `args` array in capability definitions.

### RULE1: No Duplicate Media URNs

**Description**: No two arguments in a capability may have the same `media_urn`.

**Rationale**: Each argument is uniquely identified by its `media_urn`. Duplicates cause ambiguity.

**Error**: `RULE1: Duplicate media_urn '<urn>'`

### RULE2: Sources Must Not Be Empty

**Description**: Every argument MUST have a non-empty `sources` array.

**Rationale**: An argument without sources cannot receive input.

**Error**: `RULE2: Argument '<media_urn>' has empty sources`

### RULE3: Identical Stdin Media URNs

**Description**: If multiple arguments have `stdin` sources, all stdin sources MUST specify identical `media_urn` values.

**Rationale**: There is only one stdin stream. Multiple args reading from stdin must expect the same media type.

**Error**: `RULE3: Multiple args have different stdin media_urns: '<urn1>' vs '<urn2>'`

### RULE4: No Duplicate Source Types Per Argument

**Description**: No argument may specify the same source type (stdin, position, cli_flag) more than once.

**Rationale**: Each source type represents a single input channel per argument.

**Error**: `RULE4: Argument '<media_urn>' has duplicate source type '<type>'`

### RULE5: No Duplicate Positions

**Description**: No two arguments may have the same positional index.

**Rationale**: Positional arguments must be unambiguous.

**Error**: `RULE5: Duplicate position <n> in argument '<media_urn>'`

### RULE6: Sequential Positions

**Description**: Positions must be sequential starting from 0 with no gaps.

**Rationale**: Ensures predictable positional argument ordering.

**Error**: `RULE6: Position gap - expected <n> but found <m>`

### RULE7: No Position and CLI Flag Combination

**Description**: No argument may have both a `position` source and a `cli_flag` source.

**Rationale**: An argument is either positional or named, not both. This prevents ambiguity in argument parsing.

**Error**: `RULE7: Argument '<media_urn>' has both position and cli_flag sources`

### RULE8: No Unknown Source Keys

**Description**: Source objects may only contain recognized keys: `stdin`, `position`, or `cli_flag`.

**Rationale**: Prevents typos and invalid configurations.

**Enforcement**: Validated by JSON Schema and deserialization.

**Error**: `RULE8: Argument '<media_urn>' has source with unknown keys`

### RULE9: No Duplicate CLI Flags

**Description**: No two arguments may have the same `cli_flag` value.

**Rationale**: CLI flags must uniquely identify arguments.

**Error**: `RULE9: Duplicate cli_flag '<flag>' in argument '<media_urn>'`

### RULE10: Reserved CLI Flags

**Description**: Certain CLI flags are reserved and cannot be used: `manifest`, `--help`, `--version`, `-v`, `-h`

**Rationale**: Reserved for system use.

**Error**: `RULE10: Argument '<media_urn>' uses reserved cli_flag '<flag>'`

### RULE11: CLI Flag Verbatim Usage

**Description**: CLI flags are used exactly as specified (no automatic prefixing).

**Enforcement**: By design - implementations use the flag string verbatim.

### RULE12: Media URN as Key

**Description**: Arguments are identified by `media_urn`, not a separate `name` field.

**Enforcement**: By schema - no `name` field allowed in argument definitions.

---

## Media Spec Rules

### MS1: Title Required

**Description**: Every media spec MUST have a `title` field.

**Rationale**: Titles provide human-readable identification for display and documentation.

**Error**: `Media spec '<urn>' has no title`

### MS2: Valid URN Format

**Description**: Media URNs MUST start with `media:` prefix.

**Rationale**: Distinguishes media specs from other URN types.

**Error**: `Invalid media URN: expected 'media:' prefix`

### MS3: Media Type Required

**Description**: Every media spec MUST have a `media_type` field (MIME type).

**Rationale**: Specifies the content type for proper handling.

**Error**: `Media spec '<urn>' has no media_type`

---

## Cross-Validation Rules

### XV1: No Duplicate Cap URNs

**Description**: No two capability definitions may have the same canonical Cap URN.

**Rationale**: Cap URNs uniquely identify capabilities.

**Error**: `Duplicate cap URN: <urn>`

### XV2: No Duplicate Media URNs (Global)

**Description**: No two standalone media spec definitions may have the same URN.

**Rationale**: Media URNs uniquely identify media specs in the global registry.

**Error**: `Duplicate media URN: <urn>`

### XV3: Media URN Resolution Required

**Description**: All media URNs referenced in capabilities MUST resolve to a defined media spec.

**Resolution Order**:
1. Capability's local `media_specs` table (inline definitions)
2. Standalone media spec files (global registry)

**No Fallbacks**: If a media URN cannot be resolved, validation FAILS. No auto-generation of specs.

**Checked Locations**:
- The `in` spec from the URN string (unless wildcard `*`)
- The `out` spec from the URN string (unless wildcard `*`)
- Every `args[].media_urn`
- `output.media_urn`
- Every `args[].sources[].stdin` (media URN in stdin source)

**Error**: `Unresolved media URN '<urn>' referenced in <location>`

### XV4: Inline Media Spec Title Required

**Description**: Media specs defined inline in capability `media_specs` tables MUST also have a `title` field.

**Rationale**: Same as MS1 - all media specs need titles regardless of definition location.

**Error**: `Inline media spec '<urn>' in <file> has no title`

### XV5: No Redefinition of Registry Media Specs

**Description**: Inline media specs in a capability's `media_specs` table MUST NOT redefine media specs that already exist in the global media registry.

**Rationale**: The global registry is the canonical source for standard media specs. Redefining them inline creates confusion, inconsistency, and potential for conflicting definitions. If a media spec exists in the registry, capabilities should reference it, not redefine it.

**Enforcement Behavior**:
- **With network access**: Strictly enforce. If any inline media URN matches a URN in the global registry, validation FAILS.
- **Without network access**: Check against cached/bundled specs only. If a conflict is found with cached specs, validation FAILS. If no conflict is found with cached specs but online registry is unreachable, log a warning and allow the operation to proceed (graceful degradation).

**Error**: `XV5: Inline media spec '<urn>' redefines existing registry spec`

**Warning (offline mode)**: `XV5: Could not verify inline spec '<urn>' against online registry (offline mode)`

---

## Implementation Requirements

### Fail Hard

All validation errors MUST cause immediate failure with a clear error message. No fallbacks, no silent recovery, no default values for required fields.

### Consistent Behavior

All implementations (Rust capns, JavaScript capns-js, server functions) MUST enforce identical rules with identical error messages.

### Order of Validation

1. Structural validation (JSON Schema)
2. Cap URN validation (CU1, CU2)
3. Cap definition rules (RULE1-RULE12)
4. Media spec rules (MS1-MS3)
5. Cross-validation (XV1-XV4)

---

## Error Code Summary

| Code Range | Category |
|------------|----------|
| 10-12 | Cap URN Errors |
| RULE1-RULE12 | Cap Args Validation |
| MS1-MS3 | Media Spec Validation |
| CU1-CU2 | Cap URN Validation |
| XV1-XV4 | Cross-Validation |

---

## Implementation Notes

- All implementations must validate presence of `in` and `out` tags
- All implementations must validate Media URN format for direction specs
- All implementations must include direction specs in matching logic
- Direction specs use quoted values to preserve Media URN case and special characters

---

## Changelog

- 2026-01-29: Added XV5 (No Redefinition of Registry Media Specs) validation
  - Inline media specs must not redefine existing registry specs
  - Strict enforcement with network, graceful degradation without
- 2024-01-29: Added comprehensive validation rules documentation
- Added RULE1-RULE12 for cap args validation
- Added MS1-MS3 for media spec validation
- Added XV1-XV4 for cross-validation
- Added RULE7 enforcement for position/cli_flag mutual exclusivity
- Added MS1 (title required) validation
- Added XV3 strict media URN resolution (no fallbacks)
- Added XV4 inline media spec title requirement
