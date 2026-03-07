# Validation Rules

## 1. Overview

This document specifies all validation rules for the capdag system. All implementations MUST enforce these rules identically.

### Rule Categories

| Category | Rules | Scope |
|----------|-------|-------|
| Cap URN Rules | CU1-CU2 | URN structure |
| Cap Definition Rules | RULE1-RULE12 | Capability arguments |
| Media Spec Rules | MS1-MS3 | Media specifications |
| Cross-Validation Rules | XV1-XV5 | Reference integrity |

### Validation Order

1. Structural validation (JSON Schema)
2. Cap URN validation (CU1, CU2)
3. Cap definition rules (RULE1-RULE12)
4. Media spec rules (MS1-MS3)
5. Cross-validation (XV1-XV5)

---

## 2. Cap URN Rules

### CU1: Required Direction Specifiers

**Rule**: Cap URNs in **canonical form** MUST include `in` and `out` tags.

**Surface syntax vs canonical form**: Users may omit `in`/`out` in surface syntax. During normalization, missing tags default to `media:`. Validation applies to the canonical form.

**Canonical Form**:
```
cap:in="<media-urn>";out="<media-urn>";<other-tags>
```

**Examples**:
```
# Surface syntax → Canonical form
cap:op=transform  →  cap:in=media:;op=transform;out=media:   ✓ (normalized)

# Already canonical
cap:in="media:pdf";op=extract;out="media:object"             ✓
cap:in=media:;op=generate;out="media:text"                   ✓
```

**Error**: `Cap URN requires 'in' tag` / `Cap URN requires 'out' tag`

This error occurs only for malformed inputs that bypass normalization (e.g., programmatic construction without validation).

### CU2: Valid Media URN References

**Rule**: Direction specifier values (`in` and `out`) MUST be:
- A valid Media URN (starting with `media:`)
- Or the wildcard `*` (which normalizes to `media:`)

**Examples**:
```
cap:in="media:pdf;bytes";op=extract;out="media:object"   ✓
cap:in=*;op=convert;out=*                                ✓ (normalizes to media:)
cap:in="invalid";op=test;out="media:text"                ✗
```

**Error**: `Invalid 'in' media URN: <value>. Must start with 'media:' or be '*'`

---

## 3. Cap Definition Rules (RULE1-RULE12)

These rules validate the `args` array in capability definitions.

### RULE1: No Duplicate Media URNs

**Rule**: No two arguments may have the same `media_urn`.

**Rationale**: Each argument is uniquely identified by its media URN.

**Error**: `RULE1: Duplicate media_urn '<urn>'`

### RULE2: Sources Must Not Be Empty

**Rule**: Every argument MUST have a non-empty `sources` array.

**Rationale**: An argument without sources cannot receive input.

**Error**: `RULE2: Argument '<media_urn>' has empty sources`

### RULE3: Identical Stdin Media URNs

**Rule**: If multiple arguments have `stdin` sources, all stdin sources MUST specify identical `media_urn` values.

**Rationale**: There is only one stdin stream. Multiple args reading from stdin must expect the same media type.

**Error**: `RULE3: Multiple args have different stdin media_urns: '<urn1>' vs '<urn2>'`

### RULE4: No Duplicate Source Types Per Argument

**Rule**: No argument may specify the same source type (stdin, position, cli_flag) more than once.

**Rationale**: Each source type represents a single input channel per argument.

**Error**: `RULE4: Argument '<media_urn>' has duplicate source type '<type>'`

### RULE5: No Duplicate Positions

**Rule**: No two arguments may have the same positional index.

**Rationale**: Positional arguments must be unambiguous.

**Error**: `RULE5: Duplicate position <n> in argument '<media_urn>'`

### RULE6: Sequential Positions

**Rule**: Positions must be sequential starting from 0 with no gaps.

**Rationale**: Ensures predictable positional argument ordering.

**Error**: `RULE6: Position gap - expected <n> but found <m>`

### RULE7: No Position and CLI Flag Combination

**Rule**: No argument may have both a `position` source and a `cli_flag` source.

**Rationale**: An argument is either positional or named, not both.

**Error**: `RULE7: Argument '<media_urn>' has both position and cli_flag sources`

### RULE8: No Unknown Source Keys

**Rule**: Source objects may only contain recognized keys: `stdin`, `position`, or `cli_flag`.

**Rationale**: Prevents typos and invalid configurations.

**Enforcement**: Validated by JSON Schema with `deny_unknown_fields`.

**Error**: `RULE8: Argument '<media_urn>' has source with unknown keys`

### RULE9: No Duplicate CLI Flags

**Rule**: No two arguments may have the same `cli_flag` value.

**Rationale**: CLI flags must uniquely identify arguments.

**Error**: `RULE9: Duplicate cli_flag '<flag>' in argument '<media_urn>'`

### RULE10: Reserved CLI Flags

**Rule**: These CLI flags are reserved and cannot be used:
- `manifest`
- `--help`
- `--version`
- `-v`
- `-h`

**Rationale**: Reserved for system use.

**Error**: `RULE10: Argument '<media_urn>' uses reserved cli_flag '<flag>'`

### RULE11: CLI Flag Verbatim Usage

**Rule**: CLI flags are used exactly as specified (no automatic prefixing with `--`).

**Enforcement**: By design — implementations use the flag string verbatim.

### RULE12: Media URN as Key

**Rule**: Arguments are identified by `media_urn`, not a separate `name` field.

**Enforcement**: By schema — no `name` field allowed in argument definitions.

---

## 4. Media Spec Rules (MS1-MS3)

### MS1: Title Required

**Rule**: Every media spec MUST have a `title` field.

**Rationale**: Titles provide human-readable identification.

**Error**: `Media spec '<urn>' has no title`

### MS2: Valid URN Format

**Rule**: Media URNs MUST start with `media:` prefix.

**Rationale**: Distinguishes media specs from other URN types.

**Error**: `Invalid media URN: expected 'media:' prefix`

### MS3: Media Type Required

**Rule**: Every media spec MUST have a `media_type` field (MIME type).

**Rationale**: Specifies the content type for proper handling.

**Error**: `Media spec '<urn>' has no media_type`

---

## 5. Cross-Validation Rules (XV1-XV5)

### XV1: No Duplicate Cap URNs

**Rule**: No two capability definitions may have the same canonical Cap URN.

**Rationale**: Cap URNs uniquely identify capabilities.

**Error**: `Duplicate cap URN: <urn>`

### XV2: No Duplicate Media URNs (Global)

**Rule**: No two standalone media spec definitions may have the same URN.

**Rationale**: Media URNs uniquely identify media specs in the global registry.

**Error**: `Duplicate media URN: <urn>`

### XV3: Media URN Resolution Required

**Rule**: All media URNs referenced in capabilities MUST resolve to a defined media spec.

**Resolution Order**:
1. Capability's local `media_specs` table (inline definitions)
2. Standalone media spec files (global registry)

**No Fallbacks**: If a media URN cannot be resolved, validation FAILS. No auto-generation.

**Checked Locations**:
- The `in` spec from the URN string (unless wildcard)
- The `out` spec from the URN string (unless wildcard)
- Every `args[].media_urn`
- `output.media_urn`
- Every `args[].sources[].stdin` (media URN in stdin source)

**Error**: `Unresolved media URN '<urn>' referenced in <location>`

### XV4: Inline Media Spec Title Required

**Rule**: Media specs defined inline in capability `media_specs` tables MUST have a `title` field.

**Rationale**: Same as MS1 — all media specs need titles.

**Error**: `Inline media spec '<urn>' in <file> has no title`

### XV5: No Redefinition of Registry Media Specs

**Rule**: Inline media specs MUST NOT redefine media specs that exist in the global registry.

**Rationale**: The global registry is the canonical source. Redefining creates conflicts.

**Enforcement**:
- **With network**: Strictly enforce. Any conflict with registry = FAIL.
- **Without network**: Check cached specs only. Log warning if cannot verify.

**Error**: `XV5: Inline media spec '<urn>' redefines existing registry spec`

**Warning (offline)**: `XV5: Could not verify inline spec '<urn>' against online registry (offline mode)`

---

## 6. Implementation Requirements

### 6.1 Fail Hard

All validation errors MUST cause immediate failure with a clear error message.
- No fallbacks
- No silent recovery
- No default values for required fields

### 6.2 Consistent Behavior

All implementations (Rust, JavaScript, server functions) MUST enforce identical rules with identical error messages.

### 6.3 Error Codes

| Code Range | Category |
|------------|----------|
| CU1-CU2 | Cap URN Validation |
| RULE1-RULE12 | Cap Args Validation |
| MS1-MS3 | Media Spec Validation |
| XV1-XV5 | Cross-Validation |

---

## 7. Summary Table

| Rule | Description | Scope |
|------|-------------|-------|
| CU1 | Required in/out tags | URN |
| CU2 | Valid media URN values | URN |
| RULE1 | No duplicate media_urns | Args |
| RULE2 | Non-empty sources | Args |
| RULE3 | Identical stdin media_urns | Args |
| RULE4 | No duplicate source types | Args |
| RULE5 | No duplicate positions | Args |
| RULE6 | Sequential positions | Args |
| RULE7 | No position + cli_flag combo | Args |
| RULE8 | No unknown source keys | Args |
| RULE9 | No duplicate cli_flags | Args |
| RULE10 | Reserved cli_flags forbidden | Args |
| RULE11 | CLI flags verbatim | Args |
| RULE12 | media_urn as identifier | Args |
| MS1 | Title required | Media |
| MS2 | media: prefix required | Media |
| MS3 | media_type required | Media |
| XV1 | No duplicate cap URNs | Cross |
| XV2 | No duplicate media URNs | Cross |
| XV3 | All media URNs resolve | Cross |
| XV4 | Inline specs need title | Cross |
| XV5 | No registry redefinition | Cross |
