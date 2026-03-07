# Tagged URN Domain

## 1. Definition

Let **U** be the set of all normalized Tagged URNs.

A Tagged URN has the structure:
```
prefix:key1=value1;key2=value2;...
```

Each element `u ∈ U` has:
- A **prefix** (e.g., `media`, `cap`)
- Zero or more **tags** as key-value pairs
- A well-defined **canonical representation**

---

## 2. Syntax

### 2.1 Grammar

```
tagged-urn  ::= prefix ":" tags?
prefix      ::= identifier
tags        ::= tag (";" tag)*
tag         ::= key ("=" value)?
key         ::= identifier
value       ::= unquoted-value | quoted-value
identifier  ::= [a-z][a-z0-9-]*
```

### 2.2 Examples

```
media:                           # Identity (no tags)
media:pdf;bytes                  # Two tags with implicit * values
cap:in=media:pdf;op=extract;out=media:object
cap:in="media:pdf;bytes";op=extract;out="media:object"
```

---

## 3. Normalization

All Tagged URNs are normalized on parse using these rules:

### 3.1 Case Normalization

| Component | Rule |
|-----------|------|
| Prefix | Lowercase |
| Keys | Lowercase |
| Unquoted values | Lowercase |
| Quoted values | **Preserve case** |

### 3.2 Value-less Tags

A tag without `=` is treated as having value `*`:
```
Input:  media:pdf;bytes
Parsed: media:pdf=*;bytes=*
```

When serializing, `*` values serialize back to value-less form:
```
Internal: {pdf: "*", bytes: "*"}
Output:   media:pdf;bytes
```

### 3.3 Tag Ordering

Tags are stored in a sorted map (BTreeMap) and serialized in **alphabetical key order**:
```
Input:  media:bytes;pdf
Output: media:bytes;pdf      # 'bytes' before 'pdf' alphabetically
```

### 3.4 Quoting Rules

A value requires quoting if it contains: `;`, `=`, `"`, `\`, space, or uppercase characters.

Inside quotes:
- `"` is escaped as `\"`
- `\` is escaped as `\\`
- All other characters are literal

---

## 4. Special Values

Tagged URNs support four special value forms:

| Value | Name | Meaning |
|-------|------|---------|
| `*` | Must-have-any | Key must be present with any value |
| `?` | Unspecified | No constraint on this key |
| `!` | Must-not-have | Key must be absent |
| (missing) | No constraint | Same as `?` when used as pattern |

### 4.1 Semantics

- **`*` (must-have-any)**: When matching, the instance MUST have this key with some value. The specific value doesn't matter, but the key must exist.

- **`?` (unspecified)**: No constraint. The pattern doesn't care whether the instance has this key or what value it has.

- **`!` (must-not-have)**: When matching, the instance MUST NOT have this key. If the instance has this key with any value, matching fails.

- **(missing)**: On the pattern side, a missing key means "no constraint" (same as `?`). On the instance side, a missing key means the key is absent.

---

## 5. Wildcard Truth Table

This table defines matching between an instance and a pattern for a single key:

| Instance ↓ \ Pattern → | (missing) | K=? | K=! | K=* | K=v |
|------------------------|-----------|-----|-----|-----|-----|
| **(missing)** | ✓ | ✓ | ✓ | ✗ | ✗ |
| **K=?** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **K=!** | ✓ | ✓ | ✓ | ✗ | ✗ |
| **K=\*** | ✓ | ✓ | ✗ | ✓ | ✓ |
| **K=v** | ✓ | ✓ | ✗ | ✓ | v=v only |

### 5.1 Reading the Table

- **Row**: What the instance has for key K
- **Column**: What the pattern requires for key K
- **Cell**: ✓ = match, ✗ = no match

### 5.2 Key Rules

1. **Pattern missing or `?`**: Always matches (no constraint)
2. **Pattern `!`**: Matches only if instance is missing or `!`
3. **Pattern `*`**: Matches only if instance has a value (not missing, not `!`)
4. **Pattern `v`**: Matches only if instance has exactly `v` (or `*` which accepts any)
5. **Instance `?`**: Always matches (instance doesn't constrain)

### 5.3 Examples

```
# Pattern: media:pdf    Instance: media:pdf;bytes
# Pattern has: pdf=*    Instance has: pdf=*, bytes=*
# For key 'pdf': Instance=*, Pattern=* → ✓
# For key 'bytes': Instance=*, Pattern=(missing) → ✓
# Result: MATCH

# Pattern: media:pdf;!audio    Instance: media:pdf;audio=mp3
# For key 'pdf': Instance=*, Pattern=* → ✓
# For key 'audio': Instance=mp3, Pattern=! → ✗
# Result: NO MATCH (instance has audio, pattern forbids it)

# Pattern: media:*    Instance: media:
# For key (any): Pattern requires *, but Instance has nothing
# Result: NO MATCH (pattern requires at least one tag)
```

---

## 6. The Base Relation ⪯

Define a binary relation on Tagged URNs:

```
a ⪯ b   iff   a is at least as specific as b
```

Equivalently:
- `a` refines `b`
- `b` accepts `a`
- `a` is lower in the partial order than `b`

### 6.1 Operational Definition

`a ⪯ b` holds iff for every key K:
- If `b` has a constraint on K, then `a` satisfies that constraint per the truth table
- `a` may have additional keys that `b` doesn't mention

### 6.2 Properties

The relation ⪯ is:
- **Reflexive**: `∀u ∈ U, u ⪯ u`
- **Transitive**: `(a ⪯ b ∧ b ⪯ c) ⟹ a ⪯ c`
- **Antisymmetric (modulo normalization)**: `(a ⪯ b ∧ b ⪯ a) ⟹ a ≡ b`

This makes ⪯ a **partial order** on normalized URNs.

---

## 7. Identity Elements

### 7.1 Media Identity

The Media URN identity is:
```
media:
```

This URN:
- Has no tags
- Accepts any Media URN (top of the partial order)
- Has specificity 0

**Reflexivity**: `media: ⪯ media:` holds (identity accepts itself).

### 7.2 Tagged URN Identity

For any prefix P, the identity is:
```
P:
```

A URN with no tags accepts any URN with the same prefix.

---

## 8. Partial Order Structure

Tagged URNs with the same prefix form a **partial order** (specialization order):

```
                    media:                    (top - most generic)
                      |
              media:bytes
                /         \
       media:pdf          media:image
           |                   |
   media:pdf;v=2.0      media:image;png
           |
media:pdf;v=2.0;compressed              (more specific)
```

- **Top (⊤)**: The identity `prefix:` with no tags
- **Ordering**: More tags = more specific = lower in the order

Note: While this structure has lattice-like properties (bounded above by identity), we do not formally define join (⊔) or meet (⊓) operations. The system uses only the partial order relation ⪯.

---

## 9. Parse Errors

The following conditions produce parse errors:

| Condition | Error |
|-----------|-------|
| Missing prefix | `MissingPrefix` |
| Empty prefix | `EmptyPrefix` |
| Duplicate key | `DuplicateKey` |
| Numeric key | `NumericKey` |
| Empty value after `=` | `EmptyValue` |
| Unclosed quote | `UnclosedQuote` |
| Invalid escape sequence | `InvalidEscape` |

---

## 10. Summary

The Tagged URN domain U provides:

1. **Normalized representation** — Canonical string form via case/ordering rules
2. **Wildcard semantics** — Four special values with defined matching behavior
3. **Partial order ⪯** — Reflexive, transitive, antisymmetric relation
4. **Lattice structure** — Identity at top, specificity increasing downward
5. **Compositional matching** — Per-key matching via truth table

All higher-level constructs (predicates, Cap URNs, dispatch) build on this foundation.
