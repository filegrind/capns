# Derived Predicates

## 1. Foundation

All predicates are derived from the base relation ⪯ defined in [01-TAGGED-URN-DOMAIN](./01-TAGGED-URN-DOMAIN.md):

```
a ⪯ b   iff   a is at least as specific as b
```

This document defines four predicates that provide different views of this relation.

---

## 2. The Four Predicates

### 2.1 accepts(a, b)

**Definition**:
```
accepts(a, b)  :⟺  b ⪯ a
```

**Meaning**: `a` subsumes `b`. The URN `a` (as a pattern) accepts `b` (as an instance).

**Order-theoretic**: `a` is above or equal to `b` in the partial order.

**Plain English**: "Is `a` at least as general as `b`?"

**Examples**:
```
accepts(media:bytes, media:pdf;bytes)     = true   # bytes accepts pdf;bytes
accepts(media:pdf;bytes, media:bytes)     = false  # pdf;bytes does not accept bytes
accepts(media:pdf, media:pdf)             = true   # identical
```

---

### 2.2 conforms_to(a, b)

**Definition**:
```
conforms_to(a, b)  :⟺  a ⪯ b
```

**Meaning**: `a` satisfies or refines `b`. The URN `a` (as an instance) conforms to `b` (as a pattern).

**Order-theoretic**: `a` is below or equal to `b` in the partial order.

**Equivalent to**: `accepts(b, a)`

**Plain English**: "Is `a` at least as specific as `b`?"

**Examples**:
```
conforms_to(media:pdf;bytes, media:bytes)     = true   # pdf;bytes conforms to bytes
conforms_to(media:bytes, media:pdf;bytes)     = false  # bytes does not conform to pdf;bytes
conforms_to(media:pdf, media:pdf)             = true   # identical
```

---

### 2.3 is_comparable(a, b)

**Definition**:
```
is_comparable(a, b)  :⟺  a ⪯ b  ∨  b ⪯ a
```

**Meaning**: `a` and `b` are on the same specialization chain. One subsumes the other.

**Order-theoretic**: `a` and `b` are comparable in the partial order (one refines the other).

**Equivalent to**: `accepts(a, b) ∨ accepts(b, a)`

**Plain English**: "Are these related by specialization in either direction?"

**Examples**:
```
is_comparable(media:bytes, media:pdf;bytes)   = true   # same chain
is_comparable(media:pdf, media:image)         = false  # different branches
is_comparable(media:pdf, media:pdf)           = true   # identical
```

**Note**: Comparability is necessary but NOT sufficient for dispatch. Two URNs can be comparable but not dispatchable.

---

### 2.4 is_equivalent(a, b)

**Definition**:
```
is_equivalent(a, b)  :⟺  a ⪯ b  ∧  b ⪯ a
```

**Meaning**: `a` and `b` are at the same position in the partial order. They are semantically identical.

**Order-theoretic**: `a` and `b` are in the same equivalence class.

**Equivalent to**: `accepts(a, b) ∧ accepts(b, a)`

**Plain English**: "Are these semantically identical?"

**Examples**:
```
is_equivalent(media:pdf, media:pdf)           = true
is_equivalent(media:bytes;pdf, media:pdf;bytes) = true   # same tags, different order
is_equivalent(media:pdf, media:pdf;bytes)     = false  # different specificity
```

---

## 3. Relationship Summary

| Predicate | Definition | Symmetric? | Use Case |
|-----------|------------|------------|----------|
| `accepts(a,b)` | b ⪯ a | No | Pattern matching |
| `conforms_to(a,b)` | a ⪯ b | No | Instance checking |
| `is_comparable(a,b)` | a ⪯ b ∨ b ⪯ a | Yes | Discovery, grouping |
| `is_equivalent(a,b)` | a ⪯ b ∧ b ⪯ a | Yes | Exact identity |

### 3.1 Implications

```
is_equivalent(a,b)  ⟹  is_comparable(a,b)
is_equivalent(a,b)  ⟹  accepts(a,b) ∧ accepts(b,a)
is_equivalent(a,b)  ⟹  conforms_to(a,b) ∧ conforms_to(b,a)
```

But NOT:
```
is_comparable(a,b)  ⟹̸  is_equivalent(a,b)
accepts(a,b)        ⟹̸  conforms_to(a,b)
```

---

## 4. Usage Guidelines

### 4.1 Use `accepts` when:
- Checking if a pattern matches an instance
- The first argument is the pattern/template
- You're asking "does this accept that?"

```rust
if pattern.accepts(&instance) {
    // instance satisfies pattern
}
```

### 4.2 Use `conforms_to` when:
- Checking if an instance satisfies a requirement
- The first argument is the instance/value
- You're asking "does this conform to that?"

```rust
if instance.conforms_to(&requirement) {
    // instance meets requirement
}
```

### 4.3 Use `is_comparable` when:
- Exploring related capabilities
- Building discovery/search features
- Grouping URNs by family
- Diagnostics and debugging

```rust
if urn_a.is_comparable(&urn_b) {
    // they're on the same specialization chain
}
```

### 4.4 Use `is_equivalent` when:
- Exact lookup in a registry
- Deduplication
- Verifying identity
- Checking if two URNs represent the same thing

```rust
if urn_a.is_equivalent(&urn_b) {
    // they are semantically identical
}
```

---

## 5. Common Mistakes

### 5.1 Using `accepts` for dispatch

**Wrong**:
```rust
if registered.accepts(&request) { /* route here */ }
```

**Problem**: A generic provider accepts a specific request, but may not meet the request's output requirements. Dispatch requires the 3-axis check (see [05-DISPATCH](./05-DISPATCH.md)).

### 5.2 Using `conforms_to` for dispatch

**Wrong**:
```rust
if registered.conforms_to(&request) { /* route here */ }
```

**Problem**: A specific provider conforms to a generic request, but the request may not provide what the provider needs as input.

### 5.3 Using `is_comparable` for dispatch

**Wrong**:
```rust
if registered.is_comparable(&request) { /* route here */ }
```

**Problem**: Being on the same chain doesn't mean the provider can handle the request. They might be comparable but in the wrong direction.

### 5.4 Confusing direction

**Wrong**:
```rust
// Trying to check if instance satisfies pattern
if pattern.conforms_to(&instance) { ... }  // BACKWARDS!
```

**Correct**:
```rust
if instance.conforms_to(&pattern) { ... }
// OR equivalently:
if pattern.accepts(&instance) { ... }
```

---

## 6. Algebraic Properties

### 6.1 accepts

- **Not symmetric**: `accepts(a,b) ⟹̸ accepts(b,a)`
- **Reflexive**: `accepts(a,a)` always holds
- **Transitive**: `accepts(a,b) ∧ accepts(b,c) ⟹ accepts(a,c)`

### 6.2 conforms_to

- **Not symmetric**: `conforms_to(a,b) ⟹̸ conforms_to(b,a)`
- **Reflexive**: `conforms_to(a,a)` always holds
- **Transitive**: `conforms_to(a,b) ∧ conforms_to(b,c) ⟹ conforms_to(a,c)`

### 6.3 is_comparable

- **Symmetric**: `is_comparable(a,b) ⟺ is_comparable(b,a)`
- **Reflexive**: `is_comparable(a,a)` always holds
- **Not transitive**: `is_comparable(a,b) ∧ is_comparable(b,c) ⟹̸ is_comparable(a,c)`

### 6.4 is_equivalent

- **Symmetric**: `is_equivalent(a,b) ⟺ is_equivalent(b,a)`
- **Reflexive**: `is_equivalent(a,a)` always holds
- **Transitive**: `is_equivalent(a,b) ∧ is_equivalent(b,c) ⟹ is_equivalent(a,c)`

This makes `is_equivalent` an **equivalence relation**.

---

## 7. Summary

The four predicates provide complementary views of the base relation ⪯:

| Question | Predicate |
|----------|-----------|
| "Does pattern accept instance?" | `accepts(pattern, instance)` |
| "Does instance satisfy pattern?" | `conforms_to(instance, pattern)` |
| "Are these related?" | `is_comparable(a, b)` |
| "Are these identical?" | `is_equivalent(a, b)` |

For Cap URN dispatch, none of these alone is sufficient. See [05-DISPATCH](./05-DISPATCH.md) for the correct routing predicate.
