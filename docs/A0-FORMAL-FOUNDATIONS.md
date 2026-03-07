# Formal Foundations of URN Matching and Cap Dispatch

## 1. Purpose and Scope

This document provides the **formal mathematical foundation** for the capdag URN matching and dispatch system. It defines:

- The Tagged URN semantic domain
- The base relation and derived predicates
- The Cap URN product construction
- The dispatch relation
- Ranking as a separate policy layer
- Proof obligations and sanity properties

This is the authoritative formal reference. Implementation details are in the numbered specification documents (01-11).

---

## 2. The Core Problem

Capdag uses URNs as structured semantic descriptors, not merely names. The system must answer distinct questions:

| Question | Predicate |
|----------|-----------|
| Can this provider handle this request? | Dispatch |
| Are these on the same specialization chain? | is_comparable |
| Are these semantically identical? | is_equivalent |
| Can output flow to input? | conforms_to |

Previous bugs arose from conflating these questions. A single `accepts` or `conforms_to` cannot serve all purposes because Cap URNs have **mixed variance** across three dimensions.

---

## 3. Base Domain

Let **U** be the set of normalized Tagged URNs.

Each element u ∈ U has:
- A canonical normalized representation
- Well-defined wildcard/value semantics
- A specificity score
- A base semantic relation

---

## 4. Primitive Relation

Define a binary relation on U:

```
⪯ ⊆ U × U
```

With meaning:

```
a ⪯ b  iff  a is at least as specific as b
```

Equivalently: b is at least as general as a.

**Lattice orientation**:
- Lower = more specific (more constrained)
- Upper = more generic (less constrained)

---

## 5. Derived Predicates

For a, b ∈ U:

### 5.1 Acceptance

```
accepts(a, b)  :⟺  b ⪯ a
```

Meaning: a subsumes b. Pattern a accepts instance b.

### 5.2 Conformance

```
conforms_to(a, b)  :⟺  a ⪯ b
```

Meaning: a satisfies or refines b. Instance a conforms to pattern b.

### 5.3 Comparability

```
is_comparable(a, b)  :⟺  a ⪯ b ∨ b ⪯ a
```

Meaning: One subsumes the other. They are on the same specialization chain.

### 5.4 Equivalence

```
is_equivalent(a, b)  :⟺  a ⪯ b ∧ b ⪯ a
```

Meaning: a and b denote the same semantic position.

---

## 6. Specificity Function

Define:

```
spec_U : U → ℕ
```

Such that greater specificity yields a greater score.

This function is used for **ranking only**, not for primary semantic validity.

---

## 7. Semantic Obligations on U

The system assumes U, ⪯, and spec_U are well-defined by the Tagged URN truth table.

### 7.1 Reflexivity

```
∀u ∈ U,  u ⪯ u
```

### 7.2 Transitivity

```
∀a,b,c ∈ U,  (a ⪯ b ∧ b ⪯ c) ⟹ a ⪯ c
```

If these hold, ⪯ is a **preorder** on U.

### 7.3 Antisymmetry (modulo normalization)

```
(a ⪯ b ∧ b ⪯ a) ⟹ a ≡ b
```

If this also holds, equivalence classes of U form a **partial order**.

---

## 8. Cap URNs as Product

A Cap URN is a triple over the Tagged URN domain:

```
C = U × U × U
```

For c ∈ C, write:

```
c = (i, o, y)
```

Where:
- **i** = input dimension (the `in` tag value)
- **o** = output dimension (the `out` tag value)
- **y** = non-direction cap-tag dimension

All three dimensions reuse the same base domain U and relation ⪯.

---

## 9. Cap Specificity

Define:

```
spec_C : C → ℕ
```

By:

```
spec_C(i, o, y) = media_tags(i) + media_tags(o) + count_non_wildcard(y)
```

Where:
- `media_tags(x)` = number of tags in media URN x (0 if x = identity "media:")
- `count_non_wildcard(y)` = number of y-tags with non-`*` values

This differs from a naive `spec_U(i) + spec_U(o) + spec_U(y)` because direction specs are Media URNs (counted by tag presence) while y-tags use binary wildcard/non-wildcard distinction.

This is a derived scalar used only for ranking among already valid candidates. See [03-SPECIFICITY](./03-SPECIFICITY.md) for full details.

---

## 10. The Dispatch Relation

Let:
- provider p = (i_p, o_p, y_p)
- request r = (i_r, o_r, y_r)

Define the dispatch relation:

```
Dispatch(p, r)  ⟺  i_r ⪯ i_p  ∧  o_p ⪯ o_r  ∧  y_r ⪯ y_p
```

This is the **primary routing predicate**.

---

## 11. Interpretation of Dispatch

### 11.1 Input Admissibility (Contravariant)

```
i_r ⪯ i_p
```

The request's input must be at least as specific as the provider's accepted input.

**Type-theoretic view**: Function parameters are contravariant. Provider may accept more general inputs.

### 11.2 Output Admissibility (Covariant)

```
o_p ⪯ o_r
```

The provider's output must be at least as specific as the request's required output.

**Type-theoretic view**: Function returns are covariant. Provider must guarantee at least what request demands.

### 11.3 Behavioral Refinement (Invariant + Refinement)

```
y_r ⪯ y_p
```

The provider's non-direction tags must satisfy or refine the request's constraints.

Provider may add tags (refinement) but cannot contradict explicit request constraints.

---

## 12. Variance Summary

| Dimension | Variance | Condition | Meaning |
|-----------|----------|-----------|---------|
| Input (i) | Contravariant | i_r ⪯ i_p | Provider may accept broader input |
| Output (o) | Covariant | o_p ⪯ o_r | Provider must produce tighter output |
| Cap-tags (y) | Invariant/Refinement | y_r ⪯ y_p | Provider must satisfy constraints |

---

## 13. Dispatch Is Directional

In general:

```
Dispatch(p, r)  ⟹̸  Dispatch(r, p)
```

Dispatch is **not symmetric**. This is intentional.

A specific provider can handle a generic request, but a generic request cannot "handle" a specific provider.

---

## 14. Derived Cap-Level Relations

Using base predicates componentwise:

### 14.1 Cap Equivalence

```
CapEq(c₁, c₂)  ⟺  is_equivalent(i₁, i₂) ∧ is_equivalent(o₁, o₂) ∧ is_equivalent(y₁, y₂)
```

### 14.2 Cap Comparability

```
CapComparable(c₁, c₂)  ⟺  is_comparable(i₁, i₂) ∧ is_comparable(o₁, o₂) ∧ is_comparable(y₁, y₂)
```

Useful for discovery, **not sufficient for dispatch**.

---

## 15. Correct Predicate Roles

| Purpose | Predicate |
|---------|-----------|
| Routing / execution | Dispatch(provider, request) |
| Exact lookup | CapEq(a, b) |
| Discovery / family analysis | CapComparable(a, b) |
| Pattern-instance checks | accepts, conforms_to |

---

## 16. Ranking Policy

Ranking is defined only over the valid set:

```
Valid(r) = { p ∈ C | Dispatch(p, r) }
```

A ranking policy is any total preorder on Valid(r).

### 16.1 Specificity Distance

```
dist(p, r) = spec_C(p) - spec_C(r)
```

### 16.2 Typical Preference

1. dist = 0 (equivalent) — most preferred
2. Smallest positive dist (refinement)
3. Negative dist only as fallback (generic provider)

This is **policy, not semantics**. Dispatch defines validity; ranking defines selection.

---

## 17. Fundamental Sanity Properties

### 17.1 Reflexivity of Dispatch

```
∀c ∈ C,  Dispatch(c, c)
```

Follows from reflexivity of ⪯.

### 17.2 Transitivity of Dispatch

```
∀a,b,c ∈ C,  (Dispatch(a, b) ∧ Dispatch(b, c)) ⟹ Dispatch(a, c)
```

Follows from transitivity of ⪯.

### 17.3 Monotonicity of Provider Refinement

If provider p' refines p:
- i_p ⪯ i_p' (more permissive input)
- o_p' ⪯ o_p (more specific output)
- y_p ⪯ y_p' (more specific y-tags)

And Dispatch(p, r) holds, then:

```
Dispatch(p', r)
```

Refinement preserves dispatchability.

### 17.4 Contradiction Rejection

```
¬(i_r ⪯ i_p) ∨ ¬(o_p ⪯ o_r) ∨ ¬(y_r ⪯ y_p)  ⟹  ¬Dispatch(p, r)
```

If any conjunct fails, dispatch fails.

---

## 18. Common Failure Modes

### 18.1 Using `accepts` for dispatch

Wrong: Flattens 3-dimensional mixed-variance problem into one direction.

### 18.2 Using `conforms_to` for dispatch

Wrong: Same reason.

### 18.3 Using `is_comparable` for dispatch

Wrong: Relatedness is not validity. Two URNs can be comparable but not dispatchable.

### 18.4 Ranking before validity

Wrong: Specificity alone does not imply semantic legality.

### 18.5 Treating Cap URNs as one-dimensional

Wrong: Cap URNs are structured product objects with mixed variance.

---

## 19. Separation of Validity and Preference

The specification distinguishes:

- **Semantic validity**: Whether a provider may legally serve a request (Dispatch)
- **Selection preference**: Which valid provider should be chosen (Ranking)

These must not be conflated.

---

## 20. Implementation Conformance

Any implementation conforming to this specification must ensure:

1. All Tagged URNs normalize into U
2. All dimension-level checks reduce to the same base relation ⪯
3. Cap dispatch uses exactly the mixed-direction rule (Section 10)
4. Ranking is applied only after dispatch validity is established

---

## 21. Summary

The entire system is defined from a single semantic base:

- A Tagged URN domain U
- A specificity relation ⪯
- A specificity score spec_U

Cap URNs are triples in U³.

Dispatch is the mixed-direction product relation:

```
Dispatch((i_p, o_p, y_p), (i_r, o_r, y_r))
  ⟺  i_r ⪯ i_p ∧ o_p ⪯ o_r ∧ y_r ⪯ y_p
```

This yields:
- A clean order-theoretic interpretation
- A clean type-theoretic interpretation
- Correct separation of routing, planning, discovery, and exact matching
- A foundation strong enough for formal verification
