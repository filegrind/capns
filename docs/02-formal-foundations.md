---
title: "Formal Foundations"
layout: doc
permalink: /02-formal-foundations/
---
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

Previous bugs arose from conflating these questions. A single `accepts` or `conforms_to` cannot serve all purposes because Cap URNs have **mixed variance** across four structural coordinates.

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

A Cap URN is a four-coordinate product over the Tagged URN domain:

```
C = U × U × U × E
```

For c ∈ C, write:

```
c = (i, o, y, e)
```

Where:
- **i** = input dimension (the `in` tag value)
- **o** = output dimension (the `out` tag value)
- **y** = non-direction cap-tag dimension
- **e** = effect coordinate

The `in`, `out`, and `y` dimensions reuse the base domain `U` and relation `⪯`. The `effect` coordinate is a structural axis with exact matching unless the request explicitly uses the unconstrained `?effect` form.

### 8.1 Distinguished Media URNs: Top and Unit

Two media URNs play distinguished roles in the directional axes of
C. They are formally distinct elements of U; the rest of the spec
relies on this distinction.

**Top (`media:`).** The maximum of the media URN order under ⪯:

```
∀ m ∈ MediaUrn,    m ⪯ media:
```

A directional spec set to `media:` is unconstrained on that axis: it
matches every more specific media URN. In type-theoretic terms, a
side typed `media:` is the **top type** — read as "any A."

**Unit (`media:void`).** A leaf carrying the marker tag `void`, and
**atomic** — the parser rejects any combination of `void` with
another tag:

```
media:void ∈ MediaUrn,    media:void ⪯ media:,    ¬(m ⪯ media:void) for any concrete m
```

A directional spec set to `media:void` is the **unit type** — read
as "()." It is *not* the top, *not* "invalid," *not* "absent." It is
a distinct first-class type whose meaning is "no payload flows on
this side."

The atomicity rule is structural: there is no lattice underneath the
unit. `media:void;warmup`, `media:void;heartbeat`,
`media:void;reason=foo` are **parse errors**, not refinements.
Distinctions about *why* or *how* a void slot is used belong on the
cap URN's non-directional `y` axis or in cap args, never on the
media URN itself. This forecloses an entire family of dispatch
ambiguity (different unit values? different effects? different
commands?) by refusing the syntax that would express them. See
[11-MEDIA-URNS §2.2](/docs/11-media-urns#22-mediavoid--the-unit-type).

The dispatch relation treats `media:void` exactly like any other
media URN. The fact that it admits a logical reading as the unit
type is a property the [CapKind](/docs/06-cap-urn-structure#4-cap-kinds)
classifier exploits to label caps; it never enters the dispatch
predicate as a special case.

### 8.2 Cap Kinds (Logical Layer)

The product C admits a five-way classification by inspecting `(i, o)`
and the emptiness of `y`:

| Kind        | `i`            | `o`            | `y`     | Reading      |
|-------------|----------------|----------------|---------|--------------|
| Identity    | `media:`       | `media:`       | empty   | A → A        |
| Source      | `media:void`   | not `media:void`| any    | () → B       |
| Sink        | not `media:void`| `media:void`  | any     | A → ()       |
| Effect      | `media:void`   | `media:void`   | any     | () → ()      |
| Transform   | otherwise                                       | A → B        |

This classification is **logical only**. The dispatch relation
defined in §10 does not branch on kind; the kind is a description of
the resulting morphism, not a routing dimension. See
[06-CAP-URN-STRUCTURE §4](/docs/06-cap-urn-structure#4-cap-kinds) for
the full taxonomy.

---

## 9. Cap Specificity

Define:

```
spec_C : C → ℕ
```

By:

```
spec_C(i, o, y, e) = 10_000 * spec_U(o)
                    +    100 * spec_U(i)
                    +          spec_U(y)
```

The `in`, `out`, and `y` axes go through the **same** Tagged URN specificity
function `spec_U` for the *per-axis* sum. Where the axes differ is
in their **weight** in the cap-URN total: two orders of magnitude
separate `out` from `in` from `y`, giving a single integer with
lexicographic priority `(out, in, y)`. This reflects routing
intent — producing different things is the largest semantic
difference between two caps; consuming different things is next;
y-axis metadata is least.

Per-tag scoring (the same six-form ladder applied uniformly across
every axis):

| Form         | Stored value | Score | Reading                            |
|--------------|--------------|------:|------------------------------------|
| missing/`?x` | (none)/`"?"` |     0 | no constraint                      |
| `x?=v`       | `"?=v"`      |     1 | absent OR (present and not v)      |
| `x` (=`x=*`) | `"*"`        |     2 | present with any value             |
| `x!=v`       | `"!=v"`      |     3 | present and not v                  |
| `x=v`        | `"v"`        |     4 | present and exactly v              |
| `!x`         | `"!"`        |     5 | absent (must-not-have)             |

This is a derived scalar used only for ranking among already valid
candidates. See [05-SPECIFICITY](/docs/05-specificity) for full
details and worked examples.

---

## 10. The Dispatch Relation

Let:
- provider p = (i_p, o_p, y_p, e_p)
- request r = (i_r, o_r, y_r, e_r)

Define the dispatch relation:

```
Dispatch(p, r)  ⟺  i_r ⪯ i_p  ∧  o_p ⪯ o_r  ∧  (e_r = ? ∨ e_p = e_r)  ∧  y_r ⪯ y_p
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
¬(i_r ⪯ i_p) ∨ ¬(o_p ⪯ o_r) ∨ ¬(e_r = ? ∨ e_p = e_r) ∨ ¬(y_r ⪯ y_p)  ⟹  ¬Dispatch(p, r)
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

Cap URNs are four-coordinate product elements in `U × U × U × E`.

Dispatch is the mixed-direction product relation:

```
Dispatch((i_p, o_p, y_p, e_p), (i_r, o_r, y_r, e_r))
  ⟺  i_r ⪯ i_p ∧ o_p ⪯ o_r ∧ (e_r = ? ∨ e_p = e_r) ∧ y_r ⪯ y_p
```

This yields:
- A clean order-theoretic interpretation
- A clean type-theoretic interpretation
- Correct separation of routing, planning, discovery, and exact matching
- A foundation strong enough for formal verification
