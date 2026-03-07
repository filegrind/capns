# URN Matching and Dispatch Specification

## Scope

This specification defines the **semantic foundations** for URN matching and capability dispatch in the capdag system. It covers:

- Tagged URN domain and matching semantics
- Derived predicates (accepts, conforms_to, is_comparable, is_equivalent)
- Specificity scoring
- Cap URN product structure
- The dispatch relation
- Ranking policy
- Validation rules

**Out of scope**: Runtime protocols (Bifaci), hosting architecture, execution engines, HTTP APIs, language-specific implementations.

---

## Document Map

| Document | Purpose | Dependencies |
|----------|---------|--------------|
| [A0-FORMAL-FOUNDATIONS](./A0-FORMAL-FOUNDATIONS.md) | Mathematical foundation, dispatch relation | None |
| [01-TAGGED-URN-DOMAIN](./01-TAGGED-URN-DOMAIN.md) | Base domain U, normalization, wildcard truth table | A0 |
| [02-PREDICATES](./02-PREDICATES.md) | Derived predicates from base relation | 01 |
| [03-SPECIFICITY](./03-SPECIFICITY.md) | Specificity scoring function | 01 |
| [04-CAP-URN-STRUCTURE](./04-CAP-URN-STRUCTURE.md) | Cap URN as product C = U × U × U | 01, 03 |
| [05-DISPATCH](./05-DISPATCH.md) | The dispatch predicate | 01, 02, 04 |
| [06-RANKING](./06-RANKING.md) | Selection among valid providers | 03, 05 |
| [10-VALIDATION-RULES](./10-VALIDATION-RULES.md) | Structural validation rules | 01, 04 |
| [11-MEDIA-URNS](./11-MEDIA-URNS.md) | Media URN structure and coercion | 01 |

### Reading Order

1. **A0-FORMAL-FOUNDATIONS** — Mathematical foundation (optional, for formal reference)
2. **01-TAGGED-URN-DOMAIN** — Understand the base domain
3. **02-PREDICATES** — Learn the four derived predicates
4. **03-SPECIFICITY** — Understand scoring
5. **04-CAP-URN-STRUCTURE** — See how Cap URNs compose three dimensions
6. **05-DISPATCH** — The central routing rule
7. **06-RANKING** — How to choose among valid providers
8. **10-VALIDATION-RULES** — Structural constraints
9. **11-MEDIA-URNS** — Media type details

---

## Terminology

| Term | Definition |
|------|------------|
| **Tagged URN** | A URN with structure `prefix:key1=value1;key2=value2;...` |
| **Media URN** | A Tagged URN with prefix `media:` describing a data type |
| **Cap URN** | A Tagged URN with prefix `cap:` describing a capability |
| **Pattern** | A URN used as a template or constraint |
| **Instance** | A URN representing a concrete value or request |
| **Provider** | A registered capability that can handle requests |
| **Request** | A capability URN describing what is needed |
| **Dispatch** | The act of routing a request to a valid provider |
| **Specificity** | A numeric score measuring how constrained a URN is |
| **Wildcard** | A special value (`*`, `?`, `!`) with matching semantics |

---

## Notational Conventions

### Order-Theoretic Notation

- `a ⪯ b` — "a is at least as specific as b" (a refines b)
- `a ⪰ b` — "a is at least as general as b" (equivalent to b ⪯ a)

### Cap URN Components

A Cap URN `c` is written as a triple:
```
c = (i, o, y)
```
where:
- `i` = input media URN (the `in` tag value)
- `o` = output media URN (the `out` tag value)
- `y` = non-direction cap-tags (op, ext, model, etc.)

### Subscript Notation

- `i_p` = input component of provider
- `i_r` = input component of request
- `o_p`, `o_r`, `y_p`, `y_r` similarly

---

## Foundation

This specification builds on the mathematical foundations established in:

- [A0-FORMAL-FOUNDATIONS](./A0-FORMAL-FOUNDATIONS.md) — Formal mathematical specification

That document defines the core dispatch relation:

```
Dispatch(p, r) ⟺ i_r ⪯ i_p ∧ o_p ⪯ o_r ∧ y_r ⪯ y_p
```

The numbered documents (01-11) fill in operational details: wildcard truth tables, normalization rules, specificity scoring, and validation constraints.

---

## Conformance

An implementation conforms to this specification if:

1. All Tagged URNs normalize identically (per 01-TAGGED-URN-DOMAIN)
2. All predicates compute identically (per 02-PREDICATES)
3. Specificity scores match (per 03-SPECIFICITY)
4. Dispatch validity matches (per 05-DISPATCH)
5. All validation rules are enforced (per 10-VALIDATION-RULES)

Ranking policy (06-RANKING) may vary by subsystem, but dispatch validity must not.
