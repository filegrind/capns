# Dispatch

## 1. The Central Question

Given:
- A **provider** `p` (a registered capability)
- A **request** `r` (what the caller wants)

The dispatch question is:

> Can provider `p` legally handle request `r`?

This is answered by the **dispatch predicate**.

---

## 2. The Dispatch Predicate

### 2.1 Definition

Let:
- `p = (i_p, o_p, y_p)` — provider
- `r = (i_r, o_r, y_r)` — request

Then:

```
Dispatch(p, r)  ⟺  i_r ⪯ i_p  ∧  o_p ⪯ o_r  ∧  y_r ⪯ y_p
```

### 2.2 The Three Conjuncts

| Axis | Condition | Variance | Meaning |
|------|-----------|----------|---------|
| Input | i_r ⪯ i_p | Contravariant | Request's input conforms to provider's accepted input |
| Output | o_p ⪯ o_r | Covariant | Provider's output conforms to request's required output |
| Cap-tags | y_r ⪯ y_p | Invariant/Refinement | Provider satisfies request's constraints |

---

## 3. Variance Interpretation

### 3.1 Input Axis (Contravariant)

```
i_r ⪯ i_p
```

**Meaning**: The provider may accept MORE input types than the request specifies.

**Type-theoretic**: Function parameter types are contravariant.

**Example**:
```
Request:  in="media:pdf;bytes"     (specific)
Provider: in="media:bytes"         (more general)

i_r = media:pdf;bytes
i_p = media:bytes

i_r ⪯ i_p? → Does pdf;bytes conform to bytes?
           → Yes, pdf;bytes is more specific than bytes
           → PASS ✓
```

A provider accepting `media:bytes` can handle a request sending `media:pdf;bytes`.

### 3.2 Output Axis (Covariant)

```
o_p ⪯ o_r
```

**Meaning**: The provider must produce AT LEAST as specific output as the request requires.

**Type-theoretic**: Function return types are covariant.

**Example**:
```
Request:  out="media:object"           (general requirement)
Provider: out="media:object;textable"  (more specific guarantee)

o_p = media:object;textable
o_r = media:object

o_p ⪯ o_r? → Does object;textable conform to object?
           → Yes, object;textable is more specific than object
           → PASS ✓
```

A provider guaranteeing `media:object;textable` satisfies a request needing `media:object`.

### 3.3 Cap-Tags Axis (Invariant for Explicit, Wildcard for Omitted)

```
y_r ⪯ y_p
```

**Meaning**: The provider must satisfy all explicit request constraints and may refine omitted ones.

**Example**:
```
Request:  op=extract                   (requires extract operation)
Provider: op=extract;target=metadata   (provides extract with refinement)

y_r = {op: "extract"}
y_p = {op: "extract", target: "metadata"}

y_r ⪯ y_p? → Does request conform to provider?
           → Request has op=extract, provider has op=extract → match
           → Request omits target, provider has target=metadata → OK (refinement)
           → PASS ✓
```

---

## 4. Dispatch Is NOT Symmetric

**Critical**: `Dispatch(p, r)` does NOT imply `Dispatch(r, p)`.

### 4.1 The Rule

The input condition `i_r ⪯ i_p` means:
- Request's input must be **at least as specific** as provider's input
- Equivalently: Provider's accepted input must **subsume** request's input

### 4.2 Why Asymmetry Matters

When request has `in=media:` (wildcard/identity):
- Request says "I might send anything"
- Provider with `in=media:model-spec` says "I only accept model-spec"
- Can provider handle this? **NO** — request might send non-model-spec
- `media: ⪯ media:model-spec` is FALSE (identity is NOT more specific)

When request has `in=media:model-spec`:
- Request says "I will send model-spec"
- Provider with `in=media:bytes` says "I accept any bytes"
- Can provider handle this? **YES** — model-spec conforms to bytes
- `media:model-spec ⪯ media:bytes` is TRUE

### 4.3 Wildcard Handling

For dispatch validity with wildcards:

| Request Input | Provider Input | Dispatch? | Reason |
|---------------|----------------|-----------|--------|
| `media:` | `media:` | ✓ | Both accept any |
| `media:` | `media:pdf` | ✗ | Request might send non-pdf |
| `media:pdf` | `media:` | ✓ | Provider accepts any, request sends pdf |
| `media:pdf` | `media:bytes` | ✓ | pdf conforms to bytes |
| `media:pdf` | `media:image` | ✗ | pdf does not conform to image |

---

## 5. Axis-by-Axis Rules

### 5.1 Input Axis

| Request In | Provider In | Dispatchable? | Reason |
|------------|-------------|---------------|--------|
| `media:` (any) | any | ✓ | Request doesn't constrain |
| specific | `media:` (any) | ✓ | Provider accepts any |
| specific | same | ✓ | Exact match |
| more specific | less specific | ✓ | Provider accepts broader class |
| less specific | more specific | ✗ | Request might send unsupported |
| incomparable | incomparable | ✗ | Different type families |

### 5.2 Output Axis

| Provider Out | Request Out | Dispatchable? | Reason |
|--------------|-------------|---------------|--------|
| any | `media:` (any) | ✓ | Request accepts any output |
| `media:` (any) | specific | ✗ | Provider might not produce required |
| specific | `media:` (any) | ✓ | Request accepts any |
| same | same | ✓ | Exact match |
| more specific | less specific | ✓ | Provider exceeds requirement |
| less specific | more specific | ✗ | Provider may not meet requirement |
| incomparable | incomparable | ✗ | Different type families |

### 5.3 Cap-Tags Axis

| Request Tag | Provider Tag | Dispatchable? | Reason |
|-------------|--------------|---------------|--------|
| missing | missing | ✓ | No constraint |
| missing | K=v | ✓ | Provider refines |
| K=v | K=v | ✓ | Exact match |
| K=v | K=w (w≠v) | ✗ | Contradiction |
| K=v | missing | ✗ | Provider lacks required |
| K=* | K=v | ✓ | Provider has a value |
| K=* | missing | ✗ | Provider lacks required |

---

## 6. Examples

### 6.1 Generic Request, Specific Provider

```
Request:  cap:op=download-model
Provider: cap:in="media:model-spec";op=download-model;out="media:download-result"

Input:  i_r=media:, i_p=media:model-spec
        Request doesn't constrain input → PASS ✓

Output: o_p=media:download-result, o_r=media:
        Request accepts any output → PASS ✓

Tags:   y_r={op:download-model}, y_p={op:download-model}
        Provider has required op → PASS ✓

Result: DISPATCHABLE ✓
```

### 6.2 Specific Request, Generic Provider (Fallback)

```
Request:  cap:in="media:pdf";op=extract;out="media:object"
Provider: cap:in="media:bytes";op=extract;out="media:"

Input:  i_r=media:pdf, i_p=media:bytes
        pdf ⪯ bytes? Yes → PASS ✓

Output: o_p=media:, o_r=media:object
        media: ⪯ media:object? No, identity is NOT more specific
        → FAIL ✗

Result: NOT DISPATCHABLE
```

### 6.3 Incompatible Types

```
Request:  cap:in="media:pdf";op=convert;out="media:html"
Provider: cap:in="media:image";op=convert;out="media:text"

Input:  i_r=media:pdf, i_p=media:image
        pdf ⪯ image? No, different families → FAIL ✗

Result: NOT DISPATCHABLE (fails at first axis)
```

---

## 7. Properties

### 7.1 Reflexivity

```
∀c ∈ C, Dispatch(c, c)
```

Any capability can handle itself.

**Proof**: For c = (i, o, y):
- i ⪯ i (reflexivity of ⪯)
- o ⪯ o (reflexivity of ⪯)
- y ⪯ y (reflexivity of ⪯)
- All three hold, so Dispatch(c, c) ✓

### 7.2 Transitivity

```
Dispatch(a, b) ∧ Dispatch(b, c) ⟹ Dispatch(a, c)
```

If a can handle b's requests, and b can handle c's requests, then a can handle c's requests.

**Proof**: By transitivity of ⪯ on each axis.

### 7.3 NOT Symmetric

```
Dispatch(p, r) ⟹̸ Dispatch(r, p)
```

A specific provider can dispatch a generic request, but not vice versa.

### 7.4 Monotonicity

If provider `p'` refines `p`:
- Same or more general input (i_p ⪯ i_p')
- Same or more specific output (o_p' ⪯ o_p)
- Same or more specific y-tags (y_p ⪯ y_p')

Then:
```
Dispatch(p, r) ⟹ Dispatch(p', r)
```

Refinement preserves dispatchability.

---

## 8. Implementation

### 8.1 Method Signature

```rust
impl CapUrn {
    pub fn is_dispatchable(&self, request: &CapUrn) -> bool;
}
```

Usage:
```rust
if provider.is_dispatchable(&request) {
    // provider can handle request
}
```

### 8.2 Pseudocode

```rust
fn is_dispatchable(&self, request: &CapUrn) -> bool {
    // Input axis: request input must conform to provider input
    // (Contravariant: provider can be looser)
    let req_in = MediaUrn::from_string(&request.in_urn);
    let prov_in = MediaUrn::from_string(&self.in_urn);
    if !req_in.conforms_to(&prov_in) {
        return false;
    }

    // Output axis: provider output must conform to request output
    // (Covariant: provider must be same or tighter)
    let prov_out = MediaUrn::from_string(&self.out_urn);
    let req_out = MediaUrn::from_string(&request.out_urn);
    if !prov_out.conforms_to(&req_out) {
        return false;
    }

    // Cap-tags axis: provider must satisfy request constraints
    if !self.cap_tags_dispatchable(request) {
        return false;
    }

    true
}
```

---

## 9. Common Mistakes

### 9.1 Using `accepts` for Dispatch

**Wrong**:
```rust
if provider.accepts(&request) { /* dispatch */ }
```

This ignores the mixed-variance nature of Cap URNs.

### 9.2 Using `conforms_to` for Dispatch

**Wrong**:
```rust
if provider.conforms_to(&request) { /* dispatch */ }
```

This also ignores mixed variance.

### 9.3 Checking Only One Axis

**Wrong**:
```rust
if provider.op == request.op { /* dispatch */ }
```

All three axes must be checked.

---

## 10. Summary

The dispatch predicate is:

```
Dispatch(p, r)  ⟺  i_r ⪯ i_p  ∧  o_p ⪯ o_r  ∧  y_r ⪯ y_p
```

| Property | Value |
|----------|-------|
| Input variance | Contravariant |
| Output variance | Covariant |
| Cap-tags variance | Invariant + Refinement |
| Symmetric? | NO |
| Reflexive? | YES |
| Transitive? | YES |

This is the **primary predicate for routing**. Ranking (next document) applies only after dispatch validity is established.
