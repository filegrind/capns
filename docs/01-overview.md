---
title: "Overview"
layout: doc
permalink: /01-overview/
---
# capdag Specification

## Scope

This specification defines the semantic foundations, runtime protocol, execution model, and development patterns for the capdag system.

---

## Document Map

### Foundations (2-5)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 2 | [Formal Foundations](/docs/02-formal-foundations) | Mathematical foundation, dispatch relation | None |
| 3 | [Tagged URN Domain](/docs/03-tagged-urn-domain) | Base domain U, normalization, wildcard truth table | 2 |
| 4 | [Predicates](/docs/04-predicates) | Derived predicates (accepts, conforms_to, is_comparable, is_equivalent) | 3 |
| 5 | [Specificity](/docs/05-specificity) | Specificity scoring function | 3 |

### Dispatch and Routing (6-8)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 6 | [Cap URN Structure](/docs/06-cap-urn-structure) | Cap URN as product C = U x U x U | 3, 5 |
| 7 | [Dispatch](/docs/07-dispatch) | The dispatch predicate | 3, 4, 6 |
| 8 | [Ranking](/docs/08-ranking) | Selection among valid providers | 5, 7 |

### Machine Notation and Data Types (9-11)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 9 | [Machine Notation](/docs/09-machine-notation) | Textual encoding of multi-cap data-flow graphs | 4, 6 |
| 10 | [Validation Rules](/docs/10-validation-rules) | Structural validation constraints | 3, 6 |
| 11 | [Media URNs](/docs/11-media-urns) | Media URN structure and coercion | 3 |

### Protocol (12)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 12.1 | [Architecture](/docs/12.1-architecture) | System topology, component roles | 7, 8 |
| 12.2 | [Frame Protocol](/docs/12.2-frame-protocol) | Wire format, frame types, size limits | 12.3, 12.4 |
| 12.3 | [Handshake](/docs/12.3-handshake) | Connection setup, HELLO exchange, identity verification | — |
| 12.4 | [Streaming](/docs/12.4-streaming) | Multiplexed streams, chunking, sequence numbering | — |

### Cartridge Runtime (13)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 13.1 | [Cartridge Runtime](/docs/13.1-cartridge-runtime) | Entry point, handler registration, CLI/cartridge mode | 7, 12.3 |
| 13.2 | [Input and Output](/docs/13.2-input-output) | InputStream, OutputStream, stream lookup | 13.4, 15.4 |
| 13.3 | [Peer Invocation](/docs/13.3-peer-invocation) | Cross-cartridge capability calls | — |
| 13.4 | [Progress and Logging](/docs/13.4-progress-and-logging) | LOG frames, progress mapping, keepalive | 15.3 |

### Host and Relay (14)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 14.1 | [Host Runtime](/docs/14.1-host-runtime) | Cartridge lifecycle, frame routing, health monitoring | 12.3 |
| 14.2 | [Relay Switch](/docs/14.2-relay-switch) | Cap-aware routing multiplexer | 7, 8, 15.4 |
| 14.3 | [Relay Topology](/docs/14.3-relay-topology) | RelaySlave/RelayMaster pairs, Unix socket relay chains | — |

### Execution (15)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 15.1 | [Orchestrator](/docs/15.1-orchestrator) | Machine notation parsing, DAG construction | 11 |
| 15.2 | [Execution](/docs/15.2-execution) | execute_dag, execute_fanin, topological sort | 15.3 |
| 15.3 | [Progress Mapping](/docs/15.3-progress-mapping) | Deterministic progress subdivision | — |
| 15.4 | [Planner](/docs/15.4-planner) | Path finding, LiveCapFab, MachinePlan | 15.1 |

### Cartridge Development (16)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 16.1 | [Cartridge Anatomy](/docs/16.1-cartridge-anatomy) | Cartridge structure, manifest, cap definitions | 12.3 |
| 16.2 | [Handler Patterns](/docs/16.2-handler-patterns) | Op trait, argument extraction, output emission | 15.4 |
| 16.3 | [Model Cartridges](/docs/16.3-model-cartridges) | ML cartridges, three-phase architecture | 13.4 |
| 16.4 | [Content Cartridges](/docs/16.4-content-cartridges) | Document processing, standard cap patterns | 7, 16.2 |
| 16.5 | [Rust vs Swift](/docs/16.5-rust-vs-swift) | Implementation differences, module coverage | — |

### Integration (17)

| # | Document | Purpose | Dependencies |
|---|----------|---------|--------------|
| 17.1 | [Task Integration](/docs/17.1-task-integration) | Cartridge execution in MachineFabric tasks | 15.4 |
| 17.2 | [Error Handling](/docs/17.2-error-handling) | Error type hierarchy, propagation patterns | 13.4 |
| 17.3 | [Memory Pressure Detection](/docs/17.3-memory-pressure-detection) | macOS memory pressure, cartridge lifecycle | — |

---

## Reading Order

1. **02 Formal Foundations** — mathematical foundation (optional, for formal reference)
2. **03 Tagged URN Domain** — the base domain
3. **04 Predicates** — the four derived predicates
4. **05 Specificity** — scoring
5. **06 Cap URN Structure** — how Cap URNs compose four structural coordinates
6. **07 Dispatch** — the central routing rule
7. **08 Ranking** — selection among valid providers
8. **09 Machine Notation** — wire multiple caps into a data-flow graph
9. **10 Validation Rules** — structural constraints
10. **11 Media URNs** — media type details
11. **12 Protocol** — wire format and connection setup
12. **13 Cartridge Runtime** — writing handlers
13. **14 Host and Relay** — cartridge hosting infrastructure
14. **15 Execution** — DAG orchestration and planning
15. **16 Cartridge Development** — building cartridges
16. **17 Integration** — tasks, errors, resource management

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
| **Machine** | An ordered collection of MachineStrands wired into data-flow graphs |
| **MachineStrand** | A maximal connected component of resolved cap edges with anchors |
| **Machine notation** | The textual encoding of a Machine (see [09-MACHINE-NOTATION](/docs/09-machine-notation)) |
| **Cartridge** | A standalone binary that provides one or more capabilities |
| **Cap Kind** | A logical classification of a cap into one of *Identity*, *Source*, *Sink*, *Effect*, or *Transform*, derived from `(in, out, y, effect)`. See [06-CAP-URN-STRUCTURE §4](/docs/06-cap-urn-structure#4-cap-kinds). |
| **Top type** | `media:` — the universal wildcard for media URNs. Every media URN conforms to it. A side typed `media:` reads as "any A." |
| **Unit type** | `media:void` — the nullary value. A side typed `media:void` reads as "()": no meaningful data flows there. Distinct from the top type. **Atomic**: refinements like `media:void;text` are parse errors. |

---

## Notational Conventions

### Order-Theoretic Notation

- `a <=  b` — "a is at least as specific as b" (a refines b)
- `a >= b` — "a is at least as general as b" (equivalent to b <= a)

### Cap URN Components

A Cap URN `c` is written as a quadruple:
```
c = (i, o, y, e)
```
where:
- `i` = input media URN (the `in` tag value)
- `o` = output media URN (the `out` tag value)
- `y` = non-direction cap-tags (op, ext, model, etc. — all arbitrary, none has functional privilege)
- `e` = effect on runtime media/type identity

---

## Foundation

This specification builds on the mathematical foundations in [02-FORMAL-FOUNDATIONS](/docs/02-formal-foundations), which defines the core dispatch relation:

```
Dispatch(p, r) <=> i_r <= i_p /\ o_p <= o_r /\ (e_r = ? \/ e_p = e_r) /\ y_r <= y_p
```

The numbered documents fill in operational details: wildcard truth tables, normalization rules, specificity scoring, validation constraints, machine notation, protocol, runtime, execution, and development patterns.

---

## Conformance

An implementation conforms to this specification if:

1. All Tagged URNs normalize identically (per §3)
2. All predicates compute identically (per §4)
3. Specificity scores match (per §5)
4. Dispatch validity matches (per §7)
5. All validation rules are enforced (per §10)

Ranking policy (§8) may vary by subsystem, but dispatch validity must not.
