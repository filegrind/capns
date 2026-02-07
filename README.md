# Capns - Cap Namespace System

A capability URN and definition system for plugin architectures, built on [Tagged URNs](https://github.com/filegrind/tagged-urn-rs).

## Overview

CapNs provides a formal system for defining, matching, and managing capabilities across distributed plugin systems. It extends Tagged URNs with:

- **Required direction specifiers** (`in`/`out`) for input/output media types
- **Media URN validation** for type-safe capability contracts
- **Capability registries** for provider discovery and selection
- **Schema validation** for capability arguments and outputs

The system is designed for scenarios where:
- Multiple providers can implement the same capability
- Capability selection should prioritize specificity
- Runtime capability discovery and validation is required
- Cross-language compatibility is needed

## Cap URN Format

Cap URNs extend Tagged URNs with required direction specifiers:

```
cap:in="media:void";op=generate;out="media:object"
cap:in="media:binary";op=extract;out="media:object";target=metadata
```

**Direction Specifiers:**
- `in` - Input media type (what the capability accepts)
- `out` - Output media type (what the capability produces)
- Values are Media URNs or wildcard `*`

**Common Tags:**
- `op` - The operation (e.g., `extract`, `generate`, `convert`)
- `target` - What the operation targets (e.g., `metadata`, `thumbnail`)
- `ext` - File extension for format-specific capabilities

For base Tagged URN format rules (case handling, quoting, wildcards, etc.), see [Tagged URN RULES.md](https://github.com/filegrind/tagged-urn-rs/blob/main/docs/RULES.md).

## Cap Definitions

Full capability definitions include metadata, arguments, and output schemas:

```rust
pub struct Cap {
    pub id: CapUrn,
    pub version: String,
    pub description: Option<String>,
    pub metadata: HashMap<String, String>,
    pub command: String,
    pub arguments: CapArguments,
    pub output: Option<CapOutput>,
    pub stdin: Option<String>,
}
```

**Key Fields:**
- `id` - The cap URN with direction specifiers
- `command` - CLI command or method name for execution
- `arguments` - Required and optional argument definitions with validation
- `output` - Output schema and type information
- `stdin` - If present, the media URN that stdin expects (e.g., "media:pdf;bytes"). Absence means cap doesn't accept stdin.

## Language Implementations

### Rust (`capns`)

```rust
use capns::{CapUrn, Cap, CapUrnBuilder};

// Create cap URN
let cap = CapUrn::from_string(
    "cap:in=\"media:binary\";op=extract;out=\"media:object\";target=metadata"
)?;

// Build with builder pattern
let cap = CapUrnBuilder::new()
    .in_spec("media:binary")
    .out_spec("media:object")
    .tag("op", "extract")
    .tag("target", "metadata")
    .build()?;
```

### Go (`capns-go`)

```go
import "github.com/fgnd/capns-go"

// Create cap URN
cap, err := capns.NewCapUrnFromString(
    `cap:in="media:binary";op=extract;out="media:object"`)

// Build with builder pattern
cap, err = capns.NewCapUrnBuilder().
    InSpec("media:binary").
    OutSpec("media:object").
    Tag("op", "extract").
    Build()
```

### Objective-C (`capns-objc`)

```objc
#import "CSCapUrn.h"

// Create cap URN
NSError *error;
CSCapUrn *cap = [CSCapUrn fromString:
    @"cap:in=\"media:binary\";op=extract;out=\"media:object\""
    error:&error];

// Build with builder pattern
CSCapUrnBuilder *builder = [CSCapUrnBuilder builder];
[builder inSpec:@"media:binary"];
[builder outSpec:@"media:object"];
[builder tag:@"op" value:@"extract"];
CSCapUrn *cap = [builder build:&error];
```

## Capability Matching

Capabilities match requests based on per-tag value semantics:

| Pattern Value | Meaning | Instance Missing | Instance=v | Instance=x≠v |
|---------------|---------|------------------|------------|--------------|
| (missing) | No constraint | OK | OK | OK |
| `K=?` | No constraint (explicit) | OK | OK | OK |
| `K=!` | Must-not-have | OK | NO | NO |
| `K=*` | Must-have, any value | NO | OK | OK |
| `K=v` | Must-have, exact value | NO | OK | NO |

```rust
let cap = CapUrn::from_string(
    "cap:in=\"media:binary\";op=extract;out=\"media:object\";ext=pdf")?;
let request = CapUrn::from_string(
    "cap:in=\"media:binary\";op=extract;out=\"media:object\"")?;

if cap.accepts(&request) {
    println!("Cap can handle this request");
}
```

Specificity uses graded scoring (exact=3, must-have-any=2, must-not-have=1, unspecified=0):

```rust
let general = CapUrn::from_string("cap:in=*;op=extract;out=*")?;        // specificity: 3+2+2 = 7
let specific = CapUrn::from_string(
    "cap:in=\"media:binary\";op=extract;out=\"media:object\"")?;        // specificity: 3+3+3 = 9

// specific.specificity() > general.specificity()
```

## Standard Capabilities

Common capability patterns:

**Document Processing:**
- `cap:in="media:binary";op=extract;out="media:object";target=metadata`
- `cap:in="media:binary";op=generate;out="media:binary";target=thumbnail`

**AI/ML Inference:**
- `cap:in="media:text";op=generate;out="media:object";target=embeddings`
- `cap:in="media:object";op=conversation;out="media:object"`

## Integration

### Provider Registration

```rust
let cap = CapUrn::from_string("cap:in=...;op=extract;out=...;ext=pdf")?;
provider_registry.register("pdf-provider", cap);

// Find best provider
let caller = provider_registry.can("cap:in=...;op=extract;out=...")?;
let result = caller.call(args).await?;
```

### CapCube (Multi-Provider)

```rust
let cube = CapCube::new();
cube.register_cap_set("provider-a", caps_a);
cube.register_cap_set("provider-b", caps_b);

// Automatically selects best provider by specificity
let (provider, cap) = cube.find_best_match(&request)?;
```

## Documentation

- [RULES.md](docs/RULES.md) - Cap URN specification (cap-specific rules)
- [MATCHING.md](docs/MATCHING.md) - Matching semantics
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - System architecture
- [MEDIA_SPEC_SYSTEM.md](docs/MEDIA_SPEC_SYSTEM.md) - Media specification system
- [Tagged URN RULES.md](https://github.com/filegrind/tagged-urn-rs/blob/main/docs/RULES.md) - Base URN format rules

## Cross-Language Compatibility

This Rust implementation is the reference. Identical implementations exist for:
- [Go implementation](https://github.com/filegrind/capns-go)
- [JavaScript implementation](https://github.com/filegrind/capns-js)
- [Objective-C implementation](https://github.com/filegrind/capns-objc)

All implementations pass the same test cases and follow identical rules.

## Testing

```bash
cargo test
```

## License

MIT License
