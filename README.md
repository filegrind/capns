# CapNs - Cap Definition System

A capability URN and definition system for plugin architectures, built on [Tagged URNs](../tagged-urn-rs/).

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
cap:in="media:type=void;v=1";op=generate;out="media:type=object;v=1"
cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1";target=metadata
```

**Direction Specifiers:**
- `in` - Input media type (what the capability accepts)
- `out` - Output media type (what the capability produces)
- Values are Media URNs or wildcard `*`

**Common Tags:**
- `op` - The operation (e.g., `extract`, `generate`, `convert`)
- `target` - What the operation targets (e.g., `metadata`, `thumbnail`)
- `ext` - File extension for format-specific capabilities

For base Tagged URN format rules (case handling, quoting, wildcards, etc.), see [Tagged URN RULES.md](../tagged-urn-rs/docs/RULES.md).

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
- `stdin` - If present, the media URN that stdin expects (e.g., "media:type=pdf;v=1;binary"). Absence means cap doesn't accept stdin.

## Language Implementations

### Rust (`capns`)

```rust
use capns::{CapUrn, Cap, CapUrnBuilder};

// Create cap URN
let cap = CapUrn::from_string(
    "cap:in=\"media:type=binary;v=1\";op=extract;out=\"media:type=object;v=1\";target=metadata"
)?;

// Build with builder pattern
let cap = CapUrnBuilder::new()
    .in_spec("media:type=binary;v=1")
    .out_spec("media:type=object;v=1")
    .tag("op", "extract")
    .tag("target", "metadata")
    .build()?;
```

### Go (`capns-go`)

```go
import "github.com/fgnd/capns-go"

// Create cap URN
cap, err := capns.NewCapUrnFromString(
    `cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1"`)

// Build with builder pattern
cap, err = capns.NewCapUrnBuilder().
    InSpec("media:type=binary;v=1").
    OutSpec("media:type=object;v=1").
    Tag("op", "extract").
    Build()
```

### Objective-C (`capns-objc`)

```objc
#import "CSCapUrn.h"

// Create cap URN
NSError *error;
CSCapUrn *cap = [CSCapUrn fromString:
    @"cap:in=\"media:type=binary;v=1\";op=extract;out=\"media:type=object;v=1\""
    error:&error];

// Build with builder pattern
CSCapUrnBuilder *builder = [CSCapUrnBuilder builder];
[builder inSpec:@"media:type=binary;v=1"];
[builder outSpec:@"media:type=object;v=1"];
[builder tag:@"op" value:@"extract"];
CSCapUrn *cap = [builder build:&error];
```

## Capability Matching

Capabilities match requests when all specified tags are compatible:

```rust
let cap = CapUrn::from_string(
    "cap:in=\"media:type=binary;v=1\";op=extract;out=\"media:type=object;v=1\";ext=pdf")?;
let request = CapUrn::from_string(
    "cap:in=\"media:type=binary;v=1\";op=extract;out=\"media:type=object;v=1\"")?;

if cap.matches(&request) {
    println!("Cap can handle this request");
}
```

More specific capabilities are preferred:

```rust
let general = CapUrn::from_string("cap:in=*;op=extract;out=*")?;
let specific = CapUrn::from_string(
    "cap:in=\"media:type=binary;v=1\";op=extract;out=\"media:type=object;v=1\"")?;

// specific.specificity() > general.specificity()
```

## Standard Capabilities

Common capability patterns:

**Document Processing:**
- `cap:in="media:type=binary;v=1";op=extract;out="media:type=object;v=1";target=metadata`
- `cap:in="media:type=binary;v=1";op=generate;out="media:type=binary;v=1";target=thumbnail`

**AI/ML Inference:**
- `cap:in="media:type=text;v=1";op=generate;out="media:type=object;v=1";target=embeddings`
- `cap:in="media:type=object;v=1";op=conversation;out="media:type=object;v=1"`

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
- [Tagged URN RULES.md](../tagged-urn-rs/docs/RULES.md) - Base URN format rules

## Testing

```bash
# Rust
cargo test

# Go
go test ./...

# Objective-C
swift test
```

## License

MIT License
