# CapNs - Cap Definition System

A cap identifier and definition system for plugin architectures, supporting tag-based cap matching with wildcard patterns and specificity comparison.

## Overview

CapNs provides a formal system for defining, matching, and managing caps across distributed plugin systems. It uses a flat tag-based naming scheme that supports wildcards, specificity comparison, and validation of cap arguments and outputs.

The system is designed for scenarios where:
- Multiple providers can implement the same cap 
- Cap selection should prioritize specificity
- Runtime cap discovery and validation is required
- Cross-language compatibility is needed

## Architecture

### Cap URNs

Cap URNs use a flat tag-based format: `tag1=value1;tag2=value2;tag3=value3`

**Core Tags:**
- `type` - The cap domain (e.g., `document`, `inference`, `file`)
- `action` - The operation performed (e.g., `extract`, `generate`, `transform`)
- `target` - What the action operates on (e.g., `metadata`, `embeddings`, `thumbnail`)
- `format` - Input/output format (e.g., `pdf`, `json`, `png`)
- `language` - Language support (e.g., `en`, `es`, `multilingual`)
- `output` - Output type (e.g., `binary`, `json`, `text`)

**Examples:**
```
action=extract;target=metadata;ext=pdf
action=conversation;language=en;type=unconstrained
action=generate;target=thumbnail;format=epub;output=binary
```

**Wildcards:**
- Use `*` to match any value: `action=extract;format=*;`
- Wildcards enable flexible cap requests

**Specificity:**
- More specific caps are preferred over general ones
- `action=extract;ext=pdf;` is more specific than `action=extract;`

### Cap Definitions

Full cap definitions include metadata, arguments, output schemas, and execution details:

```rust
pub struct Cap {
    pub id: CapUrn,
    pub version: String,
    pub description: Option<String>,
    pub metadata: HashMap<String, String>,
    pub command: String,
    pub arguments: CapArguments,
    pub output: Option<CapOutput>,
    pub accepts_stdin: bool,
}
```

**Key Fields:**
- `id` - The cap identifier using tag-based format
- `command` - CLI command or method name for execution
- `arguments` - Required and optional argument definitions with validation
- `output` - Output schema and type information
- `accepts_stdin` - Whether the cap accepts input via stdin

### Arguments and Validation

Arguments support multiple types with validation rules:

```rust
pub struct CapArgument {
    pub name: String,
    pub arg_type: ArgumentType, // String, Integer, Number, Boolean, Array, Object, Binary
    pub description: String,
    pub cli_flag: String,
    pub position: Option<usize>,
    pub validation: ArgumentValidation,
    pub default: Option<serde_json::Value>,
}
```

**Validation Options:**
- Numeric ranges (`min`, `max`)
- String length constraints (`min_length`, `max_length`)
- Pattern matching (regex)
- Allowed values (enumeration)

## Language Implementations

CapNs is implemented in multiple languages for cross-platform compatibility:

### Rust (`capns`)
Core implementation with full feature set.

```rust
use capns::{CapUrn, Cap, CapUrnBuilder};

// Create cap URN
let key = CapUrn::from_string("cap:action=extract;target=metadata;")?;

// Build cap URN with builder pattern
let key = CapUrnBuilder::new()
    
    .action("extract")
    .target("metadata")
    .format("pdf")
    .build()?;

// Create cap definition
let cap = Cap::new(key, "1.0.0".to_string(), "extract-metadata".to_string());
```

### Go (`capns-go`)
Feature-complete Go implementation.

```go
import "github.com/fmio/capns-go"

// Create cap URN
key, err := capns.NewCapUrnFromString("cap:action=extract;target=metadata;")

// Build with builder pattern
key, err = capns.NewCapUrnBuilder().
    Action("extract").
    Target("metadata").
    Format("pdf").
    Build()

// Create cap
cap := capns.NewCap(key, "1.0.0", "extract-metadata")
cap.AcceptsStdin = true
```

### Objective-C (`capns-objc`)
Native Objective-C/Swift implementation for Apple platforms.

```objc
#import "CSCap.h"
#import "CSCapUrn.h"

// Create cap URN
NSError *error;
CSCapUrn *key = [CSCapUrn fromString:@"cap:action=extract;target=metadata;" 
                                             error:&error];

// Build with builder pattern
CSCapUrnBuilder *builder = [CSCapUrnBuilder new];
CSCapUrn *key = [[[[builder action:@"extract"] 
                           target:@"metadata"] 
                          format:@"pdf"] 
                         build];

// Create cap
CSCap *cap = [CSCap capWithId:key
                                                  version:@"1.0.0"
                                                  command:@"extract-metadata"];
```

## Key Operations

### Cap Matching

```rust
// Check if cap can handle request
let cap_urn = CapUrn::from_string("cap:action=extract;target=metadata;ext=pdf")?;
let request_key = CapUrn::from_string("action=extract;")?;

if cap_urn.can_handle(&request_key) {
    println!("Cap can handle this request");
}
```

### Specificity Comparison

```rust
let general = CapUrn::from_string("action=extract;")?;
let specific = CapUrn::from_string("cap:action=extract;ext=pdf")?;

if specific.is_more_specific_than(&general) {
    println!("Specific cap preferred");
}
```

### Builder Pattern

All implementations support fluent builder APIs:

```rust
let key = CapUrnBuilder::new()
    .type_tag("inference")
    .action("conversation")
    .tag("language", "en")
    .tag("model", "gpt-4")
    .build()?;
```

## Standard Caps

Common cap patterns are predefined:

**Document Processing:**
- `action=extract;target=metadata;format={pdf,txt,md,...}`
- `action=extract;target=pages;format={pdf,epub,...}`
- `action=extract;target=outline;format={pdf,...}`
- `action=generate;target=thumbnail;format={pdf,epub,...};output=binary`

**AI/ML Inference:**
- `action={conversation,analysis,embedding};type=constrained;language={en,es,multilingual,...}`
- `action=generate;target=embeddings;`
- `action=dimensions;target=embeddings;`

**File Operations:**
- `action=validate;type=file`
- `action=read;type=file;format={json,xml,csv,...}`

## Input/Output Handling

### Standard Arguments
Most caps use command-line arguments for input:

```rust
// Arguments passed as CLI flags
// --input file.pdf --output metadata.json --format pdf
```

### Stdin Input
Caps can accept input via stdin for streaming scenarios:

```rust
let mut cap = Cap::new(key, "1.0.0".to_string(), "generate".to_string());
cap.accepts_stdin = true;

// Provider receives input via stdin, outputs JSON to stdout
// echo "text content" | provider generate
```

This is particularly useful for:
- Large text processing (embeddings, analysis)
- Streaming data scenarios
- Memory-efficient processing

## Integration Patterns

### Provider Registration

```rust
// Register cap with provider
let cap = extract_metadata_cap();
provider_registry.register_cap("pdf-provider", cap);

// Find best provider for cap
let caller = provider_registry.can("cap:action=extract;target=metadata;ext=pdf")?;
let result = caller.call(args).await?;
```

### Plugin Development

Standard caps are provided for common operations:

```rust
use capns_plugin_sdk::standard::*;

// Use predefined cap
let cap = extract_metadata_cap();

// Customize for specific file type
let mut pdf_cap = cap.clone();
pdf_cap.id = CapUrn::from_string("cap:action=extract;target=metadata;ext=pdf")?;
```

## Validation

CapNs includes validation for:
- **Cap URN format** - Ensures proper tag=value syntax
- **Argument types** - Validates JSON arguments against cap schema
- **Required fields** - Checks for mandatory arguments
- **Value constraints** - Enforces min/max, patterns, allowed values

## Error Handling

Common error scenarios:
- **Invalid cap format** - Malformed tag syntax
- **Cap not found** - No provider supports the requested cap
- **Validation failure** - Arguments don't match cap schema
- **Provider execution failure** - Provider command fails

## Use Cases

### Plugin Architecture
- Dynamic cap discovery
- Provider selection based on specificity
- Runtime validation of plugin caps

### Microservices
- Service cap advertisement
- Request routing based on cap matching
- Input/output validation

### AI/ML Pipelines
- Model cap definition
- Task routing to appropriate models
- Input preprocessing based on caps

### Document Processing
- Format-specific processor selection
- Cap-based workflow routing
- Output format negotiation

## Testing

All implementations include test suites covering:
- Cap URN creation and parsing
- Matching and specificity algorithms
- Builder pattern functionality
- Serialization/deserialization
- Cross-language compatibility

Run tests:
```bash
# Rust
cargo test

# Go
go test

# Objective-C
swift test
```

## Project Structure

```
capns/              # Rust implementation
capns-go/           # Go implementation  
capns-objc/         # Objective-C implementation
fmio-plugin-sdk/     # Plugin development SDK
fmio-provider-sdk/   # Provider development SDK
```

## Dependencies

**Rust:**
- `serde` - Serialization/deserialization
- `serde_json` - JSON support

**Go:**
- `github.com/stretchr/testify` - Testing framework

**Objective-C:**
- Foundation framework
- XCTest framework (testing)

## License

MIT License