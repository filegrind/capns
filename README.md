# CapDef - Capability Definition System

A capability identifier and definition system for plugin architectures, supporting tag-based capability matching with wildcard patterns and specificity comparison.

## Overview

CapDef provides a formal system for defining, matching, and managing capabilities across distributed plugin systems. It uses a flat tag-based naming scheme that supports wildcards, specificity comparison, and validation of capability arguments and outputs.

The system is designed for scenarios where:
- Multiple providers can implement the same capability 
- Capability selection should prioritize specificity
- Runtime capability discovery and validation is required
- Cross-language compatibility is needed

## Architecture

### Capability Keys

Capability keys use a flat tag-based format: `tag1=value1;tag2=value2;tag3=value3`

**Core Tags:**
- `type` - The capability domain (e.g., `document`, `inference`, `file`)
- `action` - The operation performed (e.g., `extract`, `generate`, `transform`)
- `target` - What the action operates on (e.g., `metadata`, `embeddings`, `thumbnail`)
- `format` - Input/output format (e.g., `pdf`, `json`, `png`)
- `language` - Language support (e.g., `en`, `es`, `multilingual`)
- `output` - Output type (e.g., `binary`, `json`, `text`)

**Examples:**
```
action=extract;target=metadata;type=document;format=pdf
action=conversation;language=en;type=inference
action=generate;target=thumbnail;type=document;format=epub;output=binary
```

**Wildcards:**
- Use `*` to match any value: `action=extract;format=*;type=document`
- Wildcards enable flexible capability requests

**Specificity:**
- More specific capabilities are preferred over general ones
- `action=extract;format=pdf;type=document` is more specific than `action=extract;type=document`

### Capability Definitions

Full capability definitions include metadata, arguments, output schemas, and execution details:

```rust
pub struct Capability {
    pub id: CapabilityKey,
    pub version: String,
    pub description: Option<String>,
    pub metadata: HashMap<String, String>,
    pub command: String,
    pub arguments: CapabilityArguments,
    pub output: Option<CapabilityOutput>,
    pub accepts_stdin: bool,
}
```

**Key Fields:**
- `id` - The capability identifier using tag-based format
- `command` - CLI command or method name for execution
- `arguments` - Required and optional argument definitions with validation
- `output` - Output schema and type information
- `accepts_stdin` - Whether the capability accepts input via stdin

### Arguments and Validation

Arguments support multiple types with validation rules:

```rust
pub struct CapabilityArgument {
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

CapDef is implemented in multiple languages for cross-platform compatibility:

### Rust (`capdef`)
Core implementation with full feature set.

```rust
use capdef::{CapabilityKey, Capability, CapabilityKeyBuilder};

// Create capability key
let key = CapabilityKey::from_string("action=extract;target=metadata;type=document")?;

// Build capability key with builder pattern
let key = CapabilityKeyBuilder::new()
    .type_tag("document")
    .action("extract")
    .target("metadata")
    .format("pdf")
    .build()?;

// Create capability definition
let capability = Capability::new(key, "1.0.0".to_string(), "extract-metadata".to_string());
```

### Go (`capdef-go`)
Feature-complete Go implementation.

```go
import "github.com/lbvr/capdef-go"

// Create capability key
key, err := capdef.NewCapabilityKeyFromString("action=extract;target=metadata;type=document")

// Build with builder pattern
key, err = capdef.NewCapabilityKeyBuilder().
    Type("document").
    Action("extract").
    Target("metadata").
    Format("pdf").
    Build()

// Create capability
capability := capdef.NewCapability(key, "1.0.0", "extract-metadata")
capability.AcceptsStdin = true
```

### Objective-C (`capdef-objc`)
Native Objective-C/Swift implementation for Apple platforms.

```objc
#import "CSCapability.h"
#import "CSCapabilityKey.h"

// Create capability key
NSError *error;
CSCapabilityKey *key = [CSCapabilityKey fromString:@"action=extract;target=metadata;type=document" 
                                             error:&error];

// Build with builder pattern
CSCapabilityKeyBuilder *builder = [CSCapabilityKeyBuilder new];
CSCapabilityKey *key = [[[[[builder type:@"document"] 
                            action:@"extract"] 
                           target:@"metadata"] 
                          format:@"pdf"] 
                         build];

// Create capability
CSCapability *capability = [CSCapability capabilityWithId:key
                                                  version:@"1.0.0"
                                                  command:@"extract-metadata"];
```

## Key Operations

### Capability Matching

```rust
// Check if capability can handle request
let capability_key = CapabilityKey::from_string("action=extract;target=metadata;type=document;format=pdf")?;
let request_key = CapabilityKey::from_string("action=extract;type=document")?;

if capability_key.can_handle(&request_key) {
    println!("Capability can handle this request");
}
```

### Specificity Comparison

```rust
let general = CapabilityKey::from_string("action=extract;type=document")?;
let specific = CapabilityKey::from_string("action=extract;type=document;format=pdf")?;

if specific.is_more_specific_than(&general) {
    println!("Specific capability preferred");
}
```

### Builder Pattern

All implementations support fluent builder APIs:

```rust
let key = CapabilityKeyBuilder::new()
    .type_tag("inference")
    .action("conversation")
    .tag("language", "en")
    .tag("model", "gpt-4")
    .build()?;
```

## Standard Capabilities

Common capability patterns are predefined:

**Document Processing:**
- `action=extract;target=metadata;type=document;format={pdf,txt,md,...}`
- `action=extract;target=pages;type=document;format={pdf,epub,...}`
- `action=extract;target=outline;type=document;format={pdf,...}`
- `action=generate;target=thumbnail;type=document;format={pdf,epub,...};output=binary`

**AI/ML Inference:**
- `action={conversation,analysis,embedding};type=inference;language={en,es,multilingual,...}`
- `action=generate;target=embeddings;type=document`
- `action=dimensions;target=embeddings;type=document`

**File Operations:**
- `action=validate;type=file`
- `action=read;type=file;format={json,xml,csv,...}`

## Input/Output Handling

### Standard Arguments
Most capabilities use command-line arguments for input:

```rust
// Arguments passed as CLI flags
// --input file.pdf --output metadata.json --format pdf
```

### Stdin Input
Capabilities can accept input via stdin for streaming scenarios:

```rust
let mut capability = Capability::new(key, "1.0.0".to_string(), "generate".to_string());
capability.accepts_stdin = true;

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
// Register capability with provider
let capability = extract_metadata_capability();
provider_registry.register_capability("pdf-provider", capability);

// Find best provider for capability
let caller = provider_registry.can("action=extract;target=metadata;type=document;format=pdf")?;
let result = caller.call(args).await?;
```

### Plugin Development

Standard capabilities are provided for common operations:

```rust
use capdef_plugin_sdk::standard::*;

// Use predefined capability
let capability = extract_metadata_capability();

// Customize for specific file type
let mut pdf_capability = capability.clone();
pdf_capability.id = CapabilityKey::from_string("action=extract;target=metadata;type=document;format=pdf")?;
```

## Validation

CapDef includes validation for:
- **Capability key format** - Ensures proper tag=value syntax
- **Argument types** - Validates JSON arguments against capability schema
- **Required fields** - Checks for mandatory arguments
- **Value constraints** - Enforces min/max, patterns, allowed values

## Error Handling

Common error scenarios:
- **Invalid capability format** - Malformed tag syntax
- **Capability not found** - No provider supports the requested capability
- **Validation failure** - Arguments don't match capability schema
- **Provider execution failure** - Provider command fails

## Use Cases

### Plugin Architecture
- Dynamic capability discovery
- Provider selection based on specificity
- Runtime validation of plugin capabilities

### Microservices
- Service capability advertisement
- Request routing based on capability matching
- Input/output validation

### AI/ML Pipelines
- Model capability definition
- Task routing to appropriate models
- Input preprocessing based on capabilities

### Document Processing
- Format-specific processor selection
- Capability-based workflow routing
- Output format negotiation

## Testing

All implementations include test suites covering:
- Capability key creation and parsing
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
capdef/              # Rust implementation
capdef-go/           # Go implementation  
capdef-objc/         # Objective-C implementation
lbvr-plugin-sdk/     # Plugin development SDK
lbvr-provider-sdk/   # Provider development SDK
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