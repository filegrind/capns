# CapNs Architecture

This document describes the capability lookup and execution architecture in CapNs.

## Overview

CapNs provides a unified system for discovering and executing capabilities across multiple providers. The architecture consists of three main components:

1. **CapSet** - Trait for capability execution
2. **CapMatrix** - Registry for capability providers within a single domain
3. **CapBlock** - Composite registry that aggregates multiple CapMatrix instances

```
                    CapBlock (unified lookup)
                    /                      \
           CapMatrix                   CapMatrix
         "providers"                   "plugins"
          /      \                      /      \
      CapSet   CapSet              CapSet   CapSet
      (pdf)    (docx)              (wav)    (mp3)
```

## Core Components

### CapSet

The `CapSet` trait defines the interface for executing capabilities:

```rust
pub trait CapSet: Send + Sync + std::fmt::Debug {
    fn execute_cap(
        &self,
        cap_urn: &str,
        positional_args: &[String],
        named_args: &[(String, String)],
        stdin_data: Option<Vec<u8>>
    ) -> Pin<Box<dyn Future<Output = Result<(Option<Vec<u8>>, Option<String>)>> + Send + '_>>;
}
```

**Key characteristics:**
- Async execution via pinned futures
- Thread-safe (`Send + Sync`)
- Returns either binary data or string output
- Accepts positional args, named args, and optional stdin

**Implementations:**
- `ProviderRegistry` - Executes via internal/external providers
- `FgndPluginGateway` - Executes via gRPC to plugins
- `CompositeCapSet` - Delegates to best-matching child registry

### CapMatrix

A `CapMatrix` stores capability providers for a single domain (e.g., all providers, or all plugins):

```rust
pub struct CapMatrix {
    sets: HashMap<String, CapSetEntry>,
}

struct CapSetEntry {
    name: String,
    host: Arc<dyn CapSet>,
    capabilities: Vec<Cap>,
}
```

**Operations:**

| Method | Description |
|--------|-------------|
| `register_cap_set(name, host, caps)` | Register a CapSet with its capabilities |
| `find_cap_sets(request_urn)` | Find all CapSets that can handle a request |
| `find_best_cap_set(request_urn)` | Find the most specific matching CapSet |
| `can_handle(request_urn)` | Check if any CapSet can handle the request |
| `unregister_cap_set(name)` | Remove a registered CapSet |

**Matching algorithm:**

1. Parse the request URN
2. For each registered CapSet, check if any of its caps match the request
3. A cap matches if it satisfies all tags in the request (see RULES.md sections 12-17)
4. When finding the "best" match, select the one with highest specificity

### CapBlock

A `CapBlock` aggregates multiple `CapMatrix` instances for unified lookup:

```rust
pub struct CapBlock {
    registries: Vec<(String, Arc<RwLock<CapMatrix>>)>,
}
```

**Purpose:**
- Provides a single entry point for capability lookup across providers and plugins
- Compares matches by specificity across all child registries
- Uses registry order as tiebreaker (first registry wins on equal specificity)

**Operations:**

| Method | Description |
|--------|-------------|
| `add_registry(name, matrix)` | Add a child CapMatrix (order matters for tiebreaking) |
| `find_best_cap_set(request_urn)` | Find best match across ALL child registries |
| `can(request_urn)` | Get a CapCaller for the best-matching capability |
| `can_handle(request_urn)` | Check if any registry can handle the request |

## Specificity-Based Selection

When multiple capabilities match a request, the system selects based on specificity:

```
Request: cap:op=generate_thumbnail;out=media:bytes;ext=pdf

Provider cap: cap:op=generate_thumbnail;out=media:bytes
  -> Specificity: 2 (matches op, out; missing ext treated as wildcard)

Plugin cap: cap:op=generate_thumbnail;out=media:bytes;ext=pdf
  -> Specificity: 3 (matches op, out, ext)

Winner: Plugin (specificity 3 > 2)
```

**Specificity rules:**
- Count of non-wildcard tags in the capability
- Missing tags count as 0 (implicit wildcard)
- Explicit `*` wildcard counts as 0
- Higher specificity wins

**Tiebreaking:**
When specificities are equal, the first registry added to CapBlock wins. This allows prioritizing providers over plugins (or vice versa) by controlling registration order.

## CapCaller

The `CapCaller` provides a convenient wrapper for executing capabilities:

```rust
let caller = cap_block.can("cap:op=generate_thumbnail;ext=pdf")?;
let result = caller.call(positional_args, named_args, stdin).await?;
```

**Features:**
- Validates arguments against cap schema before execution
- Returns `ResponseWrapper` with type-safe accessors
- Preserves the `can().call()` pattern for ergonomic usage

## Integration Architecture

### Provider Integration

```rust
// ProviderRegistry owns a CapMatrix for its providers
pub struct ProviderRegistry {
    cap_matrix: Arc<RwLock<CapMatrix>>,
    internal_providers: Vec<Box<dyn InternalProvider>>,
    external_providers: Vec<ExternalProvider>,
}

impl ProviderRegistry {
    // Expose the CapMatrix for CapBlock integration
    pub fn cap_matrix(&self) -> Arc<RwLock<CapMatrix>> {
        self.cap_matrix.clone()
    }
}
```

### Plugin Integration

```rust
// FgndPluginGateway owns a CapMatrix for plugin capabilities
pub struct FgndPluginGateway {
    cap_matrix: Arc<RwLock<CapMatrix>>,
    grpc_client: Arc<GrpcPluginClient>,
}

impl FgndPluginGateway {
    // Expose the CapMatrix for CapBlock integration
    pub fn cap_matrix(&self) -> Arc<RwLock<CapMatrix>> {
        self.cap_matrix.clone()
    }
}
```

### Unified Service

```rust
pub struct CapService {
    provider_registry: Arc<Mutex<ProviderRegistry>>,
    plugin_gateway: Arc<FgndPluginGateway>,
    cap_block: CapBlock,
}

impl CapService {
    pub async fn new() -> Result<Self> {
        let mut provider_registry = ProviderRegistry::new();
        provider_registry.initialize().await?;

        let mut plugin_gateway = FgndPluginGateway::new(...);
        plugin_gateway.initialize().await?;

        // Create CapBlock with both registries
        // Providers added first = higher priority on ties
        let mut cap_block = CapBlock::new();
        cap_block.add_registry("providers".into(), provider_registry.cap_matrix());
        cap_block.add_registry("plugins".into(), plugin_gateway.cap_matrix());

        Ok(Self { provider_registry, plugin_gateway, cap_block })
    }

    pub async fn can(&self, cap_urn: &str) -> Result<CapCaller> {
        // Use CapBlock for provider lookup (sync)
        let provider_result = self.cap_block.find_best_cap_set(cap_urn);

        // Use plugin gateway directly for async gRPC lookup
        let plugin_result = self.plugin_gateway.resolve(cap_urn).await;

        // Compare specificities and return best match
        // ...
    }
}
```

## Thread Safety

The architecture is designed for concurrent access:

- `CapSet` requires `Send + Sync`
- `CapMatrix` is wrapped in `Arc<RwLock<...>>` for shared mutable access
- Locks are held briefly and never across await points
- `Arc<dyn CapSet>` is cloned before async execution to release locks

## Error Handling

| Error | Description |
|-------|-------------|
| `CapMatrixError::NoSetsFound` | No capability matches the request |
| `CapMatrixError::InvalidUrn` | Request URN is malformed |
| `CapMatrixError::RegistryError` | Lock acquisition failed |

## Usage Example

```rust
// Setup
let mut cap_block = CapBlock::new();
cap_block.add_registry("providers".into(), provider_cap_matrix);
cap_block.add_registry("plugins".into(), plugin_cap_matrix);

// Lookup and execute
match cap_block.can("cap:op=generate_thumbnail;ext=pdf") {
    Ok(caller) => {
        let result = caller.call(vec![], vec![], None).await?;
        println!("Result: {:?}", result.as_bytes());
    }
    Err(CapMatrixError::NoSetsFound(urn)) => {
        println!("No provider found for: {}", urn);
    }
    Err(e) => return Err(e.into()),
}
```

## See Also

- [RULES.md](RULES.md) - Cap URN format specification and matching semantics
- [README-REGISTRY.md](README-REGISTRY.md) - capns.org registry integration
- [MEDIA_SPEC_SYSTEM.md](MEDIA_SPEC_SYSTEM.md) - Media type specifications
