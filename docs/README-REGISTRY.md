# Capns Registry Integration

This document describes the registry integration features that have been added to all capns implementations.

## Overview

The capns registry (https://capns.org) provides canonical definitions for cap URNs. The registry integration features include:

- **Local-first caching**: Check local cache before hitting the registry
- **Automatic fallback**: Query capns.org if not cached locally 
- **Persistent caching**: Cache definitions to avoid repeated network calls
- **Validation**: Reject caps without canonical definitions
- **Argument validation**: Validate cap calls against registry schemas

## Usage Examples

### Rust

```rust
use capns::{CapRegistry, RegistryValidator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Simple: Get a ready-to-use cap
    let registry = CapRegistry::new().await?;
    let cap = registry.get_cap("cap:op=extract;target=metadata").await?;
    println!("Got cap: {}", cap.urn_string());
    
    // Or try without failing
    if let Some(cap) = registry.try_get_cap("cap:op=extract;target=metadata").await {
        println!("Cap available: {}", cap.urn_string());
    }
    
    // Get multiple caps at once
    let caps = registry.get_caps(&[
        "cap:op=extract;target=metadata",
        "cap:op=generate;target=thumbnail"
    ]).await?;
    println!("Got {} caps", caps.len());
    
    // Using validator for additional validation
    let validator = RegistryValidator::with_registry().await?;
    let cap = validator.get_validated_cap("cap:op=extract;target=metadata").await?;
    println!("Validated cap: {}", cap.urn_string());
    
    Ok(())
}
```

### Go

```go
package main

import (
    "fmt"
    "log"
    
    capns "github.com/fgnd/capns-go"
)

func main() {
    // Simple: Get a ready-to-use cap
    registry, err := capns.NewCapRegistry()
    if err != nil {
        log.Fatal(err)
    }
    
    cap, err := registry.GetCap("cap:op=extract;target=metadata")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Got cap: %s\n", cap.UrnString())
    
    // Or try without failing
    if cap := registry.TryGetCap("cap:op=extract;target=metadata"); cap != nil {
        fmt.Printf("Cap available: %s\n", cap.UrnString())
    }
    
    // Get multiple caps at once
    urns := []string{
        "cap:op=extract;target=metadata",
        "cap:op=generate;target=thumbnail",
    }
    caps, err := registry.GetCaps(urns)
    if err == nil {
        fmt.Printf("Got %d caps\n", len(caps))
    }
    
    // Using validator for additional validation
    validator, err := capns.NewRegistryValidator()
    if err != nil {
        log.Fatal(err)
    }
    
    cap, err = validator.GetValidatedCap("cap:op=extract;target=metadata")
    if err != nil {
        log.Printf("Validation error: %v", err)
    } else {
        fmt.Printf("Validated cap: %s\n", cap.UrnString())
    }
}
```

### Objective-C

```objc
#import <CapNs/CapNs.h>

int main() {
    // Simple: Get a ready-to-use cap
    CSCapRegistry *registry = [CSCapRegistry registry];
    
    [registry getCap:@"cap:op=extract;target=metadata" completion:^(CSCap *cap, NSError *error) {
        if (cap) {
            NSLog(@"Got cap: %@", [cap urnString]);
        } else {
            NSLog(@"Error: %@", error);
        }
    }];
    
    // Or try without failing
    [registry tryGetCap:@"cap:op=extract;target=metadata" completion:^(CSCap *cap) {
        if (cap) {
            NSLog(@"Cap available: %@", [cap urnString]);
        }
    }];
    
    // Get multiple caps at once
    NSArray<NSString *> *urns = @[
        @"cap:op=extract;target=metadata",
        @"cap:op=generate;target=thumbnail"
    ];
    
    [registry getCaps:urns completion:^(NSArray<CSCap *> *caps, NSError *error) {
        if (caps) {
            NSLog(@"Got %lu caps", (unsigned long)caps.count);
        }
    }];
    
    // Using validator for additional validation
    CSRegistryValidator *validator = [CSRegistryValidator validator];
    
    [validator getValidatedCap:@"cap:op=extract;target=metadata" completion:^(CSCap *cap, NSError *error) {
        if (cap) {
            NSLog(@"Validated cap: %@", [cap urnString]);
        } else {
            NSLog(@"Validation error: %@", error);
        }
    }];
    
    return 0;
}
```

## Plugin SDK Integration

### Go Plugin SDK

```go
package main

import (
    "log"
    sdk "github.com/fgnd/fgnd-plugin-sdk-go"
)

func main() {
    // Create registry manager
    manager, err := sdk.NewRegistryManager()
    if err != nil {
        log.Fatal(err)
    }
    
    // Get canonical cap
    cap, err := sdk.GetStandardCapByUrnCanonical("cap:op=extract;target=metadata;")
    if err != nil {
        log.Printf("Error getting canonical cap: %v", err)
    } else {
        log.Printf("Got canonical cap: %s", cap.UrnString())
    }
    
    // Validate all standard caps
    if err := sdk.ValidateStandardCaps(); err != nil {
        log.Printf("Standard caps validation failed: %v", err)
    } else {
        log.Println("All standard caps validated successfully")
    }
}
```

### Objective-C Plugin SDK

```objc
#import <FGNDPluginSDK/FGNDPluginSDK.h>

int main() {
    // Create registry manager
    FGNDRegistryManager *manager = [FGNDRegistryManager manager];
    
    // Get canonical cap
    [FGNDStandardCaps standardCapWithUrnCanonical:@"cap:op=extract;target=metadata;" completion:^(CSCap *cap, NSError *error) {
        if (cap) {
            NSLog(@"Got canonical cap: %@", [cap urnString]);
        } else {
            NSLog(@"Error getting canonical cap: %@", error);
        }
    }];
    
    // Validate all standard caps
    [FGNDStandardCaps validateStandardCaps:^(NSError *error) {
        if (error) {
            NSLog(@"Standard caps validation failed: %@", error);
        } else {
            NSLog(@"All standard caps validated successfully");
        }
    }];
    
    return 0;
}
```

## Features

### Caching
- Registry definitions are cached locally to minimize network requests
- Cache expires after 24 hours by default
- Cache uses SHA256 hash of URN as key for efficient lookups

### Validation
- Validates caps against canonical registry definitions
- Checks version, command, stdin, and other properties
- Future versions will validate arguments and output schemas

### Error Handling
- Graceful fallback when registry is unavailable
- Clear error messages for validation failures
- Local caps work even if registry validation fails

### Performance
- Local-first approach minimizes latency
- Concurrent validation for multiple caps
- Efficient caching reduces repeated network calls

## Key Benefits

### Before (Complex)
```rust
// Had to understand registry internals
let registry = CapRegistry::new().await?;
let registry_def = registry.get_cap_definition(urn).await?;
let cap = registry.create_cap_from_registry(urn).await?;
```

### After (Simple)
```rust
// Just get the cap you need
let registry = CapRegistry::new().await?;
let cap = registry.get_cap(urn).await?; // Ready to use!
```

**Benefits:**
OK **Hide complexity**: No need to understand `RegistryCapDefinition` vs `Cap`  
OK **Descriptive errors**: Clear error messages when caps aren't found  
OK **Batch operations**: Get multiple caps efficiently  
OK **Graceful handling**: `try_get_cap()` returns `None` instead of failing  
OK **Consistent API**: Same simple interface across Rust, Go, and Objective-C