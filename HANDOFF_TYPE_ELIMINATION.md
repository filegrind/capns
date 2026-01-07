# Handoff: Eliminate ArgumentType/OutputType Enums - Replace with MediaSpec

## Progress Update (January 2026)

### ✅ COMPLETED PHASES

#### Phase 1: capns (Rust) - COMPLETE ✅
- ✅ Removed `ArgumentType` and `OutputType` enums from `cap.rs`
- ✅ Updated `CapArgument` to use `media_spec: String` field
- ✅ Updated `CapOutput` to use `media_spec: String` field
- ✅ Created `profile_schema_cache.rs` module for downloading/caching JSON Schema profiles
- ✅ Updated `validation.rs` to use `ProfileSchemaRegistry` for profile-based validation
- ✅ Updated `schema_validation.rs` to only validate explicit schemas
- ✅ Updated `caller.rs` with basic validation (async validation at higher level)
- ✅ Updated `response.rs` to use media_spec-based type checking
- ✅ Updated `caphost_registry.rs` test code to use media_spec
- ✅ Updated `lib.rs` exports
- ✅ **Rust code compiles successfully** with only minor dead code warnings

#### Phase 4: capns-js - COMPLETE ✅
- ✅ Updated `InputValidator.validateArgumentType()` to use MediaSpec parsing
- ✅ Added `validateAgainstProfile()` for basic profile validation
- ✅ Updated `OutputValidator.validateOutput()` to use MediaSpec
- ✅ Updated `ValidationError` messages to include media_spec information
- ✅ Leveraged existing `MediaSpec` class for parsing

#### Phase 5: capns_dot_org Website - COMPLETE ✅
- ✅ Updated `cap.schema.json` to require `media_spec` instead of `arg_type`/`output_type`
- ✅ Updated all 52 TOML capability definition files to use `media_spec`
- ✅ Removed deprecated `cap_output_type` metadata fields
- ✅ Updated `capns.js` (synced from capns-js)
- ✅ Verified `load-standards.js` (no changes needed - just copies fields)
- ✅ **All 52 JSON files generated successfully** from TOML
- ✅ Updated validation in `functions/api-admin-capabilities.js`
- ✅ Updated UI display in `scripts/main.js` with `getTypeFromMediaSpec()` helper
- ✅ Updated UI display in `scripts/cap-navigator.js` with `getTypeFromMediaSpec()` method

### 📝 Implementation Details

**Profile Schema Cache Pattern:**
- Embeds standard JSON schemas for common types (str, int, num, bool, obj, arrays)
- Downloads and caches schemas from profile URLs on-demand
- Two-level cache: disk cache and in-memory compiled schemas
- Cache TTL: 1 week for downloaded schemas
- Follows same pattern as CapRegistry for consistency

**MediaSpec Format in TOML:**
```toml
media_spec = "content-type: application/json; profile=https://capns.org/schemas/str"
```
Note: Profile URLs are unquoted in TOML (quotes only around the entire media_spec string)

**MediaSpec Format in JSON:**
```json
"media_spec": "content-type: application/json; profile=https://capns.org/schemas/str"
```

#### Phase 2: capns-go (Golang) - COMPLETE ✅
- ✅ Deleted `ArgumentType` and `OutputType` enums from `cap.go`
- ✅ Updated `CapArgument` to use `MediaSpec string` field
- ✅ Updated `CapOutput` to use `MediaSpec string` field
- ✅ Updated all factory methods to accept `mediaSpec string` parameter
- ✅ Updated `validation.go` to use MediaSpec parsing for type validation
- ✅ Created `validateValueAgainstProfile()` function for profile-based validation
- ✅ Updated `schema_validation.go` to check MediaSpec instead of type enums
- ✅ Updated `cap_caller.go` to use MediaSpec from cap definition
- ✅ Updated `response_wrapper.go` to use MediaSpec validation
- ✅ Updated all test files with correct media spec strings
- ✅ Updated example files with correct media spec strings
- ✅ **Go code compiles successfully and all tests pass**

### 🚧 REMAINING PHASES

- ⏸️ Phase 3: capns-objc (Objective-C)
- ⏸️ Phase 6-15: Downstream projects (fgrnd, fgrnd-mac, plugin SDKs, provider SDKs, czar services)

---

## Executive Summary

This task eliminates the `ArgumentType` and `OutputType` enums from all capns implementations and replaces them with `MediaSpec` usage. The new approach uses JSON Schema profiles for type validation, providing better extensibility and standardized validation.

## Types to Eliminate

### Location: `/Users/bahram/ws/prj/fgrnd-ws/capns/src/cap.rs`

**Lines 14-22: `ArgumentType` enum**
```rust
pub enum ArgumentType {
    String,
    Integer,
    Number,
    Boolean,
    Array,
    Object,
    Binary,
}
```

**Lines 105-113: `OutputType` enum**
```rust
pub enum OutputType {
    String,
    Integer,
    Number,
    Boolean,
    Array,
    Object,
    Binary,
}
```

## Replacement: MediaSpec with JSON Schema Profiles

### New Standard MediaSpec Values (defined in `capns/src/standard/media.rs`)

| Old Type | New MediaSpec | Content-Type | Profile URL |
|----------|---------------|--------------|-------------|
| `String` | `MEDIA_STRING` | `application/json` | `https://capns.org/schemas/str` |
| `Integer` | `MEDIA_INTEGER` | `application/json` | `https://capns.org/schemas/int` |
| `Number` | `MEDIA_NUMBER` | `application/json` | `https://capns.org/schemas/num` |
| `Boolean` | `MEDIA_BOOLEAN` | `application/json` | `https://capns.org/schemas/bool` |
| `Object` | `MEDIA_JSON_OBJECT` | `application/json` | `https://capns.org/schemas/obj` |
| `Array` (strings) | `MEDIA_STRING_ARRAY` | `application/json` | `https://capns.org/schemas/str-array` |
| `Array` (numbers) | `MEDIA_NUMBER_ARRAY` | `application/json` | `https://capns.org/schemas/num-array` |
| `Array` (booleans) | `MEDIA_BOOLEAN_ARRAY` | `application/json` | `https://capns.org/schemas/bool-array` |
| `Array` (objects) | `MEDIA_JSON_OBJECT_ARRAY` | `application/json` | `https://capns.org/schemas/obj-array` |
| `Binary` | `MEDIA_OCTET_STREAM` | `application/octet-stream` | (none) |

### JSON Schema Files (already created)
Location: `/Users/bahram/ws/prj/fgrnd-ws/capns_dot_org/schema/`
- `str.json`, `int.json`, `num.json`, `bool.json`, `obj.json`
- `str-array.json`, `num-array.json`, `bool-array.json`, `obj-array.json`

---

## Phase 1: Update capns (Rust) - Reference Implementation

### Files to Modify

#### 1. `capns/src/cap.rs`
- **DELETE**: `ArgumentType` enum (lines 14-22)
- **DELETE**: `OutputType` enum (lines 105-113)
- **MODIFY**: `CapArgument` struct - replace `arg_type: ArgumentType` with `media_spec: String`
- **MODIFY**: `CapOutput` struct - replace `output_type: OutputType` with `media_spec: String`
- **UPDATE**: All constructors and factory methods to use `media_spec` instead of type enums
- **UPDATE**: Serialization/deserialization to use `media_spec` field

#### 2. `capns/src/validation.rs`
- **REWRITE**: `validate_argument_type()` to parse MediaSpec and validate against JSON Schema
- **REWRITE**: `validate_output_type()` to parse MediaSpec and validate against JSON Schema
- **REMOVE**: All `match` statements on `ArgumentType`/`OutputType`

#### 3. `capns/src/schema_validation.rs`
- **UPDATE**: Schema resolution to use profile URLs from MediaSpec
- **UPDATE**: Validation to derive expected type from MediaSpec

#### 4. `capns/src/caller.rs`
- **UPDATE**: `is_binary_cap()` and `is_json_cap()` already use MediaSpec (done)
- **VERIFY**: No remaining references to old type enums

#### 5. `capns/src/response.rs`
- **UPDATE**: Response validation to use MediaSpec instead of OutputType

#### 6. `capns/src/caphost_registry.rs`
- **UPDATE**: Any type references

#### 7. `capns/src/lib.rs`
- **UPDATE**: Exports - remove `ArgumentType`, `OutputType` from public API
- **ADD**: Export MediaSpec convenience functions from `standard::media`

### New Structures

```rust
// In cap.rs - replace arg_type field
pub struct CapArgument {
    pub name: String,
    pub media_spec: String,  // e.g., "content-type: application/json; profile=\"https://capns.org/schemas/str\""
    pub arg_description: String,
    pub cli_flag: String,
    // ... rest unchanged
}

// In cap.rs - replace output_type field
pub struct CapOutput {
    pub media_spec: String,  // e.g., "content-type: application/json; profile=\"https://capns.org/schemas/obj\""
    pub output_description: String,
    // ... rest unchanged, remove content_type field (now in media_spec)
}
```

---

## Phase 2: Update capns-go

### Files to Modify

#### 1. `capns-go/cap.go`
- **DELETE**: `ArgumentType` type and constants
- **DELETE**: `OutputType` type and constants
- **MODIFY**: `CapArgument` struct - replace `ArgType` with `MediaSpec string`
- **MODIFY**: `CapOutput` struct - replace `OutputType` with `MediaSpec string`

#### 2. `capns-go/validation.go`
- **REWRITE**: Type validation to use MediaSpec parsing
- **REMOVE**: Type enum matching

#### 3. `capns-go/schema_validation.go`
- **UPDATE**: Schema resolution from MediaSpec profiles

#### 4. `capns-go/cap_caller.go`
- **VERIFY**: Already uses MediaSpec for binary/JSON detection (done)

#### 5. `capns-go/response_wrapper.go`
- **UPDATE**: Response type detection to use MediaSpec

#### 6. Test files
- `cap_caller_test.go`
- `response_wrapper_test.go`
- `schema_validation_test.go`
- `integration_test.go`

#### 7. Example files
- `examples/example_schema_usage.go`
- `examples/plugin_sdk_example.go`

---

## Phase 3: Update capns-objc

### Files to Modify

#### 1. `capns-objc/Sources/CapNs/include/CSCap.h`
- **DELETE**: `CSArgumentType` enum
- **DELETE**: `CSOutputType` enum
- **MODIFY**: `CSCapArgument` - replace `argType` with `mediaSpec` property
- **MODIFY**: `CSCapOutput` - replace `outputType` with `mediaSpec` property

#### 2. `capns-objc/Sources/CapNs/CSCap.m`
- **UPDATE**: Implementation to use `mediaSpec`

#### 3. `capns-objc/Sources/CapNs/include/CSCapValidator.h`
- **UPDATE**: Validation method signatures

#### 4. `capns-objc/Sources/CapNs/CSCapValidator.m`
- **REWRITE**: Type validation to use MediaSpec

#### 5. `capns-objc/Sources/CapNs/CSSchemaValidator.m`
- **UPDATE**: Schema resolution from MediaSpec

#### 6. `capns-objc/Sources/CapNs/CSCapCaller.m`
- **VERIFY**: Already uses CSMediaSpec (done)

#### 7. `capns-objc/Sources/CapNs/include/CSResponseWrapper.h`
- **DELETE**: References to CSOutputType

#### 8. `capns-objc/Sources/CapNs/CSResponseWrapper.m`
- **UPDATE**: Type detection to use MediaSpec

#### 9. Test files
- `Tests/CapNsTests/CSCapTests.m`
- `Tests/CapNsTests/CSSchemaValidationTests.m`

#### 10. Example files
- `examples/schema_validation_example.m`

---

## Phase 4: Update capns-js

### Files to Modify

#### 1. `capns-js/capns.js`
- **DELETE**: Any ArgumentType/OutputType equivalents in validation code
- **MODIFY**: `Cap` class - use `mediaSpec` instead of type enums
- **MODIFY**: `InputValidator` - validate using MediaSpec
- **MODIFY**: `OutputValidator` - validate using MediaSpec
- **VERIFY**: MediaSpec class already added (done)

#### 2. `capns-js/capns.test.js`
- **UPDATE**: All tests to use MediaSpec

---

## Phase 5: Update capns_dot_org Website

### Files to Modify

#### 1. `capns_dot_org/capns.js`
- **SYNC**: With main `capns-js/capns.js` implementation

#### 2. `capns_dot_org/standard/load-standards.js`
- **UPDATE**: TOML loading to use `media_spec` for arguments/outputs

#### 3. Standard TOML files in `capns_dot_org/standard/`
- **UPDATE ALL**: Replace `arg_type = "string"` with `media_spec = "content-type: application/json; profile=\"https://capns.org/schemas/str\""`
- **UPDATE ALL**: Replace `output_type = "object"` with `media_spec = "content-type: application/json; profile=\"https://capns.org/schemas/obj\""`

#### 4. API endpoints (if any server-side code)
- **UPDATE**: Cap registration/retrieval to use MediaSpec

#### 5. UI Components
- **UPDATE**: Cap editor/viewer to display MediaSpec instead of type dropdowns
- **UPDATE**: Validation feedback to show MediaSpec-based errors

---

## Phase 6: Update fgrnd (Main Application)

### Files to Modify

#### 1. Proto files
- `fgrnd/proto/cap.proto` - Update message definitions
- `fgrnd/proto/block.proto` - Update any type references

#### 2. gRPC Services
- `fgrnd/src/grpc/service/block_grpc_service.rs`

#### 3. Persistence Layer
- `fgrnd/src/persist/models/block.rs`
- `fgrnd/src/persist/repos/block_repo.rs`
- `fgrnd/src/persist/logic/block_logic.rs`
- `fgrnd/src/persist/logic/cap_logic.rs`
- `fgrnd/src/persist/logic/schema_deduction.rs`

#### 4. Operations
- `fgrnd/src/ops/create_block.rs`

#### 5. Tools
- `fgrnd/src/tools/chip_extraction/mod.rs`

#### 6. Prelude
- `fgrnd/src/prelude.rs` - Update re-exports

---

## Phase 7: Update fgrnd-mac

### Files to Modify

#### 1. Generated Proto Files (regenerate after proto updates)
- `FileGrindSDK/Sources/FileGrindSDK/Generated/cap.pb.swift`
- `FileGrindSDK/Sources/FileGrindSDK/Generated/block.pb.swift`

#### 2. SDK Files
- `FileGrindSDK/Sources/FileGrindSDK/gRPC/FileGrindEngineGRPCClient.swift`
- `FileGrindSDK/Sources/FileGrindSDK/API/FileGrindEngineAPI.swift`

#### 3. Core UI Files
- `FileGrindCore/Sources/FileGrind/Views/Cards/BlockCardView.swift`
- `FileGrindCore/Sources/FileGrind/Views/Components/ApplyCapTaskView.swift`
- `FileGrindCore/Sources/FileGrind/Extensions/SDKBlockOutputType+Extensions.swift` - **DELETE** this file

#### 4. Plugin XPC
- `FileGrind/PluginXPCService/PluginXPCServiceImplementation.swift`

---

## Phase 8: Update fgrnd-plugin-sdk (Rust)

### Files to Modify

#### 1. `fgrnd-plugin-sdk/src/validation.rs`
- **UPDATE**: All validation to use MediaSpec

---

## Phase 9: Update fgrnd-plugin-sdk-go

### Files to Modify

#### 1. `fgrnd-plugin-sdk-go/sdk.go`
- **UPDATE**: Cap definitions to use MediaSpec

#### 2. `fgrnd-plugin-sdk-go/standard.go`
- **UPDATE**: Standard caps to use MediaSpec

---

## Phase 10: Update fgrnd-plugin-sdk-objc

### Files to Modify

#### 1. `fgrnd-plugin-sdk-objc/Sources/FGRNDPluginSDK/FGRNDStandardCaps.m`
- **UPDATE**: Cap definitions to use MediaSpec

#### 2. `fgrnd-plugin-sdk-objc/Sources/FGRNDPluginSDK/FGRNDSchemaValidation.m`
- **UPDATE**: Validation to use MediaSpec

#### 3. `fgrnd-plugin-sdk-objc/Sources/FGRNDPluginSDK/FGRNDPluginSDK.m`
- **UPDATE**: Plugin interface to use MediaSpec

---

## Phase 11: Update fgrnd-provider-sdk (Rust)

### Files to Modify

#### 1. All source files in `fgrnd-provider-sdk/src/`
- **UPDATE**: Any cap definitions to use MediaSpec
- **UPDATE**: Any validation logic to use MediaSpec
- **VERIFY**: Dependency on capns is updated

---

## Phase 12: Update fgrnd-provider-sdk-objc

### Files to Modify

#### 1. All source files in `fgrnd-provider-sdk-objc/Sources/`
- **UPDATE**: Any cap definitions to use MediaSpec
- **UPDATE**: Any validation logic to use MediaSpec
- **VERIFY**: Dependency on capns-objc is updated

---

## Phase 13: Update pdfczar

### Files to Modify

#### 1. `pdfczar/pdfium-render-bundled/src/bindings/wasm.rs`
- **CHECK**: Any type references and update if needed

---

## Phase 14: Update embeddingczar

### Files to Modify

#### 1. All source files in `embeddingczar/src/`
- **UPDATE**: Any cap definitions to use MediaSpec
- **UPDATE**: Any validation logic to use MediaSpec
- **VERIFY**: Dependency on capns is updated

---

## Phase 15: Update modelczar

### Files to Modify

#### 1. All source files in `modelczar/src/`
- **UPDATE**: Any cap definitions to use MediaSpec
- **UPDATE**: Any validation logic to use MediaSpec
- **VERIFY**: Dependency on capns is updated

---

## Migration Checklist

### For Each File:

1. [ ] Search for `ArgumentType` - replace all occurrences
2. [ ] Search for `OutputType` - replace all occurrences
3. [ ] Search for `arg_type` - replace with `media_spec`
4. [ ] Search for `output_type` - replace with `media_spec`
5. [ ] Search for type string literals (`"string"`, `"integer"`, `"number"`, `"boolean"`, `"array"`, `"object"`, `"binary"`) in type contexts
6. [ ] Update all constructors/factory methods
7. [ ] Update all serialization/deserialization
8. [ ] Update all validation logic
9. [ ] Update all tests
10. [ ] Run tests and fix failures

### Validation Tests to Add:

For each implementation, add tests that verify:
1. MediaSpec parsing extracts correct content-type and profile
2. Type validation works by fetching and validating against JSON Schema
3. Binary detection works (content-type starts with `image/`, `audio/`, `video/`, `application/octet-stream`)
4. JSON detection works (content-type is `application/json` or ends with `+json`)
5. Schema validation against profile URLs works

---

## Breaking Changes

This is a **breaking change** for all consumers of the capns libraries. The following will break:

1. Any code that constructs `CapArgument` with `arg_type`
2. Any code that constructs `CapOutput` with `output_type`
3. Any code that matches on `ArgumentType` or `OutputType` enums
4. Any serialized cap definitions using `arg_type` or `output_type` fields
5. Any TOML files using the old format

---

## Non-Goals (Explicitly NOT doing)

1. ❌ No backward compatibility shims
2. ❌ No fallback to old type enums
3. ❌ No migration scripts (consumers must update)
4. ❌ No deprecation period
5. ❌ No dual-format support

---

## Success Criteria

1. All `ArgumentType` and `OutputType` references removed from codebase
2. All cap definitions use `media_spec` field
3. All validation uses MediaSpec parsing and JSON Schema profiles
4. All tests pass in all implementations (Rust, Go, ObjC, JS)
5. All downstream projects (fgrnd, fgrnd-mac, plugin SDKs) compile and tests pass
6. capns_dot_org website displays and edits MediaSpec correctly

---

## Order of Operations

1. **capns (Rust)** - Reference implementation, must be done first
2. **capns-go** - Follow Rust patterns exactly
3. **capns-objc** - Follow Rust patterns exactly
4. **capns-js** - Follow Rust patterns exactly
5. **capns_dot_org** - Update website and TOML files
6. **fgrnd** - Update main application
7. **fgrnd-mac** - Update macOS application
8. **fgrnd-plugin-sdk (Rust)** - Update Rust plugin SDK
9. **fgrnd-plugin-sdk-go** - Update Go plugin SDK
10. **fgrnd-plugin-sdk-objc** - Update ObjC plugin SDK
11. **fgrnd-provider-sdk (Rust)** - Update Rust provider SDK
12. **fgrnd-provider-sdk-objc** - Update ObjC provider SDK
13. **pdfczar** - Update PDF processing tool
14. **embeddingczar** - Update embedding service
15. **modelczar** - Update model management service
16. **Final verification** - End-to-end testing

---

## Estimated Scope

- **~60 files** across all projects
- **~500-1000 lines** of type enum references to replace
- **All validation logic** needs rewriting
- **All tests** need updating

---

## Key Principles

1. **Fail hard** - If MediaSpec parsing fails, error immediately, no silent fallbacks
2. **No placeholders** - Every change is production-ready
3. **Follow implications** - If a change breaks something downstream, fix it
4. **Single source of truth** - MediaSpec string contains all type information
5. **Schema-based validation** - Use JSON Schema profiles for actual validation
