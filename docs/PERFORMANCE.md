# Capns Performance

This document presents throughput measurements from the capdag cross-language interoperability test suite.

## Test Environment

- **Platform:** macOS (Darwin 25.3.0)
- **Test date:** March 2026
- **Protocol:** CBOR-encoded frames over Unix domain sockets and stdin/stdout pipes
- **Payload:** 5 MB streaming transfers per measurement
- **Test scope:** 383 tests across all language combinations

## Architecture

The test topology consists of three components:

```
Engine → Router → RelayHost → Plugin
         (UDS)      (UDS)     (stdio)
```

- **Router:** Multiplexes requests across relay hosts via Unix domain sockets
- **RelayHost:** Manages plugin lifecycle and frame routing via stdin/stdout
- **Plugin:** Processes requests and returns responses

Two router implementations were tested: Rust and Swift.

## Throughput Results

### Rust Router

| Host ↓ \ Plugin → | Rust | Go | Python | Swift |
|-------------------|------|-----|--------|-------|
| Rust | 100.32 | 85.70 | 5.79 | 76.08 |
| Go | 97.53 | 86.59 | 6.32 | 80.17 |
| Swift | 97.74 | 86.23 | 6.41 | 77.33 |

All values in MB/s.

### Swift Router

| Host ↓ \ Plugin → | Rust | Go | Python | Swift |
|-------------------|------|-----|--------|-------|
| Rust | 269.08 | 257.84 | 6.61 | 175.30 |
| Go | 228.71 | 223.41 | 6.53 | 177.78 |
| Swift | 273.43 | 264.91 | 6.57 | 181.23 |

All values in MB/s.

### Ranked Results

| Configuration | Throughput |
|---------------|------------|
| swift-swift-rust | 273.43 MB/s |
| swift-rust-rust | 269.08 MB/s |
| swift-swift-go | 264.91 MB/s |
| swift-rust-go | 257.84 MB/s |
| swift-go-rust | 228.71 MB/s |
| swift-go-go | 223.41 MB/s |
| swift-swift-swift | 181.23 MB/s |
| rust-rust-rust | 100.32 MB/s |
| rust-go-go | 86.59 MB/s |
| rust-rust-go | 85.70 MB/s |
| rust-rust-swift | 76.08 MB/s |
| swift-rust-python | 6.61 MB/s |
| rust-rust-python | 5.79 MB/s |

Configuration format: `router-host-plugin`

## Observations

1. **Router implementation matters:** The Swift router achieves 2.5-2.7x higher throughput than the Rust router for equivalent host/plugin combinations.

2. **Native plugins outperform interpreted:** Rust, Go, and Swift plugins achieve 75-275 MB/s depending on configuration. Python plugins are limited to ~6 MB/s due to interpreter overhead.

3. **Host language has minimal impact:** Within the same router, switching host languages (Rust, Go, Swift) results in <10% throughput variation for the same plugin.

4. **Go plugins perform well:** Go plugins achieve 85-265 MB/s, competitive with Rust despite garbage collection overhead.

## Comparison with Other IPC Systems

### Unix Domain Sockets (Raw)

According to [Baeldung's IPC benchmarks](https://www.baeldung.com/linux/ipc-performance-comparison), raw Unix domain sockets achieve:
- Small messages (100 bytes): ~245 Mbit/s (~30 MB/s)
- Large messages (1 MB): ~41,334 Mbit/s (~5,166 MB/s)

Capns throughput (75-275 MB/s) falls between these extremes, which is expected given the CBOR framing overhead and multi-hop routing.

### Pipes (stdin/stdout)

The [ipc-bench](https://github.com/goldsborough/ipc-bench) project reports raw pipe throughput:
- 128-byte chunks: ~1,319 Mbit/s (~165 MB/s)
- 4096-byte chunks: ~20,297 Mbit/s (~2,537 MB/s)

Capns uses pipes for plugin communication. The measured throughput (75-275 MB/s) is reasonable given CBOR encoding, checksum computation, and protocol framing.

### gRPC

Per [Nexthink's gRPC comparison](https://nexthink.com/blog/comparing-grpc-performance), gRPC throughput varies by language:
- C++ and Swift implementations show the best memory efficiency
- Throughput is typically measured in requests/second rather than MB/s

Direct comparison is difficult because gRPC benchmarks typically measure small message latency rather than bulk transfer throughput.

### Cap'n Proto RPC

According to [benchmark discussions](https://github.com/capnproto/capnproto/issues/400), Cap'n Proto achieves ~80K requests/second on localhost. Cap'n Proto focuses on zero-copy serialization for latency rather than bulk throughput.

### CBOR/MessagePack Serialization

[Serialization benchmarks](https://zderadicka.eu/comparison-of-json-like-serializations-json-vs-ubjson-vs-messagepack-vs-cbor/) show CBOR and MessagePack achieve similar throughput, typically limited by I/O rather than encoding speed for large payloads.

## Factors Affecting Throughput

1. **Frame overhead:** Each chunk includes headers (request ID, stream ID, sequence number, checksum)
2. **Checksum computation:** FNV-1a 64-bit hash on every chunk
3. **Multi-hop routing:** Data passes through router and host before reaching plugin
4. **Protocol parsing:** CBOR decode/encode at each hop
5. **Process context switches:** Plugin runs as subprocess with stdio pipes

## Python Performance

Python plugin throughput (~6 MB/s) is constrained by:
- CPython interpreter overhead
- CBOR library performance (cbor2)
- GIL contention during I/O

This is consistent with typical Python I/O-bound performance. For throughput-critical workloads, native plugins are recommended.

## Methodology

Tests use the `capdag-interop-tests` suite with:
- `pytest` test framework
- 5 MB payload per throughput test
- Warm-up request before measurement
- Single measurement per configuration (not averaged)

Results may vary based on system load, thermal conditions, and OS scheduling.
