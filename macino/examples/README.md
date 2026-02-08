# Macino Examples

This directory contains example DOT graph files demonstrating CapNS orchestration with macino.

## Examples

### 1. pdf-processing.dot
A simple PDF document processing pipeline that:
- Extracts text pages from PDF using `disbind`
- Generates a thumbnail preview using `generate_thumbnail`
- Extracts document outline/table of contents

### 2. text-processing.dot
Text file processing pipeline that:
- Extracts text chips using `disbind`
- Generates thumbnail preview

### 3. multi-format.dot
Demonstrates how different document formats (PDF, TXT) can converge to common processing:
- Multiple input formats flow into the same page extraction
- Each format gets its own thumbnail

## Running Examples

To parse and validate these examples, use the CLI tool:

```bash
cargo run --example parse_dot examples/pdf-processing.dot
```

## DOT Format

All examples use DOT digraph syntax with Cap URNs in edge labels:
- Edge labels must start with `cap:`
- Node media URNs are derived from incident caps
- All caps must be registered in CapNS

Example:
```dot
digraph G {
  input -> output [label="cap:in=\"media:pdf;bytes\";op=disbind;out=\"media:disbound-page;textable;form=list\""];
}
```

## Validation

Macino enforces strict validation:
- All edge labels must be valid Cap URNs
- All caps must exist in the registry
- Node media URNs must be consistent across incident edges
- Graph must be a DAG (no cycles)
