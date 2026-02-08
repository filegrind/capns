//! Example showing full DAG execution with plugin discovery and download

use macino::{parse_dot_to_cap_dag, executor::{execute_dag, NodeData}};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} [--dev-bins <binary> ...] <dot-file> <input-node:path> [<input-node:path> ...]", args[0]);
        eprintln!();
        eprintln!("Execute a DOT graph with Cap URNs");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --dev-bins <binary> ...  Use local plugin binaries (no hash checks)");
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  dot-file          Path to DOT file");
        eprintln!("  input-node:path   Initial input nodes (e.g., pdf_input:/path/to/file.pdf)");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  {} pipeline.dot pdf_input:/tmp/test.pdf", args[0]);
        eprintln!("  {} --dev-bins ./pdfcartridge pipeline.dot pdf_input:/tmp/test.pdf", args[0]);
        process::exit(1);
    }

    // Parse arguments
    let mut dev_binaries = Vec::new();
    let mut arg_idx = 1;

    // Parse --dev-bins flag
    if args.get(arg_idx).map(|s| s.as_str()) == Some("--dev-bins") {
        arg_idx += 1;
        while arg_idx < args.len() && !args[arg_idx].starts_with("--") && !args[arg_idx].ends_with(".dot") {
            let bin_path = PathBuf::from(&args[arg_idx]);
            if !bin_path.exists() {
                eprintln!("Error: Dev binary does not exist: {}", args[arg_idx]);
                process::exit(1);
            }
            dev_binaries.push(bin_path);
            arg_idx += 1;
        }
    }

    if arg_idx >= args.len() {
        eprintln!("Error: Missing DOT file argument");
        process::exit(1);
    }

    let dot_file = &args[arg_idx];
    arg_idx += 1;

    // Parse initial inputs
    let mut initial_inputs = HashMap::new();
    for arg in &args[arg_idx..] {
        let parts: Vec<&str> = arg.split(':').collect();
        if parts.len() != 2 {
            eprintln!("Error: Invalid input format '{}'. Expected 'node:path'", arg);
            process::exit(1);
        }

        let node = parts[0].to_string();
        let path = PathBuf::from(parts[1]);

        // Verify file exists
        if !path.exists() {
            eprintln!("Error: File does not exist: {}", parts[1]);
            process::exit(1);
        }

        // Pass file path - plugin runtime will read it
        initial_inputs.insert(node, NodeData::FilePath(path));
    }

    // Read DOT file
    let dot_content = match fs::read_to_string(dot_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", dot_file, e);
            process::exit(1);
        }
    };

    println!("=== Macino: DOT Graph Execution ===\n");
    println!("DOT file: {}", dot_file);
    println!("Initial inputs: {}", initial_inputs.len());
    for (node, _) in &initial_inputs {
        println!("  - {}", node);
    }
    println!();

    // Create CapNS registry
    println!("Creating CapNS registry...");
    let registry = match capns::CapRegistry::new().await {
        Ok(reg) => reg,
        Err(e) => {
            eprintln!("Error creating CapNS registry: {}", e);
            process::exit(1);
        }
    };

    // Parse and validate
    println!("Parsing and validating DOT graph...");
    let graph = match parse_dot_to_cap_dag(&dot_content, &registry).await {
        Ok(g) => {
            println!("✓ Validation successful!");
            println!("  Nodes: {}", g.nodes.len());
            println!("  Edges: {}", g.edges.len());
            g
        }
        Err(e) => {
            eprintln!("\n✗ Validation failed: {}", e);
            process::exit(1);
        }
    };

    // Set up plugin directory
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let plugin_dir = home.join(".macino").join("plugins");

    // Registry URL
    let registry_url = "https://filegrind.com/api/plugins".to_string();

    println!("\n=== Executing DAG ===\n");
    if !dev_binaries.is_empty() {
        println!("Dev mode: {} local binaries", dev_binaries.len());
        for bin in &dev_binaries {
            println!("  - {:?}", bin);
        }
        println!();
    }
    println!("Plugin directory: {:?}", plugin_dir);
    println!("Registry URL: {}\n", registry_url);

    // Execute the DAG
    match execute_dag(&graph, plugin_dir, registry_url, initial_inputs, dev_binaries).await {
        Ok(outputs) => {
            println!("\n=== Execution Results ===\n");
            for (node, data) in outputs {
                println!("Node '{}':", node);
                match data {
                    NodeData::Bytes(ref b) => println!("  {} bytes", b.len()),
                    NodeData::Text(ref t) => println!("  {} chars: {}", t.len(), 
                        if t.len() > 100 { &t[..100] } else { t }),
                    NodeData::FilePath(ref p) => println!("  File: {:?}", p),
                }
            }
            println!("\n✓ Execution completed successfully!");
        }
        Err(e) => {
            eprintln!("\n✗ Execution failed: {}", e);
            process::exit(1);
        }
    }
}
