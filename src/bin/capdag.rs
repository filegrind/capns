//! capdag: DOT-based DAG executor for Cap pipelines
//!
//! A unified CLI for executing and validating DOT graph pipelines.

use capdag::orchestrator::{parse_dot_to_cap_dag, execute_dag, NodeData};
use capdag::CapRegistry;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process;

/// Expand dev binary path - supports single file or directory of executables
fn expand_dev_binary_path(path: &str) -> Vec<PathBuf> {
    let path_buf = PathBuf::from(path);

    if path_buf.is_file() {
        vec![path_buf]
    } else if path_buf.is_dir() {
        // Find all executable files in directory
        match fs::read_dir(&path_buf) {
            Ok(entries) => {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| {
                        if !p.is_file() {
                            return false;
                        }
                        // Check if executable (unix)
                        if let Ok(meta) = p.metadata() {
                            let mode = meta.permissions().mode();
                            // Check if any execute bit is set
                            mode & 0o111 != 0
                        } else {
                            false
                        }
                    })
                    .collect()
            }
            Err(e) => {
                eprintln!("Error reading dev-bins directory '{}': {}", path, e);
                vec![]
            }
        }
    } else {
        eprintln!("Error: Dev binary path does not exist: {}", path);
        vec![]
    }
}

/// Find input nodes in the DOT graph (nodes with no incoming edges)
fn find_input_nodes(dot_content: &str) -> Vec<String> {
    // Simple parser: find nodes that appear as sources but never as targets
    let mut sources: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut targets: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in dot_content.lines() {
        let line = line.trim();
        if line.contains("->") {
            // Parse: source -> target [label=...]
            if let Some(arrow_pos) = line.find("->") {
                let source = line[..arrow_pos].trim().to_string();
                let rest = &line[arrow_pos + 2..];
                // Target is everything before [ or ;
                let target = rest
                    .split(|c| c == '[' || c == ';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();

                if !source.is_empty() {
                    sources.insert(source);
                }
                if !target.is_empty() {
                    targets.insert(target);
                }
            }
        }
    }

    // Input nodes are sources that are never targets
    sources
        .difference(&targets)
        .cloned()
        .collect()
}

/// File extensions to skip when expanding directories
const SKIP_EXTENSIONS: &[&str] = &[
    "json", "log", "txt", "md", "yml", "yaml", "toml",
    "sh", "py", "rb", "js", "ts", "rs", "go", "c", "h", "cpp",
    "zip", "tar", "gz", "bz2", "xz",
];

/// Files to always skip
const SKIP_FILES: &[&str] = &[".DS_Store", "Thumbs.db", ".gitignore", ".gitkeep"];

/// Check if a file should be included based on extension/name
fn should_include_file(path: &PathBuf) -> bool {
    let filename = path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    // Skip hidden files and known skip files
    if filename.starts_with('.') || SKIP_FILES.contains(&filename) {
        return false;
    }

    // Skip directories
    if path.is_dir() {
        return false;
    }

    // Skip known non-content extensions
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        if SKIP_EXTENSIONS.contains(&ext.to_lowercase().as_str()) {
            return false;
        }
    }

    true
}

/// Expand input path to list of files
/// Supports: single file, directory, glob pattern
fn expand_input_path(path: &str) -> Vec<PathBuf> {
    let path_buf = PathBuf::from(path);

    // Check if it's a glob pattern (contains * or ?)
    if path.contains('*') || path.contains('?') {
        match glob::glob(path) {
            Ok(entries) => {
                let files: Vec<PathBuf> = entries
                    .filter_map(|e| e.ok())
                    .filter(|p| p.is_file())
                    .collect();
                if files.is_empty() {
                    eprintln!("Warning: No files matched glob pattern '{}'", path);
                }
                files
            }
            Err(e) => {
                eprintln!("Error parsing glob pattern '{}': {}", path, e);
                vec![]
            }
        }
    } else if path_buf.is_dir() {
        // Directory: list content files (non-recursive), filtering out non-content
        match fs::read_dir(&path_buf) {
            Ok(entries) => {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| should_include_file(p))
                    .collect()
            }
            Err(e) => {
                eprintln!("Error reading directory '{}': {}", path, e);
                vec![]
            }
        }
    } else if path_buf.is_file() {
        vec![path_buf]
    } else {
        eprintln!("Error: Path does not exist: {}", path);
        vec![]
    }
}

fn print_usage(program: &str) {
    eprintln!("Usage: {} [options] <dot-file> <input-paths...>", program);
    eprintln!();
    eprintln!("Execute a DOT graph pipeline on input files.");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --dev-bins <binary> ...  Use local plugin binaries");
    eprintln!("  --help                   Show this help");
    eprintln!();
    eprintln!("Input paths can be:");
    eprintln!("  - Single file:   /path/to/file.pdf");
    eprintln!("  - Directory:     /path/to/pdfs/");
    eprintln!("  - Glob pattern:  /path/to/*.pdf");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} pipeline.dot /tmp/test.pdf", program);
    eprintln!("  {} pipeline.dot /tmp/pdfs/", program);
    eprintln!("  {} pipeline.dot '/tmp/*.pdf'", program);
    eprintln!("  {} --dev-bins ./pdfcartridge pipeline.dot /tmp/*.pdf", program);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage(&args[0]);
        process::exit(1);
    }

    // Parse arguments
    let mut dev_binaries = Vec::new();
    let mut arg_idx = 1;

    // Parse flags
    while arg_idx < args.len() {
        match args[arg_idx].as_str() {
            "--help" | "-h" => {
                print_usage(&args[0]);
                process::exit(0);
            }
            "--dev-bins" => {
                arg_idx += 1;
                while arg_idx < args.len()
                    && !args[arg_idx].starts_with("--")
                    && !args[arg_idx].ends_with(".dot")
                {
                    let expanded = expand_dev_binary_path(&args[arg_idx]);
                    if expanded.is_empty() {
                        eprintln!("Error: No executables found in: {}", args[arg_idx]);
                        process::exit(1);
                    }
                    dev_binaries.extend(expanded);
                    arg_idx += 1;
                }
            }
            _ => break,
        }
    }

    if arg_idx >= args.len() {
        eprintln!("Error: Missing DOT file argument");
        print_usage(&args[0]);
        process::exit(1);
    }

    let dot_file = &args[arg_idx];
    arg_idx += 1;

    // Read DOT file
    let dot_content = match fs::read_to_string(dot_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading DOT file '{}': {}", dot_file, e);
            process::exit(1);
        }
    };

    // Find input nodes automatically
    let input_nodes = find_input_nodes(&dot_content);
    if input_nodes.is_empty() {
        eprintln!("Error: No input nodes found in DOT graph");
        process::exit(1);
    }

    // Collect all input paths and expand them
    let mut all_files: Vec<PathBuf> = Vec::new();
    for arg in &args[arg_idx..] {
        let expanded = expand_input_path(arg);
        all_files.extend(expanded);
    }

    if all_files.is_empty() {
        eprintln!("Error: No input files found");
        process::exit(1);
    }

    // Sort files for consistent ordering
    all_files.sort();

    // For now, use the first input node for all files
    // TODO: Support multiple input nodes with explicit mapping
    let input_node = &input_nodes[0];

    println!("=== capdag: DOT Graph Execution ===\n");
    println!("DOT file: {}", dot_file);
    println!("Input node: {}", input_node);
    println!("Input files: {}", all_files.len());
    for f in &all_files {
        println!("  - {}", f.display());
    }
    println!();

    // Create CapNS registry
    println!("Creating CapNS registry...");
    let registry = match CapRegistry::new().await {
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
            println!("Validation successful");
            println!("  Nodes: {}", g.nodes.len());
            println!("  Edges: {}", g.edges.len());
            g
        }
        Err(e) => {
            eprintln!("\nValidation failed: {}", e);
            process::exit(1);
        }
    };

    // Set up plugin directory
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let plugin_dir = home.join(".capdag").join("plugins");

    // Registry URL
    let registry_url = "https://machinefabric.com/api/plugins".to_string();

    println!("\n=== Executing DAG ===\n");
    if !dev_binaries.is_empty() {
        println!("Dev mode: {} local binaries", dev_binaries.len());
        for bin in &dev_binaries {
            println!("  - {}", bin.display());
        }
        println!();
    }

    // Process each file
    let mut success_count = 0;
    let mut error_count = 0;

    for file in &all_files {
        println!("--- Processing: {} ---", file.display());

        let mut initial_inputs = HashMap::new();
        initial_inputs.insert(input_node.clone(), NodeData::FilePath(file.clone()));

        match execute_dag(&graph, plugin_dir.clone(), registry_url.clone(), initial_inputs, dev_binaries.clone()).await {
            Ok(outputs) => {
                println!("Results:");
                for (node, data) in outputs {
                    match data {
                        NodeData::Bytes(ref b) => println!("  {}: {} bytes", node, b.len()),
                        NodeData::Text(ref t) => {
                            let preview = if t.len() > 80 { &t[..80] } else { t };
                            println!("  {}: {}", node, preview.replace('\n', " "));
                        }
                        NodeData::FilePath(ref p) => println!("  {}: {}", node, p.display()),
                    }
                }
                success_count += 1;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                error_count += 1;
            }
        }
        println!();
    }

    println!("=== Summary ===");
    println!("Processed: {}", all_files.len());
    println!("Success: {}", success_count);
    println!("Errors: {}", error_count);

    if error_count > 0 {
        process::exit(1);
    }
}
