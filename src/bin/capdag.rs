//! capdag: Machine notation DAG executor for Cap pipelines
//!
//! A unified CLI for executing and validating machine notation pipelines.

use capdag::orchestrator::{parse_machine_to_cap_dag, execute_dag, NodeData};
use capdag::machine::Machine;
use capdag::{CapProgressFn, CapRegistry};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

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
        eprintln!("Dev binary path does not exist: {}", path);
        vec![]
    }
}

/// Find input nodes in the machine notation (root sources with no incoming edges).
///
/// Parses the machine notation into a Machine and returns the node names
/// that are root sources (not produced by any cap).
fn find_input_nodes(notation: &str) -> Vec<String> {
    let graph = match Machine::from_string(notation) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Failed to parse machine notation for input node detection: {}", e);
            return vec![];
        }
    };

    // Re-parse to get node names — Machine discards them.
    // Use the same pest extraction as the orchestrator.
    use pest::Parser;
    use capdag::machine::parser::{MachineParser, Rule};

    let pairs = match MachineParser::parse(Rule::program, notation.trim()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to re-parse machine notation: {}", e);
            return vec![];
        }
    };

    // Collect all source and target node names from wirings
    let mut all_sources: Vec<String> = Vec::new();
    let mut all_targets: Vec<String> = Vec::new();

    let program = pairs.into_iter().next().unwrap();
    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue;
        }
        let inner = pair.into_inner().next().unwrap();
        let content = inner.into_inner().next().unwrap();
        if content.as_rule() != Rule::wiring {
            continue;
        }

        let mut inner_pairs = content.into_inner();

        // Source
        let source_pair = inner_pairs.next().unwrap();
        let source_inner = source_pair.into_inner().next().unwrap();
        match source_inner.as_rule() {
            Rule::group => {
                for p in source_inner.into_inner() {
                    if p.as_rule() == Rule::alias {
                        all_sources.push(p.as_str().to_string());
                    }
                }
            }
            Rule::alias => {
                all_sources.push(source_inner.as_str().to_string());
            }
            _ => {}
        }

        // Skip arrow, loop_cap, arrow
        inner_pairs.next();
        inner_pairs.next();
        inner_pairs.next();

        // Target
        let target = inner_pairs.next().unwrap().as_str().to_string();
        all_targets.push(target);
    }

    // Input nodes are sources that never appear as targets
    let target_set: std::collections::HashSet<&str> =
        all_targets.iter().map(|s| s.as_str()).collect();

    let mut seen = std::collections::HashSet::new();
    all_sources
        .into_iter()
        .filter(|s| !target_set.contains(s.as_str()) && seen.insert(s.clone()))
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
                    eprintln!("No files matched glob pattern '{}'", path);
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
        eprintln!("Path does not exist: {}", path);
        vec![]
    }
}


fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} [options] <machine-file> [input-paths...]\n\n\
         Execute a machine notation pipeline on input files.\n\n\
         Options:\n\
           --mermaid                Output Mermaid diagram code and exit\n\
           --dev-bins <binary> ...  Use local plugin binaries\n\
           --help                   Show this help\n\n\
         Input paths can be:\n\
           - Single file:   /path/to/file.pdf\n\
           - Directory:     /path/to/pdfs/\n\
           - Glob pattern:  /path/to/*.pdf\n\n\
         Examples:\n\
           {} --mermaid pipeline.machine\n\
           {} pipeline.machine /tmp/test.pdf\n\
           {} pipeline.machine /tmp/pdfs/\n\
           {} --dev-bins ./pdfcartridge pipeline.machine /tmp/*.pdf",
        program, program, program, program, program
    );
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
    let mut mermaid_mode = false;
    let mut arg_idx = 1;

    // Parse flags
    while arg_idx < args.len() {
        match args[arg_idx].as_str() {
            "--help" | "-h" => {
                print_usage(&args[0]);
                process::exit(0);
            }
            "--mermaid" => {
                mermaid_mode = true;
                arg_idx += 1;
            }
            "--dev-bins" => {
                arg_idx += 1;
                while arg_idx < args.len()
                    && !args[arg_idx].starts_with("--")
                    && !args[arg_idx].ends_with(".machine")
                {
                    let expanded = expand_dev_binary_path(&args[arg_idx]);
                    if expanded.is_empty() {
                        eprintln!("No executables found in: {}", args[arg_idx]);
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
        eprintln!("Missing machine file argument");
        print_usage(&args[0]);
        process::exit(1);
    }

    let machine_file = &args[arg_idx];
    arg_idx += 1;

    // Read machine file
    let notation = match fs::read_to_string(machine_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading machine file '{}': {}", machine_file, e);
            process::exit(1);
        }
    };

    // Create CapDag registry
    let registry = match CapRegistry::new().await {
        Ok(reg) => Arc::new(reg),
        Err(e) => {
            eprintln!("Error creating CapDag registry: {}", e);
            process::exit(1);
        }
    };

    // Parse and validate machine notation
    let graph = match parse_machine_to_cap_dag(&notation, registry.as_ref()).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Validation failed: {}", e);
            process::exit(1);
        }
    };

    // --mermaid: output diagram and exit
    if mermaid_mode {
        println!("{}", graph.to_mermaid());
        process::exit(0);
    }

    // Find input nodes automatically
    let input_nodes = find_input_nodes(&notation);
    if input_nodes.is_empty() {
        eprintln!("No input nodes found in machine notation");
        process::exit(1);
    }

    // Collect all input paths and expand them
    let mut all_files: Vec<PathBuf> = Vec::new();
    for arg in &args[arg_idx..] {
        let expanded = expand_input_path(arg);
        all_files.extend(expanded);
    }

    if all_files.is_empty() {
        eprintln!("No input files found");
        process::exit(1);
    }

    // Sort files for consistent ordering
    all_files.sort();

    // For now, use the first input node for all files
    let input_node = &input_nodes[0];

    eprintln!("=== capdag: Machine Notation Execution ===\n");
    eprintln!("Machine file: {}", machine_file);
    eprintln!("Input node: {}", input_node);
    eprintln!("Input files: {}", all_files.len());
    for f in &all_files {
        eprintln!("  - {}", f.display());
    }

    eprintln!("Parsing and validating machine notation...");
    eprintln!("  Nodes: {}", graph.nodes.len());
    eprintln!("  Edges: {}", graph.edges.len());

    // Set up plugin directory
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let plugin_dir = home.join(".capdag").join("plugins");

    // Registry URL
    let registry_url = "https://machinefabric.com/api/plugins".to_string();

    eprintln!("\n=== Executing DAG ===\n");
    if !dev_binaries.is_empty() {
        eprintln!("Dev mode: {} local binaries", dev_binaries.len());
        for bin in &dev_binaries {
            eprintln!("  - {}", bin.display());
        }
    }

    // Process each file
    let mut success_count = 0;
    let mut error_count = 0;

    for file in &all_files {
        eprintln!("--- Processing: {} ---", file.display());
        eprintln!("Run: {}", notation);

        let mut initial_inputs = HashMap::new();
        initial_inputs.insert(input_node.clone(), NodeData::FilePath(file.clone()));

        let progress: CapProgressFn = Arc::new(|p: f32, cap_urn: &str, msg: &str| {
            eprintln!("  [{:5.1}%] {} {}", p * 100.0, cap_urn, msg);
        });

        match execute_dag(&graph, plugin_dir.clone(), registry_url.clone(), initial_inputs, dev_binaries.clone(), registry.clone(), Some(&progress)).await {
            Ok(outputs) => {
                eprintln!("Results:");
                for (node, data) in outputs {
                    match data {
                        NodeData::Bytes(ref b) => eprintln!("  {}: {} bytes", node, b.len()),
                        NodeData::Text(ref t) => {
                            let preview = if t.len() > 80 { &t[..80] } else { t };
                            eprintln!("  {}: {}", node, preview.replace('\n', " "));
                        }
                        NodeData::FilePath(ref p) => eprintln!("  {}: {}", node, p.display()),
                    }
                }
                success_count += 1;
            }
            Err(e) => {
                eprintln!("{}", e);
                error_count += 1;
            }
        }
    }

    eprintln!("=== Summary ===");
    eprintln!("Processed: {}", all_files.len());
    eprintln!("Success: {}", success_count);
    if error_count > 0 {
        eprintln!("Errors: {}", error_count);
    } else {
        eprintln!("Errors: {}", error_count);
    }

    if error_count > 0 {
        process::exit(1);
    }
}
