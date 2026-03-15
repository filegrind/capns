//! capdag: Route notation DAG executor for Cap pipelines
//!
//! A unified CLI for executing and validating route notation pipelines.

use capdag::orchestrator::{parse_route_to_cap_dag, execute_dag, NodeData, ResolvedGraph};
use capdag::route::RouteGraph;
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
                tracing::error!("Error reading dev-bins directory '{}': {}", path, e);
                vec![]
            }
        }
    } else {
        tracing::error!("Dev binary path does not exist: {}", path);
        vec![]
    }
}

/// Find input nodes in the route notation (root sources with no incoming edges).
///
/// Parses the route notation into a RouteGraph and returns the node names
/// that are root sources (not produced by any cap).
fn find_input_nodes(route_content: &str) -> Vec<String> {
    let graph = match RouteGraph::from_string(route_content) {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("Failed to parse route notation for input node detection: {}", e);
            return vec![];
        }
    };

    // Re-parse to get node names — RouteGraph discards them.
    // Use the same pest extraction as the orchestrator.
    use pest::Parser;
    use capdag::route::parser::{RouteParser, Rule};

    let pairs = match RouteParser::parse(Rule::program, route_content.trim()) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to re-parse route notation: {}", e);
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
                    tracing::warn!("No files matched glob pattern '{}'", path);
                }
                files
            }
            Err(e) => {
                tracing::error!("Error parsing glob pattern '{}': {}", path, e);
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
                tracing::error!("Error reading directory '{}': {}", path, e);
                vec![]
            }
        }
    } else if path_buf.is_file() {
        vec![path_buf]
    } else {
        tracing::error!("Path does not exist: {}", path);
        vec![]
    }
}

/// Escape text for Mermaid labels (double-quoted strings).
fn mermaid_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "#quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Generate Mermaid flowchart code from a resolved graph.
fn generate_mermaid(graph: &ResolvedGraph) -> String {
    let mut out = String::new();
    out.push_str("graph LR\n");

    // Collect which nodes are input (not a target of any edge) vs output (not a source)
    let mut targets: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut sources: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for edge in &graph.edges {
        sources.insert(&edge.from);
        targets.insert(&edge.to);
    }

    // Emit node definitions with media URN subtitle
    for (name, media_urn) in &graph.nodes {
        let is_input = sources.contains(name.as_str()) && !targets.contains(name.as_str());
        let is_output = targets.contains(name.as_str()) && !sources.contains(name.as_str());

        let escaped_name = mermaid_escape(name);
        let escaped_urn = mermaid_escape(media_urn);

        if is_input {
            // Stadium shape for inputs
            out.push_str(&format!(
                "    {}([\"{}<br/><small>{}</small>\"])\n",
                name, escaped_name, escaped_urn
            ));
        } else if is_output {
            // Double-circle shape for outputs
            out.push_str(&format!(
                "    {}(((\"{}<br/><small>{}</small>\")))\n",
                name, escaped_name, escaped_urn
            ));
        } else {
            // Rectangle for intermediate nodes
            out.push_str(&format!(
                "    {}[\"{}<br/><small>{}</small>\"]\n",
                name, escaped_name, escaped_urn
            ));
        }
    }

    out.push('\n');

    // Emit edges with cap title + URN subtitle
    // Deduplicate fan-in edges (multiple edges to same target with same cap)
    let mut seen_edges: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();

    for edge in &graph.edges {
        let key = (edge.from.clone(), edge.to.clone(), edge.cap_urn.clone());
        if !seen_edges.insert(key) {
            continue;
        }

        let title = mermaid_escape(&edge.cap.title);
        let urn = mermaid_escape(&edge.cap_urn);

        out.push_str(&format!(
            "    {} -->|\"{}<br/><small>{}</small>\"| {}\n",
            edge.from, title, urn, edge.to
        ));
    }

    out
}

fn print_usage(program: &str) {
    tracing::info!(
        "Usage: {} [options] <route-file> [input-paths...]\n\n\
         Execute a route notation pipeline on input files.\n\n\
         Options:\n\
           --mermaid                Output Mermaid diagram code and exit\n\
           --dev-bins <binary> ...  Use local plugin binaries\n\
           --help                   Show this help\n\n\
         Input paths can be:\n\
           - Single file:   /path/to/file.pdf\n\
           - Directory:     /path/to/pdfs/\n\
           - Glob pattern:  /path/to/*.pdf\n\n\
         Examples:\n\
           {} --mermaid pipeline.route\n\
           {} pipeline.route /tmp/test.pdf\n\
           {} pipeline.route /tmp/pdfs/\n\
           {} --dev-bins ./pdfcartridge pipeline.route /tmp/*.pdf",
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
                    && !args[arg_idx].ends_with(".route")
                {
                    let expanded = expand_dev_binary_path(&args[arg_idx]);
                    if expanded.is_empty() {
                        tracing::error!("No executables found in: {}", args[arg_idx]);
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
        tracing::error!("Missing route file argument");
        print_usage(&args[0]);
        process::exit(1);
    }

    let route_file = &args[arg_idx];
    arg_idx += 1;

    // Read route file
    let route_content = match fs::read_to_string(route_file) {
        Ok(content) => content,
        Err(e) => {
            tracing::error!("Error reading route file '{}': {}", route_file, e);
            process::exit(1);
        }
    };

    // Create CapDag registry
    let registry = match CapRegistry::new().await {
        Ok(reg) => Arc::new(reg),
        Err(e) => {
            tracing::error!("Error creating CapDag registry: {}", e);
            process::exit(1);
        }
    };

    // Parse and validate route notation
    let graph = match parse_route_to_cap_dag(&route_content, registry.as_ref()).await {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("Validation failed: {}", e);
            process::exit(1);
        }
    };

    // --mermaid: output diagram and exit
    if mermaid_mode {
        println!("{}", generate_mermaid(&graph));
        process::exit(0);
    }

    // Find input nodes automatically
    let input_nodes = find_input_nodes(&route_content);
    if input_nodes.is_empty() {
        tracing::error!("No input nodes found in route notation");
        process::exit(1);
    }

    // Collect all input paths and expand them
    let mut all_files: Vec<PathBuf> = Vec::new();
    for arg in &args[arg_idx..] {
        let expanded = expand_input_path(arg);
        all_files.extend(expanded);
    }

    if all_files.is_empty() {
        tracing::error!("No input files found");
        process::exit(1);
    }

    // Sort files for consistent ordering
    all_files.sort();

    // For now, use the first input node for all files
    let input_node = &input_nodes[0];

    tracing::info!("=== capdag: Route Notation Execution ===\n");
    tracing::info!("Route file: {}", route_file);
    tracing::info!("Input node: {}", input_node);
    tracing::info!("Input files: {}", all_files.len());
    for f in &all_files {
        tracing::info!("  - {}", f.display());
    }

    tracing::info!("Parsing and validating route notation...");
    tracing::info!("  Nodes: {}", graph.nodes.len());
    tracing::info!("  Edges: {}", graph.edges.len());

    // Set up plugin directory
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let plugin_dir = home.join(".capdag").join("plugins");

    // Registry URL
    let registry_url = "https://machinefabric.com/api/plugins".to_string();

    tracing::info!("\n=== Executing DAG ===\n");
    if !dev_binaries.is_empty() {
        tracing::info!("Dev mode: {} local binaries", dev_binaries.len());
        for bin in &dev_binaries {
            tracing::info!("  - {}", bin.display());
        }
    }

    // Process each file
    let mut success_count = 0;
    let mut error_count = 0;

    for file in &all_files {
        tracing::info!("--- Processing: {} ---", file.display());

        let mut initial_inputs = HashMap::new();
        initial_inputs.insert(input_node.clone(), NodeData::FilePath(file.clone()));

        let progress: CapProgressFn = Arc::new(|p: f32, cap_urn: &str, msg: &str| {
            eprintln!("  [{:5.1}%] {} {}", p * 100.0, cap_urn, msg);
        });

        match execute_dag(&graph, plugin_dir.clone(), registry_url.clone(), initial_inputs, dev_binaries.clone(), registry.clone(), Some(&progress)).await {
            Ok(outputs) => {
                tracing::info!("Results:");
                for (node, data) in outputs {
                    match data {
                        NodeData::Bytes(ref b) => tracing::info!("  {}: {} bytes", node, b.len()),
                        NodeData::Text(ref t) => {
                            let preview = if t.len() > 80 { &t[..80] } else { t };
                            tracing::info!("  {}: {}", node, preview.replace('\n', " "));
                        }
                        NodeData::FilePath(ref p) => tracing::info!("  {}: {}", node, p.display()),
                    }
                }
                success_count += 1;
            }
            Err(e) => {
                tracing::error!("{}", e);
                error_count += 1;
            }
        }
    }

    tracing::info!("=== Summary ===");
    tracing::info!("Processed: {}", all_files.len());
    tracing::info!("Success: {}", success_count);
    if error_count > 0 {
        tracing::error!("Errors: {}", error_count);
    } else {
        tracing::info!("Errors: {}", error_count);
    }

    if error_count > 0 {
        process::exit(1);
    }
}
