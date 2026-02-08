//! Example CLI tool to parse and display DOT graphs with Cap URNs

use macino::parse_dot_to_cap_dag;
use std::env;
use std::fs;
use std::process;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <dot-file>", args[0]);
        eprintln!();
        eprintln!("Parse and validate a DOT file with Cap URNs");
        process::exit(1);
    }

    let dot_file = &args[1];

    let dot_content = match fs::read_to_string(dot_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", dot_file, e);
            process::exit(1);
        }
    };

    println!("Parsing DOT file: {}", dot_file);
    println!("{}", "=".repeat(60));

    let registry = match capns::CapRegistry::new().await {
        Ok(reg) => reg,
        Err(e) => {
            eprintln!("Error creating CapNS registry: {}", e);
            process::exit(1);
        }
    };

    match parse_dot_to_cap_dag(&dot_content, &registry).await {
        Ok(graph) => {
            println!("\n✓ Graph validation successful!\n");

            if let Some(name) = graph.graph_name {
                println!("Graph name: {}", name);
            }

            println!("\nNodes ({}):", graph.nodes.len());
            for (node_id, media_urn) in &graph.nodes {
                println!("  {} -> {}", node_id, media_urn);
            }

            println!("\nEdges ({}):", graph.edges.len());
            for edge in &graph.edges {
                println!("\n  {} -> {}", edge.from, edge.to);
                println!("    Cap: {}", edge.cap_urn);
                println!("    In:  {}", edge.in_media);
                println!("    Out: {}", edge.out_media);
                println!("    Title: {}", edge.cap.title);
            }

            println!("\n{}", "=".repeat(60));
            println!("✓ Successfully parsed {} nodes and {} edges",
                     graph.nodes.len(),
                     graph.edges.len());
        }
        Err(e) => {
            eprintln!("\n✗ Validation failed:");
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}
