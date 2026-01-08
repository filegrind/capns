use capns::CapRegistry;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing specific problematic cap...");

    let registry = CapRegistry::new().await?;

    // Test the exact cap that was failing
    let problematic_urn = "cap:op=bitlogic;language=en;type=constrained";
    println!("Fetching: {}", problematic_urn);

    match registry.get_cap(problematic_urn).await {
        Ok(cap) => {
            println!("SUCCESS: Cap parsed correctly!");
            println!("  URN: {}", cap.urn_string());
            println!("  Command: {}", cap.command);
            println!("  Description: {}", cap.cap_description.as_ref().unwrap_or(&"None".to_string()));
            println!("  Accepts Stdin: {}", cap.accepts_stdin);

            if !cap.arguments.required.is_empty() {
                println!("  Required args: {}", cap.arguments.required.len());
                for arg in &cap.arguments.required {
                    println!("    - {}: {}", arg.name, arg.media_spec);
                }
            }

            if !cap.arguments.optional.is_empty() {
                println!("  Optional args: {}", cap.arguments.optional.len());
                for arg in &cap.arguments.optional {
                    println!("    - {}: {}", arg.name, arg.media_spec);
                }
            }

            if let Some(output) = &cap.output {
                println!("  Output: {} - {}", output.media_spec, output.output_description);
            }
        },
        Err(e) => {
            println!("FAILED: {}", e);
            return Err(e.into());
        }
    }

    println!("\nTest completed successfully!");
    Ok(())
}
