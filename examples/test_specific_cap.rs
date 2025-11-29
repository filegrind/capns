use capns::CapRegistry;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing specific problematic cap...");
    
    let registry = CapRegistry::new()?;
    
    // Test the exact cap that was failing
    let problematic_urn = "cap:action=bitlogic;language=en;type=constrained";
    println!("Fetching: {}", problematic_urn);
    
    match registry.get_cap(problematic_urn).await {
        Ok(cap) => {
            println!("✓ SUCCESS: Cap parsed correctly!");
            println!("  URN: {}", cap.urn_string());
            println!("  Version: {}", cap.version);
            println!("  Command: {}", cap.command);
            println!("  Description: {}", cap.description.as_ref().unwrap_or(&"None".to_string()));
            println!("  Accepts Stdin: {}", cap.accepts_stdin);
            
            if !cap.arguments.required.is_empty() {
                println!("  Required args: {}", cap.arguments.required.len());
                for arg in &cap.arguments.required {
                    println!("    - {}: {:?}", arg.name, arg.arg_type);
                }
            }
            
            if !cap.arguments.optional.is_empty() {
                println!("  Optional args: {}", cap.arguments.optional.len());
                for arg in &cap.arguments.optional {
                    println!("    - {}: {:?}", arg.name, arg.arg_type);
                }
            }
            
            if let Some(output) = &cap.output {
                println!("  Output: {:?} - {}", output.output_type, output.description);
            }
        },
        Err(e) => {
            println!("✗ FAILED: {}", e);
            return Err(e.into());
        }
    }
    
    println!("\nTest completed successfully!");
    Ok(())
}