use capns::{CapRegistry};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing registry cap parsing...");
    
    // Create a registry client
    let registry = CapRegistry::new()?;
    
    // Test with the problematic cap URN
    let cap_urn = "cap:action=bitlogic;language=en;type=constrained";
    println!("\nFetching cap: {}", cap_urn);
    
    match registry.get_cap(cap_urn).await {
        Ok(cap) => {
            println!("✓ Successfully parsed cap:");
            println!("  URN: {}", cap.urn_string());
            println!("  Version: {}", cap.version);
            println!("  Command: {}", cap.command);
            if let Some(desc) = &cap.description {
                println!("  Description: {}", desc);
            }
            println!("  Accepts Stdin: {}", cap.accepts_stdin);
            println!("  Arguments: {} required, {} optional", 
                     cap.arguments.required.len(), 
                     cap.arguments.optional.len());
        },
        Err(e) => {
            println!("✗ Failed to get cap: {}", e);
            return Err(e.into());
        }
    }
    
    println!("\nDone!");
    Ok(())
}