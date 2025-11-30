use capns::{CapRegistry, CapUrn, Cap, validate_cap_canonical};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Capns Registry Example");
    
    // Create a registry client
    let registry = CapRegistry::new().await?;
    
    // Get a canonical cap definition from registry
    let cap_urn = "cap:action=extract;target=metadata";
    println!("\nGetting cap from registry: {}", cap_urn);
    
    let cap = registry.get_cap(cap_urn).await?;
    println!("✓ Successfully retrieved cap:");
    println!("  URN: {}", cap.urn_string());
    println!("  Version: {}", cap.version);
    println!("  Command: {}", cap.command);
    if let Some(desc) = &cap.cap_description {
        println!("  Description: {}", desc);
    }
    
    // Validate a local cap against canonical definition
    println!("\nValidating local cap against registry...");
    
    let test_urn = CapUrn::from_string(cap_urn)?;
    let local_cap = Cap::new(test_urn, "1.0.0".to_string(), "test-command".to_string());
    
    match validate_cap_canonical(&registry, &local_cap).await {
        Ok(_) => println!("✓ Local cap is valid"),
        Err(e) => println!("✗ Local cap validation failed: {}", e),
    }
    
    // Get multiple caps at once
    println!("\nGetting multiple caps...");
    let cap_urns = ["cap:action=extract;target=metadata", "cap:action=generate;target=thumbnail"];
    
    let caps = registry.get_caps(&cap_urns).await?;
    println!("✓ Successfully retrieved {} caps", caps.len());
    for cap in caps {
        println!("  - {}", cap.urn_string());
    }
    
    // Check cached caps
    println!("\nChecking cached caps...");
    let cached_caps = registry.get_cached_caps().await?;
    println!("✓ Found {} cached caps", cached_caps.len());
    for cap in cached_caps {
        println!("  - {}", cap.urn_string());
    }
    
    println!("\nDone!");
    Ok(())
}