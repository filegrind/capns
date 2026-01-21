use capns::CapRegistry;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing registry cap parsing...");

    // Create a registry client
    let registry = CapRegistry::new().await?;

    // Test with the problematic cap URN
    let cap_urn = "cap:op=bitlogic;language=en;type=constrained";
    println!("\nFetching cap: {}", cap_urn);

    match registry.get_cap(cap_urn).await {
        Ok(cap) => {
            println!("Successfully parsed cap:");
            println!("  URN: {}", cap.urn_string());
            println!("  Command: {}", cap.command);
            if let Some(desc) = &cap.cap_description {
                println!("  Description: {}", desc);
            }
            println!("  Accepts stdin: {}", cap.accepts_stdin());
            if let Some(stdin_urn) = cap.get_stdin_media_urn() {
                println!("  Stdin media URN: {}", stdin_urn);
            }
            let args = cap.get_args();
            let required_count = args.iter().filter(|a| a.required).count();
            let optional_count = args.iter().filter(|a| !a.required).count();
            println!("  Arguments: {} required, {} optional", required_count, optional_count);
        },
        Err(e) => {
            println!("Failed to get cap: {}", e);
            return Err(e.into());
        }
    }

    println!("\nDone!");
    Ok(())
}
