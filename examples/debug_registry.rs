use reqwest;
use serde_json;
use capns::Cap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debug registry response parsing...");
    
    let cap_urn = "cap:op=bitlogic;language=en;type=constrained";
    let url = format!("https://capns.org/{}", cap_urn);
    
    println!("Fetching from: {}", url);
    
    // Step 1: Raw HTTP request
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?;
    
    println!("HTTP Status: {}", response.status());
    
    if response.status().is_success() {
        let text = response.text().await?;
        println!("Raw response (first 500 chars):");
        println!("{}", &text[..text.len().min(500)]);
        
        // Step 2: Parse as JSON
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(json) => {
                println!("\n✓ Successfully parsed as JSON");
                println!("JSON structure:");
                println!("{}", serde_json::to_string_pretty(&json)?);
                
                // Step 3: Parse as Cap
                match serde_json::from_str::<Cap>(&text) {
                    Ok(cap) => {
                        println!("\n✓ Successfully parsed as Cap:");
                        println!("  URN: {}", cap.urn_string());
                        println!("  Command: {}", cap.command);
                    },
                    Err(e) => {
                        println!("\n✗ Failed to parse as Cap: {}", e);
                    }
                }
            },
            Err(e) => {
                println!("\n✗ Failed to parse as JSON: {}", e);
            }
        }
    } else {
        println!("HTTP request failed: {}", response.status());
    }
    
    Ok(())
}