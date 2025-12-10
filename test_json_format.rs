use capns::*;
use std::collections::HashMap;

fn main() {
    let urn = CapUrn::from_string("cap:action=extract;target=metadata").unwrap();
    let cap = Cap::new(urn, "Extract Metadata".to_string(), "extract-metadata".to_string());
    
    let json = serde_json::to_string_pretty(&cap).unwrap();
    println!("Cap JSON format:");
    println!("{}", json);
}