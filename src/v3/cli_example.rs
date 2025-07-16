//! Example usage of the CLI menu system

use std::path::Path;
use tokio;

use crate::v3::run_cli;

/// Example function to demonstrate how to use the CLI menu system
pub async fn cli_example() -> Result<(), Box<dyn std::error::Error>> {
    // Run the CLI with the path to the config directory
    run_cli(Path::new("src/v3")).await?;
    
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::cli_example::cli_example().await
// }
// ```