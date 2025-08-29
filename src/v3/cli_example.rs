//! Example usage of the CLI menu system

use std::path::Path;
use anyhow::Result;
use tokio;

use crate::v3::run_cli;

/// Example function to demonstrate how to use the CLI menu system
pub async fn cli_example() -> Result<()> {
    // Use current working directory for config files in production
    // This allows the executable to create config files alongside itself
    run_cli(Path::new(".")).await?;
    
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    cli_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<()> {
//     v3::cli_example::cli_example().await
// }
// ```