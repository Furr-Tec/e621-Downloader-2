//! Example usage of the CLI menu system

use std::path::Path;
use std::env;
use anyhow::Result;
use tokio;

use crate::v3::run_cli;

/// Example function to demonstrate how to use the CLI menu system
pub async fn cli_example() -> Result<()> {
    // Use the directory containing the executable for config files
    // This ensures config files are always created in a consistent location
    let exe_path = env::current_exe()?;
    let config_dir = exe_path.parent().unwrap_or(Path::new("."));
    
    run_cli(config_dir).await?;
    
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