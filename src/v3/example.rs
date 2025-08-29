//! Example usage of the config loader module

use std::path::Path;
use anyhow::Result;
use tokio;

use crate::v3::{init_config, ConfigReloadEvent};

/// Example function to demonstrate how to use the config loader
pub async fn config_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    
    // Check if all required config files exist
    let files_exist = config_manager.check_config_files();
    println!("All config files exist: {}", files_exist);
    
    // Get the app config
    let app_config = config_manager.get_app_config()?;
    println!("App config loaded:");
    println!("  Download directory: {}", app_config.paths.download_directory);
    println!("  Max download concurrency: {}", app_config.pools.max_download_concurrency);
    
    // Get the e621 config
    let e621_config = config_manager.get_e621_config()?;
    println!("E621 config loaded:");
    println!("  Username: {}", e621_config.auth.username);
    println!("  Tags: {:?}", e621_config.query.tags);
    
    // Get the rules config
    let rules_config = config_manager.get_rules_config()?;
    println!("Rules config loaded:");
    println!("  Whitelist tags: {:?}", rules_config.whitelist.tags);
    println!("  Blacklist tags: {:?}", rules_config.blacklist.tags);
    
    // Subscribe to config reload events
    let mut rx = config_manager.subscribe();
    
    // Spawn a task to handle config reload events
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match event {
                ConfigReloadEvent::AppConfig => {
                    println!("App config reloaded");
                }
                ConfigReloadEvent::E621Config => {
                    println!("E621 config reloaded");
                }
                ConfigReloadEvent::RulesConfig => {
                    println!("Rules config reloaded");
                }
            }
        }
    });
    
    println!("Config loader initialized successfully!");
    println!("Try modifying one of the config files to see the live reload in action.");
    println!("Press Ctrl+C to exit.");
    
    // Keep the program running to observe file changes
    tokio::signal::ctrl_c().await?;
    
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    config_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::example::config_example().await
// }
// ```