//! Example usage of the session manager
//! 
//! This example demonstrates how to use the session manager to:
//! 1. Flush all queues and logs on shutdown
//! 2. Write latest state to disk and DB
//! 3. Release global lock
//! 4. Check DB state on startup
//! 5. Resume from last page if session was interrupted
//! 6. Validate disk size before scheduling

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;

use crate::v3::{
    init_config, init_orchestrator, init_metadata_store, init_session_manager,
    ConfigManager, Orchestrator, MetadataStore, SessionManager,
};

/// Example function to demonstrate how to use the session manager
pub async fn session_manager_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the metadata store
    info!("Initializing metadata store...");
    let metadata_store = init_metadata_store(config_manager.clone()).await?;
    
    // Initialize the orchestrator
    info!("Initializing orchestrator...");
    let orchestrator = init_orchestrator(config_manager.clone()).await?;
    
    // Initialize the session manager
    info!("Initializing session manager...");
    let session_manager = init_session_manager(config_manager.clone(), metadata_store.clone()).await?;
    
    // Validate disk space before scheduling downloads
    info!("Validating disk space...");
    let required_bytes = 1024 * 1024 * 100; // 100 MB
    let has_space = session_manager.validate_disk_space(required_bytes)?;
    
    if has_space {
        info!("Sufficient disk space available");
    } else {
        info!("Not enough disk space available");
        return Ok(());
    }
    
    // Check for interrupted session and resume if needed
    info!("Checking for interrupted session...");
    let resumed = session_manager.resume(&orchestrator).await?;
    
    if resumed {
        info!("Resumed from interrupted session");
    } else {
        info!("No interrupted session found, starting new session");
        
        // Add a sample query
        info!("Adding sample query...");
        let query_id = orchestrator.add_query(vec!["wolf".to_string(), "solo".to_string()], 1).await?;
        info!("Added query with ID: {}", query_id);
    }
    
    // Wait for a bit to simulate some work
    info!("Simulating work...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Update the session state
    info!("Updating session state...");
    session_manager.update_session_state(&orchestrator.get_job_queue())?;
    
    // Perform graceful shutdown
    info!("Performing graceful shutdown...");
    session_manager.shutdown(&orchestrator).await?;
    
    info!("Session manager example completed successfully!");
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    session_manager_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::session_manager_example::session_manager_example().await
// }
// ```