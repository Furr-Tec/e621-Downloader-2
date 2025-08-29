//! Example usage of the orchestration layer
//! 
//! This example demonstrates how to use the orchestration layer to:
//! 1. Acquire a global lock on boot to prevent multiple instances
//! 2. Create and manage a QueryQueue, Scheduler, and Conductor
//! 3. Track each task with a unique trace ID using tracing
//! 4. Ensure orderly startup/shutdown with all threads joining cleanly

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;

use crate::v3::{
    init_config, init_orchestrator,
    ConfigManager, Orchestrator, OrchestratorResult,
};

/// Example function to demonstrate how to use the orchestration layer
pub async fn orchestration_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the orchestrator
    info!("Initializing orchestrator...");
    let orchestrator = init_orchestrator(config_manager).await?;
    
    // Add some queries to the queue
    info!("Adding queries to the queue...");
    let query1_id = orchestrator.add_query(vec!["wolf".to_string(), "solo".to_string()], 1).await?;
    let query2_id = orchestrator.add_query(vec!["dragon".to_string(), "male".to_string()], 2).await?;
    
    // Wait for a bit to let the scheduler process the queries
    info!("Waiting for queries to be processed...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Get the tasks from the queue
    let tasks = orchestrator.get_tasks();
    info!("Tasks in the queue: {}", tasks.len());
    for task in tasks {
        info!("Task: {:?}", task);
    }
    
    // Stop the orchestrator
    info!("Stopping orchestrator...");
    orchestrator.stop().await?;
    
    info!("Orchestration example completed successfully!");
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    orchestration_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::orchestration_example::orchestration_example().await
// }
// ```