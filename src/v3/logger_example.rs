//! Example usage of the structured logging module
//! 
//! This example demonstrates how to use the logger to:
//! 1. Log every download, hash, API fetch
//! 2. Include task ID, post ID, source, status, and timestamp
//! 3. Write logs to a rotating file in JSON or line format

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;
use uuid::Uuid;

use crate::v3::{
    init_config, init_logger,
    ConfigManager, Logger, LoggerResult, OperationStatus,
};

/// Example function to demonstrate how to use the logger
pub async fn logger_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the logger
    info!("Initializing logger...");
    let logger = init_logger(config_manager.clone()).await?;
    
    // Generate some example task IDs
    let download_task_id = Uuid::new_v4();
    let hash_task_id = Uuid::new_v4();
    let api_task_id = Uuid::new_v4();
    
    // Log a download operation - Started
    logger.log_download(
        download_task_id,
        12345,
        "https://e621.net/posts/12345.json",
        OperationStatus::Started,
        None,
    );
    
    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Log a download operation - InProgress
    logger.log_download(
        download_task_id,
        12345,
        "https://e621.net/posts/12345.json",
        OperationStatus::InProgress,
        Some(1024),
    );
    
    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Log a download operation - Completed
    logger.log_download(
        download_task_id,
        12345,
        "https://e621.net/posts/12345.json",
        OperationStatus::Completed,
        Some(2048),
    );
    
    // Log a hash operation - Started
    logger.log_hash(
        hash_task_id,
        12345,
        "/path/to/file.jpg",
        OperationStatus::Started,
        None,
        None,
    );
    
    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Log a hash operation - Completed
    logger.log_hash(
        hash_task_id,
        12345,
        "/path/to/file.jpg",
        OperationStatus::Completed,
        Some("0123456789abcdef0123456789abcdef"),
        Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
    );
    
    // Log an API fetch operation - Started
    logger.log_api_fetch(
        api_task_id,
        "https://e621.net/posts.json",
        "tags=cute+fluffy&limit=10",
        OperationStatus::Started,
        None,
    );
    
    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Log an API fetch operation - Completed
    logger.log_api_fetch(
        api_task_id,
        "https://e621.net/posts.json",
        "tags=cute+fluffy&limit=10",
        OperationStatus::Completed,
        Some(200),
    );
    
    // Log a system event
    logger.log_system_event(
        "startup",
        "Application started successfully",
    );
    
    // Log an error
    logger.log_error(
        Some(Uuid::new_v4()),
        "network_error",
        "Failed to connect to the server",
        Some("Connection timed out after 30 seconds"),
    );
    
    // Log a failed download
    let failed_download_task_id = Uuid::new_v4();
    logger.log_download(
        failed_download_task_id,
        67890,
        "https://e621.net/posts/67890.json",
        OperationStatus::Failed,
        None,
    );
    
    info!("Logger example completed successfully!");
    info!("Check the logs directory for the generated log file.");
    
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    logger_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::logger_example::logger_example().await
// }
// ```