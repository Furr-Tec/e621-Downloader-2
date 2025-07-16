//! Example usage of the download engine
//! 
//! This example demonstrates how to use the download engine to:
//! 1. Maintain a connection pool
//! 2. Stream files to disk without loading them entirely into memory
//! 3. Use bounded concurrency (e.g., Semaphore)
//! 4. Retry failed downloads with exponential backoff
//! 5. Respect rate limits and sleep on 429 responses

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio;
use tracing::info;
use uuid::Uuid;

use crate::v3::{
    init_config, init_download_engine,
    ConfigManager, DownloadEngine, DownloadJob,
};

/// Example function to demonstrate how to use the download engine
pub async fn download_engine_example() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the download engine
    info!("Initializing download engine...");
    let download_engine = init_download_engine(config_manager.clone()).await?;
    
    // Create some example download jobs
    info!("Creating example download jobs...");
    let jobs = create_example_jobs();
    
    // Queue the jobs
    info!("Queuing {} download jobs...", jobs.len());
    for job in jobs {
        download_engine.queue_job(job).await?;
    }
    
    // Wait for a bit to let the download engine process the jobs
    info!("Waiting for downloads to complete...");
    for i in 1..=10 {
        // Get the current stats
        let stats = download_engine.get_stats();
        info!(
            "Download progress: {}/{} completed, {}/{} failed, {} bytes downloaded",
            stats.completed_jobs,
            stats.total_jobs,
            stats.failed_jobs,
            stats.total_jobs,
            stats.bytes_downloaded
        );
        
        // Check if all jobs are completed or failed
        if stats.completed_jobs + stats.failed_jobs >= stats.total_jobs {
            info!("All downloads completed or failed");
            break;
        }
        
        // Wait for a second
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    
    // Stop the download engine
    info!("Stopping download engine...");
    download_engine.stop().await?;
    
    info!("Download engine example completed successfully!");
    Ok(())
}

/// Create some example download jobs
fn create_example_jobs() -> Vec<DownloadJob> {
    vec![
        // Example 1: A small image
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 1,
            url: "https://e621.net/data/sample/eb/c1/ebc16aff87a9ecc3a2f3e2b2e8b736e7.jpg".to_string(),
            md5: "ebc16aff87a9ecc3a2f3e2b2e8b736e7".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 123456,
            tags: vec!["example".to_string(), "test".to_string()],
            priority: 1,
        },
        // Example 2: Another small image
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 2,
            url: "https://e621.net/data/sample/7d/f2/7df2462a4f09a0a9a9a9a9a9a9a9a9a9.jpg".to_string(),
            md5: "7df2462a4f09a0a9a9a9a9a9a9a9a9a9".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 234567,
            tags: vec!["example".to_string(), "test".to_string()],
            priority: 2,
        },
        // Example 3: A non-existent URL to test error handling
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 3,
            url: "https://e621.net/non-existent-url.jpg".to_string(),
            md5: "non-existent-md5".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 345678,
            tags: vec!["example".to_string(), "test".to_string()],
            priority: 3,
        },
    ]
}

/// Run the example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    download_engine_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::download_engine_example::download_engine_example().await
// }
// ```