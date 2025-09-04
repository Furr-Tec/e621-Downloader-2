//! Example usage of the blacklist-aware download handling
//! 
//! This example demonstrates how to use the blacklist handling functionality to:
//! 1. Flag posts matching blacklist tags
//! 2. Redirect downloads to ./blacklisted_downloads/<artist>/
//! 3. Log and hash blacklisted content
//! 4. Track blacklisted rejects in a database table
//! 5. Watch for deletion and avoid redownloading

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;
use uuid::Uuid;

use crate::v3::{
    init_config, init_blacklist_handler, init_download_engine,
    AppConfig, ConfigManager, DownloadJob, BlacklistHandler,
};

/// Example function to demonstrate how to use the blacklist handling functionality
pub async fn blacklist_handling_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the blacklist handler
    info!("Initializing blacklist handler...");
    let blacklist_handler = init_blacklist_handler(config_manager.clone()).await?;
    
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
        let stats = download_engine.get_stats().await;
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
    
    // Get all blacklisted rejects
    info!("Getting all blacklisted rejects...");
    let rejects = blacklist_handler.get_all_blacklisted_rejects().await?;
    info!("Found {} blacklisted rejects", rejects.len());
    for reject in &rejects {
        info!("Blacklisted reject: post_id={}, deleted_at={:?}", reject.post_id, reject.deleted_at);
    }
    
    // Mark a blacklisted reject as deleted
    if !rejects.is_empty() {
        let post_id = rejects[0].post_id;
        info!("Marking post {} as deleted...", post_id);
        blacklist_handler.mark_as_deleted(post_id).await?;
        
        // Check if the post is now marked as deleted
        let is_deleted = blacklist_handler.is_deleted_blacklisted_reject(post_id).await?;
        info!("Post {} is deleted: {}", post_id, is_deleted);
    }
    
    // Stop the download engine
    info!("Stopping download engine...");
    download_engine.stop().await?;
    
    info!("Blacklist handling example completed successfully!");
    Ok(())
}

/// Create some example download jobs
fn create_example_jobs() -> Vec<DownloadJob> {
    vec![
        // Example 1: A normal post
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 1,
            url: "https://e621.net/data/sample/eb/c1/ebc16aff87a9ecc3a2f3e2b2e8b736e7.jpg".to_string(),
            md5: "ebc16aff87a9ecc3a2f3e2b2e8b736e7".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 123456,
            tags: vec!["example".to_string(), "test".to_string(), "artist:test_artist".to_string()],
            artists: vec!["test_artist".to_string()],
            priority: 1,
            is_blacklisted: false,
        },
        // Example 2: A blacklisted post
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 2,
            url: "https://e621.net/data/sample/7d/f2/7df2462a4f09a0a9a9a9a9a9a9a9a9a9.jpg".to_string(),
            md5: "7df2462a4f09a0a9a9a9a9a9a9a9a9a9".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 234567,
            tags: vec!["example".to_string(), "blacklisted".to_string(), "artist:blacklisted_artist".to_string()],
            artists: vec!["blacklisted_artist".to_string()],
            priority: 2,
            is_blacklisted: true,
        },
        // Example 3: Another blacklisted post
        DownloadJob {
            id: Uuid::new_v4(),
            post_id: 3,
            url: "https://e621.net/data/sample/3d/f2/3df2462a4f09a0a9a9a9a9a9a9a9a9a9.jpg".to_string(),
            md5: "3df2462a4f09a0a9a9a9a9a9a9a9a9a9".to_string(),
            file_ext: "jpg".to_string(),
            file_size: 345678,
            tags: vec!["example".to_string(), "blacklisted".to_string(), "artist:another_blacklisted_artist".to_string()],
            artists: vec!["another_blacklisted_artist".to_string()],
            priority: 3,
            is_blacklisted: true,
        },
    ]
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    blacklist_handling_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::blacklist_handling_example::blacklist_handling_example().await
// }
// ```