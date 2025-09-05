//! Example usage of the disk verification and file watching functionality
//! 
//! This example demonstrates how to use the disk verifier to:
//! 1. Scan directories recursively
//! 2. Verify file hashes against metadata
//! 3. Detect renamed, moved, missing, or corrupt files
//! 4. Watch directories for changes
//! 5. Rebuild metadata if needed

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;

use crate::v3::{
    init_config, init_disk_verifier, init_file_processor, init_metadata_store,
    FileStatus, FileWatchEvent,
};
/// Example function to demonstrate how to use the disk verifier
pub async fn disk_verifier_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the metadata store
    info!("Initializing metadata store...");
    let metadata_store = init_metadata_store(config_manager.clone()).await?;
    
    // Initialize the file processor
    info!("Initializing file processor...");
    let file_processor = init_file_processor(config_manager.clone()).await?;
    
    // Initialize the disk verifier
    info!("Initializing disk verifier...");
    let disk_verifier = init_disk_verifier(
        config_manager.clone(),
        metadata_store.clone(),
        file_processor.clone(),
    ).await?;
    
    // Get the app config
    let app_config = config_manager.get_app_config()?;
    
    // Scan the download directory
    let download_dir = Path::new(&app_config.paths.download_directory);
    info!("Scanning directory: {}", download_dir.display());
    let files = disk_verifier.scan_directory(download_dir)?;
    info!("Found {} files", files.len());
    
    // Verify all files
    info!("Verifying all files...");
    let verification_results = disk_verifier.verify_all_files()?;
    
    // Count files by status
    let mut ok_count = 0;
    let mut missing_count = 0;
    let mut corrupt_count = 0;
    let mut moved_count = 0;
    let mut renamed_count = 0;
    
    for (post_id, result) in &verification_results {
        match &result.status {
            FileStatus::Ok => {
                ok_count += 1;
            }
            FileStatus::Missing => {
                missing_count += 1;
                info!("Missing file: {} ({})", post_id, result.file_path.display());
            }
            FileStatus::Corrupt => {
                corrupt_count += 1;
                info!("Corrupt file: {} ({})", post_id, result.file_path.display());
            }
            FileStatus::Moved(old_path) => {
                moved_count += 1;
                info!("Moved file: {} from {} to {}", 
                    post_id, old_path.display(), result.file_path.display());
            }
            FileStatus::Renamed(old_name) => {
                renamed_count += 1;
                info!("Renamed file: {} from {} to {}", 
                    post_id, old_name, result.file_path.file_name().unwrap().to_string_lossy());
            }
            FileStatus::Orphaned => {
                // Orphaned files are stored separately
            }
        }
    }
    
    // Get orphaned files
    let orphaned_files = disk_verifier.get_orphaned_files();
    info!("Verification results:");
    info!("  OK: {}", ok_count);
    info!("  Missing: {}", missing_count);
    info!("  Corrupt: {}", corrupt_count);
    info!("  Moved: {}", moved_count);
    info!("  Renamed: {}", renamed_count);
    info!("  Orphaned: {}", orphaned_files.len());
    
    // Rebuild metadata if needed
    if missing_count > 0 || corrupt_count > 0 || moved_count > 0 || renamed_count > 0 || !orphaned_files.is_empty() {
        info!("Rebuilding metadata...");
        let rebuilt_count = disk_verifier.rebuild_all_metadata()?;
        info!("Rebuilt metadata for {} files", rebuilt_count);
    }
    
    // Subscribe to file watch events
    let mut watch_rx = disk_verifier.subscribe();
    
    // Spawn a task to handle file watch events
    let watch_task = tokio::spawn(async move {
        info!("Watching for file changes...");
        info!("Press Ctrl+C to exit");
        
        while let Ok(event) = watch_rx.recv().await {
            match event {
                FileWatchEvent::Created(path) => {
                    info!("File created: {}", path.display());
                }
                FileWatchEvent::Modified(path) => {
                    info!("File modified: {}", path.display());
                }
                FileWatchEvent::Deleted(path) => {
                    info!("File deleted: {}", path.display());
                }
                FileWatchEvent::Renamed(old_path, new_path) => {
                    info!("File renamed: {} -> {}", old_path.display(), new_path.display());
                }
            }
        }
    });
    
    // Wait for a bit to see file watch events
    info!("Waiting for file changes (10 seconds)...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Cancel the watch task
    watch_task.abort();
    
    info!("Disk verifier example completed successfully!");
    Ok(())
}


// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::disk_verifier_example::disk_verifier_example().await
// }
// ```