//! Example usage of the file processor
//! 
//! This example demonstrates how to use the file processor to:
//! 1. Hash downloaded files using blake3 and sha256 in parallel using rayon
//! 2. Generate filenames using post_id, short_tags, artist, hash suffix
//! 3. Check for existing files/hashes in DB to avoid duplicates
//! 4. Move files to target directory structure

use std::path::Path;
use std::sync::Arc;
use std::fs;

use anyhow::Result;
use tracing::info;
use uuid::Uuid;

use crate::v3::{
    init_config, init_file_processor,
    DownloadJob,
};

/// Example function to demonstrate how to use the file processor
pub async fn file_processor_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    // Use the same directory as the executable
    let exe_dir = std::env::current_exe()?
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Failed to get executable directory"))?
        .to_path_buf();
    let config_manager = init_config(&exe_dir).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the file processor
    info!("Initializing file processor...");
    let file_processor = init_file_processor(config_manager.clone()).await?;
    
    // Get the app config
    let app_config = config_manager.get_app_config()?;
    
    // Create a temporary file for testing
    let temp_dir = Path::new(&app_config.paths.temp_directory);
    if !temp_dir.exists() {
        fs::create_dir_all(temp_dir)?;
    }
    
    let test_file_path = temp_dir.join("test_file.txt");
    fs::write(&test_file_path, "This is a test file for hashing and processing.")?;
    
    // Create a sample download job
    let job = DownloadJob {
        id: Uuid::new_v4(),
        post_id: 12345,
        url: "https://example.com/test.txt".to_string(),
        md5: "test_md5".to_string(),
        file_ext: "txt".to_string(),
        file_size: 42,
        tags: vec![
            "artist:test_artist".to_string(),
            "cute".to_string(),
            "fluffy".to_string(),
            "happy".to_string(),
        ],
        artists: vec!["test_artist".to_string()],
        priority: 1,
        is_blacklisted: false,
    };
    
    // Process the file
    info!("Processing file: {}", test_file_path.display());
    let processed_file = file_processor.process_file(&test_file_path, &job).await?;
    
    // Display the hash information
    info!("File hashes:");
    info!("  BLAKE3: {}", processed_file.hashes.blake3);
    info!("  SHA256: {}", processed_file.hashes.sha256);
    
    // Display the target path
    info!("Target path: {}", processed_file.target_path.display());
    
    // Move the file to its target location
    info!("Moving file to target location...");
    file_processor.move_file(&processed_file)?;
    
    // Verify that the file was moved
    if processed_file.target_path.exists() {
        info!("File successfully moved to: {}", processed_file.target_path.display());
    } else {
        info!("Failed to move file to: {}", processed_file.target_path.display());
    }
    
    // Try to process the same file again (should fail with FileExists error)
    info!("Attempting to process the same file again (should fail)...");
    let another_test_file_path = temp_dir.join("another_test_file.txt");
    fs::write(&another_test_file_path, "This is a test file for hashing and processing.")?;
    
    match file_processor.process_file(&another_test_file_path, &job).await {
        Ok(_) => info!("Unexpectedly succeeded in processing duplicate file"),
        Err(e) => info!("Expected error when processing duplicate file: {}", e),
    }
    
    // Clean up
    if another_test_file_path.exists() {
        fs::remove_file(another_test_file_path)?;
    }
    
    info!("File processor example completed successfully!");
    Ok(())
}


// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::file_processor_example::file_processor_example().await
// }
// ```