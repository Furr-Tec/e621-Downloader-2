//! Example usage of the metadata store
//! 
//! This example demonstrates how to use the metadata store to:
//! 1. Store post metadata in SQLite
//! 2. Append/update JSON file with the same metadata
//! 3. Maintain consistency between disk, DB, and JSON
//! 4. Enable full metadata reload and patching if desync is detected

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::fs;

use anyhow::Result;
use tokio;
use tracing::info;
use uuid::Uuid;

use crate::v3::{
    init_config, init_metadata_store, init_file_processor,
    ConfigManager, FileHash, ProcessedFile, PostMetadata,
};

/// Example function to demonstrate how to use the metadata store
pub async fn metadata_store_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the metadata store
    info!("Initializing metadata store...");
    let metadata_store = init_metadata_store(config_manager.clone()).await?;
    
    // Get the app config
    let app_config = config_manager.get_app_config()?;
    
    // Create a temporary directory for testing
    let temp_dir = Path::new(&app_config.paths.temp_directory);
    if !temp_dir.exists() {
        fs::create_dir_all(temp_dir)?;
    }
    
    // Create a sample processed file
    let processed_file = ProcessedFile {
        original_path: temp_dir.join("original.jpg"),
        target_path: temp_dir.join("target.jpg"),
        hashes: FileHash {
            blake3: "0123456789abcdef0123456789abcdef".to_string(),
            sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        },
        post_id: 12345,
        file_size: 98765,
    };
    
    // Create sample tags and artist
    let tags = vec![
        "cute".to_string(),
        "fluffy".to_string(),
        "happy".to_string(),
    ];
    let artist = Some("test_artist".to_string());
    
    // Create metadata from the processed file
    let metadata = PostMetadata::from_processed_file(&processed_file, tags, artist);
    
    // Save the metadata
    info!("Saving metadata for post {}...", metadata.post_id);
    metadata_store.save_metadata(&metadata)?;
    
    // Retrieve the metadata
    info!("Retrieving metadata for post {}...", metadata.post_id);
    let retrieved_metadata = metadata_store.get_metadata(metadata.post_id)?;
    
    if let Some(retrieved) = retrieved_metadata {
        info!("Retrieved metadata:");
        info!("  Post ID: {}", retrieved.post_id);
        info!("  File path: {}", retrieved.file_path);
        info!("  BLAKE3 hash: {}", retrieved.blake3_hash);
        info!("  SHA256 hash: {}", retrieved.sha256_hash);
        info!("  File size: {} bytes", retrieved.file_size);
        info!("  Tags: {:?}", retrieved.tags);
        info!("  Artist: {:?}", retrieved.artist);
    } else {
        info!("Failed to retrieve metadata for post {}", metadata.post_id);
    }
    
    // Check consistency between SQLite and JSON
    info!("Checking consistency between SQLite and JSON...");
    let is_consistent = metadata_store.check_consistency()?;
    
    if is_consistent {
        info!("SQLite and JSON are consistent");
    } else {
        info!("SQLite and JSON are inconsistent, repairing...");
        metadata_store.repair_consistency()?;
        
        // Check again after repair
        let is_consistent = metadata_store.check_consistency()?;
        info!("After repair, SQLite and JSON are consistent: {}", is_consistent);
    }
    
    // Create a second metadata entry
    let second_metadata = PostMetadata {
        post_id: 67890,
        file_path: temp_dir.join("another.jpg").to_string_lossy().to_string(),
        blake3_hash: "fedcba9876543210fedcba9876543210".to_string(),
        sha256_hash: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".to_string(),
        file_size: 54321,
        width: Some(1920),
        height: Some(1080),
        tags: vec!["example".to_string(), "test".to_string()],
        artist: Some("another_artist".to_string()),
        rating: Some("s".to_string()),
        score: Some(100),
        created_at: metadata.created_at,
        updated_at: metadata.updated_at,
        verified_at: None,
    };
    
    // Save the second metadata
    info!("Saving metadata for post {}...", second_metadata.post_id);
    metadata_store.save_metadata(&second_metadata)?;
    
    // Get all metadata
    info!("Getting all metadata...");
    let all_metadata = metadata_store.get_all_metadata()?;
    info!("Total metadata entries: {}", all_metadata.len());
    
    // Delete the second metadata
    info!("Deleting metadata for post {}...", second_metadata.post_id);
    metadata_store.delete_metadata(second_metadata.post_id)?;
    
    // Get all metadata again
    info!("Getting all metadata after deletion...");
    let all_metadata = metadata_store.get_all_metadata()?;
    info!("Total metadata entries after deletion: {}", all_metadata.len());
    
    // Verify files (will fail since we didn't actually create the files)
    info!("Verifying files (expected to fail since files don't exist)...");
    let (verified, missing) = metadata_store.verify_all_files()?;
    info!("Verified: {}, Missing: {}", verified, missing);
    
    info!("Metadata store example completed successfully!");
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    metadata_store_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::metadata_store_example::metadata_store_example().await
// }
// ```