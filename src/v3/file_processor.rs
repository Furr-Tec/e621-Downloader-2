﻿//! File Processing for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Hashing downloaded files using blake3 and sha256 in parallel using rayon
//! 2. Generating filenames using post_id, short_tags, artist, hash suffix
//! 3. Checking for existing files/hashes in DB to avoid duplicates
//! 4. Moving files to target directory structure

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blake3::Hasher as Blake3Hasher;
use parking_lot::RwLock;
use rayon::prelude::*;
use rusqlite::{Connection, Result as SqliteResult};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use crate::v3::{
    AppConfig, ConfigManager, ConfigResult,
    DownloadJob, Post, PostFile, PostTags,
};

/// Error types for file processing
#[derive(Error, Debug)]
pub enum FileProcessorError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("File processing error: {0}")]
    Processing(String),
    
    #[error("File already exists: {0}")]
    FileExists(String),
}

/// Result type for file processing operations
pub type FileProcessorResult<T> = Result<T, FileProcessorError>;

/// File hash information
#[derive(Debug, Clone)]
pub struct FileHash {
    pub blake3: String,
    pub sha256: String,
}

/// Processed file information
#[derive(Debug)]
pub struct ProcessedFile {
    pub original_path: PathBuf,
    pub target_path: PathBuf,
    pub hashes: FileHash,
    pub post_id: u32,
    pub file_size: u64,
}

/// Hash store for file deduplication
pub struct FileHashStore {
    blake3_store: std::collections::HashSet<String>,
    sha256_store: std::collections::HashSet<String>,
    db_connection: Connection,
}

impl FileHashStore {
    /// Create a new file hash store
    pub fn new(db_path: &Path) -> SqliteResult<Self> {
        // Create the database connection
        let db_connection = Connection::open(db_path)?;
        
        // Create the table if it doesn't exist
        db_connection.execute(
            "CREATE TABLE IF NOT EXISTS file_hashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                blake3 TEXT NOT NULL UNIQUE,
                sha256 TEXT NOT NULL UNIQUE,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                processed_at TEXT NOT NULL
            )",
            [],
        )?;
        
        // Create the memory stores
        let blake3_store = std::collections::HashSet::new();
        let sha256_store = std::collections::HashSet::new();
        
        // Load existing hashes into memory
        let mut stmt = db_connection.prepare("SELECT blake3, sha256 FROM file_hashes")?;
        let hash_iter = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        
        let mut b3_store = std::collections::HashSet::new();
        let mut sha_store = std::collections::HashSet::new();
        
        for hash_result in hash_iter {
            if let Ok((blake3, sha256)) = hash_result {
                b3_store.insert(blake3);
                sha_store.insert(sha256);
            }
        }
        
        Ok(Self {
            blake3_store: b3_store,
            sha256_store: sha_store,
            db_connection,
        })
    }
    
    /// Check if a hash exists in the store
    pub fn contains(&self, hash: &FileHash) -> bool {
        self.blake3_store.contains(&hash.blake3) || self.sha256_store.contains(&hash.sha256)
    }
    
    /// Add a hash to the store
    pub fn add(&mut self, file: &ProcessedFile) -> SqliteResult<()> {
        // Add to memory stores
        self.blake3_store.insert(file.hashes.blake3.clone());
        self.sha256_store.insert(file.hashes.sha256.clone());
        
        // Add to database
        self.db_connection.execute(
            "INSERT INTO file_hashes (post_id, blake3, sha256, file_path, file_size, processed_at) 
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            [
                &file.post_id.to_string(),
                &file.hashes.blake3,
                &file.hashes.sha256,
                &file.target_path.to_string_lossy().to_string(),
                &file.file_size.to_string(),
            ],
        )?;
        
        Ok(())
    }
}

/// File processor for managing file operations
pub struct FileProcessor {
    config_manager: Arc<ConfigManager>,
    hash_store: Arc<RwLock<FileHashStore>>,
}

impl FileProcessor {
    /// Create a new file processor
    pub fn new(config_manager: Arc<ConfigManager>, db_path: &Path) -> FileProcessorResult<Self> {
        // Create the hash store
        let hash_store = FileHashStore::new(db_path)
            .map_err(|e| FileProcessorError::Database(e))?;
        
        Ok(Self {
            config_manager,
            hash_store: Arc::new(RwLock::new(hash_store)),
        })
    }
    
    /// Create a new file processor from app config
    pub fn from_config(config_manager: Arc<ConfigManager>) -> FileProcessorResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| FileProcessorError::Config(e.to_string()))?;
        
        // Create the database path
        let db_path = Path::new(&app_config.paths.database_file);
        
        // Create the parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| FileProcessorError::Io(e))?;
            }
        }
        
        Self::new(config_manager, db_path)
    }
    
    /// Process a file
    #[instrument(skip(self, file_path, job), fields(post_id = %job.post_id))]
    pub async fn process_file(&self, file_path: &Path, job: &DownloadJob) -> FileProcessorResult<ProcessedFile> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| FileProcessorError::Config(e.to_string()))?;
        
        // Hash the file
        info!("Hashing file: {}", file_path.display());
        let hashes = self.hash_file(file_path)?;
        
        // Check if the file already exists in the hash store
        if self.hash_store.read().contains(&hashes) {
            warn!("File already exists in hash store: {}", file_path.display());
            return Err(FileProcessorError::FileExists(format!(
                "File with hash {} already exists",
                hashes.blake3
            )));
        }
        
        // Generate the filename
        let filename = self.generate_filename(job, &hashes)?;
        
        // Create the target directory structure
        let target_dir = self.create_target_directory(job, &app_config)?;
        
        // Create the target path
        let target_path = target_dir.join(filename);
        
        // Get the file size
        let file_size = fs::metadata(file_path)?.len();
        
        // Create the processed file
        let processed_file = ProcessedFile {
            original_path: file_path.to_path_buf(),
            target_path,
            hashes,
            post_id: job.post_id,
            file_size,
        };
        
        Ok(processed_file)
    }
    
    /// Move a processed file to its target location
    pub fn move_file(&self, processed_file: &ProcessedFile) -> FileProcessorResult<()> {
        // Create the parent directory if it doesn't exist
        if let Some(parent) = processed_file.target_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| FileProcessorError::Io(e))?;
            }
        }
        
        // Move the file
        info!("Moving file from {} to {}", 
            processed_file.original_path.display(), 
            processed_file.target_path.display()
        );
        
        fs::rename(&processed_file.original_path, &processed_file.target_path)
            .map_err(|e| FileProcessorError::Io(e))?;
        
        // Add the file to the hash store
        self.hash_store.write().add(processed_file)
            .map_err(|e| FileProcessorError::Database(e))?;
        
        Ok(())
    }
    
    /// Hash a file using blake3 and sha256 in parallel
    fn hash_file(&self, file_path: &Path) -> FileProcessorResult<FileHash> {
        // Read the file into memory
        let mut file = fs::File::open(file_path)
            .map_err(|e| FileProcessorError::Io(e))?;
        
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| FileProcessorError::Io(e))?;
        
        // Hash the file in parallel using rayon
        let (blake3_hash, sha256_hash) = rayon::join(
            || {
                let mut hasher = Blake3Hasher::new();
                hasher.update(&buffer);
                hex::encode(hasher.finalize().as_bytes())
            },
            || {
                let mut hasher = Sha256::new();
                hasher.update(&buffer);
                hex::encode(hasher.finalize())
            }
        );
        
        Ok(FileHash {
            blake3: blake3_hash,
            sha256: sha256_hash,
        })
    }
    
    /// Generate a filename for a job
    fn generate_filename(&self, job: &DownloadJob, hashes: &FileHash) -> FileProcessorResult<String> {
        // Extract artist tags
        let artists = job.tags.iter()
            .filter(|tag| tag.starts_with("artist:"))
            .map(|tag| tag.trim_start_matches("artist:"))
            .collect::<Vec<_>>();
        
        // Get up to 3 general tags
        let general_tags = job.tags.iter()
            .filter(|tag| !tag.contains(":"))
            .take(3)
            .collect::<Vec<_>>();
        
        // Create the artist part
        let artist_part = if !artists.is_empty() {
            artists.join("_")
        } else {
            "unknown_artist".to_string()
        };
        
        // Create the tags part
        let tags_part = if !general_tags.is_empty() {
            general_tags.join("_")
        } else {
            "no_tags".to_string()
        };
        
        // Create the hash suffix (first 8 chars of blake3)
        let hash_suffix = &hashes.blake3[0..8];
        
        // Generate the filename
        let filename = format!(
            "{}_{}_{}_{}_.{}",
            job.post_id,
            sanitize_filename(&artist_part),
            sanitize_filename(&tags_part),
            hash_suffix,
            job.file_ext
        );
        
        Ok(filename)
    }
    
    /// Create the target directory structure
    fn create_target_directory(&self, job: &DownloadJob, app_config: &AppConfig) -> FileProcessorResult<PathBuf> {
        // Extract artist tags
        let artists = job.tags.iter()
            .filter(|tag| tag.starts_with("artist:"))
            .map(|tag| tag.trim_start_matches("artist:"))
            .collect::<Vec<_>>();
        
        // Create the base directory
        let base_dir = Path::new(&app_config.paths.download_directory);
        
        // Create the artist directory
        let artist_dir = if !artists.is_empty() {
            base_dir.join("by_artist").join(sanitize_filename(&artists[0]))
        } else {
            base_dir.join("unsorted")
        };
        
        // Create the directory if it doesn't exist
        if !artist_dir.exists() {
            fs::create_dir_all(&artist_dir)
                .map_err(|e| FileProcessorError::Io(e))?;
        }
        
        Ok(artist_dir)
    }
}

/// Sanitize a filename to remove invalid characters
fn sanitize_filename(name: &str) -> String {
    // Replace invalid characters with underscores
    let invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|'];
    let mut result = name.to_string();
    
    for c in invalid_chars {
        result = result.replace(c, "_");
    }
    
    // Limit the length to avoid excessively long filenames
    if result.len() > 50 {
        result = result[0..50].to_string();
    }
    
    result
}

/// Create a new file processor
pub async fn init_file_processor(
    config_manager: Arc<ConfigManager>,
) -> FileProcessorResult<Arc<FileProcessor>> {
    let processor = FileProcessor::from_config(config_manager)?;
    Ok(Arc::new(processor))
}