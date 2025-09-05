//! File Processing for E621 Downloader
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
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Result as SqliteResult};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::{info, instrument, warn};

use crate::v3::{
    AppConfig, ConfigManager,
    DownloadJob,
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
    db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
}

impl FileHashStore {
    /// Create a new file hash store
    pub fn new(db_path: &Path) -> SqliteResult<Self> {
        // Create the connection manager
        let manager = SqliteConnectionManager::file(db_path);
        
        // Create the connection pool
        let pool = r2d2::Pool::new(manager)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        
        // Get a connection to initialize the database
        let conn = pool.get()
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        
        // Create the table if it doesn't exist
        conn.execute(
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
        
        // Load existing hashes into memory
        let mut b3_store: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut sha_store: std::collections::HashSet<String> = std::collections::HashSet::new();
        
        {
            let mut stmt = conn.prepare("SELECT blake3, sha256 FROM file_hashes")?;
            let hash_iter = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            
            for hash_result in hash_iter {
                if let Ok((blake3, sha256)) = hash_result {
                    b3_store.insert(blake3);
                    sha_store.insert(sha256);
                }
            }
        }
        
        Ok(Self {
            blake3_store: b3_store,
            sha256_store: sha_store,
            db_pool: pool,
        })
    }
    
    /// Check if a hash exists in the store
    pub fn contains(&self, hash: &FileHash) -> bool {
        // First check the in-memory cache
        if self.blake3_store.contains(&hash.blake3) || self.sha256_store.contains(&hash.sha256) {
            return true;
        }
        
        // If not found in cache, check the database
        if let Ok(conn) = self.db_pool.get() {
            if let Ok(mut stmt) = conn.prepare("SELECT 1 FROM file_hashes WHERE blake3 = ? OR sha256 = ? LIMIT 1") {
                if let Ok(exists) = stmt.exists([&hash.blake3, &hash.sha256]) {
                    return exists;
                }
            }
        }
        
        false
    }
    
    /// Add a hash to the store
    pub fn add(&mut self, file: &ProcessedFile) -> SqliteResult<()> {
        // Add to memory stores
        self.blake3_store.insert(file.hashes.blake3.clone());
        self.sha256_store.insert(file.hashes.sha256.clone());
        
        // Get a connection from the pool
        let conn = self.db_pool.get()
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        
        // Add to database
        conn.execute(
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
    
    /// Generate a filename for a job (matching stable version format)
    fn generate_filename(&self, job: &DownloadJob, _hashes: &FileHash) -> FileProcessorResult<String> {
        // Extract artist tags
        let artists = job.tags.iter()
            .filter(|tag| tag.starts_with("artist:"))
            .map(|tag| tag.trim_start_matches("artist:"))
            .collect::<Vec<_>>();
        
        // Generate filename based on stable version format:
        // - If no artists: {post_id}.{ext}
        // - With artist(s): {artist_name(s)}_{post_id}.{ext}
        let filename = if artists.is_empty() {
            format!("{}.{}", job.post_id, job.file_ext)
        } else {
            // Sanitize artist names and join with "+" for multiple artists
            let artist_names: Vec<String> = artists
                .iter()
                .map(|name| sanitize_filename(name))
                .collect();
            let artist_string = artist_names.join("+");
            
            format!("{}_{}.{}", artist_string, job.post_id, job.file_ext)
        };
        
        Ok(filename)
    }
    
    /// Create the target directory structure based on configuration
    fn create_target_directory(&self, job: &DownloadJob, app_config: &AppConfig) -> FileProcessorResult<PathBuf> {
        // Create the base directory
        let base_dir = Path::new(&app_config.paths.download_directory);
        
        // Determine directory structure based on configuration
        let target_dir = match app_config.organization.directory_strategy.as_str() {
            "by_artist" => {
                // Extract artist tags
                let artists = job.tags.iter()
                    .filter(|tag| tag.starts_with("artist:"))
                    .map(|tag| tag.trim_start_matches("artist:"))
                    .collect::<Vec<_>>();
                
                if !artists.is_empty() {
                    // Use first artist for directory name
                    let artist_name = sanitize_filename(&artists[0]);
                    base_dir.join("Artists").join(artist_name)
                } else {
                    // Files without artists go in a "Single Posts" directory
                    base_dir.join("Single Posts")
                }
            },
            "by_tag" => {
                // Extract first general tag (excluding artist tags)
                let general_tag = job.tags.iter()
                    .find(|tag| !tag.starts_with("artist:") && !tag.starts_with("meta:"))
                    .map(|tag| sanitize_filename(tag))
                    .unwrap_or_else(|| "untagged".to_string());
                
                base_dir.join("Tags").join(general_tag)
            },
            "mixed" => {
                // Prefer artist folders when artist tags are present; otherwise fall back to a tag folder
                let artists = job.tags.iter()
                    .filter(|tag| tag.starts_with("artist:"))
                    .map(|tag| tag.trim_start_matches("artist:"))
                    .collect::<Vec<_>>();
                if !artists.is_empty() {
                    let artist_name = sanitize_filename(&artists[0]);
                    base_dir.join("Artists").join(artist_name)
                } else {
                    let general_tag = job.tags.iter()
                        .find(|tag| !tag.starts_with("artist:") && !tag.starts_with("meta:"))
                        .map(|tag| sanitize_filename(tag))
                        .unwrap_or_else(|| "untagged".to_string());
                    base_dir.join("Tags").join(general_tag)
                }
            },
            "flat" | _ => {
                // All files go directly in the download directory
                base_dir.to_path_buf()
            }
        };
        
        // Create the directory if it doesn't exist
        if !target_dir.exists() {
            fs::create_dir_all(&target_dir)
                .map_err(|e| FileProcessorError::Io(e))?;
        }
        
        Ok(target_dir)
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