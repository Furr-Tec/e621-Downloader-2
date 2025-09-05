//! SQLite and JSON Metadata Persistence for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. SQLite table for post metadata (post_id, path, hashes, artist, tags, verified_at, etc.)
//! 2. Append/update JSON file post_metadata.json with same fields (by post_id key)
//! 3. Maintain consistency between disk, DB, and JSON
//! 4. Enable full metadata reload and patching if desync is detected

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, params, Transaction};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::v3::{
    ConfigManager,
    FileHash, ProcessedFile, Post,
};

/// Error types for metadata store
#[derive(Error, Debug)]
pub enum MetadataStoreError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Metadata error: {0}")]
    Metadata(String),

    #[error("Inconsistency detected: {0}")]
    Inconsistency(String),
}

/// Result type for metadata store operations
pub type MetadataStoreResult<T> = Result<T, MetadataStoreError>;

/// Post metadata stored in SQLite and JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMetadata {
    pub post_id: u32,
    pub file_path: String,
    pub blake3_hash: String,
    pub sha256_hash: String,
    pub file_size: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub tags: Vec<String>,
    pub artist: Option<String>,
    pub rating: Option<String>,
    pub score: Option<i32>,
    pub created_at: i64,
    pub updated_at: i64,
    pub verified_at: Option<i64>,
}

impl PostMetadata {
    /// Create a new post metadata from a processed file
    pub fn from_processed_file(file: &ProcessedFile, tags: Vec<String>, artist: Option<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        Self {
            post_id: file.post_id,
            file_path: file.target_path.to_string_lossy().to_string(),
            blake3_hash: file.hashes.blake3.clone(),
            sha256_hash: file.hashes.sha256.clone(),
            file_size: file.file_size,
            width: None,
            height: None,
            tags,
            artist,
            rating: None,
            score: None,
            created_at: now,
            updated_at: now,
            verified_at: None,
        }
    }

    /// Create a new post metadata from a post
    pub fn from_post(post: &Post, file_path: &Path, hashes: &FileHash) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Collect all tags
        let mut tags = Vec::new();
        tags.extend(post.tags.general.clone());
        tags.extend(post.tags.species.clone());
        tags.extend(post.tags.character.clone());
        tags.extend(post.tags.copyright.clone());
        tags.extend(post.tags.meta.clone());

        // Get the artist
        let artist = if !post.tags.artist.is_empty() {
            Some(post.tags.artist[0].clone())
        } else {
            None
        };

        Self {
            post_id: post.id,
            file_path: file_path.to_string_lossy().to_string(),
            blake3_hash: hashes.blake3.clone(),
            sha256_hash: hashes.sha256.clone(),
            file_size: post.file.size as u64,
            width: Some(post.file.width),
            height: Some(post.file.height),
            tags,
            artist,
            rating: Some(post.rating.clone()),
            score: Some(post.score.total),
            created_at: now,
            updated_at: now,
            verified_at: None,
        }
    }
}

/// Metadata store for post information
pub struct MetadataStore {
    db_connection: Mutex<Connection>,
    json_path: PathBuf,
    memory_cache: Arc<RwLock<HashMap<u32, PostMetadata>>>,
}

impl MetadataStore {
    /// Create a new metadata store
    pub fn new(db_path: &Path, json_path: &Path) -> MetadataStoreResult<Self> {
        // Create the database connection
        let db_connection = Connection::open(db_path)?;

        // Create the table if it doesn't exist
        db_connection.execute(
            "CREATE TABLE IF NOT EXISTS post_metadata (
                post_id INTEGER PRIMARY KEY,
                file_path TEXT NOT NULL,
                blake3_hash TEXT NOT NULL,
                sha256_hash TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                width INTEGER,
                height INTEGER,
                tags TEXT NOT NULL,
                artist TEXT,
                rating TEXT,
                score INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                verified_at INTEGER
            )",
            [],
        )?;

        // Create indices for faster lookups
        db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_post_metadata_blake3 ON post_metadata (blake3_hash)",
            [],
        )?;

        db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_post_metadata_sha256 ON post_metadata (sha256_hash)",
            [],
        )?;

        // Create the memory cache
        let memory_cache = Arc::new(RwLock::new(HashMap::new()));

        // Create the JSON file if it doesn't exist
        if !json_path.exists() {
            let empty_map: HashMap<u32, PostMetadata> = HashMap::new();
            let json_data = serde_json::to_string_pretty(&empty_map)?;
            fs::write(json_path, json_data)?;
        }

        // Create the metadata store
        let store = Self {
            db_connection: Mutex::new(db_connection),
            json_path: json_path.to_path_buf(),
            memory_cache,
        };

        // Load the metadata into memory
        store.load_metadata()?;

        Ok(store)
    }

    /// Create a new metadata store from app config
    pub fn from_config(config_manager: &ConfigManager) -> MetadataStoreResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| MetadataStoreError::Config(e.to_string()))?;

        // Create the database path
        let db_path = Path::new(&app_config.paths.database_file);

        // Create the JSON path
        let json_dir = db_path.parent().unwrap_or(Path::new("."));
        let json_path = json_dir.join("post_metadata.json");

        // Create the parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        Self::new(db_path, &json_path)
    }

    /// Load metadata from SQLite into memory
    pub fn load_metadata(&self) -> MetadataStoreResult<()> {
        // Clear the memory cache
        self.memory_cache.write().clear();

        // Load metadata from SQLite
        let conn = self.db_connection.lock();
        let mut stmt = conn.prepare(
            "SELECT 
                post_id, file_path, blake3_hash, sha256_hash, file_size,
                width, height, tags, artist, rating, score,
                created_at, updated_at, verified_at
             FROM post_metadata"
        )?;

        let metadata_iter = stmt.query_map([], |row| {
            let tags_str: String = row.get(7)?;
            let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();

            Ok(PostMetadata {
                post_id: row.get(0)?,
                file_path: row.get(1)?,
                blake3_hash: row.get(2)?,
                sha256_hash: row.get(3)?,
                file_size: row.get(4)?,
                width: row.get(5)?,
                height: row.get(6)?,
                tags,
                artist: row.get(8)?,
                rating: row.get(9)?,
                score: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
                verified_at: row.get(13)?,
            })
        })?;

        // Add metadata to memory cache
        let mut cache = self.memory_cache.write();
        for metadata_result in metadata_iter {
            if let Ok(metadata) = metadata_result {
                cache.insert(metadata.post_id, metadata);
            }
        }

        info!("Loaded {} metadata entries from SQLite", cache.len());
        Ok(())
    }

    /// Save metadata to SQLite and JSON
    pub fn save_metadata(&self, metadata: &PostMetadata) -> MetadataStoreResult<()> {
        // Start a transaction
        let mut conn = self.db_connection.lock();
        let tx = conn.transaction()?;

        // Save to SQLite
        self.save_to_sqlite(&tx, metadata)?;

        // Commit the transaction
        tx.commit()?;

        // Save to JSON
        self.save_to_json(metadata)?;

        // Update memory cache
        self.memory_cache.write().insert(metadata.post_id, metadata.clone());

        Ok(())
    }

    /// Save metadata to SQLite within a transaction
    fn save_to_sqlite(&self, tx: &Transaction, metadata: &PostMetadata) -> MetadataStoreResult<()> {
        // Serialize tags to JSON
        let tags_json = serde_json::to_string(&metadata.tags)?;

        // Insert or update the metadata
        tx.execute(
            "INSERT OR REPLACE INTO post_metadata (
                post_id, file_path, blake3_hash, sha256_hash, file_size,
                width, height, tags, artist, rating, score,
                created_at, updated_at, verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                metadata.post_id,
                metadata.file_path,
                metadata.blake3_hash,
                metadata.sha256_hash,
                metadata.file_size,
                metadata.width,
                metadata.height,
                tags_json,
                metadata.artist,
                metadata.rating,
                metadata.score,
                metadata.created_at,
                metadata.updated_at,
                metadata.verified_at,
            ],
        )?;

        Ok(())
    }

    /// Save metadata to JSON
    fn save_to_json(&self, metadata: &PostMetadata) -> MetadataStoreResult<()> {
        // Load the current JSON file
        let json_data = fs::read_to_string(&self.json_path)?;
        let mut metadata_map: HashMap<u32, PostMetadata> = serde_json::from_str(&json_data)?;

        // Update the metadata
        metadata_map.insert(metadata.post_id, metadata.clone());

        // Write back to the JSON file
        let json_data = serde_json::to_string_pretty(&metadata_map)?;
        fs::write(&self.json_path, json_data)?;

        Ok(())
    }

    /// Get metadata by post ID
    pub fn get_metadata(&self, post_id: u32) -> MetadataStoreResult<Option<PostMetadata>> {
        // Check memory cache first
        if let Some(metadata) = self.memory_cache.read().get(&post_id) {
            return Ok(Some(metadata.clone()));
        }

        // Query SQLite
        let conn = self.db_connection.lock();
        let mut stmt = conn.prepare(
            "SELECT 
                post_id, file_path, blake3_hash, sha256_hash, file_size,
                width, height, tags, artist, rating, score,
                created_at, updated_at, verified_at
             FROM post_metadata
             WHERE post_id = ?"
        )?;

        let metadata = stmt.query_row(params![post_id], |row| {
            let tags_str: String = row.get(7)?;
            let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();

            Ok(PostMetadata {
                post_id: row.get(0)?,
                file_path: row.get(1)?,
                blake3_hash: row.get(2)?,
                sha256_hash: row.get(3)?,
                file_size: row.get(4)?,
                width: row.get(5)?,
                height: row.get(6)?,
                tags,
                artist: row.get(8)?,
                rating: row.get(9)?,
                score: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
                verified_at: row.get(13)?,
            })
        });

        match metadata {
            Ok(metadata) => {
                // Update memory cache
                self.memory_cache.write().insert(post_id, metadata.clone());
                Ok(Some(metadata))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(MetadataStoreError::Database(e)),
        }
    }

    /// Get metadata by hash
    pub fn get_metadata_by_hash(&self, hash: &str) -> MetadataStoreResult<Option<PostMetadata>> {
        // Query SQLite
        let conn = self.db_connection.lock();
        let mut stmt = conn.prepare(
            "SELECT 
                post_id, file_path, blake3_hash, sha256_hash, file_size,
                width, height, tags, artist, rating, score,
                created_at, updated_at, verified_at
             FROM post_metadata
             WHERE blake3_hash = ? OR sha256_hash = ?"
        )?;

        let metadata = stmt.query_row(params![hash, hash], |row| {
            let tags_str: String = row.get(7)?;
            let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();

            Ok(PostMetadata {
                post_id: row.get(0)?,
                file_path: row.get(1)?,
                blake3_hash: row.get(2)?,
                sha256_hash: row.get(3)?,
                file_size: row.get(4)?,
                width: row.get(5)?,
                height: row.get(6)?,
                tags,
                artist: row.get(8)?,
                rating: row.get(9)?,
                score: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
                verified_at: row.get(13)?,
            })
        });

        match metadata {
            Ok(metadata) => {
                // Update memory cache
                self.memory_cache.write().insert(metadata.post_id, metadata.clone());
                Ok(Some(metadata))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(MetadataStoreError::Database(e)),
        }
    }

    /// Delete metadata by post ID
    pub fn delete_metadata(&self, post_id: u32) -> MetadataStoreResult<()> {
        // Start a transaction
        let mut conn = self.db_connection.lock();
        let tx = conn.transaction()?;

        // Delete from SQLite
        tx.execute(
            "DELETE FROM post_metadata WHERE post_id = ?",
            params![post_id],
        )?;

        // Commit the transaction
        tx.commit()?;

        // Delete from JSON
        let json_data = fs::read_to_string(&self.json_path)?;
        let mut metadata_map: HashMap<u32, PostMetadata> = serde_json::from_str(&json_data)?;

        metadata_map.remove(&post_id);

        let json_data = serde_json::to_string_pretty(&metadata_map)?;
        fs::write(&self.json_path, json_data)?;

        // Remove from memory cache
        self.memory_cache.write().remove(&post_id);

        Ok(())
    }

    /// Verify a file exists and matches its metadata
    pub fn verify_file(&self, post_id: u32) -> MetadataStoreResult<bool> {
        // Get the metadata
        let metadata = match self.get_metadata(post_id)? {
            Some(m) => m,
            None => return Ok(false),
        };

        // Check if the file exists
        let file_path = Path::new(&metadata.file_path);
        if !file_path.exists() {
            return Ok(false);
        }

        // Check the file size
        let file_size = fs::metadata(file_path)?.len();
        if file_size != metadata.file_size {
            return Ok(false);
        }

        // Update the verified_at timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut updated_metadata = metadata.clone();
        updated_metadata.verified_at = Some(now);

        // Save the updated metadata
        self.save_metadata(&updated_metadata)?;

        Ok(true)
    }

    /// Check for inconsistencies between SQLite and JSON
    pub fn check_consistency(&self) -> MetadataStoreResult<bool> {
        // Load all metadata from SQLite
        let sqlite_metadata = self.get_all_metadata()?;

        // Load all metadata from JSON
        let json_data = fs::read_to_string(&self.json_path)?;
        let json_metadata: HashMap<u32, PostMetadata> = serde_json::from_str(&json_data)?;

        // Check if the number of entries matches
        if sqlite_metadata.len() != json_metadata.len() {
            warn!(
                "Inconsistency detected: SQLite has {} entries, JSON has {} entries",
                sqlite_metadata.len(),
                json_metadata.len()
            );
            return Ok(false);
        }

        // Check if all entries in SQLite exist in JSON with the same values
        for (post_id, sqlite_entry) in &sqlite_metadata {
            match json_metadata.get(post_id) {
                Some(json_entry) => {
                    // Check if the entries match
                    if sqlite_entry.blake3_hash != json_entry.blake3_hash
                        || sqlite_entry.sha256_hash != json_entry.sha256_hash
                        || sqlite_entry.file_path != json_entry.file_path
                    {
                        warn!(
                            "Inconsistency detected for post {}: SQLite and JSON entries don't match",
                            post_id
                        );
                        return Ok(false);
                    }
                }
                None => {
                    warn!(
                        "Inconsistency detected: Post {} exists in SQLite but not in JSON",
                        post_id
                    );
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Repair inconsistencies between SQLite and JSON
    pub fn repair_consistency(&self) -> MetadataStoreResult<()> {
        // Load all metadata from SQLite
        let sqlite_metadata = self.get_all_metadata()?;

        // Write all metadata to JSON
        let json_data = serde_json::to_string_pretty(&sqlite_metadata)?;
        fs::write(&self.json_path, json_data)?;

        info!("Repaired consistency: wrote {} entries from SQLite to JSON", sqlite_metadata.len());
        Ok(())
    }

    /// Get all metadata from SQLite
    pub fn get_all_metadata(&self) -> MetadataStoreResult<HashMap<u32, PostMetadata>> {
        // Query SQLite
        let conn = self.db_connection.lock();
        let mut stmt = conn.prepare(
            "SELECT 
                post_id, file_path, blake3_hash, sha256_hash, file_size,
                width, height, tags, artist, rating, score,
                created_at, updated_at, verified_at
             FROM post_metadata"
        )?;

        let metadata_iter = stmt.query_map([], |row| {
            let tags_str: String = row.get(7)?;
            let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();

            Ok(PostMetadata {
                post_id: row.get(0)?,
                file_path: row.get(1)?,
                blake3_hash: row.get(2)?,
                sha256_hash: row.get(3)?,
                file_size: row.get(4)?,
                width: row.get(5)?,
                height: row.get(6)?,
                tags,
                artist: row.get(8)?,
                rating: row.get(9)?,
                score: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
                verified_at: row.get(13)?,
            })
        })?;

        // Build the metadata map
        let mut metadata_map = HashMap::new();
        for metadata_result in metadata_iter {
            if let Ok(metadata) = metadata_result {
                metadata_map.insert(metadata.post_id, metadata);
            }
        }

        Ok(metadata_map)
    }

    /// Verify all files in the database
    pub fn verify_all_files(&self) -> MetadataStoreResult<(usize, usize)> {
        // Get all metadata
        let all_metadata = self.get_all_metadata()?;

        let mut verified_count = 0;
        let mut missing_count = 0;

        // Verify each file
        for (post_id, _) in all_metadata {
            if self.verify_file(post_id)? {
                verified_count += 1;
            } else {
                missing_count += 1;
            }
        }

        info!(
            "Verified {} files, {} files missing or invalid",
            verified_count,
            missing_count
        );

        Ok((verified_count, missing_count))
    }
}

/// Create a new metadata store
pub async fn init_metadata_store(
    config_manager: Arc<ConfigManager>,
) -> MetadataStoreResult<Arc<MetadataStore>> {
    let store = MetadataStore::from_config(&config_manager)?;
    Ok(Arc::new(store))
}
