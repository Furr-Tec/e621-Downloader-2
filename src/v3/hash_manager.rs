//! Hash Management System for E621 Downloader V3
//! 
//! This module provides unified hash management that:
//! 1. Manages both E621 MD5 hashes (for duplicate prevention) and file content hashes (for integrity)
//! 2. Provides efficient in-memory caching with database persistence
//! 3. Integrates with query planner for duplicate prevention
//! 4. Integrates with download engine to record successful downloads
//! 5. Supports file existence verification by both filename and content hash

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use rusqlite::{Connection, Result as SqliteResult, params};
use thiserror::Error;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, info, warn, error};

use crate::v3::{DownloadJob, ConfigManager, AppConfig};

/// Errors for hash management operations
#[derive(Error, Debug)]
pub enum HashManagerError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Config error: {0}")]
    Config(String),
}

/// Result type for hash management operations
pub type HashManagerResult<T> = Result<T, HashManagerError>;

/// Information about a downloaded file
#[derive(Debug, Clone)]
pub struct DownloadRecord {
    pub post_id: u32,
    pub md5: String,
    pub file_path: PathBuf,
    pub file_size: u64,
    pub downloaded_at: SystemTime,
    pub tags: Vec<String>,
    pub artists: Vec<String>,
    pub is_blacklisted: bool,
}

/// Hash verification result
#[derive(Debug, Clone)]
pub struct HashVerification {
    pub exists: bool,
    pub file_path: Option<PathBuf>,
    pub post_id: Option<u32>,
    pub downloaded_at: Option<SystemTime>,
}

/// Unified hash manager for both E621 API hashes and file content hashes
pub struct HashManager {
    db_connection: Arc<TokioMutex<Connection>>,
    
    // In-memory caches for fast lookups
    md5_cache: Arc<RwLock<HashSet<String>>>,
    post_id_cache: Arc<RwLock<HashSet<u32>>>,
    file_path_cache: Arc<RwLock<HashMap<String, PathBuf>>>, // MD5 -> file path
    
    config_manager: Arc<ConfigManager>,
}

impl HashManager {
    /// Create a new hash manager
    pub fn new(config_manager: Arc<ConfigManager>) -> HashManagerResult<Self> {
        let app_config = config_manager.get_app_config()
            .map_err(|e| HashManagerError::Config(e.to_string()))?;

        // Resolve database path
        let exe_path = std::env::current_exe()
            .map_err(|e| HashManagerError::Config(format!("Failed to get executable path: {}", e)))?;
        let exe_dir = exe_path.parent()
            .ok_or_else(|| HashManagerError::Config("Failed to get executable directory".to_string()))?;
        
        let db_path = if PathBuf::from(&app_config.paths.database_file).is_absolute() {
            PathBuf::from(&app_config.paths.database_file)
        } else {
            exe_dir.join(&app_config.paths.database_file)
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // Create database connection
        let connection = Connection::open(&db_path)?;
        
        // Initialize database schema
        Self::init_database_schema(&connection)?;
        
        let hash_manager = Self {
            db_connection: Arc::new(TokioMutex::new(connection)),
            md5_cache: Arc::new(RwLock::new(HashSet::new())),
            post_id_cache: Arc::new(RwLock::new(HashSet::new())),
            file_path_cache: Arc::new(RwLock::new(HashMap::new())),
            config_manager,
        };
        
        // Note: reload_cache will be called from init_hash_manager
        
        Ok(hash_manager)
    }

    /// Initialize database schema with proper tables and indexes
    fn init_database_schema(conn: &Connection) -> SqliteResult<()> {
        debug!("Starting database schema initialization");
        
        // Main downloads table - stores information about downloaded posts
        conn.execute(
            "CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL UNIQUE,
                md5 TEXT NOT NULL UNIQUE,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                downloaded_at TEXT NOT NULL,
                is_blacklisted BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        ).map_err(|e| {
            error!("Failed to create downloads table: {}", e);
            e
        })?;
        debug!("Created downloads table successfully");

        // Tags table - stores tags for downloaded posts
        conn.execute(
            "CREATE TABLE IF NOT EXISTS download_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                FOREIGN KEY(post_id) REFERENCES downloads(post_id),
                UNIQUE(post_id, tag)
            )",
            [],
        ).map_err(|e| {
            error!("Failed to create download_tags table: {}", e);
            e
        })?;
        debug!("Created download_tags table successfully");

        // File integrity table - stores actual file hashes for verification
        conn.execute(
            "CREATE TABLE IF NOT EXISTS file_integrity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                blake3_hash TEXT,
                sha256_hash TEXT,
                verified_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(post_id) REFERENCES downloads(post_id),
                UNIQUE(post_id)
            )",
            [],
        ).map_err(|e| {
            error!("Failed to create file_integrity table: {}", e);
            e
        })?;
        debug!("Created file_integrity table successfully");

        // Artists table - stores artists for downloaded posts
        conn.execute(
            "CREATE TABLE IF NOT EXISTS download_artists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                artist TEXT NOT NULL,
                FOREIGN KEY(post_id) REFERENCES downloads(post_id),
                UNIQUE(post_id, artist)
            )",
            [],
        ).map_err(|e| {
            error!("Failed to create download_artists table: {}", e);
            e
        })?;
        debug!("Created download_artists table successfully");

        // Create indexes for performance
        let indexes = [
            ("CREATE INDEX IF NOT EXISTS idx_downloads_md5 ON downloads(md5)", "idx_downloads_md5"),
            ("CREATE INDEX IF NOT EXISTS idx_downloads_post_id ON downloads(post_id)", "idx_downloads_post_id"),
            ("CREATE INDEX IF NOT EXISTS idx_downloads_path ON downloads(file_path)", "idx_downloads_path"),
            ("CREATE INDEX IF NOT EXISTS idx_tags_post_id ON download_tags(post_id)", "idx_tags_post_id"),
            ("CREATE INDEX IF NOT EXISTS idx_tags_tag ON download_tags(tag)", "idx_tags_tag"),
            ("CREATE INDEX IF NOT EXISTS idx_integrity_post_id ON file_integrity(post_id)", "idx_integrity_post_id"),
            ("CREATE INDEX IF NOT EXISTS idx_artists_post_id ON download_artists(post_id)", "idx_artists_post_id"),
            ("CREATE INDEX IF NOT EXISTS idx_artists_artist ON download_artists(artist)", "idx_artists_artist"),
        ];
        
        for (sql, index_name) in indexes.iter() {
            conn.execute(sql, []).map_err(|e| {
                error!("Failed to create index {}: {}", index_name, e);
                e
            })?;
            debug!("Created index {} successfully", index_name);
        }

        // Verify that all tables were created by checking their existence
        let tables = ["downloads", "download_tags", "file_integrity", "download_artists"];
        for table_name in tables.iter() {
            let count: Result<i32, _> = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table_name],
                |row| row.get(0)
            );
            
            match count {
                Ok(1) => debug!("Verified table {} exists", table_name),
                Ok(0) => {
                    error!("Table {} was not created successfully", table_name);
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ABORT),
                        Some(format!("Table {} not found after creation", table_name))
                    ));
                },
                Err(e) => {
                    error!("Error verifying table {}: {}", table_name, e);
                    return Err(e);
                }
                _ => {
                    error!("Unexpected result when checking for table {}", table_name);
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ABORT),
                        Some(format!("Unexpected count for table {}", table_name))
                    ));
                }
            }
        }

        info!("Database schema initialized successfully with all required tables and indexes");
        Ok(())
    }

    /// Reload cache from database
    pub async fn reload_cache(&self) -> HashManagerResult<()> {
        let conn = self.db_connection.lock().await;
        
        let mut md5_cache = self.md5_cache.write();
        let mut post_id_cache = self.post_id_cache.write();
        let mut file_path_cache = self.file_path_cache.write();
        
        // Clear existing cache
        md5_cache.clear();
        post_id_cache.clear();
        file_path_cache.clear();
        
        // Load from database
        let mut stmt = conn.prepare("SELECT post_id, md5, file_path FROM downloads")?;
        let rows = stmt.query_map([], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let md5: String = row.get(1)?;
            let file_path: String = row.get(2)?;
            Ok((post_id, md5, file_path))
        })?;

        let mut loaded_count = 0;
        for row in rows {
            if let Ok((post_id, md5, file_path)) = row {
                md5_cache.insert(md5.clone());
                post_id_cache.insert(post_id);
                file_path_cache.insert(md5, PathBuf::from(file_path));
                loaded_count += 1;
            }
        }

        info!("Loaded {} download records into cache", loaded_count);
        Ok(())
    }

    /// Check if a post is already downloaded by MD5 hash
    pub fn contains_md5(&self, md5: &str) -> bool {
        self.md5_cache.read().contains(md5)
    }

    /// Check if a post is already downloaded by post ID
    pub fn contains_post_id(&self, post_id: u32) -> bool {
        self.post_id_cache.read().contains(&post_id)
    }

    /// Check if a post is already downloaded by either MD5 or post ID
    pub fn contains_either(&self, md5: &str, post_id: u32) -> bool {
        let md5_cache = self.md5_cache.read();
        let post_id_cache = self.post_id_cache.read();
        md5_cache.contains(md5) || post_id_cache.contains(&post_id)
    }

    /// Get verification information for a hash
    pub async fn verify_hash(&self, md5: &str) -> HashManagerResult<HashVerification> {
        let conn = self.db_connection.lock().await;
        
        let mut stmt = conn.prepare(
            "SELECT post_id, file_path, downloaded_at FROM downloads WHERE md5 = ?"
        )?;

        let result = stmt.query_row([md5], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let file_path: String = row.get(1)?;
            let downloaded_at: String = row.get(2)?;
            Ok((post_id, file_path, downloaded_at))
        });

        match result {
            Ok((post_id, file_path, downloaded_at_str)) => {
                // Parse the datetime string
                let downloaded_at = if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&downloaded_at_str) {
                    timestamp.into()
                } else {
                    SystemTime::now() // Fallback
                };

                Ok(HashVerification {
                    exists: true,
                    file_path: Some(PathBuf::from(file_path)),
                    post_id: Some(post_id),
                    downloaded_at: Some(downloaded_at),
                })
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Ok(HashVerification {
                    exists: false,
                    file_path: None,
                    post_id: None,
                    downloaded_at: None,
                })
            }
            Err(e) => Err(HashManagerError::Database(e)),
        }
    }

    /// Record a successful download
    pub async fn record_download(&self, job: &DownloadJob, file_path: &Path) -> HashManagerResult<()> {
        debug!("Recording download for post_id={}, md5={}, path={}", job.post_id, job.md5, file_path.display());
        
        let conn = self.db_connection.lock().await;
        let file_size = if file_path.exists() {
            std::fs::metadata(file_path)?.len()
        } else {
            job.file_size as u64
        };

        // Verify that the required tables exist before attempting to use them
        let tables_to_check = ["downloads", "download_tags", "download_artists"];
        for table_name in tables_to_check.iter() {
            let count: Result<i32, _> = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table_name],
                |row| row.get(0)
            );
            
            match count {
                Ok(0) => {
                    error!("Required table '{}' does not exist in database - schema may not be properly initialized", table_name);
                    return Err(HashManagerError::Database(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                        Some(format!("Required table '{}' is missing", table_name))
                    )));
                },
                Ok(1) => debug!("Verified table '{}' exists", table_name),
                Err(e) => {
                    error!("Error checking for table '{}': {}", table_name, e);
                    return Err(HashManagerError::Database(e));
                },
                _ => {
                    error!("Unexpected count when checking for table '{}'", table_name);
                    return Err(HashManagerError::Database(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                        Some(format!("Unexpected result for table '{}", table_name))
                    )));
                }
            }
        }

        // Start transaction
        conn.execute("BEGIN TRANSACTION", [])?;
        debug!("Started database transaction for recording download");

        let result = (|| -> HashManagerResult<()> {
            // Insert or update download record
            conn.execute(
                "INSERT OR REPLACE INTO downloads 
                 (post_id, md5, file_path, file_size, downloaded_at, is_blacklisted) 
                 VALUES (?1, ?2, ?3, ?4, datetime('now'), ?5)",
                params![
                    job.post_id,
                    &job.md5,
                    &file_path.to_string_lossy().to_string(),
                    file_size,
                    job.is_blacklisted
                ],
            ).map_err(|e| {
                error!("Failed to insert into downloads table: {}", e);
                HashManagerError::Database(e)
            })?;
            debug!("Inserted download record into downloads table");

            // Clear existing tags and artists for this post
            conn.execute("DELETE FROM download_tags WHERE post_id = ?1", params![job.post_id])
                .map_err(|e| {
                    error!("Failed to delete existing tags: {}", e);
                    HashManagerError::Database(e)
                })?;
            
            conn.execute("DELETE FROM download_artists WHERE post_id = ?1", params![job.post_id])
                .map_err(|e| {
                    error!("Failed to delete existing artists: {}", e);
                    HashManagerError::Database(e)
                })?;
            debug!("Cleared existing tags and artists for post_id={}", job.post_id);

            // Insert tags
            for tag in &job.tags {
                conn.execute(
                    "INSERT OR IGNORE INTO download_tags (post_id, tag) VALUES (?1, ?2)",
                    params![job.post_id, tag],
                ).map_err(|e| {
                    error!("Failed to insert tag '{}' for post_id={}: {}", tag, job.post_id, e);
                    HashManagerError::Database(e)
                })?;
            }
            debug!("Inserted {} tags for post_id={}", job.tags.len(), job.post_id);

            // Insert artists
            for artist in &job.artists {
                conn.execute(
                    "INSERT OR IGNORE INTO download_artists (post_id, artist) VALUES (?1, ?2)",
                    params![job.post_id, artist],
                ).map_err(|e| {
                    error!("Failed to insert artist '{}' for post_id={}: {}", artist, job.post_id, e);
                    HashManagerError::Database(e)
                })?;
            }
            debug!("Inserted {} artists for post_id={}", job.artists.len(), job.post_id);

            Ok(())
        })();

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
            debug!("Committed database transaction for post_id={}", job.post_id);
            
            // Update cache
            {
                let mut md5_cache = self.md5_cache.write();
                let mut post_id_cache = self.post_id_cache.write();
                let mut file_path_cache = self.file_path_cache.write();
                
                md5_cache.insert(job.md5.clone());
                post_id_cache.insert(job.post_id);
                file_path_cache.insert(job.md5.clone(), file_path.to_path_buf());
            }
            debug!("Updated in-memory caches for post_id={}", job.post_id);
            
            debug!("Successfully recorded download: post_id={}, md5={}, path={}", 
                job.post_id, job.md5, file_path.display());
        } else {
            conn.execute("ROLLBACK", [])?;
            error!("Rolled back database transaction for post_id={} due to error: {:?}", job.post_id, result);
        }

        result
    }

    /// Record file integrity hashes
    pub async fn record_file_integrity(&self, post_id: u32, file_path: &Path, blake3_hash: Option<&str>, sha256_hash: Option<&str>) -> HashManagerResult<()> {
        let conn = self.db_connection.lock().await;
        
        conn.execute(
            "INSERT OR REPLACE INTO file_integrity 
             (post_id, file_path, blake3_hash, sha256_hash) 
             VALUES (?1, ?2, ?3, ?4)",
            params![
                post_id,
                &file_path.to_string_lossy().to_string(),
                blake3_hash,
                sha256_hash
            ],
        )?;

        debug!("Recorded file integrity: post_id={}, path={}", post_id, file_path.display());
        Ok(())
    }

    /// Get download statistics
    pub async fn get_statistics(&self) -> HashManagerResult<DownloadStatistics> {
        let conn = self.db_connection.lock().await;

        let total_downloads: i64 = conn.query_row(
            "SELECT COUNT(*) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);

        let total_size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(file_size), 0) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);

        let blacklisted_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM downloads WHERE is_blacklisted = 1", [], |row| row.get(0)
        ).unwrap_or(0);

        let earliest_download: Option<String> = conn.query_row(
            "SELECT MIN(downloaded_at) FROM downloads", [], |row| row.get(0)
        ).ok();

        let latest_download: Option<String> = conn.query_row(
            "SELECT MAX(downloaded_at) FROM downloads", [], |row| row.get(0)
        ).ok();

        Ok(DownloadStatistics {
            total_downloads: total_downloads as u64,
            total_size_bytes: total_size as u64,
            blacklisted_count: blacklisted_count as u64,
            earliest_download,
            latest_download,
        })
    }

    /// Search downloads by tag
    pub async fn search_by_tag(&self, tag: &str, limit: usize) -> HashManagerResult<Vec<DownloadRecord>> {
        let conn = self.db_connection.lock().await;
        
        let mut stmt = conn.prepare(
            "SELECT DISTINCT d.post_id, d.md5, d.file_path, d.file_size, d.downloaded_at, d.is_blacklisted
             FROM downloads d
             JOIN download_tags dt ON d.post_id = dt.post_id
             WHERE dt.tag LIKE ?1
             ORDER BY d.downloaded_at DESC
             LIMIT ?2"
        )?;

        let search_pattern = format!("%{}%", tag);
        let rows = stmt.query_map(params![search_pattern, limit], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let md5: String = row.get(1)?;
            let file_path: String = row.get(2)?;
            let file_size: i64 = row.get(3)?;
            let downloaded_at: String = row.get(4)?;
            let is_blacklisted: bool = row.get(5)?;
            
            Ok((post_id, md5, file_path, file_size, downloaded_at, is_blacklisted))
        })?;

        let mut results = Vec::new();
        for row in rows {
            if let Ok((post_id, md5, file_path, file_size, downloaded_at_str, is_blacklisted)) = row {
                // Get tags and artists for this post
                let tags = self.get_tags_for_post(post_id).await?;
                let artists = self.get_artists_for_post(post_id).await?;
                
                let downloaded_at = if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&downloaded_at_str) {
                    timestamp.into()
                } else {
                    SystemTime::now()
                };
                
                results.push(DownloadRecord {
                    post_id,
                    md5,
                    file_path: PathBuf::from(file_path),
                    file_size: file_size as u64,
                    downloaded_at,
                    tags,
                    artists,
                    is_blacklisted,
                });
            }
        }

        Ok(results)
    }
    
    /// Search downloads by artist
    pub async fn search_by_artist(&self, artist: &str, limit: usize) -> HashManagerResult<Vec<DownloadRecord>> {
        let conn = self.db_connection.lock().await;
        
        let mut stmt = conn.prepare(
            "SELECT DISTINCT d.post_id, d.md5, d.file_path, d.file_size, d.downloaded_at, d.is_blacklisted
             FROM downloads d
             JOIN download_artists da ON d.post_id = da.post_id
             WHERE da.artist LIKE ?1
             ORDER BY d.downloaded_at DESC
             LIMIT ?2"
        )?;

        let search_pattern = format!("%{}%", artist);
        let rows = stmt.query_map(params![search_pattern, limit], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let md5: String = row.get(1)?;
            let file_path: String = row.get(2)?;
            let file_size: i64 = row.get(3)?;
            let downloaded_at: String = row.get(4)?;
            let is_blacklisted: bool = row.get(5)?;
            
            Ok((post_id, md5, file_path, file_size, downloaded_at, is_blacklisted))
        })?;

        let mut results = Vec::new();
        for row in rows {
            if let Ok((post_id, md5, file_path, file_size, downloaded_at_str, is_blacklisted)) = row {
                // Get tags and artists for this post
                let tags = self.get_tags_for_post(post_id).await?;
                let artists = self.get_artists_for_post(post_id).await?;
                
                let downloaded_at = if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&downloaded_at_str) {
                    timestamp.into()
                } else {
                    SystemTime::now()
                };
                
                results.push(DownloadRecord {
                    post_id,
                    md5,
                    file_path: PathBuf::from(file_path),
                    file_size: file_size as u64,
                    downloaded_at,
                    tags,
                    artists,
                    is_blacklisted,
                });
            }
        }

        Ok(results)
    }

    /// Get tags for a specific post
    pub async fn get_tags_for_post(&self, post_id: u32) -> HashManagerResult<Vec<String>> {
        let conn = self.db_connection.lock().await;
        
        let mut stmt = conn.prepare("SELECT tag FROM download_tags WHERE post_id = ? ORDER BY tag")?;
        let rows = stmt.query_map([post_id], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;

        let mut tags = Vec::new();
        for row in rows {
            if let Ok(tag) = row {
                tags.push(tag);
            }
        }

        Ok(tags)
    }
    
    /// Get artists for a specific post
    pub async fn get_artists_for_post(&self, post_id: u32) -> HashManagerResult<Vec<String>> {
        let conn = self.db_connection.lock().await;
        
        let mut stmt = conn.prepare("SELECT artist FROM download_artists WHERE post_id = ? ORDER BY artist")?;
        let rows = stmt.query_map([post_id], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;

        let mut artists = Vec::new();
        for row in rows {
            if let Ok(artist) = row {
                artists.push(artist);
            }
        }

        Ok(artists)
    }

    /// Cleanup orphaned database entries (files that no longer exist)
    pub async fn cleanup_orphaned_entries(&self) -> HashManagerResult<u32> {
        let conn = self.db_connection.lock().await;
        
        // Get all file paths
        let mut stmt = conn.prepare("SELECT post_id, file_path FROM downloads")?;
        let rows = stmt.query_map([], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let file_path: String = row.get(1)?;
            Ok((post_id, file_path))
        })?;

        let mut orphaned_post_ids = Vec::new();
        for row in rows {
            if let Ok((post_id, file_path)) = row {
                let path = Path::new(&file_path);
                if !path.exists() {
                    orphaned_post_ids.push(post_id);
                }
            }
        }

        if orphaned_post_ids.is_empty() {
            return Ok(0);
        }

        // Start transaction
        conn.execute("BEGIN TRANSACTION", [])?;

        let result = (|| -> HashManagerResult<u32> {
            let mut removed_count = 0u32;

            for post_id in &orphaned_post_ids {
                // Remove from downloads table
                conn.execute("DELETE FROM downloads WHERE post_id = ?1", params![post_id])?;
                
                // Remove from tags table
                conn.execute("DELETE FROM download_tags WHERE post_id = ?1", params![post_id])?;
                
                // Remove from artists table
                conn.execute("DELETE FROM download_artists WHERE post_id = ?1", params![post_id])?;
                
                // Remove from integrity table
                conn.execute("DELETE FROM file_integrity WHERE post_id = ?1", params![post_id])?;
                
                removed_count += 1;
            }

            Ok(removed_count)
        })();

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
            
            // Reload cache after cleanup
            self.reload_cache().await?;
            
            info!("Cleaned up {} orphaned database entries", result.as_ref().unwrap());
        } else {
            conn.execute("ROLLBACK", [])?;
        }

        result
    }
}

/// Statistics about downloads
#[derive(Debug, Clone)]
pub struct DownloadStatistics {
    pub total_downloads: u64,
    pub total_size_bytes: u64,
    pub blacklisted_count: u64,
    pub earliest_download: Option<String>,
    pub latest_download: Option<String>,
}

/// Initialize hash manager
pub async fn init_hash_manager(config_manager: Arc<ConfigManager>) -> HashManagerResult<Arc<HashManager>> {
    let hash_manager = HashManager::new(config_manager)?;
    let hash_manager = Arc::new(hash_manager);
    
    // Load existing data into cache
    hash_manager.reload_cache().await?;
    
    Ok(hash_manager)
}
