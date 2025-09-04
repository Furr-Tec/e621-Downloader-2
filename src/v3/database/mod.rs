//! Database Module for E621 Downloader V3
//!
//! This module provides a unified database layer with:
//! 1. Connection pooling for concurrent operations
//! 2. Write buffering for high-throughput downloads
//! 3. Efficient query planning
//! 4. Proper transaction handling

mod connection_pool;
mod write_buffer;
mod schema;

#[cfg(test)]
mod tests;

pub use connection_pool::{
    DatabasePool, PoolConfig, PoolError, PoolResult, PoolStats, PooledConnectionGuard,
    init_database_pool,
};

pub use write_buffer::{
    DatabaseWriteBuffer, WriteBufferConfig, WriteBufferError, WriteBufferResult,
    WriteOperation, WritePriority, WriteBufferStats, init_database_write_buffer,
};

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use rusqlite::params;
use tokio::sync::oneshot;
use thiserror::Error;
use tracing::{debug, info, error, warn};

use crate::v3::{DownloadJob, HashManagerError};

/// Database errors
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Pool error: {0}")]
    Pool(#[from] PoolError),
    
    #[error("Write buffer error: {0}")]
    WriteBuffer(#[from] WriteBufferError),
    
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Query error: {0}")]
    Query(String),
    
    #[error("Hash manager error: {0}")]
    HashManager(#[from] HashManagerError),
    
    #[error("Config error: {0}")]
    Config(String),
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// Download record from the database
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

/// Download statistics
#[derive(Debug, Clone)]
pub struct DownloadStatistics {
    pub total_downloads: u64,
    pub total_size_bytes: u64,
    pub blacklisted_count: u64,
    pub earliest_download: Option<String>,
    pub latest_download: Option<String>,
}

/// Unified database facade for E621 Downloader
pub struct Database {
    pool: Arc<DatabasePool>,
    write_buffer: Arc<DatabaseWriteBuffer>,
    db_path: PathBuf,
}

impl Database {
    /// Initialize a new database with proper configuration
    pub async fn new(db_path: impl AsRef<Path>) -> DatabaseResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        
        // Initialize connection pool
        let pool_config = PoolConfig::default();
        let pool = init_database_pool(&db_path, Some(pool_config)).await?;
        
        // Initialize write buffer
        let write_buffer_config = WriteBufferConfig::default();
        let write_buffer = init_database_write_buffer(pool.clone(), Some(write_buffer_config)).await?;
        
        // Initialize schema
        schema::initialize_schema(&pool).await?;
        
        info!("Database initialized: {}", db_path.display());
        Ok(Self {
            pool,
            write_buffer,
            db_path,
        })
    }
    
    /// Record a successful download
    pub async fn record_download(&self, job: &DownloadJob, file_path: &Path) -> DatabaseResult<()> {
        let file_size = if file_path.exists() {
            std::fs::metadata(file_path)?.len()
        } else {
            job.file_size as u64
        };
        
        let operation = WriteOperation::RecordDownload {
            job: job.clone(),
            file_path: file_path.to_path_buf(),
            file_size,
            downloaded_at: SystemTime::now(),
        };
        
        self.write_buffer.enqueue_operation(operation, WritePriority::Normal).await?;
        Ok(())
    }
    
    /// Record file integrity hashes
    pub async fn record_file_integrity(
        &self,
        post_id: u32,
        file_path: &Path,
        blake3_hash: Option<&str>,
        sha256_hash: Option<&str>,
    ) -> DatabaseResult<()> {
        let operation = WriteOperation::RecordFileIntegrity {
            post_id,
            file_path: file_path.to_path_buf(),
            blake3_hash: blake3_hash.map(String::from),
            sha256_hash: sha256_hash.map(String::from),
        };
        
        self.write_buffer.enqueue_operation(operation, WritePriority::Low).await?;
        Ok(())
    }
    
    /// Verify if a hash exists in the database
    pub async fn hash_exists(&self, md5: &str) -> DatabaseResult<bool> {
        let conn = self.pool.get_read_connection().await?;
        
        let exists: bool = conn.connection().query_row(
            "SELECT 1 FROM downloads WHERE md5 = ?1 LIMIT 1",
            params![md5],
            |_| Ok(true),
        ).unwrap_or(false);
        
        Ok(exists)
    }
    
    /// Verify if a post ID exists in the database
    pub async fn post_exists(&self, post_id: u32) -> DatabaseResult<bool> {
        let conn = self.pool.get_read_connection().await?;
        
        let exists: bool = conn.connection().query_row(
            "SELECT 1 FROM downloads WHERE post_id = ?1 LIMIT 1",
            params![post_id],
            |_| Ok(true),
        ).unwrap_or(false);
        
        Ok(exists)
    }
    
    /// Get download record by MD5 hash
    pub async fn get_download_by_md5(&self, md5: &str) -> DatabaseResult<Option<DownloadRecord>> {
        let conn = self.pool.get_read_connection().await?;
        
        let result = conn.connection().query_row(
            "SELECT post_id, md5, file_path, file_size, downloaded_at, is_blacklisted 
             FROM downloads WHERE md5 = ?1",
            params![md5],
            |row| {
                let post_id: u32 = row.get::<_, i64>(0)? as u32;
                let md5: String = row.get(1)?;
                let file_path: String = row.get(2)?;
                let file_size: i64 = row.get(3)?;
                let downloaded_at: String = row.get(4)?;
                let is_blacklisted: bool = row.get(5)?;
                
                Ok((post_id, md5, file_path, file_size, downloaded_at, is_blacklisted))
            }
        );
        
        match result {
            Ok((post_id, md5, file_path, file_size, downloaded_at_str, is_blacklisted)) => {
                // Get tags and artists
                let tags = self.get_tags_for_post(post_id).await?;
                let artists = self.get_artists_for_post(post_id).await?;
                
                // Parse timestamp
                let downloaded_at = if let Ok(timestamp) = humantime::parse_rfc3339(&downloaded_at_str) {
                    timestamp
                } else {
                    SystemTime::now()
                };
                
                Ok(Some(DownloadRecord {
                    post_id,
                    md5,
                    file_path: PathBuf::from(file_path),
                    file_size: file_size as u64,
                    downloaded_at,
                    tags,
                    artists,
                    is_blacklisted,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DatabaseError::Query(e.to_string())),
        }
    }
    
    /// Get tags for a post
    pub async fn get_tags_for_post(&self, post_id: u32) -> DatabaseResult<Vec<String>> {
        let conn = self.pool.get_read_connection().await?;
        
        let mut stmt = conn.connection().prepare(
            "SELECT tag FROM download_tags WHERE post_id = ? ORDER BY tag"
        )?;
        
        let rows = stmt.query_map(params![post_id], |row| {
            row.get::<_, String>(0)
        })?;
        
        let mut tags = Vec::new();
        for row in rows {
            tags.push(row?);
        }
        
        Ok(tags)
    }
    
    /// Get artists for a post
    pub async fn get_artists_for_post(&self, post_id: u32) -> DatabaseResult<Vec<String>> {
        let conn = self.pool.get_read_connection().await?;
        
        let mut stmt = conn.connection().prepare(
            "SELECT artist FROM download_artists WHERE post_id = ? ORDER BY artist"
        )?;
        
        let rows = stmt.query_map(params![post_id], |row| {
            row.get::<_, String>(0)
        })?;
        
        let mut artists = Vec::new();
        for row in rows {
            artists.push(row?);
        }
        
        Ok(artists)
    }
    
    /// Search downloads by tag
    pub async fn search_by_tag(&self, tag: &str, limit: usize) -> DatabaseResult<Vec<DownloadRecord>> {
        let conn = self.pool.get_read_connection().await?;
        
        let mut stmt = conn.connection().prepare(
            "SELECT DISTINCT d.post_id, d.md5, d.file_path, d.file_size, d.downloaded_at, d.is_blacklisted
             FROM downloads d
             JOIN download_tags dt ON d.post_id = dt.post_id
             WHERE dt.tag LIKE ?1
             ORDER BY d.downloaded_at DESC
             LIMIT ?2"
        )?;
        
        let search_pattern = format!("%{}%", tag);
        let rows = stmt.query_map(params![search_pattern, limit as i64], |row| {
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
            let (post_id, md5, file_path, file_size, downloaded_at_str, is_blacklisted) = row?;
            
            // Get tags and artists
            let tags = self.get_tags_for_post(post_id).await?;
            let artists = self.get_artists_for_post(post_id).await?;
            
            // Parse timestamp
            let downloaded_at = if let Ok(timestamp) = humantime::parse_rfc3339(&downloaded_at_str) {
                timestamp
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
        
        Ok(results)
    }
    
    /// Search downloads by artist
    pub async fn search_by_artist(&self, artist: &str, limit: usize) -> DatabaseResult<Vec<DownloadRecord>> {
        let conn = self.pool.get_read_connection().await?;
        
        let mut stmt = conn.connection().prepare(
            "SELECT DISTINCT d.post_id, d.md5, d.file_path, d.file_size, d.downloaded_at, d.is_blacklisted
             FROM downloads d
             JOIN download_artists da ON d.post_id = da.post_id
             WHERE da.artist LIKE ?1
             ORDER BY d.downloaded_at DESC
             LIMIT ?2"
        )?;
        
        let search_pattern = format!("%{}%", artist);
        let rows = stmt.query_map(params![search_pattern, limit as i64], |row| {
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
            let (post_id, md5, file_path, file_size, downloaded_at_str, is_blacklisted) = row?;
            
            // Get tags and artists
            let tags = self.get_tags_for_post(post_id).await?;
            let artists = self.get_artists_for_post(post_id).await?;
            
            // Parse timestamp
            let downloaded_at = if let Ok(timestamp) = humantime::parse_rfc3339(&downloaded_at_str) {
                timestamp
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
        
        Ok(results)
    }
    
    /// Get download statistics
    pub async fn get_statistics(&self) -> DatabaseResult<DownloadStatistics> {
        let conn = self.pool.get_read_connection().await?;
        
        let total_downloads: i64 = conn.connection().query_row(
            "SELECT COUNT(*) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);
        
        let total_size: i64 = conn.connection().query_row(
            "SELECT COALESCE(SUM(file_size), 0) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);
        
        let blacklisted_count: i64 = conn.connection().query_row(
            "SELECT COUNT(*) FROM downloads WHERE is_blacklisted = 1", [], |row| row.get(0)
        ).unwrap_or(0);
        
        let earliest_download: Option<String> = conn.connection().query_row(
            "SELECT MIN(downloaded_at) FROM downloads", [], |row| row.get(0)
        ).ok();
        
        let latest_download: Option<String> = conn.connection().query_row(
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
    
    /// Cleanup orphaned database entries (files that no longer exist)
    pub async fn cleanup_orphaned_entries(&self) -> DatabaseResult<u32> {
        let conn = self.pool.get_read_connection().await?;
        
        // Get all file paths
        let mut stmt = conn.connection().prepare("SELECT post_id, file_path FROM downloads")?;
        let rows = stmt.query_map([], |row| {
            let post_id: u32 = row.get::<_, i64>(0)? as u32;
            let file_path: String = row.get(1)?;
            Ok((post_id, file_path))
        })?;
        
        let mut orphaned_count = 0;
        
        for row in rows {
            let (post_id, file_path) = row?;
            let path = Path::new(&file_path);
            
            if !path.exists() {
                // Queue cleanup operation
                let operation = WriteOperation::CleanupOrphanedEntry { post_id };
                self.write_buffer.enqueue_operation(operation, WritePriority::Low).await?;
                orphaned_count += 1;
            }
        }
        
        if orphaned_count > 0 {
            // Force flush to ensure all cleanups are processed
            self.write_buffer.flush().await?;
        }
        
        Ok(orphaned_count)
    }
    
    /// Force write buffer flush
    pub async fn flush(&self) -> DatabaseResult<()> {
        self.write_buffer.flush().await?;
        Ok(())
    }
    
    /// Get database path
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
    
    /// Get connection pool statistics
    pub fn pool_stats(&self) -> PoolStats {
        self.pool.get_stats()
    }
    
    /// Get write buffer statistics
    pub fn write_buffer_stats(&self) -> WriteBufferStats {
        self.write_buffer.get_stats()
    }
    
    /// Shutdown database connections
    pub async fn shutdown(&self) -> DatabaseResult<()> {
        // Flush pending writes
        self.write_buffer.flush().await?;
        
        // Shutdown write buffer
        self.write_buffer.shutdown().await?;
        
        // Shutdown connection pool
        self.pool.shutdown().await;
        
        info!("Database shutdown complete");
        Ok(())
    }
}

/// Initialize database with configuration
pub async fn init_database(db_path: impl AsRef<Path>) -> DatabaseResult<Arc<Database>> {
    let db = Database::new(db_path).await?;
    Ok(Arc::new(db))
}
