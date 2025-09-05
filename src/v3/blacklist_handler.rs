//! Blacklist-Aware Download Handling for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Flagging posts matching blacklist tags
//! 2. Redirecting downloads to ./blacklisted_downloads/<artist>/
//! 3. Logging and hashing blacklisted content
//! 4. Tracking blacklisted rejects in a database table
//! 5. Watching for deletion and avoiding redownloading

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error};

use crate::v3::{
    ConfigManager,
    DownloadJob,
};

/// Error types for blacklist handling
#[derive(Error, Debug)]
pub enum BlacklistHandlerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Blacklist handling error: {0}")]
    Handling(String),
}

/// Result type for blacklist handling operations
pub type BlacklistHandlerResult<T> = Result<T, BlacklistHandlerError>;

/// Blacklisted reject record
#[derive(Debug, Clone)]
pub struct BlacklistedReject {
    pub post_id: u32,
    pub deleted_at: Option<i64>,
}

/// Blacklist handler for managing blacklisted content
pub struct BlacklistHandler {
    config_manager: Arc<ConfigManager>,
    db_connection: Mutex<Connection>,
    blacklisted_dir: PathBuf,
}

impl BlacklistHandler {
    /// Create a new blacklist handler
    pub async fn new(config_manager: Arc<ConfigManager>, db_path: &Path) -> BlacklistHandlerResult<Self> {
        // Create the database connection
        let db_connection = Connection::open(db_path)?;

        // Create the blacklisted_rejects table if it doesn't exist
        db_connection.execute(
            "CREATE TABLE IF NOT EXISTS blacklisted_rejects (
                post_id INTEGER PRIMARY KEY,
                deleted_at INTEGER
            )",
            [],
        )?;

        // Get the app config
        let _app_config = config_manager.get_app_config()
            .map_err(|e| BlacklistHandlerError::Config(e.to_string()))?;

        // Create the blacklisted downloads directory
        let blacklisted_dir = PathBuf::from("./blacklisted_downloads");
        if !blacklisted_dir.exists() {
            fs::create_dir_all(&blacklisted_dir)?;
        }

        Ok(Self {
            config_manager,
            db_connection: Mutex::new(db_connection),
            blacklisted_dir,
        })
    }

    /// Create a new blacklist handler from app config
    pub async fn from_config(config_manager: Arc<ConfigManager>) -> BlacklistHandlerResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| BlacklistHandlerError::Config(e.to_string()))?;

        // Create the database path
        let db_path = Path::new(&app_config.paths.database_file);

        // Create the parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        Self::new(config_manager, db_path).await
    }

    /// Check if a post is blacklisted
    pub fn is_blacklisted(&self, tags: &[String]) -> BlacklistHandlerResult<bool> {
        // Get the rules config
        let rules_config = self.config_manager.get_rules_config()
            .map_err(|e| BlacklistHandlerError::Config(e.to_string()))?;

        // Check if any tag is in the whitelist
        for tag in tags {
            if rules_config.whitelist.tags.contains(tag) {
                return Ok(false);
            }
        }

        // Check if any tag is in the blacklist
        for tag in tags {
            if rules_config.blacklist.tags.contains(tag) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get the target directory for a blacklisted download
    pub fn get_blacklisted_directory(&self, job: &DownloadJob) -> BlacklistHandlerResult<PathBuf> {
        // Extract artist tags
        let artists = job.tags.iter()
            .filter(|tag| tag.starts_with("artist:"))
            .map(|tag| tag.trim_start_matches("artist:"))
            .collect::<Vec<_>>();

        // Create the artist directory
        let artist_dir = if !artists.is_empty() {
            self.blacklisted_dir.join(sanitize_filename(&artists[0]))
        } else {
            self.blacklisted_dir.join("unknown_artist")
        };

        // Create the directory if it doesn't exist
        if !artist_dir.exists() {
            fs::create_dir_all(&artist_dir)?;
        }

        Ok(artist_dir)
    }

    /// Add a blacklisted reject to the database
    pub async fn add_blacklisted_reject(&self, post_id: u32) -> BlacklistHandlerResult<()> {
        // Insert the reject record
        let conn = self.db_connection.lock().await;
        conn.execute(
            "INSERT OR REPLACE INTO blacklisted_rejects (post_id, deleted_at) VALUES (?1, NULL)",
            params![post_id],
        )?;

        Ok(())
    }

    /// Mark a blacklisted reject as deleted
    pub async fn mark_as_deleted(&self, post_id: u32) -> BlacklistHandlerResult<()> {
        // Get the current timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Update the deleted_at timestamp
        let conn = self.db_connection.lock().await;
        conn.execute(
            "UPDATE blacklisted_rejects SET deleted_at = ?1 WHERE post_id = ?2",
            params![now, post_id],
        )?;

        Ok(())
    }

    /// Check if a post is a deleted blacklisted reject
    pub async fn is_deleted_blacklisted_reject(&self, post_id: u32) -> BlacklistHandlerResult<bool> {
        // Query the database
        let conn = self.db_connection.lock().await;
        let mut stmt = conn.prepare(
            "SELECT deleted_at FROM blacklisted_rejects WHERE post_id = ?1"
        )?;

        let result = stmt.query_row(params![post_id], |row| {
            let deleted_at: Option<i64> = row.get(0)?;
            Ok(deleted_at.is_some())
        });

        match result {
            Ok(is_deleted) => Ok(is_deleted),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(BlacklistHandlerError::Database(e)),
        }
    }

    /// Get all blacklisted rejects
    pub async fn get_all_blacklisted_rejects(&self) -> BlacklistHandlerResult<Vec<BlacklistedReject>> {
        // Query the database
        let conn = self.db_connection.lock().await;
        let mut stmt = conn.prepare(
            "SELECT post_id, deleted_at FROM blacklisted_rejects"
        )?;

        let reject_iter = stmt.query_map([], |row| {
            Ok(BlacklistedReject {
                post_id: row.get(0)?,
                deleted_at: row.get(1)?,
            })
        })?;

        let mut rejects = Vec::new();
        for reject_result in reject_iter {
            if let Ok(reject) = reject_result {
                rejects.push(reject);
            }
        }

        Ok(rejects)
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

/// Create a new blacklist handler
pub async fn init_blacklist_handler(
    config_manager: Arc<ConfigManager>,
) -> BlacklistHandlerResult<Arc<BlacklistHandler>> {
    let handler = BlacklistHandler::from_config(config_manager).await?;
    Ok(Arc::new(handler))
}
