//! Database Schema Management for E621 Downloader V3
//!
//! This module handles database schema initialization and migrations
//! with proper versioning and safety checks.

use std::sync::Arc;
use rusqlite::{Connection, Result as SqliteResult};
use tracing::{info, warn, error};

use super::{DatabasePool, PoolError, PoolResult};

/// Database schema version
const SCHEMA_VERSION: u32 = 1;

/// Initialize database schema with all required tables and indexes
pub async fn initialize_schema(pool: &Arc<DatabasePool>) -> PoolResult<()> {
    let conn = pool.get_write_connection().await?;
    
    // Check current schema version
    let current_version = get_schema_version(conn.connection())?;
    
    if current_version == 0 {
        // Fresh database - create all tables
        create_initial_schema(conn.connection()).await?;
        set_schema_version(conn.connection(), SCHEMA_VERSION)?;
        info!("Database schema initialized to version {}", SCHEMA_VERSION);
    } else if current_version < SCHEMA_VERSION {
        // Run migrations
        run_migrations(conn.connection(), current_version).await?;
        set_schema_version(conn.connection(), SCHEMA_VERSION)?;
        info!("Database schema migrated from version {} to {}", current_version, SCHEMA_VERSION);
    } else if current_version > SCHEMA_VERSION {
        error!("Database schema version {} is newer than supported version {}", 
               current_version, SCHEMA_VERSION);
        return Err(PoolError::InvalidConfig(
            "Database schema is newer than supported version".to_string()
        ));
    }
    
    Ok(())
}

/// Create initial database schema
async fn create_initial_schema(conn: &Connection) -> PoolResult<()> {
    // Enable foreign key support
    conn.execute("PRAGMA foreign_keys = ON", [])?;
    
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
    )?;

    // Tags table - stores tags for downloaded posts
    conn.execute(
        "CREATE TABLE IF NOT EXISTS download_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES downloads(post_id) ON DELETE CASCADE,
            UNIQUE(post_id, tag)
        )",
        [],
    )?;

    // Artists table - stores artists for downloaded posts
    conn.execute(
        "CREATE TABLE IF NOT EXISTS download_artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES downloads(post_id) ON DELETE CASCADE,
            UNIQUE(post_id, artist)
        )",
        [],
    )?;

    // File integrity table - stores actual file hashes for verification
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_integrity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            blake3_hash TEXT,
            sha256_hash TEXT,
            verified_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(post_id) REFERENCES downloads(post_id) ON DELETE CASCADE,
            UNIQUE(post_id)
        )",
        [],
    )?;

    // Schema version table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY,
            version INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )",
        [],
    )?;

    create_indexes(conn).await?;
    
    info!("Initial database schema created successfully");
    Ok(())
}

/// Create database indexes for optimal performance
async fn create_indexes(conn: &Connection) -> PoolResult<()> {
    // Primary lookup indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_md5 ON downloads(md5)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_post_id ON downloads(post_id)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_path ON downloads(file_path)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_downloaded_at ON downloads(downloaded_at)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_blacklisted ON downloads(is_blacklisted)", [])?;
    
    // Tag search indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_post_id ON download_tags(post_id)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_tag ON download_tags(tag)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_tag_lower ON download_tags(LOWER(tag))", [])?;
    
    // Artist search indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_post_id ON download_artists(post_id)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_artist ON download_artists(artist)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_artist_lower ON download_artists(LOWER(artist))", [])?;
    
    // File integrity indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_integrity_post_id ON file_integrity(post_id)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_integrity_blake3 ON file_integrity(blake3_hash)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_integrity_sha256 ON file_integrity(sha256_hash)", [])?;
    
    info!("Database indexes created successfully");
    Ok(())
}

/// Run database migrations between schema versions
async fn run_migrations(conn: &Connection, from_version: u32) -> PoolResult<()> {
    match from_version {
        0 => {
            // This shouldn't happen as version 0 means fresh database
            warn!("Running migrations from version 0 - this should use initial schema creation");
            create_initial_schema(conn).await?;
        }
        // Future migrations would go here
        // 1 => migrate_from_v1_to_v2(conn).await?,
        // 2 => migrate_from_v2_to_v3(conn).await?,
        _ => {
            return Err(PoolError::InvalidConfig(
                format!("No migration path from version {}", from_version)
            ));
        }
    }
    
    Ok(())
}

/// Get current schema version
fn get_schema_version(conn: &Connection) -> PoolResult<u32> {
    // Check if schema_version table exists
    let table_exists: bool = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_version'",
        [],
        |row| {
            let count: i64 = row.get(0)?;
            Ok(count > 0)
        }
    )?;
    
    if !table_exists {
        return Ok(0); // Version 0 means no schema
    }
    
    // Get the current version
    let version: u32 = conn.query_row(
        "SELECT version FROM schema_version WHERE id = 1",
        [],
        |row| row.get(0)
    ).unwrap_or(0);
    
    Ok(version)
}

/// Set schema version
fn set_schema_version(conn: &Connection, version: u32) -> PoolResult<()> {
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (id, version) VALUES (1, ?1)",
        [version],
    )?;
    
    Ok(())
}

/// Verify schema integrity
pub async fn verify_schema_integrity(pool: &Arc<DatabasePool>) -> PoolResult<bool> {
    let conn = pool.get_read_connection().await?;
    
    // Check that all expected tables exist
    let required_tables = [
        "downloads",
        "download_tags", 
        "download_artists",
        "file_integrity",
        "schema_version"
    ];
    
    for table_name in &required_tables {
        let exists: bool = conn.connection().query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [table_name],
            |row| {
                let count: i64 = row.get(0)?;
                Ok(count > 0)
            }
        )?;
        
        if !exists {
            error!("Required table '{}' does not exist", table_name);
            return Ok(false);
        }
    }
    
    // Check foreign key constraints
    let fk_enabled: bool = conn.connection().query_row(
        "PRAGMA foreign_keys",
        [],
        |row| {
            let enabled: i64 = row.get(0)?;
            Ok(enabled == 1)
        }
    )?;
    
    if !fk_enabled {
        warn!("Foreign key constraints are not enabled");
    }
    
    // Run basic integrity check
    let integrity_ok: String = conn.connection().query_row(
        "PRAGMA integrity_check(1)",
        [],
        |row| row.get(0)
    )?;
    
    if integrity_ok != "ok" {
        error!("Database integrity check failed: {}", integrity_ok);
        return Ok(false);
    }
    
    info!("Database schema integrity verified successfully");
    Ok(true)
}

/// Get database statistics for monitoring
pub async fn get_database_info(pool: &Arc<DatabasePool>) -> PoolResult<DatabaseInfo> {
    let conn = pool.get_read_connection().await?;
    
    // Get database file size
    let page_count: i64 = conn.connection().query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.connection().query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let file_size = page_count * page_size;
    
    // Get table row counts
    let download_count: i64 = conn.connection().query_row("SELECT COUNT(*) FROM downloads", [], |row| row.get(0))?;
    let tag_count: i64 = conn.connection().query_row("SELECT COUNT(*) FROM download_tags", [], |row| row.get(0))?;
    let artist_count: i64 = conn.connection().query_row("SELECT COUNT(*) FROM download_artists", [], |row| row.get(0))?;
    let integrity_count: i64 = conn.connection().query_row("SELECT COUNT(*) FROM file_integrity", [], |row| row.get(0))?;
    
    // Get unique counts
    let unique_tags: i64 = conn.connection().query_row("SELECT COUNT(DISTINCT tag) FROM download_tags", [], |row| row.get(0))?;
    let unique_artists: i64 = conn.connection().query_row("SELECT COUNT(DISTINCT artist) FROM download_artists", [], |row| row.get(0))?;
    
    // Get schema version
    let schema_version = get_schema_version(conn.connection())?;
    
    Ok(DatabaseInfo {
        schema_version,
        file_size_bytes: file_size as u64,
        download_count: download_count as u64,
        tag_count: tag_count as u64,
        artist_count: artist_count as u64,
        integrity_count: integrity_count as u64,
        unique_tags: unique_tags as u64,
        unique_artists: unique_artists as u64,
    })
}

/// Database information structure
#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub schema_version: u32,
    pub file_size_bytes: u64,
    pub download_count: u64,
    pub tag_count: u64,
    pub artist_count: u64,
    pub integrity_count: u64,
    pub unique_tags: u64,
    pub unique_artists: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::v3::database::{init_database_pool, PoolConfig};
    
    #[tokio::test]
    async fn test_schema_initialization() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let pool = init_database_pool(&db_path, Some(PoolConfig::default())).await.unwrap();
        
        // Initialize schema
        initialize_schema(&pool).await.unwrap();
        
        // Verify schema integrity
        let integrity_ok = verify_schema_integrity(&pool).await.unwrap();
        assert!(integrity_ok);
        
        // Get database info
        let db_info = get_database_info(&pool).await.unwrap();
        assert_eq!(db_info.schema_version, SCHEMA_VERSION);
        assert_eq!(db_info.download_count, 0);
    }
}
