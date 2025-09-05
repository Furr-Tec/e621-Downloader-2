//! Database Connection Pool for E621 Downloader V3
//! 
//! This module provides a high-performance connection pool specifically designed for
//! concurrent download operations with SQLite, featuring:
//! 1. Separate read/write connection pools
//! 2. WAL mode for concurrent readers
//! 3. Connection lifecycle management
//! 4. Automatic retry and failover

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rusqlite::{Connection, Error as SqliteError};
use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, Semaphore};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Connection pool errors
#[derive(Error, Debug, Clone)]
pub enum PoolError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Pool exhausted - no connections available")]
    PoolExhausted,
    
    #[error("Connection timeout after {0:?}")]
    ConnectionTimeout(Duration),
    
    #[error("Pool is shutting down")]
    ShuttingDown,
    
    #[error("Invalid pool configuration: {0}")]
    InvalidConfig(String),
}

impl From<SqliteError> for PoolError {
    fn from(error: SqliteError) -> Self {
        PoolError::Database(error.to_string())
    }
}

pub type PoolResult<T> = Result<T, PoolError>;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of read connections
    pub max_read_connections: usize,
    /// Maximum number of write connections  
    pub max_write_connections: usize,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Maximum connection idle time
    pub max_idle_time: Duration,
    /// Connection validation interval
    pub validation_interval: Duration,
    /// Enable WAL mode for concurrent readers
    pub enable_wal_mode: bool,
    /// SQLite connection pragmas
    pub pragmas: Vec<(String, String)>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_read_connections: 4,
            max_write_connections: 2,
            connection_timeout: Duration::from_secs(10),
            max_idle_time: Duration::from_secs(300), // 5 minutes
            validation_interval: Duration::from_secs(30),
            enable_wal_mode: true,
            pragmas: vec![
                ("busy_timeout".to_string(), "5000".to_string()),
                ("synchronous".to_string(), "NORMAL".to_string()),
                ("cache_size".to_string(), "-64000".to_string()), // 64MB cache
                ("temp_store".to_string(), "MEMORY".to_string()),
                ("mmap_size".to_string(), "268435456".to_string()), // 256MB mmap
            ],
        }
    }
}

/// Connection wrapper with metadata
#[derive(Debug)]
pub struct PooledConnection {
    connection: Connection,
    created_at: Instant,
    last_used: Arc<RwLock<Instant>>,
    use_count: AtomicUsize,
    is_readonly: bool,
}

impl PooledConnection {
    fn new(connection: Connection, is_readonly: bool) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used: Arc::new(RwLock::new(now)),
            use_count: AtomicUsize::new(0),
            is_readonly,
        }
    }
    
    pub fn connection(&self) -> &Connection {
        // Update last used time
        *self.last_used.write() = Instant::now();
        self.use_count.fetch_add(1, Ordering::Relaxed);
        &self.connection
    }
    
    pub fn is_stale(&self, max_idle: Duration) -> bool {
        let last_used = *self.last_used.read();
        last_used.elapsed() > max_idle
    }
    
    pub fn validate(&self) -> PoolResult<()> {
        // Simple validation query
        debug!("Validating connection with SELECT 1");
        match self.connection.query_row("SELECT 1", [], |_| Ok(())) {
            Ok(_) => {
                debug!("Connection validation succeeded");
                Ok(())
            },
            Err(e) => {
                error!("Connection validation failed: {}", e);
                Err(PoolError::Database(e.to_string()))
            },
        }
    }
}

/// Connection pool for managing SQLite connections
pub struct DatabasePool {
    db_path: PathBuf,
    config: PoolConfig,
    read_connections: Arc<TokioMutex<Vec<PooledConnection>>>,
    write_connections: Arc<TokioMutex<Vec<PooledConnection>>>,
    read_semaphore: Arc<Semaphore>,
    write_semaphore: Arc<Semaphore>,
    is_shutdown: Arc<RwLock<bool>>,
    stats: Arc<RwLock<PoolStats>>,
}

/// Connection pool statistics
#[derive(Debug, Default, Clone)]
pub struct PoolStats {
    pub total_read_connections: usize,
    pub total_write_connections: usize,
    pub active_read_connections: usize,
    pub active_write_connections: usize,
    pub connections_created: u64,
    pub connections_destroyed: u64,
    pub connection_timeouts: u64,
    pub connection_errors: u64,
    pub average_connection_time_ms: f64,
}

impl DatabasePool {
    /// Create a new database connection pool
    pub fn new(db_path: impl AsRef<Path>, config: PoolConfig) -> PoolResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        
        // Validate configuration
        if config.max_read_connections == 0 {
            return Err(PoolError::InvalidConfig("max_read_connections must be > 0".to_string()));
        }
        if config.max_write_connections == 0 {
            return Err(PoolError::InvalidConfig("max_write_connections must be > 0".to_string()));
        }
        
        // Create parent directory if needed
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| PoolError::InvalidConfig(format!("Failed to create directory: {}", e)))?;
            }
        }
        
        let pool = Self {
            db_path,
            config: config.clone(),
            read_connections: Arc::new(TokioMutex::new(Vec::new())),
            write_connections: Arc::new(TokioMutex::new(Vec::new())),
            read_semaphore: Arc::new(Semaphore::new(config.max_read_connections)),
            write_semaphore: Arc::new(Semaphore::new(config.max_write_connections)),
            is_shutdown: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(PoolStats::default())),
        };
        
        // Initialize the database schema with the first connection
        pool.init_database()?;
        
        info!("Database pool initialized: read={}, write={}", 
              config.max_read_connections, config.max_write_connections);
        
        Ok(pool)
    }
    
    /// Initialize database with schema and WAL mode
    fn init_database(&self) -> PoolResult<()> {
        let conn = self.create_raw_connection(false)?;
        
        // Enable WAL mode if configured
        if self.config.enable_wal_mode {
            debug!("Setting journal_mode = WAL");
            match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
                let mode: String = row.get(0)?;
                Ok(mode)
            }) {
                Ok(mode) => {
                    debug!("Journal mode set to: {}", mode);
                    info!("Enabled WAL mode for concurrent access");
                }
                Err(e) => {
                    error!("Failed to set journal_mode=WAL: {}", e);
                    return Err(PoolError::Database(format!("Failed to set journal_mode=WAL: {}", e)));
                }
            }
        }
        
        // Apply configuration pragmas
        for (pragma, value) in &self.config.pragmas {
            let sql = format!("PRAGMA {} = {}", pragma, value);
            debug!("Executing PRAGMA: {}", sql);
            
            // Some PRAGMAs return results, others don't. Try execute first, fall back to query_row
            if let Err(e) = conn.execute(&sql, []) {
                if e.to_string().contains("Execute returned results") {
                    debug!("PRAGMA {} returns results, using query_row", pragma);
                    match conn.query_row(&sql, [], |row| {
                        let result: String = row.get(0).unwrap_or_default();
                        Ok(result)
                    }) {
                        Ok(result) => {
                            debug!("PRAGMA {} = {} returned: {}", pragma, value, result);
                        }
                        Err(e2) => {
                            warn!("Failed to set pragma {} with query_row: {}", pragma, e2);
                        }
                    }
                } else {
                    warn!("Failed to set pragma {}: {}", pragma, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Create a raw database connection with proper configuration
    fn create_raw_connection(&self, readonly: bool) -> PoolResult<Connection> {
        let start_time = Instant::now();
        
        let conn = if readonly {
            Connection::open_with_flags(
                &self.db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )?
        } else {
            Connection::open(&self.db_path)?
        };
        
        // Apply pragmas
        for (pragma, value) in &self.config.pragmas {
            let sql = format!("PRAGMA {} = {}", pragma, value);
            
            // Some PRAGMAs return results, others don't. Try execute first, fall back to query_row
            if let Err(e) = conn.execute(&sql, []) {
                if e.to_string().contains("Execute returned results") {
                    debug!("PRAGMA {} returns results on connection, using query_row", pragma);
                    if let Err(e2) = conn.query_row(&sql, [], |row| {
                        let result: String = row.get(0).unwrap_or_default();
                        Ok(result)
                    }) {
                        debug!("Failed to set pragma {} on connection with query_row: {}", pragma, e2);
                    }
                } else {
                    debug!("Failed to set pragma {} on connection: {}", pragma, e);
                }
            }
        }
        
        // Update stats
        let connection_time = start_time.elapsed().as_millis() as f64;
        {
            let mut stats = self.stats.write();
            stats.connections_created += 1;
            stats.average_connection_time_ms = 
                (stats.average_connection_time_ms + connection_time) / 2.0;
        }
        
        debug!("Created {} connection in {:.2}ms", 
               if readonly { "read" } else { "write" }, connection_time);
        
        Ok(conn)
    }
    
    /// Get a read-only connection from the pool
    pub async fn get_read_connection(&self) -> PoolResult<PooledConnectionGuard<'_>> {
        if *self.is_shutdown.read() {
            return Err(PoolError::ShuttingDown);
        }
        
        let permit = timeout(
            self.config.connection_timeout,
            self.read_semaphore.acquire(),
        )
        .await
        .map_err(|_| PoolError::ConnectionTimeout(self.config.connection_timeout))?
        .map_err(|_| PoolError::ShuttingDown)?;
        
        let connection = self.acquire_read_connection().await?;
        
        Ok(PooledConnectionGuard {
            connection,
            _permit: permit,
            pool_stats: self.stats.clone(),
            is_read: true,
        })
    }
    
    /// Get a write connection from the pool
    pub async fn get_write_connection(&self) -> PoolResult<PooledConnectionGuard<'_>> {
        if *self.is_shutdown.read() {
            return Err(PoolError::ShuttingDown);
        }
        
        let permit = timeout(
            self.config.connection_timeout,
            self.write_semaphore.acquire(),
        )
        .await
        .map_err(|_| PoolError::ConnectionTimeout(self.config.connection_timeout))?
        .map_err(|_| PoolError::ShuttingDown)?;
        
        let connection = self.acquire_write_connection().await?;
        
        Ok(PooledConnectionGuard {
            connection,
            _permit: permit,
            pool_stats: self.stats.clone(),
            is_read: false,
        })
    }
    
    /// Acquire a read connection, creating one if necessary
    async fn acquire_read_connection(&self) -> PoolResult<PooledConnection> {
        let mut connections = self.read_connections.lock().await;
        
        // Try to reuse an existing connection
        if let Some(conn) = connections.pop() {
            // Validate connection if it's been idle too long
            if conn.is_stale(self.config.max_idle_time) {
                drop(conn); // Connection will be dropped
                self.stats.write().connections_destroyed += 1;
            } else if conn.validate().is_ok() {
                self.stats.write().active_read_connections += 1;
                return Ok(conn);
            } else {
                drop(conn); // Invalid connection
                self.stats.write().connections_destroyed += 1;
                self.stats.write().connection_errors += 1;
            }
        }
        
        // Create new connection
        match self.create_raw_connection(true) {
            Ok(raw_conn) => {
                let conn = PooledConnection::new(raw_conn, true);
                self.stats.write().active_read_connections += 1;
                self.stats.write().total_read_connections += 1;
                Ok(conn)
            }
            Err(e) => {
                self.stats.write().connection_errors += 1;
                Err(e)
            }
        }
    }
    
    /// Acquire a write connection, creating one if necessary
    async fn acquire_write_connection(&self) -> PoolResult<PooledConnection> {
        let mut connections = self.write_connections.lock().await;
        
        // Try to reuse an existing connection
        if let Some(conn) = connections.pop() {
            if conn.is_stale(self.config.max_idle_time) {
                drop(conn);
                self.stats.write().connections_destroyed += 1;
            } else if conn.validate().is_ok() {
                self.stats.write().active_write_connections += 1;
                return Ok(conn);
            } else {
                drop(conn);
                self.stats.write().connections_destroyed += 1;
                self.stats.write().connection_errors += 1;
            }
        }
        
        // Create new connection
        match self.create_raw_connection(false) {
            Ok(raw_conn) => {
                let conn = PooledConnection::new(raw_conn, false);
                self.stats.write().active_write_connections += 1;
                self.stats.write().total_write_connections += 1;
                Ok(conn)
            }
            Err(e) => {
                self.stats.write().connection_errors += 1;
                Err(e)
            }
        }
    }
    
    /// Return a connection to the pool
    async fn return_connection(&self, conn: PooledConnection) {
        if *self.is_shutdown.read() {
            return; // Don't return connections during shutdown
        }
        
        if conn.is_readonly {
            let mut connections = self.read_connections.lock().await;
            let mut stats = self.stats.write();
            
            stats.active_read_connections = stats.active_read_connections.saturating_sub(1);
            
            if connections.len() < self.config.max_read_connections && !conn.is_stale(self.config.max_idle_time) {
                connections.push(conn);
            } else {
                stats.connections_destroyed += 1;
            }
        } else {
            let mut connections = self.write_connections.lock().await;
            let mut stats = self.stats.write();
            
            stats.active_write_connections = stats.active_write_connections.saturating_sub(1);
            
            if connections.len() < self.config.max_write_connections && !conn.is_stale(self.config.max_idle_time) {
                connections.push(conn);
            } else {
                stats.connections_destroyed += 1;
            }
        }
    }
    
    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        self.stats.read().clone()
    }
    
    /// Shutdown the pool and close all connections
    pub async fn shutdown(&self) {
        *self.is_shutdown.write() = true;
        
        // Clear all connections
        {
            let mut read_conns = self.read_connections.lock().await;
            let read_count = read_conns.len();
            read_conns.clear();
            
            let mut write_conns = self.write_connections.lock().await;
            let write_count = write_conns.len();
            write_conns.clear();
            
            let mut stats = self.stats.write();
            stats.connections_destroyed += (read_count + write_count) as u64;
        }
        
        info!("Database pool shutdown complete");
    }
}

/// RAII guard for pooled connections
pub struct PooledConnectionGuard<'a> {
    connection: PooledConnection,
    _permit: tokio::sync::SemaphorePermit<'a>,
    pool_stats: Arc<RwLock<PoolStats>>,
    is_read: bool,
}

impl<'a> PooledConnectionGuard<'a> {
    pub fn connection(&self) -> &Connection {
        self.connection.connection()
    }
}

impl Drop for PooledConnectionGuard<'_> {
    fn drop(&mut self) {
        // Update stats when connection is returned
        let mut stats = self.pool_stats.write();
        if self.is_read {
            stats.active_read_connections = stats.active_read_connections.saturating_sub(1);
        } else {
            stats.active_write_connections = stats.active_write_connections.saturating_sub(1);
        }
    }
}

/// Initialize database pool with proper configuration
pub async fn init_database_pool(db_path: impl AsRef<Path>, config: Option<PoolConfig>) -> PoolResult<Arc<DatabasePool>> {
    let config = config.unwrap_or_default();
    let pool = DatabasePool::new(db_path, config)?;
    Ok(Arc::new(pool))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_pool_creation() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let pool = DatabasePool::new(&db_path, PoolConfig::default()).unwrap();
        
        let read_conn = pool.get_read_connection().await.unwrap();
        assert!(read_conn.connection().execute("SELECT 1", []).is_ok());
        
        let write_conn = pool.get_write_connection().await.unwrap();
        assert!(write_conn.connection().execute("SELECT 1", []).is_ok());
    }
    
    #[tokio::test]
    async fn test_concurrent_connections() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let pool = Arc::new(DatabasePool::new(&db_path, PoolConfig::default()).unwrap());
        
        let mut handles = vec![];
        
        // Test concurrent read connections
        for i in 0..4 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.get_read_connection().await.unwrap();
                conn.connection().execute("SELECT ?", [i]).unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                i
            }));
        }
        
        let results: Vec<i32> = futures::future::join_all(handles).await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        
        assert_eq!(results, vec![0, 1, 2, 3]);
    }
}
