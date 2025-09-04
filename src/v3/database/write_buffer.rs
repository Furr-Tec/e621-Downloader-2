//! Database Write Buffer System for E621 Downloader V3
//!
//! This module provides a high-performance write buffer that batches database operations
//! to reduce lock contention and improve throughput during concurrent downloads.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;
use rusqlite::{params, Connection};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::v3::{DownloadJob, HashManagerError};
use super::connection_pool::{DatabasePool, PoolError};

/// Write buffer errors
#[derive(Error, Debug)]
pub enum WriteBufferError {
    #[error("Database pool error: {0}")]
    Pool(#[from] PoolError),
    
    #[error("Hash manager error: {0}")]
    HashManager(#[from] HashManagerError),
    
    #[error("Buffer overflow - too many pending operations")]
    BufferOverflow,
    
    #[error("Buffer is shutting down")]
    ShuttingDown,
    
    #[error("Operation timeout: {0}")]
    Timeout(String),
}

pub type WriteBufferResult<T> = Result<T, WriteBufferError>;

/// Write buffer configuration
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// Maximum number of operations to buffer before forcing a flush
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing pending operations
    pub flush_interval: Duration,
    /// Maximum buffer size before rejecting new operations
    pub max_buffer_size: usize,
    /// Number of retry attempts for failed operations
    pub max_retries: usize,
    /// Base delay for exponential backoff
    pub retry_base_delay: Duration,
    /// Maximum delay for exponential backoff
    pub max_retry_delay: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 50,
            flush_interval: Duration::from_millis(100),
            max_buffer_size: 1000,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
        }
    }
}

/// Priority levels for write operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WritePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Types of database write operations
#[derive(Debug, Clone)]
pub enum WriteOperation {
    /// Record a successful download
    RecordDownload {
        job: DownloadJob,
        file_path: PathBuf,
        file_size: u64,
        downloaded_at: SystemTime,
    },
    /// Record file integrity hashes
    RecordFileIntegrity {
        post_id: u32,
        file_path: PathBuf,
        blake3_hash: Option<String>,
        sha256_hash: Option<String>,
    },
    /// Update blacklist status
    UpdateBlacklistStatus {
        post_id: u32,
        is_blacklisted: bool,
    },
    /// Cleanup orphaned entries
    CleanupOrphanedEntry {
        post_id: u32,
    },
}

/// Statistics for write buffer performance
#[derive(Debug, Default, Clone)]
pub struct WriteBufferStats {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub retried_operations: u64,
    pub batches_processed: u64,
    pub operations_per_batch: f64,
    pub average_flush_time_ms: f64,
    pub buffer_overflow_count: u64,
    pub current_buffer_size: usize,
}

/// Buffered write operation with metadata
#[derive(Debug)]
struct BufferedWriteOp {
    operation: WriteOperation,
    priority: WritePriority,
    created_at: Instant,
    retry_count: usize,
    response_sender: Option<oneshot::Sender<WriteBufferResult<()>>>,
}

/// Write buffer for batching database operations
pub struct DatabaseWriteBuffer {
    pool: Arc<DatabasePool>,
    config: WriteBufferConfig,
    buffer: Arc<TokioMutex<VecDeque<BufferedWriteOp>>>,
    stats: Arc<RwLock<WriteBufferStats>>,
    flush_trigger: mpsc::Sender<()>,
    shutdown_trigger: mpsc::Sender<()>,
    is_running: Arc<RwLock<bool>>,
}

impl DatabaseWriteBuffer {
    /// Create a new write buffer
    pub fn new(pool: Arc<DatabasePool>, config: WriteBufferConfig) -> Self {
        let (flush_trigger, _) = mpsc::channel(1);
        let (shutdown_trigger, _) = mpsc::channel(1);
        
        Self {
            pool,
            config,
            buffer: Arc::new(TokioMutex::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(WriteBufferStats::default())),
            flush_trigger,
            shutdown_trigger,
            is_running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Start the write buffer background task
    pub async fn start(&self) -> WriteBufferResult<()> {
        if *self.is_running.read() {
            return Ok(()); // Already running
        }
        
        *self.is_running.write() = true;
        
        let buffer = self.buffer.clone();
        let stats = self.stats.clone();
        let pool = self.pool.clone();
        let config = self.config.clone();
        let is_running = self.is_running.clone();
        let (_, mut flush_rx) = mpsc::channel(1);
        let (_, mut shutdown_rx) = mpsc::channel(1);
        
        // Start the background flush task
        tokio::spawn(async move {
            let mut interval = interval(config.flush_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            
            info!("Database write buffer started");
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::flush_buffer(&buffer, &stats, &pool, &config).await {
                            error!("Failed to flush write buffer: {}", e);
                        }
                    }
                    _ = flush_rx.recv() => {
                        if let Err(e) = Self::flush_buffer(&buffer, &stats, &pool, &config).await {
                            error!("Failed to flush write buffer on trigger: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Write buffer shutdown requested");
                        break;
                    }
                }
                
                if !*is_running.read() {
                    break;
                }
            }
            
            // Final flush on shutdown
            if let Err(e) = Self::flush_buffer(&buffer, &stats, &pool, &config).await {
                error!("Failed final flush during shutdown: {}", e);
            }
            
            info!("Database write buffer stopped");
        });
        
        Ok(())
    }
    
    /// Add a write operation to the buffer
    pub async fn enqueue_operation(
        &self,
        operation: WriteOperation,
        priority: WritePriority,
    ) -> WriteBufferResult<()> {
        if !*self.is_running.read() {
            return Err(WriteBufferError::ShuttingDown);
        }
        
        let mut buffer = self.buffer.lock().await;
        
        // Check buffer overflow
        if buffer.len() >= self.config.max_buffer_size {
            self.stats.write().buffer_overflow_count += 1;
            return Err(WriteBufferError::BufferOverflow);
        }
        
        let buffered_op = BufferedWriteOp {
            operation,
            priority,
            created_at: Instant::now(),
            retry_count: 0,
            response_sender: None,
        };
        
        // Insert based on priority (higher priority first)
        let insert_pos = buffer.iter().position(|op| op.priority < priority)
            .unwrap_or(buffer.len());
        buffer.insert(insert_pos, buffered_op);
        
        let mut stats = self.stats.write();
        stats.total_operations += 1;
        stats.current_buffer_size = buffer.len();
        
        // Trigger flush if we've reached the batch size
        if buffer.len() >= self.config.max_batch_size {
            let _ = self.flush_trigger.try_send(());
        }
        
        Ok(())
    }
    
    /// Force an immediate flush of the buffer
    pub async fn flush(&self) -> WriteBufferResult<()> {
        Self::flush_buffer(&self.buffer, &self.stats, &self.pool, &self.config).await
    }
    
    /// Internal flush implementation
    async fn flush_buffer(
        buffer: &Arc<TokioMutex<VecDeque<BufferedWriteOp>>>,
        stats: &Arc<RwLock<WriteBufferStats>>,
        pool: &Arc<DatabasePool>,
        config: &WriteBufferConfig,
    ) -> WriteBufferResult<()> {
        let start_time = Instant::now();
        let mut buffer = buffer.lock().await;
        
        if buffer.is_empty() {
            return Ok(());
        }
        
        let batch_size = std::cmp::min(buffer.len(), config.max_batch_size);
        let mut batch = VecDeque::new();
        
        // Extract a batch of operations
        for _ in 0..batch_size {
            if let Some(op) = buffer.pop_front() {
                batch.push_back(op);
            }
        }
        
        drop(buffer); // Release lock early
        
        if batch.is_empty() {
            return Ok(());
        }
        
        debug!("Flushing batch of {} operations", batch.len());
        
        // Process the batch
        let (successful, failed) = Self::process_batch(batch, pool, config).await;
        
        // Update statistics
        let flush_time = start_time.elapsed().as_millis() as f64;
        {
            let mut stats = stats.write();
            stats.batches_processed += 1;
            stats.successful_operations += successful as u64;
            stats.failed_operations += failed as u64;
            stats.operations_per_batch = 
                ((stats.operations_per_batch * (stats.batches_processed - 1) as f64) + batch_size as f64) 
                / stats.batches_processed as f64;
            stats.average_flush_time_ms = 
                ((stats.average_flush_time_ms * (stats.batches_processed - 1) as f64) + flush_time) 
                / stats.batches_processed as f64;
        }
        
        debug!("Batch processed: {} successful, {} failed in {:.2}ms", 
               successful, failed, flush_time);
        
        Ok(())
    }
    
    /// Process a batch of write operations
    async fn process_batch(
        mut batch: VecDeque<BufferedWriteOp>,
        pool: &Arc<DatabasePool>,
        config: &WriteBufferConfig,
    ) -> (usize, usize) {
        let mut successful = 0;
        let mut failed = 0;
        
        // Get a write connection for the batch
        let conn_guard = match pool.get_write_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to get write connection for batch: {}", e);
                // Notify all operations of failure
                for op in batch {
                    if let Some(sender) = op.response_sender {
                        let _ = sender.send(Err(WriteBufferError::Pool(e.clone())));
                    }
                }
                return (0, batch.len());
            }
        };
        
        // Begin transaction
        if let Err(e) = conn_guard.connection().execute("BEGIN TRANSACTION", []) {
            error!("Failed to begin transaction: {}", e);
            for op in batch {
                if let Some(sender) = op.response_sender {
                    let _ = sender.send(Err(WriteBufferError::Pool(PoolError::Database(e.clone()))));
                }
            }
            return (0, batch.len());
        }
        
        // Process each operation in the transaction
        let mut transaction_failed = false;
        
        while let Some(mut op) = batch.pop_front() {
            let result = Self::execute_operation(&op.operation, conn_guard.connection()).await;
            
            match result {
                Ok(()) => {
                    successful += 1;
                    if let Some(sender) = op.response_sender {
                        let _ = sender.send(Ok(()));
                    }
                }
                Err(e) => {
                    // Check if we should retry
                    if op.retry_count < config.max_retries && Self::is_retryable_error(&e) {
                        op.retry_count += 1;
                        
                        // Calculate exponential backoff delay
                        let delay = std::cmp::min(
                            config.retry_base_delay * 2_u32.pow(op.retry_count as u32),
                            config.max_retry_delay,
                        );
                        
                        debug!("Retrying operation (attempt {}/{}) after {:?}", 
                               op.retry_count, config.max_retries, delay);
                        
                        // Re-queue for retry (insert at front for priority)
                        batch.push_front(op);
                        sleep(delay).await;
                        continue;
                    }
                    
                    error!("Operation failed after {} retries: {}", op.retry_count, e);
                    failed += 1;
                    transaction_failed = true;
                    
                    if let Some(sender) = op.response_sender {
                        let _ = sender.send(Err(e));
                    }
                }
            }
        }
        
        // Commit or rollback transaction
        let transaction_result = if transaction_failed {
            conn_guard.connection().execute("ROLLBACK", [])
        } else {
            conn_guard.connection().execute("COMMIT", [])
        };
        
        if let Err(e) = transaction_result {
            error!("Transaction finalization failed: {}", e);
            // Consider all operations failed if transaction fails
            return (0, successful + failed);
        }
        
        (successful, failed)
    }
    
    /// Execute a single write operation
    async fn execute_operation(
        operation: &WriteOperation,
        conn: &Connection,
    ) -> WriteBufferResult<()> {
        match operation {
            WriteOperation::RecordDownload { job, file_path, file_size, downloaded_at } => {
                Self::record_download(conn, job, file_path, *file_size, *downloaded_at).await
            }
            WriteOperation::RecordFileIntegrity { post_id, file_path, blake3_hash, sha256_hash } => {
                Self::record_file_integrity(conn, *post_id, file_path, blake3_hash.as_deref(), sha256_hash.as_deref()).await
            }
            WriteOperation::UpdateBlacklistStatus { post_id, is_blacklisted } => {
                Self::update_blacklist_status(conn, *post_id, *is_blacklisted).await
            }
            WriteOperation::CleanupOrphanedEntry { post_id } => {
                Self::cleanup_orphaned_entry(conn, *post_id).await
            }
        }
    }
    
    /// Record a download in the database
    async fn record_download(
        conn: &Connection,
        job: &DownloadJob,
        file_path: &Path,
        file_size: u64,
        downloaded_at: SystemTime,
    ) -> WriteBufferResult<()> {
        let downloaded_at_str = humantime::format_rfc3339_seconds(downloaded_at).to_string();
        
        // Insert or update download record
        conn.execute(
            "INSERT OR REPLACE INTO downloads 
             (post_id, md5, file_path, file_size, downloaded_at, is_blacklisted) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                job.post_id,
                &job.md5,
                &file_path.to_string_lossy().to_string(),
                file_size,
                &downloaded_at_str,
                job.is_blacklisted
            ],
        )?;
        
        // Clear existing tags and artists for this post
        conn.execute("DELETE FROM download_tags WHERE post_id = ?1", params![job.post_id])?;
        conn.execute("DELETE FROM download_artists WHERE post_id = ?1", params![job.post_id])?;
        
        // Insert tags
        for tag in &job.tags {
            conn.execute(
                "INSERT OR IGNORE INTO download_tags (post_id, tag) VALUES (?1, ?2)",
                params![job.post_id, tag],
            )?;
        }
        
        // Insert artists
        for artist in &job.artists {
            conn.execute(
                "INSERT OR IGNORE INTO download_artists (post_id, artist) VALUES (?1, ?2)",
                params![job.post_id, artist],
            )?;
        }
        
        Ok(())
    }
    
    /// Record file integrity hashes
    async fn record_file_integrity(
        conn: &Connection,
        post_id: u32,
        file_path: &Path,
        blake3_hash: Option<&str>,
        sha256_hash: Option<&str>,
    ) -> WriteBufferResult<()> {
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
        
        Ok(())
    }
    
    /// Update blacklist status
    async fn update_blacklist_status(
        conn: &Connection,
        post_id: u32,
        is_blacklisted: bool,
    ) -> WriteBufferResult<()> {
        conn.execute(
            "UPDATE downloads SET is_blacklisted = ?1 WHERE post_id = ?2",
            params![is_blacklisted, post_id],
        )?;
        
        Ok(())
    }
    
    /// Cleanup orphaned database entry
    async fn cleanup_orphaned_entry(
        conn: &Connection,
        post_id: u32,
    ) -> WriteBufferResult<()> {
        conn.execute("DELETE FROM downloads WHERE post_id = ?1", params![post_id])?;
        conn.execute("DELETE FROM download_tags WHERE post_id = ?1", params![post_id])?;
        conn.execute("DELETE FROM download_artists WHERE post_id = ?1", params![post_id])?;
        conn.execute("DELETE FROM file_integrity WHERE post_id = ?1", params![post_id])?;
        
        Ok(())
    }
    
    /// Check if an error is retryable
    fn is_retryable_error(error: &WriteBufferError) -> bool {
        match error {
            WriteBufferError::Pool(PoolError::Database(rusqlite::Error::SqliteFailure(err, _))) => {
                matches!(err.code, rusqlite::ErrorCode::DatabaseBusy | 
                                   rusqlite::ErrorCode::DatabaseLocked)
            }
            WriteBufferError::Pool(PoolError::ConnectionTimeout(_)) => true,
            WriteBufferError::Pool(PoolError::PoolExhausted) => true,
            _ => false,
        }
    }
    
    /// Get write buffer statistics
    pub fn get_stats(&self) -> WriteBufferStats {
        self.stats.read().clone()
    }
    
    /// Shutdown the write buffer
    pub async fn shutdown(&self) -> WriteBufferResult<()> {
        *self.is_running.write() = false;
        
        // Send shutdown signal
        let _ = self.shutdown_trigger.try_send(());
        
        // Wait a bit for the background task to finish
        sleep(Duration::from_millis(100)).await;
        
        // Final flush
        self.flush().await?;
        
        info!("Write buffer shutdown complete");
        Ok(())
    }
}

/// Create and start a database write buffer
pub async fn init_database_write_buffer(
    pool: Arc<DatabasePool>,
    config: Option<WriteBufferConfig>,
) -> WriteBufferResult<Arc<DatabaseWriteBuffer>> {
    let config = config.unwrap_or_default();
    let buffer = Arc::new(DatabaseWriteBuffer::new(pool, config));
    
    buffer.start().await?;
    
    Ok(buffer)
}
