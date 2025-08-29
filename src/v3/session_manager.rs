//! Session Management for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Flushing all queues and logs on shutdown
//! 2. Writing latest state to disk and DB
//! 3. Releasing global lock
//! 4. Checking DB state on startup
//! 5. Resuming from last page if session was interrupted
//! 6. Validating disk size before scheduling

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use rusqlite::{Connection, Result as SqliteResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::{debug, error, info, instrument, warn};

use crate::v3::{
    ConfigManager,
    Orchestrator, OrchestratorResult,
    MetadataStore, MetadataStoreResult, PostMetadata,
    QueryQueue, Query, QueryStatus, QueryTask,
};

/// Error types for session management
#[derive(Error, Debug)]
pub enum SessionManagerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Session error: {0}")]
    Session(String),
    
    #[error("Not enough disk space: {0}")]
    DiskSpace(String),
}

/// Result type for session management operations
pub type SessionManagerResult<T> = Result<T, SessionManagerError>;

/// Session state stored in SQLite
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub session_id: String,
    pub start_time: i64,
    pub last_update_time: i64,
    pub completed_tasks: usize,
    pub pending_tasks: usize,
    pub last_page_processed: Option<u32>,
    pub current_query: Option<String>,
    pub is_complete: bool,
}

/// Session manager for handling startup and shutdown
pub struct SessionManager {
    config_manager: Arc<ConfigManager>,
    metadata_store: Arc<MetadataStore>,
    db_connection: Connection,
    session_state: Arc<RwLock<SessionState>>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new(
        config_manager: Arc<ConfigManager>,
        metadata_store: Arc<MetadataStore>,
        db_path: &Path,
    ) -> SessionManagerResult<Self> {
        // Create the database connection
        let db_connection = Connection::open(db_path)?;
        
        // Create the session_state table if it doesn't exist
        db_connection.execute(
            "CREATE TABLE IF NOT EXISTS session_state (
                session_id TEXT PRIMARY KEY,
                start_time INTEGER NOT NULL,
                last_update_time INTEGER NOT NULL,
                completed_tasks INTEGER NOT NULL,
                pending_tasks INTEGER NOT NULL,
                last_page_processed INTEGER,
                current_query TEXT,
                is_complete INTEGER NOT NULL
            )",
            [],
        )?;
        
        // Generate a new session ID
        let session_id = format!("{}", SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
        
        // Create a new session state
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        
        let session_state = SessionState {
            session_id,
            start_time: now,
            last_update_time: now,
            completed_tasks: 0,
            pending_tasks: 0,
            last_page_processed: None,
            current_query: None,
            is_complete: false,
        };
        
        Ok(Self {
            config_manager,
            metadata_store,
            db_connection,
            session_state: Arc::new(RwLock::new(session_state)),
        })
    }
    
    /// Create a new session manager from app config
    pub fn from_config(
        config_manager: Arc<ConfigManager>,
        metadata_store: Arc<MetadataStore>,
    ) -> SessionManagerResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| SessionManagerError::Config(e.to_string()))?;
        
        // Create the database path
        let db_path = Path::new(&app_config.paths.database_file);
        
        // Create the parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
        
        Self::new(config_manager, metadata_store, db_path)
    }
    
    /// Save the current session state to the database
    pub fn save_session_state(&self) -> SessionManagerResult<()> {
        // Get the current session state
        let mut session_state = self.session_state.write();
        
        // Update the last update time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        
        session_state.last_update_time = now;
        
        // Save to SQLite
        self.db_connection.execute(
            "INSERT OR REPLACE INTO session_state (
                session_id, start_time, last_update_time,
                completed_tasks, pending_tasks,
                last_page_processed, current_query, is_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                &session_state.session_id,
                &session_state.start_time.to_string(),
                &session_state.last_update_time.to_string(),
                &session_state.completed_tasks.to_string(),
                &session_state.pending_tasks.to_string(),
                &session_state.last_page_processed.map(|p| p.to_string()).unwrap_or_default(),
                &session_state.current_query.clone().unwrap_or_default(),
                &(if session_state.is_complete { 1 } else { 0 }).to_string(),
            ],
        )?;
        
        Ok(())
    }
    
    /// Load the last incomplete session state from the database
    pub fn load_last_session_state(&self) -> SessionManagerResult<Option<SessionState>> {
        // Query SQLite for the last incomplete session
        let mut stmt = self.db_connection.prepare(
            "SELECT 
                session_id, start_time, last_update_time,
                completed_tasks, pending_tasks,
                last_page_processed, current_query, is_complete
             FROM session_state
             WHERE is_complete = 0
             ORDER BY last_update_time DESC
             LIMIT 1"
        )?;
        
        let session = stmt.query_row([], |row| {
            let last_page_processed: Option<String> = row.get(5)?;
            let last_page = last_page_processed
                .and_then(|s| s.parse::<u32>().ok());
            
            Ok(SessionState {
                session_id: row.get(0)?,
                start_time: row.get(1)?,
                last_update_time: row.get(2)?,
                completed_tasks: row.get::<_, i64>(3)? as usize,
                pending_tasks: row.get::<_, i64>(4)? as usize,
                last_page_processed: last_page,
                current_query: row.get(6)?,
                is_complete: row.get::<_, i64>(7)? != 0,
            })
        });
        
        match session {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(SessionManagerError::Database(e)),
        }
    }
    
    /// Mark the current session as complete
    pub fn mark_session_complete(&self) -> SessionManagerResult<()> {
        // Get the current session state
        let mut session_state = self.session_state.write();
        
        // Mark as complete
        session_state.is_complete = true;
        
        // Save to SQLite
        self.save_session_state()?;
        
        Ok(())
    }
    
    /// Update the session state with the current queue status
    pub fn update_session_state(&self, queue: &QueryQueue) -> SessionManagerResult<()> {
        // Get the current tasks
        let tasks = queue.get_tasks();
        
        // Count completed and pending tasks
        let mut completed_tasks = 0;
        let mut pending_tasks = 0;
        
        for task in &tasks {
            match task.status {
                QueryStatus::Completed => completed_tasks += 1,
                QueryStatus::Pending | QueryStatus::Running => pending_tasks += 1,
                _ => {}
            }
        }
        
        // Update the session state
        let mut session_state = self.session_state.write();
        session_state.completed_tasks = completed_tasks;
        session_state.pending_tasks = pending_tasks;
        
        // Save to SQLite
        self.save_session_state()?;
        
        Ok(())
    }
    
    /// Validate disk space before scheduling downloads
    pub fn validate_disk_space(&self, required_bytes: u64) -> SessionManagerResult<bool> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| SessionManagerError::Config(e.to_string()))?;
        
        // Get the download directory
        let download_dir = Path::new(&app_config.paths.download_directory);
        
        // Check if the directory exists
        if !download_dir.exists() {
            fs::create_dir_all(download_dir)?;
        }
        
        // Get the available space
        let available_space = get_available_space(download_dir)?;
        
        // Check if there's enough space
        if available_space < required_bytes {
            warn!(
                "Not enough disk space: {} bytes available, {} bytes required",
                available_space, required_bytes
            );
            return Ok(false);
        }
        
        Ok(true)
    }
    
    /// Perform graceful shutdown
    pub async fn shutdown(&self, orchestrator: &Orchestrator) -> SessionManagerResult<()> {
        info!("Performing graceful shutdown...");
        
        // Update the session state with the current queue status
        let queue = orchestrator.get_job_queue();
        self.update_session_state(&queue)?;
        
        // Flush all queues and logs
        info!("Flushing queues and logs...");
        
        // Stop the orchestrator (this will stop the scheduler and conductor)
        info!("Stopping orchestrator...");
        orchestrator.stop().await
            .map_err(|e| SessionManagerError::Session(format!("Failed to stop orchestrator: {}", e)))?;
        
        // Mark the session as complete
        info!("Marking session as complete...");
        self.mark_session_complete()?;
        
        // Check metadata store consistency
        info!("Checking metadata store consistency...");
        if let Err(e) = self.metadata_store.check_consistency() {
            warn!("Metadata store consistency check failed: {}", e);
            
            // Try to repair
            if let Err(e) = self.metadata_store.repair_consistency() {
                error!("Failed to repair metadata store consistency: {}", e);
            } else {
                info!("Metadata store consistency repaired");
            }
        } else {
            info!("Metadata store consistency check passed");
        }
        
        info!("Graceful shutdown completed");
        Ok(())
    }
    
    /// Resume from last session if interrupted
    pub async fn resume(&self, orchestrator: &Orchestrator) -> SessionManagerResult<bool> {
        info!("Checking for interrupted session...");
        
        // Load the last incomplete session
        let last_session = self.load_last_session_state()?;
        
        if let Some(session) = last_session {
            info!("Found interrupted session: {}", session.session_id);
            
            // Check if there's a current query to resume
            if let Some(query_str) = &session.current_query {
                info!("Resuming query: {}", query_str);
                
                // Parse the query string
                let parts: Vec<&str> = query_str.split(',').collect();
                if parts.len() >= 2 {
                    let tags: Vec<String> = parts[0].split(' ').map(|s| s.to_string()).collect();
                    let priority: usize = parts[1].parse().unwrap_or(1);
                    
                    // Create a new query
                    let query = Query {
                        id: uuid::Uuid::new_v4(),
                        tags,
                        priority,
                    };
                    
                    // Add the query to the queue
                    orchestrator.add_query(query.tags.clone(), query.priority).await
                        .map_err(|e| SessionManagerError::Session(format!("Failed to add query: {}", e)))?;
                    
                    info!("Query resumed successfully");
                    return Ok(true);
                }
            }
            
            info!("No query to resume");
        } else {
            info!("No interrupted session found");
        }
        
        Ok(false)
    }
}

/// Get the available space on a disk
fn get_available_space(path: &Path) -> SessionManagerResult<u64> {
    #[cfg(target_os = "windows")]
    {
        use winapi::um::fileapi::{GetDiskFreeSpaceExW};
        use winapi::shared::minwindef::{DWORD, BOOL};
        use winapi::um::winnt::ULARGE_INTEGER;
        use std::os::windows::ffi::OsStrExt;
        use std::ffi::OsStr;
        use std::ptr;
        
        // Convert path to wide string for Windows API
        let path_str = path.to_str().ok_or_else(|| 
            SessionManagerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, 
                "Path contains invalid Unicode"
            ))
        )?;
        
        let wide_path: Vec<u16> = OsStr::new(path_str)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        
        let mut free_bytes_available: ULARGE_INTEGER = unsafe { std::mem::zeroed() };
        let mut total_number_of_bytes: ULARGE_INTEGER = unsafe { std::mem::zeroed() };
        let mut total_number_of_free_bytes: ULARGE_INTEGER = unsafe { std::mem::zeroed() };
        
        let result = unsafe {
            GetDiskFreeSpaceExW(
                wide_path.as_ptr(),
                &mut free_bytes_available,
                &mut total_number_of_bytes,
                &mut total_number_of_free_bytes
            )
        };
        
        if result == 0 {
            return Err(SessionManagerError::Io(std::io::Error::last_os_error()));
        }
        
        // Access the u64 value from ULARGE_INTEGER union
        let available_space = unsafe { 
            // ULARGE_INTEGER is a union with u and QuadPart fields
            // Access it as a u64 value through the anonymous union
            std::mem::transmute::<ULARGE_INTEGER, u64>(free_bytes_available)
        };
        Ok(available_space)
    }
    
    #[cfg(not(target_os = "windows"))]
    {
        use std::os::unix::fs::MetadataExt;
        
        let metadata = fs::metadata(path)?;
        let fs_stats = nix::sys::statvfs::statvfs(path)
            .map_err(|e| SessionManagerError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to get statvfs: {}", e)
            )))?;
        
        let available_space = fs_stats.block_size() as u64 * fs_stats.blocks_available() as u64;
        
        Ok(available_space)
    }
}

/// Create a new session manager
pub async fn init_session_manager(
    config_manager: Arc<ConfigManager>,
    metadata_store: Arc<MetadataStore>,
) -> SessionManagerResult<Arc<SessionManager>> {
    let manager = SessionManager::from_config(config_manager, metadata_store)?;
    Ok(Arc::new(manager))
}