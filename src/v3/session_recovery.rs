//! Session Recovery Module for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Persistent job state tracking
//! 2. Recovery after unexpected shutdowns
//! 3. Resume incomplete downloads
//! 4. Session state management

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc, serde::ts_seconds_option};
use rusqlite::{Connection, Result as SqliteResult, params};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, error};
use uuid::Uuid;

use crate::v3::{DownloadJob, ConfigManager};

/// Error types for session recovery
#[derive(Error, Debug)]
pub enum SessionRecoveryError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Session error: {0}")]
    Session(String),
}

/// Result type for session recovery operations
pub type SessionRecoveryResult<T> = Result<T, SessionRecoveryError>;

/// Status of a download job
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Pending,
    InProgress,
    Completed,
    Failed(String),
    Cancelled,
}

/// Session state for tracking overall progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    #[serde(with = "ts_seconds_option")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(with = "ts_seconds_option")]
    pub completed_at: Option<DateTime<Utc>>,
    pub total_jobs: usize,
    pub completed_jobs: usize,
    pub failed_jobs: usize,
    pub is_active: bool,
}

/// Persistent job record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentJob {
    pub id: Uuid,
    pub session_id: Uuid,
    pub post_id: u32,
    pub url: String,
    pub md5: String,
    pub file_ext: String,
    pub file_size: u32,
    pub tags: Vec<String>,
    pub priority: usize,
    pub is_blacklisted: bool,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    #[serde(with = "ts_seconds_option")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(with = "ts_seconds_option")]
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub last_error: Option<String>,
}

/// Session recovery manager
pub struct SessionRecovery {
    db_connection: Arc<RwLock<Connection>>,
    config_manager: Arc<ConfigManager>,
    current_session: Arc<RwLock<Option<SessionState>>>,
}

impl SessionRecovery {
    /// Create a new session recovery manager
    pub async fn new(db_path: &Path, config_manager: Arc<ConfigManager>) -> SessionRecoveryResult<Self> {
        let db_connection = Connection::open(db_path)?;
        
        // Create tables if they don't exist
        Self::create_tables(&db_connection)?;
        
        Ok(Self {
            db_connection: Arc::new(RwLock::new(db_connection)),
            config_manager,
            current_session: Arc::new(RwLock::new(None)),
        })
    }
    
    /// Create database tables for session recovery
    fn create_tables(conn: &Connection) -> SqliteResult<()> {
        // Create sessions table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                total_jobs INTEGER NOT NULL DEFAULT 0,
                completed_jobs INTEGER NOT NULL DEFAULT 0,
                failed_jobs INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT 0
            )",
            [],
        )?;
        
        // Create jobs table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS session_jobs (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                post_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                md5 TEXT NOT NULL,
                file_ext TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                tags TEXT NOT NULL,
                priority INTEGER NOT NULL,
                is_blacklisted BOOLEAN NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            )",
            [],
        )?;
        
        // Create indices for performance
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_jobs_session_id 
             ON session_jobs(session_id)",
            [],
        )?;
        
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_jobs_status 
             ON session_jobs(status)",
            [],
        )?;
        
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_is_active 
             ON sessions(is_active)",
            [],
        )?;
        
        debug!("Session recovery tables created successfully");
        Ok(())
    }
    
    /// Start a new download session
    pub async fn start_session(&self, jobs: &VecDeque<DownloadJob>) -> SessionRecoveryResult<Uuid> {
        let session_id = Uuid::new_v4();
        let now = Utc::now();
        
        let session = SessionState {
            id: session_id,
            created_at: now,
            started_at: Some(now),
            completed_at: None,
            total_jobs: jobs.len(),
            completed_jobs: 0,
            failed_jobs: 0,
            is_active: true,
        };
        
        // Store session in database
        {
            let conn = self.db_connection.write().await;
            conn.execute(
                "INSERT INTO sessions (id, created_at, started_at, total_jobs, is_active)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    session_id.to_string(),
                    now.timestamp(),
                    now.timestamp(),
                    jobs.len(),
                    true
                ],
            )?;
        }
        
        // Store all jobs
        for job in jobs {
            self.save_job(session_id, job, JobStatus::Pending).await?;
        }
        
        // Update current session
        *self.current_session.write().await = Some(session);
        
        info!("Started new download session: {} with {} jobs", session_id, jobs.len());
        Ok(session_id)
    }
    
    /// Save or update a job in the database
    pub async fn save_job(&self, session_id: Uuid, job: &DownloadJob, status: JobStatus) -> SessionRecoveryResult<()> {
        let now = Utc::now();
        let tags_json = serde_json::to_string(&job.tags)?;
        let status_json = serde_json::to_string(&status)?;
        
        let conn = self.db_connection.write().await;
        
        // Try to update existing job first
        let updated = conn.execute(
            "UPDATE session_jobs SET 
             status = ?1, 
             retry_count = retry_count + CASE WHEN status = 'Failed' THEN 1 ELSE 0 END,
             started_at = CASE WHEN ?1 = 'InProgress' AND started_at IS NULL THEN ?2 ELSE started_at END,
             completed_at = CASE WHEN ?1 IN ('Completed', 'Failed', 'Cancelled') THEN ?2 ELSE completed_at END
             WHERE id = ?3",
            params![status_json, now.timestamp(), job.id.to_string()],
        )?;
        
        if updated == 0 {
            // Insert new job
            conn.execute(
                "INSERT INTO session_jobs (
                    id, session_id, post_id, url, md5, file_ext, file_size, 
                    tags, priority, is_blacklisted, status, created_at, retry_count
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0)",
                params![
                    job.id.to_string(),
                    session_id.to_string(),
                    job.post_id,
                    job.url,
                    job.md5,
                    job.file_ext,
                    job.file_size,
                    tags_json,
                    job.priority,
                    job.is_blacklisted,
                    status_json,
                    now.timestamp(),
                ],
            )?;
        }
        
        debug!("Saved job {} with status: {:?}", job.id, status);
        Ok(())
    }
    
    /// Update job status
    pub async fn update_job_status(&self, job_id: Uuid, status: JobStatus, error: Option<String>) -> SessionRecoveryResult<()> {
        let now = Utc::now();
        let status_json = serde_json::to_string(&status)?;
        
        let conn = self.db_connection.write().await;
        conn.execute(
            "UPDATE session_jobs SET 
             status = ?1,
             completed_at = CASE WHEN ?1 IN ('Completed', 'Failed', 'Cancelled') THEN ?2 ELSE completed_at END,
             started_at = CASE WHEN ?1 = 'InProgress' AND started_at IS NULL THEN ?2 ELSE started_at END,
             last_error = ?3,
             retry_count = retry_count + CASE WHEN ?1 = 'Failed' THEN 1 ELSE 0 END
             WHERE id = ?4",
            params![status_json, now.timestamp(), error, job_id.to_string()],
        )?;
        
        debug!("Updated job {} status to: {:?}", job_id, status);
        Ok(())
    }
    
    /// Get all incomplete sessions (for recovery)
    pub async fn get_incomplete_sessions(&self) -> SessionRecoveryResult<Vec<SessionState>> {
        let conn = self.db_connection.read().await;
        let mut stmt = conn.prepare(
            "SELECT id, created_at, started_at, completed_at, total_jobs, 
                    completed_jobs, failed_jobs, is_active
             FROM sessions 
             WHERE is_active = 1 OR completed_at IS NULL
             ORDER BY created_at DESC"
        )?;
        
        let rows = stmt.query_map([], |row| {
            Ok(SessionState {
                id: Uuid::parse_str(&row.get::<_, String>(0)?).unwrap(),
                created_at: DateTime::from_timestamp(row.get(1)?, 0).unwrap(),
                started_at: row.get::<_, Option<i64>>(2)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
                completed_at: row.get::<_, Option<i64>>(3)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
                total_jobs: row.get(4)?,
                completed_jobs: row.get(5)?,
                failed_jobs: row.get(6)?,
                is_active: row.get(7)?,
            })
        })?;
        
        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        
        Ok(sessions)
    }
    
    /// Get incomplete jobs for a session
    pub async fn get_incomplete_jobs(&self, session_id: Uuid) -> SessionRecoveryResult<Vec<DownloadJob>> {
        let conn = self.db_connection.read().await;
        let mut stmt = conn.prepare(
            "SELECT id, post_id, url, md5, file_ext, file_size, tags, priority, is_blacklisted, retry_count
             FROM session_jobs 
             WHERE session_id = ?1 AND status NOT IN ('Completed', 'Cancelled')
             ORDER BY priority ASC, created_at ASC"
        )?;
        
        let rows = stmt.query_map([session_id.to_string()], |row| {
            let tags_json: String = row.get(6)?;
            let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
            
            Ok(DownloadJob {
                id: Uuid::parse_str(&row.get::<_, String>(0)?).unwrap(),
                post_id: row.get(1)?,
                url: row.get(2)?,
                md5: row.get(3)?,
                file_ext: row.get(4)?,
                file_size: row.get(5)?,
                tags: tags.clone(),
                artists: Vec::new(), // Artists not stored in session recovery yet
                source_query: tags, // Best-effort: use stored tags as source query
                priority: row.get(7)?,
                is_blacklisted: row.get(8)?,
            })
        })?;
        
        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(row?);
        }
        
        Ok(jobs)
    }
    
    /// Complete a session
    pub async fn complete_session(&self, session_id: Uuid) -> SessionRecoveryResult<()> {
        let now = Utc::now();
        
        // Update session completion status
        let conn = self.db_connection.write().await;
        conn.execute(
            "UPDATE sessions SET 
             completed_at = ?1, 
             is_active = 0,
             completed_jobs = (SELECT COUNT(*) FROM session_jobs WHERE session_id = ?2 AND status = 'Completed'),
             failed_jobs = (SELECT COUNT(*) FROM session_jobs WHERE session_id = ?2 AND status LIKE 'Failed%')
             WHERE id = ?2",
            params![now.timestamp(), session_id.to_string()],
        )?;
        
        // Update current session
        if let Some(ref mut session) = *self.current_session.write().await {
            if session.id == session_id {
                session.completed_at = Some(now);
                session.is_active = false;
            }
        }
        
        info!("Completed session: {}", session_id);
        Ok(())
    }
    
    /// Get session statistics
    pub async fn get_session_stats(&self, session_id: Uuid) -> SessionRecoveryResult<(usize, usize, usize, usize)> {
        let conn = self.db_connection.read().await;
        let mut stmt = conn.prepare(
            "SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status LIKE 'Failed%' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending
             FROM session_jobs 
             WHERE session_id = ?1"
        )?;
        
        let row = stmt.query_row([session_id.to_string()], |row| {
            Ok((
                row.get::<_, usize>(0)?,
                row.get::<_, usize>(1)?,
                row.get::<_, usize>(2)?,
                row.get::<_, usize>(3)?,
            ))
        })?;
        
        Ok(row)
    }
    
    /// Clean up old completed sessions (older than specified days)
    pub async fn cleanup_old_sessions(&self, days_to_keep: u32) -> SessionRecoveryResult<usize> {
        let cutoff_time = Utc::now() - chrono::Duration::days(days_to_keep as i64);
        
        let conn = self.db_connection.write().await;
        
        // First delete associated jobs
        let jobs_deleted = conn.execute(
            "DELETE FROM session_jobs 
             WHERE session_id IN (
                SELECT id FROM sessions 
                WHERE completed_at IS NOT NULL AND completed_at < ?1
             )",
            params![cutoff_time.timestamp()],
        )?;
        
        // Then delete sessions
        let sessions_deleted = conn.execute(
            "DELETE FROM sessions 
             WHERE completed_at IS NOT NULL AND completed_at < ?1",
            params![cutoff_time.timestamp()],
        )?;
        
        info!("Cleaned up {} old sessions and {} associated jobs", sessions_deleted, jobs_deleted);
        Ok(sessions_deleted)
    }
    
    /// Get current session
    pub async fn get_current_session(&self) -> Option<SessionState> {
        self.current_session.read().await.clone()
    }
}

/// Initialize session recovery
pub async fn init_session_recovery(
    db_path: &Path,
    config_manager: Arc<ConfigManager>,
) -> SessionRecoveryResult<Arc<SessionRecovery>> {
    let recovery = SessionRecovery::new(db_path, config_manager).await?;
    Ok(Arc::new(recovery))
}
