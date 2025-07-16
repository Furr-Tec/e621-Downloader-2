//! Structured Logging for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Logging every download, hash, API fetch
//! 2. Including task ID, post ID, source, status, and timestamp
//! 3. Writing logs to a rotating file in JSON or line format

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use thiserror::Error;
use tracing::{debug, error, info, warn, Level};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan, time::UtcTime},
    prelude::*,
    EnvFilter,
};
use uuid::Uuid;

use crate::v3::{AppConfig, ConfigManager, ConfigResult};

/// Error types for logging
#[derive(Error, Debug)]
pub enum LoggerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Logging error: {0}")]
    Logging(String),
}

/// Result type for logging operations
pub type LoggerResult<T> = Result<T, LoggerError>;

/// Log entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogEntryType {
    Download,
    Hash,
    ApiFetch,
    System,
    Error,
}

impl std::fmt::Display for LogEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntryType::Download => write!(f, "download"),
            LogEntryType::Hash => write!(f, "hash"),
            LogEntryType::ApiFetch => write!(f, "api_fetch"),
            LogEntryType::System => write!(f, "system"),
            LogEntryType::Error => write!(f, "error"),
        }
    }
}

/// Status of an operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    Started,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationStatus::Started => write!(f, "started"),
            OperationStatus::InProgress => write!(f, "in_progress"),
            OperationStatus::Completed => write!(f, "completed"),
            OperationStatus::Failed => write!(f, "failed"),
            OperationStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Logger for structured logging
pub struct Logger {
    config_manager: Arc<ConfigManager>,
    log_dir: PathBuf,
    _guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

impl Logger {
    /// Create a new logger
    pub fn new(config_manager: Arc<ConfigManager>) -> LoggerResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| LoggerError::Config(e.to_string()))?;
        
        // Create the log directory if it doesn't exist
        let log_dir = PathBuf::from(&app_config.paths.log_directory);
        if !log_dir.exists() {
            std::fs::create_dir_all(&log_dir)?;
        }
        
        // Initialize the logger
        let guard = Self::init_logger(&log_dir, &app_config.logging.log_format, &app_config.logging.log_level)?;
        
        Ok(Self {
            config_manager,
            log_dir,
            _guard: Some(guard),
        })
    }
    
    /// Initialize the logger with proper configuration
    fn init_logger(log_dir: &Path, format: &str, level: &str) -> LoggerResult<tracing_appender::non_blocking::WorkerGuard> {
        // Create a rolling file appender
        let file_appender = RollingFileAppender::new(
            Rotation::DAILY,
            log_dir,
            "e621_downloader.log",
        );
        
        // Create a non-blocking writer
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        
        // Create a filter based on the log level
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(level))
            .map_err(|e| LoggerError::Logging(e.to_string()))?;
        
        // Determine the format
        let layer = match format {
            "json" => {
                fmt::layer()
                    .json()
                    .with_writer(non_blocking)
                    .with_timer(UtcTime::rfc_3339())
                    .with_span_events(FmtSpan::CLOSE)
                    .boxed()
            }
            _ => {
                fmt::layer()
                    .with_writer(non_blocking)
                    .with_timer(UtcTime::rfc_3339())
                    .with_span_events(FmtSpan::CLOSE)
                    .boxed()
            }
        };
        
        // Initialize the tracing subscriber
        tracing_subscriber::registry()
            .with(filter)
            .with(layer)
            .try_init()
            .map_err(|e| LoggerError::Logging(e.to_string()))?;
        
        info!(
            log_format = format,
            log_level = level,
            message = "Logger initialized",
        );
        
        Ok(guard)
    }
    
    /// Log a download operation
    pub fn log_download(
        &self,
        task_id: Uuid,
        post_id: u32,
        source: &str,
        status: OperationStatus,
        file_size: Option<u64>,
    ) {
        info!(
            entry_type = %LogEntryType::Download,
            task_id = %task_id,
            post_id = post_id,
            source = source,
            status = %status,
            file_size = file_size,
            timestamp = %Utc::now().to_rfc3339(),
            message = format!("Download {} for post {}", status, post_id),
        );
    }
    
    /// Log a hash operation
    pub fn log_hash(
        &self,
        task_id: Uuid,
        post_id: u32,
        file_path: &str,
        status: OperationStatus,
        blake3_hash: Option<&str>,
        sha256_hash: Option<&str>,
    ) {
        info!(
            entry_type = %LogEntryType::Hash,
            task_id = %task_id,
            post_id = post_id,
            file_path = file_path,
            status = %status,
            blake3_hash = blake3_hash,
            sha256_hash = sha256_hash,
            timestamp = %Utc::now().to_rfc3339(),
            message = format!("Hash {} for post {}", status, post_id),
        );
    }
    
    /// Log an API fetch operation
    pub fn log_api_fetch(
        &self,
        task_id: Uuid,
        endpoint: &str,
        params: &str,
        status: OperationStatus,
        response_code: Option<u16>,
    ) {
        info!(
            entry_type = %LogEntryType::ApiFetch,
            task_id = %task_id,
            endpoint = endpoint,
            params = params,
            status = %status,
            response_code = response_code,
            timestamp = %Utc::now().to_rfc3339(),
            message = format!("API fetch {} for endpoint {}", status, endpoint),
        );
    }
    
    /// Log a system event
    pub fn log_system_event(
        &self,
        event_type: &str,
        details: &str,
    ) {
        info!(
            entry_type = %LogEntryType::System,
            event_type = event_type,
            details = details,
            timestamp = %Utc::now().to_rfc3339(),
            message = format!("System event: {}", event_type),
        );
    }
    
    /// Log an error
    pub fn log_error(
        &self,
        task_id: Option<Uuid>,
        error_type: &str,
        message: &str,
        details: Option<&str>,
    ) {
        error!(
            entry_type = %LogEntryType::Error,
            task_id = task_id.map(|id| id.to_string()),
            error_type = error_type,
            message = message,
            details = details,
            timestamp = %Utc::now().to_rfc3339(),
            message = format!("Error: {}", message),
        );
    }
}

/// Create a new logger
pub async fn init_logger(
    config_manager: Arc<ConfigManager>,
) -> LoggerResult<Arc<Logger>> {
    let logger = Logger::new(config_manager)?;
    Ok(Arc::new(logger))
}