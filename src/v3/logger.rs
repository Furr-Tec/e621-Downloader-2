//! Structured Logging for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Logging every download, hash, API fetch
//! 2. Including task ID, post ID, source, status, and timestamp
//! 3. Writing logs to a rotating file in JSON or line format

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn, Level};
use tracing_appender;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    prelude::*,
    EnvFilter,
};
use uuid::Uuid;

use crate::v3::ConfigManager;

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

/// Operation status for log entries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Log entry type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEntryType {
    Download,
    Hash,
    ApiFetch,
    SystemEvent,
    Error,
}

impl std::fmt::Display for LogEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntryType::Download => write!(f, "download"),
            LogEntryType::Hash => write!(f, "hash"),
            LogEntryType::ApiFetch => write!(f, "api_fetch"),
            LogEntryType::SystemEvent => write!(f, "system_event"),
            LogEntryType::Error => write!(f, "error"),
        }
    }
}

/// Log entry for structured logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp of the log entry
    pub timestamp: String,
    /// Type of log entry
    pub entry_type: String,
    /// Task ID (UUID)
    pub task_id: Option<String>,
    /// Post ID
    pub post_id: Option<u32>,
    /// Source URL or path
    pub source: Option<String>,
    /// Operation status
    pub status: Option<String>,
    /// Additional data (JSON object)
    pub data: Option<serde_json::Value>,
}

/// Logger for structured logging
pub struct Logger {
    config_manager: Arc<ConfigManager>,
    log_dir: PathBuf,
    log_format: String,
}

impl Logger {
    /// Create a new logger
    pub fn new(config_manager: Arc<ConfigManager>, log_dir: PathBuf, log_format: String) -> Self {
        Self {
            config_manager,
            log_dir,
            log_format,
        }
    }

    /// Initialize the logger
    pub fn init(&self) -> LoggerResult<()> {
        // Create the log directory if it doesn't exist
        if !self.log_dir.exists() {
            fs::create_dir_all(&self.log_dir)?;
        }

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| LoggerError::Config(e.to_string()))?;

        // Create a non-rolling file appender that always writes to e621_downloader.log
        let (file_writer, _guard) = tracing_appender::non_blocking(tracing_appender::rolling::never(
            &self.log_dir, 
            "e621_downloader.log",
        ));

        // Create a formatting layer for the file
        let file_layer = fmt::layer()
            .with_writer(file_writer)
            .with_ansi(false)
            .with_span_events(FmtSpan::CLOSE);

        // Create a filter based on the log level
        let log_level = match app_config.logging.log_level.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        };

        let filter_layer = EnvFilter::from_default_env()
            .add_directive(format!("e621_downloader={}", log_level).parse().unwrap());

        // Initialize the tracing subscriber
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(file_layer)
            .init();

        info!("Logger initialized");
        Ok(())
    }

    /// Get the current timestamp as an ISO 8601 string
    fn get_timestamp(&self) -> String {
        let now = SystemTime::now();
        let datetime: DateTime<Utc> = now.into();
        datetime.to_rfc3339()
    }

    /// Log a download operation
    pub fn log_download(
        &self,
        task_id: Uuid,
        post_id: u32,
        source: &str,
        status: OperationStatus,
        bytes: Option<u64>,
    ) {
        let data = if let Some(bytes) = bytes {
            Some(serde_json::json!({ "bytes": bytes }))
        } else {
            None
        };

        let _entry = LogEntry {
            timestamp: self.get_timestamp(),
            entry_type: LogEntryType::Download.to_string(),
            task_id: Some(task_id.to_string()),
            post_id: Some(post_id),
            source: Some(source.to_string()),
            status: Some(status.to_string()),
            data,
        };

        // Log the entry
        match status {
            OperationStatus::Started => info!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Download started"
            ),
            OperationStatus::InProgress => debug!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                bytes = bytes,
                "Download in progress"
            ),
            OperationStatus::Completed => info!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                bytes = bytes,
                "Download completed"
            ),
            OperationStatus::Failed => error!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Download failed"
            ),
            OperationStatus::Cancelled => warn!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Download cancelled"
            ),
        }
    }

    /// Log a hash operation
    pub fn log_hash(
        &self,
        task_id: Uuid,
        post_id: u32,
        source: &str,
        status: OperationStatus,
        blake3_hash: Option<&str>,
        sha256_hash: Option<&str>,
    ) {
        let data = if blake3_hash.is_some() || sha256_hash.is_some() {
            let mut data_map = serde_json::Map::new();
            if let Some(hash) = blake3_hash {
                data_map.insert("blake3".to_string(), serde_json::Value::String(hash.to_string()));
            }
            if let Some(hash) = sha256_hash {
                data_map.insert("sha256".to_string(), serde_json::Value::String(hash.to_string()));
            }
            Some(serde_json::Value::Object(data_map))
        } else {
            None
        };

        let _entry = LogEntry {
            timestamp: self.get_timestamp(),
            entry_type: LogEntryType::Hash.to_string(),
            task_id: Some(task_id.to_string()),
            post_id: Some(post_id),
            source: Some(source.to_string()),
            status: Some(status.to_string()),
            data,
        };

        // Log the entry
        match status {
            OperationStatus::Started => info!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Hash calculation started"
            ),
            OperationStatus::InProgress => debug!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Hash calculation in progress"
            ),
            OperationStatus::Completed => info!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                blake3 = blake3_hash,
                sha256 = sha256_hash,
                "Hash calculation completed"
            ),
            OperationStatus::Failed => error!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Hash calculation failed"
            ),
            OperationStatus::Cancelled => warn!(
                task_id = %task_id,
                post_id = %post_id,
                source = %source,
                status = %status,
                "Hash calculation cancelled"
            ),
        }
    }

    /// Log an API fetch operation
    pub fn log_api_fetch(
        &self,
        task_id: Uuid,
        url: &str,
        query: &str,
        status: OperationStatus,
        http_status: Option<u16>,
    ) {
        let data = if let Some(status) = http_status {
            Some(serde_json::json!({ "http_status": status }))
        } else {
            None
        };

        let _entry = LogEntry {
            timestamp: self.get_timestamp(),
            entry_type: LogEntryType::ApiFetch.to_string(),
            task_id: Some(task_id.to_string()),
            post_id: None,
            source: Some(format!("{}?{}", url, query)),
            status: Some(status.to_string()),
            data,
        };

        // Log the entry
        match status {
            OperationStatus::Started => info!(
                task_id = %task_id,
                url = %url,
                query = %query,
                status = %status,
                "API fetch started"
            ),
            OperationStatus::InProgress => debug!(
                task_id = %task_id,
                url = %url,
                query = %query,
                status = %status,
                "API fetch in progress"
            ),
            OperationStatus::Completed => info!(
                task_id = %task_id,
                url = %url,
                query = %query,
                status = %status,
                http_status = http_status,
                "API fetch completed"
            ),
            OperationStatus::Failed => error!(
                task_id = %task_id,
                url = %url,
                query = %query,
                status = %status,
                http_status = http_status,
                "API fetch failed"
            ),
            OperationStatus::Cancelled => warn!(
                task_id = %task_id,
                url = %url,
                query = %query,
                status = %status,
                "API fetch cancelled"
            ),
        }
    }

    /// Log a system event
    pub fn log_system_event(&self, event_type: &str, message: &str) {
        let _entry = LogEntry {
            timestamp: self.get_timestamp(),
            entry_type: LogEntryType::SystemEvent.to_string(),
            task_id: None,
            post_id: None,
            source: None,
            status: None,
            data: Some(serde_json::json!({
                "event_type": event_type,
                "message": message,
            })),
        };

        // Log the entry
        info!(
            event_type = %event_type,
            message = %message,
            "System event"
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
        let data = if let Some(details) = details {
            Some(serde_json::json!({
                "error_type": error_type,
                "details": details,
            }))
        } else {
            Some(serde_json::json!({
                "error_type": error_type,
            }))
        };

        let _entry = LogEntry {
            timestamp: self.get_timestamp(),
            entry_type: LogEntryType::Error.to_string(),
            task_id: task_id.map(|id| id.to_string()),
            post_id: None,
            source: None,
            status: None,
            data,
        };

        // Log the entry
        if let Some(task_id) = task_id {
            error!(
                task_id = %task_id,
                error_type = %error_type,
                message = %message,
                details = details,
                "Error occurred"
            );
        } else {
            error!(
                error_type = %error_type,
                message = %message,
                details = details,
                "Error occurred"
            );
        }
    }
}

/// Initialize the logger
pub async fn init_logger(config_manager: Arc<ConfigManager>) -> LoggerResult<Arc<Logger>> {
    // Get the app config
    let app_config = config_manager.get_app_config()
        .map_err(|e| LoggerError::Config(e.to_string()))?;

    // Create the log directory
    let log_dir = PathBuf::from(&app_config.paths.log_directory);

    // Create the logger
    let logger = Logger::new(
        config_manager.clone(),
        log_dir,
        app_config.logging.log_format.clone(),
    );

    // Initialize the logger
    logger.init()?;

    Ok(Arc::new(logger))
}
