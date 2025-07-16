//! Orchestration layer for E621 Downloader
//! 
//! This module provides a global orchestration layer that:
//! 1. Acquires a global lock on boot to prevent multiple instances
//! 2. Creates and manages a QueryQueue, Scheduler, and Conductor
//! 3. Tracks each task with a unique trace ID using tracing
//! 4. Ensures orderly startup/shutdown with all threads joining cleanly

use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info, instrument, warn, Instrument, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Uuid;

use crate::v3::{AppConfig, ConfigManager, ConfigResult};

/// Error types for the orchestration layer
#[derive(Error, Debug)]
pub enum OrchestratorError {
    #[error("IO error: {0}")]
    Io(#[from] IoError),

    #[error("Another instance is already running")]
    AlreadyRunning,

    #[error("Failed to initialize tracing: {0}")]
    TracingInitError(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Task error: {0}")]
    Task(String),

    #[error("Shutdown error: {0}")]
    Shutdown(String),
}

/// Result type for orchestration operations
pub type OrchestratorResult<T> = Result<T, OrchestratorError>;

/// Query type for the download queue
#[derive(Debug, Clone)]
pub struct Query {
    pub id: Uuid,
    pub tags: Vec<String>,
    pub priority: usize,
}

/// Status of a query in the queue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// A task in the query queue
#[derive(Debug)]
pub struct QueryTask {
    pub query: Query,
    pub status: QueryStatus,
    pub trace_id: Uuid,
}

/// Queue for managing download queries
pub struct QueryQueue {
    tasks: Arc<RwLock<Vec<QueryTask>>>,
    tx: mpsc::Sender<Query>,
    rx: Arc<Mutex<mpsc::Receiver<Query>>>,
}

impl QueryQueue {
    /// Create a new query queue
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tasks: Arc::new(RwLock::new(Vec::new())),
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Add a query to the queue
    pub async fn enqueue(&self, query: Query) -> OrchestratorResult<()> {
        // Generate a trace ID for the query
        let trace_id = Uuid::new_v4();

        // Add the task to the tasks list
        {
            let mut tasks = self.tasks.write();
            tasks.push(QueryTask {
                query: query.clone(),
                status: QueryStatus::Pending,
                trace_id,
            });
        }

        // Send the query to the channel
        self.tx.send(query).await.map_err(|e| {
            OrchestratorError::Task(format!("Failed to enqueue query: {}", e))
        })?;

        Ok(())
    }

    /// Get the next query from the queue
    pub async fn dequeue(&self) -> Option<Query> {
        let mut rx = self.rx.lock();
        rx.recv().await
    }

    /// Get all tasks in the queue
    pub fn get_tasks(&self) -> Vec<QueryTask> {
        self.tasks.read().clone()
    }

    /// Update the status of a task
    pub fn update_status(&self, id: Uuid, status: QueryStatus) {
        let mut tasks = self.tasks.write();
        for task in tasks.iter_mut() {
            if task.query.id == id {
                task.status = status;
                break;
            }
        }
    }
}

/// Scheduler for managing download tasks
pub struct Scheduler {
    queue: Arc<QueryQueue>,
    config: Arc<RwLock<AppConfig>>,
    shutdown_tx: broadcast::Sender<()>,
    join_set: Arc<Mutex<JoinSet<OrchestratorResult<()>>>>,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new(queue: Arc<QueryQueue>, config: Arc<RwLock<AppConfig>>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            queue,
            config,
            shutdown_tx,
            join_set: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    /// Start the scheduler
    pub async fn start(&self) -> OrchestratorResult<()> {
        let queue = self.queue.clone();
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let join_set = self.join_set.clone();

        // Spawn the scheduler task
        let handle = tokio::spawn(async move {
            info!("Scheduler started");

            loop {
                tokio::select! {
                    // Check for shutdown signal
                    _ = shutdown_rx.recv() => {
                        info!("Scheduler received shutdown signal");
                        break;
                    }

                    // Process the next query
                    Some(query) = queue.dequeue() => {
                        // Create a span for the query
                        let span = tracing::info_span!("process_query", id = %query.id);

                        // Update the task status
                        queue.update_status(query.id, QueryStatus::Running);

                        // Process the query in a separate task
                        let queue_clone = queue.clone();
                        let config_clone = config.clone();
                        let task = tokio::spawn(async move {
                            // Process the query
                            info!("Processing query: {:?}", query);

                            // Simulate processing
                            tokio::time::sleep(Duration::from_secs(2)).await;

                            // Update the task status
                            queue_clone.update_status(query.id, QueryStatus::Completed);

                            info!("Query processed: {:?}", query);
                            Ok(())
                        }.instrument(span));

                        // Add the task to the join set
                        join_set.lock().spawn(task);
                    }

                    // No queries, wait a bit
                    else => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }

            Ok(())
        });

        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> OrchestratorResult<()> {
        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for all tasks to complete
        let mut join_set = self.join_set.lock();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Task error: {}", e);
                }
                Err(e) => {
                    error!("Join error: {}", e);
                }
            }
        }

        info!("Scheduler stopped");
        Ok(())
    }
}

/// Conductor for managing the overall download process
pub struct Conductor {
    queue: Arc<QueryQueue>,
    scheduler: Arc<Scheduler>,
    config_manager: Arc<ConfigManager>,
    lock_file: Option<File>,
    lock_path: PathBuf,
}

impl Conductor {
    /// Create a new conductor
    pub async fn new(config_manager: Arc<ConfigManager>, lock_path: impl AsRef<Path>) -> OrchestratorResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| OrchestratorError::Config(e.to_string()))?;

        // Create the query queue
        let queue = Arc::new(QueryQueue::new(100));

        // Create the scheduler
        let config = Arc::new(RwLock::new(app_config));
        let scheduler = Arc::new(Scheduler::new(queue.clone(), config));

        Ok(Self {
            queue,
            scheduler,
            config_manager,
            lock_file: None,
            lock_path: lock_path.as_ref().to_path_buf(),
        })
    }

    /// Acquire the global lock
    pub fn acquire_lock(&mut self) -> OrchestratorResult<()> {
        // Try to open the lock file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.lock_path)?;

        // Try to acquire an exclusive lock
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            // Try to acquire an exclusive lock
            if let Err(_) = nix::fcntl::flock(file.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock) {
                return Err(OrchestratorError::AlreadyRunning);
            }
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;

            // Try to lock the entire file
            if let Err(_) = file.lock_exclusive() {
                return Err(OrchestratorError::AlreadyRunning);
            }
        }

        // Write the process ID to the lock file
        let pid = std::process::id().to_string();
        file.set_len(0)?;
        file.write_all(pid.as_bytes())?;
        file.flush()?;

        // Store the lock file
        self.lock_file = Some(file);

        info!("Acquired global lock: {}", self.lock_path.display());
        Ok(())
    }

    /// Initialize tracing
    pub fn init_tracing(&self) -> OrchestratorResult<()> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| OrchestratorError::Config(e.to_string()))?;

        // Create the log directory if it doesn't exist
        let log_dir = Path::new(&app_config.paths.log_directory);
        if !log_dir.exists() {
            std::fs::create_dir_all(log_dir)?;
        }

        // Create a file appender
        let file_appender = tracing_appender::rolling::daily(log_dir, "e621_downloader.log");

        // Create a formatting layer for the file
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false)
            .with_span_events(FmtSpan::CLOSE);

        // Create a formatting layer for the console
        let console_layer = tracing_subscriber::fmt::layer()
            .with_span_events(FmtSpan::CLOSE);

        // Create a filter based on the log level
        let filter_layer = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(&app_config.logging.log_level))
            .map_err(|e| OrchestratorError::TracingInitError(e.to_string()))?;

        // Initialize the tracing subscriber
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(console_layer)
            .with(file_layer)
            .try_init()
            .map_err(|e| OrchestratorError::TracingInitError(e.to_string()))?;

        info!("Tracing initialized");
        Ok(())
    }

    /// Start the conductor
    pub async fn start(&self) -> OrchestratorResult<()> {
        // Start the scheduler
        self.scheduler.start().await?;

        info!("Conductor started");
        Ok(())
    }

    /// Stop the conductor
    pub async fn stop(&self) -> OrchestratorResult<()> {
        // Stop the scheduler
        self.scheduler.stop().await?;

        info!("Conductor stopped");
        Ok(())
    }

    /// Add a query to the queue
    pub async fn add_query(&self, tags: Vec<String>, priority: usize) -> OrchestratorResult<Uuid> {
        let id = Uuid::new_v4();
        let query = Query {
            id,
            tags,
            priority,
        };

        self.queue.enqueue(query).await?;

        Ok(id)
    }

    /// Get all tasks in the queue
    pub fn get_tasks(&self) -> Vec<QueryTask> {
        self.queue.get_tasks()
    }
}

impl Drop for Conductor {
    fn drop(&mut self) {
        // Release the lock file
        self.lock_file = None;

        // Remove the lock file
        let _ = std::fs::remove_file(&self.lock_path);

        info!("Released global lock: {}", self.lock_path.display());
    }
}

/// Orchestrator for managing the entire application
pub struct Orchestrator {
    conductor: Arc<Conductor>,
}

impl Orchestrator {
    /// Create a new orchestrator
    pub async fn new(config_manager: Arc<ConfigManager>) -> OrchestratorResult<Self> {
        // Create the conductor
        let mut conductor = Conductor::new(config_manager, "e621_downloader.lock").await?;

        // Acquire the global lock
        conductor.acquire_lock()?;

        // Initialize tracing
        conductor.init_tracing()?;

        Ok(Self {
            conductor: Arc::new(conductor),
        })
    }

    /// Start the orchestrator
    pub async fn start(&self) -> OrchestratorResult<()> {
        // Start the conductor
        self.conductor.start().await?;

        info!("Orchestrator started");
        Ok(())
    }

    /// Stop the orchestrator
    pub async fn stop(&self) -> OrchestratorResult<()> {
        // Stop the conductor
        self.conductor.stop().await?;

        info!("Orchestrator stopped");
        Ok(())
    }

    /// Add a query to the queue
    pub async fn add_query(&self, tags: Vec<String>, priority: usize) -> OrchestratorResult<Uuid> {
        self.conductor.add_query(tags, priority).await
    }

    /// Get all tasks in the queue
    pub fn get_tasks(&self) -> Vec<QueryTask> {
        self.conductor.get_tasks()
    }

    /// Get the job queue
    pub fn get_job_queue(&self) -> Arc<QueryQueue> {
        self.conductor.queue.clone()
    }
}

/// Initialize the orchestrator
pub async fn init_orchestrator(config_manager: Arc<ConfigManager>) -> OrchestratorResult<Arc<Orchestrator>> {
    let orchestrator = Orchestrator::new(config_manager).await?;
    let orchestrator = Arc::new(orchestrator);

    // Start the orchestrator
    orchestrator.start().await?;

    Ok(orchestrator)
}
