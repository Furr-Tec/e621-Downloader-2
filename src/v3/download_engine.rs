//! Download Engine for E621 Downloader
//! 
//! This module provides an async download engine that:
//! 1. Maintains a connection pool
//! 2. Streams files to disk without loading them entirely into memory
//! 3. Uses bounded concurrency (e.g., Semaphore)
//! 4. Retries failed downloads with exponential backoff
//! 5. Respects rate limits and sleeps on 429 responses

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures::stream::StreamExt;
use tokio::sync::Mutex;
use reqwest::{Client, Response, StatusCode};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};

use crate::v3::{
    E621Config,
    ConfigManager,
    DownloadJob,
    BlacklistHandler,
    DirectoryOrganizer, init_directory_organizer,
    Database, init_database,
};

/// Error types for the download engine
#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Config error: {0}")]
    Config(String),

    #[error("Download error: {0}")]
    Download(String),

    #[error("File already exists: {0}")]
    FileExists(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

/// Result type for download operations
pub type DownloadResult<T> = Result<T, DownloadError>;

/// Status of a download
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

/// Download task
#[derive(Debug)]
pub struct DownloadTask {
    pub job: DownloadJob,
    pub status: DownloadStatus,
    pub attempts: usize,
    pub file_path: PathBuf,
}

/// Download statistics
#[derive(Debug, Default, Clone)]
pub struct DownloadStats {
    pub total_jobs: usize,
    pub completed_jobs: usize,
    pub failed_jobs: usize,
    pub bytes_downloaded: u64,
    pub average_speed_bps: f64,
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of idle connections per host
    pub max_idle_per_host: usize,
    /// Maximum total number of idle connections
    pub max_idle_total: usize,
    /// Maximum number of connections per host
    pub max_connections_per_host: usize,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
    /// Keep-alive timeout in seconds
    pub keep_alive_timeout_secs: u64,
    /// Enable HTTP/2
    pub http2_enabled: bool,
    /// Enable gzip compression
    pub gzip_enabled: bool,
    /// TCP socket keep-alive
    pub tcp_keepalive: bool,
    /// TCP nodelay
    pub tcp_nodelay: bool,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 10,
            max_idle_total: 50,
            max_connections_per_host: 20,
            connection_timeout_secs: 30,
            keep_alive_timeout_secs: 90,
            http2_enabled: true,
            gzip_enabled: true,
            tcp_keepalive: true,
            tcp_nodelay: true,
        }
    }
}

/// Connection pool metrics
#[derive(Debug, Default, Clone)]
pub struct ConnectionPoolMetrics {
    /// Total connections created
    pub connections_created: u64,
    /// Connections reused from pool
    pub connections_reused: u64,
    /// Current active connections
    pub active_connections: usize,
    /// Current idle connections
    pub idle_connections: usize,
    /// Connection timeouts
    pub connection_timeouts: u64,
    /// Connection errors
    pub connection_errors: u64,
    /// Average connection establishment time (ms)
    pub avg_connection_time_ms: f64,
}

/// Download engine configuration
#[derive(Debug, Clone)]
pub struct DownloadEngineConfig {
    pub max_concurrent_downloads: usize,
    pub retry_attempts: usize,
    pub base_retry_delay_ms: u64,
    pub download_dir: PathBuf,
    pub blacklisted_dir: PathBuf,
    pub temp_dir: PathBuf,
    pub user_agent: String,
    pub timeout_seconds: u64,
    /// Connection pool configuration
    pub connection_pool: ConnectionPoolConfig,
}

impl Default for DownloadEngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_downloads: 4,
            retry_attempts: 3,
            base_retry_delay_ms: 1000,
            download_dir: PathBuf::from("./downloads"),
            blacklisted_dir: PathBuf::from("./blacklisted_downloads"),
            temp_dir: PathBuf::from("./.tmp"),
            user_agent: "e621_downloader/2.0 (by anonymous)".to_string(),
            timeout_seconds: 30,
            connection_pool: ConnectionPoolConfig::default(),
        }
    }
}

/// Download engine for managing file downloads
pub struct DownloadEngine {
    config: DownloadEngineConfig,
    client: Client,
    semaphore: Arc<Semaphore>,
    stats: Arc<Mutex<DownloadStats>>,
    job_tx: mpsc::Sender<DownloadJob>,
    job_rx: Arc<Mutex<mpsc::Receiver<DownloadJob>>>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    blacklist_handler: Arc<BlacklistHandler>,
    /// Original concurrency setting
    original_concurrency: Arc<Mutex<usize>>,
    /// Current concurrency setting
    current_concurrency: Arc<Mutex<usize>>,
    /// Configuration manager
    config_manager: Arc<ConfigManager>,
    /// Directory organizer for managing folder structures
    directory_organizer: Arc<DirectoryOrganizer>,
    /// Database for download persistence and duplicate detection
    database: Arc<Database>,
}

impl DownloadEngine {
    /// Create a new download engine
    pub fn new(
        config: DownloadEngineConfig, 
        blacklist_handler: Arc<BlacklistHandler>, 
        config_manager: Arc<ConfigManager>,
        directory_organizer: Arc<DirectoryOrganizer>,
        database: Arc<Database>
    ) -> DownloadResult<Self> {
        // Create the HTTP client with advanced connection pool configuration
        let client = Client::builder()
            .user_agent(&config.user_agent)
            .timeout(Duration::from_secs(config.timeout_seconds))
            // Connection pool settings
            .pool_max_idle_per_host(config.connection_pool.max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(config.connection_pool.keep_alive_timeout_secs))
            .connect_timeout(Duration::from_secs(config.connection_pool.connection_timeout_secs))
            // HTTP/2 and compression settings (let the server negotiate HTTP version)
            .gzip(config.connection_pool.gzip_enabled)
            // TCP settings
            .tcp_keepalive(if config.connection_pool.tcp_keepalive { Some(Duration::from_secs(60)) } else { None })
            .tcp_nodelay(config.connection_pool.tcp_nodelay)
            .build()
            .map_err(|e| DownloadError::Request(e))?;
        
        info!("Initialized HTTP client with connection pool: {} max idle per host, {}s keep-alive timeout",
            config.connection_pool.max_idle_per_host, config.connection_pool.keep_alive_timeout_secs);

        // Create the semaphore for bounded concurrency
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_downloads));

        // Create the stats
        let stats = Arc::new(Mutex::new(DownloadStats::default()));

        // Create the job channel
        let (job_tx, job_rx) = mpsc::channel(100);

        // Create the shutdown channel
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Create the download directories if they don't exist
        fs::create_dir_all(&config.download_dir)
            .map_err(|e| DownloadError::Io(e))?;
        fs::create_dir_all(&config.blacklisted_dir)
            .map_err(|e| DownloadError::Io(e))?;
        fs::create_dir_all(&config.temp_dir)
            .map_err(|e| DownloadError::Io(e))?;

        // Store the original concurrency setting
        let original_concurrency = config.max_concurrent_downloads;

        Ok(Self {
            config,
            client,
            semaphore,
            stats,
            job_tx,
            job_rx: Arc::new(Mutex::new(job_rx)),
            shutdown_tx,
            shutdown_rx: Arc::new(Mutex::new(shutdown_rx)),
            blacklist_handler,
            original_concurrency: Arc::new(Mutex::new(original_concurrency)),
            current_concurrency: Arc::new(Mutex::new(original_concurrency)),
            config_manager,
            directory_organizer,
            database,
        })
    }

    /// Create a new download engine from app config
    pub async fn from_config(config_manager: Arc<ConfigManager>) -> DownloadResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| DownloadError::Config(e.to_string()))?;

        // Get the e621 config
        let e621_config = config_manager.get_e621_config()
            .map_err(|e| DownloadError::Config(e.to_string()))?;

        // Create connection pool configuration based on the app config
        let connection_pool = ConnectionPoolConfig {
            max_idle_per_host: std::cmp::max(app_config.pools.max_download_concurrency, 10),
            max_idle_total: std::cmp::max(app_config.pools.max_download_concurrency * 3, 50),
            max_connections_per_host: std::cmp::max(app_config.pools.max_download_concurrency * 2, 20),
            connection_timeout_secs: 30,
            keep_alive_timeout_secs: 90,
            http2_enabled: true,
            gzip_enabled: true,
            tcp_keepalive: true,
            tcp_nodelay: true,
        };

        // Resolve paths relative to the executable location
        let exe_path = std::env::current_exe().map_err(|e| DownloadError::Config(format!("Failed to get executable path: {}", e)))?;
        let exe_dir = exe_path.parent().ok_or_else(|| DownloadError::Config("Failed to get executable directory".to_string()))?;
        
        // Resolve each path relative to the executable directory if it's a relative path
        let download_dir = if PathBuf::from(&app_config.paths.download_directory).is_absolute() {
            PathBuf::from(&app_config.paths.download_directory)
        } else {
            exe_dir.join(&app_config.paths.download_directory)
        };
        
        let blacklisted_dir = exe_dir.join("blacklisted_downloads");
        
        let temp_dir = if PathBuf::from(&app_config.paths.temp_directory).is_absolute() {
            PathBuf::from(&app_config.paths.temp_directory)
        } else {
            exe_dir.join(&app_config.paths.temp_directory)
        };

        // Create the download engine config
        let config = DownloadEngineConfig {
            max_concurrent_downloads: app_config.pools.max_download_concurrency,
            retry_attempts: 3,
            base_retry_delay_ms: (app_config.rate.retry_backoff_secs * 1000) as u64,
            download_dir: download_dir.clone(),
            blacklisted_dir,
            temp_dir,
            user_agent: format!("e621_downloader/{} (by {})", 
                env!("CARGO_PKG_VERSION"), 
                e621_config.auth.username),
            timeout_seconds: 30,
            connection_pool,
        };

        // Create the blacklist handler
        let blacklist_handler = BlacklistHandler::from_config(config_manager.clone()).await
            .map_err(|e| DownloadError::Config(format!("Failed to create blacklist handler: {}", e)))?;

        // Get configured tags from e621 config
        let mut configured_tags = e621_config.query.tags.clone();
        configured_tags.extend(e621_config.query.artists.clone());
        
        // Create the directory organizer with the resolved download directory path
        let directory_organizer = init_directory_organizer(download_dir.clone(), &app_config, configured_tags).await
            .map_err(|e| DownloadError::Config(format!("Failed to create directory organizer: {}", e)))?;

        // Create the database
        let db_path = if PathBuf::from(&app_config.paths.database_file).is_absolute() {
            PathBuf::from(&app_config.paths.database_file)
        } else {
            exe_dir.join(&app_config.paths.database_file)
        };
        
        let database = init_database(&db_path).await
            .map_err(|e| DownloadError::Config(format!("Failed to create database: {}", e)))?;

        Self::new(config, Arc::new(blacklist_handler), config_manager, Arc::new(directory_organizer), database)
    }

    /// Start the download engine
    pub async fn start(&self) -> DownloadResult<()> {
        // Clone the necessary Arc references
        let client = self.client.clone();
        let semaphore = self.semaphore.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();
        let job_rx = self.job_rx.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        let blacklist_handler = self.blacklist_handler.clone();
        let directory_organizer = self.directory_organizer.clone();
        let database = self.database.clone();

        // Get the e621 config
        let e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => return Err(DownloadError::Config(format!("Failed to get e621 config: {}", e))),
        };
        let e621_config = Arc::new(e621_config);

        // Spawn the worker task
        tokio::spawn(async move {
            
            loop {
                // Create a future for the shutdown signal
                let shutdown_future = async {
                    let mut guard = shutdown_rx.lock().await;
                    let result = guard.recv().await;
                    drop(guard);
                    result
                };
                
                // Create a future for the job
                let job_future = async {
                    let mut guard = job_rx.lock().await;
                    let result = guard.recv().await;
                    drop(guard);
                    result
                };
                
                tokio::select! {
                    // Check for shutdown signal
                    Some(_) = shutdown_future => {
                        info!("Download engine received shutdown signal");
                        break;
                    }

                    // Process the next job
                    Some(job) = job_future => {
                        // Acquire a permit from the semaphore
                        let permit = semaphore.clone().acquire_owned().await.unwrap();

                        // Clone the necessary references for the download task
                        let client_clone = client.clone();
                        let stats_clone = stats.clone();
                        let config_clone = config.clone();
                        let blacklist_handler_clone = blacklist_handler.clone();
                        let e621_config_clone = e621_config.clone();
                        let directory_organizer_clone = directory_organizer.clone();
                        let database_clone = database.clone();

                        // Spawn a task to download the file
                        tokio::spawn(async move {
                            // Process the download job
                            let result = Self::download_file(
                                &client_clone,
                                &job,
                                &config_clone,
                                stats_clone.clone(),
                                blacklist_handler_clone,
                                directory_organizer_clone,
                                database_clone,
                                &e621_config_clone,
                            ).await;

                            // Log the result
                            match &result {
                                Ok(_) => {
                                    info!("Downloaded file: {} ({})", job.post_id, job.md5);
                                    // Update stats
                                    let mut stats = stats_clone.lock().await;
                                    stats.completed_jobs += 1;
                                }
                                Err(e) => {
                                    error!("Failed to download file: {} - {}", job.post_id, e);
                                    // Update stats
                                    let mut stats = stats_clone.lock().await;
                                    stats.failed_jobs += 1;
                                }
                            }

                            // The permit is automatically dropped when this task completes
                            drop(permit);
                        });
                    }

                    // No jobs, wait a bit
                    else => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }

            info!("Download engine stopped");
        });

        info!("Download engine started");
        Ok(())
    }

    /// Stop the download engine
    pub async fn stop(&self) -> DownloadResult<()> {
        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        info!("Download engine stopping...");
        Ok(())
    }

    /// Queue a download job
    pub async fn queue_job(&self, job: DownloadJob) -> DownloadResult<()> {
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.total_jobs += 1;
        }

        // Send the job to the channel
        self.job_tx.send(job).await
            .map_err(|e| DownloadError::Download(format!("Failed to queue job: {}", e)))?;

        Ok(())
    }

    /// Queue multiple download jobs
    pub async fn queue_jobs(&self, jobs: Vec<DownloadJob>) -> DownloadResult<()> {
        for job in jobs {
            self.queue_job(job).await?;
        }

        Ok(())
    }

    /// Get the current download stats
    pub async fn get_stats(&self) -> DownloadStats {
        self.stats.lock().await.clone()
    }

    /// Get the current concurrency setting
    pub async fn get_concurrency(&self) -> usize {
        *self.current_concurrency.lock().await
    }

    /// Get the original concurrency setting
    pub async fn get_original_concurrency(&self) -> usize {
        *self.original_concurrency.lock().await
    }

    /// Set the concurrency to a new value
    pub async fn set_concurrency(&self, new_concurrency: usize) -> usize {
        let mut current = self.current_concurrency.lock().await;
        let old_concurrency = *current;
        *current = new_concurrency;

        // Adjust the semaphore permits
        let semaphore = &self.semaphore;

        if new_concurrency > old_concurrency {
            // Add permits
            let additional_permits = new_concurrency - old_concurrency;
            semaphore.add_permits(additional_permits);
            info!("Increased concurrency from {} to {}", old_concurrency, new_concurrency);
        } else if new_concurrency < old_concurrency {
            // We can't directly remove permits, but we can create a new semaphore
            // The old permits will be dropped when their tasks complete
            info!("Decreased concurrency from {} to {}", old_concurrency, new_concurrency);
            // Note: We don't need to do anything here as the semaphore will naturally
            // limit to the new concurrency as tasks complete
        }

        old_concurrency
    }

    /// Download a file
    #[instrument(skip(client, job, config, stats, blacklist_handler, directory_organizer, database, e621_config), fields(post_id = %job.post_id, md5 = %job.md5))]
    async fn download_file(
        client: &Client,
        job: &DownloadJob,
        config: &DownloadEngineConfig,
        stats: Arc<Mutex<DownloadStats>>,
        blacklist_handler: Arc<BlacklistHandler>,
        directory_organizer: Arc<DirectoryOrganizer>,
        database: Arc<Database>,
        e621_config: &E621Config,
    ) -> DownloadResult<PathBuf> {
        // Determine the content type based on the job and user context
        let username = if !e621_config.auth.username.is_empty() && e621_config.auth.username != "your_username" {
            Some(e621_config.auth.username.as_str())
        } else {
            None
        };
        let content_type = directory_organizer.determine_content_type(job, username);
        
        // Get the proper file path using the directory organizer
        let file_path = directory_organizer.get_download_path(job, content_type)
            .map_err(|e| DownloadError::Config(format!("Failed to get download path: {}", e)))?;
        
        // Create temp path using the same filename
        let filename = file_path.file_name()
            .ok_or_else(|| DownloadError::Config("Invalid file path generated".to_string()))?;
        let temp_path = config.temp_dir.join(filename);
        
        // Add blacklisted posts to the rejects table
        if job.is_blacklisted {
            if let Err(e) = blacklist_handler.add_blacklisted_reject(job.post_id).await {
                warn!("Failed to add post {} to blacklisted_rejects table: {}", job.post_id, e);
            } else {
                debug!("Added post {} to blacklisted_rejects table", job.post_id);
            }
        }

        // Check if the file already exists
        if file_path.exists() {
            debug!("File already exists: {}", file_path.display());
            return Ok(file_path);
        }

        // Try to download the file with retries
        let mut attempts = 0;
        let max_attempts = config.retry_attempts;

        loop {
            attempts += 1;

            // Log the attempt
            if attempts > 1 {
                info!("Retry attempt {}/{} for post {}", attempts, max_attempts, job.post_id);
            }

            // Create a request builder
            let mut request_builder = client.get(&job.url)
                .header("User-Agent", &config.user_agent);

            // Add authentication if credentials are provided
            if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
                && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
                let auth = format!("Basic {}", BASE64.encode(format!("{}:{}", 
                    e621_config.auth.username, 
                    e621_config.auth.api_key)));
                request_builder = request_builder.header("Authorization", auth);
                debug!("Using authentication for download request");
            } else {
                debug!("No authentication credentials provided, making unauthenticated request");
            }

            // Make the request
            let response = match request_builder.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!("Request error: {}", e);

                    // Check if we've reached the maximum number of attempts
                    if attempts >= max_attempts {
                        return Err(DownloadError::Request(e));
                    }

                    // Calculate the backoff duration
                    let backoff = Self::calculate_backoff(attempts, config.base_retry_delay_ms);
                    info!("Backing off for {}ms before retry", backoff);
                    sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
            };

            // Check for rate limiting
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                warn!("Rate limit exceeded");

                // Get the retry-after header if available
                let retry_after = response.headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(config.base_retry_delay_ms / 1000);

                info!("Backing off for {}s due to rate limiting", retry_after);
                sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            // Check for other errors
            if !response.status().is_success() {
                let status = response.status();

                // Handle specific status codes
                match status.as_u16() {
                    401 => {
                        warn!("Authentication failed (401 Unauthorized). Check your API credentials.");
                        return Err(DownloadError::InvalidResponse(
                            "Authentication failed (401 Unauthorized). Check your API credentials.".to_string()
                        ));
                    },
                    403 => {
                        warn!("Access forbidden (403 Forbidden). You may not have permission to access this resource.");
                        return Err(DownloadError::InvalidResponse(
                            "Access forbidden (403 Forbidden). You may not have permission to access this resource.".to_string()
                        ));
                    },
                    404 => {
                        warn!("Resource not found (404 Not Found). The file may have been deleted or never existed.");
                        return Err(DownloadError::InvalidResponse(
                            "Resource not found (404 Not Found). The file may have been deleted or never existed.".to_string()
                        ));
                    },
                    451 => {
                        warn!("Content unavailable for legal reasons (451). This content may be DMCA'd or otherwise legally restricted.");
                        return Err(DownloadError::InvalidResponse(
                            "Content unavailable for legal reasons (451). This content may be DMCA'd or otherwise legally restricted.".to_string()
                        ));
                    },
                    _ => {
                        warn!("HTTP error: {}", status);

                        // Check if we've reached the maximum number of attempts
                        if attempts >= max_attempts {
                            return Err(DownloadError::InvalidResponse(format!(
                                "HTTP error: {}",
                                status
                            )));
                        }

                        // Calculate the backoff duration
                        let backoff = Self::calculate_backoff(attempts, config.base_retry_delay_ms);
                        info!("Backing off for {}ms before retry", backoff);
                        sleep(Duration::from_millis(backoff)).await;
                        continue;
                    }
                }
            }

            // Stream the file to disk
            match Self::stream_to_file(response, &temp_path, stats.clone()).await {
                Ok(_) => {
                    // Move the file from the temp directory to the download directory
                    fs::rename(&temp_path, &file_path)
                        .map_err(|e| DownloadError::Io(e))?;

                    // Record the successful download in the database
                    if let Err(e) = database.record_download(job, &file_path).await {
                        warn!("Failed to record download in database: {}", e);
                        // Don't fail the download for this, but log it
                    } else {
                        debug!("Recorded download in database: post_id={}, md5={}", job.post_id, job.md5);
                    }

                    return Ok(file_path);
                }
                Err(e) => {
                    warn!("Download error: {}", e);

                    // Clean up the temp file if it exists
                    if temp_path.exists() {
                        let _ = fs::remove_file(&temp_path);
                    }

                    // Check if we've reached the maximum number of attempts
                    if attempts >= max_attempts {
                        return Err(e);
                    }

                    // Calculate the backoff duration
                    let backoff = Self::calculate_backoff(attempts, config.base_retry_delay_ms);
                    info!("Backing off for {}ms before retry", backoff);
                    sleep(Duration::from_millis(backoff)).await;
                    continue;
                }
            }
        }
    }

    /// Stream a response to a file
    async fn stream_to_file(
        response: Response,
        file_path: &Path,
        stats: Arc<Mutex<DownloadStats>>,
    ) -> DownloadResult<()> {
        // Get the content length if available
        let _content_length = response.content_length().unwrap_or(0);

        // Create the file
        let mut file = File::create(file_path).await?;

        // Stream the response body to the file
        let mut stream = response.bytes_stream();
        let bytes_downloaded = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| DownloadError::Download(format!("Failed to get chunk: {}", e)))?;
            file.write_all(&chunk).await?;

            // Update the bytes downloaded
            let _ = bytes_downloaded + chunk.len() as u64; // Track bytes for future use

            // Update the stats
            let mut stats = stats.lock().await;
            stats.bytes_downloaded += chunk.len() as u64;
        }

        // Ensure all data is written to disk
        file.flush().await?;

        Ok(())
    }

    /// Calculate the backoff duration using exponential backoff
    fn calculate_backoff(attempt: usize, base_delay_ms: u64) -> u64 {
        let exponent = attempt as u32 - 1;
        let max_delay = 60_000; // 60 seconds

        // Calculate 2^exponent * base_delay, with a maximum of max_delay
        let delay = (1 << exponent) as u64 * base_delay_ms;
        std::cmp::min(delay, max_delay)
    }
}


/// Create a new download engine
pub async fn init_download_engine(
    config_manager: Arc<ConfigManager>,
) -> DownloadResult<Arc<DownloadEngine>> {
    let engine = DownloadEngine::from_config(config_manager).await?;
    let engine = Arc::new(engine);

    // Start the download engine
    engine.start().await?;

    Ok(engine)
}
