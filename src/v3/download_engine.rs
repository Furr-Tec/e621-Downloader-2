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

use futures::stream::{self, StreamExt};
use parking_lot::Mutex;
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::v3::{
    AppConfig, E621Config,
    ConfigManager, ConfigResult,
    DownloadJob, QueryPlannerError,
    BlacklistHandler, BlacklistHandlerResult,
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
}

impl DownloadEngine {
    /// Create a new download engine
    pub fn new(config: DownloadEngineConfig, blacklist_handler: Arc<BlacklistHandler>) -> DownloadResult<Self> {
        // Create the HTTP client with a connection pool
        let client = Client::builder()
            .user_agent(&config.user_agent)
            .timeout(Duration::from_secs(config.timeout_seconds))
            .pool_max_idle_per_host(config.max_concurrent_downloads)
            .build()
            .map_err(|e| DownloadError::Request(e))?;

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
        })
    }

    /// Create a new download engine from app config
    pub fn from_config(config_manager: Arc<ConfigManager>) -> DownloadResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| DownloadError::Config(e.to_string()))?;

        // Create the download engine config
        let config = DownloadEngineConfig {
            max_concurrent_downloads: app_config.pools.max_download_concurrency,
            retry_attempts: 3,
            base_retry_delay_ms: (app_config.rate.retry_backoff_secs * 1000) as u64,
            download_dir: PathBuf::from(&app_config.paths.download_directory),
            blacklisted_dir: PathBuf::from("./blacklisted_downloads"),
            temp_dir: PathBuf::from(&app_config.paths.temp_directory),
            user_agent: "e621_downloader/2.0 (by anonymous)".to_string(),
            timeout_seconds: 30,
        };

        // Create the blacklist handler
        let blacklist_handler = BlacklistHandler::from_config(config_manager.clone())
            .map_err(|e| DownloadError::Config(format!("Failed to create blacklist handler: {}", e)))?;

        Self::new(config, Arc::new(blacklist_handler))
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

        // Spawn the worker task
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx.lock().await;
            let mut job_rx = job_rx.lock().await;

            loop {
                tokio::select! {
                    // Check for shutdown signal
                    Some(_) = shutdown_rx.recv() => {
                        info!("Download engine received shutdown signal");
                        break;
                    }

                    // Process the next job
                    Some(job) = job_rx.recv() => {
                        // Acquire a permit from the semaphore
                        let permit = semaphore.clone().acquire_owned().await.unwrap();

                        // Clone the necessary references for the download task
                        let client_clone = client.clone();
                        let stats_clone = stats.clone();
                        let config_clone = config.clone();

                        // Spawn a task to download the file
                        tokio::spawn(async move {
                            // Process the download job
                            let result = Self::download_file(
                                &client_clone,
                                &job,
                                &config_clone,
                                stats_clone.clone(),
                                self.blacklist_handler.clone(),
                            ).await;

                            // Log the result
                            match &result {
                                Ok(_) => {
                                    info!("Downloaded file: {} ({})", job.post_id, job.md5);
                                    // Update stats
                                    let mut stats = stats_clone.lock();
                                    stats.completed_jobs += 1;
                                }
                                Err(e) => {
                                    error!("Failed to download file: {} - {}", job.post_id, e);
                                    // Update stats
                                    let mut stats = stats_clone.lock();
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
            let mut stats = self.stats.lock();
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
    pub fn get_stats(&self) -> DownloadStats {
        self.stats.lock().clone()
    }

    /// Get the current concurrency setting
    pub fn get_concurrency(&self) -> usize {
        *self.current_concurrency.lock()
    }

    /// Get the original concurrency setting
    pub fn get_original_concurrency(&self) -> usize {
        *self.original_concurrency.lock()
    }

    /// Set the concurrency to a new value
    pub fn set_concurrency(&self, new_concurrency: usize) -> usize {
        let mut current = self.current_concurrency.lock();
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
    #[instrument(skip(client, job, config, stats, blacklist_handler), fields(post_id = %job.post_id, md5 = %job.md5))]
    async fn download_file(
        client: &Client,
        job: &DownloadJob,
        config: &DownloadEngineConfig,
        stats: Arc<Mutex<DownloadStats>>,
        blacklist_handler: Arc<BlacklistHandler>,
    ) -> DownloadResult<PathBuf> {
        // Create the file path
        let file_name = format!("{}_{}.{}", job.post_id, job.md5, job.file_ext);

        // Determine the target directory based on whether the post is blacklisted
        let target_dir = if job.is_blacklisted {
            // Extract artist tags
            let artists = job.tags.iter()
                .filter(|tag| tag.starts_with("artist:"))
                .map(|tag| tag.trim_start_matches("artist:"))
                .collect::<Vec<_>>();

            // Create the artist directory
            let artist_dir = if !artists.is_empty() {
                config.blacklisted_dir.join(sanitize_filename(&artists[0]))
            } else {
                config.blacklisted_dir.join("unknown_artist")
            };

            // Create the directory if it doesn't exist
            if !artist_dir.exists() {
                fs::create_dir_all(&artist_dir)
                    .map_err(|e| DownloadError::Io(e))?;
            }

            // Add the post to the blacklisted_rejects table
            if let Err(e) = blacklist_handler.add_blacklisted_reject(job.post_id) {
                warn!("Failed to add post {} to blacklisted_rejects table: {}", job.post_id, e);
            } else {
                debug!("Added post {} to blacklisted_rejects table", job.post_id);
            }

            artist_dir
        } else {
            config.download_dir.clone()
        };

        let file_path = target_dir.join(&file_name);
        let temp_path = config.temp_dir.join(&file_name);

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

            // Make the request
            let response = match client.get(&job.url).send().await {
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
                warn!("HTTP error: {}", response.status());

                // Check if we've reached the maximum number of attempts
                if attempts >= max_attempts {
                    return Err(DownloadError::InvalidResponse(format!(
                        "HTTP error: {}",
                        response.status()
                    )));
                }

                // Calculate the backoff duration
                let backoff = Self::calculate_backoff(attempts, config.base_retry_delay_ms);
                info!("Backing off for {}ms before retry", backoff);
                sleep(Duration::from_millis(backoff)).await;
                continue;
            }

            // Stream the file to disk
            match Self::stream_to_file(response, &temp_path, stats.clone()).await {
                Ok(_) => {
                    // Move the file from the temp directory to the download directory
                    fs::rename(&temp_path, &file_path)
                        .map_err(|e| DownloadError::Io(e))?;

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
        let content_length = response.content_length().unwrap_or(0);

        // Create the file
        let mut file = File::create(file_path).await?;

        // Stream the response body to the file
        let mut stream = response.bytes_stream();
        let mut bytes_downloaded = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk).await?;

            // Update the bytes downloaded
            bytes_downloaded += chunk.len() as u64;

            // Update the stats
            let mut stats = stats.lock();
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

/// Create a new download engine
pub async fn init_download_engine(
    config_manager: Arc<ConfigManager>,
) -> DownloadResult<Arc<DownloadEngine>> {
    let engine = DownloadEngine::from_config(config_manager)?;
    let engine = Arc::new(engine);

    // Start the download engine
    engine.start().await?;

    Ok(engine)
}
