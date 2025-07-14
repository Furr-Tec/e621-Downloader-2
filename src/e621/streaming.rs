use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::collections::VecDeque;
use tokio::time::sleep;
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::task::JoinHandle;
use log::{info, warn, error, debug, trace};
use crate::e621::grabber::{Grabber, GrabbedPost, PostCollection};
use crate::e621::memory::MemoryManager;
use crate::e621::sender::RequestSender;
use crate::e621::io::directory::DirectoryManager;
use crate::e621::io::Config;
use indicatif::ProgressBar;

/// Configuration for streaming processing
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Maximum number of posts to process in a single chunk
    pub chunk_size: usize,
    /// Maximum number of chunks to buffer in memory
    pub max_buffered_chunks: usize,
    /// Memory pressure threshold (percentage of available memory)
    pub memory_pressure_threshold: f64,
    /// Delay between memory pressure checks
    pub memory_check_interval: Duration,
    /// Number of concurrent download workers
    pub concurrent_workers: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50,
            max_buffered_chunks: 4,
            memory_pressure_threshold: 0.8,
            memory_check_interval: Duration::from_secs(5),
            concurrent_workers: 4,
        }
    }
}

/// Represents a chunk of posts to be processed
#[derive(Debug, Clone)]
pub struct PostChunk {
    pub collection_name: String,
    pub posts: Vec<GrabbedPost>,
    pub chunk_index: usize,
    pub total_chunks: usize,
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub total_memory: usize,
    pub used_memory: usize,
    pub available_memory: usize,
    pub memory_pressure: f64,
    pub last_checked: Instant,
}

impl MemoryStats {
    /// Check if memory pressure is above threshold
    pub fn is_under_pressure(&self, threshold: f64) -> bool {
        self.memory_pressure > threshold
    }
    
    /// Format memory statistics for display
    pub fn format_stats(&self) -> String {
        format!(
            "Memory: {:.1}% used ({:.1} MB / {:.1} MB), pressure: {:.1}%",
            (self.used_memory as f64 / self.total_memory as f64) * 100.0,
            self.used_memory as f64 / (1024.0 * 1024.0),
            self.total_memory as f64 / (1024.0 * 1024.0),
            self.memory_pressure * 100.0
        )
    }
}

/// Streaming processor for large collections
pub struct StreamingProcessor {
    config: StreamingConfig,
    memory_manager: Arc<Mutex<MemoryManager>>,
    request_sender: RequestSender,
    progress_bar: Option<ProgressBar>,
    global_progress: Arc<AtomicUsize>,
    memory_stats: Arc<RwLock<MemoryStats>>,
}

/// Collection structure for streaming processing
#[derive(Debug, Clone)]
pub struct Collection {
    pub name: String,
    pub posts: Vec<GrabbedPost>,
}

impl StreamingProcessor {
    /// Create a new streaming processor
    pub fn new(config: StreamingConfig) -> Self {
        let memory_manager = Arc::new(Mutex::new(MemoryManager::new()));
        let global_progress = Arc::new(AtomicUsize::new(0));
        
        let memory_stats = Arc::new(RwLock::new(MemoryStats {
            total_memory: 0,
            used_memory: 0,
            available_memory: 0,
            memory_pressure: 0.0,
            last_checked: Instant::now(),
        }));
        
        Self {
            config,
            memory_manager,
            request_sender: RequestSender::new(), // Will be set later
            progress_bar: None,
            global_progress,
            memory_stats,
        }
    }
    
    /// Set the request sender for the processor
    pub fn set_request_sender(&mut self, request_sender: RequestSender) {
        self.request_sender = request_sender;
    }
    
    /// Set the progress bar for visual feedback
    pub fn set_progress_bar(&mut self, progress_bar: ProgressBar) {
        self.progress_bar = Some(progress_bar);
    }
    
    /// Process collections using streaming with async support
    pub async fn process_collection_stream(&self, grabber: &Grabber) -> Result<(), anyhow::Error> {
        let collections = grabber.posts();
        
        if collections.is_empty() {
            info!("No collections to process");
            return Ok(());
        }
        
        info!("Starting streaming processing for {} collections", collections.len());
        
        // Start memory monitoring task
        let memory_monitor_handle = self.start_memory_monitor().await;
        
        // Create channels for streaming
        let (chunk_tx, chunk_rx) = mpsc::channel::<PostChunk>(self.config.max_buffered_chunks);
        let (result_tx, mut result_rx) = mpsc::channel::<ProcessingResult>(1000);
        
        // Start chunk producer
        let producer_handle = self.start_chunk_producer(collections, chunk_tx).await;
        
        // Start processing workers
        let worker_handles = self.start_processing_workers(chunk_rx, result_tx).await;
        
        // Process results
        let mut total_processed = 0;
        let mut successful_downloads = 0;
        let mut failed_downloads = 0;
        
        // Wait for results
        while let Some(result) = result_rx.recv().await {
            match result {
                ProcessingResult::Success { file_name, .. } => {
                    successful_downloads += 1;
                    total_processed += 1;
                    
                    if let Some(pb) = &self.progress_bar {
                        pb.set_position(total_processed);
                        pb.set_message(format!("Downloaded: {}", file_name));
                    }
                    
                    self.global_progress.fetch_add(1, Ordering::SeqCst);
                    
                    // Check memory pressure periodically
                    if total_processed % 10 == 0 {
                        let stats = self.memory_stats.read().await;
                        if stats.is_under_pressure(self.config.memory_pressure_threshold) {
                            warn!("Memory pressure detected: {}", stats.format_stats());
                            // Add small delay to reduce pressure
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                },
                ProcessingResult::Error { error, .. } => {
                    failed_downloads += 1;
                    total_processed += 1;
                    warn!("Processing error: {}", error);
                    
                    if let Some(pb) = &self.progress_bar {
                        pb.set_position(total_processed);
                        pb.set_message(format!("Error: {}", error));
                    }
                },
                ProcessingResult::Complete => {
                    info!("Processing complete");
                    break;
                }
            }
        }
        
        // Wait for all tasks to complete
        let _ = producer_handle.await;
        for handle in worker_handles {
            let _ = handle.await;
        }
        memory_monitor_handle.abort();
        
        info!(
            "Streaming processing completed: {} successful, {} failed, {} total",
            successful_downloads, failed_downloads, total_processed
        );
        
        Ok(())
    }
    
    /// Process collections using the simplified interface
    pub async fn process_collections(&self, collections: Vec<Collection>) -> Result<(), anyhow::Error> {
        if collections.is_empty() {
            info!("No collections to process");
            return Ok(());
        }
        
        info!("Starting streaming processing for {} collections", collections.len());
        
        // Start memory monitoring task
        let memory_monitor_handle = self.start_memory_monitor().await;
        
        // Create channels for streaming
        let (chunk_tx, chunk_rx) = mpsc::channel::<PostChunk>(self.config.max_buffered_chunks);
        let (result_tx, mut result_rx) = mpsc::channel::<ProcessingResult>(1000);
        
        // Start chunk producer for simplified collections
        let producer_handle = self.start_simple_chunk_producer(collections, chunk_tx).await;
        
        // Start processing workers
        let worker_handles = self.start_processing_workers(chunk_rx, result_tx).await;
        
        // Process results
        let mut total_processed = 0;
        let mut successful_downloads = 0;
        let mut failed_downloads = 0;
        
        // Wait for results
        while let Some(result) = result_rx.recv().await {
            match result {
                ProcessingResult::Success { file_name, .. } => {
                    successful_downloads += 1;
                    total_processed += 1;
                    
                    if let Some(pb) = &self.progress_bar {
                        pb.set_position(total_processed);
                        pb.set_message(format!("Downloaded: {}", file_name));
                    }
                    
                    self.global_progress.fetch_add(1, Ordering::SeqCst);
                    
                    // Check memory pressure periodically
                    if total_processed % 10 == 0 {
                        let stats = self.memory_stats.read().await;
                        if stats.is_under_pressure(self.config.memory_pressure_threshold) {
                            warn!("Memory pressure detected: {}", stats.format_stats());
                            // Add small delay to reduce pressure
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                },
                ProcessingResult::Error { error, .. } => {
                    failed_downloads += 1;
                    total_processed += 1;
                    warn!("Processing error: {}", error);
                    
                    if let Some(pb) = &self.progress_bar {
                        pb.set_position(total_processed);
                        pb.set_message(format!("Error: {}", error));
                    }
                },
                ProcessingResult::Complete => {
                    info!("Processing complete");
                    break;
                }
            }
        }
        
        // Wait for all tasks to complete
        let _ = producer_handle.await;
        for handle in worker_handles {
            let _ = handle.await;
        }
        memory_monitor_handle.abort();
        
        info!(
            "Streaming processing completed: {} successful, {} failed, {} total",
            successful_downloads, failed_downloads, total_processed
        );
        
        Ok(())
    }
    
    /// Start memory monitoring task
    async fn start_memory_monitor(&self) -> JoinHandle<()> {
        let memory_stats = Arc::clone(&self.memory_stats);
        let check_interval = self.config.memory_check_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            
            loop {
                interval.tick().await;
                
                // Get memory information (platform-specific)
                let (total, used, available) = get_memory_info();
                let pressure = used as f64 / total as f64;
                
                let stats = MemoryStats {
                    total_memory: total,
                    used_memory: used,
                    available_memory: available,
                    memory_pressure: pressure,
                    last_checked: Instant::now(),
                };
                
                {
                    let mut memory_stats = memory_stats.write().await;
                    *memory_stats = stats;
                }
                
                // Log memory pressure warnings
                if pressure > 0.9 {
                    warn!("Critical memory pressure: {:.1}%", pressure * 100.0);
                } else if pressure > 0.8 {
                    warn!("High memory pressure: {:.1}%", pressure * 100.0);
                }
            }
        })
    }
    
    /// Start chunk producer task
    async fn start_chunk_producer(
        &self,
        collections: &[PostCollection],
        chunk_tx: mpsc::Sender<PostChunk>,
    ) -> JoinHandle<()> {
        let collections_data: Vec<(String, Vec<GrabbedPost>)> = collections
            .iter()
            .map(|c| (c.name().to_string(), c.posts().clone()))
            .collect();
        
        let chunk_size = self.config.chunk_size;
        let memory_stats = Arc::clone(&self.memory_stats);
        
        tokio::spawn(async move {
            for (collection_name, posts) in collections_data {
                let new_posts: Vec<GrabbedPost> = posts
                    .into_iter()
                    .filter(|post| post.is_new())
                    .collect();
                
                if new_posts.is_empty() {
                    continue;
                }
                
                let total_chunks = (new_posts.len() + chunk_size - 1) / chunk_size;
                
                info!("Processing collection '{}': {} new posts in {} chunks", 
                      collection_name, new_posts.len(), total_chunks);
                
                // Process posts in chunks
                for (chunk_index, chunk_posts) in new_posts.chunks(chunk_size).enumerate() {
                    let chunk = PostChunk {
                        collection_name: collection_name.clone(),
                        posts: chunk_posts.to_vec(),
                        chunk_index,
                        total_chunks,
                    };
                    
                    // Check memory pressure before sending chunk
                    loop {
                        let stats = memory_stats.read().await;
                        if !stats.is_under_pressure(0.85) {
                            break;
                        }
                        
                        warn!("Memory pressure too high, waiting... {}", stats.format_stats());
                        sleep(Duration::from_millis(500)).await;
                    }
                    
                    if chunk_tx.send(chunk).await.is_err() {
                        error!("Failed to send chunk to processor");
                        break;
                    }
                }
            }
            
            trace!("Chunk producer completed");
        })
    }
    
    /// Start chunk producer task for simplified Collection interface
    async fn start_simple_chunk_producer(
        &self,
        collections: Vec<Collection>,
        chunk_tx: mpsc::Sender<PostChunk>,
    ) -> JoinHandle<()> {
        let chunk_size = self.config.chunk_size;
        let memory_stats = Arc::clone(&self.memory_stats);
        
        tokio::spawn(async move {
            for collection in collections {
                let new_posts: Vec<GrabbedPost> = collection.posts
                    .into_iter()
                    .filter(|post| post.is_new())
                    .collect();
                
                if new_posts.is_empty() {
                    continue;
                }
                
                let total_chunks = (new_posts.len() + chunk_size - 1) / chunk_size;
                
                info!("Processing collection '{}': {} new posts in {} chunks", 
                      collection.name, new_posts.len(), total_chunks);
                
                // Process posts in chunks
                for (chunk_index, chunk_posts) in new_posts.chunks(chunk_size).enumerate() {
                    let chunk = PostChunk {
                        collection_name: collection.name.clone(),
                        posts: chunk_posts.to_vec(),
                        chunk_index,
                        total_chunks,
                    };
                    
                    // Check memory pressure before sending chunk
                    loop {
                        let stats = memory_stats.read().await;
                        if !stats.is_under_pressure(0.85) {
                            break;
                        }
                        
                        warn!("Memory pressure too high, waiting... {}", stats.format_stats());
                        sleep(Duration::from_millis(500)).await;
                    }
                    
                    if chunk_tx.send(chunk).await.is_err() {
                        error!("Failed to send chunk to processor");
                        break;
                    }
                }
            }
            
            trace!("Simple chunk producer completed");
        })
    }
    
    /// Start processing worker tasks
    async fn start_processing_workers(
        &self,
        chunk_rx: mpsc::Receiver<PostChunk>,
        result_tx: mpsc::Sender<ProcessingResult>,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        
        // Wrap the receiver in Arc<Mutex<>> to share it among workers
        let chunk_rx = Arc::new(Mutex::new(chunk_rx));
        
        for worker_id in 0..self.config.concurrent_workers {
            let chunk_rx = Arc::clone(&chunk_rx);
            let result_tx = result_tx.clone();
            let request_sender = self.request_sender.clone();
            
            let handle = tokio::spawn(async move {
                Self::worker_process_chunks(worker_id, chunk_rx, result_tx, request_sender).await;
            });
            
            handles.push(handle);
        }
        
        handles
    }
    
    /// Worker task for processing chunks
    async fn worker_process_chunks(
        worker_id: usize,
        chunk_rx: Arc<Mutex<mpsc::Receiver<PostChunk>>>,
        result_tx: mpsc::Sender<ProcessingResult>,
        request_sender: RequestSender,
    ) {
        debug!("Worker {} started", worker_id);
        
        loop {
            let chunk = {
                let mut rx = chunk_rx.lock().await;
                match rx.recv().await {
                    Some(chunk) => chunk,
                    None => break, // Channel closed
                }
            };
            
            debug!("Worker {} processing chunk {}/{} from collection '{}'", 
                   worker_id, chunk.chunk_index + 1, chunk.total_chunks, chunk.collection_name);
            
            for post in &chunk.posts {
                let result = process_single_post(post, &request_sender).await;
                
                if result_tx.send(result).await.is_err() {
                    error!("Worker {} failed to send result", worker_id);
                    break;
                }
            }
        }
        
        debug!("Worker {} completed", worker_id);
    }
}

/// Result of processing a single post
#[derive(Debug)]
pub enum ProcessingResult {
    Success {
        file_name: String,
        file_path: PathBuf,
        download_time: Duration,
        file_size: u64,
    },
    Error {
        file_name: String,
        error: String,
    },
    Complete,
}

/// Process a single post (download and hash)
async fn process_single_post(post: &GrabbedPost, request_sender: &RequestSender) -> ProcessingResult {
    let start_time = Instant::now();
    let file_name = post.name().to_string();
    
    let save_dir = match post.save_directory() {
        Some(dir) => dir,
        None => {
            return ProcessingResult::Error {
                file_name,
                error: "No save directory assigned".to_string(),
            };
        }
    };
    
    let file_path = save_dir.join(remove_invalid_chars(&file_name));
    
    // Download the file
    match download_file_async(post.url(), &file_path, request_sender).await {
        Ok(file_size) => {
            let download_time = start_time.elapsed();
            
            // Update database
            if let Err(e) = update_database_entry(post, &file_path).await {
                warn!("Failed to update database for {}: {}", file_name, e);
            }
            
            ProcessingResult::Success {
                file_name,
                file_path,
                download_time,
                file_size,
            }
        },
        Err(e) => ProcessingResult::Error {
            file_name,
            error: e.to_string(),
        }
    }
}

/// Async file download
async fn download_file_async(
    url: &str,
    file_path: &PathBuf,
    request_sender: &RequestSender,
) -> Result<u64, anyhow::Error> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    
    // Download in background thread to avoid blocking async runtime
    let url = url.to_string();
    let file_path = file_path.clone();
    let request_sender = request_sender.clone();
    
    let result = tokio::task::spawn_blocking(move || {
        let bytes = request_sender.get_bytes_from_url(&url)?;
        std::fs::write(&file_path, &bytes)?;
        Ok::<u64, anyhow::Error>(bytes.len() as u64)
    }).await??;
    
    Ok(result)
}

/// Update database entry for downloaded file
async fn update_database_entry(post: &GrabbedPost, file_path: &PathBuf) -> Result<(), anyhow::Error> {
    // This would normally update the DirectoryManager database
    // For now, we'll just log the operation
    trace!("Would update database for file: {}", file_path.display());
    
    // In a real implementation, this would:
    // 1. Calculate file hash
    // 2. Update DirectoryManager with file info
    // 3. Store post metadata
    
    Ok(())
}

/// Remove invalid characters from filename
fn remove_invalid_chars(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '?' | ':' | '*' | '<' | '>' | '"' | '|' => '_',
            _ => c,
        })
        .collect()
}

/// Get system memory information (platform-specific)
fn get_memory_info() -> (usize, usize, usize) {
    // This is a simplified implementation
    // In a real system, this would query actual system memory
    
    #[cfg(target_os = "windows")]
    {
        use std::mem;
        use winapi::um::sysinfoapi::{GetPhysicallyInstalledSystemMemory, GlobalMemoryStatusEx, MEMORYSTATUSEX};
        
        unsafe {
            let mut mem_status: MEMORYSTATUSEX = mem::zeroed();
            mem_status.dwLength = mem::size_of::<MEMORYSTATUSEX>() as u32;
            
            if GlobalMemoryStatusEx(&mut mem_status) != 0 {
                let total = mem_status.ullTotalPhys as usize;
                let available = mem_status.ullAvailPhys as usize;
                let used = total - available;
                return (total, used, available);
            }
        }
    }
    
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            let mut total = 0;
            let mut available = 0;
            
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    if let Some(kb) = line.split_whitespace().nth(1) {
                        total = kb.parse::<usize>().unwrap_or(0) * 1024;
                    }
                } else if line.starts_with("MemAvailable:") {
                    if let Some(kb) = line.split_whitespace().nth(1) {
                        available = kb.parse::<usize>().unwrap_or(0) * 1024;
                    }
                }
            }
            
            let used = total - available;
            return (total, used, available);
        }
    }
    
    // Fallback for other platforms or if system calls fail
    let total = 8 * 1024 * 1024 * 1024; // 8GB default
    let used = total / 2; // Assume 50% used
    let available = total - used;
    
    (total, used, available)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    
    #[test]
    async fn test_memory_stats_creation() {
        let stats = MemoryStats {
            total_memory: 8 * 1024 * 1024 * 1024, // 8GB
            used_memory: 4 * 1024 * 1024 * 1024,  // 4GB
            available_memory: 4 * 1024 * 1024 * 1024, // 4GB
            memory_pressure: 0.5,
            last_checked: Instant::now(),
        };
        
        assert!(!stats.is_under_pressure(0.8));
        assert!(stats.is_under_pressure(0.3));
        
        let formatted = stats.format_stats();
        assert!(formatted.contains("50.0% used"));
    }
    
    #[test]
    async fn test_streaming_config_default() {
        let config = StreamingConfig::default();
        assert_eq!(config.chunk_size, 50);
        assert_eq!(config.max_buffered_chunks, 4);
        assert_eq!(config.memory_pressure_threshold, 0.8);
        assert_eq!(config.concurrent_workers, 4);
    }
    
    #[test]
    async fn test_remove_invalid_chars() {
        let input = "file:with*invalid<chars>\"test|.jpg";
        let expected = "file_with_invalid_chars__test_.jpg";
        assert_eq!(remove_invalid_chars(input), expected);
    }
}
