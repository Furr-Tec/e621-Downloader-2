//! Adaptive Concurrency Management for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Managing concurrency within user-configured limits
//! 2. Maximizing resource utilization up to configured caps
//! 3. Dynamic adjustment based on network conditions and API response times
//! 4. Performance monitoring and optimization

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::v3::ConfigManager;

/// Error types for adaptive concurrency
#[derive(Error, Debug)]
pub enum ConcurrencyError {
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Concurrency error: {0}")]
    Concurrency(String),
}

/// Result type for concurrency operations
pub type ConcurrencyResult<T> = Result<T, ConcurrencyError>;

/// Performance metrics for adaptive decisions
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub avg_response_time: Duration,
    pub success_rate: f64,
    pub error_rate: f64,
    pub throughput: f64, // requests per second
    pub last_updated: Instant,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            avg_response_time: Duration::from_millis(500),
            success_rate: 1.0,
            error_rate: 0.0,
            throughput: 0.0,
            last_updated: Instant::now(),
        }
    }
}

/// Adaptive concurrency manager
pub struct AdaptiveConcurrency {
    config_manager: Arc<ConfigManager>,
    download_semaphore: Arc<Semaphore>,
    api_semaphore: Arc<Semaphore>,
    hash_semaphore: Arc<Semaphore>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    current_download_limit: Arc<RwLock<usize>>,
    current_api_limit: Arc<RwLock<usize>>,
    current_hash_limit: Arc<RwLock<usize>>,
}

impl AdaptiveConcurrency {
    /// Create a new adaptive concurrency manager
    pub async fn new(config_manager: Arc<ConfigManager>) -> ConcurrencyResult<Self> {
        // Get initial limits from config
        let app_config = config_manager.get_app_config()
            .map_err(|e| ConcurrencyError::Config(e.to_string()))?;
        
        let download_limit = app_config.pools.max_download_concurrency;
        let api_limit = app_config.pools.max_api_concurrency;
        let hash_limit = app_config.pools.max_hash_concurrency;
        
        info!("Initializing adaptive concurrency with limits: download={}, api={}, hash={}", 
              download_limit, api_limit, hash_limit);
        
        Ok(Self {
            config_manager,
            download_semaphore: Arc::new(Semaphore::new(download_limit)),
            api_semaphore: Arc::new(Semaphore::new(api_limit)),
            hash_semaphore: Arc::new(Semaphore::new(hash_limit)),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            current_download_limit: Arc::new(RwLock::new(download_limit)),
            current_api_limit: Arc::new(RwLock::new(api_limit)),
            current_hash_limit: Arc::new(RwLock::new(hash_limit)),
        })
    }
    
    /// Get download semaphore for controlling download concurrency
    pub fn get_download_semaphore(&self) -> Arc<Semaphore> {
        self.download_semaphore.clone()
    }
    
    /// Get API semaphore for controlling API request concurrency
    pub fn get_api_semaphore(&self) -> Arc<Semaphore> {
        self.api_semaphore.clone()
    }
    
    /// Get hash semaphore for controlling hash computation concurrency
    pub fn get_hash_semaphore(&self) -> Arc<Semaphore> {
        self.hash_semaphore.clone()
    }
    
    /// Update performance metrics based on recent operations
    pub async fn update_performance_metrics(
        &self,
        response_time: Duration,
        was_successful: bool,
    ) {
        let mut metrics = self.performance_metrics.write();
        let now = Instant::now();
        
        // Use exponential weighted moving average for response time
        let alpha = 0.1; // Smoothing factor
        metrics.avg_response_time = Duration::from_nanos(
            (alpha * response_time.as_nanos() as f64 + 
             (1.0 - alpha) * metrics.avg_response_time.as_nanos() as f64) as u64
        );
        
        // Update success/error rates
        if was_successful {
            metrics.success_rate = alpha * 1.0 + (1.0 - alpha) * metrics.success_rate;
            metrics.error_rate = (1.0 - alpha) * metrics.error_rate;
        } else {
            metrics.success_rate = (1.0 - alpha) * metrics.success_rate;
            metrics.error_rate = alpha * 1.0 + (1.0 - alpha) * metrics.error_rate;
        }
        
        // Calculate throughput (requests per second over last window)
        let time_diff = now.duration_since(metrics.last_updated);
        if time_diff.as_secs() >= 1 {
            // This is a simplified throughput calculation
            // In practice, you'd want to track request counts over time windows
            metrics.throughput = 1.0 / time_diff.as_secs_f64();
            metrics.last_updated = now;
        }
        
        debug!("Performance metrics updated: avg_response={:?}, success_rate={:.2}, error_rate={:.2}, throughput={:.2}",
               metrics.avg_response_time, metrics.success_rate, metrics.error_rate, metrics.throughput);
    }
    
    /// Refresh concurrency limits from config (called when config changes)
    pub async fn refresh_from_config(&self) -> ConcurrencyResult<()> {
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| ConcurrencyError::Config(e.to_string()))?;
        
        let new_download_limit = app_config.pools.max_download_concurrency;
        let new_api_limit = app_config.pools.max_api_concurrency;
        let new_hash_limit = app_config.pools.max_hash_concurrency;
        
        // Update current limits
        {
            let mut current_download = self.current_download_limit.write();
            let mut current_api = self.current_api_limit.write();
            let mut current_hash = self.current_hash_limit.write();
            
            if *current_download != new_download_limit {
                info!("Download concurrency limit changed: {} -> {}", *current_download, new_download_limit);
                *current_download = new_download_limit;
                
                // Recreate semaphore with new limit
                // Note: This is a simplified approach. In production, you might want to 
                // gradually adjust permits instead of recreating the semaphore
                *Arc::get_mut(&mut self.download_semaphore.clone()).unwrap() = 
                    Semaphore::new(new_download_limit);
            }
            
            if *current_api != new_api_limit {
                info!("API concurrency limit changed: {} -> {}", *current_api, new_api_limit);
                *current_api = new_api_limit;
                *Arc::get_mut(&mut self.api_semaphore.clone()).unwrap() = 
                    Semaphore::new(new_api_limit);
            }
            
            if *current_hash != new_hash_limit {
                info!("Hash concurrency limit changed: {} -> {}", *current_hash, new_hash_limit);
                *current_hash = new_hash_limit;
                *Arc::get_mut(&mut self.hash_semaphore.clone()).unwrap() = 
                    Semaphore::new(new_hash_limit);
            }
        }
        
        Ok(())
    }
    
    /// Get current concurrency limits
    pub fn get_current_limits(&self) -> (usize, usize, usize) {
        let download = *self.current_download_limit.read();
        let api = *self.current_api_limit.read();
        let hash = *self.current_hash_limit.read();
        (download, api, hash)
    }
    
    /// Get current performance metrics
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance_metrics.read().clone()
    }
    
    /// Optimize concurrency based on performance metrics and network conditions
    /// This function analyzes recent performance and adjusts concurrency within configured limits
    pub async fn optimize_concurrency(&self) -> ConcurrencyResult<()> {
        let metrics = self.performance_metrics.read().clone();
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| ConcurrencyError::Config(e.to_string()))?;
        
        // Get configured maximum limits
        let max_download = app_config.pools.max_download_concurrency;
        let max_api = app_config.pools.max_api_concurrency;
        let max_hash = app_config.pools.max_hash_concurrency;
        
        // Simple optimization logic based on performance metrics
        let (current_download, current_api, current_hash) = self.get_current_limits();
        
        // If error rate is low and response times are good, use maximum configured limits
        if metrics.error_rate < 0.05 && metrics.avg_response_time < Duration::from_secs(2) {
            if current_download < max_download || current_api < max_api || current_hash < max_hash {
                info!("Performance is good, using maximum configured concurrency limits");
                
                // Update to use max limits (within config bounds)
                if current_download < max_download {
                    *self.current_download_limit.write() = max_download;
                    *Arc::get_mut(&mut self.download_semaphore.clone()).unwrap() = 
                        Semaphore::new(max_download);
                }
                
                if current_api < max_api {
                    *self.current_api_limit.write() = max_api;
                    *Arc::get_mut(&mut self.api_semaphore.clone()).unwrap() = 
                        Semaphore::new(max_api);
                }
                
                if current_hash < max_hash {
                    *self.current_hash_limit.write() = max_hash;
                    *Arc::get_mut(&mut self.hash_semaphore.clone()).unwrap() = 
                        Semaphore::new(max_hash);
                }
            }
        }
        // If error rate is high or response times are slow, reduce concurrency (but not below 1)
        else if metrics.error_rate > 0.2 || metrics.avg_response_time > Duration::from_secs(5) {
            warn!("Performance degradation detected (error_rate: {:.2}, avg_response_time: {:?}), reducing concurrency",
                  metrics.error_rate, metrics.avg_response_time);
            
            let reduced_download = std::cmp::max(1, current_download * 3 / 4);
            let reduced_api = std::cmp::max(1, current_api * 3 / 4);
            let reduced_hash = std::cmp::max(1, current_hash * 3 / 4);
            
            if reduced_download != current_download {
                *self.current_download_limit.write() = reduced_download;
                *Arc::get_mut(&mut self.download_semaphore.clone()).unwrap() = 
                    Semaphore::new(reduced_download);
            }
            
            if reduced_api != current_api {
                *self.current_api_limit.write() = reduced_api;
                *Arc::get_mut(&mut self.api_semaphore.clone()).unwrap() = 
                    Semaphore::new(reduced_api);
            }
            
            if reduced_hash != current_hash {
                *self.current_hash_limit.write() = reduced_hash;
                *Arc::get_mut(&mut self.hash_semaphore.clone()).unwrap() = 
                    Semaphore::new(reduced_hash);
            }
        }
        
        debug!("Concurrency optimization completed. Current limits: download={}, api={}, hash={}",
               *self.current_download_limit.read(), 
               *self.current_api_limit.read(), 
               *self.current_hash_limit.read());
        
        Ok(())
    }
    
    /// Get available permits for each semaphore
    pub fn get_available_permits(&self) -> (usize, usize, usize) {
        (
            self.download_semaphore.available_permits(),
            self.api_semaphore.available_permits(),
            self.hash_semaphore.available_permits(),
        )
    }
    
    /// Check if the system is currently under load
    pub fn is_under_load(&self) -> bool {
        let (download_available, api_available, hash_available) = self.get_available_permits();
        let (download_limit, api_limit, hash_limit) = self.get_current_limits();
        
        // Consider system under load if less than 25% of permits are available for any resource
        let download_usage = 1.0 - (download_available as f64 / download_limit as f64);
        let api_usage = 1.0 - (api_available as f64 / api_limit as f64);
        let hash_usage = 1.0 - (hash_available as f64 / hash_limit as f64);
        
        download_usage > 0.75 || api_usage > 0.75 || hash_usage > 0.75
    }
}

/// Initialize adaptive concurrency manager
pub async fn init_adaptive_concurrency(
    config_manager: Arc<ConfigManager>
) -> ConcurrencyResult<Arc<AdaptiveConcurrency>> {
    let concurrency = AdaptiveConcurrency::new(config_manager).await?;
    Ok(Arc::new(concurrency))
}
