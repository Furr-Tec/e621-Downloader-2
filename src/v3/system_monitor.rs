//! System Monitoring and Adaptive Control for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Monitoring memory and CPU usage
//! 2. Throttling downloads if memory pressure is detected
//! 3. Suspending new fetches if swap is active
//! 4. Optionally reducing concurrency dynamically

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use sysinfo::System;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::{error, info};

use crate::v3::{
    ConfigManager,
    DownloadEngine,
};

/// Error types for system monitoring
#[derive(Error, Debug)]
pub enum SystemMonitorError {
    #[error("Config error: {0}")]
    Config(String),

    #[error("Monitoring error: {0}")]
    Monitoring(String),
}

/// Result type for system monitoring operations
pub type SystemMonitorResult<T> = Result<T, SystemMonitorError>;

/// System resource thresholds
#[derive(Debug, Clone)]
pub struct ResourceThresholds {
    /// Memory usage threshold (percentage) to start throttling
    pub memory_throttle_threshold: f32,
    /// Memory usage threshold (percentage) to suspend new fetches
    pub memory_suspend_threshold: f32,
    /// CPU usage threshold (percentage) to start throttling
    pub cpu_throttle_threshold: f32,
    /// Whether to suspend fetches if swap is active
    pub suspend_on_swap: bool,
}

impl Default for ResourceThresholds {
    fn default() -> Self {
        Self {
            memory_throttle_threshold: 80.0,
            memory_suspend_threshold: 90.0,
            cpu_throttle_threshold: 90.0,
            suspend_on_swap: true,
        }
    }
}

/// System resource status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceStatus {
    /// System resources are healthy
    Healthy,
    /// Memory pressure detected
    MemoryPressure,
    /// CPU is overloaded
    CpuOverloaded,
    /// Swap is active
    SwapActive,
}

/// System resource metrics
#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    /// Memory usage percentage
    pub memory_usage: f32,
    /// Swap usage percentage
    pub swap_usage: f32,
    /// CPU usage percentage
    pub cpu_usage: f32,
    /// Current resource status
    pub status: ResourceStatus,
    /// Recommended concurrency based on system load
    pub recommended_concurrency: usize,
}

/// Event type for resource changes
#[derive(Debug, Clone)]
pub enum ResourceEvent {
    /// Resource status changed
    StatusChanged(ResourceStatus),
    /// Recommended concurrency changed
    ConcurrencyChanged(usize),
}

/// System monitor for adaptive control
pub struct SystemMonitor {
    system: Arc<RwLock<System>>,
    thresholds: ResourceThresholds,
    download_engine: Arc<DownloadEngine>,
    config_manager: Arc<ConfigManager>,
    event_tx: broadcast::Sender<ResourceEvent>,
    last_status: Arc<RwLock<ResourceStatus>>,
    last_concurrency: Arc<RwLock<usize>>,
    update_interval: Duration,
}

impl SystemMonitor {
    /// Create a new system monitor
    pub fn new(
        download_engine: Arc<DownloadEngine>,
        config_manager: Arc<ConfigManager>,
        thresholds: ResourceThresholds,
        update_interval: Duration,
    ) -> SystemMonitorResult<Self> {
        // Initialize the system information
        let system = System::new();

        // Create broadcast channel for resource events
        let (event_tx, _) = broadcast::channel(100);

        // Note: We'll set a default concurrency and update it later
        let initial_concurrency = 1;

        Ok(Self {
            system: Arc::new(RwLock::new(system)),
            thresholds,
            download_engine,
            config_manager,
            event_tx,
            last_status: Arc::new(RwLock::new(ResourceStatus::Healthy)),
            last_concurrency: Arc::new(RwLock::new(initial_concurrency)),
            update_interval,
        })
    }

    /// Create a new system monitor from app config
    pub fn from_config(
        download_engine: Arc<DownloadEngine>,
        config_manager: Arc<ConfigManager>,
    ) -> SystemMonitorResult<Self> {
        // Get the app config
        let _app_config = config_manager.get_app_config()
            .map_err(|e| SystemMonitorError::Config(e.to_string()))?;

        // Create default thresholds
        let thresholds = ResourceThresholds::default();

        // Use a reasonable update interval (5 seconds)
        let update_interval = Duration::from_secs(5);

        Self::new(download_engine, config_manager, thresholds, update_interval)
    }

    /// Start monitoring system resources
    pub async fn start(&self) -> SystemMonitorResult<()> {
        // Clone the necessary Arc references
        let system = self.system.clone();
        let thresholds = self.thresholds.clone();
        let download_engine = self.download_engine.clone();
        let event_tx = self.event_tx.clone();
        let last_status = self.last_status.clone();
        let last_concurrency = self.last_concurrency.clone();
        let update_interval = self.update_interval;

        // Spawn the monitoring task
        tokio::spawn(async move {
            info!("System monitor started");

            loop {
                // Refresh system information
                {
                    let mut sys = system.write();
                    // Refresh all system information
                    sys.refresh_all();
                }

                // Get the current metrics
                let metrics = Self::calculate_metrics(&system, &thresholds, &download_engine);

                // Check if the status has changed
                {
                    let mut current_status = last_status.write();
                    if *current_status != metrics.status {
                        // Send status change event
                        let _ = event_tx.send(ResourceEvent::StatusChanged(metrics.status));
                        *current_status = metrics.status;
                    }
                }

                // Check if the recommended concurrency has changed
                {
                    let mut current_concurrency = last_concurrency.write();
                    if *current_concurrency != metrics.recommended_concurrency {
                        // Update the download engine concurrency
                        tokio::spawn({
                            let download_engine = download_engine.clone();
                            let concurrency = metrics.recommended_concurrency;
                            async move {
                                download_engine.set_concurrency(concurrency).await;
                            }
                        });

                        // Send concurrency change event
                        let _ = event_tx.send(ResourceEvent::ConcurrencyChanged(metrics.recommended_concurrency));
                        *current_concurrency = metrics.recommended_concurrency;
                    }
                }

                // Wait for the next update
                sleep(update_interval).await;
            }
        });

        Ok(())
    }

    /// Calculate resource metrics
    fn calculate_metrics(
        system: &Arc<RwLock<System>>,
        thresholds: &ResourceThresholds,
        _download_engine: &Arc<DownloadEngine>,
    ) -> ResourceMetrics {
        let sys = system.read();

        // Calculate memory usage
        let total_memory = sys.total_memory() as f32;
        let used_memory = sys.used_memory() as f32;
        let memory_usage = if total_memory > 0.0 {
            (used_memory / total_memory) * 100.0
        } else {
            0.0
        };

        // Calculate swap usage
        let total_swap = sys.total_swap() as f32;
        let used_swap = sys.used_swap() as f32;
        let swap_usage = if total_swap > 0.0 {
            (used_swap / total_swap) * 100.0
        } else {
            0.0
        };

        // Calculate CPU usage (average across all cores)
        let cpu_usage = sys.global_cpu_usage();

        // Determine the resource status
        let status = if swap_usage > 0.0 && thresholds.suspend_on_swap {
            ResourceStatus::SwapActive
        } else if memory_usage >= thresholds.memory_suspend_threshold {
            ResourceStatus::MemoryPressure
        } else if cpu_usage >= thresholds.cpu_throttle_threshold {
            ResourceStatus::CpuOverloaded
        } else if memory_usage >= thresholds.memory_throttle_threshold {
            ResourceStatus::MemoryPressure
        } else {
            ResourceStatus::Healthy
        };

        // Calculate recommended concurrency (we'll use blocking approach for simplicity)
        let original_concurrency = 4; // Use a reasonable default
        let recommended_concurrency = match status {
            ResourceStatus::Healthy => original_concurrency,
            ResourceStatus::MemoryPressure => {
                // Reduce concurrency based on memory pressure
                let reduction_factor: f32 = 1.0 - ((memory_usage - thresholds.memory_throttle_threshold) / 
                                            (thresholds.memory_suspend_threshold - thresholds.memory_throttle_threshold));
                let new_concurrency = (original_concurrency as f32 * reduction_factor.max(0.25)) as usize;
                new_concurrency.max(1)
            },
            ResourceStatus::CpuOverloaded => {
                // Reduce concurrency based on CPU usage
                let reduction_factor: f32 = 1.0 - ((cpu_usage - thresholds.cpu_throttle_threshold) / 
                                            (100.0 - thresholds.cpu_throttle_threshold));
                let new_concurrency = (original_concurrency as f32 * reduction_factor.max(0.25)) as usize;
                new_concurrency.max(1)
            },
            ResourceStatus::SwapActive => 1, // Minimum concurrency when swap is active
        };

        ResourceMetrics {
            memory_usage,
            swap_usage,
            cpu_usage,
            status,
            recommended_concurrency,
        }
    }

    /// Get the current resource metrics
    pub fn get_metrics(&self) -> ResourceMetrics {
        Self::calculate_metrics(&self.system, &self.thresholds, &self.download_engine)
    }

    /// Check if downloads should be throttled
    pub fn should_throttle(&self) -> bool {
        let metrics = self.get_metrics();
        matches!(metrics.status, ResourceStatus::MemoryPressure | ResourceStatus::CpuOverloaded)
    }

    /// Check if new fetches should be suspended
    pub fn should_suspend(&self) -> bool {
        let metrics = self.get_metrics();
        matches!(metrics.status, ResourceStatus::SwapActive)
    }

    /// Get a subscription to resource events
    pub fn subscribe(&self) -> broadcast::Receiver<ResourceEvent> {
        self.event_tx.subscribe()
    }

    /// Set custom resource thresholds
    pub fn set_thresholds(&mut self, thresholds: ResourceThresholds) {
        self.thresholds = thresholds;
    }
}

/// Create a new system monitor
pub async fn init_system_monitor(
    config_manager: Arc<ConfigManager>,
) -> SystemMonitorResult<Arc<SystemMonitor>> {
    // Get the app config
    let _app_config = config_manager.get_app_config()
        .map_err(|e| SystemMonitorError::Config(e.to_string()))?;

    // Initialize the download engine
    let download_engine = crate::v3::init_download_engine(config_manager.clone())
        .await
        .map_err(|e| SystemMonitorError::Config(format!("Failed to initialize download engine: {}", e)))?;

    // Create the system monitor
    let monitor = SystemMonitor::from_config(download_engine, config_manager)?;
    let monitor = Arc::new(monitor);

    // Start the system monitor
    monitor.start().await?;

    Ok(monitor)
}
