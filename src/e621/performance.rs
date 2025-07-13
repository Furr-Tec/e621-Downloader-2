use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use tracing::{info, warn, span, Level};

/// Performance metrics tracker
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_requests: usize,
    pub total_duration: Duration,
    pub avg_duration: Duration,
    pub max_duration: Duration,
    pub min_duration: Duration,
    pub error_count: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_requests: 0,
            total_duration: Duration::from_secs(0),
            avg_duration: Duration::from_secs(0),
            max_duration: Duration::from_secs(0),
            min_duration: Duration::from_secs(u64::MAX),
            error_count: 0,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
}

/// Global performance tracker
pub static PERFORMANCE_TRACKER: Lazy<Arc<Mutex<HashMap<String, PerformanceMetrics>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Performance timer for measuring operations
pub struct PerformanceTimer {
    operation: String,
    start_time: Instant,
    span: tracing::Span,
}

impl PerformanceTimer {
    pub fn new(operation: &str) -> Self {
        let span = span!(Level::INFO, "performance", operation = operation);
        let binding = span.clone();
        let _enter = binding.enter();
        
        Self {
            operation: operation.to_string(),
            start_time: std::time::Instant::now(),
            span,
        }
    }
    
    pub fn finish(self) -> Duration {
        let duration = self.start_time.elapsed();
        self.record_timing(duration);
        duration
    }
    
    pub fn finish_with_error(self) -> Duration {
        let duration = self.start_time.elapsed();
        self.record_timing_with_error(duration);
        duration
    }
    
    fn record_timing(&self, duration: Duration) {
        let _enter = self.span.enter();
        
        if let Ok(mut tracker) = PERFORMANCE_TRACKER.lock() {
            let metrics = tracker.entry(self.operation.clone()).or_default();
            
            metrics.total_requests += 1;
            metrics.total_duration += duration;
            metrics.avg_duration = metrics.total_duration / metrics.total_requests as u32;
            
            if duration > metrics.max_duration {
                metrics.max_duration = duration;
            }
            
            if duration < metrics.min_duration {
                metrics.min_duration = duration;
            }
            
            // Log slow operations
            if duration > Duration::from_secs(2) {
                warn!(
                    "Slow operation detected: {} took {:.2?}",
                    self.operation, duration
                );
            }
        }
    }
    
    fn record_timing_with_error(&self, duration: Duration) {
        let _enter = self.span.enter();
        
        if let Ok(mut tracker) = PERFORMANCE_TRACKER.lock() {
            let metrics = tracker.entry(self.operation.clone()).or_default();
            
            metrics.total_requests += 1;
            metrics.error_count += 1;
            metrics.total_duration += duration;
            metrics.avg_duration = metrics.total_duration / metrics.total_requests as u32;
        }
    }
}

/// Record cache hit
pub fn record_cache_hit(operation: &str) {
    if let Ok(mut tracker) = PERFORMANCE_TRACKER.lock() {
        let metrics = tracker.entry(operation.to_string()).or_default();
        metrics.cache_hits += 1;
    }
}

/// Record cache miss
pub fn record_cache_miss(operation: &str) {
    if let Ok(mut tracker) = PERFORMANCE_TRACKER.lock() {
        let metrics = tracker.entry(operation.to_string()).or_default();
        metrics.cache_misses += 1;
    }
}

/// Get performance summary
pub fn get_performance_summary() -> HashMap<String, PerformanceMetrics> {
    PERFORMANCE_TRACKER.lock().unwrap().clone()
}

/// Print performance summary
pub fn print_performance_summary() {
    let summary = get_performance_summary();
    
    if summary.is_empty() {
        info!("No performance metrics available");
        return;
    }
    
    info!("=== Performance Summary ===");
    
    for (operation, metrics) in summary {
        let cache_hit_rate = if metrics.cache_hits + metrics.cache_misses > 0 {
            (metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64) * 100.0
        } else {
            0.0
        };
        
        info!(
            "{}: {} requests, avg: {:.2?}, max: {:.2?}, min: {:.2?}, errors: {}, cache hit rate: {:.1}%",
            operation,
            metrics.total_requests,
            metrics.avg_duration,
            metrics.max_duration,
            if metrics.min_duration == Duration::from_secs(u64::MAX) {
                Duration::from_secs(0)
            } else {
                metrics.min_duration
            },
            metrics.error_count,
            cache_hit_rate
        );
    }
}

/// Initialize performance tracking
pub fn init_performance_tracking() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(false)
        .init();
    
    info!("Performance tracking initialized");
}

/// Macro for easy performance timing
#[macro_export]
macro_rules! time_operation {
    ($operation:expr, $code:block) => {{
        let timer = $crate::e621::performance::PerformanceTimer::new($operation);
        let result = $code;
        timer.finish();
        result
    }};
}

/// Macro for timing operations that might error
#[macro_export]
macro_rules! time_operation_with_error {
    ($operation:expr, $code:block) => {{
        let timer = $crate::e621::performance::PerformanceTimer::new($operation);
        let result = $code;
        match &result {
            Ok(_) => {
                timer.finish();
            }
            Err(_) => {
                timer.finish_with_error();
            }
        }
        result
    }};
}
