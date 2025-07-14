
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Memory management and monitoring utility
pub(crate) struct MemoryManager {
    /// System information for memory monitoring
    system: Arc<Mutex<System>>,
    /// Last memory check timestamp
    last_check: Instant,
    /// Memory check interval to avoid excessive system calls
    check_interval: Duration,
    /// Memory usage warning threshold (as percentage)
    warning_threshold: f64,
    /// Memory usage critical threshold (as percentage)
    critical_threshold: f64,
}

impl MemoryManager {
    /// Creates a new MemoryManager instance
    pub(crate) fn new() -> Self {
        let mut system = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
        system.refresh_memory();
        
        MemoryManager {
            system: Arc::new(Mutex::new(system)),
            last_check: Instant::now(),
            check_interval: Duration::from_secs(5), // Check every 5 seconds
            warning_threshold: 75.0,  // Warn at 75% memory usage
            critical_threshold: 90.0, // Critical at 90% memory usage
        }
    }

    /// Gets current memory usage information
    pub(crate) fn get_memory_info(&mut self) -> MemoryInfo {
        let now = Instant::now();
        if now.duration_since(self.last_check) >= self.check_interval {
            if let Ok(mut system) = self.system.lock() {
                system.refresh_memory();
                self.last_check = now;
            }
        }

        let system = match self.system.lock() {
            Ok(sys) => sys,
            Err(e) => {
                log::error!("Failed to acquire system lock for memory info: {}", e);
                // Return default values when unable to get system info
                return MemoryInfo {
                    total_kb: 0,
                    used_kb: 0,
                    available_kb: 0,
                    usage_percentage: 0.0,
                    warning_threshold: self.warning_threshold,
                    critical_threshold: self.critical_threshold,
                };
            }
        };
        let total_memory = system.total_memory();
        let used_memory = system.used_memory();
        let available_memory = system.available_memory();
        
        let usage_percentage = if total_memory > 0 {
            (used_memory as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };

        MemoryInfo {
            total_kb: total_memory,
            used_kb: used_memory,
            available_kb: available_memory,
            usage_percentage,
            warning_threshold: self.warning_threshold,
            critical_threshold: self.critical_threshold,
        }
    }

    /// Calculates optimal batch size based on available memory and collection size
    pub(crate) fn calculate_optimal_batch_size(
        &mut self,
        collection_count: usize,
        estimated_posts_per_collection: usize,
        estimated_memory_per_post: usize,
    ) -> BatchSizeRecommendation {
        let memory_info = self.get_memory_info();
        
        // Conservative estimate: use only 50% of available memory for batching
        let usable_memory = (memory_info.available_kb * 1024) / 2; // Convert to bytes
        
        // Calculate memory needed per collection
        let memory_per_collection = estimated_posts_per_collection * estimated_memory_per_post;
        
        // Calculate how many collections we can safely process at once
        let max_collections_by_memory = if memory_per_collection > 0 {
            (usable_memory as usize / memory_per_collection).max(1)
        } else {
            collection_count
        };
        
        // Consider CPU cores - don't exceed 2x CPU cores for I/O bound operations
        let cpu_cores = num_cpus::get();
        let max_collections_by_cpu = (cpu_cores * 2).max(1);
        
        // Take the minimum of memory and CPU constraints
        let optimal_batch_size = max_collections_by_memory
            .min(max_collections_by_cpu)
            .min(collection_count)
            .max(1);
        
        // Determine reasoning
        let reasoning = if max_collections_by_memory < max_collections_by_cpu {
            BatchSizeReasoning::MemoryLimited
        } else {
            BatchSizeReasoning::CpuLimited
        };
        
        BatchSizeRecommendation {
            batch_size: optimal_batch_size,
            reasoning,
            memory_info,
            estimated_memory_per_batch: optimal_batch_size * memory_per_collection,
        }
    }

    /// Calculates optimal download concurrency based on system resources
    pub(crate) fn calculate_optimal_concurrency(&mut self) -> ConcurrencyRecommendation {
        let memory_info = self.get_memory_info();
        let cpu_cores = num_cpus::get();
        
        // Base concurrency on CPU cores and memory availability
        let base_concurrency = cpu_cores.max(2).min(8); // Between 2 and 8
        
        // Adjust based on memory pressure
        let memory_adjusted_concurrency = if memory_info.usage_percentage > self.critical_threshold {
            // Critical memory usage - reduce concurrency significantly
            base_concurrency / 2
        } else if memory_info.usage_percentage > self.warning_threshold {
            // High memory usage - reduce concurrency moderately
            (base_concurrency * 3) / 4
        } else {
            // Normal memory usage - use base concurrency
            base_concurrency
        };
        
        let final_concurrency = memory_adjusted_concurrency.max(1).min(12);
        
        ConcurrencyRecommendation {
            concurrency: final_concurrency,
            memory_info,
            cpu_cores,
        }
    }

    /// Checks if memory usage is approaching critical levels
    pub(crate) fn is_memory_critical(&mut self) -> bool {
        let memory_info = self.get_memory_info();
        memory_info.usage_percentage > self.critical_threshold
    }

    /// Checks if memory usage is in warning range
    pub(crate) fn is_memory_warning(&mut self) -> bool {
        let memory_info = self.get_memory_info();
        memory_info.usage_percentage > self.warning_threshold
    }

    /// Formats memory size for display
    pub(crate) fn format_memory_size(bytes: u64) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;

        let size = bytes as f64;

        if size >= GB {
            format!("{:.2} GB", size / GB)
        } else if size >= MB {
            format!("{:.2} MB", size / MB)
        } else if size >= KB {
            format!("{:.2} KB", size / KB)
        } else {
            format!("{} bytes", bytes)
        }
    }
}

/// Memory usage information
#[derive(Debug, Clone)]
pub(crate) struct MemoryInfo {
    pub total_kb: u64,
    pub used_kb: u64,
    pub available_kb: u64,
    pub usage_percentage: f64,
    pub warning_threshold: f64,
    pub critical_threshold: f64,
}

impl MemoryInfo {
    pub(crate) fn is_warning(&self) -> bool {
        self.usage_percentage > self.warning_threshold
    }

    pub(crate) fn is_critical(&self) -> bool {
        self.usage_percentage > self.critical_threshold
    }

    pub(crate) fn format_usage(&self) -> String {
        format!(
            "{:.1}% ({} / {})",
            self.usage_percentage,
            MemoryManager::format_memory_size(self.used_kb * 1024),
            MemoryManager::format_memory_size(self.total_kb * 1024)
        )
    }
}

/// Batch size recommendation with reasoning
#[derive(Debug)]
pub(crate) struct BatchSizeRecommendation {
    pub batch_size: usize,
    pub reasoning: BatchSizeReasoning,
    pub memory_info: MemoryInfo,
    pub estimated_memory_per_batch: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) enum BatchSizeReasoning {
    MemoryLimited,
    CpuLimited,
}

/// Concurrency recommendation
#[derive(Debug)]
pub(crate) struct ConcurrencyRecommendation {
    pub concurrency: usize,
    pub memory_info: MemoryInfo,
    pub cpu_cores: usize,
}

impl ConcurrencyRecommendation {
    pub(crate) fn explain(&self) -> String {
        format!(
            "Recommended concurrency: {} (CPU cores: {}, Memory usage: {:.1}%)",
            self.concurrency, self.cpu_cores, self.memory_info.usage_percentage
        )
    }
}

/// Estimates memory usage for a collection of posts
pub(crate) fn estimate_collection_memory_usage(post_count: usize, avg_file_size: i64) -> usize {
    // Base memory per post structure (rough estimate)
    const BASE_MEMORY_PER_POST: usize = 1024; // 1KB per post structure
    
    // Additional memory for file processing (buffers, etc.)
    let file_processing_memory = if avg_file_size > 0 {
        // Estimate 10% of average file size for processing buffers
        (avg_file_size as usize) / 10
    } else {
        8192 // Default 8KB buffer
    };
    
    post_count * (BASE_MEMORY_PER_POST + file_processing_memory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_manager_creation() {
        let manager = MemoryManager::new();
        assert!(manager.warning_threshold > 0.0);
        assert!(manager.critical_threshold > manager.warning_threshold);
    }

    #[test]
    fn test_memory_info_formatting() {
        let info = MemoryInfo {
            total_kb: 8 * 1024 * 1024, // 8 GB
            used_kb: 4 * 1024 * 1024,  // 4 GB
            available_kb: 4 * 1024 * 1024, // 4 GB
            usage_percentage: 50.0,
            warning_threshold: 75.0,
            critical_threshold: 90.0,
        };

        let formatted = info.format_usage();
        assert!(formatted.contains("50.0%"));
        assert!(formatted.contains("4.00 GB"));
        assert!(formatted.contains("8.00 GB"));
    }

    #[test]
    fn test_format_memory_size() {
        assert_eq!(MemoryManager::format_memory_size(1024), "1.00 KB");
        assert_eq!(MemoryManager::format_memory_size(1024 * 1024), "1.00 MB");
        assert_eq!(MemoryManager::format_memory_size(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(MemoryManager::format_memory_size(512), "512 bytes");
    }

    #[test]
    fn test_estimate_collection_memory_usage() {
        let usage = estimate_collection_memory_usage(100, 1024 * 1024); // 100 posts, 1MB avg
        assert!(usage > 100 * 1024); // Should be more than just the base memory
    }
}
