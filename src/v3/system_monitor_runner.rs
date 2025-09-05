//! Example usage of the system monitoring and adaptive control
//! 
//! This example demonstrates how to use the system monitor to:
//! 1. Monitor memory and CPU usage
//! 2. Throttle downloads if memory pressure is detected
//! 3. Suspend new fetches if swap is active
//! 4. Dynamically adjust concurrency based on system resources

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;

use crate::v3::{
    init_config, init_system_monitor,
    ResourceStatus, ResourceEvent,
};

/// Example function to demonstrate how to use the system monitor
pub async fn system_monitor_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the system monitor
    info!("Initializing system monitor...");
    let system_monitor = init_system_monitor(config_manager.clone()).await?;
    
    // Subscribe to resource events
    let mut event_rx = system_monitor.subscribe();
    
    // Get initial metrics
    let metrics = system_monitor.get_metrics();
    info!("Initial system metrics:");
    info!("  Memory usage: {:.1}%", metrics.memory_usage);
    info!("  Swap usage: {:.1}%", metrics.swap_usage);
    info!("  CPU usage: {:.1}%", metrics.cpu_usage);
    info!("  Resource status: {:?}", metrics.status);
    info!("  Recommended concurrency: {}", metrics.recommended_concurrency);
    
    // Simulate a download manager that adapts to system resources
    info!("Starting simulated download manager...");
    
    // Spawn a task to handle resource events
    let monitor_clone = system_monitor.clone();
    let event_task = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match event {
                ResourceEvent::StatusChanged(status) => {
                    info!("Resource status changed to {:?}", status);
                    
                    // Demonstrate how a download manager would respond
                    match status {
                        ResourceStatus::Healthy => {
                            info!("System resources are healthy, proceeding with normal downloads");
                        }
                        ResourceStatus::MemoryPressure => {
                            info!("Memory pressure detected, throttling downloads");
                        }
                        ResourceStatus::CpuOverloaded => {
                            info!("CPU overloaded, reducing concurrency");
                        }
                        ResourceStatus::SwapActive => {
                            info!("Swap is active, suspending new downloads");
                        }
                    }
                }
                ResourceEvent::ConcurrencyChanged(concurrency) => {
                    info!("Concurrency changed to {}", concurrency);
                    info!("Adjusting download concurrency to {}", concurrency);
                }
            }
        }
    });
    
    // Simulate some work to generate system load
    info!("Simulating system load...");
    let load_task = tokio::spawn(async move {
        // Create a vector to store data and consume memory
        let mut data = Vec::new();
        
        // Generate some CPU and memory load
        for i in 0..10 {
            info!("Generating load iteration {}/10", i + 1);
            
            // Consume some memory
            for _ in 0..1_000_000 {
                data.push(i);
            }
            
            // Generate some CPU load
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_millis(500) {
                // Busy wait to consume CPU
                let _ = (0..10000).fold(0, |acc, x| acc + x);
            }
            
            // Check current system status
            let metrics = monitor_clone.get_metrics();
            info!("Current system metrics:");
            info!("  Memory usage: {:.1}%", metrics.memory_usage);
            info!("  Swap usage: {:.1}%", metrics.swap_usage);
            info!("  CPU usage: {:.1}%", metrics.cpu_usage);
            info!("  Resource status: {:?}", metrics.status);
            info!("  Recommended concurrency: {}", metrics.recommended_concurrency);
            
            // Demonstrate throttling and suspension decisions
            if monitor_clone.should_suspend() {
                info!("System monitor recommends suspending new fetches");
            } else if monitor_clone.should_throttle() {
                info!("System monitor recommends throttling downloads");
            } else {
                info!("System resources are healthy for downloads");
            }
            
            // Wait a bit before next iteration
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        
        // Clear the data to release memory
        data.clear();
    });
    
    // Wait for the load simulation to complete
    load_task.await?;
    
    // Wait a bit more to observe system recovery
    info!("Load simulation completed, observing system recovery...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Get final metrics
    let metrics = system_monitor.get_metrics();
    info!("Final system metrics:");
    info!("  Memory usage: {:.1}%", metrics.memory_usage);
    info!("  Swap usage: {:.1}%", metrics.swap_usage);
    info!("  CPU usage: {:.1}%", metrics.cpu_usage);
    info!("  Resource status: {:?}", metrics.status);
    info!("  Recommended concurrency: {}", metrics.recommended_concurrency);
    
    // Cancel the event task
    event_task.abort();
    
    info!("System monitor example completed successfully!");
    Ok(())
}


// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::system_monitor_example::system_monitor_example().await
// }
// ```