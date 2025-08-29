//! Example usage of the query planning and job creation module
//! 
//! This example demonstrates how to use the query planning module to:
//! 1. Parse input (tags, artists, etc.)
//! 2. Fetch post count via e621 API (handling pagination and filters)
//! 3. Estimate disk usage and apply download caps
//! 4. Queue tasks into a VecDeque or channel
//! 5. Deduplicate posts using in-memory and persistent hash store

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio;
use tracing::info;

use crate::v3::{
    init_config, init_orchestrator, init_query_planner,
    ConfigManager, Orchestrator, QueryPlanner, QueryPlannerResult,
};

/// Example function to demonstrate how to use the query planning module
pub async fn query_planner_example() -> Result<()> {
    // Initialize the config manager with the path to the config directory
    let config_manager = init_config(Path::new("src/v3")).await?;
    let config_manager = Arc::new(config_manager);
    
    // Initialize the orchestrator
    info!("Initializing orchestrator...");
    let orchestrator = init_orchestrator(config_manager.clone()).await?;
    
    // Initialize the query planner
    info!("Initializing query planner...");
    let query_planner = init_query_planner(
        config_manager.clone(),
        orchestrator.get_job_queue().clone(),
    ).await?;
    
    // Parse input from the e621 config
    info!("Parsing input from e621 config...");
    let queries = query_planner.parse_input().await?;
    info!("Found {} queries", queries.len());
    
    // Process each query
    for (i, query) in queries.iter().enumerate() {
        info!("Processing query {}/{}: {:?}", i + 1, queries.len(), query.tags);
        
        // Fetch post count
        let post_count = query_planner.estimate_post_count(query).await?;
        info!("Found {} posts for query", post_count);
        
        // Create a query plan
        let plan = query_planner.create_query_plan(query).await?;
        info!(
            "Created query plan with {} jobs, estimated size: {} bytes",
            plan.jobs.len(),
            plan.estimated_size_bytes
        );
        
        // Queue the jobs
        query_planner.queue_jobs(&plan).await?;
        info!("Queued {} jobs", plan.jobs.len());
    }
    
    // Wait for a bit to let the scheduler process the jobs
    info!("Waiting for jobs to be processed...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Get the tasks from the queue
    let tasks = orchestrator.get_tasks();
    info!("Tasks in the queue: {}", tasks.len());
    for task in tasks {
        info!("Task: {:?}", task);
    }
    
    // Stop the orchestrator
    info!("Stopping orchestrator...");
    orchestrator.stop().await?;
    
    info!("Query planner example completed successfully!");
    Ok(())
}

/// Run the example
#[tokio::main]
async fn main() -> Result<()> {
    query_planner_example().await
}

// To run this example, you can add the following to src/main.rs:
// 
// ```rust
// mod v3;
// 
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     v3::query_planner_example::query_planner_example().await
// }
// ```