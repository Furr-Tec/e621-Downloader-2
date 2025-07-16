//! Query Planning and Job Creation for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Parsing input (tags, artists, etc.)
//! 2. Fetching post count via e621 API (handling pagination and filters)
//! 3. Estimating disk usage and applying download caps
//! 4. Queuing tasks into a VecDeque or channel
//! 5. Deduplicating posts using in-memory and persistent hash store

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use rusqlite::{Connection, Result as SqliteResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::v3::{
    AppConfig, E621Config, RulesConfig,
    ConfigManager, ConfigResult,
    Query, QueryQueue, QueryStatus, QueryTask,
};

/// Error types for query planning
#[derive(Error, Debug)]
pub enum QueryPlannerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("API error: {0}")]
    Api(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Config error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),

    #[error("Size limit exceeded: {0}")]
    SizeLimitExceeded(String),
}

/// Result type for query planning operations
pub type QueryPlannerResult<T> = Result<T, QueryPlannerError>;

/// E621 API response for post counts
#[derive(Debug, Deserialize)]
pub struct PostCountResponse {
    pub counts: PostCounts,
}

/// Post counts from E621 API
#[derive(Debug, Deserialize)]
pub struct PostCounts {
    pub posts: u32,
}

/// E621 API response for posts
#[derive(Debug, Deserialize)]
pub struct PostsResponse {
    pub posts: Vec<Post>,
}

/// Post data from E621 API
#[derive(Debug, Deserialize, Clone)]
pub struct Post {
    pub id: u32,
    pub file: PostFile,
    pub tags: PostTags,
    pub score: PostScore,
    pub rating: String,
    pub created_at: String,
    pub updated_at: String,
}

/// File information for a post
#[derive(Debug, Deserialize, Clone)]
pub struct PostFile {
    pub width: u32,
    pub height: u32,
    pub ext: String,
    pub size: u32,
    pub md5: String,
    pub url: Option<String>,
}

/// Tags for a post
#[derive(Debug, Deserialize, Clone)]
pub struct PostTags {
    pub general: Vec<String>,
    pub species: Vec<String>,
    pub character: Vec<String>,
    pub copyright: Vec<String>,
    pub artist: Vec<String>,
    pub meta: Vec<String>,
}

/// Score information for a post
#[derive(Debug, Deserialize, Clone)]
pub struct PostScore {
    pub up: i32,
    pub down: i32,
    pub total: i32,
}

/// Download job for a post
#[derive(Debug, Clone)]
pub struct DownloadJob {
    pub id: Uuid,
    pub post_id: u32,
    pub url: String,
    pub md5: String,
    pub file_ext: String,
    pub file_size: u32,
    pub tags: Vec<String>,
    pub priority: usize,
    pub is_blacklisted: bool,
}

/// Query plan with estimated size and job count
#[derive(Debug)]
pub struct QueryPlan {
    pub query_id: Uuid,
    pub estimated_post_count: u32,
    pub estimated_size_bytes: u64,
    pub jobs: VecDeque<DownloadJob>,
}

/// Hash store for post deduplication
pub struct HashStore {
    memory_store: HashSet<String>,
    db_connection: Connection,
}

/// Query planner for creating and managing download jobs
pub struct QueryPlanner {
    config_manager: Arc<ConfigManager>,
    client: Client,
    hash_store: Arc<RwLock<HashStore>>,
    job_queue: Arc<QueryQueue>,
}

impl HashStore {
    /// Create a new hash store
    pub fn new(db_path: &Path) -> SqliteResult<Self> {
        // Create the database connection
        let db_connection = Connection::open(db_path)?;

        // Create the table if it doesn't exist
        db_connection.execute(
            "CREATE TABLE IF NOT EXISTS post_hashes (
                md5 TEXT PRIMARY KEY,
                post_id INTEGER NOT NULL,
                downloaded_at TEXT NOT NULL
            )",
            [],
        )?;

        // Create the memory store
        let memory_store = HashSet::new();

        // Load existing hashes into memory
        let mut stmt = db_connection.prepare("SELECT md5 FROM post_hashes")?;
        let hash_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut store = HashSet::new();
        for hash in hash_iter {
            if let Ok(h) = hash {
                store.insert(h);
            }
        }

        Ok(Self {
            memory_store: store,
            db_connection,
        })
    }

    /// Check if a hash exists in the store
    pub fn contains(&self, md5: &str) -> bool {
        self.memory_store.contains(md5)
    }

    /// Add a hash to the store
    pub fn add(&mut self, md5: &str, post_id: u32) -> SqliteResult<()> {
        // Add to memory store
        self.memory_store.insert(md5.to_string());

        // Add to database
        self.db_connection.execute(
            "INSERT OR REPLACE INTO post_hashes (md5, post_id, downloaded_at) VALUES (?1, ?2, datetime('now'))",
            [md5, &post_id.to_string()],
        )?;

        Ok(())
    }
}

impl QueryPlanner {
    /// Create a new query planner
    pub async fn new(config_manager: Arc<ConfigManager>, job_queue: Arc<QueryQueue>) -> QueryPlannerResult<Self> {
        // Get the app config
        let app_config = config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Create the HTTP client
        let client = Client::builder()
            .user_agent("e621_downloader/2.0 (by anonymous)")
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| QueryPlannerError::Request(e))?;

        // Create the hash store
        let db_path = Path::new(&app_config.paths.database_file);

        // Create the parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let hash_store = HashStore::new(db_path)
            .map_err(|e| QueryPlannerError::Database(e))?;

        Ok(Self {
            config_manager,
            client,
            hash_store: Arc::new(RwLock::new(hash_store)),
            job_queue,
        })
    }

    /// Parse input from E621Config and create queries
    pub async fn parse_input(&self) -> QueryPlannerResult<Vec<Query>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        let mut queries = Vec::new();

        // Process tags
        if !e621_config.query.tags.is_empty() {
            let id = Uuid::new_v4();
            let tags = e621_config.query.tags.clone();
            let query = Query {
                id,
                tags,
                priority: 1,
            };
            queries.push(query);
        }

        // Process artists
        for artist in &e621_config.query.artists {
            let id = Uuid::new_v4();
            let tags = vec![format!("artist:{}", artist)];
            let query = Query {
                id,
                tags,
                priority: 2,
            };
            queries.push(query);
        }

        // Process pools
        for pool_id in &e621_config.query.pools {
            let id = Uuid::new_v4();
            let tags = vec![format!("pool:{}", pool_id)];
            let query = Query {
                id,
                tags,
                priority: 3,
            };
            queries.push(query);
        }

        // Process collections
        for collection_id in &e621_config.query.collections {
            let id = Uuid::new_v4();
            let tags = vec![format!("set:{}", collection_id)];
            let query = Query {
                id,
                tags,
                priority: 4,
            };
            queries.push(query);
        }

        // Process specific posts
        for post_id in &e621_config.query.posts {
            let id = Uuid::new_v4();
            let tags = vec![format!("id:{}", post_id)];
            let query = Query {
                id,
                tags,
                priority: 5,
            };
            queries.push(query);
        }

        Ok(queries)
    }

    /// Fetch post count for a query
    pub async fn fetch_post_count(&self, query: &Query) -> QueryPlannerResult<u32> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the tag string
        let tag_string = query.tags.join(" ");

        // Build the URL
        let url = format!("https://e621.net/posts.json?limit=0&tags={}", tag_string);

        // Make the request
        let response = self.client.get(&url)
            .header("User-Agent", "e621_downloader/2.0 (by anonymous)")
            .send()
            .await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return self.fetch_post_count(query).await;
        }

        // Check for other errors
        if !response.status().is_success() {
            return Err(QueryPlannerError::Api(format!(
                "API error: {} - {}",
                response.status(),
                response.text().await?
            )));
        }

        // Parse the response
        let count_response: PostCountResponse = response.json().await?;

        Ok(count_response.counts.posts)
    }

    /// Fetch posts for a query with pagination
    pub async fn fetch_posts(&self, query: &Query, page: u32, limit: u32) -> QueryPlannerResult<Vec<Post>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the tag string
        let tag_string = query.tags.join(" ");

        // Build the URL
        let url = format!(
            "https://e621.net/posts.json?limit={}&page={}&tags={}",
            limit, page, tag_string
        );

        // Make the request
        let response = self.client.get(&url)
            .header("User-Agent", "e621_downloader/2.0 (by anonymous)")
            .send()
            .await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return self.fetch_posts(query, page, limit).await;
        }

        // Check for other errors
        if !response.status().is_success() {
            return Err(QueryPlannerError::Api(format!(
                "API error: {} - {}",
                response.status(),
                response.text().await?
            )));
        }

        // Parse the response
        let posts_response: PostsResponse = response.json().await?;

        Ok(posts_response.posts)
    }

    /// Create a query plan for a query
    pub async fn create_query_plan(&self, query: &Query) -> QueryPlannerResult<QueryPlan> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the rules config
        let rules_config = self.config_manager.get_rules_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Fetch the post count
        let post_count = self.fetch_post_count(query).await?;

        // Calculate the number of pages
        let limit = app_config.limits.posts_per_page as u32;
        let max_pages = app_config.limits.max_page_number as u32;
        let pages = (post_count + limit - 1) / limit;
        let pages = std::cmp::min(pages, max_pages);

        // Create a queue for download jobs
        let mut jobs = VecDeque::new();
        let mut total_size_bytes = 0u64;

        // Fetch posts for each page
        for page in 1..=pages {
            let posts = self.fetch_posts(query, page, limit).await?;

            for post in posts {
                // Skip if the post has no URL
                if post.file.url.is_none() {
                    continue;
                }

                // Skip if the post is too large
                if post.file.size > app_config.limits.file_size_cap as u32 {
                    warn!("Skipping post {} due to size limit ({} bytes)", post.id, post.file.size);
                    continue;
                }

                // Skip if the post is already downloaded
                if self.hash_store.read().contains(&post.file.md5) {
                    debug!("Skipping post {} (already downloaded)", post.id);
                    continue;
                }

                // Check blacklist and whitelist
                let all_tags = self.collect_all_tags(&post);

                // Check if the post is blacklisted
                let is_blacklisted = self.is_blacklisted(&all_tags, &rules_config);
                if is_blacklisted {
                    debug!("Post {} is blacklisted, will be downloaded to blacklisted directory", post.id);
                }

                // Create a download job
                let job = DownloadJob {
                    id: Uuid::new_v4(),
                    post_id: post.id,
                    url: post.file.url.unwrap(),
                    md5: post.file.md5.clone(),
                    file_ext: post.file.ext.clone(),
                    file_size: post.file.size,
                    tags: all_tags,
                    priority: query.priority,
                    is_blacklisted,
                };

                // Add the job to the queue
                jobs.push_back(job);

                // Update the total size
                total_size_bytes += post.file.size as u64;

                // Check if we've exceeded the total size cap
                if total_size_bytes > app_config.limits.total_size_cap as u64 {
                    warn!("Size limit reached ({} bytes)", total_size_bytes);
                    break;
                }
            }

            // Check if we've exceeded the total size cap
            if total_size_bytes > app_config.limits.total_size_cap as u64 {
                break;
            }
        }

        // Create the query plan
        let plan = QueryPlan {
            query_id: query.id,
            estimated_post_count: post_count,
            estimated_size_bytes: total_size_bytes,
            jobs,
        };

        Ok(plan)
    }

    /// Collect all tags from a post
    fn collect_all_tags(&self, post: &Post) -> Vec<String> {
        let mut all_tags = Vec::new();

        all_tags.extend(post.tags.general.clone());
        all_tags.extend(post.tags.species.clone());
        all_tags.extend(post.tags.character.clone());
        all_tags.extend(post.tags.copyright.clone());
        all_tags.extend(post.tags.artist.clone());
        all_tags.extend(post.tags.meta.clone());

        all_tags
    }

    /// Check if a post is blacklisted
    fn is_blacklisted(&self, tags: &[String], rules_config: &RulesConfig) -> bool {
        // Check if any tag is in the whitelist
        for tag in tags {
            if rules_config.whitelist.tags.contains(tag) {
                return false;
            }
        }

        // Check if any tag is in the blacklist
        for tag in tags {
            if rules_config.blacklist.tags.contains(tag) {
                return true;
            }
        }

        false
    }

    /// Queue jobs from a query plan
    pub async fn queue_jobs(&self, plan: &QueryPlan) -> QueryPlannerResult<()> {
        info!("Queuing {} jobs for query {}", plan.jobs.len(), plan.query_id);

        for job in &plan.jobs {
            // Create a query for the job
            let query = Query {
                id: job.id,
                tags: job.tags.clone(),
                priority: job.priority,
            };

            // Add the query to the queue
            self.job_queue.enqueue(query).await
                .map_err(|e| QueryPlannerError::Config(e.to_string()))?;
        }

        Ok(())
    }

    /// Process a query and create jobs
    pub async fn process_query(&self, query: &Query) -> QueryPlannerResult<QueryPlan> {
        // Create a query plan
        let plan = self.create_query_plan(query).await?;

        // Queue the jobs
        self.queue_jobs(&plan).await?;

        Ok(plan)
    }
}

/// Create a new query planner
pub async fn init_query_planner(
    config_manager: Arc<ConfigManager>,
    job_queue: Arc<QueryQueue>,
) -> QueryPlannerResult<Arc<QueryPlanner>> {
    let planner = QueryPlanner::new(config_manager, job_queue).await?;
    Ok(Arc::new(planner))
}
