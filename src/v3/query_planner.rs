//! Query Planning and Job Creation for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Parsing input (tags, artists, etc.)
//! 2. Fetching post count via e621 API (handling pagination and filters)
//! 3. Estimating disk usage and applying download caps
//! 4. Queuing tasks into a VecDeque or channel
//! 5. Deduplicating posts using in-memory and persistent hash store

use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use rusqlite::{Connection, Result as SqliteResult};
use serde::Deserialize;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::v3::{
    RulesConfig,
    ConfigManager,
    Query, QueryQueue,
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

/// E621 API response for tags
#[derive(Debug, Deserialize)]
pub struct TagsResponse {
    pub tags: Vec<TagInfo>,
}

/// Tag information from E621 API
#[derive(Debug, Deserialize, Clone)]
pub struct TagInfo {
    pub id: u32,
    pub name: String,
    pub category: u32,
    pub post_count: u32,
}

/// E621 API response for artists
#[derive(Debug, Deserialize)]
pub struct ArtistsResponse {
    pub artists: Vec<ArtistInfo>,
}

/// Artist information from E621 API
#[derive(Debug, Deserialize, Clone)]
pub struct ArtistInfo {
    pub id: u32,
    pub name: String,
    pub is_active: bool,
    pub other_names: Vec<String>,
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

        // Load existing hashes into memory
        let mut store: HashSet<String> = HashSet::new();
        
        {
            let mut stmt = db_connection.prepare("SELECT md5 FROM post_hashes")?;
            let hash_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

            for hash in hash_iter {
                if let Ok(h) = hash {
                    store.insert(h);
                }
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

    /// Estimate post count for a query by fetching a small sample
    /// E621 API doesn't provide direct count, so we estimate based on pagination
    pub async fn estimate_post_count(&self, query: &Query) -> QueryPlannerResult<u32> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Start with a small sample to check if there are any results
        let sample_posts = self.fetch_posts(query, 1, 1).await?;
        
        if sample_posts.is_empty() {
            return Ok(0);
        }

        // Try to estimate by fetching with max limit and checking pagination
        let _limit = app_config.limits.posts_per_page as u32;
        let _max_pages = app_config.limits.max_page_number as u32;
        
        // For now, return a reasonable estimate based on the fact that we have at least one post
        // We'll let the actual query plan creation handle the pagination properly
        info!("Query '{}' has results, will determine exact count during plan creation", 
            query.tags.join(" "));
        
        // Return a high estimate so the query gets processed
        Ok(1000)
    }

    /// Fetch posts for a query with pagination
    pub async fn fetch_posts(&self, query: &Query, page: u32, limit: u32) -> QueryPlannerResult<Vec<Post>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the tag string and URL encode it
        let tag_string = query.tags.join(" ");
        let encoded_tags = urlencoding::encode(&tag_string);

        // Build the URL
        let url = format!(
            "https://e621.net/posts.json?limit={}&page={}&tags={}",
            limit, page, encoded_tags
        );
        
        debug!("API request URL: {}", url);

        // Create a request builder
        let mut request_builder = self.client.get(&url)
            .header("User-Agent", format!("e621_downloader/{} (by {})", 
                env!("CARGO_PKG_VERSION"), 
                e621_config.auth.username));

        // Add authentication if credentials are provided
        if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            use base64::{Engine as _, engine::general_purpose};
            let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}", 
                e621_config.auth.username, 
                e621_config.auth.api_key)));
            request_builder = request_builder.header("Authorization", auth);
            debug!("Using authentication for API request");
        } else {
            debug!("No authentication credentials provided, making unauthenticated request");
        }

        // Make the request
        let response = request_builder.send().await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            // Use Box::pin to avoid infinitely sized future
            return Box::pin(self.fetch_posts(query, page, limit)).await;
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

        // Create a queue for download jobs
        let mut jobs = VecDeque::new();
        let mut total_size_bytes = 0u64;
        let mut actual_post_count = 0u32;

        // Calculate pagination parameters
        let limit = app_config.limits.posts_per_page as u32;
        let max_pages = app_config.limits.max_page_number as u32;

        info!("Creating query plan for: {}", query.tags.join(" "));

        // Fetch posts page by page until we run out or hit limits
        for page in 1..=max_pages {
            let posts = self.fetch_posts(query, page, limit).await?;
            
            // If no posts returned, we've reached the end
            if posts.is_empty() {
                info!("No more posts found at page {}", page);
                break;
            }

            info!("Processing page {} with {} posts", page, posts.len());
            
            let posts_len = posts.len() as u32;
            
            for post in posts {
                actual_post_count += 1;
                
                // Skip if the post has no URL
                if post.file.url.is_none() {
                    debug!("Skipping post {} (no URL)", post.id);
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

            // If we got fewer posts than the limit, we've reached the end
            if posts_len < limit {
                info!("Reached end of results at page {}", page);
                break;
            }

            // Check if we've exceeded the total size cap
            if total_size_bytes > app_config.limits.total_size_cap as u64 {
                break;
            }
        }

        info!("Query plan created: {} posts found, {} jobs queued, {} bytes total", 
            actual_post_count, jobs.len(), total_size_bytes);

        // Create the query plan
        let plan = QueryPlan {
            query_id: query.id,
            estimated_post_count: actual_post_count,
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

    /// Search for tags by name or pattern
    pub async fn search_tags(&self, search_term: &str, limit: u32) -> QueryPlannerResult<Vec<TagInfo>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the search URL
        let encoded_search = urlencoding::encode(search_term);
        let url = format!(
            "https://e621.net/tags.json?search[name_matches]={}*&limit={}",
            encoded_search, limit
        );
        
        debug!("Tag search URL: {}", url);

        // Create a request builder
        let mut request_builder = self.client.get(&url)
            .header("User-Agent", format!("e621_downloader/{} (by {})", 
                env!("CARGO_PKG_VERSION"), 
                e621_config.auth.username));

        // Add authentication if credentials are provided
        if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            use base64::{Engine as _, engine::general_purpose};
            let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}", 
                e621_config.auth.username, 
                e621_config.auth.api_key)));
            request_builder = request_builder.header("Authorization", auth);
            debug!("Using authentication for tag search");
        }

        // Make the request
        let response = request_builder.send().await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return Box::pin(self.search_tags(search_term, limit)).await;
        }

        // Check for other errors
        if !response.status().is_success() {
            return Err(QueryPlannerError::Api(format!(
                "Tag search API error: {} - {}",
                response.status(),
                response.text().await?
            )));
        }

        // Parse the response - E621 returns inconsistent formats
        // When there are results: returns direct array
        // When no results: returns { "tags": [] }
        let response_text = response.text().await?;
        
        // Try to parse as direct array first
        match serde_json::from_str::<Vec<TagInfo>>(&response_text) {
            Ok(tags) => Ok(tags),
            Err(_) => {
                // If that fails, try to parse as object with tags field
                match serde_json::from_str::<TagsResponse>(&response_text) {
                    Ok(tags_response) => Ok(tags_response.tags),
                    Err(e) => {
                        debug!("Failed to parse tag search response: {}", response_text);
                        Err(QueryPlannerError::Serialization(e))
                    }
                }
            }
        }
    }

    /// Search for artists by name or pattern
    pub async fn search_artists(&self, search_term: &str, limit: u32) -> QueryPlannerResult<Vec<ArtistInfo>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the search URL
        let encoded_search = urlencoding::encode(search_term);
        let url = format!(
            "https://e621.net/artists.json?search[name]={}*&limit={}",
            encoded_search, limit
        );
        
        debug!("Artist search URL: {}", url);

        // Create a request builder
        let mut request_builder = self.client.get(&url)
            .header("User-Agent", format!("e621_downloader/{} (by {})", 
                env!("CARGO_PKG_VERSION"), 
                e621_config.auth.username));

        // Add authentication if credentials are provided
        if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            use base64::{Engine as _, engine::general_purpose};
            let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}", 
                e621_config.auth.username, 
                e621_config.auth.api_key)));
            request_builder = request_builder.header("Authorization", auth);
            debug!("Using authentication for artist search");
        }

        // Make the request
        let response = request_builder.send().await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return Box::pin(self.search_artists(search_term, limit)).await;
        }

        // Check for other errors
        if !response.status().is_success() {
            return Err(QueryPlannerError::Api(format!(
                "Artist search API error: {} - {}",
                response.status(),
                response.text().await?
            )));
        }

        // Parse the response - Handle potential inconsistencies like tags API
        let response_text = response.text().await?;
        
        // Try to parse as direct array first (in case it's similar to tags)
        match serde_json::from_str::<Vec<ArtistInfo>>(&response_text) {
            Ok(artists) => Ok(artists),
            Err(_) => {
                // If that fails, try to parse as object with artists field
                match serde_json::from_str::<ArtistsResponse>(&response_text) {
                    Ok(artists_response) => Ok(artists_response.artists),
                    Err(e) => {
                        debug!("Failed to parse artist search response: {}", response_text);
                        Err(QueryPlannerError::Serialization(e))
                    }
                }
            }
        }
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
