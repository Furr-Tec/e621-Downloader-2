//! Query Planning and Job Creation for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Parsing input (tags, artists, etc.)
//! 2. Fetching post count via e621 API (handling pagination and filters)
//! 3. Estimating disk usage and applying download caps
//! 4. Queuing tasks into a VecDeque or channel
//! 5. Deduplicating posts using in-memory and persistent hash store

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::Deserialize;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::v3::{
    ConfigManager,
    Query, QueryQueue, HashManager,
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

/// E621 API response for user profile
#[derive(Debug, Deserialize)]
pub struct UserResponse {
    pub id: u32,
    pub name: String,
    pub level: u32,
    pub base_upload_limit: u32,
    pub post_upload_count: u32,
    pub post_update_count: u32,
    pub note_update_count: u32,
    pub is_banned: bool,
    pub can_approve_posts: bool,
    pub can_upload_free: bool,
    pub level_string: String,
    pub avatar_id: Option<u32>,
    pub blacklisted_tags: Option<String>,
    pub favorite_tags: Option<String>,
    pub created_at: String,
    pub profile_about: Option<String>,
    pub profile_artinfo: Option<String>,
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
    /// All tags collected from the API for this post (including artist:...)
    pub tags: Vec<String>,
    /// Artist names reported by the API for this post
    pub artists: Vec<String>,
    /// The original search query that produced this job (used for folder routing)
    pub source_query: Vec<String>,
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


/// Query planner for creating and managing download jobs
pub struct QueryPlanner {
    config_manager: Arc<ConfigManager>,
    client: Client,
    hash_manager: Arc<HashManager>,
    job_queue: Arc<QueryQueue>,
}


impl QueryPlanner {
    /// Create a new query planner
    pub async fn new(config_manager: Arc<ConfigManager>, job_queue: Arc<QueryQueue>, hash_manager: Arc<HashManager>) -> QueryPlannerResult<Self> {
        // Get the e621 config to build proper user agent
        let e621_config = config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Create the HTTP client with proper user agent
        let user_agent = format!("e621_downloader/{} (by {})", 
            env!("CARGO_PKG_VERSION"), 
            e621_config.auth.username);
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| QueryPlannerError::Request(e))?;

        Ok(Self {
            config_manager,
            client,
            hash_manager,
            job_queue,
        })
    }

    /// Fetch user profile and blacklisted tags from E621 API
    pub async fn fetch_user_blacklist(&self, username: &str) -> QueryPlannerResult<Vec<String>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the user profile URL
        let url = format!("https://e621.net/users/{}.json", username);
        
        debug!("Fetching user profile URL: {}", url);

        // Create a request builder with authentication (required for user profiles)
        let mut request_builder = self.client.get(&url)
            .header("User-Agent", format!("e621_downloader/{} (by {})", 
                env!("CARGO_PKG_VERSION"), 
                e621_config.auth.username));

        // Add authentication - required for user profile access
        if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            use base64::{Engine as _, engine::general_purpose};
            let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}", 
                e621_config.auth.username, 
                e621_config.auth.api_key)));
            request_builder = request_builder.header("Authorization", auth);
            debug!("Using authentication for user profile request");
        } else {
            warn!("No authentication credentials provided, user blacklist fetch may fail");
            return Ok(Vec::new()); // Return empty blacklist if no auth
        }

        // Make the request
        let response = request_builder.send().await?;

        // Check for rate limiting
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            // Wait and retry
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return Box::pin(self.fetch_user_blacklist(username)).await;
        }

        // Check for other errors
        if !response.status().is_success() {
            warn!("Failed to fetch user profile: {} - using local blacklist only", response.status());
            return Ok(Vec::new()); // Return empty blacklist on error, use local rules
        }

        // Parse the response
        let user_profile: UserResponse = response.json().await?;
        
        // Parse blacklisted tags from the profile
        let blacklisted_tags = if let Some(blacklist_str) = user_profile.blacklisted_tags {
            blacklist_str
                .split_whitespace()
                .map(|tag| tag.trim().to_string())
                .filter(|tag| !tag.is_empty())
                .collect()
        } else {
            Vec::new()
        };
        
        info!("Fetched {} blacklisted tags from E621 API for user: {}", blacklisted_tags.len(), username);
        debug!("Blacklisted tags from API: {:?}", blacklisted_tags);
        
        Ok(blacklisted_tags)
    }

    /// Parse input from E621Config and create queries
    pub async fn parse_input(&self) -> QueryPlannerResult<Vec<Query>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        let mut queries = Vec::new();

        // Process tags (each tag as its own query to avoid zero-result combinations and to mirror v2 behavior)
        if !e621_config.query.tags.is_empty() {
            for tag in &e621_config.query.tags {
                let id = Uuid::new_v4();
                let tags = vec![tag.clone()];
                let query = Query {
                    id,
                    tags,
                    priority: 1,
                };
                queries.push(query);
            }
        }

        // Process artists
        for artist in &e621_config.query.artists {
            let id = Uuid::new_v4();
            let tags = vec![artist.clone()]; // Use artist name directly, not with artist: prefix
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

        // Process favorites if enabled
        if e621_config.options.download_favorites {
            // Check if we have valid credentials for favorites
            if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
                && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
                let id = Uuid::new_v4();
                let tags = vec![format!("fav:{}", e621_config.auth.username)];
                let query = Query {
                    id,
                    tags,
                    priority: 1, // High priority for favorites
                };
                queries.push(query);
                info!("Added favorites query for user: {}", e621_config.auth.username);
            } else {
                warn!("Download favorites is enabled but no valid credentials found. Skipping favorites.");
            }
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
    /// Fetch posts for a query with numeric page
    pub async fn fetch_posts(&self, query: &Query, page: u32, limit: u32) -> QueryPlannerResult<Vec<Post>> {
        self.fetch_posts_with_page_param(query, Some(&page.to_string()), limit).await
    }

    /// Fetch posts for a query using an optional page parameter (supports cursor pagination like b<id>)
    async fn fetch_posts_with_page_param(&self, query: &Query, page_param: Option<&str>, limit: u32) -> QueryPlannerResult<Vec<Post>> {
        // Get the e621 config
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build the tag string and URL encode it (do not inject order: to match v2 behavior exactly)
        let tag_string = query.tags.join(" ");
        let encoded_tags = urlencoding::encode(&tag_string);

        // Build the URL (note: page can be numeric or cursor like b<id>)
        let mut url = format!(
            "https://e621.net/posts.json?limit={}&tags={}",
            limit, encoded_tags
        );
        if let Some(p) = page_param {
            url.push_str(&format!("&page={}", p));
        }
        
        info!("API request URL: {}", url);

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
            return Box::pin(self.fetch_posts_with_page_param(query, page_param, limit)).await;
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

        // Get the e621 config to fetch user blacklist
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Fetch user's blacklisted tags from E621 API
        let api_blacklisted_tags = if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty() 
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            self.fetch_user_blacklist(&e621_config.auth.username).await.unwrap_or_else(|e| {
                warn!("Failed to fetch user blacklist from API: {}", e);
                Vec::new()
            })
        } else {
            info!("No valid credentials provided, proceeding without blacklist filtering");
            Vec::new()
        };

        // Create a queue for download jobs
        let mut jobs = VecDeque::new();
        let mut total_size_bytes = 0u64;
        let mut actual_post_count = 0u32;

        // Pagination parameters
        let limit = app_config.limits.posts_per_page.min(320) as u32;
        let max_pages = app_config.limits.max_page_number as u32; // safety cap
        let reqs_per_sec = app_config.rate.requests_per_second.max(1) as u64;
        let sleep_ms = (1000 / reqs_per_sec).max(100);

        info!("Creating query plan for: {}", query.tags.join(" "));

        // Match v2 behavior: iterate by numeric pages (1..=max_pages)
        // This avoids edge-cases where cursor pagination under-fetches for some queries.
        for page_number in 1..=max_pages {
            let posts = self.fetch_posts(query, page_number, limit).await?;

            if posts.is_empty() {
                info!("No more posts found (page {} returned 0)", page_number);
                break;
            }

            info!("Processing numeric page {} with {} posts", page_number, posts.len());

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

                // Note: We've moved duplicate checking to post-processing phase to match v2 behavior
                // This ensures we don't miss posts due to stale database records without corresponding files

                // Check blacklist using API tags
                let all_tags = self.collect_all_tags(&post);
                let artists = post.tags.artist.clone();

                // Check if the post is blacklisted using API blacklist
                let is_blacklisted = self.is_blacklisted_by_api(&all_tags, &api_blacklisted_tags);
                if is_blacklisted {
                    debug!("Skipping post {} (blacklisted by user's E621 blacklist)", post.id);
                    continue;
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
                    artists,
                    source_query: query.tags.clone(),
                    priority: query.priority,
                    is_blacklisted: false,
                };

                // Add the job to the queue
                jobs.push_back(job);

                // Update the total size
                total_size_bytes += post.file.size as u64;

            // Check if we've exceeded the total size cap (0 => unlimited)
            if app_config.limits.total_size_cap > 0 && total_size_bytes > app_config.limits.total_size_cap as u64 {
                warn!("Size limit reached ({} bytes). To fetch all posts, increase total_size_cap in config.toml or set it to 0 for unlimited.", total_size_bytes);
                break;
            }
            }

            // pacing between requests
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

            // Size cap check (0 => unlimited)
            if app_config.limits.total_size_cap > 0 && total_size_bytes > app_config.limits.total_size_cap as u64 {
                break;
            }
        }
        
        // Post-processing phase: Filter out duplicates using verified file existence checking
        // This matches v2's approach of checking duplicates after fetching all posts
        info!("Post-processing: Filtering {} jobs for actual duplicates using file verification", jobs.len());
        let mut filtered_jobs = VecDeque::new();
        let mut verified_duplicates = 0;
        let mut verification_failures = 0;
        
        for job in jobs {
            // Use the new verified checking method that actually checks file existence
            let is_actually_downloaded = match self.hash_manager.contains_either_verified(&job.md5, job.post_id).await {
                true => {
                    debug!("Post {} confirmed as already downloaded (verified file exists)", job.post_id);
                    true
                }
                false => {
                    debug!("Post {} confirmed as new (no existing file found)", job.post_id);
                    false
                }
            };
            
            if is_actually_downloaded {
                verified_duplicates += 1;
            } else {
                filtered_jobs.push_back(job);
            }
        }
        
        if verified_duplicates > 0 {
            info!("Post-processing: Filtered out {} verified duplicates, {} new downloads remain", 
                  verified_duplicates, filtered_jobs.len());
        }
        
        // Update the jobs collection
        let original_job_count = actual_post_count; // Use the total posts found instead
        jobs = filtered_jobs;
        
        // Recalculate total size based on remaining jobs
        total_size_bytes = jobs.iter().map(|job| job.file_size as u64).sum();

        info!("Query plan created: {} posts found, {} jobs queued ({} filtered as verified duplicates), {} bytes total", 
            actual_post_count, jobs.len(), verified_duplicates, total_size_bytes);

        // Create the query plan
        let plan = QueryPlan {
            query_id: query.id,
            estimated_post_count: actual_post_count,
            estimated_size_bytes: total_size_bytes,
            jobs,
        };

        Ok(plan)
    }

    /// Query tag metadata to get API-reported post_count for a tag/artist
    async fn fetch_tag_post_count(&self, name: &str) -> QueryPlannerResult<u32> {
        // Get configs for UA and backoff
        let e621_config = self.config_manager.get_e621_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| QueryPlannerError::Config(e.to_string()))?;

        // Build search URL (exact name match preferred). Use name_matches for robustness
        let encoded = urlencoding::encode(name);
        let url = format!(
            "https://e621.net/tags.json?limit=20&search[name_matches]={}&search[order]=count",
            encoded
        );
        debug!("Tag count URL: {}", url);

        let mut request_builder = self.client.get(&url)
            .header("User-Agent", format!("e621_downloader/{} (by {})",
                env!("CARGO_PKG_VERSION"),
                e621_config.auth.username));

        if !e621_config.auth.username.is_empty() && !e621_config.auth.api_key.is_empty()
            && e621_config.auth.username != "your_username" && e621_config.auth.api_key != "your_api_key_here" {
            use base64::{Engine as _, engine::general_purpose};
            let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}",
                e621_config.auth.username,
                e621_config.auth.api_key)));
            request_builder = request_builder.header("Authorization", auth);
        }

        let response = request_builder.send().await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            sleep(Duration::from_secs(app_config.rate.retry_backoff_secs as u64)).await;
            return Box::pin(self.fetch_tag_post_count(name)).await;
        }
        if !response.status().is_success() {
            return Err(QueryPlannerError::Api(format!(
                "Tag count API error: {} - {}",
                response.status(),
                response.text().await?
            )));
        }

        // Parse either direct array or { tags: [...] }
        let text = response.text().await?;
        if let Ok(array) = serde_json::from_str::<Vec<TagInfo>>(&text) {
            // Find exact name match first; otherwise pick the first entry
            if let Some(t) = array.iter().find(|t| t.name.eq_ignore_ascii_case(name)) {
                return Ok(t.post_count);
            }
            return Ok(array.first().map(|t| t.post_count).unwrap_or(0));
        }
        if let Ok(obj) = serde_json::from_str::<TagsResponse>(&text) {
            if let Some(t) = obj.tags.iter().find(|t| t.name.eq_ignore_ascii_case(name)) {
                return Ok(t.post_count);
            }
            return Ok(obj.tags.first().map(|t| t.post_count).unwrap_or(0));
        }
        // Fallback: no parse
        Ok(0)
    }

    /// Collect all tags from a post
    fn collect_all_tags(&self, post: &Post) -> Vec<String> {
        let mut all_tags = Vec::new();

        all_tags.extend(post.tags.general.clone());
        all_tags.extend(post.tags.species.clone());
        all_tags.extend(post.tags.character.clone());
        all_tags.extend(post.tags.copyright.clone());
        
        // Add artist tags with "artist:" prefix
        for artist in &post.tags.artist {
            all_tags.push(format!("artist:{}", artist));
        }
        
        all_tags.extend(post.tags.meta.clone());

        all_tags
    }

    /// Check if a post is blacklisted using E621 API blacklist
    fn is_blacklisted_by_api(&self, tags: &[String], api_blacklisted_tags: &[String]) -> bool {
        debug!("Checking API blacklist for tags: {:?}", tags);
        debug!("API blacklist contains: {:?}", api_blacklisted_tags);
        
        // Check if any tag matches the user's blacklist from E621
        for tag in tags {
            if api_blacklisted_tags.contains(tag) {
                debug!("Post blacklisted by API tag: {}", tag);
                return true;
            }
        }

        debug!("Post not blacklisted by API");
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
    hash_manager: Arc<HashManager>,
) -> QueryPlannerResult<Arc<QueryPlanner>> {
    let planner = QueryPlanner::new(config_manager, job_queue, hash_manager).await?;
    Ok(Arc::new(planner))
}
