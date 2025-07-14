use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::mpsc;

use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, trace, warn};
use console;

use crate::e621::blacklist::Blacklist;
use crate::e621::io::tag::{Group, Tag, TagSearchType, TagType};
use crate::e621::io::{emergency_exit, Config, Login};
use crate::e621::sender::entries::{PoolEntry, PostEntry, SetEntry};
use crate::e621::sender::RequestSender;
use crate::e621::whitelist_cache::WhitelistCache;
use crate::e621::rate_limiter::AdaptiveRateLimiter;

/// Post processing pipeline structures for parallel post processing

/// Represents a post processing job in the pipeline
#[derive(Debug, Clone)]
struct PostProcessingJob {
    posts_batch: Vec<PostEntry>,  // Process posts in batches instead of individually
    page_numbers: Vec<u16>,
    thread_id: usize,
    search_tag: String,
}

/// Represents a processed post result from the pipeline
#[derive(Debug, Clone)]
struct ProcessedPostBatch {
    posts: Vec<PostEntry>,  // Return processed batches
    page_numbers: Vec<u16>,
    thread_id: usize,
    filtered_count: usize,
    invalid_count: usize,
    processing_duration: Duration,
}

/// Configuration for the post processing pipeline
#[derive(Debug, Clone)]
struct PostProcessingConfig {
    total_cores: usize,
    processing_threads: usize,
    queue_size: usize,
    use_pipeline: bool, // Switch between pipeline and in-thread processing
}

/// Represents a planned download target with pre-calculated metadata
#[derive(Debug, Clone)]
pub(crate) struct DownloadTarget {
    pub(crate) name: String,
    pub(crate) target_type: TargetType,
    pub(crate) estimated_post_count: Option<usize>,
    pub(crate) search_type: TagSearchType,
    pub(crate) category: String,
}

/// Type of download target
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TargetType {
    Tag,
    Artist,
    Pool,
    Set,
    Post,
    Favorites,
}

/// Orchestration plan for the entire download session
#[derive(Debug)]
pub(crate) struct DownloadPlan {
    pub(crate) targets: Vec<DownloadTarget>,
    pub(crate) total_estimated_posts: usize,
    pub(crate) estimated_duration: Option<Duration>,
}

/// Result of processing a single download target
#[derive(Debug)]
pub(crate) struct TargetResult {
    pub(crate) target: DownloadTarget,
    pub(crate) collection: Option<PostCollection>,
    pub(crate) posts_found: usize,
    pub(crate) processing_time: Duration,
    pub(crate) success: bool,
    pub(crate) error_message: Option<String>,
}

impl TargetResult {
    fn new(target: DownloadTarget) -> Self {
        Self {
            target,
            collection: None,
            posts_found: 0,
            processing_time: Duration::new(0, 0),
            success: false,
            error_message: None,
        }
    }

    fn from_collection(target: DownloadTarget, collection: PostCollection) -> Self {
        let posts_found = collection.posts.len();
        Self {
            target,
            collection: Some(collection),
            posts_found,
            processing_time: Duration::new(0, 0),
            success: true,
            error_message: None,
        }
    }
}

impl PostProcessingConfig {
    /// Creates a new post processing configuration
    fn new(post_count: usize, thread_count: usize) -> Self {
        let total_cores = num_cpus::get();
        // Use pipeline for large batches, in-thread for small ones
        let use_pipeline = post_count > 100 && thread_count > 2;
        let processing_threads = if use_pipeline {
            std::cmp::max(2, total_cores / 4) // Use quarter of cores for post processing
        } else {
            thread_count // Use existing search threads
        };
        let queue_size = processing_threads * 8; // 8 jobs per thread
        
        PostProcessingConfig {
            total_cores,
            processing_threads,
            queue_size,
            use_pipeline,
        }
    }
}

/// A trait for implementing a conversion function for turning a type into a [Vec] of the same type
pub(crate) trait NewVec<T> {
    fn new_vec(value: T) -> Vec<Self> where Self: Sized;
}

/// A grabbed post that contains all information needed to download a post.
#[derive(Clone, Debug)]
pub(crate) struct GrabbedPost {
    url: String,
    name: String,
    file_size: i64,
    save_directory: Option<PathBuf>,
    artist: Option<String>,
    is_new: bool,
    /// SHA-512 hash of the file contents, used for verification
    /// This is populated after the file is downloaded
    sha512_hash: Option<String>,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: i64,
}

impl GrabbedPost {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the file size in kilobytes
    /// The size is stored in bytes in the struct but converted to KB when accessed
    pub(crate) fn file_size(&self) -> i64 {
        self.file_size / 1024 // Convert bytes to KB
    }
    
    /// Returns the raw file size in bytes (as received from the API)
    pub(crate) fn file_size_bytes(&self) -> i64 {
        self.file_size
    }

    pub(crate) fn save_directory(&self) -> Option<&PathBuf> {
        self.save_directory.as_ref()
    }

    pub(crate) fn artist(&self) -> Option<&str> {
        self.artist.as_deref()
    }

    pub(crate) fn set_save_directory(&mut self, dir: PathBuf) {
        self.save_directory = Some(dir);
    }

    pub(crate) fn set_artist(&mut self, artist: String) {
        self.artist = Some(artist);
    }

    pub(crate) fn is_new(&self) -> bool {
        self.is_new
    }

    pub(crate) fn set_is_new(&mut self, is_new: bool) {
        self.is_new = is_new;
    }

    /// Gets the SHA-512 hash of the file if available
    pub(crate) fn sha512_hash(&self) -> Option<&str> {
        self.sha512_hash.as_deref()
    }

    /// Sets the SHA-512 hash of the file
    pub(crate) fn set_sha512_hash(&mut self, hash: String) {
        self.sha512_hash = Some(hash);
    }

    /// Checks if the post has a SHA-512 hash
    pub(crate) fn has_hash(&self) -> bool {
        self.sha512_hash.is_some()
    }
    
    /// Gets the short URL for the post
    pub(crate) fn short_url(&self) -> Option<&str> {
        self.short_url.as_deref()
    }
    
    /// Sets the short URL for the post
    pub(crate) fn set_short_url(&mut self, short_url: String) {
        self.short_url = Some(short_url);
    }
    
    /// Gets the post ID
    pub(crate) fn post_id(&self) -> i64 {
        self.post_id
    }
    
    /// Sets the post ID
    pub(crate) fn set_post_id(&mut self, post_id: i64) {
        self.post_id = post_id;
    }
    
    /// Generates an enhanced filename with artist name and post ID
    pub(crate) fn generate_enhanced_filename(post: &PostEntry, _name_convention: &str, _request_sender: &RequestSender) -> String {
        // If no artists, use post ID only
        if post.tags.artist.is_empty() {
            return format!("{}.{}", post.id, post.file.ext);
        }
        
        // Collect and sanitize artist names
        let mut artist_names = Vec::new();
        for artist_name in &post.tags.artist {
            // Sanitize artist name for filename (replace invalid characters)
            let sanitized_name = artist_name
                .replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_")
                .replace(' ', "_");
            artist_names.push(sanitized_name);
        }
        
        // Combine artists with "+" separator if multiple
        let artist_string = artist_names.join("+");
        
        // Final format: [ArtistName(s)]_[PostID].ext
        format!("{}_{}.{}", artist_string, post.id, post.file.ext)
    }
}

impl NewVec<Vec<PostEntry>> for GrabbedPost {
    fn new_vec(vec: Vec<PostEntry>) -> Vec<Self> {
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        let naming_convention = binding.naming_convention();
        
        // Batch check all post IDs at once for O(1) performance
        let post_ids: Vec<i64> = vec.iter().map(|e| e.id).collect();
        let duplicate_results = dir_manager.batch_has_post_ids(&post_ids);
        
        // Create a lookup map for O(1) duplicate checking
        let duplicate_map: std::collections::HashMap<i64, bool> = duplicate_results.into_iter().collect();
        
        vec.into_iter()
            .map(|e| {
                let mut post = GrabbedPost::from((e.clone(), naming_convention));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Use the batch-checked result
                let is_duplicate = duplicate_map.get(&e.id).copied().unwrap_or(false);
                post.set_is_new(!is_duplicate);
                post
            })
            .collect()
    }
}

impl GrabbedPost {
    /// Enhanced new_vec that includes artist ID information in filenames
    pub(crate) fn new_vec_with_artist_ids(vec: Vec<PostEntry>, request_sender: &RequestSender) -> Vec<Self> {
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        
        // Batch check all post IDs at once for O(1) performance
        let post_ids: Vec<i64> = vec.iter().map(|e| e.id).collect();
        let duplicate_results = dir_manager.batch_has_post_ids(&post_ids);
        
        // Create a lookup map for O(1) duplicate checking
        let duplicate_map: std::collections::HashMap<i64, bool> = duplicate_results.into_iter().collect();
        
        // Batch statistics for efficient logging
        let mut new_posts_count = 0;
        let mut duplicate_posts_count = 0;
        
        let result: Vec<Self> = vec.into_iter()
            .map(|e| {
                // Generate enhanced filename with artist names and IDs
                let enhanced_name = GrabbedPost::generate_enhanced_filename(&e, Config::get().naming_convention(), request_sender);
                
                let short_url = format!("e621.net/posts/{}", e.id);
                let mut post = GrabbedPost {
                    url: e.file.url.clone().unwrap(),
                    name: enhanced_name,
                    file_size: e.file.size,
                    save_directory: None,
                    artist: None,
                    is_new: true,
                    sha512_hash: None,
                    short_url: Some(short_url),
                    post_id: e.id,
                };
                
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                
                // Use the batch-checked result
                let is_duplicate = duplicate_map.get(&e.id).copied().unwrap_or(false);
                if is_duplicate {
                    duplicate_posts_count += 1;
                } else {
                    new_posts_count += 1;
                }
                post.set_is_new(!is_duplicate);
                post
            })
            .collect();
        
        // Log batch statistics instead of per-post traces
        if log::log_enabled!(log::Level::Trace) {
            trace!("Batch processed {} posts: {} new, {} duplicates", 
                   result.len(), new_posts_count, duplicate_posts_count);
        }
        
        result
    }
}

impl NewVec<(Vec<PostEntry>, &str)> for GrabbedPost {
    fn new_vec((vec, pool_name): (Vec<PostEntry>, &str)) -> Vec<Self> {
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        
        // Batch check all post IDs at once for O(1) performance
        let post_ids: Vec<i64> = vec.iter().map(|e| e.id).collect();
        let duplicate_results = dir_manager.batch_has_post_ids(&post_ids);
        
        // Create a lookup map for O(1) duplicate checking
        let duplicate_map: std::collections::HashMap<i64, bool> = duplicate_results.into_iter().collect();
        
        vec.iter()
            .enumerate()
            .map(|(i, e)| {
                let mut post = GrabbedPost::from((e, pool_name, (i + 1) as u16));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Use the batch-checked result
                let is_duplicate = duplicate_map.get(&e.id).copied().unwrap_or(false);
                post.set_is_new(!is_duplicate);
                post
            })
            .collect()
    }
}

impl From<(&PostEntry, &str, u16)> for GrabbedPost {
    fn from((post, name, current_page): (&PostEntry, &str, u16)) -> Self {
        let short_url = format!("e621.net/posts/{}", post.id);
        GrabbedPost {
            url: post.file.url.clone().unwrap(),
            name: format!("{} Page_{:05}.{}", name, current_page, post.file.ext),
            file_size: post.file.size, // post.file.size is already i64
            save_directory: None,
            artist: None,
            is_new: true,
            sha512_hash: None,
            short_url: Some(short_url),
            post_id: post.id,
        }
    }
}

impl From<(PostEntry, &str)> for GrabbedPost {
    fn from((post, name_convention): (PostEntry, &str)) -> Self {
        let short_url = format!("e621.net/posts/{}", post.id);
        match name_convention {
            "md5" => GrabbedPost {
                url: post.file.url.clone().unwrap(),
                name: format!("{}.{}", post.file.md5, post.file.ext),
                file_size: post.file.size, // post.file.size is already i64
                save_directory: None,
                artist: None,
                is_new: true,
                sha512_hash: None,
                short_url: Some(short_url),
                post_id: post.id,
            },
            "id" => GrabbedPost {
                url: post.file.url.clone().unwrap(),
                name: format!("{}.{}", post.id, post.file.ext),
                file_size: post.file.size, // post.file.size is already i64
                save_directory: None,
                artist: None,
                is_new: true,
                sha512_hash: None,
                short_url: Some(short_url.clone()),
                post_id: post.id,
            },
            _ => {
                // This will terminate the program, so no need for unreachable code after it
                emergency_exit("Incorrect naming convention!");
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PostCollection {
    name: String,
    category: String,
    posts: Vec<GrabbedPost>,
    base_directory: Option<PathBuf>,
}

impl PostCollection {
    pub(crate) fn new(name: &str, category: &str, posts: Vec<GrabbedPost>) -> Self {
        PostCollection {
            name: name.to_string(),
            category: category.to_string(),
            posts,
            base_directory: None,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn category(&self) -> &str {
        &self.category
    }

    pub(crate) fn posts(&self) -> &Vec<GrabbedPost> {
        &self.posts
    }

    pub(crate) fn initialize_directories(&mut self) -> anyhow::Result<()> {
        let directory_start = Instant::now();
        info!("Starting directory initialization for collection '{}' with {} posts", self.name, self.posts.len());
        
        let binding = Config::get();
        let dir_manager = binding.directory_manager()?;
        
        // Track how many posts were assigned directories
        let mut assigned_count = 0;
        
        // All content goes into Tags directory using the collection name (tag/artist name) as the main folder
        trace!("Setting up tag directory for '{}' (category: {})", self.name, self.category);
        let dir_setup_start = Instant::now();
        self.base_directory = Some(dir_manager.get_tag_directory(&self.name)?);
        let dir_setup_duration = dir_setup_start.elapsed();
        
        if dir_setup_duration > Duration::from_millis(100) {
            info!("Directory setup took {:.2?} for collection '{}'", dir_setup_duration, self.name);
        }
        
        // Assign the base directory directly to all posts (no artist subdirectories)
        if let Some(base_dir) = &self.base_directory {
            let assignment_start = Instant::now();
            for post in &mut self.posts {
                trace!("Assigned directory '{}' for post '{}'", base_dir.display(), post.name());
                post.set_save_directory(base_dir.clone());
                assigned_count += 1;
            }
            let assignment_duration = assignment_start.elapsed();
            
            if assignment_duration > Duration::from_millis(50) {
                info!("Directory assignment took {:.2?} for {} posts in collection '{}'", 
                      assignment_duration, assigned_count, self.name);
            }
        }
        
        // Note: Duplicate checking is now performed during search phase

        // Verify all posts were assigned directories
        let total_posts = self.posts.len();
        if assigned_count != total_posts {
            warn!("Not all posts were assigned directories: {}/{} assigned in collection '{}'", 
                  assigned_count, total_posts, self.name);
        } else {
            trace!("Successfully assigned directories to all {} posts in collection '{}'", 
                  total_posts, self.name);
        }
        
        let total_duration = directory_start.elapsed();
        info!("Completed directory initialization for collection '{}' in {:.2?}", self.name, total_duration);

        Ok(())
    }

    pub(crate) fn has_new_posts(&self) -> bool {
        self.posts.iter().any(|post| post.is_new())
    }

    pub(crate) fn new_posts_count(&self) -> usize {
        self.posts.iter().filter(|post| post.is_new()).count()
    }
}

impl From<(&SetEntry, Vec<GrabbedPost>)> for PostCollection {
    fn from((set, posts): (&SetEntry, Vec<GrabbedPost>)) -> Self {
        PostCollection::new(&set.name, "Sets", posts)
    }
}

pub(crate) trait Shorten<T> {
    fn shorten(&self, delimiter: T) -> String;
}

impl Shorten<&str> for PostCollection {
    fn shorten(&self, delimiter: &str) -> String {
        if self.name.len() >= 25 {
            let mut short_name = self.name[0..25].to_string();
            short_name.push_str(delimiter);
            short_name
        } else {
            self.name.to_string()
        }
    }
}

impl Shorten<char> for PostCollection {
    fn shorten(&self, delimiter: char) -> String {
        if self.name.len() >= 25 {
            let mut short_name = self.name[0..25].to_string();
            short_name.push(delimiter);
            short_name
        } else {
            self.name.to_string()
        }
    }
}

pub(crate) struct Grabber {
    posts: Vec<PostCollection>,
    request_sender: RequestSender,
    blacklist: Option<Arc<Mutex<Blacklist>>>,
    safe_mode: bool,
    /// Cache for whitelist generation to avoid repeated computation
    whitelist_cache: WhitelistCache,
    /// Adaptive rate limiter for managing API request intervals
    rate_limiter: Arc<Mutex<AdaptiveRateLimiter>>,
}

impl Grabber {
    
    pub(crate) fn estimate_post_count(&self, target: &DownloadTarget) -> usize {
        // This method estimates the post count for a target
        // Placeholder implementation
        match target.target_type {
            TargetType::Tag | TargetType::Artist => 1000,
            TargetType::Pool | TargetType::Set => 200,
            TargetType::Post => 1,
            TargetType::Favorites => 50,
        }
    }

    pub(crate) fn generate_download_plan(&self, targets: Vec<DownloadTarget>) -> DownloadPlan {
        let total_estimated_posts: usize = targets.iter().map(|t| self.estimate_post_count(t)).sum();
        let estimated_duration = Some(Duration::from_secs((total_estimated_posts / 10) as u64)); // Example estimation
        DownloadPlan {
            targets,
            total_estimated_posts,
            estimated_duration,
        }
    }

    pub(crate) fn orchestrate_download(&mut self, plan: DownloadPlan) -> Vec<TargetResult> {
        // Process each target sequentially or concurrently based on configuration
        plan.targets.into_iter().map(|target| {
            let collection = self.process_target(&target);
            if let Some(collection) = collection {
                TargetResult::from_collection(target, collection)
            } else {
                TargetResult {
                    target,
                    collection: None,
                    posts_found: 0,
                    processing_time: Duration::new(0, 0),
                    success: false,
                    error_message: Some("Failed to process target".to_string()),
                }
            }
        }).collect()
    }

    fn process_target(&mut self, target: &DownloadTarget) -> Option<PostCollection> {
        // Unified handling for both tags and artists based on the target type
        match target.target_type {
            TargetType::Tag | TargetType::Artist => self.process_tags_or_artists(target),
            TargetType::Pool => self.process_pool(target),
            TargetType::Set => self.process_set(target),
            TargetType::Post => self.process_post(target),
            TargetType::Favorites => self.process_favorites(target),
        }
    }

    fn process_tags_or_artists(&self, target: &DownloadTarget) -> Option<PostCollection> {
        // This would contain logic to handle both tag and artist targets
        Some(PostCollection::new(&target.name, &target.category, vec![]))
    }

    fn process_pool(&self, target: &DownloadTarget) -> Option<PostCollection> {
        // Process a pool target
        Some(PostCollection::new(&target.name, "Pools", vec![]))
    }

    fn process_set(&self, target: &DownloadTarget) -> Option<PostCollection> {
        // Process a set target
        Some(PostCollection::new(&target.name, "Sets", vec![]))
    }

    fn process_post(&self, target: &DownloadTarget) -> Option<PostCollection> {
        // Process a post target
        Some(PostCollection::new(&target.name, "Posts", vec![]))
    }

    fn process_favorites(&self, target: &DownloadTarget) -> Option<PostCollection> {
        // Process a favorites target
        Some(PostCollection::new(&target.name, "Favorites", vec![]))
    }
    pub(crate) fn new(request_sender: RequestSender, safe_mode: bool) -> Self {
        Grabber {
            posts: vec![PostCollection::new("Single Posts", "", Vec::new())],
            request_sender,
            blacklist: None,
            safe_mode,
            whitelist_cache: WhitelistCache::new(),
            rate_limiter: Arc::new(Mutex::new(AdaptiveRateLimiter::new())),
        }
    }

    pub(crate) fn posts(&self) -> &Vec<PostCollection> {
        &self.posts
    }

    pub(crate) fn set_blacklist(&mut self, blacklist: Arc<Mutex<Blacklist>>) {
        let set = match blacklist.lock() {
            Ok(bl) => !bl.is_empty(),
            Err(_) => false,
        };
        if set {
            self.blacklist = Some(blacklist);
        }
    }

    pub(crate) fn set_safe_mode(&mut self, mode: bool) {
        self.safe_mode = mode;
    }

    pub(crate) fn grab_favorites(&mut self) {
        let login = Login::get();
        if !login.username().is_empty() && login.download_favorites() {
            let tag = format!("fav:{}", login.username());
            let posts = self.search(&tag, &TagSearchType::Special);
            let mut collection = PostCollection::new(&tag, "", GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender));
            if let Err(e) = collection.initialize_directories() {
                error!("Failed to initialize directories for favorites: {}", e);
                emergency_exit("Directory initialization failed");
            }
            self.posts.push(collection);
            info!("{} grabbed!", console::style(format!("\"{}\"", tag)).color256(39).italic());
        }
    }

    pub(crate) fn grab_posts_by_tags(&mut self, groups: &[Group]) {
        let tags: Vec<&Tag> = groups.iter().flat_map(|e| e.tags()).collect();
        let total_tags = tags.len();
        let progress_bar = ProgressBar::new(total_tags as u64);
        progress_bar.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} tags")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-"));

        // Use thread-safe collections for concurrent post gathering
        let posts_mutex = Arc::new(Mutex::new(Vec::<PostCollection>::new()));
        let progress_bar_arc = Arc::new(progress_bar);
        
        // Calculate optimal batch size for concurrent processing
        let cpu_count = num_cpus::get();
        let optimal_batch_size = std::cmp::min(10, std::cmp::max(4, cpu_count / 4)); // 4-10 tags per batch
        let concurrent_batches = std::cmp::min(cpu_count / 2, 6); // Up to 6 concurrent batches
        
        // Create batches of tags for concurrent processing
        let tag_batches: Vec<Vec<&Tag>> = tags.chunks(optimal_batch_size).map(|chunk| chunk.to_vec()).collect();
        
        info!("Processing {} tags in {} batches of {} tags each, using {} concurrent batch processors (CPU count: {})", 
              tags.len(), tag_batches.len(), optimal_batch_size, concurrent_batches, cpu_count);
        
        // Use channel-based work distribution for batches
        let (batch_tx, batch_rx) = mpsc::channel::<Vec<&Tag>>();
        let batch_rx = Arc::new(Mutex::new(batch_rx));
        
        // Send tag batches to the channel
        for batch in tag_batches {
            batch_tx.send(batch).unwrap();
        }
        drop(batch_tx); // Close the channel
        
        // Use scoped threads with batch processing
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            
            for batch_processor_id in 0..concurrent_batches {
                let posts_mutex = Arc::clone(&posts_mutex);
                let progress_bar = Arc::clone(&progress_bar_arc);
                let request_sender = self.request_sender.clone();
                let blacklist = self.blacklist.clone();
                let safe_mode = self.safe_mode;
                let batch_rx = Arc::clone(&batch_rx);
                let rate_limiter = Arc::clone(&self.rate_limiter);
                
                let handle = scope.spawn(move || {
                    // Stagger batch processor starts to avoid API burst
                    thread::sleep(Duration::from_millis(batch_processor_id as u64 * 100));
                    
                    // Process batches
                    while let Ok(tag_batch) = batch_rx.lock().unwrap().recv() {
                        let batch_start_time = Instant::now();
                        
                        // Process tags in this batch concurrently
                        Self::process_tag_batch_concurrently(
                            tag_batch, 
                            groups, 
                            &request_sender, 
                            &blacklist, 
                            safe_mode, 
                            &posts_mutex, 
                            &progress_bar, 
                            &rate_limiter,
                            batch_processor_id
                        );
                        
                        let batch_duration = batch_start_time.elapsed();
                        info!("Batch processor {} completed batch in {:.2?}", batch_processor_id, batch_duration);
                    }
                });
                
                handles.push(handle);
            }
            
            // Wait for all batch processors to complete
            for handle in handles {
                handle.join().unwrap();
            }
        });
        
        // Extract collections from the mutex and add to self.posts
        let mut collected_posts = posts_mutex.lock().unwrap();
        self.posts.append(&mut collected_posts);
        progress_bar_arc.finish_with_message("All tags processed");
    }
    
    /// Process a batch of tags concurrently within a single batch processor
    fn process_tag_batch_concurrently(
        tag_batch: Vec<&Tag>,
        groups: &[Group],
        request_sender: &RequestSender,
        blacklist: &Option<Arc<Mutex<Blacklist>>>,
        safe_mode: bool,
        posts_mutex: &Arc<Mutex<Vec<PostCollection>>>,
        progress_bar: &Arc<ProgressBar>,
        rate_limiter: &Arc<Mutex<AdaptiveRateLimiter>>,
        batch_processor_id: usize,
    ) {
        let batch_size = tag_batch.len();
        info!("Batch processor {} starting concurrent processing of {} tags", batch_processor_id, batch_size);
        
        // Use scoped threads to process tags within the batch concurrently
        std::thread::scope(|scope| {
            let mut tag_handles = Vec::new();
            
            for (tag_index, tag) in tag_batch.into_iter().enumerate() {
                let posts_mutex = Arc::clone(posts_mutex);
                let progress_bar = Arc::clone(progress_bar);
                let request_sender = request_sender.clone();
                let blacklist = blacklist.clone();
                let rate_limiter = Arc::clone(rate_limiter);
                let tag_name = tag.name().to_string(); // Clone for error logging
                
                let handle = scope.spawn(move || {
                    // Comprehensive error handling wrapper
                    let process_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        debug!("Batch processor {} starting tag '{}' (index {})", batch_processor_id, tag_name, tag_index);
                        
                        // Stagger tag processing within the batch to respect rate limits
                        let stagger_delay = Duration::from_millis((tag_index as u64 * 150) + (batch_processor_id as u64 * 50));
                        thread::sleep(stagger_delay);
                        
                        // Apply rate limiting with timeout protection
                        let rate_limit_start = Instant::now();
                        match rate_limiter.lock() {
                            Ok(limiter) => {
                                let delay = limiter.current_delay();
                                drop(limiter); // Release lock immediately
                                thread::sleep(delay);
                                debug!("Rate limiting applied for tag '{}' in {:.2?}", tag_name, rate_limit_start.elapsed());
                            }
                            Err(e) => {
                                warn!("Rate limiter lock failed for tag '{}': {}", tag_name, e);
                                // Continue without rate limiting rather than hanging
                            }
                        }
                        
                        let tag_start_time = Instant::now();
                        debug!("Processing tag '{}' started at {:.2?}", tag_name, tag_start_time);
                        
                        // Process the tag with timeout protection
                        let collection = Self::process_tag_to_collection_with_timeout(tag, groups, &request_sender, &blacklist, safe_mode);
                        
                        if let Some(collection) = collection {
                            // Add to shared collection in thread-safe manner with timeout
                            match posts_mutex.lock() {
                                Ok(mut posts) => {
                                    posts.push(collection);
                                    debug!("Successfully added collection for tag '{}'", tag_name);
                                }
                                Err(e) => {
                                    error!("Failed to acquire posts mutex for tag '{}': {}", tag_name, e);
                                }
                            }
                        } else {
                            debug!("No collection returned for tag '{}'", tag_name);
                        }
                        
                        let tag_duration = tag_start_time.elapsed();
                        info!("Batch processor {} finished processing tag '{}' in {:.2?}", batch_processor_id, tag_name, tag_duration);
                        
                        // Always increment progress bar, even on errors
                        progress_bar.inc(1);
                    }));
                    
                    // Handle panics gracefully
                    match process_result {
                        Ok(_) => {
                            debug!("Tag '{}' processed successfully", tag_name);
                        }
                        Err(panic_info) => {
                            error!("Panic occurred while processing tag '{}': {:?}", tag_name, panic_info);
                            // Ensure progress bar is still incremented even on panic
                            progress_bar.inc(1);
                        }
                    }
                });
                
                tag_handles.push(handle);
            }
            
            // Wait for all tags in this batch to complete with better error handling
            for (index, handle) in tag_handles.into_iter().enumerate() {
                match handle.join() {
                    Ok(_) => {
                        debug!("Tag thread {} in batch processor {} completed successfully", index, batch_processor_id);
                    }
                    Err(e) => {
                        error!("Tag thread {} in batch processor {} failed: {:?}", index, batch_processor_id, e);
                    }
                }
            }
        });
        
        info!("Batch processor {} completed processing {} tags", batch_processor_id, batch_size);
    }
    
    pub(crate) fn grab_posts_by_artists(&mut self, artists: &[crate::e621::io::artist::Artist]) {
        let total_artists = artists.len();
        let progress_bar = ProgressBar::new(total_artists as u64);
        progress_bar.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} artists")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-"));

        for artist in artists.iter() {
            let start_time = Instant::now();
            self.grab_by_artist(artist);
            progress_bar.inc(1);
            let duration = start_time.elapsed();
            info!("Finished processing artist {} in {:.2?}", artist.name(), duration);
        }
        progress_bar.finish_with_message("All artists processed");
    }
    
    fn grab_by_artist(&mut self, artist: &crate::e621::io::artist::Artist) {
        let search_tag = artist.name();
        let posts = self.search(search_tag, &TagSearchType::Special);
        let mut collection = PostCollection::new(
            search_tag,
            "General Searches", // Use same category as tags to put them in Tags folder
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for artist search: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);
        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", search_tag)).color256(39).italic()
        );
    }

    /// Static method to process a tag and return a PostCollection
    /// This is thread-safe and can be called from parallel contexts
    fn process_tag_to_collection(
        tag: &Tag,
        groups: &[Group],
        request_sender: &RequestSender,
        blacklist: &Option<Arc<Mutex<Blacklist>>>,
        safe_mode: bool,
    ) -> Option<PostCollection> {
        // Create a temporary grabber instance for processing
        let mut temp_grabber = Grabber {
            posts: Vec::new(),
            request_sender: request_sender.clone(),
            blacklist: blacklist.clone(),
            safe_mode,
            whitelist_cache: WhitelistCache::new(),
            rate_limiter: Arc::new(Mutex::new(AdaptiveRateLimiter::new())),
        };
        
        // Process the tag using existing logic
        temp_grabber.grab_by_tag_type_with_context(tag, groups);
        
        // Return the first collection if any were created
        if temp_grabber.posts.len() > 1 {
            // Skip the first "Single Posts" collection and return the actual tag collection
            temp_grabber.posts.into_iter().nth(1)
        } else {
            None
        }
    }
    
    /// Process a tag with timeout protection to prevent hangs
    /// This wraps the normal processing with timeout and error handling
    fn process_tag_to_collection_with_timeout(
        tag: &Tag,
        groups: &[Group],
        request_sender: &RequestSender,
        blacklist: &Option<Arc<Mutex<Blacklist>>>,
        safe_mode: bool,
    ) -> Option<PostCollection> {
        use std::sync::mpsc;
        use std::time::Duration;
        
        let tag_name = tag.name().to_string();
        let timeout = Duration::from_secs(300); // 5 minute timeout per tag
        
        // Create a channel for the result
        let (result_tx, result_rx) = mpsc::channel();
        
        // Spawn a thread to do the actual processing
        let tag_clone = tag.clone();
        let groups_clone = groups.to_vec();
        let request_sender_clone = request_sender.clone();
        let blacklist_clone = blacklist.clone();
        let tag_name_for_thread = tag_name.clone(); // Create copy for thread
        
        std::thread::spawn(move || {
            info!("Starting tag processing for '{}' with timeout protection", tag_name_for_thread);
            
            // Process with panic protection
            let process_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Self::process_tag_to_collection(
                    &tag_clone,
                    &groups_clone,
                    &request_sender_clone,
                    &blacklist_clone,
                    safe_mode,
                )
            }));
            
            let result = match process_result {
                Ok(collection) => {
                    info!("Tag processing completed successfully for '{}'", tag_name_for_thread);
                    collection
                }
                Err(panic_info) => {
                    error!("Panic occurred during tag processing for '{}': {:?}", tag_name_for_thread, panic_info);
                    None
                }
            };
            
            // Send result - ignore errors if receiver is dropped
            let _ = result_tx.send(result);
        });
        
        // Wait for result with timeout
        match result_rx.recv_timeout(timeout) {
            Ok(result) => {
                debug!("Tag processing for '{}' completed within timeout", tag_name);
                result
            }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Increase timeout and retry processing for long-running tags
                    let retry_timeout = timeout + Duration::from_secs(60);
                    warn!("Tag processing for '{}' exceeded timeout, retrying with extended timeout of {:.1?}", tag_name, retry_timeout);

                    let result_retry = result_rx.recv_timeout(retry_timeout);
                    match result_retry {
                        Ok(retry_result) => {
                            info!("Tag '{}' processed successfully after retry", tag_name);
                            retry_result
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            error!("Tag processing for '{}' timed out again after retry", tag_name);
                            None
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            error!("Tag processing thread for '{}' disconnected unexpectedly during retry", tag_name);
                            None
                        }
                    }
                }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                error!("Tag processing thread for '{}' disconnected unexpectedly", tag_name);
                None
            }
        }
    }
    
    /// Process a tag with context awareness to handle same tag names in different sections
    fn grab_by_tag_type_with_context(&mut self, tag: &Tag, groups: &[Group]) {
        // Check if this tag name appears in multiple sections
        let tag_name = tag.name();
        let current_tag_type = tag.tag_type();
        
        // Count how many times this tag name appears across all groups
        let tag_occurrences: Vec<_> = groups.iter()
            .flat_map(|group| group.tags())
            .filter(|t| t.name() == tag_name)
            .collect();
            
        if tag_occurrences.len() > 1 {
            // Same tag name appears in multiple sections
            info!("Tag '{}' appears in {} different sections - processing each context separately", tag_name, tag_occurrences.len());
            
            // Create a unique collection name based on tag type and section
            let collection_suffix = match current_tag_type {
                TagType::Artist => "_artist_search",
                TagType::General => "_general_search",
                TagType::Pool => "_pool",
                TagType::Set => "_set", 
                TagType::Post => "_post",
                TagType::Unknown => "_unknown",
            };
            
            // Process with modified name to avoid conflicts
            self.grab_by_tag_type_with_suffix(tag, collection_suffix);
        } else {
            // Normal processing for unique tag names
            self.grab_by_tag_type(tag);
        }
    }
    
    fn grab_by_tag_type(&mut self, tag: &Tag) {
        match tag.tag_type() {
            TagType::Pool => self.grab_pool(tag),
            TagType::Set => self.grab_set(tag),
            TagType::Post => self.grab_post(tag),
            TagType::General | TagType::Artist => self.grab_general(tag),
            TagType::Unknown => unreachable!(),
        };
    }
    
    /// Process a tag with a suffix to create unique collection names
    fn grab_by_tag_type_with_suffix(&mut self, tag: &Tag, suffix: &str) {
        match tag.tag_type() {
            TagType::Pool => self.grab_pool_with_suffix(tag, suffix),
            TagType::Set => self.grab_set_with_suffix(tag, suffix),
            TagType::Post => self.grab_post_with_suffix(tag, suffix),
            TagType::General | TagType::Artist => self.grab_general_with_suffix(tag, suffix),
            TagType::Unknown => unreachable!(),
        };
    }

    fn grab_general(&mut self, tag: &Tag) {
        let posts = self.get_posts_from_tag(tag);
        let mut collection = PostCollection::new(
            tag.name(),
            "General Searches",
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for general search: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);
        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", tag.name())).color256(39).italic()
        );
    }
    
    fn grab_general_with_suffix(&mut self, tag: &Tag, suffix: &str) {
        let posts = self.get_posts_from_tag(tag);
        let collection_name = format!("{}{}", tag.name(), suffix);
        let mut collection = PostCollection::new(
            &collection_name,
            "General Searches",
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for general search with suffix: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);
        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", collection_name)).color256(39).italic()
        );
    }

    fn grab_post(&mut self, tag: &Tag) {
        let entry: PostEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "single");
        let id = entry.id;

        if self.safe_mode {
            match entry.rating.as_str() {
                "s" => {
                    self.add_single_post(entry, id);
                }
                _ => {
                    info!(
                        "Skipping Post: {} due to being explicit or questionable",
                        console::style(format!("\"{id}\"")).color256(39).italic()
                    );
                }
            }
        } else {
            self.add_single_post(entry, id);
        }
    }
    
    fn grab_post_with_suffix(&mut self, tag: &Tag, _suffix: &str) {
        // For single posts, suffix doesn't change behavior since they go to the "Single Posts" collection
        self.grab_post(tag);
    }

    fn grab_set(&mut self, tag: &Tag) {
        let entry: SetEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "set");
        let posts = self.search(&format!("set:{}", entry.shortname), &TagSearchType::Special);
        let mut collection = PostCollection::from((&entry, GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender)));
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for set: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);

        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", entry.name)).color256(39).italic()
        );
    }
    
    fn grab_set_with_suffix(&mut self, tag: &Tag, suffix: &str) {
        let entry: SetEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "set");
        let posts = self.search(&format!("set:{}", entry.shortname), &TagSearchType::Special);
        let collection_name = format!("{}{}", entry.name, suffix);
        let mut collection = PostCollection::new(
            &collection_name,
            "Sets",
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for set with suffix: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);

        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", collection_name)).color256(39).italic()
        );
    }

    fn grab_pool(&mut self, tag: &Tag) {
        let mut entry: PoolEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "pool");
        let name = &entry.name;
        let mut posts = self.search(&format!("pool:{}", entry.id), &TagSearchType::Special);

        entry.post_ids.retain(|id| posts.iter().any(|post| post.id == *id));
        Self::sort_pool_by_id(&entry, &mut posts);

        let mut collection = PostCollection::new(
            name,
            "Pools",
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for pool: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);

        info!(
            "{} grabbed!",
            console::style(format!("\"{name}\"")).color256(39).italic()
        );
    }
    
    fn grab_pool_with_suffix(&mut self, tag: &Tag, suffix: &str) {
        let mut entry: PoolEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "pool");
        let collection_name = format!("{}{}", entry.name, suffix);
        let mut posts = self.search(&format!("pool:{}", entry.id), &TagSearchType::Special);

        entry.post_ids.retain(|id| posts.iter().any(|post| post.id == *id));
        Self::sort_pool_by_id(&entry, &mut posts);

        let mut collection = PostCollection::new(
            &collection_name,
            "Pools",
            GrabbedPost::new_vec_with_artist_ids(posts, &self.request_sender),
        );
        if let Err(e) = collection.initialize_directories() {
            error!("Failed to initialize directories for pool with suffix: {}", e);
            emergency_exit("Directory initialization failed");
        }
        self.posts.push(collection);

        info!(
            "{} grabbed!",
            console::style(format!("\"{}\"", collection_name)).color256(39).italic()
        );
    }

    fn sort_pool_by_id(entry: &PoolEntry, posts: &mut [PostEntry]) {
        for (i, id) in entry.post_ids.iter().enumerate() {
            if posts[i].id != *id {
                let correct_index = posts.iter().position(|e| e.id == *id).unwrap();
                posts.swap(i, correct_index);
            }
        }
    }

    fn get_posts_from_tag(&self, tag: &Tag) -> Vec<PostEntry> {
        self.search(tag.name(), tag.search_type())
    }

    fn single_post_collection(&mut self) -> &mut PostCollection {
        self.posts.first_mut().unwrap()
    }

    fn add_single_post(&mut self, entry: PostEntry, id: i64) {
        match entry.file.url {
            None => warn!(
                "Post with ID {} has no URL!",
                console::style(format!("\"{id}\"")).color256(39).italic()
            ),
            Some(_) => {
                // Generate enhanced filename with artist names and IDs
                let enhanced_name = GrabbedPost::generate_enhanced_filename(&entry, Config::get().naming_convention(), &self.request_sender);
                let short_url = format!("e621.net/posts/{}", entry.id);
                let mut grabbed_post = GrabbedPost {
                    url: entry.file.url.clone().unwrap(),
                    name: enhanced_name,
                    file_size: entry.file.size,
                    save_directory: None,
                    artist: None,
                    is_new: true,
                    sha512_hash: None,
                    short_url: Some(short_url),
                    post_id: entry.id,
                };
                if let Some(artist_tag) = entry.tags.artist.first() {
                    grabbed_post.set_artist(artist_tag.clone());
                }
                
                // Check if this post ID has been downloaded before using batch processing
                let binding = Config::get();
                let dir_manager = binding.directory_manager().unwrap();
                let batch_results = dir_manager.batch_has_post_ids(&[entry.id]);
                let is_duplicate = batch_results.first().map(|(_, exists)| *exists).unwrap_or(false);
                grabbed_post.set_is_new(!is_duplicate);
                let collection = self.single_post_collection();
                // Add the post first, then initialize directories (which includes duplicate checking)
                collection.posts.push(grabbed_post);
                if let Err(e) = collection.initialize_directories() {
                    error!("Failed to initialize directories for single post: {}", e);
                    emergency_exit("Directory initialization failed");
                }
                info!(
                    "Post with ID {} grabbed!",
                    console::style(format!("\"{id}\"")).color256(39).italic()
                );
            }
        }
    }

    fn search(&self, searching_tag: &str, tag_search_type: &TagSearchType) -> Vec<PostEntry> {
        let search_start = Instant::now();
        info!("Starting search for tag: {}", searching_tag);
        
        let mut posts: Vec<PostEntry> = Vec::new();
        let mut filtered = 0;
        let mut invalid_posts = 0;
        match tag_search_type {
            TagSearchType::General => {
                // Start with a reasonable initial capacity, vector will grow as needed
                posts = Vec::with_capacity(320);
                self.general_search(searching_tag, &mut posts, &mut filtered, &mut invalid_posts);
            }
            TagSearchType::Special => {
                self.special_search(searching_tag, &mut posts, &mut filtered, &mut invalid_posts);
            }
            TagSearchType::None => {}
        }

        if filtered > 0 {
            info!(
                "Filtered {} total blacklisted posts from search...",
                console::style(filtered).cyan().italic()
            );
        }

        if invalid_posts > 0 {
            info!(
                "Filtered {} total invalid posts from search...",
                console::style(invalid_posts).cyan().italic()
            );
        }

        info!("Completed search for tag: {} in {:.2?}", searching_tag, search_start.elapsed());

        posts
    }

    /// Calculate optimal thread count based on the number of pages to search
    fn calculate_optimal_thread_count(&self, max_pages: usize) -> usize {
        match max_pages {
            1..=2 => 1,          // Single page searches don't need parallelism
            3..=5 => 2,          // Small searches use minimal threads
            6..=10 => 3,         // Standard searches use conservative threading
            11..=20 => 4,        // Medium searches use moderate threading
            21..=50 => 5,        // Large searches use higher threading
            _ => 6,              // Very large searches max out at 6 threads
        }
    }

    fn special_search(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
    ) {
        let max_pages = Config::get().max_pages_to_search();
        
        // Calculate optimal thread count based on max pages
        let cpu_count = num_cpus::get();
        let half_cores = std::cmp::max(1, cpu_count / 2); // Use half of available cores
        let thread_count = std::cmp::min(half_cores, self.calculate_optimal_thread_count(max_pages));
        
        info!("Searching max {} pages using {} threads (CPU count: {}, using {} cores for post processing)", 
              max_pages, thread_count, cpu_count, half_cores);
        
        // Estimate post count for pipeline decision
        let estimated_posts = max_pages * 320; // Rough estimate: 320 posts per page
        let processing_config = PostProcessingConfig::new(estimated_posts, thread_count);
        
        if processing_config.use_pipeline {
            info!("Using pipeline orchestration for large batch ({} estimated posts)", estimated_posts);
            self.special_search_with_pipeline(searching_tag, posts, filtered, invalid_posts, processing_config);
        } else {
            info!("Using enhanced in-thread processing for smaller batch ({} estimated posts)", estimated_posts);
            self.special_search_with_enhanced_threads(searching_tag, posts, filtered, invalid_posts, thread_count);
        }
    }
    
    /// Enhanced in-thread processing - processes posts immediately within fetch threads
    fn special_search_with_enhanced_threads(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
        thread_count: usize,
    ) {
        let max_pages = Config::get().max_pages_to_search();
        
        // Calculate optimal thread count based on max pages
        let thread_count = self.calculate_optimal_thread_count(max_pages);
        
        info!("Searching max {} pages using {} threads with adaptive rate limiting (stops early if 4 consecutive empty pages found)", 
              max_pages, thread_count);
        
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} Parallel searching {msg}...")
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
        );
        progress_bar.set_message(format!("batches of {} pages", thread_count));
        
        let mut batch_start = 1u16;
        let mut total_pages_searched = 0;
        let mut pages_with_new_content = 0;
        let mut total_new_posts = 0;
        let mut total_duplicate_posts = 0;
        let mut consecutive_empty_pages = 0;
        let _max_pages_u16 = max_pages as u16; // Keep for potential future use
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        
        loop {
            // Check if we've reached the adaptive search limit (allow up to 3x configured pages)
            let max_search_limit = max_pages * 3;
            if total_pages_searched >= max_search_limit {
                info!("Reached adaptive search limit of {} pages ({} configured * 3), stopping search", max_search_limit, max_pages);
                break;
            }
            
            // Continue searching until we've reached the configured maximum pages
            // Note: This ensures we search the full configured amount rather than stopping early
            // when we find fewer pages with new content than expected
            
            // Channel for this batch
            let (result_tx, result_rx) = mpsc::channel::<(u16, Vec<PostEntry>)>();
            
            // Spawn threads for this batch, but don't exceed max pages
            let mut handles = Vec::new();
            for thread_id in 0..thread_count {
                let page_to_search = batch_start + thread_id as u16;
                
                // Don't search beyond reasonable limits (prevent infinite searching)
                if page_to_search > (max_pages * 3) as u16 {
                    break;
                }
                
                let request_sender = self.request_sender.clone();
                let searching_tag = searching_tag.to_string();
                let result_tx = result_tx.clone();
                let rate_limiter = Arc::clone(&self.rate_limiter);
                let blacklist = self.blacklist.clone();
                let whitelist = self.generate_whitelist_for_search(searching_tag.as_str());
                
                let handle = thread::spawn(move || {
                    // Stagger thread starts to avoid simultaneous API hits
                    let stagger_delay_ms = 50 * thread_id as u64;
                    thread::sleep(Duration::from_millis(stagger_delay_ms));
                    
                    // Use adaptive rate limiter to wait before making the request
                    if let Ok(limiter) = rate_limiter.lock() {
                        let delay = limiter.current_delay();
                        drop(limiter); // Release lock before sleeping
                        thread::sleep(delay);
                    }
                    
                    let page_start = Instant::now();
                    let mut searched_posts = request_sender.safe_bulk_post_search(&searching_tag, page_to_search).posts;
                    let request_duration = page_start.elapsed();
                    
                    // Record the response time in the adaptive rate limiter
                    if let Ok(limiter) = rate_limiter.lock() {
                        limiter.record_response_time(request_duration);
                    }
                    
                    // Log slow requests as warnings
                    if request_duration > Duration::from_secs(3) {
                        warn!("Slow API request detected: tag '{}' page {} took {:.2?}", 
                              searching_tag, page_to_search, request_duration);
                    } else {
                        trace!("Thread {} - Page {} completed in {:.2?} - found {} posts", 
                               thread_id, page_to_search, request_duration, searched_posts.len());
                    }
                    
                    // Enhanced in-thread processing: Process posts immediately within this thread
                    if !searched_posts.is_empty() {
                        // Apply filtering with whitelist if enabled
                        let config = Config::get();
                        if config.whitelist_override_blacklist() {
                            Self::apply_whitelist_blacklist_filtering(&mut searched_posts, &blacklist, &whitelist);
                        } else {
                            Self::apply_blacklist_filtering(&mut searched_posts, &blacklist);
                        }
                        
                        // Remove invalid posts
                        Self::remove_invalid_posts_in_thread(&mut searched_posts);
                        
                        // Perform duplicate checking and enhanced filename generation
                        Self::process_posts_for_duplicates_and_filenames(&mut searched_posts);
                    }
                    
                    // Send processed results back
                    let _ = result_tx.send((page_to_search, searched_posts));
                });
                handles.push(handle);
            }
            
            // Drop our copy of the result sender
            drop(result_tx);
            
            // Collect results from this batch
            let mut batch_results = Vec::new();
            let batch_start_time = Instant::now();
            let threads_spawned = handles.len();
            
            for _ in 0..threads_spawned {
                if let Ok((page, searched_posts)) = result_rx.recv() {
                    batch_results.push((page, searched_posts));
                    total_pages_searched += 1;
                }
            }
            
            let batch_duration = batch_start_time.elapsed();
            
            // Check if this batch took too long (indication of API overload)
            if batch_duration > Duration::from_secs(15) && thread_count > 6 {
                warn!("Batch of {} pages took {:.2?} - API may be overloaded!", thread_count, batch_duration);
                warn!("Consider reducing parallel search threads in config.json for better performance.");
                
                // Add extra delay between batches when API is slow
                let extra_delay = Duration::from_millis(1000 + (thread_count * 100) as u64);
                info!("Adding {:.2?} delay before next batch to reduce API load...", extra_delay);
                thread::sleep(extra_delay);
            }
            
            // Wait for all threads in this batch to complete
            for handle in handles {
                let _ = handle.join();
            }
            
            // Sort results by page number to maintain order
            batch_results.sort_by_key(|(page, _)| *page);
            
            // Process results and check for new content
            let mut found_posts_in_batch = false;
            let mut found_any_posts_in_batch = false; // Track if ANY posts were found (including duplicates)
            let mut new_posts_in_batch = 0;
            let mut duplicate_posts_in_batch = 0;
            let mut empty_pages_in_batch = 0;
            
            for (_page, searched_posts) in batch_results {
                if !searched_posts.is_empty() {
                    found_posts_in_batch = true;
                    found_any_posts_in_batch = true;
                    
                    // All posts are now pre-processed and filtered within threads
                    // Just add them to the final results
                    new_posts_in_batch += searched_posts.len();
                    posts.extend(searched_posts);
                } else {
                    // This individual page was empty
                    empty_pages_in_batch += 1;
                }
            }
            
            // Update counters
            total_new_posts += new_posts_in_batch;
            
            if new_posts_in_batch > 0 {
                pages_with_new_content += 1;
            }
            
            // Track consecutive empty pages to detect end of content
            if found_any_posts_in_batch {
                // Reset counter if we found any posts in this batch
                consecutive_empty_pages = 0;
            } else {
                // Add the actual number of empty pages found in this batch
                consecutive_empty_pages += empty_pages_in_batch;
            }
            
            // Detailed progress reporting
            progress_bar.set_message(format!("processed {} pages ({} new posts, {} duplicates, {}/{} pages with new content)", 
                                             total_pages_searched, total_new_posts, total_duplicate_posts, pages_with_new_content, max_pages));
            
            // Log detailed batch results
            info!("Batch completed: pages {}-{}, found {} new posts, {} empty pages, {} total posts so far", 
                  batch_start, batch_start + threads_spawned as u16 - 1, new_posts_in_batch, empty_pages_in_batch, total_new_posts);
            
            // Adaptive search logic: Continue until we find enough pages with new content
            // or reach a reasonable search limit to prevent infinite searching
            let max_search_limit = max_pages * 3; // Allow searching up to 3x configured pages to find new content
            
            let should_continue = if total_pages_searched >= max_pages {
                // We've searched the configured number of pages - this is the primary condition
                info!("Searched {} pages (target: {}), search complete. Found {} pages with new content", total_pages_searched, max_pages, pages_with_new_content);
                false
            } else if total_pages_searched >= max_search_limit {
                // We've searched too many pages, stop to prevent infinite searching
                info!("Reached search limit of {} pages ({} configured * 3), stopping adaptive search", max_search_limit, max_pages);
                false
            } else if consecutive_empty_pages >= 4 {
                // We've hit 4 consecutive pages with no posts at all - likely reached end of content
                info!("Found {} consecutive empty pages, likely reached end of available content. Stopping search.", consecutive_empty_pages);
                false
            } else if !found_posts_in_batch {
                // No posts found in this batch - check if we should stop based on search depth
                // This logic is now redundant since we handle 4 consecutive empty pages above
                // Just continue searching unless we've hit the 4 consecutive limit
                info!("No posts in this batch, but continuing search ({}/{} pages searched, {} pages with new content, {} consecutive empty)", 
                      total_pages_searched, max_pages, pages_with_new_content, consecutive_empty_pages);
                let should_stop_early = false;
                !should_stop_early
            } else {
                // Found some posts (even if duplicates), continue searching
                true
            };
            
            // Log progress summary every few batches
            if total_pages_searched % (thread_count * 3) == 0 || !should_continue {
                info!("Search progress: {}/{} pages searched, {} total posts found, {} pages with new content, {} consecutive empty pages", 
                      total_pages_searched, max_pages, total_new_posts, pages_with_new_content, consecutive_empty_pages);
            }
            
            if !should_continue {
                break;
            }
            
            // Move to next batch
            batch_start += thread_count as u16;
        }
        
        progress_bar.finish_with_message(format!("Enhanced in-thread processing: {} new posts found across {} pages ({} pages with new content)", 
                                                posts.len(), total_pages_searched, pages_with_new_content));
        
        if total_duplicate_posts > 0 {
            info!("Enhanced search skipped {} already downloaded posts, found {} new posts from {} pages", 
                  total_duplicate_posts, posts.len(), total_pages_searched);
        }
    }
    
    /// Pipeline orchestration for large batches with dedicated post-processing threads
    fn special_search_with_pipeline(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
        processing_config: PostProcessingConfig,
    ) {
        let max_pages = Config::get().max_pages_to_search();
        let thread_count = self.calculate_optimal_thread_count(max_pages);
        
        info!("Using pipeline orchestration with {} fetch threads and {} processing threads", 
              thread_count, processing_config.processing_threads);
        
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} Pipeline processing {msg}...")
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
        );
        
        // Create channels for high-throughput batch processing pipeline
        let (job_tx, job_rx) = mpsc::channel::<PostProcessingJob>();
        let (result_tx, result_rx) = mpsc::channel::<ProcessedPostBatch>();
        
        // Use unbounded channels for each processing thread to eliminate mutex contention
        let mut per_thread_job_senders = Vec::new();
        let mut processing_handles = Vec::new();
        let whitelist = self.generate_whitelist_for_search(searching_tag);
        
        // Create individual channels for each processing thread
        for processor_id in 0..processing_config.processing_threads {
            let (thread_job_tx, thread_job_rx) = mpsc::channel::<PostProcessingJob>();
            per_thread_job_senders.push(thread_job_tx);
            
            let result_tx = result_tx.clone();
            let blacklist = self.blacklist.clone();
            let whitelist = whitelist.clone();
            
            let handle = thread::spawn(move || {
                // Batch accumulator for processing efficiency
                let mut batch_accumulator: Vec<PostProcessingJob> = Vec::with_capacity(50);
                let batch_timeout = Duration::from_millis(10); // Very short timeout for responsiveness
                
                loop {
                    // Try to accumulate a batch of jobs
                    let batch_start = Instant::now();
                    
                    // Get first job (blocking)
                    match thread_job_rx.recv_timeout(batch_timeout) {
                        Ok(job) => {
                            batch_accumulator.push(job);
                            
                            // Try to get more jobs quickly (non-blocking)
                            while batch_accumulator.len() < 50 && batch_start.elapsed() < batch_timeout {
                                match thread_job_rx.try_recv() {
                                    Ok(job) => batch_accumulator.push(job),
                                    Err(_) => break,
                                }
                            }
                            
                            // Process accumulated batch
                            if !batch_accumulator.is_empty() {
                                let processing_start = Instant::now();
                                let processed_batch = Self::process_post_batch(batch_accumulator, &blacklist, &whitelist);
                                let processing_duration = processing_start.elapsed();
                                
                                trace!("Processor {} completed batch of {} posts in {:.2?}", 
                                       processor_id, processed_batch.posts.len(), processing_duration);
                                       
                                let _ = result_tx.send(processed_batch);
                                batch_accumulator = Vec::with_capacity(50); // Reset for next batch
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            // Process any remaining jobs in final batch
                            if !batch_accumulator.is_empty() {
                                let processing_start = Instant::now();
                                let processed_batch = Self::process_post_batch(batch_accumulator, &blacklist, &whitelist);
                                let processing_duration = processing_start.elapsed();
                                
                                trace!("Processor {} completed final batch of {} posts in {:.2?}", 
                                       processor_id, processed_batch.posts.len(), processing_duration);
                                       
                                let _ = result_tx.send(processed_batch);
                            }
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Process accumulated batch on timeout
                            if !batch_accumulator.is_empty() {
                                let processing_start = Instant::now();
                                let processed_batch = Self::process_post_batch(batch_accumulator, &blacklist, &whitelist);
                                let processing_duration = processing_start.elapsed();
                                
                                trace!("Processor {} completed timeout batch of {} posts in {:.2?}", 
                                       processor_id, processed_batch.posts.len(), processing_duration);
                                       
                                let _ = result_tx.send(processed_batch);
                                batch_accumulator = Vec::with_capacity(50); // Reset for next batch
                            }
                        }
                    }
                }
            });
            processing_handles.push(handle);
        }
        
        // Start fetching with the same logic as enhanced threads, but send to pipeline
        let mut batch_start = 1u16;
        let mut total_pages_searched = 0;
        let mut total_jobs_sent = 0;
        let mut total_posts_found = 0;  // Track total posts found across all batches
        let mut pages_with_new_content = 0;
        let mut consecutive_empty_pages = 0;
        
        std::thread::scope(|scope| {
            // Spawn fetch threads
            let fetch_handle = scope.spawn(|| {
                loop {
                    let max_search_limit = max_pages * 3;
                    if total_pages_searched >= max_search_limit {
                        break;
                    }
                    
                    // Fetch batch
                    let (fetch_tx, fetch_rx) = mpsc::channel::<(u16, Vec<PostEntry>)>();
                    let mut fetch_handles = Vec::new();
                    
                    for thread_id in 0..thread_count {
                        let page_to_search = batch_start + thread_id as u16;
                        
                        if page_to_search > (max_pages * 3) as u16 {
                            break;
                        }
                        
                        let request_sender = self.request_sender.clone();
                        let searching_tag = searching_tag.to_string();
                        let fetch_tx = fetch_tx.clone();
                        let rate_limiter = Arc::clone(&self.rate_limiter);
                        
                        let handle = thread::spawn(move || {
                            // Stagger and rate limit
                            thread::sleep(Duration::from_millis(50 * thread_id as u64));
                            
                            if let Ok(limiter) = rate_limiter.lock() {
                                let delay = limiter.current_delay();
                                drop(limiter);
                                thread::sleep(delay);
                            }
                            
                            let page_start = Instant::now();
                            let searched_posts = request_sender.safe_bulk_post_search(&searching_tag, page_to_search).posts;
                            let request_duration = page_start.elapsed();
                            
                            if let Ok(limiter) = rate_limiter.lock() {
                                limiter.record_response_time(request_duration);
                            }
                            
                            let _ = fetch_tx.send((page_to_search, searched_posts));
                        });
                        fetch_handles.push(handle);
                    }
                    
                    drop(fetch_tx);
                    
                    // Collect fetched posts and send to processing pipeline
                    let threads_spawned = fetch_handles.len();
                    let mut posts_in_batch = 0;
                    let mut empty_pages_in_batch = 0;
                    
                    for _ in 0..threads_spawned {
                        if let Ok((page, posts_from_page)) = fetch_rx.recv() {
                            total_pages_searched += 1;
                            
                            if posts_from_page.is_empty() {
                                empty_pages_in_batch += 1;
                            } else {
                                posts_in_batch += posts_from_page.len();
                                total_posts_found += posts_from_page.len();  // Track cumulative posts
                                pages_with_new_content += 1;
                                consecutive_empty_pages = 0; // Reset on finding posts
                            }
                            
                            // Send posts in optimized batches to reduce pipeline overhead
                            if !posts_from_page.is_empty() {
                                let job = PostProcessingJob {
                                    posts_batch: posts_from_page,
                                    page_numbers: vec![page],
                                    thread_id: 0, // Will be set by processing thread
                                    search_tag: searching_tag.to_string(),
                                };
                                
                                // Distribute jobs round-robin to processing threads
                                let thread_index = (page % processing_config.processing_threads as u16) as usize;
                                if let Some(sender) = per_thread_job_senders.get(thread_index) {
                                    if sender.send(job).is_ok() {
                                        total_jobs_sent += 1;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Update consecutive empty pages counter
                    if posts_in_batch == 0 {
                        consecutive_empty_pages += empty_pages_in_batch;
                    }
                    
                    // Log detailed batch results with cumulative post tracking
                    info!("Pipeline batch completed: pages {}-{}, found {} posts (raw API responses, {} total raw), {} empty pages, {} total jobs queued so far", 
                          batch_start, batch_start + threads_spawned as u16 - 1, posts_in_batch, total_posts_found, empty_pages_in_batch, total_jobs_sent);
                    
                    // Log progress summary every few batches
                    if total_pages_searched % (thread_count * 3) == 0 {
                        info!("Pipeline progress: {}/{} pages searched, {} jobs queued, {} pages with new content, {} consecutive empty pages", 
                              total_pages_searched, max_pages, total_jobs_sent, pages_with_new_content, consecutive_empty_pages);
                    }
                    
                    // Wait for fetch threads
                    for handle in fetch_handles {
                        let _ = handle.join();
                    }
                    
                    // Check termination conditions
                    if total_pages_searched >= max_pages {
                        info!("Pipeline: Searched {} pages (target: {}), search complete. Found {} pages with new content", total_pages_searched, max_pages, pages_with_new_content);
                        break;
                    }
                    
                    // Early termination if we hit too many consecutive empty pages
                    if consecutive_empty_pages >= 4 {
                        info!("Pipeline: Found {} consecutive empty pages, likely reached end of available content. Stopping search.", consecutive_empty_pages);
                        break;
                    }
                    
                    batch_start += thread_count as u16;
                }
            });
            
            // Wait for fetching to complete
            let _ = fetch_handle.join();
        });
        
        // Signal processing pipeline to stop by closing the individual job senders
        info!("Pipeline: Finishing data collection from {} fetch threads, closing job senders...", thread_count);
        for sender in per_thread_job_senders {
            drop(sender);
        }
        drop(job_tx);
        
        // Collect processed results with improved efficiency
        let mut total_batches_processed = 0;
        let mut total_filtered = 0;
        let mut total_invalid = 0;
        
        // Use a timeout to avoid hanging if some batches are lost
        let collection_timeout = Duration::from_secs(30);
        let collection_start = Instant::now();
        
        info!("Pipeline: Starting result collection from {} processing threads with {}s timeout...", 
              processing_config.processing_threads, collection_timeout.as_secs());
        
        // Collect results until all processing threads finish or timeout
        loop {
            match result_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(processed_batch) => {
                    total_batches_processed += 1;
                    total_filtered += processed_batch.filtered_count;
                    total_invalid += processed_batch.invalid_count;
                    
                    // Add all valid posts from the batch
                    posts.extend(processed_batch.posts);
                    
                    // Update progress more frequently for batches
                    if total_batches_processed % 10 == 0 {
                        progress_bar.set_message(format!("processed {} batches ({} total posts, {} filtered, {} invalid)", 
                                                         total_batches_processed, posts.len(), total_filtered, total_invalid));
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Check if we've been waiting too long
                    if collection_start.elapsed() > collection_timeout {
                        warn!("Pipeline result collection timed out after {:.2?}. Proceeding with {} posts collected.", collection_timeout, posts.len());
                        info!("Pipeline: Processed {} batches before timeout, {} filtered, {} invalid", 
                              total_batches_processed, total_filtered, total_invalid);
                        break;
                    }
                    // Log status every 5 seconds during timeout checks
                    let elapsed = collection_start.elapsed();
                    if elapsed.as_secs() % 5 == 0 && elapsed.as_millis() % 5000 < 200 {
                        info!("Pipeline: Still collecting results... {} batches processed, {} posts collected, {:.1}s elapsed", 
                              total_batches_processed, posts.len(), elapsed.as_secs_f32());
                    }
                    // Continue waiting for more results
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // All processing threads have finished
                    break;
                }
            }
        }
        
        // Wait for all processing threads to complete
        for handle in processing_handles {
            let _ = handle.join();
        }
        
        *filtered += total_filtered as u16;
        *invalid_posts += total_invalid as u16;
        
        progress_bar.finish_with_message(format!("Pipeline processing: {} valid posts from {} batches processed ({} pages)", 
                                                posts.len(), total_batches_processed, total_pages_searched));
        
        info!("Pipeline: Result collection complete - {} posts from {} batches processed in {:.2?}", 
              posts.len(), total_batches_processed, collection_start.elapsed());
        info!("Pipeline: Final stats - {} filtered, {} invalid posts removed during processing", 
              total_filtered, total_invalid);
    }
    
    /// Helper method to apply whitelist and blacklist filtering in threads
    fn apply_whitelist_blacklist_filtering(posts: &mut Vec<PostEntry>, blacklist: &Option<Arc<Mutex<Blacklist>>>, whitelist: &[String]) {
        if let Some(bl) = blacklist {
            if let Ok(blacklist_guard) = bl.lock() {
                posts.retain(|post| {
                    // Check if post contains any whitelisted tags
                    let post_tags = post.tags.clone().combine_tags();
                    let is_whitelisted = whitelist.iter().any(|whitelist_tag| {
                        post_tags.iter().any(|post_tag| post_tag == whitelist_tag)
                    });
                    
                    if is_whitelisted {
                        return true;
                    }
                    
                    // Apply blacklist filter if not whitelisted
                    let mut single_post = vec![post.clone()];
                    let filtered_count = blacklist_guard.filter_posts(&mut single_post);
                    filtered_count == 0 // Keep post if it wasn't filtered by blacklist
                });
            }
        }
    }
    
    /// Helper method to apply blacklist filtering in threads
    fn apply_blacklist_filtering(posts: &mut Vec<PostEntry>, blacklist: &Option<Arc<Mutex<Blacklist>>>) {
        let empty_whitelist = Vec::new();
        Self::apply_whitelist_blacklist_filtering(posts, blacklist, &empty_whitelist);
    }
    
    /// Helper method to remove invalid posts in threads
    fn remove_invalid_posts_in_thread(posts: &mut Vec<PostEntry>) {
        posts.retain(|e| !e.flags.deleted && e.file.url.is_some());
    }
    
    /// Helper method to process posts for duplicates and generate enhanced filenames
    fn process_posts_for_duplicates_and_filenames(posts: &mut Vec<PostEntry>) {
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        
        // Batch check all post IDs for duplicates
        let post_ids: Vec<i64> = posts.iter().map(|post| post.id).collect();
        let duplicate_results = dir_manager.batch_has_post_ids(&post_ids);
        let duplicate_map: std::collections::HashMap<i64, bool> = duplicate_results.into_iter().collect();
        
        // Filter out duplicates
        posts.retain(|post| {
            let is_duplicate = duplicate_map.get(&post.id).copied().unwrap_or(false);
            !is_duplicate
        });
    }
    
    /// Process a batch of post processing jobs - the core batch processing method
    /// This combines multiple jobs and processes all posts together for maximum efficiency
    fn process_post_batch(
        jobs: Vec<PostProcessingJob>, 
        blacklist: &Option<Arc<Mutex<Blacklist>>>, 
        whitelist: &[String]
    ) -> ProcessedPostBatch {
        let processing_start = Instant::now();
        let mut all_posts = Vec::new();
        let mut all_page_numbers = Vec::new();
        let thread_id = jobs.first().map(|j| j.thread_id).unwrap_or(0);
        let search_tag = jobs.first().map(|j| j.search_tag.clone()).unwrap_or_default();
        
        // Combine all posts from all jobs into a single batch
        for job in jobs {
            all_posts.extend(job.posts_batch);
            all_page_numbers.extend(job.page_numbers);
        }
        
        let original_count = all_posts.len();
        let mut filtered_count = 0;
        let mut invalid_count = 0;
        
        // Apply whitelist/blacklist filtering
        let config = Config::get();
        if config.whitelist_override_blacklist() {
            Self::apply_whitelist_blacklist_filtering(&mut all_posts, blacklist, whitelist);
        } else {
            Self::apply_blacklist_filtering(&mut all_posts, blacklist);
        }
        filtered_count = original_count.saturating_sub(all_posts.len());
        
        // Remove invalid posts (deleted, no URL, etc.)
        let before_invalid_filter = all_posts.len();
        Self::remove_invalid_posts_in_thread(&mut all_posts);
        invalid_count = before_invalid_filter.saturating_sub(all_posts.len());
        
        // Check for duplicates and filter them out
        let before_duplicate_filter = all_posts.len();
        Self::process_posts_for_duplicates_and_filenames(&mut all_posts);
        let duplicate_count = before_duplicate_filter.saturating_sub(all_posts.len());
        
        // Generate enhanced filenames for remaining posts
        // Note: This is done implicitly by the batch conversion process
        // The actual filename generation happens when converting to GrabbedPost
        
        let processing_duration = processing_start.elapsed();
        
        trace!("Batch processing completed: {} input posts -> {} output posts ({} filtered, {} invalid, {} duplicates) in {:?}",
               original_count, all_posts.len(), filtered_count, invalid_count, duplicate_count, processing_duration);
        
        ProcessedPostBatch {
            posts: all_posts,
            page_numbers: all_page_numbers,
            thread_id,
            filtered_count,
            invalid_count,
            processing_duration,
        }
    }

    fn general_search(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
    ) {
        // Use the same parallel implementation as special_search
        self.special_search(searching_tag, posts, filtered, invalid_posts);
    }

    /// Generate whitelist from current search context
    /// This includes the tag being searched and any known priority tags
    /// Uses cache to avoid regenerating the same whitelist multiple times
    fn generate_whitelist_for_search(&self, searching_tag: &str) -> Vec<String> {
        // Use the cache to get or generate the whitelist
        self.whitelist_cache.get_or_generate(searching_tag, || {
            let mut whitelist = Vec::new();
            
            // Always add the currently searched tag to whitelist (cleaned)
            let clean_tag = searching_tag.trim();
            if !clean_tag.is_empty() {
                whitelist.push(clean_tag.to_string());
            }
            
            // Handle special tag prefixes
            if clean_tag.starts_with("artist:") {
                let artist_name = clean_tag.strip_prefix("artist:").unwrap_or(clean_tag);
                if !artist_name.is_empty() {
                    whitelist.push(artist_name.to_string());
                    // Also add "artist" category variants that might appear in tags
                    whitelist.push(format!("artist:{}", artist_name));
                }
            } else if clean_tag.starts_with("fav:") {
                let username = clean_tag.strip_prefix("fav:").unwrap_or(clean_tag);
                if !username.is_empty() {
                    // Add uploader variant and original username
                    whitelist.push(format!("uploader:{}", username));
                    whitelist.push(username.to_string());
                }
            } else if clean_tag.starts_with("pool:") {
                // For pool searches, the tag is the pool content, not necessarily whitelistable
                // But we'll keep the pool identifier just in case
                whitelist.push(clean_tag.to_string());
            } else {
                // For general tags, don't add artist prefix since these are likely content tags
                // The tag is already added to whitelist above (line 1964)
                // Only add artist variant if we have some indication this might be an artist name
                // For now, we'll be conservative and not auto-add artist prefix
            }
            
            // Remove duplicates and empty entries
            whitelist.sort();
            whitelist.dedup();
            whitelist.retain(|tag| !tag.trim().is_empty());
            
            info!("Generated whitelist for search '{}': {:?}", searching_tag, whitelist);
            whitelist
        })
    }

    fn filter_posts_with_blacklist_and_whitelist(&self, posts: &mut Vec<PostEntry>, whitelist: &Vec<String>) -> u16 {
        if self.request_sender.is_authenticated() {
            if let Some(ref blacklist) = self.blacklist {
                if let Ok(bl) = blacklist.lock() {
                    let original_len = posts.len();
                    posts.retain(|post| {
                        // Check if post contains any whitelisted tags
                        let post_tags = post.tags.clone().combine_tags();
                        let is_whitelisted = whitelist.iter().any(|whitelist_tag| {
                            post_tags.iter().any(|post_tag| post_tag == whitelist_tag)
                        });
                        
                        if is_whitelisted {
                            trace!("Post {} is whitelisted due to tag(s): {:?}, skipping blacklist", post.id, 
                                   whitelist.iter().filter(|wt| post_tags.iter().any(|pt| pt == *wt)).collect::<Vec<_>>());
                            return true;
                        }
                        
                        // Apply blacklist filter if not whitelisted
                        let mut single_post = vec![post.clone()];
                        let filtered_count = bl.filter_posts(&mut single_post);
                        filtered_count == 0 // Keep post if it wasn't filtered by blacklist
                    });
                    
                    let filtered_count = original_len - posts.len();
                    return filtered_count as u16;
                }
            }
        }
        0
    }
    
    /// Backward-compatible method that doesn't use whitelist
    fn filter_posts_with_blacklist(&self, posts: &mut Vec<PostEntry>) -> u16 {
        let empty_whitelist = Vec::new();
        self.filter_posts_with_blacklist_and_whitelist(posts, &empty_whitelist)
    }

    fn remove_invalid_posts(posts: &mut Vec<PostEntry>) -> u16 {
        let mut invalid_posts = 0;
        posts.retain(|e| {
            if !e.flags.deleted && e.file.url.is_some() {
                true
            } else {
                invalid_posts += 1;
                false
            }
        });

        Self::log_invalid_posts(&invalid_posts);
        invalid_posts
    }

    fn log_invalid_posts(invalid_posts: &u16) {
        match invalid_posts.cmp(&1) {
            Ordering::Less => {}
            Ordering::Equal => {
                trace!("A post was filtered for being invalid (due to the user not being logged in)");
                trace!("A post was filtered by e621...");
            }
            Ordering::Greater => {
                trace!("{} posts were filtered for being invalid (due to the user not being logged in)", invalid_posts);
                trace!("{} posts had to be filtered by e621/e926...", invalid_posts);
            }
        }
    }
}
