use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::{create_dir_all, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use sha2::{Sha512, Digest};
use hex::encode as hex_encode;

use dialoguer::{Confirm, Input};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use crate::e621::blacklist::Blacklist;
use crate::e621::grabber::{Grabber, Shorten};
use crate::e621::io::tag::Group;
use crate::e621::io::{Config, Login};
use crate::e621::memory::{MemoryManager, estimate_collection_memory_usage};
use crate::e621::tag_fetcher::TagFetcher;
use crate::e621::artist_fetcher::ArtistFetcher;
use smallvec::SmallVec;
use crate::e621::sender::entries::UserEntry;
use crate::e621::sender::RequestSender;

pub(crate) mod artist_fetcher;
pub(crate) mod blacklist;
pub(crate) mod grabber;
pub(crate) mod io;
pub(crate) mod memory;
pub(crate) mod sender;
pub(crate) mod tag_fetcher;
pub(crate) mod tui;

/// Helper struct to hold collection information for downloading
/// Optimized to use heap allocation and avoid excessive stack usage
#[derive(Debug)]
struct CollectionInfo {
    /// Collection name
    name: String,
    /// Collection category
    category: String,
    /// Short name for display
    short_name: String,
    /// Posts in the collection - stored in a Box to reduce stack usage
    posts: Box<Vec<grabber::GrabbedPost>>,
}

/// Memory-efficient lazy iterator for processing large collections
struct LazyCollectionProcessor<'a> {
    /// Reference to the grabber
    grabber: &'a Grabber,
    /// Collection indices to process
    collection_indices: SmallVec<usize, 8>,
    /// Current collection being processed
    current_collection_idx: usize,
    /// Current post index within the current collection
    current_post_idx: usize,
}

impl<'a> LazyCollectionProcessor<'a> {
    /// Creates a new lazy processor for the given collection indices
    fn new(grabber: &'a Grabber, indices: SmallVec<usize, 8>) -> Self {
        LazyCollectionProcessor {
            grabber,
            collection_indices: indices,
            current_collection_idx: 0,
            current_post_idx: 0,
        }
    }
    
    /// Gets the next batch of posts for processing, limiting memory usage
    fn next_batch(&mut self, batch_size: usize) -> Option<(String, SmallVec<&grabber::GrabbedPost, 32>)> {
        if self.current_collection_idx >= self.collection_indices.len() {
            return None;
        }
        
        let collection_idx = self.collection_indices[self.current_collection_idx];
        let collection = &self.grabber.posts()[collection_idx];
        let posts = collection.posts();
        
        if self.current_post_idx >= posts.len() {
            // Move to next collection
            self.current_collection_idx += 1;
            self.current_post_idx = 0;
            return self.next_batch(batch_size);
        }
        
        // Collect up to batch_size new posts
        let mut batch: SmallVec<&grabber::GrabbedPost, 32> = SmallVec::new();
        let start_idx = self.current_post_idx;
        
        for (idx, post) in posts[start_idx..].iter().enumerate() {
            if batch.len() >= batch_size {
                break;
            }
            if post.is_new() {
                batch.push(post);
            }
            self.current_post_idx = start_idx + idx + 1;
        }
        
        if batch.is_empty() {
            // No new posts in this collection, move to next
            self.current_collection_idx += 1;
            self.current_post_idx = 0;
            return self.next_batch(batch_size);
        }
        
        Some((collection.name().to_string(), batch))
    }
    
    /// Estimates the total number of new posts across all collections
    fn estimate_total_new_posts(&self) -> usize {
        self.collection_indices.iter()
            .map(|&idx| {
                self.grabber.posts()[idx].posts().iter()
                    .filter(|post| post.is_new())
                    .count()
            })
            .sum()
    }
}

use rayon::ThreadPoolBuilder;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::collections::VecDeque;
use num_cpus;

/// Represents a download job in the pipeline
#[derive(Debug, Clone)]
struct DownloadJob {
    url: String,
    file_path: PathBuf,
    post_name: String,
    file_size: i64,
    hash: Option<String>,
    save_directory: PathBuf,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: i64,
}

/// Represents a completed download ready for hashing
#[derive(Debug)]
struct DownloadedFile {
    file_path: PathBuf,
    post_name: String,
    file_size: i64,
    hash: Option<String>,
    save_directory: PathBuf,
    download_duration: Duration,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: i64,
}

/// Represents a hashing job
#[derive(Debug)]
struct HashingJob {
    file_path: PathBuf,
    post_name: String,
    file_size: i64,
    expected_hash: Option<String>,
    save_directory: PathBuf,
    download_duration: Duration,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: i64,
}

/// Represents a completed file processing result
#[derive(Debug)]
struct ProcessedFile {
    post_name: String,
    file_path: PathBuf,
    save_directory: PathBuf,
    calculated_hash: String,
    download_duration: Duration,
    hash_duration: Duration,
    file_size: i64,
    success: bool,
    error_message: Option<String>,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: i64,
}

/// Configuration for the multicore pipeline
#[derive(Debug, Clone)]
struct PipelineConfig {
    total_cores: usize,
    download_threads: usize,
    hash_threads: usize,
    download_queue_size: usize,
    hash_queue_size: usize,
}

impl PipelineConfig {
    /// Creates a new pipeline configuration based on system capabilities
    fn new() -> Self {
        let total_cores = num_cpus::get();
        let download_threads = std::cmp::max(1, total_cores / 2);
        let hash_threads = std::cmp::max(1, total_cores - download_threads);
        
        // Queue sizes based on thread counts to prevent memory overflow
        let download_queue_size = download_threads * 4; // 4 jobs per thread
        let hash_queue_size = hash_threads * 4; // 4 jobs per thread
        
        PipelineConfig {
            total_cores,
            download_threads,
            hash_threads,
            download_queue_size,
            hash_queue_size,
        }
    }
    
    /// Adjusts the configuration based on user concurrency settings
    fn with_user_concurrency(mut self, user_concurrency: usize) -> Self {
        // Respect user's total concurrency preference but split intelligently
        let total_requested = user_concurrency;
        self.download_threads = std::cmp::max(1, total_requested / 2);
        self.hash_threads = std::cmp::max(1, total_requested - self.download_threads);
        
        // Adjust queue sizes accordingly
        self.download_queue_size = self.download_threads * 4;
        self.hash_queue_size = self.hash_threads * 4;
        
        self
    }
}

impl CollectionInfo {
    /// Creates a new CollectionInfo from a PostCollection
    fn from_collection(collection: &grabber::PostCollection) -> Self {
        CollectionInfo {
            name: collection.name().to_string(),
            category: collection.category().to_string(),
            short_name: collection.shorten("...").to_string(),
            posts: Box::new(collection.posts().clone()),
        }
    }

    /// Returns the number of new posts (not previously downloaded)
    fn new_files_count(&self) -> usize {
        self.posts.iter().filter(|post| post.is_new()).count()
    }
    /// Returns a slice of posts to avoid cloning large vectors
    fn posts(&self) -> &[grabber::GrabbedPost] {
        &self.posts
    }

    /// Returns a mutable slice of posts
    /// Kept for API completeness
    #[allow(dead_code)]
    fn posts_mut(&mut self) -> &mut [grabber::GrabbedPost] {
        &mut self.posts
    }
}

/// A web connector that manages how the API is called (through the [RequestSender]), how posts are grabbed
pub(crate) struct E621WebConnector {
    /// The sender used for all API calls.
    request_sender: RequestSender,
    /// Progress bar that displays the current progress in downloading posts.
    progress_bar: ProgressBar,
    /// Grabber which is responsible for grabbing posts.
    grabber: Grabber,
    /// The user's blacklist.
    blacklist: Arc<Mutex<Blacklist>>,
    /// Maximum file size cap for individual files in KB (default 20GB).
    file_size_cap: u64,
    /// Total overall download size cap in KB (default 100GB).
    total_size_cap: u64,
    /// Number of collections to download simultaneously (default 2).
    batch_size: usize,
    /// Number of simultaneous downloads (concurrency level)
    max_download_concurrency: usize,
    /// Current batch number being processed
    current_batch: usize,
    /// Total number of batches
    total_batches: usize,
    /// Memory manager for optimizing batch sizes and concurrency
    memory_manager: MemoryManager,
    /// Global progress counter that tracks total files downloaded across all collections
    global_progress_counter: Arc<AtomicUsize>,
}

impl E621WebConnector {
    /// Robust confirmation prompt that works in both standalone terminals and IDE environments
    /// Falls back to console input when dialoguer fails due to TTY detection issues
    fn robust_confirm_prompt(prompt: &str, default: bool) -> Result<bool, anyhow::Error> {
        // First try dialoguer (works in real terminals)
        match Confirm::new()
            .with_prompt(prompt)
            .show_default(true)
            .default(default)
            .interact()
        {
            Ok(result) => Ok(result),
            Err(dialoguer_err) => {
                // dialoguer failed (likely due to TTY detection), fall back to console input
                use console::Term;
                use std::io::Write;
                
                let term = Term::stdout();
                let default_str = if default { "Y/n" } else { "y/N" };
                
                // Show the prompt with default indication
                print!("{} [{}]: ", prompt, default_str);
                std::io::stdout().flush()?;
                
                // Read line from terminal
                match term.read_line() {
                    Ok(input) => {
                        let input = input.trim().to_lowercase();
                        let result = match input.as_str() {
                            "y" | "yes" => true,
                            "n" | "no" => false,
                            "" => default, // Empty input uses default
                            _ => {
                                // Invalid input, use default and inform user
                                println!("Invalid input '{}', using default: {}", input, default);
                                default
                            }
                        };
                        
                        // Echo the result back to confirm
                        if input.is_empty() {
                            println!("{}", if result { "yes" } else { "no" });
                        }
                        
                        Ok(result)
                    },
                    Err(console_err) => {
                        // Both dialoguer and console failed, return the original dialoguer error
                        // but log both errors for debugging
                        warn!("dialoguer failed: {}", dialoguer_err);
                        warn!("console fallback also failed: {}", console_err);
                        Err(anyhow::anyhow!("Failed to get user input: {}", dialoguer_err))
                    }
                }
            }
        }
    }
    
    /// Robust input prompt that works in both standalone terminals and IDE environments
    /// Falls back to console input when dialoguer fails due to TTY detection issues
    fn robust_input_prompt<T>(prompt: &str, default: T) -> Result<T, anyhow::Error> 
    where 
        T: Clone + std::str::FromStr + std::fmt::Display,
        T::Err: std::fmt::Display,
    {
        // First try dialoguer (works in real terminals)
        match Input::<T>::new()
            .with_prompt(prompt)
            .default(default.clone())
            .interact()
        {
            Ok(result) => Ok(result),
            Err(dialoguer_err) => {
                // dialoguer failed (likely due to TTY detection), fall back to console input
                use console::Term;
                use std::io::Write;
                
                let term = Term::stdout();
                
                // Show the prompt with default indication
                print!("{} [{}]: ", prompt, default);
                std::io::stdout().flush()?;
                
                // Read line from terminal
                match term.read_line() {
                    Ok(input) => {
                        let input = input.trim();
                        if input.is_empty() {
                            // Empty input uses default - echo the default value
                            println!("{}", default);
                            Ok(default)
                        } else {
                            // Try to parse the input
                            match input.parse::<T>() {
                                Ok(parsed) => Ok(parsed),
                                Err(parse_err) => {
                                    // Invalid input, use default and inform user
                                    println!("Invalid input '{}' ({}), using default: {}", input, parse_err, default);
                                    Ok(default)
                                }
                            }
                        }
                    },
                    Err(console_err) => {
                        // Both dialoguer and console failed, return the original dialoguer error
                        warn!("dialoguer failed: {}", dialoguer_err);
                        warn!("console fallback also failed: {}", console_err);
                        Err(anyhow::anyhow!("Failed to get user input: {}", dialoguer_err))
                    }
                }
            }
        }
    }
    
    /// Creates instance of `Self` for grabbing and downloading posts.
    pub(crate) fn new(request_sender: &RequestSender) -> Self {
        // Default caps: 20GB per file, 100GB total
        let default_file_cap = 20 * 1024 * 1024; // 20GB in KB
        let default_total_cap = 100 * 1024 * 1024; // 100GB in KB

        E621WebConnector {
            request_sender: request_sender.clone(),
            progress_bar: ProgressBar::hidden(),
            grabber: Grabber::new(request_sender.clone(), false),
            blacklist: Arc::new(Mutex::new(Blacklist::new(request_sender.clone()))),
            file_size_cap: default_file_cap,
            total_size_cap: default_total_cap,
            batch_size: 2, // Default to 2 collections at a time
            max_download_concurrency: 3, // Default concurrency level
            current_batch: 0,
            total_batches: 0,
            memory_manager: MemoryManager::new(),
            global_progress_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Asks the user for file size limits and sets them in the connector.
    pub(crate) fn configure_size_limits(&mut self) {
        // Default is 20GB per file
        let default_file_size_gb = self.file_size_cap / 1024 / 1024;
        // Default is 100GB total
        let default_total_size_gb = self.total_size_cap / 1024 / 1024;

        let file_size_prompt = format!("Maximum size for individual files in GB (default: {}GB)", default_file_size_gb);
        let file_size_gb: u64 = Self::robust_input_prompt(&file_size_prompt, default_file_size_gb)
            .unwrap_or_else(|err| {
                warn!("Failed to get file size input: {}", err);
                warn!("Using default file size limit: {}GB", default_file_size_gb);
                default_file_size_gb
            });

        let total_size_prompt = format!("Maximum total download size in GB (default: {}GB)", default_total_size_gb);
        let total_size_gb: u64 = Self::robust_input_prompt(&total_size_prompt, default_total_size_gb)
            .unwrap_or_else(|err| {
                warn!("Failed to get total size input: {}", err);
                warn!("Using default total size limit: {}GB", default_total_size_gb);
                default_total_size_gb
            });

        // Convert GB to KB and store
        self.file_size_cap = file_size_gb * 1024 * 1024;
        self.total_size_cap = total_size_gb * 1024 * 1024;

        info!("File size limits set: {}GB per file, {}GB total", file_size_gb, total_size_gb);
    }

    /// Asks the user to configure max pages to search and saves to config file
    pub(crate) fn configure_max_pages_to_search(&self) {
        let current_max_pages = Config::get().max_pages_to_search();
        
        // Provide information about max pages to search
        println!("\nMaximum pages to search controls how deep the search goes for each tag.");
        println!("Recommended page limits:");
        println!("   - Quick (1-3 pages): Fast search, only most recent/popular posts");
        println!("   - Standard (5-10 pages): Good balance, finds most relevant content");
        println!("   - Deep (15-25 pages): Thorough search, finds older content");
        println!("   - Exhaustive (50+ pages): Complete search, may take a long time");
        println!("The system will automatically choose optimal thread count based on pages.");
        
        let pages_prompt = format!("Maximum pages to search per tag (default: {})", current_max_pages);

        let new_max_pages: usize = Self::robust_input_prompt(&pages_prompt, current_max_pages)
            .unwrap_or_else(|err| {
                warn!("Failed to get max pages input: {}", err);
                warn!("Using default max pages: {}", current_max_pages);
                current_max_pages
            });

        // Ensure minimum of 1
        let new_max_pages = new_max_pages.max(1);

        Config::update_max_pages_to_search(new_max_pages)
            .unwrap_or_else(|err| {
                warn!("Failed to update config with new max pages: {}", err);
            });

        info!("Max pages to search configuration complete.");

        // Calculate and show the optimal thread count that will be used
        let optimal_threads = Self::calculate_optimal_thread_count(new_max_pages);
        
        // Inform the user with appropriate messaging
        if new_max_pages <= 5 {
            println!("Max pages set to: {} (Quick search - {} threads)", new_max_pages, optimal_threads);
        } else if new_max_pages <= 15 {
            println!("Max pages set to: {} (Standard search - {} threads)", new_max_pages, optimal_threads);
        } else if new_max_pages <= 30 {
            println!("WARNING: Max pages set to: {} (Deep search - {} threads)", new_max_pages, optimal_threads);
        } else {
            println!("CAUTION: Max pages set to: {} (Exhaustive search - {} threads)", new_max_pages, optimal_threads);
        }
    }
    
    /// Asks the user to select download mode and manage tags or artists accordingly
    pub(crate) fn configure_download_mode(&self) {
        println!("\nDownload Mode Selection");
        println!("Choose how you want to download content:");
        println!("1. Download by Tags (Search by tags, pools, sets, and individual posts)");
        println!("2. Download by Artists (Download posts from specific artists)");
        println!("3. Download by Both (Manage both tags and artists)");
        
        let selection = loop {
            let input: String = dialoguer::Input::new()
                .with_prompt("Select download mode (1-3):")
                .interact_text()
                .unwrap_or_else(|_| "1".to_string());
            
            if let Ok(num) = input.trim().parse::<usize>() {
                if num > 0 && num <= 3 {
                    break num;
                }
            }
            println!("Invalid selection. Please enter a number between 1 and 3");
        };
        
        match selection {
            1 => {
                // Tags only
                println!("\nSelected: Download by Tags");
                self.configure_tag_fetching();
            },
            2 => {
                // Artists only
                println!("\nSelected: Download by Artists");
                self.configure_artist_fetching();
            },
            3 => {
                // Both
                println!("\nSelected: Download by Both Tags and Artists");
                self.configure_tag_fetching();
                self.configure_artist_fetching();
            },
            _ => {
                // Default to tags
                println!("\nDefaulting to: Download by Tags");
                self.configure_tag_fetching();
            }
        }
    }
    
    /// Asks the user about tag fetching and manages tags.txt file
    pub(crate) fn configure_tag_fetching(&self) {
        println!("\nTag Management Options");
        
        let tag_options = vec![
            "Interactive Tag Management (Search, discover, and curate tags)",
            "Auto-fetch Popular Tags (Quick setup with popular non-blacklisted tags)",
            "Skip tag setup (Use existing tags.txt or create manually later)"
        ];
        
        println!("How would you like to manage your tags?");
        for (i, option) in tag_options.iter().enumerate() {
            println!("{}. {}", i + 1, option);
        }
        
        let selection = loop {
            let input: String = dialoguer::Input::new()
                .with_prompt(&format!("Select option (1-{}):", tag_options.len()))
                .interact_text()
                .unwrap_or_else(|_| "3".to_string());
            
            if let Ok(num) = input.trim().parse::<usize>() {
                if num > 0 && num <= tag_options.len() {
                    break num - 1;
                }
            }
            println!("Invalid selection. Please enter a number between 1 and {}", tag_options.len());
        };
        
        match selection {
            0 => {
                // Interactive mode
                let mut tag_fetcher = TagFetcher::new(self.request_sender.clone());
                match tag_fetcher.interactive_tag_management() {
                    Ok(()) => {
                        println!("Interactive tag management completed successfully.");
                    },
                    Err(e) => {
                        warn!("Failed during interactive tag management: {}", e);
                        println!("Interactive tag management failed. You can try again later or create tags.txt manually.");
                    }
                }
                return;
            },
            1 => {
                // Auto-fetch mode (existing logic)
                // Check if tags.txt already exists
                if TagFetcher::tags_file_exists() {
                    println!("\nFound existing tags.txt file.");
                    
                    let refresh_tags = Self::robust_confirm_prompt(
                        "Would you like to refresh tags.txt with popular non-blacklisted tags from e621?",
                        false
                    ).unwrap_or(false);
                    
                    if !refresh_tags {
                        info!("Using existing tags.txt file.");
                        return;
                    }
                } else {
                    println!("\nNo tags.txt file found.");
                    
                    let create_tags = Self::robust_confirm_prompt(
                        "Would you like to fetch popular non-blacklisted tags from e621 and create tags.txt?",
                        true
                    ).unwrap_or(true);
                    
                    if !create_tags {
                        info!("Skipping tag fetching. You can manually create tags.txt with your desired tags.");
                        return;
                    }
                }
            },
            _ => {
                // Skip
                info!("Skipping tag setup. You can manually create tags.txt with your desired tags.");
                return;
            }
        }
        
        // Get user preferences for tag fetching
        println!("\nTag fetching options:");
        
        let tag_count_prompt = "Number of popular tags to fetch (default: 100)";
        let tag_count: usize = Self::robust_input_prompt(&tag_count_prompt, 100)
            .unwrap_or_else(|err| {
                warn!("Failed to get tag count input: {}", err);
                warn!("Using default tag count: 100");
                100
            });
        
        let min_posts_prompt = "Minimum post count per tag (default: 1000)";
        let min_posts: i32 = Self::robust_input_prompt(&min_posts_prompt, 1000)
            .unwrap_or_else(|err| {
                warn!("Failed to get min posts input: {}", err);
                warn!("Using default min posts: 1000");
                1000
            });
        
        // Create tag fetcher (it will fetch user blacklist internally)
        let mut tag_fetcher = TagFetcher::new(self.request_sender.clone());
        
        // Fetch and save tags
        match tag_fetcher.refresh_tags(tag_count, min_posts) {
            Ok(()) => {
                println!("Successfully fetched and saved {} popular tags to tags.txt", tag_count);
                info!("Tag fetching completed successfully.");
            },
            Err(e) => {
                warn!("Failed to fetch tags: {}", e);
                println!("Failed to fetch tags. You can manually create tags.txt or try again later.");
            }
        }
    }
    
    /// Asks the user about artist fetching and manages artists.json file
    pub(crate) fn configure_artist_fetching(&self) {
        println!("\nArtist Management Options");
        
        let artist_options = vec![
            "Interactive Artist Management (Search, discover, and curate artists)",
            "Auto-fetch Popular Artists (Quick setup with popular artists)",
            "Skip artist setup (Use existing artists.json or create manually later)"
        ];
        
        println!("How would you like to manage your artists?");
        for (i, option) in artist_options.iter().enumerate() {
            println!("{}. {}", i + 1, option);
        }
        
        let selection = loop {
            let input: String = dialoguer::Input::new()
                .with_prompt(&format!("Select option (1-{}):", artist_options.len()))
                .interact_text()
                .unwrap_or_else(|_| "3".to_string());
            
            if let Ok(num) = input.trim().parse::<usize>() {
                if num > 0 && num <= artist_options.len() {
                    break num - 1;
                }
            }
            println!("Invalid selection. Please enter a number between 1 and {}", artist_options.len());
        };
        
        match selection {
            0 => {
                // Interactive mode
                let mut artist_fetcher = ArtistFetcher::new(self.request_sender.clone());
                match artist_fetcher.interactive_artist_management() {
                    Ok(()) => {
                        println!("Interactive artist management completed successfully.");
                    },
                    Err(e) => {
                        warn!("Failed during interactive artist management: {}", e);
                        println!("Interactive artist management failed. You can try again later or create artists.json manually.");
                    }
                }
                return;
            },
            1 => {
                // Auto-fetch mode
                // Check if artists.json already exists
                if ArtistFetcher::artists_file_exists() {
                    println!("\nFound existing artists.json file.");
                    
                    let refresh_artists = Self::robust_confirm_prompt(
                        "Would you like to refresh artists.json with popular artists from e621?",
                        false
                    ).unwrap_or(false);
                    
                    if !refresh_artists {
                        info!("Using existing artists.json file.");
                        return;
                    }
                } else {
                    println!("\nNo artists.json file found.");
                    
                    let create_artists = Self::robust_confirm_prompt(
                        "Would you like to fetch popular artists from e621 and create artists.json?",
                        true
                    ).unwrap_or(true);
                    
                    if !create_artists {
                        info!("Skipping artist fetching. You can manually create artists.json with your desired artists.");
                        return;
                    }
                }
            },
            _ => {
                // Skip
                info!("Skipping artist setup. You can manually create artists.json with your desired artists.");
                return;
            }
        }
        
        // Get user preferences for artist fetching
        println!("\nArtist fetching options:");
        
        let artist_count_prompt = "Number of popular artists to fetch (default: 50)";
        let artist_count: usize = Self::robust_input_prompt(&artist_count_prompt, 50)
            .unwrap_or_else(|err| {
                warn!("Failed to get artist count input: {}", err);
                warn!("Using default artist count: 50");
                50
            });
        
        let min_posts_prompt = "Minimum post count per artist (default: 500)";
        let min_posts: i32 = Self::robust_input_prompt(&min_posts_prompt, 500)
            .unwrap_or_else(|err| {
                warn!("Failed to get min posts input: {}", err);
                warn!("Using default min posts: 500");
                500
            });
        
        // Create artist fetcher
        let mut artist_fetcher = ArtistFetcher::new(self.request_sender.clone());
        
        // Fetch and save artists
        match artist_fetcher.refresh_artists(artist_count, min_posts) {
            Ok(()) => {
                println!("Successfully fetched and saved {} popular artists to artists.json", artist_count);
                info!("Artist fetching completed successfully.");
            },
            Err(e) => {
                warn!("Failed to fetch artists: {}", e);
                println!("Failed to fetch artists. You can manually create artists.json or try again later.");
            }
        }
    }
    
    /// Calculate optimal thread count based on the number of pages to search
    fn calculate_optimal_thread_count(max_pages: usize) -> usize {
        match max_pages {
            1..=2 => 1,          // Single page searches don't need parallelism
            3..=5 => 2,          // Small searches use minimal threads
            6..=10 => 3,         // Standard searches use conservative threading
            11..=20 => 4,        // Medium searches use moderate threading
            21..=50 => 5,        // Large searches use higher threading
            _ => 6,              // Very large searches max out at 6 threads
        }
    }
    
    /// Asks the user to configure the batch size for downloads.
    pub(crate) fn configure_batch_size(&mut self) {
        let batch_size_prompt = format!("Number of collections to download simultaneously (default: {})", self.batch_size);
        let batch_size: usize = Self::robust_input_prompt(&batch_size_prompt, self.batch_size)
            .unwrap_or_else(|err| {
                warn!("Failed to get batch size input: {}", err);
                warn!("Using default batch size: {}", self.batch_size);
                self.batch_size
            });

        // Set the batch size (this was being overridden before)
        self.batch_size = batch_size.max(1); // Ensure at least 1
        info!("Batch size set to: {}", self.batch_size);

        // Configure download concurrency with more options
        println!("\nConcurrency controls how many files can be downloaded simultaneously.");
        println!("Available concurrency levels:");
        println!("   - Conservative (3): Safe for most connections, minimal API stress");
        println!("   - Standard (5): Good balance of speed and reliability");
        println!("   - Aggressive (8): Fast downloads, moderate API stress");
        println!("   - Maximum (12+): Fastest possible, high API stress risk");
        println!("WARNING: Higher concurrency may improve download speed but risks hitting e621's API rate limits.");
        println!("    This could result in temporary IP blocks or throttled connections.");

        let concurrency_options = &[
            "Conservative (3 downloads)",
            "Standard (5 downloads)",
            "Aggressive (8 downloads)",
            "Maximum (12 downloads)",
            "Ultra (16 downloads)",
            "Custom (specify your own)",
            "Override safeguards (20+ downloads)"
        ];

        let selection = dialoguer::Select::new()
            .with_prompt("Select concurrency level")
            .default(1) // Default to Standard
            .items(concurrency_options)
            .interact()
            .unwrap_or(1); // Fallback to Standard if dialog fails

        match selection {
            0 => {
                self.max_download_concurrency = 3;
                info!("Conservative concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            1 => {
                self.max_download_concurrency = 5;
                info!("Standard concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            2 => {
                self.max_download_concurrency = 8;
                info!("Aggressive concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            3 => {
                self.max_download_concurrency = 12;
                info!("Maximum concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            4 => {
                self.max_download_concurrency = 16;
                info!("Ultra concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            5 => {
                // Custom concurrency
                let custom_concurrency_prompt = "Enter custom concurrency level (4-20)";
                let custom_concurrency: usize = Self::robust_input_prompt(&custom_concurrency_prompt, 8)
                    .unwrap_or_else(|err| {
                        warn!("Failed to get custom concurrency input: {}", err);
                        warn!("Using default custom concurrency: 8");
                        8
                    });

                self.max_download_concurrency = custom_concurrency.clamp(4, 20);
                info!("Custom concurrency: {} simultaneous downloads", self.max_download_concurrency);
            },
            6 => {
                // Override safeguards
                println!("\nWARNING: You are about to override safety limits!");
                println!("This may cause:");
                println!("   - IP blocking from e621");
                println!("   - Connection timeouts");
                println!("   - Download failures");
                println!("   - System instability");

                let confirm_override = Self::robust_confirm_prompt(
                    "Are you sure you want to override safety limits?",
                    false
                ).unwrap_or(false);

                if confirm_override {
                    let extreme_concurrency_prompt = "Enter extreme concurrency level (WARNING: 20-50+)";
                    let extreme_concurrency: usize = Self::robust_input_prompt(&extreme_concurrency_prompt, 20)
                        .unwrap_or_else(|err| {
                            warn!("Failed to get extreme concurrency input: {}", err);
                            warn!("Using default extreme concurrency: 20");
                            20
                        });

                    self.max_download_concurrency = extreme_concurrency.max(20); // No upper limit when overriding
                    warn!("OVERRIDE: Extreme concurrency enabled: {} simultaneous downloads", self.max_download_concurrency);
                    warn!("WARNING: You are responsible for any consequences of this setting!");
                } else {
                    // User cancelled override, use aggressive instead
                    self.max_download_concurrency = 8;
                    info!("Override cancelled. Using aggressive concurrency: {} simultaneous downloads", self.max_download_concurrency);
                }
            },
            _ => {
                // Fallback to standard
                self.max_download_concurrency = 5;
                info!("Fallback to standard concurrency: {} simultaneous downloads", self.max_download_concurrency);
            }
        }
    }

    /// Initializes the progress bar with a fresh instance for downloads.
    ///
    /// # Arguments
    ///
    /// * `len`: Length of the progress bar.
    #[allow(dead_code)]
    fn initialize_progress_bar(&mut self, len: u64) {
        // Use a very minimal template to avoid any stack overflow risks
        // Simple fields only, no complex formatting
        const PROGRESS_TEMPLATE: &str = "{spinner} {bar:40} {pos}/{len}";

        // Create a progress bar with minimal formatting to reduce stack usage
        let progress_style = ProgressStyle::default_bar()
            .template(PROGRESS_TEMPLATE)
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=>-");

        // Create a minimal progress bar with less frequent updates to reduce overhead
        self.progress_bar = ProgressBar::new(len);
        self.progress_bar.set_style(progress_style);
        self.progress_bar.set_draw_target(ProgressDrawTarget::stderr_with_hz(5)); // Reduce to 5 refreshes per second
        self.progress_bar.enable_steady_tick(Duration::from_millis(200)); // Less frequent updates to reduce stack pressure

        // Set initial batch information
        if self.total_batches > 0 {
            self.progress_bar.set_prefix("Processing"); // Shorter prefix for better alignment
            self.update_batch_info();
        } else {
            self.progress_bar.set_prefix("Downloading"); // Default prefix
            self.progress_bar.set_message("Initializing...".to_string());
        }
    }

    /// Updates the batch information display in the progress bar
    fn update_batch_info(&self) {
        // Format with fixed width to ensure alignment
        let batch_info = format!("Batch {:>2}/{:<2}", self.current_batch, self.total_batches);
        self.progress_bar.set_message(batch_info);
    }

    /// Gets input and enters safe depending on user choice.
    pub(crate) fn should_enter_safe_mode(&mut self) {
        trace!("Prompt for safe mode...");
        
        let confirm_prompt = Self::robust_confirm_prompt("Should enter safe mode?", false)
            .unwrap_or_else(|err| {
                warn!("Failed to setup confirmation prompt: {}", err);
                warn!("Defaulting to normal mode (safe mode: false) due to prompt failure.");
                false // Default to false if the prompt fails
            });

        trace!("Safe mode decision: {confirm_prompt}");
        if confirm_prompt {
            info!("Entering safe mode as requested.");
            self.request_sender.update_to_safe();
            self.grabber.set_safe_mode(true);
        } else {
            info!("Continuing in normal mode.");
        }
    }

    /// Processes the blacklist and tokenizes for use when grabbing posts.
    pub(crate) fn process_blacklist(&mut self) {
        let username = Login::get().username();
        let user: UserEntry = self
            .request_sender
            .get_entry_from_appended_id(username, "user");
        if let Some(blacklist_tags) = user.blacklisted_tags {
            if !blacklist_tags.is_empty() {
                if let Ok(mut bl) = self.blacklist.lock() {
                    bl.parse_blacklist(blacklist_tags)
                        .cache_users();
                } else {
                    error!("Blacklist mutex is poisoned");
                }
                self.grabber.set_blacklist(self.blacklist.clone());
            }
        }
    }

    /// Creates `Grabber` and grabs all posts before returning a tuple containing all general posts and single posts
    /// (posts grabbed by its ID).
    ///
    /// # Arguments
    ///
    /// * `groups`: The groups to grab from.
    pub(crate) fn grab_all(&mut self, groups: &[Group]) {
        trace!("Grabbing posts...");
        self.grabber.grab_favorites();
        self.grabber.grab_posts_by_tags(groups);
        
        // Also grab by artists if any are configured
        if let Ok(artists) = crate::e621::io::artist::parse_artist_file(&self.request_sender) {
            if !artists.is_empty() {
                self.grabber.grab_posts_by_artists(&artists);
            }
        }
    }

    /// Removes invalid characters from directory path.
    ///
    /// # Arguments
    ///

    /// Formats file size in KB to a human-readable string with appropriate units
    /// Formats file size in bytes to a human-readable string with appropriate units
    fn format_file_size(&self, size_bytes: u64) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;

        let size = size_bytes as f64;

        if size >= GB {
            // Size in GB
            format!("{:.2} GB", size / GB)
        } else if size >= MB {
            // Size in MB
            format!("{:.2} MB", size / MB)
        } else if size >= KB {
            // Size in KB
            format!("{:.2} KB", size / KB)
        } else {
            // Size in bytes
            format!("{} bytes", size_bytes)
        }
    }
    /// Original SHA-512 hash calculation method - kept for reference
    /// This method can cause stack overflow with large buffers on the stack
    #[allow(dead_code)]
    fn calculate_sha512(&self, file_path: &Path) -> Result<String, anyhow::Error> {
        // Open the file
        let mut file = File::open(file_path)?;

        // Read the file in chunks and update the hasher
        let mut hasher = Sha512::new();
        let mut buffer = [0; 1024 * 1024]; // 1MB buffer for reading

        loop {
            let bytes_read = file.read(&mut buffer)?;

            if bytes_read == 0 {
                break; // End of file
            }

            hasher.update(&buffer[..bytes_read]);
        }

        // Finalize the hash and convert to hex string
        let hash = hasher.finalize();
        Ok(hex_encode(hash))
    }


    /// Downloads tuple of general posts and single posts.
    pub(crate) fn download_posts(&mut self) {
        // Calculate the total file size first
        let length = self.get_total_file_size();
        trace!("Total file size for all images grabbed is {length}KB");

        // First check if we have any collections to download
        {
            let collections = self.grabber.posts();
            if collections.is_empty() {
                info!("No collections to download.");
                return;
            }
        }

        // Initialize progress bar early
        // Determine approximate post count for memory estimation
        // We need to separate the post counting from the processing
        let (total_post_count, approx_total_posts, total_collections) = {
            let collections = self.grabber.posts();

            // Count total posts
            let approx_total: usize = collections.iter()
                .map(|collection| collection.posts().len())
                .sum();

            // Get total collection count
            let total_cols = collections.len();

            // Memory safety check - if we have an extremely large number of posts,
            // we'll need to count the actual new files
            const MAX_POSTS_PER_BATCH: usize = 1000; // Adjust based on testing

            let new_post_count = if approx_total > MAX_POSTS_PER_BATCH * 2 {
                // For very large downloads, count manually
                info!("Large download detected ({} posts). Analyzing in memory-efficient mode.", approx_total);

                // Count new posts for each collection
                let mut count = 0;
                for collection in collections {
                    // For each collection, count new files
                    for post in collection.posts() {
                        if post.is_new() {
                            count += 1;
                        }
                    }
                }
                count
            } else {
                // For smaller collections, we'll count later
                0
            };

            (new_post_count, approx_total, total_cols)
        };

        // Configure batch information
        self.total_batches = (total_collections + self.batch_size - 1) / self.batch_size;

        // Check if we should use memory-efficient mode
        
        let estimated_memory_per_post = estimate_collection_memory_usage(1, 1024 * 1024);  // Average 1MB per post
        
        let avg_posts_per_collection = if total_collections > 0 {
            approx_total_posts / total_collections
        } else {
            1
        };
        
        let batch_size_recommendation = self.memory_manager.calculate_optimal_batch_size(
            total_collections,
            avg_posts_per_collection,
            estimated_memory_per_post,
        );
        
        self.batch_size = batch_size_recommendation.batch_size;
        
        // Provide feedback about memory optimization
        if batch_size_recommendation.memory_info.is_warning() {
            warn!("High memory usage detected ({}). Using smaller batch sizes for safety.", 
                  batch_size_recommendation.memory_info.format_usage());
        }
        
        info!("Optimized batch size: {} collections per batch (reasoning: {})", 
              self.batch_size, 
              match batch_size_recommendation.reasoning {
                  crate::e621::memory::BatchSizeReasoning::MemoryLimited => "memory limited",
                  crate::e621::memory::BatchSizeReasoning::CpuLimited => "CPU limited",
              });
        
        // Process collections
        if approx_total_posts > self.batch_size * 2 {
            // For very large downloads, we'll process one collection at a time
            // Get the actual total post count we calculated earlier
            let post_count = total_post_count;

            // Initialize progress bar for large downloads
            self.progress_bar = ProgressBar::new(post_count as u64);
            
            // Configure progress bar to show file counts and download size
            let total_size_formatted = self.format_file_size(length * 1024); // Convert KB to bytes for formatting
            let progress_style = ProgressStyle::default_bar()
                .template(&format!("{{spinner:.green}} [{{elapsed_precise}}] [{{bar:40.cyan/blue}}] {{pos}}/{{len}} files ({}) - {{msg}}", total_size_formatted))
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("#>-");
            self.progress_bar.set_style(progress_style);
            
            // Enable the progress bar to be displayed during downloads
            self.progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));

            // Confirm large download with the user
            if !self.confirm_large_download(length, post_count) {
                info!("Download cancelled by user.");
                return;
            }

            info!("Processing {} collections in {} batches of up to {} collections each",
                  total_collections, self.total_batches, self.batch_size);

            // Process collections one at a time to minimize memory usage
            for batch_idx in 0..self.total_batches {
                self.current_batch = batch_idx + 1;
                self.update_batch_info();

                let start_idx = batch_idx * self.batch_size;
                let end_idx = (start_idx + self.batch_size).min(total_collections);

                // Process each collection in this batch
                for idx in start_idx..end_idx {
                    // Get collection info within a new scope to limit the borrow
                    let collection_info = {
                        let collection = &self.grabber.posts()[idx];
                        let name = collection.name().to_string();

                        info!("Processing batch {}/{}: {}",
                             self.current_batch, self.total_batches, name);

                        // Create CollectionInfo for just this collection
                        CollectionInfo::from_collection(collection)
                    };

                    self.progress_bar.set_message(format!("Processing {}...", collection_info.short_name));

                    // Process this collection
                    self.download_single_collection(&collection_info);

                    // Force memory cleanup
                    drop(collection_info);
                }
            }
        } else {
            // For smaller downloads, we can create all CollectionInfo objects at once
            // We need to collect all CollectionInfo objects first to avoid borrow issues
            let collection_infos: Vec<CollectionInfo> = {
                let collections = self.grabber.posts();
                collections.iter()
                    .map(CollectionInfo::from_collection)
                    .collect()
            };

            // Now that we have the collection infos, count new posts
            let new_post_count: usize = collection_infos.iter()
                .map(|info| info.new_files_count())
                .sum();

        // Set progress bar total to exact number of files to be downloaded (never over- or under-count)
        self.progress_bar = ProgressBar::new(new_post_count as u64);
        
        // Configure progress bar to show file counts and download size
        let total_size_formatted = self.format_file_size(length * 1024); // Convert KB to bytes for formatting
        let progress_style = ProgressStyle::default_bar()
            .template(&format!("{{spinner:.green}} [{{elapsed_precise}}] [{{bar:40.cyan/blue}}] {{pos}}/{{len}} files ({}) - {{msg}}", total_size_formatted))
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-");
        self.progress_bar.set_style(progress_style);

            // Confirm with user if download size is large
            if !self.confirm_large_download(length, new_post_count) {
                info!("Download cancelled by user.");
                return;
            }

            info!("Processing {} collections in {} batches of up to {} collections each",
                  total_collections, self.total_batches, self.batch_size);

            // Process in batches
            for batch_idx in 0..self.total_batches {
                self.current_batch = batch_idx + 1;
                self.update_batch_info();

                let start_idx = batch_idx * self.batch_size;
                let end_idx = (start_idx + self.batch_size).min(collection_infos.len());

                // Create batch description
                let batch_desc = if end_idx - start_idx > 1 {
                    let collection_names: Vec<String> = collection_infos[start_idx..end_idx]
                        .iter()
                        .map(|c| c.name.clone())
                        .collect();
                    format!("Batch {}/{}: {}", self.current_batch, self.total_batches,
                            collection_names.join(", "))
                } else if end_idx > start_idx {
                    format!("Batch {}/{}: {}", self.current_batch, self.total_batches,
                            collection_infos[start_idx].name)
                } else {
                    format!("Batch {}/{}", self.current_batch, self.total_batches)
                };

                info!("Processing {}", batch_desc);
                self.progress_bar.set_message(format!("Processing batch {}...", self.current_batch));

                // Process this batch of collections
                for idx in start_idx..end_idx {
                    let collection_info = &collection_infos[idx];
                    self.download_single_collection(collection_info);
                }
            }
        }

        // Use clear formatting for final message
        self.progress_bar.set_prefix("Completed");
        self.progress_bar.finish_with_message("All downloads completed successfully");

        // Wait for user to confirm they've seen completion before continuing
        info!("Download process has completed successfully.");
    }

    /// Downloads a single collection using multicore pipeline
    fn download_single_collection(&mut self, collection_info: &CollectionInfo) {
        // Get pipeline configuration
        let pipeline_config = PipelineConfig::new().with_user_concurrency(self.max_download_concurrency);
        
        info!("Multicore pipeline configured: {} download threads, {} hash threads (total: {} cores)", 
              pipeline_config.download_threads, pipeline_config.hash_threads, pipeline_config.total_cores);
        
        // Use multicore pipeline for this collection
        self.download_collection_with_pipeline(collection_info, pipeline_config);
    }
    
    /// Downloads a collection using the multicore pipeline system
    fn download_collection_with_pipeline(&mut self, collection_info: &CollectionInfo, config: PipelineConfig) {
        // Get directory manager
        let dir_manager = match Config::get().directory_manager() {
            Ok(manager_ref) => Arc::new(Mutex::new(manager_ref.clone())), // assumes DirectoryManager: Clone
            Err(err) => {
                error!("Failed to get directory manager from configuration: {}", err);
                self.progress_bar.set_message("Error: Failed to initialize directory manager!");
                self.progress_bar.finish_with_message("Download failed: Configuration error");
                return;
            }
        };

        // Queue for download jobs and completed downloads
        let (download_tx, download_rx): (Sender<DownloadJob>, Receiver<DownloadJob>) = channel();
        let (completed_tx, completed_rx): (Sender<DownloadedFile>, Receiver<DownloadedFile>) = channel();

        // Queue for hashing jobs and completed processing
        let (hash_tx, hash_rx): (Sender<HashingJob>, Receiver<HashingJob>) = channel();
        let (processed_tx, processed_rx): (Sender<ProcessedFile>, Receiver<ProcessedFile>) = channel();
        
        // Wrap receivers in Arc<Mutex<>> for sharing across threads
        let download_rx = Arc::new(Mutex::new(download_rx));
        let hash_rx = Arc::new(Mutex::new(hash_rx));

        // Initialize job queue
        let mut download_queue = VecDeque::new();

        // Thread pool for downloads
        let download_pool = ThreadPoolBuilder::new()
            .num_threads(config.download_threads)
            .build()
            .expect("Failed to create download thread pool");

        // Thread pool for hashing
        let hash_pool = ThreadPoolBuilder::new()
            .num_threads(config.hash_threads)
            .build()
            .expect("Failed to create hash thread pool");

        // Clone shared variables
        let progress_bar = self.progress_bar.clone();
        let request_sender = self.request_sender.clone();
        let global_progress_counter = Arc::clone(&self.global_progress_counter);

        // Create cloneable functions for the threads
        let download_fn = self.clone_download_fn();
        let calculate_hash = self.clone_calculate_hash();
        
        // Spawn download threads
        let mut download_handles = Vec::new();
        for _ in 0..config.download_threads {
            let download_rx = Arc::clone(&download_rx);
            let completed_tx = completed_tx.clone();
            let download_fn = Arc::clone(&download_fn);
        
            let handle = thread::spawn(move || {
                loop {
                    let job = {
                        let rx = download_rx.lock().unwrap();
                        rx.recv()
                    };
                    
                    match job {
                        Ok(job) => {
                            // Perform download
                            let start_time = Instant::now();
                            let download_result = download_fn(&job.url, &job.file_path);
                            let duration = start_time.elapsed();
                            
                            match download_result {
                                Ok(_) => {
                                    // Send completed download
                                    let completed_file = DownloadedFile {
                                        file_path: job.file_path,
                                        post_name: job.post_name,
                                        file_size: job.file_size,
                                        hash: job.hash.clone(),
                                        save_directory: job.save_directory.clone(),
                                        download_duration: duration,
                                        short_url: job.short_url,
                                        post_id: job.post_id,
                                    };
                                    let _ = completed_tx.send(completed_file);
                                },
                                Err(_) => {
                                    // Handle download error - could log here
                                }
                            }
                        },
                        Err(_) => break, // Channel closed
                    }
                }
            });
            download_handles.push(handle);
        }

        // Spawn hashing threads
        let mut hash_handles = Vec::new();
        for _ in 0..config.hash_threads {
            let hash_rx = Arc::clone(&hash_rx);
            let processed_tx = processed_tx.clone();
            let calculate_hash = Arc::clone(&calculate_hash);

            let handle = thread::spawn(move || {
                loop {
                    let job = {
                        let rx = hash_rx.lock().unwrap();
                        rx.recv()
                    };
                    
                    match job {
                        Ok(job) => {
                            // Perform hashing
                            let start_time = Instant::now();
                            let hash_result = calculate_hash(&job.file_path);
                            let duration = start_time.elapsed();

                            let processed_file = match hash_result {
                                Ok(calculated_hash) => ProcessedFile {
                                    post_name: job.post_name,
                                    file_path: job.file_path,
                                    save_directory: job.save_directory.clone(),
                                    calculated_hash,
                                    download_duration: job.download_duration,
                                    hash_duration: duration,
                                    file_size: job.file_size,
                                    success: true,
                                    error_message: None,
                                    short_url: job.short_url,
                                    post_id: job.post_id,
                                },
                                Err(err) => ProcessedFile {
                                    post_name: job.post_name,
                                    file_path: job.file_path,
                                    save_directory: job.save_directory.clone(),
                                    calculated_hash: String::new(),
                                    download_duration: job.download_duration,
                                    hash_duration: duration,
                                    file_size: job.file_size,
                                    success: false,
                                    error_message: Some(err.to_string()),
                                    short_url: job.short_url,
                                    post_id: job.post_id,
                                },
                            };

                            let _ = processed_tx.send(processed_file);
                        },
                        Err(_) => break, // Channel closed
                    }
                }
            });
            hash_handles.push(handle);
        }

        let progress_bar = self.progress_bar.clone();

        // Use a function pointer for stateless remove_invalid_chars
        fn remove_invalid_chars(text: &str) -> String {
            text.chars()
                .map(|e| match e {
                    '?' | ':' | '*' | '<' | '>' | '"' | '|' => '_',
                    _ => e,
                })
                .collect()
        }

        // Count new files that will be downloaded
        let new_files: Vec<_> = collection_info.posts().iter().filter(|post| post.is_new()).collect();
        let total_files = new_files.len();
        
        if total_files == 0 {
            // No new files to download in this collection
            return;
        }

        info!(
            "Found {} new files to download in {}",
            total_files,
            console::style(format!("\"{}\"", collection_info.name)).color256(39).italic()
        );

        // Set progress bar prefix to current collection (limited to 15 chars for consistent width)
        let short_name = if collection_info.short_name.len() > 13 {
            format!("{}…", &collection_info.short_name[..12])
        } else {
            collection_info.short_name.clone()
        };
        self.progress_bar.set_prefix(short_name);
        
        // Prepare download jobs
        for post in &new_files {
            let save_dir = match post.save_directory() {
                Some(dir) => dir,
                None => {
                    error!(
                        "Post does not have a save directory assigned: {}",
                        post.name()
                    );
                    continue;
                }
            };
            
            let file_path = save_dir.join(remove_invalid_chars(post.name()));
            
            let download_job = DownloadJob {
                url: post.url().to_string(),
                file_path,
                post_name: post.name().to_string(),
                file_size: post.file_size_bytes(),
                hash: post.sha512_hash().map(|s| s.to_string()),
                save_directory: save_dir.clone(),
                short_url: post.short_url().map(|s| s.to_string()),
                post_id: post.post_id(),
            };
            
            download_queue.push_back(download_job);
        }
        
        // Start pipeline processing
        info!("Starting pipeline: {} jobs queued for download", download_queue.len());
        
        // Feed initial jobs to download threads
        for _ in 0..std::cmp::min(config.download_queue_size, download_queue.len()) {
            if let Some(job) = download_queue.pop_front() {
                download_tx.send(job).unwrap();
            }
        }
        
        // Pipeline coordination loop
        let mut files_completed = 0;
        let mut files_processing = 0;
        
        loop {
            // Process completed downloads
            if let Ok(downloaded_file) = completed_rx.try_recv() {
                files_processing += 1;
                
                // Create hashing job
                let hash_job = HashingJob {
                    file_path: downloaded_file.file_path,
                    post_name: downloaded_file.post_name,
                    file_size: downloaded_file.file_size,
                    expected_hash: downloaded_file.hash,
                    save_directory: downloaded_file.save_directory,
                    download_duration: downloaded_file.download_duration,
                    short_url: downloaded_file.short_url,
                    post_id: downloaded_file.post_id,
                };
                
                // Send to hash queue
                hash_tx.send(hash_job).unwrap();
                
                // Feed more download jobs if available
                if let Some(job) = download_queue.pop_front() {
                    download_tx.send(job).unwrap();
                }
            }
            
            // Process completed hashes
            if let Ok(processed_file) = processed_rx.try_recv() {
                files_completed += 1;
                files_processing -= 1;
                
                // Update progress and database
                let current = global_progress_counter.fetch_add(1, Ordering::SeqCst) + 1;
                
                if processed_file.success {
                    let speed_kbps = processed_file.file_size as f64 / 1024.0 / processed_file.download_duration.as_secs_f64();
                    
                    // Store hash with the downloaded file for future verification including URL metadata
                    let relpath = processed_file.save_directory.join(&processed_file.post_name);
                    let relpath_str = relpath
                        .strip_prefix(Config::get().download_directory())
                        .unwrap_or(&relpath)
                        .to_string_lossy();
                    let mut dm = dir_manager.lock().unwrap();
                    dm.add_or_update_entry(
                        relpath_str.to_string(),
                        Some(processed_file.calculated_hash.clone()),
                        processed_file.short_url.clone(),
                        Some(processed_file.post_id),
                    );
                    
                    progress_bar.set_message(format!("Downloaded & verified: {} ({:.2} KB/s)", processed_file.post_name, speed_kbps));
                    trace!("Stored hash {} and URL {} for post ID {}", processed_file.calculated_hash, 
                           processed_file.short_url.as_deref().unwrap_or("N/A"), processed_file.post_id);
                } else {
                    warn!("Failed to process {}: {}", processed_file.post_name, processed_file.error_message.unwrap_or_else(|| "Unknown error".to_string()));
                    progress_bar.set_message(format!("Error processing: {}", processed_file.post_name));
                }
                
                progress_bar.set_position(current as u64);
            }
            
            // Check if all files are done
            if files_completed >= total_files {
                break;
            }
            
            // Small delay to prevent busy waiting
            std::thread::sleep(Duration::from_millis(10));
        }
        
        // Wait for all threads to complete
        drop(download_tx);
        drop(hash_tx);
        
        info!("Completed downloading {} files from collection: {}", total_files, collection_info.name);
        trace!("Collection {} is finished downloading...", collection_info.name);
    }
    /// Gets the total size (in KB) of every post image to be downloaded.
    /// Shows a prompt for confirming large downloads and gives the option to adjust limits
    /// Returns true if user chooses to proceed, false if user cancels
    fn confirm_large_download(&mut self, total_size_kb: u64, post_count: usize) -> bool {
        // Define 20GB threshold in KB (20 * 1024 * 1024 = 20GB in KB)
        const THRESHOLD_KB: u64 = 20 * 1024 * 1024;

        if total_size_kb <= THRESHOLD_KB {
            // Size is below threshold, no need for confirmation
            return true;
        }

        // Format size for display (convert KB to bytes for formatting)
        let formatted_size = self.format_file_size(total_size_kb * 1024);
        

        // Display warning and options
        println!("\n⚠️  WARNING: Large download detected!");
        println!("You are about to download {} files totaling {}.", post_count, formatted_size);
        println!("This exceeds the recommended size of {}.\n", self.format_file_size(THRESHOLD_KB * 1024));

        // Create selection menu for the user
        let options = &[
            "Proceed with download",
            "Adjust size limits",
            "Cancel download"
        ];

        let selection = dialoguer::Select::new()
            .with_prompt("What would you like to do?")
            .default(0)
            .items(options)
            .interact();

        match selection {
            Ok(0) => {
                // User chose to proceed
                info!("User confirmed large download of {}", formatted_size);
                true
            },
            Ok(1) => {
                // User chose to adjust limits
                info!("Reconfiguring size limits...");
                self.configure_size_limits();

                // Check if the new limits would allow the download
                let total_size_in_gb = total_size_kb as f64 / (1024.0 * 1024.0);
                let new_limit_in_gb = self.total_size_cap as f64 / (1024.0 * 1024.0);

                if total_size_in_gb > new_limit_in_gb {
                    info!("Total download size ({:.2} GB) still exceeds configured limit ({:.2} GB)",
                          total_size_in_gb, new_limit_in_gb);

                    // Ask if they want to proceed anyway
                    let proceed_anyway = Self::robust_confirm_prompt(
                        &format!(
                            "Download size ({:.2} GB) still exceeds your limit ({:.2} GB). Proceed anyway?",
                            total_size_in_gb, new_limit_in_gb
                        ),
                        false
                    )
                    .unwrap_or_else(|err| {
                        warn!("Failed to get confirmation: {}", err);
                        warn!("Defaulting to not proceed due to prompt failure.");
                        false
                    });

                    proceed_anyway
                } else {
                    // New limits are sufficient
                    info!("Adjusted limits now accommodate the download size");
                    true
                }
            },
            Ok(2) | _ => {
                // User chose to cancel or dialog error
                info!("User cancelled download due to large file size");
                false
            }
        }
    }

    /// Asks the user for confirmation before exiting
    /// Returns true if the user wants to exit, false to continue
    pub(crate) fn confirm_exit(&self, message: &str) -> bool {
        let prompt = format!("{}\nDo you want to exit the program?", message);

        Self::robust_confirm_prompt(&prompt, true)
            .unwrap_or_else(|err| {
                warn!("Failed to get exit confirmation: {}", err);
                warn!("Defaulting to exit due to prompt failure.");
                true // Default to true (exit) if dialog fails
            })
    }

    /// Helper method to create a cloneable download function
    fn clone_download_fn(&self) -> Arc<dyn Fn(&str, &Path) -> Result<(), anyhow::Error> + Send + Sync> {
        let request_sender = self.request_sender.clone();
        
        Arc::new(move |url: &str, path: &Path| -> Result<(), anyhow::Error> {
            // Create parent directory if it doesn't exist
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Get the bytes from the URL
            let bytes = match request_sender.get_bytes_from_url(url) {
                Ok(data) => data,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to download from URL {}: {}", url, e));
                }
            };

            // Write the bytes to the file directly
            let file_path_display = path.display();
            if let Err(e) = std::fs::write(path, &bytes) {
                return Err(anyhow::anyhow!("Failed to write to file {}: {}", file_path_display, e));
            }

            Ok(())
        })
    }
    
    /// Helper method to create a cloneable hash calculation function
    fn clone_calculate_hash(&self) -> Arc<dyn Fn(&Path) -> Result<String, anyhow::Error> + Send + Sync> {
        Arc::new(|file_path: &Path| -> Result<String, anyhow::Error> {
            const LARGE_FILE_THRESHOLD: u64 = 32 * 1024 * 1024; // 32MB

            let file = File::open(file_path)?;
            let metadata = file.metadata()?;
            let file_size = metadata.len();

            // For large files, use memory mapping for better performance and memory efficiency
            if file_size > LARGE_FILE_THRESHOLD {
                // Use memory mapping for large files
                let mmap = unsafe { memmap2::Mmap::map(&file)? };

                let mut hasher = Sha512::new();
                hasher.update(&mmap[..]);
                let hash = hasher.finalize();

                Ok(hex_encode(hash))
            } else {
                // For smaller files, use heap-allocated buffers instead of stack buffers
                let mut hasher = Sha512::new();
                let mut buffer = vec![0; 1024 * 1024]; // 1MB buffer allocated on the heap
                let mut reader = &file;

                loop {
                    let bytes_read = reader.read(&mut buffer)?;

                    if bytes_read == 0 {
                        break; // End of file
                    }

                    hasher.update(&buffer[..bytes_read]);
                }

                let hash = hasher.finalize();
                Ok(hex_encode(hash))
            }
        })
    }

    /// Gets the total size (in KB) of every post image to be downloaded.
    /// Includes validation to prevent unreasonable file sizes.
    fn get_total_file_size(&self) -> u64 {
        let mut total_size: u64 = 0;
        let mut post_count: usize = 0;
        let mut capped_files: usize = 0;

        // Get collections once to avoid multiple borrows
        let collections = self.grabber.posts();

        for collection in collections.iter() {
            for post in collection.posts() {
                // Skip posts that aren't new to avoid counting duplicates
                if !post.is_new() {
                    continue;
                }

                // Get file size in bytes and convert cap from KB to bytes for comparison
                let file_size_bytes = post.file_size_bytes() as u64;
                let file_size_cap_bytes = self.file_size_cap * 1024; // Convert KB cap to bytes
                post_count += 1;

                // Validate individual file size using user's cap
                if file_size_bytes > file_size_cap_bytes {
                    let formatted_original = self.format_file_size(file_size_bytes);
                    let formatted_cap = self.format_file_size(file_size_cap_bytes);

                    warn!("Post {} has large file size: {}, capping at {}",
                          post.name(), formatted_original, formatted_cap);

                    total_size += file_size_cap_bytes;
                    capped_files += 1;
                } else {
                    total_size += file_size_bytes;
                }
            }
        }

        let formatted_size = self.format_file_size(total_size);

        if capped_files > 0 {
            info!("Preparing to download {} new files ({} will be size-capped) totaling {}",
                  post_count, capped_files, formatted_size);
        } else {
            info!("Preparing to download {} new files totaling {}", post_count, formatted_size);
        }

        // Convert bytes to KB before returning (as documented in function comment)
        total_size / 1024
    }
}

