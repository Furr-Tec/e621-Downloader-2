use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::{create_dir_all, File};
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use sha2::{Sha512, Digest};
use hex::encode as hex_encode;

use anyhow::Context;
use dialoguer::{Confirm, Input};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use crate::e621::blacklist::Blacklist;
use crate::e621::grabber::{Grabber, Shorten};
use crate::e621::io::tag::Group;
use crate::e621::io::{Config, Login};
use crate::e621::sender::entries::UserEntry;
use crate::e621::sender::RequestSender;

pub(crate) mod blacklist;
pub(crate) mod grabber;
pub(crate) mod io;
pub(crate) mod sender;
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

use rayon::ThreadPoolBuilder;
const MAX_DOWNLOAD_CONCURRENCY: usize = 3;

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
    /// Current batch number being processed
    current_batch: usize,
    /// Total number of batches
    total_batches: usize,
}

impl E621WebConnector {
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
            current_batch: 0,
            total_batches: 0,
        }
    }

    /// Asks the user for file size limits and sets them in the connector.
    pub(crate) fn configure_size_limits(&mut self) {
        // Default is 20GB per file
        let default_file_size_gb = self.file_size_cap / 1024 / 1024;

        let file_size_prompt = format!("Maximum size for individual files in GB (default: {}GB)", default_file_size_gb);
        let file_size_gb: u64 = Input::new()
            .with_prompt(&file_size_prompt)
            .default(default_file_size_gb)
            .interact()
            .unwrap_or(default_file_size_gb);

        // Default is 100GB total
        let default_total_size_gb = self.total_size_cap / 1024 / 1024;

        let total_size_prompt = format!("Maximum total download size in GB (default: {}GB)", default_total_size_gb);
        let total_size_gb: u64 = Input::new()
            .with_prompt(&total_size_prompt)
            .default(default_total_size_gb)
            .interact()
            .unwrap_or(default_total_size_gb);

        // Convert GB to KB and store
        self.file_size_cap = file_size_gb * 1024 * 1024;
        self.total_size_cap = total_size_gb * 1024 * 1024;

        info!("File size limits set: {}GB per file, {}GB total", file_size_gb, total_size_gb);
    }

    /// Asks the user to configure the batch size for downloads.
    pub(crate) fn configure_batch_size(&mut self) {
        let batch_size_prompt = format!("Number of collections to download simultaneously (default: {})", self.batch_size);
        let batch_size: usize = Input::new()
            .with_prompt(&batch_size_prompt)
            .default(self.batch_size)
            .interact()
            .unwrap_or(self.batch_size);

        self.batch_size = batch_size.max(1); // Ensure at least 1
        info!("Batch size set to: {}", self.batch_size);
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
        let confirm_prompt = Confirm::new()
            .with_prompt("Should enter safe mode?")
            .show_default(true)
            .default(false)
            .interact()
            .with_context(|| {
                error!("Failed to setup confirmation prompt!");
                "Terminal unable to set up confirmation prompt..."
            })
            .unwrap();

        trace!("Safe mode decision: {confirm_prompt}");
        if confirm_prompt {
            self.request_sender.update_to_safe();
            self.grabber.set_safe_mode(true);
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

    /// Memory-efficient SHA-512 hash calculation with memory mapping for large files
    /// This avoids stack overflow by using heap allocation and memory mapping
    fn calculate_sha512_optimized(&self, file_path: &Path) -> Result<String, anyhow::Error> {
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
        const MAX_POSTS_PER_BATCH: usize = 1000; // Keep the same threshold

        // Process collections
        if approx_total_posts > MAX_POSTS_PER_BATCH * 2 {
            // For very large downloads, we'll process one collection at a time
            // Get the actual total post count we calculated earlier
            let post_count = total_post_count;

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

    /// Downloads a single collection
    fn download_single_collection(&mut self, collection_info: &CollectionInfo) {
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
        let new_files = collection_info.new_files_count();
        if new_files == 0 {
            // No new files to download in this collection
            return;
        }

        info!(
            "Found {} new files to download in {}",
            new_files,
            console::style(format!("\"{}\"", collection_info.name)).color256(39).italic()
        );

        // Configure progress bar with proper styling and length
        self.progress_bar = ProgressBar::new(new_files as u64);
        let progress_style = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {prefix}: {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-");
        self.progress_bar.set_style(progress_style);

        // Set progress bar prefix to current collection (limited to 15 chars for consistent width)
        let short_name = if collection_info.short_name.len() > 13 {
            format!("{}…", &collection_info.short_name[..12])
        } else {
            collection_info.short_name.clone()
        };
        self.progress_bar.set_prefix(short_name);
        trace!("Collection Name:            \"{}\"", collection_info.name);
        trace!("Collection Category:        \"{}\"", collection_info.category);
        trace!("Collection Post Length:     \"{}\"", collection_info.posts.len());

        // Process each post in this collection using batches to prevent stack overflow
        let posts = collection_info.posts();

        // Detect potentially problematic collections based on size and complexity
        const LARGE_COLLECTION_THRESHOLD: usize = 50; // Collections with more than 50 files
        const LARGE_FILE_SIZE_THRESHOLD: i64 = 100 * 1024 * 1024; // 100MB in bytes

        let is_problematic = posts.len() > LARGE_COLLECTION_THRESHOLD ||
            posts.iter().any(|post| post.file_size_bytes() > LARGE_FILE_SIZE_THRESHOLD);
        let batch_size = if is_problematic {
            // Calculate total size in MB for logging
            let total_size_bytes: i64 = posts.iter().map(|p| p.file_size_bytes()).sum();
            let total_size_mb = total_size_bytes / (1024 * 1024);

            info!("Large collection detected: '{}' with {} files ({}MB total). Using smaller batch size for memory efficiency.",
                  collection_info.name, posts.len(), total_size_mb);
            5  // Very small batches for large collections
        } else {
            10 // Normal batch size for smaller collections
        };

        // Convert the post slice to a Vec of only new posts first to simplify processing 
        // Use an iterator rather than collecting to avoid extra memory allocation
        let dir_manager_arc = Arc::clone(&dir_manager);
        let progress_bar = progress_bar.clone();
        let posts: Vec<_> = posts.iter().filter(|post| post.is_new()).cloned().collect();
        let new_files = posts.len();

        // Atomic counter for thread-safe progress reporting
        let download_counter = Arc::new(AtomicUsize::new(0));

        // Extract functions from self that we'll need in threads
        let request_sender = self.request_sender.clone();

        // Create download function that doesn't capture self
        let download_fn = Arc::new(move |url: &str, path: &Path| -> Result<(), anyhow::Error> {
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
        });

        // Create hash calculation function that doesn't capture self
        let calculate_hash = Arc::new(|file_path: &Path| -> Result<String, anyhow::Error> {
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
        });
        // Declare the rayon thread pool with configured concurrency
        let pool = ThreadPoolBuilder::new()
            .num_threads(MAX_DOWNLOAD_CONCURRENCY)
            .build()
            .expect("Failed to create download thread pool");

        pool.scope(|s| {
            for post in posts.into_iter() {
                let dir_manager = Arc::clone(&dir_manager_arc);
                let progress_bar = progress_bar.clone();
                let download_counter = Arc::clone(&download_counter);
                let download_fn = Arc::clone(&download_fn);
                let calculate_hash = Arc::clone(&calculate_hash);
                s.spawn(move |_| {
                    let filename = post.name();
                    let hash = post.sha512_hash();
                    let hash_ref = hash.as_deref();
                    
                    // Helper closure for progress updates and increments
                    let update_progress = |status: &str| {
                        let count = download_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        progress_bar.set_message(format!("[{}/{}] {}: {}", count, new_files, status, filename));
                        progress_bar.inc(post.file_size() as u64);
                    };
                    
                    // Check for duplicates
                    let is_dup = {
                        let dm = dir_manager.lock().unwrap();
                        dm.is_duplicate_iterative(filename, hash_ref)
                    };
                    if is_dup {
                        update_progress("Duplicate");
                        return;
                    }
                    update_progress("Downloading");

                    let save_dir = match post.save_directory() {
                        Some(dir) => dir,
                        None => {
                            error!(
                                "Post does not have a save directory assigned: {}",
                                post.name()
                            );
                            update_progress("Error: No directory");
                            return;
                        }
                    };
                    let file_path = save_dir.join(remove_invalid_chars(post.name()));

                    if let Err(err) = create_dir_all(&save_dir) {
                        let path_str = save_dir.to_string_lossy();
                        error!("Could not create directory for images: {}", err);
                        error!("Path: {}", path_str);
                        update_progress("Error: Bad directory path");
                        return;
                    }
                    let file_path_str = file_path.to_string_lossy();
                    let download_result = download_fn(post.url(), &file_path);
                    match download_result {
                        Ok(_) => {
                            // File was downloaded and saved successfully
                            // Now calculate hash and update tracking info
                            trace!("Saved {}...", file_path_str);
                            let hash_result = calculate_hash(&file_path);
                            match hash_result {
                                Ok(hash) => {
                                    // Store hash with the downloaded file for future verification
                                    let relpath = save_dir.join(post.name());
                                    let relpath_str = relpath
                                        .strip_prefix(Config::get().download_directory())
                                        .unwrap_or(&relpath)
                                        .to_string_lossy();
                                    let mut dm = dir_manager.lock().unwrap();
                                    dm.mark_file_downloaded_with_hash_simple(&relpath_str, hash.clone());
                                    trace!("Stored hash {} for post {}", hash, post.name());
                                    // Show success message with file counts
                                    update_progress("Downloaded & verified");
                                },
                                Err(e) => {
                                    warn!("Failed to calculate hash for {}: {}", file_path_str, e);
                                    let relpath = save_dir.join(post.name());
                                    let relpath_str = relpath
                                        .strip_prefix(Config::get().download_directory())
                                        .unwrap_or(&relpath)
                                        .to_string_lossy();
                                    let mut dm = dir_manager.lock().unwrap();
                                    dm.mark_file_downloaded(&relpath_str);
                                    update_progress("Saved but not verified");
                                }
                            }
                        }
                        Err(err) => {
                            error!("Failed to download/save image {}: {}", file_path_str, err);
                            // Show error message with file counts
                            update_progress("Error: Download failed");
                        }
                    }
                }); // s.spawn
            } // for post in posts
        }); // pool.scope

        // No more batch Vec or manual batch tracking is necessary here. Clean up and trace log.
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

        // Format size for display
        let formatted_size = self.format_file_size(total_size_kb);

        // Display warning and options
        println!("\n⚠️  WARNING: Large download detected!");
        println!("You are about to download {} files totaling {}.", post_count, formatted_size);
        println!("This exceeds the recommended size of 20GB.\n");

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
                    let proceed_anyway = Confirm::new()
                        .with_prompt(format!(
                            "Download size ({:.2} GB) still exceeds your limit ({:.2} GB). Proceed anyway?",
                            total_size_in_gb, new_limit_in_gb
                        ))
                        .default(false)
                        .interact()
                        .unwrap_or(false);

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

        Confirm::new()
            .with_prompt(prompt)
            .default(true)
            .interact()
            .unwrap_or(true) // Default to true (exit) if dialog fails
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

        total_size
    }

    /// Downloads a file using a streaming approach to minimize memory usage
    /// This avoids loading the entire file into memory at once
    fn download_file_with_streaming(&self, url: &str, file_path: &Path) -> Result<(), anyhow::Error> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Get the bytes from the URL
        let bytes = match self.request_sender.get_bytes_from_url(url) {
            Ok(data) => data,
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to download from URL {}: {}", url, e));
            }
        };

        // Write the bytes to the file directly
        let file_path_display = file_path.display();
        if let Err(e) = std::fs::write(file_path, &bytes) {
            return Err(anyhow::anyhow!("Failed to write to file {}: {}", file_path_display, e));
        }

        Ok(())
    }
}

