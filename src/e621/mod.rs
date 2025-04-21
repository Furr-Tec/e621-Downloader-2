use std::cell::RefCell;
use std::fs::{create_dir_all, write};
use std::rc::Rc;
use std::time::Duration;

use anyhow::Context;
use dialoguer::{Confirm, Input};
use indicatif::{ProgressBar, ProgressDrawTarget};

use crate::e621::blacklist::Blacklist;
use crate::e621::grabber::{Grabber, Shorten};
use crate::e621::io::tag::Group;
use crate::e621::io::{Config, Login};
use crate::e621::sender::entries::UserEntry;
use crate::e621::sender::RequestSender;
use crate::e621::tui::{ProgressBarBuilder, ProgressStyleBuilder};

pub(crate) mod blacklist;
pub(crate) mod grabber;
pub(crate) mod io;
pub(crate) mod sender;
pub(crate) mod tui;

/// A web connector that manages how the API is called (through the [RequestSender]), how posts are grabbed
/// (through [Grabber]), and how the posts are downloaded.
pub(crate) struct E621WebConnector {
    /// The sender used for all API calls.
    request_sender: RequestSender,
    /// Progress bar that displays the current progress in downloading posts.
    progress_bar: ProgressBar,
    /// Grabber which is responsible for grabbing posts.
    grabber: Grabber,
    /// The user's blacklist.
    blacklist: Rc<RefCell<Blacklist>>,
    /// Maximum file size cap for individual files in KB (default 20GB).
    file_size_cap: u64,
    /// Total overall download size cap in KB (default 100GB).
    total_size_cap: u64,
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
            blacklist: Rc::new(RefCell::new(Blacklist::new(request_sender.clone()))),
            file_size_cap: default_file_cap,
            total_size_cap: default_total_cap,
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
                let blacklist = self.blacklist.clone();
                blacklist
                    .borrow_mut()
                    .parse_blacklist(blacklist_tags)
                    .cache_users();
                self.grabber.set_blacklist(blacklist);
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
    /// * `dir_name`: Directory name to remove invalid chars from.
    ///
    /// returns: String
    fn remove_invalid_chars(&self, dir_name: &str) -> String {
        dir_name
            .chars()
            .map(|e| match e {
                '?' | ':' | '*' | '<' | '>' | '"' | '|' => '_',
                _ => e,
            })
            .collect()
    }

    /// Processes `PostSet` and downloads all posts from it.
    /// Processes `PostSet` and downloads all posts from it.
    fn download_collection(&mut self) {
        // Get directory manager
        let dir_manager = match Config::get().directory_manager() {
            Ok(manager) => manager,
            Err(err) => {
                error!("Failed to get directory manager from configuration: {}", err);
                self.progress_bar.set_message("Error: Failed to initialize directory manager!");
                self.progress_bar.finish_with_message("Download failed: Configuration error");
                return;
            }
        };
        let mut dir_manager = dir_manager.clone();  // Clone to get a mutable version
        for collection in self.grabber.posts().iter() {
            let collection_name = collection.name();
            let collection_category = collection.category();
            let collection_posts = collection.posts();
            let collection_count = collection_posts.len();
            let short_collection_name = collection.shorten("...");

            // Count new files that will be downloaded
            let new_files = collection_posts.iter().filter(|post| post.is_new()).count();
            if new_files > 0 {
                info!(
                    "Found {} new files to download in {}",
                    new_files,
                    console::style(format!("\"{}\"", collection_name)).color256(39).italic()
                );
            }

            trace!("Printing Collection Info:");
            trace!("Collection Name:            \"{collection_name}\"");
            trace!("Collection Category:        \"{collection_category}\"");
            trace!("Collection Post Length:     \"{collection_count}\"");

            for post in collection_posts {
                if !post.is_new() {
                    self.progress_bar.set_message("Duplicate found: skipping... ");
                    self.progress_bar.inc(post.file_size() as u64);
                    continue;
                }

                self.progress_bar
                    .set_message(format!("Downloading: {short_collection_name} "));

                // Get the save directory from the post
                let save_dir = match post.save_directory() {
                    Some(dir) => dir,
                    None => {
                        error!("Post does not have a save directory assigned: {}", post.name());
                        self.progress_bar.set_message(format!("Error saving {}: No directory assigned", post.name()));
                        self.progress_bar.inc(post.file_size() as u64);
                        continue;
                    }
                };
                let file_path = save_dir.join(self.remove_invalid_chars(post.name()));

                // Create the directory if it doesn't exist
                if let Err(err) = create_dir_all(&save_dir) {
                    let path_str = save_dir.to_string_lossy();
                    error!("Could not create directory for images: {}", err);
                    error!("Path: {}", path_str);
                    self.progress_bar.set_message(format!("Error: Could not create directory {}", path_str));
                    self.progress_bar.inc(post.file_size() as u64);
                    continue;
                }
                // Download and save the file
                let bytes = self
                    .request_sender
                    .download_image(post.url(), post.file_size());
                
                // Get file path as string
                let file_path_str = match file_path.to_str() {
                    Some(path) => path,
                    None => {
                        error!("Invalid file path for post: {}", post.name());
                        self.progress_bar.set_message(format!("Error: Invalid file path for {}", post.name()));
                        self.progress_bar.inc(post.file_size() as u64);
                        continue;
                    }
                };
                
                // Save the image
                match write(file_path_str, &bytes) {
                    Ok(_) => {
                        trace!("Saved {file_path_str}...");
                        // Mark the file as downloaded in our tracking system
                        dir_manager.mark_file_downloaded(post.name());
                        self.progress_bar.set_message(format!("Downloaded: {}", post.name()));
                    }
                    Err(err) => {
                        error!("Failed to save image {}: {}", file_path_str, err);
                        self.progress_bar.set_message(format!("Error: Could not save {}", post.name()));
                    }
                }

                self.progress_bar.inc(post.file_size() as u64);
            }

            trace!("Collection {collection_name} is finished downloading...");
        }
    }

    /// Initializes the progress bar for downloading process.
    ///
    /// # Arguments
    ///
    /// * `len`: The total bytes to download.
    fn initialize_progress_bar(&mut self, len: u64) {
        self.progress_bar = ProgressBarBuilder::new(len)
            .style(
                ProgressStyleBuilder::default()
                    .template("{msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec} {eta}")
                    .progress_chars("=>-")
                    .build())
            .draw_target(ProgressDrawTarget::stderr())
            .reset()
            .steady_tick(Duration::from_secs(1))
            .build();
    }

    /// Downloads tuple of general posts and single posts.
    pub(crate) fn download_posts(&mut self) {
        // Initializes the progress bar for downloading.
        let length = self.get_total_file_size();
        trace!("Total file size for all images grabbed is {length}KB");
        self.initialize_progress_bar(length);
        self.download_collection();
        self.progress_bar.finish_and_clear();
    }

    /// Gets the total size (in KB) of every post image to be downloaded.
    /// Formats file size in KB to a human-readable string with appropriate units
    fn format_file_size(&self, size_kb: u64) -> String {
        if size_kb >= 1024 * 1024 {
            // Size in GB
            format!("{:.2} GB", size_kb as f64 / (1024.0 * 1024.0))
        } else if size_kb >= 1024 {
            // Size in MB
            format!("{:.2} MB", size_kb as f64 / 1024.0)
        } else {
            // Size in KB
            format!("{} KB", size_kb)
        }
    }

    /// Gets the total size (in KB) of every post image to be downloaded.
    /// Includes validation to prevent unreasonable file sizes.
    fn get_total_file_size(&self) -> u64 {
        let mut total_size: u64 = 0;
        let mut post_count: usize = 0;
        let mut capped_files: usize = 0;
        
        for collection in self.grabber.posts().iter() {
            for post in collection.posts() {
                // Skip posts that aren't new to avoid counting duplicates
                if !post.is_new() {
                    continue;
                }
                
                let file_size = post.file_size() as u64;
                
                // Validate individual file size using user's cap
                if file_size > self.file_size_cap {
                    let formatted_original = self.format_file_size(file_size);
                    let formatted_cap = self.format_file_size(self.file_size_cap);
                    
                    warn!("Post {} has large file size: {}, capping at {}", 
                          post.name(), formatted_original, formatted_cap);
                          
                    total_size += self.file_size_cap;
                    capped_files += 1;
                } else {
                    total_size += file_size;
                }
                
                post_count += 1;
            }
        }
        
        // Final validation of total size
        if total_size > self.total_size_cap {
            let formatted_original = self.format_file_size(total_size);
            let formatted_cap = self.format_file_size(self.total_size_cap);
            
            warn!("Total file size ({}) exceeds user's limit, capping at {}", 
                  formatted_original, formatted_cap);
                  
            return self.total_size_cap;
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
}
