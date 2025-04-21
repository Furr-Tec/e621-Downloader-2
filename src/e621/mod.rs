use std::cell::RefCell;
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use anyhow::Context;
use dialoguer::Confirm;
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
    /// The config which is modified when grabbing posts.
    download_directory: String,
    /// Progress bar that displays the current progress in downloading posts.
    progress_bar: ProgressBar,
    /// Grabber which is responsible for grabbing posts.
    grabber: Grabber,
    /// The user's blacklist.
    blacklist: Rc<RefCell<Blacklist>>,
}
impl E621WebConnector {
    /// Creates instance of `Self` for grabbing and downloading posts.
    pub(crate) fn new(request_sender: &RequestSender) -> Self {
        E621WebConnector {
            request_sender: request_sender.clone(),
            download_directory: Config::get().download_directory().to_string(),
            progress_bar: ProgressBar::hidden(),
            grabber: Grabber::new(request_sender.clone(), false),
            blacklist: Rc::new(RefCell::new(Blacklist::new(request_sender.clone()))),
        }
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

    /// Saves image to download directory.
    fn save_image(&self, file_path: &str, bytes: &[u8]) {
        write(file_path, bytes)
            .with_context(|| {
                error!("Failed to save image!");
                "A downloaded image was unable to be saved..."
            })
            .unwrap();
        trace!("Saved {file_path}...");
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
    fn download_collection(&mut self) {
        // Get directory manager
        let dir_manager = Config::get().directory_manager().unwrap();
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
                let save_dir = post.save_directory()
                    .expect("Post should have a save directory assigned");
                let file_path = save_dir.join(self.remove_invalid_chars(post.name()));

                // Create the directory if it doesn't exist
                create_dir_all(save_dir)
                    .with_context(|| {
                        error!("Could not create directories for images!");
                        format!(
                            "Directory path unable to be created...\nPath: \"{}\"",
                            save_dir.to_str().unwrap()
                        )
                    })
                    .unwrap();

                // Download and save the file
                let bytes = self
                    .request_sender
                    .download_image(post.url(), post.file_size());
                self.save_image(file_path.to_str().unwrap(), &bytes);

                // Mark the file as downloaded in our tracking system
                dir_manager.mark_file_downloaded(post.name());

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
    fn get_total_file_size(&self) -> u64 {
        self.grabber
            .posts()
            .iter()
            .map(|e| e.posts().iter().map(|f| f.file_size() as u64).sum::<u64>())
            .sum()
    }
}
