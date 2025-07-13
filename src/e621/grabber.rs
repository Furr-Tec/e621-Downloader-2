use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::mpsc;

use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, trace, warn};
use console;

use crate::e621::blacklist::Blacklist;
use crate::e621::io::tag::{Group, Tag, TagSearchType, TagType};
use crate::e621::io::{emergency_exit, Config, Login};
use crate::e621::sender::entries::{PoolEntry, PostEntry, SetEntry};
use crate::e621::sender::RequestSender;

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
        vec.into_iter()
            .map(|e| {
                let mut post = GrabbedPost::from((e.clone(), Config::get().naming_convention()));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Check if this post ID has been downloaded before
                let is_duplicate = dir_manager.has_post_id(e.id);
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
        vec.into_iter()
            .map(|e| {
                // Generate enhanced filename with artist names and IDs
                let enhanced_name = GrabbedPost::generate_enhanced_filename(&e, Config::get().naming_convention(), request_sender);
                trace!("Generated filename '{}' for post ID {}", enhanced_name, e.id);
                
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
                
                // Check if this post ID has been downloaded before
                let is_duplicate = dir_manager.has_post_id(e.id);
                if is_duplicate {
                    trace!("Post ID {} marked as duplicate (already downloaded)", e.id);
                } else {
                    trace!("Post ID {} marked as new (not found in database)", e.id);
                }
                post.set_is_new(!is_duplicate);
                post
            })
            .collect()
    }
}

impl NewVec<(Vec<PostEntry>, &str)> for GrabbedPost {
    fn new_vec((vec, pool_name): (Vec<PostEntry>, &str)) -> Vec<Self> {
        let binding = Config::get();
        let dir_manager = binding.directory_manager().unwrap();
        vec.iter()
            .enumerate()
            .map(|(i, e)| {
                let mut post = GrabbedPost::from((e, pool_name, (i + 1) as u16));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Check if this post ID has been downloaded before  
                let is_duplicate = dir_manager.has_post_id(e.id);
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
        let binding = Config::get();
        let dir_manager = binding.directory_manager()?;
        
        // Track how many posts were assigned directories
        let mut assigned_count = 0;
        
        // All content goes into Tags directory using the collection name (tag/artist name) as the main folder
        trace!("Setting up tag directory for '{}' (category: {})", self.name, self.category);
        self.base_directory = Some(dir_manager.get_tag_directory(&self.name)?);
        
        // Assign the base directory directly to all posts (no artist subdirectories)
        if let Some(base_dir) = &self.base_directory {
            for post in &mut self.posts {
                trace!("Assigned directory '{}' for post '{}'", base_dir.display(), post.name());
                post.set_save_directory(base_dir.clone());
                assigned_count += 1;
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
}

impl Grabber {
    pub(crate) fn new(request_sender: RequestSender, safe_mode: bool) -> Self {
        Grabber {
            posts: vec![PostCollection::new("Single Posts", "", Vec::new())],
            request_sender,
            blacklist: None,
            safe_mode,
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
        
        // Create a thread pool with limited concurrency to avoid API overload
        let thread_pool_size = std::cmp::min(4, tags.len()); // Max 4 threads for API safety
        let chunk_size = (tags.len() + thread_pool_size - 1) / thread_pool_size;
        
        // Split tags into chunks for parallel processing
        let tag_chunks: Vec<Vec<&Tag>> = tags.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect();
        
        info!("Processing {} tags using {} threads in {} chunks", tags.len(), thread_pool_size, tag_chunks.len());
        
        // Use scoped threads to avoid lifetime issues
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            
            for (chunk_id, tag_chunk) in tag_chunks.into_iter().enumerate() {
                let posts_mutex = Arc::clone(&posts_mutex);
                let progress_bar = Arc::clone(&progress_bar_arc);
                let request_sender = self.request_sender.clone();
                let blacklist = self.blacklist.clone();
                let safe_mode = self.safe_mode;
                
                let handle = scope.spawn(move || {
                    // Small delay to stagger thread starts and reduce API burst
                    thread::sleep(Duration::from_millis(chunk_id as u64 * 100));
                    
                    for tag in tag_chunk {
                        let start_time = Instant::now();
                        
                        // Process each tag and create collection
                        let collection = Self::process_tag_to_collection(tag, groups, &request_sender, &blacklist, safe_mode);
                        
                        if let Some(collection) = collection {
                            // Add to shared collection in thread-safe manner
                            let mut posts = posts_mutex.lock().unwrap();
                            posts.push(collection);
                        }
                        
                        progress_bar.inc(1);
                        let duration = start_time.elapsed();
                        info!("Finished processing tag {} in {:.2?}", tag.name(), duration);
                    }
                });
                
                handles.push(handle);
            }
            
            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }
        });
        
        // Extract collections from the mutex and add to self.posts
        let mut collected_posts = posts_mutex.lock().unwrap();
        self.posts.append(&mut collected_posts);
        progress_bar_arc.finish_with_message("All tags processed");
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
                
                // Check if this post ID has been downloaded before
                let binding = Config::get();
                let dir_manager = binding.directory_manager().unwrap();
                let is_duplicate = dir_manager.has_post_id(entry.id);
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
        let thread_count = self.calculate_optimal_thread_count(max_pages);
        
        // Dynamic rate limiting based on thread count to prevent API overload
        let rate_limit_delay = match thread_count {
            1 => Duration::from_millis(100),       // Single thread - minimal delay
            2 => Duration::from_millis(200),       // Dual thread - conservative
            3 => Duration::from_millis(300),       // Triple thread - balanced
            4 => Duration::from_millis(400),       // Quad thread - moderate
            5 => Duration::from_millis(500),       // Penta thread - higher
            _ => Duration::from_millis(600),       // Max thread - highest delay
        };
        
        info!("Searching max {} pages using {} threads with {}ms rate limiting (stops early if 4 consecutive empty pages found)", 
              max_pages, thread_count, rate_limit_delay.as_millis());
        
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
                
                let handle = thread::spawn(move || {
                    // Stagger thread starts to avoid simultaneous API hits
                    let stagger_delay_ms = 50 * thread_id as u64;
                    thread::sleep(Duration::from_millis(stagger_delay_ms));
                    
                    let page_start = Instant::now();
                    let searched_posts = request_sender.safe_bulk_post_search(&searching_tag, page_to_search).posts;
                    let request_duration = page_start.elapsed();
                    
                    // Log slow requests as warnings
                    if request_duration > Duration::from_secs(3) {
                        warn!("Slow API request detected: tag '{}' page {} took {:.2?}", 
                              searching_tag, page_to_search, request_duration);
                    } else {
                        trace!("Thread {} - Page {} completed in {:.2?} - found {} posts", 
                               thread_id, page_to_search, request_duration, searched_posts.len());
                    }
                    
                    // Send results back
                    let _ = result_tx.send((page_to_search, searched_posts));
                    
                    // Dynamic rate limiting - wait between requests
                    thread::sleep(rate_limit_delay);
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
            
            for (_page, mut searched_posts) in batch_results {
                if !searched_posts.is_empty() {
                    found_posts_in_batch = true;
                    found_any_posts_in_batch = true;
                    
                    // Check if whitelist override is enabled in config
                    let config = Config::get();
                    if config.whitelist_override_blacklist() {
                        // Generate whitelist for this search context
                        let whitelist = self.generate_whitelist_for_search(searching_tag);
                        
                        // Process the posts with whitelist override
                        *filtered += self.filter_posts_with_blacklist_and_whitelist(&mut searched_posts, &whitelist);
                    } else {
                        // Use traditional blacklist filtering without whitelist override
                        *filtered += self.filter_posts_with_blacklist(&mut searched_posts);
                    }
                    *invalid_posts += Self::remove_invalid_posts(&mut searched_posts);
                    
                    // Check for duplicates before adding to results
                    let _initial_count = searched_posts.len(); // Keep for potential logging
                    let mut new_posts_from_page = Vec::new();
                    
                    for post in searched_posts {
                        // Check if this post is already downloaded
                        let filename = format!("{}.{}", post.file.md5, post.file.ext);
                        let is_duplicate = dir_manager.is_duplicate(&filename, None);
                        
                        if !is_duplicate {
                            new_posts_from_page.push(post);
                            new_posts_in_batch += 1;
                        } else {
                            duplicate_posts_in_batch += 1;
                        }
                    }
                    
                    // Only add new posts to results
                    new_posts_from_page.reverse();
                    posts.append(&mut new_posts_from_page);
                } else {
                    // This individual page was empty
                    empty_pages_in_batch += 1;
                }
            }
            
            // Update counters
            total_new_posts += new_posts_in_batch;
            total_duplicate_posts += duplicate_posts_in_batch;
            
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
            
            progress_bar.set_message(format!("processed {} pages ({} new posts, {} duplicates, {}/{} pages with new content)", 
                                             total_pages_searched, total_new_posts, total_duplicate_posts, pages_with_new_content, max_pages));
            
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
            
            if !should_continue {
                break;
            }
            
            // Move to next batch
            batch_start += thread_count as u16;
        }
        
        progress_bar.finish_with_message(format!("Adaptive search: {} new posts found across {} pages ({} pages with new content, {} duplicates skipped)", 
                                                posts.len(), total_pages_searched, pages_with_new_content, total_duplicate_posts));
        
        if total_duplicate_posts > 0 {
            info!("Adaptive search skipped {} already downloaded posts, found {} new posts from {} pages", 
                  total_duplicate_posts, posts.len(), total_pages_searched);
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
    fn generate_whitelist_for_search(&self, searching_tag: &str) -> Vec<String> {
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
            // For general tags (including artist names without prefix), add variants
            // This covers cases where an artist name is searched directly
            whitelist.push(format!("artist:{}", clean_tag));
        }
        
        // Remove duplicates and empty entries
        whitelist.sort();
        whitelist.dedup();
        whitelist.retain(|tag| !tag.trim().is_empty());
        
        info!("Generated whitelist for search '{}': {:?}", searching_tag, whitelist);
        whitelist
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
