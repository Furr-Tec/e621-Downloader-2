use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::path::PathBuf;

use crate::e621::blacklist::Blacklist;
use crate::e621::io::tag::{Group, Tag, TagSearchType, TagType};
use crate::e621::io::{emergency_exit, Config, Login};
use crate::e621::sender::entries::{PoolEntry, PostEntry, SetEntry};
use crate::e621::sender::RequestSender;

const POST_SEARCH_LIMIT: u8 = 5;

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
}

impl GrabbedPost {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn file_size(&self) -> i64 {
        self.file_size // Now directly returns i64
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
}

impl NewVec<Vec<PostEntry>> for GrabbedPost {
    fn new_vec(vec: Vec<PostEntry>) -> Vec<Self> {
        let dir_manager = Config::get().directory_manager().unwrap();
        vec.into_iter()
            .map(|e| {
                let mut post = GrabbedPost::from((e.clone(), Config::get().naming_convention()));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Check if this file has been downloaded before
                post.set_is_new(!dir_manager.file_exists(&post.name));
                post
            })
            .collect()
    }
}

impl NewVec<(Vec<PostEntry>, &str)> for GrabbedPost {
    fn new_vec((vec, pool_name): (Vec<PostEntry>, &str)) -> Vec<Self> {
        let dir_manager = Config::get().directory_manager().unwrap();
        vec.iter()
            .enumerate()
            .map(|(i, e)| {
                let mut post = GrabbedPost::from((e, pool_name, (i + 1) as u16));
                if let Some(artist_tag) = e.tags.artist.first() {
                    post.set_artist(artist_tag.clone());
                }
                // Check if this file has been downloaded before
                post.set_is_new(!dir_manager.file_exists(&post.name));
                post
            })
            .collect()
    }
}

impl From<(&PostEntry, &str, u16)> for GrabbedPost {
    fn from((post, name, current_page): (&PostEntry, &str, u16)) -> Self {
        GrabbedPost {
            url: post.file.url.clone().unwrap(),
            name: format!("{} Page_{:05}.{}", name, current_page, post.file.ext),
            file_size: post.file.size, // post.file.size is already i64
            save_directory: None,
            artist: None,
            is_new: true,
        }
    }
}

impl From<(PostEntry, &str)> for GrabbedPost {
    fn from((post, name_convention): (PostEntry, &str)) -> Self {
        match name_convention {
            "md5" => GrabbedPost {
                url: post.file.url.clone().unwrap(),
                name: format!("{}.{}", post.file.md5, post.file.ext),
                file_size: post.file.size, // post.file.size is already i64
                save_directory: None,
                artist: None,
                is_new: true,
            },
            "id" => GrabbedPost {
                url: post.file.url.clone().unwrap(),
                name: format!("{}.{}", post.id, post.file.ext),
                file_size: post.file.size, // post.file.size is already i64
                save_directory: None,
                artist: None,
                is_new: true,
            },
            _ => {
                emergency_exit("Incorrect naming convention!");
                GrabbedPost {
                    url: String::new(),
                    name: String::new(),
                    file_size: 0, // 0 as i64
                    save_directory: None,
                    artist: None,
                    is_new: false,
                }
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
        let dir_manager = Config::get().directory_manager()?;
        
        // Track how many posts were assigned directories
        let mut assigned_count = 0;
        
        // Step 1: Set up base directory based on category
        match self.category.as_str() {
            "Pools" => {
                trace!("Setting up pool directory for '{}'", self.name);
                self.base_directory = Some(dir_manager.get_pool_directory(&self.name)?);
            }
            "Sets" => {
                trace!("Setting up set directory for '{}'", self.name);
                self.base_directory = Some(dir_manager.get_tag_directory(&self.name)?);
            }
            "General Searches" => {
                trace!("Setting up tag directory for general search '{}'", self.name);
                // For general searches, create a tag directory as the base
                self.base_directory = Some(dir_manager.get_tag_directory(&self.name)?);
            }
            _ => {
                // For single posts or other categories, handle individually
                trace!("Processing individual posts for '{}'", self.name);
                for post in &mut self.posts {
                    if let Some(artist) = post.artist() {
                        let artist_dir = dir_manager.get_artist_directory(artist)?;
                        trace!("Assigned artist directory for post '{}' to artist '{}'", post.name(), artist);
                        post.set_save_directory(artist_dir);
                        assigned_count += 1;
                    } else {
                        // Fallback for posts without artist
                        let fallback_dir = dir_manager.get_tag_directory("unknown_artist")?;
                        trace!("Assigned fallback directory for post '{}' with no artist", post.name());
                        post.set_save_directory(fallback_dir);
                        assigned_count += 1;
                    }
                }
            }
        }

        // Step 2: For posts in categories with base directories, organize by artist
        if let Some(base_dir) = &self.base_directory {
            for post in &mut self.posts {
                if let Some(artist) = post.artist() {
                    let artist_subdir = dir_manager.create_artist_subdirectory(base_dir, artist)?;
                    trace!("Assigned artist subdirectory in '{}/{}' for post '{}'", 
                          self.category, self.name, post.name());
                    post.set_save_directory(artist_subdir);
                    assigned_count += 1;
                } else {
                    // Use 'unknown_artist' subdirectory as fallback
                    let unknown_artist_dir = dir_manager.create_artist_subdirectory(base_dir, "unknown_artist")?;
                    trace!("Assigned unknown_artist subdirectory for post '{}' with no artist", post.name());
                    post.set_save_directory(unknown_artist_dir);
                    assigned_count += 1;
                }
            }
        }

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
    blacklist: Option<Rc<RefCell<Blacklist>>>,
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

    pub(crate) fn set_blacklist(&mut self, blacklist: Rc<RefCell<Blacklist>>) {
        if !blacklist.borrow_mut().is_empty() {
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
            let mut collection = PostCollection::new(&tag, "", GrabbedPost::new_vec(posts));
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
        for tag in tags {
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

    fn grab_general(&mut self, tag: &Tag) {
        let posts = self.get_posts_from_tag(tag);
        let mut collection = PostCollection::new(
            tag.name(),
            "General Searches",
            GrabbedPost::new_vec(posts),
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

    fn grab_set(&mut self, tag: &Tag) {
        let entry: SetEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "set");
        let posts = self.search(&format!("set:{}", entry.shortname), &TagSearchType::Special);
        let mut collection = PostCollection::from((&entry, GrabbedPost::new_vec(posts)));
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

    fn grab_pool(&mut self, tag: &Tag) {
        let mut entry: PoolEntry = self.request_sender.get_entry_from_appended_id(tag.name(), "pool");
        let name = &entry.name;
        let mut posts = self.search(&format!("pool:{}", entry.id), &TagSearchType::Special);

        entry.post_ids.retain(|id| posts.iter().any(|post| post.id == *id));
        Self::sort_pool_by_id(&entry, &mut posts);

        let mut collection = PostCollection::new(
            name,
            "Pools",
            GrabbedPost::new_vec((posts, name.as_ref())),
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
                let mut grabbed_post = GrabbedPost::from((entry.clone(), Config::get().naming_convention()));
                if let Some(artist_tag) = entry.tags.artist.first() {
                    grabbed_post.set_artist(artist_tag.clone());
                }
                let collection = self.single_post_collection();
                if let Err(e) = collection.initialize_directories() {
                    error!("Failed to initialize directories for single post: {}", e);
                    emergency_exit("Directory initialization failed");
                }
                collection.posts.push(grabbed_post);
                info!(
                    "Post with ID {} grabbed!",
                    console::style(format!("\"{id}\"")).color256(39).italic()
                );
            }
        }
    }

    fn search(&self, searching_tag: &str, tag_search_type: &TagSearchType) -> Vec<PostEntry> {
        let mut posts: Vec<PostEntry> = Vec::new();
        let mut filtered = 0;
        let mut invalid_posts = 0;
        match tag_search_type {
            TagSearchType::General => {
                posts = Vec::with_capacity(320 * POST_SEARCH_LIMIT as usize);
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

        posts
    }

    fn special_search(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
    ) {
        let mut page = 1;

        loop {
            let mut searched_posts = self.request_sender.bulk_search(searching_tag, page).posts;
            if searched_posts.is_empty() {
                break;
            }

            *filtered += self.filter_posts_with_blacklist(&mut searched_posts);
            *invalid_posts += Self::remove_invalid_posts(&mut searched_posts);

            searched_posts.reverse();
            posts.append(&mut searched_posts);
            page += 1;
        }
    }

    fn general_search(
        &self,
        searching_tag: &str,
        posts: &mut Vec<PostEntry>,
        filtered: &mut u16,
        invalid_posts: &mut u16,
    ) {
        for page in 1..POST_SEARCH_LIMIT {
            let mut searched_posts = self.request_sender.bulk_search(searching_tag, page as u16).posts;
            if searched_posts.is_empty() {
                break;
            }

            *filtered += self.filter_posts_with_blacklist(&mut searched_posts);
            *invalid_posts += Self::remove_invalid_posts(&mut searched_posts);

            searched_posts.reverse();
            posts.append(&mut searched_posts);
        }
    }

    fn filter_posts_with_blacklist(&self, posts: &mut Vec<PostEntry>) -> u16 {
        if self.request_sender.is_authenticated() {
            if let Some(ref blacklist) = self.blacklist {
                return blacklist.borrow_mut().filter_posts(posts);
            }
        }
        0
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
