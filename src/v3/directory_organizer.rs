//! Directory Organization Module for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Creating proper folder structures based on content type (tags, artists, favorites)
//! 2. Managing directory organization strategies
//! 3. File path generation and sanitization
//! 4. Folder creation and validation

use std::path::{Path, PathBuf};
use std::fs;

use thiserror::Error;
use tracing::info;

use crate::v3::{DownloadJob, AppConfig};

/// Error types for directory organization
#[derive(Error, Debug)]
pub enum DirectoryOrganizerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Path error: {0}")]
    Path(String),
}

/// Result type for directory organization operations
pub type DirectoryOrganizerResult<T> = Result<T, DirectoryOrganizerError>;

/// Directory organization strategies
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrganizationStrategy {
    /// Organize files by tags (creates folders for each tag)
    ByTag,
    /// Organize files by artist (creates folders for each artist)
    ByArtist,
    /// Flat structure (all files in root download directory)
    Flat,
    /// Mixed strategy (creates both tag and artist folders)
    Mixed,
}

impl Default for OrganizationStrategy {
    fn default() -> Self {
        OrganizationStrategy::Mixed
    }
}

impl OrganizationStrategy {
    /// Parse strategy from config string
    pub fn from_config_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "by_tag" | "tag" => OrganizationStrategy::ByTag,
            "by_artist" | "artist" => OrganizationStrategy::ByArtist,
            "flat" => OrganizationStrategy::Flat,
            "mixed" => OrganizationStrategy::Mixed,
            _ => OrganizationStrategy::default(),
        }
    }
}

/// Content type for organization
#[derive(Debug, Clone, PartialEq)]
pub enum ContentType {
    /// Regular tagged content
    Tagged,
    /// Artist-specific content
    Artist(String),
    /// User favorites
    Favorites(String), // username
    /// Pool content
    Pool(usize), // pool ID
    /// Collection content
    Collection(usize), // collection ID
    /// Blacklisted content
    Blacklisted,
}

/// Directory organizer for managing folder structures
pub struct DirectoryOrganizer {
    base_download_dir: PathBuf,
    strategy: OrganizationStrategy,
    favorites_folder: PathBuf,
    blacklisted_folder: PathBuf,
    configured_tags: Vec<String>,
}

impl DirectoryOrganizer {
    /// Create a new directory organizer
    pub fn new(base_download_dir: PathBuf, config: &AppConfig, configured_tags: Vec<String>) -> DirectoryOrganizerResult<Self> {
        let strategy = OrganizationStrategy::from_config_string(&config.organization.directory_strategy);
        
        let favorites_folder = base_download_dir.join("favorites");
        let blacklisted_folder = base_download_dir.join("blacklisted");
        
        Ok(Self {
            base_download_dir,
            strategy,
            favorites_folder,
            blacklisted_folder,
            configured_tags,
        })
    }
    
    /// Get the appropriate directory path for a download job
    pub fn get_download_path(&self, job: &DownloadJob, content_type: ContentType) -> DirectoryOrganizerResult<PathBuf> {
        let base_path = match content_type {
            ContentType::Blacklisted => {
                self.blacklisted_folder.clone()
            }
            ContentType::Favorites(ref username) => {
                // Align folder naming with v2 structure (capitalized)
                self.base_download_dir.join("Favorites").join(sanitize_filename(username))
            }
            ContentType::Pool(pool_id) => {
                // Align folder naming with v2 structure (capitalized)
                self.base_download_dir.join("Pools").join(format!("pool_{}", pool_id))
            }
            ContentType::Collection(collection_id) => {
                // Keep collections under capitalized directory for consistency
                self.base_download_dir.join("Collections").join(format!("collection_{}", collection_id))
            }
            ContentType::Tagged | ContentType::Artist(_) => {
                self.get_organized_path(job, &content_type)?
            }
        };
        
        // Ensure the directory exists
        if !base_path.exists() {
            fs::create_dir_all(&base_path)?;
            info!("Created directory: {}", base_path.display());
        }
        
        // Generate the final file path
        let filename = self.generate_filename(job);
        let final_path = base_path.join(filename);
        tracing::debug!(
            source_query = ?job.source_query,
            artists = ?job.artists,
            decided_path = %final_path.display(),
            "Directory routing decision: mixed strategy applied"
        );
        Ok(final_path)
    }
    
    /// Get organized path based on strategy
    fn get_organized_path(&self, job: &DownloadJob, content_type: &ContentType) -> DirectoryOrganizerResult<PathBuf> {
        match self.strategy {
            OrganizationStrategy::Flat => {
                Ok(self.base_download_dir.clone())
            }
            OrganizationStrategy::ByTag => {
                self.create_tag_folders(job)
            }
            OrganizationStrategy::ByArtist => {
                self.create_artist_folders(job)
            }
            OrganizationStrategy::Mixed => {
                // Create both tag and artist folders
                match content_type {
                    ContentType::Artist(artist_name) => {
                        // Align with v2 folder casing: "Artists"
                        let artist_folder = self.base_download_dir.join("Artists").join(sanitize_filename(artist_name));
                        Ok(artist_folder)
                    }
                    _ => {
                        // For regular content, create folders for both tags and artists found in the job
                        self.create_mixed_folders(job)
                    }
                }
            }
        }
    }
    
    /// Create folders for tags
    fn create_tag_folders(&self, job: &DownloadJob) -> DirectoryOrganizerResult<PathBuf> {
        // First, try to find a configured tag that matches the post's tags
        let primary_tag = self.configured_tags.iter()
            .find(|configured_tag| {
                job.tags.iter().any(|post_tag| {
                    // Handle both exact matches and artist: prefixed tags
                    post_tag == *configured_tag || 
                    post_tag == &format!("artist:{}", configured_tag) ||
                    (post_tag.starts_with("artist:") && 
                     post_tag.strip_prefix("artist:").unwrap_or("") == *configured_tag)
                })
            })
            .cloned()
            .or_else(|| {
                // Fallback: use the first non-meta tag from the post that's in our configured tags
                job.tags.iter()
                    .find(|tag| !is_meta_tag(tag) && self.configured_tags.contains(tag))
                    .cloned()
            })
            .or_else(|| {
                // Better fallback: Use any reasonable non-meta tag from the post
                job.tags.iter()
                    .find(|tag| !is_meta_tag(tag) && !tag.starts_with("artist:") && tag.len() > 2)
                    .cloned()
            })
            .or_else(|| {
                // If from favorites, use "favorites" as tag
                if job.tags.iter().any(|tag| tag.starts_with("fav:")) {
                    Some("favorites".to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                // Final fallback: create a single unmatched folder for all non-matching posts
                "unmatched".to_string()
            });
        
        // Align with v2 folder casing: "Tags"
        let tag_folder = self.base_download_dir.join("Tags").join(sanitize_filename(&primary_tag));
        
        // Only return the primary tag folder - don't create empty additional folders
        Ok(tag_folder)
    }
    
    /// Create folders for artists
    fn create_artist_folders(&self, job: &DownloadJob) -> DirectoryOrganizerResult<PathBuf> {
        // Extract artist names from tags
        let artist_tags: Vec<String> = job.tags.iter()
            .filter(|tag| tag.starts_with("artist:") || is_likely_artist_tag(tag))
            .map(|tag| {
                if tag.starts_with("artist:") {
                    tag.strip_prefix("artist:").unwrap_or(tag).to_string()
                } else {
                    tag.clone()
                }
            })
            .collect();
        
        let artist_name = if !artist_tags.is_empty() {
            artist_tags[0].clone()
        } else {
            format!("unknown_artist_{}", job.post_id)
        };
        
        // Align with v2 folder casing: "Artists"
        let artist_folder = self.base_download_dir.join("Artists").join(sanitize_filename(&artist_name));
        
        // Only return the primary artist folder - don't create empty additional folders
        Ok(artist_folder)
    }
    
    /// Create mixed folder structure (both tags and artists)
    fn create_mixed_folders(&self, job: &DownloadJob) -> DirectoryOrganizerResult<PathBuf> {
        // Prefer routing based on the original query to avoid misclassifying tag queries as artists
        if job.source_query.len() == 1 {
            let q = job.source_query[0].as_str();
            // If the single query equals the post artist, go to Artists/<name>
            if job.artists.iter().any(|a| a.eq_ignore_ascii_case(q)) {
                return Ok(self.base_download_dir.join("Artists").join(sanitize_filename(q)));
            }
            // Otherwise, treat it as the tag folder name
            return Ok(self.base_download_dir.join("Tags").join(sanitize_filename(q)));
        }

        // If multiple terms or no clear source, fall back to heuristics favoring tags first
        if let Some(tag) = job.tags.iter().find(|t| !is_meta_tag(t) && !t.starts_with("artist:") && t.len() > 2) {
            return Ok(self.base_download_dir.join("Tags").join(sanitize_filename(tag)));
        }

        // If the only strong signal is an artist tag, use it
        if let Some(artist_tag) = job.tags.iter().find(|t| t.starts_with("artist:")) {
            let artist_name = artist_tag.trim_start_matches("artist:");
            return Ok(self.base_download_dir.join("Artists").join(sanitize_filename(artist_name)));
        }

        // Fallbacks to configured tags
        if let Some(tag) = self.configured_tags.iter().find(|configured_tag| {
            job.tags.iter().any(|post_tag| post_tag == *configured_tag)
        }) {
            return Ok(self.base_download_dir.join("Tags").join(sanitize_filename(tag)));
        }

        // Final fallback
        Ok(self.base_download_dir.join("unmatched"))
    }
    
    /// Generate a sanitized filename for the download
    fn generate_filename(&self, job: &DownloadJob) -> String {
        let extension = if job.file_ext.starts_with('.') {
            job.file_ext.clone()
        } else {
            format!(".{}", job.file_ext)
        };
        
        // Extract artist name from tags
        let artist_name = job.tags.iter()
            .find(|tag| tag.starts_with("artist:"))
            .map(|tag| tag.strip_prefix("artist:").unwrap_or(tag))
            .or_else(|| {
                // Fallback to finding likely artist tags
                job.tags.iter()
                    .find(|tag| is_likely_artist_tag(tag))
                    .map(|tag| tag.as_str())
            })
            .unwrap_or("unknown")
            .to_string();
        
        // Generate filename in format: artist_postid.extension
        let sanitized_artist = sanitize_filename(&artist_name);
        format!("{}_{}{}", sanitized_artist, job.post_id, extension)
    }
    
    /// Get the base download directory
    pub fn get_base_download_dir(&self) -> &Path {
        &self.base_download_dir
    }
    
    /// Get the favorites folder path
    pub fn get_favorites_folder(&self) -> &Path {
        &self.favorites_folder
    }
    
    /// Get the blacklisted folder path  
    pub fn get_blacklisted_folder(&self) -> &Path {
        &self.blacklisted_folder
    }
    
    /// Determine content type from download job
    pub fn determine_content_type(&self, job: &DownloadJob, username: Option<&str>) -> ContentType {
        // 1) Explicit blacklisted
        if job.is_blacklisted {
            return ContentType::Blacklisted;
        }

        // 2) Prefer the original source query to disambiguate
        //    This lets us distinguish favorites, pools, collections, artist vs. tag queries.
        if let Some(user) = username {
            if job.source_query.iter().any(|q| q == &format!("fav:{}", user)) {
                return ContentType::Favorites(user.to_string());
            }
        }

        // Pools/collections detected from the source query if present
        for q in &job.source_query {
            if let Some(pool_id) = q.strip_prefix("pool:").and_then(|s| s.parse().ok()) {
                return ContentType::Pool(pool_id);
            }
            if let Some(collection_id) = q.strip_prefix("set:").and_then(|s| s.parse().ok()) {
                return ContentType::Collection(collection_id);
            }
        }

        // If the source query is a single term that matches one of this post's artists, treat as artist
        if job.source_query.len() == 1 {
            let q = job.source_query[0].as_str();
            if job.artists.iter().any(|a| a.eq_ignore_ascii_case(q)) {
                return ContentType::Artist(q.to_string());
            }
        }

        // 3) Fallback heuristics based on post tags (legacy behavior)
        for tag in &job.tags {
            if tag.starts_with("artist:") {
                let artist = tag.strip_prefix("artist:").unwrap_or(tag);
                return ContentType::Artist(artist.to_string());
            }
        }

        // Default to tagged content
        ContentType::Tagged
    }
}

/// Sanitize a filename by removing/replacing invalid characters
pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            // Replace invalid filename characters
            '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\' | '/' => '_',
            // Replace control characters
            c if c.is_control() => '_',
            // Keep valid characters
            c => c,
        })
        .collect::<String>()
        .trim_matches('.')  // Remove leading/trailing dots
        .trim()
        .to_string()
}

/// Check if a tag is a meta tag that shouldn't be used for folder organization
fn is_meta_tag(tag: &str) -> bool {
    matches!(tag, 
        "safe" | "questionable" | "explicit" | 
        "rating:safe" | "rating:questionable" | "rating:explicit" |
        "score:*" | "fav:*" | "favcount:*" | "comment_count:*" | 
        "created:*" | "updated:*" | "id:*" | "md5:*"
    ) || tag.starts_with("rating:") 
      || tag.starts_with("score:") 
      || tag.starts_with("fav:")
      || tag.starts_with("favcount:")
      || tag.starts_with("comment_count:")
      || tag.starts_with("created:")
      || tag.starts_with("updated:")
      || tag.starts_with("id:")
      || tag.starts_with("md5:")
}

/// Check if a tag is likely an artist name
fn is_likely_artist_tag(tag: &str) -> bool {
    // This is a heuristic - in a real implementation, you might want to
    // maintain a database of known artist names or use the E621 API
    // For now, we'll use some simple heuristics
    
    // Avoid common general tags that aren't artist names
    let common_tags = [
        "solo", "duo", "group", "male", "female", "intersex", "ambiguous",
        "anthro", "human", "feral", "taur", "humanoid",
        "fur", "scales", "feathers", "skin", "clothing", "nude",
        "sitting", "standing", "lying", "looking_at_viewer",
        "smile", "open_mouth", "closed_eyes", "blush",
        "inside", "outside", "detailed_background", "simple_background",
        "white_background", "transparent_background"
    ];
    
    !common_tags.contains(&tag)
        && !tag.contains('_') // Artist names typically don't have underscores in tags
        && tag.len() > 2      // Avoid very short tags
        && tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Initialize directory organizer
pub async fn init_directory_organizer(base_download_dir: PathBuf, config: &AppConfig, configured_tags: Vec<String>) -> DirectoryOrganizerResult<DirectoryOrganizer> {
    DirectoryOrganizer::new(base_download_dir, config, configured_tags)
}
