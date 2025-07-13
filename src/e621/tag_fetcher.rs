use std::fs::{write, read_to_string};
use std::path::Path;
use std::collections::HashSet;
use anyhow::{Result, Error};
use serde::{Deserialize, Serialize};
use crate::e621::sender::RequestSender;
use crate::e621::blacklist::Blacklist;

/// Name of the tag cache file
const TAG_CACHE_FILE: &str = "tags.txt";

/// Represents a tag from the e621 API
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ApiTag {
    pub id: i32,
    pub name: String,
    pub post_count: i32,
    pub category: i32, // 0=general, 1=artist, 3=copyright, 4=character, 5=species
}

/// Category mappings for e621 tags
#[derive(Debug, Clone)]
pub enum TagCategory {
    General = 0,
    Artist = 1,
    Copyright = 3,
    Character = 4,
    Species = 5,
}

impl TagCategory {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(TagCategory::General),
            1 => Some(TagCategory::Artist),
            3 => Some(TagCategory::Copyright),
            4 => Some(TagCategory::Character),
            5 => Some(TagCategory::Species),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            TagCategory::General => "general",
            TagCategory::Artist => "artist",
            TagCategory::Copyright => "copyright",
            TagCategory::Character => "character",
            TagCategory::Species => "species",
        }
    }
}


/// Tag fetcher that retrieves popular non-blacklisted tags from e621
pub struct TagFetcher {
    request_sender: RequestSender,
    blacklist: Option<Blacklist>,
}

impl TagFetcher {
    /// Create a new tag fetcher
    pub fn new(request_sender: RequestSender, blacklist: Option<Blacklist>) -> Self {
        Self {
            request_sender,
            blacklist,
        }
    }

    /// Fetch popular tags from e621 API, filtered by blacklist
    pub fn fetch_popular_tags(&self, limit: usize, min_post_count: i32) -> Result<Vec<ApiTag>> {
        info!("Fetching popular tags from e621 API...");
        
        let mut all_tags = Vec::new();
        let mut page = 1;
        let per_page = 100; // e621 API limit
        
        // Fetch multiple pages to get enough tags
        while all_tags.len() < limit && page <= 5 { // Max 5 pages to avoid excessive API calls
            let url = format!(
                "https://e621.net/tags.json?limit={}&page={}&search[hide_empty]=true&search[order]=count",
                per_page, page
            );
            
            info!("Fetching page {} of tags...", page);
            
            // Make API request
            let response = self.request_sender.get_string(&url)?;
            let tags: Vec<ApiTag> = serde_json::from_str(&response)?;
            
            // Filter tags by post count and blacklist
            for tag in tags {
                if tag.post_count >= min_post_count && !self.is_blacklisted(&tag.name) {
                    all_tags.push(tag);
                    
                    if all_tags.len() >= limit {
                        break;
                    }
                }
            }
            
            page += 1;
        }
        
        // Sort by post count (highest first)
        all_tags.sort_by(|a, b| b.post_count.cmp(&a.post_count));
        all_tags.truncate(limit);
        
        info!("Fetched {} popular non-blacklisted tags", all_tags.len());
        Ok(all_tags)
    }

    /// Check if a tag is blacklisted
    fn is_blacklisted(&self, tag_name: &str) -> bool {
        if let Some(ref blacklist) = self.blacklist {
            blacklist.is_tag_blacklisted(tag_name)
        } else {
            false
        }
    }

    /// Save tags to the tags.txt file in the format expected by the application
    pub fn save_tags_to_file(&self, tags: &[ApiTag]) -> Result<()> {
        let mut tag_lines = Vec::new();
        
        // Group tags by category
        let mut artists = Vec::new();
        let mut species = Vec::new();
        let mut characters = Vec::new();
        let mut general = Vec::new();
        
        for tag in tags {
            match TagCategory::from_i32(tag.category) {
                Some(TagCategory::Artist) => artists.push(tag),
                Some(TagCategory::Species) => species.push(tag),
                Some(TagCategory::Character) => characters.push(tag),
                Some(TagCategory::General) => general.push(tag),
                Some(TagCategory::Copyright) => general.push(tag), // Treat copyright as general
                None => general.push(tag), // Unknown categories go to general
            }
        }
        
        // Add header comment
        tag_lines.push("# Popular tags fetched from e621 API (non-blacklisted)".to_string());
        tag_lines.push("# This file follows the standard tags.txt format".to_string());
        tag_lines.push("".to_string());
        
        // Add artists section
        tag_lines.push("[artists]".to_string());
        if !artists.is_empty() {
            for tag in artists {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        } else {
            tag_lines.push("# No popular artists found".to_string());
        }
        tag_lines.push("".to_string());
        
        // Add pools section (empty but required format)
        tag_lines.push("[pools]".to_string());
        tag_lines.push("# No pool IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add sets section (empty but required format)
        tag_lines.push("[sets]".to_string());
        tag_lines.push("# No set IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add single-post section (empty but required format)
        tag_lines.push("[single-post]".to_string());
        tag_lines.push("# No individual post IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add general tags section
        tag_lines.push("[general]".to_string());
        if !general.is_empty() {
            for tag in &general {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        // Add species as general tags if any
        if !species.is_empty() {
            if !general.is_empty() {
                tag_lines.push("# Popular species:".to_string());
            }
            for tag in &species {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        // Add characters as general tags if any
        if !characters.is_empty() {
            if !general.is_empty() || !species.is_empty() {
                tag_lines.push("# Popular characters:".to_string());
            }
            for tag in &characters {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        if general.is_empty() && species.is_empty() && characters.is_empty() {
            tag_lines.push("# No popular general tags found".to_string());
        }
        
        tag_lines.push("".to_string());
        
        // Write to file
        let content = tag_lines.join("\n");
        write(TAG_CACHE_FILE, content)?;
        
        info!("Saved {} tags to {}", tags.len(), TAG_CACHE_FILE);
        Ok(())
    }

    /// Check if tags.txt exists
    pub fn tags_file_exists() -> bool {
        Path::new(TAG_CACHE_FILE).exists()
    }

    /// Load existing tags from tags.txt (for reference)
    pub fn load_existing_tags() -> Result<Vec<String>> {
        if !Self::tags_file_exists() {
            return Ok(Vec::new());
        }
        
        let content = read_to_string(TAG_CACHE_FILE)?;
        let tags: Vec<String> = content
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .map(|line| {
                // Extract tag name (before any comments)
                if let Some(pos) = line.find('#') {
                    line[..pos].trim().to_string()
                } else {
                    line.trim().to_string()
                }
            })
            .filter(|tag| !tag.is_empty())
            .collect();
        
        Ok(tags)
    }

    /// Refresh tags - fetch new ones and update the file
    pub fn refresh_tags(&self, limit: usize, min_post_count: i32) -> Result<()> {
        let tags = self.fetch_popular_tags(limit, min_post_count)?;
        self.save_tags_to_file(&tags)?;
        Ok(())
    }
}
