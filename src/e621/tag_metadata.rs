use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use anyhow::{Context, Result};
use log::{info, warn, debug};

use crate::e621::tag_validator::TagValidator;
use crate::e621::sender::RequestSender;
use crate::e621::sender::entries::TagEntry;

/// Metadata for tracking tag statistics and changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagMetadata {
    /// The validated/normalized tag name (lowercase, cleaned)
    pub validated_name: String,
    /// The original tag name as written in tags.txt
    pub original_name: String,
    /// Total number of posts for this tag
    pub total_posts: u64,
    /// When this tag was last checked
    pub last_checked: DateTime<Utc>,
    /// The ID of the most recent post seen
    pub last_post_id: Option<i64>,
    /// Number of new posts since last check
    pub new_posts_since_last_check: u64,
    /// The type of tag (general, artist, etc.)
    pub tag_type: String,
    /// Historical post count data
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub post_count_history: Vec<PostCountRecord>,
    /// Whether this tag has been validated against the API
    pub is_validated: bool,
    /// Any validation errors encountered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_error: Option<String>,
}

/// Record of post count at a specific time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostCountRecord {
    pub date: DateTime<Utc>,
    pub count: u64,
}

/// Main metadata storage structure
#[derive(Debug, Serialize, Deserialize)]
pub struct TagMetadataStore {
    /// When this metadata was last updated
    pub last_updated: DateTime<Utc>,
    /// Map of tag names to their metadata
    pub tags: HashMap<String, TagMetadata>,
    /// Version of the metadata format
    pub version: u32,
}

impl Default for TagMetadataStore {
    fn default() -> Self {
        Self {
            last_updated: Utc::now(),
            tags: HashMap::new(),
            version: 1,
        }
    }
}

impl TagMetadataStore {
    /// Path to the metadata file
    const METADATA_FILE: &'static str = "tag_metadata.json";
    
    /// Maximum number of history records to keep per tag
    const MAX_HISTORY_RECORDS: usize = 30;
    
    /// Create a new empty metadata store
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Load metadata from disk, creating new if not exists
    pub fn load() -> Result<Self> {
        let path = Path::new(Self::METADATA_FILE);
        
        if path.exists() {
            let contents = fs::read_to_string(path)
                .context("Failed to read tag metadata file")?;
            
            let store: Self = serde_json::from_str(&contents)
                .context("Failed to parse tag metadata JSON")?;
            
            info!("Loaded tag metadata for {} tags", store.tags.len());
            Ok(store)
        } else {
            info!("No existing tag metadata found, creating new store");
            Ok(Self::new())
        }
    }
    
    /// Save metadata to disk
    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize tag metadata")?;
        
        fs::write(Self::METADATA_FILE, json)
            .context("Failed to write tag metadata file")?;
        
        debug!("Saved tag metadata for {} tags", self.tags.len());
        Ok(())
    }
    
    /// Update or create metadata for a tag
    pub fn update_tag(
        &mut self,
        original_name: &str,
        validated_name: &str,
        tag_entry: Option<&TagEntry>,
        new_post_count: Option<u64>,
        last_post_id: Option<i64>,
    ) {
        let now = Utc::now();
        
        let metadata = self.tags.entry(validated_name.to_string())
            .or_insert_with(|| TagMetadata {
                validated_name: validated_name.to_string(),
                original_name: original_name.to_string(),
                total_posts: 0,
                last_checked: now,
                last_post_id: None,
                new_posts_since_last_check: 0,
                tag_type: "unknown".to_string(),
                post_count_history: Vec::new(),
                is_validated: false,
                validation_error: None,
            });
        
        // Update from tag entry if provided
        if let Some(entry) = tag_entry {
            metadata.tag_type = match entry.category {
                0 => "general",
                1 => "artist",
                3 => "copyright",
                4 => "character",
                5 => "species",
                6 => "invalid",
                7 => "meta",
                8 => "lore",
                _ => "unknown",
            }.to_string();
            
            metadata.is_validated = true;
            metadata.validation_error = None;
        }
        
        // Update post count if provided
        if let Some(count) = new_post_count {
            // Calculate new posts since last check
            if metadata.total_posts > 0 && count > metadata.total_posts {
                metadata.new_posts_since_last_check = count - metadata.total_posts;
            } else {
                metadata.new_posts_since_last_check = 0;
            }
            
            // Add to history if count changed
            if count != metadata.total_posts {
                metadata.post_count_history.push(PostCountRecord {
                    date: now,
                    count,
                });
                
                // Trim history to max size
                if metadata.post_count_history.len() > Self::MAX_HISTORY_RECORDS {
                    let remove_count = metadata.post_count_history.len() - Self::MAX_HISTORY_RECORDS;
                    metadata.post_count_history.drain(0..remove_count);
                }
            }
            
            metadata.total_posts = count;
        }
        
        // Update last post ID if provided
        if let Some(id) = last_post_id {
            metadata.last_post_id = Some(id);
        }
        
        metadata.last_checked = now;
        self.last_updated = now;
    }
    
    /// Mark a tag as invalid with an error message
    pub fn mark_tag_invalid(&mut self, tag_name: &str, error: &str) {
        if let Some(metadata) = self.tags.get_mut(tag_name) {
            metadata.is_validated = false;
            metadata.validation_error = Some(error.to_string());
            metadata.last_checked = Utc::now();
        }
    }
    
    /// Get metadata for a tag
    pub fn get_tag(&self, tag_name: &str) -> Option<&TagMetadata> {
        self.tags.get(tag_name)
    }
    
    /// Check if a tag has new posts since last check
    pub fn has_new_posts(&self, tag_name: &str) -> bool {
        self.tags.get(tag_name)
            .map(|m| m.new_posts_since_last_check > 0)
            .unwrap_or(false)
    }
    
    /// Get tags that need checking (haven't been checked recently)
    pub fn get_tags_needing_check(&self, hours_threshold: i64) -> Vec<String> {
        let threshold = Utc::now() - chrono::Duration::hours(hours_threshold);
        
        self.tags
            .iter()
            .filter(|(_, metadata)| metadata.last_checked < threshold)
            .map(|(name, _)| name.clone())
            .collect()
    }
    
    /// Get statistics about the tracked tags
    pub fn get_statistics(&self) -> TagMetadataStatistics {
        let total_tags = self.tags.len();
        let validated_tags = self.tags.values().filter(|m| m.is_validated).count();
        let invalid_tags = self.tags.values().filter(|m| !m.is_validated).count();
        let tags_with_new_posts = self.tags.values()
            .filter(|m| m.new_posts_since_last_check > 0)
            .count();
        let total_new_posts: u64 = self.tags.values()
            .map(|m| m.new_posts_since_last_check)
            .sum();
        
        TagMetadataStatistics {
            total_tags,
            validated_tags,
            invalid_tags,
            tags_with_new_posts,
            total_new_posts,
            last_updated: self.last_updated,
        }
    }
}

/// Statistics about the tag metadata
#[derive(Debug)]
pub struct TagMetadataStatistics {
    pub total_tags: usize,
    pub validated_tags: usize,
    pub invalid_tags: usize,
    pub tags_with_new_posts: usize,
    pub total_new_posts: u64,
    pub last_updated: DateTime<Utc>,
}

/// Tag metadata manager that handles validation and tracking
pub struct TagMetadataManager {
    store: TagMetadataStore,
    validator: TagValidator,
    request_sender: RequestSender,
}

impl TagMetadataManager {
    /// Create a new metadata manager
    pub fn new(request_sender: RequestSender) -> Result<Self> {
        let store = TagMetadataStore::load()?;
        let validator = TagValidator::new();
        
        Ok(Self {
            store,
            validator,
            request_sender,
        })
    }
    
    /// Process and validate a tag from tags.txt
    pub fn process_tag(&mut self, original_tag: &str) -> Result<String> {
        // Validate the tag
        match self.validator.validate_and_clean_tag(original_tag) {
            Some(validated_tag) => {
                // Check if tag exists in API
                let tag_entries = self.request_sender.get_tags_by_name(&validated_tag);
                
                if let Some(tag_entry) = tag_entries.first() {
                    // Tag exists, update metadata
                    self.store.update_tag(
                        original_tag,
                        &validated_tag,
                        Some(tag_entry),
                        Some(tag_entry.post_count as u64),
                        None,
                    );
                    
                    info!("Validated tag '{}' -> '{}' ({} posts)", 
                          original_tag, validated_tag, tag_entry.post_count);
                    
                    Ok(validated_tag)
                } else {
                    // Tag doesn't exist
                    let error = format!("Tag '{}' not found in API", validated_tag);
                    self.store.mark_tag_invalid(&validated_tag, &error);
                    
                    warn!("{}", error);
                    Err(anyhow::anyhow!(error))
                }
            },
            None => {
                // Tag failed validation
                let error = format!("Tag '{}' failed validation", original_tag);
                warn!("{}", error);
                Err(anyhow::anyhow!(error))
            }
        }
    }
    
    /// Check for new posts for a tag
    pub fn check_for_updates(&mut self, tag_name: &str) -> Result<bool> {
        let tag_entries = self.request_sender.get_tags_by_name(tag_name);
        
        if let Some(tag_entry) = tag_entries.first() {
            let current_count = tag_entry.post_count as u64;
            
            // Get previous count
            let previous_count = self.store.get_tag(tag_name)
                .map(|m| m.total_posts)
                .unwrap_or(0);
            
            // Update metadata
            self.store.update_tag(
                tag_name,
                tag_name,
                Some(tag_entry),
                Some(current_count),
                None,
            );
            
            let has_new = current_count > previous_count;
            if has_new {
                info!("Tag '{}' has {} new posts (total: {})", 
                      tag_name, current_count - previous_count, current_count);
            }
            
            Ok(has_new)
        } else {
            Err(anyhow::anyhow!("Tag '{}' not found", tag_name))
        }
    }
    
    /// Save the metadata to disk
    pub fn save(&self) -> Result<()> {
        self.store.save()
    }
    
    /// Get the metadata store
    pub fn store(&self) -> &TagMetadataStore {
        &self.store
    }
    
    /// Get mutable reference to the metadata store
    pub fn store_mut(&mut self) -> &mut TagMetadataStore {
        &mut self.store
    }
    
    /// Load metadata from disk
    pub fn load(&mut self) -> Result<()> {
        self.store = TagMetadataStore::load()?;
        Ok(())
    }
    
    /// Check if a tag was recently validated (within 24 hours)
    pub fn is_recently_validated(&self, tag_name: &str) -> bool {
        if let Some(metadata) = self.store.get_tag(tag_name) {
            if metadata.is_validated {
                let hours_since_check = (Utc::now() - metadata.last_checked).num_hours();
                return hours_since_check < 24;
            }
        }
        false
    }
    
    /// Update a tag's metadata directly
    pub fn update_tag(
        &mut self,
        tag_name: String,
        is_valid: bool,
        post_count: u64,
    ) {
        let metadata = self.store.tags.entry(tag_name.clone())
            .or_insert_with(|| TagMetadata {
                validated_name: tag_name.clone(),
                original_name: tag_name.clone(),
                total_posts: 0,
                last_checked: Utc::now(),
                last_post_id: None,
                new_posts_since_last_check: 0,
                tag_type: "unknown".to_string(),
                post_count_history: Vec::new(),
                is_validated: false,
                validation_error: None,
            });
        
        metadata.is_validated = is_valid;
        metadata.last_checked = Utc::now();
        
        if is_valid {
            // Calculate new posts since last check
            if metadata.total_posts > 0 && post_count > metadata.total_posts {
                metadata.new_posts_since_last_check = post_count - metadata.total_posts;
            } else {
                metadata.new_posts_since_last_check = 0;
            }
            
            // Add to history if count changed
            if post_count != metadata.total_posts {
                metadata.post_count_history.push(PostCountRecord {
                    date: Utc::now(),
                    count: post_count,
                });
                
                // Trim history to max size
                if metadata.post_count_history.len() > TagMetadataStore::MAX_HISTORY_RECORDS {
                    let remove_count = metadata.post_count_history.len() - TagMetadataStore::MAX_HISTORY_RECORDS;
                    metadata.post_count_history.drain(0..remove_count);
                }
            }
            
            metadata.total_posts = post_count;
            metadata.validation_error = None;
        } else {
            metadata.validation_error = Some("Tag not found on e621".to_string());
        }
        
        self.store.last_updated = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_store() {
        let mut store = TagMetadataStore::new();
        
        // Add a tag
        store.update_tag("Fluffy", "fluffy", None, Some(100), Some(12345));
        
        // Check it was added
        assert_eq!(store.tags.len(), 1);
        let metadata = store.get_tag("fluffy").unwrap();
        assert_eq!(metadata.original_name, "Fluffy");
        assert_eq!(metadata.validated_name, "fluffy");
        assert_eq!(metadata.total_posts, 100);
        
        // Update with new posts
        store.update_tag("Fluffy", "fluffy", None, Some(110), Some(12346));
        let metadata = store.get_tag("fluffy").unwrap();
        assert_eq!(metadata.total_posts, 110);
        assert_eq!(metadata.new_posts_since_last_check, 10);
        assert_eq!(metadata.post_count_history.len(), 2);
    }
}
