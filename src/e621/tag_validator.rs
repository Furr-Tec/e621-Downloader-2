use std::collections::HashSet;
use regex::Regex;
use lazy_static::lazy_static;
use log::{warn, info, debug};

lazy_static! {
    /// Valid tag pattern according to e621 API
    static ref VALID_TAG_PATTERN: Regex = Regex::new(r"^[a-z0-9._\-()]+$").unwrap();
    
    /// Pattern for artist tags
    static ref ARTIST_TAG_PATTERN: Regex = Regex::new(r"^[a-z0-9._\-]+$").unwrap();
    
    /// Known problematic characters that should be replaced
    static ref CHAR_REPLACEMENTS: Vec<(char, char)> = vec![
        ('/', '_'),  // Forward slash to underscore
        ('\\', '_'), // Backslash to underscore
        (' ', '_'),  // Space to underscore
        (':', '_'),  // Colon to underscore (except for special prefixes)
    ];
}

/// Tag validator to ensure tags meet e621 API requirements
#[derive(Clone)]
pub struct TagValidator {
    /// Set of known valid tags (cached from successful searches)
    known_valid_tags: HashSet<String>,
    
    /// Set of known invalid tags to avoid repeated API calls
    known_invalid_tags: HashSet<String>,
}

impl TagValidator {
    /// Create a new tag validator
    pub fn new() -> Self {
        Self {
            known_valid_tags: HashSet::new(),
            known_invalid_tags: HashSet::new(),
        }
    }
    
    /// Validate and clean a tag for e621 API compatibility
    pub fn validate_and_clean_tag(&mut self, tag: &str) -> Option<String> {
        let original_tag = tag.trim().to_lowercase();
        
        // Check if we've already validated this tag
        if self.known_valid_tags.contains(&original_tag) {
            debug!("Tag '{}' is known valid", original_tag);
            return Some(original_tag);
        }
        
        if self.known_invalid_tags.contains(&original_tag) {
            debug!("Tag '{}' is known invalid", original_tag);
            return None;
        }
        
        // Handle special prefixes (fav:, pool:, set:, etc.)
        if let Some(cleaned) = self.handle_special_prefixes(&original_tag) {
            return Some(cleaned);
        }
        
        // Clean the tag
        let mut cleaned_tag = original_tag.clone();
        
        // Replace problematic characters
        for (from, to) in CHAR_REPLACEMENTS.iter() {
            cleaned_tag = cleaned_tag.replace(*from, &to.to_string());
        }
        
        // Remove any remaining invalid characters
        cleaned_tag = cleaned_tag.chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(*c, '_' | '-' | '.' | '(' | ')'))
            .collect();
        
        // Validate the cleaned tag
        if self.is_valid_tag(&cleaned_tag) {
            if cleaned_tag != original_tag {
                info!("Cleaned tag '{}' -> '{}'", original_tag, cleaned_tag);
            }
            self.known_valid_tags.insert(cleaned_tag.clone());
            Some(cleaned_tag)
        } else {
            warn!("Tag '{}' (cleaned: '{}') is invalid and cannot be used", original_tag, cleaned_tag);
            self.known_invalid_tags.insert(original_tag);
            None
        }
    }
    
    /// Handle special tag prefixes
    fn handle_special_prefixes(&self, tag: &str) -> Option<String> {
        // Handle fav: prefix
        if tag.starts_with("fav:") {
            let username = tag.strip_prefix("fav:").unwrap_or("");
            if !username.is_empty() {
                return Some(format!("fav:{}", username));
            }
        }
        
        // Handle pool: prefix
        if tag.starts_with("pool:") {
            let pool_id = tag.strip_prefix("pool:").unwrap_or("");
            if pool_id.chars().all(|c| c.is_ascii_digit()) {
                return Some(format!("pool:{}", pool_id));
            }
        }
        
        // Handle set: prefix
        if tag.starts_with("set:") {
            let set_id = tag.strip_prefix("set:").unwrap_or("");
            if set_id.chars().all(|c| c.is_ascii_digit()) {
                return Some(format!("set:{}", set_id));
            }
        }
        
        // Handle id: prefix for single posts
        if tag.starts_with("id:") {
            let post_id = tag.strip_prefix("id:").unwrap_or("");
            if post_id.chars().all(|c| c.is_ascii_digit()) {
                return Some(format!("id:{}", post_id));
            }
        }
        
        None
    }
    
    /// Check if a tag is valid according to e621 rules
    fn is_valid_tag(&self, tag: &str) -> bool {
        if tag.is_empty() || tag.len() > 100 {
            return false;
        }
        
        // Check against pattern
        VALID_TAG_PATTERN.is_match(tag)
    }
    
    /// Mark a tag as valid (after successful API response)
    pub fn mark_as_valid(&mut self, tag: &str) {
        self.known_valid_tags.insert(tag.to_lowercase());
        self.known_invalid_tags.remove(&tag.to_lowercase());
    }
    
    /// Mark a tag as invalid (after failed API response)
    pub fn mark_as_invalid(&mut self, tag: &str) {
        self.known_invalid_tags.insert(tag.to_lowercase());
        self.known_valid_tags.remove(&tag.to_lowercase());
    }
    
    /// Get statistics about validation
    pub fn get_stats(&self) -> (usize, usize) {
        (self.known_valid_tags.len(), self.known_invalid_tags.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tag_validation() {
        let mut validator = TagValidator::new();
        
        // Test valid tags
        assert!(validator.validate_and_clean_tag("blue_eyes").is_some());
        assert!(validator.validate_and_clean_tag("1girl").is_some());
        assert!(validator.validate_and_clean_tag("tag-with-dash").is_some());
        
        // Test invalid tags that get cleaned
        assert_eq!(validator.validate_and_clean_tag("tag with spaces"), Some("tag_with_spaces".to_string()));
        assert_eq!(validator.validate_and_clean_tag("tag/with/slash"), Some("tag_with_slash".to_string()));
        
        // Test special prefixes
        assert_eq!(validator.validate_and_clean_tag("fav:testuser"), Some("fav:testuser".to_string()));
        assert_eq!(validator.validate_and_clean_tag("pool:12345"), Some("pool:12345".to_string()));
    }
}
