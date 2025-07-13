use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use crate::e621::sender::entries::{BulkPostEntry, TagEntry, AliasEntry};
use crate::e621::performance::{record_cache_hit, record_cache_miss};

/// Cache entry with expiration time
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    data: T,
    expires_at: Instant,
}

impl<T> CacheEntry<T> {
    fn new(data: T, ttl: Duration) -> Self {
        Self {
            data,
            expires_at: Instant::now() + ttl,
        }
    }
    
    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Response cache for different types of API responses
pub struct ResponseCache {
    /// Cache for bulk post searches (tag + page -> BulkPostEntry)
    post_cache: Arc<RwLock<LruCache<String, CacheEntry<BulkPostEntry>>>>,
    /// Cache for tag searches (tag -> Vec<TagEntry>)
    tag_cache: Arc<RwLock<LruCache<String, CacheEntry<Vec<TagEntry>>>>>,
    /// Cache for alias searches (tag -> Vec<AliasEntry>)
    alias_cache: Arc<RwLock<LruCache<String, CacheEntry<Vec<AliasEntry>>>>>,
    /// Cache for duplicate checks (filename -> bool)
    duplicate_cache: Arc<DashMap<String, CacheEntry<bool>>>,
    /// Cache for post ID existence checks (post_id -> bool)
    post_id_cache: Arc<DashMap<i64, CacheEntry<bool>>>,
    /// Cache settings
    config: CacheConfig,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Time-to-live for post search results
    pub post_ttl: Duration,
    /// Time-to-live for tag search results
    pub tag_ttl: Duration,
    /// Time-to-live for alias search results
    pub alias_ttl: Duration,
    /// Time-to-live for duplicate checks
    pub duplicate_ttl: Duration,
    /// Time-to-live for post ID checks
    pub post_id_ttl: Duration,
    /// Maximum number of cached post search results
    pub max_post_cache_size: usize,
    /// Maximum number of cached tag search results
    pub max_tag_cache_size: usize,
    /// Maximum number of cached alias search results
    pub max_alias_cache_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            post_ttl: Duration::from_secs(300),        // 5 minutes
            tag_ttl: Duration::from_secs(1800),        // 30 minutes
            alias_ttl: Duration::from_secs(3600),      // 1 hour
            duplicate_ttl: Duration::from_secs(3600),  // 1 hour
            post_id_ttl: Duration::from_secs(3600),    // 1 hour
            max_post_cache_size: 1000,
            max_tag_cache_size: 500,
            max_alias_cache_size: 200,
        }
    }
}

impl ResponseCache {
    /// Create a new response cache with default configuration
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }
    
    /// Create a new response cache with custom configuration
    pub fn with_config(config: CacheConfig) -> Self {
        Self {
            post_cache: Arc::new(RwLock::new(LruCache::new(config.max_post_cache_size.try_into().unwrap()))),
            tag_cache: Arc::new(RwLock::new(LruCache::new(config.max_tag_cache_size.try_into().unwrap()))),
            alias_cache: Arc::new(RwLock::new(LruCache::new(config.max_alias_cache_size.try_into().unwrap()))),
            duplicate_cache: Arc::new(DashMap::new()),
            post_id_cache: Arc::new(DashMap::new()),
            config,
        }
    }
    
    /// Get cached post search results
    pub fn get_post_search(&self, search_key: &str) -> Option<BulkPostEntry> {
        let mut cache = self.post_cache.write();
        if let Some(entry) = cache.get(search_key) {
            if !entry.is_expired() {
                record_cache_hit("post_search");
                return Some(entry.data.clone());
            } else {
                cache.pop(search_key);
            }
        }
        record_cache_miss("post_search");
        None
    }
    
    /// Cache post search results
    pub fn cache_post_search(&self, search_key: String, data: BulkPostEntry) {
        let mut cache = self.post_cache.write();
        cache.put(search_key, CacheEntry::new(data, self.config.post_ttl));
    }
    
    /// Get cached tag search results
    pub fn get_tag_search(&self, tag: &str) -> Option<Vec<TagEntry>> {
        let mut cache = self.tag_cache.write();
        if let Some(entry) = cache.get(tag) {
            if !entry.is_expired() {
                record_cache_hit("tag_search");
                return Some(entry.data.clone());
            } else {
                cache.pop(tag);
            }
        }
        record_cache_miss("tag_search");
        None
    }
    
    /// Cache tag search results
    pub fn cache_tag_search(&self, tag: String, data: Vec<TagEntry>) {
        let mut cache = self.tag_cache.write();
        cache.put(tag, CacheEntry::new(data, self.config.tag_ttl));
    }
    
    /// Get cached alias search results
    pub fn get_alias_search(&self, tag: &str) -> Option<Vec<AliasEntry>> {
        let mut cache = self.alias_cache.write();
        if let Some(entry) = cache.get(tag) {
            if !entry.is_expired() {
                record_cache_hit("alias_search");
                return Some(entry.data.clone());
            } else {
                cache.pop(tag);
            }
        }
        record_cache_miss("alias_search");
        None
    }
    
    /// Cache alias search results
    pub fn cache_alias_search(&self, tag: String, data: Vec<AliasEntry>) {
        let mut cache = self.alias_cache.write();
        cache.put(tag, CacheEntry::new(data, self.config.alias_ttl));
    }
    
    /// Get cached duplicate check result
    pub fn get_duplicate_check(&self, filename: &str) -> Option<bool> {
        if let Some(entry) = self.duplicate_cache.get(filename) {
            if !entry.is_expired() {
                record_cache_hit("duplicate_check");
                return Some(entry.data);
            } else {
                self.duplicate_cache.remove(filename);
            }
        }
        record_cache_miss("duplicate_check");
        None
    }
    
    /// Cache duplicate check result
    pub fn cache_duplicate_check(&self, filename: String, is_duplicate: bool) {
        self.duplicate_cache.insert(filename, CacheEntry::new(is_duplicate, self.config.duplicate_ttl));
    }
    
    /// Batch duplicate check for multiple files (more efficient)
    pub fn batch_duplicate_check(&self, filenames: &[String]) -> Vec<(String, Option<bool>)> {
        filenames.iter().map(|filename| {
            let result = self.get_duplicate_check(filename);
            (filename.clone(), result)
        }).collect()
    }
    
    /// Batch cache duplicate check results
    pub fn batch_cache_duplicate_check(&self, results: &[(String, bool)]) {
        for (filename, is_duplicate) in results {
            self.cache_duplicate_check(filename.clone(), *is_duplicate);
        }
    }
    
    /// Get cached post ID existence check result
    pub fn get_post_id_check(&self, post_id: i64) -> Option<bool> {
        if let Some(entry) = self.post_id_cache.get(&post_id) {
            if !entry.is_expired() {
                record_cache_hit("post_id_check");
                return Some(entry.data);
            } else {
                self.post_id_cache.remove(&post_id);
            }
        }
        record_cache_miss("post_id_check");
        None
    }
    
    /// Cache post ID existence check result
    pub fn cache_post_id_check(&self, post_id: i64, exists: bool) {
        self.post_id_cache.insert(post_id, CacheEntry::new(exists, self.config.post_id_ttl));
    }
    
    /// Clear all caches
    pub fn clear_all(&self) {
        self.post_cache.write().clear();
        self.tag_cache.write().clear();
        self.alias_cache.write().clear();
        self.duplicate_cache.clear();
        self.post_id_cache.clear();
    }
    
    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStats {
        CacheStats {
            post_cache_size: self.post_cache.read().len(),
            tag_cache_size: self.tag_cache.read().len(),
            alias_cache_size: self.alias_cache.read().len(),
            duplicate_cache_size: self.duplicate_cache.len(),
            post_id_cache_size: self.post_id_cache.len(),
        }
    }
    
    /// Clean up expired entries
    pub fn cleanup_expired(&self) {
        // Clean up LRU caches - expired entries are automatically removed on access
        // Clean up DashMap caches manually
        self.duplicate_cache.retain(|_, entry| !entry.is_expired());
        self.post_id_cache.retain(|_, entry| !entry.is_expired());
    }
}

/// Cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub post_cache_size: usize,
    pub tag_cache_size: usize,
    pub alias_cache_size: usize,
    pub duplicate_cache_size: usize,
    pub post_id_cache_size: usize,
}

/// Global cache instance
use once_cell::sync::Lazy;
pub static GLOBAL_CACHE: Lazy<ResponseCache> = Lazy::new(|| {
    ResponseCache::new()
});

/// Helper function to create a cache key for post searches
pub fn create_post_search_key(tag: &str, page: u16) -> String {
    format!("{}::{}", tag, page)
}

/// Helper function to clean up expired cache entries periodically
pub fn start_cache_cleanup_task() {
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(Duration::from_secs(300)); // Every 5 minutes
            GLOBAL_CACHE.cleanup_expired();
        }
    });
}

/// Initialize the global cache and start cleanup task
pub fn init_cache() {
    Lazy::force(&GLOBAL_CACHE);
    start_cache_cleanup_task();
    log::info!("Response cache initialized");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[test]
    fn test_cache_basic_operations() {
        let cache = ResponseCache::new();
        let key = "test_key".to_string();
        
        // Test miss
        assert!(cache.get_post_search(&key).is_none());
        
        // Test hit
        let entry = BulkPostEntry::default();
        cache.cache_post_search(key.clone(), entry.clone());
        assert!(cache.get_post_search(&key).is_some());
    }
    
    #[test]
    fn test_cache_expiration() {
        let config = CacheConfig {
            post_ttl: Duration::from_millis(10), // Very short TTL
            ..Default::default()
        };
        let cache = ResponseCache::with_config(config);
        let key = "test_key".to_string();
        
        // Cache entry
        let entry = BulkPostEntry::default();
        cache.cache_post_search(key.clone(), entry);
        
        // Should be available immediately
        assert!(cache.get_post_search(&key).is_some());
        
        // Wait for expiration
        thread::sleep(Duration::from_millis(20));
        
        // Should be expired
        assert!(cache.get_post_search(&key).is_none());
    }
}
