use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use lru::LruCache;
use log::{debug, info, trace};
use std::num::NonZeroUsize;

/// Whitelist cache that stores generated whitelists for search terms
#[derive(Debug, Clone)]
pub struct WhitelistCache {
    /// LRU cache for storing whitelists
    cache: Arc<RwLock<LruCache<String, CachedWhitelist>>>,
    /// Configuration for the cache
    config: WhitelistCacheConfig,
    /// Statistics tracking
    stats: Arc<RwLock<WhitelistCacheStats>>,
}

#[derive(Debug, Clone)]
pub struct WhitelistCacheConfig {
    /// Maximum number of whitelists to cache
    pub max_entries: usize,
    /// How long to keep whitelists in cache
    pub ttl: Duration,
    /// Whether to enable statistics tracking
    pub enable_stats: bool,
    /// Whether to enable cache warming (pre-generate common whitelists)
    pub enable_warming: bool,
}

impl Default for WhitelistCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(300), // 5 minutes
            enable_stats: true,
            enable_warming: true,
        }
    }
}

#[derive(Debug, Clone)]
struct CachedWhitelist {
    /// The whitelist entries
    whitelist: Vec<String>,
    /// When this entry was created
    created_at: Instant,
    /// How many times this entry has been accessed
    access_count: u64,
    /// Last time this entry was accessed
    last_accessed: Instant,
}

impl CachedWhitelist {
    fn new(whitelist: Vec<String>) -> Self {
        let now = Instant::now();
        Self {
            whitelist,
            created_at: now,
            access_count: 0,
            last_accessed: now,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    fn access(&mut self) -> &Vec<String> {
        self.access_count += 1;
        self.last_accessed = Instant::now();
        &self.whitelist
    }
}

#[derive(Debug, Default, Clone)]
pub struct WhitelistCacheStats {
    /// Total number of cache lookups
    pub total_lookups: u64,
    /// Number of cache hits
    pub cache_hits: u64,
    /// Number of cache misses
    pub cache_misses: u64,
    /// Number of expired entries removed
    pub expired_removals: u64,
    /// Number of LRU evictions
    pub lru_evictions: u64,
    /// Total time spent generating whitelists
    pub total_generation_time: Duration,
    /// Number of whitelists generated
    pub whitelists_generated: u64,
}

impl WhitelistCacheStats {
    /// Calculate cache hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        if self.total_lookups == 0 {
            0.0
        } else {
            (self.cache_hits as f64 / self.total_lookups as f64) * 100.0
        }
    }

    /// Calculate average generation time
    pub fn average_generation_time(&self) -> Duration {
        if self.whitelists_generated == 0 {
            Duration::ZERO
        } else {
            self.total_generation_time / self.whitelists_generated as u32
        }
    }

    /// Format stats for display
    pub fn format_stats(&self) -> String {
        format!(
            "Hit Rate: {:.1}% ({}/{}) | Generated: {} (avg: {:?}) | Expired: {} | Evicted: {}",
            self.hit_rate(),
            self.cache_hits,
            self.total_lookups,
            self.whitelists_generated,
            self.average_generation_time(),
            self.expired_removals,
            self.lru_evictions
        )
    }
}

impl WhitelistCache {
    /// Create a new whitelist cache with default configuration
    pub fn new() -> Self {
        Self::with_config(WhitelistCacheConfig::default())
    }

    /// Create a new whitelist cache with custom configuration
    pub fn with_config(config: WhitelistCacheConfig) -> Self {
        let cache_size = NonZeroUsize::new(config.max_entries)
            .unwrap_or_else(|| NonZeroUsize::new(1000)
                .expect("1000 is always a valid non-zero usize"));
        
        let cache = Self {
            cache: Arc::new(RwLock::new(LruCache::new(cache_size))),
            config,
            stats: Arc::new(RwLock::new(WhitelistCacheStats::default())),
        };

        if cache.config.enable_warming {
            cache.warm_cache();
        }

        cache
    }

    /// Get or generate a whitelist for a search term
    pub fn get_or_generate<F>(&self, search_term: &str, generator: F) -> Vec<String>
    where
        F: FnOnce() -> Vec<String>,
    {
        let cache_key = self.normalize_search_term(search_term);
        
        // Try to get from cache first
        if let Some(cached) = self.get_cached(&cache_key) {
            return cached;
        }

        // Generate new whitelist
        let start_time = Instant::now();
        let whitelist = generator();
        let generation_time = start_time.elapsed();

        // Cache the generated whitelist
        self.store_cached(&cache_key, whitelist.clone(), generation_time);

        whitelist
    }

    /// Get a cached whitelist if available and not expired
    fn get_cached(&self, cache_key: &str) -> Option<Vec<String>> {
        let mut cache = self.cache.write();
        
        if let Some(cached) = cache.get_mut(cache_key) {
            // Check if expired
            if cached.is_expired(self.config.ttl) {
                cache.pop(cache_key);
                if self.config.enable_stats {
                    let mut stats = self.stats.write();
                    stats.expired_removals += 1;
                    stats.cache_misses += 1;
                    stats.total_lookups += 1;
                }
                debug!("Whitelist cache expired for key: {}", cache_key);
                return None;
            }

            // Access the cached entry
            let whitelist = cached.access().clone();
            
            if self.config.enable_stats {
                let mut stats = self.stats.write();
                stats.cache_hits += 1;
                stats.total_lookups += 1;
            }
            
            trace!("Whitelist cache hit for key: {} (access count: {})", cache_key, cached.access_count);
            return Some(whitelist);
        }

        if self.config.enable_stats {
            let mut stats = self.stats.write();
            stats.cache_misses += 1;
            stats.total_lookups += 1;
        }

        None
    }

    /// Store a generated whitelist in the cache
    fn store_cached(&self, cache_key: &str, whitelist: Vec<String>, generation_time: Duration) {
        let mut cache = self.cache.write();
        
        let cached_entry = CachedWhitelist::new(whitelist);
        let was_evicted = cache.put(cache_key.to_string(), cached_entry).is_some();

        if self.config.enable_stats {
            let mut stats = self.stats.write();
            stats.whitelists_generated += 1;
            stats.total_generation_time += generation_time;
            
            if was_evicted {
                stats.lru_evictions += 1;
            }
        }

        debug!("Stored whitelist in cache for key: {} (generation time: {:?})", cache_key, generation_time);
    }

    /// Normalize search term to create consistent cache keys
    fn normalize_search_term(&self, search_term: &str) -> String {
        // Convert to lowercase and trim whitespace
        let normalized = search_term.trim().to_lowercase();
        
        // Remove common prefixes/suffixes that don't affect whitelist generation
        let normalized = normalized
            .strip_prefix("artist:")
            .unwrap_or(&normalized)
            .strip_prefix("tag:")
            .unwrap_or(&normalized);
        
        // Replace multiple spaces with single space
        let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
        
        normalized
    }

    /// Pre-warm the cache with common search terms
    fn warm_cache(&self) {
        let common_terms = vec![
            "furry", "anthro", "male", "female", "solo", "duo", "group",
            "canine", "feline", "dragon", "wolf", "fox", "cat", "dog",
            "digital_media", "traditional_media", "sketch", "painting",
            "sfw", "safe", "explicit", "questionable", "suggestive",
        ];
        
        let terms_count = common_terms.len();

        for term in common_terms {
            let cache_key = self.normalize_search_term(term);
            
            // Generate a basic whitelist for common terms
            let whitelist = self.generate_basic_whitelist(term);
            
            let mut cache = self.cache.write();
            cache.put(cache_key.clone(), CachedWhitelist::new(whitelist));
            
            trace!("Warmed cache for common term: {}", term);
        }

        info!("Whitelist cache warmed with {} common terms", terms_count);
    }

    /// Generate a basic whitelist for common terms (used for cache warming)
    fn generate_basic_whitelist(&self, term: &str) -> Vec<String> {
        vec![
            format!("artist:{}", term),
            term.to_string(),
            format!("{}*", term), // Wildcard variant
        ]
    }

    /// Clear all cached whitelists
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
        
        if self.config.enable_stats {
            let mut stats = self.stats.write();
            *stats = WhitelistCacheStats::default();
        }
        
        info!("Whitelist cache cleared");
    }

    /// Remove expired entries from the cache
    pub fn cleanup_expired(&self) {
        let mut cache = self.cache.write();
        let mut expired_keys = Vec::new();

        // Find expired entries
        for (key, cached) in cache.iter() {
            if cached.is_expired(self.config.ttl) {
                expired_keys.push(key.clone());
            }
        }

        // Remove expired entries
        let mut removed_count = 0;
        for key in expired_keys {
            cache.pop(&key);
            removed_count += 1;
        }

        if self.config.enable_stats && removed_count > 0 {
            let mut stats = self.stats.write();
            stats.expired_removals += removed_count;
        }

        if removed_count > 0 {
            debug!("Cleaned up {} expired whitelist cache entries", removed_count);
        }
    }

    /// Get current cache statistics
    pub fn get_stats(&self) -> WhitelistCacheStats {
        if self.config.enable_stats {
            (*self.stats.read()).clone()
        } else {
            WhitelistCacheStats::default()
        }
    }

    /// Get information about current cache contents
    pub fn get_cache_info(&self) -> WhitelistCacheInfo {
        let cache = self.cache.read();
        
        let mut total_access_count = 0;
        let mut oldest_entry = None;
        let mut newest_entry = None;
        
        for (_key, cached) in cache.iter() {
            total_access_count += cached.access_count;
            
            match oldest_entry {
                None => oldest_entry = Some(cached.created_at),
                Some(oldest) if cached.created_at < oldest => oldest_entry = Some(cached.created_at),
                _ => {}
            }
            
            match newest_entry {
                None => newest_entry = Some(cached.created_at),
                Some(newest) if cached.created_at > newest => newest_entry = Some(cached.created_at),
                _ => {}
            }
        }
        
        WhitelistCacheInfo {
            current_size: cache.len(),
            max_size: cache.cap().get(),
            total_access_count,
            oldest_entry_age: oldest_entry.map(|t| t.elapsed()),
            newest_entry_age: newest_entry.map(|t| t.elapsed()),
        }
    }

    /// Get the most frequently accessed cache keys
    pub fn get_top_accessed_keys(&self, limit: usize) -> Vec<(String, u64)> {
        let cache = self.cache.read();
        let mut entries: Vec<(String, u64)> = cache.iter()
            .map(|(key, cached)| (key.clone(), cached.access_count))
            .collect();
        
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(limit);
        entries
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        if self.config.enable_stats {
            let mut stats = self.stats.write();
            *stats = WhitelistCacheStats::default();
            info!("Whitelist cache statistics reset");
        }
    }
}

/// Information about current cache state
#[derive(Debug, Clone)]
pub struct WhitelistCacheInfo {
    /// Current number of entries in cache
    pub current_size: usize,
    /// Maximum number of entries the cache can hold
    pub max_size: usize,
    /// Total number of times any entry has been accessed
    pub total_access_count: u64,
    /// Age of the oldest entry in cache
    pub oldest_entry_age: Option<Duration>,
    /// Age of the newest entry in cache
    pub newest_entry_age: Option<Duration>,
}

impl WhitelistCacheInfo {
    /// Calculate cache utilization as a percentage
    pub fn utilization(&self) -> f64 {
        if self.max_size == 0 {
            0.0
        } else {
            (self.current_size as f64 / self.max_size as f64) * 100.0
        }
    }

    /// Format cache info for display
    pub fn format_info(&self) -> String {
        format!(
            "Size: {}/{} ({:.1}%) | Total Access: {} | Age Range: {:?} - {:?}",
            self.current_size,
            self.max_size,
            self.utilization(),
            self.total_access_count,
            self.newest_entry_age,
            self.oldest_entry_age
        )
    }
}

/// Periodic task to clean up expired cache entries
pub async fn cache_cleanup_task(cache: Arc<WhitelistCache>) {
    let mut interval = tokio::time::interval(Duration::from_secs(300)); // Clean up every 5 minutes
    
    loop {
        interval.tick().await;
        cache.cleanup_expired();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_whitelist_cache_basic() {
        let cache = WhitelistCache::new();
        
        // Test cache miss and generation
        let whitelist = cache.get_or_generate("test_tag", || {
            vec!["artist:test_tag".to_string(), "test_tag".to_string()]
        });
        
        assert_eq!(whitelist.len(), 2);
        assert_eq!(whitelist[0], "artist:test_tag");
        assert_eq!(whitelist[1], "test_tag");
        
        // Test cache hit
        let cached_whitelist = cache.get_or_generate("test_tag", || {
            panic!("Should not be called for cache hit");
        });
        
        assert_eq!(cached_whitelist, whitelist);
    }

    #[test]
    fn test_search_term_normalization() {
        let cache = WhitelistCache::new();
        
        let key1 = cache.normalize_search_term("  Test_Tag  ");
        let key2 = cache.normalize_search_term("test_tag");
        let key3 = cache.normalize_search_term("artist:test_tag");
        
        assert_eq!(key1, "test_tag");
        assert_eq!(key2, "test_tag");
        assert_eq!(key3, "test_tag");
    }

    #[test]
    fn test_cache_stats() {
        let cache = WhitelistCache::new();
        
        // Generate some cache activity
        cache.get_or_generate("tag1", || vec!["result1".to_string()]);
        cache.get_or_generate("tag1", || vec!["result2".to_string()]); // Should be cache hit
        cache.get_or_generate("tag2", || vec!["result3".to_string()]);
        
        let stats = cache.get_stats();
        assert_eq!(stats.total_lookups, 3);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 2);
        assert_eq!(stats.whitelists_generated, 2);
        assert_eq!(stats.hit_rate(), 33.333333333333336);
    }

    #[test]
    fn test_cache_expiration() {
        let config = WhitelistCacheConfig {
            ttl: Duration::from_millis(100),
            ..Default::default()
        };
        let cache = WhitelistCache::with_config(config);
        
        // Store an entry
        cache.get_or_generate("test", || vec!["result".to_string()]);
        
        // Should be in cache
        let result1 = cache.get_or_generate("test", || panic!("Should not generate"));
        assert_eq!(result1, vec!["result".to_string()]);
        
        // Wait for expiration
        thread::sleep(Duration::from_millis(150));
        
        // Should generate again due to expiration
        let result2 = cache.get_or_generate("test", || vec!["new_result".to_string()]);
        assert_eq!(result2, vec!["new_result".to_string()]);
    }

    #[test]
    fn test_cache_info() {
        let cache = WhitelistCache::new();
        
        // Add some entries
        cache.get_or_generate("tag1", || vec!["result1".to_string()]);
        cache.get_or_generate("tag2", || vec!["result2".to_string()]);
        
        let info = cache.get_cache_info();
        assert_eq!(info.current_size, 2);
        assert!(info.total_access_count > 0);
        assert!(info.oldest_entry_age.is_some());
        assert!(info.newest_entry_age.is_some());
    }

    #[test]
    fn test_top_accessed_keys() {
        let cache = WhitelistCache::new();
        
        // Create entries with different access patterns
        cache.get_or_generate("popular", || vec!["result".to_string()]);
        cache.get_or_generate("popular", || vec!["result".to_string()]); // Hit
        cache.get_or_generate("popular", || vec!["result".to_string()]); // Hit
        cache.get_or_generate("less_popular", || vec!["result".to_string()]);
        cache.get_or_generate("less_popular", || vec!["result".to_string()]); // Hit
        
        let top_keys = cache.get_top_accessed_keys(5);
        assert_eq!(top_keys.len(), 2);
        assert_eq!(top_keys[0].0, "popular");
        assert_eq!(top_keys[0].1, 3);
        assert_eq!(top_keys[1].0, "less_popular");
        assert_eq!(top_keys[1].1, 2);
    }
}
