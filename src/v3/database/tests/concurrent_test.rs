//! Concurrent Database Operations Test
//!
//! This module tests the concurrent performance and correctness of the
//! new database connection pool and write buffer systems under high load.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::v3::{DownloadJob, ContentType};
    
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, Instant};
    use std::path::PathBuf;
    use tempfile::tempdir;
    use tokio::time::sleep;
    
    /// Helper function to create a test download job
    fn create_test_job(post_id: u32, md5: &str) -> DownloadJob {
        DownloadJob {
            id: uuid::Uuid::new_v4(),
            post_id,
            md5: md5.to_string(),
            url: format!("https://static1.e621.net/data/{}/{}.jpg", &md5[..2], md5),
            file_extension: "jpg".to_string(),
            file_size: 1024000,
            tags: vec!["test".to_string(), "concurrent".to_string()],
            artists: vec!["test_artist".to_string()],
            content_type: ContentType::Image,
            is_blacklisted: false,
            priority: 1,
            retry_count: 0,
            created_at: SystemTime::now(),
        }
    }
    
    #[tokio::test]
    async fn test_concurrent_write_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("concurrent_test.db");
        
        // Initialize database with small buffer for faster testing
        let mut pool_config = PoolConfig::default();
        pool_config.max_read_connections = 8;
        pool_config.max_write_connections = 4;
        
        let mut buffer_config = WriteBufferConfig::default();
        buffer_config.max_batch_size = 10;
        buffer_config.flush_interval = Duration::from_millis(50);
        
        let database = Database::new(&db_path).await.unwrap();
        
        let start_time = Instant::now();
        
        // Create 100 concurrent write operations
        let mut handles = vec![];
        for i in 0..100 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let job = create_test_job(i, &format!("abcd{:032x}", i));
                let file_path = PathBuf::from(format!("/tmp/test_{}.jpg", i));
                
                db.record_download(&job, &file_path).await.unwrap();
                i
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        // Force flush to ensure all writes are committed
        database.flush().await.unwrap();
        
        let elapsed = start_time.elapsed();
        println!("Concurrent writes completed in {:?}", elapsed);
        
        // Verify all records were written correctly
        let stats = database.get_statistics().await.unwrap();
        assert_eq!(stats.total_downloads, 100);
        
        // Verify we can read all records
        for i in 0..100 {
            let exists = database.post_exists(i).await.unwrap();
            assert!(exists, "Post {} should exist", i);
        }
        
        results.sort();
        assert_eq!(results, (0..100).collect::<Vec<_>>());
        
        database.shutdown().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_concurrent_read_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("concurrent_read_test.db");
        
        let database = Database::new(&db_path).await.unwrap();
        
        // First, insert some test data
        for i in 0..50 {
            let job = create_test_job(i, &format!("read{:032x}", i));
            let file_path = PathBuf::from(format!("/tmp/read_{}.jpg", i));
            database.record_download(&job, &file_path).await.unwrap();
        }
        
        // Flush to ensure data is written
        database.flush().await.unwrap();
        
        let start_time = Instant::now();
        
        // Create 200 concurrent read operations
        let mut handles = vec![];
        for i in 0..200 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let post_id = i % 50;
                let exists = db.post_exists(post_id).await.unwrap();
                (i, exists)
            });
            handles.push(handle);
        }
        
        // Wait for all reads to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        let elapsed = start_time.elapsed();
        println!("Concurrent reads completed in {:?}", elapsed);
        
        // Verify all reads were successful
        for (i, exists) in results {
            assert!(exists, "Read {} should find existing post", i);
        }
        
        database.shutdown().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_mixed_concurrent_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mixed_concurrent_test.db");
        
        let database = Database::new(&db_path).await.unwrap();
        
        let start_time = Instant::now();
        
        // Mix of write and read operations
        let mut handles = vec![];
        
        // 50 write operations
        for i in 0..50 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let job = create_test_job(i, &format!("mixed{:032x}", i));
                let file_path = PathBuf::from(format!("/tmp/mixed_{}.jpg", i));
                db.record_download(&job, &file_path).await.unwrap();
                ("write", i)
            });
            handles.push(handle);
        }
        
        // Add a small delay to ensure some writes complete first
        sleep(Duration::from_millis(10)).await;
        
        // 100 read operations (some will find data, some won't)
        for i in 0..100 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let post_id = i % 75; // Some existing, some non-existing
                let exists = db.post_exists(post_id).await.unwrap();
                ("read", if exists { 1 } else { 0 })
            });
            handles.push(handle);
        }
        
        // 25 more write operations
        for i in 50..75 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let job = create_test_job(i, &format!("mixed{:032x}", i));
                let file_path = PathBuf::from(format!("/tmp/mixed_{}.jpg", i));
                db.record_download(&job, &file_path).await.unwrap();
                ("write", i)
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        let mut write_count = 0;
        let mut read_count = 0;
        
        for handle in handles {
            let (op_type, _value) = handle.await.unwrap();
            match op_type {
                "write" => write_count += 1,
                "read" => read_count += 1,
                _ => {}
            }
        }
        
        // Force flush
        database.flush().await.unwrap();
        
        let elapsed = start_time.elapsed();
        println!("Mixed concurrent operations completed in {:?}", elapsed);
        println!("Executed {} writes and {} reads", write_count, read_count);
        
        assert_eq!(write_count, 75);
        assert_eq!(read_count, 100);
        
        // Verify final state
        let stats = database.get_statistics().await.unwrap();
        assert_eq!(stats.total_downloads, 75);
        
        database.shutdown().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_search_operations_under_load() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("search_load_test.db");
        
        let database = Database::new(&db_path).await.unwrap();
        
        // Insert test data with various tags and artists
        for i in 0..100 {
            let mut job = create_test_job(i, &format!("search{:032x}", i));
            
            // Add variety to tags and artists
            job.tags = match i % 4 {
                0 => vec!["wolf".to_string(), "solo".to_string()],
                1 => vec!["dragon".to_string(), "male".to_string()],
                2 => vec!["cat".to_string(), "female".to_string()],
                3 => vec!["wolf".to_string(), "female".to_string()],
                _ => vec!["misc".to_string()],
            };
            
            job.artists = match i % 3 {
                0 => vec!["artist_a".to_string()],
                1 => vec!["artist_b".to_string()],
                2 => vec!["artist_c".to_string()],
                _ => vec!["unknown".to_string()],
            };
            
            let file_path = PathBuf::from(format!("/tmp/search_{}.jpg", i));
            database.record_download(&job, &file_path).await.unwrap();
        }
        
        // Flush to ensure all data is written
        database.flush().await.unwrap();
        
        let start_time = Instant::now();
        
        // Concurrent search operations
        let mut handles = vec![];
        
        // Tag searches
        for _i in 0..50 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let results = db.search_by_tag("wolf", 10).await.unwrap();
                ("tag_search", results.len())
            });
            handles.push(handle);
        }
        
        // Artist searches  
        for _i in 0..30 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let results = db.search_by_artist("artist_a", 10).await.unwrap();
                ("artist_search", results.len())
            });
            handles.push(handle);
        }
        
        // Statistics requests
        for _i in 0..20 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let stats = db.get_statistics().await.unwrap();
                ("stats", stats.total_downloads as usize)
            });
            handles.push(handle);
        }
        
        // Wait for all searches to complete
        let mut tag_searches = 0;
        let mut artist_searches = 0;
        let mut stats_requests = 0;
        
        for handle in handles {
            let (op_type, _count) = handle.await.unwrap();
            match op_type {
                "tag_search" => tag_searches += 1,
                "artist_search" => artist_searches += 1,
                "stats" => stats_requests += 1,
                _ => {}
            }
        }
        
        let elapsed = start_time.elapsed();
        println!("Concurrent search operations completed in {:?}", elapsed);
        println!("Executed {} tag searches, {} artist searches, {} stats requests", 
                tag_searches, artist_searches, stats_requests);
        
        assert_eq!(tag_searches, 50);
        assert_eq!(artist_searches, 30);
        assert_eq!(stats_requests, 20);
        
        database.shutdown().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_database_pool_stats() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("pool_stats_test.db");
        
        let database = Database::new(&db_path).await.unwrap();
        
        // Perform some operations to generate stats
        for i in 0..10 {
            let job = create_test_job(i, &format!("stats{:032x}", i));
            let file_path = PathBuf::from(format!("/tmp/stats_{}.jpg", i));
            database.record_download(&job, &file_path).await.unwrap();
            
            // Also do some reads
            let _exists = database.post_exists(i).await.unwrap();
        }
        
        database.flush().await.unwrap();
        
        // Check pool statistics
        let pool_stats = database.pool_stats();
        println!("Pool Stats: {:?}", pool_stats);
        
        assert!(pool_stats.connections_created > 0);
        assert_eq!(pool_stats.connection_errors, 0);
        
        // Check write buffer statistics  
        let buffer_stats = database.write_buffer_stats();
        println!("Buffer Stats: {:?}", buffer_stats);
        
        assert!(buffer_stats.total_operations > 0);
        assert!(buffer_stats.successful_operations > 0);
        assert_eq!(buffer_stats.failed_operations, 0);
        assert!(buffer_stats.batches_processed > 0);
        
        database.shutdown().await.unwrap();
    }
    
    #[tokio::test] 
    async fn test_database_shutdown_gracefully() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("shutdown_test.db");
        
        let database = Database::new(&db_path).await.unwrap();
        
        // Start some background operations
        let mut handles = vec![];
        for i in 0..20 {
            let db = database.clone();
            let handle = tokio::spawn(async move {
                let job = create_test_job(i, &format!("shutdown{:032x}", i));
                let file_path = PathBuf::from(format!("/tmp/shutdown_{}.jpg", i));
                
                // Add small delay to ensure operations are in progress
                sleep(Duration::from_millis(i as u64 * 5)).await;
                
                db.record_download(&job, &file_path).await
            });
            handles.push(handle);
        }
        
        // Allow some operations to start
        sleep(Duration::from_millis(50)).await;
        
        // Shutdown database
        let shutdown_start = Instant::now();
        database.shutdown().await.unwrap();
        let shutdown_time = shutdown_start.elapsed();
        
        println!("Database shutdown completed in {:?}", shutdown_time);
        
        // Wait for any remaining operations to complete
        for handle in handles {
            // Some may succeed, some may fail due to shutdown
            let _result = handle.await;
        }
        
        // Verify shutdown was reasonably fast (should not hang)
        assert!(shutdown_time < Duration::from_secs(5));
    }
}
