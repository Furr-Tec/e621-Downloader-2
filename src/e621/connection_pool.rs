use std::time::Duration;
use std::sync::Arc;
use parking_lot::RwLock;
use reqwest::Client;
use log::{debug, info, warn};
use dashmap::DashMap;
use tokio::time::Instant;

/// Connection pool manager that maintains HTTP clients and connection statistics
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    /// The main HTTP client with connection pooling
    client: Arc<Client>,
    /// Configuration for the connection pool
    config: ConnectionPoolConfig,
    /// Statistics tracking
    stats: Arc<RwLock<ConnectionPoolStats>>,
    /// Per-host connection tracking
    host_connections: Arc<DashMap<String, HostConnectionInfo>>,
}

#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of idle connections per host
    pub max_idle_per_host: usize,
    /// Maximum number of connections per host
    pub max_connections_per_host: usize,
    /// How long to keep idle connections alive
    pub idle_timeout: Duration,
    /// Connection timeout for new connections
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// TCP keepalive duration
    pub tcp_keepalive: Duration,
    /// Enable HTTP/2
    pub http2: bool,
    /// Enable compression
    pub compression: bool,
    /// User agent string
    pub user_agent: String,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 8,
            max_connections_per_host: 16,
            idle_timeout: Duration::from_secs(90),
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            tcp_keepalive: Duration::from_secs(60),
            http2: true,
            compression: true,
            user_agent: format!(
                "{}/{} (connection pooled)",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            ),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ConnectionPoolStats {
    /// Total number of requests made
    pub total_requests: u64,
    /// Number of requests that reused connections
    pub reused_connections: u64,
    /// Number of new connections created
    pub new_connections: u64,
    /// Number of connection timeouts
    pub connection_timeouts: u64,
    /// Number of request timeouts
    pub request_timeouts: u64,
    /// Total bytes downloaded
    pub total_bytes_downloaded: u64,
    /// Average request duration
    pub average_request_duration: Duration,
    /// Peak concurrent connections
    pub peak_concurrent_connections: u64,
}

#[derive(Debug, Clone)]
struct HostConnectionInfo {
    /// Number of active connections to this host
    active_connections: u64,
    /// Number of idle connections to this host
    idle_connections: u64,
    /// Last activity timestamp
    last_activity: Instant,
    /// Total requests to this host
    total_requests: u64,
}

impl Default for HostConnectionInfo {
    fn default() -> Self {
        Self {
            active_connections: 0,
            idle_connections: 0,
            last_activity: Instant::now(),
            total_requests: 0,
        }
    }
}

impl ConnectionPool {
    /// Create a new connection pool with default configuration
    pub fn new() -> Result<Self, reqwest::Error> {
        Self::with_config(ConnectionPoolConfig::default())
    }

    /// Create a new connection pool with custom configuration
    pub fn with_config(config: ConnectionPoolConfig) -> Result<Self, reqwest::Error> {
        let client = Self::build_client(&config)?;
        
        Ok(Self {
            client: Arc::new(client),
            config,
            stats: Arc::new(RwLock::new(ConnectionPoolStats::default())),
            host_connections: Arc::new(DashMap::new()),
        })
    }

    /// Build the HTTP client with optimized settings
    fn build_client(config: &ConnectionPoolConfig) -> Result<Client, reqwest::Error> {
        let mut client_builder = Client::builder()
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(Some(config.idle_timeout))
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .tcp_keepalive(Some(config.tcp_keepalive))
            .tcp_nodelay(true)
            .user_agent(&config.user_agent);

        if config.http2 {
            client_builder = client_builder.http2_prior_knowledge();
        }

        if config.compression {
            client_builder = client_builder.gzip(true).deflate(true).brotli(true);
        }

        // Use rustls for better performance and security
        client_builder = client_builder.use_rustls_tls();

        client_builder.build()
    }

    /// Get the underlying HTTP client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Make a GET request and track statistics
    pub async fn get(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let start_time = Instant::now();
        let host = self.extract_host(url).unwrap_or_else(|| "unknown".to_string());
        
        // Update connection tracking
        self.track_request_start(&host);
        
        let result = self.client.get(url).send().await;
        
        let duration = start_time.elapsed();
        
        // Update statistics based on result
        match &result {
            Ok(response) => {
                self.track_request_success(&host, duration, response.content_length());
                debug!("GET {} -> {} in {:?}", url, response.status(), duration);
            }
            Err(e) => {
                self.track_request_error(&host, duration, e);
                warn!("GET {} failed in {:?}: {}", url, duration, e);
            }
        }
        
        result
    }

    /// Make a POST request and track statistics
    pub async fn post(&self, url: &str) -> Result<reqwest::RequestBuilder, reqwest::Error> {
        let host = self.extract_host(url).unwrap_or_else(|| "unknown".to_string());
        self.track_request_start(&host);
        
        Ok(self.client.post(url))
    }

    /// Download bytes from a URL with progress tracking
    pub async fn download_bytes(&self, url: &str) -> Result<Vec<u8>, reqwest::Error> {
        let start_time = Instant::now();
        let host = self.extract_host(url).unwrap_or_else(|| "unknown".to_string());
        
        self.track_request_start(&host);
        
        let response = self.client.get(url).send().await?;
        let content_length = response.content_length();
        let bytes = response.bytes().await?;
        
        let duration = start_time.elapsed();
        self.track_request_success(&host, duration, content_length);
        
        debug!(
            "Downloaded {} bytes from {} in {:?}",
            bytes.len(),
            url,
            duration
        );
        
        Ok(bytes.to_vec())
    }

    /// Extract host from URL for tracking
    fn extract_host(&self, url: &str) -> Option<String> {
        url.parse::<reqwest::Url>()
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
    }

    /// Track the start of a request
    fn track_request_start(&self, host: &str) {
        let mut host_info = self.host_connections.entry(host.to_string())
            .or_insert_with(HostConnectionInfo::default);
        
        host_info.active_connections += 1;
        host_info.total_requests += 1;
        host_info.last_activity = Instant::now();
        
        let mut stats = self.stats.write();
        stats.total_requests += 1;
        stats.peak_concurrent_connections = stats.peak_concurrent_connections.max(
            self.host_connections.iter().map(|entry| entry.value().active_connections).sum()
        );
    }

    /// Track a successful request
    fn track_request_success(&self, host: &str, duration: Duration, content_length: Option<u64>) {
        if let Some(mut host_info) = self.host_connections.get_mut(host) {
            host_info.active_connections = host_info.active_connections.saturating_sub(1);
            host_info.idle_connections += 1;
            host_info.last_activity = Instant::now();
        }
        
        let mut stats = self.stats.write();
        stats.reused_connections += 1;
        
        if let Some(bytes) = content_length {
            stats.total_bytes_downloaded += bytes;
        }
        
        // Update average request duration
        let total_duration = stats.average_request_duration.as_millis() as u64 * (stats.total_requests - 1) + duration.as_millis() as u64;
        stats.average_request_duration = Duration::from_millis(total_duration / stats.total_requests);
    }

    /// Track a failed request
    fn track_request_error(&self, host: &str, duration: Duration, error: &reqwest::Error) {
        if let Some(mut host_info) = self.host_connections.get_mut(host) {
            host_info.active_connections = host_info.active_connections.saturating_sub(1);
            host_info.last_activity = Instant::now();
        }
        
        let mut stats = self.stats.write();
        
        if error.is_timeout() {
            stats.request_timeouts += 1;
        } else if error.is_connect() {
            stats.connection_timeouts += 1;
        }
        
        // Still update average duration for failed requests
        let total_duration = stats.average_request_duration.as_millis() as u64 * (stats.total_requests - 1) + duration.as_millis() as u64;
        stats.average_request_duration = Duration::from_millis(total_duration / stats.total_requests);
    }

    /// Get current connection pool statistics
    pub fn get_stats(&self) -> ConnectionPoolStats {
        self.stats.read().clone()
    }

    /// Get per-host connection information
    pub fn get_host_info(&self, host: &str) -> Option<HostConnectionInfo> {
        self.host_connections.get(host).map(|entry| entry.value().clone())
    }

    /// Get information about all hosts
    pub fn get_all_hosts(&self) -> Vec<(String, HostConnectionInfo)> {
        self.host_connections.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Clean up idle connections that have exceeded the timeout
    pub async fn cleanup_idle_connections(&self) {
        let now = Instant::now();
        let mut removed_hosts = Vec::new();
        
        for mut entry in self.host_connections.iter_mut() {
            let host_info = entry.value_mut();
            
            if now.duration_since(host_info.last_activity) > self.config.idle_timeout {
                if host_info.active_connections == 0 {
                    removed_hosts.push(entry.key().clone());
                }
            }
        }
        
        for host in removed_hosts {
            self.host_connections.remove(&host);
            debug!("Cleaned up idle connections for host: {}", host);
        }
    }

    /// Get connection pool efficiency metrics
    pub fn get_efficiency_metrics(&self) -> ConnectionPoolEfficiency {
        let stats = self.stats.read();
        let total_connections = stats.new_connections + stats.reused_connections;
        
        ConnectionPoolEfficiency {
            connection_reuse_rate: if total_connections > 0 {
                (stats.reused_connections as f64 / total_connections as f64) * 100.0
            } else {
                0.0
            },
            average_request_duration: stats.average_request_duration,
            timeout_rate: if stats.total_requests > 0 {
                ((stats.connection_timeouts + stats.request_timeouts) as f64 / stats.total_requests as f64) * 100.0
            } else {
                0.0
            },
            throughput_bytes_per_second: if stats.average_request_duration.as_secs_f64() > 0.0 {
                stats.total_bytes_downloaded as f64 / stats.average_request_duration.as_secs_f64()
            } else {
                0.0
            },
            active_hosts: self.host_connections.len(),
        }
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        let mut stats = self.stats.write();
        *stats = ConnectionPoolStats::default();
        self.host_connections.clear();
        info!("Connection pool statistics reset");
    }
}

/// Connection pool efficiency metrics
#[derive(Debug, Clone)]
pub struct ConnectionPoolEfficiency {
    /// Percentage of requests that reused existing connections
    pub connection_reuse_rate: f64,
    /// Average time per request
    pub average_request_duration: Duration,
    /// Percentage of requests that timed out
    pub timeout_rate: f64,
    /// Average bytes downloaded per second
    pub throughput_bytes_per_second: f64,
    /// Number of different hosts being tracked
    pub active_hosts: usize,
}

impl ConnectionPoolEfficiency {
    /// Format efficiency metrics for display
    pub fn format_metrics(&self) -> String {
        format!(
            "Reuse: {:.1}% | Avg Duration: {:?} | Timeout: {:.1}% | Throughput: {:.1} KB/s | Hosts: {}",
            self.connection_reuse_rate,
            self.average_request_duration,
            self.timeout_rate,
            self.throughput_bytes_per_second / 1024.0,
            self.active_hosts
        )
    }
}

/// Periodic task to clean up idle connections
pub async fn connection_cleanup_task(pool: Arc<ConnectionPool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    
    loop {
        interval.tick().await;
        pool.cleanup_idle_connections().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let pool = match ConnectionPool::new() {
            Ok(p) => p,
            Err(e) => panic!("Failed to create connection pool: {}", e),
        };
        let stats = pool.get_stats();
        
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.reused_connections, 0);
    }

    #[tokio::test]
    async fn test_host_extraction() {
        let pool = match ConnectionPool::new() {
            Ok(p) => p,
            Err(e) => panic!("Failed to create connection pool: {}", e),
        };
        
        let host = match pool.extract_host("https://example.com/path") {
            Some(h) => h,
            None => panic!("Failed to extract host from URL"),
        };
        assert_eq!(host, "example.com");
        
        let host = match pool.extract_host("https://api.example.com:8080/v1/data") {
            Some(h) => h,
            None => panic!("Failed to extract host from URL"),
        };
        assert_eq!(host, "api.example.com");
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let pool = match ConnectionPool::new() {
            Ok(p) => p,
            Err(e) => panic!("Failed to create connection pool: {}", e),
        };
        
        // Simulate request tracking
        pool.track_request_start("example.com");
        pool.track_request_success("example.com", Duration::from_millis(100), Some(1024));
        
        let stats = pool.get_stats();
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.reused_connections, 1);
        assert_eq!(stats.total_bytes_downloaded, 1024);
    }

    #[tokio::test]
    async fn test_efficiency_metrics() {
        let pool = match ConnectionPool::new() {
            Ok(p) => p,
            Err(e) => panic!("Failed to create connection pool: {}", e),
        };
        
        // Simulate some requests
        pool.track_request_start("example.com");
        pool.track_request_success("example.com", Duration::from_millis(100), Some(1024));
        pool.track_request_start("example.com");
        pool.track_request_success("example.com", Duration::from_millis(50), Some(2048));
        
        let efficiency = pool.get_efficiency_metrics();
        assert_eq!(efficiency.connection_reuse_rate, 100.0);
        assert_eq!(efficiency.active_hosts, 1);
        assert!(efficiency.throughput_bytes_per_second > 0.0);
    }
}
