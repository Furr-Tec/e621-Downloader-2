use std::time::{Duration, Instant};
use std::sync::Arc;
use parking_lot::RwLock;
use log::{debug, info, warn};

/// Adaptive rate limiter that adjusts delays based on API response times
#[derive(Debug, Clone)]
pub struct AdaptiveRateLimiter {
    /// Configuration for the rate limiter
    config: RateLimiterConfig,
    /// Current state of the rate limiter
    state: Arc<RwLock<RateLimiterState>>,
}

#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Base delay between requests when API is responsive
    pub base_delay: Duration,
    /// Maximum delay between requests
    pub max_delay: Duration,
    /// Minimum delay between requests
    pub min_delay: Duration,
    /// Response time threshold to consider a request "slow"
    pub slow_request_threshold: Duration,
    /// Response time threshold to consider a request "very slow"
    pub very_slow_request_threshold: Duration,
    /// Factor to multiply delay by when requests are slow
    pub backoff_multiplier: f64,
    /// Factor to multiply delay by when requests are fast
    pub recovery_multiplier: f64,
    /// Number of consecutive slow requests before increasing delay
    pub slow_request_tolerance: u32,
    /// Number of consecutive fast requests before decreasing delay
    pub fast_request_tolerance: u32,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(300),
            max_delay: Duration::from_secs(10),
            min_delay: Duration::from_millis(100),
            slow_request_threshold: Duration::from_secs(2),
            very_slow_request_threshold: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            recovery_multiplier: 0.8,
            slow_request_tolerance: 2,
            fast_request_tolerance: 3,
        }
    }
}

#[derive(Debug)]
struct RateLimiterState {
    /// Current delay between requests
    current_delay: Duration,
    /// Number of consecutive slow requests
    consecutive_slow_requests: u32,
    /// Number of consecutive fast requests
    consecutive_fast_requests: u32,
    /// Last request timestamp
    last_request_time: Option<Instant>,
    /// Recent response times for analysis
    recent_response_times: Vec<Duration>,
    /// Maximum number of response times to track
    max_history_size: usize,
}

impl Default for RateLimiterState {
    fn default() -> Self {
        Self {
            current_delay: Duration::from_millis(300),
            consecutive_slow_requests: 0,
            consecutive_fast_requests: 0,
            last_request_time: None,
            recent_response_times: Vec::new(),
            max_history_size: 20,
        }
    }
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter with default configuration
    pub fn new() -> Self {
        Self::with_config(RateLimiterConfig::default())
    }

    /// Create a new adaptive rate limiter with custom configuration
    pub fn with_config(config: RateLimiterConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(RateLimiterState::default())),
        }
    }

    /// Wait for the appropriate amount of time before making a request
    pub async fn wait_for_request(&self) -> Duration {
        let delay = {
            let state = self.state.read();
            state.current_delay
        };

        debug!("Rate limiter waiting for {:?}", delay);
        tokio::time::sleep(delay).await;
        
        // Update last request time
        {
            let mut state = self.state.write();
            state.last_request_time = Some(Instant::now());
        }

        delay
    }

    /// Record the response time of a request to adjust future delays
    pub fn record_response_time(&self, response_time: Duration) {
        let mut state = self.state.write();
        
        // Add to recent response times
        state.recent_response_times.push(response_time);
        if state.recent_response_times.len() > state.max_history_size {
            state.recent_response_times.remove(0);
        }

        // Categorize request speed
        let is_slow = response_time >= self.config.slow_request_threshold;
        let is_very_slow = response_time >= self.config.very_slow_request_threshold;
        let is_fast = response_time < self.config.slow_request_threshold / 2;

        // Update consecutive counters
        if is_slow {
            state.consecutive_slow_requests += 1;
            state.consecutive_fast_requests = 0;
            
            if is_very_slow {
                warn!("Very slow API response detected: {:?}", response_time);
            } else {
                debug!("Slow API response detected: {:?}", response_time);
            }
        } else if is_fast {
            state.consecutive_fast_requests += 1;
            state.consecutive_slow_requests = 0;
            debug!("Fast API response detected: {:?}", response_time);
        } else {
            // Normal response time, reset counters
            state.consecutive_slow_requests = 0;
            state.consecutive_fast_requests = 0;
        }

        // Adjust delay based on response patterns
        let old_delay = state.current_delay;
        
        if state.consecutive_slow_requests >= self.config.slow_request_tolerance {
            // Increase delay for slow requests
            let multiplier = if is_very_slow {
                self.config.backoff_multiplier * 1.5 // More aggressive backoff for very slow requests
            } else {
                self.config.backoff_multiplier
            };
            
            state.current_delay = std::cmp::min(
                Duration::from_secs_f64(state.current_delay.as_secs_f64() * multiplier),
                self.config.max_delay
            );
            
            info!(
                "API response slow, increasing delay from {:?} to {:?}",
                old_delay, state.current_delay
            );
        } else if state.consecutive_fast_requests >= self.config.fast_request_tolerance {
            // Decrease delay for fast requests
            state.current_delay = std::cmp::max(
                Duration::from_secs_f64(state.current_delay.as_secs_f64() * self.config.recovery_multiplier),
                self.config.min_delay
            );
            
            debug!(
                "API response fast, decreasing delay from {:?} to {:?}",
                old_delay, state.current_delay
            );
        }
    }

    /// Get current delay setting
    pub fn current_delay(&self) -> Duration {
        self.state.read().current_delay
    }

    /// Get average response time from recent requests
    pub fn average_response_time(&self) -> Option<Duration> {
        let state = self.state.read();
        if state.recent_response_times.is_empty() {
            None
        } else {
            let total_ms: u128 = state.recent_response_times.iter()
                .map(|d| d.as_millis())
                .sum();
            let avg_ms = total_ms / state.recent_response_times.len() as u128;
            Some(Duration::from_millis(avg_ms as u64))
        }
    }

    /// Reset the rate limiter to initial state
    pub fn reset(&self) {
        let mut state = self.state.write();
        state.current_delay = self.config.base_delay;
        state.consecutive_slow_requests = 0;
        state.consecutive_fast_requests = 0;
        state.recent_response_times.clear();
        info!("Rate limiter reset to base delay: {:?}", self.config.base_delay);
    }

    /// Get statistics about the rate limiter's performance
    pub fn get_stats(&self) -> RateLimiterStats {
        let state = self.state.read();
        RateLimiterStats {
            current_delay: state.current_delay,
            consecutive_slow_requests: state.consecutive_slow_requests,
            consecutive_fast_requests: state.consecutive_fast_requests,
            average_response_time: self.average_response_time(),
            total_requests: state.recent_response_times.len(),
            slow_requests: state.recent_response_times.iter()
                .filter(|&&rt| rt >= self.config.slow_request_threshold)
                .count(),
        }
    }
}

/// Statistics about rate limiter performance
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub current_delay: Duration,
    pub consecutive_slow_requests: u32,
    pub consecutive_fast_requests: u32,
    pub average_response_time: Option<Duration>,
    pub total_requests: usize,
    pub slow_requests: usize,
}

impl RateLimiterStats {
    /// Calculate the percentage of slow requests
    pub fn slow_request_percentage(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            (self.slow_requests as f64 / self.total_requests as f64) * 100.0
        }
    }

    /// Format stats for display
    pub fn format_stats(&self) -> String {
        format!(
            "Delay: {:?} | Avg Response: {:?} | Slow: {:.1}% ({}/{}) | Consecutive: {} slow, {} fast",
            self.current_delay,
            self.average_response_time.unwrap_or(Duration::ZERO),
            self.slow_request_percentage(),
            self.slow_requests,
            self.total_requests,
            self.consecutive_slow_requests,
            self.consecutive_fast_requests
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_adaptive_rate_limiter_basic() {
        let limiter = AdaptiveRateLimiter::new();
        
        // Initial delay should be base delay
        assert_eq!(limiter.current_delay(), Duration::from_millis(300));
        
        // Wait for request
        let start = Instant::now();
        limiter.wait_for_request().await;
        let elapsed = start.elapsed();
        
        // Should wait approximately the base delay
        assert!(elapsed >= Duration::from_millis(290));
        assert!(elapsed <= Duration::from_millis(400));
    }

    #[tokio::test]
    async fn test_slow_request_backoff() {
        let limiter = AdaptiveRateLimiter::new();
        
        // Record several slow requests
        for _ in 0..3 {
            limiter.record_response_time(Duration::from_secs(3));
        }
        
        // Delay should have increased
        assert!(limiter.current_delay() > Duration::from_millis(300));
    }

    #[tokio::test]
    async fn test_fast_request_recovery() {
        let limiter = AdaptiveRateLimiter::new();
        
        // First increase delay with slow requests
        for _ in 0..3 {
            limiter.record_response_time(Duration::from_secs(3));
        }
        
        let increased_delay = limiter.current_delay();
        
        // Then record fast requests
        for _ in 0..4 {
            limiter.record_response_time(Duration::from_millis(500));
        }
        
        // Delay should have decreased
        assert!(limiter.current_delay() < increased_delay);
    }

    #[tokio::test]
    async fn test_stats_calculation() {
        let limiter = AdaptiveRateLimiter::new();
        
        // Record mix of fast and slow requests
        limiter.record_response_time(Duration::from_millis(500));
        limiter.record_response_time(Duration::from_secs(3));
        limiter.record_response_time(Duration::from_millis(800));
        
        let stats = limiter.get_stats();
        assert_eq!(stats.total_requests, 3);
        assert_eq!(stats.slow_requests, 1);
        assert_eq!(stats.slow_request_percentage(), 33.333333333333336);
    }
}
