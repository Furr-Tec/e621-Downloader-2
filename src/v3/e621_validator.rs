//! E621 API credential validation
//! Provides functions to validate E621 API credentials

use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

use crate::v3::config_loader::E621Config;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("HTTP request error: {0}")]
    Request(#[from] reqwest::Error),
    
    #[error("Invalid credentials: {0}")]
    InvalidCredentials(String),
    
    #[error("API error: {0}")]
    Api(String),
    
    #[error("Network timeout")]
    Timeout,
}

pub type ValidationResult<T> = Result<T, ValidationError>;

/// Validate E621 API credentials by making a test request
pub async fn validate_e621_credentials(username: &str, api_key: &str) -> ValidationResult<String> {
    // Create HTTP client
    let client = Client::builder()
        .user_agent(format!("e621_downloader/{} (by {})", env!("CARGO_PKG_VERSION"), username))
        .timeout(Duration::from_secs(15))
        .build()?;

    // Test with a simple API endpoint that requires authentication
    // We'll use /posts.json with a limit of 1 to minimize data transfer
    let url = "https://e621.net/posts.json?limit=1";

    // Create request with basic auth
    use base64::{Engine as _, engine::general_purpose};
    let auth_header = format!("Basic {}", general_purpose::STANDARD.encode(format!("{}:{}", username, api_key)));

    let response = client
        .get(url)
        .header("Authorization", auth_header)
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            // Try to parse the response to ensure it's valid
            let text = response.text().await?;
            let _json: Value = serde_json::from_str(&text)
                .map_err(|e| ValidationError::Api(format!("Invalid JSON response: {}", e)))?;
            
            Ok("Credentials are valid and working!".to_string())
        }
        StatusCode::UNAUTHORIZED => {
            Err(ValidationError::InvalidCredentials(
                "Invalid username or API key. Please check your credentials.".to_string()
            ))
        }
        StatusCode::FORBIDDEN => {
            Err(ValidationError::InvalidCredentials(
                "Access forbidden. Your account may be banned or restricted.".to_string()
            ))
        }
        StatusCode::TOO_MANY_REQUESTS => {
            Err(ValidationError::Api(
                "Rate limit exceeded. Please wait a moment and try again.".to_string()
            ))
        }
        status => {
            Err(ValidationError::Api(format!(
                "API returned status {}: {}",
                status.as_u16(),
                response.text().await.unwrap_or_else(|_| "Unknown error".to_string())
            )))
        }
    }
}

/// Test E621 credentials from config
pub async fn test_config_credentials(config: &E621Config) -> ValidationResult<String> {
    validate_e621_credentials(&config.auth.username, &config.auth.api_key).await
}
