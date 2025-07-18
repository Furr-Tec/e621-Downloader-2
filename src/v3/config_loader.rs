﻿use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task;

// Define error types for config loading
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("TOML parsing error: {0}")]
    Toml(#[from] toml::de::Error),
    
    #[error("Config file not found: {0}")]
    NotFound(String),
    
    #[error("Watch error: {0}")]
    Watch(#[from] notify::Error),
    
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
}

// Result type alias for config operations
pub type ConfigResult<T> = Result<T, ConfigError>;

// Config structs for config.toml
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Paths {
    pub download_directory: String,
    pub database_file: String,
    pub log_directory: String,
    pub temp_directory: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Limits {
    pub posts_per_page: usize,
    pub max_page_number: usize,
    pub file_size_cap: usize,
    pub total_size_cap: usize,
    pub verify_sample_pct: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Pools {
    pub max_download_concurrency: usize,
    pub max_hash_concurrency: usize,
    pub max_api_concurrency: usize,
    pub batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Rate {
    pub requests_per_second: usize,
    pub burst_capacity: usize,
    pub retry_backoff_secs: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Logging {
    pub log_level: String,
    pub log_format: String,
    pub log_to_terminal: bool,
    pub log_to_file: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Verifier {
    pub enable_on_shutdown: bool,
    pub enable_on_startup: bool,
    pub check_orphans: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub paths: Paths,
    pub limits: Limits,
    pub pools: Pools,
    pub rate: Rate,
    pub logging: Logging,
    pub verifier: Verifier,
}

// Default implementation for AppConfig
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            paths: Paths {
                download_directory: "./downloads".to_string(),
                database_file: "./data/posts.sqlite".to_string(),
                log_directory: "./logs".to_string(),
                temp_directory: "./.tmp".to_string(),
            },
            limits: Limits {
                posts_per_page: 320,
                max_page_number: 750,
                file_size_cap: 20_971_520,
                total_size_cap: 104_857_600,
                verify_sample_pct: 10,
            },
            pools: Pools {
                max_download_concurrency: 8,
                max_hash_concurrency: 4,
                max_api_concurrency: 4,
                batch_size: 4,
            },
            rate: Rate {
                requests_per_second: 3,
                burst_capacity: 3,
                retry_backoff_secs: 10,
            },
            logging: Logging {
                log_level: "info".to_string(),
                log_format: "json".to_string(),
                log_to_terminal: true,
                log_to_file: true,
            },
            verifier: Verifier {
                enable_on_shutdown: true,
                enable_on_startup: true,
                check_orphans: true,
            },
        }
    }
}

// Config structs for e621.toml
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Auth {
    pub username: String,
    pub api_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Options {
    pub download_favorites: bool,
    pub safe_mode: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub tags: Vec<String>,
    pub artists: Vec<String>,
    pub pools: Vec<usize>,
    pub collections: Vec<usize>,
    pub posts: Vec<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct E621Config {
    pub auth: Auth,
    pub options: Options,
    pub query: Query,
}

// Default implementation for E621Config
impl Default for E621Config {
    fn default() -> Self {
        Self {
            auth: Auth {
                username: "your_username".to_string(),
                api_key: "your_api_key_here".to_string(),
            },
            options: Options {
                download_favorites: true,
                safe_mode: false,
            },
            query: Query {
                tags: vec!["wolf".to_string(), "muscle".to_string(), "solo".to_string(), "rating:s".to_string()],
                artists: vec!["nekoyasha".to_string(), "dogbreath".to_string()],
                pools: vec![12345, 99887],
                collections: vec![222, 444],
                posts: vec![1883912, 1723001],
            },
        }
    }
}

// Config structs for rules.toml
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Whitelist {
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Blacklist {
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RulesConfig {
    pub whitelist: Whitelist,
    pub blacklist: Blacklist,
}

// Default implementation for RulesConfig
impl Default for RulesConfig {
    fn default() -> Self {
        Self {
            whitelist: Whitelist {
                tags: vec!["dragon".to_string(), "muscle".to_string(), "male_focus".to_string()],
            },
            blacklist: Blacklist {
                tags: vec![
                    "scat".to_string(),
                    "watersports".to_string(),
                    "gore".to_string(),
                    "loli".to_string(),
                    "shota".to_string(),
                    "cub".to_string(),
                    "vore".to_string(),
                ],
            },
        }
    }
}

// Config manager to handle all configuration files
pub struct ConfigManager {
    app_config: Arc<RwLock<AppConfig>>,
    e621_config: Arc<RwLock<E621Config>>,
    rules_config: Arc<RwLock<RulesConfig>>,
    config_dir: PathBuf,
    _watcher: Option<RecommendedWatcher>,
    reload_tx: broadcast::Sender<ConfigReloadEvent>,
}

// Event type for config reloads
#[derive(Debug, Clone)]
pub enum ConfigReloadEvent {
    AppConfig,
    E621Config,
    RulesConfig,
}

impl ConfigManager {
    // Create a new ConfigManager instance
    pub async fn new(config_dir: impl AsRef<Path>) -> ConfigResult<Self> {
        let config_dir = config_dir.as_ref().to_path_buf();
        
        // Check if the directory exists
        if !config_dir.exists() {
            return Err(ConfigError::NotFound(format!(
                "Config directory not found: {}",
                config_dir.display()
            )));
        }
        
        // Load initial configurations
        let app_config = Self::load_app_config(&config_dir)?;
        let e621_config = Self::load_e621_config(&config_dir)?;
        let rules_config = Self::load_rules_config(&config_dir)?;
        
        // Create broadcast channel for reload events
        let (reload_tx, _) = broadcast::channel(100);
        
        let mut manager = Self {
            app_config: Arc::new(RwLock::new(app_config)),
            e621_config: Arc::new(RwLock::new(e621_config)),
            rules_config: Arc::new(RwLock::new(rules_config)),
            config_dir,
            _watcher: None,
            reload_tx,
        };
        
        // Setup file watcher
        manager.setup_watcher()?;
        
        Ok(manager)
    }
    
    // Load app config from config.toml
    fn load_app_config(config_dir: &Path) -> ConfigResult<AppConfig> {
        let config_path = config_dir.join("config.toml");
        
        if !config_path.exists() {
            log::warn!("Config file not found: {}", config_path.display());
            return Ok(AppConfig::default());
        }
        
        let content = fs::read_to_string(&config_path)?;
        match toml::from_str(&content) {
            Ok(config) => Ok(config),
            Err(e) => {
                log::error!("Failed to parse config.toml: {}", e);
                log::info!("Using default configuration");
                Ok(AppConfig::default())
            }
        }
    }
    
    // Load e621 config from e621.toml
    fn load_e621_config(config_dir: &Path) -> ConfigResult<E621Config> {
        let config_path = config_dir.join("e621.toml");
        
        if !config_path.exists() {
            log::warn!("E621 config file not found: {}", config_path.display());
            return Ok(E621Config::default());
        }
        
        let content = fs::read_to_string(&config_path)?;
        match toml::from_str(&content) {
            Ok(config) => Ok(config),
            Err(e) => {
                log::error!("Failed to parse e621.toml: {}", e);
                log::info!("Using default e621 configuration");
                Ok(E621Config::default())
            }
        }
    }
    
    // Load rules config from rules.toml
    fn load_rules_config(config_dir: &Path) -> ConfigResult<RulesConfig> {
        let config_path = config_dir.join("rules.toml");
        
        if !config_path.exists() {
            log::warn!("Rules config file not found: {}", config_path.display());
            return Ok(RulesConfig::default());
        }
        
        let content = fs::read_to_string(&config_path)?;
        match toml::from_str(&content) {
            Ok(config) => Ok(config),
            Err(e) => {
                log::error!("Failed to parse rules.toml: {}", e);
                log::info!("Using default rules configuration");
                Ok(RulesConfig::default())
            }
        }
    }
    
    // Setup file watcher for live reloading
    fn setup_watcher(&mut self) -> ConfigResult<()> {
        let config_dir = self.config_dir.clone();
        let reload_tx = self.reload_tx.clone();
        
        // Create a channel to receive events
        let (tx, rx) = std::sync::mpsc::channel();
        
        // Create a watcher
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;
        
        // Start watching the config directory
        watcher.watch(&config_dir, RecursiveMode::NonRecursive)?;
        
        // Clone Arc references for the async task
        let app_config = self.app_config.clone();
        let e621_config = self.e621_config.clone();
        let rules_config = self.rules_config.clone();
        
        // Spawn a task to handle file change events
        task::spawn(async move {
            for res in rx {
                match res {
                    Ok(Event { kind: EventKind::Modify(_), paths, .. }) => {
                        for path in paths {
                            if let Some(file_name) = path.file_name() {
                                if let Some(file_name_str) = file_name.to_str() {
                                    match file_name_str {
                                        "config.toml" => {
                                            match Self::load_app_config(&config_dir) {
                                                Ok(new_config) => {
                                                    if let Ok(mut config) = app_config.write() {
                                                        *config = new_config;
                                                        let _ = reload_tx.send(ConfigReloadEvent::AppConfig);
                                                        log::info!("Reloaded app config");
                                                    }
                                                }
                                                Err(e) => log::error!("Failed to reload app config: {}", e),
                                            }
                                        }
                                        "e621.toml" => {
                                            match Self::load_e621_config(&config_dir) {
                                                Ok(new_config) => {
                                                    if let Ok(mut config) = e621_config.write() {
                                                        *config = new_config;
                                                        let _ = reload_tx.send(ConfigReloadEvent::E621Config);
                                                        log::info!("Reloaded e621 config");
                                                    }
                                                }
                                                Err(e) => log::error!("Failed to reload e621 config: {}", e),
                                            }
                                        }
                                        "rules.toml" => {
                                            match Self::load_rules_config(&config_dir) {
                                                Ok(new_config) => {
                                                    if let Ok(mut config) = rules_config.write() {
                                                        *config = new_config;
                                                        let _ = reload_tx.send(ConfigReloadEvent::RulesConfig);
                                                        log::info!("Reloaded rules config");
                                                    }
                                                }
                                                Err(e) => log::error!("Failed to reload rules config: {}", e),
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => log::error!("Watch error: {}", e),
                    _ => {}
                }
            }
        });
        
        self._watcher = Some(watcher);
        Ok(())
    }
    
    // Get a subscription to config reload events
    pub fn subscribe(&self) -> broadcast::Receiver<ConfigReloadEvent> {
        self.reload_tx.subscribe()
    }
    
    // Get app config
    pub fn get_app_config(&self) -> ConfigResult<AppConfig> {
        self.app_config
            .read()
            .map_err(|e| ConfigError::LockError(e.to_string()))
            .map(|config| config.clone())
    }
    
    // Get e621 config
    pub fn get_e621_config(&self) -> ConfigResult<E621Config> {
        self.e621_config
            .read()
            .map_err(|e| ConfigError::LockError(e.to_string()))
            .map(|config| config.clone())
    }
    
    // Get rules config
    pub fn get_rules_config(&self) -> ConfigResult<RulesConfig> {
        self.rules_config
            .read()
            .map_err(|e| ConfigError::LockError(e.to_string()))
            .map(|config| config.clone())
    }
    
    // Check if all required config files exist
    pub fn check_config_files(&self) -> bool {
        let config_path = self.config_dir.join("config.toml");
        let e621_path = self.config_dir.join("e621.toml");
        let rules_path = self.config_dir.join("rules.toml");
        
        config_path.exists() && e621_path.exists() && rules_path.exists()
    }
}

// Helper function to create a ConfigManager instance
pub async fn init_config(config_dir: impl AsRef<Path>) -> ConfigResult<ConfigManager> {
    ConfigManager::new(config_dir).await
}