use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

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
    
    #[error("TOML serialization error: {0}")]
    TomlSer(String),
    
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
pub struct Organization {
    pub directory_strategy: String,  // "by_tag", "by_artist", or "flat"
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub paths: Paths,
    pub limits: Limits,
    pub pools: Pools,
    pub rate: Rate,
    pub logging: Logging,
    pub verifier: Verifier,
    pub organization: Organization,
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
                // 0 means unlimited planning-size; jobs are capped only by explicit settings
                total_size_cap: 0,
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
            organization: Organization {
                directory_strategy: "mixed".to_string(),  // Default to mixed organization (Artists and Tags)
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

// Note: Local rules configuration removed - using E621 API blacklist directly

// Config manager to handle all configuration files
pub struct ConfigManager {
    app_config: Arc<RwLock<AppConfig>>,
    e621_config: Arc<RwLock<E621Config>>,
    config_dir: PathBuf,
    _watcher: Option<RecommendedWatcher>,
    reload_tx: broadcast::Sender<ConfigReloadEvent>,
}

// Event type for config reloads
#[derive(Debug, Clone)]
pub enum ConfigReloadEvent {
    AppConfig,
    E621Config,
}

impl ConfigManager {
    // Create a new ConfigManager instance
    pub async fn new(config_dir: impl AsRef<Path>) -> ConfigResult<Self> {
        let config_dir = config_dir.as_ref().to_path_buf();
        
        // Create the directory if it doesn't exist
        if !config_dir.exists() {
            log::info!("Creating config directory: {}", config_dir.display());
            fs::create_dir_all(&config_dir)?;
        }
        
        // Load initial configurations
        let app_config = Self::load_app_config(&config_dir)?;
        let e621_config = Self::load_e621_config(&config_dir)?;
        
        // Create broadcast channel for reload events
        let (reload_tx, _) = broadcast::channel(100);
        
        let mut manager = Self {
            app_config: Arc::new(RwLock::new(app_config)),
            e621_config: Arc::new(RwLock::new(e621_config)),
            config_dir,
            _watcher: None,
            reload_tx,
        };
        
        // Create default config files if they don't exist
        manager.create_default_configs()?;
        
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
                log::info!("Config file appears to be outdated or corrupted");
                log::info!("Backing up old config and creating new one with default values");
                
                // Backup the old config file
                if let Err(backup_err) = fs::rename(&config_path, config_path.with_extension("toml.backup")) {
                    log::warn!("Failed to backup old config: {}", backup_err);
                }
                
                // Create new default config
                let default_config = AppConfig::default();
                if let Ok(toml_string) = toml::to_string_pretty(&default_config) {
                    if let Err(write_err) = fs::write(&config_path, toml_string) {
                        log::error!("Failed to write new config file: {}", write_err);
                    } else {
                        log::info!("Created new config.toml with default values");
                    }
                }
                
                Ok(default_config)
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
    
    // Note: load_rules_config() removed - using E621 API blacklist directly
    
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
                                            // Rules config is deprecated - using E621 API blacklist directly
                                            log::info!("rules.toml is deprecated. Using E621 API blacklist instead.");
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
    
    // Note: get_rules_config() removed - using E621 API blacklist directly
    
    // Check if all required config files exist
    pub fn check_config_files(&self) -> bool {
        let config_path = self.config_dir.join("config.toml");
        let e621_path = self.config_dir.join("e621.toml");
        
        config_path.exists() && e621_path.exists()
    }
    
    // Save app config to file
    pub fn save_app_config(&self, config: &AppConfig) -> ConfigResult<()> {
        let config_path = self.config_dir.join("config.toml");
        let toml_string = toml::to_string_pretty(config)
            .map_err(|e| ConfigError::TomlSer(e.to_string()))?;
        fs::write(&config_path, toml_string)?;
        
        // Update the in-memory config
        let mut app_config = self.app_config
            .write()
            .map_err(|e| ConfigError::LockError(e.to_string()))?;
        *app_config = config.clone();
        
        Ok(())
    }
    
    // Save e621 config to file
    pub fn save_e621_config(&self, config: &E621Config) -> ConfigResult<()> {
        let config_path = self.config_dir.join("e621.toml");
        let toml_string = toml::to_string_pretty(config)
            .map_err(|e| ConfigError::TomlSer(e.to_string()))?;
        fs::write(&config_path, toml_string)?;
        
        // Update the in-memory config
        let mut e621_config = self.e621_config
            .write()
            .map_err(|e| ConfigError::LockError(e.to_string()))?;
        *e621_config = config.clone();
        
        Ok(())
    }
    
    // Check if e621 credentials are configured and valid
    pub fn has_valid_e621_credentials(&self) -> bool {
        match self.get_e621_config() {
            Ok(config) => {
                !config.auth.username.is_empty() &&
                !config.auth.api_key.is_empty() &&
                config.auth.username != "your_username" &&
                config.auth.api_key != "your_api_key_here"
            }
            Err(_) => false,
        }
    }
    
    // Create all default config files if they don't exist
    pub fn create_default_configs(&self) -> ConfigResult<()> {
        // Create config.toml
        let config_path = self.config_dir.join("config.toml");
        if !config_path.exists() {
            let default_config = AppConfig::default();
            let toml_string = toml::to_string_pretty(&default_config)
                .map_err(|e| ConfigError::TomlSer(e.to_string()))?;
            fs::write(&config_path, toml_string)?;
            
            // Update the in-memory config
            let mut app_config = self.app_config
                .write()
                .map_err(|e| ConfigError::LockError(e.to_string()))?;
            *app_config = default_config;
        }
        
        // Create e621.toml
        let e621_path = self.config_dir.join("e621.toml");
        if !e621_path.exists() {
            let default_config = E621Config::default();
            let toml_string = toml::to_string_pretty(&default_config)
                .map_err(|e| ConfigError::TomlSer(e.to_string()))?;
            fs::write(&e621_path, toml_string)?;
            
            // Update the in-memory config
            let mut e621_config = self.e621_config
                .write()
                .map_err(|e| ConfigError::LockError(e.to_string()))?;
            *e621_config = default_config;
        }
        
        // Note: rules.toml is deprecated - using E621 API blacklist directly
        
        Ok(())
    }
    
    // Create e621.toml file if it doesn't exist (deprecated - use create_default_configs instead)
    pub fn create_default_e621_config(&self) -> ConfigResult<()> {
        let config_path = self.config_dir.join("e621.toml");
        
        // Only create if it doesn't exist
        if !config_path.exists() {
            let default_config = E621Config::default();
            let toml_string = toml::to_string_pretty(&default_config)
                .map_err(|e| ConfigError::TomlSer(e.to_string()))?;
            fs::write(&config_path, toml_string)?;
            
            // Update the in-memory config
            let mut e621_config = self.e621_config
                .write()
                .map_err(|e| ConfigError::LockError(e.to_string()))?;
            *e621_config = default_config;
        }
        
        Ok(())
    }
    
    // Note: save_rules_config() removed - using E621 API blacklist directly
}

// Helper function to create a ConfigManager instance
pub async fn init_config(config_dir: impl AsRef<Path>) -> ConfigResult<ConfigManager> {
    ConfigManager::new(config_dir).await
}