pub mod file_metadata;

use std::fs::{read_to_string, write};
use std::io;
use std::path::Path;
use std::process::exit;

use anyhow::{Context, Error};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string_pretty};

pub(crate) mod artist;
pub(crate) mod parser;
pub(crate) mod tag;
pub(crate) mod directory;

use directory::DirectoryManager;

/// Name of the configuration file.
pub(crate) const CONFIG_NAME: &str = "config.json";

/// Name of the login file.
pub(crate) const LOGIN_NAME: &str = "login.json";

/// Config that is used to do general setup.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Config {
    /// The location of the download directory.
    #[serde(rename = "downloadDirectory")]
    download_directory: String,
    /// The file naming convention (e.g "md5", "id").
    #[serde(rename = "fileNamingConvention")]
    naming_convention: String,
    /// Batch size for processing collections (default: 4)
    #[serde(rename = "batchSize", default = "default_batch_size")]
    batch_size: usize,
    /// Concurrency level for downloads (default: 3)
    #[serde(rename = "downloadConcurrency", default = "default_download_concurrency")]
    download_concurrency: usize,
    /// Whether to use simplified folder structure (only Tags, no Artists/Pools)
    #[serde(rename = "simplifiedFolders", default = "default_simplified_folders")]
    simplified_folders: bool,
    /// Maximum number of pages to search per tag (default: 10)
    #[serde(rename = "maxPagesToSearch", default = "default_max_pages_to_search")]
    max_pages_to_search: usize,
    /// Enable whitelist override for blacklisted content (default: true)
    #[serde(rename = "whitelistOverrideBlacklist", default = "default_whitelist_override")]
    whitelist_override_blacklist: bool,
    #[serde(skip)]
    directory_manager: Option<DirectoryManager>,
}

fn default_batch_size() -> usize { 4 }
fn default_download_concurrency() -> usize { 3 }
fn default_simplified_folders() -> bool { true }
fn default_max_pages_to_search() -> usize { 10 }
fn default_whitelist_override() -> bool { true }

use std::sync::RwLock;

static CONFIG: OnceCell<RwLock<Config>> = OnceCell::new();

impl Config {
    /// The location of the download directory.
    pub(crate) fn download_directory(&self) -> &str {
        &self.download_directory
    }

    /// The file naming convention (e.g "md5", "id").
    pub(crate) fn naming_convention(&self) -> &str {
        &self.naming_convention
    }

    /// Batch size for processing collections
    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Concurrency level for downloads
    pub(crate) fn download_concurrency(&self) -> usize {
        self.download_concurrency
    }

    /// Whether to use simplified folder structure
    pub(crate) fn simplified_folders(&self) -> bool {
        self.simplified_folders
    }

    /// Maximum number of pages to search per tag
    pub(crate) fn max_pages_to_search(&self) -> usize {
        self.max_pages_to_search
    }
    
    /// Whether whitelist should override blacklist
    pub(crate) fn whitelist_override_blacklist(&self) -> bool {
        self.whitelist_override_blacklist
    }

    /// Get the directory manager instance
    pub(crate) fn directory_manager(&self) -> Result<&DirectoryManager, Error> {
        match &self.directory_manager {
            Some(manager) => Ok(manager),
            None => {
                error!("Directory manager not initialized!");
                // This will terminate the program, so no code after it will be reached
                emergency_exit("Configuration error: Directory manager not initialized");
            }
        }
    }

    /// Checks config and ensure it isn't missing.
    pub(crate) fn config_exists() -> bool {
        if !Path::new(CONFIG_NAME).exists() {
            trace!("config.json: does not exist!");
            return false;
        }

        true
    }

    /// Creates config file.
    pub(crate) fn create_config() -> Result<(), Error> {
        let json = to_string_pretty(&Config::default())?;
        write(Path::new(CONFIG_NAME), json)?;

        Ok(())
    }

    /// Get the global instance of the `Config`.
    pub(crate) fn get() -> Config {
        let config_lock = CONFIG.get_or_init(|| {
            match Self::get_config() {
                Ok(config) => RwLock::new(config),
                Err(e) => {
                    error!("Failed to load config: {}", e);
                    emergency_exit("Configuration loading failed");
                }
            }
        });
        match config_lock.read() {
            Ok(guard) => guard.clone(),
            Err(e) => {
                warn!("Failed to acquire read lock on config: {}. Recovering lock.", e);
                let guard = e.into_inner();
                guard.clone()
            }
        }
    }

    /// Updates the maximum pages to search and saves to config file
    pub(crate) fn update_max_pages_to_search(new_max_pages: usize) -> Result<(), Error> {
        // Load current config from file
        let mut config: Config = from_str(&read_to_string(CONFIG_NAME)?)?;
        
        // Update the field
        config.max_pages_to_search = new_max_pages;
        
        // Initialize directory manager if needed
        if config.directory_manager.is_none() {
            config.directory_manager = Some(DirectoryManager::new(&config.download_directory)?);
        }
        
        // Save back to file
        let json = to_string_pretty(&config)?;
        write(Path::new(CONFIG_NAME), json)?;
        
        // Update the cached CONFIG in memory
        let config_lock = CONFIG.get_or_init(|| {
            match Self::get_config() {
                Ok(config) => RwLock::new(config),
                Err(e) => {
                    error!("Failed to load config: {}", e);
                    emergency_exit("Configuration loading failed");
                }
            }
        });
        if let Ok(mut cached_config) = config_lock.write() {
            *cached_config = config;
        }
        
        info!("Max pages to search updated to: {}", new_max_pages);
        Ok(())
    }

    /// Updates the batch size and saves to config file
    pub(crate) fn update_batch_size(new_batch_size: usize) -> Result<(), Error> {
        // Load current config from file
        let mut config: Config = from_str(&read_to_string(CONFIG_NAME)?)?;
        
        // Update the field
        config.batch_size = new_batch_size;
        
        // Save back to file
        let json = to_string_pretty(&config)?;
        write(Path::new(CONFIG_NAME), json)?;
        
        info!("Batch size updated to: {}", new_batch_size);
        Ok(())
    }

    /// Updates the download concurrency and saves to config file
    pub(crate) fn update_download_concurrency(new_concurrency: usize) -> Result<(), Error> {
        // Load current config from file
        let mut config: Config = from_str(&read_to_string(CONFIG_NAME)?)?;
        
        // Update the field
        config.download_concurrency = new_concurrency;
        
        // Save back to file
        let json = to_string_pretty(&config)?;
        write(Path::new(CONFIG_NAME), json)?;
        
        info!("Download concurrency updated to: {}", new_concurrency);
        Ok(())
    }

    /// Loads and returns `config` for quick management and settings.
    fn get_config() -> Result<Self, Error> {
        let config_contents = read_to_string(CONFIG_NAME)
            .with_context(|| format!("Failed to read config file: {}", CONFIG_NAME))?;
        let mut config: Config = from_str(&config_contents)?;
        config.naming_convention = config.naming_convention.to_lowercase();
        let convention = ["md5", "id"];
        if !convention
            .iter()
            .any(|e| *e == config.naming_convention.as_str())
        {
            error!(
                "There is no naming convention {}",
                config.naming_convention
            );
            info!("The naming convention can only be [\"md5\", \"id\"]");
            emergency_exit("Naming convention is incorrect!");
        }

        // Initialize the directory manager
        config.directory_manager = Some(DirectoryManager::new(&config.download_directory)?);

        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            download_directory: String::from("downloads/"),
            naming_convention: String::from("id"),
            batch_size: default_batch_size(),
            download_concurrency: default_download_concurrency(),
            simplified_folders: default_simplified_folders(),
            max_pages_to_search: default_max_pages_to_search(),
            whitelist_override_blacklist: default_whitelist_override(),
            directory_manager: None,
        }
    }
}

/// `Login` contains all login information for obtaining information about a certain user.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Login {
    /// Username of user.
    #[serde(rename = "Username")]
    username: String,
    /// The password hash (also known as the API key) for the user.
    #[serde(rename = "APIKey")]
    api_key: String,
    /// Whether or not the user wishes to download their favorites.
    #[serde(rename = "DownloadFavorites")]
    download_favorites: bool,
}

static LOGIN: OnceCell<Login> = OnceCell::new();

impl Login {
    /// Username of user.
    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    /// The password hash (also known as the API key) for the user.
    pub(crate) fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Whether or not the user wishes to download their favorites.
    pub(crate) fn download_favorites(&self) -> bool {
        self.download_favorites
    }

    /// Gets the global instance of [Login].
    pub(crate) fn get() -> &'static Self {
        LOGIN.get_or_init(|| Self::load().unwrap_or_else(|e| {
            error!("Unable to load `login.json`. Error: {}", e);
            warn!("The program will use default values, but it is highly recommended to check your login.json file to ensure that everything is correct.");
            Login::default()
        }))
    }

    /// Loads the login file or creates one if it doesn't exist.
    fn load() -> Result<Self, Error> {
        let mut login = Login::default();
        let login_path = Path::new(LOGIN_NAME);
        if login_path.exists() {
            login = from_str(&read_to_string(login_path)?)?;
        } else {
            login.create_login()?;
        }

        Ok(login)
    }

    /// Checks if the login user and password is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.username.is_empty() || self.api_key.is_empty()
    }

    /// Creates a new login file.
    fn create_login(&self) -> Result<(), Error> {
        write(LOGIN_NAME, to_string_pretty(self)?)?;

        info!("The login file was created.");
        info!(
            "If you wish to use your Blacklist, be sure to give your username and API hash key."
        );
        info!(
            "Do not give out your API hash unless you trust this software completely, always treat your API hash like your own password."
        );

        Ok(())
    }
}

impl Default for Login {
    /// The default state for the login if none exists.
    fn default() -> Self {
        Login {
            username: String::new(),
            api_key: String::new(),
            download_favorites: true,
        }
    }
}

/// Exits the program after message explaining the error and prompting the user to press `ENTER`.
///
/// # Arguments
///
/// * `error`: The error message to print.
pub(crate) fn emergency_exit(error: &str) -> ! {
    error!("{}", error);
    println!("Press ENTER to close the application...");

    let mut line = String::new();
    io::stdin().read_line(&mut line).unwrap_or_default();

    exit(0x00FF);
}
