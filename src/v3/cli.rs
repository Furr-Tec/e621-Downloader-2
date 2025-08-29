//! Interactive CLI menu system for E621 Downloader
//! Uses dialoguer for user interaction

use std::path::Path;
use std::io::{self, IsTerminal};
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

use anyhow::Result;

use dialoguer::{theme::ColorfulTheme, Select, Input, Confirm, MultiSelect};
use console::style;

use crate::v3::{
    ConfigManager, ConfigResult, ConfigError,
    init_config, QueryPlanner, init_query_planner, Query, init_orchestrator,
};

// Custom error type for CLI operations
#[derive(Debug)]
pub enum CliError {
    Io(io::Error),
    Config(ConfigError),
    Dialoguer(dialoguer::Error),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Io(e) => write!(f, "IO error: {}", e),
            CliError::Config(e) => write!(f, "Config error: {}", e),
            CliError::Dialoguer(e) => write!(f, "UI interaction error: {}", e),
        }
    }
}

impl StdError for CliError {}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> Self {
        CliError::Io(err)
    }
}

impl From<ConfigError> for CliError {
    fn from(err: ConfigError) -> Self {
        CliError::Config(err)
    }
}

impl From<dialoguer::Error> for CliError {
    fn from(err: dialoguer::Error) -> Self {
        CliError::Dialoguer(err)
    }
}

// Result type alias for CLI operations
pub type CliResult<T> = Result<T, CliError>;

/// Main menu options
#[derive(Debug, Clone, Copy)]
pub enum MainMenuOption {
    SearchTags,
    SearchArtists,
    SearchPools,
    SearchCollections,
    SearchPosts,
    EditQuery,
    StartDownload,
    EditConfig,
    ValidateConfigs,
    VerifyArchive,
    Exit,
}

impl MainMenuOption {
    /// Get all menu options
    pub fn variants() -> &'static [MainMenuOption] {
        &[
            MainMenuOption::SearchTags,
            MainMenuOption::SearchArtists,
            MainMenuOption::SearchPools,
            MainMenuOption::SearchCollections,
            MainMenuOption::SearchPosts,
            MainMenuOption::EditQuery,
            MainMenuOption::StartDownload,
            MainMenuOption::EditConfig,
            MainMenuOption::ValidateConfigs,
            MainMenuOption::VerifyArchive,
            MainMenuOption::Exit,
        ]
    }
    
    /// Get the display name for the menu option
    pub fn display_name(&self) -> &'static str {
        match self {
            MainMenuOption::SearchTags => "Search by tags",
            MainMenuOption::SearchArtists => "Search by artists",
            MainMenuOption::SearchPools => "Search by pools",
            MainMenuOption::SearchCollections => "Search by collections",
            MainMenuOption::SearchPosts => "Search by posts",
            MainMenuOption::EditQuery => "Edit queries",
            MainMenuOption::StartDownload => "Start download",
            MainMenuOption::EditConfig => "Edit config",
            MainMenuOption::ValidateConfigs => "Validate configs",
            MainMenuOption::VerifyArchive => "Verify archive",
            MainMenuOption::Exit => "Exit",
        }
    }
}

/// Config edit menu options
#[derive(Debug, Clone, Copy)]
pub enum ConfigEditOption {
    EditAppConfig,
    EditE621Config,
    EditRulesConfig,
    Back,
}

impl ConfigEditOption {
    /// Get all config edit options
    pub fn variants() -> &'static [ConfigEditOption] {
        &[
            ConfigEditOption::EditAppConfig,
            ConfigEditOption::EditE621Config,
            ConfigEditOption::EditRulesConfig,
            ConfigEditOption::Back,
        ]
    }
    
    /// Get the display name for the config edit option
    pub fn display_name(&self) -> &'static str {
        match self {
            ConfigEditOption::EditAppConfig => "Edit application config",
            ConfigEditOption::EditE621Config => "Edit E621 config",
            ConfigEditOption::EditRulesConfig => "Edit rules config",
            ConfigEditOption::Back => "Back to main menu",
        }
    }
}

/// CLI manager for interactive menus
pub struct CliManager {
    config_manager: ConfigManager,
    theme: ColorfulTheme,
    config_dir: std::path::PathBuf,
}

impl CliManager {
    /// Create a new CLI manager
    pub async fn new(config_dir: impl AsRef<Path>) -> ConfigResult<Self> {
        let config_dir_path = config_dir.as_ref().to_path_buf();
        let config_manager = init_config(&config_dir_path).await?;
        let theme = ColorfulTheme::default();
        
        let mut cli_manager = Self {
            config_manager,
            theme,
            config_dir: config_dir_path,
        };
        
        // Check if we're in an interactive terminal
        if !Self::is_interactive_terminal() {
            println!("Warning: Not running in an interactive terminal.");
            println!("Please run this program from a proper terminal/command prompt.");
            println!("If you need to configure credentials, edit the e621.toml file manually.");
            
            // Try to create default configs but don't prompt for credentials
            if let Err(e) = cli_manager.config_manager.create_default_configs() {
                println!("Failed to create default config files: {}", e);
                return Err(e);
            }
            
            return Ok(cli_manager);
        }
        
        // Check and setup E621 credentials on first run
        cli_manager.ensure_e621_credentials().await?;
        
        Ok(cli_manager)
    }
    
    /// Check if we're running in an interactive terminal
    fn is_interactive_terminal() -> bool {
        std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
    }
    
    /// Run the main menu loop
    pub async fn run(&self) -> CliResult<()> {
        loop {
            // Display the main menu
            let selection = self.show_main_menu()?;
            
            match selection {
                MainMenuOption::SearchTags => self.search_tags().await?,
                MainMenuOption::SearchArtists => self.search_artists().await?,
                MainMenuOption::SearchPools => self.search_pools().await?,
                MainMenuOption::SearchCollections => self.search_collections().await?,
                MainMenuOption::SearchPosts => self.search_posts().await?,
                MainMenuOption::EditQuery => self.edit_query().await?,
                MainMenuOption::StartDownload => self.start_download().await?,
                MainMenuOption::EditConfig => self.edit_config().await?,
                MainMenuOption::ValidateConfigs => self.validate_configs().await?,
                MainMenuOption::VerifyArchive => self.verify_archive().await?,
                MainMenuOption::Exit => {
                    println!("{}", style("Exiting...").cyan());
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Show the main menu and get user selection
    fn show_main_menu(&self) -> CliResult<MainMenuOption> {
        let options = MainMenuOption::variants();
        let option_names: Vec<&str> = options.iter().map(|o| o.display_name()).collect();
        
        println!("\n{}", style("E621 Downloader - Main Menu").cyan().bold());
        
        let selection = Select::with_theme(&self.theme)
            .items(&option_names)
            .default(0)
            .interact()?;
        
        Ok(options[selection])
    }
    
    /// Add a new query
    async fn add_query(&self) -> CliResult<()> {
        println!("\n{}", style("Add New Query").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get tags from user
        let tags: String = Input::with_theme(&self.theme)
            .with_prompt("Enter tags (space separated)")
            .interact_text()?;
        
        // Split tags and add to config
        let new_tags: Vec<String> = tags
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .collect();
        
        if !new_tags.is_empty() {
            // Add new tags to existing tags
            for tag in new_tags {
                if !e621_config.query.tags.contains(&tag) {
                    e621_config.query.tags.push(tag);
                }
            }
            
            // Save the updated config back to file
            if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                println!("{}: {}", style("Error saving config").red().bold(), e);
            } else {
                println!("{}", style("Tags added successfully!").green());
                println!("Current tags: {:?}", e621_config.query.tags);
            }
        } else {
            println!("{}", style("No valid tags entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit an existing query
    async fn edit_query(&self) -> CliResult<()> {
        println!("\n{}", style("Edit Query").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        if e621_config.query.tags.is_empty() {
            println!("{}", style("No tags to edit. Please add tags first.").yellow());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        // Show current tags and let user select which to remove
        println!("{}", style("Current tags:").cyan());
        let selections = MultiSelect::with_theme(&self.theme)
            .items(&e621_config.query.tags)
            .with_prompt("Select tags to remove (Space to select, Enter to confirm)")
            .interact()?;
        
        if !selections.is_empty() {
            // Remove selected tags (in reverse order to avoid index issues)
            for index in selections.into_iter().rev() {
                e621_config.query.tags.remove(index);
            }
            
            // Save the updated config back to file
            if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                println!("{}: {}", style("Error saving config").red().bold(), e);
            } else {
                println!("{}", style("Tags removed successfully!").green());
                println!("Remaining tags: {:?}", e621_config.query.tags);
            }
        } else {
            println!("{}", style("No tags selected for removal.").yellow());
        }
        
        // Ask if user wants to add new tags
        if Confirm::with_theme(&self.theme)
            .with_prompt("Would you like to add new tags?")
            .default(false)
            .interact()?
        {
            // Get new tags from user
            let tags: String = Input::with_theme(&self.theme)
                .with_prompt("Enter new tags (space separated)")
                .interact_text()?;
            
            // Split tags and add to config
            let new_tags: Vec<String> = tags
                .split_whitespace()
                .map(|s| s.to_lowercase())
                .collect();
            
            if !new_tags.is_empty() {
                // Add new tags to existing tags
                for tag in new_tags {
                    if !e621_config.query.tags.contains(&tag) {
                        e621_config.query.tags.push(tag);
                    }
                }
                
                // Save the updated config back to file
                if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                    println!("{}: {}", style("Error saving config").red().bold(), e);
                } else {
                    println!("{}", style("New tags added successfully!").green());
                    println!("Current tags: {:?}", e621_config.query.tags);
                }
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Start the download process
    async fn start_download(&self) -> CliResult<()> {
        println!("\n{}", style("Start Download").cyan().bold());
        
        use crate::v3::{init_orchestrator, init_query_planner, init_download_engine, init_file_processor};
        use std::sync::Arc;
        
        // Initialize the orchestrator
        println!("{}", style("Initializing orchestrator...").cyan());
        // Create a new ConfigManager instance instead of cloning
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        match init_orchestrator(config_manager.clone()).await {
            Ok(orchestrator) => {
                println!("{}", style("✓ Orchestrator initialized").green());
                
                // Initialize the query planner
                println!("{}", style("Initializing query planner...").cyan());
                match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone()).await {
                    Ok(query_planner) => {
                        println!("{}", style("✓ Query planner initialized").green());
                        
                        // Parse input from configuration
                        println!("{}", style("Parsing queries from configuration...").cyan());
                        match query_planner.parse_input().await {
                            Ok(queries) => {
                                if queries.is_empty() {
                                    println!("{}", style("No queries found in configuration. Please add some tags first.").yellow());
                                    self.press_enter_to_continue()?;
                                    return Ok(());
                                }
                                
                                println!("{} Found {} queries", style("✓").green(), queries.len());
                                
                                // Initialize download engine
                                println!("{}", style("Initializing download engine...").cyan());
                                match init_download_engine(config_manager.clone()).await {
                                    Ok(download_engine) => {
                                        println!("{}", style("✓ Download engine initialized").green());
                                        
                                        let mut total_jobs = 0;
                                        let mut total_bytes = 0u64;
                                        
                                        // Process each query
                                        for (i, query) in queries.iter().enumerate() {
                                            println!("{} Processing query {}/{}: {:?}", 
                                                style("▶").blue(), i + 1, queries.len(), query.tags);
                                            
                                            match query_planner.create_query_plan(query).await {
                                                Ok(plan) => {
                                                    println!("{} Query plan created: {} jobs, ~{:.1} MB",
                                                        style("✓").green(),
                                                        plan.jobs.len(),
                                                        plan.estimated_size_bytes as f64 / 1_048_576.0
                                                    );
                                                    
                                                    total_jobs += plan.jobs.len();
                                                    total_bytes += plan.estimated_size_bytes;
                                                    
                                                    // Execute downloads if jobs exist and credentials are valid
                                                    if !plan.jobs.is_empty() {
                                                        // Check if we have valid credentials
                                                        let e621_config = config_manager.get_e621_config()
                                                            .map_err(|e| format!("Config error: {}", e)).unwrap();
                                                        
                                                        if e621_config.auth.username != "your_username" && 
                                                           e621_config.auth.api_key != "your_api_key_here" &&
                                                           !e621_config.auth.username.is_empty() &&
                                                           !e621_config.auth.api_key.is_empty() {
                                                            
                                                            println!("{} Executing {} downloads...", 
                                                                style("▶").blue(), plan.jobs.len());
                                                            
                                                            // Execute downloads
                                                            for job in &plan.jobs {
                                                                match download_engine.queue_job(job.clone()).await {
                                                                    Ok(()) => {
                                                                        println!("{} Queued download: {} ({})", 
                                                                            style("✓").green(), job.post_id, job.md5);
                                                                    }
                                                                    Err(e) => {
                                                                        println!("{} Failed to queue download {}: {}", 
                                                                            style("✗").red(), job.post_id, e);
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            println!("{}", style("⚠ Skipping downloads: No valid E621 credentials configured").yellow());
                                                            println!("   Please edit e621.toml and set your username and API key");
                                                        }
                                                    }
                                                    
                                                    // Queue the jobs in orchestrator for tracking
                                                    if let Err(e) = query_planner.queue_jobs(&plan).await {
                                                        println!("{} Failed to queue jobs: {}", style("✗").red(), e);
                                                    }
                                                }
                                                Err(e) => {
                                                    println!("{} Failed to create query plan: {}", style("✗").red(), e);
                                                }
                                            }
                                        }
                                        
                                        if total_jobs > 0 {
                                            println!("{} Download execution completed!", style("✓").green());
                                            println!("Total jobs processed: {}", total_jobs);
                                            println!("Total estimated size: {:.1} MB", total_bytes as f64 / 1_048_576.0);
                                        } else {
                                            println!("{}", style("No downloads found matching your criteria.").yellow());
                                        }
                                    }
                                    Err(e) => {
                                        println!("{} Failed to initialize download engine: {}", style("✗").red(), e);
                                        println!("{}", style("Download planning completed!").green());
                                        println!("{}", style("Note: Download engine initialization failed - only query planning was completed.").yellow());
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{} Failed to parse queries: {}", style("✗").red(), e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("{} Failed to initialize query planner: {}", style("✗").red(), e);
                    }
                }
            }
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("✗").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit configuration files
    async fn edit_config(&self) -> CliResult<()> {
        loop {
            println!("\n{}", style("Edit Configuration").cyan().bold());
            
            // Show config edit menu
            let options = ConfigEditOption::variants();
            let option_names: Vec<&str> = options.iter().map(|o| o.display_name()).collect();
            
            let selection = Select::with_theme(&self.theme)
                .items(&option_names)
                .default(0)
                .interact()?;
            
            match options[selection] {
                ConfigEditOption::EditAppConfig => self.edit_app_config().await?,
                ConfigEditOption::EditE621Config => self.edit_e621_config().await?,
                ConfigEditOption::EditRulesConfig => self.edit_rules_config().await?,
                ConfigEditOption::Back => break,
            }
        }
        
        Ok(())
    }
    
    /// Edit application config
    async fn edit_app_config(&self) -> CliResult<()> {
        println!("\n{}", style("Edit Application Config").cyan().bold());
        
        // Get the current app config
        let app_config = match self.config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Display current config values
        println!("{}", style("Current configuration:").cyan());
        println!("Download directory: {}", app_config.paths.download_directory);
        println!("Database file: {}", app_config.paths.database_file);
        println!("Max download concurrency: {}", app_config.pools.max_download_concurrency);
        println!("Max API concurrency: {}", app_config.pools.max_api_concurrency);
        
        println!("\n{}", style("Config editing functionality not fully implemented.").yellow());
        println!("Please edit the config.toml file directly for now.");
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit E621 config
    async fn edit_e621_config(&self) -> CliResult<()> {
        println!("\n{}", style("Edit E621 Config").cyan().bold());
        
        // Get the current e621 config
        let e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Display current config values
        println!("{}", style("Current configuration:").cyan());
        println!("Username: {}", e621_config.auth.username);
        println!("API Key: {}", if e621_config.auth.api_key == "your_api_key_here" { 
            "Not set" 
        } else { 
            "Set (hidden)" 
        });
        println!("Download favorites: {}", e621_config.options.download_favorites);
        println!("Safe mode: {}", e621_config.options.safe_mode);
        
        println!("\n{}", style("Config editing functionality not fully implemented.").yellow());
        println!("Please edit the e621.toml file directly for now.");
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit rules config
    async fn edit_rules_config(&self) -> CliResult<()> {
        println!("\n{}", style("Edit Rules Config").cyan().bold());
        
        // Get the current rules config
        let rules_config = match self.config_manager.get_rules_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Display current config values
        println!("{}", style("Current configuration:").cyan());
        println!("Whitelist tags: {:?}", rules_config.whitelist.tags);
        println!("Blacklist tags: {:?}", rules_config.blacklist.tags);
        
        println!("\n{}", style("Config editing functionality not fully implemented.").yellow());
        println!("Please edit the rules.toml file directly for now.");
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Validate all configuration files
    async fn validate_configs(&self) -> CliResult<()> {
        println!("\n{}", style("Validate Configurations").cyan().bold());
        
        // Check if all config files exist
        let files_exist = self.config_manager.check_config_files();
        if files_exist {
            println!("{}", style("All configuration files exist.").green());
        } else {
            println!("{}", style("Some configuration files are missing.").red());
        }
        
        // Try to load each config to validate
        let app_config_result = self.config_manager.get_app_config();
        let e621_config_result = self.config_manager.get_e621_config();
        let rules_config_result = self.config_manager.get_rules_config();
        
        println!("\n{}", style("Validation results:").cyan());
        
        match app_config_result {
            Ok(_) => println!("{}", style("✓ Application config is valid").green()),
            Err(e) => println!("{} {}", style("✗ Application config error:").red(), e),
        }
        
        match e621_config_result {
            Ok(config) => {
                println!("{}", style("✓ E621 config is valid").green());
                
                // Additional validation for e621 config
                if config.auth.username == "your_username" || config.auth.api_key == "your_api_key_here" {
                    println!("{}", style("  ⚠ Warning: Default credentials detected").yellow());
                }
                
                if config.query.tags.is_empty() {
                    println!("{}", style("  ⚠ Warning: No tags defined").yellow());
                }
            },
            Err(e) => println!("{} {}", style("✗ E621 config error:").red(), e),
        }
        
        match rules_config_result {
            Ok(_) => println!("{}", style("✓ Rules config is valid").green()),
            Err(e) => println!("{} {}", style("✗ Rules config error:").red(), e),
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Verify the downloaded archive
    async fn verify_archive(&self) -> CliResult<()> {
        println!("\n{}", style("Verify Archive").cyan().bold());
        println!("{}", style("Archive verification not yet implemented.").yellow());
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Ensure E621 credentials are configured, prompt if not
    async fn ensure_e621_credentials(&mut self) -> ConfigResult<()> {
        use crate::v3::{test_config_credentials};
        
        // First, make sure all config files exist
        if let Err(e) = self.config_manager.create_default_configs() {
            println!("{} Failed to create config files: {}", style("✗").red(), e);
            return Err(e);
        }
        
        // Check if credentials are already set
        if self.config_manager.has_valid_e621_credentials() {
            // Validate the existing credentials
            match self.config_manager.get_e621_config() {
                Ok(config) => {
                    println!("{}", style("Validating E621 credentials...").cyan());
                    match test_config_credentials(&config).await {
                        Ok(message) => {
                            println!("{} {}", style("✓").green(), message);
                            return Ok(());
                        }
                        Err(e) => {
                            println!("{} Credential validation failed: {}", style("⚠").yellow(), e);
                            println!("{}", style("You'll need to update your credentials.").yellow());
                        }
                    }
                }
                Err(e) => {
                    println!("{} Failed to load E621 config: {}", style("✗").red(), e);
                }
            }
        }
        
        // Prompt for credentials
        match self.setup_e621_credentials().await {
            Ok(()) => Ok(()),
            Err(CliError::Config(e)) => Err(e),
            Err(e) => {
                // Convert other CLI errors to a generic config error
                Err(ConfigError::NotFound(format!("CLI error during credential setup: {}", e)))
            }
        }
    }
    
    /// Interactive setup of E621 credentials
    async fn setup_e621_credentials(&self) -> CliResult<()> {
        use crate::v3::{validate_e621_credentials};
        
        println!("\n{}", style("E621 API Setup").cyan().bold());
        println!("{}", style("To download from E621, you need to provide your API credentials.").white());
        println!("{}", style("You can get these from: https://e621.net/users/home").white());
        println!("{}", style("(Look for 'Manage API Access')").white());
        println!();
        
        loop {
            // Get username
            let username: String = Input::with_theme(&self.theme)
                .with_prompt("E621 Username")
                .interact_text()?;
            
            if username.trim().is_empty() {
                println!("{}", style("Username cannot be empty. Please try again.").red());
                continue;
            }
            
            // Get API key
            let api_key: String = Input::with_theme(&self.theme)
                .with_prompt("E621 API Key")
                .interact_text()?;
            
            if api_key.trim().is_empty() {
                println!("{}", style("API Key cannot be empty. Please try again.").red());
                continue;
            }
            
            // Validate the credentials
            println!("{}", style("Validating credentials...").cyan());
            match validate_e621_credentials(&username, &api_key).await {
                Ok(message) => {
                    println!("{} {}", style("✓").green(), message);
                    
                    // Save the credentials
                    match self.save_credentials(&username, &api_key).await {
                        Ok(()) => {
                            println!("{}", style("Credentials saved successfully!").green());
                            return Ok(());
                        }
                        Err(e) => {
                            println!("{} Failed to save credentials: {}", style("✗").red(), e);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", style("✗").red(), e);
                    
                    if !Confirm::with_theme(&self.theme)
                        .with_prompt("Would you like to try again?")
                        .default(true)
                        .interact()?
                    {
                        println!("{}", style("Setup cancelled. You can configure credentials later by editing e621.toml").yellow());
                        return Ok(());
                    }
                }
            }
        }
    }
    
    /// Save credentials to the e621 config file
    async fn save_credentials(&self, username: &str, api_key: &str) -> CliResult<()> {
        // Get the current config
        let mut e621_config = self.config_manager.get_e621_config()
            .map_err(CliError::Config)?;
        
        // Update the credentials
        e621_config.auth.username = username.to_string();
        e621_config.auth.api_key = api_key.to_string();
        
        // Save the updated config
        self.config_manager.save_e621_config(&e621_config)
            .map_err(CliError::Config)?;
        
        Ok(())
    }
    
    /// Search by tags
    async fn search_tags(&self) -> CliResult<()> {
        println!("\n{}", style("Search by Tags").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get search term from user
        let search_term: String = Input::with_theme(&self.theme)
            .with_prompt("Enter tag name to search for")
            .interact_text()?;
        
        if !search_term.trim().is_empty() {
            // Get search results
            let tag_results = self.fetch_tag_search_results(&search_term, 10).await?;
            
            if !tag_results.is_empty() {
                // Create display items for the selection interface
                let tag_display_items: Vec<String> = tag_results.iter().map(|(tag, info)| {
                    let category_name = match info.category {
                        0 => "General",
                        1 => "Artist", 
                        3 => "Character",
                        4 => "Species",
                        5 => "Meta",
                        _ => "Other",
                    };
                    format!("{} ({}|{})", tag, category_name, info.post_count)
                }).collect();
                
                println!("\n{} Found {} matching tags", style("✓").green(), tag_results.len());
                
                // Let user select which tags to add
                let selections = MultiSelect::with_theme(&self.theme)
                    .items(&tag_display_items)
                    .with_prompt("Select tags to add to saved queries (Space to select, Enter to confirm)")
                    .interact()?;
                
                if !selections.is_empty() {
                    let mut added_count = 0;
                    for index in selections {
                        let (tag, _) = &tag_results[index];
                        if !e621_config.query.tags.contains(tag) {
                            e621_config.query.tags.push(tag.clone());
                            added_count += 1;
                        }
                    }
                    
                    if added_count > 0 {
                        // Save the updated config back to file
                        if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                            println!("{}: {}", style("Error saving config").red().bold(), e);
                        } else {
                            println!("{} {} tags added successfully!", style("✓").green(), added_count);
                        }
                    } else {
                        println!("{}", style("No new tags were added (already in saved tags).").yellow());
                    }
                } else {
                    println!("{}", style("No tags selected.").yellow());
                }
            } else {
                println!("{}", style("No matching tags found.").yellow());
            }
        } else {
            println!("{}", style("No search term entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search by artists
    async fn search_artists(&self) -> CliResult<()> {
        println!("\n{}", style("Search by Artists").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get search term from user
        let search_term: String = Input::with_theme(&self.theme)
            .with_prompt("Enter artist name to search for")
            .interact_text()?;
        
        if !search_term.trim().is_empty() {
            // Get search results
            let artist_results = self.fetch_artist_search_results(&search_term, 10).await?;
            
            if !artist_results.is_empty() {
                // Create display items for the selection interface
                let artist_display_items: Vec<String> = artist_results.iter().map(|(artist, info)| {
                    let status = if info.is_active { "Active" } else { "Inactive" };
                    let other_names = if info.other_names.is_empty() {
                        "None".to_string()
                    } else {
                        info.other_names.join(", ")
                    };
                    format!("{} ({}|{})", artist, status, other_names)
                }).collect();
                
                println!("\n{} Found {} matching artists", style("✓").green(), artist_results.len());
                
                // Let user select which artists to add
                let selections = MultiSelect::with_theme(&self.theme)
                    .items(&artist_display_items)
                    .with_prompt("Select artists to add to saved queries (Space to select, Enter to confirm)")
                    .interact()?;
                
                if !selections.is_empty() {
                    let mut added_count = 0;
                    for index in selections {
                        let (artist, _) = &artist_results[index];
                        if !e621_config.query.artists.contains(artist) {
                            e621_config.query.artists.push(artist.clone());
                            added_count += 1;
                        }
                    }
                    
                    if added_count > 0 {
                        // Save the updated config back to file
                        if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                            println!("{}: {}", style("Error saving config").red().bold(), e);
                        } else {
                            println!("{} {} artists added successfully!", style("✓").green(), added_count);
                        }
                    } else {
                        println!("{}", style("No new artists were added (already in saved artists).").yellow());
                    }
                } else {
                    println!("{}", style("No artists selected.").yellow());
                }
            } else {
                println!("{}", style("No matching artists found.").yellow());
            }
        } else {
            println!("{}", style("No search term entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search by pools
    async fn search_pools(&self) -> CliResult<()> {
        println!("\n{}", style("Search by Pools").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get pool IDs from user
        let pools: String = Input::with_theme(&self.theme)
            .with_prompt("Enter pool IDs (space separated numbers)")
            .interact_text()?;
        
        // Split pool IDs and add to config
        let mut new_pools = Vec::new();
        for pool_str in pools.split_whitespace() {
            match pool_str.parse::<usize>() {
                Ok(pool_id) => {
                    if !e621_config.query.pools.contains(&pool_id) {
                        new_pools.push(pool_id);
                    }
                }
                Err(_) => {
                    println!("{} Invalid pool ID: {}", style("⚠").yellow(), pool_str);
                }
            }
        }
        
        if !new_pools.is_empty() {
            // Add new pools to existing pools
            e621_config.query.pools.extend(new_pools);
            
            // Save the updated config back to file
            if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                println!("{}: {}", style("Error saving config").red().bold(), e);
            } else {
                println!("{}", style("Pools added successfully!").green());
                println!("Current pools: {:?}", e621_config.query.pools);
            }
        } else {
            println!("{}", style("No valid pool IDs entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search by collections
    async fn search_collections(&self) -> CliResult<()> {
        println!("\n{}", style("Search by Collections").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get collection IDs from user
        let collections: String = Input::with_theme(&self.theme)
            .with_prompt("Enter collection IDs (space separated numbers)")
            .interact_text()?;
        
        // Split collection IDs and add to config
        let mut new_collections = Vec::new();
        for collection_str in collections.split_whitespace() {
            match collection_str.parse::<usize>() {
                Ok(collection_id) => {
                    if !e621_config.query.collections.contains(&collection_id) {
                        new_collections.push(collection_id);
                    }
                }
                Err(_) => {
                    println!("{} Invalid collection ID: {}", style("⚠").yellow(), collection_str);
                }
            }
        }
        
        if !new_collections.is_empty() {
            // Add new collections to existing collections
            e621_config.query.collections.extend(new_collections);
            
            // Save the updated config back to file
            if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                println!("{}: {}", style("Error saving config").red().bold(), e);
            } else {
                println!("{}", style("Collections added successfully!").green());
                println!("Current collections: {:?}", e621_config.query.collections);
            }
        } else {
            println!("{}", style("No valid collection IDs entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search by posts
    async fn search_posts(&self) -> CliResult<()> {
        println!("\n{}", style("Search by Posts").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Get post IDs from user
        let posts: String = Input::with_theme(&self.theme)
            .with_prompt("Enter post IDs (space separated numbers)")
            .interact_text()?;
        
        // Split post IDs and add to config
        let mut new_posts = Vec::new();
        for post_str in posts.split_whitespace() {
            match post_str.parse::<usize>() {
                Ok(post_id) => {
                    if !e621_config.query.posts.contains(&post_id) {
                        new_posts.push(post_id);
                    }
                }
                Err(_) => {
                    println!("{} Invalid post ID: {}", style("⚠").yellow(), post_str);
                }
            }
        }
        
        if !new_posts.is_empty() {
            // Add new posts to existing posts
            e621_config.query.posts.extend(new_posts);
            
            // Save the updated config back to file
            if let Err(e) = self.config_manager.save_e621_config(&e621_config) {
                println!("{}: {}", style("Error saving config").red().bold(), e);
            } else {
                println!("{}", style("Posts added successfully!").green());
                println!("Current posts: {:?}", e621_config.query.posts);
            }
        } else {
            println!("{}", style("No valid post IDs entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Helper function to fetch tag search results
    async fn fetch_tag_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<(String, crate::v3::query_planner::TagInfo)>> {
        use crate::v3::orchestration::QueryQueue;

        // Initialize the orchestrator and query planner
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone()).await {
            Ok(qp) => qp,
            Err(e) => {
                println!("{} Failed to initialize query planner: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        // Search for tags
        match query_planner.search_tags(search_term, max_results as u32).await {
            Ok(tags) => {
                let results: Vec<(String, crate::v3::query_planner::TagInfo)> = tags.into_iter()
                    .map(|tag| (tag.name.clone(), tag))
                    .collect();
                Ok(results)
            },
            Err(e) => {
                println!("{} Tag search failed: {}", style("✗").red(), e);
                Ok(Vec::new())
            }
        }
    }

    /// Helper function to fetch artist search results
    async fn fetch_artist_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<(String, crate::v3::query_planner::ArtistInfo)>> {
        use crate::v3::orchestration::QueryQueue;

        // Initialize the orchestrator and query planner
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone()).await {
            Ok(qp) => qp,
            Err(e) => {
                println!("{} Failed to initialize query planner: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        // Search for artists
        match query_planner.search_artists(search_term, max_results as u32).await {
            Ok(artists) => {
                let results: Vec<(String, crate::v3::query_planner::ArtistInfo)> = artists.into_iter()
                    .map(|artist| (artist.name.clone(), artist))
                    .collect();
                Ok(results)
            },
            Err(e) => {
                println!("{} Artist search failed: {}", style("✗").red(), e);
                Ok(Vec::new())
            }
        }
    }

    /// Helper function to display tag search results
    async fn display_tag_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<String>> {
        use crate::v3::orchestration::QueryQueue;

        // Initialize the orchestrator and query planner
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone()).await {
            Ok(qp) => qp,
            Err(e) => {
                println!("{} Failed to initialize query planner: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        println!("{} Searching for tags matching: '{}'", style("🔍").cyan(), search_term);

        // Search for tags
        match query_planner.search_tags(search_term, max_results as u32).await {
            Ok(tags) => {
                if tags.is_empty() {
                    println!("{}", style("No matching tags found.").yellow());
                    return Ok(Vec::new());
                }

                println!("{} Found {} matching tags:", style("✓").green(), tags.len());
                println!();

                let mut selected_tags = Vec::new();
                // Display each tag with details
                for (i, tag) in tags.iter().enumerate() {
                    let category_name = match tag.category {
                        0 => "General",
                        1 => "Artist", 
                        3 => "Character",
                        4 => "Species",
                        5 => "Meta",
                        _ => "Other",
                    };
                    
                    println!("{} {}", 
                        style(&format!("{}.", i + 1)).bold(), 
                        style(&tag.name).cyan());
                    println!("   Category: {} | Posts: {}", category_name, tag.post_count);
                    println!();
                    
                    selected_tags.push(tag.name.clone());
                }

                Ok(selected_tags)
            },
            Err(e) => {
                println!("{} Tag search failed: {}", style("✗").red(), e);
                Ok(Vec::new())
            }
        }
    }

    /// Helper function to display artist search results
    async fn display_artist_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<String>> {
        use crate::v3::orchestration::QueryQueue;

        // Initialize the orchestrator and query planner
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone()).await {
            Ok(qp) => qp,
            Err(e) => {
                println!("{} Failed to initialize query planner: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        println!("{} Searching for artists matching: '{}'", style("🔍").cyan(), search_term);

        // Search for artists
        match query_planner.search_artists(search_term, max_results as u32).await {
            Ok(artists) => {
                if artists.is_empty() {
                    println!("{}", style("No matching artists found.").yellow());
                    return Ok(Vec::new());
                }

                println!("{} Found {} matching artists:", style("✓").green(), artists.len());
                println!();

                let mut selected_artists = Vec::new();
                // Display each artist with details
                for (i, artist) in artists.iter().enumerate() {
                    println!("{} {}", 
                        style(&format!("{}.", i + 1)).bold(), 
                        style(&artist.name).cyan());
                    println!("   Status: {} | Other names: {}", 
                        if artist.is_active { "Active" } else { "Inactive" },
                        if artist.other_names.is_empty() {
                            "None".to_string()
                        } else {
                            artist.other_names.join(", ")
                        }
                    );
                    println!();
                    
                    selected_artists.push(artist.name.clone());
                }

                Ok(selected_artists)
            },
            Err(e) => {
                println!("{} Artist search failed: {}", style("✗").red(), e);
                Ok(Vec::new())
            }
        }
    }

    /// Helper function to pause and wait for user to press Enter
    fn press_enter_to_continue(&self) -> CliResult<()> {
        println!("\nPress Enter to continue...");
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;
        Ok(())
    }
}

/// Run the CLI
pub async fn run_cli(config_dir: impl AsRef<Path>) -> Result<()> {
    let cli_manager = CliManager::new(config_dir).await?;
    cli_manager.run().await?;
    Ok(())
}
