//! Interactive CLI menu system for E621 Downloader
//! Uses dialoguer for user interaction

use std::path::Path;
use std::io::{self, IsTerminal};
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;

use dialoguer::{theme::ColorfulTheme, Select, Input, Confirm, MultiSelect};
use console::style as __console_style;

// Sanitize styled output: strip all non-ASCII (emoji/symbols) before styling
fn style<S: Into<String>>(s: S) -> console::StyledObject<String> {
    let raw: String = s.into();
    let cleaned: String = raw.chars().filter(|c| c.is_ascii()).collect();
    __console_style(cleaned)
}

use crate::v3::{
    ConfigManager, ConfigResult, ConfigError,
    init_config, init_orchestrator, init_query_planner, init_hash_manager,
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
    ToggleFavorites,
    AddQuery,
    EditQuery,
    StartDownload,
    ResumeDownloads,
    EditConfig,
    ValidateConfigs,
    VerifyArchive,
    DatabaseInspector,
    PerformanceMonitor,
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
            MainMenuOption::ToggleFavorites,
            MainMenuOption::AddQuery,
            MainMenuOption::EditQuery,
            MainMenuOption::StartDownload,
            MainMenuOption::ResumeDownloads,
            MainMenuOption::EditConfig,
            MainMenuOption::ValidateConfigs,
            MainMenuOption::VerifyArchive,
            MainMenuOption::DatabaseInspector,
            MainMenuOption::PerformanceMonitor,
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
            MainMenuOption::ToggleFavorites => "Toggle download favorites",
            MainMenuOption::AddQuery => "Add new query",
            MainMenuOption::EditQuery => "Edit queries",
            MainMenuOption::StartDownload => "Start download",
            MainMenuOption::ResumeDownloads => "Resume incomplete downloads",
            MainMenuOption::EditConfig => "Edit config",
            MainMenuOption::ValidateConfigs => "Validate configs",
            MainMenuOption::VerifyArchive => "Verify archive",
            MainMenuOption::DatabaseInspector => "Database inspector",
            MainMenuOption::PerformanceMonitor => "Performance monitor",
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
                MainMenuOption::ToggleFavorites => self.toggle_favorites().await?,
                MainMenuOption::AddQuery => self.add_query().await?,
                MainMenuOption::EditQuery => self.edit_query().await?,
                MainMenuOption::StartDownload => self.start_download().await?,
                MainMenuOption::ResumeDownloads => self.resume_downloads().await?,
                MainMenuOption::EditConfig => self.edit_config().await?,
                MainMenuOption::ValidateConfigs => self.validate_configs().await?,
                MainMenuOption::VerifyArchive => self.verify_archive().await?,
                MainMenuOption::DatabaseInspector => self.database_inspector().await?,
                MainMenuOption::PerformanceMonitor => self.performance_monitor().await?,
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
        
        use crate::v3::{init_orchestrator, init_query_planner, init_download_engine};
        use std::sync::Arc;
        use std::path::Path;
        
        // Pre-flight checks
        println!("{}", style("Performing pre-flight checks...").cyan());
        
        // Check and create download directories
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        let app_config = match config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{} Failed to get app config: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        let e621_config = match config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{} Failed to get E621 config: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Check and create download directory
        let download_dir = Path::new(&app_config.paths.download_directory);
        if !download_dir.exists() {
            println!("{} Download directory does not exist: {}", 
                style("!").yellow(), download_dir.display());
            println!("{} Creating download directory...", style("").blue());
            
            if let Err(e) = std::fs::create_dir_all(&download_dir) {
                println!("{} Failed to create download directory: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
            println!("{} Created download directory: {}", 
                style("").green(), download_dir.display());
        } else {
            println!("{} Download directory verified: {}", 
                style("").green(), download_dir.display());
        }
        
        // Check and create temp directory
        let temp_dir = Path::new(&app_config.paths.temp_directory);
        if !temp_dir.exists() {
            println!("{} Creating temp directory...", style("").blue());
            if let Err(e) = std::fs::create_dir_all(&temp_dir) {
                println!("{} Failed to create temp directory: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
            println!("{} Created temp directory: {}", 
                style("").green(), temp_dir.display());
        }
        
        // Report blacklist status (now using E621 API)
        println!("{} Blacklist will be fetched from E621 API using your account settings", 
            style("").blue());
        
        // Check credentials
        if e621_config.auth.username == "your_username" || 
           e621_config.auth.api_key == "your_api_key_here" ||
           e621_config.auth.username.is_empty() ||
           e621_config.auth.api_key.is_empty() {
            println!("{} No valid E621 credentials configured", style("").yellow());
            println!("   Downloads will be attempted without authentication");
            println!("   Some content may be inaccessible");
            println!("   To configure credentials, edit e621.toml");
        } else {
            println!("{} E621 authentication configured for user: {}", 
                style("").green(), e621_config.auth.username);
        }
        
        // Initialize the orchestrator
        println!("\n{}", style("Initializing orchestrator...").cyan());
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
                println!("{}", style(" Orchestrator initialized").green());
                
                // Initialize hash manager
                println!("{}", style("Initializing hash manager...").cyan());
                let hash_manager = match init_hash_manager(config_manager.clone()).await {
                    Ok(hm) => {
                        println!("{}", style(" Hash manager initialized").green());
                        hm
                    }
                    Err(e) => {
                        println!("{} Failed to initialize hash manager: {}", style("").red(), e);
                        self.press_enter_to_continue()?;
                        return Ok(());
                    }
                };
                
                // Initialize the query planner
                println!("{}", style("Initializing query planner...").cyan());
                match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone(), hash_manager).await {
                    Ok(query_planner) => {
                        println!("{}", style(" Query planner initialized").green());
                        
                        // Parse input from configuration
                        println!("{}", style("Parsing queries from configuration...").cyan());
                        match query_planner.parse_input().await {
                            Ok(queries) => {
                                if queries.is_empty() {
                                    println!("{}", style("No queries found in configuration. Please add some tags first.").yellow());
                                    self.press_enter_to_continue()?;
                                    return Ok(());
                                }
                                
                                println!("{} Found {} queries", style("").green(), queries.len());
                                
                                // Initialize download engine
                                println!("{}", style("Initializing download engine...").cyan());
                                match init_download_engine(config_manager.clone()).await {
                                    Ok(download_engine) => {
                                        println!("{}", style(" Download engine initialized").green());
                                        
                                        let mut total_jobs = 0;
                                        let mut total_bytes = 0u64;
                                        
                        // Process each query
                        for (i, query) in queries.iter().enumerate() {
                            println!("{} Processing query {}/{}: {:?}", 
                                style("").blue(), i + 1, queries.len(), query.tags);
                            
                            match query_planner.create_query_plan(query).await {
                                Ok(plan) => {
                                    let skipped_count = plan.estimated_post_count - plan.jobs.len() as u32;
                                    
                                    println!("{} Query plan created:", style("").green());
                                    println!("   Posts found: {}", plan.estimated_post_count);
                                    println!("   New downloads: {}", plan.jobs.len());
                                    if skipped_count > 0 {
                                        println!("   Already downloaded: {}", skipped_count);
                                    }
                                    println!("   Estimated size: ~{:.1} MB",
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
                                                                style("").blue(), plan.jobs.len());
                                                            
                                                            // Execute downloads
                                                            for job in &plan.jobs {
                                                                match download_engine.queue_job(job.clone()).await {
                                                                    Ok(()) => {
                                                                        println!("{} Queued download: {} ({})", 
                                                                            style("").green(), job.post_id, job.md5);
                                                                    }
                                                                    Err(e) => {
                                                                        println!("{} Failed to queue download {}: {}", 
                                                                            style("").red(), job.post_id, e);
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            println!("{}", style(" Skipping downloads: No valid E621 credentials configured").yellow());
                                                            println!("   Please edit e621.toml and set your username and API key");
                                                        }
                                                    }
                                                    
                                                    // Queue the jobs in orchestrator for tracking
                                                    if let Err(e) = query_planner.queue_jobs(&plan).await {
                                                        println!("{} Failed to queue jobs: {}", style("").red(), e);
                                                    }
                                                }
                                                Err(e) => {
                                                    println!("{} Failed to create query plan: {}", style("").red(), e);
                                                }
                                            }
                                        }
                                        
                                        if total_jobs > 0 {
                                            println!("{} Download execution completed!", style("").green());
                                            println!("Total jobs processed: {}", total_jobs);
                                            println!("Total estimated size: {:.1} MB", total_bytes as f64 / 1_048_576.0);
                                        } else {
                                            println!("{}", style("No downloads found matching your criteria.").yellow());
                                        }
                                    }
                                    Err(e) => {
                                        println!("{} Failed to initialize download engine: {}", style("").red(), e);
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
                        println!("{} Failed to initialize query planner: {}", style("").red(), e);
                    }
                }
            }
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Resume incomplete downloads from previous sessions
    async fn resume_downloads(&self) -> CliResult<()> {
        println!("\n{}", style("Resume Incomplete Downloads").cyan().bold());
        
        use crate::v3::{init_session_recovery, init_orchestrator, init_download_engine};
        use std::sync::Arc;
        
        // Initialize session recovery
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        let app_config = match config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{} Failed to get app config: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Initialize session recovery with database path
        let recovery_db_path = std::path::Path::new(&app_config.paths.database_file)
            .parent()
            .unwrap_or(std::path::Path::new("./data"))
            .join("session_recovery.sqlite");
        
        let session_recovery = match init_session_recovery(&recovery_db_path, config_manager.clone()).await {
            Ok(sr) => sr,
            Err(e) => {
                println!("{} Failed to initialize session recovery: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Get incomplete sessions
        let incomplete_sessions = match session_recovery.get_incomplete_sessions().await {
            Ok(sessions) => sessions,
            Err(e) => {
                println!("{} Failed to get incomplete sessions: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        if incomplete_sessions.is_empty() {
            println!("{}", style("No incomplete download sessions found.").yellow());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        // Display incomplete sessions
                println!("\n{} Found {} incomplete sessions:", style("").blue(), incomplete_sessions.len());
        
        let mut session_display = Vec::new();
        for (i, session) in incomplete_sessions.iter().enumerate() {
            let age = chrono::Utc::now() - session.created_at;
            let age_str = if age.num_days() > 0 {
                format!("{} days ago", age.num_days())
            } else if age.num_hours() > 0 {
                format!("{} hours ago", age.num_hours())
            } else {
                format!("{} minutes ago", age.num_minutes())
            };
            
            let session_str = format!(
                "Session {} - {} jobs total, {} completed, {} failed ({})",
                i + 1,
                session.total_jobs,
                session.completed_jobs,
                session.failed_jobs,
                age_str
            );
            session_display.push(session_str);
        }
        session_display.push("Cancel".to_string());
        
        let selection = Select::with_theme(&self.theme)
            .with_prompt("Select session to resume")
            .items(&session_display)
            .interact()?;
        
        if selection >= incomplete_sessions.len() {
            // User selected "Cancel"
            return Ok(());
        }
        
        let selected_session = &incomplete_sessions[selection];
        
        // Get incomplete jobs for the selected session
        let incomplete_jobs = match session_recovery.get_incomplete_jobs(selected_session.id).await {
            Ok(jobs) => jobs,
            Err(e) => {
                println!("{} Failed to get incomplete jobs: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        if incomplete_jobs.is_empty() {
            println!("{}", style("No incomplete jobs found in this session.").yellow());
            // Mark session as complete if all jobs are done
            if let Err(e) = session_recovery.complete_session(selected_session.id).await {
                println!("{} Failed to mark session as complete: {}", style("").yellow(), e);
            }
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        println!("\n{} Found {} incomplete downloads to resume", style("").green(), incomplete_jobs.len());
        
        // Ask for confirmation
        if !Confirm::with_theme(&self.theme)
            .with_prompt(format!("Resume downloading {} files?", incomplete_jobs.len()))
            .default(true)
            .interact()? {
            return Ok(());
        }
        
        // Initialize orchestrator and download engine
        let _orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        let download_engine = match init_download_engine(config_manager.clone()).await {
            Ok(de) => de,
            Err(e) => {
                println!("{} Failed to initialize download engine: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Queue incomplete jobs for download
        println!("{}", style("Resuming downloads...").cyan());
        let mut successful_queues = 0;
        
        for job in &incomplete_jobs {
            // Update job status to pending in recovery system
            if let Err(e) = session_recovery.update_job_status(
                job.id, 
                crate::v3::JobStatus::Pending, 
                None
            ).await {
                println!("{} Failed to update job status: {}", style("").yellow(), e);
            }
            
            // Queue job in download engine
            match download_engine.queue_job(job.clone()).await {
                Ok(()) => {
                    successful_queues += 1;
                    println!("{} Queued: Post {} ({})", 
                        style("").green(), job.post_id, job.md5);
                }
                Err(e) => {
                    println!("{} Failed to queue job {}: {}", 
                        style("").red(), job.post_id, e);
                    
                    // Update job as failed in recovery system
                    if let Err(e) = session_recovery.update_job_status(
                        job.id, 
                        crate::v3::JobStatus::Failed(e.to_string()), 
                        Some(e.to_string())
                    ).await {
                        println!("{} Failed to update job status: {}", style("").yellow(), e);
                    }
                }
            }
        }
        
        if successful_queues > 0 {
            println!("\n{} {} downloads resumed successfully!", 
                style("").green(), successful_queues);
            
            if successful_queues < incomplete_jobs.len() {
                println!("{} {} downloads failed to queue", 
                    style("").yellow(), incomplete_jobs.len() - successful_queues);
            }
        } else {
            println!("{}", style("No downloads were successfully queued.").red());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Database inspector for examining downloaded content
    async fn database_inspector(&self) -> CliResult<()> {
        println!("\n{}", style("Database Inspector").cyan().bold());
        
        // Get config to find database paths
        let app_config = match self.config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        let db_path = std::path::Path::new(&app_config.paths.database_file);
        if !db_path.exists() {
            println!("{}", style("Database file not found. No downloads have been recorded yet.").yellow());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        loop {
            // Database inspector menu
            println!("\n{}", style("Database Inspector Options:").cyan());
            println!("1. Show download statistics");
            println!("2. List recent downloads");
            println!("3. Search downloads by tag");
            println!("4. Search downloads by artist");
            println!("5. Show duplicate detection stats");
            println!("6. Show database file info");
            println!("7. Clean up database (remove orphaned entries)");
            println!("8. Reset database (delete all records)");
            println!("0. Back to main menu");
            
            let choice: String = Input::with_theme(&self.theme)
                .with_prompt("Select option (0-8)")
                .interact_text()?;
            
            match choice.trim() {
                "0" => break,
                "1" => self.show_download_statistics(&db_path).await?,
                "2" => self.list_recent_downloads(&db_path).await?,
                "3" => self.search_downloads_by_tag(&db_path).await?,
                "4" => self.search_downloads_by_artist(&db_path).await?,
                "5" => self.show_duplicate_detection_stats(&db_path).await?,
                "6" => self.show_database_file_info(&db_path).await?,
                "7" => self.cleanup_database(&db_path).await?,
                "8" => self.reset_database(&db_path).await?,
                _ => println!("{}", style("Invalid choice. Please select 0-8.").red()),
            }
        }
        
        Ok(())
    }
    
    /// Show download statistics from database
    async fn show_download_statistics(&self, db_path: &std::path::Path) -> CliResult<()> {
        use rusqlite::Connection;
        
        println!("\n{}", style("Download Statistics").cyan().bold());
        
        let conn = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                println!("{} Failed to open database: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        // Get basic statistics
        let total_downloads: i64 = conn.query_row(
            "SELECT COUNT(*) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);
        
        println!("{} Total downloads recorded: {}", style("").blue(), total_downloads);
        
        if total_downloads > 0 {
            // Get earliest and latest downloads
            let earliest: String = conn.query_row(
                "SELECT MIN(downloaded_at) FROM downloads", [], |row| row.get(0)
            ).unwrap_or_else(|_| "Unknown".to_string());
            
            let latest: String = conn.query_row(
                "SELECT MAX(downloaded_at) FROM downloads", [], |row| row.get(0)
            ).unwrap_or_else(|_| "Unknown".to_string());
            
            println!("{} First download: {}", style("").blue(), earliest);
            println!("{} Latest download: {}", style("").blue(), latest);
            
            // Get file type distribution
            println!("\n{}", style("File Type Distribution:").cyan());
            let mut stmt = conn.prepare(
                "SELECT SUBSTR(md5, -3) as ext, COUNT(*) as count FROM downloads GROUP BY ext ORDER BY count DESC LIMIT 10"
            ).ok();
            
            if let Some(ref mut stmt) = stmt {
                match stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?
                    ))
                }) {
                    Ok(rows) => {
                        for row in rows {
                            if let Ok((ext, count)) = row {
                                println!("  {}: {} files", ext, count);
                            }
                        }
                    }
                    Err(e) => {
                        println!("{} Query failed: {}", style("").red(), e);
                    }
                }
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// List recent downloads from database
    async fn list_recent_downloads(&self, db_path: &std::path::Path) -> CliResult<()> {
        use rusqlite::Connection;
        
        println!("\n{}", style("Recent Downloads").cyan().bold());
        
        let conn = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                println!("{} Failed to open database: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        let mut stmt = match conn.prepare(
            "SELECT post_id, md5, downloaded_at FROM downloads ORDER BY downloaded_at DESC LIMIT 20"
        ) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("{} Failed to prepare query: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?
            ))
        });
        
        match rows {
            Ok(rows) => {
                println!("{}:", style("Last 20 downloads").cyan());
                for (i, row) in rows.enumerate() {
                    if let Ok((post_id, md5, downloaded_at)) = row {
                        println!("{:2}. Post {}: {} ({})", 
                            i + 1, post_id, &md5[..8], downloaded_at);
                    }
                }
            }
            Err(e) => {
                println!("{} Query failed: {}", style("").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search downloads by tag
    async fn search_downloads_by_tag(&self, _db_path: &std::path::Path) -> CliResult<()> {
        println!("\n{}", style("Search by Tag").cyan().bold());
        
        // Get search term from user
        let search_term: String = Input::with_theme(&self.theme)
            .with_prompt("Enter tag to search for")
            .interact_text()?;
            
        if search_term.trim().is_empty() {
            println!("{}", style("Search term cannot be empty.").yellow());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        // Initialize hash manager
        let config_manager = Arc::new(init_config(&self.config_dir).await?);
        let hash_manager = match init_hash_manager(config_manager.clone()).await {
            Ok(hm) => hm,
            Err(e) => {
                println!("{} Failed to initialize hash manager: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Search for downloads with the tag
        match hash_manager.search_by_tag(&search_term, 20).await {
            Ok(results) => {
                if results.is_empty() {
                    println!("\n{}", style("No downloads found with that tag.").yellow());
                } else {
                    println!("\n{} Found {} downloads with tag '{}'", 
                        style("✓").green(), results.len(), search_term);
                    println!();
                    
                    for (i, record) in results.iter().enumerate() {
                        println!("{} {}", 
                            style(&format!("{}.", i + 1)).bold(),
                            style(&format!("Post {}", record.post_id)).cyan());
                        println!("   MD5: {}", &record.md5[..8]);
                        println!("   File: {}", record.file_path.display());
                        println!("   Size: {} bytes", record.file_size);
                        if !record.artists.is_empty() {
                            println!("   Artists: {}", record.artists.join(", "));
                        }
                        println!("   Tags: {}", record.tags.join(", "));
                        println!();
                    }
                }
            }
            Err(e) => {
                println!("{} Search failed: {}", style("").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Search downloads by artist
    async fn search_downloads_by_artist(&self, _db_path: &std::path::Path) -> CliResult<()> {
        println!("\n{}", style("Search by Artist").cyan().bold());
        
        // Get search term from user
        let search_term: String = Input::with_theme(&self.theme)
            .with_prompt("Enter artist name to search for")
            .interact_text()?;
            
        if search_term.trim().is_empty() {
            println!("{}", style("Search term cannot be empty.").yellow());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        // Initialize hash manager
        let config_manager = Arc::new(init_config(&self.config_dir).await?);
        let hash_manager = match init_hash_manager(config_manager.clone()).await {
            Ok(hm) => hm,
            Err(e) => {
                println!("{} Failed to initialize hash manager: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Search for downloads by the artist
        match hash_manager.search_by_artist(&search_term, 20).await {
            Ok(results) => {
                if results.is_empty() {
                    println!("\n{}", style("No downloads found for that artist.").yellow());
                } else {
                    println!("\n{} Found {} downloads by artist '{}'", 
                        style("✓").green(), results.len(), search_term);
                    println!();
                    
                    for (i, record) in results.iter().enumerate() {
                        println!("{} {}", 
                            style(&format!("{}.", i + 1)).bold(),
                            style(&format!("Post {}", record.post_id)).cyan());
                        println!("   MD5: {}", &record.md5[..8]);
                        println!("   File: {}", record.file_path.display());
                        println!("   Size: {} bytes", record.file_size);
                        println!("   Artists: {}", record.artists.join(", "));
                        if !record.tags.is_empty() {
                            println!("   Tags: {}", record.tags.join(", "));
                        }
                        println!();
                    }
                }
            }
            Err(e) => {
                println!("{} Search failed: {}", style("").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Show duplicate detection statistics
    async fn show_duplicate_detection_stats(&self, db_path: &std::path::Path) -> CliResult<()> {
        use rusqlite::Connection;
        
        println!("\n{}", style("Duplicate Detection Statistics").cyan().bold());
        
        let conn = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                println!("{} Failed to open database: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        // Count unique MD5 hashes vs total entries
        let unique_hashes: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT md5) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);
        
        let total_entries: i64 = conn.query_row(
            "SELECT COUNT(*) FROM downloads", [], |row| row.get(0)
        ).unwrap_or(0);
        
        println!("{} Unique files (MD5): {}", style("").blue(), unique_hashes);
        println!("{} Total database entries: {}", style("").blue(), total_entries);
        
        if total_entries > unique_hashes {
            let duplicates = total_entries - unique_hashes;
            println!("{} Duplicate entries detected: {}", style("").yellow(), duplicates);
            
            // Show some duplicate examples
            let mut stmt = conn.prepare(
                "SELECT md5, COUNT(*) as count FROM downloads GROUP BY md5 HAVING count > 1 ORDER BY count DESC LIMIT 5"
            ).ok();
            
            if let Some(ref mut stmt) = stmt {
                println!("\n{}", style("Top duplicate files:").cyan());
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?
                    ))
                });
                
                if let Ok(rows) = rows {
                    for row in rows {
                        if let Ok((md5, count)) = row {
                            println!("  {}: {} entries", &md5[..8], count);
                        }
                    }
                }
            }
        } else {
            println!("{}", style(" No duplicate entries found").green());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Show database file information
    async fn show_database_file_info(&self, db_path: &std::path::Path) -> CliResult<()> {
        println!("\n{}", style("Database File Information").cyan().bold());
        
        // File size and path
        if let Ok(metadata) = std::fs::metadata(db_path) {
            let size_mb = metadata.len() as f64 / 1_048_576.0;
            println!("{} Database path: {}", style("").blue(), db_path.display());
            println!("{} Database size: {:.2} MB", style("").blue(), size_mb);
            
            if let Ok(modified) = metadata.modified() {
                if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                    let datetime = chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0)
                        .unwrap_or_else(|| chrono::Utc::now());
                    println!("{} Last modified: {}", style("").blue(), datetime.format("%Y-%m-%d %H:%M:%S UTC"));
                }
            }
        } else {
            println!("{} Could not read database file metadata", style("").red());
        }
        
        // Database schema info
        use rusqlite::Connection;
        if let Ok(conn) = Connection::open(db_path) {
            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'").ok();
            if let Some(ref mut stmt) = stmt {
                println!("\n{}", style("Database Tables:").cyan());
                let rows = stmt.query_map([], |row| {
                    Ok(row.get::<_, String>(0)?)
                });
                
                if let Ok(rows) = rows {
                    for row in rows {
                        if let Ok(table_name) = row {
                            println!("  - {}", table_name);
                        }
                    }
                }
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Clean up database by removing orphaned entries
    async fn cleanup_database(&self, db_path: &std::path::Path) -> CliResult<()> {
        println!("\n{}", style("Database Cleanup").cyan().bold());
        
        if !Confirm::with_theme(&self.theme)
            .with_prompt("This will remove orphaned/invalid database entries. Continue?")
            .default(false)
            .interact()? {
            return Ok(());
        }
        
        use rusqlite::Connection;
        let conn = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                println!("{} Failed to open database: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        // Remove entries with empty/invalid MD5 hashes
        let removed = conn.execute(
            "DELETE FROM downloads WHERE md5 IS NULL OR LENGTH(md5) != 32", []
        ).unwrap_or(0);
        
        if removed > 0 {
            println!("{} Removed {} invalid entries", style("").green(), removed);
        } else {
            println!("{} No invalid entries found", style("").green());
        }
        
        // Vacuum the database to reclaim space
        if conn.execute("VACUUM", []).is_ok() {
            println!("{} Database optimized", style("").green());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Reset database by deleting all records from core tables
    async fn reset_database(&self, db_path: &std::path::Path) -> CliResult<()> {
        println!("\n{}", style("Reset Database").cyan().bold());
        println!("This will delete ALL records from the database tables (downloads, download_tags, download_artists, file_integrity).\n");
        
        if !Confirm::with_theme(&self.theme)
            .with_prompt("Are you sure you want to permanently reset the database? This cannot be undone.")
            .default(false)
            .interact()? {
            return Ok(());
        }
        
        use rusqlite::Connection;
        let mut conn = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                println!("{} Failed to open database: {}", style("").red(), e);
                return Ok(());
            }
        };
        
        // Use a transaction to clear tables atomically
        {
            match conn.transaction() {
                Ok(tx) => {
                    let _ = tx.execute("DELETE FROM download_tags", []);
                    let _ = tx.execute("DELETE FROM download_artists", []);
                    let _ = tx.execute("DELETE FROM file_integrity", []);
                    let _ = tx.execute("DELETE FROM downloads", []);
                    if tx.commit().is_ok() {
                        println!("{} Database tables cleared", style("").green());
                    } else {
                        println!("{} Failed to commit database reset", style("").red());
                    }
                }
                Err(_) => println!("{} Failed to start database transaction", style("").red()),
            }
        }
        
        // VACUUM to reclaim space (outside of transaction scope)
        let _ = conn.execute("VACUUM", []);
        println!("{} Database optimized", style("").green());
        
        // Suggest restarting caches by informing user
        println!("Note: In-memory caches will rebuild on next run.");
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Performance monitoring dashboard
    async fn performance_monitor(&self) -> CliResult<()> {
        println!("\n{}", style("Performance Monitor").cyan().bold());
        
        use crate::v3::{init_adaptive_concurrency};
        use std::sync::Arc;
        
        // Initialize config manager
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Initialize adaptive concurrency system
        let concurrency_manager = match init_adaptive_concurrency(config_manager).await {
            Ok(cm) => cm,
            Err(e) => {
                println!("{} Failed to initialize concurrency manager: {}", style("✗").red(), e);
                self.press_enter_to_continue()?;
                return Ok(());
            }
        };
        
        // Get current performance metrics and limits
        let (download_limit, api_limit, hash_limit) = concurrency_manager.get_current_limits();
        let (download_available, api_available, hash_available) = concurrency_manager.get_available_permits();
        let metrics = concurrency_manager.get_performance_metrics();
        let is_under_load = concurrency_manager.is_under_load();
        
        // Calculate usage percentages
        let download_usage = ((download_limit - download_available) as f64 / download_limit as f64) * 100.0;
        let api_usage = ((api_limit - api_available) as f64 / api_limit as f64) * 100.0;
        let hash_usage = ((hash_limit - hash_available) as f64 / hash_limit as f64) * 100.0;
        
        // Display current system status
        println!("\n{}", style("Current System Status").cyan());
        println!("{} System under load: {}", 
            if is_under_load { style("").yellow() } else { style("").green() },
            if is_under_load { style("Yes").red() } else { style("No").green() }
        );
        
        // Display concurrency information
        println!("\n{}", style("Concurrency Limits & Usage").cyan());
        
        println!("{} Download Concurrency: {}/{} ({:.1}% used)", 
            if download_usage > 75.0 { style("").yellow() } else { style("").blue() },
            download_limit - download_available, 
            download_limit, 
            download_usage
        );
        
        println!("{} API Concurrency: {}/{} ({:.1}% used)", 
            if api_usage > 75.0 { style("").yellow() } else { style("").blue() },
            api_limit - api_available, 
            api_limit, 
            api_usage
        );
        
        println!("{} Hash Concurrency: {}/{} ({:.1}% used)", 
            if hash_usage > 75.0 { style("").yellow() } else { style("").blue() },
            hash_limit - hash_available, 
            hash_limit, 
            hash_usage
        );
        
        // Display performance metrics
        println!("\n{}", style("Performance Metrics").cyan());
        
        println!("{} Average Response Time: {:?}", 
            if metrics.avg_response_time.as_secs() > 3 { style("").yellow() } else { style("").blue() },
            metrics.avg_response_time
        );
        
        println!("{} Success Rate: {:.1}%", 
            if metrics.success_rate < 0.9 { style("").yellow() } else { style("").green() },
            metrics.success_rate * 100.0
        );
        
        println!("{} Error Rate: {:.1}%", 
            if metrics.error_rate > 0.1 { style("").yellow() } else { style("").green() },
            metrics.error_rate * 100.0
        );
        
        println!("{} Throughput: {:.2} requests/sec", 
            style("").blue(),
            metrics.throughput
        );
        
        // Performance recommendations
        println!("\n{}", style("Recommendations").cyan());
        
        if metrics.error_rate > 0.2 {
            println!("{} High error rate detected - consider reducing concurrency limits", style("").yellow());
        } else if metrics.avg_response_time.as_secs() > 5 {
            println!("{} Slow response times - consider reducing API concurrency", style("").yellow());
        } else if !is_under_load && metrics.success_rate > 0.95 {
            println!("{} System performing well - current limits are optimal", style("").green());
        } else if is_under_load {
            println!("{} System under heavy load - downloads are progressing normally", style("").blue());
        }
        
        // Show config edit option
        println!("\n{}", style("Options:").cyan());
        
        if Confirm::with_theme(&self.theme)
            .with_prompt("Would you like to optimize concurrency settings automatically?")
            .default(false)
            .interact()? {
            
            match concurrency_manager.optimize_concurrency().await {
                Ok(()) => {
                    println!("{} Concurrency optimization completed", style("").green());
                    
                    // Show new limits
                    let (new_download, new_api, new_hash) = concurrency_manager.get_current_limits();
                    if new_download != download_limit || new_api != api_limit || new_hash != hash_limit {
                        println!("New limits: Download={}, API={}, Hash={}", new_download, new_api, new_hash);
                    } else {
                        println!("Limits remain unchanged - current settings are optimal");
                    }
                }
                Err(e) => {
                    println!("{} Optimization failed: {}", style("").red(), e);
                }
            }
        }
        
        if Confirm::with_theme(&self.theme)
            .with_prompt("Would you like to edit concurrency limits manually?")
            .default(false)
            .interact()? {
            
            self.edit_app_config().await?;
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
        let mut app_config = match self.config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        loop {
            // Display current config values
            println!("\n{}", style("Current Application Configuration:").cyan());
            println!("1. Download directory: {}", app_config.paths.download_directory);
            println!("2. Database file: {}", app_config.paths.database_file);
            println!("3. Log directory: {}", app_config.paths.log_directory);
            println!("4. Temp directory: {}", app_config.paths.temp_directory);
            println!("5. Max download concurrency: {}", app_config.pools.max_download_concurrency);
            println!("6. Max API concurrency: {}", app_config.pools.max_api_concurrency);
            println!("7. Batch size: {}", app_config.pools.batch_size);
            println!("8. Posts per page: {}", app_config.limits.posts_per_page);
            println!("9. Max page number: {}", app_config.limits.max_page_number);
            println!("10. File size cap (bytes): {}", app_config.limits.file_size_cap);
            println!("11. Total size cap (bytes): {}", app_config.limits.total_size_cap);
            println!("12. Log level: {}", app_config.logging.log_level);
            println!("0. Save and return");
            
            let choice: String = Input::with_theme(&self.theme)
                .with_prompt("Select field to edit (0-12)")
                .interact_text()?;
            
            match choice.trim() {
                "0" => {
                    // Save config and exit
                    match self.config_manager.save_app_config(&app_config) {
                        Ok(()) => {
                            println!("{}", style("Configuration saved successfully!").green());
                        }
                        Err(e) => {
                            println!("{} Failed to save configuration: {}", style("").red(), e);
                        }
                    }
                    break;
                }
                "1" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter new download directory")
                        .with_initial_text(&app_config.paths.download_directory)
                        .interact_text()?;
                    app_config.paths.download_directory = new_value;
                }
                "2" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter new database file path")
                        .with_initial_text(&app_config.paths.database_file)
                        .interact_text()?;
                    app_config.paths.database_file = new_value;
                }
                "3" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter new log directory")
                        .with_initial_text(&app_config.paths.log_directory)
                        .interact_text()?;
                    app_config.paths.log_directory = new_value;
                }
                "4" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter new temp directory")
                        .with_initial_text(&app_config.paths.temp_directory)
                        .interact_text()?;
                    app_config.paths.temp_directory = new_value;
                }
                "5" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter max download concurrency (1-32)")
                        .with_initial_text(&app_config.pools.max_download_concurrency.to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 && val <= 32 => app_config.pools.max_download_concurrency = val,
                        _ => println!("{}", style("Invalid value. Must be between 1-32.").red())
                    }
                }
                "6" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter max API concurrency (1-16)")
                        .with_initial_text(&app_config.pools.max_api_concurrency.to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 && val <= 16 => app_config.pools.max_api_concurrency = val,
                        _ => println!("{}", style("Invalid value. Must be between 1-16.").red())
                    }
                }
                "7" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter batch size (1-50)")
                        .with_initial_text(&app_config.pools.batch_size.to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 && val <= 50 => app_config.pools.batch_size = val,
                        _ => println!("{}", style("Invalid value. Must be between 1-50.").red())
                    }
                }
                "8" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter posts per page (1-320)")
                        .with_initial_text(&app_config.limits.posts_per_page.to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 && val <= 320 => app_config.limits.posts_per_page = val,
                        _ => println!("{}", style("Invalid value. Must be between 1-320.").red())
                    }
                }
                "9" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter max page number (1-1000)")
                        .with_initial_text(&app_config.limits.max_page_number.to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 && val <= 1000 => app_config.limits.max_page_number = val,
                        _ => println!("{}", style("Invalid value. Must be between 1-1000.").red())
                    }
                }
                "10" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter file size cap in MB")
                        .with_initial_text(&(app_config.limits.file_size_cap / 1_048_576).to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 => app_config.limits.file_size_cap = val * 1_048_576,
                        _ => println!("{}", style("Invalid value. Must be positive number in MB.").red())
                    }
                }
                "11" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter total size cap in MB")
                        .with_initial_text(&(app_config.limits.total_size_cap / 1_048_576).to_string())
                        .interact_text()?;
                    match new_value.parse::<usize>() {
                        Ok(val) if val > 0 => app_config.limits.total_size_cap = val * 1_048_576,
                        _ => println!("{}", style("Invalid value. Must be positive number in MB.").red())
                    }
                }
                "12" => {
                    let log_levels = vec!["trace", "debug", "info", "warn", "error"];
                    let current_index = log_levels.iter().position(|&x| x == app_config.logging.log_level)
                        .unwrap_or(2); // Default to "info"
                    
                    let selection = Select::with_theme(&self.theme)
                        .with_prompt("Select log level")
                        .default(current_index)
                        .items(&log_levels)
                        .interact()?;
                    
                    app_config.logging.log_level = log_levels[selection].to_string();
                }
                _ => {
                    println!("{}", style("Invalid choice. Please select 0-12.").red());
                }
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit E621 config
    async fn edit_e621_config(&self) -> CliResult<()> {
        println!("\n{}", style("Edit E621 Config").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        loop {
            // Display current config values
            println!("\n{}", style("Current E621 Configuration:").cyan());
            println!("1. Username: {}", e621_config.auth.username);
            println!("2. API Key: {}", if e621_config.auth.api_key == "your_api_key_here" || e621_config.auth.api_key.is_empty() { 
                "Not set" 
            } else { 
                "Set (hidden)" 
            });
            println!("3. Download favorites: {}", e621_config.options.download_favorites);
            println!("4. Safe mode: {}", e621_config.options.safe_mode);
            println!("5. Tags: {:?}", e621_config.query.tags);
            println!("6. Artists: {:?}", e621_config.query.artists);
            println!("7. Pools: {:?}", e621_config.query.pools);
            println!("8. Collections: {:?}", e621_config.query.collections);
            println!("9. Posts: {:?}", e621_config.query.posts);
            println!("0. Save and return");
            
            let choice: String = Input::with_theme(&self.theme)
                .with_prompt("Select field to edit (0-9)")
                .interact_text()?;
            
            match choice.trim() {
                "0" => {
                    // Save config and exit
                    match self.config_manager.save_e621_config(&e621_config) {
                        Ok(()) => {
                            println!("{}", style("Configuration saved successfully!").green());
                        }
                        Err(e) => {
                            println!("{} Failed to save configuration: {}", style("✗").red(), e);
                        }
                    }
                    break;
                }
                "1" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter E621 username")
                        .with_initial_text(&e621_config.auth.username)
                        .interact_text()?;
                    e621_config.auth.username = new_value;
                }
                "2" => {
                    let new_value: String = Input::with_theme(&self.theme)
                        .with_prompt("Enter E621 API key")
                        .interact_text()?;
                    if !new_value.trim().is_empty() {
                        e621_config.auth.api_key = new_value;
                        println!("{}", style("API key updated").green());
                    }
                }
                "3" => {
                    let new_value = Confirm::with_theme(&self.theme)
                        .with_prompt("Download favorites?")
                        .default(e621_config.options.download_favorites)
                        .interact()?;
                    e621_config.options.download_favorites = new_value;
                }
                "4" => {
                    let new_value = Confirm::with_theme(&self.theme)
                        .with_prompt("Enable safe mode?")
                        .default(e621_config.options.safe_mode)
                        .interact()?;
                    e621_config.options.safe_mode = new_value;
                }
                "5" => {
                    println!("\n{}", style("Edit Tags").cyan());
                    println!("Current tags: {:?}", e621_config.query.tags);
                    
                    let sub_choice = Select::with_theme(&self.theme)
                        .with_prompt("What would you like to do?")
                        .items(&["Add tags", "Remove tags", "Clear all tags", "Back"])
                        .interact()?;
                    
                    match sub_choice {
                        0 => { // Add tags
                            let tags_input: String = Input::with_theme(&self.theme)
                                .with_prompt("Enter tags to add (space separated)")
                                .interact_text()?;
                            let new_tags: Vec<String> = tags_input.split_whitespace()
                                .map(|s| s.to_lowercase())
                                .filter(|s| !e621_config.query.tags.contains(s))
                                .collect();
                            e621_config.query.tags.extend(new_tags);
                        }
                        1 => { // Remove tags
                            if !e621_config.query.tags.is_empty() {
                                let selections = MultiSelect::with_theme(&self.theme)
                                    .items(&e621_config.query.tags)
                                    .with_prompt("Select tags to remove")
                                    .interact()?;
                                
                                for index in selections.into_iter().rev() {
                                    e621_config.query.tags.remove(index);
                                }
                            } else {
                                println!("{}", style("No tags to remove.").yellow());
                            }
                        }
                        2 => { // Clear all tags
                            if Confirm::with_theme(&self.theme)
                                .with_prompt("Are you sure you want to clear all tags?")
                                .default(false)
                                .interact()? {
                                e621_config.query.tags.clear();
                                println!("{}", style("All tags cleared.").green());
                            }
                        }
                        _ => {} // Back
                    }
                }
                "6" => {
                    println!("\n{}", style("Edit Artists").cyan());
                    println!("Current artists: {:?}", e621_config.query.artists);
                    
                    let sub_choice = Select::with_theme(&self.theme)
                        .with_prompt("What would you like to do?")
                        .items(&["Add artists", "Remove artists", "Clear all artists", "Back"])
                        .interact()?;
                    
                    match sub_choice {
                        0 => { // Add artists
                            let artists_input: String = Input::with_theme(&self.theme)
                                .with_prompt("Enter artist names to add (space separated)")
                                .interact_text()?;
                            let new_artists: Vec<String> = artists_input.split_whitespace()
                                .map(|s| s.to_lowercase())
                                .filter(|s| !e621_config.query.artists.contains(s))
                                .collect();
                            e621_config.query.artists.extend(new_artists);
                        }
                        1 => { // Remove artists
                            if !e621_config.query.artists.is_empty() {
                                let selections = MultiSelect::with_theme(&self.theme)
                                    .items(&e621_config.query.artists)
                                    .with_prompt("Select artists to remove")
                                    .interact()?;
                                
                                for index in selections.into_iter().rev() {
                                    e621_config.query.artists.remove(index);
                                }
                            } else {
                                println!("{}", style("No artists to remove.").yellow());
                            }
                        }
                        2 => { // Clear all artists
                            if Confirm::with_theme(&self.theme)
                                .with_prompt("Are you sure you want to clear all artists?")
                                .default(false)
                                .interact()? {
                                e621_config.query.artists.clear();
                                println!("{}", style("All artists cleared.").green());
                            }
                        }
                        _ => {} // Back
                    }
                }
                "7" => {
                    println!("\n{}", style("Edit Pools").cyan());
                    println!("Current pools: {:?}", e621_config.query.pools);
                    
                    let sub_choice = Select::with_theme(&self.theme)
                        .with_prompt("What would you like to do?")
                        .items(&["Add pools", "Remove pools", "Clear all pools", "Back"])
                        .interact()?;
                    
                    match sub_choice {
                        0 => { // Add pools
                            let pools_input: String = Input::with_theme(&self.theme)
                                .with_prompt("Enter pool IDs to add (space separated numbers)")
                                .interact_text()?;
                            let new_pools: Vec<usize> = pools_input.split_whitespace()
                                .filter_map(|s| s.parse::<usize>().ok())
                                .filter(|id| !e621_config.query.pools.contains(id))
                                .collect();
                            e621_config.query.pools.extend(new_pools);
                        }
                        1 => { // Remove pools
                            if !e621_config.query.pools.is_empty() {
                                let pool_strings: Vec<String> = e621_config.query.pools.iter()
                                    .map(|p| p.to_string()).collect();
                                let selections = MultiSelect::with_theme(&self.theme)
                                    .items(&pool_strings)
                                    .with_prompt("Select pools to remove")
                                    .interact()?;
                                
                                for index in selections.into_iter().rev() {
                                    e621_config.query.pools.remove(index);
                                }
                            } else {
                                println!("{}", style("No pools to remove.").yellow());
                            }
                        }
                        2 => { // Clear all pools
                            if Confirm::with_theme(&self.theme)
                                .with_prompt("Are you sure you want to clear all pools?")
                                .default(false)
                                .interact()? {
                                e621_config.query.pools.clear();
                                println!("{}", style("All pools cleared.").green());
                            }
                        }
                        _ => {} // Back
                    }
                }
                "8" => {
                    println!("\n{}", style("Edit Collections").cyan());
                    println!("Current collections: {:?}", e621_config.query.collections);
                    
                    let sub_choice = Select::with_theme(&self.theme)
                        .with_prompt("What would you like to do?")
                        .items(&["Add collections", "Remove collections", "Clear all collections", "Back"])
                        .interact()?;
                    
                    match sub_choice {
                        0 => { // Add collections
                            let collections_input: String = Input::with_theme(&self.theme)
                                .with_prompt("Enter collection IDs to add (space separated numbers)")
                                .interact_text()?;
                            let new_collections: Vec<usize> = collections_input.split_whitespace()
                                .filter_map(|s| s.parse::<usize>().ok())
                                .filter(|id| !e621_config.query.collections.contains(id))
                                .collect();
                            e621_config.query.collections.extend(new_collections);
                        }
                        1 => { // Remove collections
                            if !e621_config.query.collections.is_empty() {
                                let collection_strings: Vec<String> = e621_config.query.collections.iter()
                                    .map(|c| c.to_string()).collect();
                                let selections = MultiSelect::with_theme(&self.theme)
                                    .items(&collection_strings)
                                    .with_prompt("Select collections to remove")
                                    .interact()?;
                                
                                for index in selections.into_iter().rev() {
                                    e621_config.query.collections.remove(index);
                                }
                            } else {
                                println!("{}", style("No collections to remove.").yellow());
                            }
                        }
                        2 => { // Clear all collections
                            if Confirm::with_theme(&self.theme)
                                .with_prompt("Are you sure you want to clear all collections?")
                                .default(false)
                                .interact()? {
                                e621_config.query.collections.clear();
                                println!("{}", style("All collections cleared.").green());
                            }
                        }
                        _ => {} // Back
                    }
                }
                "9" => {
                    println!("\n{}", style("Edit Posts").cyan());
                    println!("Current posts: {:?}", e621_config.query.posts);
                    
                    let sub_choice = Select::with_theme(&self.theme)
                        .with_prompt("What would you like to do?")
                        .items(&["Add posts", "Remove posts", "Clear all posts", "Back"])
                        .interact()?;
                    
                    match sub_choice {
                        0 => { // Add posts
                            let posts_input: String = Input::with_theme(&self.theme)
                                .with_prompt("Enter post IDs to add (space separated numbers)")
                                .interact_text()?;
                            let new_posts: Vec<usize> = posts_input.split_whitespace()
                                .filter_map(|s| s.parse::<usize>().ok())
                                .filter(|id| !e621_config.query.posts.contains(id))
                                .collect();
                            e621_config.query.posts.extend(new_posts);
                        }
                        1 => { // Remove posts
                            if !e621_config.query.posts.is_empty() {
                                let post_strings: Vec<String> = e621_config.query.posts.iter()
                                    .map(|p| p.to_string()).collect();
                                let selections = MultiSelect::with_theme(&self.theme)
                                    .items(&post_strings)
                                    .with_prompt("Select posts to remove")
                                    .interact()?;
                                
                                for index in selections.into_iter().rev() {
                                    e621_config.query.posts.remove(index);
                                }
                            } else {
                                println!("{}", style("No posts to remove.").yellow());
                            }
                        }
                        2 => { // Clear all posts
                            if Confirm::with_theme(&self.theme)
                                .with_prompt("Are you sure you want to clear all posts?")
                                .default(false)
                                .interact()? {
                                e621_config.query.posts.clear();
                                println!("{}", style("All posts cleared.").green());
                            }
                        }
                        _ => {} // Back
                    }
                }
                _ => {
                    println!("{}", style("Invalid choice. Please select 0-9.").red());
                }
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit rules config (deprecated)
    async fn edit_rules_config(&self) -> CliResult<()> {
        println!("\n{}", style("Rules Config (Deprecated)").cyan().bold());
        
        println!("{}", style("Local rules configuration has been deprecated!").yellow().bold());
        println!("{}", style("The application now uses your E621 account's blacklist directly.").cyan());
        println!("{}", style("To manage your blacklist:").cyan());
        println!("  1. Log into your E621 account on the website");
        println!("  2. Go to Account → Settings → Blacklisted Tags");
        println!("  3. Configure your blacklist there");
        println!("\n{}", style("Benefits of using E621 API blacklist:").green());
        println!("  Synchronized across all your devices");
        println!("  Always up-to-date with the latest tag changes");
        println!("  Consistent with what you see on the website");
        println!("  No need to manually maintain local rules");
        
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
        
        println!("\n{}", style("Validation results:").cyan());
        
        match app_config_result {
            Ok(_) => println!("{}", style(" Application config is valid").green()),
            Err(e) => println!("{} {}", style(" Application config error:").red(), e),
        }
        
        match e621_config_result {
            Ok(config) => {
                println!("{}", style(" E621 config is valid").green());
                
                // Additional validation for e621 config
                if config.auth.username == "your_username" || config.auth.api_key == "your_api_key_here" {
                    println!("{}", style("  Warning: Default credentials detected").yellow());
                }
                
                if config.query.tags.is_empty() {
                    println!("{}", style("  Warning: No tags defined").yellow());
                }
            },
            Err(e) => println!("{} {}", style(" E621 config error:").red(), e),
        }
        
        println!("{}", style(" Rules config is deprecated - using E621 API blacklist").blue());
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Verify the downloaded archive
    async fn verify_archive(&self) -> CliResult<()> {
        println!("\n{}", style("Verify Archive").cyan().bold());
        
        // Get the app config
        let app_config = match self.config_manager.get_app_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        let download_dir = Path::new(&app_config.paths.download_directory);
        if !download_dir.exists() {
            println!("{} Download directory does not exist: {}", 
                style("").red(), download_dir.display());
            self.press_enter_to_continue()?;
            return Ok(());
        }
        
        println!("Scanning download directory: {}", download_dir.display());
        
        // Initialize verification statistics
        let mut total_files = 0u64;
        let mut verified_files = 0u64;
        let mut corrupted_files = Vec::new();
        let mut missing_files = Vec::new();
        let mut total_size = 0u64;
        
        // Recursively scan the download directory
        match self.scan_directory_for_verification(download_dir, &mut total_files, &mut verified_files, 
            &mut corrupted_files, &mut missing_files, &mut total_size).await {
            Ok(()) => {
                // Display results
                println!("\n{}", style("Verification Results").cyan().bold());
                println!("{} Total files scanned: {}", style("").cyan(), total_files);
                println!("{} Verified files: {}", style("").green(), verified_files);
                println!("{} Total archive size: {:.2} MB", style("").blue(), total_size as f64 / 1_048_576.0);
                
                if !corrupted_files.is_empty() {
                    println!("\n{} {} corrupted files found:", style("").yellow(), corrupted_files.len());
                    for (i, file) in corrupted_files.iter().enumerate().take(10) {
                        println!("  {}: {}", i + 1, file);
                    }
                    if corrupted_files.len() > 10 {
                        println!("  ... and {} more", corrupted_files.len() - 10);
                    }
                }
                
                if !missing_files.is_empty() {
                    println!("\n{} {} missing files detected:", style("").red(), missing_files.len());
                    for (i, file) in missing_files.iter().enumerate().take(10) {
                        println!("  {}: {}", i + 1, file);
                    }
                    if missing_files.len() > 10 {
                        println!("  ... and {} more", missing_files.len() - 10);
                    }
                }
                
                let integrity_pct = if total_files > 0 {
                    (verified_files as f64 / total_files as f64) * 100.0
                } else {
                    100.0
                };
                
                println!("\n{} Archive integrity: {:.1}%", 
                    if integrity_pct >= 95.0 { style("").green() } 
                    else if integrity_pct >= 80.0 { style("").yellow() }
                    else { style("").red() },
                    integrity_pct
                );
                
                if integrity_pct < 100.0 {
                    println!("\n{}", style("Recommendations:").yellow());
                    if !corrupted_files.is_empty() {
                        println!("Re-download corrupted files");
                    }
                    if !missing_files.is_empty() {
                        println!("Check for incomplete downloads");
                    }
                    println!("Run verification again after cleanup");
                }
            }
            Err(e) => {
                println!("{} Verification failed: {}", style("").red(), e);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Ensure E621 credentials are configured, prompt if not
    async fn ensure_e621_credentials(&mut self) -> ConfigResult<()> {
        use crate::v3::{test_config_credentials};
        
        // First, make sure all config files exist
        if let Err(e) = self.config_manager.create_default_configs() {
            println!("{} Failed to create config files: {}", style("").red(), e);
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
                            println!("{} {}", style("").green(), message);
                            return Ok(());
                        }
                        Err(e) => {
                            println!("{} Credential validation failed: {}", style("").yellow(), e);
                            println!("{}", style("You'll need to update your credentials.").yellow());
                        }
                    }
                }
                Err(e) => {
                    println!("{} Failed to load E621 config: {}", style("").red(), e);
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
                    println!("{} {}", style("").green(), message);
                    
                    // Save the credentials
                    match self.save_credentials(&username, &api_key).await {
                        Ok(()) => {
                            println!("{}", style("Credentials saved successfully!").green());
                            return Ok(());
                        }
                        Err(e) => {
                            println!("{} Failed to save credentials: {}", style("").red(), e);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", style("").red(), e);
                    
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
                
                println!("\n{} Found {} matching tags", style("").green(), tag_results.len());
                
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
                            println!("{} {} tags added successfully!", style("").green(), added_count);
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
                
                println!("\n{} Found {} matching artists", style("").green(), artist_results.len());
                
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
                            println!("{} {} artists added successfully!", style("").green(), added_count);
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
                    println!("{} Invalid pool ID: {}", style("").yellow(), pool_str);
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
                    println!("{} Invalid collection ID: {}", style("").yellow(), collection_str);
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
                    println!("{} Invalid post ID: {}", style("").yellow(), post_str);
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
    
    /// Toggle download favorites setting
    async fn toggle_favorites(&self) -> CliResult<()> {
        println!("\n{}", style("Toggle Download Favorites").cyan().bold());
        
        // Get the current e621 config
        let mut e621_config = match self.config_manager.get_e621_config() {
            Ok(config) => config,
            Err(e) => {
                println!("{}: {}", style("Error").red().bold(), e);
                return Ok(());
            }
        };
        
        // Display current status
        let status_text = if e621_config.options.download_favorites {
            "enabled"
        } else {
            "disabled"
        };
        
        println!("{} Download favorites is currently: {}", 
            style("").blue(), 
            if e621_config.options.download_favorites {
                style(status_text).green()
            } else {
                style(status_text).yellow()
            }
        );
        
        // Check if we have valid credentials
        let has_valid_creds = !e621_config.auth.username.is_empty() && 
            !e621_config.auth.api_key.is_empty() &&
            e621_config.auth.username != "your_username" && 
            e621_config.auth.api_key != "your_api_key_here";
        
        if !has_valid_creds {
            println!("{} Note: You need valid E621 credentials to download favorites", 
                style("").yellow());
            println!("   Configure credentials first in the main menu or edit e621.toml");
        }
        
        // Ask if user wants to toggle
        let new_value = Confirm::with_theme(&self.theme)
            .with_prompt(format!("Would you like to {} download favorites?", 
                if e621_config.options.download_favorites { "disable" } else { "enable" }))
            .default(!e621_config.options.download_favorites)
            .interact()?;
        
        if new_value != e621_config.options.download_favorites {
            e621_config.options.download_favorites = new_value;
            
            // Save the updated config
            match self.config_manager.save_e621_config(&e621_config) {
                Ok(()) => {
                    let new_status = if new_value { "enabled" } else { "disabled" };
                    println!("{} Download favorites {}", 
                        style("").green(), 
                        if new_value {
                            style(new_status).green()
                        } else {
                            style(new_status).yellow()
                        }
                    );
                    
                    if new_value && has_valid_creds {
                        println!("   Your favorites will be included in the next download");
                    } else if new_value {
                        println!("   Remember to set up your E621 credentials to download favorites");
                    }
                }
                Err(e) => {
                    println!("{} Failed to save config: {}", style("").red(), e);
                }
            }
        } else {
            println!("{}", style("No changes made.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Helper function to fetch tag search results
    async fn fetch_tag_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<(String, crate::v3::query_planner::TagInfo)>> {

        // Initialize the orchestrator and query planner
        let config_manager = match init_config(&self.config_dir).await {
            Ok(cm) => Arc::new(cm),
            Err(e) => {
                println!("{} Failed to initialize config manager: {}", style("").red(), e);
                return Ok(Vec::new());
            }
        };

        let orchestrator = match init_orchestrator(config_manager.clone()).await {
            Ok(o) => o,
            Err(e) => {
                println!("{} Failed to initialize orchestrator: {}", style("").red(), e);
                return Ok(Vec::new());
            }
        };

        let hash_manager = match init_hash_manager(config_manager.clone()).await {
            Ok(hm) => hm,
            Err(e) => {
                println!("{} Failed to initialize hash manager: {}", style("").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone(), hash_manager).await {
            Ok(qp) => qp,
            Err(e) => {
                println!("{} Failed to initialize query planner: {}", style("").red(), e);
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
                println!("{} Tag search failed: {}", style("").red(), e);
                Ok(Vec::new())
            }
        }
    }

    /// Helper function to fetch artist search results
    async fn fetch_artist_search_results(&self, search_term: &str, max_results: usize) -> CliResult<Vec<(String, crate::v3::query_planner::ArtistInfo)>> {

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

        let hash_manager = match init_hash_manager(config_manager.clone()).await {
            Ok(hm) => hm,
            Err(e) => {
                println!("{} Failed to initialize hash manager: {}", style("✗").red(), e);
                return Ok(Vec::new());
            }
        };

        let query_planner = match init_query_planner(config_manager.clone(), orchestrator.get_job_queue().clone(), hash_manager).await {
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
                println!("{} Artist search failed: {}", style("").red(), e);
                Ok(Vec::new())
            }
        }
    }


    /// Scan directory recursively for verification
    async fn scan_directory_for_verification(
        &self, 
        dir: &Path, 
        total_files: &mut u64,
        verified_files: &mut u64,
        corrupted_files: &mut Vec<String>,
        missing_files: &mut Vec<String>,
        total_size: &mut u64
    ) -> CliResult<()> {
        use std::fs;
        use tokio::fs as async_fs;
        
        let entries = fs::read_dir(dir)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                // Recursively process subdirectories
                Box::pin(self.scan_directory_for_verification(&path, total_files, verified_files, 
                    corrupted_files, missing_files, total_size)).await?;
            } else if path.is_file() {
                // Only process image/video files that might be downloaded content
                if let Some(ext) = path.extension() {
                    let ext_str = ext.to_string_lossy().to_lowercase();
                    match ext_str.as_str() {
                        "jpg" | "jpeg" | "png" | "gif" | "webm" | "mp4" | "webp" | "swf" => {
                            *total_files += 1;
                            
                            // Get file size
                            if let Ok(metadata) = entry.metadata() {
                                *total_size += metadata.len();
                                
                                // Check if file is accessible and not zero-sized
                                if metadata.len() == 0 {
                                    corrupted_files.push(path.display().to_string());
                                    continue;
                                }
                                
                                // Try to read the file to verify it's not corrupted
                                match async_fs::read(&path).await {
                                    Ok(contents) => {
                                        // Basic verification - ensure file has expected magic bytes
                                        let is_valid = match ext_str.as_str() {
                                            "jpg" | "jpeg" => contents.starts_with(&[0xFF, 0xD8, 0xFF]),
                                            "png" => contents.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
                                            "gif" => contents.starts_with(b"GIF87a") || contents.starts_with(b"GIF89a"),
                                            "webm" => contents.len() > 4 && &contents[4..8] == b"webm",
                                            "mp4" => contents.len() > 8 && (&contents[4..8] == b"ftyp" || &contents[4..12] == b"ftypmp41"),
                                            "webp" => contents.starts_with(b"RIFF") && contents.len() > 12 && &contents[8..12] == b"WEBP",
                                            "swf" => contents.starts_with(b"FWS") || contents.starts_with(b"CWS") || contents.starts_with(b"ZWS"),
                                            _ => true, // For unknown formats, assume valid if readable
                                        };
                                        
                                        if is_valid {
                                            *verified_files += 1;
                                        } else {
                                            corrupted_files.push(path.display().to_string());
                                        }
                                    }
                                    Err(_) => {
                                        corrupted_files.push(path.display().to_string());
                                    }
                                }
                            } else {
                                missing_files.push(path.display().to_string());
                            }
                            
                            // Show progress every 100 files
                            if *total_files % 100 == 0 {
                                println!("Processed {} files...", *total_files);
                            }
                        }
                        _ => {} // Skip non-media files
                    }
                }
            }
        }
        
        Ok(())
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
