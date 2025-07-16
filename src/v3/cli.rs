//! Interactive CLI menu system for E621 Downloader
//! Uses dialoguer for user interaction

use std::path::Path;
use std::io;

use dialoguer::{theme::ColorfulTheme, Select, Input, Confirm, MultiSelect};
use console::style;

use crate::v3::{
    AppConfig, E621Config, RulesConfig,
    ConfigManager, ConfigError, ConfigResult, 
    init_config,
};

/// Main menu options
#[derive(Debug, Clone, Copy)]
pub enum MainMenuOption {
    AddQuery,
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
            MainMenuOption::AddQuery,
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
            MainMenuOption::AddQuery => "Add query",
            MainMenuOption::EditQuery => "Edit query",
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
}

impl CliManager {
    /// Create a new CLI manager
    pub async fn new(config_dir: impl AsRef<Path>) -> ConfigResult<Self> {
        let config_manager = init_config(config_dir).await?;
        let theme = ColorfulTheme::default();
        
        Ok(Self {
            config_manager,
            theme,
        })
    }
    
    /// Run the main menu loop
    pub async fn run(&self) -> io::Result<()> {
        loop {
            // Display the main menu
            let selection = self.show_main_menu()?;
            
            match selection {
                MainMenuOption::AddQuery => self.add_query().await?,
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
    fn show_main_menu(&self) -> io::Result<MainMenuOption> {
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
    async fn add_query(&self) -> io::Result<()> {
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
            
            // TODO: Save the updated config back to file
            println!("{}", style("Tags added successfully!").green());
            println!("Current tags: {:?}", e621_config.query.tags);
        } else {
            println!("{}", style("No valid tags entered.").yellow());
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit an existing query
    async fn edit_query(&self) -> io::Result<()> {
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
            
            // TODO: Save the updated config back to file
            println!("{}", style("Tags removed successfully!").green());
            println!("Remaining tags: {:?}", e621_config.query.tags);
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
                
                // TODO: Save the updated config back to file
                println!("{}", style("New tags added successfully!").green());
                println!("Current tags: {:?}", e621_config.query.tags);
            }
        }
        
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Start the download process
    async fn start_download(&self) -> io::Result<()> {
        println!("\n{}", style("Start Download").cyan().bold());
        println!("{}", style("Download functionality not yet implemented.").yellow());
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Edit configuration files
    async fn edit_config(&self) -> io::Result<()> {
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
    async fn edit_app_config(&self) -> io::Result<()> {
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
    async fn edit_e621_config(&self) -> io::Result<()> {
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
    async fn edit_rules_config(&self) -> io::Result<()> {
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
    async fn validate_configs(&self) -> io::Result<()> {
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
    async fn verify_archive(&self) -> io::Result<()> {
        println!("\n{}", style("Verify Archive").cyan().bold());
        println!("{}", style("Archive verification not yet implemented.").yellow());
        self.press_enter_to_continue()?;
        Ok(())
    }
    
    /// Helper function to pause and wait for user to press Enter
    fn press_enter_to_continue(&self) -> io::Result<()> {
        println!("\nPress Enter to continue...");
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;
        Ok(())
    }
}

/// Run the CLI
pub async fn run_cli(config_dir: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
    let cli_manager = CliManager::new(config_dir).await?;
    cli_manager.run().await?;
    Ok(())
}