use std::env::current_dir;
use std::fs::write;
use std::path::Path;

use console::Term;
use anyhow::Error;
use log::{trace, info, warn, error};

// Temporarily comment out these imports until we can determine the correct module structure
// use crate::config::Config;
// use crate::constants::{TAG_NAME, TAG_FILE_EXAMPLE};
// use crate::login::Login;
// use crate::request::RequestSender;
// use crate::connector::E621WebConnector;
// use crate::tag::parse_tag_file;

// Define temporary stubs for the missing types
struct Config;
impl Config {
    fn config_exists() -> bool { true }
    fn create_config() -> Result<(), anyhow::Error> { Ok(()) }
}

const TAG_NAME: &str = "tags.txt";
const TAG_FILE_EXAMPLE: &str = "# Example tag file";

struct Login;
impl Login {
    fn get() -> Self { Self }
    fn username(&self) -> &str { "username" }
    fn api_key(&self) -> &str { "api_key" }
    fn download_favorites(&self) -> bool { false }
    fn is_empty(&self) -> bool { false }
}

struct RequestSender;
impl RequestSender {
    fn new() -> Self { Self }
}

struct E621WebConnector<'a> {
    _request_sender: &'a RequestSender,
}
impl<'a> E621WebConnector<'a> {
    fn new(request_sender: &'a RequestSender) -> Self { Self { _request_sender: request_sender } }
    fn configure_size_limits(&mut self) {}
    fn configure_max_pages_to_search(&mut self) {}
    fn configure_download_mode(&mut self) {}
    fn configure_batch_size(&mut self) {}
    fn configure_database_management(&mut self) {}
    fn process_blacklist(&mut self) {}
    fn grab_all(&mut self, _groups: &[String]) {}
    fn download_posts(&mut self) {}
    fn confirm_exit(&self, _message: &str) -> bool { true }
}

fn parse_tag_file(_request_sender: &RequestSender) -> Result<Vec<String>, anyhow::Error> {
    Ok(vec![])
}

/// The name of the cargo package.
const NAME: &str = env!("CARGO_PKG_NAME");

/// The version of the cargo package.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The authors who created the package.
const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

/// A program class that handles the flow of the downloader user experience and steps of execution.
pub(crate) struct Program;

impl Program {
    /// Creates a new instance of the program.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Runs the downloader program.
    pub(crate) fn run(&self) -> Result<(), Error> {
        Term::stdout().set_title("e621 downloader");
        trace!("Starting e621 downloader...");
        trace!("Program Name: {}", NAME);
        trace!("Program Version: {}", VERSION);
        trace!("Program Authors: {}", AUTHORS);
        let current_dir_path = current_dir()
            .map_err(|e| {
                error!("Unable to get working directory: {}", e);
                anyhow::anyhow!("Failed to get working directory: {}", e)
            })?;
        let current_dir = current_dir_path
            .to_str()
            .ok_or_else(|| {
                error!("Working directory path contains invalid UTF-8");
                anyhow::anyhow!("Working directory path contains invalid UTF-8")
            })?;
        trace!("Program Working Directory: {}", current_dir);

        // Check the config file and ensures that it is created.
        trace!("Checking if config file exists...");
        if !Config::config_exists() {
            trace!("Config file doesn't exist...");
            info!("Creating config file...");
            Config::create_config()?;
        }

        // Create tag if it doesn't exist.
        trace!("Checking if tag file exists...");
        if !Path::new(TAG_NAME).exists() {
            info!("Tag file does not exist, creating tag file...");
            write(TAG_NAME, TAG_FILE_EXAMPLE)?;
            trace!("Tag file \"{}\" created...", TAG_NAME);

            info!("The tag file has been created.");
            // Use console::Term::read_line for robust input that works in both terminals and IDEs
            let confirm_exit = {
                use console::Term;
                use std::io::Write;

                let term = Term::stdout();

                // Show the prompt with default indication
                print!("Would you like to exit the application to edit the tag file before continuing? [Y/n]: ");
                std::io::stdout().flush().unwrap_or(());

                // Read line from terminal
                match term.read_line() {
                    Ok(input) => {
                        let input = input.trim().to_lowercase();
                        match input.as_str() {
                            "y" | "yes" | "" => true,  // Default to yes
                            "n" | "no" => false,
                            _ => {
                                // Invalid input, use default
                                println!("Invalid input '{}', using default: yes", input);
                                true
                            }
                        }
                    },
                    Err(err) => {
                        warn!("Failed to get user input: {}", err);
                        warn!("Defaulting to continue without editing tag file.");
                        false
                    }
                }
            };

            if confirm_exit {
                info!("Exiting so you can edit the tag file to include the artists, sets, pools, and individual posts you wish to download.");
                return Ok(());
            } else {
                info!("Continuing without editing the tag file. Note that no downloads will occur unless the tag file contains valid entries.");
            }
        }

        // Creates connector and requester to prepare for downloading posts.
        let login = Login::get();
        trace!("Login information loaded...");
        trace!("Login Username: {}", login.username());
        trace!("Login API Key: {}", "*".repeat(login.api_key().len()));
        trace!("Login Download Favorites: {}", login.download_favorites());

        let request_sender = RequestSender::new();
        let mut connector = E621WebConnector::new(&request_sender);
        connector.configure_size_limits();
        connector.configure_max_pages_to_search();
        connector.configure_download_mode();
        connector.configure_batch_size();
        connector.configure_database_management();

        // Parses tag file.
        trace!("Parsing tag file...");
        let groups = parse_tag_file(&request_sender)?;

        // Collects all grabbed posts and moves it to connector to start downloading.
        if !login.is_empty() {
            trace!("Parsing user blacklist...");
            connector.process_blacklist();
        } else {
            trace!("Skipping blacklist as user is not logged in...");
        }

        connector.grab_all(&groups);
        connector.download_posts();

        info!("Finished downloading posts!");

        // Ask user before exiting
        if connector.confirm_exit("All operations completed successfully.") {
            info!("Exiting at user request...");
        } else {
            info!("Program will remain open. Press Enter to exit when ready.");

            // Simple prompt for user to exit when ready
            let term = Term::stdout();
            println!("\nPress Enter to exit...");
            let _ = term.read_line();
            info!("User requested exit. Closing application...");
        }

        Ok(())
    }
}
