extern crate log;

use std::env;

use anyhow::Error;
use tokio;

mod program;
mod v3;

fn main() -> Result<(), Error> {
    // Check for command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--cli" => {
                // Run the interactive CLI menu system
                #[tokio::main]
                async fn run_cli_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::cli_example::cli_example().await?;
                    Ok(())
                }

                return run_cli_main();
            },
            "--orchestration" => {
                // Run the orchestration example
                #[tokio::main]
                async fn run_orchestration_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::orchestration_example::orchestration_example().await?;
                    Ok(())
                }

                return run_orchestration_main();
            },
            "--query-planner" => {
                // Run the query planner example
                #[tokio::main]
                async fn run_query_planner_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::query_planner_example::query_planner_example().await?;
                    Ok(())
                }

                return run_query_planner_main();
            },
            "--download-engine" => {
                // Run the download engine example
                #[tokio::main]
                async fn run_download_engine_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::download_engine_example::download_engine_example().await?;
                    Ok(())
                }

                return run_download_engine_main();
            },
            "--file-processor" => {
                // Run the file processor example
                #[tokio::main]
                async fn run_file_processor_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::file_processor_example::file_processor_example().await?;
                    Ok(())
                }

                return run_file_processor_main();
            },
            "--metadata-store" => {
                // Run the metadata store example
                #[tokio::main]
                async fn run_metadata_store_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::metadata_store_example::metadata_store_example().await?;
                    Ok(())
                }

                return run_metadata_store_main();
            },
            "--blacklist-handler" => {
                // Run the blacklist handling example
                #[tokio::main]
                async fn run_blacklist_handling_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::blacklist_handling_example::blacklist_handling_example().await?;
                    Ok(())
                }

                return run_blacklist_handling_main();
            },
            "--disk-verifier" => {
                // Run the disk verifier example
                #[tokio::main]
                async fn run_disk_verifier_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::disk_verifier_example::disk_verifier_example().await?;
                    Ok(())
                }

                return run_disk_verifier_main();
            },
            "--system-monitor" => {
                // Run the system monitor example
                #[tokio::main]
                async fn run_system_monitor_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::system_monitor_example::system_monitor_example().await?;
                    Ok(())
                }

                return run_system_monitor_main();
            },
            "--logger" => {
                // Run the logger example
                #[tokio::main]
                async fn run_logger_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::logger_example::logger_example().await?;
                    Ok(())
                }

                return run_logger_main();
            },
            "--session-manager" => {
                // Run the session manager example
                #[tokio::main]
                async fn run_session_manager_main() -> Result<(), Error> {
                    initialize_logger().await;
                    v3::session_manager_example::session_manager_example().await?;
                    Ok(())
                }

                return run_session_manager_main();
            },
            _ => {
                println!("Unknown argument: {}", args[1]);
                println!("Available arguments:");
                println!("  (no args): Run the interactive CLI menu system (default)");
                println!("  --cli: Run the interactive CLI menu system");
                println!("  --orchestration: Run the orchestration example");
                println!("  --query-planner: Run the query planning example");
                println!("  --download-engine: Run the download engine example");
                println!("  --file-processor: Run the file processor example");
                println!("  --metadata-store: Run the metadata store example");
                println!("  --blacklist-handler: Run the blacklist handling example");
                println!("  --disk-verifier: Run the disk verification and file watching example");
                println!("  --system-monitor: Run the system monitoring and adaptive control example");
                println!("  --logger: Run the structured logging example");
                println!("  --session-manager: Run the session management and graceful shutdown example");
                return Ok(());
            }
        }
    } else {
        // Run the interactive CLI as the default behavior
        #[tokio::main]
        async fn run_default_cli() -> Result<(), Error> {
            initialize_logger().await;
            v3::cli_example::cli_example().await?;
            Ok(())
        }

        return run_default_cli();
    }
}

/// Initializes the logger using the v3 logger system with async support
async fn initialize_logger() {
    use std::sync::Arc;
    use crate::v3::{ConfigManager, Logger};
    
    // Get the current executable directory to use as base path for config
    let exe_path = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            eprintln!("Failed to get executable path: {}", e);
            tracing_subscriber::fmt::init();
            return;
        }
    };
    
    let exe_dir = match exe_path.parent() {
        Some(dir) => dir,
        None => {
            eprintln!("Failed to get executable directory");
            tracing_subscriber::fmt::init();
            return;
        }
    };
    
    // Try to create config manager and logger
    match ConfigManager::new(exe_dir).await {
        Ok(config_manager) => {
            let config_manager = Arc::new(config_manager);
            
            // Get app config to determine log directory and format
            match config_manager.get_app_config() {
                Ok(app_config) => {
                    // Resolve log directory path
                    let log_dir = if std::path::PathBuf::from(&app_config.paths.log_directory).is_absolute() {
                        std::path::PathBuf::from(&app_config.paths.log_directory)
                    } else {
                        exe_dir.join(&app_config.paths.log_directory)
                    };
                    
                    // Create the v3 logger with proper configuration
                    let logger = Logger::new(config_manager.clone(), log_dir, app_config.logging.log_format.clone());
                    
                    if let Err(e) = logger.init() {
                        eprintln!("Failed to initialize v3 logger: {}", e);
                        // Fall back to simple console logging
                        tracing_subscriber::fmt::init();
                    }
                },
                Err(e) => {
                    eprintln!("Failed to get app config: {}", e);
                    // Fall back to simple console logging
                    tracing_subscriber::fmt::init();
                }
            }
        },
        Err(e) => {
            eprintln!("Failed to create config manager: {}", e);
            // Fall back to simple console logging
            tracing_subscriber::fmt::init();
        }
    }
}

/// Sets up graceful shutdown handling to ensure logs are flushed on exit
fn setup_shutdown_handler() -> Result<(), Error> {
    // For now, just ensure the Drop implementation handles flushing
    // Could be extended with signal handlers if needed
    Ok(())
}
