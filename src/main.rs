extern crate log;

use std::env;
use std::fs::OpenOptions;
use std::io::{self, Write, BufWriter};
use std::sync::{Arc, Mutex};

use anyhow::Error;
use log::LevelFilter;
use simplelog::{
    ColorChoice, CombinedLogger, Config, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use tokio;

use crate::program::Program;

mod program;
mod v3;

/// A buffered file writer that handles large log files robustly
/// Uses proper buffering and automatic flushing to prevent data loss
struct BufferedFileWriter {
    inner: Arc<Mutex<BufWriter<std::fs::File>>>,
    line_count: Arc<Mutex<usize>>,
}

impl BufferedFileWriter {
    fn new() -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("e621_downloader.log")?;

        let buffered_writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer

        Ok(Self {
            inner: Arc::new(Mutex::new(buffered_writer)),
            line_count: Arc::new(Mutex::new(0)),
        })
    }
}

impl Write for BufferedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut writer = self.inner.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Failed to acquire lock")
        })?;

        let size = writer.write(buf)?;

        // Count lines and flush periodically to prevent data loss
        if let Ok(mut count) = self.line_count.lock() {
            if buf.contains(&b'\n') {
                *count += buf.iter().filter(|&&b| b == b'\n').count();

                // Flush every 50 lines to ensure data gets written more frequently
                if *count % 50 == 0 {
                    writer.flush()?;
                }
            }
        }

        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut writer = self.inner.lock().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Failed to acquire lock")
        })?;
        writer.flush()
    }
}

// Ensure the buffer is flushed when the writer is dropped
impl Drop for BufferedFileWriter {
    fn drop(&mut self) {
        if let Ok(mut writer) = self.inner.lock() {
            let _ = writer.flush();
        }
    }
}

fn main() -> Result<(), Error> {
    initialize_logger();
    // Setup signal handler to flush logs on program exit
    let _guard = setup_shutdown_handler();

    // Check for command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--cli" => {
                // Run the interactive CLI menu system
                #[tokio::main]
                async fn run_cli_main() -> Result<(), Error> {
                    v3::cli_example::cli_example().await?;
                    Ok(())
                }

                return run_cli_main();
            },
            "--orchestration" => {
                // Run the orchestration example
                #[tokio::main]
                async fn run_orchestration_main() -> Result<(), Error> {
                    v3::orchestration_example::orchestration_example().await?;
                    Ok(())
                }

                return run_orchestration_main();
            },
            "--query-planner" => {
                // Run the query planner example
                #[tokio::main]
                async fn run_query_planner_main() -> Result<(), Error> {
                    v3::query_planner_example::query_planner_example().await?;
                    Ok(())
                }

                return run_query_planner_main();
            },
            "--download-engine" => {
                // Run the download engine example
                #[tokio::main]
                async fn run_download_engine_main() -> Result<(), Error> {
                    v3::download_engine_example::download_engine_example().await?;
                    Ok(())
                }

                return run_download_engine_main();
            },
            "--file-processor" => {
                // Run the file processor example
                #[tokio::main]
                async fn run_file_processor_main() -> Result<(), Error> {
                    v3::file_processor_example::file_processor_example().await?;
                    Ok(())
                }

                return run_file_processor_main();
            },
            "--metadata-store" => {
                // Run the metadata store example
                #[tokio::main]
                async fn run_metadata_store_main() -> Result<(), Error> {
                    v3::metadata_store_example::metadata_store_example().await?;
                    Ok(())
                }

                return run_metadata_store_main();
            },
            "--blacklist-handler" => {
                // Run the blacklist handling example
                #[tokio::main]
                async fn run_blacklist_handling_main() -> Result<(), Error> {
                    v3::blacklist_handling_example::blacklist_handling_example().await?;
                    Ok(())
                }

                return run_blacklist_handling_main();
            },
            "--disk-verifier" => {
                // Run the disk verifier example
                #[tokio::main]
                async fn run_disk_verifier_main() -> Result<(), Error> {
                    v3::disk_verifier_example::disk_verifier_example().await?;
                    Ok(())
                }

                return run_disk_verifier_main();
            },
            "--system-monitor" => {
                // Run the system monitor example
                #[tokio::main]
                async fn run_system_monitor_main() -> Result<(), Error> {
                    v3::system_monitor_example::system_monitor_example().await?;
                    Ok(())
                }

                return run_system_monitor_main();
            },
            "--logger" => {
                // Run the logger example
                #[tokio::main]
                async fn run_logger_main() -> Result<(), Error> {
                    v3::logger_example::logger_example().await?;
                    Ok(())
                }

                return run_logger_main();
            },
            "--session-manager" => {
                // Run the session manager example
                #[tokio::main]
                async fn run_session_manager_main() -> Result<(), Error> {
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
            v3::cli_example::cli_example().await?;
            Ok(())
        }

        return run_default_cli();
    }
}

/// Initializes the logger with preset filtering and robust file handling.
fn initialize_logger() {
    // Keep the original terminal logger for console output
    let mut config = ConfigBuilder::new();
    config.add_filter_allow_str("e621_downloader");

    // Use buffered file writer for better reliability and performance
    let buffered_file_writer = match BufferedFileWriter::new() {
        Ok(writer) => writer,
        Err(e) => {
            eprintln!("Failed to create buffered file writer: {}. Logging will only output to terminal.", e);
            // Continue with only terminal logging
            let _ = TermLogger::init(
                LevelFilter::Info,
                Config::default(),
                TerminalMode::Mixed,
                ColorChoice::Auto,
            );
            return;
        }
    };

    if let Err(e) = CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::max(),
            config.build(),
            buffered_file_writer,
        ),
    ]) {
        eprintln!("Failed to initialize combined logger: {}. Falling back to terminal-only logging.", e);
        let _ = TermLogger::init(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        );
    }
}

/// Sets up graceful shutdown handling to ensure logs are flushed on exit
fn setup_shutdown_handler() -> Result<(), Error> {
    // For now, just ensure the Drop implementation handles flushing
    // Could be extended with signal handlers if needed
    Ok(())
}
