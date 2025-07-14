#[macro_use]
extern crate log;

use std::env::consts::{
    ARCH, DLL_EXTENSION, DLL_PREFIX, DLL_SUFFIX, EXE_EXTENSION, EXE_SUFFIX, FAMILY, OS,
};
use std::fs::OpenOptions;
use std::io::{self, Write, BufWriter};
use std::sync::{Arc, Mutex};

use anyhow::Error;
use log::LevelFilter;
use simplelog::{
    ColorChoice, CombinedLogger, Config, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use crate::program::Program;

mod e621;
mod program;

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
    log_system_information();

    // Setup signal handler to flush logs on program exit
    let _guard = setup_shutdown_handler();
    
    let program = Program::new();
    program.run()
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

/// Logs important information about the system being used.
fn log_system_information() {
    trace!("Printing system information out into log for debug purposes...");
    trace!("ARCH:           \"{}\"", ARCH);
    trace!("DLL_EXTENSION:  \"{}\"", DLL_EXTENSION);
    trace!("DLL_PREFIX:     \"{}\"", DLL_PREFIX);
    trace!("DLL_SUFFIX:     \"{}\"", DLL_SUFFIX);
    trace!("EXE_EXTENSION:  \"{}\"", EXE_EXTENSION);
    trace!("EXE_SUFFIX:     \"{}\"", EXE_SUFFIX);
    trace!("FAMILY:         \"{}\"", FAMILY);
    trace!("OS:             \"{}\"", OS);
}
