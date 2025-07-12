use std::path::{Path, PathBuf};
use std::fs::{self, File};
use sha2::{Sha512, Digest};
use std::io::Read;
use std::collections::HashSet;
use std::time::Duration;
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use anyhow::{Result, Context, anyhow};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use walkdir::WalkDir;
use memmap2::Mmap;
use hex::encode as hex_encode;
use serde::{Serialize, Deserialize};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use chrono::Utc;
use crate::e621::tui::{ProgressBarBuilder, ProgressStyleBuilder};
use crate::e621::io::file_metadata::{get_file_metadata, file_metadata_changed, FileMeta};
mod walk; // Declare the walk submodule
/// Represents a file entry in the hash database
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashDatabaseEntry {
    /// Filename
    filename: String,
    /// SHA-512 hash of the file (when available)
    hash: Option<String>,
    /// Short URL for the post (e.g., "e621.net/posts/123456")
    short_url: Option<String>,
    /// Post ID from e621
    post_id: Option<i64>,
    /// The date the file was downloaded
    #[serde(default)]
    download_date: Option<String>,
}

/// Stores and manages file hashes in a persistent JSON file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashDatabase {
    /// List of file entries
    entries: Vec<HashDatabaseEntry>,
}

impl HashDatabase {
    /// Load the hash database from the specified file path
    fn load(file_path: &Path) -> Result<Self> {
        if !file_path.exists() {
            // Create an empty database if the file doesn't exist
            return Ok(HashDatabase { entries: Vec::new() });
        }
        
        let content = fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read hash database file: {}", file_path.display()))?;
            
        let database: HashDatabase = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse hash database file: {}", file_path.display()))?;
            
        Ok(database)
    }
    
    /// Save the hash database to the specified file path
    fn save(&self, file_path: &Path) -> Result<()> {
        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Failed to serialize hash database")?;
            
        let parent_dir = file_path.parent().unwrap_or_else(|| Path::new(""));
        fs::create_dir_all(parent_dir)
            .with_context(|| format!("Failed to create directory for hash database: {}", parent_dir.display()))?;
            
        fs::write(file_path, content)
            .with_context(|| format!("Failed to write hash database file: {}", file_path.display()))?;
            
        Ok(())
    }
    
    /// Convert to a HashSet for fast lookups
    fn to_hash_set(&self) -> HashSet<(String, Option<String>)> {
        self.entries.iter()
            .map(|entry| (entry.filename.clone(), entry.hash.clone()))
            .collect()
    }
    
    /// Convert from a HashSet to update the database
    fn from_hash_set(hash_set: &HashSet<(String, Option<String>)>) -> Self {
        let entries = hash_set.iter()
            .map(|(filename, hash)| HashDatabaseEntry {
                filename: filename.clone(),
                hash: hash.clone(),
                short_url: None, // Will be updated when available
                post_id: None, // Will be updated when available
                download_date: None, // Will be updated when available
            })
            .collect();
            
        HashDatabase { entries }
    }
    
    /// Add or update an entry with full metadata
    fn add_or_update_entry(&mut self, filename: String, hash: Option<String>, short_url: Option<String>, post_id: Option<i64>) {
        // Check if entry already exists
        if let Some(existing_entry) = self.entries.iter_mut().find(|e| e.filename == filename) {
            // Update existing entry
            existing_entry.hash = hash;
            existing_entry.short_url = short_url;
            existing_entry.post_id = post_id;
            existing_entry.download_date = Some(chrono::Utc::now().to_rfc3339());
        } else {
            // Create new entry
            let new_entry = HashDatabaseEntry {
                filename,
                hash,
                short_url,
                post_id,
                download_date: Some(chrono::Utc::now().to_rfc3339()),
            };
            self.entries.push(new_entry);
        }
    }
}

/// Manages the directory structure for downloaded content
#[derive(Debug, Clone)]
pub(crate) struct DirectoryManager {
    /// Base directory for all downloads
    root_dir: PathBuf,
    /// Directory for artist content
    artists_dir: PathBuf,
    /// Directory for tag-based content
    tags_dir: PathBuf,
    /// Directory for pool content
    pools_dir: PathBuf,
    /// Set of already downloaded files with optional SHA-512 hashes
    /// Tuple format: (filename, Option<hash>)
    downloaded_files: HashSet<(String, Option<String>)>,
    /// Whether to use strict SHA-512 verification (when available)
    use_strict_verification: bool,
    /// Path to the hash database file
    hash_db_path: PathBuf,
    /// Whether to use simplified folder structure (only Tags)
    simplified_folders: bool,
}

impl DirectoryManager {
    /// Creates a new DirectoryManager with the specified root directory and folder preferences
    pub(crate) fn new(root_dir: &str) -> Result<Self> {
        let root = PathBuf::from(root_dir);
        let artists = root.join("Artists");
        let tags = root.join("Tags");
        let pools = root.join("Pools");
        let hash_db_path = root.join("hash_database.json");
        
        // Use simplified folders by default (only Tags folder)
        let simplified_folders = true;
        
        // Create a manager with empty downloaded files initially
        let mut manager = DirectoryManager {
            root_dir: root.clone(),
            artists_dir: artists.clone(),
            tags_dir: tags.clone(),
            pools_dir: pools.clone(),
            downloaded_files: HashSet::new(),
            use_strict_verification: true, // Enable strict verification by default
            hash_db_path: hash_db_path.clone(),
            simplified_folders,
        };
        
        // Create directory structure first
        manager.create_directory_structure()?;

        // Try to load hash database first
        info!("Looking for existing hash database...");
        let mut loaded_from_db = false;
        
        if hash_db_path.exists() {
            match HashDatabase::load(&hash_db_path) {
                Ok(db) => {
                    let file_count = db.entries.len();
                    info!("Loaded hash database with {} file entries", file_count);
                    manager.downloaded_files = db.to_hash_set();
                    loaded_from_db = true;
                },
                Err(e) => {
                    warn!("Failed to load hash database, will scan directories instead: {}", e);
                }
            }
        } else {
            // Hash database is missing, create and persist it immediately for integrity.
            info!("hash_database.json does not exist; creating new hash database.");
            let db = HashDatabase { entries: vec![] };
            if let Err(e) = db.save(&hash_db_path) {
                error!("Failed to create empty hash database: {}", e);
            }
            manager.downloaded_files = db.to_hash_set();
        }
        
        // Always resync the hash DB with actual downloaded files found
        // This ensures new/deleted files are picked up even if DB was loaded.
        {
            let mut disk_files = HashSet::new();
            let subdirs = [&artists, &tags, &pools];
            // Gather all candidate files up front for correct ETA math
            let mut candidate_files = Vec::new();
            for dir in subdirs.iter() {
                if dir.exists() {
                    for entry in walkdir::WalkDir::new(dir) {
                        if let Ok(e) = entry {
                            let path = e.path();
                            if path.is_file() {
                                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                                    candidate_files.push((filename.to_owned(), path.to_path_buf()));
                                    disk_files.insert(filename.to_owned());
                                }
                            }
                        }
                    }
                }
            }
            let total = candidate_files.len();
            let already_db = manager.downloaded_files.len();
            use std::time::{Instant};
            let bar = ProgressBarBuilder::new(total as u64)
                .style(
                    Self::create_progress_style(
                        "{spinner:.green} DB SCAN: {wide_msg} | {elapsed_precise} | {bar:40.red/blue} {pos}/{len} | {per_sec}",
                        "=>-"
                    )
                )
                .draw_target(ProgressDrawTarget::stderr_with_hz(5))
                .reset()
                .steady_tick(Duration::from_millis(100))
                .build();
            let scan_start = Instant::now();
            bar.set_message(format!("Starting scan… {} files", total));
            // Partition into new files needing hashing and those found in db
            let mut files_to_hash: Vec<(String, PathBuf)> = Vec::new();
            for (filename, path) in &candidate_files {
                if !manager.downloaded_files.iter().any(|(f, _)| f == filename) {
                    files_to_hash.push((filename.clone(), path.clone()));
                }
            }
            let n_new = files_to_hash.len();
            if n_new == 0 {
                bar.finish_with_message(format!(
                    "DB scan complete! No new files. {}/{} files known, skipped hashing.",
                    already_db, total
                ));
            } else {
                use std::sync::atomic::{AtomicUsize, Ordering};
                use rayon::prelude::*;
                let completed = Arc::new(AtomicUsize::new(0));
                let milestone = 500usize;
                let results = Arc::new(Mutex::new(Vec::with_capacity(n_new)));
                let manager_arc = Arc::new(Mutex::new(&mut manager));
                files_to_hash.par_iter().for_each(|(filename, path)| {
                    let hash = match fs::read(path) {
                        Ok(data) => {
                            let mut hasher = Sha512::new();
                            hasher.update(&data);
                            let hash = hasher.finalize();
                            Some(hex::encode(hash))
                        }
                        Err(e) => {
                            warn!("Could not hash file '{}': {}", filename, e);
                            None
                        }
                    };
                    {
                        let mut results_guard = results.lock().unwrap();
                        results_guard.push((filename.clone(), hash));
                    }
                    let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
                    if done == 1 || done % milestone == 0 || done == n_new {
                        // Flush DB with results, thread-safe
                        let mut manager_guard = manager_arc.lock().unwrap();
                        for (f, h) in results.lock().unwrap().drain(..) {
                            manager_guard.downloaded_files.insert((f, h));
                        }
                        let db = HashDatabase::from_hash_set(&manager_guard.downloaded_files);
                        if let Err(e) = db.save(&manager_guard.hash_db_path) {
                            error!("Failed DB flush during milestone: {}", e);
                        }
                        let elapsed = scan_start.elapsed();
                        let per_file = elapsed.as_secs_f64() / (done as f64);
                        let left = n_new.saturating_sub(done);
                        let eta = left as f64 * per_file;
                        let eta_secs = eta.round() as u64;
                        let eta_fmt = format!("{:02}:{:02}:{:02}", eta_secs / 3600, (eta_secs % 3600) / 60, eta_secs % 60);
                        bar.set_message(format!(
                            "New files: {}/{} hashed, {} left, ETA: {}",
                            done, n_new, left, eta_fmt
                        ));
                        if done % (milestone * 2) == 0 || done == n_new {
                            info!("Milestone: {} new files hashed, {} left, ETA: {}",
                                done, left, eta_fmt);
                        }
                    }
                    bar.inc(1);
                });
                // Any remaining (less than a milestone) results
                let mut manager_guard = manager_arc.lock().unwrap();
                for (f, h) in results.lock().unwrap().drain(..) {
                    manager_guard.downloaded_files.insert((f, h));
                }
                bar.finish_with_message(format!(
                    "DB scan complete! {} new files hashed ({} total in DB), Total time: {:.2?}",
                    n_new, manager_guard.downloaded_files.len(), scan_start.elapsed()
                ));
                let db = HashDatabase::from_hash_set(&manager_guard.downloaded_files);
                if let Err(e) = db.save(&manager_guard.hash_db_path) {
                    error!("Failed to persist hash database after scan: {}", e);
                }
            }
            // Remove any DB entry no longer present on disk
            let to_remove: Vec<_> = manager.downloaded_files
                .iter()
                .filter(|(f,_)| !disk_files.contains(f))
                .cloned()
                .collect();
            for r in &to_remove {
                info!("Removing deleted file '{}' from hash DB", r.0);
                manager.downloaded_files.remove(r);
            }
            // Hash any files that are present but missing hash value
            let mut missing_hash: Vec<_> = vec![];
            for (f, h) in manager.downloaded_files.iter() {
                if h.is_none() && disk_files.contains(f) {
                    missing_hash.push(f.clone());
                }
            }
            for f in &missing_hash {
                let mut full_path = None;
                for dir in subdirs.iter() {
                    let cand = dir.join(&f);
                    if cand.exists() { full_path = Some(cand); break; }
                }
                if let Some(path) = full_path {
                    match fs::read(&path) {
                        Ok(data) => {
                            let mut hasher = Sha512::new();
                            hasher.update(&data);
                            let hash = hasher.finalize();
                            let hx = hex::encode(hash);
                            info!("Filling missing hash for: {}", f);
                            manager.downloaded_files.replace((f.to_string(), Some(hx)));
                        },
                        Err(e) => warn!("Could not hash file '{}': {}", f, e)
                    }
                }
            }
            if !to_remove.is_empty() || !missing_hash.is_empty() {
                // Re-save hash DB if any changes (add, remove, fill missing hashes)
                let db = HashDatabase::from_hash_set(&manager.downloaded_files);
                db.save(&hash_db_path)?;
                info!("Hash DB updated to reflect disk state.");
            }
        }
        // If database wasn't loaded, scan directories
        if !loaded_from_db {
            // Create and configure progress bar for the first phase
            let phase1_progress = Arc::new(ProgressBarBuilder::new(0)
                .style(
                      Self::create_progress_style(
                          "{spinner:.green} Phase 1: {wide_msg} | {elapsed_precise} | Found {pos} files",
                          "=>-"
                      )
                )
                .draw_target(ProgressDrawTarget::stderr_with_hz(10))
                .reset()
                .steady_tick(Duration::from_millis(100))
                .build());
                
            info!("Starting optimized two-phase directory scan...");
            
            // Configure thread count for I/O operations
            let io_threads = num_cpus::get().min(8); // Limit to 8 threads for I/O operations
            info!("Phase 1: Fast file enumeration with WalkDir using {} threads...", io_threads);

            // Create optimized thread pool for I/O operations
            let io_threads = num_cpus::get().min(8); // Limit to 8 threads for I/O operations
            let scan_pool = ThreadPoolBuilder::new()
                .num_threads(io_threads)
                .thread_name(|i| format!("scan-worker-{}", i))
                .build()?;

            // First collect all files from directories in parallel using WalkDir
            let all_files: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
            let file_count = Arc::new(AtomicUsize::new(0));

            // Scan all three directories in parallel using the scan pool
            scan_pool.scope(|s| {
                // Scan artists directory
                let files_mutex_clone = Arc::clone(&all_files);
                let count_clone = Arc::clone(&file_count);
                let progress_clone = Arc::clone(&phase1_progress);
                s.spawn(move |_| {
                    Self::fast_scan_directory(
                        &artists, 
                        "Artists", 
                        &progress_clone, 
                        files_mutex_clone, 
                        count_clone
                    );
                });
                
                // Scan tags directory
                let files_mutex_clone = Arc::clone(&all_files);
                let count_clone = Arc::clone(&file_count);
                let progress_clone = Arc::clone(&phase1_progress);
                s.spawn(move |_| {
                    Self::fast_scan_directory(
                        &tags, 
                        "Tags", 
                        &progress_clone, 
                        files_mutex_clone, 
                        count_clone
                    );
                });
                
                // Scan pools directory
                let files_mutex_clone = Arc::clone(&all_files);
                let count_clone = Arc::clone(&file_count);
                let progress_clone = Arc::clone(&phase1_progress);
                s.spawn(move |_| {
                    Self::fast_scan_directory(
                        &pools, 
                        "Pools", 
                        &progress_clone, 
                        files_mutex_clone, 
                        count_clone
                    );
                });
            });

            let total_files = file_count.load(Ordering::SeqCst);
            phase1_progress.set_position(total_files as u64);
            phase1_progress.finish_with_message(format!("Phase 1 complete! Found {} files", total_files));
            // PHASE 2: Calculate hashes for all files
            info!("Phase 2: Calculating SHA-512 hashes for {} files...", total_files);

            // Create and configure progress bar for the second phase
            let phase2_progress = ProgressBarBuilder::new(total_files as u64)
                .style(
                      Self::create_progress_style(
                          "{spinner:.green} Phase 2: {wide_msg} | {elapsed_precise} | {bar:40.cyan/blue} {pos}/{len} | {per_sec}",
                          "=>-"
                      )
                )
                .draw_target(ProgressDrawTarget::stderr_with_hz(10))
                .reset()
                .steady_tick(Duration::from_millis(100))
                .build();

            use std::time::Instant;
            let scan_start = Instant::now();
            phase2_progress.set_message("Calculating hashes...");

            // Dynamically decide thread count, chunk size, and shuffle/size-sort strategy
            let hash_threads = num_cpus::get();

            // Optionally prioritize largest files to maximize core utilization on SSD/NVMe
            let mut all_files_for_hash = {
                let files_guard = all_files.lock().unwrap();
                let mut files_vec = files_guard.clone();
                files_vec.sort_unstable_by_key(|p| {
                    fs::metadata(p).map(|m| -(m.len() as i64)).unwrap_or(0)
                });
                files_vec
            };

            // Dynamic chunk size logic
            let chunk_size = if total_files < 8_000 {
                1000
            } else if total_files < 30_000 {
                2000
            } else {
                (total_files / hash_threads.max(1)).max(1000)
            };

            // Optionally shuffle for IO balancing:
            // use rand::{thread_rng, seq::SliceRandom};
            // all_files_for_hash.shuffle(&mut thread_rng());

            let results = Arc::new(Mutex::new(HashSet::new()));
            let progress_bar = &phase2_progress;
            let total_files = all_files_for_hash.len();
            let mut processed_counter = Arc::new(AtomicUsize::new(0));

            all_files_for_hash.par_chunks(chunk_size).for_each(|chunk| {
                let counter = Arc::clone(&processed_counter);
                let chunk_results: Vec<(String, Option<String>)> = chunk.iter().map(|path| {
                    let file_name = path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let hash = match Self::optimized_calculate_hash(path) {
                        Ok(hash) => Some(hash),
                        Err(e) => {
                            warn!("Failed to calculate hash for {}: {}", path.display(), e);
                            None
                        }
                    };
                    let done = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if done == 1 || done % 20 == 0 || done == total_files {
                        let elapsed = scan_start.elapsed();
                        let per_file = elapsed.as_secs_f64() / done as f64;
                        let left = total_files.saturating_sub(done);
                        let eta = left as f64 * per_file;
                        let eta_secs = eta.round() as u64;
                        let eta_fmt = format!("{:02}:{:02}:{:02}", eta_secs / 3600, (eta_secs % 3600) / 60, eta_secs % 60);
                        progress_bar.set_message(format!(
                            "{}/{} hashed — {} left — ETA {}",
                            done, total_files, left, eta_fmt
                        ));
                    }
                    progress_bar.inc(1);
                    (file_name, hash)
                }).collect();
                let mut results_guard = results.lock().unwrap();
                for result in chunk_results {
                    results_guard.insert(result);
                }
            });
                
            let files = Arc::try_unwrap(results)
                .expect("Failed to unwrap Arc")
                .into_inner()
                .expect("Failed to unwrap Mutex");
            
            let total_time = scan_start.elapsed();
            let speed = if total_time.as_secs() > 0 { files.len() as u64 / total_time.as_secs().max(1) } else { files.len() as u64 };
            phase2_progress.finish_with_message(format!(
                "Phase 2 complete! {}/{} hashed successfully | Total time: {:.2?} | Throughput: {}/s",
                files.iter().filter(|(_, hash)| hash.is_some()).count(),
                files.len(),
                total_time,
                speed,
            ));
                
            // Save the hash database for future use
            let hash_db = HashDatabase::from_hash_set(&files);
            if let Err(e) = hash_db.save(&hash_db_path) {
                warn!("Failed to save hash database: {}", e);
            } else {
                info!("Hash database saved to {}", hash_db_path.display());
            }
            
            // Update the manager with the scanned files
            manager.downloaded_files = files;
        }

        info!("Found {} existing files in downloads directory", manager.downloaded_files.len());

        Ok(manager)
    }

    /// Creates the basic directory structure
    fn create_directory_structure(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir)
            .with_context(|| format!("Failed to create root directory at {:?}", self.root_dir))?;
        
        if self.simplified_folders {
            // Only create Tags directory for simplified structure
            fs::create_dir_all(&self.tags_dir)
                .with_context(|| format!("Failed to create tags directory at {:?}", self.tags_dir))?;
            info!("Using simplified folder structure - only Tags directory will be created");
        } else {
            // Create all directories for legacy structure
            fs::create_dir_all(&self.artists_dir)
                .with_context(|| format!("Failed to create artists directory at {:?}", self.artists_dir))?;
            fs::create_dir_all(&self.tags_dir)
                .with_context(|| format!("Failed to create tags directory at {:?}", self.tags_dir))?;
            fs::create_dir_all(&self.pools_dir)
                .with_context(|| format!("Failed to create pools directory at {:?}", self.pools_dir))?;
            info!("Using full folder structure - Artists, Tags, and Pools directories will be created");
        }
        Ok(())
    }
    /// Enable or disable strict SHA-512 verification
    /// 
    /// When strict verification is enabled, files are only considered matches if:
    /// 1. The filename matches exactly AND
    /// 2. The SHA-512 hash matches (when available)
    ///
    /// This method is kept for future configuration options that may allow users
    pub(crate) fn set_strict_verification(&mut self, enabled: bool) {
        self.use_strict_verification = enabled;
        info!("SHA-512 strict verification {}", if enabled { "enabled" } else { "disabled" });
    }
    
    /// Creates a progress style with error handling
    fn create_progress_style(template_str: &str, progress_chars: &str) -> ProgressStyle {
        // Try to create styled progress bar with template
        match ProgressStyleBuilder::default()
            .template(template_str) {
                Ok(builder) => match builder.progress_chars(progress_chars) {
                    Ok(styled_builder) => styled_builder.build(),
                    Err(e) => {
                        error!("Failed to apply progress chars: {}. Using default style.", e);
                        ProgressStyle::default_bar()
                    }
                },
                Err(e) => {
                    error!("Failed to create progress style: {}. Using default style.", e);
                    ProgressStyle::default_bar()
                }
            }
    }
    
    
    /// Fast recursive directory scanning using WalkDir
    /// This is an optimized method for quickly finding all files in a directory tree
    fn fast_scan_directory(
        dir: &Path, 
        section_name: &str, 
        progress_bar: &Arc<ProgressBar>,
        files_mutex: Arc<Mutex<Vec<PathBuf>>>,
        file_count: Arc<AtomicUsize>
    ) {
        // Check if directory exists
        if !dir.exists() {
            debug!("Directory does not exist: {}", dir.display());
            return;
        }
        
        // Update progress to show which directory we're scanning
        progress_bar.set_message(format!("Scanning {} directory...", section_name));
        
        // Set up custom buffer for WalkDir entries to batch process them
        const BATCH_SIZE: usize = 1000;
        let mut entry_batch = Vec::with_capacity(BATCH_SIZE);
        
        // Create WalkDir iterator with parallel support
        // Use thread-safe iterator with filtering for only files
        let walker = WalkDir::new(dir)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| match e {
                Ok(entry) => Some(entry),
                Err(err) => {
                    warn!("Error accessing path in {}: {}", section_name, err);
                    None
                }
            });
            
        // Process entries in batches for efficiency
        for entry in walker {
            let path = entry.path();
            
            // Skip directories, we only want files
            if path.is_dir() {
                continue;
            }
            
            // Add to batch
            entry_batch.push(path.to_path_buf());
            
            // When batch is full or for last item, process the batch
            if entry_batch.len() >= BATCH_SIZE {
                // Get file names for progress update
                let first_file = entry_batch.first()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("[unnamed]");
                    
                let last_file = entry_batch.last()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("[unnamed]");
                
                // Update progress with batch range
                progress_bar.set_message(format!("{} - Processing files: {} to {}", 
                    section_name, first_file, last_file));
                
                // Add paths to shared vector
                {
                    let batch_len = entry_batch.len();
                    let mut files = files_mutex.lock().unwrap();
                    files.extend(entry_batch.drain(..));
                    
                    // Update the atomic file count and progress
                    let new_count = file_count.fetch_add(batch_len, Ordering::SeqCst) + batch_len;
                    progress_bar.set_position(new_count as u64);
                }
            }
        }
        
        // Process any remaining entries
        if !entry_batch.is_empty() {
            let batch_len = entry_batch.len();
            
            // Update progress
            progress_bar.set_message(format!("{} - Finalizing {} remaining files", 
                section_name, batch_len));
                
            // Add paths to shared vector
            {
                let mut files = files_mutex.lock().unwrap();
                files.extend(entry_batch);
                
                // Update the atomic file count and progress
                let new_count = file_count.fetch_add(batch_len, Ordering::SeqCst) + batch_len;
                progress_bar.set_position(new_count as u64);
            }
        }
        
        // Update progress on completion
        let current_count = file_count.load(Ordering::SeqCst);
        progress_bar.set_message(format!("{} complete - Found {} files", section_name, current_count));
    }
    
    /// Optimized hash calculation using memory mapping for large files and larger buffers
    fn optimized_calculate_hash(file_path: &Path) -> Result<String> {
        const LARGE_FILE_THRESHOLD: u64 = 32 * 1024 * 1024; // 32MB
        
        let file = File::open(file_path)
            .with_context(|| format!("Failed to open file for hashing: {}", file_path.display()))?;
            
        let metadata = file.metadata()
            .with_context(|| format!("Failed to read file metadata: {}", file_path.display()))?;
        
        let file_size = metadata.len();
        
        // For large files, use memory mapping for better performance
        if file_size > LARGE_FILE_THRESHOLD {
            // Use memory mapping for large files
            let mmap = unsafe { Mmap::map(&file) }
                .with_context(|| format!("Failed to memory map file: {}", file_path.display()))?;
            
            let mut hasher = Sha512::new();
            hasher.update(&mmap[..]);
            let hash = hasher.finalize();
            
            Ok(hex::encode(hash))
        } else {
            // For smaller files, use buffered reading with a larger buffer
            let mut hasher = Sha512::new();
            let mut buffer = vec![0; 8 * 1024 * 1024]; // 8MB buffer for better throughput
            let mut reader = &file;
            
            loop {
                let bytes_read = reader.read(&mut buffer)
                    .with_context(|| format!("Failed to read file during hashing: {}", file_path.display()))?;
                
                if bytes_read == 0 {
                    break; // End of file
                }
                
                hasher.update(&buffer[..bytes_read]);
            }
            
            let hash = hasher.finalize();
            Ok(hex_encode(hash))
        }
    }
    
    /// Calculate SHA-512 hash for a file
    fn calculate_sha512(file_path: &Path) -> Result<String> {
        // Open the file
        let mut file = fs::File::open(file_path)
            .with_context(|| format!("Failed to open file for hashing: {}", file_path.display()))?;
        
        // Read the file in chunks and update the hasher
        let mut hasher = Sha512::new();
        let mut buffer = [0; 1024 * 1024]; // 1MB buffer for reading
        
        loop {
            let bytes_read = file.read(&mut buffer)
                .with_context(|| format!("Failed to read file during hashing: {}", file_path.display()))?;
            
            if bytes_read == 0 {
                break; // End of file
            }
            
            hasher.update(&buffer[..bytes_read]);
        }
        
        // Finalize the hash and convert to hex string
        let hash = hasher.finalize();
        Ok(hex::encode(hash))
    }
    /// Count total files in a directory (without calculating hashes)
    fn count_files(dir: &Path) -> Result<usize> {
        let mut count = 0;
        
        if !dir.is_dir() {
            return Ok(0);
        }
        
        let entries = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
            
        for entry in entries {
            let path = entry.path();
            
            if path.is_dir() {
                count += DirectoryManager::count_files(&path)?;
            } else {
                count += 1;
            }
        }
        
        Ok(count)
    }
    
    /// Recursively scans a directory with progress reporting
    fn scan_directory_with_progress(
        dir: &Path, 
        calculate_hashes: bool,
        progress_bar: &ProgressBar,
        section_name: &str
    ) -> Result<HashSet<(String, Option<String>)>> {
        let mut files = HashSet::new();
        
        if !dir.is_dir() {
            return Ok(files);
        }
        
        // Get all entries
        let entries = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
            
        // Process each entry sequentially for better progress reporting
        for entry in entries {
            let path = entry.path();
            
            if path.is_dir() {
                // Update progress for directory
                let dir_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("[unnamed]");
                    
                progress_bar.set_message(format!("{} - Scanning dir: {}", section_name, dir_name));
                
                // Recursively scan subdirectory
                let subdir_files = DirectoryManager::scan_directory_with_progress(
                    &path, calculate_hashes, progress_bar, section_name
                )?;
                
                files.extend(subdir_files);
            } else {
                // Process file
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        // Update progress
                        progress_bar.set_message(format!("{} - Processing: {}", section_name, name_str));
                        
                        if calculate_hashes {
                            // Calculate SHA-512 hash
                            match DirectoryManager::calculate_sha512(&path) {
                                Ok(hash) => {
                                    files.insert((name_str.to_string(), Some(hash)));
                                },
                                Err(e) => {
                                    warn!("Failed to calculate hash for {}: {}", path.display(), e);
                                    files.insert((name_str.to_string(), None));
                                }
                            }
                        } else {
                            // Just record filename
                            files.insert((name_str.to_string(), None));
                        }
                        
                        // Increment progress
                        progress_bar.inc(1);
                    }
                }
            }
        }
        
        Ok(files)
    }
    
    /// Recursively scans a directory and returns a set of files with optional hashes
    /// 
    /// This method is used for parallel scanning without progress reporting.
    /// It's kept as a fallback for non-interactive contexts or as an alternative
    /// to scan_directory_with_progress when a progress bar is not needed.
    fn scan_directory_static(dir: &Path, calculate_hashes: bool) -> Result<HashSet<(String, Option<String>)>> {
        let mut files = HashSet::new();
        
        if !dir.is_dir() {
            return Ok(files); // Return empty set if directory doesn't exist
        }
        
        // Get all file entries in the directory
        let entries: Vec<_> = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .collect();
            
        // Process entries in parallel
        let results: Vec<Result<Vec<(String, Option<String>)>>> = entries.par_iter()
            .map(|entry| {
                let path = entry.path();
                
                if path.is_dir() {
                    // Recursively scan subdirectories
                    match DirectoryManager::scan_directory_static(&path, calculate_hashes) {
                        Ok(subdir_files) => Ok(subdir_files.into_iter().collect()),
                        Err(e) => Err(e)
                    }
                } else {
                    let mut result = Vec::new();
                    
                    if let Some(name) = path.file_name() {
                        if let Some(name_str) = name.to_str() {
                            if calculate_hashes {
                                // Calculate SHA-512 hash if requested
                                match DirectoryManager::calculate_sha512(&path) {
                                    Ok(hash) => {
                                        result.push((name_str.to_string(), Some(hash)));
                                    },
                                    Err(e) => {
                                        warn!("Failed to calculate hash for {}: {}", path.display(), e);
                                        result.push((name_str.to_string(), None));
                                    }
                                }
                            } else {
                                // Just store the filename without hash
                                result.push((name_str.to_string(), None));
                            }
                        }
                    }
                    
                    Ok(result)
                }
            })
            .collect();
            
        // Combine all results
        for result in results {
            match result {
                Ok(items) => {
                    for item in items {
                        files.insert(item);
                    }
                },
                Err(e) => return Err(e)
            }
        }
        
        Ok(files)
    }

    /// Creates or gets the directory for an artist
    pub(crate) fn get_artist_directory(&self, artist_name: &str) -> Result<PathBuf> {
        let artist_dir = self.artists_dir.join(sanitize_filename(artist_name));
        fs::create_dir_all(&artist_dir)
            .with_context(|| format!("Failed to create artist directory at {:?}", artist_dir))?;
        Ok(artist_dir)
    }

    /// Creates or gets the directory for a tag
    pub(crate) fn get_tag_directory(&self, tag_name: &str) -> Result<PathBuf> {
        let tag_dir = self.tags_dir.join(sanitize_filename(tag_name));
        fs::create_dir_all(&tag_dir)
            .with_context(|| format!("Failed to create tag directory at {:?}", tag_dir))?;
        Ok(tag_dir)
    }

    /// Creates or gets the directory for a pool
    pub(crate) fn get_pool_directory(&self, pool_name: &str) -> Result<PathBuf> {
        let pool_dir = self.pools_dir.join(sanitize_filename(pool_name));
        fs::create_dir_all(&pool_dir)
            .with_context(|| format!("Failed to create pool directory at {:?}", pool_dir))?;
        Ok(pool_dir)
    }

    /// Creates an artist subdirectory within a tag or pool directory
    pub(crate) fn create_artist_subdirectory(&self, parent_dir: &Path, artist_name: &str) -> Result<PathBuf> {
        let artist_subdir = parent_dir.join(sanitize_filename(artist_name));
        fs::create_dir_all(&artist_subdir)
            .with_context(|| format!("Failed to create artist subdirectory at {:?}", artist_subdir))?;
        Ok(artist_subdir)
    }
    
    /// Checks if a file exists by hash first, falling back to filename if hash is not available
    pub(crate) fn file_exists_by_hash(&self, hash: &str) -> bool {
        self.downloaded_files.iter().any(|(_, stored_hash)| {
            stored_hash.as_ref().map_or(false, |h| h == hash)
        })
    }
    
    /// Checks if a file exists in any of the organized directories
    pub(crate) fn file_exists(&self, file_name: &str) -> bool {
        self.downloaded_files.iter().any(|(name, _)| name == file_name)
    }
    
    /// Verifies if a file with the given name and hash exists
    /// If strict verification is enabled, looks for hash match
    /// Otherwise falls back to filename-only check
    pub(crate) fn verify_file(&self, file_name: &str, hash: Option<&str>) -> bool {
        if let Some(h) = hash {
            if self.use_strict_verification {
                // In strict mode, the hash MUST match if provided
                return self.downloaded_files.iter().any(|(name, stored_hash)| {
                    (name == file_name) && stored_hash.as_ref().map_or(false, |sh| sh == h)
                });
            } else {
                // Try hash match first, then fall back to filename
                if self.file_exists_by_hash(h) {
                    return true;
                }
            }
        }
        
        // Fall back to filename check when hash is not available or not found
        self.file_exists(file_name)
    }

    /// Adds a file to the tracking set after successful download
    pub(crate) fn mark_file_downloaded(&mut self, file_name: &str) {
        warn!("Adding file '{}' without hash - hash verification will not be possible", file_name);
        self.downloaded_files.insert((file_name.to_string(), None));
        
        // Update the hash database file
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after adding file: {}", e);
            warn!("Failed to update hash database after adding file: {}", e);
        }
    }
    
    /// Adds a file with its SHA-512 hash to the tracking set without updating the database
    /// This is a simplified version to avoid deep recursion during batch operations
    pub(crate) fn mark_file_downloaded_with_hash_simple(&mut self, file_name: &str, hash: String) {
        trace!("Adding file '{}' with SHA-512 hash", file_name);
        self.downloaded_files.insert((file_name.to_string(), Some(hash)));
        // Immediate DB update for atomicity/regression prevention
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after adding file: {}", e);
        }
    }

    /// Explicitly saves the current state of the hash database to disk
    /// This method can be called at the end of session for reconciliation only.
    pub(crate) fn save_hash_database(&self) -> Result<()> {
        trace!("Saving hash database to disk...");
        
        // Create database from current state
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        
        // Attempt to save and handle errors
        hash_db.save(&self.hash_db_path)
            .with_context(|| format!("Failed to save hash database to {}", self.hash_db_path.display()))?;
            
        info!("Successfully saved hash database with {} entries", self.downloaded_files.len());
        Ok(())
    }
    
    /// Adds a file with its SHA-512 hash to the tracking set
    pub(crate) fn mark_file_downloaded_with_hash(&mut self, file_name: &str, hash: String) {
        self.mark_file_downloaded_with_hash_simple(file_name, hash);
        
        // Update the hash database file
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after adding file: {}", e);
        }
    }
    
    /// Adds a file entry with full metadata (URL, post ID, etc.) to the tracking system
    pub(crate) fn add_or_update_entry(&mut self, file_name: String, hash: Option<String>, short_url: Option<String>, post_id: Option<i64>) {
        // Add to the tracking set for compatibility with existing duplicate checking
        self.downloaded_files.insert((file_name.clone(), hash.clone()));
        
        // Load current database, update it, and save
        let mut hash_db = match HashDatabase::load(&self.hash_db_path) {
            Ok(db) => db,
            Err(_) => HashDatabase { entries: Vec::new() }
        };
        
        hash_db.add_or_update_entry(file_name, hash, short_url, post_id);
        
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database with metadata: {}", e);
        }
    }
    /// Checks if a file exists by name or hash
    /// Checks if a file exists by name or hash
    /// This is the preferred method for checking duplicates as it uses hash first when available
    pub(crate) fn is_duplicate(&self, file_name: &str, hash: Option<&str>) -> bool {
        // Check by hash first if available (most reliable)
        if let Some(h) = hash {
            if self.file_exists_by_hash(h) {
                return true;
            }
        }
        
        // Fall back to filename check
        self.file_exists(file_name)
    }
    
    /// An iterative version of is_duplicate that avoids deep recursive calls
    /// This method is optimized to prevent stack overflow during batch operations
    pub(crate) fn is_duplicate_iterative(&self, file_name: &str, hash: Option<&str>) -> bool {
        // Direct iteration over the downloaded_files set to avoid additional function calls
        
        // Check by hash first if available (most reliable)
        if let Some(h) = hash {
            for (_, stored_hash) in &self.downloaded_files {
                if let Some(stored) = stored_hash {
                    if stored == h {
                        return true;
                    }
                }
            }
        }
        
        // Fall back to filename check with direct iteration
        for (name, _) in &self.downloaded_files {
            if name == file_name {
                return true;
            }
        }
        
        false
    }
    /// Calculates the SHA-512 hash for a given file path
    /// 
    /// This is a convenience wrapper around the static calculate_sha512 method
    /// that maintains the same error handling and result type.
    /// Kept for future batch verification and file integrity checking features.
    pub(crate) fn calculate_hash_for_file(&self, file_path: &Path) -> Result<String> {
        DirectoryManager::calculate_sha512(file_path)
    }
    
    /// Scan and update hashes for all files that don't have them yet
    ///
    /// This method scans all directories to find files that are tracked but don't have hashes yet.
    /// When found, it calculates their SHA-512 hash and updates the tracking information.
    ///
    /// Returns the number of files that were successfully updated with hashes.
    /// 
    /// Kept for future manual verification features, file recovery tools,
    /// and database maintenance operations.
    pub(crate) fn update_missing_hashes(&mut self) -> Result<usize> {
        let mut updated_count = 0;
        let mut to_add = HashSet::new();
        let mut to_remove = HashSet::new();
        
        // Find files without hashes
        let files_without_hash: Vec<_> = self.downloaded_files
            .iter()
            .filter(|(_, hash)| hash.is_none())
            .map(|(name, _)| name.clone())
            .collect();
            
        if files_without_hash.is_empty() {
            return Ok(0);
        }
        
        info!("Calculating hashes for {} existing files", files_without_hash.len());
        
        // For each directory, look for matching files and calculate hashes
        let dirs = [&self.artists_dir, &self.tags_dir, &self.pools_dir];
        
        for dir in dirs {
            for file_name in &files_without_hash {
                // Find all instances of this file in the directory
                let matching_files = self.find_files_by_name(dir, file_name)?;
                
                for file_path in matching_files {
                    match self.calculate_hash_for_file(&file_path) {
                        Ok(hash) => {
                            to_add.insert((file_name.clone(), Some(hash)));
                            to_remove.insert((file_name.clone(), None));
                            updated_count += 1;
                            break; // Found and hashed one instance of this file
                        },
                        Err(e) => {
                            warn!("Failed to calculate hash for {}: {}", file_path.display(), e);
                        }
                    }
                }
            }
        }
        
        // Update the set - remove entries without hash and add entries with hash
        for item in to_remove {
            self.downloaded_files.remove(&item);
        }
        
        for item in to_add {
            self.downloaded_files.insert(item);
        }
        
        // Update the hash database after changes
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after updating hashes: {}", e);
        } else {
            info!("Updated hash database with {} newly calculated hashes", updated_count);
        }
        
        Ok(updated_count)
    }

    /// Find all files with the given name in the directory (recursively)
    /// 
    /// Searches through the directory tree to find all files matching the given name.
    /// This is used for hash calculation of existing files and file recovery operations.
    ///
    /// Kept for supporting the update_missing_hashes method and future file verification tools.
    fn find_files_by_name(&self, dir: &Path, file_name: &str) -> Result<Vec<PathBuf>> {
        let mut matching_files = Vec::new();
        
        if !dir.is_dir() {
            return Ok(matching_files);
        }
        
        // Get all entries in the directory
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => return Err(anyhow!("Failed to read directory {}: {}", dir.display(), e))
        };
            
        // Process each entry
        for entry_result in entries {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(e) => {
                    warn!("Failed to read directory entry: {}", e);
                    continue;
                }
            };
            
            let path = entry.path();
            
            if path.is_dir() {
                // Recursively search subdirectories
                if let Ok(mut subdir_files) = self.find_files_by_name(&path, file_name) {
                    matching_files.append(&mut subdir_files);
                }
            } else {
                // Check if this file matches the requested name
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        if name_str == file_name {
                            matching_files.push(path);
                        }
                    }
                }
            }
        }
        
        Ok(matching_files)
    }
}

/// Sanitizes a filename to be safe for use in file systems
fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => c
        })
        .collect()
}
