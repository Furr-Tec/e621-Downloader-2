use std::path::{Path, PathBuf};
use std::fs;
use std::io::Read;
use std::collections::HashSet;
use std::time::Duration;
use anyhow::{Result, Context, anyhow};
use rayon::prelude::*;
use sha2::{Sha512, Digest};
use hex::encode as hex_encode;
use serde::{Serialize, Deserialize};
use indicatif::{ProgressBar, ProgressDrawTarget};
use crate::e621::tui::{ProgressBarBuilder, ProgressStyleBuilder};

/// Represents a file entry in the hash database
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashDatabaseEntry {
    /// Filename
    filename: String,
    /// SHA-512 hash of the file (when available)
    hash: Option<String>,
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
            })
            .collect();
            
        HashDatabase { entries }
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
}

impl DirectoryManager {
    /// Creates a new DirectoryManager with the specified root directory
    pub(crate) fn new(root_dir: &str) -> Result<Self> {
        let root = PathBuf::from(root_dir);
        let artists = root.join("Artists");
        let tags = root.join("Tags");
        let pools = root.join("Pools");
        let hash_db_path = root.join("hash_database.json");
        
        // Create a manager with empty downloaded files initially
        let mut manager = DirectoryManager {
            root_dir: root.clone(),
            artists_dir: artists.clone(),
            tags_dir: tags.clone(),
            pools_dir: pools.clone(),
            downloaded_files: HashSet::new(),
            use_strict_verification: true, // Enable strict verification by default
            hash_db_path: hash_db_path.clone(),
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
        }
        
        // If database wasn't loaded, scan directories
        if !loaded_from_db {
            // Create and configure progress bar
            let progress_bar = ProgressBarBuilder::new(0)
                .style(
                    ProgressStyleBuilder::default()
                        .template("{spinner:.green} Scanning: {wide_msg} | {elapsed_precise} | Found {pos} files")
                        .progress_chars("=>-")
                        .build()
                )
                .draw_target(ProgressDrawTarget::stderr_with_hz(10))
                .reset()
                .steady_tick(Duration::from_millis(100))
                .build();
                
            progress_bar.set_message("Initializing file scan...");
            
            // Scan directories for existing files in parallel
            let mut files = HashSet::new();
            
            info!("Scanning existing files and calculating hashes...");
            progress_bar.set_message("Counting files to process...");
            
            // First count total files for the progress bar (quick scan without hash calculation)
            let total_files = rayon::join(
                || DirectoryManager::count_files(&artists),
                || {
                    rayon::join(
                        || DirectoryManager::count_files(&tags),
                        || DirectoryManager::count_files(&pools)
                    )
                }
            );
            
            let artist_count = total_files.0?;
            let (tags_count, pools_count) = total_files.1;
            let tags_count = tags_count?;
            let pools_count = pools_count?;
            let total_count = artist_count + tags_count + pools_count;
            
            // Update progress bar with total
            progress_bar.set_length(total_count as u64);
            progress_bar.set_position(0);
            progress_bar.set_message("Starting deep scan with hash calculation...");
            
            // Now scan with hash calculation, using the progress bar
            let scan_result = rayon::join(
                || DirectoryManager::scan_directory_with_progress(&artists, true, &progress_bar, "Artists directory"),
                || {
                    rayon::join(
                        || DirectoryManager::scan_directory_with_progress(&tags, true, &progress_bar, "Tags directory"),
                        || DirectoryManager::scan_directory_with_progress(&pools, true, &progress_bar, "Pools directory")
                    )
                }
            );

            // Combine results from parallel scans
            let artists_files = scan_result.0?;
            let (tags_files, pools_files) = scan_result.1;
            let tags_files = tags_files?;
            let pools_files = pools_files?;

            // Merge all file sets
            files.extend(artists_files);
            files.extend(tags_files);
            files.extend(pools_files);
            
            progress_bar.finish_with_message(format!("Scan complete! Found {} files with {} having hashes", 
                files.len(), 
                files.iter().filter(|(_, hash)| hash.is_some()).count()));
                
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
        fs::create_dir_all(&self.artists_dir)
            .with_context(|| format!("Failed to create artists directory at {:?}", self.artists_dir))?;
        fs::create_dir_all(&self.tags_dir)
            .with_context(|| format!("Failed to create tags directory at {:?}", self.tags_dir))?;
        fs::create_dir_all(&self.pools_dir)
            .with_context(|| format!("Failed to create pools directory at {:?}", self.pools_dir))?;
        Ok(())
    }
    /// Enable or disable strict SHA-512 verification
    /// 
    /// When strict verification is enabled, files are only considered matches if:
    /// 1. The filename matches exactly AND
    /// 2. The SHA-512 hash matches (when available)
    ///
    /// This method is kept for future configuration options that may allow users
    /// to toggle verification strictness.
    pub(crate) fn set_strict_verification(&mut self, enabled: bool) {
        self.use_strict_verification = enabled;
        info!("SHA-512 strict verification {}", if enabled { "enabled" } else { "disabled" });
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
        Ok(hex_encode(hash))
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
    /// Adds a file to the tracking set after successful download
    /// Note: This method is deprecated and should only be used when hash calculation fails
    pub(crate) fn mark_file_downloaded(&mut self, file_name: &str) {
        warn!("Adding file '{}' without hash - hash verification will not be possible", file_name);
        self.downloaded_files.insert((file_name.to_string(), None));
        
        // Update the hash database file
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after adding file: {}", e);
        }
    }
    /// Adds a file with its SHA-512 hash to the tracking set
    /// This is the preferred method for tracking downloaded files
    pub(crate) fn mark_file_downloaded_with_hash(&mut self, file_name: &str, hash: String) {
        trace!("Adding file '{}' with SHA-512 hash", file_name);
        self.downloaded_files.insert((file_name.to_string(), Some(hash)));
        
        // Update the hash database file
        let hash_db = HashDatabase::from_hash_set(&self.downloaded_files);
        if let Err(e) = hash_db.save(&self.hash_db_path) {
            warn!("Failed to update hash database after adding file: {}", e);
        }
    }
    
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
