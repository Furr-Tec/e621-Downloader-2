use std::path::{Path, PathBuf};
use std::fs;
use std::io::Read;
use std::collections::HashSet;
use anyhow::{Result, Context, anyhow};
use rayon::prelude::*;
use sha2::{Sha512, Digest};
use hex::encode as hex_encode;

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
}

impl DirectoryManager {
    /// Creates a new DirectoryManager with the specified root directory
    pub(crate) fn new(root_dir: &str) -> Result<Self> {
        let root = PathBuf::from(root_dir);
        let artists = root.join("Artists");
        let tags = root.join("Tags");
        let pools = root.join("Pools");

        let manager = DirectoryManager {
            root_dir: root.clone(),
            artists_dir: artists.clone(),
            tags_dir: tags.clone(),
            pools_dir: pools.clone(),
            downloaded_files: HashSet::new(),
            use_strict_verification: true, // Enable strict verification by default
        };

        manager.create_directory_structure()?;

        // Scan directories for existing files in parallel
        let mut files = HashSet::new();
        
        info!("Scanning existing files and calculating hashes...");
        
        // Scan all three directories in parallel 
        let scan_result = rayon::join(
            || DirectoryManager::scan_directory_static(&artists, false),
            || {
                rayon::join(
                    || DirectoryManager::scan_directory_static(&tags, false),
                    || DirectoryManager::scan_directory_static(&pools, false)
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

        info!("Found {} existing files in downloads directory", files.len());

        Ok(DirectoryManager {
            root_dir: root,
            artists_dir: artists,
            tags_dir: tags,
            pools_dir: pools,
            downloaded_files: files,
            use_strict_verification: true,
        })
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

    /// Recursively scans a directory and returns a set of files with optional hashes
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
    /// Checks if a file exists in any of the organized directories
    pub(crate) fn file_exists(&self, file_name: &str) -> bool {
        self.downloaded_files.iter().any(|(name, _)| name == file_name)
    }
    
    /// Verifies if a file with the given name and hash exists
    /// If strict verification is disabled, falls back to filename-only check
    pub(crate) fn verify_file(&self, file_name: &str, hash: Option<&str>) -> bool {
        // If we don't have a hash or strict verification is disabled, just check the filename
        if hash.is_none() || !self.use_strict_verification {
            return self.file_exists(file_name);
        }
        
        let hash = hash.unwrap();
        
        // Check for exact filename and hash match
        self.downloaded_files.iter().any(|(name, stored_hash)| {
            name == file_name && stored_hash.as_ref().map_or(false, |h| h == hash)
        })
    }

    /// Adds a file to the tracking set after successful download
    pub(crate) fn mark_file_downloaded(&mut self, file_name: &str) {
        self.downloaded_files.insert((file_name.to_string(), None));
    }
    
    /// Adds a file with its SHA-512 hash to the tracking set
    pub(crate) fn mark_file_downloaded_with_hash(&mut self, file_name: &str, hash: String) {
        self.downloaded_files.insert((file_name.to_string(), Some(hash)));
    }
    
    /// Calculate hash for an existing file by its path
    pub(crate) fn calculate_hash_for_file(&self, file_path: &Path) -> Result<String> {
        DirectoryManager::calculate_sha512(file_path)
    }
    
    /// Scan and update hashes for all files that don't have them yet
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
        
        Ok(updated_count)
    }

    /// Find all files with the given name in the directory (recursively)
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
