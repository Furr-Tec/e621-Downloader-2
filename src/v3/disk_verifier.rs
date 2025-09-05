//! Disk Verification and File Watcher for E621 Downloader
//! 
//! This module provides functionality for:
//! 1. Recursive file scanning on startup
//! 2. Verifying each file's hash vs metadata
//! 3. Detecting renamed, moved, missing, or corrupt files
//! 4. Using notify crate to watch directories if available
//! 5. Rebuilding metadata if --verify-disk flag is passed

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use blake3::Hasher as Blake3Hasher;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use notify::event::ModifyKind;
use parking_lot::RwLock;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task;
use tracing::{error, warn};
use walkdir::WalkDir;

use crate::v3::{
    ConfigManager,
    FileHash, FileProcessor, FileProcessorResult,
    MetadataStore, MetadataStoreError, MetadataStoreResult, PostMetadata,
};

/// Error types for disk verification
#[derive(Error, Debug)]
pub enum DiskVerifierError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataStoreError),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Watch error: {0}")]
    Watch(#[from] notify::Error),

    #[error("Verification error: {0}")]
    Verification(String),
}

/// Result type for disk verification operations
pub type DiskVerifierResult<T> = Result<T, DiskVerifierError>;

/// File status enum
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatus {
    /// File exists and matches metadata
    Ok,
    /// File is missing
    Missing,
    /// File exists but hash doesn't match metadata
    Corrupt,
    /// File exists but has been moved from its original location
    Moved(PathBuf),
    /// File exists but has been renamed
    Renamed(String),
    /// File exists but is not in metadata
    Orphaned,
}

/// File verification result
#[derive(Debug, Clone)]
pub struct FileVerificationResult {
    pub post_id: u32,
    pub file_path: PathBuf,
    pub status: FileStatus,
    pub metadata: Option<PostMetadata>,
}

/// Event type for file watcher
#[derive(Debug, Clone)]
pub enum FileWatchEvent {
    /// File was created
    Created(PathBuf),
    /// File was modified
    Modified(PathBuf),
    /// File was deleted
    Deleted(PathBuf),
    /// File was renamed
    Renamed(PathBuf, PathBuf),
}

/// Disk verifier for managing file verification and watching
pub struct DiskVerifier {
    config_manager: Arc<ConfigManager>,
    metadata_store: Arc<MetadataStore>,
    file_processor: Arc<FileProcessor>,
    verified_files: Arc<RwLock<HashMap<u32, FileVerificationResult>>>,
    orphaned_files: Arc<RwLock<Vec<PathBuf>>>,
    _watcher: Option<RecommendedWatcher>,
    watch_event_tx: broadcast::Sender<FileWatchEvent>,
}

impl DiskVerifier {
    /// Create a new disk verifier
    pub fn new(
        config_manager: Arc<ConfigManager>,
        metadata_store: Arc<MetadataStore>,
        file_processor: Arc<FileProcessor>,
    ) -> DiskVerifierResult<Self> {
        // Create broadcast channel for file watch events
        let (watch_event_tx, _) = broadcast::channel(100);

        Ok(Self {
            config_manager,
            metadata_store,
            file_processor,
            verified_files: Arc::new(RwLock::new(HashMap::new())),
            orphaned_files: Arc::new(RwLock::new(Vec::new())),
            _watcher: None,
            watch_event_tx,
        })
    }

    /// Create a new disk verifier from app config
    pub fn from_config(
        config_manager: Arc<ConfigManager>,
        metadata_store: Arc<MetadataStore>,
        file_processor: Arc<FileProcessor>,
    ) -> DiskVerifierResult<Self> {
        Self::new(config_manager, metadata_store, file_processor)
    }

    /// Scan a directory recursively for files
    pub fn scan_directory(&self, dir_path: &Path) -> DiskVerifierResult<Vec<PathBuf>> {
        if !dir_path.exists() {
            return Err(DiskVerifierError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Directory not found: {}", dir_path.display()),
            )));
        }

        let mut files = Vec::new();

        // Use walkdir to recursively scan the directory
        for entry in WalkDir::new(dir_path).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            // Skip directories
            if path.is_dir() {
                continue;
            }

            // Add file to the list
            files.push(path.to_path_buf());
        }

        Ok(files)
    }

    /// Hash a file using blake3 and sha256 in parallel
    fn hash_file(&self, file_path: &Path) -> DiskVerifierResult<FileHash> {
        // Read the file into memory
        let mut file = fs::File::open(file_path)
            .map_err(|e| DiskVerifierError::Io(e))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| DiskVerifierError::Io(e))?;

        // Hash the file in parallel using rayon
        let (blake3_hash, sha256_hash) = rayon::join(
            || {
                let mut hasher = Blake3Hasher::new();
                hasher.update(&buffer);
                hex::encode(hasher.finalize().as_bytes())
            },
            || {
                let mut hasher = Sha256::new();
                hasher.update(&buffer);
                hex::encode(hasher.finalize())
            }
        );

        Ok(FileHash {
            blake3: blake3_hash,
            sha256: sha256_hash,
        })
    }

    /// Verify a file against its metadata
    pub fn verify_file(&self, file_path: &Path) -> DiskVerifierResult<FileVerificationResult> {
        // Check if the file exists
        if !file_path.exists() {
            // Try to find the metadata by filename
            let filename = file_path.file_name()
                .and_then(|f| f.to_str())
                .ok_or_else(|| DiskVerifierError::Verification(
                    format!("Invalid file path: {}", file_path.display())
                ))?;

            // Extract post ID from filename (assuming format: post_id_*.*)
            let post_id = filename.split('_')
                .next()
                .and_then(|id| id.parse::<u32>().ok())
                .ok_or_else(|| DiskVerifierError::Verification(
                    format!("Could not extract post ID from filename: {}", filename)
                ))?;

            // Get metadata for the post
            let metadata = self.metadata_store.get_metadata(post_id)?;

            if let Some(metadata) = metadata {
                return Ok(FileVerificationResult {
                    post_id,
                    file_path: file_path.to_path_buf(),
                    status: FileStatus::Missing,
                    metadata: Some(metadata),
                });
            } else {
                return Ok(FileVerificationResult {
                    post_id,
                    file_path: file_path.to_path_buf(),
                    status: FileStatus::Orphaned,
                    metadata: None,
                });
            }
        }

        // Hash the file
        let hashes = self.hash_file(file_path)?;

        // Try to find metadata by hash
        let metadata_by_hash = self.metadata_store.get_metadata_by_hash(&hashes.blake3)?
            .or_else(|| self.metadata_store.get_metadata_by_hash(&hashes.sha256).ok().flatten());

        if let Some(metadata) = metadata_by_hash {
            // Check if the file has been moved
            let metadata_path = Path::new(&metadata.file_path);
            if metadata_path != file_path {
                return Ok(FileVerificationResult {
                    post_id: metadata.post_id,
                    file_path: file_path.to_path_buf(),
                    status: FileStatus::Moved(metadata_path.to_path_buf()),
                    metadata: Some(metadata),
                });
            }

            // File exists and is in the correct location
            return Ok(FileVerificationResult {
                post_id: metadata.post_id,
                file_path: file_path.to_path_buf(),
                status: FileStatus::Ok,
                metadata: Some(metadata),
            });
        }

        // Try to extract post ID from filename
        let filename = file_path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| DiskVerifierError::Verification(
                format!("Invalid file path: {}", file_path.display())
            ))?;

        let post_id = filename.split('_')
            .next()
            .and_then(|id| id.parse::<u32>().ok())
            .ok_or_else(|| DiskVerifierError::Verification(
                format!("Could not extract post ID from filename: {}", filename)
            ))?;

        // Get metadata for the post
        let metadata = self.metadata_store.get_metadata(post_id)?;

        if let Some(metadata) = metadata {
            // File exists but hash doesn't match
            return Ok(FileVerificationResult {
                post_id,
                file_path: file_path.to_path_buf(),
                status: FileStatus::Corrupt,
                metadata: Some(metadata),
            });
        }

        // File exists but is not in metadata
        Ok(FileVerificationResult {
            post_id,
            file_path: file_path.to_path_buf(),
            status: FileStatus::Orphaned,
            metadata: None,
        })
    }

    /// Verify all files in a directory
    pub fn verify_directory(&self, dir_path: &Path) -> DiskVerifierResult<HashMap<u32, FileVerificationResult>> {
        // Scan the directory for files
        let files = self.scan_directory(dir_path)?;

        // Verify each file in parallel
        let results: Vec<FileVerificationResult> = files.par_iter()
            .filter_map(|file_path| {
                match self.verify_file(file_path) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        warn!("Failed to verify file {}: {}", file_path.display(), e);
                        None
                    }
                }
            })
            .collect();

        // Store the results
        let mut verified_files = self.verified_files.write();
        let mut orphaned_files = self.orphaned_files.write();

        // Clear previous results
        verified_files.clear();
        orphaned_files.clear();

        // Process the results
        let mut result_map = HashMap::new();
        for result in results {
            if result.status == FileStatus::Orphaned {
                orphaned_files.push(result.file_path.clone());
            } else {
                result_map.insert(result.post_id, result.clone());
                verified_files.insert(result.post_id, result);
            }
        }

        Ok(result_map)
    }

    /// Verify all files in the download directory
    pub fn verify_all_files(&self) -> DiskVerifierResult<HashMap<u32, FileVerificationResult>> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| DiskVerifierError::Config(e.to_string()))?;

        // Get the download directory
        let download_dir = Path::new(&app_config.paths.download_directory);

        // Verify all files in the download directory
        self.verify_directory(download_dir)
    }

    /// Rebuild metadata for a file
    pub fn rebuild_metadata(&self, result: &FileVerificationResult) -> DiskVerifierResult<Option<PostMetadata>> {
        match &result.status {
            FileStatus::Ok => {
                // Metadata is already correct
                Ok(result.metadata.clone())
            },
            FileStatus::Missing => {
                // Can't rebuild metadata for missing files
                Ok(None)
            },
            FileStatus::Corrupt => {
                // Hash the file
                let hashes = self.hash_file(&result.file_path)?;

                // Create new metadata with the correct hashes
                if let Some(old_metadata) = &result.metadata {
                    let mut new_metadata = old_metadata.clone();
                    new_metadata.blake3_hash = hashes.blake3.clone();
                    new_metadata.sha256_hash = hashes.sha256.clone();

                    // Update the file size
                    let file_size = fs::metadata(&result.file_path)?.len();
                    new_metadata.file_size = file_size;

                    // Update the timestamp
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    new_metadata.updated_at = now;

                    // Save the updated metadata
                    self.metadata_store.save_metadata(&new_metadata)?;

                    Ok(Some(new_metadata))
                } else {
                    Ok(None)
                }
            },
            FileStatus::Moved(_old_path) => {
                // Update the file path in the metadata
                if let Some(old_metadata) = &result.metadata {
                    let mut new_metadata = old_metadata.clone();
                    new_metadata.file_path = result.file_path.to_string_lossy().to_string();

                    // Update the timestamp
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    new_metadata.updated_at = now;

                    // Save the updated metadata
                    self.metadata_store.save_metadata(&new_metadata)?;

                    Ok(Some(new_metadata))
                } else {
                    Ok(None)
                }
            },
            FileStatus::Renamed(_old_name) => {
                // Update the file path in the metadata
                if let Some(old_metadata) = &result.metadata {
                    let mut new_metadata = old_metadata.clone();
                    new_metadata.file_path = result.file_path.to_string_lossy().to_string();

                    // Update the timestamp
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    new_metadata.updated_at = now;

                    // Save the updated metadata
                    self.metadata_store.save_metadata(&new_metadata)?;

                    Ok(Some(new_metadata))
                } else {
                    Ok(None)
                }
            },
            FileStatus::Orphaned => {
                // Hash the file
                let hashes = self.hash_file(&result.file_path)?;

                // Get the file size
                let file_size = fs::metadata(&result.file_path)?.len();

                // Create new metadata
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                let new_metadata = PostMetadata {
                    post_id: result.post_id,
                    file_path: result.file_path.to_string_lossy().to_string(),
                    blake3_hash: hashes.blake3.clone(),
                    sha256_hash: hashes.sha256.clone(),
                    file_size,
                    width: None,
                    height: None,
                    tags: Vec::new(),
                    artist: None,
                    rating: None,
                    score: None,
                    created_at: now,
                    updated_at: now,
                    verified_at: Some(now),
                };

                // Save the new metadata
                self.metadata_store.save_metadata(&new_metadata)?;

                Ok(Some(new_metadata))
            },
        }
    }

    /// Rebuild metadata for all files
    pub fn rebuild_all_metadata(&self) -> DiskVerifierResult<usize> {
        // Verify all files first
        let verification_results = self.verify_all_files()?;

        // Count of successfully rebuilt metadata
        let mut rebuilt_count = 0;

        // Rebuild metadata for each file
        for (_, result) in verification_results {
            if result.status != FileStatus::Ok {
                if let Ok(Some(_)) = self.rebuild_metadata(&result) {
                    rebuilt_count += 1;
                }
            }
        }

        Ok(rebuilt_count)
    }

    /// Setup file watcher for the download directory
    pub fn setup_watcher(&mut self) -> DiskVerifierResult<()> {
        // Get the app config
        let app_config = self.config_manager.get_app_config()
            .map_err(|e| DiskVerifierError::Config(e.to_string()))?;

        // Get the download directory
        let download_dir = Path::new(&app_config.paths.download_directory);

        // Create a channel to receive events
        let (tx, rx) = std::sync::mpsc::channel();

        // Create a watcher
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

        // Start watching the download directory
        watcher.watch(download_dir, RecursiveMode::Recursive)?;

        // Clone Arc references for the async task
        let watch_event_tx = self.watch_event_tx.clone();

        // Spawn a task to handle file change events
        task::spawn(async move {
            for res in rx {
                match res {
                    Ok(Event { kind, paths, .. }) => {
                        match kind {
                            EventKind::Create(_) => {
                                for path in paths {
                                    let _ = watch_event_tx.send(FileWatchEvent::Created(path));
                                }
                            },
                            EventKind::Modify(ModifyKind::Name(_)) => {
                                if paths.len() == 2 {
                                    let _ = watch_event_tx.send(FileWatchEvent::Renamed(
                                        paths[0].clone(),
                                        paths[1].clone(),
                                    ));
                                }
                            },
                            EventKind::Modify(_) => {
                                for path in paths {
                                    let _ = watch_event_tx.send(FileWatchEvent::Modified(path));
                                }
                            },
                            EventKind::Remove(_) => {
                                for path in paths {
                                    let _ = watch_event_tx.send(FileWatchEvent::Deleted(path));
                                }
                            },
                            _ => {}
                        }
                    },
                    Err(e) => error!("Watch error: {}", e),
                }
            }
        });

        // Store the watcher
        self._watcher = Some(watcher);

        Ok(())
    }

    /// Get a subscription to file watch events
    pub fn subscribe(&self) -> broadcast::Receiver<FileWatchEvent> {
        self.watch_event_tx.subscribe()
    }

    /// Get the verified files
    pub fn get_verified_files(&self) -> HashMap<u32, FileVerificationResult> {
        self.verified_files.read().clone()
    }

    /// Get the orphaned files
    pub fn get_orphaned_files(&self) -> Vec<PathBuf> {
        self.orphaned_files.read().clone()
    }
}

/// Create a new disk verifier
pub async fn init_disk_verifier(
    config_manager: Arc<ConfigManager>,
    metadata_store: Arc<MetadataStore>,
    file_processor: Arc<FileProcessor>,
) -> DiskVerifierResult<Arc<DiskVerifier>> {
    let mut verifier = DiskVerifier::from_config(
        config_manager,
        metadata_store,
        file_processor,
    )?;

    // Setup the file watcher
    verifier.setup_watcher()?;

    Ok(Arc::new(verifier))
}
