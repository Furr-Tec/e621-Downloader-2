use std::path::{Path, PathBuf};
use std::fs;
use std::collections::HashSet;
use anyhow::{Result, Context};

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
    /// Set of already downloaded files
    downloaded_files: HashSet<String>,
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
        };

        manager.create_directory_structure()?;

        // Scan directories for existing files
        let mut files = HashSet::new();
        DirectoryManager::scan_directory_static(&artists, &mut files)?;
        DirectoryManager::scan_directory_static(&tags, &mut files)?;
        DirectoryManager::scan_directory_static(&pools, &mut files)?;

        Ok(DirectoryManager {
            root_dir: root,
            artists_dir: artists,
            tags_dir: tags,
            pools_dir: pools,
            downloaded_files: files,
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

    /// Recursively scans a directory and adds all files to the provided set
    fn scan_directory_static(dir: &Path, files: &mut HashSet<String>) -> Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    DirectoryManager::scan_directory_static(&path, files)?;
                } else {
                    if let Some(name) = path.file_name() {
                        if let Some(name_str) = name.to_str() {
                            files.insert(name_str.to_string());
                        }
                    }
                }
            }
        }
        Ok(())
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
        self.downloaded_files.contains(file_name)
    }

    /// Adds a file to the tracking set after successful download
    pub(crate) fn mark_file_downloaded(&mut self, file_name: &str) {
        self.downloaded_files.insert(file_name.to_string());
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
