/*
 * Copyright (c) 2022 McSib
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::path::{Path, PathBuf};
use std::fs;
use anyhow::{Result, Error, Context};

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
}

impl DirectoryManager {
    /// Creates a new DirectoryManager with the specified root directory
    pub(crate) fn new(root_dir: &str) -> Result<Self> {
        let root = PathBuf::from(root_dir);
        let artists = root.join("Artists");
        let tags = root.join("Tags");
        let pools = root.join("Pools");

        let manager = DirectoryManager {
            root_dir: root,
            artists_dir: artists,
            tags_dir: tags,
            pools_dir: pools,
        };

        manager.create_directory_structure()?;
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
        self.find_file_in_dir(&self.artists_dir, file_name)
            || self.find_file_in_dir(&self.tags_dir, file_name)
            || self.find_file_in_dir(&self.pools_dir, file_name)
    }

    /// Recursively searches for a file in a directory
    fn find_file_in_dir(&self, dir: &Path, file_name: &str) -> bool {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.file_name().unwrap_or_default().to_string_lossy() == file_name {
                    return true;
                } else if path.is_dir() && self.find_file_in_dir(&path, file_name) {
                    return true;
                }
            }
        }
        false
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
