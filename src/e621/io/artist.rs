use std::fs::{read_to_string, write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::e621::sender::RequestSender;

/// Constant of the artist file's name.
pub(crate) const ARTIST_FILE_NAME: &str = "artists.json";

/// An artist entry containing the artist name and optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Artist {
    /// The name of the artist.
    pub name: String,
    /// Optional display name for the artist.
    pub display_name: Option<String>,
    /// Whether this artist is enabled for downloading.
    pub enabled: bool,
}

impl Artist {
    /// Creates a new artist entry.
    pub fn new(name: String) -> Self {
        Artist {
            name,
            display_name: None,
            enabled: true,
        }
    }

    /// Creates a new artist with a custom display name.
    pub fn new_with_display_name(name: String, display_name: String) -> Self {
        Artist {
            name,
            display_name: Some(display_name),
            enabled: true,
        }
    }

    /// Gets the artist name used for searching.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the display name, falling back to the artist name if not set.
    pub fn display_name(&self) -> &str {
        self.display_name.as_ref().unwrap_or(&self.name)
    }

    /// Checks if this artist is enabled for downloading.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Sets the enabled status of this artist.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

/// Artist configuration containing all managed artists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtistConfig {
    /// List of artists to download from.
    pub artists: Vec<Artist>,
}

impl ArtistConfig {
    /// Creates a new empty artist configuration.
    pub fn new() -> Self {
        ArtistConfig {
            artists: Vec::new(),
        }
    }

    /// Loads the artist configuration from the artists.json file.
    pub fn load() -> Result<Self> {
        if !Self::exists() {
            return Ok(Self::new());
        }

        let content = read_to_string(ARTIST_FILE_NAME)
            .with_context(|| format!("Failed to read {}", ARTIST_FILE_NAME))?;

        let config: ArtistConfig = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", ARTIST_FILE_NAME))?;

        Ok(config)
    }

    /// Saves the artist configuration to the artists.json file.
    pub fn save(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Failed to serialize artist configuration")?;

        write(ARTIST_FILE_NAME, content)
            .with_context(|| format!("Failed to write {}", ARTIST_FILE_NAME))?;

        Ok(())
    }

    /// Checks if the artists.json file exists.
    pub fn exists() -> bool {
        Path::new(ARTIST_FILE_NAME).exists()
    }

    /// Adds a new artist to the configuration.
    pub fn add_artist(&mut self, artist: Artist) {
        // Check if artist already exists
        if !self.artists.iter().any(|a| a.name == artist.name) {
            self.artists.push(artist);
        }
    }

    /// Removes an artist from the configuration.
    pub fn remove_artist(&mut self, name: &str) -> bool {
        let initial_len = self.artists.len();
        self.artists.retain(|a| a.name != name);
        self.artists.len() != initial_len
    }

    /// Gets all enabled artists.
    pub fn enabled_artists(&self) -> Vec<&Artist> {
        self.artists.iter().filter(|a| a.enabled).collect()
    }

    /// Gets all artists (enabled and disabled).
    pub fn all_artists(&self) -> &Vec<Artist> {
        &self.artists
    }

    /// Finds an artist by name.
    pub fn find_artist(&self, name: &str) -> Option<&Artist> {
        self.artists.iter().find(|a| a.name == name)
    }

    /// Finds an artist by name (mutable).
    pub fn find_artist_mut(&mut self, name: &str) -> Option<&mut Artist> {
        self.artists.iter_mut().find(|a| a.name == name)
    }
}

impl Default for ArtistConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses the artist file and returns the enabled artists.
///
/// # Arguments
///
/// * `request_sender`: The request sender (currently not used but included for consistency with parse_tag_file)
///
/// returns: Result<Vec<Artist>, anyhow::Error>
pub(crate) fn parse_artist_file(_request_sender: &RequestSender) -> Result<Vec<Artist>> {
    let config = ArtistConfig::load().with_context(|| "Failed to load artist configuration")?;
    let enabled_artists = config.enabled_artists().into_iter().cloned().collect();
    Ok(enabled_artists)
}
