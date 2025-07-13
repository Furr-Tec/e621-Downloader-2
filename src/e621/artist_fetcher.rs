use std::fs::write;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::e621::sender::RequestSender;
use crate::e621::blacklist::Blacklist;
use crate::e621::sender::entries::{UserEntry, TagEntry};
use crate::e621::io::Login;
use crate::e621::io::artist::{ArtistConfig, Artist};
use dialoguer::Input;

/// Represents an artist from the e621 API
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ApiArtist {
    pub id: i32,
    pub name: String,
    pub post_count: i32,
    pub is_active: Option<bool>,
}

/// Artist fetcher that retrieves popular artists from e621
pub struct ArtistFetcher {
    request_sender: RequestSender,
    blacklist: Option<Blacklist>,
}

impl ArtistFetcher {
    /// Create a new artist fetcher
    pub fn new(request_sender: RequestSender) -> Self {
        Self {
            request_sender,
            blacklist: None,
        }
    }
    
    /// Checks if the artists.json file exists.
    pub fn artists_file_exists() -> bool {
        ArtistConfig::exists()
    }

    /// Fetch the user's blacklist from their API key
    pub fn fetch_user_blacklist(&mut self) -> Result<()> {
        info!("Fetching user blacklist from API...");
        
        let username = Login::get().username();
        let user: UserEntry = self.request_sender.get_entry_from_appended_id(username, "user");
        
        if let Some(blacklist_tags) = user.blacklisted_tags {
            if !blacklist_tags.is_empty() {
                let mut blacklist = Blacklist::new(self.request_sender.clone());
                blacklist.parse_blacklist(blacklist_tags);
                blacklist.cache_users();
                self.blacklist = Some(blacklist);
                info!("User blacklist loaded successfully");
            } else {
                info!("User has no blacklisted tags");
            }
        } else {
            info!("User blacklist is empty or not available");
        }
        
        Ok(())
    }

    /// Fetch popular artists from e621 API, filtered by blacklist
    pub fn fetch_popular_artists(&self, limit: usize, min_post_count: i32) -> Result<Vec<ApiArtist>> {
        info!("Fetching popular artists from e621 API...");
        
        let mut all_artists = Vec::new();
        let mut page = 1;
        let per_page = 100; // e621 API limit
        
        // Fetch multiple pages to get enough artists
        while all_artists.len() < limit && page <= 5 { // Max 5 pages to avoid excessive API calls
            let url = format!(
                "https://e621.net/artists.json?limit={}&page={}&search[order]=post_count&search[is_active]=true",
                per_page, page
            );
            
            info!("Fetching page {} of artists...", page);
            
            // Make API request
            let response = self.request_sender.get_string(&url)?;
            let artists: Vec<ApiArtist> = serde_json::from_str(&response)?;
            
            // Filter artists by post count and blacklist
            for artist in artists {
                if artist.post_count >= min_post_count && !self.is_blacklisted(&artist.name) {
                    all_artists.push(artist);
                    
                    if all_artists.len() >= limit {
                        break;
                    }
                }
            }
            
            page += 1;
        }
        
        // Sort by post count (highest first)
        all_artists.sort_by(|a, b| b.post_count.cmp(&a.post_count));
        all_artists.truncate(limit);
        
        info!("Fetched {} popular non-blacklisted artists", all_artists.len());
        Ok(all_artists)
    }

    /// Check if an artist is blacklisted
    fn is_blacklisted(&self, artist_name: &str) -> bool {
        if let Some(ref blacklist) = self.blacklist {
            blacklist.is_tag_blacklisted(artist_name)
        } else {
            false
        }
    }

    /// Search for artists that match a query string
    pub fn search_artists(&self, query: &str, limit: usize) -> Result<Vec<ApiArtist>> {
        info!("Searching for artists matching: {}", query);
        
        let mut all_found_artists = Vec::new();
        
        // Strategy 1: Exact and prefix matches using tag search instead of artist search
        // The e621 artist API might not be working as expected, so let's try searching tags
        let exact_url = format!(
            "https://e621.net/tags.json?limit={}&search[name_matches]={}&search[category]=1&search[order]=count",
            limit, query
        );
        
        info!("Trying exact search URL: {}", exact_url);
        match self.request_sender.get_string(&exact_url) {
            Ok(response) => {
                info!("Got response from exact search: {} bytes", response.len());
                match serde_json::from_str::<Vec<TagEntry>>(&response) {
                    Ok(tags) => {
                        info!("Parsed {} tags from exact search", tags.len());
                        for tag in tags {
                            if tag.category == 1 { // Artist category
                                let artist = ApiArtist {
                                    id: tag.id as i32,
                                    name: tag.name.clone(),
                                    post_count: tag.post_count as i32,
                                    is_active: Some(true),
                                };
                                if !self.is_blacklisted(&artist.name) {
                                    all_found_artists.push(artist);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse tags from exact search: {}", e);
                        info!("Response content: {}", &response[..std::cmp::min(200, response.len())]);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to get response from exact search: {}", e);
            }
        }
        
        // Strategy 2: Wildcard search for artists containing the query
        if all_found_artists.len() < limit {
            let wildcard_url = format!(
                "https://e621.net/tags.json?limit={}&search[name_matches]=*{}*&search[category]=1&search[order]=count",
                limit, query
            );
            
            info!("Trying wildcard search URL: {}", wildcard_url);
            match self.request_sender.get_string(&wildcard_url) {
                Ok(response) => {
                    info!("Got response from wildcard search: {} bytes", response.len());
                    match serde_json::from_str::<Vec<TagEntry>>(&response) {
                        Ok(tags) => {
                            info!("Parsed {} tags from wildcard search", tags.len());
                            for tag in tags {
                                if tag.category == 1 { // Artist category
                                    let artist = ApiArtist {
                                        id: tag.id as i32,
                                        name: tag.name.clone(),
                                        post_count: tag.post_count as i32,
                                        is_active: Some(true),
                                    };
                                    if !self.is_blacklisted(&artist.name) && 
                                       !all_found_artists.iter().any(|a| a.name == artist.name) {
                                        all_found_artists.push(artist);
                                        if all_found_artists.len() >= limit {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse tags from wildcard search: {}", e);
                            info!("Response content: {}", &response[..std::cmp::min(200, response.len())]);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get response from wildcard search: {}", e);
                }
            }
        }
        
        // Sort by post count (highest first)
        all_found_artists.sort_by(|a, b| b.post_count.cmp(&a.post_count));
        all_found_artists.truncate(limit);
        
        info!("Found {} matching artists for query: {}", all_found_artists.len(), query);
        Ok(all_found_artists)
    }

    /// Save artists to the artists.json file
    pub fn save_artists_to_file(&self, api_artists: &[ApiArtist]) -> Result<()> {
        let mut artist_config = ArtistConfig::load().unwrap_or_default();
        
        for api_artist in api_artists {
            let artist = Artist::new(api_artist.name.clone());
            artist_config.add_artist(artist);
        }
        
        artist_config.save()?;
        
        info!("Saved {} artists to artists.json", api_artists.len());
        Ok(())
    }

    /// Interactive artist management workflow
    pub fn interactive_artist_management(&mut self) -> Result<()> {
        println!("\n--- Interactive Artist Management ---");
        println!("This will help you discover and manage artists for your downloads.");
        println!("You can search for artists, then choose to add or remove them.");
        
        // First fetch the user's blacklist
        self.fetch_user_blacklist()?;
        
        // Load existing saved artists to avoid showing them again
        let mut existing_artist_config = ArtistConfig::load().unwrap_or_default();
        let existing_artist_names: Vec<String> = existing_artist_config
            .all_artists()
            .iter()
            .map(|a| a.name().to_string())
            .collect();
        
        if !existing_artist_names.is_empty() {
            println!("Loaded {} already saved artists (will be filtered from search results)", existing_artist_names.len());
        }
        
        let mut added_artists = Vec::new();
        let mut removed_artists: Vec<String> = Vec::new();
        
        loop {
            println!("\n--- Artist Search ---");
            if !added_artists.is_empty() {
                println!("Current session: {} artists added (unsaved)", added_artists.len());
            }
            
            // Get search query from user
            let query: String = Input::new()
                .with_prompt("Enter an artist name to search for (or 'done' to save & finish)")
                .interact_text()
                .unwrap_or_else(|_| "done".to_string());
            
            if query.trim().to_lowercase() == "done" {
                break;
            }
            
            // Search for matching artists
            match self.search_artists(&query, 20) {
                Ok(mut artists) => {
                    // Filter out already saved artists
                    let initial_count = artists.len();
                    artists.retain(|artist| !existing_artist_names.contains(&artist.name));
                    
                    if initial_count > artists.len() {
                        let filtered_count = initial_count - artists.len();
                        println!("Filtered out {} already saved artists", filtered_count);
                    }
                    
                    if artists.is_empty() {
                        if initial_count > 0 {
                            println!("All {} artists matching '{}' are already saved", initial_count, query);
                        } else {
                            println!("No artists found matching '{}'", query);
                        }
                        continue;
                    }
                    
                    // Show artists to user
                    println!("\nFound {} artists matching '{}':", artists.len(), query);
                    
                    let mut artist_options = Vec::new();
                    for (i, artist) in artists.iter().enumerate() {
                        let status = if self.is_blacklisted(&artist.name) {
                            "[BLACKLISTED]"
                        } else {
                            ""
                        };
                        
                        let display = format!(
                            "{} - {} posts {}", 
                            artist.name, 
                            artist.post_count,
                            status
                        );
                        artist_options.push(display);
                        
                        if i >= 19 { // Limit display to prevent overwhelming
                            break;
                        }
                    }
                    
                    artist_options.push("Search for different artists".to_string());
                    
                    // Let user select artists
                    println!("\nAvailable options:");
                    for (i, option) in artist_options.iter().enumerate() {
                        println!("{}. {}", i + 1, option);
                    }
                    
                    let selections = loop {
                        let input: String = Input::new()
                            .with_prompt(&format!("Select options (e.g., 1,2,3 or 1-3) (1-{}):", artist_options.len()))
                            .interact_text()
                            .unwrap_or_else(|_| artist_options.len().to_string());

                        let ranges: Vec<(usize, usize)> = input
                            .split(',')
                            .filter_map(|range| {
                                if range.contains('-') {
                                    let parts: Vec<&str> = range.split('-').collect();
                                    if parts.len() == 2 {
                                        if let (Ok(start), Ok(end)) = (parts[0].trim().parse::<usize>(), parts[1].trim().parse::<usize>()) {
                                            return Some((start, end));
                                        }
                                    }
                                    return None;
                                }
                                range.trim().parse::<usize>().ok().map(|num| (num, num))
                            })
                            .collect();

                        if ranges.iter().all(|&(start, end)| start > 0 && end <= artist_options.len() && start <= end) {
                            break ranges;
                        }
                        println!("Invalid selection. Please enter numbers between 1 and {}", artist_options.len());
                    };

                    let mut selected_artists = Vec::new();
                    let mut search_again = false;
                    
                    for (start, end) in selections {
                        for index in start..=end {
                            if index <= artists.len() {
                                selected_artists.push(&artists[index - 1]);
                            } else if index == artist_options.len() {
                                search_again = true;
                            }
                        }
                    }

                    if search_again || selected_artists.is_empty() {
                        continue; // "Search for different artists" selected
                    }

                    // Add selected artists
                    for artist in selected_artists {
                        added_artists.push(artist.clone());
                        println!("Added '{}' to artist list", artist.name);
                    }
                },
                Err(e) => {
                    warn!("Failed to search artists: {}", e);
                }
            }
        }
        
        // Save added artists to artists.json
        if !added_artists.is_empty() {
            let confirm_save = loop {
                let input: String = Input::new()
                    .with_prompt(&format!("Save {} added artists to artists.json? (y/n):", added_artists.len()))
                    .interact_text()
                    .unwrap_or_else(|_| "y".to_string());
                
                match input.trim().to_lowercase().as_str() {
                    "y" | "yes" | "" => break true,
                    "n" | "no" => break false,
                    _ => println!("Please enter 'y' for yes or 'n' for no."),
                }
            };
            
            if confirm_save {
                self.save_artists_to_file(&added_artists)?;
                println!("Saved {} artists to artists.json", added_artists.len());
            }
        }
        
        // Summary
        println!("\nSession Summary:");
        println!("   Added: {} artists", added_artists.len());
        println!("   Removed: {} artists", removed_artists.len());
        
        Ok(())
    }

    /// Refresh artists - fetch new ones and update the file
    pub fn refresh_artists(&mut self, limit: usize, min_post_count: i32) -> Result<()> {
        // First fetch the user's blacklist
        self.fetch_user_blacklist()?;
        
        // Then fetch and save artists
        let artists = self.fetch_popular_artists(limit, min_post_count)?;
        self.save_artists_to_file(&artists)?;
        Ok(())
    }
}
