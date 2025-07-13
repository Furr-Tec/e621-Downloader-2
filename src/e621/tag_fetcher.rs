use std::fs::{write, read_to_string};
use std::path::Path;
use std::collections::HashSet;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::e621::sender::RequestSender;
use crate::e621::blacklist::Blacklist;
use crate::e621::sender::entries::UserEntry;
use crate::e621::io::Login;
use dialoguer::Input;

/// Name of the tag cache file
const TAG_CACHE_FILE: &str = "tags.txt";

/// Represents a tag from the e621 API
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ApiTag {
    pub id: i32,
    pub name: String,
    pub post_count: i32,
    pub category: i32, // 0=general, 1=artist, 3=copyright, 4=character, 5=species
}


/// Category mappings for e621 tags
#[derive(Debug, Clone)]
pub enum TagCategory {
    General = 0,
    Artist = 1,
    Copyright = 3,
    Character = 4,
    Species = 5,
}

impl TagCategory {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(TagCategory::General),
            1 => Some(TagCategory::Artist),
            3 => Some(TagCategory::Copyright),
            4 => Some(TagCategory::Character),
            5 => Some(TagCategory::Species),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            TagCategory::General => "general",
            TagCategory::Artist => "artist",
            TagCategory::Copyright => "copyright",
            TagCategory::Character => "character",
            TagCategory::Species => "species",
        }
    }
}


/// Tag fetcher that retrieves popular non-blacklisted tags from e621
pub struct TagFetcher {
    request_sender: RequestSender,
    blacklist: Option<Blacklist>,
}

impl TagFetcher {
    /// Create a new tag fetcher
    pub fn new(request_sender: RequestSender) -> Self {
        Self {
            request_sender,
            blacklist: None,
        }
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

    /// Fetch popular tags from e621 API, filtered by blacklist
    pub fn fetch_popular_tags(&self, limit: usize, min_post_count: i32) -> Result<Vec<ApiTag>> {
        info!("Fetching popular tags from e621 API...");
        
        let mut all_tags = Vec::new();
        let mut page = 1;
        let per_page = 100; // e621 API limit
        
        // Fetch multiple pages to get enough tags
        while all_tags.len() < limit && page <= 5 { // Max 5 pages to avoid excessive API calls
            let url = format!(
                "https://e621.net/tags.json?limit={}&page={}&search[hide_empty]=true&search[order]=count",
                per_page, page
            );
            
            info!("Fetching page {} of tags...", page);
            
            // Make API request
            let response = self.request_sender.get_string(&url)?;
            let tags: Vec<ApiTag> = serde_json::from_str(&response)?;
            
            // Filter tags by post count and blacklist
            for tag in tags {
                if tag.post_count >= min_post_count && !self.is_blacklisted(&tag.name) {
                    all_tags.push(tag);
                    
                    if all_tags.len() >= limit {
                        break;
                    }
                }
            }
            
            page += 1;
        }
        
        // Sort by post count (highest first)
        all_tags.sort_by(|a, b| b.post_count.cmp(&a.post_count));
        all_tags.truncate(limit);
        
        info!("Fetched {} popular non-blacklisted tags", all_tags.len());
        Ok(all_tags)
    }

    /// Check if a tag is blacklisted
    fn is_blacklisted(&self, tag_name: &str) -> bool {
        if let Some(ref blacklist) = self.blacklist {
            blacklist.is_tag_blacklisted(tag_name)
        } else {
            false
        }
    }

    /// Save tags to the tags.txt file in the format expected by the application
    pub fn save_tags_to_file(&self, tags: &[ApiTag]) -> Result<()> {
        let mut tag_lines = Vec::new();
        
        // Group tags by category
        let mut artists = Vec::new();
        let mut species = Vec::new();
        let mut characters = Vec::new();
        let mut general = Vec::new();
        
        for tag in tags {
            match TagCategory::from_i32(tag.category) {
                Some(TagCategory::Artist) => artists.push(tag),
                Some(TagCategory::Species) => species.push(tag),
                Some(TagCategory::Character) => characters.push(tag),
                Some(TagCategory::General) => general.push(tag),
                Some(TagCategory::Copyright) => general.push(tag), // Treat copyright as general
                None => general.push(tag), // Unknown categories go to general
            }
        }
        
        // Add header comment
        tag_lines.push("# Popular tags fetched from e621 API (non-blacklisted)".to_string());
        tag_lines.push("# This file follows the standard tags.txt format".to_string());
        tag_lines.push("".to_string());
        
        // Add artists section
        tag_lines.push("[artists]".to_string());
        if !artists.is_empty() {
            for tag in artists {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        } else {
            tag_lines.push("# No popular artists found".to_string());
        }
        tag_lines.push("".to_string());
        
        // Add pools section (empty but required format)
        tag_lines.push("[pools]".to_string());
        tag_lines.push("# No pool IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add sets section (empty but required format)
        tag_lines.push("[sets]".to_string());
        tag_lines.push("# No set IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add single-post section (empty but required format)
        tag_lines.push("[single-post]".to_string());
        tag_lines.push("# No individual post IDs needed".to_string());
        tag_lines.push("".to_string());
        
        // Add general tags section
        tag_lines.push("[general]".to_string());
        if !general.is_empty() {
            for tag in &general {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        // Add species as general tags if any
        if !species.is_empty() {
            if !general.is_empty() {
                tag_lines.push("# Popular species:".to_string());
            }
            for tag in &species {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        // Add characters as general tags if any
        if !characters.is_empty() {
            if !general.is_empty() || !species.is_empty() {
                tag_lines.push("# Popular characters:".to_string());
            }
            for tag in &characters {
                tag_lines.push(format!("{} # ({} posts)", tag.name, tag.post_count));
            }
        }
        
        if general.is_empty() && species.is_empty() && characters.is_empty() {
            tag_lines.push("# No popular general tags found".to_string());
        }
        
        tag_lines.push("".to_string());
        
        // Write to file
        let content = tag_lines.join("\n");
        write(TAG_CACHE_FILE, content)?;
        
        info!("Saved {} tags to {}", tags.len(), TAG_CACHE_FILE);
        Ok(())
    }
    
    /// Append tags to the existing tags.txt file without overwriting
    pub fn append_tags_to_file(&self, tags: &[ApiTag]) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        
        // If file doesn't exist, create it normally
        if !Self::tags_file_exists() {
            return self.save_tags_to_file(tags);
        }
        
        // Read existing content
        let existing_content = read_to_string(TAG_CACHE_FILE)?;
        let mut lines: Vec<String> = existing_content.lines().map(|s| s.to_string()).collect();
        
        // Group new tags by category
        let mut new_artists = Vec::new();
        let mut new_general = Vec::new();
        
        for tag in tags {
            match TagCategory::from_i32(tag.category) {
                Some(TagCategory::Artist) => new_artists.push(tag),
                Some(TagCategory::Species) => new_general.push(tag),
                Some(TagCategory::Character) => new_general.push(tag),
                Some(TagCategory::General) => new_general.push(tag),
                Some(TagCategory::Copyright) => new_general.push(tag),
                None => new_general.push(tag),
            }
        }
        
        // Get existing tags to avoid duplicates
        let existing_tags = Self::load_existing_tags().unwrap_or_default();
        let existing_set: HashSet<String> = existing_tags.into_iter().collect();
        
        // Filter out duplicates
        new_artists.retain(|tag| !existing_set.contains(&tag.name));
        new_general.retain(|tag| !existing_set.contains(&tag.name));
        
        if new_artists.is_empty() && new_general.is_empty() {
            info!("No new tags to add - all tags already exist in file");
            return Ok(());
        }
        
        // Find section indices
        let mut artists_section_start = None;
        let mut artists_section_end = None;
        let mut general_section_start = None;
        let mut general_section_end = None;
        let mut current_section = None;
        
        for (i, line) in lines.iter().enumerate() {
            let line = line.trim();
            
            if line == "[artists]" {
                artists_section_start = Some(i);
                current_section = Some("artists");
            } else if line == "[general]" {
                general_section_start = Some(i);
                current_section = Some("general");
            } else if line.starts_with('[') && line.ends_with(']') {
                // New section started
                match current_section {
                    Some("artists") => artists_section_end = Some(i),
                    Some("general") => general_section_end = Some(i),
                    _ => {}
                }
                current_section = None;
            }
        }
        
        // If no end was found, sections go to the end of file
        if artists_section_start.is_some() && artists_section_end.is_none() {
            artists_section_end = Some(lines.len());
        }
        if general_section_start.is_some() && general_section_end.is_none() {
            general_section_end = Some(lines.len());
        }
        
        // Add new artists
        if !new_artists.is_empty() {
            if let Some(start) = artists_section_start {
                let end = artists_section_end.unwrap_or(lines.len());
                
                // Find the insertion point (after section header, before next section)
                let mut insert_pos = start + 1;
                
                // Skip any existing comments or empty lines after the section header
                while insert_pos < end {
                    let line = lines[insert_pos].trim();
                    if line.starts_with('#') || line.is_empty() {
                        insert_pos += 1;
                    } else {
                        break;
                    }
                }
                
                // If we found existing tags, insert after them
                while insert_pos < end {
                    let line = lines[insert_pos].trim();
                    if !line.starts_with('#') && !line.is_empty() && !line.starts_with('[') {
                        insert_pos += 1;
                    } else {
                        break;
                    }
                }
                
                // Insert new artist tags
                let artist_count = new_artists.len();
                for tag in new_artists {
                    lines.insert(insert_pos, format!("{} # ({} posts)", tag.name, tag.post_count));
                    insert_pos += 1;
                }
                
                info!("Added {} new artist tags", artist_count);
            }
        }
        
        // Add new general tags
        if !new_general.is_empty() {
            if let Some(start) = general_section_start {
                let end = general_section_end.unwrap_or(lines.len());
                
                // Find the insertion point (after section header, before next section)
                let mut insert_pos = start + 1;
                
                // Skip any existing comments or empty lines after the section header
                while insert_pos < end {
                    let line = lines[insert_pos].trim();
                    if line.starts_with('#') || line.is_empty() {
                        insert_pos += 1;
                    } else {
                        break;
                    }
                }
                
                // If we found existing tags, insert after them
                while insert_pos < end {
                    let line = lines[insert_pos].trim();
                    if !line.starts_with('#') && !line.is_empty() && !line.starts_with('[') {
                        insert_pos += 1;
                    } else {
                        break;
                    }
                }
                
                // Insert new general tags
                let general_count = new_general.len();
                for tag in new_general {
                    lines.insert(insert_pos, format!("{} # ({} posts)", tag.name, tag.post_count));
                    insert_pos += 1;
                }
                
                info!("Added {} new general tags", general_count);
            }
        }
        
        // Write back to file
        let content = lines.join("\n");
        write(TAG_CACHE_FILE, content)?;
        
        info!("Appended {} tags to {}", tags.len(), TAG_CACHE_FILE);
        Ok(())
    }

    /// Check if tags.txt exists
    pub fn tags_file_exists() -> bool {
        Path::new(TAG_CACHE_FILE).exists()
    }

    /// Load existing tags from tags.txt (for reference)
    pub fn load_existing_tags() -> Result<Vec<String>> {
        if !Self::tags_file_exists() {
            return Ok(Vec::new());
        }
        
        let content = read_to_string(TAG_CACHE_FILE)?;
        let mut tags = Vec::new();
        let mut in_section = false;
        
        for line in content.lines() {
            let line = line.trim();
            
            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            
            // Check for section headers
            if line.starts_with('[') && line.ends_with(']') {
                // Only collect tags from artists, general sections (where actual tags are)
                in_section = matches!(line, "[artists]" | "[general]");
                continue;
            }
            
            // Only collect tags from relevant sections
            if in_section {
                // Extract tag name (before any comments)
                let tag_name = if let Some(pos) = line.find('#') {
                    line[..pos].trim().to_string()
                } else {
                    line.trim().to_string()
                };
                
                if !tag_name.is_empty() {
                    tags.push(tag_name);
                }
            }
        }
        
        Ok(tags)
    }

    /// Search for tags that match a query string with multiple search strategies
    pub fn search_tags(&self, query: &str, limit: usize) -> Result<Vec<ApiTag>> {
        info!("Searching for tags matching: {}", query);
        
        let mut all_found_tags = Vec::new();
        let mut seen_tags = std::collections::HashSet::new();
        
        // Strategy 1: Exact and prefix matches
        let exact_url = format!(
            "https://e621.net/tags.json?limit={}&search[name_matches]={}&search[hide_empty]=true&search[order]=count",
            limit, query
        );
        
        if let Ok(response) = self.request_sender.get_string(&exact_url) {
            if let Ok(tags) = serde_json::from_str::<Vec<ApiTag>>(&response) {
                for tag in tags {
                    if seen_tags.insert(tag.name.clone()) {
                        all_found_tags.push(tag);
                    }
                }
            }
        }
        
        // Strategy 2: Wildcard search for tags containing the query
        if all_found_tags.len() < limit {
            let wildcard_url = format!(
                "https://e621.net/tags.json?limit={}&search[name_matches]=*{}*&search[hide_empty]=true&search[order]=count",
                limit, query
            );
            
            if let Ok(response) = self.request_sender.get_string(&wildcard_url) {
                if let Ok(tags) = serde_json::from_str::<Vec<ApiTag>>(&response) {
                    for tag in tags {
                        if seen_tags.insert(tag.name.clone()) && all_found_tags.len() < limit {
                            all_found_tags.push(tag);
                        }
                    }
                }
            }
        }
        
        // Strategy 3: Search for related tags if we found at least one exact match
        if !all_found_tags.is_empty() && all_found_tags.len() < limit {
            // Use the first found tag to get related tags via its related_tags field
            // This requires getting full tag info including related_tags
            if let Some(first_tag) = all_found_tags.first() {
                let tag_detail_url = format!(
                    "https://e621.net/tags/{}.json",
                    first_tag.id
                );
                
                if let Ok(response) = self.request_sender.get_string(&tag_detail_url) {
                    // Parse the detailed tag response which includes related_tags
                    if let Ok(detailed_tag) = serde_json::from_str::<serde_json::Value>(&response) {
                        if let Some(related_tags_str) = detailed_tag.get("related_tags").and_then(|v| v.as_str()) {
                            // Parse related tags string (format: "tag1 count1 tag2 count2 ...")
                            let words: Vec<&str> = related_tags_str.split_whitespace().collect();
                            let mut related_tag_names = Vec::new();
                            
                            // Extract every other word (tag names, skip counts)
                            for chunk in words.chunks(2) {
                                if let Some(tag_name) = chunk.get(0) {
                                    if !seen_tags.contains(*tag_name) {
                                        related_tag_names.push(*tag_name);
                                        if related_tag_names.len() >= 10 { // Limit related tags
                                            break;
                                        }
                                    }
                                }
                            }
                            
                            // Fetch details for related tags
                            for tag_name in related_tag_names {
                                if all_found_tags.len() >= limit {
                                    break;
                                }
                                
                                let related_url = format!(
                                    "https://e621.net/tags.json?limit=1&search[name_matches]={}&search[hide_empty]=true",
                                    tag_name
                                );
                                
                                if let Ok(response) = self.request_sender.get_string(&related_url) {
                                    if let Ok(tags) = serde_json::from_str::<Vec<ApiTag>>(&response) {
                                        for tag in tags {
                                            if seen_tags.insert(tag.name.clone()) && all_found_tags.len() < limit {
                                                all_found_tags.push(tag);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Sort by post count (highest first)
        all_found_tags.sort_by(|a, b| b.post_count.cmp(&a.post_count));
        all_found_tags.truncate(limit);
        
        info!("Found {} matching and related tags for query: {}", all_found_tags.len(), query);
        Ok(all_found_tags)
    }
    
    /// Add a tag to the user's blacklist on e621
    pub fn add_to_remote_blacklist(&self, tag_name: &str) -> Result<()> {
        info!("Adding tag '{}' to remote blacklist...", tag_name);
        
        // Get current user info to update blacklist
        let username = Login::get().username();
        let user: UserEntry = self.request_sender.get_entry_from_appended_id(username, "user");
        
        // Add tag to existing blacklist
        let current_blacklist = user.blacklisted_tags.unwrap_or_default();
        let _new_blacklist = if current_blacklist.is_empty() {
            tag_name.to_string()
        } else {
            format!("{} {}", current_blacklist, tag_name)
        };
        
        // Note: This is a simplified example. 
        // The actual API call to update blacklist would require a PATCH request
        // to the user endpoint with the updated blacklist data
        warn!("Remote blacklist update not implemented yet - would add: {}", tag_name);
        warn!("You'll need to manually add '{}' to your e621 blacklist", tag_name);
        
        Ok(())
    }
    
    /// Interactive tag management workflow
    pub fn interactive_tag_management(&mut self) -> Result<()> {
        println!("\n--- Interactive Tag Management ---");
        println!("This will help you discover and manage tags for your downloads.");
        println!("You can search for tags, then choose to whitelist or blacklist them.");
        
        // First fetch the user's blacklist
        self.fetch_user_blacklist()?;
        
        // Load existing saved tags to avoid showing them again
        let mut existing_saved_tags = Self::load_existing_tags().unwrap_or_else(|e| {
            warn!("Could not load existing tags: {}", e);
            Vec::new()
        });
        
        if !existing_saved_tags.is_empty() {
            println!("Loaded {} already saved tags (will be filtered from search results)", existing_saved_tags.len());
        }
        
        let mut whitelisted_tags = Vec::new();
        let mut blacklisted_tags = Vec::new();
        let mut total_saved_tags = 0; // Track total tags saved during session
        
        loop {
            println!("\n--- Tag Search ---");
            if !whitelisted_tags.is_empty() {
                println!("Current session: {} tags whitelisted (unsaved)", whitelisted_tags.len());
            }
            
            // Get search query from user
            let query: String = Input::new()
                .with_prompt("Enter a tag or keyword to search for (or 'done' to save & finish)")
                .interact_text()
                .unwrap_or_else(|_| "done".to_string());
            
            if query.trim().to_lowercase() == "done" {
                break;
            }
            
            // Search for matching tags
            match self.search_tags(&query, 20) {
                Ok(mut tags) => {
                    // Filter out already saved tags
                    let initial_count = tags.len();
                    tags.retain(|tag| !existing_saved_tags.contains(&tag.name));
                    
                    if initial_count > tags.len() {
                        let filtered_count = initial_count - tags.len();
                        println!("Filtered out {} already saved tags", filtered_count);
                    }
                    
                    if tags.is_empty() {
                        if initial_count > 0 {
                            println!("All {} tags matching '{}' are already saved", initial_count, query);
                        } else {
                            println!("No tags found matching '{}'", query);
                        }
                        continue;
                    }
                    
                    // Show tags to user
                    println!("\nFound {} tags matching '{}':", tags.len(), query);
                    
                    let mut tag_options = Vec::new();
                    for (i, tag) in tags.iter().enumerate() {
                        let status = if self.is_blacklisted(&tag.name) {
                            "[BLACKLISTED]"
                        } else {
                            ""
                        };
                        
                        let display = format!(
                            "{} - {} posts {}", 
                            tag.name, 
                            tag.post_count,
                            status
                        );
                        tag_options.push(display);
                        
                        if i >= 19 { // Limit display to prevent overwhelming
                            break;
                        }
                    }
                    
                    tag_options.push("Search for different tags".to_string());
                    
                    // Let user select a tag
                    println!("\nAvailable options:");
                    for (i, option) in tag_options.iter().enumerate() {
                        println!("{}. {}", i + 1, option);
                    }
                    
                    let selections = loop {
                        let input: String = Input::new()
                            .with_prompt(&format!("Select options (e.g., 1,2,3|1-3) (1-{}):", tag_options.len()))
                            .interact_text()
                            .unwrap_or_else(|_| tag_options.len().to_string());

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

                        if ranges.iter().all(|&(start, end)| start > 0 && end <= tag_options.len() && start <= end) {
                            break ranges;
                        }
                        println!("Invalid selection. Please enter numbers between 1 and {}", tag_options.len());
                    };

                    let mut selected_tags = Vec::new();
                    let mut search_again = false;
                    
                    for (start, end) in selections {
                        for index in start..=end {
                            if index <= tags.len() {
                                selected_tags.push(&tags[index - 1]);
                            } else if index == tag_options.len() {
                                search_again = true;
                            }
                        }
                    }

                    if search_again || selected_tags.is_empty() {
                        continue; // "Search for different tags" selected
                    }

                    // Batch action selection
                    println!("\nSelected {} tags:", selected_tags.len());
                    for tag in &selected_tags {
                        println!("  - {} ({} posts)", tag.name, tag.post_count);
                    }
                    
                    let batch_actions = vec![
                        "Whitelist all (add all to tags.txt for downloading)",
                        "Blacklist all (add all to e621 blacklist to avoid)",
                        "Process individually (choose action for each tag)",
                        "Go back to search"
                    ];
                    
                    println!("\nBatch actions:");
                    for (i, action) in batch_actions.iter().enumerate() {
                        println!("{}. {}", i + 1, action);
                    }
                    
                    let batch_action = loop {
                        let input: String = Input::new()
                            .with_prompt(&format!("Select batch action (1-{}):", batch_actions.len()))
                            .interact_text()
                            .unwrap_or_else(|_| "4".to_string());
                        
                        if let Ok(num) = input.trim().parse::<usize>() {
                            if num > 0 && num <= batch_actions.len() {
                                break num - 1;
                            }
                        }
                        println!("Invalid selection. Please enter a number between 1 and {}", batch_actions.len());
                    };
                    
                    match batch_action {
                        0 => {
                            // Whitelist all
                            for selected_tag in selected_tags {
                                if !whitelisted_tags.iter().any(|t: &ApiTag| t.name == selected_tag.name) {
                                    whitelisted_tags.push(selected_tag.clone());
                                    existing_saved_tags.push(selected_tag.name.clone()); // Add to filter list
                                    println!("Added '{}' to whitelist", selected_tag.name);
                                } else {
                                    println!("'{}' is already whitelisted", selected_tag.name);
                                }
                            }
                            
                            // Ask if user wants to save immediately
                            let save_now = Input::new()
                                .with_prompt("Save these tags to tags.txt now? (y/n/later):")
                                .interact_text()
                                .unwrap_or_else(|_| "later".to_string());
                            
                            match save_now.trim().to_lowercase().as_str() {
                                "y" | "yes" => {
                                    match self.append_tags_to_file(&whitelisted_tags) {
                                        Ok(()) => {
                                            println!("Saved {} tags to tags.txt", whitelisted_tags.len());
                                            // Track the saved tags for session summary
                                            total_saved_tags += whitelisted_tags.len();
                                            // Clear the whitelisted_tags since they're now saved
                                            whitelisted_tags.clear();
                                        },
                                        Err(e) => {
                                            warn!("Failed to save tags: {}", e);
                                            println!("Failed to save tags. They will be saved when you exit.");
                                        }
                                    }
                                },
                                "n" | "no" => {
                                    println!("Tags NOT saved. Type 'done' when ready to save and exit.");
                                },
                                _ => {
                                    println!("Tags will be saved when you type 'done' to exit.");
                                }
                            }
                        },
                        1 => {
                            // Blacklist all
                            for selected_tag in selected_tags {
                                match self.add_to_remote_blacklist(&selected_tag.name) {
                                    Ok(()) => {
                                        blacklisted_tags.push(selected_tag.clone());
                                        println!("Added '{}' to blacklist", selected_tag.name);
                                    },
                                    Err(e) => {
                                        warn!("Failed to add '{}' to blacklist: {}", selected_tag.name, e);
                                    }
                                }
                            }
                        },
                        2 => {
                            // Process individually
                            for selected_tag in selected_tags {
                                let actions = vec![
                                    "Whitelist (add to tags.txt for downloading)",
                                    "Blacklist (add to e621 blacklist to avoid)",
                                    "Show tag info",
                                    "Skip this tag"
                                ];
                                
                                println!("\nWhat would you like to do with '{}'?", selected_tag.name);
                                for (i, action) in actions.iter().enumerate() {
                                    println!("{}. {}", i + 1, action);
                                }
                                
                                let action = loop {
                                    let input: String = Input::new()
                                        .with_prompt(&format!("Select action (1-{}):", actions.len()))
                                        .interact_text()
                                        .unwrap_or_else(|_| "4".to_string());
                                    
                                    if let Ok(num) = input.trim().parse::<usize>() {
                                        if num > 0 && num <= actions.len() {
                                            break num - 1;
                                        }
                                    }
                                    println!("Invalid selection. Please enter a number between 1 and {}", actions.len());
                                };
                                
                                match action {
                                    0 => {
                                        // Whitelist
                                        if !whitelisted_tags.iter().any(|t: &ApiTag| t.name == selected_tag.name) {
                                            whitelisted_tags.push(selected_tag.clone());
                                            existing_saved_tags.push(selected_tag.name.clone()); // Add to filter list
                                            println!("Added '{}' to whitelist", selected_tag.name);
                                        } else {
                                            println!("'{}' is already whitelisted", selected_tag.name);
                                        }
                                    },
                                    1 => {
                                        // Blacklist
                                        match self.add_to_remote_blacklist(&selected_tag.name) {
                                            Ok(()) => {
                                                blacklisted_tags.push(selected_tag.clone());
                                                println!("Added '{}' to blacklist", selected_tag.name);
                                            },
                                            Err(e) => {
                                                warn!("Failed to add to blacklist: {}", e);
                                            }
                                        }
                                    },
                                    2 => {
                                        // Show info
                                        println!("\nTag Information:");
                                        println!("   Name: {}", selected_tag.name);
                                        println!("   Posts: {}", selected_tag.post_count);
                                        println!("   Category: {}", match TagCategory::from_i32(selected_tag.category) {
                                            Some(cat) => format!("{:?}", cat),
                                            None => "Unknown".to_string(),
                                        });
                                        println!("   Blacklisted: {}", if self.is_blacklisted(&selected_tag.name) { "Yes" } else { "No" });
                                    },
                                    _ => {
                                        // Skip this tag
                                        println!("Skipped '{}'", selected_tag.name);
                                    }
                                }
                            }
                        },
                        3 => {
                            // Go back to search
                            continue;
                        },
                        _ => {
                            // Go back to search
                            continue;
                        }
                    }
                },
                Err(e) => {
                    warn!("Failed to search tags: {}", e);
                }
            }
        }
        
        // Save whitelisted tags to tags.txt
        if !whitelisted_tags.is_empty() {
            let confirm_save = loop {
                let input: String = Input::new()
                    .with_prompt(&format!("Save {} whitelisted tags to tags.txt? (y/n):", whitelisted_tags.len()))
                    .interact_text()
                    .unwrap_or_else(|_| "y".to_string());
                
                match input.trim().to_lowercase().as_str() {
                    "y" | "yes" | "" => break true,
                    "n" | "no" => break false,
                    _ => println!("Please enter 'y' for yes or 'n' for no."),
                }
            };
            
            if confirm_save {
                self.append_tags_to_file(&whitelisted_tags)?;
                println!("Saved {} tags to tags.txt", whitelisted_tags.len());
                total_saved_tags += whitelisted_tags.len();
            }
        }
        
        // Summary
        println!("\nSession Summary:");
        let total_whitelisted = whitelisted_tags.len() + total_saved_tags;
        println!("   Whitelisted: {} tags ({})", total_whitelisted, 
                 if total_saved_tags > 0 {
                     format!("{} saved to tags.txt", total_saved_tags)
                 } else {
                     "not saved".to_string()
                 });
        println!("   Blacklisted: {} tags", blacklisted_tags.len());
        
        Ok(())
    }
    
    /// Refresh tags - fetch new ones and update the file
    pub fn refresh_tags(&mut self, limit: usize, min_post_count: i32) -> Result<()> {
        // First fetch the user's blacklist
        self.fetch_user_blacklist()?;
        
        // Then fetch and filter tags
        let tags = self.fetch_popular_tags(limit, min_post_count)?;
        self.save_tags_to_file(&tags)?;
        Ok(())
    }
}
