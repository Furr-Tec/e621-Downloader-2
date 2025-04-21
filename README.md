# E621 Downloader
A modern fork and evolution of the original `e621_downloader`, redesigned for improved organization, reliability, and artist-focused archiving.
(Link: https://github.com/McSib/e621_downloader)

## ️New Features & Enhancements

### 📁 Directory Management System

Content is now saved in a structured hierarchy to make navigation and sorting easier:
The `e621_downloader` is a low-level, close-to-hardware program meant to download a large number of images at a fast pace. It can handle bulk posts, single posts, sets, and pools via a custom easy-to-read language that I made.

Downloads/ ├── Artists/ │ └── {artist_name}/ │ └── content ├── Tags/ │ └── {tag_name}/ │ └── organized by artist ├── Pools/ │ └── {pool_name}/ │ └── organized by artist
- General searches organize by artist
- Pools and Sets generate subfolders per artist
- Single post downloads also sort by artist

### 🧠 Automatic Artist Detection

- Artist names are extracted from post tags
- Artist folders are created dynamically
- Artist info is retained across downloads for consistency

### 🧱 Enhanced Internal Structures

#### GrabbedPost
- `save_directory: Option<PathBuf>` — tracks the post’s storage location
- `artist: Option<String>` — captures the associated artist
- Methods:
  - `set_save_directory()`
  - `set_artist()`

#### PostCollection
- `base_directory: Option<PathBuf>` — root location for grouped content
- `initialize_directories()` — auto-creates folder layout for:
  - Artist
  - Tags
  - Pools
  - Sets

This overhaul enables seamless browsing, better indexing, and batch-friendly archiving workflows.

---

## 🎯 Goal

Maintain an artist-first, pool-compatible downloader that's efficient, simple to use, and long-term reliable for archival and personal collection use.

---

## 🐾 About E621/E926

E621 is a mature image board powered by the Ouroboros platform, with e926 being its safe-for-work sibling. These platforms host over 2.9 million+ pieces of content. This tool provides a way to bulk download, archive, and curate artwork from these platforms efficiently.

---

## 🛠️ To-Do List

- [ ] Add a menu system for config editing, tag control, and download management
- [ ] Transition tag file format to JSON for better interoperability
- [ ] Further refactor and optimize codebase for speed and clarity