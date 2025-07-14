use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use anyhow::{Result, Context};
use rayon::ThreadPoolBuilder;
use walkdir::WalkDir;
use indicatif::ProgressBar;
use log::{debug, warn};
use num_cpus;

const WALK_BATCH_SIZE: usize = 1000; // Process files in batches for locking efficiency

/// Walks the provided root directories in parallel to collect all file paths.
///
/// Uses a rayon ThreadPool limited to 80% of logical CPUs.
/// Updates the provided ProgressBar and collects results into the shared Mutex vector.
pub fn walk_directories_parallel(
    roots: &[PathBuf],
    progress: Arc<ProgressBar>,
    collected_files: Arc<Mutex<Vec<PathBuf>>>,
) -> Result<()> {
    // Determine thread count (80% of logical cores, minimum 1)
    let num_cores = num_cpus::get();
    let walk_threads = ((num_cores as f64 * 0.8).floor() as usize).max(1);
    debug!("Initializing parallel directory walk with {} threads", walk_threads);

    let pool = ThreadPoolBuilder::new()
        .num_threads(walk_threads)
        .thread_name(|i| format!("walk-worker-{}", i))
        .build()
        .context("Failed to create directory walk thread pool")?;

    let file_count = Arc::new(AtomicUsize::new(0));

    pool.scope(|s| {
        for root_dir in roots {
            if !root_dir.exists() || !root_dir.is_dir() {
                warn!("Skipping non-existent or invalid directory: {}", root_dir.display());
                continue;
            }

            let root_path = root_dir.clone(); // Clone for the closure
            let progress_clone = Arc::clone(&progress);
            let files_clone = Arc::clone(&collected_files);
            let count_clone = Arc::clone(&file_count);

            s.spawn(move |_| {
                let section_name = root_path.file_name().unwrap_or_default().to_string_lossy();
                let mut entry_batch = Vec::with_capacity(WALK_BATCH_SIZE);
                let walker = WalkDir::new(&root_path).follow_links(true).into_iter();

                progress_clone.set_message(format!("Scanning {}...", section_name));

                for entry_result in walker {
                    match entry_result {
                        Ok(entry) => {
                            let path = entry.path();
                            if path.is_file() {
                                entry_batch.push(path.to_path_buf());

if entry_batch.len() >= WALK_BATCH_SIZE {
    let batch_len = entry_batch.len();
    match files_clone.lock() {
        Ok(mut files) => files.extend(entry_batch.drain(..)),
        Err(e) => {
            warn!("Failed to acquire lock for file batch: {}. Recovering lock.", e);
            let mut files = e.into_inner();
            files.extend(entry_batch.drain(..));
        }
    }
    let new_count = count_clone.fetch_add(batch_len, Ordering::SeqCst) + batch_len;
    progress_clone.set_position(new_count as u64);
}
                            }
                        }
                        Err(err) => {
                            warn!("Error accessing path in {}: {}", section_name, err);
                        }
                    }
                }

// Process any remaining entries in the batch
if !entry_batch.is_empty() {
    let batch_len = entry_batch.len();
    match files_clone.lock() {
        Ok(mut files) => files.extend(entry_batch),
        Err(e) => {
            warn!("Failed to acquire lock for final batch: {}. Recovering lock.", e);
            let mut files = e.into_inner();
            files.extend(entry_batch);
        }
    }
    let new_count = count_clone.fetch_add(batch_len, Ordering::SeqCst) + batch_len;
    progress_clone.set_position(new_count as u64);
}
                 let final_count = count_clone.load(Ordering::SeqCst);
                 progress_clone.set_message(format!("{} scan complete. Found {} files total.", section_name, final_count));
            });
        }
    }); // Scope ensures all spawned tasks complete

    let total_files_found = file_count.load(Ordering::SeqCst);
    progress.set_length(total_files_found as u64); // Set final length for progress bar
    progress.finish_with_message(format!("Directory walk complete. Found {} files.", total_files_found));

    Ok(())
}

