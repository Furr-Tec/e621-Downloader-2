//! V3 module for E621 Downloader
//! Contains the next generation of the downloader with improved architecture

pub mod config_loader;
pub mod example;
pub mod cli;
pub mod cli_example;
pub mod orchestration;
pub mod orchestration_example;
pub mod query_planner;
pub mod query_planner_example;
pub mod download_engine;
pub mod download_engine_example;
pub mod file_processor;
pub mod file_processor_example;
pub mod metadata_store;
pub mod metadata_store_example;

// Re-export commonly used types for convenience
pub use config_loader::{
    AppConfig, E621Config, RulesConfig,
    ConfigManager, ConfigError, ConfigResult, ConfigReloadEvent,
    init_config,
};

// Re-export orchestration functionality
pub use orchestration::{
    Orchestrator, OrchestratorError, OrchestratorResult,
    QueryQueue, Query, QueryStatus, QueryTask,
    Scheduler, Conductor,
    init_orchestrator,
};

// Re-export query planner functionality
pub use query_planner::{
    QueryPlanner, QueryPlannerError, QueryPlannerResult,
    Post, PostFile, PostTags, PostScore,
    DownloadJob, QueryPlan, HashStore,
    init_query_planner,
};

// Re-export download engine functionality
pub use download_engine::{
    DownloadEngine, DownloadError, DownloadResult,
    DownloadStatus, DownloadTask, DownloadStats,
    DownloadEngineConfig, init_download_engine,
};

// Re-export file processor functionality
pub use file_processor::{
    FileProcessor, FileProcessorError, FileProcessorResult,
    FileHash, ProcessedFile, FileHashStore,
    init_file_processor,
};

// Re-export metadata store functionality
pub use metadata_store::{
    MetadataStore, MetadataStoreError, MetadataStoreResult,
    PostMetadata, init_metadata_store,
};

// Re-export CLI functionality
pub use cli::run_cli;
