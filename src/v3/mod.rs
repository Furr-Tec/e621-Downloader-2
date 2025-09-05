//! V3 module for E621 Downloader
//! Contains the next generation of the downloader with improved architecture

pub mod config_loader;
pub mod e621_validator;
pub mod cli;
pub mod cli_runner;
pub mod orchestration;
pub mod orchestration_runner;
pub mod query_planner;
pub mod query_planner_runner;
pub mod download_engine;
pub mod download_engine_runner;
pub mod file_processor;
pub mod file_processor_runner;
pub mod metadata_store;
pub mod metadata_store_runner;
pub mod blacklist_handler;
pub mod blacklist_handler_runner;
pub mod disk_verifier;
pub mod disk_verifier_runner;
pub mod system_monitor;
pub mod system_monitor_runner;
pub mod logger;
pub mod logger_runner;
pub mod session_manager;
pub mod session_manager_runner;
pub mod session_recovery;
pub mod adaptive_concurrency;
pub mod directory_organizer;
pub mod hash_manager;
pub mod database;

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
    DownloadJob, QueryPlan,
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

// Re-export blacklist handler functionality
pub use blacklist_handler::{
    BlacklistHandler, BlacklistHandlerError, BlacklistHandlerResult,
    BlacklistedReject, init_blacklist_handler,
};

// Re-export disk verifier functionality
pub use disk_verifier::{
    DiskVerifier, DiskVerifierError, DiskVerifierResult,
    FileStatus, FileVerificationResult, FileWatchEvent,
    init_disk_verifier,
};

// Re-export system monitor functionality
pub use system_monitor::{
    SystemMonitor, SystemMonitorError, SystemMonitorResult,
    ResourceThresholds, ResourceStatus, ResourceMetrics, ResourceEvent,
    init_system_monitor,
};

// Re-export logger functionality
pub use logger::{
    Logger, LoggerError, LoggerResult,
    OperationStatus, LogEntryType, LogEntry,
    init_logger,
};

// Re-export session manager functionality
pub use session_manager::{
    SessionManager, SessionManagerError, SessionManagerResult,
    SessionState, init_session_manager,
};

// Re-export session recovery functionality
pub use session_recovery::{
    SessionRecovery, SessionRecoveryError, SessionRecoveryResult,
    JobStatus, PersistentJob, init_session_recovery,
};

// Re-export adaptive concurrency functionality
pub use adaptive_concurrency::{
    AdaptiveConcurrency, ConcurrencyError, ConcurrencyResult,
    PerformanceMetrics, init_adaptive_concurrency,
};

// Re-export directory organizer functionality
pub use directory_organizer::{
    DirectoryOrganizer, DirectoryOrganizerError, DirectoryOrganizerResult,
    OrganizationStrategy, ContentType, sanitize_filename, init_directory_organizer,
};

// Re-export hash manager functionality
pub use hash_manager::{
    HashManager, HashManagerError, HashManagerResult,
    DownloadRecord, HashVerification, DownloadStatistics, init_hash_manager,
};

// Re-export database functionality
pub use database::{
    Database, DatabaseError, DatabaseResult,
    DownloadRecord as DbDownloadRecord, DownloadStatistics as DbDownloadStatistics,
    init_database,
};

// Re-export e621 validator functionality
pub use e621_validator::{
    ValidationError, ValidationResult,
    validate_e621_credentials, test_config_credentials,
};

// Re-export CLI functionality
pub use cli::run_cli;
