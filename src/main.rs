//! PostgreSQL Replication Checker - Rust Edition
//!
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and streams database changes to HTTP endpoints or other sinks in real-time.
//!
//! ## Architecture Overview
//!
//! This application implements the PostgreSQL logical replication protocol to:
//! 1. Connect to a PostgreSQL database as a replication client
//! 2. Create or use existing replication slots and publications
//! 3. Stream WAL (Write-Ahead Log) changes as they happen
//! 4. Parse and convert database changes into structured events
//! 5. Send events to configured sinks (HTTP endpoints, Hook0, or STDOUT)
//!
//! ## Key Concepts
//!
//! - **Logical Replication**: PostgreSQL's mechanism for replicating data at the row level
//! - **Replication Slot**: A mechanism to track which changes have been consumed
//! - **Publication**: Defines which tables/changes are published for replication
//! - **WAL**: Write-Ahead Log, PostgreSQL's transaction log containing all changes
//! - **LSN**: Log Sequence Number, a unique identifier for positions in the WAL
//!
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

// Module declarations - organized by functional areas
mod core;          // Core functionality: configuration, errors
mod protocol;      // PostgreSQL protocol handling
mod replication;  // Replication server and state management
mod events;        // Event processing and sinks
mod utils;         // Utility functions for PostgreSQL integration

// Import the core types and functionality we need
use crate::core::{ReplicationConfig, ReplicationResult};
use crate::replication::ReplicationServer;
use clap::Parser;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};


/// Command line arguments structure using clap for parsing
///
/// This structure defines the command-line interface for the application.
/// Currently, it only accepts database connection parameters, but most
/// configuration is done through environment variables for better
/// containerization and security.
#[derive(Parser, Debug)]
#[command(
    name = "walpipe",
    about = "PostgreSQL Logical Replication Checker in Rust",
    version = "0.1.0"
)]
struct Args {
    /// Database connection parameters (space-separated key=value pairs)
    ///
    /// This accepts traditional PostgreSQL connection string parameters.
    /// However, in practice, most users should set the DATABASE_URL
    /// environment variable instead for consistency.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    connection_params: Vec<String>,
}

/// Application entry point
///
/// This is the main function that initializes the application and starts the replication server.
/// It uses the `tokio` async runtime to handle asynchronous operations.
///
/// # Configuration
///
/// The application is configured primarily through environment variables:
/// - `DATABASE_URL`: PostgreSQL connection string (required)
/// - `SLOT_NAME`: Replication slot name (defaults to "sub")
/// - `PUB_NAME`: Publication name (defaults to "pub")
/// - `EVENT_SINK`: Event sink type - "http", "hook0", or "stdout" (optional, defaults to "stdout")
/// - `HTTP_ENDPOINT_URL`: URL for HTTP event sink (optional, required when using "http" service)
/// - `HOOK0_API_URL`: Hook0 API URL (optional, required when using "hook0" service)
/// - `HOOK0_APPLICATION_ID`: Hook0 application UUID (optional, required when using "hook0" service)
/// - `HOOK0_API_TOKEN`: Hook0 API token (optional, required when using "hook0" service)
///
/// # Returns
///
/// Returns `Ok(())` on successful completion or an error if replication fails.
#[tokio::main]
async fn main() -> ReplicationResult<()> {
    // Initialize structured logging with tracing
    // This sets up logging levels and output formatting
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .init();

    // Create a shutdown signal that can be shared across the application
    let shutdown_signal = Arc::new(AtomicBool::new(false));

    // Set up signal handling for graceful shutdown
    let signal_handler_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        // Wait for SIGTERM or SIGINT signals
        #[cfg(unix)]
        {
            use std::sync::atomic::Ordering;

            use tokio::signal::unix::{SignalKind, signal};
            use tracing::warn;
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {
                    warn!("Received SIGTERM signal, initiating graceful shutdown");
                    signal_handler_shutdown.store(true, Ordering::SeqCst);
                }
                _ = sigint.recv() => {
                    warn!("Received SIGINT signal, initiating graceful shutdown");
                    signal_handler_shutdown.store(true, Ordering::SeqCst);
                }
            }
        }

        #[cfg(not(unix))]
        {
            // For Windows, we'll use Ctrl-C
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to setup Ctrl-C handler");
            warn!("Received Ctrl-C, initiating graceful shutdown");
            signal_handler_shutdown.store(true, Ordering::SeqCst);
        }
    });

    // Load replication configuration from environment variables
    // The config module handles all environment variable loading and validation
    let config = ReplicationConfig::from_env()?;

    info!("Configuration loaded successfully");
    info!("Slot name: {}", config.slot_name);
    info!("Publication name: {}", config.publication_name);
    info!("Event sink type: {}", config.event_sink_type());

    let mut retry_delay = Duration::from_secs(5);
    let max_retry_delay = Duration::from_secs(60);

    loop {
        if shutdown_signal.load(Ordering::SeqCst) {
            info!("Shutting down before reconnect attempt");
            return Ok(());
        }

        match run_replication_server(config.clone(), shutdown_signal.clone()).await {
            Ok(()) => {
                info!("Replication server completed successfully");
                return Ok(());
            }
            Err(e) => {
                if shutdown_signal.load(Ordering::SeqCst) {
                    info!("Shutting down after error: {}", e);
                    return Ok(());
                }

                error!(
                    "Replication server failed: {}. Reconnecting in {}s...",
                    e,
                    retry_delay.as_secs()
                );

                sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(max_retry_delay);
            }
        }

        retry_delay = Duration::from_secs(5);
    }
}

// Include test module for graceful shutdown functionality
#[cfg(test)]
mod test_graceful_shutdown;

/// Helper function to run the replication server
///
/// This function encapsulates the core replication logic:
/// 1. Creates a new ReplicationServer instance with the provided configuration
/// 2. Identifies the PostgreSQL system (verifies connection and gets system info)
/// 3. Creates replication slot and starts the replication process
/// 4. Handles graceful shutdown when signaled
///
/// # Arguments
///
/// * `config` - The replication configuration containing connection details and settings
/// * `shutdown_signal` - Shared atomic flag to signal shutdown
///
/// # Returns
///
/// Returns `Ok(())` when replication completes or an error if any step fails
async fn run_replication_server(
    config: ReplicationConfig,
    shutdown_signal: Arc<AtomicBool>,
) -> ReplicationResult<()> {
    let mut server = ReplicationServer::new(config, shutdown_signal).await?;

    server.identify_system()?;

    server.create_replication_slot_and_start().await?;

    Ok(())
}
