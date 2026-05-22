//! PostgreSQL replication server implementation
//!
//! Main server that handles the complete logical replication lifecycle:
//! - Database connection and validation
//! - Replication slot and publication verification
//! - WAL streaming and message processing
//! - Event delivery to configured sinks

use crate::core::config::ReplicationConfig;
use crate::core::errors::ReplicationResult;
use crate::events::sink::event_history::{EventHistoryEntry, EventHistoryRecorder};
use crate::events::{EventSink, EventSinkRegistry};
use crate::protocol::buffer::{BufferReader, BufferWriter};
use crate::protocol::messages::*;
use crate::protocol::parser::MessageParser;
use crate::utils::connection::{CopyData, PGConnection};
use crate::utils::timestamp::system_time_to_postgres_timestamp;
use libpq_sys::ExecStatusType;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tracing::{debug, error, info, warn};

/// Newtype so we can hand a bare libpq socket to [`AsyncFd`] without taking
/// ownership of the descriptor — the underlying `PGconn` retains responsibility
/// for closing it on `PQfinish`.
struct PgSocket(RawFd);

impl AsRawFd for PgSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Main replication server that manages the logical replication connection
///
/// This struct coordinates all aspects of the replication process, maintaining
/// the connection state, processing messages, and ensuring reliable delivery
/// of database changes to the configured event sink.
pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
    event_sink: Option<Arc<dyn EventSink + Send + Sync>>,
    event_history_recorder: Option<Arc<EventHistoryRecorder>>,
    event_history_entry: Option<EventHistoryEntry>,
    shutdown_signal: Arc<AtomicBool>,
}

impl ReplicationServer {
    /// Creates a new replication server with the given configuration
    ///
    /// Establishes database connection and initializes the appropriate event sink
    /// based on configuration (Hook0, HTTP endpoint, or STDOUT fallback).
    pub async fn new(
        config: ReplicationConfig,
        shutdown_signal: Arc<AtomicBool>,
    ) -> ReplicationResult<Self> {
        info!("Connecting to database: {}", config.connection_string);
        let connection = PGConnection::connect(&config.connection_string)?;
        info!("Successfully connected to database server");

        // Create event_history recorder for hook0 sink
        let (event_history_recorder, event_history_entry) =
            if config.uses_hook0_sink() {
                let (recorder, entry) = EventHistoryRecorder::new(
                    &config.connection_string_without_replication_param(),
                    100,
                    Duration::from_secs(10),
                )
                .await
                .map_err(|e| {
                    crate::core::errors::ReplicationError::config(format!(
                        "Failed to create event_history recorder: {}",
                        e
                    ))
                })?;
                (Some(Arc::new(recorder)), Some(entry))
            } else {
                (None, None)
            };

        // Configure event sink based on configuration
        let sink_type = config.event_sink_type();
        info!("Initializing {} event sink", sink_type);

        let event_sink = match EventSinkRegistry::create_sink(
            sink_type,
            &config,
            event_history_recorder.clone(),
        ) {
            Ok(sink) => {
                info!("Successfully initialized {} event sink", sink_type);
                Some(sink)
            }
            Err(e) => {
                error!("Failed to initialize {} event sink: {}", sink_type, e);
                return Err(crate::core::errors::ReplicationError::protocol(
                    e.to_string(),
                ));
            }
        };

        Ok(Self {
            connection,
            config,
            state: ReplicationState::new(),
            event_sink,
            event_history_recorder,
            event_history_entry,
            shutdown_signal,
        })
    }

    /// Verifies that PostgreSQL is configured for logical replication
    ///
    /// Checks that the wal_level setting is 'logical', which is required
    /// for logical replication to function.
    pub fn check_wal_level(&self) -> ReplicationResult<()> {
        info!("Checking wal_level setting");

        let result = self.connection.exec("SHOW wal_level;")?;
        if !result.is_ok() {
            warn!("Failed to check wal_level, status: {:?}", result.status());
            return Err(crate::core::errors::ReplicationError::protocol(
                "Failed to check wal_level",
            ));
        }

        let wal_level = result.getvalue(0, 0);
        match wal_level {
            Some(level) => {
                info!("Current wal_level: {}", level);
                if level == "logical" {
                    info!("wal_level is correctly set to 'logical'");
                    Ok(())
                } else {
                    Err(crate::core::errors::ReplicationError::protocol(
                        "wal_level is not set to 'logical'. Please set wal_level to 'logical' in postgresql.conf and restart the PostgreSQL server.",
                    ))
                }
            }
            None => {
                warn!("Could not retrieve wal_level value");
                Err(crate::core::errors::ReplicationError::protocol(
                    "Could not retrieve wal_level value",
                ))
            }
        }
    }

    /// Identifies the PostgreSQL system and verifies replication support
    ///
    /// Executes IDENTIFY_SYSTEM to verify the connection supports replication
    /// and retrieves system information including timeline and WAL position.
    pub fn identify_system(&self) -> ReplicationResult<()> {
        debug!("Identifying system");
        match self.connection.exec("IDENTIFY_SYSTEM") {
            Ok(result) => {
                if !result.is_ok() {
                    return Err(crate::core::errors::ReplicationError::protocol(format!(
                        "IDENTIFY_SYSTEM failed: {:?}",
                        result.status()
                    )));
                }

                info!(
                    "IDENTIFY_SYSTEM succeeded: {:?}, system_id: {:?}, timeline: {:?}, xlogpos: {:?}, dbname: {:?}",
                    result.status(),
                    result.getvalue(0, 0),
                    result.getvalue(0, 1),
                    result.getvalue(0, 2),
                    result.getvalue(0, 3)
                );
            }
            Err(err) => {
                return Err(crate::core::errors::ReplicationError::protocol(format!(
                    "IDENTIFY_SYSTEM failed: {}",
                    err
                )));
            }
        }

        info!("System identification successful");
        Ok(())
    }

    /// Orchestrates the complete replication setup process
    ///
    /// Performs all necessary validation and setup before starting replication:
    /// 1. Verifies wal_level is 'logical'
    /// 2. Checks replication slot exists
    /// 3. Verifies publication exists
    /// 4. Starts the replication stream
    pub async fn create_replication_slot_and_start(&mut self) -> ReplicationResult<()> {
        self.check_wal_level()?;
        self.check_replication_slot()?;
        self.check_publication()?;

        self.start_replication().await?;

        Ok(())
    }

    fn check_replication_slot(&self) -> ReplicationResult<()> {
        // Check if the replication slot already exists
        let check_slot_sql = format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{}';",
            self.config.slot_name
        );

        let result = self.connection.exec(&check_slot_sql)?;
        if !result.is_ok() {
            return Err(crate::core::errors::ReplicationError::protocol(format!(
                "Failed to check existing replication slots: {:?}",
                result.status()
            )));
        }

        if result.ntuples() == 0 {
            return Err(crate::core::errors::ReplicationError::protocol(format!(
                "Replication slot '{}' does not exist. Please create it manually using the following SQL command:\n\nCREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput NOEXPORT_SNAPSHOT;\n",
                self.config.slot_name, self.config.slot_name
            )));
        }

        Ok(())
    }

    fn check_publication(&self) -> ReplicationResult<()> {
        // Check if the publication already exists and is for the correct table if specified
        debug!(
            "Checking if publication '{}' exists",
            self.config.publication_name
        );
        let check_pub_sql = format!(
            "SELECT * FROM pg_publication WHERE pubname = '{}';",
            self.config.publication_name
        );
        let result = self.connection.exec(&check_pub_sql)?;
        if !result.is_ok() {
            return Err(crate::core::errors::ReplicationError::protocol(format!(
                "Failed to check existing publications: {:?}",
                result.status()
            )));
        }

        if result.ntuples() == 0 {
            return Err(crate::core::errors::ReplicationError::protocol(format!(
                "Publication '{}' does not exist. Please create it manually using the following SQL command:\n\nCREATE PUBLICATION \"{}\" FOR TABLE {};\n\nor\n\nCREATE PUBLICATION \"{}\" FOR ALL TABLES;\n\ndepending on your configuration.",
                self.config.publication_name,
                self.config.publication_name,
                "<your_table_name>",
                self.config.publication_name
            )));
        }

        Ok(())
    }

    async fn start_replication(&mut self) -> ReplicationResult<()> {
        let start_replication_sql = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 (proto_version '2', streaming 'on', publication_names '{}');",
            self.config.slot_name, self.config.publication_name
        );

        info!(
            "Starting replication with publication: {}, executing SQL: {}",
            self.config.publication_name, start_replication_sql
        );

        let result = self.connection.exec(&start_replication_sql)?;
        if result.status() != ExecStatusType::PGRES_COPY_BOTH {
            return Err(crate::core::errors::ReplicationError::protocol(format!(
                "Failed to start replication: {:?}",
                result.status()
            )));
        }

        info!("Started receiving data from database server");
        self.replication_loop().await?;

        Ok(())
    }

    async fn replication_loop(&mut self) -> ReplicationResult<()> {
        // Switch the connection into non-blocking mode and register its socket
        // with tokio's reactor so we can `await` data instead of blocking the
        // worker thread inside libpq.
        self.connection.set_nonblocking(true)?;
        let socket = PgSocket(self.connection.socket()?);
        let async_fd = AsyncFd::with_interest(socket, Interest::READABLE)?;

        // Cap how long we'll park on the socket before checking the shutdown
        // flag and the feedback timer again. Stay well under the feedback
        // interval so periodic feedback still fires when WAL is quiet.
        let idle_wakeup = Duration::from_secs(
            self.config
                .feedback_interval_secs
                .saturating_sub(1)
                .max(1),
        );

        let stop_reason = 'outer: loop {
            if self.shutdown_signal.load(Ordering::SeqCst) {
                break "shutdown signal";
            }

            self.check_and_send_feedback()?;

            // Drain everything libpq has already buffered before going back
            // to sleep on the socket.
            loop {
                match self.connection.get_copy_data()? {
                    CopyData::Done => break 'outer "stream ended",
                    CopyData::WouldBlock => break,
                    CopyData::Row(data) => {
                        if data.is_empty() {
                            continue;
                        }
                        debug!(
                            "PQgetCopyData returned: {}, data len: {}",
                            data[0] as char,
                            data.len()
                        );

                        match data[0] as char {
                            'k' => self.process_keepalive_message(&data)?,
                            'w' => {
                                self.process_wal_message(&data).await?;
                                if self.shutdown_signal.load(Ordering::SeqCst) {
                                    break 'outer "shutdown after WAL message";
                                }
                            }
                            _ => warn!(
                                "Received unknown message type: {}",
                                data[0] as char
                            ),
                        }
                    }
                }
            }

            // Wait for more data, but wake up periodically to re-check the
            // shutdown flag and send keepalive feedback.
            match tokio::time::timeout(idle_wakeup, async_fd.readable()).await {
                Err(_) => continue, // periodic wakeup
                Ok(Ok(mut guard)) => {
                    self.connection.consume_input()?;
                    guard.clear_ready();
                }
                Ok(Err(e)) => return Err(crate::core::errors::ReplicationError::from(e)),
            }
        };

        info!("Replication loop stopping: {}", stop_reason);
        self.perform_graceful_shutdown().await?;

        info!("Replication loop completed");
        Ok(())
    }

    fn process_keepalive_message(&mut self, data: &[u8]) -> ReplicationResult<()> {
        if data.len() < 18 {
            return Err(crate::core::errors::ReplicationError::protocol(
                "Keepalive message too short",
            ));
        }

        debug!("Processing keepalive message");

        let reader = BufferReader::new(data);

        let k: KeepaliveMessage = reader.try_into()?;

        if k.reply_requested {
            debug!("Server requested feedback in keepalive");
            self.send_feedback()?;
            self.connection.flush()?;
        }
        Ok(())
    }

    async fn process_wal_message(&mut self, data: &[u8]) -> ReplicationResult<()> {
        let reader = BufferReader::new(data);

        let w = XLogDataMessage::try_from(reader)?;

        if w.data.is_empty() {
            return Err(crate::core::errors::ReplicationError::protocol(
                "WAL message has no data",
            ));
        }

        if w.data_start > 0 {
            self.state.update_lsn(w.data_start);
        }

        // Parse the actual logical replication message
        match MessageParser::parse_wal_message(&w.data) {
            Ok(message) => {
                self.process_replication_message(message).await?;
            }
            Err(e) => {
                error!("Failed to parse replication message: {}", e);
                return Err(e);
            }
        }

        self.send_feedback()?;
        Ok(())
    }

    async fn process_replication_message(
        &mut self,
        message: ReplicationMessage,
    ) -> ReplicationResult<()> {
        // Handle relation messages by storing schema information
        if let ReplicationMessage::Relation { relation } = &message {
            self.state.add_relation(relation.clone());
        }

        // Send event to configured sink if available
        if let Some(ref event_sink) = self.event_sink {
            debug!("Sending event to event sink: {:?}", message);

            match event_sink.send_event(&message).await {
                Ok(()) => {
                    debug!(
                        "Successfully sent event to sink for LSN: {:x}",
                        self.state.received_lsn
                    );
                    self.state.update_applied_lsn(self.state.received_lsn);
                }
                Err(e) => {
                    error!("Failed to send event to event sink: {}", e);
                    return Err(crate::core::errors::ReplicationError::protocol(format!(
                        "Event sink failed: {}",
                        e
                    )));
                }
            }
        } else {
            self.state.update_applied_lsn(self.state.received_lsn);
        }

        Ok(())
    }

    fn send_feedback(&mut self) -> ReplicationResult<()> {
        debug!("Sending feedback to server");

        let now = SystemTime::now();
        let timestamp = system_time_to_postgres_timestamp(now);
        let mut reply_buf = [0u8; 34];
        let bytes_written = {
            let mut writer = BufferWriter::new(&mut reply_buf);

            writer.write_u8(b'r')?;
            writer.write_u64(self.state.received_lsn)?;
            writer.write_u64(self.state.applied_lsn)?;
            writer.write_u64(self.state.applied_lsn)?;
            writer.write_i64(timestamp)?;
            writer.write_u8(0)?;

            writer.bytes_written()
        };

        if bytes_written != reply_buf.len() {
            return Err(crate::core::errors::ReplicationError::protocol(
                "Failed to write feedback data".to_string(),
            ));
        }

        self.connection.put_copy_data(&reply_buf)?;
        self.connection.flush()?;

        debug!(
            "Sent feedback with received LSN: {:x}, applied LSN: {:x}",
            self.state.received_lsn, self.state.applied_lsn
        );
        Ok(())
    }

    async fn perform_graceful_shutdown(&mut self) -> ReplicationResult<()> {
        info!("Starting graceful shutdown process");

        // Drop the event sink first (which holds an EventHistoryRecorder clone),
        // then flush remaining event_history rows.
        self.event_sink.take();
        if let Some(entry) = self.event_history_entry.take() {
            entry.shutdown().await;
        }
        self.event_history_recorder.take();

        // Send final feedback to PostgreSQL with the latest LSN position
        if let Err(e) = self.send_feedback() {
            warn!("Failed to send final feedback during shutdown: {}", e);
        } else {
            info!("Successfully sent final feedback to PostgreSQL");
        }

        // Flush any remaining data in the connection
        if let Err(e) = self.connection.flush() {
            warn!("Failed to flush connection during shutdown: {}", e);
        }

        info!("Graceful shutdown completed successfully");
        Ok(())
    }

    fn check_and_send_feedback(&mut self) -> ReplicationResult<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_feedback_time)
            > Duration::from_secs(self.config.feedback_interval_secs)
        {
            self.send_feedback()?;
            self.state.update_feedback_time();
        }
        Ok(())
    }
}
