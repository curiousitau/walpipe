//! Asynchronous batch recorder for event_history table inserts.
//!
//! Buffers delivered-event records in memory and flushes them to Postgres
//! in batches (size or time triggered). Runs a background tokio task so
//! the replication loop is never blocked by these writes.
//!
//! event_history is shared with the application (it's an event-sourcing
//! state-transition log keyed on event_history_id, FK'd to event_stream).
//! event_stream's after-insert trigger writes a `CREATED` row; walpipe
//! writes a `DELIVERED` row once a hook0 dispatch succeeds. Each row is a
//! transition, not a unique key per event — no ON CONFLICT needed.
//!
//! Live schema (managed by the application):
//!
//! ```sql
//! CREATE TABLE event_history (
//!     event_history_id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
//!     event_id         UUID NOT NULL REFERENCES event_stream(event_id) ON DELETE CASCADE,
//!     status           TEXT NOT NULL,
//!     changed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     metadata         JSONB DEFAULT '{}'::jsonb
//! );
//! ```

use chrono::DateTime;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};

/// One record to insert into event_history.
#[derive(Debug)]
pub struct EventHistoryRecord {
    pub event_id: uuid::Uuid,
    pub event_type: String,
    pub source_created_at: DateTime<chrono::Utc>,
}

/// Handle returned by [`EventHistoryRecorder::new`] that owns the shutdown
/// mechanism.
///
/// **Important:** all [`EventHistoryRecorder`] clones must be dropped *before*
/// calling `shutdown()`, otherwise the background task's channel won't close
/// and the final flush won't happen.
pub struct Entry {
    done_rx: oneshot::Receiver<()>,
}

/// Alias used by callers outside this module.
pub type EventHistoryEntry = Entry;

impl Entry {
    /// Wait for the background task to flush remaining events and exit.
    ///
    /// All `EventHistoryRecorder` clones (especially the one inside the
    /// event sink) must already be dropped before calling this method.
    pub async fn shutdown(self) {
        let _ = self.done_rx.await;
        info!("event_history background task exited");
    }
}

/// Async batch recorder.  Clone to share; the background task batches &
/// flushes to the DB.
#[derive(Clone)]
pub struct EventHistoryRecorder {
    tx: mpsc::Sender<EventHistoryRecord>,
}

impl EventHistoryRecorder {
    /// Create a new recorder connected to the same database.
    ///
    /// `batch_size` – flush when this many events are buffered.
    /// `flush_interval` – flush periodically even if buffer is smaller.
    pub async fn new(
        conn_str: &str,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Result<(Self, Entry), String> {
        // Azure Postgres requires TLS; the conn string carries `sslmode=require`,
        // and tokio-postgres negotiates the upgrade but needs a real TLS impl
        // to perform the handshake. The replication path uses libpq which
        // handles TLS internally, hence the divergence.
        let tls_connector = native_tls::TlsConnector::builder()
            .build()
            .map_err(|e| format!("event_history recorder: build TLS connector: {e}"))?;
        let make_tls = postgres_native_tls::MakeTlsConnector::new(tls_connector);

        let (client, conn) = tokio_postgres::connect(conn_str, make_tls)
            .await
            .map_err(|e| format!("event_history recorder: connect failed: {e}"))?;

        // The connection future drives socket I/O for the client; without a
        // task spawning it, queries on `client` will hang forever.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("event_history recorder connection lost: {e}");
            }
        });

        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| format!("event_history recorder: connectivity check failed: {e}"))?;

        let (tx, rx) = mpsc::channel(1024);
        let (done_tx, done_rx) = oneshot::channel();

        tokio::spawn(Self::flush_task(
            client, rx, done_tx, batch_size, flush_interval,
        ));

        info!(
            "event_history recorder started (batch={}, interval={:?}",
            batch_size, flush_interval
        );
        Ok((Self { tx }, Entry { done_rx }))
    }

    /// Queue a delivered-event record.
    ///
    /// Non-blocking at the DB level — the background task batches & flushes.
    /// Awaits the channel send to avoid dropping events under backpressure
    /// (in practice the channel never fills because the replication loop is
    /// single-threaded and the background task keeps up).
    pub async fn record(&self, record: EventHistoryRecord) {
        if self.tx.send(record).await.is_err() {
            error!("event_history recorder channel closed");
        }
    }

    #[allow(clippy::unused_async)]
    async fn flush_task(
        mut client: Client,
        mut rx: mpsc::Receiver<EventHistoryRecord>,
        done_tx: oneshot::Sender<()>,
        batch_size: usize,
        flush_interval: Duration,
    ) {
        let mut buffer: Vec<EventHistoryRecord> = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                biased;
                maybe_rec = rx.recv() => {
                    match maybe_rec {
                        Some(rec) => {
                            buffer.push(rec);
                            if buffer.len() >= batch_size {
                                if let Err(e) = Self::flush_batch(&mut client, &buffer).await {
                                    error!("event_history flush error: {e}");
                                } else {
                                    buffer.clear();
                                }
                            }
                        }
                        None => {
                            // All senders dropped — final flush.
                            if !buffer.is_empty() {
                                if let Err(e) = Self::flush_batch(&mut client, &buffer).await {
                                    error!("event_history final flush error: {e}");
                                }
                            }
                            let _ = done_tx.send(());
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = Self::flush_batch(&mut client, &buffer).await {
                            warn!("event_history periodic flush error (will retry): {e}");
                        } else {
                            buffer.clear();
                        }
                    }
                }
            }
        }
    }

    async fn flush_batch(client: &mut Client, batch: &[EventHistoryRecord]) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let sql = build_insert_sql(batch);

        debug!("flushing {} event_history rows", batch.len());
        client.simple_query(&sql).await.map_err(|e| {
            // tokio_postgres::Error's Display is intentionally generic
            // ("db error"); the actual SQLSTATE / message lives on the
            // source. Surface both so flush failures are diagnosable.
            let detail = std::error::Error::source(&e)
                .map(|s| format!(": {s}"))
                .unwrap_or_default();
            format!("flush_batch: {e}{detail}")
        })?;
        debug!("flushed {} event_history rows", batch.len());
        Ok(())
    }
}

/// Build the INSERT SQL for a batch of event_history records.
/// Pure function — easy to test.
pub(crate) fn build_insert_sql(batch: &[EventHistoryRecord]) -> String {
    let mut sql = String::from(
        "INSERT INTO event_history (event_id, status, metadata) VALUES ",
    );
    for (i, rec) in batch.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        let ts = rec.source_created_at.format("%Y-%m-%d %H:%M:%S%.f UTC");
        sql.push_str(&format!(
            "('{}', 'DELIVERED', jsonb_build_object('trigger', 'walpipe_dispatch', 'event_type', '{}', 'source_created_at', '{}'))",
            rec.event_id,
            rec.event_type.replace('\'', "''"),
            ts.to_string().replace('\'', "''"),
        ));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_record(id: uuid::Uuid, event_type: &str, created: DateTime<chrono::Utc>) -> EventHistoryRecord {
        EventHistoryRecord {
            event_id: id,
            event_type: event_type.to_string(),
            source_created_at: created,
        }
    }

    #[test]
    fn test_build_insert_sql_single() {
        let id = uuid::Uuid::nil();
        let batch = vec![make_record(id, "user.created", Utc::now())];
        let sql = build_insert_sql(&batch);

        assert!(sql.starts_with("INSERT INTO event_history (event_id, status, metadata) VALUES"));
        assert!(sql.contains(&id.to_string()));
        assert!(sql.contains("'DELIVERED'"));
        assert!(sql.contains("user.created"));
        assert!(sql.contains("'walpipe_dispatch'"));
    }

    #[test]
    fn test_build_insert_sql_multiple() {
        let batch = vec![
            make_record(uuid::Uuid::nil(), "type.a", Utc::now()),
            make_record(uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(), "type.b", Utc::now()),
        ];
        let sql = build_insert_sql(&batch);

        // Should contain two value tuples separated by comma.
        assert!(sql.contains(")),("));
        assert!(sql.contains("type.a"));
        assert!(sql.contains("type.b"));
    }

    #[test]
    fn test_build_insert_sql_escapes_quotes() {
        let batch = vec![make_record(uuid::Uuid::nil(), "user's.event", Utc::now())];
        let sql = build_insert_sql(&batch);

        assert!(sql.contains("user''s.event"));
        assert!(!sql.contains("user's.event"));
    }

    #[test]
    fn test_flush_batch_empty_returns_ok() {
        // flush_batch returns Ok early for empty input; this verifies the
        // SQL builder still produces a syntactically-recognisable prefix
        // when called directly with an empty batch.
        let sql = build_insert_sql(&[]);
        assert!(sql.contains("VALUES "));
    }
}