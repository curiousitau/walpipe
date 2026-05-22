//! Hook0 event sink implementation
//!
//! Provides an event sink for sending replication events to Hook0 API
//! with comprehensive error handling, retry logic, and email notifications.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Local, Utc};
use lettre::{Transport, transport::smtp::authentication::Credentials};
use lettre::SmtpTransport;
use lettre::address::Address;
use lettre::Message;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};
use uuid::Uuid;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use super::super::EventSink;
use crate::core::email_config::EmailConfig;
use crate::core::errors::{ReplicationError, ReplicationResult};
use crate::protocol::messages::ReplicationMessage;
use super::event_history::{EventHistoryRecord, EventHistoryRecorder};
use super::hook0_error::Hook0ErrorId;
use super::pg_type_conversion::{
    ColumnValue, ReplicationEventDecoder, ReplicationRow, parse_timestamptz,
};

use hook0_client::{Event, Hook0Client, Hook0ClientError};

/// Hook0 event sink configuration
#[derive(Debug, Clone)]
pub struct Hook0EventSinkConfig {
    /// URL of the Hook0 API endpoint
    pub api_url: String,
    /// Hook0 application ID
    pub application_id: Uuid,
    /// Hook0 API token
    pub api_token: String,
}

/// Hook0 event sink for sending replication events to Hook0 API
#[derive(Clone)]
pub struct Hook0EventSink {
    pub(crate) hook0_client: Hook0Client,
    pub(crate) email_config: Option<EmailConfig>,
    pub(crate) decoder: Arc<Mutex<ReplicationEventDecoder>>,
    unknown_event_types: Arc<Mutex<HashMap<String, DateTime<Local>>>>,
    event_history_recorder: Option<Arc<EventHistoryRecorder>>,
}

/// event table row
struct EventTableRow {
    pub event_id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub metadata: Value,
    pub payload: Value,
    pub labels: Value,
}

fn parse_event_row(row: &ReplicationRow) -> Result<EventTableRow, ReplicationError> {
    // Check that the columns exist in row before trying to grab their values
    let colnames = vec![
        "event_id",
        "event_type",
        "created_at",
        "metadata",
        "payload",
        "labels",
    ];
    let mut missingcols: Vec<String> = vec![];

    for colname in colnames {
        if !row.column_values.contains_key(&colname.to_string()) {
            missingcols.push(colname.to_string());
        }
    }

    if !missingcols.is_empty() {
        return Err(ReplicationError::MessageParsing {
            message: format!(
                "Missing columns: {:?} (found: {:?})",
                missingcols,
                row.column_values.keys()
            ),
            context: Some(format!("{:?}", row.column_values.keys())),
        });
    }

    // Helper to extract and validate a column value
    let get_json_column = |name: &str| -> Result<serde_json::Value, ReplicationError> {
        row.column_values
            .get(name)
            .ok_or(ReplicationError::MessageParsing {
                message: format!("{} column missing", name),
                context: Some(format!("{:?}", row.column_values)),
            })
            .and_then(|val| {
                if let ColumnValue::Json(val) = val {
                    Ok(val.clone())
                } else {
                    Err(ReplicationError::MessageParsing {
                        message: format!("{} not a json column value, is {:?}", name, val),
                        context: Some(format!("{:?}", row.column_values)),
                    })
                }
            })
    };

    // Helper to extract and validate a timestamptz column value
    let get_timestamptz_column = |name: &str| -> Result<DateTime<Utc>, ReplicationError> {
        row.column_values
            .get(name)
            .ok_or(ReplicationError::MessageParsing {
                message: format!("{} column missing", name),
                context: Some(format!("{:?}", row.column_values)),
            })
            .and_then(|val| match val {
                ColumnValue::TimestampTZ(s) => Ok(*s),
                ColumnValue::String(s) => match parse_timestamptz(s.clone()) {
                    Ok(dt) => Ok(dt),
                    Err(e) => Err(ReplicationError::MessageParsing {
                        message: format!("{} string parse error: {:?}", name, e),
                        context: Some(format!("Error: {:?}, full row: {:?}", e, row.column_values)),
                    }),
                },
                y => Err(ReplicationError::MessageParsing {
                    message: format!("{} not a timestamptz column value, is {:?}", name, y),
                    context: Some(format!("{:?}", row.column_values)),
                }),
            })
    };

    // Helper to extract and validate a string column value
    let get_string_column = |name: &str| -> Result<String, ReplicationError> {
        row.column_values
            .get(name)
            .ok_or(ReplicationError::MessageParsing {
                message: format!("{} column missing", name),
                context: Some(format!("{:?}", row.column_values)),
            })
            .and_then(|val| match val {
                ColumnValue::String(s) => Ok(s.clone()),
                y => Err(ReplicationError::MessageParsing {
                    message: format!("{} not a string column value, is {:?}", name, y),
                    context: Some(format!("{:?}", row.column_values)),
                }),
            })
    };

    let get_uuid_column = |name: &str| -> Result<Uuid, ReplicationError> {
        row.column_values
            .get(name)
            .ok_or(ReplicationError::MessageParsing {
                message: format!("{} column missing", name),
                context: Some(format!("{:?}", row.column_values)),
            })
            .and_then(|val| match val {
                ColumnValue::Uuid(s) => Ok(*s),
                y => Err(ReplicationError::MessageParsing {
                    message: format!("{} not a UUID column value, is {:?}", name, y),
                    context: Some(format!("{:?}", row.column_values)),
                }),
            })
    };

    Ok(EventTableRow {
        event_id: get_uuid_column("event_id")?,
        event_type: get_string_column("event_type")?,
        created_at: get_timestamptz_column("created_at")?,
        metadata: get_json_column("metadata")?,
        payload: get_json_column("payload")?,
        labels: get_json_column("labels")?,
    })
}

#[async_trait]
impl EventSink for Hook0EventSink {
    /// Send a replication event to Hook0 API
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()> {
        // Decode the replication message into a row
        let decoder = self.decoder.clone();
        let mut decoder_guard = decoder.lock().await;
        let decoded_event = decoder_guard.decode(event);

        let row = match decoded_event {
            Some(evt) => evt,
            None => return Ok(()),
        };

        let event_row = parse_event_row(&row)?;
        {
            let mut unknown_event_lock = self.unknown_event_types.lock().await;

            // Check that the event type hasn't recently been attempted and was unknown
            if let Some(last_attempt_time) = unknown_event_lock.get(&event_row.event_type) {
                let elapsed_time_since_last_attempt = Local::now() - last_attempt_time;
                if elapsed_time_since_last_attempt < Duration::minutes(5) {
                    return Ok(());
                } else {
                    unknown_event_lock.remove(&event_row.event_type);
                }
            }
        }
        // Prepare the payload and event type for Hook0
        let payload = event_row.payload.to_string();
        let event_type = event_row.event_type.clone();

        // Build the Hook0 event structure
        let hook0_event = Event {
            event_id: &Some(&event_row.event_id),
            event_type: &event_type,
            payload: Cow::Borrowed(payload.as_str()),
            payload_content_type: "application/json",
            metadata: Some(
                event_row
                    .metadata
                    .as_object()
                    .unwrap()
                    .into_iter()
                    .filter_map(|(k, v)| {
                        if v.is_string() {
                            Some((k.clone(), v.as_str().unwrap().to_string()))
                        } else {
                            None
                        }
                    })
                    .collect(),
            ),
            occurred_at: event_row.created_at.into(),
            labels: event_row
                .labels
                .as_object()
                .unwrap()
                .into_iter()
                .filter_map(|(k, v)| {
                    if v.is_string() {
                        Some((k.clone(), v.as_str().unwrap().to_string()))
                    } else {
                        None
                    }
                })
                .collect(),
        };

        // Retry configuration
        let max_retries = 5;
        let base_delay_ms = 1000; // 1 second
        let max_delay_ms = 30000; // 30 seconds

        let mut attempt = 0;
        let mut delay_ms = base_delay_ms;
        let mut continue_on_retry_exceed = false;

        loop {
            attempt += 1;

            debug!(
                "About to send event to Hook0 API (attempt {}/{}): {:?}",
                attempt, max_retries, hook0_event
            );

            let send_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                self.hook0_client.send_event(&hook0_event),
            )
            .await;

            let send_result = match send_result {
                Ok(inner) => inner,
                Err(_) => {
                    error!(
                        "Hook0 API request timed out after 30s (attempt {}/{}). Event ID: {:?}",
                        attempt, max_retries, hook0_event.event_id
                    );
                    continue;
                }
            };

            match send_result {
                Ok(_) => {
                    debug!("Successfully sent event to Hook0 API");
                    Self::record_event_history(&self.event_history_recorder, &event_row).await;
                    return Ok(());
                }
                Err(e) => {
                    match &e {
                        Hook0ClientError::EventSending {
                            body,
                            error: _,
                            event_id,
                        } => {
                            if let Some(body) = body {
                                // Deserialize the body to get the error type
                                let error_details: serde_json::Value =
                                    serde_json::from_str(body).unwrap_or_default();
                                let error_id = error_details
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .map(Hook0ErrorId::from)
                                    .unwrap_or(Hook0ErrorId::InternalServerError);
                                match error_id {
                                    Hook0ErrorId::EventTypeDoesNotExist => {
                                        let mut unknown_event_lock =
                                            self.unknown_event_types.lock().await;
                                        unknown_event_lock
                                            .insert(event_row.event_type.clone(), Local::now());
                                        self.send_email_notification(&format!(
                                            "Failed to send replication event {} to Hook0 API: Event type does not exist or was deactivated. You should (re)create it. Event ID: {}, Error: {}",
                                            event_row.event_type, event_id, e
                                        )).await;
                                        error!(
                                            "Skipping event {} due to unknown event type. Sent notification email.",
                                            event_row.event_type
                                        );
                                        return Ok(());
                                    }
                                    Hook0ErrorId::EventAlreadyIngested => {
                                        warn!(
                                            "Event already ingested by Hook0 API. Event ID: {}, Error: {}",
                                            event_id, e
                                        );
                                        Self::record_event_history(&self.event_history_recorder, &event_row).await;
                                        return Ok(()); // Skip the event if already ingested
                                    }
                                    Hook0ErrorId::InternalServerError => {
                                        // Something went wrong on the server side, retry
                                        error!(
                                            "Hook0 internal server error, retrying in {}ms. Event ID: {}, Error: {}",
                                            delay_ms, event_id, e
                                        );
                                    }
                                    Hook0ErrorId::InvalidEventId => {
                                        error!(
                                            "Invalid event ID provided to Hook0 API. Event ID: {}, Error: {}",
                                            event_id, e
                                        );
                                    }
                                    Hook0ErrorId::InvalidPayload => {
                                        error!(
                                            "Invalid payload provided to Hook0 API. Event ID: {}, Error: {}",
                                            event_id, e
                                        );
                                    }
                                    Hook0ErrorId::Unauthorized => {
                                        error!(
                                            "Unauthorized access to Hook0 API. Event ID: {}, Error: {}",
                                            event_id, e
                                        );
                                        self.send_email_notification(&format!(
                                                                "Unauthorized access to Hook0 API. Event ID: {}, Error: {}",
                                                                event_id, e
                                                            ))
                                                            .await;
                                        panic!("Unauthorized access to Hook0 API, exiting.");
                                    }
                                    Hook0ErrorId::RateLimitExceeded => {
                                        error!(
                                            "Rate limit exceeded by Hook0 API. Event ID: {}, Error: {}",
                                            event_id, e
                                        );
                                        // Rate limit exceeded, retry with exponential backoff
                                        delay_ms = (delay_ms * 2).min(max_delay_ms);
                                    }
                                }
                            }
                        }
                        e => {
                            error!(
                                "Hook0 API request failed with unexpected error type: {:?}",
                                e
                            ); // Log unexpected errors as well

                            // In the case of an unexpected error, we should set a flag to say it's OK to continue with other events
                            continue_on_retry_exceed = true;
                        }
                    }

                    if attempt >= max_retries {
                        // Send email notification and panic on final failure
                        self.send_email_notification(&format!(
                            "Failed to send replication event after {} attempts. Hook0 API error: {}",
                            max_retries,
                            e
                        )).await;

                        if continue_on_retry_exceed {
                            return Ok(());
                        }

                        panic!(
                            "Failed to send replication event after {} attempts. Hook0 API error: {}",
                            max_retries, e
                        );
                    }
                    error!(
                        "Hook0 API request failed, retrying in {}ms (attempt {}/{})",
                        delay_ms, attempt, max_retries
                    );
                }
            }

            // Exponential backoff with jitter
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;

            // Increase delay for next attempt (exponential backoff)
            delay_ms = (delay_ms * 2).min(max_delay_ms);
        }
    }
}

impl Hook0EventSink {
    /// Create a new Hook0 event sink
    pub fn new(
        config: Hook0EventSinkConfig,
        event_history_recorder: Option<Arc<EventHistoryRecorder>>,
    ) -> Result<Self, String> {
        // Validate email configuration at startup
        let email_config = match EmailConfig::from_env() {
            Ok(email_config) => Some(email_config),
            Err(e) => {
                tracing::warn!(
                    "Email configuration not found, email notifications will be disabled: {}",
                    e
                );
                None
            }
        };

        // Create Hook0 client
        let hook0_client = match Hook0Client::new(
            reqwest::Url::parse(&config.api_url).map_err(|e| format!("Invalid URL: {}", e))?,
            config.application_id,
            &config.api_token,
        ) {
            Ok(client) => client,
            Err(e) => {
                return Err(format!("Failed to create Hook0 client: {}", e));
            }
        };

        Ok(Self {
            hook0_client,
            email_config,
            decoder: Arc::new(Mutex::new(ReplicationEventDecoder::new())),
            unknown_event_types: Arc::new(Mutex::new(HashMap::new())),
            event_history_recorder,
        })
    }

    /// Send an email notification about a failure
    pub(crate) async fn send_email_notification(&self, message: &str) {
        // If email config is not available, return early
        let email_config = match &self.email_config {
            Some(config) => config,
            None => {
                error!("Email configuration not available, cannot send notification");
                return;
            }
        };

        // Build the email message
        let email = Message::builder()
            .from(
                email_config
                    .from_email
                    .parse::<Address>()
                    .expect("Invalid from email address")
                    .into(),
            )
            .to(email_config
                .to_email
                .parse::<Address>()
                .expect("Invalid to email address")
                .into())
            .subject("Replication Event Failure")
            .body(message.to_string())
            .unwrap();

        // Create SMTP transport
        let mailer = SmtpTransport::builder_dangerous(email_config.smtp_host.as_str())
            .port(email_config.smtp_port)
            .credentials(Credentials::new(
                email_config.smtp_username.clone(),
                email_config.smtp_password.clone(),
            ))
            .build();

        // Send the email
        if let Err(e) = mailer.send(&email) {
            error!("Failed to send email notification: {}", e);
        } else {
            debug!("Email notification sent successfully");
        }
    }

    async fn record_event_history(
        recorder: &Option<Arc<EventHistoryRecorder>>,
        event_row: &EventTableRow,
    ) {
        let recorder = match recorder {
            Some(r) => r.clone(),
            None => return,
        };
        let record = EventHistoryRecord {
            event_id: event_row.event_id,
            event_type: event_row.event_type.clone(),
            source_created_at: event_row.created_at,
        };
        recorder.record(record);
    }
}