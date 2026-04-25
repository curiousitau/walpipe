//! Event sink implementations for PostgreSQL logical replication
//!
//! Provides various event sink implementations for sending replication events
//! to different destinations including HTTP endpoints, Hook0, and STDOUT.

use crate::core::errors::ReplicationResult;
use super::EventSink;

pub mod event_formatter;
pub mod event_history;
pub mod hook0;
pub mod hook0_error;
pub mod http;
pub mod pg_type_conversion;
pub mod stdout;

/// Registry for managing and creating event sinks
pub struct EventSinkRegistry;

impl EventSinkRegistry {
    /// Create an event sink based on configuration.
    ///
    /// `event_history_recorder` is optional and only used by the Hook0 sink
    /// to record successful deliveries to the `event_history` table.
    pub fn create_sink(
        sink_type: &crate::core::config::EventSinkType,
        config: &crate::core::config::ReplicationConfig,
        event_history_recorder: Option<std::sync::Arc<event_history::EventHistoryRecorder>>,
    ) -> ReplicationResult<std::sync::Arc<dyn EventSink + Send + Sync>> {
        match sink_type {
            crate::core::config::EventSinkType::Http => {
                if let Some(ref url) = config.http_endpoint_url {
                    let http_config = http::HttpEventSinkConfig {
                        endpoint_url: url.clone(),
                    };
                    let sink = http::HttpEventSink::new(http_config)
                        .map_err(|e| crate::core::errors::ReplicationError::config(e))?;
                    Ok(std::sync::Arc::new(sink) as std::sync::Arc<dyn EventSink + Send + Sync>)
                } else {
                    Err(crate::core::errors::ReplicationError::config(
                        "HTTP endpoint URL required for HTTP sink",
                    ))
                }
            }
            crate::core::config::EventSinkType::Hook0 => {
                if let (Some(ref api_url), Some(app_id), Some(ref api_token)) = (
                    config.hook0_api_url.as_ref(),
                    config.hook0_application_id,
                    config.hook0_api_token.as_ref(),
                ) {
                    let hook0_config = hook0::Hook0EventSinkConfig {
                        api_url: api_url.to_string(),
                        application_id: app_id,
                        api_token: api_token.to_string(),
                    };
                    let sink = hook0::Hook0EventSink::new(hook0_config, event_history_recorder)
                        .map_err(|e| crate::core::errors::ReplicationError::config(e))?;
                    Ok(std::sync::Arc::new(sink) as std::sync::Arc<dyn EventSink + Send + Sync>)
                } else {
                    Err(crate::core::errors::ReplicationError::config(
                        "Hook0 API URL, application ID, and token required for Hook0 sink",
                    ))
                }
            }
            crate::core::config::EventSinkType::Stdout => {
                let sink = stdout::StdoutEventSink {};
                Ok(std::sync::Arc::new(sink) as std::sync::Arc<dyn EventSink + Send + Sync>)
            }
        }
    }
}