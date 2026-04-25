//! Configuration management for PostgreSQL logical replication
//!
//! This module handles loading configuration from environment variables.
//! It provides a centralized way to manage all application settings
//! with proper validation and default values.

use super::{ReplicationError, ReplicationResult};
use std::env;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub enum EventSinkType {
    Http,
    Hook0,
    Stdout,
}

impl std::fmt::Display for EventSinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSinkType::Http => write!(f, "http"),
            EventSinkType::Hook0 => write!(f, "hook0"),
            EventSinkType::Stdout => write!(f, "stdout"),
        }
    }
}

/// Configuration for the replication checker with validation
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub connection_string: String,
    pub publication_name: String,
    pub slot_name: String,
    pub feedback_interval_secs: u64,
    pub event_sink: EventSinkType,
    pub http_endpoint_url: Option<String>,
    pub hook0_api_url: Option<String>,
    pub hook0_application_id: Option<Uuid>,
    pub hook0_api_token: Option<String>,
}

impl ReplicationConfig {
    /// Load configuration from environment variables
    ///
    /// This method reads all configuration from environment variables with
    /// sensible defaults where applicable. It validates all required fields
    /// and returns an error if any required configuration is missing.
    ///
    /// # Environment Variables
    ///
    /// Required:
    /// - `DATABASE_URL`: PostgreSQL connection string
    ///
    /// Optional (with defaults):
    /// - `SLOT_NAME`: Replication slot name (default: "sub")
    /// - `PUB_NAME`: Publication name (default: "pub")
    /// - `EVENT_SINK`: Event sink type - "http", "hook0", or "stdout" (default: "stdout")
    ///
    /// Optional (event sink specific):
    /// - `HTTP_ENDPOINT_URL`: URL for HTTP event sink (required when using "http")
    /// - `HOOK0_API_URL`: Hook0 API URL (required when using "hook0")
    /// - `HOOK0_APPLICATION_ID`: Hook0 application UUID (required when using "hook0")
    /// - `HOOK0_API_TOKEN`: Hook0 API token (required when using "hook0")
    pub fn from_env() -> ReplicationResult<Self> {
        // Required: Database connection string
        let connection_string = env::var("DATABASE_URL").map_err(|_| {
            ReplicationError::config("Missing required DATABASE_URL environment variable")
        })?;

        // Optional with defaults: replication settings
        let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "sub".to_string());
        let publication_name = env::var("PUB_NAME").unwrap_or_else(|_| "pub".to_string());

        // Optional with default: event sink type
        let event_sink = env::var("EVENT_SINK").ok();

        // Optional: event sink specific configuration
        let http_endpoint_url = env::var("HTTP_ENDPOINT_URL").ok();
        let hook0_api_url = env::var("HOOK0_API_URL").ok();
        let hook0_api_token = env::var("HOOK0_API_TOKEN").ok();

        // Parse Hook0 application ID as UUID if provided
        let hook0_application_id = env::var("HOOK0_APPLICATION_ID")
            .ok()
            .and_then(|s| Uuid::parse_str(&s).ok());

        // Validate the configuration
        Self::validate_and_create(
            connection_string,
            publication_name,
            slot_name,
            event_sink,
            http_endpoint_url,
            hook0_api_url,
            hook0_application_id,
            hook0_api_token,
        )
    }

    /// Validate configuration parameters and create ReplicationConfig
    fn validate_and_create(
        connection_string: String,
        publication_name: String,
        slot_name: String,
        event_sink: Option<String>,
        http_endpoint_url: Option<String>,
        hook0_api_url: Option<String>,
        hook0_application_id: Option<Uuid>,
        hook0_api_token: Option<String>,
    ) -> ReplicationResult<Self> {
        // Validate connection string
        if connection_string.trim().is_empty() {
            return Err(ReplicationError::config("DATABASE_URL cannot be empty"));
        }

        // Validate publication name
        if publication_name.trim().is_empty() {
            return Err(ReplicationError::config("Publication name cannot be empty"));
        }

        // Validate slot name format (PostgreSQL naming rules)
        if slot_name.trim().is_empty() {
            return Err(ReplicationError::config("Slot name cannot be empty"));
        }

        if !slot_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(ReplicationError::config(
                "Slot name can only contain alphanumeric characters and underscores",
            ));
        }

        if slot_name.len() > 63 {
            // PostgreSQL identifier length limit
            return Err(ReplicationError::config(
                "Slot name cannot be longer than 63 characters",
            ));
        }

        // Validate event sink configuration

        let event_sink_val: Result<EventSinkType, ReplicationError> = match event_sink.as_ref() {
            Some(service) => {
                let service_lower = service.to_lowercase();

                match service_lower.as_str() {
                    "http" => {
                        // HTTP endpoint URL is required for HTTP sink
                        if http_endpoint_url.is_none()
                            || http_endpoint_url.as_ref().unwrap().trim().is_empty()
                        {
                            Err(ReplicationError::config(
                                "HTTP_ENDPOINT_URL is required when using 'http' event sink",
                            ))
                        } else {
                            // Validate HTTP endpoint URL format
                            let url = http_endpoint_url.as_ref().unwrap();
                            if !url.starts_with("http://") && !url.starts_with("https://") {
                                Err(ReplicationError::config(
                                    "HTTP_ENDPOINT_URL must start with http:// or https://",
                                ))
                            } else {
                                Ok(EventSinkType::Http)
                            }
                        }
                    }
                    "hook0" => {
                        // All Hook0 fields are required for Hook0 sink
                        if hook0_api_url.is_none() || hook0_api_url.as_ref().unwrap().trim().is_empty()
                        {
                            Err(ReplicationError::config(
                                "HOOK0_API_URL is required when using 'hook0' event sink",
                            ))
                        } else if hook0_application_id.is_none() {
                            Err(ReplicationError::config(
                                "HOOK0_APPLICATION_ID is required when using 'hook0' event sink",
                            ))
                        } else if hook0_api_token.is_none()
                            || hook0_api_token.as_ref().unwrap().trim().is_empty()
                        {
                            Err(ReplicationError::config(
                                "HOOK0_API_TOKEN is required when using 'hook0' event sink",
                            ))
                        } else {
                            // Validate Hook0 API URL format
                            let url = hook0_api_url.as_ref().unwrap();
                            if !url.starts_with("http://") && !url.starts_with("https://") {
                                Err(ReplicationError::config(
                                    "HOOK0_API_URL must start with http:// or https://",
                                ))
                            } else {
                                Ok(EventSinkType::Hook0)
                            }
                        }
                    }
                    "stdout" => {
                        // STDOUT sink requires no additional configuration
                        Ok(EventSinkType::Stdout)
                    }
                    _ => Err(ReplicationError::config(
                        "EVENT_SINK must be one of: 'http', 'hook0', or 'stdout'",
                    )),
                }
            }
            None => Err(ReplicationError::config(
                "EVENT_SINK must be one of: 'http', 'hook0', or 'stdout'",
            ))
        };

        Ok(Self {
            connection_string,
            publication_name,
            slot_name,
            feedback_interval_secs: 1, // Send feedback every second
            event_sink: event_sink_val?,
            http_endpoint_url,
            hook0_api_url,
            hook0_application_id,
            hook0_api_token,
        })
    }

    /// Get the event sink type with proper default handling
    pub fn event_sink_type(&self) -> &EventSinkType {
        &self.event_sink
    }

    /// Check if this configuration uses the HTTP event sink
    pub fn uses_http_sink(&self) -> bool {
        self.event_sink_type() == &EventSinkType::Http
    }

    /// Check if this configuration uses the Hook0 event sink
    pub fn uses_hook0_sink(&self) -> bool {
        self.event_sink_type() == &EventSinkType::Hook0
    }

    /// Check if this configuration uses the STDOUT event sink
    pub fn uses_stdout_sink(&self) -> bool {
        self.event_sink_type() == &EventSinkType::Stdout
    }

    /// Returns a copy of the connection string with `replication=database`
    /// (and any other `replication=` param) removed from the query string.
    ///
    /// The replication connection string contains `?replication=database&sslmode=require` —
    /// the `replication=` param breaks a normal tokio-postgres connection, but
    /// `sslmode=require` etc. must be preserved.
    pub fn connection_string_without_replication_param(&self) -> String {
        let qs_start = match self.connection_string.find('?') {
            Some(pos) => pos,
            None => return self.connection_string.clone(),
        };

        let before = &self.connection_string[..qs_start];
        let raw_query = &self.connection_string[qs_start + 1..];

        // Split on '&', drop any param starting with "replication=", rejoin
        let params: Vec<&str> = raw_query
            .split('&')
            .filter(|p| !p.starts_with("replication="))
            .collect();

        if params.is_empty() {
            before.to_string()
        } else {
            format!("{}?{}", before, params.join("&"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env_missing_database_url() {
        unsafe { std::env::remove_var("DATABASE_URL") };
        unsafe { std::env::remove_var("EVENT_SINK") };

        let result = ReplicationConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DATABASE_URL"));
    }

    #[test]
    fn test_config_with_valid_database_url() {
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");
            std::env::set_var("EVENT_SINK", "stdout");
        }

        let result = ReplicationConfig::from_env();
        assert!(result.is_ok());

        let config = result.unwrap();
        assert_eq!(config.slot_name, "sub");
        assert_eq!(config.publication_name, "pub");
        assert!(config.uses_stdout_sink());

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("EVENT_SINK");
        }
    }

    #[test]
    fn test_config_http_sink_requires_endpoint() {
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");
            std::env::set_var("EVENT_SINK", "http");
            std::env::remove_var("HTTP_ENDPOINT_URL");
        }

        let result = ReplicationConfig::from_env();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("HTTP_ENDPOINT_URL")
        );

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("EVENT_SINK");
        }
    }

    #[test]
    fn test_config_hook0_sink_requires_all_fields() {
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");
            std::env::set_var("EVENT_SINK", "hook0");
            std::env::set_var("HOOK0_API_URL", "https://api.hook0.com");
            std::env::remove_var("HOOK0_APPLICATION_ID");
            std::env::remove_var("HOOK0_API_TOKEN");
        }

        let result = ReplicationConfig::from_env();
        assert!(result.is_err());

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("EVENT_SINK");
            std::env::remove_var("HOOK0_API_URL");
        }
    }

    #[test]
    fn test_connection_string_without_replication_param() {
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                "postgres://user@host/db?replication=database&sslmode=require",
            );
            std::env::set_var("EVENT_SINK", "stdout");
        }

        let config = ReplicationConfig::from_env().unwrap();
        assert_eq!(
            config.connection_string_without_replication_param(),
            "postgres://user@host/db?sslmode=require"
        );

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("EVENT_SINK");
        }
    }

    #[test]
    fn test_connection_string_without_replication_param_noop_when_no_query() {
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");
            std::env::set_var("EVENT_SINK", "stdout");
        }

        let config = ReplicationConfig::from_env().unwrap();
        assert_eq!(
            config.connection_string_without_replication_param(),
            "postgresql://test@localhost/test"
        );

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("EVENT_SINK");
        }
    }
}
