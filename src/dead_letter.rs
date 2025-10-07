use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use time::OffsetDateTime;

/// Represents a dead letter message with metadata about the failure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct DeadLetterMessage {
    /// Unique identifier for the dead letter message in the store
    pub id: Option<i64>,
    /// The original message subject
    pub subject: String,
    /// The original message payload
    pub payload: Vec<u8>,
    /// The original message headers, if any
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// The stream name where the message originated
    pub stream: String,
    /// The consumer name that failed to process the message
    pub consumer: String,
    /// The sequence number of the message in the stream
    pub stream_sequence: u64,
    /// The reason for the message being dead lettered
    pub reason: DeadLetterReason,
    /// When the message was dead lettered
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    /// Number of delivery attempts before being dead lettered
    pub delivery_count: u64,
    /// Prefix used in subject parsing
    pub prefix: Option<String>,
    /// Aggregate ID extracted from the subject
    pub aggregate_id: Option<uuid::Uuid>,
}

/// The reason why a message was added to the dead letter queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeadLetterReason {
    /// Message reached maximum delivery attempts
    MaxDeliveriesReached,
    /// Message was manually terminated by client
    MessageTerminated,
    /// Processing error occurred
    ProcessingError(String),
}

/// Extension trait for storing dead letter messages
///
/// This trait allows implementations to use any storage backend
/// (database, file system, another NATS stream, etc.)
#[trait_variant::make(Send)]
pub trait DeadLetterStore: Send + Sync {
    /// Error type for storage operations
    type Error: StdError + Send + Sync + 'static;

    /// Store a dead letter message
    async fn store_dead_letter(&self, message: DeadLetterMessage) -> Result<(), Self::Error>;

    /// Retrieve dead letter messages with optional filtering
    async fn get_dead_letters(
        &self,
        stream: Option<&str>,
        consumer: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<DeadLetterMessage>, Self::Error>;

    /// Remove a dead letter message by its unique identifier
    /// The identifier is implementation-specific (could be sequence, ID, etc.)
    async fn remove_dead_letter(&self, identifier: &str) -> Result<(), Self::Error>;

    /// Get count of dead letter messages with optional filtering
    async fn count_dead_letters(
        &self,
        stream: Option<&str>,
        consumer: Option<&str>,
    ) -> Result<u64, Self::Error>;
}

impl DeadLetterMessage {
    /// Create a new dead letter message from a NATS stream message and advisory info
    pub fn from_stream_message(
        message: &async_nats::jetstream::message::StreamMessage,
        stream: String,
        consumer: String,
        stream_sequence: u64,
        reason: DeadLetterReason,
        delivery_count: u64,
        prefix: String,
        aggregate_id: uuid::Uuid,
    ) -> Self {
        let headers = if message.headers.is_empty() {
            None
        } else {
            Some(
                message
                    .headers
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            v.iter()
                                .map(|val| val.to_string())
                                .collect::<Vec<_>>()
                                .join(","),
                        )
                    })
                    .collect(),
            )
        };

        // Extract timestamp from message time
        let timestamp = message.time;

        Self {
            id: None,
            subject: message.subject.to_string(),
            payload: message.payload.to_vec(),
            headers,
            stream,
            consumer,
            stream_sequence,
            reason,
            timestamp,
            delivery_count,
            prefix: Some(prefix),
            aggregate_id: Some(aggregate_id),
        }
    }

    /// Create a dead letter message from advisory message data
    pub fn from_advisory(
        stream: String,
        consumer: String,
        stream_sequence: u64,
        reason: DeadLetterReason,
        delivery_count: u64,
        original_subject: String,
        payload: Vec<u8>,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        Self {
            id: None,
            subject: original_subject,
            payload,
            headers,
            stream,
            consumer,
            stream_sequence,
            reason,
            timestamp: OffsetDateTime::now_utc(),
            delivery_count,
            prefix: None,
            aggregate_id: None,
        }
    }
}
