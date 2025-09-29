#![allow(missing_docs)]
use crate::{DeadLetterMessage, DeadLetterReason, DeadLetterStore};
use serde_json;
use sqlx::{types::time::OffsetDateTime, PgPool, Row};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlxDeadLetterError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct SqlxDeadLetterStore {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct DeadLetterMessageRow {
    #[allow(unused)]
    id: i64,
    subject: String,
    payload: Vec<u8>,
    headers: Option<serde_json::Value>,
    stream: String,
    consumer: String,
    stream_sequence: i64,
    reason: String,
    timestamp: OffsetDateTime,
    delivery_count: i64,
}

impl SqlxDeadLetterStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        sqlx::query(
                r#"
-- migrations/001_create_dead_letters.sql
CREATE TABLE IF NOT EXISTS dead_letter_messages (
    id BIGSERIAL PRIMARY KEY,
    subject VARCHAR NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB,
    stream VARCHAR NOT NULL,
    consumer VARCHAR NOT NULL,
    stream_sequence BIGINT NOT NULL,
    reason VARCHAR NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivery_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- CREATE INDEX IF NOT EXISTS idx_dead_letters_stream_consumer ON dead_letter_messages (stream, consumer);
-- CREATE INDEX IF NOT EXISTS idx_dead_letters_stream_sequence ON dead_letter_messages (stream, stream_sequence);
-- CREATE INDEX IF NOT EXISTS idx_dead_letters_timestamp ON dead_letter_messages (timestamp);
            "#,
            ).execute(&self.pool).await
    }
}

impl DeadLetterStore for SqlxDeadLetterStore {
    type Error = SqlxDeadLetterError;

    async fn store_dead_letter(&self, message: DeadLetterMessage) -> Result<(), Self::Error> {
        let headers_json = message
            .headers
            .map(|h| serde_json::to_value(h))
            .transpose()?;

        let reason_str = match message.reason {
            DeadLetterReason::MaxDeliveriesReached => "max_deliveries",
            DeadLetterReason::MessageTerminated => "terminated",
            DeadLetterReason::ProcessingError(ref msg) => &format!("error: {}", msg),
        };

        sqlx::query(
                r#"
                INSERT INTO dead_letter_messages 
                (subject, payload, headers, stream, consumer, stream_sequence, reason, timestamp, delivery_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#
            )
            .bind(&message.subject)
            .bind(&message.payload)
            .bind(&headers_json)
            .bind(&message.stream)
            .bind(&message.consumer)
            .bind(message.stream_sequence as i64)
            .bind(reason_str)
            .bind(message.timestamp)
            .bind(message.delivery_count as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_dead_letters(
        &self,
        stream: Option<&str>,
        consumer: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<DeadLetterMessage>, Self::Error> {
        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let mut query = String::from(
                "SELECT id, subject, payload, headers, stream, consumer, stream_sequence, reason, timestamp, delivery_count FROM dead_letter_messages WHERE 1=1"
            );

        let mut params: Vec<String> = Vec::new();
        let mut param_count = 0;

        if let Some(s) = stream {
            param_count += 1;
            query.push_str(&format!(" AND stream = ${}", param_count));
            params.push(s.to_string());
        }

        if let Some(c) = consumer {
            param_count += 1;
            query.push_str(&format!(" AND consumer = ${}", param_count));
            params.push(c.to_string());
        }

        query.push_str(" ORDER BY timestamp DESC");

        param_count += 1;
        query.push_str(&format!(" LIMIT ${}", param_count));

        param_count += 1;
        query.push_str(&format!(" OFFSET ${}", param_count));

        let mut query_builder = sqlx::query_as::<_, DeadLetterMessageRow>(&query);

        for param in params {
            query_builder = query_builder.bind(param);
        }

        query_builder = query_builder.bind(limit as i64);
        query_builder = query_builder.bind(offset as i64);

        let rows = query_builder.fetch_all(&self.pool).await?;

        let messages = rows
            .into_iter()
            .map(|row| {
                let headers = row.headers.and_then(|h| serde_json::from_value(h).ok());

                let reason = match row.reason.as_str() {
                    "max_deliveries" => DeadLetterReason::MaxDeliveriesReached,
                    "terminated" => DeadLetterReason::MessageTerminated,
                    s if s.starts_with("error: ") => DeadLetterReason::ProcessingError(
                        s.strip_prefix("error: ").unwrap().to_string(),
                    ),
                    _ => DeadLetterReason::ProcessingError("Unknown reason".to_string()),
                };

                DeadLetterMessage {
                    subject: row.subject,
                    payload: row.payload,
                    headers,
                    stream: row.stream,
                    consumer: row.consumer,
                    stream_sequence: row.stream_sequence as u64,
                    reason,
                    timestamp: row.timestamp,
                    delivery_count: row.delivery_count as u64,
                }
            })
            .collect();

        Ok(messages)
    }

    async fn remove_dead_letter(&self, identifier: &str) -> Result<(), Self::Error> {
        // Using the ID as the identifier
        let id: i64 = identifier
            .parse()
            .map_err(|_| SqlxDeadLetterError::Database(sqlx::Error::RowNotFound))?;

        sqlx::query("DELETE FROM dead_letter_messages WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn count_dead_letters(
        &self,
        stream: Option<&str>,
        consumer: Option<&str>,
    ) -> Result<u64, Self::Error> {
        let (query, bindings): (String, Vec<&str>) = match (stream, consumer) {
                (Some(s), Some(c)) => (
                    "SELECT COUNT(*) as count FROM dead_letter_messages WHERE stream = $1 AND consumer = $2".to_string(),
                    vec![s, c]
                ),
                (Some(s), None) => (
                    "SELECT COUNT(*) as count FROM dead_letter_messages WHERE stream = $1".to_string(),
                    vec![s]
                ),
                (None, Some(c)) => (
                    "SELECT COUNT(*) as count FROM dead_letter_messages WHERE consumer = $1".to_string(),
                    vec![c]
                ),
                (None, None) => (
                    "SELECT COUNT(*) as count FROM dead_letter_messages".to_string(),
                    vec![]
                ),
            };

        let mut query_builder = sqlx::query(&query);
        for binding in bindings {
            query_builder = query_builder.bind(binding);
        }

        let row = query_builder.fetch_one(&self.pool).await?;
        let count: i64 = row.try_get("count")?;

        Ok(count as u64)
    }
}
