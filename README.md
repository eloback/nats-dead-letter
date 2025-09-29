# Nats Dead Letter Queue

A dead letter queue implementation for NATS JetStream that provides automatic handling of undelivered messages.

## Features

- **Automatic Dead Letter Detection**: Listens to NATS JetStream advisory messages for failed deliveries
- **Pluggable Storage**: Use any storage backend via the `DeadLetterStore` trait
- **Message Metadata**: Preserves original message content, headers, and timing information
- **Graceful Shutdown**: Proper cleanup of resources and background tasks
- **Customizable Streams**: Monitor specific JetStream streams
- **Multiple Failure Reasons**: Tracks max deliveries reached, terminated messages, and processing errors

## Usage

```rust
use nats_dead_letter::{NatsStore, DeadLetterStore};

// Connect to NATS and get JetStream context
let client = async_nats::connect("nats://localhost:4222").await?;
let jetstream = async_nats::jetstream::new(client);

// Create a dead letter store for monitoring your stream
let nats_store = NatsStore::new(jetstream, "admin_stream".to_string()).await?;

// Implement your storage backend
struct MyStorage;
impl DeadLetterStore for MyStorage {
    // ... implement storage methods
}

let storage = MyStorage;

// Start monitoring for dead letters
nats_store
    .run_dead_letter_automation(
        storage,
        "my_dead_letter" // unique name for consumer
    )
    .await?;

// Keep running...
nats_store.wait_graceful_shutdown().await;
```

## Storage Backends

The `DeadLetterStore` trait allows you to implement storage using any backend:

- SQL databases (PostgreSQL, MySQL, SQLite)
- NoSQL databases (MongoDB, DynamoDB)
- Key-value stores (Redis)
- File systems
- Other NATS streams

## Advisory Messages

This crate automatically handles NATS JetStream advisory messages:

- `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`

When these advisories are received, the original messages are retrieved and stored using your configured storage backend.

## Graceful Shutdown

The `NatsStore` provides graceful shutdown capabilities:

```rust
let task_tracker = nats_store.get_task_tracker();

// When shutting down...
task_tracker.close();
nats_store.wait_graceful_shutdown().await;
```

## License

MIT
