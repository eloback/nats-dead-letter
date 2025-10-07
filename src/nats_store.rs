use std::sync::{Arc, Mutex};

use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::consumer::{Consumer, DeliverPolicy};
use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, Stream as JetStream};
use async_nats::jetstream::Context;
use futures::StreamExt;
use stream_cancel::Trigger;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::dead_letter::{DeadLetterMessage, DeadLetterReason, DeadLetterStore};
use crate::error;
use crate::subject;

/// A handle to an event store implementation on top of NATS.
///
/// This type implements the needed traits for reading and writing events from
/// various event streams, encoded as durable messages in a Jetstream instance.
#[derive(Clone)]
pub struct NatsStore {
    name: &'static str,

    context: Context,
    stream: JetStream,

    subjects: Vec<String>,

    graceful_shutdown: GracefulShutdown,

    durable_consumer_options: ConsumerConfig,
}

/// A structure to help with graceful shutdown of tasks.
#[derive(Clone)]
pub struct GracefulShutdown {
    task_tracker: TaskTracker,
    exit_rx: Arc<Mutex<Receiver<Trigger>>>,
    exit_tx: Sender<Trigger>,
}

impl NatsStore {
    /// Create a new instance of a NATS event store.
    ///
    /// This uses an existing Jetstream context and a global prefix string. The
    /// method will attempt to use an existing stream with this name, or create
    /// a new one with default settings. All esrc event streams are created with
    /// this prefix, using the format `<prefix>.<event_name>.<aggregate_id>`.
    pub async fn try_new(context: Context, name: &'static str) -> error::Result<Self> {
        let stream = {
            let config = StreamConfig {
                name: name.to_owned(),
                subjects: vec!["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>".to_string()],
                discard: DiscardPolicy::New,
                ..Default::default()
            };

            context.get_or_create_stream(config).await?
        };

        let config = ConsumerConfig {
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        };

        // if there is more than 1000 automations this should be increased
        let (exit_tx, exit_rx) = tokio::sync::mpsc::channel::<stream_cancel::Trigger>(1000);
        let task_tracker = tokio_util::task::TaskTracker::new();

        let graceful_shutdown = GracefulShutdown {
            exit_tx,
            exit_rx: Mutex::new(exit_rx).into(),
            task_tracker,
        };

        Ok(Self {
            name,

            context,
            stream,

            subjects: vec!["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>".to_string()],

            graceful_shutdown,

            durable_consumer_options: config,
        })
    }

    /// get a handle to the task tracker used for graceful shutdown of tasks
    pub fn get_task_tracker(&self) -> TaskTracker {
        self.graceful_shutdown.task_tracker.clone()
    }

    ///
    pub async fn wait_graceful_shutdown(self) {
        let mut exit_rx = self
            .graceful_shutdown
            .exit_rx
            .lock()
            .expect("lock to not be poisoned");
        while let Some(trigger) = exit_rx.try_recv().ok() {
            println!("triggering graceful shutdown");
            trigger.cancel();
        }
        self.graceful_shutdown.task_tracker.close();
        self.graceful_shutdown.task_tracker.wait().await;
    }

    async fn durable_consumer(
        &self,
        name: String,
        subjects: Vec<String>,
    ) -> error::Result<Consumer<ConsumerConfig>> {
        let mut config = self.durable_consumer_options.clone();

        config.filter_subjects = subjects;
        config.durable_name = Some(name);

        Ok(self.stream.create_consumer(config).await?)
    }
}

impl NatsStore {
    /// Run the dead letter automation task.
    pub async fn run_dead_letter_automation<D>(&self, dead_letter_store: D) -> error::Result<()>
    where
        D: DeadLetterStore + Clone + 'static,
    {
        // Create a consumer for advisory messages
        let advisory_consumer = self
            .durable_consumer(self.name.to_string(), self.subjects.clone())
            .await?;

        let messages = advisory_consumer.messages().await?;

        let (exit, incoming) = stream_cancel::Valved::new(messages);
        self.graceful_shutdown
            .exit_tx
            .clone()
            .send(exit)
            .await
            .expect("should be able to send graceful trigger");

        let store = dead_letter_store.clone();
        let context = self.context.clone();
        let prefix = self.name;

        self.graceful_shutdown.task_tracker.spawn(async move {
            let mut incoming = incoming;
            while let Some(advisory_msg_result) = incoming.next().await {
                match advisory_msg_result {
                    Ok(advisory_msg) => {
                        if let Err(e) =
                            process_advisory_message(&store, &context, advisory_msg, prefix).await
                        {
                            error!("Error processing dead letter advisory: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error receiving advisory message: {:?}", e);
                    }
                }
            }
        });

        Ok(())
    }
}

async fn process_advisory_message<D>(
    store: &D,
    context: &Context,
    advisory_msg: async_nats::jetstream::Message,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    D: DeadLetterStore,
{
    let subject = advisory_msg.subject.to_string();
    let payload = &advisory_msg.payload;

    // Parse the advisory message to get the stream sequence
    let advisory_data: serde_json::Value = serde_json::from_slice(payload)?;

    let stream_seq = advisory_data
        .get("stream_seq")
        .and_then(|v| v.as_u64())
        .ok_or("Missing stream_seq in advisory message")?;

    let delivery_count = advisory_data
        .get("deliveries")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let stream_name = advisory_data
        .get("stream")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let consumer_name = advisory_data
        .get("consumer")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Determine the reason based on the advisory subject
    let reason = if subject.contains("MAX_DELIVERIES") {
        DeadLetterReason::MaxDeliveriesReached
    } else if subject.contains("MSG_TERMINATED") {
        DeadLetterReason::MessageTerminated
    } else {
        DeadLetterReason::ProcessingError("Unknown advisory type".to_string())
    };

    // Get the stream and retrieve the original message by sequence number
    let stream = context.get_stream(stream_name).await?;
    match stream.get_raw_message(stream_seq).await {
        Ok(original_msg) => {
            let (_, aggregate_id) = subject::subject_from_str(prefix, &original_msg.subject)?;
            let dead_letter_msg = DeadLetterMessage::from_stream_message(
                &original_msg,
                stream_name.to_string(),
                consumer_name.to_string(),
                stream_seq,
                reason,
                delivery_count,
                prefix.to_string(),
                aggregate_id,
            );

            if let Err(e) = store.store_dead_letter(dead_letter_msg).await {
                error!("Failed to store dead letter message: {:?}", e);
            } else {
                info!(
                    "Stored dead letter message: stream={}, consumer={}, seq={}",
                    stream_name, consumer_name, stream_seq
                );
            }
        }
        Err(e) => {
            warn!(
                "Could not retrieve original message for sequence {}: {:?}",
                stream_seq, e
            );

            // Create a dead letter message with just the advisory information
            let dead_letter_msg = DeadLetterMessage::from_advisory(
                stream_name.to_string(),
                consumer_name.to_string(),
                stream_seq,
                reason,
                delivery_count,
                "unknown".to_string(),
                vec![],
                None,
            );

            if let Err(e) = store.store_dead_letter(dead_letter_msg).await {
                error!("Failed to store dead letter message from advisory: {:?}", e);
            }
        }
    }

    // Acknowledge the advisory message
    let _ = advisory_msg.ack().await;

    Ok(())
}
