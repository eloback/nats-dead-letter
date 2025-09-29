#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Dead letter queue functionality for NATS JetStream
//!
//! This crate provides dead letter queue automation for NATS JetStream consumers.
//! It listens to advisory messages for failed message deliveries and stores them
//! using a configurable storage backend.

/// dead letter types
pub mod dead_letter;
mod nats_store;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use dead_letter::{DeadLetterMessage, DeadLetterReason, DeadLetterStore};
pub use nats_store::NatsStore;

mod error {
    pub use eyre::Result;
}

