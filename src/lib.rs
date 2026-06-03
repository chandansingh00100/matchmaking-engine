//! # Matchmaking Engine
//!
//! Production-grade 5v5 real-time matchmaking engine.
//!
//! ## Modules
//! - [`types`]   — core data types (`QueuedPlayer`, `MatchResult`, …)
//! - [`queue`]   — MMR-ordered `BTreeMap` queue with O(log n) ops
//! - [`balancer`] — optimal 5v5 team splitter via bitmask DP (O(1))
//! - [`metrics`] — lock-free health metrics
//! - [`worker`]  — per-thread matching loop
//! - [`engine`]  — public API surface (`MatchmakingEngine`)

pub mod balancer;
pub mod engine;
pub mod metrics;
pub mod queue;
pub mod types;
pub mod worker;

pub use engine::{EngineConfig, MatchmakingEngine};
pub use types::{MatchResult, PlayerSnapshot};
