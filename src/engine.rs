//! `MatchmakingEngine` — public API surface.
//!
//! # Lifecycle
//!
//! ```text
//! MatchmakingEngine::new(config)
//!     │
//!     ├── starts N worker threads
//!     ├── starts 1 stats reporter thread
//!     │
//!     ▼
//! engine.enqueue(player_id, mmr)   ← called from any thread, O(log n)
//!     │
//!     ▼
//! engine.matches()                 ← returns Receiver<MatchResult>
//!     │
//!     ▼
//! engine.shutdown()                ← signals all threads to stop
//! ```

use crate::metrics::Metrics;
use crate::queue::MatchmakingQueue;
use crate::types::{MatchResult, PlayerId, QueuedPlayer};
use crate::worker::{spawn_worker, WorkerConfig};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Engine configuration.
#[derive(Clone)]
pub struct EngineConfig {
    /// Number of parallel worker threads.
    pub worker_threads: usize,
    /// Capacity of the match output channel.
    pub match_channel_capacity: usize,
    /// How long a worker sleeps when idle (µs).
    pub worker_idle_sleep_us: u64,
    /// Whether to print periodic stats to stdout.
    pub enable_stats_reporter: bool,
    /// Stats reporter interval.
    pub stats_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus(),
            match_channel_capacity: 8192,
            worker_idle_sleep_us: 500,
            enable_stats_reporter: true,
            stats_interval: Duration::from_secs(5),
        }
    }
}

fn num_cpus() -> usize {
    // Portable CPU count without an extra crate.
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(2)
}

/// The matchmaking engine handle.
pub struct MatchmakingEngine {
    queue: Arc<MatchmakingQueue>,
    metrics: Arc<Metrics>,
    match_tx: Sender<MatchResult>,
    match_rx: Receiver<MatchResult>,
    shutdown: Arc<AtomicBool>,
    worker_handles: Vec<thread::JoinHandle<()>>,
    stats_handle: Option<thread::JoinHandle<()>>,
    start_time: Instant,
}

impl MatchmakingEngine {
    /// Construct and start the engine.
    pub fn new(config: EngineConfig) -> Self {
        let queue = Arc::new(MatchmakingQueue::new());
        let metrics = Arc::new(Metrics::new());
        let (match_tx, match_rx) = bounded(config.match_channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn worker threads.
        let worker_count = config.worker_threads;
        let mut worker_handles = Vec::with_capacity(worker_count);

        for id in 0..worker_count {
            let handle = spawn_worker(
                WorkerConfig {
                    worker_id: id,
                    worker_count,
                    idle_sleep_us: config.worker_idle_sleep_us,
                },
                Arc::clone(&queue),
                Arc::clone(&metrics),
                match_tx.clone(),
                Arc::clone(&shutdown),
            );
            worker_handles.push(handle);
        }

        // Optional stats reporter thread.
        let stats_handle = if config.enable_stats_reporter {
            let metrics_clone = Arc::clone(&metrics);
            let shutdown_clone = Arc::clone(&shutdown);
            let interval = config.stats_interval;
            let start = Instant::now();
            Some(thread::spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    thread::sleep(interval);
                    let snap = metrics_clone.snapshot();
                    snap.print(start.elapsed());
                }
            }))
        } else {
            None
        };

        Self {
            queue,
            metrics,
            match_tx,
            match_rx,
            shutdown,
            worker_handles,
            stats_handle,
            start_time: Instant::now(),
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Enqueue a player for matchmaking.
    ///
    /// Thread-safe; O(log n).  Returns `false` if the player is already
    /// in the queue (duplicate detection by (mmr, player_id) key).
    pub fn enqueue(&self, player_id: PlayerId, mmr: i32) -> bool {
        let player = Arc::new(QueuedPlayer::new(player_id, mmr));
        let added = self.queue.enqueue(player);
        if added {
            self.metrics.player_enqueued();
        }
        added
    }

    /// Receiver end of the match output channel.
    ///
    /// Callers can drain this in a dedicated thread or use `try_recv` in a
    /// poll loop.
    pub fn matches(&self) -> &Receiver<MatchResult> {
        &self.match_rx
    }

    /// Current queue depth.
    pub fn queue_size(&self) -> usize {
        self.queue.len()
    }

    /// Point-in-time metrics snapshot.
    pub fn metrics_snapshot(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Elapsed time since engine start.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Signal all workers to stop and join them.
    ///
    /// Blocks until all threads have exited.  Any unprocessed matches
    /// already in the channel remain readable after shutdown.
    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for h in self.worker_handles {
            let _ = h.join();
        }
        if let Some(h) = self.stats_handle {
            let _ = h.join();
        }
    }
}
