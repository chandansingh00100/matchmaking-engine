//! Worker thread implementation.
//!
//! # Matching Algorithm (per worker cycle)
//!
//! 1. **Anchor selection** — Each worker samples a subset of queue positions
//!    (every `worker_count`-th player, offset by `worker_id`) to avoid all
//!    workers scanning the same players simultaneously.
//!
//! 2. **Window scan** — For each anchor, call `queue.find_candidates` to get
//!    up to 10 players within `anchor.mmr_tolerance()` using BTreeMap::range
//!    → O(log n + k).
//!
//! 3. **Atomic reservation** — Call `try_reserve` on each candidate.  If we
//!    successfully reserve 10, proceed.  If any `try_reserve` fails (another
//!    worker got there first), release all already-reserved players and try
//!    the next anchor.
//!
//! 4. **Team balancing** — Call `balance_teams` on the 10 reserved players.
//!    This is O(1) (252 bitmask iterations).
//!
//! 5. **Commit** — Remove the 10 players from the BTreeMap and emit the
//!    `MatchResult` on the output channel.
//!
//! 6. **Sleep** — If no match was formed, back off for `sleep_ms` milliseconds
//!    to avoid spinning.

use crate::balancer::balance_teams;
use crate::metrics::Metrics;
use crate::queue::MatchmakingQueue;
use crate::types::{PlayerSnapshot, QueuedPlayer};
use crossbeam_channel::Sender;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::types::MatchResult;

const MATCH_SIZE: usize = 10;

/// Configuration for a worker thread.
pub struct WorkerConfig {
    pub worker_id: usize,
    pub worker_count: usize,
    /// How long to sleep (µs) when the queue has nothing to match.
    pub idle_sleep_us: u64,
}

/// Spawn a worker thread that continuously tries to form matches.
///
/// Returns the `JoinHandle` so the caller can manage lifecycle.
pub fn spawn_worker(
    config: WorkerConfig,
    queue: Arc<MatchmakingQueue>,
    metrics: Arc<Metrics>,
    match_tx: Sender<MatchResult>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        run_worker(config, queue, metrics, match_tx, shutdown);
    })
}

fn run_worker(
    cfg: WorkerConfig,
    queue: Arc<MatchmakingQueue>,
    metrics: Arc<Metrics>,
    match_tx: Sender<MatchResult>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;

    while !shutdown.load(Ordering::Relaxed) {
        metrics.worker_cycle();

        // Not enough players in queue — sleep and retry.
        if queue.len() < MATCH_SIZE {
            thread::sleep(Duration::from_micros(cfg.idle_sleep_us));
            continue;
        }

        // --- Step 1: Anchor selection ---
        // Each worker takes every `worker_count`-th anchor starting at offset
        // `worker_id`, so workers partition the search space with minimal
        // contention.
        let anchors = queue.sample_anchors(cfg.worker_count);
        let my_anchors: Vec<_> = anchors
            .iter()
            .enumerate()
            .filter(|(i, _)| i % cfg.worker_count == cfg.worker_id)
            .map(|(_, p)| Arc::clone(p))
            .collect();

        let mut formed = false;

        'anchor: for anchor in &my_anchors {
            // Skip already-reserved anchors.
            if anchor.reserved.load(Ordering::Relaxed) {
                continue;
            }

            let tolerance = anchor.mmr_tolerance();

            // --- Step 2: Window scan O(log n + k) ---
            let candidates = queue.find_candidates(anchor.mmr, tolerance, MATCH_SIZE * 3);

            if candidates.len() < MATCH_SIZE {
                continue;
            }

            // --- Step 3: Atomic reservation ---
            let mut reserved: Vec<Arc<QueuedPlayer>> = Vec::with_capacity(MATCH_SIZE);

            for candidate in &candidates {
                if candidate.try_reserve() {
                    reserved.push(Arc::clone(candidate));
                    if reserved.len() == MATCH_SIZE {
                        break;
                    }
                }
            }

            if reserved.len() < MATCH_SIZE {
                // Failed to lock 10 unique players — release all and move on.
                for p in &reserved {
                    p.release();
                }
                continue 'anchor;
            }

            // --- Step 4: Team balancing O(1) ---
            let snapshots: Vec<PlayerSnapshot> = reserved
                .iter()
                .map(|p| PlayerSnapshot {
                    player_id: p.player_id,
                    mmr: p.mmr,
                    wait_secs: p.wait_secs(),
                })
                .collect();

            let wait_times: Vec<f64> = snapshots.iter().map(|s| s.wait_secs).collect();
            let result = balance_teams(snapshots);

            // --- Step 5: Commit ---
            queue.remove_batch(&reserved);
            metrics.match_created(&wait_times);

            // Send result — channel is bounded; if full the worker blocks
            // briefly (back-pressure) rather than dropping matches.
            let _ = match_tx.send(result);

            formed = true;
            break 'anchor;
        }

        if !formed {
            thread::sleep(Duration::from_micros(cfg.idle_sleep_us));
        }
    }
}
