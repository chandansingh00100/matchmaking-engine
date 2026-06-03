//! Health metrics — collected lock-free via atomic operations.
//!
//! All hot-path updates use relaxed or release/acquire atomics so metrics
//! collection **never** blocks matchmaking threads.  The only non-atomic
//! operation is `wait_histogram`, protected by a `parking_lot::Mutex` that is
//! held for < 1 µs per update (just a Vec push).

use parking_lot::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// Global, shared metrics store.
pub struct Metrics {
    /// Current number of players waiting in the queue.
    pub queue_size: AtomicI64,
    /// Total matches created since engine start.
    pub matches_created: AtomicU64,
    /// Sum of all player wait times in milliseconds (for average calculation).
    wait_ms_total: AtomicU64,
    /// Total players that have exited the queue via a match.
    players_matched: AtomicU64,
    /// Worker cycles executed (one cycle = one scan pass over the queue).
    pub worker_cycles: AtomicU64,
    /// Raw wait-time samples (ms), used to compute percentiles on-demand.
    wait_samples: Mutex<Vec<u32>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            queue_size: AtomicI64::new(0),
            matches_created: AtomicU64::new(0),
            wait_ms_total: AtomicU64::new(0),
            players_matched: AtomicU64::new(0),
            worker_cycles: AtomicU64::new(0),
            wait_samples: Mutex::new(Vec::with_capacity(1_000_000)),
        }
    }

    // -----------------------------------------------------------------------
    // Hot-path updates (called from worker threads)
    // -----------------------------------------------------------------------

    #[inline]
    pub fn player_enqueued(&self) {
        self.queue_size.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn match_created(&self, player_wait_secs: &[f64]) {
        let n = player_wait_secs.len() as u64;
        let ms_sum: u64 = player_wait_secs
            .iter()
            .map(|&s| (s * 1000.0) as u64)
            .sum();

        self.queue_size.fetch_sub(n as i64, Ordering::Relaxed);
        self.matches_created.fetch_add(1, Ordering::Relaxed);
        self.wait_ms_total.fetch_add(ms_sum, Ordering::Relaxed);
        self.players_matched.fetch_add(n, Ordering::Relaxed);

        // Append samples — lock held < 1 µs
        let mut samples = self.wait_samples.lock();
        for &s in player_wait_secs {
            samples.push((s * 1000.0) as u32);
        }
    }

    #[inline]
    pub fn worker_cycle(&self) {
        self.worker_cycles.fetch_add(1, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Read-path (called from stats threads — infrequent)
    // -----------------------------------------------------------------------

    pub fn snapshot(&self) -> MetricsSnapshot {
        let matched = self.players_matched.load(Ordering::Relaxed);
        let wait_ms_total = self.wait_ms_total.load(Ordering::Relaxed);
        let avg_wait_ms = if matched > 0 {
            wait_ms_total as f64 / matched as f64
        } else {
            0.0
        };

        let (p50, p95) = self.percentiles();

        MetricsSnapshot {
            queue_size: self.queue_size.load(Ordering::Relaxed).max(0) as u64,
            matches_created: self.matches_created.load(Ordering::Relaxed),
            players_matched: matched,
            avg_wait_ms,
            p50_wait_ms: p50,
            p95_wait_ms: p95,
            worker_cycles: self.worker_cycles.load(Ordering::Relaxed),
        }
    }

    /// Compute p50 and p95 from the raw sample buffer.
    ///
    /// Clones and sorts a snapshot of the buffer — acceptable for a metrics
    /// read path that runs every few seconds.
    fn percentiles(&self) -> (f64, f64) {
        let samples = self.wait_samples.lock();
        if samples.is_empty() {
            return (0.0, 0.0);
        }
        let mut sorted = samples.clone();
        drop(samples); // release lock before sort
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() * 50 / 100] as f64;
        let p95 = sorted[sorted.len() * 95 / 100] as f64;
        (p50, p95)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot for reporting.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub queue_size: u64,
    pub matches_created: u64,
    pub players_matched: u64,
    pub avg_wait_ms: f64,
    pub p50_wait_ms: f64,
    pub p95_wait_ms: f64,
    pub worker_cycles: u64,
}

impl MetricsSnapshot {
    pub fn print(&self, elapsed: Duration) {
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            self.matches_created as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        println!("┌─────────────────────────────────────────────┐");
        println!("│           MATCHMAKING ENGINE METRICS         │");
        println!("├─────────────────────────────────────────────┤");
        println!("│  Queue size          : {:>10} players    │", self.queue_size);
        println!("│  Matches created     : {:>10}           │", self.matches_created);
        println!("│  Players matched     : {:>10}           │", self.players_matched);
        println!("│  Avg wait            : {:>10.1} ms        │", self.avg_wait_ms);
        println!("│  P50 wait            : {:>10.1} ms        │", self.p50_wait_ms);
        println!("│  P95 wait            : {:>10.1} ms        │", self.p95_wait_ms);
        println!("│  Worker cycles       : {:>10}           │", self.worker_cycles);
        println!("│  Throughput          : {:>10.2} matches/s │", throughput);
        println!("└─────────────────────────────────────────────┘");
    }
}
