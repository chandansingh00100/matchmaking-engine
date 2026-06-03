//! Core types for the matchmaking engine.
//!
//! Design notes:
//!  - `QueuedPlayer` is the unit of work flowing through the system.
//!  - `MatchResult` is the final product returned to callers.
//!  - All timestamps use `std::time::Instant` for monotonic, sub-ms precision.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Unique player identifier.
pub type PlayerId = u64;

/// Matchmaking Rating — signed so deltas can go negative.
pub type Mmr = i32;

// ---------------------------------------------------------------------------
// QueuedPlayer
// ---------------------------------------------------------------------------

/// A player sitting in the matchmaking queue.
///
/// `reserved` is set atomically to `true` the moment a worker thread "claims"
/// this player for a prospective match.  Any other thread that races to claim
/// the same player will see `reserved == true` and skip it, providing a
/// lock-free, single-ownership guarantee without any mutex.
#[derive(Debug)]
pub struct QueuedPlayer {
    pub player_id: PlayerId,
    pub mmr: Mmr,
    pub enqueue_time: Instant,
    /// Atomic flag: false = available, true = reserved by a worker thread.
    pub reserved: Arc<AtomicBool>,
}

impl QueuedPlayer {
    pub fn new(player_id: PlayerId, mmr: Mmr) -> Self {
        Self {
            player_id,
            mmr,
            enqueue_time: Instant::now(),
            reserved: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Attempt to atomically reserve this player.
    ///
    /// Uses `compare_exchange` with Acquire/Release ordering so that all
    /// subsequent accesses see a consistent view of the player data.
    ///
    /// Returns `true` iff this call was the one that flipped the flag.
    #[inline]
    pub fn try_reserve(&self) -> bool {
        self.reserved
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Release a reservation (e.g., match formation failed after partial claim).
    #[inline]
    pub fn release(&self) {
        self.reserved.store(false, Ordering::Release);
    }

    /// Seconds this player has spent in the queue.
    #[inline]
    pub fn wait_secs(&self) -> f64 {
        self.enqueue_time.elapsed().as_secs_f64()
    }

    /// Current acceptable MMR window half-width, expanding every 5 s by 25.
    ///
    /// Starts at ±50, grows without bound so no player waits forever.
    #[inline]
    pub fn mmr_tolerance(&self) -> Mmr {
        let elapsed = self.wait_secs();
        let expansions = (elapsed / 5.0) as Mmr;
        50 + expansions * 25
    }
}

// ---------------------------------------------------------------------------
// MatchResult
// ---------------------------------------------------------------------------

/// A completed, balanced 5v5 match.
#[derive(Debug, Clone)]
pub struct MatchResult {
    pub match_id: u64,
    pub team_a: Vec<PlayerSnapshot>,
    pub team_b: Vec<PlayerSnapshot>,
    /// |avg(team_a_mmr) - avg(team_b_mmr)|
    pub balance_score: f64,
    /// Average wait time of all 10 players (seconds).
    pub avg_wait_secs: f64,
}

impl MatchResult {
    /// Pretty-print a concise summary.
    pub fn summary(&self) -> String {
        let a_avg = avg_mmr(&self.team_a);
        let b_avg = avg_mmr(&self.team_b);
        format!(
            "Match {:>6} | TeamA avg={:.0} TeamB avg={:.0} | balance={:.1} | wait={:.2}s",
            self.match_id, a_avg, b_avg, self.balance_score, self.avg_wait_secs
        )
    }
}

/// Immutable snapshot of a player captured at match creation time.
#[derive(Debug, Clone)]
pub struct PlayerSnapshot {
    pub player_id: PlayerId,
    pub mmr: Mmr,
    pub wait_secs: f64,
}

fn avg_mmr(players: &[PlayerSnapshot]) -> f64 {
    if players.is_empty() {
        return 0.0;
    }
    players.iter().map(|p| p.mmr as f64).sum::<f64>() / players.len() as f64
}
