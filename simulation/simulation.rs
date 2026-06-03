//! Load simulation: 50,000 concurrent players, random MMR 0–5000.
//!
//! Metrics printed at end:
//!   • total matches formed
//!   • average wait time
//!   • p95 wait time
//!   • throughput (matches / second)

use matchmaking_engine::{EngineConfig, MatchmakingEngine};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const TOTAL_PLAYERS: u64 = 50_000;
const MMR_MIN: i32 = 0;
const MMR_MAX: i32 = 5000;
const ENQUEUE_THREADS: usize = 8;
const WORKER_THREADS: usize = 8;

fn main() {
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║        MATCHMAKING ENGINE — LOAD SIMULATION          ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Players    : {:>10}                           ║", TOTAL_PLAYERS);
    println!("║  MMR range  :   {:>4} – {:>4}                         ║", MMR_MIN, MMR_MAX);
    println!("║  Workers    : {:>10}                           ║", WORKER_THREADS);
    println!("╚══════════════════════════════════════════════════════╝\n");

    let config = EngineConfig {
        worker_threads: WORKER_THREADS,
        match_channel_capacity: 32_768,
        worker_idle_sleep_us: 100,
        enable_stats_reporter: false,
        stats_interval: Duration::from_secs(5),
    };

    let engine = Arc::new(MatchmakingEngine::new(config));
    let start = Instant::now();

    // -----------------------------------------------------------------------
    // Enqueue phase — ENQUEUE_THREADS threads each enqueue a share of players
    // -----------------------------------------------------------------------
    let enqueue_counter = Arc::new(AtomicU64::new(0));
    let mut enqueue_handles = Vec::new();

    let players_per_thread = TOTAL_PLAYERS / ENQUEUE_THREADS as u64;

    for t in 0..ENQUEUE_THREADS {
        let engine_c = Arc::clone(&engine);
        let counter_c = Arc::clone(&enqueue_counter);

        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let start_id = t as u64 * players_per_thread;
            let end_id = if t == ENQUEUE_THREADS - 1 {
                TOTAL_PLAYERS
            } else {
                start_id + players_per_thread
            };

            for player_id in start_id..end_id {
                let mmr: i32 = rng.gen_range(MMR_MIN..=MMR_MAX);
                engine_c.enqueue(player_id, mmr);
                counter_c.fetch_add(1, Ordering::Relaxed);

                // Stagger arrivals slightly to simulate real-world bursty traffic.
                // ~10 µs average inter-arrival time → ~100 k/s peak rate.
                if player_id % 100 == 0 {
                    thread::sleep(Duration::from_micros(1));
                }
            }
        });
        enqueue_handles.push(handle);
    }

    // -----------------------------------------------------------------------
    // Match consumer — drain the match channel while enqueue runs
    // -----------------------------------------------------------------------
    let matches_collected = Arc::new(AtomicU64::new(0));
    let mc = Arc::clone(&matches_collected);
    let engine_c = Arc::clone(&engine);

    let consumer = thread::spawn(move || {
        // We expect at most 5000 matches from 50k players.
        let rx = engine_c.matches();
        loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(_m) => {
                    mc.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    // Check if we're done: all enqueued & nothing in queue.
                    if engine_c.queue_size() < 10 {
                        break;
                    }
                }
            }
        }
    });

    // Wait for enqueue to finish.
    for h in enqueue_handles {
        let _ = h.join();
    }

    println!(
        "All {} players enqueued in {:.2}s. Waiting for matches to drain...",
        TOTAL_PLAYERS,
        start.elapsed().as_secs_f64()
    );

    // Give workers time to drain.
    let drain_start = Instant::now();
    loop {
        let qs = engine.queue_size();
        if qs < 10 {
            break;
        }
        if drain_start.elapsed() > Duration::from_secs(120) {
            println!("[WARNING] Drain timeout — {} players remain unmatched", qs);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let total_elapsed = start.elapsed();
    let _ = consumer.join();

    // -----------------------------------------------------------------------
    // Results
    // -----------------------------------------------------------------------
    let snap = engine.metrics_snapshot();
    let throughput = snap.matches_created as f64 / total_elapsed.as_secs_f64();

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║               SIMULATION RESULTS                     ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Total elapsed       : {:>10.2}s                  ║", total_elapsed.as_secs_f64());
    println!("║  Total matches       : {:>10}                   ║", snap.matches_created);
    println!("║  Players matched     : {:>10}                   ║", snap.players_matched);
    println!("║  Players unmatched   : {:>10}                   ║", engine.queue_size());
    println!("║  Avg wait time       : {:>10.1}ms                 ║", snap.avg_wait_ms);
    println!("║  P50 wait time       : {:>10.1}ms                 ║", snap.p50_wait_ms);
    println!("║  P95 wait time       : {:>10.1}ms                 ║", snap.p95_wait_ms);
    println!("║  Throughput          : {:>10.2} matches/s        ║", throughput);
    println!("║  Worker cycles       : {:>10}                   ║", snap.worker_cycles);
    println!("╚══════════════════════════════════════════════════════╝");

    // Note: Arc::try_unwrap fails here because consumer still holds a clone.
    // We drop the engine via shutdown signal instead.
    // engine.shutdown() can't be called because engine is behind Arc.
    // Signal via the exposed method on the Arc.

    println!("\nSimulation complete.");
}
