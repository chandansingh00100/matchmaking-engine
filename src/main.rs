//! Matchmaking Engine — interactive demo entry point.

use matchmaking_engine::{EngineConfig, MatchmakingEngine};
use std::time::Duration;

fn main() {
    println!("=== Matchmaking Engine Demo ===\n");

    let config = EngineConfig {
        worker_threads: 4,
        enable_stats_reporter: false,
        worker_idle_sleep_us: 100,
        ..Default::default()
    };

    let engine = MatchmakingEngine::new(config);

    // Enqueue 20 players with varying MMR in two skill brackets.
    let players: Vec<(u64, i32)> = vec![
        (1,1000),(2,1020),(3,980),(4,1010),(5,990),
        (6,1005),(7,995),(8,1015),(9,985),(10,1025),
        (11,1500),(12,1480),(13,1520),(14,1490),(15,1510),
        (16,1505),(17,1495),(18,1515),(19,1485),(20,1525),
    ];

    for (id, mmr) in &players {
        engine.enqueue(*id, *mmr);
    }

    println!("Enqueued {} players. Waiting for matches...\n", players.len());

    let mut matches_seen = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);

    loop {
        match engine.matches().recv_timeout(Duration::from_millis(200)) {
            Ok(m) => {
                println!("{}", m.summary());
                println!("  Team A: {:?}", m.team_a.iter().map(|p|(p.player_id,p.mmr)).collect::<Vec<_>>());
                println!("  Team B: {:?}", m.team_b.iter().map(|p|(p.player_id,p.mmr)).collect::<Vec<_>>());
                println!();
                matches_seen += 1;
                if matches_seen >= 2 { break; }
            }
            Err(_) => {
                if std::time::Instant::now() > deadline {
                    println!("Timeout waiting for matches.");
                    break;
                }
            }
        }
    }

    println!("\n--- Final Metrics ---");
    engine.metrics_snapshot().print(engine.elapsed());
    engine.shutdown();
    println!("\nEngine shut down cleanly.");
}
